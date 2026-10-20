//! Server-side fault injection.
//!
//! Foreign clients (pdfcraft's WS-Print client, a Windows spooler) talk straight to the HTTP
//! server, so a test that wants "the third `SendDocument` fails with a temporary error" or "the
//! printer answers 503" has to arm that on the server. An [`InjectRule`] says which requests it
//! applies to (operation, endpoint), how many of them to let through first (`skip`), how many
//! to affect (`count`, `None` = forever), an optional delay, and the [`InjectEffect`].
//!
//! Rules live in an [`Injector`], an ordered list consumed deterministically: for each request
//! the **first rule that matches decides**. If it still has `skip` left the request passes
//! untouched (and later rules are not consulted); otherwise its effect applies, its `count`
//! drops by one, and an exhausted rule is removed. The table is shared by all requests and
//! behind a mutex, so concurrent requests consume rules one at a time.
//!
//! Injection happens in the HTTP layer **before** dispatch (see `wsd::print_service`), so a
//! rule never reaches the job engine: a faulted `SendDocument` leaves the job untouched. It does
//! not apply to the `/v1/*` control routes. The IPP route consults the table too, but only
//! for rules whose `endpoint` is [`RuleEndpoint::Ipp`]; there `op` is an IPP operation name
//! (`print_job`, `get_jobs`, ..., spelling-insensitive) and the extra effect
//! [`InjectEffect::IppStatus`] answers a well-formed IPP reply with a chosen status code.
//!
//! JSON shape (the body of `POST /v1/faults` accepts `"inject": [rule, ...]`):
//!
//! ```json
//! { "op": "SendDocument",            // short name or full action URI; absent = any
//!   "endpoint": "print_service",     // device | print_service | discovery | ipp; absent = any WSD
//!   "skip": 1,                       // matches to let through first (default 0)
//!   "count": 2,                      // matches to affect; null = forever; absent = 1
//!   "delay_ms": 250,                 // sleep before processing (default none)
//!   "delay_after": false,            // true: process first, sleep before replying
//!   "effect": { "type": "fault", "code": "receiver",
//!               "subcode": "wprt:ServerErrorTemporaryError",
//!               "reason": "try later", "detail": "text", "http_status": 500 } }
//! ```
//!
//! Effects: `{"type":"pass"}` (default; with a delay it simulates a slow printer),
//! `{"type":"fault",...}` (a SOAP fault built by [`PrintFault`], so it is well formed and
//! carries `RelatesTo`), `{"type":"http","status":503,"body":"...","content_type":"text/html"}`
//! (a non-SOAP failure) and `{"type":"close_connection"}` (drop without a proper answer).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize};

use super::fault::{FaultCode, FaultKind, PrintFault};
use super::soap::{short_action, HttpReply, SoapReply, SoapRequest};
use super::types::{EndpointKind, PrintOp};
use super::validate::ReplyValidator;
use super::xml::{ns, Element, QName};

/// Namespace of the optional `detail` text of an injected fault (not a print element, so the
/// reply validator leaves it alone).
pub const INJECT_NS: &str = "urn:fakeprint:inject";

/// Longest delay a rule may ask for (ten minutes); a longer one is refused.
pub const MAX_DELAY_MS: u64 = 600_000;

/// Pending injection rules one device will hold. A request that would pass the cap is refused
/// and adds nothing.
pub const MAX_PENDING_RULES: usize = 256;

/// Largest `Http` effect body (1 MiB).
pub const MAX_HTTP_BODY: usize = 1 << 20;

/// Where a rule applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleEndpoint {
    /// `/WebServices/Device`.
    Device,
    /// `/WebServices/PrinterService` and the `/PrintService` alias.
    #[serde(alias = "service", alias = "printer_service", alias = "print")]
    PrintService,
    /// Directed WS-Discovery over HTTP (not routed through injection today; accepted so a rule
    /// can be written ahead of it).
    Discovery,
    /// `/ipp/print`.
    Ipp,
}

impl From<EndpointKind> for RuleEndpoint {
    fn from(k: EndpointKind) -> Self {
        match k {
            EndpointKind::Device => RuleEndpoint::Device,
            EndpointKind::PrintService => RuleEndpoint::PrintService,
            EndpointKind::Discovery => RuleEndpoint::Discovery,
        }
    }
}

/// `soap:Code/soap:Value` of an injected fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FaultSide {
    #[serde(alias = "sender")]
    Sender,
    #[serde(alias = "receiver")]
    Receiver,
}

impl FaultSide {
    fn code(self) -> FaultCode {
        match self {
            FaultSide::Sender => FaultCode::Sender,
            FaultSide::Receiver => FaultCode::Receiver,
        }
    }
}

/// What an injected request gets.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum InjectEffect {
    /// Process normally (use with a delay for a slow printer, or alone to let requests through).
    #[default]
    Pass,
    /// Answer a SOAP fault.
    Fault {
        /// `Sender` or `Receiver`; absent: the catalogued kind's code, else `Receiver`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<FaultSide>,
        /// A QName token: `wprt:ServerErrorTemporaryError`, `wsa:ActionNotSupported`, a bare
        /// catalogue name (`InvalidArgs`) or Clark notation `{ns}local`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subcode: Option<String>,
        /// Reason text; absent: the catalogued default.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        /// Text placed in `soap:Detail` as `<inject:Detail>`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
        /// HTTP status; absent: 400 for `Sender`, 500 for `Receiver`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        http_status: Option<u16>,
    },
    /// Answer a plain HTTP response that is not SOAP (a proxy page, a 503).
    Http {
        status: u16,
        #[serde(default)]
        body: String,
        /// Default `text/plain; charset=utf-8`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_type: Option<String>,
    },
    /// Answer a well-formed IPP response with this status code and no other attributes (IPP
    /// endpoint only). `status` is a registry name (`server-error-busy`), a decimal number or
    /// `0x` hex; `message` becomes `status-message`.
    IppStatus {
        status: IppStatusSpec,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    /// Drop the connection without a usable answer.
    CloseConnection,
}

/// The `status` of an [`InjectEffect::IppStatus`] as written in JSON: a name or a number. It is
/// resolved to the code when the rule is checked (`InjectRule::check`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IppStatusSpec {
    Code(u16),
    Name(String),
}

impl IppStatusSpec {
    /// The status code, if the spec names one.
    pub fn code(&self) -> Option<u16> {
        match self {
            IppStatusSpec::Code(c) => Some(*c),
            IppStatusSpec::Name(n) => crate::ipp::codec::status_from_str(n),
        }
    }
}

fn one() -> Option<u32> {
    Some(1)
}

fn count_field<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u32>, D::Error> {
    Option::<u32>::deserialize(d)
}

/// One injection rule. See the module documentation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InjectRule {
    /// Short operation name (`SendDocument`, case-insensitive) or a full action URI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<RuleEndpoint>,
    /// Matching requests to let through before the rule starts to apply.
    #[serde(default)]
    pub skip: u32,
    /// Requests to affect; `None` = forever. Default 1.
    #[serde(default = "one", deserialize_with = "count_field")]
    pub count: Option<u32>,
    /// Sleep this long (async) before processing, or before replying when `delay_after`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delay_ms: Option<u64>,
    #[serde(default)]
    pub delay_after: bool,
    #[serde(default)]
    pub effect: InjectEffect,
}

impl Default for InjectRule {
    fn default() -> Self {
        Self {
            op: None,
            endpoint: None,
            skip: 0,
            count: Some(1),
            delay_ms: None,
            delay_after: false,
            effect: InjectEffect::Pass,
        }
    }
}

impl InjectRule {
    /// A rule applying `effect` once to any request.
    pub fn new(effect: InjectEffect) -> Self {
        Self {
            effect,
            ..Self::default()
        }
    }

    /// A SOAP fault of a catalogued kind (subcode, code and reason follow the kind).
    pub fn fault(kind: FaultKind) -> Self {
        Self::new(InjectEffect::Fault {
            code: match kind.code() {
                FaultCode::Sender => Some(FaultSide::Sender),
                FaultCode::Receiver => Some(FaultSide::Receiver),
                _ => None,
            },
            subcode: kind.subcode_local().map(|l| match kind.subcode() {
                Some(q) if q.ns == ns::WSA => format!("wsa:{l}"),
                _ => format!("wprt:{l}"),
            }),
            reason: None,
            detail: None,
            http_status: None,
        })
    }

    /// A non-SOAP HTTP answer.
    pub fn http(status: u16, body: impl Into<String>) -> Self {
        Self::new(InjectEffect::Http {
            status,
            body: body.into(),
            content_type: None,
        })
    }

    /// Drop the connection.
    pub fn close_connection() -> Self {
        Self::new(InjectEffect::CloseConnection)
    }

    /// Only slow the matching requests down.
    pub fn delay(ms: u64) -> Self {
        Self {
            delay_ms: Some(ms),
            ..Self::default()
        }
    }

    /// Restrict to an operation (short name or action URI).
    pub fn for_op(mut self, op: impl Into<String>) -> Self {
        self.op = Some(op.into());
        self
    }

    /// Restrict to a [`PrintOp`].
    pub fn for_print_op(self, op: PrintOp) -> Self {
        self.for_op(op.name())
    }

    pub fn on(mut self, endpoint: RuleEndpoint) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn skipping(mut self, n: u32) -> Self {
        self.skip = n;
        self
    }

    pub fn times(mut self, n: u32) -> Self {
        self.count = Some(n);
        self
    }

    pub fn forever(mut self) -> Self {
        self.count = None;
        self
    }

    pub fn delayed(mut self, ms: u64) -> Self {
        self.delay_ms = Some(ms);
        self
    }

    /// Delay after processing, before the reply goes out.
    pub fn delayed_after(mut self, ms: u64) -> Self {
        self.delay_ms = Some(ms);
        self.delay_after = true;
        self
    }

    /// Whether the rule is well formed (what `POST /v1/faults` answers 400 for).
    pub fn check(&self) -> Result<(), String> {
        if self.count == Some(0) {
            return Err("count must be 1 or more (use null for forever)".into());
        }
        if self.delay_ms.is_some_and(|d| d > MAX_DELAY_MS) {
            return Err(format!("delay_ms is at most {MAX_DELAY_MS}"));
        }
        if self.op.as_deref().is_some_and(|o| o.trim().is_empty()) {
            return Err("op must not be empty".into());
        }
        match &self.effect {
            InjectEffect::Fault {
                subcode,
                http_status,
                ..
            } => {
                if let Some(s) = subcode {
                    parse_subcode(s)?;
                }
                if http_status.is_some_and(|s| !(200..=599).contains(&s)) {
                    return Err("http_status must be 200..=599".into());
                }
            }
            InjectEffect::Http {
                status,
                body,
                content_type,
            } => {
                if !(100..=599).contains(status) {
                    return Err("http status must be 100..=599".into());
                }
                if body.len() > MAX_HTTP_BODY {
                    return Err(format!("http body is at most {MAX_HTTP_BODY} bytes"));
                }
                if content_type
                    .as_deref()
                    .is_some_and(|c| c.contains(['\r', '\n']))
                {
                    return Err("content_type must be one line".into());
                }
            }
            InjectEffect::IppStatus { status, .. } => {
                if status.code().is_none() {
                    return Err(format!(
                        "ipp_status {status:?} is neither an IPP status name nor a code"
                    ));
                }
                if self.endpoint != Some(RuleEndpoint::Ipp) {
                    return Err("an ipp_status effect needs \"endpoint\": \"ipp\"".into());
                }
            }
            InjectEffect::Pass | InjectEffect::CloseConnection => {}
        }
        Ok(())
    }

    fn matches(&self, q: &InjectQuery<'_>) -> bool {
        if let Some(e) = self.endpoint {
            if e != q.endpoint {
                return false;
            }
        } else if q.endpoint == RuleEndpoint::Ipp {
            return false;
        }
        match (&self.op, q.action) {
            (None, _) => true,
            (Some(op), Some(action)) if q.endpoint == RuleEndpoint::Ipp => {
                ipp_op_matches(op, action)
            }
            (Some(op), Some(action)) => op_matches(op, action),
            (Some(_), None) => false,
        }
    }
}

/// Whether the rule's `op` names the request's `action`: the exact URI, the short name
/// (case-insensitive), or the operation the lenient action table resolves it to.
pub fn op_matches(op: &str, action: &str) -> bool {
    let op = op.trim();
    let action = action.trim();
    if op == action {
        return true;
    }
    if op.eq_ignore_ascii_case(short_action(action)) {
        return true;
    }
    if let Some(known) = PrintOp::from_action_lenient(action) {
        if op.eq_ignore_ascii_case(known.name()) || Some(op) == known.action() {
            return true;
        }
    }
    false
}

/// Whether a rule's `op` names an IPP operation: `print_job`, `Print-Job` and `PrintJob` are the
/// same name (case, `-` and `_` do not matter).
pub fn ipp_op_matches(op: &str, action: &str) -> bool {
    let norm = |s: &str| {
        s.chars()
            .filter(|c| !matches!(c, '-' | '_' | ' '))
            .collect::<String>()
            .to_ascii_lowercase()
    };
    norm(op) == norm(action)
}

/// What a request looks like to the matcher.
#[derive(Debug, Clone, Copy)]
pub struct InjectQuery<'a> {
    pub endpoint: RuleEndpoint,
    /// `wsa:Action` on the WSD routes; on the IPP route the operation name in injection spelling
    /// (`print_job`, `get_jobs`, ...), `None` when the request is not an IPP message.
    pub action: Option<&'a str>,
}

/// The decision for one request: sleep, then do the effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Injection {
    /// Which rule decided: `<op or *>[<queue index>]:<effect>` (the request log shows it).
    pub rule: String,
    pub delay: Option<Duration>,
    pub delay_after: bool,
    pub effect: InjectEffect,
}

impl Injection {
    /// Sleep for the delay when it applies before (`after == false`) or after processing.
    pub async fn sleep(&self, after: bool) {
        if let Some(d) = self.delay {
            if self.delay_after == after && !d.is_zero() {
                tokio::time::sleep(d).await;
            }
        }
    }

    /// The answer this injection gives to a WSD request (`None` for `Pass`: dispatch normally,
    /// and `CloseConnection` is reported through [`Injection::closes`]).
    pub fn wsd_reply(&self, req: &SoapRequest, validator: &ReplyValidator) -> Option<HttpReply> {
        match &self.effect {
            InjectEffect::Pass | InjectEffect::CloseConnection | InjectEffect::IppStatus { .. } => {
                None
            }
            InjectEffect::Http { .. } => self.http_reply(),
            InjectEffect::Fault { .. } => {
                let (fault, status) = self.build_fault()?;
                let mut reply: SoapReply = req.fault_reply(&fault);
                if let Some(s) = status {
                    reply = reply.with_status(s);
                }
                // A deliberately foreign wprt subcode is the test's business, not a defect.
                let foreign = fault
                    .subcode
                    .as_ref()
                    .is_some_and(|q| q.ns == ns::PRINT && FaultKind::from_subcode(q).is_none());
                if !foreign {
                    validator.check_reply(PrintOp::from_action_lenient(&req.action), &reply);
                }
                Some(reply.into_http())
            }
        }
    }

    /// The non-SOAP HTTP answer of an `Http` effect (also what the IPP route returns for a
    /// `Fault` effect, as a bare status).
    pub fn http_reply(&self) -> Option<HttpReply> {
        match &self.effect {
            InjectEffect::Http {
                status,
                body,
                content_type,
            } => Some(HttpReply {
                status: *status,
                content_type: content_type
                    .clone()
                    .unwrap_or_else(|| "text/plain; charset=utf-8".to_string()),
                body: body.clone().into_bytes(),
            }),
            InjectEffect::Fault {
                http_status,
                reason,
                code,
                ..
            } => Some(HttpReply {
                status: http_status.unwrap_or(match code {
                    Some(FaultSide::Sender) => 400,
                    _ => 500,
                }),
                content_type: "text/plain; charset=utf-8".to_string(),
                body: reason.clone().unwrap_or_default().into_bytes(),
            }),
            _ => None,
        }
    }

    /// Whether the effect is `CloseConnection`.
    pub fn closes(&self) -> bool {
        self.effect == InjectEffect::CloseConnection
    }

    fn build_fault(&self) -> Option<(PrintFault, Option<u16>)> {
        let InjectEffect::Fault {
            code,
            subcode,
            reason,
            detail,
            http_status,
        } = &self.effect
        else {
            return None;
        };
        let sub = subcode.as_deref().and_then(|s| parse_subcode(s).ok());
        let kind = sub.as_ref().and_then(FaultKind::from_subcode);
        let code = code
            .map(FaultSide::code)
            .or_else(|| kind.map(FaultKind::code))
            .unwrap_or(FaultCode::Receiver);
        let reason = reason
            .clone()
            .or_else(|| kind.map(|k| k.default_reason().to_string()))
            .unwrap_or_else(|| "injected fault".to_string());
        let mut fault = PrintFault::custom(code, sub, reason);
        if let Some(d) = detail {
            fault
                .detail
                .push(Element::text_el(INJECT_NS, "Detail", d.as_str()));
        }
        Some((fault, *http_status))
    }
}

/// Resolve a subcode token to a QName.
pub fn parse_subcode(token: &str) -> Result<QName, String> {
    let t = token.trim();
    if t.is_empty() {
        return Err("subcode must not be empty".into());
    }
    if let Some(rest) = t.strip_prefix('{') {
        let (nsuri, local) = rest
            .split_once('}')
            .filter(|(n, l)| !n.is_empty() && !l.is_empty())
            .ok_or_else(|| format!("subcode {t:?}: expected {{namespace}}local"))?;
        return Ok(QName::new(nsuri, local));
    }
    let valid_local = |l: &str| {
        !l.is_empty()
            && l.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    };
    match t.split_once(':') {
        Some((prefix, local)) if valid_local(local) => {
            let namespace = match prefix {
                "wprt" => ns::PRINT,
                "wprt11" => ns::PRINT11,
                "wprt12" => ns::PRINT12,
                "wprt20" => ns::PRINT20,
                "wsa" => ns::WSA,
                "wse" => ns::WSE,
                "wsd" => ns::WSD,
                "soap" | "s" | "env" | "soap12" => ns::SOAP12,
                other => {
                    return Err(format!(
                        "subcode {t:?}: unknown prefix {other:?} (use wprt, wsa, wse, wsd, soap or {{namespace}}local)"
                    ))
                }
            };
            Ok(QName::new(namespace, local))
        }
        Some(_) => Err(format!("subcode {t:?} is not a QName")),
        None if valid_local(t) => Ok(FaultKind::ALL
            .iter()
            .filter_map(|k| k.subcode())
            .find(|q| q.local == t)
            .unwrap_or_else(|| QName::new(ns::PRINT, t))),
        None => Err(format!("subcode {t:?} is not a QName")),
    }
}

/// One rule as `GET /v1/faults` shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PendingRule {
    /// Position in the queue (0 = consulted first).
    pub index: usize,
    #[serde(flatten)]
    pub rule: InjectRule,
    /// Matches still to be let through.
    pub skip_remaining: u32,
    /// Effects still to be applied; `null` = forever.
    pub remaining: Option<u32>,
    /// Effects this rule has applied.
    pub hits: u64,
}

struct Slot {
    rule: InjectRule,
    skip_left: u32,
    left: Option<u32>,
    hits: u64,
}

/// The ordered, thread-safe rule table. Cheap to share (`Arc<Injector>`).
#[derive(Default)]
pub struct Injector {
    slots: Mutex<Vec<Slot>>,
    hits: AtomicU64,
    passed: AtomicU64,
}

impl Injector {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, Vec<Slot>> {
        match self.slots.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    /// How many rules are still queued.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// No rules queued.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Refuse a batch that would pass [`MAX_PENDING_RULES`]. `clearing` is a reset or
    /// `inject_clear`, which drops the queue before the new rules are added.
    pub fn check_room(&self, clearing: bool, adding: usize) -> Result<(), String> {
        let have = if clearing { 0 } else { self.len() };
        if have.saturating_add(adding) > MAX_PENDING_RULES {
            Err(format!(
                "at most {MAX_PENDING_RULES} injection rules (have {have}, adding {adding})"
            ))
        } else {
            Ok(())
        }
    }

    /// Append a rule (after checking it).
    pub fn add(&self, rule: InjectRule) -> Result<(), String> {
        rule.check()?;
        let mut slots = self.lock();
        if slots.len() >= MAX_PENDING_RULES {
            return Err(format!("at most {MAX_PENDING_RULES} injection rules"));
        }
        slots.push(Slot {
            skip_left: rule.skip,
            left: rule.count,
            hits: 0,
            rule,
        });
        Ok(())
    }

    /// Append several rules, all or none (a full queue is left unchanged).
    pub fn add_all(&self, rules: Vec<InjectRule>) -> Result<(), String> {
        for (i, r) in rules.iter().enumerate() {
            r.check().map_err(|e| format!("inject[{i}]: {e}"))?;
        }
        let mut slots = self.lock();
        if slots.len().saturating_add(rules.len()) > MAX_PENDING_RULES {
            return Err(format!("at most {MAX_PENDING_RULES} injection rules"));
        }
        for r in rules {
            slots.push(Slot {
                skip_left: r.skip,
                left: r.count,
                hits: 0,
                rule: r,
            });
        }
        Ok(())
    }

    /// Drop every rule (the hit counters stay).
    pub fn clear(&self) {
        self.lock().clear();
    }

    /// Drop every rule and zero the counters.
    pub fn reset(&self) {
        self.lock().clear();
        self.hits.store(0, Ordering::Relaxed);
        self.passed.store(0, Ordering::Relaxed);
    }

    /// Rules still in the queue, in consultation order.
    pub fn pending(&self) -> Vec<PendingRule> {
        self.lock()
            .iter()
            .enumerate()
            .map(|(index, s)| PendingRule {
                index,
                rule: s.rule.clone(),
                skip_remaining: s.skip_left,
                remaining: s.left,
                hits: s.hits,
            })
            .collect()
    }

    /// Effects applied so far (a `Pass` or delay-only rule counts too).
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Matching requests let through by `skip` so far.
    pub fn skipped(&self) -> u64 {
        self.passed.load(Ordering::Relaxed)
    }

    /// Decide what happens to one request; consumes rule state. `None`: dispatch normally.
    pub fn take(&self, q: &InjectQuery<'_>) -> Option<Injection> {
        let mut slots = self.lock();
        let i = slots.iter().position(|s| s.rule.matches(q))?;
        let slot = &mut slots[i];
        if slot.skip_left > 0 {
            slot.skip_left -= 1;
            self.passed.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        slot.hits += 1;
        self.hits.fetch_add(1, Ordering::Relaxed);
        let kind = match &slot.rule.effect {
            InjectEffect::Pass => "pass",
            InjectEffect::Fault { .. } => "fault",
            InjectEffect::Http { .. } => "http",
            InjectEffect::IppStatus { .. } => "ipp_status",
            InjectEffect::CloseConnection => "close_connection",
        };
        let out = Injection {
            rule: format!("{}[{i}]:{kind}", slot.rule.op.as_deref().unwrap_or("*")),
            delay: slot.rule.delay_ms.map(Duration::from_millis),
            delay_after: slot.rule.delay_after,
            effect: slot.rule.effect.clone(),
        };
        if let Some(left) = slot.left.as_mut() {
            *left = left.saturating_sub(1);
            if *left == 0 {
                slots.remove(i);
            }
        }
        Some(out)
    }
}

/// The JSON `GET /v1/faults` reports under `"inject"`.
pub fn inject_json(inj: &Injector) -> serde_json::Value {
    serde_json::json!({
        "pending": inj.pending(),
        "hits": inj.hits(),
        "skipped": inj.skipped(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const NS_P: &str = "http://schemas.microsoft.com/windows/2006/08/wdp/print";

    fn q<'a>(action: &'a str) -> InjectQuery<'a> {
        InjectQuery {
            endpoint: RuleEndpoint::PrintService,
            action: Some(action),
        }
    }

    fn act(op: &str) -> String {
        format!("{NS_P}/{op}")
    }

    #[test]
    fn op_matches_short_name_full_uri_and_legacy_base() {
        let a = act("SendDocument");
        assert!(op_matches("SendDocument", &a));
        assert!(op_matches("senddocument", &a));
        assert!(op_matches(&a, &a));
        assert!(!op_matches("CreatePrintJob", &a));
        assert!(!op_matches("Send", &a));
        let legacy = "http://schemas.microsoft.com/windows/2006/01/wdp/print/SendDocument";
        assert!(op_matches("SendDocument", legacy));
        assert!(op_matches(&a, legacy));
    }

    #[test]
    fn default_rule_fires_once() {
        let inj = Injector::new();
        inj.add(InjectRule::fault(FaultKind::ServerErrorTemporaryError))
            .unwrap();
        assert!(inj.take(&q(&act("GetActiveJobs"))).is_some());
        assert!(inj.take(&q(&act("GetActiveJobs"))).is_none());
        assert!(inj.pending().is_empty());
        assert_eq!(inj.hits(), 1);
    }

    #[test]
    fn skip_then_count_then_exhausted() {
        let inj = Injector::new();
        inj.add(
            InjectRule::http(503, "x")
                .for_op("SendDocument")
                .skipping(2)
                .times(2),
        )
        .unwrap();
        let send = act("SendDocument");
        let create = act("CreatePrintJob");
        assert!(inj.take(&q(&create)).is_none(), "other ops never count");
        assert!(inj.take(&q(&send)).is_none());
        assert!(inj.take(&q(&send)).is_none());
        let p = inj.pending();
        assert_eq!((p[0].skip_remaining, p[0].remaining), (0, Some(2)));
        assert!(inj.take(&q(&send)).is_some());
        assert!(inj.take(&q(&send)).is_some());
        assert!(inj.take(&q(&send)).is_none());
        assert!(inj.pending().is_empty());
        assert_eq!((inj.hits(), inj.skipped()), (2, 2));
    }

    #[test]
    fn forever_never_runs_out() {
        let inj = Injector::new();
        inj.add(InjectRule::close_connection().forever()).unwrap();
        for _ in 0..50 {
            assert!(inj.take(&q(&act("GetJobHistory"))).is_some());
        }
        assert_eq!(inj.pending()[0].remaining, None);
        assert_eq!(inj.pending()[0].hits, 50);
    }

    #[test]
    fn the_first_matching_rule_decides_and_rules_queue_in_order() {
        let inj = Injector::new();
        inj.add(InjectRule::http(500, "a").for_op("SendDocument"))
            .unwrap();
        inj.add(InjectRule::http(502, "b").for_op("SendDocument"))
            .unwrap();
        inj.add(InjectRule::http(504, "c")).unwrap();
        let status = |i: Option<Injection>| i.and_then(|i| i.http_reply()).map(|r| r.status);
        let s = act("SendDocument");
        assert_eq!(status(inj.take(&q(&s))), Some(500));
        assert_eq!(status(inj.take(&q(&s))), Some(502));
        assert_eq!(
            status(inj.take(&q(&s))),
            Some(504),
            "falls to the any-op rule"
        );
        assert_eq!(status(inj.take(&q(&s))), None);
    }

    #[test]
    fn a_skipping_rule_shields_later_rules() {
        let inj = Injector::new();
        inj.add(InjectRule::http(500, "a").skipping(1)).unwrap();
        inj.add(InjectRule::http(502, "b")).unwrap();
        assert!(inj.take(&q(&act("X"))).is_none());
        assert_eq!(
            inj.take(&q(&act("X")))
                .and_then(|i| i.http_reply())
                .map(|r| r.status),
            Some(500)
        );
        assert_eq!(
            inj.take(&q(&act("X")))
                .and_then(|i| i.http_reply())
                .map(|r| r.status),
            Some(502)
        );
    }

    #[test]
    fn endpoint_filter_and_ipp_isolation() {
        let inj = Injector::new();
        inj.add(
            InjectRule::http(500, "d")
                .on(RuleEndpoint::Device)
                .forever(),
        )
        .unwrap();
        assert!(inj.take(&q(&act("GetPrinterElements"))).is_none());
        let dev = InjectQuery {
            endpoint: RuleEndpoint::Device,
            action: Some("x"),
        };
        assert!(inj.take(&dev).is_some());
        // an any-endpoint rule never fires on IPP, an ipp rule never on WSD
        let inj = Injector::new();
        inj.add(InjectRule::http(500, "").forever()).unwrap();
        let ipp = InjectQuery {
            endpoint: RuleEndpoint::Ipp,
            action: None,
        };
        assert!(inj.take(&ipp).is_none());
        inj.add(InjectRule::http(503, "").on(RuleEndpoint::Ipp))
            .unwrap();
        assert!(inj.take(&ipp).is_some());
        assert!(inj.take(&q(&act("X"))).is_some());
        // an op filter cannot match where there is no action
        let inj = Injector::new();
        inj.add(
            InjectRule::http(500, "")
                .on(RuleEndpoint::Ipp)
                .for_op("Print-Job"),
        )
        .unwrap();
        assert!(inj.take(&ipp).is_none());
    }

    #[test]
    fn json_round_trip_and_defaults() {
        let r: InjectRule = serde_json::from_str("{}").unwrap();
        assert_eq!(r, InjectRule::default());
        assert_eq!(r.count, Some(1));
        let r: InjectRule = serde_json::from_str(r#"{"count":null,"op":"X"}"#).unwrap();
        assert_eq!(r.count, None);
        let r: InjectRule = serde_json::from_str(
            r#"{"endpoint":"service","skip":2,"delay_ms":5,"delay_after":true,
                "effect":{"type":"fault","code":"sender","subcode":"InvalidArgs"}}"#,
        )
        .unwrap();
        assert_eq!(r.endpoint, Some(RuleEndpoint::PrintService));
        let back: InjectRule = serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(back, r);
        assert!(serde_json::from_str::<InjectRule>(r#"{"nope":1}"#).is_err());
        assert!(serde_json::from_str::<InjectRule>(r#"{"effect":{"type":"explode"}}"#).is_err());
    }

    #[test]
    fn ipp_status_effect_parses_checks_and_matches_ipp_operation_names() {
        let r: InjectRule = serde_json::from_str(
            r#"{"endpoint":"ipp","op":"Print-Job","effect":{"type":"ipp_status","status":"server-error-busy","message":"later"}}"#,
        )
        .unwrap();
        r.check().unwrap();
        match &r.effect {
            InjectEffect::IppStatus { status, message } => {
                assert_eq!(status.code(), Some(0x0507));
                assert_eq!(message.as_deref(), Some("later"));
            }
            other => panic!("{other:?}"),
        }
        let back: InjectRule = serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(back, r);
        let n: InjectRule = serde_json::from_str(
            r#"{"endpoint":"ipp","effect":{"type":"ipp_status","status":1030}}"#,
        )
        .unwrap();
        n.check().unwrap();
        // a rule that is not on the IPP endpoint, or names no status, is refused
        for bad in [
            r#"{"effect":{"type":"ipp_status","status":1030}}"#,
            r#"{"endpoint":"device","effect":{"type":"ipp_status","status":1030}}"#,
            r#"{"endpoint":"ipp","effect":{"type":"ipp_status","status":"nope"}}"#,
        ] {
            let r: InjectRule = serde_json::from_str(bad).unwrap();
            assert!(r.check().is_err(), "{bad}");
        }
        // matching: spelling-insensitive, IPP endpoint only
        let inj = Injector::new();
        inj.add(r.clone().forever()).unwrap();
        let q = |a| InjectQuery {
            endpoint: RuleEndpoint::Ipp,
            action: a,
        };
        for spelling in ["print_job", "Print-Job", "PrintJob"] {
            assert!(inj.take(&q(Some(spelling))).is_some(), "{spelling}");
        }
        assert!(inj.take(&q(Some("get_jobs"))).is_none());
        assert!(inj.take(&q(None)).is_none());
        assert!(inj
            .take(&InjectQuery {
                endpoint: RuleEndpoint::PrintService,
                action: Some("print_job")
            })
            .is_none());
        assert!(ipp_op_matches("get-job-attributes", "GetJobAttributes"));
    }

    #[test]
    fn bad_rules_are_refused() {
        let inj = Injector::new();
        for bad in [
            InjectRule::http(99, ""),
            InjectRule::http(600, ""),
            InjectRule::delay(MAX_DELAY_MS + 1),
            InjectRule::default().times(0),
            InjectRule::default().for_op(" "),
            InjectRule::new(InjectEffect::Fault {
                code: None,
                subcode: Some("zzz:Bad".into()),
                reason: None,
                detail: None,
                http_status: None,
            }),
            InjectRule::new(InjectEffect::Fault {
                code: None,
                subcode: None,
                reason: None,
                detail: None,
                http_status: Some(42),
            }),
        ] {
            assert!(inj.add(bad.clone()).is_err(), "{bad:?}");
        }
        assert!(inj.pending().is_empty());
        assert!(inj
            .add_all(vec![InjectRule::default(), InjectRule::http(1, "")])
            .is_err());
        assert!(inj.pending().is_empty(), "all or none");
    }

    #[test]
    fn the_queue_refuses_past_the_cap_without_keeping_a_partial_batch() {
        let inj = Injector::new();
        let batch = vec![InjectRule::default(); MAX_PENDING_RULES];
        inj.add_all(batch).unwrap();
        assert_eq!(inj.len(), MAX_PENDING_RULES);
        assert!(inj.check_room(false, 1).is_err());
        assert!(inj.add(InjectRule::default()).is_err());
        assert_eq!(
            inj.len(),
            MAX_PENDING_RULES,
            "a refused add leaves the queue"
        );
        let err = inj.add_all(vec![InjectRule::default(); 2]);
        assert!(err.is_err());
        assert_eq!(inj.len(), MAX_PENDING_RULES, "a refused batch adds nothing");
        assert!(inj.check_room(true, MAX_PENDING_RULES).is_ok());
        assert!(inj.check_room(true, MAX_PENDING_RULES + 1).is_err());
    }

    #[test]
    fn subcode_tokens() {
        assert_eq!(
            parse_subcode("wprt:ServerErrorTemporaryError").unwrap(),
            QName::new(ns::PRINT, "ServerErrorTemporaryError")
        );
        assert_eq!(
            parse_subcode("InvalidArgs").unwrap(),
            QName::new(ns::PRINT, "InvalidArgs")
        );
        assert_eq!(
            parse_subcode("ActionNotSupported").unwrap(),
            QName::new(ns::WSA, "ActionNotSupported")
        );
        assert_eq!(
            parse_subcode("{urn:x}Odd").unwrap(),
            QName::new("urn:x", "Odd")
        );
        assert!(parse_subcode("").is_err());
        assert!(parse_subcode("a:b:c").is_err());
        assert!(parse_subcode("{}x").is_err());
    }

    #[test]
    fn concurrent_requests_consume_each_rule_exactly_once() {
        use std::sync::Arc;
        let inj = Arc::new(Injector::new());
        inj.add(InjectRule::http(500, "").times(40)).unwrap();
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let inj = Arc::clone(&inj);
                std::thread::spawn(move || {
                    (0..20).filter(|_| inj.take(&q("urn:a")).is_some()).count()
                })
            })
            .collect();
        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total, 40);
        assert_eq!(inj.hits(), 40);
        assert!(inj.pending().is_empty());
    }
}
