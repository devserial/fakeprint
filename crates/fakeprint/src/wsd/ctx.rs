//! Shared context of the print-service handlers.
//!
//! [`PrintCtx`] bundles what every WS-Print operation needs: the printer model (answers
//! `GetPrinterElements`), the job table, the event sink, a clock and the configuration
//! (endpoint URLs, limits, timers, version flags, vendor quirks). Handlers in `ops.rs` are
//! plain functions of `(&PrintCtx, &SoapRequest) -> SoapReply`, so they are testable without
//! HTTP.
//!
//! The pieces that other packages plug in:
//!
//! * the printer model (`printer_model::ProfilePrinterModel`) implements [`PrinterModel`] and may read live state from a
//!   [`StatusSource`] (the job engine implements it);
//! * the event manager (`eventing::EventManager`) implements [`EventBus`]; install it with
//!   [`PrintCtx::set_event_sink`] (the context hands the engine a forwarding bus, so the sink
//!   can be replaced after the context exists);
//! * the lab control surface sets conditions and flags through [`FaultPlan`] /
//!   [`FaultBody`].

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Deserialize;

use super::inject::{InjectRule, Injector, PendingRule};
use super::jobs::JobEngine;
use super::types::{
    DeviceCondition, EndpointKind, EventBus, JobStore, NullEventBus, PrintEvent, PrintVersion,
    PrinterModel, PrinterState,
};
use super::validate::{requested_by_environment, ReplyValidator};
use crate::state::Inbox;

// ---------------------------------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------------------------------

/// Source of time. Everything time-driven (document timeout, processing progress, history age)
/// reads this and nothing else, so tests advance a [`ManualClock`] instead of sleeping.
pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTime;
}

/// The real clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// A clock that only moves when told to. Starts at 2026-01-01T00:00:00Z.
#[derive(Debug)]
pub struct ManualClock {
    base: SystemTime,
    elapsed_ms: AtomicU64,
}

impl ManualClock {
    pub fn new() -> Self {
        Self {
            base: UNIX_EPOCH + Duration::from_secs(1_767_225_600),
            elapsed_ms: AtomicU64::new(0),
        }
    }

    /// Move time forward.
    pub fn advance(&self, by: Duration) {
        let ms = u64::try_from(by.as_millis()).unwrap_or(u64::MAX);
        self.elapsed_ms.fetch_add(ms.min(1 << 40), Ordering::SeqCst);
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for ManualClock {
    fn now(&self) -> SystemTime {
        self.base + Duration::from_millis(self.elapsed_ms.load(Ordering::SeqCst))
    }
}

/// `xs:dateTime` text (UTC, whole seconds) for a time.
pub fn format_datetime(t: SystemTime) -> String {
    let secs = match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_secs()).unwrap_or(i64::MAX / 2),
        Err(e) => -i64::try_from(e.duration().as_secs()).unwrap_or(i64::MAX / 2),
    };
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // civil-from-days (proleptic Gregorian)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

// ---------------------------------------------------------------------------------------------
// Event bus indirection
// ---------------------------------------------------------------------------------------------

/// An [`EventBus`] whose target can be replaced at run time. The job engine publishes into
/// one of these; the server installs its subscription manager with [`SharedBus::set`].
pub struct SharedBus {
    inner: RwLock<Arc<dyn EventBus>>,
}

impl SharedBus {
    pub fn new(target: Arc<dyn EventBus>) -> Self {
        Self {
            inner: RwLock::new(target),
        }
    }

    pub fn set(&self, target: Arc<dyn EventBus>) {
        match self.inner.write() {
            Ok(mut g) => *g = target,
            Err(p) => *p.into_inner() = target,
        }
    }
}

impl Default for SharedBus {
    fn default() -> Self {
        Self::new(Arc::new(NullEventBus))
    }
}

impl EventBus for SharedBus {
    fn publish(&self, event: PrintEvent) {
        let target = match self.inner.read() {
            Ok(g) => Arc::clone(&g),
            Err(p) => Arc::clone(&p.into_inner()),
        };
        target.publish(event);
    }
}

// ---------------------------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------------------------

/// Hard bounds. Every collection and size the handlers touch has one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrintLimits {
    /// Largest accepted document (bytes). Larger => `InvalidArgs`.
    pub max_document: usize,
    /// Largest accepted HTTP request body (a document plus its envelope; hard cap).
    pub max_body: usize,
    /// Documents per job (only relevant when multi-document jobs are on).
    pub max_documents_per_job: u32,
    /// Non-terminal jobs at once; one more => `ServerErrorNotAcceptingJobs`.
    pub max_active_jobs: usize,
    /// Terminal jobs remembered for `GetJobHistory` / `GetJobElements`.
    pub history_cap: usize,
    /// Longest `JobName` / `JobOriginatingUserName` / `DocumentName` (schema: 255).
    pub max_name: usize,
    /// Elements per `RequestedElements` list.
    pub max_requested_names: usize,
    /// Conditions kept in the condition history.
    pub max_condition_history: usize,
}

impl Default for PrintLimits {
    fn default() -> Self {
        Self {
            max_document: 64 << 20,
            max_body: 64 << 20,
            max_documents_per_job: 64,
            max_active_jobs: 64,
            history_cap: 100,
            max_name: 255,
            max_requested_names: 64,
            max_condition_history: 64,
        }
    }
}

/// Timers of the job life cycle. All driven by the [`Clock`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrintTiming {
    /// No document within this long after `CreatePrintJob` (or after the previous, non-final
    /// document finished): the job aborts with `DocumentTimeoutError`. Spec: 60 s.
    pub document_timeout: Duration,
    /// Time a cancelled job spends in `Terminating` before `Canceled`.
    pub terminating: Duration,
    /// Fixed part of the time a document takes to print.
    pub processing_base: Duration,
    /// Extra printing time per MiB of document.
    pub processing_per_mib: Duration,
    /// How long a received document keeps the job "document active": another
    /// `SendDocument` in that window gets `ServerErrorDocumentActive`.
    pub document_busy: Duration,
    /// Bytes that make up one printed sheet in the simulation.
    pub bytes_per_sheet: u64,
    /// IPP `multiple-operation-time-out`: a job created with Create-Job aborts when the next
    /// Send-Document does not arrive within this long (advertised to IPP clients in seconds).
    pub ipp_multiple_operation_timeout: Duration,
    /// How long an IPP client can still see a finished job (Get-Jobs `completed`,
    /// Get-Job-Attributes); afterwards IPP answers `client-error-not-found`. The WSD side keeps
    /// the job until the history cap pushes it out.
    pub ipp_history_retention: Duration,
}

impl Default for PrintTiming {
    fn default() -> Self {
        Self {
            document_timeout: Duration::from_secs(60),
            terminating: Duration::ZERO,
            processing_base: Duration::from_millis(500),
            processing_per_mib: Duration::from_millis(500),
            document_busy: Duration::ZERO,
            bytes_per_sheet: 32 * 1024,
            ipp_multiple_operation_timeout: Duration::from_secs(120),
            ipp_history_retention: Duration::from_secs(300),
        }
    }
}

impl PrintTiming {
    /// Jobs take no processing time: a document is "printed" on the next tick, a cancel ends
    /// at once. The document timeout stays at the spec's 60 s.
    pub fn instant() -> Self {
        Self {
            processing_base: Duration::ZERO,
            processing_per_mib: Duration::ZERO,
            terminating: Duration::ZERO,
            document_busy: Duration::ZERO,
            ..Self::default()
        }
    }

    /// Jobs that take visible time: two seconds plus one second per MiB of document, half a
    /// second in `Terminating` after a cancel.
    pub fn realistic() -> Self {
        Self {
            processing_base: Duration::from_secs(2),
            processing_per_mib: Duration::from_secs(1),
            terminating: Duration::from_millis(500),
            ..Self::default()
        }
    }

    /// Apply a runtime change (`POST /v1/timing`): the preset first, then single fields.
    pub fn apply(&mut self, patch: &TimingPatch) -> Result<(), String> {
        const MAX_MS: u64 = 24 * 3_600_000;
        let mut next = self.clone();
        match patch.preset.as_deref().map(str::trim) {
            None => {}
            Some(p) if p.eq_ignore_ascii_case("instant") => next = Self::instant(),
            Some(p) if p.eq_ignore_ascii_case("realistic") => next = Self::realistic(),
            Some(p) if p.eq_ignore_ascii_case("default") => next = Self::default(),
            Some(other) => {
                return Err(format!(
                    "unknown preset {other:?}; known: instant realistic default"
                ))
            }
        }
        for (name, v, slot) in [
            (
                "processing_base_ms",
                patch.processing_base_ms,
                &mut next.processing_base,
            ),
            (
                "processing_per_mib_ms",
                patch.processing_per_mib_ms,
                &mut next.processing_per_mib,
            ),
            (
                "document_timeout_ms",
                patch.document_timeout_ms,
                &mut next.document_timeout,
            ),
            (
                "terminating_ms",
                patch.terminating_ms,
                &mut next.terminating,
            ),
            (
                "document_busy_ms",
                patch.document_busy_ms,
                &mut next.document_busy,
            ),
        ] {
            if let Some(ms) = v {
                if ms > MAX_MS {
                    return Err(format!("{name} is at most {MAX_MS}"));
                }
                *slot = Duration::from_millis(ms);
            }
        }
        for (name, v, slot) in [
            (
                "ipp_multiple_operation_timeout_ms",
                patch.ipp_multiple_operation_timeout_ms,
                &mut next.ipp_multiple_operation_timeout,
            ),
            (
                "ipp_history_retention_ms",
                patch.ipp_history_retention_ms,
                &mut next.ipp_history_retention,
            ),
        ] {
            if let Some(ms) = v {
                if ms > MAX_MS {
                    return Err(format!("{name} is at most {MAX_MS}"));
                }
                *slot = Duration::from_millis(ms);
            }
        }
        if next.ipp_multiple_operation_timeout.is_zero() {
            return Err("ipp_multiple_operation_timeout_ms must be 1 or more".into());
        }
        if let Some(b) = patch.bytes_per_sheet {
            if b == 0 {
                return Err("bytes_per_sheet must be 1 or more".into());
            }
            next.bytes_per_sheet = b;
        }
        if next.document_timeout.is_zero() {
            return Err("document_timeout_ms must be 1 or more".into());
        }
        *self = next;
        Ok(())
    }

    /// The settings as the control plane reports them.
    pub fn to_json(&self) -> serde_json::Value {
        let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
        serde_json::json!({
            "processing_base_ms": ms(self.processing_base),
            "processing_per_mib_ms": ms(self.processing_per_mib),
            "document_timeout_ms": ms(self.document_timeout),
            "terminating_ms": ms(self.terminating),
            "document_busy_ms": ms(self.document_busy),
            "bytes_per_sheet": self.bytes_per_sheet,
            "ipp_multiple_operation_timeout_ms": ms(self.ipp_multiple_operation_timeout),
            "ipp_history_retention_ms": ms(self.ipp_history_retention),
        })
    }
}

/// Body of `POST /v1/timing` (and `ctl timing`): a preset and/or single fields, milliseconds.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimingPatch {
    /// `instant`, `realistic` or `default`, applied before the fields.
    pub preset: Option<String>,
    pub processing_base_ms: Option<u64>,
    pub processing_per_mib_ms: Option<u64>,
    pub document_timeout_ms: Option<u64>,
    pub terminating_ms: Option<u64>,
    pub document_busy_ms: Option<u64>,
    pub bytes_per_sheet: Option<u64>,
    pub ipp_multiple_operation_timeout_ms: Option<u64>,
    pub ipp_history_retention_ms: Option<u64>,
}

/// Vendor behaviour that is NOT part of the schema. Every flag defaults to off.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Quirks {
    /// Add the lab-only `JobToken` element to `CreatePrintJobResponse` (a scan-style
    /// correlator the print schema does not have).
    pub job_token: bool,
    /// Accept a `PrintTicket` without `JobDescription` and a `JobDescription` without
    /// `JobOriginatingUserName` (the schema requires both).
    pub lenient_job_description: bool,
    /// Accept `SendDocument` without `DocumentDescription` (format taken from the ticket or
    /// `application/octet-stream`, compression `None`).
    pub lenient_document_description: bool,
}

/// Which protocol extensions the printer offers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VersionFlags {
    /// `SetPrinterElements` (v1.2).
    pub v12: bool,
    /// `CreatePrintJob2` and friends (v2.0). Their action URIs are not in the package, so
    /// nothing can be dispatched yet; the flag only decides the fault reason.
    pub v20: bool,
}

impl VersionFlags {
    pub fn allows(self, v: PrintVersion) -> bool {
        match v {
            PrintVersion::V10 => true,
            PrintVersion::V12 => self.v12,
            PrintVersion::V20 => self.v20,
        }
    }
}

/// One pair of ticket values that cannot be honoured together (both `MustHonor=true` =>
/// `ClientErrorConflictingRequiredParameters`). `path` is the leaf name, for example
/// `MediaType`; `value` the token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictRule {
    pub a: (String, String),
    pub b: (String, String),
}

/// Everything configurable about the print service.
#[derive(Debug, Clone)]
pub struct PrintConfig {
    /// `http://host:port/WebServices/Device`
    pub device_endpoint: String,
    /// `http://host:port/WebServices/PrinterService`
    pub service_endpoint: String,
    pub limits: PrintLimits,
    pub timing: PrintTiming,
    pub quirks: Quirks,
    pub versions: VersionFlags,
    /// `PrinterDescription/MultipleDocumentJobsSupported`.
    pub multiple_document_jobs: bool,
    /// AddDocument is the one optional operation; off => `ServerErrorAddDocumentNotSupported`.
    pub add_document: bool,
    /// Ticket-value conflicts to detect.
    pub conflict_rules: Vec<ConflictRule>,
}

impl Default for PrintConfig {
    fn default() -> Self {
        Self {
            device_endpoint: "http://127.0.0.1:3921/WebServices/Device".to_string(),
            service_endpoint: "http://127.0.0.1:3921/WebServices/PrinterService".to_string(),
            limits: PrintLimits::default(),
            timing: PrintTiming::default(),
            quirks: Quirks::default(),
            versions: VersionFlags::default(),
            multiple_document_jobs: true,
            add_document: false,
            conflict_rules: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Fault plan and live status
// ---------------------------------------------------------------------------------------------

/// Print-side conditions the lab can raise. Each maps to a `DeviceCondition`, a
/// `PrinterStateReason` and (for the critical ones) stops the printer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PrintCondition {
    PaperOut,
    PaperLow,
    TonerLow,
    TonerEmpty,
    CoverOpen,
    PaperJam,
    OutputFull,
    /// Generic attention (`Unknown` condition, `AttentionRequired` reason).
    Error,
}

impl PrintCondition {
    pub const ALL: [PrintCondition; 8] = [
        PrintCondition::PaperOut,
        PrintCondition::PaperLow,
        PrintCondition::TonerLow,
        PrintCondition::TonerEmpty,
        PrintCondition::CoverOpen,
        PrintCondition::PaperJam,
        PrintCondition::OutputFull,
        PrintCondition::Error,
    ];

    /// Stable `DeviceCondition/@Id` (>= 1).
    pub fn id(self) -> u32 {
        match self {
            PrintCondition::PaperOut => 1,
            PrintCondition::PaperLow => 2,
            PrintCondition::TonerLow => 3,
            PrintCondition::TonerEmpty => 4,
            PrintCondition::CoverOpen => 5,
            PrintCondition::PaperJam => 6,
            PrintCondition::OutputFull => 7,
            PrintCondition::Error => 8,
        }
    }

    /// `ConditionNameType` token.
    pub fn name(self) -> &'static str {
        match self {
            PrintCondition::PaperOut => "InputTraySupplyEmpty",
            PrintCondition::PaperLow => "InputTraySupplyLow",
            PrintCondition::TonerLow => "ConsumableLow",
            PrintCondition::TonerEmpty => "ConsumableEmpty",
            PrintCondition::CoverOpen => "CoverOpen",
            PrintCondition::PaperJam => "Jam",
            PrintCondition::OutputFull => "OutputTrayFull",
            PrintCondition::Error => "Unknown",
        }
    }

    /// Component group (`ComponentGroupType`-style word) the condition is about.
    pub fn component(self) -> &'static str {
        match self {
            PrintCondition::PaperOut | PrintCondition::PaperLow => "InputTray",
            PrintCondition::TonerLow | PrintCondition::TonerEmpty => "Consumable",
            PrintCondition::CoverOpen => "Cover",
            PrintCondition::PaperJam => "MediaPath",
            PrintCondition::OutputFull => "OutputTray",
            PrintCondition::Error => "Printer",
        }
    }

    pub fn severity(self) -> super::types::Severity {
        use super::types::Severity::*;
        match self {
            PrintCondition::PaperLow | PrintCondition::TonerLow => Warning,
            _ => Critical,
        }
    }

    /// `PrinterStateReasonType` token.
    pub fn state_reason(self) -> &'static str {
        match self {
            PrintCondition::PaperOut => "MediaEmpty",
            PrintCondition::PaperLow => "MediaLow",
            PrintCondition::TonerLow => "MarkerSupplyLow",
            PrintCondition::TonerEmpty => "MarkerSupplyEmpty",
            PrintCondition::CoverOpen => "DoorOpen",
            PrintCondition::PaperJam => "MediaJam",
            PrintCondition::OutputFull => "OutputAreaFull",
            PrintCondition::Error => "AttentionRequired",
        }
    }

    /// Whether the condition stops the printer (jobs in progress go to `ProcessingStopped`).
    pub fn stops_printer(self) -> bool {
        self.severity() == super::types::Severity::Critical
    }

    pub fn from_token(s: &str) -> Option<PrintCondition> {
        let s = s.trim();
        Self::ALL
            .iter()
            .copied()
            .find(|c| format!("{c:?}").eq_ignore_ascii_case(s) || c.name() == s)
    }
}

/// What the lab has switched on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FaultPlan {
    /// Raised conditions.
    pub conditions: BTreeSet<PrintCondition>,
    /// `CreatePrintJob` answers `ServerErrorNotAcceptingJobs` (service mode).
    pub not_accepting_jobs: bool,
    /// Create/Send answer `ServerErrorTemporaryError`.
    pub temporary_error: bool,
    /// Create/Send answer `ServerErrorInternalError`.
    pub internal_error: bool,
    /// `SendDocument` answers `ServerErrorDocumentActive`.
    pub document_active: bool,
    /// Accept the next document, then abort the job with this failure.
    pub fail_next_document: Option<DocumentFailure>,
    /// Conditions raised by name (profile start-up conditions, lab `raise` requests). They
    /// complement the eight fixed [`PrintCondition`]s and share their effects.
    pub extra: Vec<ExtraCondition>,
}

/// A raised condition with an arbitrary name, component and severity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtraCondition {
    /// `DeviceCondition/@Id` (>= 100, never reused while the engine lives).
    pub id: u32,
    /// `ConditionNameType` token.
    pub name: String,
    /// `Group` or `Group/Name`.
    pub component: String,
    pub severity: super::types::Severity,
    /// `PrinterStateReasonType` token the condition shows as.
    pub reason: String,
}

/// How an accepted document goes wrong while printing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum DocumentFailure {
    /// Job aborts with `DocumentFormatError`.
    Format,
    /// Job aborts with `CompressionError`.
    Compression,
    /// Job aborts with `DocumentTransferError`.
    Transfer,
}

impl DocumentFailure {
    /// `Format`, `Compression`, `Transfer` (any case) => `Some`; `None`/empty => no failure.
    pub fn parse(s: &str) -> Result<Option<DocumentFailure>, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "format" => Ok(Some(DocumentFailure::Format)),
            "compression" => Ok(Some(DocumentFailure::Compression)),
            "transfer" => Ok(Some(DocumentFailure::Transfer)),
            "none" | "" => Ok(None),
            _ => Err(format!(
                "fail_next_document: unknown value {s:?}; use Format, Compression, Transfer or None"
            )),
        }
    }
}

/// One condition a lab request raises by name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct RaiseSpec {
    /// `ConditionNameType` token (`InputTraySupplyEmpty`, `CoverOpen`, `Jam`, ...).
    pub name: String,
    /// `Group` or `Group/Name`; absent: derived from the name.
    #[serde(default)]
    pub component: Option<String>,
    /// `Informational`, `Warning` or `Critical`; absent: the usual severity of the name.
    #[serde(default)]
    pub severity: Option<String>,
}

/// JSON body of a lab "set faults" call (unset keys leave the prior value).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct FaultBody {
    pub paper_out: Option<bool>,
    pub paper_low: Option<bool>,
    pub toner_low: Option<bool>,
    pub toner_empty: Option<bool>,
    pub cover_open: Option<bool>,
    #[serde(alias = "jam")]
    pub paper_jam: Option<bool>,
    pub output_full: Option<bool>,
    pub error: Option<bool>,
    pub not_accepting_jobs: Option<bool>,
    pub temporary_error: Option<bool>,
    pub internal_error: Option<bool>,
    pub document_active: Option<bool>,
    /// `"Format"`, `"Compression"`, `"Transfer"`; `"None"` clears.
    pub fail_next_document: Option<String>,
    /// Clear everything first.
    #[serde(default)]
    pub reset: bool,
    /// Raise conditions by name (any `ConditionNameType`, any component and severity).
    #[serde(default)]
    pub raise: Vec<RaiseSpec>,
    /// Clear every active condition with one of these names (fixed or raised by name).
    #[serde(default)]
    pub clear: Vec<String>,
    /// Append request-injection rules (`wsd::inject`); they are applied by the HTTP layer,
    /// not by the job engine.
    #[serde(default)]
    pub inject: Vec<InjectRule>,
    /// Drop every pending injection rule first (before `inject` is added).
    #[serde(default)]
    pub inject_clear: bool,
}

impl FaultBody {
    /// Reject values the engine would otherwise ignore silently.
    pub fn check(&self) -> Result<(), String> {
        if let Some(v) = &self.fail_next_document {
            DocumentFailure::parse(v)?;
        }
        for (i, r) in self.inject.iter().enumerate() {
            r.check().map_err(|e| format!("inject[{i}]: {e}"))?;
        }
        Ok(())
    }
}

/// One entry of the condition history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConditionRecord {
    pub condition: DeviceCondition,
    /// `xs:dateTime` the condition cleared, `None` while it is active.
    pub cleared: Option<String>,
}

/// Live printer status, for `PrinterStatus` and the status events.
#[derive(Debug, Clone, PartialEq)]
pub struct PrinterStatusSnapshot {
    /// `PrinterCurrentTime`, `xs:dateTime`.
    pub current_time: String,
    pub state: PrinterState,
    /// `PrimaryStateReason`, `None` when nothing is wrong.
    pub primary_reason: Option<String>,
    /// `StateReasons` (never empty: `["None"]` when idle and healthy).
    pub reasons: Vec<String>,
    /// `QueuedJobCount`: non-terminal jobs.
    pub queued_jobs: u32,
    pub active_conditions: Vec<DeviceCondition>,
    /// Oldest first, bounded.
    pub condition_history: Vec<ConditionRecord>,
    pub accepting_jobs: bool,
}

/// Live state the printer model reads when it builds `PrinterStatus`.
pub trait StatusSource: Send + Sync {
    fn status(&self) -> PrinterStatusSnapshot;
}

// ---------------------------------------------------------------------------------------------
// The context
// ---------------------------------------------------------------------------------------------

/// Everything the print-service handlers use. Cheap to clone-by-reference (all `Arc`s).
pub struct PrintCtx {
    /// Answers `GetPrinterElements` sections.
    pub model: Arc<dyn PrinterModel>,
    /// The job table (the engine, seen through the shared trait).
    pub jobs: Arc<dyn JobStore>,
    /// Where job and printer events go (a forwarding bus: see [`PrintCtx::set_event_sink`]).
    pub events: Arc<dyn EventBus>,
    pub clock: Arc<dyn Clock>,
    pub config: PrintConfig,
    /// The concrete engine behind `jobs`: life cycle, timers, faults, status.
    pub engine: Arc<JobEngine>,
    /// Where accepted documents are written; `None` counts sizes and drops the bytes.
    pub inbox: Option<Arc<Inbox>>,
    /// Checks outgoing bodies against the schema when switched on (see `wsd::validate`).
    pub validator: Arc<ReplyValidator>,
    /// Test-time fault injection rules, applied by the HTTP layer before dispatch.
    pub inject: Arc<Injector>,
    /// The IPP face's start time and reply validator.
    pub ipp: Arc<crate::ipp::IppRuntime>,
    /// The raw request log of the device (see `reqlog`).
    pub reqlog: Arc<crate::reqlog::RequestLog>,
    bus: Arc<SharedBus>,
}

impl PrintCtx {
    /// Build a context around an engine (create it first with [`JobEngine::new`] when the
    /// model needs a [`StatusSource`]).
    pub fn new(
        model: Arc<dyn PrinterModel>,
        engine: Arc<JobEngine>,
        bus: Arc<SharedBus>,
        config: PrintConfig,
    ) -> Self {
        let clock = engine.clock();
        let jobs: Arc<dyn JobStore> = engine.clone();
        let events: Arc<dyn EventBus> = bus.clone();
        let validator = Arc::new(ReplyValidator::new(requested_by_environment()));
        validator.set_job_token_quirk(config.quirks.job_token);
        let ipp = Arc::new(crate::ipp::IppRuntime::new(clock.now(), validator.clone()));
        Self {
            model,
            jobs,
            events,
            clock,
            config,
            engine,
            inbox: None,
            validator,
            inject: Arc::new(Injector::new()),
            ipp,
            reqlog: Arc::new(crate::reqlog::RequestLog::default()),
            bus,
        }
    }

    /// One-call setup: a fresh engine on `clock`, a null event sink, the given model builder
    /// (which receives the engine as its [`StatusSource`]).
    pub fn build(
        config: PrintConfig,
        clock: Arc<dyn Clock>,
        make_model: impl FnOnce(Arc<dyn StatusSource>) -> Arc<dyn PrinterModel>,
    ) -> Self {
        let bus = Arc::new(SharedBus::default());
        let engine = Arc::new(JobEngine::new(
            config.limits.clone(),
            config.timing.clone(),
            clock,
            bus.clone(),
        ));
        let status: Arc<dyn StatusSource> = engine.clone();
        let model = make_model(status);
        Self::new(model, engine, bus, config)
    }

    /// Same context with another model (jobs, events and clock are shared).
    pub fn with_model(&self, model: Arc<dyn PrinterModel>) -> Self {
        Self {
            model,
            jobs: self.jobs.clone(),
            events: self.events.clone(),
            clock: self.clock.clone(),
            config: self.config.clone(),
            engine: self.engine.clone(),
            inbox: self.inbox.clone(),
            validator: self.validator.clone(),
            inject: self.inject.clone(),
            ipp: self.ipp.clone(),
            reqlog: self.reqlog.clone(),
            bus: self.bus.clone(),
        }
    }

    /// Same context with another configuration (jobs, events and clock are shared).
    pub fn with_config(&self, config: PrintConfig) -> Self {
        self.validator.set_job_token_quirk(config.quirks.job_token);
        Self {
            model: self.model.clone(),
            jobs: self.jobs.clone(),
            events: self.events.clone(),
            clock: self.clock.clone(),
            config,
            engine: self.engine.clone(),
            inbox: self.inbox.clone(),
            validator: self.validator.clone(),
            inject: self.inject.clone(),
            ipp: self.ipp.clone(),
            reqlog: self.reqlog.clone(),
            bus: self.bus.clone(),
        }
    }

    /// Same context writing documents to `inbox`.
    pub fn with_inbox(&self, inbox: Arc<Inbox>) -> Self {
        let mut c = self.with_model(self.model.clone());
        c.inbox = Some(inbox);
        c
    }

    /// Violations the reply validator found so far (empty when it is off or all is well).
    /// Never panics; assert on it in tests.
    pub fn validation_problems(&self) -> Vec<String> {
        self.validator.recent_problems()
    }

    /// Arm an injection rule (see `wsd::inject`). Refused when the rule is malformed.
    pub fn inject(&self, rule: InjectRule) -> Result<(), String> {
        self.inject.add(rule)
    }

    /// Drop every pending injection rule.
    pub fn clear_inject(&self) {
        self.inject.clear();
    }

    /// The pending injection rules with their remaining counts.
    pub fn pending_inject(&self) -> Vec<PendingRule> {
        self.inject.pending()
    }

    /// Install the event manager that receives every [`PrintEvent`].
    pub fn set_event_sink(&self, sink: Arc<dyn EventBus>) {
        self.bus.set(sink);
    }

    /// The timers in force (changeable at run time, see [`PrintCtx::set_timing`]).
    pub fn timing(&self) -> PrintTiming {
        self.engine.timing()
    }

    /// Change the processing/cancel/timeout timers of a running server.
    pub fn set_timing(&self, timing: PrintTiming) {
        self.engine.set_timing(timing);
    }

    /// Live printer status (same as the engine's [`StatusSource`]).
    pub fn status(&self) -> PrinterStatusSnapshot {
        self.engine.status()
    }

    /// Apply a lab fault request; raised/cleared conditions are published as events.
    pub fn apply_faults(&self, body: &FaultBody) {
        if body.reset || body.inject_clear {
            self.inject.clear();
        }
        self.engine.apply_faults(body);
        // rules were checked by `apply_faults_checked`; an unchecked caller gets none added
        let _ = self.inject.add_all(body.inject.clone());
    }

    /// [`PrintCtx::apply_faults`] after checking the body; nothing changes when it is refused.
    pub fn apply_faults_checked(&self, body: &FaultBody) -> Result<(), String> {
        body.check()?;
        self.inject
            .check_room(body.reset || body.inject_clear, body.inject.len())?;
        self.apply_faults(body);
        Ok(())
    }

    /// Run due timers (document timeout, processing progress, history trimming). Handlers
    /// call this on entry; a background task may call it periodically so events fire on time.
    pub fn tick(&self) {
        self.engine.tick();
    }

    /// `SetEventRate` value in seconds (default 30); the event manager throttles
    /// CompleteState events with it.
    pub fn event_rate_secs(&self) -> u32 {
        self.engine.event_rate()
    }

    /// Which endpoint an HTTP path serves (`/WebServices/Device` and aliases => device).
    pub fn endpoint_kind_for_path(path: &str) -> EndpointKind {
        let p = path.trim_end_matches('/').to_ascii_lowercase();
        if p.ends_with("/device") {
            EndpointKind::Device
        } else if p.contains("/stablewsdiscoveryendpoint") {
            EndpointKind::Discovery
        } else {
            EndpointKind::PrintService
        }
    }
}

/// Spawn a task that calls [`PrintCtx::tick`] every `every` (default use: 250 ms), so timers
/// fire and events go out without a client request. Returns the task handle.
pub fn spawn_ticker(ctx: Arc<PrintCtx>, every: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(every.max(Duration::from_millis(10)));
        loop {
            iv.tick().await;
            ctx.tick();
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datetime_formats_known_instants() {
        assert_eq!(format_datetime(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        assert_eq!(
            format_datetime(UNIX_EPOCH + Duration::from_secs(1_767_225_600)),
            "2026-01-01T00:00:00Z"
        );
        assert_eq!(
            format_datetime(UNIX_EPOCH + Duration::from_secs(951_782_400 + 86_399)),
            "2000-02-29T23:59:59Z"
        );
    }

    #[test]
    fn timing_presets_and_patches() {
        let i = PrintTiming::instant();
        assert!(i.processing_base.is_zero() && i.processing_per_mib.is_zero());
        assert!(i.terminating.is_zero() && i.document_busy.is_zero());
        assert_eq!(i.document_timeout, Duration::from_secs(60));
        let r = PrintTiming::realistic();
        assert!(r.processing_base >= Duration::from_secs(1));
        let mut t = PrintTiming::default();
        t.apply(&TimingPatch {
            preset: Some("Instant".into()),
            terminating_ms: Some(40),
            ..TimingPatch::default()
        })
        .unwrap();
        assert_eq!(t.terminating, Duration::from_millis(40));
        assert!(t.processing_base.is_zero());
        let before = t.clone();
        assert!(t
            .apply(&TimingPatch {
                preset: Some("nope".into()),
                ..TimingPatch::default()
            })
            .is_err());
        assert!(t
            .apply(&TimingPatch {
                processing_base_ms: Some(5),
                bytes_per_sheet: Some(0),
                ..TimingPatch::default()
            })
            .is_err());
        assert_eq!(t, before, "a refused patch changes nothing");
        assert_eq!(t.to_json()["terminating_ms"], 40);
    }

    #[test]
    fn document_failure_values() {
        assert_eq!(
            DocumentFailure::parse("format"),
            Ok(Some(DocumentFailure::Format))
        );
        assert_eq!(DocumentFailure::parse("None"), Ok(None));
        assert!(DocumentFailure::parse("bogus").is_err());
    }

    #[test]
    fn manual_clock_advances_only_on_request() {
        let c = ManualClock::new();
        let a = c.now();
        assert_eq!(c.now(), a);
        c.advance(Duration::from_secs(61));
        assert_eq!(c.now().duration_since(a).unwrap(), Duration::from_secs(61));
    }

    #[test]
    fn condition_ids_are_unique_and_positive() {
        let ids: BTreeSet<u32> = PrintCondition::ALL.iter().map(|c| c.id()).collect();
        assert_eq!(ids.len(), PrintCondition::ALL.len());
        assert!(ids.iter().all(|i| *i >= 1));
        assert_eq!(
            PrintCondition::from_token("paperout"),
            Some(PrintCondition::PaperOut)
        );
        assert_eq!(
            PrintCondition::from_token("Jam"),
            Some(PrintCondition::PaperJam)
        );
    }

    #[test]
    fn endpoint_kind_by_path() {
        assert_eq!(
            PrintCtx::endpoint_kind_for_path("/WebServices/Device/"),
            EndpointKind::Device
        );
        assert_eq!(
            PrintCtx::endpoint_kind_for_path("/WebServices/PrinterService"),
            EndpointKind::PrintService
        );
    }
}
