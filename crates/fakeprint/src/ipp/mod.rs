//! A strict IPP/2.0 (IPP Everywhere style) test device on `POST /ipp/print`.
//!
//! The device speaks RFC 8010 (encoding, [`codec`]) and RFC 8011 (model, [`service`]) and
//! shares its job engine with the WS-Print side: a job created over IPP is an ordinary engine
//! job, listed by `/v1/jobs`, subject to the same timing presets, fault plan, inbox and
//! history. `docs/ipp.md` is the reference for what is and is not implemented.
//!
//! Layout: [`codec`] (wire format, strict parse), [`caps`] (profile -> IPP values), [`attrs`]
//! (printer and job attributes, `requested-attributes`, job-template validation), [`service`]
//! (the operations), [`validate`] (reply validation switch), [`job`] (what an IPP job keeps).
//! This file is the HTTP face: content-type handling, request injection (including the
//! `ipp_status` effect), counters, validation and the browser-friendly `GET`.
//!
//! The route is mounted on `/ipp/print` (and `/ipp/print/` and `/ipp/print/<job-id>`, which a
//! client may use as the HTTP target of a job operation) for the default device and on
//! `/d/{id}/ipp/print` for the others; the WSD port-monitor probe of the Windows IPP class
//! driver association keeps working unchanged.

pub mod attrs;
pub mod caps;
pub mod codec;
pub mod job;
pub mod service;
pub mod validate;

use std::sync::Arc;
use std::time::SystemTime;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use tokio::sync::RwLock;
use tracing::{info, warn};

pub use service::IppService;

use crate::state::World;
use crate::wsd::inject::{InjectEffect, InjectQuery, RuleEndpoint};
use crate::wsd::validate::ReplyValidator;

/// Per-printer IPP runtime state that outlives a profile swap: the printer's start time (for
/// `printer-up-time` and the job time stamps) and the reply validator.
pub struct IppRuntime {
    /// When the printer "booted" (the context was created), on the engine's clock.
    pub started: SystemTime,
    pub validator: validate::IppValidator,
}

impl IppRuntime {
    pub fn new(started: SystemTime, switch: Arc<ReplyValidator>) -> Self {
        Self {
            started,
            validator: validate::IppValidator::new(switch),
        }
    }
}

/// `GET` -> short JSON (browsers); `POST application/ipp` -> binary IPP.
///
/// Registered on `/ipp/print` always when IPP is on (also needed for CID:MS_IPP WSDMON queue
/// creation, which probes this endpoint before creating `SWD\PRINTENUM`).
///
/// The body is read with [`PrintLimits::max_body`](crate::wsd::ctx::PrintLimits::max_body) as a
/// hard cap (the same cap the WSD routes use). A larger body is HTTP 413 and never reaches the
/// job engine. Axum's default 2 MiB extractor limit does not apply: this handler reads the raw
/// body itself.
pub async fn print_handler(
    State(world): State<Arc<RwLock<World>>>,
    req: axum::extract::Request,
) -> Response {
    let (parts, body) = req.into_parts();
    let max = world.read().await.print_ctx().config.limits.max_body;
    let body = match axum::body::to_bytes(body, max).await {
        Ok(b) => b,
        Err(_) => {
            warn!(max, "IPP request over the body limit");
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                "IPP request body exceeds the printer's max_body limit",
            )
                .into_response();
        }
    };
    let method = parts.method;
    let headers = parts.headers;
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let svc = world.read().await.ipp_service();

    // A browser (or a probe) with no IPP body gets the JSON summary.
    let is_ipp = method == Method::POST && ct == "application/ipp";
    let op_name = is_ipp.then(|| {
        if body.len() >= 4 {
            codec::op_inject_name(u16::from_be_bytes([body[2], body[3]]))
        } else {
            "malformed".to_string()
        }
    });
    note_ipp_request(is_ipp, &body);
    let injection = svc.ctx.inject.take(&InjectQuery {
        endpoint: RuleEndpoint::Ipp,
        action: op_name.as_deref(),
    });
    if let Some(inj) = &injection {
        crate::reqlog::annotate(|n| n.fault_rule = Some(inj.rule.clone()));
        inj.sleep(false).await;
        if inj.closes() {
            crate::reqlog::annotate(|n| n.dropped = true);
            return crate::wsd::aborted_response();
        }
        if let InjectEffect::IppStatus { status, message } = &inj.effect {
            let (version, request_id) = if body.len() >= 8 {
                (
                    (body[0], body[1]),
                    u32::from_be_bytes([body[4], body[5], body[6], body[7]]),
                )
            } else {
                ((2, 0), 1)
            };
            let bytes = svc.status_reply(
                version,
                request_id,
                status
                    .code()
                    .unwrap_or(codec::status::SERVER_INTERNAL_ERROR),
                message.clone(),
            );
            svc.ctx
                .ipp
                .validator
                .check(codec::Message::parse(&body).ok().as_ref(), &bytes);
            note_ipp_reply(request_of(&body).as_ref(), &bytes);
            inj.sleep(true).await;
            return ipp_response(bytes);
        }
        if let Some(h) = inj.http_reply() {
            inj.sleep(true).await;
            return crate::wsd::http_response(h);
        }
    }

    let response = if body.is_empty() && !ct.contains("ipp") {
        json_summary(&svc)
    } else if !is_ipp {
        (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            "IPP needs POST with Content-Type: application/ipp",
        )
            .into_response()
    } else {
        let request = codec::Message::parse(&body).ok();
        let svc2 = svc.clone();
        let b = body.clone();
        let note = crate::reqlog::current();
        let outcome =
            tokio::task::spawn_blocking(move || crate::reqlog::with_note(note, || svc2.handle(&b)))
                .await;
        match outcome {
            Ok(o) => {
                info!(
                    op = %codec::op_name(o.op).unwrap_or("?"),
                    status = %codec::status_name(o.status),
                    bytes = body.len(),
                    "IPP"
                );
                world
                    .write()
                    .await
                    .note_ipp_action(codec::op_counter_name(o.op));
                svc.ctx.ipp.validator.check(request.as_ref(), &o.bytes);
                note_ipp_reply(request.as_ref(), &o.bytes);
                ipp_response(o.bytes)
            }
            Err(_) => ipp_response(svc.status_reply(
                (2, 0),
                1,
                codec::status::SERVER_INTERNAL_ERROR,
                Some("the handler failed".into()),
            )),
        }
    };
    if let Some(inj) = &injection {
        inj.sleep(true).await;
    }
    response
}

fn request_of(body: &[u8]) -> Option<codec::Message> {
    codec::Message::parse(body).ok()
}

fn value_text(v: &codec::Value) -> String {
    use codec::Value as V;
    match v {
        V::Integer(i) | V::Enum(i) => i.to_string(),
        V::Boolean(b) => b.to_string(),
        V::Text(s)
        | V::Name(s)
        | V::Keyword(s)
        | V::Uri(s)
        | V::UriScheme(s)
        | V::Charset(s)
        | V::NaturalLanguage(s)
        | V::MimeType(s) => s.clone(),
        V::TextWithLanguage { text, .. } => text.clone(),
        V::NameWithLanguage { name, .. } => name.clone(),
        V::Resolution { x, y, units } => {
            format!("{x}x{y}{}", if *units == 4 { "dpcm" } else { "dpi" })
        }
        V::Range { lower, upper } => format!("{lower}-{upper}"),
        other => format!("{other:?}"),
    }
}

fn group_name(tag: u8) -> &'static str {
    match tag {
        codec::tag::OPERATION_ATTRIBUTES => "operation",
        codec::tag::JOB_ATTRIBUTES => "job",
        codec::tag::PRINTER_ATTRIBUTES => "printer",
        codec::tag::UNSUPPORTED_ATTRIBUTES => "unsupported",
        _ => "other",
    }
}

fn dump(m: &codec::Message) -> Vec<crate::reqlog::IppAttr> {
    let mut out = Vec::new();
    'g: for g in &m.groups {
        for a in &g.attrs {
            if out.len() >= 128 {
                break 'g;
            }
            out.push(crate::reqlog::IppAttr {
                group: group_name(g.tag).into(),
                name: a.name.clone(),
                values: a.values.iter().map(value_text).collect(),
            });
        }
    }
    out
}

/// Request half of the log note: operation, request id, the decoded attributes.
fn note_ipp_request(is_ipp: bool, body: &[u8]) {
    if !is_ipp {
        return;
    }
    let req = request_of(body);
    crate::reqlog::annotate(|n| {
        if body.len() >= 8 {
            let op = u16::from_be_bytes([body[2], body[3]]);
            n.ipp_operation_id = Some(op);
            n.request_id = Some(u32::from_be_bytes([body[4], body[5], body[6], body[7]]));
            let name = codec::op_name(op)
                .map(str::to_string)
                .unwrap_or_else(|| codec::op_inject_name(op));
            n.action = Some(name.clone());
            n.ipp_operation = Some(name);
            n.document = matches!(op, codec::op::PRINT_JOB | codec::op::SEND_DOCUMENT);
        }
        if let Some(m) = &req {
            n.job_id = m
                .groups
                .iter()
                .flat_map(|g| g.attrs.iter())
                .find(|a| a.name == "job-id")
                .and_then(|a| a.values.first())
                .and_then(|v| match v {
                    codec::Value::Integer(i) => u32::try_from(*i).ok(),
                    _ => None,
                });
            n.ipp = Some(crate::reqlog::IppDump {
                version: Some(format!("{}.{}", m.version.0, m.version.1)),
                request_attrs: dump(m),
                response_attrs: Vec::new(),
                document_bytes: m.data.len() as u64,
            });
        }
    });
}

/// Reply half: status, job id, decoded attributes, the unsupported / substituted attributes.
fn note_ipp_reply(_req: Option<&codec::Message>, bytes: &[u8]) {
    let Some(resp) = codec::Message::parse(bytes).ok() else {
        return;
    };
    let status = codec::status_name(resp.code);
    crate::reqlog::annotate(|n| {
        n.ipp_status = Some(status.clone());
        n.faulted = resp.code >= 0x0400;
        if let Some(id) = resp
            .groups
            .iter()
            .filter(|g| g.tag == codec::tag::JOB_ATTRIBUTES)
            .flat_map(|g| g.attrs.iter())
            .find(|a| a.name == "job-id")
            .and_then(|a| a.values.first())
            .and_then(|v| match v {
                codec::Value::Integer(i) => u32::try_from(*i).ok(),
                _ => None,
            })
        {
            n.job_id = Some(id);
        }
        for g in resp
            .groups
            .iter()
            .filter(|g| g.tag == codec::tag::UNSUPPORTED_ATTRIBUTES)
        {
            for a in &g.attrs {
                n.substitutions.push(crate::reqlog::Substitution {
                    field: a.name.clone(),
                    requested: a
                        .values
                        .iter()
                        .map(value_text)
                        .collect::<Vec<_>>()
                        .join(","),
                    applied: None,
                    reason: format!("unsupported-attributes group in the reply ({status})"),
                });
            }
        }
        if let Some(d) = n.ipp.as_mut() {
            d.response_attrs = dump(&resp);
        } else {
            n.ipp = Some(crate::reqlog::IppDump {
                response_attrs: dump(&resp),
                ..Default::default()
            });
        }
    });
}

pub async fn print_stub(
    State(world): State<Arc<RwLock<World>>>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut req = axum::extract::Request::new(axum::body::Body::from(body));
    *req.method_mut() = method;
    *req.uri_mut() = "/ipp/print".parse().expect("uri");
    *req.headers_mut() = headers;
    print_handler(State(world), req).await
}

fn ipp_response(bytes: Vec<u8>) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/ipp")],
        bytes,
    )
        .into_response()
}

fn json_summary(svc: &IppService) -> Response {
    (
        StatusCode::OK,
        Json(json!({
            "service": "fakeprint-ipp",
            "path": "/ipp/print",
            "printer_uri": svc.printer_uri,
            "note": "POST application/ipp (see docs/ipp.md)",
        })),
    )
        .into_response()
}
