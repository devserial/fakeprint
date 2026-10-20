//! The raw request log: a bounded, per-device ring buffer of every request the device's wire
//! endpoints (WS-Print SOAP, directed WS-Discovery, IPP) received and every callback the device
//! sent (WS-Eventing deliveries), with what a protocol-level test wants to see afterwards:
//! the HTTP headers as the client wrote them (case and order kept), the body, the parsed
//! summary (WSD action, message id, IPP operation, request id, job id), the reply, the latency,
//! the fault rule that applied, and the ticket substitutions the validator made.
//!
//! Layout
//!
//! * [`RequestLogConfig`] / [`RequestLog`]: the ring, its bounds and its JSON rendering.
//! * [`capture`]: the axum middleware mounted on the WSD / IPP / discovery routes. It tees the
//!   request body (nothing is buffered beyond the configured cap, the handler still streams
//!   the whole body), buffers the (small) reply, and files the [`Record`].
//! * [`Note`]: what the handlers add (action, job id, fault rule, ticket, IPP attributes). It
//!   reaches a handler through a task-local and, inside `spawn_blocking`, through a
//!   thread-local ([`with_note`]), so no engine signature changed.
//! * [`TapListener`] / [`TapStream`] / [`TapRegistry`]: a pass-through wrapper of the accepted
//!   sockets that frames the HTTP/1.1 request heads off the wire (without touching the bytes
//!   hyper reads). hyper lower-cases header names and groups duplicates; the tap is how the
//!   record keeps `User-Agent` as written and the headers in wire order. Without a tap (an
//!   in-process `oneshot` call) the headers come from the parsed map and `headers_raw` is
//!   false.
//!
//! Bounds (all configurable, see [`RequestLogConfig`]): at most `capacity` records, at most
//! `max_bytes` of stored payload across them (oldest dropped first), at most `body_cap` bytes
//! stored per request body and per reply body, and only `doc_head` bytes of a document-carrying
//! request (`SendDocument`, `AddDocument`, `Print-Job`, `Send-Document`, ...), whose length and
//! SHA-256 are kept instead. The worst case memory is therefore `max_bytes` plus the per-record
//! headers; the typical case (small SOAP and IPP messages) is a few hundred KiB.
//!
//! This is a test fake: bodies can contain user data (job names, user names, documents' first
//! bytes). The log lives in memory only, is cleared by `DELETE /v1/requests` and `POST
//! /v1/reset`, and is never written to disk. [`RequestLogConfig::redact`] replaces the peer
//! address and credential headers.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::{ConnectInfo, Query, Request, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::wsd::SharedWorld;

/// Default number of records kept per device.
pub const DEFAULT_CAPACITY: usize = 256;
/// Default cap of one stored body (request or reply).
pub const DEFAULT_BODY_CAP: usize = 256 * 1024;
/// Default number of bytes stored of a document-carrying request.
pub const DEFAULT_DOC_HEAD: usize = 16 * 1024;
/// Default cap of the payload bytes held by the whole ring.
pub const DEFAULT_MAX_BYTES: usize = 32 * 1024 * 1024;

/// Largest reply the capture middleware will hold while it records the exchange. Printer
/// replies are a few kilobytes; past this the connection is dropped instead of allocating
/// the whole body.
const MAX_RESPONSE_BUFFER: usize = 16 << 20;
/// Pending ticket / job side tables are bounded to this many jobs.
const JOB_META_MAX: usize = 1024;
/// Request seqs remembered per job.
const JOB_SEQS_MAX: usize = 64;
/// Largest raw ticket kept.
const TICKET_MAX: usize = 64 * 1024;

// ---------------------------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------------------------

/// The `request_log` configuration of a server or device.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RequestLogConfig {
    /// Switch. Off: the middleware returns immediately and nothing is recorded.
    pub enabled: bool,
    /// Records kept (drop-oldest). 0 means off.
    pub capacity: usize,
    /// Stored bytes of one request body and of one reply body.
    pub body_cap: usize,
    /// Stored bytes of a document-carrying request body (the rest: length + SHA-256 only).
    pub doc_head: usize,
    /// Bytes of payload the whole ring may hold.
    pub max_bytes: usize,
    /// Replace the peer address and credential headers by a placeholder.
    pub redact: bool,
}

impl Default for RequestLogConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            capacity: DEFAULT_CAPACITY,
            body_cap: DEFAULT_BODY_CAP,
            doc_head: DEFAULT_DOC_HEAD,
            max_bytes: DEFAULT_MAX_BYTES,
            redact: false,
        }
    }
}

impl RequestLogConfig {
    /// A configuration with the log off.
    pub fn off() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }

    /// Clamp to sane limits (no panics, no giant allocations from a typo).
    pub fn normalized(mut self) -> Self {
        self.capacity = self.capacity.min(100_000);
        self.body_cap = self.body_cap.min(64 << 20);
        self.doc_head = self.doc_head.min(self.body_cap.max(1));
        self.max_bytes = self.max_bytes.min(1 << 30);
        if self.capacity == 0 {
            self.enabled = false;
        }
        self
    }

    /// Apply the set fields of a patch.
    pub fn patched(&self, p: &RequestLogPatch) -> Self {
        let mut c = self.clone();
        if let Some(v) = p.enabled {
            c.enabled = v;
        }
        if let Some(v) = p.capacity {
            c.capacity = v;
        }
        if let Some(v) = p.body_cap {
            c.body_cap = v;
        }
        if let Some(v) = p.doc_head {
            c.doc_head = v;
        }
        if let Some(v) = p.max_bytes {
            c.max_bytes = v;
        }
        if let Some(v) = p.redact {
            c.redact = v;
        }
        c.normalized()
    }
}

/// `POST /v1/requests/config`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RequestLogPatch {
    pub enabled: Option<bool>,
    pub capacity: Option<usize>,
    pub body_cap: Option<usize>,
    pub doc_head: Option<usize>,
    pub max_bytes: Option<usize>,
    pub redact: Option<bool>,
}

// ---------------------------------------------------------------------------------------------
// SHA-256 (the crate has no hash dependency; this is FIPS 180-4, tested against the vectors)
// ---------------------------------------------------------------------------------------------

const K256: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

/// Incremental SHA-256.
#[derive(Clone)]
pub struct Sha256 {
    h: [u32; 8],
    buf: [u8; 64],
    fill: usize,
    len: u64,
}

impl Default for Sha256 {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha256 {
    pub fn new() -> Self {
        Self {
            h: [
                0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
                0x5be0cd19,
            ],
            buf: [0; 64],
            fill: 0,
            len: 0,
        }
    }

    fn block(h: &mut [u32; 8], b: &[u8]) {
        let mut w = [0u32; 64];
        for (i, wi) in w.iter_mut().enumerate().take(16) {
            *wi = u32::from_be_bytes([b[4 * i], b[4 * i + 1], b[4 * i + 2], b[4 * i + 3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut bb, mut c, mut d, mut e, mut f, mut g, mut hh] = *h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ (!e & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K256[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & bb) ^ (a & c) ^ (bb & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = bb;
            bb = a;
            a = t1.wrapping_add(t2);
        }
        for (x, v) in h.iter_mut().zip([a, bb, c, d, e, f, g, hh]) {
            *x = x.wrapping_add(v);
        }
    }

    pub fn update(&mut self, mut data: &[u8]) {
        self.len = self.len.wrapping_add(data.len() as u64);
        if self.fill > 0 {
            let take = (64 - self.fill).min(data.len());
            self.buf[self.fill..self.fill + take].copy_from_slice(&data[..take]);
            self.fill += take;
            data = &data[take..];
            if self.fill == 64 {
                let b = self.buf;
                Self::block(&mut self.h, &b);
                self.fill = 0;
            }
        }
        while data.len() >= 64 {
            Self::block(&mut self.h, &data[..64]);
            data = &data[64..];
        }
        if !data.is_empty() {
            self.buf[..data.len()].copy_from_slice(data);
            self.fill = data.len();
        }
    }

    pub fn finish_hex(mut self) -> String {
        let bits = self.len.wrapping_mul(8);
        let mut pad = vec![0x80u8];
        let used = (self.fill + 1) % 64;
        let zeros = if used <= 56 { 56 - used } else { 120 - used };
        pad.extend(std::iter::repeat_n(0u8, zeros));
        pad.extend_from_slice(&bits.to_be_bytes());
        let keep = self.len;
        self.update(&pad);
        self.len = keep;
        self.h.iter().map(|x| format!("{x:08x}")).collect()
    }
}

/// SHA-256 of a byte slice as lower-case hex.
pub fn sha256_hex(data: &[u8]) -> String {
    let mut s = Sha256::new();
    s.update(data);
    s.finish_hex()
}

// ---------------------------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------------------------

/// Direction of a record relative to the device.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    /// A request the device received.
    In,
    /// A request the device sent (an event delivery).
    Out,
}

/// How a request ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    /// Answered, and the answer is not a fault / error.
    Ok,
    /// Answered with a SOAP fault, an HTTP error status or an IPP error status (injected or
    /// the engine's own).
    Faulted,
    /// No answer reached the client (injected connection drop, body error, failed delivery).
    Dropped,
}

/// A stored (possibly truncated) body.
#[derive(Debug, Clone, Default)]
pub struct Payload {
    pub bytes: Vec<u8>,
    /// Real length on the wire.
    pub length: u64,
    pub truncated: bool,
    /// SHA-256 of the whole body.
    pub sha256: Option<String>,
    /// Stored as a head slice because the request carries a document.
    pub document: bool,
}

impl Payload {
    fn from_slice(b: &[u8], cap: usize) -> Self {
        let stored = b.len().min(cap);
        Self {
            bytes: b[..stored].to_vec(),
            length: b.len() as u64,
            truncated: b.len() > stored,
            sha256: (!b.is_empty()).then(|| sha256_hex(b)),
            document: false,
        }
    }

    fn size(&self) -> usize {
        self.bytes.len()
    }

    fn to_json(&self) -> Value {
        let text = std::str::from_utf8(&self.bytes).ok().map(str::to_string);
        json!({
            "length": self.length,
            "stored": self.bytes.len(),
            "truncated": self.truncated,
            "document": self.document,
            "sha256": self.sha256,
            "base64": base64::engine::general_purpose::STANDARD.encode(&self.bytes),
            "text": text,
        })
    }

    fn to_json_meta(&self) -> Value {
        json!({
            "length": self.length,
            "stored": self.bytes.len(),
            "truncated": self.truncated,
            "document": self.document,
            "sha256": self.sha256,
        })
    }
}

/// One thing the ticket validator or the IPP attribute check changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Substitution {
    pub field: String,
    pub requested: String,
    /// `None`: the element / attribute was dropped.
    pub applied: Option<String>,
    pub reason: String,
}

/// One decoded IPP attribute of a dump.
#[derive(Debug, Clone, Serialize)]
pub struct IppAttr {
    pub group: String,
    pub name: String,
    pub values: Vec<String>,
}

/// The decoded IPP request and reply.
#[derive(Debug, Clone, Default, Serialize)]
pub struct IppDump {
    pub version: Option<String>,
    pub request_attrs: Vec<IppAttr>,
    pub response_attrs: Vec<IppAttr>,
    pub document_bytes: u64,
}

/// One logged exchange.
#[derive(Debug, Clone)]
pub struct Record {
    pub seq: u64,
    pub direction: Direction,
    /// Milliseconds since the log was created (monotonic) when the request started.
    pub t_mono_ms: f64,
    pub t_wall_unix_ms: u64,
    pub peer: Option<String>,
    pub method: String,
    /// Path as the client sent it (with `/d/<id>` for a non-default device).
    pub path: String,
    pub query: Option<String>,
    pub version: String,
    /// Request headers, case and order as received when `headers_raw`.
    pub headers: Vec<(String, String)>,
    pub headers_raw: bool,
    pub body: Payload,
    pub protocol: String,
    pub endpoint: Option<String>,
    pub action: Option<String>,
    pub action_uri: Option<String>,
    pub message_id: Option<String>,
    pub request_id: Option<u32>,
    pub ipp_operation: Option<String>,
    pub ipp_operation_id: Option<u16>,
    pub ipp_status: Option<String>,
    pub job_id: Option<u32>,
    pub status: Option<u16>,
    pub response_headers: Vec<(String, String)>,
    pub response_body: Payload,
    pub latency_us: u64,
    pub fault_rule: Option<String>,
    pub outcome: Outcome,
    pub ticket_received: Option<String>,
    pub substitutions: Vec<Substitution>,
    pub ipp: Option<IppDump>,
    /// The raw request head (request line and headers, as on the wire), when tapped.
    pub raw_head: Option<Arc<Vec<u8>>>,
}

fn iso8601_ms(unix_ms: u64) -> String {
    // civil-from-days (Howard Hinnant), no time dependency in this crate for it
    let secs = (unix_ms / 1000) as i64;
    let ms = unix_ms % 1000;
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{ms:03}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

fn headers_json(h: &[(String, String)]) -> Value {
    Value::Array(h.iter().map(|(k, v)| json!([k, v])).collect())
}

impl Record {
    fn footprint(&self) -> usize {
        let hs: usize = self
            .headers
            .iter()
            .chain(&self.response_headers)
            .map(|(k, v)| k.len() + v.len())
            .sum();
        256 + hs
            + self.body.size()
            + self.response_body.size()
            + self.ticket_received.as_ref().map_or(0, String::len)
            + self.raw_head.as_ref().map_or(0, |h| h.len())
    }

    /// The JSON the control API shows; `bodies` adds base64 / text of the stored bytes.
    pub fn to_json(&self, bodies: bool) -> Value {
        let pay = |p: &Payload| {
            if bodies {
                p.to_json()
            } else {
                p.to_json_meta()
            }
        };
        json!({
            "seq": self.seq,
            "direction": self.direction,
            "t_mono_ms": self.t_mono_ms,
            "t_wall_unix_ms": self.t_wall_unix_ms,
            "t_wall": iso8601_ms(self.t_wall_unix_ms),
            "peer": self.peer,
            "method": self.method,
            "path": self.path,
            "query": self.query,
            "version": self.version,
            "protocol": self.protocol,
            "endpoint": self.endpoint,
            "action": self.action,
            "action_uri": self.action_uri,
            "message_id": self.message_id,
            "request_id": self.request_id,
            "ipp_operation": self.ipp_operation,
            "ipp_operation_id": self.ipp_operation_id,
            "ipp_status": self.ipp_status,
            "job_id": self.job_id,
            "latency_us": self.latency_us,
            "fault_rule": self.fault_rule,
            "result": self.outcome,
            "request": {
                "headers": headers_json(&self.headers),
                "headers_raw": self.headers_raw,
                "body": pay(&self.body),
            },
            "response": {
                "status": self.status,
                "headers": headers_json(&self.response_headers),
                "body": pay(&self.response_body),
            },
            "ticket": self.ticket_received.as_ref().map(|t| json!({
                "received": t,
                "substitutions": self.substitutions,
            })).or_else(|| (!self.substitutions.is_empty()).then(|| json!({
                "received": Value::Null,
                "substitutions": self.substitutions,
            }))),
            "ipp": self.ipp,
        })
    }

    /// The request as bytes: the raw head when tapped (else one rebuilt from the headers), a
    /// blank line, the stored body.
    pub fn raw_request(&self) -> Vec<u8> {
        let mut out = match &self.raw_head {
            Some(h) => h.as_ref().clone(),
            None => {
                let mut s = format!(
                    "{} {}{} {}\r\n",
                    self.method,
                    self.path,
                    self.query
                        .as_ref()
                        .map(|q| format!("?{q}"))
                        .unwrap_or_default(),
                    self.version
                );
                for (k, v) in &self.headers {
                    s.push_str(&format!("{k}: {v}\r\n"));
                }
                s.push_str("\r\n");
                s.into_bytes()
            }
        };
        out.extend_from_slice(&self.body.bytes);
        out
    }

    /// The reply as bytes: a rebuilt status line, headers, a blank line, the stored body.
    pub fn raw_response(&self) -> Vec<u8> {
        let mut s = format!("HTTP/1.1 {}\r\n", self.status.unwrap_or(0));
        for (k, v) in &self.response_headers {
            s.push_str(&format!("{k}: {v}\r\n"));
        }
        s.push_str("\r\n");
        let mut out = s.into_bytes();
        out.extend_from_slice(&self.response_body.bytes);
        out
    }
}

/// What the log keeps per job (outlives the ring's records).
#[derive(Debug, Clone, Default)]
pub struct JobMeta {
    pub ticket_received: Option<String>,
    pub substitutions: Vec<Substitution>,
    pub request_seqs: Vec<u64>,
}

#[derive(Default)]
struct Inner {
    ring: VecDeque<Arc<Record>>,
    bytes: usize,
    dropped: u64,
    jobs: HashMap<u32, JobMeta>,
    job_order: VecDeque<u32>,
}

/// The per-device request log. Cheap to share; all methods are infallible.
pub struct RequestLog {
    cfg: RwLock<RequestLogConfig>,
    enabled: AtomicBool,
    next_seq: AtomicU64,
    in_flight: AtomicU64,
    inner: Mutex<Inner>,
    started: Instant,
}

impl std::fmt::Debug for RequestLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestLog")
            .field("enabled", &self.enabled())
            .field("next_seq", &self.next_seq.load(Ordering::Relaxed))
            .finish()
    }
}

impl Default for RequestLog {
    fn default() -> Self {
        Self::new(RequestLogConfig::default())
    }
}

impl RequestLog {
    pub fn new(cfg: RequestLogConfig) -> Self {
        let cfg = cfg.normalized();
        Self {
            enabled: AtomicBool::new(cfg.enabled),
            cfg: RwLock::new(cfg),
            next_seq: AtomicU64::new(1),
            in_flight: AtomicU64::new(0),
            inner: Mutex::new(Inner::default()),
            started: Instant::now(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn config(&self) -> RequestLogConfig {
        match self.cfg.read() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    /// Replace the configuration; a smaller capacity drops the oldest records at once.
    pub fn configure(&self, cfg: RequestLogConfig) {
        let cfg = cfg.normalized();
        self.enabled.store(cfg.enabled, Ordering::Relaxed);
        match self.cfg.write() {
            Ok(mut g) => *g = cfg,
            Err(p) => *p.into_inner() = cfg,
        }
        let mut inner = self.lock();
        self.evict(&mut inner);
    }

    /// Apply a patch (`POST /v1/requests/config`); returns the new configuration.
    pub fn patch(&self, p: &RequestLogPatch) -> RequestLogConfig {
        let c = self.config().patched(p);
        self.configure(c.clone());
        c
    }

    fn evict(&self, inner: &mut Inner) {
        let cfg = self.config();
        while inner.ring.len() > cfg.capacity
            || (inner.bytes > cfg.max_bytes && !inner.ring.is_empty())
        {
            if let Some(old) = inner.ring.pop_front() {
                inner.bytes = inner.bytes.saturating_sub(old.footprint());
                inner.dropped += 1;
            }
        }
    }

    fn mono_ms(&self, at: Instant) -> f64 {
        at.saturating_duration_since(self.started).as_secs_f64() * 1000.0
    }

    /// File a finished record; returns its seq. `rec.seq` is assigned here (completion order,
    /// so a poller with a `since` cursor never misses a record).
    fn push(&self, mut rec: Record) -> u64 {
        let mut inner = self.lock();
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        rec.seq = seq;
        if let Some(job) = rec.job_id {
            self.job_entry(&mut inner, job).request_seqs.push(seq);
            let m = self.job_entry(&mut inner, job);
            if m.request_seqs.len() > JOB_SEQS_MAX {
                m.request_seqs.remove(0);
            }
        }
        inner.bytes += rec.footprint();
        inner.ring.push_back(Arc::new(rec));
        self.evict(&mut inner);
        seq
    }

    fn job_entry<'a>(&self, inner: &'a mut Inner, job: u32) -> &'a mut JobMeta {
        if !inner.jobs.contains_key(&job) {
            inner.job_order.push_back(job);
            while inner.job_order.len() > JOB_META_MAX {
                if let Some(old) = inner.job_order.pop_front() {
                    inner.jobs.remove(&old);
                }
            }
        }
        inner.jobs.entry(job).or_default()
    }

    /// The ticket a `CreatePrintJob` carried and what the validator changed (called by the
    /// engine glue right after the job id is known).
    pub fn record_ticket(&self, job: u32, raw: Option<String>, subs: Vec<Substitution>) {
        if !self.enabled() {
            return;
        }
        let mut inner = self.lock();
        let m = self.job_entry(&mut inner, job);
        m.ticket_received = raw;
        m.substitutions = subs;
    }

    /// What the log knows of a job.
    pub fn job_meta(&self, job: u32) -> Option<JobMeta> {
        self.lock().jobs.get(&job).cloned()
    }

    /// Record an outbound request (an event delivery).
    #[allow(clippy::too_many_arguments)]
    pub fn record_outbound(
        &self,
        target: &str,
        path: &str,
        headers: Vec<(String, String)>,
        body: &[u8],
        status: Option<u16>,
        error: Option<String>,
        started: Instant,
        action: &str,
    ) {
        if !self.enabled() {
            return;
        }
        let cfg = self.config();
        let wall = SystemTime::now();
        let rec = Record {
            seq: 0,
            direction: Direction::Out,
            t_mono_ms: self.mono_ms(started),
            t_wall_unix_ms: wall
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            peer: Some(if cfg.redact {
                "redacted".into()
            } else {
                target.to_string()
            }),
            method: "POST".into(),
            path: path.to_string(),
            query: None,
            version: "HTTP/1.1".into(),
            headers: redact_headers(headers, cfg.redact),
            headers_raw: true,
            body: Payload::from_slice(body, cfg.body_cap),
            protocol: "event".into(),
            endpoint: Some("subscriber".into()),
            action: Some(short_uri(action)),
            action_uri: Some(action.to_string()),
            message_id: None,
            request_id: None,
            ipp_operation: None,
            ipp_operation_id: None,
            ipp_status: None,
            job_id: None,
            status,
            response_headers: Vec::new(),
            response_body: Payload::default(),
            latency_us: started.elapsed().as_micros() as u64,
            fault_rule: None,
            outcome: match (status, &error) {
                (Some(s), None) if (200..300).contains(&s) => Outcome::Ok,
                (Some(_), _) => Outcome::Faulted,
                _ => Outcome::Dropped,
            },
            ticket_received: error,
            substitutions: Vec::new(),
            ipp: None,
            raw_head: None,
        };
        self.push(rec);
    }

    /// Drop every record (seq numbers keep counting). Job side tables lose their seq links but
    /// keep the stored tickets.
    pub fn clear(&self) {
        let mut inner = self.lock();
        inner.ring.clear();
        inner.bytes = 0;
        for m in inner.jobs.values_mut() {
            m.request_seqs.clear();
        }
    }

    /// Drop everything including the job tables (a lab reset).
    pub fn reset(&self) {
        let mut inner = self.lock();
        *inner = Inner::default();
    }

    /// Records with `seq > since`, oldest first, at most `limit`, filtered.
    pub fn query(&self, q: &RequestQuery) -> Vec<Arc<Record>> {
        let inner = self.lock();
        let mut out = Vec::new();
        for r in inner.ring.iter() {
            if r.seq <= q.since.unwrap_or(0) {
                continue;
            }
            if let Some(a) = &q.action {
                let a = a.to_ascii_lowercase();
                let hit = r
                    .action
                    .as_ref()
                    .is_some_and(|x| x.to_ascii_lowercase() == a)
                    || r.action_uri
                        .as_ref()
                        .is_some_and(|x| x.to_ascii_lowercase() == a)
                    || r.ipp_operation
                        .as_ref()
                        .is_some_and(|x| x.to_ascii_lowercase() == a);
                if !hit {
                    continue;
                }
            }
            if q.job.is_some_and(|j| r.job_id != Some(j)) {
                continue;
            }
            if let Some(d) = &q.direction {
                let want = d.to_ascii_lowercase();
                let have = match r.direction {
                    Direction::In => "in",
                    Direction::Out => "out",
                };
                if want != have {
                    continue;
                }
            }
            out.push(Arc::clone(r));
            if out.len() >= q.limit {
                break;
            }
        }
        out
    }

    pub fn get(&self, seq: u64) -> Option<Arc<Record>> {
        self.lock().ring.iter().find(|r| r.seq == seq).cloned()
    }

    /// Records currently held.
    pub fn len(&self) -> usize {
        self.lock().ring.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Records dropped to stay inside the bounds since the log was created.
    pub fn dropped(&self) -> u64 {
        self.lock().dropped
    }

    /// The seq the next record gets.
    pub fn next_seq(&self) -> u64 {
        self.next_seq.load(Ordering::SeqCst)
    }

    /// The summary object the listing starts with.
    pub fn stats_json(&self) -> Value {
        let (len, bytes, dropped) = {
            let i = self.lock();
            (i.ring.len(), i.bytes, i.dropped)
        };
        json!({
            "config": self.config(),
            "count": len,
            "stored_bytes": bytes,
            "dropped": dropped,
            "next_seq": self.next_seq(),
            "in_flight": self.in_flight.load(Ordering::Relaxed),
        })
    }
}

/// Filters of the listing.
#[derive(Debug, Clone)]
pub struct RequestQuery {
    pub since: Option<u64>,
    pub limit: usize,
    pub action: Option<String>,
    pub job: Option<u32>,
    pub direction: Option<String>,
}

impl Default for RequestQuery {
    fn default() -> Self {
        Self {
            since: None,
            limit: 100,
            action: None,
            job: None,
            direction: None,
        }
    }
}

fn short_uri(uri: &str) -> String {
    uri.rsplit(['/', '#']).next().unwrap_or(uri).to_string()
}

fn redact_headers(h: Vec<(String, String)>, redact: bool) -> Vec<(String, String)> {
    if !redact {
        return h;
    }
    h.into_iter()
        .map(|(k, v)| {
            let l = k.to_ascii_lowercase();
            if matches!(
                l.as_str(),
                "authorization" | "proxy-authorization" | "cookie" | "set-cookie"
            ) {
                (k, "redacted".into())
            } else {
                (k, v)
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// The Note: what handlers add to the record of the request they are serving
// ---------------------------------------------------------------------------------------------

/// Data a handler attaches to the in-flight record.
#[derive(Debug, Clone, Default)]
pub struct NoteData {
    pub protocol: Option<String>,
    pub endpoint: Option<String>,
    pub action: Option<String>,
    pub action_uri: Option<String>,
    pub message_id: Option<String>,
    pub request_id: Option<u32>,
    pub ipp_operation: Option<String>,
    pub ipp_operation_id: Option<u16>,
    pub ipp_status: Option<String>,
    pub job_id: Option<u32>,
    /// The request carries a document: store a head slice only.
    pub document: bool,
    pub fault_rule: Option<String>,
    /// The handler decided the reply is a fault / error.
    pub faulted: bool,
    pub dropped: bool,
    pub ticket_raw: Option<String>,
    pub substitutions: Vec<Substitution>,
    pub ipp: Option<IppDump>,
}

/// Shared handle on a [`NoteData`].
#[derive(Debug, Default)]
pub struct Note {
    data: Mutex<NoteData>,
}

impl Note {
    pub fn with<R>(&self, f: impl FnOnce(&mut NoteData) -> R) -> R {
        let mut g = match self.data.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        f(&mut g)
    }

    fn take(&self) -> NoteData {
        self.with(std::mem::take)
    }
}

tokio::task_local! {
    static TASK_NOTE: Arc<Note>;
}

thread_local! {
    static THREAD_NOTE: RefCell<Option<Arc<Note>>> = const { RefCell::new(None) };
}

/// The note of the request being served: the task-local one in async handlers, the
/// thread-local one inside [`with_note`].
pub fn current() -> Option<Arc<Note>> {
    if let Ok(n) = TASK_NOTE.try_with(Arc::clone) {
        return Some(n);
    }
    THREAD_NOTE.with(|c| c.borrow().clone())
}

/// Run `f` (typically on a `spawn_blocking` thread) with `note` as the current note.
pub fn with_note<R>(note: Option<Arc<Note>>, f: impl FnOnce() -> R) -> R {
    let prev = THREAD_NOTE.with(|c| c.replace(note));
    let out = f();
    THREAD_NOTE.with(|c| *c.borrow_mut() = prev);
    out
}

/// Edit the current note, if there is one. Never panics.
pub fn annotate(f: impl FnOnce(&mut NoteData)) {
    if let Some(n) = current() {
        n.with(f);
    }
}

/// What a note-aware handler needs of the current request, if it is being logged.
pub fn is_logging() -> bool {
    current().is_some()
}

/// The description of one validator substitution.
pub fn substitution(element: &str, requested: &str, applied: Option<&str>) -> Substitution {
    Substitution {
        field: element.to_string(),
        requested: requested.to_string(),
        applied: applied.map(str::to_string),
        reason: match applied {
            Some(_) => "value not supported by the profile; the default was applied".into(),
            None => "value not supported by the profile; the element was dropped".into(),
        },
    }
}

/// The text of the first `PrintTicket` element in `raw` (an envelope or the XML part of an MTOM
/// message), exactly as sent, or `None`.
pub fn extract_raw_ticket(raw: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(raw);
    let s = text.as_ref();
    let mut from = 0;
    while let Some(rel) = s[from..].find("PrintTicket") {
        let at = from + rel;
        from = at + "PrintTicket".len();
        // `<prefix:PrintTicket` or `<PrintTicket`, followed by whitespace, `>` or `/`.
        let before = &s[..at];
        let open = before.rfind('<')?;
        let prefix = &before[open + 1..];
        let ok_prefix = prefix.is_empty()
            || (prefix.ends_with(':')
                && prefix[..prefix.len() - 1]
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.'));
        if !ok_prefix || prefix.starts_with('/') {
            continue;
        }
        let rest = &s[from..];
        match rest.chars().next() {
            Some(c) if c.is_whitespace() || c == '>' || c == '/' => {}
            _ => continue,
        }
        // self-closing?
        let tag_end = rest.find('>')?;
        if rest[..tag_end].ends_with('/') {
            return Some(s[open..from + tag_end + 1].to_string());
        }
        let close = format!("</{prefix}PrintTicket");
        let c = rest.find(&close)?;
        let end_rel = rest[c..].find('>')?;
        let end = from + c + end_rel + 1;
        let mut out = s[open..end].to_string();
        out.truncate(TICKET_MAX);
        return Some(out);
    }
    None
}

// ---------------------------------------------------------------------------------------------
// The tee on the request body
// ---------------------------------------------------------------------------------------------

struct Tee {
    head: Vec<u8>,
    cap: usize,
    total: u64,
    sha: Sha256,
}

impl Tee {
    fn new(cap: usize) -> Self {
        Self {
            head: Vec::new(),
            cap,
            total: 0,
            sha: Sha256::new(),
        }
    }

    fn feed(&mut self, b: &[u8]) {
        self.total += b.len() as u64;
        self.sha.update(b);
        if self.head.len() < self.cap {
            let room = self.cap - self.head.len();
            self.head.extend_from_slice(&b[..b.len().min(room)]);
        }
    }

    fn finish(self, doc_head: usize, document: bool) -> Payload {
        let mut bytes = self.head;
        let cap = if document { doc_head } else { self.cap };
        bytes.truncate(cap);
        Payload {
            truncated: self.total > bytes.len() as u64,
            length: self.total,
            sha256: (self.total > 0).then(|| self.sha.finish_hex()),
            bytes,
            document,
        }
    }
}

fn lock_tee(t: &Mutex<Tee>) -> MutexGuard<'_, Tee> {
    match t.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

// ---------------------------------------------------------------------------------------------
// Raw request heads from the wire
// ---------------------------------------------------------------------------------------------

/// Where the request came in, set by [`origin_layer`] before the `/d/<id>` dispatch rewrites
/// the URI.
#[derive(Debug, Clone)]
pub struct Origin {
    /// Path and query exactly as sent.
    pub target: String,
    /// The raw head bytes, when the connection was tapped and the head could be matched.
    pub raw_head: Option<Arc<Vec<u8>>>,
}

#[derive(Debug)]
enum TapMode {
    Head,
    Body(u64),
    ChunkSize,
    ChunkData(u64),
    Trailer,
}

struct TapState {
    mode: TapMode,
    buf: Vec<u8>,
    heads: VecDeque<Arc<Vec<u8>>>,
    broken: bool,
}

/// Frames the HTTP/1.1 request heads of one connection from the bytes read off the socket.
pub struct ConnTap {
    st: Mutex<TapState>,
}

const MAX_HEAD: usize = 64 * 1024;
const MAX_QUEUED_HEADS: usize = 64;

impl Default for ConnTap {
    fn default() -> Self {
        Self {
            st: Mutex::new(TapState {
                mode: TapMode::Head,
                buf: Vec::new(),
                heads: VecDeque::new(),
                broken: false,
            }),
        }
    }
}

fn find_crlfcrlf(b: &[u8], from: usize) -> Option<usize> {
    b.get(from..)?
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| from + i)
}

impl ConnTap {
    fn lock(&self) -> MutexGuard<'_, TapState> {
        match self.st.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    /// Feed bytes read from the socket (never fails; a framing surprise just stops the tap).
    pub fn feed(&self, data: &[u8]) {
        let mut st = self.lock();
        if st.broken {
            return;
        }
        let mut pos = 0usize;
        while pos < data.len() {
            match st.mode {
                TapMode::Head => {
                    let old = st.buf.len();
                    st.buf.extend_from_slice(&data[pos..]);
                    if old == 0 && st.buf.starts_with(b"PRI * HTTP/2") {
                        st.broken = true;
                        return;
                    }
                    let from = old.saturating_sub(3);
                    match find_crlfcrlf(&st.buf, from) {
                        Some(i) => {
                            let end = i + 4;
                            let head: Vec<u8> = st.buf[..end].to_vec();
                            pos += end - old;
                            st.buf.clear();
                            let (cl, chunked) = parse_framing(&head);
                            st.heads.push_back(Arc::new(head));
                            while st.heads.len() > MAX_QUEUED_HEADS {
                                st.heads.pop_front();
                            }
                            st.mode = if chunked {
                                TapMode::ChunkSize
                            } else if cl > 0 {
                                TapMode::Body(cl)
                            } else {
                                TapMode::Head
                            };
                        }
                        None => {
                            if st.buf.len() > MAX_HEAD {
                                st.broken = true;
                                st.buf.clear();
                            }
                            return;
                        }
                    }
                }
                TapMode::Body(n) => {
                    let take = (data.len() - pos).min(usize::try_from(n).unwrap_or(usize::MAX));
                    pos += take;
                    let left = n - take as u64;
                    st.mode = if left == 0 {
                        TapMode::Head
                    } else {
                        TapMode::Body(left)
                    };
                }
                TapMode::ChunkSize | TapMode::Trailer => {
                    let is_size = matches!(st.mode, TapMode::ChunkSize);
                    let b = data[pos];
                    pos += 1;
                    st.buf.push(b);
                    if st.buf.len() > 4096 {
                        st.broken = true;
                        st.buf.clear();
                        return;
                    }
                    if b == b'\n' {
                        let line = String::from_utf8_lossy(&st.buf).trim().to_string();
                        st.buf.clear();
                        if is_size {
                            let hex = line.split(';').next().unwrap_or("").trim();
                            match u64::from_str_radix(hex, 16) {
                                Ok(0) => st.mode = TapMode::Trailer,
                                Ok(n) => st.mode = TapMode::ChunkData(n + 2),
                                Err(_) => {
                                    st.broken = true;
                                    return;
                                }
                            }
                        } else if line.is_empty() {
                            st.mode = TapMode::Head;
                        }
                    }
                }
                TapMode::ChunkData(n) => {
                    let take = (data.len() - pos).min(usize::try_from(n).unwrap_or(usize::MAX));
                    pos += take;
                    let left = n - take as u64;
                    st.mode = if left == 0 {
                        TapMode::ChunkSize
                    } else {
                        TapMode::ChunkData(left)
                    };
                }
            }
        }
    }

    /// The oldest queued head whose request line is `method target`; heads before it are
    /// dropped (they were requests that did not reach the capture layer).
    pub fn take(&self, method: &str, target: &str) -> Option<Arc<Vec<u8>>> {
        let mut st = self.lock();
        let idx = st.heads.iter().position(|h| {
            let line_end = h.iter().position(|b| *b == b'\r').unwrap_or(h.len());
            let line = String::from_utf8_lossy(&h[..line_end]);
            let mut it = line.split(' ');
            it.next() == Some(method) && it.next() == Some(target)
        })?;
        let head = st.heads.get(idx).cloned();
        for _ in 0..=idx {
            st.heads.pop_front();
        }
        head
    }

    /// `true` when the tap stopped following the connection.
    pub fn is_broken(&self) -> bool {
        self.lock().broken
    }
}

fn parse_framing(head: &[u8]) -> (u64, bool) {
    let text = String::from_utf8_lossy(head);
    let (mut cl, mut chunked) = (0u64, false);
    for line in text.lines().skip(1) {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_ascii_lowercase();
            if k == "content-length" {
                cl = v.trim().parse().unwrap_or(0);
            } else if k == "transfer-encoding" && v.to_ascii_lowercase().contains("chunked") {
                chunked = true;
            }
        }
    }
    (cl, chunked)
}

/// The taps of the live connections of one server, by peer address.
#[derive(Default)]
pub struct TapRegistry {
    conns: Mutex<HashMap<SocketAddr, Arc<ConnTap>>>,
}

impl TapRegistry {
    fn lock(&self) -> MutexGuard<'_, HashMap<SocketAddr, Arc<ConnTap>>> {
        match self.conns.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    pub fn tap_for(&self, peer: &SocketAddr) -> Option<Arc<ConnTap>> {
        self.lock().get(peer).cloned()
    }

    /// Live tapped connections.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A socket that hands everything it reads to its [`ConnTap`] and otherwise behaves exactly
/// like the inner stream.
pub struct TapStream {
    inner: tokio::net::TcpStream,
    tap: Arc<ConnTap>,
    peer: SocketAddr,
    registry: Arc<TapRegistry>,
}

impl Drop for TapStream {
    fn drop(&mut self) {
        let mut g = self.registry.lock();
        // a reused peer port may already belong to a newer connection
        if g.get(&self.peer).is_some_and(|t| Arc::ptr_eq(t, &self.tap)) {
            g.remove(&self.peer);
        }
    }
}

impl AsyncRead for TapStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let r = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &r {
            let after = buf.filled();
            if after.len() > before {
                self.tap.feed(&after[before..]);
            }
        }
        r
    }
}

impl AsyncWrite for TapStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// A TCP listener whose connections are tapped.
pub struct TapListener {
    inner: tokio::net::TcpListener,
    registry: Arc<TapRegistry>,
}

impl TapListener {
    pub fn new(inner: tokio::net::TcpListener) -> (Self, Arc<TapRegistry>) {
        let registry = Arc::new(TapRegistry::default());
        (
            Self {
                inner,
                registry: Arc::clone(&registry),
            },
            registry,
        )
    }
}

impl axum::serve::Listener for TapListener {
    type Io = TapStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.inner.accept().await {
                Ok((stream, peer)) => {
                    let tap = Arc::new(ConnTap::default());
                    self.registry.lock().insert(peer, Arc::clone(&tap));
                    let io = TapStream {
                        inner: stream,
                        tap,
                        peer,
                        registry: Arc::clone(&self.registry),
                    };
                    return (io, peer);
                }
                Err(e) => {
                    let transient = matches!(
                        e.kind(),
                        std::io::ErrorKind::ConnectionRefused
                            | std::io::ErrorKind::ConnectionAborted
                            | std::io::ErrorKind::ConnectionReset
                    );
                    if !transient {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

/// Outermost middleware of the server router: remember the target as sent and attach the raw
/// head of this request when the connection is tapped.
pub async fn origin_layer(mut req: Request, next: Next) -> Response {
    let target = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| req.uri().path().to_string());
    let mut raw_head = None;
    if let (Some(reg), Some(ConnectInfo(peer))) = (
        req.extensions().get::<Arc<TapRegistry>>().cloned(),
        req.extensions().get::<ConnectInfo<SocketAddr>>().cloned(),
    ) {
        if let Some(tap) = reg.tap_for(&peer) {
            raw_head = tap.take(req.method().as_str(), &target);
        }
    }
    req.extensions_mut().insert(Origin { target, raw_head });
    next.run(req).await
}

/// Split a raw head into (request line parts, ordered headers).
fn parse_raw_head(head: &[u8]) -> Option<(String, Vec<(String, String)>)> {
    let text = String::from_utf8_lossy(head);
    let mut lines = text.split("\r\n");
    let first = lines.next()?.to_string();
    let mut out = Vec::new();
    for l in lines {
        if l.is_empty() {
            break;
        }
        if let Some((k, v)) = l.split_once(':') {
            out.push((k.to_string(), v.trim().to_string()));
        }
    }
    Some((first, out))
}

// ---------------------------------------------------------------------------------------------
// The capture middleware
// ---------------------------------------------------------------------------------------------

fn headers_of(map: &HeaderMap) -> Vec<(String, String)> {
    map.iter()
        .map(|(k, v)| {
            (
                k.as_str().to_string(),
                String::from_utf8_lossy(v.as_bytes()).into_owned(),
            )
        })
        .collect()
}

fn protocol_of(path: &str) -> (&'static str, Option<&'static str>) {
    if path.contains("/ipp/print") {
        ("ipp", Some("ipp"))
    } else if path.ends_with("/WebServices/Device") {
        ("wsd", Some("device"))
    } else if path.ends_with("/WebServices/PrinterService") || path.ends_with("/PrintService") {
        ("wsd", Some("print"))
    } else {
        ("wsd-discovery", Some("discovery"))
    }
}

/// Is a reply body (WSD) a SOAP fault?
fn soap_fault(body: &[u8]) -> bool {
    let head = &body[..body.len().min(4096)];
    String::from_utf8_lossy(head).contains(":Fault>")
        || String::from_utf8_lossy(head).contains("<Fault")
}

/// Per-device capture middleware; mount with `from_fn_with_state(world, capture)`.
pub async fn capture(State(world): State<SharedWorld>, req: Request, next: Next) -> Response {
    let log = { world.read().await.print_ctx().reqlog.clone() };
    if !log.enabled() {
        return next.run(req).await;
    }
    let cfg = log.config();
    let started = Instant::now();
    let wall_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let (parts, body) = req.into_parts();
    let origin = parts.extensions.get::<Origin>().cloned();
    let peer = parts
        .extensions
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(a)| a.to_string());
    let method = parts.method.as_str().to_string();
    let uri_path = parts.uri.path().to_string();
    let (path, query) = match &origin {
        Some(o) => match o.target.split_once('?') {
            Some((p, q)) => (p.to_string(), Some(q.to_string())),
            None => (o.target.clone(), None),
        },
        None => (uri_path.clone(), parts.uri.query().map(str::to_string)),
    };
    let mut version = format!("{:?}", parts.version);
    let mut headers = headers_of(&parts.headers);
    let mut headers_raw = false;
    let raw_head = origin.as_ref().and_then(|o| o.raw_head.clone());
    if let Some(h) = &raw_head {
        if let Some((line, hs)) = parse_raw_head(h) {
            headers = hs;
            headers_raw = true;
            if let Some(v) = line.rsplit(' ').next() {
                if v.starts_with("HTTP/") {
                    version = v.to_string();
                }
            }
        }
    }

    let tee = Arc::new(Mutex::new(Tee::new(cfg.body_cap)));
    let teed = Arc::clone(&tee);
    let stream = body.into_data_stream().map(move |r| {
        if let Ok(b) = &r {
            lock_tee(&teed).feed(b);
        }
        r
    });
    let (proto, endpoint) = protocol_of(&path);
    let note = Arc::new(Note::default());
    note.with(|n| {
        n.protocol = Some(proto.into());
        n.endpoint = endpoint.map(str::to_string);
    });
    log.in_flight.fetch_add(1, Ordering::Relaxed);
    let req = Request::from_parts(parts, Body::from_stream(stream));
    let resp = TASK_NOTE.scope(Arc::clone(&note), next.run(req)).await;
    log.in_flight.fetch_sub(1, Ordering::Relaxed);

    let (rparts, rbody) = resp.into_parts();
    let collected = axum::body::to_bytes(rbody, MAX_RESPONSE_BUFFER).await;
    let latency_us = started.elapsed().as_micros() as u64;
    let nd = note.take();
    let (rbytes, mut dropped) = match &collected {
        Ok(b) => (b.clone(), false),
        Err(_) => (Default::default(), true),
    };
    dropped |= nd.dropped;
    let document = nd.document;
    let req_payload = {
        let taken = std::mem::replace(&mut *lock_tee(&tee), Tee::new(0));
        taken.finish(cfg.doc_head, document)
    };
    let status = rparts.status.as_u16();
    let faulted = nd.faulted
        || !rparts.status.is_success()
        || (nd.protocol.as_deref() != Some("ipp") && soap_fault(&rbytes));
    let outcome = if dropped {
        Outcome::Dropped
    } else if faulted {
        Outcome::Faulted
    } else {
        Outcome::Ok
    };
    let rec = Record {
        seq: 0,
        direction: Direction::In,
        t_mono_ms: log.mono_ms(started),
        t_wall_unix_ms: wall_ms,
        peer: if cfg.redact {
            Some("redacted".into())
        } else {
            peer
        },
        method,
        path,
        query,
        version,
        headers: redact_headers(headers, cfg.redact),
        headers_raw,
        body: req_payload,
        protocol: nd.protocol.unwrap_or_else(|| proto.into()),
        endpoint: nd.endpoint.or_else(|| endpoint.map(str::to_string)),
        action: nd.action,
        action_uri: nd.action_uri,
        message_id: nd.message_id,
        request_id: nd.request_id,
        ipp_operation: nd.ipp_operation,
        ipp_operation_id: nd.ipp_operation_id,
        ipp_status: nd.ipp_status,
        job_id: nd.job_id,
        status: (!dropped).then_some(status),
        response_headers: redact_headers(headers_of(&rparts.headers), cfg.redact),
        response_body: Payload::from_slice(&rbytes, cfg.body_cap),
        latency_us,
        fault_rule: nd.fault_rule,
        outcome,
        ticket_received: nd.ticket_raw,
        substitutions: nd.substitutions,
        ipp: nd.ipp,
        raw_head: if cfg.redact { None } else { raw_head },
    };
    log.push(rec);
    if dropped {
        return crate::wsd::aborted_response();
    }
    Response::from_parts(rparts, Body::from(rbytes))
}

// ---------------------------------------------------------------------------------------------
// Control API
// ---------------------------------------------------------------------------------------------

/// Query string of `GET /v1/requests`.
#[derive(Debug, Default, Deserialize)]
pub struct ListParams {
    pub since: Option<u64>,
    pub limit: Option<usize>,
    pub action: Option<String>,
    pub job: Option<u32>,
    pub direction: Option<String>,
    /// `0` / `false`: leave the bodies (base64 / text) out.
    pub bodies: Option<String>,
}

async fn api_log(world: &SharedWorld) -> Arc<RequestLog> {
    world.read().await.print_ctx().reqlog.clone()
}

/// `GET /v1/requests`.
pub async fn list_handler(
    State(world): State<SharedWorld>,
    Query(p): Query<ListParams>,
) -> Response {
    let log = api_log(&world).await;
    let bodies = !matches!(p.bodies.as_deref(), Some("0" | "false" | "no"));
    let q = RequestQuery {
        since: p.since,
        limit: p.limit.unwrap_or(100).clamp(1, 10_000),
        action: p.action.filter(|a| !a.is_empty()),
        job: p.job,
        direction: p.direction.filter(|a| !a.is_empty()),
    };
    let recs = log.query(&q);
    let mut out = log.stats_json();
    if let Some(o) = out.as_object_mut() {
        o.insert(
            "records".into(),
            Value::Array(recs.iter().map(|r| r.to_json(bodies)).collect()),
        );
    }
    json_response(StatusCode::OK, &out)
}

/// `GET /v1/requests/{seq}`.
pub async fn one_handler(
    State(world): State<SharedWorld>,
    axum::extract::Path(seq): axum::extract::Path<u64>,
) -> Response {
    let log = api_log(&world).await;
    match log.get(seq) {
        Some(r) => json_response(StatusCode::OK, &r.to_json(true)),
        None => json_response(
            StatusCode::NOT_FOUND,
            &json!({"error": "no such record (never existed or already dropped)"}),
        ),
    }
}

/// Query string of `GET /v1/requests/{seq}/raw`.
#[derive(Debug, Default, Deserialize)]
pub struct RawParams {
    /// `request` (default) or `response`.
    pub part: Option<String>,
    /// `all` (default: head + body) or `body` (the stored body bytes only).
    pub what: Option<String>,
    /// `base64`: answer JSON `{length, truncated, sha256, base64}` instead of the bytes (for
    /// callers that only speak JSON, such as the embedded in-process control call).
    pub encoding: Option<String>,
}

/// `GET /v1/requests/{seq}/raw`.
pub async fn raw_handler(
    State(world): State<SharedWorld>,
    axum::extract::Path(seq): axum::extract::Path<u64>,
    Query(p): Query<RawParams>,
) -> Response {
    let log = api_log(&world).await;
    let Some(r) = log.get(seq) else {
        return json_response(
            StatusCode::NOT_FOUND,
            &json!({"error": "no such record (never existed or already dropped)"}),
        );
    };
    let response = matches!(p.part.as_deref(), Some("response" | "resp"));
    let body_only = matches!(p.what.as_deref(), Some("body"));
    let (bytes, payload) = if response {
        (r.raw_response(), &r.response_body)
    } else {
        (r.raw_request(), &r.body)
    };
    let bytes = if body_only {
        payload.bytes.clone()
    } else {
        bytes
    };
    if matches!(p.encoding.as_deref(), Some("base64")) {
        return json_response(
            StatusCode::OK,
            &json!({
                "seq": seq,
                "part": if response { "response" } else { "request" },
                "what": if body_only { "body" } else { "all" },
                "size": bytes.len(),
                "length": payload.length,
                "truncated": payload.truncated,
                "sha256": payload.sha256,
                "base64": base64::engine::general_purpose::STANDARD.encode(&bytes),
            }),
        );
    }
    let mut resp = (StatusCode::OK, bytes).into_response();
    let h = resp.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    for (k, v) in [
        ("x-fakeprint-truncated", payload.truncated.to_string()),
        ("x-fakeprint-length", payload.length.to_string()),
        (
            "x-fakeprint-sha256",
            payload.sha256.clone().unwrap_or_default(),
        ),
    ] {
        if let Ok(v) = HeaderValue::from_str(&v) {
            h.insert(k, v);
        }
    }
    resp
}

/// `DELETE /v1/requests`.
pub async fn clear_handler(State(world): State<SharedWorld>) -> Response {
    let log = api_log(&world).await;
    let n = log.len();
    log.clear();
    json_response(
        StatusCode::OK,
        &json!({"cleared": n, "next_seq": log.next_seq()}),
    )
}

/// `GET /v1/requests/config`.
pub async fn config_get(State(world): State<SharedWorld>) -> Response {
    let log = api_log(&world).await;
    json_response(StatusCode::OK, &json!(log.config()))
}

/// `POST /v1/requests/config` (a partial object).
pub async fn config_post(State(world): State<SharedWorld>, raw: axum::body::Bytes) -> Response {
    let patch: RequestLogPatch = if raw.is_empty() {
        RequestLogPatch::default()
    } else {
        match serde_json::from_slice(&raw) {
            Ok(p) => p,
            Err(e) => {
                return json_response(StatusCode::BAD_REQUEST, &json!({"error": e.to_string()}))
            }
        }
    };
    let log = api_log(&world).await;
    let c = log.patch(&patch);
    json_response(StatusCode::OK, &json!(c))
}

fn json_response(status: StatusCode, v: &Value) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        v.to_string(),
    )
        .into_response()
}

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_known_vectors() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            sha256_hex(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
        let mut s = Sha256::new();
        for _ in 0..1000 {
            s.update(&[b'a'; 1000]);
        }
        assert_eq!(
            s.finish_hex(),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
        );
    }

    #[test]
    fn sha256_incremental_matches_one_shot() {
        let data: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let mut s = Sha256::new();
        for c in data.chunks(77) {
            s.update(c);
        }
        assert_eq!(s.finish_hex(), sha256_hex(&data));
    }

    #[test]
    fn iso8601_known_dates() {
        assert_eq!(iso8601_ms(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso8601_ms(1_700_000_000_123), "2023-11-14T22:13:20.123Z");
    }

    #[test]
    fn raw_ticket_is_cut_out_as_sent() {
        let env = "<s:Envelope xmlns:s='x'><s:Body><wprt:CreatePrintJobRequest><wprt:PrintTicket><wprt:JobDescription><wprt:JobName>a</wprt:JobName></wprt:JobDescription></wprt:PrintTicket></wprt:CreatePrintJobRequest></s:Body></s:Envelope>";
        let t = extract_raw_ticket(env.as_bytes()).unwrap();
        assert!(t.starts_with("<wprt:PrintTicket>") && t.ends_with("</wprt:PrintTicket>"));
        assert!(t.contains("<wprt:JobName>a</wprt:JobName>"));
        let bare = "<PrintTicket xmlns='n'><X/></PrintTicket>";
        assert_eq!(extract_raw_ticket(bare.as_bytes()).unwrap(), bare);
        assert_eq!(extract_raw_ticket(b"<a/>"), None);
        assert_eq!(
            extract_raw_ticket(b"<p:PrintTicket/>").unwrap(),
            "<p:PrintTicket/>"
        );
    }

    fn rec(job: Option<u32>, body: usize) -> Record {
        Record {
            seq: 0,
            direction: Direction::In,
            t_mono_ms: 0.0,
            t_wall_unix_ms: 0,
            peer: None,
            method: "POST".into(),
            path: "/x".into(),
            query: None,
            version: "HTTP/1.1".into(),
            headers: vec![],
            headers_raw: false,
            body: Payload::from_slice(&vec![b'x'; body], usize::MAX),
            protocol: "wsd".into(),
            endpoint: None,
            action: Some("A".into()),
            action_uri: None,
            message_id: None,
            request_id: None,
            ipp_operation: None,
            ipp_operation_id: None,
            ipp_status: None,
            job_id: job,
            status: Some(200),
            response_headers: vec![],
            response_body: Payload::default(),
            latency_us: 0,
            fault_rule: None,
            outcome: Outcome::Ok,
            ticket_received: None,
            substitutions: vec![],
            ipp: None,
            raw_head: None,
        }
    }

    #[test]
    fn ring_drops_oldest_and_seq_stays_monotonic() {
        let log = RequestLog::new(RequestLogConfig {
            capacity: 3,
            ..RequestLogConfig::default()
        });
        let seqs: Vec<u64> = (0..7).map(|_| log.push(rec(None, 1))).collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6, 7]);
        let held: Vec<u64> = log
            .query(&RequestQuery::default())
            .iter()
            .map(|r| r.seq)
            .collect();
        assert_eq!(held, vec![5, 6, 7]);
        assert_eq!(log.dropped(), 4);
        log.clear();
        assert!(log.is_empty());
        assert_eq!(log.push(rec(None, 1)), 8);
    }

    #[test]
    fn byte_budget_evicts_before_the_count() {
        let log = RequestLog::new(RequestLogConfig {
            capacity: 100,
            max_bytes: 2000,
            ..RequestLogConfig::default()
        });
        for _ in 0..10 {
            log.push(rec(None, 600));
        }
        assert!(log.len() < 10 && !log.is_empty());
        assert!(log.stats_json()["stored_bytes"].as_u64().unwrap() <= 2000 + 1000);
    }

    #[test]
    fn queries_filter_and_job_links_are_kept() {
        let log = RequestLog::default();
        log.push(rec(None, 1));
        log.push(rec(Some(7), 1));
        log.push(rec(Some(7), 1));
        let by_job = log.query(&RequestQuery {
            job: Some(7),
            ..RequestQuery::default()
        });
        assert_eq!(by_job.len(), 2);
        let since = log.query(&RequestQuery {
            since: Some(2),
            ..RequestQuery::default()
        });
        assert_eq!(since.len(), 1);
        let lim = log.query(&RequestQuery {
            limit: 1,
            ..RequestQuery::default()
        });
        assert_eq!(lim.len(), 1);
        assert_eq!(log.job_meta(7).unwrap().request_seqs, vec![2, 3]);
        log.record_ticket(
            7,
            Some("<T/>".into()),
            vec![substitution("Sides", "x", None)],
        );
        assert_eq!(
            log.job_meta(7).unwrap().ticket_received.as_deref(),
            Some("<T/>")
        );
    }

    #[test]
    fn disabled_log_records_nothing_for_tickets() {
        let log = RequestLog::new(RequestLogConfig::off());
        assert!(!log.enabled());
        log.record_ticket(1, Some("x".into()), vec![]);
        assert!(log.job_meta(1).is_none());
    }

    #[test]
    fn config_patch_normalizes() {
        let log = RequestLog::default();
        let c = log.patch(&RequestLogPatch {
            capacity: Some(0),
            ..RequestLogPatch::default()
        });
        assert!(!c.enabled);
        let c = log.patch(&RequestLogPatch {
            capacity: Some(5),
            enabled: Some(true),
            ..RequestLogPatch::default()
        });
        assert!(c.enabled && c.capacity == 5);
    }

    #[test]
    fn tee_keeps_a_head_and_hashes_everything() {
        let mut t = Tee::new(10);
        t.feed(b"0123456");
        t.feed(b"789abcdef");
        let p = t.finish(4, false);
        assert_eq!(p.bytes, b"0123456789");
        assert_eq!(p.length, 16);
        assert!(p.truncated);
        assert_eq!(p.sha256.unwrap(), sha256_hex(b"0123456789abcdef"));
        let mut t = Tee::new(10);
        t.feed(b"0123456789");
        let p = t.finish(4, true);
        assert_eq!(p.bytes, b"0123");
        assert!(p.document && p.truncated);
    }

    fn raw(method: &str, target: &str, extra: &str, body: &str) -> Vec<u8> {
        format!(
            "{method} {target} HTTP/1.1\r\nHost: h\r\n{extra}Content-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    #[test]
    fn tap_frames_heads_across_reads_and_bodies() {
        let tap = ConnTap::default();
        let mut wire = raw("POST", "/a", "User-Agent: Zed\r\n", "hello");
        wire.extend(raw("GET", "/b?q=1", "", ""));
        // one byte at a time: the worst-case split
        for b in &wire {
            tap.feed(std::slice::from_ref(b));
        }
        let a = tap.take("POST", "/a").unwrap();
        assert!(String::from_utf8_lossy(&a).contains("User-Agent: Zed"));
        let b = tap.take("GET", "/b?q=1").unwrap();
        assert!(String::from_utf8_lossy(&b).starts_with("GET /b?q=1 HTTP/1.1"));
        assert!(tap.take("GET", "/b?q=1").is_none());
    }

    #[test]
    fn tap_follows_chunked_bodies() {
        let tap = ConnTap::default();
        let mut wire =
            b"POST /c HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n3;x=y\r\nabc\r\n0\r\nT: v\r\n\r\n"
                .to_vec();
        wire.extend(raw("GET", "/d", "", ""));
        tap.feed(&wire);
        assert!(tap.take("POST", "/c").is_some());
        assert!(tap.take("GET", "/d").is_some());
        assert!(!tap.is_broken());
    }

    #[test]
    fn tap_skips_heads_of_requests_that_never_reached_the_layer() {
        let tap = ConnTap::default();
        let mut wire = raw("GET", "/not-captured", "", "");
        wire.extend(raw("POST", "/captured", "", "x"));
        tap.feed(&wire);
        assert!(tap.take("POST", "/captured").is_some());
    }

    #[test]
    fn tap_gives_up_on_http2_and_oversized_heads() {
        let tap = ConnTap::default();
        tap.feed(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        assert!(tap.is_broken() && tap.take("PRI", "*").is_none());
        let tap = ConnTap::default();
        tap.feed(&vec![b'a'; MAX_HEAD + 10]);
        assert!(tap.is_broken());
    }

    #[test]
    fn raw_head_keeps_case_and_order() {
        let h = b"POST /p HTTP/1.1\r\nuser-AGENT: x\r\nHost: h\r\nX-Dup: 1\r\nX-Dup: 2\r\n\r\n";
        let (line, hs) = parse_raw_head(h).unwrap();
        assert_eq!(line, "POST /p HTTP/1.1");
        assert_eq!(
            hs,
            vec![
                ("user-AGENT".into(), "x".into()),
                ("Host".into(), "h".into()),
                ("X-Dup".into(), "1".into()),
                ("X-Dup".into(), "2".into())
            ]
        );
    }

    #[test]
    fn redaction_masks_credentials() {
        let h = redact_headers(
            vec![
                ("Authorization".into(), "Basic abc".into()),
                ("Host".into(), "h".into()),
            ],
            true,
        );
        assert_eq!(h[0].1, "redacted");
        assert_eq!(h[1].1, "h");
    }

    #[test]
    fn note_reaches_thread_local_scope() {
        let n = Arc::new(Note::default());
        assert!(current().is_none());
        with_note(Some(Arc::clone(&n)), || {
            annotate(|d| d.job_id = Some(9));
        });
        assert_eq!(n.with(|d| d.job_id), Some(9));
        assert!(current().is_none());
    }
}
