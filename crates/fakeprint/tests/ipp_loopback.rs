//! The IPP face over a real loopback server: every operation, the error codes, filtering, the
//! multiple-operation timeout on the fake clock, cancel races, shared job rows with WS-Print,
//! per-device routing, the injection effects and the reply validator.
//!
//! The client here is an independent byte-level encoder and decoder (nothing from
//! `fakeprint::ipp::codec`), so the server's codec is checked against a second reading of
//! RFC 8010. Reply validation is on: each test ends by asserting zero problems.

mod common;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use fakeprint::profile::PrinterProfile;
use fakeprint::server::{serve, DeviceConfig, ServerConfig, ServerHandle};
use fakeprint::wsd::ctx::{ManualClock, PrintTiming};
use serde_json::json;

// ---------------------------------------------------------------------------------------------
// A tiny independent IPP client
// ---------------------------------------------------------------------------------------------

const OP_PRINT_JOB: u16 = 0x02;
const OP_VALIDATE_JOB: u16 = 0x04;
const OP_CREATE_JOB: u16 = 0x05;
const OP_SEND_DOCUMENT: u16 = 0x06;
const OP_CANCEL_JOB: u16 = 0x08;
const OP_GET_JOB_ATTRIBUTES: u16 = 0x09;
const OP_GET_JOBS: u16 = 0x0a;
const OP_GET_PRINTER_ATTRIBUTES: u16 = 0x0b;

const OK: u16 = 0x0000;
const OK_IGNORED: u16 = 0x0001;
const BAD_REQUEST: u16 = 0x0400;
const NOT_POSSIBLE: u16 = 0x0404;
const NOT_FOUND: u16 = 0x0406;
const FORMAT_NOT_SUPPORTED: u16 = 0x040a;
const ATTRS_NOT_SUPPORTED: u16 = 0x040b;
const OP_NOT_SUPPORTED: u16 = 0x0501;
const NOT_ACCEPTING: u16 = 0x0506;
const BUSY: u16 = 0x0507;

fn attr(tag: u8, name: &str, value: &[u8]) -> Vec<u8> {
    let mut v = vec![tag];
    v.extend_from_slice(&(name.len() as u16).to_be_bytes());
    v.extend_from_slice(name.as_bytes());
    v.extend_from_slice(&(value.len() as u16).to_be_bytes());
    v.extend_from_slice(value);
    v
}

fn kw(name: &str, v: &str) -> Vec<u8> {
    attr(0x44, name, v.as_bytes())
}
fn more_kw(v: &str) -> Vec<u8> {
    attr(0x44, "", v.as_bytes())
}
fn uri(name: &str, v: &str) -> Vec<u8> {
    attr(0x45, name, v.as_bytes())
}
fn nm(name: &str, v: &str) -> Vec<u8> {
    attr(0x42, name, v.as_bytes())
}
fn mime(name: &str, v: &str) -> Vec<u8> {
    attr(0x49, name, v.as_bytes())
}
fn int(name: &str, v: i32) -> Vec<u8> {
    attr(0x21, name, &v.to_be_bytes())
}
fn en(name: &str, v: i32) -> Vec<u8> {
    attr(0x23, name, &v.to_be_bytes())
}
fn boolean(name: &str, v: bool) -> Vec<u8> {
    attr(0x22, name, &[u8::from(v)])
}

struct Req {
    version: (u8, u8),
    op: u16,
    id: u32,
    op_attrs: Vec<Vec<u8>>,
    job_attrs: Vec<Vec<u8>>,
    data: Vec<u8>,
    lead: bool,
}

impl Req {
    fn new(op: u16, printer: &str) -> Req {
        Req {
            version: (2, 0),
            op,
            id: 1,
            op_attrs: vec![uri("printer-uri", printer)],
            job_attrs: vec![],
            data: vec![],
            lead: true,
        }
    }
    fn with(mut self, a: Vec<u8>) -> Req {
        self.op_attrs.push(a);
        self
    }
    fn job(mut self, a: Vec<u8>) -> Req {
        self.job_attrs.push(a);
        self
    }
    fn data(mut self, d: &[u8]) -> Req {
        self.data = d.to_vec();
        self
    }
    fn bytes(&self) -> Vec<u8> {
        let mut b = vec![self.version.0, self.version.1];
        b.extend_from_slice(&self.op.to_be_bytes());
        b.extend_from_slice(&self.id.to_be_bytes());
        b.push(0x01);
        if self.lead {
            b.extend(attr(0x47, "attributes-charset", b"utf-8"));
            b.extend(attr(0x48, "attributes-natural-language", b"en"));
        }
        for a in &self.op_attrs {
            b.extend_from_slice(a);
        }
        if !self.job_attrs.is_empty() {
            b.push(0x02);
            for a in &self.job_attrs {
                b.extend_from_slice(a);
            }
        }
        b.push(0x03);
        b.extend_from_slice(&self.data);
        b
    }
}

type Values = Vec<(u8, Vec<u8>)>;

struct IppResp {
    version: (u8, u8),
    status: u16,
    id: u32,
    groups: Vec<(u8, Vec<(String, Values)>)>,
}

impl IppResp {
    fn group(&self, tag: u8) -> Option<&Vec<(String, Values)>> {
        self.groups.iter().find(|g| g.0 == tag).map(|g| &g.1)
    }
    fn get(&self, tag: u8, name: &str) -> Option<&Values> {
        self.group(tag)?.iter().find(|a| a.0 == name).map(|a| &a.1)
    }
    fn int(&self, tag: u8, name: &str) -> Option<i32> {
        let v = self.get(tag, name)?.first()?;
        (v.1.len() == 4).then(|| i32::from_be_bytes([v.1[0], v.1[1], v.1[2], v.1[3]]))
    }
    fn strs(&self, tag: u8, name: &str) -> Vec<String> {
        self.get(tag, name)
            .map(|v| {
                v.iter()
                    .map(|x| String::from_utf8_lossy(&x.1).into_owned())
                    .collect()
            })
            .unwrap_or_default()
    }
    fn job_groups(&self) -> Vec<&Vec<(String, Values)>> {
        self.groups
            .iter()
            .filter(|g| g.0 == 0x02)
            .map(|g| &g.1)
            .collect()
    }
    fn job_id(&self) -> u32 {
        u32::try_from(self.int(0x02, "job-id").expect("job-id")).unwrap()
    }
    fn job_state(&self) -> i32 {
        self.int(0x02, "job-state").expect("job-state")
    }
    fn unsupported_names(&self) -> Vec<String> {
        self.group(0x05)
            .map(|g| g.iter().map(|a| a.0.clone()).collect())
            .unwrap_or_default()
    }
}

fn job_name(g: &[(String, Values)]) -> String {
    let v = &g.iter().find(|a| a.0 == "job-name").expect("job-name").1[0].1;
    String::from_utf8_lossy(v).into_owned()
}

fn decode(b: &[u8]) -> IppResp {
    assert!(b.len() >= 9, "short IPP reply: {b:?}");
    let mut groups: Vec<(u8, Vec<(String, Values)>)> = Vec::new();
    let mut i = 8;
    loop {
        let t = b[i];
        i += 1;
        if t == 0x03 {
            break;
        }
        if t < 0x10 {
            groups.push((t, vec![]));
            continue;
        }
        let nl = u16::from_be_bytes([b[i], b[i + 1]]) as usize;
        i += 2;
        let name = String::from_utf8_lossy(&b[i..i + nl]).into_owned();
        i += nl;
        let vl = u16::from_be_bytes([b[i], b[i + 1]]) as usize;
        i += 2;
        let val = b[i..i + vl].to_vec();
        i += vl;
        let g = &mut groups.last_mut().expect("attribute before a group").1;
        if name.is_empty() {
            g.last_mut().expect("continuation").1.push((t, val));
        } else {
            g.push((name, vec![(t, val)]));
        }
    }
    assert_eq!(i, b.len(), "nothing follows a reply's end-of-attributes");
    IppResp {
        version: (b[0], b[1]),
        status: u16::from_be_bytes([b[2], b[3]]),
        id: u32::from_be_bytes([b[4], b[5], b[6], b[7]]),
        groups,
    }
}

async fn post(addr: SocketAddr, path: &str, body: Vec<u8>) -> IppResp {
    let r = request(
        addr,
        "POST",
        path,
        vec![("Content-Type".into(), "application/ipp".into())],
        body,
    )
    .await;
    assert_eq!(r.status, 200, "HTTP {}: {}", r.status, r.text());
    decode(&r.body)
}

// ---------------------------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------------------------

struct Rig {
    srv: ServerHandle,
    clock: Arc<ManualClock>,
    _tmp: TempDir,
    addr: SocketAddr,
    uri: String,
}

fn profile(id: &str) -> PrinterProfile {
    PrinterProfile::load(&profiles_dir(), id).expect("shipped profile")
}

async fn rig_with(profile_id: &str, timing: PrintTiming) -> Rig {
    let tmp = TempDir::new("ipp");
    let clock = Arc::new(ManualClock::new());
    let mut cfg = ServerConfig::embedded(tmp.path().join("inbox"));
    cfg.profile = Some(profile(profile_id));
    cfg.timing = timing;
    cfg.clock = Some(clock.clone());
    cfg.tick = Duration::from_millis(20);
    let srv = serve(cfg).await.expect("server starts");
    let addr = srv.local_addr();
    Rig {
        uri: format!("ipp://127.0.0.1:{}/ipp/print", addr.port()),
        srv,
        clock,
        _tmp: tmp,
        addr,
    }
}

async fn rig() -> Rig {
    rig_with("generic-wsd", PrintTiming::instant()).await
}

impl Rig {
    fn req(&self, op: u16) -> Req {
        Req::new(op, &self.uri)
    }
    async fn send(&self, r: Req) -> IppResp {
        post(self.addr, "/ipp/print", r.bytes()).await
    }
    async fn print(&self, r: Req) -> IppResp {
        self.send(r.data(b"%PDF-1.4 fake")).await
    }
    async fn state(&self, id: u32) -> (i32, Vec<String>) {
        let r = self
            .send(
                self.req(OP_GET_JOB_ATTRIBUTES)
                    .with(int("job-id", id as i32)),
            )
            .await;
        assert_eq!(r.status, OK, "get-job-attributes");
        (r.job_state(), r.strs(0x02, "job-state-reasons"))
    }
    fn advance(&self, ms: u64) {
        self.clock.advance(Duration::from_millis(ms));
    }
    async fn finish(self) {
        assert_eq!(
            self.srv.ipp_validation_problems().await,
            Vec::<String>::new(),
            "the server's own replies must satisfy RFC 8011"
        );
        self.srv.shutdown().await;
    }
}

// ---------------------------------------------------------------------------------------------
// Printer attributes
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_printer_attributes_reports_the_profile_and_the_live_status() {
    let r = rig_with("multi-format", PrintTiming::instant()).await;
    let resp = r.send(r.req(OP_GET_PRINTER_ATTRIBUTES)).await;
    assert_eq!((resp.status, resp.version, resp.id), (OK, (2, 0), 1));
    let g = |n: &str| resp.get(0x04, n).unwrap_or_else(|| panic!("{n} missing"));
    assert_eq!(
        resp.strs(0x04, "printer-uri-supported"),
        std::slice::from_ref(&r.uri)
    );
    assert_eq!(
        resp.strs(0x04, "ipp-versions-supported"),
        ["1.0", "1.1", "2.0"]
    );
    assert_eq!(
        resp.strs(0x04, "ipp-features-supported"),
        ["ipp-everywhere"]
    );
    assert_eq!(resp.int(0x04, "printer-state"), Some(3));
    assert_eq!(resp.strs(0x04, "printer-state-reasons"), ["none"]);
    assert_eq!(resp.strs(0x04, "uri-authentication-supported"), ["none"]);
    assert_eq!(resp.strs(0x04, "uri-security-supported"), ["none"]);
    // the formats are exactly the profile's list
    let formats = resp.strs(0x04, "document-format-supported");
    for f in [
        "image/jpeg",
        "image/png",
        "image/tiff",
        "application/pdf",
        "image/pwg-raster",
        "image/urf",
        "application/postscript",
        "text/plain",
        "application/octet-stream",
    ] {
        assert!(formats.contains(&f.to_string()), "{f} in {formats:?}");
    }
    assert_eq!(formats, profile("multi-format").documents.formats);
    assert_eq!(
        resp.strs(0x04, "document-format-default"),
        ["application/octet-stream"]
    );
    assert!(resp.strs(0x04, "printer-uuid")[0].starts_with("urn:uuid:00000000-0000-4000-8000-"));
    assert_eq!(resp.strs(0x04, "sides-supported").len(), 3);
    assert_eq!(g("printer-resolution-supported").len(), 3);
    assert_eq!(resp.int(0x04, "multiple-operation-time-out"), Some(120));
    let ops = g("operations-supported");
    assert_eq!(ops.len(), 8);
    assert!(resp.get(0x04, "marker-levels").is_some());
    assert_eq!(
        g("marker-names").len(),
        g("marker-levels").len(),
        "marker lists are parallel"
    );
    assert!(resp
        .strs(0x04, "job-creation-attributes-supported")
        .contains(&"copies".to_string()));
    assert!(resp.get(0x04, "media-col-ready").is_some());
    assert!(resp.int(0x04, "printer-up-time").unwrap() >= 1);
    assert_eq!(g("printer-current-time")[0].1.len(), 11);

    // requested-attributes: names, groups, unknown names
    let one = r
        .send(
            r.req(OP_GET_PRINTER_ATTRIBUTES)
                .with(kw("requested-attributes", "queued-job-count")),
        )
        .await;
    assert_eq!(one.group(0x04).unwrap().len(), 1);
    let tpl = r
        .send(
            r.req(OP_GET_PRINTER_ATTRIBUTES)
                .with(kw("requested-attributes", "job-template")),
        )
        .await;
    assert!(tpl.get(0x04, "sides-default").is_some() && tpl.get(0x04, "printer-name").is_none());
    let desc = r
        .send(
            r.req(OP_GET_PRINTER_ATTRIBUTES)
                .with(kw("requested-attributes", "printer-description")),
        )
        .await;
    assert!(desc.get(0x04, "printer-name").is_some() && desc.get(0x04, "sides-default").is_none());
    let all = r
        .send(
            r.req(OP_GET_PRINTER_ATTRIBUTES)
                .with(kw("requested-attributes", "all")),
        )
        .await;
    assert!(all.get(0x04, "printer-name").is_some() && all.get(0x04, "sides-default").is_some());
    let unk = r
        .send(
            r.req(OP_GET_PRINTER_ATTRIBUTES)
                .with(kw("requested-attributes", "printer-state"))
                .with(more_kw("printer-bogus")),
        )
        .await;
    assert_eq!(unk.status, OK_IGNORED);
    assert_eq!(unk.unsupported_names(), ["requested-attributes"]);
    assert_eq!(unk.strs(0x05, "requested-attributes"), ["printer-bogus"]);
    assert_eq!(
        unk.groups.iter().map(|g| g.0).collect::<Vec<_>>(),
        [1, 5, 4]
    );
    r.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn printer_state_reasons_follow_the_fault_flags() {
    let r = rig().await;
    let reasons = |resp: &IppResp| resp.strs(0x04, "printer-state-reasons");
    let ask = || {
        r.req(OP_GET_PRINTER_ATTRIBUTES)
            .with(kw("requested-attributes", "printer-state"))
            .with(more_kw("printer-state-reasons"))
            .with(more_kw("printer-is-accepting-jobs"))
    };
    let resp = r.send(ask()).await;
    assert_eq!(reasons(&resp), ["none"]);
    let p = post_json(
        r.addr,
        "/v1/faults",
        &json!({"paper_jam": true, "cover_open": true, "toner_low": true}),
    )
    .await;
    assert_eq!(p.status, 200, "{}", p.text());
    let resp = r.send(ask()).await;
    assert_eq!(resp.int(0x04, "printer-state"), Some(5));
    let rs = reasons(&resp);
    for want in [
        "media-jam-error",
        "cover-open-error",
        "marker-supply-low-warning",
    ] {
        assert!(rs.contains(&want.to_string()), "{want} in {rs:?}");
    }
    post_json(
        r.addr,
        "/v1/faults",
        &json!({"reset": true, "not_accepting_jobs": true}),
    )
    .await;
    let resp = r.send(ask()).await;
    assert!(reasons(&resp).contains(&"offline-report".to_string()));
    assert_eq!(
        resp.get(0x04, "printer-is-accepting-jobs").unwrap()[0].1,
        [0]
    );
    for op in [OP_PRINT_JOB, OP_CREATE_JOB, OP_VALIDATE_JOB] {
        let resp = r.send(r.req(op).data(b"%PDF")).await;
        assert_eq!(resp.status, NOT_ACCEPTING, "op {op}");
    }
    r.finish().await;
}

// ---------------------------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn print_job_lifecycle_and_the_shared_job_rows() {
    let r = rig_with("generic-wsd", PrintTiming::realistic()).await;
    let resp = r
        .print(
            r.req(OP_PRINT_JOB)
                .with(nm("requesting-user-name", "alice"))
                .with(nm("job-name", "report"))
                .with(mime("document-format", "application/pdf"))
                .job(int("copies", 2))
                .job(kw("sides", "one-sided"))
                .job(en("print-quality", 4)),
        )
        .await;
    assert_eq!(resp.status, OK);
    let id = resp.job_id();
    assert!(resp.strs(0x02, "job-uri")[0].ends_with(&format!("/ipp/print/{id}")));
    assert!(resp.strs(0x02, "job-uuid")[0].starts_with("urn:uuid:00000000-0000-4000-8000-"));
    // realistic timing: processing for a while, then completed
    let (st, reasons) = r.state(id).await;
    assert_eq!(st, 5, "{reasons:?}");
    assert_eq!(reasons, ["job-printing"]);
    r.advance(1_000);
    let mid = r
        .send(r.req(OP_GET_JOB_ATTRIBUTES).with(int("job-id", id as i32)))
        .await;
    assert!(mid.int(0x02, "time-at-processing").is_some());
    assert!(
        mid.get(0x02, "time-at-completed").unwrap()[0].0 == 0x13,
        "no-value while running"
    );
    r.advance(10_000);
    let done = r
        .send(r.req(OP_GET_JOB_ATTRIBUTES).with(int("job-id", id as i32)))
        .await;
    assert_eq!(done.job_state(), 9);
    assert_eq!(
        done.strs(0x02, "job-state-reasons"),
        ["job-completed-successfully"]
    );
    assert_eq!(done.strs(0x02, "job-name"), ["report"]);
    assert_eq!(done.strs(0x02, "job-originating-user-name"), ["alice"]);
    assert_eq!(done.int(0x02, "number-of-documents"), Some(1));
    assert_eq!(done.int(0x02, "copies"), Some(2));
    assert!(
        done.int(0x02, "job-media-sheets-completed").unwrap() >= 2,
        "two copies"
    );
    assert!(done.int(0x02, "job-impressions-completed").unwrap() >= 2);
    assert!(
        done.int(0x02, "time-at-completed").unwrap()
            >= done.int(0x02, "time-at-processing").unwrap()
    );
    assert!(done.int(0x02, "time-at-creation").unwrap() >= 0);
    assert!(done.int(0x02, "job-printer-up-time").unwrap() >= 1);
    assert_eq!(
        done.strs(0x02, "job-printer-uri"),
        std::slice::from_ref(&r.uri)
    );

    // the same row on the control plane, with the ticket-equivalent attributes
    let jobs = get_json(r.addr, "/v1/jobs").await;
    let row = jobs["print_jobs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|j| j["id"] == id)
        .expect("the IPP job is in /v1/jobs");
    assert_eq!(row["state"], "Completed");
    assert_eq!(row["protocol"], "ipp");
    assert_eq!(row["name"], "report");
    assert_eq!(row["user"], "alice");
    assert_eq!(row["ticket"]["copies"]["value"], 2);
    assert_eq!(row["ticket"]["sides"]["value"], "OneSided");
    assert_eq!(row["ticket"]["quality"]["value"], "Normal");
    assert_eq!(row["ipp"]["attributes"]["copies"], 2);
    assert_eq!(row["documents"][0]["format"], "application/pdf");
    assert_eq!(row["documents"][0]["bytes"], 13);
    // the document reached the inbox
    let file = row["documents"][0]["path"].as_str().unwrap();
    assert_eq!(std::fs::read(file).unwrap(), b"%PDF-1.4 fake");
    assert_eq!(get_json(r.addr, &format!("/v1/jobs/{id}")).await["id"], id);

    // and the WS-Print side sees it as an ordinary job
    let wsd = soap(
        r.addr,
        "/WebServices/PrinterService",
        envelope(
            &print_action("GetJobHistory"),
            "",
            "<wprt:GetJobHistoryRequest/>",
        ),
    )
    .await;
    assert_eq!(wsd.status, 200, "{}", wsd.text());
    assert!(wsd.text().contains(&format!(">{id}<")), "{}", wsd.text());
    assert_eq!(
        get_json(r.addr, "/v1/status").await["ipp_actions"]["PrintJob"],
        1
    );
    r.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jobs_created_over_wsd_are_listed_and_cancelable_over_ipp() {
    let r = rig().await;
    let wsd_id = job_id_of(
        &soap(
            r.addr,
            "/WebServices/PrinterService",
            envelope(
                &print_action("CreatePrintJob"),
                "",
                &ticket_body("from wsd"),
            ),
        )
        .await,
    );
    let resp = r.send(r.req(OP_GET_JOBS)).await;
    assert_eq!(resp.status, OK);
    let ids: Vec<i32> = resp
        .job_groups()
        .iter()
        .map(|g| {
            i32::from_be_bytes(
                g.iter().find(|a| a.0 == "job-id").unwrap().1[0].1[..4]
                    .try_into()
                    .unwrap(),
            )
        })
        .collect();
    assert_eq!(ids, [wsd_id as i32]);
    let attrs = r
        .send(
            r.req(OP_GET_JOB_ATTRIBUTES)
                .with(int("job-id", wsd_id as i32)),
        )
        .await;
    assert_eq!(attrs.job_state(), 3);
    assert_eq!(attrs.strs(0x02, "job-name"), ["from wsd"]);
    let c = r
        .send(r.req(OP_CANCEL_JOB).with(int("job-id", wsd_id as i32)))
        .await;
    assert_eq!(c.status, OK);
    assert_eq!(
        get_json(r.addr, &format!("/v1/jobs/{wsd_id}")).await["state"],
        "Canceled"
    );
    r.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_job_and_send_document_with_last_document() {
    let r = rig().await;
    let c = r
        .send(
            r.req(OP_CREATE_JOB)
                .with(nm("job-name", "two docs"))
                .job(int("copies", 1)),
        )
        .await;
    assert_eq!(c.status, OK);
    let id = c.job_id() as i32;
    assert_eq!(c.job_state(), 3);
    assert_eq!(
        c.strs(0x02, "job-state-reasons"),
        ["job-incoming", "job-data-insufficient"]
    );
    let send = |last: Option<bool>, data: &'static [u8]| {
        let mut q = r.req(OP_SEND_DOCUMENT).with(int("job-id", id)).data(data);
        if let Some(l) = last {
            q = q.with(boolean("last-document", l));
        }
        r.send(q)
    };
    assert_eq!(
        send(None, b"x").await.status,
        BAD_REQUEST,
        "last-document is required"
    );
    let first = send(Some(false), b"%PDF first").await;
    assert_eq!((first.status, first.job_state()), (OK, 5));
    assert!(first
        .strs(0x02, "job-state-reasons")
        .contains(&"job-incoming".to_string()));
    r.advance(50);
    assert_eq!(send(Some(true), b"%PDF second").await.status, OK);
    r.advance(50);
    let (st, reasons) = r.state(id as u32).await;
    assert_eq!(
        (st, reasons),
        (9, vec!["job-completed-successfully".to_string()])
    );
    let row = get_json(r.addr, &format!("/v1/jobs/{id}")).await;
    assert_eq!(row["documents"].as_array().unwrap().len(), 2);
    // nothing more can be added
    assert_eq!(send(Some(true), b"third").await.status, NOT_POSSIBLE);
    // a last document without data closes a job whose documents were all sent
    let c2 = r.send(r.req(OP_CREATE_JOB)).await;
    let id2 = c2.job_id() as i32;
    let s = r
        .send(
            r.req(OP_SEND_DOCUMENT)
                .with(int("job-id", id2))
                .with(boolean("last-document", false))
                .data(b"%PDF a"),
        )
        .await;
    assert_eq!(s.status, OK);
    r.advance(50);
    let close = r
        .send(
            r.req(OP_SEND_DOCUMENT)
                .with(int("job-id", id2))
                .with(boolean("last-document", true)),
        )
        .await;
    assert_eq!(close.status, OK);
    r.advance(50);
    assert_eq!(r.state(id2 as u32).await.0, 9);
    // job-uri addressing works for Send-Document too
    let c3 = r.send(r.req(OP_CREATE_JOB)).await;
    let job_uri = c3.strs(0x02, "job-uri")[0].clone();
    let by_uri = post(
        r.addr,
        &format!("/ipp/print/{}", c3.job_id()),
        Req {
            op_attrs: vec![uri("job-uri", &job_uri), boolean("last-document", true)],
            ..r.req(OP_SEND_DOCUMENT)
        }
        .data(b"%PDF uri")
        .bytes(),
    )
    .await;
    assert_eq!(by_uri.status, OK);
    r.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_multiple_operation_timeout_is_advertised_and_enforced_on_the_fake_clock() {
    let r = rig().await;
    let advertised = r
        .send(
            r.req(OP_GET_PRINTER_ATTRIBUTES)
                .with(kw("requested-attributes", "multiple-operation-time-out")),
        )
        .await;
    assert_eq!(
        advertised.int(0x04, "multiple-operation-time-out"),
        Some(120)
    );
    let id = r.send(r.req(OP_CREATE_JOB)).await.job_id();
    r.clock.advance(Duration::from_secs(119));
    assert_eq!(r.state(id).await.0, 3, "still waiting at 119 s");
    r.clock.advance(Duration::from_secs(2));
    let (st, reasons) = r.state(id).await;
    assert_eq!(st, 8);
    assert_eq!(reasons, ["aborted-by-system", "submission-interrupted"]);
    let late = r
        .send(
            r.req(OP_SEND_DOCUMENT)
                .with(int("job-id", id as i32))
                .with(boolean("last-document", true))
                .data(b"late"),
        )
        .await;
    assert_eq!(late.status, NOT_POSSIBLE);

    // the timer restarts after each document, and the value is configurable at run time
    let t = post_json(
        r.addr,
        "/v1/timing",
        &json!({"ipp_multiple_operation_timeout_ms": 5000}),
    )
    .await;
    assert_eq!(t.status, 200, "{}", t.text());
    assert_eq!(t.json()["ipp_multiple_operation_timeout_ms"], 5000);
    let advertised = r
        .send(
            r.req(OP_GET_PRINTER_ATTRIBUTES)
                .with(kw("requested-attributes", "multiple-operation-time-out")),
        )
        .await;
    assert_eq!(advertised.int(0x04, "multiple-operation-time-out"), Some(5));
    let id2 = r.send(r.req(OP_CREATE_JOB)).await.job_id();
    let first = r
        .send(
            r.req(OP_SEND_DOCUMENT)
                .with(int("job-id", id2 as i32))
                .with(boolean("last-document", false))
                .data(b"%PDF 1"),
        )
        .await;
    assert_eq!(first.status, OK);
    r.advance(10);
    r.clock.advance(Duration::from_secs(4));
    assert_eq!(r.state(id2).await.0, 5, "waiting for the next document");
    r.clock.advance(Duration::from_secs(2));
    let (st, reasons) = r.state(id2).await;
    assert_eq!((st, reasons[0].as_str()), (8, "aborted-by-system"));
    r.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_job_races_and_error_codes() {
    let r = rig_with("generic-wsd", PrintTiming::realistic()).await;
    let cancel = |id: i32| r.send(r.req(OP_CANCEL_JOB).with(int("job-id", id)));
    assert_eq!(cancel(999).await.status, NOT_FOUND);
    let id = r.print(r.req(OP_PRINT_JOB)).await.job_id() as i32;
    r.advance(100);
    assert_eq!(cancel(id).await.status, OK);
    // being cancelled: processing with a stop point, then canceled
    let (st, reasons) = r.state(id as u32).await;
    assert_eq!(st, 5);
    assert_eq!(
        reasons,
        ["processing-to-stop-point", "job-canceled-by-user"]
    );
    assert_eq!(cancel(id).await.status, NOT_POSSIBLE);
    r.advance(1_000);
    let (st, reasons) = r.state(id as u32).await;
    assert_eq!((st, reasons), (7, vec!["job-canceled-by-user".to_string()]));
    assert_eq!(cancel(id).await.status, NOT_POSSIBLE);
    // a document for a cancelled job is refused, and its capture left the inbox
    let late = r
        .send(
            r.req(OP_SEND_DOCUMENT)
                .with(int("job-id", id))
                .with(boolean("last-document", true))
                .data(b"x"),
        )
        .await;
    assert_eq!(late.status, NOT_POSSIBLE);
    assert!(get_json(r.addr, "/v1/jobs").await["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .all(|c| c["job_id"] != id));
    // cancel racing completion: whichever lands second is "not possible"
    let id2 = r.print(r.req(OP_PRINT_JOB)).await.job_id() as i32;
    r.advance(60_000);
    assert_eq!(cancel(id2).await.status, NOT_POSSIBLE);
    assert_eq!(r.state(id2 as u32).await.0, 9);
    // cancel a pending Create-Job job
    let id3 = r.send(r.req(OP_CREATE_JOB)).await.job_id() as i32;
    assert_eq!(cancel(id3).await.status, OK);
    r.advance(1_000);
    assert_eq!(r.state(id3 as u32).await.0, 7);
    r.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_jobs_which_jobs_my_jobs_limit_and_requested_attributes() {
    let r = rig().await;
    let mk =
        |user: &'static str| r.print(r.req(OP_PRINT_JOB).with(nm("requesting-user-name", user)));
    let a = mk("alice").await.job_id();
    r.advance(10);
    let b = mk("bob").await.job_id();
    r.advance(10);
    let c = mk("alice").await.job_id();
    let pending = r
        .send(
            r.req(OP_CREATE_JOB)
                .with(nm("requesting-user-name", "carol")),
        )
        .await
        .job_id();
    r.advance(10);
    let ids = |resp: &IppResp| -> Vec<u32> {
        resp.job_groups()
            .iter()
            .map(|g| {
                let v = &g.iter().find(|a| a.0 == "job-id").unwrap().1[0].1;
                u32::from_be_bytes([v[0], v[1], v[2], v[3]])
            })
            .collect()
    };
    let nc = r.send(r.req(OP_GET_JOBS)).await;
    assert_eq!(ids(&nc), [pending], "not-completed is the default");
    assert_eq!(
        nc.job_groups()[0]
            .iter()
            .map(|a| a.0.as_str())
            .collect::<Vec<_>>(),
        ["job-id", "job-uri"],
        "Get-Jobs returns job-id and job-uri by default"
    );
    let done = r
        .send(r.req(OP_GET_JOBS).with(kw("which-jobs", "completed")))
        .await;
    assert_eq!(ids(&done), [c, b, a], "newest first");
    let mine = r
        .send(
            r.req(OP_GET_JOBS)
                .with(kw("which-jobs", "completed"))
                .with(boolean("my-jobs", true))
                .with(nm("requesting-user-name", "alice")),
        )
        .await;
    assert_eq!(ids(&mine), [c, a]);
    let limited = r
        .send(
            r.req(OP_GET_JOBS)
                .with(kw("which-jobs", "completed"))
                .with(int("limit", 2)),
        )
        .await;
    assert_eq!(ids(&limited), [c, b]);
    let sel = r
        .send(
            r.req(OP_GET_JOBS)
                .with(kw("which-jobs", "completed"))
                .with(kw("requested-attributes", "job-state"))
                .with(more_kw("job-name")),
        )
        .await;
    assert_eq!(sel.job_groups()[0].len(), 2);
    // errors
    assert_eq!(
        r.send(r.req(OP_GET_JOBS).with(kw("which-jobs", "bogus")))
            .await
            .status,
        ATTRS_NOT_SUPPORTED
    );
    assert_eq!(
        r.send(r.req(OP_GET_JOBS).with(int("limit", 0)))
            .await
            .status,
        BAD_REQUEST
    );
    assert_eq!(
        r.send(r.req(OP_GET_JOBS).with(boolean("my-jobs", true)))
            .await
            .status,
        BAD_REQUEST
    );
    r.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finished_jobs_are_forgotten_after_the_retention_window_but_not_by_wsd() {
    let r = rig().await;
    let id = r.print(r.req(OP_PRINT_JOB)).await.job_id();
    r.advance(50);
    assert_eq!(r.state(id).await.0, 9);
    r.clock.advance(Duration::from_secs(299));
    assert_eq!(r.state(id).await.0, 9);
    r.clock.advance(Duration::from_secs(2));
    let gone = r
        .send(r.req(OP_GET_JOB_ATTRIBUTES).with(int("job-id", id as i32)))
        .await;
    assert_eq!(gone.status, NOT_FOUND);
    let listed = r
        .send(r.req(OP_GET_JOBS).with(kw("which-jobs", "completed")))
        .await;
    assert!(listed.job_groups().is_empty());
    assert_eq!(
        get_json(r.addr, &format!("/v1/jobs/{id}")).await["state"],
        "Completed"
    );
    // the window is configurable
    post_json(
        r.addr,
        "/v1/timing",
        &json!({"ipp_history_retention_ms": 1000}),
    )
    .await;
    let id2 = r.print(r.req(OP_PRINT_JOB)).await.job_id();
    r.advance(50);
    assert_eq!(r.state(id2).await.0, 9);
    r.advance(1_500);
    assert_eq!(
        r.send(r.req(OP_GET_JOB_ATTRIBUTES).with(int("job-id", id2 as i32)))
            .await
            .status,
        NOT_FOUND
    );
    r.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seeded_jobs_make_get_jobs_scenarios_instant() {
    let r = rig().await;
    let p = post_json(
        r.addr,
        "/v1/jobs",
        &json!({"jobs": [
            {"name": "old", "user": "alice", "state": "completed", "age_ms": 400000},
            {"name": "recent", "user": "alice", "state": "completed", "age_ms": 1000, "bytes": 100000},
            {"name": "waiting", "user": "bob", "state": "pending"},
            {"name": "busy", "user": "bob", "state": "processing"},
            {"name": "bad", "user": "bob", "state": "aborted", "abort_reason": "format"},
            {"name": "gone", "user": "bob", "state": "canceled"}
        ]}),
    )
    .await;
    assert_eq!(p.status, 200, "{}", p.text());
    let created = p.json()["created"].as_array().unwrap().len();
    assert_eq!(created, 6);
    let nc = r
        .send(
            r.req(OP_GET_JOBS)
                .with(kw("requested-attributes", "job-name")),
        )
        .await;
    let names: Vec<String> = nc.job_groups().iter().map(|g| job_name(g)).collect();
    assert_eq!(names, ["waiting", "busy"]);
    let done = r
        .send(
            r.req(OP_GET_JOBS)
                .with(kw("which-jobs", "completed"))
                .with(kw("requested-attributes", "job-name"))
                .with(more_kw("job-state"))
                .with(more_kw("job-state-reasons")),
        )
        .await;
    let names: Vec<String> = done.job_groups().iter().map(|g| job_name(g)).collect();
    assert_eq!(
        names.len(),
        3,
        "the 400 s old job is past the retention window: {names:?}"
    );
    assert!(!names.contains(&"old".to_string()));
    let bad = done
        .job_groups()
        .into_iter()
        .find(|g| job_name(g) == "bad")
        .unwrap();
    let reasons: Vec<String> = bad
        .iter()
        .find(|a| a.0 == "job-state-reasons")
        .unwrap()
        .1
        .iter()
        .map(|v| String::from_utf8_lossy(&v.1).into_owned())
        .collect();
    assert_eq!(reasons, ["aborted-by-system", "document-format-error"]);
    // a bad description is a 400 and creates nothing
    assert_eq!(
        post_json(r.addr, "/v1/jobs", &json!({"state": "exploded"}))
            .await
            .status,
        400
    );
    assert_eq!(
        post_json(r.addr, "/v1/jobs", &json!({"nope": 1}))
            .await
            .status,
        400
    );
    r.finish().await;
}

// ---------------------------------------------------------------------------------------------
// Validation and error codes
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn envelope_errors_are_ipp_errors_not_http_errors() {
    let r = rig().await;
    let status = |resp: &IppResp| resp.status;
    // request-id 0
    let mut q = r.req(OP_GET_PRINTER_ATTRIBUTES);
    q.id = 0;
    assert_eq!(status(&r.send(q).await), BAD_REQUEST);
    // charset / language missing first
    let mut q = r.req(OP_GET_PRINTER_ATTRIBUTES);
    q.lead = false;
    assert_eq!(status(&r.send(q).await), BAD_REQUEST);
    // unsupported version answers in a supported one
    let mut q = r.req(OP_GET_PRINTER_ATTRIBUTES);
    q.version = (3, 0);
    let resp = r.send(q).await;
    assert_eq!((resp.status, resp.version), (0x0503, (2, 0)));
    // 1.1 is echoed
    let mut q = r.req(OP_GET_PRINTER_ATTRIBUTES);
    q.version = (1, 1);
    q.id = 4242;
    let resp = r.send(q).await;
    assert_eq!((resp.status, resp.version, resp.id), (OK, (1, 1), 4242));
    // truncated attribute
    let resp = post(
        r.addr,
        "/ipp/print",
        vec![2, 0, 0, 0x0b, 0, 0, 0, 9, 1, 0x44, 0, 40],
    )
    .await;
    assert_eq!((resp.status, resp.id), (BAD_REQUEST, 9));
    // unknown operation, known-but-unsupported operation
    assert_eq!(status(&r.send(r.req(0x4001)).await), OP_NOT_SUPPORTED);
    assert_eq!(status(&r.send(r.req(0x0003)).await), OP_NOT_SUPPORTED);
    // charset the printer does not know
    let mut b = r.req(OP_GET_PRINTER_ATTRIBUTES).bytes();
    let pos = b.windows(5).position(|w| w == b"utf-8").unwrap();
    b.splice(pos..pos + 5, *b"utf-7");
    assert_eq!(post(r.addr, "/ipp/print", b).await.status, 0x040d);
    // printer-uri: wrong path, wrong scheme, missing
    let wrong = Req::new(
        OP_GET_PRINTER_ATTRIBUTES,
        &format!("ipp://127.0.0.1:{}/ipp/other", r.addr.port()),
    );
    assert_eq!(status(&r.send(wrong).await), NOT_FOUND);
    let ftp = Req::new(OP_GET_PRINTER_ATTRIBUTES, "ftp://127.0.0.1/ipp/print");
    assert_eq!(status(&r.send(ftp).await), 0x040c);
    let mut none = r.req(OP_GET_PRINTER_ATTRIBUTES);
    none.op_attrs.clear();
    assert_eq!(status(&r.send(none).await), BAD_REQUEST);
    // a different host with the right path is accepted
    let other_host = Req::new(
        OP_GET_PRINTER_ATTRIBUTES,
        "ipp://printer.example:631/ipp/print",
    );
    assert_eq!(status(&r.send(other_host).await), OK);
    // wrong syntax and too long
    assert_eq!(
        status(&r.send(r.req(OP_GET_JOBS).with(kw("limit", "ten"))).await),
        BAD_REQUEST
    );
    assert_eq!(
        status(
            &r.send(
                r.req(OP_GET_JOBS)
                    .with(nm("requesting-user-name", &"x".repeat(300)))
            )
            .await
        ),
        0x0409
    );
    // HTTP level: wrong content type, GET
    let bad_ct = request(
        r.addr,
        "POST",
        "/ipp/print",
        vec![("Content-Type".into(), "text/plain".into())],
        b"hello".to_vec(),
    )
    .await;
    assert_eq!(bad_ct.status, 415);
    let get = request(r.addr, "GET", "/ipp/print", vec![], vec![]).await;
    assert_eq!(get.status, 200);
    assert!(get.text().contains("ipp://"));
    r.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn job_template_unsupported_values_follow_the_fidelity_rule() {
    let r = rig().await;
    let before = get_json(r.addr, "/v1/jobs").await["print_jobs"]
        .as_array()
        .unwrap()
        .len();
    // fidelity false: the job is accepted, the offenders are reported
    let ok = r
        .print(
            r.req(OP_PRINT_JOB)
                .job(int("copies", 5))
                .job(kw("sides", "two-sided-long-edge"))
                .job(int("x-not-an-attribute", 1)),
        )
        .await;
    assert_eq!(ok.status, OK_IGNORED);
    assert_eq!(ok.unsupported_names(), ["sides", "x-not-an-attribute"]);
    assert_eq!(
        ok.strs(0x05, "sides"),
        ["two-sided-long-edge"],
        "the original value comes back"
    );
    assert_eq!(
        ok.get(0x05, "x-not-an-attribute").unwrap()[0].0,
        0x10,
        "unsupported out-of-band tag"
    );
    let id = ok.job_id();
    let attrs = r
        .send(r.req(OP_GET_JOB_ATTRIBUTES).with(int("job-id", id as i32)))
        .await;
    assert_eq!(attrs.int(0x02, "copies"), Some(5));
    assert!(attrs.get(0x02, "sides").is_none());
    // fidelity true: refused, nothing created, everything unsupported listed
    let refused = r
        .print(
            r.req(OP_PRINT_JOB)
                .with(boolean("ipp-attribute-fidelity", true))
                .job(int("copies", 5))
                .job(kw("sides", "two-sided-long-edge"))
                .job(en("orientation-requested", 5))
                .job(kw("media", "iso_a0_841x1189mm"))
                .job(kw("print-color-mode", "rainbow"))
                .job(int("number-up", 3))
                .job(kw("job-hold-until", "indefinite")),
        )
        .await;
    assert_eq!(refused.status, ATTRS_NOT_SUPPORTED);
    assert_eq!(
        refused.unsupported_names(),
        [
            "sides",
            "orientation-requested",
            "media",
            "print-color-mode",
            "number-up",
            "job-hold-until"
        ]
    );
    assert!(refused.group(0x02).is_none());
    assert_eq!(
        get_json(r.addr, "/v1/jobs").await["print_jobs"]
            .as_array()
            .unwrap()
            .len(),
        before + 1
    );
    // Validate-Job: same verdicts, never a job
    let v = r
        .send(
            r.req(OP_VALIDATE_JOB)
                .with(boolean("ipp-attribute-fidelity", true))
                .job(kw("sides", "one-sided"))
                .job(int("copies", 3)),
        )
        .await;
    assert_eq!(v.status, OK);
    assert!(v.group(0x02).is_none());
    let v = r
        .send(
            r.req(OP_VALIDATE_JOB)
                .with(boolean("ipp-attribute-fidelity", true))
                .job(int("copies", 0)),
        )
        .await;
    assert_eq!(v.status, ATTRS_NOT_SUPPORTED);
    assert_eq!(v.int(0x05, "copies"), Some(0));
    // media-col reports only the offending member
    let col = {
        let mut b = vec![0x34];
        b.extend_from_slice(&9u16.to_be_bytes());
        b.extend_from_slice(b"media-col");
        b.extend_from_slice(&0u16.to_be_bytes());
        // media-type = hologram
        b.extend(attr(0x4a, "", b"media-type"));
        b.extend(attr(0x44, "", b"hologram"));
        // media-source = tray-9 is fine to be wrong too, keep one offender only
        b.extend(attr(0x37, "", b""));
        b
    };
    let mut q = r
        .req(OP_VALIDATE_JOB)
        .with(boolean("ipp-attribute-fidelity", true));
    q.job_attrs.push(col);
    let v = r.send(q).await;
    assert_eq!(v.status, ATTRS_NOT_SUPPORTED);
    assert_eq!(v.unsupported_names(), ["media-col"]);
    // format and compression: refused whatever the fidelity says
    let f = r
        .print(
            r.req(OP_PRINT_JOB)
                .with(mime("document-format", "image/heic")),
        )
        .await;
    assert_eq!(f.status, FORMAT_NOT_SUPPORTED);
    assert_eq!(f.strs(0x05, "document-format"), ["image/heic"]);
    let c = r
        .print(r.req(OP_PRINT_JOB).with(kw("compression", "gzip")))
        .await;
    assert_eq!(c.status, 0x040f);
    assert_eq!(
        r.send(r.req(OP_PRINT_JOB)).await.status,
        BAD_REQUEST,
        "no document data"
    );
    r.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_fault_flags_map_to_server_errors() {
    let r = rig().await;
    post_json(r.addr, "/v1/faults", &json!({"temporary_error": true})).await;
    assert_eq!(r.send(r.req(OP_CREATE_JOB)).await.status, 0x0505);
    post_json(
        r.addr,
        "/v1/faults",
        &json!({"temporary_error": false, "internal_error": true}),
    )
    .await;
    assert_eq!(r.send(r.req(OP_CREATE_JOB)).await.status, 0x0500);
    post_json(r.addr, "/v1/faults", &json!({"reset": true})).await;
    assert_eq!(r.send(r.req(OP_CREATE_JOB)).await.status, OK);
    r.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_document_over_two_mebibytes_prints() {
    let r = rig().await;
    let big = vec![b'x'; 3 << 20];
    let resp = post(r.addr, "/ipp/print", r.req(OP_PRINT_JOB).data(&big).bytes()).await;
    assert_eq!(resp.status, OK);
    r.advance(100);
    let row = get_json(r.addr, &format!("/v1/jobs/{}", resp.job_id())).await;
    assert_eq!(row["documents"][0]["bytes"], 3 << 20);
    r.finish().await;
}

// ---------------------------------------------------------------------------------------------
// Per-device routing
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_device_has_its_own_ipp_endpoint_uuid_and_jobs() {
    let tmp = TempDir::new("ipp-md");
    let mut cfg = ServerConfig::embedded(tmp.path().join("inbox")).with_devices(vec![
        DeviceConfig::derived("a", "generic-wsd"),
        DeviceConfig::derived("b", "mono-laser"),
    ]);
    cfg.profile = Some(profile("generic-wsd"));
    cfg.profile_dir = Some(profiles_dir());
    cfg.timing = PrintTiming::instant();
    let srv = serve(cfg).await.unwrap();
    let addr = srv.local_addr();
    let rows = get_json(addr, "/v1/devices").await;
    let uri_of = |i: usize| rows[i]["ipp_endpoint"].as_str().unwrap().to_string();
    assert!(uri_of(1).ends_with("/d/b/ipp/print"), "{}", uri_of(1));
    let path_of = |u: &str| {
        u.split_once("//")
            .unwrap()
            .1
            .split_once('/')
            .unwrap()
            .1
            .to_string()
    };
    let (pa, pb) = (
        format!("/{}", path_of(&uri_of(0))),
        format!("/{}", path_of(&uri_of(1))),
    );
    let ask = |uri: String, path: String| async move {
        post(
            addr,
            &path,
            Req::new(OP_GET_PRINTER_ATTRIBUTES, &uri).bytes(),
        )
        .await
    };
    let ra = ask(uri_of(0), pa.clone()).await;
    let rb = ask(uri_of(1), pb.clone()).await;
    assert_eq!((ra.status, rb.status), (OK, OK));
    assert_eq!(ra.strs(0x04, "printer-uri-supported"), [uri_of(0)]);
    assert_eq!(rb.strs(0x04, "printer-uri-supported"), [uri_of(1)]);
    assert_ne!(ra.strs(0x04, "printer-uuid"), rb.strs(0x04, "printer-uuid"));
    assert_eq!(
        rb.strs(0x04, "printer-uuid")[0],
        format!("urn:uuid:{}", rows[1]["ipp_uuid"].as_str().unwrap())
    );
    assert_ne!(
        ra.strs(0x04, "printer-make-and-model"),
        rb.strs(0x04, "printer-make-and-model")
    );
    // a printer-uri that names the other device is "not found"
    let cross = post(
        addr,
        &pb,
        Req::new(OP_GET_PRINTER_ATTRIBUTES, &uri_of(0)).bytes(),
    )
    .await;
    assert_eq!(cross.status, NOT_FOUND);
    // jobs stay on their device
    let pj = post(
        addr,
        &pb,
        Req::new(OP_PRINT_JOB, &uri_of(1)).data(b"%PDF b").bytes(),
    )
    .await;
    assert_eq!(pj.status, OK);
    assert!(pj.strs(0x02, "job-uri")[0].contains("/d/b/ipp/print/"));
    let on_a = post(
        addr,
        &pa,
        Req::new(OP_GET_JOBS, &uri_of(0))
            .with(kw("which-jobs", "completed"))
            .bytes(),
    )
    .await;
    assert!(on_a.job_groups().is_empty());
    let on_b = post(
        addr,
        &pb,
        Req::new(OP_GET_JOBS, &uri_of(1))
            .with(kw("which-jobs", "completed"))
            .bytes(),
    )
    .await;
    assert_eq!(on_b.job_groups().len(), 1);
    assert_eq!(
        get_json(addr, "/d/b/v1/jobs").await["print_jobs"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        get_json(addr, "/d/a/v1/jobs").await["print_jobs"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    // job-uri as the HTTP target
    let job_path = format!("{pb}/{}", pj.job_id());
    let by_uri = post(
        addr,
        &job_path,
        Req {
            op_attrs: vec![uri("job-uri", &pj.strs(0x02, "job-uri")[0])],
            ..Req::new(OP_GET_JOB_ATTRIBUTES, &uri_of(1))
        }
        .bytes(),
    )
    .await;
    assert_eq!(by_uri.status, OK);
    assert_eq!(srv.ipp_validation_problems().await, Vec::<String>::new());
    srv.shutdown().await;
}

// ---------------------------------------------------------------------------------------------
// Fault injection
// ---------------------------------------------------------------------------------------------

async fn arm(r: &Rig, rules: serde_json::Value) -> Resp {
    post_json(r.addr, "/v1/faults", &json!({ "inject": rules })).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ipp_status_effect_skip_count_and_op_names() {
    let r = rig().await;
    let armed = arm(
        &r,
        json!([{"endpoint": "ipp", "op": "print_job", "skip": 1, "count": 2,
                "effect": {"type": "ipp_status", "status": "server-error-busy", "message": "try later"}}]),
    )
    .await;
    assert_eq!(armed.status, 200, "{}", armed.text());
    let print = || r.print(r.req(OP_PRINT_JOB));
    assert_eq!(print().await.status, OK, "skipped");
    let busy = print().await;
    assert_eq!(busy.status, BUSY);
    assert_eq!(busy.strs(0x01, "status-message"), ["try later"]);
    assert_eq!(busy.id, 1, "the request id is echoed");
    assert!(busy.group(0x02).is_none());
    assert_eq!(print().await.status, BUSY);
    assert_eq!(print().await.status, OK, "rule consumed");
    // the faulted requests never reached the job engine
    assert_eq!(
        get_json(r.addr, "/v1/jobs").await["print_jobs"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    // other operations were never affected by a print_job rule
    let st = get_json(r.addr, "/v1/faults").await;
    assert_eq!(st["inject"]["hits"], 2);
    assert_eq!(st["inject"]["skipped"], 1);
    // the spelling of the operation does not matter; numeric codes and hex work
    for (op_name, status) in [
        ("Get-Printer-Attributes", json!(1030)),
        ("GetJobs", json!("0x0502")),
        ("cancel_job", json!("client-error-not-authorized")),
        ("create-job", json!("server-error-service-unavailable")),
        ("send_document", json!("server-error-internal-error")),
        ("validate_job", json!("server-error-not-accepting-jobs")),
        ("get_job_attributes", json!("client-error-not-possible")),
    ] {
        let a = arm(&r, json!([{"endpoint": "ipp", "op": op_name, "effect": {"type": "ipp_status", "status": status}}])).await;
        assert_eq!(a.status, 200, "{op_name}: {}", a.text());
    }
    assert_eq!(
        r.send(r.req(OP_GET_PRINTER_ATTRIBUTES)).await.status,
        NOT_FOUND
    );
    assert_eq!(r.send(r.req(OP_GET_JOBS)).await.status, 0x0502);
    assert_eq!(
        r.send(r.req(OP_CANCEL_JOB).with(int("job-id", 1)))
            .await
            .status,
        0x0403
    );
    assert_eq!(r.send(r.req(OP_CREATE_JOB)).await.status, 0x0502);
    assert_eq!(
        r.send(
            r.req(OP_SEND_DOCUMENT)
                .with(int("job-id", 1))
                .with(boolean("last-document", true))
        )
        .await
        .status,
        0x0500
    );
    assert_eq!(r.send(r.req(OP_VALIDATE_JOB)).await.status, NOT_ACCEPTING);
    assert_eq!(
        r.send(r.req(OP_GET_JOB_ATTRIBUTES).with(int("job-id", 1)))
            .await
            .status,
        NOT_POSSIBLE
    );
    assert_eq!(r.send(r.req(OP_GET_PRINTER_ATTRIBUTES)).await.status, OK);
    r.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_close_and_delay_effects_on_the_ipp_endpoint() {
    let r = rig().await;
    // an HTTP 503 with a body
    arm(
        &r,
        json!([{"endpoint": "ipp", "op": "get_printer_attributes",
                    "effect": {"type": "http", "status": 503, "body": "maintenance"}}]),
    )
    .await;
    let resp = request(
        r.addr,
        "POST",
        "/ipp/print",
        vec![("Content-Type".into(), "application/ipp".into())],
        r.req(OP_GET_PRINTER_ATTRIBUTES).bytes(),
    )
    .await;
    assert_eq!((resp.status, resp.text().as_str()), (503, "maintenance"));
    assert_eq!(r.send(r.req(OP_GET_PRINTER_ATTRIBUTES)).await.status, OK);
    // a dropped connection
    arm(
        &r,
        json!([{"endpoint": "ipp", "op": "print_job", "effect": {"type": "close_connection"}}]),
    )
    .await;
    let body = r.req(OP_PRINT_JOB).data(b"%PDF").bytes();
    let addr = r.addr;
    let raw = tokio::task::spawn_blocking(move || {
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let head = format!(
            "POST /ipp/print HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Type: application/ipp\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        s.write_all(head.as_bytes()).unwrap();
        s.write_all(&body).unwrap();
        let mut out = Vec::new();
        let _ = s.read_to_end(&mut out);
        out
    })
    .await
    .unwrap();
    assert!(
        !String::from_utf8_lossy(&raw).contains("200 OK"),
        "{:?}",
        String::from_utf8_lossy(&raw)
    );
    assert_eq!(
        get_json(r.addr, "/v1/jobs").await["print_jobs"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    // a slow printer
    arm(
        &r,
        json!([{"endpoint": "ipp", "op": "get_jobs", "delay_ms": 300}]),
    )
    .await;
    let t = Instant::now();
    assert_eq!(r.send(r.req(OP_GET_JOBS)).await.status, OK);
    assert!(
        t.elapsed() >= Duration::from_millis(250),
        "{:?}",
        t.elapsed()
    );
    // a rule without an op or an IPP endpoint never touches IPP
    arm(&r, json!([{"effect": {"type": "http", "status": 500}}])).await;
    assert_eq!(r.send(r.req(OP_GET_JOBS)).await.status, OK);
    // library API
    r.srv
        .inject(
            fakeprint::wsd::inject::InjectRule::new(
                fakeprint::wsd::inject::InjectEffect::IppStatus {
                    status: fakeprint::wsd::inject::IppStatusSpec::Name("server-error-busy".into()),
                    message: None,
                },
            )
            .on(fakeprint::wsd::inject::RuleEndpoint::Ipp)
            .for_op("get_jobs"),
        )
        .unwrap();
    assert_eq!(r.send(r.req(OP_GET_JOBS)).await.status, BUSY);
    r.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_ipp_rules_are_refused_by_the_control_plane() {
    let r = rig().await;
    for bad in [
        json!({"endpoint": "ipp", "effect": {"type": "ipp_status", "status": "nonsense"}}),
        json!({"op": "print_job", "effect": {"type": "ipp_status", "status": "server-error-busy"}}),
        json!({"endpoint": "print_service", "effect": {"type": "ipp_status", "status": 1283}}),
        json!({"endpoint": "ipp", "effect": {"type": "ipp_status"}}),
    ] {
        let a = arm(&r, json!([bad.clone()])).await;
        assert_eq!(a.status, 400, "{bad}: {}", a.text());
    }
    assert!(r.srv.pending_inject().is_empty());
    r.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_reports_ipp_counters_and_validation() {
    let r = rig().await;
    r.send(r.req(OP_GET_PRINTER_ATTRIBUTES)).await;
    r.print(r.req(OP_PRINT_JOB)).await;
    r.send(r.req(OP_GET_JOBS)).await;
    let st = get_json(r.addr, "/v1/status").await;
    assert_eq!(st["ipp_actions"]["GetPrinterAttributes"], 1);
    assert_eq!(st["ipp_actions"]["PrintJob"], 1);
    assert_eq!(st["ipp_actions"]["GetJobs"], 1);
    assert_eq!(st["ipp_validation"]["enabled"], true);
    assert_eq!(st["ipp_validation"]["checked"], 3);
    assert_eq!(st["ipp_validation"]["problems"], 0);
    let t = get_json(r.addr, "/v1/timing").await;
    assert_eq!(t["ipp_multiple_operation_timeout_ms"], 120_000);
    assert_eq!(t["ipp_history_retention_ms"], 300_000);
    // reset clears jobs and the counters
    post_json(r.addr, "/v1/reset", &json!({})).await;
    let st = get_json(r.addr, "/v1/status").await;
    assert_eq!(st["ipp_validation"]["checked"], 0);
    assert_eq!(
        get_json(r.addr, "/v1/jobs").await["print_jobs"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    r.finish().await;
}

// ---------------------------------------------------------------------------------------------
// print-scaling (PWG 5100.13)
// ---------------------------------------------------------------------------------------------

/// A flat one-page A4 PDF (595.28 x 841.89 pt) as a client would send it.
const A4_PDF: &[u8] = b"%PDF-1.4\n1 0 obj\n<< /Type /Page /MediaBox [0 0 595.28 841.89] >>\nendobj\ntrailer\n<< /Root 1 0 R >>\n%%EOF\n";

async fn ipp_row(r: &Rig, id: u32) -> serde_json::Value {
    let rows = get_json(r.addr, "/v1/jobs").await;
    rows["print_jobs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|j| j["id"] == id)
        .cloned()
        .unwrap_or_else(|| panic!("no row for job {id}: {rows}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn print_scaling_is_advertised_validated_recorded_and_its_effective_scale_reported() {
    let r = rig().await;
    let a = r.send(r.req(OP_GET_PRINTER_ATTRIBUTES)).await;
    assert_eq!(
        a.strs(0x04, "print-scaling-supported"),
        ["auto", "auto-fit", "fill", "fit", "none"]
    );
    assert_eq!(a.strs(0x04, "print-scaling-default"), ["none"]);
    assert!(a
        .strs(0x04, "job-creation-attributes-supported")
        .contains(&"print-scaling".to_string()));
    // A4 page (210 x 297 mm) to a Letter job, shrink-to-fit: 279.4 / 297 (the height limits).
    let ok = r
        .send(
            r.req(OP_PRINT_JOB)
                .with(boolean("ipp-attribute-fidelity", true))
                .job(kw("media", "na_letter_8.5x11in"))
                .job(kw("print-scaling", "auto-fit"))
                .data(A4_PDF),
        )
        .await;
    assert_eq!(ok.status, OK);
    let id = ok.job_id();
    r.advance(100);
    let row = ipp_row(&r, id).await;
    assert_eq!(
        row["ipp"]["attributes"]["print-scaling"],
        json!("auto-fit"),
        "{row}"
    );
    let scale = row["ipp"]["effective_scale"].as_f64().expect("a scale");
    assert!((scale - 279.4 / 297.0).abs() < 5e-4, "{scale}");
    assert_eq!(row["ipp"]["scaling"], json!("auto-fit"));
    // none: 1; fill covers (the wider ratio); auto is the printer's choice and is not modelled
    for (k, expect) in [("none", 1.0), ("auto", 1.0), ("fill", 215.9 / 210.0)] {
        let ok = r
            .send(
                r.req(OP_PRINT_JOB)
                    .job(kw("media", "na_letter_8.5x11in"))
                    .job(kw("print-scaling", k))
                    .data(A4_PDF),
            )
            .await;
        assert_eq!(ok.status, OK);
        let row = ipp_row(&r, ok.job_id()).await;
        let s = row["ipp"]["effective_scale"].as_f64().unwrap();
        assert!((s - expect).abs() < 5e-4, "{k}: {s} vs {expect}");
    }
    // no attribute: the printer's default, none here
    let ok = r.send(r.req(OP_PRINT_JOB).data(A4_PDF)).await;
    let row = ipp_row(&r, ok.job_id()).await;
    assert_eq!(row["ipp"]["scaling"], json!("none"));
    // a keyword the printer does not list: ignored and reported, refused under fidelity
    let bad = r
        .send(
            r.req(OP_PRINT_JOB)
                .with(boolean("ipp-attribute-fidelity", true))
                .job(kw("print-scaling", "stretch"))
                .data(A4_PDF),
        )
        .await;
    assert_eq!(bad.status, ATTRS_NOT_SUPPORTED);
    assert_eq!(bad.unsupported_names(), ["print-scaling"]);
    assert_eq!(bad.strs(0x05, "print-scaling"), ["stretch"]);
    let lenient = r
        .send(
            r.req(OP_PRINT_JOB)
                .job(kw("print-scaling", "stretch"))
                .data(A4_PDF),
        )
        .await;
    assert_eq!(lenient.status, OK_IGNORED);
    r.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_minimal_profile_does_not_advertise_print_scaling_and_refuses_it() {
    let r = rig_with("minimal-ipp", PrintTiming::instant()).await;
    let a = r.send(r.req(OP_GET_PRINTER_ATTRIBUTES)).await;
    assert!(a.get(0x04, "print-scaling-supported").is_none());
    assert!(a.get(0x04, "print-scaling-default").is_none());
    assert!(!a
        .strs(0x04, "job-creation-attributes-supported")
        .contains(&"print-scaling".to_string()));
    let refused = r
        .send(
            r.req(OP_PRINT_JOB)
                .with(boolean("ipp-attribute-fidelity", true))
                .job(kw("print-scaling", "auto-fit"))
                .data(A4_PDF),
        )
        .await;
    assert_eq!(refused.status, ATTRS_NOT_SUPPORTED);
    assert_eq!(refused.unsupported_names(), ["print-scaling"]);
    let lenient = r
        .send(
            r.req(OP_PRINT_JOB)
                .job(kw("print-scaling", "auto-fit"))
                .data(A4_PDF),
        )
        .await;
    assert_eq!(lenient.status, OK_IGNORED);
    let row = ipp_row(&r, lenient.job_id()).await;
    assert!(row["ipp"]["attributes"].get("print-scaling").is_none());
    assert_eq!(row["ipp"]["scaling"], serde_json::Value::Null);
    r.finish().await;
}

#[tokio::test]
async fn an_ipp_body_over_max_body_is_http_413_and_creates_no_job() {
    let tmp = TempDir::new("ipp-cap");
    let limits = fakeprint::wsd::ctx::PrintLimits {
        max_body: 32,
        ..Default::default()
    };
    let mut cfg = ServerConfig::embedded(tmp.path().join("inbox"));
    cfg.profile = Some(profile("generic-wsd"));
    cfg.limits = limits;
    cfg.timing = PrintTiming::instant();
    let srv = serve(cfg).await.expect("server starts");
    let addr = srv.local_addr();
    let uri = format!("ipp://127.0.0.1:{}/ipp/print", addr.port());
    let body = Req::new(OP_GET_PRINTER_ATTRIBUTES, &uri).bytes();
    assert!(body.len() > 32, "the fixture has to be larger than the cap");
    let r = request(
        addr,
        "POST",
        "/ipp/print",
        vec![("Content-Type".into(), "application/ipp".into())],
        body,
    )
    .await;
    assert_eq!(r.status, 413, "{}", r.text());
    assert!(r.text().contains("max_body"));
    let jobs = get_json(addr, "/v1/jobs").await;
    assert!(jobs["print_jobs"].as_array().unwrap().is_empty());
    let browse = request(addr, "GET", "/ipp/print", vec![], vec![]).await;
    assert_eq!(browse.status, 200, "an empty GET is not a body-limit error");
    srv.shutdown().await;
}
