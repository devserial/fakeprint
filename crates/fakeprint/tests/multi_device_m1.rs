//! Several devices in one embedded server: isolation, `/d/{id}` dispatch, run-time add/remove,
//! reconfigure, levels, JetDirect opt-in, loopback guard (docs/multi-device.md sections 1, 2, 10).

mod common;

use common::*;
use fakeprint::server::{serve, DeviceConfig, ServerConfig, ServerHandle};
use serde_json::json;

fn generic() -> fakeprint::profile::PrinterProfile {
    fakeprint::profile::PrinterProfile::load(&profiles_dir(), "generic-wsd").unwrap()
}

async fn two(tmp: &TempDir) -> ServerHandle {
    let mut cfg = ServerConfig::embedded(tmp.path().join("inbox")).with_devices(vec![
        DeviceConfig::derived("a", "generic-wsd"),
        DeviceConfig::derived("b", "generic-wsd"),
    ]);
    cfg.profile = Some(generic());
    cfg.profile_dir = Some(profiles_dir());
    cfg.timing = fakeprint::wsd::ctx::PrintTiming::instant();
    serve(cfg).await.unwrap()
}

const SERVICE: &str = "/WebServices/PrinterService";

async fn print_one(addr: std::net::SocketAddr, prefix: &str, name: &str) -> u32 {
    let path = format!("{prefix}{SERVICE}");
    let job = job_id_of(
        &soap(
            addr,
            &path,
            envelope(&print_action("CreatePrintJob"), "", &ticket_body(name)),
        )
        .await,
    );
    let r = soap(
        addr,
        &path,
        envelope(
            &print_action("SendDocument"),
            "",
            &send_body(
                job,
                1,
                "application/pdf",
                "None",
                true,
                "<wprt:DocumentData>JVBERi0xLjQ=</wprt:DocumentData>",
            ),
        ),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text());
    job
}

async fn count_jobs(addr: std::net::SocketAddr, path: &str) -> usize {
    get_json(addr, path).await["print_jobs"]
        .as_array()
        .unwrap()
        .len()
}

#[tokio::test]
async fn two_devices_are_isolated() {
    let tmp = TempDir::new("md-two");
    let srv = two(&tmp).await;
    let addr = srv.local_addr();
    let rows = get_json(addr, "/v1/devices").await;
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], "a");
    assert_eq!(rows[1]["id"], "b");
    assert_ne!(rows[0]["uuid"], rows[1]["uuid"]);
    assert_ne!(rows[0]["inbox_dir"], rows[1]["inbox_dir"]);
    assert_eq!(rows[1]["base_path"], "/d/b");
    assert!(rows[1]["device_endpoint"]
        .as_str()
        .unwrap()
        .ends_with("/d/b/WebServices/Device"));
    assert!(rows[1]["ipp_endpoint"]
        .as_str()
        .unwrap()
        .ends_with("/d/b/ipp/print"));
    assert_eq!(rows[1]["friendly_name"], "fakeprint b");

    // a job on b appears only in b; the legacy routes are device a
    print_one(addr, "/d/b", "on b").await;
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    assert_eq!(count_jobs(addr, "/d/b/v1/jobs").await, 1);
    assert_eq!(count_jobs(addr, "/d/a/v1/jobs").await, 0);
    assert_eq!(count_jobs(addr, "/v1/jobs").await, 0);
    print_one(addr, "", "on default").await;
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    assert_eq!(count_jobs(addr, "/d/a/v1/jobs").await, 1);
    assert_eq!(count_jobs(addr, "/d/b/v1/jobs").await, 1);

    // faults, timing, reset are per device
    post_json(addr, "/d/b/v1/faults", &json!({"paper_out": true})).await;
    let rows = get_json(addr, "/v1/devices").await;
    assert_eq!(rows[1]["live"]["faults"]["paper_out"], true);
    assert_eq!(rows[0]["live"]["faults"]["paper_out"], false);
    post_json(addr, "/d/a/v1/timing", &json!({"preset": "realistic"})).await;
    let rows = get_json(addr, "/v1/devices").await;
    assert_eq!(rows[0]["live"]["timing_preset"], "realistic");
    assert_eq!(rows[1]["live"]["timing_preset"], "instant");
    post_json(addr, "/d/b/v1/reset", &json!({})).await;
    assert_eq!(count_jobs(addr, "/d/b/v1/jobs").await, 0);
    assert_eq!(count_jobs(addr, "/d/a/v1/jobs").await, 1);
    assert_eq!(get_json(addr, "/d/b/v1/status").await["device_id"], "b");

    // unknown id, trailing slash forms
    let r = request(addr, "GET", "/d/nope/v1/status", vec![], vec![]).await;
    assert_eq!(r.status, 404);
    assert_eq!(r.json()["error"], "unknown device");
    assert_eq!(
        request(addr, "GET", "/d/b/", vec![], vec![]).await.status,
        200
    );
    assert_eq!(
        request(addr, "GET", "/d/b", vec![], vec![]).await.status,
        200
    );
    srv.shutdown().await;
}

#[tokio::test]
async fn metadata_and_ipp_advertise_the_device_prefix() {
    let tmp = TempDir::new("md-meta");
    let srv = two(&tmp).await;
    let addr = srv.local_addr();
    let r = soap(
        addr,
        "/d/b/WebServices/Device",
        envelope("http://schemas.xmlsoap.org/ws/2004/09/transfer/Get", "", ""),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(
        r.text().contains("/d/b/WebServices/PrinterService"),
        "{}",
        r.text()
    );
    let infos = srv.devices().await;
    assert!(infos[1]
        .ipp_endpoint
        .as_deref()
        .unwrap()
        .contains("/d/b/ipp/print"));
    let r = request(addr, "GET", "/d/b/ipp/print", vec![], vec![]).await;
    assert!(r.text().contains("/d/b/ipp/print"), "{}", r.text());
    srv.shutdown().await;
}

#[tokio::test]
async fn injection_is_per_device() {
    let tmp = TempDir::new("md-inject");
    let srv = two(&tmp).await;
    let b = srv.device("b").await.unwrap();
    b.inject(
        serde_json::from_value(
            json!({"endpoint": "device", "effect": {"type": "http", "status": 503}}),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(srv.device("a").await.unwrap().injector().pending().len(), 0);
    assert_eq!(b.injector().pending().len(), 1);
    srv.shutdown().await;
}

#[tokio::test]
async fn add_and_remove_at_run_time() {
    let tmp = TempDir::new("md-add");
    let srv = two(&tmp).await;
    let addr = srv.local_addr();
    let r = post_json(
        addr,
        "/v1/devices",
        &json!({"id": "late", "profile": "generic-wsd"}),
    )
    .await;
    assert_eq!(r.status, 201, "{}", r.text());
    assert_eq!(r.json()["base_path"], "/d/late");
    assert_eq!(srv.devices().await.len(), 3);
    assert_eq!(
        request(addr, "GET", "/d/late/v1/status", vec![], vec![])
            .await
            .status,
        200
    );
    let dup = post_json(
        addr,
        "/v1/devices",
        &json!({"id": "late", "profile": "generic-wsd"}),
    )
    .await;
    assert_eq!(dup.status, 409);
    let bad = post_json(addr, "/v1/devices", &json!({"id": "bad id"})).await;
    assert_eq!(bad.status, 400);
    let unk = post_json(
        addr,
        "/v1/devices",
        &json!({"id": "x", "profile": "no-such"}),
    )
    .await;
    assert_eq!(unk.status, 400, "{}", unk.text());
    let name = post_json(
        addr,
        "/v1/devices",
        &json!({"id": "n", "profile": "generic-wsd", "friendly_name": "Brother"}),
    )
    .await;
    assert_eq!(name.status, 400);
    let uuid = srv.device("late").await.unwrap().info.uuid;
    let dupuuid = post_json(
        addr,
        "/v1/devices",
        &json!({"id": "dup", "profile": "generic-wsd", "device_uuid": uuid.to_string()}),
    )
    .await;
    assert_eq!(dupuuid.status, 409);
    // identity is the UUID prefix: a foreign WSD or IPP UUID is a 400 over HTTP (no override there)
    let foreign = "3f2b9c1e-7d44-4a55-9e0b-1c2d3e4f5a6b";
    let fw = post_json(
        addr,
        "/v1/devices",
        &json!({"id": "fw", "profile": "generic-wsd", "device_uuid": foreign}),
    )
    .await;
    assert_eq!(fw.status, 400, "{}", fw.text());
    let fi = post_json(
        addr,
        "/v1/devices",
        &json!({"id": "fi", "profile": "generic-wsd", "ipp_uuid": foreign}),
    )
    .await;
    assert_eq!(fi.status, 400, "{}", fi.text());
    let ok_uuid = post_json(
        addr,
        "/v1/devices",
        &json!({"id": "fu", "profile": "generic-wsd", "device_uuid": "00000000-0000-4000-8000-0000000000a9"}),
    )
    .await;
    assert_eq!(ok_uuid.status, 201, "{}", ok_uuid.text());

    let r = request(addr, "DELETE", "/v1/devices/late", vec![], vec![]).await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(
        request(addr, "GET", "/d/late/v1/status", vec![], vec![])
            .await
            .status,
        404
    );
    assert_eq!(
        request(addr, "DELETE", "/v1/devices/late", vec![], vec![])
            .await
            .status,
        404
    );
    assert_eq!(
        request(addr, "DELETE", "/v1/devices/a", vec![], vec![])
            .await
            .status,
        400,
        "the default device cannot be removed"
    );
    srv.shutdown().await;
}

#[tokio::test]
async fn reconfigure_and_levels() {
    let tmp = TempDir::new("md-reconf");
    let srv = two(&tmp).await;
    let addr = srv.local_addr();
    let before = srv
        .device("b")
        .await
        .unwrap()
        .info
        .config
        .published_fingerprint;
    let profs = get_json(addr, "/v1/profiles").await;
    let ids: Vec<String> = profs
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["id"].as_str().unwrap().to_string())
        .collect();
    let other = ids
        .iter()
        .find(|id| *id != "generic-wsd")
        .expect("a second shipped profile")
        .clone();
    let r = post_json(addr, "/d/b/v1/profile", &json!({"id": other})).await;
    assert_eq!(r.status, 200, "{}", r.text());
    let after = srv.device("b").await.unwrap();
    assert_eq!(after.info.profile, other);
    assert_ne!(after.info.config.published_fingerprint, before);
    assert_eq!(srv.device("a").await.unwrap().info.profile, "generic-wsd");
    let r = post_json(
        addr,
        "/v1/devices/b/reconfigure",
        &json!({"profile": "generic-wsd"}),
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["config"]["published_fingerprint"], before.as_str());
    let missing = post_json(
        addr,
        "/v1/devices/zz/reconfigure",
        &json!({"profile": "generic-wsd"}),
    )
    .await;
    assert_eq!(missing.status, 404);

    // levels need a profile with consumables
    let with_levels = ids.iter().find(|id| {
        fakeprint::profile::PrinterProfile::load(&profiles_dir(), id)
            .map(|p| p.consumables.as_ref().is_some_and(|c| !c.is_empty()))
            .unwrap_or(false)
    });
    if let Some(id) = with_levels {
        post_json(addr, "/v1/devices/b/reconfigure", &json!({"profile": id})).await;
        let live = srv.device("b").await.unwrap().info.live;
        let name = live.consumables[0].name.clone();
        let r = post_json(
            addr,
            "/d/b/v1/levels",
            &json!({"consumables": {name.clone(): 15}}),
        )
        .await;
        assert_eq!(r.status, 200, "{}", r.text());
        assert_eq!(r.json()["consumables"][0]["level"], 15);
        let bad = post_json(addr, "/d/b/v1/levels", &json!({"consumables": {"nope": 1}})).await;
        assert_eq!(bad.status, 400);
        let high = post_json(addr, "/d/b/v1/levels", &json!({"consumables": {name: 101}})).await;
        assert_eq!(high.status, 400);
    }
    srv.shutdown().await;
}

#[tokio::test]
async fn jetdirect_is_opt_in_and_conflicts_are_refused() {
    let tmp = TempDir::new("md-jd");
    let mut raw = DeviceConfig::derived("raw", "generic-wsd");
    raw.jetdirect_port = Some(0);
    let mut cfg = ServerConfig::embedded(tmp.path().join("inbox"))
        .with_devices(vec![DeviceConfig::derived("a", "generic-wsd"), raw]);
    cfg.profile_dir = Some(profiles_dir());
    let srv = serve(cfg).await.unwrap();
    let infos = srv.devices().await;
    assert_eq!(infos[0].jetdirect_port, None, "embedded: no default 9100");
    let port = infos[1].jetdirect_port.expect("ephemeral port reported");
    assert_ne!(port, 0);
    let mut dc = DeviceConfig::derived("clash", "generic-wsd");
    dc.jetdirect_port = Some(port);
    let e = srv.manager().add(dc).await.unwrap_err();
    assert_eq!(fakeprint::manager::error_status(&e), 409, "{e:#}");
    let mut dc = DeviceConfig::derived("clash2", "generic-wsd");
    dc.jetdirect_port = Some(srv.port());
    let e = srv.manager().add(dc).await.unwrap_err();
    assert_eq!(fakeprint::manager::error_status(&e), 409);
    {
        use tokio::io::AsyncWriteExt;
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        s.write_all(b"%PDF-1.4 raw").await.unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(srv.device("raw").await.unwrap().info.live.captured_jobs, 1);
    assert_eq!(srv.device("a").await.unwrap().info.live.captured_jobs, 0);
    srv.manager().remove("raw").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .is_err());
    srv.shutdown().await;
}

#[tokio::test]
async fn embedded_announces_nothing_and_has_no_queue() {
    let tmp = TempDir::new("md-nothing");
    let srv = two(&tmp).await;
    assert!(srv
        .devices()
        .await
        .iter()
        .all(|d| d.jetdirect_port.is_none() && !d.announcing));
    assert!(srv.manager().queue().await.head.is_none());
    srv.shutdown().await;
}

#[tokio::test]
async fn mutations_need_a_loopback_peer_but_in_process_calls_pass() {
    use tower::ServiceExt;
    let tmp = TempDir::new("md-guard");
    let srv = two(&tmp).await;
    let mk = |id: &str, peer: Option<std::net::SocketAddr>| {
        let body = json!({"id": id, "profile": "generic-wsd"}).to_string();
        let mut req = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/devices")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body))
            .unwrap();
        if let Some(p) = peer {
            req.extensions_mut().insert(axum::extract::ConnectInfo(p));
        }
        req
    };
    let lan: std::net::SocketAddr = "192.168.1.9:5555".parse().unwrap();
    let r = srv.router().oneshot(mk("g", Some(lan))).await.unwrap();
    assert_eq!(r.status(), 403);
    // A dual-stack listener reports 127.0.0.1 as ::ffff:127.0.0.1. That is still loopback.
    // A mapped LAN address is not.
    let mapped_lo: std::net::SocketAddr = "[::ffff:127.0.0.1]:9".parse().unwrap();
    let mapped_lan: std::net::SocketAddr = "[::ffff:192.168.1.9]:9".parse().unwrap();
    let r = srv
        .router()
        .oneshot(mk("g", Some(mapped_lan)))
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    assert_eq!(srv.devices().await.len(), 2);
    let r = srv
        .router()
        .oneshot(mk("g", Some(mapped_lo)))
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    assert_eq!(srv.devices().await.len(), 3);
    let r = srv.router().oneshot(mk("h", None)).await.unwrap();
    assert_eq!(r.status(), 201);
    let mut req = axum::http::Request::builder()
        .uri("/d/a/v1/status")
        .body(axum::body::Body::empty())
        .unwrap();
    req.extensions_mut().insert(axum::extract::ConnectInfo(lan));
    assert_eq!(srv.router().oneshot(req).await.unwrap().status(), 200);
    srv.shutdown().await;
}

#[tokio::test]
async fn ctl_selects_a_device() {
    let tmp = TempDir::new("md-ctl");
    let srv = two(&tmp).await;
    let addr = srv.local_addr();
    let r = post_json(
        addr,
        "/v1/ctl",
        &json!({"cmd": "status", "args": {"device": "b"}}),
    )
    .await;
    assert_eq!(r.json()["device_id"], "b");
    let r = post_json(addr, "/v1/ctl", &json!({"cmd": "status"})).await;
    assert_eq!(r.json()["device_id"], "a");
    let r = post_json(
        addr,
        "/v1/ctl",
        &json!({"cmd": "status", "args": {"device": "zz"}}),
    )
    .await;
    assert_eq!(r.status, 404);
    srv.shutdown().await;
}
