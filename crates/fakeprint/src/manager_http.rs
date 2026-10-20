//! The manager router: the HTTP surface in front of the per-device routers
//! (docs/multi-device.md section 2).
//!
//! * `/health`, `/v1/devices[...]`, `/v1/profiles`, `/v1/queue[...]` are manager routes.
//! * `/d/{id}/...` is handed to device `{id}`'s own router with the prefix stripped (dispatch is
//!   dynamic: devices come and go at run time; unknown id is 404 `{"error":"unknown device"}`).
//! * every other path is a legacy alias of the default (first) device.
//!
//! Mutations of the device set are loopback-peer only (403 otherwise). A request without peer
//! information (an in-process `oneshot` call, an embedding application) is allowed.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, Path as UrlPath, Query, Request, State};
use axum::http::{StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use tower::ServiceExt;
use tracing::info_span;
use uuid::Uuid;

use crate::manager::{error_status, DeviceManager};
use crate::profile::PrinterProfile;
use crate::server::DeviceConfig;

type Mgr = Arc<DeviceManager>;
type Peer = Option<axum::extract::Extension<ConnectInfo<SocketAddr>>>;

fn json_error(code: StatusCode, msg: impl Into<String>) -> Response {
    (code, Json(json!({ "error": msg.into() }))).into_response()
}

fn err_response(e: anyhow::Error) -> Response {
    let code = StatusCode::from_u16(error_status(&e)).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    json_error(code, format!("{e:#}"))
}

/// True for a loopback peer, or when the peer is unknown (an in-process call).
/// IPv4-mapped IPv6 loopback (`::ffff:127.0.0.1`) counts; a mapped LAN address does not.
pub fn peer_allowed(peer: &Peer) -> bool {
    match peer {
        Some(axum::extract::Extension(ConnectInfo(a))) => crate::lab::is_loopback_ip(a.ip()),
        None => true,
    }
}

/// The 403 answer for a mutation from a non-loopback peer.
fn forbidden() -> Response {
    json_error(
        StatusCode::FORBIDDEN,
        "this operation is only allowed from the loopback interface",
    )
}

/// The router a server serves: manager routes, `/d/{id}` dispatch and the legacy aliases.
/// `trace` logs one span per request.
pub fn manager_router(manager: Arc<DeviceManager>, trace: bool) -> Router {
    let mut app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/v1/devices", get(devices_list).post(devices_add))
        .route("/v1/devices/{id}", delete(devices_remove))
        .route("/v1/devices/{id}/reconfigure", post(devices_reconfigure))
        .route("/v1/profiles", get(profiles_get))
        .merge(ui_routes())
        .route("/v1/queue", get(queue_get).post(queue_post))
        .route("/v1/queue/advance", post(queue_advance))
        .route("/v1/queue/skip", post(queue_skip))
        .route("/v1/registry", get(registry_get))
        .route("/v1/reconcile", post(reconcile_post))
        .route("/v1/devices/{id}/installed", post(device_installed))
        .route("/v1/devices/{id}/forget", post(device_forget))
        .route("/d/{id}", any(dispatch))
        .route("/d/{id}/", any(dispatch))
        .route("/d/{id}/{*rest}", any(dispatch))
        .fallback(dispatch);
    if trace {
        app = app.layer(
            tower_http::trace::TraceLayer::new_for_http().make_span_with(|req: &Request<_>| {
                let peer = req
                    .extensions()
                    .get::<ConnectInfo<SocketAddr>>()
                    .map(|ConnectInfo(a)| a.to_string())
                    .unwrap_or_else(|| "-".into());
                info_span!("http", method = %req.method(), uri = %req.uri(), peer = %peer)
            }),
        );
    }
    app.with_state(manager)
}

/// Strip `/d/{id}` and hand the request to that device's router; anything else goes to the
/// default device's router unchanged.
async fn dispatch(State(m): State<Mgr>, mut req: Request<Body>) -> Response {
    let path = req.uri().path().to_string();
    let (router, new_path) = if let Some(rest) = path.strip_prefix("/d/") {
        let (id, tail) = match rest.split_once('/') {
            Some((id, tail)) => (id, format!("/{tail}")),
            None => (rest, "/".to_string()),
        };
        match m.router_for(id) {
            Some(r) => (r, tail),
            None => return json_error(StatusCode::NOT_FOUND, "unknown device"),
        }
    } else {
        match m.default_router() {
            Some(r) => (r, path),
            None => return json_error(StatusCode::NOT_FOUND, "unknown device"),
        }
    };
    let pq = match req.uri().query() {
        Some(q) => format!("{new_path}?{q}"),
        None => new_path,
    };
    let mut parts = req.uri().clone().into_parts();
    parts.path_and_query = pq.parse().ok();
    match Uri::from_parts(parts) {
        Ok(u) => *req.uri_mut() = u,
        Err(_) => return json_error(StatusCode::BAD_REQUEST, "bad request path"),
    }
    match router.oneshot(req).await {
        Ok(r) => r,
        Err(never) => match never {},
    }
}

async fn devices_list(State(m): State<Mgr>) -> Json<Vec<crate::manager::DeviceInfo>> {
    Json(m.list().await)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AddBody {
    id: String,
    profile: Option<String>,
    device_uuid: Option<Uuid>,
    ipp_uuid: Option<Uuid>,
    friendly_name: Option<String>,
    jetdirect_port: Option<u16>,
    /// Per-printer multicast switch; absent = inherit the daemon's `--discovery`.
    discovery: Option<bool>,
}

async fn devices_add(State(m): State<Mgr>, peer: Peer, raw: Bytes) -> Response {
    if !peer_allowed(&peer) {
        return forbidden();
    }
    let body: AddBody = match serde_json::from_slice(&raw) {
        Ok(b) => b,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, format!("body: {e}")),
    };
    let profile = body
        .profile
        .or_else(|| m.config().default_profile.clone())
        .unwrap_or_else(|| "generic-wsd".to_string());
    let mut dc = DeviceConfig::derived(body.id, profile);
    if let Some(u) = body.device_uuid {
        dc.device_uuid = u;
    }
    dc.ipp_uuid = body.ipp_uuid;
    dc.friendly_name = body.friendly_name;
    dc.jetdirect_port = body.jetdirect_port;
    dc.discovery = body.discovery;
    match m.add(dc).await {
        Ok(info) => (StatusCode::CREATED, Json(info)).into_response(),
        Err(e) => err_response(e),
    }
}

#[derive(Debug, Deserialize)]
struct RemoveQuery {
    #[serde(default)]
    forget: bool,
}

async fn devices_remove(
    State(m): State<Mgr>,
    peer: Peer,
    UrlPath(id): UrlPath<String>,
    Query(q): Query<RemoveQuery>,
) -> Response {
    if !peer_allowed(&peer) {
        return forbidden();
    }
    match m.remove_row(&id, q.forget).await {
        Ok(()) => Json(json!({ "removed": id, "forget": q.forget })).into_response(),
        Err(e) => err_response(e),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReconfigureBody {
    profile: String,
}

async fn devices_reconfigure(
    State(m): State<Mgr>,
    peer: Peer,
    UrlPath(id): UrlPath<String>,
    raw: Bytes,
) -> Response {
    if !peer_allowed(&peer) {
        return forbidden();
    }
    let body: ReconfigureBody = match serde_json::from_slice(&raw) {
        Ok(b) => b,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, format!("body: {e}")),
    };
    match m.reconfigure(&id, &body.profile).await {
        Ok(info) => Json(info).into_response(),
        Err(e) => err_response(e),
    }
}

/// The operator page (`/ui`, loopback peers only) when the `ui` feature is on.
#[cfg(feature = "ui")]
fn ui_routes() -> Router<Mgr> {
    Router::new()
        .route("/ui", get(crate::ui::ui_page))
        .route("/ui/", get(crate::ui::ui_page))
}

#[cfg(not(feature = "ui"))]
fn ui_routes() -> Router<Mgr> {
    Router::new()
}

#[cfg(feature = "ui")]
async fn profiles_get(state: State<Mgr>) -> Response {
    crate::ui::list_profiles(state).await.into_response()
}

#[cfg(not(feature = "ui"))]
async fn profiles_get(State(m): State<Mgr>) -> Json<Vec<Value>> {
    Json(profile_summaries(m.config().profile_dir.as_deref()))
}

async fn queue_get(State(m): State<Mgr>) -> Json<crate::manager::QueueInfo> {
    Json(m.queue().await)
}

/// `GET /v1/profiles`: `[{id, label, description, formats, color_modes, sides, media_sizes,
/// input_bins, versions}]` for every profile in `dir` (`None`: the stock generic printer).
pub fn profile_summaries(dir: Option<&std::path::Path>) -> Vec<Value> {
    let profiles: Vec<PrinterProfile> = match dir {
        Some(d) => PrinterProfile::list_ids(d)
            .unwrap_or_default()
            .iter()
            .filter_map(|id| PrinterProfile::load(d, id).ok())
            .collect(),
        None => vec![PrinterProfile::generic()],
    };
    profiles
        .iter()
        .map(|p| {
            let s = crate::registry::config_summary(p);
            json!({
                "id": p.id,
                "label": p.label,
                "description": format!("{} {}", p.identity.manufacturer, p.identity.model),
                "formats": s.formats,
                "color_modes": s.color_modes,
                "sides": s.sides,
                "media_sizes": s.media_sizes,
                "input_bins": s.input_bins,
                "versions": s.versions,
            })
        })
        .collect()
}

// ---- install state, queue, registry (loopback only) ----

async fn registry_get(State(m): State<Mgr>, peer: Peer) -> Response {
    if !peer_allowed(&peer) {
        return forbidden();
    }
    Json(m.registry().await).into_response()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReconcileBody {
    #[serde(default)]
    printers: Vec<crate::registry::WindowsPrinter>,
}

async fn reconcile_post(State(m): State<Mgr>, peer: Peer, raw: Bytes) -> Response {
    if !peer_allowed(&peer) {
        return forbidden();
    }
    let body: ReconcileBody = match serde_json::from_slice(&raw) {
        Ok(b) => b,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, format!("body: {e}")),
    };
    match m.reconcile_with_windows(&body.printers) {
        Ok(r) => Json(r).into_response(),
        Err(e) => err_response(e),
    }
}

#[derive(Debug, Deserialize)]
struct InstalledBody {
    windows: crate::registry::WindowsPrinter,
    /// Accepted for documentation; the kind is derived from the port (section 3.4).
    #[serde(default)]
    #[allow(dead_code)]
    kind: Option<String>,
}

async fn device_installed(
    State(m): State<Mgr>,
    peer: Peer,
    UrlPath(id): UrlPath<String>,
    raw: Bytes,
) -> Response {
    if !peer_allowed(&peer) {
        return forbidden();
    }
    let body: InstalledBody = match serde_json::from_slice(&raw) {
        Ok(b) => b,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, format!("body: {e}")),
    };
    if let Err(e) = m.mark_installed(&id, body.windows).await {
        return err_response(e);
    }
    device_json(&m, &id).await
}

async fn device_forget(State(m): State<Mgr>, peer: Peer, UrlPath(id): UrlPath<String>) -> Response {
    if !peer_allowed(&peer) {
        return forbidden();
    }
    if let Err(e) = m.forget(&id).await {
        return err_response(e);
    }
    device_json(&m, &id).await
}

async fn device_json(m: &Mgr, id: &str) -> Response {
    match m.get(id).await {
        Some(h) => Json(h.info).into_response(),
        None => json_error(StatusCode::NOT_FOUND, "unknown device"),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OrderBody {
    order: Vec<String>,
}

async fn queue_post(State(m): State<Mgr>, peer: Peer, raw: Bytes) -> Response {
    if !peer_allowed(&peer) {
        return forbidden();
    }
    let body: OrderBody = match serde_json::from_slice(&raw) {
        Ok(b) => b,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, format!("body: {e}")),
    };
    match m.queue_reorder(&body.order).await {
        Ok(q) => Json(q).into_response(),
        Err(e) => err_response(e),
    }
}

async fn queue_advance(State(m): State<Mgr>, peer: Peer) -> Response {
    if !peer_allowed(&peer) {
        return forbidden();
    }
    Json(m.queue_advance().await).into_response()
}

async fn queue_skip(State(m): State<Mgr>, peer: Peer) -> Response {
    if !peer_allowed(&peer) {
        return forbidden();
    }
    Json(m.queue_skip().await).into_response()
}

// ---- ctl: manager commands ----

/// ctl commands that need a manager (HTTP `POST /v1/ctl` and the named pipe).
pub const MANAGER_CMDS: &[&str] = &[
    "devices",
    "device_add",
    "device_remove",
    "reconfigure",
    "installed",
    "mark_added",
    "forget",
    "reconcile",
    "levels",
    "queue",
    "queue_advance",
    "queue_skip",
    "queue_reorder",
    "registry",
    "profiles",
    "open_settings",
    "shutdown",
];

/// Manager commands that need a loopback peer over HTTP (device-set and install mutations,
/// queue, registry, process control). Reads of `devices`, `profiles`, `queue` and `levels` stay
/// open like the per-device control plane.
pub fn cmd_needs_loopback(cmd: &str) -> bool {
    !matches!(cmd, "devices" | "profiles" | "queue" | "levels")
}

type CtlResult = Result<Value, (StatusCode, String)>;

fn bad(msg: impl Into<String>) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, msg.into())
}

fn arg_str<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    args.get(key).and_then(Value::as_str)
}

fn to_json<T: serde::Serialize>(v: &T) -> CtlResult {
    serde_json::to_value(v).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

/// Run one manager command. `own` is the device of the router the call arrived on (the default
/// device for the pipe); `args.id` / `args.device` select another one.
pub async fn manager_cmd(m: &Mgr, cmd: &str, args: &Value, own: Option<&str>) -> CtlResult {
    use crate::server::api_error;
    let id = || -> Result<String, (StatusCode, String)> {
        arg_str(args, "id")
            .or_else(|| arg_str(args, "device"))
            .or(own)
            .map(str::to_string)
            .ok_or_else(|| bad("args.id required"))
    };
    match cmd {
        "devices" => to_json(&m.list().await),
        "device_add" => {
            let mut dc = DeviceConfig::derived(
                arg_str(args, "id").ok_or_else(|| bad("args.id required"))?,
                arg_str(args, "profile")
                    .or(m.config().default_profile.as_deref())
                    .unwrap_or("generic-wsd"),
            );
            if let Some(u) = arg_str(args, "device_uuid") {
                dc.device_uuid = u
                    .parse()
                    .map_err(|_| bad("args.device_uuid is not a UUID"))?;
            }
            if let Some(u) = arg_str(args, "ipp_uuid") {
                dc.ipp_uuid = Some(u.parse().map_err(|_| bad("args.ipp_uuid is not a UUID"))?);
            }
            dc.friendly_name = arg_str(args, "friendly_name").map(str::to_string);
            dc.jetdirect_port = args
                .get("jetdirect_port")
                .and_then(Value::as_u64)
                .and_then(|p| u16::try_from(p).ok());
            dc.discovery = args.get("discovery").and_then(Value::as_bool);
            to_json(&m.add(dc).await.map_err(api_error)?)
        }
        "device_remove" => {
            let id = arg_str(args, "id").ok_or_else(|| bad("args.id required"))?;
            let forget = args.get("forget").and_then(Value::as_bool).unwrap_or(false);
            m.remove_row(id, forget).await.map_err(api_error)?;
            Ok(json!({ "removed": id, "forget": forget }))
        }
        "reconfigure" => {
            let profile = arg_str(args, "profile").ok_or_else(|| bad("args.profile required"))?;
            to_json(&m.reconfigure(&id()?, profile).await.map_err(api_error)?)
        }
        "installed" => {
            let win: crate::registry::WindowsPrinter =
                serde_json::from_value(args.get("windows").cloned().unwrap_or(Value::Null))
                    .map_err(|e| bad(format!("args.windows: {e}")))?;
            let id = id()?;
            m.mark_installed(&id, win).await.map_err(api_error)?;
            to_json(&m.get(&id).await.map(|h| h.info))
        }
        "mark_added" => {
            let id = id()?;
            m.mark_installed_legacy(&id).await.map_err(api_error)?;
            to_json(&m.get(&id).await.map(|h| h.info))
        }
        "forget" => {
            let id = id()?;
            m.forget(&id).await.map_err(api_error)?;
            to_json(&m.get(&id).await.map(|h| h.info))
        }
        "reconcile" => {
            let printers: Vec<crate::registry::WindowsPrinter> = serde_json::from_value(
                args.get("printers")
                    .cloned()
                    .unwrap_or(Value::Array(vec![])),
            )
            .map_err(|e| bad(format!("args.printers: {e}")))?;
            to_json(&m.reconcile_with_windows(&printers).map_err(api_error)?)
        }
        "levels" => {
            let id = id()?;
            let world = m
                .world_of(&id)
                .ok_or_else(|| (StatusCode::NOT_FOUND, format!("unknown device {id:?}")))?;
            let map = |key: &str| -> Result<std::collections::BTreeMap<String, i32>, (StatusCode, String)> {
                match args.get(key) {
                    None | Some(Value::Null) => Ok(Default::default()),
                    Some(v) => serde_json::from_value(v.clone())
                        .map_err(|e| bad(format!("args.{key}: {e}"))),
                }
            };
            to_json(
                &crate::server::apply_levels(&world, &map("consumables")?, &map("inputs")?).await?,
            )
        }
        "queue" => to_json(&m.queue().await),
        "queue_advance" => to_json(&m.queue_advance().await),
        "queue_skip" => to_json(&m.queue_skip().await),
        "queue_reorder" => {
            let order: Vec<String> =
                serde_json::from_value(args.get("order").cloned().unwrap_or(Value::Null))
                    .map_err(|e| bad(format!("args.order: {e}")))?;
            to_json(&m.queue_reorder(&order).await.map_err(api_error)?)
        }
        "registry" => to_json(&m.registry().await),
        "profiles" => Ok(Value::Array(profile_summaries(
            m.config().profile_dir.as_deref(),
        ))),
        "open_settings" => {
            let page = arg_str(args, "page").unwrap_or("printers");
            if page != "printers" {
                return Err(bad("only page \"printers\" can be opened"));
            }
            Ok(json!({ "opened": m.open_settings(page) }))
        }
        "shutdown" => {
            if args.get("confirm").and_then(Value::as_bool) != Some(true) {
                return Err(bad("shutdown requires args.confirm = true"));
            }
            m.request_exit();
            Ok(json!({ "shutting_down": true }))
        }
        other => Err(bad(format!("unknown manager cmd {other:?}"))),
    }
}
