//! The fakeprint server as a library: control-plane router, WSD Print Service, IPP, and the
//! background tasks that keep the printer alive (timers, event delivery, WS-Discovery).
//!
//! `main.rs` is argument parsing around [`serve`]. An embedder (the Python binding, a test)
//! calls the same function with [`ServerConfig::embedded`] and gets the exact server the lab
//! runs, in process, on an ephemeral loopback port, with
//!
//! * no multicast, no WS-Discovery, no JetDirect listener, no named pipe, no instance lock,
//!   no Ctrl-C handler;
//! * no file written anywhere except the configured inbox directory;
//! * a graceful [`ServerHandle::shutdown`] that ends every subscription and stops every task.
//!
//! Control plane (JSON): `GET /health`, `GET /v1/status`, `GET /v1/jobs`, `GET /v1/jobs/{id}`,
//! `POST /v1/reset`, `POST /v1/profile`, `GET|POST /v1/ctl`, `GET|POST|DELETE /v1/faults`,
//! `GET|POST /v1/timing`, `GET|DELETE /v1/requests` (+ `/v1/requests/{seq}`, `/raw`, `GET|POST /config`: the raw request log, `docs/request-log.md`).
//!
//! `POST /v1/faults` also takes request-injection rules (`"inject": [...]`, `"inject_clear"`),
//! see `wsd::inject`; they apply to the WSD (and, per rule, IPP) routes and never to `/v1/*`.
//! `POST /v1/timing` switches job processing times at run time (`{"preset":"instant"}`).
//!
//! To mount the server inside another axum application use [`ServerHandle::router`] (the same
//! routes, sharing the running server's state) or build one yourself with [`router`] and
//! [`RouterOptions`].

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use axum::body::Bytes;
use axum::extract::{ConnectInfo, Extension, Path as UrlPath, State};
use axum::http::{header, HeaderMap, Request, StatusCode};
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, RwLock};
use tokio::task::{AbortHandle, JoinHandle};
use tower_http::trace::TraceLayer;
use tracing::{info, info_span, warn};
use uuid::Uuid;

use crate::discovery::{self, DiscoveryConfig, DiscoveryService, DiscoveryStats};
use crate::lab;
use crate::manager::{
    world_device_info, DeviceCtx, DeviceError, DeviceHandle, DeviceInfo, DeviceManager, DeviceMeta,
    DeviceTasks, ManagedDevice, ManagerConfig, Shared,
};
use crate::profile::PrinterProfile;
use crate::registry::{ConfigInfo, Install, RegistryStore};
use crate::state::{Status, World, WorldOptions};
use crate::wsd::ctx::{
    spawn_ticker, Clock, FaultBody, PrintCtx, PrintLimits, PrintTiming, TimingPatch,
};
use crate::wsd::eventing::{AddressPolicy, EventManager, HttpDeliver};
use crate::wsd::inject::{InjectRule, Injector, PendingRule};
use crate::wsd::soap::SoapRequest;
use crate::wsd::validate::{ReplyValidator, ValidatingDeliver};
use crate::wsd::{self, SharedWorld, WSD_PATHS};
use crate::{ipp, jetdirect};

/// Tick of the job timers and of the event pump.
pub const DEFAULT_TICK: Duration = Duration::from_millis(250);

/// What only the daemon (the `fakeprint` binary) turns on. Every field is ignored when
/// [`ServerConfig::embedded`] is set.
#[derive(Debug, Clone)]
pub struct DaemonOptions {
    /// Announce on WS-Discovery (multicast Hello/Bye, Probe answers).
    pub discovery: bool,
    /// Bind UDP :3702 to answer Probes.
    pub probe_listen: bool,
    /// JetDirect RAW listener port (`None` = off).
    pub jetdirect_port: Option<u16>,
    /// Windows named-pipe control surface.
    pub named_pipe: bool,
    /// Per-port single-instance lock (skipped for ephemeral ports).
    pub instance_lock: bool,
}

impl Default for DaemonOptions {
    fn default() -> Self {
        Self {
            discovery: true,
            probe_listen: !cfg!(windows),
            jetdirect_port: Some(9100),
            named_pipe: true,
            instance_lock: true,
        }
    }
}

/// One emulated printer of a server (docs/multi-device.md section 1.1). Every `Option` field
/// inherits the [`ServerConfig`] default when `None`.
#[derive(Debug, Clone)]
pub struct DeviceConfig {
    /// Slug `[A-Za-z0-9_-]{1,32}`; `"default"` is the legacy device.
    pub id: String,
    /// Profile id (`<profiles>/<id>.json`).
    pub profile: String,
    /// WSD device UUID.
    pub device_uuid: Uuid,
    /// IPP `printer-uuid`; `None`: [`lab::derive_ipp_uuid`] of `device_uuid`.
    pub ipp_uuid: Option<Uuid>,
    /// `None`: the default device shows the profile label, the others `fakeprint <id>`.
    pub friendly_name: Option<String>,
    /// `None`: see [`ServerConfig::resolved_devices`] (inbox rule, section 1.4).
    pub inbox_dir: Option<PathBuf>,
    /// `None`: inherit [`ServerConfig::timing`].
    pub timing: Option<PrintTiming>,
    /// `None`: inherit [`ServerConfig::limits`].
    pub limits: Option<PrintLimits>,
    /// Own JetDirect RAW listener port; `None`: none (section 2.4).
    pub jetdirect_port: Option<u16>,
    /// `None`: inherit the server (daemon: the registry row value).
    pub discovery: Option<bool>,
    /// Lab-safety override, default `false`: accept a WSD or IPP UUID outside
    /// [`lab::FAKE_UUID_PREFIX`]. Never reachable from the control plane (`POST /v1/devices`).
    pub allow_foreign_uuid: bool,
}

impl DeviceConfig {
    /// A device with an explicit WSD UUID.
    pub fn new(id: impl Into<String>, profile: impl Into<String>, uuid: Uuid) -> Self {
        Self {
            id: id.into(),
            profile: profile.into(),
            device_uuid: uuid,
            ipp_uuid: None,
            friendly_name: None,
            inbox_dir: None,
            timing: None,
            limits: None,
            jetdirect_port: None,
            discovery: None,
            allow_foreign_uuid: false,
        }
    }

    /// A device whose UUID is derived from its id ([`lab::derive_device_uuid`]).
    pub fn derived(id: impl Into<String>, profile: impl Into<String>) -> Self {
        let id = id.into();
        let uuid = lab::derive_device_uuid(&id);
        Self::new(id, profile, uuid)
    }
}

/// The id of the legacy / synthesized device.
pub const DEFAULT_DEVICE_ID: &str = "default";

/// True for `[A-Za-z0-9_-]{1,32}`.
pub fn valid_device_id(id: &str) -> bool {
    (1..=32).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// A [`DeviceConfig`] with every inherited value filled in and validated.
#[derive(Debug, Clone)]
pub struct ResolvedDevice {
    pub id: String,
    /// The first device: answers the legacy unprefixed routes.
    pub is_default: bool,
    /// `""` for the default device, `/d/<id>` for the others.
    pub base_path: String,
    pub profile: String,
    pub device_uuid: Uuid,
    pub ipp_uuid: Uuid,
    /// `None`: the profile's own friendly name / label (default device only).
    pub friendly_name: Option<String>,
    pub inbox_dir: PathBuf,
    pub timing: PrintTiming,
    pub limits: PrintLimits,
    pub jetdirect_port: Option<u16>,
    pub discovery: Option<bool>,
}

/// Everything [`serve`] needs, already resolved (CLI parsing is the caller's job).
#[derive(Clone)]
pub struct ServerConfig {
    /// Listen address; port 0 picks a free port (see [`ServerHandle::port`]).
    pub listen: SocketAddr,
    /// Allow a listen address that is not loopback. Without it [`serve`] refuses.
    pub allow_non_loopback: bool,
    /// Host advertised in the endpoint URLs and WS-Discovery; default: the listen IP (a LAN
    /// IPv4 when listening on all interfaces).
    pub advertise: Option<IpAddr>,
    /// Library use: switches off everything that touches the network beyond the listener or
    /// the machine beyond the inbox (see the module documentation) and ignores `daemon`.
    pub embedded: bool,
    /// Directory of `<id>.json` profiles (for `/v1/profile` and loading by name).
    pub profile_dir: Option<PathBuf>,
    /// Profile id to load from `profile_dir`.
    pub profile_name: String,
    /// A ready profile; wins over `profile_dir`/`profile_name`. With neither a directory nor
    /// a profile the stock [`PrinterProfile::generic`] printer is used (embedded only).
    pub profile: Option<PrinterProfile>,
    /// Where captured documents go (created if missing).
    pub inbox_dir: PathBuf,
    pub device_uuid: Uuid,
    /// IPP `printer-uuid`; default: derived from `device_uuid` (never equal to it).
    pub ipp_uuid: Option<Uuid>,
    /// Lab-safety override (default `false`): the synthesized default device and the devices
    /// restored from the registry may use UUIDs outside [`lab::FAKE_UUID_PREFIX`].
    pub allow_foreign_uuid: bool,
    /// Lab instance name (registry row, named pipe).
    pub lab_name: String,
    /// Serve the WSD endpoints.
    pub wsd: bool,
    /// Serve `/ipp/print`.
    pub ipp: bool,
    /// Serve the directed WS-Discovery Probe/Resolve HTTP endpoint (pure HTTP, no multicast).
    pub directed_discovery: bool,
    pub limits: PrintLimits,
    pub timing: PrintTiming,
    /// Force reply validation on/off; `None` follows `FAKEPRINT_VALIDATE_REPLIES`.
    pub validate_replies: Option<bool>,
    /// Which `NotifyTo` addresses subscriptions may use.
    pub address_policy: AddressPolicy,
    /// Push the current state to a new subscriber (WSDMON lab experiment).
    pub initial_events: bool,
    /// Initial event rate in seconds; `None`: the profile's `PrinterEventRate`.
    pub event_rate_secs: Option<u32>,
    /// Time source (tests inject a manual clock).
    pub clock: Option<Arc<dyn Clock>>,
    /// Period of the job timers and the event pump.
    pub tick: Duration,
    pub daemon: DaemonOptions,
    /// The printers. The first is the default device. Empty: one `"default"` device is
    /// synthesized from the per-server fields above, which are the defaults every device
    /// inherits (`profile_name`, `device_uuid`, `ipp_uuid`, `inbox_dir`, `limits`, `timing`).
    pub devices: Vec<DeviceConfig>,
    /// Daemon only: the registry file (`None` for embedded servers).
    pub registry: Option<PathBuf>,
    /// Daemon only: the devices come from the registry rows of this `listen`.
    pub manage: bool,
    /// WS-Discovery timings (`None`: the specification defaults; tests use
    /// [`discovery::DiscoveryTimings::immediate`]).
    pub discovery_timings: Option<discovery::DiscoveryTimings>,
    /// The raw request log every device starts with (`crate::reqlog`): on by default, 256
    /// records per device. `POST /d/<id>/v1/requests/config` changes it per device at run time.
    pub request_log: crate::reqlog::RequestLogConfig,
}

impl ServerConfig {
    /// A server for a test or a binding: loopback, ephemeral port, nothing but the inbox
    /// touched, validation on, no initial event push.
    pub fn embedded(inbox_dir: impl Into<PathBuf>) -> Self {
        Self {
            listen: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            allow_non_loopback: false,
            advertise: None,
            embedded: true,
            profile_dir: None,
            profile_name: "generic-wsd".to_string(),
            profile: None,
            inbox_dir: inbox_dir.into(),
            device_uuid: lab::random_fake_uuid(),
            ipp_uuid: None,
            allow_foreign_uuid: false,
            lab_name: lab::LAB_NAME.to_string(),
            wsd: true,
            ipp: true,
            directed_discovery: false,
            limits: PrintLimits::default(),
            timing: PrintTiming::default(),
            validate_replies: Some(true),
            address_policy: AddressPolicy::PrivateOnly,
            initial_events: false,
            event_rate_secs: None,
            clock: None,
            tick: DEFAULT_TICK,
            daemon: DaemonOptions::default(),
            devices: Vec::new(),
            registry: None,
            manage: false,
            discovery_timings: None,
            request_log: crate::reqlog::RequestLogConfig::default(),
        }
    }

    /// Replace the device list (the first entry is the default device).
    pub fn with_devices(mut self, devices: Vec<DeviceConfig>) -> Self {
        self.devices = devices;
        self
    }

    /// A registry-backed daemon (`fakeprint --manage`): loopback `127.0.0.1:3921`, discovery
    /// off (rule 7), devices loaded from the registry rows for `listen`.
    pub fn managed(
        profile_dir: impl Into<PathBuf>,
        inbox_dir: impl Into<PathBuf>,
        registry: impl Into<PathBuf>,
    ) -> Self {
        let mut cfg = Self::daemon(profile_dir, inbox_dir);
        cfg.listen = SocketAddr::from((Ipv4Addr::LOCALHOST, lab::LAB_PORT));
        cfg.daemon.discovery = false;
        cfg.registry = Some(registry.into());
        cfg.manage = true;
        cfg.devices = Vec::new();
        cfg
    }

    /// The device list [`serve`] runs: `devices`, or the one synthesized `"default"` device.
    ///
    /// Inheritance (section 1.1): `timing`/`limits` fall back to the server's; the inbox is
    /// `inbox_dir` itself for the synthesized device and `<inbox_dir>/<id>` for explicit ones
    /// unless set; the IPP UUID is derived; a non-default device is called `fakeprint <id>`.
    /// Validation (readable errors): slug ids, duplicate ids and UUIDs, no UUID equal to another
    /// device's WSD or IPP UUID, a WSD or IPP UUID outside [`lab::FAKE_UUID_PREFIX`] (unless
    /// allowed), friendly names starting with `fakeprint` (except the legacy
    /// default device). Unknown profiles are checked when the device is built.
    pub fn resolved_devices(&self) -> Result<Vec<ResolvedDevice>> {
        let synthesized = self.devices.is_empty();
        let list: Vec<DeviceConfig> = if synthesized {
            vec![DeviceConfig {
                id: DEFAULT_DEVICE_ID.to_string(),
                profile: self.profile_name.clone(),
                device_uuid: self.device_uuid,
                ipp_uuid: self.ipp_uuid,
                friendly_name: None,
                inbox_dir: None,
                timing: None,
                limits: None,
                jetdirect_port: None,
                discovery: None,
                allow_foreign_uuid: self.allow_foreign_uuid,
            }]
        } else {
            self.devices.clone()
        };
        let mut out: Vec<ResolvedDevice> = Vec::with_capacity(list.len());
        for (i, d) in list.into_iter().enumerate() {
            let r = self.resolve_device(d, i == 0, synthesized, &out)?;
            out.push(r);
        }
        Ok(out)
    }

    /// Resolve and validate one device against the devices already accepted (`existing`).
    /// `synthesized`: the device list was empty and this is the legacy default device. Also used
    /// by [`DeviceManager::add`].
    pub fn resolve_device(
        &self,
        d: DeviceConfig,
        is_default: bool,
        synthesized: bool,
        existing: &[ResolvedDevice],
    ) -> Result<ResolvedDevice, DeviceError> {
        if !valid_device_id(&d.id) {
            return Err(DeviceError::BadRequest(format!(
                "invalid device id {:?}: use 1-32 characters of A-Z a-z 0-9 _ -",
                d.id
            )));
        }
        if existing.iter().any(|o| o.id == d.id) {
            return Err(DeviceError::Conflict(format!(
                "duplicate device id {:?}",
                d.id
            )));
        }
        let legacy = is_default && d.id == DEFAULT_DEVICE_ID;
        if let Some(n) = &d.friendly_name {
            if !legacy && !n.to_ascii_lowercase().starts_with("fakeprint") {
                return Err(DeviceError::BadRequest(format!(
                    "friendly name {n:?} of device {:?} must start with \"fakeprint\"",
                    d.id
                )));
            }
        }
        // Identity is the UUID, never the name: only the fake namespace is ours.
        if !d.allow_foreign_uuid {
            if !lab::uuid_is_fake(&d.device_uuid) {
                return Err(DeviceError::BadRequest(format!(
                    "WSD UUID {} of device {:?} is outside the fake namespace {}*",
                    d.device_uuid,
                    d.id,
                    lab::FAKE_UUID_PREFIX
                )));
            }
            if let Some(u) = d.ipp_uuid.filter(|u| !lab::uuid_is_fake(u)) {
                return Err(DeviceError::BadRequest(format!(
                    "IPP UUID {u} of device {:?} is outside the fake namespace {}*",
                    d.id,
                    lab::FAKE_UUID_PREFIX
                )));
            }
        }
        // The IPP UUID must differ from the WSD UUID (Windows DAFWSDProvider collision).
        let ipp_uuid = match d.ipp_uuid {
            Some(u) if u != d.device_uuid => u,
            _ => lab::derive_ipp_uuid(d.device_uuid),
        };
        let friendly_name = match d.friendly_name {
            Some(n) => Some(n),
            None if legacy => None,
            None => Some(format!("fakeprint {}", d.id)),
        };
        let inbox_dir = match d.inbox_dir {
            Some(p) => p,
            None if synthesized || legacy => self.inbox_dir.clone(),
            None => self.inbox_dir.join(&d.id),
        };
        let r = ResolvedDevice {
            base_path: if is_default {
                String::new()
            } else {
                format!("/d/{}", d.id)
            },
            id: d.id,
            is_default,
            profile: d.profile,
            device_uuid: d.device_uuid,
            ipp_uuid,
            friendly_name,
            inbox_dir,
            timing: d.timing.unwrap_or_else(|| self.timing.clone()),
            limits: d.limits.unwrap_or_else(|| self.limits.clone()),
            jetdirect_port: d.jetdirect_port,
            discovery: d.discovery,
        };
        for o in existing {
            let mine = [r.device_uuid, r.ipp_uuid];
            let theirs = [o.device_uuid, o.ipp_uuid];
            if mine.iter().any(|u| theirs.contains(u)) {
                return Err(DeviceError::Conflict(format!(
                    "devices {:?} and {:?} share a UUID (WSD and IPP UUIDs must all differ)",
                    o.id, r.id
                )));
            }
            if let (Some(a), Some(b)) = (r.jetdirect_port, o.jetdirect_port) {
                if a != 0 && a == b {
                    return Err(DeviceError::Conflict(format!(
                        "JetDirect port {a} is already used by device {:?}",
                        o.id
                    )));
                }
            }
        }
        Ok(r)
    }

    /// The lab daemon: `0.0.0.0:3921`, the stable lab UUID, everything on.
    pub fn daemon(profile_dir: impl Into<PathBuf>, inbox_dir: impl Into<PathBuf>) -> Self {
        Self {
            listen: lab::default_listen(),
            allow_non_loopback: true,
            embedded: false,
            profile_dir: Some(profile_dir.into()),
            device_uuid: lab::lab_device_uuid(),
            ipp_uuid: Some(Uuid::parse_str(lab::LAB_IPP_UUID).unwrap_or_else(|_| Uuid::nil())),
            directed_discovery: true,
            validate_replies: None,
            initial_events: true,
            ..Self::embedded(inbox_dir)
        }
    }
}

/// Which routes [`router`] mounts. `Default` mounts only the control plane (`/`, `/health`,
/// `/v1/*`); switch the rest on field by field.
///
/// ```ignore
/// let app = fakeprint::server::router(
///     world,
///     &RouterOptions { wsd: true, ipp: true, ..RouterOptions::default() },
/// );
/// let outer = axum::Router::new().nest("/printer", app);
/// ```
#[derive(Clone, Default)]
pub struct RouterOptions {
    /// Mount the WSD endpoints (`/WebServices/Device`, `/WebServices/PrinterService`,
    /// `/PrintService`). Request injection applies to them.
    pub wsd: bool,
    /// Mount `/ipp/print` (and `/ipp/print/`). Injection applies to rules with
    /// `endpoint: "ipp"` only.
    pub ipp: bool,
    /// The service behind the directed discovery endpoint (`None`: not mounted).
    pub directed_discovery: Option<Arc<DiscoveryService>>,
    /// Log one span per request (a `tracing` span with method, URI and peer).
    pub trace: bool,
}

/// A running server. Dropping it asks every task to stop; [`ServerHandle::shutdown`] also
/// waits for them (and for the final delivery of `SubscriptionEnd` messages).
pub struct ServerHandle {
    local_addr: SocketAddr,
    base_url: String,
    device_endpoint: String,
    service_endpoint: String,
    device_uuid: Uuid,
    ipp_uuid: Uuid,
    inbox_dir: PathBuf,
    world: SharedWorld,
    validator: Arc<ReplyValidator>,
    injector: Arc<Injector>,
    router: Router,
    events: Arc<EventManager>,
    manager: Arc<DeviceManager>,
    _exit_rx: tokio::sync::watch::Receiver<bool>,
    stop_http: Option<oneshot::Sender<()>>,
    joins: Vec<JoinHandle<()>>,
    aborts: Vec<AbortHandle>,
    _lock: Option<lab::InstanceLock>,
}

impl ServerHandle {
    /// The device manager (the default device is the only device in M0).
    pub fn manager(&self) -> Arc<DeviceManager> {
        Arc::clone(&self.manager)
    }

    /// Turns `true` when a client asks the process to exit (ctl `shutdown`, tray Quit).
    pub fn exit_signal(&self) -> tokio::sync::watch::Receiver<bool> {
        self._exit_rx.clone()
    }

    /// Every device with fresh live state (`GET /v1/devices`).
    pub async fn devices(&self) -> Vec<DeviceInfo> {
        self.manager.list().await
    }

    /// One device by id.
    pub async fn device(&self, id: &str) -> Option<DeviceHandle> {
        self.manager.get(id).await
    }

    /// The bound address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn port(&self) -> u16 {
        self.local_addr.port()
    }

    /// `http://host:port` (no trailing slash).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// `http://host:port/WebServices/Device`
    pub fn device_endpoint(&self) -> &str {
        &self.device_endpoint
    }

    /// `http://host:port/WebServices/PrinterService`
    pub fn service_endpoint(&self) -> &str {
        &self.service_endpoint
    }

    pub fn device_uuid(&self) -> Uuid {
        self.device_uuid
    }

    pub fn ipp_uuid(&self) -> Uuid {
        self.ipp_uuid
    }

    /// The capture directory.
    pub fn inbox_dir(&self) -> &std::path::Path {
        &self.inbox_dir
    }

    /// The shared state (jobs, profile, status).
    pub fn world(&self) -> SharedWorld {
        Arc::clone(&self.world)
    }

    /// The current print context (jobs, engine, model, clock).
    pub async fn ctx(&self) -> Arc<PrintCtx> {
        self.world.read().await.print_ctx()
    }

    /// The subscription manager.
    pub fn events(&self) -> Arc<EventManager> {
        Arc::clone(&self.events)
    }

    /// Schema violations found in outgoing messages so far (newest 64).
    pub fn validation_problems(&self) -> Vec<String> {
        self.validator.recent_problems()
    }

    /// RFC 8011 violations found in outgoing IPP replies so far (newest 64; the same switch as
    /// [`ServerHandle::validation_problems`]).
    pub async fn ipp_validation_problems(&self) -> Vec<String> {
        let ctx = self.world.read().await.print_ctx();
        ctx.ipp.validator.recent_problems()
    }

    /// Number of schema violations found in outgoing messages so far.
    pub fn validation_problem_count(&self) -> u64 {
        self.validator.problem_count()
    }

    /// The reply validator itself (counters, switch).
    pub fn validator(&self) -> Arc<ReplyValidator> {
        Arc::clone(&self.validator)
    }

    /// The routes this server serves (the manager router: `/v1/devices`, `/d/{id}/...` and the
    /// legacy default-device aliases), sharing its state, for mounting in another axum
    /// application (`Router::nest` / `merge`). Cheap to call; every clone talks to the same
    /// printers. The server's own listener keeps running.
    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// The request-injection table (also reachable through `POST /v1/faults`).
    pub fn injector(&self) -> Arc<Injector> {
        Arc::clone(&self.injector)
    }

    /// Arm a request-injection rule. Refused (`Err`) when the rule is malformed.
    pub fn inject(&self, rule: InjectRule) -> Result<(), String> {
        self.injector.add(rule)
    }

    /// Drop every pending injection rule.
    pub fn clear_inject(&self) {
        self.injector.clear();
    }

    /// The pending injection rules with their remaining counts.
    pub fn pending_inject(&self) -> Vec<PendingRule> {
        self.injector.pending()
    }

    /// Effects injected so far.
    pub fn inject_hits(&self) -> u64 {
        self.injector.hits()
    }

    fn signal_stop(&mut self) {
        if let Some(tx) = self.stop_http.take() {
            let _ = tx.send(());
        }
        self.manager.stop_devices_sync();
    }

    /// Stop gracefully: close the listener, end every subscription (`SourceShuttingDown` is
    /// delivered best effort), say Bye on WS-Discovery, and wait for the tasks.
    pub async fn shutdown(mut self) {
        self.signal_stop();
        for h in self.aborts.drain(..) {
            h.abort();
        }
        for j in self.joins.drain(..) {
            let _ = j.await;
        }
        self.manager.shutdown().await;
    }

    /// Wait until the server task ends on its own (a `Drop`/`shutdown` stops it).
    pub async fn wait(&mut self) {
        if let Some(first) = self.joins.first_mut() {
            let _ = first.await;
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.signal_stop();
        for h in self.aborts.drain(..) {
            h.abort();
        }
    }
}

/// Load the profile `name` the way a device needs it: the ready `cfg.profile` for the default
/// device (`use_cfg_profile`), else `<profile_dir>/<name>.json`, else (embedded, no directory)
/// the stock generic printer for `generic-wsd`. Returns the profile and the profile directory.
pub(crate) fn load_profile(
    cfg: &ServerConfig,
    name: &str,
    use_cfg_profile: bool,
) -> Result<(PrinterProfile, PathBuf)> {
    let dir = cfg.profile_dir.clone().unwrap_or_default();
    if use_cfg_profile {
        if let Some(p) = &cfg.profile {
            return Ok((p.clone(), dir));
        }
    }
    match &cfg.profile_dir {
        Some(d) => Ok((
            PrinterProfile::load(d, name)
                .with_context(|| format!("profile {name:?} in {}", d.display()))?,
            dir,
        )),
        None if cfg.embedded && name == "generic-wsd" => Ok((PrinterProfile::generic(), dir)),
        None if cfg.embedded => {
            bail!("unknown profile {name:?} (no profile directory is configured)")
        }
        None => bail!("a profile or a profile directory is required"),
    }
}

/// Build and start one device: world, discovery service, router, ticker, event pump and (when
/// asked for) its JetDirect listener. Does not register it with the manager.
pub(crate) async fn build_device(
    shared: &Arc<Shared>,
    mgr: &Arc<DeviceManager>,
    mut r: crate::server::ResolvedDevice,
) -> Result<ManagedDevice> {
    let cfg = &shared.cfg;
    let (mut profile, profiles_dir) = load_profile(cfg, &r.profile, r.is_default)
        .map_err(|e| DeviceError::BadRequest(format!("{e:#}")))?;
    if let Some(name) = &r.friendly_name {
        profile.identity.friendly_name = Some(name.clone());
    }
    info!(device = %r.id, profile = %profile.id, "loaded profile");

    // JetDirect: the default device gets the daemon port (soft failure, as before); any device
    // may opt in with an explicit port (a conflict is an error).
    let jd_explicit = r.jetdirect_port;
    let jd_port = jd_explicit.or_else(|| {
        (r.is_default && !cfg.embedded)
            .then_some(shared.daemon.jetdirect_port)
            .flatten()
            .filter(|p| *p != 0)
    });
    let mut jd_listener = None;
    if let Some(port) = jd_port {
        if port != 0 && port == shared.http_port {
            return Err(
                DeviceError::Conflict(format!("JetDirect port {port} is the HTTP port")).into(),
            );
        }
        match jetdirect::bind(SocketAddr::new(shared.listen_ip, port)).await {
            Ok(l) => jd_listener = Some(l),
            Err(e) if jd_explicit.is_some() => {
                return Err(DeviceError::Conflict(format!(
                    "JetDirect port {port} cannot be bound: {e}"
                ))
                .into())
            }
            Err(e) => {
                warn!(port, error = %e, "JetDirect bind failed (TCPMON RAW jobs will stick)")
            }
        }
        r.jetdirect_port = jd_listener
            .as_ref()
            .and_then(|l| l.local_addr().ok())
            .map(|a| a.port());
    }

    let config = {
        let friendly = profile
            .identity
            .friendly_name
            .clone()
            .unwrap_or_else(|| profile.label.clone());
        ConfigInfo::for_profile(&profile, &friendly)
    };
    let device_endpoint = format!("{}{}/WebServices/Device", shared.base_url, r.base_path);
    let service_endpoint = format!(
        "{}{}/WebServices/PrinterService",
        shared.base_url, r.base_path
    );
    let stats = Arc::new(DiscoveryStats::default());
    let mut world = World::with_options(
        profiles_dir,
        r.inbox_dir.clone(),
        profile,
        Arc::clone(&stats),
        WorldOptions {
            clock: cfg
                .clock
                .clone()
                .unwrap_or_else(|| Arc::new(crate::wsd::ctx::SystemClock)),
            limits: r.limits.clone(),
            timing: r.timing.clone(),
            validate_replies: cfg.validate_replies,
            address_policy: cfg.address_policy,
            initial_events: cfg.initial_events,
            event_rate_secs: cfg.event_rate_secs,
        },
    )?;
    world.set_lab_name(&cfg.lab_name);
    world.set_device_id(&r.id);
    world.set_identity(
        device_endpoint.clone(),
        service_endpoint.clone(),
        r.device_uuid,
    );
    world.set_ipp_uuid(r.ipp_uuid);
    let events = world.events();
    let ctx = world.print_ctx();
    let validator = Arc::clone(&ctx.validator);
    let injector = Arc::clone(&ctx.inject);
    ctx.reqlog.configure(cfg.request_log.clone());
    let reqlog = Arc::clone(&ctx.reqlog);
    let world: SharedWorld = Arc::new(RwLock::new(world));

    // WS-Discovery: the service is shared by the multicast loop and the directed endpoint.
    let discovery_service = cfg
        .wsd
        .then(|| Arc::new(DiscoveryService::new(&discovery_config(shared, &r, &stats))));

    let app = router_with(
        Arc::clone(&world),
        &RouterOptions {
            wsd: cfg.wsd,
            ipp: cfg.ipp,
            directed_discovery: discovery_service.clone().filter(|_| cfg.directed_discovery),
            trace: false,
        },
        Some(DeviceCtx::new(Arc::downgrade(mgr), r.id.clone())),
    );

    let mut aborts: Vec<AbortHandle> =
        vec![spawn_ticker(Arc::clone(&ctx), cfg.tick).abort_handle()];
    let (stop_pump, pump_rx) = oneshot::channel();
    let deliver = Arc::new(ValidatingDeliver {
        inner: Arc::new(HttpDeliver {
            policy: cfg.address_policy,
            log: Some(reqlog),
            ..HttpDeliver::default()
        }),
        validator: Arc::clone(&validator),
    });
    let pump = tokio::spawn(wsd::eventing::run_pump(
        Arc::clone(&events),
        deliver,
        cfg.tick,
        pump_rx,
    ));
    if let Some(l) = jd_listener {
        let jd_world = Arc::clone(&world);
        aborts.push(
            tokio::spawn(async move { jetdirect::serve_listener(jd_world, l).await })
                .abort_handle(),
        );
    }
    Ok(ManagedDevice {
        cfg: r,
        wsd: cfg.wsd,
        ipp: cfg.ipp,
        world,
        events,
        validator,
        injector,
        router: app,
        install: Install::default(),
        config,
        stats,
        discovery: discovery_service,
        tasks: Arc::new(DeviceTasks {
            stop_pump: std::sync::Mutex::new(Some(stop_pump)),
            pump: std::sync::Mutex::new(Some(pump)),
            aborts: std::sync::Mutex::new(aborts),
        }),
    })
}

fn discovery_config(
    shared: &Shared,
    r: &crate::server::ResolvedDevice,
    stats: &Arc<DiscoveryStats>,
) -> DiscoveryConfig {
    let multicast_iface = match shared.adv_ip {
        IpAddr::V4(v4) if !v4.is_unspecified() => v4,
        _ => Ipv4Addr::UNSPECIFIED,
    };
    DiscoveryConfig {
        multicast_iface,
        listen_probes: shared.daemon.probe_listen,
        stats: Arc::clone(stats),
        timings: shared.cfg.discovery_timings.clone().unwrap_or_default(),
        ..DiscoveryConfig::new(
            format!("{}{}/WebServices/Device", shared.base_url, r.base_path),
            r.device_uuid,
        )
    }
}

/// Start the server. Returns once the listener is bound and the routes are live.
pub async fn serve(mut cfg: ServerConfig) -> Result<ServerHandle> {
    if !cfg.allow_non_loopback && !cfg.listen.ip().is_loopback() {
        bail!(
            "refusing to listen on {}: the address is not loopback (set allow_non_loopback to serve the network)",
            cfg.listen
        );
    }
    let daemon = if cfg.embedded {
        DaemonOptions {
            discovery: false,
            probe_listen: false,
            jetdirect_port: None,
            named_pipe: false,
            instance_lock: false,
        }
    } else {
        cfg.daemon.clone()
    };

    // Managed daemon: the devices are the registry rows of this `listen` (or the default one).
    let mut rows: Vec<(String, crate::registry::RegistryDevice)> = Vec::new();
    let mut queue_order: Vec<String> = Vec::new();
    if cfg.manage {
        let path = cfg
            .registry
            .clone()
            .context("a managed server needs a registry path")?;
        let reg = RegistryStore::open(path).load()?;
        let (devices, found) = crate::manager::devices_from_registry(&cfg, &reg);
        cfg.devices = devices;
        rows = found;
        queue_order = reg.queue_order.clone();
    }
    let devs = cfg.resolved_devices()?;

    let lock = if daemon.instance_lock && cfg.listen.port() != 0 {
        Some(lab::acquire_instance_lock_for(
            cfg.listen.port(),
            devs[0].device_uuid,
        )?)
    } else {
        None
    };
    let listener = lab::bind_listener(cfg.listen).await?;
    let local_addr = listener.local_addr()?;
    let adv_ip = match cfg.advertise.unwrap_or_else(|| local_addr.ip()) {
        ip if ip.is_unspecified() => lab::first_lan_ipv4()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        ip => ip,
    };
    let host = match adv_ip {
        IpAddr::V6(v6) => format!("[{v6}]"),
        v4 => v4.to_string(),
    };
    let base_url = format!("http://{host}:{}", local_addr.port());

    let (manager, exit_rx) = DeviceManager::new(ManagerConfig {
        registry: cfg.registry.clone(),
        manage: cfg.manage,
        embedded: cfg.embedded,
        profile_dir: cfg.profile_dir.clone(),
        inbox_dir: cfg.inbox_dir.clone(),
        default_profile: Some(cfg.profile_name.clone()),
    });
    let shared = Shared {
        cfg: cfg.clone(),
        daemon: daemon.clone(),
        base_url: base_url.clone(),
        adv_ip,
        listen_ip: local_addr.ip(),
        http_port: local_addr.port(),
    };
    manager.set_shared(shared);
    let shared = manager.shared_arc()?;
    let any_discovery = devs.iter().any(|r| r.discovery == Some(true));
    manager.set_discovery_default(daemon.discovery);
    for r in devs {
        let d = match build_device(&shared, &manager, r).await {
            Ok(d) => d,
            Err(e) => {
                manager.stop_devices().await;
                return Err(e);
            }
        };
        manager.register(d);
    }
    let first = manager
        .default_managed()
        .context("a server needs at least one device")?;
    manager.set_queue_order(queue_order);
    for (id, row) in rows {
        manager.set_install(&id, row.install, &row.added);
    }
    for d in manager.list().await {
        // fingerprints, staleness / healing against the stored install state, first-run row
        let _ = manager.refresh_config(&d.id).await;
    }

    let (listener, taps) = crate::reqlog::TapListener::new(listener);
    let app = crate::manager_http::manager_router(Arc::clone(&manager), !cfg.embedded)
        .layer(axum::middleware::from_fn(crate::reqlog::origin_layer))
        .layer(Extension(taps));
    let handle_router = app.clone();
    let mut joins: Vec<JoinHandle<()>> = Vec::new();
    let mut aborts: Vec<AbortHandle> = Vec::new();

    // The listener task goes first so `wait` follows the HTTP server.
    let (stop_http, http_rx) = oneshot::channel::<()>();
    joins.push(tokio::spawn(async move {
        // the tap listener is a pass-through; `tap_io` is how axum derives `ConnectInfo<SocketAddr>`
        // for a listener that is not a plain `TcpListener`
        let served = axum::serve(
            axum::serve::ListenerExt::tap_io(listener, |_| {}),
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = http_rx.await;
        })
        .await;
        if let Err(e) = served {
            warn!(error = %e, "HTTP server stopped with an error");
        }
    }));

    // WS-Discovery: one hub (one socket) for every device; the install queue decides which
    // device announces. Off unless discovery is switched on for the daemon or for a row.
    if !cfg.embedded {
        let want_hub = cfg.wsd && (daemon.discovery || any_discovery);
        let mut hub = None;
        if want_hub {
            let iface = match adv_ip {
                IpAddr::V4(v4) if !v4.is_unspecified() => v4,
                _ => Ipv4Addr::UNSPECIFIED,
            };
            match discovery::DiscoveryHub::start(
                iface,
                daemon.probe_listen,
                discovery::DiscoveryTimings::default().hello_interval,
            ) {
                Ok(h) => hub = Some(h),
                Err(e) => tracing::error!(error = %e, "WS-Discovery stopped"),
            }
        }
        match hub {
            Some(h) => manager.attach_hub(h),
            None => manager.enable_queue(),
        }
        let weak = Arc::downgrade(&manager);
        aborts.push(
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    match weak.upgrade() {
                        Some(m) => m.queue_step(),
                        None => break,
                    }
                }
            })
            .abort_handle(),
        );
    }
    #[cfg(windows)]
    if daemon.named_pipe {
        let pipe_manager = Arc::clone(&manager);
        let pipe_name = cfg.lab_name.clone();
        aborts.push(
            tokio::spawn(
                async move { crate::pipe::serve_named_pipe(&pipe_name, pipe_manager).await },
            )
            .abort_handle(),
        );
    }

    info!(listen = %local_addr, name = %cfg.lab_name, "listening");
    let inbox_dir = first.cfg.inbox_dir.clone();
    Ok(ServerHandle {
        local_addr,
        base_url,
        device_endpoint: format!("{}/WebServices/Device", shared.base_url),
        service_endpoint: format!("{}/WebServices/PrinterService", shared.base_url),
        device_uuid: first.cfg.device_uuid,
        ipp_uuid: first.cfg.ipp_uuid,
        inbox_dir,
        world: first.world,
        validator: first.validator,
        injector: first.injector,
        router: handle_router,
        events: first.events,
        manager,
        _exit_rx: exit_rx,
        stop_http: Some(stop_http),
        joins,
        aborts,
        _lock: lock,
    })
}

// ---------------------------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------------------------

/// The routes the binary serves: control plane, optionally IPP, the WSD endpoints and the
/// directed discovery endpoint.
pub fn router(world: SharedWorld, opts: &RouterOptions) -> Router {
    router_with(world, opts, None)
}

/// [`router`] plus the manager context of the device (installed as an `Extension` so handlers
/// reach the [`DeviceManager`]). Without it a router reports itself as the one `default` device
/// on `GET /v1/devices`.
pub fn router_with(world: SharedWorld, opts: &RouterOptions, device: Option<DeviceCtx>) -> Router {
    let mut app = Router::new()
        .route("/", get(presentation))
        .route("/health", get(health))
        .route("/v1/status", get(status))
        .route("/v1/reset", post(reset))
        .route("/v1/jobs", get(list_jobs).post(seed_jobs))
        .route("/v1/jobs/{id}", get(get_job))
        .route("/v1/profile", post(set_profile))
        .route("/v1/devices", get(devices_get))
        .route("/v1/levels", post(levels_post))
        .route("/v1/ctl", get(ctl_get).post(ctl_post))
        .route(
            "/v1/faults",
            get(faults_get).post(faults_post).delete(faults_delete),
        )
        .route("/v1/timing", get(timing_get).post(timing_post))
        .route(
            "/v1/requests",
            get(crate::reqlog::list_handler).delete(crate::reqlog::clear_handler),
        )
        .route(
            "/v1/requests/config",
            get(crate::reqlog::config_get).post(crate::reqlog::config_post),
        )
        .route("/v1/requests/{seq}", get(crate::reqlog::one_handler))
        .route("/v1/requests/{seq}/raw", get(crate::reqlog::raw_handler));
    let capture = || axum::middleware::from_fn_with_state(world.clone(), crate::reqlog::capture);
    if opts.ipp {
        let ipp_routes = Router::new()
            .route(
                "/ipp/print",
                get(ipp::print_handler).post(ipp::print_handler),
            )
            .route(
                "/ipp/print/",
                get(ipp::print_handler).post(ipp::print_handler),
            )
            .route(
                "/ipp/print/{*job}",
                get(ipp::print_handler).post(ipp::print_handler),
            );
        // The handler reads the body with `limits.max_body` (not axum's 2 MiB `Bytes` default,
        // and not the old 257 MiB extractor cap). A larger POST is HTTP 413.
        app = app.merge(with_probe(ipp_routes.layer(capture()), &device));
    }
    if opts.wsd {
        let mut wsd_routes = Router::new();
        for path in WSD_PATHS {
            wsd_routes = wsd_routes.route(path, post(wsd::print_service));
        }
        app = app.merge(with_probe(wsd_routes.layer(capture()), &device));
    }
    if let Some(service) = &opts.directed_discovery {
        let directed = Router::new()
            .route(discovery::DIRECTED_PATH, post(directed_discovery))
            .layer(Extension(Arc::clone(service)));
        app = app.merge(with_probe(directed.layer(capture()), &device));
    }
    if let Some(ctx) = &device {
        app = app.layer(Extension(ctx.clone()));
    }
    if opts.trace {
        app = app.layer(
            TraceLayer::new_for_http().make_span_with(|req: &Request<_>| {
                let peer = req
                    .extensions()
                    .get::<ConnectInfo<SocketAddr>>()
                    .map(|ConnectInfo(a)| a.to_string())
                    .unwrap_or_else(|| "-".into());
                info_span!("http", method = %req.method(), uri = %req.uri(), peer = %peer)
            }),
        );
    }
    app.with_state(world)
}

/// Report the peer of every request on these routes to the manager (`new -> probed`).
fn with_probe(routes: Router<SharedWorld>, device: &Option<DeviceCtx>) -> Router<SharedWorld> {
    match device {
        Some(ctx) => routes.layer(axum::middleware::from_fn_with_state(
            ctx.clone(),
            probe_peer,
        )),
        None => routes,
    }
}

async fn probe_peer(
    State(ctx): State<DeviceCtx>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if let Some(ConnectInfo(addr)) = req.extensions().get::<ConnectInfo<SocketAddr>>() {
        let (ip, ctx) = (addr.ip(), ctx.clone());
        tokio::spawn(async move { ctx.probe(ip).await });
    }
    next.run(req).await
}

/// Directed (unicast) WS-Discovery Probe / Resolve over HTTP.
async fn directed_discovery(
    Extension(service): Extension<Arc<DiscoveryService>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    let reply = match SoapRequest::parse(content_type, None, &body) {
        Ok(req) => service.handle_directed(&req),
        Err(fault) => fault.to_reply(None),
    };
    wsd::http_response(reply.into_http())
}

/// `GET /v1/devices`: the list of `DeviceInfo` (docs/multi-device.md 2.2). M0: the default device
/// only. A router mounted without a manager reports itself as the single `default` device.
async fn devices_get(
    State(world): State<SharedWorld>,
    ctx: Option<Extension<DeviceCtx>>,
) -> Json<Vec<DeviceInfo>> {
    if let Some(m) = ctx.and_then(|Extension(c)| c.manager()) {
        return Json(m.list().await);
    }
    Json(vec![
        world_device_info(&world, &DeviceMeta::default_device(), &Install::default()).await,
    ])
}

/// `POST /v1/levels` `{"consumables":{"Black":15},"inputs":{"Tray1":0}}`: set consumable and
/// input-bin levels (0..=100, -1 unknown). Runtime only: publishes `PrinterElementsChange`, never
/// touches fingerprints. Answers the device's `live` block.
async fn levels_post(
    State(world): State<SharedWorld>,
    raw: Bytes,
) -> Result<Json<crate::manager::DeviceLive>, ApiError> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        #[serde(default)]
        consumables: std::collections::BTreeMap<String, i32>,
        #[serde(default)]
        inputs: std::collections::BTreeMap<String, i32>,
    }
    let body: Body = parse_body(&raw)?;
    apply_levels(&world, &body.consumables, &body.inputs)
        .await
        .map(Json)
}

/// Validate and apply consumable / input-bin levels, answering the device's `live` block.
pub(crate) async fn apply_levels(
    world: &SharedWorld,
    consumables: &std::collections::BTreeMap<String, i32>,
    inputs: &std::collections::BTreeMap<String, i32>,
) -> Result<crate::manager::DeviceLive, ApiError> {
    {
        let w = world.read().await;
        let p = w.live_profile();
        let bad = |m: String| (StatusCode::BAD_REQUEST, m);
        for (name, level) in consumables {
            let known = p.consumables.iter().flatten().any(|c| &c.name == name);
            if !known || !(-1..=100).contains(level) {
                return Err(bad(format!(
                    "unknown consumable {name:?} or level {level} outside -1..=100"
                )));
            }
        }
        for (name, level) in inputs {
            let known = p.inputs.iter().flatten().any(|b| &b.name == name);
            if !known || !(-1..=100).contains(level) {
                return Err(bad(format!(
                    "unknown input {name:?} or level {level} outside -1..=100"
                )));
            }
        }
        for (name, level) in consumables {
            w.set_consumable_level(name, *level);
        }
        for (name, level) in inputs {
            w.set_input_level(name, *level);
        }
    }
    Ok(
        world_device_info(world, &DeviceMeta::default_device(), &Install::default())
            .await
            .live,
    )
}

async fn health() -> &'static str {
    "ok"
}

/// Minimal page for ThisModel PresentationUrl (Settings "Acquiring data").
async fn presentation(State(world): State<SharedWorld>) -> axum::response::Html<String> {
    let label = world.read().await.profile().label.clone();
    let label = crate::wsd::xml::esc(&label);
    axum::response::Html(format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>{label}</title></head>\
         <body><h1>{label}</h1><p>fakeprint lab presentation</p></body></html>"
    ))
}

async fn status(State(world): State<SharedWorld>) -> Json<Status> {
    Json(world.read().await.status())
}

async fn reset(State(world): State<SharedWorld>) -> Json<Status> {
    let mut w = world.write().await;
    w.reset();
    Json(w.status())
}

/// One print job as the control plane lists it: `id`, `name`, `user`, `ticket` (the effective
/// stored ticket as typed JSON, `null` for IPP/raw jobs), `state`, `reasons`, `created` /
/// `ended` (ISO 8601 UTC, plus `*_unix_ms`), counters and
/// `documents: [{seq, id, format, compression, bytes, file, path}]`.
fn job_row(j: &crate::wsd::types::PrintJob, log: &crate::reqlog::RequestLog) -> serde_json::Value {
    let mut row = crate::wsd::jobs::job_meta_json(j, None);
    let docs: Vec<serde_json::Value> = j
        .documents
        .iter()
        .map(|d| {
            serde_json::json!({
                "seq": d.id,
                "id": d.id,
                "format": d.format,
                "compression": d.compression,
                "bytes": d.size,
                "file": d.capture_path.as_ref().and_then(|p| p.file_name()).and_then(|n| n.to_str()),
                "path": d.capture_path.as_ref().map(|p| p.display().to_string()),
            })
        })
        .collect();
    if let Some(o) = row.as_object_mut() {
        o.insert("id".into(), j.id.into());
        let meta = log.job_meta(j.id).unwrap_or_default();
        o.insert("request_seqs".into(), json_seqs(&meta.request_seqs));
        o.insert("ticket_received".into(), meta.ticket_received.into());
        o.insert(
            "ticket_substitutions".into(),
            serde_json::to_value(&meta.substitutions).unwrap_or_default(),
        );
        o.insert("documents".into(), docs.into());
        o.insert("k_octets_processed".into(), j.k_octets_processed().into());
        o.insert(
            "media_sheets_completed".into(),
            j.media_sheets_completed.into(),
        );
        o.insert(
            "last_document_received".into(),
            j.last_document_received.into(),
        );
    }
    row
}

fn json_seqs(seqs: &[u64]) -> serde_json::Value {
    serde_json::Value::Array(seqs.iter().map(|s| (*s).into()).collect())
}

/// Captured documents (`jobs`, as before) and the print jobs, history included (`print_jobs`,
/// ascending by id).
fn jobs_json(w: &World) -> serde_json::Value {
    let ctx = w.print_ctx();
    let mut all = ctx.jobs.history();
    all.extend(ctx.jobs.active());
    all.sort_by_key(|j| j.id);
    let rows: Vec<serde_json::Value> = all.iter().map(|j| job_row(j, &ctx.reqlog)).collect();
    serde_json::json!({ "jobs": w.captured(), "print_jobs": rows })
}

async fn list_jobs(State(world): State<SharedWorld>) -> Json<serde_json::Value> {
    Json(jobs_json(&*world.read().await))
}

/// One job of `POST /v1/jobs`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedSpecBody {
    name: Option<String>,
    user: Option<String>,
    /// `pending`, `processing`, `completed` (default), `canceled`, `aborted`.
    state: Option<String>,
    /// For `aborted`: `timeout` (default), `format`, `transfer`, `compression`.
    abort_reason: Option<String>,
    /// Shorthand for one document.
    format: Option<String>,
    bytes: Option<u64>,
    documents: Option<Vec<SeedDocBody>>,
    copies: Option<u32>,
    /// How long ago the job was created / finished, on the engine clock.
    age_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedDocBody {
    format: Option<String>,
    bytes: Option<u64>,
}

fn seed_from(b: SeedSpecBody) -> Result<crate::wsd::jobs::SeedJob, String> {
    use crate::wsd::types::{JobState, JobStateReason};
    let state = match b.state.as_deref().unwrap_or("completed") {
        "pending" => JobState::Pending,
        "processing" => JobState::Processing,
        "completed" => JobState::Completed,
        "canceled" | "cancelled" => JobState::Canceled,
        "aborted" => JobState::Aborted,
        other => {
            return Err(format!(
                "state {other:?}: use pending, processing, completed, canceled or aborted"
            ))
        }
    };
    let abort_reason = match b.abort_reason.as_deref() {
        None | Some("timeout") => None,
        Some("format") => Some(JobStateReason::DocumentFormatError),
        Some("transfer") => Some(JobStateReason::DocumentTransferError),
        Some("compression") => Some(JobStateReason::CompressionError),
        Some(other) => return Err(format!("abort_reason {other:?} is not known")),
    };
    let documents: Vec<(String, u64)> = match b.documents {
        Some(list) => list
            .into_iter()
            .map(|d| {
                (
                    d.format.unwrap_or_else(|| "application/pdf".into()),
                    d.bytes.unwrap_or(1024),
                )
            })
            .collect(),
        None if state == JobState::Pending && b.format.is_none() && b.bytes.is_none() => Vec::new(),
        None => vec![(
            b.format.unwrap_or_else(|| "application/pdf".into()),
            b.bytes.unwrap_or(1024),
        )],
    };
    Ok(crate::wsd::jobs::SeedJob {
        name: b.name.unwrap_or_else(|| "seeded".into()),
        user: b.user.unwrap_or_else(|| "seed".into()),
        state,
        abort_reason,
        documents,
        copies: b.copies.unwrap_or(1),
        age: Duration::from_millis(b.age_ms.unwrap_or(0)),
    })
}

/// `POST /v1/jobs`: put jobs straight into the table in a chosen state, for client test suites
/// that need `Get-Jobs` / reconcile scenarios without waiting. Body: one job object or
/// `{"jobs": [...]}` (at most 1000); answers `{"created": [ids]}`. The jobs are ordinary
/// engine jobs: both protocols list them, `cancel` works on the non-terminal ones.
async fn seed_jobs(
    State(world): State<SharedWorld>,
    raw: Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    let v: serde_json::Value = parse_body(&raw)?;
    let list = match v.get("jobs") {
        Some(j) => j.clone(),
        None => serde_json::Value::Array(vec![v]),
    };
    let specs: Vec<SeedSpecBody> = serde_json::from_value(list)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad job description: {e}")))?;
    if specs.len() > 1000 {
        return Err((StatusCode::BAD_REQUEST, "at most 1000 jobs at once".into()));
    }
    let seeds = specs
        .into_iter()
        .map(seed_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let ctx = world.read().await.print_ctx();
    let mut ids = Vec::new();
    for s in seeds {
        let id = ctx
            .engine
            .seed_job(s)
            .map_err(|f| (StatusCode::CONFLICT, f.reason.to_string()))?;
        ids.push(id);
    }
    Ok(Json(serde_json::json!({ "created": ids })))
}

async fn get_job(
    State(world): State<SharedWorld>,
    UrlPath(id): UrlPath<u32>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let ctx = world.read().await.print_ctx();
    ctx.jobs
        .get(id)
        .map(|j| Json(job_row(&j, &ctx.reqlog)))
        .ok_or(StatusCode::NOT_FOUND)
}

// ---- profile -------------------------------------------------------------------------------

/// `POST /v1/profile`: `{"id": "..."}` loads `<profiles>/<id>.json`; `{"profile": {...}}`
/// installs a schema-2 profile document directly.
#[derive(Debug, Deserialize)]
struct ProfileBody {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    profile: Option<serde_json::Value>,
}

pub(crate) type ApiError = (StatusCode, String);

fn swap_profile(w: &mut World, body: &ProfileBody) -> Result<(), ApiError> {
    let bad = |e: anyhow::Error| (StatusCode::BAD_REQUEST, format!("{e:#}"));
    match (&body.id, &body.profile) {
        (Some(id), None) => w.hot_swap_profile(id).map_err(bad),
        (None, Some(doc)) => {
            let p = PrinterProfile::from_json(&doc.to_string()).map_err(bad)?;
            w.swap_profile(p);
            Ok(())
        }
        _ => Err((
            StatusCode::BAD_REQUEST,
            "give exactly one of \"id\" or \"profile\"".into(),
        )),
    }
}

async fn set_profile(
    State(world): State<SharedWorld>,
    ctx: Option<Extension<DeviceCtx>>,
    Json(body): Json<ProfileBody>,
) -> Result<Json<Status>, ApiError> {
    if let (Some(id), None, Some(m)) = (
        body.id.as_deref().filter(|i| !i.is_empty()),
        &body.profile,
        ctx.as_ref().and_then(|Extension(c)| c.manager()),
    ) {
        let dev = ctx
            .as_ref()
            .map(|Extension(c)| c.id.clone())
            .unwrap_or_default();
        m.reconfigure(&dev, id).await.map_err(api_error)?;
        return Ok(Json(world.read().await.status()));
    }
    let status = {
        let mut w = world.write().await;
        swap_profile(&mut w, &body)?;
        w.status()
    };
    if let Some(Extension(c)) = &ctx {
        if let Some(m) = c.manager() {
            let _ = m.refresh_config(&c.id).await;
        }
    }
    Ok(Json(status))
}

// ---- faults --------------------------------------------------------------------------------

/// Armed one-shot faults (`pending`) and what is switched on (`active`).
fn faults_json(ctx: &PrintCtx) -> serde_json::Value {
    let plan = ctx.engine.faults();
    let st = ctx.status();
    let conditions: Vec<serde_json::Value> = st
        .active_conditions
        .iter()
        .map(|c| {
            serde_json::json!({
                "id": c.id,
                "name": c.name,
                "component": c.component,
                "severity": c.severity.token(),
                "time": c.time,
            })
        })
        .collect();
    serde_json::json!({
        "pending": {
            "fail_next_document": plan.fail_next_document.map(|f| format!("{f:?}")),
        },
        "active": {
            "not_accepting_jobs": plan.not_accepting_jobs,
            "temporary_error": plan.temporary_error,
            "internal_error": plan.internal_error,
            "document_active": plan.document_active,
            "conditions": conditions,
            "printer_state": st.state.token(),
            "state_reasons": st.reasons,
        },
        "inject": crate::wsd::inject::inject_json(&ctx.inject),
    })
}

async fn faults_get(State(world): State<SharedWorld>) -> Json<serde_json::Value> {
    let ctx = world.read().await.print_ctx();
    Json(faults_json(&ctx))
}

/// `POST /v1/faults` with a [`FaultBody`]: raise or clear conditions and switch service faults.
async fn faults_post(
    State(world): State<SharedWorld>,
    raw: Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    let body: FaultBody = parse_body(&raw)?;
    let ctx = world.read().await.print_ctx();
    ctx.apply_faults_checked(&body)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    Ok(Json(faults_json(&ctx)))
}

// ---- timing --------------------------------------------------------------------------------

async fn timing_get(State(world): State<SharedWorld>) -> Json<serde_json::Value> {
    Json(world.read().await.print_ctx().timing().to_json())
}

/// `POST /v1/timing`: `{"preset":"instant"|"realistic"|"default", "processing_base_ms":..,
/// "processing_per_mib_ms":.., "document_timeout_ms":.., "terminating_ms":..,
/// "document_busy_ms":.., "bytes_per_sheet":..}`; every key optional. Applies to documents that
/// arrive after the call.
async fn timing_post(
    State(world): State<SharedWorld>,
    raw: Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    let patch: TimingPatch = parse_body(&raw)?;
    apply_timing(&world, &patch).await.map(Json)
}

/// JSON body of a control call; a malformed or unknown field is a 400 with the reason.
fn parse_body<T: serde::de::DeserializeOwned>(raw: &[u8]) -> Result<T, ApiError> {
    let raw = if raw.iter().all(u8::is_ascii_whitespace) {
        &b"{}"[..]
    } else {
        raw
    };
    serde_json::from_slice(raw).map_err(|e| (StatusCode::BAD_REQUEST, format!("body: {e}")))
}

async fn apply_timing(
    world: &SharedWorld,
    patch: &TimingPatch,
) -> Result<serde_json::Value, ApiError> {
    let ctx = world.read().await.print_ctx();
    let mut t = ctx.timing();
    t.apply(patch).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    ctx.set_timing(t);
    Ok(ctx.timing().to_json())
}

/// `DELETE /v1/faults`: clear every condition and flag.
async fn faults_delete(State(world): State<SharedWorld>) -> Json<serde_json::Value> {
    let ctx = world.read().await.print_ctx();
    ctx.apply_faults(&FaultBody {
        reset: true,
        ..FaultBody::default()
    });
    Json(faults_json(&ctx))
}

// ---- ctl -----------------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct CtlInfo {
    name: String,
    port: u16,
    uuid: String,
    base_url: String,
    pipe: String,
    cmds: Vec<&'static str>,
}

async fn ctl_get(State(world): State<SharedWorld>) -> Json<CtlInfo> {
    let w = world.read().await;
    let st = w.status();
    // Extract port from device_endpoint or fall back to lab default.
    let port = st
        .device_endpoint
        .split(':')
        .next_back()
        .and_then(|s| s.split('/').next())
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(lab::LAB_PORT);
    let name = st.name.clone();
    let pipe = lab::pipe_path(&name);
    Json(CtlInfo {
        name,
        port,
        uuid: st.device_uuid.to_string(),
        base_url: format!("http://127.0.0.1:{port}"),
        pipe,
        cmds: [
            "status", "health", "jobs", "reset", "profile", "faults", "timing", "save",
        ]
        .into_iter()
        .chain(crate::manager_http::MANAGER_CMDS.iter().copied())
        .collect(),
    })
}

#[derive(Debug, Deserialize)]
pub(crate) struct CtlRequest {
    pub cmd: String,
    #[serde(default)]
    pub args: serde_json::Value,
}

async fn ctl_post(
    State(world): State<SharedWorld>,
    ctx: Option<Extension<DeviceCtx>>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    Json(req): Json<CtlRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // Manager commands (device set, install state, queue, process).
    if crate::manager_http::MANAGER_CMDS.contains(&req.cmd.as_str()) {
        let Some(m) = ctx.as_ref().and_then(|Extension(c)| c.manager()) else {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("{:?} needs a device manager; this router has none", req.cmd),
            ));
        };
        if crate::manager_http::cmd_needs_loopback(&req.cmd)
            && !crate::manager_http::peer_allowed(&peer)
        {
            return Err((
                StatusCode::FORBIDDEN,
                "this command is only allowed from the loopback interface".into(),
            ));
        }
        let own = ctx.as_ref().map(|Extension(c)| c.id.clone());
        return crate::manager_http::manager_cmd(&m, &req.cmd, &req.args, own.as_deref())
            .await
            .map(Json);
    }
    // `args.device` selects another printer of the same server.
    let world = match (req.args.get("device").and_then(|v| v.as_str()), ctx.clone()) {
        (Some(id), Some(Extension(c))) => c
            .manager()
            .and_then(|m| m.world_of(id))
            .ok_or_else(|| (StatusCode::NOT_FOUND, format!("unknown device {id:?}")))?,
        _ => world,
    };
    let is_profile = req.cmd == "profile";
    let out = dispatch_ctl(&req.cmd, &req.args, &world).await.map(Json);
    if is_profile && out.is_ok() {
        // `args.device` may have selected another printer; the owner of `world` refreshes
        if let Some(Extension(c)) = &ctx {
            if let Some(m) = c.manager() {
                let id = req
                    .args
                    .get("device")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&c.id);
                let _ = m.refresh_config(id).await;
            }
        }
    }
    out
}

/// An error of the manager as an HTTP error.
pub(crate) fn api_error(e: anyhow::Error) -> ApiError {
    let code = StatusCode::from_u16(crate::manager::error_status(&e))
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (code, format!("{e:#}"))
}

/// Shared dispatcher of `POST /v1/ctl` and the named pipe.
pub(crate) async fn dispatch_ctl(
    cmd: &str,
    args: &serde_json::Value,
    world: &SharedWorld,
) -> Result<serde_json::Value, ApiError> {
    let to_value = |s: Status| serde_json::to_value(s).unwrap_or(serde_json::Value::Null);
    match cmd {
        "status" => Ok(to_value(world.read().await.status())),
        "health" => Ok(serde_json::json!({"ok": true})),
        "jobs" => Ok(jobs_json(&*world.read().await)),
        "reset" => {
            let mut w = world.write().await;
            w.reset();
            Ok(to_value(w.status()))
        }
        "profile" => {
            let body = ProfileBody {
                id: args.get("id").and_then(|v| v.as_str()).map(str::to_string),
                profile: args.get("profile").cloned(),
            };
            if body.id.as_deref().is_some_and(str::is_empty) {
                return Err((StatusCode::BAD_REQUEST, "args.id required".into()));
            }
            let mut w = world.write().await;
            swap_profile(&mut w, &body)?;
            Ok(to_value(w.status()))
        }
        "faults" => {
            let ctx = world.read().await.print_ctx();
            if !args.is_null() && args.as_object().is_some_and(|o| !o.is_empty()) {
                let body: FaultBody = serde_json::from_value(args.clone())
                    .map_err(|e| (StatusCode::BAD_REQUEST, format!("args: {e}")))?;
                ctx.apply_faults_checked(&body)
                    .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
            }
            Ok(faults_json(&ctx))
        }
        "timing" => {
            if args.as_object().is_some_and(|o| !o.is_empty()) {
                let patch: TimingPatch = serde_json::from_value(args.clone())
                    .map_err(|e| (StatusCode::BAD_REQUEST, format!("args: {e}")))?;
                apply_timing(world, &patch).await
            } else {
                Ok(world.read().await.print_ctx().timing().to_json())
            }
        }
        "save" => {
            // Registry write is a PowerShell/Python CLI responsibility.
            // The process exposes its current config via /v1/status for the script to read.
            Ok(serde_json::json!({
                "ok": true,
                "note": "use: .\\scripts\\fakeprint.ps1 save  (writes current status to registry)"
            }))
        }
        other => Err((
            StatusCode::BAD_REQUEST,
            format!(
                "unknown cmd {other:?}; known: status health jobs reset profile faults timing save"
            ),
        )),
    }
}
