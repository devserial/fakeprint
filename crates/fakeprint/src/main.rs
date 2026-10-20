//! fakeprint binary: control plane + WSD Print Service + discovery. Argument parsing around
//! [`fakeprint::server::serve`].

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use fakeprint::lab::{LAB_DEVICE_UUID, LAB_IPP_UUID, LAB_NAME};
use fakeprint::server::{serve, DaemonOptions, DeviceConfig, ServerConfig};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(
    name = "fakeprint",
    about = "Fake WSD / IPP printer for Windows testing"
)]
struct Args {
    #[command(subcommand)]
    command: Option<Cmd>,

    /// Device manager mode: the printers are the rows of the registry (devices.json) for this
    /// listen address; loopback and no WS-Discovery multicast unless asked for. Starts the tray
    /// on Windows builds that have it.
    #[arg(long)]
    manage: bool,

    /// Allow WS-Discovery multicast (Hello / Probe answers). Default off with --manage.
    #[arg(long)]
    discovery: bool,

    /// Never multicast (the bare command multicasts by default).
    #[arg(long)]
    no_discovery: bool,

    /// Extra printer without a registry: ID=PROFILE[@UUID]. Repeatable; the first one is the
    /// default device. Not combined with --manage.
    #[arg(long = "device", value_parser = parse_device)]
    devices: Vec<DeviceConfig>,

    /// Do not start the tray icon (--manage).
    #[arg(long)]
    no_tray: bool,

    /// Registry file (default: %LOCALAPPDATA%\fakeprint\devices.json or FAKEPRINT_REGISTRY).
    #[arg(long)]
    registry: Option<PathBuf>,

    /// Listen address for the control API and Print Service.
    /// Default 0.0.0.0:3921 (fakeprint lab port), 127.0.0.1:3921 with --manage.
    /// Use a LAN IP for multi-host.
    #[arg(long)]
    listen: Option<SocketAddr>,

    /// Advertise this host in WS-Discovery XAddrs (defaults to first LAN IPv4
    /// when listen is 0.0.0.0, otherwise the listen IP).
    #[arg(long)]
    advertise: Option<IpAddr>,

    /// Disable WS-Discovery / Print Service (control API only).
    #[arg(long)]
    no_wsd: bool,

    /// Directory of capability profiles (JSON).
    #[arg(long, default_value = "profiles")]
    profiles: PathBuf,

    /// Profile id to load at start.
    #[arg(long, default_value = "generic-wsd")]
    profile: String,

    /// Directory where captured print jobs are written.
    #[arg(long, default_value = "inbox")]
    inbox: PathBuf,

    /// Stable device UUID for discovery.
    /// Defaults to the canonical lab UUID (…000f3). Random with --device-uuid $(uuidgen).
    #[arg(long, default_value = LAB_DEVICE_UUID)]
    device_uuid: Uuid,

    /// UUID returned in IPP Get-Printer-Attributes (`printer-uuid`).
    ///
    /// Must differ from `--device-uuid` (the WSD UUID). When Windows
    /// `Add-Printer -IppURL` gets a uuid that already appears in
    /// `SWD\DAFWSDProvider` (the WSD device tree) it returns "printer already
    /// exists" and refuses to create an Internet Port — even with a different
    /// printer name. A distinct IPP UUID sidesteps this check.
    ///
    /// Default: last byte of `--device-uuid` incremented by 1
    /// (…000f3 → …000f4 = `LAB_IPP_UUID`).
    #[arg(long, default_value = LAB_IPP_UUID)]
    ipp_uuid: Uuid,

    /// Lab-safety override: accept device UUIDs outside the fake namespace
    /// (`00000000-0000-4000-8000-*`). Without it such a UUID is refused at start.
    #[arg(long)]
    allow_foreign_uuid: bool,

    /// Bind UDP :3702 to answer WS-Discovery Probes.
    #[arg(long, default_value_t = !cfg!(windows))]
    probe_listen: bool,

    /// Force Hello-only mode (never bind :3702).
    #[arg(long)]
    no_probe_listen: bool,

    /// Enable IPP on `/ipp/print` (Get-Printer-Attributes). Default on.
    #[arg(long, default_value_t = true)]
    ipp: bool,

    /// Disable IPP routes (WSD-only).
    #[arg(long)]
    no_ipp: bool,

    /// Listen for Windows TCPMON RAW jobs (JetDirect AppSocket). Default :9100.
    /// Needed because Add-PrinterPort -PrinterHostAddress always creates RAW :9100.
    #[arg(long, default_value_t = 9100)]
    jetdirect_port: u16,

    /// Disable the JetDirect RAW listener.
    #[arg(long)]
    no_jetdirect: bool,

    /// Lab instance name (registry row key; sets named-pipe suffix).
    /// Override via FAKEPRINT_NAME env var.
    #[arg(long, env = "FAKEPRINT_NAME", default_value = LAB_NAME)]
    lab_name: String,

    /// Skip the single-instance lock (advanced; may confuse WSDMON).
    #[arg(long)]
    no_lock: bool,

    /// Validate every outgoing SOAP body against the WS-Print schema; violations are logged
    /// loudly and counted on /v1/status. Same as FAKEPRINT_VALIDATE_REPLIES=1.
    #[arg(long)]
    validate_replies: bool,

    /// Headless mode for client test suites and CI: one loopback listener (ephemeral port
    /// unless --listen is given), no WS-Discovery, no WSD endpoints, no JetDirect, no
    /// single-instance lock, no named pipe, reply validation on, job timing `instant` (unless
    /// --timing says otherwise). Prints one `FAKEPRINT_READY {json}` line on stdout when the
    /// server accepts connections. See docs/ipp.md.
    #[arg(long)]
    headless: bool,

    /// Job timing preset: instant, realistic or default (headless: instant).
    #[arg(long)]
    timing: Option<String>,

    /// Raw request log: records kept per device (default 256, 0 switches the log off). Memory is
    /// bounded by --request-log-bytes. See docs/request-log.md. Env: FAKEPRINT_REQUEST_LOG.
    #[arg(long, env = "FAKEPRINT_REQUEST_LOG")]
    request_log: Option<usize>,

    /// Raw request log: bytes stored per request or reply body (default 262144).
    #[arg(long)]
    request_log_body: Option<usize>,

    /// Raw request log: bytes of payload the whole ring of one device may hold (default 32 MiB).
    #[arg(long)]
    request_log_bytes: Option<usize>,

    /// Raw request log: replace peer addresses and credential headers by a placeholder.
    #[arg(long)]
    request_log_redact: bool,

    /// Shut down cleanly when stdin reaches end of file (a test harness that owns the process
    /// closes the pipe instead of signalling).
    #[arg(long)]
    exit_on_stdin_close: bool,

    /// Verbose logs.
    #[arg(short = 'v', long)]
    verbose: bool,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Scheduled task that starts `fakeprint --manage` at logon (uses --profiles, --listen,
    /// --advertise, --discovery).
    Daemon {
        #[command(subcommand)]
        action: DaemonCmd,
    },
}

#[derive(Debug, Clone, Copy, Subcommand)]
enum DaemonCmd {
    /// Create (or replace) the per-user ONLOGON task `fakeprint-daemon`.
    Install,
    /// Delete the task.
    Uninstall,
    /// Show the task.
    Status,
}

/// `ID=PROFILE[@UUID]`.
fn parse_device(spec: &str) -> Result<DeviceConfig, String> {
    let (id, rest) = spec.split_once('=').ok_or("expected ID=PROFILE[@UUID]")?;
    let (profile, uuid) = match rest.split_once('@') {
        Some((p, u)) => (p, Some(u)),
        None => (rest, None),
    };
    if !fakeprint::server::valid_device_id(id) {
        return Err(format!("invalid device id {id:?}"));
    }
    if profile.is_empty() {
        return Err("empty profile".into());
    }
    let mut d = DeviceConfig::derived(id, profile);
    if let Some(u) = uuid {
        d.device_uuid = u.parse().map_err(|e| format!("bad UUID {u:?}: {e}"))?;
    }
    Ok(d)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        if args.verbose {
            "fakeprint=debug,tower_http=debug".into()
        } else {
            "fakeprint=info,tower_http=info".into()
        }
    });
    if args.headless {
        // stdout carries the one machine-readable ready line; logs go to stderr
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }

    if let Some(Cmd::Daemon { action }) = args.command {
        let spec = fakeprint::daemon::TaskSpec {
            exe: std::env::current_exe()?,
            profiles: std::fs::canonicalize(&args.profiles).unwrap_or(args.profiles.clone()),
            listen: args.listen,
            advertise: args.advertise,
            discovery: args.discovery,
        };
        let act = match action {
            DaemonCmd::Install => fakeprint::daemon::Action::Install,
            DaemonCmd::Uninstall => fakeprint::daemon::Action::Uninstall,
            DaemonCmd::Status => fakeprint::daemon::Action::Status,
        };
        match fakeprint::daemon::run(act, &spec) {
            Ok(out) => {
                println!("{}", out.trim());
                return Ok(());
            }
            Err(e) => {
                eprintln!("{e:#}");
                std::process::exit(1);
            }
        }
    }
    if args.manage && !args.devices.is_empty() {
        eprintln!("--device is for ad-hoc runs; with --manage the printers come from the registry");
        std::process::exit(2);
    }

    // Q1: --manage is loopback with discovery off; the bare command keeps 0.0.0.0 + discovery.
    let listen = args.listen.unwrap_or_else(|| {
        if args.headless {
            SocketAddr::from(([127, 0, 0, 1], 0))
        } else if args.manage {
            SocketAddr::from(([127, 0, 0, 1], fakeprint::lab::LAB_PORT))
        } else {
            fakeprint::lab::default_listen()
        }
    });
    let discovery = if args.headless {
        false
    } else if args.manage {
        args.discovery && !args.no_discovery
    } else {
        !args.no_discovery
    };
    let mut cfg = ServerConfig::daemon(args.profiles, args.inbox);
    if args.manage {
        cfg.registry = Some(
            args.registry
                .clone()
                .unwrap_or_else(fakeprint::registry::registry_path),
        );
        cfg.manage = true;
    } else {
        cfg.devices = args.devices;
        for d in &mut cfg.devices {
            d.allow_foreign_uuid = args.allow_foreign_uuid;
        }
    }
    cfg.listen = listen;
    cfg.advertise = args.advertise;
    cfg.profile_name = args.profile;
    cfg.allow_foreign_uuid = args.allow_foreign_uuid;
    cfg.device_uuid = args.device_uuid;
    cfg.ipp_uuid = Some(args.ipp_uuid);
    cfg.lab_name = args.lab_name;
    cfg.wsd = !args.no_wsd && !args.headless;
    cfg.ipp = args.ipp && !args.no_ipp;
    if args.validate_replies {
        cfg.validate_replies = Some(true);
    }
    cfg.daemon = DaemonOptions {
        discovery,
        probe_listen: args.probe_listen && !args.no_probe_listen && !args.headless,
        jetdirect_port: (!args.no_jetdirect && args.jetdirect_port != 0 && !args.headless)
            .then_some(args.jetdirect_port),
        named_pipe: !args.headless,
        instance_lock: !args.no_lock && !args.headless,
    };
    if let Some(n) = args.request_log {
        cfg.request_log.capacity = n;
        cfg.request_log.enabled = n > 0;
    }
    if let Some(n) = args.request_log_body {
        cfg.request_log.body_cap = n;
    }
    if let Some(n) = args.request_log_bytes {
        cfg.request_log.max_bytes = n;
    }
    cfg.request_log.redact = args.request_log_redact;
    cfg.request_log = cfg.request_log.clone().normalized();
    if args.headless {
        cfg.validate_replies = Some(true);
        cfg.timing = fakeprint::wsd::ctx::PrintTiming::instant();
    }
    if let Some(preset) = &args.timing {
        let mut t = cfg.timing.clone();
        let patch = fakeprint::wsd::ctx::TimingPatch {
            preset: Some(preset.clone()),
            ..Default::default()
        };
        if let Err(e) = t.apply(&patch) {
            eprintln!("--timing: {e}");
            std::process::exit(2);
        }
        cfg.timing = t;
    }

    let handle = match serve(cfg).await {
        Ok(h) => h,
        Err(e) => {
            eprintln!("{e:#}");
            std::process::exit(1);
        }
    };
    #[cfg(windows)]
    let manager = handle.manager();
    #[cfg(windows)]
    manager.set_settings_launcher(std::sync::Arc::new(
        fakeprint::manager::WindowsSettingsLauncher,
    ));
    #[cfg(all(windows, feature = "tray"))]
    let _tray = if args.manage && !args.no_tray && !args.no_wsd {
        let quit_manager = std::sync::Arc::clone(&manager);
        fakeprint::tray::spawn(fakeprint::tray::TrayConfig {
            port: handle.port(),
            quit: std::sync::Arc::new(move || quit_manager.request_exit()),
        })
    } else {
        None
    };
    if args.headless {
        print_ready(&handle).await;
    }
    let (stdin_tx, stdin_rx) = tokio::sync::oneshot::channel::<()>();
    if args.exit_on_stdin_close {
        std::thread::spawn(move || {
            use std::io::Read;
            let mut buf = [0u8; 256];
            let mut stdin = std::io::stdin();
            while matches!(stdin.read(&mut buf), Ok(n) if n > 0) {}
            let _ = stdin_tx.send(());
        });
    } else {
        std::mem::forget(stdin_tx);
    }
    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    #[cfg(unix)]
    let term = async move { sigterm.recv().await };
    #[cfg(not(unix))]
    let term = std::future::pending::<Option<()>>();
    let mut exit = handle.exit_signal();
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term => {}
        _ = stdin_rx, if args.exit_on_stdin_close => {}
        _ = async { while exit.changed().await.is_ok() { if *exit.borrow() { break; } } } => {}
    }
    handle.shutdown().await;
    Ok(())
}

/// The machine-readable "ready" line of `--headless`: one line, `FAKEPRINT_READY ` and a JSON
/// object with the IPP URL and the control-plane URL of the default device, the port, and every
/// device's pair.
async fn print_ready(handle: &fakeprint::server::ServerHandle) {
    let devices = handle.devices().await;
    let rows: Vec<serde_json::Value> = devices
        .iter()
        .map(|d| {
            let ipp = d.ipp_endpoint.clone();
            let control = ipp.as_deref().map(|u| {
                let http = u.replacen("ipp://", "http://", 1);
                http.strip_suffix("/ipp/print").unwrap_or(&http).to_string()
            });
            serde_json::json!({ "id": d.id, "ipp_url": ipp, "control_url": control })
        })
        .collect();
    let first = rows.first().cloned().unwrap_or(serde_json::Value::Null);
    let line = serde_json::json!({
        "ipp_url": first["ipp_url"],
        "control_url": first["control_url"],
        "port": handle.port(),
        "inbox": handle.inbox_dir().display().to_string(),
        "devices": rows,
    });
    println!("FAKEPRINT_READY {line}");
    use std::io::Write;
    let _ = std::io::stdout().flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_specs_parse() {
        let d = parse_device("mono=mono-laser").unwrap();
        assert_eq!((d.id.as_str(), d.profile.as_str()), ("mono", "mono-laser"));
        assert_eq!(d.device_uuid, fakeprint::lab::derive_device_uuid("mono"));
        let d = parse_device("c=color@00000000-0000-4000-8000-0000000000a1").unwrap();
        assert_eq!(
            d.device_uuid.to_string(),
            "00000000-0000-4000-8000-0000000000a1"
        );
        for bad in ["mono", "=p", "bad id=p", "a=", "a=p@nope"] {
            assert!(parse_device(bad).is_err(), "{bad}");
        }
    }
}
