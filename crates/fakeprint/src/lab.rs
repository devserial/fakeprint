//! Stable lab identity: one TCP port, one device UUID, one process.
//!
//! Windows PnP / WSDMON association keys off advertise URL + UUID. Restarting with
//! a different listen/uuid forces remove+re-Add. Prefer `/v1/*` and
//! `scripts/fakeprint.ps1 ensure` over kill/restart.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::process;

use uuid::Uuid;

/// Canonical lab TCP port (never change without a new UUID).
pub const LAB_PORT: u16 = 3921;

/// Canonical WS-Discovery / PnP device UUID for the Windows lab host.
pub const LAB_DEVICE_UUID: &str = "00000000-0000-4000-8000-0000000000f3";

/// Every fake device UUID (fakeprint and fakescan, WSD and IPP) starts with this prefix. It, and
/// not the friendly name, is how a device is recognised as ours (lab safety: nothing outside the
/// prefix is ever created, matched or driven as a fake device unless explicitly allowed).
pub const FAKE_UUID_PREFIX: &str = "00000000-0000-4000-8000-";

/// True if `value` is a UUID in the fake namespace ([`FAKE_UUID_PREFIX`]). Case-insensitive;
/// accepts braces and `urn:uuid:` forms; anything unparseable is `false`.
pub fn is_fake_uuid(value: &str) -> bool {
    Uuid::parse_str(value.trim()).is_ok_and(|u| uuid_is_fake(&u))
}

/// [`is_fake_uuid`] for an already parsed [`Uuid`].
pub fn uuid_is_fake(u: &Uuid) -> bool {
    u.hyphenated().to_string().starts_with(FAKE_UUID_PREFIX)
}

/// A random UUID in the fake namespace (`FAKE_UUID_PREFIX` + 12 random hex digits).
pub fn random_fake_uuid() -> Uuid {
    let mut bytes = *Uuid::new_v4().as_bytes();
    bytes[..10].copy_from_slice(&[0, 0, 0, 0, 0, 0, 0x40, 0, 0x80, 0]);
    Uuid::from_bytes(bytes)
}

/// Canonical IPP Internet Port UUID — distinct from the WSD UUID so that
/// `Add-Printer -IppURL` does not collide with the DAFWSDProvider WSD device.
///
/// Derived from `LAB_DEVICE_UUID` by incrementing the last byte: `f3` → `f4`.
/// Use `--ipp-uuid` to override (e.g., for multi-instance labs).
pub const LAB_IPP_UUID: &str = "00000000-0000-4000-8000-0000000000f4";

/// Derive the IPP UUID from a WSD device UUID by incrementing the last byte
/// (wrapping). Produces a stable, distinct UUID for each device UUID.
pub fn derive_ipp_uuid(device_uuid: Uuid) -> Uuid {
    let mut bytes = *device_uuid.as_bytes();
    bytes[15] = bytes[15].wrapping_add(1);
    Uuid::from_bytes(bytes)
}

/// Stable WSD UUID for a device id that has no stored one: `00000000-0000-4000-8000-<12 hex>`
/// where the 48 bits are FNV-1a (64-bit, folded to 48) of `"fakeprint:" + id`. The salt keeps
/// fakeprint ids from colliding with fakescan's derivation of the same id.
pub fn derive_device_uuid(id: &str) -> Uuid {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in format!("fakeprint:{id}").bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let h48 = h & 0x0000_ffff_ffff_ffff;
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&[0, 0, 0, 0, 0, 0, 0x40, 0]);
    bytes[8] = 0x80;
    bytes[10..16].copy_from_slice(&h48.to_be_bytes()[2..8]);
    // the last byte stays even so that `derive_ipp_uuid` (+1) never lands on another derived uuid
    bytes[15] &= 0xfe;
    Uuid::from_bytes(bytes)
}

/// Default lab instance name (matches the registry default row).
pub const LAB_NAME: &str = "default";

/// Windows named-pipe path for this instance: `\\.\pipe\fakeprint-<name>`.
/// Non-Windows: returns an empty string (pipe feature is Windows-only).
pub fn pipe_path(name: &str) -> String {
    #[cfg(windows)]
    return format!(r"\\.\pipe\fakeprint-{name}");
    #[cfg(not(windows))]
    {
        let _ = name;
        String::new()
    }
}

/// Per-product device registry: `%LOCALAPPDATA%\fakeprint\devices.json`.
/// Falls back to a temp-dir path on non-Windows or when LOCALAPPDATA is unset.
pub fn registry_path() -> PathBuf {
    crate::registry::registry_path()
}

/// The path without the `FAKEPRINT_REGISTRY` override (see [`crate::registry::registry_path`]).
pub(crate) fn default_registry_path() -> PathBuf {
    if let Ok(local) = std::env::var("LOCALAPPDATA") {
        PathBuf::from(local).join("fakeprint").join("devices.json")
    } else {
        std::env::temp_dir().join("fakeprint-devices.json")
    }
}

pub fn lab_device_uuid() -> Uuid {
    Uuid::parse_str(LAB_DEVICE_UUID).expect("LAB_DEVICE_UUID")
}

pub fn default_listen() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::UNSPECIFIED, LAB_PORT))
}

/// True for a loopback address, including an IPv4 loopback written as an IPv4-mapped IPv6
/// address (`::ffff:127.0.0.1`). `IpAddr::is_loopback` is false for that form, which is how a
/// dual-stack listener reports a connection to `127.0.0.1`.
pub fn is_loopback_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => {
            v6.is_loopback() || v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
        }
    }
}

/// First non-loopback IPv4 (for `--advertise` when listen is `0.0.0.0`).
pub fn first_lan_ipv4() -> Option<Ipv4Addr> {
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    match socket.local_addr().ok()?.ip() {
        IpAddr::V4(v4) if !v4.is_loopback() && !v4.is_unspecified() => Some(v4),
        _ => None,
    }
}

fn lock_path(port: u16) -> PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!("fakeprint-{port}.lock"));
    dir
}

pub struct InstanceLock {
    _file: File,
    path: PathBuf,
}

impl Drop for InstanceLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Exclusive lab lock for `port`. Stale locks (dead PID) are replaced.
pub fn acquire_instance_lock(port: u16) -> anyhow::Result<InstanceLock> {
    acquire_instance_lock_for(port, lab_device_uuid())
}

/// Like [`acquire_instance_lock`], recording the default device's UUID in the lock file (one
/// lock per listen port, not per device).
pub fn acquire_instance_lock_for(port: u16, uuid: Uuid) -> anyhow::Result<InstanceLock> {
    let path = lock_path(port);
    match try_create_lock(&path, port, uuid) {
        Ok(file) => Ok(InstanceLock { _file: file, path }),
        Err(_) => {
            if let Some(stale) = read_lock_pid(&path) {
                if !pid_alive(stale) {
                    let _ = fs::remove_file(&path);
                    let file = try_create_lock(&path, port, uuid)?;
                    return Ok(InstanceLock { _file: file, path });
                }
            }
            Err(anyhow::anyhow!(already_running_message(port)))
        }
    }
}

fn try_create_lock(path: &std::path::Path, port: u16, uuid: Uuid) -> std::io::Result<File> {
    let mut f = OpenOptions::new().write(true).create_new(true).open(path)?;
    writeln!(f, "pid={}\nport={}\nuuid={}\n", process::id(), port, uuid)?;
    f.sync_all()?;
    Ok(f)
}

fn read_lock_pid(path: &std::path::Path) -> Option<u32> {
    let mut s = String::new();
    File::open(path).ok()?.read_to_string(&mut s).ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("pid=") {
            return rest.trim().parse().ok();
        }
    }
    None
}

#[cfg(windows)]
fn pid_alive(pid: u32) -> bool {
    use std::os::windows::process::CommandExt;
    let out = process::Command::new("tasklist")
        .args(["/NH", "/FI", &format!("PID eq {pid}")])
        .creation_flags(0x08000000)
        .output();
    match out {
        Ok(o) => String::from_utf8_lossy(&o.stdout).contains(&pid.to_string()),
        Err(_) => true,
    }
}

#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    std::path::Path::new(&format!("/proc/{pid}")).exists()
}

#[cfg(not(any(windows, unix)))]
fn pid_alive(_pid: u32) -> bool {
    true
}

pub fn already_running_message(port: u16) -> String {
    format!(
        "fakeprint already running on :{port} (lab UUID {LAB_DEVICE_UUID}).\n\
         Attach:  Invoke-RestMethod http://127.0.0.1:{port}/v1/status\n\
         Reconfigure via /v1/* — do NOT kill/restart (breaks Windows PnP / WSDMON).\n\
         Start helper:  .\\scripts\\fakeprint.ps1 ensure"
    )
}

/// Bind TCP; on AddrInUse print attach guidance.
pub async fn bind_listener(addr: SocketAddr) -> anyhow::Result<tokio::net::TcpListener> {
    match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => Ok(l),
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            Err(anyhow::anyhow!(already_running_message(addr.port())))
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lab_uuid_parses() {
        assert_eq!(lab_device_uuid().to_string(), LAB_DEVICE_UUID);
    }

    #[test]
    fn derive_ipp_uuid_increments_last_byte() {
        let dev = lab_device_uuid(); // ends in f3
        let ipp = derive_ipp_uuid(dev);
        assert_ne!(ipp, dev);
        assert_eq!(ipp.to_string(), LAB_IPP_UUID); // f3 + 1 = f4
    }

    #[test]
    fn derived_device_uuid_is_stable_namespaced_and_distinct() {
        let a = derive_device_uuid("mono");
        assert_eq!(a, derive_device_uuid("mono"));
        assert_ne!(a, derive_device_uuid("color"));
        assert!(a.to_string().starts_with("00000000-0000-4000-8000-"));
        assert_ne!(derive_ipp_uuid(a), a);
        assert_ne!(a, lab_device_uuid());
    }

    #[test]
    fn fake_uuid_prefix_rule() {
        assert!(is_fake_uuid(LAB_DEVICE_UUID) && is_fake_uuid(LAB_IPP_UUID));
        let u = "00000000-0000-4000-8000-0000000000F3";
        for v in [
            u.to_string(),
            format!("{{{u}}}"),
            format!("urn:uuid:{u}"),
            format!(" {u} "),
        ] {
            assert!(is_fake_uuid(&v), "{v}");
        }
        for v in [
            "",
            "nope",
            "3f2b9c1e-7d44-4a55-9e0b-1c2d3e4f5a6b",
            "00000000-0000-4000-8001-0000000000f3",
            "00000000-0000-4000-8000",
        ] {
            assert!(!is_fake_uuid(v), "{v}");
        }
        assert!(uuid_is_fake(&random_fake_uuid()));
        assert_ne!(random_fake_uuid(), random_fake_uuid());
        assert!(uuid_is_fake(&derive_device_uuid("x")));
        assert!(uuid_is_fake(&derive_ipp_uuid(derive_device_uuid("x"))));
    }

    #[test]
    fn derive_ipp_uuid_wraps() {
        let dev = Uuid::parse_str("00000000-0000-4000-8000-0000000000ff").unwrap();
        let ipp = derive_ipp_uuid(dev);
        assert_eq!(ipp.to_string(), "00000000-0000-4000-8000-000000000000");
    }

    #[test]
    fn default_listen_is_all_interfaces_3921() {
        assert_eq!(default_listen().port(), 3921);
        assert!(default_listen().ip().is_unspecified());
    }

    #[test]
    fn lab_name_is_default() {
        assert_eq!(LAB_NAME, "default");
    }

    #[test]
    fn registry_path_contains_fakeprint() {
        let p = registry_path();
        let s = p.to_string_lossy();
        assert!(
            s.contains("fakeprint"),
            "registry path {s:?} should contain fakeprint"
        );
    }

    #[cfg(windows)]
    #[test]
    fn pipe_path_format() {
        assert_eq!(pipe_path("default"), r"\\.\pipe\fakeprint-default");
        assert_eq!(pipe_path("bpl0"), r"\\.\pipe\fakeprint-bpl0");
    }
}
