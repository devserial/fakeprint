//! Operator web page (`GET /ui`) and its helpers (docs/multi-device.md 5.4).
//!
//! The page is one self-contained file ([`INDEX_HTML`], `ui/index.html`: inline CSS and vanilla
//! JS, no external requests, no HTML string building) that polls `GET /v1/devices`,
//! `GET /v1/queue`, `GET /d/{id}/v1/jobs` and drives the documented control plane. It is served
//! to loopback peers only.
//!
//! Wiring (done by the manager router, WP-M):
//!
//! * `GET /ui`, `GET /ui/` -> [`ui_page`] (handler; needs `ConnectInfo<SocketAddr>` as a request
//!   extension, i.e. `into_make_service_with_connect_info`; without it the answer is 403),
//! * `GET /v1/profiles` -> [`list_profiles`] (handler with `State<Arc<DeviceManager>>`; reads
//!   `manager.config().profile_dir`, falling back to the built-in generic profile),
//! * ctl `open_settings` -> [`check_open_settings`] before calling
//!   [`DeviceManager::open_settings`](crate::manager::DeviceManager::open_settings).

use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;

use axum::extract::{ConnectInfo, Extension, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

use crate::manager::DeviceManager;
use crate::profile::PrinterProfile;
use crate::registry::{config_summary, ConfigSummary};

/// The page, embedded in the binary. Relative URLs only; no external requests.
pub const INDEX_HTML: &str = include_str!("ui/index.html");

/// True for 127.0.0.0/8, `::1`, and IPv4-mapped IPv6 forms of 127.0.0.0/8.
pub fn is_loopback_peer(ip: IpAddr) -> bool {
    crate::lab::is_loopback_ip(ip)
}

/// Validate an `open_settings` request: the peer must be known and loopback, and only the
/// `printers` page may be opened.
pub fn check_open_settings(peer: Option<IpAddr>, page: &str) -> Result<(), String> {
    match peer {
        Some(ip) if is_loopback_peer(ip) => {}
        _ => return Err("open_settings: loopback peers only".to_string()),
    }
    if page != "printers" {
        return Err(format!(
            "open_settings: page {page:?} not allowed (only \"printers\")"
        ));
    }
    Ok(())
}

fn forbidden() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({ "error": "the device manager UI is loopback-only" })),
    )
        .into_response()
}

/// `GET /ui`, `GET /ui/`: the page for loopback peers, 403 for everyone else (and for a request
/// whose peer address is unknown).
pub async fn ui_page(peer: Option<Extension<ConnectInfo<SocketAddr>>>) -> Response {
    match peer {
        Some(Extension(ConnectInfo(a))) if is_loopback_peer(a.ip()) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                (header::CACHE_CONTROL, "no-store"),
                (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
                (
                    header::CONTENT_SECURITY_POLICY,
                    "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src 'self'; form-action 'none'; base-uri 'none'; frame-ancestors 'none'",
                ),
            ],
            INDEX_HTML,
        )
            .into_response(),
        _ => forbidden(),
    }
}

/// One row of `GET /v1/profiles` (docs/multi-device.md 2.1).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProfileSummary {
    pub id: String,
    pub label: String,
    /// Same as `label` (kept for clients that read `description`).
    pub description: String,
    pub formats: Vec<String>,
    pub color_modes: Vec<String>,
    pub sides: Vec<String>,
    pub media_sizes: Vec<String>,
    pub input_bins: Vec<String>,
    /// `{v11, v12, v20}`.
    pub versions: std::collections::BTreeMap<String, bool>,
}

impl ProfileSummary {
    /// Summarize one profile through the same `config_summary` the registry fingerprints use.
    pub fn of(p: &PrinterProfile) -> ProfileSummary {
        let ConfigSummary {
            formats,
            color_modes,
            sides,
            media_sizes,
            input_bins,
            versions,
        } = config_summary(p);
        ProfileSummary {
            id: p.id.clone(),
            label: p.label.clone(),
            description: p.label.clone(),
            formats,
            color_modes,
            sides,
            media_sizes,
            input_bins,
            versions,
        }
    }
}

/// Every loadable profile in `dir`, sorted by id. Unreadable or invalid files are skipped (and
/// logged); a missing directory is an empty list.
pub fn profile_summaries(dir: &Path) -> Vec<ProfileSummary> {
    let ids = match PrinterProfile::list_ids(dir) {
        Ok(ids) => ids,
        Err(e) => {
            tracing::warn!(dir = %dir.display(), error = %e, "listing profiles failed");
            return Vec::new();
        }
    };
    let mut out: Vec<ProfileSummary> = ids
        .into_iter()
        .filter_map(|id| match PrinterProfile::load(dir, &id) {
            Ok(p) => Some(ProfileSummary::of(&p)),
            Err(e) => {
                tracing::warn!(profile = %id, error = %format!("{e:#}"), "skipping unusable profile");
                None
            }
        })
        .collect();
    out.sort_by(|a, b| a.id.cmp(&b.id));
    out
}

/// The profiles a manager offers: its profile directory, or (no directory configured) the built-in
/// generic profile so the page is never empty.
pub fn manager_profiles(profile_dir: Option<&Path>) -> Vec<ProfileSummary> {
    match profile_dir {
        Some(dir) => profile_summaries(dir),
        None => vec![ProfileSummary::of(&PrinterProfile::generic())],
    }
}

/// `GET /v1/profiles`.
pub async fn list_profiles(State(manager): State<Arc<DeviceManager>>) -> Json<Vec<ProfileSummary>> {
    let dir = manager.config().profile_dir.clone();
    let list = tokio::task::spawn_blocking(move || manager_profiles(dir.as_deref()))
        .await
        .unwrap_or_default();
    Json(list)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt;

    fn profiles_dir() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../profiles")
    }

    async fn get_ui(peer: Option<SocketAddr>) -> (StatusCode, axum::http::HeaderMap, String) {
        let app = Router::new()
            .route("/ui", get(ui_page))
            .route("/ui/", get(ui_page));
        let mut req = Request::builder().uri("/ui").body(Body::empty()).unwrap();
        if let Some(a) = peer {
            req.extensions_mut().insert(ConnectInfo(a));
        }
        let resp = app.oneshot(req).await.unwrap();
        let (parts, body) = resp.into_parts();
        let bytes = axum::body::to_bytes(body, 1 << 22).await.unwrap();
        (
            parts.status,
            parts.headers,
            String::from_utf8_lossy(&bytes).into_owned(),
        )
    }

    #[tokio::test]
    async fn page_is_served_to_loopback_only() {
        for ok in [
            "127.0.0.1:5000",
            "127.9.9.9:1",
            "[::1]:5000",
            "[::ffff:127.0.0.1]:9",
        ] {
            let (st, h, body) = get_ui(Some(ok.parse().unwrap())).await;
            assert_eq!(st, StatusCode::OK, "{ok}");
            assert_eq!(h[header::CONTENT_TYPE], "text/html; charset=utf-8");
            assert_eq!(h[header::CACHE_CONTROL], "no-store");
            assert!(h[header::CONTENT_SECURITY_POLICY]
                .to_str()
                .unwrap()
                .contains("default-src 'none'"));
            assert_eq!(body, INDEX_HTML);
        }
        for bad in [
            "192.168.1.5:80",
            "10.0.0.1:1",
            "[2001:db8::1]:80",
            "[::ffff:10.0.0.1]:80",
        ] {
            let (st, _, body) = get_ui(Some(bad.parse().unwrap())).await;
            assert_eq!(st, StatusCode::FORBIDDEN, "{bad}");
            assert!(!body.contains("<script"), "no page for {bad}");
        }
        // Unknown peer (no ConnectInfo) is not trusted.
        let (st, _, _) = get_ui(None).await;
        assert_eq!(st, StatusCode::FORBIDDEN);
    }

    #[test]
    fn loopback_and_open_settings_rules() {
        for ok in ["127.0.0.1", "127.255.255.254", "::1", "::ffff:127.0.0.1"] {
            assert!(is_loopback_peer(ok.parse().unwrap()), "{ok}");
        }
        for bad in [
            "0.0.0.0",
            "192.168.0.2",
            "::",
            "2001:db8::1",
            "::ffff:8.8.8.8",
        ] {
            assert!(!is_loopback_peer(bad.parse().unwrap()), "{bad}");
        }
        let lo: Option<IpAddr> = Some("127.0.0.1".parse().unwrap());
        assert!(check_open_settings(lo, "printers").is_ok());
        for page in ["network", "", "Printers", "printers & calc", "printers\n"] {
            assert!(check_open_settings(lo, page).is_err(), "{page:?}");
        }
        assert!(check_open_settings(Some("192.168.1.5".parse().unwrap()), "printers").is_err());
        assert!(check_open_settings(None, "printers").is_err());
    }

    #[test]
    fn profile_summaries_have_the_documented_shape() {
        let list = profile_summaries(&profiles_dir());
        assert!(!list.is_empty());
        assert!(list.windows(2).all(|w| w[0].id < w[1].id), "sorted by id");
        let v = serde_json::to_value(&list).unwrap();
        for row in v.as_array().unwrap() {
            for k in [
                "id",
                "label",
                "description",
                "formats",
                "color_modes",
                "sides",
                "media_sizes",
                "input_bins",
                "versions",
            ] {
                assert!(row.get(k).is_some(), "missing {k} in {row}");
            }
            for k in ["v11", "v12", "v20"] {
                assert!(row["versions"][k].is_boolean(), "{k}");
            }
            assert!(row["formats"].is_array());
        }
        // The profile the page offers for "generic-wsd" matches the registry summary.
        let g = list
            .iter()
            .find(|p| p.id == "generic-wsd")
            .expect("generic-wsd");
        let p = PrinterProfile::load(&profiles_dir(), "generic-wsd").unwrap();
        assert_eq!(g.formats, config_summary(&p).formats);
    }

    #[test]
    fn missing_dir_is_empty_and_no_dir_offers_the_generic_profile() {
        assert!(profile_summaries(Path::new("D:/definitely/not/here")).is_empty());
        let list = manager_profiles(None);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, "generic-wsd");
    }

    #[test]
    fn unusable_profile_files_are_skipped() {
        let dir =
            std::env::temp_dir().join(format!("fakeprint-ui-profiles-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("broken.json"), "{ not json").unwrap();
        std::fs::copy(
            profiles_dir().join("generic-wsd.json"),
            dir.join("generic-wsd.json"),
        )
        .unwrap();
        let list = profile_summaries(&dir);
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, "generic-wsd");
    }
}
