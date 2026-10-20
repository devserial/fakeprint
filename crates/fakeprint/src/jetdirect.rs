//! Raw JetDirect / AppSocket listener (TCP :9100).
//!
//! Windows `Add-PrinterPort -PrinterHostAddress` always creates TCPMON RAW :9100,
//! even when given an `http://…/ipp/print` URL. Jobs then stick at
//! Printing/Retained unless something accepts the raw stream. This listener
//! captures the stream to the inbox so the TCPMON lab path works.
//!
//! A job is the bytes up to a clean close, and only when they fit in the printer's
//! `max_document`. A stream that runs past that limit, stalls, or dies mid-way is dropped:
//! it is not stored as a shorter document and it does not become a completed job. At most
//! [`MAX_IN_FLIGHT`] jobs are read at once.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::{info, warn};

use crate::state::World;

/// Idle time between socket reads. A sender that stalls is not a finished job.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Simultaneous RAW jobs. Each one holds its document in memory, so this is the memory cap
/// (`max_document` times this), not just a courtesy.
const MAX_IN_FLIGHT: usize = 4;

/// Bind the RAW listener (separate from [`serve_listener`] so the caller can report a port
/// conflict synchronously). Port 0 picks a free port.
pub async fn bind(bind: std::net::SocketAddr) -> std::io::Result<TcpListener> {
    TcpListener::bind(bind).await
}

/// Accept raw print jobs on `bind` (JetDirect). A bind failure is logged and the task ends.
pub async fn serve(world: Arc<RwLock<World>>, bind: std::net::SocketAddr) {
    match TcpListener::bind(bind).await {
        Ok(l) => serve_listener(world, l).await,
        Err(e) => {
            warn!(%bind, error = %e, "JetDirect bind failed (TCPMON RAW jobs will stick)");
        }
    }
}

/// Accept raw print jobs on an already bound listener; runs until aborted.
pub async fn serve_listener(world: Arc<RwLock<World>>, listener: TcpListener) {
    serve_listener_with(world, listener, IDLE_TIMEOUT, MAX_IN_FLIGHT).await;
}

/// [`serve_listener`] with a testable idle timeout and in-flight cap.
async fn serve_listener_with(
    world: Arc<RwLock<World>>,
    listener: TcpListener,
    idle: Duration,
    max_in_flight: usize,
) {
    if let Ok(addr) = listener.local_addr() {
        info!(%addr, "JetDirect RAW listener ready");
    }
    let gate = Arc::new(Semaphore::new(max_in_flight.max(1)));
    let mut jobs = JoinSet::new();

    loop {
        tokio::select! {
            acc = listener.accept() => {
                let (sock, peer) = match acc {
                    Ok(x) => x,
                    Err(e) => {
                        warn!(error = %e, "JetDirect accept failed");
                        continue;
                    }
                };
                let Ok(permit) = gate.clone().try_acquire_owned() else {
                    warn!(%peer, "JetDirect busy; closing");
                    drop(sock);
                    continue;
                };
                let world = Arc::clone(&world);
                jobs.spawn(async move {
                    let _permit = permit;
                    handle_job(world, sock, peer, idle).await;
                });
            }
            finished = jobs.join_next(), if !jobs.is_empty() => {
                if let Some(Err(e)) = finished {
                    if !e.is_cancelled() {
                        warn!(error = %e, "JetDirect job task failed");
                    }
                }
            }
        }
    }
}

/// Why a RAW stream was not stored.
enum RawStop {
    /// The peer went away before sending a byte.
    Empty,
    /// The stream passed `max_document`. Nothing is stored.
    Overflow,
    /// The peer stalled. A partial buffer is not a finished job.
    Timeout,
    /// The read failed. A partial buffer is not a finished job.
    Read,
}

/// Read one job until a clean EOF, or stop without a buffer the caller should store.
async fn read_raw(
    sock: &mut tokio::net::TcpStream,
    max_document: usize,
    idle: Duration,
) -> Result<Vec<u8>, RawStop> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 16 * 1024];
    loop {
        match timeout(idle, sock.read(&mut chunk)).await {
            Ok(Ok(0)) => {
                return if buf.is_empty() {
                    Err(RawStop::Empty)
                } else {
                    Ok(buf)
                };
            }
            Ok(Ok(n)) => {
                if buf.len().saturating_add(n) > max_document {
                    return Err(RawStop::Overflow);
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Ok(Err(_)) => return Err(RawStop::Read),
            Err(_) => return Err(RawStop::Timeout),
        }
    }
}

async fn handle_job(
    world: Arc<RwLock<World>>,
    mut sock: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    idle: Duration,
) {
    let max_document = world.read().await.print_ctx().config.limits.max_document;
    let buf = match read_raw(&mut sock, max_document, idle).await {
        Ok(buf) => buf,
        Err(RawStop::Empty) => {
            info!(%peer, "JetDirect empty connection");
            return;
        }
        Err(RawStop::Overflow) => {
            warn!(%peer, max_document, "JetDirect job exceeds the document limit; not stored");
            return;
        }
        Err(RawStop::Timeout) => {
            warn!(%peer, "JetDirect sender stalled; partial job not stored");
            return;
        }
        Err(RawStop::Read) => {
            warn!(%peer, "JetDirect read failed; partial job not stored");
            return;
        }
    };
    let fmt = sniff_format(&buf);
    let mut w = world.write().await;
    w.note_ipp_action("JetDirect");
    let job_id = w.create_job("jetdirect-job", fmt, "tcpmon");
    if job_id == 0 {
        warn!(%peer, "JetDirect job refused by the printer");
        return;
    }
    match w.capture_document(job_id, &buf, fmt, "tcpmon") {
        Ok(cap) => {
            info!(
                %peer,
                job_id,
                bytes = cap.bytes,
                %fmt,
                "JetDirect job captured to inbox"
            );
            w.end_job(job_id);
        }
        Err(e) => {
            warn!(%peer, job_id, error = %e, "JetDirect capture failed");
            w.cancel_job(job_id);
        }
    }
}

fn sniff_format(buf: &[u8]) -> &'static str {
    if buf.starts_with(b"%PDF") {
        "application/pdf"
    } else if buf.starts_with(b"\x1b%-12345X") || buf.windows(5).any(|w| w == b"%!PS-") {
        "application/postscript"
    } else if buf.starts_with(b"\x1b") {
        "application/vnd.hp-PCL"
    } else {
        "application/octet-stream"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::PrinterProfile;
    use crate::state::{World, WorldOptions};
    use crate::wsd::ctx::PrintTiming;
    use std::path::PathBuf;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    struct Tmp(PathBuf);
    impl Tmp {
        fn new() -> Self {
            let p = std::env::temp_dir().join(format!("fakeprint-jd-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&p).unwrap();
            Self(p)
        }
    }
    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn world(tmp: &Tmp, max_document: usize) -> Arc<RwLock<World>> {
        let mut opts = WorldOptions::default();
        opts.limits.max_document = max_document;
        opts.timing = PrintTiming::instant();
        let w = World::with_options(
            tmp.0.join("profiles"),
            tmp.0.join("inbox"),
            PrinterProfile::generic(),
            Arc::new(crate::discovery::DiscoveryStats::default()),
            opts,
        )
        .unwrap();
        Arc::new(RwLock::new(w))
    }

    fn job_count(w: &World) -> usize {
        let ctx = w.print_ctx();
        ctx.jobs.history().len() + ctx.jobs.active().len()
    }

    #[tokio::test]
    async fn a_finished_job_is_stored_and_a_too_long_or_stalled_stream_is_dropped() {
        let tmp = Tmp::new();
        let world = world(&tmp, 8);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let serve = {
            let world = Arc::clone(&world);
            tokio::spawn(async move {
                serve_listener_with(world, listener, Duration::from_millis(80), 2).await;
            })
        };

        let mut ok = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        ok.write_all(b"%PDF-ok!").await.unwrap();
        ok.shutdown().await.unwrap();
        drop(ok);
        for _ in 0..50 {
            if world.read().await.captured().len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let mut big = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        big.write_all(b"%PDF-too!").await.unwrap();
        big.shutdown().await.unwrap();
        drop(big);

        let mut stall = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        stall.write_all(b"%PDF").await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(stall);
        tokio::time::sleep(Duration::from_millis(50)).await;

        {
            let w = world.read().await;
            let caps = w.captured();
            assert_eq!(caps.len(), 1, "only the clean 8-byte job is kept");
            assert_eq!(std::fs::read(&caps[0].path).unwrap(), b"%PDF-ok!");
            assert_eq!(
                job_count(&w),
                1,
                "oversize and stalled streams are not jobs"
            );
        }
        serve.abort();
    }

    #[tokio::test]
    async fn a_fifth_style_extra_connection_is_closed_while_the_cap_is_full() {
        let tmp = Tmp::new();
        let world = world(&tmp, 64);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let serve = {
            let world = Arc::clone(&world);
            tokio::spawn(async move {
                serve_listener_with(world, listener, Duration::from_secs(5), 1).await;
            })
        };

        let mut hold = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        hold.write_all(b"x").await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;

        let mut extra = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let wrote = extra.write_all(b"%PDF-ok!").await;
        let _ = extra.shutdown().await;
        drop(extra);
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            world.read().await.captured().is_empty(),
            "the busy listener must not store the extra job (write result: {wrote:?})"
        );
        drop(hold);
        serve.abort();
    }
}
