#![allow(clippy::manual_async_fn)]
//! Reproductions for the 2026-11 file I/O audit (not for merge as is).
//! Each test asserts the behaviour the audit says should hold; a failure is
//! the finding reproduced on this backend.

mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use ringline::{AsyncEventHandler, Config, ConfigBuilder, Connection, RinglineBuilder};

static OUTCOME: Mutex<Option<String>> = Mutex::new(None);

fn config() -> Config {
    ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .max_connections(64)
        .send_pool(64, 16384)
        .resolver_threads(0)
        .direct_io(ringline::DirectIoConfig::default().max_files(2))
        .build()
        .expect("valid config")
}

fn temp_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("ringline-audit-{}-{name}", std::process::id()))
}

fn finish(outcome: String) {
    *OUTCOME.lock().unwrap() = Some(outcome);
    ringline::request_shutdown().ok();
}

fn run<H: AsyncEventHandler>() -> String {
    *OUTCOME.lock().unwrap() = None;
    let (shutdown, handles) = RinglineBuilder::new(config())
        .launch::<H>()
        .expect("launch");
    common::join_workers(&shutdown, handles);
    OUTCOME
        .lock()
        .unwrap()
        .take()
        .expect("on_start did not finish")
}

// ── F1: a write issued before close in the same poll ────────────────

struct WriteThenClose;

impl AsyncEventHandler for WriteThenClose {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let path = temp_path("write-then-close");
            let file = ringline::fs::create(&path).unwrap().await.unwrap();
            // Both calls in one poll: the write's SQE is queued, the close
            // runs its synchronous unregister, then the loop flushes.
            let write = ringline::fs::write_from(file, 0, bytes::BytesMut::from(&b"audit"[..]))
                .expect("submit");
            let closed = ringline::fs::close(file);
            let (result, _buf) = write.await;
            let on_disk = std::fs::read(&path).unwrap_or_default();
            let _ = std::fs::remove_file(&path);
            finish(format!(
                "close={closed:?} write={result:?} on_disk={:?}",
                String::from_utf8_lossy(&on_disk)
            ));
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        WriteThenClose
    }
}

#[test]
fn a_write_issued_before_close_in_the_same_poll_lands() {
    let out = run::<WriteThenClose>();
    eprintln!("{out}");
    assert!(
        out.contains("write=Ok(5)") && out.contains("on_disk=\"audit\""),
        "{out}"
    );
}

// ── F4: stat of a symlink ────────────────────────────────────────────

struct StatSymlink;

impl AsyncEventHandler for StatSymlink {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let target = temp_path("symlink-target");
            let link = temp_path("symlink");
            std::fs::write(&target, b"x").unwrap();
            let _ = std::fs::remove_file(&link);
            std::os::unix::fs::symlink(&target, &link).unwrap();
            let out = match ringline::fs::stat(&link).unwrap().await {
                Ok(m) => format!(
                    "is_symlink={} is_file={} size={}",
                    m.is_symlink, m.is_file, m.size
                ),
                Err(e) => format!("stat failed: {e}"),
            };
            let _ = std::fs::remove_file(&link);
            let _ = std::fs::remove_file(&target);
            finish(out);
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        StatSymlink
    }
}

#[test]
fn stat_of_a_symlink_reports_is_symlink() {
    let out = run::<StatSymlink>();
    eprintln!("{out}");
    assert!(out.contains("is_symlink=true"), "{out}");
}

// ── F2: a NUL in the path leaks a direct I/O slot ───────────────────

struct NulPath;

impl AsyncEventHandler for NulPath {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let valid = temp_path("direct");
            std::fs::write(&valid, vec![0u8; 8192]).unwrap();
            let mut log = String::new();
            for i in 0..3 {
                let e = ringline::open_direct_io_file("a\0b")
                    .err()
                    .map(|e| e.to_string());
                log.push_str(&format!("nul open {i}: {e:?}; "));
            }
            let r = ringline::open_direct_io_file(valid.to_str().unwrap());
            log.push_str(&format!(
                "valid open after 3 nul opens (max_files=2): {:?}",
                r.as_ref().map(|_| "ok").map_err(|e| e.to_string())
            ));
            if let Ok(f) = r {
                let _ = ringline::close_direct_io_file(f);
            }
            let _ = std::fs::remove_file(&valid);
            finish(log);
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        NulPath
    }
}

#[test]
fn a_nul_path_does_not_leak_a_direct_io_slot() {
    let out = run::<NulPath>();
    eprintln!("{out}");
    assert!(out.ends_with("Ok(\"ok\")"), "{out}");
}
