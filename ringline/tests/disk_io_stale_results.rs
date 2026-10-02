//! A disk-I/O future dropped before it resolves must not leave a result that
//! a later operation with the same key takes as its own completion (#574).
//!
//! On io_uring the key is a 16-bit sequence plus the slab index, so it comes
//! round again after 65,536 operations. A stale result under a read's key
//! made the read's dropped future free its buffer at once, while the kernel
//! could still write into it.
//!
//! Its own test binary: it installs a global allocator that counts frees of
//! one distinctive allocation size. io_uring only: mio's sequence is 32-bit.

#![cfg(all(target_os = "linux", has_io_uring))]
#![allow(clippy::manual_async_fn)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use ringline::fs::{FsConfig, OpenFlags};
use ringline::{AsyncEventHandler, ConfigBuilder, Connection, RinglineBuilder};

/// The capacity of the pending read's buffer. Nothing else in the process
/// allocates exactly this many bytes.
const BUF_SIZE: usize = 777_787;

/// How often the io_uring disk-I/O key repeats.
const KEY_PERIOD: usize = 1 << 16;

static BUF_FREES: AtomicUsize = AtomicUsize::new(0);

struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if layout.size() == BUF_SIZE {
            BUF_FREES.fetch_add(1, Ordering::SeqCst);
        }
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

static DIR: Mutex<Option<PathBuf>> = Mutex::new(None);
/// The tests share `DIR`, so they run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());
static OUTCOME: Mutex<Option<Result<usize, String>>> = Mutex::new(None);

struct StaleKey;

impl AsyncEventHandler for StaleKey {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let outcome = drop_a_read_on_a_reused_key().await;
            *OUTCOME.lock().unwrap() = Some(outcome);
            ringline::request_shutdown().ok();
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        StaleKey
    }
}

/// Returns how many times the read's buffer was freed right after the read's
/// future was dropped, while the read was still pending in the kernel.
async fn drop_a_read_on_a_reused_key() -> Result<usize, String> {
    let dir = DIR.lock().unwrap().clone().unwrap();
    let path = dir.join("file");
    // Read-write, so the open does not wait for a writer and a read waits
    // for data that never comes. Opened before the key cycle starts.
    let fifo = ringline::fs::open(dir.join("fifo"), OpenFlags::READ_WRITE, 0)
        .map_err(|e| e.to_string())?
        .await
        .map_err(|e| e.to_string())?;

    // A stat dropped before it resolves. Its result arrives with nothing to
    // take it. Yield so it completes, and frees its slab slot, before the
    // next operation: every later operation is then alone in flight and
    // reuses that slot.
    drop(ringline::fs::stat(&path).map_err(|e| e.to_string())?);
    ringline::sleep(Duration::from_millis(10)).await;

    // Bring the key round to the dropped stat's.
    for _ in 1..KEY_PERIOD {
        ringline::fs::stat(&path)
            .map_err(|e| e.to_string())?
            .await
            .map_err(|e| e.to_string())?;
    }

    // This read takes the dropped stat's key. It waits on the FIFO, so
    // dropping it must park the buffer, not free it.
    let before = BUF_FREES.load(Ordering::SeqCst);
    drop(
        ringline::fs::read_into(fifo, 0, BytesMut::with_capacity(BUF_SIZE))
            .map_err(|e| e.to_string())?,
    );
    Ok(BUF_FREES.load(Ordering::SeqCst) - before)
}

#[test]
fn a_stale_result_does_not_free_a_pending_reads_buffer() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("ringline-stale-key-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("file"), b"x").unwrap();
    let fifo = dir.join("fifo");
    let c = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0);
    *DIR.lock().unwrap() = Some(dir.clone());

    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(16, 1024)
        .max_connections(16)
        .send_pool(16, 16384)
        .blocking_threads(0)
        .resolver_threads(0)
        .spawner_threads(0)
        .fs(FsConfig {
            max_files: 4,
            max_commands_in_flight: 64,
        })
        .build()
        .expect("valid config");
    let (_runtime, handles) = RinglineBuilder::new(config)
        .launch::<StaleKey>()
        .expect("launch");
    for h in handles {
        h.join().expect("worker panicked").expect("worker failed");
    }
    let _ = std::fs::remove_dir_all(&dir);
    let frees = OUTCOME
        .lock()
        .unwrap()
        .take()
        .expect("on_start did not finish")
        .unwrap();
    assert_eq!(
        frees, 0,
        "a stale result freed the pending read's buffer while the kernel could still write into it"
    );
}

static REUSE_OUTCOME: Mutex<Option<Result<(), String>>> = Mutex::new(None);

struct ResolvedKeyReused;

impl AsyncEventHandler for ResolvedKeyReused {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let outcome = reuse_a_resolved_key().await;
            *REUSE_OUTCOME.lock().unwrap() = Some(outcome);
            ringline::request_shutdown().ok();
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        ResolvedKeyReused
    }
}

/// Every stat here resolves before it is dropped. The operation that reuses
/// the first one's key must still get its result.
async fn reuse_a_resolved_key() -> Result<(), String> {
    let path = DIR.lock().unwrap().clone().unwrap().join("file");
    for _ in 0..=KEY_PERIOD {
        let stat = ringline::fs::stat(&path).map_err(|e| e.to_string())?;
        match ringline::timeout(Duration::from_secs(5), stat).await {
            Ok(result) => {
                result.map_err(|e| e.to_string())?;
            }
            Err(_) => return Err("a stat on a reused key never resolved".into()),
        }
    }
    Ok(())
}

/// A future that resolved, then was dropped, does not affect the operation
/// that later reuses its key.
#[test]
fn a_resolved_futures_key_can_be_reused() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("ringline-reused-key-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("file"), b"x").unwrap();
    *DIR.lock().unwrap() = Some(dir.clone());

    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(16, 1024)
        .max_connections(16)
        .send_pool(16, 16384)
        .blocking_threads(0)
        .resolver_threads(0)
        .spawner_threads(0)
        .build()
        .expect("valid config");
    let (_runtime, handles) = RinglineBuilder::new(config)
        .launch::<ResolvedKeyReused>()
        .expect("launch");
    for h in handles {
        h.join().expect("worker panicked").expect("worker failed");
    }
    let _ = std::fs::remove_dir_all(&dir);
    REUSE_OUTCOME
        .lock()
        .unwrap()
        .take()
        .expect("on_start did not finish")
        .unwrap();
}
