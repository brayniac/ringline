//! A buffer whose read was abandoned must stay allocated while the read can
//! still write into it, including after its worker has exited.
//!
//! Its own test binary: it installs a global allocator that counts frees of
//! one distinctive allocation size. mio only: the read is queued on the
//! disk-I/O pool behind an open that is held until after the worker joins.
//! Linux only: it uses `mkfifo`.

#![cfg(all(target_os = "linux", not(has_io_uring)))]
#![allow(clippy::manual_async_fn)]

mod common;

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

/// The capacity of the abandoned read's buffer. Nothing else in the process
/// allocates exactly this many bytes.
const BUF_SIZE: usize = 777_777;

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
static OUTCOME: Mutex<Option<Result<(), String>>> = Mutex::new(None);

struct AbandonsRead;

impl AsyncEventHandler for AbandonsRead {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let outcome = abandon_a_queued_read().await;
            *OUTCOME.lock().unwrap() = Some(outcome);
            ringline::request_shutdown().ok();
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        AbandonsRead
    }
}

async fn abandon_a_queued_read() -> Result<(), String> {
    let dir = DIR.lock().unwrap().clone().unwrap();
    let file = ringline::fs::open(dir.join("data"), OpenFlags::READ, 0)
        .map_err(|e| e.to_string())?
        .await
        .map_err(|e| e.to_string())?;
    // Park the pool's only thread in an open that waits for a writer, which
    // the test opens after the worker has exited.
    drop(ringline::fs::open(dir.join("fifo"), OpenFlags::READ, 0).map_err(|e| e.to_string())?);
    // Queued behind it. Dropping the future parks the buffer until the read
    // completes.
    let read = ringline::fs::read_into(file, 0, BytesMut::with_capacity(BUF_SIZE))
        .map_err(|e| e.to_string())?;
    drop(read);
    Ok(())
}

#[test]
fn an_abandoned_reads_buffer_outlives_its_worker() {
    let dir = std::env::temp_dir().join(format!("ringline-graveyard-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("data"), vec![7u8; 4096]).unwrap();
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
        .disk_io_threads(1)
        .fs(FsConfig {
            max_files: 4,
            max_commands_in_flight: 64,
        })
        .build()
        .expect("valid config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .launch::<AbandonsRead>()
        .expect("launch");
    common::join_workers(&runtime, handles);
    drop(runtime);
    OUTCOME
        .lock()
        .unwrap()
        .take()
        .expect("on_start did not finish")
        .unwrap();
    // The read is still queued behind the parked open.
    let frees = BUF_FREES.load(Ordering::SeqCst);

    // Let the pool run the open and then the read.
    let writer = std::fs::OpenOptions::new().write(true).open(&fifo).unwrap();
    std::thread::sleep(Duration::from_millis(200));
    drop(writer);
    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(
        frees, 0,
        "the buffer was freed while its read was still queued"
    );
}
