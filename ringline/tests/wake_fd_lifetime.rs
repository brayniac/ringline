//! A background thread that wakes a worker after the `Runtime` has dropped
//! must not write into whatever file now holds the wake fd's number.
//!
//! A blocking task still running at shutdown finishes on its pool thread and
//! then wakes the worker that requested it. This test drops the `Runtime` and
//! joins the workers while such a task runs, refills every fd number the
//! shutdown freed with the write end of an unrelated pipe, and checks that no
//! pipe receives the wake, then that every fd `launch()` opened is closed once
//! the task has finished.
//!
//! Its own test binary, because it reads `/proc/self/fd` and claims freed fd
//! numbers, which other tests in the same process would disturb. Linux only.

#![cfg(target_os = "linux")]
#![allow(clippy::manual_async_fn)]

use std::collections::HashSet;
use std::future::Future;
use std::os::fd::RawFd;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, RinglineBuilder};

static BLOCKING_STARTED: AtomicBool = AtomicBool::new(false);
static BLOCKING_FINISHED: AtomicBool = AtomicBool::new(false);

const BLOCKING_TASK: Duration = Duration::from_millis(300);

/// Starts one blocking task on worker 0 that outlives the runtime.
struct SlowBlocking;

impl AsyncEventHandler for SlowBlocking {
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }

    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let task = ringline::spawn_blocking(|| {
                BLOCKING_STARTED.store(true, Ordering::Release);
                std::thread::sleep(BLOCKING_TASK);
                BLOCKING_FINISHED.store(true, Ordering::Release);
            })
            .expect("spawn_blocking");
            let _ = task.await;
        }))
    }

    fn create_for_worker(_id: usize) -> Self {
        SlowBlocking
    }
}

/// The process's open fds, excluding the directory handle used to list them.
fn open_fds() -> HashSet<RawFd> {
    std::fs::read_dir("/proc/self/fd")
        .expect("read /proc/self/fd")
        .filter_map(|e| {
            let e = e.ok()?;
            let target = std::fs::read_link(e.path()).ok()?;
            if target.starts_with("/proc") {
                return None;
            }
            e.file_name().to_str()?.parse().ok()
        })
        .collect()
}

/// Whether `fd` is open, checked without opening anything.
fn is_open(fd: RawFd) -> bool {
    unsafe { libc::fcntl(fd, libc::F_GETFD) >= 0 }
}

#[test]
fn a_late_wake_does_not_write_into_a_reused_fd() {
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(16, 1024)
        .max_connections(16)
        .send_pool(16, 16384)
        .blocking_threads(1)
        .build()
        .expect("valid config");
    let baseline = open_fds();
    let (runtime, handles) = RinglineBuilder::new(config)
        .launch::<SlowBlocking>()
        .expect("launch");

    let deadline = Instant::now() + Duration::from_secs(5);
    while !BLOCKING_STARTED.load(Ordering::Acquire) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        BLOCKING_STARTED.load(Ordering::Acquire),
        "blocking task never started"
    );

    let before = open_fds();
    drop(runtime);
    for h in handles {
        h.join().expect("worker panicked").expect("worker error");
    }
    assert!(
        !BLOCKING_FINISHED.load(Ordering::Acquire),
        "the blocking task finished before shutdown completed; the test proves nothing"
    );
    let freed: Vec<RawFd> = before.iter().copied().filter(|&fd| !is_open(fd)).collect();
    assert!(!freed.is_empty(), "shutdown freed no fd numbers");

    // Put an unrelated pipe's write end on every freed number, so a write to a
    // stale wake fd lands in a pipe this test can read.
    let mut claimed: Vec<(RawFd, RawFd)> = Vec::new();
    for &target in &freed {
        let mut fds = [0 as RawFd; 2];
        assert_eq!(
            unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) },
            0
        );
        // `pipe2` takes the lowest free numbers, which are the freed ones, so
        // move both ends out of the way before placing the write end.
        let read = unsafe { libc::fcntl(fds[0], libc::F_DUPFD_CLOEXEC, 512) };
        let write = unsafe { libc::fcntl(fds[1], libc::F_DUPFD_CLOEXEC, 512) };
        assert!(read >= 512 && write >= 512);
        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
        assert_eq!(
            unsafe { libc::dup3(write, target, libc::O_CLOEXEC) },
            target
        );
        unsafe { libc::close(write) };
        claimed.push((read, target));
    }

    let deadline = Instant::now() + BLOCKING_TASK * 3;
    while !BLOCKING_FINISHED.load(Ordering::Acquire) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        BLOCKING_FINISHED.load(Ordering::Acquire),
        "blocking task never finished"
    );
    // The pool thread wakes the worker right after the task returns.
    std::thread::sleep(Duration::from_millis(100));

    let mut stray = Vec::new();
    for &(read, write) in &claimed {
        let mut buf = [0u8; 64];
        let n = unsafe { libc::read(read, buf.as_mut_ptr().cast(), buf.len()) };
        if n > 0 {
            stray.push((write, n));
        }
        unsafe {
            libc::close(read);
            libc::close(write);
        }
    }
    assert!(
        stray.is_empty(),
        "a wake after Runtime drop wrote into an unrelated pipe: (fd, bytes) = {stray:?}, \
         freed fds {freed:?}"
    );

    // Holding the wake fds open past shutdown must not leak them: once the
    // last thread that could wake a worker exits, every fd `launch()` opened
    // is closed.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut leaked: Vec<RawFd> = open_fds().difference(&baseline).copied().collect();
    while !leaked.is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
        leaked = open_fds().difference(&baseline).copied().collect();
    }
    assert!(
        leaked.is_empty(),
        "fds opened by launch() still open 5s after shutdown: {leaked:?}"
    );
}
