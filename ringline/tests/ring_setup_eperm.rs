//! Regression for #355: a refused `io_uring_setup(2)` must surface as
//! `Error::RingSetup` with an actionable message, not as a bare
//! `Error::Io(PermissionDenied)`.
//!
//! The refusal is produced with a seccomp filter that answers
//! `io_uring_setup` with `EPERM` — the same shape a container runtime's
//! default profile or a systemd `SystemCallFilter=` produces. The filter is
//! installed on a dedicated thread and is inherited only by the threads
//! that thread spawns (the workers), so it cannot leak into other tests in
//! this binary.

#![cfg(has_io_uring)]
#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::io;

use ringline::{AsyncEventHandler, ConfigBuilder, ConnCtx, Connection, Error, RinglineBuilder};

struct NoopHandler;

impl AsyncEventHandler for NoopHandler {
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }

    fn create_for_worker(_id: usize) -> Self {
        NoopHandler
    }
}

// Classic BPF as consumed by `seccomp(2)`. Defined locally rather than via
// `libc` so the test does not depend on which of these the pinned libc
// version happens to export.
#[repr(C)]
struct SockFilter {
    code: u16,
    jt: u8,
    jf: u8,
    k: u32,
}

#[repr(C)]
struct SockFprog {
    len: u16,
    filter: *const SockFilter,
}

const PR_SET_SECCOMP: libc::c_int = 22;
/// `BPF_LD | BPF_W | BPF_ABS`
const BPF_LD_W_ABS: u16 = 0x20;
/// `BPF_JMP | BPF_JEQ | BPF_K`
const BPF_JMP_JEQ_K: u16 = 0x15;
/// `BPF_RET | BPF_K`
const BPF_RET_K: u16 = 0x06;

/// Make `io_uring_setup(2)` fail with `EPERM` on the calling thread and on
/// every thread it subsequently spawns. Returns `false` if the kernel does
/// not support seccomp filters, in which case the test cannot run.
fn deny_io_uring_setup() -> bool {
    let filter = [
        // A = seccomp_data.nr
        SockFilter {
            code: BPF_LD_W_ABS,
            jt: 0,
            jf: 0,
            k: 0,
        },
        // if A == io_uring_setup: fall through to ERRNO, else skip to ALLOW
        SockFilter {
            code: BPF_JMP_JEQ_K,
            jt: 0,
            jf: 1,
            k: libc::SYS_io_uring_setup as u32,
        },
        SockFilter {
            code: BPF_RET_K,
            jt: 0,
            jf: 0,
            k: libc::SECCOMP_RET_ERRNO | libc::EPERM as u32,
        },
        SockFilter {
            code: BPF_RET_K,
            jt: 0,
            jf: 0,
            k: libc::SECCOMP_RET_ALLOW,
        },
    ];
    let prog = SockFprog {
        len: filter.len() as u16,
        filter: filter.as_ptr(),
    };
    // SAFETY: plain prctl calls; `prog` and `filter` outlive the calls, and
    // the kernel copies the program on installation.
    unsafe {
        if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
            eprintln!(
                "skipping: PR_SET_NO_NEW_PRIVS failed: {}",
                io::Error::last_os_error()
            );
            return false;
        }
        if libc::prctl(
            PR_SET_SECCOMP,
            libc::SECCOMP_MODE_FILTER,
            &prog as *const SockFprog,
        ) != 0
        {
            eprintln!(
                "skipping: PR_SET_SECCOMP failed: {}",
                io::Error::last_os_error()
            );
            return false;
        }
    }
    true
}

#[test]
fn eperm_from_io_uring_setup_is_an_actionable_ring_setup_error() {
    let outcome = std::thread::spawn(|| {
        if !deny_io_uring_setup() {
            return None;
        }
        let config = ConfigBuilder::new()
            .workers(1)
            .pin_to_core(false)
            .sq_entries(64)
            .recv_buffer(16, 1024)
            .max_connections(16)
            .send_pool(16, 16384)
            .build()
            .expect("valid config");
        Some(
            RinglineBuilder::new(config)
                .launch::<NoopHandler>()
                .err()
                .expect("launch must fail when io_uring_setup is denied"),
        )
    })
    .join()
    .expect("test thread panicked");

    let Some(err) = outcome else {
        return; // seccomp unavailable; nothing to assert
    };

    assert!(
        matches!(err, Error::RingSetup(_)),
        "expected Error::RingSetup, got {err:?}"
    );
    let text = err.to_string();
    assert!(
        text.starts_with("ring setup: io_uring_setup(2): "),
        "{text}"
    );
    assert!(text.contains("EPERM"), "{text}");
    // The message attributes EPERM to whichever policy the host actually has.
    // On a host with `kernel.io_uring_disabled` at 1 or 2 (RHEL-family
    // defaults) that is the sysctl; everywhere else it is the seccomp filter
    // this test installed.
    let disabled: u32 = std::fs::read_to_string("/proc/sys/kernel/io_uring_disabled")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    if disabled >= 1 {
        assert!(
            text.contains(&format!("kernel.io_uring_disabled is {disabled}")),
            "{text}"
        );
        assert!(!text.contains("seccomp"), "{text}");
    } else {
        assert!(text.contains("seccomp"), "{text}");
    }
    assert!(text.contains("force-mio"), "{text}");
}
