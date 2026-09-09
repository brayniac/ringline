use std::io;

use thiserror::Error;

/// Errors returned by the ringline driver.
///
/// # Recovery Guidance
///
/// | Error | Cause | Recovery |
/// |-------|-------|----------|
/// | `Io` | System call failure | Check `io::ErrorKind`; transient network errors may be retryable |
/// | `RingSetup` | io_uring refused or unsupported | Read the message: it names the sysctl/seccomp/kernel cause; or build with the `force-mio` feature |
/// | `BufferRegistration` | `mmap()` or io_uring registration failed | Check system memory limits (`ulimit -v`) |
/// | `ConnectionLimitReached` | All connection slots in use | Increase via `ConfigBuilder::max_connections(...)` or close idle connections |
/// | `InvalidConnection` | Stale token, connection closed | Re-establish connection; do not reuse the `ConnCtx` |
/// | `SendPoolExhausted` | All send buffer slots in use | Await pending sends to complete before sending more |
/// | `InvalidRegion` | Region ID not registered | Check `MemoryRegion` registration; ensure region outlives usage |
/// | `PointerOutOfRegion` | SendGuard pointer outside registered region | Verify pointer arithmetic; region boundaries are strict |
/// | `ResourceLimit` | `RLIMIT_NOFILE` too low | Increase with `ulimit -n` (recommended: 65536+) |
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// I/O error from a system call.
    ///
    /// Check the underlying [`io::ErrorKind`] for transient vs permanent failures.
    /// Network-related errors (e.g., `ConnectionReset`, `BrokenPipe`) typically
    /// indicate the peer closed the connection.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// io_uring ring setup failed, or the configuration cannot be applied.
    ///
    /// When `io_uring_setup(2)` itself is refused the message names the
    /// errno and the likely cause:
    /// - `EPERM`: the `kernel.io_uring_disabled` sysctl (Linux 6.6+; `2`
    ///   refuses everyone including root, `1` refuses callers outside
    ///   `kernel.io_uring_group` without `CAP_SYS_ADMIN`), or a seccomp
    ///   profile (Docker/containerd default, gVisor, systemd
    ///   `SystemCallFilter=`). The message says which, based on the sysctl.
    /// - `ENOSYS`: kernel built without io_uring.
    /// - `EINVAL`: kernel older than 6.0 rejecting a required setup flag.
    ///
    /// The backend is chosen at build time by `build.rs` (Linux 6.0+ host
    /// gets io_uring); the only opt-out is the `force-mio` cargo feature,
    /// e.g. `cargo build --features ringline/force-mio` from a dependent
    /// crate. There is no runtime fallback.
    #[error("ring setup: {0}")]
    RingSetup(String),

    /// Buffer registration with io_uring failed.
    ///
    /// This typically indicates a system resource limit (memory, VMAs) or
    /// an invalid registration request. Check `ulimit -v` for virtual memory limits.
    #[error("buffer registration: {0}")]
    BufferRegistration(String),

    /// Connection limit reached.
    ///
    /// The worker has no free slots for new connections. Either:
    /// - Increase via `ConfigBuilder::max_connections(...)` (default: 16000)
    /// - Close idle connections to free slots
    /// - Add more worker threads to distribute load
    #[error("connection limit reached")]
    ConnectionLimitReached,

    /// Invalid or stale connection token.
    ///
    /// This occurs when:
    /// - The connection was closed and the slot was reused
    /// - The `ConnCtx` was used after the peer disconnected
    /// - A `ConnToken` was incorrectly cached and reused
    ///
    /// Do not retry with the same token; establish a new connection.
    #[error("invalid connection")]
    InvalidConnection,

    /// Send pool exhausted.
    ///
    /// All send buffer slots are in flight. This is a backpressure signal:
    /// - Await pending `send()` futures before sending more
    /// - Use `send_nowait()` for fire-and-forget with explicit error handling
    /// - Increase via `ConfigBuilder::send_pool(count, slot_size)` (default count: 1024)
    #[error("send pool exhausted")]
    SendPoolExhausted,

    /// Invalid memory region ID.
    ///
    /// The `RegionId` passed to `SendGuard` does not correspond to a
    /// registered `MemoryRegion`. Ensure:
    /// - The region was registered via [`ConfigBuilder::registered_regions`](crate::ConfigBuilder::registered_regions)
    ///   or [`ShutdownHandle::register_region`](crate::ShutdownHandle::register_region)
    /// - The region is still valid (not dropped)
    #[error("invalid memory region ID")]
    InvalidRegion,

    /// Pointer not within the registered memory region.
    ///
    /// `SendGuard` requires the pointer to be strictly within the bounds
    /// of the registered region. This check prevents:
    /// - Use-after-free (pointer to freed memory)
    /// - Buffer overflows (pointer past region end)
    ///
    /// Debug by printing the pointer and region bounds when registering.
    #[error("pointer not within registered region")]
    PointerOutOfRegion,

    /// System resource limit is too low.
    ///
    /// Ringline requires sufficient file descriptors for connections.
    /// The default `RLIMIT_NOFILE` (often 1024) is insufficient for
    /// high-concurrency workloads.
    ///
    /// Set before running: `ulimit -n 65536` or higher.
    #[error("{0}")]
    ResourceLimit(String),
}

/// What the kernel's io_uring policy sysctls reported when ring setup failed.
///
/// `None` means the sysctl could not be read — either the kernel predates
/// it (`kernel.io_uring_disabled` arrived in 6.6) or `/proc/sys` is not
/// mounted. Both fields are read once, only on the failure path.
#[cfg(any(has_io_uring, test))]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RingSetupProbe {
    /// `kernel.io_uring_disabled`: 0 = allowed, 1 = restricted to
    /// `kernel.io_uring_group` members and `CAP_SYS_ADMIN`, 2 = refused for
    /// everyone including root.
    pub(crate) io_uring_disabled: Option<u32>,
    /// `kernel.io_uring_group`: the gid exempted when `io_uring_disabled`
    /// is 1, or `-1` when no group is configured.
    pub(crate) io_uring_group: Option<i64>,
}

#[cfg(has_io_uring)]
impl RingSetupProbe {
    /// Read the policy sysctls from `/proc/sys`. Every failure collapses to
    /// `None`; this runs only after `io_uring_setup(2)` has already failed,
    /// so nothing here may fail loudly.
    pub(crate) fn read() -> Self {
        fn read_sysctl<T: std::str::FromStr>(path: &str) -> Option<T> {
            std::fs::read_to_string(path).ok()?.trim().parse().ok()
        }
        Self {
            io_uring_disabled: read_sysctl("/proc/sys/kernel/io_uring_disabled"),
            io_uring_group: read_sysctl("/proc/sys/kernel/io_uring_group"),
        }
    }
}

#[cfg(any(has_io_uring, test))]
const MIO_HINT: &str = "or build with the `force-mio` cargo feature \
    (`--features ringline/force-mio`) to use the mio backend instead";

/// Name an errno the way strace and the man pages do, so the message can
/// be searched for. `None` for non-OS errors.
#[cfg(any(has_io_uring, test))]
fn errno_name(err: &io::Error) -> Option<&'static str> {
    Some(match err.raw_os_error()? {
        libc::EPERM => "EPERM",
        libc::ENOSYS => "ENOSYS",
        libc::EINVAL => "EINVAL",
        libc::ENOMEM => "ENOMEM",
        libc::EMFILE => "EMFILE",
        libc::ENFILE => "ENFILE",
        libc::EAGAIN => "EAGAIN",
        libc::EFAULT => "EFAULT",
        _ => return None,
    })
}

/// Turn a raw `io_uring_setup(2)` failure into a message that names the
/// cause and the fix.
///
/// `EPERM` is the case that matters: on modern kernels it is almost always
/// either the `kernel.io_uring_disabled` sysctl or a seccomp profile
/// (Docker/containerd default since 2023, gVisor, systemd
/// `SystemCallFilter=`), and the sysctl value distinguishes the two. The
/// bare OS error ("Operation not permitted") says none of that, and `sudo`
/// does not help when the sysctl is 2.
#[cfg(any(has_io_uring, test))]
pub(crate) fn describe_ring_setup_failure(err: &io::Error, probe: &RingSetupProbe) -> String {
    let mut msg = String::from("io_uring_setup(2): ");
    msg.push_str(&err.to_string());
    if let Some(name) = errno_name(err) {
        msg.push_str(" (");
        msg.push_str(name);
        msg.push(')');
    }
    msg.push_str(". ");

    match err.raw_os_error() {
        Some(libc::EPERM) => match probe.io_uring_disabled {
            Some(2) => {
                msg.push_str(
                    "kernel.io_uring_disabled is 2, which refuses io_uring for every \
                     process including root. Set `sysctl kernel.io_uring_disabled=0` \
                     (or =1 and add this user to the kernel.io_uring_group gid), ",
                );
            }
            Some(1) => {
                msg.push_str(
                    "kernel.io_uring_disabled is 1, which refuses io_uring to callers \
                     that lack CAP_SYS_ADMIN and are not in kernel.io_uring_group ",
                );
                match probe.io_uring_group {
                    Some(gid) if gid >= 0 => {
                        msg.push_str(&format!("(gid {gid}). Add this user to that group, "));
                    }
                    _ => msg.push_str("(no group is configured). Set `sysctl kernel.io_uring_group=<gid>` and add this user to it, "),
                }
                msg.push_str("set `sysctl kernel.io_uring_disabled=0`, ");
            }
            other => {
                match other {
                    Some(v) => msg.push_str(&format!("kernel.io_uring_disabled is {v}, so the sysctl is not the cause; ")),
                    None => msg.push_str("this kernel has no kernel.io_uring_disabled sysctl, so it is not the cause; "),
                }
                msg.push_str(
                    "io_uring_setup is most likely denied by a seccomp profile (the \
                     Docker/containerd default profile, gVisor, or a systemd \
                     SystemCallFilter=). Allow io_uring_setup, io_uring_enter and \
                     io_uring_register in that profile, ",
                );
            }
        },
        Some(libc::ENOSYS) => {
            msg.push_str("this kernel was built without io_uring. Use a kernel with CONFIG_IO_URING (6.0+), ");
        }
        Some(libc::EINVAL) => {
            msg.push_str(
                "the kernel rejected a setup flag ringline requires \
                 (IORING_SETUP_DEFER_TASKRUN / SINGLE_ISSUER / COOP_TASKRUN need \
                 Linux 6.0+). Upgrade the kernel, ",
            );
        }
        _ => {
            msg.push_str("Fix the underlying error, ");
        }
    }
    msg.push_str(MIO_HINT);
    msg.push('.');
    msg
}

#[cfg(any(has_io_uring, test))]
impl Error {
    /// Wrap an `io_uring_setup(2)` failure as [`Error::RingSetup`] with the
    /// cause and fix spelled out. Probes the policy sysctls on the failure
    /// path only.
    #[cfg(has_io_uring)]
    pub(crate) fn ring_setup(err: io::Error) -> Self {
        Self::ring_setup_with_probe(err, &RingSetupProbe::read())
    }

    pub(crate) fn ring_setup_with_probe(err: io::Error, probe: &RingSetupProbe) -> Self {
        Error::RingSetup(describe_ring_setup_failure(&err, probe))
    }
}

/// Errors returned by UDP send operations.
///
/// UDP sends can fail due to resource exhaustion even though UDP is
/// connectionless. The ringline runtime maintains per-worker send pools
/// to bound memory usage.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum UdpSendError {
    /// UDP send pool exhausted.
    ///
    /// No free send slot or copy-pool slot available. This is transient:
    /// await pending UDP receives/sends to complete, then retry.
    #[error("UDP send pool exhausted")]
    PoolExhausted,

    /// UDP submission queue full.
    ///
    /// The io_uring submission queue is full. This is rare and indicates
    /// the application is submitting faster than the kernel can process.
    /// Await pending operations before submitting more.
    #[error("UDP submission queue full")]
    SubmissionQueueFull,

    /// UDP I/O error.
    #[error("UDP I/O error: {0}")]
    Io(#[from] io::Error),
}

/// Error returned by [`try_sleep`](crate::try_sleep) and
/// [`try_timeout`](crate::try_timeout) when the timer slot pool is full.
///
/// The timer pool is pre-allocated to avoid allocations during async
/// execution. When exhausted, use the infallible variants [`sleep()`]
/// and [`timeout()`] which will panic instead (preferred in most cases).
///
/// [`sleep()`]: crate::sleep
/// [`timeout()`]: crate::timeout
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("timer slot pool exhausted")]
pub struct TimerExhausted;

#[cfg(test)]
mod tests {
    use super::*;

    fn eperm() -> io::Error {
        io::Error::from_raw_os_error(libc::EPERM)
    }

    fn probe(disabled: Option<u32>, group: Option<i64>) -> RingSetupProbe {
        RingSetupProbe {
            io_uring_disabled: disabled,
            io_uring_group: group,
        }
    }

    #[test]
    fn ring_setup_error_lands_in_ring_setup_variant_not_io() {
        let err = Error::ring_setup_with_probe(eperm(), &probe(None, None));
        assert!(matches!(err, Error::RingSetup(_)), "got {err:?}");
        let text = err.to_string();
        assert!(
            text.starts_with("ring setup: io_uring_setup(2): "),
            "{text}"
        );
        assert!(text.contains("EPERM"), "{text}");
    }

    #[test]
    fn eperm_with_sysctl_2_names_the_sysctl_and_the_fix() {
        let text = describe_ring_setup_failure(&eperm(), &probe(Some(2), Some(-1)));
        assert!(text.contains("kernel.io_uring_disabled is 2"), "{text}");
        assert!(text.contains("including root"), "{text}");
        assert!(text.contains("sysctl kernel.io_uring_disabled=0"), "{text}");
        assert!(text.contains("force-mio"), "{text}");
        assert!(!text.contains("seccomp"), "{text}");
    }

    #[test]
    fn eperm_with_sysctl_1_points_at_group_and_cap_sys_admin() {
        let text = describe_ring_setup_failure(&eperm(), &probe(Some(1), Some(1234)));
        assert!(text.contains("kernel.io_uring_disabled is 1"), "{text}");
        assert!(text.contains("kernel.io_uring_group (gid 1234)"), "{text}");
        assert!(text.contains("CAP_SYS_ADMIN"), "{text}");
        assert!(text.contains("force-mio"), "{text}");
        assert!(!text.contains("seccomp"), "{text}");
    }

    #[test]
    fn eperm_with_sysctl_1_and_no_group_says_so() {
        let text = describe_ring_setup_failure(&eperm(), &probe(Some(1), Some(-1)));
        assert!(text.contains("no group is configured"), "{text}");
    }

    #[test]
    fn eperm_with_sysctl_0_blames_seccomp() {
        let text = describe_ring_setup_failure(&eperm(), &probe(Some(0), Some(-1)));
        assert!(text.contains("kernel.io_uring_disabled is 0"), "{text}");
        assert!(text.contains("seccomp"), "{text}");
        assert!(text.contains("force-mio"), "{text}");
    }

    #[test]
    fn eperm_without_sysctl_blames_seccomp() {
        let text = describe_ring_setup_failure(&eperm(), &probe(None, None));
        assert!(
            text.contains("no kernel.io_uring_disabled sysctl"),
            "{text}"
        );
        assert!(text.contains("seccomp"), "{text}");
        assert!(text.contains("force-mio"), "{text}");
    }

    #[test]
    fn enosys_says_kernel_lacks_io_uring() {
        let err = io::Error::from_raw_os_error(libc::ENOSYS);
        let text = describe_ring_setup_failure(&err, &probe(None, None));
        assert!(text.contains("ENOSYS"), "{text}");
        assert!(text.contains("without io_uring"), "{text}");
        assert!(text.contains("force-mio"), "{text}");
    }

    #[test]
    fn einval_points_at_kernel_version() {
        let err = io::Error::from_raw_os_error(libc::EINVAL);
        let text = describe_ring_setup_failure(&err, &probe(None, None));
        assert!(text.contains("EINVAL"), "{text}");
        assert!(text.contains("6.0"), "{text}");
        assert!(text.contains("force-mio"), "{text}");
    }

    #[test]
    fn other_errors_keep_the_os_message_and_mio_hint() {
        let err = io::Error::from_raw_os_error(libc::ENOMEM);
        let text = describe_ring_setup_failure(&err, &probe(None, None));
        assert!(text.starts_with("io_uring_setup(2): "), "{text}");
        assert!(text.contains("ENOMEM"), "{text}");
        assert!(text.contains("force-mio"), "{text}");
        assert!(!text.contains("seccomp"), "{text}");
    }

    #[test]
    fn non_os_error_has_no_errno_name() {
        let err = io::Error::other("boom");
        let text = describe_ring_setup_failure(&err, &probe(None, None));
        assert!(text.starts_with("io_uring_setup(2): boom"), "{text}");
        assert!(!text.contains("()"), "{text}");
    }
}
