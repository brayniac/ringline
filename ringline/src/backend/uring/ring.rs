use std::io;
use std::os::fd::RawFd;

use io_uring::cqueue;
use io_uring::squeue;
use io_uring::types::DestinationSlot;
use io_uring::{IoUring, opcode};

use crate::backend::ProvidedBufRing;
use crate::buffer::fixed::FixedBufferRegistry;
use crate::completion::{OpTag, UserData};
use crate::config::Config;
use crate::error::{Error, MemlockLimit, describe_buffer_registration_failure, errno_name};
use crate::memlock::KernelVersion;
use crate::nvme::{NVME_URING_CMD_IO, NvmeUringCmd};

use super::sqe::{self, Link, Op, Sqe};

/// The first kernel that releases a socket removed from the fixed-file table
/// once that socket's own requests have completed. Earlier kernels release
/// removed files in order, so any earlier request on a registered file or
/// buffer, on any connection, holds the socket open (#581).
const FIXED_FILES_RELEASED_PER_FILE_SINCE: KernelVersion = KernelVersion {
    major: 6,
    minor: 13,
};

/// The error for a refused provided-buffer-ring registration.
fn provided_ring_failure(
    err: &io::Error,
    bgid: u16,
    entries: impl std::fmt::Display,
    probe: &crate::error::RingSetupProbe,
) -> Error {
    let name = errno_name(err)
        .map(|n| format!(" ({n})"))
        .unwrap_or_default();
    Error::BufferRegistration(format!(
        "provided buffer ring (bgid {bgid}, {entries} entries): {err}{name}. \
         EINVAL here usually means a kernel older than 5.19 or a \
         ring size that is not a power of two. {}",
        crate::error::provided_ring_enomem_hint(probe)
    ))
}

/// Whether a ring setup error is ENOMEM from `io_uring_setup` or from
/// registering the provided buffer ring.
///
/// On Linux 6.14+ both are charged to RLIMIT_MEMLOCK, and a dropped ring's
/// charge is released asynchronously, so tests that set up rings in quick
/// succession can fail this way until earlier rings are freed (#589). The
/// errors carry only a message, built by `describe_ring_setup_failure` and
/// `provided_ring_failure`, so this reads the errno name from it.
#[cfg(test)]
pub(crate) fn is_memlock_enomem(err: &Error) -> bool {
    match err {
        Error::RingSetup(msg) => {
            msg.starts_with("io_uring_setup(2): ") && msg.contains(" (ENOMEM)")
        }
        Error::BufferRegistration(msg) => {
            msg.starts_with("provided buffer ring ") && msg.contains(" (ENOMEM)")
        }
        _ => false,
    }
}

/// What is hard-linked ahead of a connection's `Close`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CloseLead {
    /// The `Close` alone: a socket handed to another worker, which must stay
    /// open.
    Nothing,
    /// `shutdown(SHUT_RDWR)`, before Linux 6.13. Requests on any connection
    /// can hold the socket open after the `Close`; the shutdown queues the
    /// FIN regardless, and ends this connection's own requests. It runs on
    /// the bounded io-wq pool (#581, #586).
    Shutdown,
    /// Cancel every request on the connection's fixed file. Used for every
    /// close from Linux 6.13: from 6.13 only this connection's requests hold
    /// the socket open, so the `Close` sends the FIN once they have ended, or
    /// an RST if received data is unread. Also used at worker exit on every
    /// kernel (`Driver::run_shutdown`). The cancel runs inline (#586).
    CancelAll,
}

/// The [`CloseLead`] for a connection close on `kernel`: [`Shutdown`] before
/// Linux 6.13 or on a kernel whose version is unknown, [`CancelAll`] from
/// 6.13.
///
/// [`Shutdown`]: CloseLead::Shutdown
/// [`CancelAll`]: CloseLead::CancelAll
pub(crate) fn close_lead_for(kernel: Option<KernelVersion>) -> CloseLead {
    if kernel.is_none_or(|k| k < FIXED_FILES_RELEASED_PER_FILE_SINCE) {
        CloseLead::Shutdown
    } else {
        CloseLead::CancelAll
    }
}

/// Wrapper around IoUring providing high-level SQE submission helpers.
///
/// The ring uses 128-byte SQEs and 32-byte CQEs (`IoUring<Entry128, Entry32>`)
/// to support NVMe passthrough via `IORING_OP_URING_CMD` / `UringCmd80`.
/// [`Sqe::encode`] produces the 128-byte entries; 64-byte opcodes are
/// zero-padded.
///
/// Memory overhead of Big SQE/CQE: +32 KB per worker with default config
/// (256 SQ × 64B extra + 1024 CQ × 16B extra), negligible relative to the
/// ~20 MB of buffer pools allocated per worker.
pub struct Ring {
    pub(crate) ring: IoUring<squeue::Entry128, cqueue::Entry32>,
    /// Recv buffer group ID for multishot recv.
    bgid: u16,
    /// Reusable Entry128 conversion scratch for chain pushes — avoids a
    /// heap allocation per chained send.
    chain_scratch: Vec<squeue::Entry128>,
    /// Whether the ring was set up with `IORING_SETUP_DEFER_TASKRUN`. When
    /// set, the kernel runs task_work — and so posts the CQEs it generates —
    /// only on an `io_uring_enter` carrying `IORING_ENTER_GETEVENTS`.
    defer_taskrun: bool,
    /// Whether the kernel supports `IORING_OP_FIXED_FD_INSTALL` (6.8+).
    ///
    /// Park (tier 3, #443) has to hand a real fd to another worker, but an
    /// established connection's fd lives only in this ring's fixed-file
    /// table — `install_accepted` closes the raw fd once it is registered.
    /// This opcode is the only way to get one back, so it decides whether
    /// park is available at all. See [`Ring::supports_park`].
    fixed_fd_install: bool,
    /// What goes ahead of a connection's `Close` on this kernel. See
    /// [`close_lead_for`].
    close_lead: CloseLead,
    /// Test-only: number of upcoming `push_sqe`/`push_sqe128` calls that
    /// fail as if the SQ were still full after a submit. See
    /// [`Ring::force_push_failures`].
    #[cfg(test)]
    forced_push_failures: usize,
    /// Test-only: the last 64-byte entry pushed by `push_sqe` or
    /// `push_entry`, without link flags, so a test can check which operation
    /// a handler submitted. `UringCmd80` pushes are not recorded.
    #[cfg(test)]
    pub(crate) last_pushed: Option<squeue::Entry>,
    /// Test-only: the registered file index of the last drain `send`, which
    /// `last_pushed` does not show.
    #[cfg(test)]
    pub(crate) last_drain_index: Option<u32>,
}

impl Ring {
    /// Create and configure the io_uring instance.
    ///
    /// Returns [`Error::RingSetup`] rather than `Error::Io` so a refused
    /// `io_uring_setup(2)` names the subsystem and, for `EPERM`, the
    /// `kernel.io_uring_disabled` sysctl or seccomp profile behind it.
    pub fn setup(config: &Config) -> Result<Self, Error> {
        let cq_entries = config
            .sq_entries
            .checked_mul(4)
            .unwrap_or(config.sq_entries);

        let mut builder = IoUring::<squeue::Entry128, cqueue::Entry32>::builder();
        builder.setup_cqsize(cq_entries);
        builder.setup_coop_taskrun();
        builder.setup_single_issuer();

        if config.sqpoll {
            builder.setup_sqpoll(config.sqpoll_idle_ms);
            if let Some(cpu) = config.sqpoll_cpu {
                builder.setup_sqpoll_cpu(cpu);
            }
            // DEFER_TASKRUN is incompatible with SQPOLL (kernel returns EINVAL).
        } else {
            builder.setup_defer_taskrun();
        }

        let ring = builder
            .build(config.sq_entries)
            .map_err(Error::ring_setup)?;

        // Applies to the calling thread's io-wq, which is why the ring is
        // set up on its worker's thread. A zero slot leaves that limit
        // unchanged and reads back its current value, so the first call only
        // reads. The cap is an upper bound: registering it where the
        // kernel's own limit is lower would raise the limit instead.
        if config.iowq_max_workers > 0 {
            let refused = |e: io::Error| {
                Error::RingSetup(format!(
                    "io_uring refused an io-wq worker cap of {}: {e}",
                    config.iowq_max_workers
                ))
            };
            let mut current = [0, 0];
            ring.submitter()
                .register_iowq_max_workers(&mut current)
                .map_err(refused)?;
            if config.iowq_max_workers < current[0] {
                let mut limits = [config.iowq_max_workers, 0];
                ring.submitter()
                    .register_iowq_max_workers(&mut limits)
                    .map_err(refused)?;
            }
        }

        // Probed once here rather than per park: the answer cannot change for
        // the life of the ring, and a failed probe is not a setup failure —
        // it only means park is unavailable.
        let fixed_fd_install = {
            let mut probe = io_uring::Probe::new();
            match ring.submitter().register_probe(&mut probe) {
                Ok(()) => probe.is_supported(opcode::FixedFdInstall::CODE),
                // `IORING_REGISTER_PROBE` is 5.6 and the crate floor is 6.1,
                // so this should not happen — but a refused probe means
                // "assume not supported", never "fail to start".
                Err(_) => false,
            }
        };

        Ok(Ring {
            ring,
            bgid: config.recv_buffer.bgid,
            chain_scratch: Vec::new(),
            defer_taskrun: !config.sqpoll,
            fixed_fd_install,
            close_lead: Self::close_lead_from(config),
            #[cfg(test)]
            forced_push_failures: 0,
            #[cfg(test)]
            last_pushed: None,
            #[cfg(test)]
            last_drain_index: None,
        })
    }

    /// Whether this kernel can return a registered fd to the process table,
    /// and so whether park (tier 3, #443) is available.
    ///
    /// Requires Linux 6.8 for `IORING_OP_FIXED_FD_INSTALL`. The crate floor
    /// stays at 6.1: below 6.8 park is simply unavailable, and nothing else
    /// changes. That is a smaller loss than it sounds, because park exists
    /// only to repair the placement imbalance
    /// [`AcceptMode::Merged`](crate::AcceptMode::Merged) introduces — the
    /// default [`Pool`](crate::AcceptMode::Pool) mode places by round-robin
    /// and has nothing to rebalance. A pre-6.8 deployment that wants even
    /// placement stays on the default and loses nothing.
    #[allow(dead_code)] // first caller lands with the handover (#443 step 5c)
    pub(crate) fn supports_park(&self) -> bool {
        self.fixed_fd_install
    }

    /// What goes ahead of a connection's `Close` on the running kernel. See
    /// [`close_lead_for`].
    pub(crate) fn close_lead(&self) -> CloseLead {
        self.close_lead
    }

    fn close_lead_from(config: &Config) -> CloseLead {
        #[cfg(test)]
        if let Some(lead) = config.close_lead_override {
            return lead;
        }
        let _ = config;
        close_lead_for(KernelVersion::current())
    }

    /// Re-probe an arbitrary opcode. Exists so tests can establish that the
    /// probe mechanism answers at all — a probe that silently reported
    /// everything unsupported would disable park permanently and look
    /// exactly like an old kernel.
    #[cfg(test)]
    pub(crate) fn probe_supported(&self, code: u8) -> bool {
        let mut probe = io_uring::Probe::new();
        match self.ring.submitter().register_probe(&mut probe) {
            Ok(()) => probe.is_supported(code),
            Err(_) => false,
        }
    }

    /// Register a sparse fixed-buffer table sized to the registry, then
    /// fill in any occupied slots via `register_buffers_update`.
    ///
    /// The sparse path lets us add and remove regions dynamically after
    /// launch without re-registering the entire table.
    ///
    /// Failures come back as [`Error::BufferRegistration`] naming the cause;
    /// `ENOMEM` is the `RLIMIT_MEMLOCK` limit in practice.
    pub fn register_buffers(&self, registry: &FixedBufferRegistry) -> Result<(), Error> {
        let iovecs = registry.iovecs();
        if iovecs.is_empty() {
            return Ok(());
        }
        let total: u64 = iovecs.iter().map(|iov| iov.iov_len as u64).sum();
        let attribute =
            |e: io::Error| Error::buffer_registration(e, total, MemlockLimit::read().ok().as_ref());
        let submitter = self.ring.submitter();
        submitter
            .register_buffers_sparse(iovecs.len() as u32)
            .map_err(attribute)?;

        // Apply each occupied slot. Empty slots stay zeroed in the kernel.
        for (slot, iov) in iovecs.iter().enumerate() {
            if iov.iov_base.is_null() {
                continue;
            }
            // Safety: the iovec points at user memory documented to outlive
            // the runtime; tags are unused.
            unsafe {
                submitter
                    .register_buffers_update(slot as u32, std::slice::from_ref(iov), None)
                    .map_err(attribute)?;
            }
        }
        Ok(())
    }

    /// Update a single fixed-buffer slot with a new iovec.
    ///
    /// `iov.iov_base.is_null()` clears the slot.
    ///
    /// # Safety
    ///
    /// The memory described by `iov` must remain valid until either the slot
    /// is cleared or the runtime shuts down. No SQE referencing the slot may
    /// be in flight when this is called.
    pub unsafe fn register_buffers_update_one(
        &self,
        slot: u16,
        iov: libc::iovec,
    ) -> io::Result<()> {
        unsafe {
            self.ring
                .submitter()
                .register_buffers_update(slot as u32, std::slice::from_ref(&iov), None)
                .map_err(|e| {
                    // Surfaces to the caller of `Runtime::register_region`
                    // as an `io::Error`; keep the kind, replace the bare
                    // "Cannot allocate memory" with the memlock guidance.
                    let text = describe_buffer_registration_failure(
                        &e,
                        iov.iov_len as u64,
                        MemlockLimit::read().ok().as_ref(),
                    );
                    io::Error::new(e.kind(), text)
                })?;
        }
        Ok(())
    }

    /// The calling thread's io-wq limits, `[bounded, unbounded]`. Passing 0
    /// for both changes nothing and returns the current values.
    #[cfg(test)]
    pub(crate) fn iowq_max_workers(&self) -> io::Result<[u32; 2]> {
        let mut limits = [0, 0];
        self.ring
            .submitter()
            .register_iowq_max_workers(&mut limits)?;
        Ok(limits)
    }

    /// Register a sparse file table for direct descriptors.
    ///
    /// The kernel sizes this table against `RLIMIT_NOFILE`, so `EMFILE`
    /// means the limit, not fd exhaustion, and is reported as such.
    pub fn register_files_sparse(&self, count: u32) -> Result<(), Error> {
        self.ring
            .submitter()
            .register_files_sparse(count)
            .map_err(|e| match e.raw_os_error() {
                Some(libc::EMFILE | libc::ENFILE) => Error::ResourceLimit(format!(
                    "RLIMIT_NOFILE too low for the fixed file table: io_uring refused \
                     {count} entries ({e}). Raise it with `ulimit -n` to at least \
                     {count} plus overhead, or lower ConfigBuilder::max_connections"
                )),
                _ => Error::Io(e),
            })?;
        Ok(())
    }

    /// Update registered file table at given offset.
    pub fn register_files_update(&self, offset: u32, fds: &[RawFd]) -> io::Result<()> {
        self.ring.submitter().register_files_update(offset, fds)?;
        Ok(())
    }

    /// Register the provided buffer ring with the kernel.
    pub fn register_buf_ring(&self, provided: &ProvidedBufRing) -> Result<(), Error> {
        // Safety: ring_addr points to valid mmap'd memory that outlives the registration.
        unsafe {
            self.ring
                .submitter()
                .register_buf_ring_with_flags(
                    provided.ring_addr(),
                    provided.ring_entries() as u16,
                    provided.bgid(),
                    0,
                )
                .map_err(|e| {
                    provided_ring_failure(
                        &e,
                        provided.bgid(),
                        provided.ring_entries(),
                        &crate::error::RingSetupProbe::read(),
                    )
                })?;
        }
        Ok(())
    }

    /// Unregister the provided buffer ring from the kernel.
    /// Must be called before the ring memory is munmap'd.
    pub fn unregister_buf_ring(&self, bgid: u16) -> io::Result<()> {
        self.ring.submitter().unregister_buf_ring(bgid)?;
        Ok(())
    }

    /// Submit a multishot recvmsg with provided buffer ring for a connection.
    /// Used when SO_TIMESTAMPING is enabled to receive cmsg ancillary data
    /// (kernel timestamps) alongside TCP payload.
    ///
    /// `generation` is the connection's generation at arm time and is carried
    /// whole in the payload — see `Ring::submit_multishot_recv` for why.
    /// Any cancel targeting this request must encode the same payload.
    #[cfg(feature = "timestamps")]
    pub fn submit_multishot_recvmsg(
        &mut self,
        conn_index: u32,
        generation: u32,
        msghdr: *const libc::msghdr,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::RecvMsgMultiTs, conn_index, generation);
        let entry = Sqe::new(
            Op::RecvMsgMulti {
                fd: sqe::Fd::Fixed(conn_index),
                msg: msghdr,
                buf_group: self.bgid,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a one-shot fallback recv into fallback-pool memory for a
    /// connection whose multishot recv is parked on ENOBUFS. The pool slot
    /// is carried in the payload and released by `handle_recv_fallback`;
    /// the pool owns `ptr` until that CQE arrives (SQE memory outlives the
    /// operation even across close/slot-reuse).
    pub fn submit_recv_fallback(
        &mut self,
        conn_index: u32,
        ptr: *mut u8,
        len: u32,
        pool_slot: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::RecvFallback, conn_index, pool_slot as u32);
        let entry = Sqe::new(
            Op::Recv {
                fd: sqe::Fd::Fixed(conn_index),
                buf: ptr,
                len,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a multishot recv with provided buffer ring for a connection.
    ///
    /// `generation` is the connection's generation at arm time. It occupies the
    /// whole 32-bit payload (an exact match, unlike the truncated send-family
    /// generations), so `handle_recv_multi` can reject a completion that
    /// outlived its connection slot: a multishot can survive the fixed-file
    /// `Close` (its cancel is best-effort and is dropped when the SQ is full),
    /// and without this the terminal `-ECONNRESET` would be misattributed to
    /// whichever connection next occupies the index.
    ///
    /// Any cancel targeting this request must encode the same payload — a
    /// cancel matches by `user_data`.
    pub fn submit_multishot_recv(&mut self, conn_index: u32, generation: u32) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::RecvMulti, conn_index, generation);
        let entry = Sqe::new(
            Op::RecvMulti {
                fd: sqe::Fd::Fixed(conn_index),
                buf_group: self.bgid,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Arm a multishot accept on a listener this worker owns (merged accept
    /// mode).
    ///
    /// The `conn_index` field of the user_data carries the **listener index**,
    /// not a connection: no slot exists until a CQE arrives. One CQE per
    /// accepted connection; `IORING_CQE_F_MORE` means the arm is still live,
    /// and its absence means re-arm.
    ///
    /// The listener fd is a plain fd, not a fixed-file index — listeners live
    /// outside the connection table, whose fixed slots are indexed by
    /// `conn_index`.
    ///
    /// No `sockaddr` comes back with a multishot accept (the kernel has
    /// nowhere per-completion to put it), so the caller must `getpeername(2)`
    /// on the accepted fd to learn the peer.
    pub fn submit_accept_multi(&mut self, listener_index: u32, listen_fd: RawFd) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::AcceptMulti, listener_index, 0);
        // The same flags `accept_nonblock` passes to `accept4` on the pool
        // path. Multishot accept defaults to zero, so without this the merged
        // path is the one place in the runtime that hands out an fd which
        // survives `exec` and blocks on a direct read (#460).
        let entry = Sqe::new(
            Op::AcceptMulti {
                fd: sqe::Fd::Raw(listen_fd),
                flags: libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a copied send. The data must be in a SendCopyPool slot.
    /// The pool slot index is stored in the payload for release on CQE.
    pub fn submit_send_copied(
        &mut self,
        conn_index: u32,
        generation: u32,
        ptr: *const u8,
        len: u32,
        pool_slot: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(
            OpTag::Send,
            conn_index,
            UserData::send_payload(pool_slot, generation),
        );
        let entry = Sqe::new(
            Op::Send {
                fd: sqe::Fd::Fixed(conn_index),
                buf: ptr,
                len,
                flags: crate::completion::STREAM_SEND_FLAGS,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a SendMsgZc operation.
    /// The slab index is stored in the payload for lookup on CQE.
    pub fn submit_send_msg_zc(
        &mut self,
        conn_index: u32,
        msg: *const libc::msghdr,
        slab_idx: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::SendMsgZc, conn_index, slab_idx as u32);
        let entry = Sqe::new(
            Op::SendMsgZc {
                fd: sqe::Fd::Fixed(conn_index),
                msg,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a coalesced plaintext send: one plain (non-ZC) `sendmsg` whose
    /// iovecs gather several queued sends. The slab index is in the payload.
    pub fn submit_send_msg_coalesced(
        &mut self,
        conn_index: u32,
        msg: *const libc::msghdr,
        slab_idx: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::SendMsgCoalesced, conn_index, slab_idx as u32);
        let entry = Sqe::new(
            Op::SendMsg {
                fd: sqe::Fd::Fixed(conn_index),
                msg,
                flags: crate::completion::STREAM_SEND_FLAGS as u32,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a plain `send` of `len` bytes at `ptr` on a registered file,
    /// after a vectored send returned `-EAGAIN`.
    ///
    /// io_uring reports `POLLRDHUP` on every poll, so once the peer has
    /// half-closed a `POLLOUT` poll completes at once and the vectored send
    /// fails with `-EAGAIN` again (#603). A `send` waits until the socket has
    /// room; in that state it runs on an io-wq worker thread (#605). `user_data`
    /// names the operation whose completion handler takes the result as a
    /// partial write.
    ///
    /// # Safety
    /// The `len` bytes at `ptr` must stay valid until the CQE arrives.
    pub unsafe fn submit_drain_send_fixed(
        &mut self,
        index: u32,
        ptr: *const u8,
        len: u32,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = Sqe::new(
            Op::Send {
                fd: sqe::Fd::Fixed(index),
                buf: ptr,
                len,
                flags: crate::completion::STREAM_SEND_FLAGS,
            },
            user_data.raw(),
        );
        #[cfg(test)]
        {
            self.last_drain_index = Some(index);
        }
        unsafe { self.push_sqe(&entry) }
    }

    /// As [`submit_drain_send_fixed`](Self::submit_drain_send_fixed), on a raw
    /// descriptor.
    ///
    /// # Safety
    /// The `len` bytes at `ptr` must stay valid until the CQE arrives.
    pub unsafe fn submit_drain_send_fd(
        &mut self,
        fd: RawFd,
        ptr: *const u8,
        len: u32,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = Sqe::new(
            Op::Send {
                fd: sqe::Fd::Raw(fd),
                buf: ptr,
                len,
                flags: crate::completion::STREAM_SEND_FLAGS,
            },
            user_data.raw(),
        );
        unsafe { self.push_sqe(&entry) }
    }

    /// Submit a zero-copy recv-forward send: one plain (non-ZC) `sendmsg` whose
    /// iovecs point directly into held provided recv buffers. The slab index is
    /// in the payload; the slab entry holds the bids to replenish on completion.
    pub fn submit_send_recv_bufs_coalesced(
        &mut self,
        conn_index: u32,
        msg: *const libc::msghdr,
        slab_idx: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::SendRecvBufsCoalesced, conn_index, slab_idx as u32);
        let entry = Sqe::new(
            Op::SendMsg {
                fd: sqe::Fd::Fixed(conn_index),
                msg,
                flags: crate::completion::STREAM_SEND_FLAGS as u32,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a **gathered** Mode A forward write to a socket sink: several
    /// held provided buffers in one `sendmsg`.
    ///
    /// This is what makes forwarding cost one completion per *batch* rather
    /// than one per provided buffer — the asymmetry `run_direct_echo` has
    /// always exploited by gathering a drain's worth into a single send (#397).
    /// Ordering is preserved: `sendmsg` writes the iovecs in order, and the
    /// caller still keeps one write in flight per connection.
    ///
    /// # Safety
    /// `msghdr`, the iovec array it points at, and every buffer those iovecs
    /// point at must stay valid until the CQE arrives. The driver owns all
    /// three in `ForwardWriteState`, which is neither moved nor rebuilt while a
    /// write is in flight.
    pub unsafe fn submit_forward_writev_socket(
        &mut self,
        fd: RawFd,
        msghdr: *const libc::msghdr,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = Sqe::new(
            Op::SendMsg {
                fd: sqe::Fd::Raw(fd),
                msg: msghdr,
                flags: crate::completion::STREAM_SEND_FLAGS as u32,
            },
            user_data.raw(),
        );
        unsafe { self.push_sqe(&entry) }
    }

    /// Gathered Mode A forward write to another **connection** on this worker,
    /// through its registered file index. See
    /// [`submit_forward_writev_socket`](Self::submit_forward_writev_socket).
    ///
    /// # Safety
    /// As `submit_forward_writev_socket`, plus: the sink connection's slot must
    /// not be recycled before the CQE, which the caller checks by generation.
    pub unsafe fn submit_forward_writev_conn(
        &mut self,
        sink_index: u32,
        msghdr: *const libc::msghdr,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = Sqe::new(
            Op::SendMsg {
                fd: sqe::Fd::Fixed(sink_index),
                msg: msghdr,
                flags: crate::completion::STREAM_SEND_FLAGS as u32,
            },
            user_data.raw(),
        );
        unsafe { self.push_sqe(&entry) }
    }

    /// Gathered Mode A forward write to a **file** sink, at `offset`.
    ///
    /// `writev` rather than `sendmsg`: a file sink has an offset and no message
    /// semantics.
    ///
    /// # Safety
    /// The iovec array and the buffers it points at must stay valid until the
    /// CQE arrives; the driver owns both.
    pub unsafe fn submit_forward_writev_file(
        &mut self,
        fd: RawFd,
        iovecs: *const libc::iovec,
        count: u32,
        offset: u64,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = Sqe::new(
            Op::Writev {
                fd: sqe::Fd::Raw(fd),
                iovecs,
                count,
                offset,
            },
            user_data.raw(),
        );
        unsafe { self.push_sqe(&entry) }
    }

    /// Submit a TLS-internal send (handshake, alert). Uses OpTag::TlsSend
    /// so the CQE handler releases the pool slot without calling on_send_complete.
    pub fn submit_tls_send(
        &mut self,
        conn_index: u32,
        generation: u32,
        ptr: *const u8,
        len: u32,
        pool_slot: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(
            OpTag::TlsSend,
            conn_index,
            UserData::send_payload(pool_slot, generation),
        );
        let entry = Sqe::new(
            Op::Send {
                fd: sqe::Fd::Fixed(conn_index),
                buf: ptr,
                len,
                flags: crate::completion::STREAM_SEND_FLAGS,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit an eventfd read (8 bytes).
    pub fn submit_eventfd_read(&mut self, eventfd: RawFd, buf: *mut u8) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::EventFdRead, 0, 0);
        let entry = Sqe::new(
            Op::Read {
                fd: sqe::Fd::Raw(eventfd),
                buf,
                len: 8,
                offset: 0,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a close for a direct file descriptor.
    pub fn submit_close(&mut self, conn_index: u32, lead: CloseLead) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::Close, conn_index, 0);
        let close = Sqe::new(
            Op::Close {
                fd: sqe::Fd::Fixed(conn_index),
            },
            user_data.raw(),
        );
        // A socket removed from the fixed-file table stays open until the
        // requests holding it complete: before Linux 6.13, earlier requests
        // on any registered file or buffer; from 6.13, this connection's own
        // requests. See `CloseLead`. The lead is hard-linked, so the Close
        // runs after it even when it fails. `push_sqe_pair` pushes both
        // together, so a submit cannot separate them. Config validation
        // guarantees an SQ of at least two entries.
        let first = match lead {
            CloseLead::Nothing => {
                unsafe {
                    self.push_sqe(&close)?;
                }
                return Ok(());
            }
            CloseLead::Shutdown => Sqe::new(
                Op::Shutdown {
                    fd: sqe::Fd::Fixed(conn_index),
                    how: libc::SHUT_RDWR,
                },
                UserData::encode(OpTag::CloseShutdown, conn_index, 0).raw(),
            ),
            CloseLead::CancelAll => Sqe::new(
                Op::CancelFdAll {
                    fd: sqe::Fd::Fixed(conn_index),
                },
                UserData::encode(OpTag::CloseCancel, conn_index, 0).raw(),
            ),
        };
        let first = first.link(Link::Hard);
        unsafe { self.push_sqe_pair(first.encode(), close.encode()) }
    }

    /// Submit an async connect for a direct file descriptor.
    pub fn submit_connect(
        &mut self,
        conn_index: u32,
        addr: *const libc::sockaddr,
        addrlen: libc::socklen_t,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::Connect, conn_index, 0);
        let entry = Sqe::new(
            Op::Connect {
                fd: sqe::Fd::Fixed(conn_index),
                addr,
                addrlen,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a timeout SQE. The timespec must remain valid until the CQE arrives.
    pub fn submit_timeout(
        &mut self,
        timespec: *const crate::backend::uring::abi::Timespec,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = Sqe::new(
            Op::Timeout {
                ts: timespec,
                abs: false,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit an absolute timeout SQE. The timespec contains absolute
    /// `CLOCK_MONOTONIC` seconds/nanoseconds. The timespec must remain valid
    /// until the CQE arrives.
    pub fn submit_timeout_abs(
        &mut self,
        timespec: *const crate::backend::uring::abi::Timespec,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = Sqe::new(
            Op::Timeout {
                ts: timespec,
                abs: true,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit an async cancel targeting a specific user_data value.
    /// Recover a real fd for a registered connection, so it can be handed to
    /// another worker (tier 3, #443).
    ///
    /// When `cancel_recv_user_data` is given, an `AsyncCancel` for the armed
    /// multishot recv is pushed first with `IOSQE_IO_LINK`, so the kernel
    /// runs it *before* the install. That ordering is load-bearing: an armed
    /// multishot recv pins the socket independently of the fixed-file table
    /// (see `try_finalize_close`), so a recv left armed on this worker would
    /// keep consuming bytes from a socket already handed to another one.
    ///
    /// A link is all-or-nothing: if the cancel fails — most likely `ENOENT`
    /// because the recv self-terminated between the check and the kernel
    /// running it — the install is completed with `ECANCELED` instead. The
    /// caller treats that as "abandon this park and try again later", which
    /// is the correct outcome rather than an error: park is best-effort.
    /// Cancel a connection's multishot recv ahead of a park.
    ///
    /// Not linked to the install any more. A cancel cannot retract recv CQEs the
    /// kernel has already posted, so an install linked behind it lands while
    /// those are still being delivered and finds the handler's offer already
    /// withdrawn — measured at 9,427 of 9,427 abandonments. The install is
    /// submitted separately, once the handler has drained and re-offered.
    pub fn submit_park_recv_cancel(
        &mut self,
        conn_index: u32,
        cancel_recv_user_data: u64,
    ) -> io::Result<()> {
        let cancel_ud = UserData::encode(OpTag::Cancel, conn_index, 0);
        let cancel = Sqe::new(
            Op::Cancel {
                target: cancel_recv_user_data,
            },
            cancel_ud.raw(),
        );
        unsafe {
            self.push_sqe(&cancel)?;
        }
        Ok(())
    }

    /// Recover a real fd for a connection whose recv is already cancelled.
    pub fn submit_park_install(&mut self, conn_index: u32, generation: u32) -> io::Result<()> {
        let ud = UserData::encode(OpTag::ParkInstall, conn_index, generation);
        let entry = Sqe::new(Op::FixedFdInstall { index: conn_index }, ud.raw());
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    pub fn submit_async_cancel(
        &mut self,
        target_user_data: u64,
        conn_index: u32,
    ) -> io::Result<()> {
        let ud = UserData::encode(OpTag::Cancel, conn_index, 0);
        let entry = Sqe::new(
            Op::Cancel {
                target: target_user_data,
            },
            ud.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a shutdown(SHUT_WR) for a connection.
    pub fn submit_shutdown(&mut self, conn_index: u32, generation: u32) -> io::Result<()> {
        // The generation rides in the payload so the completion can reject a
        // CQE that outlived its connection slot (domain invariant 3). It used
        // to be a bare 0, and the completion was `{}`.
        let user_data = UserData::encode(OpTag::Shutdown, conn_index, generation);
        let entry = Sqe::new(
            Op::Shutdown {
                fd: sqe::Fd::Fixed(conn_index),
                how: libc::SHUT_WR,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a multishot recvmsg for a UDP socket backed by a provided buffer ring.
    ///
    /// `msghdr` is used as a *template* by the kernel to decide how to lay out
    /// each datagram inside the ring buffer it picks (name / control / payload
    /// regions). It must remain valid for as long as the multishot is armed.
    /// Use [`crate::backend::uring::abi::RecvMsgOut::parse`] on the returned buffer to
    /// extract the datagram.
    pub fn submit_recvmsg_multishot(
        &mut self,
        fd_index: u32,
        msghdr: *const libc::msghdr,
        bgid: u16,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = Sqe::new(
            Op::RecvMsgMulti {
                fd: sqe::Fd::Fixed(fd_index),
                msg: msghdr,
                buf_group: bgid,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a sendmsg (copying) for a UDP socket with destination address.
    pub fn submit_sendmsg(
        &mut self,
        fd_index: u32,
        msghdr: *const libc::msghdr,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = Sqe::new(
            Op::SendMsg {
                fd: sqe::Fd::Fixed(fd_index),
                msg: msghdr,
                flags: 0,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a multishot Recv (no peer info, kernel uses the socket's
    /// connected peer) for a UDP socket. Lighter than `RecvMsgMulti` —
    /// the CQE buffer contains only the payload, no `io_uring_recvmsg_out`
    /// header or sockaddr. The socket must already be `connect(2)`ed.
    pub fn submit_multishot_recv_udp(
        &mut self,
        fd_index: u32,
        bgid: u16,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = Sqe::new(
            Op::RecvMulti {
                fd: sqe::Fd::Fixed(fd_index),
                buf_group: bgid,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a single-shot Send for a connected UDP socket. The data lives
    /// in a `send_copy_pool` slot; the slot index is in the payload of
    /// `user_data` so the CQE handler can release it.
    pub fn submit_send_udp(
        &mut self,
        fd_index: u32,
        ptr: *const u8,
        len: u32,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = Sqe::new(
            Op::Send {
                fd: sqe::Fd::Fixed(fd_index),
                buf: ptr,
                len,
                flags: 0,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a PollAdd for a raw file descriptor (e.g., pidfd for process exit).
    pub fn submit_poll_add(&mut self, fd: RawFd, mask: u32, ud: u64) -> io::Result<()> {
        let entry = Sqe::new(
            Op::PollAdd {
                fd: sqe::Fd::Raw(fd),
                mask,
            },
            ud,
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a one-shot PollAdd on `POLLOUT` for a fixed-table TCP fd
    /// after a Send returned `-EAGAIN`. The CQE encodes `pool_slot` so
    /// the handler can resubmit the original send from where it
    /// stopped. (`current_ptr_remaining(pool_slot)` gives the right
    /// `(ptr, len)` to retry with.)
    pub fn submit_send_pollout(
        &mut self,
        conn_index: u32,
        generation: u32,
        pool_slot: u16,
        is_tls: bool,
    ) -> io::Result<()> {
        // Payload: pool_slot in the low 16 bits, is_tls flag in bit 16, and
        // the connection generation's low 15 bits in bits 17..31 so the
        // POLLOUT handler resubmits on the right completion path and can
        // reject a CQE that outlived its connection slot.
        let payload = UserData::send_pollout_payload(pool_slot, is_tls, generation);
        let user_data = UserData::encode(OpTag::SendPollOut, conn_index, payload);
        let entry = Sqe::new(
            Op::PollAdd {
                fd: sqe::Fd::Fixed(conn_index),
                mask: libc::POLLOUT as u32,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit all pending SQEs and wait for at least `min_complete` CQEs.
    ///
    /// A bare `?` here would kill the worker thread (and every connection on
    /// it) on the first transient `io_uring_enter` failure:
    /// - `EINTR`: any signal delivered to the worker interrupts the wait
    ///   regardless of `SA_RESTART` — restart it.
    /// - `EBUSY`: the CQ is backed up (overflow list non-empty); return `Ok`
    ///   so the caller drains completions, which frees CQ space.
    pub fn submit_and_wait(&self, min_complete: u32) -> io::Result<()> {
        loop {
            match self.ring.submitter().submit_and_wait(min_complete as usize) {
                Ok(_) => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) if e.raw_os_error() == Some(libc::EBUSY) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }

    /// Submit pending SQEs and reap deferred completions **without blocking**.
    ///
    /// Use this instead of `submit_and_wait(0)` whenever the event loop
    /// declines to block because a task is runnable.
    ///
    /// `submit_and_wait(0)` does not set `IORING_ENTER_GETEVENTS` (the
    /// io-uring crate sets it only for `want > 0`), and under
    /// `IORING_SETUP_DEFER_TASKRUN` the kernel runs task_work only when that
    /// flag is present. A worker with a permanently runnable task therefore
    /// never blocks, never sets GETEVENTS, and — if the runnable task also
    /// queues no SQEs, so `flush()` takes its empty-SQ shortcut — never reaps
    /// a single completion: no accepts, no recvs, no send completions, and so
    /// no send-pool slots recycled, for as long as that task stays runnable.
    ///
    /// Costs the same one syscall as the `submit_and_wait(0)` it replaces.
    /// Without DEFER_TASKRUN (SQPOLL rings, which cannot enable it) the kernel
    /// posts completions eagerly, so this delegates.
    pub fn submit_and_get_events(&self) -> io::Result<()> {
        if !self.defer_taskrun {
            return self.submit_and_wait(0);
        }
        loop {
            // Safety: as in `flush()` — a shared view of the SQ head/tail
            // atomics, read-only.
            let n = unsafe { self.ring.submission_shared().len() } as u32;
            match unsafe {
                self.ring
                    .submitter()
                    .enter::<()>(n, 0, 1 /* IORING_ENTER_GETEVENTS */, None)
            } {
                Ok(_) => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) if e.raw_os_error() == Some(libc::EBUSY) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }

    /// Submit a timeout SQE that fires after the given duration.
    /// Produces a CQE with the given user_data when it fires (-ETIME)
    /// or is cancelled (-ECANCELED).
    pub fn submit_tick_timeout(
        &mut self,
        ts: *const crate::backend::uring::abi::Timespec,
        user_data: u64,
    ) -> io::Result<()> {
        let entry = Sqe::new(Op::Timeout { ts, abs: false }, user_data);
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Append every completion the ring holds to `out` as
    /// `(user_data, result, flags)`, consuming them.
    pub(crate) fn reap(&mut self, out: &mut Vec<(u64, i32, u32)>) {
        out.extend(
            self.ring
                .completion()
                .map(|cqe| (cqe.user_data(), cqe.result(), cqe.flags())),
        );
    }

    /// Test-only: the number of entries queued in the SQ and not yet
    /// submitted.
    #[cfg(test)]
    pub(crate) fn sq_len(&mut self) -> usize {
        self.ring.submission().len()
    }

    /// Submit pending SQEs without waiting. Used for mid-iteration flush.
    ///
    /// After submitting the SQEs this method issues a second `io_uring_enter`
    /// with `IORING_ENTER_GETEVENTS` and `min_complete=0`.  With
    /// `IORING_SETUP_DEFER_TASKRUN` the kernel only runs task_work (and posts
    /// deferred CQEs to the completion ring) when `IORING_ENTER_GETEVENTS` is
    /// set.  A plain `submit()` call does NOT set that flag, so send-completion
    /// CQEs for the SQEs we just submitted sit in kernel-internal task_work
    /// until the next `submit_and_wait(1)`, causing a "dead" event-loop
    /// iteration that wakes up only to process those CQEs.
    ///
    /// By issuing a non-blocking `enter(GETEVENTS, min=0)` right after submit
    /// we flush task_work inline — the send CQEs land in the CQ ring before
    /// `flush()` returns, so the `drain_completions()` call that follows in
    /// the event loop can consume them immediately.
    pub fn flush(&self) -> io::Result<()> {
        // Combine submit + DEFER_TASKRUN flush into a single kernel entry.
        //
        // The old two-call path was:
        //   submit()                           → enter(sq_len, 0, 0=no-GETEVENTS, None)
        //   enter::<()>(0, 0, GETEVENTS, None) → enter(0,      0, GETEVENTS,       None)
        //
        // Merged into one:
        //   enter(sq_len, 0, GETEVENTS, None)
        //
        // This submits any pending SQEs AND triggers DEFER_TASKRUN task_work
        // delivery in a single syscall, saving one round-trip to the kernel
        // per flush() invocation (≈ once or twice per event-loop iteration).
        //
        // Safety: `submission_shared()` gives a shared view of the SQ head/tail
        // atomics.  We only read `.len()` (sq_tail − sq_head) and never push
        // new entries here, so there is no aliasing or mutation hazard.
        let n = unsafe { self.ring.submission_shared().len() } as u32;
        if n == 0 {
            // Nothing to submit. Pending DEFER_TASKRUN task_work and CQEs are
            // reaped by the event loop's next ring entry, which always carries
            // GETEVENTS — `submit_and_wait(1)` when it blocks, and
            // `submit_and_get_events()` when it declines to because a task is
            // runnable. Skipping the syscall here therefore defers completion
            // reaping by at most one loop iteration. (That second case is why
            // `submit_and_get_events` exists: a plain `submit_and_wait(0)` sets
            // no GETEVENTS, and combined with this shortcut it would strand
            // task_work indefinitely.)
            return Ok(());
        }
        unsafe {
            self.ring
                .submitter()
                .enter::<()>(n, 0, 1 /* IORING_ENTER_GETEVENTS */, None)?;
        }
        Ok(())
    }

    /// Submit a NOP with injected result for error injection testing.
    ///
    /// The kernel will post a CQE with the given `user_data` and `result`,
    /// allowing tests to simulate any CQE (send error, recv EOF, etc.)
    /// through the real submit_and_wait → dispatch_cqe pipeline.
    ///
    /// Requires kernel 6.6+ (IORING_NOP_INJECT_RESULT support).
    #[cfg(test)]
    pub(crate) fn submit_nop_inject(&mut self, user_data: u64, result: i32) -> io::Result<()> {
        let mut entry = opcode::Nop::new().build().user_data(user_data);
        // The high-level Entry doesn't expose nop_flags or len fields.
        // Use raw pointer arithmetic to patch the SQE in-place.
        // SQE layout (64 bytes): opcode(1) flags(1) ioprio(2) fd(4) off(8) addr(8)
        //                         len(4@24) rw_flags/nop_flags(4@28) user_data(8) ...
        let ptr = &mut entry as *mut squeue::Entry as *mut u8;
        unsafe {
            // len is at byte offset 24 in the SQE
            std::ptr::write_unaligned(ptr.add(24) as *mut u32, result as u32);
            // nop_flags (union with rw_flags) is at byte offset 28
            std::ptr::write_unaligned(ptr.add(28) as *mut u32, 1); // IORING_NOP_INJECT_RESULT
        }
        unsafe { self.push_entry(&entry) }
    }

    /// Like `submit_nop_inject` but with IOSQE_IO_LINK set, so the
    /// next SQE in the submission queue is linked to this one.
    #[cfg(test)]
    pub(crate) fn submit_nop_inject_linked(
        &mut self,
        user_data: u64,
        result: i32,
    ) -> io::Result<()> {
        let mut entry = opcode::Nop::new()
            .build()
            .user_data(user_data)
            .flags(squeue::Flags::IO_LINK);
        let ptr = &mut entry as *mut squeue::Entry as *mut u8;
        unsafe {
            std::ptr::write_unaligned(ptr.add(24) as *mut u32, result as u32);
            std::ptr::write_unaligned(ptr.add(28) as *mut u32, 1); // IORING_NOP_INJECT_RESULT
        }
        unsafe { self.push_entry(&entry) }
    }

    /// Push an operation to the submission queue.
    ///
    /// # Safety
    /// The operation's pointers must stay valid until its completion
    /// arrives, and for `SendMsgZc` until its notification.
    pub(crate) unsafe fn push_sqe(&mut self, sqe: &Sqe) -> io::Result<()> {
        unsafe {
            self.push_sqe128(sqe.encode())?;
        }
        #[cfg(test)]
        if !matches!(sqe.op, Op::UringCmd80 { .. }) {
            self.last_pushed = Some(sqe.encode64());
        }
        Ok(())
    }

    /// Push a raw 64-byte entry: the test-only NOP injections, which set
    /// fields `Sqe` does not describe.
    #[cfg(test)]
    unsafe fn push_entry(&mut self, entry: &squeue::Entry) -> io::Result<()> {
        unsafe {
            self.push_sqe128(entry.clone().into())?;
        }
        self.last_pushed = Some(entry.clone());
        Ok(())
    }

    /// Push a 128-byte entry to the submission queue.
    ///
    /// # Safety
    /// The entry must reference valid memory for the lifetime of the operation.
    unsafe fn push_sqe128(&mut self, entry: squeue::Entry128) -> io::Result<()> {
        #[cfg(test)]
        if self.forced_push_failures > 0 {
            self.forced_push_failures -= 1;
            crate::metrics::RING.increment(crate::metrics::ring::SQE_SUBMIT_FAILURES);
            return Err(io::Error::other("forced SQ push failure"));
        }

        // Try to push; if SQ is full, submit first to make room.
        unsafe {
            if self.ring.submission().push(&entry).is_err() {
                self.ring.submit()?;
                if self.ring.submission().push(&entry).is_err() {
                    crate::metrics::RING.increment(crate::metrics::ring::SQE_SUBMIT_FAILURES);
                    return Err(io::Error::other("SQ still full after submit"));
                }
            }
        }
        Ok(())
    }

    /// Push two SQEs adjacently, so a linked pair is never split across
    /// submissions. Submits first if the SQ has room for fewer than two.
    ///
    /// # Safety
    /// Both SQEs must reference valid memory for the lifetime of the operation.
    unsafe fn push_sqe_pair(
        &mut self,
        first: squeue::Entry128,
        second: squeue::Entry128,
    ) -> io::Result<()> {
        #[cfg(test)]
        if self.forced_push_failures > 0 {
            self.forced_push_failures -= 1;
            crate::metrics::RING.increment(crate::metrics::ring::SQE_SUBMIT_FAILURES);
            return Err(io::Error::other("forced SQ push failure"));
        }

        let pair = [first, second];
        unsafe {
            if self.ring.submission().push_multiple(&pair).is_err() {
                self.ring.submit()?;
                if self.ring.submission().push_multiple(&pair).is_err() {
                    crate::metrics::RING.increment(crate::metrics::ring::SQE_SUBMIT_FAILURES);
                    return Err(io::Error::other("SQ still full after submit"));
                }
            }
        }
        Ok(())
    }

    /// Test-only: make the next `count` `push_sqe`/`push_sqe128` calls fail.
    ///
    /// Each forced failure returns an error of the same kind (`Other`) as
    /// the real "SQ still full after submit" path, increments the same
    /// `SQE_SUBMIT_FAILURES` metric, and consumes one unit of `count`
    /// before the real submission queue is touched. `push_sqe` routes
    /// through `push_sqe128`, so every `submit_*` helper is covered.
    /// `push_sqe_chain`'s multi-entry path (`push_multiple`) is not
    /// affected.
    #[cfg(test)]
    pub(crate) fn force_push_failures(&mut self, count: usize) {
        self.forced_push_failures = count;
    }

    /// Push a chain of linked SQEs atomically.
    ///
    /// Sets `IOSQE_IO_LINK` on all entries except the last, so the kernel
    /// executes them sequentially. All entries are pushed via `push_multiple`
    /// to guarantee contiguous placement in the SQ.
    ///
    /// # Safety
    /// All SQEs must reference valid memory for the lifetime of their operations.
    pub(crate) unsafe fn push_sqe_chain(&mut self, entries: &mut [Sqe]) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        if entries.len() == 1 {
            return unsafe { self.push_sqe(&entries[0]) };
        }

        // Link every entry to the next, except the last.
        let last = entries.len() - 1;
        for entry in entries[..last].iter_mut() {
            debug_assert_eq!(entry.link, Link::None, "a chain sets its own links");
            entry.link = Link::Soft;
        }

        // Convert to Entry128 for the Big SQ ring, reusing the scratch to
        // avoid a per-chain heap allocation.
        let mut entries128 = std::mem::take(&mut self.chain_scratch);
        entries128.clear();
        entries128.extend(entries.iter().map(Sqe::encode));

        // Ensure enough room in the SQ for the entire chain.
        {
            let sq = self.ring.submission();
            if sq.capacity() - sq.len() < entries128.len() {
                drop(sq);
                self.ring.submit()?;
                let sq = self.ring.submission();
                if sq.capacity() - sq.len() < entries128.len() {
                    entries128.clear();
                    self.chain_scratch = entries128;
                    return Err(io::Error::other("SQ too small for chain"));
                }
            }
        }

        // Atomic push of the entire chain.
        let pushed = unsafe {
            self.ring
                .submission()
                .push_multiple(&entries128)
                .map_err(|_| io::Error::other("SQ full after flush for chain"))
        };
        // Return the scratch for reuse regardless of outcome.
        entries128.clear();
        self.chain_scratch = entries128;
        pushed?;
        Ok(())
    }

    /// Submit an NVMe passthrough command via `IORING_OP_URING_CMD`.
    ///
    /// The `fd_index` must be a fixed file table index pointing to an opened
    /// NVMe-generic character device (`/dev/ng<X>n<Y>`).
    ///
    /// # Safety
    /// The buffer referenced by `cmd.addr` / `cmd.data_len` must remain valid
    /// until the CQE arrives.
    pub unsafe fn submit_nvme_cmd(
        &mut self,
        fd_index: u32,
        cmd: &NvmeUringCmd,
        user_data: UserData,
    ) -> io::Result<()> {
        let cmd_bytes = cmd.to_bytes();
        let entry = Sqe::new(
            Op::UringCmd80 {
                fd: sqe::Fd::Fixed(fd_index),
                cmd_op: NVME_URING_CMD_IO,
                cmd: cmd_bytes,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a direct I/O read via `IORING_OP_READ`.
    ///
    /// The `fd_index` must be a fixed file table index pointing to a file
    /// opened with `O_DIRECT`.
    ///
    /// # Safety
    /// The buffer at `buf` with length `len` must remain valid and properly
    /// aligned until the CQE arrives. For `O_DIRECT`, the buffer address,
    /// length, and file offset must all be aligned to the logical block size.
    pub unsafe fn submit_direct_read(
        &mut self,
        fd_index: u32,
        buf: *mut u8,
        len: u32,
        offset: u64,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = Sqe::new(
            Op::Read {
                fd: sqe::Fd::Fixed(fd_index),
                buf,
                len,
                offset,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a direct I/O write via `IORING_OP_WRITE`.
    ///
    /// The `fd_index` must be a fixed file table index pointing to a file
    /// opened with `O_DIRECT`.
    ///
    /// # Safety
    /// The buffer at `buf` with length `len` must remain valid and properly
    /// aligned until the CQE arrives. For `O_DIRECT`, the buffer address,
    /// length, and file offset must all be aligned to the logical block size.
    pub unsafe fn submit_direct_write(
        &mut self,
        fd_index: u32,
        buf: *const u8,
        len: u32,
        offset: u64,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = Sqe::new(
            Op::Write {
                fd: sqe::Fd::Fixed(fd_index),
                buf,
                len,
                offset,
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit an fsync via `IORING_OP_FSYNC`.
    ///
    /// The `fd_index` must be a fixed file table index pointing to an opened file.
    pub fn submit_direct_fsync(&mut self, fd_index: u32, user_data: UserData) -> io::Result<()> {
        let entry = Sqe::new(
            Op::Fsync {
                fd: sqe::Fd::Fixed(fd_index),
            },
            user_data.raw(),
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    // ── Filesystem I/O submission methods ──────────────────────────────

    /// Submit an openat via io_uring. The fd is installed directly into the
    /// fixed file table at `fd_index`.
    ///
    /// # Safety
    /// `pathname` must point to a valid null-terminated C string that remains
    /// valid until the CQE arrives.
    pub unsafe fn submit_openat(
        &mut self,
        fd_index: u32,
        pathname: *const libc::c_char,
        flags: i32,
        mode: u32,
        ud: u64,
    ) -> io::Result<()> {
        // `Sqe::encode` builds the destination slot again and relies on
        // this check.
        DestinationSlot::try_from_slot_target(fd_index)
            .map_err(|_| io::Error::other("invalid fd_index for openat"))?;
        let entry = Sqe::new(
            Op::OpenAt {
                path: pathname,
                flags,
                mode,
                file_index: fd_index,
            },
            ud,
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a statx via io_uring.
    ///
    /// # Safety
    /// `pathname` must point to a valid null-terminated C string and `statxbuf`
    /// must point to valid memory, both remaining valid until the CQE arrives.
    pub unsafe fn submit_statx(
        &mut self,
        pathname: *const libc::c_char,
        statxbuf: *mut libc::statx,
        ud: u64,
    ) -> io::Result<()> {
        // STATX_BASIC_STATS = 0x7ff
        let entry = Sqe::new(
            Op::Statx {
                path: pathname,
                buf: statxbuf,
            },
            ud,
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a renameat via io_uring.
    ///
    /// # Safety
    /// `oldpath` and `newpath` must point to valid null-terminated C strings
    /// that remain valid until the CQE arrives.
    pub unsafe fn submit_renameat(
        &mut self,
        oldpath: *const libc::c_char,
        newpath: *const libc::c_char,
        ud: u64,
    ) -> io::Result<()> {
        let entry = Sqe::new(
            Op::RenameAt {
                old: oldpath,
                new: newpath,
            },
            ud,
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit an unlinkat via io_uring.
    ///
    /// # Safety
    /// `pathname` must point to a valid null-terminated C string that remains
    /// valid until the CQE arrives.
    pub unsafe fn submit_unlinkat(
        &mut self,
        pathname: *const libc::c_char,
        flags: i32,
        ud: u64,
    ) -> io::Result<()> {
        let entry = Sqe::new(
            Op::UnlinkAt {
                path: pathname,
                flags,
            },
            ud,
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }

    /// Submit a mkdirat via io_uring.
    ///
    /// # Safety
    /// `pathname` must point to a valid null-terminated C string that remains
    /// valid until the CQE arrives.
    pub unsafe fn submit_mkdirat(
        &mut self,
        pathname: *const libc::c_char,
        mode: u32,
        ud: u64,
    ) -> io::Result<()> {
        let entry = Sqe::new(
            Op::MkDirAt {
                path: pathname,
                mode,
            },
            ud,
        );
        unsafe {
            self.push_sqe(&entry)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigBuilder;

    fn ring_with(cap: Option<u32>) -> Ring {
        let mut builder = ConfigBuilder::new().workers(1).sq_entries(256);
        if let Some(cap) = cap {
            builder = builder.iowq_max_workers(cap);
        }
        let config = builder.build().expect("valid config");
        // Up to 5 s for earlier rings' memlock charge to be released (#589).
        for _ in 0..50 {
            match Ring::setup(&config) {
                Err(e) if is_memlock_enomem(&e) => {
                    std::thread::sleep(std::time::Duration::from_millis(100))
                }
                result => return result.expect("ring"),
            }
        }
        Ring::setup(&config).expect("ring")
    }

    fn online_cpus() -> u32 {
        // SAFETY: sysconf has no preconditions.
        unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) as u32 }
    }

    /// The transient-ENOMEM check matches the messages ring setup and
    /// provided-ring registration build for that errno, and nothing else.
    #[test]
    fn is_memlock_enomem_reads_the_errno_from_the_message() {
        let probe = crate::error::RingSetupProbe::default();
        let enomem =
            Error::ring_setup_with_probe(io::Error::from_raw_os_error(libc::ENOMEM), &probe);
        let eperm = Error::ring_setup_with_probe(io::Error::from_raw_os_error(libc::EPERM), &probe);
        assert!(is_memlock_enomem(&enomem), "{enomem}");
        assert!(!is_memlock_enomem(&eperm), "{eperm}");
        assert!(!is_memlock_enomem(&Error::Io(
            io::Error::from_raw_os_error(libc::ENOMEM)
        )));
        let provided =
            |errno| provided_ring_failure(&io::Error::from_raw_os_error(errno), 0, 16, &probe);
        assert!(is_memlock_enomem(&provided(libc::ENOMEM)));
        assert!(!is_memlock_enomem(&provided(libc::EINVAL)));
    }

    /// The kernel's own bounded limit on this host.
    fn kernel_default() -> u32 {
        256.min(4 * online_cpus())
    }

    /// The default config leaves the kernel's limit.
    #[test]
    fn the_default_leaves_the_kernel_limit() {
        let ring = ring_with(None);
        assert_eq!(ring.iowq_max_workers().expect("query")[0], kernel_default());
    }

    #[test]
    fn a_configured_cap_reaches_the_kernel() {
        let ring = ring_with(Some(2));
        assert_eq!(ring.iowq_max_workers().expect("query")[0], 2);
    }

    /// A cap above the kernel's limit does not raise it.
    #[test]
    fn a_cap_above_the_kernel_limit_leaves_it() {
        let ring = ring_with(Some(100_000));
        assert_eq!(ring.iowq_max_workers().expect("query")[0], kernel_default());
    }

    /// A shutdown goes ahead of a close before 6.13 and on an unknown kernel,
    /// a cancel of the connection's requests from 6.13 (#586).
    #[test]
    fn a_close_leads_with_a_shutdown_before_6_13_and_a_cancel_after() {
        let k = |major, minor| Some(KernelVersion { major, minor });
        assert_eq!(close_lead_for(k(6, 1)), CloseLead::Shutdown);
        assert_eq!(close_lead_for(k(6, 12)), CloseLead::Shutdown);
        assert_eq!(close_lead_for(k(6, 13)), CloseLead::CancelAll);
        assert_eq!(close_lead_for(k(7, 1)), CloseLead::CancelAll);
        assert_eq!(close_lead_for(None), CloseLead::Shutdown);
    }

    /// The ring decides from the running kernel.
    #[test]
    fn the_ring_decides_the_close_lead_from_the_running_kernel() {
        let ring = ring_with(None);
        assert_eq!(ring.close_lead(), close_lead_for(KernelVersion::current()));
    }

    /// A cap of 0 registers nothing.
    #[test]
    fn a_zero_cap_leaves_the_kernel_default() {
        let ring = ring_with(Some(0));
        assert_eq!(ring.iowq_max_workers().expect("query")[0], kernel_default());
    }
}
