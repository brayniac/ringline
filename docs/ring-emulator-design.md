# One driver: an io_uring engine and a userspace ring emulator

Status: proposal. Nothing here is implemented.

## Goal

Ringline has two drivers. The io_uring driver (`backend/uring/`) submits
SQEs and handles CQEs. The mio driver (`backend/mio/`) is a second,
readiness-based implementation of the same runtime: its own send queue,
recv path, close ordering, timers and teardown. Every behaviour exists
twice, and the two copies drift. Recent examples: #617 needed fixing on
both; a TLS `send().await` reported different lengths on each; mio's
`forward_held` moved a different amount per call (#620).

This design keeps one driver, the io_uring one, and puts an engine under
it:

- **`UringEngine`** encodes each submission as an SQE and hands it to the
  kernel. This is what runs today.
- **`EmulatedEngine`** executes the same submissions in userspace with
  nonblocking syscalls, mio readiness and the existing disk-I/O thread
  pool, and posts the completions the kernel would have posted.

The driver cannot tell the two apart. On a platform without io_uring
(macOS, a Linux with `io_uring_disabled`, a seccomp profile that denies
`io_uring_setup`), ringline runs the io_uring driver on the emulator.

What this buys:

- One implementation of every send, recv, close and teardown rule.
- The io_uring driver builds and runs its tests on macOS. Today it cannot
  even be type-checked there.
- The io_uring-only API (`send_chain`, segmented recv, `forward_to`,
  direct echo) becomes available everywhere.
- A test engine: the emulator can inject short sends, `ENOBUFS`,
  `EAGAIN`, reordered completions and late notifications on demand,
  which the kernel produces only under load.

Non-goals:

- Matching io_uring's performance on the emulator. The emulated path has
  to be correct and not pathological, as the mio backend does today.
- Emulating ops the driver does not use (splice, `LINK_TIMEOUT`, futex,
  `READ_FIXED`, ...).

## Buffers

Every operation already reads from or writes into memory ringline owns:

| Memory | Owner | Used by |
|---|---|---|
| Send-pool slots | `SendCopyPool` | copy sends, TLS records |
| Slab entries (iovecs, guards) | `InFlightSendSlab` | coalesced, zero-copy and recv-forward sends |
| Provided recv buffers | `ProvidedBufRing` | multishot recv, UDP recv, recv forward |
| Fallback recv slots | fallback pool | one-shot recv |
| Disk buffers | caller, held by the disk-I/O future | fs, direct I/O |

The engines share these pools. An operation borrows its buffer from the
pool before it is submitted and returns it when its completion is handled,
on either engine. The only difference is who fills or drains the memory:

- **`UringEngine`**: the kernel. A send hands the slot's address to
  `IORING_OP_SEND`; a recv lets the kernel pick a provided buffer.
- **`EmulatedEngine`**: the engine itself. A send `write`s or `writev`s
  from the borrowed memory and posts a CQE when it is done; a recv pops a
  buffer id from the emulated provided ring, `read`s into that buffer and
  posts a CQE that names the id.

The provided buffer ring is ringline memory on both engines. On io_uring
`ProvidedBufRing` writes entries into an mmapped ring and publishes the
tail; the kernel consumes from the head. The emulator keeps the same
entries in a userspace queue and consumes them the same way, so
`replenish`, `free()` and the `ENOBUFS` accounting are unchanged.

## The submission type

The driver builds `io_uring::squeue::Entry` values today. The `io_uring`
crate is Linux-only, so the first step is a ringline-owned type:

```rust
pub(crate) struct Sqe {
    pub(crate) op: Op,
    pub(crate) user_data: u64,
    pub(crate) link: Link, // None, Soft (IO_LINK), Hard (IO_HARDLINK)
}

pub(crate) enum Op {
    RecvMulti { fd: Fd, buf_group: u16 },
    Send { fd: Fd, ptr: *const u8, len: u32, flags: i32 },
    SendMsg { fd: Fd, msg: *const libc::msghdr, flags: i32 },
    SendMsgZc { fd: Fd, msg: *const libc::msghdr },
    Close { fd: Fd },
    // ... one variant per opcode in the table below
}

pub(crate) enum Fd { Fixed(u32), Raw(RawFd) }
```

`UringEngine` encodes an `Sqe` into an `squeue::Entry` (or `Entry128`
for NVMe) at push time. `EmulatedEngine` executes it.

The refactor is bounded by where the driver touches io_uring types today:

| Site | Count |
|---|---|
| `Ring::submit_*` builders in `ring.rs` | 41 |
| `Send` / `SendMsgZc` builders outside `ring.rs` (`handler.rs`, `tls/mod.rs`, `driver.rs`, `runtime/io.rs`, `event_loop.rs`) | 15 |
| Push choke points (`push_sqe`, `push_sqe128`, `push_sqe_pair`, `push_sqe_chain`) | 4 |
| Stored entries (`BuiltSend.entry`) | 1 field |
| Reads of a stored entry's user_data | 3 sites |
| Other io_uring types in the driver: `Timespec`, `RecvMsgOut::parse`, `cqueue::{more, notif, buffer_select}` | small; replaced by ringline types and flag constants |

The ~100 calls to `Ring::submit_*` across the driver keep their
signatures; only the bodies of the 41 builders change.

Completions need no new type. The event loop already copies each CQE into
`cqe_batch: Vec<(u64, i32, u32)>` and dispatches the tuple
(`dispatch_cqe`). An engine produces tuples.

## The engine interface

```rust
pub(crate) trait Engine {
    /// Queue one submission. Err when the queue is full (the driver's
    /// existing retry lists handle that, as for a full SQ).
    fn push(&mut self, sqe: Sqe) -> io::Result<()>;
    /// Queue a linked chain atomically.
    fn push_chain(&mut self, sqes: &[Sqe]) -> io::Result<()>;
    /// Start queued work without waiting.
    fn submit(&mut self) -> io::Result<()>;
    /// Start queued work and wait until at least one completion is ready
    /// or `timeout` passes.
    fn submit_and_wait(&mut self, timeout: Option<Duration>) -> io::Result<()>;
    /// Move ready completions into `out`.
    fn reap(&mut self, out: &mut Vec<(u64, i32, u32)>);
}
```

`Ring` becomes `UringEngine`. The emulator implements the same calls.

The interface is chosen at compile time (`has_io_uring` selects
`UringEngine`). A runtime choice is not part of this design: it would
fight the `CURRENT_DRIVER` raw-pointer design as backend selection does
today. A later change could pick the engine at launch on Linux, so a
kernel that refuses `io_uring_setup` falls back instead of failing with
`Error::RingSetup`; that needs only the engine, not the driver, to be
generic.

## Completion timing

The driver depends on when completions appear, not just on what they say.
The emulator follows the rules the driver relies on with `DEFER_TASKRUN`:

1. **A completion is never posted inside `push`.** Work starts in
   `submit` or `submit_and_wait`, and completions become visible only
   through `reap`. Handlers never run re-entrantly inside a submission.
2. **Independent submissions are not ordered.** The driver must not
   depend on completion order between unrelated SQEs (Domain Invariant
   2), and the emulator does not promise one. Its default order is
   submission order; a test mode shuffles independent completions to
   catch code that relies on it.
3. **A linked chain runs in order**, and a failed or short link fails the
   rest with `-ECANCELED`.

## Op-by-op

Difficulty: **A** maps directly onto a syscall, **B** needs state in the
emulator, **C** reproduces a kernel rule the driver depends on, **U**
unsupported on the emulator.

| OpTag(s) | Opcode | Emulation | |
|---|---|---|---|
| RecvMulti | RECV multishot, buffer select | On readable: pop a buffer id, `read` into it, post `(res, F_BUFFER\|bid<<16\|F_MORE)`; repeat until `EAGAIN`. No buffer: post `-ENOBUFS` without `F_MORE` (the arm ends; the driver re-arms on replenish, Domain Invariant 6). EOF: post `0` without `F_MORE`. | C |
| RecvMsgMultiTs, RecvMsgUdp | RECVMSG multishot | As RecvMulti, and write the `io_uring_recvmsg_out` header, name and control data into the buffer in the kernel's layout so `RecvMsgOut::parse` (or its replacement) reads it. Timestamps and GRO control messages are Linux-only. | C |
| RecvUdp | RECV multishot | As RecvMulti on a UDP socket. | B |
| RecvFallback | RECV one-shot into a pool slot | `read` when readable, one CQE. | A |
| Send, TlsSend, SendRecvBuf, *Drain | SEND, `MSG_WAITALL` | `write` until all bytes are sent, waiting for writable in between; one CQE with the total, or the error. `MSG_WAITALL` means the kernel never reports a short count while the socket is open, so neither does the emulator. | B |
| SendMsgCoalesced, SendRecvBufsCoalesced, ForwardWrite | SENDMSG / WRITEV | `writev` the iovecs with the same waiting rule. The emulator need not reproduce `-EAGAIN` after a peer's half-close (#603); the driver's drain path stays correct either way. | B |
| SendMsgZc | SENDMSG_ZC | `writev` (a copy, as mio does today), then post the operation CQE with `F_MORE` and a separate `F_NOTIF` CQE in the same reap. The driver's notification accounting (#487) runs unchanged. | C |
| SendPollOut | POLL_ADD POLLOUT | Post when writable. | A |
| Connect | CONNECT | Nonblocking `connect`, then `SO_ERROR` on writable, as the mio backend does today. | A |
| Shutdown, CloseShutdown | SHUTDOWN | `shutdown(2)`. | A |
| Close | CLOSE (fixed) | Remove the fd from the emulated fixed-file table and close it. Requests still pending on that fd are cancelled first, as Linux 6.13+ does for the connection's own requests (Domain Invariant 8). The emulator reports itself as the 6.13 behaviour, so `close_lead_for` picks the cancel lead. | C |
| CloseCancel, Cancel | ASYNC_CANCEL (by user_data; by fixed fd, all) | Exact 64-bit user_data match, or every pending request on the fixed fd. The cancelled request posts `-ECANCELED`; the cancel posts `0` or `-ENOENT`. | B |
| Timeout, TickTimeout, Timer | TIMEOUT (relative, `ABS`) | A min-heap of deadlines, as `TimerSlotPool` has for mio today. Fires post `-ETIME`; a cancel posts `-ECANCELED`. `submit_and_wait` waits until the earliest deadline. | B |
| EventFdRead | READ on the eventfd | The worker's wake fd (a pipe on macOS): post when readable. | A |
| AcceptMulti | ACCEPT multishot | On readable, `accept` until `EAGAIN`, one CQE per fd with `F_MORE`. | B |
| ParkInstall | FIXED_FD_INSTALL | `dup` the fixed slot's fd and post it. | A |
| PidfdPoll | POLL_ADD on a pidfd | Linux only. Elsewhere `ChildProcess::wait` stays unsupported, as on mio today. | A / U |
| SendMsgUdp, SendUdp | SENDMSG (with `UDP_SEGMENT`), SEND | `sendmsg`; GSO is Linux-only, so elsewhere the engine splits into one datagram per segment. | B |
| Fs, DirectIo | OPENAT (direct install), READ, WRITE, FSYNC, STATX, RENAMEAT, UNLINKAT, MKDIRAT | The existing disk-I/O thread pool (`disk_io_pool.rs`) runs the call and posts the CQE through the engine. OPENAT installs into the emulated fixed-file table. STATX is filled from `statx` on Linux and `fstatat` elsewhere. | B |
| NvmeCmd | URING_CMD (SQE128) | Not emulated: post `-EOPNOTSUPP`. NVMe already requires io_uring. | U |

The fixed-file table is emulated as a slot → fd array. The driver's
`register_files_update` calls go to the engine; on io_uring they register
with the kernel, on the emulator they write the array. Fixed buffer
registration is a no-op on the emulator: no SQE the driver builds
references `buf_index`.

The emulator implements the ring setup flags' effects only where the
driver can observe them. `COOP_TASKRUN`, `SINGLE_ISSUER` and the io-wq
worker cap have no counterpart.

## Tests

Three layers:

1. **Engine conformance tests.** For each op, a test drives it through
   the `Engine` interface against real sockets and files and checks the
   CQEs: result, flags and buffer ids. On Linux the same tests run
   against `UringEngine` and `EmulatedEngine`, so the emulator is checked
   against the kernel rather than against a description of it.
2. **The driver's unit tests.** The ~700 io_uring lib tests run on both
   engines. Tests that inject results through the real ring
   (`IORING_NOP_INJECT_RESULT`) need an engine-level injection hook; the
   emulator implements it directly.
3. **The integration tests** (`tests/*.rs`) run on both engines, on
   Linux in CI and on macOS.

## Landing

Each step is a separate PR and leaves both backends working.

1. **`Sqe`.** Introduce the ringline-owned submission type and encode it
   in `Ring`. `BuiltSend` stores an `Sqe`. Linux only, no behaviour
   change.
2. **`Engine`.** Turn `Ring` into `UringEngine` behind the `Engine`
   interface. Replace the remaining io_uring types in the driver
   (`Timespec`, `RecvMsgOut`, the `cqueue` helpers) with ringline types.
   After this the driver compiles without the `io_uring` crate.
3. **First emulated slice.** `EmulatedEngine` on Linux behind a
   `ring-emulator` feature, with RecvMulti, Send, Close, the close leads,
   Cancel, Timeout/TickTimeout/Timer and EventFdRead. Acceptance: the
   plain TCP tests in `tests/echo.rs` pass on the emulator, and the
   conformance tests for those ops pass on both engines.
4. **The rest of the table**, op group by op group: connect and accept;
   coalesced, zero-copy and forward sends; chains; UDP; fs and direct
   I/O; park.
5. **macOS.** Build the io_uring driver on the emulator on macOS and run
   the full suite in CI.
6. **Retire the mio backend** once the emulator passes everything the
   mio backend passes. `force-mio` becomes the switch that selects the
   emulator on Linux.

## Questions for the owner

1. **Retire the mio backend at step 6, or keep it?** Keeping it keeps
   two drivers, which is the cost this design removes. Retiring it means
   the emulator carries macOS alone.
2. **Engine choice at launch.** Should step 6 also let a Linux build fall
   back to the emulator when `io_uring_setup` is refused (RHEL 10 ships
   `io_uring_disabled = 2`), instead of requiring a `force-mio` build?
3. **Emulated recv copies twice** (socket → provided buffer →
   accumulator), where mio copies once into its accumulator today. The
   emulator could read straight into the accumulator for RecvMulti, but
   then the provided-buffer accounting the driver relies on would not be
   exercised. This design takes the second copy; measure before changing
   it.

## Found while surveying

- **The connect-timeout cancel never matches.** `arm_connect_timeout`
  arms the timeout with `payload = generation` (`handler.rs`), but the
  three cancels of it encode payload 0, and `ASYNC_CANCEL` matches the
  whole user_data. The timeout therefore always runs to `-ETIME`, which
  `handle_timeout` ignores once the connect has completed. Harmless
  today; the emulator's exact-match cancel must not hide it.
- **A stale comment** in `ring.rs` (around `submit_park_cancel`)
  describes an `IO_LINK` from the park recv-cancel to the install, which
  the code no longer has.
