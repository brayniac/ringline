# Receiving into the accumulator

Status: proposal, with per-connection rings chosen over the hybrid by the
owner. Nothing here is implemented. The kernel behaviour it relies on was
probed on Linux 6.12 (results below); the performance case is not yet
measured, and the design does not land until it is.

Related: `docs/journal/2026-09-incremental-buffer-consumption.md` (the
open INC entry, which kept one shared ring and set GO/NO-GO criteria),
`docs/journal/2026-07-enobufs-fallback-recv.md` (#274, which rejected
receiving into accumulator spare capacity), `docs/segmented-recv-design.md`,
`docs/recv-multi-identity-design.md`.

## Goal

A TCP receive on io_uring lands in a buffer from one provided-buffer ring
shared by every connection on the worker. The driver then lends that
buffer to the task when the accumulator is empty (`pending_recv_bufs`,
no copy) and otherwise copies it into the connection's `RecvAccumulator`.
The copy is paid by messages that span completions, by `with_bytes`
callers and by streams. Separately, about 860 lines of the driver, plus
the bid lifecycle of the lending paths, exist only because the buffers
are shared and one connection can empty the ring for all of them (see
"What this removes").

This design has the kernel write each connection's bytes into memory that
connection owns, so the copy is gone for every plain-TCP API except
recv sinks, and the shared-ring state goes once no TCP connection uses
the shared ring. The emulator proposal (`docs/ring-emulator-design.md`
on branch `docs/ring-emulator-design`, PR #621) would do the same with
`read(2)` into that memory.

Two shapes were considered:

- **Per-connection rings (chosen).** Every TCP connection receives into
  memory it owns.
- **Hybrid.** Small or idle connections stay on the shared ring and busy
  ones switch to their own. It keeps everything the shared ring needs and
  adds a switch on a live connection. It returns only if the measurements
  below rule out per-connection rings.

## Kernel facts

Probed on Linux 6.12.111 (arm64), io-uring 0.7.12 (the crate ringline
uses), a ring with `SINGLE_ISSUER` and `DEFER_TASKRUN` as ringline sets
up. The probe sources become conformance tests in step 1.

| Question | Result |
|---|---|
| Multishot `RECV` on an `IOU_PBUF_RING_INC` ring | Successive receives append into one buffer: sends of 5, 6 and 10 bytes landed at offsets 0, 5 and 11, each CQE with `F_BUFFER`, `F_MORE`, `F_BUF_MORE`. |
| The ring entry | Rewritten by the kernel in place: after 6 bytes, `addr = base + 6`, `len = 4090`. |
| The CQE that fills the buffer | Clears `F_BUF_MORE`; the arm stays live (`F_MORE`) with nothing posted. |
| More data after that, nothing posted | `-ENOBUFS` without `F_MORE`, ending the arm. It comes at once if bytes are already queued, otherwise at the next arrival. |
| A new entry posted after `F_BUF_MORE` cleared | The live arm continues into it, with no `-ENOBUFS`. |
| A posted entry rewritten in place between `io_uring_enter` calls, arm live | The next receive lands at the new address; the old one is untouched. Depends on `DEFER_TASKRUN` (see "Moving the region"). |
| EOF on a partly used buffer | `res = 0`, no `F_BUFFER`, no `F_MORE`; the entry stays posted at its offset. |
| Cancel the multishot | Terminal `-ECANCELED`; nothing is written afterwards. |
| Unregister the ring, arm live, no data pending | No CQE: the arm stays live. If the bgid is then registered again on new memory, the stale arm writes into it. |
| Unregister the ring, then data arrives | `-ENOBUFS`, ending the arm; the old buffer is untouched. |
| One-shot `RECV` into a region, and its cancel | Completes normally; a cancel completes with `-ECANCELED`. Probed on 6.12 only; one-shot `RECV` is available on every kernel ringline supports. |
| Ring memory | Must be page-aligned (`EINVAL` at a 16-byte offset); a one-entry ring is accepted; bgid 65535 is accepted. |
| Registration cost, 10,000 one-entry rings | Register 5.4–9.2 ms, unregister 2.5–3.7 ms (four runs). |
| `RLIMIT_MEMLOCK` on 6.12 | Not charged: 10,000 rings (39 MiB) registered with an 8 MiB limit, and `VmPin`/`VmLck` stayed at 0. CLAUDE.md states rings are charged from 6.14; not probed here. |

Not established: behaviour under SQPOLL, where the kernel thread may
select buffers concurrently with the worker; charging on 6.14+; whether
`IOU_PBUF_RING_MMAP` rings are charged differently.

## Memory ownership

The kernel writes into a connection's memory, a task may hold slices of
it (`with_bytes`), sends may read from it after its connection closes
(recv-forward, direct echo), and a stale multishot may outlive the
connection. A count on the slot cannot cover all of these, so the memory
is a reference-counted allocation:

- A connection's receive memory is a `RecvRegion`: a raw allocation
  (`NonNull<u8>` and a length, never a `Vec` or a `Box<[u8]>`, so no Rust
  reference spans bytes the kernel writes) behind an `Arc`.
- The cursors live in the driver's per-connection state, not in the
  `Arc`: `head` (first unread byte), `tail` (end of reaped bytes) and
  `posted` (the range posted to the kernel, if any). Views drop on other
  threads, so nothing they touch is mutable.
- These hold a reference: the connection; the posted entry, until its
  arm's terminal CQE; an in-flight one-shot `RECV`, until its CQE; every
  `Bytes` handed to a task; every lend to a send, until its last CQE
  (and zero-copy notification).
- `with_bytes` hands out `Bytes::from_owner` views (`bytes` 1.12.0,
  already a dependency). Each owner is `{Arc<RecvRegion>, offset, len}`
  and exposes only its own range through `as_ref()`. Slicing a view is
  O(1); creating one allocates (`from_owner` boxes its owner), which is a
  cost today's held-remainder path does not pay (see costs).
- Unread and newly received bytes are adjacent in one allocation, so
  `with_bytes` has no merge step. Today a merge copies the remainder when
  new bytes arrive while a task holds slices of it
  (`accumulator.rs`, `unfreeze`).
- The region is reclaimed in place (head and tail back to the front)
  only when the only references are the internal ones (connection,
  posted entry, in-flight one-shot), checked with an acquire load of the
  count. Otherwise, when the connection needs room, it **allocates a new
  region** and copies its unread bytes into it; the old allocation lives
  until its last reference drops.
- Slot reuse never inherits memory: a reactivated slot starts with a new
  region, or a recycled one whose count is one. The sites that reset an
  accumulator today become "take a new region": on io_uring the install
  and connect paths (`event_loop.rs` `install_accepted`, the two connect
  paths); on mio `driver.rs` and `event_loop.rs` at slot reactivation.

This replaces today's `BytesMut` accumulator for TCP. The mio backend
adopts the same type in the same release, so `with_bytes` returns the
same kind of `Bytes` on both backends (an owner-backed `Bytes` never
converts back with `try_into_mut`, which no caller relies on today).

## Arming

A region is posted in one of two ways:

| Arm | Used when | Ends when |
|---|---|---|
| A one-entry INC ring and one multishot `RECV` on it | The kernel supports `IOU_PBUF_RING_INC` (probed at startup: registering with the flag fails with `EINVAL` otherwise), the ring was set up with `DEFER_TASKRUN` (not SQPOLL), and the worker has a free buffer group id | EOF, an error, a cancel, or more data while nothing is posted |
| One-shot `RECV` into `[tail, end)` | Otherwise | Every completion |

A connection's arm kind is chosen when it is first armed and does not
change while it lives.

A buffer group id is a `u16`, so a worker holds at most 65,536 rings,
less the shared UDP ring and quarantined ids. Connections past that on
one worker use the one-shot arm; more workers per core keep every
connection on a ring.

## Moving the region

The kernel writes only into a posted entry, and it writes ahead of the
completions the driver has reaped: in an `io_uring_enter` that runs task
work (`GETEVENTS`), it copies received bytes and advances the INC entry
before the driver reads the CQE (probed). The driver's ringline enters
that do so are the blocking wait, `submit_and_get_events` and `flush()`,
including the mid-drain `flush()` that `flush_interval_us` enables by
default. A plain `submit()` without `GETEVENTS` does not run deferred task
work for an armed recv (probed); it can execute a recv SQE submitted in
that call inline, which only the driver's own arm SQEs are.

The rules:

1. **The kernel's write position is the posted entry's address.** INC
   rewrites the entry as it consumes, so `entry.addr - base` is the true
   end of written bytes; `tail` (from reaped CQEs) lags it. A move copies
   `[head, entry.addr)`, and later CQEs for bytes already moved only
   advance `tail`.
2. **Moves happen only while the CQ is settled**: drained, with no
   `GETEVENTS` enter since. That holds while tasks are polled
   (`poll_ready_tasks` runs after the drain and before the post-poll
   `flush()`). A move requested outside that window (a `flush()` mid-drain
   has run, or the CQ is not empty) is deferred to the next settled
   point, and the task is re-polled then.
3. **Re-post on `F_BUF_MORE` clear.** When a CQE clears `F_BUF_MORE`, the
   posted range is used up and the arm is still live. The driver posts
   the next free range before the next enter, and the arm continues. If
   bytes were already queued when the entry filled, the arm ends with
   `-ENOBUFS` in that same enter (probed) and the driver re-arms; bulk
   streams pay that re-arm on every fill. A two-entry ring, the second
   entry posted at the first one's end address, lets the kernel continue
   into contiguous memory without ending the arm; step 0 measures
   whether it is worth the second entry.
4. **A ring-armed region moves by rewriting its posted entry**, under
   rule 2, on the worker thread. The probe shows the arm continues at the
   new address. This relies on `DEFER_TASKRUN` keeping the kernel's work
   inside the worker's enters, which is why the ring arm is not used under
   SQPOLL.
5. **One-shot arms move between completions.** A one-shot `RECV` is
   re-armed only at the end of the loop iteration, so tasks polled in that
   iteration see an unarmed region. A move needed while a one-shot is in
   flight waits for its CQE.
6. **Nothing else writes into a posted range.** Today's `append` callers
   that write received bytes go away with the bids and
   `pending_recv_bufs`. The park install seeds carried bytes before the
   first arm. `settle_forward_end`, which returns held forward bytes to the
   accumulator today, becomes "move `head` back over the returned range"
   when the region has not moved since, and a copy to the front of a new
   region otherwise.

The conformance tests pin rules 1, 3 and 4, including the reaped-versus-
written gap, so a kernel that behaves differently fails a test, not a
connection.

## Close and slot reuse

This answers the objection that retired the idea before (#274: "accumulator
spare capacity is unsound across close/slot-reuse"). Three rules:

1. **The posted entry holds a reference until its arm's terminal CQE.**
   The connection's ring is unregistered in `close_connection`, but unregistering does
   not end a live arm. The arm ends with the close lead (`ShutdownRdWr`
   before 6.13, `CancelAll` from 6.13: `ring.rs` `close_lead_for`) or the
   recv cancel. Today that cancel is best-effort, dropped when the SQ is
   full; it becomes guaranteed (parked and retried like any other push,
   Domain Invariant 7).
2. **bgid quarantine.** A buffer group id returns to the free list only
   after that terminal CQE. Probed: a stale arm writes into a ring
   registered again under its bgid.
3. **Slot reuse takes new memory.** The reactivated slot never inherits
   the old region, so a one-shot recv or a lend still holding the old
   allocation cannot overlap the new occupant's writes. This is what
   makes the one-shot arm safe (on kernels before 6.12, under SQPOLL,
   and past bgid exhaustion) without gating the Close on the recv, which
   would deadlock: the recv is ended by the lead linked ahead of that
   Close. The one-shot needs its own cancel at close, since today's close
   cancels only the multishot by its user_data.

The generation check on the RecvMulti payload stays; it guards
bookkeeping, not memory.

**Park** moves an idle connection to another worker (#443) and is
unrelated to buffer pressure. For a ring-armed connection it already
cancels the recv and waits for `-ECANCELED`, which is the terminal CQE
rule 1 needs; it retires the ring after the install succeeds, not at
`begin_park`, since an abandoned park re-arms (`abandon_park_drain`).
A one-shot-armed connection always has a recv in flight, which
`park_blocker` refuses today; park cancels the one-shot and waits for
its CQE the same way.

**Worker shutdown** drops the ring before the accumulators (`Driver` field
order, `driver.rs`), as it does for the shared ring today; this design
relies on that order.

## Backpressure

- A region starts at `recv_accumulator_capacity` (default 4 KiB). It grows
  to the parser's `NeedAtLeast` hint (`reserve_target`) or doubles,
  up to `recv_accumulator_max`.
- `recv_accumulator_max` keeps its contract: a connection whose region is
  full at the maximum while the parser still needs more is closed, as
  today. The default is 1 GiB, so per-connection backpressure does not
  engage by default.
- **Lent bytes are capped per connection.** Bytes held by lends (views in
  forward, echo or segment state, not yet released by their last CQE)
  count against a per-connection cap, the role `forward_hold_cap` has
  today. Above it the driver does not re-post; a release re-posts. Without
  the cap, a slow `forward_to` sink would make each fill allocate a new
  region while the lent ones stay alive.
- Re-posting after the task consumes, or after a lend is released, is the
  event that frees space (Domain Invariant 6's rule, re-arm on an event,
  per connection).
- Regions shrink back to `recv_accumulator_capacity` after staying under
  a quarter full for a period, as `CiphertextBuf` already does.

## Paths that lend received memory

Recv-forward (`forward_held`), direct echo, `forward_recv_buf`, segmented
recv and `forward_to` Mode A hand received bytes onward without copying.
Today they hold provided buffers by bid. Here a lend holds a `Bytes` view
of the region (a reference, see "Memory ownership"). The view moves into
the state that already holds lent bytes (`HeldRecvBuf::Owned(Bytes)`,
the slab entry, the forward state) and drops at the send's last CQE or
zero-copy notification. So:

- a lend can outlive its connection, and a send reading it stays valid
  (Domain Invariant 1);
- a lend does not stop the region from growing (growth allocates new
  memory and the old allocation lives until the lend is released), but
  lent bytes are capped (see backpressure);
- release is dropping the view, not replenishing a bid.

Mode B segments (`segments()`) exist to skip the accumulator copy. With
no copy, `with_bytes` gives the same zero-copy views, so Mode B can become
a thin wrapper over it.

**Recv sinks** (`set_recv_sink`, user memory the CQE data is written to)
are filled by a copy out of the region, as from the shared ring today.
Making user memory a kernel target would need the close semantics above
for memory ringline does not own.

## What stays on a shared ring

- **UDP.** Datagrams need no accumulator; the UDP ring is unchanged.
- **The `timestamps` feature.** Its multishot `RECVMSG` interleaves a
  header and control data with the payload in each buffer. It can move to
  a one-shot `RECVMSG` with the payload iovec at the region and control
  data in a small per-connection buffer; it moves only if that passes its
  gate (see measurement), otherwise it stays.
- **TLS, buffered engine** (the default). rustls copies ciphertext into
  its own buffer (`read_tls`), so receiving ciphertext into a region saves
  no copy. It is moved to a region anyway: it is the default engine, and
  while it stays on the shared ring, the starvation and replenish
  machinery stays too. The cost is the region's memory per TLS
  connection.
- **TLS, unbuffered engine.** `CiphertextBuf` becomes a `RecvRegion`
  (today it is a `Vec<u8>` that reallocates in `grow_to`, frees in
  `discard` and copies in `compact`, each of which would break the move
  rules). Receiving into it removes today's provided-buffer →
  `CiphertextBuf` copy. Its no-deadlock bound assumes appends of at most
  64 KiB (`tls/ciphertext.rs`), so a posted range is capped at 64 KiB.

A TLS connection also holds a plaintext region that is never posted to
the kernel (decrypted bytes are written into it, as `tls/mod.rs` appends
today).

While any TCP connection uses the shared ring (timestamps, or kernels
where the measurement keeps the shared ring), the shared-ring mechanisms
stay for those connections.

## What this removes

Each row lists when it can go. Line counts are the inventory's
estimates of non-test code, by reading, not a tool count.

| Mechanism | Today | After |
|---|---|---|
| Fallback one-shot recv (`recv_fallback_inflight`, `fallback_recv_pool`, `fallback_slot_owner`, `OpTag::RecvFallback`), #274 | ~290 LOC, ~410 test LOC | Removed when plain TCP moves (TLS, segmented and forward connections are already ineligible for it) |
| Worker-wide starvation (`recv_starved`, the cross-connection arbitration in `flush_replenish_and_rearm`, `pending_replenish` for TCP), #245 | ~160 LOC | Removed when no TCP connection uses the shared ring |
| Segmented-recv aggregate reserve (`recv_segment_reserve`, `delivery_decision`) | ~60 LOC + config | Removed |
| `pending_recv_bufs` (lending one held bid) | ~200 LOC | Removed: the bytes are in the region |
| `forward_hold_cap` throttle (cancel and re-arm of the multishot) | ~150 LOC | Replaced by the lent-bytes cap (not re-posting) |
| Bid lifecycle in recv-forward, direct echo, segments, `forward_to` (bid arrays, `SendRecvBuf` bid payloads, exactly-once replenish), in `driver.rs`, `event_loop.rs` and `runtime/io.rs` | not counted | Replaced by `Bytes` views |
| Shared-ring fairness | design constraint | Removed |

Public configuration affected:

- `recv_segment_reserve` loses its purpose; `forward_hold_cap` becomes
  the lent-bytes cap.
- `recv_accumulator_capacity` (default 4 KiB) becomes the initial region
  size, so it caps what one completion delivers until the region grows;
  today one completion delivers up to the shared ring's 16 KiB buffer.
  Its default is revisited in step 0.
- `recv_buffer` / `recv_buffer_bgid` size only the rings that remain
  shared (timestamps), and per-connection bgids are allocated around the
  TCP and UDP ids the user chose.

These are breaking changes to batch into a coordinated release.

## What this costs

| Cost | Per connection | At 16,000 connections per worker |
|---|---|---|
| Ring page (6.12+) | 4 KiB | 62.5 MiB of ring pages; locked on 6.14+ (not on 6.12, probed) unless the process has `CAP_IPC_LOCK`. The launch preflight counts `max_connections` × 4 KiB per worker; an `ENOMEM` at registration gives that connection the one-shot arm |
| bgid | 1 of 65,536 per worker, held until the arm's terminal CQE | Past the limit: one-shot arm |
| Region memory | 4 KiB at the default, grows under load | 62.5 MiB, the same as today: `AccumulatorTable` already allocates `max_connections` × 4 KiB. What changes is residency: today's lending fast path can leave an accumulator untouched; a posted region is written by the kernel |
| Syscalls | register and unregister once per connection | measured above |
| State per connection | ring pointer, bgid, `head`/`tail`/`posted`, arm kind | against the fields removed above |
| Kernel lookup | one buffer group lookup per selection, among up to 16,000 groups | not measured; the 16,000-connection cell covers it |
| View allocation | one small heap allocation per `with_bytes` view (`from_owner`) | measured by instructions per byte |
| Buffered TLS | a ciphertext region per connection, with no copy saved | the price of retiring the shared ring |

## Measurement before landing

Step 1 measures the step-0 prototype.

A SystemsLab experiment on the two-machine rig, three configurations:

1. **shared**: the shared ring (today);
2. **ring**: per-connection INC rings;
3. **one-shot**: one-shot `RECV` into the region, on the same kernel.

Workloads, each at 64, 1,000 and 16,000 connections, and one-shot also at
100,000 connections on one worker:

- echo with `with_data` and with `with_bytes`, at 256 B, 4 KiB, 64 KiB
  and 1 MiB;
- mixed message sizes in one stream (the INC journal found homogeneous
  sweeps cannot show this effect);
- the #415 `forward_to` proxy;
- unbuffered TLS echo;
- connection churn (a register and unregister per connection);
- a slow consumer, for resident memory.

Record throughput, p99, instructions per byte, RSS and locked memory.
Five runs per cell, configurations interleaved. The noise of a cell is
max − min of its five runs; a difference between two configurations
counts when it exceeds the larger of their two noises.

The gate is on median throughput and median p99:

- **ring vs shared**: on every workload, throughput no more than 2% lower
  and p99 no more than 2% higher; at 64 KiB and above, throughput higher
  by more than the noise.
- **one-shot vs ring**: throughput within 10% at 1,000 connections.
- **timestamps**: the one-shot `RECVMSG` within 5% of the shared ring on
  throughput, for timestamps to move.
- A cell whose noise exceeds the threshold it is judged against is rerun
  with ten runs before it can pass or fail.

If one-shot misses its gate, kernels below 6.12 keep the shared ring (the
hybrid, by kernel version), and the bgid limit becomes a reason to run
more workers.

## Landing

0. **Prototype.** `RecvRegion`, the ring and one-shot arms, and the move
   rules for plain TCP behind a feature flag, including the two-entry ring
   variant. Enough to run the experiment; not the default.
1. **Probe and measure.** The kernel facts, including the reaped-versus-
   written gap, as conformance tests; then the experiment.
2. **Region receive for TCP, lending paths included**, behind the feature
   flag: plain TCP, recv-forward, direct echo, `forward_recv_buf`,
   segments and `forward_to`, with the lent-bytes cap. Lending modes are
   switched on for live connections, so a connection receiving into a
   region must already support them; they cannot land separately.
3. **TLS** on regions, both engines.
4. **Default on**, once steps 2 and 3 pass the full test suite on both
   arm kinds. The fallback recv is deleted here.
5. **Timestamps**, if it passes its gate.
6. **Delete the shared-ring mechanisms for TCP** once no TCP connection
   uses the shared ring on any supported kernel, with the configuration
   changes, in a coordinated release.
7. **mio** adopts `RecvRegion`, in the same release as step 4, so
   `with_bytes` returns the same `Bytes` kind on both backends.
8. **Emulator**: the region-receive op in the emulated engine (a `read`
   into the region), with conformance tests on both engines.

**Metrics** replace the shared-ring ones: connections on each arm kind,
bgid exhaustion, `-ENOBUFS` arm ends, bytes copied by region
reallocation, lent bytes at the cap.

## Owner decisions

- Per-connection rings, not the hybrid.
- Memory that scales with connections is acceptable (about 8 GB at
  1,000,000 connections, including ring pages).
- The realistic high end is about 10,000 connections on a 4-core cache
  server: 2,500 per worker, about 80 MB of regions and ring pages in
  total, 10 MB of it locked per worker on 6.14+. The 65,536-ring limit
  per worker is far above that; the one-shot overflow past it is a
  safety rule, and more workers per core keep every connection on a ring
  if a deployment ever needs it.

- Locked memory: requiring `CAP_IPC_LOCK` or a raised `RLIMIT_MEMLOCK`
  on 6.14+ for per-connection rings is acceptable.
- Decided ahead of the measurement, so the shared ring for TCP is always
  deleted: kernels below 6.12 and SQPOLL rings use the one-shot arm
  whatever its numbers, and buffered TLS moves to regions.
- Shared-ring INC and one-shot-only are full candidates in the
  measurement, alongside per-connection rings.

## Questions for the owner

1. **Kernel floor.** If the one-shot configuration misses its gate, should kernels below
   6.12 keep the shared ring, or should the floor rise?
2. **SQPOLL.** The ring arm is not used under SQPOLL, so SQPOLL
   deployments run the one-shot arm. Is that acceptable, or should SQPOLL
   be dropped as a configuration?
3. **Buffered TLS on regions.** This design moves buffered TLS to regions
   to retire the shared ring, at a region's memory per TLS connection and
   with no copy saved. The alternative keeps buffered TLS, and with it the
   starvation and replenish machinery, on the shared ring.
