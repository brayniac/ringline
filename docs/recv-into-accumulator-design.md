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
callers and by streams. Separately, about 700 lines of the driver, plus
the bid lifecycle of the lending paths, exist only because the buffers
are shared and one connection can empty the ring for all of them (see
"What this removes").

This design has the kernel write each connection's bytes into memory that
connection owns, so the copy is gone for every API, and the shared-ring
state goes once no TCP connection uses the shared ring. The emulator
proposal (`docs/ring-emulator-design.md`, not yet merged) would do the
same with `read(2)` into that memory.

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

- A connection's receive memory is a `RecvRegion`: one allocation behind
  an `Arc`, with `head` (first unread byte), `tail` (end of written
  bytes) and `posted` (the range currently posted to the kernel, if any).
- Everything that may read or write the allocation holds a reference:
  the connection, the posted entry (until its arm's terminal CQE),
  every `Bytes` handed to a task, and every lend to a send.
- `with_bytes` hands out `Bytes::from_owner` views of the region
  (`bytes` 1.12.0, already a dependency). A view shares the allocation;
  slicing it is O(1). There is no "merge" step: unread bytes and newly
  received bytes are already adjacent in the same allocation, so the copy
  `accumulator.rs` warns about (69 GB copied to receive 4.8 GB when the
  merge fell back to copying) does not arise.
- When the connection needs room it cannot get in place (the region is
  full and the head cannot be reclaimed because views or lends still
  reference it), it **allocates a new region** and copies only its unread
  bytes into it. The old allocation lives until its last reference drops.
- Slot reuse never inherits memory: a reactivated slot starts with a new
  region (or a recycled one whose reference count is one).

This replaces today's `BytesMut` accumulator for TCP on io_uring. The
emulator and the mio backend can use the same type.

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

The kernel writes only into a posted entry. Under `DEFER_TASKRUN` the
kernel selects the buffer and copies into it inside the worker's own
`io_uring_enter`, so between two enters no kernel write is in progress.
The rules:

1. **Re-post on `F_BUF_MORE` clear.** When a CQE clears `F_BUF_MORE`, the
   posted range is used up and the arm is still live. The driver
   compacts or grows the region if needed and posts the new free range
   before the next enter. The arm continues; there is no `-ENOBUFS`
   round trip.
2. **Moves between enters.** Any other move of a ring-armed region
   (compaction on the task's side, a new region for the reasons above)
   rewrites the posted entry in place, between enters, on the worker
   thread. The probe shows the arm continues at the new address.
3. **One-shot arms move between completions.** A one-shot `RECV` is not
   re-armed until the end of the loop iteration, so task polls in that
   iteration see an unarmed region and may move it freely. A move needed
   while a one-shot is in flight waits for its CQE.
4. **Nothing else writes into a posted range.** Today's `append` callers
   that write received bytes go away with the bids and
   `pending_recv_bufs`; the park install seeds carried bytes before the
   first arm, which stays correct.

Rule 2 relies on `DEFER_TASKRUN` keeping the kernel's buffer selection
inside the worker's enter. That is why the ring arm is not used under
SQPOLL, and why the conformance tests pin rule 2 so a kernel that behaves
differently fails a test, not a connection.

## Close and slot reuse

This answers the objection that retired the idea before (#274: "accumulator
spare capacity is unsound across close/slot-reuse"). Three rules:

1. **The posted entry holds a reference until its arm's terminal CQE.**
   The connection's ring is unregistered at close, but unregistering does
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
   makes the pre-6.12 path safe without gating the Close on the recv
   (which would deadlock: the recv is ended by the lead linked ahead of
   that Close).

The generation check on the RecvMulti payload stays; it guards
bookkeeping, not memory.

**Park** moves an idle connection to another worker (#443) and is
unrelated to buffer pressure. It already cancels the recv and waits for
`-ECANCELED`, which is the terminal CQE rule 1 needs. It retires the
ring after the install succeeds, not at `begin_park`, since an abandoned
park re-arms (`abandon_park_drain`).

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
- Re-posting after the task consumes, or after a lend is released, is the
  event that frees space (Domain Invariant 6's rule, re-arm on an event,
  per connection).

## Paths that lend received memory

Recv-forward (`forward_held`), direct echo, `forward_recv_buf`, segmented
recv and `forward_to` Mode A hand received bytes onward without copying.
Today they hold provided buffers by bid. Here a lend holds a `Bytes` view
of the region (a reference, see "Memory ownership"), so:

- a lend can outlive its connection, and a send reading it stays valid
  (Domain Invariant 1);
- a lend never blocks the region from growing: growth allocates new
  memory and the old allocation lives until the lend is released;
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
  data in a small per-connection buffer; it moves only if that measures
  within 5% of the shared ring, otherwise it stays.
- **TLS, buffered engine.** rustls copies ciphertext into its own buffer
  (`read_tls`), so a per-connection ciphertext region saves no copy and
  costs memory. These connections stay on the shared ring.
- **TLS, unbuffered engine.** `CiphertextBuf` is a natural region:
  receiving into it removes today's provided-buffer → `CiphertextBuf`
  copy. Its no-deadlock bound assumes appends of at most 64 KiB
  (`tls/ciphertext.rs`), so a posted range is capped at 64 KiB. It costs
  its existing 32 KiB per connection.

While any TCP connection uses the shared ring (buffered TLS, timestamps,
or kernels where the measurement keeps the shared ring), the shared-ring
mechanisms stay for those connections.

## What this removes

When no TCP connection uses the shared ring. Line counts are the
inventory's estimates of non-test code, by reading, not a tool count.

| Mechanism | Today | After |
|---|---|---|
| Fallback one-shot recv (`recv_fallback_inflight`, `fallback_recv_pool`, `fallback_slot_owner`, `OpTag::RecvFallback`), #274 | ~290 LOC, ~410 test LOC | Removed: the connection's own region is the target |
| Worker-wide starvation (`recv_starved`, the cross-connection arbitration in `flush_replenish_and_rearm`, `pending_replenish` for TCP), #245 | ~160 LOC | Per connection |
| Segmented-recv aggregate reserve (`recv_segment_reserve`, `delivery_decision`) | ~60 LOC + config | Removed |
| `pending_recv_bufs` (lending one held bid) | ~200 LOC | Removed: the bytes are in the region |
| `forward_hold_cap` throttle | ~150 LOC | Ordinary per-connection flow control |
| Bid lifecycle in recv-forward, direct echo, segments, `forward_to` (bid arrays, `SendRecvBuf` bid payloads, exactly-once replenish) | spread over ~2,600 LOC | Replaced by `Bytes` views |
| Shared-ring fairness | design constraint | Removed |

Public configuration affected: `recv_segment_reserve` and
`forward_hold_cap` lose their purpose, and `recv_buffer` /
`recv_buffer_bgid` no longer size TCP receive. These are breaking changes
to batch into a coordinated release.

## What this costs

| Cost | Per connection | At 16,000 connections per worker |
|---|---|---|
| Ring page (6.12+) | 4 KiB | 62.5 MiB of ring pages; locked on 6.14+ (not on 6.12, probed) unless the process has `CAP_IPC_LOCK` |
| bgid | 1 of 65,536 per worker, held until the arm's terminal CQE | Past the limit: one-shot arm |
| Region memory | 4 KiB at the default, grows under load | 62.5 MiB, the same as today: `AccumulatorTable` already allocates `max_connections` × 4 KiB. What changes is residency: today's lending fast path can leave an accumulator untouched; a posted region is written by the kernel |
| Syscalls | register and unregister once per connection | measured above |
| State per connection | ring pointer, bgid, `head`/`tail`/`posted`, arm kind | against the fields removed above |
| Kernel lookup | one buffer group lookup per selection, among up to 16,000 groups | not measured; the 16,000-connection arm covers it |

## Measurement before landing

Steps 0 and 1 need a prototype of arms 2 and 3 behind a feature flag.

A SystemsLab experiment on the two-machine rig, three arms:

1. the shared ring (today);
2. per-connection INC rings;
3. one-shot `RECV` into the region, run on the same kernel.

Workloads, each at 64, 1,000 and 16,000 connections, and arm 3 also at
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
Five runs per cell, arms interleaved; a difference counts when it exceeds
the larger run-to-run spread of the two arms.

The design proceeds if, on every workload, arm 2 is within 2% of arm 1 or
better, and at 64 KiB and above it is better by more than the noise. Arm
3 must be within 10% of arm 2 at 1,000 connections. If arm 3 misses that,
kernels below 6.12 keep the shared ring (the hybrid, by kernel version),
and the bgid limit becomes a reason to run more workers.

## Landing

0. **Prototype.** Arms 2 and 3 for plain TCP behind a feature flag, with
   `RecvRegion`. Enough to run the experiment; not merged as the default.
1. **Probe and measure.** The kernel facts as conformance tests, and the
   experiment above.
2. **Region receive for plain TCP** by default: `RecvRegion`, both arms,
   the moving rules, close and quarantine. The shared ring stays for every
   connection not yet moved.
3. **Lending paths** move from bids to `Bytes` views.
4. **Unbuffered TLS** ciphertext regions.
5. **Timestamps**, if it measures within 5%.
6. **Delete the shared-ring mechanisms** once no TCP connection uses the
   shared ring on any supported kernel, together with the configuration
   changes, in a coordinated release.
7. **Emulator**: the region-receive op in the emulated engine (a `read`
   into the region), with conformance tests on both engines.

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

## Questions for the owner

1. **Kernel floor.** If arm 3 misses its threshold, should kernels below
   6.12 keep the shared ring, or should the floor rise?
2. **SQPOLL.** The ring arm is not used under SQPOLL, so SQPOLL
   deployments run the one-shot arm. Is that acceptable, or should SQPOLL
   be dropped as a configuration?
