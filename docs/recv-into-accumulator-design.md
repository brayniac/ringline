# Receiving into the accumulator

Status: proposal, with per-connection rings chosen over the hybrid by
the owner. Nothing here is implemented. The kernel behaviour it relies on
was probed on Linux 6.12 (results below); the performance case is not yet
measured, and the design does not land until it is.

## Goal

A TCP receive on io_uring lands in a buffer from one provided-buffer ring
shared by every connection on the worker, and the driver then copies it
into the connection's `RecvAccumulator`. That is one userspace copy per
byte, and a large part of the driver exists only because the buffers are
shared: one connection can empty the ring and starve the others.

This design has the kernel write each connection's bytes straight into
that connection's accumulator. It removes the copy, and it removes the
state the shared ring needs. The emulator (`docs/ring-emulator-design.md`)
does the same with `read(2)` into the same memory, so both engines make
zero userspace copies.

Two shapes were considered:

- **Per-connection rings (this design).** Every TCP connection receives
  into its own accumulator.
- **Hybrid.** Small or idle connections stay on the shared ring and busy
  ones switch to their own. It keeps everything the shared ring needs and
  adds a switch between the two modes on a live connection. Not chosen;
  it returns only if the measurements below rule out per-connection
  rings.

## Kernel facts

Probed on Linux 6.12.111 (arm64) with io-uring 0.7.12, the crate version
ringline already uses:

| Question | Result |
|---|---|
| Does multishot `RECV` on an `IOU_PBUF_RING_INC` ring append into one buffer? | Yes. Sends of 5, 6 and 10 bytes landed at offsets 0, 5 and 11 of the same buffer; each CQE had `F_BUFFER`, `F_MORE` and `F_BUF_MORE`. |
| Does the kernel move the ring entry? | Yes, in place: after 6 bytes the entry read `addr = base + 6`, `len = 4090`. |
| What happens when the buffer fills? | The CQE that fills it clears `F_BUF_MORE` (the buffer is handed back). The next arrival ends the arm with `-ENOBUFS` and no `F_MORE`. |
| EOF on a partly used buffer | `res = 0`, no `F_BUFFER`, no `F_MORE`. The entry stays posted at its current offset. |
| Cancel the multishot | A terminal `-ECANCELED`. Bytes sent afterwards did not reach the buffer. |
| Unregister the ring while a multishot on it is armed | Allowed. The armed recv ends with `-ENOBUFS` on the next arrival; the buffer is untouched. |
| Ring memory | Must be page-aligned (`EINVAL` at a 16-byte offset). A one-entry ring is accepted. Many rings can share one mapping, one page each. |
| Registration cost | 10,000 one-entry rings registered in 9.2 ms (0.9 µs each), unregistered in 3.7 ms. |
| One-shot `RECV` into a given region (no ring) | Works on every kernel ringline supports. |

Not established here: whether Linux 6.14+ charges each ring to
`RLIMIT_MEMLOCK` as CLAUDE.md states for the shared ring (the probe host
is 6.12); and how recv completes under SQPOLL, where the kernel may run
it concurrently with the worker.

## Design

### The receive region

Each connection's accumulator has a **receive region**: its free space,
`[len, capacity)` of the `BytesMut`. The kernel writes there and nowhere
else. The driver tracks how far the kernel has written by summing `res`
(the ring entry carries the same position).

The region is armed in one of two ways:

| Arm | Used when | Ends when |
|---|---|---|
| A one-entry INC ring whose entry is the region, and one multishot `RECV` on it | The kernel supports `IOU_PBUF_RING_INC` (6.12+; probed at startup, since registering with the flag fails with `EINVAL` without it) and the worker has a free buffer group id | The region is full (`-ENOBUFS`), EOF, an error, or a cancel |
| One-shot `RECV` into the region | Otherwise | Every completion |

A connection's arm kind is chosen when it is first armed and does not
change while the connection lives, so there is no switch between modes.

A buffer group id is a `u16`, so one worker holds at most 65,536 rings,
less the shared UDP ring's id. Connections past that on the same worker
use the one-shot arm. A deployment that wants every connection on a ring
runs more workers (more than one per core if needed).

Both give the driver the same state machine:

1. Arm a receive into the region.
2. On each completion, extend the accumulator by `res` and wake the task.
3. When the arm ends, decide: re-arm on the remaining space, or grow or
   compact the accumulator first, or stop (see backpressure).

### When the accumulator may move

Domain Invariant 1 says memory the kernel may write must stay valid until
the kernel is done with it. The accumulator moves when it grows or
compacts (`append`/`reserve`/`prepend`, `with_bytes`'s `take_frozen` and
`put_back`). The rule is:

> The accumulator may move only while no receive is armed into it.

The multishot form gives a natural point for this: `-ENOBUFS` ends the arm
exactly when the region is full, which is when growing or compacting is
needed. The one-shot form has the point after every completion. A move
that is needed while an arm is live (for example `with_bytes` returning a
frozen remainder) waits for the arm to end, or ends it with a cancel and
waits for the terminal CQE.

`with_bytes` hands out `Bytes` slices of the accumulator. The region must
stay owned by the accumulator, not by those slices: `put_back` today can
leave the allocation owned only by user-held `Bytes`, which would free the
kernel's target when they drop. The accumulator keeps its own handle on
any allocation with a posted region.

### Close and slot reuse

This is the objection that retired the idea once before (#274: "accumulator
spare capacity is unsound across close/slot-reuse"). A multishot can
outlive its connection's Close, and with per-connection memory its late
data would land in whatever reuses that memory.

The design answers it with two rules:

1. **Revoke the region at close.** On 6.12+, `close_connection`
   unregisters the connection's ring. The probe shows that an armed
   multishot then gets `-ENOBUFS` and writes nothing. On older kernels a
   one-shot recv cannot be revoked, so the slot's accumulator is not
   reset or reused until that recv's CQE arrives; the close already
   cancels it (the cancel lead, Domain Invariant 8), so the wait is
   bounded, and it joins the existing gates on close (`shutdown_inflight`).
2. **Quarantine the buffer group id.** A stale multishot selects buffers
   by bgid. If the next occupant's ring reused the bgid before the old
   arm's terminal CQE, the stale arm would write into the new
   connection's region. A bgid returns to the free list only after the
   terminal CQE of every arm that used it.

The generation check on the RecvMulti payload stays; it now guards
bookkeeping, not memory.

### Backpressure

Today backpressure comes from the shared ring running out, which also
starves unrelated connections. Here it is per connection:

- The region starts at `recv_accumulator_capacity` (default 4 KiB).
- When the arm ends full, the driver compacts if the task has consumed
  from the front, otherwise grows (doubling), up to
  `recv_accumulator_max`.
- At `recv_accumulator_max` the driver does not re-arm. It re-arms when
  the task consumes, which is the event that frees space. This keeps
  Domain Invariant 6's rule (re-arm on an event, never in a loop) per
  connection.

### Paths that lend received memory

Several paths hand received bytes onward without copying: recv-forward
(`forward_held`), direct echo, segmented recv, `forward_to` Mode A and
`forward_recv_buf`. Today they hold provided buffers by bid. Here they
hold **ranges of the accumulator**, with a pin count:

- A lend pins `[off, off + len)` and the accumulator does not move while
  any pin is live (an extra condition on the rule above).
- The kernel only writes past the region's start, so a pinned range is
  never overwritten.
- Release is "unpin", not "replenish a bid".

Mode B segments (`segments()`) exist to skip the accumulator copy. With no
copy, `with_bytes` gives the same zero-copy slices, so Mode B can become a
thin wrapper over it. This is a simplification, not a requirement.

### What stays on a shared ring

- **UDP.** Datagrams are independent and need no accumulator; the UDP ring
  is unchanged.
- **The `timestamps` feature.** Its multishot `RECVMSG` interleaves a
  header and control data with the payload in each buffer. It moves to a
  one-shot `RECVMSG` per arm with the payload iovec pointing at the region
  and the control data in a small per-connection buffer. If that proves
  too slow it stays on the shared ring; the decision belongs to its own
  PR.
- **TLS.** Ciphertext has to be decrypted, so the plaintext copy into the
  accumulator remains. The ciphertext lands in a per-connection ciphertext
  region the same way plaintext does, and is fed to rustls from there:
  the same copy count as today, without the shared ring. The unbuffered
  engine's `CiphertextBuf` compacts and grows, so it follows the same
  move rule.

## What this removes

From the inventory of state that exists only because receive buffers are
shared (line counts are approximate, excluding tests unless stated):

| Mechanism | Today | After |
|---|---|---|
| Fallback one-shot recv (`recv_fallback_inflight`, `fallback_recv_pool`, `fallback_slot_owner`, `OpTag::RecvFallback`), #274 | ~290 LOC, ~410 test LOC | Removed: the connection's own region is the fallback |
| Worker-wide starvation (`recv_starved`, the cross-connection arbitration in `flush_replenish_and_rearm`, `pending_replenish` for TCP), #245 | ~160 LOC | Per-connection: "region full" is handled by that connection alone |
| Segmented-recv aggregate reserve (`recv_segment_reserve`, `delivery_decision`) | ~60 LOC + config | Removed: a connection holding its own bytes starves only itself |
| `pending_recv_bufs` (with_data borrowing a single held bid) | ~200 LOC | Removed: the bytes are already in the accumulator |
| `forward_hold_cap` / throttle | ~150 LOC | Becomes ordinary per-connection flow control |
| Bid lifecycle in recv-forward, direct echo, segments, `forward_to` (bid arrays, `SendRecvBuf` bid payloads, exactly-once replenish) | spread over ~2,600 LOC | Replaced by accumulator range pins |
| Shared-ring fairness | design constraint | Removed; replaced by a memory budget (below) |

**Park is unaffected.** Ringline's park moves an idle connection to
another worker (#443); it is not about buffer pressure. It gains one step:
the source retires the connection's ring, and the target registers a new
one at install.

## What this costs

| Cost | Per connection | At 16,000 connections per worker |
|---|---|---|
| Ring page (6.12+) | 4 KiB | 62.5 MiB; charged to `RLIMIT_MEMLOCK` on 6.14+ unless the process has `CAP_IPC_LOCK` |
| bgid | 1 of 65,536 per worker | Connections past 65,536 on one worker use the one-shot arm |
| Region memory | starts at 4 KiB, grows under load | 62.5 MiB at the default 4 KiB, against today's fixed 4 MiB shared ring plus 4 KiB accumulators |
| Syscalls | register and unregister once per connection | — |
| State | ring pointer, bgid, written offset, pin count, armed flag | offset by the state removed above |

The memory model changes from a fixed pool per worker to memory that
scales with connections. At 1,000,000 connections that is about 4 GB of
regions, as accumulators cost today, plus about 4 GB of ring pages, which
on 6.14+ need a matching `RLIMIT_MEMLOCK` or `CAP_IPC_LOCK`. Regions
become resident only when data arrives, as accumulators do today. The
owner judged this acceptable.

## Measurement before landing

A SystemsLab experiment on the two-machine rig, with three arms:

1. the shared ring with a copy (today);
2. per-connection INC rings;
3. one-shot `RECV` into the region (the pre-6.12 path, run on 6.12).

Workloads: echo at 256 B, 4 KiB, 64 KiB and 1 MiB messages, and the
`forward_to` proxy from #415, at 64, 1,000 and 16,000 connections. Record
throughput, p99, instructions per byte, RSS and locked memory.

The one-shot arm is also run at 100,000 connections on one worker, the
case where connections past the bgid limit use it.

The design proceeds if arm 2 is no slower than arm 1 at small messages
and faster at large ones, and arm 3 is within a margin to be agreed of
arm 2. If arm 3 is much slower, kernels below 6.12 keep the shared ring
(the hybrid, by kernel version), and the bgid limit becomes a reason to
run more workers rather than an overflow path.

## Landing

1. **Probe and measure.** The experiment above, plus conformance tests
   for the kernel facts table so a kernel that behaves differently fails
   a test, not a connection.
2. **Region receive for plain TCP.** Per-connection rings and the
   one-shot fallback, the move rule, close revocation and bgid quarantine.
   The fallback recv, starvation arbitration, segment reserve and
   `pending_recv_bufs` are deleted.
3. **Lending paths** move from bids to accumulator pins: recv-forward,
   direct echo, `forward_recv_buf`, segments, `forward_to`.
4. **TLS** ciphertext regions.
5. **Timestamps**, if the one-shot `RECVMSG` measures well.
6. **Emulator**: the region-receive op in the emulated engine (a `read`
   into the region), with conformance tests on both engines.

## Owner decisions

- Per-connection rings, not the hybrid.
- Memory that scales with connections is acceptable (about 8 GB at
  1,000,000 connections, including ring pages).
- Around 100,000 connections per worker is an acceptable target; past
  65,536 on one worker the one-shot arm applies, and more workers per
  core is the way to keep every connection on a ring.

## Questions for the owner

1. **`RLIMIT_MEMLOCK`.** One page per connection on 6.14+ raises the
   memlock requirement by about `connections × 4 KiB` per worker. Should
   the startup check require it (failing launch, as today), or should a
   worker that cannot lock more pages use the one-shot arm for further
   connections?
2. **Kernel floor.** If the measurement shows the one-shot arm is too
   slow, should kernels below 6.12 keep the shared ring, or should the
   floor rise?
