# Two shared receive rings per worker, consumed incrementally where supported

Status: proposal. Nothing here is implemented. The measurements behind it,
and the alternatives that were measured and not chosen, are in
`docs/journal/2026-09-incremental-buffer-consumption.md`, sections
"2026-10: measurements" and "2026-10: kernels, two rings, tiered caches".

Related: `docs/segmented-recv-design.md` (the bid lifecycle of the lending
paths), `docs/journal/2026-07-enobufs-fallback-recv.md` (#274, the fallback
recv).

## Goal

Today a TCP receive on io_uring lands in a buffer from one provided-buffer
ring shared by the worker's connections, 256 buffers of 16 KiB
(`config.rs` `RecvBufferConfig::default`). Buffer size sets how much one
completion can deliver; at a fixed memory budget, larger buffers mean fewer
of them. Messages larger than a buffer pay a completion per buffer, and a
connection whose task holds its bytes keeps buffers every other connection
needs.

This design gives each worker two shared TCP rings, registered as two buffer
groups. The defaults follow the ring kind selected (see "Selecting the ring
kind"), not the kernel version:

| Ring kind | Selected on | Small group (every connection starts here) | Large group (promoted connections) | Large group default | Ring memory per worker |
|---|---|---|---|---|---|
| `IOU_PBUF_RING_INC` | Linux 6.12 and later | 64 buffers of 1 MiB | 64 buffers of 1 MiB | off | 64 MiB, 128 MiB with the large group |
| plain | Linux 6.1–6.11, or `recv_incremental(false)` | 4096 buffers of 64 KiB | 256 buffers of 1 MiB | on | 512 MiB |

With `IOU_PBUF_RING_INC` one buffer is consumed incrementally by successive
completions, from any connection, and returns to the ring only when it is
used up, so a large buffer also serves small arrivals. Plain rings are
backed with `MADV_NOHUGEPAGE` so a completion makes resident only the 4 KiB
pages it writes.

A connection moves to the large group when it streams or holds a lend; see
"The large group". Promoted streaming connections draw from the large
group's buffers instead of the small group's.

With an INC ring the large group is off by default. On 6.12, each
difference between one and two groups was smaller than the rep-to-rep
range of one of the two configurations (three reps). The sign changed
between the run with the first promotion rule and the run with the
`SOCK_NONEMPTY` rule (journal). Landing step 6's measurements decide
whether it is turned on. Before 6.12, in every cell with 16 streamers
sharing the worker, two groups cut request p50 and p99 by 2.8× to 25×. In
cells without streamers they tied one group, except p999 in the
heavy-tailed tiered cell, which rose from 5.2 to 6.6 ms.

The copy into the accumulator, the lend-in-place paths and the `ENOBUFS`
fallback stay. The accumulator copy is bounded: see "Bounded accumulator".
Under INC a buffer holds the bytes of several completions at increasing
offsets, and returns to the ring only when the kernel has finished with it
and nothing holds any of its bytes.

## Costs

- The receive copy into the accumulator stays, bounded to what completes a
  pending message when the parser announced its length.
- The `ENOBUFS` park, the fallback recv and the starvation arbitration stay,
  and the buffer state below is added, once per group.
- Above a group's lend cap, arrivals are copied instead of lent.
- `with_bytes` keeps its merge copy when bytes arrive while a task holds
  slices of the remainder.
- Ring memory per worker rises from today's 4 MiB to 64 or 128 MiB (6.12+,
  without and with the large group) or 512 MiB (before 6.12). Each group's
  touched pages stay resident. Every measurement used one worker. Peak
  process RSS in the benchmark, MiB, median of three reps per cell (range
  across the cells in the row):

  | Cells | 6.12 one group | 6.12 two groups | 6.1 4096 × 64 KiB | 6.1 two groups |
  |---|---|---|---|---|
  | mixed (1000 request/response + 16 streamers) | 71 | 117–135 | 282 | 580–583 |
  | streaming (1000 connections) | 264 | 328–329 | 361 | 709 |
  | heavy-tailed request sizes | 774–778 | 823–842 | 970–990 | 1061–1275 |

  In the heavy-tailed cells the accumulators dominate: a bounded
  accumulator holds up to one message, 1 MiB here, per connection, at 1000
  connections.
- A promoted connection's small messages each take a whole large-group
  buffer on a plain ring. On 6.1, holders promoted alongside 16 streamers
  left the 256-buffer large group returning `ENOBUFS` 7.8k times per run
  (10.9k with heavy-tailed sizes). The same holders without streamers
  returned none.
- On 6.1 (Amazon Linux 2023, loopback) the 4096 × 64 KiB ring had a tail no
  other geometry had at 10,000 connections: p99 906 ms and p999 2.7 s at
  256 B against 130 and 134 ms for the 1024-buffer geometries, and p999
  5.1 s at 64 KiB. It is unexplained. Across two hosts on Debian 12's 6.1
  its p999 at 64 KiB × 10,000 was 1074 ms against 805–872 ms for the
  1024-buffer geometries.
- At equal offered load with one group, latency matched the 256 × 16 KiB
  ring's or was lower, except p999 at 4 KiB messages near capacity
  (1000 connections): 2.4 ms against 1.8 ms on hv01, 3.3 ms against 2.8 ms
  across hosts.
- The ring emulator (#621), if it lands, must reproduce incremental shared
  buffers and two groups.

## Selecting the ring kind

At startup the worker registers the small group with `IOU_PBUF_RING_INC`.
`EINVAL` selects plain rings for both groups; Linux 6.1 (Debian 12 and
Amazon Linux 2023) returns it, and 6.12 and 7.1 accept the flag. Any other
error from either attempt fails the worker's startup with
`Error::BufferRegistration`, as a registration failure does today
(`ENOMEM` from `RLIMIT_MEMLOCK` on 6.14+ included). If the large group's
registration fails after the small group's succeeded, startup fails the
same way and names the large group.

`ConfigBuilder::recv_incremental(bool)` gates the attempt. With an INC
ring the defaults are 64 × 1 MiB, large group off. With a plain ring they
are 4096 × 64 KiB plus 256 × 1 MiB, large group on. From step 7,
`recv_incremental(false)` selects the plain row on any kernel, which lets
CI cover the plain path on a kernel that supports INC; before step 7 it
selects today's 256 × 16 KiB ring with no large group, and it is the
default.

The incremental-buffer code had fixes in 6.12 stable releases. Which 6.12.y
release first has all the fixes this design relies on is not known. Step 0
either names a minimum 6.12.y release and checks the running kernel against
it, or keeps a runtime probe of the behaviour table on a one-entry ring.

Ubuntu's 6.8 kernels from 6.8.0-139 reject every provided-ring registration
whose reserved words are zero, the form other kernels require, and accept
one with `resv[0]` set (#626). PR #627 makes `Ring::register_buf_ring`
retry that way only after `EINVAL` on a 6.8 kernel; until it lands,
ringline's io_uring backend does not start on these kernels. On them, ring
selection sees `EINVAL` from the INC attempt in both forms and from the
plain attempt's standard form before the plain retry succeeds. That
sequence is untested. Measurements of this design on 6.8 are pending a
rerun with #627.

## Kernel behaviour relied on

| Behaviour | Result |
|---|---|
| Multishot `RECV` on an INC ring | Successive completions append into one buffer at increasing offsets. Each carries `F_BUFFER` and the bid, and `F_BUF_MORE` while the buffer has room left. |
| The completion that uses up a buffer | Clears `F_BUF_MORE`. The kernel does not write into that buffer again until it is posted again. |
| Data offset | Not in the CQE. Completions for one buffer are reaped in the order the kernel filled it, across every connection sharing it. |
| The ring entry | Rewritten by the kernel as it consumes (`addr` advances, `len` shrinks). |
| More data queued | A multishot receive completion carries `IORING_CQE_F_SOCK_NONEMPTY` when the socket still has data after it. On Debian 12's 6.1.0-53 every full completion of a streaming connection carried it; no request/response completion that filled its 64 KiB buffer did. |
| No buffer left | `-ENOBUFS` without `F_MORE`, ending the arm. |
| `RLIMIT_MEMLOCK` (6.14+) | Charged for each ring's entry array, not the buffers: 16 bytes per entry, at least a page per ring. |

The offset order was checked byte for byte on Linux 6.12 and 7.1 with a
64-entry CQ and 32-entry SQ, under SQPOLL, with held lends and at 10,000
connections (journal). It assumes INC-ring receives are not punted to
io-wq: the driver sets no `IOSQE_ASYNC` on them.

Step 0's conformance tests cover each row, a forced-async receive, the
behaviour at EOF on a partly used buffer, and multishot `RECVMSG` (the
`timestamps` feature) on an INC ring, which have not been probed. CI's
Linux runners (`ubuntu-latest`, 6.17) cover one kernel; the tests also run
as SystemsLab experiments on 6.1 (Debian 12), 6.8 (Ubuntu 24.04), 6.12
(Debian 13) and 7.1 (Debian 13 backports).

## Buffer state

Each buffer, identified by its group and bid, has in the driver:

- `written`: bytes the kernel has written since the buffer was last posted;
- `exhausted`: the kernel is done with it;
- `holds`: the number of live lends into it.

The rules:

1. Every completion that carries `F_BUFFER` updates its buffer first, before
   the connection's identity is checked: it advances `written` by `res` and,
   if `F_BUF_MORE` is clear, sets `exhausted`. The group is read from the
   completion's tag, not from the connection, since a completion can arrive
   after its connection moved groups. The stale-CQE early return does this,
   returns the buffer if rule 3 allows it, and touches no connection state.
2. The completion's data is `[base + written_before, base + written)`.
3. A buffer returns to its group's ring when it is `exhausted` and `holds` is
   zero, and only then. It is posted whole (`addr = base`,
   `len = buffer_size`), and `written` resets to zero.
4. A path that keeps bytes past the completion handler takes a hold and
   drops it when it is done. The copy paths (the accumulator copy, TLS, recv
   sinks, `ForceCopy`, timestamps) take none.
5. On a plain ring every completion exhausts its buffer, at offset 0. One
   code path serves both ring kinds.

Rules 3 and 4 replace the per-path "exactly one replenish per bid" checks
(`docs/segmented-recv-design.md`) with one count per buffer and one return
site. A runtime check guards rule 1: when a completion clears `F_BUF_MORE`,
`written` must equal `buffer_size`, and while it is set, be less; a mismatch
is a bug and fails loudly.

A buffer counts as out of the ring when it is exhausted and not yet
returned; a partly filled buffer is still in the ring. A group's `free()` is
then its `ring_size` minus its buffers out, as of the last reaped
completion. It can overstate the kernel's view. That costs an extra
`ENOBUFS`: the connection parks, and since re-arming is driven by
replenishment, it re-arms when a buffer returns.

### Changes in the driver

These read a provided buffer at its base, release by bid, or count
buffers; each now also names the group:

- A multishot receive's 32-bit payload is the connection's full generation
  (`ring.rs` `submit_multishot_recv`), so there is no spare bit for the
  group. A receive armed on the large group uses a second tag,
  `OpTag::RecvMultiLarge`, handled like `RecvMulti` with the large group's
  buffers. Generation checks are unchanged. The connection records which
  tag its live receive uses.
- Every site that builds a `RecvMulti` user_data to cancel the live receive
  uses the live receive's tag: `DriverCtx::cancel` (`handler.rs`, which
  matches on `RecvArm` and gains a variant for the large group), the
  close-time cancel (`driver.rs`), and in `event_loop.rs` the Mode A
  hold-cap throttle and `begin_park`'s linked cancel. A cancel with the
  wrong tag matches nothing.
- `Ring` holds one bgid (`ring.rs`), which `submit_multishot_recv` uses for
  every arm. The arm takes the target group per call.
- `OpTag::SendRecvBuf`'s payload carries the bid in its low 16 bits and
  `SEND_RECV_BUF_REMAINDER` at bit 16 (`completion.rs`); bit 17 carries the
  group. The payload is built in `driver.rs`, `io.rs` and `event_loop.rs`.
- `RecvMsgMultiTs` (the `timestamps` feature) has no large-group tag.
  Timestamped connections are not promoted.
- The completion handlers (`event_loop.rs` `handle_recv_multi`,
  `handle_recv_msg_multi_ts`) read from the base.
- `HeldRecvBuf::Pinned { bid, len }` (`driver.rs`), `SegBacking::Pinned` and
  `SegSettle::Pinned` (`runtime/io.rs`) gain `group` and `off`. Their readers
  re-derive the base: `driver.rs` forward-write iovecs, the Mode A split,
  `advance_forward` and `settle_forward_end`; `io.rs` segment readers and the
  `with_segments` remainder.
- `PendingRecvBuf`'s pointer, today the buffer's base, becomes the data's
  address. `PendingRecvBuf` (backing `pending_recv_bufs` and `recv_hold`)
  gains `group`; a migrating connection can hold entries from both groups.
- The send slab's recv-forward entries record a group per bid (`bids`
  becomes `(group, bid)` pairs), read by `recv_forward_bids` at the
  `SendRecvBufsCoalesced` completion.
- `copy_out_bid` (park) takes the group and offset, or the data pointer,
  and copies from the data's address; today it reads from the base.
- `handle_send_recv_buf` resubmits a partial send from
  `base + (original_len - remaining)`. Its user_data carries the bid, the
  remainder bit and the group bit. A per-connection `send_recv_buf_ptr`,
  next to `send_recv_buf_original_lens`, holds the original data address.
- Every `pending_replenish.push` of a TCP bid becomes a hold release. These
  include the `segment_pinned` single-release check, the recv-forward slab's
  one-bid-per-iovec replenish, `release_queued_sends`, the Mode A
  forward-write completion and `fail_forward_write`, `start_forward_write`'s
  error paths, the close and park drains, and the `pending_recv_bufs`
  flushes (`io.rs` `with_data`, `with_bytes`, segmented entry,
  `with_segments`, direct-echo arm; `stream.rs`; the starved and accumulator
  flushes in `event_loop.rs`).
- `on_handout` and `free()` count buffers out of each group's ring as
  defined above.
- `MAX_FORWARD_IOV` (16), `FORWARD_HELD_MAX_BUFFERS` (32) and the send
  slab's `MAX_IOVECS` (32) bound ranges per call; one gathered write can hold
  that many shared 1 MiB buffers.
- `replenish_batch`'s `debug_assert` is count-based and cannot see one bid
  returned twice; the runtime check above and rule 3's single return site
  replace it.

The UDP ring shares the `ProvidedBufRing` type and stays plain; under rule 5
it needs no special case.

## Bounded accumulator

When a connection's parser last returned `ParseResult::NeedAtLeast(n)`, a
completion copies into the accumulator only the bytes that complete that
message, parses it, and delivers the rest of the completion in place as if
nothing were buffered. With `NeedMore` (no announced length) the completion
is copied whole, as today. The accumulator then holds at most one message
plus the bytes of one completion, whatever the buffer size.

In the benchmark's streaming cells it cut process RSS from 2.4 GB to 262 MiB
and bytes copied sevenfold, at 10–58% more throughput. The benchmark parses
in the completion handler; ringline parses in the connection's task, so the
gain for ringline is inferred, not measured.

## The large group

### Promotion

A connection's next arm targets the large group when either holds:

- four consecutive completions were each at least `promote_bytes` (default
  64 KiB) and each carried `IORING_CQE_F_SOCK_NONEMPTY`;
- a lend from this connection is still held when its task next parks.
  Every in-place delivery is a lend held past the completion handler, so
  the test is at the park, not after the handler.

The rule uses a byte threshold rather than "filled its buffer": under INC a
completion clears `F_BUF_MORE` whenever it reaches the end of a shared
buffer, at any size, so "filled" does not identify a stream there.

The `SOCK_NONEMPTY` condition separates a stream from request/response
messages that reach the threshold. In the two-host 6.1 run, 64 KiB
request/response at 1000 connections produced 102,678–104,001 completions
of 64 KiB per run, none flagged, and promoted no connection. On 6.12 that
cell also promoted no connection, but the benchmark did not count
completions of at least 64 KiB there, so how often the threshold was
reached is unknown. Requests larger than `promote_bytes` (256 KiB or 1 MiB
requests) can satisfy it and promote a request/response connection: with
heavy-tailed request sizes on 6.1, about 290 of 1000 connections were in
the large group at the end of a run, at the same p50 and p99; p999 in the
tiered heavy-tailed cell rose from 5.2 to 6.6 ms in all three reps, with
454 connections in the large group.

Promotion on a held lend was measured only as a static rule: in the
benchmark a fixed set of connections held every range and were promoted on
their first hold. The runtime rule above is unmeasured.

There is no cap on promoted connections. A cap of 64 kept slow handlers and
streamers in the small group, and every cell that needed more than 64
promotions was worse with it than without: on 6.1, streaming throughput
1137 against 1328 MB/s, and request p99 with slow handlers and streamers
570 against 117 ms.

### Migration

A connection whose target differs from its live receive's group cancels
that receive (`ASYNC_CANCEL` by its user_data, with the live receive's
tag). Its completions, up to the final one without `F_MORE`, are handled
under the old group's buffer state (rule 1). The final completion re-arms
the connection on its target group. A receive that ended on its own
(`ENOBUFS`, or `F_MORE` clear) re-arms on the target without a cancel.
Generation checks are unchanged.

### Demotion

A promoted connection returns to the small group after a run of
completions below `promote_bytes`. A connection holding a lend is not
demoted while it holds. In the benchmark the run was 64 completions, and
connections moved back and forth: on 6.1, 1 MiB request/ack connections
were promoted and demoted about 600 times per eight-second run, and on 6.12
streaming produced 58–91 demotions. Demotion after a quiet period, and the
time each migration takes, are being measured; landing step 5 settles the
rule.

## Lends

A lend (`pending_recv_bufs`, `forward_recv_buf`, recv-forward, direct echo,
segments, `forward_to` Mode A) holds a range of a buffer until its task polls
or its send completes. Under INC the buffer is shared, so a 200-byte lend
keeps a whole 1 MiB buffer out of the ring. A connection whose lend is held
when its task parks is promoted, so its later lends pin large-group
buffers.

- Per group, a lend is taken only while fewer than half that group's
  buffers are held. Plaintext lends in `pending_recv_bufs` are otherwise
  uncapped, and connections whose tasks do not poll could hold every
  buffer, leaving a parked connection with nothing to re-arm into. The
  two-ring runs set no lend cap (`--lend-cap` 1.0), so the half-the-group
  cap is unmeasured.
- Above the cap, each path copies instead:
  - `pending_recv_bufs`: into the accumulator;
  - segments and Mode A: into `HeldRecvBuf::Owned`, as the `ForceCopy`
    decision does;
  - recv-forward and direct echo, which deliver only from `recv_hold`: into a
    new owned variant of the `recv_hold` entry (a `SendCopyPool` slot).
- Per connection, `forward_hold_cap` still counts held ranges; one forwarder
  can pin up to that many shared buffers, which the per-group cap bounds.
- With INC on, `recv_segment_reserve` is ignored and the per-group cap
  governs; its default (64 free buffers) would otherwise force a copy on
  every segmented and Mode A delivery from a 64-buffer ring. It is removed
  in step 7, which removes a public `ConfigBuilder` method.
- Lends are sent with plain `send`, `sendmsg` and `writev`, never zero-copy,
  so a hold is released at the send's completion. A lend sent zero-copy
  would have to hold until the notification.

## Starvation and the fallback recv

A group can be used up: 10,000 connections × 64 KiB in flight is 625 MiB.
The `ENOBUFS` park and the fallback one-shot recv (#274) stay, per group.

Measured, per run:

- On 6.1 and 6.12, plain rings return `ENOBUFS` at 10,000 connections,
  falling with depth (Debian 12 6.1, 256 B messages: 12.9M for
  256 × 16 KiB, 4.3M for 1024 × 64 KiB, 0.83M for 4096 × 64 KiB).
- INC 64 × 1 MiB returned none at 256 B × 10,000 on 6.12, but 2.3M at
  64 KiB × 10,000 (hv01).
- Streaming at 64 connections used up the 64 MiB INC ring in every run on
  hv01 (about 30–42k).
- On 6.12 with two groups, a median of 88 of 1000 streaming connections
  ended in the large group, and the small group still returned 122k.
- On 7.1 no ring tested, plain or INC, returned any at 10,000 or 50,000
  connections.

The fallback chunk today is `max(4 × buffer_size, 1 MiB)`, and its
arbitration prefers the fallback over a re-arm on the premise that the
chunk exceeds the ring's capacity (`event_loop.rs`
`flush_replenish_and_rearm`). That premise does not hold for a 64 MiB ring.
Step 3 sets the chunk to 1 MiB and re-arms when the group's
`free() × buffer_size` exceeds it. The benchmark used the 4 MiB chunk, so
step 6 measures this.

When a group is empty the multishot ends, the bytes stay in the socket's
receive queue, and TCP closes the window until the worker re-arms.

## Sizing

- Per worker: 64 MiB on 6.12+ (128 MiB with the large group), 512 MiB
  before 6.12. Only touched pages are resident: the plain groups use
  `MADV_NOHUGEPAGE`, and with transparent huge pages a completion would
  otherwise make its whole 2 MiB region resident.
- A group covers the bytes that arrive while a worker is not reaping:
  ingress rate into that worker × the longest stall to absorb before TCP
  backpressure starts. At 100 Gbit/s (12.5 GB/s) into one worker, 64 MiB is
  about 5 ms; a 10 ms stall needs about 128 MiB.
- Before 6.12 the small group's 4096 buffers are for depth: of the plain
  geometries measured on 6.1, it had the fewest `ENOBUFS` and the most
  throughput at 10,000 connections. The large group's 256 buffers are the
  only large-group size measured.
- When `prefault_buffers` is set (off by default), it touches each group's
  memory at startup.
- `RLIMIT_MEMLOCK` (6.14+) is charged only for entry arrays. The launch
  preflight (`worker.rs` `memlock_required`, through `memlock.rs`) counts
  both groups' entries: 4096 + 256 for plain rings, 64 + 64 for INC.
- `recv_accumulator_max` must be at least the larger of 1 MiB and every
  configured buffer size (`recv_buffer`, `recv_large_buffer`), since the
  geometry is chosen at runtime; `build()` rejects a smaller value. Today's
  check (`config.rs`) covers only `recv_buffer`. That is a breaking change,
  made in step 7.
- `recv_buffer(ring_size, buffer_size)` still sets the small group's
  geometry, which is then used as given on either kind of ring.
  `RecvBufferConfig` gains a flag recording that `recv_buffer` was called;
  `recv_buffer_bgid` does not set it.
- `ConfigBuilder::recv_large_group(bool)` turns the large group on or off
  (default per ring kind, as in the first table, from step 7);
  `recv_large_buffer(ring_size, buffer_size)` sets its geometry; and
  `recv_large_buffer_bgid(u16)` its buffer group id, default 2. `build()`
  rejects a large-group bgid equal to the TCP bgid (default 0), or the UDP
  bgid (default 1) when UDP is in use.

## Unchanged

- The UDP ring (`udp_recv_buffer`, its own bgid) stays plain.
- `with_data` and `with_bytes` read the accumulator.
- TLS (both engines) copies out of the ring and takes no hold.
- The `timestamps` feature shares the small TCP group. If multishot
  `RECVMSG` works on an INC ring, it stays there, with `RecvMsgOut::parse`
  reading at the completion's offset; otherwise timestamped connections get
  their own plain ring.
- mio. If the ring emulator (#621) lands, it implements this state machine.

## Metrics

Kept: `buffer_ring_empty`, `recv_parked`, `recv_fallback`,
`forward_throttled`. New, per group where it applies: buffers out of the
ring, buffers held by lends, lends refused by the lend cap, `ENOBUFS`,
promotions, demotions, connections in the large group, and the ring kind in
use.

## Not measured

- Anything in ringline itself: every number is from the `recv-strategies`
  benchmark, including the bounded accumulator's gain.
- Two groups on 7.1.
- Kernels 6.2–6.11; 6.8 is pending #627.
- Large-group sizes other than 256 buffers of 1 MiB before 6.12.
- More than one worker.
- The runtime hold-promotion rule (only a static per-connection rule ran).
- The half-the-group lend cap.
- The 1 MiB fallback chunk with the free-space re-arm.
- The demotion rule and the time a migration takes.
- Multishot `RECVMSG` on an INC ring.

## Landing

0. Probe and conformance tests: each row of the behaviour table, the
   byte-verified offset order (a 64-entry CQ and 32-entry SQ, SQPOLL, a
   forced-async receive), EOF on a partly used buffer, and multishot
   `RECVMSG` on an INC ring; run on CI and as SystemsLab experiments on
   6.1, 6.8, 6.12 and 7.1. Settle the 6.12.y minimum or keep a probe.
1. Bounded accumulator: copy only what completes a `NeedAtLeast` message.
   Independent of the ring changes.
2. Buffer state (`written`, `exhausted`, `holds`) with the ring registered
   plain. Every completion exhausts its buffer at offset 0, so behaviour is
   unchanged; the per-path single-release checks become holds. The existing
   lifecycle tests (double replenish, leaks, close drain, the Mode A hold
   cap) pass unchanged. Steps 2 and 3 can be one change if that is simpler.
3. Offsets: the `off` fields, data at `base + written`,
   `send_recv_buf_ptr`, `copy_out_bid` and `settle_forward_end`. Still a
   plain ring, where every offset is 0, so these paths are exercised only
   from step 4.
4. INC and the plain geometry, behind `recv_incremental` (default `false`):
   ring-kind selection, `MADV_NOHUGEPAGE` and the 4096 × 64 KiB plain
   geometry, the per-group lend cap and its copy paths,
   `recv_segment_reserve` ignored under INC, the fallback arbitration, the
   memlock preflight, and the per-group metrics.
5. The large group, behind `recv_large_group` (default `false`): its bgid
   and validation, the arm taking the group per call,
   `OpTag::RecvMultiLarge` and the `SendRecvBuf` group bit, `group` in
   `PendingRecvBuf` and the send slab, the cancel sites, promotion,
   migration and demotion, and the two-group memlock preflight. Demotion's rule is settled here, with migration
   timed.
6. Measure ringline on hv01 and across hv01/hv02, on Linux 6.1, 6.8, 6.12
   and 7.1: the geometry per ring kind with and without the large group
   against the 256 × 16 KiB ring, with the bench suite (echo at 256 B to
   1 MiB, mixed and heavy-tailed sizes, the #415 forward proxy, streaming,
   slow handlers alongside request/response, 64 to 10,000 connections).
   Record throughput, RSS, and p50/p99/p999 at fixed offered load (an
   open-loop client measuring from each message's scheduled time, with
   streamers held to a fixed rate), five reps or more per cell with an A/A
   pair on each setup; a difference counts when it exceeds the A/A spread.
   Gate: no cell slower, or worse at p99, than the 256 × 16 KiB ring beyond
   that spread, and no sustained `buffer_ring_empty`. A cell that fails is
   resolved before step 7, by geometry for that kernel, by leaving the
   large group off there, or by keeping the 256 × 16 KiB ring there. This
   step decides the large group's default on 6.12+.
7. New defaults (the geometry per ring kind, `recv_incremental` on, the
   large group's default per ring kind, the `recv_accumulator_max` floor,
   the removal of `recv_segment_reserve`) in a coordinated release.

Steps 1 to 3 change no ring behaviour and can land before INC is switched
on.
