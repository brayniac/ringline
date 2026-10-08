# Two shared receive rings per worker, consumed incrementally where supported

Status: proposal. Nothing here is implemented. The measurements behind it,
and the alternatives that were measured and not chosen, are in
`docs/journal/2026-09-incremental-buffer-consumption.md`, sections
"2026-10: measurements" and "2026-10: kernels, two rings, tiered caches".

Related: `docs/segmented-recv-design.md` (the bid lifecycle of the lending
paths), `docs/journal/2026-07-enobufs-fallback-recv.md` (#274, the fallback
recv).

## Goal

A TCP receive on io_uring lands in a buffer from one provided-buffer ring
shared by the worker's connections, 256 buffers of 16 KiB before this change
(`config.rs` `RecvBufferConfig::default`). Buffer size sets how much one
completion can deliver; at a fixed memory budget, larger buffers mean fewer
of them. Messages larger than a buffer pay a completion per buffer, and a
connection whose task holds its bytes keeps buffers every other connection
needs.

This design gives each worker two shared TCP rings, registered as two buffer
groups, with geometry chosen by kernel:

| Kernel | Small group (every connection starts here) | Large group (promoted connections) | Ring memory per worker |
|---|---|---|---|
| Linux 6.12 and later | `IOU_PBUF_RING_INC`, 64 buffers of 1 MiB | `IOU_PBUF_RING_INC`, 64 buffers of 1 MiB | 128 MiB |
| Linux 6.1–6.11 | plain, 4096 buffers of 64 KiB | plain, 256 buffers of 1 MiB | 512 MiB |

With `IOU_PBUF_RING_INC` one buffer is consumed incrementally by successive
completions, from any connection, and returns to the ring only when it is
used up, so a large buffer also serves small arrivals. Plain rings are
backed with `MADV_NOHUGEPAGE` so a completion makes resident only the 4 KiB
pages it writes.

A connection moves to the large group when it streams (its completions fill
their buffers with more data queued behind them) or when it holds a lend;
see "The large group". Request/response connections stay in the small
group, so a streaming or slow connection cannot use up the buffers they
need.

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
- Ring memory per worker rises from 4 MiB to 128 MiB (6.12+) or 512 MiB
  (before 6.12). Each group's touched pages stay resident. In the benchmark,
  peak process RSS was 71 MB with one INC group and 117–135 MB with two
  (6.12), and 282 MB with the plain 4096 × 64 KiB group alone against
  about 580 MB with both plain groups (6.1).
- A promotion cancels the connection's live receive and re-arms it on the
  other group. On 6.12, two cells with streamers had a higher p999 with two
  groups than with one (37.8 ms against 54.5 and 56.6 ms) while p50 and p99
  fell; whether migration causes it is being measured (see "Demotion").
- At equal offered load with one group, latency matched the 256 × 16 KiB
  ring's or was lower, except p999 at 4 KiB messages near capacity
  (1000 connections): 2.4 ms against 1.8 ms on hv01, 3.3 ms against 2.8 ms
  across hosts.
- The ring emulator (#621), if it lands, must reproduce incremental shared
  buffers and two groups.

## Selecting the ring kind

At startup the worker registers the small group with `IOU_PBUF_RING_INC`.
`EINVAL` selects plain rings for both groups; Linux 6.1 (Debian 12 and
Amazon Linux 2023) returns it, and 6.12 and 7.1 accept the flag. Step 0's
conformance tests check the behaviour table below on each kernel in CI.

Ubuntu's 6.8 kernels from 6.8.0-139 reject every provided-ring registration
whose reserved words are zero, the form other kernels require, and accept
one with `resv[0]` set (#626). `Ring::register_buf_ring` retries that way
only after `EINVAL` on a 6.8 kernel (#627). Measurements of this design on
6.8 are pending a rerun with that fix.

## Kernel behaviour relied on

| Behaviour | Result |
|---|---|
| Multishot `RECV` on an INC ring | Successive completions append into one buffer at increasing offsets. Each carries `F_BUFFER` and the bid, and `F_BUF_MORE` while the buffer has room left. |
| The completion that uses up a buffer | Clears `F_BUF_MORE`. The kernel does not write into that buffer again until it is posted again. |
| Data offset | Not in the CQE. Completions for one buffer are reaped in the order the kernel filled it, across every connection sharing it. |
| The ring entry | Rewritten by the kernel as it consumes (`addr` advances, `len` shrinks). |
| More data queued | A receive completion carries `IORING_CQE_F_SOCK_NONEMPTY` when the socket still has data after it. A 64 KiB request that fills a 64 KiB buffer exactly does not set it; a streamer's full completions do. |
| No buffer left | `-ENOBUFS` without `F_MORE`, ending the arm. |
| Plain rings under many connections | On 6.1 and 6.12, plain rings return `ENOBUFS` at 10,000 connections, falling with depth (Debian 12 6.1, 256 B messages, per run: 12.9M for 256 × 16 KiB, 4.3M for 1024 × 64 KiB, 0.83M for 4096 × 64 KiB). On 7.1 no ring tested returned any at 10,000 or 50,000 connections. |
| `RLIMIT_MEMLOCK` (6.14+) | Charged for each ring's entry array, not the buffers: 16 bytes per entry, at least a page per ring. |

The offset order was checked byte for byte on Linux 6.12 and 7.1 with a
64-entry CQ and 32-entry SQ, under SQPOLL, with held lends and at 10,000
connections (journal). It assumes INC-ring receives are not punted to
io-wq: the driver sets no `IOSQE_ASYNC` on them.

Step 0's conformance tests cover each row, a forced-async receive, the
behaviour at EOF on a partly used buffer, and multishot `RECVMSG` (the
`timestamps` feature) on an INC ring, which have not been probed.

## Buffer state

Each buffer, identified by its group and bid, has in the driver:

- `written`: bytes the kernel has written since the buffer was last posted;
- `exhausted`: the kernel is done with it;
- `holds`: the number of live lends into it.

The rules:

1. Every completion that carries `F_BUFFER` updates its buffer first, before
   the connection's identity is checked: it advances `written` by `res` and,
   if `F_BUF_MORE` is clear, sets `exhausted`. The group is read from the
   completion's user_data, not from the connection, since a completion can
   arrive after its connection moved groups. The stale-CQE early return does
   this, returns the buffer if rule 3 allows it, and touches no connection
   state.
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
completion. It can overstate the kernel's view, which costs an extra
`ENOBUFS`, never a hang.

### Changes in the driver

These read a provided buffer at its base, release by bid, or count
buffers; each now also names the group:

- The completion handlers (`event_loop.rs` `handle_recv_multi`,
  `handle_recv_msg_multi_ts`) read from the base.
- The multishot receive's user_data carries the group, and the receive is
  armed with that group's bgid. The connection records the group its live
  receive uses and the group its next arm uses.
- `HeldRecvBuf::Pinned { bid, len }` (`driver.rs`), `SegBacking::Pinned` and
  `SegSettle::Pinned` (`runtime/io.rs`) gain `group` and `off`. Their readers
  re-derive the base: `driver.rs` forward-write iovecs, the Mode A split,
  `advance_forward` and `settle_forward_end`; `io.rs` segment readers and the
  `with_segments` remainder.
- `PendingRecvBuf` carries a pointer, set to the base; it is set to the
  data's address. `copy_out_bid` (park) reads from the base and ignores it.
- `handle_send_recv_buf` resubmits a partial send from
  `base + (original_len - remaining)`, and the user_data carries only the
  bid. A per-connection `send_recv_buf_ptr`, next to
  `send_recv_buf_original_lens`, holds the original data address.
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
is copied whole, as before this change. The accumulator then holds at most
one message plus the bytes of one completion, whatever the buffer size.

In the benchmark's streaming cells it cut process RSS from 2.4 GB to 262 MB
and bytes copied sevenfold, at 10–58% more throughput.

## The large group

### Promotion

A connection's next arm targets the large group when either holds:

- four consecutive completions each filled its buffer and carried
  `IORING_CQE_F_SOCK_NONEMPTY`;
- it took a lend that is still held when its handler returns.

The `SOCK_NONEMPTY` condition separates a stream from request/response
messages that happen to fill a buffer. In the two-host runs, 64 KiB
request/response at 1000 connections promoted no connection on 6.1 or 6.12;
in a local check, none of its 739k full completions carried the flag, while
every full completion of a streaming connection did. Requests larger than a
small-group buffer (a 256 KiB or 1 MiB set) can satisfy it and promote a
request/response connection: with heavy-tailed request sizes on 6.1, about
290 of 1000 connections were in the large group at the end of a run, with
no measured cost.

There is no cap on promoted connections. A cap of 64 kept slow handlers and
streamers in the small group, and every cell that needed more than 64
promotions was worse with it than without: on 6.1, streaming throughput
1137 against 1328 MB/s, and request p99 with slow handlers and streamers
570 against 117 ms.

### Migration

A connection whose target differs from its live receive's group cancels
that receive (`ASYNC_CANCEL` by its user_data). Its completions, up to the
final one without `F_MORE`, are handled under the old group's buffer state
(rule 1). The final completion re-arms the connection on its target group.
A receive that ended on its own (`ENOBUFS`, or `F_MORE` clear) re-arms on
the target without a cancel. Generation checks are unchanged.

### Demotion

A promoted connection returns to the small group after a run of
completions that do not fill their buffer. In the benchmark the run was 64
completions, and connections moved back and forth: on 6.1, 1 MiB
request/ack connections were promoted and demoted about 600 times per
eight-second run, and on 6.12 streaming produced 58–91 demotions. On 6.12,
two cells with streamers had a higher p999 with two groups (37.8 ms against
54.5 and 56.6 ms). Demotion after a quiet period, and the time each
migration takes, are being measured; step 4 settles the rule.

## Lends

A lend (`pending_recv_bufs`, `forward_recv_buf`, recv-forward, direct echo,
segments, `forward_to` Mode A) holds a range of a buffer until its task polls
or its send completes. Under INC the buffer is shared, so a 200-byte lend
keeps a whole 1 MiB buffer out of the ring. A connection that holds a lend
is promoted, so its later lends pin large-group buffers.

- Per group, a lend is taken only while fewer than half that group's
  buffers are held. Plaintext lends in `pending_recv_bufs` are otherwise
  uncapped, and connections whose tasks do not poll could hold every
  buffer, leaving a parked connection with nothing to re-arm into.
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
  in step 6, which removes a public `ConfigBuilder` method.
- Lends are sent with plain `send`, `sendmsg` and `writev`, never zero-copy,
  so a hold is released at the send's completion. A lend sent zero-copy
  would have to hold until the notification.

## Starvation and the fallback recv

A group can be used up: 10,000 connections × 64 KiB in flight is 625 MiB.
In the benchmark, streaming at 64 connections used up the 64 MiB INC ring in
every run on hv01 (about 30–42k `ENOBUFS` per run), and plain rings on 6.1
and 6.12 returned up to millions per run at 10,000 connections. The
`ENOBUFS` park and the fallback one-shot recv (#274) stay, per group.

The fallback chunk is `max(4 × buffer_size, 1 MiB)` before this change, and
its arbitration prefers the fallback over a re-arm on the premise that the
chunk exceeds the ring's capacity (`event_loop.rs`
`flush_replenish_and_rearm`). That premise does not hold for a 64 MiB ring.
Step 3 sets the chunk to 1 MiB and re-arms when the group's
`free() × buffer_size` exceeds it. The benchmark used the 4 MiB chunk, so
step 4 measures this.

An empty ring is backpressure, not loss: the multishot ends, the bytes stay
in the socket's receive queue, and TCP closes the window until the worker
re-arms.

## Sizing

- 128 MiB per worker on 6.12+ (64 MiB per group), 512 MiB before 6.12
  (256 MiB per group). Only touched pages are resident: the plain groups
  use `MADV_NOHUGEPAGE`, and with transparent huge pages a completion would
  otherwise make its whole 2 MiB region resident.
- A group covers the bytes that arrive while a worker is not reaping:
  ingress rate per worker × the longest stall to absorb before TCP
  backpressure starts. At 100 Gbit/s (12.5 GB/s) into one worker, 64 MiB is
  about 5 ms. A 10 ms stall needs about 120 MiB, 128 MiB as a power of two.
  Spread across N workers, each needs about a 1/N share.
- When `prefault_buffers` is set (off by default), it touches each group's
  memory at startup.
- `RLIMIT_MEMLOCK` (6.14+) is charged only for entry arrays. The launch
  preflight (`worker.rs` `memlock_required`, through `memlock.rs`) counts
  both groups' entries: 4096 + 256 entries for plain rings, 64 + 64 for INC.
- `recv_accumulator_max` must be at least the largest buffer size. `build()`
  validates it against 1 MiB, since the geometry is chosen at runtime; a
  value below 1 MiB is rejected. That is a breaking change, made in step 6.
- `recv_buffer(ring_size, buffer_size)` still sets the small group's
  geometry, which is then used as given on either kind of ring.
  `RecvBufferConfig` gains a flag recording that `recv_buffer` was called;
  `recv_buffer_bgid` does not set it. A new builder method sets the large
  group's geometry, or disables it.

## Unchanged

- The UDP ring (`udp_recv_buffer`, its own bgid) stays plain.
- `with_data` and `with_bytes` read the accumulator.
- TLS (both engines) copies out of the ring and takes no hold.
- The `timestamps` feature shares the TCP groups. If multishot `RECVMSG`
  works on an INC ring, it stays there, with `RecvMsgOut::parse` reading at
  the completion's offset; otherwise timestamped connections get their own
  plain ring.
- mio. If the ring emulator (#621) lands, it implements this state machine.

## Metrics

Kept: `buffer_ring_empty`, `recv_parked`, `recv_fallback`,
`forward_throttled`. New, per group where it applies: buffers out of the
ring, buffers held by lends, lends refused by the lend cap, `ENOBUFS`,
promotions, demotions, connections in the large group, and the ring kind in
use.

## Landing

0. Probe and conformance tests: each row of the behaviour table, the
   byte-verified offset order (a 64-entry CQ and 32-entry SQ, SQPOLL, a
   forced-async receive), EOF on a partly used buffer, and multishot
   `RECVMSG` on an INC ring.
1. Buffer state (`written`, `exhausted`, `holds`) with the ring registered
   plain. Every completion exhausts its buffer at offset 0, so behaviour is
   unchanged; the per-path single-release checks become holds. The existing
   lifecycle tests (double replenish, leaks, close drain, the Mode A hold
   cap) pass unchanged.
2. Offsets: the `off` fields, data at `base + written`,
   `send_recv_buf_ptr`, `copy_out_bid` and `settle_forward_end`. Still a
   plain ring.
3. INC, off by default behind a switch: ring-kind selection, the per-group
   lend cap and its copy paths, `recv_segment_reserve` ignored under INC,
   the fallback arbitration, and the bounded accumulator.
4. The large group, off by default behind a switch: the group in user_data
   and in buffer state, promotion, migration and demotion. Demotion's rule
   is settled here, with migration timed.
5. Measure ringline on hv01 and across hv01/hv02, on Linux 6.1, 6.8, 6.12
   and 7.1: the per-kernel geometry with and without the large group
   against the 256 × 16 KiB ring, with the bench suite (echo at 256 B to
   1 MiB, mixed and heavy-tailed sizes, the #415 forward proxy, streaming,
   slow handlers alongside request/response, 64 to 10,000 connections).
   Record throughput, RSS, and p50/p99/p999 at fixed offered load (an
   open-loop client measuring from each message's scheduled time, with
   streamers held to a fixed rate), five reps or more per cell with an A/A
   pair on each setup; a difference counts when it exceeds the A/A spread.
   Gate: no cell slower, or worse at p99, than the 256 × 16 KiB ring beyond
   that spread, and no sustained `buffer_ring_empty`. A cell that fails is
   resolved before step 6, by geometry for that kernel, by leaving the
   large group off there, or by keeping the 256 × 16 KiB ring there.
6. New defaults (the per-kernel geometry, both switches on, the
   `recv_accumulator_max` floor, the removal of `recv_segment_reserve`) in a
   coordinated release.

Steps 1 and 2 change no behaviour and can land before INC is switched on.
