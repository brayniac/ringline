# One shared receive ring per worker, consumed incrementally

Status: proposal. Nothing here is implemented. The measurements behind it,
and the alternatives that were measured and not chosen, are in
`docs/journal/2026-09-incremental-buffer-consumption.md`, section "2026-10:
measurements".

Related: `docs/segmented-recv-design.md` (the bid lifecycle of the lending
paths), `docs/journal/2026-07-enobufs-fallback-recv.md` (#274, the fallback
recv).

## Goal

A TCP receive on io_uring lands in a buffer from one provided-buffer ring
shared by the worker's connections, 256 buffers of 16 KiB before this change
(`config.rs` `RecvBufferConfig::default`). Buffer size sets how much one
completion can deliver; at a fixed memory budget, larger buffers mean fewer
of them. Messages larger than a buffer pay a completion per buffer.

This design keeps one shared TCP ring per worker and sets its geometry by
kernel:

| Kernel | Ring | Memory per worker |
|---|---|---|
| Linux 6.12 and later | `IOU_PBUF_RING_INC`, 64 buffers of 1 MiB | 64 MiB |
| Linux 6.1–6.11 | plain, 1024 buffers of 64 KiB | 64 MiB |

With `IOU_PBUF_RING_INC` one buffer is consumed incrementally by successive
completions, from any connection, and returns to the ring only when it is
used up, so a large buffer also serves small arrivals.

The copy into the accumulator, the lend-in-place paths and the `ENOBUFS`
fallback stay. Under INC a buffer holds the bytes of several completions at
increasing offsets, and returns to the ring only when the kernel has
finished with it and nothing holds any of its bytes.

## Costs

- The receive copy into the accumulator stays.
- The `ENOBUFS` park, the fallback recv and the starvation arbitration stay,
  and the buffer state below is added.
- Above the per-worker lend cap, arrivals are copied instead of lent.
- `with_bytes` keeps its merge copy when bytes arrive while a task holds
  slices of the remainder.
- Memory per worker rises from 4 MiB to 64 MiB of ring, all of it resident
  once the worker has received 64 MiB. Completions of up to 1 MiB also grow
  accumulators: in the benchmark, process RSS streaming at 1000 connections
  was 737–891 MB with the INC ring against 92–264 MB with the 4 MiB ring.
- At 1 MiB messages over 64 connections on hv01, p99 latency was 13.6 ms
  with the INC ring against 5.0 ms with the 4 MiB ring, at 70% more
  throughput.
- The ring emulator (#621), if it lands, must reproduce incremental shared
  buffers.

## Detecting INC

At startup the worker probes INC behaviour on a separate one-entry ring,
using a buffer group id distinct from the TCP ring's, the UDP ring's and any
user-set one, over a TCP loopback connection:

1. Register the ring with `IOU_PBUF_RING_INC`. An error selects the plain
   ring.
2. Arm a multishot `RECV` on it, with a linked timeout.
3. Send twice, each less than the buffer. Both completions must carry
   `F_BUFFER` and `F_BUF_MORE`, and the second's bytes must follow the
   first's in the buffer.
4. Send enough to use up the rest of the buffer. That completion must clear
   `F_BUF_MORE`.
5. End the arm (cancel, and reap its terminal completion) before
   unregistering the ring.

Any failure or timeout selects the plain ring. Kernels before 6.12 are
expected to reject the flag at step 1, but only 6.12 and 7.1 were probed, so
the choice rests on behaviour, not on the error.

## Kernel behaviour relied on

| Behaviour | Result |
|---|---|
| Multishot `RECV` on an INC ring | Successive completions append into one buffer at increasing offsets. Each carries `F_BUFFER` and the bid, and `F_BUF_MORE` while the buffer has room left. |
| The completion that uses up a buffer | Clears `F_BUF_MORE`. The kernel does not write into that buffer again until it is posted again. |
| Data offset | Not in the CQE. Completions for one buffer are reaped in the order the kernel filled it, across every connection sharing it. |
| The ring entry | Rewritten by the kernel as it consumes (`addr` advances, `len` shrinks). |
| No buffer left | `-ENOBUFS` without `F_MORE`, ending the arm. |
| `RLIMIT_MEMLOCK` (6.14+) | Charged for the ring's entry array, not the buffers: a page for 64 entries, four pages for 1024. |

The offset order was checked byte for byte on Linux 6.12 and 7.1 with a
64-entry CQ and 32-entry SQ, under SQPOLL, with held lends and at 10,000
connections (journal). It assumes INC-ring receives are not punted to
io-wq: the driver sets no `IOSQE_ASYNC` on them.

Step 0's conformance tests cover each row, a forced-async receive, the
behaviour at EOF on a partly used buffer, and multishot `RECVMSG` (the
`timestamps` feature) on an INC ring, which have not been probed.

## Buffer state

Each buffer has, in the driver:

- `written`: bytes the kernel has written since the buffer was last posted;
- `exhausted`: the kernel is done with it;
- `holds`: the number of live lends into it.

The rules:

1. Every completion that carries `F_BUFFER` updates its buffer first, before
   the connection's identity is checked: it advances `written` by `res` and,
   if `F_BUF_MORE` is clear, sets `exhausted`. The stale-CQE early return
   does this, returns the buffer if rule 3 allows it, and touches no
   connection state.
2. The completion's data is `[base + written_before, base + written)`.
3. A buffer returns to the ring when it is `exhausted` and `holds` is zero,
   and only then. It is posted whole (`addr = base`, `len = buffer_size`),
   and `written` resets to zero.
4. A path that keeps bytes past the completion handler takes a hold and
   drops it when it is done. The copy paths (the accumulator copy, TLS, recv
   sinks, `ForceCopy`, timestamps) take none.
5. On a plain ring every completion exhausts its buffer, at offset 0. One
   code path serves both rings.

Rules 3 and 4 replace the per-path "exactly one replenish per bid" checks
(`docs/segmented-recv-design.md`) with one count per buffer and one return
site. A runtime check guards rule 1: when a completion clears `F_BUF_MORE`,
`written` must equal `buffer_size`, and while it is set, be less; a mismatch
is a bug and fails loudly.

A buffer counts as out of the ring when it is exhausted and not yet
returned; a partly filled buffer is still in the ring. `free()` is then
`ring_size` minus the buffers out, as of the last reaped completion. It can
overstate the kernel's view, which costs an extra `ENOBUFS`, never a hang.

### Changes in the driver

These read a provided buffer at its base, release by bid, or count
buffers:

- The completion handlers (`event_loop.rs` `handle_recv_multi`,
  `handle_recv_msg_multi_ts`) read from the base.
- `HeldRecvBuf::Pinned { bid, len }` (`driver.rs`), `SegBacking::Pinned` and
  `SegSettle::Pinned` (`runtime/io.rs`) gain `off`. Their readers re-derive
  the base: `driver.rs` forward-write iovecs, the Mode A split,
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
- `on_handout` and `free()` count buffers out of the ring as defined above.
- `MAX_FORWARD_IOV` (16), `FORWARD_HELD_MAX_BUFFERS` (32) and the send
  slab's `MAX_IOVECS` (32) bound ranges per call; one gathered write can hold
  that many shared 1 MiB buffers.
- `replenish_batch`'s `debug_assert` is count-based and cannot see one bid
  returned twice; the runtime check above and rule 3's single return site
  replace it.

The UDP ring shares the `ProvidedBufRing` type and stays plain; under rule 5
it needs no special case.

## Lends

A lend (`pending_recv_bufs`, `forward_recv_buf`, recv-forward, direct echo,
segments, `forward_to` Mode A) holds a range of a buffer until its task polls
or its send completes. Under INC the buffer is shared, so a 200-byte lend
keeps a whole 1 MiB buffer out of the ring.

- Per worker, a lend is taken only while fewer than half the ring's buffers
  are held. Plaintext lends in `pending_recv_bufs` are otherwise uncapped,
  and connections whose tasks do not poll could hold every buffer, leaving a
  parked connection with nothing to re-arm into.
- Above the cap, each path copies instead:
  - `pending_recv_bufs`: into the accumulator;
  - segments and Mode A: into `HeldRecvBuf::Owned`, as the `ForceCopy`
    decision does;
  - recv-forward and direct echo, which deliver only from `recv_hold`: into a
    new owned variant of the `recv_hold` entry (a `SendCopyPool` slot).
- Per connection, `forward_hold_cap` still counts held ranges; one forwarder
  can pin up to that many shared buffers, which the per-worker cap bounds.
- With INC on, `recv_segment_reserve` is ignored and the per-worker cap
  governs; its default (64 free buffers) would otherwise force a copy on
  every segmented and Mode A delivery from a 64-buffer ring. It is removed
  in step 5, which removes a public `ConfigBuilder` method.
- Lends are sent with plain `send`, `sendmsg` and `writev`, never zero-copy,
  so a hold is released at the send's completion. A lend sent zero-copy
  would have to hold until the notification.

## Starvation and the fallback recv

The ring can be used up: 10,000 connections × 64 KiB in flight is 625 MiB.
In the benchmark, streaming at 64 connections used up the 64 MiB INC ring in
every run on hv01 (about 30–42k `ENOBUFS` per run). The `ENOBUFS` park and
the fallback one-shot recv (#274) stay.

The fallback chunk is `max(4 × buffer_size, 1 MiB)` before this change, and
its arbitration prefers the fallback over a re-arm on the premise that the
chunk exceeds the ring's capacity (`event_loop.rs`
`flush_replenish_and_rearm`). That premise does not hold for a 64 MiB ring.
Step 3 sets the chunk to 1 MiB and re-arms when `free() × buffer_size`
exceeds it. The benchmark used the 4 MiB chunk, so step 4 measures this.

An empty ring is backpressure, not loss: the multishot ends, the bytes stay
in the socket's receive queue, and TCP closes the window until the worker
re-arms.

## Sizing

- 64 MiB per worker in both ring kinds.
- The ring covers the bytes that arrive while a worker is not reaping:
  ingress rate per worker × the longest stall to absorb before TCP
  backpressure starts. At 100 Gbit/s (12.5 GB/s) into one worker, 64 MiB is
  about 5 ms. A 10 ms stall needs about 120 MiB, 128 MiB as a power of two.
  Spread across N workers, each needs about a 1/N share.
- The backing is a heap allocation, resident once touched, and the ring is
  used in order: a worker that has received 64 MiB has all of it resident,
  64 MiB × workers per host. When `prefault_buffers` is set (off by
  default), it touches all 64 MiB at startup. Otherwise, with transparent
  huge pages, each 2 MiB region's first touch happens on the receive path
  (`docs/journal/2026-09-prefault-measurement.md`).
- `RLIMIT_MEMLOCK` (6.14+) is charged only for entry arrays. The launch
  preflight (`worker.rs` `memlock_required`, through `memlock.rs`) runs
  before the probe; it counts the plain ring's 1024 entries and the probe
  ring's page.
- `recv_accumulator_max` must be at least `buffer_size`. `build()` validates
  it against the largest default buffer, 1 MiB, since the geometry is chosen
  at runtime; a value between 64 KiB and 1 MiB is rejected. That is a
  breaking change, made in step 5.
- `recv_buffer(ring_size, buffer_size)` still sets the geometry, which is
  then used as given on either kind of ring. `RecvBufferConfig` gains a flag
  recording that `recv_buffer` was called; `recv_buffer_bgid` does not set
  it.

## Unchanged

- The UDP ring (`udp_recv_buffer`, its own bgid) stays plain.
- `with_data` and `with_bytes` read the accumulator.
- TLS (both engines) copies out of the ring and takes no hold.
- The `timestamps` feature shares the TCP ring. If multishot `RECVMSG` works
  on an INC ring, it stays there, with `RecvMsgOut::parse` reading at the
  completion's offset; otherwise timestamped connections get their own plain
  ring.
- mio. If the ring emulator (#621) lands, it implements this state machine.

## Metrics

Kept: `buffer_ring_empty`, `recv_parked`, `recv_fallback`,
`forward_throttled`. New: buffers out of the ring, buffers held by lends,
lends refused by the per-worker cap, and the ring kind in use.

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
3. INC, off by default behind a switch: the startup probe, the per-worker
   lend cap and its copy paths, `recv_segment_reserve` ignored under INC,
   and the fallback arbitration.
4. Measure ringline on hv01 and across hv01/hv02, on Linux 6.12, 7.1 and a
   kernel before 6.12: INC 64 × 1 MiB and plain 1024 × 64 KiB against the
   256 × 16 KiB ring, with the bench suite (echo at 256 B to 1 MiB, mixed
   sizes, the #415 forward proxy, streaming, 64 to 10,000 connections).
   Record throughput, p99 and RSS, five reps or more per cell with an A/A
   pair on each setup; a difference counts when it exceeds the A/A spread.
   Gate: no cell slower, or worse at p99, than the 256 × 16 KiB ring beyond
   that spread, and no sustained `buffer_ring_empty`. A cell that fails is
   resolved before step 5, by geometry for that kernel or by keeping the
   256 × 16 KiB ring there.
5. New defaults (the per-kernel geometry, the switch on, the
   `recv_accumulator_max` floor, the removal of `recv_segment_reserve`) in a
   coordinated release.

Steps 1 and 2 change no behaviour and can land before INC is switched on.
