# One shared receive ring per worker, consumed incrementally

Status: proposal, chosen by the owner on 2026-10-07. Nothing here is
implemented. The measurements behind it are in
`docs/journal/2026-09-incremental-buffer-consumption.md`, "2026-10:
measurements".

Related: `docs/segmented-recv-design.md` (the bid lifecycle of the lending
paths), `docs/journal/2026-07-enobufs-fallback-recv.md` (#274, the fallback
recv).

## Goal

A TCP receive on io_uring lands in a buffer from one provided-buffer ring
shared by the worker's connections: 256 buffers of 16 KiB by default
(`config.rs` `RecvBufferConfig::default`). Buffer size sets how much one
completion can deliver, and at a fixed memory budget it trades against how
many buffers the ring holds. Messages larger than a buffer pay a completion
per buffer, and the default is 8–40% behind the best measured geometry on
64 KiB, 1 MiB and mixed-size traffic.

This design keeps one shared TCP ring per worker and changes its geometry
per kernel:

| Kernel | Ring | Memory per worker |
|---|---|---|
| Linux 6.12 and later | `IOU_PBUF_RING_INC`, 64 buffers of 1 MiB | 64 MiB |
| Linux 6.1–6.11 | plain, 1024 buffers of 64 KiB | 64 MiB |

With `IOU_PBUF_RING_INC` (6.12) one buffer is consumed incrementally by
successive completions, from any connection, and returns to the ring only
when it is used up, so large buffers serve small arrivals as well as large
ones. Without it, 64 KiB buffers are the best plain geometry measured.

The copy into the accumulator, the lend-in-place paths and the `ENOBUFS`
fallback stay. What changes is how a buffer is tracked: under INC a buffer
holds the bytes of several completions at increasing offsets, and returns to
the ring only when the kernel has finished with it and nothing holds any of
its bytes.

Per-connection receive memory was measured as the alternative and not
chosen; see "Per-connection receive memory" below.

## Per-connection receive memory

Each connection receives into memory it owns: a one-entry INC ring per
connection over the connection's region, or a one-shot `RECV` into it. It
removes the receive copy and isolates connections from each other's memory.
Measured on hv01 and across hosts (journal), it is not the default because:

- **Moving a posted region in place corrupts data.** Rewriting a live ring
  entry to move, grow or compact the region delivered wrong bytes on Linux
  6.12 and 7.1 when the SQ filled and a push submitted inline (byte-verified
  runs; up to 26 bad messages in five runs). Without in-place moves the
  variant was clean in every run.
- **Without in-place moves it loses request/response traffic.** A region can
  grow or compact only after the kernel uses its posted range up. With
  regions that grow under load, it led INC by 21–26% on streaming at 1000
  connections and trailed it by 8–39% on request/response, pipelined and
  mixed traffic, and by 6% on streaming at 64 connections.
- **Its memory does not come back.** Grown regions shrink only once the
  posted range is used up; at 1000 connections they held 1 GiB, and 6.2 GB
  at 10,000 connections × 64 KiB, against INC's fixed 64 MiB pool.
- **It costs a kernel object per connection**: a buffer group id, a page of
  `RLIMIT_MEMLOCK` per connection on 6.14+ (10,000 connections do not
  register under the default 8 MiB limit), and a quarantine before a buffer
  group id is reused.

A later hybrid can give per-connection receive memory to the connections
that gain from it, streaming and forwarding ones (`forward_to`, segmented
receive), while the rest use the shared ring. The buffer state below does
not prevent that.

## What this design gives up

Against per-connection receive memory:

- **The receive copy stays.** Every byte is copied from the ring into the
  accumulator. Per-connection memory removes it, which is worth 21–26% on
  streaming at 1000 connections and about 24% at 1 MiB messages across
  hosts.
- **The shared-ring mechanisms stay and grow**: the `ENOBUFS` park, the
  fallback recv and the starvation arbitration remain, and the buffer state
  below is added.
- **Lends pin shared memory**, so above the per-worker cap arrivals are copied
  rather than lent.
- **`with_bytes` keeps its merge copy** when bytes arrive while a task holds
  slices of the remainder; per-connection memory would have kept unread and
  new bytes adjacent.
- **The ring emulator (#621)** must reproduce incremental shared buffers
  rather than read into connection memory.

In return it keeps memory fixed at 64 MiB per worker, needs no kernel
object per connection, and leads per-connection memory on request/response,
pipelined and mixed traffic.

## Detecting INC

The worker registers its ring with `IOU_PBUF_RING_INC` and confirms the
behaviour before relying on it: at startup it receives twice into a
one-entry INC ring over a socket pair and checks that the first completion
carries `IORING_CQE_F_BUF_MORE` and the second lands after the first. A
registration error, or completions without `F_BUF_MORE`, selects the plain
ring. Kernels before 6.12 are expected to reject the flag with `EINVAL`,
but only 6.12 and 7.1 were probed, so the check is behavioural rather than
trusting the error.

## Kernel behaviour relied on

Probed on Linux 6.12 and 7.1.13 (`experiments/recv-strategies/src/probe.rs`
on branch `exp/recv-strategies`); these become conformance tests in landing
step 0.

| Behaviour | Result |
|---|---|
| Multishot `RECV` on an INC ring | Successive completions append into one buffer at increasing offsets. Each carries `F_BUFFER` and the bid, and `F_BUF_MORE` while the buffer has room left. |
| The completion that uses up a buffer | Clears `F_BUF_MORE`. The kernel does not write into that buffer again until it is posted again. |
| Data offset | Not in the CQE. Within one connection, the first completion for a posting starts at the entry's address and each later one where the previous one ended. Across connections, see below. |
| The ring entry | Rewritten by the kernel as it consumes (`addr` advances, `len` shrinks). |
| No buffer left | `-ENOBUFS` without `F_MORE`, ending the arm, as on a plain ring. |
| EOF on a partly used buffer | `res = 0` without `F_BUFFER`; the buffer stays posted at its offset. |
| `RLIMIT_MEMLOCK` (6.14+) | Charged for the ring's entry array, not the buffers: a page for 64 entries, four pages for 1024. |

The driver derives each completion's data address from a per-buffer write
offset, which is correct only if completions for one buffer are reaped in
the order the kernel filled them, across every connection sharing it. The
benchmark checked this byte for byte: every message stamped with its
connection's sequence number and a payload derived from it, verified by the
server, on Linux 6.12 and 7.1, with a CQ small enough to overflow, under
SQPOLL, with held lends, and at 10,000 connections. No byte was wrong in 186
runs, and a deliberate one-byte offset error was caught at once. The
conformance test in step 0 repeats this against ringline.

Not yet probed: multishot `RECVMSG` (the `timestamps` feature) on an INC
ring.

## Buffer state

Today a buffer id (bid) is either posted, or out with exactly one
completion's bytes from offset 0, and returns once (`provided.rs`
`replenish_batch`, with a double-replenish `debug_assert`). Under INC a
posted buffer can be partly filled, its bytes can belong to several
connections, and some of them can be held by lends while the kernel keeps
filling the rest.

Each buffer has, in the driver:

- `written`: bytes the kernel has written since the buffer was last posted;
- `exhausted`: the kernel is done with it;
- `holds`: the number of live lends into it.

The rules:

1. **Every completion that carries `F_BUFFER` updates its buffer first**,
   before the connection's identity is checked: it advances `written` by
   `res` and, if `F_BUF_MORE` is clear, sets `exhausted`. The stale-CQE early
   return does this and nothing else. Otherwise the next completion into that
   buffer would be read at the wrong offset.
2. **The completion's data is `[base + written_before, base + written)`**.
3. **A buffer returns to the ring when it is `exhausted` and `holds` is
   zero**, and only then. It is posted whole (`addr = base`, `len =
   buffer_size`), and `written` resets to zero.
4. **A path that keeps bytes past the completion handler takes a hold**, and
   drops it when it is done. The copy paths (the accumulator copy, TLS, recv
   sinks, `ForceCopy`, timestamps) take none.
5. **On a plain ring every completion exhausts its buffer**, at offset 0. One
   code path serves both rings.

Rules 3 and 4 replace the per-path "exactly one replenish per bid" checks
(`docs/segmented-recv-design.md`) with one count per buffer and one return
site.

A buffer counts as out of the ring when it is exhausted and not yet
returned. A partly filled buffer is still in the ring: the kernel can fill
it. With that definition, `free() == 0` exactly when the kernel would return
`ENOBUFS`, which the starved re-arm (`event_loop.rs`, `free() > 0`) relies on.

### What has to change

These assume offset 0, or one completion per buffer, today:

- The completion handlers slice from the buffer base (`event_loop.rs`
  `handle_recv_multi`, `handle_recv_msg_multi_ts`).
- `HeldRecvBuf::Pinned { bid, len }` (`driver.rs`) stores no offset; nor do
  `SegBacking::Pinned` and `SegSettle::Pinned` (`runtime/io.rs`). Each gains
  `off`. Their readers re-derive the base: `driver.rs` forward-write iovecs,
  the Mode A split and `settle_forward_end`; `io.rs` segment readers and the
  `with_segments` remainder.
- `PendingRecvBuf` carries a pointer, set to the base today; it is set to the
  data's address. `copy_out_bid` (park) reads from the base and ignores that
  pointer.
- `handle_send_recv_buf` resubmits a partial send from
  `base + (original_len - remaining)`, and the user_data carries only the
  bid. A per-connection `send_recv_buf_ptr`, next to
  `send_recv_buf_original_lens`, holds the original data address.
- Releases that identify a lend by its bid become hold releases, which are
  correct when one bid appears in several lends: the `segment_pinned`
  single-release check, the recv-forward slab's one-bid-per-iovec replenish,
  `release_queued_sends`, the close and park drains, and the starved and
  accumulator flushes of `pending_recv_bufs`.
- `on_handout` and `free()` count buffers out of the ring as defined above.
- `MAX_FORWARD_IOV` (16) was sized as 16 × 16 KiB per write; one gathered
  write can now hold up to 16 shared 1 MiB buffers.

The UDP ring shares the `ProvidedBufRing` type and stays plain; with rule 5
it needs no special case.

## Lends hold shared memory

A lend (`pending_recv_bufs`, `forward_recv_buf`, recv-forward, direct echo,
segments, `forward_to` Mode A) holds a range of a buffer until its task polls
or its send completes. Under INC the buffer is shared: a 200-byte lend keeps
a whole 1 MiB buffer, and every other connection's bytes in it, out of the
ring.

- **Per worker, a lend is taken only while fewer than half the ring's
  buffers are held.** Above that, new arrivals are copied into the
  accumulator instead (the `ForceCopy` decision the segmented reserve makes
  today). This is a liveness rule, not only a tuning one: plaintext lends in
  `pending_recv_bufs` are uncapped today, and connections whose tasks do not
  poll could hold every buffer, leaving a parked connection with nothing to
  re-arm into. With the cap, at least half the ring keeps cycling. Measured
  with held lends on the INC ring, every second connection holding each
  range for 50 ms, 64 KiB messages, 1000 connections (hv01; two hosts in
  parentheses): no cap, 20.1k msg/s (19.9k); half, 36.9k (22.8k); copying
  everything, 36.6k (22.5k); no holds, 38.2k (23.2k). Every cap from 0 to
  0.75 was within the noise of the others, on every workload measured, so
  the value is not a tuning knob.
- **Per connection, `forward_hold_cap` still counts held ranges**, but one
  forwarder can now pin up to `forward_hold_cap` shared buffers; the
  per-worker cap bounds that.
- The per-worker cap replaces `recv_segment_reserve`, which counts free
  buffers for the segmented paths only. Removing it removes a public
  `ConfigBuilder` method.
- Lends are sent with plain `send` and `sendmsg`/`writev`, never zero-copy,
  so a hold is released at the send's completion. A lend sent zero-copy would
  have to hold until the notification.

## Starvation and the fallback recv

A 64 MiB pool can run dry: 10,000 connections × 64 KiB in flight is 625 MiB.
No measured cell on hv01 or across hosts ran it dry at the chosen geometry.
The `ENOBUFS` park and the fallback one-shot recv (#274) stay.

The fallback's chunk is `max(4 × buffer_size, 1 MiB)` today, and its
arbitration prefers the fallback over a re-arm because the chunk is meant to
exceed the ring's capacity (`event_loop.rs` `flush_replenish_and_rearm`). A
64 MiB ring breaks that premise. Step 3 sets the chunk to 1 MiB and re-arms
when `free() × buffer_size` exceeds the chunk, and step 4 measures it.

Running dry is backpressure, not loss: the multishot ends, the bytes stay in
the socket's receive queue, and TCP closes the window until the worker
re-arms.

## Sizing

- **64 MiB per worker.** At 64 × 1 MiB, INC beat today's ring or matched it
  in every measured cell on hv01 and across hosts (journal). On hv01 a
  16 MiB pool of 1 MiB buffers streamed 23% faster at 64 connections and 9%
  slower at 1000, and a 4 MiB pool of 64 KiB buffers was 44% slower at 1 MiB
  messages; 64 MiB is the size that holds across connection counts and
  message sizes.
- **Rule for other links.** The pool covers the bytes that arrive while a
  worker is not reaping: ingress rate per worker × the longest stall to absorb
  before TCP backpressure starts. At 100 Gbit/s (12.5 GB/s) into one worker,
  64 MiB is about 5 ms, and a 10 ms stall needs about 120 MiB (128 MiB as a
  power of two); spread across N workers, each needs about a 1/N share.
- **Memory.** The backing is a heap allocation, resident once touched, and
  the ring is used in order, so a worker that has received 64 MiB in total
  has all of it resident: 64 MiB × workers per host, against 4 MiB × workers
  today. `prefault_buffers` touches all 64 MiB at startup. With transparent
  huge pages, each 2 MiB region's first touch otherwise happens on the
  receive path (`docs/journal/2026-09-prefault-measurement.md`).
- **`RLIMIT_MEMLOCK`.** Only the entry array is charged (6.14+); the startup
  preflight (`memlock.rs`) runs before the probe and assumes the plain ring's
  1024 entries.
- `recv_accumulator_max` must be at least `buffer_size` (validated today);
  its default is 1 GiB.

The ring stays configurable with `recv_buffer(ring_size, buffer_size)`. A
user-set geometry is used as given on either kind of ring; `RecvBufferConfig`
gains a flag recording whether it was set.

## What stays

- **The UDP ring** (`udp_recv_buffer`, its own bgid): plain.
- **The accumulator and its copy.** `with_data` and `with_bytes` read the
  accumulator as today.
- **TLS** (both engines) copies out of the ring and takes no hold.
- **The `timestamps` feature** shares the TCP ring today. If multishot
  `RECVMSG` works on an INC ring (step 0 probes it), it stays, with
  `RecvMsgOut::parse` reading at the completion's offset; otherwise
  timestamped connections get their own plain ring.
- **mio** is unchanged. If the ring emulator (#621) lands, it must implement
  this state machine.

## Metrics

Kept: `buffer_ring_empty`, `recv_parked`, `recv_fallback`,
`forward_throttled`. New: buffers out of the ring, buffers held by lends,
lends refused by the per-worker cap, and the ring kind in use.

## Landing

0. **Probe and conformance tests**: the behaviour above, the byte-verified
   offset order (with an overflowing CQ and under SQPOLL), and multishot
   `RECVMSG` on an INC ring.
1. **Buffer state** (`written`, `exhausted`, `holds`) with the ring
   registered plain. Every completion exhausts its buffer, so behaviour is
   unchanged; the per-path single-release checks become holds. The existing
   lifecycle tests (double replenish, leaks, close drain, the Mode A hold cap)
   pass unchanged.
2. **Offsets**: the `off` fields, data at `base + written`, the
   `send_recv_buf_ptr`, `copy_out_bid` and `settle_forward_end` fixes. Still a
   plain ring.
3. **INC**, off by default behind a switch, with the startup probe, the
   per-worker lend cap and the fallback arbitration.
4. **Measure ringline**, not the benchmark, on hv01 and across hv01/hv02:
   INC 64 × 1 MiB and plain 1024 × 64 KiB against today's default, with the
   bench suite (echo at 256 B to 1 MiB, mixed sizes, the #415 forward proxy,
   streaming, 64 to 10,000 connections, RSS). Five reps or more per cell with
   an A/A pair; a difference counts when it exceeds the A/A spread. Gate: no
   cell slower than today beyond that spread, and none at 10,000 connections
   with sustained `buffer_ring_empty`.
5. **New defaults** (the per-kernel geometry, the switch on, the removal of
   `recv_segment_reserve`) in a coordinated release.

Steps 1 and 2 change no behaviour and can land before INC is switched on.

## Owner decisions

- One shared ring per worker, not per-connection receive memory or one-shot
  recv (2026-10-06; reconfirmed 2026-10-07 after the per-connection rings
  were rebuilt with adaptive regions). A hybrid for streaming and forwarding
  connections is a follow-up.
- INC at 64 × 1 MiB on 6.12+; a plain ring of 1024 × 64 KiB below (2026-10-07).
- Measurements are taken on hv01 and hv02, not on the validation host,
  which carries other tenants' load (2026-10-06).
