# One incremental receive ring per worker

Status: proposal, chosen by the owner on 2026-10-06 after the measurements
in `docs/journal/2026-09-incremental-buffer-consumption.md` ("2026-10: INC
against the alternatives"). Nothing here is implemented.

Related: `docs/segmented-recv-design.md` (the bid lifecycle of the lending
paths and the exactly-once replenish rule),
`docs/journal/2026-07-enobufs-fallback-recv.md` (#274, the fallback recv),
`docs/ring-emulator-design.md` (#621).

## Goal

A TCP receive on io_uring lands in a buffer from one provided-buffer ring
shared by the worker's connections: 256 buffers of 16 KiB (4 MiB) by default
(`config.rs` `RecvBufferConfig::default`). That ring has to be tuned two ways
at once. Small buffers fragment large messages across many completions, each
paying a fixed per-completion cost (#415 measured 25% between 16 and 64 KiB
on the forward path). A fixed memory budget in large buffers makes the ring
shallow, and a shallow ring starves under many connections (#416 Phase A: a
4-deep ring starved 3,066,467 times). At 10,000 connections the default
starves on its own: 15.7M `ENOBUFS` in one 8-second loopback run of 256 B
messages.

`IOU_PBUF_RING_INC` (Linux 6.12) removes the trade. One buffer is consumed
incrementally by successive completions, from any connections, and returns to
the ring only when it is used up. Large buffers then serve small arrivals
without wasting the rest of the buffer, so the same ring has both payload per
completion and depth.

This design keeps one shared TCP ring per worker, registers it with
`IOU_PBUF_RING_INC`, and sizes it at 64 MiB in 1 MiB buffers. It keeps the
copy into the accumulator, the lend-in-place paths, and the `ENOBUFS`
fallback. What changes is how a buffer is tracked: a buffer can now hold the
bytes of several completions at increasing offsets, and returns to the ring
only when the kernel has finished with it and nothing holds any of its bytes.

## Why this shape

Measured against the alternatives on loopback (Linux 6.12) and across two
hosts (Linux 7.1); the journal entry has the experiments and full tables.

| | Today's ring | This design | Per-connection rings | One-shot recv |
|---|---|---|---|---|
| 10k conns × 256 B, loopback | 47k msg/s, 15.7M `ENOBUFS` | 80k, 0 `ENOBUFS` (4 MiB) | 75k | 57k |
| 1000 × 64 KiB, two hosts | 20k | 23.7k (64 MiB, 1 MiB buffers) | 24.1k | 20k |
| 10k × 64 KiB, loopback | 19k | 35.6k (64 MiB, 1 MiB buffers) | 45.3k | 37k |
| 10k × 64 KiB, two hosts | — | 21.6k | 21.9k | — |
| stream 16 KiB × 64, two hosts | 104k | 103k (4 MiB) | 81k | 81k |
| Kernel objects per connection | none | none | a ring, a bgid, a page of `RLIMIT_MEMLOCK` on 6.14+ | none |
| Kernels before 6.12, SQPOLL | the ring | today's ring (see "Kernels without INC") | the one-shot arm | the arm |

Per-connection receive memory removes the copy into the accumulator, and wins
where that copy dominates: large messages at high connection counts on
loopback. Across hosts the receive work the strategies share bounded every
configuration, and the 1 MiB-buffer INC ring matched it. Per-connection rings
also cost a buffer group id per connection held until its arm's terminal CQE,
a quarantine before reuse (a stale arm writes into a re-registered bgid,
probed on 6.12 and 7.1), a dependence on rewriting a posted ring entry, and
22% of streaming throughput. One-shot recv halved small-message throughput at
1000 connections.

A larger plain ring is not a substitute: at the same 64 MiB, a ring of
16 KiB buffers did 24k msg/s at 10k × 64 KiB where INC in 1 MiB buffers did
35.6k, and at 64 KiB × 64 connections a 4 MiB INC ring did 108k where a
64 MiB plain ring did 59k.

## Kernel behaviour relied on

Probed on Linux 6.12 and 7.1.13 (`experiments/recv-strategies/src/probe.rs`);
these become conformance tests in landing step 1.

| Behaviour | Result |
|---|---|
| Multishot `RECV` on an INC ring | Successive completions append into one buffer at increasing offsets; each carries `F_BUFFER` and the bid, and `F_BUF_MORE` while the buffer has room left. |
| The completion that uses up a buffer | Clears `F_BUF_MORE`. The kernel will not write into that buffer again until it is posted again. |
| Data offset | Not in the CQE. The first completion for a posting starts at the entry's address; each later one starts where the previous one ended. |
| The ring entry | Rewritten by the kernel as it consumes (`addr` advances, `len` shrinks). |
| No buffer left | `-ENOBUFS` without `F_MORE`, ending the arm, as on a plain ring. |
| Registration without INC support | `EINVAL` (6.1–6.11). |
| `RLIMIT_MEMLOCK` (6.14+) | Charged for the ring's entry array, a page for this ring; the buffers are not charged. |

The driver derives each completion's data address from a per-buffer write
offset, which is correct only if completions for one buffer are reaped in the
order the kernel filled it. That is an assumption, not a probed fact. The
benchmark derived offsets this way for every byte of every run, with a
length-prefixed framing that a wrong offset would have broken, and no run
failed. A conformance test makes it a check: many connections, framed random
payloads, every byte verified.

Not yet probed: INC under SQPOLL, where the kernel thread fills buffers
concurrently with the worker; multishot `RECVMSG` (the `timestamps` feature)
on an INC ring.

## Buffer state

Today a buffer id (bid) is either posted, or out with exactly one completion's
bytes from offset 0, and returns once (`provided.rs` `replenish_batch`, with a
double-replenish `debug_assert`). Under INC a posted buffer can be partly
filled, its bytes can belong to several connections, and some of those bytes
can be held by lends while the kernel keeps filling the rest.

Each buffer gets a state in the driver:

- `written`: bytes the kernel has written since the buffer was last posted,
  advanced by each completion that names the bid;
- `exhausted`: a completion for this bid cleared `F_BUF_MORE`, so the kernel
  is done with it;
- `holds`: the number of live lends into it.

A buffer returns to the ring when it is `exhausted` and `holds` is zero, and
only then. It is posted whole (`addr = base`, `len = buffer_size`), and
`written` resets to zero. This replaces the per-path "exactly one replenish
per bid" discriminants (`docs/segmented-recv-design.md`) with one count per
buffer: a path takes a hold when it keeps bytes past the completion handler
and drops it when it is done, and the return happens in one place.

Every completion's data is `[base + written, base + written + res)`, and the
handler advances `written` before anything else reads it. Nothing returns a
buffer from inside the handler: the paths that return it right after copying
today (the copy into the accumulator, TLS, recv sinks, the stale-CQE early
return, `ForceCopy`, timestamps) copy and take no hold, and the return
happens when the buffer is exhausted.

On a ring registered without INC (see below), every completion is treated as
exhausting its buffer, at offset 0. One code path serves both.

### What has to change

These assume offset 0 or one completion per buffer today:

- The completion handlers slice from the buffer base (`event_loop.rs`
  `handle_recv_multi`, `handle_recv_msg_multi_ts`): they read
  `base + written`.
- `HeldRecvBuf::Pinned { bid, len }` stores no offset, and every reader
  re-derives the base (`driver.rs` forward-write iovecs and the Mode A split,
  `io.rs` segment readers and the `with_segments` remainder). It gains the
  offset: `Pinned { bid, off, len }`.
- `PendingRecvBuf` already carries a pointer, set to the base today; it is set
  to the data's address.
- `SendRecvBuf` resubmits a partial send from `base + (original_len -
  remaining)` (`event_loop.rs` `handle_send_recv_buf`): it resubmits from the
  held pointer.
- Single-release checks that identify a lend by its bid (`segment_pinned`
  compares `pinned_bid == bid`; the recv-forward slab pushes one bid per
  iovec; close and park drains push one bid per entry) become hold releases,
  which are correct when one bid appears in several lends.
- `on_handout` counts one per completion and `free()` is
  `ring_size - completions not yet returned`. Under INC it counts buffers out
  of the ring: a buffer leaves on its first completion after posting and
  returns once.

## Lends hold shared memory

A lend (`pending_recv_bufs`, `forward_recv_buf`, recv-forward, direct echo,
segments, `forward_to` Mode A) holds a range of a buffer until its task polls
or its send completes. Under INC that buffer is shared: a lend of 200 bytes
keeps a whole 1 MiB buffer out of the ring, along with every other
connection's bytes in it.

Two limits keep lends from emptying the ring:

- **Per connection**, `forward_hold_cap` keeps bounding one slow forward. It
  counts held ranges, as it counts held buffers today, so its meaning does not
  change.
- **Per worker**, a lend is taken only while the buffers out of the ring with
  holds stay under half of `ring_size`. Above that, new arrivals are copied
  into the accumulator instead of lent (the `ForceCopy` decision the
  segmented reserve makes today). This replaces `recv_segment_reserve`, which
  counts free buffers for the segmented paths only, with one rule for every
  lending path.

The copy fallback keeps every lending path correct when the cap bites; it
costs the copy the lend avoided.

## Starvation

A 64 MiB pool can still run dry: 10,000 connections × 64 KiB in flight is
625 MiB. Loopback at that load saw 2.4M `ENOBUFS` per 8-second run at 64 MiB
in 1 MiB buffers; across hosts, none. The `ENOBUFS` park and the fallback
one-shot recv (#274) stay, with one change: `fallback_chunk` is
`max(4 × buffer_size, 1 MiB)` today, which at 1 MiB buffers is 4 MiB per slot
and 128 MiB for the 32-slot pool. It becomes 1 MiB.

Running dry is backpressure, not loss: the multishot ends, the bytes stay in
the socket's receive queue, and TCP closes the window until the worker
re-arms.

## Sizing

The default ring is 64 buffers of 1 MiB per worker.

- **Buffer size, 1 MiB.** Across hosts, 1 MiB buffers matched the
  per-connection rings at 1000 × 64 KiB, where 64 KiB buffers stayed 10%
  behind at every pool size. A larger buffer also raises the payload one
  completion can deliver, which is what the forward path's per-completion cost
  needs (#415). `recv_accumulator_max` must be at least `buffer_size`
  (validated today); its default is 1 GiB.
- **Pool size, 64 MiB.** The smallest size with no `ENOBUFS` in every cell but
  the 10k × 64 KiB loopback burst. 128 and 256 MiB changed nothing measurable
  across hosts.
- **Rule for other links.** The pool covers the bytes that arrive while a
  worker is not reaping: ingress rate per worker × the longest stall to absorb
  before TCP backpressure starts. At 100 Gbit/s (12.5 GB/s) into one worker,
  64 MiB is about 5 ms and a 10 ms stall needs about 128 MiB; spread across
  N workers, each needs about a 1/N share. Past that, the cost is the re-arm
  and a throughput dip while windows reopen, not data loss. The rig cannot
  measure 100 GbE.
- **Memory.** The backing is a heap allocation (`provided.rs`), resident once
  touched: a burst that reaches every buffer leaves 64 MiB resident per worker.
  `RLIMIT_MEMLOCK` is charged only for the 64-entry array.

The ring stays configurable with `recv_buffer(ring_size, buffer_size)`; only
the defaults change.

## Kernels without INC

INC is probed at startup by registering with `IOU_PBUF_RING_INC`; `EINVAL`
means the kernel predates it (6.1–6.11). The worker then registers a plain
ring over the same backing, cut into 16 KiB buffers (4096 of them, 64 MiB),
unless the user set `recv_buffer` explicitly. A plain ring of 64 buffers of
1 MiB would be the shallow ring Phase A measured starving. The same applies if
INC turns out not to work under SQPOLL (step 1 probes it).

## What stays

- **The UDP ring** (`udp_recv_buffer`, its own bgid). Datagrams are consumed
  whole; it stays plain.
- **The accumulator and its copy.** `with_data` and `with_bytes` read the
  accumulator as today.
- **The lending paths**, with offsets and holds.
- **TLS** (both engines) copies out of the ring as today and takes no hold.
- **The `timestamps` feature** shares the TCP ring today. If multishot
  `RECVMSG` works on an INC ring (step 1 probes it), it stays on it, with
  `RecvMsgOut::parse` reading at the completion's offset; otherwise
  timestamped connections get their own plain ring.
- **mio** is unchanged. The ring emulator (#621) implements INC by reading
  into the buffer at its write offset, so both engines share this state
  machine.

## Metrics

Kept: `buffer_ring_empty`, `recv_parked`, `recv_fallback`,
`forward_throttled`. New: buffers out of the ring, buffers out with holds,
lends refused by the per-worker cap, and whether INC is active.

## Landing

0. **Probe and conformance tests**: the INC behaviour above (including the
   reaped order of one buffer's completions under load), INC under SQPOLL,
   and multishot `RECVMSG` on an INC ring.
1. **Buffer state**: `written`, `exhausted` and `holds`, with the ring
   registered plain. Every completion exhausts its buffer, so behaviour is
   unchanged; the per-path single-release checks become holds. The existing
   lifecycle tests (double replenish, leaks, close drain, the Mode A hold cap)
   must pass unchanged.
2. **Offsets**: `Pinned { bid, off, len }`, data at `base + written`, partial
   resubmits from the held pointer. Still a plain ring.
3. **INC**, behind the startup probe, with the per-worker lend cap and the
   1 MiB `fallback_chunk`. Default geometry unchanged.
4. **Measure** ringline itself on the two-host rig against today's default,
   with the bench suite: echo at 256 B to 1 MiB, mixed sizes, the #415
   forward proxy, streaming, 64 to 10,000 connections, and RSS. Gate: no
   workload more than 2% slower or 2% worse at p99 than today; at 10,000
   connections, `buffer_ring_empty` near zero.
5. **New defaults**: 64 × 1 MiB, and the 16 KiB re-cut on kernels without
   INC. A breaking default change, in a coordinated release.

Steps 1 and 2 change no behaviour and can land before INC is switched on.

## Owner decisions

- One shared incremental ring per worker, not per-connection receive memory
  or one-shot recv (2026-10-06).
- 64 MiB per worker in 1 MiB buffers by default (2026-10-06).

## Questions for the owner

1. **Kernels without INC.** Re-cut the same 64 MiB into 16 KiB buffers (the
   proposal), or keep today's 4 MiB default there?
2. **The per-worker lend cap.** Half the ring is a guess, not a measurement;
   step 4 can sweep it. Should it be configurable, or fixed?
