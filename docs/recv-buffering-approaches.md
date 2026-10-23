# TCP receive buffering on io_uring: approaches

Status: reference. This document compares the ways an io_uring worker can
get TCP bytes from the kernel, what memory each needs, and how each behaves
when that memory runs short. The design chosen for ringline is
`docs/recv-incremental-ring-design.md`; this document places it among the
alternatives. Kernel versions and known kernel bugs for each mechanism are
in `docs/recv-kernel-errata.md`. Measurements are cited by source:

- the journal entry `docs/journal/2026-09-incremental-buffer-consumption.md`
  (the standalone `recv-strategies` benchmark, one worker);
- the journal entry `docs/journal/2026-10-receive-starvation.md` (ringline's
  `bench-server`, two workers on two hosts, and one worker locally).

## What a receive path has to hold

A worker receives for many connections at once. Whatever memory the kernel
writes received bytes into has to exist before the bytes arrive, so a
receive path is defined by who owns that memory and how much of it exists.

The memory in use at one moment is about:

    connections with data arriving × bytes held per connection

Bytes are held per connection from the completion until the task has
parsed them, or, for a forward, until the send that carries them completes.
For a request/response protocol with one request in flight per connection,
that is up to one message per connection. The connection count and message
size come from the peers, so any fixed amount of receive memory can be
exceeded by enough connections sending at once.

Every approach below answers two questions:

- How much memory does it allocate, and does the allocation depend on the
  load?
- When the memory runs out, which connections wait, and is the total
  bounded?

## Shared provided-buffer ring

The worker registers one ring of equal-sized buffers with the kernel (a
provided-buffer ring, `IORING_REGISTER_PBUF_RING`). A receive with buffer
select takes one buffer when data arrives and returns its id in the
completion. The worker returns the buffer to the ring when it is done with
the bytes. Memory is `entries × buffer size` per worker, allocated at
startup.

### One-shot receive (ringline's default)

Each connection has one receive in flight. When it completes, the next is
armed. A connection holds at most one buffer that the kernel is filling, plus
whatever buffers its task still holds.

When the ring is empty the receive completes with `-ENOBUFS`. The worker
parks the connection and re-arms it when a buffer returns
(`flush_replenish_and_rearm`). Bytes stay in the socket's receive queue,
and once it fills TCP advertises a zero window, so the peer is slowed.

Ring buffers are not the only receive memory. On the `with_data` path a
completion is appended to the connection's accumulator, a heap buffer,
whenever the accumulator already holds bytes or the task has not yet taken
the previous buffer; its ring buffer then returns at once. A task that
returns `NeedMore`, or consumes part of a buffer, has the rest copied too.
On the `with_bytes` path every completion is copied into the accumulator, at
the latest when the task reads it. For a parser of messages larger than one
buffer, the accumulator holds up to one message per connection when each
connection has one request in flight. Accumulators are bounded per
connection by `recv_accumulator_max` (1 GiB by default), not in total. A
forward echo, which forwards each buffer without parsing, copies rarely. In
the local one-worker forward echo at 1 MiB × 512 on a 1024 × 64 KiB ring,
99.9% of data completions were forwarded from ring buffers, and peak RSS
stayed at 147 MiB.

Which parked connection re-arms first decides fairness. A pass re-arms every
parked connection while any buffer is free, in the order `swap_remove`
leaves the list, so which connection gets a buffer is arbitrary. At 10,000
connections on a 256 × 16 KiB ring, 75 of 80 runs left connections with no
response for the whole run, most of them thousands. #657, not merged at the
time of writing, re-arms the oldest first, at most one per free buffer;
with its first commit, the same test left connections unanswered in 3 of
80 runs.

The geometry decides how often the ring empties. In ringline's two-worker
sweep at 16,384 connections:

- Up to 4 KiB messages, entries per connection decide. 16,384 4 KiB buffers
  per worker (64 MiB, two entries per connection) emptied the ring at most
  about 21,000 times per run. At 256 B it matched the incremental ring on
  6.12 (127.6k against 127.8k req/s) and ran 25% slower on 7.1 (118k
  against 158k).
- At 64 KiB, buffer size decides. 4 KiB buffers cost 16 completions per
  message, and that configuration ran at a third of the incremental ring's
  rate. 64 MiB of 64 KiB buffers came within about 10% of it, but left 233
  and 1,916 connections unanswered in its two runs on 6.12. 4 MiB of 64 KiB
  buffers ran 30% slower than the incremental ring.
- A ring with too few entries for the connections emptied 0.5 million to
  5.7 million times in most runs, and, on 7.1, in some runs of the same
  configuration not at all. Throughput varied by up to 1.8× between such
  runs (7.1, 256 B, 16 MiB of 16 KiB buffers: 89k with 5.0 million empties,
  163k with 746), and some connections went unanswered in the emptying
  runs. Configurations that rarely emptied the ring varied by at most 12%.

No single geometry was best at every message size. A ring sized for a known
load (connections per worker × buffers per message, at the message size)
avoids running out. A runtime whose load is set by its peers cannot size
the ring this way.

### Multishot receive (`recv_multishot(true)`)

One receive stays armed and takes a new buffer for each arrival until it
ends. It saves an SQE per buffer and the wait for the next arm. With
one-shot receives, one connection streaming large messages receives about
a fifth less per second (`ConfigBuilder::recv_multishot`).

A multishot receive keeps taking buffers while its socket has data, so a few
connections with large messages can take most of the ring while others get
none. The arm ends with `-ENOBUFS` when the ring is empty, and the same
park and re-arm path applies. ringline made one-shot the default for this
reason (#647). A multishot receive also takes buffers past a connection's
hold cap before the cancel that throttles it lands: 2 to 72 buffers per
throttle on a 256 × 16 KiB ring on Linux 6.12 (#638).

### Fallback receive on `ENOBUFS` (#274)

A parked connection on the plaintext `with_data` or `with_bytes` path that
has part of a message in its accumulator gets a one-shot receive into a slot
of a per-worker pool instead of waiting for ring buffers. The pool has 32
slots of `max(4 × buffer size, 1 MiB)`, or 1 MiB with `recv_incremental`,
and is allocated on first use. The bytes are then appended to the
accumulator. The fallback keeps the connection's TCP window open while the
ring is empty. TLS and segmented connections keep waiting for ring
buffers. With `recv_incremental`, a parked
connection re-arms on the ring instead when the ring's free bytes exceed one
chunk.

## Incremental ring (`recv_incremental`)

The ring is registered with `IOU_PBUF_RING_INC` (Linux 6.12 and later).
Successive completions, from any connection, fill one buffer at increasing
offsets, and the buffer returns to the ring only when it is used up and
nothing holds any of its bytes. A 256 B arrival uses 256 B of a 1 MiB
buffer, so large buffers also serve small messages. The default geometry is
64 × 1 MiB per worker.

Measured:

- In the benchmark it matched or beat the 256 × 16 KiB ring on throughput in
  every cell. Its gains over a 1024 × 64 KiB plain ring were at 1 MiB
  messages on one host (not across hosts) and on streaming.
- In ringline's sweep it never left a connection unanswered through the
  receive path, at 16 to 16,384 connections, at 256 B to 64 KiB.

A held range pins its whole shared buffer. To keep held lends from pinning
the whole ring, ringline lends a completion only while at most half the
ring's buffers have a hold (`Driver::may_lend`). Above that, the completion
is copied into the accumulator for `with_data` and `with_bytes`. The
accumulator copies have no total bound. An incremental completion can be a
whole message, so each copy can be a whole message. In ringline's forward
echo, peak RSS reached 1.5 GiB at 64 KiB × 16,384 and 5.6 GiB at 1 MiB ×
4,096, against 128 MiB of ring (two workers). The growth comes from the
lend cap and the accumulator; the incremental ring's own memory stays at 64
MiB per worker.
It has to be bounded before `recv_incremental` is the default
(`docs/journal/2026-10-receive-starvation.md`). An accumulator releases its
allocation when its connection closes (#660).

The kernel behaviour the driver relies on is checked at startup on the
running kernel (`docs/recv-incremental-ring-design.md`, "Selecting the ring
kind"), because the incremental-ring code had fixes in 6.12 stable releases
that a version check cannot detect.

## Two buffer groups

The design proposes a second group of 1 MiB buffers. Connections that stream
or hold lends move to it, so they stop taking the small group's buffers. On
6.1 it cut request latency 2.8× to 25× in cells with streamers. On 6.12,
with an incremental ring, the difference was within run-to-run variation.
It is proposed on for plain rings and off for incremental ones, to be
decided by landing step 6 (`docs/recv-incremental-ring-design.md`, "The
large group").

## Per-connection rings

Each connection gets its own one-entry incremental ring over memory it owns,
so no connection can take another's buffers. Measured in the benchmark and
not chosen:

- The variant that moves a connection's region by rewriting the posted ring
  entry delivered wrong bytes with a 64-entry CQ and 32-entry SQ, on 6.12
  and 7.1.
- The variant that never rewrites a posted entry delivered no wrong byte.
  Three of its small-queue streaming runs each lost one connection, which is
  unexplained.
- On 7.1 each ring is charged a page of `RLIMIT_MEMLOCK`. Under the default
  8 MiB, 2,041 rings registered before `ENOMEM`, so 10,000 connections did
  not start.
- With adaptive region sizes, regions grew to their cap and stayed: about
  1 GiB at 1,000 connections, 6.1 GiB at 10,000 × 64 KiB.

## One-shot receive into connection-owned memory

Each connection has its own buffer, and a one-shot `RECV` without buffer
select reads into it. This is how a readiness-based server reads. The
kernel cannot run out of shared buffers, and memory is
`connections × buffer size`, allocated whether or not data arrives.

In the benchmark (`oneshot`) it led at 256 B and 64 KiB with 64
connections and fell behind at 1,000 and more: 64 KiB × 1,000 at 28.8k
against 38.1k for the incremental ring, and mixed traffic at 47.2k against
74.0k. ringline's fallback receive reads into memory it owns in the same
way, for parked connections with a partial message.

## Bundles

A receive with `IORING_RECVSEND_BUNDLE` (Linux 6.10) takes several buffers
in one completion, so a message larger than a buffer completes once rather
than once per buffer. It changes the completion count for large messages on
a plain ring. It does not change how many buffers a message takes, so it
does not change when the ring runs out. It has not been measured here.

## Byte limit on a multishot receive

A length on a multishot `RECV` caps the bytes per completion. The benchmark
tried it (`--recv-len`) and dropped it: 6.12 rejected a nonzero length on
multishot `RECV` with `EINVAL`, and on 7.1, which accepted it, throughput
fell. The errata document records the kernel versions.

## Zero-copy receive

io_uring zero-copy receive (`IORING_REGISTER_ZCRX_IFQ`, Linux 6.15) has the
NIC write payload into memory the application registered, on a dedicated
hardware queue with header split and flow steering. It removes the copy into
the accumulator. It needs NIC and driver support and a hardware queue per
worker (the kernel's `iou-zcrx` documentation). It has not been measured
here.

## Choosing between copying and waiting

When receive memory runs short, a receive path either makes the connection
wait or copies its bytes somewhere else:

| Approach | When memory runs short | Memory bound |
|---|---|---|
| Plain ring, one-shot or multishot | the connection parks; TCP holds the bytes and slows the peer | the ring for buffers held in place; accumulators up to `recv_accumulator_max` per connection |
| Plain ring with the fallback receive | a connection with a partial message reads into a pool slot (32 per worker), then into its accumulator | as above, plus the pool |
| Incremental ring with the lend cap | above half the ring held, completions are copied to the heap | the ring, one ring's worth of owned copies, and accumulators up to `recv_accumulator_max` per connection |
| Per-connection memory | never short; each connection has its own | connections × region size, allocated up front |

Waiting puts the cost on the waiting connection's latency, and fairness
then depends on the re-arm order (#657). Copying keeps every connection
moving and puts the cost on memory. A runtime can bound the copy
path with a per-worker copy budget and wait once the budget is used. That is
not implemented.

## What is not known

- Whether the final version of #657 still starves connections at 16,384
  connections (`docs/journal/2026-10-receive-starvation.md`, pending).
- ringline's own measurements cover echo and forward workloads with two
  workers on one pair of hosts. The benchmark's cover more workloads with
  one worker.
- Bundles and zero-copy receive have not been measured.
- A per-worker copy budget for the incremental ring has not been designed.
