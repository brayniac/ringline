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
  `bench-server`, two workers).

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
(`flush_replenish_and_rearm`). Bytes stay in the socket's receive queue, and
TCP closes the window, so the peer is slowed and memory is bounded by the
ring.

Which parked connection re-arms first decides fairness. Before #657 the
order was arbitrary. At 10,000 connections on a 256 × 16 KiB ring, 75 of 80
runs left connections with no response for the whole run, most in the
thousands. #657 re-arms the oldest first, one per free buffer. With it, 3 of
80 runs had any unanswered connections.

The geometry decides how often the ring empties. In ringline's two-worker
sweep at 16,384 connections:

- Up to 4 KiB messages, entries per connection decide. 16,384 4 KiB buffers
  per worker (64 MiB, two entries per connection) never emptied and matched
  the incremental ring.
- At 64 KiB, buffer size decides. 4 KiB buffers cost 16 completions per
  message, and that configuration ran at a third of the incremental ring's
  rate. 64 KiB buffers came within about 10% of it.
- A ring that does not cover the connections empties it millions of times
  per run. Throughput then varies by up to 1.5× between runs of one
  configuration, and some connections go unanswered.

No single geometry was best at every message size. A ring sized for a known
load (connections per worker × buffers per message, at the message size)
avoids running out. That covers a deployment whose load is known. It does
not cover one whose load is set by peers.

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

A parked connection whose task is waiting for a message larger than the ring
can hold gets a one-shot receive into its own accumulator memory instead of
waiting for ring buffers. This lets a message larger than the ring
complete. The fallback reads chunks of `max(4 × buffer size, 1 MiB)`, or
1 MiB with `recv_incremental`. Its memory comes from the heap, per
connection.

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
  messages and on streaming.
- In ringline's sweep it never left a connection unanswered through the
  receive path, at 16 to 16,384 connections, at 256 B to 64 KiB.

A held range pins its whole shared buffer. To keep held lends from pinning
the whole ring, ringline lends a completion only while at most half the
ring's buffers have a hold (`Driver::may_lend`). Above that, the completion
is copied to the heap: into the accumulator for `with_data` and
`with_bytes`, into an owned buffer for recv-forward and direct echo. The
copies have no total bound. In ringline's forward echo they reached 1.5 GiB
at 64 KiB × 16,384 and 5.6 GiB at 1 MiB × 4,096, against 64 MiB of ring per
worker. Accumulators are also not freed when their connection closes. This
is a property of the lend cap and the accumulator, not of the incremental
ring, and it has to be fixed before `recv_incremental` is the default
(journal, 2026-10).

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
- The variant that never rewrites a posted entry delivered no wrong byte. It
  lost one connection in three small-queue streaming runs, which is
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
connections and fell behind at 1,000 and more: 64 KiB × 1,000 at 28.8k against 38.1k for the incremental
ring, and mixed traffic at 47.2k against 74.0k. ringline's fallback receive
is this approach used only for a connection whose message does not fit the
ring.

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
the accumulator. It needs NIC and driver support and a queue per worker, and
it does not apply to loopback (`docs/io-uring-primer.md`). It has not been
measured here.

## Choosing between copying and waiting

When receive memory runs short, a receive path either makes the connection
wait or copies its bytes somewhere else:

| Approach | When memory runs short | Memory bound |
|---|---|---|
| Plain ring, one-shot or multishot | the connection parks; TCP holds the bytes and slows the peer | the ring |
| Plain ring with the fallback receive | a connection waiting for a large message reads into its accumulator | the ring, plus `recv_accumulator_max` per connection using the fallback |
| Incremental ring with the lend cap | above half the ring held, completions are copied to the heap | none in total; `recv_accumulator_max` per connection |
| Per-connection memory | never short; each connection has its own | connections × region size, allocated up front |

Waiting bounds memory and puts the cost on the waiting connection's latency,
and fairness then depends on the re-arm order (#657). Copying keeps every
connection moving and puts the cost on memory. A runtime can bound the copy
path with a per-worker copy budget and wait once the budget is used. That is
not implemented.

## What is not known

- Whether the final version of #657 still starves connections at 16,384
  connections (journal 2026-10, pending).
- ringline's own measurements cover echo and forward workloads with two
  workers on one pair of hosts. The benchmark's cover more workloads with
  one worker.
- Bundles and zero-copy receive have not been measured.
- A per-worker copy budget for the incremental ring has not been designed.
