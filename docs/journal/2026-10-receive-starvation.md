# Receive starvation at high connection counts, and what holds receive memory

- **Status:** open
- **Span:** 2026-10-12 to 2026-10-14 · #657 (open), #658 (issue) · rig
  experiments on branch `exp/inc-step6-matrix` of the brayniac fork,
  `experiments/recv-inc-matrix/` (not on `main`)

## Goal

Landing step 6 of #622 (`docs/recv-incremental-ring-design.md`) measures
ringline with `recv_incremental` against the 256 × 16 KiB ring. Those runs
found connections that received no response for a whole run, in both
configurations, and found that the incremental configuration's resident
memory grows with connection count and message size. This entry records what
each of those turned out to be.

## Setup

Every rig number here is from the same setup unless a section says
otherwise:

- Server VM on hv02 (shape `z1.c`), load-generator VM on hv01 (`z2.c`), over
  each guest's 4 × 10 GbE bond.
- `bench-server --echo-mode forward` with 2 workers. That mode is `with_data`
  plus `forward_recv_buf`, what a protocol server's read loop does.
- `bench-client`, closed loop, one message in flight per connection, 3 s
  warmup and 12 s measured.
- Kernels: Debian 12 (6.1), Ubuntu 24.04 (6.8), Debian 13 (6.12), and
  Debian 13 backports (7.1).
- TCP receives are one-shot buffer-select receives, the default since #647:
  each connection has one receive in flight, which takes one buffer from the
  ring.

"Unanswered" means a connection that the load generator opened and that got
no response in the measured window. The load generator reports it per run
(`per_conn.zero`), with the minimum, p10, median and maximum responses per
connection.

## What happened

### Unanswered connections are three different failures

The server's metrics (`ringline/connections` `accepted` and
`accept_backlog_dropped`) separate them:

1. Never accepted. `accepted` is short of the connections opened by exactly
   the unanswered count. These are lost in the listen queue, which
   `Config::backlog` sets to 1024 by default.
2. Closed by the acceptor. `accept_backlog_dropped` equals the unanswered
   count. The acceptor calls `accept4` and then drops the connection when
   every worker's accept channel (`accept_queue_capacity`, 1024) is full.
   The peer sees its request answered by EOF. Locally, with one worker and
   10,000 connections at 256 B, 1,955 connections were dropped this way, and
   each one had sent 256 B and read EOF.
3. Accepted and never answered. The receive path never delivered the
   connection's bytes, or never re-armed its receive.

Classes 1 and 2 are tracked in #658. Class 3 is receive starvation.

### Receive starvation before and after #657

Before #657, `flush_replenish_and_rearm` re-armed every parked connection
whenever any buffer was free, and walked the parked list with `swap_remove`,
so the order connections were re-armed in was arbitrary. With the
256 × 16 KiB ring at 10,000 connections, 73 of 80 runs (4 kernels, 256 B and
64 KiB) left 3,000 or more connections unanswered, all of them accepted.

#657 re-arms parked connections oldest first, at most one per free buffer.
Its first commit counted each re-arm against the budget twice; the second
fixes that. With the first commit, 10 runs per kernel and cell (5 boots, two
identical copies of the configuration per boot), 80 runs in all:

| Cell, 256 × 16 KiB ring | Before | After |
|---|---|---|
| 256 B × 10,000, req/s | 6.1–6.12: 39k–47k; 7.1: 42k–164k, median 136k | 6.1–6.12: 88k–124k; 7.1: 162k–165k |
| 64 KiB × 10,000, req/s | 6.5k–8.7k | 12.3k–15.3k |
| Runs with unanswered connections | 75 of 80, most in the thousands | 3 of 80 (75, 1, 340) |

Across those 80 runs and 80 more of the same configuration, 3 of 160 runs
had accepted-but-unanswered connections (1, 54 and 2). The other misses
were classes 1 and 2.

### Starvation at 16,384 connections

A sweep of ring geometry (phase r: 4, 16 and 64 MiB per worker in buffers of
4, 16 and 64 KiB, named `m<MiB>-b<KiB>k`, plus the incremental ring) ran #657's
first commit at 16 to 16,384 connections on 6.12 and 7.1. At 16,384
connections each worker has 8,192.

At 64 KiB × 16,384 on 6.12, two runs each:

| Ring | Entries per worker | req/s | Unanswered | Responses per connection, min / median / max (run with more unanswered) | Ring empty |
|---|---|---|---|---|---|
| m64-b4k | 16,384 | 7.3k, 7.4k | 0, 0 | 5 / 5 / 6 | 0.1M |
| incremental, 64 × 1 MiB | 64 | 23.3k, 23.3k | 0, 0 | 1 / 15 / 151 | 0.09M |
| m64-b16k | 4,096 | 16.1k, 16.4k | 2,258, 4,359 | 0 / 13 / 25 | 2.9M |
| m16-b4k | 4,096 | 7.0k, 6.8k | 5,694, 2,878 | 0 / 5 / 11 | 5.1M |
| m4-b64k | 64 | 15.8k, 16.2k | 0, 4,004 | 0 / 12 / 24 | 0.5M |

Every unanswered connection in these runs was accepted (`accepted` equal to
the connections opened, `accept_backlog_dropped` 0).

- The test is long enough. m64-b4k serves every connection evenly at about
  five responses each, at the lowest rate of any configuration.
- The load generator is not saturated. The incremental ring pushes three
  times m64-b4k's traffic through the same load generator and reaches every
  connection.
- The failure is unfairness. m64-b16k delivers more in total than m64-b4k
  and leaves 27% of connections with nothing, while its median connection
  gets 13 responses.

At 256 B and 4 KiB the pattern is tied to ring exhaustion more cleanly. At
256 B × 16,384, the two 4,096-entry configurations either never emptied the
ring (about 158k req/s, p99 about 0.1 s) or emptied it 3.4M–4.8M times
(108k–119k req/s, p99 0.6–1.6 s), in the same configuration across runs.
Three runs in the second regime left 60, 89 and 1,481 connections
unanswered, all accepted. Configurations that never emptied the ring left
nobody unanswered except through classes 1 and 2.

At 64 KiB the link is weaker: m4-b64k emptied the ring 0.5M times in both
of its runs and left 0 and 4,004 connections unanswered. A connection that
misses its first re-arms appears to stay out for the rest of the run. These
runs used #657's first commit; whether the final version still starves at
16,384 connections is measured by phase d (pending, below).

### Ring geometry

From phase r, 2 workers, at a fixed memory budget per worker:

- Up to 4 KiB messages, entries per connection decide. m64-b4k (16,384
  entries per worker, two per connection at 16,384 connections) never
  emptied the ring and matched the incremental ring: 127.6k against 127.8k
  req/s at 256 B on 6.12.
- At 64 KiB and above, buffer size decides. 64 KiB buffers came within
  about 10% of the incremental ring at 64 KiB, with about a fifth of its
  memory at 16,384 connections. 4 KiB buffers cost 16 completions per
  message: m64-b4k ran 7.3k req/s at 64 KiB × 16,384 against 23.3k.
- No fixed geometry was best at every message size.
- At 1 MiB × 16,384 every configuration left most connections unanswered,
  the incremental ring included. Throughput was 0.6k–1.1k req/s, under one
  response per connection in 12 s even if shared evenly, so that cell is too
  short to judge.
- At 16,384 connections the same configuration varied by up to 1.5× in
  throughput between runs when it emptied the ring (4 KiB on 7.1: m4-b16k
  71k and 111k). Configurations that did not empty it varied by under 10%.

### Incremental ring memory

The incremental configuration's peak RSS grew with load: on 6.12 at
64 KiB, 291 MiB at 256 connections, 661 MiB at 4,096 and 1.5 GiB at 16,384;
at 1 MiB, 1.7 GiB at 1,024 and 5.6 GiB at 4,096. The fixed rings stayed at
about 170–300 MiB in every cell. The ring is not the cause: it is 64 MiB per
worker.

With `recv_incremental` a completion is lent only while at most half the
ring's buffers have a hold (`Driver::may_lend`, 32 of 64). A forward holds
its buffer until the send completes, so above about 32 forwards in flight
per worker every arrival is refused a lend and copied into the connection's
accumulator. `forward_recv_buf` then sends the whole accumulator zero-copy
(`take_frozen` under an `AccumulatorGuard`), and the copy stays allocated
until the zero-copy notification. Each active connection then holds one to
two messages on the heap. Nothing bounds the total except
`recv_accumulator_max` per connection (1 GiB by default).

Measured locally (one worker, Linux 7.2, arm64, loopback):

| Cell | Lends refused | Peak RSS |
|---|---|---|
| 256 × 16 KiB ring, 64 KiB × 2,048 and 1 MiB × 512 | 0 | 151 MiB |
| incremental, 64 KiB × 2,048, three runs | 0 | 151 MiB |
| incremental, 64 KiB × 2,048, one run | 1,637 | 251 MiB |
| incremental, 1 MiB × 512, four runs | 10,591–32,055 | 862–920 MiB |

RSS rose only in runs that refused lends. On the rig, the cells where it
grew are the cells that refused lends; the 4 KiB × 16,384 cell refused
44k–76k lends with flat RSS, because each copy there is 4 KiB.

The memory also stays after the connections close. After every connection
of a 1 MiB × 512 run closed, RSS was 888 MiB. Accumulators are reset when a
slot is reused (`install_accepted_with_pending`), not when a connection
closes, so each closed slot keeps its allocation. Freeing the accumulator at
close (an experimental patch) brought RSS after close to 344 MiB. The rest
is glibc's arenas: with `MALLOC_MMAP_THRESHOLD_=131072` as well, it was
181 MiB.

The fixed rings never refuse a lend (they have no lend cap). When they run
out, the receive gets `ENOBUFS`, the connection parks, and TCP holds the
bytes in the socket.

### Test artifacts found on the way

- At 16,384 connections, some cells lost 800–1,034 connects to
  `EADDRNOTAVAIL` on the load generator. The load generator closes first, so
  it holds the TIME_WAIT sockets (checked on loopback: 2,001 TIME_WAIT
  sockets on the load generator's side, none on the server's). The phase r
  and phase d specs do not set `tcp_tw_reuse`, and Debian's default (2)
  reuses only on loopback. Four configurations of 16,386 connections in a
  row need 65,544 ports against 64,512 in `ip_local_port_range`, 1,032
  short, which matches the failures; they fell on every fourth
  configuration in a boot. Runs at 10,000 connections are not affected.
- A connection the server never accepted, or that the acceptor closed, looks
  the same to the load generator as a starved one. Check `accepted` and
  `accept_backlog_dropped` before attributing an unanswered connection to
  the receive path.

## Outcome

- #657 (fair re-arm) is open. Its merge waits for phase d, which runs the
  final version with a dump of every open connection's state at shutdown:
  10 boots at 10,000 connections on 6.8, 4 at 64 KiB × 16,384 and 6 at
  256 B × 16,384 on 6.12 and 7.1.
- #658 tracks the accept-path losses. No design is chosen.
- The incremental ring's copy path has no memory bound, and accumulators
  outlive their connections. Both need fixing before `recv_incremental` is
  on by default (step 7 of #622).

## Open questions

- Does the final version of #657 still starve connections at 16,384
  connections, and in which state are they (parked, armed with no buffer,
  or something else)?
- Why does starvation at 64 KiB not track the number of times the ring
  emptied?
- Should a refused lend copy at all, or should the receive wait for a buffer
  instead, as the fixed rings do? A copy keeps the connection moving at the
  cost of unbounded memory; waiting bounds memory at the cost of the
  connection's latency.
- Phase r ran one or two runs per cell, which cannot rank the fixed rings
  against each other at 16,384 connections.
