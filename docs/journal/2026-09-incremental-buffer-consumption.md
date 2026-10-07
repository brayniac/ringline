# Incremental provided-buffer consumption (`IOU_PBUF_RING_INC`)

- **Status:** open — design chosen, not built. **2026-10-07: GO on the
  owner's decision, after the measurements in "2026-10: measurements".
  Design: `docs/recv-incremental-ring-design.md` (#622).**
  Phase A (2026-09-17) had narrowed the case: see "What Phase A did to
  criterion 1".
- **Span:** 2026-09-17 → (open) · follows #415 (282773b), #416 Phase A ·
  measurements on branch `exp/recv-strategies`

## Goal

Break the link between **payload per completion** and **ring depth**.

Today those are one knob. A provided buffer ring is `count × size` at a fixed
memory budget, so making buffers bigger makes the ring shallower, and ring depth
is what bounds concurrent arrivals. Every workload therefore gets to pick which
of the two it wants to be wrong about.

`IOU_PBUF_RING_INC` (Linux **6.12**) lets one buffer be consumed incrementally:
each completion for a given bid continues where the previous one left off, the
CQE carries `IORING_CQE_F_BUF_MORE` while the buffer is still live, and the bid
returns only when it is fully consumed or errors. One large buffer then serves
many small arrivals — large payload per completion *and* deep effective
concurrency, from the same memory.

## Background: why this is on the table now

**#415 (282773b) measured the size axis and found it worth 25%.** A forwarding
proxy across two X710 guests at 40 GbE, one worker pair so the proxy is the
bottleneck, arms interleaved, medians of three:

| arm | Gbit/s | instr/byte |
|---|---|---|
| io_uring, 16 KiB buffers (the default) | 11.2 | 1.26 |
| mio, copy path | 12.8 | 1.12 |
| io_uring, 64 KiB buffers | 14.0 | 0.96 |

Solving the two io_uring points for a fixed per-completion cost gives **~6,600
instructions per completion cycle** — CQE decode, hold push, task wake, future
poll, write SQE, write CQE, bid replenish — against a **~0.86 instr/byte** floor
that is the kernel's own copy and TCP work. At 16 KiB that fixed cost is 32% of
all work; at 64 KiB, 10%. Bigger buffers are how Mode A gets cheap.

**#416 Phase A, chunk 1 measured the depth axis and found the other edge.**
Experiment `01a0b0ac-f3c7-714b-fa95-51e1f8a0f3bf`, 256 B echo, 64 connections:

| geometry | MiB/worker | ops/s | `buffer_ring_empty` | `recv_parked` |
|---|---|---|---|---|
| 1 MiB × 4 (constant memory) | 4.0 | 279,265 | 3,066,467 | 3,066,467 |
| 1 MiB × 256 (constant count) | 256.0 | 297,888 | 0 | 0 |
| 16 KiB × 256 (today's default) | 4.0 | 290,769 | 0 | 0 |

The 1 MiB buffer is not what hurt — the same buffer with depth held starves zero
times and is the *fastest* arm in the group. What hurt was the **four-deep ring**
that 1 MiB implies at a 4 MiB budget. That is exactly the constraint INC removes,
and it is measured rather than argued.

(Those counters are readable at all only because of #417, which added
`bench-server --metrics-out`; nothing in the workspace had ever read
`BUFFER_RING_EMPTY` or `RECV_PARKED` back out.)

## The honest ledger: a prior negative result I cannot cite

I have a recollection of an earlier 200 GbE sweep that evaluated INC alongside
adaptive sizing and found **INC not a win**. That result is **not checked in
anywhere in this repo** — `grep -rn "IOU_PBUF_RING_INC" docs/` finds only two
forward references in `docs/tls-unbuffered-design.md:607` and the Mode C
ordering notes in `docs/segmented-recv-design.md`, none of them a measurement.
Per the journal's ground rules, an uncheckable figure is stated as such: treat
"INC was already tried and lost" as **unverified** until either the data is
found or it is re-measured here.

What *is* on the record is the adjacent NO-GO: **self-adaptive recv buffering**
(`docs/journal/2026-07-self-adaptive-recv-buffering.md`, 2026-07-21), which
measured that (1) big buffers cost virtual and not resident memory, (2) the
discard path runs at line rate on the default 16 KiB ring, and (3) on the
**copying** path 16 KiB ≥ 256 KiB, because there a bigger buffer means a bigger
`memcpy`. That entry retired *adaptive* sizing — geometry that reacts to
observed traffic — and its reasoning does not cover INC, which changes how one
buffer is consumed rather than how buffers are chosen.

Its re-evaluation criteria are A (faster NICs), B (resident memory pressure),
C (a mixed workload where a fixed size loses on a gather path), D (a
mandatory-hold zero-copy consumer that starves). **None of them is what #415
found.** Mode A is not starving and not memory-bound; it is paying a fixed
per-completion cost that only a larger payload per completion amortises. If this
effort proceeds, it should add a **criterion E** to that entry rather than
quietly contradict it.

## What Phase A did to criterion 1

Two saturated passes (#416, 2 workers, 64 vs 1024 connections, 108 arms)
measured the gap between the shipped default and the best geometry chosen with
hindsight at the same memory budget — the "tuning burden" criterion 1 asks
about. The homogeneous version of that gap is now known:

| workload | default vs best-with-hindsight |
|---|---|
| echo, 5 message sizes, both fan-ins | −4% to −11% (one −19%) |
| **forward stream, 64 conns** | **−34%** |
| **forward stream, 1024 conns** | **−18%** |

**This is narrower than the case this entry opened with, and the entry should
say so.** For request/response traffic the default is within ~6% of the best
tuned geometry, so there is little burden for INC to remove — criterion 1's
">10% throughput" bar is not met there. The case now rests on **forwarding and
streaming**, where the gap is 18–34% and systematic across both fan-ins.

What Phase A *did* confirm is the mechanism, in a stronger form than expected.
The optimum moves along **two** axes in opposite directions — message size pulls
toward bigger buffers (payload per completion), fan-in pulls toward deeper rings
(concurrent arrivals) — and at fixed memory those trade directly. Six different
geometries win the twelve workloads; 1 MiB × 4 is the outright winner on four of
them and 83% down on another, with a **1.03-second p99** at 256 B × 1024
connections. So "you cannot pick one geometry" is established. What is *not*
established is that the resulting loss is large enough to justify a recv-path
rework, except for forwarding.

Criterion 1 therefore stands, with its target changed: measure the oracle gap on
a **mixed** workload **on the forwarding path**, where the homogeneous gap is
already 18–34%, rather than on request/response where it is ~6%. If a mixture
does not exceed the worst of its components there, this should be recorded
NO-GO.

## Why this is not a flag flip

Ringline's provided-buffer model assumes **one bid is one discrete segment**,
and INC makes a bid live across many completions. The assumption is load-bearing
in at least these places:

- `pending_replenish` returns a bid on completion. Under INC that must not
  happen while `F_BUF_MORE` is set.
- `segment_hold` holds one `HeldRecvBuf::Pinned { bid, len }` per segment, and
  Mode A writes exactly one held buffer per serialized write
  (`ForwardToFuture::poll`).
- The "**exactly one replenish per bid**" invariant
  (`docs/segmented-recv-design.md:307`) is enforced by a single-release
  discriminant across guard drop, `into_owned`, the Mode A write CQE, and
  `close_connection`.
- `close_connection` drains held bids; a partially consumed bid has no current
  representation.
- The design doc already names the hazard in user space: force-replenishing a
  bid a consumer still holds is "kernel DMA over live consumer data — the INC
  bug in user code" (`docs/segmented-recv-design.md:223`).

So the work is a recv-path rework with a new bid state (*partially consumed,
not returnable*), not a register-flag change.

## Kernel floor: runtime probe, not a build gate

`ringline/build.rs` emits `has_io_uring` at **6.1+** (`SendMsgZc` and
`DEFER_TASKRUN`). Gating INC at build time would drop every 6.1–6.11 host, which
is not an acceptable trade for an optimisation. It has to be a **runtime probe**:
register the ring with `IOU_PBUF_RING_INC`, fall back to a plain ring on
`EINVAL`. One extra register call per worker at startup, and it tests the actual
capability rather than parsing a version string — the same reason the existing
`Error::RingSetup` path checks behaviour rather than `uname`.

The rack can measure this today: the anvil guests run **6.12.63**.

## The criteria this started with were framed wrong

The first version of this entry asked INC to win on peak throughput for a
**tuned, homogeneous** workload. That is the case INC is *least* likely to win:
if you know the message size and can pick the geometry for it, a fixed ring
already gets the payload-per-completion you want. Owner's framing, 2026-09-17,
and it is the right one: the value of INC is **robustness without tuning**, and
mixed traffic is where fixed geometry actually hurts.

The mechanism, stated so it can be falsified:

- Size the buffer for the large messages and, at a fixed memory budget, the ring
  gets shallow — so the *small* messages starve on depth. Measured: 3,066,467
  starvations at 4-deep (Phase A chunk 1).
- Size it for the small messages and the large ones fragment across many
  completions — so the per-completion cost multiplies. Measured: 4 KiB buffers
  cost 34% of the forward path's throughput against 16 KiB (14.52 vs 21.99
  Gbit/s, Phase A chunk 4), and #415 measured the same axis worth 25% between
  16 and 64 KiB.
- INC serves many small arrivals from one large buffer, which is both.

A homogeneous sweep cannot see this, because each arm gets to be tuned for its
own single size. **Phase A is entirely homogeneous** — one `--msg-size` per arm
— so its flat copy-path surface is evidence about tuned workloads only and says
nothing about the mixed case. The 2026-07 NO-GO's GO gate was itself "a mixed
small+huge workload showing fixed-buffer waste", and it went unmet in part
because nothing was measured there. Repeating that omission would reach the same
non-answer by the same route.

## GO / NO-GO — revised

Proceed to implementation only if **all** of:

1. **A mixed workload costs a fixed geometry something an oracle would not
   pay.** Run a size distribution (small + large) against: (a) the geometry an
   operator would pick *without* knowing the distribution — the shipped default;
   (b) the best fixed geometry chosen *with hindsight*, by sweeping it; and
   (c) the worst plausible misconfiguration. The gap between (a) and (b) is the
   tuning burden INC would remove, and it has to be worth removing — call it
   >10% throughput or >20% p99 — or there is nothing here.
2. **A prototype closes most of that gap without being tuned**: at the default
   memory budget, within a few percent of (b) while beating (a), on a
   distribution it was not configured for.
3. **Neither path regresses when homogeneous.** The copying path in particular:
   the 2026-07 entry measured bigger buffers hurting it, and Phase A measured
   buffer size as flat for it across a 64× range. INC should be neutral there —
   same bytes, same copies, fewer bids. If it is not, that is its own finding.
4. **The bid lifecycle rework passes the existing invariants**: no double
   replenish, no leak, correct close-drain with a partially consumed bid, and
   the Mode A hold cap still bounds one slow forward.

Abandon and record if (1) is small. A tuning burden nobody is paying is not a
problem, however elegant the mechanism that would remove it.

Abandon and record if the prototype cannot hold invariant 4 without making the
recv path materially harder to reason about. A 10% throughput win is not worth
re-opening the class of bug that #236–#244 and #415 spent their time closing.

## Plan

0. **Mixed traffic needs no new client code.** `bench-client` has no size
   distribution — `--msg-size` is scalar, and its op accounting counts bytes
   (`(recorded + 1) * msg_size <= total_read`), which only works for a uniform
   size — but the property that matters is *one ring serving two size
   populations at once*, and two concurrent client processes against one server
   produce exactly that. It is also the better experiment: each population
   reports its own throughput and p99, so the table shows **which** population
   pays. An aggregate would hide the failure mode worth finding, which is the
   small messages starving while the large ones hold the ring.

   What it does not exercise is *within-connection* size variation. The ring is
   shared per worker and sees arrivals from every connection, so across-connection
   mixing is what reaches it either way; a per-operation mix would test the
   consumer's framing, not the ring's geometry.
1. Land #416 Phase A; read the homogeneous surface, which bounds criterion 3 but
   cannot decide criterion 1.
2. Measure the oracle gap: default vs hindsight-best vs misconfigured, on a
   mixed distribution. **This is the go/no-go.**
3. Only if the gap is worth it: prototype behind a runtime probe, no config
   surface — INC ring, `F_BUF_MORE` handling, partially-consumed bid state,
   replenish only on final completion.
4. Either: design doc + criterion E in the 2026-07 entry + implementation PR; or
   NO-GO here with the mechanism and the measured gap recorded.

## Open questions

- **Does INC compose with Mode A at all?** A forward writes one held buffer per
  write. Under INC the "buffer" is a moving window inside one bid, so either
  Mode A writes sub-ranges of a live bid (and the bid outlives many writes), or
  it copies out. The second defeats the purpose.
- **What does the hold cap bound under INC?** `forward_hold_cap` counts held
  buffers as a proxy for pinned ring capacity. With one bid serving many
  arrivals the count stops meaning that.
- **Does `F_BUF_MORE` interact with the ENOBUFS park/fallback path?** The
  fallback recv (#274) exists because a small ring starves; INC changes what
  "small" means, and #415 had to exclude segmented connections from the fallback
  entirely.
- **Is the win just "bigger buffers" in disguise?** If a deep ring of large
  buffers (constant-count, 256 MiB/worker) is affordable in RSS terms, the cheap
  answer may be to raise the memory budget rather than rework the recv path.
  But the affordability claim needs re-testing rather than inheriting, and the
  mechanism matters:

  The provided ring's backing is a plain `vec![0u8; ring_size * buf_size]`
  (`backend/uring/provided.rs:54`) — **not** `mlock`ed, no `MAP_LOCKED`, no
  `MAP_POPULATE`, and not charged to `RLIMIT_MEMLOCK` (that limit covers
  *registered fixed buffers* from `ConfigBuilder::registered_regions`;
  `register_buf_ring` pins only the ring structure, entries × 16 B). So
  over-provisioning costs virtual address space up front, exactly as the
  2026-07 entry found.

  The catch is that `alloc_zeroed` pages become resident **when touched, and
  stay resident**. The 2026-07 measurement (~11 MB RSS for a 256 KiB-buffer
  ring) was taken on a steady workload: it is a statement about what that
  benchmark touched, not a guarantee about what production touches. A burst that
  reaches every buffer converts the whole ring to RSS — 256 MiB/worker, 2 GiB
  across 8 workers, permanently. Over-provisioning to avoid tuning is therefore
  cheap under steady load and expensive under bursty load, which is the case an
  operator cannot predict and the reason this question is not settled by the
  earlier number. Measure RSS *and* peak-touched pages under a bursty mixed
  workload before treating a deep ring as free.

## 2026-10: measurements

The recv redesign (#622) first proposed per-connection receive memory (each
connection's bytes land in memory it owns). Before deciding, the owner asked
for measurements of every candidate. This section records them and how they
were read.

### Method

`experiments/recv-strategies/` (branch `exp/recv-strategies`) is a standalone
io_uring program, not ringline: a single-threaded server pinned to one CPU,
`DEFER_TASKRUN`, length-prefixed messages, and a client that counts acked
messages and records latency. Each receive strategy is a small module
written directly against io_uring:

| Strategy | Receive model |
|---|---|
| `shared` (or `plain_*`) | ringline today: one plain provided-buffer ring per worker, parse in place when nothing is buffered, copy otherwise, fallback one-shot recv on `ENOBUFS` |
| `inc_*` | the same ring registered with `IOU_PBUF_RING_INC` |
| `ring` | a one-entry INC ring per connection over the connection's region; moves rewrite the posted entry in place |
| `ring_norewrite` | the same, never touching a posted entry |
| `oneshot` | one-shot `RECV` into the connection's region |

`--adapt` (added 2026-10-07) grows a per-connection region when its posted
range is used up while data flows, and shrinks it after it stays empty.
`--verify` stamps every message with its connection's sequence number and a
derived payload, and the server checks every byte. `--hold-every` /
`--hold-us` / `--lend-cap` make every second connection keep each received
range lent for a time, as a forward to a slow sink does, with a cap on how
much of the ring held lends may pin.

Two setups, both on Linux 7.1.13 (the backports image):

- **hv01**: one VM (`z2.c`, 56 vCPU), server and client in it over
  loopback, which delivers bytes in bursts at memory speed.
- **Two hosts**: server VM on hv02 (`z1.c`), client VM on hv01, over each
  guest's 4 × 10 GbE 802.3ad bond (layer3+4). A calibration run
  (`01a114e0-077e-7100-6a96-ee2f7660185b`) found NIC interrupts and NET_RX
  softirq spread over all 24 server vCPUs (5% on the server's CPU) and eight
  servers reaching 26.6 Gb/s, so one server's core is bounded by its own
  work: these numbers are one worker's steady-state cost at about 1.5 GB/s.

Five reps per cell, configurations interleaved within each rep; medians
below. An A/A pair (the same configuration twice) agreed within 1–2% on
hv01.

**Superseded.** The first loopback runs (v1 `01a1121f`, v2 `01a11274`, v2b
`01a112fc-f7ea`, the pool sweep `01a1134e-f20c`, the plain-ring and lend-cap
sweeps `01a114ba-bb57` and `01a114ba-bbf0`) ran on the validation host,
delta, which also runs frigate and the infra VMs. Their A/A spreads reached
19.5% and the same configuration moved 25% between experiments; two of
their findings did not reproduce on hv01 (today's ring starving at 10,000
connections, and today's ring winning streaming). They are not used below.
Two bugs in the benchmark also invalidated v2 cells (a CQ larger than
`IORING_MAX_CQ_ENTRIES`, and an accumulator that never compacted under
streaming), and its framing checked lengths only until `--verify` was added.

### Results

Core comparison, hv01 (`01a11520-5424-7143-8c86-731a6e6738bb`), msg/s:

| Workload (conns) | Today 256 × 16 KiB | INC 64 × 64 KiB | INC 64 × 1 MiB | `ring` | `oneshot` |
|---|---|---|---|---|---|
| 256 B (64 / 1000 / 10k) | 90k / 83k / 47k | 92k / 85k / 46k | 90k / 84k / 47k | 92k / 83k / — | 95k / 51k / 43k |
| 64 KiB (64) | 37.3k | 48.8k | 43.0k | 47.9k | 51.3k |
| 64 KiB (1000) | 32.0k | 44.5k | 38.1k | 38.0k | 28.8k |
| 64 KiB (10k) | 29.8k | 39.5k | 34.8k | — | 30.7k |
| 1 MiB (64) | 3.2k | 3.3k | 5.7k | 5.4k | 5.4k |
| mixed (1000) | 61.0k | 65.2k | 74.0k | 76.9k | 47.2k |
| pipelined mixed (1000) | 199k, p99 76 ms | 209k | 258k, p99 33 ms | 166k | 118k |
| stream, mixed sizes (1000) | 110k | 133k | 128k | 85k | 88k |

`ring` at 10k connections did not start: on 7.1 each one-entry ring is
charged a page of `RLIMIT_MEMLOCK`, and 2041 fit under the default 8 MiB.

Geometry, two hosts (`01a11520-54ad-71fa-9dce-bc7e913ed435`), msg/s:

| Workload (conns) | Today | INC 4 MiB / 64 KiB | INC 16 MiB / 1 MiB | INC 64 MiB / 1 MiB | `ring` |
|---|---|---|---|---|---|
| mixed (1000) | 79.2k | 80.2k | 85.0k | 85.7k | 84.6k |
| 64 KiB (1000) | 19.9k | 21.6k | 23.5k | 23.6k | 24.0k |
| 1 MiB (64) | 1.6k | 1.6k | 1.7k | 1.7k | 2.1k |
| 256 B (10k) | 106.7k | 107.0k | 107.1k | 107.0k | 105.8k |
| stream 16 KiB (64) | 109.6k | 108.6k | 110.5k | 114.2k | 83.1k |
| stream 16 KiB (1000) | 93.9k | 103.6k | 96.9k | 102.0k | 73.2k |

Geometry, hv01 (`01a11520-55d2-7179-7c79-b2a7536f50c1`): a 16 MiB pool of
1 MiB buffers did 6.3k at 1 MiB × 64 and 368k streaming at 64 connections
(64 MiB: 5.7k and 300k), and 214k streaming at 1000 (64 MiB: 234k).

Streaming alignment, hv01 (`01a11520-5543-7171-7461-ba465491f200`). INC
beat today's ring in every streaming cell, at 12000 B, 16384 B, 20000 B and
mixed sizes, 64 and 1000 connections: by 8–45%, at 10–30% less CPU per KiB.
Today's ring parsed 99% of bytes in place only when the message size equalled
its 16 KiB buffer and the ring kept running dry (16384 B × 1000), and 0–4%
otherwise.

Plain rings for kernels without INC, hv01 and two hosts
(`01a11520-56b7-71f4-ce93-6f895c7992ed`, `01a11520-5644-7109-7752-ad578a83da4d`),
msg/s:

| Workload (conns) | 256 × 16 KiB | 1024 × 16 KiB | 4096 × 16 KiB | 1024 × 64 KiB | INC 64 × 1 MiB |
|---|---|---|---|---|---|
| hv01 mixed (1000) | 58.4k | 60.5k | 56.1k | 73.5k | 73.6k |
| hv01 64 KiB (1000) | 32.3k | 28.9k | 28.4k | 38.9k | 38.3k |
| hv01 64 KiB (10k) | 29.6k | 27.0k | 27.2k | 35.1k | 34.8k |
| hv01 1 MiB (64) | 3.3k | 2.7k | 2.2k | 2.9k | 5.7k |
| hv01 stream 16 KiB (1000) | 216.6k | 187.4k | 180.6k | 215.4k | 234.0k |
| two hosts mixed (1000) | 79.0k | 79.2k | 78.6k | 86.5k | 85.2k |
| two hosts 64 KiB (1000) | 20.0k | 19.8k | 20.0k | 23.6k | 23.5k |
| two hosts stream 16 KiB (1000) | 92.0k | 87.9k | 86.5k | 94.5k | 105.3k |

Small messages were the same in every column.

Lend cap, INC 64 × 1 MiB, every second connection holding each range for
50 ms, 64 KiB × 1000 (hv01 `01a11520-57a3-7131-2c26-f10fce555c09`, two
hosts `01a11520-5730-7182-d5f0-6a93de5e30e7`): no cap 20.1k (19.9k); caps
0–0.75 between 36.6k and 38.0k (22.5–22.8k); no holds 38.2k (23.2k). On
4 KiB and mixed traffic, and with 5 ms holds, every setting was within the
noise.

Per-connection rings with adaptive regions, hv01
(`01a11649-5df3-7153-91b9-ff606706d842`), `ring_norewrite --adapt` (1 MiB
cap) against INC 64 × 1 MiB: streaming at 1000 connections +21% (16 KiB) and
+26% (mixed sizes); streaming at 64 connections −6%; pipelined mixed −8%;
mixed −13%; 64 KiB × 1000 −19%; 64 KiB × 10k −39%; 1 MiB × 64 −11%. Its
regions grew to the cap and stayed: RSS about 1 GiB at 1000 connections on
every workload, 6.2 GB at 10k × 64 KiB. A 256 KiB cap stalled at 1 MiB
messages, which cannot fit.

### Correctness

Every byte verified (`--verify`), Linux 6.12 and 7.1, 1000–10,000
connections, mixed, pipelined and streaming traffic:

- INC on the shared ring (`01a114c0-d1a4`, `01a1150d-01a6`): clean in 186
  runs, including a 64-entry CQ with a 32-entry SQ (overflow and inline
  submits), SQPOLL, SQPOLL with the small queues, and held lends.
- Per-connection rings (`01a11648-ef98`, `01a11648-f03c`, and the bisect
  `01a1166a-fbc5`, `01a1166a-fc47`): `ring`, which rewrites a posted entry in
  place to move its region, delivered wrong bytes with the small queues on
  both kernels, adaptive or not (up to 26 bad messages in five runs).
  `ring_norewrite` was clean in every run, including runs where the kernel
  had written past the reaped bytes.

### Kernel facts on 7.1

`inc-probe` passed every fact on 7.1.13 as on 6.12: contiguous append, the
entry advanced in place, re-posting after `F_BUF_MORE` clears continues the
arm, a posted entry rewritten between enters is honoured, a stale arm writes
into a re-registered bgid, and unregister then data gives `-ENOBUFS`.
`ring-limit`: under the default 8 MiB `RLIMIT_MEMLOCK`, 2041 one-entry rings
registered before `ENOMEM`.

### Reading against GO / NO-GO

1. **Tuning burden on a mixed workload.** Not measured as an oracle gap. On
   mixed traffic INC 64 × 1 MiB beat today's ring by 21% on hv01 and 8%
   across hosts, and the best plain geometry (1024 × 64 KiB) matched it, so
   much of that is buffer size rather than INC.
2. **Closing the gap untuned.** INC 64 × 1 MiB beat today's ring or matched
   it in every measured cell on both setups; the best fixed alternatives won
   only cells shaped like them (64 KiB buffers at 64 KiB messages).
3. **No homogeneous regression.** None on hv01 or across hosts.
4. **The bid lifecycle.** Not tested; it is the design's subject.

**"Is the win just bigger buffers in disguise?"** Partly. A plain ring of
1024 × 64 KiB matches INC 64 × 1 MiB on 64 KiB, mixed and small traffic.
INC's own gains are at 1 MiB messages (5.7k against 2.9k on hv01) and on
streaming (234k against 215k on hv01, 105k against 95k across hosts). That
plain ring is the design's choice for kernels without INC.

**Decision (owner, 2026-10-06 and 2026-10-07).** One shared ring per worker:
INC 64 × 1 MiB on 6.12+, plain 1024 × 64 KiB below. Per-connection receive
memory was rebuilt with adaptive regions and still not chosen: the variant
that can move a live region corrupts data, and the one that cannot loses
request/response traffic and keeps its grown memory. A hybrid that gives
streaming and forwarding connections their own receive memory is a
follow-up.

**RSS.** The open question above about touched pages staying resident
stands, now with an answer for the ring: a 64 MiB pool is fully resident on
any worker that has received 64 MiB.
