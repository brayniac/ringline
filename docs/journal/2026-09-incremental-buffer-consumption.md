# Incremental provided-buffer consumption (`IOU_PBUF_RING_INC`)

- **Status:** open — intent recorded before building, per the journal ground
  rules. **Phase A (2026-09-17) narrowed the case: see "What Phase A did to
  criterion 1".**
- **Span:** 2026-09-17 → (open) · follows #415 (282773b), blocks on #416 Phase A

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
