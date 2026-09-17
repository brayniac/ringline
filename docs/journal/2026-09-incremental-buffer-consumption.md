# Incremental provided-buffer consumption (`IOU_PBUF_RING_INC`)

- **Status:** open — intent recorded before building, per the journal ground rules
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

## GO / NO-GO — written before the data

Proceed to implementation only if **all** of:

1. **#416 Phase A shows the depth/size conflict is real beyond one workload** —
   at least one non-exotic (mode, message size) where the constant-memory policy
   starves at the buffer size that is otherwise fastest. Chunk 1 is one such
   point; one point is an anecdote.
2. **A prototype beats the best fixed geometry** on the forward path by >10% at
   equal memory, or matches it at materially less memory — measured on two
   guests, arms interleaved, three reps.
3. **The copying path does not regress**, since the 2026-07 entry measured that
   bigger buffers hurt it. INC should be neutral there (same bytes, same copies,
   fewer bids) — if it is not, that is a finding worth the entry on its own.
4. **The bid lifecycle rework passes the existing invariants**: no double
   replenish, no leak, correct close-drain with a partially consumed bid, and
   the Mode A hold cap still bounds one slow forward.

Abandon and record if the prototype cannot hold invariant 4 without making the
recv path materially harder to reason about. A 10% throughput win is not worth
re-opening the class of bug that #236–#244 and #415 spent their time closing.

## Plan

1. Land #416 Phase A; read the surface for the criterion-1 evidence.
2. Prototype behind a runtime probe, no config surface yet: INC ring, `F_BUF_MORE`
   handling, partially-consumed bid state, replenish only on final completion.
3. Measure against Phase A's best fixed geometry on the same harness.
4. Either: design doc + criterion E in the 2026-07 entry + implementation PR; or
   NO-GO here with the mechanism recorded.

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
- **Is the win just "bigger buffers" in disguise?** If Phase A shows a deep ring
  of large buffers (constant-count, 256 MiB/worker) is affordable in RSS terms —
  the 2026-07 entry's finding that big buffers cost virtual, not resident,
  memory — then the cheap answer may be to raise the memory budget rather than
  rework the recv path. Measure RSS at 512 connections before assuming INC is
  the only route.
