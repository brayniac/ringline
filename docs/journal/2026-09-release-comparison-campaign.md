# Release comparison campaign: ringline vs tokio, io_uring vs mio

- **Status:** open — intent only. No measurements yet.
- **Span:** 2026-09-13 → · PRs TBD · pre-release (post-0.6.4)

Opened **before** the work, per this journal's "land intent before building"
rule. The previous two entries both had to record the opposite
([2026-09-unbuffered-tls.md](2026-09-unbuffered-tls.md),
[2026-09-backpressured-sends.md](2026-09-backpressured-sends.md)); this is the
correction.

## Goal

Refresh the runtime comparison before cutting a release, on the two-guest X710
rig with rezolus instrumentation, so the claims in
[`BENCHMARKS.md`](../../BENCHMARKS.md) and
[`docs/syscalls-and-copies.md`](../syscalls-and-copies.md) are measured on
current `main` rather than inherited.

Three comparisons, not one:

1. **ringline vs tokio** — the published claim.
2. **io_uring vs mio within ringline** — never published; `CLAUDE.md` asserts
   mio should be "correct and non-pathological, not optimal", which is an
   untested claim about how large the gap is.
3. **ringline default vs `--recv-forward`** — the zero-copy recv path, which a
   prior bare-metal run found was the entire difference at 256 B.

## Why this is not just a re-run

Two recorded findings make a naive re-run misleading, and both are the reason
the axes below are what they are.

**The connection-scaled latency penalty is unresolved.** On this rig, in the
latency-bound regime with idle CPUs, ringline's *server* is 25–38% slower than
tokio, and the added latency scales with connection count (~2.5 µs × N): 1
conn identical, 64 conns +0.15 ms, 512 conns +1.3 ms. It is a property of the
server event loop — a runtime-cross 2×2 showed the client runtime is
irrelevant. Two fix hypotheses (incremental flush, `tick_timeout_us` sweep) are
already refuted by clean A/Bs. The working model is
`latency ≈ (conns_per_worker / CQEs_per_iter) × iteration_latency`.

`BENCHMARKS.md` fixes connections at 128 and therefore cannot see this. A
campaign that does the same would publish a flattering number over a known
weakness. **Connection count is a first-class axis here for that reason**, and
rezolus can test the model directly (CQEs per iteration, syscalls per op) rather
than inferring it.

Nothing has re-measured this since the send-path series (#369–#389) landed,
which changed the send path substantially.

**The harness is not currently fair.** Three gaps, all of which must be fixed
before any number is trusted:

| Gap | Where | Consequence |
|---|---|---|
| tokio server uses `read_exact(msg_size)` — one read syscall per message | `ringline-bench/src/bin/server.rs:285` | Previously manufactured a spurious ~9.6× ringline "win". Fixed on the unmerged `bench/fair-throughput`; never landed on `main`. |
| client hardcodes `send_pool(32768, …)` | `ringline-bench/src/client.rs:578` | At 4096 connections that is 8 slots/conn; the client collapses to 33k ops/s and the arm measures a client limit, not the server. |
| no closed-loop pipeline depth | `ringline-bench/src/bin/client.rs` | `--max-inflight` covers open loop only, so the syscall-amortization claim cannot be tested in closed loop. |

With the fairness fix applied, the previously measured fair 256 B single-core
picture was tokio 2.08M, ringline io_uring 1.67M, ringline mio 1.41M, ringline
io_uring `--recv-forward` 2.04M — i.e. **fair tokio beat ringline-default by
~20%**, and ringline only drew level using zero-copy recv-forward. That is the
number this campaign has to either reproduce or overturn.

## Rig hazards to gate on

An entire previous bare-metal campaign on these hosts was invalidated after the
fact: IRQ and wakeup locality funnelled all work onto one core, capping every
arm at ~208k ops/s regardless of runtime, connection count or loop mode. "All
runtimes are equal" was the *artifact*, not the result.

Every arm is therefore gated before its numbers are used:

- **Per-core utilization** — no single core saturated while siblings idle.
- **Host quiet** — the `check_quiet.sh` guard from the send-path A/B, which
  rejects arms whose host block I/O was not near idle.
- **Little's law** — in closed loop `N = X·R` must hold. If measured
  p50 × throughput ≉ connections, the arm is discarded: that inequality is the
  fingerprint of coordinated omission, a client-side bottleneck, or a stalled
  arm, which is the class of error that produced the withdrawn 2026-05 numbers.

## Plan

Phased, because each phase's output is the next phase's input.

- **Phase 0 — harness + rig validation.** The three fairness gaps above, then a
  validation run whose only job is to show the gates fire: deliberately
  oversubscribe one core and confirm the per-core gate rejects it.
- **Phase 1 — closed-loop peak throughput.** size × connections × server
  config. Produces the saturation point each Phase 2 rate is expressed against.
- **Phase 2 — open-loop latency vs load.** Rates at 10/50/80/95% of Phase 1
  saturation. This is the chart that decides whether "better latency at a given
  load" is true, and the instrument for the connection-scaled penalty.
- **Phase 3 — worker count and pipeline depth.** Tests whether
  connections-per-worker is the real parameter, and whether syscalls/op falls
  with depth as the copy/syscall doc claims.
- **Phase 4 (separate effort) — segcache.** Revive `bench/fair-throughput`
  (28k lines, vendors segcache + cache-core, predates the series) so
  `BENCHMARKS.md`'s own workload can be refreshed. Deliberately after the echo
  campaign: echo answers the scaling questions more cheaply, and the revival is
  a substantial job that should not block them.

## What would change our mind

Stated now so the analysis cannot be steered later:

- If the connection-scaled latency penalty reproduces, `BENCHMARKS.md` gains a
  section saying so. A published comparison that omits a known regime where we
  lose is not an honest baseline.
- If fair tokio still beats ringline-default at small messages, that is the
  headline, and the recv-forward path's status (currently opt-in) becomes a
  design question rather than a footnote.
- If measured syscalls/op or copies/op disagree with
  `docs/syscalls-and-copies.md`, that document is corrected — it makes counted
  claims, and counted claims are falsifiable.
- If the per-core or Little's-law gates reject a large fraction of arms, the
  campaign stops and the rig is fixed first. Measuring harder against a broken
  rig is what produced the last withdrawal.

## Outcome

Open.

## Lessons / open questions

Open.
