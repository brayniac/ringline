# 2026-09 — Two-phase park: induce quiescence instead of sampling for it

- **Status:** open — phase 1 shipped and GO; phase 2 not started
- **Span:** Sep 2026 · follows #443 tier 3, #470, #477, #479

## Goal

Make park work in the case it was built for, and then make it fire in the case
that actually hurts. Two independent changes, in order, each with its own number
to hit.

The measurements that motivate this are in
[park-abandonment-design.md](../park-abandonment-design.md) and
[2026-09-tier3-park.md](2026-09-tier3-park.md). In short: park converges a
manufactured imbalance and costs nothing when idle, but

- **94% of started parks are abandoned** on `ParkBlocker::NotOffered` — the
  offer was withdrawn by bytes arriving during the install round trip, and the
  handler had already consumed those bytes by the time the CQE was handled
  (`data_pending` is only 6%, so the accumulator was *empty*);
- the policy compares **connection counts**, so six saturating connections per
  worker against four idle workers produced **zero** parks;
- and consequently park cannot help the one shape that matters — a worker pinned
  by a few expensive pipelined connections.

Removing the tier was considered on that evidence. The reason not to is that both
failures are design choices rather than structural limits, and the first one is
cheap to fix.

## Phase 1 — two-phase park

A pipelined connection is not inherently unquiescable. It is unquiescable *while
it is still being fed*. Cancel the armed recv **and keep it cancelled**, and
nothing more can reach the accumulator, so what remains is a bounded amount of
work: the handler drains it, responds, and reaches a genuine quiescent point.

"And keep it cancelled" is doing the work in that sentence. See the amendment
below — the runtime re-arms on `ECANCELED` today, so the premise does not hold
without a change.

Today `begin_park` links the recv-cancel immediately ahead of the
`FIXED_FD_INSTALL`, so the install CQE lands behind the cancel with no interval
in which a drain could happen, and `handle_park_install` abandons rather than
waits. Park samples for quiescence; it never induces it.

The change:

1. Cancel the armed recv, **and suppress the `ECANCELED` re-arm** for this
   connection. Do **not** link the install to the cancel. Mark the connection as
   draining for park.
2. No new bytes can arrive, so the handler drains the buffered pipeline and
   re-offers at its next idle point — with *fresh* state.
3. On that offer, submit the install. It cannot lose the race, because nothing
   can arrive to withdraw the offer.

Waiting for the re-offer, rather than un-clearing the withdrawn flag, is what
keeps this clear of the rejected "carry unconsumed buffers" design: that one
shipped state deposited *before* a request which was then in flight. Here the
handler processes those requests and re-deposits, which is what the offer
protocol already means.

**GO/NO-GO.** Rerun the saturated case from park-abandonment-design.md
(600k open loop, four workers pegged). Currently `not_offered` is 94.1% of
abandonments and completion is 1.6–6.4%.

- **GO** if `not_offered` falls below 20% of abandonments *and* completion
  exceeds 50%.
- **NO-GO** if completion does not improve, or if `data_pending` simply replaces
  `not_offered` — that would mean the drain never reaches empty and the premise
  is wrong.

**The assumption was checked before building, and it was false.** The plan above
says "no new bytes can arrive" once the recv is cancelled. That is not true
today: the `ECANCELED` branch of the recv handler calls
`rearm_multishot_if_idle` unconditionally (`event_loop.rs:1383`), so a cancelled
recv is re-armed immediately and data keeps flowing. The comment there explains
why it exists — without it a connection "sat `Open`/`Multi` with no recv armed
and its bytes piling up in the accumulator, forever."

Two consequences.

First, the drain window cannot open without an amendment: **the `ECANCELED`
re-arm has to be suppressed while a park is draining**, and the abort path has to
re-arm on the way out. That suppression is the linchpin of phase 1; nothing else
in the plan works without it, and it is the first thing to build.

Second, it sharpens the diagnosis of the 94%. The cancel in today's park is
close to a no-op for quiescence, because the recv returns immediately — so the
install is racing a *live* recv, not a cancelled one. The withdrawn offer is the
gate that reports, but the re-arm is why data keeps arriving at all.

This was established by reading the control flow rather than by measurement,
which is decisive for a question of this shape — it is a branch that either runs
or does not, not a timing property. The timing properties below still need
measuring.

### Phase 1 outcome: GO

Built in two steps, because the first was necessary and not sufficient.

**Step 1 — suppress the `ECANCELED` re-arm.** Measured on its own: the
suppression fired on every single park (9,809 of 9,809) and changed nothing.
`not_offered` stayed at 95.2%, completion 9.12% against a 50% bar. NO-GO.

Four diagnostic counters said why, and it was not a hypothesis anyone could have
reasoned to:

```
cancel_submitted         9809    every park
cancel_absent               0
rearm_suppressed         9809    suppression fires 100%
withdraw_while_draining  9427    == every single abandonment
```

A cancel cannot retract recv CQEs the kernel has **already posted**. Those
deliver, withdraw the offer, and the install — still linked behind the cancel —
lands with nothing standing. The accumulator drains fine (`data_pending` 2.9%);
it is the *offer* that in-flight data destroys.

**Step 2 — unlink the install and submit it on the re-offer.** `begin_park` now
submits only the cancel and enters a `ParkDrain` state; `drive_park_drains`
submits the install once the connection is parkable and offered again, by which
point nothing can be in flight.

| | baseline | step 1 | step 2 |
|---|---|---|---|
| completion | 0.76–6.36% | 9.12% | **100.00%** (230/230) |
| `not_offered` share | 94.1% | 95.2% | **0.0%** |
| `park_started` | ~19,415 | 4,079 | **230** |
| abandonments | ~19,268 | 3,707 | **0** |

Both GO criteria met. `loaded_cores` still converges 4 → 8, and the retry storm
is gone: 230 attempts for 230 moves where it previously took 19,415 for 264. The
policy reaches its setpoint and stops, which is what the sub-saturation runs
looked like all along.

The counters confirm the mechanism rather than just the outcome:
`install_after_drain` = 230 (every park took the new path),
`withdraw_while_draining` = 136 (in-flight CQEs still arrive on 59% of parks and
are now harmless), `drain_timeout` = 0, `drain_stale` = 0.

`drain_timeout = 0` is worth noting because it was the predicted failure: a
handler parked awaiting a read, with no recv armed, has nothing to wake it and
might never re-offer. On this workload it always re-offers well inside the
64-tick budget. That budget is a number chosen by hand and validated on one
workload, which is the weakest part of this change.

### The tail

p99 was 11,989 µs with phase 1 complete, against 10,985 µs on the step-1 run
where almost nothing parked. That points at most of the tail being inherent to
saturated 600k rather than caused by draining, but step 1 still cancelled 4,079
recvs so it is not a clean control. A park-off run at the same rate is the right
comparison and is still pending on rig availability — the number here should be
treated as bounded, not measured.

**Costs to measure, not assume.** The stall lands in the tail, not the mean, so
throughput may look unchanged while p99 moves — name that statistic when
reporting. The TCP window closes while reads are withheld. A handler awaiting
something external may never re-offer, so phase 2 needs a deadline and an abort
that re-arms recv and leaves the connection in place. The cancelled recv's
provided buffers have to be returned on a path that does not exist today.

## Phase 2 — a busyness signal in the policy

Phase 1 fixes parks that *start*. It does nothing for the case where the policy
never fires: a worker pinned by few-but-expensive connections is
count-balanced, so `mine >= min_load + PARK_MARGIN` never holds
(`experiments/park-heterogeneous.toml`: 82.5% vs 7.7% busy, **zero** parks).

`publish_load` stores `connections.active_count()`. The change is to publish
busy time alongside it and let tier 3 — not tier 1 — consult it. Tier 1 must
keep using counts: it places a connection that does not exist yet, so its cost
is unknowable. Tier 3 moves a live connection, whose cost is observable.

**GO/NO-GO.** Rerun `park-heterogeneous.toml`, which currently fires zero parks
against a ninefold CPU gap.

- **GO** if parks fire and `loaded_cores` converges 4 → 8, with the balanced
  penalty still inside the ~4% bound from #470's balanced control.
- **NO-GO** if it thrashes — parks firing continuously without the CPU gap
  closing, which would mean the move is not relieving the load and the selection
  of *which* connection to move is the real problem.

**Known risk.** Busyness is a rate needing a window, where a count is an exact
integer; `PARK_MARGIN = 8` does not translate and will need hysteresis. Two
metrics across tiers can also fight — the wide margin exists partly to stop tier
1 and tier 3 oscillating — so phase 2 has to be measured against the balanced
control, not just the imbalanced one.

## What this does not attempt

Case B in full. Even with both phases, park moves *whichever connection the
handler offered*, and on a mixed worker that may be an idle one while the
expensive connection keeps the worker pegged. Per-connection cost attribution is
a third change, unscoped here.

Tier 4 shed remains the alternative for handlers that cannot park at all, and is
still unimplemented.
