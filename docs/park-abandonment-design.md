# Why park abandons most of what it starts

Tier 3 park is described in
[listeners-and-accept-design.md](listeners-and-accept-design.md); this note is
about one measured property of it. Park starts far more moves than it finishes —
and the reason turns out to be its own bookkeeping rather than anything about the
connection.

The counters this note rests on are `ringline/park_abandoned`, one slot per
`ParkBlocker`.

## What was measured

`experiments/park-imbalance.toml` with the offered rate swept in open loop.
Completion is `park_completed / park_started`.

| offered rate | per-connection | workers 0–3 | completion |
|---|---|---|---|
| 5,000 | ~20/s | idle | **100%** |
| 20,000 | ~78/s | idle | **100%** |
| 80,000 | ~313/s | <50% busy | **100%** |
| 320,000 | ~1,211/s | loaded, not pegged | **95.7%** |
| 600,000 | ~2,100/s | **100%** | **1.6%** |
| 1,200,000 | ~4,500/s | **100%** | **1.6%** |

The knee is CPU saturation of the parking worker, not arrival rate: 64× of rate
below saturation moves completion by 4 points, while crossing into saturation
moves it by ~94. Closed loop at 370k (`park-imbalance.toml`, depth 1) sits with
the saturated cases at 0.84%, so loop shape is not the variable either.

## Why the abandonments happen

With one slot per blocker, a saturated run says it plainly:

```
started   = 3191
completed = 203 (6.36%)
abandoned = 2988

  not_offered      2811  94.1%
  data_pending      177   5.9%
  (13 others)         0   0.0%
  (sum)            2988          == abandoned
```

`begin_park` submits a linked recv-cancel plus a `FIXED_FD_INSTALL`, and
`handle_park_install` re-checks the gate when the CQE lands. The sequence that
produces `NotOffered`:

1. The handler reaches a quiescent point and calls `offer_for_park`.
2. `begin_park` submits the cancel and the install.
3. Bytes arrive. `withdraw_park_offer` clears the flag
   (`event_loop.rs:2050`, called from the three recv delivery paths).
4. The handler consumes those bytes and returns to idle. This is why
   `data_pending` is only 6%: the accumulator is *empty* by the time the install
   CQE is handled.
5. The install CQE lands, `park_blocker` answers `NotOffered`, the move is
   abandoned and the recovered fd is closed.

So a park is abandoned on **the historical fact that data arrived during the
round trip**, not on the connection's state when the decision is taken. The
handler does re-offer at its next idle point — just not before the CQE lands.
The policy then fires again, submits another round trip, and loses the same way,
which is the retry storm: 3,191 attempts for 203 moves in the run above, and
31,409 for 264 in the original imbalance measurement.

`begin_park` does not clear the offer itself. That was checked, because it would
have produced the same counter reading for a completely different reason.

## Candidate fix

At the install re-check, do not re-require the offer flag; require the **state**
blockers only.

The argument that this is safe is in the code already. The comment on
`withdraw_park_offer` says `ParkBlocker::DataPending` is a state-based backstop
so that "a path added later fails closed rather than parking mid-request" — the
state check is what guarantees the handler holds nothing, and the 6% shows it
firing correctly when data genuinely is pending. The flag is defence in depth on
top of that, and it currently costs 94% of all parks.

`NotOffered` conflates two things at the re-check: "this handler never opted in"
and "it opted in, then bytes arrived". The first must still refuse. But by the
time an install CQE exists the handler demonstrably opted in — `park_in_flight`
records it — so that bit is already available without the flag.

### What this does not resolve

`withdraw_park_offer` also drops `park_carry`, the state the handler deposited at
the offer. Restoring the park is not just un-clearing a flag: state deposited
*before* a request that has since been processed may be stale, which is the
reason the "carry unconsumed buffers" design was rejected in the first place
(see listeners-and-accept-design.md). Whether the deposited state is still valid
after the handler has processed a request is the substantive question here, and
it is not answered by the counters.

## Two things the counters cannot see

**Run-to-run variance at saturation is large.** Two runs at the same nominal
configuration gave 19,415 started / 0.76% completion and 3,191 / 6.36%. The
dominant *reason* is stable across both; the completion percentage is not, and
should not be quoted as a figure.

**An earlier version of these counters hid this result.** The variants believed
unreachable at the re-check were collapsed into one `other_blocker` slot, which
then held 97.8% of abandonments — the grouping encoded an assumption and
absorbed the entire finding. Hence one slot per variant now, including ones that
read zero.

## Whether tier 3 should carry its own weight

Worth stating alongside the mechanism, because the measurements bear on it.

What park demonstrably does: converges a manufactured standing imbalance in
11–18 s, recovers the full 8-worker headroom (592,446 steady ops against a
592,739 balanced ceiling), and costs nothing above a ~4% resolution when the
fleet is balanced.

What it demonstrably does not do:

- **Move a connection that is never quiescent.** Park requires a quiescent
  point, so a client that keeps a pipeline full has no parkable moment. That is
  the shape `ringline-redis` and `ringline-memcache` promote.
- **See busyness.** The policy compares connection counts
  (`experiments/park-heterogeneous.toml`): six saturating connections per worker
  against four idle workers produced **zero** parks, because `6 >= 0 + 8` never
  holds. A worker pinned by a few expensive connections never sheds.
- **Work efficiently when it matters.** 94% of attempts abandoned precisely when
  the worker is saturated.

And the condition it repairs — a standing imbalance with no new arrivals — had to
be manufactured to measure at all, by steering workers out of the accept
rotation. A healthy merged-mode fleet is balanced at accept by tier 1.

So the case for de-emphasising tier 3 is not that the mechanism is broken; it
converges, and it is honest about its cost when idle. It is that the cases where
rebalancing would matter most — pipelined connections, heterogeneous per-
connection cost, few-but-expensive connections — are the ones park structurally
cannot serve, while the case it does serve is one that tier 1 already prevents.
Marking it experimental and pointing at tier 1 placement or the listener pool is
a defensible reading of these numbers.

What would change that reading: evidence of a real workload that produces a
standing post-accept imbalance tier 1 cannot fix, in which the connections are
idle enough to be parkable. Nothing measured so far is that workload.
