# Direct-forward: submitting Mode A writes from the completion handler

- **Status:** **NO-GO on the stated theory; the redirect it produced SHIPPED.**
  Submitting from the completion handler bought nothing (instructions/byte
  unchanged). Gathering — the alternative this entry isolated by elimination —
  is worth **+30%** and closes #416's forwarding exception. Both outcomes below.
- **Span:** 2026-09-17 → (open) · follows #415 (282773b) and #416 Phase A

## Goal

Make `forward_to` / `forward_to_conn` stop waking a task for every provided
buffer. Submit the next write from the write-completion handler, the way
`run_direct_echo` already submits its echo from inside `handle_recv_multi`.

## Why: the measurement that reframed the problem

#416 Phase A was set up to ask whether ringline's recv buffer geometry is
wrong. The answer was no — `recv_buffer(256, 16384)` is minimax-optimal across
108 saturated arms — with **one** systematic exception: forwarding, where the
default runs 34% behind the best geometry at 64 connections and 18% behind at
1024.

The obvious reading was "streaming wants bigger buffers". Instructions per byte
says otherwise. At the *same* 16 KiB default buffer, on the same two-guest rig:

| path | instr/byte | over the ~0.86 kernel floor |
|---|---|---|
| echo, `run_direct_echo` | **0.985** | +13% |
| forward, Mode A | **1.26** | +32% |
| forward, Mode A at 64 KiB buffers | 0.96 | +10% |

**Echo at the default buffer is already as efficient as forwarding is at four
times the buffer.** Buffer size is not what separates them; it is only the lever
that spreads Mode A's per-buffer cost over more bytes. The ~6,600 instructions
per completion solved for in #415 is therefore mostly *avoidable work*, not
intrinsic cost.

Reading the two paths against each other gives two differences:

1. **Mode A wakes the task once per buffer.** `handle_forward_write` sets
   `forward_done[ci]` and calls `wake_recv`; the executor then polls
   `ForwardToFuture`, which pops the next held buffer and calls
   `start_forward_write`. So every 16 KiB costs: recv CQE → hold push →
   `wake_recv` → `collect_wakeups` → `poll_ready_tasks` → future poll →
   `with_state` → SQE build → write CQE → replenish → `wake_recv` again.
   `run_direct_echo` does none of this: it submits from the CQE handler and
   explicitly "bypasses `collect_wakeups` → `poll_ready_tasks` entirely".
2. **Direct echo gathers; Mode A cannot.** #397 made direct echo stage arriving
   buffers and coalesce a drain's worth into one send. Mode A writes exactly one
   held buffer per write.

This entry is about (1). (2) is a separate, larger change and is deliberately
out of scope until (1) is measured — if the scheduler round-trip is most of the
cost, gathering buys much less than it looks like it should.

## A second finding from the same data, which bounds the alternative

Bigger buffers are not a substitute, because past ~100 KB they stop filling:

| buffer | bytes per recv completion | fill |
|---|---|---|
| 64 KiB | ~47 KB | 74% |
| 256 KiB | ~91 KB | 36% |
| 1 MiB | ~102 KB | 10% |

A multishot recv completes with what TCP has queued, not with a full buffer. So
payload-per-completion has a ceiling set by the socket, and buying it with ring
depth is what produced the 1.03-second p99 at 256 B × 1024 connections
(137,895,774 starvations on a 4-deep ring). Tuning geometry cannot reach what
removing the per-buffer wake-up can.

## Outcome: the scheduler round-trip was not the cost

Built it, and it works: driver-side `ForwardProgress`, `advance_forward` called
from `handle_forward_write` and the segmented recv branch, the task woken once
per forward instead of once per provided buffer. Correctness gate on io_uring:
clippy clean, full suite green, the #415 proxy regression test **0/60**.

It is not faster. Two guests, 2 workers so the proxy is the bottleneck, arms
interleaved against a baseline built from the same commit minus this change:

| geometry | baseline | direct-forward | delta |
|---|---|---|---|
| 16 KiB × 256 (default) | 11.32 Gbit/s | 10.96 | −3.2% |
| 64 KiB × 64 | 14.29 | 13.78 | −3.6% |

And the diagnostic that settles *why*, rather than leaving it at "no effect":

| | instructions/byte |
|---|---|
| baseline | **1.236** |
| direct-forward | **1.243** |

**Unchanged.** The wake path — `wake_recv` → `collect_wakeups` →
`poll_ready_tasks` → future poll → `with_state` — costs essentially nothing at
this scale. The ~6,600 instructions per completion solved for in #415 is real,
but it does not live where this entry assumed.

GO criterion 1 required >10% and a visible drop toward the 0.86 floor. Neither
happened, so this closes NO-GO. The ~3% regression is within run-to-run spread
(baseline 11.29–11.61, prototype 10.89–11.70) and is not itself the finding; the
flat instructions/byte is.

## The redirect worked: gathering is +30%

Built as this entry predicted: up to 16 held buffers per `sendmsg`/`writev`,
ordering unchanged, bids released per batch. Gate on io_uring — clippy clean,
full suite green, the #415 proxy regression test 0/60.

Three interleaved reps each at the default geometry, 2 workers, two guests:

| | instructions/byte | Gbit/s |
|---|---|---|
| baseline (one buffer per write) | 1.236 | 11.23 (12.03, 11.19, 11.23) |
| **gathered (≤16 per write)** | **0.906** | **14.60 (15.14, 14.58, 14.60)** |
| direct echo, reference | 0.985 | — |
| kernel floor | ~0.86 | — |

**+30% throughput, and instructions/byte past direct echo to within 5% of the
floor.** Per-buffer overhead falls from 0.38 instr/byte to 0.05: **87% of Mode
A's avoidable cost was the completion count.** Ranges do not overlap.

Taken with the failed attempt, this decomposes the ~6,600 instructions per
completion cleanly:

- **scheduler round-trip: ~0.** Direct submission moved instructions/byte from
  1.236 to 1.243.
- **completion count: ~all of it.** Gathering moved it to 0.906.

The NO-GO was worth its cost: it eliminated the wrong explanation, which is what
left only the right one.

## It also closes #416's forwarding exception

#416 concluded that `recv_buffer(256, 16384)` is minimax-optimal everywhere
*except* forwarding, where it ran 34% behind the best geometry at 64
connections. That exception was an artifact of one-buffer-per-write. Gathered,
at the same 4 MiB/worker budget:

| geometry | gathered | ungathered |
|---|---|---|
| 16 KiB × 256 (default) | **14.60** | 11.23 |
| 64 KiB × 64 | 15.13 | 14.29 |
| 256 KiB × 16 | 14.22 | ~14.80 |

The default is now within **3.5%** of the best forwarding geometry — the same
order as its deficit on every echo workload (−4% to −11%). 256 KiB has become
*worse* than the default, which fits: a sixteen-buffer batch at 256 KiB is 4 MiB
of iovec against a socket that rarely has ~100 KB queued, so batches stay short
while the ring gets shallower.

So the tuning advice `forward_to` carried since #415 is retired rather than
revised, and ringline's answer to "what buffer geometry should I use" is now
**the default, for everything measured**.

Caveat, stated rather than buried: this is 64 connections and three buffer
sizes. The c1024 depth axis is untouched by gathering (a batch cannot conjure
buffers a shallow ring does not have), so small-buffer/deep-ring should still
win there — which argues *for* the default, not against it. #416's forward arms
should be re-run on a gathered build before the sweep's conclusion is rewritten.

## Where the cost actually is

Eliminating the wake leaves exactly one difference between Mode A and
`run_direct_echo`, and it is the one this entry deferred:

> **Direct echo gathers; Mode A cannot.** #397 made direct echo stage arriving
> buffers and coalesce a drain's worth into one send. Mode A writes exactly one
> held buffer per write.

Per N buffers, direct echo costs N recv CQEs + **1** send CQE; Mode A costs N
recv + **N** write CQEs, plus N SQE constructions and N bid replenishes. At
16 KiB that difference is 0.26 instructions/byte (1.24 vs direct echo's 0.985),
or roughly 4,300 instructions per buffer — which is most of the gap and is now
the only remaining explanation.

So the next attempt is **gathering**: pop several held buffers and write them as
one `writev`/`SendMsg` iovec. Ordering survives (one vectored write, in hold
order, still one in flight per connection); the work is partial-write handling
across iovecs, releasing several bids on one completion, and what the hold cap
means when a write covers many buffers.

## Should the prototype land anyway?

**Not on its own merits.** It is a correct −3% change, and the repo's stance is
that performance claims require measurement — this one measured as a small
regression.

It is, however, a *prerequisite* for gathering: to write several held buffers
from the completion handler, the driver must own the forward state, which is
precisely what this does (and what `MioForwardState` already does on mio). Kept
on a branch rather than landed, to be revived by the gathering attempt or
discarded with it. Landing a regression against speculation about a future
change would be the wrong trade.

## Design sketch

The change is to move Mode A's forward state from the future into the driver —
which is **exactly the shape the mio backend already has**. `MioForwardState`
(`backend/mio/driver.rs`, added in #415) holds `sink_index`, `sink_generation`,
`len`, `forwarded` in the driver, and the mio event loop drives the relay
without polling a future per chunk. The io_uring side keeps that state in
`ForwardToFuture` (`len`, `forwarded`, `target`), which is why it must poll.

So:

- Add `forward_state: Vec<Option<UringForwardState>>` to the io_uring driver —
  `target: SinkTarget`, `len`, `forwarded` — mirroring `MioForwardState`.
- `handle_forward_write`: on success, advance `forwarded`; if the forward is
  complete, or EOF, or errored, set `forward_done` and `wake_recv` as today;
  **otherwise pop the next held buffer and submit its write immediately.**
- `handle_recv_multi`'s segmented branch: if a forward is active and no write is
  in flight, submit directly rather than only waking.
- `ForwardToFuture::poll` becomes what the mio one already is: park, then read
  the terminal result. The overshoot split (bytes past `len` stashed into the
  accumulator) moves into the driver alongside `settle_forward_end`, which is
  already there.

Ordering is unchanged: still exactly one write in flight per connection, still
issued in hold order. It is only *who* issues it that changes.

## GO / NO-GO

1. **>10% throughput on the forward path at the default geometry**, two guests,
   saturated, arms interleaved, three reps. The gap to close is 0.40 instr/byte
   against a 0.86 floor; if direct submission does not move instructions/byte
   materially, the theory is wrong and the entry closes.
2. **The buffer-size sensitivity shrinks.** If the per-completion cost is what
   made 16 KiB → 64 KiB worth 26%, that spread should narrow. If it does not,
   something else is dominating and this was the wrong fix.
3. **No regression in the invariants #415 spent its time on**: one write in
   flight per connection, bytes in hold order, exactly one replenish per bid,
   correct behaviour on sink close (EPIPE), EOF truncation, hold-cap throttle
   and its cancel/re-arm, and the five bugs #415 fixed stay fixed. The proxy
   test at `forward_hold_cap(1)` must stay 0/100.

Abandon if (1) fails, or if (3) requires a second copy of the recv-arming state
machine that #415 showed is already the hardest part of this code to keep
correct.

## Plan

1. This entry (intent).
2. Prototype: driver-side forward state, direct submission from both handlers.
3. Local: `cargo test --all` both backends; the #415 regression tests.
4. Rack: proxy test 100 reps on io_uring, then the two-guest A/B at the default
   geometry and at 64 KiB, against `main`.
5. Outcome here: numbers, or the mechanism that made it not work.
