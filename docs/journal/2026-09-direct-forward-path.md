# Direct-forward: submitting Mode A writes from the completion handler

- **Status:** open — intent recorded before building, per the journal ground rules
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
