# Flake and scarcity reproducers

The specs in this directory that exist to make an *intermittent* failure happen on
demand, rather than to measure performance. They are here so a flake can be
re-run instead of re-derived — every one of them cost a round of guessing first.

Submit with `systemslab submit experiments/<spec>.toml`. They all default to
`main` on `ringline-rs/ringline` and run on a single `validation` agent, so none
of them queues for the hv01/hv02 measurement pair. Debug profile, because the CI
job that failed was `Test`, not `Test (release)`.

## The one thing to get right

**A reproducer with no positive control cannot tell "fixed" from "blind".** This
campaign learned that the expensive way: a clean arm was read as a fix, the claim
went into a merged PR, and the failure was still there — it had only moved. Every
spec below either carries a control arm or says in its header what it can and
cannot conclude. When adding one, state the prediction *and* what would refute it
before running, in the spec.

## CPU scarcity (the condition most of these need)

Several of these flakes only appear when the test is squeezed onto 2 CPUs, which
approximates a GitHub runner. The specs use `taskset` for that, clamped to what
the guest actually has — asking for more CPUs than exist kills the arm and reads
as "the wide configuration does not reproduce it".

| spec | what it does |
|---|---|
| `scarcity-flake-sweep.toml` | one named test in a loop, crossed with CPU budget |
| `scarcity-suite-sweep.toml` | the whole test binary in a loop, which is the parallel context most flakes actually occurred in |
| `flake-census.toml` | high-repeat sweep across *all* tests within a time budget, to find which ones are flaky rather than confirm a suspicion. 29,146 runs found exactly two: `throughput` (11/773, since converted to a completion test) and the `echo` positive controls |

## #518 — a `Shutdown` SQE half-closing a live connection

Fixed in #523, backported in #524. Root cause: `Shutdown` names its socket as the
registered-file slot `Fixed(conn_index)`, and nothing pinned that slot for the
operation's lifetime, so the `Close` that frees the slot could complete first and
the FIN could land on the slot's next occupant.

| spec | status | use it for |
|---|---|---|
| `bpsend-shutdown-fix-verify.toml` | **live** | The A/B to reach for. Both arms build from `main`; the `unfixed` arm reverts the fix commit, so putting the bug back must bring the failure back (46/400 vs 0/400) |
| `bpsend-shutdown-sqe.toml` | live, now 0 | The run that isolated the op: removing `shutdown_write()` took 49/400 to 0/400, and a single connection to 0/400 |
| `bpsend-epipe-diagnose.toml` | live, now 0 | Regression detector. Its 2×2 over the stall and the pool size is what exonerated the send-capacity admission FIFO — the failure survived with no stall and with a pool too large to park |
| `bpsend-probe-control.toml` | **historical** | Not re-runnable; its arms are deleted branches. Kept because it refuted its own PR's hypothesis, which is the reason the rest of this list exists |
| `backpressured-send-repro.toml` | live | The first attempt, before the instrument: the original test in a loop across CPU budgets, `alone` vs `suite` |
| `pi-backpressured-send-repro.toml` | live | The same on `pi4b` aarch64 guests, which answers "would the Pis have caught this" (1/200 — yes, but barely) |

The durable reproducer is not a spec at all: `repro518_instrumented` in
`ringline/tests/echo.rs` is `#[ignore]`d and parameterized by environment
(`RL518_ITERS`, `RL518_SLEEP_MS`, `RL518_POOL_SLOTS`, `RL518_CONNS`,
`RL518_SHUTDOWN`). It records, per connection, which accept it was, how many
sends completed, the raw `errno`, whether the handler reached `shutdown_write`,
and what the client saw. Run it directly:

```bash
RL518_ITERS=400 RL518_SLEEP_MS=0 RL518_POOL_SLOTS=8 RL518_CONNS=16 \
  taskset -c 0,1 cargo test -p ringline --test echo repro518_instrumented \
  -- --ignored --nocapture --test-threads=1
```

It asserts nothing on purpose — its job is evidence, not a verdict. Before
trusting a clean run from it, check it can still see a failure: cap the send loop
and confirm it reports anomalies. Reading `sends_ok × 4096 == bytes received`
straight off its output is what showed this was never data loss but an early
half-close, which both the issue and the test's own assertion message had
misdescribed.

io_uring only, for a concrete reason: `close_submitted` is written solely by the
io_uring close path and is inert on mio, so a mio build reports a clean sweep for
the wrong reason. Every spec gates on the backend.
