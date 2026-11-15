# Experiment B: what send_admitted costs, and what FIFO order costs

- **Status:** closed. Both conditions of section 4's decision rule held; the
  latency cost in the contended case, which the rule does not count, is for
  the owner to weigh.
- **Span:** Nov 2026 · follows #930 (`send_admitted`) and #939 (`abort`) ·
  SystemsLab context `send-admission-expB`

## Goal

Section 4 of `docs/send-admission-design.md` asks whether a server can send
every plain reply with `send_admitted`. Two costs decide it: the CPU a
`send_admitted` call adds when the pool is free, and the replies that wait
behind the FIFO's head while slots are free.

## Method

Binaries: `experiments/send-admission-b-build.toml`, io_uring builds of one
commit of `exp/send-admission-b`.

Uncontended: `experiments/send-admission-b-echo.toml`. `bench-server` with one
worker, pinned to CPU 0, in a guest on hv02 (z1, 24 vCPU); `bench-client` in a
guest on hv01 (z2, 56 vCPU), across the X710 LAG. 256 B messages, 256
connections. Three arms, `--echo-mode`:

| arm | the send, after `with_bytes` returns |
| --- | --- |
| `bytes-nowait` | `send_nowait` |
| `bytes-admitted` | `send_admitted`, the `SendCompletion` dropped |
| `bytes-admitted-timeout` | `send_admitted` under a 1 s `timeout`, then the `SendCompletion` under a 1 s `timeout` |

Each cell runs its own server and two windows: a closed-loop window, which
saturates the worker, and an open-loop window at 55,000 requests/s, about half
the closed-loop rate. The primary statistic is the worker thread's CPU time
(`/proc/<pid>/task/<tid>/schedstat`) over the closed window, divided by the
client's operations in it. The secondary is the open window's p99, measured
from each request's scheduled send time.

Two single-arm `bytes-nowait` experiments ran first, each in its own pair of
guests (`experiments/send-admission-b-echo-single.toml`). They measured 8.87 and 9.22 us of worker CPU per operation, at
111,786 and 108,620 operations/s: a floor of 4.0% between boots, as large as
the effect being measured. So the arms are compared within one boot instead,
as the November echo rerun on this rig did (`BENCHMARKS.md`, run `01a1bc9d`):
`experiments/send-admission-b-echo-gen.py` writes one experiment with a
discarded warm-up cell and four rounds of the three arms, the order rotated
each round. The experiment ran twice, in two boots, to see whether the
differences between arms hold across boots.

Contended: `experiments/send-admission-b-contended.toml`, one guest on hv03.
`admission-server` (one worker) and `admission-load` over loopback, with
`net.ipv4.tcp_wmem` capped at 64 KiB. Without the cap the kernel's send buffer
took each stalled 2 MiB reply whole and nothing was contended: in the first
pilot the large reply arrived in 2 to 5 ms in all 10 episodes, and no stalled
reply was aborted. Every 4 s,
three peers that never read each ask for a 2 MiB reply from a 512 x 16 KiB
pool, and 200 ms later one 7 MiB reply waits at the head of the FIFO. Small
256 B requests arrive at 2,000/s on 16 connections. `settle_limit` is 1 s and
`admit_limit` is 0.5, 1 or 2 s. Small replies are sent with `send_admitted`
or with `send`; large ones always with `send_admitted`. Four repetitions of
each of the six cells.

## Prediction

Written before the first rack run. The contended case's prediction was
revised after a local smoke run on mio (macOS), which showed the
large-reply timeout at a ratio of 0.5 and seven small replies just past the
limit.

Uncontended, closed loop:

- `bytes-admitted` adds a completion-table entry, a FIFO enqueue and pop, and
  a dropped `SendCompletion` per reply. The design predicts CPU per operation
  within the `bytes-nowait` floor. I expect it 1 to 4% above, which may be
  outside a floor under 1%.
- `bytes-admitted-timeout` also arms and cancels two timers and wakes the task
  when the send completes. I expect 5 to 15% above `bytes-nowait`, outside the
  floor.
- Refuted if `bytes-admitted` is within the floor (the design's prediction
  holds), or if `bytes-admitted-timeout` is too.

Uncontended, open loop: p99 within the spread for `bytes-admitted`, and higher
for `bytes-admitted-timeout` only if its CPU cost is large enough to queue
replies at the fixed rate.

Contended:

- The large reply is delivered after about 800 ms (the stalled peers' abort at
  `settle_limit`, less the 200 ms delay) at ratios 1 and 2, in both arms. At
  0.5 it times out at `admit_limit` in both arms. If it is delivered in
  milliseconds the pool was not contended and the run measures nothing.
- `send`: every small reply within `admit_limit` at every ratio, p99 of a few
  milliseconds.
- `send_admitted` at ratios 1 and 2: every small reply within `admit_limit`,
  since each waits only until the head is admitted after the abort. p99 near
  the wait behind the head, up to about 800 ms.
- `send_admitted` at 0.5: a few late, those scheduled just before the head
  arrived and read after it, which wait the head's whole `admit_limit`. In a
  FIFO where every wait has the same bound, the waiters ahead of a reply leave
  by their own deadlines, which come before its deadline. So "fewer below a
  ratio of 1" holds only by that margin.
- The design's decision rule then holds for the contended case: at ratios of
  1 or more `send_admitted` delivers no fewer small replies than `send`. Its
  cost there is latency, which the rule does not count.

## Results

Binaries: io_uring builds of `exp/send-admission-b` at cf283561 (the first two
echo runs and the first contended pilots) and 31d31976 (the rest; it adds
`bytes-admitted-await`). Linux 6.12.116, `debian-13-6.12` guests. No server
refused or closed a connection in any echo window (84 of 84 in the control
runs, and every window in the first two), and the worker was 96% to 100% busy
in every closed window.

### Uncontended

Worker CPU per operation, closed loop, in microseconds. Each run is one boot of
the two guests; each figure is the median of its four cells, with the change
against `bytes-nowait` in the same run. Runs 1 and 2 are
`send-admission-b-echo.toml` (`01a1dde5-8bb4`, `01a1dde5-8c01`), runs 3 and 4
`send-admission-b-echo-controls.toml` (`01a1de0c-50d4`, `01a1de0c-510d`).

| arm | run 1 | run 2 | run 3 | run 4 | all cells |
| --- | --- | --- | --- | --- | --- |
| `bytes-nowait` | 8.95 | 9.06 | 9.02 | 9.09 | 9.00 [8.63-9.33], n=16 |
| `bytes-admitted` | 9.20 (+2.8%) | 9.04 (-0.3%) | 8.90 (-1.4%) | 9.09 (-0.1%) | 9.09 [8.77-9.39], n=16 |
| `bytes-admitted-timeout` | 7.72 (-13.7%) | 7.61 (-16.1%) | 7.71 (-14.6%) | 7.80 (-14.2%) | 7.67 [7.50-8.15], n=16 |
| `bytes-admitted-await` | | | 7.73 (-14.3%) | 7.70 (-15.3%) | 7.71 [7.59-8.40], n=8 |
| `bytes-nowait-defer` | | | 7.91 (-12.3%) | 7.80 (-14.2%) | 7.80 [7.63-8.42], n=8 |

- **The floor.** `bytes-nowait`'s four cells spread 1.3%, 6.1%, 6.1% and
  6.2% within a run, and its run medians 1.6% across the four runs. Two
  single-arm runs in separate boots differed by 4.0%.
- **`bytes-admitted`** differs from `bytes-nowait` by -1.4% to +2.8% in the
  four runs, inside the within-run floor. The design's prediction holds; mine
  (1 to 4% more) is not supported.
- **`bytes-admitted-timeout`** is 14 to 16% cheaper, in every run. My
  prediction (5 to 15% more) is refuted. Two controls, added after runs 1 and
  2, place the cause:
  - `bytes-admitted-await` awaits the same settlement with no timers, and is
    as cheap. The two timers cost nothing measurable.
  - `bytes-nowait` with `--defer-recv-arm` is about as cheap, with
    `send_nowait` and no wait. The saving comes from the receive being armed
    after the reply's send completes rather than with it, the mechanism
    `BENCHMARKS.md` records for `ConfigBuilder::defer_recv_arm` (21.11 to
    19.10 us per operation, at 8 workers). It is a property of when the
    next receive is armed, not of `send_admitted`.
- **Open loop**, 55,000 requests/s: every arm's worker CPU per operation is
  within 15.13 +/- 0.25 us (cell range 14.28 to 15.65), and p99 is 385 to
  395 us at the median (cell range 369 to 491). Nothing separates the arms.
- At 256 connections the closed loop also measures throughput, since the
  worker is saturated: `bytes-admitted-timeout` served 129,000 operations/s
  against 110,000 for `bytes-nowait`.

### Contended

`send-admission-b-contended.toml` with its defaults (1 MiB `tcp_wmem` cap,
three 3 MiB stalled replies, a 7 MiB head), 24 runs in a rotated order, four
per cell. `settle_limit` is 1 s. "Not within" is small replies whose latency
from their scheduled send time exceeded `admit_limit`, late or lost, of about
80,000 offered in each run's 40 s window.

| ratio | small replies | not within | small p99 | large replies delivered |
| --- | --- | --- | --- | --- |
| 0.5 | `send_admitted` | 124, 200, 186, 176 | 470 ms | 0 of 40 (admission timeout) |
| 0.5 | `send` | 0, 0, 0, 0 | 11.5 ms | 0 of 40 (admission timeout) |
| 1 | `send_admitted` | 0, 0, 0, 0 | 771 ms | 40 of 40, after 808 ms |
| 1 | `send` | 0, 0, 0, 0 | 11.5 ms | 40 of 40, after 805 ms |
| 2 | `send_admitted` | 0, 0, 0, 0 | 770 ms | 40 of 40, after 808 ms |
| 2 | `send` | 0, 4, 0, 0 | 11.4 ms | 40 of 40, after 806 ms |

- Every run was contended: in each episode the large reply either waited
  about 808 ms for the stalled peers' abort, or timed out in admission. All
  30 stalled replies per run were aborted at `settle_limit`.
- At ratios of 1 and 2, `send_admitted` delivered every small reply within the
  limit. At 0.5 it missed 0.16 to 0.25% of them, all delivered late and none
  lost, at most 513 ms after their scheduled send. Those are the requests
  scheduled just before the head arrived and read after it, which waited the
  head's whole `admit_limit`. The prediction held.
- `send` was refused, and its connection aborted, 4 times in one run at
  ratio 2 and 8 times in one run at 0.5. The 4 cost 4 small replies. The run
  with 8 lost no small reply in the window, so those refusals hit requests
  scheduled outside it. A refusal means no slot was free; when each happened,
  and which admission took the last slots, was not recorded.
- The cost of FIFO order is latency: small replies sent with `send_admitted`
  wait behind the head until it is admitted or times out, p99 about 770 ms
  against 11.5 ms with `send`.
- Small-reply p50 was about 6 ms in both arms on loopback. I do not know
  why; it is the same in both arms and below every limit.

## Outcome

Section 4's decision rule holds on both conditions:

- uncontended, `send_admitted`'s CPU per operation is within the floor of
  `send_nowait`'s;
- contended, at `admit_limit` of `settle_limit` or more, `send_admitted`
  delivered no fewer small replies within the limit than `send`.

By the rule, the specification for section 4 covers only guarded batches,
and a server sends every plain reply with `send_admitted`, with `admit_limit`
at or above `settle_limit`.

The rule does not count latency. While a head waits for stalled peers to be
aborted, every reply on the worker waits behind it, up to `settle_limit`,
with slots free: here a p99 of 770 ms against 11.5 ms. A server that cannot
accept that needs tries, as the section's constraints describe. Whether it
can is for the owner.

## Not covered

- One message size (256 B), one rig (a z1 server guest, one worker) and one
  kernel (6.12). A larger reply, which needs several slots, was not measured
  uncontended.
- Neither arm sent TLS, guarded batches, or zero-copy replies.
- The contended case ran on loopback with the kernel's send buffer capped at
  1 MiB, so that stalled replies stay in the pool. With the default cap the
  kernel took each 2 MiB stalled reply whole.
- Syscalls and completions per operation were not read from the recordings,
  so the receive-arming mechanism rests on the two controls, not on a count.
- The design's A-against-A pair ran in separate boots. The comparison ran
  within one boot instead, as the November echo rerun did, because the
  between-boot floor (4.0%) was as large as the effect being measured.

