# Experiment B: what send_admitted costs, and what FIFO order costs

- **Status:** open.
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
