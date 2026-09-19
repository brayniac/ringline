# Prefaulting the buffer pools: measuring what the knob is worth

- **Status:** **shipped, default off** — measured to pay at large rings and to
  do nothing at the default, which is what `prefault_buffers(false)` already
  encoded. The measurement supplies the guidance the knob was missing.
- **Span:** 2026-09-17 → 2026-09-18 · PR #419 · two X710 guests

## Goal

`vec![0u8; n]` goes through `alloc_zeroed`, so a large pool is mapped but
untouched — every page is the shared zero page until written. On the recv path
that first write is the *kernel* copying an skb in, so the minor fault lands on
the completion path rather than at startup.

#419 added `ConfigBuilder::prefault_buffers(bool)`, default off, claiming it
buys **predictability, not throughput**: faults stop appearing as ramp-phase
p99 outliers, and RSS after startup equals RSS under load. The knob shipped
unmeasured. This entry measures it.

## What it took to measure it at all

The knob was **unreachable from the harness** — it existed on `ConfigBuilder`
and nothing in `ringline-bench` set it. That is the whole reason it went
unmeasured; a flag was added to `bench-server` here.

Two harness defaults would have produced a false null, and both are worth
remembering before anyone "re-measures and sees nothing":

- `bench-client --warmup 3` **discards the ramp**, which is the entire phase
  under test.
- Window length decides detectability arithmetically. ~400 fault events across
  a 10 s run (~10M ops) is 0.005% — below p9999, invisible *by construction*.
  At 3 s it is ~0.05%, inside the p999 band.

Design: 16 rounds alternating off/on inside **one** guest pair, so the A/B
carries no VM-to-VM or NIC-placement variance; server restarted every round,
because prefault is a startup property.

## Outcome

**Over-provisioned ring, 1024 × 64 KiB (512 MiB across 8 workers):**

| metric | off | on | delta | ranges |
|---|---|---|---|---|
| p50 | 221,664 ns | 216,496 ns | −2.3% | overlap |
| p99 | 435,881 | 423,368 | −2.9% | overlap |
| **p999** | **798,335** | **541,273** | **−32.2%** | **disjoint** |
| p9999 | 2,647,568 | 2,635,303 | −0.5% | overlap |
| ops/s | 830,778 | 852,304 | +2.6% | overlap |

Mechanism on the same runs: traffic-time `minflt` **410 → 4**, RSS growth under
load **+513,750 kB → +32 kB**.

**Default ring, 256 × 16 KiB (32 MiB across 8 workers):** null. p999 −1.3%,
p99 −1.2%, every range overlapping.

The result is causal rather than correlational because the arithmetic predicts
*which* percentile should move: 410 faults / 830,778 ops = 0.049%, inside the
worst 0.1% and outside the worst 0.01%. p999 moved by a third; p9999 and max
did not move at all.

## The finding: fault *cost*, not fault count

Two quantitative predictions failed here, and the correction is the real
result. Page-count reasoning says a 16× smaller ring should fault 16× less. It
faults **more**:

| geometry | pool | traffic faults | RSS per fault | p999 |
|---|---|---|---|---|
| 1024 × 64 KiB | 512 MiB | 410 | **~1.2 MiB** | −32% |
| 256 × 16 KiB | 32 MiB | 1,027 | ~20 KiB | none |

A large contiguous pool is backed by transparent huge pages (`THP: [always]` on
the rig), so each fault zeroes ~1.2 MiB — tens of microseconds of stall on a
completion — enough to push an operation out of the p999 band. The default pool
is too small for THP to coalesce much, so its faults are numerous but ~20 KiB
each, too cheap to show against an ambient p999 of ~530 µs.

**Prefault pays exactly when the ring is large enough for THP to back it** —
the over-provisioning case #416's sweep predicted people would create.

## Lessons / open questions

- **A knob that no harness can set is a knob that cannot be measured.** #419
  shipped behind a config flag with no path from the benchmark to the flag, so
  "measure it later" was unreachable by construction. Wire the harness in the
  same PR as the knob.
- **Sizing models built on page counts mislead once THP is in play.** The
  quantity that matters is bytes zeroed per fault, not pages faulted. Both
  failed predictions here came from counting pages.
- **A measurement window can be incapable of detecting its own subject.**
  State the dilution arithmetic before running, not after reading a null.
- Not tested: whether `MADV_POPULATE_WRITE` (Linux 5.14+) changes the cost of
  the startup half, and whether explicitly-requested huge pages make the
  default ring behave like the large one.
- Ramp only. On a long-lived server these faults are a one-time cost that
  amortises to nothing; this measures restart, deploy, and first-burst.
