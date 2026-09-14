#!/usr/bin/env python3
"""Quality gates for the release comparison campaign.

An arm's numbers are used only if it passes every gate here. The gates exist
because of two recorded failures, not as ceremony:

* **Per-core saturation.** A previous bare-metal campaign on these hosts was
  invalidated after the fact: IRQ and wakeup locality funnelled all work onto
  one core, capping every arm at ~208k ops/s regardless of runtime, connection
  count or loop mode. "All runtimes are equal" was the artifact. One core at
  100% beside idle siblings is that fingerprint.

* **Little's law.** In closed loop `N = X * E[R]` is an identity, not a
  hypothesis. A violation means the measurement is wrong — coordinated
  omission, mis-counted ops, or a stalled arm — which is the class of error
  that forced the 2026-05 withdrawal. Checked against the *mean*, because a
  percentile is not the quantity the law is about.

Usage: phase0-gate.py <arm-dir> <server-workers>
Exit 0 = PASS, 1 = REJECT.
"""
import json
import pathlib
import sys

# A core at or above this is doing real work. The funnel is not "one core is
# hot" — a thread-per-core runtime is *supposed* to saturate exactly as many
# cores as it has workers and leave the rest idle. The funnel is work
# collapsing onto FEWER cores than the runtime was configured to use.
BUSY_CORE_MIN = 50.0
# Little's law tolerance. A good closed-loop arm lands within a couple of
# percent — the first real rig arm closed to 0.2% — so 15% leaves room for
# warmup edges and sampling without admitting a broken arm.
LITTLE_TOLERANCE = 0.15


def check_percore(path, workers):
    """percore.txt: one `cpu<N> <busy_percent>` per line, sampled over the run.

    Rejects the single-core funnel that invalidated a previous bare-metal
    campaign here: IRQ and wakeup locality pulled all work onto one core and
    capped every arm at the same number regardless of what was under test.

    The first version of this check compared the hottest core against the
    median and would have rejected every healthy arm in the campaign. On the
    real rig an 8-worker server on a 24-core guest runs 8 cores at 100% and 16
    at ~0%, so hottest=100 and median=0 — indistinguishable, by that rule, from
    one core doing everything. Counting busy cores against the configured
    worker count is the distinction that actually matters.
    """
    if not path.exists():
        return ["no per-core sample: cannot rule out the single-core funnel"]
    busy = []
    for line in path.read_text().split("\n"):
        parts = line.split()
        if len(parts) == 2 and parts[0].startswith("cpu") and parts[0] != "cpu":
            try:
                busy.append(float(parts[1]))
            except ValueError:
                pass
    if not busy:
        return ["per-core sample present but unparseable"]
    busy_cores = sum(1 for b in busy if b >= BUSY_CORE_MIN)
    if workers > 1 and busy_cores <= 1:
        return [
            f"single-core funnel: {busy_cores} core(s) above {BUSY_CORE_MIN:.0f}% "
            f"busy for a {workers}-worker server across {len(busy)} cores — this "
            f"is the artifact that invalidated a previous campaign, not a result"
        ]
    # Short of the full collapse, work spread over less than half the workers
    # still means the arm is not measuring what it claims to.
    if workers > 1 and busy_cores * 2 < workers:
        return [
            f"work collapsed onto {busy_cores} of {workers} configured workers "
            f"({len(busy)} cores sampled): the arm is bounded by placement, not "
            f"by the runtime under test"
        ]
    return []


def check_littles_law(result):
    n = result.get("clients", 0)
    x = result.get("ops_per_sec", 0.0)
    mean_s = result.get("mean_ns", 0) / 1e9
    depth = result.get("depth", 1) or 1
    if result.get("mode") == "open":
        return []  # the law bounds in-flight work, not offered rate
    if not n or not x or not mean_s:
        return ["closed-loop arm missing clients/ops/mean: cannot check Little's law"]
    # With pipelining, N*depth requests are in flight per connection.
    expected = n * depth
    observed = x * mean_s
    err = abs(observed - expected) / expected
    if err > LITTLE_TOLERANCE:
        return [
            f"Little's law violated: X*E[R] = {observed:.1f} but in-flight = "
            f"{expected} ({err * 100:.0f}% off). The measurement is wrong — "
            f"coordinated omission, mis-counted ops, or a stalled arm."
        ]
    return []


def main():
    arm = pathlib.Path(sys.argv[1])
    # The configured server worker count: the per-core check is meaningless
    # without it, because "how many cores should be busy" is exactly what it
    # decides.
    workers = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    problems = check_percore(arm / "percore.txt", workers)
    results = sorted(arm.glob("results-*.json"))
    if not results:
        problems.append("no results-*.json in the arm directory")
    for r in results:
        try:
            data = json.loads(r.read_text())
        except Exception as exc:  # noqa: BLE001 - a malformed arm is a rejected arm
            problems.append(f"{r.name}: unreadable ({exc})")
            continue
        problems += [f"{r.name}: {p}" for p in check_littles_law(data)]

    if problems:
        print(f"REJECT {arm.name}")
        for p in problems:
            print(f"  - {p}")
        return 1
    print(f"PASS   {arm.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
