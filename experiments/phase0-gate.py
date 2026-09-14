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

Usage: phase0-gate.py <arm-dir>   # expects results-*.json and percore.txt
Exit 0 = PASS, 1 = REJECT.
"""
import json
import pathlib
import sys

# One core this busy while the median core is below IDLE_CORE_MAX is the
# single-core funnel. Thresholds are deliberately wide: this catches a
# pathology, not a mild imbalance.
HOT_CORE_MIN = 90.0
IDLE_CORE_MAX = 30.0
# Little's law tolerance. A good closed-loop arm lands within a couple of
# percent (1.4% measured locally with the mean); 15% leaves room for warmup
# edges and sampling without admitting a broken arm.
LITTLE_TOLERANCE = 0.15


def check_percore(path):
    """percore.txt: one `cpu<N> <busy_percent>` per line, sampled over the run."""
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
    busy.sort()
    hottest = busy[-1]
    median = busy[len(busy) // 2]
    if hottest >= HOT_CORE_MIN and median <= IDLE_CORE_MAX:
        return [
            f"single-core funnel: hottest core {hottest:.0f}% busy, median core "
            f"{median:.0f}% across {len(busy)} cores — this is the artifact that "
            f"invalidated a previous campaign, not a result"
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
    problems = check_percore(arm / "percore.txt")
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
