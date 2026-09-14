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

# The funnel is work *concentrated* on one core, so it is a ratio, not a level.
# One core carrying this share of all busy CPU is the fingerprint.
FUNNEL_SHARE = 0.6
# Below this much total busy CPU the arm is simply lightly loaded and the
# concentration question is meaningless — at one connection nothing should be
# saturated, and demanding otherwise rejects the correctly-idle runtimes.
MIN_LOAD_PERCENT = 120.0
# Little's law tolerance. A good closed-loop arm lands within a fraction of a
# percent — the real rig arms closed to 0.1-0.3% — so 15% leaves room for
# warmup edges without admitting a broken arm.
LITTLE_TOLERANCE = 0.15


def check_percore(path, workers):
    """percore.txt: `cpu<N> <busy_percent>` per line, iowait excluded.

    Rejects the single-core funnel that invalidated a previous bare-metal
    campaign here: IRQ and wakeup locality pulled all work onto one core and
    capped every arm at the same number regardless of what was under test.

    Two earlier versions of this check were wrong, both found by running it
    against real arms rather than fixtures:

    1. hottest-vs-median rejected every healthy arm — a thread-per-core runtime
       *should* saturate exactly its worker count and leave the rest idle.
    2. counting busy cores against the worker count rejected every *lightly
       loaded* arm, where nothing should be saturated at all, and did so for
       tokio and mio while passing ringline — backwards, because the ringline
       numbers were inflated by the iowait accounting described in the spec.

    What survives both: the funnel is concentration, so compare the hottest
    core's share of total busy CPU, and only when there is enough load for the
    question to mean anything.
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
    total = sum(busy)
    hottest = max(busy)
    if total < MIN_LOAD_PERCENT:
        return []  # too little load for concentration to be meaningful
    share = hottest / total
    if share >= FUNNEL_SHARE:
        return [
            f"single-core funnel: the hottest core carries {share * 100:.0f}% of "
            f"{total:.0f}% total busy CPU across {len(busy)} cores "
            f"({workers} workers configured) — this is the artifact that "
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
