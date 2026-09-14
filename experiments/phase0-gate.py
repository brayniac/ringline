#!/usr/bin/env python3
"""Quality gates for the release comparison campaign.

An arm's numbers are used only if it passes every gate here. Both gates exist
because of a recorded failure, not as ceremony.

* **Per-core concentration.** A previous bare-metal campaign on these hosts was
  invalidated after the fact: IRQ and wakeup locality funnelled all work onto
  one core, capping every arm at ~208k ops/s regardless of runtime, connection
  count or loop mode. "All runtimes are equal" was the artifact.

  Read from the arm's rezolus recording (`cpu_usage` by `id`), not from a
  hand-rolled /proc/stat sampler. The sampler this replaces counted *iowait* as
  busy, and a ringline worker blocked in `submit_and_wait` is accounted as
  iowait rather than idle — so an idle io_uring server looked like it was
  burning every core it owned (806% against tokio's 33%, where the truth was
  ~22% of one core). That error biased the comparison against io_uring
  specifically, since epoll-based runtimes have no such accounting quirk.

* **Little's law.** In closed loop `N = X * E[R]` is an identity. A violation
  means the measurement is wrong — coordinated omission, mis-counted ops, or a
  stalled arm — which is the class of error that forced the 2026-05
  withdrawal. Checked against the *mean*, because a percentile is not the
  quantity the law is about.

Usage: phase0-gate.py <arm-dir>
Expects results-*.json and, for the concentration gate, server.rez.
Exit 0 = PASS, 1 = REJECT.
"""
import json
import pathlib
import re
import subprocess
import sys

# One core carrying this share of all busy CPU is the funnel fingerprint. It is
# a ratio, not a level: a thread-per-core runtime is *supposed* to saturate its
# worker count and leave the rest idle, so "one core is hot" is normal and
# "one core is hot and the others are doing nothing" is not.
FUNNEL_SHARE = 0.6
# Below this much total CPU the arm is lightly loaded and concentration is a
# meaningless question — at one connection nothing should be saturated.
MIN_LOAD_CORES = 1.2
# A good closed-loop arm closes the law to a fraction of a percent; the real
# rig arms landed at 0.1-0.3%. 15% leaves room for warmup edges without
# admitting a broken arm.
LITTLE_TOLERANCE = 0.15


def rezolus_series_means(rez, query):
    """Run one PromQL query, returning {series_label: mean}."""
    try:
        out = subprocess.run(
            ["rezolus", "mcp", "query", str(rez), query],
            capture_output=True, text=True, timeout=180,
        ).stdout
    except Exception:  # noqa: BLE001 - an unreadable recording is a rejected arm
        return {}
    means, label = {}, None
    for line in out.split("\n"):
        if line.startswith("{"):
            label = line.strip().rstrip(":")
        elif "Mean:" in line and label is not None:
            try:
                means[label] = float(line.split("Mean:")[1].strip())
            except ValueError:
                pass
            label = None
    return means


def check_concentration(rez):
    if not rez.exists():
        return ["no server recording: cannot rule out the single-core funnel"]
    per_cpu = rezolus_series_means(rez, "sum by (id) (rate(cpu_usage[30s]))")
    if not per_cpu:
        return ["server recording has no per-CPU cpu_usage: cannot check concentration"]
    # cpu_usage is CPU-nanoseconds per second, so 1e9 == one core.
    cores = {k: v / 1e9 for k, v in per_cpu.items()}
    total = sum(cores.values())
    if total < MIN_LOAD_CORES:
        return []
    hottest = max(cores.values())
    share = hottest / total
    if share >= FUNNEL_SHARE:
        return [
            f"single-core funnel: the hottest core carries {share * 100:.0f}% of "
            f"{total:.1f} cores of busy CPU across {len(cores)} CPUs — this is the "
            f"artifact that invalidated a previous campaign, not a result"
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
    problems = check_concentration(arm / "server.rez")
    results = sorted(arm.glob("results-*.json"))
    if not results:
        problems.append("no results-*.json in the arm directory")
    for r in results:
        try:
            data = json.loads(r.read_text())
        except Exception as exc:  # noqa: BLE001
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
