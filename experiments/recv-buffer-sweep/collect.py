#!/usr/bin/env python3
"""Collect a recv-buffer sweep phase into one table (#416).

Pulls each chunk's client results and server metrics tarballs, joins them by
arm id, and prints the surface. Every row carries the counters that explain it,
not just the throughput that states it — an arm that lost while starving its
ring is a different finding from one that lost at a full ring, and the table
has to keep them apart.

Usage:
    ./collect.py <experiment-id> [<experiment-id> ...]
    ./collect.py --json results.json <experiment-id> ...
"""

import argparse
import io
import json
import re
import subprocess
import sys
import tarfile


def api(path: str) -> bytes:
    return subprocess.run(
        ["systemslab", "api", path], capture_output=True, check=True
    ).stdout


def artifacts(exp_id: str):
    d = json.loads(api(f"/api/v1/experiment/{exp_id}"))
    return {a.get("name"): a["id"] for a in d.get("artifacts", [])}, d.get("state")


def members(artifact_id: str) -> dict:
    """Every file in a results tarball, keyed by basename."""
    raw = api(f"/api/v1/artifact/{artifact_id}")
    out = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tf:
        for m in tf.getmembers():
            if m.isfile():
                f = tf.extractfile(m)
                if f:
                    out[m.name.split("/")[-1]] = f.read()
    return out


ARM_RE = re.compile(
    r"^(?P<mode>echo|forward)-(?P<msg>\d+b|stream)-buf(?P<buf>\d+)-ring(?P<ring>\d+)-c(?P<conns>\d+)$"
)


def parse_arm(aid: str):
    m = ARM_RE.match(aid)
    return m.groupdict() if m else None


def counter(metrics: dict, group: str, op: str) -> int:
    for e in metrics:
        if e["name"] == group and e.get("op") == op:
            return e["value"]
    return 0


def collect(exp_ids):
    rows = []
    for exp in exp_ids:
        arts, state = artifacts(exp)
        if state != "success":
            print(f"  ! {exp}: state={state}, skipping", file=sys.stderr)
            continue
        if "client-results.tgz" not in arts or "server-results.tgz" not in arts:
            print(f"  ! {exp}: missing results tarballs, skipping", file=sys.stderr)
            continue
        client = members(arts["client-results.tgz"])
        server = members(arts["server-results.tgz"])

        for name, body in sorted(client.items()):
            if not name.endswith(".json"):
                continue
            aid = name[: -len(".json")]
            arm = parse_arm(aid)
            if not arm:
                continue
            try:
                res = json.loads(body)
            except json.JSONDecodeError:
                print(f"  ! {aid}: client produced no parsable result", file=sys.stderr)
                continue

            mfile = server.get(f"{aid}.metrics.json")
            metrics = json.loads(mfile) if mfile else []
            # A missing or empty metrics file is not a zero — it means the arm's
            # server never dumped, and its counters must not be read as "clean".
            have_metrics = bool(metrics)

            rows.append(
                {
                    "arm": aid,
                    **arm,
                    "buf": int(arm["buf"]),
                    "ring": int(arm["ring"]),
                    "conns": int(arm["conns"]),
                    "mib_per_worker": int(arm["buf"]) * int(arm["ring"]) / (1024 * 1024),
                    "ops_per_sec": res.get("ops_per_sec"),
                    "gbit_per_sec": res.get("gbit_per_sec"),
                    "p99_ns": (res.get("latency") or {}).get("p99_ns"),
                    "have_metrics": have_metrics,
                    "ring_empty": counter(metrics, "ringline/pool", "buffer_ring_empty"),
                    "recv_parked": counter(metrics, "ringline/pool", "recv_parked"),
                    "recv_fallback": counter(metrics, "ringline/pool", "recv_fallback"),
                    "fwd_throttled": counter(metrics, "ringline/pool", "forward_throttled"),
                    "bytes_recv": counter(metrics, "ringline/bytes", "received"),
                }
            )
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("experiments", nargs="+")
    ap.add_argument("--json", dest="json_out")
    args = ap.parse_args()

    rows = collect(args.experiments)
    if not rows:
        print("no rows collected", file=sys.stderr)
        sys.exit(1)

    rows.sort(key=lambda r: (r["mode"], r["msg"], r["conns"], r["buf"], r["ring"]))
    hdr = f"{'arm':<46}{'MiB/wkr':>9}{'ops/s':>12}{'Gbit/s':>9}{'ring_empty':>12}{'parked':>10}{'fallback':>10}"
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        ops = f"{r['ops_per_sec']:,.0f}" if r["ops_per_sec"] else "-"
        gb = f"{r['gbit_per_sec']:.2f}" if r["gbit_per_sec"] else "-"
        # An arm whose server never dumped shows `?`, never 0: silence is not
        # a clean ring.
        star = "" if r["have_metrics"] else "  (no metrics)"
        ring_empty = f"{r['ring_empty']:,}" if r["have_metrics"] else "?"
        parked = f"{r['recv_parked']:,}" if r["have_metrics"] else "?"
        fb = f"{r['recv_fallback']:,}" if r["have_metrics"] else "?"
        print(
            f"{r['arm']:<46}{r['mib_per_worker']:>9.1f}{ops:>12}{gb:>9}"
            f"{ring_empty:>12}{parked:>10}{fb:>10}{star}"
        )

    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(rows, f, indent=2)
        print(f"\nwrote {args.json_out} ({len(rows)} arms)")


if __name__ == "__main__":
    main()
