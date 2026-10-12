#!/usr/bin/env python3
"""Collect and summarise the #622 step 6 matrix (generate.py).

    ./collect.py rows ids.txt rows.jsonl   # ids.txt: "<kernel> <rep> <experiment id>" lines
    ./collect.py table rows.jsonl          # per kernel and cell: base vs inc against the A/A pair
    ./collect.py rates rows.jsonl > rates.json   # phase l offered rates: 70% of base's median

A row is one arm of one boot. A client result with zero samples or no
JSON is kept with `ok: false`, so a failed arm is visible as one rather
than missing.

The gate per cell compares inc's median with the 2 x reps runs of base and
base2 together: throughput below their minimum, or p99 above their maximum,
fails the cell. inc's `buffer_ring_empty` fails the cell when its median is
above RING_EMPTY_RATIO times the smallest count among the base and base2
runs; when that smallest count is 0, any nonzero median fails.
"""

import json
import pathlib
import re
import statistics
import subprocess
import sys
import tarfile
import tempfile

SL = ["systemslab", "--systemslab-url", "http://systemslab"]
# inc's ring may run empty, but well below base's rate: at most this
# fraction of the smallest base or base2 count.
RING_EMPTY_RATIO = 0.10

COUNTERS = ["buffer_ring_empty", "recv_parked", "recv_fallback", "incremental", "plain", "lend_refused"]


def fetch(exp, dest):
    lst = subprocess.run(SL + ["artifact", "list", "--experiment", exp], capture_output=True, text=True).stdout
    for name in ["server-results.tgz", "client-results.tgz"]:
        m = re.search(rf"^(\S+)\s+\S+\s+{re.escape(name)}", lst, re.M) or re.search(rf"^(\S+).*{re.escape(name)}", lst, re.M)
        if not m:
            print(f"{exp}: no {name}", file=sys.stderr)
            continue
        out = dest / name
        subprocess.run(SL + ["artifact", "download", "--artifact", m.group(1), "--output", str(out)], capture_output=True)
        with tarfile.open(out) as t:
            t.extractall(dest / name.split("-")[0], filter="data")


def rows(ids_path, out_path):
    out = open(out_path, "w")
    for line in open(ids_path):
        if not line.strip():
            continue
        kernel, rep, exp = line.split()
        exp = exp.rsplit("/", 1)[-1]
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
            fetch(exp, d)
            for cj in sorted((d / "client" / "results").glob("*.json")):
                aid = cj.stem
                cell, arm = aid.rsplit("-", 1)
                row = {"kernel": kernel, "rep": int(rep), "exp": exp, "cell": cell, "arm": arm, "ok": False}
                try:
                    c = json.loads(cj.read_text())
                    if "bytes_per_sec" in c:
                        row.update(tput=c["bytes_per_sec"], ok=c["bytes"] > 0)
                    else:
                        row.update(tput=c["ops_per_sec"], p50=c["p50_ns"], p99=c["p99_ns"],
                                   p999=c["p999_ns"], ok=c["sample_count"] > 0)
                except (ValueError, KeyError):
                    pass
                sdir = d / "server" / "results"
                log = sdir / f"{aid}.server.log"
                if log.exists():
                    text = log.read_text(errors="replace")
                    m = re.search(r"VmHWM:\s+(\d+) kB", text)
                    row["rss_mib"] = int(m.group(1)) / 1024 if m else None
                    row["panic"] = "panicked" in text
                mj = sdir / f"{aid}.metrics.json"
                if mj.exists():
                    for e in json.loads(mj.read_text()):
                        if e.get("op") in COUNTERS:
                            row[e["op"]] = e["value"]
                out.write(json.dumps(row) + "\n")
        out.flush()
        print(kernel, rep, exp, file=sys.stderr)


def load(path):
    return [json.loads(l) for l in open(path) if l.strip()]


def group(rs):
    g = {}
    for r in rs:
        g.setdefault((r["kernel"], r["cell"]), {}).setdefault(r["arm"], []).append(r)
    return g


def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def table(path):
    rs = load(path)
    bad = [r for r in rs if not r["ok"] or r.get("panic")]
    for r in bad:
        print(f"FAILED ARM {r['kernel']} rep{r['rep']} {r['cell']} {r['arm']} panic={r.get('panic')}")
    hdr = f"{'kernel':5} {'cell':24} {'base':>10} {'A/A min-max':>21} {'inc':>10} {'d%':>6} {'p99 base':>9} {'p99 inc':>8} {'rss b/i MiB':>12} {'ring_empty b/i':>16} {'i/bmin':>6} gate"
    print(hdr)
    for (kernel, cell), arms in sorted(group([r for r in rs if r["ok"]]).items()):
        baseline = arms.get("base", []) + arms.get("base2", [])
        inc = arms.get("inc", [])
        if not baseline or not inc:
            continue
        bt = [r["tput"] for r in baseline]
        b_med, i_med = med(r["tput"] for r in arms.get("base", [])), med(r["tput"] for r in inc)
        bp = [r.get("p99") for r in baseline if r.get("p99")]
        ip = med(r.get("p99") for r in inc)
        fails = []
        if i_med < min(bt):
            fails.append("slower")
        if bp and ip and ip > max(bp):
            fails.append("p99")
        ie = med(r.get("buffer_ring_empty") for r in inc) or 0
        be_min = min((r.get("buffer_ring_empty") or 0) for r in baseline)
        if ie > RING_EMPTY_RATIO * be_min:
            fails.append("ring_empty")
        unit = 1e6 if "proxy" in cell else 1e3
        p99 = lambda v: f"{v / 1e6:.2f}" if v else "-"
        print(
            f"{kernel:5} {cell:24} {b_med / unit:10.1f} {min(bt) / unit:10.1f}-{max(bt) / unit:<10.1f} {i_med / unit:10.1f} "
            f"{100 * (i_med / b_med - 1):6.1f} {p99(med(bp)):>9} {p99(ip):>8} "
            f"{med(r.get('rss_mib') for r in arms.get('base', [])) or 0:5.0f}/{med(r.get('rss_mib') for r in inc) or 0:<6.0f} "
            f"{med(r.get('buffer_ring_empty') for r in arms.get('base', [])) or 0:>7}/{ie:<8} {100 * ie / be_min if be_min else 0:5.1f}% "
            f"{','.join(fails) or 'pass'}"
        )
    print("throughput: k ops/s, or MB/s for proxy cells; p99 in ms; ring_empty is a whole-run count")


def rates(path):
    out = {}
    for (kernel, cell), arms in group([r for r in load(path) if r["ok"]]).items():
        if "proxy" in cell or "base" not in arms:
            continue
        # One rate per cell for every kernel: the lowest kernel's 70%, so
        # no kernel is offered more than it sustains.
        r = int(0.7 * med(x["tput"] for x in arms["base"]))
        out[cell] = min(out.get(cell, r), r)
    json.dump(out, sys.stdout, indent=1, sort_keys=True)


if __name__ == "__main__":
    cmd = sys.argv[1]
    {"rows": lambda: rows(sys.argv[2], sys.argv[3]), "table": lambda: table(sys.argv[2]),
     "rates": lambda: rates(sys.argv[2])}[cmd]()
