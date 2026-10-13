#!/usr/bin/env python3
"""Collect and summarise the #622 step 6 matrix (generate.py).

    ./collect.py rows ids.txt rows.jsonl   # ids.txt: "<kernel> <rep> <experiment id>" lines
    ./collect.py table rows.jsonl          # per kernel and cell: base vs inc against the A/A pair
    ./collect.py rates rows.jsonl > rates.json   # phase l offered rates: 70% of base's median
    ./collect.py ladder rows.jsonl > ladder.json # phase s: base's median per kernel and cell
    ./collect.py knees rows.jsonl          # phase s: SLO-limited throughput per arm
    ./collect.py searches rows.jsonl       # phase q: the client's search result per arm

A row is one arm of one boot. A client result with zero samples or no
JSON is kept with `ok: false`, so a failed arm is visible as one rather
than missing. Phase s adds a row per ladder step (`step` sNN). Phase q adds
a row per search step (`step` qNN) and one `search` row per arm with the
client's result; an arm whose client printed no result line has
`stopped: "no result"`. With generate.py --cooldown, a `qNNc` row
(`phase` "cooldown", no `pass`) follows each failed step NN that did not
end the search.

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
                # phase s names each ladder step <aid>.sNN.json
                aid, _, step = cj.stem.partition(".")
                cell, arm = aid.rsplit("-", 1)
                row = {"kernel": kernel, "rep": int(rep), "exp": exp, "cell": cell, "arm": arm, "ok": False}
                if step:
                    row["step"] = step
                try:
                    c = json.loads(cj.read_text())
                    if "bytes_per_sec" in c:
                        row.update(tput=c["bytes_per_sec"], ok=c["bytes"] > 0)
                    else:
                        row.update(tput=c["ops_per_sec"], p50=c["p50_ns"], p99=c["p99_ns"],
                                   p999=c["p999_ns"], ok=c["sample_count"] > 0)
                        if c.get("offered_rate"):
                            row["offered"] = c["offered_rate"]
                        if c.get("per_conn"):
                            row["per_conn"] = c["per_conn"]
                except (ValueError, KeyError):
                    pass
                row.update(server_fields(d, aid))
                out.write(json.dumps(row) + "\n")
            for sj in sorted((d / "client" / "results").glob("*.search.jsonl")):
                aid = sj.name.removesuffix(".search.jsonl")
                cell, arm = aid.rsplit("-", 1)
                key = {"kernel": kernel, "rep": int(rep), "exp": exp, "cell": cell, "arm": arm}
                result = None
                for line in sj.read_text().splitlines():
                    try:
                        e = json.loads(line)
                    except ValueError:
                        continue
                    if e.get("type") == "step":
                        out.write(json.dumps(key | {
                            "step": f"q{e['index']:02}", "phase": e["phase"],
                            "offered": e["offered_rate"], "tput": e["ops_per_sec"],
                            "p50": e["p50_ns"], "p99": e["p99_ns"], "p999": e["p999_ns"],
                            "pass": e["pass"], "fail_reason": e["fail_reason"],
                            "ok": e["sample_count"] > 0,
                            "drain_ms": e.get("drain_ms"),
                            "send_pool_full": e.get("send_pool_full"),
                            "per_conn": e.get("per_conn"),
                        }) + "\n")
                    elif e.get("type") == "cooldown":
                        out.write(json.dumps(key | {
                            "step": f"q{e['after_index']:02}c", "phase": "cooldown",
                            "offered": e["offered_rate"], "tput": e["ops_per_sec"],
                            "p50": e["p50_ns"], "p99": e["p99_ns"], "p999": e["p999_ns"],
                            "ok": e["sample_count"] > 0, "drain_ms": e["drain_ms"],
                            "send_pool_full": e["send_pool_full"], "per_conn": e["per_conn"],
                        }) + "\n")
                    elif e.get("type") in ("result", "step_timeout"):
                        result = e
                row = key | {"search": True, "ok": False, "knee": None, "stopped": "no result"}
                if result and result["type"] == "result":
                    row.update(ok=True, knee=result["knee"], stopped=result["stopped"], steps=result["steps"])
                elif result:
                    # A client built before 75643f0 prints no `period`.
                    period = result.get("period", "step")
                    where = {"cooldown": f"cooldown after step {result['index']}",
                             "step": f"step {result['index']}"}.get(period, period)
                    row["stopped"] = f"step_timeout in {where}"
                row.update(server_fields(d, aid))
                out.write(json.dumps(row) + "\n")
        out.flush()
        print(kernel, rep, exp, file=sys.stderr)


def server_fields(d, aid):
    """Peak RSS, panic, forward failure and retry counts, forward failure
    log lines, and ring counters from one arm's server results."""
    row = {}
    sdir = d / "server" / "results"
    log = sdir / f"{aid}.server.log"
    if log.exists():
        text = log.read_text(errors="replace")
        m = re.search(r"VmHWM:\s+(\d+) kB", text)
        row["rss_mib"] = int(m.group(1)) / 1024 if m else None
        row["panic"] = "panicked" in text
        # A server built before 23072fa prints no counts and logs this line
        # for every failed forward; a later one logs it once per stall.
        row["forward_fail_lines"] = text.count("forward_recv_buf failed: ")
        for k in ("forward_failures", "forward_retries"):
            m = re.search(rf"bench-server: {k} = (\d+)", text)
            row[k] = int(m.group(1)) if m else None
    mj = sdir / f"{aid}.metrics.json"
    if mj.exists():
        for e in json.loads(mj.read_text()):
            if e.get("op") in COUNTERS:
                row[e["op"]] = e["value"]
    return row


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


def ladder(path):
    """Phase s input: base's closed-loop median per kernel and cell."""
    out = {}
    for (kernel, cell), arms in group([r for r in load(path) if r["ok"]]).items():
        if "base" in arms:
            out.setdefault(kernel, {})[cell] = int(med(x["tput"] for x in arms["base"]))
    json.dump(out, sys.stdout, indent=1, sort_keys=True)


from generate import SLO_P99_NS  # noqa: E402  phase s SLOs, one definition


def knees(path):
    """Per kernel, cell and arm: the highest achieved rate over the ladder
    steps that achieved at least 90% of the offered rate with p99 within
    the cell's SLO. Medians over reps; inc is compared with base and base2."""
    rs = [r for r in load(path) if r.get("step", "").startswith("s") and r["ok"]]
    per = {}
    for r in rs:
        msg = int(re.search(r"-(\d+)b-", r["cell"]).group(1))
        good = r["tput"] >= 0.9 * r["offered"] and r["p99"] <= SLO_P99_NS[msg]
        k = (r["kernel"], r["cell"], r["arm"], r["rep"])
        top = max((s["offered"] for s in rs if (s["kernel"], s["cell"], s["arm"], s["rep"]) == k), default=0)
        best = per.setdefault(k, {"knee": 0, "top": top})
        if good:
            best["knee"] = max(best["knee"], r["tput"])
    print(f"{'kernel':5} {'cell':24} {'base':>10} {'A/A min-max':>21} {'inc':>10} {'d%':>7} note")
    for kc in sorted({(k[0], k[1]) for k in per}):
        arms = {}
        capped = []
        for (kernel, cell, arm, rep), v in per.items():
            if (kernel, cell) == kc:
                arms.setdefault(arm, []).append(v["knee"])
                if v["knee"] and v["knee"] >= 0.9 * v["top"]:
                    capped.append(arm)
        b = arms.get("base", []) + arms.get("base2", [])
        if not b or not arms.get("inc"):
            continue
        bm, im = med(arms["base"]), med(arms["inc"])
        d = f"{100 * (im / bm - 1):7.1f}" if bm else "      -"
        note = ("ladder top reached: " + ",".join(sorted(set(capped)))) if capped else ""
        print(f"{kc[0]:5} {kc[1]:24} {bm / 1e3:10.1f} {min(b) / 1e3:10.1f}-{max(b) / 1e3:<10.1f} {im / 1e3:10.1f} {d} {note}")
    print("knee: k ops/s achieved at the highest step meeting p99 SLO and >= 90% of offered; 0 = no step met it")


def searches(path):
    """Phase q: per kernel, cell and arm, the median over reps of the
    client's knee, the highest offered rate whose step passed the SLO. An
    arm with no passing step counts as 0. inc is compared with base and
    base2; any arm that did not converge is named in the note."""
    rs = [r for r in load(path) if r.get("search")]
    g = group(rs)
    print(f"{'kernel':5} {'cell':24} {'base':>10} {'A/A min-max':>21} {'inc':>10} {'d%':>7} note")
    for (kernel, cell) in sorted(g):
        arms = g[(kernel, cell)]
        knee = {a: [r["knee"] or 0 for r in v] for a, v in arms.items()}
        b = knee.get("base", []) + knee.get("base2", [])
        if not b or not knee.get("inc"):
            continue
        bm, im = med(knee["base"]), med(knee["inc"])
        d = f"{100 * (im / bm - 1):7.1f}" if bm else "      -"
        odd = sorted({f"{r['arm']} r{r['rep']}: {r['stopped']}" for v in arms.values() for r in v
                      if r["stopped"] != "converged"})
        print(f"{kernel:5} {cell:24} {bm / 1e3:10.1f} {min(b) / 1e3:10.1f}-{max(b) / 1e3:<10.1f} {im / 1e3:10.1f} {d} {'; '.join(odd)}")
    print("knee: k req/s offered at the highest search step meeting the p99 SLO and >= 90% of offered; 0 = none")


if __name__ == "__main__":
    cmd = sys.argv[1]
    {"rows": lambda: rows(sys.argv[2], sys.argv[3]), "table": lambda: table(sys.argv[2]),
     "rates": lambda: rates(sys.argv[2]), "ladder": lambda: ladder(sys.argv[2]),
     "knees": lambda: knees(sys.argv[2]), "searches": lambda: searches(sys.argv[2])}[cmd]()
