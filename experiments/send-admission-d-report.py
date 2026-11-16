#!/usr/bin/env python3
"""Summarise Experiment D's runs and apply the design's decision rule.

Each argument is a directory made by `systemslab artifact download-all <id>`
for one run of send-admission-d.toml. The RUN and WORKER lines come from the
job's logs.json; admission-load's and admission-server's JSON come from
results.tgz, or from the LOAD and SERVER lines when there is no results.tgz.
Runs of workloads other than U are also parsed by send-admission-c0-report.py's
`one`.

An arm is (order, stream_piece, stream_gate). A cell is (workload,
stall_peers, tcp_wmem_max, drainers, drain_read_bps, pipeline, admit_ms,
host). Runs are grouped by cell and arm and never merged across either. If
a cell's runs differ in any other RUN parameter (other than rep and
load_rc), the cell is split by those parameters, all its arms with it, and a
warning is printed; each part is then a cell of its own, in the decision
too.

Per cell and arm, over its runs:
- fast: the median [min-max] of the per-run p99 of the fast small replies'
  delay from scheduled time to admission, in ms; and the fast small replies
  beyond admit_ms or not admitted, summed and per run;
- 7 MiB: the heads (tag 0xffffffff) the client read in full, of those
  requested, and their time from request to last byte (median, max) over
  the completed ones, in ms;
- drain: the drainers' replies read in full, of those requested (in W5,
  the slow readers of large replies);
- free: the fewest free main-region slots during a head, from the server's
  10 ms samples: the minimum over the runs and the median of the per-run
  minima;
- waited: pieces whose SendCompletion waited for room, of the pieces sent,
  by connection: stalled peers (tags below 0x80000000), drainers, and other
  (the head);
- aborts by cause, summed;
- U: per run the closed loop's median and p99 per reply in microseconds and
  the worker's CPU per byte (cpu_ns from the WORKER line over the bytes of
  every reply the loop read, its uncounted warmup included); over the runs
  the median [min-max].

Decision (docs/send-admission-design.md, section 4, Experiment D), for each
arm with the gate on:
- in W1, W2, W4 at tcp_wmem 1 MiB (every k run) and W5, it completes every
  7 MiB reply and every W5 slow reader's reply, keeps the fast small p99 at
  or below 10 ms (the median over the cell's runs, as Experiment C's report
  reads it; the maximum is printed beside it), and has no more fast small
  replies beyond admit_ms than O0 whole (counted per run, since O0 whole has
  more runs). A cell the arm or O0 whole did not run fails the condition.
- Among the passing arms, the larger stream_piece is named.
- U: each gated arm's CPU per byte against O0 whole's; above twice, the arm
  goes to the owner before it is chosen. The difference in median time per
  reply and in CPU per reply, divided by the pieces per reply, is the cost of
  one piece; O0 whole's spread is printed beside it.

Calibration runs (workload CAL) also get, per run: the fewest free
main-region slots over the load's window and, divided over the stalled
peers, the slots each held (h for whole replies, h_p for pieces); and from
ss.log in results.tgz, the largest sk_sndbuf (tb) and wmem_queued (w) seen
for the first stalled peer and the first drainer.

Then the prediction's refutation conditions: in any cell, a gated arm's fast
small p99 (median over runs) above 10 ms, a 7 MiB reply or a W5 slow reader's
reply not completed, fewer than 3/4 of the pool free during a 7 MiB reply in
W4, or a stalled peer's gated piece that waited for room in W4.
"""

import importlib.util
import io
import json
import pathlib
import re
import sys
import tarfile
from collections import defaultdict
from statistics import median

here = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("c0", here / "send-admission-c0-report.py")
c0 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c0)

HEAD_TAG = 0xFFFFFFFF
DRAIN_TAG = 0x80000000
ORDERS = {
    "strict": "O0",
    "first-fit": "O2",
    "bounded": "O3",
    "bounded-idle": "O4",
    "split-pool": "O5",
    "budget": "O7",
    "budget-idle": "O7w",
}
CELL_KEYS = (
    "workload",
    "stall_peers",
    "tcp_wmem_max",
    "drainers",
    "drain_read_bps",
    "pipeline",
    "admit_ms",
    "host",
)
ARM_KEYS = ("order", "stream_piece", "stream_gate")
NOT_GROUPED = {"rep", "load_rc"}
DECISION_WORKLOADS = ("W1", "W2", "W4", "W5")


def arm_name(arm):
    order, piece, gate = arm
    o = ORDERS.get(order, order)
    if piece in (None, "0"):
        return f"{o} whole"
    kib = int(piece) // 1024
    return f"{o} {'gated' if gate == 'on' else 'ungated'} {kib}K"


def is_o0_whole(arm):
    return arm[0] == "strict" and arm[1] in (None, "0")


def fmt(x, spec=".1f"):
    return "-" if x is None else format(x, spec)


def spread(xs, spec=".1f"):
    xs = [x for x in xs if x is not None]
    if not xs:
        return "-"
    return f"{format(median(xs), spec)} [{format(min(xs), spec)}-{format(max(xs), spec)}]"


def worker_line(root):
    """cpu_ns from the run's worker.txt, or from its WORKER log line."""
    for tgz in root.rglob("results.tgz"):
        with tarfile.open(fileobj=io.BytesIO(tgz.read_bytes())) as t:
            for m in t.getmembers():
                if m.isfile() and pathlib.Path(m.name).name == "worker.txt":
                    return parse_worker(t.extractfile(m).read().decode())
    for text in c0.log_lines(root):
        if text.startswith("WORKER "):
            return parse_worker(text)
    return None


def parse_worker(text):
    words = dict(w.split("=", 1) for w in text.split() if "=" in w)
    try:
        return int(words["cpu_ns"])
    except (KeyError, ValueError):
        return None


def ss_lines(root):
    """The SS lines of the run's ss.log, if its results.tgz has one."""
    for tgz in root.rglob("results.tgz"):
        with tarfile.open(fileobj=io.BytesIO(tgz.read_bytes())) as t:
            for m in t.getmembers():
                if m.isfile() and pathlib.Path(m.name).name == "ss.log":
                    text = t.extractfile(m).read().decode(errors="replace")
                    return [l for l in text.splitlines() if l.startswith("SS ")]
    return []


def raw_run(root):
    """The RUN parameters, and the load's and server's JSON."""
    run = load = server = None
    for text in c0.log_lines(root):
        if text.startswith("RUN "):
            run = c0.parse_run(text)
        elif text.startswith("LOAD "):
            load = json.loads(text[5:])
        elif text.startswith("SERVER "):
            server = json.loads(text[7:])
    got = c0.from_tgz(root)
    if got:
        load, server = got
    if not (run and load and server):
        raise SystemExit(f"{root}: missing RUN, LOAD or SERVER")
    return run, load, server


def one(root):
    root = pathlib.Path(root)
    run, load, server = raw_run(root)
    st = server["statistics"]
    pool = server["pool_slots"]
    r = dict(root=str(root), run=run, rc=int(run.get("load_rc", "-1")), pool=pool)
    r["cpu_ns"] = worker_line(root)
    r["aborts"] = server.get("aborts", {})
    larges = st["large_replies"]
    heads = [l for l in larges if l["tag"] == HEAD_TAG]
    waited = defaultdict(lambda: [0, 0])
    for l in larges:
        kind = (
            "other"
            if l["tag"] == HEAD_TAG
            else ("drainer" if l["tag"] & DRAIN_TAG else "stalled")
        )
        waited[kind][0] += l.get("pieces_waited", 0)
        waited[kind][1] += l.get("pieces_sent", 0)
    r["waited"] = dict(waited)
    frees = [h["min_main_free"] for h in heads if h.get("min_main_free") is not None]
    r["head_min_free"] = min(frees) if frees else None
    r["server_heads_completed"] = sum(1 for h in heads if h.get("outcome") == "completed")
    r["server_heads"] = len(heads)
    lo, hi = load.get("window_ns", [0, 0])
    samples = [f for t, f in st.get("main_free_samples", []) if lo <= t * 1_000_000 <= hi]
    r["window_min_free"] = min(samples) if samples else None
    ss = defaultdict(lambda: [0, 0])
    for line in ss_lines(root):
        words = line.split()
        kind = words[2] if len(words) > 2 else "?"
        tb = re.search(r"\btb(\d+)", line)
        w = re.search(r",w(\d+)", line)
        if tb:
            ss[kind][0] = max(ss[kind][0], int(tb.group(1)))
        if w:
            ss[kind][1] = max(ss[kind][1], int(w.group(1)))
    r["ss"] = dict(ss)
    if "closed_loop" in load:
        cl = load["closed_loop"]
        r["u"] = cl
        replies = cl["warmup"] + cl["completed"]
        r["cpu_per_byte"] = (
            r["cpu_ns"] / (replies * cl["size"])
            if r["cpu_ns"] is not None and replies
            else None
        )
        r["cpu_per_reply_us"] = (
            r["cpu_ns"] / replies / 1000 if r["cpu_ns"] is not None and replies else None
        )
        r["fast_p99"] = None
        r["fast_bad"] = 0
        r["heads_ok"] = cl["completed"]
        r["heads_n"] = cl["requests"]
        r["head_times"] = [u / 1000 for u in cl["us"]]
        r["slow_ok"] = r["slow_n"] = 0
        return r
    base = c0.one(root)
    r["fast_p99"] = base["fast_p99"]
    r["fast_bad"] = base["fast_beyond"] + base["fast_na"]
    done = load.get("large_completed")
    if done is None:  # a load from before D: a head counts if it has a time
        done = [x is not None for x in load.get("large_ms", [])]
    if int(run.get("large_size", "1")) == 0:
        done = []
    r["heads_ok"] = sum(1 for x in done if x)
    r["heads_n"] = len(done)
    times = load.get("large_us") or [None if x is None else x * 1000 for x in load.get("large_ms", [])]
    r["head_times"] = [t / 1000 for t, ok in zip(times, done) if ok and t is not None]
    dc = load["drainers"].get("completed")
    if dc is None:
        dc = [[x is not None for x in ep] for ep in load["drainers"]["ms"]]
    flat = [x for ep in dc for x in ep]
    r["slow_ok"] = sum(1 for x in flat if x)
    r["slow_n"] = len(flat)
    r["u"] = None
    return r


def group(rows):
    """{(cell, arm): [rows]}. A cell whose runs differ in a RUN parameter
    outside the cell and arm keys (other than rep and load_rc) is split by
    that parameter, across all its arms, so that no cell or arm is merged."""
    cells = defaultdict(list)
    for r in rows:
        cells[tuple((k, r["run"].get(k, "-")) for k in CELL_KEYS)].append(r)
    out = defaultdict(list)
    for cell, rs in cells.items():
        others = sorted(
            {k for r in rs for k in r["run"]} - set(CELL_KEYS) - set(ARM_KEYS) - NOT_GROUPED
        )
        differ = [k for k in others if len({r["run"].get(k) for r in rs}) > 1]
        if differ:
            print(f"warning: {cell_name(cell)}: runs differ in {differ}; split", file=sys.stderr)
        for r in rs:
            arm = tuple(
                r["run"].get(k, "0" if k == "stream_piece" else "off" if k == "stream_gate" else "-")
                for k in ARM_KEYS
            )
            extra = tuple((k, r["run"].get(k)) for k in differ)
            out[(cell + extra, arm)].append(r)
    return dict(out)


def cell_name(cell):
    return " ".join(f"{k}={v}" for k, v in cell)


def summarise(rs):
    p99 = [r["fast_p99"] for r in rs if r["fast_p99"] is not None]
    times = [t for r in rs for t in r["head_times"]]
    frees = [r["head_min_free"] for r in rs if r["head_min_free"] is not None]
    waited = defaultdict(lambda: [0, 0])
    aborts = defaultdict(int)
    for r in rs:
        for k, (w, n) in r["waited"].items():
            waited[k][0] += w
            waited[k][1] += n
        for k, v in r["aborts"].items():
            aborts[k] += v
    u = [r["u"] for r in rs if r["u"]]
    return dict(
        n=len(rs),
        rc_bad=sum(1 for r in rs if r["rc"] != 0),
        fast_p99=median(p99) if p99 else None,
        fast_p99_max=max(p99) if p99 else None,
        fast_p99_runs=p99,
        fast_bad=sum(r["fast_bad"] for r in rs),
        fast_bad_per_run=sum(r["fast_bad"] for r in rs) / len(rs),
        heads_ok=sum(r["heads_ok"] for r in rs),
        heads_n=sum(r["heads_n"] for r in rs),
        head_med=median(times) if times else None,
        head_max=max(times) if times else None,
        slow_ok=sum(r["slow_ok"] for r in rs),
        slow_n=sum(r["slow_n"] for r in rs),
        free_min=min(frees) if frees else None,
        free_med=median(frees) if frees else None,
        pool=rs[0]["pool"],
        waited=dict(waited),
        aborts=dict(aborts),
        u_p50=[x["p50_us"] for x in u],
        u_p99=[x["p99_us"] for x in u],
        u_cpu_b=[r["cpu_per_byte"] for r in rs if r.get("cpu_per_byte") is not None],
        u_cpu_reply=[r["cpu_per_reply_us"] for r in rs if r.get("cpu_per_reply_us") is not None],
        u_rcvbuf=sorted({x["rcvbuf_reported"] for x in u}),
        u_size=u[0]["size"] if u else None,
        u_errors=[x["error"] for x in u if x.get("error")],
    )


def waited_str(w):
    return " ".join(f"{k} {w[k][0]}/{w[k][1]}" for k in ("stalled", "drainer", "other") if k in w) or "-"


def print_tables(groups):
    by_cell = defaultdict(dict)
    for (cell, arm), rs in groups.items():
        by_cell[cell][arm] = summarise(rs)
    for cell in sorted(by_cell):
        arms = by_cell[cell]
        print(f"\n== {cell_name(cell)}")
        u = dict(cell).get("workload") == "U"
        if u:
            print("arm                runs | p50 us [range]            p99 us [range]            | CPU ns/B [range]          CPU us/reply | rcvbuf   | completed")
        else:
            print("arm                runs | fast p99 ms [range]  max | fast bad (per run) | 7MiB ok/n  med ms  max ms | drain ok/n | free min med | waited (w/sent) | aborts")
        for arm in sorted(arms, key=arm_sort):
            s = arms[arm]
            if u:
                print(
                    f"{arm_name(arm):18} {s['n']:>4} | {spread(s['u_p50'], '.0f'):25} {spread(s['u_p99'], '.0f'):25} | "
                    f"{spread(s['u_cpu_b'], '.3f'):25} {spread(s['u_cpu_reply'], '.0f'):>12} | {s['u_rcvbuf']} | {s['heads_ok']}/{s['heads_n']}"
                    + (f" errors {s['u_errors']}" if s["u_errors"] else "")
                )
                continue
            ab = " ".join(f"{k}={v}" for k, v in sorted(s["aborts"].items()) if v) or "none"
            print(
                f"{arm_name(arm):18} {s['n']:>4} | {spread(s['fast_p99_runs']):20} {fmt(s['fast_p99_max']):>4} | "
                f"{s['fast_bad']:>4} ({s['fast_bad_per_run']:.1f}) | "
                f"{s['heads_ok']:>3}/{s['heads_n']:<3} {fmt(s['head_med']):>7} {fmt(s['head_max']):>7} | "
                f"{s['slow_ok']:>3}/{s['slow_n']:<3}  | {fmt(s['free_min'], '.0f'):>4} {fmt(s['free_med'], '.0f'):>4} | "
                f"{waited_str(s['waited'])} | {ab}"
                + (f" | load_rc!=0 in {s['rc_bad']}" if s["rc_bad"] else "")
            )
    return by_cell


def arm_sort(arm):
    order, piece, gate = arm
    return (order != "strict", int(piece or 0) != 0, gate == "on", int(piece or 0), order)


def decision(by_cell):
    print("\n== decision (design, Experiment D)")
    arms = sorted({a for cells in by_cell.values() for a in cells if a[2] == "on"}, key=arm_sort)
    if not arms:
        print("no gated arm ran")
    required = [
        c for c in by_cell
        if dict(c)["workload"] in DECISION_WORKLOADS
        and not (dict(c)["workload"] == "W4" and dict(c)["tcp_wmem_max"] != "1048576")
    ]
    for w in DECISION_WORKLOADS:
        if not any(dict(c)["workload"] == w for c in required):
            print(f"  no {w} cell ran: every gated arm fails for want of it")
    passing = []
    for arm in arms:
        reasons, checks = [], 0
        for w in DECISION_WORKLOADS:
            if not any(dict(c)["workload"] == w for c in required):
                reasons.append(f"{w}: not run")
        for cell in sorted(required):
            cells = by_cell[cell]
            name = cell_name(cell)
            s = cells.get(arm)
            o0 = next((v for a, v in cells.items() if is_o0_whole(a)), None)
            if s is None:
                reasons.append(f"{name}: this arm did not run")
                continue
            checks += 1
            ok_heads = s["heads_n"] > 0 and s["heads_ok"] == s["heads_n"]
            print(f"  {arm_name(arm)} | {name}: 7 MiB {s['heads_ok']}/{s['heads_n']} {'ok' if ok_heads else 'FAIL'}", end="")
            if not ok_heads:
                reasons.append(f"{name}: 7 MiB replies completed {s['heads_ok']}/{s['heads_n']}")
            if dict(cell)["workload"] == "W5":
                ok_slow = s["slow_n"] > 0 and s["slow_ok"] == s["slow_n"]
                print(f"; slow readers {s['slow_ok']}/{s['slow_n']} {'ok' if ok_slow else 'FAIL'}", end="")
                if not ok_slow:
                    reasons.append(f"{name}: slow readers' replies completed {s['slow_ok']}/{s['slow_n']}")
            ok_p99 = s["fast_p99"] is not None and s["fast_p99"] <= 10.0
            print(f"; fast p99 {fmt(s['fast_p99'])} ms (max {fmt(s['fast_p99_max'])}) {'ok' if ok_p99 else 'FAIL'}", end="")
            if not ok_p99:
                reasons.append(f"{name}: fast p99 {fmt(s['fast_p99'])} ms > 10 ms")
            if o0 is None:
                print("; O0 whole did not run: FAIL")
                reasons.append(f"{name}: no O0 whole runs to compare")
                continue
            ok_bad = s["fast_bad_per_run"] <= o0["fast_bad_per_run"]
            print(
                f"; fast beyond {s['fast_bad_per_run']:.1f}/run against O0 whole's "
                f"{o0['fast_bad_per_run']:.1f} {'ok' if ok_bad else 'FAIL'}"
            )
            if not ok_bad:
                reasons.append(
                    f"{name}: {s['fast_bad_per_run']:.1f} fast replies beyond admit_ms per run against O0 whole's {o0['fast_bad_per_run']:.1f}"
                )
        verdict = "passes" if not reasons and checks else "fails"
        print(f"{arm_name(arm)}: {verdict}")
        for reason in reasons:
            print(f"    {reason}")
        if verdict == "passes":
            passing.append(arm)
    if passing:
        best = max(passing, key=lambda a: int(a[1]))
        print(f"larger passing p: {int(best[1]) // 1024} KiB ({arm_name(best)})")
    else:
        print("no gated arm passes: the result goes to the owner, and Experiment C's outcome stands for plaintext")
    u_cost(by_cell)


def u_cost(by_cell):
    print("\n== U: cost of pieces against O0 whole")
    ucells = [c for c in by_cell if dict(c)["workload"] == "U"]
    if not ucells:
        print("no U cell ran")
    for cell in sorted(ucells):
        cells = by_cell[cell]
        o0 = next((v for a, v in cells.items() if is_o0_whole(a)), None)
        print(cell_name(cell))
        if o0 is None or not o0["u_cpu_b"]:
            print("  no O0 whole runs")
            continue
        base_b = median(o0["u_cpu_b"])
        base_t = median(o0["u_p50"])
        base_r = median(o0["u_cpu_reply"])
        print(
            f"  O0 whole: CPU {base_b:.3f} ns/B (spread {max(o0['u_cpu_b']) - min(o0['u_cpu_b']):.3f}), "
            f"p50 {base_t:.0f} us (spread {max(o0['u_p50']) - min(o0['u_p50']):.0f}), {len(o0['u_cpu_b'])} runs"
        )
        for arm in sorted(cells, key=arm_sort):
            if is_o0_whole(arm):
                continue
            s = cells[arm]
            if not s["u_cpu_b"]:
                print(f"  {arm_name(arm)}: no CPU figure")
                continue
            ratio = median(s["u_cpu_b"]) / base_b if base_b else None
            line = f"  {arm_name(arm)}: CPU {median(s['u_cpu_b']):.3f} ns/B, x{fmt(ratio, '.2f')} O0 whole"
            if arm[1] not in (None, "0") and s["u_size"]:
                pieces = -(-s["u_size"] // int(arm[1]))
                dt = (median(s["u_p50"]) - base_t) / pieces
                dc = (median(s["u_cpu_reply"]) - base_r) / pieces
                line += f"; per piece ({pieces} a reply): {dt:+.1f} us of reply time, {dc:+.1f} us of worker CPU"
            if arm[2] == "on" and ratio is not None and ratio > 2:
                line += "  -> more than twice O0 whole per byte: to the owner before it is chosen"
            print(line)


def calibration(groups):
    cal = [(c, a, rs) for (c, a), rs in groups.items() if dict(c)["workload"] == "CAL"]
    if not cal:
        return
    print("\n== calibration: slots held per stalled peer, and the sockets' buffers")
    for cell, arm, rs in sorted(cal, key=lambda x: (x[0], arm_sort(x[1]))):
        k = int(dict(cell)["stall_peers"])
        for r in sorted(rs, key=lambda r: r["run"].get("rep", "")):
            held = None if r["window_min_free"] is None else r["pool"] - r["window_min_free"]
            per = None if held is None or k == 0 else held / k
            sock = "; ".join(
                f"{kind} tb {v[0]} w {v[1]}" for kind, v in sorted(r["ss"].items())
            ) or "no ss.log"
            print(
                f"{arm_name(arm):18} k={k} rep {r['run'].get('rep')}: fewest free {fmt(r['window_min_free'], '.0f')} "
                f"of {r['pool']}, held {fmt(held, '.0f')}, per stalled peer {fmt(per, '.1f')} | {sock}"
            )


def prediction(by_cell):
    print("\n== prediction's refutation conditions (gated arms)")
    refuted = []
    for cell in sorted(by_cell):
        c = dict(cell)
        if c["workload"] in ("U", "CAL"):
            continue
        for arm, s in by_cell[cell].items():
            if arm[2] != "on":
                continue
            name = f"{arm_name(arm)} | {cell_name(cell)}"
            if s["fast_p99"] is not None and s["fast_p99"] > 10.0:
                refuted.append(f"{name}: fast p99 {fmt(s['fast_p99'])} ms > 10 ms")
            if s["heads_n"] and s["heads_ok"] < s["heads_n"]:
                refuted.append(f"{name}: 7 MiB replies completed {s['heads_ok']}/{s['heads_n']}")
            if c["workload"] == "W5" and s["slow_ok"] < s["slow_n"]:
                refuted.append(f"{name}: slow readers' replies completed {s['slow_ok']}/{s['slow_n']}")
            if c["workload"] == "W4":
                if s["free_min"] is not None and s["free_min"] < 0.75 * s["pool"]:
                    refuted.append(f"{name}: {s['free_min']} of {s['pool']} slots free during a 7 MiB reply")
                stalled = s["waited"].get("stalled", [0, 0])
                if stalled[0] > 0:
                    refuted.append(f"{name}: {stalled[0]} of {stalled[1]} stalled peers' pieces waited for room")
    if refuted:
        print("refuted:")
        for x in refuted:
            print(f"  {x}")
    else:
        print("no condition met in the cells run")


def main():
    rows = []
    for a in sys.argv[1:]:
        try:
            rows.append(one(a))
        except SystemExit as e:
            print(f"skipped: {e}", file=sys.stderr)
    if not rows:
        raise SystemExit("usage: send-admission-d-report.py RUN_DIR...")
    groups = group(rows)
    by_cell = print_tables(groups)
    calibration(groups)
    decision(by_cell)
    prediction(by_cell)


if __name__ == "__main__":
    main()
