import json, sys
from collections import defaultdict

rows = []
for p in sys.argv[1:]:
    rows += json.load(open(p))

def score(r): return r["ops_per_sec"] or r["gbit_per_sec"] or 0

# best per workload (mode, msg, conns) within the 4 MiB default budget
best = defaultdict(float)
for r in rows:
    if r["mib_per_worker"] <= 4.0 and score(r):
        k = (r["mode"], r["msg"], r["conns"])
        best[k] = max(best[k], score(r))

# regret per geometry: its worst shortfall vs best-at-that-workload
geo = defaultdict(dict)
for r in rows:
    if r["mib_per_worker"] > 4.0 or not score(r):
        continue
    k = (r["mode"], r["msg"], r["conns"])
    g = (r["buf"], r["ring"])
    geo[g][k] = score(r) / best[k] - 1

workloads = sorted({k for g in geo.values() for k in g})
print(f"{'geometry (<=4 MiB)':<22}{'worst':>9}{'median':>9}  per-workload deficit vs best")
print("-" * 96)
scored = []
for g, m in geo.items():
    if len(m) < len(workloads):        # only geometries measured everywhere
        continue
    vals = sorted(m.values())
    worst = vals[0]
    med = vals[len(vals)//2]
    scored.append((worst, med, g, m))
for worst, med, g, m in sorted(scored, reverse=True):
    label = f"{g[0]//1024}KiB x {g[1]}"
    cells = "  ".join(f"{m[k]*100:+5.0f}%" for k in workloads)
    star = "  <- default" if g == (16384, 256) else ""
    print(f"{label:<22}{worst*100:>8.0f}%{med*100:>8.0f}%  {cells}{star}")
print()
print("workloads, in column order:")
for k in workloads:
    print(f"  {k[0]} {k[1]} c{k[2]}")
