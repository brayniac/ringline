#!/usr/bin/env python3
"""Generate #622 step 6's measurement matrix as SystemsLab experiments.

Design: `docs/recv-incremental-ring-design.md`, "Landing", step 6.

Server alone on hv02, load generator and proxy sink on hv01, one guest
boot per (kernel, rep). Each boot runs every cell under every arm, the arm
order rotated by rep. The arms are:

    base   256 x 16 KiB plain ring (`recv_buffer(256, 16384)`)
    base2  the same configuration again: the A/A pair
    inc    `recv_incremental(true)` with the geometry for the ring kind the
           worker selects: 64 x 1 MiB INC on 6.12 and 7.1, 4096 x 64 KiB
           plain on 6.1 and 6.8

A difference between base and inc counts only when it exceeds the spread
between base and base2.

Each arm's server is sent SIGTERM once the client's connections have all
opened and closed (or at a deadline, if they never do), so it
dumps its counters (`--metrics-out`) and peak RSS when it stops. Barriers
pair each server arm with its client arm.

Phase `t` is closed loop, for throughput. Phase `l` is open loop at a fixed
offered rate per cell (`--rates`, a JSON file of cell id -> requests/s,
written by collect.py from phase t's base arms).

Phase `s` is open loop over a ladder of offered rates (LADDER, fractions of
base's phase t median on that kernel; `--rates` from `collect.py ladder`),
one client run per rate against one server per arm. collect.py `knees`
reports the highest rate per arm whose p99 meets the cell's SLO.

Usage:
    ./generate.py --bins-artifact <id> --phase t [--dry]
    ./generate.py --bins-artifact <id> --phase l --rates rates.json
    ./generate.py --bins-artifact <id> --phase s --rates ladder.json --kernels k612,k71 --reps 3 --warmup 2 --duration 6
"""

import argparse
import json
import math
import pathlib

KIB = 1024
MIB = 1024 * KIB

KERNELS = {
    "k61": "spool/images/debian-12-base-20261008.1@golden",
    "k68": "spool/images/ubuntu-24.04-base-20261008.1@golden",
    "k612": "spool/images/debian-13-base-20260912.1@golden",
    "k71": "spool/images/debian-13-ci-bpo-20261001.1@golden",
}

ARMS = {
    "base": "--recv-buffer-bytes 16384 --recv-ring-size 256",
    "base2": "--recv-buffer-bytes 16384 --recv-ring-size 256",
    "inc": "--recv-incremental",
}


def cells():
    """Every cell as a dict. `kind` is echo, slow (echo with a busy
    handler) or proxy (a stream forwarded to the sink)."""
    out = []
    for msg in [256, 4 * KIB, 64 * KIB, 1 * MIB]:
        for conns in [64, 1000]:
            out.append({"kind": "echo", "msg": msg, "conns": conns, "depth": 1})
    for msg in [256, 64 * KIB]:
        out.append({"kind": "echo", "msg": msg, "conns": 10000, "depth": 1})
    # Pipelined: several messages per receive, so INC packs completions
    # into one buffer.
    out.append({"kind": "echo", "msg": 4 * KIB, "conns": 1000, "depth": 16})
    out.append({"kind": "slow", "msg": 4 * KIB, "conns": 1000, "depth": 1, "service_us": 20})
    for conns in [64, 1000]:
        out.append({"kind": "proxy", "msg": 16 * KIB, "conns": conns, "depth": 1})
    return out


def cell_id(c) -> str:
    s = f"{c['kind']}-{c['msg']}b-c{c['conns']}"
    if c["depth"] > 1:
        s += f"-d{c['depth']}"
    return s


SERVER_ARM = """
[[jobs.steps]]
type = "barrier"
name = "{aid}-start"
timeout = 3600

[[jobs.steps]]
type = "anvil-vm"
mode = "exec"
payload_timeout = 3600
payload = '''
set -uo pipefail
mkdir -p /home/anvil/results
sudo prlimit --nofile=1048576:1048576 --memlock=unlimited:unlimited --pid $$
{pre}/home/anvil/out/bench-server \\
  --runtime ringline --addr {{server_ip}}:{{port}} --workers {{workers}} \\
  {server_args} \\
  --metrics-out /home/anvil/results/{aid}.metrics.json \\
  > /home/anvil/results/{aid}.server.log 2>&1 &
pid=$!
# Stop the server once all {conns} client connections have opened and then
# stayed closed for {quiet} s, or after {server_secs} s if that never happens.
conns() {{ ss -Htn state established "( sport = :{{port}} )" | wc -l; }}
end=$((SECONDS + {server_secs}))
seen=0
zero_at=-1
while kill -0 $pid 2>/dev/null && [ $SECONDS -lt $end ]; do
  n=$(conns)
  if [ $seen -eq 0 ]; then
    [ "$n" -ge {conns} ] && seen=1
  elif [ "$n" -eq 0 ]; then
    [ $zero_at -lt 0 ] && zero_at=$SECONDS
    [ $((SECONDS - zero_at)) -ge {quiet} ] && break
  else
    zero_at=-1
  fi
  sleep 0.5
done
echo "stop seen=$seen after=$SECONDS s" | tee -a /home/anvil/results/{aid}.server.log
kill -TERM $pid 2>/dev/null; wait $pid
grep -E "ready|VmHWM|panicked" /home/anvil/results/{aid}.server.log | head -5
'''

[[jobs.steps]]
type = "barrier"
name = "{aid}-done"
timeout = 3600
"""

CLIENT_ARM = """
[[jobs.steps]]
type = "barrier"
name = "{aid}-start"
timeout = 3600

[[jobs.steps]]
type = "anvil-vm"
mode = "exec"
payload_timeout = 3600
payload = '''
set -uo pipefail
cd /home/anvil && mkdir -p results
sudo prlimit --nofile=1048576:1048576 --memlock=unlimited:unlimited --pid $$
for i in $(seq 1 120); do
  (exec 3<>/dev/tcp/{{server_ip}}/{{port}}) 2>/dev/null && {{ exec 3>&-; break; }}
  sleep 1
done
{client_cmd} > results/{aid}.json 2> results/{aid}.log || true
echo "== {aid}"; tr -d '\\n ' < results/{aid}.json | head -c 400; echo
'''

[[jobs.steps]]
type = "barrier"
name = "{aid}-done"
timeout = 3600
"""


LADDER_ARM = """
[[jobs.steps]]
type = "barrier"
name = "{aid}-start"
timeout = 3600

[[jobs.steps]]
type = "anvil-vm"
mode = "exec"
payload_timeout = 3600
payload = '''
set -uo pipefail
cd /home/anvil && mkdir -p results
sudo prlimit --nofile=1048576:1048576 --memlock=unlimited:unlimited --pid $$
# Each step closes every connection; reuse TIME_WAIT ports so 10,000
# connections per step do not run out of local ports.
sudo sysctl -qw net.ipv4.tcp_tw_reuse=1
{pre}# One client run per offered rate, lowest first. The arm stops after two
# consecutive steps that miss the p99 SLO ({slo_ns} ns) or achieve under
# 90% of the offered rate.
fails=0
for st in {steps}; do
  s=$(echo $st | cut -d: -f1)
  rate=$(echo $st | cut -d: -f2)
  for i in $(seq 1 120); do
    (exec 3<>/dev/tcp/{{server_ip}}/{{port}}) 2>/dev/null && {{ exec 3>&-; break; }}
    sleep 1
  done
  # A step that runs past {step_cap} s is killed and counts as a failed
  # step; the guest's memory and kernel log are printed for diagnosis.
  timeout -k 10 {step_cap} {client_cmd} --open --rate $rate > results/{aid}.$s.json 2> results/{aid}.$s.log
  rc=$?
  if [ $rc -eq 124 ] || [ $rc -eq 137 ]; then
    echo "== {aid} $s client timed out after {step_cap} s (rc=$rc)"
    free -m; sudo dmesg | tail -20
  fi
  ach=$(tr -d ' \\n' < results/{aid}.$s.json | grep -o '"ops_per_sec":[0-9.]*' | cut -d: -f2)
  p99=$(tr -d ' \\n' < results/{aid}.$s.json | grep -o '"p99_ns":[0-9]*' | cut -d: -f2)
  echo "== {aid} $s offered=$rate achieved=$ach p99_ns=$p99"
  if awk -v a="$ach" -v r="$rate" -v p="$p99" 'BEGIN{{exit !(a + 0 >= 0.9 * r && p != "" && p + 0 <= {slo_ns})}}'; then
    fails=0
  else
    fails=$((fails + 1))
    [ $fails -ge 2 ] && break
  fi
done
# A failed test on the last step would otherwise be the payload's status.
exit 0
'''

[[jobs.steps]]
type = "barrier"
name = "{aid}-done"
timeout = 3600
"""

# Phase s: host state at the start of each arm, on both guests, written to
# results/<arm>.diag with a one-line summary in the job log. Sockets left by
# the previous cell show in sockstat and the TIME_WAIT count.
DIAG = """diag() {{
  v=$(vmstat 1 5 | tail -1)
  {{ echo "-- $1 $(date -u +%T)"; cat /proc/net/sockstat
     echo "time_wait $(ss -Htan state time-wait | wc -l)"
     echo "vmstat $v"; cat /proc/softirqs; }} >> {results}/{aid}.diag
  echo "== {aid} diag $1 $(grep ^TCP: /proc/net/sockstat) tw=$(ss -Htan state time-wait | wc -l) vmstat: $v"
}}
"""


def diag(aid, results, pause):
    """Shell for the start of a phase s arm: record host state, and with
    `pause` > 0 record it, wait `pause` s and record it again."""
    text = DIAG.format(aid=aid, results=results)
    if pause:
        return text + f"diag before-pause\nsleep {pause}\ndiag start\n"
    return text + "diag start\n"


# Phase q: the server exits by itself once the search's last step has closed
# its connections, or at {server_secs} s.
SEARCH_SERVER_ARM = """
[[jobs.steps]]
type = "barrier"
name = "{aid}-start"
timeout = 3600

[[jobs.steps]]
type = "anvil-vm"
mode = "exec"
payload_timeout = {payload_timeout}
payload = '''
set -uo pipefail
mkdir -p /home/anvil/results
sudo prlimit --nofile=1048576:1048576 --memlock=unlimited:unlimited --pid $$
/home/anvil/out/bench-server \\
  --runtime ringline --addr {{server_ip}}:{{port}} --workers {{workers}} \\
  {server_args} \\
  --exit-when-idle 5 --exit-after {server_secs} \\
  --metrics-out /home/anvil/results/{aid}.metrics.json \\
  > /home/anvil/results/{aid}.server.log 2>&1
echo "server exited $?" | tee -a /home/anvil/results/{aid}.server.log
grep -E "ready|VmHWM|panicked|exiting" /home/anvil/results/{aid}.server.log | head -6
exit 0
'''

[[jobs.steps]]
type = "barrier"
name = "{aid}-done"
timeout = {payload_timeout}
"""

# Phase q: one bench-client SLO search per arm. The client prints one JSON
# line per step and a final result line; collect.py reads them.
SEARCH_ARM = """
[[jobs.steps]]
type = "barrier"
name = "{aid}-start"
timeout = 3600

[[jobs.steps]]
type = "anvil-vm"
mode = "exec"
payload_timeout = {payload_timeout}
payload = '''
set -uo pipefail
cd /home/anvil && mkdir -p results
sudo prlimit --nofile=1048576:1048576 --memlock=unlimited:unlimited --pid $$
# Each step opens new connections; reuse TIME_WAIT ports so 10,000
# connections per step do not run out of local ports.
sudo sysctl -qw net.ipv4.tcp_tw_reuse=1
for i in $(seq 1 120); do
  (exec 3<>/dev/tcp/{{server_ip}}/{{port}}) 2>/dev/null && {{ exec 3>&-; break; }}
  sleep 1
done
{client_cmd} --search --search-start {start} --search-factor {factor} \\
  --search-max-rate {max_rate} --slo-p99-us {slo_us} \\
  > results/{aid}.search.jsonl 2> results/{aid}.search.log
echo "== {aid} bench-client exited $?"
grep -E "bench-client: (step|search|cooldown)" results/{aid}.search.log
exit 0
'''

[[jobs.steps]]
type = "barrier"
name = "{aid}-done"
timeout = {payload_timeout}
"""

# Phase q search, as fractions of base's closed-loop median from phase t:
# start at 0.5, climb by 1.25, never offer more than 8.
SEARCH_START = 0.5
SEARCH_FACTOR = 1.25
SEARCH_MAX = 8.0
# The client's defaults: bisect until within 5%, at most 8 bisection steps.
SEARCH_MAX_BISECT = 8
# The search keeps one set of connections and changes the rate in place
# (--search-reuse), waiting this long after each change before measuring.
# --fresh opens new connections for every step instead.
SEARCH_DRAIN_MS = 1000
# With --search-reuse, each rate change first waits, at most this long, for
# the requests in flight at the previous rate to be answered; a step whose
# drain times out fails. The server's lifetime allows for it on every step.
SEARCH_DRAIN_TIMEOUT_S = 60
# payload_timeout of the phase q server and client steps.
SEARCH_PAYLOAD_TIMEOUT_S = 7200

# Offered rates for phase s, as fractions of base's closed-loop median:
# geometric steps of 12% from 50% to 385%. Arms stop two steps past the
# SLO, so the upper steps run only where the SLO still holds.
LADDER = [round(0.5 * 1.12 ** i, 3) for i in range(19)]

# p99 SLO per message size for phase s, fixed before the runs. collect.py
# imports it.
SLO_P99_NS = {256: 1_000_000, 4096: 1_000_000, 65536: 10_000_000, 1048576: 100_000_000}


def arm_cmds(c, arm, phase, rate, warmup, duration):
    threads = min(48, max(16, c["conns"] // 32))
    if c["kind"] == "proxy":
        server_args = (
            "--proxy-backend {client_ip}:{drain_port} --proxy-api conn "
            f"--msg-size {c['msg']} {ARMS[arm]}"
        )
        client_cmd = (
            "./out/proxy-load --addr {server_ip}:{port} "
            f"--clients {c['conns']} --threads {threads} --msg-size {c['msg']} "
            f"--warmup {warmup} --duration {duration}"
        )
        return server_args, client_cmd
    server_args = f"--echo-mode forward --msg-size {c['msg']} {ARMS[arm]}"
    if c["kind"] == "slow":
        server_args += f" --service-time-us {c['service_us']}"
    client_cmd = (
        "./out/bench-client --runtime ringline --addr {server_ip}:{port} "
        f"--clients {c['conns']} --threads {threads} --msg-size {c['msg']} "
        f"--warmup {warmup} --duration {duration}"
    )
    if phase == "l":
        client_cmd += f" --open --rate {rate}"
    elif c["depth"] > 1:
        client_cmd += f" --depth {c['depth']}"
    return server_args, client_cmd


HEAD = """# GENERATED by experiments/recv-inc-matrix/generate.py; do not hand-edit.
# #622 step 6, phase {phase}, {kernel}, rep {rep}: {n} arms, one guest boot.
name = "ringline #622 step 6 {phase} {kernel} rep{rep}"

[params]
bins_artifact = "{bins}"
image         = "{image}"
server_ip     = "172.31.0.1"
client_ip     = "172.31.0.2"
port          = "7878"
drain_port    = "7900"
workers       = "{workers}"

# server under test: hv02

[[jobs]]
name = "server"
tags = ["z1.baremetal"]

[[jobs.steps]]
type = "anvil-vm"
mode = "start"
shape = "z1.c"
image = "{{image}}"
gpu = false
data_address = "{{server_ip}}/24"
boot_timeout = 600

[[jobs.steps]]
type = "anvil-vm"
mode = "exec"
inputs = [{{ artifact = "{{bins_artifact}}", name = "bins.tgz", dest = "/home/anvil" }}]
payload = '''
set -uo pipefail
cd /home/anvil && tar xzf bins.tgz && chmod +x out/*
sudo sysctl -qw net.core.somaxconn=65535 net.ipv4.tcp_max_syn_backlog=65535 \\
  net.core.rmem_max=134217728 net.core.wmem_max=134217728
echo "KERNEL $(uname -r) nproc=$(nproc) mem=$(free -g | awk '/Mem:/{{print $2}}')G"
./out/bench-server --print-backend
'''
"""

SERVER_TAIL = """
[[jobs.steps]]
type = "anvil-vm"
mode = "exec"
upload = true
artifacts = ["/home/anvil/server-results.tgz"]
payload = '''
tar czf /home/anvil/server-results.tgz -C /home/anvil results
'''

[[jobs.steps]]
type = "anvil-vm"
mode = "stop"
upload = true

# load generator and proxy sink: hv01

[[jobs]]
name = "load"
tags = ["z2.baremetal"]

[[jobs.steps]]
type = "anvil-vm"
mode = "start"
shape = "z2.c"
image = "{image}"
gpu = false
data_address = "{client_ip}/24"
boot_timeout = 600

[[jobs.steps]]
type = "anvil-vm"
mode = "exec"
inputs = [{ artifact = "{bins_artifact}", name = "bins.tgz", dest = "/home/anvil" }]
payload = '''
set -uo pipefail
cd /home/anvil && tar xzf bins.tgz && chmod +x out/*
sudo sysctl -qw net.core.rmem_max=134217728 net.core.wmem_max=134217728 \\
  net.ipv4.ip_local_port_range="1024 65535"
'''

[[jobs.steps]]
type = "anvil-vm"
mode = "exec"
detach = true
unit = "proxy-drain"
payload = "exec /home/anvil/out/proxy-drain --addr 0.0.0.0:{drain_port}"
"""

CLIENT_TAIL = """
[[jobs.steps]]
type = "anvil-vm"
mode = "exec"
upload = true
artifacts = ["/home/anvil/client-results.tgz"]
payload = '''
tar czf /home/anvil/client-results.tgz -C /home/anvil results
'''

[[jobs.steps]]
type = "anvil-vm"
mode = "stop"
upload = true
"""


def render_search(kernel, rep, bins, workers, warmup, duration, ladder, only=None, reuse=True,
                  label="", cooldown=0):
    """Phase q: a bench-client SLO search per arm against one server.
    `ladder` maps kernel -> cell id -> base's closed-loop median from
    phase t (collect.py ladder). With `cooldown` seconds, each failed step
    is followed by that long at the start rate (--search-cooldown-secs)."""
    names = list(ARMS)
    k = (rep - 1) % len(names)
    order = names[k:] + names[:k]
    server, client = [], []
    n = 0
    climb = math.ceil(math.log(SEARCH_MAX / SEARCH_START) / math.log(SEARCH_FACTOR)) + 1
    max_steps = climb + SEARCH_MAX_BISECT
    for c in cells():
        cid = cell_id(c)
        # proxy-load has no open loop; open loop ignores depth, so the d16
        # cell would repeat the 4 KiB c1000 one.
        if c["kind"] == "proxy" or c["depth"] > 1 or cid not in ladder[kernel]:
            continue
        if only and cid not in only:
            continue
        base = ladder[kernel][cid]
        per_step = warmup + duration + (30 if c["conns"] >= 10000 else 10)
        if reuse:
            per_step += SEARCH_DRAIN_TIMEOUT_S
        if cooldown:
            per_step += cooldown + SEARCH_DRAIN_TIMEOUT_S + 2
        server_secs = max_steps * per_step + 60
        # payload_timeout starts before the server does; keep --exit-after
        # 60 s inside it.
        if server_secs + 60 > SEARCH_PAYLOAD_TIMEOUT_S:
            raise SystemExit(
                f"{cid}: the server may run {server_secs} s, within 60 s of the "
                f"{SEARCH_PAYLOAD_TIMEOUT_S} s payload timeout; lower --cooldown, --warmup or --duration")
        for arm in order:
            aid = f"{cid}-{arm}"
            sa, cc = arm_cmds(c, arm, "t", None, warmup, duration)
            if reuse:
                cc += (
                    f" --search-reuse --drain-window-ms {SEARCH_DRAIN_MS}"
                    f" --drain-timeout-secs {SEARCH_DRAIN_TIMEOUT_S}"
                )
                if cooldown:
                    cc += f" --search-cooldown-secs {cooldown}"
            server.append(SEARCH_SERVER_ARM.format(
                aid=aid, server_secs=server_secs, server_args=sa,
                payload_timeout=SEARCH_PAYLOAD_TIMEOUT_S,
            ))
            client.append(SEARCH_ARM.format(
                aid=aid, client_cmd=cc, payload_timeout=SEARCH_PAYLOAD_TIMEOUT_S,
                start=max(1, int(SEARCH_START * base)), factor=SEARCH_FACTOR,
                max_rate=max(1, int(SEARCH_MAX * base)),
                slo_us=SLO_P99_NS[c["msg"]] // 1000,
            ))
            n += 1
    head = HEAD.format(
        phase="q" + label, kernel=kernel, rep=rep, n=n, bins=bins,
        image=KERNELS[kernel], workers=workers,
    )
    return head + "".join(server) + SERVER_TAIL + "".join(client) + CLIENT_TAIL, n


def render(kernel, rep, phase, bins, workers, warmup, duration, rates, only=None, reuse=True,
           first=None, label="", cooldown=0):
    if phase == "s":
        return render_ladder(kernel, rep, bins, workers, warmup, duration, rates, first, label)
    if phase == "q":
        return render_search(kernel, rep, bins, workers, warmup, duration, rates, only, reuse, label,
                             cooldown)
    names = list(ARMS)
    k = (rep - 1) % len(names)
    order = names[k:] + names[:k]
    server, client = [], []
    n = 0
    for c in cells():
        cid = cell_id(c)
        if phase == "l" and (c["kind"] == "proxy" or cid not in rates):
            continue
        # 10,000 connections take longer to open than 64.
        slack = 40 if c["conns"] >= 10000 else 20
        server_secs = warmup + duration + slack
        for arm in order:
            aid = f"{cid}-{arm}"
            sa, cc = arm_cmds(c, arm, phase, rates.get(cid), warmup, duration)
            server.append(SERVER_ARM.format(
                aid=aid, conns=c["conns"], quiet=0, server_secs=server_secs, server_args=sa,
                pre="",
            ))
            client.append(CLIENT_ARM.format(aid=aid, client_cmd=cc))
            n += 1
    head = HEAD.format(
        phase=phase + label, kernel=kernel, rep=rep, n=n, bins=bins,
        image=KERNELS[kernel], workers=workers,
    )
    return head + "".join(server) + SERVER_TAIL + "".join(client) + CLIENT_TAIL, n


# Phase s: seconds to wait before the slow cell when another cell ran
# before it in the boot.
SLOW_PAUSE = 30


def render_ladder(kernel, rep, bins, workers, warmup, duration, ladder, first=None, label=""):
    """Phase s: an offered-rate ladder per arm against one server.
    `ladder` maps kernel -> cell id -> base's closed-loop median from
    phase t (collect.py ladder). `first` moves that cell to the front of
    the boot."""
    names = list(ARMS)
    k = (rep - 1) % len(names)
    order = names[k:] + names[:k]
    server, client = [], []
    n = 0
    all_cells = cells()
    if first:
        lead = [c for c in all_cells if cell_id(c) == first]
        if not lead:
            raise SystemExit(f"--first: no cell {first}")
        all_cells = lead + [c for c in all_cells if cell_id(c) != first]
    for c in all_cells:
        cid = cell_id(c)
        # proxy-load has no open loop; open loop ignores depth, so the d16
        # cell would repeat the 4 KiB c1000 one.
        if c["kind"] == "proxy" or c["depth"] > 1 or cid not in ladder[kernel]:
            continue
        base = ladder[kernel][cid]
        steps = " ".join(f"s{i:02}:{max(1, int(f * base))}" for i, f in enumerate(LADDER))
        per_step = warmup + duration + (30 if c["conns"] >= 10000 else 10)
        server_secs = len(LADDER) * per_step + 60
        for i, arm in enumerate(order):
            aid = f"{cid}-{arm}"
            sa, cc = arm_cmds(c, arm, "t", None, warmup, duration)
            pause = SLOW_PAUSE if c["kind"] == "slow" and i == 0 and n > 0 else 0
            server.append(SERVER_ARM.format(
                aid=aid, conns=c["conns"], quiet=5, server_secs=server_secs,
                server_args=sa, pre=diag(aid, "/home/anvil/results", pause),
            ))
            client.append(LADDER_ARM.format(
                aid=aid, steps=steps, client_cmd=cc, slo_ns=SLO_P99_NS[c["msg"]],
                step_cap=per_step + 60, pre=diag(aid, "results", pause),
            ))
            n += 1
    head = HEAD.format(
        phase="s" + label, kernel=kernel, rep=rep, n=n, bins=bins,
        image=KERNELS[kernel], workers=workers,
    )
    return head + "".join(server) + SERVER_TAIL + "".join(client) + CLIENT_TAIL, n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bins-artifact", required=True)
    ap.add_argument("--phase", default="t", choices=["t", "l", "s", "q"])
    ap.add_argument("--rates", default=None, help="phase l: JSON of cell id -> rate; phase s and q: kernel -> cell id -> base median")
    ap.add_argument("--kernels", default=",".join(KERNELS))
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--warmup", type=int, default=3)
    ap.add_argument("--duration", type=int, default=12)
    ap.add_argument("--out", default=None)
    ap.add_argument("--cells", default=None, help="phase q: comma-separated cell ids to keep")
    ap.add_argument("--fresh", action="store_true", help="phase q: new connections for every search step, instead of one set per search")
    ap.add_argument("--first", default=None, help="phase s: run this cell id first in each boot")
    ap.add_argument("--label", default="", help="appended to the phase in experiment names and file names")
    ap.add_argument("--cooldown", type=int, default=0, help="phase q with reuse: seconds at the start rate after each failed step")
    ap.add_argument("--dry", action="store_true")
    args = ap.parse_args()
    rates = json.load(open(args.rates)) if args.rates else {}
    if args.phase in ("l", "s", "q") and not rates:
        ap.error(f"phase {args.phase} needs --rates")
    if args.cooldown and (args.phase != "q" or args.fresh):
        ap.error("--cooldown needs phase q with reused connections")

    out_dir = pathlib.Path(args.out or pathlib.Path(__file__).parent / f"phase-{args.phase}")
    total = 0
    for kernel in args.kernels.split(","):
        for rep in range(1, args.reps + 1):
            text, n = render(
                kernel, rep, args.phase, args.bins_artifact, args.workers,
                args.warmup, args.duration, rates,
                set(args.cells.split(",")) if args.cells else None,
                not args.fresh, args.first, args.label, args.cooldown,
            )
            total += n
            path = out_dir / f"{kernel}{args.label}-rep{rep}.toml"
            if not args.dry:
                out_dir.mkdir(parents=True, exist_ok=True)
                path.write_text(text)
            print(f"{path} {n} arms")
    per = args.warmup + args.duration + 8
    if args.phase == "s":
        per *= len(LADDER)
        print("phase s: the estimate assumes every step runs; an arm stops after two saturated steps")
    if args.phase == "q":
        per = (args.warmup + args.duration + 10) * 14
        if args.cooldown:
            per += (args.cooldown + 2) * 5
            print("phase q: the estimate assumes 14 steps and 5 cool-downs per arm")
        else:
            print("phase q: the estimate assumes 14 steps per arm")
    print(f"{total} arms, about {total * per // 3600} h {total * per % 3600 // 60} min plus boots")


if __name__ == "__main__":
    main()
