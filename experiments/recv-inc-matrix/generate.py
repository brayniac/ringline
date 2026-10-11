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

Usage:
    ./generate.py --bins-artifact <id> --phase t [--dry]
    ./generate.py --bins-artifact <id> --phase l --rates rates.json
"""

import argparse
import json
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
payload = '''
set -uo pipefail
mkdir -p /home/anvil/results
sudo prlimit --nofile=1048576:1048576 --memlock=unlimited:unlimited --pid $$
/home/anvil/out/bench-server \\
  --runtime ringline --addr {{server_ip}}:{{port}} --workers {{workers}} \\
  {server_args} \\
  --metrics-out /home/anvil/results/{aid}.metrics.json \\
  > /home/anvil/results/{aid}.server.log 2>&1 &
pid=$!
# Stop the server once all {conns} client connections have opened and then
# closed, or after {server_secs} s if that never happens.
conns() {{ ss -Htn state established "( sport = :{{port}} )" | wc -l; }}
end=$((SECONDS + {server_secs}))
seen=0
while kill -0 $pid 2>/dev/null && [ $SECONDS -lt $end ]; do
  n=$(conns)
  if [ $seen -eq 0 ]; then
    [ "$n" -ge {conns} ] && seen=1
  elif [ "$n" -eq 0 ]; then
    break
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


def render(kernel, rep, phase, bins, workers, warmup, duration, rates):
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
                aid=aid, conns=c["conns"], server_secs=server_secs, server_args=sa,
            ))
            client.append(CLIENT_ARM.format(aid=aid, client_cmd=cc))
            n += 1
    head = HEAD.format(
        phase=phase, kernel=kernel, rep=rep, n=n, bins=bins,
        image=KERNELS[kernel], workers=workers,
    )
    return head + "".join(server) + SERVER_TAIL + "".join(client) + CLIENT_TAIL, n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bins-artifact", required=True)
    ap.add_argument("--phase", default="t", choices=["t", "l"])
    ap.add_argument("--rates", default=None, help="phase l: JSON of cell id -> rate")
    ap.add_argument("--kernels", default=",".join(KERNELS))
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--warmup", type=int, default=3)
    ap.add_argument("--duration", type=int, default=12)
    ap.add_argument("--out", default=None)
    ap.add_argument("--dry", action="store_true")
    args = ap.parse_args()
    rates = json.load(open(args.rates)) if args.rates else {}
    if args.phase == "l" and not rates:
        ap.error("phase l needs --rates")

    out_dir = pathlib.Path(args.out or pathlib.Path(__file__).parent / f"phase-{args.phase}")
    total = 0
    for kernel in args.kernels.split(","):
        for rep in range(1, args.reps + 1):
            text, n = render(
                kernel, rep, args.phase, args.bins_artifact, args.workers,
                args.warmup, args.duration, rates,
            )
            total += n
            path = out_dir / f"{kernel}-rep{rep}.toml"
            if not args.dry:
                out_dir.mkdir(parents=True, exist_ok=True)
                path.write_text(text)
            print(f"{path} {n} arms")
    per = args.warmup + args.duration + 8
    print(f"{total} arms, about {total * per // 3600} h {total * per % 3600 // 60} min plus boots")


if __name__ == "__main__":
    main()
