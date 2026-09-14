# Benchmarks

Point-in-time performance numbers for the ringline **server**, alongside a
tokio reference. Checked in so future changes have a baseline to beat (or to
flag a regression against).

Two runs are recorded here, on different rigs and different workloads. Both are
two-machine (client and server on separate physical hosts); neither is
co-located.

| | workload | rig | date / commit |
|---|---|---|---|
| [TCP echo](#tcp-echo-comparison-two-machine-x710-40g) | TCP echo, 4 message sizes x 4 connection counts | bare metal, Xeon, X710 40G LAG | Sept 2026, `afcccfb` (pre-0.7) |
| [Segcache](#segcache-cache-server-comparison-two-machine-aws-graviton4) | Segcache GET, read-heavy | AWS Graviton4 c8g | June 2026, `c77cfba` (0.1.3) |

The echo run is the current one and the one to measure against; it also covers
the io_uring-vs-mio backend comparison, which the segcache run does not. The
segcache run is kept because it measures a realistic cache server rather than a
byte pipe, and nothing has re-run it on the current release.

---

## TCP echo comparison (two-machine, X710 40G)

Four server configurations over the same closed-loop TCP echo workload:

- **ringline io_uring** — the default production path.
- **ringline io_uring + `--recv-forward`** — recv buffers forwarded straight to
  the send path without passing through the accumulator. A byte-pipe mode, not
  a general tuning knob (see the caveat below).
- **ringline mio** — the epoll fallback backend, same binary, `force-mio`.
- **tokio** — reference, same `bench-server` binary, bulk-echo loop.

### Test rig

| Item | Value |
|:-----|:------|
| Server | bare metal, 24 vCPU Xeon |
| Client | bare metal, 56 vCPU Xeon |
| Network | 4x Intel X710 in a 40G LAG, direct-attached, no switch hop |
| Guests | one ephemeral VM per host with all four PFs passed through |
| Server workers | 12 |
| Load | closed loop, `conns` connections, depth 1, 20 s steady window after warmup |
| Reps | 3 per cell; tables report the median |

### Validity gates

Every one of the 192 arms was gated before it entered these tables:

- **Little's law** — `connections = throughput x mean latency` within 15%. A
  closed-loop arm that violates it is not measuring what it claims to.
- **Load floor** — at least 1.2 server cores busy, so an arm is not reporting
  client-side idle.
- **Funnel** — the hottest core holds no more than 60% of total busy server CPU
  when loaded, which catches the IRQ/flow-steering single-core funnel that has
  masqueraded as a ringline regression before.

CPU comes from rezolus `cpu_usage` (BPF-derived on-CPU nanoseconds, `user` +
`system`), not from `/proc/stat` — see the caveat on iowait below.

**189 of 192 arms passed.** The three failures are the same cell in all three
reps (tokio, 2048 connections, 16 KiB, Little's law 18% out), so that one cell is
excluded rather than reported.

### Throughput — median ops/s, n=3

| conns | uring | recv-fwd | tokio | mio |
|------:|------:|---------:|------:|----:|
| **256 B** ||||
| 1 | 6,632 | **6,741** | 6,064 | 5,940 |
| 64 | 285,133 | **291,397** | 173,721 | 265,479 |
| 512 | **621,570** | 602,873 | 431,515 | 450,541 |
| 2048 | **531,199** | 507,381 | 420,798 | 437,845 |
| **1 KiB** ||||
| 64 | **273,884** | 272,464 | 170,598 | 250,618 |
| 512 | **592,973** | 572,676 | 418,075 | 431,819 |
| 2048 | **513,275** | 488,172 | 418,494 | 408,636 |
| **4 KiB** ||||
| 64 | **198,679** | 195,186 | 127,555 | 183,599 |
| 512 | **322,848** | 314,260 | 238,755 | 260,061 |
| 2048 | **300,284** | 289,419 | 234,501 | 232,425 |
| **16 KiB** ||||
| 64 | 118,395 | 145,213 | 89,028 | **152,303** |
| 512 | 122,592 | **159,903** | 107,377 | 130,407 |
| 2048 | 109,015 | **151,735** | — | 125,759 |

At 64 connections and above, ringline io_uring leads at 256 B through 4 KiB by
**23–64%** over tokio and by **7–38%** over its own mio backend. At a single
connection the four are within 9% of each other — nothing is saturated. At
**16 KiB io_uring loses to both** mio and recv-forward; that is a known defect,
not a property of the design (see *The 16 KiB regression* below).

### Latency — median of 3, ms

| conns | | uring p50 | tokio p50 | uring p99 | tokio p99 |
|------:|---|---:|---:|---:|---:|
| **256 B** ||||||
| 1 | | **0.146** | 0.166 | **0.214** | 0.217 |
| 64 | | **0.212** | 0.365 | **0.415** | 0.627 |
| 512 | | **0.814** | 1.132 | **1.221** | 2.538 |
| 2048 | | 3.932 | **3.911** | **6.155** | 12.356 |
| **4 KiB** ||||||
| 64 | | **0.309** | 0.494 | **0.615** | 0.877 |
| 512 | | **1.576** | 2.015 | **2.217** | 4.897 |
| 2048 | | **7.157** | 7.395 | **9.861** | 20.633 |

The tail is where the gap is widest: at 512 connections ringline's p99 is **half**
tokio's (1.22 ms vs 2.54 ms at 256 B, 2.22 ms vs 4.90 ms at 4 KiB), and at 2048
connections tokio's p99 is 2x ringline's at both sizes even where the medians are
level.

### CPU efficiency — 512 connections, ops per server core-second

| size | uring | tokio | mio |
|-----:|------:|------:|----:|
| 256 B | **47,261** | 37,320 | 38,477 |
| 1 KiB | **45,178** | 36,896 | 37,848 |
| 4 KiB | **23,566** | 19,579 | 20,493 |

The throughput lead is not bought with CPU: ringline is **20–27% more efficient
per core** than tokio while serving **35–44% more operations** at the same
connection count.

### Syscall amortization

At 512 connections the io_uring server issues **0.08 `io_uring_enter` per
operation** — about 12 operations submitted and completed per syscall. The mio
backend's epoll path issues none by construction (it uses `epoll_wait` +
`read`/`write`), and the comparison is one of syscall *shape*, not count.

### The 16 KiB regression (issue #397)

At 16 KiB the best alternative configuration — mio at 64 connections,
`--recv-forward` at 512 and 2048 — delivers **29–39% more throughput** than the
default io_uring path. Root cause: `run_direct_echo` submits one `Send` per
recv completion, and a 16 KiB message spans more than one completion, so the
reply leaves as roughly two 8 KiB segments instead of one 15 KiB one. Packets per
operation is flat at 1.00 for 256 B, 1 KiB and 4 KiB and jumps to **1.97 at
16 KiB** — exactly the size at which a message stops fitting one completion.

Tracked as **#397**. Until it is fixed, `--recv-forward` (or the mio backend) is
the faster choice for large-message byte-pipe workloads.

### `--recv-forward` is a mode, not a tuning knob

It wins at 16 KiB and is roughly neutral below it, which makes it look like a
candidate for the default. It is not. While recv-forward is enabled,
**`with_data` / `with_bytes` never observe the data** — buffers are held for
forwarding and never reach the accumulator. It turns the connection into a byte
pipe, so every handler that parses its input would break. Use it for echo and
proxy workloads; the path to a competitive default at 16 KiB is #397.

### Caveats specific to this run

- **The 2048-connection column is directional, not quotable.** Little's law
  errors climb to 6–8% even in the arms that pass, and it is the only column
  with a gated-out cell.
- **Echo is a byte pipe.** It measures the runtime's I/O path with no parsing,
  allocation, or application work. The segcache tables below are the closer
  proxy for a real server.
- **Do not measure server CPU from `/proc/stat`.** A worker blocked in
  io_uring's `submit_and_wait` is accounted as **iowait**, not idle. A
  `/proc/stat` sampler that counts iowait as busy reads an idle io_uring server
  at 806% CPU against tokio's 33%, and the error biases against io_uring
  specifically, since epoll runtimes have no equivalent accounting quirk. These
  figures come from rezolus `cpu_usage`, which has only `user`/`system` states.
- **Depends on a client-side fairness fix.** Before #392 the tokio echo server
  read with `read_exact`, costing one syscall per message while ringline read in
  bulk. Numbers taken before that fix are not comparable to these.

### Reproducing

Build both servers on the rack's build host, then run one arm per
configuration through SystemsLab anvil-vm jobs on two hosts with the X710 PFs
passed through (`ports = 4`, bonded, fixed `172.31.0.1/.2`). The campaign spec
is `experiments/campaign-phase1.toml`, the gate script is
`experiments/phase0-gate.py`, and the server records rezolus metrics to a `.rez`
per arm.

Server: `bench-server --runtime <ringline|tokio> --protocol echo --workers 12
[--recv-forward]`, built with `--features force-mio` for the mio arm.
Client: `bench-client --clients <conns> --msg-size <bytes> --depth 1`.

---

## Segcache cache-server comparison (two-machine, AWS Graviton4)

**Run:** June 2026, ringline 0.1.3 (commit `c77cfba`). Not re-run on the current
release — treat it as the realistic-workload reference, and the echo tables
above as the current baseline.

This compares the **ringline server** against a **tokio server**, both serving the
same read-heavy cache workload: a real Segcache (segment-structured TTL cache)
answering rotating-key `GET`s. On a hit the
value is borrowed zero-copy from segment memory (`ValueRef`) and written to the
socket — ringline serves it through its send path, tokio copies it on `write`.
Client and server run on **separate physical instances**.

### Summary

With each runtime tuned to a **load-appropriate worker count** (see *Worker count*
below), the ringline server is the stronger cache server:

- **Small/medium values (256 B, 1 KiB):** ringline delivers **~30% more
  throughput**, **~25% lower p50** and **15–18% lower p99** latency, at **~22%
  better CPU efficiency** (ops per server-core-second) than tokio.
- **Large values (4 KiB):** throughput and median latency **tie** — the workload
  is bandwidth-bound at that size — but ringline still serves it on **~14% less
  CPU** with an **8% lower p99 tail**.
- **At raw saturation** (open-loop, max offered rate) the two runtimes are a
  **dead tie**: both saturate the instance's network/packet-per-second ceiling,
  and the server runtime stops mattering. ringline's advantage is in *latency and
  CPU efficiency at a given load*, not in a higher saturation ceiling on this rig.

### Test rig

| Item | Value |
|:-----|:------|
| Server | AWS EC2 `c8g.4xlarge` — 16 vCPU, Graviton4 (Neoverse-V2), aarch64 |
| Client | AWS EC2 `c8g.8xlarge` — 32 vCPU, Graviton4, aarch64 |
| Placement | Same AZ (`us-west-2b`), cluster placement group |
| Network | VPC-private ENA between the two instances (data plane); the SystemsLab control plane is out-of-band over Tailscale |
| OS / kernel | Debian 13 (trixie), Linux 6.12 |
| Rust | stable |
| Cache | Segcache, 16384 keys pre-populated, value size per row; GET-only |
| Server workers | per-core pinned; worker count noted per table |

### Methodology

- **Closed-loop** (throughput/latency/CPU tables): 128 connections (8 client
  processes × 16 connections), each connection issuing the next request on
  response. Reported throughput is the aggregate; latency percentiles are
  coordinated-omission-free service latency.
- **Server CPU** is measured on the server instance only (it runs alone): the
  `bench-server` process `utime+stime` from `/proc/<pid>/stat` over a 30 s steady
  window (warmup excluded). An **idle baseline** (server up, zero connections) is
  subtracted, so `cores busy` reflects load-attributable CPU. `ops/core` =
  achieved ops ÷ load-attributable cores busy. Idle baseline was ≤ 0.08 cores —
  the workers park when idle, so the loaded figure is real per-op work.
- **Open-loop** (saturation): 24 client processes at a high offered rate to find
  the throughput ceiling.
- Both runtimes are pinned one worker thread per core and **swept across worker
  counts**; the headline compares each at its best (matched) worker count.

### Closed-loop: ringline vs tokio at matched worker count

Both runtimes at 4 workers (the efficiency sweet spot for this 128-connection
load — see below), 128 connections:

| value | metric | ringline | tokio | ringline advantage |
|------:|:-------|--------:|------:|:-------------------|
| **256 B** | throughput (ops/s) | **803,916** | 613,552 | **+31%** |
| | p50 latency | **149 µs** | 207 µs | **−28%** |
| | p99 latency | **260 µs** | 305 µs | **−15%** |
| | ops / server-core-s | **214,000** | 176,000 | **+22%** |
| **1 KiB** | throughput (ops/s) | **772,533** | 593,163 | **+30%** |
| | p50 latency | **158 µs** | 209 µs | **−24%** |
| | p99 latency | **268 µs** | 328 µs | **−18%** |
| | ops / server-core-s | **207,000** | 168,000 | **+23%** |
| **4 KiB** | throughput (ops/s) | 451,155 | 451,145 | tie (bandwidth-bound) |
| | p50 latency | 279 µs | 277 µs | tie |
| | p99 latency | **422 µs** | 460 µs | **−8%** |
| | ops / server-core-s | **146,000** | 128,000 | **+14%** |

At 256 B and 1 KiB ringline wins every axis. At 4 KiB the per-request bytes
dominate and the two converge on throughput and median latency; ringline's
remaining edge is CPU efficiency and the tail.

### Worker count: efficiency vs. throughput

A thread-per-core runtime wants its worker count matched to the offered
concurrency — **not** set to the full core count. Sweeping the ringline server's
workers at fixed 128-connection load (256 B):

| workers | throughput (ops/s) | ops / server-core-s | p50 | p99 |
|--------:|-------------------:|--------------------:|----:|----:|
| 4 | 825,518 | **221,000** | 146 µs | 249 µs |
| 8 | 928,976 | 157,000 | 138 µs | 218 µs |
| 16 | 982,153 | 127,000 | 136 µs | 202 µs |

More workers buy **more throughput** (982k at 16w, +19% over 4w) and slightly
lower latency, but at a **steep CPU cost** — efficiency falls from 221k to 127k
ops/core because the same 128 connections are spread thinner across more event
loops. tokio shows the same shape (most efficient at 4 workers, ~176k ops/core).
For this load, 4 workers is the efficiency-optimal point for both; if you are
throughput-bound and CPU-rich, ringline scales further by adding workers.

At 4 KiB the throughput is flat across worker counts (~451k regardless) — the
workload is bandwidth-bound, so extra workers only erode efficiency.

### Peak throughput (open-loop saturation) — a network-bound tie

Driven open-loop at maximum offered rate (24 client processes, 16 server
workers), both runtimes converge on the instance's network/packet-per-second
ceiling:

| value | ringline | tokio |
|------:|---------:|------:|
| 256 B | ~7.03 M ops/s | ~7.04 M ops/s |
| 1 KiB | ~1.79 M ops/s | ~1.79 M ops/s |
| 4 KiB | ~441 k ops/s | ~448 k ops/s |

These are within run-to-run noise of each other, and the ringline server's event
loop reports ~50% idle iterations at this point — i.e. the **server is not the
bottleneck**, the NIC/PPS ceiling is. Peak throughput on this rig is a property of
the network, not the runtime. (The 16 KiB tokio cell errored in this harness and
is excluded pending investigation; ringline served ~109 k ops/s at 16 KiB.)

### Recommendation

| scenario | recommendation |
|:---------|:---------------|
| Cache values ≤ 1 KiB, latency- or CPU-sensitive | ringline: ~30% more throughput, ~25% lower p50, ~22% better CPU efficiency |
| Cache values ~4 KiB | throughput/median tie; ringline for lower tail + less CPU |
| Throughput-bound and CPU-rich | ringline scales with added workers (trading CPU efficiency for ops/s) |
| Driving the NIC to saturation | tied — bottleneck is the network, not the runtime |

---

## Caveats (segcache run)

- **Single rig, aarch64 Graviton4.** Absolute numbers are specific to this
  instance pair; ratios should travel better than absolutes, but a different CPU
  or NIC will shift them.
- **GET-only, read-heavy.** This measures cache reads with zero-copy value
  serving. SET/mixed workloads and other protocols are not covered here.
- **Worker count matters.** The comparison is best-vs-best at a load-appropriate
  worker count. Over-provisioning workers reduces CPU efficiency for *both*
  runtimes; size workers to your concurrency.
- **Large values are bandwidth-bound.** At ≥ 4 KiB and at open-loop saturation the
  bottleneck moves off the runtime onto network bandwidth / PPS, and the runtimes
  converge.
- **Two real machines.** Unlike the previously-withdrawn numbers, client and
  server are separate EC2 instances in a cluster placement group; there is no
  shared-host or shared-switch incast artifact.

## Reproducing (segcache run)

The distributed runs are SystemsLab experiments against an EC2 Graviton pair
(`aws.server` / `aws.client` tags). The `--protocol segcache` support on
`bench-server`/`bench-client`, the vendored Segcache, and the experiment specs
live on the **`bench/fair-throughput`** branch (not yet on `main`); these numbers
were produced from that branch.

The server is `bench-server --runtime <ringline|tokio> --protocol segcache
--cache-keys 16384 --workers <N> --msg-size <B>`. Closed-loop runs use a
closed-loop client (`bench-client ... --clients 16` without `--open`, 8 processes)
plus a server-side `/proc/<pid>/stat` CPU sampler with an idle baseline; the
open-loop saturation sweep is `experiments/tcp-aws-segcache.toml` on that branch.

## Updating this file

Re-run the experiments above on an equivalent two-machine rig, re-derive the
ratios from the raw `ops_per_sec` / latency / CPU figures, and update the tables.
Keep the methodology notes honest: state the worker counts, whether load was
sub-saturation, and how CPU was attributed.
