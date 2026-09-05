# ringline-benchmarks

Single-machine, multi-protocol benchmark harness. Unpublished tooling; not part
of the library's public surface.

Two independent modes:

- **The matrix** (default) — TCP / UDP / QUIC / HTTP1 / HTTP2 / HTTP3 / Redis /
  Memcache, sweeping message size × concurrency, comparing ringline and tokio
  clients and servers in-process.
- **The TLS echo bench** (`--tls`) — the A/B harness for ringline's
  `tls-unbuffered` feature. Described below.

```bash
cargo run --release -p ringline-benchmarks -- --quick
cargo run --release -p ringline-benchmarks -- --only tcp --sizes 64,4096
```

`--features force-mio` builds against ringline's mio backend so the io_uring and
mio paths can be A/B'd on the same workload.

---

## The TLS echo bench (`--tls`)

### What it is for

ringline's default-off `tls-unbuffered` cargo feature swaps rustls' buffered
record layer for its unbuffered one, which removes one copy from the TLS
**application-data send** path: `WriteTraffic::encrypt` reads plaintext straight
out of caller memory instead of bouncing it through rustls' internal
`sendable_plaintext` buffer first. Sends go from 2 copies to 1.

The effort's GO/NO-GO criterion (`docs/journal/2026-09-unbuffered-tls.md`) is:

> **GO** if TLS sends measurably drop to 1 copy with no regression in throughput
> or latency on the rig, on both backends.

Removing a copy is a **CPU-efficiency** win. It shows up as throughput only when
the server is CPU-bound — and co-located on one box, where the load generator
competes for the same cores, it may not show up as throughput at all. So the
primary metric here is **server CPU time per operation**, with throughput and
latency as the "no regression" guard rather than the headline.

### How to run the two arms

Build the same binary twice and hold everything else constant. From the
workspace root:

```bash
# Arm A — buffered engine (ringline's default)
cargo run --release -p ringline-benchmarks -- \
    --tls \
    --sizes 64,1024,16384,262144 \
    --clients 8 \
    --warmup 3 --duration 15 \
    --json /tmp/tls-buffered.json

# Arm B — unbuffered engine
cargo run --release -p ringline-benchmarks --features tls-unbuffered -- \
    --tls \
    --sizes 64,1024,16384,262144 \
    --clients 8 \
    --warmup 3 --duration 15 \
    --json /tmp/tls-unbuffered.json
```

Then diff the `cpu_ns_per_op` field of matching rows:

```bash
jq -r '.tls_engine as $e
       | .tls_echo[]
       | [$e, .tls, .msg_size, .clients,
          (.cpu_ns_per_op|round), (.client.ops_per_sec|round),
          .client.latency.p50_ns, .client.latency.p99_ns]
       | @tsv' /tmp/tls-*.json | column -t
```

Add `--features force-mio` to both arms to repeat the comparison on the mio
backend (on macOS that is the only backend; on Linux it is the fallback path).

### Options

| flag | meaning |
| --- | --- |
| `--tls` | run *only* the TLS echo bench (exclusive mode) |
| `--sizes` | message sizes to sweep, bytes |
| `--clients` | concurrent connections to sweep |
| `--warmup` / `--duration` | seconds before / of the measurement window |
| `--tls-client-threads` | tokio worker threads on the client side (0 = half of available parallelism, min 2) |
| `--tls-rate` | aggregate target ops/sec across all clients; 0 (default) is closed-loop |
| `--json` | write the full result set |

Two things to know before using `--tls-rate`. Pacing runs on tokio's ~1 ms timer
wheel, so **one client tops out near 1000 ops/s**: a target above
`1000 * --clients` cannot be reached no matter how idle the server is, and any
row that misses its target by more than 10% is flagged. And a paced run *well*
below saturation measures a different regime — at roughly one wakeup per
operation there is no batching to amortise the event loop over, so per-op CPU
comes out several times higher than in a saturated closed-loop run and the copy
is a much smaller share of it. Pace to just under the closed-loop throughput,
not far under it.

### What it does, and why

- **The server runs in a child process** (`current_exe --tls-server-child`).
  Server CPU has to be isolated from client CPU; every other bench in this crate
  runs both sides in one process, where `cpu_ns` is client + server summed and
  cannot attribute a server-side copy to anything.
- **CPU is captured exactly, not sampled.** The child reads its own
  `getrusage(RUSAGE_SELF)` — the kernel's own accounting — on request from the
  parent over a control socket, at the two edges of the measurement window. An
  earlier experiment sampled `/proc/<pid>/stat` from outside, which quantises to
  10 ms clock ticks, races the window boundaries, and does not exist on macOS.
- **The client is tokio + rustls, and cannot be switched to ringline.** A
  ringline client would link the engine under test, so an engine change would
  move both ends of the measurement at once.
- **The server is TLS-terminating ringline**, using a self-signed cert generated
  at runtime by `rcgen` in the child and handed to the parent as hex DER on the
  child's `READY` line. No PEM files on disk.
- **A plaintext control cell runs beside every TLS cell.** `--tls` builds a
  `BenchmarkDefinition` with `.with_tls()`, and `combinations()` emits both
  `tls=none` and `tls=tls` for each (size, concurrency). The feature cannot
  touch the plaintext path, so the plaintext rows should match across the two
  arms; how much they differ is your noise floor, and it bounds how much of the
  TLS delta you may believe.
- **The echo is framed at exactly `msg_size` per operation.** An unframed echo
  would turn one 256 KiB message into an arrival-dependent number of sends, so
  "operations" would depend on TCP segmentation rather than on the workload.
- **The engine name comes from the child, not from the invocation.** It is
  printed and stored in the JSON as `tls_engine`, so an arm run with a forgotten
  `--features` is visible in the output instead of silently folded into a diff.

### Reading the result

Expected shape, if the change does what it claims:

- **At 64 bytes, the two engines should be near-identical.** The removed copy is
  proportional to payload; at 64 bytes there is nothing to save. A large win at
  64 bytes is a measurement error, not a result.
- **The gap should grow with message size**, and be clearest at 16 KiB and
  256 KiB.
- **The plaintext control rows should not move** between arms.

If those three do not hold, the harness or the environment is wrong and the
numbers should not be reported — **except** for the one known, sourced reason
they will not hold on the mio backend, below.

#### The mio backend has a confound the io_uring backend does not

The two backends do not encrypt the same way, and only one of them is the clean
copy-count experiment:

- **io_uring** (`tls::backend_uring::encrypt_to_sends`) encrypts straight into
  send-pool slots via `encrypt_chunk`. Nothing else changes between the arms;
  this is the 2-copies-to-1 comparison the GO/NO-GO describes.
- **mio** (`tls::backend_mio::encrypt_for_send_mio` → `unbuffered::encrypt_to_vec`)
  has no pool slot to encrypt into, so it encrypts into a `Vec` that it grows
  with `out.resize(start + 32 KiB, 0)` per chunk. That zero-fill is
  *independent of message size* — a 64-byte send zeroes 32 KiB — and `ringline`'s
  own comment on that function says it "costs roughly one extra pass over the
  payload". So the mio arm trades one copy away and buys a memset back, and the
  small-message cells can come out *worse* under the unbuffered engine.

Read a mio run as "does the mio path regress", not as "did the copy come out".
The copy-count claim is an io_uring measurement.

The harness flags rows it does not trust — a server that dropped sends because
its pool was exhausted, a server that completed no operations, or a server that
saw fewer connections than there were clients — with a `!!` line and a `warning`
field in the JSON. Do not read a flagged row as a result.

### Caveats

- **Closed loop by default.** Each client sends the next request as soon as the
  previous response lands, so a faster server is offered more load. Per-op CPU
  normalises most of that, but not batching effects: more ops per event-loop
  iteration amortise the loop better. `--tls-rate N` paces both arms to the same
  aggregate rate, which is the cleaner comparison when the server has headroom.
- **Co-located.** Client and server share the host's cores. This inflates both
  arms and caps the achievable rate; a two-machine rig run is what the GO/NO-GO
  actually calls for.
- The child's CPU includes its acceptor and control threads. Both are idle
  during the window — the control thread is blocked on a socket read between the
  two samples — so this is a small constant, not a term that varies by arm.
