# Splice-backed forwarding (`forward_to_splice`)

Proxy a stream from a ringline connection to a sink descriptor without the
bytes entering user space **or the provided-buffer ring**.

## Why

`forward_to` (Mode A, `docs/segmented-recv-design.md`) already avoids a
userspace copy: arriving provided buffers are held driver-side and written
straight to the sink. What it still pays, per operation, is the provided-buffer
ring — `buffer_select` on the way in, replenish on the write completion — plus a
recv completion dispatched for every buffer.

That cost is measurable. From the September 2026 comparison grid, 16 KiB echo:

| 64 connections | ops/s | syscalls/op | µs-CPU/op | packets/op |
|---|---|---|---|---|
| ringline io_uring (direct echo) | 148,659 | 1.41 | 74.3 | 4.77 |
| ringline io_uring (recv-forward) | 147,388 | ~1.4 | 74.9 | 4.75 |
| tokio, `splice(2)` + thread-per-core | **165,699** | 3.82 | **66.4** | 4.53 |

The splice arm issues **2.7x more syscalls and still uses 11% less CPU per
operation**, at equivalent packetization. The difference is not copies and not
syscalls — it is that its data path has no user-space bookkeeping at all. The
kernel moves pages between two descriptors; user space only issues the calls.

ringline can have that data path *and* keep io_uring's syscall amortization,
which the tokio arm could not: it needed a real `splice(2)` per direction, while
`IORING_OP_SPLICE` submits through the ring with the rest of the batch.

## Non-goals

Splice is a byte-relay technique and its applicability is narrow. It cannot
surface a byte to a handler: no parsing, no TLS termination, no inspection, no
transformation. It is for the shape where a prefix is parsed in user space and
the remainder is relayed opaquely — `CONNECT` tunnels, TLS passthrough,
TCP-mode load balancing, WebSocket after upgrade, SOCKS.

It is also not free at scale: a pipe pair is two descriptors and a kernel pipe
buffer (64 KiB by default). In the same grid the tokio splice arm fell from
169,660 ops/s at 512 connections to 121,202 at 2048 while ringline's non-splice
paths held at ~161,000. **Pooling the pipes is therefore part of the design, not
an optimization.**

Below 16 KiB it is a pessimization: splice cost that arm 8-15% at 256 B, because
it adds syscalls to save copies that were not costing anything.

## API

```rust
pub fn forward_to_splice<'a>(&self, sink: &'a SinkFd<'a>, len: usize)
    -> SpliceForwardFuture<'a>;
```

A separate entry point rather than a mode inside `forward_to`, because none of
`forward_to`'s documented contract survives: there are no held buffers, no bids
to release on write completion, and no force-copy under the low-water reserve —
the shared ring is not involved at all. Silently swapping the strategy would
also make fd and pipe-buffer consumption unpredictable for the caller.

Resolves to `Ok(bytes_forwarded)`, with the same truncation semantics as
`forward_to`: a value `< len` means the peer sent FIN first.

## Mechanism

### io_uring

Per chunk, two operations:

1. `Splice(conn_fd -> pipe_w, len = CHUNK, SPLICE_F_MOVE | SPLICE_F_NONBLOCK)`
2. `Splice(pipe_r -> sink_fd, len = <result of 1>, SPLICE_F_MOVE)`

Submitted sequentially (each on the previous one's CQE) in this first version.
`IOSQE_IO_LINK` would remove one round trip per chunk, but the second op's
length is only known after the first completes, so a linked pair has to splice a
speculative length and reconcile — worth doing, but only once the sequential
form is measured and correct.

`off_in`/`off_out` must be `-1` for a pipe end. A **file** sink passes the
advancing forward offset as `off_out`, matching `forward_to`'s `pwrite`
behaviour; a socket sink passes `-1`.

Both descriptors go through `impl UseFixed`, so the connection side uses the
registered-file index it already has.

### mio

The same two `splice(2)` calls, issued from the readable/writable handlers with
`SPLICE_F_NONBLOCK`, retried on `EAGAIN` via the existing interest machinery.
No batching to be had here; the point is parity, so a `force-mio` build of a
proxy keeps working rather than losing the API.

## Pipe pool

Per worker, `Config::splice_pipes` (default 16) pipe pairs, created lazily.
A forward acquires one for its duration and returns it on completion or
teardown.

**Exhaustion falls back to `forward_to`'s Mode A path**, so splice is strictly
an optimization and never a failure mode. This matters more than the fast path:
a proxy that starts refusing forwards at connection 17 is worse than one that
gets slower.

## Guards

- Linux only (`has_io_uring` or the mio backend on Linux); elsewhere
  `forward_to_splice` falls back to Mode A.
- TLS connections rejected with `InvalidInput`: splice would move ciphertext
  past the record layer.
- `O_DIRECT` sinks rejected, as `forward_to` already does.
- Accumulated plaintext must drain first — the bytes already in the accumulator
  precede anything splice would move, and reordering them would corrupt the
  stream.

## Correctness notes

- **EOF** is a `0` result from the first splice, exactly like `read`.
- **Short splice** on either leg: track bytes resident in the pipe and resubmit
  the remainder. The pipe is the only place partial state can live, so it must
  drain fully before the next chunk is spliced in, or the stream reorders.
- **Teardown with a loaded pipe** discards those bytes; the forward resolves
  truncated, which is the same contract as Mode A losing a held buffer.
- A pipe returning to the pool must be empty. If a forward is torn down with
  bytes still resident, the pipe is **closed rather than returned** — draining
  it would cost a syscall on the teardown path and a leaked byte would corrupt
  the next forward that borrowed it.

## Testing

- socket -> socket proxy round-trip, byte-exact, across sizes that span the
  pipe capacity.
- file sink with an advancing offset.
- FIN mid-forward truncates and reports the short count.
- pipe-pool exhaustion falls back to Mode A and still forwards correctly.
- TLS connection is rejected rather than silently relaying ciphertext.
- both backends.

## Measurement

The bar is the number that motivated the work: **165,699 ops/s at 16 KiB / 64
connections**, against ringline's current 148,659. A/B on the two-guest X710
rig, gated on Little's law, with packets/op and CPU/op recorded — a win in
throughput that does not also show a drop in CPU per operation would mean
something other than the intended mechanism moved.
