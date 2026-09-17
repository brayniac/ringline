# Splice-backed forwarding — designed, built, measured, rejected

**Status: rejected on measurement (#407). Do not re-propose for the io_uring
backend without reading "What the measurements said".**

`forward_to_splice` was built, tested and benchmarked. With the provided recv
buffer sized comparably, it is **a wash at 256 B and 17–18% slower than
`forward_to` at 4–16 KiB**, and it costs more CPU per byte. The implementation
was correct; the idea was wrong for this backend.

This document is kept because the design is still the right starting point if
the mio backend ever gets a proxy path (#410), and because a measured negative
is worth more than the absence of a document — without it this gets proposed
again.

## What the measurements said

One-way relay, client -> ringline proxy -> drain, 64 connections, 8 workers,
two-machine X710 rig. `forward_to` against `forward_to_splice`, differing only
in which call the handler makes:

| message | `forward_to` | `forward_to_splice` | |
|---|---|---|---|
| 256 B | 6.08 Gb/s · 20.0 ns/byte | 6.02 · 17.7 | −0.9% |
| 4 KiB | **22.11** · **3.86** | 18.38 · 4.68 | **−16.9%** |
| 16 KiB | **22.28** · **3.51** | 18.16 · 4.64 | **−18.5%** |

Nothing was saturated (10–15 of 24 cores), so these are per-byte costs rather
than a ceiling.

### Why it loses

`IORING_OP_SPLICE` is not handled inline. It is punted to io-wq kernel worker
threads, and those threads do real work: `ringline-worker` drops from 6.3 cores
to 2.7, but `iou-wrk-*` adds about 6. Reading the worker thread alone suggests
a 3.5x win; counting the kernel threads doing the work turns it into a loss.

**The deeper reason is that ringline had no copy left to remove.** The same
technique is worth +34% to *tokio* at 16 KiB, because tokio's `read`/`write`
loop copies kernel->user->kernel and splice removes both. Mode A already DMAs
into a provided buffer and sends from it. Both are zero *user-space* copy —
socket-to-socket data lands in the destination's skb either way — so what was
actually traded was inline completion handling for io-wq, and that trade loses.

**Splice's value is inversely proportional to how good the existing zero-copy
path is.** That is the transferable lesson, and it is why #410 may still want
this: mio has no provided-buffer ring and its send path degrades guards to
copies, which is structurally tokio's situation rather than io_uring's.

### What was not measured

The rejection is bounded to what was tested: no TLS (excluded by design —
splice would move ciphertext past the record layer), no file sinks, no
connection counts above 64, throughput and CPU but **not latency**, and kernel
6.12 only. The io-wq punt is a kernel implementation detail, not a guarantee.

### A harness caveat worth carrying forward

The first version of this comparison reported splice **+161%**. That number was
wrong three ways: the drain was the bottleneck (8.2 of 24 cores, more than the
proxy under test), the recv buffer was derived from message size so Mode A ran
with 4 KiB buffers against splice's 64 KiB pipe chunk, and only
`ringline-worker` was counted rather than the io-wq threads. Fixing all three
inverted the result. Any future forwarding measurement should size the recv
buffer explicitly, keep the sink off the machine under test, and count kernel
worker threads.

---

# The design as built

What follows is the design as implemented, retained for #410.

## Why it looked promising

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

### mio: out of scope

An earlier version of this document argued for a mio `splice(2)` path "so a
`force-mio` build of a proxy keeps working rather than losing the API". That
premise was wrong: **mio has no proxy API to lose.** `forward_to` and `SinkFd`
are both `#[cfg(has_io_uring)]`, so a `force-mio` build cannot proxy today with
or without splice, and there is no regression to prevent.

It would also break this design's central safety property. Pool exhaustion,
TLS, and already-buffered data all fall back to Mode A — and on mio there is no
Mode A to fall back to, so those cases would have to fail instead, making
splice a failure mode rather than an optimisation.

`forward_to_splice` therefore joins `forward_to` as io_uring-only. Giving mio a
proxy path means first giving it a `forward_to` — its own buffering and
write-readiness handling — which is a larger piece of work than the splice path
and is tracked separately.

## Pipe pool

Per worker, `Config::splice_pipes` (default 16) pipe pairs, created lazily.
A forward acquires one for its duration and returns it on completion or
teardown.

**Exhaustion falls back to `forward_to`'s Mode A path**, so splice is strictly
an optimization and never a failure mode. This matters more than the fast path:
a proxy that starts refusing forwards at connection 17 is worse than one that
gets slower.

## Guards

- io_uring only, like `forward_to` itself.
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
- io_uring only (see "mio: out of scope").

## Measurement — superseded

This section originally set a bar of 165,699 ops/s from an echo benchmark. That
bar was never the right validation: it came from tokio doing hairpin echo,
which ringline cannot express and which nobody deploys. The comparison that
actually tested the claim was ringline against ringline on a one-way relay, and
its result is at the top of this document.
