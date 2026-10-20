# Direct receive: one-shot receives into the connection's accumulator

Status: on `feat/recv-direct`, measured through build C8; owner decisions
made (see Decisions). Open: the scale run, and the last runs of build C8
(see Landing).

Related: `docs/recv-buffering-approaches.md` ("One-shot receive into
connection-owned memory", "Per-connection rings"),
`docs/recv-segment-views-design.md`, `docs/recv-kernel-errata.md`.

## Goal

A TCP connection that opts in receives without a provided buffer. Each
receive is a one-shot `RECV` whose destination is the free space of the
connection's `RecvAccumulator`. The kernel writes the bytes where `with_data`
and `with_bytes` read them, so the copy from a provided buffer into the
accumulator does not happen, and neither does the move of buffered bytes when
the accumulator grows. Parsers and the protocol crates do not change.

The intended user is a client making outbound plaintext connections.

## Evidence

All runs below are cachecannon with 16 client workers and 1,000 connections
(62 or 63 per worker) against Valkey (RESP) and memcached, on two VMs, at a
fixed request rate per cell, in `experiments/segviews-screen.toml` on branch
`feat/segment-clients`. Client CPU is the cachecannon process's user plus
system time for the run. Each build runs in every cell of a SystemsLab
experiment, and the three experiments rotate the build order.

### What the ring path copies

`with_bytes` on the shared multishot ring copies 1.0 to 3.1 bytes into
accumulators per byte received (`ringline/recv_ring` `BYTES_COPIED` divided
by `ringline/bytes` `RECEIVED`). The ratio exceeds 1 because growing the
accumulator moves the bytes already in it, and `BYTES_COPIED` counts both.

### Segment views did not remove enough

Build B read with `with_bytes_segments` and the segment parsers of
unreleased `resp-proto` and `memcache-proto` revisions. On the incremental
ring (`bi`) its client CPU relative to A was 0.949 to 0.973 at 8 to 64 KiB.
At 64 KiB and up the parser still copied 0.92 to 0.95 bytes per received
byte, because receives from many connections interleave in each ring buffer,
and the incremental ring added about 1 GiB of peak RSS
(`01a151fb-71b7-71c1-d9de-4cf187e4ba3f`, `01a151fb-7249-714c-93c9-b7db10eae5c3`,
`01a151fb-72b0-7179-3e37-47c2b8e111fe`).

### Direct receive (build C)

Build C is cachecannon `exp/segviews-c2` on ringline `exp/segviews-c2`
(`feat/recv-direct` on the commit build A used), binaries
`01a154c2-7be1-7364-667e-192b469e7a14`. In the screen, `c` connects with
`recv_direct(true)`, `c0` is the same binary on the multishot ring, and `c1`
is the same binary with one-shot ring receives (`recv_multishot(false)`).
Experiments `01a154c2-aff0-713a-2088-c31a3277e2ed`,
`01a154c2-b062-71ec-499e-2eb343075ee1` and
`01a154c2-b0c7-718f-7432-8370282733f8`.

Client CPU relative to A, mean of three runs, and bytes copied per byte
received:

| Cell | `c0` | `c1` | `c` | `c` copies | A copies |
|---|---|---|---|---|---|
| memcache 64 B, depth 16 | 0.999 | 1.006 | 1.039 | 0.000 | 1.01 |
| RESP 64 B, depth 16 | 0.998 | 1.017 | 1.009 | 0.000 | 1.00 |
| memcache 4 KiB, depth 1 | 1.000 | 1.015 | 1.007 | 0.000 | 1.35 |
| memcache 4 KiB, depth 16 | 0.998 | 1.027 | 0.972 | 0.015 | 2.37 |
| memcache 8 KiB, depth 16 | 1.002 | 1.037 | 0.967 | 0.050 | 2.54 |
| memcache 64 KiB, depth 1 | 1.000 | 1.042 | 0.964 | 0.000 | 2.06 |
| memcache 64 KiB, depth 16 | 1.003 | 1.031 | 0.960 | 0.073 | 3.09 |
| RESP 64 KiB, depth 16 | 0.997 | 1.022 | 0.948 | 0.070 | 2.92 |
| memcache 1 MiB, depth 1 | 1.002 | 1.058 | 0.928 | 0.000 | 2.86 |
| RESP 1 MiB, depth 1 | 0.995 | 1.048 | 0.950 | 0.000 | 1.98 |

- `c0` is within 1.4% of A in every run, so the prototype did not change the
  ring path.
- At 8 KiB and up, `c` lowers user time by 26 to 67% and raises system time
  by 2 to 5%. `c1` pays a similar system-time increase for one-shot arming
  without the copy saving, and costs 2 to 6% more than A.
- At memcache 64 B, `c` costs 3.9% more than A, against 0.6% for `c1`, so the
  extra cost is in the direct path's own work, not in arming a one-shot
  receive. Profile `01a15549-6402-71d7-74c4-134a9ee740c0` found it:
  `madvise` was 8.4% of `c`'s worker samples (`zap_page_range_single`,
  `flush_tlb_mm_range`, `smp_call_function_many_cond`) and absent in `c0` and
  `c1`. Each receive allocated a new 64 KiB buffer and freed the previous one,
  and glibc trimmed the thread's heap on the free (see "Reusing a consumed
  buffer"). Counted on loopback with an `LD_PRELOAD` allocator shim: 92,594
  allocations of 32 KiB or more in 66,315 direct receives, against 641 on the
  ring. Commit 06c6c43 fixed it: in build C3, `c` at memcache 64 B was 1.004
  of `c0`'s CPU, mean of three runs (`01a1556b-604f-710c-e4bc-1f4aaa2ee165`,
  `01a1556b-60d2-716b-893f-80d6a1500614`,
  `01a1556b-6146-7100-d5c4-9bfde714bafe`).
- p50 and p99 are unchanged within run-to-run spread, except memcache 64 B
  p50, which rose from about 400 µs to about 430 µs.
- Peak RSS: 510 MiB lower than A at memcache 1 MiB, 945 MiB higher at RESP
  1 MiB, within 60 MiB elsewhere.
- No completion arrived for a closed connection (`STALE` 0), and the bytes
  the direct receives counted equal the bytes received.

### The pool (builds C5 to C8)

Build C5 is ringline ff61e77: the per-worker pool, a 32 KiB own buffer, no
`MSG_WAITALL`. Its screen adds two cells: 1 MiB values at 100 requests per
second, which leaves most connections idle after a large value, and mixed
values, where all 400 keys hold 1 MiB and then the first 300 are rewritten at
4 KiB, so a quarter of GETs return 1 MiB. Experiments
`01a157db-d318-7132-bb08-a01d91b2363e`,
`01a157db-d38e-71ed-a8ff-f6a91eaee037` and
`01a157db-d3fd-71b3-f1cc-c584357eb3b1`; mean of three runs:

| Cell | `c` CPU / A | `c` / `c0` | `c` copies | A copies | Peak RSS A | Peak RSS `c` |
|---|---|---|---|---|---|---|
| memcache 64 B, depth 16 | 0.992 | 1.008 | 0.000 | 1.01 | 2,412 MiB | 2,368 MiB |
| RESP 64 B, depth 16 | 0.995 | 0.992 | 0.000 | 1.00 | 2,410 MiB | 2,368 MiB |
| memcache 4 KiB, depth 1 | 1.007 | 1.006 | 0.000 | 1.35 | 2,410 MiB | 2,367 MiB |
| memcache 4 KiB, depth 16 | 0.978 | 0.978 | 0.047 | 2.37 | 2,452 MiB | 2,444 MiB |
| memcache 8 KiB, depth 16 | 0.972 | 0.970 | 0.108 | 2.55 | 2,481 MiB | 2,451 MiB |
| memcache 64 KiB, depth 1 | 0.982 | 0.984 | 0.454 | 2.07 | 2,494 MiB | 2,542 MiB |
| memcache 64 KiB, depth 16 | 0.954 | 0.953 | 0.191 | 3.09 | 2,822 MiB | 2,703 MiB |
| RESP 64 KiB, depth 16 | 0.944 | 0.950 | 0.183 | 2.91 | 2,789 MiB | 2,890 MiB |
| memcache 1 MiB, depth 1 | 0.936 | 0.935 | 0.028 | 2.86 | 3,921 MiB | 3,566 MiB |
| RESP 1 MiB, depth 1 | 0.965 | 0.962 | 0.018 | 1.98 | 3,423 MiB | 3,381 MiB |
| memcache 1 MiB, idle | 0.939 | 0.963 | 0.031 | 3.47 | 3,911 MiB | 3,389 MiB |
| mixed, depth 1 | 0.932 | 0.935 | 0.029 | 3.18 | 3,935 MiB | 2,947 MiB |
| mixed, depth 16 | 0.959 | 0.956 | 0.190 | 3.51 | 3,225 MiB | 5,891 MiB |

Every build's client holds about 2.4 GiB, 1.1 GiB of it in huge pages, at
64 B values, so the differences in the RSS columns are what the receive path
holds. The client hosts run THP in mode `always` with `defrag` `madvise`.

- 4 KiB depth 16 costs about 1% more than in C3 (0.968). A 64 KiB batch no
  longer fits the 32 KiB own buffer, so its first part is copied into a
  borrowed buffer. 64 KiB depth 1 costs about 2.5% more than in C3 (0.956)
  for the same reason, at 0.45 bytes copied per byte.
- RESP 1 MiB costs 0.965 against C3's 0.943 (see Known lengths).
- Mixed values at depth 16 held 2.7 GB more than A, and no borrowed buffer
  was reused. Diagnostic counters (build C6, `01a159c1-ad72-7117-17a8-5a3f5bb6909c`)
  showed borrowed buffers averaging 2.4 MiB when given back: a buffer that
  ran out of room with part of a message of no known length buffered grew by
  the whole receive size with `BytesMut::reserve`, which doubles the
  allocation. A few buffers of about 5 MiB filled each worker's 16 MiB of
  free buffers, no borrow could use them, and every buffer given back after
  that was freed. Commit c67d328 sizes a grown buffer to the receive size and
  evicts the largest free buffers (see Buffers).

Build C7 is c67d328 with the diagnostic counters (experiments
`01a159d3-e857-71bf-0a65-6c6903c6bb6e`,
`01a159d3-e8ce-718e-c651-340efa5fda70`,
`01a159d3-e946-7148-afb8-545333526581`). At mixed depth 16, CPU is 0.936 of
A, peak RSS 4,392 MiB, and every borrow after the warmup reused a free
buffer. The buffers given back average 2.0 MiB: a connection's first 1 MiB
value grows by doubling, because no message has set its receive size yet, and
a borrow accepts a free buffer up to twice the size it needs, so those
buffers stay in use. Moving partial messages raised copies there from 0.19 to
0.74 bytes per byte. The other C7 cells match C5 within 1%.

Build C8 is c67d328 without the counters (binaries
`01a159f3-e054-73af-bc3f-8b6ea08f60b1`).

- Throughput with no rate limit (`01a159f3-edbf-7115-c5dd-6d420d46108d`,
  `01a159f3-ee2e-71c3-e05d-043c27ea7fee`; a third run lost its client VM):
  every memcache cell from 4 KiB depth 16 up reaches 37.4 Gb/s with A and with
  `c`, so the link limits them. memcache throughput of `c` is 0.994 to 1.009
  of A's in every cell. The RESP cells are limited by Valkey and move by 2 to
  7% between runs in both directions. At the same throughput `c` uses up to
  9% less client CPU (memcache 1 MiB: 710 against 642 CPU-seconds).
- Latency at the C5 rates, one run so far (`01a159f3-ee9e-719d-b861-353068a62e55`):
  p50 of `c` is at or below A's in 11 of 13 cells, and 2 to 4% lower in the
  depth-16 cells of 8 KiB and up. p99 of `c` is above A's at memcache 64 B
  (1,925 against 1,679 µs), RESP 64 B (909 against 692 µs) and RESP 1 MiB
  (4,653 against 4,014 µs); `c0` falls between them in two of the three, and
  p99 has moved by up to 15% between runs of one build.

The standalone benchmark's `oneshot` strategy
(`docs/recv-buffering-approaches.md`) fell behind the shared ring at 1,000
connections per worker. That strategy grew its regions from 4 KiB and parsed
in the completion handler; this design does neither. It has not been run
above 63 connections per worker.

Per-connection provided rings are not proposed. The benchmark measured them
and recorded wrong bytes in the variant that rewrites a posted entry, one
`RLIMIT_MEMLOCK` page per ring on 7.1, and regions that grew to their cap and
stayed (`recv-buffering-approaches.md`).

## Mechanism

### When a receive is armed

A direct connection has at most one receive in flight, and it is armed only
when a read parks: `with_data`, `with_bytes` or `recv_ready` returns
`Pending` after the parser has seen every buffered byte and returned
`NeedMore` or `NeedAtLeast` (`park_recv` in `runtime/io.rs`). Nothing is
armed at connect or at a completion. While the task works on what it has,
no receive is in flight; data that arrives in that time waits in the socket's
receive queue, and a FIN is seen at the next read.

Arming after the parse means no receive is outstanding while `with_bytes`
holds the accumulator frozen, so the remainder the parser left can be taken
back without a copy (below).

### The buffer in flight

`AccumulatorTable::detach_for_recv` first takes back the frozen remainder
(`reclaim`). If the parser kept no slice of it, `Bytes::try_into_mut`
succeeds and the remainder becomes the mutable buffer in place; otherwise the
remainder is copied once into a new buffer, counted in `BYTES_COPIED`. It then
grows the buffer if needed (below) and moves the whole `BytesMut`, unconsumed
bytes included, out of the accumulator. The driver stores it with the
connection index and generation in a per-worker slab entry
(`Driver::direct_recvs`) and submits `RECV` on the fixed file with the
buffer's spare capacity as the destination, under `OpTag::RecvDirect` with
the slab index as payload.

While the buffer is in the slab the accumulator is empty, and nothing else
writes to a direct connection's accumulator, so nothing can move or free the
memory the kernel is writing. The slab entry keeps the allocation alive until
the completion arrives (domain invariant 1).

On completion (`handle_recv_direct`), the driver frees the slab entry and
checks the connection: the generation must match and the lifecycle must be
`Open`. Otherwise the completion is stale, the buffer is dropped, and
`RECV_DIRECT` `STALE` is incremented. For a live connection,
`attach_received` sets the buffer's length to include the received bytes and
puts it back as the accumulator's buffer, without a copy. Then:

- `n > 0`: count the bytes, record the arrival, wake the reader.
- `0`: the peer closed; mark EOF, wake the reader, close.
- `-ECANCELED`: wake the reader, which re-arms when it parks again.
- Any other error: mark the read half failed, fail the read with the error,
  close.

### Reusing a consumed buffer

`with_bytes` freezes the accumulator's contents for the parser
(`take_frozen`) and leaves the accumulator without a handle to the
allocation. When the parser consumes everything, the frozen view and the
parser's values are the only references to it. On the ring path the next
append allocates a small buffer. On a direct connection the next arm
reserved a buffer of at least the receive size, so a workload whose
receives end at message boundaries allocated and freed 64 KiB per receive.

A direct connection's `with_bytes` therefore keeps the view it consumed whole
(`keep_spent`). `detach_for_recv` takes its allocation back with
`Bytes::try_into_mut`, which succeeds when no slice of it is alive, clears it,
and receives into it. If the caller still holds a value from it, a new buffer
is allocated as before. `tests/recv_direct_alloc.rs` counts allocations of
32 KiB or more over 2,000 request-response batches: 0 with the change, 1,999
without it.

### Buffers

Each direct connection owns one buffer of `recv_direct_region` bytes
(default 32 KiB). With nothing buffered, a receive goes into it, so an idle
connection holds that much and no more. A message that does not fit borrows
a larger buffer from the worker's pool (`DirectPool` in `accumulator.rs`):
the part of the message received so far, at most `recv_direct_region` bytes,
is copied into the borrowed buffer, and the rest of the message is received
behind it. When the parser has consumed everything and no value read from
the borrowed buffer is alive (the `keep_spent` check above), the next receive
goes into the connection's own buffer again and the borrowed one goes back to
the pool.

A buffer grows when it holds part of a message and less than the parser's
`NeedAtLeast` value, or less than half of `recv_direct_region`, is spare. It
grows to hold the rest of the message when the `NeedAtLeast` value is at
least `recv_direct_region`, and otherwise to the larger of the receive size
and the buffered bytes plus `recv_direct_region`. The receive size is
`recv_direct_region`, or for a connection whose largest `Consumed` value so
far is `m`, the larger of `recv_direct_region` and
`min(4m, m + 4 * recv_direct_region)`. The own buffer grows by borrowing. A
borrowed buffer moves the buffered bytes to the front of its allocation when
that makes room (`BytesMut::try_reclaim`), and otherwise takes a buffer of
the new size from the pool, copies the buffered bytes into it, and gives the
old one back. A message longer than any before it on the connection grows
with `BytesMut::reserve` instead, which doubles the allocation, so a 100 MiB
value is not copied once per region; glibc grows large allocations with
`mremap` without copying their bytes.

The pool keeps free buffers by capacity, with no fixed classes. A borrow takes
the smallest free buffer that holds the message and is at most twice the size
it would allocate; otherwise it allocates one of at least 64 KiB, rounded up
to one of eight steps per power of two up to 2 MiB, and to a multiple of
2 MiB (a transparent huge page) above that. Messages of nearly the same size
therefore share buffers, and a 100 MiB value wastes at most 2 MiB. Free
buffers are kept up to `recv_direct_pool_max` bytes per worker (default
16 MiB). A buffer given back when there is no room frees the largest free
buffers that are larger than it until it fits; if they are not enough, it is
freed itself. A buffer larger than `recv_direct_pool_max` is never kept, so a
workload with 100 MiB values reuses their buffers only with
`recv_direct_pool_max` above 100 MiB.

A buffer recovered from a consumed view starts where that view started, so
its `capacity()` leaves out the bytes before it. `put` moves its start back
to the start of the allocation (`try_reclaim`) before keeping it; without
that, a buffer that had held several messages measured under 64 KiB and was
freed, and every batch of 16 × 4 KiB allocated a new one (C5's 1.94 million
allocations per run before ff61e77, 0 after).

A receive that ends inside a message leaves that part to be moved to the
front of the buffer at the next growth. Sizing receives to several messages
makes that move once per several messages. On loopback with 70,000-byte
messages at pipeline depth 16, sizing a receive to the most the connection
had buffered before copied 0.82 bytes per byte received, because each receive
ended at the same point inside a message; sizing by the largest message
copied 0.011.

Before the pool, a connection kept the largest buffer it had grown to until
it closed. That is the 945 MiB RSS increase measured at RESP 1 MiB with 1,000
connections in build C, and it would have been about 20 GiB at 16,000.

`tests/recv_direct_alloc.rs` reads request-response batches of 16 × 60 B,
1 × 64 KiB, 4 × 64 KiB, 1 × 1 MiB, 16 × 4 KiB and 16 × 8 KiB messages: after
four warmup batches, none allocates 32 KiB or more. With the pool keeping
nothing, 1 × 64 KiB makes 196 such allocations in 200 batches, and so do
16 × 4 KiB without the `try_reclaim` in `put`.

In THP mode `always`, the kernel backs each aligned 2 MiB range inside a large
borrowed buffer with a huge page at first touch, so a contiguous buffer gets
huge pages without being split into 2 MiB chunks. A buffer of about 1.1 MiB
contains no aligned 2 MiB range, so at 1 MiB values `c` holds no more huge
pages than A. Mode `madvise` would need `MADV_HUGEPAGE` on borrowed buffers,
which under `defrag=madvise` compacts memory at fault time.

A chain of 2 MiB chunks would need no size classes, but a message spanning
chunks is not contiguous, so `with_bytes` could not hand it to the parser
as one `Bytes` without a copy. It fits a reader that streams huge values
(`get_stream`), which a direct connection refuses today, and is not part of
this design.

### Known lengths

When the parser returns `NeedAtLeast(additional)` with `additional` of at
least `recv_direct_region`, the buffer is grown to hold the rest of the
message before the receive is armed. Growing copies the part of the message
already received, once. The receive asks for all the spare capacity, like
any other, and every completion wakes the reader, whose parser sees the
bytes so far and returns a smaller `NeedAtLeast` or `Consumed`. The receives
that follow land behind the bytes already received, in place, because the
buffer already holds the whole message.

So a parser that overestimates `additional` costs memory and nothing else,
as the `NeedAtLeast` contract documents (`an_overestimated_need_does_not_stall_a_direct_read`).
A reset in the middle of a message completes the next receive with
`ECONNRESET`, which `with_data_result` returns as an error
(`a_reset_in_the_middle_of_a_message_is_an_error`).

An earlier version of the prototype asked for exactly `additional` bytes with
`MSG_WAITALL`, so that a large value arrived in one completion. Both tests
above fail against it: the overestimating reader stalled, and a reset after
part of the rest had arrived was reported as a clean EOF, because the kernel
returns the partial byte count without the error.

In the build C screen only RESP 1 MiB had a known length (about 45,000 such
receives per run, then with `MSG_WAITALL`). memcache 1 MiB copied 0 bytes per
byte without a length, because receives sized by the largest message hold
whole values. Without `MSG_WAITALL`, RESP 1 MiB takes about 27 completions
per value, each of which wakes the reader, and costs 0.965 of A's CPU (C5)
against 0.943 with it (C3).

### Accumulator cap

`detach_for_recv` clamps every receive to `recv_accumulator_max` minus the
bytes buffered. When the accumulator is full and the parser still needs
more, the connection is closed, as it is on the ring path (`config.rs`
documents "The connection is closed if the cap is exceeded").

### Close

`try_finalize_close` cancels a multishot receive by its user data, gated on
`recv_multishot_armed`, which a direct connection never sets. The direct
receive is ended by the close's lead instead: `shutdown` before 6.13,
`ASYNC_CANCEL` of every request on the fixed file from 6.13. Outbound
connections are never handed to another worker, and a handed-off socket is
the only case that closes without a lead. The buffer stays in its slab entry
until the receive's completion, which arrives before the `Close` completion
that frees the connection slot.

### Worker exit

`run_shutdown` waits, among its other conditions, until every
`direct_recvs` entry is empty, and handles `RecvDirect` completions in its
drain loop. The wait is bounded at 100 × 100 ms like the rest of the loop.
The `Ring` field drops before the slab, and the kernel tears the ring down
asynchronously after its fd closes, so an entry still in flight when the
bound runs out is freed while the kernel may hold it. Every connection has
been closed with a lead by then, so the receive has been cancelled; I have not
checked whether the cancel can still be pending after 10 s.

## Which reads a direct connection supports

- `with_data`, `with_bytes`, `with_data_result`, `recv_ready`: supported.
- `segments`, `recv_owned_segment`, `with_segments`, `with_bytes_segments`,
  `forward_to`, `forward_to_conn` and `forward_held` return
  `io::ErrorKind::Unsupported`. They read held provided buffers, and a
  direct connection has none.
- `enable_recv_forward` does nothing on a direct connection, because it
  returns `()`; `forward_held` then returns `Unsupported`. `run_direct_echo`
  resolves at once, because its output is `()`.
- `forward_recv_buf` is a send from the accumulator and is not refused. It
  has not been tested on a direct connection.
- TLS: `.tls(..).recv_direct(true)` fails the connect with `Unsupported`, on
  both backends. The kernel's bytes are ciphertext, and rustls copies the
  plaintext into the accumulator in any case.
- mio: `recv_direct` is ignored. The mio backend reads into one 8 KiB buffer
  per worker and appends to the accumulator (`handle_readable`); reading
  plaintext connections into the accumulator's spare capacity is a separate
  change.
- Accepted connections: not covered.

## API

- `TcpConnect::recv_direct(bool)`, set before the connect is awaited. The
  choice holds for the connection's life.
- `ConfigBuilder::recv_direct_region(bytes)`, the size of a connection's
  own buffer, default 32 KiB, at least 1.
- `ConfigBuilder::recv_direct_pool_max(bytes)`, the most free borrowed
  buffers a worker keeps, default 16 MiB.
- `PoolConfig::recv_direct(bool)` in ringline-redis and ringline-memcache,
  default off, passed to every connection the pool opens. On io_uring, the
  client methods that read with segments fail with an `Unsupported` I/O
  error on such a pool: ringline-redis `recv_meta`, `recv_get_discard` and
  `recv_get_segments`, and ringline-memcache `get_stream` and `get_cas`.

`ClientBuilder::new` takes a `Connection` that is already open, so a caller
that connects itself passes `recv_direct` to `connect`. The sharded and
cluster clients open their own connections from `ShardedConfig` and
`ClusterConfig`, which have public fields and no `#[non_exhaustive]`; their
option waits for the next breaking release. cachecannon opens its own
connections, so build C needed only a cachecannon change.

## Metrics

`ringline/recv_direct`: `ARMED` (receives submitted), `KNOWN_LENGTH` (of
those, receives into a buffer grown for a `NeedAtLeast` of at least
`recv_direct_region`), `BYTES` (bytes the kernel wrote into accumulators),
`STALE` (completions for a closed or reused slot), `POOL_REUSED` and
`POOL_ALLOCATED` (borrowed buffers that came from the pool, and ones
allocated because none fit). Copies on a direct connection still count in
`ringline/recv_ring` `BYTES_COPIED`.

## Tests

- `tests/recv_direct.rs`:
  - Patterned messages from 1 B to 1 MiB + 3, written in uneven pieces with
    pauses, read and checked byte by byte with `with_bytes` (keeping every
    fourth value alive across the next read) and with `with_data`.
  - A connection closed with a receive armed, followed by a connection on the
    same slot that must read only its own bytes.
  - A reader whose `NeedAtLeast` is 10 MiB for 200,000-byte frames reads
    every frame without an EOF.
  - A reset after 2,000 bytes of a 100,000-byte frame is returned as
    `ConnectionReset`.
  - Every refusal listed above, the TLS connect refusal, and a `with_data`
    read on the same connection afterwards.
  - `recv_ready` resolves at once when a read gave up with part of a message
    held in the buffer of an armed receive (fails when `recv_buffered` does
    not count `recv_direct_held`).

  Dropping one received byte fails the first three tests. The overestimate,
  reset and refusal tests fail against the `MSG_WAITALL` version.
- `tests/recv_direct_alloc.rs` (its own binary, for the counting global
  allocator): request-response batches of small, 64 KiB, 1 MiB and
  16 × 4 KiB and 16 × 8 KiB messages make fewer than one allocation of
  32 KiB or more per 20 batches after the warmup (see Buffers).
- `accumulator.rs` unit tests: borrowed sizes round to the steps and to
  2 MiB; the pool reuses a fitting buffer, refuses one more than twice the
  size, keeps no more than its cap, gives a smaller buffer room by freeing the
  largest free one, and frees a larger one; a connection returns its borrowed
  buffer once nothing is buffered and borrows the same one for the next
  large message; a borrowed buffer holding a 1 MiB message, three 4 KiB
  messages and the start of the next 1 MiB message grows to the receive size
  in place (fails against `reserve`, which grows it to 1.49 MiB). Both pool
  tests fail when the pool keeps nothing.
- `tests/recv_direct_copies.rs` (its own binary, for the process-wide
  counters): 70,000-byte messages in 64 KiB and 1 MiB writes copy at most a
  quarter of a byte per byte received; 500 B messages at most 1/20; 1 MiB
  messages at most 1/5, with at least one known-length receive.
- `event_loop.rs` unit tests: a synthetic `RecvDirect` completion with data
  for a connection whose generation moved on is dropped and counted stale
  (fails with the generation check removed); a direct connection whose
  accumulator is at the cap is closed instead of armed.
- The ringline-redis and ringline-memcache `streaming` tests read small and
  100,000-byte values through a pool with `recv_direct(true)` and check that
  a segment read is refused (fails when the pool does not pass the option).

Not tested: worker exit with a receive armed.

## Landing

Done: the 64 B fix (C3), the pool (C5), its growth and eviction fix (C7),
throughput and latency of the commit (C8).

1. PR for the runtime change and the `PoolConfig` option, with a fresh
   review, before the remaining C8 runs: latency runs 2 and 3
   (`01a159f3-ef0a-71ed-fef5-60c8b2cad632`,
   `01a159f3-ef74-71da-d9a7-9f3283f547b1`) and throughput with the client on
   the smaller host (`experiments/segviews-screen-flip.toml`: client on Zen 1
   with 12 threads, servers on Zen 2), where more cells are limited by the
   client. Their results go into this section before the merge.
2. A scale run: 16 workers with 4,000 and 16,000 connections, cells up to
   64 KiB. With 1 MiB values at depth 1, almost every connection is partway
   through a value, so 16,000 connections would hold about 16 GiB of
   borrowed buffers, as the ring path's accumulators would.
3. `ShardedConfig` and `ClusterConfig` option in the next breaking release.
4. mio: read plaintext connections into the accumulator (separate PR).

## Decisions

Made by the owner on 2026-10-19:

1. No `MSG_WAITALL`: a known length grows the buffer, and every completion
   wakes the reader.
2. Reads that need the provided-buffer ring, and TLS with `recv_direct`,
   return `Unsupported`.
3. Client crates: `PoolConfig::recv_direct` in the next release;
   `ShardedConfig` and `ClusterConfig` in the next breaking release.
4. No length hint for ringline-memcache: memcache 1 MiB copied 0 bytes per
   byte without one.
5. Opt-in per connection, off by default, until C3 and the scale run show no
   cell where it costs more than the ring.
6. Segment views: the client switch on `feat/segment-clients` and the proto
   releases it needs are not pursued.

Made by the owner on 2026-10-20:

7. Accept the cost of no `MSG_WAITALL` at RESP 1 MiB (0.965 of A's CPU
   against 0.943).
8. Keep the borrow fit at twice the size: at mixed depth 16 a tighter fit
   would save about 0.8 GB of the 4.4 GB, which is not worth more early
   allocations.
9. Open the PR on the C8 results so far, assuming the remaining runs agree.

Open: whether the scanner PRs in `resp-proto` and `memcache-proto` are
closed or merged without a release.

## Not known

- Whether `c`'s higher p99 in three cells of the first C8 latency run
  (memcache 64 B, RESP 64 B, RESP 1 MiB) holds over three runs.
- Whether `c` raises throughput where the client, not the link or the
  server, is the limit (the flipped-host runs).
- Behaviour above 63 connections per worker.
- Whether a cancelled direct receive can still be in flight when the worker
  exit bound runs out.
