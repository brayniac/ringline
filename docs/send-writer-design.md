# Encoding sends directly into the send pool

Status: proposal. Prototype and measurement pending (see "Measurement").

## Problem

An application that builds its response bytes pays one copy more on
ringline than on a readiness runtime. It encodes into its own buffer, and
`send` / `send_nowait` copy that buffer into `SendCopyPool` slots. A tokio
server writes from its own buffer, and the only copy is the kernel's.

The profile that showed this ran a RESP server benchmark
(`ringline-bench/src/bin/resp-server.rs` on the `exp/oneshot-ab` branch) on
the two-host rig: 8 workers, 2048 connections, pipeline depth 16, 80/20
GET/SET. The ringline arm used a 1024-buffer receive ring, so it had no
receive-ring starvation. It was compared with the same server on tokio, one
current-thread runtime per core. Experiments `01a12a6d-1a82` (4 KiB values)
and `01a12a6d-1af9` (64 KiB values):

| Values | Runtime | ops/s | system CPU per op | user `memmove` per op | kernel `rep_movs_alternative` per op |
|---|---|---|---|---|---|
| 4 KiB | ringline | 896k | 24.1 µs | 3.18 µs | 2.03 µs |
| 4 KiB | tokio | 1083k | 17.7 µs | 1.87 µs | 1.23 µs |
| 64 KiB | ringline | 61.7k | 340 µs | 60.3 µs | 34.8 µs |
| 64 KiB | tokio | 72.0k | 259 µs | 34.7 µs | 26.4 µs |

User `memmove` is the largest single difference. Two copies on ringline's
side have no tokio counterpart: the response buffer into the send pool, and
the provided receive buffer into the accumulator. With 80% GETs, response
bytes per operation are about four times request bytes, so most of the
extra `memmove` is the send copy. That split is inferred from the byte
counts; the profile has no call graph that separates the two copies.

An arm of the same server that sent GET values of 4 KiB and up zero-copy
(`send_parts` with guards; experiments `01a12922-edca` and `01a12922-ef13`)
matched tokio's throughput at 4 KiB (1082k against 1080k ops/s) and beat it
at 64 KiB (84k against 73k). It used 9.95 cores of softirq at 4 KiB and
18.43 at 64 KiB. In the profile above, at 4 KiB, the copy arm used 7.44
and tokio 6.06; those are a different run. Guards also
require the value to live in memory the application can hand to
the kernel until the zero-copy notification arrives, which a cache whose
values move or expire does not offer cheaply.

This document proposes a send API that removes the application-to-pool copy
without zero-copy: the application encodes into pool slots directly, and the
slots are submitted as they are today. The receive copy is a separate
problem and is not addressed here.

## API

```rust
impl SendHalf {
    /// A writer whose bytes are sent on this connection when it is committed.
    pub fn writer(&self) -> SendWriter;

    /// A writer that waits for send-pool capacity for `len` bytes, through
    /// the same admission queue as `send_backpressured`.
    pub fn writer_backpressured(&self, len: usize) -> SendWriterFuture;
}

pub struct SendWriter { /* connection token, slot chain, state */ }

impl SendWriter {
    /// Reserve pool capacity for `additional` more bytes, so that writes up
    /// to that total cannot fail.
    pub fn reserve(&mut self, additional: usize) -> io::Result<()>;

    /// Hand `f` a contiguous region of `max` bytes in the current slot,
    /// starting a new slot if the current one has less room. `f` returns
    /// how many bytes it wrote. `max` must not exceed the slot size.
    pub fn contiguous<F>(&mut self, max: usize, f: F) -> io::Result<usize>
    where
        F: FnOnce(&mut [u8]) -> usize;

    /// Bytes written so far.
    pub fn len(&self) -> usize;

    /// Queue the bytes on the connection and return without waiting.
    pub fn commit(self) -> io::Result<()>;

    /// Queue the bytes and return a future that resolves like `send`'s.
    pub fn commit_awaited(self) -> io::Result<SendFuture>;
}

impl std::io::Write for SendWriter { /* ... */ }
unsafe impl bytes::BufMut for SendWriter { /* ... */ }
```

`ConnCtx` and `Connection` get the same two constructors, delegating as
their other send methods do. Dropping a writer without committing it
releases its slots and sends nothing.

A RESP response loop changes from

```rust
let mut out = Vec::new();
encode_responses(&requests, &mut out);
tx.send_nowait(&out)?;
```

to

```rust
let mut w = tx.writer();
encode_responses(&requests, &mut w); // io::Write or BufMut
w.commit()?;
```

The encoders in `resp-proto` and `memcache-proto` write into `&mut [u8]`
and report their length through `encoded_len()`. They use `contiguous`:

```rust
let n = value.encoded_len();
w.contiguous(n, |buf| value.encode(buf))?;
```

A value larger than one slot is written with `put_slice` or `write_all`,
which continue into the next slot.

## Semantics

### Admission and pool exhaustion

A writer takes slots from the pool as it fills them. Its slots count against
the pool from the moment they are taken, as a reservation does.

- `io::Write::write` returns `Err(Other, "send copy pool exhausted")` when it
  needs a slot and none is free. Bytes already written stay in the writer.
- `BufMut` cannot report an error. When a `BufMut` write needs a slot and
  none is free, the writer marks itself failed. Further bytes are written
  into a per-worker scratch slot and discarded, and `commit` returns the
  same `Err`.
- `contiguous` returns `Err` when it needs a slot and none is free, and
  `InvalidInput` when `max` exceeds the slot size.
- `commit` on a failed writer releases every slot and returns `Err`.
  Nothing was queued or transmitted, which is the guarantee `send` gives
  (`docs/copied-send-reservation-design.md`). The application can run its
  encoder again.
- A writer whose total exceeds the whole pool can never commit. The first
  slot request beyond the pool's slot count returns, or records,
  `InvalidInput`, matching `send`'s refusal of a buffer wider than the pool.
- `reserve(additional)` calls `SendCopyPool::reserve_slots` for the slots
  the extra bytes need. After it returns `Ok`, writes up to the reserved
  total take reserved slots and cannot fail. An application that knows its
  response length (`encoded_len()`) reserves it first and handles
  exhaustion once, before encoding anything.
- `writer_backpressured(len)` joins the send-capacity FIFO
  (`docs/send-capacity-fifo-design.md`) with `ceil(len / slot_size)` slots
  and resolves to a writer that holds that reservation. Writing past `len`
  takes further slots as `writer()` does.

### Order

A writer's bytes are queued on the connection at `commit`, as one unit, in
the per-connection send queue. Wire order between writers, and between a
writer and `send` calls on the same connection, is commit order. Two writers
on one connection can be open at once; their bytes do not interleave. This
is the guarantee `send` gives today: one call's bytes reach the wire
contiguously.

### Holding a writer

A writer can be held across `.await`. It holds its slots for that time, so
a task that opens a writer and then waits on something unrelated takes pool
capacity from every other connection on the worker. The default pool is
1024 slots of 16,448 bytes per worker. The documentation states this, and a
`ringline/pool` gauge counts slots held by uncommitted writers.

### Stale connections

`commit` checks the connection's generation and `close_submitted`, as
`DriverCtx::send` does, and on either releases the slots and returns
`NotConnected`.

## Implementation on io_uring

### Slot chain

The writer records its slots without a heap allocation per send (the
backpressured-sends series requires that no per-send heap allocation be
added). `SendCopyPool` gains a `next: Vec<u16>` with one entry per slot.
The writer keeps the first slot, the slot being filled, its base pointer,
capacity and fill level, and the slot count. Sealing a slot sets its filled
length (`set_filled`) and links it to the next one. The internal TLS
`PoolWriter` (`tls/buffered.rs`) keeps the same state in a `Vec`. It can
move to the chain afterwards; that is not required here.

### Pool access

The writer holds no borrow of the driver between calls. Writes inside the
current slot are pointer arithmetic on the slot's memory, which stays at a
fixed address for the life of the worker (`SendCopyPool::backing` is
allocated once). Taking or releasing a slot goes through `with_state`, once
per slot.

A writer used inside a `with_data` closure therefore reaches the driver
from inside that closure's `with_state`, as `send_nowait` already does in
the documented split-echo pattern (`Connection::split`). The writer adds no
new kind of nested access. Whether that pattern is sound under Rust's
aliasing rules is an existing question, not one this change opens.

### Commit

`commit` seals the tail slot and walks the chain. For `commit_awaited`,
the last slot carries the send's id and total length (`set_send_id`), so
the send settles once. Each slot becomes one `Sqe::stream_send` through `submit_or_queue`,
the same loop as `DriverCtx::send_inner` after its copy step. The
coalescing in `submit_next_queued` applies to these entries as it does to
any queued send.

Slots that a reservation promised but writes did not fill go back through
`release_reservation`.

## TLS

A writer on a TLS connection writes plaintext into a buffer owned by the
writer, taken from a per-worker free list so steady-state use does not
allocate. `commit` passes it to `encrypt_tls_send`, so admission against the
ciphertext bound and the two-copy count are what `send` has today. The API
is the same on TLS and plaintext connections; TLS gains nothing from it.

## mio

The mio backend queues a `Vec<u8>` per send (`data.to_vec()` in
`DriverCtx::send_inner`). A writer on mio writes into that `Vec` directly,
which removes the `to_vec` copy, and `commit` queues it as
`PendingSend::plain` or `PendingSend::awaited`. As with `send` on mio, the
pool limits only `writer_backpressured`, whose admission budget is the pool
reservation (`docs/mio-bounded-send-design.md`).

## Protocol clients

`ringline-redis` and `ringline-memcache` encode a request into a reused
scratch buffer and copy it into the pool with `send_nowait`. With
`max_batch_size` 1, `fire_*` can encode into a writer and commit it, which
removes that copy.

With coalescing (`max_batch_size` > 1), the client keeps one buffer across
several `fire_*` calls and sends it when the batch fills. Holding a writer
across those calls in place of that buffer works under the rules above.
The pool slots are then held until the flush. That is a change in client
memory behaviour, so it goes in its own change, after the server-side
measurement.

## Relation to the other send methods

`send`, `send_nowait`, `send_backpressured`, `send_parts`, `send_chain`,
`forward_recv_buf` and `forward_to_conn` are unchanged. The writer is for
bytes the application produces. Bytes that already exist in memory the
application owns go through `send` (one copy) or a guard (no copy, at or
above `send_zc_threshold`).

The writer becomes the documented first choice for responses an application
encodes, in the crate docs and in `docs/syscalls-and-copies.md`. Its row in
the CLAUDE.md copy table is "user encode → `SendCopyPool` slot: 0 copies
beyond the encode".

## Measurement

The prototype goes on an experiment branch with the io_uring path only. It
adds a `--writer` arm to the RESP server benchmark that encodes through
`io::Write` into a `SendWriter`. Runs: the rig setup of the profile above,
4 KiB and 64 KiB values, the writer arm against the copy arm and tokio,
three reps each, with a server profile per arm.

Predictions, recorded before the runs:

- User `memmove` per op falls by most of the difference to tokio: by at
  least 0.9 µs at 4 KiB and at least 18 µs at 64 KiB.
- Throughput rises at both sizes and stays below the zero-copy arm at
  64 KiB. Softirq CPU per op stays at the copy arm's level, below the
  zero-copy arm's.
- If `memmove` falls and throughput does not, the send copy was not what
  limited throughput, and the next candidate is the receive copy.
- If `memmove` does not fall, the inference above that most of the extra
  `memmove` is the send copy is wrong.

## Decisions for the owner

1. Name. `writer()` / `SendWriter` here. `send_with(|w| ...)` with a
   closure is the alternative. It would not be holdable across `.await`,
   which the protocol clients need for coalescing.
2. Whether the writer borrows `&mut SendHalf`. Borrowing prevents a `send`
   on the same connection while a writer is open. That rules out an
   ordering surprise, but it also rules out two open writers and the
   coalescing client's use. This proposal does not borrow; order is commit
   order.
3. Whether `BufMut` failure is recorded silently until `commit`, as above,
   or panics. `Vec<u8>`'s `BufMut` cannot fail, so encoders written for
   `BufMut` do not check for errors, and a panic would take down the
   worker.

## Not in scope

- The receive copy into the accumulator.
- Zero-copy parts inside a writer (a guard between two filled slots). It
  would merge the writer with `send_parts`, and the measurement above
  decides whether it is wanted.
- Moving the TLS `PoolWriter` to the slot chain.
