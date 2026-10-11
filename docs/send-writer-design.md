# Encoding sends directly into the send pool

Status: proposal. Prototype and measurement pending (see "Measurement").

## Problem

An application that builds its response bytes pays one copy more on
ringline than on a readiness runtime. It encodes into its own buffer, and
`send` / `send_nowait` copy that buffer into `SendCopyPool` slots. A tokio
server writes from its own buffer, and the only copy is the kernel's.

The profile that showed this ran a RESP server benchmark
(`ringline-bench/src/bin/resp-server.rs` on the `exp/oneshot-ab` branch of
the brayniac/ringline fork, commit 9b72617, not merged) on the two-host rig:
8 workers, 2048 connections, pipeline depth 16, 80/20 GET/SET. The ringline
arm used a 1024-buffer receive ring, so it had no receive-ring starvation.
It was compared with the same server on tokio, one current-thread runtime
per core. Experiments `01a12a6d-1a82` (4 KiB values) and `01a12a6d-1af9`
(64 KiB values):

| Values | Runtime | ops/s | system CPU per op | user `memmove` per op | kernel `rep_movs_alternative` per op |
|---|---|---|---|---|---|
| 4 KiB | ringline | 896k | 24.1 µs | 3.18 µs | 2.03 µs |
| 4 KiB | tokio | 1083k | 17.7 µs | 1.87 µs | 1.23 µs |
| 64 KiB | ringline | 61.7k | 340 µs | 60.3 µs | 34.8 µs |
| 64 KiB | tokio | 72.0k | 259 µs | 34.7 µs | 26.4 µs |

Of the functions in the profile, user `memmove` differs most: 1.31 µs per
op at 4 KiB and 25.6 µs at 64 KiB. Two copies on ringline's side have no
tokio counterpart: the response buffer into the send pool, and the provided
receive buffer into the accumulator. With 80% GETs, response bytes per
operation are about four times request bytes, so most of the extra
`memmove` is the send copy. That split is inferred from the byte counts;
the profile has no call graph that separates the two copies.

An arm of the same server that sent GET values of 4 KiB and up zero-copy
(`send_parts` with guards; experiments `01a12922-edca` and `01a12922-ef13`)
matched tokio's throughput at 4 KiB (1082k against 1080k ops/s) and beat it
at 64 KiB (84k against 73k). It used 9.95 cores of softirq at 4 KiB and
18.43 at 64 KiB. In the profile above, at 4 KiB, the copy arm used 7.44
cores of softirq and tokio 6.06; the zero-copy arm's figures come from a
different run. Guards also require the value to live in memory the
application can hand to the kernel until the zero-copy notification
arrives, which a cache whose values move or expire does not offer cheaply.

This document proposes a send API that removes the application-to-pool copy
without zero-copy: the application encodes into pool slots directly, and the
slots are submitted as they are today. The receive copy is a separate
problem and is not addressed here.

## API

The writer is scoped to one call. The application encodes inside a closure,
and the call commits the bytes when the closure returns.

```rust
impl SendHalf {
    /// Run `f` with a writer over send-pool slots, then queue what it wrote
    /// on this connection. Fire-and-forget, like `send_nowait`.
    pub fn send_with<R>(&mut self, f: impl FnOnce(&mut SendWriter<'_>) -> R) -> io::Result<R>;

    /// As `send_with`, and also return a future that resolves like `send`'s.
    pub fn send_with_awaited<R>(
        &mut self,
        f: impl FnOnce(&mut SendWriter<'_>) -> R,
    ) -> io::Result<(R, SendFuture)>;

    /// Wait for send-pool capacity for `len` bytes, through the same
    /// admission queue as `send_backpressured`, then run `f` and queue its
    /// bytes. Resolves like `send_backpressured`.
    pub fn send_with_backpressured<F, R>(&mut self, len: usize, f: F) -> SendWithFuture<F>
    where
        F: FnOnce(&mut SendWriter<'_>) -> R;
}

pub struct SendWriter<'a> { /* slot chain, state; borrows the call */ }

impl SendWriter<'_> {
    /// Reserve pool capacity for `additional` more bytes written through
    /// `io::Write` or `BufMut`.
    pub fn reserve(&mut self, additional: usize) -> io::Result<()>;

    /// Hand `f` a contiguous region of `max` bytes in the current slot,
    /// starting a new slot if the current one has less room. `f` returns
    /// how many bytes it wrote. `max` must not exceed the slot size.
    pub fn contiguous<F>(&mut self, max: usize, f: F) -> io::Result<usize>
    where
        F: FnOnce(&mut [u8]) -> usize;

    /// Bytes written so far.
    pub fn len(&self) -> usize;

    /// Discard everything written; the call then sends nothing and returns
    /// `Ok` with the closure's result.
    pub fn discard(&mut self);
}

impl std::io::Write for SendWriter<'_> { /* ... */ }
unsafe impl bytes::BufMut for SendWriter<'_> { /* ... */ }
```

`Connection` gets the same three methods, delegating to its `SendHalf` as
its other send methods do. `ConnCtx`'s send methods are `pub(crate)`, and
its `send_with` would be too. The methods take `&mut self`, as every public
send method on `SendHalf` does.

A RESP response loop changes from

```rust
let mut out = Vec::new();
encode_responses(&requests, &mut out);
tx.send_nowait(&out)?;
```

to

```rust
tx.send_with(|w| encode_responses(&requests, w))?; // io::Write or BufMut
```

`resp-proto` encoders write into `&mut [u8]` and report their length
through `encoded_len()`. They use `contiguous`:

```rust
let n = value.encoded_len();
w.contiguous(n, |buf| value.encode(buf))?;
```

`memcache-proto` encoders also write into `&mut [u8]` and have no
`encoded_len()`. `ringline-memcache` already computes an upper bound for
its scratch buffer; it passes that bound as `max` and uses the returned
count.

`contiguous` cannot carry an encoding larger than one slot, and
`resp-proto`'s `encode` has no streaming form. A response with a value
larger than a slot is written by the caller as the frame header, then
`put_slice` (or `write_all`) of the value, which continues into the next
slot, then the trailer.

### Why scoped

A writer that outlives the call (`let w = tx.writer(); ... w.commit()`)
can be held across `.await`. Three problems follow, and the scoped form has
none of them:

- A connection's task is dropped at teardown with no driver installed
  (`handle_close` → `Executor::remove_connection`), so a held writer's
  `Drop` cannot release its slots. `handle_close` already reclaims
  `RecvSegment` bids explicitly for the same reason. A held writer would
  need a thread-local list of orphaned slot chains, drained by the event
  loop.
- The send-capacity FIFO admits its head only when `free_count()` covers
  the head's slots (`SendCompletions::turn`). Every slot outside a
  reservation is freed today by kernel progress. A held writer's slots are
  freed only by the task holding it. A task that holds a writer and then
  awaits `send_backpressured` for more than the remaining free slots waits
  for itself. A task that holds a writer and awaits a message from another
  task, while that task is the FIFO head waiting for capacity, waits
  forever, and every sender behind that head stalls.
- A writer carrying only a connection token can be committed after its
  `SendHalf` is dropped and the write side is taken by another owner
  (`take_send`), for example one running a forward.

Inside the closure the application cannot `.await`, so none of these can
happen. The cost is the coalescing client below.

## Semantics

### Admission and pool exhaustion

The writer takes slots from the pool as it fills them. Its slots count
against the pool from the moment they are taken.

- Any write that needs a slot when none is free marks the writer failed.
  `io::Write::write` and `contiguous` return `Err(Other, "send copy pool
  exhausted")`. `BufMut` cannot report an error. After a failure, its
  `chunk_mut` returns a 64-byte buffer inside the writer, and bytes written
  there are discarded.
- `remaining_mut()` returns `isize::MAX as usize - len()`, as `Vec<u8>`'s
  does. The `BufMut` contract does not allow it to track pool space: it may
  change only through `advance_mut`, and the default `put_slice` panics
  when it is smaller than the slice.
- When the closure returns, a failed writer releases every slot and the
  call returns `Err`, whatever the closure returned. Nothing was queued or
  transmitted, which is the guarantee `send` gives
  (`docs/copied-send-reservation-design.md`). The application can run its
  encoder again. A failed writer is never committed, so a frame cut short
  by a failed write cannot reach the wire.
- A writer whose slots would exceed the pool's slot count fails with
  `InvalidInput`, matching `send`'s refusal of a buffer wider than the
  pool.
- `contiguous` with `max` larger than the slot size returns `InvalidInput`
  and does not mark the writer failed.
- `reserve(additional)` calls `SendCopyPool::reserve_slots` for the slots
  `additional` bytes need after the current slot's free space. After it
  returns `Ok`, `io::Write` and `BufMut` writes up to the reserved total
  take reserved slots and cannot fail. `contiguous` can leave the tail of a
  slot unused, so a writer that mixes `contiguous` with a byte-count
  reservation can need more slots than it reserved; those come from the
  free pool and can fail. An application that wants a guarantee for
  `contiguous` writes reserves one slot per call that can start a new slot.
  Unused reserved slots go back through `release_reservation` when the call
  ends.
- `SendCopyPool` gains `alloc_raw_reserved(&mut SlotReservation)`, the
  `alloc_raw` counterpart of `copy_in_reserved`: it pops a promised slot
  for incremental filling.

### Backpressured form

`send_with_backpressured(len, f)` joins the send-capacity FIFO
(`docs/send-capacity-fifo-design.md`) with the slots `len` needs:
`ceil(len / slot_size)` on a plaintext connection, `bounded_send_slots`'
ciphertext bound on TLS. On a plaintext connection, admission takes that
reservation and `f` runs with it. On TLS the writer holds no reservation
(see "TLS"). The future's id is attached to the last slot at commit, as
`send_bounded` attaches it, so the future resolves on that slot's
completion with the number of bytes written. A closure that writes nothing
settles the id at once with `Ok(0)`, as `send_inner` does for an empty
send. A failed writer cancels the id. Writing past `len` takes further
slots as `send_with` does.

### Order

The bytes are queued on the connection when the closure returns, as one
unit in the per-connection send queue. Wire order between `send_with`
calls, and between `send_with` and `send` on the same connection, is call
order. This is the guarantee `send` gives today: one call's bytes reach the
wire contiguously.

### Stale connections

The call checks the connection's generation, and on io_uring
`close_submitted`, before the closure runs and again before queueing, as
`DriverCtx::send_inner` does. On either, it releases the slots and returns
`NotConnected`.

## Implementation on io_uring

### Slot chain

The writer records its slots without a heap allocation per send
(`docs/backpressured-sends-series-design.md` requires that no per-send heap
allocation be added). `SendCopyPool` gains a `next: Vec<u16>` with one
entry per slot. The writer keeps the first slot, the slot being filled, its
base pointer, capacity and fill level, and the slot count. Sealing a slot
sets its filled length (`set_filled`) and links it to the next one. The
internal TLS `PoolWriter` (`tls/buffered.rs`) keeps the same state in a
`Vec`. It can move to the chain afterwards; that is not required here.

### Pool access

The closure runs outside any `with_state` borrow taken by `send_with`.
Writes inside the current slot are pointer arithmetic on the slot's memory,
which stays at a fixed address for the life of the worker
(`SendCopyPool::backing` is allocated once). Taking or releasing a slot
goes through `with_state`, once per slot.

A `send_with` inside a `with_data` closure reaches the driver from inside
that closure's `with_state`, as `send_nowait` already does in the
documented split-echo pattern (`Connection::split`). That pattern's
soundness under Rust's aliasing rules predates this change and is unchanged
by it.

### Commit

When the closure returns, the call seals the tail slot and walks the chain.
For `send_with_awaited` and the backpressured form, the last slot carries
the send's id and total length (`set_send_id`), so the send settles once.
Each slot becomes one `Sqe::stream_send` through `submit_or_queue`, the
same loop as `DriverCtx::send_inner` after its copy step, inside one
`with_state`. The coalescing in `submit_next_queued` applies to these
entries as it does to any queued send.

## TLS

A writer on a TLS connection writes plaintext into a buffer owned by the
call, taken from a per-worker free list so steady-state use does not
allocate. When the closure returns, the buffer goes to `encrypt_tls_send`,
so admission against the ciphertext bound and the two-copy count are what
`send` has today.

The writer holds no pool reservation on TLS. `encrypt_to_sends` allocates
ciphertext slots with `alloc_raw`, which cannot take reserved slots, and
`encrypt_tls_send` checks `free_count()`, which subtracts reservations. A
reservation held by the writer would therefore count against its own
encryption. On TLS, `reserve` succeeds without reserving, and
`send_with_backpressured` releases its admission reservation before the
closure runs, as `send_bounded` does. The API is the same on TLS and
plaintext connections; TLS gains nothing from it.

## mio

The mio backend queues a `Vec<u8>` per send (`data.to_vec()` in
`DriverCtx::send_inner`). A writer on mio writes into that `Vec` directly,
which removes the `to_vec` copy, and the call queues it as
`PendingSend::plain` or `PendingSend::awaited`. As with `send` on mio, the
pool limits only the backpressured form, whose admission budget is the
pool reservation (`docs/mio-bounded-send-design.md`).

## Protocol clients

`ringline-redis` and `ringline-memcache` encode a request into a reused
scratch buffer and copy it into the pool with `send_nowait`. With
`max_batch_size` 1 (the default), `fire_*` can encode with `send_with`,
which removes that copy.

With coalescing (`max_batch_size` > 1), the client keeps one buffer across
several `fire_*` calls and sends it when the batch fills. A scoped writer
cannot span those calls, so the coalescing path keeps its buffer. A
holdable writer would also not cover it as it stands: the redis client
mixes guard parts into a batch (`fire_set_with_guard`, sent through
`send_parts().submit_batch`), which a writer does not carry, and pool
exhaustion would move from `flush()` to `fire_*`, which changes the
client's recovery path (`pending.truncate`).

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
adds a `--writer` arm to the RESP server benchmark that encodes responses
with `send_with` through `io::Write`: header with `write!`, value with
`write_all`, as the server's copy arm builds its buffer today. Runs: the
rig setup of the profile above, 4 KiB and 64 KiB values, the writer arm
against the copy arm and tokio, three reps each, with a server profile per
arm.

Predictions, recorded before the runs:

- User `memmove` per op falls by most of the difference to tokio: by at
  least 0.9 µs at 4 KiB and at least 18 µs at 64 KiB.
- Throughput rises at both sizes and stays below the zero-copy arm at
  64 KiB. At 4 KiB, softirq CPU stays near the copy arm's 7.44 cores and
  below the zero-copy arm's 9.95. At 64 KiB the comparison needs the copy
  arm's softirq from the same run.
- If `memmove` falls and throughput does not, the send copy was not what
  limited throughput, and the next candidate is the receive copy.
- If `memmove` does not fall, the inference above that most of the extra
  `memmove` is the send copy is wrong.

## Decisions for the owner

1. Scoped or holdable. This proposal is scoped (`send_with`). A holdable
   writer (`writer()` with `commit`) is what the coalescing clients would
   need, and it brings the teardown, FIFO and ownership problems in "Why
   scoped". It could be added later behind those three fixes: an orphan
   list drained by the event loop, a documented rule against awaiting
   admission or other tasks while holding one, and a re-check of the
   write-side owner at commit.
2. Name: `send_with` / `SendWriter`.
3. Whether a `BufMut` write that finds the pool empty is recorded until the
   call ends, as above, or panics. `Vec<u8>`'s `BufMut` cannot fail, so
   encoders written for `BufMut` do not check for errors, and a panic would
   take down the worker.

## Not in scope

- The receive copy into the accumulator.
- Zero-copy parts inside a writer (a guard between two filled slots). It
  would merge the writer with `send_parts`, and the measurement above
  decides whether it is wanted.
- Moving the TLS `PoolWriter` to the slot chain.
- A gauge for slots held by writers: a scoped writer holds slots only
  during one synchronous call.
