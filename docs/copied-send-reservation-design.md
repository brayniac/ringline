# Transactional copied-send reservation (series PR 4)

Fourth PR of the series that lands #318
(`docs/backpressured-sends-series-design.md`, "PR 4"). Design origin:
@thinkingfish in #318; re-derived on current `main`.

## Problem

`DriverCtx::send` on io_uring (`ringline/src/handler.rs`) splits data larger
than one send-pool slot into `data.chunks(slot_size)` and, per chunk, calls
`SendCopyPool::copy_in` and then `submit_or_queue`. When the pool runs out on
chunk *k*, chunks 1..k-1 are already queued (or on the wire) and the caller
gets `Err("send copy pool exhausted")`. The method's own doc says so and tells
callers to treat the error as fatal for the connection, because a retry would
duplicate the prefix. `ConnCtx::send` / `send_nowait` document only "Err if
the pool is exhausted or the SQ is full", so a handler that retries after an
error — the natural reaction to a pool-pressure error — silently corrupts the
stream for any send wider than one slot. A send wider than the *whole* pool
can never succeed and today fails only after committing the first
`slot_count` chunks (the 4 MiB-on-64-slots incident during PR 1).

The other multi-slot producers are already transactional:

- TLS, both engines (`tls/backend_uring.rs`, `tls/buffered.rs`): stage slots,
  `release_all` on failure, drain into the send vector only on success.
- `send_parts` / `send_chain` copy parts: one `copy_in_gather` slot, or an
  atomic chain push.
- mio `DriverCtx::send`: one `Vec<u8>` per logical send.

Only the plaintext io_uring path is non-transactional. This PR fixes that,
with no per-send heap allocation (series departure 3).

## Design

### Pool: count-based reservation

`SendCopyPool` gains a `reserved: usize` counter and a token type:

```rust
#[must_use = "an unfilled reservation must go back through release_reservation"]
pub struct SlotReservation { remaining: usize }

pub enum ReserveError {
    /// Fewer than `needed` slots are free right now (retryable).
    Exhausted,
    /// `needed` exceeds the pool's total slot count (never retryable).
    TooLarge { needed: usize, capacity: usize },
}

impl SendCopyPool {
    /// Promise `n` slots to the caller without popping them. Fails without
    /// side effects (other than the SEND_EXHAUSTED metric on `Exhausted`).
    pub fn reserve_slots(&mut self, n: usize) -> Result<SlotReservation, ReserveError>;

    /// Pop one promised slot and copy `data` into it. `data.len()` must be
    /// <= slot_size and the reservation must have slots remaining (both
    /// `debug_assert`ed; the caller iterates `chunks(slot_size)` over a
    /// buffer whose chunk count it reserved).
    pub fn copy_in_reserved(&mut self, r: &mut SlotReservation, data: &[u8]) -> (u16, *const u8, u32);

    /// Return the unfilled remainder of a reservation to the pool.
    pub fn release_reservation(&mut self, r: SlotReservation);

    /// Slots available to a new allocation: free_list.len() - reserved.
    pub fn free_count(&self) -> usize;   // was #[cfg(test)]
    /// Total slots.
    pub fn slot_count(&self) -> usize;
}
```

- `reserve_slots(n)`: `n > count` → `TooLarge`; `n > free_count()` →
  `Exhausted` (+ metric); else `reserved += n`, `Ok(SlotReservation { remaining: n })`.
  `n == 0` succeeds with an empty reservation (empty sends stay a no-op).
- `copy_in_reserved`: pops `free_list` (guaranteed non-empty by the
  accounting), fills exactly as `copy_in` does, `reserved -= 1`,
  `r.remaining -= 1`.
- `release_reservation`: `reserved -= r.remaining`.
- `copy_in`, `copy_in_gather`, `alloc_raw` take a slot only when
  `free_count() > 0`, so an outstanding reservation is honoured even if a
  future caller interleaves an allocation. Today nothing does: the reservation
  lives inside one synchronous `DriverCtx::send` call, and `push_sqe`'s
  internal `ring.submit()` cannot run completions (no `GETEVENTS`).
- `SlotReservation` has a `Drop` with `debug_assert_eq!(remaining, 0)`. It
  cannot release without the pool, so misuse is caught in debug/test builds
  rather than silently shrinking the pool.
- `free_count` and `slot_count` become `pub(crate)`-visible (the `buffer`
  module is `pub(crate)`) with no `cfg(test)`. Both have production callers
  in this PR (`reserve_slots` uses both), so no dead-code lint on either
  backend; PR 5's admission FIFO is their next consumer.

### io_uring `DriverCtx::send`

The plaintext branch becomes:

```rust
let slot_size = self.send_copy_pool.slot_size() as usize;
let needed = data.len().div_ceil(slot_size);
let mut reservation = match self.send_copy_pool.reserve_slots(needed) {
    Ok(r) => r,
    Err(ReserveError::Exhausted) => return Err(io::Error::other("send copy pool exhausted")),
    Err(ReserveError::TooLarge { needed, capacity }) => return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("send of {} bytes needs {needed} send-pool slots but the pool has {capacity} \
                 (raise Config::send_pool)", data.len()),
    )),
};
let mut chunks = data.chunks(slot_size).peekable();
while let Some(chunk) = chunks.next() {
    let (slot, ptr, len) = self.send_copy_pool.copy_in_reserved(&mut reservation, chunk);
    self.send_copy_pool.set_end_of_send(slot, chunks.peek().is_none());
    // ... build the SQE exactly as today ...
    if let Err(e) = self.submit_or_queue(conn.index, built) {
        self.send_copy_pool.release_reservation(reservation);
        return Err(e);
    }
}
self.send_copy_pool.release_reservation(reservation); // remaining == 0
Ok(())
```

Why this is transactional: `submit_or_queue` pushes to the ring only when
the connection has no send in flight, i.e. only for chunk 1; once chunk 1 is
accepted, `in_flight` is set and every later chunk is queued, which cannot
fail. So the only failure after reservation is a full SQ on chunk 1, which
`submit_or_queue` already handles by releasing that slot before returning —
nothing has been transmitted or queued, and the remaining reservation is
returned. The error kind stays `Other` (series departure 1).

The error message strings are unchanged for the `Exhausted` case so existing
tests and the documented `send_nowait` contract keep matching.

### Ring: `#[cfg(test)] force_push_failures(n)`

`Ring` gains `#[cfg(test)] forced_push_failures: usize`;
`push_sqe128` returns `io::Error::other("forced SQ push failure")` and
decrements while the counter is non-zero, before touching the real queue.
`push_sqe` routes through `push_sqe128`, so `submit_or_queue`,
`submit_next_queued` and every `submit_*` helper are covered;
`push_sqe_chain`'s multi-entry path is not (it uses `push_multiple`) and is
out of scope. The kind is `Other`, not `WouldBlock` as in #318 (departure 1).

### Docs

- `DriverCtx::send` doc: replace the "chunks queued before it are already
  committed ... treat a mid-buffer error as fatal" paragraph with the new
  contract: on `Err` nothing was queued or transmitted; the same buffer may
  be resent later.
- `ConnCtx::send` and `send_nowait` "# Errors": add the one-line guarantee
  and name `InvalidInput` for a send wider than the whole pool.
- `docs/send-completion-design.md`: one paragraph under the copied-send
  section stating the reservation invariant (this is the required reading
  for the send path).
- CHANGELOG `Unreleased` / Fixed: one entry. The series doc said "no
  changelog line" for PR 4 on the assumption that only PR 9 would surface
  the change; the retry hazard above is observable today through the public
  `send`/`send_nowait` API, so it gets its own line, and the series doc's
  PR 4 paragraph is updated to say so.
- Journal `docs/journal/2026-09-backpressured-sends-series.md`: PR 4 entry.

## Tests

Pool unit tests (`buffer/send_copy.rs`, both backends):

- `reserve_then_fill_consumes_exactly_the_reserved_slots`: pool(4, 8);
  reserve 3; `free_count` == 1; fill three chunks; `free_count` == 1,
  `reserved` back to 0 (observable as `free_count` == `free_list.len()`
  after `release_reservation`).
- `reserve_fails_without_side_effects_when_short`: pool(2, 8); take one
  via `copy_in`; `reserve_slots(2)` → `Exhausted`; `free_count` == 1;
  `copy_in` still succeeds.
- `reserve_rejects_more_than_the_pool_holds`: pool(2, 8);
  `reserve_slots(3)` → `TooLarge { needed: 3, capacity: 2 }`; `free_count` == 2.
- `reservation_blocks_plain_allocation_until_released`: pool(2, 8);
  reserve 2; `copy_in` → `None`; `alloc_raw` → `None`; release; `copy_in`
  → `Some`.
- `partially_filled_reservation_releases_the_rest`: pool(3, 8); reserve 3;
  fill 1; release; `free_count` == 2.
- `zero_slot_reservation_is_a_no_op`.

io_uring event-loop tests (`backend/uring/event_loop.rs`, `cfg(has_io_uring)`,
run on Linux CI and the delta VM job), config `send_pool(4, 64)`:

- `send_wider_than_free_slots_commits_nothing`: `copy_in` three fillers;
  `make_ctx().send(token, &[0; 100])` (needs 2) → `Err` (kind `Other`);
  `free_count` == 1; send queue empty; `in_flight` false.
- `send_wider_than_the_pool_is_invalid_input`: `send(token, &[0; 300])` →
  `Err` kind `InvalidInput`; `free_count` == 4; nothing queued.
- `send_with_full_sq_and_idle_queue_commits_nothing`:
  `ring.force_push_failures(1)`; `send(token, &[0; 200])` → `Err`;
  `free_count` == 4; queue empty; `in_flight` false; a following
  `send(token, b"ok")` succeeds (the hook is consumed).
- `multi_chunk_send_still_queues_all_chunks_in_order`: with an in-flight
  send, `send(token, &[0; 200])` queues 4 entries whose `pool_slot`s carry
  `end_of_send` false,false,false,true (guards the streaming rewrite).

Integration (`ringline/tests/echo.rs`, both backends, small pool): a handler
that sends a response wider than the free pool, gets `Err`, waits one tick,
retries, and the client receives exactly one copy of the response. On mio
this passes today (Vec-backed) and pins the contract; on io_uring it fails
before this PR (duplicate prefix) — verified red-then-green on the VM.

## Verification

- `cargo fmt`, clippy `-D warnings` default + `force-mio` (+ `tls-unbuffered`
  compile check), `cargo test --all` both backends on the Mac.
- io_uring: one-guest VM job on delta (`uring-*-job.json` template: clone
  under `$HOME`, clippy + tests, `clippy_exit=`/`test_exit=` markers) before
  the PR opens; adversarial subagent review of the diff against Domain
  Invariants 1–5 and this document; CI green before merge.

## Out of scope

- Bounded-send identity (`slot_bounded_send_id` in #318) — PR 6/7.
- TLS pre-mutation bound — PR 8.
- A `WouldBlock` push error or any parking on SQ pressure — rejected by
  series departure 1.
- `push_sqe_chain` failure injection.
