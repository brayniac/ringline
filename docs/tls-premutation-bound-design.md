# TLS pre-mutation ciphertext bound (series PR 8)

Eighth PR of the series that lands #318
(`docs/backpressured-sends-series-design.md`, "PR 8"). Origin: @thinkingfish
in #318; re-derived on `main` after a survey of both TLS engines. Sits on
#376, #377, #381, #382 and #384. PR 9 consumes it.

No public API. No changelog line.

## The problem

`send_backpressured` must decide admission **before** rustls mutates. Today
both backends size a bounded TLS send's permit by *plaintext* length, and
ciphertext is larger by per-record overhead. Worse, on io_uring the
reservation cannot even be held across encryption, because encryption
allocates from the same pool and an outstanding reservation hides those
slots from it — so the TLS branch returns before reserving at all.

The cost of getting this wrong is asymmetric, and that asymmetry drives
every decision below:

- **A bound that is too large** makes a bounded send wait for capacity it
  did not strictly need. The caller is delayed. Nothing breaks.
- **A bound that is too small** lets the send start encrypting and run out
  of pool mid-record. rustls has already advanced its record sequence, so
  by departure 1 the operation fails *and the connection is closed*. A
  peer that would have seen backpressure instead sees a dead connection.

So every estimate below is deliberately conservative, and a debug tripwire
asserts the bound was never beaten.

## What the bound actually is

Measured against rustls 0.23.41 (`Cargo.lock`), whose relevant constants are
all `pub(crate)` and therefore unreachable — ringline already keeps its own
copies and says why (`tls/unbuffered/mod.rs`).

Per record: a 5-byte header plus AEAD expansion. TLS 1.3 adds 17 bytes
(content-type byte + tag); TLS 1.2 GCM adds 24 (explicit nonce + tag). So
**22 bytes per record on TLS 1.3, 29 on TLS 1.2 GCM**.

**The bound uses 29.** The library build negotiates only TLS 1.3 — `rustls`
is pulled without the `tls12` feature — but `ringline/Cargo.toml` enables
`tls12` for dev-dependencies, and with resolver 2 that unifies for every
test target. A downstream crate can unify it in too. A bound that is right
for the library build and wrong under `cargo test` is not a bound. This is
also why the existing `MAX_RECORD_WIRE_LEN` is the wrong constant to reuse:
it assumes TLS 1.3 and explicitly disclaims correctness dependence.

Fragment size comes from `ClientConfig`/`ServerConfig::max_fragment_size`
(a `pub` field, default `None` → 16389). **It includes the 5-byte header**,
so plaintext per record is `cfg - 5`; rustls' own
`MessageFragmenter::set_max_fragment_size` does the same subtraction. There
is no negotiated per-connection value — the `max_fragment_length` extension
is never negotiated — so the config is the only source. `TlsConn` gains a
`max_plaintext_per_record` field recorded at `create` / `create_client`,
already subtracted, so no call site can repeat the off-by-five.

**Byte bound** (both engines, both backends):
`records = ceil(plaintext / F)`, `bytes = records * (F + 29)`, where `F` is
the plaintext per record.

## Slot bounds diverge, and must not share a constant

**Buffered** packs ciphertext contiguously through `PoolWriter`, letting a
record straddle two slots, so `slots = ceil(bytes / slot_size)` exactly.

**Unbuffered** cannot straddle: `encrypt_chunk` writes whole records into
one slot. The byte formula therefore *under*-estimates it. A worked case:
at `slot_size` 32768 and 49152 bytes of plaintext, the byte formula says 2
slots; the engine takes 3.

The engine's real per-slot capacity is not a closed form. It is a cached
`max_plaintext_per_chunk` that only ever shrinks for a given basis, and the
hint that seeds it hardcodes the default fragment constants — so with an
overridden fragment size the hint is wrong and the shrink loop converges
instead. A bound derived from that cache would be coupling admission to an
internal heuristic that is allowed to become pessimistic.

**Decision: bound the unbuffered slot count at one record per slot**,
`slots = records`. That is the engine's own worst case and is always safe,
whatever the cache does. It over-reserves when slots are large enough to
hold several records, which costs admission latency and nothing else — the
right side of the asymmetry. If a later measurement shows the waste matters,
the fix is a tighter *measured* bound, not a cleverer formula.

**Slack for `sendable_tls`.** rustls drains anything already queued — a TLS
1.3 `key_update`, an alert — into the front of the same destination, and its
size is not a function of the plaintext. The unbuffered engine exposes no
byte count for it at all (`tls_bytes_to_write` is buffered-only). The bound
therefore adds **one whole record** of headroom unconditionally.

## Where the check goes

**Departure 2 holds with no signature changes.** Both bounded call sites are
distinct functions from their plain twins, so `encrypt_to_sends` and
`encrypt_for_send_mio` are untouched and plain `send` / `send_nowait` /
`send_parts` keep today's admission behaviour exactly.

**io_uring** (`DriverCtx::send_bounded`, TLS branch): check
`send_copy_pool.free_count() >= capacity_slots` immediately before
`encrypt_to_sends`; on failure return the same `Exhausted` error as the
plaintext path, *before* anything mutates. A plain capacity check is
sufficient here and was verified: the worker is single-threaded, no
reservation is outstanding on this path, and nothing allocates from the pool
between the check and the first allocation inside the call.

**mio** (`DriverCtx::send_bounded`, TLS branch): the reservation is already
taken before the TLS branch; only its *size* is wrong. Size it by
`capacity_slots` instead of the plaintext, then shrink the permit to what
the ciphertext actually needs once `encrypt_for_send_mio` returns.

`SlotReservation` has no partial-release API — it is a bare count, and its
`Drop` debug-asserts the remainder was returned. PR 8 adds
`SendCopyPool::shrink_reservation(&mut SlotReservation, keep: usize)`,
which returns the difference to the pool. Release-then-re-reserve is
rejected: it would need an `expect` that is only sound because nothing runs
in between, and that is exactly the kind of invariant that stops being true
later.

## The tripwire

Bounds that are checked only by the tests that were written for them tend to
drift. Both bounded paths therefore `debug_assert` that the slots actually
consumed did not exceed the bound — io_uring by comparing the pool's free
count before and after `encrypt_to_sends`, mio by comparing the ciphertext
length against the byte bound. If any of the three modelled hazards is ever
beaten (unbuffered record waste, a pessimistic cache, an unmodelled
`sendable_tls` drain), a debug build says so loudly instead of closing a
connection in production. This is the same technique as #384's release
guard, for the same reason.

## Forward coupling with PR 9

`SendCapacityQueue::enqueue` takes `required_slots`, and `turn` compares it
against the pool's free count. PR 9's future **must compute that number with
this same capacity function**, or the FIFO will admit a message the backend
then refuses. The function is therefore public to the crate and documented
as the single source of that number, and PR 9's oversize-rejection test must
use it too.

## Tests

Against real rustls records, using the in-memory handshake harnesses that
already exist (`tls/buffered.rs`'s `test_certs`/`pump`/`handshaked`, and
`tls/unbuffered/tests.rs`'s `conn_pair`/`handshake`). The buffered harness is
currently gated `cfg(all(test, has_io_uring))`, which keeps it off the
development machine for no reason; PR 8 widens that to `cfg(test)`.

- default fragment size: bound ≥ actual, for a plaintext spanning one
  record, exactly one record, and several;
- **fragment-size override** (`max_fragment_size = Some(2048)`): same, and
  the off-by-five is visible here — a bound that forgot the header
  subtraction under-counts records;
- TLS 1.2 (`rustls::version::TLS12`, reachable in test builds only): the
  29-byte worst case is exercised rather than assumed;
- the unbuffered slot bound against a slot size that holds several records,
  which is the case the byte formula under-estimates;
- `shrink_reservation` returns the difference and leaves the remainder
  releasable.

## Verification

Four configurations must be green, per the series doc: default/io_uring,
default/mio, `tls-unbuffered`/io_uring, `tls-unbuffered`/mio. macOS covers
the two mio ones; the io_uring pair runs on a Debian 13 anvil guest.

The two-guest A/B does not apply — this PR changes only admission arithmetic
on a path with no caller until PR 9.
