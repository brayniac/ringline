# Unbuffered TLS send path

- **Status:** open (foundation shipped; engine not yet built)
- **Span:** 2026-09-03 → · PR #338 · unreleased (post-0.6.0)

Written after the foundation landed rather than before it, contrary to this
journal's own "land intent before building" rule. Recording that here because
the omission is the kind of thing the rule exists to prevent.

## Goal

Remove one of the two copies TLS pays on every send.
[`docs/syscalls-and-copies.md`](../syscalls-and-copies.md) records TLS sends at
2 copies versus 1 for plaintext:

1. user bytes → rustls' internal `sendable_plaintext` buffer (`conn.writer().write`)
2. rustls encrypts out of that buffer into the send-pool slot (`conn.write_tls`)

Copy 2 is intrinsic, and #254 already removed the scratch buffer that used to
sit beside it. **Copy 1 is pure buffering overhead** — the plaintext is already
contiguous in caller memory — and the buffered rustls API has no way to skip it.
rustls' *unbuffered* API does: `WriteTraffic::encrypt(&[u8], &mut [u8])` reads
plaintext from a caller slice and writes ciphertext into a caller buffer.

Secondary payoff: `send_parts().guard()` is refused outright under TLS today.
With `encrypt` taking `&[u8]`, a `SendGuard`'s memory can be the plaintext
source. Not zero-copy to the wire — that needs NIC offload — but it stops the
API refusing a reasonable request.

## Why not kTLS, which was the original question

kTLS was where this started. Two findings moved it to a follow-on:

1. **kTLS is all-or-nothing per connection.** `dangerous_extract_secrets(self)`
   consumes the rustls connection on all three overloads, so there is no "kTLS
   for TX, rustls for RX" — extraction destroys the state machine that would
   decrypt inbound records. That makes kTLS strictly larger than it looks:
   Linux-only, kernel-cipher-restricted, needing the `RecvMsgMulti` + cmsg path
   to read TLS control records, plus KeyUpdate handling, kernel-version gating,
   and a fallback for every unsupported case.
2. **The unbuffered path gets the same send-side copy count on *both* backends**,
   including macOS, with none of that — and is a prerequisite anyway, since
   `dangerous_extract_secrets` is implemented on `UnbufferedConnectionCommon`.

kTLS remains the only route to `sendfile` and NIC crypto offload, which is where
the *remaining* copy goes. Reopen it once the unbuffered engine is real.

## GO/NO-GO criteria

- **GO** if TLS sends measurably drop to 1 copy with no regression in
  throughput or latency on the rig, on both backends.
- **NO-GO** if driving rustls' 8-state `ConnectionState` machine proves to cost
  more than the copy it saves, or if the ciphertext-buffer management needed by
  `process_tls_records` cannot be made amortized O(1) — see below, where that
  nearly happened.

## What happened

Design: [`docs/tls-unbuffered-design.md`](../tls-unbuffered-design.md). Split
into three plans; **plan 1 (foundation) shipped in PR #338**, behavior-neutral:

- `a2ea825` — `tls.rs` (1181 lines) split into `tls/mod.rs` + `tls/buffered.rs`,
  verified byte-identical by ordered reconstruction diff.
- `32ac78b` — `CiphertextBuf`, the incoming-ciphertext buffer the engine will
  drive. Not yet wired up.
- `b1bc91f` — default-off `tls-unbuffered` cargo feature and its CI job, so the
  second path is linted from the start.

### Three bugs in `CiphertextBuf`, all found by adversarial review

Recorded because the first one shipped behind a test that proved nothing, and
the pattern is worth not repeating.

- **Unguarded second compaction site.** `make_room` had two `compact()` calls;
  only one was gated on "compaction pays for itself". The other fired exactly
  when the first did not (`live > buf.len()/2`), so once the buffer was backed
  up every append memmoved the whole live set — **16383× amplification** at a
  1 MiB cap, remotely triggerable by a peer that outruns the reader. This is the
  O(N·K) shape #279 removed. The original test ran at 0.002% occupancy, the one
  regime where the bound holds regardless of implementation.
- **`WouldBlock` deadlock at the cap floor.** The fix for the above returns
  `WouldBlock` when the buffer is full of live data. `WouldBlock` implies
  `live > (cap − additional)/2`, so safety needs `cap/2` to exceed the largest
  set rustls can leave unprocessed — but rustls joins handshake messages
  spanning records *inside the caller's buffer* up to `MAX_HANDSHAKE_SIZE`
  (0xffff), making the first floor exactly half what was required. Demonstrated:
  `cap=83972 start=20000 live=63972 append(1448)` hangs. Floor is now
  `2·MAX_UNPROCESSABLE + MAX_SINGLE_APPEND` (233,480), verified at 0 refusals
  across 71,829 unprocessable states.
- **Cap was not a hard ceiling.** `make_room` resized from a `needed` computed
  before compaction, so `.max(needed)` defeated the `.min(cap)` clamp.

Every fix carries a regression test **verified to fail against the broken
implementation**. The first replacement test written for the cap bug passed
against both versions and was discarded for a brute-forced case that
discriminates.

### A cfg-gated bug macOS could not catch

`TlsTable::send_close_notify_queued` in `mod.rs` calls `take_tls_output_sends`,
which the split moved to `buffered.rs` as a private fn. A child module sees an
ancestor's privates; the reverse does not hold. The call is
`#[cfg(has_io_uring)]`, so every local check on macOS passed and Linux CI failed
six jobs on one root cause. Fixed with `pub(super)`, which is the scope it had
as a single file. Notably a reviewer *did* flag cross-module visibility as a
risk but named the opposite direction — the one that is actually free.

## Outcome

Foundation only. No copy has been removed yet; the engine is plan 2.

## Lessons / open questions

- **A test that asserts a bound while exercising the wrong regime is worse than
  no test**, because it converts an unexamined assumption into apparent
  evidence. Both amortization bugs here passed a green suite. The cheap
  countermeasure that caught both: require a regression test to *fail against
  the broken implementation* before trusting it.
- **The module split is by file size, not cleanly by engine.** `TlsConnKind`,
  `TlsConn`, `TlsTable` and `drain_tls_plaintext` remain bound to the buffered
  API (`ClientConnection`/`ServerConnection`, `reader()`/`writer()`). Only
  `TlsInfo`, `TlsRecvResult`, `PlaintextSink` and `build_pool_send` are
  genuinely engine-agnostic. Plan 2 must thread an engine dimension through the
  former set — work the original design did not account for.
- **`MAX_SINGLE_APPEND` is a cross-module coupling.** The cap floor's
  no-deadlock argument assumes no single `append` exceeds 64 KiB. `append`
  `debug_assert`s it, but plan 2's recv wiring must size its buffer accordingly.
- Open: chunk-size policy for `encrypt` (fill slots to `slot_size − overhead`,
  or a fixed 16 KiB record with a larger slot?) wants a measurement.
- Open: `CiphertextBuf`'s capacity default — reuse `recv_buffer_size`, or a
  dedicated setting? `MIN_CIPHERTEXT_CAP` is the floor.
