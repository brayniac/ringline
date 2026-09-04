# Unbuffered TLS send path

- **Status:** open (engine shipped on both backends; interop and rig sweep are plan 4)
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
(Superseded — the engine landed. See "Plan 3 — the engine" below.)

### Plan 3 — the engine (2026-09-04)

The engine is real: `drive()` runs rustls' eight-state machine, `encrypt_chunk`
writes application-data ciphertext straight into a pool slot, and close_notify
goes through `WriteTraffic::queue_close_notify`. Both backends build and test
green under `--features tls-unbuffered`.

**The copy win is application data only, and on io_uring it comes with a
handshake-side cost.** `WriteTraffic::encrypt` reads plaintext from caller
memory and writes ciphertext straight into the pool slot (io_uring) or the
queued buffer (mio) — that path is genuinely 1 copy, down from 2, on both
backends. But `EncodeTlsData::encode` — the handshake path — is all-or-nothing
on one contiguous `&mut [u8]` and does not take an `io::Write`, and a full-size
handshake record can exceed the default 16384-byte `send_copy_slot_size`. The
buffered engine's `PoolWriter` wrote handshake output straight into pool slots
(1 copy); the unbuffered engine can't, so handshake output goes rustls →
scratch `Vec` → pool slot (2 copies) — one *more* than buffered, on io_uring
only (mio has no pool-slot step either way, so its handshake output stays 1
copy under both engines). Handshake is once per connection, not the
steady-state path this work targets, but the copy tables now say so explicitly
rather than claiming an unqualified win.

**Recv is unchanged at 1 copy, on both engines.** rustls 0.23.41's
`ReadTraffic::next_record` pops an owned `Vec` out of `received_plaintext`
rather than decrypting in place — the unbuffered API's `_incoming_tls` field is
kept only "for forwards compatibility; to support in-place decryption in the
future" (that's rustls' own comment, not ours). So plaintext still has to be
copied into the `RecvAccumulator`. Nobody should read the send-side win and
assume recv moved too.

**`TlsInfo::sni_hostname()` is always `None` for server connections on this
engine**, and stays that way until follow-on work. rustls has no equivalent of
`ServerConnection::server_name()` on `UnbufferedServerConnection` —
`server_name()` lives only on the handshake-callback `ClientHello` type and on
the buffered `ServerConnection`. Recovering it needs a `ClientHello` callback
on `ServerConfig`. Real, user-visible, in the CHANGELOG.

**A pre-existing io_uring bug, found but not fixed here.** `ConnCtx::close()`
(`runtime/io.rs`) calls `Driver::close_connection`, which on io_uring only sets
`recv_mode = Closed` and tears down recv/forward-write state — it never queues
close_notify. The TLS close_notify queueing lives in `DriverCtx::close(conn:
ConnToken)` (`handler.rs`), a different function the `ConnCtx::close()` path
never reaches. So an io_uring TLS server that closes a connection from inside
its own handler (the common case) sends a bare FIN, not a close_notify alert —
the peer can't distinguish that from truncation. This reproduces against the
**buffered** engine too; it predates this effort entirely and isn't caused by
either engine. Filed here rather than fixed, since it's out of scope for a
docs-only task and deserves its own change with its own tests.

Five things the design doc did not anticipate, all found while building
(design-doc `> **Correction**` blocks record the first three in place; the
other two are here because they're more process than design):

- **`process_tls_records` is not callable generically.** rustls implements it
  separately per connection role, and the shared body is private, so
  `ConnectionState<'_, '_, Data>` differs between `UnbufferedClientConnection`
  and `UnbufferedServerConnection` — no single non-generic function can match
  on both. The engine needed a private `UnbufferedEngine` trait with an
  associated `Data` type, dispatched once at the top of `drive()`; the state
  machine body monomorphizes from there.
- **Re-entering `process_tls_records` on a sizing retry is unsound, and it bit
  twice in two different functions before the pattern was recognized.**
  `write_plaintext`'s `perhaps_write_key_update()` and
  `eager_send_close_notify`'s `send_close_notify()` both queue into
  `sendable_tls` *before* the caller ever calls `check_required_size`, and
  `process_tls_records_common` pops `sendable_tls` into the next
  `EncodeTlsData` before `WriteTraffic` is reachable again. A naive
  "try `encrypt`, get `InsufficientSize`, re-enter the state machine and try
  again" retry therefore lands on a *different* state than the one it started
  in. The close_notify case failed silently: the alert got stranded inside
  rustls, `close_notify_sent` was never set, and the connection closed as a
  truncation rather than a clean shutdown — no panic, no error, just a wrong
  answer that only a wire capture would catch. Both call sites now enter the
  state machine once, hold the `WriteTraffic` handle across the whole retry
  loop, and never re-enter for a sizing-only reason.
- **Never drive the machine to "flush" after encrypting or queueing an
  alert.** `write_fragments` drains `sendable_tls` into the *front* of the
  destination buffer, and `check_required_size` reserves room for it — so
  anything already queued (a TLS 1.3 `key_update`, most notably) rides out
  ahead of the fragments about to be encrypted, correctly ordered, as a side
  effect of the same `encrypt`/`queue_close_notify` call. Driving the machine
  again afterwards and emitting that queued output as a *separate* send would
  put it after the already-encrypted records on the wire instead of before —
  the peer would see `bad_record_mac`. This only had to be gotten right once
  because it was caught in review, not in a wire capture, but it's exactly the
  kind of mistake this journal exists to keep someone from making twice.
- **`MAX_SINGLE_APPEND` (64 KiB) is reachable from public config, not just an
  internal assumption.** `ConfigBuilder::recv_buffer` accepts buffers larger
  than that, and the io_uring recv path hands one whole provided buffer to the
  TLS feed in one call. `CiphertextBuf::append`'s `debug_assert` on the 64 KiB
  bound is silent in a release build, so `feed()` chunks its own appends at
  `MAX_SINGLE_APPEND` rather than trusting the caller to stay under it.
- **Dead-code lints are invisible on macOS for this whole module, and that
  cost a red branch once and was rediscovered twice more.** `lib.rs` applies
  `#[cfg_attr(not(has_io_uring), allow(dead_code))]` to all of `tls` (it
  predates this feature — `buffer`, `chain`, `completion`, `fs`, `nvme` etc.
  carry the same attribute), so a clean local `cargo clippy -D warnings` on
  macOS says nothing about unwired unbuffered-engine surface. Only Linux CI
  (or `hv01`) actually lints it.

Still open, deferred to plan 4: interop testing (buffered client ↔ unbuffered
server and the reverse — streams can't be byte-identical, so the assertion is
interoperability, not equality), and the chunk-size rig sweep the design doc's
open question 1 always deferred to measurement.

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
