# Unbuffered TLS send path — design

**Status:** Draft / design (not yet planned or implemented)
**Date:** 2026-09-03
**Related:** [`docs/syscalls-and-copies.md`](syscalls-and-copies.md), [`docs/send-completion-design.md`](send-completion-design.md), `ringline/src/tls.rs`
**Follow-on:** kTLS (kernel TLS offload) — see *Why this is the stepping stone to kTLS*

## Goal

Remove one of the two copies TLS currently pays on every send, by encrypting
**directly from caller memory into a send-pool slot** instead of first copying
plaintext into rustls' internal buffer.

The unbuffered path is built **alongside** the existing buffered one. Whether the
buffered path is later feature-gated or deleted is deliberately deferred.

## Motivation

`docs/syscalls-and-copies.md:195` records TLS sends at **2 copies**, versus 1 for
plaintext. `tls.rs::encrypt_to_sends` shows exactly where they are:

```rust
let n = tls_conn.conn.writer().write(&plaintext[offset..])?;  // copy 1
while tls_conn.conn.wants_write() {
    tls_conn.conn.write_tls(&mut writer)?;                    // copy 2
}
```

1. **Copy 1** — user bytes into rustls' internal `sendable_plaintext` buffer.
2. **Copy 2** — rustls encrypts out of that buffer into the pool slot.

Copy 2 is intrinsic: encryption must read plaintext and write ciphertext
somewhere, and rustls already writes straight into the pool slot via the
`PoolWriter` adapter — #254 removed the intermediate scratch buffer that used to
sit there, so this side is already as tight as the buffered API allows.
**Copy 1 is pure buffering overhead** — the plaintext is already sitting in
caller memory, contiguous and valid for the duration of the call. It is the last
one the buffered API cannot remove.

Removing it brings TLS sends to parity with plaintext sends (1 copy).

### Second payoff: guards become usable under TLS

`send_parts().guard()` is refused outright under TLS today, because the buffered
API has no way to encrypt from caller-owned memory. `WriteTraffic::encrypt` takes
`&[u8]`, so a `SendGuard`'s memory can be the plaintext source directly.

This is **not** zero-copy to the wire — ciphertext still lands in a pool slot, and
true zero-copy under TLS needs NIC crypto offload. But it removes the copy of user
data into a gather buffer, and it makes the API stop refusing a reasonable request.
Treated as a **follow-on**, not part of this milestone (see *Out of scope*).

## What rustls gives us

Verified against `rustls` 0.23.41 (the version in `Cargo.lock`).

```rust
impl<Data> WriteTraffic<'_, Data> {
    pub fn encrypt(&mut self, application_data: &[u8], outgoing_tls: &mut [u8])
        -> Result<usize, EncryptError>;
    pub fn queue_close_notify(&mut self, outgoing_tls: &mut [u8])
        -> Result<usize, EncryptError>;
}
```

`encrypt` reads plaintext from a caller slice and writes ciphertext into a caller
buffer, with no intermediate. `queue_close_notify` fits ringline's existing
"close_notify is serialized through the per-connection send queue" rule.

`WriteTraffic` is reachable only from `UnbufferedClientConnection` /
`UnbufferedServerConnection` via `process_tls_records`, which returns:

```rust
pub struct UnbufferedStatus<'c, 'i, Data> {
    pub discard: usize,
    pub state: Result<ConnectionState<'c, 'i, Data>, Error>,
}
```

Eight states: `ReadTraffic`, `ReadEarlyData`, `EncodeTlsData`, `TransmitTlsData`,
`BlockedHandshake`, `WriteTraffic`, `PeerClosed`, `Closed`.

The API carries no `unstable`/experimental markers, and `ConnectionState` is
`#[non_exhaustive]` for forward compatibility. rustls positions it for exactly
this use case.

## The costs we take on

Honest accounting — these are things rustls was doing correctly on our behalf that
become our responsibility.

| # | Cost | Mitigation |
|---|---|---|
| 1 | **Ciphertext buffer compaction.** `discard` bytes must be removed *from the front* of `incoming_tls` before the next `process_tls_records`. A naive `drain(..discard)` is a memmove per call whenever a partial record remains — the O(N·K) shape already fixed once in #279. | Offset-based buffer, compact only on capacity pressure (below). Built and tested as a standalone unit. |
| 2 | **`encrypt` is all-or-nothing on the output buffer.** It fragments >16 KiB internally but needs one contiguous buffer for the whole result, else `InsufficientSizeError { required_size }` and writes nothing. `send_copy_slot_size` defaults to **16384**, and 16 KiB of plaintext exceeds that once header and tag are added. | Chunk plaintext per slot with headroom; drive retry off `required_size` rather than a hardcoded overhead constant. |
| 3 | **`EncryptExhausted` surfaces to us.** Traffic-key exhaustion was internal before. TLS 1.3 sets a rekey-pending flag we must drive; TLS 1.2 force-closes. | Explicit handling; connection-fatal on 1.2, rekey on 1.3. |
| 4 | **Handshake control flow becomes ours.** Eight states instead of `read_tls`/`process_new_packets`/`write_tls`/`wants_write`. Mistakes here are silent interop failures (`bad_record_mac` at the peer), not crashes. | Explicit state-mapping table (below); the buffered path stays in tree as a working reference and A/B target. |
| 5 | **Borrow friction.** `ConnectionState<'c, 'i, Data>` borrows the connection *and* the incoming buffer, while the send pool must be live inside the match arms. | Same split-borrow shape the current code already uses (`encrypt_to_sends(tls_table, send_copy_pool, ...)`), with the ciphertext buffer passed separately. |

No performance downside is claimed — none has been measured, and removing a copy
should dominate. Cost 1 is the one that turns into a *performance* regression if
implemented naively, which is why it gets its own unit.

## Design

### Module split

`tls.rs` is 1181 lines and already carries both backends. Adding a second full
implementation to it is not viable, so it becomes a directory:

```
ringline/src/tls/
  mod.rs           — shared types + public surface (TlsInfo, TlsTable, TlsConn),
                     unchanged externally; dispatches to an engine
  buffered.rs      — today's implementation, moved verbatim
  unbuffered.rs    — new path
  ciphertext.rs    — CiphertextBuf: the incoming_tls buffer + discard/compaction
```

`TlsInfo`, `TlsConfig` and every public item keep their current paths via
re-export. This is a pure move for the buffered code — no behavior change — and
should land as its own commit so the diff for the new path is readable.

### `CiphertextBuf` — the risky primitive, isolated

`process_tls_records` needs a **contiguous** `&mut [u8]` whose front is the next
unprocessed byte, so a wrapping ring is not an option. Instead: a linear buffer
with a start offset, compacted only under capacity pressure.

```rust
pub(crate) struct CiphertextBuf {
    buf: Vec<u8>,
    start: usize,   // first unprocessed byte
    end: usize,     // one past last byte written
}
```

- `pending(&mut self) -> &mut [u8]` → `&mut buf[start..end]`, what gets passed in.
- `discard(&mut self, n)` → `start += n`; `O(1)`, no move. If `start == end`, reset
  both to 0 — the common case once a record is fully consumed.
- `append(&mut self, bytes)` → writes at `end`. If it does not fit, first reclaim
  by compacting (`copy_within(start..end, 0)`) and only then grow.

Compaction cost is amortized `O(1)` per byte: bytes move at most once per pass
through the buffer, the same argument that makes `RecvAccumulator`'s advance
`O(1)`. This is the direct answer to cost 1, and the reason it is a separate
struct with its own tests rather than inline offset arithmetic.

**Growth bound.** The buffer must hold at least one full TLS record
(16 KiB + overhead) or the handshake cannot progress. It is capped; a peer that
sends a record larger than the cap is a protocol error and closes the connection,
matching the `recv_accumulator_max` stance.

### Send path

```rust
pub fn encrypt_to_sends_unbuffered(
    tls: &mut TlsTable,
    pool: &mut SendCopyPool,
    conn_index: u32,
    generation: u32,
    plaintext: &[u8],
) -> io::Result<Vec<BuiltSend>>
```

Per chunk of plaintext:

1. Acquire a pool slot.
2. `encrypt(&plaintext[off..off + chunk], slot)`.
3. On `InsufficientSize { required_size }`, shrink the chunk from `required_size`
   and retry — never hardcode record overhead, which differs between TLS 1.2
   (explicit nonce) and 1.3 (content-type byte).
4. Emit a `BuiltSend` per filled slot.

**The completion contract is unchanged.** Intermediate slots carry
`OpTag::TlsSend`, the final slot carries `OpTag::Send` so it wakes the waiter and
drives the queue — identical to `encrypt_to_sends` today. Nothing about
`docs/send-completion-design.md` changes: no CQE-skip, pool slots still live until
their CQE, sends still serialize through the per-connection queue.

### Recv path — semantics deliberately unchanged

`ReadTraffic` yields plaintext borrowed from `CiphertextBuf`, decrypted in place.
That memory is reused on the next `process_tls_records`, so those slices **must
not** escape.

Plaintext is therefore drained into the `RecvAccumulator` exactly as today.
`with_data`/`with_bytes` semantics, and the `Bytes`-slices-stay-valid-after-advance
contract that `ringline-redis` and `ringline-memcache` depend on, are untouched.
**Recv copy count does not change.**

Handing out borrowed plaintext to avoid the accumulator copy is a real
opportunity, but it is the segmented-recv problem (holdable ⇒ copied), not this
one. Out of scope.

### Handshake state mapping

| `ConnectionState` | Ringline action |
|---|---|
| `EncodeTlsData` | Encode into a pool slot; queue as a send (handshake records go through the per-connection send queue like everything else). |
| `TransmitTlsData` | Records already queued; call `.done()`. `may_encrypt_app_data()` gates early app-data sends. |
| `BlockedHandshake` | Need more ciphertext — return, wait for the next recv completion. |
| `WriteTraffic` | Handshake complete. `TlsInfo` becomes available; fire the existing on-connect path. Steady state for `encrypt`. |
| `ReadTraffic` | Drain plaintext into `RecvAccumulator`, wake the task. |
| `PeerClosed` | Peer sent close_notify. Maps to the existing clean-EOF path; `eof_truncated()` must keep distinguishing this from a mid-message FIN. |
| `Closed` | Terminal — close the connection. |
| `ReadEarlyData` | Not supported. Ringline exposes no 0-RTT API; treated as a protocol error rather than silently ignored. |

### close_notify

`queue_close_notify(outgoing_tls)` encrypts the alert into a pool slot, which is
then queued like any other send. The existing `close_notify_timeout_ms` deadline
and force-close behavior (fixed in #325's batch) apply unchanged.

### Both backends

The uring and mio paths both need the unbuffered engine. mio is not optional here:
it is the only path testable on the maintainer's macOS dev machine, and the
io_uring path cannot even be type-checked there.

The state machine itself is backend-agnostic — it produces "encrypt this into a
buffer" and "these bytes are ready" events. Only the transport differs
(`BuiltSend` + SQE vs. direct `TcpStream` write), mirroring how `encrypt_to_sends`
and `encrypt_for_send_mio` split today.

## Path selection (provisional)

Both engines compile; a connection uses one for its lifetime, chosen at
`TlsTable::create` time.

**Provisional mechanism: a cargo feature `tls-unbuffered`, default off.** It adds
no public config surface, lets CI build and test both, and leaves the eventual
decision cheap — flip the default, or delete `buffered.rs`.

This is explicitly the deferred decision. Alternatives, for when it is taken:

- **Feature flag** (above) — no public surface, but every TLS fix lands twice
  while both exist.
- **Runtime `ConfigBuilder` knob** — lets users fall back without recompiling, but
  permanently doubles the code and makes an implementation detail public. Weakest
  fit with the project's opaque-config / minimal-surface stance.
- **Straight swap, delete buffered** — the end state once the unbuffered path has
  run on the rig.

## Invariants preserved

Checked against CLAUDE.md's *Domain Invariants*:

1. **SQE memory outlives the operation** — ciphertext lives in pool slots held
   until their CQE. Plaintext is read during `encrypt`, before submission, so
   caller memory need not outlive the call.
2. **Ordering** — TLS records still go through the per-connection send queue,
   never as parallel SQEs. Unchanged.
3. **Stale CQEs** — completion handling untouched; same `OpTag`s, same generation
   validation.
4. **No CQE-skip for pool-backed sends** — unchanged.
5. **Short sends** — unchanged; same `BuiltSend` path and `MSG_WAITALL`.

## Testing

- **`CiphertextBuf` unit tests** — partial records, exact fits, remainders,
  compaction under capacity pressure, and an explicit assertion that bytes are
  moved at most once per pass (the anti-#279 test).
- **Chunk sizing** — `InsufficientSize` retry converges; records near
  `send_copy_slot_size`; a plaintext larger than one record.
- **Interop, both directions** — buffered client ↔ unbuffered server and the
  reverse. Streams cannot be byte-identical (record boundaries, nonces), so the
  assertion is interoperability, not equality.
- **Existing TLS integration tests parameterized over both engines**, including
  the 1 MiB multichunk case added in #324.
- **Copy-count verification** — update `docs/syscalls-and-copies.md`; the TLS send
  row moves from 2 to 1. CLAUDE.md's copy table must move with it.

## Out of scope

- **kTLS.** Follow-on, below.
- **Guards under TLS.** Enabled by this work but separate: it touches
  `send_parts`, and the "guards are impossible under TLS" documentation.
- **Reducing recv copies.** That is segmented recv's problem.
- **`sendfile` / NVMe-to-socket.** Needs kTLS first, and NVMe passthrough reads
  into *user* memory, so it does not compose into a zero-copy socket write the way
  `sendfile` does. Needs its own investigation before being counted on.

## Why this is the stepping stone to kTLS

kTLS was the original request; this design is deliberately its first half.

`dangerous_extract_secrets` is implemented on `UnbufferedConnectionCommon`, so the
unbuffered connection is exactly what a future kTLS path needs to hand keys to the
kernel. More importantly, kTLS turns out to be **all-or-nothing per connection**:
`dangerous_extract_secrets(self)` consumes the rustls connection on all three
overloads, so there is no "kTLS for TX, rustls for RX" — extraction destroys the
state machine that would decrypt inbound records.

That makes kTLS strictly larger than it first appears: Linux-only, restricted to
kernel-supported ciphers, requiring the `RecvMsgMulti` + cmsg path (which already
exists for the `timestamps` feature) to read TLS control records, plus KeyUpdate
handling, kernel-version gating, and a fallback for every unsupported case.

This design gets the same send-side copy count, on **both** backends including
macOS, with none of that. kTLS then becomes an opt-in Linux fast path layered on
the same unbuffered connection — and the only route to `sendfile` and NIC crypto
offload, which is where the *remaining* copy goes.

## Open questions

1. **Chunk size policy.** Fill slots to `slot_size - overhead` (fewer, larger
   records) or use a fixed 16 KiB record and a slot large enough to hold it? The
   second is cleaner but changes the `send_pool` default. Wants a measurement.
2. **`CiphertextBuf` capacity default and its config knob** — reuse
   `recv_buffer_size`, or a dedicated setting?
3. **Does the buffered path stay as the mio default** while the unbuffered path
   proves out on the rig, or do both backends switch together?
