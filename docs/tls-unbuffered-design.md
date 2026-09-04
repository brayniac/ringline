# Unbuffered TLS send path — design

**Status:** Implemented behind the default-off `tls-unbuffered` feature (PRs
#338, #341, #345, and this engine). Interop testing and the chunk-size rig
sweep remain. The `> **Correction**` blocks below record where building it
falsified the design; read those before extending it.
**Date:** 2026-09-03 (corrections 2026-09-04)
**Related:** [`docs/syscalls-and-copies.md`](syscalls-and-copies.md), [`docs/send-completion-design.md`](send-completion-design.md), `ringline/src/tls/`
**Follow-on:** kTLS (kernel TLS offload) — see *Why this is the stepping stone to kTLS*

## Goal

Remove one of the two copies TLS currently pays on every send, by encrypting
**directly from caller memory into a send-pool slot** instead of first copying
plaintext into rustls' internal buffer.

The unbuffered path is built **alongside** the existing buffered one. Whether the
buffered path is later feature-gated or deleted is deliberately deferred.

## Design target: 800 GbE and beyond

Every trade-off below is sized against **800 GbE**, not today's rigs.

**What is structural.** The copy count on the TLS send path follows from the
mechanism and can be verified by reading the code:

| TLS send path | Copies on send |
|---|---|
| Today (buffered) | 2 — user → rustls' plaintext buffer → pool slot |
| Unbuffered (this design) | 1 — user → pool slot, encrypting in transit |
| kTLS, software | 1 — `sendmsg` copies plaintext in; kernel encrypts in place |
| kTLS + NIC offload (`TLS_HW`) | 0 — in principle; unmeasured here |

**What is not measured.** How much a copy *costs* at 800 GbE is unknown. The one
number this codebase has is **~3.2 GiB/s/core**, from real 200 GbE on
Graviton4 / kernel 6.12 ([`segmented-recv-design.md`](segmented-recv-design.md)).
Dividing 800 GbE's ~93 GiB/s by it suggests one copy of a saturated stream is on
the order of tens of cores — but that is a linear extrapolation onto hardware
that does not exist yet, and it assumes per-core copy bandwidth holds at the new
line rate, that copies parallelize cleanly across cores, and that copying stays
the binding constraint rather than per-CQE processing, NIC queueing, or something
unforeseen. **Treat it as a reason to measure, not as a projection**, and do not
quote a core count as if it were a result. Where numbers appear below they are
order-of-magnitude framing for prioritisation.

Three consequences follow from the *structural* column, which is the part that
does not depend on the extrapolation:

1. **The value of removing a copy grows with line rate.** Trivially true given a
   fixed per-core copy bandwidth; the interesting question is only how much,
   which is unmeasured.
2. **Software kTLS does not remove the remaining copy.** `sendmsg` copies
   plaintext into the kernel, which encrypts in place — the same count as the
   unbuffered path. Only NIC crypto offload can reach zero, and that is
   unmeasured here too. See *Why this is the stepping stone to kTLS*.
3. **Per-record overhead is a real term.** A saturated 800 GbE link carries on
   the order of millions of TLS records per second (16 KiB max record), each
   costing one `WriteTraffic::encrypt` call with `maybe_refresh_traffic_keys` and
   fragmenter setup. `write_plaintext` fragments *internally*, so one call with
   more plaintext emits several records and amortizes that. This argues for
   larger plaintext chunks per call, hence larger contiguous output buffers
   (`encrypt` is all-or-nothing on its output). It is **not** a licence to raise
   `send_copy_slot_size` — the direction is argued, the value is not. Treat chunk
   size as an internal knob and **measure on the rig** before touching any
   `Config` default.

A fourth consequence is a rule rather than a number, and it does not rest on the
extrapolation at all: at high connection counts, **do not zero memory the peer
has not sent**. The adaptive-recv journal's finding that big provided buffers are
RSS-free relies on demand paging over *untouched* pages; anything that `memset`s
forfeits it. `CiphertextBuf` must grow by appending received bytes, never by
pre-filling.

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

> **Correction (PR #338).** The split as built is by *file size*, not cleanly by
> engine, and `mod.rs` is **not** "shared types" as written above. Roughly 230 of
> its 413 lines are bound to rustls' buffered API: `TlsConnKind` is an enum over
> `ClientConnection`/`ServerConnection` and its 13 methods (`read_tls`,
> `write_tls`, `process_new_packets`, `reader`, `writer`) *are* that API;
> `TlsConn` and `TlsTable` are built on it; `drain_tls_plaintext` drives
> `reader().fill_buf()`. The unbuffered API has no `reader()`, `writer()` or
> `read_tls()`.
>
> Only `TlsInfo`, `TlsRecvResult`, `PlaintextSink` and `build_pool_send` are
> genuinely engine-agnostic. **`unbuffered.rs` therefore cannot simply be dropped
> in alongside `buffered.rs`**: an engine dimension has to be threaded through
> `TlsConnKind`/`TlsConn`/`TlsTable` first, and an unbuffered counterpart to
> `drain_tls_plaintext` written. That is unbudgeted work this design missed, and
> it is the first thing the engine plan must address.
>
> One consequence already visible: `TlsTable::send_close_notify_queued` lives on
> a `mod.rs` type but calls into the buffered engine, which is what forced
> `take_tls_output_sends` to `pub(super)`.

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

> **Correction (PR #338).** The sketch above is right in shape and wrong in three
> specifics. Isolating this primitive was worth it: three rounds of adversarial
> review were needed to get it correct, and two of the bugs were remotely
> triggerable.
>
> **The amortization argument needs a different guard.** "Bytes move at most once
> per pass" does not follow from compacting on capacity pressure. The rule that
> works is *compaction must pay for itself*: run it only when `start >= live`, so
> bytes moved never exceed bytes reclaimed, and reclaimed bytes were already
> discarded. An earlier revision guarded only one of two compaction sites and hit
> **16383× amplification** at a 1 MiB cap once the buffer was backed up — the
> O(N·K) shape of #279, reachable for free by any peer that outruns the reader.
> Compaction is also tried *before* growth: growing first keeps resident memory
> proportional to `cap` rather than to the working set (measured 64× worse).
>
> **A full buffer returns `WouldBlock`, and the engine must handle it.** New
> contract this design did not anticipate. `append` is all-or-nothing, so a
> caller that gets `WouldBlock` must retain the chunk and retry after draining,
> must not treat it as fatal, and must not spin — the state can persist for many
> appends. `ErrorKind::InvalidData` is the separate, fatal "cannot ever fit"
> case.
>
> **The cap floor is much larger than one record**, and getting it wrong
> deadlocks. `WouldBlock` implies `live > (cap − additional)/2`, so it is only
> safe if `cap/2` exceeds the largest live set rustls can leave *unprocessable* —
> and rustls' unbuffered path joins handshake messages spanning records inside
> the caller's buffer up to `MAX_HANDSHAKE_SIZE` (0xffff), returning
> `discard == 0` until complete. The floor is therefore
> `2·(0xffff + MAX_TLS_WIRE_RECORD) + MAX_SINGLE_APPEND` ≈ 228 KiB, not one
> record. Note also that `MAX_TLS_WIRE_RECORD` is rustls' `MAX_WIRE_SIZE`
> (`5 + 16384 + 2048`), not RFC 8446's smaller `2^14 + 256`, because the crate
> enables `tls12`.
>
> **`MAX_SINGLE_APPEND` is a cross-module coupling.** That floor's derivation
> assumes no single `append` exceeds 64 KiB. `append` `debug_assert`s it, but the
> engine's recv wiring must size its buffer accordingly — a constraint on the
> send-path work that originates here.

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

> **Correction (this engine).** Three things above did not survive building it.
>
> **`EncodeTlsData` does not "encode into a pool slot" on io_uring.**
> `EncodeTlsData::encode` needs one contiguous `&mut [u8]` and takes no
> `io::Write`, so a full-size handshake record can outgrow a single
> `send_copy_slot_size` slot before the code even knows how many slots it will
> need. Handshake output is encoded into a scratch `Vec` instead, then chunked
> across as many pool slots as it takes — one copy more than this row implies,
> and one more than the buffered engine's `PoolWriter` pays for the same
> record. Application data does not go through this: `WriteTraffic::encrypt`
> (steady state, the `ReadTraffic`/`WriteTraffic` rows) still writes straight
> into the slot. mio has no pool-slot step for either engine, so it is
> unaffected. See `docs/journal/2026-09-unbuffered-tls.md`, "Plan 3".
>
> **This table's per-state actions cannot be one non-generic function.**
> `process_tls_records` — the call that produces every `ConnectionState` row
> above — is implemented separately on `UnbufferedClientConnection` and
> `UnbufferedServerConnection`, and the shared body is private, so
> `ConnectionState<'_, '_, Data>` differs between the two. The engine needed a
> private `UnbufferedEngine` trait with an associated `Data` type, dispatched
> once per `drive()` call, not a single match over both connection kinds.
>
> **Retrying a sizing failure by re-entering `process_tls_records` is unsound**,
> not just awkward as cost 2 in "The costs we take on" implies. `write_plaintext`
> (`perhaps_write_key_update`) and `eager_send_close_notify`
> (`send_close_notify`) both queue into `sendable_tls` *before* the caller can
> even check the required size, and the next `process_tls_records` call pops
> `sendable_tls` into the *next* `EncodeTlsData` rather than handing back the
> same `WriteTraffic` — so "get `InsufficientSize`, re-enter, try again" lands
> in the wrong state and drops what was queued. The close_notify case failed
> silently this way: the alert stranded inside rustls, `close_notify_sent` was
> never set, and the connection closed as a truncation instead of cleanly. The
> engine now enters the state machine once per call and holds `WriteTraffic`
> across the whole retry loop. The mirror-image mistake — driving the machine
> again *after* a successful `encrypt`/`queue_close_notify` to flush anything
> still queued — reorders a pending TLS 1.3 `key_update` to *after* the record
> it should have preceded, which is `bad_record_mac` at the peer; both
> `backend_uring.rs` and `backend_mio.rs` carry a top-of-file warning against
> it.

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
- **Reducing recv copies.** That is segmented recv's problem — and the two do
  **not** compose, which is worth stating so it is not rediscovered.

  Segmented recv ([`segmented-recv-design.md`](segmented-recv-design.md), design
  only, unimplemented) exists to avoid the copy into the contiguous
  `RecvAccumulator` by delivering *discontiguous, buffer-sized segments*. But
  `process_tls_records` takes a `&mut [u8]` whose front is the next unprocessed
  byte and decrypts **in place**, so ciphertext ingest is contiguity-bound. That
  is precisely why `CiphertextBuf` is a linear buffer with a start offset rather
  than a ring.

  So segmented recv's zero-copy Modes A and B cannot feed rustls: TLS ciphertext
  must be gathered contiguously regardless, and TLS recv keeps paying that copy
  whatever segmented recv does. The two efforts are on different axes — this one
  is send-side only.

  The one place they touch is `CiphertextBuf::MAX_SINGLE_APPEND` (64 KiB), which
  the cap floor's no-deadlock argument depends on. A future segmented or
  incremental-buffer (`IOU_PBUF_RING_INC`) recv path that coalesced into larger
  chunks would violate it, and would have to either chunk its appends or raise
  the floor in step. Note `IOU_PBUF_RING_INC` is **not** implemented today, and
  self-adaptive recv buffering was measured **NO-GO** (2026-07-21) — see
  `docs/journal/2026-07-self-adaptive-recv-buffering.md`, whose re-evaluation
  trigger A is *"faster NICs (400/800 GbE)"*, i.e. exactly this design's target.
  If that trigger fires, revisit this paragraph before the recv geometry changes.
- **`sendfile` / NVMe-to-socket.** Needs kTLS first, and NVMe passthrough reads
  into *user* memory, so it does not compose into a zero-copy socket write the way
  `sendfile` does. Needs its own investigation before being counted on.

## Why this is the stepping stone to kTLS

kTLS was the original request; this design is deliberately its first half — and
at the 800 GbE target it is the *necessary* first half of a two-part answer, not
a detour.

**The remaining copy is a floor that only hardware offload removes.** This
design takes TLS sends from 2 copies to 1, reaching parity with plaintext sends.
It cannot go further: encryption must read plaintext and write ciphertext, and no
userspace arrangement removes that. Software kTLS does not help — `sendmsg`
copies plaintext into the kernel, which encrypts in place, so the count is
unchanged. **Only NIC crypto offload (`TLS_HW`) can reach zero copies**, and
reaching it requires kTLS as the delivery mechanism. That is the structural case
for kTLS; `sendfile` is a secondary benefit, not the motivation.

How much that floor actually costs at 800 GbE is unmeasured — see *Design
target*. The argument here is about the copy count, which is structural, not
about a core figure.

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

1. **Chunk size policy — resolved in direction, open in value.** Because
   `write_plaintext` fragments internally, passing more plaintext per `encrypt`
   call amortizes the per-call overhead across several records, which matters at
   the millions of records per second a saturated 800 GbE link implies. So the
   direction is *larger* chunks, bounded by `encrypt` needing its whole
   ciphertext output contiguous.

   The value is **not** settled and must not be guessed: implement chunk size as
   an internal knob, sweep it (16 K / 64 K / 256 K plaintext per call) on the
   rig, and report records/s/core and cycles/record before proposing any change
   to `send_copy_slot_size`. The 800 GbE figures in this document are a
   projection from a 200 GbE measurement; they justify measuring, nothing more.
   If a larger slot does win, prefer giving TLS its own slot class over resizing
   the shared `SendCopyPool` and charging non-TLS users for it.
2. **`CiphertextBuf` capacity default and its config knob** — reuse
   `recv_buffer_size`, or a dedicated setting?

   **Resolved provisionally:** no new config knob. Every connection gets
   `CiphertextBuf::new(INITIAL_SHRINK_TO, MIN_CIPHERTEXT_CAP)`. Revisit if the
   rig sweep shows the cap binding.
3. **Does the buffered path stay as the mio default** while the unbuffered path
   proves out on the rig, or do both backends switch together?
