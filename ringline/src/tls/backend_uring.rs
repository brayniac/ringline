//! io_uring-facing TLS entry points, dispatching by engine.
//!
//! Each function keeps the name and signature the backend already called, so
//! `backend/uring/event_loop.rs` and `handler.rs` are unaware of which record
//! layer is compiled in. The buffered halves live in [`super::buffered`] under
//! `*_buffered` names — the same split [`super::backend_mio`] uses.
//!
//! Unlike mio, ciphertext here lands in [`SendCopyPool`] slots whose lifetime
//! the completion handlers own, so the contract from
//! `docs/send-completion-design.md` is preserved verbatim: no CQE-skip, slots
//! live until their CQE, and a logical send that spans several slots tags the
//! intermediate ones [`OpTag::TlsSend`] and only the final one [`OpTag::Send`],
//! so exactly one waiter wake happens per logical send. Every send built here
//! is routed through the per-connection send queue by the caller: io_uring does
//! not order independent SQEs, and interleaved TLS records are `bad_record_mac`
//! at the peer.
//!
//! Handshake output takes a scratch `Vec` on the way to those slots.
//! `EncodeTlsData::encode` is all-or-nothing on one contiguous buffer, and a
//! full-size handshake record (`5 + 16384 + overhead`) does not fit the default
//! 16384-byte `send_copy_slot_size` — so the engine writes it to a `Vec` and
//! [`ciphertext_to_sends`] chunks it across slots. Application data — the path
//! this engine exists for — is encrypted directly into a slot and never comes
//! through that scratch.
//!
//! **Never drive the state machine to "flush" leftovers after encrypting or
//! queueing an alert.** rustls' `write_fragments` drains `sendable_tls` into
//! the *front* of the destination buffer ahead of the fragments it is about to
//! encrypt, and `check_required_size` reserves that space (verified against
//! rustls 0.23.41's `CommonState`). Anything already queued — a TLS 1.3
//! `key_update`, most notably — therefore rides out inside the destination,
//! correctly ordered. Driving afterwards and queueing that output as a separate
//! send puts it *after* those records on the wire, and the peer fails with
//! `bad_record_mac`. See `docs/tls-unbuffered-design.md`.

use std::io;

use crate::buffer::send_copy::SendCopyPool;
use crate::handler::BuiltSend;

use super::{PlaintextSink, TlsRecvResult, TlsTable};

#[cfg(not(feature = "tls-unbuffered"))]
use super::buffered;

#[cfg(feature = "tls-unbuffered")]
use super::{build_pool_send, unbuffered};
#[cfg(feature = "tls-unbuffered")]
use crate::completion::OpTag;

/// Copy `ciphertext` into pool slots and append the resulting sends to `out`,
/// all tagged [`OpTag::TlsSend`] — this only ever carries handshake records and
/// alerts, which never complete a logical application send.
///
/// **All-or-nothing.** Slots are staged locally and appended to `out` only once
/// the whole blob has one; running out of pool partway releases everything this
/// call took and appends nothing, returning `false`. Appending a prefix would
/// put a truncated TLS record on the wire — half a Certificate message is
/// strictly worse for the peer than none of it — and the callers tear the
/// connection down on `false` either way. Sends appended to `out` by *earlier*
/// calls are untouched, which is where this matches
/// [`buffered::take_tls_output_sends`][super::buffered::take_tls_output_sends]:
/// it stages into a `PoolWriter` and drains into the caller's vector only on
/// success, for the same reason.
///
/// An empty `ciphertext` appends nothing and succeeds.
#[cfg(feature = "tls-unbuffered")]
pub(super) fn ciphertext_to_sends(
    pool: &mut SendCopyPool,
    conn_index: u32,
    generation: u32,
    ciphertext: &[u8],
    out: &mut Vec<BuiltSend>,
) -> bool {
    let slot_size = pool.slot_size() as usize;
    // (slot, ciphertext length) in transmission order, same staging shape as
    // `encrypt_to_sends` below.
    let mut staged: Vec<(u16, u32)> = Vec::new();
    for chunk in ciphertext.chunks(slot_size) {
        // `copy_in` leaves `slot_end_of_send` at its default of `true`, and
        // that must stay true for every TLS slot. `submit_next_queued`
        // coalesces *consecutive* pool-backed sends into one
        // `SendMsgCoalesced` and does not inspect `OpTag`; the only thing
        // stopping TLS chunks from merging is that the coalescing run breaks
        // at the first end-of-send slot, i.e. immediately. Clearing the flag
        // here — an obvious-looking tidy-up, since these chunks really are
        // parts of one blob — would merge them into a single send and destroy
        // the one-wake-per-logical-send invariant the `TlsSend`/`Send` split
        // exists to hold.
        match pool.copy_in(chunk) {
            Some((slot, _, len)) => staged.push((slot, len)),
            None => {
                release_all(pool, &staged);
                return false;
            }
        }
    }
    for &(slot, len) in &staged {
        let (ptr, _) = pool.current_ptr_remaining(slot);
        out.push(build_pool_send(
            conn_index,
            generation,
            ptr,
            len,
            slot,
            OpTag::TlsSend,
        ));
    }
    true
}

/// Feed received ciphertext into the TLS connection, route decrypted plaintext
/// to `sink`, and append any TLS output (handshake responses, alerts) to
/// `out_sends`.
///
/// The output is appended whatever the outcome — an alert generated on the way
/// to a fatal error still has to reach the peer — and the caller must queue it
/// whatever the return value, or the pool slots leak.
///
/// `sink` selects where decrypted plaintext lands: the recv accumulator (the
/// default `with_data`/`with_bytes` path) or, for a connection in the segmented
/// recv domain, owned segments pushed to its hold (see [`PlaintextSink`]).
pub fn feed_tls_recv(
    tls_table: &mut TlsTable,
    sink: PlaintextSink<'_>,
    send_copy_pool: &mut SendCopyPool,
    conn_index: u32,
    generation: u32,
    ciphertext: &[u8],
    out_sends: &mut Vec<BuiltSend>,
) -> TlsRecvResult {
    #[cfg(feature = "tls-unbuffered")]
    {
        let mut sink = sink;
        let Some(tls_conn) = tls_table.get_mut(conn_index) else {
            return TlsRecvResult::Closed;
        };
        // Handshake records and alerts only; application data is encrypted
        // straight into a slot by `encrypt_to_sends`, never through here.
        let mut scratch = Vec::new();
        let outcome = unbuffered::feed(
            tls_conn,
            Some(&mut sink),
            &mut scratch,
            ciphertext,
            conn_index,
        );
        if !ciphertext_to_sends(send_copy_pool, conn_index, generation, &scratch, out_sends) {
            return TlsRecvResult::Error(rustls::Error::General(
                "send pool exhausted during TLS output flush".into(),
            ));
        }
        match outcome {
            unbuffered::DriveOutcome::Ok => TlsRecvResult::Ok,
            unbuffered::DriveOutcome::HandshakeJustCompleted => {
                TlsRecvResult::HandshakeJustCompleted
            }
            unbuffered::DriveOutcome::Closed => TlsRecvResult::Closed,
            unbuffered::DriveOutcome::Error(e) => TlsRecvResult::Error(e),
        }
    }
    #[cfg(not(feature = "tls-unbuffered"))]
    buffered::feed_tls_recv_buffered(
        tls_table,
        sink,
        send_copy_pool,
        conn_index,
        generation,
        ciphertext,
        out_sends,
    )
}

/// Collect pending TLS output as queueable sends. Used on the client path to
/// push the ClientHello once the connect CQE lands.
///
/// Returns `false` if the output could not be produced or drained in full;
/// sends already appended must still be queued by the caller.
pub fn flush_tls_output(
    tls_table: &mut TlsTable,
    send_copy_pool: &mut SendCopyPool,
    conn_index: u32,
    generation: u32,
    out_sends: &mut Vec<BuiltSend>,
) -> bool {
    #[cfg(feature = "tls-unbuffered")]
    {
        let Some(tls_conn) = tls_table.get_mut(conn_index) else {
            return true;
        };
        let mut scratch = Vec::new();
        // No ciphertext to add: driving an idle machine is what emits the
        // ClientHello (`EncodeTlsData` with an empty incoming buffer). `feed`
        // treats an empty slice as a deliberate flush for exactly this.
        if let unbuffered::DriveOutcome::Error(_) =
            unbuffered::feed(tls_conn, None, &mut scratch, &[], conn_index)
        {
            return false;
        }
        ciphertext_to_sends(send_copy_pool, conn_index, generation, &scratch, out_sends)
    }
    #[cfg(not(feature = "tls-unbuffered"))]
    buffered::flush_tls_output_buffered(
        tls_table,
        send_copy_pool,
        conn_index,
        generation,
        out_sends,
    )
}

/// Encrypt application data into pool-backed sends for the per-connection send
/// queue.
///
/// This is where the copy is removed: `WriteTraffic::encrypt` reads the
/// plaintext out of caller memory and writes the ciphertext straight into the
/// slot, with no bounce through rustls' `sendable_plaintext`.
///
/// On any failure every slot allocated by this call is released before
/// returning, so a rejected send leaks nothing and nothing reaches the send
/// queue. It is *not* retryable, though: records encrypted before the failure
/// consumed TLS sequence numbers that no longer have ciphertext on the wire, so
/// the connection is finished. Treat an error here as fatal for the connection
/// and close it — the same rule `DriverCtx::send` documents for its own
/// mid-buffer failures, and the same behaviour the buffered engine has.
pub fn encrypt_to_sends(
    tls_table: &mut TlsTable,
    send_copy_pool: &mut SendCopyPool,
    conn_index: u32,
    generation: u32,
    plaintext: &[u8],
) -> io::Result<Vec<BuiltSend>> {
    #[cfg(feature = "tls-unbuffered")]
    {
        let tls_conn = tls_table.get_mut(conn_index).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "no TLS state for connection")
        })?;

        // (slot, ciphertext length) in transmission order.
        let mut filled: Vec<(u16, u32)> = Vec::new();
        let mut offset = 0;
        while offset < plaintext.len() {
            // `alloc_raw` defaults `slot_end_of_send` to `true` and it must
            // stay that way even for the intermediate chunks below.
            // `submit_next_queued` coalesces consecutive pool-backed sends
            // without looking at `OpTag`, and only the end-of-send flag breaks
            // the run — so marking intermediates as not-end-of-send would fold
            // a multi-slot TLS send into one SQE and collapse the
            // `TlsSend`/`Send` tagging into a single wake with a wrong count.
            let Some((slot, ptr, cap)) = send_copy_pool.alloc_raw() else {
                release_all(send_copy_pool, &filled);
                return Err(io::Error::other("send copy pool exhausted for TLS"));
            };
            // SAFETY: `ptr`/`cap` are the base and size of the slot `alloc_raw`
            // just handed out; this call holds it exclusively until the
            // `set_filled`/`release` below, and the borrow ends before either.
            let dst = unsafe { std::slice::from_raw_parts_mut(ptr, cap as usize) };
            match unbuffered::encrypt_chunk(tls_conn, &plaintext[offset..], dst) {
                // `used_ct` can exceed this chunk's own records: rustls drains
                // anything it had queued (a TLS 1.3 `key_update`) into the front
                // of `dst`. Transmitting `dst` as-is is what keeps that ordered.
                Ok((used_pt, used_ct)) if used_pt > 0 && used_ct > 0 => {
                    send_copy_pool.set_filled(slot, used_ct as u32);
                    filled.push((slot, used_ct as u32));
                    offset += used_pt;
                }
                Ok(_) => {
                    send_copy_pool.release(slot);
                    release_all(send_copy_pool, &filled);
                    return Err(io::Error::other("TLS encryption made no progress"));
                }
                Err(e) => {
                    send_copy_pool.release(slot);
                    release_all(send_copy_pool, &filled);
                    return Err(e);
                }
            }
        }

        let mut built = Vec::with_capacity(filled.len());
        for (i, &(slot, len)) in filled.iter().enumerate() {
            let (ptr, _) = send_copy_pool.current_ptr_remaining(slot);
            // Final chunk completes the logical send (wakes the waiter, drives
            // the queue via handle_send); intermediates are TLS-internal.
            let tag = if i + 1 == filled.len() {
                OpTag::Send
            } else {
                OpTag::TlsSend
            };
            built.push(build_pool_send(conn_index, generation, ptr, len, slot, tag));
        }
        Ok(built)
    }
    #[cfg(not(feature = "tls-unbuffered"))]
    buffered::encrypt_to_sends_buffered(
        tls_table,
        send_copy_pool,
        conn_index,
        generation,
        plaintext,
    )
}

/// Return every slot in `filled` to the pool. Error paths only: a slot that
/// never becomes an SQE has no CQE coming to release it.
#[cfg(feature = "tls-unbuffered")]
fn release_all(pool: &mut SendCopyPool, filled: &[(u16, u32)]) {
    for &(slot, _) in filled {
        pool.release(slot);
    }
}

#[cfg(all(test, feature = "tls-unbuffered"))]
mod tests {
    use super::*;

    // Pool exhaustion partway through a blob must append nothing and leak
    // nothing. A prefix reaching the wire is a truncated TLS record at the
    // peer, and staged slots have no CQE coming to release them.
    #[test]
    fn ciphertext_to_sends_is_all_or_nothing_on_exhaustion() {
        let mut pool = SendCopyPool::new(2, 64);
        let mut out = Vec::new();
        // Three slots' worth of output into a two-slot pool: the third
        // `copy_in` is the one that fails.
        let ciphertext = vec![0xA5u8; 3 * 64];

        assert!(
            !ciphertext_to_sends(&mut pool, 0, 0, &ciphertext, &mut out),
            "exhaustion must be reported to the caller"
        );
        assert!(
            out.is_empty(),
            "a partial blob must not reach the send queue"
        );
        assert_eq!(
            pool.free_count(),
            2,
            "every slot this call took must be released"
        );
    }

    // Output already in `out` from an earlier call is the caller's, and a
    // later failure must not disturb it.
    #[test]
    fn ciphertext_to_sends_leaves_earlier_sends_alone() {
        let mut pool = SendCopyPool::new(3, 64);
        let mut out = Vec::new();

        assert!(ciphertext_to_sends(&mut pool, 0, 0, &[7u8; 16], &mut out));
        assert_eq!(out.len(), 1);

        // Two slots left, three needed.
        assert!(!ciphertext_to_sends(
            &mut pool,
            0,
            0,
            &[9u8; 3 * 64],
            &mut out
        ));
        assert_eq!(out.len(), 1, "the earlier call's send must survive");
        assert_eq!(pool.free_count(), 2, "only the first call still holds one");
    }

    // The success path splits across slots in order and appends one send each.
    #[test]
    fn ciphertext_to_sends_chunks_across_slots() {
        let mut pool = SendCopyPool::new(4, 64);
        let mut out = Vec::new();
        let ciphertext = vec![0x5Au8; 130];

        assert!(ciphertext_to_sends(&mut pool, 0, 0, &ciphertext, &mut out));
        assert_eq!(out.len(), 3, "130 bytes over 64-byte slots is 64 + 64 + 2");
        assert_eq!(
            out.iter().map(|b| b.total_len).sum::<u32>(),
            130,
            "every byte must be accounted for exactly once"
        );
        assert_eq!(out[2].total_len, 2, "the tail slot carries the remainder");
        assert_eq!(pool.free_count(), 1);
    }

    // An empty blob is a no-op, not a failure: `feed`/`flush` call this
    // unconditionally and the steady state has no handshake output at all.
    #[test]
    fn ciphertext_to_sends_accepts_an_empty_blob() {
        let mut pool = SendCopyPool::new(2, 64);
        let mut out = Vec::new();
        assert!(ciphertext_to_sends(&mut pool, 0, 0, &[], &mut out));
        assert!(out.is_empty());
        assert_eq!(pool.free_count(), 2);
    }
}
