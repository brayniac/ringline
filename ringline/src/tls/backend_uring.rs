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
/// An empty `ciphertext` appends nothing and succeeds. Returns `false` on pool
/// exhaustion; sends already appended must still be queued by the caller or
/// their slots leak (the same contract
/// [`buffered::take_tls_output_sends`][super::buffered::take_tls_output_sends]
/// carries).
#[cfg(feature = "tls-unbuffered")]
fn ciphertext_to_sends(
    pool: &mut SendCopyPool,
    conn_index: u32,
    generation: u32,
    ciphertext: &[u8],
    out: &mut Vec<BuiltSend>,
) -> bool {
    let slot_size = pool.slot_size() as usize;
    for chunk in ciphertext.chunks(slot_size) {
        match pool.copy_in(chunk) {
            Some((slot, ptr, len)) => out.push(build_pool_send(
                conn_index,
                generation,
                ptr,
                len,
                slot,
                OpTag::TlsSend,
            )),
            None => return false,
        }
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
