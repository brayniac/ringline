use std::sync::Arc;

use rustls::pki_types::ServerName;

use super::{DriveOutcome, MAX_SINGLE_APPEND, UnbufferedConn, drive, feed, queue_close_notify};
use crate::accumulator::AccumulatorTable;
use crate::tls::{PlaintextSink, TlsConn, TlsConnKind};

fn empty_client_config() -> Arc<rustls::ClientConfig> {
    rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth()
        .into()
}

fn client_conn() -> UnbufferedConn {
    let server_name: ServerName<'static> = "localhost".try_into().unwrap();
    UnbufferedConn::new_client(empty_client_config(), server_name)
        .expect("constructing an unbuffered client connection does not drive the handshake")
}

// A connection built on the unbuffered engine reports itself as such:
// `as_buffered_mut` returns `None` rather than panicking or silently
// handing back a buffered view.
#[test]
fn unbuffered_connection_is_not_buffered() {
    let mut tls_conn = TlsConn {
        conn: TlsConnKind::Unbuffered(client_conn()),
        handshake_complete: false,
        peer_sent_close_notify: false,
        close_notify_sent: false,
    };
    assert!(tls_conn.conn.as_buffered_mut().is_none());
    assert!(tls_conn.conn.as_unbuffered_mut().is_some());
}

// A fresh connection starts with an empty ciphertext buffer and no
// deferred plaintext; `split_mut` hands back all three parts disjointly.
#[test]
fn fresh_conn_has_empty_buffers() {
    let mut conn = client_conn();
    let (_kind, incoming, pending) = conn.split_mut();
    assert!(incoming.is_empty());
    assert!(pending.is_empty());
}

fn test_certs() -> (
    Vec<rustls::pki_types::CertificateDer<'static>>,
    rustls::pki_types::PrivateKeyDer<'static>,
) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    let cert_der = rustls::pki_types::CertificateDer::from(cert.cert);
    (vec![cert_der], key.into())
}

fn conn_pair() -> (TlsConn, TlsConn) {
    let (certs, key) = test_certs();
    let server_config = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs.clone(), key)
            .unwrap(),
    );
    let mut roots = rustls::RootCertStore::empty();
    for c in &certs {
        roots.add(c.clone()).unwrap();
    }
    let client_config: Arc<rustls::ClientConfig> = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth()
        .into();
    let name: ServerName<'static> = "localhost".try_into().unwrap();

    let wrap = |c| TlsConn {
        conn: TlsConnKind::Unbuffered(c),
        handshake_complete: false,
        peer_sent_close_notify: false,
        close_notify_sent: false,
    };
    (
        wrap(UnbufferedConn::new_server(server_config).unwrap()),
        wrap(UnbufferedConn::new_client(client_config, name).unwrap()),
    )
}

/// Push `bytes` into `to`'s ciphertext buffer and drive it, collecting its
/// own output. Returns (outcome, output ciphertext).
fn pump(to: &mut TlsConn, bytes: &[u8], accs: &mut AccumulatorTable) -> (DriveOutcome, Vec<u8>) {
    if !bytes.is_empty() {
        let (_, incoming, _) = to.conn.as_unbuffered_mut().unwrap().split_mut();
        incoming
            .append(bytes)
            .expect("test appends stay under the cap");
    }
    let mut out = Vec::new();
    let mut sink = PlaintextSink::Accumulator(accs);
    let outcome = drive(to, Some(&mut sink), &mut out, 0);
    (outcome, out)
}

/// Run both sides to a completed handshake by feeding each one's output to
/// the other, using `drive()` alone. Returns how many times each side
/// reported `HandshakeJustCompleted`, as (client, server).
fn handshake(
    server: &mut TlsConn,
    client: &mut TlsConn,
    accs: &mut AccumulatorTable,
) -> (u32, u32) {
    let mut client_completions = 0;
    let mut server_completions = 0;

    // Client speaks first (ClientHello) with no input.
    let (outcome, mut to_server) = pump(client, &[], accs);
    if matches!(outcome, DriveOutcome::HandshakeJustCompleted) {
        client_completions += 1;
    }
    assert!(!to_server.is_empty(), "client must emit a ClientHello");

    let mut to_client = Vec::new();
    for _ in 0..10 {
        if !to_server.is_empty() {
            let (o, out) = pump(server, &to_server, accs);
            assert!(!matches!(o, DriveOutcome::Error(_)), "server drive: {o:?}");
            if matches!(o, DriveOutcome::HandshakeJustCompleted) {
                server_completions += 1;
            }
            to_server.clear();
            to_client = out;
        }
        if !to_client.is_empty() {
            let (o, out) = pump(client, &to_client, accs);
            assert!(!matches!(o, DriveOutcome::Error(_)), "client drive: {o:?}");
            if matches!(o, DriveOutcome::HandshakeJustCompleted) {
                client_completions += 1;
            }
            to_client.clear();
            to_server = out;
        }
        if !server.conn.is_handshaking() && !client.conn.is_handshaking() {
            break;
        }
    }

    assert!(!client.conn.is_handshaking(), "client handshake stalled");
    assert!(!server.conn.is_handshaking(), "server handshake stalled");
    (client_completions, server_completions)
}

// A full handshake completes when each side's output is fed to the other
// and both are driven by `drive()` alone. Both report
// HandshakeJustCompleted exactly once.
#[test]
fn drive_completes_a_handshake() {
    let (mut server, mut client) = conn_pair();
    let mut accs = AccumulatorTable::new(2, 4096);

    let (client_completions, server_completions) = handshake(&mut server, &mut client, &mut accs);

    assert!(client.handshake_complete);
    assert!(server.handshake_complete);
    assert_eq!(client_completions, 1, "completion must be edge-triggered");
    assert_eq!(server_completions, 1, "completion must be edge-triggered");
}

// A plaintext flood that overruns the accumulator's bound must fail the
// connection, not silently drop bytes: the caller's contract for
// `DriveOutcome::Error` is "tear this connection down". Mirrors
// `drain_tls_plaintext`'s `false` return in the buffered engine.
#[test]
fn plaintext_over_the_sink_bound_fails_the_connection() {
    let (mut server, mut client) = conn_pair();
    // Bound the accumulator well below the payload.
    let mut accs = AccumulatorTable::new_with_max(2, 1024, 4096);
    handshake(&mut server, &mut client, &mut accs);
    accs.reset(0);

    // Encrypt more application data than the accumulator will accept.
    // 32 KiB (three records once framed) rather than more: the whole
    // ciphertext goes in through one `CiphertextBuf::append`, which
    // debug-asserts a `MAX_SINGLE_APPEND` (64 KiB) bound.
    let plaintext = vec![0x7Eu8; 32 * 1024];
    let cipher = encrypt_all(&mut client, &plaintext);

    let (_, incoming, _) = server.conn.as_unbuffered_mut().unwrap().split_mut();
    incoming
        .append(&cipher)
        .expect("test append stays under the cap");
    let mut out = Vec::new();
    let mut sink = PlaintextSink::Accumulator(&mut accs);
    let outcome = drive(&mut server, Some(&mut sink), &mut out, 0);
    assert!(
        matches!(outcome, DriveOutcome::Error(_)),
        "over-bound plaintext must fail the connection, got {outcome:?}"
    );
}

// Handshake ciphertext delivered in one-byte pieces drives the machine
// without erroring: each partial record leaves it BlockedHandshake, and
// the byte that completes the ClientHello produces the response. This is
// the ingest path's basic contract — `feed` must never treat "not enough
// yet" as failure.
#[test]
fn feed_handshake_bytes_in_small_pieces() {
    let (mut server, mut client) = conn_pair();
    let mut accs = AccumulatorTable::new(2, 4096);
    let (_, hello) = pump(&mut client, &[], &mut accs);

    let mut out = Vec::new();
    let mut sink = PlaintextSink::Accumulator(&mut accs);
    for b in &hello {
        let outcome = feed(&mut server, Some(&mut sink), &mut out, &[*b], 0);
        assert!(!matches!(outcome, DriveOutcome::Error(_)), "{outcome:?}");
    }
    assert!(!out.is_empty(), "server must answer a complete ClientHello");
}

// A single `feed` call larger than MAX_SINGLE_APPEND is chunked rather
// than tripping `append`'s debug_assert. `ConfigBuilder::recv_buffer`
// accepts buffer sizes above 64 KiB, so this is reachable from public
// config; the assert is silent in release.
#[test]
fn feed_chunks_oversized_input() {
    let (mut server, _client) = conn_pair();
    let mut accs = AccumulatorTable::new(2, 4096);

    // Garbage, but it must be *appended* in chunks before rustls rejects
    // it — the assertion is that we get a clean protocol error rather
    // than a debug_assert panic.
    let junk = vec![0u8; MAX_SINGLE_APPEND * 2 + 7];
    let mut out = Vec::new();
    let mut sink = PlaintextSink::Accumulator(&mut accs);
    let outcome = feed(&mut server, Some(&mut sink), &mut out, &junk, 0);
    assert!(
        matches!(outcome, DriveOutcome::Error(_)),
        "unparseable ciphertext must be a protocol error, got {outcome:?}"
    );
}

/// Drive the handshake but hold back the client's final flight, returning
/// it undelivered. Lets a test put that flight and post-handshake records
/// into one `feed` — which is what a peer does by coalescing them onto a
/// single segment.
fn handshake_holding_client_flight(
    server: &mut TlsConn,
    client: &mut TlsConn,
    accs: &mut AccumulatorTable,
) -> Vec<u8> {
    let (_, mut to_server) = pump(client, &[], accs);
    for _ in 0..10 {
        let (_, to_client) = pump(server, &to_server, accs);
        assert!(!to_client.is_empty(), "server must answer");
        let (_, out) = pump(client, &to_client, accs);
        to_server = out;
        if !client.conn.is_handshaking() {
            break;
        }
    }
    assert!(!client.conn.is_handshaking(), "client handshake stalled");
    assert!(
        server.conn.is_handshaking(),
        "the server must still be waiting on the flight held back"
    );
    assert!(!to_server.is_empty(), "client must emit a final flight");
    to_server
}

/// Encrypt `plaintext` on `from`, returning all of its ciphertext, through
/// the same entry point the mio backend uses.
fn encrypt_all(from: &mut TlsConn, plaintext: &[u8]) -> Vec<u8> {
    let mut cipher = Vec::new();
    super::encrypt_to_vec(from, plaintext, &mut cipher).expect("encrypt");
    cipher
}

// A feed that both completes the handshake and sees close_notify must
// report the completion: the backends wake a connect waiter only on
// `HandshakeJustCompleted` — the `Closed` arm does not, and
// `close_connection` does not either — so losing that edge hangs an
// outbound TLS connect until its timeout. The close is not lost:
// `peer_sent_close_notify` still carries it.
//
// The two signals must land in *different* chunks for this to exercise
// `fold_outcome`: one chunk is one `drive`, and `drive` already applies
// the precedence itself. Hence the application data padding the flight
// past `MAX_SINGLE_APPEND` — which is also how a real peer would produce
// it, by coalescing its last flight, some data and the alert.
#[test]
fn handshake_completion_outranks_a_close_in_the_same_feed() {
    let (mut server, mut client) = conn_pair();
    let mut accs = AccumulatorTable::new(2, 4096);
    let mut coalesced = handshake_holding_client_flight(&mut server, &mut client, &mut accs);

    let record = vec![0x5Au8; 8 * 1024];
    while coalesced.len() <= MAX_SINGLE_APPEND {
        coalesced.extend_from_slice(&encrypt_all(&mut client, &record));
    }
    let mut alert = Vec::new();
    queue_close_notify(&mut client, &mut alert).expect("queue close_notify");
    assert!(!alert.is_empty(), "the close must be a real alert");
    coalesced.extend_from_slice(&alert);
    assert!(
        coalesced.len() > MAX_SINGLE_APPEND,
        "the alert must fall in a later chunk than the flight"
    );

    let mut out = Vec::new();
    let mut sink = PlaintextSink::Accumulator(&mut accs);
    let outcome = feed(&mut server, Some(&mut sink), &mut out, &coalesced, 0);
    assert!(
        matches!(outcome, DriveOutcome::HandshakeJustCompleted),
        "completion must outrank the close, got {outcome:?}"
    );
    assert!(
        server.peer_sent_close_notify,
        "the close must still be recorded on the connection"
    );
}

// A queued close_notify is a real encrypted alert: the peer sees it as a
// clean close (PeerClosed -> Closed), not as a truncation.
#[test]
fn close_notify_is_seen_as_a_clean_close() {
    let (mut server, mut client) = conn_pair();
    let mut accs = AccumulatorTable::new(2, 1024 * 1024);
    handshake(&mut server, &mut client, &mut accs);
    accs.reset(0);

    let mut alert = Vec::new();
    queue_close_notify(&mut client, &mut alert).expect("queue close_notify");
    assert!(!alert.is_empty(), "close_notify must produce ciphertext");
    assert!(client.close_notify_sent);

    let mut out = Vec::new();
    let mut sink = PlaintextSink::Accumulator(&mut accs);
    let outcome = feed(&mut server, Some(&mut sink), &mut out, &alert, 0);
    assert!(matches!(outcome, DriveOutcome::Closed), "got {outcome:?}");
    assert!(
        server.peer_sent_close_notify,
        "eof_truncated() depends on this flag being set"
    );
}

// Closing a connection that never reached traffic state is a clean no-op,
// not an error: there is no alert to encrypt, so nothing is queued and the
// close-notify deadline is not armed for a record that was never sent. The
// teardown path calls this unconditionally, including on connections that
// died mid-handshake.
#[test]
fn close_notify_on_a_handshaking_connection_queues_nothing() {
    let (mut server, _client) = conn_pair();
    let mut out = Vec::new();
    queue_close_notify(&mut server, &mut out).expect("a mid-handshake close is not an error");
    assert!(
        out.is_empty(),
        "there is nothing to queue before traffic state"
    );
    assert!(
        !server.close_notify_sent,
        "arming the close deadline for an unsent alert invents a stall"
    );
}

// An empty feed drives the machine rather than doing nothing. A fresh
// client has a ClientHello queued with no ciphertext to deframe, and the
// backends flush it exactly this way once the TCP connect completes — so
// if `feed` short-circuited on an empty slice, outbound TLS would never
// send its first flight.
#[test]
fn an_empty_feed_still_drives_the_machine() {
    let (_server, mut client) = conn_pair();
    let mut accs = AccumulatorTable::new(2, 4096);
    let mut out = Vec::new();
    let mut sink = PlaintextSink::Accumulator(&mut accs);

    let outcome = feed(&mut client, Some(&mut sink), &mut out, &[], 0);

    assert!(matches!(outcome, DriveOutcome::Ok), "got {outcome:?}");
    assert!(
        !out.is_empty(),
        "an empty feed must still emit the ClientHello"
    );
}

// A `WouldBlock` that a drive can relieve is backpressure, not failure:
// `feed` drains, retries the *same* chunk, and delivers every byte in
// order. `append` is all-or-nothing, so a caller that dropped the chunk
// here would silently lose a whole record.
//
// The buffer is put into the refusing state directly — an unprocessed but
// complete record sitting behind a consumed prefix — because `feed` alone
// cannot produce it: every append it makes is followed by a drive that
// drains everything drainable. The branch exists so a `drive` that ever
// stops short is backpressure rather than data loss.
#[test]
fn feed_retries_an_append_a_drive_can_relieve() {
    let (mut server, mut client) = conn_pair();
    let mut accs = AccumulatorTable::new(2, 4096);
    handshake(&mut server, &mut client, &mut accs);

    // Encrypted in the order they are fed; TLS records are sequenced.
    let marker = encrypt_all(&mut client, &[0xA1u8; 16]);
    let held = encrypt_all(&mut client, &[0xB2u8; 2000]);
    let next = encrypt_all(&mut client, &[0xC3u8; 2000]);

    // `cap == live + additional` is the tightest cap that still admits the
    // append on retry, so `append` must refuse it while `marker`'s
    // consumed prefix keeps `end` ahead of `live`.
    let cap = held.len() + next.len();
    assert!(
        marker.len() < held.len(),
        "compaction must not pay for itself"
    );
    server
        .conn
        .as_unbuffered_mut()
        .unwrap()
        .set_ciphertext_cap_for_test(512, cap);

    // Consume `marker` so `start > 0`, leaving `held` half-delivered.
    let split = held.len() / 2;
    let mut prefix = marker.clone();
    prefix.extend_from_slice(&held[..split]);
    let mut out = Vec::new();
    {
        let mut sink = PlaintextSink::Accumulator(&mut accs);
        let outcome = feed(&mut server, Some(&mut sink), &mut out, &prefix, 0);
        assert!(matches!(outcome, DriveOutcome::Ok), "{outcome:?}");
    }

    // Complete `held` without driving, so the refusing buffer is full of
    // data a drive *can* consume.
    {
        let (_, incoming, _) = server.conn.as_unbuffered_mut().unwrap().split_mut();
        incoming
            .append(&held[split..])
            .expect("completing the held record fits under the cap");
    }
    accs.reset(0);

    let mut sink = PlaintextSink::Accumulator(&mut accs);
    let outcome = feed(&mut server, Some(&mut sink), &mut out, &next, 0);
    assert!(
        matches!(outcome, DriveOutcome::Ok),
        "relievable backpressure must not fail the connection, got {outcome:?}"
    );
    let mut expected = vec![0xB2u8; 2000];
    expected.extend_from_slice(&[0xC3u8; 2000]);
    assert_eq!(
        accs.data(0),
        &expected[..],
        "the retried chunk must arrive intact and in order"
    );
}

// A `WouldBlock` no drive can relieve fails the connection. Retrying it
// forever would pin a worker thread, and in a thread-per-core runtime
// that is a wedged core, not a slow one.
#[test]
fn feed_fails_rather_than_spinning_on_an_undrainable_buffer() {
    let (mut server, mut client) = conn_pair();
    let mut accs = AccumulatorTable::new(2, 4096);
    handshake(&mut server, &mut client, &mut accs);

    let marker = encrypt_all(&mut client, &[0xA1u8; 16]);
    let stuck = encrypt_all(&mut client, &[0xB2u8; 2000]);
    let next = encrypt_all(&mut client, &[0xC3u8; 2000]);

    // Only half of `stuck` is ever delivered, so rustls buffers it waiting
    // for the rest and discards nothing — no drive can free a byte.
    let split = stuck.len() / 2;
    let cap = split + next.len();
    assert!(marker.len() < split, "compaction must not pay for itself");
    server
        .conn
        .as_unbuffered_mut()
        .unwrap()
        .set_ciphertext_cap_for_test(512, cap);

    let mut prefix = marker.clone();
    prefix.extend_from_slice(&stuck[..split]);
    let mut out = Vec::new();
    {
        let mut sink = PlaintextSink::Accumulator(&mut accs);
        let outcome = feed(&mut server, Some(&mut sink), &mut out, &prefix, 0);
        assert!(matches!(outcome, DriveOutcome::Ok), "{outcome:?}");
    }

    let mut sink = PlaintextSink::Accumulator(&mut accs);
    let outcome = feed(&mut server, Some(&mut sink), &mut out, &next, 0);
    let DriveOutcome::Error(err) = outcome else {
        panic!("an undrainable buffer must fail the connection, got {outcome:?}");
    };
    assert!(
        err.to_string().contains("undrainable"),
        "must be the anti-spin guard, not an unrelated failure: {err}"
    );
}

// Round-trip: encrypt on the client through the chunking path, decrypt on
// the server through `feed`. Exercises multi-record plaintext (> 16 KiB)
// so the fragmenter and the chunk-size retry both run.
#[test]
fn encrypt_round_trips_multi_record_plaintext() {
    let (mut server, mut client) = conn_pair();
    let mut accs = AccumulatorTable::new(2, 4 * 1024 * 1024);
    handshake(&mut server, &mut client, &mut accs);
    accs.reset(0);

    let plaintext: Vec<u8> = (0..300_000u32)
        .map(|i| i.wrapping_mul(2654435761) as u8)
        .collect();
    let cipher = encrypt_all(&mut client, &plaintext);

    let mut out = Vec::new();
    let mut sink = PlaintextSink::Accumulator(&mut accs);
    let outcome = feed(&mut server, Some(&mut sink), &mut out, &cipher, 0);
    assert!(matches!(outcome, DriveOutcome::Ok), "got {outcome:?}");
    assert_eq!(accs.data(0), &plaintext[..]);
}

// A destination buffer far smaller than one record still makes progress:
// the chunk size shrinks from rustls' own `required_size` rather than a
// hardcoded overhead constant.
#[test]
fn encrypt_converges_on_a_small_destination() {
    let (mut server, mut client) = conn_pair();
    let mut accs = AccumulatorTable::new(2, 1024 * 1024);
    handshake(&mut server, &mut client, &mut accs);
    accs.reset(0);

    let plaintext = vec![0x42u8; 20_000];
    let mut cipher = Vec::new();
    let mut offset = 0;
    // 512-byte destinations: every call must fit, and the loop must
    // terminate.
    while offset < plaintext.len() {
        let mut dst = vec![0u8; 512];
        let (used_pt, used_ct) = super::encrypt_chunk(&mut client, &plaintext[offset..], &mut dst)
            .expect("encrypt into a small buffer");
        assert!(used_pt > 0, "must make progress");
        assert!(used_ct <= 512);
        cipher.extend_from_slice(&dst[..used_ct]);
        offset += used_pt;
    }

    let mut out = Vec::new();
    let mut sink = PlaintextSink::Accumulator(&mut accs);
    let outcome = feed(&mut server, Some(&mut sink), &mut out, &cipher, 0);
    assert!(matches!(outcome, DriveOutcome::Ok), "got {outcome:?}");
    assert_eq!(accs.data(0), &plaintext[..]);
}

// A short send must not teach the connection a small chunk size. The cache
// is a starting point for every later send on the connection and only ever
// shrinks, so a 100-byte send that cached its own size would fragment
// every subsequent send into 100-byte records with no way back.
#[test]
fn a_short_send_does_not_poison_the_chunk_cache() {
    let (mut server, mut client) = conn_pair();
    let mut accs = AccumulatorTable::new(2, 4096);
    handshake(&mut server, &mut client, &mut accs);

    // Two big sends against the same destination size: the first learns
    // the ceiling, the second confirms it has converged.
    let big = vec![0x11u8; 64 * 1024];
    let mut dst = vec![0u8; 4096];
    super::encrypt_chunk(&mut client, &big, &mut dst).expect("encrypt");
    let (converged, _) = super::encrypt_chunk(&mut client, &big, &mut dst).expect("encrypt");
    assert!(
        converged > 1024,
        "a 4 KiB destination fits far more than that"
    );

    // A short send, same destination size.
    let (used, _) = super::encrypt_chunk(&mut client, &[0x22u8; 100], &mut dst).expect("encrypt");
    assert_eq!(used, 100);

    let (after, _) = super::encrypt_chunk(&mut client, &big, &mut dst).expect("encrypt");
    assert_eq!(
        after, converged,
        "a short send must not shrink the cached chunk size"
    );
}

// `BlockedHandshake` with nothing to send is a quiet return, not an error
// and not a spin: a freshly-created server has no input and emits nothing.
#[test]
fn drive_on_a_blocked_server_is_quiet() {
    let (mut server, _client) = conn_pair();
    let mut accs = AccumulatorTable::new(2, 4096);
    let (outcome, out) = pump(&mut server, &[], &mut accs);
    assert!(matches!(outcome, DriveOutcome::Ok), "got {outcome:?}");
    assert!(out.is_empty());
}
