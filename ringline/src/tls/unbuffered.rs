//! Unbuffered TLS record layer, built on rustls' `UnbufferedConnectionCommon`.
//!
//! Encrypts directly from caller memory into a send-pool slot via
//! `WriteTraffic::encrypt`, removing the copy into rustls' internal plaintext
//! buffer that the buffered engine pays.
//!
//! Implemented in a follow-on (see `docs/journal/2026-09-unbuffered-tls.md`);
//! this module exists so the
//! `tls-unbuffered` feature builds and is exercised by CI from the start.
//!
//! `TlsConnKind` now carries an [`UnbufferedConn`] arm (see `tls/mod.rs`), so
//! this engine is constructible and reachable through the shared
//! `CommonState` accessors — but nothing yet feeds it ciphertext or drives a
//! handshake. `TlsConn`, `TlsTable` and `drain_tls_plaintext` still assume
//! the buffered engine wherever they reach past `TlsConnKind` (e.g.
//! `reader()` for plaintext draining, or constructing `BufferedKind`
//! directly in `TlsTable::create`); wiring a second engine through those is
//! the follow-on plan. [`super::ciphertext::CiphertextBuf`] is the
//! incoming-ciphertext buffer this engine will drive.

// The whole engine is unreferenced until `TlsTable::create` is pointed at it:
// the constructors, the state machine, and the chunk-sizing cache are all
// reachable only from code that does not exist yet. A module-level allow is
// the honest granularity — the alternative is sprinkling six attributes that
// all say the same thing. REMOVE THIS when engine selection is wired up;
// leaving it would silence real dead code in the finished engine.
//
// This lint is invisible on macOS: `lib.rs` applies
// `#[cfg_attr(not(has_io_uring), allow(dead_code))]` to the whole `tls`
// module, so only a Linux build fails on it.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::io;
use std::sync::Arc;

use rustls::client::{ClientConnectionData, UnbufferedClientConnection};
use rustls::pki_types::ServerName;
use rustls::server::{ServerConnectionData, UnbufferedServerConnection};
use rustls::unbuffered::{ConnectionState, EncodeError, UnbufferedStatus};

use super::ciphertext::{CiphertextBuf, INITIAL_SHRINK_TO, MAX_SINGLE_APPEND, MIN_CIPHERTEXT_CAP};
use super::{PlaintextSink, TlsConn};

/// A rustls connection driven through the *unbuffered* API
/// (`process_tls_records` + `WriteTraffic::encrypt`).
///
/// Constructible today; not yet driven — feeding ciphertext in and encrypting
/// records out lands in a follow-on plan (see `docs/journal/2026-09-unbuffered-tls.md`).
pub enum UnbufferedKind {
    Server(UnbufferedServerConnection),
    Client(UnbufferedClientConnection),
}

impl UnbufferedKind {
    pub fn wants_write(&self) -> bool {
        match self {
            UnbufferedKind::Server(c) => c.wants_write(),
            UnbufferedKind::Client(c) => c.wants_write(),
        }
    }

    pub fn is_handshaking(&self) -> bool {
        match self {
            UnbufferedKind::Server(c) => c.is_handshaking(),
            UnbufferedKind::Client(c) => c.is_handshaking(),
        }
    }

    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        match self {
            UnbufferedKind::Server(c) => c.alpn_protocol(),
            UnbufferedKind::Client(c) => c.alpn_protocol(),
        }
    }

    pub fn negotiated_cipher_suite(&self) -> Option<rustls::SupportedCipherSuite> {
        match self {
            UnbufferedKind::Server(c) => c.negotiated_cipher_suite(),
            UnbufferedKind::Client(c) => c.negotiated_cipher_suite(),
        }
    }

    pub fn protocol_version(&self) -> Option<rustls::ProtocolVersion> {
        match self {
            UnbufferedKind::Server(c) => c.protocol_version(),
            UnbufferedKind::Client(c) => c.protocol_version(),
        }
    }

    /// Always `None`: `UnbufferedServerConnection` does not expose the SNI
    /// hostname rustls parsed from the ClientHello — unlike the buffered
    /// `ServerConnection::server_name()`, no equivalent accessor exists on
    /// `UnbufferedServerConnection` or on `CommonState`, which is all that
    /// `UnbufferedConnectionCommon` derefs to (verified against rustls
    /// 0.23.41 source; `server_name()` lives only on the handshake-callback
    /// `ClientHello` type and on the buffered `ServerConnection`). Capturing
    /// SNI for the unbuffered engine would need a `ClientHello` callback on
    /// `ServerConfig` to stash it on `TlsConn` at handshake time — tracked as
    /// follow-on work, out of scope here. See `TlsInfo::sni_hostname` for the
    /// documented limitation this produces.
    pub fn sni_hostname(&self) -> Option<&str> {
        None
    }

    // No `send_close_notify` here: it's buffered-only, not just
    // differently-shaped for this engine. `CommonState::send_close_notify(&mut
    // self)` is unreachable from here at all --
    // `UnbufferedServerConnection`/`UnbufferedClientConnection` `DerefMut`
    // down to `UnbufferedConnectionCommon<Data>`, but
    // `UnbufferedConnectionCommon` implements only `Deref` (not `DerefMut`)
    // to `CommonState` (verified against rustls 0.23.41's `src/conn.rs`),
    // and Rust's deref coercion requires `DerefMut` at every step. This
    // isn't a gap: rustls's unbuffered API expresses close_notify
    // differently on purpose -- `docs/tls-unbuffered-design.md` ("###
    // close_notify") already designs the real mechanism,
    // `WriteTraffic::queue_close_notify(&mut self, outgoing_tls: &mut
    // [u8])`, reachable only from inside the `process_tls_records` state
    // machine. Wiring that in is part of driving the engine (a later plan).
    // `TlsConnKind::send_close_notify` doesn't exist either, for the same
    // reason -- see `buffered::BufferedKind::send_close_notify` and
    // `TlsTable::send_close_notify_queued`, its one caller.
}

/// A TLS connection driven by the unbuffered engine, alongside the state the
/// buffered engine instead keeps inside rustls: the incoming-ciphertext
/// buffer, a stash for plaintext that surfaced on a path with nowhere to put
/// it, and a chunk-sizing cache the send path will use once `encrypt` lands
/// (write-only until then).
pub struct UnbufferedConn {
    kind: UnbufferedKind,
    /// Received ciphertext awaiting `process_tls_records`. Deliberately sized
    /// from the module constants rather than a `Config` knob: the cap only has
    /// to clear `MIN_CIPHERTEXT_CAP`'s no-deadlock floor, and adding public
    /// surface for it would be hard to walk back. Revisit if a rig sweep shows
    /// the cap binding — see `docs/journal/2026-09-unbuffered-tls.md`.
    incoming: CiphertextBuf,
    /// Plaintext popped from `ReadTraffic` while no [`super::PlaintextSink`]
    /// was available (i.e. the send path drove the state machine and found
    /// application data pending). The recv path drains this into the sink
    /// before touching rustls again, so the byte stream keeps its order.
    ///
    /// Expected to stay empty: the recv path always drives until no plaintext
    /// remains. It exists so that a state machine that surprises us loses
    /// throughput, not bytes.
    pending_plaintext: VecDeque<Vec<u8>>,
    /// Largest plaintext slice known to encrypt into one output buffer of
    /// `chunk_basis` bytes. Learned from rustls' `required_size` on the first
    /// `InsufficientSize`, then reused. `0` means "not yet learned".
    max_plaintext_per_chunk: usize,
    /// Output buffer size `max_plaintext_per_chunk` was learned against. A
    /// different size invalidates it.
    chunk_basis: usize,
}

impl UnbufferedConn {
    pub fn new_server(config: Arc<rustls::ServerConfig>) -> Result<Self, rustls::Error> {
        Ok(Self::wrap(UnbufferedKind::Server(
            UnbufferedServerConnection::new(config)?,
        )))
    }

    pub fn new_client(
        config: Arc<rustls::ClientConfig>,
        server_name: ServerName<'static>,
    ) -> Result<Self, rustls::Error> {
        Ok(Self::wrap(UnbufferedKind::Client(
            UnbufferedClientConnection::new(config, server_name)?,
        )))
    }

    fn wrap(kind: UnbufferedKind) -> Self {
        Self {
            kind,
            incoming: CiphertextBuf::new(INITIAL_SHRINK_TO, MIN_CIPHERTEXT_CAP),
            pending_plaintext: VecDeque::new(),
            max_plaintext_per_chunk: 0,
            chunk_basis: 0,
        }
    }

    /// Test-only: rebuild the incoming-ciphertext buffer with a `cap` below
    /// [`MIN_CIPHERTEXT_CAP`], via [`CiphertextBuf::with_cap_unchecked`], so
    /// [`feed`]'s `WouldBlock` backpressure path becomes reachable. A
    /// conforming peer cannot reach it on a real connection — the floor is
    /// sized to prevent exactly that — which would otherwise leave both the
    /// retry and the anti-spin guard untested.
    ///
    /// Panics if ciphertext is pending: the replacement would silently drop
    /// it, and a test that re-caps mid-stream is measuring nothing.
    #[cfg(test)]
    pub(crate) fn set_ciphertext_cap_for_test(&mut self, initial: usize, cap: usize) {
        assert!(
            self.incoming.is_empty(),
            "re-capping would drop pending ciphertext"
        );
        self.incoming = CiphertextBuf::with_cap_unchecked(initial, cap);
    }

    pub fn kind(&self) -> &UnbufferedKind {
        &self.kind
    }

    /// Borrow the rustls connection, the ciphertext buffer and the deferred
    /// plaintext stash at once. `process_tls_records` borrows the connection
    /// *and* the slice handed to it, so the state machine cannot reach these
    /// through `&mut self` while a `ConnectionState` is alive.
    pub fn split_mut(
        &mut self,
    ) -> (
        &mut UnbufferedKind,
        &mut CiphertextBuf,
        &mut VecDeque<Vec<u8>>,
    ) {
        (
            &mut self.kind,
            &mut self.incoming,
            &mut self.pending_plaintext,
        )
    }
}

/// `process_tls_records` is implemented separately on
/// `UnbufferedConnectionCommon<ClientConnectionData>` and
/// `<ServerConnectionData>` (the shared body is private), and
/// `ConnectionState<'_, '_, Data>` differs between them — so one non-generic
/// function cannot match on both. This trait dispatches once, at the top of
/// [`drive`], and the state machine below monomorphizes over `Data`.
trait UnbufferedEngine {
    type Data;
    fn process<'c, 'i>(
        &'c mut self,
        incoming: &'i mut [u8],
    ) -> UnbufferedStatus<'c, 'i, Self::Data>;
}

impl UnbufferedEngine for UnbufferedClientConnection {
    type Data = ClientConnectionData;
    fn process<'c, 'i>(
        &'c mut self,
        incoming: &'i mut [u8],
    ) -> UnbufferedStatus<'c, 'i, Self::Data> {
        self.process_tls_records(incoming)
    }
}

impl UnbufferedEngine for UnbufferedServerConnection {
    type Data = ServerConnectionData;
    fn process<'c, 'i>(
        &'c mut self,
        incoming: &'i mut [u8],
    ) -> UnbufferedStatus<'c, 'i, Self::Data> {
        self.process_tls_records(incoming)
    }
}

/// What [`drive`] observed. Mapped to `TlsRecvResult` by the backend wiring.
#[derive(Debug)]
pub(crate) enum DriveOutcome {
    /// The machine ran to a blocking state with nothing else to report.
    Ok,
    /// The handshake completed on this call (edge-triggered, once).
    HandshakeJustCompleted,
    /// The peer sent close_notify, or the connection is fully closed.
    Closed,
    /// Fatal: the connection must be torn down.
    Error(rustls::Error),
}

/// Drive the unbuffered state machine until it blocks.
///
/// Handshake ciphertext (`EncodeTlsData`) is appended to `out`; the caller
/// transmits it in order. Decrypted plaintext goes to `sink` — `None` on the
/// send path, where any plaintext found is stashed on the connection instead
/// (see [`UnbufferedConn::pending_plaintext`]).
///
/// Application data is *not* encrypted here: that is the send path's
/// `WriteTraffic::encrypt`, which writes straight into a pool slot. `out` only
/// ever carries handshake records and alerts.
///
/// When a handshake completion and a close land on the same call, this returns
/// `HandshakeJustCompleted`. That is the same precedence [`fold_outcome`]
/// applies across the chunks of a [`feed`]; its doc comment carries the
/// reasoning, and the rule is stated there alone so the two cannot drift.
pub(crate) fn drive(
    tls_conn: &mut TlsConn,
    mut sink: Option<&mut PlaintextSink<'_>>,
    out: &mut Vec<u8>,
    conn_index: u32,
) -> DriveOutcome {
    let was_handshaking = !tls_conn.handshake_complete;

    // Anything stashed by an earlier sink-less drive comes out first, so the
    // application sees one ordered byte stream.
    if let Some(s) = sink.as_deref_mut()
        && !drain_pending_plaintext(tls_conn, s, conn_index)
    {
        return DriveOutcome::Error(rustls::Error::General(
            "recv accumulator limit exceeded".into(),
        ));
    }

    let Some(conn) = tls_conn.conn.as_unbuffered_mut() else {
        return DriveOutcome::Error(rustls::Error::General(
            "connection not driven by the unbuffered TLS engine".into(),
        ));
    };
    let (kind, incoming, pending) = conn.split_mut();

    // `sink` moves into whichever arm runs — the arms are exclusive, so no
    // reborrow is needed here.
    let (outcome, peer_closed) = match kind {
        UnbufferedKind::Server(c) => drive_inner(c, incoming, pending, sink, out, conn_index),
        UnbufferedKind::Client(c) => drive_inner(c, incoming, pending, sink, out, conn_index),
    };

    if peer_closed {
        tls_conn.peer_sent_close_notify = true;
    }
    if matches!(outcome, DriveOutcome::Error(_)) {
        return outcome;
    }
    if was_handshaking && !tls_conn.conn.is_handshaking() {
        tls_conn.handshake_complete = true;
        return DriveOutcome::HandshakeJustCompleted;
    }
    outcome
}

/// The state machine proper, monomorphized per connection role.
///
/// Returns the outcome plus whether the peer's close_notify was observed (the
/// caller owns `TlsConn`'s flags; this function only sees the rustls half).
fn drive_inner<C: UnbufferedEngine>(
    conn: &mut C,
    incoming: &mut CiphertextBuf,
    pending: &mut VecDeque<Vec<u8>>,
    mut sink: Option<&mut PlaintextSink<'_>>,
    out: &mut Vec<u8>,
    conn_index: u32,
) -> (DriveOutcome, bool) {
    let mut peer_closed = false;
    let mut closed = false;
    let mut sink_overflow = false;

    loop {
        let status = conn.process(incoming.pending());
        let mut discard = status.discard;
        // `blocked` means the machine cannot progress without more input from
        // the peer or another call from us; that is the loop's exit condition.
        let mut blocked = false;
        let mut error: Option<rustls::Error> = None;

        match status.state {
            Err(e) => {
                error = Some(e);
            }
            Ok(ConnectionState::ReadTraffic(mut rt)) => {
                while let Some(record) = rt.next_record() {
                    match record {
                        Ok(r) => {
                            // rustls' contract: this is *additional* discard on
                            // top of `UnbufferedStatus::discard`. Zero in
                            // 0.23.41; honoured so an in-place-decryption
                            // release does not silently desynchronise us.
                            discard += r.discard;
                            // Overflow drops this record where the `None` arm
                            // below would stash it: `status.discard` was fixed
                            // before the loop, so the ciphertext is gone either
                            // way, and overflow returns `Error` — teardown, no
                            // retry. Stashing would only defer bytes nobody
                            // will read.
                            match sink.as_deref_mut() {
                                Some(PlaintextSink::Accumulator(accs)) => {
                                    if !accs.append(conn_index, r.payload) {
                                        sink_overflow = true;
                                        break;
                                    }
                                }
                                #[cfg(has_io_uring)]
                                Some(PlaintextSink::Segments {
                                    hold,
                                    outstanding,
                                    max,
                                }) => {
                                    if outstanding.saturating_add(r.payload.len()) > *max {
                                        sink_overflow = true;
                                        break;
                                    }
                                    hold.push_back(crate::backend::HeldRecvBuf::Owned(
                                        bytes::Bytes::copy_from_slice(r.payload),
                                    ));
                                    *outstanding += r.payload.len();
                                }
                                None => pending.push_back(r.payload.to_vec()),
                            }
                        }
                        Err(e) => {
                            error = Some(e);
                            break;
                        }
                    }
                }
            }
            Ok(ConnectionState::EncodeTlsData(mut enc)) => {
                // `encode` is all-or-nothing and needs one contiguous buffer,
                // which can exceed a send-pool slot — hence the scratch `Vec`.
                // Handshake-only; the data path never comes through here.
                let start = out.len();
                let mut room = out.capacity().saturating_sub(start).max(1024);
                loop {
                    out.resize(start + room, 0);
                    match enc.encode(&mut out[start..]) {
                        Ok(n) => {
                            out.truncate(start + n);
                            break;
                        }
                        Err(EncodeError::InsufficientSize(need)) => {
                            room = need.required_size;
                        }
                        Err(e) => {
                            out.truncate(start);
                            error = Some(rustls::Error::General(e.to_string()));
                            break;
                        }
                    }
                }
            }
            Ok(ConnectionState::TransmitTlsData(mut transmit)) => {
                // Everything encoded so far is in `out`, and the caller queues
                // `out` through the per-connection send queue before any later
                // record — so from rustls' point of view it is transmitted.
                // `may_encrypt_app_data` is deliberately not consulted:
                // ringline exposes no early-data API, so there is never app
                // data waiting at this point.
                let _ = transmit.may_encrypt_app_data();
                transmit.done();
            }
            Ok(ConnectionState::BlockedHandshake) => blocked = true,
            Ok(ConnectionState::WriteTraffic(_)) => blocked = true,
            Ok(ConnectionState::PeerClosed) => {
                peer_closed = true;
            }
            Ok(ConnectionState::Closed) => {
                closed = true;
                blocked = true;
            }
            Ok(ConnectionState::ReadEarlyData(_)) => {
                // Ringline exposes no 0-RTT API. Treated as a protocol error
                // rather than silently dropped, per the design doc.
                error = Some(rustls::Error::General(
                    "TLS early data received but not supported".into(),
                ));
            }
            // `ConnectionState` is `#[non_exhaustive]`: a rustls upgrade can
            // introduce a state this loop does not know how to service.
            // Failing the connection beats spinning on it forever.
            Ok(_) => {
                error = Some(rustls::Error::General(
                    "unhandled rustls unbuffered connection state".into(),
                ));
            }
        }

        // `status` is dead here, so the ciphertext buffer is free again.
        incoming.discard(discard);

        if let Some(e) = error {
            return (DriveOutcome::Error(e), peer_closed);
        }
        if sink_overflow {
            return (
                DriveOutcome::Error(rustls::Error::General(
                    "recv accumulator limit exceeded".into(),
                )),
                peer_closed,
            );
        }
        if blocked {
            break;
        }
    }

    if closed || peer_closed {
        (DriveOutcome::Closed, peer_closed)
    } else {
        (DriveOutcome::Ok, peer_closed)
    }
}

/// Fold a chunk's outcome into the running one. Edge-triggered signals must
/// survive later chunks of the same feed, so this is a rank, not "last wins":
///
///   Error > HandshakeJustCompleted > Closed > Ok
///
/// `HandshakeJustCompleted` outranks `Closed` because the backends act on it
/// to wake a connect waiter (`wake_connect`) or spawn the accept task, while
/// the `Closed` arm does neither and `close_connection` does not wake connect
/// waiters either — so dropping the completion edge hangs an outbound TLS
/// connect until its timeout. A peer can produce both in one feed by putting
/// its last handshake flight and a close_notify in the same segment. The close
/// is not lost: `peer_sent_close_notify` is set, so the following FIN reads as
/// a clean close rather than a truncation.
///
/// `Error` outranks everything, though `feed` returns early on it today.
fn fold_outcome(last: DriveOutcome, next: DriveOutcome) -> DriveOutcome {
    fn rank(o: &DriveOutcome) -> u8 {
        match o {
            DriveOutcome::Ok => 0,
            DriveOutcome::Closed => 1,
            DriveOutcome::HandshakeJustCompleted => 2,
            DriveOutcome::Error(_) => 3,
        }
    }
    if rank(&next) >= rank(&last) {
        next
    } else {
        last
    }
}

/// Append received ciphertext and drive the state machine over it.
///
/// `ciphertext` is chunked at [`MAX_SINGLE_APPEND`]: `CiphertextBuf::append`
/// `debug_assert`s that bound (silent in release), and it is reachable from
/// public config — `ConfigBuilder::recv_buffer` accepts buffer sizes above
/// 64 KiB and the io_uring recv path hands one whole provided buffer here.
///
/// `append` is all-or-nothing and returns `WouldBlock` when the buffer holds
/// only live data. That is backpressure, not an error: drive first (which
/// discards what rustls consumed), then retry. If a drive frees nothing and
/// the append still refuses, the connection is unrecoverable — `append` would
/// otherwise be retried forever. `MIN_CIPHERTEXT_CAP` is sized so this cannot
/// happen for a conforming peer; the check is what stops a non-conforming one
/// from spinning the worker.
pub(crate) fn feed(
    tls_conn: &mut TlsConn,
    mut sink: Option<&mut PlaintextSink<'_>>,
    out: &mut Vec<u8>,
    ciphertext: &[u8],
    conn_index: u32,
) -> DriveOutcome {
    let mut last = DriveOutcome::Ok;
    for chunk in ciphertext.chunks(MAX_SINGLE_APPEND) {
        loop {
            let Some(conn) = tls_conn.conn.as_unbuffered_mut() else {
                return DriveOutcome::Error(rustls::Error::General(
                    "connection not driven by the unbuffered TLS engine".into(),
                ));
            };
            let (_, incoming, _) = conn.split_mut();
            let before = incoming.len();
            match incoming.append(chunk) {
                Ok(()) => break,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // Drain, then retry. Each retry requires the drive to have
                    // strictly shrunk the live set, so the loop terminates; a
                    // drive that frees nothing means the buffer is full of data
                    // rustls will not consume.
                    let outcome = drive(tls_conn, sink.as_deref_mut(), out, conn_index);
                    if let DriveOutcome::Error(err) = outcome {
                        return DriveOutcome::Error(err);
                    }
                    let (_, incoming, _) = tls_conn
                        .conn
                        .as_unbuffered_mut()
                        .expect("engine checked above")
                        .split_mut();
                    if incoming.len() >= before {
                        return DriveOutcome::Error(rustls::Error::General(
                            "TLS ciphertext buffer full and undrainable".into(),
                        ));
                    }
                    last = fold_outcome(last, outcome);
                }
                Err(e) => {
                    return DriveOutcome::Error(rustls::Error::General(e.to_string()));
                }
            }
        }
        let outcome = drive(tls_conn, sink.as_deref_mut(), out, conn_index);
        if matches!(outcome, DriveOutcome::Error(_)) {
            return outcome;
        }
        last = fold_outcome(last, outcome);
    }
    // An empty feed is a deliberate flush, not a no-op: `chunks()` yields
    // nothing for an empty slice, so without this the loop body never runs and
    // the machine is never driven. Callers rely on it to make an idle
    // connection emit — a fresh client has a ClientHello waiting with no input
    // to deframe, which is exactly how the backends kick off an outbound
    // handshake once the TCP connect completes.
    if ciphertext.is_empty() {
        return drive(tls_conn, sink, out, conn_index);
    }
    last
}

/// Move any stashed plaintext into `sink`, oldest first. Returns `false` if
/// the sink hit its bound — the chunk is left in the stash and the caller must
/// kill the connection, matching `drain_tls_plaintext`'s contract.
fn drain_pending_plaintext(
    tls_conn: &mut TlsConn,
    sink: &mut PlaintextSink<'_>,
    conn_index: u32,
) -> bool {
    let Some(conn) = tls_conn.conn.as_unbuffered_mut() else {
        return true;
    };
    let (_, _, pending) = conn.split_mut();
    while let Some(chunk) = pending.front() {
        match sink {
            PlaintextSink::Accumulator(accs) => {
                if !accs.append(conn_index, chunk) {
                    return false;
                }
            }
            #[cfg(has_io_uring)]
            PlaintextSink::Segments {
                hold,
                outstanding,
                max,
            } => {
                if outstanding.saturating_add(chunk.len()) > *max {
                    return false;
                }
                hold.push_back(crate::backend::HeldRecvBuf::Owned(
                    bytes::Bytes::copy_from_slice(chunk),
                ));
                *outstanding += chunk.len();
            }
        }
        pending.pop_front();
    }
    true
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rustls::pki_types::ServerName;

    use super::{DriveOutcome, MAX_SINGLE_APPEND, UnbufferedConn, drive, feed};
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
    fn pump(
        to: &mut TlsConn,
        bytes: &[u8],
        accs: &mut AccumulatorTable,
    ) -> (DriveOutcome, Vec<u8>) {
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

    /// Test-only: drive to `WriteTraffic` and encrypt `plaintext` into `dst` in
    /// one call, returning the ciphertext length. The real send path — with
    /// chunk sizing against a pool slot — is a later task; this exists so the
    /// recv-side sink bound can be tested before it lands.
    fn encrypt_one_shot(
        kind: &mut super::UnbufferedKind,
        incoming: &mut super::CiphertextBuf,
        plaintext: &[u8],
        dst: &mut [u8],
    ) -> usize {
        match kind {
            super::UnbufferedKind::Server(c) => encrypt_with(c, incoming, plaintext, dst),
            super::UnbufferedKind::Client(c) => encrypt_with(c, incoming, plaintext, dst),
        }
    }

    fn encrypt_with<C: super::UnbufferedEngine>(
        conn: &mut C,
        incoming: &mut super::CiphertextBuf,
        plaintext: &[u8],
        dst: &mut [u8],
    ) -> usize {
        let status = conn.process(incoming.pending());
        assert_eq!(
            status.discard, 0,
            "a completed handshake leaves no ciphertext to discard"
        );
        match status.state {
            Ok(super::ConnectionState::WriteTraffic(mut wt)) => wt
                .encrypt(plaintext, dst)
                .expect("dst is sized for the payload"),
            _ => panic!("expected WriteTraffic after a completed handshake"),
        }
    }

    // A full handshake completes when each side's output is fed to the other
    // and both are driven by `drive()` alone. Both report
    // HandshakeJustCompleted exactly once.
    #[test]
    fn drive_completes_a_handshake() {
        let (mut server, mut client) = conn_pair();
        let mut accs = AccumulatorTable::new(2, 4096);

        let (client_completions, server_completions) =
            handshake(&mut server, &mut client, &mut accs);

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
        let mut cipher = vec![0u8; 48 * 1024];
        let n = {
            let (kind, incoming, _) = client.conn.as_unbuffered_mut().unwrap().split_mut();
            encrypt_one_shot(kind, incoming, &plaintext, &mut cipher)
        };
        cipher.truncate(n);

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

    /// Test-only: queue a close_notify alert into `dst`, returning its length.
    /// Reaches `WriteTraffic` the same way `encrypt_one_shot` does, so a test
    /// can produce a close in the same feed as a handshake completion. The
    /// engine's real close path is a later task.
    fn close_notify_one_shot(
        kind: &mut super::UnbufferedKind,
        incoming: &mut super::CiphertextBuf,
        dst: &mut [u8],
    ) -> usize {
        match kind {
            super::UnbufferedKind::Server(c) => close_notify_with(c, incoming, dst),
            super::UnbufferedKind::Client(c) => close_notify_with(c, incoming, dst),
        }
    }

    fn close_notify_with<C: super::UnbufferedEngine>(
        conn: &mut C,
        incoming: &mut super::CiphertextBuf,
        dst: &mut [u8],
    ) -> usize {
        let status = conn.process(incoming.pending());
        match status.state {
            Ok(super::ConnectionState::WriteTraffic(mut wt)) => wt
                .queue_close_notify(dst)
                .expect("dst is sized for a close_notify alert"),
            _ => panic!("expected WriteTraffic after a completed handshake"),
        }
    }

    /// Encrypt one application-data record on `conn`, returning its ciphertext.
    fn encrypt_record(conn: &mut TlsConn, plaintext: &[u8]) -> Vec<u8> {
        let mut buf = vec![0u8; plaintext.len() + 1024];
        let n = {
            let (kind, incoming, _) = conn.conn.as_unbuffered_mut().unwrap().split_mut();
            encrypt_one_shot(kind, incoming, plaintext, &mut buf)
        };
        buf.truncate(n);
        buf
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
            coalesced.extend_from_slice(&encrypt_record(&mut client, &record));
        }
        let mut alert = vec![0u8; 1024];
        let n = {
            let (kind, incoming, _) = client.conn.as_unbuffered_mut().unwrap().split_mut();
            close_notify_one_shot(kind, incoming, &mut alert)
        };
        coalesced.extend_from_slice(&alert[..n]);
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
        let marker = encrypt_record(&mut client, &[0xA1u8; 16]);
        let held = encrypt_record(&mut client, &[0xB2u8; 2000]);
        let next = encrypt_record(&mut client, &[0xC3u8; 2000]);

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

        let marker = encrypt_record(&mut client, &[0xA1u8; 16]);
        let stuck = encrypt_record(&mut client, &[0xB2u8; 2000]);
        let next = encrypt_record(&mut client, &[0xC3u8; 2000]);

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
}
