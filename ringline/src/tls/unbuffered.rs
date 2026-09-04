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
//! `TlsConnKind` now carries an [`UnbufferedKind`] arm (see `tls/mod.rs`), so
//! this engine is constructible and reachable through the shared
//! `CommonState` accessors — but nothing yet feeds it ciphertext or drives a
//! handshake. `TlsConn`, `TlsTable` and `drain_tls_plaintext` still assume
//! the buffered engine wherever they reach past `TlsConnKind` (e.g.
//! `reader()` for plaintext draining, or constructing `BufferedKind`
//! directly in `TlsTable::create`); wiring a second engine through those is
//! the follow-on plan. [`super::ciphertext::CiphertextBuf`] is the
//! incoming-ciphertext buffer this engine will drive.

use std::collections::VecDeque;
use std::sync::Arc;

use rustls::client::UnbufferedClientConnection;
use rustls::pki_types::ServerName;
use rustls::server::UnbufferedServerConnection;

use super::ciphertext::{CiphertextBuf, INITIAL_SHRINK_TO, MIN_CIPHERTEXT_CAP};

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

/// A TLS connection driven by the unbuffered engine, with the two pieces of
/// state the buffered engine keeps inside rustls: the incoming-ciphertext
/// buffer `process_tls_records` deframes out of, and a stash for plaintext
/// that surfaced on a path with nowhere to put it.
pub struct UnbufferedConn {
    kind: UnbufferedKind,
    /// Received ciphertext awaiting `process_tls_records`. Sized from the
    /// module constants rather than a `Config` knob — see
    /// `docs/superpowers/plans/2026-09-04-unbuffered-tls-engine.md`.
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rustls::pki_types::ServerName;

    use super::UnbufferedConn;
    use crate::tls::{TlsConn, TlsConnKind};

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
}
