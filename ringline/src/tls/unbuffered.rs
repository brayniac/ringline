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

use rustls::client::UnbufferedClientConnection;
use rustls::server::UnbufferedServerConnection;

/// A rustls connection driven through the *unbuffered* API
/// (`process_tls_records` + `WriteTraffic::encrypt`).
///
/// Constructible today; not yet driven — feeding ciphertext in and encrypting
/// records out lands in a follow-on plan (see `docs/journal/2026-09-unbuffered-tls.md`).
pub enum UnbufferedKind {
    // Neither variant is built by production code yet (see `TlsConnKind::Unbuffered`
    // in `tls/mod.rs`); `Client` is constructed by `tests::unbuffered_connection_is_not_buffered`
    // below, but that's test-only and invisible to the plain `lib` build, and
    // nothing constructs `Server` at all yet. `-D warnings` dead_code fires
    // "variant is never constructed" without these -- remove once the engine
    // is actually wired in.
    #[allow(dead_code)]
    Server(UnbufferedServerConnection),
    #[allow(dead_code)]
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rustls::client::UnbufferedClientConnection;
    use rustls::pki_types::ServerName;

    use crate::tls::{TlsConn, TlsConnKind};

    fn empty_client_config() -> Arc<rustls::ClientConfig> {
        rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth()
            .into()
    }

    // A connection built on the unbuffered engine reports itself as such:
    // `as_buffered_mut` returns `None` rather than panicking or silently
    // handing back a buffered view. This only pins the plumbing — driving
    // the connection (handshake, records) is a later plan.
    #[test]
    fn unbuffered_connection_is_not_buffered() {
        let config = empty_client_config();
        let server_name: ServerName<'static> = "localhost".try_into().unwrap();
        let client = UnbufferedClientConnection::new(config, server_name)
            .expect("constructing an unbuffered client connection does not drive the handshake");

        let mut tls_conn = TlsConn {
            conn: TlsConnKind::Unbuffered(super::UnbufferedKind::Client(client)),
            handshake_complete: false,
            peer_sent_close_notify: false,
            close_notify_sent: false,
        };

        assert!(tls_conn.conn.as_buffered_mut().is_none());
    }
}
