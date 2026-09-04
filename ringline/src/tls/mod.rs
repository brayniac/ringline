//! TLS support.
//!
//! Shared types live here; the record-layer implementation lives in the engine
//! submodules. `buffered` drives rustls' buffered `Connection` API
//! (`read_tls`/`process_new_packets`/`write_tls`).
//!
//! `TlsConnKind` is tagged by engine: [`TlsConnKind::Buffered`] wraps
//! [`buffered::BufferedKind`], which carries the buffered-only surface
//! (`read_tls`/`write_tls`/`process_new_packets`/`reader`/`writer`/
//! `send_close_notify`). `send_close_notify` lives there rather than on
//! `TlsConnKind` because it isn't actually `CommonState`-reachable from the
//! unbuffered connection types (`UnbufferedConnectionCommon` derefs to
//! `CommonState` but doesn't `DerefMut` to it) — a shared-looking method
//! name that turned out to be buffered-only once checked against rustls
//! 0.23.41, the same way `read_tls` et al. are. `TlsConnKind` itself keeps
//! only the methods rustls exposes through `CommonState` and that are
//! actually reachable that way from every engine's connection type — so the
//! feature-gated [`TlsConnKind::Unbuffered`] variant (wrapping
//! [`unbuffered::UnbufferedKind`]) adds a match arm to those, rather than
//! forcing every caller to unwrap an engine first. `TlsConn`, `TlsTable` and
//! `drain_tls_plaintext` still assume a buffered connection wherever they
//! reach past `TlsConnKind` (e.g. `reader()` for plaintext draining, or
//! constructing `BufferedKind` directly in `TlsTable::create`); the
//! unbuffered engine is constructible but not yet driven — threading it
//! through those is follow-on work.

#[allow(unused_imports)]
use std::io::{self, Read as _, Write as _};
use std::sync::Arc;

use rustls::pki_types::ServerName;
use rustls::{ClientConnection, ServerConnection};

#[allow(unused_imports)]
use crate::accumulator::AccumulatorTable;
#[cfg(has_io_uring)]
#[allow(unused_imports)]
use crate::buffer::send_copy::SendCopyPool;

mod buffered;
mod ciphertext;
#[cfg(feature = "tls-unbuffered")]
mod unbuffered;

// Glob re-export keeps call sites at `crate::tls::*`. When a second engine
// module lands, two globs sharing a name compile fine and fail only at the
// call site with E0659; give the engines distinct names, cfg-gate so only one
// glob is live, or switch to explicit re-exports at that point.
pub use buffered::*;

/// Information about a negotiated TLS session.
pub struct TlsInfo {
    pub(crate) protocol_version: Option<rustls::ProtocolVersion>,
    pub(crate) cipher_suite: Option<rustls::SupportedCipherSuite>,
    pub(crate) alpn_protocol: Option<Vec<u8>>,
    pub(crate) sni_hostname: Option<String>,
}

impl TlsInfo {
    /// The negotiated TLS protocol version, if the handshake has completed.
    pub fn protocol_version(&self) -> Option<rustls::ProtocolVersion> {
        self.protocol_version
    }

    /// The negotiated cipher suite, if the handshake has completed.
    pub fn cipher_suite(&self) -> Option<rustls::SupportedCipherSuite> {
        self.cipher_suite
    }

    /// The ALPN protocol negotiated for this session, if any.
    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        self.alpn_protocol.as_deref()
    }

    /// The SNI hostname the peer requested, if any.
    ///
    /// **Known limitation:** on a server connection driven by the
    /// `tls-unbuffered` engine, this is always `None`. rustls does not
    /// expose an equivalent of `ServerConnection::server_name()` on
    /// `UnbufferedServerConnection` (verified against rustls 0.23.41 —
    /// `server_name()` lives only on the handshake-callback `ClientHello`
    /// type and on the buffered `ServerConnection`; `UnbufferedServerConnection`
    /// derefs only as far as `CommonState`, which doesn't carry it either).
    /// Buffered connections are unaffected. Recovering SNI for the
    /// unbuffered engine would need a `ClientHello` callback on
    /// `ServerConfig` to capture it at handshake time and stash it on
    /// `TlsConn` — tracked as follow-on work, not yet implemented.
    pub fn sni_hostname(&self) -> Option<&str> {
        self.sni_hostname.as_deref()
    }
}

/// A TLS connection, tagged by which record-layer engine drives it.
///
/// Engine-specific surface lives on the inner kinds, so the compiler rejects
/// (say) `read_tls` on an unbuffered connection rather than leaving it a
/// runtime surprise. Only operations rustls exposes through `CommonState` --
/// which both engine families `Deref` to -- stay on this enum.
pub enum TlsConnKind {
    Buffered(buffered::BufferedKind),
    #[cfg(feature = "tls-unbuffered")]
    Unbuffered(unbuffered::UnbufferedConn),
}

impl TlsConnKind {
    /// The buffered connection, or `None` if another engine drives this one.
    ///
    /// `Some` for every connection when only the buffered engine is
    /// compiled in; `None` for a `tls-unbuffered`-engine connection once
    /// that feature is enabled. Do not collapse this to an infallible
    /// accessor.
    #[allow(
        clippy::unnecessary_wraps,
        reason = "unconditionally Some without the tls-unbuffered feature; kept fallible so both configurations share one signature"
    )]
    pub fn as_buffered_mut(&mut self) -> Option<&mut buffered::BufferedKind> {
        match self {
            Self::Buffered(k) => Some(k),
            #[cfg(feature = "tls-unbuffered")]
            Self::Unbuffered(_) => None,
        }
    }

    /// The unbuffered connection, or `None` if another engine drives this one.
    #[cfg(feature = "tls-unbuffered")]
    pub fn as_unbuffered_mut(&mut self) -> Option<&mut unbuffered::UnbufferedConn> {
        match self {
            Self::Buffered(_) => None,
            Self::Unbuffered(c) => Some(c),
        }
    }

    pub fn wants_write(&self) -> bool {
        match self {
            Self::Buffered(k) => k.wants_write(),
            #[cfg(feature = "tls-unbuffered")]
            Self::Unbuffered(c) => c.kind().wants_write(),
        }
    }

    pub fn is_handshaking(&self) -> bool {
        match self {
            Self::Buffered(k) => k.is_handshaking(),
            #[cfg(feature = "tls-unbuffered")]
            Self::Unbuffered(c) => c.kind().is_handshaking(),
        }
    }

    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        match self {
            Self::Buffered(k) => k.alpn_protocol(),
            #[cfg(feature = "tls-unbuffered")]
            Self::Unbuffered(c) => c.kind().alpn_protocol(),
        }
    }

    pub fn negotiated_cipher_suite(&self) -> Option<rustls::SupportedCipherSuite> {
        match self {
            Self::Buffered(k) => k.negotiated_cipher_suite(),
            #[cfg(feature = "tls-unbuffered")]
            Self::Unbuffered(c) => c.kind().negotiated_cipher_suite(),
        }
    }

    pub fn protocol_version(&self) -> Option<rustls::ProtocolVersion> {
        match self {
            Self::Buffered(k) => k.protocol_version(),
            #[cfg(feature = "tls-unbuffered")]
            Self::Unbuffered(c) => c.kind().protocol_version(),
        }
    }

    /// The SNI hostname the peer requested, if any.
    ///
    /// Unbuffered server connections always report `None` here — see
    /// [`unbuffered::UnbufferedKind::sni_hostname`] for why no equivalent to
    /// `ServerConnection::server_name()` is reachable from
    /// `UnbufferedServerConnection`. This is a real, documented limitation
    /// of the unbuffered engine (tracked on [`TlsInfo::sni_hostname`]), not
    /// a bug in this accessor — the buffered path is unaffected.
    pub fn sni_hostname(&self) -> Option<&str> {
        match self {
            Self::Buffered(k) => k.sni_hostname(),
            #[cfg(feature = "tls-unbuffered")]
            Self::Unbuffered(c) => c.kind().sni_hostname(),
        }
    }

    // No `send_close_notify` here: it is buffered-only, like `read_tls` et
    // al. -- see `buffered::BufferedKind::send_close_notify` for why, and
    // `TlsTable::send_close_notify_queued` for the one caller, which goes
    // through `as_buffered_mut()` instead.
}

/// Per-connection TLS state.
pub struct TlsConn {
    pub conn: TlsConnKind,
    pub handshake_complete: bool,
    /// True once the peer's close_notify alert has been processed. A TCP
    /// FIN arriving while this is false is a truncation (possibly an
    /// attacker-injected FIN) and must not look like a clean EOF.
    pub peer_sent_close_notify: bool,
    /// True when `send_close_notify` has been called. Used by the
    /// close_notify timeout mechanism to detect stalled shutdowns.
    pub close_notify_sent: bool,
}

/// Table of TLS connections, indexed by connection slot.
/// Stored as a separate EventLoop field for borrow splitting.
pub struct TlsTable {
    conns: Vec<Option<TlsConn>>,
    server_config: Option<Arc<rustls::ServerConfig>>,
    client_config: Option<Arc<rustls::ClientConfig>>,
    /// Single shared ciphertext scratch buffer (one per worker thread).
    /// Only used synchronously — we process one connection at a time.
    /// io_uring builds write ciphertext directly into pool slots via
    /// `PoolWriter` and don't need it.
    #[cfg(not(has_io_uring))]
    write_buf: Vec<u8>,
}

impl TlsTable {
    /// Create a table with capacity for `max_connections`.
    pub fn new(
        max_connections: u32,
        server_config: Option<Arc<rustls::ServerConfig>>,
        client_config: Option<Arc<rustls::ClientConfig>>,
    ) -> Self {
        let mut conns = Vec::with_capacity(max_connections as usize);
        conns.resize_with(max_connections as usize, || None);
        TlsTable {
            conns,
            server_config,
            client_config,
            #[cfg(not(has_io_uring))]
            write_buf: Vec::new(),
        }
    }

    /// Whether a server config is present (for TLS accept on inbound connections).
    pub fn has_server_config(&self) -> bool {
        self.server_config.is_some()
    }

    /// Whether a client config is present (for TLS connect on outbound connections).
    pub fn has_client_config(&self) -> bool {
        self.client_config.is_some()
    }

    /// Create a new TLS server connection at the given index.
    pub fn create(&mut self, conn_index: u32) -> Result<(), rustls::Error> {
        let server_config = self
            .server_config
            .as_ref()
            .expect("create() called without server_config");
        let conn = ServerConnection::new(server_config.clone())?;
        self.conns[conn_index as usize] = Some(TlsConn {
            conn: TlsConnKind::Buffered(buffered::BufferedKind::Server(conn)),
            handshake_complete: false,
            peer_sent_close_notify: false,
            close_notify_sent: false,
        });
        Ok(())
    }

    /// Create a new TLS client connection at the given index.
    pub fn create_client(
        &mut self,
        conn_index: u32,
        server_name: ServerName<'static>,
    ) -> Result<(), rustls::Error> {
        let client_config = self
            .client_config
            .as_ref()
            .expect("create_client() called without client_config");
        let conn = ClientConnection::new(client_config.clone(), server_name)?;
        self.conns[conn_index as usize] = Some(TlsConn {
            conn: TlsConnKind::Buffered(buffered::BufferedKind::Client(conn)),
            handshake_complete: false,
            peer_sent_close_notify: false,
            close_notify_sent: false,
        });
        Ok(())
    }

    /// Get a mutable reference to the TLS connection at the given index.
    pub fn get_mut(&mut self, conn_index: u32) -> Option<&mut TlsConn> {
        self.conns[conn_index as usize].as_mut()
    }

    /// Check if a connection has TLS state.
    pub fn has(&self, conn_index: u32) -> bool {
        self.conns[conn_index as usize].is_some()
    }

    /// Remove TLS state for a connection.
    pub fn remove(&mut self, conn_index: u32) {
        self.conns[conn_index as usize] = None;
    }

    /// Get TLS session information for a connection.
    pub fn get_info(&self, conn_index: u32) -> Option<TlsInfo> {
        let tls_conn = self.conns[conn_index as usize].as_ref()?;
        Some(TlsInfo {
            protocol_version: tls_conn.conn.protocol_version(),
            cipher_suite: tls_conn.conn.negotiated_cipher_suite(),
            alpn_protocol: tls_conn.conn.alpn_protocol().map(|s| s.to_vec()),
            sni_hostname: tls_conn.conn.sni_hostname().map(|s| s.to_string()),
        })
    }

    /// Send a TLS close_notify alert, encrypting the ciphertext into
    /// `BuiltSend`s for the caller to route through the per-connection send
    /// queue. Serializing through the queue (rather than pushing linked
    /// SQEs directly) keeps the alert ordered behind any in-flight send and
    /// lets the deferred Close fire only after it completes.
    ///
    /// `send_close_notify` is buffered-only (see
    /// `buffered::BufferedKind::send_close_notify`), so this goes through
    /// `as_buffered_mut()` -- consistent with `take_tls_output_sends` below,
    /// which already reaches into the buffered engine directly. A
    /// connection on an engine with no buffered close path (the
    /// `tls-unbuffered` engine, once real connections use it) has nothing to
    /// queue here; the `None` arm is a deliberate no-op, not an oversight.
    /// That engine will drive close_notify through
    /// `WriteTraffic::queue_close_notify` from inside `process_tls_records`
    /// instead, per `docs/tls-unbuffered-design.md` ("### close_notify").
    #[cfg(has_io_uring)]
    pub fn send_close_notify_queued(
        &mut self,
        conn_index: u32,
        generation: u32,
        send_copy_pool: &mut SendCopyPool,
        out: &mut Vec<crate::handler::BuiltSend>,
    ) {
        if let Some(tls_conn) = self.get_mut(conn_index)
            && let Some(buffered) = tls_conn.conn.as_buffered_mut()
        {
            buffered.send_close_notify();
            tls_conn.close_notify_sent = true;
            let _ = take_tls_output_sends(tls_conn, send_copy_pool, conn_index, generation, out);
        }
    }
}

/// Result of feeding ciphertext into a TLS connection.
pub enum TlsRecvResult {
    /// Data processed successfully.
    Ok,
    /// TLS handshake just completed — caller should fire on_accept.
    HandshakeJustCompleted,
    /// TLS error occurred.
    #[allow(dead_code)] // variant matched; inner value reserved for future error reporting
    Error(rustls::Error),
    /// Peer sent close_notify or connection is cleanly closed.
    Closed,
}

/// Where decrypted TLS plaintext chunks are delivered by [`drain_tls_plaintext`].
///
/// TLS recv is *copy-per-chunk*: rustls owns its decrypted-plaintext buffer, so
/// the bytes must be copied out either way — there is no zero-copy TLS recv (see
/// `docs/segmented-recv-design.md`, "## TLS"). The sink chooses the destination
/// of that copy based on the connection's recv domain.
pub(crate) enum PlaintextSink<'a> {
    /// Default path: append each chunk into the connection's contiguous recv
    /// accumulator (one copy). Bounded by the accumulator's `max_size`
    /// (`Config::recv_accumulator_max`); `append` returning `false` is the
    /// flood-kill signal.
    Accumulator(&'a mut AccumulatorTable),
    /// Segmented recv domain (io_uring only): each drained plaintext chunk is
    /// copied into an owned [`Bytes`] and pushed as a `HeldRecvBuf::Owned`
    /// segment to the connection's hold. TLS segments are *always* owned — the
    /// decrypt copy is the release, so they never pin the provided ring. The
    /// same outstanding bound as the accumulator path is enforced: `outstanding`
    /// tracks total held owned bytes and `max` mirrors `recv_accumulator_max`;
    /// exceeding it is the flood-kill signal.
    #[cfg(has_io_uring)]
    Segments {
        hold: &'a mut std::collections::VecDeque<crate::backend::HeldRecvBuf>,
        outstanding: usize,
        max: usize,
    },
}

/// Drain all currently-decrypted plaintext from a TLS connection into `sink`,
/// with no intermediate scratch buffer.
///
/// rustls's `Reader` implements `BufRead`: `fill_buf()` exposes the decrypted
/// plaintext in rustls's own buffer, and `consume()` advances past what we copied.
/// The chunk `fill_buf` returns is *not* one ≤16 KiB record — it is as much
/// contiguous plaintext as rustls has buffered, so segment sizes are arbitrary.
///
/// Returns `false` if the sink hit its outstanding bound (accumulator
/// `max_size`, or the held-plaintext `max` for the segmented domain) — the
/// plaintext was NOT consumed from rustls, and the caller must treat the
/// connection as broken (silently consuming would put a permanent gap in the
/// byte stream; an unbounded plaintext flood must kill the connection).
#[must_use]
fn drain_tls_plaintext(
    tls_conn: &mut TlsConn,
    sink: &mut PlaintextSink<'_>,
    conn_index: u32,
) -> bool {
    use std::io::BufRead;
    let mut reader = tls_conn
        .conn
        .as_buffered_mut()
        .expect("drain_tls_plaintext: connection not driven by the buffered TLS engine")
        .reader();
    loop {
        let chunk = match reader.fill_buf() {
            Ok([]) => break,
            Ok(b) => b,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(_) => break,
        };
        let n = chunk.len();
        match sink {
            PlaintextSink::Accumulator(accumulators) => {
                if !accumulators.append(conn_index, chunk) {
                    return false;
                }
            }
            #[cfg(has_io_uring)]
            PlaintextSink::Segments {
                hold,
                outstanding,
                max,
            } => {
                // Bound total outstanding held plaintext exactly as the
                // accumulator path bounds its buffer: an over-limit chunk is
                // NOT consumed from rustls, and the caller kills the connection.
                if outstanding.saturating_add(n) > *max {
                    return false;
                }
                hold.push_back(crate::backend::HeldRecvBuf::Owned(
                    bytes::Bytes::copy_from_slice(chunk),
                ));
                *outstanding += n;
            }
        }
        reader.consume(n);
    }
    true
}

/// Build a pool-backed send SQE entry without submitting it. The caller
/// routes it through the per-connection send queue (`submit_or_queue`) so
/// TLS ciphertext is serialized with every other send on the connection:
/// io_uring does not order independent SQEs, and a partial-send resubmit of
/// chunk A after chunk B already transmitted interleaves ciphertext on the
/// wire (bad_record_mac at the peer).
#[cfg(has_io_uring)]
fn build_pool_send(
    conn_index: u32,
    generation: u32,
    ptr: *const u8,
    len: u32,
    pool_slot: u16,
    tag: crate::completion::OpTag,
) -> crate::handler::BuiltSend {
    let user_data = crate::completion::UserData::encode(
        tag,
        conn_index,
        crate::completion::UserData::send_payload(pool_slot, generation),
    );
    let entry = io_uring::opcode::Send::new(io_uring::types::Fixed(conn_index), ptr, len)
        .flags(crate::completion::STREAM_SEND_FLAGS)
        .build()
        .user_data(user_data.raw());
    crate::handler::BuiltSend {
        entry,
        pool_slot,
        slab_idx: u16::MAX,
        total_len: len,
    }
}
