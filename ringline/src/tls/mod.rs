//! TLS support.
//!
//! Shared types live here; the record-layer implementation lives in the engine
//! submodules. `buffered` drives rustls' buffered `Connection` API
//! (`read_tls`/`process_new_packets`/`write_tls`).
//!
//! Note the split is currently by *file size*, not cleanly by engine:
//! `TlsConnKind`, `TlsConn`, `TlsTable` and `drain_tls_plaintext` are still
//! tied to the buffered API (`ClientConnection`/`ServerConnection`,
//! `reader()`/`writer()`). Only `TlsInfo`, `TlsRecvResult`, `PlaintextSink`
//! and `build_pool_send` are genuinely engine-agnostic. Adding a second
//! engine will require threading an engine dimension through the former set.

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
    pub fn sni_hostname(&self) -> Option<&str> {
        self.sni_hostname.as_deref()
    }
}

/// TLS connection kind — server (inbound) or client (outbound).
pub enum TlsConnKind {
    Server(ServerConnection),
    Client(ClientConnection),
}

impl TlsConnKind {
    pub fn read_tls(&mut self, rd: &mut dyn io::Read) -> io::Result<usize> {
        match self {
            TlsConnKind::Server(c) => c.read_tls(rd),
            TlsConnKind::Client(c) => c.read_tls(rd),
        }
    }

    pub fn write_tls(&mut self, wr: &mut dyn io::Write) -> io::Result<usize> {
        match self {
            TlsConnKind::Server(c) => c.write_tls(wr),
            TlsConnKind::Client(c) => c.write_tls(wr),
        }
    }

    pub fn process_new_packets(&mut self) -> Result<rustls::IoState, rustls::Error> {
        match self {
            TlsConnKind::Server(c) => c.process_new_packets(),
            TlsConnKind::Client(c) => c.process_new_packets(),
        }
    }

    pub fn reader(&mut self) -> rustls::Reader<'_> {
        match self {
            TlsConnKind::Server(c) => c.reader(),
            TlsConnKind::Client(c) => c.reader(),
        }
    }

    pub fn writer(&mut self) -> rustls::Writer<'_> {
        match self {
            TlsConnKind::Server(c) => c.writer(),
            TlsConnKind::Client(c) => c.writer(),
        }
    }

    pub fn wants_write(&self) -> bool {
        match self {
            TlsConnKind::Server(c) => c.wants_write(),
            TlsConnKind::Client(c) => c.wants_write(),
        }
    }

    pub fn is_handshaking(&self) -> bool {
        match self {
            TlsConnKind::Server(c) => c.is_handshaking(),
            TlsConnKind::Client(c) => c.is_handshaking(),
        }
    }

    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        match self {
            TlsConnKind::Server(c) => c.alpn_protocol(),
            TlsConnKind::Client(c) => c.alpn_protocol(),
        }
    }

    pub fn negotiated_cipher_suite(&self) -> Option<rustls::SupportedCipherSuite> {
        match self {
            TlsConnKind::Server(c) => c.negotiated_cipher_suite(),
            TlsConnKind::Client(c) => c.negotiated_cipher_suite(),
        }
    }

    pub fn protocol_version(&self) -> Option<rustls::ProtocolVersion> {
        match self {
            TlsConnKind::Server(c) => c.protocol_version(),
            TlsConnKind::Client(c) => c.protocol_version(),
        }
    }

    pub fn sni_hostname(&self) -> Option<&str> {
        match self {
            TlsConnKind::Server(c) => c.server_name(),
            TlsConnKind::Client(_) => None,
        }
    }

    pub fn send_close_notify(&mut self) {
        match self {
            TlsConnKind::Server(c) => c.send_close_notify(),
            TlsConnKind::Client(c) => c.send_close_notify(),
        }
    }
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
            conn: TlsConnKind::Server(conn),
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
            conn: TlsConnKind::Client(conn),
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
    #[cfg(has_io_uring)]
    pub fn send_close_notify_queued(
        &mut self,
        conn_index: u32,
        generation: u32,
        send_copy_pool: &mut SendCopyPool,
        out: &mut Vec<crate::handler::BuiltSend>,
    ) {
        if let Some(tls_conn) = self.get_mut(conn_index) {
            tls_conn.conn.send_close_notify();
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
    let mut reader = tls_conn.conn.reader();
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
