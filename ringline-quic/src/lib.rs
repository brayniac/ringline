//! ringline-quic — QUIC protocol support via quinn-proto.
//!
//! This crate wraps [quinn-proto](https://docs.rs/quinn-proto)'s sans-IO QUIC
//! state machine and exposes an event-based [`QuicEndpoint`] API.
//!
//! # Example
//!
//! ```rust,ignore
//! use ringline_quic::{QuicConfig, QuicEndpoint, QuicEvent, TlsConfig};
//!
//! let tls = TlsConfig::builder().cert(chain, key).build()?;
//! let config = QuicConfig::builder().server(tls).alpn(&[b"h3"]).build()?;
//! let mut quic = QuicEndpoint::new(config, local_addr);
//!
//! // Read the clock once per event-loop iteration and pass it to every call.
//! let now = Instant::now();
//! // Feed incoming datagrams:
//! quic.handle_datagram(now, data, peer);
//! while let Some(event) = quic.poll_event() {
//!     match event {
//!         QuicEvent::NewConnection(conn) => { /* ... */ }
//!         QuicEvent::StreamReadable { conn, stream } => {
//!             let mut buf = [0u8; 4096];
//!             let (n, fin) = quic.stream_recv(now, conn, stream, &mut buf)?;
//!             quic.stream_send(conn, stream, &buf[..n])?;
//!         }
//!         _ => {}
//!     }
//! }
//! // Drain outgoing packets:
//! while let Some(pkt) = quic.poll_send() {
//!     match pkt.segment_size {
//!         Some(seg) => udp.send_to_gso(pkt.destination, &pkt.data, seg)?,
//!         None => udp.send_to(pkt.destination, &pkt.data)?,
//!     }
//! }
//! ```

pub mod config;
pub mod endpoint;
pub mod error;
pub mod event;
pub mod types;

pub use config::{ConfigError, QuicConfig, QuicConfigBuilder};
pub use endpoint::{BatchGuard, Datagrams, OutgoingPacket, QuicEndpoint};
pub use error::Error;
pub use event::{QuicConnId, QuicEvent};
pub use types::{
    ConnectError, ConnectionError, ConnectionStats, Dir, ReadError, ReadableError, Side, StreamId,
    VarInt, VarIntBoundsExceeded, WriteError,
};

/// The TLS configuration types [`QuicConfigBuilder`] takes.
pub use ringline_tls_config::{TlsClientConfig, TlsConfig};

/// The quinn-proto crate ringline-quic is built on, for
/// [`QuicConfig::from_proto`]. Items reached through it follow quinn-proto's
/// version, not ringline-quic's.
pub use quinn_proto as proto;

/// The TLS configuration crate, including its rustls re-export.
pub use ringline_tls_config as tls_config;
