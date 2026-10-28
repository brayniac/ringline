//! Stream identifiers, integers, errors and statistics that ringline-quic
//! owns, so its public API does not name quinn-proto types.

use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;

/// The largest value a QUIC variable-length integer holds, 2^62 - 1.
const VARINT_MAX: u64 = (1 << 62) - 1;

/// A QUIC variable-length integer: a value in `0..2^62`. Stream error codes
/// and application close codes are `VarInt`s.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct VarInt(u64);

impl VarInt {
    /// The largest `VarInt`, 2^62 - 1.
    pub const MAX: Self = Self(VARINT_MAX);

    /// The `VarInt` for a `u32`. Every `u32` fits.
    pub const fn from_u32(value: u32) -> Self {
        Self(value as u64)
    }

    /// The `VarInt` for a `u64`, or an error if `value` is 2^62 or more.
    pub fn from_u64(value: u64) -> Result<Self, VarIntBoundsExceeded> {
        if value <= VARINT_MAX {
            Ok(Self(value))
        } else {
            Err(VarIntBoundsExceeded)
        }
    }

    /// The value.
    pub const fn into_inner(self) -> u64 {
        self.0
    }

    pub(crate) fn to_proto(self) -> quinn_proto::VarInt {
        // In range by construction.
        quinn_proto::VarInt::from_u64(self.0).unwrap_or(quinn_proto::VarInt::MAX)
    }

    pub(crate) fn from_proto(v: quinn_proto::VarInt) -> Self {
        Self(v.into_inner())
    }
}

impl From<u8> for VarInt {
    fn from(value: u8) -> Self {
        Self(value.into())
    }
}

impl From<u16> for VarInt {
    fn from(value: u16) -> Self {
        Self(value.into())
    }
}

impl From<u32> for VarInt {
    fn from(value: u32) -> Self {
        Self(value.into())
    }
}

impl TryFrom<u64> for VarInt {
    type Error = VarIntBoundsExceeded;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::from_u64(value)
    }
}

impl From<VarInt> for u64 {
    fn from(value: VarInt) -> Self {
        value.0
    }
}

impl fmt::Display for VarInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// [`VarInt::from_u64`] was given a value of 2^62 or more.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("value is too large for a QUIC variable-length integer")]
pub struct VarIntBoundsExceeded;

/// Whether a stream carries data in both directions or one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Dir {
    /// Bidirectional.
    Bi,
    /// Unidirectional.
    Uni,
}

impl Dir {
    pub(crate) fn to_proto(self) -> quinn_proto::Dir {
        match self {
            Self::Bi => quinn_proto::Dir::Bi,
            Self::Uni => quinn_proto::Dir::Uni,
        }
    }

    pub(crate) fn from_proto(dir: quinn_proto::Dir) -> Self {
        match dir {
            quinn_proto::Dir::Bi => Self::Bi,
            quinn_proto::Dir::Uni => Self::Uni,
        }
    }
}

/// Which endpoint opened a stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Side {
    /// The client.
    Client,
    /// The server.
    Server,
}

/// A QUIC stream identifier (RFC 9000 §2.1): the stream's index, direction
/// and initiator packed into one integer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(u64);

impl StreamId {
    /// The stream with the given initiator, direction and index.
    pub fn new(initiator: Side, dir: Dir, index: u64) -> Self {
        let side = match initiator {
            Side::Client => 0,
            Side::Server => 1,
        };
        let dir = match dir {
            Dir::Bi => 0,
            Dir::Uni => 2,
        };
        Self((index << 2) | dir | side)
    }

    /// Which endpoint opened the stream.
    pub fn initiator(self) -> Side {
        if self.0 & 1 == 0 {
            Side::Client
        } else {
            Side::Server
        }
    }

    /// The stream's direction.
    pub fn dir(self) -> Dir {
        if self.0 & 2 == 0 { Dir::Bi } else { Dir::Uni }
    }

    /// The stream's index among the streams of its initiator and direction.
    pub fn index(self) -> u64 {
        self.0 >> 2
    }

    /// The stream identifier as it appears on the wire.
    pub fn into_inner(self) -> u64 {
        self.0
    }

    pub(crate) fn to_proto(self) -> quinn_proto::StreamId {
        let side = match self.initiator() {
            Side::Client => quinn_proto::Side::Client,
            Side::Server => quinn_proto::Side::Server,
        };
        quinn_proto::StreamId::new(side, self.dir().to_proto(), self.index())
    }

    pub(crate) fn from_proto(id: quinn_proto::StreamId) -> Self {
        Self(quinn_proto::VarInt::from(id).into_inner())
    }
}

impl From<StreamId> for u64 {
    fn from(id: StreamId) -> Self {
        id.0
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let initiator = match self.initiator() {
            Side::Client => "client",
            Side::Server => "server",
        };
        let dir = match self.dir() {
            Dir::Bi => "bidirectional",
            Dir::Uni => "unidirectional",
        };
        write!(f, "{initiator} {dir} stream {}", self.index())
    }
}

/// Why [`QuicEndpoint::connect`](crate::QuicEndpoint::connect) could not
/// start a connection.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectError {
    /// The endpoint can no longer create connections.
    #[error("endpoint stopping")]
    EndpointStopping,
    /// Not enough connection IDs are available.
    #[error("connection IDs exhausted")]
    CidsExhausted,
    /// The server name is malformed.
    #[error("invalid server name: {0}")]
    InvalidServerName(String),
    /// The remote address is malformed, for example port 0.
    #[error("invalid remote address: {0}")]
    InvalidRemoteAddress(SocketAddr),
    /// The endpoint has no client configuration.
    #[error("no client configuration")]
    NoClientConfig,
    /// The endpoint does not support the configured QUIC version.
    #[error("unsupported QUIC version")]
    UnsupportedVersion,
}

impl ConnectError {
    pub(crate) fn from_proto(e: quinn_proto::ConnectError) -> Self {
        use quinn_proto::ConnectError as P;
        match e {
            P::EndpointStopping => Self::EndpointStopping,
            P::CidsExhausted => Self::CidsExhausted,
            P::InvalidServerName(name) => Self::InvalidServerName(name),
            P::InvalidRemoteAddress(addr) => Self::InvalidRemoteAddress(addr),
            P::NoDefaultClientConfig => Self::NoClientConfig,
            P::UnsupportedVersion => Self::UnsupportedVersion,
        }
    }
}

/// Why a connection closed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectionError {
    /// The peer does not implement a QUIC version this endpoint supports.
    #[error("peer does not support a QUIC version this endpoint supports")]
    VersionMismatch,
    /// The peer violated the QUIC protocol. `code` is the QUIC transport
    /// error code.
    #[error("transport error {code:#x}: {reason}")]
    TransportError {
        /// The QUIC transport error code.
        code: u64,
        /// A description of the violation.
        reason: String,
    },
    /// The peer closed the connection with a QUIC transport error code.
    #[error("closed by peer: transport error {code:#x}")]
    ConnectionClosed {
        /// The QUIC transport error code.
        code: u64,
        /// The reason the peer sent.
        reason: Bytes,
    },
    /// The peer's application closed the connection.
    #[error("closed by peer application: code {code}")]
    ApplicationClosed {
        /// The application's error code.
        code: VarInt,
        /// The reason the peer sent.
        reason: Bytes,
    },
    /// The peer reset the connection with a stateless reset.
    #[error("reset by peer")]
    Reset,
    /// The connection was idle for longer than the idle timeout.
    #[error("timed out")]
    TimedOut,
    /// This endpoint closed the connection.
    #[error("closed")]
    LocallyClosed,
    /// The connection ran out of connection IDs.
    #[error("connection IDs exhausted")]
    CidsExhausted,
}

impl ConnectionError {
    pub(crate) fn from_proto(e: quinn_proto::ConnectionError) -> Self {
        use quinn_proto::ConnectionError as P;
        match e {
            P::VersionMismatch => Self::VersionMismatch,
            P::TransportError(t) => Self::TransportError {
                code: u64::from(t.code),
                reason: t.reason,
            },
            P::ConnectionClosed(c) => Self::ConnectionClosed {
                code: u64::from(c.error_code),
                reason: c.reason,
            },
            P::ApplicationClosed(c) => Self::ApplicationClosed {
                code: VarInt::from_proto(c.error_code),
                reason: c.reason,
            },
            P::Reset => Self::Reset,
            P::TimedOut => Self::TimedOut,
            P::LocallyClosed => Self::LocallyClosed,
            P::CidsExhausted => Self::CidsExhausted,
        }
    }
}

/// Why a stream read failed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ReadError {
    /// No data is available now. A
    /// [`QuicEvent::StreamReadable`](crate::QuicEvent::StreamReadable)
    /// follows when more arrives.
    #[error("blocked")]
    Blocked,
    /// The peer reset the stream with this application error code.
    #[error("reset by peer: code {0}")]
    Reset(VarInt),
}

impl ReadError {
    pub(crate) fn from_proto(e: quinn_proto::ReadError) -> Self {
        match e {
            quinn_proto::ReadError::Blocked => Self::Blocked,
            quinn_proto::ReadError::Reset(code) => Self::Reset(VarInt::from_proto(code)),
        }
    }
}

/// Why a stream could not be opened for reading.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ReadableError {
    /// The stream is not open, or was already stopped, finished or reset.
    #[error("closed stream")]
    ClosedStream,
    /// An ordered read followed an unordered one.
    #[error("ordered read after unordered read")]
    IllegalOrderedRead,
}

impl ReadableError {
    pub(crate) fn from_proto(e: quinn_proto::ReadableError) -> Self {
        match e {
            quinn_proto::ReadableError::ClosedStream => Self::ClosedStream,
            quinn_proto::ReadableError::IllegalOrderedRead => Self::IllegalOrderedRead,
        }
    }
}

/// Why a stream write failed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum WriteError {
    /// The peer cannot accept more data now. A
    /// [`QuicEvent::StreamWritable`](crate::QuicEvent::StreamWritable)
    /// follows when it can.
    #[error("unable to accept further writes")]
    Blocked,
    /// The peer stopped the stream with this application error code.
    #[error("stopped by peer: code {0}")]
    Stopped(VarInt),
    /// The stream is not open, or was already finished or reset.
    #[error("closed stream")]
    ClosedStream,
}

impl WriteError {
    pub(crate) fn from_proto(e: quinn_proto::WriteError) -> Self {
        match e {
            quinn_proto::WriteError::Blocked => Self::Blocked,
            quinn_proto::WriteError::Stopped(code) => Self::Stopped(VarInt::from_proto(code)),
            quinn_proto::WriteError::ClosedStream => Self::ClosedStream,
        }
    }
}

/// A snapshot of a connection's statistics, from
/// [`QuicEndpoint::connection_stats`](crate::QuicEndpoint::connection_stats).
#[derive(Clone, Copy, Debug)]
pub struct ConnectionStats {
    rtt: Duration,
    cwnd: u64,
    congestion_events: u64,
    current_mtu: u16,
    sent_packets: u64,
    lost_packets: u64,
    lost_bytes: u64,
    udp_tx_datagrams: u64,
    udp_tx_bytes: u64,
    udp_rx_datagrams: u64,
    udp_rx_bytes: u64,
}

impl ConnectionStats {
    pub(crate) fn from_proto(s: &quinn_proto::ConnectionStats) -> Self {
        Self {
            rtt: s.path.rtt,
            cwnd: s.path.cwnd,
            congestion_events: s.path.congestion_events,
            current_mtu: s.path.current_mtu,
            sent_packets: s.path.sent_packets,
            lost_packets: s.path.lost_packets,
            lost_bytes: s.path.lost_bytes,
            udp_tx_datagrams: s.udp_tx.datagrams,
            udp_tx_bytes: s.udp_tx.bytes,
            udp_rx_datagrams: s.udp_rx.datagrams,
            udp_rx_bytes: s.udp_rx.bytes,
        }
    }

    /// The current smoothed round-trip time estimate.
    pub fn rtt(&self) -> Duration {
        self.rtt
    }

    /// The congestion window, in bytes.
    pub fn cwnd(&self) -> u64 {
        self.cwnd
    }

    /// How many congestion events the connection has had.
    pub fn congestion_events(&self) -> u64 {
        self.congestion_events
    }

    /// The current path MTU, in bytes.
    pub fn current_mtu(&self) -> u16 {
        self.current_mtu
    }

    /// Packets sent.
    pub fn sent_packets(&self) -> u64 {
        self.sent_packets
    }

    /// Packets declared lost.
    pub fn lost_packets(&self) -> u64 {
        self.lost_packets
    }

    /// Bytes declared lost.
    pub fn lost_bytes(&self) -> u64 {
        self.lost_bytes
    }

    /// UDP datagrams sent.
    pub fn udp_tx_datagrams(&self) -> u64 {
        self.udp_tx_datagrams
    }

    /// UDP payload bytes sent.
    pub fn udp_tx_bytes(&self) -> u64 {
        self.udp_tx_bytes
    }

    /// UDP datagrams received.
    pub fn udp_rx_datagrams(&self) -> u64 {
        self.udp_rx_datagrams
    }

    /// UDP payload bytes received.
    pub fn udp_rx_bytes(&self) -> u64 {
        self.udp_rx_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_id_round_trips_through_quinn() {
        for side in [Side::Client, Side::Server] {
            for dir in [Dir::Bi, Dir::Uni] {
                for index in [0, 1, 7, 1 << 40] {
                    let ours = StreamId::new(side, dir, index);
                    assert_eq!(ours.initiator(), side);
                    assert_eq!(ours.dir(), dir);
                    assert_eq!(ours.index(), index);
                    let theirs = ours.to_proto();
                    assert_eq!(
                        quinn_proto::VarInt::from(theirs).into_inner(),
                        ours.into_inner()
                    );
                    assert_eq!(StreamId::from_proto(theirs), ours);
                }
            }
        }
        // RFC 9000 §2.1: client bidi 0, server bidi 1, client uni 2, server uni 3.
        assert_eq!(StreamId::new(Side::Client, Dir::Bi, 0).into_inner(), 0);
        assert_eq!(StreamId::new(Side::Server, Dir::Bi, 0).into_inner(), 1);
        assert_eq!(StreamId::new(Side::Client, Dir::Uni, 0).into_inner(), 2);
        assert_eq!(StreamId::new(Side::Server, Dir::Uni, 1).into_inner(), 7);
    }

    #[test]
    fn varint_bounds() {
        assert_eq!(VarInt::from_u64(VARINT_MAX).unwrap(), VarInt::MAX);
        assert_eq!(VarInt::from_u64(VARINT_MAX + 1), Err(VarIntBoundsExceeded));
        assert_eq!(VarInt::MAX.to_proto(), quinn_proto::VarInt::MAX);
        assert_eq!(VarInt::from_u32(7).into_inner(), 7);
    }
}
