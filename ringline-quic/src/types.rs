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
    /// The stream with the given initiator, direction and index. `index`
    /// is below 2^60, the most streams of one kind a connection can open;
    /// higher bits are dropped.
    pub fn new(initiator: Side, dir: Dir, index: u64) -> Self {
        let index = index & ((1 << 60) - 1);
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

impl TryFrom<u64> for StreamId {
    type Error = VarIntBoundsExceeded;
    /// The stream with this wire identifier, which must be below 2^62.
    fn try_from(id: u64) -> Result<Self, Self::Error> {
        VarInt::from_u64(id).map(|v| Self(v.into_inner()))
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
    /// The connection failed with a QUIC transport error, raised by either
    /// endpoint, including TLS handshake failures (codes 0x100–0x1ff).
    /// `code` is the QUIC transport error code.
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

/// Defines an opaque statistics struct copied field by field from its
/// quinn-proto counterpart, with one accessor per field.
macro_rules! stats_struct {
    (
        $(#[$meta:meta])*
        $name:ident from $proto:path {
            $( $(#[$fmeta:meta])* $field:ident : $ty:ty ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Default)]
        pub struct $name {
            $( $field: $ty, )*
        }

        impl $name {
            pub(crate) fn from_proto(s: &$proto) -> Self {
                Self { $( $field: s.$field, )* }
            }

            $(
                $(#[$fmeta])*
                pub fn $field(&self) -> $ty {
                    self.$field
                }
            )*
        }
    };
}

stats_struct! {
    /// UDP traffic in one direction.
    UdpStats from quinn_proto::UdpStats {
        /// Datagrams.
        datagrams: u64,
        /// Payload bytes.
        bytes: u64,
        /// System calls (more than one datagram can go in one with GSO or GRO).
        ios: u64,
    }
}

stats_struct! {
    /// QUIC frames in one direction, counted by frame type.
    FrameStats from quinn_proto::FrameStats {
        /// ACK frames.
        acks: u64,
        /// ACK_FREQUENCY frames.
        ack_frequency: u64,
        /// CRYPTO frames.
        crypto: u64,
        /// CONNECTION_CLOSE frames.
        connection_close: u64,
        /// DATA_BLOCKED frames.
        data_blocked: u64,
        /// DATAGRAM frames.
        datagram: u64,
        /// HANDSHAKE_DONE frames.
        handshake_done: u8,
        /// IMMEDIATE_ACK frames.
        immediate_ack: u64,
        /// MAX_DATA frames.
        max_data: u64,
        /// MAX_STREAM_DATA frames.
        max_stream_data: u64,
        /// MAX_STREAMS frames for bidirectional streams.
        max_streams_bidi: u64,
        /// MAX_STREAMS frames for unidirectional streams.
        max_streams_uni: u64,
        /// NEW_CONNECTION_ID frames.
        new_connection_id: u64,
        /// NEW_TOKEN frames.
        new_token: u64,
        /// PATH_CHALLENGE frames.
        path_challenge: u64,
        /// PATH_RESPONSE frames.
        path_response: u64,
        /// PING frames.
        ping: u64,
        /// RESET_STREAM frames.
        reset_stream: u64,
        /// RETIRE_CONNECTION_ID frames.
        retire_connection_id: u64,
        /// STREAM_DATA_BLOCKED frames.
        stream_data_blocked: u64,
        /// STREAMS_BLOCKED frames for bidirectional streams.
        streams_blocked_bidi: u64,
        /// STREAMS_BLOCKED frames for unidirectional streams.
        streams_blocked_uni: u64,
        /// STOP_SENDING frames.
        stop_sending: u64,
        /// STREAM frames.
        stream: u64,
    }
}

stats_struct! {
    /// The network path's statistics.
    PathStats from quinn_proto::PathStats {
        /// The current smoothed round-trip time estimate.
        rtt: Duration,
        /// The smallest round-trip time measured.
        min_rtt: Duration,
        /// The congestion window, in bytes.
        cwnd: u64,
        /// Congestion events.
        congestion_events: u64,
        /// Packets declared lost.
        lost_packets: u64,
        /// Bytes declared lost.
        lost_bytes: u64,
        /// Packets sent.
        sent_packets: u64,
        /// Path MTU discovery probes sent.
        sent_plpmtud_probes: u64,
        /// Path MTU discovery probes declared lost.
        lost_plpmtud_probes: u64,
        /// Times the path MTU was found to have shrunk.
        black_holes_detected: u64,
        /// The current path MTU, in bytes.
        current_mtu: u16,
    }
}

/// A snapshot of a connection's statistics, from
/// [`QuicEndpoint::connection_stats`](crate::QuicEndpoint::connection_stats).
/// It holds every field of quinn-proto's `ConnectionStats`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ConnectionStats {
    udp_tx: UdpStats,
    udp_rx: UdpStats,
    frame_tx: FrameStats,
    frame_rx: FrameStats,
    path: PathStats,
}

impl ConnectionStats {
    pub(crate) fn from_proto(s: &quinn_proto::ConnectionStats) -> Self {
        Self {
            udp_tx: UdpStats::from_proto(&s.udp_tx),
            udp_rx: UdpStats::from_proto(&s.udp_rx),
            frame_tx: FrameStats::from_proto(&s.frame_tx),
            frame_rx: FrameStats::from_proto(&s.frame_rx),
            path: PathStats::from_proto(&s.path),
        }
    }

    /// UDP traffic sent.
    pub fn udp_tx(&self) -> UdpStats {
        self.udp_tx
    }

    /// UDP traffic received.
    pub fn udp_rx(&self) -> UdpStats {
        self.udp_rx
    }

    /// Frames sent.
    pub fn frame_tx(&self) -> FrameStats {
        self.frame_tx
    }

    /// Frames received.
    pub fn frame_rx(&self) -> FrameStats {
        self.frame_rx
    }

    /// The network path's statistics: RTT, congestion window, loss, MTU.
    pub fn path(&self) -> PathStats {
        self.path
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
    fn stream_id_display_and_wire_form() {
        assert_eq!(
            StreamId::new(Side::Client, Dir::Bi, 3).to_string(),
            "client bidirectional stream 3"
        );
        assert_eq!(
            StreamId::new(Side::Server, Dir::Uni, 0).to_string(),
            "server unidirectional stream 0"
        );
        let id = StreamId::try_from(7).unwrap();
        assert_eq!(
            (id.initiator(), id.dir(), id.index()),
            (Side::Server, Dir::Uni, 1)
        );
        assert!(StreamId::try_from(1u64 << 62).is_err());
    }

    #[test]
    fn connection_error_conversion() {
        let transport = quinn_proto::TransportError {
            code: quinn_proto::TransportErrorCode::crypto(0x2a),
            frame: None,
            reason: "bad certificate".into(),
        };
        assert_eq!(
            ConnectionError::from_proto(quinn_proto::ConnectionError::TransportError(transport)),
            ConnectionError::TransportError {
                code: 0x12a,
                reason: "bad certificate".into(),
            }
        );
        let close = quinn_proto::ConnectionClose {
            error_code: quinn_proto::TransportErrorCode::crypto(0x28),
            frame_type: None,
            reason: Bytes::from_static(b"handshake"),
        };
        assert_eq!(
            ConnectionError::from_proto(quinn_proto::ConnectionError::ConnectionClosed(close)),
            ConnectionError::ConnectionClosed {
                code: 0x128,
                reason: Bytes::from_static(b"handshake"),
            }
        );
        let app = quinn_proto::ApplicationClose {
            error_code: quinn_proto::VarInt::from_u32(0x99),
            reason: Bytes::from_static(b"bye"),
        };
        assert_eq!(
            ConnectionError::from_proto(quinn_proto::ConnectionError::ApplicationClosed(app)),
            ConnectionError::ApplicationClosed {
                code: VarInt::from_u32(0x99),
                reason: Bytes::from_static(b"bye"),
            }
        );
        for (theirs, ours) in [
            (
                quinn_proto::ConnectionError::VersionMismatch,
                ConnectionError::VersionMismatch,
            ),
            (quinn_proto::ConnectionError::Reset, ConnectionError::Reset),
            (
                quinn_proto::ConnectionError::TimedOut,
                ConnectionError::TimedOut,
            ),
            (
                quinn_proto::ConnectionError::LocallyClosed,
                ConnectionError::LocallyClosed,
            ),
            (
                quinn_proto::ConnectionError::CidsExhausted,
                ConnectionError::CidsExhausted,
            ),
        ] {
            assert_eq!(ConnectionError::from_proto(theirs), ours);
        }
    }

    #[test]
    fn varint_bounds() {
        assert_eq!(VarInt::from_u64(VARINT_MAX).unwrap(), VarInt::MAX);
        assert_eq!(VarInt::from_u64(VARINT_MAX + 1), Err(VarIntBoundsExceeded));
        assert_eq!(VarInt::MAX.to_proto(), quinn_proto::VarInt::MAX);
        assert_eq!(VarInt::from_u32(7).into_inner(), 7);
    }
}
