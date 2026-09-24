//! Moving a live connection between workers (tier 3, #443).
//!
//! Backend-neutral on purpose. The payload holds nothing io_uring-specific,
//! and `Config` has to name it on every backend — but park itself exists only
//! on io_uring, because it repairs a placement imbalance only merged accept
//! mode creates. On mio the channels are simply never populated, exactly as
//! `peer_accept` is empty in pool mode.

/// A connection lifted off this worker and ready to hand to another
/// (tier 3, #443). Every field is owned and `Send`.
///
/// The fd is a *second* reference to the socket, obtained via
/// `IORING_OP_FIXED_FD_INSTALL`. That is what lets the ordinary teardown path
/// run on the parking worker — closing the fixed-file entry drops per-worker
/// state without sending a FIN, because this handle keeps the socket alive.
// On mio nothing reads these: park is io_uring-only, but `Config` names the
// type on both backends.
#[cfg_attr(not(has_io_uring), allow(dead_code))]
pub(crate) struct ParkedFd {
    pub fd: std::os::fd::OwnedFd,
    pub listener: crate::ListenerId,
    pub peer: crate::connection::PeerAddr,
    /// Received but unconsumed bytes, in wire order.
    pub pending: Vec<bytes::Bytes>,
    /// Worker index this connection is bound for.
    pub target: usize,
}
