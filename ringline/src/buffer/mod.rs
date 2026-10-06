pub mod fixed;
pub(crate) mod prefault;
pub mod send_copy;
#[cfg(has_io_uring)]
pub mod send_slab;

/// The most receive buffers one `forward_held` call forwards. On io_uring
/// that is the number of held provided buffers gathered into its one
/// `sendmsg`; on mio, which holds bytes rather than buffers, a call forwards
/// at most this many buffers' worth of bytes (`recv_buffer` size each), so
/// one call moves at most the same amount on both backends.
pub(crate) const FORWARD_HELD_MAX_BUFFERS: usize = 32;
