use std::fmt;
use std::path::PathBuf;

/// Peer address for a connection — either TCP (IPv4/IPv6) or Unix domain socket.
#[derive(Debug, Clone)]
pub enum PeerAddr {
    /// TCP peer address (IPv4 or IPv6).
    Tcp(std::net::SocketAddr),
    /// Unix domain socket path. Empty path for unnamed/abstract sockets.
    Unix(PathBuf),
}

impl fmt::Display for PeerAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerAddr::Tcp(addr) => write!(f, "{addr}"),
            PeerAddr::Unix(path) => {
                if path.as_os_str().is_empty() {
                    f.write_str("(unnamed)")
                } else {
                    write!(f, "{}", path.display())
                }
            }
        }
    }
}

/// Which receive operation the driver currently has armed. Mechanism, not
/// lifecycle: `Idle` says nothing about whether more data will arrive —
/// read [`ConnectionState::recv_finished`] for that.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvArm {
    /// Nothing armed: slot inactive, connect in flight, recv cancelled, or
    /// the multishot self-terminated and has not been re-armed.
    Idle,
    /// Multishot recv with the provided buffer ring (io_uring) / readable
    /// interest (mio).
    Multi,
    /// Multishot recvmsg with cmsg timestamps (io_uring, `timestamps`).
    #[cfg(feature = "timestamps")]
    MsgMulti,
}

/// The TCP read half as this end observed it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadHalf {
    /// The peer may still send.
    Open,
    /// The peer's FIN arrived. `truncated` is the TLS case where the FIN
    /// came without a preceding close_notify: a length- or
    /// delimiter-framed protocol should treat it as an error.
    Eof { truncated: bool },
    /// A socket read failed. The exact error, if a task wants it, is in
    /// `Executor::recv_errors` (generation-tagged; see `with_data_result`).
    Error,
    /// The application cancelled the pending receive (`DriverCtx::cancel`;
    /// io_uring only — mio's `cancel` is `Unsupported`).
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    Cancelled,
}

/// Where the connection is in its life.
///
/// Only the driver's `close_connection` and the `DriverCtx` close move a
/// connection to `Closing`, on both backends; teardown then finalizes once
/// queued sends drain. The sub-states of `Closing` live on
/// `ConnSendState`: `close_pending` (waiting for the drain) and
/// `close_submitted` (io_uring: Close SQE committed, no new SQEs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    /// Slot not allocated.
    Inactive,
    /// Outbound connect in flight; no recv armed yet.
    Connecting,
    /// Accepted or connected; the task may send and receive.
    Open,
    /// Teardown requested; finalizing once queued sends drain.
    Closing,
}

/// Per-connection state tracked by the driver.
pub struct ConnectionState {
    /// Which receive operation is armed (mechanism; see `recv_finished`).
    pub recv_arm: RecvArm,
    /// The TCP read half as observed by this end.
    pub read: ReadHalf,
    /// Where the connection is in its life.
    pub lifecycle: Lifecycle,
    /// Whether the connection is active.
    pub active: bool,
    /// Generation counter to detect stale ConnTokens.
    ///
    /// `u32` wraps after 2^32 close/reuse cycles on the same slot — at a
    /// sustained 10 000 reuses per second per slot that's ~5 days, at
    /// realistic per-slot reuse rates many years. Widening to `u64`
    /// cascades into a clippy `large_enum_variant` warning in downstream
    /// protocol crates whose `Client` structs carry a `ConnCtx` — if the
    /// wrap risk becomes practical, widen *and* box the affected
    /// downstream variants in the same change.
    pub generation: u32,
    /// Whether this is an outbound (connect) connection.
    pub outbound: bool,
    /// Whether the connection has been fully established (TCP+TLS handshake done).
    /// `on_close` is only fired when `established == true`.
    pub established: bool,
    /// Peer address (set on accept or connect).
    pub peer_addr: Option<PeerAddr>,
    /// Whether a connect timeout SQE is armed for this connection.
    pub connect_timeout_armed: bool,
    /// Most recent kernel RX timestamp (nanoseconds since epoch, CLOCK_REALTIME).
    /// Set when a `RecvMsgMulti` completion delivers a `SCM_TIMESTAMPING` cmsg.
    #[cfg(feature = "timestamps")]
    pub recv_timestamp_ns: u64,
    /// When true, `handle_recv_multi` echoes received data directly from the CQE
    /// handler instead of waking the connection's task. This eliminates the
    /// task-wakeup overhead (collect_wakeups → poll_ready_tasks) on the hot echo
    /// path, reducing per-message latency from ~2 event-loop iterations to ~1.
    #[cfg(has_io_uring)]
    pub direct_echo: bool,
    /// Whether a plain multishot `RecvMulti` SQE is currently in-flight in the
    /// kernel (armed). Set when the multishot is (re-)armed; cleared when a
    /// completion arrives without `IORING_CQE_F_MORE` (the kernel terminated
    /// the multishot). On a proactive close this tells the close path it must
    /// cancel the still-armed recv so its reference on the socket file is
    /// dropped and the kernel actually sends the peer a FIN — closing the fixed
    /// descriptor alone does not, since the in-flight recv pins the socket.
    /// (Only tracked for the plain `RecvMulti` path, not the timestamped
    /// `RecvMsgMulti` path.)
    #[cfg(has_io_uring)]
    pub recv_multishot_armed: bool,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionState {
    pub fn new() -> Self {
        ConnectionState {
            recv_arm: RecvArm::Idle,
            read: ReadHalf::Open,
            lifecycle: Lifecycle::Inactive,
            active: false,
            generation: 0,
            outbound: false,
            established: false,
            peer_addr: None,
            connect_timeout_armed: false,
            #[cfg(feature = "timestamps")]
            recv_timestamp_ns: 0,
            #[cfg(has_io_uring)]
            direct_echo: false,
            #[cfg(has_io_uring)]
            recv_multishot_armed: false,
        }
    }

    pub fn activate(&mut self) {
        self.active = true;
        self.lifecycle = Lifecycle::Open;
        self.read = ReadHalf::Open;
        self.recv_arm = RecvArm::Multi;
    }

    /// Activate as an outbound (connect) connection.
    pub fn activate_outbound(&mut self) {
        self.active = true;
        self.outbound = true;
        self.established = false;
        self.lifecycle = Lifecycle::Connecting;
        self.read = ReadHalf::Open;
        self.recv_arm = RecvArm::Idle;
    }

    /// An outbound connect completed: the connection is open and its
    /// multishot recv is (about to be) armed.
    pub fn mark_connected(&mut self) {
        self.lifecycle = Lifecycle::Open;
        self.recv_arm = RecvArm::Multi;
    }

    /// A recv future must resolve to EOF instead of parking: the peer
    /// closed, the read failed, the receive was cancelled, or a close was
    /// requested.
    pub fn recv_finished(&self) -> bool {
        !matches!(self.read, ReadHalf::Open) || matches!(self.lifecycle, Lifecycle::Closing)
    }

    /// A close has already been requested; a second request is a no-op.
    pub fn close_requested(&self) -> bool {
        matches!(self.lifecycle, Lifecycle::Closing)
    }

    /// Record that the peer's FIN arrived. Idempotent; never downgrades a
    /// truncated EOF to a clean one.
    pub fn note_eof(&mut self, truncated: bool) {
        if !matches!(self.read, ReadHalf::Eof { truncated: true }) {
            self.read = ReadHalf::Eof { truncated };
        }
    }

    pub fn deactivate(&mut self) {
        self.active = false;
        self.lifecycle = Lifecycle::Inactive;
        self.read = ReadHalf::Open;
        self.recv_arm = RecvArm::Idle;
        self.outbound = false;
        self.established = false;
        self.peer_addr = None;
        self.connect_timeout_armed = false;
        #[cfg(feature = "timestamps")]
        {
            self.recv_timestamp_ns = 0;
        }
        #[cfg(has_io_uring)]
        {
            self.direct_echo = false;
            self.recv_multishot_armed = false;
        }
        self.generation = self.generation.wrapping_add(1);
    }
}

/// Manages connection slots with a free list for O(1) allocation.
pub struct ConnectionTable {
    slots: Vec<ConnectionState>,
    free_list: Vec<u32>,
}

impl ConnectionTable {
    pub fn new(max_connections: u32) -> Self {
        let mut slots = Vec::with_capacity(max_connections as usize);
        for _ in 0..max_connections {
            slots.push(ConnectionState::new());
        }
        // Free list: indices in reverse order so pop gives lowest first.
        let free_list: Vec<u32> = (0..max_connections).rev().collect();
        ConnectionTable { slots, free_list }
    }

    /// Allocate a connection slot. Returns the slot index.
    pub fn allocate(&mut self) -> Option<u32> {
        let idx = self.free_list.pop()?;
        self.slots[idx as usize].activate();
        Some(idx)
    }

    /// Allocate a connection slot for an outbound connection. Returns the slot index.
    pub fn allocate_outbound(&mut self) -> Option<u32> {
        let idx = self.free_list.pop()?;
        self.slots[idx as usize].activate_outbound();
        Some(idx)
    }

    /// Release a connection slot back to the free list.
    pub fn release(&mut self, idx: u32) {
        if (idx as usize) < self.slots.len() {
            if !self.slots[idx as usize].active {
                return; // Already released — avoid double-push to free list
            }
            self.slots[idx as usize].deactivate();
            self.free_list.push(idx);
        }
    }

    /// Get a reference to a connection's state.
    pub fn get(&self, idx: u32) -> Option<&ConnectionState> {
        self.slots.get(idx as usize).filter(|s| s.active)
    }

    /// Get a mutable reference to a connection's state.
    pub fn get_mut(&mut self, idx: u32) -> Option<&mut ConnectionState> {
        self.slots.get_mut(idx as usize).filter(|s| s.active)
    }

    /// Number of active connections.
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    pub fn active_count(&self) -> usize {
        self.slots.len().saturating_sub(self.free_list.len())
    }

    /// Total number of connection slots (max_connections).
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    pub fn max_slots(&self) -> u32 {
        self.slots.len() as u32
    }

    /// Get the generation for a slot (valid even if inactive).
    pub fn generation(&self, idx: u32) -> u32 {
        self.slots[idx as usize].generation
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_returns_indices_and_marks_active() {
        let mut table = ConnectionTable::new(4);
        assert_eq!(table.active_count(), 0);

        let idx = table.allocate().unwrap();
        assert_eq!(table.active_count(), 1);
        assert!(table.get(idx).is_some());
        assert!(table.get(idx).unwrap().active);
        let cs = table.get(idx).unwrap();
        assert!(cs.lifecycle == Lifecycle::Open && cs.recv_arm == RecvArm::Multi);
    }

    #[test]
    fn allocate_outbound_sets_connecting_mode() {
        let mut table = ConnectionTable::new(4);
        let idx = table.allocate_outbound().unwrap();

        let conn = table.get(idx).unwrap();
        assert!(conn.outbound);
        assert!(!conn.established);
        assert!(conn.lifecycle == Lifecycle::Connecting);
    }

    #[test]
    fn release_makes_slot_reusable() {
        let mut table = ConnectionTable::new(2);
        let idx0 = table.allocate().unwrap();
        let idx1 = table.allocate().unwrap();
        assert_eq!(table.active_count(), 2);
        assert!(table.allocate().is_none()); // full

        table.release(idx0);
        assert_eq!(table.active_count(), 1);
        assert!(table.get(idx0).is_none()); // no longer active

        // Can allocate again — gets the released slot.
        let idx_new = table.allocate().unwrap();
        assert_eq!(idx_new, idx0);
        assert_eq!(table.active_count(), 2);

        table.release(idx1);
        table.release(idx_new);
    }

    #[test]
    fn release_increments_generation() {
        let mut table = ConnectionTable::new(4);
        let idx = table.allocate().unwrap();
        assert_eq!(table.generation(idx), 0);

        table.release(idx);
        assert_eq!(table.generation(idx), 1);

        let idx2 = table.allocate().unwrap();
        assert_eq!(idx2, idx);
        assert_eq!(table.generation(idx), 1); // generation persists across reuse

        table.release(idx);
        assert_eq!(table.generation(idx), 2);
    }

    #[test]
    fn generation_wraps_at_u32_max() {
        let mut table = ConnectionTable::new(1);
        let idx = table.allocate().unwrap();

        // Manually set generation near max. At realistic per-slot reuse
        // rates this takes weeks to months to reach in practice; see the
        // doc-comment on `ConnectionState::generation` for the trade-off
        // with downstream protocol crate enum sizes.
        table.slots[idx as usize].generation = u32::MAX;
        table.release(idx);
        assert_eq!(table.generation(idx), 0); // wraps to 0
    }

    #[test]
    fn double_release_is_no_op() {
        let mut table = ConnectionTable::new(4);
        let idx = table.allocate().unwrap();
        let gen_before = table.generation(idx);

        table.release(idx);
        let gen_after = table.generation(idx);
        assert_eq!(gen_after, gen_before + 1);

        // Second release: already inactive, should be no-op.
        table.release(idx);
        assert_eq!(table.generation(idx), gen_after); // generation unchanged
        assert_eq!(table.active_count(), 0);

        // Free list should have exactly max_slots entries (no double-push).
        let idx0 = table.allocate().unwrap();
        let idx1 = table.allocate().unwrap();
        let idx2 = table.allocate().unwrap();
        let idx3 = table.allocate().unwrap();
        assert!(table.allocate().is_none()); // exactly 4 slots, all used
        table.release(idx0);
        table.release(idx1);
        table.release(idx2);
        table.release(idx3);
    }

    #[test]
    fn get_returns_none_for_inactive_slot() {
        let mut table = ConnectionTable::new(4);
        // Unallocated slot.
        assert!(table.get(0).is_none());

        let idx = table.allocate().unwrap();
        assert!(table.get(idx).is_some());

        table.release(idx);
        assert!(table.get(idx).is_none());
    }

    #[test]
    fn get_returns_none_for_out_of_bounds() {
        let table = ConnectionTable::new(4);
        assert!(table.get(99).is_none());
    }

    #[test]
    fn release_out_of_bounds_is_no_op() {
        let mut table = ConnectionTable::new(4);
        // Should not panic.
        table.release(99);
        assert_eq!(table.active_count(), 0);
    }

    #[test]
    fn exhaust_all_slots() {
        let mut table = ConnectionTable::new(3);
        let a = table.allocate().unwrap();
        let b = table.allocate().unwrap();
        let c = table.allocate().unwrap();
        assert!(table.allocate().is_none());
        assert_eq!(table.active_count(), 3);

        table.release(b);
        assert_eq!(table.active_count(), 2);

        let d = table.allocate().unwrap();
        assert_eq!(d, b); // reuses released slot
        assert_eq!(table.active_count(), 3);
        assert!(table.allocate().is_none());

        table.release(a);
        table.release(c);
        table.release(d);
    }

    #[test]
    fn deactivate_resets_all_fields() {
        let mut table = ConnectionTable::new(4);
        let idx = table.allocate_outbound().unwrap();

        // Simulate connection becoming established.
        if let Some(cs) = table.get_mut(idx) {
            cs.established = true;
            cs.connect_timeout_armed = true;
            cs.peer_addr = Some(PeerAddr::Tcp("127.0.0.1:8080".parse().unwrap()));
        }

        table.release(idx);

        // After release, all fields should be reset.
        let cs = &table.slots[idx as usize];
        assert!(!cs.active);
        assert!(!cs.outbound);
        assert!(!cs.established);
        assert!(!cs.connect_timeout_armed);
        assert!(cs.peer_addr.is_none());
        assert!(cs.lifecycle == Lifecycle::Inactive);
    }

    #[test]
    fn recv_finished_and_close_requested_follow_the_table() {
        let mut cs = ConnectionState::new();
        assert!(!cs.close_requested());
        cs.activate();
        assert!(!cs.recv_finished(), "open connection parks on recv");
        cs.note_eof(false);
        assert!(cs.recv_finished(), "peer FIN finishes the read half");
        assert!(!cs.close_requested(), "FIN alone does not request a close");
        cs.lifecycle = Lifecycle::Closing;
        assert!(cs.close_requested());

        let mut cs = ConnectionState::new();
        cs.activate();
        cs.lifecycle = Lifecycle::Closing;
        assert!(
            cs.recv_finished(),
            "a requested close finishes the read half"
        );

        for read in [ReadHalf::Error, ReadHalf::Cancelled] {
            let mut cs = ConnectionState::new();
            cs.activate();
            cs.read = read;
            assert!(cs.recv_finished(), "{read:?} finishes the read half");
        }
    }

    #[test]
    fn note_eof_never_downgrades_truncation() {
        let mut cs = ConnectionState::new();
        cs.activate();
        cs.note_eof(true);
        cs.note_eof(false);
        assert_eq!(cs.read, ReadHalf::Eof { truncated: true });
    }

    #[test]
    fn outbound_lifecycle_transitions() {
        let mut cs = ConnectionState::new();
        cs.activate_outbound();
        assert_eq!(cs.lifecycle, Lifecycle::Connecting);
        assert_eq!(cs.recv_arm, RecvArm::Idle);
        assert!(!cs.recv_finished(), "connecting is not finished");
        cs.mark_connected();
        assert_eq!(cs.lifecycle, Lifecycle::Open);
        assert_eq!(cs.recv_arm, RecvArm::Multi);
        cs.deactivate();
        assert_eq!(cs.lifecycle, Lifecycle::Inactive);
        assert_eq!(cs.read, ReadHalf::Open);
        assert_eq!(cs.recv_arm, RecvArm::Idle);
    }

    #[test]
    fn max_slots_returns_capacity() {
        let table = ConnectionTable::new(16);
        assert_eq!(table.max_slots(), 16);
        assert_eq!(table.active_count(), 0);
    }

    #[test]
    fn allocate_gives_lowest_index_first() {
        let table = ConnectionTable::new(4);
        // Free list is reversed, so pop gives lowest first.
        assert_eq!(table.free_list.last(), Some(&0));
    }
}
