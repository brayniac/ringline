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
    /// What the handler deposited via `offer_for_park`.
    pub state: Option<ParkState>,
    /// Worker index this connection is bound for.
    pub target: usize,
}

/// Handler state carried across a park (tier 3, #443).
///
/// Park drops the future the handler was running and recreates it on the new
/// worker, so anything the future held is gone. A handler that wants to keep
/// something deposits it here at a quiescent point, and gets it back in
/// [`AsyncEventHandler::on_adopt`].
///
/// # Why this is not statically typed
///
/// The state crosses from a connection handle to the handler, and
/// `Connection`/`ConnCtx` are plain tokens — not generic over the handler
/// type. So the deposit site cannot name the handler's state type, and an
/// associated type on the handler trait would give a typed *take* while
/// leaving the *deposit* untyped: the mismatch would move rather than
/// disappear. Making it genuinely static would mean threading the handler
/// type through every connection handle.
///
/// So the erasure is kept explicit and in one place, and a mismatch is made
/// loud instead: [`ParkState::take`] panics in debug builds naming both
/// types, so the wrong type fails on the first test run rather than silently
/// adopting with no state.
///
/// ```ignore
/// // deposit, after finishing a response
/// conn.offer_for_park(Some(ParkState::new(Session { db: 3 })));
///
/// // and on the worker that adopts it
/// let session = state.and_then(ParkState::take::<Session>);
/// ```
pub struct ParkState {
    value: Box<dyn std::any::Any + Send>,
    /// Captured at deposit so a mismatch can name both sides. A `dyn Any`
    /// cannot report the type it erased, and `TypeId` is opaque in a panic
    /// message — the point of failing loudly is that the message is useful.
    deposited_as: &'static str,
}

impl ParkState {
    /// Deposit `value` to be carried to the adopting worker.
    pub fn new<T: std::any::Any + Send>(value: T) -> Self {
        ParkState {
            value: Box::new(value),
            deposited_as: std::any::type_name::<T>(),
        }
    }

    /// Take the value back as `T`.
    ///
    /// `None` if the deposited type was not `T`. In debug builds this panics
    /// naming both types instead: depositing one type and taking another is a
    /// bug in the handler, not a condition worth handling at runtime, and a
    /// silent `None` would present as a connection that quietly lost its
    /// session.
    pub fn take<T: std::any::Any + Send>(self) -> Option<T> {
        let deposited_as = self.deposited_as;
        match self.value.downcast::<T>() {
            Ok(v) => Some(*v),
            Err(_) => {
                debug_assert!(
                    false,
                    "park state was deposited as `{}` but taken as `{}`",
                    deposited_as,
                    std::any::type_name::<T>(),
                );
                None
            }
        }
    }

    /// The type name recorded at deposit. For diagnostics.
    pub fn deposited_as(&self) -> &'static str {
        self.deposited_as
    }
}

impl std::fmt::Debug for ParkState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParkState")
            .field("deposited_as", &self.deposited_as)
            .finish()
    }
}
