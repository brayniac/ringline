//! Per-connection recv delivery domain (segmented-recv, see
//! `docs/segmented-recv-design.md`).
//!
//! Selects how a connection's received bytes are delivered:
//! - `CopyOrConsume` (the default) — bytes go to the `RecvAccumulator` and are
//!   surfaced through `with_data`/`with_bytes`.
//! - `Segmented` — received bytes are held in `segment_hold` instead of the
//!   accumulator while a `forward_to` (Mode A) runs. Only a TLS connection
//!   enters it, and its decrypted plaintext is held as owned entries. On
//!   io_uring a plaintext connection receives directly into its accumulator,
//!   and `forward_to` returns `io::ErrorKind::Unsupported` on it.
//!   `forward_to_conn` sends from the accumulator through the sink's send
//!   queue; while a `forward_to` runs from the source it gets `EBUSY`.
//!
//! This is a minimal per-connection selector; the holding machinery lives on the
//! `Driver` (`segment_hold`).

/// How received bytes are delivered for a connection.
///
/// Consumed only by the io_uring backend's recv path; under the mio backend the
/// enum is unused (there is no provided-buffer ring), hence the `dead_code`
/// allow — matching the "inert until a consumer exists" pattern in
/// `recv::occupancy`.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum RecvDomain {
    /// Deliver into the accumulator, surfaced via `with_data`/`with_bytes`. The
    /// default for every connection.
    #[default]
    CopyOrConsume,
    /// Hold received bytes in `segment_hold` for Mode A forwards. On io_uring
    /// only a TLS connection enters it.
    Segmented,
}
