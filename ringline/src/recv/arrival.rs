//! Per-connection receive arrival log (`docs/recv-arrival-times-design.md`).
//!
//! Each TCP connection records, against stream offsets, the batch time at
//! which each run of received bytes was delivered. A reporting reader call
//! looks up the times of the first and last byte it consumed and stores them
//! for [`RecvArrival`].
//!
//! The log is backend-agnostic. Each driver owns one [`ArrivalLog`] per
//! connection slot and one [`BatchClock`], calls [`ArrivalLog::add`] where
//! received bytes enter the connection, and supplies `buffered`, the bytes the
//! connection holds that no consumer has taken.
//!
//! Building with `--cfg ringline_no_arrival_log` compiles the records and
//! lookups out, for measuring their cost; `recv_arrival` then returns `None`.

#![cfg_attr(ringline_no_arrival_log, allow(dead_code))]

use std::collections::VecDeque;
use std::time::Instant;

/// When the bytes consumed by the most recent reader call on a connection
/// arrived.
///
/// An arrival time is the time of the batch in which the worker handled the
/// receive that delivered the byte: one pass over the reaped completions on
/// io_uring, or over the readable sockets on mio. The time is read once per
/// batch, at the first receive in it. On io_uring it is later than the
/// kernel's receipt of the byte; on mio it can be earlier for a socket read
/// late in the batch. For TLS, a byte arrives when the record that contains
/// it is decrypted.
///
/// Returned by `Connection::recv_arrival` and `RecvHalf::recv_arrival`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecvArrival {
    first: Instant,
    last: Instant,
}

impl RecvArrival {
    /// Arrival time of the first byte the call consumed.
    pub fn first(&self) -> Instant {
        self.first
    }

    /// Arrival time of the last byte the call consumed.
    pub fn last(&self) -> Instant {
        self.last
    }
}

/// Entries a log holds before a push merges its two oldest.
pub(crate) const MAX_ENTRIES: usize = 1024;
/// A record trims only a log holding more entries than this.
const TRIM_MIN_ENTRIES: usize = 64;
/// A trim that leaves fewer than `TRIM_MIN_ENTRIES` entries shrinks a deque
/// whose capacity exceeds this to `TRIM_MIN_ENTRIES`.
const SHRINK_ABOVE: usize = 256;

/// The batch state of one worker: a sequence number, incremented when a batch
/// begins, and the batch time, read at the first record in the batch.
pub(crate) struct BatchClock {
    seq: u64,
    time: Option<Instant>,
}

impl BatchClock {
    pub(crate) fn new() -> Self {
        BatchClock { seq: 0, time: None }
    }

    /// Start a batch. Its time is read by the first record in it.
    pub(crate) fn begin(&mut self) {
        self.seq = self.seq.wrapping_add(1);
        self.time = None;
    }

    /// Start a batch whose time is `time`.
    #[cfg(test)]
    pub(crate) fn begin_at(&mut self, time: Instant) {
        self.seq = self.seq.wrapping_add(1);
        self.time = Some(time);
    }

    /// The batch's sequence number and time, reading the clock if no record
    /// in this batch has read it yet.
    fn now(&mut self) -> (u64, Instant) {
        let time = *self.time.get_or_insert_with(Instant::now);
        (self.seq, time)
    }
}

/// One connection's arrival log.
///
/// Offsets count bytes delivered to the connection since its slot was set up;
/// for TLS they count plaintext. Each entry is the end offset of a run of
/// bytes and the batch time at which the run was delivered. The run starts at
/// the previous entry's end, or at `base` for the oldest entry.
#[derive(Default)]
pub(crate) struct ArrivalLog {
    /// One past the last byte delivered.
    delivered: u64,
    /// End offset of the last entry dropped. Offsets below it have no entry.
    base: u64,
    entries: VecDeque<(u64, Instant)>,
    /// Batch sequence number of the newest entry.
    newest_batch: u64,
    /// Entry count after the previous trim.
    after_trim: usize,
    /// Times of the bytes the most recent reporting reader call consumed.
    latest: Option<RecvArrival>,
}

impl ArrivalLog {
    /// Empty the log for a new connection in the slot. Keeps the deque's
    /// allocation.
    pub(crate) fn clear(&mut self) {
        self.delivered = 0;
        self.base = 0;
        self.entries.clear();
        self.after_trim = 0;
        self.latest = None;
    }

    /// Empty the log at teardown and free its allocation.
    pub(crate) fn release(&mut self) {
        self.clear();
        self.entries = VecDeque::new();
    }

    pub(crate) fn delivered(&self) -> u64 {
        self.delivered
    }

    pub(crate) fn latest(&self) -> Option<RecvArrival> {
        self.latest.clone()
    }

    /// Whether a record should trim before it adds: the log holds more than
    /// `TRIM_MIN_ENTRIES` entries and at least twice the count left by the
    /// previous trim, or `MAX_ENTRIES`.
    pub(crate) fn trim_due(&self) -> bool {
        let len = self.entries.len();
        len > TRIM_MIN_ENTRIES && len >= (2 * self.after_trim).min(MAX_ENTRIES)
    }

    /// Drop the entries whose run ends at or below `consumed`, the first
    /// offset not yet consumed.
    pub(crate) fn trim(&mut self, consumed: u64) {
        while let Some(&(end, _)) = self.entries.front() {
            if end > consumed {
                break;
            }
            self.base = end;
            self.entries.pop_front();
        }
        self.after_trim = self.entries.len();
        if self.entries.len() < TRIM_MIN_ENTRIES && self.entries.capacity() > SHRINK_ABOVE {
            self.entries.shrink_to(TRIM_MIN_ENTRIES);
        }
    }

    /// Record `n` bytes delivered in the current batch of `clock`. The caller
    /// trims first when [`trim_due`](Self::trim_due) says so, before the bytes
    /// are placed anywhere.
    pub(crate) fn add(&mut self, n: u64, clock: &mut BatchClock) {
        if n == 0 {
            return;
        }
        let (seq, time) = clock.now();
        self.delivered += n;
        if self.newest_batch == seq
            && let Some(newest) = self.entries.back_mut()
        {
            newest.0 = self.delivered;
            return;
        }
        if self.entries.len() >= MAX_ENTRIES {
            // Keep the later end and the earlier time.
            if let Some((_, oldest_time)) = self.entries.pop_front()
                && let Some(next) = self.entries.front_mut()
            {
                next.1 = oldest_time;
            }
            crate::metrics::RECV_ARRIVAL.increment(crate::metrics::recv_arrival::MERGED);
        }
        self.entries.push_back((self.delivered, time));
        self.newest_batch = seq;
    }

    /// Settle a reporting reader call. `before` and `after` are the
    /// connection's `buffered` count at the start of the call's resolving poll
    /// and once it has applied the parser's result. `report` is false for a
    /// call that resolved without consuming input (an EOF or a close).
    ///
    /// The bytes the call removed are `before - after`, starting at
    /// `delivered - before`. Their first and last times become `latest`, and
    /// entries ending at or below the range's end are dropped.
    pub(crate) fn settle_read(&mut self, before: usize, after: usize, report: bool) {
        let k = before.saturating_sub(after) as u64;
        if k == 0 {
            self.latest = None;
            return;
        }
        let start = self.delivered.saturating_sub(before as u64);
        let end = start + k;
        self.latest = if report {
            match (self.time_at(start), self.time_at(end - 1)) {
                (Some(first), Some(last)) => Some(RecvArrival { first, last }),
                _ => None,
            }
        } else {
            None
        };
        self.trim(end);
    }

    /// The arrival time of the byte at `offset`. An offset outside the log
    /// returns the nearest end's time and counts `CLAMPED`; an empty log
    /// returns `None`.
    fn time_at(&self, offset: u64) -> Option<Instant> {
        let (Some(oldest), Some(newest)) = (self.entries.front(), self.entries.back()) else {
            crate::metrics::RECV_ARRIVAL.increment(crate::metrics::recv_arrival::CLAMPED);
            return None;
        };
        if offset < self.base {
            crate::metrics::RECV_ARRIVAL.increment(crate::metrics::recv_arrival::CLAMPED);
            return Some(oldest.1);
        }
        if offset >= self.delivered {
            crate::metrics::RECV_ARRIVAL.increment(crate::metrics::recv_arrival::CLAMPED);
            return Some(newest.1);
        }
        let i = self.entries.partition_point(|&(end, _)| end <= offset);
        Some(self.entries[i].1)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.entries.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn t(base: Instant, ms: u64) -> Instant {
        base + Duration::from_millis(ms)
    }

    #[test]
    fn records_in_one_batch_extend_one_entry() {
        let base = Instant::now();
        let mut clock = BatchClock::new();
        let mut log = ArrivalLog::default();
        clock.begin_at(t(base, 1));
        log.add(10, &mut clock);
        log.add(5, &mut clock);
        assert_eq!(log.len(), 1);
        clock.begin_at(t(base, 2));
        log.add(5, &mut clock);
        assert_eq!(log.len(), 2);
        assert_eq!(log.delivered(), 20);

        // Read bytes 0..12: first from batch 1, last from batch 1.
        log.settle_read(20, 8, true);
        let a = log.latest().unwrap();
        assert_eq!((a.first(), a.last()), (t(base, 1), t(base, 1)));
        // Read 12..20: first from batch 1, last from batch 2.
        log.settle_read(8, 0, true);
        let a = log.latest().unwrap();
        assert_eq!((a.first(), a.last()), (t(base, 1), t(base, 2)));
        assert_eq!(log.len(), 0);
    }

    #[test]
    fn a_zero_byte_record_reads_no_clock_and_adds_nothing() {
        let mut clock = BatchClock::new();
        let mut log = ArrivalLog::default();
        clock.begin();
        log.add(0, &mut clock);
        assert_eq!(log.len(), 0);
        assert!(clock.time.is_none());
    }

    #[test]
    fn a_read_that_removed_nothing_clears_latest() {
        let base = Instant::now();
        let mut clock = BatchClock::new();
        let mut log = ArrivalLog::default();
        clock.begin_at(base);
        log.add(4, &mut clock);
        log.settle_read(4, 2, true);
        assert!(log.latest().is_some());
        log.settle_read(2, 2, true);
        assert!(log.latest().is_none());
    }

    #[test]
    fn an_unreported_read_trims_and_clears_latest() {
        let base = Instant::now();
        let mut clock = BatchClock::new();
        let mut log = ArrivalLog::default();
        clock.begin_at(base);
        log.add(4, &mut clock);
        log.settle_read(4, 0, false);
        assert!(log.latest().is_none());
        assert_eq!(log.len(), 0);
    }

    #[test]
    fn a_full_log_merges_its_two_oldest_entries() {
        let base = Instant::now();
        let mut clock = BatchClock::new();
        let mut log = ArrivalLog::default();
        for i in 0..=MAX_ENTRIES as u64 {
            clock.begin_at(t(base, i));
            log.add(1, &mut clock);
        }
        assert_eq!(log.len(), MAX_ENTRIES);
        // Bytes 0 and 1 share the merged entry, which kept batch 0's time.
        log.settle_read(MAX_ENTRIES + 1, MAX_ENTRIES - 1, true);
        let a = log.latest().unwrap();
        assert_eq!((a.first(), a.last()), (t(base, 0), t(base, 0)));
        log.settle_read(MAX_ENTRIES - 1, MAX_ENTRIES - 2, true);
        assert_eq!(log.latest().unwrap().first(), t(base, 2));
    }

    #[test]
    fn a_lookup_below_base_returns_the_oldest_time() {
        let base = Instant::now();
        let mut clock = BatchClock::new();
        let mut log = ArrivalLog::default();
        clock.begin_at(t(base, 1));
        log.add(4, &mut clock);
        clock.begin_at(t(base, 2));
        log.add(4, &mut clock);
        // Bytes 0..4 are dropped by a trim that ran without a read.
        log.trim(4);
        // A read that claims 6 bytes held starts at offset 2, below base.
        log.settle_read(6, 0, true);
        let a = log.latest().unwrap();
        assert_eq!((a.first(), a.last()), (t(base, 2), t(base, 2)));
    }

    #[test]
    fn trim_waits_for_the_log_to_double() {
        let base = Instant::now();
        let mut clock = BatchClock::new();
        let mut log = ArrivalLog::default();
        for i in 0..=TRIM_MIN_ENTRIES as u64 {
            clock.begin_at(t(base, i));
            log.add(1, &mut clock);
        }
        assert!(log.trim_due());
        // Nothing consumed: the trim drops nothing and sets the threshold to
        // twice the live count.
        log.trim(0);
        assert_eq!(log.after_trim, TRIM_MIN_ENTRIES + 1);
        // Due again at twice that count, and not one entry before.
        for i in 0..TRIM_MIN_ENTRIES as u64 {
            clock.begin_at(t(base, 100 + i));
            log.add(1, &mut clock);
        }
        assert!(!log.trim_due());
        clock.begin_at(t(base, 200));
        log.add(1, &mut clock);
        assert!(log.trim_due());
    }

    #[test]
    fn a_trim_threshold_above_the_cap_is_capped() {
        let base = Instant::now();
        let mut clock = BatchClock::new();
        let mut log = ArrivalLog::default();
        for i in 0..600u64 {
            clock.begin_at(t(base, i));
            log.add(1, &mut clock);
        }
        log.trim(0);
        assert_eq!(log.after_trim, 600);
        for i in 600..MAX_ENTRIES as u64 {
            clock.begin_at(t(base, i));
            log.add(1, &mut clock);
        }
        assert!(log.trim_due());
    }

    #[test]
    fn a_trim_to_a_small_log_shrinks_a_large_deque() {
        let base = Instant::now();
        let mut clock = BatchClock::new();
        let mut log = ArrivalLog::default();
        for i in 0..MAX_ENTRIES as u64 {
            clock.begin_at(t(base, i));
            log.add(1, &mut clock);
        }
        assert!(log.capacity() > SHRINK_ABOVE);
        log.trim(MAX_ENTRIES as u64 - 1);
        assert_eq!(log.len(), 1);
        assert!(log.capacity() <= SHRINK_ABOVE);
    }

    #[test]
    fn clear_resets_every_field() {
        let base = Instant::now();
        let mut clock = BatchClock::new();
        let mut log = ArrivalLog::default();
        clock.begin_at(base);
        log.add(4, &mut clock);
        log.settle_read(4, 0, true);
        clock.begin_at(base);
        log.add(4, &mut clock);
        log.clear();
        assert_eq!(log.delivered(), 0);
        assert_eq!(log.len(), 0);
        assert!(log.latest().is_none());
        assert_eq!(log.base, 0);
        assert_eq!(log.after_trim, 0);
    }
}
