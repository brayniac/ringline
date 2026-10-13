//! Connections whose recv is parked until provided-ring buffers return.

use std::collections::VecDeque;

/// Parked connection indices, oldest first, with O(1) membership.
///
/// `flush_replenish_and_rearm` pops from the front and pushes a connection it
/// does not re-arm onto the back, so connections are re-armed in the order
/// they parked.
#[derive(Default)]
pub(crate) struct ParkedConns {
    queue: VecDeque<u32>,
    /// `member[i]` is set while connection `i` is in `queue`.
    member: Vec<bool>,
}

impl ParkedConns {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Appends `conn_index` unless it is already parked. Returns whether it
    /// was appended.
    pub(crate) fn push(&mut self, conn_index: u32) -> bool {
        let i = conn_index as usize;
        if i >= self.member.len() {
            self.member.resize(i + 1, false);
        }
        if self.member[i] {
            return false;
        }
        self.member[i] = true;
        self.queue.push_back(conn_index);
        true
    }

    pub(crate) fn contains(&self, conn_index: u32) -> bool {
        self.member
            .get(conn_index as usize)
            .copied()
            .unwrap_or(false)
    }

    /// Removes `conn_index` if parked. Returns whether it was. O(n) in the
    /// number parked.
    pub(crate) fn remove(&mut self, conn_index: u32) -> bool {
        if !self.contains(conn_index) {
            return false;
        }
        self.member[conn_index as usize] = false;
        if let Some(pos) = self.queue.iter().position(|&c| c == conn_index) {
            self.queue.remove(pos);
        }
        true
    }

    /// Removes and returns the connection parked longest.
    pub(crate) fn pop_front(&mut self) -> Option<u32> {
        let conn_index = self.queue.pop_front()?;
        self.member[conn_index as usize] = false;
        Some(conn_index)
    }

    /// The `i`th parked connection, oldest first.
    pub(crate) fn get(&self, i: usize) -> Option<u32> {
        self.queue.get(i).copied()
    }

    pub(crate) fn len(&self) -> usize {
        self.queue.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_ignores_a_connection_already_parked() {
        let mut p = ParkedConns::new();
        assert!(p.push(3));
        assert!(!p.push(3));
        assert_eq!(p.len(), 1);
        assert!(p.contains(3));
        assert!(!p.contains(2));
        assert!(!p.contains(1000));
    }

    #[test]
    fn pop_front_returns_connections_in_park_order() {
        let mut p = ParkedConns::new();
        for c in [5, 1, 9, 2] {
            p.push(c);
        }
        let order: Vec<u32> = std::iter::from_fn(|| p.pop_front()).collect();
        assert_eq!(order, [5, 1, 9, 2]);
        assert!(p.is_empty());
        assert!(!p.contains(5));
    }

    #[test]
    fn remove_takes_out_one_connection_and_keeps_the_order() {
        let mut p = ParkedConns::new();
        for c in [4, 7, 8] {
            p.push(c);
        }
        assert!(p.remove(7));
        assert!(!p.remove(7));
        assert!(!p.contains(7));
        assert_eq!(p.get(0), Some(4));
        assert_eq!(p.get(1), Some(8));
        assert_eq!(p.len(), 2);
        // A removed connection can park again, at the back.
        assert!(p.push(7));
        assert_eq!(p.get(2), Some(7));
    }
}
