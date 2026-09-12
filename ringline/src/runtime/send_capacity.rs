//! Per-worker admission queue for bounded (backpressured) sends.
//!
//! A bounded send (`ConnCtx::send_backpressured`, series PR 9) may only
//! reserve copy-pool slots once it is the *oldest* waiter on the worker and
//! the pool can admit its whole message. This module is the FIFO that
//! decides whose turn it is, tracks each admitted operation until its
//! completion arrives, and hands the exact result of that one operation
//! back to the future that submitted it.
//!
//! The queue is plain worker-local state owned by the [`Executor`], next to
//! the other per-connection waiter bookkeeping. It never allocates on the
//! hot paths (`enqueue` is an amortized push; `turn`, `mark_submitted`,
//! `complete` and `take_result` are lookups and swaps); only the teardown
//! paths (`remove_connection`, `fail_waiting`) build a small `Vec` of task
//! ids to wake.
//!
//! Lifecycle of one entry:
//!
//! ```text
//! enqueue ─▶ waiting ─mark_submitted─▶ InFlight ─complete─▶ Done ─take_result─▶ gone
//!              │                          │                       ▲
//!              │ cancel ─▶ gone           │ cancel ─▶ Abandoned   │ complete
//!              │                          │             └complete─▶ gone
//!              │ remove_connection        │ remove_connection     │
//!              ▼                          ▼                       │
//!           Aborted ◀───────────────── Aborted ───────────────────┘
//!              └─ take_result ─▶ gone
//! ```
//!
//! **A synthetic teardown abort is provisional.** `remove_connection` runs
//! from [`Executor::remove_connection`], and on mio that has three callers,
//! not one: `drain_pending_closes` (step 6b of the run loop, after the
//! flush) *and* `poll_ready_tasks` (step 6, before it) for a connection task
//! that returned `Poll::Ready` or panicked. So a bounded send owned by a
//! task that outlives the connection can be aborted by teardown at step 6
//! and then written to the socket in full by step 6a's flush, in the same
//! iteration. Teardown therefore records `Aborted`, not `Done`, and
//! [`complete`](SendCapacityQueue::complete) overwrites it with the driver's
//! result: a message that was fully delivered must never be reported to its
//! caller as aborted. An `Aborted` that nothing overwrites still resolves
//! the future ([`take_result`](SendCapacityQueue::take_result) takes it), so
//! a genuinely torn-down send does not hang. "First result wins" survives
//! only between two *driver* results for one id.
//!
//! Every entry records the task id that owns it. The owner is refreshed on
//! each poll (`set_owner`, the same discipline `SendFuture` follows with
//! `owner_task`), and the `Executor` wrappers call `wake_task` immediately on
//! whatever id the queue returns — no deferred wake list. Between a
//! completion and the next poll no poll runs, so the owner resolved at
//! completion time is the owner that will be polled.
//!
//! Design: `docs/send-capacity-fifo-design.md` (series PR 5 of
//! `docs/backpressured-sends-series-design.md`).

// No callers outside this module's tests until series PRs 6 and 7 (the
// driver's slot-release hook calls `wake_send_capacity`, the backends call
// `complete_bounded_send`) and PR 9 (`send_backpressured` drives the rest).
// PR 9 removes this attribute.
#![cfg_attr(not(test), allow(dead_code))]

use std::collections::VecDeque;
use std::io;

use super::Executor;

/// Identity of one bounded send on its worker.
///
/// Monotonic per worker, never reused (a `u64` cannot wrap in practice).
/// Handed out by [`SendCapacityQueue::enqueue`] and held by the
/// `send_backpressured` future (PR 9) for every later call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct BoundedSendId(u64);

/// A bounded send that has not been admitted yet.
struct Waiter {
    id: BoundedSendId,
    conn_index: u32,
    generation: u32,
    /// Copy-pool slots the whole message needs; `turn` admits the head only
    /// when at least this many are free.
    required_slots: usize,
    /// Task to wake when this entry becomes the head or gains capacity.
    task_id: u32,
}

/// Where an admitted operation stands.
enum Completion {
    /// Submitted to the driver; no completion yet.
    InFlight,
    /// The driver reported; the result waits for `take_result`. Final — a
    /// second driver result for the same id is ignored.
    Done(io::Result<u32>),
    /// Teardown recorded this before the driver reported. Provisional: a
    /// real driver result overwrites it (`complete`), and if none ever
    /// arrives `take_result` hands this error to the future. See the module
    /// docs for why teardown can run before the flush that completes the
    /// send.
    Aborted(io::Error),
    /// The future was dropped while the operation was in flight. The driver
    /// still owns the operation; its completion removes the entry.
    Abandoned,
}

/// A bounded send that has been admitted and submitted.
struct Submitted {
    id: BoundedSendId,
    conn_index: u32,
    /// Task to wake when the completion arrives.
    task_id: u32,
    state: Completion,
}

/// FIFO admission queue for bounded sends on one worker.
///
/// Owned by [`Executor::send_capacity`]; the `Executor` methods further down
/// in this file are the only intended entry points outside tests, because
/// they pair every returned task id with `wake_task`.
#[derive(Default)]
pub(crate) struct SendCapacityQueue {
    /// Waiters in arrival order. Only the head may be admitted.
    waiting: VecDeque<Waiter>,
    /// Admitted operations awaiting a completion or a `take_result`. Order
    /// is irrelevant; entries are found by linear scan (in-flight bounded
    /// sends per worker are bounded by pool slots) and removed by
    /// `swap_remove`.
    submitted: Vec<Submitted>,
    /// Next id to hand out.
    next_id: u64,
}

impl SendCapacityQueue {
    /// Empty queue. Called once per worker from `Executor::new`.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Append a waiter and return its id.
    ///
    /// Called by PR 9's `send_backpressured` on its first poll, with the
    /// connection it targets, the number of copy-pool slots its message
    /// needs, and the polling task's id.
    pub(crate) fn enqueue(
        &mut self,
        conn_index: u32,
        generation: u32,
        required_slots: usize,
        task_id: u32,
    ) -> BoundedSendId {
        let id = BoundedSendId(self.next_id);
        self.next_id = self
            .next_id
            .checked_add(1)
            .expect("bounded send id space exhausted");
        self.waiting.push_back(Waiter {
            id,
            conn_index,
            generation,
            required_slots,
            task_id,
        });
        id
    }

    /// Record the task that currently owns `id`, whether it is waiting or
    /// submitted. A no-op for an unknown id.
    ///
    /// Called by PR 9's future on every poll after the first, so that a
    /// future moved between tasks (or polled from a different task than the
    /// one that enqueued it) is woken where it now lives.
    pub(crate) fn set_owner(&mut self, id: BoundedSendId, task_id: u32) {
        if let Some(w) = self.waiting.iter_mut().find(|w| w.id == id) {
            w.task_id = task_id;
        } else if let Some(s) = self.submitted.iter_mut().find(|s| s.id == id) {
            s.task_id = task_id;
        }
    }

    /// Whether `id` may reserve slots now: it must be the head of the queue
    /// *and* `free_slots` must cover its `required_slots`.
    ///
    /// Called by PR 9's future on each poll while waiting; a `false` parks
    /// the future until [`head_ready`](Self::head_ready) or a promotion
    /// wakes it.
    pub(crate) fn turn(&self, id: BoundedSendId, free_slots: usize) -> bool {
        self.waiting
            .front()
            .is_some_and(|head| head.id == id && free_slots >= head.required_slots)
    }

    /// Move `id` from waiting to submitted (`InFlight`).
    ///
    /// Returns the new head's task id if `id` was the head, so the caller
    /// can wake it (it re-checks [`turn`](Self::turn) on its next poll).
    /// Returns `None` if `id` was not the head, if there is no new head, or
    /// if `id` is not waiting.
    ///
    /// Called by PR 9's future right after it reserved its slots and
    /// submitted the send.
    pub(crate) fn mark_submitted(&mut self, id: BoundedSendId) -> Option<u32> {
        let pos = self.waiting.iter().position(|w| w.id == id)?;
        let was_head = pos == 0;
        let w = if was_head {
            self.waiting.pop_front()?
        } else {
            self.waiting.remove(pos)?
        };
        self.submitted.push(Submitted {
            id: w.id,
            conn_index: w.conn_index,
            task_id: w.task_id,
            state: Completion::InFlight,
        });
        if was_head {
            self.waiting.front().map(|next| next.task_id)
        } else {
            None
        }
    }

    /// Deliver the driver's completion of `id` — the authoritative result.
    ///
    /// `InFlight` becomes `Done(result)` and the owner's task id is returned
    /// for waking. `Aborted` does the same: a teardown's synthetic abort is
    /// provisional, and on mio it can be recorded *before* the flush that
    /// puts the send's last byte on the socket (see the module docs), so a
    /// fully delivered message would otherwise be reported to its caller as
    /// `ConnectionAborted`. `Abandoned` entries are removed and `result` is
    /// discarded (the future is gone). An entry that is already `Done` keeps
    /// its result — between two *driver* results for one id the first wins.
    /// An unknown id is ignored: it is a stale completion for an entry that
    /// [`remove_connection`](Self::remove_connection) already resolved and
    /// the future already collected.
    ///
    /// Called by the backends' send-completion handlers (PRs 6 and 7).
    pub(crate) fn complete(&mut self, id: BoundedSendId, result: io::Result<u32>) -> Option<u32> {
        let pos = self.submitted.iter().position(|s| s.id == id)?;
        match self.submitted[pos].state {
            Completion::InFlight | Completion::Aborted(_) => {
                self.submitted[pos].state = Completion::Done(result);
                Some(self.submitted[pos].task_id)
            }
            Completion::Abandoned => {
                self.submitted.swap_remove(pos);
                None
            }
            Completion::Done(_) => None,
        }
    }

    /// Take the result of `id` if it has one — a driver result (`Done`) or
    /// a teardown abort no driver result overwrote (`Aborted`) — removing
    /// the entry. Anything else (waiting, in flight, abandoned, unknown) is
    /// `None`.
    ///
    /// `Aborted` resolves here too, and must: a send whose connection really
    /// was torn down before the driver ever reported has no other way to
    /// finish, and would park forever.
    ///
    /// Called by PR 9's future on each poll after it submitted; `None`
    /// parks it until [`complete`](Self::complete) wakes it.
    pub(crate) fn take_result(&mut self, id: BoundedSendId) -> Option<io::Result<u32>> {
        let pos = self.submitted.iter().position(|s| {
            s.id == id && matches!(s.state, Completion::Done(_) | Completion::Aborted(_))
        })?;
        match self.submitted.swap_remove(pos).state {
            Completion::Done(result) => Some(result),
            Completion::Aborted(err) => Some(Err(err)),
            Completion::InFlight | Completion::Abandoned => unreachable!("filtered by position"),
        }
    }

    /// The future for `id` was dropped.
    ///
    /// Waiting: removed; if it was the head, the new head's task id is
    /// returned for waking. `InFlight`: becomes `Abandoned` (the driver
    /// still owns the operation; its completion removes the entry). `Done`
    /// or `Aborted`: removed, result discarded (a driver result that arrives
    /// afterwards finds no entry and is ignored). Unknown: ignored.
    ///
    /// Called from PR 9's `Drop` via `try_with_state`. A drop outside the
    /// executor skips this, as it does for every other future in the crate;
    /// [`remove_connection`](Self::remove_connection) is the backstop.
    pub(crate) fn cancel(&mut self, id: BoundedSendId) -> Option<u32> {
        if let Some(pos) = self.waiting.iter().position(|w| w.id == id) {
            if pos == 0 {
                self.waiting.pop_front();
                return self.waiting.front().map(|next| next.task_id);
            }
            self.waiting.remove(pos);
            return None;
        }
        if let Some(pos) = self.submitted.iter().position(|s| s.id == id) {
            match self.submitted[pos].state {
                Completion::InFlight => self.submitted[pos].state = Completion::Abandoned,
                Completion::Done(_) | Completion::Aborted(_) => {
                    self.submitted.swap_remove(pos);
                }
                Completion::Abandoned => {}
            }
        }
        None
    }

    /// The head's task id if `free_slots` covers its `required_slots`.
    ///
    /// The driver's slot-release hook (PRs 6 and 7) calls this through
    /// [`Executor::wake_send_capacity`] after every pool release, so the
    /// head is woken only when it can now fit rather than on every release.
    pub(crate) fn head_ready(&self, free_slots: usize) -> Option<u32> {
        self.waiting
            .front()
            .filter(|head| free_slots >= head.required_slots)
            .map(|head| head.task_id)
    }

    /// The connection at `conn_index` is being torn down (any generation).
    ///
    /// Every waiting and in-flight entry for the connection becomes
    /// `Aborted(ConnectionAborted)` (waiting ones move to `submitted`) and
    /// its owner's task id is in the returned list — the owner may be a
    /// standalone task, or a connection task on another index, that
    /// outlives this connection. Abandoned entries are removed. If the head
    /// was removed, the new head's task id is appended as well.
    ///
    /// `Aborted` rather than `Done` because this abort is *provisional*: on
    /// mio `Executor::remove_connection` is also called from
    /// `poll_ready_tasks` (step 6), before the step-6a flush that can still
    /// put the whole message on the socket, and the driver's `Ok(n)` must
    /// win over the abort recorded here. See the module docs. (An entry
    /// still waiting was never submitted, so no driver result can arrive for
    /// it; it is recorded the same way so teardown has one rule, and
    /// `take_result` resolves both alike.)
    ///
    /// Entries owned by *this* connection's own task (`task_id ==
    /// conn_index`) are the exception: `Executor::remove_connection` calls
    /// this after `task_slab.remove` has dropped that task's future outside
    /// any poll, so its `cancel` never ran and nothing can ever
    /// `take_result`. Parking a `Done` for it would leak one entry per
    /// closed connection. Those entries are instead treated as cancelled:
    /// waiting → removed, `InFlight` → `Abandoned` (the completion still
    /// arrives and removes it), `Done` → removed.
    ///
    /// Allocates the returned `Vec`; this is the teardown path.
    pub(crate) fn remove_connection(&mut self, conn_index: u32) -> Vec<u32> {
        let mut wakes = Vec::new();
        let head_removed = self
            .waiting
            .front()
            .is_some_and(|w| w.conn_index == conn_index);

        let submitted = &mut self.submitted;
        self.waiting.retain(|w| {
            if w.conn_index != conn_index {
                return true;
            }
            if w.task_id != conn_index {
                submitted.push(Submitted {
                    id: w.id,
                    conn_index: w.conn_index,
                    task_id: w.task_id,
                    state: Completion::Aborted(connection_aborted()),
                });
                wakes.push(w.task_id);
            }
            false
        });

        self.submitted.retain_mut(|s| {
            if s.conn_index != conn_index {
                return true;
            }
            let owner_gone = s.task_id == conn_index;
            match s.state {
                Completion::InFlight if owner_gone => {
                    s.state = Completion::Abandoned;
                    true
                }
                Completion::InFlight => {
                    s.state = Completion::Aborted(connection_aborted());
                    wakes.push(s.task_id);
                    true
                }
                // Already resolved (including by an earlier teardown of the
                // same slot): keep it only while someone can still take it.
                Completion::Done(_) | Completion::Aborted(_) => !owner_gone,
                Completion::Abandoned => false,
            }
        });

        if head_removed && let Some(next) = self.waiting.front() {
            wakes.push(next.task_id);
        }
        wakes
    }

    /// Fail every *waiting* entry for exactly (`conn_index`, `generation`)
    /// with `io::Error::new(kind, msg)`; in-flight entries are left alone.
    /// Returns the owners' task ids for waking, plus the new head's task id
    /// if the head was among the failed.
    ///
    /// PR 9's `shutdown_write` calls this with `BrokenPipe` so that bounded
    /// sends queued behind a write shutdown fail instead of waiting forever.
    ///
    /// Records `Done`, not the provisional `Aborted`
    /// [`remove_connection`](Self::remove_connection) uses: these entries
    /// were never submitted, so no driver result is coming to overwrite
    /// them.
    ///
    /// Allocates the returned `Vec` and one boxed error per failed entry.
    pub(crate) fn fail_waiting(
        &mut self,
        conn_index: u32,
        generation: u32,
        kind: io::ErrorKind,
        msg: &'static str,
    ) -> Vec<u32> {
        let mut wakes = Vec::new();
        let head_removed = self
            .waiting
            .front()
            .is_some_and(|w| w.conn_index == conn_index && w.generation == generation);

        let submitted = &mut self.submitted;
        self.waiting.retain(|w| {
            if w.conn_index != conn_index || w.generation != generation {
                return true;
            }
            submitted.push(Submitted {
                id: w.id,
                conn_index: w.conn_index,
                task_id: w.task_id,
                state: Completion::Done(Err(io::Error::new(kind, msg))),
            });
            wakes.push(w.task_id);
            false
        });

        if head_removed && let Some(next) = self.waiting.front() {
            wakes.push(next.task_id);
        }
        wakes
    }

    #[cfg(test)]
    fn waiting_len(&self) -> usize {
        self.waiting.len()
    }

    #[cfg(test)]
    fn submitted_len(&self) -> usize {
        self.submitted.len()
    }
}

fn connection_aborted() -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionAborted, "connection closed")
}

/// Thin wrappers that pair every task id the queue returns with
/// [`Executor::wake_task`]. These are the entry points the rest of the crate
/// uses; the `wake_task` bool is ignored (a task that is not parked does not
/// need waking).
impl Executor {
    /// [`SendCapacityQueue::enqueue`]. PR 9, first poll of
    /// `send_backpressured`.
    pub(crate) fn enqueue_send_capacity(
        &mut self,
        conn_index: u32,
        generation: u32,
        required_slots: usize,
        task_id: u32,
    ) -> BoundedSendId {
        self.send_capacity
            .enqueue(conn_index, generation, required_slots, task_id)
    }

    /// [`SendCapacityQueue::set_owner`]. PR 9, every later poll.
    pub(crate) fn set_bounded_send_owner(&mut self, id: BoundedSendId, task_id: u32) {
        self.send_capacity.set_owner(id, task_id);
    }

    /// [`SendCapacityQueue::turn`]. PR 9, each poll while waiting.
    pub(crate) fn send_capacity_turn(&self, id: BoundedSendId, free_slots: usize) -> bool {
        self.send_capacity.turn(id, free_slots)
    }

    /// [`SendCapacityQueue::mark_submitted`] plus a wake of the new head.
    /// PR 9, after the reserve-copy-submit step.
    pub(crate) fn mark_bounded_send_submitted(&mut self, id: BoundedSendId) {
        if let Some(next) = self.send_capacity.mark_submitted(id) {
            let _ = self.wake_task(next);
        }
    }

    /// [`SendCapacityQueue::complete`] plus a wake of the owner. Backends'
    /// send-completion handlers, PRs 6 and 7.
    pub(crate) fn complete_bounded_send(&mut self, id: BoundedSendId, result: io::Result<u32>) {
        if let Some(owner) = self.send_capacity.complete(id, result) {
            let _ = self.wake_task(owner);
        }
    }

    /// [`SendCapacityQueue::take_result`]. PR 9, each poll after submission.
    pub(crate) fn take_bounded_send_result(
        &mut self,
        id: BoundedSendId,
    ) -> Option<io::Result<u32>> {
        self.send_capacity.take_result(id)
    }

    /// [`SendCapacityQueue::cancel`] plus a wake of the new head. PR 9,
    /// from the future's `Drop` via `try_with_state`.
    pub(crate) fn cancel_bounded_send(&mut self, id: BoundedSendId) {
        if let Some(next) = self.send_capacity.cancel(id) {
            let _ = self.wake_task(next);
        }
    }

    /// [`SendCapacityQueue::head_ready`] plus a wake of the head. The
    /// driver's copy-pool slot-release hook, PRs 6 and 7, with the number of
    /// slots now free.
    pub(crate) fn wake_send_capacity(&mut self, free_slots: usize) {
        if let Some(head) = self.send_capacity.head_ready(free_slots) {
            let _ = self.wake_task(head);
        }
    }

    /// [`SendCapacityQueue::fail_waiting`] plus a wake of every returned
    /// task. PR 9's `shutdown_write`, with `BrokenPipe`.
    pub(crate) fn fail_waiting_bounded_sends(
        &mut self,
        conn_index: u32,
        generation: u32,
        kind: io::ErrorKind,
        msg: &'static str,
    ) {
        for task_id in self
            .send_capacity
            .fail_waiting(conn_index, generation, kind, msg)
        {
            let _ = self.wake_task(task_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::waker::STANDALONE_BIT;

    // Task ids in these tests carry STANDALONE_BIT so they never collide
    // with a connection index (see `remove_connection`'s dead-owner rule).
    const fn task(n: u32) -> u32 {
        n | STANDALONE_BIT
    }

    fn kind_of(r: Option<io::Result<u32>>) -> io::ErrorKind {
        r.expect("result present")
            .expect_err("result is an error")
            .kind()
    }

    #[test]
    fn fifo_admits_only_the_head_and_only_with_capacity() {
        let mut q = SendCapacityQueue::new();
        let first = q.enqueue(1, 0, 3, task(1));
        let second = q.enqueue(2, 0, 1, task(2));

        assert!(!q.turn(second, 8), "second must wait behind the head");
        assert!(!q.turn(first, 2), "head must wait for its 3 slots");
        assert!(q.turn(first, 3), "head with capacity is admitted");
        assert!(q.turn(first, 100));
    }

    #[test]
    fn mark_submitted_promotes_and_reports_the_next_head() {
        let mut q = SendCapacityQueue::new();
        let a = q.enqueue(1, 0, 1, task(10));
        let b = q.enqueue(1, 0, 1, task(11));
        let c = q.enqueue(2, 0, 1, task(12));

        assert!(!q.turn(b, 8));
        assert_eq!(q.mark_submitted(a), Some(task(11)), "b is the new head");
        assert!(q.turn(b, 8));
        assert_eq!(q.waiting_len(), 2);
        assert_eq!(q.submitted_len(), 1);

        // Submitting from behind the head promotes nobody.
        assert_eq!(q.mark_submitted(c), None);
        assert!(q.turn(b, 8));

        // The last waiter leaves no head to report.
        assert_eq!(q.mark_submitted(b), None);
        assert_eq!(q.waiting_len(), 0);
        assert_eq!(q.submitted_len(), 3);

        // Already submitted: nothing to do.
        assert_eq!(q.mark_submitted(a), None);
        assert_eq!(q.submitted_len(), 3);
    }

    #[test]
    fn cancel_at_head_promotes_next_but_cancel_behind_head_wakes_nobody() {
        let mut q = SendCapacityQueue::new();
        let a = q.enqueue(1, 0, 1, task(1));
        let b = q.enqueue(1, 0, 1, task(2));
        let c = q.enqueue(1, 0, 1, task(3));

        assert_eq!(q.cancel(b), None, "b was not the head");
        assert!(q.turn(a, 1), "a is still the head");
        assert_eq!(q.cancel(a), Some(task(3)), "c is promoted past the gone b");
        assert!(q.turn(c, 1));
        assert_eq!(q.cancel(c), None, "nobody left to promote");
        assert_eq!(q.waiting_len(), 0);
        assert_eq!(q.cancel(c), None, "cancelling an unknown id is ignored");
    }

    #[test]
    fn abandoned_operation_discards_its_late_completion() {
        let mut q = SendCapacityQueue::new();
        let id = q.enqueue(1, 0, 1, task(1));
        assert_eq!(q.mark_submitted(id), None);
        assert_eq!(q.cancel(id), None, "in flight: nothing to promote");
        assert_eq!(
            q.submitted_len(),
            1,
            "abandoned entry stays until completion"
        );

        assert_eq!(q.complete(id, Ok(5)), None, "no owner to wake");
        assert_eq!(q.submitted_len(), 0, "completion removed the entry");
        assert!(q.take_result(id).is_none());
        assert_eq!(q.complete(id, Ok(5)), None, "second completion is ignored");
    }

    #[test]
    fn take_result_returns_exactly_once() {
        let mut q = SendCapacityQueue::new();
        let id = q.enqueue(1, 0, 1, task(1));
        assert!(q.take_result(id).is_none(), "waiting: no result");
        q.mark_submitted(id);
        assert!(q.take_result(id).is_none(), "in flight: no result");

        assert_eq!(q.complete(id, Ok(42)), Some(task(1)));
        assert_eq!(q.take_result(id).unwrap().unwrap(), 42);
        assert!(q.take_result(id).is_none(), "second take is None");
        assert_eq!(q.submitted_len(), 0);
    }

    #[test]
    fn remove_connection_fails_waiting_and_in_flight_and_wakes_unrelated_head() {
        const A: u32 = 1;
        const B: u32 = 2;
        let mut q = SendCapacityQueue::new();
        let a_abandoned = q.enqueue(A, 0, 1, task(10));
        q.mark_submitted(a_abandoned);
        q.cancel(a_abandoned);
        let a_in_flight = q.enqueue(A, 0, 1, task(11));
        q.mark_submitted(a_in_flight);
        let a_waiting = q.enqueue(A, 0, 1, task(12));
        let b_waiting = q.enqueue(B, 0, 1, task(20));
        assert!(q.turn(a_waiting, 8));

        let wakes = q.remove_connection(A);

        assert!(wakes.contains(&task(11)), "in-flight owner woken");
        assert!(wakes.contains(&task(12)), "waiting owner woken");
        assert!(wakes.contains(&task(20)), "B is the new head and woken");
        assert!(!wakes.contains(&task(10)), "abandoned entry wakes nobody");
        assert_eq!(wakes.len(), 3);

        assert_eq!(
            kind_of(q.take_result(a_in_flight)),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(
            kind_of(q.take_result(a_waiting)),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(q.submitted_len(), 0, "abandoned entry removed");
        assert_eq!(q.complete(a_abandoned, Ok(1)), None);
        assert!(q.take_result(a_abandoned).is_none());

        assert!(q.turn(b_waiting, 1), "B is the head");
        assert_eq!(q.waiting_len(), 1);

        // A later completion for A's in-flight op is stale and ignored.
        assert_eq!(q.complete(a_in_flight, Ok(1)), None);
        assert_eq!(q.submitted_len(), 0);
    }

    #[test]
    fn remove_connection_drops_entries_owned_by_the_dead_connection_task() {
        // Owner task id == conn index: the connection's own task, whose
        // future task_slab.remove already dropped. Nothing can take a
        // result for it, so no Done entry may be left behind.
        const A: u32 = 3;
        let mut q = SendCapacityQueue::new();
        let in_flight = q.enqueue(A, 0, 1, A);
        q.mark_submitted(in_flight);
        let done = q.enqueue(A, 0, 1, A);
        q.mark_submitted(done);
        assert_eq!(q.complete(done, Ok(1)), Some(A));
        let waiting = q.enqueue(A, 0, 1, A);
        let other = q.enqueue(4, 0, 1, task(40));

        let wakes = q.remove_connection(A);
        assert_eq!(wakes, vec![task(40)], "only the new head is woken");
        assert_eq!(q.waiting_len(), 1);
        assert_eq!(q.submitted_len(), 1, "only the in-flight op remains");
        assert!(q.take_result(waiting).is_none());
        assert!(q.take_result(done).is_none());
        assert_eq!(q.complete(in_flight, Ok(1)), None, "abandoned: no wake");
        assert_eq!(q.submitted_len(), 0, "its completion removed it");
        assert!(q.turn(other, 1));
    }

    #[test]
    fn a_real_result_overwrites_a_teardown_abort_but_not_another_result() {
        // The mio loop tears a connection down in `poll_ready_tasks`
        // (step 6) and flushes its queued sends in step 6a, so a send that
        // reached the socket is completed *after* teardown aborted it. The
        // driver's result is the authoritative one.
        const A: u32 = 7;
        let mut q = SendCapacityQueue::new();
        let id = q.enqueue(A, 0, 1, task(1));
        q.mark_submitted(id);

        assert_eq!(
            q.remove_connection(A),
            vec![task(1)],
            "teardown aborts the in-flight entry and wakes its owner"
        );

        assert_eq!(
            q.complete(id, Ok(200)),
            Some(task(1)),
            "the driver's result must land, and wake the owner again"
        );
        assert_eq!(
            q.take_result(id).expect("resolved").expect("delivered"),
            200,
            "a fully delivered message is not ConnectionAborted"
        );
        assert_eq!(q.submitted_len(), 0);

        // Two driver results for one id: the first still wins.
        let id = q.enqueue(A, 0, 1, task(2));
        q.mark_submitted(id);
        assert_eq!(q.complete(id, Ok(1)), Some(task(2)));
        assert_eq!(q.complete(id, Ok(2)), None, "second driver result ignored");
        assert_eq!(q.take_result(id).expect("resolved").expect("ok"), 1);
    }

    #[test]
    fn a_teardown_abort_nothing_overwrites_still_resolves() {
        // The other half of the provisional-abort rule: if no driver result
        // ever arrives, `take_result` must still hand the abort to the
        // future, or the send parks forever.
        const A: u32 = 8;
        let mut q = SendCapacityQueue::new();
        let in_flight = q.enqueue(A, 0, 1, task(1));
        q.mark_submitted(in_flight);
        let waiting = q.enqueue(A, 0, 1, task(2));

        q.remove_connection(A);

        assert_eq!(
            kind_of(q.take_result(in_flight)),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(
            kind_of(q.take_result(waiting)),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(q.submitted_len(), 0, "both entries removed by the take");
    }

    #[test]
    fn cancelling_an_aborted_entry_drops_it_and_its_late_result() {
        // The future was dropped after teardown aborted its entry: nothing
        // can take a result, so the entry goes and the driver's later
        // completion finds no id.
        const A: u32 = 9;
        let mut q = SendCapacityQueue::new();
        let id = q.enqueue(A, 0, 1, task(1));
        q.mark_submitted(id);
        q.remove_connection(A);

        assert_eq!(q.cancel(id), None);
        assert_eq!(q.submitted_len(), 0, "the aborted entry is gone");
        assert_eq!(q.complete(id, Ok(4)), None, "a late result has no owner");
        assert!(q.take_result(id).is_none());
    }

    #[test]
    fn fail_waiting_leaves_in_flight_alone_and_uses_the_given_kind() {
        let mut q = SendCapacityQueue::new();
        let in_flight = q.enqueue(1, 5, 1, task(1));
        q.mark_submitted(in_flight);
        let same_gen = q.enqueue(1, 5, 1, task(2));
        let other_gen = q.enqueue(1, 6, 1, task(3));
        let other_conn = q.enqueue(2, 5, 1, task(4));

        // PR 9 will pass BrokenPipe; an arbitrary kind here proves the kind
        // is plumbed through rather than hardcoded.
        let wakes = q.fail_waiting(1, 5, io::ErrorKind::TimedOut, "write side shut down");

        assert_eq!(
            wakes,
            vec![task(2), task(3)],
            "failed owner, then the promoted head"
        );
        assert_eq!(kind_of(q.take_result(same_gen)), io::ErrorKind::TimedOut);
        assert!(q.take_result(in_flight).is_none(), "in flight untouched");
        assert_eq!(q.complete(in_flight, Ok(9)), Some(task(1)));
        assert_eq!(q.take_result(in_flight).unwrap().unwrap(), 9);
        assert!(q.turn(other_gen, 1), "other generation is now the head");
        assert_eq!(q.cancel(other_gen), Some(task(4)));
        assert!(q.turn(other_conn, 1));
    }

    #[test]
    fn head_ready_respects_required_slots() {
        let mut q = SendCapacityQueue::new();
        assert_eq!(q.head_ready(usize::MAX), None, "empty queue");
        q.enqueue(1, 0, 4, task(1));
        q.enqueue(1, 0, 1, task(2));
        assert_eq!(q.head_ready(3), None, "head needs 4");
        assert_eq!(q.head_ready(4), Some(task(1)));
        assert_eq!(q.head_ready(9), Some(task(1)), "never the second waiter");
    }

    #[test]
    fn set_owner_redirects_the_wake() {
        let mut q = SendCapacityQueue::new();
        let id = q.enqueue(1, 0, 1, task(1));
        q.set_owner(id, task(2));
        assert_eq!(q.head_ready(1), Some(task(2)));

        let behind = q.enqueue(1, 0, 1, task(5));
        q.set_owner(behind, task(6));
        assert_eq!(
            q.mark_submitted(id),
            Some(task(6)),
            "promotion uses the new owner"
        );

        q.set_owner(id, task(3));
        assert_eq!(
            q.complete(id, Ok(0)),
            Some(task(3)),
            "completion uses the new owner"
        );

        // Unknown id: ignored.
        q.set_owner(BoundedSendId(u64::MAX), task(9));
        assert_eq!(q.head_ready(1), Some(task(6)));
    }
}
