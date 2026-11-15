//! Per-worker completion table for awaited sends, and the admission FIFO
//! for bounded sends.
//!
//! Every awaited send (`SendHalf::send`, `submit_batch_await`, `send_chain`,
//! `send_admitted`, `send_backpressured`) holds an entry here, named by the
//! [`SendId`] its future keeps. The driver settles the entry when that send
//! completes, and the future resolves with the result of that one send. A
//! bounded send (`SendHalf::send_admitted`, `send_backpressured`) is also
//! admission-controlled: under the default order it may only reserve
//! copy-pool slots once it is the *oldest* waiter on the worker and the pool
//! can admit its whole message (see "Admission orders" below for the others).
//!
//! The table is plain worker-local state owned by the [`Executor`]. Every
//! entry holds a slot indexed by its id, and is also listed under its target
//! connection and, when a connection task owns it, under that task; waiting
//! bounded sends are listed in arrival order. `register`, `set_owner`,
//! `complete`, `take_result`, `mark_submitted` and `cancel` are index
//! lookups; `remove_connection`, `fail_waiting` and `abort_connection`
//! visit the entries listed under their connection. An entry that leaves the
//! waiting state leaves its id in the waiting list, and the reads of the head
//! pop the ids at the front that no longer name a waiting entry, so each id is
//! pushed once and leaves once, popped at the front or dropped by `compact`.
//! Design:
//! `docs/send-completion-slots-design.md`, `docs/send-admission-design.md`
//! (section 2).
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
//! **An entry leaves the table through its future, never through its
//! owner.** The future's `Drop` cancels the entry (`cancel`): a waiting one
//! goes, an in-flight one becomes `Abandoned` and its completion frees it,
//! a resolved one goes with its result. The owning task's death changes no
//! entry's state. A connection task is dropped with the driver state
//! installed, in its task-exit arm and at its connection's release
//! ([`Executor::remove_connection`] runs under `with_driver_state`), so
//! every future that died with it cancels its entry; `cancel` finds the
//! entry by id, so it does not matter that the `Ready` arms drop the future
//! after `remove_task` ran. An entry still owned by the dead task is
//! therefore held by a future the task handed to another task that has not
//! polled it yet: it is re-owned to [`NO_OWNER`] and kept, resolved or in
//! flight, and that task's first poll (`set_owner`) claims it.
//!
//! **A connection task's exit and its connection's release are separate
//! steps.** When a connection task returns or panics, the backend's
//! task-exit arm calls [`Executor::remove_connection_task`], which re-owns
//! the entries the task owned (`remove_task`); the connection is then
//! closing, and the driver delivers or fails every queued send on it, waking
//! the live owners through [`complete`](SendCompletions::complete), before
//! the slot is released. `remove_connection` runs at the release
//! ([`Executor::remove_connection`], from io_uring's `handle_close` and
//! mio's `drain_pending_closes`) and aborts whatever is still outstanding on
//! the connection.
//!
//! **A synthetic teardown abort is provisional.** `remove_connection`
//! records `Aborted`, not `Done`, and `complete` overwrites it with the
//! driver's result: a message that was fully delivered must never be
//! reported to its caller as aborted. In the current run loops no driver
//! result reaches `complete` after the release: mio finalizes a close only
//! once `pending_sends` is empty, io_uring commits the Close only once the
//! queue is empty and nothing is in flight, and a CQE that outlives the slot
//! goes to `forget`. The rule has no effect today and stays so that
//! `remove_connection` is correct from any call site. An `Aborted` that
//! nothing overwrites still
//! resolves the future ([`take_result`](SendCompletions::take_result) takes
//! it), so a genuinely torn-down send does not hang. "First result wins"
//! survives only between two *driver* results for one id.
//!
//! Every entry records the task id that owns it. The owner is refreshed on
//! each poll of its future (`set_owner`), and the `Executor` wrappers call `wake_task` immediately on
//! whatever id the queue returns — no deferred wake list. Between a
//! completion and the next poll no poll runs, so the owner resolved at
//! completion time is the owner that will be polled.
//!
//! Admission design: `docs/send-capacity-fifo-design.md`.
//!
//! **Admission orders.** The worker's [`SendAdmissionOrder`] is set once,
//! when the executor is created. Under `Strict`, the default, only the head
//! is admitted, through [`turn`](SendCompletions::turn) and
//! [`head_ready`](SendCompletions::head_ready). The entry points the other
//! orders share with it (`Executor::wake_send_capacity_in`,
//! `Executor::send_scan_due`, `set_order`, `order`) test the order or a flag
//! only the other orders set and take the `Strict` path; the rest of what
//! this paragraph describes does not run under `Strict`. The other orders
//! are an experiment
//! (`docs/send-admission-design.md`, section 4, "Admission orders"): while
//! the head does not fit, they also admit a waiter behind it
//! ([`admit`](SendCompletions::admit)). A waiter admitted that way leaves its
//! id in `waiting`, as a cancelled waiter does, so the rule above still
//! holds: each id is pushed once and leaves once. A waiter can become able to
//! pass without its own poll noticing: when capacity comes back, when the
//! head changes, when an earlier waiter on its connection leaves, or under
//! `BoundedIdle` and `BudgetIdle` when its connection goes idle. The first
//! is the release hook's wake; for the others the queue sets `scan_due`, and
//! the event loop's end-of-iteration capacity wake then runs
//! [`scan`](SendCompletions::scan), which wakes the waiters behind the head
//! that could pass. Those polls check again with `admit`. A poll whose
//! `admit` says wait also sets `scan_due`, so a scan that woke a waiter which
//! then could not pass (the pool changed in between) is followed by another.
//!
//! `SplitPool` passes no head. Its waiters stay in the one `waiting` list,
//! each marked for the small FIFO or the main one, and each FIFO's head is the
//! oldest waiter marked for it whose connection has no earlier waiter in
//! either FIFO ([`region_head`](SendCompletions::region_head)). The same
//! `scan_due` and release hook reach its wake, which wakes each FIFO's head
//! whose region has room.

use std::collections::VecDeque;
use std::io;

use super::Executor;
use crate::buffer::send_copy::SendCopyPool;

/// The order in which a worker's admission FIFO admits bounded sends
/// ([`SendHalf::send_admitted`](crate::SendHalf::send_admitted),
/// [`send_backpressured`](crate::SendHalf::send_backpressured)). An
/// experiment option, outside semver; set with
/// [`ConfigBuilder::send_admission_order`](crate::ConfigBuilder::send_admission_order).
///
/// In every order the FIFO's head is admitted when the free copy-pool slots
/// cover its whole message. The orders differ in what happens while they do
/// not. A waiter behind the head is then admitted only if its message fits in
/// the free slots and no earlier waiter on its own connection is still
/// waiting, so each connection's sends keep their order, and only if the
/// order's own condition holds. A send admitted that way has passed the head.
///
/// Terms, as in `docs/send-admission-design.md`, section 4: P is the pool's
/// slots, F the free slots, H the slots the head needs, and B the slots held
/// by sends that passed a head, counted until each slot returns to the pool,
/// across changes of head.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SendAdmissionOrder {
    /// Only the head is admitted (O0). The default.
    #[default]
    Strict,
    /// A waiter behind the head passes whenever it fits (O2).
    FirstFit,
    /// As `FirstFit`, while F + B < H (O3). Once F + B >= H is seen while a
    /// head waits, nothing passes until that head is admitted or leaves the
    /// FIFO.
    Bounded,
    /// As `Bounded`, and only a waiter whose connection has no send queued or
    /// in flight and whose most recent send did not wait for room in the
    /// socket (O4). How each backend decides that a send waited is in
    /// `ConnSendState::room_waited`; with every send on the connection
    /// awaited, the two agree on this rule.
    BoundedIdle,
    /// A waiter passes while B plus its own slots is at most the smaller of
    /// this many slots and P - H (O7). No latch.
    Budget(u16),
    /// As `Budget`, and only a waiter whose connection meets `BoundedIdle`'s
    /// condition (O7w). No latch.
    BudgetIdle(u16),
    /// Two pools (O5): the copy pool gains a small region of `small_slots`
    /// slots of `small_slot_size` bytes. A bounded send on a plain (not TLS)
    /// connection whose message fits one small slot takes a small slot, and
    /// waits in the small FIFO; every other send uses the main region, and a
    /// bounded one waits in the main FIFO, as under `Strict`. Each FIFO is
    /// strict: its head is admitted when its region has room for it. A waiter
    /// whose connection has an earlier waiter in either FIFO is not its
    /// FIFO's head until that waiter leaves, so it holds back no other
    /// waiter. Nothing passes a head, so B stays 0.
    SplitPool {
        /// Slots in the small region.
        small_slots: u16,
        /// Bytes per small slot.
        small_slot_size: u32,
    },
}

impl SendAdmissionOrder {
    /// Whether the order lets a waiter pass a head that does not fit, so the
    /// pool keeps the passing marks B counts: every order but `Strict` and
    /// `SplitPool`.
    pub(crate) fn lets_pass(self) -> bool {
        !matches!(
            self,
            SendAdmissionOrder::Strict | SendAdmissionOrder::SplitPool { .. }
        )
    }

    /// The small region `SplitPool` gives the copy pool, as `(slots, bytes
    /// per slot)`.
    pub(crate) fn small_region(self) -> Option<(u16, u32)> {
        match self {
            SendAdmissionOrder::SplitPool {
                small_slots,
                small_slot_size,
            } => Some((small_slots, small_slot_size)),
            _ => None,
        }
    }

    /// Whether a waiter passes only from a connection with no send queued or
    /// in flight whose most recent send did not wait for room:
    /// `BoundedIdle` and `BudgetIdle`. The drivers keep each connection's
    /// room-wait flags only under these orders.
    pub(crate) fn needs_idle_connection(self) -> bool {
        matches!(
            self,
            SendAdmissionOrder::BoundedIdle | SendAdmissionOrder::BudgetIdle(_)
        )
    }
}

/// The copy-pool figures an order other than `Strict` reads, taken from the
/// pool when an admission check or a wake scan runs.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PoolView {
    /// Slots available to a new allocation (F).
    pub(crate) free: usize,
    /// Slots held by sends that passed a head (B).
    pub(crate) passing: usize,
    /// Slots in the pool's main region (P).
    pub(crate) slots: usize,
    /// Slots of the small region available, under `SplitPool`.
    pub(crate) small_free: usize,
    /// The largest F + B at a return since the previous view taken with
    /// [`take`](Self::take), for the latch of `Bounded` and `BoundedIdle`;
    /// 0 when there was none or it is unknown, and then `free + passing` is
    /// all the latch reads.
    pub(crate) free_plus_passing_peak: usize,
}

impl PoolView {
    /// The figures of `pool` now, with no F + B peak. Tests only: the
    /// runtime reads the peak with every view.
    #[cfg(test)]
    pub(crate) fn of(pool: &SendCopyPool) -> Self {
        Self::with_peak(pool, 0)
    }

    /// The figures of `pool` now, with the largest F + B at a return since
    /// the previous `take` (`SendCopyPool::take_free_plus_passing_peak`),
    /// which starts the next window empty.
    ///
    /// A head that changes because its predecessor was admitted starts with
    /// an empty window: the admission check took the window, and the admitted
    /// head's own reservation follows it with no return in between. A head
    /// that changes by a cancel or a teardown inherits the returns since the
    /// last check, which belong to its predecessor's wait. Resetting the
    /// window there would need the pool in `SendCompletions::cancel` and
    /// `remove_connection`, which a future's `Drop` and the release path
    /// reach with the executor alone. The latch then closes early only if
    /// F + B reached the new head's need at such a return and fell back below
    /// it before the head changed, which needs a send outside the FIFO to
    /// take the slots in between.
    pub(crate) fn take(pool: &mut SendCopyPool) -> Self {
        let peak = pool.take_free_plus_passing_peak();
        Self::with_peak(pool, peak)
    }

    /// The figures of `pool` now, with `free_plus_passing_peak` as given.
    pub(crate) fn with_peak(pool: &SendCopyPool, free_plus_passing_peak: usize) -> Self {
        PoolView {
            free: pool.free_count(),
            passing: pool.passing_held(),
            slots: pool.slot_count(),
            small_free: pool.small_free_count(),
            free_plus_passing_peak,
        }
    }
}

/// What [`SendCompletions::admit`] decided for one waiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Admit {
    /// Not now; the future parks.
    Wait,
    /// The waiter is the head and fits.
    Head,
    /// The waiter passes the head, which does not fit. `slots` is the
    /// waiter's `required_slots`.
    Pass { slots: usize },
    /// Under `SplitPool`, the waiter is the small FIFO's head and a small
    /// slot is free.
    Small,
}

/// What the admission orders did on one worker, for the Experiment C server
/// (`docs/send-admission-design.md`, section 4). Read with
/// [`send_admission_counters`]. Every count starts at zero when the worker
/// starts and only grows, except `passing_held` and the free slots, which are
/// figures of now; under `Strict` all but `main_free` stay zero.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SendAdmissionCounters {
    /// Sends admitted past a waiting head.
    pub passing_admissions: u64,
    /// The copy-pool slots those sends were admitted against (each one's
    /// worst-case cost, as the FIFO counts it), summed.
    pub passing_slots: u64,
    /// The slots sends that passed a head hold now (B).
    pub passing_held: u64,
    /// The largest `passing_held` has been.
    pub passing_held_max: u64,
    /// Under `Bounded` and `BoundedIdle`, the heads whose latch closed while
    /// they did not fit: F < H <= F + B, the slots of passing sends making
    /// the difference. A latch that closes while its head fits is not
    /// counted.
    pub latch_closings: u64,
    /// Wake scans that looked behind a head that did not fit.
    pub wake_scans: u64,
    /// The waiters those scans woke.
    pub scan_wakes: u64,
    /// Slots of the main region free now.
    pub main_free: u64,
    /// Under `SplitPool`, slots of the small region free now.
    pub small_free: u64,
    /// Under `SplitPool`, sends admitted from the small FIFO.
    pub small_admissions: u64,
}

/// The admission counters of the worker this runs on: see
/// [`SendAdmissionCounters`].
///
/// # Panics
///
/// Outside a task or handler callback on a ringline worker.
#[doc(hidden)]
pub fn send_admission_counters() -> SendAdmissionCounters {
    super::io::with_state(|driver, executor| {
        executor.send_admission_counters(&driver.send_copy_pool)
    })
}

/// Whether arrival number `a` came before `b`. Arrival numbers wrap; the
/// waiters in the FIFO at any one time span far fewer than 2^31 of them.
fn arrived_before(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) < 0
}

/// Identity of one send operation on its worker: a slot in
/// [`SendCompletions`]'s table and that slot's generation.
///
/// A slot's generation advances each time the slot is freed, so an id that
/// outlives its operation never matches the slot's next occupant: every
/// method treats it as unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SendId {
    index: u32,
    generation: u32,
}

/// The owner of an entry whose owning task is gone while the future that
/// holds it is alive, handed to a task that has not polled it yet. A wake
/// for it reaches no task ([`Executor::wake_task`] finds no slot); the next
/// poll records the polling task as the owner.
pub(crate) const NO_OWNER: u32 = u32::MAX;

/// Where one operation stands.
enum Completion {
    /// Waiting for admission, in the FIFO. `generation` is the target
    /// connection's generation, which `fail_waiting` matches; the slot's own
    /// generation is unrelated.
    Waiting {
        generation: u32,
        /// Copy-pool slots the whole message needs; `turn` admits the head
        /// only when at least this many are free.
        required_slots: usize,
        /// Arrival number, which orders the waiters of one connection: under
        /// an order other than `Strict` a waiter passes only if no waiter on
        /// its connection arrived before it. `by_conn` keeps no order. 0
        /// under `Strict`, which never reads it.
        seq: u32,
        /// Under `SplitPool`, the waiter takes a slot of the small region and
        /// waits in the small FIFO; `required_slots` is then 1.
        small: bool,
        /// The scan (`scan_epoch`) that last woke this waiter to pass, if it
        /// has not polled since; 0 if none. A waiter woken and not polled by
        /// the next scan loses its claim on the room it was woken for.
        woken_in: u32,
    },
    /// Submitted to the driver; no completion yet.
    InFlight,
    /// The driver reported; the result waits for `take_result`. Final — a
    /// second driver result for the same id is ignored.
    Done(io::Result<u32>),
    /// Teardown recorded this before the driver reported. Provisional: a
    /// real driver result overwrites it (`complete`), and if none ever
    /// arrives `take_result` hands this error to the future. See the module
    /// docs for the provisional-abort rule.
    Aborted(io::Error),
    /// The future was dropped while the operation was in flight. The driver
    /// still owns the operation; its completion frees the slot. Nothing
    /// polls a dropped future, so nothing leaves this state but that
    /// completion.
    Abandoned,
}

/// A live operation.
struct Entry {
    /// The connection the send targets.
    conn_index: u32,
    /// Task to wake on admission or completion.
    task_id: u32,
    state: Completion,
}

/// One slot of the table: its current generation and, while an operation
/// holds it, that operation.
struct Slot {
    generation: u32,
    entry: Option<Entry>,
}

/// Completion table for awaited sends, and admission FIFO for bounded
/// sends, on one worker.
///
/// Owned by [`Executor::send_completions`]; the `Executor` methods further down
/// in this file are the only intended entry points outside tests, because
/// they pair every returned task id with `wake_task`.
#[derive(Default)]
pub(crate) struct SendCompletions {
    /// Every operation, waiting or admitted, indexed by
    /// `SendId::index`.
    slots: Vec<Slot>,
    /// Indices of the slots no operation holds.
    free: Vec<u32>,
    /// Waiting operations in arrival order. Under `Strict` only the head may
    /// be admitted; the other orders also admit waiters behind it. Also
    /// holds ids that no longer name a waiting entry, which
    /// [`front`](Self::front) pops when they reach the head and
    /// [`compact`](Self::compact) drops when they number more than the live
    /// ones plus 64.
    waiting: VecDeque<SendId>,
    /// How many ids in `waiting` name a waiting entry.
    waiting_live: usize,
    /// Ids of `waiting` examined, for the tests that bound it.
    #[cfg(test)]
    visited: usize,
    /// Slot indices of the live operations targeting each connection,
    /// indexed by connection index.
    by_conn: Vec<Vec<u32>>,
    /// Slot indices of the live operations owned by each connection task,
    /// indexed by task id. Standalone tasks and [`NO_OWNER`] are not indexed:
    /// [`remove_task`](Self::remove_task) and
    /// [`remove_connection`](Self::remove_connection) never look them up.
    by_task: Vec<Vec<u32>>,
    /// The worker's admission order, set when the executor is created.
    order: SendAdmissionOrder,
    /// The arrival number the next waiter gets, under an order other than
    /// `Strict`.
    next_seq: u32,
    /// The number of the latest scan that looked behind the head, under the
    /// orders that pass; never 0, which `woken_in` uses for none.
    scan_epoch: u32,
    /// Under `Bounded` and `BoundedIdle`, the head while whose wait
    /// F + B >= H has been seen: nothing passes it. A later head is another
    /// id, so the latch opens when the head changes.
    latched_head: Option<SendId>,
    /// A [`scan`](Self::scan) is due at the end of the event-loop iteration.
    /// Set only under an order other than `Strict`, when a waiter leaves the
    /// waiting state and when a later poll of a waiter is told to wait.
    scan_due: bool,
    /// The task ids a scan wakes, kept between scans by
    /// [`Executor::wake_send_capacity_in`] so that a scan allocates only
    /// when it wakes more tasks than any scan before it.
    wake_scratch: Vec<u32>,
    /// The counts the FIFO keeps; `passing_held` and `passing_held_max`
    /// stay zero here and are read from the pool
    /// ([`Executor::send_admission_counters`]).
    counters: SendAdmissionCounters,
}

/// Record `index` under `key`.
fn index_add(lists: &mut Vec<Vec<u32>>, key: u32, index: u32) {
    let key = key as usize;
    if lists.len() <= key {
        lists.resize_with(key + 1, Vec::new);
    }
    lists[key].push(index);
}

/// Forget `index` under `key`. A list holds the few operations of one
/// connection or task, so the search is short.
fn index_remove(lists: &mut [Vec<u32>], key: u32, index: u32) {
    if let Some(list) = lists.get_mut(key as usize)
        && let Some(pos) = list.iter().position(|&i| i == index)
    {
        list.swap_remove(pos);
    }
}

/// Whether `task_id` is a connection task, the only kind `by_task` indexes.
fn is_connection_task(task_id: u32) -> bool {
    task_id & crate::runtime::waker::STANDALONE_BIT == 0
}

impl SendCompletions {
    /// Empty queue, in the `Strict` order. Called once per worker from
    /// `Executor::new`.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Empty queue in `order`.
    #[cfg(test)]
    fn with_order(order: SendAdmissionOrder) -> Self {
        let mut q = Self::new();
        q.set_order(order);
        q
    }

    /// Set the admission order. Called once, before any waiter is enqueued,
    /// by [`Executor::set_send_admission_order`].
    pub(crate) fn set_order(&mut self, order: SendAdmissionOrder) {
        debug_assert_eq!(self.waiting_live, 0, "the order changed under waiters");
        self.order = order;
    }

    /// The admission order.
    pub(crate) fn order(&self) -> SendAdmissionOrder {
        self.order
    }

    /// Put `entry` in a free slot and return its id.
    fn insert(&mut self, entry: Entry) -> SendId {
        let index = match self.free.pop() {
            Some(index) => index,
            None => {
                let index = u32::try_from(self.slots.len()).expect("send id space exhausted");
                self.slots.push(Slot {
                    generation: 0,
                    entry: None,
                });
                index
            }
        };
        index_add(&mut self.by_conn, entry.conn_index, index);
        if is_connection_task(entry.task_id) {
            index_add(&mut self.by_task, entry.task_id, index);
        }
        let slot = &mut self.slots[index as usize];
        slot.entry = Some(entry);
        SendId {
            index,
            generation: slot.generation,
        }
    }

    /// The live operation `id` names, or `None` for a stale or unknown id.
    fn entry(&self, id: SendId) -> Option<&Entry> {
        self.slots
            .get(id.index as usize)
            .filter(|slot| slot.generation == id.generation)
            .and_then(|slot| slot.entry.as_ref())
    }

    /// Mutable form of [`entry`](Self::entry).
    fn entry_mut(&mut self, id: SendId) -> Option<&mut Entry> {
        self.slots
            .get_mut(id.index as usize)
            .filter(|slot| slot.generation == id.generation)
            .and_then(|slot| slot.entry.as_mut())
    }

    /// Free the slot `id` names and return its operation. The slot's
    /// generation advances, so `id` and every copy of it go stale.
    fn remove(&mut self, id: SendId) -> Option<Entry> {
        if self.slots.get(id.index as usize)?.generation != id.generation {
            return None;
        }
        self.free_index(id.index)
    }

    /// Free slot `index`, whatever its generation, and return its operation.
    fn free_index(&mut self, index: u32) -> Option<Entry> {
        let slot = &mut self.slots[index as usize];
        let entry = slot.entry.take()?;
        slot.generation = slot.generation.wrapping_add(1);
        self.free.push(index);
        index_remove(&mut self.by_conn, entry.conn_index, index);
        if is_connection_task(entry.task_id) {
            index_remove(&mut self.by_task, entry.task_id, index);
        }
        Some(entry)
    }

    /// Whether `id` names a waiting entry.
    fn is_waiting(&self, id: SendId) -> bool {
        matches!(
            self.entry(id),
            Some(Entry {
                state: Completion::Waiting { .. },
                ..
            })
        )
    }

    /// The FIFO's head: pops the ids at the front that no longer name a
    /// waiting entry, and returns the first that does.
    fn front(&mut self) -> Option<SendId> {
        while let Some(&id) = self.waiting.front() {
            #[cfg(test)]
            {
                self.visited += 1;
            }
            if self.is_waiting(id) {
                return Some(id);
            }
            self.waiting.pop_front();
        }
        None
    }

    /// An entry left the waiting state without leaving `waiting`. Drops the
    /// stale ids once they number more than the live ones plus 64, so that a
    /// head that waits for a long time does not leave the list growing with
    /// every waiter behind it that times out. The `retain` visits every id;
    /// when it runs the stale ids are more than half of them, and each went
    /// stale after the previous `retain`, so its cost is less than two visits
    /// per entry that left the waiting state.
    fn left_waiting(&mut self) {
        self.waiting_live -= 1;
        self.waiter_left();
        self.compact_if_mostly_stale();
    }

    /// [`compact`](Self::compact) once the stale ids in `waiting` number
    /// more than the live ones plus 64.
    fn compact_if_mostly_stale(&mut self) {
        if self.waiting.len() > 2 * self.waiting_live + 64 {
            self.compact();
        }
    }

    /// A waiter left the waiting state. Under an order other than `Strict`
    /// that can let a waiter behind the head pass: the head may have changed,
    /// or the waiter that left was the earliest of its connection. The scan
    /// at the end of the iteration wakes whoever can.
    fn waiter_left(&mut self) {
        if self.order != SendAdmissionOrder::Strict {
            self.scan_due = true;
        }
    }

    /// Drop every id in `waiting` that no longer names a waiting entry.
    fn compact(&mut self) {
        let slots = &self.slots;
        #[cfg(test)]
        {
            self.visited += self.waiting.len();
        }
        self.waiting.retain(|&id| {
            let slot = &slots[id.index as usize];
            slot.generation == id.generation
                && matches!(
                    slot.entry,
                    Some(Entry {
                        state: Completion::Waiting { .. },
                        ..
                    })
                )
        });
    }

    /// The task owning the FIFO's head, if any.
    fn head_task(&mut self) -> Option<u32> {
        let id = self.front()?;
        self.entry(id).map(|e| e.task_id)
    }

    /// Append a waiter and return its id.
    ///
    /// Called by `Admission` on its first poll, with the
    /// connection it targets, the number of copy-pool slots its message
    /// needs, and the polling task's id.
    pub(crate) fn enqueue(
        &mut self,
        conn_index: u32,
        generation: u32,
        required_slots: usize,
        task_id: u32,
    ) -> SendId {
        self.enqueue_in(conn_index, generation, required_slots, task_id, false)
    }

    /// Under `SplitPool`, append a waiter of the small FIFO, which needs one
    /// small slot, and return its id. `Admission`'s first poll, for a send
    /// `bounded_send_small` puts in the small region.
    pub(crate) fn enqueue_small(
        &mut self,
        conn_index: u32,
        generation: u32,
        task_id: u32,
    ) -> SendId {
        debug_assert!(self.order.small_region().is_some());
        self.enqueue_in(conn_index, generation, 1, task_id, true)
    }

    /// [`enqueue`](Self::enqueue) into the small FIFO if `small`.
    fn enqueue_in(
        &mut self,
        conn_index: u32,
        generation: u32,
        required_slots: usize,
        task_id: u32,
        small: bool,
    ) -> SendId {
        let seq = if self.order == SendAdmissionOrder::Strict {
            0
        } else {
            let seq = self.next_seq;
            self.next_seq = seq.wrapping_add(1);
            seq
        };
        let id = self.insert(Entry {
            conn_index,
            task_id,
            state: Completion::Waiting {
                generation,
                required_slots,
                seq,
                small,
                woken_in: 0,
            },
        });
        self.waiting.push_back(id);
        self.waiting_live += 1;
        id
    }

    /// Register an awaited send that needs no admission, already `InFlight`,
    /// and return its id.
    ///
    /// Called by the awaiting send APIs (`SendHalf::send`,
    /// `submit_batch_await`, `send_chain`) before they
    /// submit. A submission that fails must [`withdraw`](Self::withdraw) the id
    /// it registered.
    pub(crate) fn register(&mut self, conn_index: u32, task_id: u32) -> SendId {
        self.insert(Entry {
            conn_index,
            task_id,
            state: Completion::InFlight,
        })
    }

    /// Record the task that currently owns `id`, whether it is waiting or
    /// submitted. A no-op for an unknown id.
    ///
    /// Called by the send futures on every poll after the first, so that a
    /// future moved between tasks (or polled from a different task than the
    /// one that enqueued it) is woken where it now lives, and so that an
    /// entry its creator's exit left with [`NO_OWNER`] has one again.
    pub(crate) fn set_owner(&mut self, id: SendId, task_id: u32) {
        let Some(e) = self.entry_mut(id) else {
            return;
        };
        let old = std::mem::replace(&mut e.task_id, task_id);
        if old != task_id {
            if is_connection_task(old) {
                index_remove(&mut self.by_task, old, id.index);
            }
            if is_connection_task(task_id) {
                index_add(&mut self.by_task, task_id, id.index);
            }
        }
    }

    /// Remove `id`, which no submission took: its send was refused or
    /// queued nothing, so no completion will ever arrive for it. Unlike
    /// [`cancel`](Self::cancel), which leaves an in-flight entry for its
    /// completion to free, this frees it now.
    ///
    /// Called by the awaited send APIs when their submission fails.
    pub(crate) fn withdraw(&mut self, id: SendId) {
        if matches!(self.entry(id).map(|e| &e.state), Some(Completion::InFlight)) {
            self.remove(id);
        }
    }

    /// A completion for `id` arrived that must not settle it, because its
    /// connection has been torn down: teardown already recorded the abort,
    /// and a driver result would override it. An `Abandoned` entry waits
    /// for exactly this completion, so it is freed; any other state is left
    /// for its future to take.
    ///
    /// Called by the completion handlers' stale-generation branches.
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    pub(crate) fn forget(&mut self, id: SendId) {
        if matches!(
            self.entry(id).map(|e| &e.state),
            Some(Completion::Abandoned)
        ) {
            self.remove(id);
        }
    }

    /// Whether `id` may reserve slots now: it must be the head of the queue
    /// *and* `free_slots` must cover its `required_slots`.
    ///
    /// Called by `Admission` on each poll while waiting; a `false` parks
    /// the future until [`head_ready`](Self::head_ready) or a promotion
    /// wakes it.
    pub(crate) fn turn(&mut self, id: SendId, free_slots: usize) -> bool {
        self.front() == Some(id) && self.head_ready(free_slots).is_some()
    }

    /// Move `id` from waiting to submitted (`InFlight`).
    ///
    /// Returns the new head's task id, so the caller can wake it (it
    /// re-checks [`turn`](Self::turn) on its next poll). Returns `None` if
    /// `id` is not the head, which under `Strict` [`turn`](Self::turn) has
    /// just checked it is and under the other orders means it passed the
    /// head ([`admit`](Self::admit)), or if there is no new head. A waiter
    /// that passed leaves its id in `waiting`, for `front` or `compact` to
    /// drop.
    ///
    /// Called by `Admission` right after it reserved its slots and submitted
    /// the send.
    pub(crate) fn mark_submitted(&mut self, id: SendId) -> Option<u32> {
        if !self.is_waiting(id) {
            return None;
        }
        let at_front = self.front() == Some(id);
        self.entry_mut(id)?.state = Completion::InFlight;
        if at_front {
            self.waiting.pop_front();
            self.waiting_live -= 1;
            self.waiter_left();
            // Under the orders that pass, waiters behind this head can have
            // left already, and the scan's cost rests on `waiting` holding at
            // most twice the live ids plus 64. `Strict` never scans, and keeps
            // the pop it always had.
            if self.order != SendAdmissionOrder::Strict {
                self.compact_if_mostly_stale();
            }
            self.head_task()
        } else {
            self.left_waiting();
            None
        }
    }

    /// Deliver the driver's completion of `id` — the authoritative result.
    ///
    /// `InFlight` becomes `Done(result)` and the owner's task id is returned
    /// for waking. `Aborted` does the same: a teardown abort is provisional,
    /// and a driver result that arrives after it is authoritative (module
    /// docs). `Abandoned` entries are removed and `result` is
    /// discarded (the future is gone). An entry that is already `Done` keeps
    /// its result — between two *driver* results for one id the first wins.
    /// A waiting entry is not in flight and ignores it. An unknown id is
    /// ignored: it is a stale completion for an entry that
    /// [`remove_connection`](Self::remove_connection) already resolved and
    /// the future already collected.
    ///
    /// Called by the backends' send-completion handlers.
    pub(crate) fn complete(&mut self, id: SendId, result: io::Result<u32>) -> Option<u32> {
        let e = self.entry_mut(id)?;
        match e.state {
            Completion::InFlight | Completion::Aborted(_) => {
                e.state = Completion::Done(result);
                Some(e.task_id)
            }
            Completion::Abandoned => {
                self.remove(id);
                None
            }
            Completion::Done(_) | Completion::Waiting { .. } => None,
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
    /// Called by the send futures on each poll after submission; `None`
    /// parks it until [`complete`](Self::complete) wakes it.
    pub(crate) fn take_result(&mut self, id: SendId) -> Option<io::Result<u32>> {
        if !matches!(
            self.entry(id)?.state,
            Completion::Done(_) | Completion::Aborted(_)
        ) {
            return None;
        }
        match self.remove(id)?.state {
            Completion::Done(result) => Some(result),
            Completion::Aborted(err) => Some(Err(err)),
            _ => unreachable!("checked above"),
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
    /// Called from `SendCompletion`'s and `Admission`'s `Drop`. A
    /// connection task is dropped with the driver state installed, at its
    /// exit and at its connection's release (`with_driver_state`), so its
    /// futures reach this. A drop outside the executor (on another thread,
    /// or in the synchronous body of `on_accept`) skips it and leaves the
    /// entry in the table for the life of the worker.
    pub(crate) fn cancel(&mut self, id: SendId) -> Option<u32> {
        let e = self.entry_mut(id)?;
        match e.state {
            Completion::Waiting { .. } => {
                let was_head = self.front() == Some(id);
                self.remove(id);
                self.left_waiting();
                if was_head {
                    return self.head_task();
                }
            }
            Completion::InFlight => e.state = Completion::Abandoned,
            Completion::Done(_) | Completion::Aborted(_) => {
                self.remove(id);
            }
            Completion::Abandoned => {}
        }
        None
    }

    /// The head's task id if `free_slots` covers its `required_slots`.
    ///
    /// The driver's slot-release hook calls this through
    /// [`Executor::wake_send_capacity`] after every pool release, so the
    /// head is woken only when it can now fit rather than on every release.
    pub(crate) fn head_ready(&mut self, free_slots: usize) -> Option<u32> {
        let head = self.front()?;
        let e = self.entry(head)?;
        match e.state {
            Completion::Waiting { required_slots, .. } if free_slots >= required_slots => {
                Some(e.task_id)
            }
            _ => None,
        }
    }

    /// Under an order other than `Strict`, whether `id` may reserve slots
    /// now: as the head, when `pool.free` covers its `required_slots`, or
    /// past a head that does not fit, when the order lets it
    /// ([`may_pass`](Self::may_pass)) with what is left once the older
    /// waiters that could pass have taken their room
    /// ([`left_after_older_passers`](Self::left_after_older_passers)), so
    /// passers go oldest first whichever polls first. An older waiter that a
    /// scan woke keeps that claim only until the next scan; one its task does
    /// not poll then stops holding the newer ones back
    /// ([`could_pass`](Self::could_pass)). `idle` says, for a
    /// connection index, whether the connection has no send queued or in
    /// flight and its most recent send did not wait for room; it is called
    /// only under `BoundedIdle` and `BudgetIdle`, for `id` once it passes
    /// every other test and for the older waiters. Under `SplitPool` it
    /// admits each FIFO's head when its region has room
    /// ([`admit_split`](Self::admit_split)).
    ///
    /// An answer of wait on any poll but the first sets `scan_due`: the
    /// poll may have come from a scan that counted on this waiter's slots,
    /// or the head may have stopped fitting because a send outside the FIFO
    /// took slots, and either way another waiter may now be able to pass.
    /// So does a wait because an older waiter takes the room, on any poll,
    /// so that the scan wakes the older one.
    ///
    /// Cost, for a waiter that would pass on its own: one visit per id ahead
    /// of it in `waiting`, and the operations of each older waiter's
    /// connection that fits. A rejection on its own test costs nothing more.
    ///
    /// Called by `Admission` on each poll while waiting, in place of
    /// [`turn`](Self::turn).
    pub(crate) fn admit(
        &mut self,
        id: SendId,
        pool: PoolView,
        first_poll: bool,
        idle: impl Fn(u32) -> bool,
    ) -> Admit {
        debug_assert_ne!(self.order, SendAdmissionOrder::Strict);
        // This is the poll a scan's wake asked for, if one did.
        if let Some(Entry {
            state: Completion::Waiting { woken_in, .. },
            ..
        }) = self.entry_mut(id)
        {
            *woken_in = 0;
        }
        let admit = self.admit_inner(id, pool, idle);
        if admit == Admit::Wait && !first_poll {
            self.scan_due = true;
        }
        admit
    }

    /// [`admit`](Self::admit) without its effect on `scan_due`.
    fn admit_inner(&mut self, id: SendId, pool: PoolView, idle: impl Fn(u32) -> bool) -> Admit {
        if self.order.small_region().is_some() {
            return self.admit_split(id, pool);
        }
        let Some((head, head_needs)) = self.head_and_need() else {
            return Admit::Wait;
        };
        self.observe_latch(head, head_needs, pool);
        if pool.free >= head_needs {
            return if head == id { Admit::Head } else { Admit::Wait };
        }
        if head == id {
            return Admit::Wait;
        }
        let Some(Entry {
            conn_index,
            state:
                Completion::Waiting {
                    required_slots,
                    seq,
                    ..
                },
            ..
        }) = self.entry(id)
        else {
            return Admit::Wait;
        };
        let (conn_index, required_slots, seq) = (*conn_index, *required_slots, *seq);
        if !self.may_pass(head, head_needs, conn_index, required_slots, seq, pool) {
            return Admit::Wait;
        }
        if self.order.needs_idle_connection() && !idle(conn_index) {
            return Admit::Wait;
        }
        // Passers go oldest first, as the scan wakes them: an older waiter
        // that could pass takes its room before this one, even when this
        // poll comes first (a new waiter's first poll, in the same iteration
        // as the release that made the room). If what is left does not cover
        // this waiter, the scan at the end of the iteration wakes the older
        // one.
        let left = self.left_after_older_passers(head, head_needs, id, pool, &idle);
        if !self.may_pass(head, head_needs, conn_index, required_slots, seq, left) {
            self.scan_due = true;
            return Admit::Wait;
        }
        Admit::Pass {
            slots: required_slots,
        }
    }

    /// `pool` less what the waiters between the head and `id` that could
    /// pass take, oldest first, each counted against the free slots and B as
    /// [`scan`](Self::scan) counts the waiters it wakes.
    ///
    /// Cost: one visit per id in `waiting` ahead of `id`, stale ones
    /// included, and for each waiter that fits, the operations of its
    /// connection. It runs only for a waiter that would pass on its own.
    fn left_after_older_passers(
        &self,
        head: SendId,
        head_needs: usize,
        id: SendId,
        pool: PoolView,
        idle: &impl Fn(u32) -> bool,
    ) -> PoolView {
        let mut left = pool;
        for &older in self.waiting.iter().skip(1) {
            if older == id {
                break;
            }
            if let Some((required_slots, _)) =
                self.could_pass(older, head, head_needs, left, idle, false)
            {
                left.free -= required_slots;
                left.passing += required_slots;
            }
        }
        left
    }

    /// If `id` is a waiter with an owner that could pass `head` with
    /// `left`, its `required_slots` and owner. The test the scan applies to
    /// each waiter it might wake (`from_scan`), and that an admission applies
    /// to the waiters older than itself. A waiter with no owner
    /// ([`NO_OWNER`]) is left out: no task would poll it, and its holder's
    /// first poll checks it.
    ///
    /// A waiter a scan woke that has not polled since is left out of the
    /// scan, which would only wake the same task again; its task was woken,
    /// and polls it whenever it next awaits it. An admission still counts it
    /// while it was woken by the latest scan, which gives it the iteration
    /// that wake is polled in, and not after: a waiter its task does not poll
    /// would otherwise hold its room for as long as the task does not poll
    /// it, and every newer waiter behind it.
    fn could_pass(
        &self,
        id: SendId,
        head: SendId,
        head_needs: usize,
        left: PoolView,
        idle: &impl Fn(u32) -> bool,
        from_scan: bool,
    ) -> Option<(usize, u32)> {
        let Some(Entry {
            conn_index,
            task_id,
            state:
                Completion::Waiting {
                    required_slots,
                    seq,
                    woken_in,
                    ..
                },
        }) = self.entry(id)
        else {
            return None;
        };
        let woken_earlier = if from_scan {
            *woken_in != 0
        } else {
            *woken_in != 0 && *woken_in != self.scan_epoch
        };
        let passes = *task_id != NO_OWNER
            && !woken_earlier
            && self.may_pass(head, head_needs, *conn_index, *required_slots, *seq, left)
            && (!self.order.needs_idle_connection() || idle(*conn_index));
        passes.then_some((*required_slots, *task_id))
    }

    /// Under `SplitPool`: `id` is admitted when it is its FIFO's head
    /// ([`region_head`](Self::region_head)) and its region has room for it.
    fn admit_split(&mut self, id: SendId, pool: PoolView) -> Admit {
        let Some(Entry {
            state: Completion::Waiting { small, .. },
            ..
        }) = self.entry(id)
        else {
            return Admit::Wait;
        };
        let small = *small;
        let Some((head, required_slots, _)) = self.region_head(small) else {
            return Admit::Wait;
        };
        if head != id {
            return Admit::Wait;
        }
        match small {
            true if pool.small_free >= required_slots => Admit::Small,
            false if pool.free >= required_slots => Admit::Head,
            _ => Admit::Wait,
        }
    }

    /// Under `SplitPool`, the head of the small FIFO (`small`) or the main
    /// one: the oldest waiter of that FIFO whose connection has no earlier
    /// waiter in either FIFO. Returns its id, `required_slots` and owner.
    ///
    /// A waiter passed over here is held back by an earlier waiter on its
    /// connection in the other FIFO, so it holds back no waiter of its own.
    /// Visits `waiting` from the front up to the head, skipping stale ids and
    /// the other FIFO's waiters, and for each waiter of this FIFO the
    /// operations of its connection; the waiters held back that way are the
    /// only ones of this FIFO it visits before the head.
    fn region_head(&mut self, small: bool) -> Option<(SendId, usize, u32)> {
        self.front()?;
        self.waiting.iter().find_map(|&id| match self.entry(id) {
            Some(Entry {
                conn_index,
                task_id,
                state:
                    Completion::Waiting {
                        required_slots,
                        seq,
                        small: region,
                        ..
                    },
            }) if *region == small && !self.has_earlier_waiter(*conn_index, *seq) => {
                Some((id, *required_slots, *task_id))
            }
            _ => None,
        })
    }

    /// The FIFO's head and the slots it needs.
    fn head_and_need(&mut self) -> Option<(SendId, usize)> {
        let head = self.front()?;
        match self.entry(head)?.state {
            Completion::Waiting { required_slots, .. } => Some((head, required_slots)),
            _ => None,
        }
    }

    /// Whether a waiter behind `head`, which needs `head_needs` slots and
    /// does not fit, may pass it: its `required_slots` fit in `pool.free`, no
    /// waiter on `conn_index` arrived before it (`seq`), and the order allows
    /// it ([`order_allows`](Self::order_allows)). `pool.passing` is B as the
    /// caller counts it, which a scan raises by the waiters it has woken.
    fn may_pass(
        &self,
        head: SendId,
        head_needs: usize,
        conn_index: u32,
        required_slots: usize,
        seq: u32,
        pool: PoolView,
    ) -> bool {
        required_slots <= pool.free
            && !self.has_earlier_waiter(conn_index, seq)
            && self.order_allows(head, head_needs, required_slots, pool)
    }

    /// The order's own condition for a waiter of `required_slots` to pass
    /// `head`. `Bounded` and `BoundedIdle` read the latch, which
    /// [`observe_latch`](Self::observe_latch) has just closed if F + B >= H.
    fn order_allows(
        &self,
        head: SendId,
        head_needs: usize,
        required_slots: usize,
        pool: PoolView,
    ) -> bool {
        match self.order {
            SendAdmissionOrder::Strict | SendAdmissionOrder::SplitPool { .. } => false,
            SendAdmissionOrder::FirstFit => true,
            SendAdmissionOrder::Bounded | SendAdmissionOrder::BoundedIdle => {
                self.latched_head != Some(head)
            }
            SendAdmissionOrder::Budget(budget) | SendAdmissionOrder::BudgetIdle(budget) => {
                let others = pool.slots.saturating_sub(head_needs);
                pool.passing + required_slots <= usize::from(budget).min(others)
            }
        }
    }

    /// Under `Bounded` and `BoundedIdle`, close `head`'s latch if
    /// F + B >= H now, or was at a return since the previous admission check
    /// or scan (`pool.free_plus_passing_peak`). Called by every admission
    /// check and scan. See [`PoolView::take`] for when that window can hold a
    /// moment of the previous head's wait.
    fn observe_latch(&mut self, head: SendId, head_needs: usize, pool: PoolView) {
        if matches!(
            self.order,
            SendAdmissionOrder::Bounded | SendAdmissionOrder::BoundedIdle
        ) && self.latched_head != Some(head)
            && (pool.free + pool.passing).max(pool.free_plus_passing_peak) >= head_needs
        {
            self.latched_head = Some(head);
            if pool.free < head_needs {
                self.counters.latch_closings += 1;
            }
        }
    }

    /// Whether a waiter on `conn_index` that arrived before `seq` is still
    /// waiting. Visits the operations indexed under the connection, which
    /// are few.
    fn has_earlier_waiter(&self, conn_index: u32, seq: u32) -> bool {
        let Some(list) = self.by_conn.get(conn_index as usize) else {
            return false;
        };
        list.iter().any(|&index| {
            matches!(
                self.slots[index as usize].entry,
                Some(Entry {
                    state: Completion::Waiting { seq: earlier, .. },
                    ..
                }) if arrived_before(earlier, seq)
            )
        })
    }

    /// Whether a [`scan`](Self::scan) is due: the event loop runs its
    /// capacity wake at the end of the iteration when this is set, as it
    /// does when slots came back.
    pub(crate) fn scan_due(&self) -> bool {
        self.scan_due
    }

    /// The capacity wake under an order other than `Strict`, and the scan
    /// [`scan_due`](Self::scan_due) asks for: push onto `wakes` the head's
    /// task if the head fits, and otherwise the tasks of the waiters behind
    /// it that could pass, oldest first. Clears `scan_due`. Under `SplitPool`,
    /// [`scan_split`](Self::scan_split) instead.
    ///
    /// Each waiter woken is counted against the free slots, and against the
    /// budget under `Budget`, as if it had passed, so that the waiters woken
    /// together fit together. Under `BoundedIdle` and `BudgetIdle` it reads
    /// `idle` for the
    /// waiter's connection. A waiter with no owner ([`NO_OWNER`]) is skipped
    /// and not counted: no task would poll it, and its holder's first poll
    /// checks it. The wakes may still be more than can pass, when the pool
    /// changes before the woken tasks poll; each poll checks again
    /// ([`admit`](Self::admit)).
    ///
    /// Cost: once the head does not fit and the order lets anything pass, it
    /// visits every id in `waiting`, stale ones included (at most twice the
    /// live ones plus 64, see [`left_waiting`](Self::left_waiting)), and for
    /// each waiter that fits, the operations of its connection. It runs at
    /// most once per event-loop iteration. It keeps visiting after the free
    /// slots are used up, because a waiter that needs no slots (an empty
    /// message) can still pass. Stale ids are skipped, not popped: only
    /// `front` and `compact` remove ids.
    pub(crate) fn scan(
        &mut self,
        pool: PoolView,
        idle: impl Fn(u32) -> bool,
        wakes: &mut Vec<u32>,
    ) {
        debug_assert_ne!(self.order, SendAdmissionOrder::Strict);
        self.scan_due = false;
        if self.order.small_region().is_some() {
            self.scan_split(pool, wakes);
            return;
        }
        let Some((head, head_needs)) = self.head_and_need() else {
            return;
        };
        self.observe_latch(head, head_needs, pool);
        if pool.free >= head_needs {
            if let Some(e) = self.entry(head) {
                wakes.push(e.task_id);
            }
            return;
        }
        if matches!(
            self.order,
            SendAdmissionOrder::Bounded | SendAdmissionOrder::BoundedIdle
        ) && self.latched_head == Some(head)
        {
            return;
        }
        self.counters.wake_scans += 1;
        self.scan_epoch = self.scan_epoch.wrapping_add(1).max(1);
        let woken_before = wakes.len();
        let mut left = pool;
        // `front` left the head at the front of `waiting`.
        for position in 1..self.waiting.len() {
            let id = self.waiting[position];
            if let Some((required_slots, task_id)) =
                self.could_pass(id, head, head_needs, left, &idle, true)
            {
                wakes.push(task_id);
                left.free -= required_slots;
                left.passing += required_slots;
                let epoch = self.scan_epoch;
                if let Some(Entry {
                    state: Completion::Waiting { woken_in, .. },
                    ..
                }) = self.entry_mut(id)
                {
                    *woken_in = epoch;
                }
            }
        }
        let woken = wakes.len() - woken_before;
        self.counters.scan_wakes += woken as u64;
        // A waiter woken here that does not poll before the next scan loses
        // its claim then, and the waiters it held back get their turn; the
        // next scan has to run for that even if nothing else asks for it.
        if woken > 0 {
            self.scan_due = true;
        }
    }

    /// Under `SplitPool`, the capacity wake: each FIFO's head
    /// ([`region_head`](Self::region_head)) if its region has room for it. A
    /// release in either region, a head that left and a waiter whose earlier
    /// waiter on its connection left all reach here, through the release
    /// hook or `scan_due`.
    fn scan_split(&mut self, pool: PoolView, wakes: &mut Vec<u32>) {
        for (small, free) in [(false, pool.free), (true, pool.small_free)] {
            if let Some((_, required_slots, task_id)) = self.region_head(small)
                && free >= required_slots
            {
                wakes.push(task_id);
            }
        }
    }

    /// `id`, the small FIFO's head, was submitted: count the admission, then
    /// [`mark_submitted`](Self::mark_submitted).
    pub(crate) fn mark_small(&mut self, id: SendId) -> Option<u32> {
        self.counters.small_admissions += 1;
        self.mark_submitted(id)
    }

    /// `id`, a waiter [`admit`](Self::admit) let pass the head with
    /// `slots`, was submitted: count the pass, then
    /// [`mark_submitted`](Self::mark_submitted).
    pub(crate) fn mark_passed(&mut self, id: SendId, slots: usize) -> Option<u32> {
        self.counters.passing_admissions += 1;
        self.counters.passing_slots += slots as u64;
        self.mark_submitted(id)
    }

    /// The connection at `conn_index` is released (any generation): the
    /// backend has closed its socket and is about to free the slot.
    ///
    /// Every waiting and in-flight entry for the connection becomes
    /// `Aborted(ConnectionAborted)` (a waiting one's id leaves the FIFO when it
    /// reaches the head, or at the next `compact`) and its
    /// owner's task id is in the returned list — the owner may be a
    /// standalone task, or a connection task on another index, that
    /// outlives this connection. A resolved entry keeps its result for the
    /// future that holds it. Abandoned entries are removed. If the head was
    /// removed, the new head's task id is appended as well.
    ///
    /// `Aborted` rather than `Done` because this abort is *provisional*: a
    /// driver's `Ok(n)` that arrives after it wins over the abort recorded
    /// here. See the module docs. (An entry still waiting was never
    /// submitted, so no driver result can arrive for it; it is recorded the
    /// same way so teardown has one rule, and `take_result` resolves both
    /// alike.)
    ///
    /// The connection's own task dies here too, if it was still alive, and
    /// so does its ownership of entries on any connection: `Executor::remove_connection`
    /// drops the task with the driver state installed, so the futures that
    /// died with it have already cancelled their entries, and an entry
    /// still owned by the task is held by a future the task handed off. It
    /// is re-owned to [`NO_OWNER`] and otherwise treated as any other entry.
    ///
    /// Visits the entries indexed under `conn_index`, and allocates the
    /// returned `Vec` and a list of those entries; this is the teardown path.
    /// An entry that leaves the waiting state can trigger a `compact`, which
    /// visits every id in the FIFO.
    pub(crate) fn remove_connection(&mut self, conn_index: u32) -> Vec<u32> {
        let mut wakes = Vec::new();
        let old_head = self.front();

        // Every entry this touches is indexed under `conn_index`, as its
        // target or as its connection-task owner.
        let mut candidates: Vec<u32> = Vec::new();
        for lists in [&self.by_conn, &self.by_task] {
            if let Some(list) = lists.get(conn_index as usize) {
                candidates.extend_from_slice(list);
            }
        }
        candidates.sort_unstable();
        candidates.dedup();

        for index in candidates {
            if self.owned_by_connection_task(index, conn_index) {
                self.disown(index);
            }
            let Some(e) = self.slots[index as usize].entry.as_mut() else {
                continue;
            };
            if e.conn_index != conn_index {
                continue;
            }
            let was_waiting = matches!(e.state, Completion::Waiting { .. });
            let keep = match e.state {
                Completion::Waiting { .. } | Completion::InFlight => {
                    e.state = Completion::Aborted(connection_aborted());
                    if e.task_id != NO_OWNER {
                        wakes.push(e.task_id);
                    }
                    true
                }
                // Resolved, by the driver or by an earlier teardown of the
                // same slot: the future that holds it takes it.
                Completion::Done(_) | Completion::Aborted(_) => true,
                Completion::Abandoned => false,
            };
            if !keep {
                self.free_index(index);
            }
            if was_waiting {
                self.left_waiting();
            }
        }
        self.wake_new_head(old_head, &mut wakes);
        wakes
    }

    /// Whether slot `index` holds an entry owned by connection task
    /// `conn_index`.
    fn owned_by_connection_task(&self, index: u32, conn_index: u32) -> bool {
        self.slots[index as usize]
            .entry
            .as_ref()
            .is_some_and(|e| is_connection_task(e.task_id) && e.task_id == conn_index)
    }

    /// Re-own the entry in slot `index` to [`NO_OWNER`]: its owning
    /// connection task is gone and the future that holds it has not been
    /// polled since.
    fn disown(&mut self, index: u32) {
        let Some(e) = self.slots[index as usize].entry.as_mut() else {
            return;
        };
        let old = std::mem::replace(&mut e.task_id, NO_OWNER);
        if is_connection_task(old) {
            index_remove(&mut self.by_task, old, index);
        }
    }

    /// The connection task `conn_index` returned or panicked while its
    /// connection is still being closed. The task's own futures cancelled
    /// their entries when the task was dropped, so an entry it still owns,
    /// whichever connection it targets, is held by a future the task handed
    /// to another task that has not polled it yet: it is re-owned to
    /// [`NO_OWNER`] and left in its state, waiting, in flight or resolved,
    /// for that poll. Entries other tasks own on this connection are left
    /// as they are: the close is deferred behind the queued sends, so the
    /// driver delivers or fails each of them before
    /// [`remove_connection`](Self::remove_connection) runs at the slot's
    /// release. Returns nothing to wake: no entry changes state.
    ///
    /// Called from [`Executor::remove_connection_task`], the task-exit arms
    /// of both backends' `poll_ready_tasks`.
    pub(crate) fn remove_task(&mut self, conn_index: u32) -> Vec<u32> {
        let candidates: Vec<u32> = self
            .by_task
            .get(conn_index as usize)
            .cloned()
            .unwrap_or_default();
        for index in candidates {
            if self.owned_by_connection_task(index, conn_index) {
                self.disown(index);
            }
        }
        Vec::new()
    }

    /// If the head changed from `old_head`, push the new head's task onto
    /// `wakes`.
    fn wake_new_head(&mut self, old_head: Option<SendId>, wakes: &mut Vec<u32>) {
        if self.front() != old_head
            && let Some(next) = self.head_task()
        {
            wakes.push(next);
        }
    }

    /// Fail every *waiting* entry for exactly (`conn_index`, `generation`)
    /// with `io::Error::new(kind, msg)`; in-flight entries are left alone.
    /// Returns the owners' task ids for waking, plus the new head's task id
    /// if the head was among the failed.
    ///
    /// `SendHalf::shutdown_write` calls this with `BrokenPipe` so that bounded
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
        let old_head = self.front();
        let candidates: Vec<u32> = self
            .by_conn
            .get(conn_index as usize)
            .cloned()
            .unwrap_or_default();
        for index in candidates {
            let Some(e) = self.slots[index as usize].entry.as_mut() else {
                continue;
            };
            let matches = e.conn_index == conn_index
                && matches!(e.state, Completion::Waiting { generation: g, .. } if g == generation);
            if !matches {
                continue;
            }
            e.state = Completion::Done(Err(io::Error::new(kind, msg)));
            wakes.push(e.task_id);
            self.left_waiting();
        }
        self.wake_new_head(old_head, &mut wakes);
        wakes
    }

    /// `abort` on `conn_index`: every entry on it that is waiting for
    /// admission (for `generation`) or in flight records
    /// `Done(Err(ConnectionAborted))`, and its owner's task id is returned for
    /// waking; if the FIFO head left, the new head's task id is appended.
    ///
    /// `Done`, not the provisional `Aborted`: the caller asked for these
    /// sends to fail, so a driver result that arrives later is ignored, as
    /// any second result is. An entry already `Done` keeps its result, an
    /// `Aborted` one is a torn-down earlier occupant's, and an `Abandoned`
    /// one has no future. An entry in flight on `conn_index` belongs to the
    /// current occupant: `remove_connection` aborts every one at the slot's
    /// release.
    ///
    /// Allocates the returned `Vec` and one boxed error per failed entry.
    pub(crate) fn abort_connection(&mut self, conn_index: u32, generation: u32) -> Vec<u32> {
        let mut wakes = Vec::new();
        let old_head = self.front();
        let candidates: Vec<u32> = self
            .by_conn
            .get(conn_index as usize)
            .cloned()
            .unwrap_or_default();
        for index in candidates {
            let Some(e) = self.slots[index as usize].entry.as_mut() else {
                continue;
            };
            if e.conn_index != conn_index {
                continue;
            }
            let was_waiting = match e.state {
                Completion::Waiting { generation: g, .. } if g == generation => true,
                Completion::InFlight => false,
                _ => continue,
            };
            e.state = Completion::Done(Err(crate::handler::aborted_error()));
            if e.task_id != NO_OWNER {
                wakes.push(e.task_id);
            }
            if was_waiting {
                self.left_waiting();
            }
        }
        self.wake_new_head(old_head, &mut wakes);
        wakes
    }

    #[cfg(test)]
    fn waiting_len(&self) -> usize {
        self.waiting_live
    }

    /// Operations holding a slot, waiting or admitted.
    #[cfg(test)]
    pub(crate) fn live(&self) -> usize {
        self.slots.iter().filter(|s| s.entry.is_some()).count()
    }

    #[cfg(test)]
    fn submitted_len(&self) -> usize {
        let live = self.slots.iter().filter(|s| s.entry.is_some()).count();
        live - self.waiting_live
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
    /// [`SendCompletions::enqueue`]. `Admission`, first poll.
    pub(crate) fn enqueue_send_capacity(
        &mut self,
        conn_index: u32,
        generation: u32,
        required_slots: usize,
        task_id: u32,
    ) -> SendId {
        self.send_completions
            .enqueue(conn_index, generation, required_slots, task_id)
    }

    /// [`SendCompletions::register`], owned by the polling task. The
    /// awaiting send APIs, before they submit.
    pub(crate) fn register_send(&mut self, conn_index: u32) -> SendId {
        let task_id = super::CURRENT_TASK_ID.with(|c| c.get());
        self.send_completions.register(conn_index, task_id)
    }

    /// [`SendCompletions::withdraw`]. The awaited send APIs, when their
    /// submission fails.
    pub(crate) fn withdraw_send(&mut self, id: SendId) {
        self.send_completions.withdraw(id);
    }

    /// [`SendCompletions::forget`]. The completion handlers' stale-generation
    /// branches.
    #[cfg_attr(not(has_io_uring), allow(dead_code))]
    pub(crate) fn forget_send(&mut self, id: SendId) {
        self.send_completions.forget(id);
    }

    /// [`SendCompletions::set_owner`]. The send futures, every later poll.
    pub(crate) fn set_send_owner(&mut self, id: SendId, task_id: u32) {
        self.send_completions.set_owner(id, task_id);
    }

    /// [`SendCompletions::turn`]. `Admission`, each poll while
    /// waiting.
    pub(crate) fn send_capacity_turn(&mut self, id: SendId, free_slots: usize) -> bool {
        self.send_completions.turn(id, free_slots)
    }

    /// [`SendCompletions::mark_submitted`] plus a wake of the new head.
    /// `Admission`, after the reserve-copy-submit step.
    pub(crate) fn mark_send_submitted(&mut self, id: SendId) {
        if let Some(next) = self.send_completions.mark_submitted(id) {
            let _ = self.wake_task(next);
        }
    }

    /// [`SendCompletions::complete`] plus a wake of the owner. Backends'
    /// send-completion handlers.
    pub(crate) fn complete_send(&mut self, id: SendId, result: io::Result<u32>) {
        if let Some(owner) = self.send_completions.complete(id, result) {
            let _ = self.wake_task(owner);
        }
    }

    /// [`SendCompletions::take_result`]. The send futures, each poll after
    /// submission.
    pub(crate) fn take_send_result(&mut self, id: SendId) -> Option<io::Result<u32>> {
        self.send_completions.take_result(id)
    }

    /// [`SendCompletions::cancel`] plus a wake of the new head. The send
    /// futures' `Drop`.
    pub(crate) fn cancel_send(&mut self, id: SendId) {
        if let Some(next) = self.send_completions.cancel(id) {
            let _ = self.wake_task(next);
        }
    }

    /// [`SendCompletions::head_ready`] plus a wake of the head. The
    /// driver's copy-pool slot-release hook, with the number of
    /// slots now free.
    ///
    /// Counted under `cfg(test)` in [`Executor::send_capacity_wakes`]: the
    /// mio loop's "one capacity wake per iteration" is otherwise invisible,
    /// since waking an already-Ready head pushes nothing.
    pub(crate) fn wake_send_capacity(&mut self, free_slots: usize) {
        if let Some(head) = self.send_completions.head_ready(free_slots) {
            #[cfg(test)]
            {
                self.send_capacity_wakes += 1;
            }
            let _ = self.wake_task(head);
        }
    }

    /// [`SendCompletions::set_order`]. The event loops, right after
    /// `Executor::new`, with `Config::send_admission_order`.
    pub(crate) fn set_send_admission_order(&mut self, order: SendAdmissionOrder) {
        self.send_completions.set_order(order);
    }

    /// The worker's admission order. `Admission`, each poll while waiting,
    /// to choose between [`send_capacity_turn`](Self::send_capacity_turn)
    /// and [`send_capacity_admit`](Self::send_capacity_admit).
    pub(crate) fn send_admission_order(&self) -> SendAdmissionOrder {
        self.send_completions.order()
    }

    /// [`SendCompletions::admit`]. `Admission`, each poll while waiting,
    /// under an order other than `Strict`.
    pub(crate) fn send_capacity_admit(
        &mut self,
        id: SendId,
        pool: PoolView,
        first_poll: bool,
        idle: impl Fn(u32) -> bool,
    ) -> Admit {
        self.send_completions.admit(id, pool, first_poll, idle)
    }

    /// [`SendCompletions::enqueue_small`]. `Admission`, first poll, under
    /// `SplitPool`, for a send that takes a small slot.
    pub(crate) fn enqueue_small_send(
        &mut self,
        conn_index: u32,
        generation: u32,
        task_id: u32,
    ) -> SendId {
        self.send_completions
            .enqueue_small(conn_index, generation, task_id)
    }

    /// [`SendCompletions::mark_small`] plus a wake of the new head of the
    /// whole FIFO, as [`mark_send_submitted`](Self::mark_send_submitted)
    /// does. `Admission`, after submitting the small FIFO's head.
    pub(crate) fn mark_send_small(&mut self, id: SendId) {
        if let Some(next) = self.send_completions.mark_small(id) {
            let _ = self.wake_task(next);
        }
    }

    /// [`SendCompletions::mark_passed`]. `Admission`, after submitting a
    /// send that passed the head. `id` was behind the head, so the head does
    /// not change and nothing is woken.
    pub(crate) fn mark_send_passed(&mut self, id: SendId, slots: usize) {
        let next = self.send_completions.mark_passed(id, slots);
        debug_assert!(next.is_none(), "a send that passed was the head");
    }

    /// The worker's [`SendAdmissionCounters`]: the FIFO's counts, with B and
    /// its largest value from `pool`.
    pub(crate) fn send_admission_counters(&self, pool: &SendCopyPool) -> SendAdmissionCounters {
        SendAdmissionCounters {
            passing_held: pool.passing_held() as u64,
            passing_held_max: pool.passing_peak() as u64,
            main_free: pool.free_count() as u64,
            small_free: pool.small_free_count() as u64,
            ..self.send_completions.counters
        }
    }

    /// [`SendCompletions::scan_due`]. The event loops' end-of-iteration
    /// capacity wake runs when this is set or slots came back.
    pub(crate) fn send_scan_due(&self) -> bool {
        self.send_completions.scan_due()
    }

    /// The event loops' end-of-iteration capacity wake, with the worker's
    /// copy pool and its F + B peak since the last read
    /// (`SendCopyPool::take_free_plus_passing_peak`). Under `Strict`, [`wake_send_capacity`](Self::wake_send_capacity)
    /// with the free count, as before the other orders existed. Under the
    /// others, [`SendCompletions::scan`] plus a wake of every task it
    /// returns; `idle` answers the connection test of `BoundedIdle` and
    /// `BudgetIdle` for a connection index.
    pub(crate) fn wake_send_capacity_in(
        &mut self,
        pool: &SendCopyPool,
        free_plus_passing_peak: usize,
        idle: impl Fn(u32) -> bool,
    ) {
        if self.send_completions.order() == SendAdmissionOrder::Strict {
            self.wake_send_capacity(pool.free_count());
            return;
        }
        let mut wakes = std::mem::take(&mut self.send_completions.wake_scratch);
        let view = PoolView::with_peak(pool, free_plus_passing_peak);
        self.send_completions.scan(view, idle, &mut wakes);
        for task_id in wakes.drain(..) {
            let _ = self.wake_task(task_id);
        }
        self.send_completions.wake_scratch = wakes;
    }

    /// [`SendCompletions::abort_connection`] plus a wake of every returned
    /// task. `ConnCtx::abort`.
    pub(crate) fn abort_sends(&mut self, conn_index: u32, generation: u32) {
        for task_id in self
            .send_completions
            .abort_connection(conn_index, generation)
        {
            let _ = self.wake_task(task_id);
        }
    }

    /// [`SendCompletions::fail_waiting`] plus a wake of every returned
    /// task. `SendHalf::shutdown_write`, with `BrokenPipe`.
    pub(crate) fn fail_waiting_bounded_sends(
        &mut self,
        conn_index: u32,
        generation: u32,
        kind: io::ErrorKind,
        msg: &'static str,
    ) {
        for task_id in self
            .send_completions
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
        let mut q = SendCompletions::new();
        let first = q.enqueue(1, 0, 3, task(1));
        let second = q.enqueue(2, 0, 1, task(2));

        assert!(!q.turn(second, 8), "second must wait behind the head");
        assert!(!q.turn(first, 2), "head must wait for its 3 slots");
        assert!(q.turn(first, 3), "head with capacity is admitted");
        assert!(q.turn(first, 100));
    }

    #[test]
    fn mark_submitted_promotes_and_reports_the_next_head() {
        let mut q = SendCompletions::new();
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
        let mut q = SendCompletions::new();
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
        let mut q = SendCompletions::new();
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
        let mut q = SendCompletions::new();
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
        let mut q = SendCompletions::new();
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

    /// Entries on A that A's own task owns at A's release are held by
    /// futures the task handed off (its own cancelled themselves when it was
    /// dropped). The release treats them as any other entry on A: the open
    /// ones record the provisional abort and a resolved one keeps its
    /// result; none is woken, since the owner is gone and the holder has
    /// not polled.
    #[test]
    fn remove_connection_keeps_entries_the_dead_task_handed_off() {
        const A: u32 = 3;
        let mut q = SendCompletions::new();
        let in_flight = q.enqueue(A, 0, 1, A);
        q.mark_submitted(in_flight);
        let done = q.enqueue(A, 0, 1, A);
        q.mark_submitted(done);
        assert_eq!(q.complete(done, Ok(1)), Some(A));
        let waiting = q.enqueue(A, 0, 1, A);
        let other = q.enqueue(4, 0, 1, task(40));

        let wakes = q.remove_connection(A);
        assert_eq!(wakes, vec![task(40)], "only the new head is woken");
        assert_eq!(q.waiting_len(), 1, "A's waiter left the FIFO");
        assert_eq!(q.submitted_len(), 3, "all three of A's entries are kept");
        assert_eq!(
            kind_of(q.take_result(waiting)),
            io::ErrorKind::ConnectionAborted,
            "never submitted: the abort stands"
        );
        assert_eq!(
            q.take_result(done).expect("kept").expect("ok"),
            1,
            "a delivered send keeps its result"
        );
        assert_eq!(q.complete(in_flight, Ok(1)), Some(NO_OWNER));
        assert_eq!(q.take_result(in_flight).expect("resolved").expect("ok"), 1);
        assert_eq!(q.submitted_len(), 0);
        assert!(q.turn(other, 1));
    }

    #[test]
    fn a_real_result_overwrites_a_teardown_abort_but_not_another_result() {
        // A teardown abort is provisional: a driver result that follows it
        // is the authoritative one, whatever order a run loop produces.
        const A: u32 = 7;
        let mut q = SendCompletions::new();
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
        let mut q = SendCompletions::new();
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
        let mut q = SendCompletions::new();
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
        let mut q = SendCompletions::new();
        let in_flight = q.enqueue(1, 5, 1, task(1));
        q.mark_submitted(in_flight);
        let same_gen = q.enqueue(1, 5, 1, task(2));
        let other_gen = q.enqueue(1, 6, 1, task(3));
        let other_conn = q.enqueue(2, 5, 1, task(4));

        // `shutdown_write` passes BrokenPipe; an arbitrary kind here proves the kind
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

    /// `abort_connection` fails the connection's waiting entries of its
    /// generation and its in-flight ones with `ConnectionAborted`, and a
    /// driver result that arrives afterwards does not replace that. A
    /// resolved entry, another generation's waiter and another connection's
    /// entries keep their state.
    #[test]
    fn abort_connection_fails_waiting_and_in_flight_entries() {
        let mut q = SendCompletions::new();
        let in_flight = q.enqueue(1, 5, 1, task(1));
        q.mark_submitted(in_flight);
        let done = q.enqueue(1, 5, 1, task(2));
        q.mark_submitted(done);
        q.complete(done, Ok(4));
        let waiting = q.enqueue(1, 5, 1, task(3));
        let other_gen = q.enqueue(1, 6, 1, task(4));
        let other_conn = q.enqueue(2, 5, 1, task(5));
        let other_conn_in_flight = q.enqueue(2, 5, 1, task(6));
        q.mark_submitted(other_conn_in_flight);

        let wakes = q.abort_connection(1, 5);

        assert_eq!(
            wakes,
            vec![task(1), task(3), task(4)],
            "the failed owners, then the promoted head"
        );
        assert_eq!(
            q.complete(in_flight, Ok(9)),
            None,
            "a driver result after the abort is ignored"
        );
        let err = q.take_result(in_flight).unwrap().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionAborted);
        assert_eq!(err.to_string(), "connection aborted");
        assert_eq!(
            kind_of(q.take_result(waiting)),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(q.take_result(done).unwrap().unwrap(), 4, "resolved before");
        assert!(
            q.take_result(other_gen).is_none(),
            "another generation waits"
        );
        assert!(q.turn(other_gen, 1), "and is now the head");
        assert_eq!(q.cancel(other_gen), Some(task(5)));
        assert!(q.take_result(other_conn).is_none());
        assert!(q.take_result(other_conn_in_flight).is_none());
        assert_eq!(q.waiting_len(), 1, "only the other connection's waiter");
    }

    /// `withdraw` frees an entry no submission took, and leaves alone one
    /// that has moved on.
    #[test]
    fn withdraw_frees_only_an_unsubmitted_entry() {
        let mut q = SendCompletions::new();
        for _ in 0..100 {
            let id = q.register(1, task(1));
            q.withdraw(id);
        }
        assert_eq!(q.live(), 0, "every withdrawn entry is freed");

        let done = q.register(1, task(1));
        q.complete(done, Ok(3));
        q.withdraw(done);
        assert_eq!(q.take_result(done).unwrap().unwrap(), 3, "a result is kept");
    }

    /// `forget` frees an abandoned entry, whose completion is the one it was
    /// waiting for, and leaves a teardown abort for its future to take.
    #[test]
    fn forget_frees_only_an_abandoned_entry() {
        const A: u32 = 3;
        let mut q = SendCompletions::new();
        let abandoned = q.register(A, task(1));
        q.cancel(abandoned);
        q.forget(abandoned);
        assert_eq!(q.live(), 0, "the abandoned entry is freed");

        let aborted = q.register(A, task(2));
        q.forget(aborted);
        assert_eq!(q.live(), 1, "an in-flight entry is kept");
        q.remove_connection(A);
        q.forget(aborted);
        assert_eq!(q.live(), 1, "an aborted entry is kept");
        assert_eq!(
            kind_of(q.take_result(aborted)),
            io::ErrorKind::ConnectionAborted
        );
    }

    /// `remove_connection` finds an entry through its current owner: after
    /// `set_owner` moves it to connection task `C`, tearing `C` down drops
    /// it, and tearing down its first owner does not.
    /// An entry on D that C's task owns is left in flight when C is
    /// released: C's own futures cancelled themselves when C's task was
    /// dropped, so an entry still owned by C is held by a future handed to
    /// a task that has not polled it yet. The entry has no owner to wake
    /// until that poll.
    #[test]
    fn remove_connection_leaves_an_entry_its_dead_task_handed_off_in_flight() {
        const B: u32 = 2;
        const C: u32 = 3;
        const D: u32 = 7;
        let mut q = SendCompletions::new();
        let id = q.register(D, B);
        q.set_owner(id, C);

        assert!(q.remove_connection(B).is_empty(), "B no longer owns it");
        assert_eq!(q.live(), 1);
        assert!(q.remove_connection(C).is_empty(), "nobody to wake yet");
        assert_eq!(q.live(), 1, "kept for the future that holds it");
        assert_eq!(
            q.complete(id, Ok(1)),
            Some(NO_OWNER),
            "the driver's result lands with no owner to wake"
        );
        assert_eq!(q.take_result(id).expect("resolved").expect("ok"), 1);
        assert_eq!(q.live(), 0);
    }

    /// A slot freed by one operation is reused by the next, and an id the
    /// first operation left behind is stale: it reaches nothing.
    #[test]
    fn a_stale_id_never_reaches_the_slot_s_next_occupant() {
        let mut q = SendCompletions::new();
        let old = q.enqueue(1, 0, 1, task(1));
        q.mark_submitted(old);
        q.complete(old, Ok(3));
        assert_eq!(q.take_result(old).unwrap().unwrap(), 3);

        let new = q.enqueue(2, 0, 1, task(2));
        assert_eq!(new.index, old.index, "the freed slot is reused");
        q.mark_submitted(new);

        assert_eq!(q.complete(old, Ok(99)), None, "stale completion ignored");
        assert!(q.take_result(old).is_none());
        q.set_owner(old, task(9));
        assert_eq!(q.cancel(old), None);
        assert_eq!(
            q.complete(new, Ok(7)),
            Some(task(2)),
            "the new operation is still in flight under its own owner"
        );
        assert_eq!(q.take_result(new).unwrap().unwrap(), 7);
    }

    #[test]
    fn head_ready_respects_required_slots() {
        let mut q = SendCompletions::new();
        assert_eq!(q.head_ready(usize::MAX), None, "empty queue");
        q.enqueue(1, 0, 4, task(1));
        q.enqueue(1, 0, 1, task(2));
        assert_eq!(q.head_ready(3), None, "head needs 4");
        assert_eq!(q.head_ready(4), Some(task(1)));
        assert_eq!(q.head_ready(9), Some(task(1)), "never the second waiter");
    }

    #[test]
    fn set_owner_redirects_the_wake() {
        let mut q = SendCompletions::new();
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
        q.set_owner(
            SendId {
                index: u32::MAX,
                generation: 0,
            },
            task(9),
        );
        assert_eq!(q.head_ready(1), Some(task(6)));
    }
    /// A proxy task on connection C with a bounded send waiting on
    /// connection D, handed with its `SendHalf` to a task that has not
    /// polled it when C is released. The entry keeps its place in the FIFO:
    /// a waiting entry leaves the FIFO only through its future's drop
    /// (`cancel`), and the future is alive. Its wake goes to no task until
    /// that poll records an owner.
    #[test]
    fn removing_a_connection_keeps_a_waiting_entry_its_task_handed_off_at_the_head() {
        let mut q = SendCompletions::new();
        const C: u32 = 3;
        const D: u32 = 7;

        let handed_off = q.enqueue(D, 1, 1, C);
        let behind = q.enqueue(D, 1, 1, 9 | STANDALONE_BIT);

        assert!(q.remove_connection(C).is_empty(), "the head did not move");

        assert!(q.turn(handed_off, 1), "still the head");
        assert!(!q.turn(behind, 1));
        assert_eq!(
            q.head_ready(1),
            Some(NO_OWNER),
            "a wake for it reaches nobody"
        );
        assert_eq!(q.waiting_len(), 2);

        // The holder polls it: it owns the head and admission proceeds.
        q.set_owner(handed_off, task(5));
        assert_eq!(q.head_ready(1), Some(task(5)));
        assert_eq!(q.mark_submitted(handed_off), Some(9 | STANDALONE_BIT));
        assert_eq!(q.complete(handed_off, Ok(1)), Some(task(5)));
        assert_eq!(q.take_result(handed_off).expect("resolved").expect("ok"), 1);
    }

    /// The result of a send whose creator task returned, and whose
    /// connection was then released, is still there for the future the task
    /// handed off: the send completed before the release (a close finalizes
    /// only once the connection's sends have drained), and the release
    /// keeps a resolved entry.
    #[test]
    fn a_result_that_landed_before_the_release_waits_for_the_handed_off_future() {
        const N: u32 = 3;
        let mut q = SendCompletions::new();
        let id = q.register(N, N);
        assert!(q.remove_task(N).is_empty(), "the task returned");
        assert_eq!(
            q.complete(id, Ok(5)),
            Some(NO_OWNER),
            "delivered, nobody to wake"
        );
        assert!(q.remove_connection(N).is_empty(), "released");
        assert_eq!(q.live(), 1, "the result waits for the future");

        // The task the future was handed to polls it.
        q.set_owner(id, task(5));
        assert_eq!(q.take_result(id).expect("resolved").expect("ok"), 5);
        assert_eq!(q.live(), 0);
    }

    /// An entry still in flight when its creator task returns stays in
    /// flight: the task's own futures cancelled themselves when the task was
    /// dropped, so this one is held by a future handed to another task. The
    /// completion, landing before that task's first poll, is kept for it.
    #[test]
    fn remove_task_leaves_an_in_flight_entry_for_the_future_that_holds_it() {
        const N: u32 = 3;
        let mut q = SendCompletions::new();
        let id = q.register(N, N);
        assert!(q.remove_task(N).is_empty());
        assert_eq!(q.live(), 1, "kept, not abandoned");
        assert_eq!(
            q.complete(id, Ok(5)),
            Some(NO_OWNER),
            "the result is recorded; the wake reaches nobody"
        );
        q.set_owner(id, task(5));
        assert_eq!(q.take_result(id).expect("resolved").expect("ok"), 5);
        assert_eq!(q.live(), 0);
    }

    /// `remove_task` changes no entry's state. The entries the dead task
    /// owned are held by futures it handed off and lose their owner until
    /// polled; entries other tasks own are untouched.
    #[test]
    fn remove_task_leaves_every_entry_where_it_is() {
        const A: u32 = 3;
        let mut q = SendCompletions::new();
        // Entries A's task owned, now held elsewhere.
        let own_in_flight = q.enqueue(A, 0, 1, A);
        q.mark_submitted(own_in_flight);
        let own_done = q.enqueue(A, 0, 1, A);
        q.mark_submitted(own_done);
        assert_eq!(q.complete(own_done, Ok(1)), Some(A));
        let own_waiting = q.enqueue(A, 0, 1, A);
        // Entries on A owned by a standalone task and by connection task 4.
        let other_in_flight = q.register(A, task(40));
        let other_waiting = q.enqueue(A, 0, 1, 4);

        assert!(q.remove_task(A).is_empty(), "the head did not move");
        assert_eq!(q.waiting_len(), 2);
        assert_eq!(q.live(), 5);
        assert!(
            q.turn(own_waiting, 1),
            "still the head, for its holder's poll"
        );
        assert_eq!(q.head_ready(1), Some(NO_OWNER));
        assert_eq!(q.take_result(own_done).expect("kept").expect("ok"), 1);
        assert_eq!(q.complete(own_in_flight, Ok(1)), Some(NO_OWNER));
        assert_eq!(
            q.take_result(own_in_flight).expect("resolved").expect("ok"),
            1
        );

        assert!(q.take_result(other_in_flight).is_none(), "still in flight");
        assert_eq!(
            q.complete(other_in_flight, Ok(5)),
            Some(task(40)),
            "the driver's result reaches the live owner"
        );
        assert_eq!(q.take_result(other_in_flight).unwrap().unwrap(), 5);
        assert!(!q.turn(other_waiting, 1), "behind the handed-off waiter");
        assert_eq!(q.live(), 2);
    }

    /// A connection task's entries on *another* connection (a proxy's
    /// bounded send to upstream) are kept the same way.
    #[test]
    fn remove_task_keeps_the_task_s_entries_on_other_connections() {
        const C: u32 = 3;
        const D: u32 = 7;
        let mut q = SendCompletions::new();
        let waiting = q.enqueue(D, 0, 1, C);
        let in_flight = q.register(D, C);
        let unrelated = q.enqueue(D, 0, 1, task(9));

        assert!(q.remove_task(C).is_empty());
        assert!(q.turn(waiting, 1), "still the head");
        assert_eq!(q.complete(in_flight, Ok(1)), Some(NO_OWNER));
        assert_eq!(q.take_result(in_flight).expect("resolved").expect("ok"), 1);
        assert!(!q.turn(unrelated, 1));
        assert_eq!(q.live(), 2);
    }

    /// Cancelling waiters behind the head leaves their ids in place, and the
    /// head's next reads pop them once each: no cancel searches the FIFO.
    /// Cancelling from the back is the order in which a search costs most.
    #[test]
    fn cancelling_waiters_behind_the_head_does_not_search_the_fifo() {
        const A: u32 = 4;
        let mut q = SendCompletions::new();
        let head = q.enqueue(A, 0, 1, task(1));
        let behind: Vec<SendId> = (0..64).map(|i| q.enqueue(A, 0, 1, task(2 + i))).collect();
        q.visited = 0;
        for &id in behind.iter().rev() {
            assert_eq!(q.cancel(id), None, "not the head: nobody to wake");
        }
        assert_eq!(
            q.waiting.len(),
            65,
            "the cancelled ids stay until they reach the head"
        );
        assert_eq!(q.waiting_len(), 1);
        // Each cancel reads the head once.
        assert_eq!(q.visited, 64);

        assert_eq!(q.mark_submitted(head), None, "no waiter left to wake");
        assert!(q.waiting.is_empty(), "the stale ids were popped");
        // The head read once, then the 64 stale ids popped once each.
        assert_eq!(q.visited, 64 + 1 + 64);
    }

    /// A head that waits while the waiters behind it come and go does not
    /// leave the FIFO growing: the stale ids are dropped once they outnumber
    /// the live ones.
    #[test]
    fn stale_ids_behind_a_waiting_head_are_dropped() {
        const A: u32 = 5;
        let mut q = SendCompletions::new();
        let head = q.enqueue(A, 0, 4, task(1));
        for i in 0..1000 {
            let id = q.enqueue(A, 0, 1, task(2 + i));
            q.cancel(id);
            assert!(q.waiting.len() <= 2 * q.waiting_len() + 64 + 1);
        }
        assert_eq!(q.waiting_len(), 1);
        assert!(q.turn(head, 4), "the head kept its place");
    }

    /// An aborted head left by a released connection is skipped by the next
    /// read of the head, whatever the free count, and its id leaves the FIFO.
    #[test]
    fn a_released_connection_s_waiter_does_not_hold_the_head() {
        const A: u32 = 6;
        const B: u32 = 7;
        let mut q = SendCompletions::new();
        let a = q.enqueue(A, 0, 1, task(1));
        let b = q.enqueue(B, 0, 1, task(2));
        q.remove_connection(A);
        assert_eq!(q.head_ready(1), Some(task(2)), "B is admitted with A gone");
        assert!(q.turn(b, 1));
        assert_eq!(q.mark_submitted(b), None);
        assert!(q.waiting.is_empty());
        assert_eq!(
            kind_of(q.take_result(a)),
            io::ErrorKind::ConnectionAborted,
            "A's future still collects its abort"
        );
    }

    // ── Admission orders other than Strict ──────────────────────────────

    /// The orders that let a waiter pass a head that does not fit, with a
    /// budget that never binds in the tests that are not about it.
    const PASSING_ORDERS: [SendAdmissionOrder; 5] = [
        SendAdmissionOrder::FirstFit,
        SendAdmissionOrder::Bounded,
        SendAdmissionOrder::BoundedIdle,
        SendAdmissionOrder::Budget(u16::MAX),
        SendAdmissionOrder::BudgetIdle(u16::MAX),
    ];

    fn pool(free: usize, passing: usize, slots: usize) -> PoolView {
        PoolView {
            free,
            passing,
            slots,
            small_free: 0,
            free_plus_passing_peak: 0,
        }
    }

    fn idle(_conn_index: u32) -> bool {
        true
    }

    /// The task ids `scan` wakes for `view`, every connection idle.
    fn scanned(q: &mut SendCompletions, view: PoolView) -> Vec<u32> {
        let mut wakes = Vec::new();
        q.scan(view, |_| true, &mut wakes);
        wakes
    }

    /// Under `Strict` the queue keeps no arrival numbers and asks for no
    /// scan, whatever leaves the FIFO: none of the other orders' work runs.
    #[test]
    fn strict_keeps_no_arrival_numbers_and_asks_for_no_scan() {
        let mut q = SendCompletions::new();
        let head = q.enqueue(1, 0, 6, task(1));
        let behind = q.enqueue(2, 0, 1, task(2));
        let gone = q.enqueue(3, 0, 1, task(3));
        assert_eq!(q.next_seq, 0);
        assert!(!q.turn(behind, 4), "nothing passes the head");
        q.cancel(gone);
        q.mark_submitted(head);
        q.remove_connection(2);
        assert!(!q.scan_due());
        assert_eq!(q.counters, SendAdmissionCounters::default());
    }

    /// Each order lets a waiter that fits pass a head that does not, and
    /// admits the head, not the waiter, once the head fits.
    #[test]
    fn each_order_lets_a_waiter_that_fits_pass_a_head_that_does_not() {
        for order in PASSING_ORDERS {
            let mut q = SendCompletions::with_order(order);
            let head = q.enqueue(1, 0, 6, task(1));
            let small = q.enqueue(2, 0, 1, task(2));
            assert_eq!(q.admit(head, pool(4, 0, 8), true, idle), Admit::Wait);
            assert_eq!(
                q.admit(small, pool(4, 0, 8), true, idle),
                Admit::Pass { slots: 1 },
                "{order:?}"
            );
            assert_eq!(
                q.admit(small, pool(6, 0, 8), true, idle),
                Admit::Wait,
                "{order:?}: the head fits and goes first"
            );
            assert_eq!(q.admit(head, pool(6, 0, 8), true, idle), Admit::Head);
            assert_eq!(
                q.admit(small, pool(0, 0, 8), true, idle),
                Admit::Wait,
                "{order:?}: a waiter that does not fit waits"
            );
        }
    }

    /// No order lets a waiter pass while an earlier waiter on its own
    /// connection waits, the head's connection included; once the earlier
    /// one leaves, it may.
    #[test]
    fn a_waiter_whose_connection_has_an_earlier_waiter_does_not_pass() {
        for order in PASSING_ORDERS {
            let mut q = SendCompletions::with_order(order);
            let head = q.enqueue(1, 0, 6, task(1));
            let behind_head = q.enqueue(1, 0, 1, task(2));
            let large = q.enqueue(2, 0, 5, task(3));
            let small = q.enqueue(2, 0, 1, task(4));
            let view = pool(4, 0, 8);
            assert_eq!(
                q.admit(behind_head, view, true, idle),
                Admit::Wait,
                "{order:?}"
            );
            assert_eq!(q.admit(large, view, true, idle), Admit::Wait, "{order:?}");
            assert_eq!(q.admit(small, view, true, idle), Admit::Wait, "{order:?}");
            q.cancel(large);
            assert_eq!(
                q.admit(small, view, true, idle),
                Admit::Pass { slots: 1 },
                "{order:?}"
            );
            q.mark_submitted(head);
            assert_eq!(
                q.admit(behind_head, view, true, idle),
                Admit::Head,
                "{order:?}: the next on the head's connection is the new head"
            );
        }
    }

    /// `Bounded`: once F + B >= H is seen during a head's wait, nothing
    /// passes that head, even after F + B falls back below H; the next head
    /// starts with its latch open. The head's own check closes the latch
    /// too.
    #[test]
    fn bounded_passes_nothing_once_free_plus_passing_covered_the_head() {
        for order in [SendAdmissionOrder::Bounded, SendAdmissionOrder::BoundedIdle] {
            let mut q = SendCompletions::with_order(order);
            let first = q.enqueue(1, 0, 6, task(1));
            let a = q.enqueue(2, 0, 1, task(2));
            let b = q.enqueue(3, 0, 1, task(3));
            let second = q.enqueue(5, 0, 6, task(5));
            let c = q.enqueue(4, 0, 1, task(4));
            assert_eq!(
                q.admit(a, pool(4, 1, 8), true, idle),
                Admit::Pass { slots: 1 }
            );
            q.mark_submitted(a);
            assert_eq!(
                q.admit(b, pool(3, 3, 8), true, idle),
                Admit::Wait,
                "{order:?}: F + B = 6 covers the head"
            );
            assert_eq!(
                q.admit(b, pool(3, 0, 8), true, idle),
                Admit::Wait,
                "{order:?}: F + B fell to 3, the latch stays closed"
            );
            assert!(scanned(&mut q, pool(3, 0, 8)).is_empty(), "{order:?}");

            q.cancel(b);
            q.cancel(first);
            assert_eq!(q.front(), Some(second));
            assert_eq!(
                q.admit(c, pool(3, 0, 8), true, idle),
                Admit::Pass { slots: 1 },
                "{order:?}: the new head's latch is open"
            );

            assert_eq!(q.admit(second, pool(2, 5, 8), true, idle), Admit::Wait);
            assert_eq!(
                q.admit(c, pool(3, 0, 8), true, idle),
                Admit::Wait,
                "{order:?}: closed by the head's own check"
            );
        }
    }

    /// `FirstFit` has no latch: F + B covering the head stops nothing.
    #[test]
    fn first_fit_passes_whatever_the_passing_sends_hold() {
        let mut q = SendCompletions::with_order(SendAdmissionOrder::FirstFit);
        q.enqueue(1, 0, 6, task(1));
        let a = q.enqueue(2, 0, 1, task(2));
        assert_eq!(
            q.admit(a, pool(3, 5, 8), true, idle),
            Admit::Pass { slots: 1 }
        );
    }

    /// `Budget(R)`: B plus a passing send's slots stays within min(R, P - H),
    /// with B counted by a real pool as the passing sends take slots.
    #[test]
    fn budget_never_lets_passing_sends_hold_more_than_the_budget() {
        // (R, P, H, slots each waiter needs): R binds, then P - H binds.
        let cases = [(3u16, 16u16, 10, 1), (10, 16, 14, 1), (4, 16, 10, 3)];
        for (budget, slots, head_needs, each) in cases {
            for order in [
                SendAdmissionOrder::Budget(budget),
                SendAdmissionOrder::BudgetIdle(budget),
            ] {
                budget_case(order, budget, slots, head_needs, each);
            }
        }
    }

    /// One case of the test above: `order` with budget `budget`, a pool of
    /// `slots`, a head of `head_needs` and waiters of `each` slots.
    fn budget_case(
        order: SendAdmissionOrder,
        budget: u16,
        slots: u16,
        head_needs: usize,
        each: usize,
    ) {
        let mut q = SendCompletions::with_order(order);
        let mut copy_pool = SendCopyPool::new(slots, 8);
        copy_pool.track_passing();
        // Sends outside the FIFO hold all but 8 slots, so the head waits.
        let held: Vec<u16> = (0..slots - 8)
            .map(|_| copy_pool.copy_in(b"x").unwrap().0)
            .collect();
        q.enqueue(1, 0, head_needs, task(1));
        let limit = usize::from(budget).min(usize::from(slots) - head_needs);
        let mut passed = 0;
        for conn in 2..20 {
            let id = q.enqueue(conn, 0, each, task(conn));
            match q.admit(id, PoolView::of(&copy_pool), true, idle) {
                Admit::Pass { slots } => {
                    copy_pool.begin_passing();
                    for _ in 0..slots {
                        copy_pool.copy_in(b"p").unwrap();
                    }
                    copy_pool.end_passing();
                    q.mark_submitted(id);
                    passed += slots;
                }
                Admit::Wait => {
                    q.cancel(id);
                }
                Admit::Head | Admit::Small => panic!("not the head"),
            }
            assert!(copy_pool.passing_held() <= limit, "B within min(R, P - H)");
        }
        assert_eq!(copy_pool.passing_held(), passed);
        assert!(
            passed + each > limit,
            "{order:?}: stopped only at the limit, at {passed}"
        );
        for slot in held {
            copy_pool.release(slot);
        }
    }

    /// `BoundedIdle` and `BudgetIdle` let no waiter pass while its
    /// connection has a send queued or in flight; the other orders do not
    /// ask.
    #[test]
    fn an_idle_order_does_not_pass_a_waiter_whose_connection_is_busy() {
        for order in [
            SendAdmissionOrder::BoundedIdle,
            SendAdmissionOrder::BudgetIdle(8),
        ] {
            let mut q = SendCompletions::with_order(order);
            q.enqueue(1, 0, 6, task(1));
            let w = q.enqueue(2, 0, 1, task(2));
            assert_eq!(q.admit(w, pool(4, 0, 8), true, |_| false), Admit::Wait);
            let mut wakes = Vec::new();
            q.scan(pool(4, 0, 8), |conn| conn != 2, &mut wakes);
            assert!(wakes.is_empty(), "{order:?}: the scan reads the same test");
            q.scan(pool(4, 0, 8), |_| true, &mut wakes);
            assert_eq!(wakes, vec![task(2)], "{order:?}");
            assert_eq!(
                q.admit(w, pool(4, 0, 8), true, idle),
                Admit::Pass { slots: 1 },
                "{order:?}"
            );
        }

        for order in [
            SendAdmissionOrder::FirstFit,
            SendAdmissionOrder::Bounded,
            SendAdmissionOrder::Budget(8),
        ] {
            let mut q = SendCompletions::with_order(order);
            q.enqueue(1, 0, 6, task(1));
            let w = q.enqueue(2, 0, 1, task(2));
            let asked = std::cell::Cell::new(false);
            let admit = q.admit(w, pool(4, 0, 8), true, |_| {
                asked.set(true);
                false
            });
            assert_eq!(admit, Admit::Pass { slots: 1 }, "{order:?}");
            assert!(!asked.get(), "{order:?} does not test idleness");
        }
    }

    /// A waiter admitted past the head leaves its id in `waiting`, like a
    /// cancelled one: `front` pops it once it reaches the front, and the
    /// live count drops at once.
    #[test]
    fn a_waiter_that_passed_leaves_its_id_for_front_to_pop() {
        let mut q = SendCompletions::with_order(SendAdmissionOrder::FirstFit);
        let head = q.enqueue(1, 0, 6, task(1));
        let w = q.enqueue(2, 0, 1, task(2));
        assert_eq!(
            q.admit(w, pool(4, 0, 8), true, idle),
            Admit::Pass { slots: 1 }
        );
        assert_eq!(q.mark_submitted(w), None, "passing promotes nobody");
        assert_eq!(q.waiting_len(), 1);
        assert_eq!(
            q.waiting.len(),
            2,
            "the id stays until it reaches the front"
        );
        assert_eq!(q.mark_submitted(head), None, "no waiter left");
        assert!(q.waiting.is_empty(), "popped behind the head");
        assert_eq!(q.complete(w, Ok(1)), Some(task(2)));
        assert_eq!(q.take_result(w).unwrap().unwrap(), 1);
    }

    /// The scan wakes the head alone when it fits, and otherwise the oldest
    /// waiters that could pass, counting each against the free slots,
    /// skipping one behind an earlier waiter on its connection, a stale id
    /// and a waiter with no owner. A waiter that needs no slots is woken
    /// after the free slots run out. Having woken passers, it asks for the
    /// next scan, which does not wake them again before they poll.
    #[test]
    fn scan_wakes_the_waiters_that_could_pass_oldest_first() {
        let mut q = SendCompletions::with_order(SendAdmissionOrder::FirstFit);
        let head = q.enqueue(1, 0, 10, task(1));
        q.enqueue(2, 0, 2, task(2));
        let gone = q.enqueue(3, 0, 1, task(3));
        q.enqueue(4, 0, 3, task(4));
        q.enqueue(2, 0, 1, task(5));
        q.enqueue(5, 0, 1, NO_OWNER);
        q.enqueue(6, 0, 1, task(6));
        q.enqueue(7, 0, 5, task(7));
        q.enqueue(8, 0, 0, task(8));
        q.cancel(gone);
        assert!(q.scan_due());

        assert_eq!(
            scanned(&mut q, pool(10, 0, 16)),
            vec![task(1)],
            "the head fits"
        );
        assert_eq!(
            scanned(&mut q, pool(6, 0, 16)),
            vec![task(2), task(4), task(6), task(8)],
            "2 + 3 + 1 slots, then the empty message"
        );
        assert!(q.scan_due(), "a scan that woke passers asks for the next");
        assert_eq!(q.front(), Some(head), "the scan pops nothing");
        let again = scanned(&mut q, pool(6, 0, 16));
        assert!(
            [task(2), task(4), task(6), task(8)]
                .iter()
                .all(|woken| !again.contains(woken)),
            "the next scan does not wake them again before they poll: {again:?}"
        );
    }

    /// The scan counts the waiters it wakes against the budget as if they
    /// had passed.
    #[test]
    fn scan_counts_woken_waiters_against_the_budget() {
        for order in [
            SendAdmissionOrder::Budget(3),
            SendAdmissionOrder::BudgetIdle(3),
        ] {
            let mut q = SendCompletions::with_order(order);
            q.enqueue(1, 0, 10, task(1));
            for conn in 2..8 {
                q.enqueue(conn, 0, 1, task(conn));
            }
            assert_eq!(
                scanned(&mut q, pool(6, 1, 16)),
                vec![task(2), task(3)],
                "{order:?}: B = 1, so two more fit the budget of 3"
            );
        }
    }

    /// Under an order that passes, `scan_due` is set whenever a waiter
    /// leaves the waiting state, whatever the way, and by a later poll told
    /// to wait; a first poll told to wait does not set it.
    #[test]
    fn scan_due_follows_every_way_a_waiter_leaves() {
        let mut q = SendCompletions::with_order(SendAdmissionOrder::FirstFit);
        let head = q.enqueue(1, 0, 6, task(1));
        let w = q.enqueue(1, 0, 1, task(2));
        assert_eq!(q.admit(w, pool(4, 0, 8), true, idle), Admit::Wait);
        assert!(!q.scan_due(), "a first poll that waits");
        assert_eq!(q.admit(w, pool(4, 0, 8), false, idle), Admit::Wait);
        assert!(q.scan_due(), "a later poll that waits");

        let leave: [fn(&mut SendCompletions, SendId); 5] = [
            |q, id| {
                q.cancel(id);
            },
            |q, id| {
                q.mark_submitted(id);
            },
            |q, _| {
                q.remove_connection(3);
            },
            |q, _| {
                q.fail_waiting(3, 0, io::ErrorKind::BrokenPipe, "shut down");
            },
            |q, _| {
                q.abort_connection(3, 0);
            },
        ];
        for (n, leave) in leave.iter().enumerate() {
            let id = q.enqueue(3, 0, 1, task(10));
            q.scan(pool(0, 0, 8), |_| true, &mut Vec::new());
            assert!(!q.scan_due());
            leave(&mut q, id);
            assert!(q.scan_due(), "way {n}");
        }
        q.scan(pool(0, 0, 8), |_| true, &mut Vec::new());
        q.mark_submitted(head);
        assert!(q.scan_due(), "the head was admitted");
    }

    /// The FIFO's counts: a pass and its slots, a latch that closes while
    /// its head does not fit (counted once), and a scan that looks behind
    /// the head, with the waiters it wakes. Neither a latch that closes
    /// while its head fits nor a scan that stops at a head that fits or at a
    /// closed latch is counted.
    #[test]
    fn counters_record_passes_latch_closings_and_scans() {
        let mut q = SendCompletions::with_order(SendAdmissionOrder::Bounded);
        let head = q.enqueue(1, 0, 6, task(1));
        let a = q.enqueue(2, 0, 2, task(2));
        let b = q.enqueue(3, 0, 1, task(3));
        assert_eq!(
            q.admit(a, pool(4, 0, 8), true, idle),
            Admit::Pass { slots: 2 }
        );
        assert_eq!(q.mark_passed(a, 2), None);
        assert_eq!(scanned(&mut q, pool(2, 2, 8)), vec![task(3)]);
        assert_eq!(q.admit(b, pool(2, 4, 8), true, idle), Admit::Wait);
        assert_eq!(q.admit(b, pool(2, 4, 8), true, idle), Admit::Wait);
        assert!(scanned(&mut q, pool(2, 4, 8)).is_empty(), "latched");
        assert_eq!(
            q.counters,
            SendAdmissionCounters {
                passing_admissions: 1,
                passing_slots: 2,
                latch_closings: 1,
                wake_scans: 1,
                scan_wakes: 1,
                ..SendAdmissionCounters::default()
            }
        );

        q.cancel(head);
        assert_eq!(scanned(&mut q, pool(6, 0, 8)), vec![task(3)], "b fits");
        assert_eq!(q.counters.latch_closings, 1, "b's latch closed as it fit");
        assert_eq!(q.counters.wake_scans, 1);
    }

    /// The executor's counters take B and its largest value from the pool.
    #[test]
    fn executor_counters_take_passing_slots_from_the_pool() {
        let mut exec = Executor::new(8, 8, 8, 0, 0);
        exec.set_send_admission_order(SendAdmissionOrder::FirstFit);
        let mut copy_pool = SendCopyPool::new(8, 8);
        copy_pool.track_passing();
        copy_pool.begin_passing();
        let (first, _p, _l) = copy_pool.copy_in(b"p").unwrap();
        copy_pool.copy_in(b"q").unwrap();
        copy_pool.end_passing();
        copy_pool.release(first);
        let counters = exec.send_admission_counters(&copy_pool);
        assert_eq!((counters.passing_held, counters.passing_held_max), (1, 2));
    }

    // ── SplitPool ───────────────────────────────────────────────────────

    const SPLIT: SendAdmissionOrder = SendAdmissionOrder::SplitPool {
        small_slots: 2,
        small_slot_size: 1024,
    };

    fn split_pool(free: usize, small_free: usize) -> PoolView {
        PoolView {
            free,
            passing: 0,
            slots: 8,
            small_free,
            free_plus_passing_peak: 0,
        }
    }

    /// A large head waits in the main FIFO while small waiters are admitted
    /// from the small region, which is strict in turn: its head goes first,
    /// and only while a small slot is free.
    #[test]
    fn split_pool_admits_small_waiters_while_a_large_head_waits() {
        let mut q = SendCompletions::with_order(SPLIT);
        let head = q.enqueue(1, 0, 6, task(1));
        let first = q.enqueue_small(2, 0, task(2));
        let second = q.enqueue_small(3, 0, task(3));
        let third = q.enqueue_small(4, 0, task(4));
        assert_eq!(q.admit(head, split_pool(4, 2), true, idle), Admit::Wait);
        assert_eq!(
            q.admit(second, split_pool(4, 2), true, idle),
            Admit::Wait,
            "first is ahead"
        );
        assert_eq!(q.admit(first, split_pool(4, 2), true, idle), Admit::Small);
        assert_eq!(
            q.mark_small(first),
            None,
            "the whole FIFO's head did not move"
        );
        assert_eq!(q.admit(second, split_pool(4, 1), true, idle), Admit::Small);
        q.mark_small(second);
        assert_eq!(
            q.admit(third, split_pool(4, 0), true, idle),
            Admit::Wait,
            "no small slot"
        );
        assert_eq!(q.admit(third, split_pool(4, 1), true, idle), Admit::Small);
        assert_eq!(q.admit(head, split_pool(4, 1), true, idle), Admit::Wait);
        assert_eq!(q.admit(head, split_pool(6, 0), true, idle), Admit::Head);
        assert_eq!(q.counters.small_admissions, 2);
    }

    /// A waiter whose connection has an earlier waiter in the other FIFO is
    /// not admitted, in either direction, and is not its FIFO's head, so the
    /// waiters behind it in that FIFO are admitted as if it were not there.
    #[test]
    fn split_pool_keeps_each_connection_s_order_across_regions() {
        let mut q = SendCompletions::with_order(SPLIT);
        // Small before main on connection 1.
        let small_first = q.enqueue_small(1, 0, task(1));
        let main_behind = q.enqueue(1, 0, 1, task(2));
        let main_other = q.enqueue(2, 0, 1, task(3));
        // Main before small on connection 3.
        let main_first = q.enqueue(3, 0, 6, task(4));
        let small_behind = q.enqueue_small(3, 0, task(5));
        let small_other = q.enqueue_small(4, 0, task(6));

        assert_eq!(
            q.admit(main_behind, split_pool(4, 0), true, idle),
            Admit::Wait
        );
        assert_eq!(
            q.admit(main_other, split_pool(4, 0), true, idle),
            Admit::Head,
            "main_behind is not the main head"
        );
        q.mark_submitted(main_other);
        assert_eq!(
            q.admit(small_first, split_pool(4, 1), true, idle),
            Admit::Small
        );
        q.mark_small(small_first);
        assert_eq!(
            q.admit(main_behind, split_pool(4, 0), true, idle),
            Admit::Head,
            "its connection's earlier waiter left"
        );
        q.mark_submitted(main_behind);

        assert_eq!(
            q.admit(small_behind, split_pool(4, 2), true, idle),
            Admit::Wait
        );
        assert_eq!(
            q.admit(small_other, split_pool(4, 2), true, idle),
            Admit::Small,
            "small_behind is not the small head"
        );
        q.cancel(main_first);
        assert_eq!(
            q.admit(small_behind, split_pool(4, 2), true, idle),
            Admit::Small
        );
    }

    /// The capacity wake under `SplitPool` wakes each FIFO's head whose
    /// region has room for it, and no other waiter.
    #[test]
    fn split_pool_wakes_each_fifo_s_head_when_its_region_has_room() {
        let mut q = SendCompletions::with_order(SPLIT);
        q.enqueue(1, 0, 6, task(1));
        q.enqueue(2, 0, 1, task(2));
        q.enqueue_small(3, 0, task(3));
        q.enqueue_small(4, 0, task(4));
        assert_eq!(scanned(&mut q, split_pool(4, 0)), Vec::<u32>::new());
        assert_eq!(scanned(&mut q, split_pool(4, 2)), vec![task(3)]);
        assert_eq!(scanned(&mut q, split_pool(6, 0)), vec![task(1)]);
        assert_eq!(scanned(&mut q, split_pool(6, 2)), vec![task(1), task(3)]);
    }

    /// Under `SplitPool` a waiter that leaves asks for the wake, which then
    /// finds the waiter it held back as its FIFO's head.
    #[test]
    fn split_pool_wakes_the_waiter_an_earlier_one_held_back() {
        let mut q = SendCompletions::with_order(SPLIT);
        let small_first = q.enqueue_small(1, 0, task(1));
        q.enqueue(1, 0, 1, task(2));
        assert!(scanned(&mut q, split_pool(4, 0)).is_empty(), "held back");
        q.cancel(small_first);
        assert!(q.scan_due());
        assert_eq!(scanned(&mut q, split_pool(4, 0)), vec![task(2)]);
    }

    /// The executor's counters report each region's free slots.
    #[test]
    fn executor_counters_report_each_region_s_free_slots() {
        let mut exec = Executor::new(8, 8, 8, 0, 0);
        exec.set_send_admission_order(SPLIT);
        let mut copy_pool = SendCopyPool::with_small_region(8, 64, 2, 16);
        copy_pool.copy_in(b"main").unwrap();
        copy_pool.copy_in_small(b"small").unwrap();
        let counters = exec.send_admission_counters(&copy_pool);
        assert_eq!((counters.main_free, counters.small_free), (7, 1));
    }

    /// Under an order that passes, heads admitted at the front keep the
    /// stale ids that passers left behind them within twice the live ids
    /// plus 64, the bound the scan's cost rests on.
    #[test]
    fn heads_admitted_at_the_front_keep_stale_ids_bounded() {
        let mut q = SendCompletions::with_order(SendAdmissionOrder::FirstFit);
        let heads: Vec<SendId> = (0..65).map(|i| q.enqueue(i, 0, 6, task(i))).collect();
        for i in 0..100 {
            let passer = q.enqueue(100 + i, 0, 1, task(100 + i));
            assert_eq!(
                q.admit(passer, pool(4, 0, 8), true, idle),
                Admit::Pass { slots: 1 }
            );
            q.mark_submitted(passer);
        }
        for head in heads {
            q.mark_submitted(head);
            assert!(
                q.waiting.len() <= 2 * q.waiting_len() + 64,
                "{} ids for {} live",
                q.waiting.len(),
                q.waiting_len()
            );
        }
    }

    /// A waiter's first poll does not take the room an older parked waiter
    /// could pass with: the older one is counted first, as the scan that
    /// wakes it counts it. `FirstFit` with one slot coming back, and
    /// `Budget(2)` with B falling from 2 to 1.
    #[test]
    fn an_older_waiter_that_could_pass_goes_before_a_newer_one() {
        let mut q = SendCompletions::with_order(SendAdmissionOrder::FirstFit);
        q.enqueue(1, 0, 6, task(1));
        let older = q.enqueue(2, 0, 1, task(2));
        assert_eq!(q.admit(older, pool(0, 0, 8), true, idle), Admit::Wait);
        let newer = q.enqueue(3, 0, 1, task(3));
        assert_eq!(
            q.admit(newer, pool(1, 0, 8), true, idle),
            Admit::Wait,
            "the slot is the older waiter's"
        );
        assert!(q.scan_due(), "the older waiter is woken");
        assert_eq!(scanned(&mut q, pool(1, 0, 8)), vec![task(2)]);
        assert_eq!(
            q.admit(older, pool(1, 0, 8), false, idle),
            Admit::Pass { slots: 1 }
        );

        let mut q = SendCompletions::with_order(SendAdmissionOrder::Budget(2));
        q.enqueue(1, 0, 10, task(1));
        let older = q.enqueue(2, 0, 1, task(2));
        let budget = |passing| PoolView {
            free: 4,
            passing,
            slots: 16,
            small_free: 0,
            free_plus_passing_peak: 0,
        };
        assert_eq!(q.admit(older, budget(2), true, idle), Admit::Wait);
        let newer = q.enqueue(3, 0, 1, task(3));
        assert_eq!(q.admit(newer, budget(1), true, idle), Admit::Wait);
        assert_eq!(
            q.admit(older, budget(1), false, idle),
            Admit::Pass { slots: 1 }
        );
    }

    /// `Bounded` closes the latch on a moment of F + B >= H that the pool
    /// saw between two checks, although F + B is below H at the check.
    #[test]
    fn bounded_latches_on_a_moment_between_checks() {
        let mut q = SendCompletions::with_order(SendAdmissionOrder::Bounded);
        q.enqueue(1, 0, 6, task(1));
        let w = q.enqueue(2, 0, 1, task(2));
        let view = |peak| PoolView {
            free: 3,
            passing: 0,
            slots: 8,
            small_free: 0,
            free_plus_passing_peak: peak,
        };
        assert_eq!(q.admit(w, view(5), true, idle), Admit::Pass { slots: 1 });
        assert_eq!(q.admit(w, view(6), true, idle), Admit::Wait, "F + B was 6");
        assert_eq!(
            q.admit(w, view(0), true, idle),
            Admit::Wait,
            "and stays latched"
        );
    }

    /// The scan closes the latch on its own: when only the scan sees
    /// F + B >= H, a later admission check below H still passes nothing.
    #[test]
    fn the_scan_alone_closes_the_latch() {
        for order in [SendAdmissionOrder::Bounded, SendAdmissionOrder::BoundedIdle] {
            let mut q = SendCompletions::with_order(order);
            q.enqueue(1, 0, 6, task(1));
            let w = q.enqueue(2, 0, 1, task(2));
            assert!(scanned(&mut q, pool(3, 3, 8)).is_empty(), "{order:?}");
            assert_eq!(
                q.admit(w, pool(3, 0, 8), true, idle),
                Admit::Wait,
                "{order:?}: latched by the scan"
            );
        }
    }

    /// The next head does not inherit the F + B its predecessor saw before
    /// it reserved: H1 (4 slots) is admitted with 8 free, H2 (6 slots)
    /// becomes the head with F = 4 and B = 0, so F + B stays 4 < 6 through
    /// H2's wait, and a 1-slot waiter passes under `Bounded`.
    #[test]
    fn the_next_head_does_not_inherit_its_predecessor_s_free_plus_passing() {
        for order in [SendAdmissionOrder::Bounded, SendAdmissionOrder::BoundedIdle] {
            let mut copy_pool = SendCopyPool::new(8, 8);
            copy_pool.track_passing();
            let mut q = SendCompletions::with_order(order);
            let first = q.enqueue(1, 0, 4, task(1));
            q.enqueue(2, 0, 6, task(2));
            let w = q.enqueue(3, 0, 1, task(3));
            // The first head's own poll and submit, as `Admission` makes them.
            assert_eq!(
                q.admit(first, PoolView::take(&mut copy_pool), true, idle),
                Admit::Head
            );
            let mut reservation = copy_pool.reserve_slots(4).unwrap();
            for _ in 0..4 {
                copy_pool.copy_in_reserved(&mut reservation, b"h");
            }
            copy_pool.release_reservation(reservation);
            q.mark_submitted(first);
            // The end-of-iteration scan its admission asks for.
            let peak = copy_pool.take_free_plus_passing_peak();
            q.scan(
                PoolView::with_peak(&copy_pool, peak),
                |_| true,
                &mut Vec::new(),
            );
            assert_eq!(
                q.admit(w, PoolView::take(&mut copy_pool), true, idle),
                Admit::Pass { slots: 1 },
                "{order:?}: F + B was 4 < 6 throughout the second head's wait"
            );
            assert_eq!(q.counters.latch_closings, 0, "{order:?}");
        }
    }

    /// An older waiter that a scan woke and that does not poll keeps its
    /// claim on the room only until the next scan: then a newer waiter that
    /// fits is woken and passes.
    #[test]
    fn an_older_waiter_that_does_not_poll_loses_its_claim_at_the_next_scan() {
        let mut q = SendCompletions::with_order(SendAdmissionOrder::FirstFit);
        q.enqueue(1, 0, 6, task(1));
        let older = q.enqueue(2, 0, 1, task(2));
        assert_eq!(q.admit(older, pool(0, 0, 8), true, idle), Admit::Wait);
        assert_eq!(scanned(&mut q, pool(1, 0, 8)), vec![task(2)]);
        let newer = q.enqueue(3, 0, 1, task(3));
        assert_eq!(
            q.admit(newer, pool(1, 0, 8), true, idle),
            Admit::Wait,
            "the older one was woken for that slot this iteration"
        );
        assert!(q.scan_due());
        assert_eq!(
            scanned(&mut q, pool(1, 0, 8)),
            vec![task(3)],
            "the older one did not poll: the newer one is woken"
        );
        assert_eq!(
            q.admit(newer, pool(1, 0, 8), false, idle),
            Admit::Pass { slots: 1 }
        );

        // Once the older one polls, it competes again.
        assert_eq!(q.admit(older, pool(0, 0, 8), false, idle), Admit::Wait);
        assert_eq!(scanned(&mut q, pool(1, 0, 8)), vec![task(2)]);
    }

    /// Two waiters of one task, the task polling only the newer: the
    /// newer is admitted within two scans, and then the scans stop, instead
    /// of each poll asking for a scan that wakes the same task again.
    #[test]
    fn a_task_that_polls_only_its_newer_waiter_does_not_spin() {
        let mut q = SendCompletions::with_order(SendAdmissionOrder::FirstFit);
        q.enqueue(1, 0, 6, task(1));
        let older = q.enqueue(2, 0, 1, task(9));
        let newer = q.enqueue(3, 0, 1, task(9));
        assert_eq!(q.admit(older, pool(0, 0, 8), true, idle), Admit::Wait);
        assert_eq!(q.admit(newer, pool(0, 0, 8), true, idle), Admit::Wait);
        let mut scans = 0;
        let mut admitted = false;
        while q.scan_due() || scans == 0 {
            scans += 1;
            assert!(scans <= 4, "still scanning after {scans} scans");
            if !scanned(&mut q, pool(1, 0, 8)).is_empty()
                && !admitted
                && q.admit(newer, pool(1, 0, 8), false, idle) == (Admit::Pass { slots: 1 })
            {
                q.mark_passed(newer, 1);
                admitted = true;
            }
        }
        assert!(admitted, "the newer waiter passed");
    }
}
