//! `shared` and `shared-inc`: one provided-buffer ring per worker, shared by
//! every connection, as ringline receives today.
//!
//! Like ringline: a completion whose connection has nothing buffered is
//! parsed in place from the provided buffer (lent, no copy), and only its
//! leftover partial message is copied into the connection's accumulator;
//! otherwise the bytes are copied in. On `-ENOBUFS` a connection with a
//! partial message takes a one-shot fallback recv into a pool slot; one
//! with nothing buffered re-arms after the batch has replenished.
//!
//! `shared-inc` registers the ring with `IOU_PBUF_RING_INC`: a buffer is
//! consumed incrementally, possibly by several connections, and returns to
//! the ring only when a completion clears `F_BUF_MORE`. Same memory as
//! `shared` by default, in fewer, larger buffers.
//!
//! Held lends (`--hold-every K --hold-us T`, or `--hold-first N` for
//! connections 0..N): a holding connection keeps each received range lent
//! for T µs, as a forward to a slow sink does. A held range pins its whole
//! buffer. `--lend-cap F` lends only while fewer than F x bufs buffers of
//! the group are pinned and copies otherwise. A buffer returns to the ring
//! when the kernel is done with it and no hold remains.
//!
//! `two_ring` and `two_ring_inc` add a second group (`--large-bufs`,
//! `--large-buf-size`) for streaming connections. A connection moves to it
//! after `--promote-after` consecutive completions of at least
//! `--promote-bytes`, or on its first held lend with `--promote-on-hold`,
//! and back after `--demote-after` consecutive smaller completions (a
//! holding connection is not demoted). A move takes effect at the next
//! arm: a live multishot recv is cancelled, and its last completion re-arms
//! the connection on its new group.

use crate::common::{BufRing, PAGE, PBUF_RING_INC, mmap_anon};
use crate::server::{Ctx, Strategy, TAG_RECV, TAG_RECV_AUX, ud};
use io_uring::{cqueue, opcode, types};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const FALLBACK_SLOTS: usize = 32;
/// The cancel SQE's tag; the server loop ignores its completion.
const TAG_CANCEL: u64 = 4;

struct Conn {
    acc: Vec<u8>,
    head: usize,
    rearm: bool,
    fallback: Option<usize>,
    dead: bool,
    /// The group the live recv is armed on, and the one the next arm uses.
    group: usize,
    target: usize,
    /// A cancel is in flight for the live recv.
    cancelling: bool,
    /// Consecutive completions at or above, and below, `promote_bytes`.
    full_run: u32,
    small_run: u32,
    /// When the connection last delivered bytes.
    last_data: Option<Instant>,
    /// The longest gap between two deliveries.
    max_gap: Duration,
    /// The gap before the latest delivery.
    last_gap: Option<Duration>,
    /// When the first `-ENOBUFS` since the last delivery arrived.
    starved_at: Option<Instant>,
    enobufs: u32,
}

/// One provided-buffer ring and its buffers.
struct Group {
    bufs: usize,
    buf_size: usize,
    ring: Option<BufRing>,
    backing: *mut u8,
    /// Incremental rings: where the kernel's next write into each buffer lands.
    buf_off: Vec<usize>,
    holds: Vec<u32>,
    /// The kernel is done with the buffer; it returns when its holds end.
    exhausted: Vec<bool>,
    /// Most buffers that may be pinned by holds at once.
    lend_cap: usize,
    pinned: usize,
    pinned_peak: usize,
    /// Buffers the kernel is done with that wait for their holds.
    waiting: usize,
    /// Connections parked on `-ENOBUFS` while every buffer waits on holds;
    /// re-armed when one returns, as ringline's `recv_starved`.
    starved: Vec<usize>,
    returned: bool,
    enobufs: u64,
}

impl Group {
    fn new(bufs: usize, buf_size: usize, lend_cap: f64) -> Self {
        Group {
            bufs,
            buf_size,
            ring: None,
            backing: std::ptr::null_mut(),
            buf_off: vec![0; bufs],
            holds: vec![0; bufs],
            exhausted: vec![false; bufs],
            lend_cap: (lend_cap * bufs as f64) as usize,
            pinned: 0,
            pinned_peak: 0,
            waiting: 0,
            starved: Vec::new(),
            returned: false,
            enobufs: 0,
        }
    }

    fn buffer(&self, bid: usize) -> *mut u8 {
        unsafe { self.backing.add(bid * self.buf_size) }
    }

    fn give_back(&mut self, bid: usize) {
        if self.exhausted[bid] {
            self.waiting -= 1;
        }
        self.returned = true;
        self.exhausted[bid] = false;
        self.buf_off[bid] = 0;
        let addr = self.buffer(bid) as u64;
        self.ring
            .as_mut()
            .unwrap()
            .push(addr, self.buf_size as u32, bid as u16);
    }

    /// Map the buffers, register the ring as buffer group `bgid` and post
    /// every buffer.
    fn start(&mut self, cx: &mut Ctx, bgid: u16, inc: bool, no_thp: bool) {
        let ring_mem = mmap_anon((self.bufs * 16).max(PAGE));
        self.backing = mmap_anon(self.bufs * self.buf_size);
        if no_thp {
            // 4 KiB pages: a completion makes resident only the pages it
            // writes, rather than the 2 MiB huge page around them.
            unsafe {
                libc::madvise(
                    self.backing.cast(),
                    self.bufs * self.buf_size,
                    libc::MADV_NOHUGEPAGE,
                )
            };
        }
        let flags = if inc { PBUF_RING_INC } else { 0 };
        unsafe {
            cx.uring
                .submitter()
                .register_buf_ring_with_flags(ring_mem as u64, self.bufs as u16, bgid, flags)
                .expect("register shared ring");
        }
        let mut ring = BufRing::new(ring_mem, self.bufs as u16);
        for i in 0..self.bufs {
            ring.push(self.buffer(i) as u64, self.buf_size as u32, i as u16);
        }
        self.ring = Some(ring);
    }

    fn resident_kb(&self) -> usize {
        resident_kb(self.backing, self.bufs * self.buf_size)
    }
}

/// When connections move to the large group.
pub struct Promote {
    pub bytes: usize,
    pub after: u32,
    pub demote_after: u32,
    pub on_hold: bool,
    /// `--promote-gap-us`: a full completion counts toward promotion only
    /// if it arrives within this long of the connection's previous one; 0
    /// for no limit. Separates a stream from request/response messages
    /// that happen to fill a buffer.
    pub gap: Duration,
    /// `--max-promoted`: the most connections in the large group at once;
    /// 0 for no limit.
    pub max: usize,
    /// `--promote-nonempty`: a full completion counts toward promotion only
    /// if the kernel reports more data queued on the socket
    /// (`IORING_CQE_F_SOCK_NONEMPTY`).
    pub nonempty: bool,
}

pub struct Shared {
    inc: bool,
    /// Group 0 takes every connection; group 1, when present, the
    /// streaming ones.
    groups: Vec<Group>,
    promote: Option<Promote>,
    conns: Vec<Conn>,
    fallback_chunk: usize,
    fallback_mem: *mut u8,
    fallback_free: Vec<usize>,
    lent: u64,
    copied: u64,
    fallbacks: u64,
    rearms: u64,
    promotions: u64,
    demotions: u64,
    /// Connections whose target is the large group.
    promoted: usize,
    /// Completions that filled their buffer, and those of them flagged
    /// `IORING_CQE_F_SOCK_NONEMPTY`.
    full_completions: u64,
    full_nonempty: u64,
    hold_every: usize,
    hold_first: usize,
    hold: Duration,
    /// `--no-thp`: back the buffers with 4 KiB pages.
    no_thp: bool,
    /// `--recv-len`: the most bytes one completion takes; 0 for no cap.
    recv_len: u32,
    /// `--bounded-acc`: copy into the accumulator only what completes a
    /// pending message, and parse the rest of a completion in place.
    bounded_acc: bool,
    releases: VecDeque<(Instant, usize, usize)>,
    scratch: Vec<u8>,
    held_lends: u64,
    capped_copies: u64,
    /// Time from a connection's first `-ENOBUFS` to its next delivery.
    starve_waits: Vec<Duration>,
}

impl Shared {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        inc: bool,
        bufs: usize,
        buf_size: usize,
        nconns: usize,
        hold_every: usize,
        hold_first: usize,
        hold_us: u64,
        lend_cap: f64,
        no_thp: bool,
        recv_len: u32,
        bounded_acc: bool,
        large: Option<(usize, usize, Promote)>,
    ) -> Self {
        let max_buf = large.as_ref().map_or(buf_size, |l| l.1.max(buf_size));
        let fallback_chunk = (4 * max_buf).max(1 << 20);
        let mut groups = vec![Group::new(bufs, buf_size, lend_cap)];
        let promote = large.map(|(lbufs, lsize, p)| {
            groups.push(Group::new(lbufs, lsize, lend_cap));
            p
        });
        Shared {
            inc,
            groups,
            promote,
            conns: (0..nconns)
                .map(|_| Conn {
                    acc: Vec::with_capacity(PAGE),
                    head: 0,
                    rearm: false,
                    fallback: None,
                    dead: false,
                    group: 0,
                    target: 0,
                    cancelling: false,
                    full_run: 0,
                    small_run: 0,
                    last_data: None,
                    max_gap: Duration::ZERO,
                    last_gap: None,
                    starved_at: None,
                    enobufs: 0,
                })
                .collect(),
            fallback_chunk,
            fallback_mem: mmap_anon(FALLBACK_SLOTS * fallback_chunk),
            fallback_free: (0..FALLBACK_SLOTS).rev().collect(),
            lent: 0,
            copied: 0,
            fallbacks: 0,
            rearms: 0,
            promotions: 0,
            demotions: 0,
            promoted: 0,
            full_completions: 0,
            full_nonempty: 0,
            hold_every,
            hold_first,
            no_thp,
            recv_len,
            bounded_acc,
            hold: Duration::from_micros(hold_us),
            releases: VecDeque::new(),
            scratch: Vec::new(),
            held_lends: 0,
            capped_copies: 0,
            starve_waits: Vec::new(),
        }
    }

    fn holder(&self, c: usize) -> bool {
        (self.hold_every != 0 && c % self.hold_every == 0) || c < self.hold_first
    }

    /// Connection `c` keeps `data` (in buffer `bid` of group `g`) lent for
    /// `hold`, or copies it when the group's lend cap is reached.
    fn hold_or_copy(&mut self, c: usize, g: usize, bid: usize, data: &[u8]) {
        if !self.holder(c) {
            return;
        }
        if self.promote.as_ref().is_some_and(|p| p.on_hold) {
            self.retarget(c, 1);
        }
        let grp = &mut self.groups[g];
        if grp.holds[bid] == 0 && grp.pinned >= grp.lend_cap {
            self.scratch.clear();
            self.scratch.extend_from_slice(data);
            self.copied += data.len() as u64;
            self.capped_copies += 1;
            return;
        }
        if grp.holds[bid] == 0 {
            grp.pinned += 1;
            grp.pinned_peak = grp.pinned_peak.max(grp.pinned);
        }
        grp.holds[bid] += 1;
        self.held_lends += 1;
        self.releases
            .push_back((Instant::now() + self.hold, g, bid));
    }

    /// Count a completion of `res` bytes toward moving `c` between groups.
    fn classify(&mut self, c: usize, res: usize, nonempty: bool) {
        let Some(p) = &self.promote else { return };
        let (bytes, after, demote_after, gap) = (p.bytes, p.after, p.demote_after, p.gap);
        let need_nonempty = p.nonempty;
        let holder = self.holder(c) && p.on_hold;
        let conn = &mut self.conns[c];
        let close = gap.is_zero() || conn.last_gap.is_some_and(|g| g <= gap);
        if res >= bytes && close && (nonempty || !need_nonempty) {
            conn.full_run += 1;
            conn.small_run = 0;
            if conn.full_run >= after {
                self.retarget(c, 1);
            }
        } else {
            conn.small_run += 1;
            conn.full_run = 0;
            if conn.small_run >= demote_after && !holder {
                self.retarget(c, 0);
            }
        }
    }

    fn retarget(&mut self, c: usize, g: usize) {
        if self.conns[c].target == g {
            return;
        }
        if g == 1 {
            let max = self.promote.as_ref().map_or(0, |p| p.max);
            if max != 0 && self.promoted >= max {
                return;
            }
            self.promoted += 1;
        } else {
            self.promoted -= 1;
        }
        let conn = &mut self.conns[c];
        conn.target = g;
        conn.full_run = 0;
        conn.small_run = 0;
        if g == 1 {
            self.promotions += 1;
        } else {
            self.demotions += 1;
        }
    }

    fn arm(&mut self, cx: &mut Ctx, c: usize) {
        let g = self.conns[c].target;
        self.conns[c].group = g;
        // `recv_len` caps how many bytes one completion takes (0: the whole
        // buffer), which bounds how far a connection's accumulator grows.
        let sqe = opcode::RecvMulti::new(types::Fd(cx.fds[c]), g as u16)
            .len(self.recv_len)
            .build()
            .user_data(ud(TAG_RECV, g as u32, c));
        cx.push(sqe);
        self.rearms += 1;
    }

    /// Hand `data` (bytes just received for `c`) to the application: in
    /// place when nothing is buffered, through the accumulator otherwise.
    fn receive(&mut self, cx: &mut Ctx, c: usize, data: &[u8]) {
        let mut data = data;
        if self.bounded_acc {
            // Complete a pending partial message from the front of `data`,
            // then parse the rest in place: the accumulator never holds more
            // than one message, whatever the completion's size.
            loop {
                let conn = &mut self.conns[c];
                if conn.head == conn.acc.len() || data.is_empty() {
                    break;
                }
                let have = conn.acc.len() - conn.head;
                let need = next_need(&conn.acc[conn.head..]);
                let take = (need - have).min(data.len());
                conn.acc.extend_from_slice(&data[..take]);
                self.copied += take as u64;
                data = &data[take..];
                if have + take < need {
                    return;
                }
                if need == 4 {
                    // Only the header was completed; read its length next.
                    continue;
                }
                let p = cx.deliver(c, &conn.acc[conn.head..]);
                conn.head += p.consumed;
                if conn.head == conn.acc.len() {
                    conn.acc.clear();
                    conn.head = 0;
                }
            }
        }
        let conn = &mut self.conns[c];
        if conn.head == conn.acc.len() {
            let p = cx.deliver(c, data);
            if p.consumed > 0 {
                self.lent += p.consumed as u64;
            }
            let rest = &data[p.consumed..];
            conn.acc.clear();
            conn.head = 0;
            conn.acc.extend_from_slice(rest);
            self.copied += rest.len() as u64;
            return;
        }
        conn.acc.extend_from_slice(data);
        self.copied += data.len() as u64;
        let p = cx.deliver(c, &conn.acc[conn.head..]);
        conn.head += p.consumed;
        if conn.head == conn.acc.len() {
            conn.acc.clear();
            conn.head = 0;
        } else if conn.head >= conn.acc.len() / 2 {
            // A stream always leaves a partial message: drop what was
            // consumed, as ringline's accumulator does, or `acc` grows
            // without bound.
            conn.acc.drain(..conn.head);
            conn.head = 0;
        }
    }
}

/// p50, p99 and max of `v` in milliseconds, as `p50/p99/max`.
fn ms_quantiles(mut v: Vec<Duration>) -> String {
    if v.is_empty() {
        return "-".into();
    }
    v.sort_unstable();
    let q = |p: f64| v[((v.len() - 1) as f64 * p).round() as usize].as_secs_f64() * 1e3;
    format!("{:.1}/{:.1}/{:.1}", q(0.5), q(0.99), q(1.0))
}

impl Shared {
    /// Per-connection stalls: the longest gap between deliveries, split by
    /// whether the connection ever saw `-ENOBUFS`, and how long a starved
    /// connection waited for its next delivery. Covers warmup too.
    fn stall_report(&self) -> String {
        let live = self.conns.iter().filter(|c| c.last_data.is_some());
        let (hit, clean): (Vec<_>, Vec<_>) = live.partition(|c| c.enobufs > 0);
        format!(
            "enob_conns={} gap_enob_ms={} gap_clean_ms={} starve_wait_ms={}",
            hit.len(),
            ms_quantiles(hit.iter().map(|c| c.max_gap).collect()),
            ms_quantiles(clean.iter().map(|c| c.max_gap).collect()),
            ms_quantiles(self.starve_waits.clone()),
        )
    }
}

/// The full size of the message starting at `buf` (header included), or 4
/// while its header is incomplete.
fn next_need(buf: &[u8]) -> usize {
    if buf.len() < 4 {
        return 4;
    }
    4 + u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize
}

/// Resident KiB of the `len` bytes at `base`, by `mincore`.
fn resident_kb(base: *mut u8, len: usize) -> usize {
    if base.is_null() || len == 0 {
        return 0;
    }
    let pages = len.div_ceil(PAGE);
    let mut vec = vec![0u8; pages];
    let rc = unsafe { libc::mincore(base.cast(), len, vec.as_mut_ptr()) };
    if rc != 0 {
        return 0;
    }
    vec.iter().filter(|&&b| b & 1 != 0).count() * PAGE / 1024
}

impl Strategy for Shared {
    fn name(&self) -> String {
        let g = &self.groups[0];
        let base = match (self.promote.is_some(), self.inc) {
            (false, false) => "shared",
            (false, true) => "shared_inc",
            (true, false) => "two_ring",
            (true, true) => "two_ring_inc",
        };
        match self.groups.get(1) {
            Some(l) => format!("{base}-{}x{}+{}x{}", g.bufs, g.buf_size, l.bufs, l.buf_size),
            None => format!("{base}-{}x{}", g.bufs, g.buf_size),
        }
    }

    fn start(&mut self, cx: &mut Ctx) {
        for (i, g) in self.groups.iter_mut().enumerate() {
            g.start(cx, i as u16, self.inc, self.no_thp);
        }
        for c in 0..self.conns.len() {
            self.arm(cx, c);
        }
    }

    fn on_recv(&mut self, cx: &mut Ctx, c: usize, tag: u64, extra: u32, res: i32, flags: u32) {
        let now = Instant::now();
        if res > 0 {
            let conn = &mut self.conns[c];
            conn.last_gap = conn.last_data.map(|at| now - at);
            if let Some(gap) = conn.last_gap {
                conn.max_gap = conn.max_gap.max(gap);
            }
            conn.last_data = Some(now);
            if let Some(at) = conn.starved_at.take() {
                self.starve_waits.push(now - at);
            }
        } else if res == -libc::ENOBUFS {
            let conn = &mut self.conns[c];
            conn.enobufs += 1;
            conn.starved_at.get_or_insert(now);
        }
        if tag == TAG_RECV_AUX {
            // A fallback recv into pool slot `extra`.
            let slot = extra as usize;
            self.conns[c].fallback = None;
            if res > 0 {
                let data = unsafe {
                    std::slice::from_raw_parts(
                        self.fallback_mem.add(slot * self.fallback_chunk),
                        res as usize,
                    )
                };
                self.receive(cx, c, data);
                self.conns[c].rearm = true;
            } else {
                self.conns[c].dead = true;
            }
            self.fallback_free.push(slot);
            return;
        }
        // The group this recv was armed on; a completion that arrives after
        // its connection moved still names the buffer of the old group.
        let g = extra as usize;
        let more = cqueue::more(flags);
        if !more {
            self.conns[c].cancelling = false;
        }
        if res > 0 {
            let bid = cqueue::buffer_select(flags).expect("recv without a buffer") as usize;
            let off = if self.inc {
                self.groups[g].buf_off[bid]
            } else {
                0
            };
            let data = unsafe {
                std::slice::from_raw_parts(self.groups[g].buffer(bid).add(off), res as usize)
            };
            self.receive(cx, c, data);
            self.hold_or_copy(c, g, bid, data);
            if res as usize >= self.groups[g].buf_size {
                self.full_completions += 1;
                if cqueue::sock_nonempty(flags) {
                    self.full_nonempty += 1;
                }
            }
            self.classify(c, res as usize, cqueue::sock_nonempty(flags));
            // The buffer goes back once the kernel is done with it and no
            // hold remains.
            let grp = &mut self.groups[g];
            let done = if self.inc {
                grp.buf_off[bid] += res as usize;
                !cqueue::buffer_more(flags)
            } else {
                true
            };
            if done {
                if grp.holds[bid] == 0 {
                    grp.give_back(bid);
                } else {
                    grp.exhausted[bid] = true;
                    grp.waiting += 1;
                }
            }
            let conn = &mut self.conns[c];
            if !more {
                conn.rearm = true;
            } else if conn.target != conn.group && !conn.cancelling {
                // Move: end the live recv; its last completion re-arms the
                // connection on its new group.
                conn.cancelling = true;
                let sqe = opcode::AsyncCancel::new(ud(TAG_RECV, g as u32, c))
                    .build()
                    .user_data(ud(TAG_CANCEL, 0, c));
                cx.push(sqe);
            }
            return;
        }
        if res == -libc::ECANCELED {
            self.conns[c].rearm = true;
            return;
        }
        if res == -libc::ENOBUFS {
            self.groups[g].enobufs += 1;
            let conn = &self.conns[c];
            if conn.head < conn.acc.len() && conn.fallback.is_none() {
                if let Some(slot) = self.fallback_free.pop() {
                    let ptr = unsafe { self.fallback_mem.add(slot * self.fallback_chunk) };
                    let sqe =
                        opcode::Recv::new(types::Fd(cx.fds[c]), ptr, self.fallback_chunk as u32)
                            .build()
                            .user_data(ud(TAG_RECV_AUX, slot as u32, c));
                    cx.push(sqe);
                    self.conns[c].fallback = Some(slot);
                    self.fallbacks += 1;
                    return;
                }
            }
            let grp = &mut self.groups[g];
            if grp.waiting == grp.bufs && self.conns[c].target == g {
                grp.starved.push(c);
            } else {
                self.conns[c].rearm = true;
            }
            return;
        }
        self.conns[c].dead = true;
    }

    fn settle(&mut self, cx: &mut Ctx, c: usize) {
        if self.conns[c].rearm && !self.conns[c].dead && self.conns[c].fallback.is_none() {
            self.conns[c].rearm = false;
            self.arm(cx, c);
        }
    }

    fn dead(&self, c: usize) -> bool {
        self.conns[c].dead
    }

    fn tick(&mut self, cx: &mut Ctx) -> Option<Instant> {
        let now = Instant::now();
        while let Some(&(at, g, bid)) = self.releases.front() {
            if at > now {
                break;
            }
            self.releases.pop_front();
            let grp = &mut self.groups[g];
            grp.holds[bid] -= 1;
            if grp.holds[bid] == 0 {
                grp.pinned -= 1;
                if grp.exhausted[bid] {
                    grp.give_back(bid);
                }
            }
        }
        for g in 0..self.groups.len() {
            if self.groups[g].returned {
                self.groups[g].returned = false;
                for c in std::mem::take(&mut self.groups[g].starved) {
                    if !self.conns[c].dead && self.conns[c].fallback.is_none() {
                        self.arm(cx, c);
                    }
                }
            }
        }
        self.releases.front().map(|&(at, _, _)| at)
    }

    fn report(&self) -> String {
        let g0 = &self.groups[0];
        let mut s = format!(
            "lent_bytes={} copied_bytes={} enobufs={} fallbacks={} rearms={} held_lends={} capped_copies={} pinned_peak={} ring_resident_kb={} {}",
            self.lent,
            self.copied,
            self.groups.iter().map(|g| g.enobufs).sum::<u64>(),
            self.fallbacks,
            self.rearms,
            self.held_lends,
            self.capped_copies,
            g0.pinned_peak,
            self.groups.iter().map(|g| g.resident_kb()).sum::<usize>(),
            self.stall_report(),
        );
        if let Some(l) = self.groups.get(1) {
            s += &format!(
                " small_enobufs={} large_enobufs={} large_pinned_peak={} large_resident_kb={} promotions={} demotions={} large_conns={} full={} full_nonempty={}",
                g0.enobufs,
                l.enobufs,
                l.pinned_peak,
                l.resident_kb(),
                self.promotions,
                self.demotions,
                self.conns.iter().filter(|c| c.group == 1).count(),
                self.full_completions,
                self.full_nonempty,
            );
        }
        s
    }
}
