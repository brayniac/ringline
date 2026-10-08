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
//! Held lends (`--hold-every K --hold-us T`): every K-th connection keeps
//! each received range lent for T µs, as a forward to a slow sink does. A
//! held range pins its whole buffer. `--lend-cap F` lends only while fewer
//! than F x bufs buffers are pinned and copies otherwise. A buffer returns
//! to the ring when the kernel is done with it and no hold remains.

use crate::common::{BufRing, PAGE, PBUF_RING_INC, mmap_anon};
use crate::server::{Ctx, Strategy, TAG_RECV, TAG_RECV_AUX, ud};
use io_uring::{cqueue, opcode, types};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const FALLBACK_SLOTS: usize = 32;

struct Conn {
    acc: Vec<u8>,
    head: usize,
    rearm: bool,
    fallback: Option<usize>,
    dead: bool,
    /// When the connection last delivered bytes.
    last_data: Option<Instant>,
    /// The longest gap between two deliveries.
    max_gap: Duration,
    /// When the first `-ENOBUFS` since the last delivery arrived.
    starved_at: Option<Instant>,
    enobufs: u32,
}

pub struct Shared {
    inc: bool,
    bufs: usize,
    buf_size: usize,
    ring: Option<BufRing>,
    backing: *mut u8,
    /// `shared-inc`: where the kernel's next write into each buffer lands.
    buf_off: Vec<usize>,
    conns: Vec<Conn>,
    fallback_chunk: usize,
    fallback_mem: *mut u8,
    fallback_free: Vec<usize>,
    lent: u64,
    copied: u64,
    enobufs: u64,
    fallbacks: u64,
    rearms: u64,
    hold_every: usize,
    hold: Duration,
    /// `--no-thp`: back the buffers with 4 KiB pages.
    no_thp: bool,
    /// `--recv-len`: the most bytes one completion takes; 0 for no cap.
    recv_len: u32,
    /// `--bounded-acc`: copy into the accumulator only what completes a
    /// pending message, and parse the rest of a completion in place.
    bounded_acc: bool,
    /// Most buffers that may be pinned by holds at once.
    lend_cap: usize,
    holds: Vec<u32>,
    /// The kernel is done with the buffer; it returns when its holds end.
    exhausted: Vec<bool>,
    pinned: usize,
    /// Buffers the kernel is done with that wait for their holds.
    waiting: usize,
    /// Connections parked on `-ENOBUFS` while every buffer waits on holds;
    /// re-armed when one returns, as ringline's `recv_starved`.
    starved: Vec<usize>,
    returned: bool,
    releases: VecDeque<(Instant, usize)>,
    scratch: Vec<u8>,
    held_lends: u64,
    capped_copies: u64,
    pinned_peak: usize,
    /// Time from a connection's first `-ENOBUFS` to its next delivery.
    starve_waits: Vec<Duration>,
}

impl Shared {
    pub fn new(
        inc: bool,
        bufs: usize,
        buf_size: usize,
        nconns: usize,
        hold_every: usize,
        hold_us: u64,
        lend_cap: f64,
        no_thp: bool,
        recv_len: u32,
        bounded_acc: bool,
    ) -> Self {
        let fallback_chunk = (4 * buf_size).max(1 << 20);
        Shared {
            inc,
            bufs,
            buf_size,
            ring: None,
            backing: std::ptr::null_mut(),
            buf_off: vec![0; bufs],
            conns: (0..nconns)
                .map(|_| Conn {
                    acc: Vec::with_capacity(PAGE),
                    head: 0,
                    rearm: false,
                    fallback: None,
                    dead: false,
                    last_data: None,
                    max_gap: Duration::ZERO,
                    starved_at: None,
                    enobufs: 0,
                })
                .collect(),
            fallback_chunk,
            fallback_mem: mmap_anon(FALLBACK_SLOTS * fallback_chunk),
            fallback_free: (0..FALLBACK_SLOTS).rev().collect(),
            lent: 0,
            copied: 0,
            enobufs: 0,
            fallbacks: 0,
            rearms: 0,
            hold_every,
            no_thp,
            recv_len,
            bounded_acc,
            hold: Duration::from_micros(hold_us),
            lend_cap: (lend_cap * bufs as f64) as usize,
            holds: vec![0; bufs],
            exhausted: vec![false; bufs],
            pinned: 0,
            waiting: 0,
            starved: Vec::new(),
            returned: false,
            releases: VecDeque::new(),
            scratch: Vec::new(),
            held_lends: 0,
            capped_copies: 0,
            pinned_peak: 0,
            starve_waits: Vec::new(),
        }
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

    /// Connection `c` keeps `data` (in buffer `bid`) lent for `hold`, or
    /// copies it when the lend cap is reached.
    fn hold_or_copy(&mut self, c: usize, bid: usize, data: &[u8]) {
        if self.hold_every == 0 || c % self.hold_every != 0 {
            return;
        }
        if self.holds[bid] == 0 && self.pinned >= self.lend_cap {
            self.scratch.clear();
            self.scratch.extend_from_slice(data);
            self.copied += data.len() as u64;
            self.capped_copies += 1;
            return;
        }
        if self.holds[bid] == 0 {
            self.pinned += 1;
            self.pinned_peak = self.pinned_peak.max(self.pinned);
        }
        self.holds[bid] += 1;
        self.held_lends += 1;
        self.releases.push_back((Instant::now() + self.hold, bid));
    }

    fn arm(&mut self, cx: &mut Ctx, c: usize) {
        // `recv_len` caps how many bytes one completion takes (0: the whole
        // buffer), which bounds how far a connection's accumulator grows.
        let sqe = opcode::RecvMulti::new(types::Fd(cx.fds[c]), 0)
            .len(self.recv_len)
            .build()
            .user_data(ud(TAG_RECV, 0, c));
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

    fn buffer(&self, bid: usize) -> *mut u8 {
        unsafe { self.backing.add(bid * self.buf_size) }
    }
}

/// The full size of the message starting at `buf` (header included), or 4
/// while its header is incomplete.
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
        format!(
            "{}-{}x{}",
            if self.inc { "shared_inc" } else { "shared" },
            self.bufs,
            self.buf_size
        )
    }

    fn start(&mut self, cx: &mut Ctx) {
        let ring_mem = mmap_anon((self.bufs * 16).max(PAGE));
        self.backing = mmap_anon(self.bufs * self.buf_size);
        if self.no_thp {
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
        let flags = if self.inc { PBUF_RING_INC } else { 0 };
        unsafe {
            cx.uring
                .submitter()
                .register_buf_ring_with_flags(ring_mem as u64, self.bufs as u16, 0, flags)
                .expect("register shared ring");
        }
        let mut ring = BufRing::new(ring_mem, self.bufs as u16);
        for i in 0..self.bufs {
            ring.push(self.buffer(i) as u64, self.buf_size as u32, i as u16);
        }
        self.ring = Some(ring);
        for c in 0..self.conns.len() {
            self.arm(cx, c);
        }
    }

    fn on_recv(&mut self, cx: &mut Ctx, c: usize, tag: u64, extra: u32, res: i32, flags: u32) {
        let now = Instant::now();
        if res > 0 {
            let conn = &mut self.conns[c];
            if let Some(at) = conn.last_data {
                conn.max_gap = conn.max_gap.max(now - at);
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
        if res > 0 {
            let bid = cqueue::buffer_select(flags).expect("recv without a buffer") as usize;
            let off = if self.inc { self.buf_off[bid] } else { 0 };
            let data =
                unsafe { std::slice::from_raw_parts(self.buffer(bid).add(off), res as usize) };
            self.receive(cx, c, data);
            self.hold_or_copy(c, bid, data);
            // The buffer goes back once the kernel is done with it and no
            // hold remains.
            let done = if self.inc {
                self.buf_off[bid] += res as usize;
                !cqueue::buffer_more(flags)
            } else {
                true
            };
            if done {
                if self.holds[bid] == 0 {
                    self.give_back(bid);
                } else {
                    self.exhausted[bid] = true;
                    self.waiting += 1;
                }
            }
            if !cqueue::more(flags) {
                self.conns[c].rearm = true;
            }
            return;
        }
        if res == -libc::ENOBUFS {
            self.enobufs += 1;
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
            if self.waiting == self.bufs {
                self.starved.push(c);
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
        while let Some(&(at, bid)) = self.releases.front() {
            if at > now {
                break;
            }
            self.releases.pop_front();
            self.holds[bid] -= 1;
            if self.holds[bid] == 0 {
                self.pinned -= 1;
                if self.exhausted[bid] {
                    self.give_back(bid);
                }
            }
        }
        if self.returned {
            self.returned = false;
            for c in std::mem::take(&mut self.starved) {
                if !self.conns[c].dead && self.conns[c].fallback.is_none() {
                    self.arm(cx, c);
                }
            }
        }
        self.releases.front().map(|&(at, _)| at)
    }

    fn report(&self) -> String {
        format!(
            "lent_bytes={} copied_bytes={} enobufs={} fallbacks={} rearms={} held_lends={} capped_copies={} pinned_peak={} ring_resident_kb={} {}",
            self.lent,
            self.copied,
            self.enobufs,
            self.fallbacks,
            self.rearms,
            self.held_lends,
            self.capped_copies,
            self.pinned_peak,
            resident_kb(self.backing, self.bufs * self.buf_size),
            self.stall_report(),
        )
    }
}
