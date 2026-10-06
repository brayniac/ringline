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

use crate::common::{BufRing, PAGE, PBUF_RING_INC, mmap_anon};
use crate::server::{Ctx, Strategy, TAG_RECV, TAG_RECV_AUX, ud};
use io_uring::{cqueue, opcode, types};

const FALLBACK_SLOTS: usize = 32;

struct Conn {
    acc: Vec<u8>,
    head: usize,
    rearm: bool,
    fallback: Option<usize>,
    dead: bool,
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
}

impl Shared {
    pub fn new(inc: bool, bufs: usize, buf_size: usize, nconns: usize) -> Self {
        let fallback_chunk = (4 * buf_size).max(1 << 20);
        Shared {
            inc,
            bufs,
            buf_size,
            ring: None,
            backing: std::ptr::null_mut(),
            buf_off: vec![0; bufs],
            conns: (0..nconns)
                .map(|_| Conn { acc: Vec::with_capacity(PAGE), head: 0, rearm: false, fallback: None, dead: false })
                .collect(),
            fallback_chunk,
            fallback_mem: mmap_anon(FALLBACK_SLOTS * fallback_chunk),
            fallback_free: (0..FALLBACK_SLOTS).rev().collect(),
            lent: 0,
            copied: 0,
            enobufs: 0,
            fallbacks: 0,
            rearms: 0,
        }
    }

    fn arm(&mut self, cx: &mut Ctx, c: usize) {
        let sqe = opcode::RecvMulti::new(types::Fd(cx.fds[c]), 0).build().user_data(ud(TAG_RECV, 0, c));
        cx.push(sqe);
        self.rearms += 1;
    }

    /// Hand `data` (bytes just received for `c`) to the application: in
    /// place when nothing is buffered, through the accumulator otherwise.
    fn receive(&mut self, cx: &mut Ctx, c: usize, data: &[u8]) {
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

impl Strategy for Shared {
    fn name(&self) -> String {
        format!("{}-{}x{}", if self.inc { "shared_inc" } else { "shared" }, self.bufs, self.buf_size)
    }

    fn start(&mut self, cx: &mut Ctx) {
        let ring_mem = mmap_anon((self.bufs * 16).max(PAGE));
        self.backing = mmap_anon(self.bufs * self.buf_size);
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
        if tag == TAG_RECV_AUX {
            // A fallback recv into pool slot `extra`.
            let slot = extra as usize;
            self.conns[c].fallback = None;
            if res > 0 {
                let data = unsafe {
                    std::slice::from_raw_parts(self.fallback_mem.add(slot * self.fallback_chunk), res as usize)
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
            let data = unsafe { std::slice::from_raw_parts(self.buffer(bid).add(off), res as usize) };
            self.receive(cx, c, data);
            // The buffer goes back once the kernel is done with it.
            let done = if self.inc {
                self.buf_off[bid] += res as usize;
                !cqueue::buffer_more(flags)
            } else {
                true
            };
            if done {
                self.buf_off[bid] = 0;
                let addr = self.buffer(bid) as u64;
                self.ring.as_mut().unwrap().push(addr, self.buf_size as u32, bid as u16);
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
                    let sqe = opcode::Recv::new(types::Fd(cx.fds[c]), ptr, self.fallback_chunk as u32)
                        .build()
                        .user_data(ud(TAG_RECV_AUX, slot as u32, c));
                    cx.push(sqe);
                    self.conns[c].fallback = Some(slot);
                    self.fallbacks += 1;
                    return;
                }
            }
            self.conns[c].rearm = true;
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

    fn report(&self) -> String {
        format!(
            "lent_bytes={} copied_bytes={} enobufs={} fallbacks={} rearms={}",
            self.lent, self.copied, self.enobufs, self.fallbacks, self.rearms
        )
    }
}
