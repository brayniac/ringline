//! `ring` and `ring_norewrite`: each connection has a one-entry
//! `IOU_PBUF_RING_INC` ring whose entry is its region's free space, and one
//! multishot recv; the kernel appends into the region.
//!
//! The region moves (resets to the front, compacts, grows) only in
//! `settle`, after the CQ is drained and before the next enter. The kernel
//! may have written past the reaped `tail` (bytes whose CQE is not yet
//! reaped, for example after an inline submit on a full SQ), so a move
//! copies through the kernel's write position, which INC records in the
//! posted entry.
//!
//! `ring` moves a posted entry in place: it rewrites the entry's address
//! between enters (relies on DEFER_TASKRUN). `ring_norewrite` never touches
//! a posted entry: the region moves only after the kernel has used the
//! entry up (`F_BUF_MORE` clear, or `-ENOBUFS`).

use crate::common::{BufRing, PAGE, PBUF_RING_INC, Region, mmap_anon};
use crate::server::{Ctx, Strategy, TAG_RECV, ud};
use io_uring::{cqueue, opcode, types};

struct Conn {
    region: Region,
    ring: BufRing,
    /// The ring's entry is live: the kernel may write into it.
    posted: bool,
    /// The next incomplete message's full size.
    need: usize,
    rearm: bool,
    dead: bool,
}

pub struct Ring {
    rewrite: bool,
    region_max: usize,
    conns: Vec<Conn>,
    /// The kernel had written past the reaped bytes when a move ran.
    ahead_of_reaped: u64,
    moves: u64,
    grows: u64,
    rearms: u64,
    enobufs: u64,
}

impl Ring {
    pub fn new(rewrite: bool, nconns: usize, region: usize, region_max: usize) -> Self {
        assert!(nconns < 65535, "one buffer group id per connection");
        let pages = mmap_anon(nconns * PAGE);
        let conns = (0..nconns)
            .map(|i| Conn {
                region: Region::new(region),
                ring: BufRing::new(unsafe { pages.add(i * PAGE) }, 1),
                posted: false,
                need: 4,
                rearm: false,
                dead: false,
            })
            .collect();
        Ring { rewrite, region_max, conns, ahead_of_reaped: 0, moves: 0, grows: 0, rearms: 0, enobufs: 0 }
    }

    fn bgid(c: usize) -> u16 {
        1 + c as u16
    }

    fn arm(&mut self, cx: &mut Ctx, c: usize) {
        let sqe = opcode::RecvMulti::new(types::Fd(cx.fds[c]), Self::bgid(c))
            .build()
            .user_data(ud(TAG_RECV, 0, c));
        cx.push(sqe);
        self.rearms += 1;
    }
}

impl Strategy for Ring {
    fn name(&self) -> String {
        format!(
            "{}-{}",
            if self.rewrite { "ring" } else { "ring_norewrite" },
            self.conns.first().map(|c| c.region.cap).unwrap_or(0)
        )
    }

    fn start(&mut self, cx: &mut Ctx) {
        for c in 0..self.conns.len() {
            let conn = &mut self.conns[c];
            unsafe {
                cx.uring
                    .submitter()
                    .register_buf_ring_with_flags(conn.ring.base as u64, 1, Self::bgid(c), PBUF_RING_INC)
                    .expect("register connection ring");
            }
            conn.ring.push(conn.region.base as u64, conn.region.cap as u32, 0);
            conn.posted = true;
            self.arm(cx, c);
        }
    }

    fn on_recv(&mut self, cx: &mut Ctx, c: usize, _tag: u64, _extra: u32, res: i32, flags: u32) {
        let conn = &mut self.conns[c];
        if res > 0 {
            conn.region.tail += res as usize;
            if !cqueue::buffer_more(flags) {
                conn.posted = false;
            }
            let p = cx.deliver(c, conn.region.unread());
            conn.region.head += p.consumed;
            conn.need = p.need;
            if !cqueue::more(flags) {
                conn.rearm = true;
            }
            return;
        }
        if res == -libc::ENOBUFS {
            self.enobufs += 1;
            conn.posted = false;
            conn.rearm = true;
            return;
        }
        conn.dead = true;
    }

    fn settle(&mut self, cx: &mut Ctx, c: usize) {
        let rewrite = self.rewrite;
        let region_max = self.region_max;
        let conn = &mut self.conns[c];
        if conn.dead {
            return;
        }
        let r = &mut conn.region;
        // Where the kernel's writes end: the posted entry's address. It can
        // be past the reaped `tail` if the kernel ran the recv while a push
        // submitted inline.
        let mut written = if conn.posted {
            (unsafe { (*conn.ring.last()).addr } - r.base as u64) as usize
        } else {
            r.tail
        };
        if written != r.tail {
            self.ahead_of_reaped += 1;
        }
        let mut moved = false;
        if rewrite || !conn.posted {
            let head = r.head;
            if conn.need > r.cap {
                let new_cap = conn.need.next_power_of_two().min(region_max).max(r.cap);
                r.relocate(new_cap, written);
                self.grows += 1;
                moved = true;
            } else if head > 0 && (written == head || written == r.cap) {
                // Empty: start over at the front. Full, with consumed bytes
                // in front: compact.
                r.relocate(r.cap, written);
                moved = true;
            }
            if moved {
                written -= head;
                self.moves += 1;
            }
        }
        if conn.posted {
            if moved {
                // Re-point the live entry at the free space, in place.
                let e = conn.ring.last();
                unsafe {
                    (*e).addr = r.base as u64 + written as u64;
                    (*e).len = (r.cap - written) as u32;
                }
            }
        } else if written < r.cap {
            conn.ring.push(r.base as u64 + written as u64, (r.cap - written) as u32, 0);
            conn.posted = true;
        }
        if conn.rearm {
            conn.rearm = false;
            self.arm(cx, c);
        }
    }

    fn dead(&self, c: usize) -> bool {
        self.conns[c].dead
    }

    fn report(&self) -> String {
        let copied: u64 = self.conns.iter().map(|c| c.region.copied).sum();
        format!(
            "moves={} grows={} copied_bytes={} ahead_of_reaped={} enobufs={} rearms={}",
            self.moves, self.grows, copied, self.ahead_of_reaped, self.enobufs, self.rearms
        )
    }
}
