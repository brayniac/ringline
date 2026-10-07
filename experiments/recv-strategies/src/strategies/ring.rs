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
//! With `--adapt`, a connection whose posted space was used up while data
//! was flowing (`F_BUF_MORE` cleared, or `-ENOBUFS`) doubles its region at
//! the next settle, up to `--region-max`, so a busy connection posts enough
//! for the kernel to keep the arm live; one that stays empty for
//! `SHRINK_AFTER` settles halves it, down to the starting size.
//!
//! `ring` moves a posted entry in place: it rewrites the entry's address
//! between enters (relies on DEFER_TASKRUN). `ring_norewrite` never touches
//! a posted entry: the region moves only after the kernel has used the
//! entry up (`F_BUF_MORE` clear, or `-ENOBUFS`).

use crate::common::{BufRing, PAGE, PBUF_RING_INC, Region, mmap_anon};
use crate::server::{Ctx, Strategy, TAG_RECV, ud};
use io_uring::{cqueue, opcode, types};

/// Settles a connection's region must stay empty before it halves.
const SHRINK_AFTER: u32 = 64;

struct Conn {
    region: Region,
    /// `--adapt`: the posted space was used up since the last settle.
    filled: bool,
    /// `--adapt`: consecutive settles with nothing unread.
    quiet: u32,
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
    adapt: bool,
    initial: usize,
    shrinks: u64,
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
    pub fn new(rewrite: bool, adapt: bool, nconns: usize, region: usize, region_max: usize) -> Self {
        assert!(nconns < 65535, "one buffer group id per connection");
        let pages = mmap_anon(nconns * PAGE);
        let conns = (0..nconns)
            .map(|i| Conn {
                region: Region::new(region),
                filled: false,
                quiet: 0,
                ring: BufRing::new(unsafe { pages.add(i * PAGE) }, 1),
                posted: false,
                need: 4,
                rearm: false,
                dead: false,
            })
            .collect();
        Ring {
            rewrite,
            adapt,
            initial: region,
            shrinks: 0,
            region_max,
            conns,
            ahead_of_reaped: 0,
            moves: 0,
            grows: 0,
            rearms: 0,
            enobufs: 0,
        }
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
            match (self.rewrite, self.adapt) {
                (true, false) => "ring",
                (false, false) => "ring_norewrite",
                (true, true) => "ring_adapt",
                (false, true) => "ring_norewrite_adapt",
            },
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
                conn.filled = true;
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
            conn.filled = true;
            conn.rearm = true;
            return;
        }
        conn.dead = true;
    }

    fn settle(&mut self, cx: &mut Ctx, c: usize) {
        let rewrite = self.rewrite;
        let adapt = self.adapt;
        let initial = self.initial;
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
        let filled = std::mem::take(&mut conn.filled);
        if adapt {
            conn.quiet = if written == r.head { conn.quiet + 1 } else { 0 };
        }
        if rewrite || !conn.posted {
            let head = r.head;
            let mut want = r.cap;
            if conn.need > r.cap {
                want = conn.need.next_power_of_two();
            }
            if adapt && filled {
                want = want.max(r.cap * 2);
            }
            let want = want.min(region_max).max(r.cap);
            if want > r.cap {
                r.relocate(want, written);
                self.grows += 1;
                moved = true;
            } else if adapt && conn.quiet >= SHRINK_AFTER && r.cap > initial && written == head {
                r.relocate(r.cap / 2, written);
                conn.quiet = 0;
                self.shrinks += 1;
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
            "moves={} grows={} shrinks={} copied_bytes={} ahead_of_reaped={} enobufs={} rearms={} region_kib_total={}",
            self.moves,
            self.grows,
            self.shrinks,
            copied,
            self.ahead_of_reaped,
            self.enobufs,
            self.rearms,
            self.conns.iter().map(|c| c.region.cap).sum::<usize>() / 1024
        )
    }
}
