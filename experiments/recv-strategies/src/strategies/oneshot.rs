//! `oneshot`: one `RECV` at a time into the region's free space. The
//! kernel owns `[tail, cap)` until the recv completes, so the region moves
//! freely between completions; the recv is re-armed in `settle`. No buffer
//! rings, no buffer group ids, no reliance on DEFER_TASKRUN: it runs the
//! same under SQPOLL.

use crate::common::Region;
use crate::server::{Ctx, Strategy, TAG_RECV, ud};
use io_uring::{opcode, types};

struct Conn {
    region: Region,
    need: usize,
    in_flight: bool,
    dead: bool,
}

pub struct Oneshot {
    region_max: usize,
    conns: Vec<Conn>,
    moves: u64,
    grows: u64,
    arms: u64,
}

impl Oneshot {
    pub fn new(nconns: usize, region: usize, region_max: usize) -> Self {
        let conns = (0..nconns)
            .map(|_| Conn { region: Region::new(region), need: 4, in_flight: false, dead: false })
            .collect();
        Oneshot { region_max, conns, moves: 0, grows: 0, arms: 0 }
    }

    fn arm(&mut self, cx: &mut Ctx, c: usize) {
        let conn = &mut self.conns[c];
        let r = &conn.region;
        let sqe = opcode::Recv::new(types::Fd(cx.fds[c]), r.spare_ptr(r.tail), (r.cap - r.tail) as u32)
            .build()
            .user_data(ud(TAG_RECV, 0, c));
        conn.in_flight = true;
        cx.push(sqe);
        self.arms += 1;
    }
}

impl Strategy for Oneshot {
    fn name(&self) -> String {
        format!("oneshot-{}", self.conns.first().map(|c| c.region.cap).unwrap_or(0))
    }

    fn start(&mut self, cx: &mut Ctx) {
        for c in 0..self.conns.len() {
            self.arm(cx, c);
        }
    }

    fn on_recv(&mut self, cx: &mut Ctx, c: usize, _tag: u64, _extra: u32, res: i32, _flags: u32) {
        let conn = &mut self.conns[c];
        conn.in_flight = false;
        if res <= 0 {
            conn.dead = true;
            return;
        }
        conn.region.tail += res as usize;
        let p = cx.deliver(c, conn.region.unread());
        conn.region.head += p.consumed;
        conn.need = p.need;
    }

    fn settle(&mut self, cx: &mut Ctx, c: usize) {
        let region_max = self.region_max;
        let conn = &mut self.conns[c];
        if conn.dead || conn.in_flight {
            return;
        }
        let r = &mut conn.region;
        if conn.need > r.cap {
            let new_cap = conn.need.next_power_of_two().min(region_max).max(r.cap);
            let tail = r.tail;
            r.relocate(new_cap, tail);
            self.grows += 1;
            self.moves += 1;
        } else if r.head > 0 && (r.head == r.tail || r.tail == r.cap) {
            let tail = r.tail;
            r.relocate(r.cap, tail);
            self.moves += 1;
        }
        self.arm(cx, c);
    }

    fn dead(&self, c: usize) -> bool {
        self.conns[c].dead
    }

    fn report(&self) -> String {
        let copied: u64 = self.conns.iter().map(|c| c.region.copied).sum();
        format!("moves={} grows={} copied_bytes={} arms={}", self.moves, self.grows, copied, self.arms)
    }
}
