//! `SendWriter`: encode a send directly into send-pool slots
//! (`docs/send-writer-design.md`).
//!
//! Prototype: on io_uring a plaintext connection's writer fills pool slots,
//! and the commit queues them as one send. Every other connection (TLS, and
//! every connection on mio) writes into a buffer that the commit hands to
//! `send`, so the API is the same everywhere and only the io_uring plaintext
//! path saves the copy.

// On mio every writer buffers, so the pool path is unused there.
#![cfg_attr(not(has_io_uring), allow(dead_code))]

use std::io;
use std::marker::PhantomData;

use crate::handler::ConnToken;
use crate::runtime::io::{try_with_state, with_state};

/// Slots a writer records inline before it allocates.
const INLINE_SLOTS: usize = 16;

/// The slots a pool-backed writer has sealed, in order.
struct SlotList {
    inline: [u16; INLINE_SLOTS],
    len: usize,
    spill: Vec<u16>,
}

impl SlotList {
    fn new() -> Self {
        SlotList {
            inline: [0; INLINE_SLOTS],
            len: 0,
            spill: Vec::new(),
        }
    }

    fn push(&mut self, slot: u16) {
        if self.len < INLINE_SLOTS {
            self.inline[self.len] = slot;
        } else {
            self.spill.push(slot);
        }
        self.len += 1;
    }

    fn for_each(&self, mut f: impl FnMut(u16)) {
        for &s in &self.inline[..self.len.min(INLINE_SLOTS)] {
            f(s);
        }
        for &s in &self.spill {
            f(s);
        }
    }

    fn clear(&mut self) {
        self.len = 0;
        self.spill.clear();
    }
}

/// The slot being filled.
struct Current {
    slot: u16,
    base: *mut u8,
    cap: usize,
    fill: usize,
}

enum Target {
    /// io_uring plaintext: pool slots.
    Pool {
        sealed: SlotList,
        cur: Option<Current>,
        slot_size: usize,
        slot_count: usize,
    },
    /// Everything else: a buffer handed to `send` at commit.
    Buffer(Vec<u8>),
}

/// A writer over send-pool slots, handed to the closure of
/// [`SendHalf::send_with`](crate::SendHalf::send_with).
///
/// Bytes written through [`io::Write`] or [`contiguous`](Self::contiguous)
/// are queued on the connection, in order and as one unit, when the closure
/// returns. A write that needs a slot when the pool has none free fails, and
/// the call then sends nothing.
pub struct SendWriter<'a> {
    conn: ConnToken,
    target: Target,
    len: usize,
    failed: Option<io::ErrorKind>,
    _call: PhantomData<&'a mut ()>,
}

impl SendWriter<'_> {
    /// Bytes the writer has accepted.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the writer has accepted no bytes.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The pool's slot size: the largest `max` [`contiguous`](Self::contiguous)
    /// accepts.
    pub fn slot_size(&self) -> usize {
        match &self.target {
            Target::Pool { slot_size, .. } => *slot_size,
            Target::Buffer(_) => usize::MAX,
        }
    }

    /// Hand `f` a contiguous region of `max` bytes, starting a new slot if
    /// the current one has less room. `f` returns how many bytes it wrote,
    /// at most `max`. `max` larger than the slot size returns `InvalidInput`.
    pub fn contiguous<F>(&mut self, max: usize, f: F) -> io::Result<usize>
    where
        F: FnOnce(&mut [u8]) -> usize,
    {
        self.check()?;
        match &mut self.target {
            Target::Buffer(v) => {
                let at = v.len();
                v.resize(at + max, 0);
                let n = f(&mut v[at..]).min(max);
                v.truncate(at + n);
                self.len += n;
                Ok(n)
            }
            Target::Pool { slot_size, .. } => {
                if max > *slot_size {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "contiguous region larger than a send-pool slot",
                    ));
                }
                let room = self.current_room();
                if room < max && !self.next_slot() {
                    let kind = self.failed.unwrap_or(io::ErrorKind::Other);
                    return Err(self.fail(kind));
                }
                let Target::Pool { cur: Some(c), .. } = &mut self.target else {
                    unreachable!("next_slot leaves a current slot");
                };
                // SAFETY: the slot is ours until the commit or `Drop`, it
                // lives in the pool's backing allocation, which never moves,
                // and `fill + max <= cap`.
                let buf = unsafe { std::slice::from_raw_parts_mut(c.base.add(c.fill), max) };
                let n = f(buf).min(max);
                c.fill += n;
                self.len += n;
                Ok(n)
            }
        }
    }

    fn check(&self) -> io::Result<()> {
        match self.failed {
            Some(kind) => Err(io::Error::new(kind, "send writer failed earlier")),
            None => Ok(()),
        }
    }

    fn fail(&mut self, kind: io::ErrorKind) -> io::Error {
        self.failed = Some(kind);
        if kind == io::ErrorKind::InvalidInput {
            io::Error::new(kind, "send needs more slots than the send pool has")
        } else {
            io::Error::other("send copy pool exhausted")
        }
    }

    fn current_room(&self) -> usize {
        match &self.target {
            Target::Pool { cur: Some(c), .. } => c.cap - c.fill,
            _ => 0,
        }
    }

    /// Seal the current slot and take a new one. `false` if the pool has no
    /// free slot or the send would exceed the pool.
    #[cfg(has_io_uring)]
    fn next_slot(&mut self) -> bool {
        let Target::Pool {
            sealed,
            cur,
            slot_count,
            ..
        } = &mut self.target
        else {
            return false;
        };
        let held = sealed.len + usize::from(cur.is_some());
        if held + 1 > *slot_count {
            self.failed = Some(io::ErrorKind::InvalidInput);
            return false;
        }
        let taken = with_state(|driver, _| {
            if let Some(c) = cur.take() {
                driver.send_copy_pool.set_filled(c.slot, c.fill as u32);
                sealed.push(c.slot);
            }
            driver.send_copy_pool.alloc_raw()
        });
        match taken {
            Some((slot, base, cap)) => {
                *cur = Some(Current {
                    slot,
                    base,
                    cap: cap as usize,
                    fill: 0,
                });
                true
            }
            None => false,
        }
    }

    #[cfg(not(has_io_uring))]
    fn next_slot(&mut self) -> bool {
        false
    }

    /// Release every slot the writer holds.
    fn release_slots(&mut self) {
        if let Target::Pool { sealed, cur, .. } = &mut self.target {
            if sealed.len == 0 && cur.is_none() {
                return;
            }
            let cur = cur.take();
            try_with_state(|driver, _| {
                sealed.for_each(|s| driver.send_copy_pool.release(s));
                if let Some(c) = cur {
                    driver.send_copy_pool.release(c.slot);
                }
            });
            sealed.clear();
        }
    }
}

impl io::Write for SendWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.check()?;
        if buf.is_empty() {
            return Ok(0);
        }
        if let Target::Buffer(v) = &mut self.target {
            v.extend_from_slice(buf);
            self.len += buf.len();
            return Ok(buf.len());
        }
        if self.current_room() == 0 && !self.next_slot() {
            let kind = self.failed.unwrap_or(io::ErrorKind::Other);
            return Err(self.fail(kind));
        }
        let Target::Pool { cur: Some(c), .. } = &mut self.target else {
            unreachable!("next_slot leaves a current slot");
        };
        let n = buf.len().min(c.cap - c.fill);
        // SAFETY: as in `contiguous`; `fill + n <= cap`.
        unsafe { std::ptr::copy_nonoverlapping(buf.as_ptr(), c.base.add(c.fill), n) };
        c.fill += n;
        self.len += n;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for SendWriter<'_> {
    fn drop(&mut self) {
        // A commit empties the writer, so this releases slots only when the
        // closure panicked or the commit refused the send.
        self.release_slots();
    }
}

/// Run `f` with a writer for `conn`, then queue what it wrote.
pub(crate) fn send_with<R>(
    conn: ConnToken,
    f: impl FnOnce(&mut SendWriter<'_>) -> R,
) -> io::Result<R> {
    let pool = with_state(|driver, _| driver.writer_pool(conn))?;
    let target = match pool {
        Some((slot_size, slot_count)) => Target::Pool {
            sealed: SlotList::new(),
            cur: None,
            slot_size,
            slot_count,
        },
        None => Target::Buffer(Vec::new()),
    };
    let mut w = SendWriter {
        conn,
        target,
        len: 0,
        failed: None,
        _call: PhantomData,
    };
    let r = f(&mut w);
    if let Some(kind) = w.failed {
        // `w`'s Drop releases its slots.
        return Err(match kind {
            io::ErrorKind::InvalidInput => io::Error::new(
                kind,
                "send needs more slots than the send pool has (raise Config::send_pool)",
            ),
            _ => io::Error::other("send copy pool exhausted"),
        });
    }
    match &mut w.target {
        Target::Buffer(v) => {
            let data = std::mem::take(v);
            with_state(|driver, _| driver.make_ctx().send(conn, &data))?;
        }
        #[cfg(has_io_uring)]
        Target::Pool { sealed, cur, .. } => {
            if let Some(c) = cur.take() {
                with_state(|driver, _| {
                    driver.send_copy_pool.set_filled(c.slot, c.fill as u32);
                });
                sealed.push(c.slot);
            }
            let mut slots: Vec<u16> = Vec::new();
            let list: &[u16] = if sealed.len <= INLINE_SLOTS {
                &sealed.inline[..sealed.len]
            } else {
                slots.extend_from_slice(&sealed.inline);
                slots.extend_from_slice(&sealed.spill);
                &slots
            };
            let committed =
                with_state(|driver, _| driver.make_ctx().commit_writer_slots(w.conn, list));
            match committed {
                Ok(()) => sealed.clear(),
                // `w`'s Drop releases the slots.
                Err(e) => return Err(e),
            }
        }
        #[cfg(not(has_io_uring))]
        Target::Pool { .. } => unreachable!("mio writers use a buffer"),
    }
    Ok(r)
}
