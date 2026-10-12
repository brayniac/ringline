//! Cross-thread control plane for dynamic fixed-buffer region registration.
//!
//! Each worker owns a [`RegionControlRx`] that the event loop drains once per
//! tick. [`Runtime::register_region`](crate::Runtime::register_region)
//! and [`Runtime::unregister_region`](crate::Runtime::unregister_region)
//! send a [`RegionControlMsg`] to every worker, wake them via their
//! [`WakeHandle`](crate::WakeHandle), and block until every worker that
//! received the message has acked it or exited.
//!
//! Slot indices are coherent across workers: the registrar picks a slot, then
//! every worker writes the same iovec into the same `RegionId`. A failed
//! register is followed by a `Release` broadcast, which clears the slot on
//! any worker that holds it.

use std::io;
use std::sync::Mutex;

use crossbeam_channel::{Receiver, Sender, bounded, unbounded};

use crate::buffer::fixed::{MemoryRegion, RegionId};
use crate::wakeup::WakeHandle;

/// A control message dispatched to every worker for a region operation.
pub(crate) enum RegionControlMsg {
    Register {
        slot: u16,
        region: MemoryRegion,
        ack: Sender<io::Result<()>>,
    },
    Unregister {
        slot: u16,
        ack: Sender<io::Result<()>>,
    },
    /// Clear the slot if this worker holds a region in it; a no-op if the
    /// slot is empty. Sent after a failed `Register`, which can leave the
    /// region registered on some workers and not others.
    Release {
        slot: u16,
        ack: Sender<io::Result<()>>,
    },
}

/// Per-worker receiver. The event loop owns this and drains it each tick.
pub(crate) type RegionControlRx = Receiver<RegionControlMsg>;

/// Shared registrar held by [`Runtime`]. Owns the slot allocator and
/// the per-worker senders.
pub(crate) struct RegionRegistrar {
    inner: Mutex<RegistrarInner>,
    workers: Vec<Sender<RegionControlMsg>>,
    wake_handles: Vec<WakeHandle>,
}

struct RegistrarInner {
    /// Stack of free slot indices, popped on `register`, pushed on `unregister`.
    free_slots: Vec<u16>,
}

impl RegionRegistrar {
    /// Build a registrar with `max_slots` total capacity, where slots
    /// `0..reserved` are pre-occupied by the initial `Config::registered_regions`.
    pub(crate) fn new(
        max_slots: u16,
        reserved: u16,
        workers: Vec<Sender<RegionControlMsg>>,
        wake_handles: Vec<WakeHandle>,
    ) -> Self {
        debug_assert!(reserved <= max_slots);
        debug_assert_eq!(workers.len(), wake_handles.len());
        // Free list ordered so smaller slots come out first.
        let free_slots: Vec<u16> = (reserved..max_slots).rev().collect();
        RegionRegistrar {
            inner: Mutex::new(RegistrarInner { free_slots }),
            workers,
            wake_handles,
        }
    }

    /// Allocate a slot, broadcast a `Register` message to every worker, and
    /// block until all workers ack. On any worker error the first error is
    /// returned and a `Release` is broadcast. The slot goes back to the free
    /// list only if every worker acks the release. If a worker's control
    /// channel is disconnected, or its release fails, the slot is not
    /// returned to the free list and later registrations use other slots.
    pub(crate) fn register(&self, region: MemoryRegion) -> io::Result<RegionId> {
        let slot = {
            let mut inner = self.inner.lock().expect("registrar mutex poisoned");
            inner
                .free_slots
                .pop()
                .ok_or_else(|| io::Error::other("registered-region table is full"))?
        };

        match self.broadcast(|ack| RegionControlMsg::Register {
            slot,
            region: region.clone(),
            ack,
        }) {
            Ok(()) => Ok(RegionId(slot)),
            Err(e) => {
                if self
                    .broadcast(|ack| RegionControlMsg::Release { slot, ack })
                    .is_ok()
                {
                    self.return_slot(slot);
                }
                Err(e)
            }
        }
    }

    /// Broadcast an `Unregister` message and block until every worker acks.
    /// On success the slot returns to the free list.
    pub(crate) fn unregister(&self, id: RegionId) -> io::Result<()> {
        if id == RegionId::UNREGISTERED {
            return Err(io::Error::other("cannot unregister UNREGISTERED sentinel"));
        }
        let slot = id.0;
        self.broadcast(|ack| RegionControlMsg::Unregister { slot, ack })?;
        self.return_slot(slot);
        Ok(())
    }

    fn broadcast(
        &self,
        mut build: impl FnMut(Sender<io::Result<()>>) -> RegionControlMsg,
    ) -> io::Result<()> {
        let n = self.workers.len();
        let (ack_tx, ack_rx) = bounded(n);

        // A failed send (the worker's receiver has been dropped) is recorded as
        // the error and the remaining workers still get the message, so a
        // `Release` reaches every running worker. The failed message drops its ack sender, so the
        // collection below sees one fewer ack and then a disconnect.
        let mut first_err: Option<io::Error> = None;
        for (tx, wake) in self.workers.iter().zip(&self.wake_handles) {
            // Each message carries its own ack sender so workers can reply
            // independently without coordinating with each other.
            let msg = build(ack_tx.clone());
            if tx.send(msg).is_err() {
                first_err
                    .get_or_insert_with(|| io::Error::other("worker control channel disconnected"));
                continue;
            }
            wake.wake();
        }
        drop(ack_tx);

        // Collect every ack; surface the first error encountered while still
        // draining the rest so no worker is left blocked on a send.
        for _ in 0..n {
            match ack_rx.recv() {
                Ok(Ok(())) => {}
                Ok(Err(e)) if first_err.is_none() => first_err = Some(e),
                Ok(Err(_)) => {}
                Err(_) => {
                    if first_err.is_none() {
                        first_err = Some(io::Error::other(
                            "worker terminated before acknowledging region update",
                        ));
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn return_slot(&self, slot: u16) {
        let mut inner = self.inner.lock().expect("registrar mutex poisoned");
        inner.free_slots.push(slot);
    }
}

/// Build per-worker sender/receiver pairs for the region control channel.
pub(crate) fn build_worker_channels(
    num_workers: usize,
) -> (Vec<Sender<RegionControlMsg>>, Vec<RegionControlRx>) {
    let mut txs = Vec::with_capacity(num_workers);
    let mut rxs = Vec::with_capacity(num_workers);
    for _ in 0..num_workers {
        let (tx, rx) = unbounded();
        txs.push(tx);
        rxs.push(rx);
    }
    (txs, rxs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::thread;

    /// A worker that keeps a slot table the way the event loop does, without
    /// a ring. `fail_registers` is how many `Register` messages it refuses,
    /// without recording the slot, before it starts accepting them.
    fn fake_worker(rx: RegionControlRx, mut fail_registers: usize) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut held: HashSet<u16> = HashSet::new();
            for msg in rx {
                match msg {
                    RegionControlMsg::Register { slot, ack, .. } => {
                        let r = if held.contains(&slot) {
                            Err(io::Error::new(io::ErrorKind::InvalidInput, "slot occupied"))
                        } else if fail_registers > 0 {
                            fail_registers -= 1;
                            Err(io::Error::from_raw_os_error(libc::EFAULT))
                        } else {
                            held.insert(slot);
                            Ok(())
                        };
                        let _ = ack.send(r);
                    }
                    RegionControlMsg::Unregister { slot, ack } => {
                        let r = if held.remove(&slot) {
                            Ok(())
                        } else {
                            Err(io::Error::new(io::ErrorKind::InvalidInput, "slot empty"))
                        };
                        let _ = ack.send(r);
                    }
                    RegionControlMsg::Release { slot, ack } => {
                        held.remove(&slot);
                        let _ = ack.send(Ok(()));
                    }
                }
            }
        })
    }

    /// A register that fails on one worker and succeeds on another leaves
    /// the slot usable: the retry gets the same slot and succeeds on both.
    #[test]
    fn failed_register_on_one_worker_releases_the_slot_everywhere() {
        let (txs, rxs) = build_worker_channels(2);
        let wakes = (0..2)
            .map(|_| crate::wakeup::create_wake_fd().unwrap().1)
            .collect();
        let registrar = RegionRegistrar::new(4, 0, txs, wakes);
        let mut rxs = rxs.into_iter();
        let ok = fake_worker(rxs.next().unwrap(), 0);
        let failing = fake_worker(rxs.next().unwrap(), 1);

        let mut backing = [0u8; 64];
        let region = unsafe { MemoryRegion::new(backing.as_mut_ptr(), backing.len()) };
        let err = registrar.register(region.clone()).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EFAULT));

        let id = registrar
            .register(region)
            .expect("retry after a failed register");
        assert_eq!(id, RegionId(0), "the released slot is reused");
        registrar.unregister(id).unwrap();

        drop(registrar);
        ok.join().unwrap();
        failing.join().unwrap();
    }
}
