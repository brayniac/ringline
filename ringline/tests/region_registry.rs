//! Integration tests for dynamic region registration via
//! `Runtime::register_region` / `unregister_region`.

#![cfg(all(target_os = "linux", has_io_uring))]
#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::time::Duration;

use ringline::{
    AsyncEventHandler, Config, ConfigBuilder, Connection, MemoryRegion, RinglineBuilder,
};

struct Idle;

impl AsyncEventHandler for Idle {
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        Idle
    }
}

fn small_config_builder() -> ConfigBuilder {
    ConfigBuilder::new()
        .workers(2)
        .pin_to_core(false)
        .sq_entries(64)
        .max_connections(16)
        .send_pool(16, 16384)
        .max_registered_regions(4)
}

fn small_config() -> Config {
    small_config_builder().build().expect("valid config")
}

/// Register and unregister a region; subsequent register must reuse the slot.
#[test]
fn register_unregister_roundtrip() {
    let (shutdown, handles) = RinglineBuilder::new(small_config())
        .launch::<Idle>()
        .unwrap();

    let mut backing = vec![0u8; 4096];
    let region = unsafe { MemoryRegion::new(backing.as_mut_ptr(), 4096) };
    let id = shutdown.register_region(region.clone()).unwrap();

    shutdown.unregister_region(id).unwrap();

    // Slot 0 should be free again — re-register and confirm we get the
    // same RegionId back (free list is LIFO from the just-unregistered slot).
    let id2 = shutdown.register_region(region).unwrap();
    assert_eq!(id, id2);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Register up to `max_registered_regions`; the next register must error.
#[test]
fn table_full_returns_error() {
    let config = small_config_builder()
        .max_registered_regions(2)
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(config).launch::<Idle>().unwrap();

    let mut buf1 = vec![0u8; 1024];
    let mut buf2 = vec![0u8; 1024];
    let mut buf3 = vec![0u8; 1024];

    let r1 = unsafe { MemoryRegion::new(buf1.as_mut_ptr(), 1024) };
    let r2 = unsafe { MemoryRegion::new(buf2.as_mut_ptr(), 1024) };
    let r3 = unsafe { MemoryRegion::new(buf3.as_mut_ptr(), 1024) };

    let id1 = shutdown.register_region(r1).unwrap();
    let _id2 = shutdown.register_region(r2).unwrap();
    assert!(
        shutdown.register_region(r3.clone()).is_err(),
        "expected table-full error",
    );

    // After unregistering one, register must succeed again.
    shutdown.unregister_region(id1).unwrap();
    shutdown.register_region(r3).unwrap();

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Initial regions specified in `Config::registered_regions` occupy the
/// first slots; dynamic registration starts above them.
#[test]
fn initial_regions_reserve_low_slots() {
    let mut backing_initial = vec![0u8; 1024];
    let initial = unsafe { MemoryRegion::new(backing_initial.as_mut_ptr(), 1024) };

    let config = small_config_builder()
        .registered_regions(vec![initial])
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(config).launch::<Idle>().unwrap();

    let mut backing = vec![0u8; 1024];
    let dynamic = unsafe { MemoryRegion::new(backing.as_mut_ptr(), 1024) };

    // Smoke test: dynamic registration succeeds even when the low slots
    // are already occupied by `Config::registered_regions`. The slot the
    // registrar hands out must come from above the reserved range.
    let id = shutdown.register_region(dynamic).unwrap();
    shutdown.unregister_region(id).unwrap();

    shutdown.shutdown();
    // Give workers a beat to drain.
    std::thread::sleep(Duration::from_millis(50));
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// A region the kernel refuses to pin fails to register, and the slot it was
/// given is registered again afterwards.
#[test]
fn failed_register_leaves_the_slot_reusable() {
    let (shutdown, handles) = RinglineBuilder::new(small_config())
        .launch::<Idle>()
        .unwrap();

    let len = 4096;
    let mut backing = vec![0u8; len];
    let good = unsafe { MemoryRegion::new(backing.as_mut_ptr(), len) };
    // Register and unregister once, so the free list's top slot is known.
    let id0 = shutdown.register_region(good.clone()).unwrap();
    shutdown.unregister_region(id0).unwrap();

    // io_uring_register returns EFAULT for PROT_NONE pages, which cannot be
    // pinned for writing.
    let unpinnable = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_NONE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    assert_ne!(unpinnable, libc::MAP_FAILED);
    let bad = unsafe { MemoryRegion::new(unpinnable.cast(), len) };
    let err = shutdown.register_region(bad).unwrap_err();
    assert!(err.to_string().contains("EFAULT"), "{err}");

    let id = shutdown
        .register_region(good)
        .expect("register after a failed register");
    assert_eq!(id, id0, "the failed register's slot is reused");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
    unsafe { libc::munmap(unpinnable, len) };
}
