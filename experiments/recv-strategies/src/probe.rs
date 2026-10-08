//! Kernel probes for the facts `docs/recv-into-accumulator-design.md`
//! relies on, so each kernel the benchmark runs on reports them.
//!
//! `inc-probe` prints one `FACT name=... ok=...` line per fact.
//! `ring-limit` registers one-entry rings until registration fails and
//! prints how many fit under the current `RLIMIT_MEMLOCK`.

use crate::common::{BufRing, PAGE, PBUF_RING_INC, mmap_anon};
use io_uring::{IoUring, cqueue, opcode, types};
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::time::Duration;

fn uring() -> IoUring {
    IoUring::builder()
        .setup_single_issuer()
        .setup_defer_taskrun()
        .build(64)
        .expect("io_uring setup")
}

fn pair() -> (TcpStream, TcpStream) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let c = TcpStream::connect(l.local_addr().unwrap()).unwrap();
    let (s, _) = l.accept().unwrap();
    (c, s)
}

/// Enter with GETEVENTS (so deferred work runs) and collect CQEs.
fn reap(u: &mut IoUring, wait: usize) -> Vec<(u64, i32, u32)> {
    let ts = types::Timespec::new().nsec(200_000_000);
    let _ = u
        .submitter()
        .submit_with_args(wait, &types::SubmitArgs::new().timespec(&ts));
    std::thread::sleep(Duration::from_millis(20));
    let _ = u
        .submitter()
        .submit_with_args(0, &types::SubmitArgs::new().timespec(&ts));
    u.completion()
        .map(|c| (c.user_data(), c.result(), c.flags()))
        .collect()
}

fn fact(name: &str, ok: bool, detail: String) {
    println!("FACT name={name} ok={ok} {detail}");
}

fn register(u: &IoUring, bgid: u16) -> BufRing {
    let mem = mmap_anon(PAGE);
    unsafe {
        u.submitter()
            .register_buf_ring_with_flags(mem as u64, 1, bgid, PBUF_RING_INC)
            .expect("register INC ring");
    }
    BufRing::new(mem, 1)
}

pub fn inc_probe() {
    let region = mmap_anon(64 * 1024);
    let base = region as u64;
    let bytes =
        |off: usize, n: usize| unsafe { std::slice::from_raw_parts(region.add(off), n).to_vec() };

    // Contiguous append and in-place entry advance.
    {
        let mut u = uring();
        let mut ring = register(&u, 1);
        ring.push(base, 4096, 0);
        let (mut c, s) = pair();
        unsafe {
            u.submission()
                .push(
                    &opcode::RecvMulti::new(types::Fd(s.as_raw_fd()), 1)
                        .build()
                        .user_data(1),
                )
                .unwrap()
        };
        let mut flags = Vec::new();
        for m in [&b"hello"[..], b"world!"] {
            c.write_all(m).unwrap();
            for (_, r, f) in reap(&mut u, 1) {
                flags.push((r, cqueue::buffer_more(f), cqueue::more(f)));
            }
        }
        let entry_addr = unsafe { (*ring.last()).addr } - base;
        fact(
            "append_contiguous",
            bytes(0, 11) == b"helloworld!",
            format!("cqes={flags:?} entry_offset={entry_addr}"),
        );
        fact(
            "entry_advanced_in_place",
            entry_addr == 11,
            format!("entry_offset={entry_addr}"),
        );
    }

    // Fill, then re-post after F_BUF_MORE clears: the live arm continues.
    {
        let mut u = uring();
        let mut ring = register(&u, 2);
        ring.push(base + 8192, 16, 0);
        let (mut c, s) = pair();
        unsafe {
            u.submission()
                .push(
                    &opcode::RecvMulti::new(types::Fd(s.as_raw_fd()), 2)
                        .build()
                        .user_data(2),
                )
                .unwrap()
        };
        c.write_all(&[b'a'; 16]).unwrap();
        let first = reap(&mut u, 1);
        let filled = first
            .iter()
            .any(|&(_, r, f)| r == 16 && !cqueue::buffer_more(f) && cqueue::more(f));
        ring.push(base + 12288, 64, 0);
        c.write_all(b"second").unwrap();
        let second = reap(&mut u, 1);
        let continued = second.iter().any(|&(_, r, f)| r == 6 && cqueue::more(f))
            && bytes(12288, 6) == b"second";
        fact(
            "repost_after_buf_more_clear",
            filled && continued,
            format!("first={first:?} second={second:?}"),
        );
    }

    // Rewrite a posted, partly used entry in place between enters.
    {
        let mut u = uring();
        let mut ring = register(&u, 3);
        ring.push(base + 16384, 4096, 0);
        let (mut c, s) = pair();
        unsafe {
            u.submission()
                .push(
                    &opcode::RecvMulti::new(types::Fd(s.as_raw_fd()), 3)
                        .build()
                        .user_data(3),
                )
                .unwrap()
        };
        c.write_all(b"abc").unwrap();
        let _ = reap(&mut u, 1);
        unsafe {
            let e = ring.last();
            (*e).addr = base + 24576;
            (*e).len = 4096;
        }
        c.write_all(b"moved").unwrap();
        let r = reap(&mut u, 1);
        let ok = bytes(24576, 5) == b"moved" && bytes(16384 + 3, 5) == [0u8; 5];
        fact("rewrite_posted_entry_in_place", ok, format!("cqes={r:?}"));
    }

    // Unregister with the arm live, then register the same bgid on new
    // memory: does the stale arm write into it?
    {
        let mut u = uring();
        let mut ring = register(&u, 4);
        ring.push(base + 32768, 4096, 0);
        let (mut c, s) = pair();
        unsafe {
            u.submission()
                .push(
                    &opcode::RecvMulti::new(types::Fd(s.as_raw_fd()), 4)
                        .build()
                        .user_data(4),
                )
                .unwrap()
        };
        u.submit().unwrap();
        let unreg = u.submitter().unregister_buf_ring(4);
        let mut fresh = register(&u, 4);
        fresh.push(base + 40960, 4096, 0);
        c.write_all(b"stale!").unwrap();
        let r = reap(&mut u, 1);
        let wrote = bytes(40960, 6) == b"stale!";
        fact(
            "stale_arm_writes_into_reused_bgid",
            wrote,
            format!("unregister={unreg:?} cqes={r:?} (ok=true means the quarantine is required)"),
        );
    }

    // Unregister with the arm live, then data: -ENOBUFS, old buffer untouched.
    {
        let mut u = uring();
        let mut ring = register(&u, 5);
        ring.push(base + 49152, 4096, 0);
        let (mut c, s) = pair();
        unsafe {
            u.submission()
                .push(
                    &opcode::RecvMulti::new(types::Fd(s.as_raw_fd()), 5)
                        .build()
                        .user_data(5),
                )
                .unwrap()
        };
        u.submit().unwrap();
        let _ = u.submitter().unregister_buf_ring(5);
        c.write_all(b"late").unwrap();
        let r = reap(&mut u, 1);
        let ok = r
            .iter()
            .any(|&(_, res, f)| res == -libc::ENOBUFS && !cqueue::more(f))
            && bytes(49152, 4) == [0u8; 4];
        fact("unregister_then_data_enobufs", ok, format!("cqes={r:?}"));
    }
}

pub fn ring_limit() {
    let u = uring();
    let limit = {
        let mut rl: libc::rlimit = unsafe { std::mem::zeroed() };
        unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut rl) };
        rl.rlim_cur
    };
    let mem = mmap_anon(70_000 * PAGE);
    let mut n = 0u32;
    let mut err = None;
    for i in 0..65_000u32 {
        let r = unsafe {
            u.submitter().register_buf_ring_with_flags(
                mem.add(i as usize * PAGE) as u64,
                1,
                i as u16,
                PBUF_RING_INC,
            )
        };
        match r {
            Ok(()) => n += 1,
            Err(e) => {
                err = Some(e);
                break;
            }
        }
    }
    println!(
        "RINGLIMIT memlock_soft_kib={} rings_registered={} ring_kib_total={} stopped_by={:?}",
        limit / 1024,
        n,
        n as u64 * 4,
        err
    );
}

/// `pbuf-variants`: try `IORING_REGISTER_PBUF_RING` several ways, by raw
/// syscall, and print the errno of each, to find what a kernel rejects.
pub fn pbuf_variants() {
    #[repr(C)]
    struct BufReg {
        ring_addr: u64,
        ring_entries: u32,
        bgid: u16,
        flags: u16,
        resv: [u64; 3],
    }
    const REGISTER_PBUF_RING: libc::c_uint = 22;
    const REGISTER_BUFFERS: libc::c_uint = 0;
    fn reg(fd: i32, addr: u64, entries: u32, bgid: u16, flags: u16) -> String {
        reg_resv(fd, addr, entries, bgid, flags, 0)
    }
    fn reg_resv(fd: i32, addr: u64, entries: u32, bgid: u16, flags: u16, resv0: u64) -> String {
        let r = BufReg {
            ring_addr: addr,
            ring_entries: entries,
            bgid,
            flags,
            resv: [resv0, 0, 0],
        };
        let rc = unsafe {
            libc::syscall(
                libc::SYS_io_uring_register,
                fd,
                REGISTER_PBUF_RING,
                &r as *const BufReg,
                1,
            )
        };
        if rc < 0 {
            format!("err={}", std::io::Error::last_os_error())
        } else {
            "ok".into()
        }
    }
    let rings: [(&str, fn() -> IoUring); 2] = [
        ("default", || IoUring::new(64).expect("setup")),
        ("defer", uring),
    ];
    for (setup, mk) in rings {
        for entries in [1u32, 8, 256, 4096] {
            let u = mk();
            let mem = mmap_anon((entries as usize * 16).max(PAGE));
            println!(
                "PBUF setup={setup} mem=mmap entries={entries} flags=0 {}",
                reg(u.as_raw_fd(), mem as u64, entries, 0, 0)
            );
        }
        let u = mk();
        let mut v: *mut libc::c_void = std::ptr::null_mut();
        unsafe { libc::posix_memalign(&mut v, PAGE, PAGE) };
        println!(
            "PBUF setup={setup} mem=heap entries=8 flags=0 {}",
            reg(u.as_raw_fd(), v as u64, 8, 0, 0)
        );
        let u = mk();
        println!(
            "PBUF setup={setup} mem=kernel entries=8 flags=MMAP {}",
            reg(u.as_raw_fd(), 0, 8, 0, 1)
        );
        let u = mk();
        let mem = mmap_anon(PAGE);
        println!(
            "PBUF setup={setup} mem=mmap entries=8 bgid=7 {}",
            reg(u.as_raw_fd(), mem as u64, 8, 7, 0)
        );
        // Ubuntu's 6.8.0-139 and later are reported to invert the check on
        // `resv`: zeroed is rejected, nonzero accepted.
        let u = mk();
        let mem = mmap_anon(PAGE);
        println!(
            "PBUF setup={setup} mem=mmap entries=8 resv0=1 {}",
            reg_resv(u.as_raw_fd(), mem as u64, 8, 0, 0, 1)
        );
        // Fixed buffers, as a control for registration in general.
        let u = mk();
        let buf = mmap_anon(PAGE);
        let iov = libc::iovec {
            iov_base: buf.cast(),
            iov_len: PAGE,
        };
        let rc = unsafe {
            libc::syscall(
                libc::SYS_io_uring_register,
                u.as_raw_fd(),
                REGISTER_BUFFERS,
                &iov,
                1,
            )
        };
        println!(
            "PBUF setup={setup} register_buffers {}",
            if rc < 0 {
                format!("err={}", std::io::Error::last_os_error())
            } else {
                "ok".into()
            }
        );
    }
    // Legacy provided buffers, as a control.
    let mut u = uring();
    let mem = mmap_anon(8 * PAGE);
    let sqe = opcode::ProvideBuffers::new(mem, PAGE as i32, 8, 3, 0)
        .build()
        .user_data(9);
    unsafe { u.submission().push(&sqe).unwrap() };
    u.submit_and_wait(1).unwrap();
    let res = u.completion().next().map(|c| c.result());
    println!("PBUF provide_buffers res={res:?}");
}
