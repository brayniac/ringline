//! Steering a `SO_REUSEPORT` group with classic BPF.
//!
//! Merged accept mode gives every worker its own listener in one reuseport
//! group, and the kernel picks which one receives each connection. This module
//! takes that choice back so a worker can be removed from the rotation without
//! closing its socket — closing one **resets** whatever is already queued on it.
//!
//! Classic BPF, not eBPF, on purpose: `SO_ATTACH_REUSEPORT_CBPF` needs no
//! privileges, and a server should not have to ask for `CAP_BPF` to stop
//! sending work to a busy worker. The cost is that classic BPF cannot read a
//! map, so a policy change re-attaches a freshly built program rather than
//! updating one in place.
//!
//! The program is `idx = random % live_count`, then a jump chain mapping `idx`
//! to the live socket's index within the group. `SKF_AD_RANDOM` rather than
//! `SKF_AD_CPU`: the CPU is the *receiving* one, constant for a loopback
//! client, which funnels every connection onto a single live socket
//! (measured — see `docs/listeners-and-accept-design.md` §5).

#![cfg(target_os = "linux")]

use std::io;
use std::os::fd::RawFd;

/// Ancillary load offset base for classic BPF.
const SKF_AD_OFF: i32 = -0x1000;
/// Ancillary: a fresh random u32 per packet.
const SKF_AD_RANDOM: i32 = 56;

// Classic BPF opcode pieces. `libc` does not name these.
const BPF_LD: u16 = 0x00;
const BPF_W: u16 = 0x00;
const BPF_ABS: u16 = 0x20;
const BPF_ALU: u16 = 0x04;
const BPF_MOD: u16 = 0x90;
const BPF_K: u16 = 0x00;
const BPF_JMP: u16 = 0x05;
const BPF_JEQ: u16 = 0x10;
const BPF_RET: u16 = 0x06;

fn insn(code: u16, jt: u8, jf: u8, k: u32) -> libc::sock_filter {
    libc::sock_filter { code, jt, jf, k }
}

/// Build a program that selects uniformly among `live`, which holds indices
/// into the reuseport group.
///
/// Returns `None` for an empty live set: a program that can select nothing
/// would leave the group unable to accept at all, which is worse than the
/// imbalance it was meant to fix.
pub(crate) fn build_live_set_program(live: &[u32]) -> Option<Vec<libc::sock_filter>> {
    if live.is_empty() {
        return None;
    }
    let mut prog = Vec::with_capacity(2 + live.len() * 2 + 1);
    // A = random
    prog.push(insn(
        BPF_LD | BPF_W | BPF_ABS,
        0,
        0,
        (SKF_AD_OFF + SKF_AD_RANDOM) as u32,
    ));
    // A %= live_count
    prog.push(insn(BPF_ALU | BPF_MOD | BPF_K, 0, 0, live.len() as u32));
    // if A == i { return live[i] }
    for (i, &target) in live.iter().enumerate() {
        prog.push(insn(BPF_JMP | BPF_JEQ | BPF_K, 0, 1, i as u32));
        prog.push(insn(BPF_RET | BPF_K, 0, 0, target));
    }
    // Unreachable once the modulo holds, but a classic BPF program must not
    // fall off the end.
    prog.push(insn(BPF_RET | BPF_K, 0, 0, live[0]));
    Some(prog)
}

/// Attach a selection program to the reuseport group `fd` belongs to.
///
/// The program governs the whole group, not just this socket, so any member
/// will do — but it must be a member, and the group must already exist.
pub(crate) fn attach_live_set(fd: RawFd, live: &[u32]) -> io::Result<()> {
    let prog = build_live_set_program(live).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "refusing to steer a reuseport group to an empty live set",
        )
    })?;
    let fprog = libc::sock_fprog {
        len: prog.len() as u16,
        filter: prog.as_ptr() as *mut libc::sock_filter,
    };
    let ret = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ATTACH_REUSEPORT_CBPF,
            &fprog as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::sock_fprog>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_live_set_is_refused() {
        // A group that can select nothing accepts nothing.
        assert!(build_live_set_program(&[]).is_none());
    }

    #[test]
    fn the_program_starts_by_loading_the_random_ancillary() {
        let prog = build_live_set_program(&[0, 1]).unwrap();
        assert_eq!(prog[0].code, BPF_LD | BPF_W | BPF_ABS);
        assert_eq!(prog[0].k, (SKF_AD_OFF + SKF_AD_RANDOM) as u32);
    }

    #[test]
    fn the_modulo_is_the_live_count_not_the_group_size() {
        // Three of four workers live: the index space is 3, and the jump chain
        // maps it onto the real group indices.
        let prog = build_live_set_program(&[0, 2, 3]).unwrap();
        assert_eq!(prog[1].code, BPF_ALU | BPF_MOD | BPF_K);
        assert_eq!(prog[1].k, 3);
    }

    #[test]
    fn each_live_index_gets_a_compare_and_a_return() {
        let live = [1u32, 3];
        let prog = build_live_set_program(&live).unwrap();
        for (i, &target) in live.iter().enumerate() {
            let cmp = prog[2 + i * 2];
            let ret = prog[3 + i * 2];
            assert_eq!(cmp.code, BPF_JMP | BPF_JEQ | BPF_K);
            assert_eq!(cmp.k, i as u32, "compares against the live-set position");
            assert_eq!(ret.code, BPF_RET | BPF_K);
            assert_eq!(ret.k, target, "returns the group index, not the position");
        }
    }

    #[test]
    fn an_excluded_index_is_never_returned() {
        // Worker 2 is out: no instruction may return 2.
        let prog = build_live_set_program(&[0, 1, 3]).unwrap();
        for i in &prog {
            if i.code == BPF_RET | BPF_K {
                assert_ne!(i.k, 2, "excluded worker must not be a return target");
            }
        }
    }

    #[test]
    fn the_program_ends_in_a_return_so_it_cannot_fall_through() {
        let prog = build_live_set_program(&[0, 1, 2, 3]).unwrap();
        assert_eq!(prog.last().unwrap().code, BPF_RET | BPF_K);
    }

    #[test]
    fn a_single_live_worker_still_produces_a_valid_program() {
        let prog = build_live_set_program(&[2]).unwrap();
        assert_eq!(prog[1].k, 1, "modulo 1");
        assert!(prog.iter().any(|i| i.code == BPF_RET | BPF_K && i.k == 2));
    }
}
