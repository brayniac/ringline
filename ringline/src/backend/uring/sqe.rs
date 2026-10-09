//! A ringline-owned submission: what to do, on which file, and how to tag
//! the completion.
//!
//! The driver describes every operation as an [`Sqe`] and the ring encodes
//! it into an io_uring submission queue entry when it is pushed. Nothing
//! outside this module and `ring.rs` builds an `io_uring::squeue::Entry`.
//! Step 1 of the ring emulator (#621).

use io_uring::opcode;
use io_uring::squeue::{Entry, Entry128, Flags};
use io_uring::types::{self, CancelBuilder, DestinationSlot, Fixed, TimeoutFlags};
use std::os::fd::RawFd;

/// The file an operation targets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Fd {
    /// A slot in the ring's registered file table.
    Fixed(u32),
    /// A plain file descriptor.
    Raw(RawFd),
}

/// How an operation is linked to the next one pushed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Link {
    None,
    /// `IOSQE_IO_LINK`: the next op runs only if this one succeeds.
    Soft,
    /// `IOSQE_IO_HARDLINK`: the next op runs whatever this one's result.
    Hard,
}

/// One operation and its arguments. Pointers must stay valid until the
/// operation's completion arrives, and for `SendMsgZc` until its
/// notification (Domain Invariant 1).
#[derive(Clone, Copy, Debug)]
pub(crate) enum Op {
    /// Multishot recv selecting from provided-buffer group `buf_group`.
    RecvMulti {
        fd: Fd,
        buf_group: u16,
    },
    /// Multishot recvmsg selecting from `buf_group`, laid out by `msg`.
    RecvMsgMulti {
        fd: Fd,
        msg: *const libc::msghdr,
        buf_group: u16,
    },
    /// One-shot recv into `buf`.
    Recv {
        fd: Fd,
        buf: *mut u8,
        len: u32,
    },
    /// Multishot accept; `flags` are `accept4(2)` flags.
    AcceptMulti {
        fd: Fd,
        flags: i32,
    },
    Send {
        fd: Fd,
        buf: *const u8,
        len: u32,
        flags: i32,
    },
    SendMsg {
        fd: Fd,
        msg: *const libc::msghdr,
        flags: u32,
    },
    SendMsgZc {
        fd: Fd,
        msg: *const libc::msghdr,
    },
    Writev {
        fd: Fd,
        iovecs: *const libc::iovec,
        count: u32,
        offset: u64,
    },
    Read {
        fd: Fd,
        buf: *mut u8,
        len: u32,
        offset: u64,
    },
    Write {
        fd: Fd,
        buf: *const u8,
        len: u32,
        offset: u64,
    },
    Fsync {
        fd: Fd,
    },
    Close {
        fd: Fd,
    },
    Shutdown {
        fd: Fd,
        how: i32,
    },
    /// Cancel every request on `fd` (`ASYNC_CANCEL` with `FD | ALL`).
    CancelFdAll {
        fd: Fd,
    },
    /// Cancel the request whose user_data is exactly `target`.
    Cancel {
        target: u64,
    },
    Connect {
        fd: Fd,
        addr: *const libc::sockaddr,
        addrlen: libc::socklen_t,
    },
    /// A timeout, relative or (`abs`) absolute `CLOCK_MONOTONIC`.
    Timeout {
        ts: *const super::abi::Timespec,
        abs: bool,
    },
    /// Install a real fd for registered file `index` (`FIXED_FD_INSTALL`).
    FixedFdInstall {
        index: u32,
    },
    PollAdd {
        fd: Fd,
        mask: u32,
    },
    /// `openat(AT_FDCWD, …)`, installing the result into registered slot
    /// `file_index`.
    OpenAt {
        path: *const libc::c_char,
        flags: i32,
        mode: u32,
        file_index: u32,
    },
    /// `statx(AT_FDCWD, path, AT_STATX_SYNC_AS_STAT, STATX_BASIC_STATS)`.
    Statx {
        path: *const libc::c_char,
        buf: *mut libc::statx,
    },
    RenameAt {
        old: *const libc::c_char,
        new: *const libc::c_char,
    },
    UnlinkAt {
        path: *const libc::c_char,
        flags: i32,
    },
    MkDirAt {
        path: *const libc::c_char,
        mode: u32,
    },
    /// NVMe passthrough (`URING_CMD`, `cmd_op`) with an 80-byte command.
    UringCmd80 {
        fd: Fd,
        cmd_op: u32,
        cmd: [u8; 80],
    },
}

/// An operation, its completion tag and its link to the next operation.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Sqe {
    pub(crate) op: Op,
    pub(crate) user_data: u64,
    pub(crate) link: Link,
}

impl Sqe {
    pub(crate) fn new(op: Op, user_data: u64) -> Self {
        Sqe {
            op,
            user_data,
            link: Link::None,
        }
    }

    /// A stream send of `len` bytes at `buf` on registered file `index`,
    /// with `MSG_WAITALL` (Domain Invariant 5).
    pub(crate) fn stream_send(index: u32, buf: *const u8, len: u32, user_data: u64) -> Self {
        Sqe::new(
            Op::Send {
                fd: Fd::Fixed(index),
                buf,
                len,
                flags: crate::completion::STREAM_SEND_FLAGS,
            },
            user_data,
        )
    }

    /// A zero-copy `sendmsg` on registered file `index`.
    pub(crate) fn send_msg_zc(index: u32, msg: *const libc::msghdr, user_data: u64) -> Self {
        Sqe::new(
            Op::SendMsgZc {
                fd: Fd::Fixed(index),
                msg,
            },
            user_data,
        )
    }

    pub(crate) fn link(mut self, link: Link) -> Self {
        self.link = link;
        self
    }

    /// The 128-byte entry the ring's submission queue holds.
    pub(crate) fn encode(&self) -> Entry128 {
        let e: Entry128 = match self.op {
            Op::UringCmd80 { fd, cmd_op, cmd } => {
                let e = match fd {
                    Fd::Fixed(i) => opcode::UringCmd80::new(Fixed(i), cmd_op).cmd(cmd).build(),
                    Fd::Raw(f) => opcode::UringCmd80::new(types::Fd(f), cmd_op)
                        .cmd(cmd)
                        .build(),
                };
                e.user_data(self.user_data)
            }
            _ => self.encode64().into(),
        };
        match self.link {
            Link::None => e,
            Link::Soft => e.flags(Flags::IO_LINK),
            Link::Hard => e.flags(Flags::IO_HARDLINK),
        }
    }

    /// The 64-byte entry, without link flags. Panics on `UringCmd80`, which
    /// needs [`Sqe::encode`].
    pub(crate) fn encode64(&self) -> Entry {
        macro_rules! on {
            ($fd:expr, |$t:ident| $build:expr) => {
                match $fd {
                    Fd::Fixed(i) => {
                        let $t = Fixed(i);
                        $build
                    }
                    Fd::Raw(f) => {
                        let $t = types::Fd(f);
                        $build
                    }
                }
            };
        }
        let e = match self.op {
            Op::RecvMulti { fd, buf_group } => {
                on!(fd, |t| opcode::RecvMulti::new(t, buf_group).build())
            }
            Op::RecvMsgMulti { fd, msg, buf_group } => {
                on!(fd, |t| opcode::RecvMsgMulti::new(t, msg, buf_group).build())
            }
            Op::Recv { fd, buf, len } => on!(fd, |t| opcode::Recv::new(t, buf, len).build()),
            Op::AcceptMulti { fd, flags } => {
                on!(fd, |t| opcode::AcceptMulti::new(t).flags(flags).build())
            }
            Op::Send {
                fd,
                buf,
                len,
                flags,
            } => {
                on!(fd, |t| opcode::Send::new(t, buf, len).flags(flags).build())
            }
            Op::SendMsg { fd, msg, flags } => {
                on!(fd, |t| opcode::SendMsg::new(t, msg).flags(flags).build())
            }
            Op::SendMsgZc { fd, msg } => on!(fd, |t| opcode::SendMsgZc::new(t, msg).build()),
            Op::Writev {
                fd,
                iovecs,
                count,
                offset,
            } => {
                on!(fd, |t| opcode::Writev::new(t, iovecs, count)
                    .offset(offset)
                    .build())
            }
            Op::Read {
                fd,
                buf,
                len,
                offset,
            } => {
                on!(fd, |t| opcode::Read::new(t, buf, len)
                    .offset(offset)
                    .build())
            }
            Op::Write {
                fd,
                buf,
                len,
                offset,
            } => {
                on!(fd, |t| opcode::Write::new(t, buf, len)
                    .offset(offset)
                    .build())
            }
            Op::Fsync { fd } => on!(fd, |t| opcode::Fsync::new(t).build()),
            Op::Close { fd } => match fd {
                Fd::Fixed(i) => opcode::Close::new(Fixed(i)).build(),
                Fd::Raw(f) => opcode::Close::new(types::Fd(f)).build(),
            },
            Op::Shutdown { fd, how } => on!(fd, |t| opcode::Shutdown::new(t, how).build()),
            Op::CancelFdAll { fd } => match fd {
                Fd::Fixed(i) => {
                    opcode::AsyncCancel2::new(CancelBuilder::fd(Fixed(i)).all()).build()
                }
                Fd::Raw(f) => {
                    opcode::AsyncCancel2::new(CancelBuilder::fd(types::Fd(f)).all()).build()
                }
            },
            Op::Cancel { target } => opcode::AsyncCancel::new(target).build(),
            Op::Connect { fd, addr, addrlen } => {
                on!(fd, |t| opcode::Connect::new(t, addr, addrlen).build())
            }
            Op::Timeout { ts, abs } => {
                // `abi::Timespec` is `struct __kernel_timespec`, as is the
                // crate's type; a test pins the layouts together.
                let ts = ts.cast::<types::Timespec>();
                if abs {
                    opcode::Timeout::new(ts).flags(TimeoutFlags::ABS).build()
                } else {
                    opcode::Timeout::new(ts).build()
                }
            }
            Op::FixedFdInstall { index } => opcode::FixedFdInstall::new(Fixed(index), 0).build(),
            Op::PollAdd { fd, mask } => on!(fd, |t| opcode::PollAdd::new(t, mask).build()),
            Op::OpenAt {
                path,
                flags,
                mode,
                file_index,
            } => {
                let dest = DestinationSlot::try_from_slot_target(file_index)
                    .expect("file_index validated by the caller");
                opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path)
                    .flags(flags)
                    .mode(mode)
                    .file_index(Some(dest))
                    .build()
            }
            Op::Statx { path, buf } => {
                opcode::Statx::new(types::Fd(libc::AT_FDCWD), path, buf as *mut types::statx)
                    .flags(libc::AT_STATX_SYNC_AS_STAT)
                    .mask(0x7ff) // STATX_BASIC_STATS
                    .build()
            }
            Op::RenameAt { old, new } => opcode::RenameAt::new(
                types::Fd(libc::AT_FDCWD),
                old,
                types::Fd(libc::AT_FDCWD),
                new,
            )
            .build(),
            Op::UnlinkAt { path, flags } => opcode::UnlinkAt::new(types::Fd(libc::AT_FDCWD), path)
                .flags(flags)
                .build(),
            Op::MkDirAt { path, mode } => opcode::MkDirAt::new(types::Fd(libc::AT_FDCWD), path)
                .mode(mode)
                .build(),
            Op::UringCmd80 { .. } => unreachable!("URING_CMD needs a 128-byte entry; use encode"),
        };
        e.user_data(self.user_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use io_uring::squeue::Flags;

    fn bytes(e: &Entry128) -> Vec<u8> {
        let n = std::mem::size_of::<Entry128>();
        unsafe { std::slice::from_raw_parts(e as *const Entry128 as *const u8, n).to_vec() }
    }

    /// Each operation encodes to the same bytes as the equivalent
    /// `io_uring::opcode` builder chain: opcode, fields, flags and
    /// user_data. Call sites' arguments are not covered here.
    #[test]
    fn every_op_encodes_like_its_opcode_builder() {
        let ud = 0x0123_4567_89ab_cdef;
        let p = 0x1000 as *mut u8;
        let msg = 0x2000 as *const libc::msghdr;
        let path = 0x3000 as *const libc::c_char;
        let path2 = 0x3100 as *const libc::c_char;
        let ts = 0x4000 as *const crate::backend::uring::abi::Timespec;
        let iov = 0x5000 as *const libc::iovec;
        let addr = 0x6000 as *const libc::sockaddr;
        let stx = 0x7000 as *mut libc::statx;
        let cmd = [7u8; 80];
        let fx = Fixed(9);
        let raw = types::Fd(11);
        let e = |x: Entry| -> Entry128 { x.user_data(ud).into() };
        let cases: Vec<(Sqe, Entry128)> = vec![
            (
                Sqe::new(
                    Op::RecvMulti {
                        fd: Fd::Fixed(9),
                        buf_group: 3,
                    },
                    ud,
                ),
                e(opcode::RecvMulti::new(fx, 3).build()),
            ),
            (
                Sqe::new(
                    Op::RecvMsgMulti {
                        fd: Fd::Fixed(9),
                        msg,
                        buf_group: 3,
                    },
                    ud,
                ),
                e(opcode::RecvMsgMulti::new(fx, msg, 3).build()),
            ),
            (
                Sqe::new(
                    Op::Recv {
                        fd: Fd::Fixed(9),
                        buf: p,
                        len: 77,
                    },
                    ud,
                ),
                e(opcode::Recv::new(fx, p, 77).build()),
            ),
            (
                Sqe::new(
                    Op::AcceptMulti {
                        fd: Fd::Raw(11),
                        flags: libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                    },
                    ud,
                ),
                e(opcode::AcceptMulti::new(raw)
                    .flags(libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
                    .build()),
            ),
            (
                Sqe::stream_send(9, p, 77, ud),
                e(opcode::Send::new(fx, p, 77)
                    .flags(crate::completion::STREAM_SEND_FLAGS)
                    .build()),
            ),
            (
                Sqe::new(
                    Op::Send {
                        fd: Fd::Raw(11),
                        buf: p,
                        len: 77,
                        flags: 0,
                    },
                    ud,
                ),
                e(opcode::Send::new(raw, p, 77).build()),
            ),
            (
                Sqe::new(
                    Op::SendMsg {
                        fd: Fd::Raw(11),
                        msg,
                        flags: crate::completion::STREAM_SEND_FLAGS as u32,
                    },
                    ud,
                ),
                e(opcode::SendMsg::new(raw, msg)
                    .flags(crate::completion::STREAM_SEND_FLAGS as u32)
                    .build()),
            ),
            (
                Sqe::new(
                    Op::SendMsg {
                        fd: Fd::Fixed(9),
                        msg,
                        flags: 0,
                    },
                    ud,
                ),
                e(opcode::SendMsg::new(fx, msg).build()),
            ),
            (
                Sqe::send_msg_zc(9, msg, ud),
                e(opcode::SendMsgZc::new(fx, msg).build()),
            ),
            (
                Sqe::new(
                    Op::Writev {
                        fd: Fd::Raw(11),
                        iovecs: iov,
                        count: 4,
                        offset: 99,
                    },
                    ud,
                ),
                e(opcode::Writev::new(raw, iov, 4).offset(99).build()),
            ),
            (
                Sqe::new(
                    Op::Read {
                        fd: Fd::Raw(11),
                        buf: p,
                        len: 8,
                        offset: 0,
                    },
                    ud,
                ),
                e(opcode::Read::new(raw, p, 8).build()),
            ),
            (
                Sqe::new(
                    Op::Read {
                        fd: Fd::Fixed(9),
                        buf: p,
                        len: 4096,
                        offset: 8192,
                    },
                    ud,
                ),
                e(opcode::Read::new(fx, p, 4096).offset(8192).build()),
            ),
            (
                Sqe::new(
                    Op::Write {
                        fd: Fd::Fixed(9),
                        buf: p,
                        len: 4096,
                        offset: 8192,
                    },
                    ud,
                ),
                e(opcode::Write::new(fx, p, 4096).offset(8192).build()),
            ),
            (
                Sqe::new(Op::Fsync { fd: Fd::Fixed(9) }, ud),
                e(opcode::Fsync::new(fx).build()),
            ),
            (
                Sqe::new(Op::Close { fd: Fd::Fixed(9) }, ud),
                e(opcode::Close::new(fx).build()),
            ),
            (
                Sqe::new(
                    Op::Shutdown {
                        fd: Fd::Fixed(9),
                        how: libc::SHUT_RDWR,
                    },
                    ud,
                ),
                e(opcode::Shutdown::new(fx, libc::SHUT_RDWR).build()),
            ),
            (
                Sqe::new(Op::CancelFdAll { fd: Fd::Fixed(9) }, ud),
                e(opcode::AsyncCancel2::new(CancelBuilder::fd(fx).all()).build()),
            ),
            (
                Sqe::new(Op::Cancel { target: 42 }, ud),
                e(opcode::AsyncCancel::new(42).build()),
            ),
            (
                Sqe::new(
                    Op::Connect {
                        fd: Fd::Fixed(9),
                        addr,
                        addrlen: 16,
                    },
                    ud,
                ),
                e(opcode::Connect::new(fx, addr, 16).build()),
            ),
            (
                Sqe::new(Op::Timeout { ts, abs: false }, ud),
                e(opcode::Timeout::new(ts.cast()).build()),
            ),
            (
                Sqe::new(Op::Timeout { ts, abs: true }, ud),
                e(opcode::Timeout::new(ts.cast())
                    .flags(TimeoutFlags::ABS)
                    .build()),
            ),
            (
                Sqe::new(Op::FixedFdInstall { index: 9 }, ud),
                e(opcode::FixedFdInstall::new(fx, 0).build()),
            ),
            (
                Sqe::new(
                    Op::PollAdd {
                        fd: Fd::Fixed(9),
                        mask: libc::POLLOUT as u32,
                    },
                    ud,
                ),
                e(opcode::PollAdd::new(fx, libc::POLLOUT as u32).build()),
            ),
            (
                Sqe::new(
                    Op::OpenAt {
                        path,
                        flags: libc::O_RDONLY,
                        mode: 0o644,
                        file_index: 5,
                    },
                    ud,
                ),
                e(opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path)
                    .flags(libc::O_RDONLY)
                    .mode(0o644)
                    .file_index(Some(DestinationSlot::try_from_slot_target(5).unwrap()))
                    .build()),
            ),
            (
                Sqe::new(Op::Statx { path, buf: stx }, ud),
                e(
                    opcode::Statx::new(types::Fd(libc::AT_FDCWD), path, stx as *mut types::statx)
                        .flags(libc::AT_STATX_SYNC_AS_STAT)
                        .mask(0x7ff)
                        .build(),
                ),
            ),
            (
                Sqe::new(
                    Op::RenameAt {
                        old: path,
                        new: path2,
                    },
                    ud,
                ),
                e(opcode::RenameAt::new(
                    types::Fd(libc::AT_FDCWD),
                    path,
                    types::Fd(libc::AT_FDCWD),
                    path2,
                )
                .build()),
            ),
            (
                Sqe::new(
                    Op::UnlinkAt {
                        path,
                        flags: libc::AT_REMOVEDIR,
                    },
                    ud,
                ),
                e(opcode::UnlinkAt::new(types::Fd(libc::AT_FDCWD), path)
                    .flags(libc::AT_REMOVEDIR)
                    .build()),
            ),
            (
                Sqe::new(Op::MkDirAt { path, mode: 0o755 }, ud),
                e(opcode::MkDirAt::new(types::Fd(libc::AT_FDCWD), path)
                    .mode(0o755)
                    .build()),
            ),
            (
                Sqe::new(
                    Op::UringCmd80 {
                        fd: Fd::Fixed(9),
                        cmd_op: 0x42,
                        cmd,
                    },
                    ud,
                ),
                opcode::UringCmd80::new(fx, 0x42)
                    .cmd(cmd)
                    .build()
                    .user_data(ud),
            ),
        ];
        for (i, (sqe, want)) in cases.iter().enumerate() {
            assert_eq!(bytes(&sqe.encode()), bytes(want), "case {i}: {:?}", sqe.op);
        }
    }

    #[test]
    fn links_set_the_link_flags() {
        let base = Sqe::new(Op::Cancel { target: 1 }, 2);
        let plain: Entry128 = opcode::AsyncCancel::new(1).build().user_data(2).into();
        let cases = [
            (Link::None, plain.clone()),
            (Link::Soft, plain.clone().flags(Flags::IO_LINK)),
            (Link::Hard, plain.flags(Flags::IO_HARDLINK)),
        ];
        for (link, want) in cases {
            assert_eq!(bytes(&base.link(link).encode()), bytes(&want), "{link:?}");
        }
    }
}
