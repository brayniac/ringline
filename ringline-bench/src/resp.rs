//! A minimal RESP GET/SET server core shared by every arm of `resp-server`:
//! one request parser, one per-worker store, one response encoder. The arms
//! differ only in how bytes reach `process` and how its output reaches the
//! socket, so a difference between them is a difference in the I/O path.

use std::cell::RefCell;
use std::collections::HashMap;

/// A parsed command, borrowing from the receive buffer.
pub enum Cmd<'a> {
    Get(&'a [u8]),
    Set(&'a [u8], &'a [u8]),
    Ping,
    Other,
}

/// Longest bulk string accepted, so a malformed length cannot make the
/// caller buffer without limit.
const MAX_BULK: usize = 64 << 20;
/// Most array elements accepted in one command.
const MAX_ARGS: usize = 16;

/// Parse one command from the front of `buf`.
///
/// `Ok(None)` means more bytes are needed; `Err(())` is a protocol error.
#[allow(clippy::result_unit_err)]
pub fn parse(buf: &[u8]) -> Result<Option<(Cmd<'_>, usize)>, ()> {
    let Some((n, mut pos)) = header(buf, b'*')? else {
        return Ok(None);
    };
    if n == 0 || n > MAX_ARGS {
        return Err(());
    }
    let mut args: [&[u8]; MAX_ARGS] = [&[]; MAX_ARGS];
    for arg in args.iter_mut().take(n) {
        let Some((len, at)) = header(&buf[pos..], b'$')? else {
            return Ok(None);
        };
        if len > MAX_BULK {
            return Err(());
        }
        let start = pos + at;
        let end = start + len;
        if buf.len() < end + 2 {
            return Ok(None);
        }
        if &buf[end..end + 2] != b"\r\n" {
            return Err(());
        }
        *arg = &buf[start..end];
        pos = end + 2;
    }
    let cmd = match (n, args[0]) {
        (2, c) if c.eq_ignore_ascii_case(b"GET") => Cmd::Get(args[1]),
        (3, c) if c.eq_ignore_ascii_case(b"SET") => Cmd::Set(args[1], args[2]),
        (1, c) if c.eq_ignore_ascii_case(b"PING") => Cmd::Ping,
        _ => Cmd::Other,
    };
    Ok(Some((cmd, pos)))
}

/// Parse `<tag><decimal>\r\n` at the front of `buf`: the value and the
/// bytes it took.
fn header(buf: &[u8], tag: u8) -> Result<Option<(usize, usize)>, ()> {
    if buf.is_empty() {
        return Ok(None);
    }
    if buf[0] != tag {
        return Err(());
    }
    let mut v: usize = 0;
    let mut i = 1;
    while i < buf.len() {
        match buf[i] {
            b'\r' => {
                if i + 1 >= buf.len() {
                    return Ok(None);
                }
                if buf[i + 1] != b'\n' || i == 1 {
                    return Err(());
                }
                return Ok(Some((v, i + 2)));
            }
            d @ b'0'..=b'9' => {
                v = v
                    .checked_mul(10)
                    .and_then(|v| v.checked_add((d - b'0') as usize))
                    .ok_or(())?;
            }
            _ => return Err(()),
        }
        i += 1;
        if i > 21 {
            return Err(());
        }
    }
    Ok(None)
}

thread_local! {
    /// One store per worker thread. Every arm runs one worker per thread,
    /// so every arm uses the same store shape; with the client's
    /// backfill-on-miss each worker fills its own copy during warmup.
    static STORE: RefCell<HashMap<Vec<u8>, Vec<u8>>> = RefCell::new(HashMap::new());
}

/// Execute every complete command at the front of `buf`, appending each
/// response to `out`. Returns the bytes consumed, or `Err(())` on a protocol
/// error (the caller closes the connection).
#[allow(clippy::result_unit_err)]
pub fn process(buf: &[u8], out: &mut Vec<u8>) -> Result<usize, ()> {
    let mut used = 0;
    STORE.with(|store| {
        let mut store = store.borrow_mut();
        while let Some((cmd, n)) = parse(&buf[used..])? {
            match cmd {
                Cmd::Get(k) => match store.get(k) {
                    Some(v) => {
                        use std::io::Write as _;
                        let _ = write!(out, "${}\r\n", v.len());
                        out.extend_from_slice(v);
                        out.extend_from_slice(b"\r\n");
                    }
                    None => out.extend_from_slice(b"$-1\r\n"),
                },
                Cmd::Set(k, v) => {
                    match store.get_mut(k) {
                        Some(slot) => {
                            slot.clear();
                            slot.extend_from_slice(v);
                        }
                        None => {
                            store.insert(k.to_vec(), v.to_vec());
                        }
                    }
                    out.extend_from_slice(b"+OK\r\n");
                }
                Cmd::Ping => out.extend_from_slice(b"+PONG\r\n"),
                Cmd::Other => out.extend_from_slice(b"-ERR unsupported\r\n"),
            }
            used += n;
        }
        Ok(used)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_set_round_trip_across_split_input() {
        let req = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
        let mut out = Vec::new();
        // Every split point: the first part is processed, the rest after.
        for cut in 0..req.len() {
            out.clear();
            let a = process(&req[..cut], &mut out).unwrap();
            let mut rest = req[a..].to_vec();
            let b = process(&rest, &mut out).unwrap();
            rest.drain(..b);
            assert!(rest.is_empty(), "cut {cut}");
            assert_eq!(out, b"+OK\r\n$5\r\nhello\r\n", "cut {cut}");
        }
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse(b"GET k\r\n").is_err());
        assert!(parse(b"*2\r\n$99999999999999999999999\r\n").is_err());
    }
}
