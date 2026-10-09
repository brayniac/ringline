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
    /// A SET whose value is at least the caller's streaming threshold. Only
    /// the header (up to and including `$<len>\r\n`) is consumed; the
    /// caller receives `len` value bytes and the trailing CRLF itself.
    SetLarge(&'a [u8], usize),
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
    parse_ext(buf, 0)
}

/// `parse`, but a SET whose value is `stream_min` bytes or more (when
/// `stream_min > 0`) returns `Cmd::SetLarge` once its header has arrived.
#[allow(clippy::result_unit_err)]
pub fn parse_ext(buf: &[u8], stream_min: usize) -> Result<Option<(Cmd<'_>, usize)>, ()> {
    let Some((n, mut pos)) = header(buf, b'*')? else {
        return Ok(None);
    };
    if n == 0 || n > MAX_ARGS {
        return Err(());
    }
    let mut args: [&[u8]; MAX_ARGS] = [&[]; MAX_ARGS];
    for i in 0..n {
        let Some((len, at)) = header(&buf[pos..], b'$')? else {
            return Ok(None);
        };
        if len > MAX_BULK {
            return Err(());
        }
        if i == 2
            && n == 3
            && stream_min > 0
            && len >= stream_min
            && args[0].eq_ignore_ascii_case(b"SET")
        {
            return Ok(Some((Cmd::SetLarge(args[1], len), pos + at)));
        }
        let arg = &mut args[i];
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
        let _ = &arg;
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
    static STORE: RefCell<HashMap<Vec<u8>, bytes::Bytes>> = RefCell::new(HashMap::new());
}

/// Output of one `process_ext` call: response bytes, plus values to be sent
/// from the store without a copy, each inserted at an offset of `buf`.
#[derive(Default)]
pub struct Out {
    pub buf: Vec<u8>,
    pub vals: Vec<(usize, bytes::Bytes)>,
}

impl Out {
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty() && self.vals.is_empty()
    }
    pub fn clear(&mut self) {
        self.buf.clear();
        self.vals.clear();
    }
}

/// Per-arm options.
#[derive(Clone, Copy, Default)]
pub struct Opts {
    /// GET values of at least this many bytes go into `Out::vals` instead of
    /// being copied into `Out::buf`. 0 copies every value.
    pub zc_min: usize,
    /// SET values of at least this many bytes stop processing with
    /// `Step::Body`. 0 parses every SET in place.
    pub stream_min: usize,
}

/// Where `process_ext` stopped.
pub enum Step {
    /// Bytes consumed; more input is needed for the next command.
    Done(usize),
    /// Bytes consumed up to a large SET's value; the caller receives `len`
    /// value bytes plus the CRLF, then calls `store_set`.
    Body {
        consumed: usize,
        key: Vec<u8>,
        len: usize,
    },
}

/// Store a value received outside `process_ext`, and append its reply.
pub fn store_set(key: Vec<u8>, value: bytes::Bytes, out: &mut Out) {
    STORE.with(|store| {
        store.borrow_mut().insert(key, value);
    });
    out.buf.extend_from_slice(b"+OK\r\n");
}

/// Execute every complete command at the front of `buf`, appending each
/// response to `out`. Returns the bytes consumed, or `Err(())` on a protocol
/// error (the caller closes the connection).
#[allow(clippy::result_unit_err)]
pub fn process(buf: &[u8], out: &mut Vec<u8>) -> Result<usize, ()> {
    let mut o = Out {
        buf: std::mem::take(out),
        vals: Vec::new(),
    };
    let r = process_ext(buf, &mut o, &Opts::default());
    *out = o.buf;
    match r? {
        Step::Done(n) => Ok(n),
        Step::Body { .. } => unreachable!("streaming is off"),
    }
}

/// `process` with options: zero-copy GET values and streamed SET bodies.
#[allow(clippy::result_unit_err)]
pub fn process_ext(buf: &[u8], out: &mut Out, o: &Opts) -> Result<Step, ()> {
    use std::io::Write as _;
    let mut used = 0;
    STORE.with(|store| {
        let mut store = store.borrow_mut();
        while let Some((cmd, n)) = parse_ext(&buf[used..], o.stream_min)? {
            match cmd {
                Cmd::Get(k) => match store.get(k) {
                    Some(v) => {
                        let _ = write!(out.buf, "${}\r\n", v.len());
                        if o.zc_min > 0 && v.len() >= o.zc_min {
                            out.vals.push((out.buf.len(), v.clone()));
                        } else {
                            out.buf.extend_from_slice(v);
                        }
                        out.buf.extend_from_slice(b"\r\n");
                    }
                    None => out.buf.extend_from_slice(b"$-1\r\n"),
                },
                Cmd::Set(k, v) => {
                    store.insert(k.to_vec(), bytes::Bytes::copy_from_slice(v));
                    out.buf.extend_from_slice(b"+OK\r\n");
                }
                Cmd::SetLarge(k, len) => {
                    return Ok(Step::Body {
                        consumed: used + n,
                        key: k.to_vec(),
                        len,
                    });
                }
                Cmd::Ping => out.buf.extend_from_slice(b"+PONG\r\n"),
                Cmd::Other => out.buf.extend_from_slice(b"-ERR unsupported\r\n"),
            }
            used += n;
        }
        Ok(Step::Done(used))
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
