//! Git pkt-line framing: `<4 hex digits length>` + payload, with `0000` flush,
//! `0001` delimiter and `0002` response-end packets.

use std::io::Read;

use crate::util::{Context, Result};

pub const MAX_PKT: usize = 65520;

/// Appends one data packet.
pub fn put(out: &mut Vec<u8>, data: &[u8]) {
    let len = data.len().saturating_add(4);
    let mut hex = [0u8; 4];
    crate::util::encode_hex(
        &u16::try_from(len).unwrap_or(u16::MAX).to_be_bytes(),
        &mut hex,
    );
    out.extend_from_slice(&hex);
    out.extend_from_slice(data);
}

pub fn put_str(out: &mut Vec<u8>, s: &str) {
    put(out, s.as_bytes());
}

pub fn flush(out: &mut Vec<u8>) {
    out.extend_from_slice(b"0000");
}

pub fn delim(out: &mut Vec<u8>) {
    out.extend_from_slice(b"0001");
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pkt {
    Data(usize),
    Flush,
    Delim,
    End,
    Eof,
}

/// Reads packets from a byte stream into an internal buffer.
pub struct Reader<R: Read> {
    r: R,
    buf: Vec<u8>,
}

impl<R: Read> Reader<R> {
    pub fn new(r: R) -> Reader<R> {
        Reader {
            r,
            buf: vec![0u8; MAX_PKT],
        }
    }

    /// The next packet; `Pkt::Data(n)` payload is `self.data(n)`. `Eof` at a clean end.
    pub fn read(&mut self) -> Result<Pkt> {
        let mut head = [0u8; 4];
        let mut got = 0usize;
        while got < 4 {
            let n = self
                .r
                .read(head.get_mut(got..).unwrap_or(&mut []))
                .ctx("reading pkt-line")?;
            if n == 0 {
                if got == 0 {
                    return Ok(Pkt::Eof);
                }
                crate::bail!("truncated pkt-line header");
            }
            got = got.saturating_add(n);
        }
        let mut len_bytes = [0u8; 2];
        if !crate::util::decode_hex(&head, &mut len_bytes) {
            crate::bail!("bad pkt-line length {:?}", String::from_utf8_lossy(&head));
        }
        let len = usize::from(u16::from_be_bytes(len_bytes));
        match len {
            0 => return Ok(Pkt::Flush),
            1 => return Ok(Pkt::Delim),
            2 => return Ok(Pkt::End),
            3 => crate::bail!("bad pkt-line length 3"),
            _ => {}
        }
        let payload = len.saturating_sub(4);
        let dst = self.buf.get_mut(..payload).ctx("pkt-line too long")?;
        self.r.read_exact(dst).ctx("reading pkt-line payload")?;
        Ok(Pkt::Data(payload))
    }

    pub fn data(&self, n: usize) -> &[u8] {
        self.buf.get(..n).unwrap_or(&[])
    }

    /// Payload without a trailing newline.
    pub fn line(&self, n: usize) -> &[u8] {
        let d = self.data(n);
        d.strip_suffix(b"\n").unwrap_or(d)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let mut b = Vec::new();
        put_str(&mut b, "command=fetch\n");
        delim(&mut b);
        put(&mut b, b"x");
        flush(&mut b);
        assert_eq!(&b[..4], b"0012");
        let mut r = Reader::new(&b[..]);
        assert_eq!(r.read().unwrap(), Pkt::Data(14));
        assert_eq!(r.line(14), b"command=fetch");
        assert_eq!(r.read().unwrap(), Pkt::Delim);
        assert_eq!(r.read().unwrap(), Pkt::Data(1));
        assert_eq!(r.read().unwrap(), Pkt::Flush);
        assert_eq!(r.read().unwrap(), Pkt::Eof);
        assert!(Reader::new(&b"00"[..]).read().is_err());
    }
}
