//! Git pkt-line framing (protocol v2 uses it for requests and responses).

use core::fmt;

use crate::arena::{OutOfCapacity, try_extend};

pub const FLUSH_PKT: &[u8; 4] = b"0000";
pub const DELIM_PKT: &[u8; 4] = b"0001";
pub const RESPONSE_END_PKT: &[u8; 4] = b"0002";
pub const MAX_PKT_LEN: usize = 65520;
pub const MAX_PKT_PAYLOAD: usize = MAX_PKT_LEN - 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pkt<'a> {
    Flush,
    Delim,
    ResponseEnd,
    Data(&'a [u8]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PktError {
    BadLength,
}

impl fmt::Display for PktError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid pkt-line length")
    }
}

impl std::error::Error for PktError {}

#[inline]
fn hex_nibble(c: u8) -> Option<usize> {
    match c {
        b'0'..=b'9' => Some(usize::from(c.wrapping_sub(b'0'))),
        b'a'..=b'f' => Some(usize::from(c.wrapping_sub(b'a')).saturating_add(10)),
        b'A'..=b'F' => Some(usize::from(c.wrapping_sub(b'A')).saturating_add(10)),
        _ => None,
    }
}

/// Parses one pkt-line at the front of `b`. `Ok(None)` means more bytes are needed.
/// Returns the packet and the number of bytes it occupied.
pub fn parse_pkt(b: &[u8]) -> Result<Option<(Pkt<'_>, usize)>, PktError> {
    let Some(&[a, bb, c, d]) = b.first_chunk::<4>() else {
        return Ok(None);
    };
    let len = (hex_nibble(a).ok_or(PktError::BadLength)? << 12)
        | (hex_nibble(bb).ok_or(PktError::BadLength)? << 8)
        | (hex_nibble(c).ok_or(PktError::BadLength)? << 4)
        | hex_nibble(d).ok_or(PktError::BadLength)?;
    match len {
        0 => Ok(Some((Pkt::Flush, 4))),
        1 => Ok(Some((Pkt::Delim, 4))),
        2 => Ok(Some((Pkt::ResponseEnd, 4))),
        3 => Err(PktError::BadLength),
        _ if len > MAX_PKT_LEN => Err(PktError::BadLength),
        _ => match b.get(4..len) {
            Some(payload) => Ok(Some((Pkt::Data(payload), len))),
            None => Ok(None),
        },
    }
}

/// Appends one data pkt-line made of `parts` to `out`.
pub fn push_pkt(out: &mut Vec<u8>, parts: &[&[u8]]) -> Result<(), OutOfCapacity> {
    let len = parts.iter().fold(4usize, |a, p| a.saturating_add(p.len()));
    if len > MAX_PKT_LEN {
        return Err(OutOfCapacity {
            requested: len,
            available: MAX_PKT_LEN,
        });
    }
    let digit = |v: usize| b"0123456789abcdef".get(v & 0xF).copied().unwrap_or(b'0');
    let hdr = [
        digit(len >> 12),
        digit(len >> 8),
        digit(len >> 4),
        digit(len),
    ];
    try_extend(out, &hdr)?;
    for p in parts {
        try_extend(out, p)?;
    }
    Ok(())
}

/// Strips one trailing `\n`.
#[inline]
pub fn chomp(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\n").unwrap_or(line)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::arithmetic_side_effects)]
    use super::*;

    #[test]
    fn roundtrip() {
        let mut v = Vec::with_capacity(64);
        push_pkt(&mut v, &[b"command=", b"ls-refs\n"]).unwrap();
        try_extend(&mut v, DELIM_PKT).unwrap();
        try_extend(&mut v, FLUSH_PKT).unwrap();
        let (p, n) = parse_pkt(&v).unwrap().unwrap();
        assert_eq!(p, Pkt::Data(b"command=ls-refs\n"));
        assert_eq!(n, 20);
        assert_eq!(parse_pkt(&v[n..]).unwrap().unwrap(), (Pkt::Delim, 4));
        assert_eq!(parse_pkt(&v[n + 4..]).unwrap().unwrap(), (Pkt::Flush, 4));
        assert_eq!(parse_pkt(&v[..10]).unwrap(), None);
        assert_eq!(parse_pkt(b"zz00"), Err(PktError::BadLength));
        assert_eq!(parse_pkt(b"0003"), Err(PktError::BadLength));
        assert_eq!(chomp(b"abc\n"), b"abc");
    }
}
