//! Git `pkt-line` framing (protocol v2 requests/responses and side-band demux).

use crate::buf::{ByteWriter, Overflow};
use crate::oid::HEX_LUT_ANY;

pub const MAX_PKT_LEN: usize = 65520;
pub const MAX_PKT_PAYLOAD: usize = MAX_PKT_LEN - 4;

pub const FLUSH_PKT: &[u8; 4] = b"0000";
pub const DELIM_PKT: &[u8; 4] = b"0001";
pub const RESPONSE_END_PKT: &[u8; 4] = b"0002";

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
    TooLong,
}

impl core::fmt::Display for PktError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::BadLength => "invalid pkt-line length",
            Self::TooLong => "pkt-line exceeds 65520 bytes",
        })
    }
}

impl std::error::Error for PktError {}

pub fn parse_pkt(buf: &[u8]) -> Result<Option<(Pkt<'_>, usize)>, PktError> {
    let Some(hdr) = buf.get(..4) else {
        return Ok(None);
    };
    let mut len: usize = 0;
    for &c in hdr {
        let v = HEX_LUT_ANY.get(usize::from(c)).copied().unwrap_or(0xFF);
        if v > 15 {
            return Err(PktError::BadLength);
        }
        len = len.wrapping_shl(4) | usize::from(v);
    }
    match len {
        0 => Ok(Some((Pkt::Flush, 4))),
        1 => Ok(Some((Pkt::Delim, 4))),
        2 => Ok(Some((Pkt::ResponseEnd, 4))),
        3 => Err(PktError::BadLength),
        n if n > MAX_PKT_LEN => Err(PktError::TooLong),
        n => match buf.get(4..n) {
            Some(payload) => Ok(Some((Pkt::Data(payload), n))),
            None => Ok(None),
        },
    }
}

pub fn write_pkt_parts(w: &mut ByteWriter<'_>, parts: &[&[u8]]) -> Result<(), Overflow> {
    let payload: usize = parts
        .iter()
        .fold(0usize, |acc, p| acc.saturating_add(p.len()));
    if payload > MAX_PKT_PAYLOAD {
        return Err(Overflow);
    }
    let total = payload.checked_add(4).ok_or(Overflow)?;
    let mut hdr = [0u8; 4];
    let digits = b"0123456789abcdef";
    for (i, d) in hdr.iter_mut().enumerate() {
        let shift = u32::try_from(12usize.saturating_sub(i.saturating_mul(4))).unwrap_or(0);
        let nib = total.wrapping_shr(shift) & 0xF;
        *d = digits.get(nib).copied().unwrap_or(b'0');
    }
    w.put(&hdr)?;
    for p in parts {
        w.put(p)?;
    }
    Ok(())
}

pub fn write_pkt(w: &mut ByteWriter<'_>, payload: &[u8]) -> Result<(), Overflow> {
    write_pkt_parts(w, &[payload])
}

pub fn chomp(line: &[u8]) -> &[u8] {
    match line.split_last() {
        Some((b'\n', rest)) => rest,
        _ => line,
    }
}
