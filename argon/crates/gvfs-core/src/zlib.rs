//! Reusable zlib engines plus zero-recompression loose-object splicing.

use core::ops::Range;

use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status};

use crate::pack::{ObjType, loose_header};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZError {
    Corrupt,
    Truncated,
    OutputTooSmall,
    SizeMismatch,
    UnsupportedStream,
}

impl core::fmt::Display for ZError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Corrupt => "corrupt zlib stream",
            Self::Truncated => "truncated zlib stream",
            Self::OutputTooSmall => "zlib output exceeds buffer",
            Self::SizeMismatch => "inflated size does not match header",
            Self::UnsupportedStream => "unsupported zlib stream (preset dictionary)",
        })
    }
}

impl std::error::Error for ZError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Step {
    pub consumed: usize,
    pub produced: usize,
    pub finished: bool,
}

#[derive(Debug)]
pub struct Inflater {
    z: Decompress,
}

impl Default for Inflater {
    fn default() -> Self {
        Self::new()
    }
}

impl Inflater {
    pub fn new() -> Self {
        Self {
            z: Decompress::new(true),
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        self.z.reset(true);
    }

    pub fn step(&mut self, input: &[u8], out: &mut [u8]) -> Result<Step, ZError> {
        let in0 = self.z.total_in();
        let out0 = self.z.total_out();
        let status = self
            .z
            .decompress(input, out, FlushDecompress::None)
            .map_err(|_| ZError::Corrupt)?;
        let consumed =
            usize::try_from(self.z.total_in().saturating_sub(in0)).map_err(|_| ZError::Corrupt)?;
        let produced = usize::try_from(self.z.total_out().saturating_sub(out0))
            .map_err(|_| ZError::Corrupt)?;
        Ok(Step {
            consumed,
            produced,
            finished: status == Status::StreamEnd,
        })
    }

    pub fn inflate(&mut self, input: &[u8], out: &mut [u8]) -> Result<(usize, usize), ZError> {
        self.reset();
        let mut consumed = 0usize;
        let mut produced = 0usize;
        loop {
            let src = input.get(consumed..).unwrap_or(&[]);
            let dst = out.get_mut(produced..).unwrap_or(&mut []);
            let dst_empty = dst.is_empty();
            let s = self.step(src, dst)?;
            consumed = consumed.saturating_add(s.consumed);
            produced = produced.saturating_add(s.produced);
            if s.finished {
                return Ok((consumed, produced));
            }
            if s.consumed == 0 && s.produced == 0 {
                return Err(if consumed >= input.len() {
                    ZError::Truncated
                } else if dst_empty {
                    ZError::OutputTooSmall
                } else {
                    ZError::Corrupt
                });
            }
        }
    }

    pub fn inflate_exact(
        &mut self,
        input: &[u8],
        out: &mut [u8],
        expected: usize,
    ) -> Result<usize, ZError> {
        let dst = out.get_mut(..expected).ok_or(ZError::OutputTooSmall)?;
        let (consumed, produced) = self.inflate(input, dst).map_err(|e| match e {
            ZError::OutputTooSmall => ZError::SizeMismatch,
            other => other,
        })?;
        if produced != expected {
            return Err(ZError::SizeMismatch);
        }
        Ok(consumed)
    }
}

pub fn compress_bound(n: usize) -> usize {
    n.saturating_add(n.wrapping_shr(12))
        .saturating_add(n.wrapping_shr(14))
        .saturating_add(n.wrapping_shr(25))
        .saturating_add(13)
}

#[derive(Debug)]
pub struct Deflater {
    z: Compress,
}

impl Deflater {
    pub fn new(level: u32) -> Self {
        Self {
            z: Compress::new(Compression::new(level.min(9)), true),
        }
    }

    pub fn deflate_parts(&mut self, parts: &[&[u8]], out: &mut [u8]) -> Result<usize, ZError> {
        self.z.reset();
        let mut written = 0usize;
        let last_idx = parts.len().saturating_sub(1);
        for (i, part) in parts.iter().enumerate() {
            let flush = if i == last_idx {
                FlushCompress::Finish
            } else {
                FlushCompress::None
            };
            let mut used = 0usize;
            loop {
                let src = part.get(used..).unwrap_or(&[]);
                let dst = out.get_mut(written..).unwrap_or(&mut []);
                let in0 = self.z.total_in();
                let out0 = self.z.total_out();
                let status = self
                    .z
                    .compress(src, dst, flush)
                    .map_err(|_| ZError::Corrupt)?;
                let c = usize::try_from(self.z.total_in().saturating_sub(in0))
                    .map_err(|_| ZError::Corrupt)?;
                let p = usize::try_from(self.z.total_out().saturating_sub(out0))
                    .map_err(|_| ZError::Corrupt)?;
                used = used.saturating_add(c);
                written = written.saturating_add(p);
                if status == Status::StreamEnd {
                    return Ok(written);
                }
                if used >= part.len() && i != last_idx {
                    break;
                }
                if c == 0 && p == 0 {
                    return Err(ZError::OutputTooSmall);
                }
            }
        }
        if parts.is_empty() {
            return self.deflate_parts(&[&[]], out);
        }
        Err(ZError::Corrupt)
    }

    pub fn deflate(&mut self, input: &[u8], out: &mut [u8]) -> Result<usize, ZError> {
        self.deflate_parts(&[input], out)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryError {
    Header,
    Delta,
    TooLarge,
    Zlib(ZError),
}

pub fn inflate_stored_entry(
    raw: &[u8],
    inflater: &mut Inflater,
    out: &mut [u8],
) -> Result<(ObjType, usize), EntryError> {
    let h = crate::pack::parse_entry_header(raw)
        .ok()
        .flatten()
        .ok_or(EntryError::Header)?;
    if h.kind.is_delta() {
        return Err(EntryError::Delta);
    }
    let size = usize::try_from(h.size).map_err(|_| EntryError::TooLarge)?;
    if size > out.len() {
        return Err(EntryError::TooLarge);
    }
    let z = raw.get(h.header_len..).ok_or(EntryError::Header)?;
    inflater
        .inflate_exact(z, out, size)
        .map_err(EntryError::Zlib)?;
    Ok((h.kind, size))
}

const ADLER_MOD: u32 = 65521;

pub fn adler32(data: &[u8]) -> u32 {
    let mut a: u32 = 1;
    let mut b: u32 = 0;
    for chunk in data.chunks(5552) {
        for &x in chunk {
            a = a.wrapping_add(u32::from(x));
            b = b.wrapping_add(a);
        }
        a = a.wrapping_rem(ADLER_MOD);
        b = b.wrapping_rem(ADLER_MOD);
    }
    b.wrapping_shl(16) | a
}

pub fn adler32_combine(adler1: u32, adler2: u32, len2: u64) -> u32 {
    const BASE: u64 = ADLER_MOD as u64;
    let rem = len2.wrapping_rem(BASE);
    let mut sum1 = u64::from(adler1 & 0xFFFF);
    let mut sum2 = rem.wrapping_mul(sum1).wrapping_rem(BASE);
    sum1 = sum1
        .wrapping_add(u64::from(adler2 & 0xFFFF))
        .wrapping_add(BASE)
        .wrapping_sub(1);
    sum2 = sum2
        .wrapping_add(u64::from(adler1.wrapping_shr(16)))
        .wrapping_add(u64::from(adler2.wrapping_shr(16)))
        .wrapping_add(BASE)
        .wrapping_sub(rem);
    if sum1 >= BASE {
        sum1 = sum1.wrapping_sub(BASE);
    }
    if sum1 >= BASE {
        sum1 = sum1.wrapping_sub(BASE);
    }
    if sum2 >= BASE.wrapping_shl(1) {
        sum2 = sum2.wrapping_sub(BASE.wrapping_shl(1));
    }
    if sum2 >= BASE {
        sum2 = sum2.wrapping_sub(BASE);
    }
    u32::try_from(sum1 | sum2.wrapping_shl(16)).unwrap_or(0)
}

pub const LOOSE_PREFIX_MAX: usize = 2 + 5 + 32;

#[derive(Debug, Clone)]
pub struct LooseSplice {
    prefix: [u8; LOOSE_PREFIX_MAX],
    prefix_len: usize,
    pub body: Range<usize>,
    pub trailer: [u8; 4],
}

impl LooseSplice {
    pub fn plan(kind: ObjType, size: u64, zlib: &[u8]) -> Result<Self, ZError> {
        let [cmf, flg, ..] = *zlib else {
            return Err(ZError::Truncated);
        };
        let header_ok = cmf & 0x0F == 8
            && (u16::from(cmf).wrapping_shl(8) | u16::from(flg)).wrapping_rem(31) == 0;
        if !header_ok {
            return Err(ZError::Corrupt);
        }
        if flg & 0x20 != 0 {
            return Err(ZError::UnsupportedStream);
        }
        let body_end = zlib
            .len()
            .checked_sub(4)
            .filter(|&e| e >= 2)
            .ok_or(ZError::Truncated)?;
        let trailer_bytes: [u8; 4] = zlib
            .get(body_end..)
            .and_then(|t| t.try_into().ok())
            .ok_or(ZError::Truncated)?;
        let content_adler = u32::from_be_bytes(trailer_bytes);

        let mut hdr = [0u8; 32];
        let hdr_len = loose_header(kind, size, &mut hdr);
        let hdr = hdr.get(..hdr_len).ok_or(ZError::Corrupt)?;
        if hdr.is_empty() {
            return Err(ZError::Corrupt);
        }
        let combined = adler32_combine(adler32(hdr), content_adler, size);

        let len16 = u16::try_from(hdr_len).map_err(|_| ZError::Corrupt)?;
        let [l0, l1] = len16.to_le_bytes();
        let [n0, n1] = (!len16).to_le_bytes();
        let mut prefix = [0u8; LOOSE_PREFIX_MAX];
        let mut w = crate::buf::ByteWriter::new(&mut prefix);
        w.put(&[0x78, 0x01, 0x00, l0, l1, n0, n1])
            .map_err(|_| ZError::Corrupt)?;
        w.put(hdr).map_err(|_| ZError::Corrupt)?;
        let prefix_len = w.len();
        Ok(Self {
            prefix,
            prefix_len,
            body: 2..body_end,
            trailer: combined.to_be_bytes(),
        })
    }

    pub fn prefix(&self) -> &[u8] {
        self.prefix.get(..self.prefix_len).unwrap_or(&[])
    }

    pub fn total_len(&self) -> usize {
        self.prefix_len
            .saturating_add(self.body.end.saturating_sub(self.body.start))
            .saturating_add(4)
    }
}
