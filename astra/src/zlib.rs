//! Reusable zlib engines plus the zero-recompression loose-object splice.
//!
//! [`Inflater`] / [`Deflater`] wrap a single `flate2` state each, created once at startup
//! and `reset()` between objects so the backend never re-allocates its internal state.
//!
//! [`LooseSplice`] turns a stored pack entry (`zlib(content)`) into a loose object
//! (`zlib("<type> <size>\0" ++ content)`) *without inflating or deflating*: the loose
//! header is emitted as an uncompressed deflate "stored" block, the pack entry's raw
//! deflate blocks follow verbatim, and the Adler-32 trailer is recomputed with
//! `adler32_combine` from the two checksums.

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

/// Progress made by one [`Inflater::step`] call.
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

    /// Feeds `input`, producing into `out`. Never allocates.
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

    /// Inflates one complete zlib stream at the front of `input` into `out`.
    /// Returns (compressed bytes consumed, bytes produced).
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

    /// Inflates and checks that exactly `expected` bytes come out.
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

/// zlib's `compressBound()`.
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

    /// Deflates the concatenation of `parts` into `out` as one zlib stream.
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

/// Why a stored pack entry could not be inflated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryError {
    Header,
    Delta,
    TooLarge,
    Zlib(ZError),
}

impl core::fmt::Display for EntryError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Header => f.write_str("truncated or invalid entry header"),
            Self::Delta => f.write_str("stored entries are never deltas"),
            Self::TooLarge => f.write_str("object larger than the inflate buffer"),
            Self::Zlib(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for EntryError {}

/// Inflates a stored (never deltified) pack entry `raw` into `out`.
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
    // 5552 is zlib's NMAX: the largest block for which `b` cannot overflow 32 bits.
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

/// Port of zlib's `adler32_combine()`: the checksum of `A ++ B` from `adler(A)`,
/// `adler(B)` and `len(B)`.
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

/// zlib header (`CMF=0x78`, `FLG=0x01`) + stored-block header (5 bytes) + loose header.
pub const LOOSE_PREFIX_MAX: usize = 2 + 5 + 32;

/// Plan for emitting a pack entry's zlib stream as a loose object:
/// `prefix() ++ zlib[body] ++ trailer`.
#[derive(Debug, Clone)]
pub struct LooseSplice {
    prefix: [u8; LOOSE_PREFIX_MAX],
    prefix_len: usize,
    /// Range of the raw deflate blocks inside the entry's zlib stream.
    pub body: Range<usize>,
    pub trailer: [u8; 4],
}

impl LooseSplice {
    /// `zlib` is the complete zlib stream of a non-delta pack entry of `kind`/`size`.
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
        // BFINAL=0, BTYPE=00 (stored) at a byte boundary, then LEN/NLEN and the bytes.
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

    /// Total size of the loose object stream.
    pub fn total_len(&self) -> usize {
        self.prefix_len
            .saturating_add(self.body.end.saturating_sub(self.body.start))
            .saturating_add(4)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn zlib_ref(data: &[u8], level: u32) -> Vec<u8> {
        use std::io::Write;
        let mut e = flate2::write::ZlibEncoder::new(Vec::new(), Compression::new(level));
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }

    #[test]
    fn adler_matches_reference_and_combines() {
        let a = b"blob 12\0";
        let b = b"hello world\n";
        let whole: Vec<u8> = [&a[..], &b[..]].concat();
        let big: Vec<u8> = (0..200_000u32).map(|i| (i * 7 + 3) as u8).collect();
        for data in [&whole[..], &big[..], &b""[..]] {
            let z = zlib_ref(data, 6);
            let tail: [u8; 4] = z[z.len() - 4..].try_into().unwrap();
            assert_eq!(adler32(data), u32::from_be_bytes(tail));
        }
        assert_eq!(
            adler32_combine(adler32(a), adler32(b), b.len() as u64),
            adler32(&whole)
        );
        assert_eq!(adler32_combine(adler32(a), adler32(b""), 0), adler32(a));
        assert_eq!(
            adler32_combine(adler32(a), adler32(&big), big.len() as u64),
            adler32(&[&a[..], &big[..]].concat())
        );
    }

    #[test]
    fn inflate_deflate_roundtrip_exact() {
        let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let mut d = Deflater::new(6);
        let mut comp = vec![0u8; compress_bound(data.len())];
        let n = d
            .deflate_parts(&[&data[..10], &data[10..]], &mut comp)
            .unwrap();
        let mut inf = Inflater::new();
        let mut out = vec![0u8; data.len()];
        let used = inf.inflate_exact(&comp[..n], &mut out, data.len()).unwrap();
        assert_eq!(used, n);
        assert_eq!(out, data);
        // Trailing bytes after the stream are not consumed.
        let mut padded = comp[..n].to_vec();
        padded.extend_from_slice(b"NEXT");
        assert_eq!(inf.inflate_exact(&padded, &mut out, data.len()).unwrap(), n);
        // Wrong expected size is detected both ways.
        assert_eq!(
            inf.inflate_exact(&comp[..n], &mut out, data.len() - 1),
            Err(ZError::SizeMismatch)
        );
        let mut bigger = vec![0u8; data.len() + 1];
        assert_eq!(
            inf.inflate_exact(&comp[..n], &mut bigger, data.len() + 1),
            Err(ZError::SizeMismatch)
        );
        assert_eq!(
            inf.inflate_exact(&comp[..n - 3], &mut out, data.len()),
            Err(ZError::Truncated)
        );
        // Empty input.
        let n = d.deflate(b"", &mut comp).unwrap();
        assert_eq!(inf.inflate_exact(&comp[..n], &mut out, 0).unwrap(), n);
    }

    #[test]
    fn loose_splice_produces_valid_loose_object() {
        let payloads: [&[u8]; 4] = [b"", b"x", b"hello world\n", &[7u8; 70_000]];
        for content in payloads {
            for level in [0, 1, 6, 9] {
                let z = zlib_ref(content, level);
                let plan = LooseSplice::plan(ObjType::Blob, content.len() as u64, &z).unwrap();
                let mut spliced = plan.prefix().to_vec();
                spliced.extend_from_slice(&z[plan.body.clone()]);
                spliced.extend_from_slice(&plan.trailer);
                assert_eq!(spliced.len(), plan.total_len());
                let mut dec = flate2::read::ZlibDecoder::new(&spliced[..]);
                let mut out = Vec::new();
                dec.read_to_end(&mut out).unwrap();
                let mut expected = format!("blob {}\0", content.len()).into_bytes();
                expected.extend_from_slice(content);
                assert_eq!(out, expected, "level {level} len {}", content.len());
            }
        }
        assert_eq!(
            LooseSplice::plan(ObjType::Blob, 0, b"\x78").unwrap_err(),
            ZError::Truncated
        );
        assert_eq!(
            LooseSplice::plan(ObjType::Blob, 0, b"\x00\x00\x00\x00\x00\x00").unwrap_err(),
            ZError::Corrupt
        );
    }
}
