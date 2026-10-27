//! Reusable zlib engines plus the zero-recompression loose-object splice.
//!
//! [`Inflater`] / [`Deflater`] wrap one `flate2` (zlib-rs) state each, created once and
//! `reset()` between objects so the backend never re-allocates. [`loose_splice`] turns a
//! stored non-delta pack entry (`zlib(content)`) into a loose object
//! (`zlib("<type> <size>\0" ++ content)`) without inflating or deflating: the loose header
//! goes out as an uncompressed deflate "stored" block, the entry's own deflate blocks follow
//! verbatim and the Adler-32 trailer is recomputed with `adler32_combine`.

use core::fmt;

use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status};

use crate::pack::{ObjType, loose_header};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZError {
    Corrupt,
    Truncated,
    OutputTooSmall,
    SizeMismatch,
}

impl fmt::Display for ZError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Corrupt => "corrupt zlib stream",
            Self::Truncated => "truncated zlib stream",
            Self::OutputTooSmall => "zlib output exceeds buffer",
            Self::SizeMismatch => "inflated size does not match header",
        })
    }
}

impl std::error::Error for ZError {}

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

    /// Inflates one complete zlib stream at the front of `input` into `out`.
    /// Returns (compressed bytes consumed, bytes produced). Never allocates.
    pub fn inflate(&mut self, input: &[u8], out: &mut [u8]) -> Result<(usize, usize), ZError> {
        self.z.reset(true);
        let mut consumed = 0usize;
        let mut produced = 0usize;
        loop {
            let src = input.get(consumed..).unwrap_or(&[]);
            let dst = out.get_mut(produced..).unwrap_or(&mut []);
            let dst_empty = dst.is_empty();
            let in0 = self.z.total_in();
            let out0 = self.z.total_out();
            let status = self
                .z
                .decompress(src, dst, FlushDecompress::None)
                .map_err(|_| ZError::Corrupt)?;
            let c = usize::try_from(self.z.total_in().saturating_sub(in0))
                .map_err(|_| ZError::Corrupt)?;
            let p = usize::try_from(self.z.total_out().saturating_sub(out0))
                .map_err(|_| ZError::Corrupt)?;
            consumed = consumed.saturating_add(c);
            produced = produced.saturating_add(p);
            if status == Status::StreamEnd {
                return Ok((consumed, produced));
            }
            if c == 0 && p == 0 {
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

    /// Finds the end of the zlib stream at the front of `input` and the number of bytes it
    /// inflates to, discarding the output through `scratch` (any size ≥ 1 byte works; a
    /// cache-resident 64 KiB buffer is ideal). Returns (consumed, produced).
    pub fn measure(&mut self, input: &[u8], scratch: &mut [u8]) -> Result<(usize, usize), ZError> {
        self.z.reset(true);
        let mut consumed = 0usize;
        let mut produced = 0usize;
        loop {
            let src = input.get(consumed..).unwrap_or(&[]);
            let in0 = self.z.total_in();
            let out0 = self.z.total_out();
            let status = self
                .z
                .decompress(src, scratch, FlushDecompress::None)
                .map_err(|_| ZError::Corrupt)?;
            let c = usize::try_from(self.z.total_in().saturating_sub(in0))
                .map_err(|_| ZError::Corrupt)?;
            let p = usize::try_from(self.z.total_out().saturating_sub(out0))
                .map_err(|_| ZError::Corrupt)?;
            consumed = consumed.saturating_add(c);
            produced = produced.saturating_add(p);
            if status == Status::StreamEnd {
                return Ok((consumed, produced));
            }
            if c == 0 && p == 0 {
                return Err(if consumed >= input.len() {
                    ZError::Truncated
                } else {
                    ZError::Corrupt
                });
            }
        }
    }

    /// Inflates only the first `limit` bytes of output (e.g. a delta's size varints).
    pub fn inflate_prefix(&mut self, input: &[u8], out: &mut [u8]) -> Result<usize, ZError> {
        self.z.reset(true);
        let mut consumed = 0usize;
        let mut produced = 0usize;
        loop {
            let src = input.get(consumed..).unwrap_or(&[]);
            let dst = out.get_mut(produced..).unwrap_or(&mut []);
            if dst.is_empty() {
                return Ok(produced);
            }
            let in0 = self.z.total_in();
            let out0 = self.z.total_out();
            let status = self
                .z
                .decompress(src, dst, FlushDecompress::None)
                .map_err(|_| ZError::Corrupt)?;
            let c = usize::try_from(self.z.total_in().saturating_sub(in0))
                .map_err(|_| ZError::Corrupt)?;
            let p = usize::try_from(self.z.total_out().saturating_sub(out0))
                .map_err(|_| ZError::Corrupt)?;
            consumed = consumed.saturating_add(c);
            produced = produced.saturating_add(p);
            if status == Status::StreamEnd {
                return Ok(produced);
            }
            if c == 0 && p == 0 {
                return Err(if consumed >= input.len() {
                    ZError::Truncated
                } else {
                    ZError::Corrupt
                });
            }
        }
    }
}

/// zlib's `compressBound()`.
pub fn compress_bound(n: usize) -> usize {
    n.saturating_add(n >> 12)
        .saturating_add(n >> 14)
        .saturating_add(n >> 25)
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

    /// Deflates the concatenation of `parts` into `out` as one zlib stream; returns the
    /// compressed length. Never allocates.
    pub fn deflate_parts(&mut self, parts: &[&[u8]], out: &mut [u8]) -> Result<usize, ZError> {
        self.z.reset();
        let mut written = 0usize;
        let n_parts = parts.len();
        let mut i = 0usize;
        loop {
            let (part, is_last): (&[u8], bool) = match parts.get(i) {
                Some(p) => (p, i.saturating_add(1) >= n_parts),
                None => (&[], true),
            };
            let flush = if is_last {
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
                if used >= part.len() && !is_last {
                    break;
                }
                if c == 0 && p == 0 {
                    return Err(ZError::OutputTooSmall);
                }
            }
            i = i.saturating_add(1);
        }
    }

    pub fn deflate(&mut self, input: &[u8], out: &mut [u8]) -> Result<usize, ZError> {
        self.deflate_parts(&[input], out)
    }

    /// Starts a zlib stream for incremental use with [`Deflater::step`].
    pub fn begin(&mut self) {
        self.z.reset();
    }

    /// One incremental step: consumes what fits of `input`, produces what fits in `out`.
    /// `finish` once `input` is the whole remainder of the stream. Returns `(consumed,
    /// produced, stream ended)`; `(0, 0, false)` means `out` is full.
    pub fn step(
        &mut self,
        input: &[u8],
        out: &mut [u8],
        finish: bool,
    ) -> Result<(usize, usize, bool), ZError> {
        let flush = if finish {
            FlushCompress::Finish
        } else {
            FlushCompress::None
        };
        let in0 = self.z.total_in();
        let out0 = self.z.total_out();
        let status = self
            .z
            .compress(input, out, flush)
            .map_err(|_| ZError::Corrupt)?;
        let c =
            usize::try_from(self.z.total_in().saturating_sub(in0)).map_err(|_| ZError::Corrupt)?;
        let p = usize::try_from(self.z.total_out().saturating_sub(out0))
            .map_err(|_| ZError::Corrupt)?;
        Ok((c, p, status == Status::StreamEnd))
    }
}

const ADLER_BASE: u32 = 65521;

pub fn adler32(data: &[u8]) -> u32 {
    let mut a: u32 = 1;
    let mut b: u32 = 0;
    for chunk in data.chunks(5552) {
        for &c in chunk {
            a = a.wrapping_add(u32::from(c));
            b = b.wrapping_add(a);
        }
        a %= ADLER_BASE;
        b %= ADLER_BASE;
    }
    (b << 16) | a
}

/// zlib's `adler32_combine`: the checksum of `A ++ B` from `adler(A)`, `adler(B)`, `len(B)`.
pub fn adler32_combine(adler1: u32, adler2: u32, len2: u64) -> u32 {
    let rem = len2.checked_rem(u64::from(ADLER_BASE)).unwrap_or(0) as u32;
    let mut sum1 = adler1 & 0xFFFF;
    let mut sum2 = rem.wrapping_mul(sum1) % ADLER_BASE;
    sum1 = sum1
        .wrapping_add(adler2 & 0xFFFF)
        .wrapping_add(ADLER_BASE)
        .wrapping_sub(1);
    sum2 = sum2
        .wrapping_add((adler1 >> 16) & 0xFFFF)
        .wrapping_add((adler2 >> 16) & 0xFFFF)
        .wrapping_add(ADLER_BASE)
        .wrapping_sub(rem);
    if sum1 >= ADLER_BASE {
        sum1 = sum1.wrapping_sub(ADLER_BASE);
    }
    if sum1 >= ADLER_BASE {
        sum1 = sum1.wrapping_sub(ADLER_BASE);
    }
    if sum2 >= ADLER_BASE << 1 {
        sum2 = sum2.wrapping_sub(ADLER_BASE << 1);
    }
    if sum2 >= ADLER_BASE {
        sum2 = sum2.wrapping_sub(ADLER_BASE);
    }
    sum1 | (sum2 << 16)
}

/// The pieces of a spliced loose object: `prefix` (zlib header + stored block + loose
/// header), then the entry's deflate blocks `zlib[2..len-4]`, then `trailer`.
#[derive(Debug, Clone, Copy)]
pub struct LooseSplice {
    pub prefix: [u8; 40],
    pub prefix_len: usize,
    pub trailer: [u8; 4],
}

/// Builds the splice for a non-delta entry whose zlib stream is `zlib` and inflates to
/// `size` bytes. Returns `None` for streams we cannot splice (preset dictionary, too short).
pub fn loose_splice(kind: ObjType, size: u64, zlib: &[u8]) -> Option<LooseSplice> {
    let (&[cmf, flg], _) = zlib.split_first_chunk::<2>()?;
    if cmf & 0x0F != 8 || flg & 0x20 != 0 || zlib.len() < 6 {
        return None;
    }
    let adler_tail: &[u8; 4] = zlib.last_chunk::<4>()?;
    let content_adler = u32::from_be_bytes(*adler_tail);
    let mut hdr = [0u8; 32];
    let hdr_len = loose_header(kind, size, &mut hdr);
    let hdr_bytes = hdr.get(..hdr_len)?;
    let len16 = u16::try_from(hdr_len).ok()?;
    let mut prefix = [0u8; 40];
    // zlib header (deflate, 32 KiB window, default level) then a non-final stored block.
    let head: [u8; 7] = [
        0x78,
        0x01,
        0x00,
        len16 as u8,
        (len16 >> 8) as u8,
        !len16 as u8,
        (!len16 >> 8) as u8,
    ];
    for (d, s) in prefix.iter_mut().zip(head.iter().chain(hdr_bytes)) {
        *d = *s;
    }
    let prefix_len = head.len().checked_add(hdr_len)?;
    let combined = adler32_combine(adler32(hdr_bytes), content_adler, size);
    Some(LooseSplice {
        prefix,
        prefix_len,
        trailer: combined.to_be_bytes(),
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::arithmetic_side_effects)]
    use super::*;

    #[test]
    fn inflate_deflate_measure_roundtrip() {
        let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let mut d = Deflater::new(6);
        let mut comp = vec![0u8; compress_bound(data.len())];
        let n = d.deflate(&data, &mut comp).unwrap();
        let mut i = Inflater::new();
        let mut out = vec![0u8; data.len()];
        let (consumed, produced) = i.inflate(&comp[..n + 10], &mut out).unwrap();
        assert_eq!((consumed, produced), (n, data.len()));
        assert_eq!(out, data);
        let mut scratch = [0u8; 4096];
        assert_eq!(
            i.measure(&comp[..n + 3], &mut scratch).unwrap(),
            (n, data.len())
        );
        let mut prefix = [0u8; 16];
        assert_eq!(i.inflate_prefix(&comp[..n], &mut prefix).unwrap(), 16);
        assert_eq!(&prefix[..], &data[..16]);
        assert_eq!(i.inflate(&comp[..n / 2], &mut out), Err(ZError::Truncated));
        let mut small = vec![0u8; 10];
        assert_eq!(
            i.inflate(&comp[..n], &mut small),
            Err(ZError::OutputTooSmall)
        );
        assert_eq!(
            i.inflate_exact(&comp[..n], &mut out, data.len() - 1),
            Err(ZError::SizeMismatch)
        );
        let parts = d
            .deflate_parts(&[&data[..10], &data[10..]], &mut comp)
            .unwrap();
        let (_, p) = i.inflate(&comp[..parts], &mut out).unwrap();
        assert_eq!(p, data.len());
    }

    #[test]
    fn adler_combine_matches_direct() {
        let a = b"blob 11\0";
        let b = b"hello world";
        let mut all = a.to_vec();
        all.extend_from_slice(b);
        assert_eq!(
            adler32_combine(adler32(a), adler32(b), b.len() as u64),
            adler32(&all)
        );
        let big: Vec<u8> = (0..70_000u32).map(|i| (i * 7 % 256) as u8).collect();
        let mut all2 = a.to_vec();
        all2.extend_from_slice(&big);
        assert_eq!(
            adler32_combine(adler32(a), adler32(&big), big.len() as u64),
            adler32(&all2)
        );
    }

    #[test]
    fn loose_splice_inflates_to_a_loose_object() {
        let content = b"hello world";
        let mut d = Deflater::new(6);
        let mut comp = vec![0u8; 64];
        let n = d.deflate(content, &mut comp).unwrap();
        let s = loose_splice(ObjType::Blob, content.len() as u64, &comp[..n]).unwrap();
        let mut spliced = s.prefix[..s.prefix_len].to_vec();
        spliced.extend_from_slice(&comp[2..n - 4]);
        spliced.extend_from_slice(&s.trailer);
        let mut i = Inflater::new();
        let mut out = vec![0u8; 64];
        let (c, p) = i.inflate(&spliced, &mut out).unwrap();
        assert_eq!(c, spliced.len());
        assert_eq!(&out[..p], b"blob 11\0hello world");
    }
}
