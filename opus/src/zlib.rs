//! Reusable zlib engines plus the zero-recompression loose-object splice.
//!
//! [`Inflater`] / [`Deflater`] wrap one zlib-rs stream state each, created once per worker
//! and reset between objects so the state is never re-allocated.
//!
//! [`LooseSplice`] turns a whole pack entry (`zlib(content)`) into a loose object
//! (`zlib("<type> <size>\0" ++ content)`) without inflating or deflating: the loose header
//! goes in front as an uncompressed deflate "stored" block, the entry's deflate blocks
//! follow verbatim, and the Adler-32 trailer is recomputed with `adler32_combine`.

use zlib_rs::{Deflate, DeflateFlush, Inflate, InflateFlush, Status};

use crate::pack::ObjectHeader;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZError {
    Corrupt,
    Truncated,
    /// The stream inflates to a different size than its header declared.
    SizeMismatch,
    /// The output buffer is too small.
    NoRoom,
}

impl std::fmt::Display for ZError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ZError::Corrupt => "corrupt zlib stream",
            ZError::Truncated => "truncated zlib stream",
            ZError::SizeMismatch => "zlib stream size does not match its entry header",
            ZError::NoRoom => "zlib output buffer too small",
        })
    }
}

impl From<ZError> for crate::util::Error {
    fn from(e: ZError) -> Self {
        crate::util::Error::new(e.to_string())
    }
}

pub struct Inflater {
    z: Inflate,
}

impl Default for Inflater {
    fn default() -> Self {
        Self::new()
    }
}

impl Inflater {
    pub fn new() -> Inflater {
        Inflater {
            z: Inflate::new(true, 15),
        }
    }

    fn consumed(&self) -> usize {
        crate::util::us(self.z.total_in())
    }

    fn produced(&self) -> usize {
        crate::util::us(self.z.total_out())
    }

    /// Inflates the zlib stream at the start of `input` into `out`, which must be exactly
    /// the inflated size. Returns the number of compressed bytes the stream occupies.
    pub fn inflate_exact(&mut self, input: &[u8], out: &mut [u8]) -> Result<usize, ZError> {
        self.z.reset(true);
        loop {
            let src = input.get(self.consumed()..).unwrap_or(&[]);
            let produced = self.produced();
            let dst = out.get_mut(produced..).unwrap_or(&mut []);
            if dst.is_empty() {
                // All expected bytes are out; the stream must end without producing more.
                let mut extra = [0u8; 1];
                let status = self
                    .z
                    .decompress(src, &mut extra, InflateFlush::NoFlush)
                    .map_err(|_| ZError::Corrupt)?;
                if self.produced() != out.len() {
                    return Err(ZError::SizeMismatch);
                }
                return match status {
                    Status::StreamEnd => Ok(self.consumed()),
                    _ => Err(ZError::Truncated),
                };
            }
            let before_in = self.consumed();
            let status = self
                .z
                .decompress(src, dst, InflateFlush::NoFlush)
                .map_err(|_| ZError::Corrupt)?;
            if status == Status::StreamEnd {
                return if self.produced() == out.len() {
                    Ok(self.consumed())
                } else {
                    Err(ZError::SizeMismatch)
                };
            }
            if self.consumed() == before_in && self.produced() == produced {
                return Err(ZError::Truncated);
            }
        }
    }

    /// Inflates the zlib stream at the start of `input` through the bounded `chunk` buffer,
    /// handing every produced piece to `sink`, and checks that exactly `expected` bytes
    /// come out. Returns the number of compressed bytes the stream occupies.
    pub fn inflate_into(
        &mut self,
        input: &[u8],
        expected: u64,
        chunk: &mut [u8],
        mut sink: impl FnMut(&[u8]),
    ) -> Result<usize, ZError> {
        if chunk.is_empty() {
            return Err(ZError::NoRoom);
        }
        self.z.reset(true);
        loop {
            let src = input.get(self.consumed()..).unwrap_or(&[]);
            let before_in = self.consumed();
            let before_out = self.produced();
            let status = self
                .z
                .decompress(src, chunk, InflateFlush::NoFlush)
                .map_err(|_| ZError::Corrupt)?;
            let made = self.produced().saturating_sub(before_out);
            if crate::util::u64_of(self.produced()) > expected {
                return Err(ZError::SizeMismatch);
            }
            sink(chunk.get(..made).unwrap_or(&[]));
            if status == Status::StreamEnd {
                return if crate::util::u64_of(self.produced()) == expected {
                    Ok(self.consumed())
                } else {
                    Err(ZError::SizeMismatch)
                };
            }
            if self.consumed() == before_in && made == 0 {
                return Err(ZError::Truncated);
            }
        }
    }
}

pub struct Deflater {
    z: Deflate,
}

impl Deflater {
    pub fn new(level: i32) -> Deflater {
        Deflater {
            z: Deflate::new(level.clamp(0, 9), true, 15),
        }
    }

    /// Deflates the concatenation of `parts` as one zlib stream into `out` (size it with
    /// [`compress_bound`]). Returns the stream length.
    pub fn deflate_parts(&mut self, parts: &[&[u8]], out: &mut [u8]) -> Result<usize, ZError> {
        self.z.reset();
        let last = parts.len().saturating_sub(1);
        for (i, part) in parts.iter().enumerate() {
            let flush = if i == last {
                DeflateFlush::Finish
            } else {
                DeflateFlush::NoFlush
            };
            let mut used = 0usize;
            loop {
                let src = part.get(used..).unwrap_or(&[]);
                let written = crate::util::us(self.z.total_out());
                let dst = out.get_mut(written..).unwrap_or(&mut []);
                let in0 = self.z.total_in();
                let out0 = self.z.total_out();
                let status = self
                    .z
                    .compress(src, dst, flush)
                    .map_err(|_| ZError::Corrupt)?;
                used = used.saturating_add(crate::util::us(self.z.total_in().saturating_sub(in0)));
                if status == Status::StreamEnd {
                    return Ok(crate::util::us(self.z.total_out()));
                }
                if i != last && used >= part.len() {
                    break;
                }
                if self.z.total_in() == in0 && self.z.total_out() == out0 {
                    return Err(ZError::NoRoom);
                }
            }
        }
        if parts.is_empty() {
            return self.deflate_parts(&[&[]], out);
        }
        Err(ZError::Corrupt)
    }
}

/// Upper bound of a zlib stream for `n` input bytes.
pub fn compress_bound(n: usize) -> usize {
    zlib_rs::compress_bound(n)
}

pub fn adler32(data: &[u8]) -> u32 {
    zlib_rs::adler32::adler32(1, data)
}

/// zlib header (`0x78 0x01`) + stored-block header (5 bytes) + `"<type> <size>\0"`.
pub const LOOSE_PREFIX_MAX: usize = 2 + 5 + 32;

/// A loose object built from a whole pack entry's zlib stream:
/// `prefix() ++ zlib[body_start..body_end] ++ trailer`.
#[derive(Clone, Copy)]
pub struct LooseSplice {
    prefix: [u8; LOOSE_PREFIX_MAX],
    prefix_len: usize,
    pub body_start: usize,
    pub body_end: usize,
    pub trailer: [u8; 4],
}

impl LooseSplice {
    /// `zlib` is the complete zlib stream of a whole (non-delta) entry of `kind` / `size`.
    pub fn plan(kind: u8, size: u64, zlib: &[u8]) -> Result<LooseSplice, ZError> {
        let [cmf, flg, ..] = *zlib else {
            return Err(ZError::Truncated);
        };
        let header_ok = cmf & 0x0F == 8
            && (u16::from(cmf).wrapping_shl(8) | u16::from(flg)).wrapping_rem(31) == 0;
        if !header_ok || flg & 0x20 != 0 {
            return Err(ZError::Corrupt);
        }
        let body_end = zlib
            .len()
            .checked_sub(4)
            .filter(|&e| e >= 2)
            .ok_or(ZError::Truncated)?;
        let tail: [u8; 4] = zlib
            .get(body_end..)
            .and_then(|t| t.try_into().ok())
            .ok_or(ZError::Truncated)?;
        let content_adler = u32::from_be_bytes(tail);

        let hdr = ObjectHeader::new(kind, size);
        let hdr = hdr.as_slice();
        let combined = zlib_rs::adler32::adler32_combine(adler32(hdr), content_adler, size);
        let len16 = u16::try_from(hdr.len()).map_err(|_| ZError::Corrupt)?;
        let [l0, l1] = len16.to_le_bytes();
        let [n0, n1] = (!len16).to_le_bytes();

        let mut prefix = [0u8; LOOSE_PREFIX_MAX];
        let mut n = 0usize;
        // zlib header, then BFINAL=0 / BTYPE=00 (stored) at a byte boundary, LEN, NLEN, bytes.
        for &b in [0x78u8, 0x01, 0x00, l0, l1, n0, n1]
            .iter()
            .chain(hdr.iter())
        {
            if let Some(slot) = prefix.get_mut(n) {
                *slot = b;
                n = n.wrapping_add(1);
            }
        }
        Ok(LooseSplice {
            prefix,
            prefix_len: n,
            body_start: 2,
            body_end,
            trailer: combined.to_be_bytes(),
        })
    }

    pub fn prefix(&self) -> &[u8] {
        self.prefix.get(..self.prefix_len).unwrap_or(&[])
    }

    pub fn total_len(&self) -> usize {
        self.prefix_len
            .saturating_add(self.body_end.saturating_sub(self.body_start))
            .saturating_add(4)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zlib_ref(data: &[u8], level: i32) -> Vec<u8> {
        let mut d = Deflater::new(level);
        let mut out = vec![0u8; compress_bound(data.len())];
        let n = d.deflate_parts(&[data], &mut out).unwrap();
        out.truncate(n);
        out
    }

    #[test]
    fn inflate_exact_and_stream() {
        let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let comp = zlib_ref(&data, 6);
        let mut inf = Inflater::new();
        let mut out = vec![0u8; data.len()];
        assert_eq!(inf.inflate_exact(&comp, &mut out).unwrap(), comp.len());
        assert_eq!(out, data);
        // Trailing bytes after the stream are not consumed.
        let mut padded = comp.clone();
        padded.extend_from_slice(b"NEXT");
        assert_eq!(inf.inflate_exact(&padded, &mut out).unwrap(), comp.len());
        // Wrong sizes are detected both ways; truncation is detected.
        assert_eq!(
            inf.inflate_exact(&comp, &mut out[..data.len() - 1]),
            Err(ZError::SizeMismatch)
        );
        let mut bigger = vec![0u8; data.len() + 1];
        assert_eq!(
            inf.inflate_exact(&comp, &mut bigger),
            Err(ZError::SizeMismatch)
        );
        assert_eq!(
            inf.inflate_exact(&comp[..comp.len() - 3], &mut out),
            Err(ZError::Truncated)
        );
        // Streaming through a small chunk.
        let mut got = Vec::new();
        let mut chunk = [0u8; 777];
        let used = inf
            .inflate_into(&padded, data.len() as u64, &mut chunk, |p| {
                got.extend_from_slice(p)
            })
            .unwrap();
        assert_eq!((used, got.as_slice()), (comp.len(), data.as_slice()));
        assert_eq!(
            inf.inflate_into(&comp, data.len() as u64 - 1, &mut chunk, |_| {}),
            Err(ZError::SizeMismatch)
        );
        // Empty object.
        let e = zlib_ref(b"", 1);
        assert_eq!(inf.inflate_exact(&e, &mut []).unwrap(), e.len());
    }

    #[test]
    fn deflate_parts_matches_whole() {
        let a = b"blob 11\0";
        let b = b"hello world";
        let mut d = Deflater::new(1);
        let mut out = vec![0u8; compress_bound(64)];
        let n = d.deflate_parts(&[a, b], &mut out).unwrap();
        let mut inf = Inflater::new();
        let mut back = vec![0u8; a.len() + b.len()];
        inf.inflate_exact(&out[..n], &mut back).unwrap();
        assert_eq!(back, [&a[..], &b[..]].concat());
    }

    #[test]
    fn loose_splice_produces_valid_loose_object() {
        let payloads: [&[u8]; 4] = [b"", b"x", b"hello world\n", &[7u8; 70_000]];
        for content in payloads {
            for level in [0, 1, 6, 9] {
                let z = zlib_ref(content, level);
                let plan =
                    LooseSplice::plan(crate::pack::OBJ_BLOB, content.len() as u64, &z).unwrap();
                let mut spliced = plan.prefix().to_vec();
                spliced.extend_from_slice(&z[plan.body_start..plan.body_end]);
                spliced.extend_from_slice(&plan.trailer);
                assert_eq!(spliced.len(), plan.total_len());
                let mut expected = format!("blob {}\0", content.len()).into_bytes();
                expected.extend_from_slice(content);
                let mut out = vec![0u8; expected.len()];
                Inflater::new().inflate_exact(&spliced, &mut out).unwrap();
                assert_eq!(out, expected, "level {level} len {}", content.len());
            }
        }
        assert_eq!(
            LooseSplice::plan(crate::pack::OBJ_BLOB, 0, b"\x78").err(),
            Some(ZError::Truncated)
        );
    }
}
