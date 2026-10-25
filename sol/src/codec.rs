use std::fmt;

use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status};
use sha1::{Digest, Sha1};

use crate::arena::Writer;
use crate::{Error, Result};

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Oid(pub [u8; 20]);

impl Oid {
    pub const ZERO: Self = Self([0; 20]);

    pub fn parse(hex: &[u8]) -> Result<Self> {
        let mut raw = [0; 20];
        decode_hex(hex, &mut raw)?;
        Ok(Self(raw))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(Self(
            bytes.try_into().map_err(|_| Error::Invalid("OID length"))?,
        ))
    }

    pub fn hex(self) -> [u8; 40] {
        let mut out = [0; 40];
        encode_hex(&self.0, &mut out);
        out
    }
}

impl fmt::Display for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

pub fn decode_hex(hex: &[u8], out: &mut [u8]) -> Result<()> {
    if hex.len()
        != out
            .len()
            .checked_mul(2)
            .ok_or(Error::Invalid("hex length"))?
    {
        return Err(Error::Invalid("hex length"));
    }
    // Fixed-size byte operations let LLVM vectorize the hash/pointer decoders.
    let mut invalid = 0u8;
    for (dst, pair) in out.iter_mut().zip(hex.chunks_exact(2)) {
        let [a, b] = pair else {
            return Err(Error::Invalid("hex pair"));
        };
        let decode = |c: u8| {
            let digit = c.wrapping_sub(b'0');
            let letter = (c | 0x20).wrapping_sub(b'a');
            let valid_digit = digit < 10;
            invalid |= u8::from(!valid_digit && letter >= 6);
            if valid_digit {
                digit
            } else {
                letter.wrapping_add(10)
            }
        };
        let mut decode = decode;
        *dst = decode(*a).wrapping_shl(4) | decode(*b);
    }
    if invalid != 0 {
        Err(Error::Invalid("hex digit"))
    } else {
        Ok(())
    }
}

pub fn encode_hex(raw: &[u8], out: &mut [u8]) {
    for (byte, pair) in raw.iter().zip(out.chunks_exact_mut(2)) {
        let nibble = |v: u8| {
            if v < 10 {
                b'0'.wrapping_add(v)
            } else {
                b'a'.wrapping_add(v.wrapping_sub(10))
            }
        };
        if let [a, b] = pair {
            *a = nibble(byte >> 4);
            *b = nibble(byte & 15);
        }
    }
}

pub fn number(bytes: &[u8]) -> Result<u64> {
    if bytes.is_empty() {
        return Err(Error::Invalid("empty number"));
    }
    bytes.iter().try_fold(0u64, |value, byte| {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 {
            return Err(Error::Invalid("decimal digit"));
        }
        value
            .checked_mul(10)
            .and_then(|v| v.checked_add(u64::from(digit)))
            .ok_or(Error::Invalid("number overflow"))
    })
}

pub fn take(bytes: &[u8], offset: usize, len: usize) -> Result<&[u8]> {
    let end = offset
        .checked_add(len)
        .ok_or(Error::Invalid("slice overflow"))?;
    bytes
        .get(offset..end)
        .ok_or(Error::Invalid("truncated input"))
}

pub fn be32(bytes: &[u8], offset: usize) -> Result<u32> {
    Ok(u32::from_be_bytes(
        take(bytes, offset, 4)?
            .try_into()
            .map_err(|_| Error::Invalid("u32"))?,
    ))
}

pub fn be64(bytes: &[u8], offset: usize) -> Result<u64> {
    Ok(u64::from_be_bytes(
        take(bytes, offset, 8)?
            .try_into()
            .map_err(|_| Error::Invalid("u64"))?,
    ))
}

pub fn checksum(bytes: &[u8]) -> [u8; 20] {
    Sha1::digest(bytes).into()
}

pub fn verify_checksum(bytes: &[u8]) -> Result<&[u8]> {
    let end = bytes
        .len()
        .checked_sub(20)
        .ok_or(Error::Invalid("checksum length"))?;
    let body = take(bytes, 0, end)?;
    if checksum(body) != take(bytes, end, 20)? {
        return Err(Error::Invalid("SHA-1 checksum"));
    }
    Ok(body)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    Commit = 1,
    Tree = 2,
    Blob = 3,
    Tag = 4,
    OfsDelta = 6,
    RefDelta = 7,
}

impl Kind {
    pub fn parse(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Commit),
            2 => Ok(Self::Tree),
            3 => Ok(Self::Blob),
            4 => Ok(Self::Tag),
            6 => Ok(Self::OfsDelta),
            7 => Ok(Self::RefDelta),
            _ => Err(Error::Invalid("pack object type")),
        }
    }

    pub fn name(self) -> Result<&'static [u8]> {
        match self {
            Self::Commit => Ok(b"commit"),
            Self::Tree => Ok(b"tree"),
            Self::Blob => Ok(b"blob"),
            Self::Tag => Ok(b"tag"),
            Self::OfsDelta | Self::RefDelta => Err(Error::Invalid("unresolved delta")),
        }
    }

    pub fn full(self) -> bool {
        !matches!(self, Self::OfsDelta | Self::RefDelta)
    }
}

pub fn loose_header(kind: Kind, size: u64, out: &mut [u8; 40]) -> Result<usize> {
    let mut writer = Writer::new(out);
    writer.put(kind.name()?)?;
    writer.put(b" ")?;
    writer.decimal(size)?;
    writer.put(&[0])?;
    Ok(writer.len())
}

pub fn object_id(kind: Kind, bytes: &[u8]) -> Result<Oid> {
    let mut header = [0; 40];
    let len = loose_header(kind, bytes.len() as u64, &mut header)?;
    let mut sha = Sha1::new();
    sha.update(take(&header, 0, len)?);
    sha.update(bytes);
    Ok(Oid(sha.finalize().into()))
}

pub fn pack_header(count: u32) -> [u8; 12] {
    let mut header = *b"PACK\0\0\0\x02\0\0\0\0";
    for (dst, src) in header.iter_mut().skip(8).zip(count.to_be_bytes()) {
        *dst = src;
    }
    header
}

pub fn pack_count(bytes: &[u8]) -> Result<u32> {
    if take(bytes, 0, 4)? != b"PACK" || !matches!(be32(bytes, 4)?, 2 | 3) {
        return Err(Error::Invalid("PACK header"));
    }
    be32(bytes, 8)
}

#[derive(Clone, Copy, Debug)]
pub enum Base {
    None,
    Offset(u64),
    Oid(Oid),
}

#[derive(Clone, Copy, Debug)]
pub struct EntryHeader {
    pub kind: Kind,
    pub size: usize,
    pub base: Base,
    pub len: usize,
}

pub fn entry_header(bytes: &[u8], entry_offset: u64) -> Result<EntryHeader> {
    let first = *bytes.first().ok_or(Error::Invalid("entry header"))?;
    let kind = Kind::parse((first >> 4) & 7)?;
    let mut value = u64::from(first & 15);
    let mut shift = 4u32;
    let mut cursor = 1usize;
    let mut byte = first;
    while byte & 0x80 != 0 {
        byte = *bytes.get(cursor).ok_or(Error::Invalid("entry size"))?;
        cursor = cursor.saturating_add(1);
        let part = u64::from(byte & 127);
        if shift >= 64 || part > (u64::MAX >> shift) {
            return Err(Error::Invalid("entry size overflow"));
        }
        value |= part << shift;
        shift = shift.saturating_add(7);
    }
    let base = match kind {
        Kind::OfsDelta => {
            byte = *bytes.get(cursor).ok_or(Error::Invalid("OFS_DELTA"))?;
            cursor = cursor.saturating_add(1);
            let mut distance = u64::from(byte & 127);
            while byte & 128 != 0 {
                byte = *bytes.get(cursor).ok_or(Error::Invalid("OFS_DELTA"))?;
                cursor = cursor.saturating_add(1);
                distance = distance
                    .checked_add(1)
                    .and_then(|v| v.checked_mul(128))
                    .and_then(|v| v.checked_add(u64::from(byte & 127)))
                    .ok_or(Error::Invalid("delta distance overflow"))?;
            }
            if distance == 0 {
                return Err(Error::Invalid("zero delta distance"));
            }
            Base::Offset(
                entry_offset
                    .checked_sub(distance)
                    .ok_or(Error::Invalid("delta base before pack"))?,
            )
        }
        Kind::RefDelta => {
            let oid = Oid::from_bytes(take(bytes, cursor, 20)?)?;
            cursor = cursor.saturating_add(20);
            Base::Oid(oid)
        }
        _ => Base::None,
    };
    Ok(EntryHeader {
        kind,
        size: usize::try_from(value).map_err(|_| Error::Capacity("object size"))?,
        base,
        len: cursor,
    })
}

pub fn write_entry_header(kind: Kind, size: usize, out: &mut [u8; 10]) -> Result<usize> {
    let mut rest = (size as u64) >> 4;
    let mut byte = (kind as u8).wrapping_shl(4) | ((size as u8) & 15);
    let mut cursor = 0usize;
    loop {
        *out.get_mut(cursor).ok_or(Error::Capacity("entry header"))? =
            byte | if rest != 0 { 128 } else { 0 };
        cursor = cursor.saturating_add(1);
        if rest == 0 {
            return Ok(cursor);
        }
        byte = (rest as u8) & 127;
        rest >>= 7;
    }
}

pub fn write_ofs_distance(mut distance: u64, out: &mut [u8; 10]) -> Result<usize> {
    if distance == 0 {
        return Err(Error::Invalid("zero OFS distance"));
    }
    let mut reverse = [0; 10];
    let mut cursor = reverse.len().saturating_sub(1);
    *reverse
        .get_mut(cursor)
        .ok_or(Error::Capacity("OFS varint"))? = (distance & 127) as u8;
    distance >>= 7;
    while distance != 0 {
        distance = distance.saturating_sub(1);
        cursor = cursor.checked_sub(1).ok_or(Error::Capacity("OFS varint"))?;
        *reverse
            .get_mut(cursor)
            .ok_or(Error::Capacity("OFS varint"))? = 128 | ((distance & 127) as u8);
        distance >>= 7;
    }
    let bytes = reverse.get(cursor..).ok_or(Error::Invalid("OFS bytes"))?;
    out.get_mut(..bytes.len())
        .ok_or(Error::Capacity("OFS output"))?
        .copy_from_slice(bytes);
    Ok(bytes.len())
}

pub struct Zlib {
    inflate: Decompress,
    deflate: Compress,
}

impl Zlib {
    pub fn new(level: u32) -> Self {
        Self {
            inflate: Decompress::new(true),
            deflate: Compress::new(Compression::new(level.min(9)), true),
        }
    }

    pub fn inflate(&mut self, input: &[u8], out: &mut [u8], expected: usize) -> Result<usize> {
        let out = out
            .get_mut(..expected)
            .ok_or(Error::Capacity("inflate scratch"))?;
        self.inflate.reset(true);
        let mut consumed = 0usize;
        let mut produced = 0usize;
        loop {
            let before_in = self.inflate.total_in();
            let before_out = self.inflate.total_out();
            let status = self
                .inflate
                .decompress(
                    input.get(consumed..).ok_or(Error::Invalid("zlib cursor"))?,
                    out.get_mut(produced..)
                        .ok_or(Error::Invalid("inflate cursor"))?,
                    FlushDecompress::None,
                )
                .map_err(|_| Error::Invalid("zlib stream"))?;
            let used = usize::try_from(self.inflate.total_in().saturating_sub(before_in))
                .map_err(|_| Error::Invalid("zlib input length"))?;
            let made = usize::try_from(self.inflate.total_out().saturating_sub(before_out))
                .map_err(|_| Error::Invalid("zlib output length"))?;
            consumed = consumed.saturating_add(used);
            produced = produced.saturating_add(made);
            if status == Status::StreamEnd {
                if produced != expected {
                    return Err(Error::Invalid("inflated object size"));
                }
                return Ok(consumed);
            }
            if used == 0 && made == 0 {
                return Err(Error::Invalid("truncated or oversized zlib stream"));
            }
        }
    }

    pub fn deflate(&mut self, input: &[u8], out: &mut [u8]) -> Result<usize> {
        self.deflate.reset();
        let mut consumed = 0usize;
        let mut produced = 0usize;
        loop {
            let before_in = self.deflate.total_in();
            let before_out = self.deflate.total_out();
            let status = self
                .deflate
                .compress(
                    input
                        .get(consumed..)
                        .ok_or(Error::Invalid("deflate input"))?,
                    out.get_mut(produced..)
                        .ok_or(Error::Capacity("deflate scratch"))?,
                    FlushCompress::Finish,
                )
                .map_err(|_| Error::Invalid("deflate stream"))?;
            let used = usize::try_from(self.deflate.total_in().saturating_sub(before_in))
                .map_err(|_| Error::Capacity("deflate input"))?;
            let made = usize::try_from(self.deflate.total_out().saturating_sub(before_out))
                .map_err(|_| Error::Capacity("deflate output"))?;
            consumed = consumed.saturating_add(used);
            produced = produced.saturating_add(made);
            if status == Status::StreamEnd {
                return Ok(produced);
            }
            if used == 0 && made == 0 {
                return Err(Error::Capacity("deflate scratch"));
            }
        }
    }
}

fn delta_number(delta: &[u8], cursor: &mut usize) -> Result<u64> {
    let mut number = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *delta.get(*cursor).ok_or(Error::Invalid("delta varint"))?;
        *cursor = cursor.saturating_add(1);
        let part = u64::from(byte & 127);
        if shift >= 64 || part > (u64::MAX >> shift) {
            return Err(Error::Invalid("delta varint overflow"));
        }
        number |= part << shift;
        if byte & 128 == 0 {
            return Ok(number);
        }
        shift = shift.saturating_add(7);
    }
}

pub fn apply_delta(base: &[u8], delta: &[u8], out: &mut [u8]) -> Result<usize> {
    let mut cursor = 0usize;
    if delta_number(delta, &mut cursor)? != base.len() as u64 {
        return Err(Error::Invalid("delta base size"));
    }
    let result_len = usize::try_from(delta_number(delta, &mut cursor)?)
        .map_err(|_| Error::Capacity("delta size"))?;
    let out = out
        .get_mut(..result_len)
        .ok_or(Error::Capacity("delta scratch"))?;
    let mut written = 0usize;
    while let Some(&op) = delta.get(cursor) {
        cursor = cursor.saturating_add(1);
        if op == 0 {
            return Err(Error::Invalid("reserved delta opcode"));
        }
        let bytes = if op & 128 != 0 {
            let mut offset = 0usize;
            let mut len = 0usize;
            for bit in 0..7u32 {
                if op & (1u8 << bit) != 0 {
                    let byte = usize::from(
                        *delta
                            .get(cursor)
                            .ok_or(Error::Invalid("delta copy operand"))?,
                    );
                    cursor = cursor.saturating_add(1);
                    if bit < 4 {
                        offset |= byte << bit.saturating_mul(8);
                    } else {
                        len |= byte << bit.saturating_sub(4).saturating_mul(8);
                    }
                }
            }
            if len == 0 {
                len = 65536;
            }
            take(base, offset, len)?
        } else {
            let bytes = take(delta, cursor, usize::from(op))?;
            cursor = cursor.saturating_add(bytes.len());
            bytes
        };
        let end = written
            .checked_add(bytes.len())
            .ok_or(Error::Invalid("delta result overflow"))?;
        out.get_mut(written..end)
            .ok_or(Error::Invalid("delta result size"))?
            .copy_from_slice(bytes);
        written = end;
    }
    if written != result_len {
        return Err(Error::Invalid("delta result length"));
    }
    Ok(written)
}

#[derive(Clone, Copy)]
pub struct TreeEntry<'a> {
    pub oid: Oid,
    pub name: &'a [u8],
    pub directory: bool,
    pub gitlink: bool,
    pub raw: &'a [u8],
    pub offset: usize,
}

pub struct TreeEntries<'a> {
    remaining: &'a [u8],
    offset: usize,
}

impl<'a> TreeEntries<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            remaining: bytes,
            offset: 0,
        }
    }

    pub fn next_entry(&mut self) -> Result<Option<TreeEntry<'a>>> {
        if self.remaining.is_empty() {
            return Ok(None);
        }
        let bytes = self.remaining;
        let space = bytes
            .iter()
            .take(7)
            .position(|byte| *byte == b' ')
            .ok_or(Error::Invalid("tree mode"))?;
        let mode = take(bytes, 0, space)?;
        if !matches!(
            mode,
            b"40000" | b"040000" | b"100644" | b"100755" | b"120000" | b"160000"
        ) {
            return Err(Error::Invalid("tree entry mode"));
        }
        let name_at = space.saturating_add(1);
        let rest = bytes.get(name_at..).ok_or(Error::Invalid("tree name"))?;
        let nul = rest
            .iter()
            .position(|byte| *byte == 0)
            .ok_or(Error::Invalid("tree filename terminator"))?;
        let name = take(rest, 0, nul)?;
        if name.is_empty() || name.contains(&b'/') || matches!(name, b"." | b"..") {
            return Err(Error::Invalid("tree filename"));
        }
        let oid_at = name_at.saturating_add(nul).saturating_add(1);
        let oid = Oid::from_bytes(take(bytes, oid_at, 20)?)?;
        let raw_len = oid_at.saturating_add(20);
        let offset = self.offset;
        self.offset = self
            .offset
            .checked_add(raw_len)
            .ok_or(Error::Invalid("tree cursor overflow"))?;
        self.remaining = bytes
            .get(oid_at.saturating_add(20)..)
            .ok_or(Error::Invalid("tree OID"))?;
        Ok(Some(TreeEntry {
            oid,
            name,
            directory: matches!(mode, b"40000" | b"040000"),
            gitlink: mode == b"160000",
            raw: take(bytes, 0, raw_len)?,
            offset,
        }))
    }
}
