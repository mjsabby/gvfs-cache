//! Shared idx v2 encoder and a borrowed view for linear cold-prefetch index merging.

use crate::arena::append;
use crate::codec::{Oid, be32, be64, checksum, take};
use crate::{Error, Result};

const HEADER: usize = 1032;
const LARGE: u64 = 0x7fff_ffff;

#[derive(Clone, Copy, Debug)]
pub struct Entry {
    pub oid: Oid,
    pub crc: u32,
    pub offset: u64,
}

impl Entry {
    pub const EMPTY: Self = Self {
        oid: Oid::ZERO,
        crc: 0,
        offset: 0,
    };
}

pub fn encoded_len(count: usize, large: usize) -> Result<usize> {
    count
        .checked_mul(28)
        .and_then(|n| n.checked_add(large.checked_mul(8)?))
        .and_then(|n| n.checked_add(1072))
        .ok_or(Error::Capacity("idx bytes"))
}

pub fn write(
    count: usize,
    get: impl Fn(usize) -> Result<Entry>,
    pack: Oid,
    out: &mut Vec<u8>,
) -> Result<()> {
    let mut counts = [0u32; 256];
    let mut previous = None;
    for i in 0..count {
        let entry = get(i)?;
        if previous.is_some_and(|oid| oid >= entry.oid) {
            return Err(Error::Invalid("unsorted or duplicate idx key"));
        }
        previous = Some(entry.oid);
        let [first, ..] = entry.oid.0;
        let slot = counts
            .get_mut(usize::from(first))
            .ok_or(Error::Invalid("idx fanout"))?;
        *slot = slot.saturating_add(1);
    }
    out.clear();
    append(out, b"\xfftOc\0\0\0\x02")?;
    let mut total = 0u32;
    for count in counts {
        total = total.saturating_add(count);
        append(out, &total.to_be_bytes())?;
    }
    for i in 0..count {
        append(out, &get(i)?.oid.0)?;
    }
    for i in 0..count {
        append(out, &get(i)?.crc.to_be_bytes())?;
    }
    let mut large = 0u32;
    for i in 0..count {
        let offset = get(i)?.offset;
        let value = if offset > LARGE {
            let value = large | 0x8000_0000;
            large = large.saturating_add(1);
            value
        } else {
            offset as u32
        };
        append(out, &value.to_be_bytes())?;
    }
    for i in 0..count {
        let offset = get(i)?.offset;
        if offset > LARGE {
            append(out, &offset.to_be_bytes())?;
        }
    }
    append(out, &pack.0)?;
    let sum = checksum(out);
    append(out, &sum)
}

pub fn write_sorted(entries: &[Entry], pack: Oid, out: &mut Vec<u8>) -> Result<()> {
    write(
        entries.len(),
        |i| entries.get(i).copied().ok_or(Error::Invalid("idx row")),
        pack,
        out,
    )
}

pub struct View<'a> {
    bytes: &'a [u8],
    count: usize,
    crc_at: usize,
    offsets_at: usize,
    large_at: usize,
}
impl<'a> View<'a> {
    pub fn parse(bytes: &'a [u8], count: usize) -> Result<Self> {
        if take(bytes, 0, 8)? != b"\xfftOc\0\0\0\x02" || be32(bytes, 1028)? as usize != count {
            return Err(Error::Invalid("idx header"));
        }
        let crc_at = HEADER
            .checked_add(count.checked_mul(20).ok_or(Error::Invalid("idx count"))?)
            .ok_or(Error::Invalid("idx count"))?;
        let offsets_at = crc_at
            .checked_add(count.checked_mul(4).ok_or(Error::Invalid("idx count"))?)
            .ok_or(Error::Invalid("idx count"))?;
        let large_at = offsets_at
            .checked_add(count.checked_mul(4).ok_or(Error::Invalid("idx count"))?)
            .ok_or(Error::Invalid("idx count"))?;
        let extra = bytes
            .len()
            .checked_sub(large_at.saturating_add(40))
            .ok_or(Error::Invalid("idx length"))?;
        if !extra.is_multiple_of(8) {
            return Err(Error::Invalid("idx large-offset table"));
        }
        Ok(Self {
            bytes,
            count,
            crc_at,
            offsets_at,
            large_at,
        })
    }
    pub fn entry(&self, i: usize) -> Result<Entry> {
        if i >= self.count {
            return Err(Error::Invalid("idx ordinal"));
        }
        let value = be32(
            self.bytes,
            self.offsets_at.saturating_add(i.saturating_mul(4)),
        )?;
        let offset = if value & 0x8000_0000 == 0 {
            u64::from(value)
        } else {
            let ordinal = (value & 0x7fff_ffff) as usize;
            let large_count = self
                .bytes
                .len()
                .saturating_sub(self.large_at)
                .saturating_sub(40)
                / 8;
            if ordinal >= large_count {
                return Err(Error::Invalid("idx large-offset ordinal"));
            }
            be64(
                self.bytes,
                self.large_at.saturating_add(ordinal.saturating_mul(8)),
            )?
        };
        Ok(Entry {
            oid: Oid::from_bytes(take(
                self.bytes,
                HEADER.saturating_add(i.saturating_mul(20)),
                20,
            )?)?,
            crc: be32(self.bytes, self.crc_at.saturating_add(i.saturating_mul(4)))?,
            offset,
        })
    }
}

/// CRC concatenation is linear in the prefix CRC. This replaces a header without
/// fetching/inflating the payload, including when the OFS varint length changes.
pub fn replace_prefix_crc(original: u32, old: &[u8], new: &[u8], payload_len: u64) -> u32 {
    let mut difference =
        crc32fast::Hasher::new_with_initial(crc32fast::hash(old) ^ crc32fast::hash(new));
    difference.combine(&crc32fast::Hasher::new_with_initial_len(0, payload_len));
    original ^ difference.finalize()
}
