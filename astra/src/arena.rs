use crate::{Error, Result};
use memmap2::{Mmap, MmapMut};
use std::io::Read;

/// Anonymous mappings avoid the unsound external-mutation contract of file mmap.
/// Immutable file snapshots are read once at admission, then shared without copies.
pub struct Mapping(Mmap);

impl Mapping {
    pub fn read(mut input: impl Read, len: usize) -> Result<Self> {
        if len == 0 {
            return Err(Error::Invalid("empty mapping"));
        }
        let mut map = MmapMut::map_anon(len)?;
        input.read_exact(&mut map)?;
        Ok(Self(map.make_read_only()?))
    }

    pub fn bytes(&self) -> &[u8] {
        &self.0
    }
}

pub struct Arena {
    bytes: MmapMut,
    used: usize,
}

impl Arena {
    pub fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(Error::Capacity("zero-sized arena"));
        }
        let mut bytes = MmapMut::map_anon(capacity)?;
        #[cfg(target_os = "linux")]
        bytes.advise(memmap2::Advice::HugePage)?;
        alloc_guard::prefault(&mut bytes);
        Ok(Self { bytes, used: 0 })
    }

    pub fn reset(&mut self) {
        self.used = 0;
    }
    pub fn len(&self) -> usize {
        self.used
    }
    pub fn is_empty(&self) -> bool {
        self.used == 0
    }
    pub fn capacity(&self) -> usize {
        self.bytes.len()
    }
    pub fn buffer(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
    pub fn bytes(&self) -> &[u8] {
        self.bytes.get(..self.used).unwrap_or(&[])
    }

    pub fn set_len(&mut self, len: usize) -> Result<()> {
        if len > self.capacity() {
            return Err(Error::Capacity("arena"));
        }
        self.used = len;
        Ok(())
    }

    pub fn extend(&mut self, data: &[u8]) -> Result<()> {
        let end = self
            .used
            .checked_add(data.len())
            .ok_or(Error::Capacity("arena"))?;
        self.bytes
            .get_mut(self.used..end)
            .ok_or(Error::Capacity("arena"))?
            .copy_from_slice(data);
        self.used = end;
        Ok(())
    }
}

pub fn push<T>(items: &mut Vec<T>, item: T) -> Result<()> {
    if items.len() == items.capacity() {
        return Err(Error::Capacity("record arena"));
    }
    items.push(item);
    Ok(())
}

pub fn hash_oid(oid: &crate::Oid20) -> u64 {
    let [a, b, c, d, e, f, g, h, ..] = oid.0;
    u64::from_le_bytes([a, b, c, d, e, f, g, h])
}

pub fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
