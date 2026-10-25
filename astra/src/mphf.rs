use crate::arena::{Mapping, mix, push};
use crate::{Error, ObjType, Oid20, Result};
use sha1::{Digest, Sha1};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

pub const NO_BASE: u32 = u32::MAX;
const HEADER: usize = 64;
const ROW: usize = 56;
const MAGIC: &[u8; 8] = b"GVFSMH02";
const LENGTH_ESCAPE: u64 = 0x3fff;
const OFFSET_MASK: u64 = (1u64 << 34) - 1;
const SINGLE: u32 = 1u32 << 31;

#[derive(Debug, Clone, Copy)]
pub struct Overflow {
    pub offset: u64,
    pub len: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct Record {
    pub oid: Oid20,
    pub size_kind: u32,
    pub location: u64,
    pub timestamp: u64,
    pub family: u64,
    pub base: u32,
    pub crc: u32,
}

impl Record {
    pub fn new(oid: Oid20, kind: ObjType, size: usize, timestamp: u64, base: u32) -> Result<Self> {
        if size >= (1 << 29) || kind.is_delta() {
            return Err(Error::Capacity("object size (maximum 512 MiB)"));
        }
        Ok(Self {
            oid,
            size_kind: (u32::from(kind.code()) << 29) | size as u32,
            location: 0,
            timestamp,
            family: 0,
            base,
            crc: 0,
        })
    }

    pub fn kind(&self) -> Result<ObjType> {
        ObjType::from_code((self.size_kind >> 29) as u8)
            .filter(|k| !k.is_delta())
            .ok_or(Error::Invalid("index object type"))
    }
    pub fn size(&self) -> usize {
        (self.size_kind & 0x1fff_ffff) as usize
    }

    pub fn locate(&mut self, offset: u64, len: u32, overflow: &mut Vec<Overflow>) -> Result<()> {
        let fp = u64::from(fingerprint(&self.oid)) << 48;
        if offset <= OFFSET_MASK && u64::from(len) < LENGTH_ESCAPE {
            self.location = fp | (offset << 14) | u64::from(len);
        } else {
            let index =
                u64::try_from(overflow.len()).map_err(|_| Error::Capacity("overflow table"))?;
            if index > OFFSET_MASK {
                return Err(Error::Capacity("overflow table"));
            }
            push(overflow, Overflow { offset, len })?;
            self.location = fp | (index << 14) | LENGTH_ESCAPE;
        }
        Ok(())
    }

    pub fn encode(&self) -> [u8; ROW] {
        let mut out = [0; ROW];
        for (dst, src) in out.iter_mut().zip(
            self.oid
                .0
                .into_iter()
                .chain(self.size_kind.to_le_bytes())
                .chain(self.location.to_le_bytes())
                .chain(self.timestamp.to_le_bytes())
                .chain(self.family.to_le_bytes())
                .chain(self.base.to_le_bytes())
                .chain(self.crc.to_le_bytes()),
        ) {
            *dst = src;
        }
        out
    }
}

fn fingerprint(oid: &Oid20) -> u16 {
    let [.., a, b] = oid.0;
    u16::from_be_bytes([a, b])
}

fn hash(oid: &Oid20) -> u64 {
    let [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t] = oid.0;
    mix(u64::from_le_bytes([a, b, c, d, e, f, g, h])
        ^ u64::from_le_bytes([i, j, k, l, m, n, o, p]).rotate_left(23)
        ^ u64::from(u32::from_le_bytes([q, r, s, t])))
}

fn modulo(hash: u64, count: usize) -> Result<usize> {
    Ok(hash
        .checked_rem(count as u64)
        .ok_or(Error::Invalid("zero MPHF dimension"))? as usize)
}

fn position(hash: u64, pilot: u32, count: usize) -> Result<usize> {
    modulo(
        mix(hash ^ u64::from(pilot).wrapping_mul(0x9e37_79b9_7f4a_7c15)),
        count,
    )
}

#[derive(Clone, Copy, Default)]
struct Bucket {
    id: u32,
    start: u32,
    count: u32,
}

/// CHD: displace multi-key buckets, then assign singleton buckets to the free slots.
/// Construction allocates its arrays once; neither the placement loop nor lookup allocates.
fn build(records: &[Record]) -> Result<(Vec<u32>, Vec<u32>)> {
    let n = records.len();
    if n >= SINGLE as usize {
        return Err(Error::Capacity("MPHF keys"));
    }
    let b = n.div_ceil(4).max(1);
    let mut buckets = vec![Bucket::default(); b];
    for row in records {
        let id = modulo(hash(&row.oid), b)?;
        let bucket = buckets.get_mut(id).ok_or(Error::Invalid("MPHF bucket"))?;
        bucket.count = bucket
            .count
            .checked_add(1)
            .ok_or(Error::Capacity("MPHF bucket"))?;
    }
    let mut cursor = 0u32;
    let mut largest = 0usize;
    for (id, bucket) in buckets.iter_mut().enumerate() {
        bucket.id = id as u32;
        bucket.start = cursor;
        cursor = cursor
            .checked_add(bucket.count)
            .ok_or(Error::Capacity("MPHF"))?;
        largest = largest.max(bucket.count as usize);
    }
    let mut next: Vec<u32> = buckets.iter().map(|b| b.start).collect();
    let mut grouped = vec![0u32; n];
    for (index, row) in records.iter().enumerate() {
        let id = modulo(hash(&row.oid), b)?;
        let slot = next.get_mut(id).ok_or(Error::Invalid("MPHF bucket"))?;
        *grouped
            .get_mut(*slot as usize)
            .ok_or(Error::Invalid("MPHF group"))? = index as u32;
        *slot = slot.saturating_add(1);
    }
    buckets.sort_unstable_by(|a, b| b.count.cmp(&a.count).then(a.id.cmp(&b.id)));
    let mut pilots = vec![0; b];
    let mut slots = vec![NO_BASE; n];
    let mut candidates = vec![0; largest];
    let mut free = 0usize;
    for bucket in buckets {
        if bucket.count == 0 {
            continue;
        }
        let group = grouped
            .get(bucket.start as usize..bucket.start.saturating_add(bucket.count) as usize)
            .ok_or(Error::Invalid("MPHF group"))?;
        let pilot_out = pilots
            .get_mut(bucket.id as usize)
            .ok_or(Error::Invalid("MPHF pilot"))?;
        if bucket.count == 1 {
            while slots.get(free).is_some_and(|&v| v != NO_BASE) {
                free = free.saturating_add(1);
            }
            *slots
                .get_mut(free)
                .ok_or(Error::Invalid("MPHF free slot"))? =
                *group.first().ok_or(Error::Invalid("MPHF singleton"))?;
            *pilot_out = SINGLE | free as u32;
            continue;
        }
        for (i, &a) in group.iter().enumerate() {
            let oid = records
                .get(a as usize)
                .ok_or(Error::Invalid("MPHF row"))?
                .oid;
            for &b in group.get(..i).ok_or(Error::Invalid("MPHF group"))? {
                if records.get(b as usize).is_some_and(|r| r.oid == oid) {
                    return Err(Error::Invalid("duplicate OID in slab"));
                }
            }
        }
        let mut found = false;
        for pilot in 1..1_000_000u32 {
            let mut valid = true;
            for (i, &row) in group.iter().enumerate() {
                let h = hash(
                    &records
                        .get(row as usize)
                        .ok_or(Error::Invalid("MPHF row"))?
                        .oid,
                );
                let slot = position(h, pilot, n)?;
                if slots.get(slot) != Some(&NO_BASE)
                    || candidates
                        .get(..i)
                        .ok_or(Error::Invalid("MPHF scratch"))?
                        .contains(&slot)
                {
                    valid = false;
                    break;
                }
                *candidates
                    .get_mut(i)
                    .ok_or(Error::Invalid("MPHF scratch"))? = slot;
            }
            if valid {
                for (&row, &slot) in group.iter().zip(&candidates) {
                    *slots.get_mut(slot).ok_or(Error::Invalid("MPHF slot"))? = row;
                }
                *pilot_out = pilot;
                found = true;
                break;
            }
        }
        if !found {
            return Err(Error::Capacity("MPHF pilot search"));
        }
    }
    Ok((pilots, slots))
}

pub struct Index {
    map: Mapping,
    count: usize,
    buckets: usize,
    overflow_count: usize,
    overflow_at: usize,
    pilots_at: usize,
    slots_at: usize,
    pub pack_len: u64,
    pub pack_hash: Oid20,
    pub checksum: Oid20,
}

fn u32_at(bytes: &[u8], offset: usize) -> Result<u32> {
    let a: [u8; 4] = bytes
        .get(offset..offset.saturating_add(4))
        .ok_or(Error::Invalid("truncated index"))?
        .try_into()
        .map_err(|_| Error::Invalid("u32"))?;
    Ok(u32::from_le_bytes(a))
}
fn u64_at(bytes: &[u8], offset: usize) -> Result<u64> {
    let a: [u8; 8] = bytes
        .get(offset..offset.saturating_add(8))
        .ok_or(Error::Invalid("truncated index"))?
        .try_into()
        .map_err(|_| Error::Invalid("u64"))?;
    Ok(u64::from_le_bytes(a))
}

impl Index {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let len = usize::try_from(file.metadata()?.len())
            .map_err(|_| Error::Capacity("index mapping"))?;
        Self::parse(Mapping::read(file, len)?)
    }

    fn parse(map: Mapping) -> Result<Self> {
        let bytes = map.bytes();
        let body = bytes
            .len()
            .checked_sub(20)
            .ok_or(Error::Invalid("index checksum"))?;
        if bytes.get(..8) != Some(MAGIC)
            || bytes.get(body..)
                != Some(&crate::sha1_parts(&[bytes
                    .get(..body)
                    .ok_or(Error::Invalid("index"))?]))
        {
            return Err(Error::Invalid("MPHF magic or checksum"));
        }
        let count = u32_at(bytes, 8)? as usize;
        let buckets = u32_at(bytes, 12)? as usize;
        let overflow_count = u32_at(bytes, 16)? as usize;
        if count >= SINGLE as usize || buckets != count.div_ceil(4).max(1) || overflow_count > count
        {
            return Err(Error::Invalid("MPHF dimensions"));
        }
        let overflow_at = HEADER
            .checked_add(count.checked_mul(ROW).ok_or(Error::Capacity("index"))?)
            .ok_or(Error::Capacity("index"))?;
        let pilots_at = overflow_at
            .checked_add(overflow_count.saturating_mul(16))
            .ok_or(Error::Capacity("index"))?;
        let slots_at = pilots_at
            .checked_add(buckets.saturating_mul(4))
            .ok_or(Error::Capacity("index"))?;
        if slots_at.checked_add(count.saturating_mul(4)) != Some(body) {
            return Err(Error::Invalid("index length"));
        }
        let pack_len = u64_at(bytes, 24)?;
        let pack_hash = Oid20::from_slice(bytes.get(32..52).ok_or(Error::Invalid("pack digest"))?)
            .ok_or(Error::Invalid("pack digest"))?;
        let checksum = Oid20::from_slice(bytes.get(body..).ok_or(Error::Invalid("index digest"))?)
            .ok_or(Error::Invalid("index digest"))?;
        let index = Self {
            map,
            count,
            buckets,
            overflow_count,
            overflow_at,
            pilots_at,
            slots_at,
            pack_len,
            pack_hash,
            checksum,
        };
        let mut end = 12u64;
        for i in 0..count {
            let row = index.record(i as u32)?;
            row.kind()?;
            let loc = index.location(&row)?;
            if loc.offset != end
                || loc.len == 0
                || (row.base != NO_BASE && row.base as usize >= count)
            {
                return Err(Error::Invalid("noncontiguous slab or invalid delta base"));
            }
            end = end
                .checked_add(u64::from(loc.len))
                .ok_or(Error::Invalid("pack bounds"))?;
            if index.find(&row.oid)? != Some(i as u32) {
                return Err(Error::Invalid("MPHF membership"));
            }
            let mut base = row.base;
            let mut depth = 0usize;
            while base != NO_BASE {
                depth = depth.saturating_add(1);
                if depth > 128 {
                    return Err(Error::Invalid("cyclic or excessive delta chain"));
                }
                base = index.record(base)?.base;
            }
        }
        if end.checked_add(20) != Some(pack_len) {
            return Err(Error::Invalid("pack length in index"));
        }
        Ok(index)
    }

    pub fn len(&self) -> usize {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn record(&self, ordinal: u32) -> Result<Record> {
        if ordinal as usize >= self.count {
            return Err(Error::Invalid("object ordinal"));
        }
        let at = HEADER.saturating_add((ordinal as usize).saturating_mul(ROW));
        let b = self
            .map
            .bytes()
            .get(at..at.saturating_add(ROW))
            .ok_or(Error::Invalid("index row"))?;
        Ok(Record {
            oid: Oid20::from_slice(b.get(..20).ok_or(Error::Invalid("OID"))?)
                .ok_or(Error::Invalid("OID"))?,
            size_kind: u32_at(b, 20)?,
            location: u64_at(b, 24)?,
            timestamp: u64_at(b, 32)?,
            family: u64_at(b, 40)?,
            base: u32_at(b, 48)?,
            crc: u32_at(b, 52)?,
        })
    }

    pub fn location(&self, row: &Record) -> Result<Overflow> {
        let len = row.location & LENGTH_ESCAPE;
        let offset = (row.location >> 14) & OFFSET_MASK;
        if len != LENGTH_ESCAPE {
            return Ok(Overflow {
                offset,
                len: len as u32,
            });
        }
        let i = usize::try_from(offset).map_err(|_| Error::Invalid("extended offset"))?;
        if i >= self.overflow_count {
            return Err(Error::Invalid("extended offset"));
        }
        let at = self.overflow_at.saturating_add(i.saturating_mul(16));
        Ok(Overflow {
            offset: u64_at(self.map.bytes(), at)?,
            len: u32_at(self.map.bytes(), at.saturating_add(8))?,
        })
    }

    pub fn find(&self, oid: &Oid20) -> Result<Option<u32>> {
        if self.count == 0 {
            return Ok(None);
        }
        let h = hash(oid);
        let bucket = modulo(h, self.buckets)?;
        let pilot = u32_at(
            self.map.bytes(),
            self.pilots_at.saturating_add(bucket.saturating_mul(4)),
        )?;
        let slot = if pilot & SINGLE != 0 {
            (pilot & !SINGLE) as usize
        } else {
            position(h, pilot, self.count)?
        };
        if slot >= self.count {
            return Err(Error::Invalid("MPHF slot"));
        }
        let ordinal = u32_at(
            self.map.bytes(),
            self.slots_at.saturating_add(slot.saturating_mul(4)),
        )?;
        let row = self.record(ordinal)?;
        if (row.location >> 48) as u16 != fingerprint(oid) || row.oid != *oid {
            return Ok(None);
        }
        Ok(Some(ordinal))
    }
}

pub fn write_index(
    path: &Path,
    records: &[Record],
    overflow: &[Overflow],
    pack_len: u64,
    pack_hash: Oid20,
) -> Result<()> {
    let (pilots, slots) = build(records)?;
    let mut header = [0u8; HEADER];
    let mut writer = crate::buf::ByteWriter::new(&mut header);
    writer.put(MAGIC)?;
    writer.put(&(records.len() as u32).to_le_bytes())?;
    writer.put(&(pilots.len() as u32).to_le_bytes())?;
    writer.put(&(overflow.len() as u32).to_le_bytes())?;
    writer.put(&0u32.to_le_bytes())?;
    writer.put(&pack_len.to_le_bytes())?;
    writer.put(&pack_hash.0)?;
    let file = File::create(path)?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    let mut sha = Sha1::new();
    let mut write = |data: &[u8]| -> Result<()> {
        sha.update(data);
        out.write_all(data)?;
        Ok(())
    };
    write(&header)?;
    for record in records {
        write(&record.encode())?;
    }
    for loc in overflow {
        write(&loc.offset.to_le_bytes())?;
        write(&loc.len.to_le_bytes())?;
        write(&0u32.to_le_bytes())?;
    }
    for pilot in pilots {
        write(&pilot.to_le_bytes())?;
    }
    for slot in slots {
        write(&slot.to_le_bytes())?;
    }
    out.write_all(&sha.finalize())?;
    out.flush()?;
    out.get_ref().sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_membership_and_large_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let mut rows = Vec::new();
        let mut overflow = Vec::with_capacity(10000);
        let mut offset = 12;
        for i in 0..10000u64 {
            let oid = Oid20(crate::sha1_parts(&[&i.to_le_bytes()]));
            let mut r = Record::new(oid, ObjType::Blob, 9, 1, NO_BASE).unwrap();
            let len = if i % 7 == 0 { 1 << 24 } else { 99 };
            r.locate(offset, len, &mut overflow).unwrap();
            offset += u64::from(len);
            rows.push(r);
        }
        let path = dir.path().join("test.mphf");
        write_index(&path, &rows, &overflow, offset + 20, Oid20([7; 20])).unwrap();
        let index = Index::open(&path).unwrap();
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(index.find(&row.oid).unwrap(), Some(i as u32));
        }
        for i in 10000..20000u64 {
            assert_eq!(
                index
                    .find(&Oid20(crate::sha1_parts(&[&i.to_le_bytes()])))
                    .unwrap(),
                None
            );
        }
        assert!(index.location(&index.record(9999).unwrap()).unwrap().offset > (1 << 34));
        let duplicate = [rows[0], rows[0]];
        assert!(build(&duplicate).is_err());
    }
}
