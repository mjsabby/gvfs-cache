use crate::buf::ByteWriter;
use crate::{Error, Oid20, Result};

pub const MAX_SLABS: usize = 1024;
pub const WEEK: u64 = 604800;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum Tier {
    Live = 0,
    M15 = 900,
    H1 = 3600,
    H6 = 21600,
    H12 = 43200,
    H24 = 86400,
    WeeklyDelta = 604800,
    Baseline = 604801,
}

impl Tier {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "15m" | "m15" => Ok(Self::M15),
            "1h" | "h1" => Ok(Self::H1),
            "6h" | "h6" => Ok(Self::H6),
            "12h" | "h12" => Ok(Self::H12),
            "24h" | "h24" => Ok(Self::H24),
            "168h" | "h168" => Ok(Self::WeeklyDelta),
            _ => Err(Error::Invalid("tier (15m, 1h, 6h, 12h, 24h, 168h, auto)")),
        }
    }
    fn decode(n: u32) -> Result<Self> {
        match n {
            0 => Ok(Self::Live),
            900 => Ok(Self::M15),
            3600 => Ok(Self::H1),
            21600 => Ok(Self::H6),
            43200 => Ok(Self::H12),
            86400 => Ok(Self::H24),
            604800 => Ok(Self::WeeklyDelta),
            604801 => Ok(Self::Baseline),
            _ => Err(Error::Invalid("manifest tier")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlabRef {
    pub hash: Oid20,
    pub index_hash: Oid20,
    pub tip: Oid20,
    pub tier: Tier,
    pub start: u64,
    pub end: u64,
    pub count: u32,
    pub pack_len: u64,
    pub idx_len: u64,
}

impl SlabRef {
    pub fn key(&self, extension: &str) -> String {
        format!("{}-{}.{}", self.hash, self.index_hash, extension)
    }
    fn write(&self, w: &mut ByteWriter<'_>) -> Result<()> {
        w.put(&self.hash.0)?;
        w.put(&self.index_hash.0)?;
        w.put(&self.tip.0)?;
        w.put(&(self.tier as u32).to_le_bytes())?;
        w.put(&self.start.to_le_bytes())?;
        w.put(&self.end.to_le_bytes())?;
        w.put(&self.count.to_le_bytes())?;
        w.put(&self.pack_len.to_le_bytes())?;
        w.put(&self.idx_len.to_le_bytes())?;
        Ok(())
    }
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let slab = Self {
            hash: r.oid()?,
            index_hash: r.oid()?,
            tip: r.oid()?,
            tier: Tier::decode(r.u32()?)?,
            start: r.u64()?,
            end: r.u64()?,
            count: r.u32()?,
            pack_len: r.u64()?,
            idx_len: r.u64()?,
        };
        if slab.start > slab.end
            || slab.end > i64::MAX as u64
            || slab.pack_len < 32
            || slab.idx_len < 1072
        {
            return Err(Error::Invalid("manifest slab bounds"));
        }
        Ok(slab)
    }
}

#[derive(Debug, Clone)]
pub struct Manifest {
    pub generation: u64,
    pub timestamp: u64,
    pub genesis: u64,
    pub tip: Oid20,
    pub baseline: SlabRef,
    pub weekly: Vec<SlabRef>,
    pub pyramid: Vec<SlabRef>,
}

impl Manifest {
    pub fn encode(&self) -> Result<Vec<u8>> {
        if self.weekly.len().saturating_add(self.pyramid.len()) >= MAX_SLABS {
            return Err(Error::Capacity("manifest slabs"));
        }
        let mut bytes = vec![0; 256usize.saturating_add(MAX_SLABS.saturating_mul(100))];
        let mut w = ByteWriter::new(&mut bytes);
        w.put(b"GVFSMN02")?;
        w.put(&self.generation.to_le_bytes())?;
        w.put(&self.timestamp.to_le_bytes())?;
        w.put(&self.genesis.to_le_bytes())?;
        w.put(&self.tip.0)?;
        self.baseline.write(&mut w)?;
        w.put(&(self.weekly.len() as u32).to_le_bytes())?;
        w.put(&(self.pyramid.len() as u32).to_le_bytes())?;
        for slab in self.weekly.iter().chain(&self.pyramid) {
            slab.write(&mut w)?;
        }
        let digest = crate::sha1_parts(&[w.written()]);
        w.put(&digest)?;
        let len = w.len();
        bytes.truncate(len);
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let end = bytes
            .len()
            .checked_sub(20)
            .ok_or(Error::Invalid("manifest trailer"))?;
        let body = bytes.get(..end).ok_or(Error::Invalid("manifest"))?;
        if bytes.get(end..) != Some(&crate::sha1_parts(&[body])) {
            return Err(Error::Invalid("manifest checksum"));
        }
        let mut r = Reader(body);
        if r.take(8)? != b"GVFSMN02" {
            return Err(Error::Invalid("manifest version"));
        }
        let generation = r.u64()?;
        let timestamp = r.u64()?;
        let genesis = r.u64()?;
        let tip = r.oid()?;
        let baseline = SlabRef::read(&mut r)?;
        let nw = r.u32()? as usize;
        let np = r.u32()? as usize;
        if nw.saturating_add(np) >= MAX_SLABS
            || baseline.tier != Tier::Baseline
            || genesis > timestamp
            || baseline.end > timestamp
        {
            return Err(Error::Invalid("manifest bounds"));
        }
        let mut weekly = Vec::with_capacity(nw);
        let mut pyramid = Vec::with_capacity(np);
        let mut prev = genesis;
        for _ in 0..nw {
            let s = SlabRef::read(&mut r)?;
            if s.tier != Tier::WeeklyDelta || s.start != prev || s.end > baseline.end {
                return Err(Error::Invalid("weekly coverage gap"));
            }
            prev = s.end;
            weekly.push(s);
        }
        if prev != baseline.end {
            return Err(Error::Invalid("weekly coverage does not reach baseline"));
        }
        for _ in 0..np {
            let s = SlabRef::read(&mut r)?;
            if matches!(s.tier, Tier::Live | Tier::Baseline | Tier::WeeklyDelta)
                || s.start != prev
                || s.end > timestamp
            {
                return Err(Error::Invalid("pyramid coverage gap"));
            }
            prev = s.end;
            pyramid.push(s);
        }
        if prev != timestamp || !r.0.is_empty() {
            return Err(Error::Invalid("manifest coverage or trailing bytes"));
        }
        Ok(Self {
            generation,
            timestamp,
            genesis,
            tip,
            baseline,
            weekly,
            pyramid,
        })
    }
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let bytes = self
            .0
            .get(..n)
            .ok_or(Error::Invalid("truncated manifest"))?;
        self.0 = self
            .0
            .get(n..)
            .ok_or(Error::Invalid("truncated manifest"))?;
        Ok(bytes)
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| Error::Invalid("u32"))?,
        ))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| Error::Invalid("u64"))?,
        ))
    }
    fn oid(&mut self) -> Result<Oid20> {
        Oid20::from_slice(self.take(20)?).ok_or(Error::Invalid("OID"))
    }
}
