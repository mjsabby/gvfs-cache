use crate::arena::{append, push, reserve};
use crate::codec::{Oid, be32, be64, checksum, take, verify_checksum};
use crate::{Error, Result};

pub const MAX_SLABS: usize = 4096;
const MAGIC: &[u8; 8] = b"GMAN\0\0\0\x01";
const SLAB_LEN: usize = 80;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u32)]
pub enum Tier {
    L0 = 0,
    M15 = 15,
    H1 = 60,
    H6 = 360,
    H12 = 720,
    H24 = 1440,
    WeeklyDelta = 10080,
    Baseline = 10081,
}

impl Tier {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "15m" | "m15" => Ok(Self::M15),
            "1h" | "h1" => Ok(Self::H1),
            "6h" | "h6" => Ok(Self::H6),
            "12h" | "h12" => Ok(Self::H12),
            "24h" | "h24" => Ok(Self::H24),
            "168h" | "h168" => Ok(Self::Baseline),
            _ => Err(Error::Usage(
                "tier must be 15m, 1h, 6h, 12h, 24h, or 168h".into(),
            )),
        }
    }

    fn decode(value: u32) -> Result<Self> {
        match value {
            0 => Ok(Self::L0),
            15 => Ok(Self::M15),
            60 => Ok(Self::H1),
            360 => Ok(Self::H6),
            720 => Ok(Self::H12),
            1440 => Ok(Self::H24),
            10080 => Ok(Self::WeeklyDelta),
            10081 => Ok(Self::Baseline),
            _ => Err(Error::Invalid("manifest tier")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlabRef {
    pub id: Oid,
    pub tier: Tier,
    pub start: u64,
    pub end: u64,
    pub tip: Oid,
    pub count: u32,
    pub pack_len: u64,
    pub times: bool,
}

impl SlabRef {
    pub fn encode(self, out: &mut Vec<u8>) -> Result<()> {
        append(out, &self.id.0)?;
        append(out, &(self.tier as u32).to_be_bytes())?;
        append(out, &self.start.to_be_bytes())?;
        append(out, &self.end.to_be_bytes())?;
        append(out, &self.tip.0)?;
        append(out, &self.count.to_be_bytes())?;
        append(out, &self.pack_len.to_be_bytes())?;
        append(out, &u64::from(self.times).to_be_bytes())
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let value = Self {
            id: Oid::from_bytes(take(bytes, 0, 20)?)?,
            tier: Tier::decode(be32(bytes, 20)?)?,
            start: be64(bytes, 24)?,
            end: be64(bytes, 32)?,
            tip: Oid::from_bytes(take(bytes, 40, 20)?)?,
            count: be32(bytes, 60)?,
            pack_len: be64(bytes, 64)?,
            times: be64(bytes, 72)? & 1 != 0,
        };
        if be64(bytes, 72)? > 1 {
            return Err(Error::Invalid("manifest slab flags"));
        }
        if value.start > value.end
            || value.end > i64::MAX as u64
            || value.pack_len < 32
            || value.id == Oid::ZERO
        {
            return Err(Error::Invalid("manifest slab bounds"));
        }
        Ok(value)
    }
}

#[derive(Clone, Debug)]
pub struct Manifest {
    pub generation: u64,
    pub tip: Oid,
    pub baseline: SlabRef,
    pub weekly: Vec<SlabRef>,
    pub pyramid: Vec<SlabRef>,
}

impl Manifest {
    pub fn genesis(baseline: SlabRef) -> Result<Self> {
        Ok(Self {
            generation: 1,
            tip: baseline.tip,
            baseline,
            weekly: reserve(MAX_SLABS)?,
            pyramid: reserve(MAX_SLABS)?,
        })
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let capacity = 64usize
            .checked_add(MAX_SLABS.saturating_add(1).saturating_mul(SLAB_LEN))
            .and_then(|n| n.checked_add(20))
            .ok_or(Error::Capacity("manifest"))?;
        let mut out = reserve(capacity)?;
        append(&mut out, MAGIC)?;
        append(&mut out, &self.generation.to_be_bytes())?;
        append(&mut out, &self.tip.0)?;
        append(&mut out, &(self.weekly.len() as u32).to_be_bytes())?;
        append(&mut out, &(self.pyramid.len() as u32).to_be_bytes())?;
        append(&mut out, &[0; 4])?;
        self.baseline.encode(&mut out)?;
        for slab in self.weekly.iter().chain(&self.pyramid) {
            slab.encode(&mut out)?;
        }
        let sum = checksum(&out);
        append(&mut out, &sum)?;
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let body = verify_checksum(bytes)?;
        if take(body, 0, 8)? != MAGIC {
            return Err(Error::Invalid("manifest version"));
        }
        let weekly = be32(body, 36)? as usize;
        let pyramid = be32(body, 40)? as usize;
        let total = weekly
            .checked_add(pyramid)
            .ok_or(Error::Invalid("manifest count"))?;
        if total > MAX_SLABS
            || body.len()
                != 48usize.saturating_add(total.saturating_add(1).saturating_mul(SLAB_LEN))
        {
            return Err(Error::Invalid("manifest length"));
        }
        let baseline = SlabRef::decode(take(body, 48, SLAB_LEN)?)?;
        let mut manifest = Self::genesis(baseline)?;
        manifest.generation = be64(body, 8)?;
        manifest.tip = Oid::from_bytes(take(body, 16, 20)?)?;
        let mut cursor = 48usize.saturating_add(SLAB_LEN);
        for i in 0..total {
            let entry = SlabRef::decode(take(body, cursor, SLAB_LEN)?)?;
            if i < weekly {
                push(&mut manifest.weekly, entry)?;
            } else {
                push(&mut manifest.pyramid, entry)?;
            }
            cursor = cursor.saturating_add(SLAB_LEN);
        }
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<()> {
        if self.generation == 0
            || self.baseline.tier != Tier::Baseline
            || self.weekly.len().saturating_add(self.pyramid.len()) > MAX_SLABS
        {
            return Err(Error::Invalid("manifest invariant"));
        }
        let mut end = None;
        for slab in &self.weekly {
            if slab.tier != Tier::WeeklyDelta
                || end.is_some_and(|prior| slab.start != prior)
                || slab.start > slab.end
            {
                return Err(Error::Invalid("weekly coverage"));
            }
            end = Some(slab.end);
        }
        if end.is_some_and(|value| value != self.baseline.end) {
            return Err(Error::Invalid("weekly baseline coverage"));
        }
        let mut end = self.baseline.end;
        for slab in &self.pyramid {
            if slab.start != end
                || slab.end <= slab.start
                || !(Tier::M15..=Tier::H24).contains(&slab.tier)
            {
                return Err(Error::Invalid("pyramid coverage"));
            }
            end = slab.end;
        }
        let tip = self
            .pyramid
            .last()
            .map_or(self.baseline.tip, |slab| slab.tip);
        if tip != self.tip {
            return Err(Error::Invalid("manifest tip"));
        }
        Ok(())
    }

    /// Expired cursors fall back to the baseline instead of silently missing history.
    pub fn select(&self, timestamp: u64, out: &mut Vec<SlabRef>) -> Result<()> {
        out.clear();
        let floor = self
            .weekly
            .first()
            .map_or(self.baseline.end, |slab| slab.start);
        if timestamp == 0 || timestamp < floor {
            push(out, self.baseline)?;
        } else {
            for slab in &self.weekly {
                if slab.end > timestamp {
                    push(out, *slab)?;
                }
            }
        }
        for slab in &self.pyramid {
            if timestamp == 0 || timestamp < floor || slab.end > timestamp {
                push(out, *slab)?;
            }
        }
        Ok(())
    }

    pub fn cursor(&self) -> u64 {
        self.pyramid
            .last()
            .map_or(self.baseline.end, |slab| slab.end)
    }
}
