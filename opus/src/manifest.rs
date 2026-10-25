//! `manifest.bin`: the one mutable object in the store, replaced by compare-and-swap.
//!
//! It names the sealed slabs and the commit they reach:
//!
//! * `baseline` — the full history up to some tip (cold clones start from it);
//! * `weekly` — the last weeks' net-new objects (warm clients that missed a baseline);
//! * `pyramid` — 15m / 1h / 6h / 12h / 24h slabs since the baseline, oldest first;
//! * `retained` — finer slabs a rollup replaced, still listed for a while so that a warm
//!   client is answered from exactly where its last pack ended (never in a cold plan).
//!
//! ```text
//! "GVDMANI1" | version u32 | generation u64 | tip [20] | max_ts u64 | slab count u32
//! slab*: role u8 | tier u8 | id_len u16 | id | start_ts u64 | end_ts u64 | have [20]
//!        | want [20] | objects u32 | pack_len u64 | index_len u64 | pack_sha [20]
//! SHA-256 of everything before it [32]
//! ```

use crate::slab::{SlabMeta, Tier};
use crate::util::{Context, Oid, Result, le_u16, le_u32, le_u64, sub};

pub const MANIFEST_KEY: &str = "manifest.bin";
const MAGIC: &[u8; 8] = b"GVDMANI1";
/// Version 2 added `retained`; version 1 manifests still decode.
const VERSION: u32 = 2;

const ROLE_BASELINE: u8 = 0;
const ROLE_WEEKLY: u8 = 1;
const ROLE_PYRAMID: u8 = 2;
const ROLE_RETAINED: u8 = 3;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    /// Incremented by every publication.
    pub generation: u64,
    /// The commit every sealed slab together reaches (`last_15m_tip_oid`).
    pub tip: Oid,
    /// The newest `end_ts` of any sealed slab: the timestamp of `tip`.
    pub max_ts: u64,
    pub baseline: SlabMeta,
    pub weekly: Vec<SlabMeta>,
    pub pyramid: Vec<SlabMeta>,
    pub retained: Vec<SlabMeta>,
}

impl Manifest {
    /// Every slab: baseline, weekly deltas, pyramid (each oldest first), then retained.
    pub fn slabs(&self) -> impl Iterator<Item = &SlabMeta> {
        std::iter::once(&self.baseline)
            .chain(self.weekly.iter())
            .chain(self.pyramid.iter())
            .chain(self.retained.iter())
    }

    /// The commits of every sealed tip, newest first: `have` candidates for fetches (an
    /// upstream that lost the newest one after a force push still knows an older one).
    pub fn known_tips(&self) -> Vec<Oid> {
        let mut tips = vec![self.tip];
        for s in self
            .pyramid
            .iter()
            .rev()
            .chain(self.weekly.iter().rev())
            .chain(std::iter::once(&self.baseline))
        {
            if !s.want.is_zero() && !tips.contains(&s.want) {
                tips.push(s.want);
            }
        }
        tips
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.extend_from_slice(&self.generation.to_le_bytes());
        out.extend_from_slice(&self.tip.0);
        out.extend_from_slice(&self.max_ts.to_le_bytes());
        let count = 1usize
            .saturating_add(self.weekly.len())
            .saturating_add(self.pyramid.len())
            .saturating_add(self.retained.len());
        out.extend_from_slice(&crate::util::u32_of(count).to_le_bytes());
        let roles = std::iter::once((ROLE_BASELINE, &self.baseline))
            .chain(self.weekly.iter().map(|s| (ROLE_WEEKLY, s)))
            .chain(self.pyramid.iter().map(|s| (ROLE_PYRAMID, s)))
            .chain(self.retained.iter().map(|s| (ROLE_RETAINED, s)));
        for (role, s) in roles {
            out.push(role);
            out.push(s.tier as u8);
            let id = s.id.as_bytes();
            out.extend_from_slice(&u16::try_from(id.len()).unwrap_or(0).to_le_bytes());
            out.extend_from_slice(id);
            out.extend_from_slice(&s.start_ts.to_le_bytes());
            out.extend_from_slice(&s.end_ts.to_le_bytes());
            out.extend_from_slice(&s.have.0);
            out.extend_from_slice(&s.want.0);
            out.extend_from_slice(&s.objects.to_le_bytes());
            out.extend_from_slice(&s.pack_len.to_le_bytes());
            out.extend_from_slice(&s.index_len.to_le_bytes());
            out.extend_from_slice(&s.pack_sha);
        }
        let digest = ring::digest::digest(&ring::digest::SHA256, &out);
        out.extend_from_slice(digest.as_ref());
        out
    }

    pub fn decode(b: &[u8]) -> Result<Manifest> {
        let body_len = b.len().checked_sub(32).ctx("manifest too short")?;
        let (body, digest) = b.split_at_checked(body_len).ctx("manifest too short")?;
        if ring::digest::digest(&ring::digest::SHA256, body).as_ref() != digest {
            crate::bail!("manifest checksum mismatch");
        }
        if body.get(..8) != Some(&MAGIC[..]) || !matches!(le_u32(body, 8), Some(1..=VERSION)) {
            crate::bail!("not a gvfs-daemon manifest (or an unsupported version)");
        }
        let generation = le_u64(body, 12).ctx("manifest header")?;
        let tip =
            Oid::from_slice(sub(body, 20, 20).ctx("manifest header")?).ctx("manifest header")?;
        let max_ts = le_u64(body, 40).ctx("manifest header")?;
        let count = le_u32(body, 48).ctx("manifest header")?;
        let mut at = 52usize;
        let mut baseline = None;
        let mut weekly = Vec::new();
        let mut pyramid = Vec::new();
        let mut retained = Vec::new();
        for _ in 0..count {
            let role = *body.get(at).ctx("truncated manifest")?;
            let tier = Tier::from_u8(*body.get(at.wrapping_add(1)).ctx("truncated manifest")?)
                .ctx("bad tier in manifest")?;
            let id_len = usize::from(le_u16(body, at.wrapping_add(2)).ctx("truncated manifest")?);
            let id_at = at.wrapping_add(4);
            let id = std::str::from_utf8(sub(body, id_at, id_len).ctx("truncated manifest")?)
                .map_err(|_| crate::err!("bad slab id"))?;
            if !valid_id(id) {
                crate::bail!("bad slab id {id:?} in manifest");
            }
            let f = id_at.checked_add(id_len).ctx("truncated manifest")?;
            let oid_at = |o: usize| -> Result<Oid> {
                Oid::from_slice(sub(body, f.wrapping_add(o), 20).ctx("truncated manifest")?)
                    .ctx("bad oid")
            };
            let meta = SlabMeta {
                id: id.to_string(),
                tier,
                start_ts: le_u64(body, f).ctx("truncated manifest")?,
                end_ts: le_u64(body, f.wrapping_add(8)).ctx("truncated manifest")?,
                have: oid_at(16)?,
                want: oid_at(36)?,
                objects: le_u32(body, f.wrapping_add(56)).ctx("truncated manifest")?,
                pack_len: le_u64(body, f.wrapping_add(60)).ctx("truncated manifest")?,
                index_len: le_u64(body, f.wrapping_add(68)).ctx("truncated manifest")?,
                pack_sha: sub(body, f.wrapping_add(76), 20)
                    .and_then(|s| s.try_into().ok())
                    .ctx("truncated manifest")?,
            };
            at = f.checked_add(96).ctx("truncated manifest")?;
            match role {
                ROLE_BASELINE if baseline.is_none() => baseline = Some(meta),
                ROLE_WEEKLY => weekly.push(meta),
                ROLE_PYRAMID => pyramid.push(meta),
                ROLE_RETAINED => retained.push(meta),
                _ => crate::bail!("bad slab role {role} in manifest"),
            }
        }
        if at != body.len() {
            crate::bail!("trailing bytes in manifest");
        }
        Ok(Manifest {
            generation,
            tip,
            max_ts,
            baseline: baseline.ctx("manifest has no baseline")?,
            weekly,
            pyramid,
            retained,
        })
    }

    /// Human-readable dump (`gvfs-daemon inspect manifest`).
    pub fn describe(&self) -> String {
        let mut s = format!(
            "generation {}  tip {}  max_ts {}\n",
            self.generation, self.tip, self.max_ts
        );
        let mut row = |role: &str, m: &SlabMeta| {
            s.push_str(&format!(
                "  {role:8} {:5} {:>12} objects {:>12}  ({}..{}]  {}  {}..{}\n",
                m.tier.name(),
                m.objects,
                crate::util::human_bytes(m.pack_len),
                m.start_ts,
                m.end_ts,
                m.id,
                short(&m.have),
                short(&m.want)
            ));
        };
        row("baseline", &self.baseline);
        for w in &self.weekly {
            row("weekly", w);
        }
        for p in &self.pyramid {
            row("pyramid", p);
        }
        if !self.retained.is_empty() {
            let bytes: u64 = self.retained.iter().map(|r| r.pack_len).sum();
            let mut tiers: Vec<String> = Vec::new();
            for t in Tier::ALL {
                let n = self.retained.iter().filter(|r| r.tier == t).count();
                if n > 0 {
                    tiers.push(format!("{n} {}", t.name()));
                }
            }
            s.push_str(&format!(
                "  retained {} slabs ({}), {} for warm clients\n",
                self.retained.len(),
                tiers.join(", "),
                crate::util::human_bytes(bytes)
            ));
        }
        s
    }
}

fn short(o: &Oid) -> String {
    let h = o.to_string();
    h.get(..10).unwrap_or(&h).to_string()
}

/// Slab ids become object-store keys and file names: keep them boring.
pub fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(tier: Tier, end: u64) -> SlabMeta {
        SlabMeta {
            id: SlabMeta::make_id(tier, end, &[end as u8; 20]),
            tier,
            start_ts: end.saturating_sub(900),
            end_ts: end,
            have: Oid([1; 20]),
            want: Oid([end as u8; 20]),
            objects: 42,
            pack_len: 1000,
            index_len: 200,
            pack_sha: [end as u8; 20],
        }
    }

    #[test]
    fn roundtrip_and_integrity() {
        let m = Manifest {
            generation: 7,
            tip: Oid([9; 20]),
            max_ts: 5000,
            baseline: meta(Tier::Base, 1000),
            weekly: vec![meta(Tier::Week, 2000)],
            pyramid: vec![meta(Tier::H1, 3000), meta(Tier::M15, 4000)],
            retained: vec![meta(Tier::M15, 2900), meta(Tier::M15, 3000)],
        };
        let b = m.encode();
        assert_eq!(Manifest::decode(&b).unwrap(), m);
        let mut bad = b.clone();
        bad[30] ^= 1;
        assert!(Manifest::decode(&bad).is_err());
        assert_eq!(m.known_tips()[0], Oid([9; 20]));
        assert!(
            m.describe().contains("pyramid") && m.describe().contains("retained 2 slabs (2 m15)")
        );
        assert_eq!(m.slabs().count(), 6);
        // Retained slabs are not fetch candidates: their tips are behind the pyramid's.
        assert!(!m.known_tips().contains(&Oid([0x54; 20])));
        assert!(!valid_id("../x"));
    }
}
