//! Unified Manifest & Two-Track Routing (`UnifiedManifest`, PDF pp. 133–168).
//!
//! Coordinates Pipeline Compaction (`gvfs-daemon pipeline`), the Remote Edge Server
//! (`gvfs-daemon edge` / `serve`), and the Localhost Workstation Proxy (`gvfs-daemon proxy`):
//!
//! - **Track A (Cold Clone: `lastPackTimestamp == None | Some(0)`)**:
//!   Selects `weekly_baseline` (168h full snapshot) + `sub_week_pyramid` slabs newer than
//!   the baseline + `< 15m` `l0_ram_slab`, which are then spliced into **1 single pack**!
//! - **Track B (Warm Workstation / Agent: `lastPackTimestamp == Some(ts)`)**:
//!   Skips the 4 GiB `weekly_baseline`! Selects any `weekly_deltas` (~80 MiB "Aha" deltas)
//!   with `end_ts > ts`, plus `sub_week_pyramid` slabs with `end_ts > ts`, plus `l0_ram_slab`,
//!   which are spliced into **1 single pack**!

use std::sync::Arc;

use crate::buf::{ByteWriter, Overflow, be16, be32, be64, sub};
use crate::oid::Oid20;
use crate::slab::{Slab, Tier};

pub const MANIFEST_MAGIC: &[u8; 8] = b"GMAN\x00\x00\x00\x01";

/// Compact descriptor for a slab stored in `manifest.bin`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlabManifestEntry {
    pub id: String,
    pub tier: Tier,
    pub start_ts: u64,
    pub end_ts: u64,
    pub have_commit: Oid20,
    pub tip_commit: Oid20,
    pub object_count: u32,
    pub pack_byte_len: u64,
}

impl SlabManifestEntry {
    pub fn from_slab(slab: &Slab) -> Self {
        Self {
            id: slab.id.clone(),
            tier: slab.tier,
            start_ts: slab.start_ts,
            end_ts: slab.end_ts,
            have_commit: slab.have_commit,
            tip_commit: slab.tip_commit,
            object_count: slab.object_count(),
            pack_byte_len: slab.pack_byte_len,
        }
    }

    fn encode(&self, w: &mut ByteWriter<'_>) -> Result<(), Overflow> {
        let id_bytes = self.id.as_bytes();
        let id_len = u16::try_from(id_bytes.len()).map_err(|_| Overflow)?;
        w.put_u16_be(id_len)?;
        w.put(id_bytes)?;
        w.put_u16_be(self.tier.code())?;
        w.put_u64_be(self.start_ts)?;
        w.put_u64_be(self.end_ts)?;
        w.put(&self.have_commit.0)?;
        w.put(&self.tip_commit.0)?;
        w.put_u32_be(self.object_count)?;
        w.put_u64_be(self.pack_byte_len)?;
        Ok(())
    }

    fn decode(data: &[u8], at: &mut usize) -> Option<Self> {
        let id_len = usize::from(be16(data, *at)?);
        *at = at.checked_add(2)?;
        let id_raw = sub(data, *at, id_len)?;
        let id = std::str::from_utf8(id_raw).ok()?.to_string();
        *at = at.checked_add(id_len)?;
        let tier = Tier::from_code(be16(data, *at)?)?;
        *at = at.checked_add(2)?;
        let start_ts = be64(data, *at)?;
        *at = at.checked_add(8)?;
        let end_ts = be64(data, *at)?;
        *at = at.checked_add(8)?;
        let have_commit = Oid20::from_slice(sub(data, *at, 20)?)?;
        *at = at.checked_add(20)?;
        let tip_commit = Oid20::from_slice(sub(data, *at, 20)?)?;
        *at = at.checked_add(20)?;
        let object_count = be32(data, *at)?;
        *at = at.checked_add(4)?;
        let pack_byte_len = be64(data, *at)?;
        *at = at.checked_add(8)?;
        Some(Self {
            id,
            tier,
            start_ts,
            end_ts,
            have_commit,
            tip_commit,
            object_count,
            pack_byte_len,
        })
    }
}

/// Serialized representation of `manifest.bin` before hydrating `Arc<Slab>` handles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestDescriptor {
    pub version_seq: u64,
    pub last_15m_tip_oid: Oid20,
    pub weekly_baseline: SlabManifestEntry,
    pub weekly_deltas: Vec<SlabManifestEntry>,
    pub sub_week_pyramid: Vec<SlabManifestEntry>,
}

impl ManifestDescriptor {
    pub fn encode(&self) -> Result<Vec<u8>, Overflow> {
        let mut buf = vec![0u8; 64 * 1024];
        let mut w = ByteWriter::new(&mut buf);
        w.put(MANIFEST_MAGIC)?;
        w.put_u64_be(self.version_seq)?;
        w.put(&self.last_15m_tip_oid.0)?;
        self.weekly_baseline.encode(&mut w)?;
        w.put_u32_be(u32::try_from(self.weekly_deltas.len()).map_err(|_| Overflow)?)?;
        for d in &self.weekly_deltas {
            d.encode(&mut w)?;
        }
        w.put_u32_be(u32::try_from(self.sub_week_pyramid.len()).map_err(|_| Overflow)?)?;
        for s in &self.sub_week_pyramid {
            s.encode(&mut w)?;
        }
        let len = w.len();
        buf.truncate(len);
        Ok(buf)
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.get(..8) != Some(&MANIFEST_MAGIC[..]) {
            return None;
        }
        let version_seq = be64(data, 8)?;
        let last_15m_tip_oid = Oid20::from_slice(sub(data, 16, 20)?)?;
        let mut at = 36usize;
        let weekly_baseline = SlabManifestEntry::decode(data, &mut at)?;
        let deltas_len = usize::try_from(be32(data, at)?).ok()?;
        at = at.checked_add(4)?;
        let mut weekly_deltas = Vec::with_capacity(deltas_len.min(1024));
        for _ in 0..deltas_len {
            weekly_deltas.push(SlabManifestEntry::decode(data, &mut at)?);
        }
        let pyramid_len = usize::try_from(be32(data, at)?).ok()?;
        at = at.checked_add(4)?;
        let mut sub_week_pyramid = Vec::with_capacity(pyramid_len.min(1024));
        for _ in 0..pyramid_len {
            sub_week_pyramid.push(SlabManifestEntry::decode(data, &mut at)?);
        }
        if at != data.len() {
            return None;
        }
        Some(Self {
            version_seq,
            last_15m_tip_oid,
            weekly_baseline,
            weekly_deltas,
            sub_week_pyramid,
        })
    }
}

/// Live in-memory manifest holding `Arc<Slab>` handles (PDF p. 160).
#[derive(Debug, Clone)]
pub struct UnifiedManifest {
    pub version_seq: u64,
    pub last_15m_tip_oid: Oid20,
    /// 4 GiB full snapshot (Track A: Cold Clones)
    pub weekly_baseline: Arc<Slab>,
    /// ~80 MiB "Aha" weekly rollups (Track B: Warm Workstations)
    pub weekly_deltas: Vec<Arc<Slab>>,
    /// Active 24h, 12h, 6h, 1h, 15m slabs
    pub sub_week_pyramid: Vec<Arc<Slab>>,
}

impl UnifiedManifest {
    pub fn to_descriptor(&self) -> ManifestDescriptor {
        ManifestDescriptor {
            version_seq: self.version_seq,
            last_15m_tip_oid: self.last_15m_tip_oid,
            weekly_baseline: SlabManifestEntry::from_slab(&self.weekly_baseline),
            weekly_deltas: self
                .weekly_deltas
                .iter()
                .map(|s| SlabManifestEntry::from_slab(s))
                .collect(),
            sub_week_pyramid: self
                .sub_week_pyramid
                .iter()
                .map(|s| SlabManifestEntry::from_slab(s))
                .collect(),
        }
    }

    /// Selects the exact slabs for `GET /gvfs/prefetch?lastPackTimestamp=client_ts`
    /// and includes the `< 15m` in-memory L0 slab so the client gets **1 spliced pack**
    /// (PDF pp. 160–161).
    pub fn select_slabs_for_prefetch(
        &self,
        client_last_ts: Option<u64>,
        l0_ram_slab: Option<Arc<Slab>>,
    ) -> Vec<Arc<Slab>> {
        let mut selected = Vec::new();

        // Earliest timestamp covered by our retained warm-track history (weekly deltas + pyramid).
        let earliest_warm_ts = self
            .weekly_deltas
            .first()
            .map(|d| d.start_ts)
            .or_else(|| self.sub_week_pyramid.first().map(|s| s.start_ts))
            .unwrap_or(self.weekly_baseline.end_ts);

        match client_last_ts {
            None | Some(0) => {
                // COLD CLONE (Track A): Give 168h Full Baseline + this week's pyramid + <15m L0 RAM slab
                selected.push(Arc::clone(&self.weekly_baseline));
                for s in &self.sub_week_pyramid {
                    if s.end_ts > self.weekly_baseline.end_ts {
                        selected.push(Arc::clone(s));
                    }
                }
            }
            Some(ts) if ts < earliest_warm_ts && ts < self.weekly_baseline.end_ts => {
                // Client is older than our retained warm-delta window; fall back to Track A baseline.
                selected.push(Arc::clone(&self.weekly_baseline));
                for s in &self.sub_week_pyramid {
                    if s.end_ts > self.weekly_baseline.end_ts {
                        selected.push(Arc::clone(s));
                    }
                }
            }
            Some(ts) => {
                // WARM WORKSTATION / AGENT (Track B):
                // Never re-download the 4 GiB baseline! Use the ~80 MiB Weekly "Aha" Deltas
                // for any missed weeks, plus sub-week pyramid slabs > ts.
                for w_delta in &self.weekly_deltas {
                    if w_delta.end_ts > ts {
                        selected.push(Arc::clone(w_delta));
                    }
                }
                for s in &self.sub_week_pyramid {
                    if s.end_ts > ts && s.start_ts >= self.weekly_baseline.end_ts {
                        selected.push(Arc::clone(s));
                    }
                }
            }
        }

        // Append the `< 15m` live in-memory slab if new commits exist since the last 15m cron
        // and the client hasn't already fetched up to `l0.end_ts`.
        if let Some(l0) = l0_ram_slab {
            if !l0.is_empty() && client_last_ts.is_none_or(|ts| l0.end_ts > ts) {
                selected.push(l0);
            }
        }

        // Order selected slabs from newest to oldest so that if an object was modified or
        // duplicated across slabs, the first encounter during deduplication is deterministic.
        selected
    }

    /// Returns all active slabs in search order for `POST /gvfs/objects` point lookups:
    /// 1. `< 15m` L0 RAM Slab (if present)
    /// 2. Sub-week pyramid slabs (newest to oldest)
    /// 3. Weekly Full Baseline (`168h`, covering the entire historical repository)
    pub fn active_lookup_slabs(&self, l0_ram_slab: Option<Arc<Slab>>) -> Vec<Arc<Slab>> {
        let mut slabs = Vec::with_capacity(self.sub_week_pyramid.len().saturating_add(2));
        if let Some(l0) = l0_ram_slab {
            if !l0.is_empty() {
                slabs.push(l0);
            }
        }
        for s in self.sub_week_pyramid.iter().rev() {
            slabs.push(Arc::clone(s));
        }
        slabs.push(Arc::clone(&self.weekly_baseline));
        slabs
    }

    /// Fallback `have` commit OIDs from the previous 24h or 168h slabs (PDF p. 169:
    /// used when upstream ADO gets force-pushed and the old 15m tip was garbage-collected).
    pub fn fallback_have_commits(&self) -> Vec<Oid20> {
        let mut haves = Vec::new();
        for s in self.sub_week_pyramid.iter().rev() {
            if s.tier >= Tier::H24 && !s.tip_commit.is_zero() && !haves.contains(&s.tip_commit) {
                haves.push(s.tip_commit);
            }
        }
        if !self.weekly_baseline.tip_commit.is_zero()
            && !haves.contains(&self.weekly_baseline.tip_commit)
        {
            haves.push(self.weekly_baseline.tip_commit);
        }
        haves
    }

    /// All slab IDs referenced by this manifest (used by orphan GC sweep, PDF p. 169).
    pub fn referenced_slab_ids(&self) -> Vec<&str> {
        let mut ids = Vec::with_capacity(
            1usize
                .saturating_add(self.weekly_deltas.len())
                .saturating_add(self.sub_week_pyramid.len()),
        );
        ids.push(self.weekly_baseline.id.as_str());
        for d in &self.weekly_deltas {
            ids.push(d.id.as_str());
        }
        for s in &self.sub_week_pyramid {
            ids.push(s.id.as_str());
        }
        ids
    }
}
