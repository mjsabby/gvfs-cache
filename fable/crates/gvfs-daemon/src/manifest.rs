//! `manifest.bin`: the one piece of mutable state, replaced atomically (compare-and-swap on
//! its generation) by the pipeline and polled by every `serve` instance.
//!
//! Two tracks keep warm clients from ever re-downloading the baseline:
//!
//! * **Track A — cold clones** (`lastPackTimestamp` absent or 0, or older than the retained
//!   weekly deltas): the baseline plus every pyramid slab published after it.
//! * **Track B — warm clients**: the weekly "aha" deltas and pyramid slabs newer than the
//!   client's timestamp, never the baseline.
//!
//! Slabs are pairwise disjoint by construction (each ingest deduplicates against everything
//! published before it, rollups concatenate, the baseline is rebuilt from the tip), so any
//! selection splices into one valid pack.
//!
//! A rollup below the pipeline's `--leaf-tier` keeps its *leaves* — the original slabs it
//! replaced — so a warm client whose cursor falls inside the rollup's window receives only
//! the leaves it lacks instead of the whole rollup (bandwidth), while clients outside the
//! window still get the superset rollup (bounded pack count, tombstoning).

use core::fmt;

use sha1::{Digest, Sha1};

use crate::oid::Oid;

pub const MANIFEST_MAGIC: [u8; 4] = *b"GVMF";
/// Version 2 adds the leaf lists; version 1 files still decode (no leaves).
pub const MANIFEST_VERSION: u32 = 2;
const HEADER_LEN: usize = 64;
const SLAB_REF_LEN: usize = 80;
/// Most slabs a manifest (or a prefetch selection) can hold.
pub const MAX_SLABS: usize = 1024;
pub const MAX_SELECTION: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Tier {
    /// The in-memory live slab of a `serve` process (never in a manifest).
    L0 = 0,
    M15 = 1,
    H1 = 2,
    H6 = 3,
    H12 = 4,
    H24 = 5,
    /// Weekly "aha" delta: one week of net-new objects for warm clients.
    Weekly = 6,
    /// Full snapshot for cold clones.
    Baseline = 7,
}

impl Tier {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Self::L0,
            1 => Self::M15,
            2 => Self::H1,
            3 => Self::H6,
            4 => Self::H12,
            5 => Self::H24,
            6 => Self::Weekly,
            7 => Self::Baseline,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::L0 => "l0",
            Self::M15 => "m15",
            Self::H1 => "h1",
            Self::H6 => "h6",
            Self::H12 => "h12",
            Self::H24 => "h24",
            Self::Weekly => "h168",
            Self::Baseline => "baseline",
        }
    }

    /// Rollup tiers in ascending order (what `pipeline --tier` cascades through).
    pub const ROLLUPS: [Tier; 4] = [Tier::H1, Tier::H6, Tier::H12, Tier::H24];
}

impl fmt::Display for Tier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A published, immutable slab.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlabRef {
    pub seq: u64,
    pub tier: Tier,
    /// First and last ingest time covered (unix seconds). The prefetch timestamp of a
    /// spliced response is the max `end_ts` of its members.
    pub start_ts: u64,
    pub end_ts: u64,
    /// Branch tip whose objects end this slab.
    pub tip: Oid,
    pub object_count: u32,
    pub pack_len: u64,
    pub idx_len: u64,
    pub mphf_len: u64,
}

impl SlabRef {
    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.seq.to_le_bytes());
        out.extend_from_slice(&self.start_ts.to_le_bytes());
        out.extend_from_slice(&self.end_ts.to_le_bytes());
        out.extend_from_slice(&self.tip.0);
        out.extend_from_slice(&self.object_count.to_le_bytes());
        out.extend_from_slice(&[self.tier as u8, 0, 0, 0]);
        out.extend_from_slice(&self.pack_len.to_le_bytes());
        out.extend_from_slice(&self.idx_len.to_le_bytes());
        out.extend_from_slice(&self.mphf_len.to_le_bytes());
        out.extend_from_slice(&[0u8; 4]); // reserved: 80 bytes per record
    }

    fn decode(b: &[u8]) -> Result<Self, ManifestError> {
        if b.len() < SLAB_REF_LEN {
            return Err(ManifestError::Truncated);
        }
        let u64_at = |at: usize| -> u64 {
            b.get(at..at.saturating_add(8))
                .and_then(|s| s.try_into().ok())
                .map(u64::from_le_bytes)
                .unwrap_or(0)
        };
        let u32_at = |at: usize| -> u32 {
            b.get(at..at.saturating_add(4))
                .and_then(|s| s.try_into().ok())
                .map(u32::from_le_bytes)
                .unwrap_or(0)
        };
        Ok(Self {
            seq: u64_at(0),
            start_ts: u64_at(8),
            end_ts: u64_at(16),
            tip: Oid::from_slice(b.get(24..44).unwrap_or(&[])).ok_or(ManifestError::Truncated)?,
            object_count: u32_at(44),
            tier: Tier::from_u8(b.get(48).copied().unwrap_or(255)).ok_or(ManifestError::BadTier)?,
            pack_len: u64_at(52),
            idx_len: u64_at(60),
            mphf_len: u64_at(68),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestError {
    BadMagic,
    BadVersion,
    Truncated,
    BadChecksum,
    BadTier,
    TooManySlabs,
}

impl fmt::Display for ManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::BadMagic => "bad manifest magic",
            Self::BadVersion => "unsupported manifest version",
            Self::Truncated => "truncated manifest",
            Self::BadChecksum => "manifest checksum mismatch",
            Self::BadTier => "unknown slab tier",
            Self::TooManySlabs => "too many slabs",
        })
    }
}

impl std::error::Error for ManifestError {}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Manifest {
    /// Compare-and-swap token: every publish increments it.
    pub generation: u64,
    pub updated_ts: u64,
    /// Branch tip covered by the published slabs (next ingest starts here).
    pub tip: Oid,
    pub next_seq: u64,
    pub baseline: Option<SlabRef>,
    /// Oldest first.
    pub weekly: Vec<SlabRef>,
    /// Oldest first; tiers mixed (e.g. `h24 h24 h6 h1 h1 m15`).
    pub pyramid: Vec<SlabRef>,
    /// `leaves[i]` are the retained original slabs of `pyramid[i]` (empty for an m15 or a
    /// rollup at/above the leaf tier). Always `leaves.len() == pyramid.len()`.
    pub leaves: Vec<Vec<SlabRef>>,
}

impl Manifest {
    pub fn encode(&self) -> Vec<u8> {
        let n = 1usize
            .saturating_add(self.weekly.len())
            .saturating_add(self.pyramid.len());
        let mut out = Vec::with_capacity(
            HEADER_LEN
                .saturating_add(n.saturating_mul(SLAB_REF_LEN))
                .saturating_add(20),
        );
        out.extend_from_slice(&MANIFEST_MAGIC);
        out.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        out.extend_from_slice(&self.generation.to_le_bytes());
        out.extend_from_slice(&self.updated_ts.to_le_bytes());
        out.extend_from_slice(&self.tip.0);
        out.extend_from_slice(&self.next_seq.to_le_bytes());
        out.extend_from_slice(&[u8::from(self.baseline.is_some()), 0, 0, 0]);
        out.extend_from_slice(&(self.weekly.len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.pyramid.len() as u32).to_le_bytes());
        out.resize(HEADER_LEN, 0);
        if let Some(b) = &self.baseline {
            b.encode(&mut out);
        }
        for s in &self.weekly {
            s.encode(&mut out);
        }
        for s in &self.pyramid {
            s.encode(&mut out);
        }
        // Version 2: one leaf list per pyramid slab.
        for i in 0..self.pyramid.len() {
            let leaves = self.leaves.get(i).map(Vec::as_slice).unwrap_or(&[]);
            out.extend_from_slice(&(leaves.len() as u32).to_le_bytes());
            for l in leaves {
                l.encode(&mut out);
            }
        }
        let digest: [u8; 20] = Sha1::digest(&out).into();
        out.extend_from_slice(&digest);
        out
    }

    pub fn decode(b: &[u8]) -> Result<Self, ManifestError> {
        if b.get(..4) != Some(&MANIFEST_MAGIC[..]) {
            return Err(ManifestError::BadMagic);
        }
        let body_len = b.len().checked_sub(20).ok_or(ManifestError::Truncated)?;
        if body_len < HEADER_LEN {
            return Err(ManifestError::Truncated);
        }
        let digest: [u8; 20] = Sha1::digest(b.get(..body_len).unwrap_or(&[])).into();
        if b.get(body_len..) != Some(&digest[..]) {
            return Err(ManifestError::BadChecksum);
        }
        let u64_at = |at: usize| -> u64 {
            b.get(at..at.saturating_add(8))
                .and_then(|s| s.try_into().ok())
                .map(u64::from_le_bytes)
                .unwrap_or(0)
        };
        let u32_at = |at: usize| -> u32 {
            b.get(at..at.saturating_add(4))
                .and_then(|s| s.try_into().ok())
                .map(u32::from_le_bytes)
                .unwrap_or(0)
        };
        let version = u32_at(4);
        if version != 1 && version != MANIFEST_VERSION {
            return Err(ManifestError::BadVersion);
        }
        let has_baseline = b.get(52).copied().unwrap_or(0) != 0;
        let n_weekly = u32_at(56) as usize;
        let n_pyramid = u32_at(60) as usize;
        if n_weekly.saturating_add(n_pyramid) > MAX_SLABS {
            return Err(ManifestError::TooManySlabs);
        }
        let mut at = HEADER_LEN;
        let next = |at: &mut usize| -> Result<SlabRef, ManifestError> {
            let r = SlabRef::decode(b.get(*at..body_len).unwrap_or(&[]))?;
            *at = at.saturating_add(SLAB_REF_LEN);
            Ok(r)
        };
        let baseline = if has_baseline {
            Some(next(&mut at)?)
        } else {
            None
        };
        let mut weekly = Vec::with_capacity(n_weekly);
        for _ in 0..n_weekly {
            weekly.push(next(&mut at)?);
        }
        let mut pyramid = Vec::with_capacity(n_pyramid);
        for _ in 0..n_pyramid {
            pyramid.push(next(&mut at)?);
        }
        let mut leaves = vec![Vec::new(); n_pyramid];
        if version >= 2 {
            let mut total = 0usize;
            for list in leaves.iter_mut() {
                let count = u32_at(at) as usize;
                at = at.saturating_add(4);
                total = total.saturating_add(count);
                if total > MAX_SLABS.saturating_mul(16) {
                    return Err(ManifestError::TooManySlabs);
                }
                for _ in 0..count {
                    list.push(next(&mut at)?);
                }
            }
        }
        Ok(Self {
            generation: u64_at(8),
            updated_ts: u64_at(16),
            tip: Oid::from_slice(b.get(24..44).unwrap_or(&[])).ok_or(ManifestError::Truncated)?,
            next_seq: u64_at(44),
            baseline,
            weekly,
            pyramid,
            leaves,
        })
    }

    /// Appends a pyramid slab with its leaves (keeps the two lists in step).
    pub fn push_pyramid(&mut self, slab: SlabRef, leaves: Vec<SlabRef>) {
        self.leaves.resize(self.pyramid.len(), Vec::new());
        self.pyramid.push(slab);
        self.leaves.push(leaves);
    }

    /// Drops the pyramid suffix from `n` on, returning the removed slabs.
    pub fn truncate_pyramid(&mut self, n: usize) -> Vec<SlabRef> {
        self.leaves.resize(self.pyramid.len(), Vec::new());
        self.leaves.truncate(n);
        self.pyramid.split_off(n.min(self.pyramid.len()))
    }

    pub fn clear_pyramid(&mut self) {
        self.pyramid.clear();
        self.leaves.clear();
    }

    /// The leaves of `pyramid[i]` (none for an m15 or a superset-tier rollup).
    pub fn leaves_of(&self, i: usize) -> &[SlabRef] {
        self.leaves.get(i).map(Vec::as_slice).unwrap_or(&[])
    }

    /// The leaves a rollup of `members` would carry when the pipeline keeps leaves: a
    /// member's own leaves, or the member itself when it has none.
    pub fn flatten_leaves(&self, first: usize) -> Vec<SlabRef> {
        let mut out = Vec::new();
        for (i, s) in self.pyramid.iter().enumerate().skip(first) {
            let l = self.leaves_of(i);
            if l.is_empty() {
                out.push(*s);
            } else {
                out.extend_from_slice(l);
            }
        }
        out
    }

    /// Every published slab (baseline, weeklies, pyramid, retained leaves).
    pub fn all_slabs(&self) -> impl Iterator<Item = &SlabRef> {
        self.baseline
            .iter()
            .chain(self.weekly.iter())
            .chain(self.pyramid.iter())
            .chain(self.leaves.iter().flatten())
    }

    /// Slabs an incremental ingest must deduplicate against: everything that can reach a
    /// client (the baseline and the pyramid; weeklies are subsets of the baseline track).
    pub fn lookup_slabs(&self) -> impl Iterator<Item = &SlabRef> {
        self.baseline.iter().chain(self.pyramid.iter())
    }

    /// Timestamp a client ends up with after a cold clone (max over Track A).
    pub fn latest_ts(&self) -> u64 {
        self.all_slabs().map(|s| s.end_ts).max().unwrap_or(0)
    }

    /// `end_ts` for a slab sealed now: wall-clock seconds, but always strictly after every
    /// published slab, so a client's `lastPackTimestamp` can never straddle two slabs.
    pub fn next_end_ts(&self, now: u64) -> u64 {
        now.max(self.latest_ts().saturating_add(1))
    }

    pub fn allocate_seq(&mut self) -> u64 {
        let s = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        s
    }

    /// Picks the slabs for `GET /gvfs/prefetch?lastPackTimestamp=client_ts`, oldest first,
    /// into `out` (fixed capacity, so never allocates). `l0_end_ts` is the live slab's
    /// timestamp if one exists.
    pub fn select_for_prefetch(&self, client_ts: u64, l0_end_ts: Option<u64>, out: &mut Selection) {
        self.select(client_ts, l0_end_ts, false, out);
    }

    /// Like [`Manifest::select_for_prefetch`] but never descends into leaves: the
    /// `compact=1` mode a client hook uses to replace many small local packs by their
    /// superset rollups (then unlinking the small ones).
    pub fn select_compact(&self, client_ts: u64, l0_end_ts: Option<u64>, out: &mut Selection) {
        self.select(client_ts, l0_end_ts, true, out);
    }

    fn select(&self, client_ts: u64, l0_end_ts: Option<u64>, compact: bool, out: &mut Selection) {
        out.clear();
        let oldest_weekly_start = self.weekly.first().map(|w| w.start_ts);
        let cold = client_ts == 0
            || oldest_weekly_start.is_none_or(|s| client_ts < s)
                && self.baseline.is_some_and(|b| client_ts < b.end_ts);
        if cold {
            // The pyramid is always newer than the baseline by construction (a baseline is
            // cut only after the pyramid was rolled into a weekly delta).
            if self.baseline.is_some() {
                out.push(Pick::Baseline);
            }
            for i in 0..self.pyramid.len() {
                out.push(Pick::Pyramid(i as u16));
            }
        } else {
            for (i, w) in self.weekly.iter().enumerate() {
                if w.end_ts > client_ts {
                    out.push(Pick::Weekly(i as u16));
                }
            }
            for (i, s) in self.pyramid.iter().enumerate() {
                if s.end_ts <= client_ts {
                    continue;
                }
                // Inside a rollup's window with retained leaves: only the missing leaves,
                // as long as they fit the selection; nothing at all when the client already
                // holds every leaf (the rollup is then old news; `compact=1` asks for it
                // explicitly); otherwise the whole rollup.
                let leaves = self.leaves_of(i);
                if !compact && client_ts > s.start_ts && !leaves.is_empty() {
                    let needed = leaves.iter().filter(|l| l.end_ts > client_ts).count();
                    if needed == 0 {
                        continue;
                    }
                    if needed <= out.room() {
                        for (j, l) in leaves.iter().enumerate() {
                            if l.end_ts > client_ts {
                                out.push(Pick::Leaf(i as u16, j as u16));
                            }
                        }
                        continue;
                    }
                }
                out.push(Pick::Pyramid(i as u16));
            }
        }
        if let Some(ts) = l0_end_ts
            && (ts > client_ts || cold)
        {
            out.push(Pick::L0);
        }
    }

    pub fn slab(&self, pick: Pick) -> Option<&SlabRef> {
        match pick {
            Pick::Baseline => self.baseline.as_ref(),
            Pick::Weekly(i) => self.weekly.get(usize::from(i)),
            Pick::Pyramid(i) => self.pyramid.get(usize::from(i)),
            Pick::Leaf(i, j) => self.leaves_of(usize::from(i)).get(usize::from(j)),
            Pick::L0 => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pick {
    Baseline,
    Weekly(u16),
    Pyramid(u16),
    /// Leaf `j` of pyramid slab `i`.
    Leaf(u16, u16),
    L0,
}

/// A fixed-capacity list of picks (lives on the stack of a request handler).
#[derive(Debug, Clone, Copy)]
pub struct Selection {
    items: [Pick; MAX_SELECTION],
    len: usize,
}

impl Default for Selection {
    fn default() -> Self {
        Self::new()
    }
}

impl Selection {
    pub const fn new() -> Self {
        Self {
            items: [Pick::L0; MAX_SELECTION],
            len: 0,
        }
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Drops picks past the capacity (a client then simply fetches again).
    pub fn push(&mut self, p: Pick) {
        if let Some(slot) = self.items.get_mut(self.len) {
            *slot = p;
            self.len = self.len.saturating_add(1);
        }
    }

    pub fn as_slice(&self) -> &[Pick] {
        self.items.get(..self.len).unwrap_or(&[])
    }

    /// Picks that still fit (one slot is kept for the live slab).
    pub fn room(&self) -> usize {
        MAX_SELECTION.saturating_sub(self.len).saturating_sub(1)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::arithmetic_side_effects)]
    use super::*;

    fn slab(seq: u64, tier: Tier, start: u64, end: u64) -> SlabRef {
        SlabRef {
            seq,
            tier,
            start_ts: start,
            end_ts: end,
            tip: Oid([seq as u8; 20]),
            object_count: 10,
            pack_len: 100,
            idx_len: 50,
            mphf_len: 20,
        }
    }

    #[test]
    fn encodes_and_decodes() {
        let m = Manifest {
            generation: 7,
            updated_ts: 1000,
            tip: Oid([9; 20]),
            next_seq: 5,
            baseline: Some(slab(1, Tier::Baseline, 0, 500)),
            weekly: vec![slab(2, Tier::Weekly, 100, 500)],
            pyramid: vec![slab(3, Tier::H1, 500, 560), slab(4, Tier::M15, 560, 575)],
            leaves: vec![
                vec![slab(20, Tier::M15, 500, 530), slab(21, Tier::M15, 530, 560)],
                vec![],
            ],
        };
        let b = m.encode();
        assert_eq!(Manifest::decode(&b).unwrap(), m);
        assert_eq!(m.all_slabs().count(), 6);
        // A version-1 manifest (no leaf lists) still decodes.
        let mut v1 = Manifest {
            leaves: vec![vec![], vec![]],
            ..m.clone()
        };
        let mut bytes = v1.encode();
        bytes.truncate(bytes.len() - 20 - 8); // drop the checksum and the two empty leaf counts
        if let Some(v) = bytes.get_mut(4..8) {
            v.copy_from_slice(&1u32.to_le_bytes());
        }
        let digest: [u8; 20] = Sha1::digest(&bytes).into();
        bytes.extend_from_slice(&digest);
        v1.leaves = vec![vec![], vec![]];
        assert_eq!(Manifest::decode(&bytes).unwrap(), v1);
        let mut bad = b.clone();
        bad[70] ^= 1;
        assert_eq!(
            Manifest::decode(&bad).err(),
            Some(ManifestError::BadChecksum)
        );
        assert_eq!(
            Manifest::decode(&b[..30]).err(),
            Some(ManifestError::Truncated)
        );
        assert_eq!(
            Manifest::decode(b"GVMFxxxx").err(),
            Some(ManifestError::Truncated)
        );
        assert_eq!(
            Manifest::encode(&Manifest::default()).len(),
            HEADER_LEN + 20
        );
    }

    #[test]
    fn two_track_selection() {
        let m = Manifest {
            baseline: Some(slab(10, Tier::Baseline, 0, 1000)),
            weekly: vec![
                slab(8, Tier::Weekly, 300, 650),
                slab(9, Tier::Weekly, 650, 1000),
            ],
            pyramid: vec![
                slab(11, Tier::H24, 1000, 1100),
                slab(12, Tier::H1, 1100, 1160),
                slab(13, Tier::M15, 1160, 1175),
            ],
            ..Manifest::default()
        };
        let mut sel = Selection::new();
        // Cold clone: baseline + pyramid + live slab.
        m.select_for_prefetch(0, Some(1180), &mut sel);
        assert_eq!(
            sel.as_slice(),
            &[
                Pick::Baseline,
                Pick::Pyramid(0),
                Pick::Pyramid(1),
                Pick::Pyramid(2),
                Pick::L0
            ]
        );
        // Warm client from mid last week: weekly deltas + pyramid, no baseline.
        m.select_for_prefetch(700, None, &mut sel);
        assert_eq!(
            sel.as_slice(),
            &[
                Pick::Weekly(1),
                Pick::Pyramid(0),
                Pick::Pyramid(1),
                Pick::Pyramid(2)
            ]
        );
        // Warm client from an hour ago.
        m.select_for_prefetch(1150, Some(1180), &mut sel);
        assert_eq!(
            sel.as_slice(),
            &[Pick::Pyramid(1), Pick::Pyramid(2), Pick::L0]
        );
        // Up to date except for the live slab.
        m.select_for_prefetch(1175, Some(1180), &mut sel);
        assert_eq!(sel.as_slice(), &[Pick::L0]);
        m.select_for_prefetch(1180, Some(1180), &mut sel);
        assert!(sel.is_empty());
        // Older than the retained weeklies: back to the cold track.
        m.select_for_prefetch(100, None, &mut sel);
        assert_eq!(sel.as_slice()[0], Pick::Baseline);
        // No baseline yet: everything in the pyramid.
        let m2 = Manifest {
            pyramid: m.pyramid.clone(),
            ..Manifest::default()
        };
        m2.select_for_prefetch(0, None, &mut sel);
        assert_eq!(sel.len(), 3);
    }

    #[test]
    fn leaves_serve_clients_inside_a_window() {
        let mut m = Manifest {
            baseline: Some(slab(10, Tier::Baseline, 0, 1000)),
            ..Manifest::default()
        };
        m.push_pyramid(
            slab(11, Tier::H1, 1000, 1060),
            vec![
                slab(1, Tier::M15, 1000, 1015),
                slab(2, Tier::M15, 1015, 1030),
                slab(3, Tier::M15, 1030, 1045),
                slab(4, Tier::M15, 1045, 1060),
            ],
        );
        m.push_pyramid(slab(12, Tier::M15, 1060, 1075), vec![]);
        let mut sel = Selection::new();
        // Inside the h1 window: only the leaves after the cursor, then the m15.
        m.select_for_prefetch(1030, None, &mut sel);
        assert_eq!(
            sel.as_slice(),
            &[Pick::Leaf(0, 2), Pick::Leaf(0, 3), Pick::Pyramid(1)]
        );
        // compact=1: the superset rollup instead of its leaves.
        m.select_compact(1030, None, &mut sel);
        assert_eq!(sel.as_slice(), &[Pick::Pyramid(0), Pick::Pyramid(1)]);
        // At the window start: the rollup whole.
        m.select_for_prefetch(1000, None, &mut sel);
        assert_eq!(sel.as_slice(), &[Pick::Pyramid(0), Pick::Pyramid(1)]);
        // Cold: baseline + rollups, never leaves.
        m.select_for_prefetch(0, None, &mut sel);
        assert_eq!(
            sel.as_slice(),
            &[Pick::Baseline, Pick::Pyramid(0), Pick::Pyramid(1)]
        );
        assert_eq!(
            m.flatten_leaves(0)
                .iter()
                .map(|s| s.seq)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 12]
        );
        assert_eq!(m.slab(Pick::Leaf(0, 1)).unwrap().seq, 2);
        let removed = m.truncate_pyramid(1);
        assert_eq!(removed.len(), 1);
        assert_eq!((m.pyramid.len(), m.leaves.len()), (1, 1));
        m.clear_pyramid();
        assert!(m.all_slabs().count() == 1);
    }
}
