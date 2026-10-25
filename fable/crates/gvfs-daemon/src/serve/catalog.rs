//! The catalog: an immutable snapshot of every slab a `serve` process can read, replaced
//! wholesale when the manifest changes or the live slab grows. Requests clone the `Arc` and
//! keep working on their snapshot; nothing is ever mutated underneath them.

use std::sync::Arc;

use crate::manifest::{Manifest, Pick, Selection};
use crate::mphf::Loc;
use crate::oid::Oid;
use crate::slab::{Slab, SlabError};
use crate::splice::{SlabList, SpliceError};
use crate::store::FsStore;

#[derive(Debug, Default)]
pub struct Catalog {
    pub manifest: Manifest,
    pub generation: u64,
    pub baseline: Option<Arc<Slab>>,
    /// Prefetch-only (no mphf): weekly deltas are subsets of the baseline track.
    pub weekly: Vec<Arc<Slab>>,
    pub pyramid: Vec<Arc<Slab>>,
    /// Retained leaves per pyramid slab (prefetch-only, like the weeklies).
    pub leaves: Vec<Vec<Arc<Slab>>>,
    /// The live (<15 min) in-memory slab.
    pub l0: Option<Arc<Slab>>,
}

impl Catalog {
    /// Maps every slab of `manifest` (probe indexes for the baseline and the pyramid).
    pub fn load(store: &FsStore, manifest: Manifest) -> Result<Self, SlabError> {
        let baseline = match manifest.baseline {
            Some(r) => Some(Arc::new(Slab::open(store, r, true)?)),
            None => None,
        };
        let weekly = manifest
            .weekly
            .iter()
            .map(|r| Slab::open(store, *r, false).map(Arc::new))
            .collect::<Result<Vec<_>, _>>()?;
        let pyramid = manifest
            .pyramid
            .iter()
            .map(|r| Slab::open(store, *r, true).map(Arc::new))
            .collect::<Result<Vec<_>, _>>()?;
        let mut leaves = Vec::with_capacity(pyramid.len());
        for i in 0..manifest.pyramid.len() {
            leaves.push(
                manifest
                    .leaves_of(i)
                    .iter()
                    .map(|r| Slab::open(store, *r, false).map(Arc::new))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        Ok(Self {
            generation: manifest.generation,
            manifest,
            baseline,
            weekly,
            pyramid,
            leaves,
            l0: None,
        })
    }

    /// The same snapshot with a different live slab.
    pub fn with_l0(&self, l0: Option<Arc<Slab>>) -> Self {
        Self {
            manifest: self.manifest.clone(),
            generation: self.generation,
            baseline: self.baseline.clone(),
            weekly: self.weekly.clone(),
            pyramid: self.pyramid.clone(),
            leaves: self.leaves.clone(),
            l0,
        }
    }

    /// Slabs probed for point lookups, newest first: L0, pyramid (newest first), baseline.
    pub fn probe_slabs(&self) -> impl Iterator<Item = &Arc<Slab>> {
        self.l0
            .iter()
            .chain(self.pyramid.iter().rev())
            .chain(self.baseline.iter())
    }

    /// Number of probe slabs (bounded by `MAX_PROBE`).
    pub fn probe_count(&self) -> usize {
        self.probe_slabs().count()
    }

    /// Total object count over the probe slabs (sizes the per-request seen-bitsets).
    pub fn probe_keys(&self) -> u64 {
        self.probe_slabs()
            .map(|s| u64::from(s.meta.object_count))
            .sum()
    }

    /// Finds `oid`: (probe index, slab, mphf slot, location).
    #[inline]
    pub fn lookup(&self, oid: &Oid) -> Option<(usize, &Arc<Slab>, usize, Loc)> {
        for (k, s) in self.probe_slabs().enumerate() {
            if let Some((slot, loc)) = s.lookup(oid) {
                return Some((k, s, slot, loc));
            }
        }
        None
    }

    #[inline]
    pub fn probe(&self, k: usize) -> Option<&Arc<Slab>> {
        self.probe_slabs().nth(k)
    }

    /// The branch tip the live sync continues from.
    pub fn tip(&self) -> Oid {
        self.l0.as_ref().map_or(self.manifest.tip, |l| l.meta.tip)
    }

    /// Slabs for `GET /gvfs/prefetch?lastPackTimestamp=ts[&compact=1]`, oldest first.
    pub fn select(&self, ts: u64, compact: bool) -> Result<SlabList, SpliceError> {
        let mut sel = Selection::new();
        let l0_ts = self.l0.as_ref().map(|l| l.meta.end_ts);
        if compact {
            self.manifest.select_compact(ts, l0_ts, &mut sel);
        } else {
            self.manifest.select_for_prefetch(ts, l0_ts, &mut sel);
        }
        let mut list = SlabList::new();
        for pick in sel.as_slice() {
            let slab = match *pick {
                Pick::Baseline => self.baseline.clone(),
                Pick::Weekly(i) => self.weekly.get(usize::from(i)).cloned(),
                Pick::Pyramid(i) => self.pyramid.get(usize::from(i)).cloned(),
                Pick::Leaf(i, j) => self
                    .leaves
                    .get(usize::from(i))
                    .and_then(|l| l.get(usize::from(j)))
                    .cloned(),
                Pick::L0 => self.l0.clone(),
            };
            if let Some(s) = slab {
                list.push(s)?;
            }
        }
        Ok(list)
    }
}
