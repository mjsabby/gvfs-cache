//! What `serve` answers from: an immutable snapshot of the sealed slabs plus the live
//! tip's L0 slabs, swapped atomically whenever either changes.
//!
//! **Every prefetch response is ONE pack holding exactly what the client lacks**: the
//! timeline's exact cover ([`crate::timeline`]) from the client's cursor to the newest
//! boundary. A cover of several slabs is repacked into one ([`crate::repack`]), so the pack
//! is about what one upstream fetch of exactly that range would be.
//!
//! * Cold clients (no cursor, or one older than the oldest weekly delta's start) get the
//!   baseline plus the cover from the baseline's end.
//! * Warm clients get the cover from their cursor, never the baseline.
//! * Superset deliveries (`--superset-tier`, default 24h): a client whose cursor lies in a
//!   sealed daily or weekly rollup ([`crate::timeline::superset_start`]) is answered from the
//!   rollup's start once the tip has moved past it, so it receives the rollup whole, once.
//!   Its smaller packs from that window become redundant, which lets stock client
//!   maintenance (`git multi-pack-index expire`) or an interval hook delete them: without
//!   it, Scalar's default `incremental-repack` keeps nearly every small pack.
//!
//! A cursor is answered from the newest boundary at or before it (a slab start), so a
//! snapshot has at most one plan per boundary; each is built on first use and shared.

use std::sync::{Arc, OnceLock};

use crate::ingest::{Dest, Indexer};
use crate::manifest::Manifest;
use crate::repack::{Input, Mode, Repacker};
use crate::slab::{Slab, Tier};
use crate::splice::PackPlan;
use crate::util::Oid;

pub struct View {
    /// Local snapshot number (changes with the manifest or L0).
    pub generation: u64,
    pub manifest: Manifest,
    pub etag: String,
    /// The newest commit served (the newest live L0 slab's tip, or the manifest's), and
    /// its timestamp.
    pub tip: Oid,
    pub tip_ts: u64,
    /// Object lookup order, newest first: live L0 slabs, pyramid, weekly deltas, baseline.
    pub all: Vec<Arc<Slab>>,
    /// Live L0 slabs (after the manifest's tip): `all[0..l0_len]`.
    pub l0_len: usize,
    /// Index into `all` of the baseline.
    baseline: usize,
    /// What plans are made of: weekly deltas, pyramid, retained slabs and every L0 slab,
    /// sorted by `(start_ts, end_ts)`; `spans` are their bounds.
    warm: Vec<Arc<Slab>>,
    spans: Vec<(u64, u64)>,
    /// Distinct span starts, ascending: the plan keys.
    starts: Vec<u64>,
    /// Clients older than this get the cold plan.
    warm_floor: u64,
    /// Sealed rollups delivered whole to the clients inside them (`(start, end]`).
    rollups: Vec<(u64, u64)>,
    plans: Vec<OnceLock<Option<Arc<PackPlan>>>>,
    cold_plan: OnceLock<Arc<PackPlan>>,
    /// The store's blob packs (`--blobs separate` stores; `None`: blobs are inline).
    pub blobs: Option<Arc<super::blobs::BlobSet>>,
}

/// A prefetch answer: nothing new, or one pack covering `(start, plan.timestamp]`
/// (`start` 0 for a cold clone).
pub enum Prefetch {
    UpToDate,
    Pack(Arc<PackPlan>, u64),
}

impl View {
    /// `sealed` in manifest order ([`Manifest::slabs`]: baseline, weekly deltas, pyramid,
    /// retained); `l0` oldest first. Sealed slabs of tier `supersets` and above (24h or the
    /// weekly delta) are delivered whole to the clients inside them; `None`: never.
    pub fn new(
        generation: u64,
        manifest: Manifest,
        etag: String,
        sealed: Vec<Arc<Slab>>,
        l0: Vec<Arc<Slab>>,
        supersets: Option<Tier>,
        blobs: Option<Arc<super::blobs::BlobSet>>,
    ) -> View {
        let (nw, np) = (manifest.weekly.len(), manifest.pyramid.len());
        let weekly = sealed.get(1..nw.saturating_add(1)).unwrap_or(&[]);
        let pyramid = sealed
            .get(nw.saturating_add(1)..nw.saturating_add(np).saturating_add(1))
            .unwrap_or(&[]);
        let retained = sealed
            .get(nw.saturating_add(np).saturating_add(1)..)
            .unwrap_or(&[]);
        let live: Vec<&Arc<Slab>> = l0
            .iter()
            .filter(|s| s.meta.start_ts >= manifest.max_ts)
            .collect();
        let tip = live.last().map_or(manifest.tip, |s| s.meta.want);
        let tip_ts = live.last().map_or(manifest.max_ts, |s| s.meta.end_ts);

        let mut all: Vec<Arc<Slab>> = live.iter().rev().map(|s| Arc::clone(s)).collect();
        let l0_len = all.len();
        all.extend(pyramid.iter().rev().cloned());
        all.extend(weekly.iter().rev().cloned());
        all.extend(sealed.first().cloned());
        let baseline = all.len().saturating_sub(1);

        // L0 slabs first: the sort is stable and covers take the last of equal spans, so a
        // sealed slab wins over an L0 slab with the same bounds.
        let mut warm: Vec<Arc<Slab>> = l0
            .iter()
            .chain(weekly)
            .chain(pyramid)
            .chain(retained)
            .cloned()
            .collect();
        warm.sort_by_key(|s| (s.meta.start_ts, s.meta.end_ts));
        let spans: Vec<(u64, u64)> = warm
            .iter()
            .map(|s| (s.meta.start_ts, s.meta.end_ts))
            .collect();
        let mut starts: Vec<u64> = spans.iter().map(|s| s.0).collect();
        starts.dedup();
        let warm_floor = manifest
            .weekly
            .first()
            .map_or(manifest.baseline.end_ts, |w| w.start_ts);
        let plans = starts.iter().map(|_| OnceLock::new()).collect();
        let mut rollups: Vec<(u64, u64)> = match supersets {
            Some(min) => weekly
                .iter()
                .chain(pyramid)
                .chain(retained)
                .filter(|s| matches!(s.meta.tier, Tier::H24 | Tier::Week) && s.meta.tier >= min)
                .map(|s| (s.meta.start_ts, s.meta.end_ts))
                .collect(),
            None => Vec::new(),
        };
        rollups.sort_unstable();
        View {
            generation,
            manifest,
            etag,
            tip,
            tip_ts,
            all,
            l0_len,
            baseline,
            warm,
            spans,
            starts,
            warm_floor,
            rollups,
            plans,
            cold_plan: OnceLock::new(),
            blobs,
        }
    }

    /// The size of a blob the trees of this view's slabs reference (from their sizes
    /// tables; `--blobs separate` stores only).
    #[inline]
    pub fn blob_size(&self, oid: &Oid) -> Option<u64> {
        self.all.iter().find_map(|s| s.sizes()?.get(oid))
    }

    /// Whether a sealed slab's tree names blob `oid` (its sizes table has it). Once
    /// `blobs.bin` is complete, every such blob is in the store.
    pub fn blob_sealed(&self, oid: &Oid) -> bool {
        self.all
            .get(self.l0_len..)
            .unwrap_or(&[])
            .iter()
            .any(|s| s.sizes().is_some_and(|t| t.get(oid).is_some()))
    }

    /// Finds `oid` in the newest slab that has it.
    #[inline]
    pub fn find(&self, oid: &Oid) -> Option<(u16, u32)> {
        self.all.iter().enumerate().find_map(|(i, s)| {
            s.find(oid)
                .map(|slot| (u16::try_from(i).unwrap_or(u16::MAX), slot))
        })
    }

    /// The live L0 slabs, oldest first.
    pub fn l0(&self) -> impl Iterator<Item = &Arc<Slab>> {
        self.all.get(..self.l0_len).unwrap_or(&[]).iter().rev()
    }

    /// The (shared, lazily built) plan for a client whose newest pack is `client_ts`.
    pub fn prefetch(&self, client_ts: u64) -> Prefetch {
        if client_ts != 0 && client_ts >= self.tip_ts {
            return Prefetch::UpToDate;
        }
        let client_ts = crate::timeline::superset_start(&self.rollups, client_ts, self.tip_ts)
            .unwrap_or(client_ts);
        let key = self
            .starts
            .partition_point(|&s| s <= client_ts)
            .checked_sub(1);
        let warm = key.and_then(|i| Some((self.plans.get(i)?, *self.starts.get(i)?)));
        match warm {
            Some((cell, from)) if client_ts >= self.warm_floor => {
                let plan = cell.get_or_init(|| {
                    let _cold = gvfs_alloc_guard::cold_section();
                    let (slabs, sources, _) = self.covered(from, &[]);
                    (!slabs.is_empty()).then(|| Arc::new(PackPlan::with_sources(slabs, sources)))
                });
                match plan {
                    Some(p) => Prefetch::Pack(p.clone(), from),
                    None => Prefetch::UpToDate,
                }
            }
            _ => Prefetch::Pack(
                self.cold_plan
                    .get_or_init(|| {
                        let _cold = gvfs_alloc_guard::cold_section();
                        let mut slabs: Vec<Arc<Slab>> =
                            self.all.get(self.baseline).cloned().into_iter().collect();
                        // The tail is concatenated after the baseline in ONE pack: it must
                        // not repeat any baseline object (upstreams re-send objects that
                        // older history already has, e.g. after a revert), or the pack has
                        // duplicate entries that `index-pack --strict` rejects.
                        let base = slabs.clone();
                        let (tail, sources, end) =
                            self.covered(self.manifest.baseline.end_ts, &base);
                        slabs.extend(tail);
                        let mut plan = PackPlan::with_sources(slabs, sources.saturating_add(1));
                        // Even when every tail object was already in the baseline, the
                        // client is now current up to the cover's end.
                        plan.timestamp = plan.timestamp.max(end);
                        Arc::new(plan)
                    })
                    .clone(),
                0,
            ),
        }
    }

    /// The cold plan (built if needed), for warming its trailer in the background.
    pub fn cold_plan(&self) -> Option<Arc<PackPlan>> {
        match self.prefetch(0) {
            Prefetch::Pack(p, _) => Some(p),
            Prefetch::UpToDate => None,
        }
    }

    /// The slabs that take a client at boundary `from` to the tip: the timeline's cover,
    /// repacked into one slab when it has several, or when it shares objects with `exclude`
    /// (slabs sent in the same pack ahead of it), which the result then leaves out. Also
    /// returns how many timeline slabs the answer is made of and the cover's end.
    fn covered(&self, from: u64, exclude: &[Arc<Slab>]) -> (Vec<Arc<Slab>>, usize, u64) {
        let mut idx = Vec::new();
        let cover: Vec<Arc<Slab>> =
            if crate::timeline::cover(&self.spans, from, self.tip_ts, &mut idx) {
                idx.iter()
                    .filter_map(|&i| self.warm.get(i).cloned())
                    .collect()
            } else {
                // A hole in the timeline (which slabs never leave): every slab past `from`.
                crate::log!(
                    "serve: no exact cover from {from} to {}; sending every slab after it",
                    self.tip_ts
                );
                self.all
                    .get(..self.baseline)
                    .unwrap_or(&[])
                    .iter()
                    .rev()
                    .filter(|s| s.meta.end_ts > from)
                    .cloned()
                    .collect()
            };
        let n = cover.len();
        let end = cover.iter().map(|s| s.meta.end_ts).max().unwrap_or(0);
        let overlaps = !exclude.is_empty()
            && cover.iter().any(|s| {
                s.records()
                    .any(|r| exclude.iter().any(|e| e.find(&r.oid).is_some()))
            });
        if n > 1 || overlaps {
            match merge(&cover, exclude) {
                Merged::One(one) => return (vec![one], n, end),
                Merged::Nothing => return (Vec::new(), n, end),
                // Never concatenate overlapping slabs: the client gets what `exclude`
                // holds now and the rest from its next (warm) prefetch.
                Merged::Failed if overlaps => return (Vec::new(), n, 0),
                Merged::Failed => {}
            }
        }
        (cover, n, end)
    }
}

enum Merged {
    One(Arc<Slab>),
    /// Every object of the cover is in the excluded slabs.
    Nothing,
    Failed,
}

/// One slab with the objects of `cover` (consecutive slabs) that are not in `exclude`, trees
/// re-deltified across them. [`Merged::Failed`]: send the slabs as they are.
fn merge(cover: &[Arc<Slab>], exclude: &[Arc<Slab>]) -> Merged {
    let (Some(first), Some(last)) = (cover.first(), cover.last()) else {
        return Merged::Nothing;
    };
    let mut repacker = Repacker::new();
    repacker.quiet = true;
    let mut indexer = Indexer::new(1);
    let input = Input {
        slabs: cover,
        frozen: None,
        exclude,
        mode: Mode::Trees,
    };
    let range = (first.meta.start_ts, last.meta.end_ts);
    match repacker.repack(
        &mut indexer,
        &input,
        Tier::L0,
        range,
        (first.meta.have, last.meta.want),
        Dest::Memory,
    ) {
        Ok(Some(s)) => {
            // The merged slab carries what its parts' trees introduce.
            let tables: Vec<&crate::sizes::Sizes> = cover
                .iter()
                .filter_map(|c| c.sizes().map(|t| t.as_ref()))
                .collect();
            if !tables.is_empty() {
                match crate::sizes::Sizes::from_heap(crate::sizes::merge(&tables)) {
                    Ok(t) => s.slab.set_sizes(Arc::new(t)),
                    Err(e) => crate::log!("serve: merging sizes tables failed: {e}"),
                }
            }
            Merged::One(s.slab)
        }
        Ok(None) => Merged::Nothing,
        Err(e) => {
            crate::log!(
                "serve: repacking a prefetch plan failed ({e}); sending its {} slabs as they are",
                cover.len()
            );
            Merged::Failed
        }
    }
}
