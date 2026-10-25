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
    plans: Vec<OnceLock<Option<Arc<PackPlan>>>>,
    cold_plan: OnceLock<Arc<PackPlan>>,
}

/// A prefetch answer: nothing new, or one pack.
pub enum Prefetch {
    UpToDate,
    Pack(Arc<PackPlan>),
}

impl View {
    /// `sealed` in manifest order ([`Manifest::slabs`]: baseline, weekly deltas, pyramid,
    /// retained); `l0` oldest first.
    pub fn new(
        generation: u64,
        manifest: Manifest,
        etag: String,
        sealed: Vec<Arc<Slab>>,
        l0: Vec<Arc<Slab>>,
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
            plans,
            cold_plan: OnceLock::new(),
        }
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
        let key = self
            .starts
            .partition_point(|&s| s <= client_ts)
            .checked_sub(1);
        let warm = key.and_then(|i| Some((self.plans.get(i)?, *self.starts.get(i)?)));
        match warm {
            Some((cell, from)) if client_ts >= self.warm_floor => {
                let plan = cell.get_or_init(|| {
                    let _cold = gvfs_alloc_guard::cold_section();
                    let (slabs, sources) = self.covered(from);
                    (!slabs.is_empty()).then(|| Arc::new(PackPlan::with_sources(slabs, sources)))
                });
                match plan {
                    Some(p) => Prefetch::Pack(p.clone()),
                    None => Prefetch::UpToDate,
                }
            }
            _ => Prefetch::Pack(
                self.cold_plan
                    .get_or_init(|| {
                        let _cold = gvfs_alloc_guard::cold_section();
                        let mut slabs: Vec<Arc<Slab>> =
                            self.all.get(self.baseline).cloned().into_iter().collect();
                        let (tail, sources) = self.covered(self.manifest.baseline.end_ts);
                        slabs.extend(tail);
                        Arc::new(PackPlan::with_sources(slabs, sources.saturating_add(1)))
                    })
                    .clone(),
            ),
        }
    }

    /// The cold plan (built if needed), for warming its trailer in the background.
    pub fn cold_plan(&self) -> Option<Arc<PackPlan>> {
        match self.prefetch(0) {
            Prefetch::Pack(p) => Some(p),
            Prefetch::UpToDate => None,
        }
    }

    /// The slabs that take a client at boundary `from` to the tip: the timeline's cover,
    /// repacked into one slab when it has several. Also returns how many timeline slabs the
    /// answer is made of.
    fn covered(&self, from: u64) -> (Vec<Arc<Slab>>, usize) {
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
        if n > 1 {
            if let Some(one) = merge(&cover) {
                return (vec![one], n);
            }
        }
        (cover, n)
    }
}

/// One slab with the objects of `cover` (consecutive slabs), trees re-deltified across
/// them. `None` (send the slabs as they are) if repacking fails.
fn merge(cover: &[Arc<Slab>]) -> Option<Arc<Slab>> {
    let (first, last) = (cover.first()?, cover.last()?);
    let mut repacker = Repacker::new();
    repacker.quiet = true;
    let mut indexer = Indexer::new(1);
    let input = Input {
        slabs: cover,
        frozen: None,
        exclude: &[],
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
        Ok(s) => s.map(|s| s.slab),
        Err(e) => {
            crate::log!(
                "serve: repacking a prefetch plan failed ({e}); sending its {} slabs as they are",
                cover.len()
            );
            None
        }
    }
}
