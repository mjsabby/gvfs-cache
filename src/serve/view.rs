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
    /// The ids of every slab before the baseline in `all`: a lookup it rules out goes
    /// straight to the baseline (see [`crate::idfilter`]).
    recent: Arc<crate::idfilter::IdFilter>,
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
    /// Boundaries the edge handed out that are gone (`(from, upto]` stands for `from`, see
    /// [`super::LiveTip::retracted`]), and every boundary this view has, ascending.
    retracted: Vec<(u64, u64)>,
    ends: Vec<u64>,
    /// The store's blob packs (`--blobs separate` stores; `None`: blobs are inline).
    pub blobs: Option<Arc<super::blobs::BlobSet>>,
}

/// A prefetch answer: nothing new, or one pack covering `(start, plan.timestamp]`
/// (`start` 0 for a cold clone).
pub enum Prefetch {
    UpToDate,
    Pack(Arc<PackPlan>, u64),
}

/// Where [`View::prefetch`] finds (or builds) a client's plan.
enum Slot<'a> {
    UpToDate,
    /// The plan for clients from boundary `.1` on.
    Warm(&'a OnceLock<Option<Arc<PackPlan>>>, u64),
    Cold,
}

impl View {
    /// `sealed` in manifest order ([`Manifest::slabs`]: baseline, weekly deltas, pyramid,
    /// retained); `l0` oldest first. Sealed slabs of tier `supersets` and above (24h or the
    /// weekly delta) are delivered whole to the clients inside them; `None`: never. A cursor
    /// in a `retracted` range that is not a boundary here is answered from the range's start.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        generation: u64,
        manifest: Manifest,
        etag: String,
        sealed: Vec<Arc<Slab>>,
        l0: Vec<Arc<Slab>>,
        supersets: Option<Tier>,
        blobs: Option<Arc<super::blobs::BlobSet>>,
        retracted: &[(u64, u64)],
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
        // Lookups try the slabs in this order. One filter over every slab before the
        // baseline sends most of them (ids only the baseline has) straight there; each of
        // those slabs also gets its own (built once per slab) for the ids that pass it.
        let before = all.get(..baseline).unwrap_or(&[]);
        for s in before {
            s.prepare_filter();
        }
        let recent = Arc::new(crate::idfilter::IdFilter::new(
            before
                .iter()
                .map(|s| crate::util::us(u64::from(s.count())))
                .sum(),
            before.iter().flat_map(|s| s.ids()),
        ));

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
        let mut ends: Vec<u64> = spans
            .iter()
            .map(|s| s.1)
            .chain([manifest.max_ts, manifest.baseline.end_ts])
            .collect();
        ends.sort_unstable();
        ends.dedup();
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
            recent,
            warm,
            spans,
            starts,
            warm_floor,
            rollups,
            plans,
            cold_plan: OnceLock::new(),
            blobs,
            retracted: retracted.to_vec(),
            ends,
        }
    }

    /// This view with the store's blob packs as `blobs` now, and nothing else changed: the
    /// same slabs, and the plans made of them so far, which a `blobs.bin` has no part in.
    /// (A view made anew starts without plans, and its cold pack is hashed again: 1.2 s
    /// on SearchGold, 3.5 s on RepoC, at every `blobs.bin` a running mirror publishes.)
    pub fn with_blobs(&self, generation: u64, blobs: Option<Arc<super::blobs::BlobSet>>) -> View {
        View {
            generation,
            manifest: self.manifest.clone(),
            etag: self.etag.clone(),
            tip: self.tip,
            tip_ts: self.tip_ts,
            all: self.all.clone(),
            l0_len: self.l0_len,
            baseline: self.baseline,
            recent: Arc::clone(&self.recent),
            warm: self.warm.clone(),
            spans: self.spans.clone(),
            starts: self.starts.clone(),
            warm_floor: self.warm_floor,
            rollups: self.rollups.clone(),
            // A cell being filled as it is cloned comes out empty: that plan is made again.
            plans: self.plans.clone(),
            cold_plan: self.cold_plan.clone(),
            retracted: self.retracted.clone(),
            ends: self.ends.clone(),
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
        let from = if self.recent.may_contain(oid) {
            0
        } else {
            self.baseline
        };
        self.all.iter().enumerate().skip(from).find_map(|(i, s)| {
            s.find(oid)
                .map(|slot| (u16::try_from(i).unwrap_or(u16::MAX), slot))
        })
    }

    /// Starts loading what [`View::find`] reads for `oid` in the baseline (where an id the
    /// newer slabs' filter rules out is).
    #[inline]
    pub fn preload(&self, oid: &Oid) {
        if !self.recent.may_contain(oid)
            && let Some(b) = self.all.get(self.baseline)
        {
            b.preload(oid);
        }
    }

    /// The live L0 slabs, oldest first.
    pub fn l0(&self) -> impl Iterator<Item = &Arc<Slab>> {
        self.all.get(..self.l0_len).unwrap_or(&[]).iter().rev()
    }

    /// The (shared, lazily built) plan for a client whose newest pack is `client_ts`.
    pub fn prefetch(&self, client_ts: u64) -> Prefetch {
        match self.slot(client_ts) {
            Slot::UpToDate => Prefetch::UpToDate,
            Slot::Warm(cell, from) => {
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
            Slot::Cold => Prefetch::Pack(
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

    /// [`View::prefetch`]'s answer if it is ready, `None` if its plan is still to be built
    /// (seconds of work, for a cover that must be repacked).
    pub fn prefetch_ready(&self, client_ts: u64) -> Option<Prefetch> {
        match self.slot(client_ts) {
            Slot::UpToDate => Some(Prefetch::UpToDate),
            Slot::Warm(cell, from) => Some(match cell.get()? {
                Some(p) => Prefetch::Pack(p.clone(), from),
                None => Prefetch::UpToDate,
            }),
            Slot::Cold => self.cold_plan.get().map(|p| Prefetch::Pack(p.clone(), 0)),
        }
    }

    /// Where the plan a client at `client_ts` is answered from is kept, as a number: the
    /// same for every client that shares the plan (they wait for one build of it).
    pub fn plan_key(&self, client_ts: u64) -> usize {
        match self.slot(client_ts) {
            Slot::UpToDate => 0,
            Slot::Warm(cell, _) => std::ptr::from_ref(cell).addr(),
            Slot::Cold => std::ptr::from_ref(&self.cold_plan).addr(),
        }
    }

    /// Where the plan for a client at `client_ts` lives.
    fn slot(&self, client_ts: u64) -> Slot<'_> {
        let client_ts = self.unretracted(client_ts);
        if client_ts != 0 && client_ts >= self.tip_ts {
            return Slot::UpToDate;
        }
        // Whether the client is warm is asked of its cursor, not of the start of a rollup
        // it is sent whole: a retained day that ends at the floor starts below it, and a
        // client sitting on that boundary, which is current there, got the cold pack.
        let warm_client = client_ts >= self.warm_floor;
        let client_ts = crate::timeline::superset_start(&self.rollups, client_ts, self.tip_ts)
            .unwrap_or(client_ts);
        let key = self
            .starts
            .partition_point(|&s| s <= client_ts)
            .checked_sub(1);
        let warm = key.and_then(|i| Some((self.plans.get(i)?, *self.starts.get(i)?)));
        match warm {
            Some((cell, from)) if warm_client => Slot::Warm(cell, from),
            _ => Slot::Cold,
        }
    }

    /// The boundary a client at `ts` holds everything up to: `ts` itself, unless it is a
    /// boundary this edge retracted (from an L0 slab a forced push abandoned, which the
    /// branch's later commits may be stamped below), which stands for the boundary before
    /// it. A boundary this view has is taken as it is.
    fn unretracted(&self, mut ts: u64) -> u64 {
        while !self.retracted.is_empty() && self.ends.binary_search(&ts).is_err() {
            let from = self
                .retracted
                .iter()
                .filter(|&&(from, upto)| from < ts && ts <= upto)
                .map(|&(from, _)| from)
                .min();
            match from {
                Some(f) => ts = f,
                None => break,
            }
        }
        ts
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

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use super::*;

    /// A slab of one blob (`n` says which), covering `(start, end]`.
    fn slab(n: u8, tier: Tier, start: u64, end: u64) -> Arc<Slab> {
        let content = [n; 40];
        let mut z = vec![0u8; crate::zlib::compress_bound(content.len())];
        let len = crate::zlib::Deflater::new(1)
            .deflate_parts(&[&content], &mut z)
            .unwrap();
        let mut pack = crate::pack::pack_header(1).to_vec();
        let header = crate::pack::HeaderBytes::whole(crate::pack::OBJ_BLOB, 40);
        pack.extend_from_slice(header.as_slice());
        pack.extend_from_slice(&z[..len]);
        let trailer = <sha1::Sha1 as sha1::Digest>::digest(&pack);
        pack.extend_from_slice(&trailer);
        let commits = (Oid([n; 20]), Oid([n + 1; 20]));
        crate::ingest::memory_slab(&mut Indexer::new(1), pack, tier, (start, end), commits)
            .unwrap()
            .slab
    }

    /// A client whose cursor is the warm floor is warm. With superset deliveries it is
    /// sent a retained day that ends there whole, from the day's start, which lies below
    /// the floor: for that it got the cold pack.
    #[test]
    fn a_client_at_the_warm_floor_is_warm() {
        let base = slab(1, Tier::Base, 0, 100);
        let week = slab(2, Tier::Week, 100, 200);
        let tail = slab(3, Tier::M15, 200, 300);
        let day = slab(4, Tier::H24, 50, 100);
        let manifest = Manifest {
            generation: 1,
            tip: tail.meta.want,
            max_ts: 300,
            baseline: base.meta.clone(),
            weekly: vec![week.meta.clone()],
            pyramid: vec![tail.meta.clone()],
            retained: vec![day.meta.clone()],
            flags: 0,
            blobs_generation: 0,
        };
        let sealed = vec![base, week, tail, day];
        let view = |supersets: Option<Tier>| {
            let (m, slabs) = (manifest.clone(), sealed.clone());
            View::new(1, m, String::new(), slabs, Vec::new(), supersets, None, &[])
        };
        // Where the answer starts, and how many objects it holds.
        let answer = |v: &View, cursor: u64| match v.prefetch(cursor) {
            Prefetch::Pack(plan, start) => Some((start, plan.entries)),
            Prefetch::UpToDate => None,
        };
        // Exact covers: from the floor, the week and what follows it.
        assert_eq!(answer(&view(None), 100), Some((100, 2)));
        // Supersets: the day that ends at the floor too, not the baseline.
        let supersets = view(Some(Tier::H24));
        assert_eq!(answer(&supersets, 100), Some((50, 3)));
        // Below the floor, or with no cursor: the cold pack (the baseline, the week and
        // what follows; what a retained slab holds is in a real baseline).
        assert_eq!(answer(&supersets, 99), Some((0, 3)));
        assert_eq!(answer(&supersets, 0), Some((0, 3)));
        assert_eq!(answer(&supersets, 300), None);
    }
}
