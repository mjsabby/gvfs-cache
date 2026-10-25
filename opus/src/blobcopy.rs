//! Copying blobs from the upstream into the store (`--blobs separate`): the one writer of
//! blob packs, used by the pipeline (each window's new blobs, before its trees) and by
//! `mirror` (the seed).
//!
//! * [`plan`] cuts the work into items, in the order given (path order for the mirror, so
//!   a sparse cone maps to few packs). A blob of at least [`LARGE_MIN`] is an item of its
//!   own: one request, streamed straight into the store, with no local disk and fixed
//!   memory whatever its size. Smaller blobs are batched into items of up to `item_bytes`,
//!   fetched [`BATCH`] ids per request and spooled to a local file, where the deltas a
//!   batch may hold are resolved against bases read back ([`crate::blobstream`]).
//! * [`copy_item`] turns an item into one blob pack in the store. Every blob is verified
//!   by its id as it streams through. Pack ids are fresh per copy, so a pack is never
//!   overwritten once written (a crashed copy leaves an unlisted pack that GC removes).
//! * [`publish`] indexes finished packs with a segment and appends both to `blobs.bin` by
//!   compare-and-swap, where readers find them; [`fold`] then merges the smallest
//!   segments as needed to keep them a geometric progression by blob count ([`fold_set`],
//!   git's `repack --geometric`): O(log n) segments to probe, and each record rewritten
//!   (and downloaded again by every edge) O(log n) times, not once per fold.
//! * [`referenced_blobs`] lists the blobs a slab's trees reference.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use sha1::{Digest, Sha1};

use crate::blobs::{
    BLOBS_KEY, BlobManifest, BlobRec, PackMeta, PackRef, Segment, SegmentMeta, build_segment,
    pack_key, segment_key,
};
use crate::blobstream::{BlobSink, FileSpool, Spool, Throttle};
use crate::object::TreeIter;
use crate::pack::OBJ_TREE;
use crate::slab::Slab;
use crate::store::{Cond, Store};
use crate::upstream::{BATCH, Upstream};
use crate::util::{Context, Oid, Result, hex_string, le_u32, u32_of, u64_of};

/// Blobs at least this big are items of their own: one request, streamed to the store.
pub const LARGE_MIN: u64 = 16 << 20;
/// Most blobs in one batch item.
const ITEM_BLOBS_MAX: usize = 1 << 20;
/// Each segment holds at least this many times the blobs of the next smaller one.
pub const FOLD_FACTOR: u64 = 2;
const CAS_ATTEMPTS: u32 = 50;
const COPY_ATTEMPTS: u32 = 3;

/// A unit of copy work: one blob pack.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Item {
    Large { oid: Oid, size: u64 },
    Batch { oids: Vec<Oid>, bytes: u64 },
}

impl Item {
    pub fn oids(&self) -> &[Oid] {
        match self {
            Item::Large { oid, .. } => std::slice::from_ref(oid),
            Item::Batch { oids, .. } => oids,
        }
    }

    pub fn bytes(&self) -> u64 {
        match self {
            Item::Large { size, .. } => *size,
            Item::Batch { bytes, .. } => *bytes,
        }
    }
}

/// Cuts `wants` (`(id, size)`, in the order given; repeats are dropped) into items.
pub fn plan(wants: &[(Oid, u64)], item_bytes: u64) -> Vec<Item> {
    let mut seen = HashSet::with_capacity(wants.len());
    let mut items = Vec::new();
    let mut oids = Vec::new();
    let mut bytes = 0u64;
    for &(oid, size) in wants {
        if !seen.insert(oid) {
            continue;
        }
        if size >= LARGE_MIN {
            items.push(Item::Large { oid, size });
            continue;
        }
        if !oids.is_empty()
            && (bytes.saturating_add(size) > item_bytes || oids.len() >= ITEM_BLOBS_MAX)
        {
            items.push(Item::Batch {
                oids: std::mem::take(&mut oids),
                bytes,
            });
            bytes = 0;
        }
        oids.push(oid);
        bytes = bytes.saturating_add(size);
    }
    if !oids.is_empty() {
        items.push(Item::Batch { oids, bytes });
    }
    items
}

/// One copied item: its pack and its blobs' records (pack index 0).
#[derive(Clone, Debug)]
pub struct Copied {
    pub pack: PackMeta,
    pub recs: Vec<BlobRec>,
}

static NONCE_SEQ: AtomicU64 = AtomicU64::new(0);

/// A pack id no other copy uses: a hash of the ids and a per-copy nonce.
pub fn fresh_id(oids: &[Oid]) -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut h = Sha1::new();
    let mut nonce = std::collections::hash_map::RandomState::new().build_hasher();
    nonce.write_u64(NONCE_SEQ.fetch_add(1, Ordering::Relaxed));
    nonce.write_u32(std::process::id());
    h.update(nonce.finish().to_le_bytes());
    h.update(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
            .to_le_bytes(),
    );
    for o in oids {
        h.update(o.0);
    }
    hex_string(&h.finalize())
}

/// One fetch into the sink; the sink's own error wins over the transport's report of it.
fn fetch_into<S: Spool>(up: &mut Upstream, sink: &mut BlobSink<S>, oids: &[Oid]) -> Result<()> {
    if let Err(e) = up.fetch_objects(oids, sink) {
        return Err(sink.failure().unwrap_or(e));
    }
    sink.end_input()
}

/// Copies `item` from the upstream into a new blob pack in the store (retrying transient
/// failures); `work` holds the spool of a batch.
pub fn copy_item(up: &mut Upstream, store: &Store, work: &Path, item: &Item) -> Result<Copied> {
    copy_item_with(up, store, work, item, None)
}

/// [`copy_item`], paced by a shared [`Throttle`].
pub fn copy_item_with(
    up: &mut Upstream,
    store: &Store,
    work: &Path,
    item: &Item,
    throttle: Option<&Arc<Throttle>>,
) -> Result<Copied> {
    let mut attempt = 0u32;
    loop {
        match copy_once(up, store, work, item, throttle) {
            Err(e) if attempt.saturating_add(1) < COPY_ATTEMPTS => {
                crate::log!("blob copy failed ({e}); retrying");
                attempt = attempt.saturating_add(1);
                std::thread::sleep(std::time::Duration::from_millis(
                    500u64.saturating_mul(1u64.wrapping_shl(attempt.saturating_mul(2))),
                ));
            }
            other => return other,
        }
    }
}

fn copy_once(
    up: &mut Upstream,
    store: &Store,
    work: &Path,
    item: &Item,
    throttle: Option<&Arc<Throttle>>,
) -> Result<Copied> {
    let id = fresh_id(item.oids());
    let key = pack_key(&id);
    let (len, sha, recs) = match item {
        Item::Large { oid, .. } => {
            let one = std::slice::from_ref(oid);
            let mut sink = BlobSink::new(store.writer(&key)?, one)?;
            if let Some(t) = throttle {
                sink.throttle(Arc::clone(t));
            }
            fetch_into(up, &mut sink, one)?;
            let f = sink.finish()?;
            f.out.finish()?;
            (f.len, f.sha, f.recs)
        }
        Item::Batch { oids, .. } => {
            std::fs::create_dir_all(work).ctx(&format!("creating {}", work.display()))?;
            let path = work.join(format!("{id}.pack"));
            let result = (|| -> Result<(u64, [u8; 20], Vec<BlobRec>)> {
                let mut sink = BlobSink::new(FileSpool::create(&path)?, oids)?;
                if let Some(t) = throttle {
                    sink.throttle(Arc::clone(t));
                }
                for chunk in oids.chunks(BATCH) {
                    fetch_into(up, &mut sink, chunk)?;
                }
                let f = sink.finish()?;
                f.out.close()?;
                store.upload_file(&key, &path)?;
                Ok((f.len, f.sha, f.recs))
            })();
            let _ = std::fs::remove_file(&path);
            result?
        }
    };
    Ok(Copied {
        pack: PackMeta {
            id,
            len,
            sha,
            blobs: u32_of(recs.len()),
        },
        recs,
    })
}

/// The current `blobs.bin` and its etag (`None`: there is none yet).
pub fn load(store: &Store) -> Result<(BlobManifest, Option<String>)> {
    match store.get(BLOBS_KEY)? {
        Some((bytes, etag)) => Ok((
            BlobManifest::decode(&bytes).ctx("reading blobs.bin")?,
            Some(etag),
        )),
        None => Ok((BlobManifest::default(), None)),
    }
}

/// Applies `edit` to `blobs.bin` by compare-and-swap (re-reading and re-applying on a lost
/// race); every successful swap bumps the generation. Returns the published manifest.
pub fn update(
    store: &Store,
    mut edit: impl FnMut(&mut BlobManifest) -> Result<()>,
) -> Result<BlobManifest> {
    for attempt in 0..CAS_ATTEMPTS {
        let (mut m, etag) = load(store)?;
        edit(&mut m)?;
        m.generation = m.generation.saturating_add(1);
        let cond = match etag {
            Some(e) => Cond::IfMatch(e),
            None => Cond::IfNoneMatch,
        };
        match store.put(BLOBS_KEY, &m.encode(), cond) {
            Ok(_) => return Ok(m),
            Err(e) if e.is_conflict() => std::thread::sleep(std::time::Duration::from_millis(
                20u64.saturating_mul(u64::from(attempt).saturating_add(1)),
            )),
            Err(e) => return Err(e),
        }
    }
    crate::bail!("blobs.bin: gave up after {CAS_ATTEMPTS} lost races")
}

/// Writes a segment over `packs` and `recs` to the store; returns its `blobs.bin` entry.
fn write_segment(
    store: &Store,
    packs: &[PackRef],
    recs: &[BlobRec],
    threads: usize,
) -> Result<SegmentMeta> {
    let bytes = build_segment(packs, recs, threads)?;
    let id = hex_string(&Sha1::digest(&bytes));
    let mut w = store.writer(&segment_key(&id))?;
    w.write_all(&bytes).ctx("uploading a blob segment")?;
    w.finish()?;
    Ok(SegmentMeta {
        blobs: u64::from(le_u32(&bytes, 12).ctx("segment header")?),
        index_len: u64_of(bytes.len()),
        packs: packs.iter().map(|p| p.id.clone()).collect(),
        id,
    })
}

/// Indexes the `copied` packs with a new segment and appends both to `blobs.bin` in one
/// compare-and-swap, together with `edit` (e.g. recording `tip complete`); then folds the
/// segments if there are too many. Returns the published `blobs.bin`.
pub fn publish(
    store: &Store,
    work: &Path,
    copied: &[Copied],
    threads: usize,
    mut edit: impl FnMut(&mut BlobManifest) -> Result<()>,
) -> Result<BlobManifest> {
    let segment = if copied.is_empty() {
        None
    } else {
        let refs: Vec<PackRef> = copied
            .iter()
            .map(|c| PackRef {
                id: c.pack.id.clone(),
                len: c.pack.len,
                sha: c.pack.sha,
            })
            .collect();
        let recs: Vec<BlobRec> = copied
            .iter()
            .enumerate()
            .flat_map(|(i, c)| {
                c.recs.iter().map(move |r| BlobRec {
                    pack: u32_of(i),
                    ..*r
                })
            })
            .collect();
        Some(write_segment(store, &refs, &recs, threads)?)
    };
    let m = update(store, |m| {
        if let Some(s) = &segment {
            if !m.segments.iter().any(|x| x.id == s.id) {
                m.segments.push(s.clone());
            }
            for c in copied {
                if !m.packs.iter().any(|p| p.id == c.pack.id) {
                    m.packs.push(c.pack.clone());
                }
            }
        }
        edit(m)
    })?;
    if fold_set(&segment_counts(&m)).len() >= 2 {
        let pick = |m: &BlobManifest| -> Vec<String> {
            fold_set(&segment_counts(m))
                .into_iter()
                .filter_map(|i| m.segments.get(i).map(|s| s.id.clone()))
                .collect()
        };
        if let Some(folded) = fold(store, work, threads, pick, &HashSet::new())? {
            return Ok(folded);
        }
    }
    Ok(m)
}

fn segment_counts(m: &BlobManifest) -> Vec<u64> {
    m.segments.iter().map(|s| s.blobs).collect()
}

/// The segments (indexes into `counts`, their blob counts) to merge so that, by blob count,
/// they form a geometric progression with factor [`FOLD_FACTOR`] again: the smallest ones
/// up to the first pair out of progression, plus any bigger one their merge would not be
/// [`FOLD_FACTOR`] times smaller than (git's `repack --geometric`). Empty: nothing to merge.
pub fn fold_set(counts: &[u64]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..counts.len()).collect();
    order.sort_by_key(|&i| counts.get(i).copied().unwrap_or(0));
    let w: Vec<u64> = order
        .iter()
        .map(|&i| counts.get(i).copied().unwrap_or(0).max(1))
        .collect();
    let mut i = w.len().saturating_sub(1);
    while i > 0 {
        let below = i.saturating_sub(1);
        let (Some(&ours), Some(&prev)) = (w.get(i), w.get(below)) else {
            break;
        };
        if ours < FOLD_FACTOR.saturating_mul(prev) {
            break;
        }
        i = below;
    }
    let mut split = if i > 0 { i.saturating_add(1) } else { 0 };
    let mut total = w
        .get(..split)
        .unwrap_or(&[])
        .iter()
        .fold(0u64, |a, &b| a.saturating_add(b));
    while let Some(&ours) = w.get(split) {
        if split == 0 || ours >= FOLD_FACTOR.saturating_mul(total) {
            break;
        }
        total = total.saturating_add(ours);
        split = split.saturating_add(1);
    }
    if split < 2 {
        return Vec::new();
    }
    order.truncate(split);
    order
}

/// Opens a segment listed in `blobs.bin` (in place for directory stores, else downloaded
/// to `work`).
pub fn open_segment(store: &Store, work: &Path, meta: &SegmentMeta) -> Result<Segment> {
    let key = segment_key(&meta.id);
    let path = match store.local_path(&key) {
        Some(p) => p,
        None => {
            let p = work.join(format!("{}.bidx", meta.id));
            if !p.exists() {
                store.download_file(&key, &p)?;
            }
            p
        }
    };
    Segment::open(&path, &meta.id)
}

/// Merges the segments `pick` names (from the `blobs.bin` current when it runs) into one,
/// leaving out the records of packs in `lost`; packs no segment indexes any more leave
/// `blobs.bin`. `None` when there is nothing to do, or another writer changed those
/// segments meanwhile (it folds, or the next publish does).
pub fn fold(
    store: &Store,
    work: &Path,
    threads: usize,
    pick: impl Fn(&BlobManifest) -> Vec<String>,
    lost: &HashSet<String>,
) -> Result<Option<BlobManifest>> {
    let (m, _) = load(store)?;
    let picked: HashSet<String> = pick(&m).into_iter().collect();
    let metas: Vec<&SegmentMeta> = m
        .segments
        .iter()
        .filter(|s| picked.contains(&s.id))
        .collect();
    if metas.is_empty() || (metas.len() < 2 && lost.is_empty()) {
        return Ok(None);
    }
    let mut refs: Vec<PackRef> = Vec::new();
    let mut index: HashMap<String, u32> = HashMap::new();
    let mut recs: Vec<BlobRec> = Vec::new();
    let mut dropped = 0usize;
    for meta in &metas {
        let seg = open_segment(store, work, meta)?;
        let mut remap: Vec<Option<u32>> = Vec::with_capacity(seg.packs().len());
        for p in seg.packs() {
            remap.push(if lost.contains(&p.id) {
                None
            } else {
                Some(*index.entry(p.id.clone()).or_insert_with(|| {
                    refs.push(p.clone());
                    u32_of(refs.len().saturating_sub(1))
                }))
            });
        }
        for r in seg.records() {
            match remap
                .get(crate::util::us(u64::from(r.pack)))
                .ctx("segment record names no pack")?
            {
                Some(pack) => recs.push(BlobRec { pack: *pack, ..r }),
                None => dropped = dropped.saturating_add(1),
            }
        }
    }
    let merged = if recs.is_empty() {
        None
    } else {
        Some(write_segment(store, &refs, &recs, threads)?)
    };
    let folded: HashSet<String> = metas.iter().map(|s| s.id.clone()).collect();
    let mut stale = false;
    let out = update(store, |cur| {
        stale = !folded
            .iter()
            .all(|id| cur.segments.iter().any(|s| &s.id == id));
        if stale {
            crate::bail!("blobs.bin: segments changed during a fold");
        }
        let at = cur
            .segments
            .iter()
            .position(|s| folded.contains(&s.id))
            .unwrap_or(0);
        cur.segments.retain(|s| !folded.contains(&s.id));
        if let Some(mg) = &merged {
            cur.segments.insert(at.min(cur.segments.len()), mg.clone());
        }
        let indexed: HashSet<String> = cur
            .segments
            .iter()
            .flat_map(|s| s.packs.iter().cloned())
            .collect();
        cur.packs.retain(|p| indexed.contains(&p.id));
        Ok(())
    });
    let out = match out {
        Err(_) if stale => return Ok(None),
        other => other?,
    };
    crate::log!(
        "blobs.bin: folded {} segments into {}{}",
        folded.len(),
        merged.as_ref().map_or_else(
            || "none".to_string(),
            |g| format!("{} ({} blobs)", g.id, g.blobs)
        ),
        if dropped > 0 {
            format!(", dropping {dropped} records of lost packs")
        } else {
            String::new()
        }
    );
    Ok(Some(out))
}

/// The type of object a tree entry's mode names: a directory is a tree, a gitlink a
/// commit (of another repository), anything else a blob, whatever the zero padding (as
/// git reads modes). `None` if the mode is not octal.
pub fn mode_kind(mode: &[u8]) -> Option<u8> {
    if mode.is_empty() || mode.len() > 7 {
        return None;
    }
    let mut v = 0u32;
    for &c in mode {
        if !(b'0'..=b'7').contains(&c) {
            return None;
        }
        v = v.wrapping_shl(3) | u32::from(c.wrapping_sub(b'0'));
    }
    Some(match v & 0o170_000 {
        0o040_000 => OBJ_TREE,
        0o160_000 => crate::pack::OBJ_COMMIT,
        _ => crate::pack::OBJ_BLOB,
    })
}

/// Every blob id the trees of `slab` reference: sorted, distinct.
pub fn referenced_blobs(slab: &Slab, threads: usize) -> Result<Vec<Oid>> {
    const STEP: usize = 4096;
    let trees: Vec<u32> = slab
        .records()
        .enumerate()
        .filter(|(_, r)| r.kind == OBJ_TREE)
        .map(|(slot, _)| u32_of(slot))
        .collect();
    let next = AtomicUsize::new(0);
    let threads = threads.clamp(1, 64);
    let parts = std::thread::scope(|sc| -> Result<Vec<Vec<Oid>>> {
        let workers: Vec<_> = (0..threads)
            .map(|_| {
                sc.spawn(|| -> Result<Vec<Oid>> {
                    let mut resolver = crate::splice::Resolver::default();
                    let mut out: Vec<Oid> = Vec::new();
                    loop {
                        let start = next.fetch_add(STEP, Ordering::Relaxed);
                        let Some(slots) =
                            trees.get(start..start.saturating_add(STEP).min(trees.len()))
                        else {
                            break;
                        };
                        if slots.is_empty() {
                            break;
                        }
                        for &slot in slots {
                            let (kind, content) = resolver.resolve(slab, slot)?;
                            if kind != OBJ_TREE {
                                crate::bail!("slot {slot} is not a tree");
                            }
                            let mut it = TreeIter::new(content);
                            let mut bad = it.malformed;
                            for e in &mut it {
                                match mode_kind(e.mode) {
                                    Some(crate::pack::OBJ_BLOB) => out.push(e.oid),
                                    Some(_) => {}
                                    None => bad = true,
                                }
                            }
                            if bad || it.malformed {
                                let oid = slab.rec(slot).map(|r| r.oid).unwrap_or(Oid::ZERO);
                                crate::bail!("malformed tree {oid}");
                            }
                        }
                        if out.len() > 1 << 22 {
                            out.sort_unstable();
                            out.dedup();
                        }
                    }
                    out.sort_unstable();
                    out.dedup();
                    Ok(out)
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|w| {
                w.join()
                    .map_err(|_| crate::err!("tree walk worker failed"))?
            })
            .collect()
    })?;
    let mut all: Vec<Oid> = parts.into_iter().flatten().collect();
    all.sort_unstable();
    all.dedup();
    Ok(all)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
mod tests {
    use super::*;

    #[test]
    fn items_keep_order_and_isolate_large_blobs() {
        let o = |i: u8| Oid([i; 20]);
        let wants = [
            (o(1), 10),
            (o(2), LARGE_MIN),
            (o(3), 60),
            (o(1), 10),
            (o(4), 50),
            (o(5), 0),
            (o(6), 100),
        ];
        assert_eq!(
            plan(&wants, 100),
            vec![
                Item::Large {
                    oid: o(2),
                    size: LARGE_MIN
                },
                Item::Batch {
                    oids: vec![o(1), o(3)],
                    bytes: 70
                },
                Item::Batch {
                    oids: vec![o(4), o(5)],
                    bytes: 50
                },
                Item::Batch {
                    oids: vec![o(6)],
                    bytes: 100
                },
            ]
        );
        assert!(plan(&[], 100).is_empty());
        // Geometric folding (by blob count): nothing while each segment is at least twice
        // the next smaller; else the smallest ones, and any bigger one they would not be
        // twice smaller than.
        let sorted = |mut v: Vec<usize>| {
            v.sort_unstable();
            v
        };
        assert!(fold_set(&[]).is_empty() && fold_set(&[5]).is_empty());
        assert!(fold_set(&[8, 4, 2, 1]).is_empty());
        assert!(fold_set(&[1000, 1]).is_empty());
        assert_eq!(sorted(fold_set(&[1, 1])), vec![0, 1]);
        assert_eq!(sorted(fold_set(&[10, 6])), vec![0, 1]);
        assert_eq!(sorted(fold_set(&[100, 10, 3, 1, 1])), vec![2, 3, 4]);
        assert_eq!(sorted(fold_set(&[1, 1, 100, 3, 10])), vec![0, 1, 3]);
        // Publishing one blob at a time: few segments, each record rewritten few times.
        let (mut segs, mut rewritten) = (Vec::<u64>::new(), 0u64);
        for _ in 0..100_000 {
            segs.push(1);
            let f = fold_set(&segs);
            if f.len() >= 2 {
                let merged: u64 = f.iter().map(|&i| segs[i]).sum();
                rewritten += merged;
                let mut k = 0;
                segs.retain(|_| {
                    k += 1;
                    !f.contains(&(k - 1))
                });
                segs.push(merged);
            }
            assert!(segs.len() <= 18, "{segs:?}");
            assert!(fold_set(&segs).is_empty());
        }
        assert!(rewritten <= 100_000 * 18, "{rewritten}");
        use crate::pack::{OBJ_BLOB, OBJ_COMMIT};
        for (mode, kind) in [
            (&b"100644"[..], Some(OBJ_BLOB)),
            (b"100755", Some(OBJ_BLOB)),
            (b"120000", Some(OBJ_BLOB)),
            (b"100664", Some(OBJ_BLOB)),
            (b"40000", Some(OBJ_TREE)),
            (b"040000", Some(OBJ_TREE)),
            (b"160000", Some(OBJ_COMMIT)),
            (b"", None),
            (b"1006x4", None),
            (b"12345678", None),
        ] {
            assert_eq!(mode_kind(mode), kind, "{mode:?}");
        }
    }
}
