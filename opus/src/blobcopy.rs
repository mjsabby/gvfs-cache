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
//!   compare-and-swap, where readers find them; [`fold`] merges the segments once there
//!   are more than [`FOLD_AT`], so a lookup probes a few at most.
//! * [`referenced_blobs`] lists the blobs a slab's trees reference.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use sha1::{Digest, Sha1};

use crate::blobs::{
    BLOBS_KEY, BlobManifest, BlobRec, PackMeta, PackRef, Segment, SegmentMeta, build_segment,
    pack_key, segment_key,
};
use crate::blobstream::{BlobSink, FileSpool, Spool};
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
/// `blobs.bin` lists at most this many segments; [`fold`] merges them beyond.
pub const FOLD_AT: usize = 16;
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
fn fresh_id(oids: &[Oid]) -> String {
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
    let mut attempt = 0u32;
    loop {
        match copy_once(up, store, work, item) {
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

fn copy_once(up: &mut Upstream, store: &Store, work: &Path, item: &Item) -> Result<Copied> {
    let id = fresh_id(item.oids());
    let key = pack_key(&id);
    let (len, sha, recs) = match item {
        Item::Large { oid, .. } => {
            let one = std::slice::from_ref(oid);
            let mut sink = BlobSink::new(store.writer(&key)?, one)?;
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
    if m.segments.len() > FOLD_AT {
        if let Some(folded) = fold(store, work, threads)? {
            return Ok(folded);
        }
    }
    Ok(m)
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

/// Merges every segment of `blobs.bin` into one. `None` if another writer changed the
/// segments meanwhile (it folds, or the next publish does).
pub fn fold(store: &Store, work: &Path, threads: usize) -> Result<Option<BlobManifest>> {
    let (m, _) = load(store)?;
    if m.segments.len() < 2 {
        return Ok(None);
    }
    let mut refs: Vec<PackRef> = Vec::new();
    let mut index: HashMap<String, u32> = HashMap::new();
    let mut recs: Vec<BlobRec> = Vec::new();
    for meta in &m.segments {
        let seg = open_segment(store, work, meta)?;
        let mut remap = Vec::with_capacity(seg.packs().len());
        for p in seg.packs() {
            let at = *index.entry(p.id.clone()).or_insert_with(|| {
                refs.push(p.clone());
                u32_of(refs.len().saturating_sub(1))
            });
            remap.push(at);
        }
        for r in seg.records() {
            let pack = *remap
                .get(crate::util::us(u64::from(r.pack)))
                .ctx("segment record names no pack")?;
            recs.push(BlobRec { pack, ..r });
        }
    }
    let merged = write_segment(store, &refs, &recs, threads)?;
    let folded: HashSet<String> = m.segments.iter().map(|s| s.id.clone()).collect();
    let mut stale = false;
    let out = update(store, |cur| {
        stale = !folded
            .iter()
            .all(|id| cur.segments.iter().any(|s| &s.id == id));
        if !stale {
            cur.segments.retain(|s| !folded.contains(&s.id));
            cur.segments.insert(0, merged.clone());
        }
        Ok(())
    })?;
    if stale {
        return Ok(None);
    }
    crate::log!(
        "blobs.bin: folded {} segments into {} ({} blobs)",
        folded.len(),
        merged.id,
        merged.blobs
    );
    Ok(Some(out))
}

/// Whether a tree entry's mode makes it a blob (git: anything but a directory or a
/// gitlink, whatever the zero padding).
fn is_blob_mode(mode: &[u8]) -> bool {
    let mut v = 0u32;
    for &c in mode {
        if !(b'0'..=b'7').contains(&c) {
            return false;
        }
        v = v.wrapping_shl(3) | u32::from(c.wrapping_sub(b'0'));
    }
    !matches!(v & 0o170_000, 0o040_000 | 0o160_000)
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
                            for e in &mut it {
                                if is_blob_mode(e.mode) {
                                    out.push(e.oid);
                                }
                            }
                            if it.malformed {
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
        assert!(is_blob_mode(b"100644") && is_blob_mode(b"100755") && is_blob_mode(b"120000"));
        assert!(!is_blob_mode(b"40000") && !is_blob_mode(b"040000") && !is_blob_mode(b"160000"));
    }
}
