//! Stores that keep blobs in their own packs (`--blobs separate`): what the pipeline,
//! `mirror` and the edge share.
//!
//! **The consistency rule.** Every blob that a tree of a published manifest references is
//! in a blob pack listed in `blobs.bin` at the generation the manifest records
//! (`blobs_generation`), with two exceptions: blobs that only commits before genesis
//! reference (the manifest says `blobs: partial`; the mirror copies the genesis tip's,
//! and edges read anything else through), and the edge's own L0 overlay. Independently,
//! every such blob's size is in the sizes table of the slab whose trees introduced it, so
//! sizes are complete for every tree a client can see, mirrored or not.
//!
//! A pipeline run keeps the rule by working blobs first: it copies the blobs its new trees
//! reference that the store lacks, publishes them in `blobs.bin`, and only then publishes
//! the trees.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::blobcopy::{Copied, copy_item, plan, publish};
use crate::blobs::{BlobManifest, BlobRec, Segment};
use crate::manifest::Manifest;
use crate::sizes::Sizes;
use crate::slab::SlabMeta;
use crate::store::Store;
use crate::upstream::Upstream;
use crate::util::{Context, Oid, Result};

/// Default bytes of small blobs per blob pack (`--blob-pack-mib`).
pub const ITEM_BYTES: u64 = 2 << 30;

/// Opens an object of the store in place (directory stores) or from a download to
/// `work` (kept: these objects are immutable, so a later run may reuse it).
fn local_copy(store: &Store, work: &Path, key: &str) -> Result<PathBuf> {
    if let Some(p) = store.local_path(key) {
        return Ok(p);
    }
    let name = key.replace('/', "_");
    let p = work.join(name);
    if !p.exists() {
        store.download_file(key, &p)?;
    }
    Ok(p)
}

/// Sizes known from a set of tables.
pub struct KnownSizes {
    tables: Vec<Sizes>,
}

impl KnownSizes {
    pub fn get(&self, oid: &Oid) -> Option<u64> {
        self.tables.iter().find_map(|t| t.get(oid))
    }

    pub fn tables(&self) -> &[Sizes] {
        &self.tables
    }
}

/// The sizes tables of `metas`.
pub fn load_sizes(store: &Store, work: &Path, metas: &[&SlabMeta]) -> Result<KnownSizes> {
    let mut tables = Vec::with_capacity(metas.len());
    for m in metas {
        let path = local_copy(store, work, &m.sizes_key())?;
        tables.push(Sizes::open(&path).ctx(&format!("sizes of slab {}", m.id))?);
    }
    Ok(KnownSizes { tables })
}

/// Every size a manifest's trees need: the baseline's table and the pyramid's (a
/// rollup's table is the union of its parts', a new baseline's the old one's plus the
/// week's, so these cover every slab ever published).
pub fn known_sizes(store: &Store, work: &Path, m: &Manifest) -> Result<KnownSizes> {
    let metas: Vec<&SlabMeta> = std::iter::once(&m.baseline).chain(&m.pyramid).collect();
    load_sizes(store, work, &metas)
}

/// Uploads `table` as slab `meta`'s sizes table.
pub fn put_sizes(store: &Store, meta: &SlabMeta, table: &[u8]) -> Result<()> {
    let mut w = store.writer(&meta.sizes_key())?;
    w.write_all(table).ctx("uploading a sizes table")?;
    w.finish()
}

/// Uploads the union of `parts`' sizes tables as `out`'s.
pub fn union_sizes(store: &Store, work: &Path, parts: &[&SlabMeta], out: &SlabMeta) -> Result<()> {
    let known = load_sizes(store, work, parts)?;
    let tables: Vec<&Sizes> = known.tables.iter().collect();
    put_sizes(store, out, &crate::sizes::merge(&tables))
}

/// Lookups across the segments `blobs.bin` lists.
pub struct BlobIndex {
    segments: Vec<Segment>,
}

impl BlobIndex {
    pub fn load(store: &Store, work: &Path, bm: &BlobManifest) -> Result<BlobIndex> {
        let mut segments = Vec::with_capacity(bm.segments.len());
        for s in &bm.segments {
            segments.push(crate::blobcopy::open_segment(store, work, s)?);
        }
        Ok(BlobIndex { segments })
    }

    /// The blob's record and the segment that has it.
    pub fn find(&self, oid: &Oid) -> Option<(&Segment, BlobRec)> {
        self.segments
            .iter()
            .find_map(|s| s.find(oid).map(|r| (s, r)))
    }
}

/// Copies the blobs `need` into new blob packs (sizes asked of the upstream first, so
/// large blobs stream alone) and publishes them in `blobs.bin` as one segment. Returns the
/// published `blobs.bin` (`None` if nothing was needed) and every copied blob's size, as
/// verified by hashing its content.
pub fn copy_needed(
    up: &mut Upstream,
    store: &Store,
    work: &Path,
    need: &[Oid],
    item_bytes: u64,
    threads: usize,
) -> Result<(Option<BlobManifest>, HashMap<Oid, u64>)> {
    let mut got = HashMap::with_capacity(need.len());
    if need.is_empty() {
        return Ok((None, got));
    }
    let reported = up.object_sizes(need)?;
    let mut wants = Vec::with_capacity(need.len());
    for (oid, size) in need.iter().zip(reported) {
        let Some(size) = size else {
            crate::bail!("the upstream has no blob {oid}, which a tree references");
        };
        wants.push((*oid, size));
    }
    let items = plan(&wants, item_bytes);
    let spool = work.join("blob-spool");
    let mut copied: Vec<Copied> = Vec::with_capacity(items.len());
    for item in &items {
        copied.push(copy_item(up, store, &spool, item)?);
    }
    for c in &copied {
        for r in &c.recs {
            got.insert(r.oid, r.size);
        }
    }
    for (oid, size) in &wants {
        match got.get(oid) {
            Some(s) if s == size => {}
            Some(s) => {
                crate::bail!("blob {oid}: the upstream reported {size} bytes, its content has {s}")
            }
            None => crate::bail!("blob {oid} was not copied"),
        }
    }
    let bytes: u64 = wants.iter().map(|w| w.1).sum();
    let bm = publish(store, work, &copied, threads, |_| Ok(()))?;
    crate::log!(
        "copied {} blobs ({}) into {} blob packs; blobs.bin generation {}",
        wants.len(),
        crate::util::human_bytes(bytes),
        copied.len(),
        bm.generation
    );
    Ok((Some(bm), got))
}
