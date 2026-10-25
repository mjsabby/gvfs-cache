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
//! A pipeline run keeps the rule by working blobs first: it copies the blobs its new
//! commits' trees reference that the store lacks, publishes them in `blobs.bin`, and only
//! then publishes the trees. Which blobs a window's commits reference is found by walking
//! each commit's whole tree against the manifest tip's ([`window_blobs`]), not just the
//! trees in the window's pack: an upstream leaves out of that pack every subtree its haves
//! reach, and such a subtree may hold versions from before genesis (an old side branch
//! merged; a revert, against an upstream with reachability bitmaps).

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::blobcopy::{Copied, copy_item, mode_kind, plan, publish};
use crate::blobs::{BlobManifest, BlobRec, Segment};
use crate::manifest::Manifest;
use crate::object::{TreeIter, commit_tree};
use crate::pack::{OBJ_BLOB, OBJ_COMMIT, OBJ_TREE, type_name};
use crate::sizes::Sizes;
use crate::slab::{Slab, SlabMeta};
use crate::splice::Resolver;
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

/// The blobs the trees of `window`'s commits reference, except blobs and subtrees a tree
/// shares, at the same path, with the tree of commit `base` (the manifest tip, whose blobs
/// are in the store, or will be once `mirror --scope tip` finishes). Every commit is walked
/// from its root tree; trees come from `window` first, then from `store` (the manifest's
/// slabs, oldest first), so a subtree the upstream left out of the window's pack is walked
/// too. Sorted, distinct.
pub fn window_blobs(window: &Arc<Slab>, store: &[Arc<Slab>], base: Oid) -> Result<Vec<Oid>> {
    let mut slabs: Vec<&Arc<Slab>> = Vec::with_capacity(store.len().saturating_add(1));
    slabs.push(window);
    slabs.extend(store.iter().rev());
    let mut w = TreeWalk {
        slabs,
        resolver: Resolver::default(),
        walked: HashSet::new(),
        out: Vec::new(),
    };
    let base_tree = w.commit_tree(&base)?;
    let commits: Vec<Oid> = window
        .records()
        .filter(|r| r.kind == OBJ_COMMIT)
        .map(|r| r.oid)
        .collect();
    for c in &commits {
        let tree = w.commit_tree(c)?;
        w.walk(tree, base_tree)?;
    }
    let mut out = w.out;
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

struct TreeWalk<'a> {
    slabs: Vec<&'a Arc<Slab>>,
    resolver: Resolver,
    /// Trees walked already: each blob of theirs is collected or in `base`'s tree, whatever
    /// path they turn up at again.
    walked: HashSet<Oid>,
    out: Vec<Oid>,
}

impl TreeWalk<'_> {
    fn object(&mut self, oid: &Oid, kind: u8) -> Result<&[u8]> {
        let Some((slab, slot)) = self
            .slabs
            .iter()
            .find_map(|s| s.find(oid).map(|slot| (*s, slot)))
        else {
            crate::bail!(
                "{} {oid} is in neither the window nor the store",
                type_name(kind)
            );
        };
        let (k, content) = self.resolver.resolve(slab, slot)?;
        if k != kind {
            crate::bail!("{oid} is a {}, not a {}", type_name(k), type_name(kind));
        }
        Ok(content)
    }

    fn commit_tree(&mut self, c: &Oid) -> Result<Oid> {
        let content = self.object(c, OBJ_COMMIT)?;
        match commit_tree(content) {
            Some(t) => Ok(t),
            None => crate::bail!("commit {c} names no tree"),
        }
    }

    /// `(name, kind, id)` of each entry of tree `t`.
    fn entries(&mut self, t: &Oid) -> Result<Vec<(Vec<u8>, u8, Oid)>> {
        let content = self.object(t, OBJ_TREE)?;
        let mut it = TreeIter::new(content);
        let mut out = Vec::new();
        for e in &mut it {
            let Some(kind) = mode_kind(e.mode) else {
                crate::bail!("malformed tree {t}");
            };
            out.push((e.name.to_vec(), kind, e.oid));
        }
        if it.malformed {
            crate::bail!("malformed tree {t}");
        }
        Ok(out)
    }

    /// Collects the blobs of `root` that `base` (the tree at the same path in the base
    /// commit) does not have at the same path.
    fn walk(&mut self, root: Oid, base: Oid) -> Result<()> {
        let mut stack: Vec<(Oid, Option<Oid>)> = vec![(root, Some(base))];
        while let Some((t, b)) = stack.pop() {
            if Some(t) == b || !self.walked.insert(t) {
                continue;
            }
            let entries = self.entries(&t)?;
            let theirs: HashMap<Vec<u8>, (u8, Oid)> = match b {
                Some(b) => self
                    .entries(&b)?
                    .into_iter()
                    .map(|(name, kind, oid)| (name, (kind, oid)))
                    .collect(),
                None => HashMap::new(),
            };
            for (name, kind, oid) in entries {
                let same = theirs.get(&name).copied();
                match kind {
                    OBJ_BLOB if same != Some((OBJ_BLOB, oid)) => self.out.push(oid),
                    OBJ_TREE => {
                        let sub = match same {
                            Some((OBJ_TREE, o)) => Some(o),
                            _ => None,
                        };
                        if sub != Some(oid) {
                            stack.push((oid, sub));
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }
}

/// Keeps in a pipeline runner's store cache (`--cache-dir`: downloaded copies of immutable
/// store objects, reused across runs) only what manifest `m` and `blobs.bin` `bm` list.
pub fn prune_cache(dir: &Path, m: &Manifest, bm: &BlobManifest) -> Result<usize> {
    let mut keep: HashSet<String> = HashSet::new();
    for s in m.slabs() {
        keep.insert(format!("{}.pack", s.id));
        keep.insert(format!("{}.mphf", s.id));
        keep.insert(s.sizes_key().replace('/', "_"));
    }
    for seg in &bm.segments {
        keep.insert(format!("{}.bidx", seg.id));
    }
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => {
            return Err(crate::util::Error::from(e)).ctx(&format!("listing {}", dir.display()));
        }
    };
    let mut removed = 0usize;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        // Dot files are downloads in progress.
        if name.starts_with('.') || keep.contains(&name) {
            continue;
        }
        if std::fs::remove_file(entry.path()).is_ok() {
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}
