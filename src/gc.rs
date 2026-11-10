//! Garbage collection of a store: the pass a pipeline run ends with.
//!
//! A pass deletes the files that no listing references (`manifest.bin`, and `blobs.bin`
//! for a store with `--blobs separate`) once they are older than the grace period: what a
//! crashed run uploaded and never published, and what later manifests dropped.
//!
//! It is two steps. [`Pass::survey`] lists the store and picks what is due;
//! [`Pass::sweep`] deletes it. Another run may work between the two: it builds a slab
//! whose files are in the store, unlisted (ids are content-addressed: the rollup an
//! earlier run uploaded before it died has the id of the same rollup made again), uploads
//! it and publishes a manifest that lists it. So each delete is conditional
//! ([`Store::delete_if`]): it goes through only if the object still is the one the survey
//! listed, and an upload always makes another object ([`Store::upload_file`]). (A pass
//! deleted whatever had the key by then, and an upload to a directory left a file of that
//! name as it was: the manifest then listed a slab that was gone, both runs having
//! exited 0.)

use std::collections::HashSet;

use crate::manifest::Manifest;
use crate::slab::SlabMeta;
use crate::store::{ObjectInfo, Store};
use crate::util::{Context, Error, Result};

/// A pass after its listing: what it found due.
pub struct Pass {
    /// Unreferenced and older than the grace period, as listed.
    due: Vec<ObjectInfo>,
}

/// What a pass did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Swept {
    pub deleted: usize,
    /// Due, but written again after the pass had listed them: left.
    pub replaced: usize,
}

/// The keys the listings reference: the files of every slab of `m`, and for a store with
/// `--blobs separate` their sizes tables and the packs and segments of `blobs.bin` as it
/// is now.
fn referenced(store: &Store, m: &Manifest) -> Result<HashSet<String>> {
    let mut keys: HashSet<String> = m
        .slabs()
        .flat_map(|s| [s.pack_key(), s.index_key()])
        .collect();
    // The baseline's tail on the one before it, for the edges that still have that one.
    keys.insert(m.baseline.tail_key());
    if m.separate_blobs() {
        keys.extend(m.slabs().map(SlabMeta::sizes_key));
        // Blob packs and segments: only what `blobs.bin` lists now is kept; a copy that has
        // not published yet is younger than the grace period.
        let (bm, _) = crate::blobcopy::load(store)?;
        keys.extend(bm.packs.iter().map(|p| crate::blobs::pack_key(&p.id)));
        keys.extend(bm.segments.iter().map(|s| crate::blobs::segment_key(&s.id)));
    }
    Ok(keys)
}

impl Pass {
    /// Lists the store, whose manifest is `m`, at `now`: the objects that no listing
    /// references and that were written more than `grace` seconds ago are due. A prefix
    /// that cannot be listed ends the pass.
    pub fn survey(store: &Store, m: &Manifest, now: u64, grace: u64) -> Result<Pass> {
        let referenced = referenced(store, m)?;
        let cutoff = now.saturating_sub(grace);
        let mut due = Vec::new();
        let prefixes: &[&str] = if m.separate_blobs() {
            &["slabs/", "blobs/"]
        } else {
            &["slabs/"]
        };
        for prefix in prefixes {
            let listed = store.list(prefix).ctx(&format!("listing {prefix}"))?;
            due.extend(
                listed
                    .into_iter()
                    .filter(|obj| !referenced.contains(&obj.key) && obj.modified < cutoff),
            );
        }
        Ok(Pass { due })
    }

    /// The keys of what is due.
    pub fn due(&self) -> impl Iterator<Item = &str> {
        self.due.iter().map(|obj| obj.key.as_str())
    }

    /// Deletes what is due and still is the object the survey listed. A file that cannot
    /// be deleted is passed over and the rest collected; the error then says how many
    /// there were and names the first.
    pub fn sweep(self, store: &Store) -> Result<Swept> {
        let mut swept = Swept::default();
        // The files that could not be deleted: how many, and the first one's error.
        let mut kept: Option<(usize, Error)> = None;
        for obj in &self.due {
            // A file that cannot be deleted does not keep the ones listed after it: it
            // did, at every run.
            match store.delete_if(&obj.key, &obj.version) {
                Ok(true) => swept.deleted = swept.deleted.saturating_add(1),
                Ok(false) => swept.replaced = swept.replaced.saturating_add(1),
                Err(e) => {
                    kept = Some(match kept {
                        Some((n, first)) => (n.saturating_add(1), first),
                        None => (1, e),
                    });
                }
            }
        }
        if swept.deleted > 0 {
            crate::log!("gc: deleted {} unreferenced slab files", swept.deleted);
        }
        if swept.replaced > 0 {
            crate::log!(
                "gc: {} unreferenced slab files were uploaded again after they were listed: \
                 left",
                swept.replaced
            );
        }
        match kept {
            None => Ok(swept),
            Some((n, first)) => Err(crate::err!(
                "{n} unreferenced files were not deleted, the first: {first}"
            )),
        }
    }
}

/// One pass over `store`, whose manifest is `m`: [`Pass::survey`], then [`Pass::sweep`].
pub fn collect(store: &Store, m: &Manifest, now: u64, grace: u64) -> Result<Swept> {
    Pass::survey(store, m, now, grace)?.sweep(store)
}
