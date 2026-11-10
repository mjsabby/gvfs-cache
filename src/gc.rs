//! Garbage collection of a store: the pass a pipeline run ends with, and `gc.bin`, where
//! passes note what they found unlisted.
//!
//! Runs take objects out of the listings (`manifest.bin`, and `blobs.bin` for a store with
//! `--blobs separate`): on most runs a retained slab that has aged out, every week the
//! old baseline, after a fold the segments it merged. Readers of the listing before may
//! still want them: an edge that is hydrating, a run that started from it. And a run that
//! dies leaves uploads that no listing ever had. Both are deleted, a grace period
//! (`--gc-grace-secs`) after a pass first saw them unlisted:
//!
//! * A pass lists the store. An object that no listing references and `gc.bin` does not
//!   have is noted there, with its version in the listing
//!   ([`crate::store::ObjectInfo::version`]) and the pass's clock. It stays.
//! * An object noted at least the grace period ago is deleted, if it still is the object
//!   that was noted: the listing gives it the version noted, and the delete goes through
//!   only for that version ([`Store::delete_if`]).
//! * An object that is listed again, or gone, loses its note. One written again since it
//!   was noted (ids are content-addressed: a run that makes the rollup an earlier run
//!   uploaded before it died uploads the same id) is noted anew, and its grace period
//!   starts over: by its end the run that uploaded it has published.
//!
//! So nothing is deleted sooner than a grace period after a pass saw it unlisted. That
//! is how long a reader has from a listing to the objects it names, and a run from an
//! upload to the listing that names it. (The grace period was the object's age. What a
//! run dropped was older than that and went seconds after the manifest that listed it:
//! an edge still hydrating did not start, and a run working from that manifest failed.)
//!
//! [`Pass::survey`] reads and lists; [`Pass::sweep`] deletes and writes `gc.bin`. Another
//! run may work between the two, and upload again and publish a slab that the survey
//! found due: the deletes being conditional, and every upload another object
//! ([`Store::upload_file`]), it stays. (A pass deleted whatever had the key by then, and
//! an upload to a directory left a file of that name as it was: the manifest then listed
//! a slab that was gone, both runs having exited 0.)
//!
//! `gc.bin` is replaced by compare-and-swap, once per pass that changed it. A pass that
//! loses the swap, or cannot write, has deleted only what was due, and what it noted is
//! noted by the next pass: later, never sooner. A pass that cannot read `gc.bin` ends
//! there. A `gc.bin` that does not decode counts as absent: everything unlisted is noted
//! anew, and that pass deletes nothing. Edges never read it.
//!
//! ```text
//! "GVDGCOL1" | version u32 | count u32
//! entry*: since u64 | key_len u16 | key | version_len u16 | version
//! SHA-256 of everything before it [32]
//! ```

use std::collections::{HashMap, HashSet};

use crate::manifest::Manifest;
use crate::slab::SlabMeta;
use crate::store::{Cond, Store};
use crate::util::{Context, Error, Result, le_u16, le_u32, le_u64, sub};

pub const GC_KEY: &str = "gc.bin";
const MAGIC: &[u8; 8] = b"GVDGCOL1";
const VERSION: u32 = 1;

/// An entry of `gc.bin`: an object a pass saw in the store and in no listing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unlisted {
    pub key: String,
    /// The object's version in that pass's listing.
    pub version: String,
    /// The clock (Unix seconds) of the first pass that saw this object unlisted.
    pub since: u64,
}

/// `gc.bin` of `entries`.
pub fn encode(entries: &[Unlisted]) -> Vec<u8> {
    let mut body = Vec::new();
    let mut count = 0u32;
    for e in entries {
        // No key or version is this long. One that were is left out: never due.
        let (Ok(key_len), Ok(version_len)) =
            (u16::try_from(e.key.len()), u16::try_from(e.version.len()))
        else {
            continue;
        };
        body.extend_from_slice(&e.since.to_le_bytes());
        body.extend_from_slice(&key_len.to_le_bytes());
        body.extend_from_slice(e.key.as_bytes());
        body.extend_from_slice(&version_len.to_le_bytes());
        body.extend_from_slice(e.version.as_bytes());
        count = count.saturating_add(1);
    }
    let mut out = Vec::with_capacity(body.len().saturating_add(48));
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&body);
    let digest = ring::digest::digest(&ring::digest::SHA256, &out);
    out.extend_from_slice(digest.as_ref());
    out
}

/// The entries of a `gc.bin`.
pub fn decode(b: &[u8]) -> Result<Vec<Unlisted>> {
    let body_len = b.len().checked_sub(32).ctx("gc.bin too short")?;
    let (body, digest) = b.split_at_checked(body_len).ctx("gc.bin too short")?;
    if ring::digest::digest(&ring::digest::SHA256, body).as_ref() != digest {
        crate::bail!("gc.bin checksum mismatch");
    }
    if body.get(..8) != Some(&MAGIC[..]) || le_u32(body, 8) != Some(VERSION) {
        crate::bail!("not a gc.bin (or an unsupported version)");
    }
    let count = le_u32(body, 12).ctx("gc.bin header")?;
    let mut at = 16usize;
    let mut entries = Vec::new();
    for _ in 0..count {
        let since = le_u64(body, at).ctx("truncated gc.bin")?;
        at = at.saturating_add(8);
        let mut text = || -> Result<String> {
            let len = usize::from(le_u16(body, at).ctx("truncated gc.bin")?);
            let bytes = sub(body, at.saturating_add(2), len).ctx("truncated gc.bin")?;
            at = at.saturating_add(2).saturating_add(len);
            String::from_utf8(bytes.to_vec()).map_err(|_| crate::err!("bad text in gc.bin"))
        };
        let (key, version) = (text()?, text()?);
        entries.push(Unlisted {
            key,
            version,
            since,
        });
    }
    if at != body.len() {
        crate::bail!("trailing bytes in gc.bin");
    }
    Ok(entries)
}

/// The entries of the store's `gc.bin`: none if it has none.
pub fn load(store: &Store) -> Result<Vec<Unlisted>> {
    match store.get(GC_KEY)? {
        Some((bytes, _)) => decode(&bytes).ctx("reading gc.bin"),
        None => Ok(Vec::new()),
    }
}

/// A pass after its reads: what it found due, and what stays noted.
pub struct Pass {
    /// `gc.bin` as it was read: its etag and its entries (`None`: there was none).
    was: Option<(String, Vec<Unlisted>)>,
    /// It did not decode: it is written again, whatever the pass finds.
    undecoded: bool,
    /// Unlisted for the grace period: to delete.
    due: Vec<Unlisted>,
    /// Unlisted for less: these stay noted.
    kept: Vec<Unlisted>,
    /// How many of `kept` this pass was the first to see unlisted.
    noted: usize,
    grace: u64,
}

/// What a pass did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Swept {
    /// Objects this pass was the first to see unlisted: noted, to go a grace period on.
    pub noted: usize,
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
        // A copy that has not published its packs yet has the grace period to do so,
        // from the first pass that sees them.
        let (bm, _) = crate::blobcopy::load(store)?;
        keys.extend(bm.packs.iter().map(|p| crate::blobs::pack_key(&p.id)));
        keys.extend(bm.segments.iter().map(|s| crate::blobs::segment_key(&s.id)));
    }
    Ok(keys)
}

impl Pass {
    /// Reads `gc.bin` and lists the store, whose manifest is `m`, at `now` (Unix
    /// seconds, the clock the notes are in). Of the objects that no listing references,
    /// those noted at least `grace` seconds ago are due; the others are noted, or stay
    /// so. A note from a clock ahead of this one counts from now. A `gc.bin` or a prefix
    /// that cannot be read ends the pass.
    pub fn survey(store: &Store, m: &Manifest, now: u64, grace: u64) -> Result<Pass> {
        let referenced = referenced(store, m)?;
        let (was, undecoded) = match store.get(GC_KEY)? {
            None => (None, false),
            Some((bytes, etag)) => match decode(&bytes) {
                Ok(entries) => (Some((etag, entries)), false),
                Err(e) => {
                    crate::log!(
                        "gc: gc.bin does not decode ({e}): taken as absent, so this run \
                         deletes nothing and notes anew what is unlisted"
                    );
                    (Some((etag, Vec::new())), true)
                }
            },
        };
        let (mut due, mut kept, mut noted) = (Vec::new(), Vec::new(), 0usize);
        {
            let notes: HashMap<&str, &Unlisted> = was
                .iter()
                .flat_map(|(_, entries)| entries)
                .map(|e| (e.key.as_str(), e))
                .collect();
            let prefixes: &[&str] = if m.separate_blobs() {
                &["slabs/", "blobs/"]
            } else {
                &["slabs/"]
            };
            for prefix in prefixes {
                for obj in store.list(prefix).ctx(&format!("listing {prefix}"))? {
                    if referenced.contains(&obj.key) {
                        continue;
                    }
                    match notes.get(obj.key.as_str()) {
                        Some(note) if note.version == obj.version => {
                            let since = note.since.min(now);
                            let entry = Unlisted {
                                key: obj.key,
                                version: obj.version,
                                since,
                            };
                            if now.saturating_sub(since) >= grace {
                                due.push(entry);
                            } else {
                                kept.push(entry);
                            }
                        }
                        // Seen unlisted for the first time, or another object than the
                        // one noted: written again since, by a run that may be about to
                        // list it.
                        _ => {
                            noted = noted.saturating_add(1);
                            kept.push(Unlisted {
                                key: obj.key,
                                version: obj.version,
                                since: now,
                            });
                        }
                    }
                }
            }
        }
        Ok(Pass {
            was,
            undecoded,
            due,
            kept,
            noted,
            grace,
        })
    }

    /// The keys of what is due.
    pub fn due(&self) -> impl Iterator<Item = &str> {
        self.due.iter().map(|e| e.key.as_str())
    }

    /// Deletes what is due and still is the object the survey listed, then writes
    /// `gc.bin` if it is to hold something else than it did. A file that cannot be
    /// deleted is passed over, stays noted, and the rest is collected; the error then
    /// says how many there were and names the first.
    pub fn sweep(self, store: &Store) -> Result<Swept> {
        let Pass {
            was,
            undecoded,
            due,
            mut kept,
            noted,
            grace,
        } = self;
        let mut swept = Swept {
            noted,
            ..Swept::default()
        };
        // The files that could not be deleted: how many, and the first one's error.
        let mut failed: Option<(usize, Error)> = None;
        for entry in due {
            // A file that cannot be deleted does not keep the ones listed after it: it
            // did, at every run.
            match store.delete_if(&entry.key, &entry.version) {
                Ok(true) => swept.deleted = swept.deleted.saturating_add(1),
                // Its note goes too: the next pass notes the object that is there now,
                // if no listing has it by then.
                Ok(false) => swept.replaced = swept.replaced.saturating_add(1),
                Err(e) => {
                    failed = Some(match failed {
                        Some((n, first)) => (n.saturating_add(1), first),
                        None => (1, e),
                    });
                    kept.push(entry);
                }
            }
        }
        kept.sort_by(|a, b| a.key.cmp(&b.key));
        let (etag, mut before) = match was {
            Some((etag, entries)) => (Some(etag), entries),
            None => (None, Vec::new()),
        };
        before.sort_by(|a, b| a.key.cmp(&b.key));
        let written = if undecoded || kept != before {
            write(store, &kept, etag)
        } else {
            Ok(())
        };
        if swept.noted > 0 {
            crate::log!(
                "gc: {} unlisted files noted in gc.bin: deleted once they have been \
                 unlisted for {grace}s",
                swept.noted
            );
        }
        if swept.deleted > 0 {
            crate::log!("gc: deleted {} files unlisted for {grace}s", swept.deleted);
        }
        if swept.replaced > 0 {
            crate::log!(
                "gc: {} files that were due are not the ones listed any more (uploaded \
                 again since): left",
                swept.replaced
            );
        }
        match (failed, written) {
            (None, Ok(())) => Ok(swept),
            (None, Err(e)) => Err(e),
            (Some((n, first)), written) => Err(crate::err!(
                "{n} unreferenced files were not deleted, the first: {first}{}",
                written.err().map_or_else(String::new, |e| format!("; {e}"))
            )),
        }
    }
}

/// Replaces `gc.bin`, which had `etag` (`None`: there was none), by `entries`. A lost
/// swap is not an error: another pass wrote it meanwhile, from a listing as new as this
/// one's, and what only this pass noted is noted by the next.
fn write(store: &Store, entries: &[Unlisted], etag: Option<String>) -> Result<()> {
    let cond = match etag {
        Some(etag) => Cond::IfMatch(etag),
        None => Cond::IfNoneMatch,
    };
    match store.put(GC_KEY, &encode(entries), cond) {
        Ok(_) => Ok(()),
        Err(e) if e.is_conflict() => {
            crate::log!(
                "gc: another run wrote gc.bin meanwhile; what this one noted is noted by \
                 the next run"
            );
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// One pass over `store`, whose manifest is `m`: [`Pass::survey`], then [`Pass::sweep`].
pub fn collect(store: &Store, m: &Manifest, now: u64, grace: u64) -> Result<Swept> {
    Pass::survey(store, m, now, grace)?.sweep(store)
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use super::*;
    use crate::slab::Tier;
    use crate::util::Oid;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    const GRACE: u64 = 7200;
    const T: u64 = 1_800_000_000;

    fn meta(tier: Tier, end_ts: u64) -> SlabMeta {
        SlabMeta {
            id: SlabMeta::make_id(tier, end_ts, &[end_ts as u8; 20]),
            tier,
            start_ts: end_ts.saturating_sub(1),
            end_ts,
            have: Oid::ZERO,
            want: Oid::ZERO,
            objects: 1,
            pack_len: 4,
            index_len: 4,
            pack_sha: [end_ts as u8; 20],
        }
    }

    /// A manifest of a baseline and retained slabs.
    fn manifest(baseline: &SlabMeta, retained: &[&SlabMeta]) -> Manifest {
        Manifest {
            generation: 1,
            tip: Oid::ZERO,
            max_ts: baseline.end_ts,
            baseline: baseline.clone(),
            weekly: Vec::new(),
            pyramid: Vec::new(),
            retained: retained.iter().map(|s| (*s).clone()).collect(),
            flags: 0,
            blobs_generation: 0,
        }
    }

    fn files(slab: &SlabMeta) -> [String; 2] {
        [slab.pack_key(), slab.index_key()]
    }

    /// An empty directory store.
    fn store(name: &str) -> (Store, PathBuf) {
        let dir = std::env::temp_dir().join(format!("gvd-gc-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let store = Store::open(&format!("fs:{}", dir.display()), "UNUSED").unwrap();
        (store, dir)
    }

    /// Uploads `key`, as if a month ago: an object's age decides nothing.
    fn upload(store: &Store, dir: &Path, key: &str) {
        let mut w = store.writer(key).unwrap();
        std::io::Write::write_all(&mut w, b"slab").unwrap();
        w.finish().unwrap();
        let long_ago = std::time::SystemTime::now() - std::time::Duration::from_secs(30 * 86_400);
        let file = std::fs::File::options()
            .write(true)
            .open(dir.join(key))
            .unwrap();
        file.set_modified(long_ago).unwrap();
    }

    fn noted(store: &Store) -> Vec<(String, u64)> {
        let entries = load(store).unwrap();
        entries.into_iter().map(|e| (e.key, e.since)).collect()
    }

    fn swept(noted: usize, deleted: usize, replaced: usize) -> Swept {
        Swept {
            noted,
            deleted,
            replaced,
        }
    }

    #[test]
    fn gc_bin_roundtrip_and_integrity() {
        let entries = vec![
            Unlisted {
                key: "blobs/0123.pack".to_string(),
                version: "\"0x8DE1F\"".to_string(),
                since: T,
            },
            Unlisted {
                key: "slabs/stray~".to_string(),
                version: "1a-4-6af337fd.439589731".to_string(),
                since: u64::MAX,
            },
        ];
        let b = encode(&entries);
        assert_eq!(decode(&b).unwrap(), entries);
        assert_eq!(b.len(), 16 + (12 + 15 + 9) + (12 + 12 + 23) + 32);
        assert_eq!(decode(&encode(&[])).unwrap(), []);
        let mut bad = b.clone();
        bad[20] ^= 1;
        assert!(decode(&bad).is_err());
        assert!(decode(&b[..b.len() - 1]).is_err());
        assert!(decode(b"").is_err());
        // Another version, checksum and all, is not read as this one.
        let mut other = b[..b.len() - 32].to_vec();
        other[8] = 2;
        let digest = ring::digest::digest(&ring::digest::SHA256, &other);
        other.extend_from_slice(digest.as_ref());
        assert!(decode(&other).is_err());
    }

    /// What a run drops, and what no manifest ever listed, stay in the store until a pass
    /// at least a grace period after the pass that first saw them unlisted, whenever they
    /// were written. (Written long before, they went at the first pass that did not find
    /// them listed: the pass of the run that dropped them.) `gc.bin` holds them
    /// meanwhile, and nothing once they are gone.
    #[test]
    fn an_unlisted_file_goes_a_grace_period_after_a_pass_first_saw_it() {
        let (store, dir) = store("grace");
        let (base, dropped) = (meta(Tier::Base, 100), meta(Tier::M15, 90));
        let orphan = "slabs/h1-95-0123456789abcdef.pack".to_string();
        for key in files(&base).iter().chain(&files(&dropped)).chain([&orphan]) {
            upload(&store, &dir, key);
        }
        let (before, after) = (manifest(&base, &[&dropped]), manifest(&base, &[]));
        let exists = |key: &String| dir.join(key).exists();
        let pass = |m: &Manifest, now: u64| collect(&store, m, now, GRACE).unwrap();

        // A pass that finds an orphan, then the pass of the run that drops a slab.
        assert_eq!(pass(&before, T - 1000), swept(1, 0, 0));
        assert_eq!(noted(&store), [(orphan.clone(), T - 1000)]);
        assert_eq!(pass(&after, T), swept(2, 0, 0));
        assert!(files(&dropped).iter().all(exists) && exists(&orphan));
        let mut all = vec![(orphan.clone(), T - 1000)];
        all.extend(files(&dropped).into_iter().map(|key| (key, T)));
        all.sort();
        assert_eq!(noted(&store), all);

        // A second short of the orphan's grace period: nothing goes, and `gc.bin` stays
        // the file it is. Then the orphan goes; the dropped slab a second short of its
        // own does not, and then does.
        let gc_bin = std::fs::read(dir.join(GC_KEY)).unwrap();
        assert_eq!(pass(&after, T - 1000 + GRACE - 1), swept(0, 0, 0));
        assert!(std::fs::read(dir.join(GC_KEY)).unwrap() == gc_bin);
        assert_eq!(pass(&after, T - 1000 + GRACE), swept(0, 1, 0));
        assert!(files(&dropped).iter().all(exists) && !exists(&orphan));
        assert_eq!(noted(&store).len(), 2);
        assert_eq!(pass(&after, T + GRACE - 1), swept(0, 0, 0));
        assert!(files(&dropped).iter().all(exists));
        assert_eq!(pass(&after, T + GRACE), swept(0, 2, 0));
        assert!(!files(&dropped).iter().any(exists));
        assert_eq!(noted(&store), []);
        assert!(files(&base).iter().all(exists));
        // A grace period of none still takes two passes.
        upload(&store, &dir, &orphan);
        assert_eq!(collect(&store, &after, T, 0).unwrap(), swept(1, 0, 0));
        assert_eq!(collect(&store, &after, T, 0).unwrap(), swept(0, 1, 0));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A file that a manifest lists again loses its note: unlisted once more, however
    /// much later, it has a grace period from then.
    #[test]
    fn a_file_listed_again_loses_its_note() {
        let (store, dir) = store("relisted");
        let (base, slab) = (meta(Tier::Base, 100), meta(Tier::H1, 90));
        for key in files(&base).iter().chain(&files(&slab)) {
            upload(&store, &dir, key);
        }
        let (without, with) = (manifest(&base, &[]), manifest(&base, &[&slab]));
        let pass = |m: &Manifest, now: u64| collect(&store, m, now, GRACE).unwrap();
        assert_eq!(pass(&without, T), swept(2, 0, 0));
        assert_eq!(pass(&with, T + 100), swept(0, 0, 0));
        assert_eq!(noted(&store), []);
        assert_eq!(pass(&with, T + 10 * GRACE), swept(0, 0, 0));
        assert_eq!(pass(&without, T + 20 * GRACE), swept(2, 0, 0));
        assert!(files(&slab).iter().all(|key| dir.join(key).exists()));
        assert_eq!(pass(&without, T + 21 * GRACE), swept(0, 2, 0));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A noted file that is uploaded again is another object: its grace period starts
    /// over, whether a pass comes after the upload (the listing gives another version
    /// than the note) or listed before it (the delete is of the version listed). The run
    /// that uploaded it has that long to list it.
    #[test]
    fn a_file_uploaded_again_has_its_grace_period_again() {
        let (store, dir) = store("again");
        let base = meta(Tier::Base, 100);
        let key = "slabs/h1-95-0123456789abcdef.pack";
        for file in files(&base).iter().map(String::as_str).chain([key]) {
            upload(&store, &dir, file);
        }
        let m = manifest(&base, &[]);
        let pass = |now: u64| collect(&store, &m, now, GRACE).unwrap();
        assert_eq!(pass(T), swept(1, 0, 0));

        upload(&store, &dir, key);
        assert_eq!(pass(T + GRACE), swept(1, 0, 0));
        assert_eq!(noted(&store), [(key.to_string(), T + GRACE)]);

        let listed = Pass::survey(&store, &m, T + 2 * GRACE, GRACE).unwrap();
        assert_eq!(listed.due().collect::<Vec<_>>(), [key]);
        upload(&store, &dir, key);
        assert_eq!(listed.sweep(&store).unwrap(), swept(0, 0, 1));
        assert!(dir.join(key).exists());
        assert_eq!(noted(&store), []);

        assert_eq!(pass(T + 2 * GRACE + 1), swept(1, 0, 0));
        assert_eq!(pass(T + 3 * GRACE), swept(0, 0, 0));
        assert_eq!(pass(T + 3 * GRACE + 1), swept(0, 1, 0));
        assert!(!dir.join(key).exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// No `gc.bin` (every store until its first pass), one that does not decode, one
    /// that another pass wrote meanwhile: no pass fails, none deletes a file before its
    /// grace period, and a note that is lost is made again, later.
    #[test]
    fn gc_bin_absent_undecodable_or_written_meanwhile() {
        let (store, dir) = store("gc-bin");
        let base = meta(Tier::Base, 100);
        let (old, late) = (
            "slabs/m15-90-0123456789abcdef.pack",
            "slabs/m15-91-0123456789abcdef.pack",
        );
        for file in files(&base).iter().map(String::as_str).chain([old]) {
            upload(&store, &dir, file);
        }
        let m = manifest(&base, &[]);
        let pass = |now: u64| collect(&store, &m, now, GRACE).unwrap();

        // The first pass a store ever has: the file is a month old, and stays.
        assert!(!dir.join(GC_KEY).exists());
        assert_eq!(pass(T), swept(1, 0, 0));
        assert!(dir.join(old).exists());

        // Its note is due, and `gc.bin` does not decode: noted anew.
        std::fs::write(dir.join(GC_KEY), b"not a gc.bin").unwrap();
        assert_eq!(pass(T + GRACE), swept(1, 0, 0));
        assert!(dir.join(old).exists());
        assert_eq!(noted(&store), [(old.to_string(), T + GRACE)]);
        let mut bytes = std::fs::read(dir.join(GC_KEY)).unwrap();
        bytes[17] ^= 1;
        std::fs::write(dir.join(GC_KEY), &bytes).unwrap();
        assert_eq!(pass(T + 2 * GRACE), swept(1, 0, 0));
        assert_eq!(noted(&store), [(old.to_string(), T + 2 * GRACE)]);

        // A pass finds one file due and notes another; `gc.bin` is written by another
        // pass before it writes. It deletes what was due and does not fail; the other
        // pass's `gc.bin` stands.
        upload(&store, &dir, late);
        let listed = Pass::survey(&store, &m, T + 3 * GRACE, GRACE).unwrap();
        let others = [Unlisted {
            key: old.to_string(),
            version: "another pass's".to_string(),
            since: T + 3 * GRACE,
        }];
        store.put(GC_KEY, &encode(&others), Cond::None).unwrap();
        assert_eq!(listed.sweep(&store).unwrap(), swept(1, 1, 0));
        assert!(!dir.join(old).exists() && dir.join(late).exists());
        assert_eq!(load(&store).unwrap(), others);
        // The note it lost is made by the next pass, and counts from that one. The note
        // of the file that is gone goes.
        assert_eq!(pass(T + 3 * GRACE + 10), swept(1, 0, 0));
        assert_eq!(noted(&store), [(late.to_string(), T + 3 * GRACE + 10)]);
        assert_eq!(pass(T + 4 * GRACE), swept(0, 0, 0));
        assert_eq!(pass(T + 4 * GRACE + 10), swept(0, 1, 0));
        assert_eq!(noted(&store), []);

        // A note from a clock ahead of the pass's counts from the pass.
        upload(&store, &dir, late);
        assert_eq!(pass(T + 400 * GRACE), swept(1, 0, 0));
        assert_eq!(pass(T + 5 * GRACE), swept(0, 0, 0));
        assert_eq!(noted(&store), [(late.to_string(), T + 5 * GRACE)]);
        assert_eq!(pass(T + 6 * GRACE), swept(0, 1, 0));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A store with separate blobs: the sizes tables of listed slabs and what `blobs.bin`
    /// lists stay; the segments a fold merged, which `blobs.bin` has dropped, and a pack
    /// it never listed go a grace period after the pass that saw them so.
    #[test]
    fn what_blobs_bin_drops_has_the_grace_period_too() {
        use crate::blobs::{BLOBS_KEY, BlobManifest, PackMeta, SegmentMeta, pack_key, segment_key};
        let (store, dir) = store("separate");
        let base = meta(Tier::Base, 100);
        let m = Manifest {
            flags: crate::manifest::FLAG_SEPARATE_BLOBS,
            ..manifest(&base, &[])
        };
        let segment = |id: &str| SegmentMeta {
            id: id.to_string(),
            blobs: 1,
            index_len: 4,
            packs: vec!["p1".to_string()],
        };
        let blobs_bin = |segments: &[&str]| {
            let bm = BlobManifest {
                generation: 1,
                segments: segments.iter().map(|id| segment(id)).collect(),
                packs: vec![PackMeta {
                    id: "p1".to_string(),
                    len: 4,
                    sha: [0; 20],
                    blobs: 2,
                }],
                ..BlobManifest::default()
            };
            store.put(BLOBS_KEY, &bm.encode(), Cond::None).unwrap();
        };
        let listed = [base.sizes_key(), pack_key("p1"), segment_key("s12")];
        let dropped = [segment_key("s1"), segment_key("s2"), pack_key("p0")];
        for key in files(&base).iter().chain(&listed).chain(&dropped) {
            upload(&store, &dir, key);
        }
        let exist = |keys: &[String]| keys.iter().filter(|key| dir.join(key).exists()).count();
        let pass = |now: u64| collect(&store, &m, now, GRACE).unwrap();

        blobs_bin(&["s1", "s2"]);
        assert_eq!(pass(T), swept(2, 0, 0));
        // The fold: `s12` for `s1` and `s2`.
        blobs_bin(&["s12"]);
        assert_eq!(pass(T + 100), swept(2, 0, 0));
        assert_eq!((exist(&listed), exist(&dropped)), (3, 3));
        assert_eq!(pass(T + GRACE), swept(0, 1, 0));
        assert!(!dir.join(pack_key("p0")).exists());
        assert_eq!(pass(T + GRACE + 99), swept(0, 0, 0));
        assert_eq!(pass(T + GRACE + 100), swept(0, 2, 0));
        assert_eq!((exist(&listed), exist(&dropped)), (3, 0));
        assert_eq!(noted(&store), []);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A Blob service on this machine, as much of it as a pass uses: blobs with ETags,
    /// listings by prefix, conditional PUTs and DELETEs. Returns a store on it, what it
    /// was asked, and the request it is to refuse (`"GET gc.bin"`; none at first).
    #[allow(clippy::type_complexity)]
    fn blob_service() -> (Store, Arc<Mutex<Vec<String>>>, Arc<Mutex<&'static str>>) {
        use crate::testhttp::{Reply, Request};
        let asked = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&asked);
        let denied = Arc::new(Mutex::new(""));
        let deny = Arc::clone(&denied);
        let state = Mutex::new((
            1u32,
            std::collections::BTreeMap::<String, (u32, Vec<u8>)>::new(),
        ));
        let port = crate::testhttp::serve(move |r: &Request| {
            let mut words = r.line.split(' ');
            let (method, target) = (words.next().unwrap(), words.next().unwrap());
            let mut state = state.lock().unwrap();
            let (serial, blobs) = &mut *state;
            if let Some((_, query)) = target.split_once('?') {
                let prefix = query.split('&').find_map(|p| p.strip_prefix("prefix="));
                let prefix = prefix.unwrap().replace("%2F", "/");
                log.lock().unwrap().push(format!("LIST {prefix}"));
                let mut xml = String::from("<EnumerationResults><Blobs>");
                for (key, (etag, body)) in blobs.iter().filter(|(key, _)| key.starts_with(&prefix))
                {
                    xml.push_str(&format!(
                        "<Blob><Name>{key}</Name><Properties><Last-Modified>Sat, 24 Oct 2026 \
                         17:15:10 GMT</Last-Modified><Etag>0x{etag:X}</Etag><Content-Length>{}\
                         </Content-Length></Properties></Blob>",
                        body.len()
                    ));
                }
                xml.push_str("</Blobs><NextMarker /></EnumerationResults>");
                return Reply::ok(xml.as_bytes());
            }
            let key = target.strip_prefix("/acct/packs/").unwrap().to_string();
            let request = format!("{method} {key}");
            let forbidden = *deny.lock().unwrap() == request;
            log.lock().unwrap().push(request);
            if forbidden {
                return Reply::status("403 Forbidden");
            }
            let quoted = |etag: u32| format!("\"0x{etag:X}\"");
            let current = blobs.get(&key).map(|(etag, _)| quoted(*etag));
            let refused = match r.header("if-match") {
                Some(etag) => current.as_deref() != Some(etag),
                None => r.header("if-none-match") == Some("*") && current.is_some(),
            };
            match method {
                "GET" => match blobs.get(&key) {
                    Some((etag, body)) => {
                        Reply::Whole(format!("200 OK\r\nETag: {}", quoted(*etag)), body.clone())
                    }
                    None => Reply::status("404 Not Found"),
                },
                "DELETE" if current.is_none() => Reply::status("404 Not Found"),
                _ if refused => Reply::status("412 Precondition Failed"),
                "PUT" => {
                    *serial += 1;
                    blobs.insert(key, (*serial, r.body.clone()));
                    Reply::status(&format!("201 Created\r\nETag: {}", quoted(*serial)))
                }
                _ => {
                    blobs.remove(&key);
                    Reply::status("202 Accepted")
                }
            }
        });
        let account =
            format!("BlobEndpoint=http://127.0.0.1:{port}/acct;AccountName=acct;AccountKey=a2V5");
        let store = crate::azure::AzureStore::new(&account, "packs").unwrap();
        (Store::Azure(store), asked, denied)
    }

    /// A pass that cannot read `gc.bin` deletes nothing. One that cannot write it has
    /// deleted what was due, and what it noted is noted by the next pass, from which its
    /// grace period then counts. Each is an error for the run to log.
    #[test]
    fn a_pass_that_cannot_read_or_write_gc_bin() {
        let (store, asked, denied) = blob_service();
        let base = meta(Tier::Base, 100);
        let m = manifest(&base, &[]);
        let (due, late) = (
            "slabs/m15-90-0123456789abcdef.pack",
            "slabs/m15-91-0123456789abcdef.pack",
        );
        let put = |key: &str| drop(store.put(key, b"slab", Cond::None).unwrap());
        files(&base).iter().for_each(|key| put(key));
        put(due);
        assert_eq!(collect(&store, &m, T, GRACE).unwrap(), swept(1, 0, 0));
        put(late);
        let there = |key: &str| store.list(key).unwrap().len();

        *denied.lock().unwrap() = "GET gc.bin";
        asked.lock().unwrap().clear();
        let failed = collect(&store, &m, T + GRACE, GRACE).unwrap_err();
        assert!(failed.to_string().contains("GET gc.bin"), "{failed}");
        assert_eq!(*asked.lock().unwrap(), ["GET gc.bin"]);
        assert_eq!((there(due), there(late)), (1, 1));

        *denied.lock().unwrap() = "PUT gc.bin";
        let failed = collect(&store, &m, T + GRACE, GRACE).unwrap_err();
        assert!(failed.to_string().contains("PUT gc.bin"), "{failed}");
        assert_eq!((there(due), there(late)), (0, 1));
        assert_eq!(noted(&store), [(due.to_string(), T)]);

        *denied.lock().unwrap() = "";
        assert_eq!(
            collect(&store, &m, T + GRACE + 5, GRACE).unwrap(),
            swept(1, 0, 0)
        );
        assert_eq!(noted(&store), [(late.to_string(), T + GRACE + 5)]);
        let pass = |now: u64| collect(&store, &m, now, GRACE).unwrap();
        assert_eq!(pass(T + 2 * GRACE + 4), swept(0, 0, 0));
        assert_eq!(pass(T + 2 * GRACE + 5), swept(0, 1, 0));
        assert_eq!(there(late), 0);
    }

    /// What a pass asks of an object store: `gc.bin`, one listing, a delete for each
    /// file that is due, and `gc.bin` again if it is to hold something else: 100 bytes
    /// for one file. A store with separate blobs: `blobs.bin` and a second listing too.
    /// The deletes and the write are conditional there as in a directory.
    #[test]
    fn a_pass_is_a_read_a_listing_and_a_write_when_something_changed() {
        let (store, asked, _) = blob_service();
        let base = meta(Tier::Base, 100);
        let m = manifest(&base, &[]);
        let orphan = "slabs/h1-95-0123456789abcdef.pack";
        let put = |key: &str| drop(store.put(key, b"slab", Cond::None).unwrap());
        files(&base).iter().for_each(|key| put(key));
        let requests = |m: &Manifest, now: u64, did: Swept| {
            asked.lock().unwrap().clear();
            assert_eq!(collect(&store, m, now, GRACE).unwrap(), did);
            std::mem::take(&mut *asked.lock().unwrap())
        };
        let idle = ["GET gc.bin", "LIST slabs/"];
        assert_eq!(requests(&m, T, swept(0, 0, 0)), idle);
        put(orphan);
        let noting = ["GET gc.bin", "LIST slabs/", "PUT gc.bin"];
        assert_eq!(requests(&m, T + 900, swept(1, 0, 0)), noting);
        assert!(store.get(GC_KEY).unwrap().unwrap().0.len() <= 100);
        assert_eq!(requests(&m, T + 1800, swept(0, 0, 0)), idle);
        let deleting = [
            "GET gc.bin",
            "LIST slabs/",
            "DELETE slabs/h1-95-0123456789abcdef.pack",
            "PUT gc.bin",
        ];
        assert_eq!(requests(&m, T + 900 + GRACE, swept(0, 1, 0)), deleting);
        assert_eq!(requests(&m, T + 1800 + GRACE, swept(0, 0, 0)), idle);
        assert_eq!(store.list(orphan).unwrap().len(), 0);
        assert_eq!(load(&store).unwrap(), []);

        // Uploaded again after the listing: the service refuses the delete.
        put(orphan);
        assert_eq!(requests(&m, T + 2 * GRACE, swept(1, 0, 0)), noting);
        let listed = Pass::survey(&store, &m, T + 3 * GRACE, GRACE).unwrap();
        put(orphan);
        assert_eq!(listed.sweep(&store).unwrap(), swept(0, 0, 1));
        assert_eq!(store.list(orphan).unwrap().len(), 1);
        // `gc.bin` written by another pass after the survey: the service refuses the
        // write, and the pass has not failed.
        let listed = Pass::survey(&store, &m, T + 4 * GRACE, GRACE).unwrap();
        store.put(GC_KEY, &encode(&[]), Cond::None).unwrap();
        assert_eq!(listed.sweep(&store).unwrap(), swept(1, 0, 0));
        assert_eq!(load(&store).unwrap(), []);

        let separate = Manifest {
            flags: crate::manifest::FLAG_SEPARATE_BLOBS,
            ..m.clone()
        };
        put(&base.sizes_key());
        assert_eq!(
            requests(&separate, T + 5 * GRACE, swept(1, 0, 0)),
            [
                "GET blobs.bin",
                "GET gc.bin",
                "LIST slabs/",
                "LIST blobs/",
                "PUT gc.bin"
            ]
        );
    }
}
