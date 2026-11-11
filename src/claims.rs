//! `claims.bin`: the blobs that runs are copying into the store now, so that runs that
//! overlap copy no blob twice, and still share the work.
//!
//! Two pipeline runs of one window (a schedule's next run, started while the last one
//! still copies), two mirrors, a mirror and a window: each plans the blobs the store
//! lacks, and each then copied them. (The runs took their items in orders of their own
//! and left out what another had published by the time they came to it: two runs of a
//! window of 32 blobs fetched 43 to 60 between them, and two mirrors every blob twice.)
//! Before a run asks the upstream for an item's blobs it claims them here
//! ([`Ledger::claim`], for [`crate::separate::copy_all`]), and it leaves to the other
//! runs the blobs their claims hold.
//!
//! It is one object, replaced by compare-and-swap, as `blobs.bin` is. Runs plan
//! different items (a batch of small blobs is cut by what the store lists when its run
//! plans, and a later run's window may be longer), so a claim is of blob ids, and is
//! compared with the others by them: an object per item, created only if absent, would
//! tell two runs apart only where they planned alike.
//!
//! A claim is a run's: the blobs of the items it is copying or about to, and when the
//! claim lapses (`until`: the run's clock plus `--claim-secs`). The run writes it again,
//! `until` moved on, with each claim it makes and each it gives up, and at least every
//! third of `--claim-secs`. It gives up a blob once `blobs.bin` lists it, and its whole
//! claim as it ends, having copied or failed. A run that dies stops writing: its claim
//! lapses, and the other runs take its blobs.
//!
//! **The two orders.** A run that is about to claim reads `claims.bin` first and
//! `blobs.bin` after it ([`Ledger::look`]). A run that has copied publishes `blobs.bin`
//! first and gives up its claim after. So a blob leaves a claim only once it is listed,
//! and a run that reads the claims before the listing finds a blob whose claim has been
//! given up listed. The claim is then written only if `claims.bin` is still the object
//! that was read (`If-Match`): a run that read it before another's write is refused, and
//! reads both again. Any other way round leaves a gap. Run A reads `blobs.bin`: blob X
//! is not listed. Run B claims X, copies it, publishes it and gives up its claim. A
//! reads `claims.bin`: no claim has X, and nothing has been written since. A claims X
//! and copies it a second time.
//!
//! A run that copies alone does not read before each claim. Its last write left
//! `claims.bin` as it has it; a write on that etag goes through only if no other run has
//! written since, and then none has claimed or given up anything since this run read
//! `blobs.bin`: what it has is still so. That is one request per claim while no other run
//! copies; a write that is refused is followed by both reads.
//!
//! Every write carries a number of its own, so that no two are the same bytes. A
//! directory store's etag is a hash of the bytes: a `claims.bin` that had come back to
//! what it was (a claim made and given up) would pass for one that nobody had written,
//! and the gap above would be open again.
//!
//! **Clocks.** `until` is by the clock of the run that holds the claim, and is compared
//! with the reader's. A claim has between two thirds of `--claim-secs` and all of it to
//! go, so a reader whose clock is ahead by more than two thirds may take a claim that
//! holds for one that has lapsed. A claim that would hold for more than twice the time
//! it was made for is from a clock that far ahead, and is not believed: believed, it
//! would hold every other run up for as long as its clock is ahead, were its run to die.
//! Either way the cost is a blob copied twice, which is what every overlap cost before,
//! never a store that is wrong: what `blobs.bin` lists is content verified by its id,
//! whoever copied it. The times are the system clock's, whatever `--now` says: other
//! runs compare them with theirs.
//!
//! Nothing else reads `claims.bin`: no edge, and not garbage collection, which lists
//! `slabs/` and `blobs/` only. One that does not decode counts as empty and is written
//! anew by the next claim.
//!
//! ```text
//! "GVDCLAM1" | version u32 | nonce u64 | count u32
//! claim*: run u64 | until u64 | secs u32 | blobs u32 | id[20]*
//! SHA-256 of everything before it [32]
//! ```

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::blobcopy::Item;
use crate::blobs::{BLOBS_KEY, BlobManifest};
use crate::separate::{BlobIndex, SizeOf, prune_segments};
use crate::store::{Cond, Fresh, Poll, Store};
use crate::util::{Context, Oid, Result, le_u32, le_u64, sub};

pub const CLAIMS_KEY: &str = "claims.bin";
/// How long a claim holds without being written again, unless `--claim-secs` says.
pub const CLAIM_SECS: u64 = 120;
const MAGIC: &[u8; 8] = b"GVDCLAM1";
const VERSION: u32 = 1;
/// A write that other runs' writes keep refusing is given up after this many.
const ATTEMPTS: u32 = 50;

/// A run's claim: the blobs it is copying, or is about to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claim {
    /// The run's own number.
    pub run: u64,
    /// When the claim lapses unless it is written again: Unix seconds, by the run's
    /// clock.
    pub until: u64,
    /// How long it was made for (`--claim-secs`).
    pub secs: u32,
    pub oids: Vec<Oid>,
}

impl Claim {
    /// Whether the claim holds at `now`, the reader's clock: it has not lapsed, and it
    /// is not from a clock more than its own length ahead (module docs).
    pub fn holds(&self, now: u64) -> bool {
        self.until >= now
            && self.until.saturating_sub(now) <= u64::from(self.secs).saturating_mul(2)
    }
}

/// `claims.bin` of `claims`, as the write numbered `nonce`.
pub fn encode<'c>(claims: impl IntoIterator<Item = &'c Claim>, nonce: u64) -> Vec<u8> {
    let mut body = Vec::new();
    let mut count = 0u32;
    for c in claims {
        // No run copies this many blobs at once. A claim that had them is left out.
        let Ok(blobs) = u32::try_from(c.oids.len()) else {
            continue;
        };
        body.extend_from_slice(&c.run.to_le_bytes());
        body.extend_from_slice(&c.until.to_le_bytes());
        body.extend_from_slice(&c.secs.to_le_bytes());
        body.extend_from_slice(&blobs.to_le_bytes());
        for oid in &c.oids {
            body.extend_from_slice(&oid.0);
        }
        count = count.saturating_add(1);
    }
    let mut out = Vec::with_capacity(body.len().saturating_add(56));
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&nonce.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&body);
    let digest = ring::digest::digest(&ring::digest::SHA256, &out);
    out.extend_from_slice(digest.as_ref());
    out
}

/// The claims of a `claims.bin`, lapsed or not.
#[allow(clippy::chunks_exact_to_as_chunks)]
pub fn decode(b: &[u8]) -> Result<Vec<Claim>> {
    let body_len = b.len().checked_sub(32).ctx("claims.bin too short")?;
    let (body, digest) = b.split_at_checked(body_len).ctx("claims.bin too short")?;
    if ring::digest::digest(&ring::digest::SHA256, body).as_ref() != digest {
        crate::bail!("claims.bin checksum mismatch");
    }
    if body.get(..8) != Some(&MAGIC[..]) || le_u32(body, 8) != Some(VERSION) {
        crate::bail!("not a claims.bin (or an unsupported version)");
    }
    let count = le_u32(body, 20).ctx("claims.bin header")?;
    let mut at = 24usize;
    let mut claims = Vec::new();
    for _ in 0..count {
        let truncated = "truncated claims.bin";
        let run = le_u64(body, at).ctx(truncated)?;
        let until = le_u64(body, at.saturating_add(8)).ctx(truncated)?;
        let secs = le_u32(body, at.saturating_add(16)).ctx(truncated)?;
        let blobs = crate::util::us(u64::from(
            le_u32(body, at.saturating_add(20)).ctx(truncated)?,
        ));
        at = at.saturating_add(24);
        let ids = sub(body, at, blobs.saturating_mul(20)).ctx(truncated)?;
        at = at.saturating_add(ids.len());
        claims.push(Claim {
            run,
            until,
            secs,
            oids: ids.chunks_exact(20).filter_map(Oid::from_slice).collect(),
        });
    }
    if at != body.len() {
        crate::bail!("trailing bytes in claims.bin");
    }
    Ok(claims)
}

/// The claims in the store's `claims.bin`: none if it has none.
pub fn load(store: &Store) -> Result<Vec<Claim>> {
    match store.get(CLAIMS_KEY)? {
        Some((bytes, _)) => decode(&bytes).ctx("reading claims.bin"),
        None => Ok(Vec::new()),
    }
}

/// A number that no other run and no other write has: the standard library's hasher
/// keys (the system's randomness), over this process, the clock and a count.
fn fresh() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(SEQ.fetch_add(1, Ordering::Relaxed));
    h.write_u32(std::process::id());
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    h.finish()
}

/// What a run may do with the items it came to ([`Ledger::claim`]).
#[derive(Debug, Default)]
pub struct Taken<'a> {
    /// The items it has claimed, to copy: all of an item, or what of it is neither
    /// listed nor another run's. Each with the number its claim is given up by
    /// ([`Ledger::release`]).
    pub ready: Vec<(u64, Cow<'a, Item>)>,
    /// What other runs' claims hold of the items: to come back to.
    pub later: Vec<Item>,
    /// How many of the items' blobs `blobs.bin` lists.
    pub listed: usize,
}

/// A part of an item.
enum Part {
    None,
    All,
    Some(Item),
}

/// A run's hand in `claims.bin`, and what it knows of the store: the other runs'
/// claims, and what `blobs.bin` lists. One thread of the run has it.
pub struct Ledger<'a> {
    store: &'a Store,
    /// Where the segment indexes it reads are kept ([`BlobIndex::load`]).
    cache: &'a Path,
    /// Whose it is: begins its log lines.
    who: &'a str,
    /// This run's number in `claims.bin`.
    run: u64,
    /// How long its claim holds without a write (`--claim-secs`).
    secs: u64,
    poll: Poll,
    /// `claims.bin` and `blobs.bin` have been read.
    looked: bool,
    /// `claims.bin` as it was last read or written here: its etag (`None`: there is
    /// none), and the other runs' claims in it that held.
    etag: Option<String>,
    others: Vec<Claim>,
    /// Until when each blob of `others` is claimed.
    held: HashMap<Oid, u64>,
    /// What this run has claimed and not given up: the items, by their numbers.
    mine: Vec<(u64, Cow<'a, Item>)>,
    /// The last number given.
    number: u64,
    /// `claims.bin` has blobs of this run that `mine` has no more.
    dirty: bool,
    /// When this run's claim was last written.
    written: Instant,
    /// `blobs.bin` as it was last read: its etag, and its index.
    listing: Option<String>,
    index: Arc<BlobIndex>,
}

impl<'a> Ledger<'a> {
    /// For a run that has read `blobs.bin` as `index` and is about to copy: it holds
    /// its claims for `secs` seconds at a time.
    pub fn new(
        store: &'a Store,
        cache: &'a Path,
        who: &'a str,
        secs: u64,
        index: Arc<BlobIndex>,
    ) -> Ledger<'a> {
        Ledger {
            store,
            cache,
            who,
            run: fresh(),
            secs: secs.max(1),
            poll: Poll::default(),
            looked: false,
            etag: None,
            others: Vec::new(),
            held: HashMap::new(),
            mine: Vec::new(),
            number: 0,
            dirty: false,
            written: Instant::now(),
            listing: None,
            index,
        }
    }

    /// What `blobs.bin` listed when it was last read.
    pub fn index(&self) -> &Arc<BlobIndex> {
        &self.index
    }

    /// Reads `claims.bin`, and then `blobs.bin`: in that order (module docs). Neither
    /// is fetched again if it is the object this run has.
    pub fn look(&mut self) -> Result<()> {
        self.look_at_claims()?;
        self.look_at_listing()?;
        self.looked = true;
        Ok(())
    }

    fn look_at_claims(&mut self) -> Result<()> {
        match self
            .store
            .get_unless(&mut self.poll, CLAIMS_KEY, self.etag.as_deref())?
        {
            Fresh::Same => {}
            Fresh::Gone => {
                self.etag = None;
                self.read(Vec::new());
            }
            Fresh::New(bytes, etag) => {
                let claims = decode(&bytes).unwrap_or_else(|e| {
                    crate::log!(
                        "{}: claims.bin does not decode ({e}): taken as empty, and \
                         written anew",
                        self.who
                    );
                    Vec::new()
                });
                self.etag = Some(etag);
                self.read(claims);
            }
        }
        Ok(())
    }

    fn look_at_listing(&mut self) -> Result<()> {
        match self
            .store
            .get_unless(&mut self.poll, BLOBS_KEY, self.listing.as_deref())?
        {
            Fresh::Same => {}
            Fresh::Gone => {
                self.listing = None;
                self.index = Arc::new(BlobIndex::load(
                    self.store,
                    self.cache,
                    &BlobManifest::default(),
                )?);
            }
            Fresh::New(bytes, etag) => {
                let bm = BlobManifest::decode(&bytes).ctx("reading blobs.bin")?;
                // An index that cannot be read now is read at the next look: until
                // then a blob listed since may be copied again, no worse.
                match BlobIndex::load(self.store, self.cache, &bm) {
                    Ok(index) => {
                        self.index = Arc::new(index);
                        self.listing = Some(etag);
                        prune_segments(self.cache, &bm);
                    }
                    Err(e) => crate::log!("{}: reloading the blob index failed ({e})", self.who),
                }
            }
        }
        Ok(())
    }

    /// Takes `claims`, a `claims.bin` just read, for the other runs': those that hold.
    /// This run's own claim in it is what `mine` says, whatever was written.
    fn read(&mut self, claims: Vec<Claim>) {
        let now = crate::util::now_secs();
        self.others = claims;
        self.others.retain(|c| c.run != self.run && c.holds(now));
        self.held.clear();
        for c in &self.others {
            for oid in &c.oids {
                let until = self.held.entry(*oid).or_insert(c.until);
                *until = (*until).max(c.until);
            }
        }
    }

    /// What of `item` this run may claim at `now` (no listing has it, no other run's
    /// claim holds it), what other runs' claims hold, and how many of its blobs are
    /// listed.
    fn split(&self, item: &Item, now: u64, sizes: &SizeOf) -> Result<(Part, Part, usize)> {
        let oids = item.oids();
        let listed = |o: &Oid| self.index.find(o).is_some();
        let held = |o: &Oid| self.held.get(o).is_some_and(|until| *until >= now);
        let (mut free, mut wait, mut done) = (0usize, 0usize, 0usize);
        for o in oids {
            if listed(o) {
                done = done.saturating_add(1);
            } else if held(o) {
                wait = wait.saturating_add(1);
            } else {
                free = free.saturating_add(1);
            }
        }
        let part = |n: usize, of: &dyn Fn(&Oid) -> bool| -> Result<Part> {
            if n == 0 {
                return Ok(Part::None);
            }
            if n == oids.len() {
                return Ok(Part::All);
            }
            let some: Vec<Oid> = oids.iter().filter(|o| of(o)).copied().collect();
            let mut bytes = 0u64;
            for o in &some {
                let Some(size) = (sizes.get)(o) else {
                    crate::bail!("no size for blob {o}");
                };
                bytes = bytes.saturating_add(size);
            }
            Ok(Part::Some(Item::Batch { oids: some, bytes }))
        };
        Ok((
            part(free, &|o| !listed(o) && !held(o))?,
            part(wait, &|o| !listed(o) && held(o))?,
            done,
        ))
    }

    /// Claims what of `items` is free: the blobs that `blobs.bin` does not list and no
    /// other run's claim holds. On what this run has of the two objects if it has
    /// written `claims.bin` before; if that write is refused (another run has written
    /// since), or at its first claim, on both read again ([`Ledger::look`]), until a
    /// write goes through. Nothing is written if nothing is free.
    pub fn claim(&mut self, items: &[Cow<'a, Item>], sizes: &SizeOf) -> Result<Taken<'a>> {
        if !self.looked {
            self.look()?;
        }
        let mut attempt = 0u32;
        loop {
            let now = crate::util::now_secs();
            let mut taken = Taken::default();
            for item in items {
                let (free, wait, listed) = self.split(item, now, sizes)?;
                taken.listed = taken.listed.saturating_add(listed);
                let free = match free {
                    Part::None => None,
                    Part::All => Some(item.clone()),
                    Part::Some(part) => Some(Cow::Owned(part)),
                };
                if let Some(item) = free {
                    self.number = self.number.wrapping_add(1);
                    taken.ready.push((self.number, item));
                }
                match wait {
                    Part::None => {}
                    Part::All => taken.later.push(item.clone().into_owned()),
                    Part::Some(part) => taken.later.push(part),
                }
            }
            if taken.ready.is_empty() {
                return Ok(taken);
            }
            let before = self.mine.len();
            self.mine.extend(taken.ready.iter().cloned());
            if self.write(now)? {
                return Ok(taken);
            }
            self.mine.truncate(before);
            attempt = attempt.saturating_add(1);
            self.lost(attempt)?;
        }
    }

    /// After a write that was refused, the `attempt`th in a row: another run wrote
    /// `claims.bin` first. Both objects are read again.
    fn lost(&mut self, attempt: u32) -> Result<()> {
        if attempt >= ATTEMPTS {
            crate::bail!("claims.bin: gave up after {ATTEMPTS} lost races");
        }
        std::thread::sleep(Duration::from_millis(u64::from(attempt).saturating_mul(5)));
        self.look()
    }

    /// Writes `claims.bin` if it is still the object this run has: the other runs'
    /// claims that hold at `now`, and this run's, for `secs` from `now`. `false`: it is
    /// another object by now.
    fn write(&mut self, now: u64) -> Result<bool> {
        self.others.retain(|c| c.holds(now));
        let mine = Claim {
            run: self.run,
            until: now.saturating_add(self.secs),
            secs: u32::try_from(self.secs).unwrap_or(u32::MAX),
            oids: self
                .mine
                .iter()
                .flat_map(|(_, item)| item.oids().iter().copied())
                .collect(),
        };
        let claims = self
            .others
            .iter()
            .chain(std::iter::once(&mine).filter(|c| !c.oids.is_empty()));
        let cond = match &self.etag {
            Some(etag) => Cond::IfMatch(etag.clone()),
            // Nothing to give up in an object that is not there.
            None if mine.oids.is_empty() => {
                self.dirty = false;
                return Ok(true);
            }
            None => Cond::IfNoneMatch,
        };
        match self.store.put(CLAIMS_KEY, &encode(claims, fresh()), cond) {
            Ok(etag) => {
                self.etag = Some(etag);
                self.dirty = false;
                self.written = Instant::now();
                Ok(true)
            }
            Err(e) if e.is_conflict() => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Gives up the claims numbered `numbers`: their blobs are listed, or will not be
    /// copied by this run. `claims.bin` has them until the next write
    /// ([`Ledger::due`]).
    pub fn release(&mut self, numbers: &[u64]) {
        let before = self.mine.len();
        self.mine.retain(|(number, _)| !numbers.contains(number));
        self.dirty |= self.mine.len() != before;
    }

    /// Gives up every claim of this run.
    pub fn release_all(&mut self) {
        self.dirty |= !self.mine.is_empty();
        self.mine.clear();
    }

    /// Whether `claims.bin` is to be written ([`Ledger::sync`]): it has claims this
    /// run has given up, or this run's claim has a third of its time behind it.
    pub fn due(&self) -> bool {
        let third = Duration::from_millis(self.secs.saturating_mul(1000) / 3);
        self.dirty || (!self.mine.is_empty() && self.written.elapsed() >= third)
    }

    /// Writes this run's claim as it is now, for `--claim-secs` from now, whatever
    /// other runs write meanwhile.
    pub fn sync(&mut self) -> Result<()> {
        let mut attempt = 0u32;
        while !self.write(crate::util::now_secs())? {
            attempt = attempt.saturating_add(1);
            self.lost(attempt)?;
        }
        Ok(())
    }

    /// How many blobs of `items` other runs' claims hold, and until when at the
    /// latest.
    pub fn waits(&self, items: &[Item]) -> (usize, u64) {
        let now = crate::util::now_secs();
        let (mut blobs, mut latest) = (0usize, 0u64);
        for until in items
            .iter()
            .flat_map(Item::oids)
            .filter_map(|oid| self.held.get(oid))
            .filter(|until| **until >= now)
        {
            blobs = blobs.saturating_add(1);
            latest = latest.max(*until);
        }
        (blobs, latest)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
mod tests {
    use super::*;
    use crate::blobcopy::{Copied, publish};
    use crate::blobs::{BlobRec, PackMeta};
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Mutex;

    fn oid(n: u8) -> Oid {
        Oid([n; 20])
    }

    fn oids(ids: &[u8]) -> Vec<Oid> {
        ids.iter().copied().map(oid).collect()
    }

    /// An item of small blobs, 1000 bytes each.
    fn batch(ids: &[u8]) -> Cow<'static, Item> {
        Cow::Owned(Item::Batch {
            oids: oids(ids),
            bytes: 1000 * ids.len() as u64,
        })
    }

    const SIZES: SizeOf<'static> = SizeOf {
        said: "the test says",
        get: &|_| Some(1000),
    };

    /// An empty directory store, in a directory that also has room for what its runs
    /// download.
    fn store(name: &str) -> (Store, PathBuf) {
        let dir = std::env::temp_dir().join(format!("gvd-claims-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let store = Store::open(&format!("fs:{}", dir.join("store").display()), "UNUSED").unwrap();
        (store, dir)
    }

    /// A run about to copy into `store`, which lists nothing it knows of.
    fn run<'a>(store: &'a Store, dir: &'a Path) -> Ledger<'a> {
        let index = BlobIndex::load(store, dir, &BlobManifest::default()).unwrap();
        Ledger::new(store, dir, "test", CLAIM_SECS, Arc::new(index))
    }

    /// Publishes the blobs `ids` in `blobs.bin`, as a run that has copied them into a
    /// pack does.
    fn list(store: &Store, dir: &Path, ids: &[u8]) {
        let recs = ids.iter().enumerate().map(|(i, n)| BlobRec {
            oid: oid(*n),
            pack: 0,
            offset: 12 + 100 * i as u64,
            entry_len: 100,
            size: 1000,
            hdr_len: 2,
            stored: false,
        });
        let pack = PackMeta {
            id: format!("{:040x}", ids[0]),
            len: 1000,
            sha: [ids[0]; 20],
            blobs: ids.len() as u32,
        };
        let copied = Copied {
            pack,
            recs: recs.collect(),
        };
        publish(store, dir, &[copied], 1, |_| Ok(())).unwrap();
    }

    /// The blobs each run has claimed in the store's `claims.bin`, sorted.
    fn claimed(store: &Store) -> Vec<Vec<Oid>> {
        let mut claims: Vec<Vec<Oid>> = load(store).unwrap().into_iter().map(|c| c.oids).collect();
        claims.iter_mut().for_each(|oids| oids.sort());
        claims.sort();
        claims
    }

    /// The blobs of the items a run got to copy, and of those it is to come back to.
    fn blobs(taken: &Taken) -> (Vec<Vec<Oid>>, Vec<Vec<Oid>>) {
        let ready = taken.ready.iter().map(|(_, item)| item.oids().to_vec());
        let later = taken.later.iter().map(|item| item.oids().to_vec());
        (ready.collect(), later.collect())
    }

    #[test]
    fn claims_bin_roundtrip_and_integrity() {
        let claims = vec![
            Claim {
                run: 7,
                until: 1_800_000_000,
                secs: 120,
                oids: oids(&[1, 2, 3]),
            },
            Claim {
                run: u64::MAX,
                until: 0,
                secs: 1,
                oids: Vec::new(),
            },
        ];
        let b = encode(&claims, 5);
        assert_eq!(decode(&b).unwrap(), claims);
        assert_eq!(b.len(), 24 + (24 + 60) + 24 + 32);
        assert_eq!(decode(&encode(&[], 0)).unwrap(), []);
        // The same claims written again are other bytes.
        assert_ne!(encode(&claims, 6), b);
        let mut bad = b.clone();
        bad[30] ^= 1;
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

    /// A claim holds until its time, by a clock near the one it was written by: not
    /// after, and not when it would hold for more than twice the time it was made for.
    #[test]
    fn a_claim_holds_until_its_time_by_a_clock_near_its_own() {
        let now = 1_800_000_000;
        let claim = |until: u64| Claim {
            run: 1,
            until,
            secs: 120,
            oids: Vec::new(),
        };
        assert!(claim(now + 120).holds(now));
        assert!(claim(now).holds(now));
        assert!(!claim(now - 1).holds(now));
        // The reader's clock is behind the writer's: by a claim's length, and by more.
        assert!(claim(now + 240).holds(now));
        assert!(!claim(now + 241).holds(now));
        assert!(!claim(u64::MAX).holds(now));
    }

    /// Two runs that planned the same blobs, in items cut elsewhere, each claim what
    /// the other has not: no blob is in both claims. What the other holds a run comes
    /// back to: listed by then, it is done; given up unlisted (that copy failed), it is
    /// this run's to copy.
    #[test]
    fn runs_claim_what_no_other_run_holds() {
        let (store, dir) = store("disjoint");
        let (mut a, mut b) = (run(&store, &dir), run(&store, &dir));
        // A: 1 2 3 | 4 5. B: 1 2 | 3 4 | 5 6.
        let first = a.claim(&[batch(&[1, 2, 3])], &SIZES).unwrap();
        assert_eq!(blobs(&first), (vec![oids(&[1, 2, 3])], vec![]));
        let taken = b.claim(&[batch(&[1, 2]), batch(&[3, 4])], &SIZES).unwrap();
        assert_eq!(
            blobs(&taken),
            (vec![oids(&[4])], vec![oids(&[1, 2]), oids(&[3])])
        );
        assert_eq!(taken.ready[0].1.bytes(), 1000);
        // A has not read since B wrote: its write is refused, and it reads again.
        let second = a.claim(&[batch(&[4, 5])], &SIZES).unwrap();
        assert_eq!(blobs(&second), (vec![oids(&[5])], vec![oids(&[4])]));
        let taken = b.claim(&[batch(&[5, 6])], &SIZES).unwrap();
        assert_eq!(blobs(&taken), (vec![oids(&[6])], vec![oids(&[5])]));
        assert_eq!(claimed(&store), [oids(&[1, 2, 3, 5]), oids(&[4, 6])]);

        // A copies its first item, lists it, and only then gives up its claim.
        list(&store, &dir, &[1, 2, 3]);
        a.release(&[first.ready[0].0]);
        assert!(a.due());
        a.sync().unwrap();
        assert!(!a.due());
        assert_eq!(claimed(&store), [oids(&[4, 6]), oids(&[5])]);
        // B comes back to what A held: three are listed, one A still holds. Nothing
        // is written for that.
        let before = store.get(CLAIMS_KEY).unwrap();
        b.look().unwrap();
        let back = [batch(&[1, 2]), batch(&[3]), batch(&[5])];
        let taken = b.claim(&back, &SIZES).unwrap();
        assert_eq!(blobs(&taken), (vec![], vec![oids(&[5])]));
        assert_eq!(taken.listed, 3);
        assert_eq!(b.waits(&taken.later).0, 1);
        assert_eq!(store.get(CLAIMS_KEY).unwrap(), before);
        // A's copy of 5 fails, and A gives it up: it is B's to copy.
        a.release(&[second.ready[0].0]);
        a.sync().unwrap();
        b.look().unwrap();
        let taken = b.claim(&[batch(&[5])], &SIZES).unwrap();
        assert_eq!(blobs(&taken), (vec![oids(&[5])], vec![]));
        assert_eq!(claimed(&store), [oids(&[4, 5, 6])]);
        // A run that ends gives up the rest: `claims.bin` stays, with no claim.
        b.release_all();
        b.sync().unwrap();
        assert_eq!(claimed(&store), Vec::<Vec<Oid>>::new());
        assert!(store.get(CLAIMS_KEY).unwrap().is_some());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A blob that another run claimed, copied, listed and gave up, all since this run
    /// last read, is not claimed again: `claims.bin` is by then what it was when this
    /// run read it, but for its number, and this run's write is refused all the same.
    /// (In a directory, where an object's etag is a hash of its bytes, the write went
    /// through, on what this run had read: the blob in no claim and not listed.)
    #[test]
    fn a_blob_copied_and_given_up_meanwhile_is_not_claimed_again() {
        let (store, dir) = store("aba");
        // A run that came and went: `claims.bin` is there, with no claim.
        let mut gone = run(&store, &dir);
        gone.claim(&[batch(&[9])], &SIZES).unwrap();
        gone.release_all();
        gone.sync().unwrap();
        let empty = claimed(&store);

        let (mut a, mut b) = (run(&store, &dir), run(&store, &dir));
        a.look().unwrap();
        let taken = b.claim(&[batch(&[1])], &SIZES).unwrap();
        list(&store, &dir, &[1]);
        b.release(&[taken.ready[0].0]);
        b.sync().unwrap();
        assert_eq!(claimed(&store), empty);

        let taken = a.claim(&[batch(&[1])], &SIZES).unwrap();
        assert_eq!(blobs(&taken), (vec![], vec![]), "claimed a second time");
        assert_eq!(taken.listed, 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// What an object store in memory holds and was asked, behind HTTP as Azure Blob
    /// Storage answers a ledger's requests.
    #[derive(Default)]
    struct Service {
        /// `<method> <key> <status>` of each request.
        asked: Vec<String>,
        serial: u32,
        objects: BTreeMap<String, (u32, Vec<u8>)>,
        /// Objects that are put before the GET after `skip` more is answered: another
        /// run's writes, between two reads of this one.
        puts: Vec<(String, Vec<u8>)>,
        skip: usize,
    }

    impl Service {
        fn put(&mut self, key: &str, body: Vec<u8>) -> u32 {
            self.serial += 1;
            self.objects.insert(key.to_string(), (self.serial, body));
            self.serial
        }
    }

    fn service() -> (Store, Arc<Mutex<Service>>) {
        use crate::testhttp::{Reply, Request};
        let service = Arc::new(Mutex::new(Service::default()));
        let state = Arc::clone(&service);
        let port = crate::testhttp::serve(move |r: &Request| {
            let mut words = r.line.split(' ');
            let (method, target) = (words.next().unwrap(), words.next().unwrap());
            let key = target.strip_prefix("/acct/packs/").unwrap();
            let mut s = state.lock().unwrap();
            if method == "GET" {
                match s.skip.checked_sub(1) {
                    Some(left) => s.skip = left,
                    None => {
                        for (key, body) in std::mem::take(&mut s.puts) {
                            s.put(&key, body);
                        }
                    }
                }
            }
            let quoted = |etag: u32| format!("\"0x{etag:X}\"");
            let had = s.objects.get(key).cloned();
            let current = had.as_ref().map(|(etag, _)| quoted(*etag));
            let (status, body) = match (method, had) {
                ("GET", None) => ("404 Not Found".to_string(), Vec::new()),
                ("GET", Some(_)) if r.header("if-none-match") == current.as_deref() => {
                    ("304 Not Modified".to_string(), Vec::new())
                }
                ("GET", Some((etag, body))) => (format!("200 OK\r\nETag: {}", quoted(etag)), body),
                ("PUT", _) => {
                    let refused = match r.header("if-match") {
                        Some(etag) => current.as_deref() != Some(etag),
                        None => r.header("if-none-match") == Some("*") && current.is_some(),
                    };
                    if refused {
                        ("412 Precondition Failed".to_string(), Vec::new())
                    } else {
                        let etag = s.put(key, r.body.clone());
                        (format!("201 Created\r\nETag: {}", quoted(etag)), Vec::new())
                    }
                }
                _ => ("400 Bad Request".to_string(), Vec::new()),
            };
            let code = status.split(' ').next().unwrap();
            s.asked.push(format!("{method} {key} {code}"));
            Reply::Whole(status, body)
        });
        let account =
            format!("BlobEndpoint=http://127.0.0.1:{port}/acct;AccountName=acct;AccountKey=a2V5");
        let store = crate::azure::AzureStore::new(&account, "packs").unwrap();
        (Store::Azure(store), service)
    }

    /// The requests the service has had since this was last asked.
    fn asked(service: &Mutex<Service>) -> Vec<String> {
        std::mem::take(&mut service.lock().unwrap().asked)
    }

    /// A run that is about to claim reads `claims.bin` before `blobs.bin`. Between its
    /// two reads here, another run lists the blob it copied and gives up its claim:
    /// this run finds the blob claimed, or listed, and leaves it. (Reading the listing
    /// first, it finds the blob not listed, then in no claim, in a `claims.bin` that
    /// nobody writes after that: it claims the blob and copies it a second time.)
    #[test]
    fn the_claims_are_read_before_the_listing() {
        // The other run's `blobs.bin` and segment, made in a directory.
        let (made, dir) = store("order");
        list(&made, &dir, &[1]);
        let listing = made.get(BLOBS_KEY).unwrap().unwrap().0;
        let segment = made.list("blobs/").unwrap().remove(0).key;
        let index = made.get(&segment).unwrap().unwrap().0;

        let (store, service) = service();
        let now = crate::util::now_secs();
        let theirs = Claim {
            run: 1,
            until: now + CLAIM_SECS,
            secs: CLAIM_SECS as u32,
            oids: oids(&[1]),
        };
        {
            let mut s = service.lock().unwrap();
            s.put(CLAIMS_KEY, encode(&[theirs], 1));
            s.put(&segment, index);
            let given_up = (CLAIMS_KEY.to_string(), encode(&[], 2));
            s.puts = vec![(BLOBS_KEY.to_string(), listing), given_up];
            s.skip = 1;
        }
        let cache = dir.join("cache");
        let mut a = run(&store, &cache);
        let taken = a.claim(&[batch(&[1])], &SIZES).unwrap();
        assert_eq!(blobs(&taken), (vec![], vec![]), "claimed a second time");
        assert_eq!(taken.listed, 1);
        assert_eq!(
            asked(&service),
            [
                "GET claims.bin 200",
                "GET blobs.bin 200",
                &format!("GET {segment} 200")
            ]
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// What claiming asks of the store. A run's first claim: both reads, and a write.
    /// Each later claim, and each claim given up, while no other run writes: the write
    /// alone. After another run's write: the write, refused, both reads (a `blobs.bin`
    /// that is the one the run has is not sent again), and the write.
    #[test]
    fn a_run_alone_claims_with_one_request() {
        let (store, service) = service();
        let dir = std::env::temp_dir().join(format!("gvd-claims-alone-{}", std::process::id()));
        let mut a = run(&store, &dir);
        let first = a.claim(&[batch(&[1]), batch(&[2])], &SIZES).unwrap();
        assert_eq!(first.ready.len(), 2);
        assert_eq!(
            asked(&service),
            [
                "GET claims.bin 404",
                "GET blobs.bin 404",
                "PUT claims.bin 201"
            ]
        );
        a.claim(&[batch(&[3])], &SIZES).unwrap();
        assert_eq!(asked(&service), ["PUT claims.bin 201"]);
        a.release(&[first.ready[0].0, first.ready[1].0]);
        a.sync().unwrap();
        assert_eq!(asked(&service), ["PUT claims.bin 201"]);
        assert_eq!(claimed(&store), [oids(&[3])]);
        asked(&service);
        // Nothing to write: nothing is asked.
        assert!(!a.due());
        let none = a.claim(&[], &SIZES).unwrap();
        assert!(none.ready.is_empty() && asked(&service).is_empty());

        // Another run claims 4 and 5.
        let now = crate::util::now_secs();
        let theirs = Claim {
            run: 1,
            until: now + CLAIM_SECS,
            secs: CLAIM_SECS as u32,
            oids: oids(&[4, 5]),
        };
        let mut all = load(&store).unwrap();
        all.push(theirs);
        store.put(CLAIMS_KEY, &encode(&all, 9), Cond::None).unwrap();
        asked(&service);
        let taken = a.claim(&[batch(&[5, 6])], &SIZES).unwrap();
        assert_eq!(blobs(&taken), (vec![oids(&[6])], vec![oids(&[5])]));
        assert_eq!(
            asked(&service),
            [
                "PUT claims.bin 412",
                "GET claims.bin 200",
                "GET blobs.bin 404",
                "PUT claims.bin 201"
            ]
        );
        assert_eq!(claimed(&store), [oids(&[3, 6]), oids(&[4, 5])]);
    }

    /// A `claims.bin` that does not decode counts as empty, and the next claim writes
    /// it anew.
    #[test]
    fn a_claims_bin_that_does_not_decode_counts_as_empty() {
        let (store, dir) = store("undecoded");
        let good = encode(
            &[Claim {
                run: 1,
                until: u64::MAX / 2,
                secs: u32::MAX,
                oids: oids(&[1]),
            }],
            1,
        );
        for bad in [&b"not a claims.bin"[..], &good[..good.len() - 1], b""] {
            store.put(CLAIMS_KEY, bad, Cond::None).unwrap();
            assert!(load(&store).is_err());
            let mut a = run(&store, &dir);
            let taken = a.claim(&[batch(&[1, 2])], &SIZES).unwrap();
            assert_eq!(blobs(&taken), (vec![oids(&[1, 2])], vec![]));
            assert_eq!(claimed(&store), [oids(&[1, 2])]);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The claim of a run that died lapses: its blobs are taken by the next run that
    /// comes to them, whose write drops it. So is a claim from a clock too far ahead.
    /// One that holds stays as it is.
    #[test]
    fn a_claim_that_has_lapsed_is_taken_and_dropped() {
        let (store, dir) = store("lapsed");
        let now = crate::util::now_secs();
        let claim = |run: u64, until: u64, ids: &[u8]| Claim {
            run,
            until,
            secs: CLAIM_SECS as u32,
            oids: oids(ids),
        };
        let (dead, ahead, live) = (
            claim(1, now - 1, &[1, 2]),
            claim(2, now + 3 * CLAIM_SECS, &[3]),
            claim(3, now + CLAIM_SECS / 2, &[4]),
        );
        let all = [dead, ahead, live.clone()];
        store.put(CLAIMS_KEY, &encode(&all, 1), Cond::None).unwrap();
        let mut a = run(&store, &dir);
        let taken = a.claim(&[batch(&[1, 2, 3, 4, 5])], &SIZES).unwrap();
        assert_eq!(blobs(&taken), (vec![oids(&[1, 2, 3, 5])], vec![oids(&[4])]));
        assert_eq!(a.waits(&taken.later), (1, live.until));
        let mut written = load(&store).unwrap();
        written.sort_by_key(|c| c.oids.len());
        assert_eq!(written[0], live);
        assert_eq!(written[1].oids, oids(&[1, 2, 3, 5]));
        assert_eq!(written.len(), 2);
        // A's own claim holds from when it was written.
        assert!((now + CLAIM_SECS..=now + CLAIM_SECS + 5).contains(&written[1].until));
        assert_eq!(written[1].secs, CLAIM_SECS as u32);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A run writes its claim again once a third of its time is behind it: the claim
    /// then holds for all of its time again.
    #[test]
    fn a_claim_is_written_again_before_it_lapses() {
        let (store, dir) = store("renewed");
        let mut a = run(&store, &dir);
        assert!(!a.due());
        a.claim(&[batch(&[1])], &SIZES).unwrap();
        assert!(!a.due());
        // As if written a third of two minutes ago, with less time to go.
        let mut old = load(&store).unwrap();
        old[0].until -= 60;
        let etag = store.put(CLAIMS_KEY, &encode(&old, 1), Cond::None).unwrap();
        a.etag = Some(etag);
        a.written = Instant::now().checked_sub(Duration::from_secs(39)).unwrap();
        assert!(!a.due());
        a.written = Instant::now().checked_sub(Duration::from_secs(41)).unwrap();
        assert!(a.due());
        a.sync().unwrap();
        assert!(!a.due());
        let new = load(&store).unwrap();
        assert_eq!(new[0].oids, old[0].oids);
        assert!(new[0].until >= old[0].until + 60);
        // A run with no claim has none to write again.
        a.release_all();
        a.sync().unwrap();
        a.written = Instant::now().checked_sub(Duration::from_secs(41)).unwrap();
        assert!(!a.due());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Garbage collection leaves `claims.bin` alone: it is at the store's root, and a
    /// pass lists `slabs/` and `blobs/`.
    #[test]
    fn garbage_collection_leaves_claims_bin() {
        use crate::slab::{SlabMeta, Tier};
        let (store, dir) = store("gc");
        let mut a = run(&store, &dir);
        a.claim(&[batch(&[1])], &SIZES).unwrap();
        let claims = store.get(CLAIMS_KEY).unwrap();
        let baseline = SlabMeta {
            id: SlabMeta::make_id(Tier::Base, 100, &[1; 20]),
            tier: Tier::Base,
            start_ts: 0,
            end_ts: 100,
            have: Oid::ZERO,
            want: Oid::ZERO,
            objects: 1,
            pack_len: 4,
            index_len: 4,
            pack_sha: [1; 20],
        };
        let m = crate::manifest::Manifest {
            generation: 1,
            tip: Oid::ZERO,
            max_ts: 100,
            baseline,
            weekly: Vec::new(),
            pyramid: Vec::new(),
            retained: Vec::new(),
            flags: crate::manifest::FLAG_SEPARATE_BLOBS,
            blobs_generation: 0,
        };
        // A pack that no listing has goes with the second pass; `claims.bin` stays.
        let orphan = "blobs/0123.pack";
        store.put(orphan, b"pack", Cond::None).unwrap();
        let now = crate::util::now_secs();
        assert_eq!(crate::gc::collect(&store, &m, now, 0).unwrap().noted, 1);
        assert_eq!(crate::gc::collect(&store, &m, now, 0).unwrap().deleted, 1);
        assert!(store.get(orphan).unwrap().is_none());
        assert_eq!(store.get(CLAIMS_KEY).unwrap(), claims);
        assert!(crate::gc::load(&store).unwrap().is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
