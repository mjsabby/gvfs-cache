//! Blob packs in as they arrive, one whole-blob pack out (`--blobs separate`).
//!
//! [`BlobSink`] is the `Write` end of upstream fetches of blobs by id: one or more packs,
//! fed in whatever pieces the wire delivers. It parses them incrementally and writes every
//! blob asked for, exactly once, as a whole entry of one output pack
//! ([`crate::blobs::PackWriter`]):
//!
//! * **Whole entries stream through** in fixed memory, whatever their size: inflated once
//!   in 64 KiB pieces to verify the id (SHA-1 of `blob <size>\0` + content) and the size,
//!   and written either as the upstream's zlib bytes untouched or, for a blob of at least
//!   [`STORED_MIN`] whose first 256 KiB compress by less than 5%, as stored blocks (which
//!   inflate at the speed of a copy). The sink deflates those itself to tell, so the
//!   choice is the content's: a blob is encoded alike however it arrives.
//! * **Deltas** (OFS or REF) are inflated in memory and applied to their base, read back
//!   from the output (the output must be a [`Spool`] that can do that, like a local file),
//!   and the result is written whole. A REF base may arrive after its delta: the delta
//!   waits for it. Requests for large blobs are made one blob each, so their packs hold
//!   one whole entry and need no read-back.
//! * The output declares as many entries as blobs were asked for; any other object, a
//!   blob twice, a blob missing, a size or trailer mismatch fails the pack. So does a zlib
//!   stream longer than twice its content plus 1 KiB: one padded with empty blocks, which
//!   no encoder writes, and which the sink would hold in memory or write out as it came.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::Path;

use sha1::{Digest, Sha1};
use zlib_rs::{Inflate, InflateFlush, Status};

use crate::blobs::{BlobRec, Encoding, PackWriter, STORED_MIN};
use crate::pack::{
    OBJ_BLOB, OBJ_OFS_DELTA, OBJ_REF_DELTA, ObjectHeader, PACK_HEADER_LEN, PACK_TRAILER_LEN,
    parse_entry_header, parse_pack_header, type_name,
};
use crate::util::{Context, Oid, Result, u64_of, us};
use crate::zlib::{Deflater, Inflater, compress_bound};

/// Inflate output piece.
const CHUNK: usize = 64 << 10;
/// The content that chooses a large blob's encoding: its first 256 KiB.
const SAMPLE: usize = 256 << 10;
/// An entry's zlib stream may be as long as twice its content plus this. No encoder needs
/// more: deflate's longest code is 15 bits, so a byte costs under two, a block's header is
/// at most 286 bytes (the code lengths of a dynamic block), and a zlib stream's own
/// framing is 6. A longer stream is padded with empty blocks, which are valid and can go
/// on without end, as a hostile or broken upstream may send them: every byte of them was
/// held while a large blob's encoding was undecided, held again when a delta's base was
/// read back, and written to the store.
const PADDING_MAX: u64 = 1 << 10;
/// Largest delta, base or delta result held in memory.
pub const RESOLVE_MAX: u64 = 1 << 30;
/// Most delta bytes waiting for a base that has not arrived.
const WAITING_MAX: u64 = 1 << 30;
/// zlib level of blobs rebuilt from deltas, and of the sample that chooses a large blob's
/// encoding (git's default).
const LEVEL: i32 = 6;
/// Longest entry header: a 10-byte size varint and a 20-byte base id.
const HEADER_MAX: usize = 32;

/// Where a blob pack is written.
pub trait Spool: Write {
    /// Reads `buf.len()` bytes written at `at` back (the bases of deltas). Spools that
    /// cannot (a stream to object storage) fail, and so does a delta written through them.
    fn read_back(&mut self, at: u64, buf: &mut [u8]) -> Result<()>;
}

impl Spool for Vec<u8> {
    fn read_back(&mut self, at: u64, buf: &mut [u8]) -> Result<()> {
        let src = crate::util::sub(self, us(at), buf.len()).ctx("read back past the end")?;
        buf.copy_from_slice(src);
        Ok(())
    }
}

/// A blob pack spooled to a local file.
pub struct FileSpool {
    w: std::io::BufWriter<std::fs::File>,
}

impl FileSpool {
    pub fn create(path: &Path) -> Result<FileSpool> {
        let file = std::fs::File::options()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)
            .ctx(&format!("creating {}", path.display()))?;
        Ok(FileSpool {
            w: std::io::BufWriter::with_capacity(1 << 20, file),
        })
    }

    /// Flushes and syncs the file.
    pub fn close(self) -> Result<()> {
        self.end()?.sync_all().ctx("syncing blob pack")
    }

    /// Flushes the file without syncing it (a cache's file, which a crash may lose).
    pub fn end(self) -> Result<std::fs::File> {
        self.w
            .into_inner()
            .map_err(|e| crate::err!("writing blob pack: {e}"))
    }
}

impl Write for FileSpool {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.w.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
    }
}

impl Spool for FileSpool {
    fn read_back(&mut self, at: u64, buf: &mut [u8]) -> Result<()> {
        use std::os::unix::fs::FileExt;
        self.w.flush().ctx("writing blob pack")?;
        self.w
            .get_ref()
            .read_exact_at(buf, at)
            .ctx("reading back a blob pack")
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    PackHeader,
    EntryHeader,
    Whole,
    Delta,
    Trailer,
    Done,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Choice {
    Undecided,
    Raw,
    Stored,
}

/// What a delta waits for: the entry at an input offset (OFS) or an object id (REF).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Key {
    Offset(u64),
    Oid(Oid),
}

struct Waiting {
    /// Input offset of the delta's entry.
    offset: u64,
    data: Vec<u8>,
}

/// A finished blob pack.
pub struct Finished<S> {
    pub out: S,
    pub len: u64,
    pub sha: [u8; 20],
    /// One record per blob, in pack order (pack index 0).
    pub recs: Vec<BlobRec>,
}

/// A byte-rate cap shared by concurrent copies (`mirror --rate-mbps`): a token bucket of
/// `bytes_per_sec` that keeps at most `BURST_NANOS` of unused time, so a pause is not
/// credit. (A cap on the average since the start made it credit: after a 15-minute wait
/// on a 429 at 4,000 Mbps, about 450 GB passed at whatever rate the upstream gave.)
pub struct Throttle {
    bytes_per_sec: u64,
    start: std::time::Instant,
    /// When the bytes counted so far are paid for at the rate, in nanoseconds after
    /// `start`. One atomic: the copies' threads share the bucket without a lock.
    due: std::sync::atomic::AtomicU64,
}

/// The unused time a [`Throttle`] keeps as credit, a quarter of a second: after a pause
/// that much passes at once (125 MB at 4,000 Mbps), then the rate holds again. The gaps
/// between two reads of a copy (a late wake-up, a flush to the store) are milliseconds, so
/// the copies still reach the rate.
const BURST_NANOS: u64 = 250_000_000;

impl Throttle {
    pub fn new(bytes_per_sec: u64) -> Throttle {
        Throttle {
            bytes_per_sec: bytes_per_sec.max(1),
            start: std::time::Instant::now(),
            due: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Counts `n` bytes and sleeps while the copies are ahead of the rate.
    pub fn account(&self, n: usize) {
        let wait = self.charge(n, self.start.elapsed());
        if !wait.is_zero() {
            std::thread::sleep(wait);
        }
    }

    /// Counts `n` bytes taken at `now` (the time since `start`); returns how far ahead of
    /// the rate the copies then are, which is how long the caller is to wait.
    fn charge(&self, n: usize, now: std::time::Duration) -> std::time::Duration {
        use std::sync::atomic::Ordering::Relaxed;
        let now = u64::try_from(now.as_nanos()).unwrap_or(u64::MAX);
        let cost = u128::from(u64_of(n))
            .saturating_mul(1_000_000_000)
            .checked_div(u128::from(self.bytes_per_sec))
            .unwrap_or(0);
        let cost = u64::try_from(cost).unwrap_or(u64::MAX);
        // Unused time beyond the burst is lost: `due` never lags the clock by more.
        let floor = now.saturating_sub(BURST_NANOS);
        let pay = |due: u64| due.max(floor).saturating_add(cost);
        let due = pay(self.due.update(Relaxed, Relaxed, pay));
        std::time::Duration::from_nanos(due.saturating_sub(now))
    }
}

pub struct BlobSink<S: Spool> {
    out: PackWriter<S>,
    throttle: Option<std::sync::Arc<Throttle>>,
    want: HashSet<Oid>,
    recs: Vec<BlobRec>,
    by_oid: HashMap<Oid, usize>,
    failed: Option<crate::util::Error>,

    // The input pack being parsed.
    stage: Stage,
    hdr: Vec<u8>,
    left: u32,
    pos: u64,
    input_sha: Sha1,
    by_offset: HashMap<u64, usize>,
    waiting: HashMap<Key, Vec<Waiting>>,
    waiting_bytes: u64,

    // The entry being inflated.
    entry_at: u64,
    size: u64,
    key: Key,
    z: Inflate,
    obj_sha: Sha1,
    produced: u64,
    choice: Choice,
    sample_z: Vec<u8>,
    sample_c: Vec<u8>,
    delta: Vec<u8>,
    chunk: Vec<u8>,

    // Delta resolution.
    inflater: Inflater,
    deflater: Deflater,
    entry_buf: Vec<u8>,
    base_buf: Vec<u8>,
    /// The id whose content `base_buf` holds.
    base_oid: Option<Oid>,
    target: Vec<u8>,
    zbuf: Vec<u8>,
}

impl<S: Spool> BlobSink<S> {
    /// A pack of exactly the blobs `want` (duplicates count once) is written to `out`.
    pub fn new(out: S, want: &[Oid]) -> Result<BlobSink<S>> {
        let want: HashSet<Oid> = want.iter().copied().collect();
        let count = u32::try_from(want.len())
            .ok()
            .ctx("too many blobs for one pack")?;
        Ok(BlobSink {
            out: PackWriter::new(out, count)?,
            throttle: None,
            recs: Vec::with_capacity(want.len()),
            by_oid: HashMap::with_capacity(want.len()),
            want,
            failed: None,
            stage: Stage::PackHeader,
            hdr: Vec::with_capacity(HEADER_MAX),
            left: 0,
            pos: 0,
            input_sha: Sha1::new(),
            by_offset: HashMap::new(),
            waiting: HashMap::new(),
            waiting_bytes: 0,
            entry_at: 0,
            size: 0,
            key: Key::Offset(0),
            z: Inflate::new(true, 15),
            obj_sha: Sha1::new(),
            produced: 0,
            choice: Choice::Raw,
            sample_z: Vec::new(),
            sample_c: Vec::new(),
            delta: Vec::new(),
            chunk: vec![0u8; CHUNK],
            inflater: Inflater::new(),
            deflater: Deflater::new(LEVEL),
            entry_buf: Vec::new(),
            base_buf: Vec::new(),
            base_oid: None,
            target: Vec::new(),
            zbuf: Vec::new(),
        })
    }

    /// Paces the input to a shared byte rate.
    pub fn throttle(&mut self, t: std::sync::Arc<Throttle>) {
        self.throttle = Some(t);
    }

    /// The error that failed the sink, if any (taken: a write error only carries its text).
    pub fn failure(&mut self) -> Option<crate::util::Error> {
        self.failed.take()
    }

    /// Blobs asked for that have not arrived (yet).
    pub fn missing(&self) -> impl Iterator<Item = &Oid> {
        self.want.iter()
    }

    /// Bytes of the output so far.
    pub fn written(&self) -> u64 {
        self.out.position()
    }

    /// The input pack fed so far must be complete; the next bytes start a new one.
    pub fn end_input(&mut self) -> Result<()> {
        if let Some(e) = self.failed.take() {
            return Err(e);
        }
        if self.stage != Stage::Done {
            crate::bail!(
                "truncated pack: {} entries and the trailer missing",
                self.left
            );
        }
        if !self.waiting.is_empty() {
            let n: usize = self.waiting.values().map(Vec::len).sum();
            crate::bail!("{n} deltas whose base is not in the pack");
        }
        self.stage = Stage::PackHeader;
        self.by_offset.clear();
        self.pos = 0;
        self.hdr.clear();
        self.input_sha = Sha1::new();
        Ok(())
    }

    /// Every blob asked for has arrived: writes the trailer.
    pub fn finish(self) -> Result<Finished<S>> {
        if let Some(e) = self.failed {
            return Err(e);
        }
        if self.stage != Stage::PackHeader {
            crate::bail!("blob pack finished in the middle of an input pack");
        }
        if let Some(first) = self.want.iter().next() {
            crate::bail!(
                "{} blobs asked for did not arrive (e.g. {first})",
                self.want.len()
            );
        }
        let (out, len, sha) = self.out.finish()?;
        Ok(Finished {
            out,
            len,
            sha,
            recs: self.recs,
        })
    }

    fn feed(&mut self, mut buf: &[u8]) -> Result<()> {
        while !buf.is_empty() {
            let used = match self.stage {
                Stage::PackHeader => self.pack_header(buf)?,
                Stage::EntryHeader => self.entry_header(buf)?,
                Stage::Whole | Stage::Delta => self.body(buf)?,
                Stage::Trailer => self.trailer(buf)?,
                Stage::Done => crate::bail!("bytes after the pack trailer"),
            };
            buf = buf.get(used..).unwrap_or(&[]);
        }
        Ok(())
    }

    /// Takes input bytes into `hdr` up to `n`; returns how many it took.
    fn take(&mut self, buf: &[u8], n: usize) -> usize {
        let k = n.saturating_sub(self.hdr.len()).min(buf.len());
        self.hdr.extend_from_slice(buf.get(..k).unwrap_or(&[]));
        k
    }

    fn consumed(&mut self, bytes: &[u8]) {
        self.input_sha.update(bytes);
        self.pos = self.pos.saturating_add(u64_of(bytes.len()));
    }

    fn pack_header(&mut self, buf: &[u8]) -> Result<usize> {
        let used = self.take(buf, PACK_HEADER_LEN);
        if self.hdr.len() == PACK_HEADER_LEN {
            self.left = parse_pack_header(&self.hdr).ctx("not a pack")?;
            // Every entry is a blob still asked for, once, so a pack that declares more
            // entries than that is refused before any is read. (Its deltas would wait for
            // bases that never come, bounded in bytes only: 2,000,000 empty ones, a 58 MB
            // pack, took 494 MB.)
            if us(u64::from(self.left)) > self.want.len() {
                crate::bail!(
                    "the upstream's pack has {} entries for the {} blobs asked for",
                    self.left,
                    self.want.len()
                );
            }
            let hdr = std::mem::take(&mut self.hdr);
            self.consumed(&hdr);
            self.hdr = hdr;
            self.hdr.clear();
            self.stage = if self.left == 0 {
                Stage::Trailer
            } else {
                Stage::EntryHeader
            };
        }
        Ok(used)
    }

    fn entry_header(&mut self, buf: &[u8]) -> Result<usize> {
        for (i, &b) in buf.iter().enumerate() {
            self.hdr.push(b);
            if let Some(h) = parse_entry_header(&self.hdr) {
                self.entry_at = self.pos;
                let hdr = std::mem::take(&mut self.hdr);
                self.consumed(&hdr);
                self.hdr = hdr;
                self.hdr.clear();
                self.start_entry(h.kind, h.size, h.base_distance, h.base_oid)?;
                return Ok(i.saturating_add(1));
            }
            if self.hdr.len() >= HEADER_MAX {
                crate::bail!("malformed entry header at offset {}", self.pos);
            }
        }
        Ok(buf.len())
    }

    fn start_entry(&mut self, kind: u8, size: u64, distance: u64, base: Oid) -> Result<()> {
        self.size = size;
        self.produced = 0;
        self.z.reset(true);
        match kind {
            OBJ_BLOB => {
                self.stage = Stage::Whole;
                self.obj_sha = Sha1::new();
                self.obj_sha
                    .update(ObjectHeader::new(OBJ_BLOB, size).as_slice());
                self.sample_z.clear();
                self.sample_c.clear();
                self.choice = if size >= STORED_MIN {
                    Choice::Undecided
                } else {
                    self.out.begin(size, Encoding::Raw)?;
                    Choice::Raw
                };
            }
            OBJ_OFS_DELTA | OBJ_REF_DELTA => {
                if size > RESOLVE_MAX {
                    crate::bail!("a {size}-byte delta is too large to resolve");
                }
                self.key = if kind == OBJ_OFS_DELTA {
                    let at = self
                        .entry_at
                        .checked_sub(distance)
                        .filter(|&at| distance > 0 && at >= u64_of(PACK_HEADER_LEN))
                        .ctx("delta base offset out of range")?;
                    Key::Offset(at)
                } else {
                    Key::Oid(base)
                };
                self.stage = Stage::Delta;
                self.delta = Vec::with_capacity(us(size));
            }
            other => crate::bail!(
                "the upstream sent a {} where blobs were asked for",
                type_name(other)
            ),
        }
        Ok(())
    }

    fn body(&mut self, buf: &[u8]) -> Result<usize> {
        let (in0, out0) = (self.z.total_in(), self.z.total_out());
        let status = self
            .z
            .decompress(buf, &mut self.chunk, InflateFlush::NoFlush)
            .map_err(|_| crate::err!("corrupt zlib stream at offset {}", self.entry_at))?;
        let used = us(self.z.total_in().saturating_sub(in0));
        let made = us(self.z.total_out().saturating_sub(out0));
        let input = buf.get(..used).ctx("inflate consumed past its input")?;
        self.consumed(input);
        self.produced = self.produced.saturating_add(u64_of(made));
        if self.produced > self.size {
            crate::bail!(
                "entry at offset {} inflates past its declared {} bytes",
                self.entry_at,
                self.size
            );
        }
        let taken = self.z.total_in();
        if taken > self.produced.saturating_mul(2).saturating_add(PADDING_MAX) {
            crate::bail!(
                "entry at offset {}: {taken} bytes of zlib stream for {} bytes of content, \
                 more than any encoder needs",
                self.entry_at,
                self.produced
            );
        }
        let content = self.chunk.get(..made).ctx("inflate output")?;
        if self.stage == Stage::Whole {
            self.obj_sha.update(content);
            match self.choice {
                Choice::Raw => self.out.raw(input)?,
                Choice::Stored => self.out.content(content)?,
                Choice::Undecided => {
                    // Less than SAMPLE + CHUNK of content is held, and so (the check
                    // above) at most twice that plus PADDING_MAX of its stream: 641 KiB.
                    self.sample_z.extend_from_slice(input);
                    self.sample_c.extend_from_slice(content);
                    if self.sample_c.len() >= SAMPLE {
                        self.decide()?;
                    }
                }
            }
        } else {
            self.delta.extend_from_slice(content);
        }
        match status {
            Status::StreamEnd => {
                if self.produced != self.size {
                    crate::bail!(
                        "entry at offset {} inflates to {} bytes, not {}",
                        self.entry_at,
                        self.produced,
                        self.size
                    );
                }
                if self.stage == Stage::Whole {
                    self.end_whole()?;
                } else {
                    self.end_delta()?;
                }
                self.left = self.left.saturating_sub(1);
                self.stage = if self.left == 0 {
                    Stage::Trailer
                } else {
                    Stage::EntryHeader
                };
            }
            _ if used == 0 && made == 0 => {
                crate::bail!("zlib stream at offset {} is stuck", self.entry_at)
            }
            _ => {}
        }
        Ok(used)
    }

    /// Chooses a large blob's encoding from its first [`SAMPLE`] bytes and writes out
    /// what was held meanwhile.
    fn decide(&mut self) -> Result<()> {
        if incompressible(&mut self.deflater, &mut self.zbuf, &self.sample_c)? {
            self.out.begin(self.size, Encoding::Stored)?;
            self.out.content(&self.sample_c)?;
            self.choice = Choice::Stored;
        } else {
            self.out.begin(self.size, Encoding::Raw)?;
            self.out.raw(&self.sample_z)?;
            self.choice = Choice::Raw;
        }
        self.sample_z.clear();
        self.sample_c.clear();
        Ok(())
    }

    fn end_whole(&mut self) -> Result<()> {
        if self.choice == Choice::Undecided {
            self.decide()?;
        }
        let oid = Oid(std::mem::take(&mut self.obj_sha).finalize().into());
        if !self.want.remove(&oid) {
            crate::bail!("the upstream sent blob {oid}, which was not asked for (or twice)");
        }
        let rec = self.out.end(oid)?;
        let i = self.record(rec, self.entry_at);
        self.wake(self.entry_at, oid, i)
    }

    fn end_delta(&mut self) -> Result<()> {
        let data = std::mem::take(&mut self.delta);
        let base = match self.key {
            Key::Offset(at) => self.by_offset.get(&at).copied(),
            Key::Oid(oid) => self.by_oid.get(&oid).copied(),
        };
        match base {
            Some(b) => {
                let at = self.entry_at;
                let (oid, i) = self.apply(b, &data, at)?;
                self.wake(at, oid, i)
            }
            None => {
                self.waiting_bytes = self.waiting_bytes.saturating_add(u64_of(data.len()));
                if self.waiting_bytes > WAITING_MAX {
                    crate::bail!("too many deltas wait for bases further in the pack");
                }
                self.waiting.entry(self.key).or_default().push(Waiting {
                    offset: self.entry_at,
                    data,
                });
                Ok(())
            }
        }
    }

    fn record(&mut self, rec: BlobRec, input_at: u64) -> usize {
        let i = self.recs.len();
        self.recs.push(rec);
        self.by_oid.insert(rec.oid, i);
        self.by_offset.insert(input_at, i);
        i
    }

    /// Resolves the deltas waiting for the blob just written (and theirs, in turn).
    fn wake(&mut self, at: u64, oid: Oid, i: usize) -> Result<()> {
        let mut ready = vec![(at, oid, i)];
        while let Some((at, oid, i)) = ready.pop() {
            for key in [Key::Offset(at), Key::Oid(oid)] {
                for w in self.waiting.remove(&key).unwrap_or_default() {
                    self.waiting_bytes = self.waiting_bytes.saturating_sub(u64_of(w.data.len()));
                    let (oid, j) = self.apply(i, &w.data, w.offset)?;
                    ready.push((w.offset, oid, j));
                }
            }
        }
        Ok(())
    }

    /// Applies `delta` to blob `base` (a record), writes the result whole and records it
    /// for the input entry at `at`.
    fn apply(&mut self, base: usize, delta: &[u8], at: u64) -> Result<(Oid, usize)> {
        let rec = *self.recs.get(base).ctx("delta base record")?;
        self.load(&rec)?;
        // The size the delta's ops make, not the one its header declares, which costs
        // nothing to state: a few bytes declaring a gigabyte had it allocated and zeroed.
        let size = crate::delta::result_size(delta, us(rec.size))
            .map_err(|e| crate::err!("the delta at offset {at}: {e:?}"))?;
        if size > RESOLVE_MAX {
            crate::bail!("a {size}-byte blob behind a delta is too large to resolve");
        }
        self.target.resize(us(size), 0);
        let base_bytes = self.base_buf.get(..us(rec.size)).ctx("delta base")?;
        crate::delta::apply_delta(base_bytes, delta, &mut self.target)
            .map_err(|e| crate::err!("applying the delta at offset {at}: {e:?}"))?;
        let mut h = Sha1::new();
        h.update(ObjectHeader::new(OBJ_BLOB, size).as_slice());
        h.update(&self.target);
        let oid = Oid(h.finalize().into());
        if !self.want.remove(&oid) {
            crate::bail!("the upstream sent blob {oid}, which was not asked for (or twice)");
        }
        if size >= STORED_MIN && incompressible(&mut self.deflater, &mut self.zbuf, &self.target)? {
            self.out.begin(size, Encoding::Stored)?;
            self.out.content(&self.target)?;
        } else {
            let n = compress_bound(self.target.len());
            self.zbuf.resize(n, 0);
            let zlen = self
                .deflater
                .deflate_parts(&[&self.target], &mut self.zbuf)?;
            self.out.begin(size, Encoding::Raw)?;
            self.out.raw(self.zbuf.get(..zlen).ctx("deflate output")?)?;
        }
        let new = self.out.end(oid)?;
        // The result is the likeliest next base (chains are written base first).
        std::mem::swap(&mut self.base_buf, &mut self.target);
        self.base_oid = Some(oid);
        Ok((oid, self.record(new, at)))
    }

    /// Loads blob `rec`'s content into `base_buf`.
    fn load(&mut self, rec: &BlobRec) -> Result<()> {
        if self.base_oid == Some(rec.oid) {
            return Ok(());
        }
        if rec.size > RESOLVE_MAX {
            crate::bail!("a {}-byte delta base is too large to resolve", rec.size);
        }
        self.base_oid = None;
        // The entry is at most twice the base plus PADDING_MAX (and its header): `body` lets
        // no upstream stream be longer, and what is written here otherwise is shorter.
        self.entry_buf.resize(us(rec.entry_len), 0);
        self.out
            .spool()
            .read_back(rec.offset, &mut self.entry_buf)?;
        let zlib = self
            .entry_buf
            .get(usize::from(rec.hdr_len)..)
            .ctx("blob entry")?;
        self.base_buf.resize(us(rec.size), 0);
        self.inflater.inflate_exact(zlib, &mut self.base_buf)?;
        self.base_oid = Some(rec.oid);
        Ok(())
    }

    fn trailer(&mut self, buf: &[u8]) -> Result<usize> {
        let used = self.take(buf, PACK_TRAILER_LEN);
        if self.hdr.len() == PACK_TRAILER_LEN {
            let sha: [u8; 20] = std::mem::take(&mut self.input_sha).finalize().into();
            if self.hdr.as_slice() != sha.as_slice() {
                crate::bail!("pack trailer does not match its content");
            }
            self.hdr.clear();
            self.stage = Stage::Done;
        }
        Ok(used)
    }
}

/// Whether a blob of at least [`STORED_MIN`] is written as stored blocks: when its first
/// [`SAMPLE`] bytes deflate by less than 5% (at [`LEVEL`], into `zbuf`). That depends on
/// the blob's bytes alone, so every copy of a blob chooses alike: however the upstream
/// compressed it, whole or behind a delta, and however the wire cut it into pieces. (The
/// measure used to be the upstream's bytes for as much content as the pieces had made when
/// a MiB was in, or, behind a delta, the whole blob deflated: one blob could go either
/// way.) The price is a deflate of 256 KiB per large blob, about 4 ms, text or noise,
/// where inflating and hashing a MiB take 0.5 ms: a mirror of 1.2 MiB blobs from a local
/// upstream takes 1.5 to 1.7 times as long for it. (A sample of a whole MiB cost 16 ms,
/// and that mirror 3.4 to 4.1 times as long.)
fn incompressible(deflater: &mut Deflater, zbuf: &mut Vec<u8>, blob: &[u8]) -> Result<bool> {
    let sample = blob.get(..SAMPLE).unwrap_or(blob);
    zbuf.resize(compress_bound(sample.len()), 0);
    let zlen = deflater.deflate_parts(&[sample], zbuf)?;
    Ok(u64_of(zlen).saturating_mul(100) >= u64_of(sample.len()).saturating_mul(95))
}

impl<S: Spool> Write for BlobSink<S> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Some(e) = &self.failed {
            return Err(std::io::Error::other(e.to_string()));
        }
        if let Some(t) = &self.throttle {
            t.account(buf.len());
        }
        match self.feed(buf) {
            Ok(()) => Ok(buf.len()),
            Err(e) => {
                let msg = e.to_string();
                self.failed = Some(e);
                Err(std::io::Error::other(msg))
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
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
    use crate::pack::HeaderBytes;
    use std::process::{Command, Stdio};

    fn git(dir: &Path, args: &[&str], stdin: &[u8]) -> Vec<u8> {
        let mut cmd = Command::new("git");
        cmd.args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null");
        crate::upstream::hermetic_git_env(&mut cmd);
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let data = stdin.to_vec();
        let feeder = std::thread::spawn(move || input.write_all(&data));
        let out = child.wait_with_output().unwrap();
        feeder.join().unwrap().unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        out.stdout
    }

    fn blob_id(content: &[u8]) -> Oid {
        let mut h = Sha1::new();
        h.update(ObjectHeader::new(OBJ_BLOB, u64_of(content.len())).as_slice());
        h.update(content);
        Oid(h.finalize().into())
    }

    fn noise(n: usize, seed: u64) -> Vec<u8> {
        let mut x = seed | 1;
        (0..n)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    struct Repo {
        dir: std::path::PathBuf,
        blobs: Vec<(Oid, Vec<u8>)>,
    }

    impl Drop for Repo {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// A repository with versions of a text file (deltas), an empty blob, a large
    /// incompressible blob and a large compressible one.
    fn repo(tag: &str) -> Repo {
        let dir = std::env::temp_dir().join(format!("gvd-bs-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        git(&dir, &["init", "-q", "--bare", "r.git"], b"");
        let r = dir.join("r.git");
        let mut contents: Vec<Vec<u8>> = Vec::new();
        let mut text: Vec<u8> = (0..2000)
            .flat_map(|i| format!("line {i} of a file that changes a little\n").into_bytes())
            .collect();
        for v in 0..12 {
            let at = (v * 3001) % (text.len() - 10);
            text[at..at + 5].copy_from_slice(format!("v{v:04}").as_bytes());
            contents.push(text.clone());
        }
        contents.push(Vec::new());
        contents.push(noise(2 << 20, 7));
        contents.push(vec![b'z'; (3 << 20) + 17]);
        contents.push(b"tiny\n".to_vec());
        let mut blobs = Vec::new();
        for c in contents {
            let oid = git(&r, &["hash-object", "-w", "--stdin"], &c);
            let oid = Oid::from_hex(oid.trim_ascii()).unwrap();
            assert_eq!(oid, blob_id(&c));
            blobs.push((oid, c));
        }
        Repo { dir, blobs }
    }

    fn pack_of(r: &Repo, ids: &[Oid], ofs: bool) -> Vec<u8> {
        let list: String = ids.iter().map(|o| format!("{o}\n")).collect();
        let mut args = vec![
            "pack-objects",
            "--stdout",
            "-q",
            "--window=10",
            "--depth=20",
        ];
        if ofs {
            args.push("--delta-base-offset");
        }
        git(&r.dir.join("r.git"), &args, list.as_bytes())
    }

    fn kinds(pack: &[u8]) -> Vec<u8> {
        let mut idx = crate::ingest::Indexer::new(1);
        let _ = idx.index(pack).unwrap();
        let mut out = Vec::new();
        let mut pos = PACK_HEADER_LEN;
        let n = parse_pack_header(pack).unwrap();
        let mut inf = Inflater::new();
        let mut chunk = vec![0u8; 1 << 16];
        for _ in 0..n {
            let h = parse_entry_header(&pack[pos..]).unwrap();
            out.push(h.kind);
            let used = inf
                .inflate_into(&pack[pos + h.header_len..], h.size, &mut chunk, |_| {})
                .unwrap();
            pos += h.header_len + used;
        }
        out
    }

    fn feed_in_pieces<S: Spool>(sink: &mut BlobSink<S>, pack: &[u8], pieces: &[usize]) {
        let mut at = 0;
        let mut i = 0;
        while at < pack.len() {
            let n = pieces[i % pieces.len()].min(pack.len() - at);
            sink.write_all(&pack[at..at + n]).unwrap();
            at += n;
            i += 1;
        }
        sink.end_input().unwrap();
    }

    /// The output is a valid pack of exactly the blobs, each record describing its entry.
    fn check_output(r: &Repo, f: &Finished<Vec<u8>>, want: &[(Oid, Vec<u8>)]) {
        let check = r.dir.join(format!("check-{}.git", f.len));
        let _ = std::fs::remove_dir_all(&check);
        git(
            &r.dir,
            &["init", "-q", "--bare", check.to_str().unwrap()],
            b"",
        );
        git(&check, &["index-pack", "--stdin", "--strict"], &f.out);
        let listed = String::from_utf8(git(
            &check,
            &[
                "cat-file",
                "--batch-all-objects",
                "--batch-check=%(objectname) %(objecttype) %(objectsize)",
            ],
            b"",
        ))
        .unwrap();
        let mut got: Vec<String> = listed.lines().map(str::to_string).collect();
        let mut expect: Vec<String> = want
            .iter()
            .map(|(o, c)| format!("{o} blob {}", c.len()))
            .collect();
        got.sort();
        expect.sort();
        assert_eq!(got, expect);
        assert_eq!(f.recs.len(), want.len());
        assert!(
            kinds(&f.out).iter().all(|&k| k == OBJ_BLOB),
            "whole blobs only"
        );
        let mut inf = Inflater::new();
        for rec in &f.recs {
            let content = &want.iter().find(|w| w.0 == rec.oid).unwrap().1;
            assert_eq!(rec.size, u64_of(content.len()));
            let entry = &f.out[us(rec.offset)..us(rec.offset + rec.entry_len)];
            let h = parse_entry_header(entry).unwrap();
            assert_eq!(
                (h.kind, h.size, h.header_len),
                (OBJ_BLOB, rec.size, usize::from(rec.hdr_len))
            );
            let mut buf = vec![0u8; content.len()];
            let used = inf.inflate_exact(&entry[h.header_len..], &mut buf).unwrap();
            assert_eq!(used, entry.len() - h.header_len);
            assert_eq!(&buf, content);
        }
    }

    #[test]
    fn streams_whole_blobs_and_resolves_deltas() {
        let r = repo("main");
        let all: Vec<Oid> = r.blobs.iter().map(|b| b.0).collect();
        for ofs in [false, true] {
            let pack = pack_of(&r, &all, ofs);
            let k = kinds(&pack);
            let delta_kind = if ofs { OBJ_OFS_DELTA } else { OBJ_REF_DELTA };
            assert!(k.contains(&delta_kind), "the test pack has deltas ({k:?})");
            for pieces in [&[1usize, 2, 3, 5, 7, 11][..], &[1 << 20], &[usize::MAX]] {
                let mut sink = BlobSink::new(Vec::new(), &all).unwrap();
                feed_in_pieces(&mut sink, &pack, pieces);
                let f = sink.finish().unwrap();
                check_output(&r, &f, &r.blobs);
                let by: HashMap<Oid, &BlobRec> = f.recs.iter().map(|x| (x.oid, x)).collect();
                assert!(
                    by[&r.blobs[13].0].stored,
                    "incompressible 2 MiB blob stored"
                );
                assert!(
                    !by[&r.blobs[14].0].stored,
                    "compressible 3 MiB blob kept raw"
                );
            }
        }
        // The same blobs over two input packs, one pack out.
        let (a, b) = all.split_at(6);
        let mut sink = BlobSink::new(Vec::new(), &all).unwrap();
        feed_in_pieces(&mut sink, &pack_of(&r, a, true), &[4096]);
        feed_in_pieces(&mut sink, &pack_of(&r, b, false), &[777]);
        check_output(&r, &sink.finish().unwrap(), &r.blobs);
    }

    fn deflate(data: &[u8]) -> Vec<u8> {
        let mut d = Deflater::new(6);
        let mut out = vec![0u8; compress_bound(data.len())];
        let n = d.deflate_parts(&[data], &mut out).unwrap();
        out.truncate(n);
        out
    }

    /// `base ++ tail` as a delta: one copy of the whole base, one insert.
    fn copy_insert_delta(base_len: usize, tail: &[u8]) -> Vec<u8> {
        fn varint(mut v: usize, out: &mut Vec<u8>) {
            loop {
                let b = (v & 0x7f) as u8;
                v >>= 7;
                if v == 0 {
                    out.push(b);
                    return;
                }
                out.push(b | 0x80);
            }
        }
        let mut d = Vec::new();
        varint(base_len, &mut d);
        varint(base_len + tail.len(), &mut d);
        d.push(0x80 | 0x10 | 0x20 | 0x40);
        d.extend_from_slice(&[
            (base_len & 0xff) as u8,
            (base_len >> 8 & 0xff) as u8,
            (base_len >> 16) as u8,
        ]);
        d.push(u8::try_from(tail.len()).unwrap());
        d.extend_from_slice(tail);
        d
    }

    fn hand_pack(entries: &[(HeaderBytes, Vec<u8>)]) -> Vec<u8> {
        let deflated: Vec<(HeaderBytes, Vec<u8>)> =
            entries.iter().map(|(h, b)| (*h, deflate(b))).collect();
        raw_pack(&deflated)
    }

    /// A pack of entries given with their zlib streams.
    fn raw_pack(entries: &[(HeaderBytes, Vec<u8>)]) -> Vec<u8> {
        let mut p = crate::pack::pack_header(u32::try_from(entries.len()).unwrap()).to_vec();
        for (h, zlib) in entries {
            p.extend_from_slice(h.as_slice());
            p.extend_from_slice(zlib);
        }
        let sha: [u8; 20] = Sha1::digest(&p).into();
        p.extend_from_slice(&sha);
        p
    }

    #[test]
    fn forward_ref_deltas_wait_for_their_base() {
        let r = repo("fwd");
        let base = b"the base blob, which arrives last\n".repeat(50);
        let mid = [base.clone(), b"one more line\n".to_vec()].concat();
        let top = [mid.clone(), b"and another\n".to_vec()].concat();
        let (b, m, t) = (blob_id(&base), blob_id(&mid), blob_id(&top));
        let d_mid = copy_insert_delta(base.len(), b"one more line\n");
        let d_top = copy_insert_delta(mid.len(), b"and another\n");
        // top waits for mid, which waits for base.
        let pack = hand_pack(&[
            (HeaderBytes::ref_delta(u64_of(d_top.len()), &m), d_top),
            (HeaderBytes::ref_delta(u64_of(d_mid.len()), &b), d_mid),
            (
                HeaderBytes::whole(OBJ_BLOB, u64_of(base.len())),
                base.clone(),
            ),
        ]);
        let mut sink = BlobSink::new(Vec::new(), &[t, m, b]).unwrap();
        feed_in_pieces(&mut sink, &pack, &[3]);
        let f = sink.finish().unwrap();
        check_output(&r, &f, &[(b, base), (m, mid), (t, top)]);
    }

    /// A pack that declares more entries than blobs were asked for is refused at its
    /// header: nothing it holds is read. (Each entry was refused when it came; deltas
    /// whose base never comes waited, and their number had no bound.) And a delta whose
    /// header declares a result its ops do not make sizes no buffer by it.
    #[test]
    fn a_pack_cannot_ask_for_more_than_its_blobs_take() {
        let base = b"a small base\n".to_vec();
        let (b, absent) = (blob_id(&base), Oid([9; 20]));
        let empty_deltas: Vec<(HeaderBytes, Vec<u8>)> = (0..3)
            .map(|_| (HeaderBytes::ref_delta(0, &absent), Vec::new()))
            .collect();
        let pack = hand_pack(&empty_deltas);
        let mut sink = BlobSink::new(Vec::new(), &[b, absent]).unwrap();
        assert!(sink.write_all(&pack[..PACK_HEADER_LEN]).is_err());
        let e = sink.failure().unwrap().to_string();
        assert!(e.contains("3 entries for the 2 blobs asked for"), "{e}");

        // `base ++ "x"`, as a delta whose header says the result is a gigabyte.
        let mut lying = copy_insert_delta(base.len(), b"x");
        let honest_len = lying.len();
        lying.splice(1..2, [0x80, 0x80, 0x80, 0x80, 0x04]);
        assert_eq!(lying.len(), honest_len + 4);
        let pack = hand_pack(&[
            (HeaderBytes::whole(OBJ_BLOB, u64_of(base.len())), base),
            (HeaderBytes::ref_delta(u64_of(lying.len()), &b), lying),
        ]);
        let mut sink = BlobSink::new(Vec::new(), &[b, absent]).unwrap();
        assert!(sink.write_all(&pack).is_err());
        let e = sink.failure().unwrap().to_string();
        assert!(e.contains("ResultSize"), "{e}");
        assert!(
            sink.target.capacity() < 1 << 20,
            "a buffer of {}",
            sink.target.capacity()
        );
    }

    #[test]
    fn rejects_what_was_not_asked_for() {
        let r = repo("err");
        let all: Vec<Oid> = r.blobs.iter().map(|b| b.0).collect();
        let pack = pack_of(&r, &all, true);
        let feed = |want: &[Oid], pack: &[u8]| -> Result<Finished<Vec<u8>>> {
            let mut sink = BlobSink::new(Vec::new(), want)?;
            let _ = sink.write_all(pack);
            sink.end_input()?;
            sink.finish()
        };
        let err = |r: Result<Finished<Vec<u8>>>| r.err().unwrap().to_string();
        let mut other = all[1..].to_vec();
        other.push(Oid([8; 20]));
        assert!(err(feed(&other, &pack)).contains("not asked for"));
        let fewer = format!("{} entries for the {} blobs", all.len(), all.len() - 1);
        assert!(err(feed(&all[1..], &pack)).contains(&fewer));
        let mut more = all.clone();
        more.push(Oid([7; 20]));
        assert!(err(feed(&more, &pack)).contains("did not arrive"));
        let mut bad = pack.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert!(err(feed(&all, &bad)).contains("trailer"));
        assert!(err(feed(&all, &pack[..pack.len() - 30])).contains("truncated"));
        let mut flipped = pack.clone();
        let mid = flipped.len() / 2;
        flipped[mid] ^= 0x55;
        assert!(feed(&all, &flipped).is_err());
        let tree = hand_pack(&[(HeaderBytes::whole(crate::pack::OBJ_TREE, 0), Vec::new())]);
        assert!(err(feed(&all, &tree)).contains("tree"));

        // A spool that cannot read back takes whole blobs only.
        struct Stream(Vec<u8>);
        impl Write for Stream {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.write(b)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl Spool for Stream {
            fn read_back(&mut self, _: u64, _: &mut [u8]) -> Result<()> {
                crate::bail!("streamed: no read back")
            }
        }
        let mut sink = BlobSink::new(Stream(Vec::new()), &all).unwrap();
        assert!(sink.write_all(&pack).is_err());
        assert!(
            sink.end_input()
                .unwrap_err()
                .to_string()
                .contains("read back")
        );
        let one = [r.blobs[13].0];
        let mut sink = BlobSink::new(Stream(Vec::new()), &one).unwrap();
        sink.write_all(&pack_of(&r, &one, true)).unwrap();
        sink.end_input().unwrap();
        let f = sink.finish().unwrap();
        assert_eq!(f.recs.len(), 1);
        assert!(f.recs[0].stored);
    }

    fn whole_entry(content: &[u8]) -> (HeaderBytes, Vec<u8>) {
        (
            HeaderBytes::whole(OBJ_BLOB, u64_of(content.len())),
            content.to_vec(),
        )
    }

    fn entry_bytes<'a>(pack: &'a [u8], rec: &BlobRec) -> &'a [u8] {
        &pack[us(rec.offset)..us(rec.offset + rec.entry_len)]
    }

    /// A blob on the 5% line whose content changes right after its first 256 KiB: those
    /// deflate to about 96%, then come zeros. Its encoding is chosen from them alone, so
    /// however the input is cut into writes, the output is one and the same pack. (The
    /// choice used to be made from as much content as the writes had produced once the
    /// sample was in: a first write of 57,000 bytes moved that point tens of kilobytes into
    /// the zeros, and the blob stored after one cut was kept deflated after another.)
    #[test]
    fn the_encoding_does_not_depend_on_how_the_input_is_cut() {
        let r = repo("cut");
        let mut content = Vec::new();
        while content.len() < SAMPLE {
            content.extend_from_slice(&noise(3900, 2 * content.len() as u64 + 1));
            content.extend_from_slice(&[0u8; 196]);
        }
        content.extend_from_slice(&vec![0u8; us(STORED_MIN)]);
        let first = deflate(&content[..SAMPLE]).len();
        assert!(
            first * 100 >= SAMPLE * 95,
            "the first 256 KiB deflate to {first}"
        );
        assert!(deflate(&content[..SAMPLE + 60_000]).len() * 100 < (SAMPLE + 60_000) * 95);
        let oid = blob_id(&content);
        let pack = hand_pack(&[whole_entry(&content)]);
        let cuts: [&[usize]; 5] = [
            &[usize::MAX],
            &[57_000, usize::MAX],
            &[CHUNK],
            &[1, 2, 3, 5, 7, 11],
            &[100_000, 4096],
        ];
        let outs: Vec<Finished<Vec<u8>>> = cuts
            .iter()
            .map(|cut| {
                let mut sink = BlobSink::new(Vec::new(), &[oid]).unwrap();
                feed_in_pieces(&mut sink, &pack, cut);
                sink.finish().unwrap()
            })
            .collect();
        check_output(&r, &outs[0], &[(oid, content)]);
        for (f, cut) in outs.iter().zip(cuts) {
            assert!(f.recs[0].stored, "cut {cut:?}");
            assert!(f.out == outs[0].out, "cut {cut:?}: another pack");
        }
    }

    /// One blob, whole in one pack and a delta's result in another: one encoding, and the
    /// same entry bytes. Its first 256 KiB are noise and the rest zeros, so it is stored.
    /// (A delta's result used to be judged by all of it: a fifth of its size deflated, it
    /// was written deflated.)
    #[test]
    fn a_blob_is_encoded_alike_whole_or_from_a_delta() {
        let r = repo("alike");
        let base = [noise(SAMPLE, 11), vec![0u8; us(STORED_MIN)]].concat();
        let tail = b"and a last line\n";
        let top = [base.clone(), tail.to_vec()].concat();
        let (b, t) = (blob_id(&base), blob_id(&top));
        let d = copy_insert_delta(base.len(), tail);
        let with_delta = hand_pack(&[
            whole_entry(&base),
            (HeaderBytes::ref_delta(u64_of(d.len()), &b), d),
        ]);
        let mut sink = BlobSink::new(Vec::new(), &[b, t]).unwrap();
        feed_in_pieces(&mut sink, &with_delta, &[CHUNK]);
        let f = sink.finish().unwrap();
        check_output(&r, &f, &[(b, base), (t, top.clone())]);
        let from_delta = f.recs.iter().find(|x| x.oid == t).unwrap();

        let mut sink = BlobSink::new(Vec::new(), &[t]).unwrap();
        feed_in_pieces(&mut sink, &hand_pack(&[whole_entry(&top)]), &[CHUNK]);
        let g = sink.finish().unwrap();
        assert!(from_delta.stored && g.recs[0].stored);
        assert!(entry_bytes(&f.out, from_delta) == entry_bytes(&g.out, &g.recs[0]));
    }

    /// `zlib` with `blocks` empty stored blocks (5 bytes each, no content) after its
    /// header: still a valid stream of the same content.
    fn padded(zlib: &[u8], blocks: usize) -> Vec<u8> {
        let mut out = zlib[..2].to_vec();
        for _ in 0..blocks {
            out.extend_from_slice(&[0, 0, 0, 0xff, 0xff]);
        }
        out.extend_from_slice(&zlib[2..]);
        out
    }

    /// A zlib stream padded with empty blocks is valid, and nothing an encoder writes. The
    /// sink held a large blob's padding while the blob's encoding was undecided, and read a
    /// delta base's back whole: memory without a bound. A stream longer than twice its
    /// content plus 1 KiB now fails the pack in the write that passes the bound; under it,
    /// padding is accepted as before, and so are the longest streams git writes, at level
    /// 0 (11 bytes for an empty blob).
    #[test]
    fn padded_streams_fail_the_pack() {
        let r = repo("pad");
        let all: Vec<Oid> = r.blobs.iter().map(|b| b.0).collect();
        let list: String = all.iter().map(|o| format!("{o}\n")).collect();
        let level0 = git(
            &r.dir.join("r.git"),
            &["-c", "pack.compression=0", "pack-objects", "--stdout", "-q"],
            list.as_bytes(),
        );
        let mut sink = BlobSink::new(Vec::new(), &all).unwrap();
        feed_in_pieces(&mut sink, &level0, &[4096]);
        check_output(&r, &sink.finish().unwrap(), &r.blobs);

        let large = noise(us(STORED_MIN) + 5, 21);
        let small = b"a small blob\n".to_vec();
        let tail = b"and more\n";
        let top = [small.clone(), tail.to_vec()].concat();
        let (l, s, t) = (blob_id(&large), blob_id(&small), blob_id(&top));
        let d = copy_insert_delta(small.len(), tail);
        let pack = |blocks: usize, large_first: bool| {
            let small_entry = (
                HeaderBytes::whole(OBJ_BLOB, u64_of(small.len())),
                padded(&deflate(&small), if large_first { 0 } else { blocks }),
            );
            let large_entry = (
                HeaderBytes::whole(OBJ_BLOB, u64_of(large.len())),
                padded(&deflate(&large), if large_first { blocks } else { 0 }),
            );
            let delta = (HeaderBytes::ref_delta(u64_of(d.len()), &s), deflate(&d));
            if large_first {
                raw_pack(&[large_entry, small_entry, delta])
            } else {
                raw_pack(&[small_entry, delta, large_entry])
            }
        };
        // 1,000 bytes of padding are within the bound, before a large blob or a delta's base.
        for large_first in [true, false] {
            let mut sink = BlobSink::new(Vec::new(), &[l, s, t]).unwrap();
            feed_in_pieces(&mut sink, &pack(200, large_first), &[4096]);
            let want = [(l, large.clone()), (s, small.clone()), (t, top.clone())];
            check_output(&r, &sink.finish().unwrap(), &want);
        }
        // 1.5 MB of it are not: the first write of 4 KiB fails the pack.
        for large_first in [true, false] {
            let pack = pack(300_000, large_first);
            let mut sink = BlobSink::new(Vec::new(), &[l, s, t]).unwrap();
            let taken = pack
                .chunks(4096)
                .take_while(|piece| sink.write_all(piece).is_ok())
                .count();
            assert_eq!(taken, 0, "writes of 4 KiB accepted");
            let e = sink.end_input().unwrap_err().to_string();
            assert!(e.contains("more than any encoder needs"), "{e}");
        }
    }

    /// On a clock that only the throttle's own waits advance, at `--rate-mbps 4000`: the
    /// rate holds from the first byte; after a 15-minute pause (a 429 waited out) a quarter
    /// of a second's bytes pass at once, where the pause used to be 450 GB of credit; then
    /// the rate holds again.
    #[test]
    fn throttle_caps_the_rate_not_the_average_since_the_start() {
        use std::time::Duration;
        const RATE: u64 = 4000 * 1_000_000 / 8;
        const WRITE: usize = 64 << 10;
        let t = Throttle::new(RATE);
        // 10 GB in 64 KiB writes, each followed by the wait the throttle asks for.
        let ten_gb = |now: &mut Duration| {
            let began = *now;
            for _ in 0..10_000_000_000 / WRITE as u64 {
                *now += t.charge(WRITE, *now);
            }
            (*now - began).as_secs_f64()
        };
        let mut now = Duration::ZERO;
        let secs = ten_gb(&mut now);
        assert!((secs - 20.0).abs() < 0.001, "{secs}");

        now += Duration::from_secs(900);
        let mut free = 0u64;
        let wait = loop {
            let wait = t.charge(WRITE, now);
            if !wait.is_zero() {
                break wait;
            }
            free += WRITE as u64;
        };
        assert!(
            free <= RATE / 4 && free > RATE / 4 - 2 * WRITE as u64,
            "{free} bytes passed without a wait"
        );
        now += wait;
        let secs = ten_gb(&mut now);
        assert!((secs - 20.0).abs() < 0.001, "{secs}");
    }

    /// The real clock, and one bucket for all copies: after a pause of 0.4 s, four threads
    /// take 1.5 MiB between them at 4 MiB/s. The burst pays for 1 MiB; the rest takes
    /// 0.125 s. (As credit, the pause paid for 1.6 MiB: no wait at all.)
    #[test]
    fn throttled_copies_share_one_bucket() {
        use std::time::{Duration, Instant};
        let t = Throttle::new(4 << 20);
        std::thread::sleep(Duration::from_millis(400));
        let began = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| {
                    for _ in 0..24 {
                        t.account(16 << 10);
                    }
                });
            }
        });
        let took = began.elapsed();
        assert!(took >= Duration::from_millis(120), "{took:?}");
    }
}
