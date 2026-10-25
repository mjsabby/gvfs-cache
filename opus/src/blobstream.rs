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
//!   [`STORED_MIN`] whose first MiB compresses by less than 5%, as stored blocks (every
//!   content byte then has a fixed place in the pack, so ranges are served in place).
//! * **Deltas** (OFS or REF) are inflated in memory and applied to their base, read back
//!   from the output (the output must be a [`Spool`] that can do that, like a local file),
//!   and the result is written whole. A REF base may arrive after its delta: the delta
//!   waits for it. Requests for large blobs are made one blob each, so their packs hold
//!   one whole entry and need no read-back.
//! * The output declares as many entries as blobs were asked for; any other object, a
//!   blob twice, a blob missing, a size or trailer mismatch fails the pack.

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
/// Content of a large blob inflated before its encoding is chosen.
const SAMPLE: usize = 1 << 20;
/// Largest delta, base or delta result held in memory.
pub const RESOLVE_MAX: u64 = 1 << 30;
/// Most delta bytes waiting for a base that has not arrived.
const WAITING_MAX: u64 = 1 << 30;
/// zlib level of blobs rebuilt from deltas (git's default).
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
        let file = self
            .w
            .into_inner()
            .map_err(|e| crate::err!("writing blob pack: {e}"))?;
        file.sync_all().ctx("syncing blob pack")
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

/// A byte-rate cap shared by concurrent copies (`mirror --rate-mbps`).
pub struct Throttle {
    bytes_per_sec: u64,
    start: std::time::Instant,
    total: std::sync::atomic::AtomicU64,
}

impl Throttle {
    pub fn new(bytes_per_sec: u64) -> Throttle {
        Throttle {
            bytes_per_sec: bytes_per_sec.max(1),
            start: std::time::Instant::now(),
            total: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Counts `n` bytes and sleeps while the copies are ahead of the rate.
    pub fn account(&self, n: usize) {
        let total = self
            .total
            .fetch_add(u64_of(n), std::sync::atomic::Ordering::Relaxed)
            .saturating_add(u64_of(n));
        let due_ms = total
            .saturating_mul(1000)
            .checked_div(self.bytes_per_sec)
            .unwrap_or(0);
        let due = std::time::Duration::from_millis(due_ms);
        let elapsed = self.start.elapsed();
        if due > elapsed {
            std::thread::sleep(due.saturating_sub(elapsed));
        }
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
        let content = self.chunk.get(..made).ctx("inflate output")?;
        if self.stage == Stage::Whole {
            self.obj_sha.update(content);
            match self.choice {
                Choice::Raw => self.out.raw(input)?,
                Choice::Stored => self.out.content(content)?,
                Choice::Undecided => {
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

    /// Chooses a large blob's encoding from the sample and writes the sample out.
    fn decide(&mut self) -> Result<()> {
        let z = u64_of(self.sample_z.len());
        let c = u64_of(self.sample_c.len());
        if z.saturating_mul(100) >= c.saturating_mul(95) {
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
        let (_, size, _) = crate::delta::delta_sizes(delta).ctx("bad delta header")?;
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
        let n = compress_bound(self.target.len());
        self.zbuf.resize(n, 0);
        let zlen = self
            .deflater
            .deflate_parts(&[&self.target], &mut self.zbuf)?;
        if size >= STORED_MIN && u64_of(zlen).saturating_mul(100) >= size.saturating_mul(95) {
            self.out.begin(size, Encoding::Stored)?;
            self.out.content(&self.target)?;
        } else {
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
            if rec.stored {
                let mut ranges = Vec::new();
                for (from, len) in [
                    (0u64, 10u64),
                    (65_530, 20),
                    (rec.size - 3, 3),
                    (1, rec.size - 2),
                ] {
                    rec.stored_ranges(from, len, &mut ranges);
                    let mut got = Vec::new();
                    for (at, n) in &ranges {
                        got.extend_from_slice(&f.out[us(*at)..us(at + n)]);
                    }
                    assert_eq!(got, content[us(from)..us(from + len)]);
                }
            }
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
        let mut p = crate::pack::pack_header(u32::try_from(entries.len()).unwrap()).to_vec();
        for (h, body) in entries {
            p.extend_from_slice(h.as_slice());
            p.extend_from_slice(&deflate(body));
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
        assert!(err(feed(&all[1..], &pack)).contains("not asked for"));
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
}
