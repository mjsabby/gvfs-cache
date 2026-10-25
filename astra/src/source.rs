use crate::arena::{Arena, Mapping, hash_oid, mix, push};
use crate::idx::IdxView;
use crate::mphf::NO_BASE;
use crate::pack::{DeltaBase, EntryHeader, parse_entry_header};
use crate::zlib::Inflater;
use crate::{Error, ObjType, Oid20, Result};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

#[derive(Clone, Copy)]
pub struct SourceEntry {
    pub oid: Oid20,
    pub offset: u64,
    pub len: u32,
    pub crc: u32,
    pack: u32,
}

struct SourcePack {
    path: PathBuf,
    bytes: Mapping,
    start: usize,
    end: usize,
}

/// Admission is cold; the object table, decoder, DFS and splice loops are bounded.
pub struct Source {
    packs: Vec<SourcePack>,
    pub entries: Vec<SourceEntry>,
    table: Vec<u32>,
}

impl Source {
    pub fn new(max_objects: usize) -> Result<Self> {
        if max_objects == 0 || max_objects >= (1 << 31) {
            return Err(Error::Capacity("source object count"));
        }
        let slots = max_objects
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .ok_or(Error::Capacity("source OID table"))?;
        Ok(Self {
            packs: Vec::new(),
            entries: Vec::with_capacity(max_objects),
            table: vec![NO_BASE; slots],
        })
    }

    pub fn find(&self, oid: &Oid20) -> Result<Option<u32>> {
        let mask = self.table.len().saturating_sub(1);
        let mut slot = hash_oid(oid) as usize & mask;
        for _ in 0..self.table.len() {
            let ordinal = *self.table.get(slot).ok_or(Error::Invalid("source table"))?;
            if ordinal == NO_BASE {
                return Ok(None);
            }
            if self.entry(ordinal)?.oid == *oid {
                return Ok(Some(ordinal));
            }
            slot = slot.wrapping_add(1) & mask;
        }
        Err(Error::Capacity("source OID table"))
    }

    fn insert(&mut self, ordinal: u32) -> Result<()> {
        let oid = self.entry(ordinal)?.oid;
        let mask = self.table.len().saturating_sub(1);
        let mut slot = hash_oid(&oid) as usize & mask;
        for _ in 0..self.table.len() {
            let previous = *self.table.get(slot).ok_or(Error::Invalid("source table"))?;
            if previous == NO_BASE {
                *self
                    .table
                    .get_mut(slot)
                    .ok_or(Error::Invalid("source table"))? = ordinal;
                return Ok(());
            }
            if self.entry(previous)?.oid == oid {
                return Ok(());
            }
            slot = slot.wrapping_add(1) & mask;
        }
        Err(Error::Capacity("source OID table"))
    }

    pub fn add_directory(&mut self, dir: &Path) -> Result<()> {
        if !dir.exists() {
            return Ok(());
        }
        let mut paths = fs::read_dir(dir)?
            .map(|e| e.map(|e| e.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        paths.sort_unstable();
        for path in paths {
            if path.extension().is_some_and(|e| e == "pack") {
                self.add_pack(&path)?;
            }
        }
        Ok(())
    }

    pub fn add_pack(&mut self, path: &Path) -> Result<()> {
        if self.packs.iter().any(|p| p.path == path) {
            return Ok(());
        }
        let index_file = File::open(path.with_extension("idx"))?;
        let index_len = usize::try_from(index_file.metadata()?.len())
            .map_err(|_| Error::Capacity("Git index"))?;
        let index_map = Mapping::read(index_file, index_len)?;
        let index = IdxView::parse(index_map.bytes())?;
        if !index.verify_checksum() {
            return Err(Error::Invalid("Git index checksum"));
        }
        if self
            .entries
            .len()
            .checked_add(index.len())
            .is_none_or(|n| n > self.entries.capacity())
        {
            return Err(Error::Capacity("source objects; raise --max-objects"));
        }
        let file = File::open(path)?;
        let len =
            usize::try_from(file.metadata()?.len()).map_err(|_| Error::Capacity("Git pack"))?;
        let map = Mapping::read(file, len)?;
        let end = len.checked_sub(20).ok_or(Error::Invalid("pack trailer"))?;
        let hash = crate::sha1_parts(&[map.bytes().get(..end).ok_or(Error::Invalid("pack"))?]);
        if index.pack_checksum() != Some(hash) || map.bytes().get(end..) != Some(&hash) {
            return Err(Error::Invalid("Git pack checksum"));
        }
        if crate::pack::parse_pack_header(map.bytes())? as usize != index.len() {
            return Err(Error::Invalid("pack/index object count"));
        }
        let start = self.entries.len();
        let pack = u32::try_from(self.packs.len()).map_err(|_| Error::Capacity("source packs"))?;
        for i in 0..index.len() {
            push(
                &mut self.entries,
                SourceEntry {
                    oid: index.oid(i).ok_or(Error::Invalid("Git index OID"))?,
                    offset: index.offset(i).ok_or(Error::Invalid("Git index offset"))?,
                    crc: index.crc32(i).ok_or(Error::Invalid("Git index CRC"))?,
                    len: 0,
                    pack,
                },
            )?;
        }
        let rows = self
            .entries
            .get_mut(start..)
            .ok_or(Error::Invalid("source entries"))?;
        rows.sort_unstable_by_key(|r| r.offset);
        let mut next = end as u64;
        for row in rows.iter_mut().rev() {
            row.len = u32::try_from(
                next.checked_sub(row.offset)
                    .filter(|&n| n > 0)
                    .ok_or(Error::Invalid("Git pack offsets"))?,
            )
            .map_err(|_| Error::Capacity("compressed object"))?;
            next = row.offset;
        }
        if next != 12 && !index.is_empty() {
            return Err(Error::Invalid("first pack offset"));
        }
        let count = self.entries.len();
        self.packs.push(SourcePack {
            path: path.to_owned(),
            bytes: map,
            start,
            end: count,
        });
        for i in start..count {
            self.insert(i as u32)?;
        }
        Ok(())
    }

    pub fn entry(&self, ordinal: u32) -> Result<&SourceEntry> {
        self.entries
            .get(ordinal as usize)
            .ok_or(Error::Invalid("source ordinal"))
    }
    pub fn raw(&self, ordinal: u32) -> Result<&[u8]> {
        let entry = self.entry(ordinal)?;
        let pack = self
            .packs
            .get(entry.pack as usize)
            .ok_or(Error::Invalid("source pack"))?;
        let start = usize::try_from(entry.offset).map_err(|_| Error::Invalid("source offset"))?;
        pack.bytes
            .bytes()
            .get(start..start.saturating_add(entry.len as usize))
            .ok_or(Error::Invalid("source entry bounds"))
    }
    pub fn header(&self, ordinal: u32) -> Result<EntryHeader> {
        parse_entry_header(self.raw(ordinal)?)?.ok_or(Error::Invalid("truncated pack entry"))
    }
    pub fn base(&self, ordinal: u32, header: &EntryHeader) -> Result<Option<u32>> {
        match header.base {
            DeltaBase::None => Ok(None),
            DeltaBase::Ref(oid) => Ok(Some(self.find(&oid)?.ok_or(Error::Missing(oid))?)),
            DeltaBase::Ofs(distance) => {
                let row = self.entry(ordinal)?;
                let offset = row
                    .offset
                    .checked_sub(distance)
                    .ok_or(Error::Invalid("delta distance"))?;
                let pack = self
                    .packs
                    .get(row.pack as usize)
                    .ok_or(Error::Invalid("source pack"))?;
                let rows = self
                    .entries
                    .get(pack.start..pack.end)
                    .ok_or(Error::Invalid("source rows"))?;
                let i = rows
                    .binary_search_by_key(&offset, |r| r.offset)
                    .map_err(|_| Error::Invalid("OFS base not an entry"))?;
                Ok(Some(pack.start.saturating_add(i) as u32))
            }
        }
    }
}

const CACHE_SLOT_BYTES: usize = 64 << 10;

pub struct Decoder {
    a: Arena,
    b: Arena,
    delta: Arena,
    inflate: Inflater,
    cache: Arena,
    keys: Vec<u32>,
    lengths: Vec<u32>,
    kinds: Vec<ObjType>,
    pub hits: u64,
    pub misses: u64,
}

impl Decoder {
    /// Ordinal cache keys belong to one Source. Reuse buffers, not cached objects,
    /// when a client admits a different independently indexed pack.
    pub fn clear_cache(&mut self) {
        self.keys.fill(NO_BASE);
    }

    pub fn new(max_object: usize, cache_bytes: usize) -> Result<Self> {
        let slots = cache_bytes.wrapping_div(CACHE_SLOT_BYTES).max(1);
        Ok(Self {
            a: Arena::new(max_object)?,
            b: Arena::new(max_object)?,
            delta: Arena::new(max_object)?,
            inflate: Inflater::new(),
            cache: Arena::new(slots.saturating_mul(CACHE_SLOT_BYTES))?,
            keys: vec![NO_BASE; slots],
            lengths: vec![0; slots],
            kinds: vec![ObjType::Blob; slots],
            hits: 0,
            misses: 0,
        })
    }
    fn cache_slot(&self, key: u32) -> Result<usize> {
        Ok(mix(u64::from(key))
            .checked_rem(self.keys.len() as u64)
            .ok_or(Error::Invalid("zero cache slots"))? as usize)
    }
    fn cache_read(&mut self, key: u32) -> Result<Option<ObjType>> {
        let slot = self.cache_slot(key)?;
        if self.keys.get(slot) != Some(&key) {
            return Ok(None);
        }
        let len = *self.lengths.get(slot).ok_or(Error::Invalid("cache slot"))? as usize;
        let start = slot.saturating_mul(CACHE_SLOT_BYTES);
        let bytes = self
            .cache
            .buffer()
            .get(start..start.saturating_add(len))
            .ok_or(Error::Invalid("cache bounds"))?;
        self.a
            .buffer()
            .get_mut(..len)
            .ok_or(Error::Capacity("object"))?
            .copy_from_slice(bytes);
        self.a.set_len(len)?;
        self.hits = self.hits.saturating_add(1);
        Ok(self.kinds.get(slot).copied())
    }
    fn cache_write(&mut self, key: u32, kind: ObjType) -> Result<()> {
        if self.a.len() > CACHE_SLOT_BYTES {
            return Ok(());
        }
        let slot = self.cache_slot(key)?;
        let start = slot.saturating_mul(CACHE_SLOT_BYTES);
        self.cache
            .buffer()
            .get_mut(start..start.saturating_add(self.a.len()))
            .ok_or(Error::Invalid("cache bounds"))?
            .copy_from_slice(self.a.bytes());
        *self
            .keys
            .get_mut(slot)
            .ok_or(Error::Invalid("cache slot"))? = key;
        *self
            .lengths
            .get_mut(slot)
            .ok_or(Error::Invalid("cache slot"))? = self.a.len() as u32;
        *self
            .kinds
            .get_mut(slot)
            .ok_or(Error::Invalid("cache slot"))? = kind;
        Ok(())
    }
    pub fn decode<'a>(&'a mut self, source: &Source, ordinal: u32) -> Result<(ObjType, &'a [u8])> {
        let mut chain = [0u32; 128];
        let mut depth = 0usize;
        let mut current = ordinal;
        let kind;
        loop {
            if let Some(cached) = self.cache_read(current)? {
                kind = cached;
                break;
            }
            self.misses = self.misses.saturating_add(1);
            let header = source.header(current)?;
            if let Some(base) = source.base(current, &header)? {
                *chain
                    .get_mut(depth)
                    .ok_or(Error::Invalid("delta depth exceeds 128 or cycle"))? = current;
                depth = depth.saturating_add(1);
                current = base;
            } else {
                kind = header.kind;
                let size = usize::try_from(header.size).map_err(|_| Error::Capacity("object"))?;
                let raw = source
                    .raw(current)?
                    .get(header.header_len..)
                    .ok_or(Error::Invalid("zlib offset"))?;
                if self.inflate.inflate_exact(raw, self.a.buffer(), size)? != raw.len() {
                    return Err(Error::Invalid("trailing zlib data"));
                }
                self.a.set_len(size)?;
                self.cache_write(current, kind)?;
                break;
            }
        }
        while depth != 0 {
            depth = depth.saturating_sub(1);
            current = *chain.get(depth).ok_or(Error::Invalid("delta stack"))?;
            let header = source.header(current)?;
            let raw = source
                .raw(current)?
                .get(header.header_len..)
                .ok_or(Error::Invalid("zlib offset"))?;
            let size = usize::try_from(header.size).map_err(|_| Error::Capacity("delta"))?;
            if self.inflate.inflate_exact(raw, self.delta.buffer(), size)? != raw.len() {
                return Err(Error::Invalid("trailing delta data"));
            }
            self.delta.set_len(size)?;
            let len =
                crate::delta::apply_delta(self.a.bytes(), self.delta.bytes(), self.b.buffer())?;
            self.b.set_len(len)?;
            std::mem::swap(&mut self.a, &mut self.b);
            self.cache_write(current, kind)?;
        }
        Ok((kind, self.a.bytes()))
    }
}

pub(crate) fn command() -> Command {
    let mut c = Command::new("git");
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("GIT_CONFIG_") {
            c.env_remove(key);
        }
    }
    c.env("GIT_TERMINAL_PROMPT", "0");
    c.args(["-c", "gc.auto=0", "-c", "maintenance.auto=false"]);
    c
}

fn checked_output(command: &mut Command) -> Result<Vec<u8>> {
    let output = command.stderr(Stdio::inherit()).output()?;
    if !output.status.success() {
        return Err(Error::Git(output.status.code().unwrap_or(-1)));
    }
    Ok(output.stdout)
}

pub struct GitUpstream {
    git_dir: PathBuf,
    upstream: String,
    branch: String,
    work: PathBuf,
    local: bool,
    auth_header: Option<String>,
}

impl GitUpstream {
    pub fn open(
        upstream: &str,
        branch: &str,
        work: &Path,
        auth_header: Option<String>,
    ) -> Result<Self> {
        fs::create_dir_all(work)?;
        let local = Path::new(upstream).is_dir();
        let git_dir = if local {
            let raw = checked_output(
                command()
                    .arg("-C")
                    .arg(upstream)
                    .args(["rev-parse", "--absolute-git-dir"]),
            )?;
            PathBuf::from(
                std::str::from_utf8(&raw)
                    .map_err(|_| Error::Invalid("Git directory encoding"))?
                    .trim(),
            )
        } else {
            let dir = work.join("upstream.git");
            if !dir.exists() {
                checked_output(command().args(["init", "--bare", "--quiet"]).arg(&dir))?;
            }
            dir
        };
        Ok(Self {
            git_dir,
            upstream: upstream.to_owned(),
            branch: branch.to_owned(),
            work: work.to_owned(),
            local,
            auth_header,
        })
    }

    fn git(&self) -> Command {
        let mut git = command();
        git.arg("--git-dir").arg(&self.git_dir);
        if let Some(header) = &self.auth_header {
            git.env("GIT_CONFIG_COUNT", "1")
                .env("GIT_CONFIG_KEY_0", "http.extraHeader")
                .env("GIT_CONFIG_VALUE_0", header);
        }
        git
    }

    pub fn tip(&self) -> Result<Oid20> {
        if !self.local {
            checked_output(
                self.git()
                    .args([
                        "-c",
                        "fetch.unpackLimit=0",
                        "-c",
                        "transfer.unpackLimit=0",
                        "fetch",
                        "--quiet",
                        "--no-tags",
                        "--no-auto-maintenance",
                        "--keep",
                        "--force",
                        "--",
                    ])
                    .arg(&self.upstream)
                    .arg(format!("{}:refs/gvfs/upstream", self.branch)),
            )?;
        }
        let reference = if self.local {
            self.branch.as_str()
        } else {
            "refs/gvfs/upstream"
        };
        let rev = format!("{reference}^{{commit}}");
        let raw = checked_output(
            self.git()
                .args(["rev-parse", "--verify", "--end-of-options"])
                .arg(rev),
        )?;
        Oid20::from_hex(raw.strip_suffix(b"\n").unwrap_or(&raw))
            .ok_or(Error::Invalid("upstream tip"))
    }

    pub fn prepare(&self, source: &mut Source, tip: Oid20, have: Oid20) -> Result<()> {
        source.add_directory(&self.git_dir.join("objects/pack"))?;
        if source.find(&tip)?.is_some() {
            return Ok(());
        }
        // Loose local commits are transported through native Git just like a remote fetch.
        // Never write to, repack, or change refs in the user's repository.
        let pack = self.work.join(format!("incoming-{tip}.pack"));
        if !pack.with_extension("idx").exists() {
            let output = File::create(&pack)?;
            let mut child = self
                .git()
                .args(["pack-objects", "--stdout", "--revs", "--delta-base-offset"])
                .stdin(Stdio::piped())
                .stdout(output)
                .stderr(Stdio::inherit())
                .spawn()?;
            let mut input = child.stdin.take().ok_or(Error::Invalid("Git stdin"))?;
            writeln!(input, "{tip}")?;
            // A newly published tip can be in Git/storage without having passed
            // through this edge's decoder. Git, not the decoder cache, knows haves.
            if have != Oid20([0; 20]) {
                let exists = self
                    .git()
                    .args(["cat-file", "-e"])
                    .arg(format!("{have}^{{commit}}"))
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()?;
                if exists.success() {
                    writeln!(input, "^{have}")?;
                }
            }
            drop(input);
            let status = child.wait()?;
            if !status.success() {
                return Err(Error::Git(status.code().unwrap_or(-1)));
            }
            checked_output(
                command()
                    .args(["index-pack", "--index-version=2"])
                    .arg(&pack),
            )?;
        }
        source.add_pack(&pack)?;
        Ok(())
    }
}
