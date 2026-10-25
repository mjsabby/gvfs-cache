//! Managed client cache: a pinned baseline and one locally spliced delta pack.

mod state;
#[cfg(test)]
mod tests;

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use crate::arena::Mapping;
use crate::idx::IdxView;
use crate::manifest::{SlabRef, Tier};
use crate::mphf::{Index, NO_BASE, Record};
use crate::pack::{DeltaBase, parse_pack_header};
use crate::slab::{Artifact, Slab};
use crate::source::{Decoder, Source};
use crate::splice::{Plan, seal_plan};
use crate::storage::RangeReader;
use crate::{Error, Oid20, Result};
use fs2::FileExt;
use sha1::{Digest, Sha1};
use state::{PackFile, Role, State};

#[derive(Clone)]
pub struct Config {
    pub server: String,
    pub cache_dir: PathBuf,
    pub authorization: Option<String>,
    pub max_objects: usize,
    pub max_object_bytes: usize,
    pub max_download_bytes: u64,
}

impl Config {
    pub fn new(server: String, cache_dir: PathBuf) -> Self {
        Self {
            server,
            cache_dir,
            authorization: None,
            max_objects: 16777216,
            max_object_bytes: 32 << 20,
            max_download_bytes: 64 << 30,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ResponseMeta {
    pub etag: Oid20,
    pub start: Option<u64>,
    pub end: Option<u64>,
    pub baseline: bool,
    pub tip: Option<Oid20>,
}

impl ResponseMeta {
    pub fn from_headers(headers: &ureq::http::HeaderMap) -> Result<Self> {
        let tag = headers
            .get("etag")
            .ok_or(Error::Invalid("prefetch response has no ETag"))?
            .as_bytes();
        let etag = Oid20::from_hex(
            tag.strip_prefix(b"\"")
                .and_then(|s| s.strip_suffix(b"\""))
                .ok_or(Error::Invalid("prefetch ETag"))?,
        )
        .ok_or(Error::Invalid("prefetch ETag"))?;
        let tip = headers
            .get("x-gvfs-tip")
            .map(|value| Oid20::from_hex(value.as_bytes()).ok_or(Error::Invalid("prefetch tip")))
            .transpose()?;
        let number = |key| -> Result<Option<u64>> {
            headers
                .get(key)
                .map(|value| {
                    crate::buf::parse_dec_u64(value.as_bytes())
                        .ok_or(Error::Invalid("prefetch coverage"))
                })
                .transpose()
        };
        let start = number("x-gvfs-pack-start")?;
        let end = number("x-gvfs-pack-end")?;
        let mode = headers.get("x-gvfs-pack-mode").map(|v| v.as_bytes());
        let baseline = match (start, end, mode) {
            (None, None, None) => false,
            (Some(_), Some(_), Some(b"baseline")) => true,
            (Some(_), Some(_), Some(b"incremental")) => false,
            _ => return Err(Error::Invalid("incomplete prefetch coverage")),
        };
        Ok(Self {
            etag,
            start,
            end,
            baseline,
            tip,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SyncReport {
    pub not_modified: bool,
    pub downloaded_bytes: u64,
    pub local_pack_bytes_written: u64,
    pub managed_packs: usize,
    pub cursor: u64,
}

pub struct Cache {
    config: Config,
    _lock: File,
    state: State,
    anchor: Option<Mapping>,
    loaded_anchor: Option<PackFile>,
    delta: Option<Arc<Slab>>,
    plan: Plan,
    buffer: Vec<u8>,
    decoder: Decoder,
    agent: ureq::Agent,
    #[cfg(test)]
    failure: Option<Failure>,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum Failure {
    Journal,
    Pack,
    State,
    Retire,
}

impl Cache {
    pub fn open(mut config: Config) -> Result<Self> {
        config.server = config.server.trim_end_matches('/').to_owned();
        let uri: ureq::http::Uri = config
            .server
            .parse()
            .map_err(|_| Error::Invalid("cache server URL"))?;
        if !matches!(uri.scheme_str(), Some("http" | "https"))
            || uri.authority().is_none_or(|a| a.as_str().contains('@'))
            || uri.query().is_some()
            || config.server.contains('#')
            || config.server.len() > 4096
        {
            return Err(Error::Invalid(
                "use an HTTP(S) server URL without credentials/query; use --auth-env",
            ));
        }
        if config.max_objects == 0
            || config.max_objects >= (1 << 31)
            || config.max_object_bytes == 0
            || config.max_object_bytes >= (1 << 29)
            || config.max_download_bytes < 32
        {
            return Err(Error::Invalid("client cache limits"));
        }
        fs::create_dir_all(&config.cache_dir)?;
        config.cache_dir = config.cache_dir.canonicalize()?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(config.cache_dir.join(".gvfs-sync.lock"))?;
        match lock.try_lock_exclusive() {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                return Err(Error::Invalid("cache is already being synchronized"));
            }
            Err(e) => return Err(e.into()),
        }
        if !config.cache_dir.join("HEAD").is_file() || !config.cache_dir.join("objects").is_dir() {
            for entry in fs::read_dir(&config.cache_dir)? {
                if entry?.file_name() != ".gvfs-sync.lock" {
                    return Err(Error::Invalid("cache must be empty or a Git directory"));
                }
            }
            checked(
                git_command(&config.cache_dir)
                    .args(["init", "--bare", "--quiet"])
                    .arg(&config.cache_dir),
            )?;
            checked(git_command(&config.cache_dir).args(["config", "gc.auto", "0"]))?;
            checked(git_command(&config.cache_dir).args(["config", "maintenance.auto", "false"]))?;
            checked(git_command(&config.cache_dir).args([
                "symbolic-ref",
                "HEAD",
                "refs/heads/gvfs-cache",
            ]))?;
        }
        let metadata = config.cache_dir.join("gvfs-daemon");
        fs::create_dir_all(metadata.join("stage"))?;
        fs::create_dir_all(metadata.join("work"))?;
        let state_path = metadata.join("state.bin");
        let current = if let Some(bytes) = state::read(&state_path)? {
            State::decode(&bytes)?
        } else {
            if metadata.join("transaction.bin").exists() {
                return Err(Error::Invalid("transaction without cache state"));
            }
            for entry in fs::read_dir(config.cache_dir.join("objects/pack"))? {
                let name = entry?.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("gvfs-base-") || name.starts_with("gvfs-delta-") {
                    return Err(Error::Invalid(
                        "untracked files use the reserved managed-cache prefix",
                    ));
                }
            }
            if read_tip(&config.cache_dir)?.is_some() {
                return Err(Error::Invalid(
                    "reserved gvfs-cache ref exists without client state",
                ));
            }
            let state = State::new(config.server.clone());
            state::save(&state_path, &state.encode()?)?;
            state
        };
        if current.server != config.server {
            return Err(Error::Invalid("cache is bound to a different server URL"));
        }
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .max_redirects(0)
            .timeout_global(Some(Duration::from_secs(300)))
            .build()
            .into();
        let mut cache = Self {
            plan: Plan::new(config.max_objects, config.max_objects),
            decoder: Decoder::new(config.max_object_bytes, 16 << 20)?,
            config,
            _lock: lock,
            state: current,
            anchor: None,
            loaded_anchor: None,
            delta: None,
            buffer: vec![0; 1 << 20],
            agent,
            #[cfg(test)]
            failure: None,
        };
        cache.recover()?;
        cache.load_current()?;
        cache.retire()?;
        cache.clear_stage()?;
        Ok(cache)
    }

    pub fn cursor(&self) -> u64 {
        self.state.cursor
    }
    pub fn etag(&self) -> Option<Oid20> {
        self.state.etag
    }
    pub fn tip(&self) -> Option<Oid20> {
        self.state.tip
    }
    pub fn managed_pack_count(&self) -> usize {
        usize::from(self.state.anchor.is_some())
            .saturating_add(usize::from(self.state.delta.is_some()))
    }
    pub fn cache_dir(&self) -> &Path {
        &self.config.cache_dir
    }
    pub fn delta_bytes(&self) -> u64 {
        self.state.delta.map_or(0, |p| p.pack_len)
    }

    pub fn sync_once(&mut self) -> Result<SyncReport> {
        self.settle()?;
        let url = format!(
            "{}/gvfs/prefetch?lastPackTimestamp={}",
            self.config.server, self.state.cursor
        );
        let mut request = ureq::http::Request::builder().method("GET").uri(url);
        if let Some(tag) = self.state.etag {
            request = request.header("If-None-Match", format!("\"{tag}\""));
        }
        if let Some(auth) = &self.config.authorization {
            request = request.header("Authorization", auth);
        }
        let mut response = self.agent.run(
            request
                .body(())
                .map_err(|_| Error::Invalid("prefetch request"))?,
        )?;
        let status = response.status().as_u16();
        if status != 200 && status != 304 {
            return Err(Error::Http(status));
        }
        if status == 200
            && response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .is_none_or(|s| s.split(';').next() != Some(crate::gpre::CONTENT_TYPE_PREFETCH))
        {
            return Err(Error::Invalid("unexpected prefetch content type"));
        }
        let meta = ResponseMeta::from_headers(response.headers())?;
        self.apply_response(status, meta, &mut response.body_mut().as_reader())
    }

    /// Shared by HTTP sync and the deterministic replay. No file is retired based on timestamps.
    pub fn apply_response(
        &mut self,
        status: u16,
        meta: ResponseMeta,
        input: &mut impl Read,
    ) -> Result<SyncReport> {
        self.settle()?;
        if status == 304 {
            if self.state.anchor.is_none() || self.state.etag != Some(meta.etag) {
                return Err(Error::Invalid("unsolicited or mismatched 304"));
            }
            return Ok(self.report(true, 0, 0));
        }
        if status != 200 {
            return Err(Error::Http(status));
        }
        self.clear_stage()?;
        let mut read = CountingRead { input, bytes: 0 };
        let mut header = [0; 8];
        read.read_exact(&mut header)?;
        let count =
            crate::gpre::parse_gpre_file_header(&header).ok_or(Error::Invalid("GPRE header"))?;
        if count == 0 {
            if self.state.anchor.is_none() {
                return Err(Error::Invalid("empty initial prefetch"));
            }
            eof(&mut read)?;
            if self.state.etag != Some(meta.etag)
                || (meta.tip.is_some() && meta.tip != self.state.tip)
            {
                let tip = meta.tip.or(self.state.tip);
                update_tip(&self.config.cache_dir, self.state.tip, tip)?;
                self.state.etag = Some(meta.etag);
                self.state.tip = tip;
                self.save_state()?;
            }
            return Ok(self.report(false, read.bytes, 0));
        }
        if count != 1 {
            return Err(Error::Invalid("expected exactly one prefetch pack"));
        }
        let mut header = [0; 24];
        read.read_exact(&mut header)?;
        let pack = crate::gpre::parse_gpre_pack_header(&header)
            .ok_or(Error::Invalid("GPRE pack header"))?;
        let cursor =
            u64::try_from(pack.timestamp).map_err(|_| Error::Invalid("prefetch timestamp"))?;
        if meta.start != Some(self.state.cursor)
            || meta.end != Some(cursor)
            || cursor < self.state.cursor
            || meta.baseline != self.state.anchor.is_none()
        {
            return Err(Error::Invalid("prefetch cursor/coverage mismatch"));
        }
        let pack_len = u64::try_from(pack.pack_len).map_err(|_| Error::Invalid("pack size"))?;
        let idx_len = pack.idx_len.map(|n| n as u64).unwrap_or(0);
        if pack_len < 32 || pack_len.saturating_add(idx_len) > self.config.max_download_bytes {
            return Err(Error::Capacity("prefetch download"));
        }
        let incoming = self.stage("incoming.pack");
        let mut file = File::create(&incoming)?;
        let hash = copy_verified_pack(
            &mut read,
            &mut file,
            pack_len,
            self.config.max_objects,
            &mut self.buffer,
        )?;
        file.sync_all()?;
        discard(&mut read, idx_len, &mut self.buffer)?;
        eof(&mut read)?;
        let downloaded = read.bytes;
        let idx = self.stage("incoming.idx");
        checked(
            git_command(&self.config.cache_dir)
                .args(["index-pack", "--index-version=2", "-o"])
                .arg(&idx)
                .arg(&incoming),
        )?;
        let mapping = map_file(
            &idx,
            self.config
                .max_objects
                .saturating_mul(36)
                .saturating_add(1072) as u64,
        )?;
        let native = IdxView::parse(mapping.bytes())?;
        if !native.verify_checksum()
            || native.pack_checksum() != Some(hash.0)
            || native.len() > self.config.max_objects
        {
            return Err(Error::Invalid("downloaded pack/index mismatch"));
        }
        let mut next = self.state.clone();
        next.sequence = next
            .sequence
            .checked_add(1)
            .ok_or(Error::Capacity("client sequence"))?;
        next.cursor = cursor;
        if let Some(tip) = meta.tip {
            next.tip = Some(tip);
        }
        // A validator is scoped to its requested cursor, not the next request's URI.
        next.etag = if cursor == self.state.cursor {
            Some(meta.etag)
        } else {
            None
        };
        let received = PackFile {
            hash,
            idx_hash: trailing_hash(mapping.bytes())?,
            metadata_hash: Oid20([0; 20]),
            count: native.len() as u32,
            pack_len,
            idx_len: mapping.bytes().len() as u64,
        };
        if self.state.anchor.is_none() {
            if native.is_empty() {
                return Err(Error::Invalid("empty baseline"));
            }
            drop(mapping);
            fs::rename(&incoming, self.stage("next.pack"))?;
            fs::rename(&idx, self.stage("next.idx"))?;
            next.anchor = Some(received);
            self.commit(Role::Anchor, next)?;
            return Ok(self.report(false, downloaded, pack_len));
        }
        let anchor = IdxView::parse(
            self.anchor
                .as_ref()
                .ok_or(Error::Invalid("anchor index"))?
                .bytes(),
        )?;
        let mut missing = false;
        for i in 0..native.len() {
            let oid = native.oid(i).ok_or(Error::Invalid("incoming OID"))?;
            if anchor.find(&oid).is_none()
                && self
                    .delta
                    .as_ref()
                    .map(|d| d.index.find(&oid))
                    .transpose()?
                    .flatten()
                    .is_none()
            {
                missing = true;
                break;
            }
        }
        if !missing {
            update_tip(&self.config.cache_dir, self.state.tip, next.tip)?;
            self.state = next;
            self.save_state()?;
            self.clear_stage()?;
            return Ok(self.report(false, downloaded, 0));
        }
        drop(mapping);
        let slab = self.admit(received, cursor)?;
        let artifact = self.compact(&slab, cursor)?;
        // Native Git verifies the locally assembled representation before the old file can disappear.
        checked(
            git_command(&self.config.cache_dir)
                .args(["index-pack", "--verify"])
                .arg(artifact.dir.path().join("slab.pack")),
        )?;
        let candidate = Index::open(&artifact.dir.path().join("slab.mphf"))?;
        self.check_coverage(&slab, &candidate)?;
        let idx_bytes = map_file(
            &artifact.dir.path().join("slab.idx"),
            self.config
                .max_objects
                .saturating_mul(36)
                .saturating_add(1072) as u64,
        )?;
        let replacement = PackFile {
            hash: artifact.meta.hash,
            idx_hash: trailing_hash(idx_bytes.bytes())?,
            metadata_hash: artifact.meta.index_hash,
            count: artifact.meta.count,
            pack_len: artifact.meta.pack_len,
            idx_len: artifact.meta.idx_len,
        };
        drop(idx_bytes);
        drop(candidate);
        drop(slab);
        self.plan.reset()?;
        for extension in ["pack", "idx", "mphf"] {
            fs::rename(
                artifact.dir.path().join(format!("slab.{extension}")),
                self.stage(&format!("next.{extension}")),
            )?;
        }
        next.retired = next.delta;
        next.delta = Some(replacement);
        self.commit(Role::Delta, next)?;
        Ok(self.report(false, downloaded, replacement.pack_len))
    }

    fn admit(&mut self, received: PackFile, cursor: u64) -> Result<Arc<Slab>> {
        self.decoder.clear_cache();
        let mut source = Source::new((received.count as usize).max(1))?;
        source.add_pack(&self.stage("incoming.pack"))?;
        let mut rows = Vec::with_capacity(received.count as usize);
        let mut overflow = Vec::with_capacity(received.count as usize);
        {
            let _hot = alloc_guard::hot_path();
            for (i, entry) in source.entries.iter().enumerate() {
                let header = source.header(i as u32)?;
                if matches!(header.base, DeltaBase::Ofs(_)) {
                    return Err(Error::Invalid(
                        "client compaction requires REF_DELTA/standalone server entries",
                    ));
                }
                let base = source.base(i as u32, &header)?.unwrap_or(NO_BASE);
                let (kind, bytes) = self.decoder.decode(&source, i as u32)?;
                if crate::object_id(kind, bytes) != entry.oid {
                    return Err(Error::Invalid("incoming object identity"));
                }
                let mut row = Record::new(entry.oid, kind, bytes.len(), cursor, base)?;
                row.crc = entry.crc;
                row.locate(entry.offset, entry.len, &mut overflow)?;
                crate::arena::push(&mut rows, row)?;
            }
        }
        let path = self.stage("incoming.mphf");
        crate::mphf::write_index(&path, &rows, &overflow, received.pack_len, received.hash)?;
        let index = Index::open(&path)?;
        Ok(Arc::new(Slab {
            meta: SlabRef {
                hash: received.hash,
                index_hash: index.checksum,
                tip: Oid20([0; 20]),
                tier: Tier::Live,
                start: 0,
                end: cursor,
                count: received.count,
                pack_len: received.pack_len,
                idx_len: received.idx_len,
            },
            index,
            pack: RangeReader::File(File::open(self.stage("incoming.pack"))?),
            idx: RangeReader::File(File::open(self.stage("incoming.idx"))?),
        }))
    }

    fn compact(&mut self, incoming: &Arc<Slab>, cursor: u64) -> Result<Artifact> {
        self.plan.reset()?;
        if let Some(old) = &self.delta {
            self.plan.add(old)?;
        }
        self.plan.add(incoming)?;
        let anchor = IdxView::parse(
            self.anchor
                .as_ref()
                .ok_or(Error::Invalid("anchor"))?
                .bytes(),
        )?;
        {
            let _hot = alloc_guard::hot_path();
            for source in 0..self.plan.slabs.len() {
                let n = self
                    .plan
                    .slabs
                    .get(source)
                    .ok_or(Error::Invalid("client slab"))?
                    .index
                    .len();
                for i in 0..n {
                    let oid = self
                        .plan
                        .slabs
                        .get(source)
                        .ok_or(Error::Invalid("client slab"))?
                        .index
                        .record(i as u32)?
                        .oid;
                    if anchor.find(&oid).is_none() {
                        self.plan.select(source, i as u32)?;
                    }
                }
            }
            self.plan.finish()?;
        }
        if self.plan.count == 0 {
            return Err(Error::Invalid(
                "missing objects disappeared during compaction",
            ));
        }
        let work = self.metadata().join("work");
        seal_plan(
            &self.plan,
            Tier::Live,
            0,
            cursor,
            Oid20([0; 20]),
            &work,
            &mut self.buffer,
        )
    }

    fn check_coverage(&self, incoming: &Slab, replacement: &Index) -> Result<()> {
        let anchor = IdxView::parse(
            self.anchor
                .as_ref()
                .ok_or(Error::Invalid("anchor"))?
                .bytes(),
        )?;
        for slab in self
            .delta
            .iter()
            .map(|s| s.as_ref())
            .chain(std::iter::once(incoming))
        {
            for i in 0..slab.index.len() {
                let oid = slab.index.record(i as u32)?.oid;
                if anchor.find(&oid).is_none() && replacement.find(&oid)?.is_none() {
                    return Err(Error::Invalid(
                        "replacement does not cover an existing/downloaded object",
                    ));
                }
            }
        }
        Ok(())
    }

    fn commit(&mut self, role: Role, next: State) -> Result<()> {
        let candidate = match role {
            Role::Anchor => next.anchor,
            Role::Delta => next.delta,
        }
        .ok_or(Error::Invalid("replacement metadata"))?;
        let tracked = match role {
            Role::Anchor => self.state.anchor,
            Role::Delta => self.state.delta,
        };
        if tracked != Some(candidate) {
            for extension in ["pack", "idx", "keep", "mphf"] {
                if self.path(role, candidate, extension).exists() {
                    return Err(Error::Invalid(
                        "refusing to adopt an untracked managed-name artifact",
                    ));
                }
            }
        }
        File::open(self.metadata().join("stage"))?.sync_all()?;
        state::save(
            &self.metadata().join("transaction.bin"),
            &state::transaction(role, &next)?,
        )?;
        #[cfg(test)]
        self.fail(Failure::Journal)?;
        self.recover()?;
        self.load_current()?;
        self.retire()?;
        self.clear_stage()
    }

    fn recover(&mut self) -> Result<()> {
        let path = self.metadata().join("transaction.bin");
        let Some(bytes) = state::read(&path)? else {
            return Ok(());
        };
        let (role, next) = state::parse_transaction(&bytes)?;
        if next.server != self.config.server
            || (next.sequence != self.state.sequence
                && Some(next.sequence) != self.state.sequence.checked_add(1))
        {
            return Err(Error::Invalid("client transaction does not follow state"));
        }
        if next.sequence != self.state.sequence {
            match role {
                Role::Anchor
                    if self.state.anchor.is_none()
                        && next.delta.is_none()
                        && next.retired.is_none() => {}
                Role::Delta
                    if next.anchor == self.state.anchor && next.retired == self.state.delta => {}
                _ => {
                    return Err(Error::Invalid(
                        "transaction changes ownership of the baseline/predecessor",
                    ));
                }
            }
        }
        let pack = match role {
            Role::Anchor => next.anchor,
            Role::Delta => next.delta,
        }
        .ok_or(Error::Invalid("transaction pack"))?;
        for extension in ["pack", "idx"] {
            install(
                &self.stage(&format!("next.{extension}")),
                &self.path(role, pack, extension),
            )?;
            #[cfg(test)]
            if extension == "pack" {
                self.fail(Failure::Pack)?;
            }
        }
        if role == Role::Delta {
            install(&self.stage("next.mphf"), &self.path(role, pack, "mphf"))?;
        }
        verify_pair(
            &self.path(role, pack, "pack"),
            &self.path(role, pack, "idx"),
            pack,
            &mut self.buffer,
        )?;
        if role == Role::Delta {
            let index = Index::open(&self.path(role, pack, "mphf"))?;
            if index.checksum != pack.metadata_hash || index.pack_hash != pack.hash {
                return Err(Error::Invalid("client metadata hash"));
            }
        }
        let keep = self.path(role, pack, "keep");
        if !keep.exists() {
            let mut file = OpenOptions::new().write(true).create_new(true).open(keep)?;
            file.write_all(b"gvfs-daemon managed cache\n")?;
            file.sync_all()?;
        }
        File::open(self.config.cache_dir.join("objects/pack"))?.sync_all()?;
        File::open(self.metadata())?.sync_all()?;
        update_tip(&self.config.cache_dir, self.state.tip, next.tip)?;
        state::save(&self.metadata().join("state.bin"), &next.encode()?)?;
        self.state = next;
        #[cfg(test)]
        self.fail(Failure::State)?;
        remove(&path)?;
        File::open(self.metadata())?.sync_all()?;
        Ok(())
    }

    fn load_current(&mut self) -> Result<()> {
        self.plan.reset()?;
        self.delta = None;
        if let Some(anchor) = self.state.anchor {
            self.check_limits(anchor)?;
            if self.loaded_anchor != Some(anchor) {
                verify_pair(
                    &self.path(Role::Anchor, anchor, "pack"),
                    &self.path(Role::Anchor, anchor, "idx"),
                    anchor,
                    &mut self.buffer,
                )?;
                self.anchor = Some(map_file(
                    &self.path(Role::Anchor, anchor, "idx"),
                    self.config
                        .max_objects
                        .saturating_mul(36)
                        .saturating_add(1072) as u64,
                )?);
                self.loaded_anchor = Some(anchor);
            }
        }
        if let Some(delta) = self.state.delta {
            self.check_limits(delta)?;
            verify_pair(
                &self.path(Role::Delta, delta, "pack"),
                &self.path(Role::Delta, delta, "idx"),
                delta,
                &mut self.buffer,
            )?;
            let index = Index::open(&self.path(Role::Delta, delta, "mphf"))?;
            if index.checksum != delta.metadata_hash
                || index.pack_hash != delta.hash
                || index.len() != delta.count as usize
            {
                return Err(Error::Invalid("client delta metadata"));
            }
            self.delta = Some(Arc::new(Slab {
                meta: SlabRef {
                    hash: delta.hash,
                    index_hash: delta.metadata_hash,
                    tip: Oid20([0; 20]),
                    tier: Tier::Live,
                    start: 0,
                    end: self.state.cursor,
                    count: delta.count,
                    pack_len: delta.pack_len,
                    idx_len: delta.idx_len,
                },
                index,
                pack: RangeReader::File(File::open(self.path(Role::Delta, delta, "pack"))?),
                idx: RangeReader::File(File::open(self.path(Role::Delta, delta, "idx"))?),
            }));
        }
        Ok(())
    }

    fn settle(&mut self) -> Result<()> {
        if self.metadata().join("transaction.bin").exists() {
            self.recover()?;
            self.load_current()?;
        }
        self.retire()
    }

    fn check_limits(&self, pack: PackFile) -> Result<()> {
        if pack.count as usize > self.config.max_objects
            || pack.pack_len > self.config.max_download_bytes
        {
            return Err(Error::Capacity(
                "existing client cache exceeds configured limits",
            ));
        }
        Ok(())
    }

    fn retire(&mut self) -> Result<()> {
        let Some(old) = self.state.retired else {
            return Ok(());
        };
        let current = self
            .state
            .delta
            .ok_or(Error::Invalid("retirement without replacement"))?;
        if old.hash == current.hash {
            return Err(Error::Invalid("cannot retire current pack"));
        }
        #[cfg(test)]
        self.fail(Failure::Retire)?;
        // Rebuild an existing MIDX with an explicit list BEFORE removing a named pack.
        // Do not run `midx expire`, which could delete unrelated, unmanaged packs.
        let packs = self.config.cache_dir.join("objects/pack");
        if packs.join("multi-pack-index").exists() || packs.join("multi-pack-index.d").exists() {
            let old_idx = self.path(Role::Delta, old, "idx");
            let mut names = Vec::new();
            for entry in fs::read_dir(&packs)? {
                let path = entry?.path();
                if path.extension().is_some_and(|e| e == "idx")
                    && path != old_idx
                    && path.with_extension("pack").is_file()
                {
                    let name = path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .ok_or(Error::Invalid("pack filename"))?
                        .to_owned();
                    if name.contains(['\r', '\n']) {
                        return Err(Error::Invalid("pack filename"));
                    }
                    names.push(name);
                }
            }
            names.sort_unstable();
            let mut child = git_command(&self.config.cache_dir)
                .args(["multi-pack-index", "write", "--stdin-packs", "--no-bitmap"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()?;
            let mut input = child.stdin.take().ok_or(Error::Invalid("MIDX stdin"))?;
            for name in names {
                writeln!(input, "{name}")?;
            }
            drop(input);
            let status = child.wait()?;
            if !status.success() {
                return Err(Error::Git(status.code().unwrap_or(-1)));
            }
        }
        for extension in ["pack", "idx", "keep", "rev", "bitmap", "mphf"] {
            remove(&self.path(Role::Delta, old, extension))?;
        }
        File::open(packs)?.sync_all()?;
        File::open(self.metadata())?.sync_all()?;
        self.state.retired = None;
        self.save_state()
    }

    fn report(
        &self,
        not_modified: bool,
        downloaded_bytes: u64,
        local_pack_bytes_written: u64,
    ) -> SyncReport {
        SyncReport {
            not_modified,
            downloaded_bytes,
            local_pack_bytes_written,
            managed_packs: self.managed_pack_count(),
            cursor: self.state.cursor,
        }
    }
    fn metadata(&self) -> PathBuf {
        self.config.cache_dir.join("gvfs-daemon")
    }
    fn stage(&self, name: &str) -> PathBuf {
        self.metadata().join("stage").join(name)
    }
    fn path(&self, role: Role, pack: PackFile, extension: &str) -> PathBuf {
        let parent = if extension == "mphf" {
            self.metadata()
        } else {
            self.config.cache_dir.join("objects/pack")
        };
        parent.join(format!("{}-{}.{}", role.prefix(), pack.hash, extension))
    }
    fn save_state(&self) -> Result<()> {
        state::save(&self.metadata().join("state.bin"), &self.state.encode()?)
    }
    fn clear_stage(&self) -> Result<()> {
        for name in [
            "incoming.pack",
            "incoming.idx",
            "incoming.rev",
            "incoming.mphf",
            "next.pack",
            "next.idx",
            "next.rev",
            "next.mphf",
        ] {
            remove(&self.stage(name))?;
        }
        Ok(())
    }
    #[cfg(test)]
    fn fail(&mut self, point: Failure) -> Result<()> {
        if self.failure == Some(point) {
            self.failure = None;
            return Err(Error::Invalid("injected cache interruption"));
        }
        Ok(())
    }
}

fn git_command(cache: &Path) -> Command {
    let mut command = crate::source::command();
    for key in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_COMMON_DIR",
        "GIT_INDEX_FILE",
    ] {
        command.env_remove(key);
    }
    command.arg("--git-dir").arg(cache);
    command
}
const CACHE_REF: &str = "refs/heads/gvfs-cache";

fn read_tip(cache: &Path) -> Result<Option<Oid20>> {
    let result = git_command(cache)
        .args([
            "for-each-ref",
            "--format=%(objectname) %(refname)",
            CACHE_REF,
        ])
        .stderr(Stdio::inherit())
        .output()?;
    if !result.status.success() {
        return Err(Error::Git(result.status.code().unwrap_or(-1)));
    }
    for line in result.stdout.split(|&b| b == b'\n') {
        let mut fields = line.splitn(2, |&b| b == b' ');
        let oid = fields.next();
        if fields.next() == Some(CACHE_REF.as_bytes()) {
            return Ok(Some(
                Oid20::from_hex(oid.ok_or(Error::Invalid("cache ref"))?)
                    .ok_or(Error::Invalid("cache ref"))?,
            ));
        }
    }
    Ok(None)
}

fn update_tip(cache: &Path, old: Option<Oid20>, new: Option<Oid20>) -> Result<()> {
    let Some(new) = new else {
        return Ok(());
    };
    let current = read_tip(cache)?;
    if current == Some(new) {
        return Ok(());
    }
    if current != old {
        return Err(Error::Invalid(
            "reserved cache ref was modified outside sync",
        ));
    }
    checked(
        git_command(cache)
            .args(["cat-file", "-e"])
            .arg(format!("{new}^{{commit}}")),
    )?;
    checked(
        git_command(cache)
            .args(["update-ref", CACHE_REF])
            .arg(new.to_string())
            .arg(old.unwrap_or(Oid20([0; 20])).to_string()),
    )
}

fn checked(command: &mut Command) -> Result<()> {
    let status = command
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(Error::Git(status.code().unwrap_or(-1)))
    }
}
fn map_file(path: &Path, limit: u64) -> Result<Mapping> {
    let file = File::open(path)?;
    let len = file.metadata()?.len();
    if len > limit {
        return Err(Error::Capacity("client index"));
    }
    Mapping::read(
        file,
        usize::try_from(len).map_err(|_| Error::Capacity("client index"))?,
    )
}
fn trailing_hash(bytes: &[u8]) -> Result<Oid20> {
    Oid20::from_slice(
        bytes
            .get(bytes.len().saturating_sub(20)..)
            .ok_or(Error::Invalid("index checksum"))?,
    )
    .ok_or(Error::Invalid("index checksum"))
}
fn verify_pair(
    pack_path: &Path,
    idx_path: &Path,
    expected: PackFile,
    buffer: &mut [u8],
) -> Result<()> {
    let mut file = File::open(pack_path)?;
    if file.metadata()?.len() != expected.pack_len || expected.pack_len < 32 {
        return Err(Error::Invalid("client pack length"));
    }
    let hash = copy_verified_pack(
        &mut file,
        &mut std::io::sink(),
        expected.pack_len,
        expected.count as usize,
        buffer,
    )?;
    if hash != expected.hash {
        return Err(Error::Invalid("client pack hash"));
    }
    let map = map_file(idx_path, expected.idx_len)?;
    let idx = IdxView::parse(map.bytes())?;
    if map.bytes().len() as u64 != expected.idx_len
        || !idx.verify_checksum()
        || trailing_hash(map.bytes())? != expected.idx_hash
        || idx.pack_checksum() != Some(hash.0)
        || idx.len() != expected.count as usize
    {
        return Err(Error::Invalid("client index hash/count"));
    }
    Ok(())
}
fn copy_verified_pack(
    input: &mut impl Read,
    output: &mut impl Write,
    len: u64,
    max_objects: usize,
    buffer: &mut [u8],
) -> Result<Oid20> {
    if len < 32 {
        return Err(Error::Invalid("pack length"));
    }
    let mut header = [0; 12];
    input.read_exact(&mut header)?;
    if parse_pack_header(&header)? as usize > max_objects {
        return Err(Error::Capacity("downloaded object count"));
    }
    let mut hash = Sha1::new();
    hash.update(header);
    output.write_all(&header)?;
    let mut remaining = len.saturating_sub(32);
    while remaining > 0 {
        let n = remaining.min(buffer.len() as u64) as usize;
        let bytes = buffer
            .get_mut(..n)
            .ok_or(Error::Capacity("download buffer"))?;
        input.read_exact(bytes)?;
        output.write_all(bytes)?;
        hash.update(&*bytes);
        remaining = remaining.saturating_sub(n as u64);
    }
    let mut trailer = [0; 20];
    input.read_exact(&mut trailer)?;
    let actual: [u8; 20] = hash.finalize().into();
    if trailer != actual {
        return Err(Error::Invalid("downloaded pack checksum"));
    }
    output.write_all(&trailer)?;
    Ok(Oid20(trailer))
}
fn discard(input: &mut impl Read, mut len: u64, buffer: &mut [u8]) -> Result<()> {
    while len > 0 {
        let n = len.min(buffer.len() as u64) as usize;
        input.read_exact(
            buffer
                .get_mut(..n)
                .ok_or(Error::Capacity("discard buffer"))?,
        )?;
        len = len.saturating_sub(n as u64);
    }
    Ok(())
}
fn eof(input: &mut impl Read) -> Result<()> {
    if input.read(&mut [0; 1])? != 0 {
        return Err(Error::Invalid("extra data after prefetch"));
    }
    Ok(())
}
fn remove(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
fn install(source: &Path, destination: &Path) -> Result<()> {
    if destination.exists() {
        // Recovery validates the installed pair against the journal before advancing state.
        return Ok(());
    }
    fs::hard_link(source, destination)?;
    File::open(destination)?.sync_all()?;
    remove(source)
}

struct CountingRead<'a, R: Read> {
    input: &'a mut R,
    bytes: u64,
}
impl<R: Read> Read for CountingRead<'_, R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let n = self.input.read(out)?;
        self.bytes = self
            .bytes
            .checked_add(n as u64)
            .ok_or_else(|| std::io::Error::other("download byte overflow"))?;
        Ok(n)
    }
}
