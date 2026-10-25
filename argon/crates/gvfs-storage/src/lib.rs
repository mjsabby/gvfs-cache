//! `gvfs-storage` — Storage client for `gvfs-daemon` slabs and atomic `If-Match` CAS `manifest.bin`.
//!
//! Implements the storage operations from the Unified Binary Architecture (PDF pp. 163–169):
//! - Immutable slab uploads (`.pack`, `.mphf`, `.idx`, `.meta`, `.hfs`)
//! - Atomic `manifest.bin` creation (`If-None-Match: *`) and updates (`If-Match: <etag>`)
//! - Memory-mapped zero-copy `.pack` / `.idx` loading (`gvfs_alloc_guard::mmap_readonly`)
//! - Coalesced HTTP `Range: bytes=start-end` reads (`fetch_range_batches`)
//! - Orphaned slab garbage collection (`gc_unreferenced_slabs`) for crashed pipeline runs
#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use sha1::{Digest, Sha1};

use gvfs_core::{
    HttpRangeBatch, ManifestDescriptor, MphfSlabIndex, Slab, SlabManifestEntry, UnifiedManifest,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ETag(pub String);

#[derive(Debug)]
pub enum StorageError {
    /// `If-Match: <etag>` or `If-None-Match: *` precondition failed (HTTP 412).
    PreconditionFailed,
    NotFound(String),
    Corrupt(String),
    Io(std::io::Error),
}

impl core::fmt::Display for StorageError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::PreconditionFailed => {
                f.write_str("HTTP 412 Precondition Failed (CAS ETag mismatch)")
            }
            Self::NotFound(k) => write!(f, "storage object not found: {k}"),
            Self::Corrupt(m) => write!(f, "corrupt storage artifact: {m}"),
            Self::Io(e) => write!(f, "storage I/O error: {e}"),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

#[derive(Clone, Debug)]
struct MemEntry {
    data: Bytes,
    etag: ETag,
    created_ts: u64,
}

#[derive(Default, Debug)]
struct MemBucket {
    objects: HashMap<String, MemEntry>,
    seq: u64,
}

static MEM_BUCKETS: OnceLock<Mutex<HashMap<String, Arc<Mutex<MemBucket>>>>> = OnceLock::new();
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(1);

fn get_mem_bucket(name: &str) -> Arc<Mutex<MemBucket>> {
    let map = MEM_BUCKETS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Arc::clone(
        guard
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(MemBucket::default()))),
    )
}

#[derive(Debug, Clone)]
enum Backend {
    Fs {
        root: PathBuf,
        cas_lock: Arc<Mutex<()>>,
    },
    Mem {
        name: String,
        bucket: Arc<Mutex<MemBucket>>,
    },
}

/// Unified storage client (`fs:<dir>` or `mem:<name>`), supporting both eager OS `mmap`
/// and on-demand coalesced `Range: bytes=start-end` reads.
#[derive(Debug, Clone)]
pub struct StorageClient {
    backend: Backend,
    /// When `true` (default on local `fs:`), `.pack` files are memory-mapped into `Slab::in_memory_pack`.
    /// When `false` (simulating remote cloud S3/Azure Blob), `.pack` bytes stay on storage and
    /// are fetched via coalesced `Range` reads (`fetch_range_batches`).
    pub mmap_packs: bool,
    pub range_requests_issued: Arc<AtomicU64>,
    pub range_bytes_fetched: Arc<AtomicU64>,
}

fn compute_etag(bytes: &[u8], seq: u64) -> ETag {
    let digest: [u8; 20] = Sha1::digest(bytes).into();
    let oid = gvfs_core::Oid20(digest);
    ETag(format!("\"{oid}-{seq}\""))
}

fn parse_slab_timestamp_from_key(key: &str) -> Option<u64> {
    let stem = key
        .strip_suffix(".pack")
        .or_else(|| key.strip_suffix(".mphf"))
        .or_else(|| key.strip_suffix(".idx"))
        .or_else(|| key.strip_suffix(".meta"))
        .or_else(|| key.strip_suffix(".hfs"))?;
    let (_, ts_str) = stem.rsplit_once('-')?;
    ts_str.parse::<u64>().ok()
}

fn slab_id_from_key(key: &str) -> Option<&str> {
    key.strip_suffix(".pack")
        .or_else(|| key.strip_suffix(".mphf"))
        .or_else(|| key.strip_suffix(".idx"))
        .or_else(|| key.strip_suffix(".meta"))
        .or_else(|| key.strip_suffix(".hfs"))
}

impl StorageClient {
    pub fn open(spec: &str) -> Result<Self, StorageError> {
        if let Some(name) = spec.strip_prefix("mem:") {
            return Ok(Self {
                backend: Backend::Mem {
                    name: name.to_string(),
                    bucket: get_mem_bucket(name),
                },
                mmap_packs: true,
                range_requests_issued: Arc::new(AtomicU64::new(0)),
                range_bytes_fetched: Arc::new(AtomicU64::new(0)),
            });
        }
        let path_str = spec.strip_prefix("fs:").unwrap_or(spec);
        let root = PathBuf::from(path_str);
        fs::create_dir_all(&root)?;
        Ok(Self {
            backend: Backend::Fs {
                root,
                cas_lock: Arc::new(Mutex::new(())),
            },
            mmap_packs: true,
            range_requests_issued: Arc::new(AtomicU64::new(0)),
            range_bytes_fetched: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn with_mmap_packs(mut self, mmap_packs: bool) -> Self {
        self.mmap_packs = mmap_packs;
        self
    }

    pub fn describe(&self) -> String {
        match &self.backend {
            Backend::Fs { root, .. } => format!("fs:{}", root.display()),
            Backend::Mem { name, .. } => format!("mem:{name}"),
        }
    }

    pub fn put_raw(&self, key: &str, data: Bytes, created_ts: u64) -> Result<ETag, StorageError> {
        let data = gvfs_core::buf::share_bytes(data);
        match &self.backend {
            Backend::Mem { bucket, .. } => {
                let mut b = bucket
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                b.seq = b.seq.saturating_add(1);
                let etag = compute_etag(&data, b.seq);
                b.objects.insert(
                    key.to_string(),
                    MemEntry {
                        data,
                        etag: etag.clone(),
                        created_ts,
                    },
                );
                Ok(etag)
            }
            Backend::Fs { root, .. } => {
                let path = root.join(key);
                let seq = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
                let tmp = root.join(format!(".tmp-{key}-{}-{seq}", std::process::id()));
                {
                    let mut f = fs::File::create(&tmp)?;
                    f.write_all(&data)?;
                    f.sync_all()?;
                }
                fs::rename(&tmp, &path)?;
                Ok(compute_etag(&data, 0))
            }
        }
    }

    pub fn get_raw(&self, key: &str) -> Result<Option<Bytes>, StorageError> {
        match &self.backend {
            Backend::Mem { bucket, .. } => {
                let b = bucket
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                Ok(b.objects.get(key).map(|e| e.data.clone()))
            }
            Backend::Fs { root, .. } => {
                let path = root.join(key);
                if !path.exists() {
                    return Ok(None);
                }
                let mapped = gvfs_alloc_guard::mmap_readonly(&path)?;
                Ok(Some(mapped))
            }
        }
    }

    pub fn get_range_raw(&self, key: &str, start: u64, end: u64) -> Result<Bytes, StorageError> {
        self.range_requests_issued.fetch_add(1, Ordering::Relaxed);
        let len = end.saturating_sub(start);
        self.range_bytes_fetched.fetch_add(len, Ordering::Relaxed);

        match &self.backend {
            Backend::Mem { bucket, .. } => {
                let b = bucket
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let entry = b
                    .objects
                    .get(key)
                    .ok_or_else(|| StorageError::NotFound(key.to_string()))?;
                let s = usize::try_from(start)
                    .map_err(|_| StorageError::Corrupt("range start".into()))?;
                let e =
                    usize::try_from(end).map_err(|_| StorageError::Corrupt("range end".into()))?;
                if e > entry.data.len() || s > e {
                    return Err(StorageError::Corrupt("range out of bounds".into()));
                }
                Ok(entry.data.slice(s..e))
            }
            Backend::Fs { root, .. } => {
                let path = root.join(key);
                if !path.exists() {
                    return Err(StorageError::NotFound(key.to_string()));
                }
                let mut f = fs::File::open(&path)?;
                f.seek(SeekFrom::Start(start))?;
                let n =
                    usize::try_from(len).map_err(|_| StorageError::Corrupt("range len".into()))?;
                let mut buf = vec![0u8; n];
                f.read_exact(&mut buf)?;
                Ok(Bytes::from(buf))
            }
        }
    }

    /// Uploads a sealed `Slab` (`.pack`, `.mphf`, `.idx`, `.meta`, `.hfs`) to immutable storage.
    pub fn upload_slab(&self, slab: &Slab) -> Result<(), StorageError> {
        let pack = slab
            .in_memory_pack
            .as_ref()
            .ok_or_else(|| StorageError::Corrupt("slab has no pack bytes to upload".into()))?;
        self.put_raw(&format!("{}.pack", slab.id), pack.clone(), slab.end_ts)?;

        if let Some(ref idx) = slab.in_memory_idx {
            self.put_raw(&format!("{}.idx", slab.id), idx.clone(), slab.end_ts)?;
        }

        let mphf_len = slab
            .mphf
            .encoded_len()
            .ok_or_else(|| StorageError::Corrupt("mphf length overflow".into()))?;
        let mut mphf_buf = vec![0u8; mphf_len];
        slab.mphf
            .encode_into(&mut mphf_buf)
            .map_err(|_| StorageError::Corrupt("mphf encode failed".into()))?;
        self.put_raw(
            &format!("{}.mphf", slab.id),
            Bytes::from(mphf_buf),
            slab.end_ts,
        )?;

        if let Some(meta_buf) = slab.encode_objects_meta() {
            self.put_raw(
                &format!("{}.meta", slab.id),
                Bytes::from(meta_buf),
                slab.end_ts,
            )?;
        }
        if let Some(hfs_buf) = slab.encode_hfs_sidecar() {
            self.put_raw(
                &format!("{}.hfs", slab.id),
                Bytes::from(hfs_buf),
                slab.end_ts,
            )?;
        }
        Ok(())
    }

    /// Loads a `Slab` from storage (memory-mapping `.pack` and `.idx` when `self.mmap_packs` is true,
    /// and loading `.mphf`, `.meta`, `.hfs` into RAM).
    pub fn load_slab(&self, entry: &SlabManifestEntry) -> Result<Arc<Slab>, StorageError> {
        let mphf_bytes = self
            .get_raw(&format!("{}.mphf", entry.id))?
            .ok_or_else(|| StorageError::NotFound(format!("{}.mphf", entry.id)))?;
        let mphf = MphfSlabIndex::decode(&mphf_bytes)
            .ok_or_else(|| StorageError::Corrupt(format!("corrupt {}.mphf", entry.id)))?;

        let objects = match self.get_raw(&format!("{}.meta", entry.id))? {
            Some(meta_bytes) => Slab::decode_objects_meta(&meta_bytes)
                .ok_or_else(|| StorageError::Corrupt(format!("corrupt {}.meta", entry.id)))?,
            None => Vec::new(),
        };

        let hfs_sidecar = match self.get_raw(&format!("{}.hfs", entry.id))? {
            Some(hfs_bytes) => Slab::decode_hfs_sidecar(&hfs_bytes)
                .ok_or_else(|| StorageError::Corrupt(format!("corrupt {}.hfs", entry.id)))?,
            None => Vec::new(),
        };

        let (in_memory_pack, in_memory_idx) = if self.mmap_packs {
            let p = self.get_raw(&format!("{}.pack", entry.id))?;
            let i = self.get_raw(&format!("{}.idx", entry.id))?;
            (p, i)
        } else {
            (None, None)
        };

        Ok(Arc::new(Slab {
            id: entry.id.clone(),
            tier: entry.tier,
            start_ts: entry.start_ts,
            end_ts: entry.end_ts,
            have_commit: entry.have_commit,
            tip_commit: entry.tip_commit,
            objects,
            mphf,
            hfs_sidecar,
            in_memory_pack,
            in_memory_idx,
            mmap_bundle: None,
            pack_byte_len: entry.pack_byte_len,
        }))
    }

    fn hydrate_manifest(&self, desc: ManifestDescriptor) -> Result<UnifiedManifest, StorageError> {
        let weekly_baseline = self.load_slab(&desc.weekly_baseline)?;
        let mut weekly_deltas = Vec::with_capacity(desc.weekly_deltas.len());
        for d in &desc.weekly_deltas {
            weekly_deltas.push(self.load_slab(d)?);
        }
        let mut sub_week_pyramid = Vec::with_capacity(desc.sub_week_pyramid.len());
        for s in &desc.sub_week_pyramid {
            sub_week_pyramid.push(self.load_slab(s)?);
        }
        Ok(UnifiedManifest {
            version_seq: desc.version_seq,
            last_15m_tip_oid: desc.last_15m_tip_oid,
            weekly_baseline,
            weekly_deltas,
            sub_week_pyramid,
        })
    }

    /// Reads `manifest.bin` and its current `ETag` from storage (`None` on Day-0 before bootstrap).
    pub fn get_manifest_with_etag(&self) -> Result<Option<(UnifiedManifest, ETag)>, StorageError> {
        match &self.backend {
            Backend::Mem { bucket, .. } => {
                let (data, etag) = {
                    let b = bucket
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let Some(entry) = b.objects.get("manifest.bin") else {
                        return Ok(None);
                    };
                    (entry.data.clone(), entry.etag.clone())
                };
                let desc = ManifestDescriptor::decode(&data)
                    .ok_or_else(|| StorageError::Corrupt("corrupt manifest.bin".into()))?;
                let manifest = self.hydrate_manifest(desc)?;
                Ok(Some((manifest, etag)))
            }
            Backend::Fs { root, cas_lock } => {
                let _guard = cas_lock
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let path = root.join("manifest.bin");
                if !path.exists() {
                    return Ok(None);
                }
                let data = fs::read(&path)?;
                let etag = compute_etag(&data, 0);
                let desc = ManifestDescriptor::decode(&data)
                    .ok_or_else(|| StorageError::Corrupt("corrupt manifest.bin".into()))?;
                drop(_guard);
                let manifest = self.hydrate_manifest(desc)?;
                Ok(Some((manifest, etag)))
            }
        }
    }

    /// Returns `Some((new_manifest, new_etag))` only if `manifest.bin` has changed since `current_etag`.
    pub fn get_manifest_if_changed(
        &self,
        current_etag: &ETag,
    ) -> Result<Option<(UnifiedManifest, ETag)>, StorageError> {
        match &self.backend {
            Backend::Mem { bucket, .. } => {
                let (data, etag) = {
                    let b = bucket
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let Some(entry) = b.objects.get("manifest.bin") else {
                        return Ok(None);
                    };
                    if &entry.etag == current_etag {
                        return Ok(None);
                    }
                    (entry.data.clone(), entry.etag.clone())
                };
                let desc = ManifestDescriptor::decode(&data)
                    .ok_or_else(|| StorageError::Corrupt("corrupt manifest.bin".into()))?;
                let manifest = self.hydrate_manifest(desc)?;
                Ok(Some((manifest, etag)))
            }
            Backend::Fs { root, cas_lock } => {
                let _guard = cas_lock
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let path = root.join("manifest.bin");
                if !path.exists() {
                    return Ok(None);
                }
                let data = fs::read(&path)?;
                let etag = compute_etag(&data, 0);
                if &etag == current_etag {
                    return Ok(None);
                }
                let desc = ManifestDescriptor::decode(&data)
                    .ok_or_else(|| StorageError::Corrupt("corrupt manifest.bin".into()))?;
                drop(_guard);
                let manifest = self.hydrate_manifest(desc)?;
                Ok(Some((manifest, etag)))
            }
        }
    }

    /// Pre-warms `.mphf` pilot + descriptor tables into RAM (PDF p. 166).
    pub fn warm_mphf_indexes(&self, manifest: &UnifiedManifest) -> Result<(), StorageError> {
        let mut touch_acc = 0u64;
        for d in manifest.weekly_baseline.mphf.descriptors.iter() {
            touch_acc = touch_acc.wrapping_add(d.0);
        }
        for s in &manifest.sub_week_pyramid {
            for d in s.mphf.descriptors.iter() {
                touch_acc = touch_acc.wrapping_add(d.0);
            }
        }
        std::hint::black_box(touch_acc);
        Ok(())
    }

    /// Atomic genesis creation (`If-None-Match: *`, PDF p. 168).
    /// Fails with [`StorageError::PreconditionFailed`] if `manifest.bin` already exists.
    pub fn put_manifest_if_none_match(
        &self,
        manifest: &UnifiedManifest,
    ) -> Result<ETag, StorageError> {
        let raw = manifest
            .to_descriptor()
            .encode()
            .map_err(|_| StorageError::Corrupt("manifest encode overflow".into()))?;
        let data = Bytes::from(raw);

        match &self.backend {
            Backend::Mem { bucket, .. } => {
                let mut b = bucket
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if b.objects.contains_key("manifest.bin") {
                    return Err(StorageError::PreconditionFailed);
                }
                b.seq = b.seq.saturating_add(1);
                let etag = compute_etag(&data, b.seq);
                b.objects.insert(
                    "manifest.bin".to_string(),
                    MemEntry {
                        data,
                        etag: etag.clone(),
                        created_ts: manifest.weekly_baseline.end_ts,
                    },
                );
                Ok(etag)
            }
            Backend::Fs { root, cas_lock } => {
                let _guard = cas_lock
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let path = root.join("manifest.bin");
                if path.exists() {
                    return Err(StorageError::PreconditionFailed);
                }
                let seq = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
                let tmp = root.join(format!(".tmp-manifest-{}-{seq}", std::process::id()));
                {
                    let mut f = fs::File::create(&tmp)?;
                    f.write_all(&data)?;
                    f.sync_all()?;
                }
                fs::rename(&tmp, &path)?;
                Ok(compute_etag(&data, 0))
            }
        }
    }

    /// Atomic CAS update (`If-Match: <etag>`, PDF p. 169).
    /// Fails with [`StorageError::PreconditionFailed`] if `manifest.bin` was modified concurrently.
    pub fn put_manifest_if_match(
        &self,
        manifest: &UnifiedManifest,
        expected_etag: &ETag,
    ) -> Result<ETag, StorageError> {
        let mut desc = manifest.to_descriptor();
        desc.version_seq = desc.version_seq.saturating_add(1);
        let raw = desc
            .encode()
            .map_err(|_| StorageError::Corrupt("manifest encode overflow".into()))?;
        let data = Bytes::from(raw);

        match &self.backend {
            Backend::Mem { bucket, .. } => {
                let mut b = bucket
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let Some(current) = b.objects.get("manifest.bin") else {
                    return Err(StorageError::PreconditionFailed);
                };
                if &current.etag != expected_etag {
                    return Err(StorageError::PreconditionFailed);
                }
                b.seq = b.seq.saturating_add(1);
                let etag = compute_etag(&data, b.seq);
                b.objects.insert(
                    "manifest.bin".to_string(),
                    MemEntry {
                        data,
                        etag: etag.clone(),
                        created_ts: manifest.weekly_baseline.end_ts,
                    },
                );
                Ok(etag)
            }
            Backend::Fs { root, cas_lock } => {
                let _guard = cas_lock
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let path = root.join("manifest.bin");
                if !path.exists() {
                    return Err(StorageError::PreconditionFailed);
                }
                let existing = fs::read(&path)?;
                let current_etag = compute_etag(&existing, 0);
                if &current_etag != expected_etag {
                    return Err(StorageError::PreconditionFailed);
                }
                let seq = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
                let tmp = root.join(format!(".tmp-manifest-{}-{seq}", std::process::id()));
                {
                    let mut f = fs::File::create(&tmp)?;
                    f.write_all(&data)?;
                    f.sync_all()?;
                }
                fs::rename(&tmp, &path)?;
                Ok(compute_etag(&data, 0))
            }
        }
    }

    /// Executes coalesced HTTP `Range: bytes=start-end` reads for `batches`, using zero-copy
    /// in-memory / `mmap` slices when the slab's `.pack` is already mapped locally, or issuing
    /// ranged storage reads otherwise.
    pub fn fetch_range_batches(
        &self,
        batches: &[HttpRangeBatch],
        slabs: &[Arc<Slab>],
    ) -> Result<Vec<(HttpRangeBatch, Bytes)>, StorageError> {
        let mut out = Vec::with_capacity(batches.len());
        for batch in batches {
            if let Some(slab) = slabs.get(batch.slab_index) {
                if let Some(ref pack) = slab.in_memory_pack {
                    let start = usize::try_from(batch.range_start)
                        .map_err(|_| StorageError::Corrupt("range start".into()))?;
                    let end = usize::try_from(batch.range_end)
                        .map_err(|_| StorageError::Corrupt("range end".into()))?;
                    if end <= pack.len() && start <= end {
                        out.push((batch.clone(), pack.slice(start..end)));
                        continue;
                    }
                }
            }
            let range_bytes = self.get_range_raw(
                &format!("{}.pack", batch.slab_id),
                batch.range_start,
                batch.range_end,
            )?;
            out.push((batch.clone(), range_bytes));
        }
        Ok(out)
    }

    /// Sweeps any orphaned/crashed `.pack`, `.mphf`, `.idx`, `.meta`, `.hfs` files older than
    /// `cutoff_ts` (`now_ts.saturating_sub(7200)`) that are not referenced by `manifest.bin`
    /// (PDF p. 165 & 169).
    pub fn gc_unreferenced_slabs(
        &self,
        manifest: &UnifiedManifest,
        cutoff_ts: u64,
    ) -> Result<usize, StorageError> {
        let referenced = manifest.referenced_slab_ids();
        let mut deleted = 0usize;

        match &self.backend {
            Backend::Mem { bucket, .. } => {
                let mut b = bucket
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let keys_to_remove: Vec<String> = b
                    .objects
                    .iter()
                    .filter_map(|(k, entry)| {
                        if k == "manifest.bin" {
                            return None;
                        }
                        let slab_id = slab_id_from_key(k)?;
                        let ts = if entry.created_ts > 0 {
                            entry.created_ts
                        } else {
                            parse_slab_timestamp_from_key(k).unwrap_or(u64::MAX)
                        };
                        if !referenced.contains(&slab_id) && ts <= cutoff_ts {
                            Some(k.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                for k in keys_to_remove {
                    b.objects.remove(&k);
                    deleted = deleted.saturating_add(1);
                }
            }
            Backend::Fs { root, .. } => {
                for dir_entry in fs::read_dir(root)? {
                    let dir_entry = dir_entry?;
                    let file_name = dir_entry.file_name();
                    let Some(k) = file_name.to_str() else {
                        continue;
                    };
                    if k == "manifest.bin" || k.starts_with(".tmp-") {
                        continue;
                    }
                    let Some(slab_id) = slab_id_from_key(k) else {
                        continue;
                    };
                    let Some(ts) = parse_slab_timestamp_from_key(k) else {
                        continue;
                    };
                    if !referenced.contains(&slab_id) && ts <= cutoff_ts {
                        let _ = fs::remove_file(dir_entry.path());
                        deleted = deleted.saturating_add(1);
                    }
                }
            }
        }

        Ok(deleted)
    }
}

/// Helper to mount an existing local `.git/objects/pack/pack-*.{pack,idx,rev}` directly as
/// a zero-copy memory-mapped `Slab`.
pub fn mmap_existing_git_pack(
    git_dir: &Path,
    tip_commit: gvfs_core::Oid20,
    now_ts: u64,
) -> Result<Option<Slab>, StorageError> {
    let pack_dir = git_dir.join("objects").join("pack");
    if !pack_dir.exists() {
        return Ok(None);
    }
    let mut pack_file: Option<PathBuf> = None;
    for entry in fs::read_dir(&pack_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("pack") {
            pack_file = Some(path);
            break;
        }
    }
    let Some(pack_path) = pack_file else {
        return Ok(None);
    };
    let idx_path = pack_path.with_extension("idx");
    let rev_path = pack_path.with_extension("rev");
    if !idx_path.exists() {
        return Ok(None);
    }

    let pack_bytes = gvfs_alloc_guard::mmap_readonly(&pack_path)?;
    let idx_bytes = gvfs_alloc_guard::mmap_readonly(&idx_path)?;
    let rev_bytes = if rev_path.exists() {
        Some(gvfs_alloc_guard::mmap_readonly(&rev_path)?)
    } else {
        None
    };

    let slab = Slab::from_mmap_pack_bundle(
        format!("slab-168h-base-{now_ts}"),
        gvfs_core::Tier::H168Base,
        0,
        now_ts,
        tip_commit,
        pack_bytes,
        idx_bytes,
        rev_bytes,
    )
    .ok_or_else(|| StorageError::Corrupt("failed to parse mmap git pack/idx/rev".into()))?;
    Ok(Some(slab))
}
