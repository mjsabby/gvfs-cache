use crate::arena::Mapping;
use crate::manifest::SlabRef;
use crate::mphf::{Index, Overflow, Record};
use crate::storage::{RangeReader, Storage};
use crate::{Error, Oid20, Result};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

pub struct Slab {
    pub meta: SlabRef,
    pub index: Index,
    pub pack: RangeReader,
    pub idx: RangeReader,
}

impl Slab {
    pub fn load(store: &Storage, meta: SlabRef, cache: &Path) -> Result<Arc<Self>> {
        fs::create_dir_all(cache)?;
        let key = meta.key("mphf");
        let path = match store {
            Storage::Fs(root) => root.join(&key),
            Storage::Http(_) => {
                let path = cache.join(&key);
                if !path.exists() {
                    let mut temp = tempfile::NamedTempFile::new_in(cache)?;
                    let limit = (meta.count as u64).saturating_mul(80).saturating_add(1024);
                    store.download(&key, temp.as_file_mut(), limit)?;
                    // Validate before an immutable cache entry becomes visible.
                    Index::open(temp.path())?;
                    match temp.persist_noclobber(&path) {
                        Ok(_) => {}
                        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {}
                        Err(e) => return Err(Error::Io(e.error)),
                    }
                }
                path
            }
        };
        let index = Index::open(&path)?;
        if index.checksum != meta.index_hash
            || index.pack_hash != meta.hash
            || index.pack_len != meta.pack_len
            || index.len() != meta.count as usize
        {
            return Err(Error::Invalid("slab/manifest mismatch"));
        }
        let pack = store.reader(&meta.key("pack"), meta.pack_len)?;
        let idx = store.reader(&meta.key("idx"), meta.idx_len)?;
        Ok(Arc::new(Self {
            meta,
            index,
            pack,
            idx,
        }))
    }

    pub fn from_artifact(artifact: &Artifact, memory_limit: usize) -> Result<Arc<Self>> {
        if artifact.meta.pack_len > memory_limit as u64 {
            return Err(Error::Capacity(
                "live slab; raise --live-mib or run pipeline",
            ));
        }
        let index = Index::open(&artifact.dir.path().join("slab.mphf"))?;
        let pack = RangeReader::Memory(Mapping::read(
            File::open(artifact.dir.path().join("slab.pack"))?,
            artifact.meta.pack_len as usize,
        )?);
        let idx = RangeReader::Memory(Mapping::read(
            File::open(artifact.dir.path().join("slab.idx"))?,
            artifact.meta.idx_len as usize,
        )?);
        Ok(Arc::new(Self {
            meta: artifact.meta,
            index,
            pack,
            idx,
        }))
    }
}

pub struct Artifact {
    pub meta: SlabRef,
    pub dir: tempfile::TempDir,
}

impl Artifact {
    pub fn publish(&self, store: &Storage) -> Result<()> {
        for extension in ["pack", "idx", "mphf"] {
            store.upload(
                &self.meta.key(extension),
                &self.dir.path().join(format!("slab.{extension}")),
            )?;
        }
        Ok(())
    }
}

pub fn finish_artifact(
    dir: tempfile::TempDir,
    mut meta: SlabRef,
    records: &[Record],
    overflow: &[Overflow],
) -> Result<Artifact> {
    let mut entries = Vec::with_capacity(records.len());
    let mut offset = 12u64;
    for row in records {
        entries.push(crate::idx::IdxEntry {
            oid: row.oid,
            crc32: row.crc,
            offset,
        });
        offset = offset
            .checked_add(u64::from(location(row, overflow)?.len))
            .ok_or(Error::Capacity("pack offsets"))?;
    }
    if offset.checked_add(20) != Some(meta.pack_len) {
        return Err(Error::Invalid("artifact length"));
    }
    let large = entries.iter().filter(|e| e.offset > 0x7fff_ffff).count();
    let len = crate::idx::idx_len(entries.len(), large).ok_or(Error::Capacity("Git index"))?;
    let mut bytes = crate::arena::Arena::new(len)?;
    crate::idx::write_idx_v2(&mut entries, &meta.hash.0, bytes.buffer())?;
    let mut idx = File::create(dir.path().join("slab.idx"))?;
    idx.write_all(bytes.buffer())?;
    idx.sync_all()?;
    meta.idx_len = len as u64;
    crate::mphf::write_index(
        &dir.path().join("slab.mphf"),
        records,
        overflow,
        meta.pack_len,
        meta.hash,
    )?;
    let mut file = File::open(dir.path().join("slab.mphf"))?;
    file.seek(SeekFrom::End(-20))?;
    let mut hash = [0; 20];
    file.read_exact(&mut hash)?;
    meta.index_hash = Oid20(hash);
    Ok(Artifact { meta, dir })
}

pub fn location(row: &Record, overflow: &[Overflow]) -> Result<Overflow> {
    let len = row.location & 0x3fff;
    let offset = (row.location >> 14) & 0x3_ffff_ffff;
    if len != 0x3fff {
        return Ok(Overflow {
            offset,
            len: len as u32,
        });
    }
    overflow
        .get(offset as usize)
        .copied()
        .ok_or(Error::Invalid("overflow record"))
}

pub fn find(slabs: &[Arc<Slab>], oid: &Oid20) -> Result<Option<(usize, u32)>> {
    for (source, slab) in slabs.iter().enumerate() {
        if let Some(ordinal) = slab.index.find(oid)? {
            return Ok(Some((source, ordinal)));
        }
    }
    Ok(None)
}
