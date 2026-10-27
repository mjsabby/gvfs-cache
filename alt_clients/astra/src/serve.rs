use crate::arena::{Arena, push};
use crate::buf::{ByteWriter, parse_dec_u64};
use crate::mphf::NO_BASE;
use crate::object::{TreeIter, parse_commit};
use crate::pack::{ObjType, parse_entry_header};
use crate::pipeline::{Catalog, Config, Engine};
use crate::slab::Slab;
use crate::splice::{Plan, splice_slab_ranges};
use crate::zlib::{Deflater, Inflater, LooseSplice, compress_bound};
use crate::{Error, Oid20, Result};
use arc_swap::ArcSwap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub struct ServerOptions {
    pub bind: SocketAddr,
    pub workers: usize,
    pub poll: Duration,
    pub manifest_poll: Duration,
    pub request_timeout: Duration,
    pub read_only: bool,
    pub public_url: Option<String>,
    pub blob_store_url: Option<String>,
    pub prefetch_index: bool,
    pub branch: String,
}

struct Snapshot {
    revision: Oid20,
    catalog: Catalog,
    active: Vec<Arc<Slab>>,
    sealed: Arc<Plan>,
    cold: Arc<Plan>,
}

impl Snapshot {
    fn build(catalog: &Catalog, config: &Config, previous: Option<&Self>) -> Result<Self> {
        let sealed = if let Some(previous) =
            previous.filter(|p| p.catalog.manifest.generation == catalog.manifest.generation)
        {
            Arc::clone(&previous.sealed)
        } else {
            let mut plan = Plan::new(config.max_objects, config.max_objects);
            plan.add(&catalog.baseline)?;
            for slab in &catalog.pyramid {
                plan.add(slab)?;
            }
            plan.select_since(0)?;
            plan.freeze();
            Arc::new(plan)
        };
        let cold = if let Some(live) = &catalog.live {
            let mut plan = sealed.compiled_copy();
            plan.append_disjoint_live(live)?;
            Arc::new(plan)
        } else {
            Arc::clone(&sealed)
        };
        Ok(Self {
            revision: catalog.revision(),
            catalog: catalog.clone(),
            active: catalog.active(),
            sealed,
            cold,
        })
    }
}

struct Shared {
    snapshot: ArcSwap<Snapshot>,
    healthy: AtomicBool,
    requests: AtomicU64,
    errors: AtomicU64,
    not_modified: AtomicU64,
    prefetch_index: bool,
    config: Vec<u8>,
    branch: String,
}

pub fn now() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Invalid("system clock"))?
        .as_secs())
}

pub fn run(mut engine: Engine, options: ServerOptions) -> Result<()> {
    if options.workers == 0
        || options.workers > 256
        || options.poll.is_zero()
        || options.manifest_poll.is_zero()
        || options.request_timeout.is_zero()
    {
        return Err(Error::Invalid("worker count or timeout"));
    }
    engine.reload()?;
    if !options.read_only {
        engine.live_sync(now()?)?;
    }
    let snapshot = Snapshot::build(
        engine
            .catalog
            .as_ref()
            .ok_or(Error::Invalid("run pipeline before serve"))?,
        &engine.config,
        None,
    )?;
    let listener = TcpListener::bind(options.bind)?;
    let address = listener.local_addr()?;
    let public = options
        .public_url
        .unwrap_or_else(|| format!("http://{address}"));
    let mut config = vec![0; 8192];
    let mut w = ByteWriter::new(&mut config);
    w.put(b"{\"AllowedGvfsClientVersions\":[{\"Min\":{\"Major\":0,\"Minor\":0,\"Build\":0,\"Revision\":0},\"Max\":null}],\"CacheServers\":[{\"Name\":\"default\",\"Url\":")?;
    crate::json::write_json_string(&mut w, public.as_bytes())?;
    w.put(b",\"GlobalDefault\":true}],\"BlobStores\":[")?;
    if let Some(url) = options.blob_store_url {
        if url.is_empty() {
            return Err(Error::Invalid("empty BlobStore URL"));
        }
        w.put(b"{\"Name\":\"default\",\"Url\":")?;
        crate::json::write_json_string(&mut w, url.as_bytes())?;
        w.put(b",\"GlobalDefault\":true}")?;
    }
    w.put(b"]}")?;
    let len = w.len();
    config.truncate(len);
    let shared = Arc::new(Shared {
        snapshot: ArcSwap::from_pointee(snapshot),
        healthy: AtomicBool::new(true),
        requests: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        not_modified: AtomicU64::new(0),
        prefetch_index: options.prefetch_index,
        config,
        branch: options.branch,
    });
    let barrier = Arc::new(Barrier::new(options.workers.saturating_add(1)));
    let mut workers = Vec::with_capacity(options.workers);
    for _ in 0..options.workers {
        let mut worker = Worker::new(&engine.config)?;
        let mut receive = Arena::new(4 << 20)?;
        let listener = listener.try_clone()?;
        let shared = Arc::clone(&shared);
        let barrier = Arc::clone(&barrier);
        let timeout = options.request_timeout;
        workers.push(std::thread::spawn(move || {
            {
                let warm = shared.snapshot.load();
                std::hint::black_box(&warm);
            }
            barrier.wait();
            loop {
                match listener.accept() {
                    Ok((stream, _)) => {
                        if let Err(e) =
                            connection(stream, &mut receive, &mut worker, &shared, timeout)
                        {
                            shared.errors.fetch_add(1, Ordering::Relaxed);
                            eprintln!("request failed: {e}");
                        }
                    }
                    Err(e) => {
                        eprintln!("accept failed: {e}");
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        }));
    }
    let sync_shared = Arc::clone(&shared);
    std::thread::spawn(move || {
        let mut next_manifest = Instant::now();
        loop {
            std::thread::sleep(options.poll);
            let mut update = || -> Result<bool> {
                let mut changed = false;
                if Instant::now() >= next_manifest {
                    changed |= engine.reload()?;
                    next_manifest = Instant::now()
                        .checked_add(options.manifest_poll)
                        .ok_or(Error::Invalid("manifest poll duration"))?;
                }
                if !options.read_only {
                    changed |= engine.live_sync(now()?)?;
                }
                // A failed earlier reconstruction must not leave the old snapshot published.
                if changed || !sync_shared.healthy.load(Ordering::Acquire) {
                    let previous = sync_shared.snapshot.load();
                    let snapshot = Snapshot::build(
                        engine.catalog.as_ref().ok_or(Error::Invalid("catalog"))?,
                        &engine.config,
                        Some(&previous),
                    )?;
                    sync_shared.snapshot.store(Arc::new(snapshot));
                }
                Ok(changed)
            };
            match update() {
                Ok(_) => {
                    sync_shared.healthy.store(true, Ordering::Release);
                }
                Err(e) => {
                    sync_shared.healthy.store(false, Ordering::Release);
                    eprintln!("live/manifest sync failed; readiness disabled: {e}");
                }
            }
        }
    });
    alloc_guard::seal();
    barrier.wait();
    println!("listening http://{address}");
    std::io::stdout().flush()?;
    for worker in workers {
        if worker.join().is_err() {
            return Err(Error::Invalid("HTTP worker terminated"));
        }
    }
    Ok(())
}

struct Worker {
    plan: Plan,
    oids: Vec<Oid20>,
    queue: Vec<(Oid20, u64, bool)>,
    expanded: Vec<u64>,
    expanded_words: Vec<u32>,
    range: Arena,
    raw: Arena,
    a: Arena,
    b: Arena,
    delta: Arena,
    compressed: Arena,
    inflate: Inflater,
    deflate: Deflater,
    started: bool,
}

impl Worker {
    fn new(config: &Config) -> Result<Self> {
        let words = config.max_objects.div_ceil(64);
        Ok(Self {
            plan: Plan::new(config.max_objects, config.max_objects),
            oids: Vec::with_capacity(65536),
            queue: Vec::with_capacity(config.max_objects),
            expanded: vec![0; words],
            expanded_words: Vec::with_capacity(words),
            range: Arena::new(1 << 20)?,
            raw: Arena::new(compress_bound(config.max_object_bytes).saturating_add(32))?,
            a: Arena::new(config.max_object_bytes)?,
            b: Arena::new(config.max_object_bytes)?,
            delta: Arena::new(config.max_object_bytes)?,
            compressed: Arena::new(compress_bound(config.max_object_bytes).saturating_add(64))?,
            inflate: Inflater::new(),
            deflate: Deflater::new(1),
            started: false,
        })
    }
    fn read_object(&mut self, slab: &Slab, row: u32) -> Result<(ObjType, usize)> {
        let mut chain = [0u32; 129];
        let mut depth = 0usize;
        let mut current = row;
        loop {
            *chain.get_mut(depth).ok_or(Error::Invalid("delta depth"))? = current;
            depth = depth.saturating_add(1);
            let record = slab.index.record(current)?;
            if record.base == NO_BASE {
                break;
            }
            current = record.base;
        }
        let wanted = slab.index.record(row)?;
        let mut cluster_range = None;
        if wanted.base != NO_BASE {
            let base = slab.index.record(wanted.base)?;
            if base.base == NO_BASE {
                let first = slab.index.location(&base)?;
                let last = slab.index.location(&wanted)?;
                let end = last
                    .offset
                    .checked_add(u64::from(last.len))
                    .ok_or(Error::Invalid("cluster end"))?;
                if last.offset > first.offset
                    && end.saturating_sub(first.offset) <= crate::tree_delta::MAX_CLUSTER_BYTES
                {
                    let len = end.saturating_sub(first.offset) as usize;
                    slab.pack.read(
                        first.offset,
                        self.range
                            .buffer()
                            .get_mut(..len)
                            .ok_or(Error::Capacity("cluster buffer"))?,
                    )?;
                    cluster_range = Some((first.offset, end));
                }
            }
        }
        while depth > 0 {
            depth = depth.saturating_sub(1);
            let row = *chain.get(depth).ok_or(Error::Invalid("delta stack"))?;
            let record = slab.index.record(row)?;
            let loc = slab.index.location(&record)?;
            let raw = self
                .raw
                .buffer()
                .get_mut(..loc.len as usize)
                .ok_or(Error::Capacity("compressed object"))?;
            if let Some((start, _)) = cluster_range.filter(|&(start, end)| {
                loc.offset >= start && loc.offset.saturating_add(u64::from(loc.len)) <= end
            }) {
                let at = loc.offset.saturating_sub(start) as usize;
                raw.copy_from_slice(
                    self.range
                        .buffer()
                        .get(at..at.saturating_add(loc.len as usize))
                        .ok_or(Error::Invalid("cluster slice"))?,
                );
            } else {
                slab.pack.read(loc.offset, raw)?;
            }
            if crc32fast::hash(raw) != record.crc {
                return Err(Error::Invalid("stored object CRC"));
            }
            let header = parse_entry_header(raw)?.ok_or(Error::Invalid("stored entry"))?;
            let zlib = raw
                .get(header.header_len..)
                .ok_or(Error::Invalid("zlib offset"))?;
            if record.base == NO_BASE {
                if header.kind != record.kind()? || header.size != record.size() as u64 {
                    return Err(Error::Invalid("stored entry metadata"));
                }
                if self
                    .inflate
                    .inflate_exact(zlib, self.a.buffer(), record.size())?
                    != zlib.len()
                {
                    return Err(Error::Invalid("trailing zlib bytes"));
                }
                self.a.set_len(record.size())?;
            } else {
                let base_oid = slab.index.record(record.base)?.oid;
                if header.base != crate::pack::DeltaBase::Ref(base_oid) {
                    return Err(Error::Invalid("stored delta reference"));
                }
                let len = usize::try_from(header.size).map_err(|_| Error::Capacity("delta"))?;
                if self.inflate.inflate_exact(zlib, self.delta.buffer(), len)? != zlib.len() {
                    return Err(Error::Invalid("trailing delta bytes"));
                }
                self.delta.set_len(len)?;
                let n =
                    crate::delta::apply_delta(self.a.bytes(), self.delta.bytes(), self.b.buffer())?;
                if n != record.size() {
                    return Err(Error::Invalid("delta result size"));
                }
                self.b.set_len(n)?;
                std::mem::swap(&mut self.a, &mut self.b);
            }
        }
        let kind = wanted.kind()?;
        if crate::object_id(kind, self.a.bytes()) != wanted.oid {
            return Err(Error::Invalid("stored object OID"));
        }
        Ok((kind, self.a.len()))
    }

    fn objects_plan(&mut self, snapshot: &Snapshot, depth: u64) -> Result<()> {
        self.plan.reset()?;
        self.queue.clear();
        for &word in &self.expanded_words {
            *self
                .expanded
                .get_mut(word as usize)
                .ok_or(Error::Invalid("closure bitset"))? = 0;
        }
        self.expanded_words.clear();
        for slab in &snapshot.active {
            self.plan.add(slab)?;
        }
        for &oid in &self.oids {
            push(&mut self.queue, (oid, depth, false))?;
        }
        let mut cursor = 0usize;
        while let Some(&(oid, depth, tree_closure)) = self.queue.get(cursor) {
            cursor = cursor.saturating_add(1);
            let (source, row) = self.plan.select_oid(&oid)?;
            let slab = Arc::clone(
                snapshot
                    .active
                    .get(source)
                    .ok_or(Error::Invalid("closure source"))?,
            );
            let record = slab.index.record(row)?;
            if record.kind()? == ObjType::Blob || (record.kind()? == ObjType::Tree && !tree_closure)
            {
                continue;
            }
            let bit = self.plan.row_index(source, row)?;
            let word = self
                .expanded
                .get_mut(bit >> 6)
                .ok_or(Error::Capacity("closure bitset"))?;
            let mask = 1u64 << (bit & 63);
            if *word & mask != 0 {
                continue;
            }
            if *word == 0 {
                push(&mut self.expanded_words, (bit >> 6) as u32)?;
            }
            *word |= mask;
            match record.kind()? {
                ObjType::Blob => {}
                ObjType::Commit => {
                    self.read_object(&slab, row)?;
                    let info = parse_commit(self.a.bytes(), |parent| {
                        depth <= 1
                            || push(&mut self.queue, (parent, depth.saturating_sub(1), false))
                                .is_ok()
                    })
                    .ok_or(Error::Invalid("commit or closure capacity"))?;
                    push(&mut self.queue, (info.tree, 0, true))?;
                }
                ObjType::Tree => {
                    self.read_object(&slab, row)?;
                    for item in TreeIter::new(self.a.bytes()) {
                        let entry = item.map_err(|_| Error::Invalid("tree"))?;
                        if entry.is_tree() || entry.is_blob() {
                            push(&mut self.queue, (entry.oid, 0, true))?;
                        }
                    }
                }
                ObjType::Tag => {
                    self.read_object(&slab, row)?;
                    let line = self
                        .a
                        .bytes()
                        .split(|&b| b == b'\n')
                        .next()
                        .ok_or(Error::Invalid("tag"))?;
                    let target = Oid20::from_hex(
                        line.strip_prefix(b"object ").ok_or(Error::Invalid("tag"))?,
                    )
                    .ok_or(Error::Invalid("tag OID"))?;
                    push(&mut self.queue, (target, depth, tree_closure))?;
                }
                _ => return Err(Error::Invalid("object type")),
            }
        }
        self.plan.finish()
    }

    fn handle(
        &mut self,
        request: &Request,
        body: &[u8],
        shared: &Shared,
        stream: &mut Deadline<'_>,
    ) -> Result<()> {
        self.started = false;
        shared.requests.fetch_add(1, Ordering::Relaxed);
        if !shared.healthy.load(Ordering::Acquire) && !matches!(request.route, Route::Metrics) {
            return Err(Error::Invalid(
                "cache is not ready; upstream/manifest sync failed",
            ));
        }
        let snapshot = shared.snapshot.load();
        let _hot = alloc_guard::hot_path();
        match request.route {
            Route::Health => self.fixed(stream, 200, "text/plain", b"ready\n", request.close),
            Route::Config => self.fixed(
                stream,
                200,
                "application/json",
                &shared.config,
                request.close,
            ),
            Route::Metrics => {
                let mut bytes = [0; 512];
                let mut w = ByteWriter::new(&mut bytes);
                for (key, value) in [
                    (
                        b"gvfs_requests_total ".as_slice(),
                        shared.requests.load(Ordering::Relaxed),
                    ),
                    (
                        b"gvfs_errors_total ".as_slice(),
                        shared.errors.load(Ordering::Relaxed),
                    ),
                    (
                        b"gvfs_alloc_hot_path_total ".as_slice(),
                        alloc_guard::stats().hot_path,
                    ),
                    (
                        b"gvfs_alloc_post_seal_total ".as_slice(),
                        alloc_guard::stats().post_seal,
                    ),
                    (
                        b"gvfs_prefetch_not_modified_total ".as_slice(),
                        shared.not_modified.load(Ordering::Relaxed),
                    ),
                ] {
                    w.put(key)?;
                    w.put_dec(value)?;
                    w.put(b"\n")?;
                }
                self.fixed(stream, 200, "text/plain", w.written(), request.close)
            }
            Route::Prefetch(timestamp) => {
                snapshot.catalog.validate_cursor(timestamp)?;
                let etag = crate::gpre::etag(snapshot.revision, timestamp, shared.prefetch_index);
                let metadata = PrefetchHeaders {
                    etag,
                    tip: snapshot.catalog.tip(),
                    coverage: None,
                };
                if request.if_none_match.matches(etag) {
                    self.started = true;
                    shared.not_modified.fetch_add(1, Ordering::Relaxed);
                    return head_with_metadata(
                        stream,
                        304,
                        crate::gpre::CONTENT_TYPE_PREFETCH,
                        None,
                        request.close,
                        Some(metadata),
                    );
                }
                let plan = if timestamp == 0 {
                    &snapshot.cold
                } else {
                    snapshot.catalog.prefetch(timestamp, &mut self.plan)?;
                    &self.plan
                };
                if plan.count == 0 {
                    self.started = true;
                    head_with_metadata(
                        stream,
                        200,
                        crate::gpre::CONTENT_TYPE_PREFETCH,
                        Some(8),
                        request.close,
                        Some(metadata),
                    )?;
                    stream.write_all(&crate::gpre::gpre_file_header(0))?;
                    return Ok(());
                }
                let whole = plan.whole_slab();
                let idx_len = if shared.prefetch_index {
                    whole.map_or(0, |s| s.meta.idx_len)
                } else {
                    0
                };
                let length = 32u64
                    .checked_add(plan.pack_len)
                    .and_then(|n| n.checked_add(idx_len))
                    .ok_or(Error::Capacity("response"))?;
                self.started = true;
                head_with_metadata(
                    stream,
                    200,
                    crate::gpre::CONTENT_TYPE_PREFETCH,
                    Some(length),
                    request.close,
                    Some(PrefetchHeaders {
                        coverage: Some(PackCoverage {
                            start: timestamp,
                            end: snapshot.catalog.manifest.timestamp,
                            baseline: timestamp == 0,
                            live: snapshot
                                .catalog
                                .live
                                .as_ref()
                                .is_some_and(|s| s.meta.count != 0),
                        }),
                        ..metadata
                    }),
                )?;
                stream.write_all(&crate::gpre::gpre_file_header(1))?;
                // Only the durable manifest advances the cursor. L0 is replayed until sealed,
                // so independent edge clocks and pipeline races cannot make clients miss objects.
                let cursor = snapshot.catalog.manifest.timestamp;
                stream.write_all(&crate::gpre::gpre_pack_header(
                    cursor as i64,
                    i64::try_from(plan.pack_len).map_err(|_| Error::Capacity("pack"))?,
                    if idx_len == 0 {
                        -1
                    } else {
                        i64::try_from(idx_len).map_err(|_| Error::Capacity("index"))?
                    },
                ))?;
                if let Some(slab) = whole {
                    copy_range(
                        &slab.pack,
                        0,
                        slab.meta.pack_len,
                        stream,
                        self.range.buffer(),
                    )?;
                    if idx_len > 0 {
                        copy_range(&slab.idx, 0, idx_len, stream, self.range.buffer())?;
                    }
                } else {
                    splice_slab_ranges(plan, stream, self.range.buffer(), 64 << 10)?;
                }
                Ok(())
            }
            Route::Object(oid) => {
                let (source, row) =
                    crate::slab::find(&snapshot.active, &oid)?.ok_or(Error::Missing(oid))?;
                let slab = snapshot.active.get(source).ok_or(Error::Invalid("slab"))?;
                let record = slab.index.record(row)?;
                if record.base == NO_BASE {
                    let loc = slab.index.location(&record)?;
                    let raw = self
                        .raw
                        .buffer()
                        .get_mut(..loc.len as usize)
                        .ok_or(Error::Capacity("loose object"))?;
                    slab.pack.read(loc.offset, raw)?;
                    if crc32fast::hash(raw) != record.crc {
                        return Err(Error::Invalid("stored CRC"));
                    }
                    let header = parse_entry_header(raw)?.ok_or(Error::Invalid("entry"))?;
                    if header.kind != record.kind()? || header.size != record.size() as u64 {
                        return Err(Error::Invalid("stored metadata"));
                    }
                    let zlib = raw.get(header.header_len..).ok_or(Error::Invalid("zlib"))?;
                    let loose = LooseSplice::plan(record.kind()?, record.size() as u64, zlib)?;
                    self.started = true;
                    head(
                        stream,
                        200,
                        crate::gpre::CONTENT_TYPE_LOOSE_OBJECT,
                        Some(loose.total_len() as u64),
                        request.close,
                    )?;
                    stream.write_all(loose.prefix())?;
                    stream.write_all(
                        zlib.get(loose.body.clone())
                            .ok_or(Error::Invalid("loose splice"))?,
                    )?;
                    stream.write_all(&loose.trailer)?;
                } else {
                    let (kind, len) = self.read_object(slab, row)?;
                    let mut header = [0; 32];
                    let n = crate::pack::loose_header(kind, len as u64, &mut header);
                    let n = self.deflate.deflate_parts(
                        &[
                            header.get(..n).ok_or(Error::Invalid("loose header"))?,
                            self.a.bytes(),
                        ],
                        self.compressed.buffer(),
                    )?;
                    self.started = true;
                    head(
                        stream,
                        200,
                        crate::gpre::CONTENT_TYPE_LOOSE_OBJECT,
                        Some(n as u64),
                        request.close,
                    )?;
                    stream.write_all(
                        self.compressed
                            .buffer()
                            .get(..n)
                            .ok_or(Error::Invalid("loose output"))?,
                    )?;
                }
                Ok(())
            }
            Route::Objects => {
                self.oids.clear();
                let info = crate::json::scan_objects_request(body, |oid| {
                    push(&mut self.oids, oid).is_ok()
                })
                .map_err(|_| Error::Invalid("object request JSON or capacity"))?;
                if self.oids.is_empty() {
                    return Err(Error::Invalid("objectIds must not be empty"));
                }
                self.objects_plan(&snapshot, info.commit_depth)?;
                self.started = true;
                head(
                    stream,
                    200,
                    crate::gpre::CONTENT_TYPE_PACKFILE,
                    Some(self.plan.pack_len),
                    request.close,
                )?;
                splice_slab_ranges(&self.plan, stream, self.range.buffer(), 64 << 10)?;
                Ok(())
            }
            Route::Sizes => {
                self.oids.clear();
                crate::json::scan_oid_array(body, |oid| push(&mut self.oids, oid).is_ok())
                    .map_err(|_| Error::Invalid("sizes JSON or capacity"))?;
                // Check all OIDs before sending a successful status.
                for oid in &self.oids {
                    crate::slab::find(&snapshot.active, oid)?.ok_or(Error::Missing(*oid))?;
                }
                self.started = true;
                head(stream, 200, "application/json", None, request.close)?;
                let mut chunked = Chunked(stream);
                chunked.write_all(b"[")?;
                for (i, oid) in self.oids.iter().enumerate() {
                    let (s, r) =
                        crate::slab::find(&snapshot.active, oid)?.ok_or(Error::Missing(*oid))?;
                    let row = snapshot
                        .active
                        .get(s)
                        .ok_or(Error::Invalid("slab"))?
                        .index
                        .record(r)?;
                    let mut buffer = [0; 128];
                    let mut w = ByteWriter::new(&mut buffer);
                    if i != 0 {
                        w.put(b",")?;
                    }
                    w.put(b"{\"Id\":\"")?;
                    w.put(&oid.to_hex())?;
                    w.put(b"\",\"Size\":")?;
                    w.put_dec(row.size() as u64)?;
                    w.put(b"}")?;
                    chunked.write_all(w.written())?;
                }
                chunked.write_all(b"]")?;
                chunked.finish()?;
                Ok(())
            }
            Route::Refs => {
                let mut buffer = [0; 2048];
                let mut w = ByteWriter::new(&mut buffer);
                pkt(&mut w, b"# service=git-upload-pack\n")?;
                w.put(b"0000")?;
                let mut line = [0; 1024];
                let mut l = ByteWriter::new(&mut line);
                l.put(&snapshot.catalog.tip().to_hex())?;
                l.put(b" HEAD\0side-band-64k ofs-delta symref=HEAD:")?;
                l.put(shared.branch.as_bytes())?;
                l.put(b" agent=gvfs-daemon\n")?;
                pkt(&mut w, l.written())?;
                let mut l = ByteWriter::new(&mut line);
                l.put(&snapshot.catalog.tip().to_hex())?;
                l.put(b" ")?;
                l.put(shared.branch.as_bytes())?;
                l.put(b"\n")?;
                pkt(&mut w, l.written())?;
                w.put(b"0000")?;
                self.fixed(
                    stream,
                    200,
                    "application/x-git-upload-pack-advertisement",
                    w.written(),
                    request.close,
                )
            }
            Route::UploadPack => {
                self.oids.clear();
                let mut data = body;
                let mut done = false;
                let mut sideband = false;
                while !data.is_empty() {
                    let n = parse_hex(data.get(..4).ok_or(Error::Invalid("pkt-line"))?)?;
                    data = data.get(4..).ok_or(Error::Invalid("pkt-line"))?;
                    if n == 0 {
                        continue;
                    }
                    if n < 4 {
                        return Err(Error::Invalid("pkt-line control"));
                    }
                    let line = data
                        .get(..n.saturating_sub(4))
                        .ok_or(Error::Invalid("pkt-line length"))?;
                    data = data
                        .get(n.saturating_sub(4)..)
                        .ok_or(Error::Invalid("pkt-line length"))?;
                    if let Some(want) = line.strip_prefix(b"want ") {
                        let oid = Oid20::from_hex(want.get(..40).ok_or(Error::Invalid("want"))?)
                            .ok_or(Error::Invalid("want OID"))?;
                        push(&mut self.oids, oid)?;
                        sideband |= want
                            .windows(b"side-band-64k".len())
                            .any(|s| s == b"side-band-64k");
                    } else if line == b"done\n" || line == b"done" {
                        done = true;
                    } else if !line.starts_with(b"have ") {
                        return Err(Error::Invalid("unsupported Git fetch request"));
                    }
                }
                if !done {
                    return self.fixed(
                        stream,
                        200,
                        "application/x-git-upload-pack-result",
                        b"0008NAK\n",
                        request.close,
                    );
                }
                if self.oids.is_empty() {
                    return Err(Error::Invalid("fetch without wants"));
                }
                let tip_only = self.oids.as_slice() == [snapshot.catalog.tip()];
                if !tip_only {
                    self.objects_plan(&snapshot, u64::MAX)?;
                }
                let plan = if tip_only { &snapshot.cold } else { &self.plan };
                self.started = true;
                head(
                    stream,
                    200,
                    "application/x-git-upload-pack-result",
                    None,
                    request.close,
                )?;
                let mut chunks = Chunked(stream);
                chunks.write_all(b"0008NAK\n")?;
                if sideband {
                    let mut side = Sideband(&mut chunks);
                    splice_slab_ranges(plan, &mut side, self.range.buffer(), 64 << 10)?;
                    chunks.write_all(b"0000")?;
                } else {
                    splice_slab_ranges(plan, &mut chunks, self.range.buffer(), 64 << 10)?;
                }
                chunks.finish()?;
                Ok(())
            }
            Route::Unknown => self.fixed(stream, 404, "text/plain", b"not found\n", request.close),
        }
    }
    fn fixed(
        &mut self,
        stream: &mut impl Write,
        status: u16,
        kind: &str,
        bytes: &[u8],
        close: bool,
    ) -> Result<()> {
        self.started = true;
        head(stream, status, kind, Some(bytes.len() as u64), close)?;
        stream.write_all(bytes)?;
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Route {
    Health,
    Metrics,
    Config,
    Prefetch(u64),
    Object(Oid20),
    Objects,
    Sizes,
    Refs,
    UploadPack,
    Unknown,
}
struct Request {
    route: Route,
    header: usize,
    length: usize,
    chunked: bool,
    close: bool,
    expect: bool,
    if_none_match: IfNoneMatch,
}

struct IfNoneMatch {
    any: bool,
    tags: [Oid20; 16],
    len: usize,
}

impl IfNoneMatch {
    fn new() -> Self {
        Self {
            any: false,
            tags: [Oid20([0; 20]); 16],
            len: 0,
        }
    }
    fn add(&mut self, value: &[u8]) -> Result<()> {
        for tag in value.split(|&b| b == b',') {
            let tag = tag.trim_ascii();
            if tag == b"*" {
                self.any = true;
                continue;
            }
            let tag = tag.strip_prefix(b"W/").unwrap_or(tag);
            let Some(hex) = tag.strip_prefix(b"\"").and_then(|s| s.strip_suffix(b"\"")) else {
                continue;
            };
            let Some(oid) = Oid20::from_hex(hex) else {
                continue;
            };
            if oid.to_hex() != hex {
                continue;
            } // ETags are case-sensitive opaque values.
            *self
                .tags
                .get_mut(self.len)
                .ok_or(Error::Capacity("If-None-Match tags"))? = oid;
            self.len = self.len.saturating_add(1);
        }
        Ok(())
    }
    fn matches(&self, tag: Oid20) -> bool {
        self.any
            || self
                .tags
                .get(..self.len)
                .is_some_and(|tags| tags.contains(&tag))
    }
}

fn parse_request(bytes: &[u8]) -> Result<Option<Request>> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut r = httparse::Request::new(&mut headers);
    let header = match r.parse(bytes).map_err(|_| Error::Invalid("HTTP headers"))? {
        httparse::Status::Partial => return Ok(None),
        httparse::Status::Complete(n) => n,
    };
    if header > 16384 {
        return Err(Error::Capacity("HTTP headers"));
    }
    let path = r.path.ok_or(Error::Invalid("HTTP path"))?;
    let method = r.method.ok_or(Error::Invalid("HTTP method"))?;
    let route = if method == "GET" {
        if path == "/healthz" {
            Route::Health
        } else if path == "/metrics" {
            Route::Metrics
        } else if path.ends_with("/gvfs/config") {
            Route::Config
        } else if let Some((_, tail)) = path.rsplit_once("/gvfs/prefetch") {
            let ts = if tail.is_empty() {
                0
            } else {
                parse_dec_u64(
                    tail.strip_prefix("?lastPackTimestamp=")
                        .ok_or(Error::Invalid("prefetch query"))?
                        .as_bytes(),
                )
                .ok_or(Error::Invalid("prefetch timestamp"))?
            };
            Route::Prefetch(ts)
        } else if let Some((_, hex)) = path.rsplit_once("/gvfs/objects/") {
            Route::Object(Oid20::from_hex(hex.as_bytes()).ok_or(Error::Invalid("object OID"))?)
        } else if path.ends_with("/info/refs?service=git-upload-pack") {
            Route::Refs
        } else {
            Route::Unknown
        }
    } else if method == "POST" {
        if path.ends_with("/gvfs/objects") {
            Route::Objects
        } else if path.ends_with("/gvfs/sizes") {
            Route::Sizes
        } else if path.ends_with("/git-upload-pack") {
            Route::UploadPack
        } else {
            Route::Unknown
        }
    } else {
        Route::Unknown
    };
    let mut length = None;
    let mut chunked = false;
    let mut close = r.version != Some(1);
    let mut expect = false;
    let mut if_none_match = IfNoneMatch::new();
    for h in r.headers {
        if h.name.eq_ignore_ascii_case("if-none-match") {
            if_none_match.add(h.value)?;
        } else if h.name.eq_ignore_ascii_case("content-length") {
            if length.is_some() {
                return Err(Error::Invalid("duplicate content-length"));
            }
            length = Some(
                usize::try_from(parse_dec_u64(h.value).ok_or(Error::Invalid("content-length"))?)
                    .map_err(|_| Error::Capacity("body"))?,
            );
        } else if h.name.eq_ignore_ascii_case("transfer-encoding") {
            if chunked || !h.value.eq_ignore_ascii_case(b"chunked") {
                return Err(Error::Invalid("transfer encoding"));
            }
            chunked = true;
        } else if h.name.eq_ignore_ascii_case("connection")
            && h.value.eq_ignore_ascii_case(b"close")
        {
            close = true;
        } else if h.name.eq_ignore_ascii_case("expect") {
            if !h.value.eq_ignore_ascii_case(b"100-continue") {
                return Err(Error::Invalid("expectation"));
            }
            expect = true;
        }
    }
    if chunked && length.is_some() {
        return Err(Error::Invalid("ambiguous HTTP framing"));
    }
    Ok(Some(Request {
        route,
        header,
        length: length.unwrap_or(0),
        chunked,
        close,
        expect,
        if_none_match,
    }))
}

fn read_more(stream: &mut TcpStream, bytes: &mut [u8], used: &mut usize) -> Result<()> {
    let space = bytes
        .get_mut(*used..)
        .filter(|b| !b.is_empty())
        .ok_or(Error::Capacity("HTTP request"))?;
    let n = stream.read(space)?;
    if n == 0 {
        return Err(Error::Invalid("truncated HTTP request"));
    }
    *used = used.saturating_add(n);
    Ok(())
}

fn connection(
    mut stream: TcpStream,
    receive: &mut Arena,
    worker: &mut Worker,
    shared: &Shared,
    timeout: Duration,
) -> Result<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(15)))?;
    stream.set_write_timeout(Some(Duration::from_secs(15)))?;
    let mut used = 0usize;
    loop {
        if used == 0 {
            used = stream.read(receive.buffer())?;
            if used == 0 {
                return Ok(());
            }
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(Error::Invalid("request timeout"))?;
        let request = loop {
            if let Some(request) = parse_request(
                receive
                    .buffer()
                    .get(..used)
                    .ok_or(Error::Invalid("request bytes"))?,
            )? {
                break request;
            }
            if used >= 16384 || Instant::now() >= deadline {
                return Err(Error::Capacity("request headers or deadline"));
            }
            read_more(&mut stream, receive.buffer(), &mut used)?;
        };
        if request.expect {
            stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
        }
        let (body_end, consumed) = if request.chunked {
            let mut read = request.header;
            let mut write = request.header;
            loop {
                let line_end = loop {
                    if let Some(i) = receive
                        .buffer()
                        .get(read..used)
                        .ok_or(Error::Invalid("chunk"))?
                        .windows(2)
                        .position(|p| p == b"\r\n")
                    {
                        break read.saturating_add(i);
                    }
                    if used.saturating_sub(read) > 32 || Instant::now() >= deadline {
                        return Err(Error::Invalid("chunk header"));
                    }
                    read_more(&mut stream, receive.buffer(), &mut used)?;
                };
                let n = parse_hex(
                    receive
                        .buffer()
                        .get(read..line_end)
                        .ok_or(Error::Invalid("chunk"))?,
                )?;
                read = line_end.saturating_add(2);
                let end = read
                    .checked_add(n)
                    .and_then(|n| n.checked_add(2))
                    .filter(|&n| n <= receive.capacity())
                    .ok_or(Error::Capacity("chunked body"))?;
                while used < end {
                    if Instant::now() >= deadline {
                        return Err(Error::Invalid("request deadline"));
                    }
                    read_more(&mut stream, receive.buffer(), &mut used)?;
                }
                if receive.buffer().get(end.saturating_sub(2)..end) != Some(b"\r\n") {
                    return Err(Error::Invalid("chunk terminator or unsupported trailers"));
                }
                if n == 0 {
                    break (write, end);
                }
                receive
                    .buffer()
                    .copy_within(read..read.saturating_add(n), write);
                write = write.checked_add(n).ok_or(Error::Capacity("body"))?;
                read = end;
            }
        } else {
            let end = request
                .header
                .checked_add(request.length)
                .filter(|&n| n <= receive.capacity())
                .ok_or(Error::Capacity("HTTP body"))?;
            while used < end {
                if Instant::now() >= deadline {
                    return Err(Error::Invalid("request deadline"));
                }
                read_more(&mut stream, receive.buffer(), &mut used)?;
            }
            (end, end)
        };
        let mut writer = Deadline {
            stream: &mut stream,
            deadline,
        };
        let result = worker.handle(
            &request,
            receive
                .buffer()
                .get(request.header..body_end)
                .ok_or(Error::Invalid("body bounds"))?,
            shared,
            &mut writer,
        );
        if let Err(e) = result {
            if !worker.started {
                let status = match e {
                    Error::Missing(_) => 404,
                    Error::Capacity(_) => 413,
                    _ => 503,
                };
                head(&mut writer, status, "text/plain", Some(15), true)?;
                writer.write_all(b"request failed\n")?;
            }
            return Err(e);
        }
        if request.close {
            return Ok(());
        }
        receive.buffer().copy_within(consumed..used, 0);
        used = used.saturating_sub(consumed);
    }
}

#[derive(Clone, Copy)]
struct PackCoverage {
    start: u64,
    end: u64,
    baseline: bool,
    live: bool,
}

#[derive(Clone, Copy)]
struct PrefetchHeaders {
    etag: Oid20,
    tip: Oid20,
    coverage: Option<PackCoverage>,
}

fn head(
    out: &mut impl Write,
    status: u16,
    kind: &str,
    len: Option<u64>,
    close: bool,
) -> Result<()> {
    head_with_metadata(out, status, kind, len, close, None)
}

fn head_with_metadata(
    out: &mut impl Write,
    status: u16,
    kind: &str,
    len: Option<u64>,
    close: bool,
    metadata: Option<PrefetchHeaders>,
) -> Result<()> {
    let mut bytes = [0; 1024];
    let mut w = ByteWriter::new(&mut bytes);
    w.put(b"HTTP/1.1 ")?;
    w.put_dec(u64::from(status))?;
    w.put(match status {
        200 => b" OK\r\n",
        304 => b" Not Modified\r\n",
        404 => b" Not Found\r\n",
        413 => b" Payload Too Large\r\n",
        _ => b" Service Unavailable\r\n",
    })?;
    if status != 304 {
        w.put(b"Content-Type: ")?;
        w.put(kind.as_bytes())?;
        w.put(b"\r\n")?;
    }
    if status == 304 {
        // A 304 has no body and must not advertise chunked transfer encoding.
    } else if let Some(n) = len {
        w.put(b"Content-Length: ")?;
        w.put_dec(n)?;
        w.put(b"\r\n")?;
    } else {
        w.put(b"Transfer-Encoding: chunked\r\n")?;
    }
    if let Some(metadata) = metadata {
        w.put(b"ETag: \"")?;
        w.put(&metadata.etag.to_hex())?;
        w.put(b"\"\r\nCache-Control: private, max-age=0, must-revalidate\r\n")?;
        if status != 304 {
            w.put(b"X-GVFS-Tip: ")?;
            w.put(&metadata.tip.to_hex())?;
            w.put(b"\r\n")?;
        }
    }
    if let Some(coverage) = metadata.and_then(|m| m.coverage) {
        // These bounds describe sealed progress, not the birth times of delta bases or L0.
        w.put(b"X-GVFS-Pack-Start: ")?;
        w.put_dec(coverage.start)?;
        w.put(b"\r\nX-GVFS-Pack-End: ")?;
        w.put_dec(coverage.end)?;
        w.put(b"\r\nX-GVFS-Pack-Mode: ")?;
        w.put(if coverage.baseline {
            b"baseline"
        } else {
            b"incremental"
        })?;
        w.put(b"\r\nX-GVFS-Pack-Contains-Live: ")?;
        w.put(if coverage.live { b"true" } else { b"false" })?;
        w.put(b"\r\n")?;
    }
    w.put(if close {
        b"Connection: close\r\n\r\n"
    } else {
        b"Connection: keep-alive\r\n\r\n"
    })?;
    out.write_all(w.written())?;
    Ok(())
}

fn copy_range(
    reader: &crate::storage::RangeReader,
    mut offset: u64,
    mut len: u64,
    out: &mut Deadline<'_>,
    buffer: &mut [u8],
) -> Result<()> {
    #[cfg(target_os = "linux")]
    if let crate::storage::RangeReader::File(file) = reader {
        let mut position = i64::try_from(offset).map_err(|_| Error::Capacity("sendfile offset"))?;
        while len > 0 {
            if Instant::now() >= out.deadline {
                return Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into());
            }
            let n = match nix::sys::sendfile::sendfile(
                &*out.stream,
                file,
                Some(&mut position),
                len.min(0x7fff_f000) as usize,
            ) {
                Ok(n) => n,
                Err(nix::errno::Errno::EINTR) => continue,
                Err(e) => return Err(std::io::Error::from_raw_os_error(e as i32).into()),
            };
            if n == 0 {
                return Err(Error::Invalid("short sendfile source"));
            }
            len = len.saturating_sub(n as u64);
        }
        return Ok(());
    }
    while len > 0 {
        let n = len.min(buffer.len() as u64) as usize;
        let bytes = buffer.get_mut(..n).ok_or(Error::Capacity("range"))?;
        reader.read(offset, bytes)?;
        out.write_all(bytes)?;
        offset = offset.saturating_add(n as u64);
        len = len.saturating_sub(n as u64);
    }
    Ok(())
}
fn parse_hex(bytes: &[u8]) -> Result<usize> {
    if bytes.is_empty() {
        return Err(Error::Invalid("empty hex integer"));
    }
    let mut value = 0usize;
    for &b in bytes {
        let d = match b {
            b'0'..=b'9' => b.wrapping_sub(b'0'),
            b'a'..=b'f' => b.wrapping_sub(b'a').wrapping_add(10),
            b'A'..=b'F' => b.wrapping_sub(b'A').wrapping_add(10),
            _ => return Err(Error::Invalid("hex integer")),
        };
        value = value
            .checked_mul(16)
            .and_then(|n| n.checked_add(usize::from(d)))
            .ok_or(Error::Invalid("hex overflow"))?;
    }
    Ok(value)
}
fn hex4(n: usize) -> Result<[u8; 4]> {
    if n > 65535 {
        return Err(Error::Capacity("pkt-line"));
    }
    let mut out = [0; 4];
    let mut v = n;
    for b in out.iter_mut().rev() {
        *b = *b"0123456789abcdef"
            .get(v & 15)
            .ok_or(Error::Invalid("hex"))?;
        v >>= 4;
    }
    Ok(out)
}
fn pkt(out: &mut ByteWriter<'_>, bytes: &[u8]) -> Result<()> {
    out.put(&hex4(bytes.len().saturating_add(4))?)?;
    out.put(bytes)?;
    Ok(())
}
struct Chunked<'a, W: Write>(&'a mut W);
impl<W: Write> Chunked<'_, W> {
    fn finish(&mut self) -> std::io::Result<()> {
        self.0.write_all(b"0\r\n\r\n")
    }
}
impl<W: Write> Write for Chunked<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        write!(self.0, "{:x}\r\n", bytes.len())?;
        self.0.write_all(bytes)?;
        self.0.write_all(b"\r\n")?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}
struct Sideband<'a, W: Write>(&'a mut W);
impl<W: Write> Write for Sideband<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        for part in bytes.chunks(65515) {
            let header = hex4(part.len().saturating_add(5)).map_err(std::io::Error::other)?;
            self.0.write_all(&header)?;
            self.0.write_all(&[1])?;
            self.0.write_all(part)?;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}
struct Deadline<'a> {
    stream: &'a mut TcpStream,
    deadline: Instant,
}
impl Write for Deadline<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if Instant::now() >= self.deadline {
            return Err(std::io::ErrorKind::TimedOut.into());
        }
        self.stream.write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}
