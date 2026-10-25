//! Mode 2 & 3: `gvfs-daemon serve` / `edge` / `proxy` — Crash-recoverable runtime state
//! (`ServerRuntimeState`, PDF pp. 165–167) and zero-allocation HTTP/1.1 + HTTP/2 server.
//!
//! Crucially:
//! - `GET /gvfs/prefetch?lastPackTimestamp=T` selects slabs via the Two-Track manifest +
//!   `< 15m` `L0Ram` slab and splices them into **1 single deduplicated `.pack` + `.idx`**
//!   (`GPRE` with `pack_count = 1`, or `0` if up to date).
//! - `POST /gvfs/objects` looks up requested OIDs (and commit -> tree closures when
//!   `commitDepth > 0`) via $O(1)$ MPHF / mmap indexes, coalesces range reads, and returns
//!   **1 single deduplicated `.pack` stream**.
//! - Every steady-state request executes inside a pre-allocated [`WorkerArena`] with
//!   zero heap allocations after `gvfs_alloc_guard::seal()`.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use http::header::{CACHE_CONTROL, CONTENT_TYPE};
use http::{HeaderValue, Method, Request, Response, StatusCode};
use http_body::{Body, Frame, SizeHint};
use hyper::body::Incoming;
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use gvfs_core::arena::{ScratchArena, SeenOidSet};
use gvfs_core::buf::{ByteWriter, bm_try_extend};
use gvfs_core::gpre::{
    CONTENT_TYPE_LOOSE_OBJECT, CONTENT_TYPE_PACKFILE, CONTENT_TYPE_PREFETCH, gpre_file_header,
    gpre_pack_header,
};
use gvfs_core::idx::{IdxEntry, idx_len, write_idx_v2};
use gvfs_core::json::{scan_objects_request, scan_oid_array};
use gvfs_core::object::{TreeIter, parse_commit};
use gvfs_core::pack::{ObjType, parse_entry_header};
use gvfs_core::slab::{
    SpliceArena, Tier, assemble_spliced_pack, plan_deduplicated_range_splice,
    splice_in_memory_slabs,
};
use gvfs_core::zlib::{Inflater, LooseSplice, inflate_stored_entry};
use gvfs_core::{Oid20, Slab, UnifiedManifest};
use gvfs_storage::{ETag, StorageClient, mmap_existing_git_pack};

use crate::ingest::{IngestArena, ingest_git_delta_with_arena};
use crate::pipeline::DaemonError;
use crate::upstream::GitUpstream;

/// Represents the live, crash-recoverable state of `gvfs-daemon serve`
/// (identical for both `localhost` proxy and remote `gvfs-edge`, PDF p. 165).
#[derive(Debug)]
pub struct ServerRuntimeState {
    pub manifest_etag: ETag,
    pub manifest: UnifiedManifest,
    /// Holds all commits between `manifest.last_15m_tip_oid` and current upstream `HEAD`.
    pub l0_ram_slab: Option<Arc<Slab>>,
    ingest_arena: IngestArena,
}

impl ServerRuntimeState {
    pub fn from_parts(
        manifest_etag: ETag,
        manifest: UnifiedManifest,
        l0_ram_slab: Option<Arc<Slab>>,
    ) -> Self {
        Self {
            manifest_etag,
            manifest,
            l0_ram_slab,
            ingest_arena: IngestArena::default(),
        }
    }

    /// COLD BOOT / CRASH RECOVERY SEQUENCE FOR `gvfs-daemon serve` (PDF pp. 165–166):
    /// Reconstructs all in-memory state before the HTTP port opens.
    pub fn boot_or_recover(
        storage: &StorageClient,
        ado: &dyn GitUpstream,
        now_ts: u64,
    ) -> Result<Self, DaemonError> {
        let mut ingest_arena = IngestArena::default();

        // 1. Load the atomic manifest from S3/Azure Blob/storage
        let (manifest, etag) = storage
            .get_manifest_with_etag()?
            .ok_or(DaemonError::ManifestNotBootstrappedYet)?;

        // 2. Pre-warm `.mphf` pilot + descriptor tables into RAM (~40-80 MB total).
        storage.warm_mphf_indexes(&manifest)?;

        // 3. Reconstruct lost `< 15m` RAM state if ADO has moved past the last 15m slab
        let current_ado_head = ado.get_main_head_oid()?;
        let l0_ram_slab = if current_ado_head != manifest.last_15m_tip_oid {
            let fallback_haves = manifest.fallback_have_commits();
            let recovered_l0 = ingest_git_delta_with_arena(
                ado,
                manifest.last_15m_tip_oid,
                current_ado_head,
                &fallback_haves,
                Tier::L0Ram,
                now_ts,
                &mut ingest_arena,
            )?;
            Some(Arc::new(recovered_l0))
        } else {
            None
        };

        Ok(Self {
            manifest_etag: etag,
            manifest,
            l0_ram_slab,
            ingest_arena,
        })
    }

    /// Boots directly from an existing local Git repository's single `.pack` + `.idx` + `.rev`
    /// via zero-copy OS `mmap` (e.g. `/home/muks/SearchGold`) in < 1 ms!
    pub fn boot_from_local_git_pack(
        git_dir: &Path,
        ado: &dyn GitUpstream,
        now_ts: u64,
    ) -> Result<Self, DaemonError> {
        let current_head = ado.get_main_head_oid()?;
        let slab = mmap_existing_git_pack(git_dir, current_head, now_ts)?.ok_or_else(|| {
            DaemonError::Config(format!("no .pack/.idx found in {}", git_dir.display()))
        })?;
        let baseline = Arc::new(slab);
        let manifest = UnifiedManifest {
            version_seq: 1,
            last_15m_tip_oid: current_head,
            weekly_baseline: baseline,
            weekly_deltas: Vec::new(),
            sub_week_pyramid: Vec::new(),
        };
        Ok(Self {
            manifest_etag: ETag("\"mmap-local-pack-1\"".into()),
            manifest,
            l0_ram_slab: None,
            ingest_arena: IngestArena::default(),
        })
    }

    /// Runs every 2 seconds (or on-demand during a request) to keep `< 15m` freshness
    /// without re-fetching already-ingested L0 commits from ADO (PDF pp. 166–167).
    pub fn tick_live_sync(
        &mut self,
        storage: &StorageClient,
        ado: &dyn GitUpstream,
        now_ts: u64,
    ) -> Result<(), DaemonError> {
        // A. Check if the 15m ADO Pipeline published a new `manifest.bin`
        if let Some((new_manifest, new_etag)) =
            storage.get_manifest_if_changed(&self.manifest_etag)?
        {
            storage.warm_mphf_indexes(&new_manifest)?;
            self.manifest = new_manifest;
            self.manifest_etag = new_etag;
            // Reset L0 RAM slab since those commits are now sealed in storage!
            self.l0_ram_slab = None;
        }

        // B. Check if ADO `HEAD` advanced past our current L0 tip (or manifest tip)
        let current_ado_head = ado.get_main_head_oid()?;
        let our_latest_tip = self
            .l0_ram_slab
            .as_ref()
            .map_or(self.manifest.last_15m_tip_oid, |s| s.tip_commit);

        if current_ado_head != our_latest_tip {
            let fallback_haves = self.manifest.fallback_have_commits();
            // Pull ONLY the tiny 2-second delta (`our_latest_tip -> current_ado_head`)
            let micro_delta = Arc::new(ingest_git_delta_with_arena(
                ado,
                our_latest_tip,
                current_ado_head,
                &fallback_haves,
                Tier::L0Ram,
                now_ts,
                &mut self.ingest_arena,
            )?);

            // Merge `micro_delta` into `l0_ram_slab` using our exact same splice primitive!
            self.l0_ram_slab = match self.l0_ram_slab.take() {
                None => Some(micro_delta),
                Some(prev_l0) => {
                    let merged = splice_in_memory_slabs(
                        &[micro_delta, prev_l0],
                        current_ado_head,
                        Tier::L0Ram,
                        now_ts,
                    )
                    .ok_or(DaemonError::SlabBuildFailed)?;
                    Some(Arc::new(merged))
                }
            };
        }

        Ok(())
    }
}

/// Pre-allocated per-worker arena leased during HTTP request handling.
#[derive(Debug)]
pub struct WorkerArena {
    pub scratch: ScratchArena,
    pub splice: SpliceArena,
    pub out_buf: BytesMut,
    out_cap: usize,
    pub inflater: Inflater,
    pub obj_scratch: Vec<u8>,
    pub commit_queue: Vec<(Oid20, u64)>,
    pub tree_queue: Vec<Oid20>,
    pub closure_seen: SeenOidSet,
}

impl WorkerArena {
    pub fn new(
        scratch_bytes: usize,
        max_oids: usize,
        pack_cap: usize,
        idx_cap: usize,
        out_cap: usize,
        max_obj_bytes: usize,
    ) -> Self {
        let mut out_buf = BytesMut::with_capacity(out_cap);
        out_buf.resize(out_cap, 0);
        out_buf.clear();
        drop(out_buf.split_to(0));

        let mut obj_scratch = vec![0u8; max_obj_bytes];
        gvfs_core::buf::prefault(&mut obj_scratch);

        let mut commit_queue = Vec::with_capacity(max_oids);
        gvfs_core::buf::prefault_capacity(&mut commit_queue, (Oid20::ZERO, 0));
        let mut tree_queue = Vec::with_capacity(max_oids);
        gvfs_core::buf::prefault_capacity(&mut tree_queue, Oid20::ZERO);

        Self {
            scratch: ScratchArena::with_capacity(scratch_bytes, max_oids),
            splice: SpliceArena::new(18, max_oids, pack_cap, idx_cap),
            out_buf,
            out_cap,
            inflater: Inflater::new(),
            obj_scratch,
            commit_queue,
            tree_queue,
            closure_seen: SeenOidSet::with_capacity_log2(16),
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        self.scratch.reset();
        self.splice.reset();
        self.out_buf.clear();
        let _ = self.out_buf.try_reclaim(self.out_cap);
        self.inflater.reset();
        self.commit_queue.clear();
        self.tree_queue.clear();
        self.closure_seen.clear();
    }

    #[inline]
    pub fn emit_slice(&mut self, data: &[u8]) -> Option<Bytes> {
        if self.out_buf.capacity().saturating_sub(self.out_buf.len()) < data.len()
            && self.out_buf.is_empty()
        {
            let _ = self.out_buf.try_reclaim(self.out_cap);
        }
        bm_try_extend(&mut self.out_buf, data).ok()?;
        Some(self.out_buf.split().freeze())
    }
}

#[derive(Debug)]
pub struct WorkerPool {
    free: Mutex<Vec<WorkerArena>>,
    sem: Arc<Semaphore>,
}

impl WorkerPool {
    pub fn new(
        count: usize,
        scratch_bytes: usize,
        max_oids: usize,
        pack_cap: usize,
        idx_cap: usize,
        out_cap: usize,
        max_obj_bytes: usize,
    ) -> Arc<Self> {
        let arenas: Vec<WorkerArena> = (0..count)
            .map(|_| {
                WorkerArena::new(
                    scratch_bytes,
                    max_oids,
                    pack_cap,
                    idx_cap,
                    out_cap,
                    max_obj_bytes,
                )
            })
            .collect();
        Arc::new(Self {
            free: Mutex::new(arenas),
            sem: Arc::new(Semaphore::new(count)),
        })
    }

    pub async fn lease(self: &Arc<Self>) -> Option<WorkerGuard> {
        let permit = Arc::clone(&self.sem).acquire_owned().await.ok()?;
        let arena = self
            .free
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop()?;
        Some(WorkerGuard {
            arena: Some(arena),
            pool: Arc::clone(self),
            _permit: permit,
        })
    }
}

pub struct WorkerGuard {
    arena: Option<WorkerArena>,
    pool: Arc<WorkerPool>,
    _permit: OwnedSemaphorePermit,
}

impl WorkerGuard {
    #[inline]
    pub fn get_mut(&mut self) -> Option<&mut WorkerArena> {
        self.arena.as_mut()
    }
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        if let Some(mut a) = self.arena.take() {
            a.reset();
            self.pool
                .free
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(a);
        }
    }
}

/// Zero-allocation multi-chunk HTTP response body (up to 6 zero-copy `Bytes` slices).
#[derive(Debug, Default)]
pub struct SlabBody {
    chunks: [Option<Bytes>; 6],
    cursor: usize,
    total_len: u64,
}

impl SlabBody {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn single(b: Bytes) -> Self {
        let total_len = u64::try_from(b.len()).unwrap_or(0);
        Self {
            chunks: [Some(b), None, None, None, None, None],
            cursor: 0,
            total_len,
        }
    }

    pub fn from_chunks(chunks: [Option<Bytes>; 6]) -> Self {
        let total_len = chunks.iter().flatten().fold(0u64, |acc, c| {
            acc.saturating_add(u64::try_from(c.len()).unwrap_or(0))
        });
        Self {
            chunks,
            cursor: 0,
            total_len,
        }
    }
}

impl Body for SlabBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        while self.cursor < self.chunks.len() {
            let idx = self.cursor;
            self.cursor = self.cursor.saturating_add(1);
            if let Some(slot) = self.chunks.get_mut(idx) {
                if let Some(bytes) = slot.take() {
                    if !bytes.is_empty() {
                        return Poll::Ready(Some(Ok(Frame::data(bytes))));
                    }
                }
            }
        }
        Poll::Ready(None)
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.total_len)
    }
}

#[derive(Debug, Default)]
pub struct DaemonCounters {
    pub requests_total: AtomicU64,
    pub prefetch_requests: AtomicU64,
    pub objects_post_requests: AtomicU64,
    pub objects_get_requests: AtomicU64,
    pub sizes_requests: AtomicU64,
    pub bytes_served: AtomicU64,
}

/// Shared state for the HTTP/1.1 + HTTP/2 server (`serve` / `edge` / `proxy`).
pub struct DaemonHttpState {
    pub runtime: RwLock<ServerRuntimeState>,
    pub storage: StorageClient,
    pub pool: Arc<WorkerPool>,
    pub config_json: Bytes,
    pub counters: DaemonCounters,
}

pub fn default_config_json(cache_url: &str, blob_url: &str) -> Bytes {
    let s = format!(
        "{{\"AllowedGvfsClientVersions\":[{{\"Min\":{{\"Major\":0,\"Minor\":1,\"Build\":0,\"Revision\":0}},\"Max\":null}}],\"CacheServers\":[{{\"Url\":\"{cache_url}\",\"Name\":\"local\",\"GlobalDefault\":true}}],\"BlobStores\":[{{\"Url\":\"{blob_url}\",\"Name\":\"cas\",\"GlobalDefault\":true}}]}}"
    );
    gvfs_core::buf::share_bytes(Bytes::from(s.into_bytes()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    Config,
    Prefetch,
    Objects,
    Object(Option<Oid20>),
    Sizes,
    Healthz,
    Metrics,
    NotFound,
}

fn classify_route(path: &str) -> Route {
    match path {
        "/healthz" => return Route::Healthz,
        "/metrics" => return Route::Metrics,
        _ => {}
    }
    let Some(rest) = path
        .find("/gvfs/")
        .and_then(|i| path.get(i.saturating_add(6)..))
    else {
        return Route::NotFound;
    };
    let rest = rest.trim_end_matches('/');
    match rest {
        "config" => Route::Config,
        "prefetch" => Route::Prefetch,
        "objects" => Route::Objects,
        "sizes" => Route::Sizes,
        _ => {
            if let Some(hex) = rest.strip_prefix("objects/") {
                return Route::Object(Oid20::from_hex(hex.as_bytes()));
            }
            Route::NotFound
        }
    }
}

fn parse_last_pack_timestamp(query: Option<&str>) -> Option<u64> {
    let q = query?;
    for kv in q.split('&') {
        if let Some(val) = kv.strip_prefix("lastPackTimestamp=") {
            return val.parse::<u64>().ok();
        }
    }
    None
}

struct Reply {
    status: StatusCode,
    content_type: &'static str,
    pack_range: Option<(u64, u64)>,
    body: SlabBody,
}

impl Reply {
    fn into_http_response(self) -> Response<SlabBody> {
        let mut r = Response::new(self.body);
        *r.status_mut() = self.status;
        let h = r.headers_mut();
        h.insert(CONTENT_TYPE, HeaderValue::from_static(self.content_type));
        h.insert(CACHE_CONTROL, HeaderValue::from_static("private"));
        if let Some((start_ts, end_ts)) = self.pack_range {
            let mut s_buf = [0u8; 20];
            let s_len = gvfs_core::buf::fmt_dec(start_ts, &mut s_buf);
            if let Some(s_slice) = s_buf.get(..s_len) {
                if let Ok(hv) = HeaderValue::from_bytes(s_slice) {
                    h.insert("x-gvfs-pack-start", hv);
                }
            }
            let mut e_buf = [0u8; 20];
            let e_len = gvfs_core::buf::fmt_dec(end_ts, &mut e_buf);
            if let Some(e_slice) = e_buf.get(..e_len) {
                if let Ok(hv) = HeaderValue::from_bytes(e_slice) {
                    h.insert("x-gvfs-pack-end", hv);
                }
            }
            let mut r_buf = [0u8; 44];
            let mut w = ByteWriter::new(&mut r_buf);
            if w.put_dec(start_ts).is_ok() && w.put_u8(b'-').is_ok() && w.put_dec(end_ts).is_ok() {
                if let Ok(hv) = HeaderValue::from_bytes(w.written()) {
                    h.insert("x-gvfs-pack-range", hv);
                }
            }
        }
        r
    }
}

fn static_reply(status: StatusCode, msg: &'static str) -> Reply {
    Reply {
        status,
        content_type: "text/plain; charset=utf-8",
        pack_range: None,
        body: SlabBody::single(Bytes::from_static(msg.as_bytes())),
    }
}

pub async fn handle_http_request(
    state: Arc<DaemonHttpState>,
    req: Request<Incoming>,
) -> Result<Response<SlabBody>, Infallible> {
    state
        .counters
        .requests_total
        .fetch_add(1, Ordering::Relaxed);
    let reply = dispatch_request(&state, req).await;
    state
        .counters
        .bytes_served
        .fetch_add(reply.body.total_len, Ordering::Relaxed);
    Ok(reply.into_http_response())
}

async fn dispatch_request(state: &DaemonHttpState, req: Request<Incoming>) -> Reply {
    let route = classify_route(req.uri().path());
    let is_get = req.method() == Method::GET || req.method() == Method::HEAD;
    let is_post = req.method() == Method::POST;

    match route {
        Route::Config if is_get => {
            let _hot = gvfs_alloc_guard::hot_path();
            Reply {
                status: StatusCode::OK,
                content_type: "application/json",
                pack_range: None,
                body: SlabBody::single(state.config_json.clone()),
            }
        }
        Route::Prefetch if is_get => {
            state
                .counters
                .prefetch_requests
                .fetch_add(1, Ordering::Relaxed);
            let last_ts = parse_last_pack_timestamp(req.uri().query());
            handle_prefetch(state, last_ts).await
        }
        Route::Object(Some(oid)) if is_get => {
            state
                .counters
                .objects_get_requests
                .fetch_add(1, Ordering::Relaxed);
            handle_get_object(state, oid).await
        }
        Route::Object(None) => static_reply(StatusCode::BAD_REQUEST, "invalid object id\n"),
        Route::Objects if is_post => {
            state
                .counters
                .objects_post_requests
                .fetch_add(1, Ordering::Relaxed);
            handle_post_objects(state, req).await
        }
        Route::Sizes if is_post => {
            state
                .counters
                .sizes_requests
                .fetch_add(1, Ordering::Relaxed);
            handle_post_sizes(state, req).await
        }
        Route::Healthz if is_get => static_reply(StatusCode::OK, "ok\n"),
        Route::Metrics if is_get => handle_metrics(state),
        Route::NotFound => static_reply(StatusCode::NOT_FOUND, "not found\n"),
        _ => static_reply(StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n"),
    }
}

/// `GET /gvfs/prefetch?lastPackTimestamp=T`
///
/// Selects slabs via Two-Track manifest + `< 15m` `L0Ram` slab, and splices them into
/// **AT MOST 1 SINGLE PACK** (`pack_count = 1`, or `0` if up to date)!
async fn handle_prefetch(state: &DaemonHttpState, client_last_ts: Option<u64>) -> Reply {
    let Some(mut guard) = state.pool.lease().await else {
        return static_reply(StatusCode::SERVICE_UNAVAILABLE, "all worker arenas busy\n");
    };

    let selected = {
        let rt = state
            .runtime
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rt.manifest
            .select_slabs_for_prefetch(client_last_ts, rt.l0_ram_slab.clone())
    };

    if selected.is_empty() {
        let _hot = gvfs_alloc_guard::hot_path();
        let Some(arena) = guard.get_mut() else {
            return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "arena error\n");
        };
        let hdr = gpre_file_header(0);
        let Some(hdr_bytes) = arena.emit_slice(&hdr) else {
            return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "framing overflow\n");
        };
        return Reply {
            status: StatusCode::OK,
            content_type: CONTENT_TYPE_PREFETCH,
            pack_range: None,
            body: SlabBody::single(hdr_bytes),
        };
    }

    let min_ts = selected.iter().map(|s| s.start_ts).min().unwrap_or(0);
    let max_ts = selected.iter().map(|s| s.end_ts).max().unwrap_or(1);
    let all_in_memory = selected.iter().all(|s| s.in_memory_pack.is_some());

    if all_in_memory {
        let _hot = gvfs_alloc_guard::hot_path();
        let Some(arena) = guard.get_mut() else {
            return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "arena error\n");
        };
        let Some((pack_bytes, idx_bytes, _count)) =
            arena.splice.splice_local_slabs(&selected, None)
        else {
            return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "splice overflow\n");
        };
        // Emit 1 single pack in the GPRE envelope (`num_packs = 1`).
        let mut gpre_hdr = [0u8; 32];
        gpre_hdr
            .get_mut(..8)
            .unwrap_or(&mut [])
            .copy_from_slice(&gpre_file_header(1));
        gpre_hdr
            .get_mut(8..32)
            .unwrap_or(&mut [])
            .copy_from_slice(&gpre_pack_header(
                i64::try_from(max_ts).unwrap_or(1),
                i64::try_from(pack_bytes.len()).unwrap_or(0),
                i64::try_from(idx_bytes.len()).unwrap_or(-1),
            ));
        let Some(framing_bytes) = arena.emit_slice(&gpre_hdr) else {
            return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "framing overflow\n");
        };
        return Reply {
            status: StatusCode::OK,
            content_type: CONTENT_TYPE_PREFETCH,
            pack_range: Some((min_ts, max_ts)),
            body: SlabBody::from_chunks([
                Some(framing_bytes),
                Some(pack_bytes),
                Some(idx_bytes),
                None,
                None,
                None,
            ]),
        };
    }

    // Remote storage path: plan deduplicated range splice, fetch coalesced range batches,
    // and assemble 1 single pack + idx!
    let (unique_count, batches) = plan_deduplicated_range_splice(&selected, None, 4096);
    let Ok(range_buffers) = state.storage.fetch_range_batches(&batches, &selected) else {
        return static_reply(
            StatusCode::INTERNAL_SERVER_ERROR,
            "storage range fetch failed\n",
        );
    };
    let (pack_bytes, objects) = assemble_spliced_pack(unique_count, &range_buffers);
    let pack_checksum: [u8; 20] = if pack_bytes.len() >= 20 {
        pack_bytes
            .get(pack_bytes.len().saturating_sub(20)..)
            .and_then(|s| s.try_into().ok())
            .unwrap_or([0u8; 20])
    } else {
        [0u8; 20]
    };
    let mut idx_entries: Vec<IdxEntry> = objects
        .iter()
        .map(|o| IdxEntry {
            oid: o.oid,
            crc32: o.crc32,
            offset: o.offset,
        })
        .collect();
    let n_large = idx_entries
        .iter()
        .filter(|e| e.offset > 0x7FFF_FFFF)
        .count();
    let Some(req_idx_len) = idx_len(idx_entries.len(), n_large) else {
        return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "idx overflow\n");
    };
    let mut idx_buf = vec![0u8; req_idx_len];
    if write_idx_v2(&mut idx_entries, &pack_checksum, &mut idx_buf).is_err() {
        return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "idx build failed\n");
    }
    let idx_bytes = Bytes::from(idx_buf);

    let Some(arena) = guard.get_mut() else {
        return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "arena error\n");
    };
    let mut gpre_hdr = [0u8; 32];
    gpre_hdr
        .get_mut(..8)
        .unwrap_or(&mut [])
        .copy_from_slice(&gpre_file_header(1));
    gpre_hdr
        .get_mut(8..32)
        .unwrap_or(&mut [])
        .copy_from_slice(&gpre_pack_header(
            i64::try_from(max_ts).unwrap_or(1),
            i64::try_from(pack_bytes.len()).unwrap_or(0),
            i64::try_from(idx_bytes.len()).unwrap_or(-1),
        ));
    let Some(framing_bytes) = arena.emit_slice(&gpre_hdr) else {
        return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "framing overflow\n");
    };
    Reply {
        status: StatusCode::OK,
        content_type: CONTENT_TYPE_PREFETCH,
        pack_range: Some((min_ts, max_ts)),
        body: SlabBody::from_chunks([
            Some(framing_bytes),
            Some(pack_bytes),
            Some(idx_bytes),
            None,
            None,
            None,
        ]),
    }
}

/// `GET /gvfs/objects/<oid>` — Serves a single loose object spliced from the slab without
/// inflating or deflating its payload.
async fn handle_get_object(state: &DaemonHttpState, oid: Oid20) -> Reply {
    let Some(mut guard) = state.pool.lease().await else {
        return static_reply(StatusCode::SERVICE_UNAVAILABLE, "all worker arenas busy\n");
    };

    let active_slabs = {
        let rt = state
            .runtime
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rt.manifest.active_lookup_slabs(rt.l0_ram_slab.clone())
    };

    for slab in &active_slabs {
        let Some(meta) = slab.lookup_oid(&oid) else {
            continue;
        };
        if let Some(ref pack) = slab.in_memory_pack {
            let _hot = gvfs_alloc_guard::hot_path();
            let Some(arena) = guard.get_mut() else {
                return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "arena error\n");
            };
            let start = usize::try_from(meta.offset).unwrap_or(usize::MAX);
            let len = usize::try_from(meta.compressed_len).unwrap_or(usize::MAX);
            let Some(entry_end) = start.checked_add(len) else {
                return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "offset overflow\n");
            };
            let Some(raw_entry) = pack.get(start..entry_end) else {
                return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "corrupt entry slice\n");
            };
            let Some(hdr) = parse_entry_header(raw_entry).ok().flatten() else {
                return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "corrupt entry header\n");
            };
            if !hdr.kind.is_delta() {
                let Some(z_slice) = raw_entry.get(hdr.header_len..) else {
                    return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "corrupt zlib slice\n");
                };
                if let Ok(plan) = LooseSplice::plan(hdr.kind, hdr.size, z_slice) {
                    let body_start = start
                        .saturating_add(hdr.header_len)
                        .saturating_add(plan.body.start);
                    let body_end = start
                        .saturating_add(hdr.header_len)
                        .saturating_add(plan.body.end);
                    let Some(prefix_bytes) = arena.emit_slice(plan.prefix()) else {
                        return static_reply(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "framing overflow\n",
                        );
                    };
                    let Some(trailer_bytes) = arena.emit_slice(&plan.trailer) else {
                        return static_reply(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "framing overflow\n",
                        );
                    };
                    let body_bytes = pack.slice(body_start..body_end);
                    return Reply {
                        status: StatusCode::OK,
                        content_type: CONTENT_TYPE_LOOSE_OBJECT,
                        pack_range: None,
                        body: SlabBody::from_chunks([
                            Some(prefix_bytes),
                            Some(body_bytes),
                            Some(trailer_bytes),
                            None,
                            None,
                            None,
                        ]),
                    };
                }
            }
        } else {
            // Remote storage fallback: fetch the single entry range and loose-splice it.
            let obj_end = meta.offset.saturating_add(u64::from(meta.compressed_len));
            if let Ok(raw_entry) =
                state
                    .storage
                    .get_range_raw(&format!("{}.pack", slab.id), meta.offset, obj_end)
            {
                if let Some(hdr) = parse_entry_header(&raw_entry).ok().flatten() {
                    if let Some(z_slice) = raw_entry.get(hdr.header_len..) {
                        if let Ok(plan) = LooseSplice::plan(hdr.kind, hdr.size, z_slice) {
                            let Some(arena) = guard.get_mut() else {
                                return static_reply(
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "arena error\n",
                                );
                            };
                            let Some(prefix_bytes) = arena.emit_slice(plan.prefix()) else {
                                return static_reply(
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "framing overflow\n",
                                );
                            };
                            let Some(trailer_bytes) = arena.emit_slice(&plan.trailer) else {
                                return static_reply(
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "framing overflow\n",
                                );
                            };
                            let body_bytes = raw_entry.slice(
                                hdr.header_len.saturating_add(plan.body.start)
                                    ..hdr.header_len.saturating_add(plan.body.end),
                            );
                            return Reply {
                                status: StatusCode::OK,
                                content_type: CONTENT_TYPE_LOOSE_OBJECT,
                                pack_range: None,
                                body: SlabBody::from_chunks([
                                    Some(prefix_bytes),
                                    Some(body_bytes),
                                    Some(trailer_bytes),
                                    None,
                                    None,
                                    None,
                                ]),
                            };
                        }
                    }
                }
            }
        }
    }

    static_reply(StatusCode::NOT_FOUND, "object not found\n")
}

async fn read_body_into_arena(
    mut body: Incoming,
    arena: &mut WorkerArena,
) -> Result<gvfs_core::ArenaSpan, Reply> {
    let max_cap = arena.scratch.bytes_remaining();
    let (_, span) = arena
        .scratch
        .alloc_bytes(max_cap)
        .map_err(|_| static_reply(StatusCode::PAYLOAD_TOO_LARGE, "arena full\n"))?;
    let mut written = 0usize;

    while let Some(frame_res) = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await
    {
        let Ok(frame) = frame_res else {
            return Err(static_reply(
                StatusCode::BAD_REQUEST,
                "unreadable request body\n",
            ));
        };
        let Ok(data) = frame.into_data() else {
            continue;
        };
        let next_end = written
            .checked_add(data.len())
            .ok_or_else(|| static_reply(StatusCode::PAYLOAD_TOO_LARGE, "body too large\n"))?;
        let dst_all = arena
            .scratch
            .bytes_mut(span)
            .map_err(|_| static_reply(StatusCode::PAYLOAD_TOO_LARGE, "body too large\n"))?;
        let Some(dst) = dst_all.get_mut(written..next_end) else {
            return Err(static_reply(
                StatusCode::PAYLOAD_TOO_LARGE,
                "body too large\n",
            ));
        };
        dst.copy_from_slice(&data);
        written = next_end;
    }

    let used_u32 = u32::try_from(written)
        .map_err(|_| static_reply(StatusCode::PAYLOAD_TOO_LARGE, "body too large\n"))?;
    arena
        .scratch
        .shrink_last(span, used_u32)
        .map_err(|_| static_reply(StatusCode::INTERNAL_SERVER_ERROR, "span shrink failed\n"))
}

/// `POST /gvfs/objects` — Zero-alloc JSON scan + optional commit closure expansion +
/// range coalescing + pack splicing into **1 single deduplicated `.pack` stream**!
async fn handle_post_objects(state: &DaemonHttpState, req: Request<Incoming>) -> Reply {
    let Some(mut guard) = state.pool.lease().await else {
        return static_reply(StatusCode::SERVICE_UNAVAILABLE, "all worker arenas busy\n");
    };
    let Some(arena) = guard.get_mut() else {
        return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "arena error\n");
    };
    let body_span = match read_body_into_arena(req.into_body(), arena).await {
        Ok(s) => s,
        Err(r) => return r,
    };

    let active_slabs = {
        let rt = state
            .runtime
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rt.manifest.active_lookup_slabs(rt.l0_ram_slab.clone())
    };
    let all_in_memory = active_slabs.iter().all(|s| s.in_memory_pack.is_some());

    if all_in_memory {
        let _hot = gvfs_alloc_guard::hot_path();
        let req_meta = {
            let Ok((raw, mut sink)) = arena.scratch.bytes_and_oid_sink(body_span) else {
                return static_reply(StatusCode::BAD_REQUEST, "bad body span\n");
            };
            let mut cap_err = false;
            let parsed = scan_objects_request(raw, |oid| {
                if sink.push(oid).is_err() {
                    cap_err = true;
                    return false;
                }
                true
            });
            match parsed {
                Ok(m) if !cap_err => m,
                _ => return static_reply(StatusCode::BAD_REQUEST, "malformed objects JSON\n"),
            }
        };

        if req_meta.object_count == 0 {
            return static_reply(StatusCode::BAD_REQUEST, "empty objectIds\n");
        }

        // Expand commits to their trees (and parent commits up to `commit_depth`) in place!
        if !expand_commit_closures_in_arena(arena, &active_slabs, req_meta.commit_depth.max(1)) {
            return static_reply(StatusCode::NOT_FOUND, "requested object not found\n");
        }

        let requested = arena.scratch.active_oids();
        let Some((pack_bytes, _idx_bytes, unique_count)) = arena
            .splice
            .splice_local_slabs(&active_slabs, Some(requested))
        else {
            return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "splice overflow\n");
        };
        if unique_count == 0 {
            return static_reply(StatusCode::NOT_FOUND, "objects not found\n");
        }
        return Reply {
            status: StatusCode::OK,
            content_type: CONTENT_TYPE_PACKFILE,
            pack_range: None,
            body: SlabBody::single(pack_bytes),
        };
    }

    // Remote storage path: scan requested OIDs, coalesce range reads across slabs, and splice!
    {
        let Ok((raw, mut sink)) = arena.scratch.bytes_and_oid_sink(body_span) else {
            return static_reply(StatusCode::BAD_REQUEST, "bad body span\n");
        };
        if scan_objects_request(raw, |oid| sink.push(oid).is_ok()).is_err() {
            return static_reply(StatusCode::BAD_REQUEST, "malformed objects JSON\n");
        }
    }
    let requested = arena.scratch.active_oids();
    if requested.is_empty() {
        return static_reply(StatusCode::BAD_REQUEST, "empty objectIds\n");
    }

    let (unique_count, batches) =
        plan_deduplicated_range_splice(&active_slabs, Some(requested), 4096);
    if unique_count == 0 {
        return static_reply(StatusCode::NOT_FOUND, "objects not found\n");
    }
    let Ok(range_buffers) = state.storage.fetch_range_batches(&batches, &active_slabs) else {
        return static_reply(
            StatusCode::INTERNAL_SERVER_ERROR,
            "storage range fetch failed\n",
        );
    };
    let (pack_bytes, _) = assemble_spliced_pack(unique_count, &range_buffers);
    Reply {
        status: StatusCode::OK,
        content_type: CONTENT_TYPE_PACKFILE,
        pack_range: None,
        body: SlabBody::single(pack_bytes),
    }
}

/// Expands any commit OIDs in `arena.scratch` to their reachable trees (and parent commits
/// up to `commit_depth`) using pre-allocated queues in `arena` (zero heap allocations).
/// Also verifies that all explicitly requested OIDs exist in `active_slabs`.
fn expand_commit_closures_in_arena(
    arena: &mut WorkerArena,
    active_slabs: &[Arc<Slab>],
    commit_depth: u64,
) -> bool {
    arena.commit_queue.clear();
    arena.tree_queue.clear();
    arena.closure_seen.clear();

    let initial_count = arena.scratch.active_oids().len();
    for i in 0..initial_count {
        let Some(&oid) = arena.scratch.active_oids().get(i) else {
            return false;
        };
        let Some((slab_idx, meta)) = find_in_slabs(active_slabs, &oid) else {
            return false;
        };
        let _ = arena.closure_seen.insert(oid);
        let Some(slab) = active_slabs.get(slab_idx) else {
            return false;
        };
        let Some(ref pack) = slab.in_memory_pack else {
            continue;
        };
        let start = usize::try_from(meta.offset).unwrap_or(usize::MAX);
        let len = usize::try_from(meta.compressed_len).unwrap_or(usize::MAX);
        let Some(raw) = start.checked_add(len).and_then(|e| pack.get(start..e)) else {
            continue;
        };
        if let Some(hdr) = parse_entry_header(raw).ok().flatten() {
            if hdr.kind == ObjType::Commit
                && arena.commit_queue.len() < arena.commit_queue.capacity()
            {
                arena.commit_queue.push((oid, 1));
            }
        }
    }

    let mut c_cursor = 0usize;
    while c_cursor < arena.commit_queue.len() {
        let Some(&(c_oid, depth)) = arena.commit_queue.get(c_cursor) else {
            break;
        };
        c_cursor = c_cursor.saturating_add(1);

        let Some((slab_idx, meta)) = find_in_slabs(active_slabs, &c_oid) else {
            continue;
        };
        let Some(slab) = active_slabs.get(slab_idx) else {
            continue;
        };
        let Some(ref pack) = slab.in_memory_pack else {
            continue;
        };
        let start = usize::try_from(meta.offset).unwrap_or(usize::MAX);
        let len = usize::try_from(meta.compressed_len).unwrap_or(usize::MAX);
        let Some(raw) = start.checked_add(len).and_then(|e| pack.get(start..e)) else {
            continue;
        };
        let Ok((ObjType::Commit, n)) =
            inflate_stored_entry(raw, &mut arena.inflater, &mut arena.obj_scratch)
        else {
            continue;
        };
        let Some(c_bytes) = arena.obj_scratch.get(..n) else {
            continue;
        };
        let next_depth = depth.saturating_add(1);
        let commit_queue = &mut arena.commit_queue;
        let closure_seen = &mut arena.closure_seen;
        let scratch = &mut arena.scratch;
        let Some(info) = parse_commit(c_bytes, |parent_oid| {
            if depth < commit_depth && closure_seen.insert(parent_oid) == Some(true) {
                let _ = scratch.push_oid(parent_oid);
                if commit_queue.len() < commit_queue.capacity() {
                    commit_queue.push((parent_oid, next_depth));
                }
            }
            true
        }) else {
            continue;
        };

        if arena.closure_seen.insert(info.tree) == Some(true) {
            let _ = arena.scratch.push_oid(info.tree);
            if arena.tree_queue.len() < arena.tree_queue.capacity() {
                arena.tree_queue.push(info.tree);
            }
        }
    }

    let mut t_cursor = 0usize;
    while t_cursor < arena.tree_queue.len() {
        let Some(&t_oid) = arena.tree_queue.get(t_cursor) else {
            break;
        };
        t_cursor = t_cursor.saturating_add(1);

        let Some((slab_idx, meta)) = find_in_slabs(active_slabs, &t_oid) else {
            continue;
        };
        let Some(slab) = active_slabs.get(slab_idx) else {
            continue;
        };
        let Some((ObjType::Tree, n)) = slab.inflate_object_into(
            &meta,
            &mut arena.inflater,
            &mut arena.splice.base_scratch,
            &mut arena.splice.delta_scratch,
            &mut arena.obj_scratch,
        ) else {
            continue;
        };
        let Some(t_bytes) = arena.obj_scratch.get(..n) else {
            continue;
        };
        for entry_res in TreeIter::new(t_bytes) {
            let Ok(entry) = entry_res else {
                break;
            };
            if entry.is_tree() && arena.closure_seen.insert(entry.oid) == Some(true) {
                let _ = arena.scratch.push_oid(entry.oid);
                if arena.tree_queue.len() < arena.tree_queue.capacity() {
                    arena.tree_queue.push(entry.oid);
                }
            }
        }
    }

    true
}

#[inline]
fn find_in_slabs(slabs: &[Arc<Slab>], oid: &Oid20) -> Option<(usize, gvfs_core::ObjSliceMeta)> {
    for (idx, slab) in slabs.iter().enumerate() {
        if let Some(meta) = slab.lookup_oid(oid) {
            return Some((idx, meta));
        }
    }
    None
}

/// `POST /gvfs/sizes` — Returns `[{"Id":"<oid>","Size":N},...]` with zero heap allocations.
async fn handle_post_sizes(state: &DaemonHttpState, req: Request<Incoming>) -> Reply {
    let Some(mut guard) = state.pool.lease().await else {
        return static_reply(StatusCode::SERVICE_UNAVAILABLE, "all worker arenas busy\n");
    };
    let Some(arena) = guard.get_mut() else {
        return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "arena error\n");
    };
    let body_span = match read_body_into_arena(req.into_body(), arena).await {
        Ok(s) => s,
        Err(r) => return r,
    };

    let active_slabs = {
        let rt = state
            .runtime
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rt.manifest.active_lookup_slabs(rt.l0_ram_slab.clone())
    };

    let _hot = gvfs_alloc_guard::hot_path();
    {
        let Ok((raw, mut sink)) = arena.scratch.bytes_and_oid_sink(body_span) else {
            return static_reply(StatusCode::BAD_REQUEST, "bad body span\n");
        };
        if scan_oid_array(raw, |oid| sink.push(oid).is_ok()).is_err() {
            return static_reply(StatusCode::BAD_REQUEST, "malformed sizes JSON\n");
        }
    }

    let oids = arena.scratch.active_oids();
    let max_out = oids.len().saturating_mul(80).saturating_add(16);
    if arena.out_buf.capacity() < max_out {
        let _ = arena.out_buf.try_reclaim(arena.out_cap);
    }
    arena
        .out_buf
        .resize(max_out.min(arena.out_buf.capacity()), 0);
    let written_len = {
        let mut w = ByteWriter::new(&mut arena.out_buf);
        if w.put_u8(b'[').is_err() {
            return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "framing overflow\n");
        }
        for (i, oid) in oids.iter().enumerate() {
            let Some(size) = active_slabs.iter().find_map(|s| s.lookup_object_size(oid)) else {
                return static_reply(StatusCode::NOT_FOUND, "object not found\n");
            };
            if i > 0 && w.put_u8(b',').is_err() {
                return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "framing overflow\n");
            }
            if w.put(b"{\"Id\":\"").is_err()
                || w.put_hex_oid(oid).is_err()
                || w.put(b"\",\"Size\":").is_err()
                || w.put_dec(size).is_err()
                || w.put_u8(b'}').is_err()
            {
                return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "framing overflow\n");
            }
        }
        if w.put_u8(b']').is_err() {
            return static_reply(StatusCode::INTERNAL_SERVER_ERROR, "framing overflow\n");
        }
        w.len()
    };
    arena.out_buf.truncate(written_len);
    let out_bytes = arena.out_buf.split().freeze();
    Reply {
        status: StatusCode::OK,
        content_type: "application/json",
        pack_range: None,
        body: SlabBody::single(out_bytes),
    }
}

fn handle_metrics(state: &DaemonHttpState) -> Reply {
    let alloc = gvfs_alloc_guard::stats();
    let rt = state
        .runtime
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pyramid_count = rt.manifest.sub_week_pyramid.len();
    let l0_objs = rt.l0_ram_slab.as_ref().map_or(0, |s| s.object_count());
    let body = format!(
        "gvfs_requests_total {}\ngvfs_prefetch_requests_total {}\ngvfs_objects_post_requests_total {}\ngvfs_objects_get_requests_total {}\ngvfs_sizes_requests_total {}\ngvfs_bytes_served_total {}\ngvfs_alloc_sealed {}\ngvfs_alloc_hot_path_total {}\ngvfs_alloc_hot_path_bytes {}\ngvfs_alloc_post_seal_total {}\ngvfs_range_requests_total {}\ngvfs_range_bytes_fetched_total {}\ngvfs_pyramid_slabs {}\ngvfs_l0_ram_objects {}\n",
        state.counters.requests_total.load(Ordering::Relaxed),
        state.counters.prefetch_requests.load(Ordering::Relaxed),
        state.counters.objects_post_requests.load(Ordering::Relaxed),
        state.counters.objects_get_requests.load(Ordering::Relaxed),
        state.counters.sizes_requests.load(Ordering::Relaxed),
        state.counters.bytes_served.load(Ordering::Relaxed),
        u8::from(alloc.sealed),
        alloc.hot_path,
        alloc.hot_path_bytes,
        alloc.post_seal,
        state.storage.range_requests_issued.load(Ordering::Relaxed),
        state.storage.range_bytes_fetched.load(Ordering::Relaxed),
        pyramid_count,
        l0_objs,
    );
    Reply {
        status: StatusCode::OK,
        content_type: "text/plain; charset=utf-8",
        pack_range: None,
        body: SlabBody::single(Bytes::from(body.into_bytes())),
    }
}

/// Starts the HTTP/1.1 + HTTP/2 server on `bind_addr` and returns the bound `SocketAddr`
/// plus a join handle for the accept loop.
pub async fn spawn_http_server(
    bind_addr: SocketAddr,
    state: Arc<DaemonHttpState>,
) -> Result<(SocketAddr, tokio::task::JoinHandle<()>), DaemonError> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|e| DaemonError::Http(format!("bind {bind_addr}: {e}")))?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| DaemonError::Http(format!("local_addr: {e}")))?;

    let handle = tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                break;
            };
            let _ = stream.set_nodelay(true);
            let io = hyper_util::rt::TokioIo::new(stream);
            let st = Arc::clone(&state);
            tokio::spawn(async move {
                let svc = hyper::service::service_fn(move |req| {
                    handle_http_request(Arc::clone(&st), req)
                });
                let builder = hyper_util::server::conn::auto::Builder::new(
                    hyper_util::rt::TokioExecutor::new(),
                );
                let _ = builder.serve_connection(io, svc).await;
            });
        }
    });

    Ok((local_addr, handle))
}
