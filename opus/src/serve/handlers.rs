//! The GVFS protocol endpoints.
//!
//! | endpoint | answer |
//! | --- | --- |
//! | `GET /gvfs/config` | static JSON built at boot |
//! | `GET /gvfs/prefetch[?lastPackTimestamp=N]` | `GPRE` stream with exactly ONE pack (or none) |
//! | `GET /gvfs/objects/<oid>` | loose object: spliced from a whole entry (no zlib work), or resolved |
//! | `POST /gvfs/objects` | one pack of the objects (+ delta bases); commits expand to parents and trees |
//! | `POST /gvfs/sizes` | `[{"Id":..,"Size":..}]` from the slab records |
//! | `GET /gvfs/blobs/<oid>?offset=&length=` | a byte range of a blob's content (`--blobs separate`) |
//! | `GET /healthz`, `GET /metrics` | readiness and counters |
//!
//! In a `--blobs separate` store, blobs come from the blob packs (spliced whole, like
//! slab entries), or through the proxy for the L0 overlay's ([`super::blobs`]); a blob a
//! sealed tree names that a complete store lacks is a fault, served through the proxy too
//! but counted (`gvfs_blobs_missing_total`) and logged. A prefetch that sent
//! `X-GVFS-Sizes: 1` gets a `GSZ1` section after its packs (the sizes of the blobs its
//! trees introduce), and `X-GVFS-Sizes: 1` in the response says so.
//!
//! Any prefix before `/gvfs/` is accepted (`https://host/org/repo/gvfs/...`). Handlers use
//! only the connection's buffers and a leased request arena: no heap allocation. Loops
//! that compute for long (batches of ids, commit expansion, hashing a pack) yield to the
//! runtime's other tasks every [`super::budget::QUANTUM`]; plans built once run in
//! [`blocking`] sections; fetches from the upstream or object storage are awaited ([`cold`]
//! sections: their clients allocate).

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use sha1::{Digest, Sha1};
use tokio::net::TcpStream;

use super::Ctx;
use super::blobs::BlobPack;
use super::budget::{Budget, blocking, cold};
use super::conn::Bufs;
use super::httpd::{
    Body, Grow, Method, Out, Request, format_head, read_body, respond_text, write_all, write_head,
    write_response,
};
use super::view::{Prefetch, View};
use crate::arena::{Arena, BlobSrc, Lease};
use crate::blobs::BlobRec;
use crate::pack::{OBJ_BLOB, OBJ_COMMIT, OBJ_TREE, ObjectHeader, pack_header, parse_entry_header};
use crate::splice::{ObjRef, Piece, piece_bytes};
use crate::util::{Oid, u64_of, us};
use crate::zlib::LooseSplice;

const CT_PREFETCH: &str = "application/x-gvfs-timestamped-packfiles-indexes";
const CT_PACK: &str = "application/x-git-packfile";
const CT_LOOSE: &str = "application/x-git-loose-object";
const CT_JSON: &str = "application/json";
const CT_BYTES: &str = "application/octet-stream";
const CT_TEXT: &str = "text/plain";

/// Request counters exposed at `/metrics`.
#[derive(Default)]
pub struct Metrics {
    pub prefetch: AtomicU64,
    pub prefetch_packs: AtomicU64,
    pub object_get: AtomicU64,
    pub object_post: AtomicU64,
    pub sizes: AtomicU64,
    pub config: AtomicU64,
    pub not_found: AtomicU64,
    pub busy: AtomicU64,
    pub errors: AtomicU64,
    pub bytes_out: AtomicU64,
    pub objects_out: AtomicU64,
    pub l0_ingests: AtomicU64,
    pub manifest_loads: AtomicU64,
    /// Blobs served from the store's blob packs, and through the proxy.
    pub blobs_store: AtomicU64,
    pub blobs_proxied: AtomicU64,
    /// Blobs a sealed tree names that a complete store lacked (a fault: `gvfs-daemon
    /// mirror` repairs the store).
    pub blobs_missing: AtomicU64,
    pub unavailable: AtomicU64,
    /// Connections closed for idling past `--idle-secs`, or for going idle while
    /// `--max-idle-conns` others were.
    pub idle_closed: AtomicU64,
    pub idle_overflow: AtomicU64,
}

fn inc(c: &AtomicU64) {
    c.fetch_add(1, Ordering::Relaxed);
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Route {
    Config,
    Prefetch(u64),
    ObjectGet(Oid),
    ObjectsPost,
    SizesPost,
    /// A blob's content from `offset`, `length` bytes (to its end without one). Parsed with
    /// the route: once the head is consumed its buffer may hold the next request.
    BlobRange(Oid, u64, Option<u64>),
    Health,
    Metrics,
    NotFound,
    /// A query parameter that should be a number is not.
    BadRequest,
}

/// Maps a request path (any prefix before `/gvfs/`) and query to an endpoint.
pub fn route(method: Method, path: &[u8], query: &[u8]) -> Route {
    let gvfs = path
        .windows(6)
        .rposition(|w| w == b"/gvfs/")
        .and_then(|i| path.get(i.saturating_add(6)..));
    match (method, gvfs) {
        (Method::Get, Some(b"config")) => Route::Config,
        (Method::Get, Some(b"prefetch")) => match query_number(query, b"lastPackTimestamp") {
            Ok(since) => Route::Prefetch(since.unwrap_or(0)),
            Err(()) => Route::BadRequest,
        },
        (Method::Get, Some(rest)) if rest.starts_with(b"objects/") => rest
            .get(8..)
            .and_then(Oid::from_hex)
            .map(Route::ObjectGet)
            .unwrap_or(Route::NotFound),
        (Method::Get, Some(rest)) if rest.starts_with(b"blobs/") => {
            let oid = rest.get(6..).and_then(Oid::from_hex);
            match (
                oid,
                query_number(query, b"offset"),
                query_number(query, b"length"),
            ) {
                (None, _, _) => Route::NotFound,
                (Some(oid), Ok(offset), Ok(length)) => {
                    Route::BlobRange(oid, offset.unwrap_or(0), length)
                }
                _ => Route::BadRequest,
            }
        }
        (Method::Post, Some(b"objects")) => Route::ObjectsPost,
        (Method::Post, Some(b"sizes")) => Route::SizesPost,
        (Method::Get, None) if path == b"/healthz" => Route::Health,
        (Method::Get, None) if path == b"/metrics" => Route::Metrics,
        _ => Route::NotFound,
    }
}

/// Query parameter `key`: absent, or a decimal number. `Err` if present but anything else
/// (an unreadable cursor must not silently become a cold clone).
fn query_number(query: &[u8], key: &[u8]) -> Result<Option<u64>, ()> {
    let value = query
        .split(|&b| b == b'&')
        .find_map(|kv| kv.strip_prefix(key)?.strip_prefix(b"="));
    match value {
        None => Ok(None),
        Some(v) => super::httpd::decimal_u64(v).map(Some).ok_or(()),
    }
}

/// Handles one request. Returns whether the connection may be reused.
pub async fn handle(
    ctx: &Ctx,
    b: &mut Bufs,
    stream: &TcpStream,
    req: &Request,
    deadline: tokio::time::Instant,
) -> std::io::Result<bool> {
    // HEAD asks what GET would answer.
    let method = match req.method {
        Method::Head => Method::Get,
        m => m,
    };
    let route = route(method, b.input.bytes(req.path), b.input.bytes(req.query));
    let keep = req.keep_alive;
    if req.method != Method::Post {
        b.input.consume(req.head_len);
    }
    if req.method == Method::Head {
        return head(ctx, stream, route, keep).await;
    }
    match route {
        Route::Config => {
            inc(&ctx.metrics.config);
            let t = ctx.deadlines.write;
            write_response(stream, 200, CT_JSON, &ctx.config_json, keep, t).await?;
            Ok(keep)
        }
        Route::Health => {
            let (status, text) = health(ctx);
            respond(ctx, stream, status, text, keep).await?;
            Ok(keep)
        }
        Route::Metrics => {
            let text = {
                let _cold = gvfs_alloc_guard::cold_section();
                super::metrics_text(ctx)
            };
            let sent = respond(ctx, stream, 200, &text, keep).await;
            let _cold = gvfs_alloc_guard::cold_section();
            drop(text);
            sent?;
            Ok(keep)
        }
        Route::Prefetch(since) => {
            inc(&ctx.metrics.prefetch);
            let idx = match ctx.prefetch_idx {
                super::IdxPolicy::Auto => !req.git_agent,
                super::IdxPolicy::Always => true,
                super::IdxPolicy::Never => false,
            };
            prefetch(ctx, b, stream, since, idx, req.wants_sizes, keep).await
        }
        Route::ObjectGet(oid) => {
            inc(&ctx.metrics.object_get);
            let view = ctx.view();
            if let Some((pack, rec)) = stored_blob(&view, &oid) {
                inc(&ctx.metrics.blobs_store);
                return loose_blob(ctx, b, stream, pack, &rec, keep).await;
            }
            match view.find(&oid) {
                Some((k, slot)) => loose(ctx, b, stream, &view, k, slot, None, keep).await,
                None => match find_blob(ctx, &view, &oid).await {
                    Blob::Found(pack, rec) => loose_blob(ctx, b, stream, &pack, &rec, keep).await,
                    Blob::Unknown => not_found(ctx, stream, keep).await,
                    Blob::Failed => unavailable(ctx, stream, keep).await,
                },
            }
        }
        Route::BlobRange(oid, offset, length) => {
            inc(&ctx.metrics.object_get);
            let view = ctx.view();
            match find_blob(ctx, &view, &oid).await {
                Blob::Found(pack, rec) => {
                    blob_range(ctx, b, stream, &pack, &rec, offset, length, keep).await
                }
                Blob::Unknown => not_found(ctx, stream, keep).await,
                Blob::Failed => unavailable(ctx, stream, keep).await,
            }
        }
        Route::ObjectsPost | Route::SizesPost => {
            // The whole body first, into this connection's own buffer: a client that sends
            // slowly holds nothing shared. The arena is leased once the request is whole.
            let mut body = Grow {
                buf: &mut b.out,
                extra: &mut b.extra,
                max: ctx.body_max,
                spare: &ctx.spare,
            };
            let t = ctx.deadlines.write;
            match read_body(stream, &mut b.input, req, &mut body, deadline, t).await {
                Ok(Body::Read) => {}
                Ok(Body::TooLarge) => {
                    respond(ctx, stream, 413, "request too large\n", false).await?;
                    return Ok(false);
                }
                Ok(Body::Busy) => {
                    inc(&ctx.metrics.busy);
                    respond(ctx, stream, 503, "busy\n", false).await?;
                    return Ok(false);
                }
                Err(e) => {
                    super::httpd::refuse(stream, &e, t).await;
                    return Ok(false);
                }
            }
            let Some(lease) = ctx.arenas.lease(ctx.arena_wait).await else {
                inc(&ctx.metrics.busy);
                respond(ctx, stream, 503, "busy\n", false).await?;
                return Ok(false);
            };
            if route == Route::SizesPost {
                inc(&ctx.metrics.sizes);
                sizes(ctx, b, stream, lease, keep).await
            } else {
                inc(&ctx.metrics.object_post);
                let (loose_ok, pack_ok) = (req.accepts_loose, req.accepts_pack);
                objects_post(ctx, b, stream, lease, loose_ok, pack_ok, keep).await
            }
        }
        // An unread POST body would be parsed as the next request: close instead.
        Route::NotFound => not_found(ctx, stream, keep && req.method != Method::Post).await,
        Route::BadRequest => {
            respond(ctx, stream, 400, BAD_REQUEST, keep).await?;
            Ok(keep)
        }
    }
}

const NOT_FOUND: &str = "not found\n";
const BAD_REQUEST: &str = "bad request\n";

/// `/healthz`: ready once started and (a separate-blob store) once every blob of the
/// history is in the store.
fn health(ctx: &Ctx) -> (u16, &'static str) {
    let started = ctx.ready.load(Ordering::Relaxed);
    let blobs = ctx.view().blobs.as_ref().is_none_or(|b| b.complete);
    match (started, blobs) {
        (true, true) => (200, "ok\n"),
        (true, false) => (503, "blobs incomplete (gvfs-daemon mirror running)\n"),
        _ => (503, "starting\n"),
    }
}

/// `HEAD`: the head GET would answer with, and no body, where that answer is cheap to know
/// (the configuration, health, a 404 or 400); otherwise 405 with `Allow: GET`, since the
/// length alone would cost the answer's whole work (a prefetch plan, an object).
async fn head(ctx: &Ctx, stream: &TcpStream, route: Route, keep: bool) -> std::io::Result<bool> {
    let (status, content_type, len, extra): (u16, &str, usize, &[u8]) = match route {
        Route::Config => (200, CT_JSON, ctx.config_json.len(), b""),
        Route::Health => {
            let (status, text) = health(ctx);
            (status, CT_TEXT, text.len(), b"")
        }
        Route::NotFound => (404, CT_TEXT, NOT_FOUND.len(), b""),
        Route::BadRequest => (400, CT_TEXT, BAD_REQUEST.len(), b""),
        _ => (405, CT_TEXT, 0, b"Allow: GET\r\n"),
    };
    let t = ctx.deadlines.write;
    write_head(stream, status, content_type, u64_of(len), keep, extra, t).await?;
    Ok(keep)
}

/// A small plain-text response.
async fn respond(
    ctx: &Ctx,
    stream: &TcpStream,
    status: u16,
    text: &str,
    keep: bool,
) -> std::io::Result<()> {
    respond_text(stream, status, text, keep, ctx.deadlines.write).await
}

async fn not_found(ctx: &Ctx, stream: &TcpStream, keep: bool) -> std::io::Result<bool> {
    inc(&ctx.metrics.not_found);
    respond(ctx, stream, 404, NOT_FOUND, keep).await?;
    Ok(keep)
}

/// The upstream could not supply a blob the store lacks (clients retry a 503).
async fn unavailable(ctx: &Ctx, stream: &TcpStream, keep: bool) -> std::io::Result<bool> {
    inc(&ctx.metrics.unavailable);
    respond(ctx, stream, 503, "blob unavailable upstream, retry\n", keep).await?;
    Ok(keep)
}

/// Where a blob comes from.
enum Blob {
    Found(Arc<BlobPack>, BlobRec),
    /// Not a blob the store's trees name (or not a separate-blob store).
    Unknown,
    /// Named by a tree, but the upstream could not supply it.
    Failed,
}

/// A single blob at least this large is answered with a pack, not a loose object, when the
/// client accepts both (the store's large-blob threshold).
const LOOSE_BLOB_MAX: u64 = crate::blobcopy::LARGE_MIN;

/// Blob `oid` in the store's blob packs (`--blobs separate` stores). Looked up before the
/// slabs: they hold only commits and trees in such a store, so a blob id would miss every
/// slab's index first (each a probe of a large table) before reaching the one that has it.
#[inline]
fn stored_blob<'v>(view: &'v View, oid: &Oid) -> Option<(&'v Arc<BlobPack>, BlobRec)> {
    view.blobs.as_ref()?.find(oid)
}

/// Counts and logs blobs that sealed trees name and a complete store lacks (cold path).
fn missing(ctx: &Ctx, generation: u64, count: usize, first: &Oid) {
    let _cold = gvfs_alloc_guard::cold_section();
    ctx.metrics
        .blobs_missing
        .fetch_add(u64_of(count), Ordering::Relaxed);
    crate::log!(
        "serve: {count} blobs of sealed trees are not in blobs.bin generation {generation} \
         (first: {first}); proxying them. gvfs-daemon mirror copies them into the store"
    );
}

/// A blob from the store's packs, else (if the store's trees name it) through the proxy.
async fn find_blob(ctx: &Ctx, view: &View, oid: &Oid) -> Blob {
    let Some(set) = view.blobs.as_ref() else {
        return Blob::Unknown;
    };
    if let Some((pack, rec)) = set.find(oid) {
        inc(&ctx.metrics.blobs_store);
        return Blob::Found(Arc::clone(pack), rec);
    }
    if let Some((pack, rec)) = ctx.proxy.as_ref().and_then(|p| p.cached(oid)) {
        inc(&ctx.metrics.blobs_proxied);
        return Blob::Found(pack, rec);
    }
    if view.blob_size(oid).is_none() {
        return Blob::Unknown;
    }
    if set.complete && view.blob_sealed(oid) {
        missing(ctx, set.generation, 1, oid);
    }
    // Named by a tree but not in the store: only the upstream has it.
    let Some(proxy) = ctx.proxy.as_ref() else {
        return Blob::Failed;
    };
    match cold(proxy.fetch(std::slice::from_ref(oid))).await {
        Ok(mut v) => match v.pop() {
            Some((pack, rec)) => {
                inc(&ctx.metrics.blobs_proxied);
                Blob::Found(pack, rec)
            }
            None => Blob::Unknown,
        },
        Err(e) => {
            let _cold = gvfs_alloc_guard::cold_section();
            crate::log!("serve: proxying blob {oid} failed: {e}");
            Blob::Failed
        }
    }
}

/// Bytes hashed between budget checks.
const HASH_STEP: usize = 64 << 10;

/// Hashes `bytes` into `h`, yielding as `budget` says (one unit per 4 KiB: ~1.4 µs).
async fn hash_into(h: &mut Sha1, bytes: &[u8], budget: &mut Budget) {
    for chunk in bytes.chunks(HASH_STEP) {
        h.update(chunk);
        if budget.spend(super::budget::byte_units(chunk.len())) {
            budget.pause().await;
        }
    }
}

/// File ranges at least this long leave with `sendfile(2)` ([`Out::mapped`]).
const SENDFILE_MIN: u64 = 16 << 10;

/// A small range of a mapped blob pack, copied into the response buffer without an
/// `.await` (see `Out::put`); false if it is large, remote, or does not fit.
#[inline]
fn put_mapped(out: &mut Out<'_>, pack: &BlobPack, at: u64, len: u64) -> bool {
    len < SENDFILE_MIN
        && pack
            .bytes()
            .and_then(|b| crate::util::sub(b, us(at), us(len)))
            .is_some_and(|r| out.put(r))
}

/// Bytes of a remote blob pack read per step (through the block cache or the store).
const REMOTE_STEP: u64 = 1 << 20;

/// Sends `len` bytes of a blob pack at `at` (mapped: `sendfile(2)` when large; remote: a
/// step at a time, each read in a [`blocking`] section), hashing them into `hash` when
/// given.
async fn send_pack_range(
    out: &mut Out<'_>,
    pack: &BlobPack,
    at: u64,
    len: u64,
    mut hash: Option<&mut Sha1>,
    budget: &mut Budget,
) -> std::io::Result<()> {
    if let Some(bytes) = pack.bytes() {
        if let Some(h) = hash {
            let range = crate::util::sub(bytes, us(at), us(len))
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
            hash_into(h, range, budget).await;
        }
        return out.mapped(pack.file(), bytes, at, len).await;
    }
    let mut buf = {
        let _cold = gvfs_alloc_guard::cold_section();
        vec![0u8; us(len.min(REMOTE_STEP))]
    };
    let mut pos = at;
    let end = at.saturating_add(len);
    while pos < end {
        let n = end.saturating_sub(pos).min(REMOTE_STEP);
        let piece = buf.get_mut(..us(n)).unwrap_or(&mut []);
        cold(read_pack(pack, pos, piece))
            .await
            .map_err(super::httpd::other)?;
        if let Some(h) = hash.as_mut() {
            hash_into(h, piece, budget).await;
        }
        out.bytes(piece).await?;
        pos = pos.saturating_add(n);
    }
    let _cold = gvfs_alloc_guard::cold_section();
    drop(buf);
    Ok(())
}

/// Exactly `buf.len()` bytes of a blob pack at `at` (cold path for remote packs).
async fn read_pack(pack: &BlobPack, at: u64, buf: &mut [u8]) -> crate::util::Result<()> {
    let mut n = 0usize;
    pack.read(at, u64_of(buf.len()), &mut |chunk| {
        let end = n.saturating_add(chunk.len());
        buf.get_mut(n..end)
            .ok_or_else(|| crate::err!("blob pack read overflow"))?
            .copy_from_slice(chunk);
        n = end;
        Ok(())
    })
    .await?;
    if n != buf.len() {
        crate::bail!("short blob pack read");
    }
    Ok(())
}

/// A blob as a loose object: its entry's deflate blocks spliced between a stored block
/// and the combined Adler-32, never inflated (a remote entry: only its ends are read).
async fn loose_blob(
    ctx: &Ctx,
    b: &mut Bufs,
    stream: &TcpStream,
    pack: &BlobPack,
    rec: &BlobRec,
    keep: bool,
) -> std::io::Result<bool> {
    let (zat, zlen) = rec.zlib();
    let plan = match pack.bytes() {
        Some(b) => crate::util::sub(b, us(zat), us(zlen))
            .and_then(|z| LooseSplice::plan(OBJ_BLOB, rec.size, z).ok()),
        None => {
            cold(async {
                let (mut head, mut tail) = ([0u8; 2], [0u8; 4]);
                read_pack(pack, zat, &mut head).await.ok()?;
                let tail_at = zat.saturating_add(zlen).saturating_sub(4);
                read_pack(pack, tail_at, &mut tail).await.ok()?;
                LooseSplice::plan_ends(OBJ_BLOB, rec.size, us(zlen), head, tail).ok()
            })
            .await
        }
    };
    let Some(plan) = plan else {
        inc(&ctx.metrics.errors);
        respond(ctx, stream, 500, "blob unreadable\n", false).await?;
        return Ok(false);
    };
    let mut budget = Budget::new();
    let mut out = Out::new(stream, &mut b.out, ctx.deadlines.write);
    out.head(200, CT_LOOSE, u64_of(plan.total_len()), keep)?;
    if !out.put(plan.prefix()) {
        out.bytes(plan.prefix()).await?;
    }
    let (at, n) = (
        zat.saturating_add(u64_of(plan.body_start)),
        u64_of(plan.body_end.saturating_sub(plan.body_start)),
    );
    if !put_mapped(&mut out, pack, at, n) {
        send_pack_range(&mut out, pack, at, n, None, &mut budget).await?;
    }
    if !out.put(&plan.trailer) {
        out.bytes(&plan.trailer).await?;
    }
    out.flush().await?;
    ctx.metrics.bytes_out.fetch_add(out.sent, Ordering::Relaxed);
    inc(&ctx.metrics.objects_out);
    Ok(keep)
}

/// Whether `rec` is the only entry of `pack`: header, the entry, trailer. Every blob of
/// [`crate::blobcopy::LARGE_MIN`] or more is copied into a pack of its own.
fn own_pack(pack: &BlobPack, rec: &BlobRec) -> bool {
    let header = u64_of(crate::pack::PACK_HEADER_LEN);
    let trailer = u64_of(crate::pack::PACK_TRAILER_LEN);
    rec.offset == header && header.saturating_add(rec.entry_len).saturating_add(trailer) == pack.len
}

/// A blob's own pack file as a one-blob pack response: its header and trailer were written
/// when it was copied, so nothing is hashed (mapped: `sendfile(2)`; remote: ranged reads).
async fn whole_pack(
    ctx: &Ctx,
    b: &mut Bufs,
    stream: &TcpStream,
    pack: &BlobPack,
    keep: bool,
) -> std::io::Result<bool> {
    let mut budget = Budget::new();
    let mut out = Out::new(stream, &mut b.out, ctx.deadlines.write);
    out.head(200, CT_PACK, pack.len, keep)?;
    send_pack_range(&mut out, pack, 0, pack.len, None, &mut budget).await?;
    out.flush().await?;
    ctx.metrics.bytes_out.fetch_add(out.sent, Ordering::Relaxed);
    inc(&ctx.metrics.objects_out);
    Ok(keep)
}

/// Bytes `[offset, offset + length)` of a blob's content (to its end without `length`).
/// Stored entries are read in place; compressed ones are inflated from their start (cold
/// path). `X-GVFS-Blob-Size` gives the blob's size.
#[allow(clippy::too_many_arguments)]
async fn blob_range(
    ctx: &Ctx,
    b: &mut Bufs,
    stream: &TcpStream,
    pack: &BlobPack,
    rec: &BlobRec,
    offset: u64,
    length: Option<u64>,
    keep: bool,
) -> std::io::Result<bool> {
    if offset > rec.size {
        respond(ctx, stream, 416, "offset beyond the blob\n", keep).await?;
        return Ok(keep);
    }
    let len = length
        .unwrap_or(u64::MAX)
        .min(rec.size.saturating_sub(offset));
    let mut extra = [0u8; 48];
    let extra_len = {
        let mut cur = std::io::Cursor::new(extra.as_mut_slice());
        let _ = write!(cur, "X-GVFS-Blob-Size: {}\r\n", rec.size);
        usize::try_from(cur.position()).unwrap_or(0)
    };
    let mut budget = Budget::new();
    let mut out = Out::new(stream, &mut b.out, ctx.deadlines.write);
    out.head_with(
        200,
        CT_BYTES,
        len,
        keep,
        extra.get(..extra_len).unwrap_or(&[]),
    )?;
    if rec.stored {
        for (at, n) in rec.stored_range_iter(offset, len) {
            send_pack_range(&mut out, pack, at, n, None, &mut budget).await?;
        }
    } else {
        // Rare (a compressed blob read by range): its state lives on the heap.
        cold(inflate_range(&mut out, pack, rec, offset, len, &mut budget)).await?;
    }
    out.flush().await?;
    ctx.metrics.bytes_out.fetch_add(out.sent, Ordering::Relaxed);
    Ok(keep)
}

/// Inflates a compressed blob entry from its start, sending content `[offset, offset+len)`
/// (cold path: its buffers are allocated per request).
async fn inflate_range(
    out: &mut Out<'_>,
    pack: &BlobPack,
    rec: &BlobRec,
    offset: u64,
    len: u64,
    budget: &mut Budget,
) -> std::io::Result<()> {
    let bad = |m: &str| super::httpd::other(m);
    let (zat, zlen) = rec.zlib();
    let (mut z, mut chunk, mut input) = {
        let _cold = gvfs_alloc_guard::cold_section();
        let input = match pack.bytes() {
            Some(_) => Vec::new(),
            None => vec![0u8; us(zlen.min(REMOTE_STEP))],
        };
        (zlib_rs::Inflate::new(true, 15), vec![0u8; 64 << 10], input)
    };
    let (mut produced, end) = (0u64, offset.saturating_add(len));
    let mut pos = zat;
    let zend = zat.saturating_add(zlen);
    'stream: while pos < zend {
        // The next piece of the entry: in place when mapped, else read (a blocking section).
        let piece: &[u8] = match pack.bytes() {
            Some(b) => crate::util::sub(b, us(pos), us(zend.saturating_sub(pos)))
                .ok_or_else(|| bad("blob entry range"))?,
            None => {
                let n = zend.saturating_sub(pos).min(REMOTE_STEP);
                let buf = input.get_mut(..us(n)).unwrap_or(&mut []);
                cold(read_pack(pack, pos, buf))
                    .await
                    .map_err(super::httpd::other)?;
                buf
            }
        };
        pos = pos.saturating_add(u64_of(piece.len()));
        let mut rest = piece;
        while !rest.is_empty() {
            let (in0, out0) = (z.total_in(), z.total_out());
            let status = z
                .decompress(rest, &mut chunk, zlib_rs::InflateFlush::NoFlush)
                .map_err(|_| bad("corrupt blob entry"))?;
            let used = us(z.total_in().saturating_sub(in0));
            let made = z.total_out().saturating_sub(out0);
            let (lo, hi) = (produced.max(offset), produced.saturating_add(made).min(end));
            if lo < hi {
                let part = chunk
                    .get(us(lo.saturating_sub(produced))..us(hi.saturating_sub(produced)))
                    .ok_or_else(|| bad("inflate range"))?;
                out.bytes(part).await?;
            }
            produced = produced.saturating_add(made);
            rest = rest.get(used..).unwrap_or(&[]);
            if status == zlib_rs::Status::StreamEnd || produced >= end {
                break 'stream;
            } else if used == 0 && made == 0 {
                return Err(bad("blob entry stream is stuck"));
            }
            if budget.spend(64) {
                budget.pause().await;
            }
        }
    }
    let _cold = gvfs_alloc_guard::cold_section();
    drop((z, chunk, input));
    if produced < end {
        return Err(bad("blob entry ended early"));
    }
    Ok(())
}

/// `GPRE` + version + count, then per pack: timestamp, pack length, index length (-1 for
/// none), the pack, and its `.idx` when `want_idx` (built once per plan).
#[allow(clippy::too_many_arguments)]
async fn prefetch(
    ctx: &Ctx,
    b: &mut Bufs,
    stream: &TcpStream,
    since: u64,
    want_idx: bool,
    want_sizes: bool,
    keep: bool,
) -> std::io::Result<bool> {
    let view = ctx.view();
    let sizes = want_sizes && view.blobs.is_some();
    // A plan not built yet (a new snapshot's, before the warmer got to it) takes a while.
    let answer = match view.prefetch_ready(since) {
        Some(a) => a,
        None => blocking(|| view.prefetch(since)),
    };
    let (plan, start) = match answer {
        Prefetch::UpToDate if sizes => {
            let mut out = Out::new(stream, &mut b.out, ctx.deadlines.write);
            let body: &[u8] = b"GPRE \x01\x00\x00GSZ1\0\0\0\0\0\0\0\0";
            out.head_with(
                200,
                CT_PREFETCH,
                u64_of(body.len()),
                keep,
                b"X-GVFS-Sizes: 1\r\n",
            )?;
            out.bytes(body).await?;
            out.flush().await?;
            return Ok(keep);
        }
        Prefetch::UpToDate => {
            let t = ctx.deadlines.write;
            write_response(stream, 200, CT_PREFETCH, b"GPRE \x01\x00\x00", keep, t).await?;
            return Ok(keep);
        }
        Prefetch::Pack(plan, start) => (plan, start),
    };
    // The sections are built once per plan, usually by the warmer before anyone asks.
    let section: &[u8] = if sizes {
        ctx.sizes_wanted.store(true, Ordering::Relaxed);
        if plan.sizes_ready() {
            plan.sizes_section()
        } else {
            blocking(|| plan.sizes_section())
        }
    } else {
        &[]
    };
    inc(&ctx.metrics.prefetch_packs);
    let idx = if want_idx {
        ctx.idx_wanted.store(true, Ordering::Relaxed);
        if plan.idx_ready() {
            plan.idx()
        } else {
            blocking(|| plan.idx())
        }
    } else {
        None
    };
    let idx_len = idx.map_or(u64::MAX, |i| u64_of(i.len()));
    let mut head = [0u8; 8 + 24];
    for (dst, src) in head.iter_mut().zip(
        b"GPRE \x01\x01\x00"
            .iter()
            .chain(plan.timestamp.to_le_bytes().iter())
            .chain(plan.len.to_le_bytes().iter())
            .chain(idx_len.to_le_bytes().iter()),
    ) {
        *dst = *src;
    }
    let body_len = plan
        .len
        .saturating_add(32)
        .saturating_add(idx.map_or(0, |i| u64_of(i.len())))
        .saturating_add(u64_of(section.len()));
    // The interval the pack covers: an interval hook (fable's `client-prefetch`) unlinks
    // every local pack whose own interval lies inside it.
    let mut extra = [0u8; 128];
    let extra_len = {
        let mut cur = std::io::Cursor::new(extra.as_mut_slice());
        let _ = write!(
            cur,
            "X-GVFS-Pack-Start: {start}\r\nX-GVFS-Pack-End: {}\r\n{}",
            plan.timestamp,
            if sizes { "X-GVFS-Sizes: 1\r\n" } else { "" }
        );
        usize::try_from(cur.position()).unwrap_or(0)
    };
    let mut out = Out::new(stream, &mut b.out, ctx.deadlines.write);
    out.head_with(
        200,
        CT_PREFETCH,
        body_len,
        keep,
        extra.get(..extra_len).unwrap_or(&[]),
    )?;
    if !(out.put(&head) && out.put(&plan.header)) {
        return Err(super::httpd::other(
            "prefetch head does not fit the output buffer",
        ));
    }
    for (slab, start, len) in plan.bodies() {
        out.slab(slab, start, len).await?;
    }
    // Computed once per plan; usually ready (warmed in the background) by now.
    let trailer = if plan.trailer_ready() {
        plan.trailer()
    } else {
        blocking(|| plan.trailer())
    };
    out.bytes(&trailer).await?;
    // Mapped from a memfd when large: sent like the pack, not copied into the socket.
    if let Some(idx) = idx {
        out.mapped(plan.idx_file(), idx, 0, u64_of(idx.len()))
            .await?;
    }
    if sizes {
        out.mapped(plan.sizes_file(), section, 0, u64_of(section.len()))
            .await?;
    }
    out.flush().await?;
    ctx.metrics.bytes_out.fetch_add(out.sent, Ordering::Relaxed);
    ctx.metrics
        .objects_out
        .fetch_add(u64::from(plan.entries), Ordering::Relaxed);
    Ok(keep)
}

/// A loose object: `zlib("<type> <size>\0" ++ content)`. Whole entries are spliced
/// (stored block + original deflate blocks + combined Adler-32); deltas are resolved.
#[allow(clippy::too_many_arguments)]
async fn loose(
    ctx: &Ctx,
    b: &mut Bufs,
    stream: &TcpStream,
    view: &Arc<View>,
    k: u16,
    slot: u32,
    lease: Option<Lease<'_>>,
    keep: bool,
) -> std::io::Result<bool> {
    let Some(slab) = view.all.get(usize::from(k)) else {
        return not_found(ctx, stream, keep).await;
    };
    let Some(rec) = slab.rec(slot) else {
        return not_found(ctx, stream, keep).await;
    };
    let entry = slab.entry(&rec).unwrap_or(&[]);
    let hdr_len = parse_entry_header(entry).map(|h| h.header_len).unwrap_or(0);
    if !rec.is_delta() {
        if let Ok(plan) = LooseSplice::plan(rec.kind, rec.size, entry.get(hdr_len..).unwrap_or(&[]))
        {
            // A whole entry needs no arena.
            drop(lease);
            let zlib_at = rec.offset.saturating_add(u64_of(hdr_len));
            let mut out = Out::new(stream, &mut b.out, ctx.deadlines.write);
            out.head(200, CT_LOOSE, u64_of(plan.total_len()), keep)?;
            if !out.put(plan.prefix()) {
                out.bytes(plan.prefix()).await?;
            }
            let (at, n) = (
                zlib_at.saturating_add(u64_of(plan.body_start)),
                u64_of(plan.body_end.saturating_sub(plan.body_start)),
            );
            let small = n < SENDFILE_MIN
                && crate::util::sub(slab.pack(), us(at), us(n)).is_some_and(|b| out.put(b));
            if !small {
                out.slab(slab, at, n).await?;
            }
            if !out.put(&plan.trailer) {
                out.bytes(&plan.trailer).await?;
            }
            out.flush().await?;
            ctx.metrics.bytes_out.fetch_add(out.sent, Ordering::Relaxed);
            inc(&ctx.metrics.objects_out);
            return Ok(keep);
        }
    }
    // Deltified (or unusual) entry: resolve and deflate in an arena.
    let mut lease = match lease {
        Some(l) => l,
        None => match ctx.arenas.lease(ctx.arena_wait).await {
            Some(l) => l,
            None => {
                inc(&ctx.metrics.busy);
                respond(ctx, stream, 503, "busy\n", false).await?;
                return Ok(false);
            }
        },
    };
    let Some(arena) = lease.arena() else {
        return Ok(false);
    };
    let Arena {
        resolver,
        out: buf,
        deflater,
        ..
    } = arena;
    // A large object's chain takes milliseconds to resolve and deflate: off the worker.
    let mut work = || -> Result<usize, &'static str> {
        let (kind, content) = resolver
            .resolve(slab, slot)
            .map_err(|_| "object unreadable\n")?;
        let hdr = ObjectHeader::new(kind, u64_of(content.len()));
        let bound = crate::zlib::compress_bound(content.len().saturating_add(hdr.as_slice().len()));
        if buf.capacity() < bound {
            // A larger object than any before: grow the arena once (cold path).
            let _cold = gvfs_alloc_guard::cold_section();
            buf.reserve(bound.saturating_sub(buf.len()));
        }
        buf.clear();
        buf.resize(bound, 0);
        deflater
            .deflate_parts(&[hdr.as_slice(), content], buf)
            .map_err(|_| "compression failed\n")
    };
    let made = if rec.size >= RESOLVE_INLINE_MAX {
        blocking(work)
    } else {
        work()
    };
    let n = match made {
        Ok(n) => n,
        Err(why) => {
            inc(&ctx.metrics.errors);
            respond(ctx, stream, 500, why, false).await?;
            return Ok(false);
        }
    };
    let t = ctx.deadlines.write;
    let object = buf.get(..n).unwrap_or(&[]);
    let mut head = [0u8; 320];
    let h = format_head(&mut head, 200, CT_LOOSE, u64_of(n), keep);
    if h.saturating_add(n) <= b.out.capacity() {
        // Almost every resolved object (a tree, a commit) fits this connection's buffer:
        // copied there, the arena is free before the client reads.
        b.out.clear();
        b.out.extend_from_slice(head.get(..h).unwrap_or(&[]));
        b.out.extend_from_slice(object);
        drop(lease);
        write_all(stream, &b.out, t).await?;
    } else {
        write_response(stream, 200, CT_LOOSE, object, keep, t).await?;
    }
    ctx.metrics
        .bytes_out
        .fetch_add(u64_of(n), Ordering::Relaxed);
    inc(&ctx.metrics.objects_out);
    Ok(keep)
}

/// Objects at least this large are resolved in a [`blocking`] section (a delta chain of a
/// large blob: milliseconds of inflating, applying and deflating).
const RESOLVE_INLINE_MAX: u64 = 256 << 10;

/// Every quoted 40-hex-digit string in `body`, appended to `out` up to its capacity.
/// Returns false if `out` overflowed.
pub fn scan_oids(body: &[u8], out: &mut Vec<Oid>) -> bool {
    let mut at = 0usize;
    loop {
        match scan_oids_step(body, &mut at, out, usize::MAX) {
            Scan::More => {}
            Scan::Done => return true,
            Scan::Full => return false,
        }
    }
}

/// Where [`scan_oids_step`] stopped.
pub enum Scan {
    Done,
    /// `max` ids taken; more may follow.
    More,
    /// `out` is full.
    Full,
}

/// [`scan_oids`] from byte `*at`, stopping after `max` ids (so that a caller can yield).
pub fn scan_oids_step(body: &[u8], at: &mut usize, out: &mut Vec<Oid>, max: usize) -> Scan {
    let mut i = *at;
    let mut taken = 0usize;
    while let Some(q) = body
        .get(i..)
        .and_then(|b| b.iter().position(|&c| c == b'"'))
    {
        if taken >= max {
            *at = i;
            return Scan::More;
        }
        let start = i.saturating_add(q).saturating_add(1);
        let end = start.saturating_add(40);
        if body.get(end) == Some(&b'"') {
            if let Some(oid) = body.get(start..end).and_then(Oid::from_hex) {
                if !crate::util::try_push(out, oid) {
                    return Scan::Full;
                }
                taken = taken.saturating_add(1);
                i = end.saturating_add(1);
                continue;
            }
        }
        i = start;
    }
    *at = body.len();
    Scan::Done
}

/// Ids parsed between budget checks (~10 µs).
const SCAN_STEP: usize = 256;

/// [`scan_oids`], yielding as `budget` says.
async fn scan_ids(body: &[u8], out: &mut Vec<Oid>, budget: &mut Budget) -> bool {
    let mut at = 0usize;
    loop {
        match scan_oids_step(body, &mut at, out, SCAN_STEP) {
            Scan::Done => return true,
            Scan::Full => return false,
            Scan::More => {
                if budget.spend(64) {
                    budget.pause().await;
                }
            }
        }
    }
}

/// The integer after `"key"` and a colon.
pub fn scan_number(body: &[u8], key: &[u8]) -> Option<u64> {
    let at = body.windows(key.len()).position(|w| w == key)?;
    let rest = body.get(at.saturating_add(key.len())..)?;
    let rest = rest
        .strip_prefix(b"\"")?
        .trim_ascii_start()
        .strip_prefix(b":")?
        .trim_ascii_start();
    let digits = rest.iter().take_while(|c| c.is_ascii_digit()).count();
    std::str::from_utf8(rest.get(..digits)?).ok()?.parse().ok()
}

#[allow(clippy::too_many_arguments)]
/// `POST /gvfs/objects`, its body read into `b.out`.
async fn objects_post(
    ctx: &Ctx,
    b: &mut Bufs,
    stream: &TcpStream,
    mut lease: Lease<'_>,
    accepts_loose: bool,
    accepts_pack: bool,
    keep: bool,
) -> std::io::Result<bool> {
    let Some(arena) = lease.arena() else {
        return Ok(false);
    };
    let mut budget = Budget::new();
    let depth = scan_number(&b.out, b"\"commitDepth")
        .unwrap_or(1)
        .clamp(1, 1000);
    if !scan_ids(&b.out, &mut arena.oids, &mut budget).await {
        respond(ctx, stream, 413, "too many objects\n", keep).await?;
        return Ok(keep);
    }
    if arena.oids.is_empty() {
        respond(ctx, stream, 400, "no objectIds\n", keep).await?;
        return Ok(keep);
    }
    // The body is parsed: the buffer goes back to answering, at its pooled size.
    b.shrink(&ctx.spare);
    let view = ctx.view();
    // One object: a loose object when the client accepts one (not a commit: those expand),
    // except that a large blob goes out as a pack to a client that takes both: its
    // maintenance then keeps the entry as it is, where a loose object must be inflated and
    // deflated again (`git maintenance`'s loose-objects task: ~15 s per GiB). A blob that
    // goes out as a pack and has a pack of its own in the store (every blob of 16 MiB or
    // more) is sent as that pack file, which is the response already.
    if let [oid] = arena.oids.as_slice() {
        let oid = *oid;
        let stored = stored_blob(&view, &oid);
        let in_slab = if stored.is_some() {
            None
        } else {
            view.find(&oid)
        };
        match in_slab {
            Some((k, slot)) => {
                let rec = view.all.get(usize::from(k)).and_then(|s| s.rec(slot));
                let is_commit = rec.is_some_and(|r| r.kind == OBJ_COMMIT);
                let large_blob =
                    rec.is_some_and(|r| r.kind == OBJ_BLOB && r.size >= LOOSE_BLOB_MAX);
                if accepts_loose && !is_commit && !(accepts_pack && large_blob) {
                    return loose(ctx, b, stream, &view, k, slot, Some(lease), keep).await;
                }
            }
            None => {
                let size = stored
                    .map(|(_, r)| r.size)
                    .or_else(|| view.blob_size(&oid))
                    .unwrap_or(0);
                let large = size >= LOOSE_BLOB_MAX;
                if !accepts_loose || (accepts_pack && large) {
                    if let Some((pack, rec)) = stored {
                        if own_pack(pack, &rec) {
                            inc(&ctx.metrics.blobs_store);
                            return whole_pack(ctx, b, stream, pack, keep).await;
                        }
                    }
                } else if let Some((pack, rec)) = stored {
                    inc(&ctx.metrics.blobs_store);
                    return loose_blob(ctx, b, stream, pack, &rec, keep).await;
                } else {
                    match find_blob(ctx, &view, &oid).await {
                        Blob::Found(pack, rec) => {
                            return loose_blob(ctx, b, stream, &pack, &rec, keep).await;
                        }
                        Blob::Failed => return unavailable(ctx, stream, keep).await,
                        Blob::Unknown => {}
                    }
                }
            }
        }
    }
    if let Err(miss) = take_stored_blobs(&view, arena, &mut budget).await {
        return match miss {
            Miss::Capacity => {
                respond(ctx, stream, 413, "request expands beyond the arena\n", keep).await?;
                Ok(keep)
            }
            Miss::NotFound => not_found(ctx, stream, keep).await,
            Miss::Broken => unavailable(ctx, stream, keep).await,
        };
    }
    match plan_objects(&view, arena, depth, &mut budget).await {
        Ok(()) => {}
        Err(Miss::NotFound) => return not_found(ctx, stream, keep).await,
        Err(Miss::Capacity) => {
            respond(ctx, stream, 413, "request expands beyond the arena\n", keep).await?;
            return Ok(keep);
        }
        Err(Miss::Broken) => {
            inc(&ctx.metrics.errors);
            respond(ctx, stream, 500, "object unreadable\n", false).await?;
            return Ok(false);
        }
    }
    match plan_blobs(ctx, &view, arena, &mut budget).await {
        Ok(()) => {}
        Err(Miss::NotFound) => return not_found(ctx, stream, keep).await,
        Err(Miss::Capacity) => {
            respond(ctx, stream, 413, "request expands beyond the arena\n", keep).await?;
            return Ok(keep);
        }
        Err(Miss::Broken) => return unavailable(ctx, stream, keep).await,
    }
    let count = match lay_out(&view, arena, &mut budget).await {
        Some(n) => n,
        None => {
            respond(ctx, stream, 413, "request expands beyond the arena\n", keep).await?;
            return Ok(keep);
        }
    };
    // Slab entries, then whole blob entries; the trailer is hashed as the bytes go out.
    let count = count.saturating_add(crate::util::u32_of(arena.blobs.len()));
    let header = pack_header(count);
    let body_len = arena
        .splice
        .pieces
        .iter()
        .map(Piece::len)
        .chain(arena.blobs.iter().map(|(_, _, r)| r.entry_len))
        .fold(0u64, u64::saturating_add);
    let mut h = Sha1::new();
    h.update(header);
    let mut out = Out::new(stream, &mut b.out, ctx.deadlines.write);
    out.head(200, CT_PACK, body_len.saturating_add(32), keep)?;
    if !out.put(&header) {
        out.bytes(&header).await?;
    }
    // Small pieces are hashed and copied without an `.await` (see `Out::put`).
    for p in &arena.splice.pieces {
        let bytes = piece_bytes(p, &view.all, &arena.splice.bytes)
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
        if bytes.len() <= HASH_STEP {
            h.update(bytes);
            if budget.spend(super::budget::byte_units(bytes.len())) {
                budget.pause().await;
            }
        } else {
            hash_into(&mut h, bytes, &mut budget).await;
        }
        let small = u64_of(bytes.len()) < SENDFILE_MIN && out.put(bytes);
        if small {
            continue;
        }
        match *p {
            Piece::Slab { slab, start, len } => match view.all.get(usize::from(slab)) {
                Some(s) => out.slab(s, start, len).await?,
                None => return Err(std::io::ErrorKind::InvalidData.into()),
            },
            Piece::Bytes { .. } => out.bytes(bytes).await?,
        }
    }
    let pack_of = |src: BlobSrc, rec: &BlobRec| match src {
        BlobSrc::Store(seg) => view.blobs.as_ref().and_then(|s| s.pack(seg, rec)),
        BlobSrc::Proxied(k) => arena.proxied.get(us(u64::from(k))),
    };
    for (i, (_, src, rec)) in arena.blobs.iter().enumerate() {
        // The first two lines of an entry ahead (most are small; a large one's reads stream).
        if let Some((_, s, r)) = arena.blobs.get(i.saturating_add(AHEAD)) {
            if let Some(b) = pack_of(*s, r).and_then(|p| p.bytes()) {
                let at = us(r.offset);
                crate::util::prefetch(b, at);
                crate::util::prefetch(b, at.saturating_add(64));
            }
        }
        let pack = pack_of(*src, rec)
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
        let small = pack
            .bytes()
            .filter(|_| rec.entry_len < SENDFILE_MIN)
            .and_then(|b| crate::util::sub(b, us(rec.offset), us(rec.entry_len)));
        if let Some(entry) = small {
            h.update(entry);
            if !out.put(entry) {
                out.bytes(entry).await?;
            }
            if budget.spend(super::budget::byte_units(entry.len())) {
                budget.pause().await;
            }
            continue;
        }
        send_pack_range(
            &mut out,
            pack,
            rec.offset,
            rec.entry_len,
            Some(&mut h),
            &mut budget,
        )
        .await?;
    }
    let trailer: [u8; 20] = h.finalize().into();
    if !out.put(&trailer) {
        out.bytes(&trailer).await?;
    }
    out.flush().await?;
    ctx.metrics.bytes_out.fetch_add(out.sent, Ordering::Relaxed);
    ctx.metrics
        .objects_out
        .fetch_add(u64::from(count), Ordering::Relaxed);
    Ok(keep)
}

/// Entries laid out between budget checks.
const LAYOUT_STEP: usize = 512;

/// Sorts more entries than this in a [`blocking`] section (a commit's expansion: hundreds
/// of thousands of trees).
const SORT_INLINE_MAX: usize = 50_000;

/// The splice's layout ([`crate::splice::ObjectSplice::plan`]) in steps; `None` if it
/// outgrows the arena.
async fn lay_out(view: &View, arena: &mut Arena, budget: &mut Budget) -> Option<u32> {
    let grow = arena.grow;
    let splice = &mut arena.splice;
    if splice.refs.len() > SORT_INLINE_MAX {
        blocking(|| splice.plan_begin());
    } else {
        splice.plan_begin();
    }
    loop {
        match splice.plan_step(&view.all, grow, LAYOUT_STEP) {
            Ok(Some(n)) => return Some(n),
            Ok(None) => {
                if budget.spend(64) {
                    budget.pause().await;
                }
            }
            Err(_) => return None,
        }
    }
}

enum Miss {
    NotFound,
    Capacity,
    Broken,
}

/// Adds the requested objects to the arena's splice; commits bring `depth - 1` levels of
/// parents and every tree reachable from each commit (the GVFS protocol's expansion).
/// In a separate-blob store, leaves in `arena.oids` only the ids no slab has, for
/// [`plan_blobs`].
async fn plan_objects(
    view: &View,
    arena: &mut Arena,
    depth: u64,
    budget: &mut Budget,
) -> Result<(), Miss> {
    let Arena {
        oids,
        oid_at,
        found,
        seen,
        visited,
        work,
        commits,
        next_commits,
        splice,
        resolver,
        grow,
        ..
    } = arena;
    // Commit expansion can name hundreds of thousands of trees: let it grow the arena. The
    // lookups that tell are kept for the loop below (as many as `found` holds), and stop
    // where they did before once a commit settles it.
    found.clear();
    *grow = false;
    for (i, o) in oids.iter().enumerate() {
        if *grow && i >= found.capacity() {
            break;
        }
        if let Some(next) = oids.get(i.saturating_add(AHEAD)) {
            view.preload(next);
        }
        let hit = view.find(o);
        *grow |= hit
            .and_then(|(k, s)| view.all.get(usize::from(k)).and_then(|sl| sl.rec(s)))
            .is_some_and(|r| r.kind == OBJ_COMMIT);
        if i < found.capacity() {
            found.push(hit);
        }
        if budget.due() {
            budget.pause().await;
        }
    }
    let grow = *grow;
    let mut rest = 0usize;
    for i in 0..oids.len() {
        if budget.due() {
            budget.pause().await;
        }
        let Some(&oid) = oids.get(i) else { break };
        let hit = match found.get(i) {
            Some(&hit) => hit,
            None => view.find(&oid),
        };
        let Some((k, slot)) = hit else {
            // Blobs of a separate-blob store: see `plan_blobs`.
            if view.blobs.is_some() {
                if let Some(o) = oids.get_mut(rest) {
                    *o = oid;
                }
                if let Some(at) = oid_at.get(i).copied() {
                    if let Some(a) = oid_at.get_mut(rest) {
                        *a = at;
                    }
                }
                rest = rest.saturating_add(1);
                continue;
            }
            return Err(Miss::NotFound);
        };
        let rec = view
            .all
            .get(usize::from(k))
            .and_then(|s| s.rec(slot))
            .ok_or(Miss::Broken)?;
        if rec.kind != OBJ_COMMIT {
            splice
                .add(&view.all, ObjRef { slab: k, slot }, seen, grow)
                .map_err(|_| Miss::Capacity)?;
            continue;
        }
        commits.clear();
        if !crate::arena::push(commits, oid, grow) {
            return Err(Miss::Capacity);
        }
        for _level in 0..depth {
            next_commits.clear();
            for j in 0..commits.len() {
                if budget.spend(8) {
                    budget.pause().await;
                }
                let Some(&c) = commits.get(j) else { break };
                if !visited.insert_grow(&c, grow).map_err(|_| Miss::Capacity)? {
                    continue;
                }
                let (ck, cslot) = view.find(&c).ok_or(Miss::NotFound)?;
                splice
                    .add(
                        &view.all,
                        ObjRef {
                            slab: ck,
                            slot: cslot,
                        },
                        seen,
                        grow,
                    )
                    .map_err(|_| Miss::Capacity)?;
                let slab = view.all.get(usize::from(ck)).ok_or(Miss::Broken)?;
                let (_, content) = resolver.resolve(slab, cslot).map_err(|_| Miss::Broken)?;
                let tree = crate::object::commit_tree(content).ok_or(Miss::Broken)?;
                if !crate::arena::push(work, tree, grow) {
                    return Err(Miss::Capacity);
                }
                for p in crate::object::commit_parents(content) {
                    if !crate::arena::push(next_commits, p, grow) {
                        return Err(Miss::Capacity);
                    }
                }
            }
            // Every tree of this level's commits.
            while let Some(t) = work.pop() {
                if budget.spend(8) {
                    budget.pause().await;
                }
                if !visited.insert_grow(&t, grow).map_err(|_| Miss::Capacity)? {
                    continue;
                }
                let (tk, tslot) = view.find(&t).ok_or(Miss::NotFound)?;
                splice
                    .add(
                        &view.all,
                        ObjRef {
                            slab: tk,
                            slot: tslot,
                        },
                        seen,
                        grow,
                    )
                    .map_err(|_| Miss::Capacity)?;
                let slab = view.all.get(usize::from(tk)).ok_or(Miss::Broken)?;
                let (kind, content) = resolver.resolve(slab, tslot).map_err(|_| Miss::Broken)?;
                if kind != OBJ_TREE {
                    return Err(Miss::Broken);
                }
                let mut entries = crate::object::TreeIter::new(content);
                for e in entries.by_ref() {
                    if e.is_tree()
                        && !visited.contains(&e.oid)
                        && !crate::arena::push(work, e.oid, grow)
                    {
                        return Err(Miss::Capacity);
                    }
                }
                if entries.malformed {
                    return Err(Miss::Broken);
                }
            }
            if next_commits.is_empty() {
                break;
            }
            std::mem::swap(commits, next_commits);
        }
    }
    oids.truncate(rest);
    oid_at.truncate(rest);
    Ok(())
}

/// Moves the requested blobs the store's blob packs hold (`--blobs separate` stores) to
/// `arena.blobs`, one blob-pack lookup each (no slab has them), and leaves the other ids in
/// `arena.oids`, with their request positions in `arena.oid_at`: commits and trees for
/// [`plan_objects`], blobs only the upstream has for [`plan_blobs`] (which also drops
/// repeated ids).
async fn take_stored_blobs(
    view: &View,
    arena: &mut Arena,
    budget: &mut Budget,
) -> Result<(), Miss> {
    let Some(set) = view.blobs.as_ref() else {
        return Ok(());
    };
    let Arena {
        oids,
        oid_at,
        blobs,
        ..
    } = arena;
    oid_at.clear();
    let mut rest = 0usize;
    for i in 0..oids.len() {
        if budget.due() {
            budget.pause().await;
        }
        let Some(&oid) = oids.get(i) else { break };
        // Ahead of `i`: not yet moved by the compaction (it writes at `rest <= i`).
        if let Some(next) = oids.get(i.saturating_add(AHEAD)) {
            set.preload(next);
        }
        let at = crate::util::u32_of(i);
        match set.locate(&oid) {
            Some((seg, rec)) => {
                if !crate::arena::push(blobs, (at, BlobSrc::Store(seg), rec), true) {
                    return Err(Miss::Capacity);
                }
            }
            None => {
                if let Some(o) = oids.get_mut(rest) {
                    *o = oid;
                }
                if !crate::arena::push(oid_at, at, true) {
                    return Err(Miss::Capacity);
                }
                rest = rest.saturating_add(1);
            }
        }
    }
    oids.truncate(rest);
    Ok(())
}

/// The requested blobs, each once, in request order: the store's ([`take_stored_blobs`])
/// and the proxy's cached ones among the ids no slab has ([`plan_objects`] leaves them in
/// `arena.oids`); then, from the upstream in one request, the rest (only blobs the store's
/// trees name; anything else is not found). Sealed trees' blobs a complete store lacks are
/// counted as missing.
async fn plan_blobs(
    ctx: &Ctx,
    view: &View,
    arena: &mut Arena,
    budget: &mut Budget,
) -> Result<(), Miss> {
    let Some(set) = view.blobs.as_ref() else {
        return Ok(());
    };
    let Arena {
        oids,
        oid_at,
        blobs,
        proxied,
        blob_seen,
        ..
    } = arena;
    let mut kept = 0usize;
    for i in 0..blobs.len() {
        if budget.due() {
            budget.pause().await;
        }
        let Some(oid) = blobs.get(i).map(|b| b.2.oid) else {
            break;
        };
        if blob_seen
            .insert_grow(&oid, true)
            .map_err(|_| Miss::Capacity)?
        {
            if kept != i {
                blobs.swap(kept, i);
            }
            kept = kept.saturating_add(1);
        }
    }
    blobs.truncate(kept);
    let from_store = u64_of(kept);
    let (mut unresolved, mut from_cache) = (0usize, 0u64);
    for (j, oid) in oids.iter().enumerate() {
        if budget.due() {
            budget.pause().await;
        }
        if !blob_seen
            .insert_grow(oid, true)
            .map_err(|_| Miss::Capacity)?
        {
            continue;
        }
        match ctx.proxy.as_ref().and_then(|p| p.cached(oid)) {
            Some((pack, rec)) => {
                from_cache = from_cache.saturating_add(1);
                let at = oid_at.get(j).copied().unwrap_or(u32::MAX);
                let src = BlobSrc::Proxied(crate::util::u32_of(proxied.len()));
                if !crate::arena::push(proxied, pack, true)
                    || !crate::arena::push(blobs, (at, src, rec), true)
                {
                    return Err(Miss::Capacity);
                }
            }
            None => unresolved = unresolved.saturating_add(1),
        }
    }
    // The store's blobs and the cached ones, each list in request order: one list (an
    // in-place sort, positions are distinct).
    if from_cache > 0 {
        blobs.sort_unstable_by_key(|b| b.0);
    }
    ctx.metrics
        .blobs_store
        .fetch_add(from_store, Ordering::Relaxed);
    ctx.metrics
        .blobs_proxied
        .fetch_add(from_cache, Ordering::Relaxed);
    if unresolved == 0 {
        return Ok(());
    }
    // Blobs only the upstream has (the L0 overlay's, or a fault): a cold section.
    cold(fetch_unresolved(
        ctx, view, set, oids, blobs, proxied, unresolved,
    ))
    .await
}

/// The requested blobs neither the store nor the proxy cache had: from the upstream in one
/// request (only blobs the store's trees name; anything else is not found).
async fn fetch_unresolved(
    ctx: &Ctx,
    view: &View,
    set: &super::blobs::BlobSet,
    oids: &[Oid],
    blobs: &mut Vec<(u32, BlobSrc, BlobRec)>,
    proxied: &mut Vec<Arc<BlobPack>>,
    unresolved: usize,
) -> Result<(), Miss> {
    let mut have: std::collections::HashSet<Oid> = blobs.iter().map(|b| b.2.oid).collect();
    let mut need = Vec::with_capacity(unresolved);
    for oid in oids.iter() {
        if have.insert(*oid) {
            if view.blob_size(oid).is_none() {
                return Err(Miss::NotFound);
            }
            need.push(*oid);
        }
    }
    if set.complete {
        let mut sealed = need.iter().filter(|o| view.blob_sealed(o));
        if let Some(first) = sealed.next() {
            missing(ctx, set.generation, sealed.count().saturating_add(1), first);
        }
    }
    // Named by trees but not in the store: only the upstream has them.
    let proxy = ctx.proxy.as_ref().ok_or(Miss::Broken)?;
    let got = proxy.fetch(&need).await.map_err(|e| {
        crate::log!("serve: proxying {} blobs failed: {e}", need.len());
        Miss::Broken
    })?;
    ctx.metrics
        .blobs_proxied
        .fetch_add(u64_of(got.len()), Ordering::Relaxed);
    // After the others, in the order the upstream's answer has them (as they always went).
    for (pack, rec) in got {
        let src = BlobSrc::Proxied(crate::util::u32_of(proxied.len()));
        if !crate::arena::push(proxied, pack, true)
            || !crate::arena::push(blobs, (u32::MAX, src, rec), true)
        {
            return Err(Miss::Capacity);
        }
    }
    Ok(())
}

/// How far ahead of the id it is on a batched lookup starts loading another's record.
const AHEAD: usize = 16;

/// The size of an object no slab and no blob pack has: a blob the trees name (sizes
/// tables), or known to the upstream.
async fn size_outside_slabs(ctx: &Ctx, view: &View, oid: &Oid) -> Option<u64> {
    if let Some(s) = view.blob_size(oid) {
        return Some(s);
    }
    let proxy = ctx.proxy.as_ref()?;
    if let Some((_, r)) = proxy.cached(oid) {
        return Some(r.size);
    }
    cold(proxy.sizes(std::slice::from_ref(oid)))
        .await
        .ok()?
        .first()
        .copied()
        .flatten()
}

/// `POST /gvfs/sizes`, its body read into `b.out`: `[{"Id":..,"Size":..}]`. The answer is
/// built in the connection's buffer, where the body was, so that the arena is free before
/// the client reads it.
async fn sizes(
    ctx: &Ctx,
    b: &mut Bufs,
    stream: &TcpStream,
    mut lease: Lease<'_>,
    keep: bool,
) -> std::io::Result<bool> {
    let Some(arena) = lease.arena() else {
        return Ok(false);
    };
    let mut budget = Budget::new();
    if !scan_ids(&b.out, &mut arena.oids, &mut budget).await {
        respond(ctx, stream, 413, "too many objects\n", keep).await?;
        return Ok(keep);
    }
    let view = ctx.view();
    let oids = &arena.oids;
    let mut out = Grow {
        buf: &mut b.out,
        extra: &mut b.extra,
        max: ctx.out_max,
        spare: &ctx.spare,
    };
    out.buf.clear();
    let mut ok = out.buf.capacity() > 2;
    out.buf.push(b'[');
    for (i, oid) in oids.iter().enumerate() {
        if budget.due() {
            budget.pause().await;
        }
        if let Some(next) = oids.get(i.saturating_add(AHEAD)) {
            match view.blobs.as_ref() {
                Some(set) => set.preload(next),
                None => view.preload(next),
            }
        }
        let size = match stored_blob(&view, oid) {
            Some((_, r)) => r.size,
            None => match view.find(oid) {
                Some((k, slot)) => view
                    .all
                    .get(usize::from(k))
                    .and_then(|s| s.rec(slot))
                    .map(|r| r.size)
                    .unwrap_or(0),
                None => match size_outside_slabs(ctx, &view, oid).await {
                    Some(s) => s,
                    None => return not_found(ctx, stream, keep).await,
                },
            },
        };
        // Room for one more item, growing the buffer if the answer outgrows it.
        ok = ok && out.room(out.buf.len().saturating_add(ITEM_MAX));
        ok = ok && size_item(out.buf, i == 0, oid, size);
    }
    ok = ok && out.room(out.buf.len().saturating_add(1)) && crate::util::try_push(out.buf, b']');
    if !ok {
        respond(ctx, stream, 413, "response too large\n", keep).await?;
        return Ok(keep);
    }
    drop(lease);
    write_response(stream, 200, CT_JSON, &b.out, keep, ctx.deadlines.write).await?;
    Ok(keep)
}

/// The longest item [`size_item`] writes.
const ITEM_MAX: usize = 80;

/// `,{"Id":"<40 hex>","Size":<digits>}` (no comma first) onto `out`; false if it does not
/// fit. Every copy has a constant length (the digits' right-aligned 20-byte buffer goes in
/// whole, first, and the fixed parts over its leading bytes): copies of a length known only
/// at run time are `memmove` calls, five per id (17% of a 4,000-id request).
fn size_item(out: &mut Vec<u8>, first: bool, oid: &Oid, size: u64) -> bool {
    const HEAD: &[u8; 8] = b",{\"Id\":\"";
    const MID: &[u8; 9] = b"\",\"Size\":";
    let mut digits = [0u8; 20];
    let d = crate::util::decimal(size, &mut digits).len();
    let mut item = [0u8; 80];
    let end = 57usize.saturating_add(d);
    if let Some(dst) = item.get_mut(end.saturating_sub(20)..end) {
        dst.copy_from_slice(&digits);
    }
    if let Some(dst) = item.get_mut(..8) {
        dst.copy_from_slice(HEAD);
    }
    if let Some(dst) = item.get_mut(8..48) {
        dst.copy_from_slice(&oid.to_hex());
    }
    if let Some(dst) = item.get_mut(48..57) {
        dst.copy_from_slice(MID);
    }
    if let Some(b) = item.get_mut(end) {
        *b = b'}';
    }
    let from = usize::from(first);
    crate::util::try_extend(out, item.get(from..end.saturating_add(1)).unwrap_or(&[]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_and_json() {
        let oid = "a".repeat(40);
        assert_eq!(route(Method::Get, b"/gvfs/config", b""), Route::Config);
        assert_eq!(
            route(
                Method::Get,
                b"/org/repo/gvfs/prefetch",
                b"lastPackTimestamp=123"
            ),
            Route::Prefetch(123)
        );
        assert_eq!(
            route(
                Method::Get,
                format!("/x/gvfs/objects/{oid}").as_bytes(),
                b""
            ),
            Route::ObjectGet(Oid([0xaa; 20]))
        );
        assert_eq!(
            route(Method::Post, b"/gvfs/objects", b""),
            Route::ObjectsPost
        );
        assert_eq!(route(Method::Post, b"/gvfs/sizes", b""), Route::SizesPost);
        assert_eq!(
            route(
                Method::Get,
                format!("/gvfs/blobs/{oid}").as_bytes(),
                b"length=7&offset=5"
            ),
            Route::BlobRange(Oid([0xaa; 20]), 5, Some(7))
        );
        assert_eq!(
            route(Method::Get, format!("/gvfs/blobs/{oid}").as_bytes(), b""),
            Route::BlobRange(Oid([0xaa; 20]), 0, None)
        );
        assert_eq!(
            route(Method::Get, b"/gvfs/objects/zz", b""),
            Route::NotFound
        );
        // Unreadable numbers are refused, not read as 0 (a cold clone).
        for q in [
            &b"lastPackTimestamp=abc"[..],
            b"lastPackTimestamp=",
            b"lastPackTimestamp=+5",
            b"lastPackTimestamp=-1",
            b"lastPackTimestamp=99999999999999999999",
        ] {
            assert_eq!(route(Method::Get, b"/gvfs/prefetch", q), Route::BadRequest);
        }
        assert_eq!(
            route(Method::Get, b"/gvfs/prefetch", b"x=1&lastPackTimestamp=7"),
            Route::Prefetch(7)
        );
        assert_eq!(
            route(
                Method::Get,
                format!("/gvfs/blobs/{oid}").as_bytes(),
                b"offset=x"
            ),
            Route::BadRequest
        );
        let body = format!(
            "{{\"objectIds\":[\"{oid}\", \"{}\"],\"commitDepth\" : 3}}",
            "b".repeat(40)
        );
        let mut oids = Vec::with_capacity(4);
        assert!(scan_oids(body.as_bytes(), &mut oids));
        assert_eq!(oids, vec![Oid([0xaa; 20]), Oid([0xbb; 20])]);
        assert_eq!(scan_number(body.as_bytes(), b"\"commitDepth"), Some(3));
        let mut one = Vec::with_capacity(1);
        assert!(!scan_oids(body.as_bytes(), &mut one));
        let mut out = Vec::with_capacity(256);
        assert!(size_item(&mut out, true, &Oid([0xab; 20]), 0));
        assert!(size_item(&mut out, false, &Oid([0x01; 20]), u64::MAX));
        assert_eq!(
            String::from_utf8(out).unwrap(),
            format!(
                "{{\"Id\":\"{}\",\"Size\":0}},{{\"Id\":\"{}\",\"Size\":{}}}",
                "ab".repeat(20),
                "01".repeat(20),
                u64::MAX
            )
        );
        let mut tight = Vec::with_capacity(10);
        assert!(!size_item(&mut tight, true, &Oid([0xab; 20]), 7));
    }
}
