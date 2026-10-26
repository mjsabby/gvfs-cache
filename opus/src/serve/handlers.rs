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
//! only the worker's buffers and a leased request arena: no heap allocation.

use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use sha1::{Digest, Sha1};

use super::Ctx;
use super::blobs::BlobPack;
use super::httpd::{Method, Out, Request, read_body, respond_text, write_response};
use super::view::{Prefetch, View};
use crate::arena::{Arena, BlobSrc};
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
}

/// Maps a request path (any prefix before `/gvfs/`) and query to an endpoint.
pub fn route(method: Method, path: &[u8], query: &[u8]) -> Route {
    let gvfs = path
        .windows(6)
        .rposition(|w| w == b"/gvfs/")
        .and_then(|i| path.get(i.saturating_add(6)..));
    match (method, gvfs) {
        (Method::Get, Some(b"config")) => Route::Config,
        (Method::Get, Some(b"prefetch")) => {
            Route::Prefetch(query_number(query, b"lastPackTimestamp").unwrap_or(0))
        }
        (Method::Get, Some(rest)) if rest.starts_with(b"objects/") => rest
            .get(8..)
            .and_then(Oid::from_hex)
            .map(Route::ObjectGet)
            .unwrap_or(Route::NotFound),
        (Method::Get, Some(rest)) if rest.starts_with(b"blobs/") => {
            match rest.get(6..).and_then(Oid::from_hex) {
                Some(oid) => Route::BlobRange(
                    oid,
                    query_number(query, b"offset").unwrap_or(0),
                    query_number(query, b"length"),
                ),
                None => Route::NotFound,
            }
        }
        (Method::Post, Some(b"objects")) => Route::ObjectsPost,
        (Method::Post, Some(b"sizes")) => Route::SizesPost,
        (Method::Get, None) if path == b"/healthz" => Route::Health,
        (Method::Get, None) if path == b"/metrics" => Route::Metrics,
        _ => Route::NotFound,
    }
}

fn query_number(query: &[u8], key: &[u8]) -> Option<u64> {
    query.split(|&b| b == b'&').find_map(|kv| {
        let v = kv.strip_prefix(key)?.strip_prefix(b"=")?;
        std::str::from_utf8(v).ok()?.parse().ok()
    })
}

/// Handles one request. Returns whether the connection may be reused.
pub fn handle(
    ctx: &Ctx,
    w: &mut super::Worker,
    stream: &mut TcpStream,
    req: &Request,
) -> std::io::Result<bool> {
    let route = route(
        req.method,
        w.input.bytes(req.path),
        w.input.bytes(req.query),
    );
    let keep = req.keep_alive;
    if req.method != Method::Post {
        w.input.consume(req.head_len);
    }
    match route {
        Route::Config => {
            inc(&ctx.metrics.config);
            write_response(stream, 200, CT_JSON, &ctx.config_json, keep)?;
            Ok(keep)
        }
        Route::Health => {
            let started = ctx.ready.load(Ordering::Relaxed);
            // A separate-blob store is ready once every blob of its history is in it.
            let blobs = ctx.view().blobs.as_ref().is_none_or(|b| b.complete);
            let (status, text) = match (started, blobs) {
                (true, true) => (200, "ok\n"),
                (true, false) => (503, "blobs incomplete (gvfs-daemon mirror running)\n"),
                _ => (503, "starting\n"),
            };
            respond_text(stream, status, text, keep)?;
            Ok(keep)
        }
        Route::Metrics => {
            let _cold = gvfs_alloc_guard::cold_section();
            let text = super::metrics_text(ctx);
            respond_text(stream, 200, &text, keep)?;
            Ok(keep)
        }
        Route::Prefetch(since) => {
            inc(&ctx.metrics.prefetch);
            let idx = match ctx.prefetch_idx {
                super::IdxPolicy::Auto => !req.git_agent,
                super::IdxPolicy::Always => true,
                super::IdxPolicy::Never => false,
            };
            prefetch(ctx, w, stream, since, idx, req.wants_sizes, keep)
        }
        Route::ObjectGet(oid) => {
            inc(&ctx.metrics.object_get);
            let view = ctx.view();
            if let Some((pack, rec)) = stored_blob(&view, &oid) {
                inc(&ctx.metrics.blobs_store);
                return loose_blob(ctx, w, stream, pack, &rec, keep);
            }
            match view.find(&oid) {
                Some((k, slot)) => loose(ctx, w, stream, &view, k, slot, None, keep),
                None => match find_blob(ctx, &view, &oid) {
                    Blob::Found(pack, rec) => loose_blob(ctx, w, stream, &pack, &rec, keep),
                    Blob::Unknown => not_found(ctx, stream, keep),
                    Blob::Failed => unavailable(ctx, stream, keep),
                },
            }
        }
        Route::BlobRange(oid, offset, length) => {
            inc(&ctx.metrics.object_get);
            let view = ctx.view();
            match find_blob(ctx, &view, &oid) {
                Blob::Found(pack, rec) => {
                    blob_range(ctx, w, stream, &pack, &rec, offset, length, keep)
                }
                Blob::Unknown => not_found(ctx, stream, keep),
                Blob::Failed => unavailable(ctx, stream, keep),
            }
        }
        Route::ObjectsPost | Route::SizesPost => {
            let Some(mut lease) = ctx.arenas.lease(ctx.arena_wait) else {
                inc(&ctx.metrics.busy);
                respond_text(stream, 503, "busy\n", false)?;
                return Ok(false);
            };
            let Some(arena) = lease.arena() else {
                return Ok(false);
            };
            if !read_body(stream, &mut w.input, req, &mut arena.body)? {
                respond_text(stream, 413, "request too large\n", false)?;
                return Ok(false);
            }
            if route == Route::SizesPost {
                inc(&ctx.metrics.sizes);
                sizes(ctx, w, stream, arena, keep)
            } else {
                inc(&ctx.metrics.object_post);
                objects_post(
                    ctx,
                    w,
                    stream,
                    arena,
                    req.accepts_loose,
                    req.accepts_pack,
                    keep,
                )
            }
        }
        // An unread POST body would be parsed as the next request: close instead.
        Route::NotFound => not_found(ctx, stream, keep && req.method != Method::Post),
    }
}

fn not_found(ctx: &Ctx, stream: &mut TcpStream, keep: bool) -> std::io::Result<bool> {
    inc(&ctx.metrics.not_found);
    respond_text(stream, 404, "not found\n", keep)?;
    Ok(keep)
}

/// The upstream could not supply a blob the store lacks (clients retry a 503).
fn unavailable(ctx: &Ctx, stream: &mut TcpStream, keep: bool) -> std::io::Result<bool> {
    inc(&ctx.metrics.unavailable);
    respond_text(stream, 503, "blob unavailable upstream, retry\n", keep)?;
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
fn find_blob(ctx: &Ctx, view: &View, oid: &Oid) -> Blob {
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
    match proxy.fetch(std::slice::from_ref(oid)) {
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

/// Sends `len` bytes of a blob pack at `at` (mapped: `sendfile(2)` when large; remote: a
/// ranged read, cold path), hashing them into `hash` when given.
fn send_pack_range(
    out: &mut Out<'_>,
    pack: &BlobPack,
    at: u64,
    len: u64,
    mut hash: Option<&mut Sha1>,
) -> std::io::Result<()> {
    if let Some(bytes) = pack.bytes() {
        if let Some(h) = hash {
            h.update(
                crate::util::sub(bytes, us(at), us(len))
                    .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidData))?,
            );
        }
        return out.mapped(pack.file(), bytes, at, len);
    }
    let _cold = gvfs_alloc_guard::cold_section();
    let mut failed: Option<std::io::Error> = None;
    let read = pack.read(at, len, &mut |chunk| {
        if let Some(h) = hash.as_mut() {
            h.update(chunk);
        }
        out.bytes(chunk).map_err(|e| {
            let msg = e.to_string();
            failed = Some(e);
            crate::util::Error::new(msg)
        })
    });
    if let Some(e) = failed {
        return Err(e);
    }
    read.map_err(|e| std::io::Error::other(e.to_string()))
}

/// Exactly `buf.len()` bytes of a blob pack at `at` (cold path for remote packs).
fn read_pack(pack: &BlobPack, at: u64, buf: &mut [u8]) -> crate::util::Result<()> {
    let mut n = 0usize;
    pack.read(at, u64_of(buf.len()), &mut |chunk| {
        let end = n.saturating_add(chunk.len());
        buf.get_mut(n..end)
            .ok_or_else(|| crate::err!("blob pack read overflow"))?
            .copy_from_slice(chunk);
        n = end;
        Ok(())
    })?;
    if n != buf.len() {
        crate::bail!("short blob pack read");
    }
    Ok(())
}

/// A blob as a loose object: its entry's deflate blocks spliced between a stored block
/// and the combined Adler-32, never inflated (a remote entry: only its ends are read).
fn loose_blob(
    ctx: &Ctx,
    w: &mut super::Worker,
    stream: &mut TcpStream,
    pack: &BlobPack,
    rec: &BlobRec,
    keep: bool,
) -> std::io::Result<bool> {
    let (zat, zlen) = rec.zlib();
    let plan = match pack.bytes() {
        Some(b) => crate::util::sub(b, us(zat), us(zlen))
            .and_then(|z| LooseSplice::plan(OBJ_BLOB, rec.size, z).ok()),
        None => {
            let _cold = gvfs_alloc_guard::cold_section();
            let (mut head, mut tail) = ([0u8; 2], [0u8; 4]);
            read_pack(pack, zat, &mut head)
                .and_then(|()| {
                    read_pack(pack, zat.saturating_add(zlen).saturating_sub(4), &mut tail)
                })
                .ok()
                .and_then(|()| {
                    LooseSplice::plan_ends(OBJ_BLOB, rec.size, us(zlen), head, tail).ok()
                })
        }
    };
    let Some(plan) = plan else {
        inc(&ctx.metrics.errors);
        respond_text(stream, 500, "blob unreadable\n", false)?;
        return Ok(false);
    };
    let mut out = Out::new(stream, &mut w.out);
    out.head(200, CT_LOOSE, u64_of(plan.total_len()), keep)?;
    out.bytes(plan.prefix())?;
    send_pack_range(
        &mut out,
        pack,
        zat.saturating_add(u64_of(plan.body_start)),
        u64_of(plan.body_end.saturating_sub(plan.body_start)),
        None,
    )?;
    out.bytes(&plan.trailer)?;
    out.flush()?;
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
fn whole_pack(
    ctx: &Ctx,
    w: &mut super::Worker,
    stream: &mut TcpStream,
    pack: &BlobPack,
    keep: bool,
) -> std::io::Result<bool> {
    let mut out = Out::new(stream, &mut w.out);
    out.head(200, CT_PACK, pack.len, keep)?;
    send_pack_range(&mut out, pack, 0, pack.len, None)?;
    out.flush()?;
    ctx.metrics.bytes_out.fetch_add(out.sent, Ordering::Relaxed);
    inc(&ctx.metrics.objects_out);
    Ok(keep)
}

/// Bytes `[offset, offset + length)` of a blob's content (to its end without `length`).
/// Stored entries are read in place; compressed ones are inflated from their start (cold
/// path). `X-GVFS-Blob-Size` gives the blob's size.
#[allow(clippy::too_many_arguments)]
fn blob_range(
    ctx: &Ctx,
    w: &mut super::Worker,
    stream: &mut TcpStream,
    pack: &BlobPack,
    rec: &BlobRec,
    offset: u64,
    length: Option<u64>,
    keep: bool,
) -> std::io::Result<bool> {
    if offset > rec.size {
        respond_text(stream, 416, "offset beyond the blob\n", keep)?;
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
    let mut out = Out::new(stream, &mut w.out);
    out.head_with(
        200,
        CT_BYTES,
        len,
        keep,
        extra.get(..extra_len).unwrap_or(&[]),
    )?;
    if rec.stored {
        for (at, n) in rec.stored_range_iter(offset, len) {
            send_pack_range(&mut out, pack, at, n, None)?;
        }
    } else {
        let _cold = gvfs_alloc_guard::cold_section();
        inflate_range(&mut out, pack, rec, offset, len)?;
    }
    out.flush()?;
    ctx.metrics.bytes_out.fetch_add(out.sent, Ordering::Relaxed);
    Ok(keep)
}

/// Inflates a compressed blob entry from its start, sending content `[offset, offset+len)`.
fn inflate_range(
    out: &mut Out<'_>,
    pack: &BlobPack,
    rec: &BlobRec,
    offset: u64,
    len: u64,
) -> std::io::Result<()> {
    let bad = |m: &str| std::io::Error::other(m.to_string());
    let (zat, zlen) = rec.zlib();
    let mut z = zlib_rs::Inflate::new(true, 15);
    let mut chunk = vec![0u8; 256 << 10];
    let (mut produced, end) = (0u64, offset.saturating_add(len));
    let mut done = false;
    let mut failed: Option<std::io::Error> = None;
    let read = pack.read(zat, zlen, &mut |mut input| {
        while !input.is_empty() && !done {
            let (in0, out0) = (z.total_in(), z.total_out());
            let status = z
                .decompress(input, &mut chunk, zlib_rs::InflateFlush::NoFlush)
                .map_err(|_| crate::err!("corrupt blob entry"))?;
            let used = us(z.total_in().saturating_sub(in0));
            let made = z.total_out().saturating_sub(out0);
            let (lo, hi) = (produced.max(offset), produced.saturating_add(made).min(end));
            if lo < hi {
                let piece = chunk
                    .get(us(lo.saturating_sub(produced))..us(hi.saturating_sub(produced)))
                    .ok_or_else(|| crate::err!("inflate range"))?;
                if let Err(e) = out.bytes(piece) {
                    let msg = e.to_string();
                    failed = Some(e);
                    crate::bail!("{msg}");
                }
            }
            produced = produced.saturating_add(made);
            input = input.get(used..).unwrap_or(&[]);
            if status == zlib_rs::Status::StreamEnd || produced >= end {
                done = true;
            } else if used == 0 && made == 0 {
                crate::bail!("blob entry stream is stuck");
            }
        }
        Ok(())
    });
    if let Some(e) = failed {
        return Err(e);
    }
    read.map_err(|e| bad(&e.to_string()))?;
    if produced < end {
        return Err(bad("blob entry ended early"));
    }
    Ok(())
}

/// `GPRE` + version + count, then per pack: timestamp, pack length, index length (-1 for
/// none), the pack, and its `.idx` when `want_idx` (built once per plan).
#[allow(clippy::too_many_arguments)]
fn prefetch(
    ctx: &Ctx,
    w: &mut super::Worker,
    stream: &mut TcpStream,
    since: u64,
    want_idx: bool,
    want_sizes: bool,
    keep: bool,
) -> std::io::Result<bool> {
    let view = ctx.view();
    let sizes = want_sizes && view.blobs.is_some();
    let (plan, start) = match view.prefetch(since) {
        Prefetch::UpToDate if sizes => {
            let mut out = Out::new(stream, &mut w.out);
            let body: &[u8] = b"GPRE \x01\x00\x00GSZ1\0\0\0\0\0\0\0\0";
            out.head_with(
                200,
                CT_PREFETCH,
                u64_of(body.len()),
                keep,
                b"X-GVFS-Sizes: 1\r\n",
            )?;
            out.bytes(body)?;
            out.flush()?;
            return Ok(keep);
        }
        Prefetch::UpToDate => {
            write_response(stream, 200, CT_PREFETCH, b"GPRE \x01\x00\x00", keep)?;
            return Ok(keep);
        }
        Prefetch::Pack(plan, start) => (plan, start),
    };
    let section: &[u8] = if sizes {
        ctx.sizes_wanted.store(true, Ordering::Relaxed);
        plan.sizes_section()
    } else {
        &[]
    };
    inc(&ctx.metrics.prefetch_packs);
    let idx = if want_idx {
        ctx.idx_wanted.store(true, Ordering::Relaxed);
        plan.idx()
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
    let mut out = Out::new(stream, &mut w.out);
    out.head_with(
        200,
        CT_PREFETCH,
        body_len,
        keep,
        extra.get(..extra_len).unwrap_or(&[]),
    )?;
    out.bytes(&head)?;
    out.bytes(&plan.header)?;
    for (slab, start, len) in plan.bodies() {
        out.slab(slab, start, len)?;
    }
    // Computed once per plan; usually ready (warmed in the background) by now.
    let trailer = plan.trailer();
    out.bytes(&trailer)?;
    // Mapped from a memfd when large: sent like the pack, not copied into the socket.
    if let Some(idx) = idx {
        out.mapped(plan.idx_file(), idx, 0, u64_of(idx.len()))?;
    }
    if sizes {
        out.mapped(plan.sizes_file(), section, 0, u64_of(section.len()))?;
    }
    out.flush()?;
    ctx.metrics.bytes_out.fetch_add(out.sent, Ordering::Relaxed);
    ctx.metrics
        .objects_out
        .fetch_add(u64::from(plan.entries), Ordering::Relaxed);
    Ok(keep)
}

/// A loose object: `zlib("<type> <size>\0" ++ content)`. Whole entries are spliced
/// (stored block + original deflate blocks + combined Adler-32); deltas are resolved.
#[allow(clippy::too_many_arguments)]
fn loose(
    ctx: &Ctx,
    w: &mut super::Worker,
    stream: &mut TcpStream,
    view: &Arc<View>,
    k: u16,
    slot: u32,
    arena: Option<&mut Arena>,
    keep: bool,
) -> std::io::Result<bool> {
    let Some(slab) = view.all.get(usize::from(k)) else {
        return not_found(ctx, stream, keep);
    };
    let Some(rec) = slab.rec(slot) else {
        return not_found(ctx, stream, keep);
    };
    let entry = slab.entry(&rec).unwrap_or(&[]);
    let hdr_len = parse_entry_header(entry).map(|h| h.header_len).unwrap_or(0);
    if !rec.is_delta() {
        if let Ok(plan) = LooseSplice::plan(rec.kind, rec.size, entry.get(hdr_len..).unwrap_or(&[]))
        {
            let zlib_at = rec.offset.saturating_add(u64_of(hdr_len));
            let mut out = Out::new(stream, &mut w.out);
            out.head(200, CT_LOOSE, u64_of(plan.total_len()), keep)?;
            out.bytes(plan.prefix())?;
            out.slab(
                slab,
                zlib_at.saturating_add(u64_of(plan.body_start)),
                u64_of(plan.body_end.saturating_sub(plan.body_start)),
            )?;
            out.bytes(&plan.trailer)?;
            out.flush()?;
            ctx.metrics.bytes_out.fetch_add(out.sent, Ordering::Relaxed);
            inc(&ctx.metrics.objects_out);
            return Ok(keep);
        }
    }
    // Deltified (or unusual) entry: resolve and deflate in an arena.
    let mut lease = None;
    let arena = match arena {
        Some(a) => a,
        None => {
            lease = ctx.arenas.lease(ctx.arena_wait);
            match lease.as_mut().and_then(|l| l.arena()) {
                Some(a) => a,
                None => {
                    inc(&ctx.metrics.busy);
                    respond_text(stream, 503, "busy\n", false)?;
                    return Ok(false);
                }
            }
        }
    };
    let Arena {
        resolver,
        out: buf,
        deflater,
        ..
    } = arena;
    let Ok((kind, content)) = resolver.resolve(slab, slot) else {
        inc(&ctx.metrics.errors);
        respond_text(stream, 500, "object unreadable\n", false)?;
        return Ok(false);
    };
    let hdr = ObjectHeader::new(kind, u64_of(content.len()));
    let bound = crate::zlib::compress_bound(content.len().saturating_add(hdr.as_slice().len()));
    if buf.capacity() < bound {
        // A larger object than any before: grow the arena once (cold path).
        let _cold = gvfs_alloc_guard::cold_section();
        buf.reserve(bound.saturating_sub(buf.len()));
    }
    buf.clear();
    buf.resize(bound, 0);
    let n = match deflater.deflate_parts(&[hdr.as_slice(), content], buf) {
        Ok(n) => n,
        Err(_) => {
            inc(&ctx.metrics.errors);
            respond_text(stream, 500, "compression failed\n", false)?;
            return Ok(false);
        }
    };
    write_response(stream, 200, CT_LOOSE, buf.get(..n).unwrap_or(&[]), keep)?;
    ctx.metrics
        .bytes_out
        .fetch_add(u64_of(n), Ordering::Relaxed);
    inc(&ctx.metrics.objects_out);
    drop(lease);
    Ok(keep)
}

/// Every quoted 40-hex-digit string in `body`, appended to `out` up to its capacity.
/// Returns false if `out` overflowed.
pub fn scan_oids(body: &[u8], out: &mut Vec<Oid>) -> bool {
    let mut i = 0usize;
    while let Some(q) = body
        .get(i..)
        .and_then(|b| b.iter().position(|&c| c == b'"'))
    {
        let start = i.saturating_add(q).saturating_add(1);
        let end = start.saturating_add(40);
        if body.get(end) == Some(&b'"') {
            if let Some(oid) = body.get(start..end).and_then(Oid::from_hex) {
                if !crate::util::try_push(out, oid) {
                    return false;
                }
                i = end.saturating_add(1);
                continue;
            }
        }
        i = start;
    }
    true
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

fn objects_post(
    ctx: &Ctx,
    w: &mut super::Worker,
    stream: &mut TcpStream,
    arena: &mut Arena,
    accepts_loose: bool,
    accepts_pack: bool,
    keep: bool,
) -> std::io::Result<bool> {
    let depth = scan_number(&arena.body, b"\"commitDepth")
        .unwrap_or(1)
        .clamp(1, 1000);
    if !scan_oids(&arena.body, &mut arena.oids) {
        respond_text(stream, 413, "too many objects\n", keep)?;
        return Ok(keep);
    }
    if arena.oids.is_empty() {
        respond_text(stream, 400, "no objectIds\n", keep)?;
        return Ok(keep);
    }
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
                    return loose(ctx, w, stream, &view, k, slot, Some(arena), keep);
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
                            return whole_pack(ctx, w, stream, pack, keep);
                        }
                    }
                } else if let Some((pack, rec)) = stored {
                    inc(&ctx.metrics.blobs_store);
                    return loose_blob(ctx, w, stream, pack, &rec, keep);
                } else {
                    match find_blob(ctx, &view, &oid) {
                        Blob::Found(pack, rec) => {
                            return loose_blob(ctx, w, stream, &pack, &rec, keep);
                        }
                        Blob::Failed => return unavailable(ctx, stream, keep),
                        Blob::Unknown => {}
                    }
                }
            }
        }
    }
    if let Err(miss) = take_stored_blobs(&view, arena) {
        return match miss {
            Miss::Capacity => {
                respond_text(stream, 413, "request expands beyond the arena\n", keep)?;
                Ok(keep)
            }
            Miss::NotFound => not_found(ctx, stream, keep),
            Miss::Broken => unavailable(ctx, stream, keep),
        };
    }
    match plan_objects(&view, arena, depth) {
        Ok(()) => {}
        Err(Miss::NotFound) => return not_found(ctx, stream, keep),
        Err(Miss::Capacity) => {
            respond_text(stream, 413, "request expands beyond the arena\n", keep)?;
            return Ok(keep);
        }
        Err(Miss::Broken) => {
            inc(&ctx.metrics.errors);
            respond_text(stream, 500, "object unreadable\n", false)?;
            return Ok(false);
        }
    }
    match plan_blobs(ctx, &view, arena) {
        Ok(()) => {}
        Err(Miss::NotFound) => return not_found(ctx, stream, keep),
        Err(Miss::Capacity) => {
            respond_text(stream, 413, "request expands beyond the arena\n", keep)?;
            return Ok(keep);
        }
        Err(Miss::Broken) => return unavailable(ctx, stream, keep),
    }
    let Ok(count) = arena.splice.plan(&view.all, arena.grow) else {
        respond_text(stream, 413, "request expands beyond the arena\n", keep)?;
        return Ok(keep);
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
    let mut out = Out::new(stream, &mut w.out);
    out.head(200, CT_PACK, body_len.saturating_add(32), keep)?;
    out.bytes(&header)?;
    for p in &arena.splice.pieces {
        let bytes = piece_bytes(p, &view.all, &arena.splice.bytes)
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
        h.update(bytes);
        match *p {
            Piece::Slab { slab, start, len } => match view.all.get(usize::from(slab)) {
                Some(s) => out.slab(s, start, len)?,
                None => return Err(std::io::ErrorKind::InvalidData.into()),
            },
            Piece::Bytes { .. } => out.bytes(bytes)?,
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
        send_pack_range(&mut out, pack, rec.offset, rec.entry_len, Some(&mut h))?;
    }
    let trailer: [u8; 20] = h.finalize().into();
    out.bytes(&trailer)?;
    out.flush()?;
    ctx.metrics.bytes_out.fetch_add(out.sent, Ordering::Relaxed);
    ctx.metrics
        .objects_out
        .fetch_add(u64::from(count), Ordering::Relaxed);
    Ok(keep)
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
fn plan_objects(view: &View, arena: &mut Arena, depth: u64) -> Result<(), Miss> {
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
    }
    let grow = *grow;
    let mut rest = 0usize;
    for i in 0..oids.len() {
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
fn take_stored_blobs(view: &View, arena: &mut Arena) -> Result<(), Miss> {
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
fn plan_blobs(ctx: &Ctx, view: &View, arena: &mut Arena) -> Result<(), Miss> {
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
    let _cold = gvfs_alloc_guard::cold_section();
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
    let got = proxy.fetch(&need).map_err(|e| {
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
fn size_outside_slabs(ctx: &Ctx, view: &View, oid: &Oid) -> Option<u64> {
    if let Some(s) = view.blob_size(oid) {
        return Some(s);
    }
    let proxy = ctx.proxy.as_ref()?;
    if let Some((_, r)) = proxy.cached(oid) {
        return Some(r.size);
    }
    proxy
        .sizes(std::slice::from_ref(oid))
        .ok()?
        .first()
        .copied()
        .flatten()
}

fn sizes(
    ctx: &Ctx,
    _w: &mut super::Worker,
    stream: &mut TcpStream,
    arena: &mut Arena,
    keep: bool,
) -> std::io::Result<bool> {
    if !scan_oids(&arena.body, &mut arena.oids) {
        respond_text(stream, 413, "too many objects\n", keep)?;
        return Ok(keep);
    }
    let view = ctx.view();
    let Arena { oids, out, .. } = arena;
    out.clear();
    let mut ok = out.capacity() > 2;
    out.push(b'[');
    for (i, oid) in oids.iter().enumerate() {
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
                None => match size_outside_slabs(ctx, &view, oid) {
                    Some(s) => s,
                    None => return not_found(ctx, stream, keep),
                },
            },
        };
        let hex = oid.to_hex();
        let mut digits = [0u8; 20];
        let digits = crate::util::decimal(size, &mut digits);
        // `,{"Id":"<40 hex>","Size":<digits>}` built on the stack with constant-length copies.
        let mut item = [0u8; 80];
        let mut n = 0usize;
        let mut put = |src: &[u8]| {
            let end = n.saturating_add(src.len());
            if let Some(dst) = item.get_mut(n..end) {
                dst.copy_from_slice(src);
                n = end;
            }
        };
        if i != 0 {
            put(b",");
        }
        put(b"{\"Id\":\"");
        put(&hex);
        put(b"\",\"Size\":");
        put(digits);
        put(b"}");
        ok &= crate::util::try_extend(out, item.get(..n).unwrap_or(&[]));
    }
    ok &= crate::util::try_push(out, b']');
    if !ok {
        respond_text(stream, 413, "response too large\n", keep)?;
        return Ok(keep);
    }
    write_response(stream, 200, CT_JSON, out, keep)?;
    Ok(keep)
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
    }
}
