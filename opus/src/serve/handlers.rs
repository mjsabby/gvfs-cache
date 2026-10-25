//! The GVFS protocol endpoints.
//!
//! | endpoint | answer |
//! | --- | --- |
//! | `GET /gvfs/config` | static JSON built at boot |
//! | `GET /gvfs/prefetch[?lastPackTimestamp=N]` | `GPRE` stream with exactly ONE pack (or none) |
//! | `GET /gvfs/objects/<oid>` | loose object: spliced from a whole entry (no zlib work), or resolved |
//! | `POST /gvfs/objects` | one pack of the objects (+ delta bases); commits expand to parents and trees |
//! | `POST /gvfs/sizes` | `[{"Id":..,"Size":..}]` from the slab records |
//! | `GET /healthz`, `GET /metrics` | readiness and counters |
//!
//! Any prefix before `/gvfs/` is accepted (`https://host/org/repo/gvfs/...`). Handlers use
//! only the worker's buffers and a leased request arena: no heap allocation.

use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use sha1::{Digest, Sha1};

use super::Ctx;
use super::httpd::{Method, Out, Request, read_body, respond_text, write_response};
use super::view::{Prefetch, View};
use crate::arena::Arena;
use crate::pack::{OBJ_COMMIT, OBJ_TREE, ObjectHeader, pack_header, parse_entry_header};
use crate::splice::{ObjRef, Piece, piece_bytes};
use crate::util::{Oid, u64_of};
use crate::zlib::LooseSplice;

const CT_PREFETCH: &str = "application/x-gvfs-timestamped-packfiles-indexes";
const CT_PACK: &str = "application/x-git-packfile";
const CT_LOOSE: &str = "application/x-git-loose-object";
const CT_JSON: &str = "application/json";

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
            let ready = ctx.ready.load(Ordering::Relaxed);
            respond_text(
                stream,
                if ready { 200 } else { 503 },
                if ready { "ok\n" } else { "starting\n" },
                keep,
            )?;
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
            prefetch(ctx, w, stream, since, idx, keep)
        }
        Route::ObjectGet(oid) => {
            inc(&ctx.metrics.object_get);
            let view = ctx.view();
            match view.find(&oid) {
                None => not_found(ctx, stream, keep),
                Some((k, slot)) => loose(ctx, w, stream, &view, k, slot, None, keep),
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
                objects_post(ctx, w, stream, arena, req.accepts_loose, keep)
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

/// `GPRE` + version + count, then per pack: timestamp, pack length, index length (-1 for
/// none), the pack, and its `.idx` when `want_idx` (built once per plan).
fn prefetch(
    ctx: &Ctx,
    w: &mut super::Worker,
    stream: &mut TcpStream,
    since: u64,
    want_idx: bool,
    keep: bool,
) -> std::io::Result<bool> {
    let view = ctx.view();
    let plan = match view.prefetch(since) {
        Prefetch::UpToDate => {
            write_response(stream, 200, CT_PREFETCH, b"GPRE \x01\x00\x00", keep)?;
            return Ok(keep);
        }
        Prefetch::Pack(plan) => plan,
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
        .saturating_add(idx.map_or(0, |i| u64_of(i.len())));
    let mut out = Out::new(stream, &mut w.out);
    out.head(200, CT_PREFETCH, body_len, keep)?;
    out.bytes(&head)?;
    out.bytes(&plan.header)?;
    for (slab, start, len) in plan.bodies() {
        out.slab(slab, start, len)?;
    }
    // Computed once per plan; usually ready (warmed in the background) by now.
    let trailer = plan.trailer();
    out.bytes(&trailer)?;
    if let Some(idx) = idx {
        out.bytes(idx)?;
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
    // One non-commit object: answer as a loose object when the client accepts it.
    if let ([oid], true) = (arena.oids.as_slice(), accepts_loose) {
        if let Some((k, slot)) = view.find(oid) {
            let is_commit = view
                .all
                .get(usize::from(k))
                .and_then(|s| s.rec(slot))
                .is_some_and(|r| r.kind == OBJ_COMMIT);
            if !is_commit {
                return loose(ctx, w, stream, &view, k, slot, Some(arena), keep);
            }
        }
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
    let Ok(count) = arena.splice.plan(&view.all, arena.grow) else {
        respond_text(stream, 413, "request expands beyond the arena\n", keep)?;
        return Ok(keep);
    };
    // Header, pieces, trailer: hash first (the bytes are mapped), then send.
    let header = pack_header(count);
    let mut h = Sha1::new();
    h.update(header);
    let mut body_len = 0u64;
    for p in &arena.splice.pieces {
        h.update(piece_bytes(p, &view.all, &arena.splice.bytes).unwrap_or(&[]));
        body_len = body_len.saturating_add(p.len());
    }
    let trailer: [u8; 20] = h.finalize().into();
    let mut out = Out::new(stream, &mut w.out);
    out.head(200, CT_PACK, body_len.saturating_add(32), keep)?;
    out.bytes(&header)?;
    for p in &arena.splice.pieces {
        match *p {
            Piece::Slab { slab, start, len } => match view.all.get(usize::from(slab)) {
                Some(s) => out.slab(s, start, len)?,
                None => return Err(std::io::ErrorKind::InvalidData.into()),
            },
            Piece::Bytes { .. } => {
                out.bytes(piece_bytes(p, &view.all, &arena.splice.bytes).unwrap_or(&[]))?
            }
        }
    }
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
fn plan_objects(view: &View, arena: &mut Arena, depth: u64) -> Result<(), Miss> {
    // Commit expansion can name hundreds of thousands of trees: let it grow the arena.
    arena.grow = arena.oids.iter().any(|o| {
        view.find(o)
            .and_then(|(k, s)| view.all.get(usize::from(k)).and_then(|sl| sl.rec(s)))
            .is_some_and(|r| r.kind == OBJ_COMMIT)
    });
    let Arena {
        oids,
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
    let grow = *grow;
    for i in 0..oids.len() {
        let Some(&oid) = oids.get(i) else { break };
        let (k, slot) = view.find(&oid).ok_or(Miss::NotFound)?;
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
    Ok(())
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
        let Some((k, slot)) = view.find(oid) else {
            return not_found(ctx, stream, keep);
        };
        let size = view
            .all
            .get(usize::from(k))
            .and_then(|s| s.rec(slot))
            .map(|r| r.size)
            .unwrap_or(0);
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
