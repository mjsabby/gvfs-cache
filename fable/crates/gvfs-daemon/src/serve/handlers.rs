//! GVFS protocol endpoints. Any path prefix before `/gvfs/` is accepted
//! (`https://host/org/project/_git/repo/gvfs/...`).
//!
//! Each endpoint does its work inside a hot-path region and returns an [`Outcome`]; the
//! HTTP response (whose header map allocates) is built afterwards, so the allocation guard
//! measures exactly our own code. An object no slab holds is queued for the live thread,
//! which fetches it from the upstream into the live slab; the request waits (outside the
//! hot region) for that round and looks again, once.

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, header};
use http_body::Body as _;
use hyper::body::Incoming;

use crate::arena::Span;
use crate::oid::Oid;
use crate::pack::parse_entry_header;
use crate::splice::Plan;
use crate::zlib::loose_splice;

use super::State;
use super::body::{
    Body, CONTENT_TYPE_JSON, CONTENT_TYPE_LOOSE, CONTENT_TYPE_PACKFILE, CONTENT_TYPE_PREFETCH,
    Deflating, LooseStream, ObjectsStream, PrefetchStream, SizesStream, selection_key,
};
use super::catalog::Catalog;
use super::json::{oid_at, scan_object_ids};
use super::objects::{ObjError, object_info, plan_objects, resolve_full};
use super::pool::ArenaGuard;

/// How long a request waits for the live thread to fetch missing objects.
const MISS_WAIT: Duration = Duration::from_secs(60);

/// What an endpoint decided, before any HTTP object exists.
enum Outcome {
    Error(StatusCode, &'static str),
    Full(&'static str, Bytes),
    Prefetch(Box<Plan>, Option<([u8; 20], [u8; 20])>, u64),
    Loose(Bytes, Bytes, Bytes, ArenaGuard),
    /// A resolved delta, deflated while streaming.
    LooseStream(ArenaGuard, Deflating),
    Objects(ArenaGuard, Arc<Catalog>, Option<u64>),
    Sizes(ArenaGuard, usize, usize),
    /// Missing objects were queued (ticket); run the request again after that round.
    Retry(ArenaGuard, Span, u64),
}

fn respond(status: StatusCode, content_type: &'static str, body: Body) -> Response<Body> {
    let mut r = Response::new(body);
    *r.status_mut() = status;
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static(content_type),
    );
    r
}

fn build(state: &State, out: Outcome) -> Response<Body> {
    match out {
        Outcome::Error(status, msg) => respond(status, "text/plain", Body::text(msg)),
        Outcome::Full(ct, b) => respond(StatusCode::OK, ct, Body::full(b)),
        Outcome::Prefetch(plan, known, key) => {
            // The interval this pack covers: a client hook may unlink every local prefetch
            // pack whose own interval lies inside it (superset tombstoning, no `git gc`).
            let (start, end) = (plan.start_ts, plan.timestamp);
            let mut buf = super::pool::promoted(64);
            let stream =
                PrefetchStream::new(*plan, known, Arc::clone(&state.trailers), key, &mut buf);
            let mut r = respond(
                StatusCode::OK,
                CONTENT_TYPE_PREFETCH,
                Body::prefetch(stream),
            );
            r.headers_mut().insert(
                header::HeaderName::from_static("x-gvfs-pack-start"),
                header::HeaderValue::from(start),
            );
            r.headers_mut().insert(
                header::HeaderName::from_static("x-gvfs-pack-end"),
                header::HeaderValue::from(end),
            );
            r
        }
        Outcome::Loose(a, b, c, guard) => respond(
            StatusCode::OK,
            CONTENT_TYPE_LOOSE,
            Body::frames(a, b, c, Some(guard)),
        ),
        Outcome::LooseStream(guard, d) => respond(
            StatusCode::OK,
            CONTENT_TYPE_LOOSE,
            Body::loose(LooseStream::new(guard, d)),
        ),
        Outcome::Objects(guard, cat, exact) => respond(
            StatusCode::OK,
            CONTENT_TYPE_PACKFILE,
            Body::objects(ObjectsStream::new(guard, cat, exact)),
        ),
        Outcome::Sizes(guard, oids_start, count) => respond(
            StatusCode::OK,
            CONTENT_TYPE_JSON,
            Body::sizes(SizesStream::new(guard, oids_start, count)),
        ),
        Outcome::Retry(..) => respond(
            StatusCode::NOT_FOUND,
            "text/plain",
            Body::text("object not found\n"),
        ),
    }
}

/// Reads the request body into the arena (413 if it does not fit).
async fn read_body(mut body: Incoming, guard: &mut ArenaGuard) -> Result<Span, StatusCode> {
    let arena = &mut guard.get().bytes;
    let start = arena.mark();
    loop {
        let frame = std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await;
        match frame {
            None => break,
            Some(Err(_)) => return Err(StatusCode::BAD_REQUEST),
            Some(Ok(f)) => {
                if let Ok(data) = f.into_data() {
                    arena
                        .push(&data)
                        .map_err(|_| StatusCode::PAYLOAD_TOO_LARGE)?;
                }
            }
        }
    }
    Ok(Span::new(start, arena.mark().saturating_sub(start)))
}

fn parse_last_pack_timestamp(query: &str) -> Option<u64> {
    query
        .split('&')
        .find_map(|kv| kv.strip_prefix("lastPackTimestamp="))
        .and_then(|v| v.parse::<u64>().ok())
}

/// `compact=1`: a client hook asking for superset rollups instead of leaves.
fn parse_compact(query: &str) -> bool {
    query
        .split('&')
        .any(|kv| kv == "compact=1" || kv == "compact=true")
}

fn prefetch(state: &State, catalog: &Catalog, query: &str) -> Outcome {
    let since = parse_last_pack_timestamp(query).unwrap_or(0);
    let compact = parse_compact(query);
    let (plan, known, key) = {
        let _hot = gvfs_alloc_guard::HotPathGuard::enter();
        let Ok(list) = catalog.select(since, compact) else {
            return Outcome::Error(StatusCode::INTERNAL_SERVER_ERROR, "selection too large\n");
        };
        if list.is_empty() {
            return Outcome::Full(CONTENT_TYPE_PREFETCH, PrefetchStream::empty());
        }
        let Ok(plan) = Plan::new(list) else {
            return Outcome::Error(StatusCode::INTERNAL_SERVER_ERROR, "corrupt slab index\n");
        };
        state.metrics.prefetch_bytes.fetch_add(
            plan.pack_len.saturating_add(plan.idx_len),
            Ordering::Relaxed,
        );
        let key = selection_key(&plan);
        let known = state.trailers.get(key);
        if known.is_some() {
            state.metrics.trailer_hits.fetch_add(1, Ordering::Relaxed);
        }
        (plan, known, key)
    };
    // Boxed outside the hot region: the plan is ~1.6 KiB of slab handles.
    Outcome::Prefetch(Box::new(plan), known, key)
}

fn loose_object(state: &State, catalog: &Catalog, hex: &str, mut guard: ArenaGuard) -> Outcome {
    let Some(oid) = Oid::from_hex(hex.as_bytes()) else {
        return Outcome::Error(StatusCode::BAD_REQUEST, "bad object id\n");
    };
    let _hot = gvfs_alloc_guard::HotPathGuard::enter();
    let Some((_, slab, _, loc)) = catalog.lookup(&oid) else {
        state.metrics.misses.fetch_add(1, Ordering::Relaxed);
        return Outcome::Error(StatusCode::NOT_FOUND, "object not found\n");
    };
    let entry = slab.entry(loc);
    let Ok(h) = parse_entry_header(entry) else {
        return Outcome::Error(StatusCode::INTERNAL_SERVER_ERROR, "corrupt entry\n");
    };
    if !h.kind.is_delta() {
        // Zero inflate, zero deflate: a stored block + the entry's own deflate blocks.
        let z = entry.get(h.header_len..).unwrap_or(&[]);
        if let Some(s) = loose_splice(h.kind, h.size, z) {
            let start = (loc.offset as usize)
                .saturating_add(h.header_len)
                .saturating_add(2);
            let end = (loc.offset as usize)
                .saturating_add(loc.len as usize)
                .saturating_sub(4);
            let payload = slab.pack.slice(start..end.max(start));
            // A fresh arena: its ring is empty, so the chunk is there.
            let Some(frames) = guard
                .get()
                .frames
                .chunk(s.prefix_len.saturating_add(s.trailer.len()))
            else {
                return Outcome::Error(StatusCode::SERVICE_UNAVAILABLE, "staging busy\n");
            };
            frames.extend_from_slice(s.prefix.get(..s.prefix_len).unwrap_or(&[]));
            let prefix = frames.split().freeze();
            frames.extend_from_slice(&s.trailer);
            let trailer = frames.split().freeze();
            return Outcome::Loose(prefix, payload, trailer, guard);
        }
    }
    // A delta (or an unspliceable stream): resolve into the arena, deflate while streaming.
    let ra = guard.get();
    let mark = ra.bytes.mark();
    let mut inflater = ra.inflater.take().unwrap_or_default();
    let mut chain = core::mem::take(&mut ra.chain);
    let resolved = resolve_full(slab, loc, &mut ra.bytes, &mut inflater, &mut chain);
    ra.inflater = Some(inflater);
    ra.chain = chain;
    match resolved {
        Ok((kind, content)) => Outcome::LooseStream(guard, Deflating::loose(kind, content, mark)),
        Err(_) => Outcome::Error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "object does not fit the arena\n",
        ),
    }
}

fn objects(
    state: &State,
    catalog: &Arc<Catalog>,
    mut guard: ArenaGuard,
    body: Span,
    first_pass: bool,
) -> Outcome {
    let _hot = gvfs_alloc_guard::HotPathGuard::enter();
    let ra = guard.get();
    let Ok(request) = scan_object_ids(&mut ra.bytes, body) else {
        return Outcome::Error(StatusCode::BAD_REQUEST, "malformed JSON\n");
    };
    match plan_objects(catalog, &request, ra) {
        Ok(stats) => {
            if stats.missing > 0 && first_pass && state.misses.enabled {
                // Queue what no slab holds; the request runs again after the fetch round.
                let missing = (0..request.count)
                    .filter_map(|i| oid_at(&ra.bytes, &request, i))
                    .filter(|o| catalog.lookup(o).is_none());
                if let Some(ticket) = state.misses.request(missing) {
                    return Outcome::Retry(guard, body, ticket);
                }
            }
            state
                .metrics
                .objects_served
                .fetch_add(stats.entries as u64, Ordering::Relaxed);
            state
                .metrics
                .misses
                .fetch_add(stats.missing as u64, Ordering::Relaxed);
            if stats.entries == 0 {
                return Outcome::Error(StatusCode::NOT_FOUND, "no requested object is available\n");
            }
            Outcome::Objects(guard, Arc::clone(catalog), stats.exact_len)
        }
        Err(ObjError::PlanFull | ObjError::ArenaFull) => Outcome::Error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request expands beyond the arena\n",
        ),
        Err(_) => Outcome::Error(StatusCode::INTERNAL_SERVER_ERROR, "corrupt entry\n"),
    }
}

fn sizes(catalog: &Catalog, mut guard: ArenaGuard, body: Span) -> Outcome {
    let _hot = gvfs_alloc_guard::HotPathGuard::enter();
    let ra = guard.get();
    let Ok(request) = scan_object_ids(&mut ra.bytes, body) else {
        return Outcome::Error(StatusCode::BAD_REQUEST, "malformed JSON\n");
    };
    let mut inflater = ra.inflater.take().unwrap_or_default();
    ra.nums.clear();
    for i in 0..request.count {
        let size = oid_at(&ra.bytes, &request, i)
            .and_then(|o| catalog.lookup(&o))
            .and_then(|(_, slab, _, loc)| object_info(slab, loc, &mut inflater).ok())
            .map_or(0, |(_, size)| size);
        if ra.nums.len() < ra.nums.capacity() {
            ra.nums.push(size);
        }
    }
    ra.inflater = Some(inflater);
    let count = ra.nums.len();
    Outcome::Sizes(guard, request.oids.start, count)
}

pub async fn handle(
    state: Arc<State>,
    req: Request<Incoming>,
) -> Result<Response<Body>, Infallible> {
    state.metrics.requests.fetch_add(1, Ordering::Relaxed);
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    let Some(at) = path.rfind("/gvfs/") else {
        return Ok(match path.as_str() {
            "/healthz" => respond(StatusCode::OK, "text/plain", Body::text("ok\n")),
            "/metrics" => respond(
                StatusCode::OK,
                "text/plain",
                Body::full(state.metrics_text()),
            ),
            _ => respond(
                StatusCode::NOT_FOUND,
                "text/plain",
                Body::text("not found\n"),
            ),
        });
    };
    let route = path.get(at.saturating_add(6)..).unwrap_or("").to_string();
    let method = req.method().clone();
    let catalog = state.catalog();
    let outcome = match (method, route.as_str()) {
        (Method::GET, "config") => Outcome::Full(CONTENT_TYPE_JSON, state.config_json.clone()),
        (Method::GET, "prefetch") => prefetch(&state, &catalog, &query),
        (Method::GET, r) if r.starts_with("objects/") => {
            let hex = r.get(8..).unwrap_or("");
            match state.pool.lease().await {
                Some(guard) => match loose_object(&state, &catalog, hex, guard) {
                    // Unknown here: have the live thread fetch it, then look once more.
                    Outcome::Error(StatusCode::NOT_FOUND, _) if state.misses.enabled => {
                        match Oid::from_hex(hex.as_bytes())
                            .and_then(|oid| state.misses.request([oid]))
                        {
                            Some(ticket) => {
                                state.misses.wait(ticket, MISS_WAIT).await;
                                match state.pool.lease().await {
                                    Some(guard) => {
                                        loose_object(&state, &state.catalog(), hex, guard)
                                    }
                                    None => Outcome::Error(
                                        StatusCode::SERVICE_UNAVAILABLE,
                                        "no arena\n",
                                    ),
                                }
                            }
                            None => Outcome::Error(StatusCode::NOT_FOUND, "object not found\n"),
                        }
                    }
                    out => out,
                },
                None => Outcome::Error(StatusCode::SERVICE_UNAVAILABLE, "no arena\n"),
            }
        }
        (Method::POST, "objects") => match state.pool.lease().await {
            Some(mut guard) => match read_body(req.into_body(), &mut guard).await {
                Ok(span) => match objects(&state, &catalog, guard, span, true) {
                    Outcome::Retry(mut guard, span, ticket) => {
                        state.misses.wait(ticket, MISS_WAIT).await;
                        guard.get().reset_keep(span.end());
                        objects(&state, &state.catalog(), guard, span, false)
                    }
                    out => out,
                },
                Err(code) => Outcome::Error(code, "request body rejected\n"),
            },
            None => Outcome::Error(StatusCode::SERVICE_UNAVAILABLE, "no arena\n"),
        },
        (Method::POST, "sizes") => match state.pool.lease().await {
            Some(mut guard) => match read_body(req.into_body(), &mut guard).await {
                Ok(span) => sizes(&catalog, guard, span),
                Err(code) => Outcome::Error(code, "request body rejected\n"),
            },
            None => Outcome::Error(StatusCode::SERVICE_UNAVAILABLE, "no arena\n"),
        },
        _ => Outcome::Error(StatusCode::NOT_FOUND, "unknown gvfs endpoint\n"),
    };
    Ok(build(&state, outcome))
}
