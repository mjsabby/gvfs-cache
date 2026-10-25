#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
mod common;

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use gvfs_daemon::codec::{Oid, checksum};
use gvfs_daemon::manifest::Tier;
use gvfs_daemon::storage::{Storage, StoreKind, next_data};
use gvfs_daemon::tree::{TreeLayout, TreeReader};
use http::{Request, Response, StatusCode};
use http_body::{Body, Frame, SizeHint};
use hyper::body::Incoming;
use hyper::service::service_fn;

struct Full(Option<Bytes>);
impl Body for Full {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.0.take().map(|bytes| Ok(Frame::data(bytes))))
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.0.as_ref().map_or(0, |bytes| bytes.len() as u64))
    }
}

#[derive(Default)]
struct Data {
    objects: BTreeMap<String, Bytes>,
    blocks: BTreeMap<(String, String), Bytes>,
    parts: BTreeMap<(String, usize), Bytes>,
}
#[derive(Default)]
struct Fake {
    data: Mutex<Data>,
    fault: AtomicU8,
    ranges: AtomicUsize,
    multipart: AtomicUsize,
}
struct Server {
    url: String,
    fake: Arc<Fake>,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let fake = Arc::new(Fake::default());
        let state = Arc::clone(&fake);
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap(); let state = Arc::clone(&state);
                        connections.spawn(async move {
                            let service = service_fn(move |request| serve(request, Arc::clone(&state)));
                            let _ = hyper::server::conn::http1::Builder::new()
                                .serve_connection(hyper_util::rt::TokioIo::new(socket), service).await;
                        });
                    }
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Self {
            url: format!("http://{address}/bucket?sig=fake"),
            fake,
            stop,
            task,
        }
    }
    fn storage(&self, dir: &std::path::Path, kind: StoreKind, maximum: usize) -> Storage {
        Storage::open(&self.url, dir.into(), 65536, maximum, kind, None, None, 2).unwrap()
    }
    async fn stop(self) {
        self.stop.send(()).unwrap();
        self.task.await.unwrap();
    }
}

fn response(
    status: StatusCode,
    bytes: Bytes,
    etag: Option<String>,
    range: Option<String>,
) -> Response<Full> {
    let mut builder = Response::builder()
        .status(status)
        .header("content-length", bytes.len());
    if let Some(etag) = etag {
        builder = builder.header("etag", etag);
    }
    if let Some(range) = range {
        builder = builder.header("content-range", range);
    }
    builder.body(Full(Some(bytes))).unwrap()
}

fn decode_url(value: &str) -> String {
    let mut out = Vec::new();
    let mut bytes = value.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let pair = [bytes.next().unwrap(), bytes.next().unwrap()];
            out.push(u8::from_str_radix(std::str::from_utf8(&pair).unwrap(), 16).unwrap());
        } else {
            out.push(byte);
        }
    }
    String::from_utf8(out).unwrap()
}

async fn serve(request: Request<Incoming>, fake: Arc<Fake>) -> Result<Response<Full>, Infallible> {
    let (head, mut body) = request.into_parts();
    let key = head.uri.path().strip_prefix("/bucket/").unwrap().to_owned();
    let query: BTreeMap<String, String> = head
        .uri
        .query()
        .unwrap_or_default()
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(key, value)| (key.to_owned(), decode_url(value)))
        .collect();
    assert_eq!(query.get("sig").map(String::as_str), Some("fake"));
    let mut received = Vec::new();
    while let Some(bytes) = next_data(&mut body).await.unwrap() {
        received.extend_from_slice(&bytes);
    }
    let bytes = Bytes::from(received);
    let mut data = fake.data.lock().unwrap();
    let etag = |bytes: &[u8]| format!("\"{}\"", Oid(checksum(bytes)));
    let existing = data.objects.get(&key);
    if head
        .headers
        .get("if-none-match")
        .is_some_and(|value| value == "*")
        && existing.is_some()
        || head.headers.get("if-match").is_some_and(|value| {
            existing.is_none_or(|bytes| value.to_str().unwrap() != etag(bytes))
        })
    {
        return Ok(response(
            StatusCode::PRECONDITION_FAILED,
            Bytes::new(),
            None,
            None,
        ));
    }
    if head.method == http::Method::GET {
        let Some(bytes) = data.objects.get(&key).cloned() else {
            return Ok(response(StatusCode::NOT_FOUND, Bytes::new(), None, None));
        };
        if let Some(range) = head.headers.get("range") {
            fake.ranges.fetch_add(1, Ordering::Relaxed);
            let (start, end) = range
                .to_str()
                .unwrap()
                .strip_prefix("bytes=")
                .unwrap()
                .split_once('-')
                .unwrap();
            let start = start.parse::<usize>().unwrap();
            let end = end.parse::<usize>().unwrap();
            let fault = fake.fault.load(Ordering::Relaxed);
            if fault == 1 {
                return Ok(response(StatusCode::OK, bytes, None, None));
            }
            let contents = bytes.slice(start..=end);
            let contents = if fault == 3 {
                contents.slice(..contents.len() - 1)
            } else {
                contents
            };
            return Ok(response(
                StatusCode::PARTIAL_CONTENT,
                contents,
                None,
                Some(format!(
                    "bytes {}-{end}/{}",
                    if fault == 2 { start + 1 } else { start },
                    bytes.len()
                )),
            ));
        }
        return Ok(response(
            StatusCode::OK,
            bytes.clone(),
            Some(etag(&bytes)),
            None,
        ));
    }
    if head.method == http::Method::PUT && query.get("comp").is_some_and(|value| value == "block") {
        data.blocks.insert((key, query["blockid"].clone()), bytes);
        return Ok(response(StatusCode::CREATED, Bytes::new(), None, None));
    }
    if head.method == http::Method::PUT
        && query.get("comp").is_some_and(|value| value == "blocklist")
    {
        fake.multipart.fetch_add(1, Ordering::Relaxed);
        let xml = std::str::from_utf8(&bytes).unwrap();
        let mut merged = Vec::new();
        for part in xml.split("<Latest>").skip(1) {
            let block = part.split("</Latest>").next().unwrap();
            merged.extend_from_slice(
                &data
                    .blocks
                    .remove(&(key.clone(), block.to_owned()))
                    .unwrap(),
            );
        }
        data.objects.insert(key, Bytes::from(merged));
        return Ok(response(StatusCode::CREATED, Bytes::new(), None, None));
    }
    if head.method == http::Method::POST && query.contains_key("uploads") {
        return Ok(response(StatusCode::OK, Bytes::from_static(b"<InitiateMultipartUploadResult><UploadId>fixture+upload/1=</UploadId></InitiateMultipartUploadResult>"), None, None));
    }
    if head.method == http::Method::PUT && query.contains_key("partNumber") {
        assert_eq!(query["uploadId"], "fixture+upload/1=");
        let tag = etag(&bytes);
        data.parts
            .insert((key, query["partNumber"].parse().unwrap()), bytes);
        return Ok(response(StatusCode::OK, Bytes::new(), Some(tag), None));
    }
    if head.method == http::Method::POST && query.contains_key("uploadId") {
        fake.multipart.fetch_add(1, Ordering::Relaxed);
        let mut merged = Vec::new();
        let mut part = 1usize;
        while let Some(bytes) = data.parts.remove(&(key.clone(), part)) {
            merged.extend_from_slice(&bytes);
            part += 1;
        }
        data.objects.insert(key, Bytes::from(merged));
        return Ok(response(StatusCode::OK, Bytes::from_static(b"<CompleteMultipartUploadResult><ETag>fixture</ETag></CompleteMultipartUploadResult>"), None, None));
    }
    if head.method == http::Method::PUT {
        let tag = etag(&bytes);
        data.objects.insert(key, bytes);
        return Ok(response(StatusCode::CREATED, Bytes::new(), Some(tag), None));
    }
    Ok(response(StatusCode::NO_CONTENT, Bytes::new(), None, None))
}

#[tokio::test]
async fn remote_conditional_manifest_indexes_and_one_cluster_range() {
    let fixture = common::Fixture::new(20);
    let slab = common::decode(
        &fixture.pack,
        fixture.tip(),
        TreeLayout::Clustered,
        Tier::Baseline,
        0,
        100,
        &[],
    );
    let server = Server::start().await;
    let dir = tempfile::tempdir().unwrap();
    let storage = server.storage(dir.path(), StoreKind::Azure, 16 << 20);
    assert!(storage.manifest().await.unwrap().is_none());
    common::publish(&storage, &slab).await;
    storage.upload(&slab).await.unwrap();
    let (manifest, old_etag) = storage.manifest().await.unwrap().unwrap();
    let mut newer = manifest.clone();
    newer.generation += 1;
    storage
        .publish_manifest(&newer, Some(&old_etag))
        .await
        .unwrap();
    assert!(matches!(
        storage.publish_manifest(&manifest, Some(&old_etag)).await,
        Err(gvfs_daemon::Error::Conflict)
    ));
    let remote = storage.load_slab(manifest.baseline).await.unwrap();
    assert!(
        remote.pack.is_none(),
        "edge startup must not download the remote pack"
    );
    let rank = (0..remote.reference.count)
        .find(|rank| remote.record(*rank).unwrap().base != gvfs_daemon::slab::NO_BASE)
        .unwrap();
    let (start, len) = TreeReader::span(&remote, rank).unwrap();
    let mut bytes = vec![0; len];
    storage
        .read_range(remote.reference.id, start, &mut bytes)
        .await
        .unwrap();
    assert_eq!(server.fake.ranges.load(Ordering::Relaxed), 1);
    let mut reader = TreeReader::new(common::limits(TreeLayout::Clustered)).unwrap();
    let size = reader.read_span(&remote, rank, start, &bytes).unwrap();
    assert_eq!(
        gvfs_daemon::codec::object_id(
            gvfs_daemon::codec::Kind::Tree,
            reader.content(size).unwrap()
        )
        .unwrap(),
        remote.record(rank).unwrap().oid
    );
    for fault in [1, 2, 3] {
        server.fake.fault.store(fault, Ordering::Relaxed);
        let cache = tempfile::tempdir().unwrap();
        let fresh = server.storage(cache.path(), StoreKind::Azure, 16 << 20);
        assert!(
            fresh
                .read_range(remote.reference.id, start, &mut bytes)
                .await
                .is_err()
        );
    }
    server.stop().await;
}

#[tokio::test]
async fn azure_block_and_s3_multipart_uploads_preserve_all_bytes() {
    let bytes = Bytes::from(vec![0x5a; (64 << 20) + 4096]);
    for kind in [StoreKind::Azure, StoreKind::S3] {
        let server = Server::start().await;
        let dir = tempfile::tempdir().unwrap();
        let storage = server.storage(dir.path(), kind, 80 << 20);
        storage
            .put_immutable("slabs/large.pack", bytes.clone())
            .await
            .unwrap();
        assert_eq!(server.fake.multipart.load(Ordering::Relaxed), 1);
        assert_eq!(
            server
                .fake
                .data
                .lock()
                .unwrap()
                .objects
                .get("slabs/large.pack"),
            Some(&bytes)
        );
        server.stop().await;
    }
}
