mod body;
pub mod json;
pub mod runtime;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body::Body;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use sha1::{Digest, Sha1};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::arena::{Writer, push, reserve, zeroed};
use crate::codec::{Oid, Zlib, be32, entry_header, loose_header, number, pack_header, take};
use crate::manifest::{MAX_SLABS, SlabRef};
use crate::slab::{Limits, NO_BASE, Slab};
use crate::splice::{SpliceArena, locate};
use crate::storage::{Storage, next_data};
use crate::tree::TreeReader;
use crate::upstream::Upstream;
use crate::{Error, Result};

use body::{ChannelReturn, Chunk, Message, WireBody, send};
use runtime::{Catalog, State};

#[derive(Clone)]
pub struct ServerOptions {
    pub bind: SocketAddr,
    pub advertise_url: Option<String>,
    pub limits: Limits,
    pub live_limits: Limits,
    pub range_bytes: usize,
    pub gap: u64,
    pub max_ranges: usize,
    pub arenas: usize,
    pub request_bytes: usize,
    pub request_objects: usize,
    pub live: bool,
    pub poll_seconds: u64,
    pub manifest_seconds: u64,
    pub snapshot_slots: usize,
    pub connections: usize,
    pub connection_seconds: u64,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    /// Disable only when an embedding application advances synchronization itself.
    pub automatic_sync: bool,
    /// Compatibility/performance control for comparing whole-rollup delivery.
    pub coarse_prefetch: bool,
    pub prefetch_indexes: bool,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], 8080)),
            advertise_url: None,
            limits: Limits::default(),
            live_limits: Limits {
                objects: 65536,
                pack_bytes: 256 << 20,
                links: 1 << 20,
                ..Limits::default()
            },
            range_bytes: 4 << 20,
            gap: 4096,
            max_ranges: 1 << 18,
            arenas: 4,
            request_bytes: 1 << 20,
            request_objects: 16384,
            live: true,
            poll_seconds: 2,
            manifest_seconds: 30,
            snapshot_slots: 8,
            connections: 1024,
            connection_seconds: 300,
            tls_cert: None,
            tls_key: None,
            automatic_sync: true,
            coarse_prefetch: false,
            prefetch_indexes: true,
        }
    }
}

impl ServerOptions {
    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        self.live_limits.validate()?;
        if self.arenas == 0
            || self.request_bytes == 0
            || self.request_objects == 0
            || self.range_bytes < 64
            || self.poll_seconds == 0
            || self.manifest_seconds == 0
            || self.snapshot_slots < 2
            || self.connections == 0
            || self.connection_seconds == 0
            || self.max_ranges == 0
            || self.request_objects > self.limits.objects
            || self.tls_cert.is_some() != self.tls_key.is_some()
        {
            return Err(Error::Usage(
                "invalid server limits or incomplete TLS identity".into(),
            ));
        }
        Ok(())
    }
}

struct Worker {
    plan: SpliceArena,
    oids: Vec<Oid>,
    slabs: Vec<Arc<Slab>>,
    refs: Vec<SlabRef>,
    request: Vec<u8>,
    read: Vec<u8>,
    encoded: Vec<u8>,
    trees: TreeReader,
    graph: crate::graph::Scratch,
    index: Vec<crate::idx::Entry>,
    index_bytes: Vec<u8>,
    zlib: Zlib,
    sender: mpsc::Sender<Message>,
    receiver: Option<mpsc::Receiver<Message>>,
    channel_return: Arc<ChannelReturn>,
}

impl Worker {
    fn new(options: &ServerOptions) -> Result<Self> {
        let scratch = options
            .limits
            .object_bytes
            .checked_add(options.limits.object_bytes / 100)
            .and_then(|n| n.checked_add(4096))
            .ok_or(Error::Capacity("HTTP object scratch"))?
            .max(options.range_bytes);
        let (sender, receiver) = mpsc::channel(2);
        let mut value = Self {
            plan: SpliceArena::new(options.limits.objects, options.max_ranges)?,
            oids: reserve(options.request_objects)?,
            slabs: reserve(MAX_SLABS.saturating_add(2))?,
            refs: reserve(MAX_SLABS.saturating_add(2))?,
            request: zeroed(options.request_bytes)?,
            read: zeroed(scratch)?,
            encoded: zeroed(scratch)?,
            trees: TreeReader::new(options.limits)?,
            graph: crate::graph::Scratch::new(options.limits.object_bytes)?,
            index: reserve(options.limits.objects)?,
            index_bytes: reserve(crate::idx::encoded_len(
                options.limits.objects,
                options.limits.objects,
            )?)?,
            zlib: Zlib::new(1),
            sender,
            receiver: Some(receiver),
            channel_return: ChannelReturn::new(),
        };
        let len = value.zlib.deflate(b"warmup", &mut value.encoded)?;
        value
            .zlib
            .inflate(take(&value.encoded, 0, len)?, &mut value.read, 6)?;
        Ok(value)
    }
    fn reset(&mut self) {
        self.plan.reset();
        self.oids.clear();
        self.slabs.clear();
        self.refs.clear();
        self.index.clear();
        self.index_bytes.clear();
    }
}

pub struct WorkerPool {
    workers: Mutex<Vec<Worker>>,
    permits: Arc<Semaphore>,
}

impl WorkerPool {
    fn new(options: &ServerOptions) -> Result<Arc<Self>> {
        let mut workers = reserve(options.arenas)?;
        for _ in 0..options.arenas {
            push(&mut workers, Worker::new(options)?)?;
        }
        Ok(Arc::new(Self {
            workers: Mutex::new(workers),
            permits: Arc::new(Semaphore::new(options.arenas)),
        }))
    }
    fn take(self: &Arc<Self>) -> Result<WorkerLease> {
        let permit = Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| Error::Capacity("HTTP arenas busy"))?;
        let worker = self
            .workers
            .lock()
            .map_err(|_| Error::Invalid("HTTP arena pool poisoned"))?
            .pop()
            .ok_or(Error::Invalid("HTTP pool accounting"))?;
        Ok(WorkerLease {
            worker: Some(worker),
            pool: Arc::clone(self),
            _permit: permit,
        })
    }
}

struct WorkerLease {
    worker: Option<Worker>,
    pool: Arc<WorkerPool>,
    _permit: OwnedSemaphorePermit,
}

impl WorkerLease {
    fn get(&mut self) -> Result<&mut Worker> {
        self.worker
            .as_mut()
            .ok_or(Error::Invalid("HTTP arena lease"))
    }
}

impl Drop for WorkerLease {
    fn drop(&mut self) {
        if let Some(mut worker) = self.worker.take() {
            worker.reset();
            match self.pool.workers.lock() {
                Ok(mut workers) => workers.push(worker),
                Err(_) => eprintln!("HTTP arena pool poisoned while returning worker"),
            }
        }
    }
}

pub async fn serve(
    storage: Arc<Storage>,
    upstream: Option<Upstream>,
    options: ServerOptions,
) -> Result<()> {
    let (mut state, upstream) = runtime::boot(storage, upstream, options).await?;
    let listener = tokio::net::TcpListener::bind(state.options.bind).await?;
    let address = listener.local_addr()?;
    if state.options.bind.port() == 0 {
        let state = Arc::get_mut(&mut state)
            .ok_or(Error::Invalid("startup state was shared before binding"))?;
        state.options.bind = address;
        state.config = config_json(&state.options)?;
    }
    eprintln!(
        "gvfs-daemon listening on {address}; generation={}; ready",
        state.snapshot()?.manifest.generation
    );
    run_listener(listener, state, upstream, async {
        tokio::signal::ctrl_c().await.map_err(Error::from)
    })
    .await
}

pub async fn run_listener(
    listener: tokio::net::TcpListener,
    state: Arc<State>,
    upstream: Option<Upstream>,
    shutdown: impl std::future::Future<Output = Result<()>>,
) -> Result<()> {
    let tls = tls_identity(&state.options)?;
    let connections = Arc::new(Semaphore::new(state.options.connections));
    let (stop, stop_rx) = tokio::sync::watch::channel(false);
    let synchronizer = if state.options.automatic_sync {
        Some(tokio::spawn(runtime::synchronize(
            Arc::clone(&state),
            upstream,
            stop_rx,
        )))
    } else {
        None
    };
    let mut tasks = tokio::task::JoinSet::new();
    tokio::pin!(shutdown);
    let result = loop {
        let permit = tokio::select! {
            result = &mut shutdown => break result,
            permit = Arc::clone(&connections).acquire_owned() => permit.map_err(|_| Error::Invalid("connection semaphore"))?,
        };
        let accepted = tokio::select! {
            result = &mut shutdown => break result,
            accepted = listener.accept() => accepted,
        };
        let (socket, _) = accepted?;
        socket.set_nodelay(true)?;
        let state = Arc::clone(&state);
        let tls = tls.clone();
        tasks.spawn(async move {
            let _permit = permit;
            let timeout = Duration::from_secs(state.options.connection_seconds);
            let state_for_errors = Arc::clone(&state);
            let connection = async move {
                if let Some(acceptor) = tls {
                    let stream =
                        tokio::time::timeout(Duration::from_secs(10), acceptor.accept(socket))
                            .await
                            .map_err(|_| {
                                Error::Transport("TLS client handshake stalled".into())
                            })??;
                    connection(TokioIo::new(stream), state).await
                } else {
                    connection(TokioIo::new(socket), state).await
                }
            };
            match tokio::time::timeout(timeout, connection).await {
                Ok(Ok(())) => (),
                Ok(Err(error)) => {
                    state_for_errors.errors.fetch_add(1, Ordering::Relaxed);
                    eprintln!("client connection: {error}");
                }
                Err(_) => {
                    state_for_errors.errors.fetch_add(1, Ordering::Relaxed);
                    eprintln!("client connection lifetime exceeded");
                }
            }
        });
        while tasks.try_join_next().is_some() {}
    };
    if synchronizer.is_some() && stop.send(true).is_err() {
        eprintln!("synchronizer already stopped");
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    if let Some(synchronizer) = synchronizer {
        match synchronizer.await {
            Ok(()) => (),
            Err(error) => eprintln!("joining synchronizer: {error}"),
        }
    }
    result
}

async fn connection<T>(io: T, state: Arc<State>) -> Result<()>
where
    T: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    let service = service_fn(move |request| {
        let state = Arc::clone(&state);
        async move { Ok::<_, Infallible>(handle(request, state).await) }
    });
    let mut builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(10));
    builder
        .http2()
        .timer(TokioTimer::new())
        .keep_alive_interval(Some(Duration::from_secs(30)))
        .keep_alive_timeout(Duration::from_secs(10));
    builder
        .serve_connection_with_upgrades(io, service)
        .await
        .map_err(|error| Error::Transport(error.to_string()))
}

fn tls_identity(options: &ServerOptions) -> Result<Option<tokio_rustls::TlsAcceptor>> {
    use rustls::pki_types::pem::PemObject;
    let (Some(cert), Some(key)) = (&options.tls_cert, &options.tls_key) else {
        return Ok(None);
    };
    let certs = rustls::pki_types::CertificateDer::pem_file_iter(cert)
        .map_err(|error| Error::Transport(error.to_string()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| Error::Transport(error.to_string()))?;
    let key = rustls::pki_types::PrivateKeyDer::from_pem_file(key)
        .map_err(|error| Error::Transport(error.to_string()))?;
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|error| Error::Transport(error.to_string()))?
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .map_err(|error| Error::Transport(error.to_string()))?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Some(tokio_rustls::TlsAcceptor::from(Arc::new(config))))
}

pub(crate) fn config_json(options: &ServerOptions) -> Result<Bytes> {
    let url = options.advertise_url.clone().unwrap_or_else(|| {
        format!(
            "{}://{}",
            if options.tls_cert.is_some() {
                "https"
            } else {
                "http"
            },
            options.bind
        )
    });
    let _: http::Uri = url
        .parse()
        .map_err(|_| Error::Usage("invalid advertised URL".into()))?;
    if url
        .bytes()
        .any(|byte| byte < 32 || matches!(byte, b'"' | b'\\'))
    {
        return Err(Error::Invalid("advertised URL JSON"));
    }
    Ok(Bytes::from(format!(
        "{{\"AllowedGvfsClientVersions\":[{{\"Min\":\"0.0\",\"Max\":\"255.255\"}}],\"CacheServers\":[{{\"Name\":\"gvfs-daemon\",\"Url\":\"{url}\",\"GlobalDefault\":true}}],\"BlobStores\":[]}}"
    )))
}

async fn handle(request: Request<Incoming>, state: Arc<State>) -> Response<WireBody> {
    state.requests.fetch_add(1, Ordering::Relaxed);
    let snapshot = match state.snapshot() {
        Ok(snapshot) => snapshot,
        Err(error) => {
            eprintln!("request snapshot: {error}");
            return text_response(StatusCode::SERVICE_UNAVAILABLE, "not ready\n");
        }
    };
    match dispatch(request, Arc::clone(&state), Arc::clone(&snapshot)).await {
        Ok(mut response) => {
            response.body_mut().pin_snapshot(snapshot);
            response
        }
        Err(error) => {
            state.errors.fetch_add(1, Ordering::Relaxed);
            let (status, text) = match &error {
                Error::Missing(_) => (StatusCode::NOT_FOUND, "object not found\n"),
                Error::Capacity("HTTP arenas busy") => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "all worker arenas are busy\n",
                ),
                Error::Capacity(_) => (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "configured capacity exceeded\n",
                ),
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "request failed\n"),
            };
            eprintln!("HTTP request: {error}");
            text_response(status, text)
        }
    }
}

async fn dispatch(
    request: Request<Incoming>,
    state: Arc<State>,
    snapshot: Arc<Catalog>,
) -> Result<Response<WireBody>> {
    let path = request.uri().path();
    if request.method() == Method::GET && path == "/healthz" {
        return Ok(if state.ready.load(Ordering::Acquire) {
            text_response(StatusCode::OK, "ready\n")
        } else {
            text_response(StatusCode::SERVICE_UNAVAILABLE, "synchronization failed\n")
        });
    }
    if request.method() == Method::GET && path == "/metrics" {
        let mut bytes = [0u8; 1024];
        let mut out = Writer::new(&mut bytes);
        for (name, value) in [
            (
                "gvfs_requests_total",
                state.requests.load(Ordering::Relaxed),
            ),
            ("gvfs_errors_total", state.errors.load(Ordering::Relaxed)),
            (
                "gvfs_range_reads_total",
                state.ranges.load(Ordering::Relaxed),
            ),
            (
                "gvfs_hot_allocations_total",
                gvfs_platform::hot_allocations(),
            ),
            ("gvfs_manifest_generation", snapshot.manifest.generation),
        ] {
            out.put(name.as_bytes())?;
            out.put(b" ")?;
            out.decimal(value)?;
            out.put(b"\n")?;
        }
        let mut buffer = state.buffers.take().await?;
        let len = out.len();
        let data = buffer.buffer_mut()?;
        data.bytes
            .get_mut(..len)
            .ok_or(Error::Capacity("metrics buffer"))?
            .copy_from_slice(out.written());
        push(&mut data.spans, 0..len)?;
        buffer.seal()?;
        return response(
            StatusCode::OK,
            "text/plain",
            WireBody::direct([Some(Chunk::Pooled(buffer)), None, None, None]),
        );
    }
    let path = path
        .find("/gvfs/")
        .and_then(|at| path.get(at..))
        .unwrap_or(path);
    if request.method() == Method::GET && path == "/gvfs/config" {
        return response(
            StatusCode::OK,
            "application/json",
            WireBody::bytes(state.config.clone()),
        );
    }
    if request.method() == Method::GET && path == "/gvfs/prefetch" {
        let timestamp = match prefetch_timestamp(request.uri().query()) {
            Ok(timestamp) => timestamp,
            Err(error) => {
                eprintln!("prefetch query: {error}");
                return Ok(text_response(
                    StatusCode::BAD_REQUEST,
                    "invalid prefetch timestamp\n",
                ));
            }
        };
        let mut lease = state.workers.take()?;
        let worker = lease.get()?;
        snapshot.manifest.select(timestamp, &mut worker.refs)?;
        let mut response_timestamp = worker
            .refs
            .iter()
            .map(|reference| reference.end)
            .max()
            .unwrap_or(0);
        for reference in worker.refs.iter().rev() {
            push(&mut worker.slabs, snapshot.find(*reference)?)?;
        }
        if let Some(live) = &snapshot.live
            && live.reference.end > timestamp
        {
            response_timestamp = response_timestamp.max(live.reference.end);
            worker.slabs.insert(0, Arc::clone(live));
        }
        if worker.slabs.is_empty() {
            return response(
                StatusCode::OK,
                PREFETCH_TYPE,
                WireBody::direct([Some(Chunk::inline(b"GPRE \x01\0\0")?), None, None, None]),
            );
        }
        if let [slab] = worker.slabs.as_slice()
            && let Some(pack) = &slab.pack
            && (state.options.coarse_prefetch
                || timestamp == 0
                || timestamp < slab.minimum_epoch()?)
        {
            let header = prefetch_header(
                response_timestamp,
                slab.reference.pack_len,
                Some(slab.idx.len() as u64),
            )?;
            return response(
                StatusCode::OK,
                PREFETCH_TYPE,
                WireBody::direct([
                    Some(Chunk::inline(&header)?),
                    Some(Chunk::Shared(pack.clone())),
                    Some(Chunk::Shared(slab.idx.clone())),
                    None,
                ]),
            );
        }
        {
            let hot = gvfs_platform::HotRegion::enter();
            if state.options.coarse_prefetch {
                worker.plan.plan(
                    &worker.slabs,
                    None,
                    state.options.gap,
                    state.options.range_bytes,
                )?;
            } else {
                worker.plan.plan_after(
                    &worker.slabs,
                    timestamp,
                    state.options.gap,
                    state.options.range_bytes,
                )?;
            }
            if hot.allocations() != 0 {
                return Err(Error::Invalid("prefetch planning allocated"));
            }
            if worker.plan.entries.is_empty() {
                return response(
                    StatusCode::OK,
                    PREFETCH_TYPE,
                    WireBody::direct([Some(Chunk::inline(b"GPRE \x01\0\0")?), None, None, None]),
                );
            }
        }
        let index_len = if state.options.prefetch_indexes {
            let hot = gvfs_platform::HotRegion::enter();
            let len = worker
                .plan
                .prepare_index(&worker.slabs, &mut worker.index)?;
            if hot.allocations() != 0 {
                return Err(Error::Invalid("prefetch index planning allocated"));
            }
            len as u64
        } else {
            0
        };
        let len = worker
            .plan
            .pack_len()?
            .checked_add(32)
            .and_then(|len| len.checked_add(index_len))
            .ok_or(Error::Capacity("prefetch length"))?;
        let timestamp = response_timestamp;
        let body = stream_response(
            lease,
            state,
            snapshot,
            StreamKind::Pack {
                prefetch: Some(timestamp),
            },
            len,
        )?;
        return response(StatusCode::OK, PREFETCH_TYPE, body);
    }
    if request.method() == Method::GET
        && let Some(hex) = path.strip_prefix("/gvfs/objects/")
    {
        let oid = match Oid::parse(hex.as_bytes()) {
            Ok(oid) => oid,
            Err(error) => {
                eprintln!("object request: {error}");
                return Ok(text_response(
                    StatusCode::BAD_REQUEST,
                    "invalid object ID\n",
                ));
            }
        };
        let mut lease = state.workers.take()?;
        let worker = lease.get()?;
        snapshot.points(&mut worker.slabs)?;
        let selected = locate(&worker.slabs, oid)?.ok_or(Error::Missing(oid))?;
        let slab = worker
            .slabs
            .get(selected.slab as usize)
            .ok_or(Error::Invalid("loose slab"))?;
        let record = slab.record(selected.ordinal)?;
        if record.base == NO_BASE
            && let Some(pack) = &slab.pack
        {
            return loose_from_pack(pack, record);
        }
        let len = prepare_loose(&state, worker, selected).await?;
        let body = stream_response(
            lease,
            state,
            snapshot,
            StreamKind::Encoded { len },
            len as u64,
        )?;
        return response(StatusCode::OK, LOOSE_TYPE, body);
    }
    if request.method() == Method::POST && matches!(path, "/gvfs/objects" | "/gvfs/sizes") {
        let sizes = path == "/gvfs/sizes";
        let mut lease = state.workers.take()?;
        let worker = lease.get()?;
        let mut incoming = request.into_body();
        let received: Result<usize> = tokio::time::timeout(Duration::from_secs(10), async {
            let mut used = 0usize;
            while let Some(bytes) = next_data(&mut incoming).await? {
                let end = used
                    .checked_add(bytes.len())
                    .ok_or(Error::Capacity("request body"))?;
                worker
                    .request
                    .get_mut(used..end)
                    .ok_or(Error::Capacity("request body"))?
                    .copy_from_slice(&bytes);
                used = end;
            }
            Ok(used)
        })
        .await
        .map_err(|_| Error::Transport("request body deadline exceeded".into()))?;
        let used = received?;
        let parsed = {
            let _hot = gvfs_platform::HotRegion::enter();
            if sizes {
                json::sizes_request(take(&worker.request, 0, used)?, &mut worker.oids).map(|()| 0)
            } else {
                json::object_request(take(&worker.request, 0, used)?, &mut worker.oids)
            }
        };
        let depth = match parsed {
            Ok(depth) => depth,
            Err(Error::Capacity(what)) => return Err(Error::Capacity(what)),
            Err(error) => {
                eprintln!("objects JSON: {error}");
                return Ok(text_response(
                    StatusCode::BAD_REQUEST,
                    "invalid object request\n",
                ));
            }
        };
        snapshot.points(&mut worker.slabs)?;
        if sizes {
            return sizes_response(&state, worker).await;
        }
        {
            let hot = gvfs_platform::HotRegion::enter();
            worker.plan.plan_objects(
                &worker.slabs,
                &worker.oids,
                depth,
                &mut worker.graph,
                state.options.gap,
                state.options.range_bytes,
            )?;
            if hot.allocations() != 0 {
                return Err(Error::Invalid("object planning allocated"));
            }
        }
        let len = worker.plan.pack_len()?;
        let body = stream_response(
            lease,
            state,
            snapshot,
            StreamKind::Pack { prefetch: None },
            len,
        )?;
        return response(StatusCode::OK, PACK_TYPE, body);
    }
    Ok(text_response(StatusCode::NOT_FOUND, "not found\n"))
}

const PREFETCH_TYPE: &str = "application/x-gvfs-timestamped-packfiles-indexes";
const PACK_TYPE: &str = "application/x-git-packfile";
const LOOSE_TYPE: &str = "application/x-git-loose-object";

fn response(
    status: StatusCode,
    content_type: &'static str,
    body: WireBody,
) -> Result<Response<WireBody>> {
    let len = body
        .size_hint()
        .exact()
        .ok_or(Error::Invalid("response length"))?;
    Ok(Response::builder()
        .status(status)
        .header("content-type", content_type)
        .header("content-length", len)
        .header("cache-control", "no-store")
        .body(body)?)
}

fn text_response(status: StatusCode, text: &'static str) -> Response<WireBody> {
    let mut response = Response::new(WireBody::text(text));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert("content-type", http::HeaderValue::from_static("text/plain"));
    response
}

pub fn prefetch_timestamp(query: Option<&str>) -> Result<u64> {
    let Some(query) = query else {
        return Ok(0);
    };
    let mut value = None;
    for pair in query.split('&') {
        let (key, bytes) = pair
            .split_once('=')
            .ok_or(Error::Invalid("prefetch query"))?;
        if key != "lastPackTimestamp" || value.is_some() {
            return Err(Error::Invalid("prefetch query field"));
        }
        let timestamp = number(bytes.as_bytes())?;
        if timestamp > i64::MAX as u64 {
            return Err(Error::Invalid("prefetch timestamp range"));
        }
        value = Some(timestamp);
    }
    value.ok_or(Error::Invalid("prefetch timestamp"))
}

pub fn prefetch_header(timestamp: u64, pack_len: u64, idx_len: Option<u64>) -> Result<[u8; 32]> {
    let timestamp = i64::try_from(timestamp).map_err(|_| Error::Capacity("GPRE timestamp"))?;
    let pack_len = i64::try_from(pack_len).map_err(|_| Error::Capacity("GPRE pack length"))?;
    let idx_len = idx_len
        .map(i64::try_from)
        .transpose()
        .map_err(|_| Error::Capacity("GPRE index length"))?
        .unwrap_or(-1);
    let mut bytes = [0; 32];
    let mut writer = Writer::new(&mut bytes);
    writer.put(b"GPRE \x01\x01\0")?;
    writer.put(&timestamp.to_le_bytes())?;
    writer.put(&pack_len.to_le_bytes())?;
    writer.put(&idx_len.to_le_bytes())?;
    Ok(bytes)
}

async fn sizes_response(state: &State, worker: &mut Worker) -> Result<Response<WireBody>> {
    let mut buffer = state.buffers.take().await?;
    let data = buffer.buffer_mut()?;
    let mut writer = Writer::new(&mut data.bytes);
    writer.put(b"[")?;
    for (i, &oid) in worker.oids.iter().enumerate() {
        let selected = locate(&worker.slabs, oid)?.ok_or(Error::Missing(oid))?;
        let record = worker
            .slabs
            .get(selected.slab as usize)
            .ok_or(Error::Invalid("sizes slab"))?
            .record(selected.ordinal)?;
        if i != 0 {
            writer.put(b",")?;
        }
        writer.put(b"{\"Id\":\"")?;
        writer.put(&oid.hex())?;
        writer.put(b"\",\"Size\":")?;
        writer.decimal(record.size)?;
        writer.put(b"}")?;
    }
    writer.put(b"]")?;
    let len = writer.len();
    push(&mut data.spans, 0..len)?;
    buffer.seal()?;
    response(
        StatusCode::OK,
        "application/json",
        WireBody::direct([Some(Chunk::Pooled(buffer)), None, None, None]),
    )
}

/// Prepend a non-final stored deflate block with the Git loose header, then reuse
/// the original deflate stream and combine Adler checksums. No inflate or deflate.
fn loose_from_pack(pack: &Bytes, record: crate::slab::Record) -> Result<Response<WireBody>> {
    let at = record.offset as usize;
    let raw = take(pack, at, record.len as usize)?;
    let header = entry_header(raw, record.offset)?;
    if header.kind != record.kind || header.size as u64 != record.size {
        return Err(Error::Invalid("loose pack header"));
    }
    let zlib = raw.get(header.len..).ok_or(Error::Invalid("loose zlib"))?;
    let trailer = zlib
        .len()
        .checked_sub(4)
        .filter(|end| *end >= 2)
        .ok_or(Error::Invalid("zlib trailer"))?;
    if zlib.get(1).is_none_or(|flg| flg & 32 != 0) {
        return Err(Error::Invalid("zlib dictionary"));
    }
    let mut git_header = [0; 40];
    let len = loose_header(record.kind, record.size, &mut git_header)?;
    let git_header = take(&git_header, 0, len)?;
    let mut prefix = [0; 64];
    let mut writer = Writer::new(&mut prefix);
    writer.put(&[0x78, 0x01, 0])?;
    writer.put(&(len as u16).to_le_bytes())?;
    writer.put(&(!(len as u16)).to_le_bytes())?;
    writer.put(git_header)?;
    let adler = combine_adler(adler32(git_header), be32(zlib, trailer)?, record.size).to_be_bytes();
    let start = at.saturating_add(header.len).saturating_add(2);
    let end = at.saturating_add(header.len).saturating_add(trailer);
    response(
        StatusCode::OK,
        LOOSE_TYPE,
        WireBody::direct([
            Some(Chunk::inline(writer.written())?),
            Some(Chunk::Shared(pack.slice(start..end))),
            Some(Chunk::inline(&adler)?),
            None,
        ]),
    )
}

fn adler32(bytes: &[u8]) -> u32 {
    let mut a = 1u32;
    let mut b = 0u32;
    for &byte in bytes {
        a = a.wrapping_add(u32::from(byte)) % 65521;
        b = b.wrapping_add(a) % 65521;
    }
    (b << 16) | a
}

fn combine_adler(first: u32, second: u32, second_len: u64) -> u32 {
    let modulus = 65521u64;
    let a1 = u64::from(first & 65535);
    let b1 = u64::from(first >> 16);
    let a2 = u64::from(second & 65535);
    let b2 = u64::from(second >> 16);
    let a = a1.wrapping_add(a2).wrapping_add(modulus).wrapping_sub(1) % modulus;
    let b = b1.wrapping_add(b2).wrapping_add(
        (second_len % modulus).wrapping_mul(a1.wrapping_add(modulus).wrapping_sub(1)),
    ) % modulus;
    ((b as u32) << 16) | a as u32
}

async fn prepare_loose(
    state: &State,
    worker: &mut Worker,
    selected: crate::splice::Selection,
) -> Result<usize> {
    let slab = worker
        .slabs
        .get(selected.slab as usize)
        .ok_or(Error::Invalid("remote loose slab"))?;
    let record = slab.record(selected.ordinal)?;
    let (start, span) = TreeReader::span(slab, selected.ordinal)?;
    let raw = worker
        .read
        .get_mut(..span)
        .ok_or(Error::Capacity("loose range scratch"))?;
    if let Some(pack) = &slab.pack {
        raw.copy_from_slice(take(pack, start as usize, span)?);
    } else {
        state.ranges.fetch_add(1, Ordering::Relaxed);
        state
            .storage
            .read_range(slab.reference.id, start, raw)
            .await?;
    }
    let len = worker.trees.read_span(slab, selected.ordinal, start, raw)?;
    let mut header = [0; 40];
    let header_len = loose_header(record.kind, len as u64, &mut header)?;
    // A reusable byte arena holds the header plus canonical bytes; compression uses
    // the other fixed arena, never a temporary Vec.
    let total = header_len
        .checked_add(len)
        .ok_or(Error::Capacity("loose bytes"))?;
    worker
        .read
        .get_mut(..header_len)
        .ok_or(Error::Capacity("loose header"))?
        .copy_from_slice(take(&header, 0, header_len)?);
    worker
        .read
        .get_mut(header_len..total)
        .ok_or(Error::Capacity("loose object"))?
        .copy_from_slice(worker.trees.content(len)?);
    worker
        .zlib
        .deflate(take(&worker.read, 0, total)?, &mut worker.encoded)
}

enum StreamKind {
    Pack { prefetch: Option<u64> },
    Encoded { len: usize },
}

fn stream_response(
    mut lease: WorkerLease,
    state: Arc<State>,
    snapshot: Arc<Catalog>,
    kind: StreamKind,
    len: u64,
) -> Result<WireBody> {
    let worker = lease.get()?;
    let receiver = worker
        .receiver
        .take()
        .ok_or(Error::Invalid("worker response receiver"))?;
    let returned = Arc::clone(&worker.channel_return);
    returned.begin();
    let body = WireBody::Stream {
        receiver: Some(receiver),
        state: Arc::clone(&returned),
        remaining: len,
        ended: false,
    };
    tokio::spawn(async move {
        let result = produce(&mut lease, &state, &snapshot, kind).await;
        if let Err(error) = result {
            if !returned.closed.load(Ordering::Acquire) {
                state.errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("streaming pack: {error}");
                if let Ok(worker) = lease.get() {
                    let _ = send(&worker.sender, &returned, Message::Failed(error)).await;
                }
            }
        } else if let Ok(worker) = lease.get() {
            let _ = send(&worker.sender, &returned, Message::End).await;
        }
        loop {
            let receiver = match returned.returned.lock() {
                Ok(mut slot) => slot.take(),
                Err(_) => {
                    eprintln!("response channel return poisoned");
                    None
                }
            };
            if let Some(mut receiver) = receiver {
                while receiver.try_recv().is_ok() {}
                if let Ok(worker) = lease.get() {
                    worker.receiver = Some(receiver);
                }
                break;
            }
            returned.notify.notified().await;
        }
        drop(snapshot);
    });
    Ok(body)
}

async fn produce(
    lease: &mut WorkerLease,
    state: &State,
    snapshot: &Arc<Catalog>,
    kind: StreamKind,
) -> Result<()> {
    let worker = lease.get()?;
    let sender = &worker.sender;
    let returned = &worker.channel_return;
    if let StreamKind::Encoded { len } = kind {
        for bytes in take(&worker.encoded, 0, len)?.chunks(state.options.range_bytes) {
            send_copy(state, sender, returned, bytes).await?;
        }
        return Ok(());
    }
    let StreamKind::Pack { prefetch } = kind else {
        return Err(Error::Invalid("stream kind"));
    };
    if let Some(timestamp) = prefetch {
        send(
            sender,
            returned,
            Message::Data(Chunk::inline(&prefetch_header(
                timestamp,
                worker.plan.pack_len()?,
                if state.options.prefetch_indexes {
                    Some(crate::idx::encoded_len(
                        worker.index.len(),
                        worker
                            .index
                            .iter()
                            .filter(|entry| entry.offset > 0x7fff_ffff)
                            .count(),
                    )? as u64)
                } else {
                    None
                },
            )?)?),
        )
        .await?;
    }
    let header = pack_header(
        u32::try_from(worker.plan.entries.len())
            .map_err(|_| Error::Capacity("wire object count"))?,
    );
    let mut sha = Sha1::new();
    sha.update(header);
    send(sender, returned, Message::Data(Chunk::inline(&header)?)).await?;
    for range in &worker.plan.ranges {
        let slab = worker
            .slabs
            .get(range.slab as usize)
            .ok_or(Error::Invalid("stream range slab"))?;
        let len = usize::try_from(
            range
                .end
                .checked_sub(range.start)
                .ok_or(Error::Invalid("stream range order"))?,
        )
        .map_err(|_| Error::Capacity("stream range length"))?;
        let remote = if slab.pack.is_none() {
            let bytes = worker
                .read
                .get_mut(..len)
                .ok_or(Error::Capacity("stream range scratch"))?;
            state.ranges.fetch_add(1, Ordering::Relaxed);
            state
                .storage
                .read_range(slab.reference.id, range.start, bytes)
                .await?;
            Some(bytes as &[u8])
        } else {
            None
        };
        let finish = (range.first as usize).saturating_add(range.count as usize);
        let entries = worker
            .plan
            .entries
            .get(range.first as usize..finish)
            .ok_or(Error::Invalid("stream range entries"))?;
        if let Some(bytes) = remote {
            for entry in entries {
                let record = slab.record(entry.ordinal)?;
                let relative = record
                    .offset
                    .checked_sub(range.start)
                    .ok_or(Error::Invalid("range entry offset"))?;
                let raw = take(
                    bytes,
                    usize::try_from(relative).map_err(|_| Error::Invalid("range offset width"))?,
                    record.len as usize,
                )?;
                if crc32fast::hash(raw) != record.crc {
                    return Err(Error::Invalid("remote pack entry CRC"));
                }
            }
        }
        let mut cursor = 0usize;
        while cursor < entries.len() {
            let selected = *entries
                .get(cursor)
                .ok_or(Error::Invalid("stream selection"))?;
            let record = slab.record(selected.ordinal)?;
            let mut replacement = [0; 20];
            let (new_header, skip) =
                worker
                    .plan
                    .rewrite(&worker.slabs, selected, &mut replacement)?;
            let begin = record.offset;
            let mut end = begin
                .checked_add(u64::from(record.len))
                .ok_or(Error::Invalid("stream entry end"))?;
            let mut next = cursor.saturating_add(1);
            if new_header == 0 {
                while let Some(&following) = entries.get(next) {
                    let following_record = slab.record(following.ordinal)?;
                    let (rewrite, _) =
                        worker
                            .plan
                            .rewrite(&worker.slabs, following, &mut replacement)?;
                    if rewrite != 0 || following_record.offset != end {
                        break;
                    }
                    end = end
                        .checked_add(u64::from(following_record.len))
                        .ok_or(Error::Invalid("stream run end"))?;
                    next = next.saturating_add(1);
                }
            } else {
                let header = take(&replacement, 0, new_header)?;
                sha.update(header);
                send(sender, returned, Message::Data(Chunk::inline(header)?)).await?;
            }
            let source_start = begin
                .checked_add(skip as u64)
                .ok_or(Error::Invalid("stream source header"))?;
            let source_len = usize::try_from(
                end.checked_sub(source_start)
                    .ok_or(Error::Invalid("stream source range"))?,
            )
            .map_err(|_| Error::Capacity("stream run length"))?;
            if let Some(remote) = remote {
                let bytes = take(
                    remote,
                    usize::try_from(source_start.saturating_sub(range.start))
                        .map_err(|_| Error::Invalid("remote relative range"))?,
                    source_len,
                )?;
                sha.update(bytes);
                for chunk in bytes.chunks(state.options.range_bytes) {
                    send_copy(state, sender, returned, chunk).await?;
                }
            } else {
                let pack = slab
                    .pack
                    .as_ref()
                    .ok_or(Error::Invalid("mapped stream pack"))?;
                let start = usize::try_from(source_start)
                    .map_err(|_| Error::Invalid("mapped run offset"))?;
                let end = start
                    .checked_add(source_len)
                    .ok_or(Error::Invalid("mapped run bounds"))?;
                sha.update(take(pack, start, source_len)?);
                let mut chunk = Chunk::Shared(pack.slice(start..end));
                chunk.pin_snapshot(Arc::clone(snapshot));
                send(sender, returned, Message::Data(chunk)).await?;
            }
            cursor = next;
        }
    }
    let checksum: [u8; 20] = sha.finalize().into();
    send(sender, returned, Message::Data(Chunk::inline(&checksum)?)).await?;
    if prefetch.is_some() && state.options.prefetch_indexes {
        {
            let hot = gvfs_platform::HotRegion::enter();
            crate::idx::write_sorted(&worker.index, Oid(checksum), &mut worker.index_bytes)?;
            if hot.allocations() != 0 {
                return Err(Error::Invalid("prefetch index serialization allocated"));
            }
        }
        for bytes in worker.index_bytes.chunks(state.options.range_bytes) {
            send_copy(state, sender, returned, bytes).await?;
        }
    }
    Ok(())
}

async fn send_copy(
    state: &State,
    sender: &mpsc::Sender<Message>,
    returned: &ChannelReturn,
    bytes: &[u8],
) -> Result<()> {
    let mut buffer = state.buffers.take().await?;
    let data = buffer.buffer_mut()?;
    data.bytes
        .get_mut(..bytes.len())
        .ok_or(Error::Capacity("stream buffer"))?
        .copy_from_slice(bytes);
    push(&mut data.spans, 0..bytes.len())?;
    buffer.seal()?;
    send(sender, returned, Message::Data(Chunk::Pooled(buffer))).await
}
