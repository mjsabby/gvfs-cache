//! The upstream git server: where `ingest_git_delta` pulls `have`/`want` deltas from.
//!
//! Speaks Git protocol v2 (`ls-refs`, `fetch` with `done`, so one round trip and no
//! negotiation) and falls back to the v0 smart protocol for servers without v2. Two
//! transports carry the same bytes:
//!
//! * smart HTTP(S): `POST <url>/git-upload-pack` (Azure DevOps, GitHub, any git server);
//! * a local repository: `git upload-pack --stateless-rpc <path>` (mirrors, tests).
//!
//! Packs are always requested non-thin (every delta base is inside the pack), so each
//! fetched pack is a self-contained slab.
//!
//! Stores that keep blobs apart (`--blobs separate`) also need: commits and trees without
//! blobs (`filter blob:none`), blobs by id, and blob sizes. A local repository serves them
//! through `upload-pack` (allowed here with `-c`); an HTTP upstream that speaks the GVFS
//! protocol (Azure DevOps, another gvfs-daemon) through `POST /gvfs/objects` and
//! `POST /gvfs/sizes`, otherwise through protocol v2 (`filter`, `want <id>`, `object-info`)
//! where the server advertises it.
//!
//! [`AsyncUpstream`] makes every request, on the async client or an async `git upload-pack`,
//! so that a fetch parks its task, not a thread: the edge's blob proxy uses it while it
//! serves. [`Upstream`] makes the same requests for threads that block (the pipeline, the
//! mirror, the edge's live loop), on a runtime of its own.

use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use crate::http::Url;
use crate::pktline::{self, Pkt};
use crate::util::{Context, Oid, Result};

const AGENT: &str = concat!("agent=gvfs-daemon/", env!("CARGO_PKG_VERSION"), "\n");

/// Ids per `object-info` / `/gvfs/sizes` request, and per blob batch.
pub const BATCH: usize = 4000;

/// A password in the upstream URL would not be used (credentials come from the
/// environment), yet would be on the command line for every local user to read: refused.
/// A user name alone (ADO's clone URLs: `https://org@dev.azure.com/...`) is fine.
fn refuse_password(spec: &str) -> Result<()> {
    if crate::http::has_password(spec) {
        crate::bail!(
            "upstream {}: the URL holds a password, which is not used: pass the token in an \
             environment variable named by --token-env (and --token-user), or --auth-header-env",
            crate::http::redacted(spec)
        );
    }
    Ok(())
}

/// Credentials for an HTTP upstream.
#[derive(Clone, Default)]
pub struct Auth {
    /// Full `Authorization` header value.
    pub header: Option<String>,
}

/// The upstream for threads that block: [`AsyncUpstream`]'s requests, each run to its end on
/// a runtime of this upstream's own (on the calling thread). Not for use inside a runtime.
pub struct Upstream {
    inner: AsyncUpstream,
    rt: tokio::runtime::Runtime,
    /// `refs/heads/<branch>`.
    branch_ref: String,
}

impl Upstream {
    /// `spec` is an http(s) URL or a local repository path.
    pub fn open(spec: &str, branch: &str, auth: &Auth) -> Result<Upstream> {
        let inner = AsyncUpstream::open(spec, auth)?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .ctx("starting the upstream's runtime")?;
        let branch_ref = if branch.starts_with("refs/") {
            branch.to_string()
        } else {
            format!("refs/heads/{branch}")
        };
        Ok(Upstream {
            inner,
            rt,
            branch_ref,
        })
    }

    /// Human-readable description for logs (no credentials).
    pub fn label(&self) -> &str {
        &self.inner.label
    }

    /// The current tip of the followed branch.
    pub fn head(&mut self) -> Result<Oid> {
        self.rt.block_on(self.inner.head(&self.branch_ref))
    }

    /// Fetches the objects reachable from `want` but not from any of `haves` (none =
    /// everything) as one non-thin pack, written to `out`. Returns the pack length. Haves
    /// the upstream does not know (history rewritten) are ignored by it.
    pub fn fetch(&mut self, haves: &[Oid], want: &Oid, out: &mut dyn Write) -> Result<u64> {
        self.rt
            .block_on(self.inner.fetch(haves, want, None, &mut ToWriter(out)))
    }

    /// Like [`Upstream::fetch`] with a filter (`blob:none`: commits and trees only).
    pub fn fetch_filtered(
        &mut self,
        haves: &[Oid],
        want: &Oid,
        filter: &str,
        out: &mut dyn Write,
    ) -> Result<u64> {
        self.rt.block_on(
            self.inner
                .fetch(haves, want, Some(filter), &mut ToWriter(out)),
        )
    }

    /// Fetches exactly the objects `oids` (blobs) as one pack written to `out`: through
    /// `POST /gvfs/objects` from a GVFS upstream, else as protocol v2 `want`s. Returns the
    /// pack length.
    pub fn fetch_objects(&mut self, oids: &[Oid], out: &mut dyn Write) -> Result<u64> {
        self.rt
            .block_on(self.inner.fetch_objects(oids, &mut ToWriter(out)))
    }

    /// The sizes of objects `oids`, in order (`None`: the upstream does not have it):
    /// `POST /gvfs/sizes` from a GVFS upstream, else protocol v2 `object-info`.
    pub fn object_sizes(&mut self, oids: &[Oid]) -> Result<Vec<Option<u64>>> {
        self.rt.block_on(self.inner.object_sizes(oids))
    }
}

/// A fetched pack's pieces, written to a blocking writer.
struct ToWriter<'a>(&'a mut dyn Write);

impl PackPieces for ToWriter<'_> {
    async fn put(&mut self, piece: bytes::Bytes) -> Result<()> {
        self.0.write_all(&piece).ctx("writing pack")
    }
}

/// Whether an upstream offers protocol v2 capability `name` (`fetch` features are matched
/// as `fetch:<feature>`). Local repositories offer everything we ask for.
fn has_cap(local: bool, caps: &[String], name: &str) -> bool {
    if local {
        return true;
    }
    caps.iter().any(|c| {
        if let Some(feature) = name.strip_prefix("fetch:") {
            c.strip_prefix("fetch=")
                .is_some_and(|v| v.split(' ').any(|f| f == feature))
        } else {
            c == name || c.strip_prefix(name).is_some_and(|r| r.starts_with('='))
        }
    })
}

/// `{"objectIds":[...],"commitDepth":1}` for `POST /gvfs/objects`.
fn gvfs_objects_body(oids: &[Oid]) -> String {
    let mut body = String::with_capacity(oids.len().saturating_mul(43).saturating_add(40));
    body.push_str("{\"objectIds\":[");
    for (i, o) in oids.iter().enumerate() {
        if i > 0 {
            body.push(',');
        }
        body.push('"');
        body.push_str(&o.to_string());
        body.push('"');
    }
    body.push_str("],\"commitDepth\":1}");
    body
}

/// `["<id>",...]` for `POST /gvfs/sizes`.
fn gvfs_sizes_body(oids: &[Oid]) -> String {
    let mut body = String::with_capacity(oids.len().saturating_mul(43).saturating_add(2));
    body.push('[');
    for (i, o) in oids.iter().enumerate() {
        if i > 0 {
            body.push(',');
        }
        body.push('"');
        body.push_str(&o.to_string());
        body.push('"');
    }
    body.push(']');
    body
}

/// A protocol v2 `object-info` request for the sizes of `oids`.
fn object_info_request(oids: &[Oid]) -> Vec<u8> {
    let mut req = Vec::new();
    pktline::put_str(&mut req, "command=object-info\n");
    pktline::put_str(&mut req, AGENT);
    pktline::delim(&mut req);
    pktline::put_str(&mut req, "size\n");
    for o in oids {
        pktline::put_str(&mut req, &format!("oid {o}\n"));
    }
    pktline::flush(&mut req);
    req
}

/// The sizes in an `object-info` response.
fn parse_object_info(r: &mut dyn Read, n: usize) -> Result<std::collections::HashMap<Oid, u64>> {
    let mut rd = pktline::Reader::new(r);
    let mut found = std::collections::HashMap::with_capacity(n);
    loop {
        match rd.read()? {
            Pkt::Data(n) => {
                let line = rd.line(n);
                check_err(line)?;
                if line == b"size" {
                    continue;
                }
                let (hex, rest) = line.split_at_checked(40).ctx("bad object-info line")?;
                let oid = Oid::from_hex(hex).ctx("bad object-info id")?;
                let size = std::str::from_utf8(rest.strip_prefix(b" ").unwrap_or(rest))
                    .ok()
                    .and_then(|v| v.trim().parse::<u64>().ok());
                if let Some(sz) = size {
                    found.insert(oid, sz);
                }
            }
            Pkt::Flush | Pkt::Eof => return Ok(found),
            _ => {}
        }
    }
}

/// A one-round-trip fetch request: protocol v2 `fetch` with `done`, or v0 `want`s with
/// `side-band-64k`; never thin, no progress.
fn fetch_request(v2: bool, haves: &[Oid], wants: &[Oid], filter: Option<&str>) -> Result<Vec<u8>> {
    let want = wants.first().copied().ctx("fetch without a want")?;
    let mut req = Vec::new();
    if v2 {
        pktline::put_str(&mut req, "command=fetch\n");
        pktline::put_str(&mut req, AGENT);
        pktline::delim(&mut req);
        pktline::put_str(&mut req, "ofs-delta\n");
        pktline::put_str(&mut req, "no-progress\n");
        for w in wants {
            pktline::put_str(&mut req, &format!("want {w}\n"));
        }
        for have in haves.iter().filter(|h| !h.is_zero()) {
            pktline::put_str(&mut req, &format!("have {have}\n"));
        }
        if let Some(f) = filter {
            pktline::put_str(&mut req, &format!("filter {f}\n"));
        }
        pktline::put_str(&mut req, "done\n");
        pktline::flush(&mut req);
    } else {
        pktline::put_str(
            &mut req,
            &format!(
                "want {want} multi_ack_detailed side-band-64k ofs-delta no-progress {}",
                AGENT.trim_end()
            ),
        );
        pktline::flush(&mut req);
        for have in haves.iter().filter(|h| !h.is_zero()) {
            pktline::put_str(&mut req, &format!("have {have}\n"));
        }
        pktline::put_str(&mut req, "done\n");
    }
    Ok(req)
}

/// Removes configuration injected through the environment (`GIT_CONFIG_COUNT` and
/// friends) so the child git sees only the repository's own configuration.
pub fn hermetic_git_env(cmd: &mut Command) {
    for (k, _) in std::env::vars_os() {
        let k = k.to_string_lossy();
        if k == "GIT_CONFIG_COUNT"
            || k.starts_with("GIT_CONFIG_KEY_")
            || k.starts_with("GIT_CONFIG_VALUE_")
            || k == "GIT_CONFIG_PARAMETERS"
        {
            cmd.env_remove(&*k);
        }
    }
}

/// The capability lines of a protocol v2 advertisement (after `version 2`).
fn advertised_caps(body: &[u8]) -> Result<Vec<String>> {
    let mut rd = pktline::Reader::new(body);
    let mut caps = Vec::new();
    let mut started = false;
    loop {
        match rd.read()? {
            Pkt::Data(n) => {
                let line = rd.line(n);
                if line.starts_with(b"# service=") {
                    continue;
                }
                if line == b"version 2" {
                    started = true;
                    continue;
                }
                if started {
                    caps.push(String::from_utf8_lossy(line).into_owned());
                }
            }
            Pkt::Flush if !started => continue,
            _ => return Ok(caps),
        }
    }
}

/// `[{"Id":"<hex>","Size":<n>},...]` (any field order, any spacing).
fn parse_gvfs_sizes(text: &[u8]) -> std::collections::HashMap<Oid, u64> {
    let mut found = std::collections::HashMap::new();
    for obj in text.split(|&b| b == b'}') {
        let field = |name: &[u8]| -> Option<&[u8]> {
            let at = obj
                .windows(name.len())
                .position(|w| w.eq_ignore_ascii_case(name))?;
            let rest = obj.get(at.checked_add(name.len())?..)?;
            let colon = rest.iter().position(|&b| b == b':')?;
            let v = rest.get(colon.checked_add(1)?..)?;
            let start = v
                .iter()
                .position(|b| !b.is_ascii_whitespace() && *b != b'"')?;
            let v = v.get(start..)?;
            let end = v
                .iter()
                .position(|&b| b == b'"' || b == b',' || b.is_ascii_whitespace())
                .unwrap_or(v.len());
            v.get(..end)
        };
        let (Some(id), Some(size)) = (field(b"\"Id\""), field(b"\"Size\"")) else {
            continue;
        };
        if let (Some(oid), Some(n)) = (
            Oid::from_hex(id),
            std::str::from_utf8(size).ok().and_then(|v| v.parse().ok()),
        ) {
            found.insert(oid, n);
        }
    }
    found
}

/// The id of `branch_ref` in an `ls-refs` response.
fn ls_refs_tip(body: &[u8], branch_ref: &str) -> Result<Option<Oid>> {
    let mut rd = pktline::Reader::new(body);
    let mut found = None;
    loop {
        match rd.read()? {
            Pkt::Data(n) => {
                let line = rd.line(n);
                check_err(line)?;
                let (hex, rest) = line.split_at_checked(40).ctx("bad ls-refs line")?;
                let name = rest.strip_prefix(b" ").unwrap_or(rest);
                let name = name.split(|&b| b == b' ').next().unwrap_or(name);
                if name == branch_ref.as_bytes() {
                    found = Oid::from_hex(hex);
                }
            }
            Pkt::Flush | Pkt::Eof => return Ok(found),
            _ => {}
        }
    }
}

/// The id of `branch_ref` in a v0 ref advertisement.
fn v0_refs_tip(body: &[u8], branch_ref: &str) -> Result<Option<Oid>> {
    let mut rd = pktline::Reader::new(body);
    loop {
        match rd.read()? {
            Pkt::Data(n) => {
                let line = rd.line(n);
                let line = line.split(|&b| b == 0).next().unwrap_or(line);
                if let Some((hex, name)) = line.split_at_checked(40) {
                    if name.strip_prefix(b" ") == Some(branch_ref.as_bytes()) {
                        return Oid::from_hex(hex).ctx("bad ref line").map(Some);
                    }
                }
            }
            Pkt::Eof => return Ok(None),
            _ => {}
        }
    }
}

fn advertisement_is_v2(body: &[u8]) -> Result<bool> {
    let mut rd = pktline::Reader::new(body);
    loop {
        match rd.read()? {
            Pkt::Data(n) => {
                let line = rd.line(n);
                if line.starts_with(b"# service=") {
                    continue;
                }
                return Ok(line == b"version 2");
            }
            Pkt::Flush => continue,
            _ => return Ok(false),
        }
    }
}

fn check_err(line: &[u8]) -> Result<()> {
    if let Some(msg) = line.strip_prefix(b"ERR ") {
        crate::bail!("upstream error: {}", String::from_utf8_lossy(msg));
    }
    Ok(())
}

/// A fetch response's parser, fed the response in pieces of any size: pkt-lines up to the
/// pack, then the side-band stream, whose channel-1 bytes are the pack, written to `out`.
/// It ends at the first flush packet.
struct PackStream {
    v2: bool,
    in_pack: bool,
    done: bool,
    written: u64,
    /// The start of a packet that the next piece continues.
    pending: Vec<u8>,
}

impl PackStream {
    fn new(v2: bool) -> PackStream {
        PackStream {
            v2,
            in_pack: !v2,
            done: false,
            written: 0,
            pending: Vec::new(),
        }
    }

    /// The length of the packet whose 4-byte header is `head`, header included.
    fn packet_len(head: &[u8]) -> Result<usize> {
        let mut len_bytes = [0u8; 2];
        if head.len() != 4 || !crate::util::decode_hex(head, &mut len_bytes) {
            crate::bail!("bad pkt-line length {:?}", String::from_utf8_lossy(head));
        }
        match usize::from(u16::from_be_bytes(len_bytes)) {
            0..=2 => Ok(4),
            3 => crate::bail!("bad pkt-line length 3"),
            n if n.saturating_sub(4) > pktline::MAX_PKT => crate::bail!("pkt-line too long"),
            n => Ok(n),
        }
    }

    /// Parses the packets `piece` completes; keeps a packet it only begins.
    fn feed(&mut self, piece: &[u8], out: &mut dyn Write) -> Result<()> {
        let mut rest = piece;
        if !self.pending.is_empty() {
            if self.pending.len() < 4 {
                let take = 4usize.saturating_sub(self.pending.len()).min(rest.len());
                self.pending
                    .extend_from_slice(rest.get(..take).unwrap_or(&[]));
                rest = rest.get(take..).unwrap_or(&[]);
                if self.pending.len() < 4 {
                    return Ok(());
                }
            }
            let total = Self::packet_len(self.pending.get(..4).unwrap_or(&[]))?;
            let take = total.saturating_sub(self.pending.len()).min(rest.len());
            self.pending
                .extend_from_slice(rest.get(..take).unwrap_or(&[]));
            rest = rest.get(take..).unwrap_or(&[]);
            if self.pending.len() < total {
                return Ok(());
            }
            let pkt = std::mem::take(&mut self.pending);
            self.packet(&pkt, out)?;
        }
        while !self.done {
            let Some(head) = rest.get(..4) else { break };
            let total = Self::packet_len(head)?;
            let Some((pkt, tail)) = rest.split_at_checked(total) else {
                break;
            };
            self.packet(pkt, out)?;
            rest = tail;
        }
        if !self.done {
            self.pending.extend_from_slice(rest);
        }
        Ok(())
    }

    /// One whole packet, header included.
    fn packet(&mut self, pkt: &[u8], out: &mut dyn Write) -> Result<()> {
        match pkt {
            // Flush ends the response; delim and response-end packets carry nothing.
            b"0000" => {
                self.done = true;
                return Ok(());
            }
            b"0001" | b"0002" => return Ok(()),
            _ => {}
        }
        let data = pkt.get(4..).unwrap_or(&[]);
        if !self.in_pack {
            // v2 sections before the pack (shallow-info, wanted-refs, ...).
            let line = data.strip_suffix(b"\n").unwrap_or(data);
            check_err(line)?;
            if line == b"packfile" {
                self.in_pack = true;
            }
            return Ok(());
        }
        if !self.v2 && (data.starts_with(b"NAK") || data.starts_with(b"ACK ")) {
            return Ok(());
        }
        check_err(data.strip_suffix(b"\n").unwrap_or(data))?;
        match data.split_first() {
            Some((1, pack)) => {
                out.write_all(pack).ctx("writing pack")?;
                self.written = self.written.saturating_add(crate::util::u64_of(pack.len()));
            }
            Some((2, _)) => {}
            Some((3, msg)) => {
                crate::bail!("upstream error: {}", String::from_utf8_lossy(msg).trim())
            }
            _ => crate::bail!("unexpected packet in the pack stream"),
        }
        Ok(())
    }

    /// The pack's length, once the response has ended.
    fn finish(self) -> Result<u64> {
        if !self.done && !self.pending.is_empty() {
            crate::bail!("truncated pkt-line");
        }
        if self.written == 0 {
            crate::bail!("upstream sent no pack");
        }
        Ok(self.written)
    }
}

trait OptionCtx<T> {
    fn with_ctx_str(self, f: impl FnOnce() -> String) -> Result<T>;
}

impl<T> OptionCtx<T> for Option<T> {
    fn with_ctx_str(self, f: impl FnOnce() -> String) -> Result<T> {
        self.ok_or_else(|| crate::util::Error::new(f()))
    }
}

/// Where a fetch puts the pack, a piece at a time (awaited: a sink may hand its pieces on
/// to other threads, with backpressure).
pub trait PackPieces {
    fn put(&mut self, piece: bytes::Bytes) -> impl std::future::Future<Output = Result<()>>;
}

/// The upstream's requests, on the async client (smart HTTP, GVFS) or an async
/// `git upload-pack` (a local repository), so that a fetch parks its task, not a thread.
/// Use one from a single runtime (its connections run there).
pub struct AsyncUpstream {
    transport: AsyncTransport,
    v2: bool,
    detected: bool,
    caps: Vec<String>,
    gvfs: Option<bool>,
    pub label: String,
}

enum AsyncTransport {
    Local { git_dir: PathBuf },
    Http { client: crate::aclient::AClient },
}

/// A response being read: an HTTP body, or `git upload-pack`'s output.
enum AsyncBody {
    Http(crate::aclient::AResponse),
    Local {
        child: tokio::process::Child,
        stdout: tokio::process::ChildStdout,
        buf: Vec<u8>,
    },
}

impl AsyncBody {
    /// The next piece of the response; `None` at its end.
    async fn chunk(&mut self) -> Result<Option<bytes::Bytes>> {
        match self {
            AsyncBody::Http(r) => r.chunk().await,
            AsyncBody::Local { stdout, buf, .. } => {
                use tokio::io::AsyncReadExt;
                let n = stdout.read(buf).await.ctx("reading from upload-pack")?;
                Ok((n > 0).then(|| bytes::Bytes::copy_from_slice(buf.get(..n).unwrap_or(&[]))))
            }
        }
    }

    /// The whole response (at most `limit` bytes).
    async fn read_all(&mut self, limit: usize) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        while let Some(c) = self.chunk().await? {
            if out.len().saturating_add(c.len()) > limit {
                crate::bail!("upstream response larger than {limit} bytes");
            }
            out.extend_from_slice(&c);
        }
        Ok(out)
    }

    /// Ends the exchange after `result` (its parse): the rest of an HTTP body is read; a
    /// local `upload-pack` must exit successfully (its stderr explains a failure).
    async fn finish<T>(self, result: Result<T>) -> Result<T> {
        match self {
            AsyncBody::Http(mut r) => {
                let v = result?;
                while r.chunk().await?.is_some() {}
                Ok(v)
            }
            AsyncBody::Local {
                mut child, stdout, ..
            } => {
                use tokio::io::AsyncReadExt;
                drop(stdout);
                let mut stderr = Vec::new();
                if let Some(mut e) = child.stderr.take() {
                    let _ = e.read_to_end(&mut stderr).await;
                }
                let status = child.wait().await.ctx("waiting for upload-pack")?;
                let stderr = String::from_utf8_lossy(&stderr);
                match result {
                    Err(e) => Err(crate::err!("{e} ({})", stderr.trim())),
                    Ok(v) if status.success() => Ok(v),
                    Ok(_) => crate::bail!("git upload-pack failed: {}", stderr.trim()),
                }
            }
        }
    }
}

impl AsyncUpstream {
    /// `spec` is an http(s) URL or a local repository path.
    pub fn open(spec: &str, auth: &Auth) -> Result<AsyncUpstream> {
        refuse_password(spec)?;
        let transport = if spec.starts_with("http://") || spec.starts_with("https://") {
            let mut client =
                crate::aclient::AClient::new(Url::parse(spec)?, Duration::from_secs(300));
            if let Some(h) = &auth.header {
                client
                    .default_headers
                    .push(("Authorization".into(), h.clone()));
            }
            AsyncTransport::Http { client }
        } else {
            let p = PathBuf::from(spec.strip_prefix("file://").unwrap_or(spec));
            if !p.exists() {
                crate::bail!(
                    "upstream {}: no such repository",
                    crate::http::redacted(spec)
                );
            }
            AsyncTransport::Local { git_dir: p }
        };
        let is_local = matches!(transport, AsyncTransport::Local { .. });
        Ok(AsyncUpstream {
            transport,
            v2: true,
            detected: is_local,
            caps: Vec::new(),
            gvfs: if is_local { Some(false) } else { None },
            label: crate::http::redacted(spec),
        })
    }

    /// Starts one stateless request.
    async fn rpc(&mut self, request: &[u8], v2: bool) -> Result<AsyncBody> {
        match &mut self.transport {
            AsyncTransport::Local { git_dir } => {
                use tokio::io::AsyncWriteExt;
                let mut cmd = Command::new("git");
                // Our own repository: allow what separate-blob stores ask for.
                cmd.args([
                    "-c",
                    "uploadpack.allowFilter=true",
                    "-c",
                    "uploadpack.allowAnySHA1InWant=true",
                    "-c",
                    "transfer.advertiseObjectInfo=true",
                ]);
                cmd.arg("upload-pack").arg("--stateless-rpc").arg(&*git_dir);
                hermetic_git_env(&mut cmd);
                if v2 {
                    cmd.env("GIT_PROTOCOL", "version=2");
                }
                cmd.stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped());
                let mut child = tokio::process::Command::from(cmd)
                    .kill_on_drop(true)
                    .spawn()
                    .ctx("running git upload-pack")?;
                {
                    let mut stdin = child.stdin.take().ctx("upload-pack stdin")?;
                    stdin
                        .write_all(request)
                        .await
                        .ctx("writing to upload-pack")?;
                }
                let stdout = child.stdout.take().ctx("upload-pack stdout")?;
                Ok(AsyncBody::Local {
                    child,
                    stdout,
                    buf: vec![0u8; 64 << 10],
                })
            }
            AsyncTransport::Http { client } => {
                let mut headers = vec![
                    ("Content-Type", "application/x-git-upload-pack-request"),
                    ("Accept", "application/x-git-upload-pack-result"),
                ];
                if v2 {
                    headers.push(("Git-Protocol", "version=2"));
                }
                let body = bytes::Bytes::copy_from_slice(request);
                let resp = client
                    .request("POST", "/git-upload-pack", &headers, body)
                    .await?
                    .ensure_ok("git-upload-pack")
                    .await?;
                Ok(AsyncBody::Http(resp))
            }
        }
    }

    /// Detects protocol v2 from the HTTP capability advertisement (once).
    async fn detect(&mut self) -> Result<()> {
        if self.detected {
            return Ok(());
        }
        if let AsyncTransport::Http { client } = &self.transport {
            let mut resp = client
                .request(
                    "GET",
                    "/info/refs?service=git-upload-pack",
                    &[("Git-Protocol", "version=2")],
                    bytes::Bytes::new(),
                )
                .await?
                .ensure_ok("info/refs")
                .await?;
            let body = resp.body_vec(64 << 20).await?;
            self.v2 = advertisement_is_v2(&body)?;
            if self.v2 {
                self.caps = advertised_caps(&body)?;
            }
        }
        self.detected = true;
        Ok(())
    }

    fn has_cap(&self, name: &str) -> bool {
        has_cap(
            matches!(self.transport, AsyncTransport::Local { .. }),
            &self.caps,
            name,
        )
    }

    /// Whether this is an HTTP upstream answering the GVFS protocol (detected once).
    async fn is_gvfs(&mut self) -> bool {
        if let Some(g) = self.gvfs {
            return g;
        }
        let g = match &self.transport {
            AsyncTransport::Http { client } => {
                match client
                    .request("GET", "/gvfs/config", &[], bytes::Bytes::new())
                    .await
                {
                    Ok(mut r) => {
                        let ok = r.status == 200;
                        let _ = r.body_vec(1 << 20).await;
                        ok
                    }
                    Err(_) => false,
                }
            }
            AsyncTransport::Local { .. } => false,
        };
        self.gvfs = Some(g);
        g
    }

    /// The current tip of branch `branch_ref` (`refs/heads/<branch>`).
    pub async fn head(&mut self, branch_ref: &str) -> Result<Oid> {
        self.detect().await?;
        let found = if self.v2 {
            let mut req = Vec::new();
            pktline::put_str(&mut req, "command=ls-refs\n");
            pktline::put_str(&mut req, AGENT);
            pktline::delim(&mut req);
            pktline::put_str(&mut req, &format!("ref-prefix {branch_ref}\n"));
            pktline::flush(&mut req);
            let mut body = self.rpc(&req, true).await?;
            let read = body.read_all(64 << 20).await;
            let found = read.and_then(|b| ls_refs_tip(&b, branch_ref));
            body.finish(found).await?
        } else {
            let AsyncTransport::Http { client } = &self.transport else {
                crate::bail!("v0 is only used over HTTP");
            };
            let mut resp = client
                .request(
                    "GET",
                    "/info/refs?service=git-upload-pack",
                    &[],
                    bytes::Bytes::new(),
                )
                .await?
                .ensure_ok("info/refs")
                .await?;
            v0_refs_tip(&resp.body_vec(256 << 20).await?, branch_ref)?
        };
        found.with_ctx_str(|| format!("{}: no {branch_ref}", self.label))
    }

    /// The objects reachable from `want` but not from any of `haves` (none = everything),
    /// filtered by `filter` if given (`blob:none`: commits and trees only), as one non-thin
    /// pack handed to `out` in pieces. Returns the pack length. Haves the upstream does not
    /// know (history rewritten) are ignored by it.
    pub async fn fetch<P: PackPieces>(
        &mut self,
        haves: &[Oid],
        want: &Oid,
        filter: Option<&str>,
        out: &mut P,
    ) -> Result<u64> {
        self.detect().await?;
        if filter.is_some() && (!self.v2 || !self.has_cap("fetch:filter")) {
            crate::bail!(
                "{}: the upstream does not support fetch filters",
                self.label
            );
        }
        self.fetch_with(haves, std::slice::from_ref(want), filter, out)
            .await
    }

    /// Exactly the blobs `oids` as one pack, handed to `out` in pieces: through
    /// `POST /gvfs/objects` from a GVFS upstream, else as protocol v2 `want`s. Returns the
    /// pack length.
    pub async fn fetch_objects<P: PackPieces>(&mut self, oids: &[Oid], out: &mut P) -> Result<u64> {
        if self.is_gvfs().await {
            let body = gvfs_objects_body(oids);
            let AsyncTransport::Http { client } = &self.transport else {
                crate::bail!("GVFS upstreams are HTTP");
            };
            let mut resp = client
                .request(
                    "POST",
                    "/gvfs/objects",
                    &[
                        ("Content-Type", "application/json"),
                        ("Accept", "application/x-git-packfile"),
                    ],
                    bytes::Bytes::from(body),
                )
                .await?
                .ensure_ok("POST /gvfs/objects")
                .await?;
            let mut n = 0u64;
            while let Some(c) = resp.chunk().await.ctx("reading /gvfs/objects")? {
                n = n.saturating_add(crate::util::u64_of(c.len()));
                out.put(c).await?;
            }
            return Ok(n);
        }
        self.detect().await?;
        if !self.v2 {
            crate::bail!("{}: fetching blobs by id needs protocol v2", self.label);
        }
        self.fetch_with(&[], oids, None, out).await
    }

    /// One fetch request (protocol v2, or v0 where the upstream speaks only that), its pack
    /// handed to `out` in pieces.
    async fn fetch_with<P: PackPieces>(
        &mut self,
        haves: &[Oid],
        wants: &[Oid],
        filter: Option<&str>,
        out: &mut P,
    ) -> Result<u64> {
        let v2 = self.v2;
        let req = fetch_request(v2, haves, wants, filter)?;
        let mut body = self.rpc(&req, v2).await?;
        let mut parse = PackStream::new(v2);
        let fed: Result<()> = async {
            while !parse.done {
                let Some(c) = body.chunk().await? else { break };
                let mut pack = Vec::new();
                parse.feed(&c, &mut pack)?;
                if !pack.is_empty() {
                    out.put(bytes::Bytes::from(pack)).await?;
                }
            }
            Ok(())
        }
        .await;
        let result = fed.and_then(|()| parse.finish());
        body.finish(result).await
    }

    /// The sizes of objects `oids`, in order (`None`: the upstream does not have it):
    /// `POST /gvfs/sizes` from a GVFS upstream, else protocol v2 `object-info`.
    pub async fn object_sizes(&mut self, oids: &[Oid]) -> Result<Vec<Option<u64>>> {
        let mut out = Vec::with_capacity(oids.len());
        for batch in oids.chunks(BATCH) {
            if self.is_gvfs().await {
                let AsyncTransport::Http { client } = &self.transport else {
                    crate::bail!("GVFS upstreams are HTTP");
                };
                let mut resp = client
                    .request(
                        "POST",
                        "/gvfs/sizes",
                        &[("Content-Type", "application/json")],
                        bytes::Bytes::from(gvfs_sizes_body(batch)),
                    )
                    .await?
                    .ensure_ok("POST /gvfs/sizes")
                    .await?;
                let limit = batch.len().saturating_mul(128).saturating_add(1 << 16);
                let text = resp.body_vec(limit).await?;
                let found = parse_gvfs_sizes(&text);
                out.extend(batch.iter().map(|o| found.get(o).copied()));
                continue;
            }
            self.detect().await?;
            if !self.v2 || !self.has_cap("object-info") {
                crate::bail!(
                    "{}: the upstream offers neither /gvfs/sizes nor object-info",
                    self.label
                );
            }
            let mut body = self.rpc(&object_info_request(batch), true).await?;
            let read = body.read_all(64 << 20).await;
            let found = read.and_then(|b| parse_object_info(&mut &b[..], batch.len()));
            let found = body.finish(found).await?;
            out.extend(batch.iter().map(|o| found.get(o).copied()));
        }
        Ok(out)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
mod tests {
    use super::*;

    /// A password in the upstream URL is refused (and not repeated); a user name alone is
    /// fine, and labels (which errors and logs print) hold no userinfo.
    #[test]
    fn upstream_urls_with_passwords_are_refused() {
        let auth = Auth::default();
        for spec in [
            "https://me:s3cret@dev.azure.com/o/p/_git/r",
            "http://u:s3cret@127.0.0.1:1/r",
        ] {
            let e = Upstream::open(spec, "main", &auth)
                .err()
                .unwrap()
                .to_string();
            assert!(e.contains("--token-env") && !e.contains("s3cret"), "{e}");
            let e = AsyncUpstream::open(spec, &auth).err().unwrap().to_string();
            assert!(!e.contains("s3cret"), "{e}");
        }
        let u = Upstream::open("https://org@dev.azure.com/org/p/_git/r", "main", &auth).unwrap();
        assert_eq!(u.label(), "https://dev.azure.com/org/p/_git/r");
    }

    fn git(dir: &std::path::Path, args: &[&str], stdin: &[u8]) -> Vec<u8> {
        let mut cmd = Command::new("git");
        cmd.args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t");
        hermetic_git_env(&mut cmd);
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(stdin).unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        out.stdout
    }

    fn pack_objects(dir: &std::path::Path, pack: &[u8], strict: bool) -> Vec<(String, String)> {
        let repo = dir.join(format!("check-{}.git", pack.len()));
        let _ = std::fs::remove_dir_all(&repo);
        git(dir, &["init", "-q", "--bare", repo.to_str().unwrap()], b"");
        // `--strict` also checks connectivity, which a `blob:none` pack does not have.
        let args: &[&str] = if strict {
            &["index-pack", "--stdin", "--strict"]
        } else {
            &["index-pack", "--stdin"]
        };
        git(&repo, args, pack);
        let out = git(
            &repo,
            &[
                "cat-file",
                "--batch-all-objects",
                "--batch-check=%(objectname) %(objecttype)",
            ],
            b"",
        );
        String::from_utf8(out)
            .unwrap()
            .lines()
            .map(|l| {
                let (a, b) = l.split_once(' ').unwrap();
                (a.to_string(), b.to_string())
            })
            .collect()
    }

    /// The three requests separate-blob stores make of a local upstream.
    #[test]
    fn filtered_fetch_blob_wants_and_sizes() {
        let dir = std::env::temp_dir().join(format!("gvfs-upstream-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let work = dir.join("w");
        git(
            &dir,
            &["init", "-q", "-b", "main", work.to_str().unwrap()],
            b"",
        );
        std::fs::create_dir_all(work.join("d")).unwrap();
        std::fs::write(work.join("a.txt"), "alpha\n").unwrap();
        std::fs::write(work.join("d/b.bin"), vec![7u8; 100_000]).unwrap();
        git(&work, &["add", "."], b"");
        git(&work, &["commit", "-q", "-m", "one"], b"");
        let head = Oid::from_hex(git(&work, &["rev-parse", "HEAD"], b"").trim_ascii()).unwrap();
        let blobs: Vec<Oid> = String::from_utf8(git(&work, &["ls-tree", "-r", "HEAD"], b""))
            .unwrap()
            .lines()
            .map(|l| Oid::from_hex(l.split_whitespace().nth(2).unwrap().as_bytes()).unwrap())
            .collect();
        let mut up = Upstream::open(
            work.join(".git").to_str().unwrap(),
            "main",
            &Auth::default(),
        )
        .unwrap();

        let mut meta = Vec::new();
        up.fetch_filtered(&[], &head, "blob:none", &mut meta)
            .unwrap();
        let kinds: Vec<String> = pack_objects(&dir, &meta, false)
            .into_iter()
            .map(|o| o.1)
            .collect();
        assert!(
            kinds.iter().all(|k| k == "commit" || k == "tree"),
            "{kinds:?}"
        );
        assert_eq!(kinds.len(), 3);

        let mut only = Vec::new();
        up.fetch_objects(&blobs, &mut only).unwrap();
        let mut got: Vec<String> = pack_objects(&dir, &only, true)
            .into_iter()
            .map(|o| o.0)
            .collect();
        let mut want: Vec<String> = blobs.iter().map(|o| o.to_string()).collect();
        got.sort();
        want.sort();
        assert_eq!(got, want);

        let mut ask = blobs.clone();
        ask.push(Oid([0x42; 20]));
        let sizes = up.object_sizes(&ask).unwrap();
        let expect: Vec<Option<u64>> = blobs
            .iter()
            .map(|o| {
                let s = git(&work, &["cat-file", "-s", &o.to_string()], b"");
                Some(std::str::from_utf8(&s).unwrap().trim().parse().unwrap())
            })
            .chain(std::iter::once(None))
            .collect();
        assert_eq!(sizes, expect);
        assert_eq!(
            parse_gvfs_sizes(br#"[{"Id":"4242424242424242424242424242424242424242","Size":7}, { "Size" : 9 , "Id" : "4343434343434343434343434343434343434343" }]"#)
                .len(),
            2
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
