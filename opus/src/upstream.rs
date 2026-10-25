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

use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use crate::http::{Client, Url};
use crate::pktline::{self, Pkt};
use crate::util::{Context, Oid, Result};

const AGENT: &str = concat!("agent=gvfs-daemon/", env!("CARGO_PKG_VERSION"), "\n");

enum Transport {
    Local { git_dir: PathBuf },
    Http { client: Client },
}

pub struct Upstream {
    transport: Transport,
    /// `refs/heads/<branch>`.
    pub branch_ref: String,
    /// Protocol v2 (detected for HTTP; always for local repositories).
    v2: bool,
    detected: bool,
    /// Protocol v2 capabilities advertised over HTTP (`fetch=... filter`, `object-info`).
    caps: Vec<String>,
    /// The HTTP upstream answers the GVFS protocol (`GET /gvfs/config`): detected once.
    gvfs: Option<bool>,
    /// Human-readable description for logs.
    pub label: String,
}

/// Ids per `object-info` / `/gvfs/sizes` request, and per blob batch.
pub const BATCH: usize = 4000;

/// Credentials for an HTTP upstream.
#[derive(Clone, Default)]
pub struct Auth {
    /// Full `Authorization` header value.
    pub header: Option<String>,
}

impl Upstream {
    /// `spec` is an http(s) URL or a local repository path.
    pub fn open(spec: &str, branch: &str, auth: &Auth) -> Result<Upstream> {
        let branch_ref = if branch.starts_with("refs/") {
            branch.to_string()
        } else {
            format!("refs/heads/{branch}")
        };
        let transport = if spec.starts_with("http://") || spec.starts_with("https://") {
            let mut client = Client::new(Url::parse(spec)?, Duration::from_secs(300));
            if let Some(h) = &auth.header {
                client
                    .default_headers
                    .push(("Authorization".into(), h.clone()));
            }
            Transport::Http { client }
        } else {
            let p = PathBuf::from(spec.strip_prefix("file://").unwrap_or(spec));
            if !p.exists() {
                crate::bail!("upstream {spec}: no such repository");
            }
            Transport::Local { git_dir: p }
        };
        let is_local = matches!(transport, Transport::Local { .. });
        Ok(Upstream {
            transport,
            branch_ref,
            v2: true,
            detected: is_local,
            caps: Vec::new(),
            gvfs: if is_local { Some(false) } else { None },
            label: spec.to_string(),
        })
    }

    /// Runs one stateless request and hands the response stream to `on_response`.
    fn rpc<T>(
        &mut self,
        request: &[u8],
        v2: bool,
        on_response: impl FnOnce(&mut dyn Read) -> Result<T>,
    ) -> Result<T> {
        match &mut self.transport {
            Transport::Local { git_dir } => {
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
                let mut child = cmd
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .ctx("running git upload-pack")?;
                {
                    let mut stdin = child.stdin.take().ctx("upload-pack stdin")?;
                    stdin.write_all(request).ctx("writing to upload-pack")?;
                }
                let mut stdout = child.stdout.take().ctx("upload-pack stdout")?;
                let result = on_response(&mut stdout);
                drop(stdout);
                let out = child.wait_with_output().ctx("waiting for upload-pack")?;
                match result {
                    Err(e) => {
                        let stderr = String::from_utf8_lossy(&out.stderr);
                        Err(crate::err!("{e} ({})", stderr.trim()))
                    }
                    Ok(v) if out.status.success() => Ok(v),
                    Ok(_) => crate::bail!(
                        "git upload-pack failed: {}",
                        String::from_utf8_lossy(&out.stderr).trim()
                    ),
                }
            }
            Transport::Http { client } => {
                let mut headers = vec![
                    ("Content-Type", "application/x-git-upload-pack-request"),
                    ("Accept", "application/x-git-upload-pack-result"),
                ];
                if v2 {
                    headers.push(("Git-Protocol", "version=2"));
                }
                let mut resp = client
                    .request("POST", "/git-upload-pack", &headers, request)?
                    .ensure_ok("git-upload-pack")?;
                let v = on_response(&mut resp)?;
                resp.finish()?;
                Ok(v)
            }
        }
    }

    /// Detects protocol v2 from the HTTP capability advertisement (once).
    fn detect(&mut self) -> Result<()> {
        if self.detected {
            return Ok(());
        }
        if let Transport::Http { client } = &mut self.transport {
            let mut resp = client
                .request(
                    "GET",
                    "/info/refs?service=git-upload-pack",
                    &[("Git-Protocol", "version=2")],
                    b"",
                )?
                .ensure_ok("info/refs")?;
            let body = resp.body_vec(64 << 20)?;
            self.v2 = advertisement_is_v2(&body)?;
            if self.v2 {
                self.caps = advertised_caps(&body)?;
            }
        }
        self.detected = true;
        Ok(())
    }

    /// Whether the upstream offers protocol v2 capability `name` (`fetch` features are
    /// matched as `fetch:<feature>`). Local repositories offer everything we ask for.
    fn has_cap(&self, name: &str) -> bool {
        if matches!(self.transport, Transport::Local { .. }) {
            return true;
        }
        self.caps.iter().any(|c| {
            if let Some(feature) = name.strip_prefix("fetch:") {
                c.strip_prefix("fetch=")
                    .is_some_and(|v| v.split(' ').any(|f| f == feature))
            } else {
                c == name || c.strip_prefix(name).is_some_and(|r| r.starts_with('='))
            }
        })
    }

    /// Whether this is an HTTP upstream answering the GVFS protocol (detected once).
    pub fn is_gvfs(&mut self) -> bool {
        if let Some(g) = self.gvfs {
            return g;
        }
        let g = match &mut self.transport {
            Transport::Http { client } => client
                .request("GET", "/gvfs/config", &[], b"")
                .ok()
                .and_then(|mut r| {
                    let ok = r.status == 200;
                    let _ = r.body_vec(1 << 20);
                    ok.then_some(true)
                })
                .unwrap_or(false),
            Transport::Local { .. } => false,
        };
        self.gvfs = Some(g);
        g
    }

    /// The current tip of the followed branch.
    pub fn head(&mut self) -> Result<Oid> {
        self.detect()?;
        if !self.v2 {
            return self.head_v0();
        }
        let mut req = Vec::new();
        pktline::put_str(&mut req, "command=ls-refs\n");
        pktline::put_str(&mut req, AGENT);
        pktline::delim(&mut req);
        pktline::put_str(&mut req, &format!("ref-prefix {}\n", self.branch_ref));
        pktline::flush(&mut req);
        let branch = self.branch_ref.clone();
        let found = self.rpc(&req, true, |r| {
            let mut rd = pktline::Reader::new(r);
            let mut found = None;
            loop {
                match rd.read()? {
                    Pkt::Data(n) => {
                        let line = rd.line(n);
                        check_err(line)?;
                        let (hex, rest) = line.split_at_checked(40).ctx("bad ls-refs line")?;
                        let name = rest.strip_prefix(b" ").unwrap_or(rest);
                        let name = name.split(|&b| b == b' ').next().unwrap_or(name);
                        if name == branch.as_bytes() {
                            found = Oid::from_hex(hex);
                        }
                    }
                    Pkt::Flush | Pkt::Eof => return Ok(found),
                    _ => {}
                }
            }
        })?;
        found.with_ctx_str(|| format!("{}: no {}", self.label, self.branch_ref))
    }

    fn head_v0(&mut self) -> Result<Oid> {
        let Transport::Http { client } = &mut self.transport else {
            crate::bail!("v0 is only used over HTTP");
        };
        let mut resp = client
            .request("GET", "/info/refs?service=git-upload-pack", &[], b"")?
            .ensure_ok("info/refs")?;
        let body = resp.body_vec(256 << 20)?;
        drop(resp);
        let mut rd = pktline::Reader::new(&body[..]);
        loop {
            match rd.read()? {
                Pkt::Data(n) => {
                    let line = rd.line(n);
                    let line = line.split(|&b| b == 0).next().unwrap_or(line);
                    if let Some((hex, name)) = line.split_at_checked(40) {
                        if name.strip_prefix(b" ") == Some(self.branch_ref.as_bytes()) {
                            return Oid::from_hex(hex).ctx("bad ref line");
                        }
                    }
                }
                Pkt::Eof => crate::bail!("{}: no {}", self.label, self.branch_ref),
                _ => {}
            }
        }
    }

    /// Fetches the objects reachable from `want` but not from any of `haves` (none =
    /// everything) as one non-thin pack, written to `out`. Returns the pack length. Haves
    /// the upstream does not know (history rewritten) are ignored by it.
    pub fn fetch(&mut self, haves: &[Oid], want: &Oid, out: &mut dyn Write) -> Result<u64> {
        self.fetch_with(haves, std::slice::from_ref(want), None, out)
    }

    /// Like [`Upstream::fetch`] with a filter (`blob:none`: commits and trees only).
    pub fn fetch_filtered(
        &mut self,
        haves: &[Oid],
        want: &Oid,
        filter: &str,
        out: &mut dyn Write,
    ) -> Result<u64> {
        self.detect()?;
        if !self.v2 || !self.has_cap("fetch:filter") {
            crate::bail!(
                "{}: the upstream does not support fetch filters",
                self.label
            );
        }
        self.fetch_with(haves, std::slice::from_ref(want), Some(filter), out)
    }

    /// Fetches exactly the objects `oids` (blobs) as one pack written to `out`: through
    /// `POST /gvfs/objects` from a GVFS upstream, else as protocol v2 `want`s. Returns the
    /// pack length.
    pub fn fetch_objects(&mut self, oids: &[Oid], out: &mut dyn Write) -> Result<u64> {
        if self.is_gvfs() {
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
            let Transport::Http { client } = &mut self.transport else {
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
                    body.as_bytes(),
                )?
                .ensure_ok("POST /gvfs/objects")?;
            let n = std::io::copy(&mut resp, out).ctx("reading /gvfs/objects")?;
            resp.finish()?;
            return Ok(n);
        }
        self.detect()?;
        if !self.v2 {
            crate::bail!("{}: fetching blobs by id needs protocol v2", self.label);
        }
        self.fetch_with(&[], oids, None, out)
    }

    /// The sizes of objects `oids`, in order (`None`: the upstream does not have it):
    /// `POST /gvfs/sizes` from a GVFS upstream, else protocol v2 `object-info`.
    pub fn object_sizes(&mut self, oids: &[Oid]) -> Result<Vec<Option<u64>>> {
        let mut out = Vec::with_capacity(oids.len());
        for batch in oids.chunks(BATCH) {
            if self.is_gvfs() {
                self.gvfs_sizes(batch, &mut out)?;
            } else {
                self.object_info(batch, &mut out)?;
            }
        }
        Ok(out)
    }

    fn gvfs_sizes(&mut self, oids: &[Oid], out: &mut Vec<Option<u64>>) -> Result<()> {
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
        let Transport::Http { client } = &mut self.transport else {
            crate::bail!("GVFS upstreams are HTTP");
        };
        let mut resp = client
            .request(
                "POST",
                "/gvfs/sizes",
                &[("Content-Type", "application/json")],
                body.as_bytes(),
            )?
            .ensure_ok("POST /gvfs/sizes")?;
        let text = resp.body_vec(oids.len().saturating_mul(128).saturating_add(1 << 16))?;
        let found = parse_gvfs_sizes(&text);
        out.extend(oids.iter().map(|o| found.get(o).copied()));
        Ok(())
    }

    fn object_info(&mut self, oids: &[Oid], out: &mut Vec<Option<u64>>) -> Result<()> {
        self.detect()?;
        if !self.v2 || !self.has_cap("object-info") {
            crate::bail!(
                "{}: the upstream offers neither /gvfs/sizes nor object-info",
                self.label
            );
        }
        let mut req = Vec::new();
        pktline::put_str(&mut req, "command=object-info\n");
        pktline::put_str(&mut req, AGENT);
        pktline::delim(&mut req);
        pktline::put_str(&mut req, "size\n");
        for o in oids {
            pktline::put_str(&mut req, &format!("oid {o}\n"));
        }
        pktline::flush(&mut req);
        let found = self.rpc(&req, true, |r| {
            let mut rd = pktline::Reader::new(r);
            let mut found = std::collections::HashMap::with_capacity(oids.len());
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
        })?;
        out.extend(oids.iter().map(|o| found.get(o).copied()));
        Ok(())
    }

    fn fetch_with(
        &mut self,
        haves: &[Oid],
        wants: &[Oid],
        filter: Option<&str>,
        out: &mut dyn Write,
    ) -> Result<u64> {
        self.detect()?;
        let want = wants.first().copied().ctx("fetch without a want")?;
        let mut req = Vec::new();
        if self.v2 {
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
        let v2 = self.v2;
        self.rpc(&req, v2, |r| read_pack_response(r, v2, out))
    }
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

/// Reads a fetch response up to and through its side-band pack stream.
fn read_pack_response(r: &mut dyn Read, v2: bool, out: &mut dyn Write) -> Result<u64> {
    let mut rd = pktline::Reader::new(r);
    let mut in_pack = !v2;
    let mut written = 0u64;
    loop {
        match rd.read()? {
            Pkt::Data(n) => {
                let data = rd.data(n);
                if !in_pack {
                    // v2 sections before the pack (shallow-info, wanted-refs, ...).
                    let line = rd.line(n);
                    check_err(line)?;
                    if line == b"packfile" {
                        in_pack = true;
                    }
                    continue;
                }
                if !v2 && (data.starts_with(b"NAK") || data.starts_with(b"ACK ")) {
                    continue;
                }
                check_err(data.strip_suffix(b"\n").unwrap_or(data))?;
                match data.split_first() {
                    Some((1, pack)) => {
                        out.write_all(pack).ctx("writing pack")?;
                        written = written.saturating_add(crate::util::u64_of(pack.len()));
                    }
                    Some((2, _)) => {}
                    Some((3, msg)) => {
                        crate::bail!("upstream error: {}", String::from_utf8_lossy(msg).trim())
                    }
                    _ => crate::bail!("unexpected packet in the pack stream"),
                }
            }
            Pkt::Flush | Pkt::Eof => break,
            Pkt::Delim | Pkt::End => {}
        }
    }
    if written == 0 {
        crate::bail!("upstream sent no pack");
    }
    Ok(written)
}

trait OptionCtx<T> {
    fn with_ctx_str(self, f: impl FnOnce() -> String) -> Result<T>;
}

impl<T> OptionCtx<T> for Option<T> {
    fn with_ctx_str(self, f: impl FnOnce() -> String) -> Result<T> {
        self.ok_or_else(|| crate::util::Error::new(f()))
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
