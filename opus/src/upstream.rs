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
    /// Human-readable description for logs.
    pub label: String,
}

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
        }
        self.detected = true;
        Ok(())
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
        self.detect()?;
        let mut req = Vec::new();
        if self.v2 {
            pktline::put_str(&mut req, "command=fetch\n");
            pktline::put_str(&mut req, AGENT);
            pktline::delim(&mut req);
            pktline::put_str(&mut req, "ofs-delta\n");
            pktline::put_str(&mut req, "no-progress\n");
            pktline::put_str(&mut req, &format!("want {want}\n"));
            for have in haves.iter().filter(|h| !h.is_zero()) {
                pktline::put_str(&mut req, &format!("have {have}\n"));
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
