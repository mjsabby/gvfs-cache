//! Native upload-pack framing, shared by ADO, GitHub and local Git repositories.
//! No blob/tree filtering: a real HFS repository may also contain ordinary blobs.

use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};

use crate::arena::{Writer, append, reserve};
use crate::codec::{Oid, take};
use crate::storage::{Endpoint, HttpClient, TempFile, next_data};
use crate::{Error, Result};

pub struct Upstream {
    source: String,
    branch: String,
    local: bool,
    http: Option<HttpClient>,
    authorization: Option<HeaderValue>,
    advertised: bool,
    packet_buffer: Vec<u8>,
}

impl Upstream {
    pub fn new(
        source: String,
        branch: String,
        auth_env: Option<&str>,
        token_env: Option<&str>,
    ) -> Result<Self> {
        if source.starts_with('-')
            || branch.is_empty()
            || branch
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte < 32)
            || branch.starts_with('-')
        {
            return Err(Error::Usage("invalid upstream or branch".into()));
        }
        let local = !source.starts_with("http://") && !source.starts_with("https://");
        let authorization = if let Some(env) = auth_env {
            let value = std::env::var(env).map_err(|_| Error::Usage(format!("missing {env}")))?;
            Some(
                HeaderValue::from_str(&value)
                    .map_err(|_| Error::Invalid("upstream authorization"))?,
            )
        } else if let Some(env) = token_env {
            let token = std::env::var(env).map_err(|_| Error::Usage(format!("missing {env}")))?;
            let ado = source.contains("dev.azure.com") || source.contains("visualstudio.com");
            let user = if ado { "" } else { "x-access-token" };
            let value = format!(
                "Basic {}",
                crate::storage::base64(format!("{user}:{token}").as_bytes())
            );
            Some(HeaderValue::from_str(&value).map_err(|_| Error::Invalid("upstream token"))?)
        } else {
            None
        };
        let http = if local {
            None
        } else {
            Some(HttpClient::new(Endpoint::parse(&source)?)?)
        };
        Ok(Self {
            source,
            branch,
            local,
            http,
            authorization,
            advertised: false,
            packet_buffer: crate::arena::zeroed(65516)?,
        })
    }

    fn headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("git-protocol", HeaderValue::from_static("version=2"));
        headers.insert(
            "content-type",
            HeaderValue::from_static("application/x-git-upload-pack-request"),
        );
        headers.insert(
            "accept",
            HeaderValue::from_static("application/x-git-upload-pack-result"),
        );
        if let Some(auth) = &self.authorization {
            headers.insert("authorization", auth.clone());
        }
        headers
    }

    async fn advertise(&mut self) -> Result<()> {
        if self.advertised {
            return Ok(());
        }
        let headers = self.headers();
        let client = self
            .http
            .as_mut()
            .ok_or(Error::Invalid("Git HTTP client"))?;
        let response = client
            .request(
                Method::GET,
                "info/refs",
                "service=git-upload-pack",
                headers,
                None,
            )
            .await?;
        if response.status() != StatusCode::OK {
            return Err(Error::Http(response.status().as_u16()));
        }
        let mut bytes = reserve(65536)?;
        let mut body = response.into_body();
        while let Some(chunk) = next_data(&mut body).await? {
            append(&mut bytes, &chunk)?;
        }
        if !bytes.windows(10).any(|part| part == b"version 2\n") {
            return Err(Error::Invalid(
                "upstream does not advertise Git protocol v2",
            ));
        }
        self.advertised = true;
        Ok(())
    }

    pub async fn head(&mut self) -> Result<Oid> {
        if self.local {
            let source = self.source.strip_prefix("file://").unwrap_or(&self.source);
            if !Path::new(source).exists() {
                return Err(Error::Usage(
                    "upstream must be an existing Git repository or an HTTP(S) Git URL".into(),
                ));
            }
            let reference = if self.branch == "HEAD" || self.branch.starts_with("refs/") {
                self.branch.clone()
            } else {
                format!("refs/heads/{}", self.branch)
            };
            let mut command = git_command();
            command.args([
                "-C",
                source,
                "rev-parse",
                "--verify",
                &format!("{reference}^{{commit}}"),
            ]);
            // Explicit --git-dir is required for bare repositories in this CLI's environment.
            if !Path::new(source).join(".git").exists() {
                command = git_command();
                command.arg("--git-dir").arg(source).args([
                    "rev-parse",
                    "--verify",
                    &format!("{reference}^{{commit}}"),
                ]);
            }
            let mut child = command
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .stdin(Stdio::null())
                .spawn()?;
            let result = read_bounded(
                child.stdout.take().ok_or(Error::Invalid("Git stdout"))?,
                4096,
            );
            let status = child.wait()?;
            if !status.success() {
                return Err(Error::Transport(format!("git rev-parse failed: {status}")));
            }
            let bytes = result?;
            let line = bytes
                .split(|byte| byte.is_ascii_whitespace())
                .next()
                .ok_or(Error::Invalid("Git tip"))?;
            return Oid::parse(line);
        }
        self.advertise().await?;
        let reference = if self.branch == "HEAD" || self.branch.starts_with("refs/") {
            self.branch.clone()
        } else {
            format!("refs/heads/{}", self.branch)
        };
        let mut request = reserve(4096)?;
        pkt(&mut request, b"command=ls-refs\n")?;
        append(&mut request, b"0001")?;
        pkt(&mut request, b"peel\n")?;
        pkt(&mut request, format!("ref-prefix {reference}\n").as_bytes())?;
        append(&mut request, b"0000")?;
        let headers = self.headers();
        let response = self
            .http
            .as_mut()
            .ok_or(Error::Invalid("Git HTTP client"))?
            .request(
                Method::POST,
                "git-upload-pack",
                "",
                headers,
                Some(Bytes::from(request)),
            )
            .await?;
        if response.status() != StatusCode::OK {
            return Err(Error::Http(response.status().as_u16()));
        }
        let mut bytes = reserve(65536)?;
        let mut body = response.into_body();
        while let Some(chunk) = next_data(&mut body).await? {
            append(&mut bytes, &chunk)?;
        }
        let mut cursor = 0usize;
        while cursor < bytes.len() {
            let len = packet_len(take(&bytes, cursor, 4)?)?;
            cursor = cursor.saturating_add(4);
            if len <= 2 {
                continue;
            }
            if len < 4 {
                return Err(Error::Invalid("Git ls-refs pkt-line"));
            }
            let line = take(&bytes, cursor, len.saturating_sub(4))?;
            cursor = cursor.saturating_add(line.len());
            let mut words = line.split(|byte| byte.is_ascii_whitespace());
            let oid = words.next().ok_or(Error::Invalid("Git ref OID"))?;
            if words.next() == Some(reference.as_bytes()) {
                return Oid::parse(oid);
            }
        }
        Err(Error::Invalid("upstream branch not advertised"))
    }

    pub async fn fetch(
        &mut self,
        have: Oid,
        want: Oid,
        cache: &Path,
        maximum: usize,
    ) -> Result<Bytes> {
        match self.fetch_once(have, want, cache, maximum).await {
            Err(Error::Transport(message))
                if have != Oid::ZERO && message.contains("not our ref") =>
            {
                eprintln!("upstream no longer knows the have tip; retrying a full fetch");
                self.fetch_once(Oid::ZERO, want, cache, maximum).await
            }
            result => result,
        }
    }

    async fn fetch_once(
        &mut self,
        have: Oid,
        want: Oid,
        cache: &Path,
        maximum: usize,
    ) -> Result<Bytes> {
        let mut temp = TempFile::new(&cache.join("incoming.pack"))?;
        let mut request = reserve(4096)?;
        if self.local {
            pkt(
                &mut request,
                format!("want {want} ofs-delta side-band-64k no-progress\n").as_bytes(),
            )?;
            append(&mut request, b"0000")?;
            if have != Oid::ZERO {
                pkt(&mut request, format!("have {have}\n").as_bytes())?;
            }
            pkt(&mut request, b"done\n")?;
            let source = self.source.strip_prefix("file://").unwrap_or(&self.source);
            let mut child = git_command()
                .args(["upload-pack", "--stateless-rpc", source])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()?;
            let result = (|| -> Result<()> {
                let mut stdin = child.stdin.take().ok_or(Error::Invalid("Git stdin"))?;
                stdin.write_all(&request)?;
                drop(stdin);
                let mut stdout = child
                    .stdout
                    .take()
                    .ok_or(Error::Invalid("Git pack stdout"))?;
                let mut sink = PacketSink::new(&mut temp.file, maximum, &mut self.packet_buffer);
                let mut chunk = [0; 32768];
                loop {
                    let len = stdout.read(&mut chunk)?;
                    if len == 0 {
                        break;
                    }
                    sink.feed(take(&chunk, 0, len)?)?;
                }
                sink.finish()
            })();
            if result.is_err()
                && let Err(error) = child.kill()
            {
                eprintln!("stopping upload-pack: {error}");
            }
            let status = child.wait()?;
            result?;
            if !status.success() {
                return Err(Error::Transport(format!(
                    "git upload-pack failed: {status}"
                )));
            }
        } else {
            self.advertise().await?;
            pkt(&mut request, b"command=fetch\n")?;
            append(&mut request, b"0001")?;
            pkt(&mut request, b"ofs-delta\n")?;
            pkt(&mut request, b"no-progress\n")?;
            pkt(&mut request, format!("want {want}\n").as_bytes())?;
            if have != Oid::ZERO {
                pkt(&mut request, format!("have {have}\n").as_bytes())?;
            }
            pkt(&mut request, b"done\n")?;
            append(&mut request, b"0000")?;
            let headers = self.headers();
            let response = self
                .http
                .as_mut()
                .ok_or(Error::Invalid("Git HTTP client"))?
                .request(
                    Method::POST,
                    "git-upload-pack",
                    "",
                    headers,
                    Some(Bytes::from(request)),
                )
                .await?;
            if response.status() != StatusCode::OK {
                return Err(Error::Http(response.status().as_u16()));
            }
            let mut body = response.into_body();
            let mut sink = PacketSink::new(&mut temp.file, maximum, &mut self.packet_buffer);
            while let Some(chunk) = next_data(&mut body).await? {
                sink.feed(&chunk)?;
            }
            sink.finish()?;
        }
        temp.file.flush()?;
        Ok(Bytes::from_owner(gvfs_platform::snapshot(
            &temp.path, maximum,
        )?))
    }
}

pub fn git_command() -> Command {
    let mut command = Command::new("git");
    for (key, _) in std::env::vars_os() {
        let name = key.to_string_lossy();
        if name == "GIT_CONFIG_COUNT"
            || name.starts_with("GIT_CONFIG_KEY_")
            || name.starts_with("GIT_CONFIG_VALUE_")
            || matches!(
                name.as_ref(),
                "GIT_DIR"
                    | "GIT_WORK_TREE"
                    | "GIT_INDEX_FILE"
                    | "GIT_OBJECT_DIRECTORY"
                    | "GIT_ALTERNATE_OBJECT_DIRECTORIES"
            )
        {
            command.env_remove(key);
        }
    }
    command
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_PAGER", "cat");
    command
}

fn read_bounded(mut read: impl Read, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = reserve(limit)?;
    let mut chunk = [0u8; 1024];
    loop {
        let len = read.read(&mut chunk)?;
        if len == 0 {
            return Ok(bytes);
        }
        append(&mut bytes, take(&chunk, 0, len)?)?;
    }
}

pub fn pkt(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let len = bytes
        .len()
        .checked_add(4)
        .filter(|len| *len <= 65520)
        .ok_or(Error::Capacity("pkt-line"))?;
    let mut header = [0u8; 4];
    let mut writer = Writer::new(&mut header);
    let digits = format!("{len:04x}");
    writer.put(digits.as_bytes())?;
    append(out, writer.written())?;
    append(out, bytes)
}

fn packet_len(bytes: &[u8]) -> Result<usize> {
    let mut value = 0usize;
    for &byte in bytes {
        let digit = match byte {
            b'0'..=b'9' => byte.wrapping_sub(b'0'),
            b'a'..=b'f' => byte.wrapping_sub(b'a').wrapping_add(10),
            b'A'..=b'F' => byte.wrapping_sub(b'A').wrapping_add(10),
            _ => return Err(Error::Invalid("pkt-line hex")),
        };
        value = value
            .checked_mul(16)
            .and_then(|v| v.checked_add(usize::from(digit)))
            .ok_or(Error::Invalid("pkt-line length"))?;
    }
    Ok(value)
}

pub struct PacketSink<'a> {
    out: &'a mut std::fs::File,
    prefix: [u8; 4],
    prefix_len: usize,
    payload: &'a mut [u8],
    payload_len: usize,
    expected: usize,
    total: usize,
    maximum: usize,
}

impl<'a> PacketSink<'a> {
    pub fn new(out: &'a mut std::fs::File, maximum: usize, payload: &'a mut [u8]) -> Self {
        Self {
            out,
            prefix: [0; 4],
            prefix_len: 0,
            payload,
            payload_len: 0,
            expected: 0,
            total: 0,
            maximum,
        }
    }

    pub fn feed(&mut self, mut bytes: &[u8]) -> Result<()> {
        while !bytes.is_empty() {
            if self.expected == 0 {
                let count = self
                    .prefix
                    .len()
                    .saturating_sub(self.prefix_len)
                    .min(bytes.len());
                let end = self.prefix_len.saturating_add(count);
                self.prefix
                    .get_mut(self.prefix_len..end)
                    .ok_or(Error::Invalid("pkt prefix"))?
                    .copy_from_slice(take(bytes, 0, count)?);
                self.prefix_len = end;
                bytes = bytes.get(count..).ok_or(Error::Invalid("pkt cursor"))?;
                if self.prefix_len != 4 {
                    continue;
                }
                let len = packet_len(&self.prefix)?;
                self.prefix_len = 0;
                if len <= 2 {
                    continue;
                }
                if !(5..=65520).contains(&len) {
                    return Err(Error::Invalid("pkt-line payload size"));
                }
                self.expected = len.saturating_sub(4);
                self.payload_len = 0;
            }
            let count = self
                .expected
                .saturating_sub(self.payload_len)
                .min(bytes.len());
            let end = self.payload_len.saturating_add(count);
            self.payload
                .get_mut(self.payload_len..end)
                .ok_or(Error::Invalid("pkt payload"))?
                .copy_from_slice(take(bytes, 0, count)?);
            self.payload_len = end;
            bytes = bytes
                .get(count..)
                .ok_or(Error::Invalid("pkt payload cursor"))?;
            if self.payload_len != self.expected {
                continue;
            }
            let payload = take(self.payload, 0, self.expected)?;
            if let Some(data) = payload.strip_prefix(&[1]) {
                if data.is_empty() {
                    // An empty band-1 packet is upload-pack's keep-alive (uploadpack.keepAlive),
                    // sent while a large pack is still being counted.
                    self.expected = 0;
                    continue;
                }
                if self.total == 0 && !(data.starts_with(b"PACK") || b"PACK".starts_with(data)) {
                    return Err(Error::Invalid("sideband PACK signature"));
                }
                self.total = self
                    .total
                    .checked_add(data.len())
                    .ok_or(Error::Capacity("incoming pack"))?;
                if self.total > self.maximum {
                    return Err(Error::Capacity("incoming pack bytes"));
                }
                self.out.write_all(data)?;
            } else if payload.first() == Some(&3) || payload.starts_with(b"ERR ") {
                return Err(Error::Transport(
                    String::from_utf8_lossy(payload).into_owned(),
                ));
            } else if payload.first() != Some(&2)
                && !payload.starts_with(b"ACK ")
                && payload != b"NAK\n"
                && payload != b"packfile\n"
                && payload != b"acknowledgments\n"
                && payload != b"ready\n"
            {
                return Err(Error::Invalid("Git response section"));
            }
            self.expected = 0;
        }
        Ok(())
    }

    pub fn finish(&self) -> Result<()> {
        if self.prefix_len != 0 || self.expected != 0 || self.total < 32 {
            return Err(Error::Invalid("truncated Git pack response"));
        }
        Ok(())
    }
}

pub fn input_pack(path: &Path, maximum: usize) -> Result<Bytes> {
    Ok(Bytes::from_owner(gvfs_platform::snapshot(path, maximum)?))
}
