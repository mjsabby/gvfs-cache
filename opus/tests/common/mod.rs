//! Shared helpers for the end-to-end tests: scratch repositories, the daemon binary, a
//! small HTTP client, and git-based verification of what the server sends.
#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use gvfs_daemon::http::{Client, Url};
use gvfs_daemon::manifest::{MANIFEST_KEY, Manifest};

pub const CT_PACK: &str = "application/x-git-packfile";
pub const CT_LOOSE: &str = "application/x-git-loose-object";

pub fn tempdir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("gvd-e2e-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// `git` that ignores the user's, the system's, and environment-injected configuration.
pub fn git_cmd(dir: &Path) -> Command {
    let mut c = Command::new("git");
    c.current_dir(dir)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null");
    gvfs_daemon::upstream::hermetic_git_env(&mut c);
    c
}

pub fn git(dir: &Path, args: &[&str]) -> String {
    let out = git_cmd(dir).args(args).output().unwrap();
    assert!(
        out.status.success(),
        "git {args:?} in {}: {}",
        dir.display(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

pub fn git_stdin(dir: &Path, args: &[&str], stdin: &[u8]) -> Vec<u8> {
    let mut child = git_cmd(dir)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let data = stdin.to_vec();
    let writer = std::thread::spawn(move || input.write_all(&data));
    let out = child.wait_with_output().unwrap();
    writer.join().unwrap().unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

/// An empty bare repository (a client object store).
pub fn fresh_repo(root: &Path, name: &str) -> PathBuf {
    let p = root.join(name);
    git(root, &["init", "-q", "--bare", p.to_str().unwrap()]);
    p
}

/// A bare upstream plus a work tree that pushes to it.
pub struct Upstream {
    pub bare: PathBuf,
    pub work: PathBuf,
    n: u32,
}

impl Upstream {
    pub fn new(root: &Path) -> Upstream {
        let bare = root.join("upstream.git");
        let work = root.join("work");
        git(
            root,
            &["init", "-q", "--bare", "-b", "main", bare.to_str().unwrap()],
        );
        git(root, &["init", "-q", "-b", "main", work.to_str().unwrap()]);
        git(&work, &["remote", "add", "origin", bare.to_str().unwrap()]);
        Upstream { bare, work, n: 0 }
    }

    /// One commit: an edit to a large text file (git deltifies it), new GitHFS pointer
    /// files in nested directories (trees that change by a few entries), and a push.
    pub fn commit(&mut self, pointers: u32) -> String {
        self.n += 1;
        let n = self.n;
        let mut big = String::new();
        for l in 0..600usize {
            big.push_str(&format!(
                "line {l}{}\n",
                if l % 97 == n as usize % 97 {
                    format!(" edit {n}")
                } else {
                    String::new()
                }
            ));
        }
        std::fs::write(self.work.join("big.txt"), big).unwrap();
        for i in 0..pointers {
            let dir = self.work.join(format!("d{}/e{}", i % 4, i % 3));
            std::fs::create_dir_all(&dir).unwrap();
            let sha = format!("{:064X}", u64::from(n) * 1000 + u64::from(i));
            std::fs::write(
                dir.join(format!("f{n}-{i}.dat")),
                format!("~~Git-hfs 1.0\nblob:{sha}\nsize:{}\n", n * 7 + i),
            )
            .unwrap();
        }
        git(&self.work, &["add", "-A"]);
        git(
            &self.work,
            &[
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@t",
                "commit",
                "-q",
                "-m",
                &format!("commit {n}"),
            ],
        );
        git(&self.work, &["push", "-q", "origin", "HEAD:main"]);
        self.tip()
    }

    pub fn tip(&self) -> String {
        git(&self.bare, &["rev-parse", "main"]).trim().to_string()
    }

    /// Every object id reachable from `tip`.
    pub fn objects(&self, tip: &str) -> Vec<String> {
        git(&self.bare, &["rev-list", "--objects", tip])
            .lines()
            .map(|l| l[..40].to_string())
            .collect()
    }
}

/// Runs `gvfs-daemon pipeline` against a local upstream; returns its log.
pub fn pipeline(root: &Path, store: &str, up: &Upstream, tier: &str, extra: &[&str]) -> String {
    pipeline_at(root, store, up.bare.to_str().unwrap(), tier, extra, &[])
        .unwrap_or_else(|log| panic!("pipeline --tier {tier} {extra:?} failed:\n{log}"))
}

/// Runs `gvfs-daemon pipeline` against any upstream spec; `Err` holds the log of a failure.
pub fn pipeline_at(
    root: &Path,
    store: &str,
    upstream: &str,
    tier: &str,
    extra: &[&str],
    env: &[(&str, &str)],
) -> Result<String, String> {
    let work = root.join("pipeline-work");
    std::fs::create_dir_all(&work).unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_gvfs-daemon"));
    cmd.args([
        "pipeline",
        "--store",
        store,
        "--upstream",
        upstream,
        "--tier",
        tier,
        "--threads",
        "2",
    ])
    .args(["--work-dir", work.to_str().unwrap()])
    .args(extra)
    .envs(env.iter().copied());
    gvfs_daemon::upstream::hermetic_git_env(&mut cmd);
    let out = cmd.output().unwrap();
    let log = String::from_utf8_lossy(&out.stderr).into_owned();
    if out.status.success() {
        Ok(log)
    } else {
        Err(log)
    }
}

/// The published manifest of a `fs:` store.
pub fn manifest(store_dir: &Path) -> Manifest {
    let bytes = std::fs::read(store_dir.join(MANIFEST_KEY)).unwrap();
    Manifest::decode(&bytes).unwrap()
}

/// Prefetch packs: (timestamp, pack bytes), oldest first.
pub type Packs = Vec<(u64, Vec<u8>)>;

/// The daemon's `serve` command, killed on drop.
pub struct Server {
    child: Child,
    pub port: u16,
}

impl Server {
    pub fn start(args: &[&str]) -> Server {
        Server::start_with_env(args, &[])
    }

    /// Like [`Server::start`], but returns as soon as the server answers `/healthz` at all
    /// (a `--blobs separate` store is not ready until its tip is complete).
    pub fn start_unready(args: &[&str]) -> Server {
        Server::spawn(args, &[], false)
    }

    pub fn start_with_env(args: &[&str], env: &[(&str, &str)]) -> Server {
        Server::spawn(args, env, true)
    }

    fn spawn(args: &[&str], env: &[(&str, &str)], ready: bool) -> Server {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let bind = format!("127.0.0.1:{port}");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_gvfs-daemon"));
        // Defaults first: the last occurrence of a flag wins, so a test can override them.
        cmd.arg("serve")
            .args([
                "--bind",
                &bind,
                "--workers",
                "8",
                "--arenas",
                "4",
                "--alloc-guard",
                "strict",
            ])
            .args(args)
            .envs(env.iter().copied());
        gvfs_daemon::upstream::hermetic_git_env(&mut cmd);
        let child = cmd.spawn().unwrap();
        let mut s = Server { child, port };
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            match s.try_get("/healthz") {
                Ok((200, _, _)) => return s,
                Ok(_) if !ready => return s,
                _ => {}
            }
            if let Ok(Some(status)) = s.child.try_wait() {
                panic!("server exited during startup: {status}");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("server did not become ready");
    }

    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn client(&self) -> Client {
        Client::new(Url::parse(&self.url()).unwrap(), Duration::from_secs(60))
    }

    fn try_get(&self, path: &str) -> gvfs_daemon::util::Result<(u16, String, Vec<u8>)> {
        let mut c = self.client();
        let mut r = c.request("GET", path, &[], b"")?;
        let ct = r.header("content-type").unwrap_or("").to_string();
        let body = r.body_vec(1 << 32)?;
        Ok((r.status, ct, body))
    }

    pub fn get(&self, path: &str) -> (u16, String, Vec<u8>) {
        self.try_get(path).unwrap()
    }

    /// A GET with extra request headers: status, response headers asked for, body.
    pub fn get_with(
        &self,
        path: &str,
        headers: &[(&str, &str)],
        want: &[&str],
    ) -> (u16, Vec<Option<String>>, Vec<u8>) {
        let mut c = self.client();
        let mut r = c.request("GET", path, headers, b"").unwrap();
        let got = want
            .iter()
            .map(|h| r.header(h).map(str::to_string))
            .collect();
        let body = r.body_vec(1 << 34).unwrap();
        (r.status, got, body)
    }

    pub fn post(&self, path: &str, body: &[u8], accept: &str) -> (u16, String, Vec<u8>) {
        let mut c = self.client();
        let mut r = c
            .request(
                "POST",
                path,
                &[("Content-Type", "application/json"), ("Accept", accept)],
                body,
            )
            .unwrap();
        let ct = r.header("content-type").unwrap_or("").to_string();
        let body = r.body_vec(1 << 32).unwrap();
        (r.status, ct, body)
    }

    /// One packfile per GVFS prefetch (`since` = the client's newest pack timestamp), as a
    /// VFS for Git client gets it: with its `.idx`, which must be exactly git's.
    pub fn prefetch(&self, since: u64) -> Vec<(u64, Vec<u8>)> {
        self.prefetch_as(since, None)
            .into_iter()
            .map(|(ts, pack, idx)| {
                assert_idx_is_gits(&pack, &idx.expect("a .idx for clients other than git"));
                (ts, pack)
            })
            .collect()
    }

    /// A prefetch as gvfs-helper (`User-Agent: git/...`) sees it: packs without indexes.
    pub fn prefetch_git(&self, since: u64) -> Vec<(u64, Vec<u8>)> {
        self.prefetch_as(since, Some("git/2.45.2.vfs.0.0"))
            .into_iter()
            .map(|(ts, pack, idx)| {
                assert!(idx.is_none(), "git clients index packs themselves");
                (ts, pack)
            })
            .collect()
    }

    /// A prefetch as VFS for Git gets it, with the interval the pack covers
    /// (`X-GVFS-Pack-Start`/`End`).
    pub fn prefetch_interval(&self, since: u64) -> (Packs, Option<(u64, u64)>) {
        let mut c = self.client();
        let mut r = c
            .request(
                "GET",
                &format!("/org/repo/gvfs/prefetch?lastPackTimestamp={since}"),
                &[],
                b"",
            )
            .unwrap();
        assert_eq!(r.status, 200);
        let num = |h: &str| r.header(h).map(|v| v.parse::<u64>().unwrap());
        let interval = num("x-gvfs-pack-start").zip(num("x-gvfs-pack-end"));
        let packs = parse_gpre(&r.body_vec(1 << 34).unwrap())
            .into_iter()
            .map(|(ts, pack, idx)| {
                assert_idx_is_gits(&pack, &idx.expect("a .idx for clients other than git"));
                (ts, pack)
            })
            .collect();
        (packs, interval)
    }

    fn prefetch_as(&self, since: u64, agent: Option<&str>) -> Vec<(u64, Vec<u8>, Option<Vec<u8>>)> {
        let headers: Vec<(&str, &str)> = agent.map(|a| ("User-Agent", a)).into_iter().collect();
        let mut c = self.client();
        let mut r = c
            .request(
                "GET",
                &format!("/org/repo/gvfs/prefetch?lastPackTimestamp={since}"),
                &headers,
                b"",
            )
            .unwrap();
        assert_eq!(r.status, 200);
        assert_eq!(
            r.header("content-type"),
            Some("application/x-gvfs-timestamped-packfiles-indexes")
        );
        parse_gpre(&r.body_vec(1 << 34).unwrap())
    }

    pub fn metric(&self, name: &str) -> u64 {
        let (_, _, body) = self.get("/metrics");
        let text = String::from_utf8(body).unwrap();
        text.lines()
            .find_map(|l| {
                l.strip_prefix(name)
                    .and_then(|v| v.strip_prefix(' '))
                    .and_then(|v| v.trim().parse().ok())
            })
            .unwrap_or_else(|| panic!("no metric {name} in:\n{text}"))
    }

    /// Polls until `f` holds (panics after 30 s).
    pub fn wait_for(&self, what: &str, f: impl Fn(&Server) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if f(self) {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("timed out waiting for {what}");
    }

    /// The server's invariants: no hot-path allocation (strict mode would have aborted),
    /// no internal errors.
    pub fn assert_clean(&self) {
        assert_eq!(
            self.metric("gvfs_alloc_hot_path_total"),
            0,
            "hot-path allocations"
        );
        assert_eq!(self.metric("gvfs_errors_total"), 0, "server errors");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The packs of a `GPRE` prefetch response: `(timestamp, pack, .idx if sent)`.
pub fn parse_gpre(body: &[u8]) -> Vec<(u64, Vec<u8>, Option<Vec<u8>>)> {
    assert_eq!(&body[..6], b"GPRE \x01");
    let count = u16::from_le_bytes([body[6], body[7]]) as usize;
    let mut at = 8;
    let mut packs = Vec::new();
    for _ in 0..count {
        let ts = u64::from_le_bytes(body[at..at + 8].try_into().unwrap());
        let len = u64::from_le_bytes(body[at + 8..at + 16].try_into().unwrap()) as usize;
        let idx_len = i64::from_le_bytes(body[at + 16..at + 24].try_into().unwrap());
        at += 24;
        let pack = body[at..at + len].to_vec();
        at += len;
        let idx = (idx_len >= 0).then(|| {
            let idx = body[at..at + idx_len as usize].to_vec();
            at += idx_len as usize;
            idx
        });
        packs.push((ts, pack, idx));
    }
    assert_eq!(at, body.len(), "trailing bytes after the packs");
    packs
}

/// The server's `.idx` is byte for byte what `git index-pack` writes for the pack.
pub fn assert_idx_is_gits(pack: &[u8], idx: &[u8]) {
    static SEQ: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let dir = std::env::temp_dir().join(format!(
        "gvd-idx-check-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("p.pack"), pack).unwrap();
    git(&dir, &["index-pack", "-o", "p.idx", "p.pack"]);
    let want = std::fs::read(dir.join("p.idx")).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        idx == want,
        "the server's .idx ({} bytes) differs from git's ({} bytes)",
        idx.len(),
        want.len()
    );
}

/// Indexes `pack` into `repo` the way gvfs-helper does (`index-pack`, which also checks
/// the trailer and every object's hash); returns the number of entries.
pub fn install_pack(repo: &Path, pack: &[u8]) -> usize {
    assert_eq!(&pack[..4], b"PACK");
    let count = u32::from_be_bytes(pack[8..12].try_into().unwrap()) as usize;
    git_stdin(repo, &["index-pack", "--stdin", "--keep=test"], pack);
    count
}

/// Fails unless every object reachable from `tip` is in `repo`.
pub fn assert_complete(repo: &Path, tip: &str) {
    let out = git_cmd(repo)
        .args(["rev-list", "--objects", "--missing=print", tip])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "rev-list {tip}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8(out.stdout).unwrap();
    let missing: Vec<&str> = text.lines().filter(|l| l.starts_with('?')).collect();
    assert!(
        missing.is_empty(),
        "{} objects of {tip} missing, e.g. {:?}",
        missing.len(),
        &missing[..missing.len().min(5)]
    );
}

/// `git cat-file --batch` output for `ids` (type, size, and content of each).
pub fn cat_batch(repo: &Path, ids: &[String]) -> Vec<u8> {
    git_stdin(repo, &["cat-file", "--batch"], ids.join("\n").as_bytes())
}

/// `(oid, type)` of each id, from `repo`.
pub fn types(repo: &Path, ids: &[String]) -> Vec<(String, String)> {
    let out = git_stdin(
        repo,
        &["cat-file", "--batch-check=%(objectname) %(objecttype)"],
        ids.join("\n").as_bytes(),
    );
    String::from_utf8(out)
        .unwrap()
        .lines()
        .map(|l| {
            let (o, t) = l.split_once(' ').unwrap();
            (o.to_string(), t.to_string())
        })
        .collect()
}

pub fn json_ids(ids: &[String], commit_depth: Option<u32>) -> String {
    let list: Vec<String> = ids.iter().map(|i| format!("\"{i}\"")).collect();
    match commit_depth {
        Some(d) => format!("{{\"objectIds\":[{}],\"commitDepth\":{d}}}", list.join(",")),
        None => format!("{{\"objectIds\":[{}]}}", list.join(",")),
    }
}

/// Microsoft git with gvfs-helper, if available (`GVFS_TEST_MSGIT` or ~/git).
pub fn msgit() -> Option<PathBuf> {
    let p = std::env::var_os("GVFS_TEST_MSGIT")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|h| PathBuf::from(h).join("git/bin-wrappers/git"))
        })?;
    let ok = Command::new(&p)
        .args(["gvfs-helper", "--help"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .ok()?;
    // `--help` exits 129 when the command exists.
    (ok.code() == Some(129) || ok.success()).then_some(p)
}

/// A smart-HTTP git server for `repo` at `http://127.0.0.1:<port>/org/proj/_git/repo`, like
/// Azure DevOps: HTTP Basic auth (any user, password `token`), keep-alive connections, and
/// chunked `git-upload-pack` responses. With `v2` false it ignores `Git-Protocol` and
/// speaks only protocol v0, like an old server.
pub struct SmartHttp {
    pub port: u16,
    pub url: String,
    posts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    v2_posts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

pub const SMART_HTTP_PREFIX: &str = "/org/proj/_git/repo";

impl SmartHttp {
    pub fn start(repo: &Path, token: &str, v2: bool) -> SmartHttp {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let posts = Arc::new(AtomicUsize::new(0));
        let v2_posts = Arc::new(AtomicUsize::new(0));
        let expected_auth = gvfs_daemon::http::basic_auth("", token);
        let (repo, p, p2) = (repo.to_path_buf(), posts.clone(), v2_posts.clone());
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(conn) = conn else { continue };
                let (repo, auth, p, p2) =
                    (repo.clone(), expected_auth.clone(), p.clone(), p2.clone());
                std::thread::spawn(move || {
                    let _ = smart_http_connection(conn, &repo, &auth, v2, &p, &p2);
                });
            }
        });
        SmartHttp {
            port,
            url: format!("http://127.0.0.1:{port}{SMART_HTTP_PREFIX}"),
            posts,
            v2_posts,
        }
    }

    pub fn posts(&self) -> usize {
        self.posts.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn v2_posts(&self) -> usize {
        self.v2_posts.load(std::sync::atomic::Ordering::Relaxed)
    }
}

fn upload_pack(repo: &Path, v2: bool, advertise: bool) -> Command {
    let mut c = git_cmd(repo);
    c.arg("upload-pack").arg("--stateless-rpc");
    if advertise {
        c.arg("--advertise-refs");
    }
    c.arg(repo);
    if v2 {
        c.env("GIT_PROTOCOL", "version=2");
    }
    c
}

fn smart_http_connection(
    conn: std::net::TcpStream,
    repo: &Path,
    expected_auth: &str,
    allow_v2: bool,
    posts: &std::sync::atomic::AtomicUsize,
    v2_posts: &std::sync::atomic::AtomicUsize,
) -> std::io::Result<()> {
    use std::io::{BufRead, BufReader, Read};
    use std::sync::atomic::Ordering;
    let mut reader = BufReader::new(conn.try_clone()?);
    let mut out = conn;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Ok(());
        }
        let mut parts = line.split_whitespace();
        let (method, target) = (
            parts.next().unwrap_or("").to_string(),
            parts.next().unwrap_or("").to_string(),
        );
        let mut headers = Vec::new();
        loop {
            let mut h = String::new();
            reader.read_line(&mut h)?;
            let h = h.trim_end();
            if h.is_empty() {
                break;
            }
            if let Some((k, v)) = h.split_once(':') {
                headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
            }
        }
        let header = |name: &str| {
            headers
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.as_str())
        };
        let mut body = vec![
            0u8;
            header("content-length")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0)
        ];
        reader.read_exact(&mut body)?;
        if header("authorization") != Some(expected_auth) {
            out.write_all(b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"test\"\r\nContent-Length: 0\r\n\r\n")?;
            continue;
        }
        let v2 = allow_v2 && header("git-protocol").is_some_and(|v| v.contains("version=2"));
        if method == "GET"
            && target == format!("{SMART_HTTP_PREFIX}/info/refs?service=git-upload-pack")
        {
            let refs = upload_pack(repo, v2, true).output()?;
            assert!(
                refs.status.success(),
                "{}",
                String::from_utf8_lossy(&refs.stderr)
            );
            let mut body = Vec::new();
            if !v2 {
                body.extend_from_slice(b"001e# service=git-upload-pack\n0000");
            }
            body.extend_from_slice(&refs.stdout);
            write!(
                out,
                "HTTP/1.1 200 OK\r\nContent-Type: application/x-git-upload-pack-advertisement\r\nContent-Length: {}\r\n\r\n",
                body.len()
            )?;
            out.write_all(&body)?;
        } else if method == "POST" && target == format!("{SMART_HTTP_PREFIX}/git-upload-pack") {
            posts.fetch_add(1, Ordering::Relaxed);
            if v2 {
                v2_posts.fetch_add(1, Ordering::Relaxed);
            }
            let mut child = upload_pack(repo, v2, false)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()?;
            let mut stdin = child.stdin.take().unwrap();
            let writer = std::thread::spawn(move || stdin.write_all(&body));
            let mut stdout = child.stdout.take().unwrap();
            out.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-git-upload-pack-result\r\nTransfer-Encoding: chunked\r\n\r\n")?;
            let mut buf = [0u8; 8192];
            loop {
                let n = stdout.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                write!(out, "{n:x}\r\n")?;
                out.write_all(&buf[..n])?;
                out.write_all(b"\r\n")?;
            }
            out.write_all(b"0\r\n\r\n")?;
            let _ = writer.join();
            child.wait()?;
        } else {
            out.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")?;
        }
    }
}
