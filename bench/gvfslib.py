"""Implementation-neutral GVFS client + stock-git validation helpers.

Everything here judges servers only with stock git (index-pack, rev-list, cat-file),
never with an implementation's own reader.
"""
import hashlib
import http.client
import json
import os
import socket
import struct
import subprocess
import tempfile
import time
import zlib

GIT = "git"


def clean_env(extra=None):
    env = {k: v for k, v in os.environ.items()
           if not (k.startswith("GIT_CONFIG_") or k in (
               "GIT_CONFIG_COUNT", "GIT_CONFIG_PARAMETERS", "GIT_DIR", "GIT_WORK_TREE",
               "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_OPTIONAL_LOCKS",
               "GIT_ASKPASS"))}
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    env["GIT_CONFIG_GLOBAL"] = "/dev/null"
    env["GIT_TERMINAL_PROMPT"] = "0"
    env.setdefault("GIT_AUTHOR_NAME", "t")
    env.setdefault("GIT_AUTHOR_EMAIL", "t@t")
    env.setdefault("GIT_COMMITTER_NAME", "t")
    env.setdefault("GIT_COMMITTER_EMAIL", "t@t")
    if extra:
        env.update(extra)
    return env


def git(git_dir, *args, input=None, check=True, env=None, text=False):
    cmd = [GIT, f"--git-dir={git_dir}", *args] if git_dir else [GIT, *args]
    p = subprocess.run(cmd, input=input, capture_output=True, env=clean_env(env), text=text)
    if check and p.returncode != 0:
        err = p.stderr if text else p.stderr.decode(errors="replace")
        raise RuntimeError(f"git {' '.join(map(str, args))} failed ({p.returncode}): {err[-2000:]}")
    return p


def git_out(git_dir, *args, **kw):
    return git(git_dir, *args, **kw).stdout.decode().strip()


def init_bare(path):
    os.makedirs(path, exist_ok=True)
    git(None, "init", "-q", "--bare", "-b", "main", path)
    return path


# ---------------------------------------------------------------- HTTP

class Resp:
    def __init__(self, status, headers, body, secs):
        self.status = status
        self.headers = {k.lower(): v for k, v in headers}
        self.body = body
        self.secs = secs

    def __repr__(self):
        return f"<Resp {self.status} {len(self.body)}B {self.secs * 1000:.1f}ms>"


def request(port, method, path, body=None, headers=None, timeout=600, host="127.0.0.1"):
    h = dict(headers or {})
    if body is not None and not isinstance(body, (bytes, bytearray)):
        body = json.dumps(body).encode()
        h.setdefault("Content-Type", "application/json")
    t0 = time.perf_counter()
    c = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        c.request(method, path, body=body, headers=h)
        r = c.getresponse()
        data = r.read()
        return Resp(r.status, r.getheaders(), data, time.perf_counter() - t0)
    finally:
        c.close()


def wait_http(port, path="/gvfs/config", timeout=120, proc=None):
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"server exited with {proc.returncode} before becoming ready")
        try:
            r = request(port, "GET", path, timeout=5)
            if r.status == 200:
                return time.time() - t0
            last = r.status
        except (ConnectionRefusedError, ConnectionResetError, socket.timeout, OSError) as e:
            last = e
        time.sleep(0.05)
    raise TimeoutError(f"server on {port} not ready after {timeout}s (last={last})")


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def metrics(port):
    try:
        r = request(port, "GET", "/metrics", timeout=10)
    except OSError:
        return {}
    out = {}
    if r.status != 200:
        return out
    for line in r.body.decode(errors="replace").splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) >= 2:
            try:
                out[parts[0]] = float(parts[1])
            except ValueError:
                pass
    return out


# ---------------------------------------------------------------- GPRE

class GpreError(Exception):
    pass


def parse_gpre(body):
    """Return list of (timestamp, pack_bytes, idx_bytes_or_None). Strict: no trailing bytes."""
    if len(body) < 8 or body[:5] != b"GPRE " or body[5] != 1:
        raise GpreError(f"bad GPRE header {body[:8]!r}")
    (n,) = struct.unpack_from("<H", body, 6)
    off = 8
    packs = []
    for _ in range(n):
        if off + 24 > len(body):
            raise GpreError("truncated pack header")
        ts, plen, ilen = struct.unpack_from("<qqq", body, off)
        off += 24
        if plen < 0 or off + plen > len(body):
            raise GpreError(f"bad pack length {plen}")
        pack = body[off:off + plen]
        off += plen
        idx = None
        if ilen >= 0:
            if off + ilen > len(body):
                raise GpreError(f"bad idx length {ilen}")
            idx = body[off:off + ilen]
            off += ilen
        elif ilen != -1:
            raise GpreError(f"bad idx length {ilen}")
        packs.append((ts, pack, idx))
    if off != len(body):
        raise GpreError(f"{len(body) - off} trailing bytes after GPRE")
    ts_list = [p[0] for p in packs]
    if ts_list != sorted(ts_list):
        raise GpreError(f"packs not in increasing timestamp order: {ts_list}")
    return packs


def pack_object_count(pack):
    if len(pack) < 32 or pack[:4] != b"PACK":
        raise GpreError("not a pack")
    (ver, n) = struct.unpack_from(">II", pack, 4)
    if ver not in (2, 3):
        raise GpreError(f"pack version {ver}")
    if hashlib.sha1(pack[:-20]).digest() != pack[-20:]:
        raise GpreError("pack trailer SHA-1 mismatch")
    return n


# ---------------------------------------------------------------- validation with stock git

def index_pack(pack_bytes, workdir, strict=False, verify_idx=None, repo=None):
    """git index-pack a pack inside an empty scratch repo (so REF bases cannot silently come from
    elsewhere). Returns (pack_path, idx_path, [(oid, type, size)]). Raises on any git complaint.
    If verify_idx is given, it must be byte-identical to git's own .idx."""
    os.makedirs(workdir, exist_ok=True)
    scratch = repo or init_bare(os.path.join(workdir, "ip.git"))
    fd, ppath = tempfile.mkstemp(suffix=".pack", dir=workdir)
    with os.fdopen(fd, "wb") as f:
        f.write(pack_bytes)
    ipath = ppath[:-5] + ".idx"
    args = ["index-pack"]
    if strict:
        args.append("--strict")
    args += ["-o", ipath, ppath]
    git(scratch, *args)
    if verify_idx is not None:
        with open(ipath, "rb") as f:
            mine = f.read()
        if mine != verify_idx:
            raise GpreError(f"server .idx ({len(verify_idx)}B) differs from git index-pack's ({len(mine)}B)")
    vp = git_out(scratch, "verify-pack", "-v", ipath)
    objs = []
    for line in vp.splitlines():
        parts = line.split()
        if len(parts) >= 3 and len(parts[0]) == 40 and parts[1] in ("commit", "tree", "blob", "tag"):
            objs.append((parts[0], parts[1], int(parts[2])))
    return ppath, ipath, objs


def install_pack(client_repo, pack_bytes):
    """Install a pack into a client repo the way a client would (index-pack --stdin)."""
    p = subprocess.run([GIT, f"--git-dir={client_repo}", "index-pack", "--stdin"],
                       input=pack_bytes, capture_output=True, env=clean_env())
    if p.returncode != 0:
        raise GpreError("index-pack --stdin failed: " + p.stderr.decode(errors="replace")[-1500:])
    return p.stdout.decode().strip()


def missing_objects(repo, tip):
    """Objects reachable from tip that repo lacks (the '?' lines of rev-list --missing=print)."""
    p = git(repo, "rev-list", "--objects", "--missing=print", tip, check=False)
    if p.returncode != 0:
        return ["<rev-list failed: " + p.stderr.decode(errors="replace")[:300] + ">"]
    return [l[1:] for l in p.stdout.decode().splitlines() if l.startswith("?")]


def has_object(repo, oid):
    return git(repo, "cat-file", "-e", oid, check=False).returncode == 0


def rev_list_objects(repo, *revs):
    out = git_out(repo, "rev-list", "--objects", *revs)
    return {l.split()[0] for l in out.splitlines() if l}


def cat_batch(repo, oids):
    """{oid: (type, bytes) or None} via cat-file --batch."""
    oids = list(oids)
    inp = ("\n".join(oids) + "\n").encode()
    out = git(repo, "cat-file", "--batch", input=inp).stdout
    res = {}
    i = 0
    for oid in oids:
        nl = out.index(b"\n", i)
        hdr = out[i:nl].decode().split()
        i = nl + 1
        if len(hdr) == 2 and hdr[1] == "missing":
            res[oid] = None
            continue
        t, sz = hdr[1], int(hdr[2])
        res[oid] = (t, out[i:i + sz])
        i += sz + 1
    return res


def check_loose(oid, body):
    """GET /gvfs/objects/<oid> must inflate to '<type> <size>\\0<content>' hashing to oid."""
    d = zlib.decompressobj()
    raw = d.decompress(body) + d.flush()
    if d.unused_data:
        raise GpreError(f"{oid}: {len(d.unused_data)} bytes after the zlib stream")
    nul = raw.index(b"\0")
    t, sz = raw[:nul].split(b" ")
    content = raw[nul + 1:]
    if int(sz) != len(content):
        raise GpreError(f"{oid}: loose header size {sz} != {len(content)}")
    if hashlib.sha1(raw).hexdigest() != oid:
        raise GpreError(f"{oid}: loose object hashes to {hashlib.sha1(raw).hexdigest()}")
    return t.decode(), content
