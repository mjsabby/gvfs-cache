"""Replay of a `--blobs separate` store over a synthetic repository's two weeks of pushes
(bench/corpus_gen.py): the pipeline on its cron, a live edge (L0 from the upstream, its blobs
read through), and clients that prefetch commits and trees and then fetch the blobs of
their sparse cone, as gvfs-helper does. Judged by stock git.

Per prefetch (with `X-GVFS-Sizes: 1`): strict GPRE, <= 1 pack, the pack self-contained and
complete (every commit and tree the client lacks, exactly as in bench/replay.py: object
sets from `rev-list --objects --filter=blob:none`), blobs never in it; a GSZ1 section whose
every size is git's, so that the client knows the size of every file of its cone at the tip
before it fetches any blob.
Per blob fetch (POST, at most 4,000 ids, what gvfs-helper sends): one pack streamed through
a parser that inflates every entry and hashes it: exactly the blobs asked for, each git's.
Cold clones (CI, every 2 h): one pack with every commit and tree of the tip, the history's
sizes, then the cone's blobs from scratch.

Warm clients start at genesis with their cone checked out (they already hold its blobs).
Edge: no hot-path allocation, no blob of a sealed tree missing from the store.

    python3 bench/replay_separate.py --corpus ~/gvfs-bench/corpus-l [--limit N]
Results: bench/results/replay-separate.json (+ responses.csv in the work dir).
"""
import argparse
import datetime as dt
import hashlib
import http.client
import json
import os
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import threading
import time
import zlib

sys.path.insert(0, os.path.dirname(__file__))
import impls  # noqa: E402
from gvfslib import GpreError, clean_env, git_out, init_bare, request  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UA = {"User-Agent": "git/2.53.0.vfs.0.0"}
BATCH = 4000
# Client: (cone, warm). None = every file.
CONES = {
    "agent-1m": ["src"],
    "agent-15m": ["services", "lib"],
    "agent-1h": ["models"],
    "dev-hourly": ["web", "sdk", "tests"],
    "dev-3x": ["bin", "config"],
    "dev-morning": None,
    "dev-vacation": ["services", "tests", "assets"],
    "stale": ["tools"],
    "ci-cold": ["services", "tests", "web"],
}


def utc(t):
    return dt.datetime.fromtimestamp(t, dt.timezone.utc)


def cron_tier(t):
    d = utc(t)
    if d.minute != 0:
        return "15m"
    if d.hour == 0 and d.weekday() == 6:
        return "168h"
    if d.hour == 0:
        return "24h"
    if d.hour % 12 == 0:
        return "12h"
    if d.hour % 6 == 0:
        return "6h"
    return "1h"


def schedule(S, E):
    ev = []
    t = S
    while t < E:
        d = utc(t)
        ev.append((t + 30, "agent-1m"))
        if d.minute % 15 == 0:
            ev.append((t + 450, "agent-15m"))
        if d.minute == 0:
            ev.append((t + 1800, "agent-1h"))
            if d.hour % 2 == 0:
                ev.append((t + 420, "ci-cold"))
            if d.weekday() < 5 and 8 <= d.hour <= 18:
                ev.append((t + 900, "dev-hourly"))
            if d.weekday() < 5 and d.hour in (9, 13):
                ev.append((t, "dev-3x"))
            if d.weekday() < 5 and d.hour == 17:
                ev.append((t + 1800, "dev-3x"))
            if d.weekday() < 5 and d.hour == 9:
                ev.append((t, "dev-morning"))
        t += 60
    ev.append((S + 9 * 3600, "dev-vacation"))
    ev.append((E - 15 * 3600, "dev-vacation"))
    ev.append((E - 60, "stale"))
    return sorted(e for e in ev if S < e[0] < E)


class Reader:
    """Buffered reads from an HTTP response, with put-back (for zlib's unused tail)."""

    def __init__(self, resp):
        self.resp, self.buf, self.pos, self.n = resp, bytearray(), 0, 0

    def read(self, n):
        while len(self.buf) - self.pos < n:
            chunk = self.resp.read(1 << 20)
            if not chunk:
                break
            if self.pos > (1 << 20):
                del self.buf[:self.pos]
                self.pos = 0
            self.buf += chunk
        out = bytes(self.buf[self.pos:self.pos + n])
        self.pos += len(out)
        self.n += len(out)
        return out

    def some(self):
        if self.pos < len(self.buf):
            out = bytes(self.buf[self.pos:])
        else:
            out = self.resp.read(1 << 20)
        self.buf, self.pos = bytearray(), 0
        self.n += len(out)
        return out

    def unread(self, data):
        self.buf = bytearray(data) + self.buf[self.pos:]
        self.pos = 0
        self.n -= len(data)


def verify_blob_pack(resp, want):
    """Streams a pack of blobs: every entry inflated and hashed. Returns ({id: size}, bytes)."""
    trail = hashlib.sha1()
    r = Reader(resp)
    head = r.read(12)
    trail.update(head)
    if head[:4] != b"PACK" or struct.unpack(">I", head[4:8])[0] not in (2, 3):
        raise GpreError("not a pack")
    n = struct.unpack(">I", head[8:12])[0]
    got = {}
    for _ in range(n):
        hdr = bytearray()
        c = r.read(1)
        hdr += c
        kind = (c[0] >> 4) & 7
        size = c[0] & 15
        shift = 4
        while c[0] & 0x80:
            c = r.read(1)
            hdr += c
            size |= (c[0] & 0x7F) << shift
            shift += 7
        trail.update(bytes(hdr))
        if kind != 3:
            raise GpreError(f"entry of type {kind} in a blob response")
        d = zlib.decompressobj()
        h = hashlib.sha1(b"blob %d\0" % size)
        seen = 0
        while not d.eof:
            chunk = r.some()
            if not chunk:
                raise GpreError("truncated entry")
            out = d.decompress(chunk)
            used = len(chunk) - len(d.unused_data)
            trail.update(chunk[:used])
            if d.unused_data:
                r.unread(d.unused_data)
            h.update(out)
            seen += len(out)
        if seen != size:
            raise GpreError(f"entry inflates to {seen} bytes, header says {size}")
        got[h.hexdigest()] = size
    tail = r.read(20)
    if tail != trail.digest():
        raise GpreError("pack trailer mismatch")
    if r.read(1):
        raise GpreError("bytes after the pack")
    if set(got) != set(want):
        raise GpreError(f"blobs asked {len(want)}, got {len(got)}; {len(set(want) - set(got))} missing, "
                        f"{len(set(got) - set(want))} not asked")
    return got, r.n


class Oracle:
    """Exact commit+tree sets (see bench/replay.py: `rev-list A ^B` is not a set difference),
    cones and sizes, from the upstream."""

    def __init__(self, up, seed):
        self.up, self.seed_tip = up, seed
        self.lock = threading.Lock()
        out = git_out(up, "rev-list", "--objects", "--filter=blob:none", seed)
        self.seed = frozenset(l[:40] for l in out.splitlines() if l)
        self.new = {}
        self.cones = {}
        self.ideal = {}
        self.sizes = {}

    def new_since_seed(self, tip):
        if tip == self.seed_tip:
            return frozenset()
        with self.lock:
            if tip in self.new:
                return self.new[tip]
        out = git_out(self.up, "rev-list", "--objects", "--filter=blob:none", tip, f"^{self.seed_tip}")
        v = frozenset(o for o in (l[:40] for l in out.splitlines() if l) if o not in self.seed)
        with self.lock:
            self.new[tip] = v
            if len(self.new) > 256:
                self.new.pop(next(iter(self.new)))
        return v

    def need(self, have, want):
        return self.new_since_seed(want) - self.new_since_seed(have)

    def count(self, tip):
        return len(self.seed) + len(self.new_since_seed(tip))

    def ideal_bytes(self, have, want):
        key = (have, want)
        with self.lock:
            if key in self.ideal:
                return self.ideal[key]
        p = subprocess.run(["git", f"--git-dir={self.up}", "pack-objects", "--revs", "--stdout", "-q",
                            "--filter=blob:none"], input=f"{want}\n^{have}\n".encode(),
                           capture_output=True, env=clean_env())
        with self.lock:
            self.ideal[key] = len(p.stdout)
        return len(p.stdout)

    def cone(self, tip, cone):
        """{blob id: size} of the cone at `tip` (every file when `cone` is None)."""
        key = (tip, tuple(cone) if cone else None)
        with self.lock:
            if key in self.cones:
                return self.cones[key]
        out = git_out(self.up, "ls-tree", "-r", "-l", tip, "--", *(cone or ["."]))
        v = {}
        for line in out.splitlines():
            meta, _ = line.split("\t", 1)
            _, kind, oid, size = meta.split()
            if kind == "blob":
                v[oid] = int(size)
        with self.lock:
            self.cones[key] = v
            if len(self.cones) > 512:
                self.cones.pop(next(iter(self.cones)))
        return v


class Client:
    def __init__(self, name, cursor, tip, blobs):
        self.name, self.cursor, self.tip = name, cursor, tip
        self.cone = CONES[name]
        self.blobs = set(blobs)          # blob ids it holds
        self.sizes = {}                  # sizes it was told (GSZ1)
        self.st = dict(requests=0, packs=0, pack_bytes=0, gpre_bytes=0, ideal_bytes=0, sizes_bytes=0,
                       sizes_records=0, empty=0, failures=0, max_s=0.0, sum_s=0.0,
                       blob_requests=0, blobs=0, blob_bytes=0, blob_content_bytes=0, blob_s=0.0,
                       max_blob_request_s=0.0)


class Replay:
    def __init__(self, a, meta):
        self.a, self.meta = a, meta
        self.work = a.work
        self.fail = []
        self.log = open(os.path.join(self.work, "replay.log"), "a")
        commits = meta["commits"]
        self.g_idx = meta["genesis_index"]
        self.commits = commits
        self.seed = meta["genesis"]
        self.S = commits[self.g_idx][1] - commits[self.g_idx][1] % 900 + 900
        self.E = min(commits[-1][1] + 1, self.S + a.days * 86400)
        self.clock = impls.Clock(os.path.join(self.work, "faketime"), self.S - 300)
        self.im = impls.ALL["opus"](self.work, clock=self.clock, extra_serve=["--workers", "64"],
                                    extra_pipeline=["--threads", "16"])
        self.up = os.path.join(self.work, "up.git")
        self.scratch = os.path.join(self.work, "scratch")
        os.makedirs(self.scratch, exist_ok=True)
        self.pipe = {"runs": 0, "by_tier": {}, "max_s": 0.0}
        self.csv = open(os.path.join(self.work, "responses.csv"), "w")
        self.csv.write("t,client,kind,status,objects,bytes,need,missing,ideal,secs\n")
        self.cold = []

    def L(self, *x):
        print(" ".join(str(v) for v in x), file=self.log, flush=True)

    def failure(self, what):
        self.fail.append(what)
        self.L("FAIL", what)

    def tip_at(self, t):
        k = self.g_idx
        while k + 1 < len(self.commits) and self.commits[k + 1][1] <= t:
            k += 1
        return self.commits[k][0]

    # -------------------------------------------------------------- setup
    def seed_store(self):
        init_bare(self.up)
        with open(os.path.join(self.up, "objects", "info", "alternates"), "w") as f:
            f.write(os.path.join(self.a.corpus, "up.git", "objects") + "\n")
        subprocess.run(["git", "--git-dir", self.up, "update-ref", "refs/heads/main", self.seed],
                       check=True, env=clean_env())
        t0 = time.time()
        self.im.run(self.im.pipeline_cmd(self.up, "15m", self.S - 300) + ["--blobs", "separate"])
        t_gen = time.time() - t0
        log = os.path.join(self.work, "mirror.log")
        t0 = time.time()
        with open(log, "wb") as f:
            p = subprocess.run(["/usr/bin/time", "-v", self.im.bin, "mirror", "--store", f"fs:{self.im.store}",
                                "--upstream", self.up, "--jobs", str(self.a.jobs), "--threads", "16",
                                "--work-dir", os.path.join(self.work, "mw")],
                               stdout=f, stderr=subprocess.STDOUT, env=clean_env())
        t_mirror = time.time() - t0
        text = open(log, errors="replace").read()
        if p.returncode != 0:
            raise RuntimeError("mirror failed:\n" + text[-3000:])
        rss = int(re.search(r"Maximum resident set size \(kbytes\): (\d+)", text).group(1))
        planned = re.search(r"mirror: (\d+) blobs in the history \(([^)]+)\).*planned in ([\d.]+)s", text)
        self.seeded = {"genesis_s": round(t_gen, 1), "mirror_s": round(t_mirror, 1), "mirror_peak_rss_mib": rss // 1024,
                       "history_blobs": int(planned.group(1)), "history_bytes": planned.group(2),
                       "mirror_planned_s": float(planned.group(3)), "mirror_jobs": self.a.jobs}
        print(f"seed: {self.seeded}", flush=True)

    # -------------------------------------------------------------- requests
    def prefetch(self, c, tip):
        """A warm prefetch with sizes; checked against the oracle."""
        c.st["requests"] += 1
        t0 = time.perf_counter()
        r = request(self.port, "GET", f"/org/repo/gvfs/prefetch?lastPackTimestamp={c.cursor}",
                    headers={**UA, "X-GVFS-Sizes": "1"})
        secs = time.perf_counter() - t0
        c.st["sum_s"] += secs
        c.st["max_s"] = max(c.st["max_s"], secs)
        if r.status != 200:
            c.st["failures"] += 1
            return self.failure(f"{c.name} prefetch status {r.status}")
        try:
            packs, sizes = parse_gpre_sizes(r.body)
        except GpreError as e:
            c.st["failures"] += 1
            return self.failure(f"{c.name} GPRE/GSZ1: {e}")
        need = self.oracle.need(c.tip, tip) if c.tip != tip else frozenset()
        c.st["sizes_records"] += len(sizes or {})
        c.st["sizes_bytes"] += (12 + 28 * len(sizes)) if sizes is not None else 0
        if sizes:
            self.check_sizes(c, sizes)
        if len(packs) > 1:
            c.st["failures"] += 1
            self.failure(f"{c.name} {len(packs)} packs")
        if not packs:
            c.st["empty"] += 1
            if need:
                c.st["failures"] += 1
                self.failure(f"{c.name} told up to date but lacks {len(need)} objects of {tip[:10]}")
            self.csv.write(f"{self.clock.now},{c.name},prefetch,200,0,8,{len(need)},{len(need)},0,{secs:.4f}\n")
            if not need:
                c.tip = tip
            return
        ts, pack, _ = packs[0]
        objs = self.index(pack)
        if objs is None:
            c.st["failures"] += 1
            return
        missing = need - objs
        c.st["packs"] += 1
        c.st["pack_bytes"] += len(pack)
        c.st["gpre_bytes"] += 32 + len(pack)
        ideal = self.oracle.ideal_bytes(c.tip, tip) if c.tip != tip else 0
        self.csv.write(f"{self.clock.now},{c.name},prefetch,200,{len(objs)},{len(pack)},{len(need)},"
                       f"{len(missing)},{ideal},{secs:.4f}\n")
        if missing:
            c.st["failures"] += 1
            self.failure(f"{c.name} response misses {len(missing)} objects of {tip[:10]}")
            return
        c.st["ideal_bytes"] += 32 + ideal
        c.tip = tip
        c.cursor = max(c.cursor, ts)

    def check_sizes(self, c, sizes):
        ids = list(sizes)
        unknown = [o for o in ids if o not in self.oracle.sizes]
        if unknown:
            p = subprocess.run(["git", f"--git-dir={self.up}", "cat-file", "--batch-check=%(objectname) %(objectsize)"],
                               input="\n".join(unknown).encode(), capture_output=True, env=clean_env())
            for line in p.stdout.decode().splitlines():
                parts = line.split()
                if len(parts) == 2 and parts[1].isdigit():
                    self.oracle.sizes[parts[0]] = int(parts[1])
        bad = [o for o in ids if self.oracle.sizes.get(o) != sizes[o]]
        if bad:
            c.st["failures"] += 1
            self.failure(f"{c.name}: {len(bad)} GSZ1 sizes differ from git's (e.g. {bad[0]})")
        c.sizes.update(sizes)

    def fetch_blobs(self, c, tip):
        """The cone's blobs the client lacks, in POSTs of 4,000 (gvfs-helper's batch)."""
        cone = self.oracle.cone(tip, c.cone)
        # The client knows every size of its cone before it fetches anything.
        unsized = [o for o in cone if c.sizes.get(o, -1) != cone[o]]
        if unsized:
            c.st["failures"] += 1
            self.failure(f"{c.name} at {tip[:10]}: {len(unsized)} cone files without their git size")
            c.sizes.update({o: cone[o] for o in unsized})
        need = sorted(o for o in cone if o not in c.blobs)
        for i in range(0, len(need), BATCH):
            ids = need[i:i + BATCH]
            t0 = time.perf_counter()
            conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3600)
            try:
                conn.request("POST", "/org/repo/gvfs/objects", body=json.dumps({"objectIds": ids, "commitDepth": 1}),
                             headers={**UA, "Content-Type": "application/json", "Accept": "application/x-git-packfile"})
                resp = conn.getresponse()
                if resp.status != 200:
                    c.st["failures"] += 1
                    self.failure(f"{c.name} blob POST status {resp.status} {resp.read()[:100]!r}")
                    continue
                got, nbytes = verify_blob_pack(resp, ids)
            except (GpreError, OSError, http.client.HTTPException) as e:
                c.st["failures"] += 1
                self.failure(f"{c.name} blob POST of {len(ids)}: {e}")
                continue
            finally:
                conn.close()
            secs = time.perf_counter() - t0
            content = sum(cone[o] for o in ids)
            if any(got[o] != cone[o] for o in ids):
                c.st["failures"] += 1
                self.failure(f"{c.name}: a blob of the wrong size")
            c.blobs.update(ids)
            c.st["blob_requests"] += 1
            c.st["blobs"] += len(ids)
            c.st["blob_bytes"] += nbytes
            c.st["blob_content_bytes"] += content
            c.st["blob_s"] += secs
            c.st["max_blob_request_s"] = max(c.st["max_blob_request_s"], secs)
            self.csv.write(f"{self.clock.now},{c.name},blobs,200,{len(ids)},{nbytes},{len(ids)},0,{content},{secs:.4f}\n")

    def index(self, pack):
        d = tempfile.mkdtemp(dir=self.scratch)
        try:
            repo = init_bare(os.path.join(d, "r.git"))
            pp = os.path.join(d, "p.pack")
            with open(pp, "wb") as f:
                f.write(pack)
            p = subprocess.run(["git", f"--git-dir={repo}", "index-pack", "-o", pp[:-5] + ".idx", pp],
                               capture_output=True, env=clean_env())
            if p.returncode != 0:
                self.failure("pack not self-contained / invalid: " + p.stderr.decode()[-200:])
                return None
            with open(pp[:-5] + ".idx", "rb") as f:
                si = subprocess.run(["git", "show-index"], stdin=f, capture_output=True, env=clean_env())
            objs = {l.split()[1] for l in si.stdout.decode().splitlines()}
            kinds = subprocess.run(["git", f"--git-dir={repo}", "cat-file", "--batch-all-objects",
                                    "--batch-check=%(objecttype)"], capture_output=True, env=clean_env()).stdout
            if b"blob" in kinds:
                self.failure("a prefetch pack holds blobs")
            return objs
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def cold_clone(self, tip):
        """CI: a cold prefetch (one pack: every commit and tree of the tip, the history's
        sizes), then the cone's blobs from scratch."""
        t0 = time.perf_counter()
        c = Client("ci-cold", 0, self.seed, ())
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3600)
        conn.request("GET", "/org/repo/gvfs/prefetch?lastPackTimestamp=0", headers={**UA, "X-GVFS-Sizes": "1"})
        r = conn.getresponse()
        body = r.read()
        conn.close()
        try:
            packs, sizes = parse_gpre_sizes(body)
        except GpreError as e:
            return self.failure(f"cold clone: {e}")
        if len(packs) != 1:
            return self.failure(f"cold clone has {len(packs)} packs")
        ts, pack, _ = packs[0]
        objects = struct.unpack(">I", pack[8:12])[0]
        expect = self.oracle.count(tip)
        h_ok = hashlib.sha1(pack[:-20]).digest() == pack[-20:]
        t_prefetch = time.perf_counter() - t0
        if objects != expect or not h_ok:
            self.failure(f"cold clone at {tip[:10]}: {objects} objects, expected {expect}, trailer ok {h_ok}")
        self.check_sizes(c, sizes or {})
        c.tip, c.cursor = tip, ts
        before = dict(c.st)
        self.fetch_blobs(c, tip)
        rec = dict(t=self.clock.now, prefetch_bytes=len(body), objects=objects, expected=expect,
                   sizes=len(sizes or {}), prefetch_s=round(t_prefetch, 2),
                   blobs=c.st["blobs"], blob_bytes=c.st["blob_bytes"], blob_s=round(c.st["blob_s"], 2),
                   ok=objects == expect and h_ok and c.st["failures"] == before["failures"])
        self.cold.append(rec)
        self.csv.write(f"{self.clock.now},ci-cold,cold,200,{objects},{len(body)},{expect},0,0,{t_prefetch:.4f}\n")

    def sync(self, c, tip):
        self.prefetch(c, tip)
        if c.tip == tip:
            self.fetch_blobs(c, tip)

    # -------------------------------------------------------------- main loop
    def run(self):
        self.oracle = Oracle(self.up, self.seed)
        self.clock.set(self.S - 60)
        srv = self.im.serve(self.up, poll_s=1, live=True, ready_timeout=3600)
        self.srv, self.port = srv, srv.port
        # What a client cloned at genesis holds: the cold pack's stamp and the history's sizes.
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3600)
        conn.request("GET", "/gvfs/prefetch?lastPackTimestamp=0", headers={**UA, "X-GVFS-Sizes": "1"})
        seed_packs, seed_sizes = parse_gpre_sizes(conn.getresponse().read())
        conn.close()
        seed_ts = seed_packs[0][0]
        names = [n for n in CONES if n not in ("ci-cold",)]
        clients = {}
        for n in names:
            cursor = 1 if n == "stale" else seed_ts
            have = () if n == "stale" else self.oracle.cone(self.seed, CONES[n])
            cl = Client(n, cursor, self.seed, have)
            if n != "stale":
                cl.sizes.update(seed_sizes or {})
            clients[n] = cl
        events = schedule(self.S, self.E)
        ticks = [self.S + 900 * k for k in range(1, (self.E - self.S) // 900 + 1)]
        all_t = sorted(set([e[0] for e in events] + ticks))
        due = {}
        for t, n in events:
            due.setdefault(t, []).append(n)
        tickset = set(ticks)
        cur = self.seed
        t_start = time.time()
        limit = self.a.limit or len(all_t)
        for i, t in enumerate(all_t[:limit]):
            self.clock.set(t)
            tip = self.tip_at(t)
            if tip != cur:
                subprocess.run(["git", "--git-dir", self.up, "update-ref", "refs/heads/main", tip],
                               check=True, env=clean_env())
                cur = tip
            if t in tickset:
                t0 = time.perf_counter()
                tier = cron_tier(t)
                try:
                    self.im.run(self.im.pipeline_cmd(self.up, tier, t))
                except RuntimeError as e:
                    self.failure(f"pipeline {tier} at {utc(t)}: {str(e)[-300:]}")
                dtm = time.perf_counter() - t0
                b = self.pipe["by_tier"].setdefault(tier, [0, 0.0, 0.0])
                b[0] += 1
                b[1] += dtm
                b[2] = max(b[2], dtm)
                self.pipe["runs"] += 1
                self.pipe["max_s"] = max(self.pipe["max_s"], dtm)
            if t in due:
                # The edge serves the upstream's tip (L0) within its poll.
                deadline = time.time() + 90
                while request(self.port, "GET", f"/org/repo/gvfs/objects/{cur}", headers=UA).status != 200:
                    if time.time() > deadline:
                        self.failure(f"tip {cur[:10]} not served within 90 s")
                        break
                    time.sleep(0.05)
                for n in due[t]:
                    if n == "ci-cold":
                        self.cold_clone(cur)
                    else:
                        self.sync(clients[n], cur)
            if not srv.alive():
                self.failure("server died: " + srv.log_tail(10))
                break
            if i % 500 == 0:
                self.L(f"progress {i}/{len(all_t)} t={utc(t)} elapsed {time.time() - t_start:.0f}s fails={len(self.fail)}")
                print(f"progress {i}/{len(all_t)} t={utc(t)} elapsed {time.time() - t_start:.0f}s "
                      f"fails={len(self.fail)}", flush=True)
        m = srv.metrics()
        srv.stop()
        self.csv.close()
        keys = ("gvfs_alloc_hot_path_total", "gvfs_blobs_from_store_total", "gvfs_blobs_proxied_total",
                "gvfs_blobs_missing_total", "gvfs_blobs_unavailable_total", "gvfs_errors_total",
                "gvfs_l0_ingests_total", "gvfs_manifest_loads_total")
        res = dict(corpus=self.a.corpus, days=round((self.E - self.S) / 86400, 2), seed=self.seeded,
                   failures=len(self.fail), failure_samples=self.fail[:20], pipeline=self.pipe,
                   clients={n: dict(cone=c.cone, **c.st) for n, c in clients.items()}, cold=self.cold,
                   edge={k: m.get(k) for k in keys}, elapsed_s=round(time.time() - t_start))
        return res


def parse_gpre_sizes(body):
    """Packs of a GPRE stream (each (ts, pack, idx)) and its GSZ1 section ({id: size}, or None)."""
    if body[:5] != b"GPRE ":
        raise GpreError("bad GPRE header")
    n = struct.unpack_from("<H", body, 6)[0]
    at = 8
    packs = []
    for _ in range(n):
        ts, plen, ilen = struct.unpack_from("<qqq", body, at)
        at += 24
        pack = body[at:at + plen]
        at += plen
        idx = None
        if ilen > 0:
            idx = body[at:at + ilen]
            at += ilen
        packs.append((ts, pack, idx))
    sizes = None
    if at < len(body):
        if body[at:at + 4] != b"GSZ1":
            raise GpreError("bytes after the packs that are not a GSZ1 section")
        cnt = struct.unpack_from("<Q", body, at + 4)[0]
        recs = body[at + 12:]
        if len(recs) != 28 * cnt:
            raise GpreError("GSZ1 length")
        sizes = {recs[i:i + 20].hex(): struct.unpack_from("<Q", recs, i + 20)[0] for i in range(0, len(recs), 28)}
    return packs, sizes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", default=os.path.expanduser("~/gvfs-bench/corpus-l"))
    ap.add_argument("--work", default=os.path.expanduser("~/gvfs-bench/replay-sep"))
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--jobs", type=int, default=16, help="mirror jobs")
    ap.add_argument("--limit", type=int, default=0, help="stop after N events (smoke runs)")
    a = ap.parse_args()
    meta = json.load(open(os.path.join(a.corpus, "corpus.json")))
    shutil.rmtree(a.work, ignore_errors=True)
    os.makedirs(a.work)
    rp = Replay(a, meta)
    rp.seed_store()
    res = rp.run()
    print(json.dumps({k: v for k, v in res.items() if k != "clients"}, indent=1, default=str))
    for n, c in res["clients"].items():
        print(f"{n:13s} req={c['requests']:6d} fail={c['failures']} packs={c['packs']:5d} "
              f"prefetch={c['gpre_bytes'] / 1e6:8.1f}MB ideal={c['ideal_bytes'] / 1e6:8.1f}MB "
              f"blobs={c['blobs']:6d} blob_bytes={c['blob_bytes'] / 1e9:7.2f}GB content={c['blob_content_bytes'] / 1e9:7.2f}GB")
    os.makedirs(os.path.join(ROOT, "bench/results"), exist_ok=True)
    json.dump(res, open(os.path.join(ROOT, "bench/results/replay-separate.json"), "w"), indent=1, default=str)


if __name__ == "__main__":
    main()
