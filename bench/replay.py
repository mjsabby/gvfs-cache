"""Black-box SearchGold replay: 7 real days of first-parent pushes, the pipeline on its cron,
a live edge, and nine client behaviours (agents, CI, humans).  Judged by stock git.

Per response:  strict GPRE, <= 1 pack, pack self-contained (`git index-pack` in an empty repo),
and complete: every object of `rev-list --objects <tip> ^<client's previous tip>` is in the
response or in what the client received before.  An empty response while the client lacks
objects of the server's tip is a failure ("told up to date while behind").
Ideal per warm response = git's own non-thin pack for tip ^ previous tip (cached).
Cold clones: one pack whose object count must equal rev-list --objects <tip>; streamed + SHA-1.
"""
import concurrent.futures as cf
import datetime as dt
import hashlib
import http.client
import json
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import threading
import time
import traceback

import impls
from gvfslib import clean_env, git, git_out, init_bare, request, GpreError, parse_gpre

R = "/home/muks/gvfs-bench/replay"                 # history + ideal-bytes cache (shared)
W = os.environ.get("REPLAY_WORK", R)              # per-implementation stores and results
SGOBJ = "/home/muks/SearchGold/.git/objects"
UA = {"User-Agent": "git/2.53.0.vfs.0.0"}   # what gvfs-helper sends
SOL_CAP = ["--max-objects", "16777216", "--max-links", "2147483647", "--pack-mib", "65536",
           "--object-mib", "128", "--delta-cache-mib", "512"]
SOL_SERVE = SOL_CAP + ["--arenas", "8"]
SCHED = {"opus": "cron", "fable": "cron", "sol": "auto", "astra": "auto"}
FROZEN = os.environ.get("FROZEN", "/home/muks/gvfs-bench/frozen")
# `<impl>@<variant>`: the same binary with extra pipeline flags, in its own work dir.
VARIANTS = {"fable@h24": ["--leaf-tier", "h24"], "opus@ss": []}   # opus@ss: run with FROZEN=<dir of a superset build>
# Keep every warm response (pack, git's idx, interval headers) and the seed clone under
# <work>/rx for client-side lifecycle studies (bench/workstation.py).
KEEP_RX = os.environ.get("REPLAY_KEEP_RX") == "1"


def base(name):
    return name.split("@")[0]


def binary(name):
    p = os.path.join(FROZEN, f"{base(name)}-gvfs-daemon")
    return p if os.path.exists(p) else None


def extra_pipeline(name):
    return (SOL_CAP if base(name) == "sol" else []) + VARIANTS.get(name, [])

H = json.load(open(os.path.join(R, "history-w7.json")))
S, E, SEED = H["start"], H["end"], H["seed"]
COMMITS = H["commits"]           # [oid, effective, original]
POS = {c[0]: i for i, c in enumerate(COMMITS)}


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


def tip_at(t):
    best = SEED
    for oid, eff, _ in COMMITS:
        if eff <= t:
            best = oid
        else:
            break
    return best


# ------------------------------------------------------------------ shared oracle (cached)
class Oracle:
    """Exact object-set arithmetic.  `git rev-list --objects A ^B` is NOT a set difference: it
    only excludes what B's boundary trees reach, so it lists old objects that were deleted and
    re-added.  With S = objects(SEED) loaded exactly and SEED an ancestor of every tip:
        objects(t) = S | R(t ^SEED)          NEW(t) = R(t ^SEED) - S
        need(prev -> t) = NEW(t) - NEW(prev)  count(t) = |S| + |NEW(t)|"""

    def __init__(self, up):
        self.up = up
        self.lock = threading.Lock()
        self.ideal = {}
        self.inflight = {}
        self.new = {}           # tip -> frozenset (small LRU)
        self.order = []
        cache = os.path.join(R, "oracle.json")
        if os.path.exists(cache):
            d = json.load(open(cache))
            self.ideal = {tuple(k.split(":")): v for k, v in d.get("ideal", {}).items()}
        t0 = time.time()
        out = subprocess.run(["git", f"--git-dir={up}", "rev-list", "--objects", SEED],
                             capture_output=True, env=clean_env()).stdout
        self.seed = frozenset(l[:40] for l in out.decode().splitlines() if l)
        print(f"oracle: |objects(SEED)| = {len(self.seed)} in {time.time() - t0:.0f}s", flush=True)

    def save(self):
        """Persist the ideal-bytes cache (best effort; called from every replay thread)."""
        with self.lock:
            d = {"ideal": {f"{a}:{b}": v for (a, b), v in self.ideal.items()}}
            tmp = os.path.join(R, f"oracle.json.{os.getpid()}.{threading.get_ident()}.tmp")
            try:
                with open(tmp, "w") as f:
                    json.dump(d, f)
                os.replace(tmp, os.path.join(R, "oracle.json"))
            except OSError:
                pass

    def _once(self, key, fn, store):
        ikey = (id(store), key)
        with self.lock:
            if key in store:
                return store[key]
            ev = self.inflight.get(ikey)
            if ev is None:
                ev = self.inflight[ikey] = threading.Event()
                mine = True
            else:
                mine = False
        if not mine:
            ev.wait()
            with self.lock:
                return store[key]
        v = fn()
        with self.lock:
            store[key] = v
            if store is self.new:
                self.order.append(key)
                while len(self.order) > 64:
                    old = self.order.pop(0)
                    self.new.pop(old, None)
            self.inflight.pop(ikey, None)
            ev.set()
        return v

    def new_since_seed(self, tip):
        if tip == SEED:
            return frozenset()

        def f():
            # Exact and cheap: objects(t) = objects(a) | R(t ^a) for any ancestor a, so extend the
            # newest cached first-parent ancestor instead of walking back to SEED every time.
            pos = POS.get(tip)
            base, base_set = SEED, frozenset()
            if pos is not None:
                with self.lock:
                    cands = [(POS[k], k) for k in self.new if k in POS and POS[k] < pos]
                if cands:
                    _, base = max(cands)
                    with self.lock:
                        base_set = self.new.get(base)
                    if base_set is None:
                        base, base_set = SEED, frozenset()
            out = git_out(self.up, "rev-list", "--objects", tip, f"^{base}")
            fresh = (o for o in (l[:40] for l in out.splitlines() if l) if o not in self.seed)
            return base_set | frozenset(fresh)
        return self._once(tip, f, self.new)

    def new_objects(self, have, want):
        return self.new_since_seed(want) - self.new_since_seed(have)

    def ideal_bytes(self, have, want):
        def f():
            p = subprocess.run(["git", f"--git-dir={self.up}", "pack-objects", "--revs", "--stdout", "-q"],
                               input=f"{want}\n^{have}\n".encode(), capture_output=True, env=clean_env())
            return len(p.stdout)
        return self._once((have, want), f, self.ideal)

    def count(self, tip):
        return len(self.seed) + len(self.new_since_seed(tip))


# ------------------------------------------------------------------ clients
def schedule():
    ev = []   # (time, client)
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
    ev.append((S + 9 * 3600, "dev-vacation"))         # Tue 09:00, then back next Mon 09:00
    ev.append((E - 15 * 3600, "dev-vacation"))
    ev.append((E - 60, "stale"))
    return sorted(e for e in ev if S < e[0] < E)


class Client:
    def __init__(self, name, cursor, tip):
        self.name = name
        self.cursor = cursor
        self.tip = tip
        self.got = set()
        self.st = dict(requests=0, packs=0, pack_bytes=0, idx_bytes=0, gpre_bytes=0, ideal_bytes=0,
                       empty=0, slow=0, max_s=0.0, sum_s=0.0, failures=0, not_advancing=0,
                       resent_objects=0)


class Replay:
    def __init__(self, name, oracle):
        self.name = name
        self.work = os.path.join(W, name)
        self.oracle = oracle
        self.fail = []
        self.log = open(os.path.join(self.work, "replay.log"), "a")
        self.clock = impls.Clock(os.path.join(self.work, "faketime"), S - 300)
        extra_s = SOL_SERVE if base(name) == "sol" else (["--workers", "64"] if base(name) == "astra" else [])
        self.im = impls.ALL[base(name)](self.work, clock=self.clock, extra_pipeline=extra_pipeline(name),
                                        extra_serve=extra_s, binary=binary(name))
        self.rx = os.path.join(self.work, "rx")
        self.rx_seq = 0
        if KEEP_RX:
            shutil.rmtree(self.rx, ignore_errors=True)
            os.makedirs(self.rx)
            self.rx_log = open(os.path.join(self.rx, "rx.jsonl"), "w")
        self.up = os.path.join(self.work, "up.git")
        self.scratch = os.path.join(self.work, "scratch")
        os.makedirs(self.scratch, exist_ok=True)
        self.pipe = {"runs": 0, "by_tier": {}, "max_s": 0.0}
        self.csv = open(os.path.join(self.work, "responses.csv"), "w")
        self.csv.write("t,client,cursor,status,packs,ts,pack_bytes,idx_bytes,objects,need,missing,"
                       "already_had,secs\n")
        self.live = []
        self.cold = []

    def L(self, *a):
        print(f"[{self.name}] " + " ".join(str(x) for x in a), file=self.log, flush=True)

    def failure(self, what):
        self.fail.append(what)
        self.L("FAIL", what)

    def pipeline(self, t):
        tier = cron_tier(t)
        if SCHED[base(self.name)] == "auto":
            cmds = [self.im.pipeline_cmd(self.up, "auto", t)]
        else:
            cmds = [self.im.pipeline_cmd(self.up, tier, t)]
        t0 = time.perf_counter()
        for c in cmds:
            self.im.run(c, env=self.im.pipeline_env(t))
        dtm = time.perf_counter() - t0
        self.pipe["runs"] += 1
        b = self.pipe["by_tier"].setdefault(tier, [0, 0.0, 0.0])
        b[0] += 1
        b[1] += dtm
        b[2] = max(b[2], dtm)
        self.pipe["max_s"] = max(self.pipe["max_s"], dtm)

    def generation(self):
        m = self.srv.metrics()
        return m.get("gvfs_manifest_generation")

    def wait_reload(self, gen_before, tier):
        if gen_before is None:
            time.sleep(1.3 if tier in ("15m", "1h") else 5.0)
            return
        limit = 1.6 if tier in ("15m", "1h", "6h", "12h") else 30.0
        t0 = time.time()
        while time.time() - t0 < limit:
            g = self.generation()
            if g is not None and g != gen_before:
                return
            time.sleep(0.05)

    def wait_live(self, tip):
        t0 = time.time()
        while time.time() - t0 < 90:
            r = request(self.srv.port, "GET", f"/org/repo/gvfs/objects/{tip}", headers=UA)
            if r.status == 200:
                self.live.append(time.time() - t0)
                return True
            time.sleep(0.05)
        self.failure(f"live tip {tip[:10]} not served within 90 s")
        return False

    # -------------------------------------------------------------- requests
    def warm(self, c, tip):
        c.st["requests"] += 1
        t0 = time.perf_counter()
        r = request(self.srv.port, "GET", f"/org/repo/gvfs/prefetch?lastPackTimestamp={c.cursor}", headers=UA)
        secs = time.perf_counter() - t0
        c.st["sum_s"] += secs
        c.st["max_s"] = max(c.st["max_s"], secs)
        if secs > 1.0:
            c.st["slow"] += 1
        if r.status != 200:
            c.st["failures"] += 1
            return self.failure(f"{c.name} status {r.status} {r.body[:100]!r}")
        try:
            packs = parse_gpre(r.body)
        except GpreError as e:
            c.st["failures"] += 1
            return self.failure(f"{c.name} GPRE {e}")
        if len(packs) > 1:
            c.st["failures"] += 1
            self.failure(f"{c.name} {len(packs)} packs")
        need = self.oracle.new_objects(c.tip, tip) if c.tip != tip else frozenset()
        if not packs:
            self.csv.write(f"{self.clock.now},{c.name},{c.cursor},200,0,,0,0,0,{len(need)},"
                           f"{len(need - c.got)},0,{secs:.6f}\n")
            c.st["empty"] += 1
            c.st["gpre_bytes"] += 8
            if need - c.got:
                c.st["failures"] += 1
                self.failure(f"{c.name} told up to date at cursor {c.cursor} but lacks "
                             f"{len(need - c.got)} objects of {tip[:10]}")
            else:
                c.tip = tip
            return
        ts, pack, idx = packs[0]
        if ts <= c.cursor:
            c.st["not_advancing"] += 1
        c.st["packs"] += 1
        c.st["pack_bytes"] += len(pack)
        c.st["idx_bytes"] += len(idx) if idx else 0
        c.st["gpre_bytes"] += 32 + len(pack) + (len(idx) if idx else 0)
        keep = None
        if KEEP_RX and len(pack) < (1 << 30):
            self.rx_seq += 1
            keep = os.path.join(self.rx, c.name, f"{self.rx_seq:06d}")
            os.makedirs(os.path.dirname(keep), exist_ok=True)
        objs = self.index(pack, idx, keep)
        if objs is None:
            c.st["failures"] += 1
            return
        if keep:
            hs = {k: r.headers.get(f"x-gvfs-pack-{k}") for k in ("start", "end")}
            self.rx_log.write(json.dumps(dict(
                t=self.clock.now, client=c.name, cursor=c.cursor, ts=ts,
                start=int(hs["start"]) if hs["start"] else None,
                end=int(hs["end"]) if hs["end"] else None,
                pack=len(pack), idx=len(idx) if idx else 0, objects=len(objs), tip=tip,
                stem=os.path.relpath(keep, self.rx))) + "\n")
            self.rx_log.flush()
        c.st["resent_objects"] += len(objs & c.got)
        missing = need - c.got - objs
        self.csv.write(f"{self.clock.now},{c.name},{c.cursor},200,1,{ts},{len(pack)},"
                       f"{len(idx) if idx else 0},{len(objs)},{len(need)},{len(missing)},"
                       f"{len(objs & c.got) + len(objs & self.oracle.seed)},{secs:.6f}\n")
        if missing:
            c.st["failures"] += 1
            self.failure(f"{c.name} response at cursor {c.cursor} misses {len(missing)} objects of "
                         f"{tip[:10]} (e.g. {sorted(missing)[:2]})")
        else:
            if c.tip != tip:
                c.st["ideal_bytes"] += 32 + self.oracle.ideal_bytes(c.tip, tip)
            c.tip = tip
        c.got |= objs
        c.cursor = max(c.cursor, ts)

    def index(self, pack, idx, keep=None):
        """The pack's object ids (None if git rejects it); `keep`: move pack + git's idx there."""
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
            if idx is not None and open(pp[:-5] + ".idx", "rb").read() != idx:
                self.failure("served .idx differs from git index-pack's")
            with open(pp[:-5] + ".idx", "rb") as f:
                si = subprocess.run(["git", "show-index"], stdin=f, capture_output=True, env=clean_env())
            if keep:
                os.replace(pp, keep + ".pack")
                os.replace(pp[:-5] + ".idx", keep + ".idx")
            return {l.split()[1] for l in si.stdout.decode().splitlines()}
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def cold_clone(self, tip):
        t0 = time.perf_counter()
        c = http.client.HTTPConnection("127.0.0.1", self.srv.port, timeout=1800)
        c.request("GET", "/org/repo/gvfs/prefetch?lastPackTimestamp=0", headers=UA)
        r = c.getresponse()
        rec = {"status": r.status}
        if r.status != 200:
            self.failure(f"cold clone status {r.status}")
            return
        head = r.read(8)
        n = struct.unpack_from("<H", head, 6)[0]
        rec["packs"] = n
        if n != 1:
            self.failure(f"cold clone has {n} packs")
            return
        ts, plen, ilen = struct.unpack("<qqq", r.read(24))
        h = hashlib.sha1()
        got = 0
        first = None
        last20 = b""
        while got < plen:
            chunk = r.read(min(16 << 20, plen - got))
            if not chunk:
                break
            if first is None:
                first = chunk[:12]
            body_end = plen - 20
            if got < body_end:
                h.update(chunk[:max(0, body_end - got)])
            last20 = (last20 + chunk)[-20:]
            got += len(chunk)
        if ilen > 0:
            left = ilen
            while left > 0:
                ch = r.read(min(16 << 20, left))
                if not ch:
                    break
                left -= len(ch)
        rest = r.read()
        secs = time.perf_counter() - t0
        c.close()
        objects = struct.unpack(">I", first[8:12])[0] if first else 0
        expect = self.oracle.count(tip)
        ok = (got == plen and h.digest() == last20 and objects == expect and not rest)
        if not ok:
            self.failure(f"cold clone at {tip[:10]}: got {got}/{plen} sha_ok={h.digest() == last20} "
                         f"objects {objects} expected {expect} trailing {len(rest)}")
        self.cold.append(dict(t=self.clock.now, bytes=32 + plen + max(ilen, 0), pack=plen, idx=ilen,
                              objects=objects, expected=expect, secs=round(secs, 3), ok=ok))

    # -------------------------------------------------------------- main loop
    def run(self):
        os.makedirs(self.work, exist_ok=True)
        if not os.path.exists(os.path.join(self.work, "store")):
            raise RuntimeError("missing seed store; run with --seed first")
        git(self.up, "update-ref", "refs/heads/main", SEED)
        self.clock.set(S - 60)
        self.srv = self.im.serve(self.up, poll_s=1, live=True, ready_timeout=1800)
        self.L("ready", self.srv.ready_secs)
        # every warm client cloned at the seed: cursor = the seed's cold timestamp
        c = http.client.HTTPConnection("127.0.0.1", self.srv.port, timeout=600)
        c.request("GET", "/gvfs/prefetch?lastPackTimestamp=0", headers=UA)
        rr = c.getresponse()
        head = rr.read(32)
        seed_ts = struct.unpack_from("<q", head, 8)[0]
        if KEEP_RX:
            # The clone every warm client starts from (its .idx is built offline by git).
            plen = struct.unpack_from("<q", head, 16)[0]
            with open(os.path.join(self.rx, "seed.pack"), "wb") as f:
                left = plen
                while left > 0:
                    chunk = rr.read(min(16 << 20, left))
                    if not chunk:
                        raise RuntimeError("truncated seed clone")
                    f.write(chunk)
                    left -= len(chunk)
            with open(os.path.join(self.rx, "seed.json"), "w") as f:
                json.dump(dict(ts=seed_ts, tip=SEED, pack=plen), f)
        c.close()
        names = ["agent-1m", "agent-15m", "agent-1h", "dev-hourly", "dev-3x", "dev-morning", "dev-vacation"]
        clients = {n: Client(n, seed_ts, SEED) for n in names}
        clients["stale"] = Client("stale", 1, SEED)
        events = schedule()
        ticks = [S + 900 * k for k in range(1, (E - S) // 900 + 1)]
        all_t = sorted(set([e[0] for e in events] + ticks))
        due = {}
        for t, n in events:
            due.setdefault(t, []).append(n)
        tickset = set(ticks)
        cur = SEED
        caught = SEED
        t_start = time.time()
        limit = int(os.environ.get("REPLAY_LIMIT", "0")) or len(all_t)
        for i, t in enumerate(all_t[:limit]):
            self.clock.set(t)
            tip = tip_at(t)
            if tip != cur:
                git(self.up, "update-ref", "refs/heads/main", tip)
                cur = tip
            if t in tickset:
                g = self.generation()
                self.pipeline(t)
                self.wait_reload(g, cron_tier(t))
            if t in due:
                if caught != cur:
                    if self.wait_live(cur):
                        caught = cur
                for n in due[t]:
                    if n == "ci-cold":
                        self.cold_clone(cur)
                    else:
                        self.warm(clients[n], cur)
            if not self.srv.alive():
                self.failure("server died: " + self.srv.log_tail(10))
                break
            if i % 500 == 0:
                self.L(f"progress {i}/{len(all_t)} t={utc(t)} elapsed {time.time() - t_start:.0f}s "
                       f"fails={len(self.fail)}")
                self.oracle.save()
        m = self.srv.metrics()
        self.srv.stop()
        self.csv.close()
        res = dict(impl=self.name, failures=len(self.fail), failure_samples=self.fail[:20],
                   pipeline=self.pipe, live_wait_s=dict(n=len(self.live),
                   p50=sorted(self.live)[len(self.live) // 2] if self.live else None,
                   max=max(self.live) if self.live else None),
                   clients={n: c.st for n, c in clients.items()}, cold=self.cold,
                   metrics={k: v for k, v in m.items() if "alloc" in k or "error" in k},
                   elapsed_s=time.time() - t_start)
        with open(os.path.join(self.work, "result.json"), "w") as f:
            json.dump(res, f, indent=1, default=str)
        self.oracle.save()
        return res


def seed(name):
    work = os.path.join(W, name)
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    up = init_bare(os.path.join(work, "up.git"))
    with open(os.path.join(up, "objects", "info", "alternates"), "w") as f:
        f.write(SGOBJ + "\n")
    git(up, "update-ref", "refs/heads/main", SEED)
    clock = impls.Clock(os.path.join(work, "faketime"), S - 300)
    im = impls.ALL[base(name)](work, clock=clock, extra_pipeline=extra_pipeline(name), binary=binary(name))
    t0 = time.time()
    im.run(im.pipeline_cmd(up, "15m", S - 300), env=im.pipeline_env(S - 300))
    print(f"seed {name}: {time.time() - t0:.1f}s", flush=True)


def main():
    args = sys.argv[1:]
    if args and args[0] == "--seed":
        for n in args[1:]:
            seed(n)
        return
    names = args or ["opus", "fable", "sol", "astra"]
    oracle = Oracle(os.path.join(W, names[0], "up.git"))
    oracle.count(SEED)
    with cf.ThreadPoolExecutor(len(names)) as ex:
        def go(n):
            rp = Replay(n, oracle)
            try:
                return rp.run()
            finally:
                if getattr(rp, "srv", None):
                    rp.srv.stop()
        futs = {ex.submit(go, n): n for n in names}
        for f in cf.as_completed(futs):
            n = futs[f]
            try:
                r = f.result()
                print(f"== {n}: failures={r['failures']} elapsed={r['elapsed_s']:.0f}s", flush=True)
            except Exception as e:
                print(f"== {n}: EXCEPTION {e}\n{traceback.format_exc()}", flush=True)


if __name__ == "__main__":
    main()
