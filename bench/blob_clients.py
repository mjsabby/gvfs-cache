"""Client side of `--blobs separate`: what real clients accumulate from blob packs.

Stock microsoft/git clients (gvfs-helper, Scalar's GVFS settings and recommended config)
follow a separate-blob edge for two weeks of a synthetic repository (bench/corpus_gen.py):
the pipeline seals every 15 minutes (rollups on their cron), and each client syncs on its
schedule: `gvfs-helper prefetch` (commits and trees), then `checkout --detach <tip>` of its
sparse cone, which faults the missing blobs in (POST batches of up to 4,000 ids, each a
`vfs-<sha>.pack`). The developer also runs history commands that fault old versions (`log
-p`, and `show <old>:<large file>`, a single GET that gvfs-helper writes as a loose object).

Patterns: agent-15m (text-heavy cone, every 15 min), agent-1h (binary-heavy cone, hourly),
dev (mixed cone, 09:00/13:00/17:30 on weekdays, plus history commands), full (no sparse
checkout, daily). Each runs under four maintenance regimes, once a day at 00:00 UTC:

  stock      none
  scalar     Scalar's daily tasks (`maintenance.strategy=incremental`): incremental-repack
             (multi-pack-index write, expire, repack --batch-size=<2nd largest pack + 1, <= 2g>)
             and loose-objects
  geometric  `git maintenance run --task=geometric-repack` (repack --geometric=2)
  vfs        VFS for Git's steps: multi-pack-index write, expire, repack --batch-size=2g, and
             loose objects packed

Measured per client and day: packs and loose objects on disk, object bytes, bytes the
maintenance wrote (new or rewritten files), its time; blob bytes fetched. Checked at the
end: every blob of the cone at the tip present, the work tree equal to the tip's tree.

    python3 bench/blob_clients.py [--corpus ~/gvfs-bench/corpus-s] [--days 14]
"""
import argparse
import concurrent.futures as cf
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(__file__))
from gvfslib import clean_env, free_port, metrics, request, wait_http  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "opus/target/release/gvfs-daemon")
MSGIT = os.environ.get("GVFS_TEST_MSGIT", os.path.expanduser("~/git/bin-wrappers/git"))
SCALAR = os.path.join(os.path.dirname(MSGIT), "scalar")
# core.gvfs as `scalar clone` sets it: block commands, missing OK, skip reachability,
# prefetch during fetch.
SCALAR_GVFS_MODE = 2 | 4 | 16 | 128
REGIMES = ["stock", "scalar", "geometric", "vfs"]
PATTERNS = {
    "agent-15m": {"cone": ["src", "lib"]},
    "agent-1h": {"cone": ["models"]},
    "dev": {"cone": ["services", "bin", "tests"]},
    "full": {"cone": None},
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


def run(cmd, cwd=None, check=True, env=None):
    p = subprocess.run(cmd, cwd=cwd, capture_output=True, env=clean_env(env))
    if check and p.returncode != 0:
        raise RuntimeError(f"{' '.join(map(str, cmd[:6]))} failed ({p.returncode}): "
                           f"{p.stderr.decode(errors='replace')[-1500:]}")
    return p


def due(pattern, t):
    d = utc(t)
    if pattern == "agent-15m":
        return True
    if pattern == "agent-1h":
        return d.minute == 0
    if pattern == "dev":
        return d.weekday() < 5 and (d.hour, d.minute) in ((9, 0), (13, 0), (17, 30))
    if pattern == "full":
        return (d.hour, d.minute) == (9, 0)
    return False


def snapshot(objdir):
    """{relative path: (inode, size, mtime)} of every file under objects/."""
    out = {}
    for dp, _, fs in os.walk(objdir):
        for f in fs:
            p = os.path.join(dp, f)
            try:
                st = os.stat(p)
            except FileNotFoundError:
                continue
            out[os.path.relpath(p, objdir)] = (st.st_ino, st.st_size, st.st_mtime_ns)
    return out


def counts(objdir):
    packs = [f for f in os.listdir(os.path.join(objdir, "pack")) if f.endswith(".pack")]
    pack_bytes = sum(os.path.getsize(os.path.join(objdir, "pack", f)) for f in packs)
    loose, loose_bytes = 0, 0
    for d in os.listdir(objdir):
        if len(d) == 2 and os.path.isdir(os.path.join(objdir, d)):
            for f in os.listdir(os.path.join(objdir, d)):
                loose += 1
                loose_bytes += os.path.getsize(os.path.join(objdir, d, f))
    return {"packs": len(packs), "vfs_packs": sum(f.startswith("vfs-") for f in packs),
            "prefetch_packs": sum(f.startswith("prefetch-") for f in packs),
            "pack_bytes": pack_bytes, "loose": loose, "loose_bytes": loose_bytes}


def largest(up, rev, cone):
    out = run(["git", "--git-dir", up, "ls-tree", "-r", "-l", rev, "--", *cone]).stdout.decode()
    best = None
    for line in out.splitlines():
        meta, path = line.split("\t", 1)
        size = meta.split()[3]
        if size != "-" and (best is None or int(size) > best[0]):
            best = (int(size), path)
    return best[1] if best else None


class Client:
    def __init__(self, root, pattern, regime, port, corpus):
        # A private global config: `scalar register` records the enlistment there.
        self.env = {"GIT_CONFIG_GLOBAL": os.path.join(root, "global.gitconfig")}
        if not os.path.exists(self.env["GIT_CONFIG_GLOBAL"]):
            open(self.env["GIT_CONFIG_GLOBAL"], "a").close()
        self.name = f"{pattern}/{regime}"
        self.pattern, self.regime = pattern, regime
        self.dir = os.path.join(root, "clients", pattern, regime)
        self.objdir = os.path.join(self.dir, ".git", "objects")
        self.cone = PATTERNS[pattern]["cone"]
        self.corpus = corpus
        self.lock = threading.Lock()
        self.tip = None
        self.st = {"maint_fetched_bytes": 0, "syncs": 0, "sync_s": 0.0, "max_sync_s": 0.0, "blob_pack_bytes": 0, "blob_packs_created": 0,
                   "loose_created": 0, "loose_bytes_created": 0, "prefetch_bytes": 0, "maint_runs": 0,
                   "maint_written": 0, "maint_s": 0.0, "max_maint_s": 0.0, "history_ops": 0, "errors": []}
        self.days = []
        shutil.rmtree(self.dir, ignore_errors=True)
        os.makedirs(os.path.dirname(self.dir), exist_ok=True)
        run([MSGIT, "init", "-q", self.dir], env=self.env)
        self.g("config", "remote.origin.url", f"http://127.0.0.1:{port}/org/repo")
        # What `scalar clone` sets for a GVFS-protocol server (no shared cache: objects stay
        # in .git/objects), and Scalar's recommended config (gc.auto=0, ...).
        for k, v in [("core.useGVFSHelper", "true"), ("core.gvfs", str(SCALAR_GVFS_MODE)),
                     ("http.version", "HTTP/1.1"), ("maintenance.auto", "false")]:
            self.g("config", k, v)
        run([SCALAR, "register", "--no-maintenance", self.dir], env=self.env)
        r = run([MSGIT, "-C", self.dir, "config", "gc.auto"], check=False, env=self.env)
        if r.stdout.strip() != b"0":
            self.g("config", "gc.auto", "0")
        if self.cone:
            self.g("sparse-checkout", "set", "--cone", *self.cone)

    def g(self, *args, check=True):
        return run([MSGIT, "-C", self.dir, *args], check=check, env=self.env)

    def sync(self, tip):
        t0 = time.perf_counter()
        before = snapshot(self.objdir)
        self.g("gvfs-helper", "--cache-server=disable", "prefetch")
        self.g("-c", "core.useGVFSHelper=true", "checkout", "-q", "-f", "--detach", tip)
        secs = time.perf_counter() - t0
        self.account(before, "sync")
        self.tip = tip
        self.st["syncs"] += 1
        self.st["sync_s"] += secs
        self.st["max_sync_s"] = max(self.st["max_sync_s"], secs)

    def history(self, up, old):
        """A developer reading history: diffs of recent commits in the cone, then the cone's
        largest file as it was days ago (a single object: a GET, which gvfs-helper writes as
        a loose object). The largest is looked up in the upstream: `ls-tree -l` here would
        fault in every blob of the cone for its size."""
        before = snapshot(self.objdir)
        cone = self.cone or ["."]
        self.g("-c", "core.useGVFSHelper=true", "log", "-p", "-n", "3", "--format=%H", "--", *cone)
        big = largest(up, old, cone)
        if big:
            self.g("-c", "core.useGVFSHelper=true", "cat-file", "-p", f"{old}:{big}")
        self.account(before, "history")
        self.st["history_ops"] += 1

    def account(self, before, what):
        after = snapshot(self.objdir)
        for rel, (ino, size, _) in after.items():
            if rel in before and before[rel][0] == ino:
                continue
            name = os.path.basename(rel)
            if rel.startswith("pack/vfs-") and name.endswith(".pack"):
                self.st["blob_pack_bytes"] += size
                self.st["blob_packs_created"] += 1
            elif rel.startswith("pack/prefetch-") and name.endswith(".pack"):
                self.st["prefetch_bytes"] += size
            elif len(rel) == 41 and rel[2] == "/":
                self.st["loose_created"] += 1
                self.st["loose_bytes_created"] += size

    def maintain(self, day):
        pre = counts(self.objdir)
        before = snapshot(self.objdir)
        t0 = time.perf_counter()
        if self.regime == "scalar":
            self.g("maintenance", "run", "--task=incremental-repack", "--task=loose-objects", "--quiet")
        elif self.regime == "geometric":
            self.g("maintenance", "run", "--task=geometric-repack", "--quiet")
        elif self.regime == "vfs":
            self.g("multi-pack-index", "write", "--no-progress")
            self.g("multi-pack-index", "expire", "--no-progress")
            self.g("multi-pack-index", "repack", "--no-progress", "--batch-size=2g")
            self.g("maintenance", "run", "--task=loose-objects", "--quiet")
        secs = time.perf_counter() - t0
        after = snapshot(self.objdir)
        written = sum(size for rel, (ino, size, _) in after.items()
                      if rel not in before or before[rel][0] != ino)
        # Objects maintenance itself had gvfs-helper download (new loose objects).
        fetched = sum(size for rel, (ino, size, _) in after.items()
                      if rel not in before and len(rel) == 41 and rel[2] == "/")
        self.st["maint_fetched_bytes"] += fetched
        post = counts(self.objdir)
        if self.regime != "stock":
            self.st["maint_runs"] += 1
            self.st["maint_written"] += written
            self.st["maint_s"] += secs
            self.st["max_maint_s"] = max(self.st["max_maint_s"], secs)
        self.days.append({"day": day, "before": pre, "after": post, "maint_written": written,
                          "maint_fetched": fetched,
                          "maint_s": round(secs, 2), "fetched_blob_bytes": self.st["blob_pack_bytes"]
                          + self.st["loose_bytes_created"]})

    def verify(self):
        """Every blob of the cone at the client's tip is present; the work tree is its tree."""
        tip = self.tip
        out = self.g("ls-tree", "-r", tip, "--", *(self.cone or ["."])).stdout.decode().splitlines()
        want = [l.split()[2] for l in out if l.split()[1] == "blob"]
        p = subprocess.run([MSGIT, "-C", self.dir, "-c", "core.useGVFSHelper=false", "cat-file",
                            "--batch-check=%(objectname)"], input="\n".join(want).encode(),
                           capture_output=True, env=clean_env(self.env))
        missing = [l for l in p.stdout.decode().splitlines() if l.endswith("missing")]
        status = self.g("status", "--porcelain").stdout.decode().strip()
        head = self.g("rev-parse", "HEAD").stdout.decode().strip()
        files = sum(1 for _ in out)
        ok = not missing and not status and head == tip
        if not ok:
            self.st["errors"].append(f"verify: head {head[:10]} tip {tip[:10]}, {len(missing)} cone blobs "
                                     f"missing, status {status[:200]!r}")
        return {"ok": ok, "cone_files": files, "cone_blobs_missing": len(missing)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", default=os.path.expanduser("~/gvfs-bench/corpus-s"))
    ap.add_argument("--work", default=os.path.expanduser("~/gvfs-bench/blob-clients"))
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--jobs", type=int, default=16)
    a = ap.parse_args()
    meta = json.load(open(os.path.join(a.corpus, "corpus.json")))
    commits = meta["commits"]
    g_idx = meta["genesis_index"]
    W = a.work
    shutil.rmtree(W, ignore_errors=True)
    os.makedirs(W)
    # The upstream the pipeline follows: the corpus's objects, its own refs.
    up = os.path.join(W, "up.git")
    run(["git", "init", "-q", "--bare", "-b", "main", up])
    with open(os.path.join(up, "objects", "info", "alternates"), "w") as f:
        f.write(os.path.join(a.corpus, "up.git", "objects") + "\n")
    run(["git", "--git-dir", up, "update-ref", "refs/heads/main", meta["genesis"]])
    store = os.path.join(W, "store")
    t_gen = commits[g_idx][1] + 60

    def pipeline(t, tier, extra=()):
        return run([BIN, "pipeline", "--store", f"fs:{store}", "--upstream", up, "--tier", tier,
                    "--now", str(t), "--work-dir", os.path.join(W, "pw"), "--threads", "8", *extra])

    t0 = time.time()
    pipeline(t_gen, "15m", ["--blobs", "separate"])
    run([BIN, "mirror", "--store", f"fs:{store}", "--upstream", up, "--jobs", "8",
         "--work-dir", os.path.join(W, "mw")])
    print(f"store: genesis + mirror in {time.time() - t0:.0f}s", flush=True)
    port = free_port()
    edge_log = open(os.path.join(W, "edge.log"), "wb")
    edge = subprocess.Popen([BIN, "serve", "--store", f"fs:{store}", "--bind", f"127.0.0.1:{port}",
                             "--cache-dir", os.path.join(W, "edge-cache"), "--poll-ms", "100",
                             "--alloc-guard", "strict", "--workers", "64"],
                            stderr=edge_log, stdout=subprocess.DEVNULL, env=clean_env())
    wait_http(port, "/healthz", timeout=600, proc=edge)

    clients = [Client(W, p, r, port, meta) for p in PATTERNS for r in REGIMES]
    pool = cf.ThreadPoolExecutor(a.jobs)

    def each(fn, cs):
        errs = []
        for c, f in [(c, pool.submit(fn, c)) for c in cs]:
            try:
                f.result()
            except Exception as e:  # noqa: BLE001
                c.st["errors"].append(str(e)[:400])
                errs.append(f"{c.name}: {e}")
        return errs

    start = commits[g_idx][1] - commits[g_idx][1] % 900 + 900
    end = start + a.days * 86400
    tip, k = meta["genesis"], g_idx
    cur_day = utc(start).date()
    first = True
    t_run = time.time()
    failures = []
    for t in range(start, end, 900):
        while k + 1 < len(commits) and commits[k + 1][1] <= t:
            k += 1
        if commits[k][0] != tip:
            tip = commits[k][0]
            run(["git", "--git-dir", up, "update-ref", "refs/heads/main", tip])
        pipeline(t, cron_tier(t))
        # The edge serves the sealed tip once it has loaded the manifest naming it.
        deadline = time.time() + 30
        while request(port, "GET", f"/org/repo/gvfs/objects/{tip}").status != 200:
            if time.time() > deadline:
                failures.append(f"{utc(t)}: the edge does not serve tip {tip[:10]}")
                break
            time.sleep(0.05)
        if utc(t).date() != cur_day:
            cur_day = utc(t).date()
            failures += each(lambda c: c.maintain(str(cur_day)), clients)
        todo = [c for c in clients if first or due(c.pattern, t)]
        failures += each(lambda c: c.sync(tip), todo)
        if utc(t).weekday() < 5 and (utc(t).hour, utc(t).minute) == (13, 0):
            old = commits[max(g_idx, k - 300)][0]
            failures += each(lambda c: c.history(up, old), [c for c in clients if c.pattern == "dev"])
        first = False
        if (t - start) % 86400 == 0:
            print(f"{utc(t)} tip #{k} elapsed {time.time() - t_run:.0f}s failures {len(failures)}", flush=True)
    failures += each(lambda c: c.maintain("end"), clients)
    results = {}
    for c in clients:
        v = c.verify()
        results[c.name] = {"pattern": c.pattern, "regime": c.regime, "verify": v, "final": counts(c.objdir),
                           **c.st, "days": c.days}
    m = metrics(port)
    edge.terminate()
    edge.wait()
    out = {"corpus": a.corpus, "days": a.days, "tip": tip, "failures": failures[:50],
           "edge": {k2: m.get(k2) for k2 in ("gvfs_alloc_hot_path_total", "gvfs_blobs_from_store_total",
                                             "gvfs_blobs_proxied_total", "gvfs_blobs_missing_total",
                                             "gvfs_errors_total")},
           "elapsed_s": round(time.time() - t_run), "clients": results}
    os.makedirs(os.path.join(ROOT, "bench/results"), exist_ok=True)
    json.dump(out, open(os.path.join(ROOT, "bench/results/blob-clients.json"), "w"), indent=1)
    print(f"done in {time.time() - t_run:.0f}s, {len(failures)} failures")
    for name, r in results.items():
        f = r["final"]
        print(f"{name:22s} ok={r['verify']['ok']} syncs={r['syncs']:4d} blob_packs={r['blob_packs_created']:4d} "
              f"fetched={(r['blob_pack_bytes'] + r['loose_bytes_created']) / 1e6:8.1f}MB packs_end={f['packs']:4d} "
              f"loose_end={f['loose']:4d} maint_written={r['maint_written'] / 1e6:9.1f}MB "
              f"maint_fetched={r['maint_fetched_bytes'] / 1e6:8.1f}MB maint_s={r['maint_s']:.1f}")


if __name__ == "__main__":
    main()
