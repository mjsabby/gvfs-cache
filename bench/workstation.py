#!/usr/bin/env python3
"""Client-side lifecycles: what a workstation or agent host accumulates on disk from each
server's responses over the replayed week, under four client regimes.

  stock   gvfs-helper as shipped and nothing else: every non-empty prefetch response becomes
          prefetch-<ts>-<sha>.pack/.idx/.keep and older prefetch .keep files are deleted
          (microsoft/git gvfs-helper.c: finalize_prefetch_packfile, delete_stale_keep_files).
  scalar  stock + Scalar's daily `git maintenance run --task=incremental-repack`
          (multi-pack-index write, expire, repack --batch-size=<auto: 2nd largest pack + 1>).
  vfs     stock + VFS for Git's daily PackfileMaintenanceStep
          (multi-pack-index write, expire, repack --batch-size=2g).
  geometric  stock + `git maintenance run --task=geometric-repack` daily (the opt-in
          `maintenance.strategy=geometric`: repack --geometric=2 -d --write-midx).
  hook    fable's `client-prefetch` rule: a pack is named by the response's interval
          (X-GVFS-Pack-Start/End, else (cursor, timestamp]); after each receipt every pack whose
          interval lies inside another's is unlinked.

scalar and vfs run real git (microsoft/git) on real files: the seed clone and every response
are hard-linked into one bare repository per client, and maintenance runs before the first
response of each new (UTC) day.  stock and hook are exact bookkeeping.

Checks: the newest prefetch pack (the gvfs-helper cursor) survives maintenance; after the
week `rev-list --objects --missing=print <last tip>` is empty in every scalar/vfs client; in
hook, every object an unlinked pack held is still in a kept pack or in the seed clone.

Input: <REPLAY_WORK>/<impl>/rx from `REPLAY_KEEP_RX=1 python3 replay.py <impls>`.
Usage: python3 workstation.py opus fable fable@h24      (REPLAY_WORK, WS_JOBS, WS_REGIMES)
"""
import concurrent.futures as cf
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

from gvfslib import clean_env

W = os.environ.get("REPLAY_WORK", "/home/muks/gvfs-bench/ws")
MSGIT = os.environ.get("GVFS_TEST_MSGIT", os.path.expanduser("~/git/bin-wrappers/git"))
CLIENTS = os.environ.get("WS_CLIENTS", "agent-1m,agent-15m,agent-1h,dev-hourly,dev-3x,dev-morning,dev-vacation").split(",")
JOBS = int(os.environ.get("WS_JOBS", "12"))
REAL = [r for r in os.environ.get("WS_REGIMES", "scalar,vfs").split(",") if r]
PERIOD = int(os.environ.get("WS_MAINT_SECS", "86400"))     # maintenance cadence (UTC-aligned)


def git(repo, *args, check=True):
    p = subprocess.run([MSGIT, f"--git-dir={repo}", *args], capture_output=True, env=clean_env())
    if check and p.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} in {repo}: {p.stderr.decode()[-400:]}")
    return p


def utc_day(t):
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).date()


def trailer(path):
    with open(path, "rb") as f:
        f.seek(-20, os.SEEK_END)
        return f.read(20).hex()


def oids(idx):
    with open(idx, "rb") as f:
        p = subprocess.run([MSGIT, "show-index"], stdin=f, capture_output=True, env=clean_env())
    return {l.split()[1] for l in p.stdout.decode().splitlines()}


class Rx:
    """One implementation's recorded week."""

    def __init__(self, impl):
        self.impl = impl
        self.dir = os.path.join(W, impl, "rx")
        self.seed = json.load(open(os.path.join(self.dir, "seed.json")))
        self.rows = {c: [] for c in CLIENTS}
        for line in open(os.path.join(self.dir, "rx.jsonl")):
            r = json.loads(line)
            if r["client"] in self.rows:
                self.rows[r["client"]].append(r)
        self.seed_pack = os.path.join(self.dir, "seed.pack")
        self.seed_idx = os.path.join(self.dir, "seed.idx")
        self.seed_repo = os.path.join(self.dir, "seed.git")

    def prepare(self):
        """git's own .idx for the seed clone, and a repository holding just the seed."""
        if not os.path.exists(self.seed_idx):
            tmp = self.seed_idx + ".tmp"
            with tempfile.TemporaryDirectory(dir=self.dir) as d:
                subprocess.run([MSGIT, "init", "-q", "--bare", d], check=True, env=clean_env())
                git(d, "index-pack", "--threads=16", "-o", tmp, self.seed_pack)
            os.replace(tmp, self.seed_idx)
        if not os.path.exists(self.seed_repo):
            subprocess.run([MSGIT, "init", "-q", "--bare", self.seed_repo], check=True, env=clean_env())
            pd = os.path.join(self.seed_repo, "objects", "pack")
            stem = os.path.join(pd, f"pack-{trailer(self.seed_pack)}")
            os.link(self.seed_pack, stem + ".pack")
            os.link(self.seed_idx, stem + ".idx")

    def path(self, r, ext):
        return os.path.join(self.dir, r["stem"] + ext)


# ---------------------------------------------------------------------------- bookkeeping
def stock(rx, rows):
    n = len(rows)
    disk = sum(os.path.getsize(rx.path(r, ".pack")) + os.path.getsize(rx.path(r, ".idx")) for r in rows)
    return dict(packs_end=1 + n, peak=1 + n, per_day=n / 7.0, disk_mb=disk / 1e6)


def hook(rx, rows):
    """fable client.rs: tombstone() after every receipt; the seed is the cold clone (0, ts]."""
    local = [dict(s=0, e=rx.seed["ts"], stem=None)]
    removed, peak, trace, cursor_bad = [], 1, [], 0
    for k, r in enumerate(rows):
        s = r["start"] if r["start"] is not None else r["cursor"]
        e = r["end"] if r["end"] is not None else r["ts"]
        local.append(dict(s=s, e=e, stem=r["stem"]))
        i = 0
        while i < len(local):
            p = local[i]
            if any(j != i and q["s"] <= p["s"] and p["e"] <= q["e"] for j, q in enumerate(local)):
                removed.append(p["stem"])
                local.pop(i)
            else:
                i += 1
        peak = max(peak, len(local))
        trace.append((r["t"], len(local)))
        # The hook's cursor is the newest interval end on disk: it must be what the replay sent next.
        nxt = rows[k + 1]["cursor"] if k + 1 < len(rows) else None
        if nxt is not None and max(p["e"] for p in local) != nxt:
            cursor_bad += 1
    if None in removed:
        return dict(error="the seed clone was tombstoned")
    kept = set()
    for p in local:
        if p["stem"]:
            kept |= oids(os.path.join(rx.dir, p["stem"] + ".idx"))
    gone = set()
    for stem in removed:
        gone |= oids(os.path.join(rx.dir, stem + ".idx"))
    lost = sorted(gone - kept)
    missing = 0
    if lost:
        p = subprocess.run([MSGIT, f"--git-dir={rx.seed_repo}", "cat-file", "--batch-check"],
                           input="\n".join(lost).encode() + b"\n", capture_output=True, env=clean_env())
        missing = sum(1 for l in p.stdout.decode().splitlines() if l.endswith(" missing"))
    disk = sum(os.path.getsize(os.path.join(rx.dir, p["stem"] + ext))
               for p in local if p["stem"] for ext in (".pack", ".idx"))
    days = {}
    for t, n in trace:
        days[str(utc_day(t))] = n
    return dict(packs_end=len(local), peak=peak, unlinked=len(removed), objects_lost=missing,
                checked_objects=len(lost), cursor_mismatch=cursor_bad, disk_mb=disk / 1e6,
                end_of_day=days)


# ---------------------------------------------------------------------------- real git
def packs_in(pd):
    return sorted(f[:-5] for f in os.listdir(pd) if f.endswith(".pack"))


def size_of(pd, stems):
    return sum(os.path.getsize(os.path.join(pd, s + ".pack")) for s in stems)


def install(pd, pack, idx, ts):
    """gvfs-helper: prefetch-<ts>-<sha>.{pack,idx,keep}, then every older prefetch .keep goes."""
    stem = os.path.join(pd, f"prefetch-{ts}-{trailer(pack)}")
    if not os.path.exists(stem + ".pack"):
        os.link(pack, stem + ".pack")
        os.link(idx, stem + ".idx")
        open(stem + ".keep", "w").close()
    keeps = [f for f in os.listdir(pd) if f.startswith("prefetch-") and f.endswith(".keep")]
    newest = max(keeps, key=lambda f: int(f.split("-")[1]))
    for f in keeps:
        if f != newest:
            os.unlink(os.path.join(pd, f))


def maintain(repo, regime):
    if regime == "scalar":
        git(repo, "maintenance", "run", "--task=incremental-repack")
    elif regime == "geometric":
        git(repo, "maintenance", "run", "--task=geometric-repack")
    else:
        git(repo, "multi-pack-index", "write", "--no-progress")
        git(repo, "multi-pack-index", "expire", "--no-progress")
        git(repo, "multi-pack-index", "repack", "--no-progress", "--batch-size=2g")


def real(rx, client, rows, regime):
    repo = os.path.join(W, "clients", rx.impl, regime, client + ".git")
    shutil.rmtree(repo, ignore_errors=True)
    os.makedirs(os.path.dirname(repo), exist_ok=True)
    subprocess.run([MSGIT, "init", "-q", "--bare", repo], check=True, env=clean_env())
    pd = os.path.join(repo, "objects", "pack")
    install(pd, rx.seed_pack, rx.seed_idx, rx.seed["ts"])
    git(repo, "update-ref", "refs/remotes/origin/main", rx.seed["tip"])
    day = rx.seed["ts"] // PERIOD
    peak, runs, maint_s, written, cursor_bad = 1, 0, 0.0, 0, 0
    trace, end_of_day = [], {}
    newest = rx.seed["ts"]
    for r in rows:
        d = r["t"] // PERIOD
        if d != day:
            before = set(packs_in(pd))
            t0 = time.perf_counter()
            maintain(repo, regime)
            maint_s += time.perf_counter() - t0
            runs += 1
            after = set(packs_in(pd))
            written += size_of(pd, after - before)
            if not any(s.startswith(f"prefetch-{newest}-") for s in after):
                cursor_bad += 1
            trace.append((r["t"], "maint", len(before), len(after)))
            day = d
        install(pd, rx.path(r, ".pack"), rx.path(r, ".idx"), r["ts"])
        git(repo, "update-ref", "refs/remotes/origin/main", r["tip"])    # what `git fetch` leaves
        newest = max(newest, r["ts"])
        n = len(packs_in(pd))
        peak = max(peak, n)
        end_of_day[str(utc_day(r["t"]))] = n
    stems = packs_in(pd)
    # The next day's maintenance, after the week: what the week's last deliveries let it drop.
    maintain(repo, regime)
    runs += 1
    after_next = len(packs_in(pd))
    p = git(repo, "rev-list", "--objects", "--missing=print", rows[-1]["tip"] if rows else rx.seed["tip"])
    missing = sum(1 for l in p.stdout.decode().splitlines() if l.startswith("?"))
    disk = sum(os.path.getsize(os.path.join(pd, f)) for f in os.listdir(pd)
               if f.endswith((".pack", ".idx", ".rev", ".bitmap")) and not f.startswith("prefetch-" + str(rx.seed["ts"])))
    midx = os.path.join(pd, "multi-pack-index")
    return dict(packs_end=len(stems), after_next_maint=after_next, peak=peak, maint_runs=runs,
                maint_s=round(maint_s, 2),
                maint_written_mb=written / 1e6, missing_objects=missing, cursor_lost=cursor_bad,
                disk_mb_excl_seed=disk / 1e6,
                midx_mb=os.path.getsize(midx) / 1e6 if os.path.exists(midx) else 0,
                end_of_day=end_of_day, maint=trace)


# ---------------------------------------------------------------------------- driver
def main():
    impls = sys.argv[1:] or ["opus", "fable", "fable@h24"]
    rxs = {i: Rx(i) for i in impls}
    with cf.ThreadPoolExecutor(len(rxs)) as ex:
        list(ex.map(lambda r: r.prepare(), rxs.values()))
    res = {i: {} for i in impls}
    jobs = []
    with cf.ThreadPoolExecutor(JOBS) as ex:
        for i, rx in rxs.items():
            for c in CLIENTS:
                rows = rx.rows[c]
                sent = sum(r["pack"] + r["idx"] for r in rows)
                res[i][c] = dict(responses=len(rows), received_mb=sent / 1e6,
                                 stock=stock(rx, rows), hook=hook(rx, rows))
                for regime in REAL:
                    jobs.append((i, c, regime, ex.submit(real, rx, c, rows, regime)))
        for i, c, regime, f in jobs:
            try:
                res[i][c][regime] = f.result()
            except Exception as e:  # keep the others
                res[i][c][regime] = dict(error=str(e))
            print(f"{i:10} {c:13} {regime:7} {json.dumps({k: v for k, v in res[i][c][regime].items() if k not in ('end_of_day', 'maint')})}",
                  flush=True)
    out = os.path.join(W, "workstation.json")
    if os.path.exists(out):     # other implementations' and regimes' results stay
        old = json.load(open(out))
        for i, clients in res.items():
            for c, v in clients.items():
                old.setdefault(i, {}).setdefault(c, {}).update(v)
        res = old
    with open(out, "w") as f:
        json.dump(res, f, indent=1)
    print("wrote", out)


if __name__ == "__main__":
    main()
