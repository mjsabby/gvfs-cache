"""A synthetic repository of real content shaped like SearchGold, for `--blobs separate`.

Blob sizes follow SearchGold's measured tip distribution (p50 9.8 KB, p90 826 KB, p99 29 MB;
8.4% over 1 MB, 0.40% over 100 MB, 0.056% over 1 GB), with the largest files 2-6 GB and one
over 4 GiB. Small files are text (compressible), files of 1 MB and more random bytes (as
build outputs and models are). Some files are copies of others (SearchGold: 4.43M files,
2.47M distinct blobs). Files sit in components (top-level directories) of nested
directories; large files cluster in a few components.

History is a timeline of commits: an initial snapshot, `--pre-days` of history before the
store's genesis, then `--days` of commits for the replay. Each day changes about
`--churn` of the tip's bytes (SearchGold: 2.6%/day), as ~300 changed files: text edits,
binary rebuilds (new content of a similar size), added and deleted files. Commits cluster
in working hours, fewer on weekends; binaries change in commits of their own.

Everything is deterministic in `--seed`: the same arguments give the same commit ids.
Blobs are written by `--jobs` parallel `git fast-import` processes (text zlib-compressed
at git's default level, random binaries stored: deflating them gains nothing and runs at
31 MB/s); one more writes the trees and commits. `refs/heads/main` is left at the genesis commit, `refs/heads/replay-end`
at the last one (nothing is dangling).

Output (<out>/): up.git, corpus.json (commits with times, genesis, components, stats).

    python3 bench/corpus_gen.py --out ~/gvfs-bench/corpus-l --scale 1.0     (~130 GB)
    python3 bench/corpus_gen.py --out ~/gvfs-bench/corpus-s --scale 0.05    (~6 GB)
"""
import argparse
import bisect
import hashlib
import json
import math
import multiprocessing as mp
import os
import random
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from gvfslib import clean_env  # noqa: E402

KB, MB, GB = 1 << 10, 1 << 20, 1 << 30
# SearchGold's tip, distinct blobs: (size, cumulative fraction). Interpolated in log(size).
CDF = [(64, 0.0), (2 * KB, 0.25), (9782, 0.50), (825577, 0.90), (1 * MB, 0.9159),
       (10 * MB, 0.9657), (29177193, 0.99), (100 * MB, 0.9960), (1 * GB, 0.99944)]
COMPONENTS = ["src", "lib", "services", "web", "tests", "tools", "docs", "config", "sdk",
              "infra", "models", "data", "assets", "bin", "third_party", "build"]
# Components holding most large files (models, datasets, binaries, media).
HEAVY = {"models": 6, "data": 5, "assets": 3, "bin": 3, "third_party": 1, "build": 1}
TEXT_POOL = 64 * MB
CHUNK = 64 * MB


def sample_size(rnd, top):
    """A size from SearchGold's tip distribution; the tail above 1 GB runs to `top`."""
    u = rnd.random()
    pts = CDF + [(top, 1.0)]
    fs = [p[1] for p in pts]
    i = max(1, bisect.bisect_left(fs, u))
    (s0, f0), (s1, f1) = pts[i - 1], pts[i]
    x = 0.0 if f1 == f0 else (u - f0) / (f1 - f0)
    return int(math.exp(math.log(s0) + x * (math.log(s1) - math.log(s0))))


# ------------------------------------------------------------------ the model

class Blob:
    __slots__ = ("idx", "size", "seed", "text")

    def __init__(self, idx, size, seed, text):
        self.idx, self.size, self.seed, self.text = idx, size, seed, text


def build_model(a):
    rnd = random.Random(a.seed)
    blobs = []

    def new_blob(size):
        b = Blob(len(blobs), max(1, size), rnd.getrandbits(63), size < MB)
        blobs.append(b)
        return b

    # Directories: per component, a nested tree.
    dirs = {}
    n_files = max(200, int(a.files * a.scale))
    n_dirs = max(len(COMPONENTS) * 2, n_files // 14)
    for comp in COMPONENTS:
        dirs[comp] = [comp]
    for k in range(n_dirs):
        comp = COMPONENTS[k % len(COMPONENTS)]
        parent = rnd.choice(dirs[comp])
        if parent.count("/") >= 5:
            parent = comp
        dirs[comp].append(f"{parent}/d{k}")
    # Files at the initial snapshot. Sizes by class: large ones go to heavy components.
    top = int(a.max_gb * GB)
    files = {}                           # path -> Blob
    heavy = [c for c, w in HEAVY.items() for _ in range(w)]
    distinct = int(n_files / (1 + a.dup))
    for i in range(distinct):
        size = sample_size(rnd, top)
        comp = rnd.choice(heavy) if size >= MB and rnd.random() < 0.85 else rnd.choice(COMPONENTS)
        d = rnd.choice(dirs[comp])
        ext = "txt" if size < MB else "bin"
        files[f"{d}/f{i}.{ext}"] = new_blob(size)
    # The tail: a few files of 2-6 GB, one over 4 GiB (scaled for small corpora).
    big = max(1, round(4 * a.scale))
    for j in range(big):
        size = int((4.3 * GB) if j == 0 else rnd.uniform(2, a.max_gb) * GB)
        size = min(size, int(a.max_gb * GB)) if a.scale < 1 else size
        d = rnd.choice(dirs[rnd.choice(["models", "data"])])
        files[f"{d}/huge{j}.bin"] = new_blob(size)
    # Copies: the same content under other paths.
    originals = list(files)
    for i in range(n_files - distinct):
        src = files[rnd.choice(originals)] if i % 64 else None
        if src is None or src.size >= 64 * MB:
            continue
        d = rnd.choice(dirs[rnd.choice(COMPONENTS)])
        files[f"{d}/copy{i}.{'txt' if src.text else 'bin'}"] = src
    initial = dict(files)
    tip_bytes = sum(b.size for b in {id(b): b for b in files.values()}.values())

    # The timeline: commits per day, changes per commit.
    start = a.start
    days = a.pre_days + a.days
    commits = [(start, "initial", [("M", p, b) for p, b in sorted(files.items())])]
    per_day_bytes = a.churn * tip_bytes
    n = 0
    for day in range(days):
        t0 = start + 86400 * (day + 1)
        weekday = ((t0 // 86400) + 4) % 7          # 1970-01-01 was a Thursday
        weekend = weekday >= 5
        n_commits = int(a.commits_per_day * (0.2 if weekend else 1.0))
        times = sorted(t0 + int(rnd.betavariate(4, 4) * 86400) for _ in range(n_commits))
        budget = per_day_bytes * (0.2 if weekend else 1.0)
        paths = sorted(files)
        bins = [p for p in paths if not files[p].text and files[p].size < 2 * GB]
        texts = [p for p in paths if files[p].text]
        changes_today = int(a.changes_per_day * (0.2 if weekend else 1.0))
        plan = []
        # Binary rebuilds take most of the bytes.
        spent = 0
        while spent < budget * 0.95 and bins:
            p = rnd.choice(bins)
            size = max(MB, int(files[p].size * rnd.uniform(0.9, 1.1)))
            if spent + size > budget * 1.3:
                break
            plan.append(("bin", p, size))
            spent += size
        for _ in range(max(0, changes_today - len(plan))):
            r = rnd.random()
            if r < 0.88 and texts:
                p = rnd.choice(texts)
                plan.append(("text", p, max(16, int(files[p].size * rnd.uniform(0.95, 1.08)))))
            elif r < 0.96:
                comp = rnd.choice(COMPONENTS)
                n += 1
                plan.append(("add", f"{rnd.choice(dirs[comp])}/n{day}_{n}.txt", sample_size(rnd, MB - 1)))
            elif texts:
                plan.append(("del", rnd.choice(texts), 0))
        rnd.shuffle(plan)
        # Binaries ship alone; text changes are grouped 1-4 per commit.
        slots = [[] for _ in times] or [[]]
        k = 0
        for c in plan:
            if c[0] == "bin":
                slots[k % len(slots)].append(c)
                k += 1
        for c in plan:
            if c[0] != "bin":
                j = rnd.randrange(len(slots))
                if not any(x[0] == "bin" for x in slots[j]) and len(slots[j]) < 4:
                    slots[j].append(c)
                else:
                    slots[rnd.randrange(len(slots))].append(c)
        for t, slot in zip(times or [t0 + 43200], slots):
            ops = []
            seen = set()
            for kind, p, size in slot:
                if p in seen:
                    continue
                seen.add(p)
                if kind == "del":
                    if p in files:
                        del files[p]
                        ops.append(("D", p, None))
                elif kind == "add" or p in files:
                    b = new_blob(size)
                    files[p] = b
                    ops.append(("M", p, b))
            if ops:
                commits.append((t, f"day{day}", ops))
    genesis_idx = max(i for i, c in enumerate(commits) if c[0] < start + 86400 * (a.pre_days + 1))
    return blobs, commits, genesis_idx, initial, files


# ------------------------------------------------------------------ content

def text_pool(seed):
    rnd = random.Random(seed)
    words = [bytes(rnd.choice(b"abcdefghijklmnopqrstuvwxyz") for _ in range(rnd.randint(2, 10)))
             for _ in range(8000)]
    out = bytearray()
    while len(out) < TEXT_POOL:
        line = b" ".join(rnd.choice(words) for _ in range(rnd.randint(3, 14)))
        out += line + b"\n"
    return bytes(out[:TEXT_POOL])


def content_chunks(b, pool):
    """The blob's bytes, in chunks: text from the pool, binaries random."""
    if b.text:
        head = b"%d %d\n" % (b.idx, b.seed)
        off = b.seed % (TEXT_POOL - max(b.size, 1))
        yield (head + pool[off:off + b.size])[:b.size]
        return
    left, k = b.size, 0
    while left > 0:
        n = min(CHUNK, left)
        yield random.Random(b.seed * 1000003 + k).randbytes(n)
        left -= n
        k += 1


def write_blobs(args):
    """One fast-import process: the blobs `specs` (idx, size, seed, text); returns marks."""
    git_dir, specs, seed, job, compression = args
    pool = text_pool(seed)
    marks = os.path.join(git_dir, f"marks-{job}")
    p = subprocess.Popen(["git", "--git-dir", git_dir, "-c", f"core.compression={compression}",
                          "-c", f"pack.compression={compression}", "fast-import", "--quiet", "--depth=0",
                          "--big-file-threshold=1m", f"--export-marks={marks}"],
                         stdin=subprocess.PIPE, env=clean_env())
    w = p.stdin
    for idx, size, bseed, text in specs:
        b = Blob(idx, size, bseed, text)
        w.write(b"blob\nmark :%d\ndata %d\n" % (idx + 1, size))
        for ch in content_chunks(b, pool):
            w.write(ch)
        w.write(b"\n")
    w.close()
    if p.wait() != 0:
        raise RuntimeError(f"fast-import job {job} failed")
    out = {}
    for line in open(marks):
        m, oid = line.split()
        out[int(m[1:]) - 1] = oid
    os.unlink(marks)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--files", type=int, default=32000, help="files at the initial snapshot, at scale 1")
    ap.add_argument("--dup", type=float, default=0.15, help="copies per distinct file")
    ap.add_argument("--max-gb", type=float, default=6.0)
    ap.add_argument("--pre-days", type=int, default=7)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--churn", type=float, default=0.025, help="new bytes per weekday, of the tip")
    ap.add_argument("--changes-per-day", type=int, default=300)
    ap.add_argument("--commits-per-day", type=int, default=150)
    ap.add_argument("--start", type=int, default=1790812800, help="initial snapshot time (a Monday 00:00 UTC)")
    ap.add_argument("--seed", type=int, default=20261026)
    ap.add_argument("--jobs", type=int, default=12)
    ap.add_argument("--compression", type=int, default=6, help="zlib level of text blobs")
    ap.add_argument("--plan-only", action="store_true")
    a = ap.parse_args()

    t0 = time.time()
    blobs, commits, genesis_idx, initial, final = build_model(a)
    total = sum(b.size for b in blobs)
    print(f"model: {len(blobs)} blobs, {total / 1e9:.1f} GB; {len(commits)} commits "
          f"(genesis #{genesis_idx}); {time.time() - t0:.1f}s", flush=True)
    if a.plan_only:
        sizes = sorted(b.size for b in blobs)
        print({k: sizes[int(len(sizes) * q)] for k, q in (("p50", .5), ("p90", .9), ("p99", .99))},
              "max", sizes[-1], ">1GB", sum(s >= GB for s in sizes))
        return

    shutil.rmtree(a.out, ignore_errors=True)
    os.makedirs(a.out)
    up = os.path.join(a.out, "up.git")
    subprocess.run(["git", "init", "-q", "--bare", "-b", "main", up], check=True, env=clean_env())
    for k, v in [("gc.auto", "0"), ("core.bigFileThreshold", "1m")]:
        subprocess.run(["git", "--git-dir", up, "config", k, v], check=True, env=clean_env())

    # Blobs, in parallel: text in two shards (compressed), binaries in the rest (stored),
    # balanced by bytes, big ones spread first.
    t1 = time.time()
    n_text = 2
    shards = [[] for _ in range(a.jobs)]
    load = [0] * a.jobs
    for b in sorted(blobs, key=lambda b: -b.size):
        pool_ids = range(n_text) if b.text else range(n_text, a.jobs)
        j = min(pool_ids, key=lambda k: load[k])
        shards[j].append((b.idx, b.size, b.seed, b.text))
        load[j] += b.size
    for s in shards:
        s.sort()
    oids = {}
    jobs = [(up, s, a.seed, j, a.compression if j < n_text else 0) for j, s in enumerate(shards) if s]
    with mp.Pool(a.jobs) as pool:
        for got in pool.imap_unordered(write_blobs, jobs):
            oids.update(got)
    t_blobs = time.time() - t1
    print(f"blobs written in {t_blobs:.0f}s ({total / t_blobs / 1e9:.2f} GB/s)", flush=True)

    # Trees and commits.
    t2 = time.time()
    marks = os.path.join(a.out, "commit-marks")
    p = subprocess.Popen(["git", "--git-dir", up, "fast-import", "--quiet", f"--export-marks={marks}"],
                         stdin=subprocess.PIPE, env=clean_env())
    w = p.stdin
    for i, (t, msg, ops) in enumerate(commits):
        w.write(b"commit refs/heads/replay-end\nmark :%d\n" % (i + 1))
        w.write(b"author Corpus <corpus@gen> %d +0000\ncommitter Corpus <corpus@gen> %d +0000\n" % (t, t))
        m = f"{msg} #{i}".encode()
        w.write(b"data %d\n%s\n" % (len(m), m))
        if i > 0:
            w.write(b"from :%d\n" % i)
        for op, path, b in ops:
            if op == "D":
                w.write(b"D %s\n" % path.encode())
            else:
                w.write(b"M 100644 %s %s\n" % (oids[b.idx].encode(), path.encode()))
        w.write(b"\n")
    w.close()
    if p.wait() != 0:
        sys.exit("fast-import (commits) failed")
    commit_oids = {}
    for line in open(marks):
        m, oid = line.split()
        commit_oids[int(m[1:]) - 1] = oid
    os.unlink(marks)
    genesis = commit_oids[genesis_idx]
    subprocess.run(["git", "--git-dir", up, "update-ref", "refs/heads/main", genesis], check=True, env=clean_env())
    subprocess.run(["git", "--git-dir", up, "commit-graph", "write", "--reachable"], check=True, env=clean_env())
    subprocess.run(["git", "--git-dir", up, "multi-pack-index", "write"], check=True, env=clean_env())
    t_commits = time.time() - t2

    # What the replay and the client studies need: the timeline, components, stats.
    def comps(fs):
        out = {}
        for path, b in fs.items():
            c = out.setdefault(path.split("/")[0], {"files": 0, "bytes": 0, "over_100mb": 0})
            c["files"] += 1
            c["bytes"] += b.size
            c["over_100mb"] += b.size >= 100 * MB
        return out

    sizes = sorted(b.size for b in blobs)
    meta = {
        "args": vars(a),
        "genesis": genesis, "genesis_index": genesis_idx, "genesis_time": commits[genesis_idx][0],
        "end": commit_oids[len(commits) - 1], "start_time": commits[0][0], "end_time": commits[-1][0],
        "commits": [[commit_oids[i], c[0]] for i, c in enumerate(commits)],
        "blobs": len(blobs), "blob_bytes": total,
        "size_quantiles": {k: sizes[int(len(sizes) * q)] for k, q in (("p50", .5), ("p90", .9), ("p99", .99))},
        "max_blob": sizes[-1], "over_1gb": sum(s >= GB for s in sizes), "over_4gib": sum(s > 4 * GB for s in sizes),
        "components_at_genesis": comps(initial), "components_at_end": comps(final),
        "files_at_end": len(final),
        "timing_s": {"model": round(t1 - t0, 1), "blobs": round(t_blobs, 1), "commits": round(t_commits, 1)},
    }
    json.dump(meta, open(os.path.join(a.out, "corpus.json"), "w"), indent=1)
    print(json.dumps({k: v for k, v in meta.items() if k not in ("commits", "args", "components_at_genesis",
                                                                  "components_at_end")}, indent=1))


if __name__ == "__main__":
    main()
