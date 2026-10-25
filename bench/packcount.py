#!/usr/bin/env python3
"""What pack count costs git on a client: the seed clone plus N small prefetch packs (a real
agent's week of responses, hard-linked under distinct names when N exceeds it), with and
without a multi-pack-index.  Median of 5 runs of each operation, microsoft/git.

  absent    `cat-file --batch-check`, 2,000 ids no pack holds (every lookup misses everywhere)
  recent    `cat-file --batch-check`, 2,000 ids from the newest packs (what agents touch)
  log       `log --format=%H -n 500` from the tip (commit reads across the packs)
  diff      `diff-tree -r <tip~100> <tip>` (tree reads, recent and old)
  startup   `rev-parse <tip>^{tree}` (one command's fixed cost)

Usage: python3 packcount.py [impl] [client]     (REPLAY_WORK; default opus agent-1m)
"""
import json
import os
import random
import statistics
import subprocess
import sys
import time

from gvfslib import clean_env

W = os.environ.get("REPLAY_WORK", "/home/muks/gvfs-bench/ws")
MSGIT = os.environ.get("GVFS_TEST_MSGIT", os.path.expanduser("~/git/bin-wrappers/git"))
SIZES = [int(x) for x in os.environ.get("PC_SIZES", "0,100,1000,5000").split(",")]


def git(repo, *args, stdin=None):
    p = subprocess.run([MSGIT, f"--git-dir={repo}", *args], input=stdin, capture_output=True,
                       env=clean_env())
    if p.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)}: {p.stderr.decode()[-300:]}")
    return p.stdout


def timed(fn, n=5):
    xs = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        xs.append(time.perf_counter() - t0)
    return statistics.median(xs)


def trailer(path):
    with open(path, "rb") as f:
        f.seek(-20, os.SEEK_END)
        return f.read(20).hex()


def main():
    impl = sys.argv[1] if len(sys.argv) > 1 else "opus"
    client = sys.argv[2] if len(sys.argv) > 2 else "agent-1m"
    rxd = os.path.join(W, impl, "rx")
    rows = [json.loads(l) for l in open(os.path.join(rxd, "rx.jsonl"))]
    rows = [r for r in rows if r["client"] == client]
    seed = json.load(open(os.path.join(rxd, "seed.json")))
    tip = rows[-1]["tip"]
    recent = []
    for r in reversed(rows[-50:]):
        with open(os.path.join(rxd, r["stem"] + ".idx"), "rb") as f:
            out = subprocess.run([MSGIT, "show-index"], stdin=f, capture_output=True, env=clean_env())
        recent += [l.split()[1] for l in out.stdout.decode().splitlines()]
    random.seed(7)
    recent = random.sample(recent, min(2000, len(recent)))
    absent = ["%040x" % random.getrandbits(160) for _ in range(2000)]
    base = os.path.join(W, "packcount")
    os.makedirs(base, exist_ok=True)
    res = []
    for n in SIZES:
        repo = os.path.join(base, f"{impl}-{client}-{n}.git")
        subprocess.run(["rm", "-rf", repo], check=True)
        subprocess.run([MSGIT, "init", "-q", "--bare", repo], check=True, env=clean_env())
        pd = os.path.join(repo, "objects", "pack")
        os.link(os.path.join(rxd, "seed.pack"), os.path.join(pd, f"prefetch-{seed['ts']}-{trailer(os.path.join(rxd, 'seed.pack'))}.pack"))
        os.link(os.path.join(rxd, "seed.idx"), os.path.join(pd, f"prefetch-{seed['ts']}-{trailer(os.path.join(rxd, 'seed.pack'))}.idx"))
        # The newest packs are the real ones (so `recent` and the tip resolve). With fewer than
        # the week's packs the older ones are consolidated into one (`pack-objects
        # --stdin-packs`, as geometric repack does), so history stays complete; with more,
        # older copies pad.
        names = []
        for k in range(min(n, len(rows)) if n < len(rows) else max(n, len(rows))):
            r = rows[len(rows) - 1 - (k % len(rows))]
            name = f"prefetch-{r['ts'] - (k // len(rows)) * 10**6}-{trailer(os.path.join(rxd, r['stem'] + '.pack'))}"
            if k >= len(rows):
                name += f"x{k // len(rows)}"
            for ext in (".pack", ".idx"):
                os.link(os.path.join(rxd, r["stem"] + ext), os.path.join(pd, name + ext))
            names.append(name)
        if 0 < n < len(rows):
            older = rows[:len(rows) - (n - 1)]
            for r in older:
                name = f"prefetch-{r['ts']}-{trailer(os.path.join(rxd, r['stem'] + '.pack'))}"
                if name in names:
                    continue
                for ext in (".pack", ".idx"):
                    os.link(os.path.join(rxd, r["stem"] + ext), os.path.join(pd, name + ext))
            keep = set(names[:n - 1])
            olds = [f[:-5] for f in os.listdir(pd) if f.endswith(".pack") and f.startswith("prefetch-")
                    and f[:-5] not in keep and not f.startswith(f"prefetch-{seed['ts']}-")]
            git(repo, "pack-objects", "--stdin-packs", "-q", os.path.join(pd, "pack"),
                stdin="".join(o + ".pack\n" for o in olds).encode())
            for o in olds:
                for ext in (".pack", ".idx"):
                    os.unlink(os.path.join(pd, o + ext))
        packs_now = len([f for f in os.listdir(pd) if f.endswith(".pack")])
        assert packs_now == n + 1, (n, packs_now)
        if n:
            git(repo, "update-ref", "refs/heads/main", tip)
        else:
            git(repo, "update-ref", "refs/heads/main", seed["tip"])
        head = "main"
        for midx in (False, True):
            if midx:
                t0 = time.perf_counter()
                git(repo, "multi-pack-index", "write", "--no-progress")
                midx_s = time.perf_counter() - t0
            else:
                midx_s = None
            rec = recent if n else []
            row = dict(packs=n + 1, midx=midx, midx_write_s=midx_s)
            row["absent_ms"] = 1e3 * timed(lambda: git(repo, "cat-file", "--batch-check", stdin="\n".join(absent).encode() + b"\n"))
            row["recent_ms"] = 1e3 * timed(lambda: git(repo, "cat-file", "--batch-check", stdin="\n".join(rec).encode() + b"\n")) if rec else None
            row["log_ms"] = 1e3 * timed(lambda: git(repo, "log", "--format=%H", "-n", "500", head))
            row["diff_ms"] = 1e3 * timed(lambda: git(repo, "diff-tree", "-r", f"{head}~100", head))
            row["startup_ms"] = 1e3 * timed(lambda: git(repo, "rev-parse", f"{head}^{{tree}}"))
            res.append(row)
            print(json.dumps({k: (round(v, 1) if isinstance(v, float) else v) for k, v in row.items()}), flush=True)
        subprocess.run(["rm", "-rf", repo], check=True)
    with open(os.path.join(W, f"packcount-{impl}-{client}.json"), "w") as f:
        json.dump(res, f, indent=1)


if __name__ == "__main__":
    main()
