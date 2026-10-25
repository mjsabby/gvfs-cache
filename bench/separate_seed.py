"""Seed benchmark for `--blobs separate` stores (the plan's P0, scaled down).

Builds a synthetic repository of real content shaped like SearchGold (many small text files,
some medium binaries, a few large incompressible ones), then runs genesis with
`--blobs separate` and `mirror --scope tip`, and checks a sample of blobs through an edge
against git. Prints and saves a JSON summary (bench/results/separate-seed.json).

    python3 bench/separate_seed.py [--small 20000] [--medium 200] [--large 16] [--jobs 8]

The upstream is a local repository, so the copy rate is bounded by git's `pack-objects`
(re-deflating every blob it sends), not by the daemon; peak RSS is the daemon's own.
"""
import argparse
import json
import os
import random
import re
import shutil
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))
from gvfslib import clean_env  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "opus/target/release/gvfs-daemon")


def run(cmd, **kw):
    t0 = time.time()
    out = subprocess.run(cmd, env=clean_env(), capture_output=True, text=True, **kw)
    if out.returncode != 0:
        sys.exit(f"{cmd[:3]} failed:\n{out.stderr[-4000:]}")
    return out, time.time() - t0


def corpus(work, small, medium, large, seed=7):
    rnd = random.Random(seed)
    words = [bytes(rnd.choice(b"abcdefghijklmnopqrstuvwxyz") for _ in range(rnd.randint(2, 9))) for _ in range(5000)]
    sizes = {"small": 0, "medium": 0, "large": 0}
    for i in range(small):
        d = os.path.join(work, f"src/m{i % 97}/d{i % 13}")
        os.makedirs(d, exist_ok=True)
        n = int(rnd.lognormvariate(9.2, 1.0))  # median ~10 KB
        text = b" ".join(rnd.choice(words) for _ in range(max(1, n // 6)))[:n]
        open(os.path.join(d, f"f{i}.txt"), "wb").write(text)
        sizes["small"] += len(text)
    for i in range(medium):
        d = os.path.join(work, f"bin/b{i % 11}")
        os.makedirs(d, exist_ok=True)
        n = rnd.randint(1 << 20, 8 << 20)
        open(os.path.join(d, f"m{i}.bin"), "wb").write(os.urandom(n))
        sizes["medium"] += n
    for i in range(large):
        d = os.path.join(work, "data")
        os.makedirs(d, exist_ok=True)
        n = rnd.randint(64 << 20, 512 << 20)
        with open(os.path.join(d, f"l{i}.bin"), "wb") as f:
            left = n
            while left:
                k = min(left, 32 << 20)
                f.write(os.urandom(k))
                left -= k
        sizes["large"] += n
    return sizes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--work", default=os.path.expanduser("~/gvfs-bench/sep-seed"))
    ap.add_argument("--small", type=int, default=20000)
    ap.add_argument("--medium", type=int, default=200)
    ap.add_argument("--large", type=int, default=16)
    ap.add_argument("--jobs", type=int, default=8)
    a = ap.parse_args()
    W = a.work
    shutil.rmtree(W, ignore_errors=True)
    os.makedirs(W)
    up, tree = os.path.join(W, "up.git"), os.path.join(W, "tree")
    t0 = time.time()
    sizes = corpus(tree, a.small, a.medium, a.large)
    t_gen = time.time() - t0
    run(["git", "init", "-q", "--bare", "-b", "main", up])
    for k, v in [("core.compression", "1"), ("pack.compression", "1"), ("gc.auto", "0")]:
        run(["git", "--git-dir", up, "config", k, v])
    g = ["git", "--git-dir", up, "--work-tree", tree, "-c", "user.name=b", "-c", "user.email=b@b"]
    _, t_add = run(g + ["add", "-A"])
    run(g + ["commit", "-q", "-m", "corpus"])
    store = os.path.join(W, "store")
    out, t_genesis = run([BIN, "pipeline", "--store", f"fs:{store}", "--upstream", up, "--tier", "15m",
                          "--blobs", "separate", "--work-dir", os.path.join(W, "pw")])
    out, t_mirror = run([BIN, "mirror", "--store", f"fs:{store}", "--upstream", up, "--jobs", str(a.jobs),
                         "--work-dir", os.path.join(W, "mw")])
    log = out.stderr
    rss = re.search(r"peak RSS (.+)", log).group(1)
    plan = re.search(r"to copy in (\d+) items \((\d+) large\)", log)
    total = sum(sizes.values())

    # A sample through an edge, checked by git.
    port = 18900 + os.getpid() % 90
    srv = subprocess.Popen([BIN, "serve", "--store", f"fs:{store}", "--bind", f"127.0.0.1:{port}",
                            "--cache-dir", os.path.join(W, "cache"), "--alloc-guard", "strict"],
                           env=clean_env(), stderr=subprocess.DEVNULL)
    try:
        for _ in range(200):
            try:
                if urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz").status == 200:
                    break
            except Exception:
                time.sleep(0.1)
        ls = run(["git", "--git-dir", up, "ls-tree", "-r", "main"])[0].stdout.split("\n")
        blobs = [l.split()[2] for l in ls if l]
        rnd = random.Random(1)
        sample = rnd.sample(blobs, min(400, len(blobs)))
        check = os.path.join(W, "check.git")
        run(["git", "init", "-q", "--bare", check])
        body = json.dumps({"objectIds": sample}).encode()
        req = urllib.request.Request(f"http://127.0.0.1:{port}/gvfs/objects", data=body, method="POST",
                                     headers={"Content-Type": "application/json", "Accept": "application/x-git-packfile"})
        t1 = time.time()
        pack = urllib.request.urlopen(req).read()
        t_post = time.time() - t1
        p = subprocess.run(["git", "--git-dir", check, "index-pack", "--stdin", "--strict"], input=pack,
                           env=clean_env(), capture_output=True)
        assert p.returncode == 0, p.stderr
        want = subprocess.run(["git", "--git-dir", up, "cat-file", "--batch"], input="\n".join(sample).encode(),
                              env=clean_env(), capture_output=True).stdout
        got = subprocess.run(["git", "--git-dir", check, "cat-file", "--batch"], input="\n".join(sample).encode(),
                             env=clean_env(), capture_output=True).stdout
        assert want == got, "sample differs"
        metrics = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics").read().decode()
        hot = int(re.search(r"gvfs_alloc_hot_path_total (\d+)", metrics).group(1))
    finally:
        srv.kill()
        srv.wait()
    blob_bytes = sum(os.path.getsize(os.path.join(store, "blobs", f)) for f in os.listdir(os.path.join(store, "blobs"))
                     if f.endswith(".pack"))
    res = {
        "files": a.small + a.medium + a.large,
        "content_bytes": total,
        "by_class": sizes,
        "corpus_gen_s": round(t_gen, 1),
        "git_add_s": round(t_add, 1),
        "genesis_s": round(t_genesis, 1),
        "mirror_s": round(t_mirror, 1),
        "mirror_MBps": round(total / t_mirror / 1e6, 1),
        "mirror_items": int(plan.group(1)) if plan else None,
        "mirror_large_items": int(plan.group(2)) if plan else None,
        "mirror_peak_rss": rss,
        "jobs": a.jobs,
        "blob_pack_bytes": blob_bytes,
        "sample_post": {"blobs": len(sample), "bytes": len(pack), "s": round(t_post, 3), "git_verified": True},
        "edge_hot_path_allocs": hot,
    }
    print(json.dumps(res, indent=2))
    os.makedirs(os.path.join(ROOT, "bench/results"), exist_ok=True)
    json.dump(res, open(os.path.join(ROOT, "bench/results/separate-seed.json"), "w"), indent=2)


if __name__ == "__main__":
    main()
