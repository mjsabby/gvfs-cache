"""Two builds' edges on one store, the same requests to both: every response must be
byte-identical (status, content type, body). A sample is also judged by git (`index-pack`
and the object set of each pack; sizes against `cat-file`), so that the two are not equally
wrong. Request mixes: blob batches of 1 to 20,000 ids (repeats included), blobs with trees
and a commit, single-blob POSTs under each Accept, sizes of blobs, trees and commits, GETs.

    python3 bench/ab_responses.py --store ~/gvfs-bench/prof/sg7-sep/store \\
        --git ~/gvfs-bench/prof/sg7-sep/up.git --blobs ~/gvfs-bench/prof/sg7-sep/blobs.txt

`--upstream`: both edges also serve a live tip ahead of the store (L0) and proxy its new
blobs, which the mix then includes: fetched from the upstream the first time, from each
edge's proxy cache after, interleaved with the store's. Give the upstream `pack.threads=1`,
so that its packs, and with them each edge's L0 slab and proxy cache, are the same for both
edges. Where two packs still differ in bytes, they must hold the same objects in the same
order (git `index-pack`); loose objects, the same content.
"""
import argparse
import hashlib
import json
import os
import random
import shutil
import subprocess
import sys
import time
import zlib

sys.path.insert(0, os.path.dirname(__file__))
from gvfslib import clean_env, free_port, metrics, request, wait_http  # noqa: E402

BENCH = os.path.expanduser("~/gvfs-bench")
PACK = "application/x-git-packfile"
LOOSE = "application/x-git-loose-object"


def serve(binary, store, log, cache, upstream=None):
    port = free_port()
    shutil.rmtree(cache, ignore_errors=True)
    cmd = [binary, "serve", "--store", f"fs:{store}", "--bind", f"127.0.0.1:{port}",
           "--cache-dir", cache, "--alloc-guard", "strict"]
    if upstream:
        cmd += ["--upstream", upstream, "--branch", "main", "--poll-ms", "200"]
    p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=open(log, "wb"), env=clean_env())
    wait_http(port, "/healthz", timeout=1800, proc=p)
    return p, port


def git(git_dir, *args, input=None):
    return subprocess.run(["git", "--git-dir", git_dir, *args], input=input, capture_output=True,
                          check=True, env=clean_env()).stdout


def pack_objects(pack, scratch):
    """The ids in `pack` (git index-pack computes each from its content)."""
    shutil.rmtree(scratch, ignore_errors=True)
    subprocess.run(["git", "init", "-q", "--bare", scratch], check=True, env=clean_env())
    path = os.path.join(scratch, "in.pack")
    open(path, "wb").write(pack)
    subprocess.run(["git", "--git-dir", scratch, "index-pack", path], check=True, capture_output=True,
                   env=clean_env())
    out = git(scratch, "show-index", input=open(path[:-5] + ".idx", "rb").read()).decode()
    return {line.split()[1] for line in out.splitlines()}


def pack_order(pack, scratch):
    """The ids in `pack`, in entry order."""
    shutil.rmtree(scratch, ignore_errors=True)
    subprocess.run(["git", "init", "-q", "--bare", scratch], check=True, env=clean_env())
    path = os.path.join(scratch, "in.pack")
    open(path, "wb").write(pack)
    subprocess.run(["git", "--git-dir", scratch, "index-pack", path], check=True, capture_output=True,
                   env=clean_env())
    out = git(scratch, "show-index", input=open(path[:-5] + ".idx", "rb").read()).decode()
    return [oid for _, oid in sorted((int(line.split()[0]), line.split()[1]) for line in out.splitlines())]


def stream_digest(port, path, headers):
    """Status, content type and SHA-256 of a (large) response body, streamed."""
    import http.client
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=3600)
    try:
        c.request("GET", path, headers=headers)
        r = c.getresponse()
        h, n = hashlib.sha256(), 0
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            h.update(chunk)
            n += len(chunk)
        return r.status, r.getheader("Content-Type"), n, h.hexdigest()
    finally:
        c.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", required=True)
    ap.add_argument("--git", required=True, help="a repository holding every object of the store")
    ap.add_argument("--blobs", required=True)
    ap.add_argument("--base", default=os.path.join(BENCH, "prof/bin/gvfs-daemon-base"))
    ap.add_argument("--new", default=os.path.join(BENCH, "prof/bin/gvfs-daemon-new"))
    ap.add_argument("--rounds", type=int, default=6)
    ap.add_argument("--since", default=None,
                    help="the baseline's tip: objects after it (the newer slabs') are mixed in")
    ap.add_argument("--upstream", default=None, help="an upstream ahead of the store (see above)")
    a = ap.parse_args()
    rnd = random.Random(11)
    blobs = open(a.blobs).read().split()
    tip = git(a.git, "rev-parse", "main").decode().strip()
    trees = git(a.git, "rev-list", "--objects", "--no-object-names", "--filter=object:type=tree",
                "--filter-provided-objects", tip).decode().split()
    commits = git(a.git, "rev-list", "--max-count=200", tip).decode().split()
    recent = []
    if a.since:
        # Objects of the newer slabs (pyramid, weekly): newest-first lookups find them first.
        recent = git(a.git, "rev-list", "--objects", "--no-object-names", tip, f"^{a.since}").decode().split()
        kinds = git(a.git, "cat-file", "--batch-check=%(objecttype)", input="\n".join(recent).encode()).decode().split()
        recent_blobs = [o for o, k in zip(recent, kinds) if k == "blob"]
        print(f"recent objects: {len(recent)} ({len(recent_blobs)} blobs)", flush=True)
        blobs = blobs + recent_blobs * max(1, len(blobs) // (20 * max(1, len(recent_blobs))))
        trees = trees + [o for o, k in zip(recent, kinds) if k == "tree"] * 20
        commits = commits + [o for o, k in zip(recent, kinds) if k == "commit"]
    l0 = []
    if a.upstream:
        up_tip = git(a.upstream, "rev-parse", "main").decode().strip()
        l0 = git(a.upstream, "rev-list", "--objects", "--no-object-names", "--filter=object:type=blob",
                 "--filter-provided-objects", up_tip, f"^{tip}").decode().split()
        print(f"live tip {up_tip[:10]}: {len(l0)} blobs newer than the store's tip", flush=True)
    scratch = os.path.join(BENCH, "prof/ab-scratch.git")
    servers = {t: serve(b, a.store, os.path.join(BENCH, f"prof/out/abresp-{t}.log"),
                        os.path.join(BENCH, f"prof/cache-ab-{t}"), a.upstream)
               for t, b in (("base", a.base), ("new", a.new))}
    if a.upstream:
        for t, (_, port) in servers.items():
            deadline = time.time() + 600
            while request(port, "GET", f"/gvfs/objects/{up_tip}").status != 200:
                assert time.time() < deadline, f"{t}: live tip not served"
                time.sleep(0.2)
    checked = {"posts": 0, "post_objects": 0, "sizes": 0, "gets": 0, "identical": 0, "git_checked": 0,
               "same_order": 0, "same_content": 0}

    def both(method, path, body=None, headers=None):
        rs = {t: request(port, method, path, body=body, headers=headers or {}, timeout=3600)
              for t, (_, port) in servers.items()}
        b, n = rs["base"], rs["new"]
        kind = n.headers.get("content-type")
        assert b.status == n.status and b.headers.get("content-type") == kind, (path, b.status, n.status)
        if b.body == n.body:
            checked["identical"] += 1
        elif n.status == 200 and kind == PACK:
            ob, on = pack_order(b.body, scratch), pack_order(n.body, scratch)
            first = next((i for i, (x, y) in enumerate(zip(ob, on)) if x != y), None)
            assert ob == on, (path, "entries differ", len(ob), len(on), "first at", first)
            checked["same_order"] += 1
        elif n.status == 200 and kind == LOOSE:
            assert zlib.decompress(b.body) == zlib.decompress(n.body), (path, "content differs")
            checked["same_content"] += 1
        else:
            raise AssertionError((path, n.status, kind, len(b.body), len(n.body)))
        return n

    def with_l0(ids, k):
        """`ids` with `k` blobs of the live tip mixed in."""
        out = ids + rnd.sample(l0, min(k, len(l0))) if l0 else ids
        rnd.shuffle(out)
        return out

    try:
        for r in range(a.rounds):
            for n in (1, 2, 100, 4000, 20000):
                ids = rnd.sample(blobs, n)
                ids += rnd.sample(ids, max(1, n // 10))     # repeats
                rnd.shuffle(ids)
                if n >= 100:
                    ids = with_l0(ids, n // 20)
                resp = both("POST", "/gvfs/objects", body={"objectIds": ids, "commitDepth": 1},
                            headers={"Accept": PACK})
                assert resp.status == 200
                checked["posts"] += 1
                if r < 2:
                    got = pack_objects(resp.body, scratch)
                    # Exactly the ids asked for, plus (inline stores) the delta bases they need.
                    assert set(ids) <= got, (n, len(got), len(set(ids)))
                    checked["bases"] = checked.get("bases", 0) + len(got) - len(set(ids))
                    checked["git_checked"] += 1
                    checked["post_objects"] += len(got)
            # Blobs with trees (and, every other round, a commit: its trees come too).
            ids = rnd.sample(blobs, 3000) + rnd.sample(trees, 500)
            if r % 2:
                ids.append(rnd.choice(commits))
            ids = with_l0(ids, 150)
            if l0:
                # The live tip's blobs alone (each fetched or cached).
                resp = both("POST", "/gvfs/objects", body={"objectIds": with_l0([], 60), "commitDepth": 1},
                            headers={"Accept": PACK})
                assert resp.status == 200
            resp = both("POST", "/gvfs/objects", body={"objectIds": ids, "commitDepth": 1},
                        headers={"Accept": PACK})
            if r < 2:
                got = pack_objects(resp.body, scratch)
                assert set(ids) <= got, "missing objects"
                checked["git_checked"] += 1
            # One blob under each Accept (loose, pack, both), from the store and the live tip.
            for accept in (LOOSE, PACK, f"{PACK}, {LOOSE}"):
                for oid in [rnd.choice(blobs)] + ([rnd.choice(l0)] if l0 else []):
                    both("POST", "/gvfs/objects", body={"objectIds": [oid], "commitDepth": 1},
                         headers={"Accept": accept})
            # Sizes: blobs, trees and commits.
            ids = with_l0(rnd.sample(blobs, 3900) + rnd.sample(trees, 90) + rnd.sample(commits, 10), 50)
            resp = both("POST", "/gvfs/sizes", body=ids)
            assert resp.status == 200
            if r < 2:
                answered = {e["Id"]: e["Size"] for e in json.loads(resp.body)}
                want = {}
                for line in git(a.git, "cat-file", "--batch-check=%(objectname) %(objectsize)",
                                input="\n".join(ids).encode()).decode().splitlines():
                    o, s = line.split()
                    want[o] = int(s)
                assert answered == want, "sizes differ from git"
                checked["git_checked"] += 1
            checked["sizes"] += 1
            # GETs: blobs (loose, checked by hash), a tree, a commit.
            for oid in with_l0(rnd.sample(blobs, 300) + rnd.sample(trees, 5) + rnd.sample(commits, 5), 20):
                resp = both("GET", f"/gvfs/objects/{oid}")
                assert resp.status == 200, (oid, resp.status)
                raw = zlib.decompress(resp.body)
                assert hashlib.sha1(raw).hexdigest() == oid, oid
                checked["gets"] += 1
            if r == 0:
                # Prefetch: cold with sizes (gvfs-helper), cold with the .idx (VFS for Git),
                # and warm from a few boundaries.
                for path, hdrs in (("/gvfs/prefetch?lastPackTimestamp=0",
                                    {"User-Agent": "git/2.53.0.vfs.0.0", "X-GVFS-Sizes": "1"}),
                                   ("/gvfs/prefetch?lastPackTimestamp=0", {"User-Agent": "GVFS/1.0"}),
                                   ("/gvfs/prefetch?lastPackTimestamp=1792400000",
                                    {"User-Agent": "git/2.53.0.vfs.0.0", "X-GVFS-Sizes": "1"}),
                                   ("/gvfs/prefetch?lastPackTimestamp=1792440000", {"User-Agent": "GVFS/1.0"})):
                    got = {t: stream_digest(port, path, hdrs) for t, (_, port) in servers.items()}
                    assert got["base"] == got["new"], (path, hdrs, got)
                    assert got["new"][0] == 200, got
                    checked["prefetch"] = checked.get("prefetch", 0) + 1
                    checked["prefetch_bytes"] = checked.get("prefetch_bytes", 0) + got["new"][2]
            print(f"round {r}: {checked}", flush=True)
        for t, (_, port) in servers.items():
            m = metrics(port)
            keys = ("gvfs_alloc_hot_path_total", "gvfs_errors_total", "gvfs_blobs_from_store_total",
                    "gvfs_blobs_proxied_total", "gvfs_blobs_missing_total")
            m = {k: m.get(k, 0.0) for k in keys}
            print(t, m)
            assert int(float(m["gvfs_alloc_hot_path_total"])) == 0, m
            assert int(float(m["gvfs_errors_total"])) == 0, m
    finally:
        for p, _ in servers.values():
            p.terminate()
            p.wait()
        shutil.rmtree(scratch, ignore_errors=True)
    print("OK", json.dumps(checked))


if __name__ == "__main__":
    main()
