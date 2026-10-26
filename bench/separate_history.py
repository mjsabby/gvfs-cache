"""Validation of `--blobs separate` stores that hold the whole history (opus aa475fa and later).

Two runs, each judged by stock git:

  sg     SearchGold's main: 5,882,161 commits and trees and 3,342,769 blobs (its pointer
         files). A copy of the separate-mode genesis store (~/gvfs-bench/sep-sg/store, slabs
         hard-linked) gets the whole history mirrored from ~/SearchGold. Checks: blobs.bin
         complete; every blob of main listed exactly once, with git's size; segments a
         geometric progression; every blob pack through `index-pack --strict`; an edge with
         no upstream (nothing can be proxied) under `--alloc-guard strict` answering sampled
         GETs, POSTs and sizes byte for byte. Load: blob GETs and 4,000-blob POSTs against
         this store and the inline store of the same history (~/gvfs-bench/sg/opus2/store),
         same binary, CPU-pinned, rounds interleaved.
  synth  Real content with history: a synthetic corpus (text, binaries, large incompressible
         files) and commits of new versions before genesis; then the mirror runs while
         commits land and 15-minute pipeline runs seal them (reverts of pre-genesis commits,
         one against a bitmap upstream). Afterwards every blob of main is listed (copies
         counted: a window and the mirror may both copy an old version a revert brings back
         while the mirror is in flight) and an edge with no upstream returns every blob of
         main byte for byte (POST batches through `index-pack --strict`, ranges of a large
         blob).

    python3 bench/separate_history.py sg [--jobs 8]
    python3 bench/separate_history.py synth [--small 20000 --medium 200 --large 8 --history 30]

Results: bench/results/separate-history-<run>.json.
"""
import argparse
import hashlib
import json
import os
import random
import re
import shutil
import statistics
import struct
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from gvfslib import check_loose, clean_env, free_port, metrics, request, wait_http  # noqa: E402
from separate_seed import corpus  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "opus/target/release/gvfs-daemon")
LOADGEN = os.path.expanduser("~/gvfs-server-benchmark/target/loadgen/release/loadgen")
SERVER_CPUS = os.environ.get("SERVER_CPUS", "0-3,16-19")
LOADGEN_CPUS = os.environ.get("LOADGEN_CPUS", "8-15,24-31")
BENCH = os.path.expanduser("~/gvfs-bench")


def sh(cmd, input=None, check=True):
    p = subprocess.run(cmd, input=input, capture_output=True, env=clean_env())
    if check and p.returncode != 0:
        sys.exit(f"{' '.join(map(str, cmd[:4]))} failed ({p.returncode}):\n"
                 f"{p.stderr.decode(errors='replace')[-4000:]}")
    return p


def elapsed(text):
    """GNU time's wall clock ("h:mm:ss" or "m:ss.ss") in seconds."""
    v = re.search(r"Elapsed \(wall clock\) time \(h:mm:ss or m:ss\): ([\d:.]+)", text).group(1)
    secs = 0.0
    for part in v.split(":"):
        secs = secs * 60 + float(part)
    return secs


def daemon_rss_kib(text):
    """The `peak RSS` the daemon logs for itself, in KiB."""
    v, unit = re.search(r"peak RSS ([\d.]+) (\w+)", text).groups()
    return int(float(v) * {"B": 1 / 1024, "KiB": 1, "MiB": 1024, "GiB": 1 << 20}[unit])


def timed(cmd, log):
    """Runs `cmd` under GNU time; returns (seconds, peak RSS KiB, log text)."""
    t0 = time.time()
    with open(log, "wb") as f:
        p = subprocess.run(["/usr/bin/time", "-v"] + cmd, stdout=f, stderr=subprocess.STDOUT,
                           env=clean_env())
    secs = time.time() - t0
    text = open(log, errors="replace").read()
    if p.returncode != 0:
        sys.exit(f"{' '.join(cmd[:3])} failed:\n{text[-4000:]}")
    rss = int(re.search(r"Maximum resident set size \(kbytes\): (\d+)", text).group(1))
    return secs, rss, text


# ---------------------------------------------------------------- store formats

def blobs_bin(path):
    b = open(path, "rb").read()
    body, digest = b[:-32], b[-32:]
    assert hashlib.sha256(body).digest() == digest, "blobs.bin checksum"
    assert body[:8] == b"GVDBLOBM" and struct.unpack_from("<I", body, 8)[0] == 2, "blobs.bin v2"
    gen, flags = struct.unpack_from("<QI", body, 12)
    genesis = body[24:44].hex()

    def string(at):
        n = struct.unpack_from("<H", body, at)[0]
        return body[at + 2:at + 2 + n].decode(), at + 2 + n

    at = 44
    nseg = struct.unpack_from("<I", body, at)[0]
    at += 4
    segments = []
    for _ in range(nseg):
        sid, at = string(at)
        blobs, index_len, np_ = struct.unpack_from("<QQI", body, at)
        at += 20
        packs = []
        for _ in range(np_):
            p, at = string(at)
            packs.append(p)
        segments.append({"id": sid, "blobs": blobs, "index_len": index_len, "packs": packs})
    npk = struct.unpack_from("<I", body, at)[0]
    at += 4
    packs = []
    for _ in range(npk):
        pid, at = string(at)
        ln = struct.unpack_from("<Q", body, at)[0]
        nb = struct.unpack_from("<I", body, at + 28)[0]
        at += 32
        packs.append({"id": pid, "len": ln, "blobs": nb})
    assert at == len(body), "blobs.bin trailing bytes"
    return {"generation": gen, "mirrored": bool(flags & 1), "genesis": genesis,
            "segments": segments, "packs": packs}


def segment_records(path):
    """(id, size) of every record of a segment (`blobs/<id>.bidx`)."""
    b = open(path, "rb").read()
    assert b[:8] == b"GVDBLOBX", path
    count = struct.unpack_from("<I", b, 12)[0]
    at = struct.unpack_from("<Q", b, 64)[0]
    mv = memoryview(b)
    out = []
    for i in range(count):
        o = at + 48 * i
        out.append((bytes(mv[o:o + 20]).hex(), struct.unpack_from("<Q", b, o + 40)[0]))
    return out


def fold_set(counts):
    """opus `blobcopy::fold_set`: segments to merge to restore the geometric progression."""
    order = sorted(range(len(counts)), key=lambda i: counts[i])
    w = [max(1, counts[i]) for i in order]
    i = len(w) - 1
    while i > 0 and w[i] >= 2 * w[i - 1]:
        i -= 1
    split = i + 1 if i > 0 else 0
    total = sum(w[:split])
    while 0 < split < len(w) and w[split] < 2 * total:
        total += w[split]
        split += 1
    return order[:split] if split >= 2 else []


def git_blobs(git_dir, rev="main"):
    """{id: size} of every blob reachable from `rev`."""
    rl = subprocess.Popen(["git", "--git-dir", git_dir, "rev-list", "--objects", rev],
                          stdout=subprocess.PIPE, env=clean_env())
    cf = subprocess.Popen(["git", "--git-dir", git_dir, "cat-file",
                           "--batch-check=%(objectname) %(objecttype) %(objectsize)"],
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=clean_env())
    import threading

    def feed():
        for line in rl.stdout:
            cf.stdin.write(line[:40] + b"\n")
        cf.stdin.close()

    t = threading.Thread(target=feed)
    t.start()
    out = {}
    for line in cf.stdout:
        oid, kind, size = line.split()
        if kind == b"blob":
            out[oid.decode()] = int(size)
    t.join()
    assert rl.wait() == 0 and cf.wait() == 0
    return out


def check_store(store, want, index_pack_all=True, check_repo=None):
    """Everything about the store's blob side, against `want` ({id: size} from git)."""
    bm = blobs_bin(os.path.join(store, "blobs.bin"))
    listed, records = {}, 0
    for s in bm["segments"]:
        for oid, size in segment_records(os.path.join(store, "blobs", f"{s['id']}.bidx")):
            records += 1
            assert listed.setdefault(oid, size) == size, f"{oid}: two sizes"
    missing = [o for o in want if o not in listed]
    wrong = [o for o, s in want.items() if o in listed and listed[o] != s]
    extra = [o for o in listed if o not in want]
    indexed = sorted(p for s in bm["segments"] for p in s["packs"])
    packs_ok = indexed == sorted(p["id"] for p in bm["packs"])
    for p in bm["packs"]:
        path = os.path.join(store, "blobs", f"{p['id']}.pack")
        assert os.path.getsize(path) == p["len"], f"pack {p['id']} length"
    t0 = time.time()
    if index_pack_all:
        for p in bm["packs"]:
            idx = os.path.join(check_repo, "check.idx")
            sh(["git", "--git-dir", check_repo, "index-pack", "--strict", "-o", idx,
                os.path.join(store, "blobs", f"{p['id']}.pack")])
            os.unlink(idx)
    res = {
        "generation": bm["generation"],
        "complete": bm["mirrored"],
        "segments": len(bm["segments"]),
        "segment_blob_counts": sorted((s["blobs"] for s in bm["segments"]), reverse=True),
        "segments_geometric": fold_set([s["blobs"] for s in bm["segments"]]) == [],
        "packs": len(bm["packs"]),
        "pack_bytes": sum(p["len"] for p in bm["packs"]),
        "every_pack_indexed_by_one_segment": packs_ok,
        "git_blobs": len(want),
        "listed_distinct": len(listed),
        "records": records,
        "listed_twice": records - len(listed),
        "missing": len(missing),
        "wrong_size": len(wrong),
        "not_in_git": len(extra),
        "index_pack_strict_all": index_pack_all,
        "index_pack_s": round(time.time() - t0, 1),
    }
    print(json.dumps(res), flush=True)
    assert res["complete"] and not missing and not wrong and not extra and packs_ok, res
    assert res["segments_geometric"], res
    return res


# ---------------------------------------------------------------- edges

def serve(store, cache, log, cpus=None):
    port = free_port()
    cmd = [BIN, "serve", "--store", f"fs:{store}", "--bind", f"127.0.0.1:{port}",
           "--cache-dir", cache, "--alloc-guard", "strict", "--workers", "64"]
    if cpus:
        cmd = ["taskset", "-c", cpus] + cmd
    p = subprocess.Popen(cmd, env=clean_env(), stdout=subprocess.DEVNULL, stderr=open(log, "wb"))
    secs = wait_http(port, "/healthz", timeout=1800, proc=p)
    return p, port, secs


def stop(p):
    p.terminate()
    try:
        p.wait(timeout=30)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()


def post_pack(port, ids, check_repo):
    """POST ids, index the pack with `--strict` into `check_repo`; returns (bytes, secs)."""
    r = request(port, "POST", "/gvfs/objects", body={"objectIds": ids, "commitDepth": 1},
                headers={"Accept": "application/x-git-packfile"}, timeout=3600)
    assert r.status == 200, (r.status, r.body[:200])
    p = subprocess.run(["git", "--git-dir", check_repo, "index-pack", "--stdin", "--strict"],
                       input=r.body, capture_output=True, env=clean_env())
    assert p.returncode == 0, p.stderr.decode(errors="replace")[-2000:]
    return len(r.body), r.secs


def edge_counters(port):
    m = metrics(port)
    keys = ["gvfs_alloc_hot_path_total", "gvfs_blobs_missing_total", "gvfs_blobs_proxied_total",
            "gvfs_blobs_unavailable_total", "gvfs_blobs_from_store_total", "gvfs_errors_total"]
    return {k: int(m.get(k, -1)) for k in keys}


def edge_sample(store, want, W, git_dir, n_get=3000, n_post=4000):
    """Sampled GETs, a POST and sizes through an edge with no upstream, judged by git."""
    check = os.path.join(W, "edge-check.git")
    shutil.rmtree(check, ignore_errors=True)
    sh(["git", "init", "-q", "--bare", check])
    p, port, ready = serve(store, os.path.join(W, "edge-cache"), os.path.join(W, "edge.log"))
    try:
        rnd = random.Random(5)
        ids = sorted(want)
        gets = rnd.sample(ids, min(n_get, len(ids)))
        t0 = time.time()
        for oid in gets:
            r = request(port, "GET", f"/gvfs/objects/{oid}")
            assert r.status == 200, (oid, r.status)
            kind, content = check_loose(oid, r.body)
            assert kind == "blob" and len(content) == want[oid], oid
        t_get = time.time() - t0
        posts = rnd.sample(ids, min(n_post, len(ids)))
        nbytes, t_post = post_pack(port, posts, check)
        got = sh(["git", "--git-dir", check, "cat-file",
                  "--batch-check=%(objectname) %(objecttype) %(objectsize)"],
                 input="\n".join(posts).encode()).stdout.decode().split("\n")
        for line in filter(None, got):
            oid, kind, size = line.split()
            assert kind == "blob" and int(size) == want[oid], line
        sized = rnd.sample(ids, min(4000, len(ids)))
        r = request(port, "POST", "/gvfs/sizes", body=sized)
        assert r.status == 200
        answered = {e["Id"]: e["Size"] for e in json.loads(r.body)}
        assert answered == {o: want[o] for o in sized}, "sizes differ"
        counters = edge_counters(port)
    finally:
        stop(p)
    assert counters["gvfs_alloc_hot_path_total"] == 0 and counters["gvfs_blobs_missing_total"] == 0, counters
    assert counters["gvfs_blobs_proxied_total"] == 0 and counters["gvfs_errors_total"] == 0, counters
    res = {"ready_s": round(ready, 2), "get_sampled": len(gets), "get_s": round(t_get, 2),
           "post_blobs": len(posts), "post_bytes": nbytes, "post_s": round(t_post, 3),
           "sizes_checked": len(sized), "counters": counters}
    print(json.dumps(res), flush=True)
    return res


def loadgen(port, args, conns, out, warmup=2.0, duration=10.0):
    cmd = ["taskset", "-c", LOADGEN_CPUS, LOADGEN, "--addr", f"127.0.0.1:{port}", "--conns", str(conns),
           "--warmup", str(warmup), "--duration", str(duration), "--threads", "16", "--timeout", "60",
           "--json", out] + args
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=duration + 300)
    return json.load(open(out))


def load_compare(stores, oids, W, rounds=3):
    """Blob GETs and 4,000-blob POSTs, the same ids against each store; rounds interleaved."""
    work = [("get_blob", ["--workload", "get-object", "--oids", oids], 64),
            ("post_4000_blobs", ["--workload", "post-objects", "--oids", oids, "--batch", "4000"], 32)]
    raw = {name: {w: [] for w, _, _ in work} for name in stores}
    for rnd in range(rounds):
        order = list(stores)
        random.Random(rnd).shuffle(order)
        for name in order:
            p, port, _ = serve(stores[name], os.path.join(W, f"load-cache-{name}"),
                               os.path.join(W, f"load-{name}.log"), cpus=SERVER_CPUS)
            try:
                for w, args, conns in work:
                    r = loadgen(port, args, conns, os.path.join(W, f"load-{name}-{w}-{rnd}.json"))
                    raw[name][w].append(r)
                    print(f"  r{rnd} {name:9s} {w:16s} ok/s={r['ok_rps']:>10.1f} MB/s={r['mbps']:>8.1f} "
                          f"p50={r['lat_us']['p50']}us p99={r['lat_us']['p99']}us st={r['statuses']}",
                          flush=True)
                assert edge_counters(port)["gvfs_alloc_hot_path_total"] == 0
            finally:
                stop(p)
    out = {}
    for name, ws in raw.items():
        out[name] = {}
        for w, rs in ws.items():
            ok = [set(r["statuses"]) <= {"200"} for r in rs]
            out[name][w] = {
                "ok_rps": round(statistics.median(r["ok_rps"] for r in rs), 1),
                "MBps": round(statistics.median(r["mbps"] for r in rs), 1),
                "p50_us": statistics.median(r["lat_us"]["p50"] for r in rs),
                "p99_us": statistics.median(r["lat_us"]["p99"] for r in rs),
                "all_200": all(ok),
            }
    return out


# ---------------------------------------------------------------- runs

def run_sg(a):
    W = os.path.join(BENCH, "sep-sg-history")
    shutil.rmtree(W, ignore_errors=True)
    store = os.path.join(W, "store")
    os.makedirs(os.path.join(store, "slabs"))
    src = os.path.join(BENCH, "sep-sg/store")
    for f in os.listdir(os.path.join(src, "slabs")):
        os.link(os.path.join(src, "slabs", f), os.path.join(store, "slabs", f))
    for f in ("manifest.bin", "blobs.bin"):
        shutil.copy(os.path.join(src, f), os.path.join(store, f))
    up = os.path.expanduser("~/SearchGold")
    secs, rss, log = timed([BIN, "mirror", "--store", f"fs:{store}", "--upstream", up, "--jobs", str(a.jobs),
                            "--blob-pack-mib", str(a.pack_mib), "--publish-secs", "10",
                            "--work-dir", os.path.join(W, "mw"), "--threads", "16"],
                           os.path.join(W, "mirror.log"))
    planned = float(re.search(r"planned in ([\d.]+)s", log).group(1))
    items = int(re.search(r"to copy in (\d+) items", log).group(1))
    folds = len(re.findall(r"blobs.bin: folded", log))
    publishes = len(re.findall(r"mirror: \d+/\d+ items", log))
    print(f"mirror: {secs:.1f}s, planned in {planned}s, {items} items, {publishes} publishes, "
          f"{folds} folds, peak RSS {rss // 1024} MiB", flush=True)
    t0 = time.time()
    want = git_blobs(os.path.join(up, ".git"))
    t_git = time.time() - t0
    check_repo = os.path.join(W, "check.git")
    sh(["git", "init", "-q", "--bare", check_repo])
    store_res = check_store(store, want, check_repo=check_repo)
    edge = edge_sample(store, want, W, os.path.join(up, ".git"))
    load = load_compare({"separate": store, "inline": os.path.join(BENCH, "sg/opus2/store")},
                        os.path.join(BENCH, "sg/oids/blobs.txt"), W, rounds=a.rounds) if a.rounds else None
    res = {"mirror": {"s": round(secs, 1), "planned_s": planned, "items": items, "publishes": publishes,
                      "folds": folds, "peak_rss_mib": rss // 1024, "jobs": a.jobs, "blob_pack_mib": a.pack_mib,
                      "blobs_per_s": round(len(want) / secs)},
           "git_rev_list_s": round(t_git, 1), "store": store_res, "edge": edge, "load": load}
    return res


def run_synth(a):
    W = os.path.join(BENCH, "sep-synth-history")
    shutil.rmtree(W, ignore_errors=True)
    os.makedirs(W)
    up, tree, store = os.path.join(W, "up.git"), os.path.join(W, "tree"), os.path.join(W, "store")
    sh(["git", "init", "-q", "--bare", "-b", "main", up])
    for k, v in [("core.compression", "1"), ("pack.compression", "1"), ("gc.auto", "0")]:
        sh(["git", "--git-dir", up, "config", k, v])
    G = ["git", "--git-dir", up, "--work-tree", tree, "-c", "user.name=b", "-c", "user.email=b@b"]

    def commit(msg):
        sh(G + ["add", "-A"])
        sh(G + ["commit", "-q", "-m", msg])
        return sh(G + ["rev-parse", "HEAD"]).stdout.decode().strip()

    t0 = time.time()
    sizes = corpus(tree, a.small, a.medium, a.large)
    commit("corpus")
    rnd = random.Random(11)
    files = lambda top: sorted(os.path.join(dp, f) for dp, _, fs in os.walk(os.path.join(tree, top)) for f in fs)
    small, medium, large = files("src"), files("bin"), files("data")
    pre, large_changes = [], []
    for k in range(a.history):
        for f in rnd.sample(small, max(1, len(small) // 100)):
            with open(f, "ab") as fh:
                fh.write(f"edit {k}\n".encode())
        for f in rnd.sample(medium, min(2, len(medium))):
            open(f, "wb").write(os.urandom(rnd.randint(1 << 20, 4 << 20)))
        if k % 10 == 9 and large:
            path = rnd.choice(large)
            with open(path, "wb") as fh:
                for _ in range(rnd.randint(2, 4)):
                    fh.write(os.urandom(32 << 20))
            large_changes.append((k, os.path.relpath(path, tree)))
        pre.append(commit(f"h{k}"))
    sh(["git", "--git-dir", up, "repack", "-adq"])
    t_build = time.time() - t0
    history_bytes = sum(git_blobs(up).values())
    print(f"corpus + {a.history} commits in {t_build:.0f}s; main's blobs hold {history_bytes / 1e9:.2f} GB",
          flush=True)

    def pipeline(name):
        return timed([BIN, "pipeline", "--store", f"fs:{store}", "--upstream", up, "--tier", "15m",
                      "--work-dir", os.path.join(W, "pw"), "--threads", "16"] +
                     (["--blobs", "separate"] if name == "genesis" else []),
                     os.path.join(W, f"pipeline-{name}.log"))

    t_genesis, rss_genesis, _ = pipeline("genesis")
    mlog = os.path.join(W, "mirror.log")
    m = subprocess.Popen(["/usr/bin/time", "-v", BIN, "mirror", "--store", f"fs:{store}", "--upstream", up,
                          "--jobs", str(a.jobs), "--blob-pack-mib", str(a.pack_mib), "--publish-secs", "5",
                          "--work-dir", os.path.join(W, "mw"), "--threads", "16"] +
                         (["--rate-mbps", str(a.rate_mbps)] if a.rate_mbps else []),
                         stdout=open(mlog, "wb"), stderr=subprocess.STDOUT, env=clean_env())
    # Commits land while the mirror runs; each is sealed by a 15-minute run. Two bring back
    # versions from before genesis (as reverts do), restored with `checkout <old> -- path`.
    windows = []
    old_dir = os.path.relpath(os.path.dirname(medium[0]), tree) if medium else None
    for w in range(a.windows):
        for f in rnd.sample(small, max(1, len(small) // 200)):
            with open(f, "ab") as fh:
                fh.write(f"window {w}\n".encode())
        if w == 1 and large_changes:
            # A large file's version from before its last change (plain upstream: the new
            # directory tree naming it is in the window's pack).
            k, path = large_changes[-1]
            sh(G + ["checkout", pre[k] + "^", "--", path])
        elif w == 3 and old_dir:
            # A whole directory as it was early on, against a bitmap upstream: its old tree is
            # one the haves reach, so the window's pack leaves it out.
            sh(["git", "--git-dir", up, "repack", "-adbq"])
            sh(G + ["checkout", pre[min(2, len(pre) - 1)], "--", old_dir])
        commit(f"w{w}")
        running = m.poll() is None
        secs, rss, wlog = pipeline(f"w{w}")
        copied = re.search(r"copied (\d+) blobs \(([^)]+)\)", wlog)
        windows.append({"s": round(secs, 1), "peak_rss_mib": rss // 1024, "mirror_running_at_start": running,
                        "copied_blobs": int(copied.group(1)) if copied else 0,
                        "copied": copied.group(2) if copied else "0 B"})
    rc = m.wait()
    log = open(mlog, errors="replace").read()
    assert rc == 0, log[-4000:]
    t_mirror = elapsed(log)
    # The daemon's own peak (its log); GNU time's includes the local git upstream's processes.
    rss_mirror = daemon_rss_kib(log)
    skipped = [int(x) for x in re.findall(r"(\d+) items listed meanwhile, skipped", log)]
    pipeline("final")
    want = git_blobs(up)
    check_repo = os.path.join(W, "check.git")
    sh(["git", "init", "-q", "--bare", check_repo])
    store_res = check_store(store, want, check_repo=check_repo)

    # Every blob of main through an edge with no upstream, byte for byte.
    edge_repo = os.path.join(W, "edge-all.git")
    sh(["git", "init", "-q", "--bare", edge_repo])
    p, port, ready = serve(store, os.path.join(W, "edge-cache"), os.path.join(W, "edge.log"))
    try:
        ids = sorted(want, key=lambda o: want[o])
        batches, cur, cur_bytes = [], [], 0
        for o in ids:
            if cur and (len(cur) >= 4000 or cur_bytes + want[o] > (1 << 30)):
                batches.append(cur)
                cur, cur_bytes = [], 0
            cur.append(o)
            cur_bytes += want[o]
        if cur:
            batches.append(cur)
        t0 = time.time()
        sent = 0
        for b in batches:
            n, _ = post_pack(port, b, edge_repo)
            sent += n
        t_all = time.time() - t0
        got = sh(["git", "--git-dir", edge_repo, "cat-file", "--batch-check=%(objectname) %(objectsize)"],
                 input="\n".join(ids).encode()).stdout.decode().split("\n")
        back = dict(line.split() for line in got if line and "missing" not in line)
        assert {o: int(s) for o, s in back.items()} == want, "an edge POST lost or changed a blob"
        big = ids[-1]
        content = sh(["git", "--git-dir", up, "cat-file", "blob", big]).stdout
        r2 = random.Random(3)
        for _ in range(8):
            off = r2.randrange(0, len(content))
            ln = r2.randint(1, 8 << 20)
            r = request(port, "GET", f"/gvfs/blobs/{big}?offset={off}&length={ln}")
            assert r.status == 200 and r.body == content[off:off + ln], (off, ln, r.status)
        counters = edge_counters(port)
    finally:
        stop(p)
    assert counters["gvfs_alloc_hot_path_total"] == 0 and counters["gvfs_errors_total"] == 0, counters
    content_bytes = sum(want.values())
    return {
        "corpus": {"files": a.small + a.medium + a.large, "by_class": sizes, "history_commits": a.history,
                   "build_s": round(t_build, 1)},
        "main_blobs": len(want), "main_blob_bytes": content_bytes,
        "genesis": {"s": round(t_genesis, 1), "peak_rss_mib": rss_genesis // 1024},
        "mirror": {"s": round(t_mirror, 1), "MBps": round(content_bytes / t_mirror / 1e6, 1),
                   "peak_rss_mib": rss_mirror // 1024, "jobs": a.jobs, "blob_pack_mib": a.pack_mib,
                   "rate_mbps_cap": a.rate_mbps,
                   "items_skipped_listed_meanwhile": max(skipped) if skipped else 0,
                   "planned_s": float(re.search(r"planned in ([\d.]+)s", log).group(1))},
        "windows_during_mirror": windows,
        "store": store_res,
        "edge": {"ready_s": round(ready, 2), "post_all_blobs": len(ids), "post_batches": len(batches),
                 "post_bytes": sent, "post_s": round(t_all, 1), "ranges_checked": 8, "counters": counters},
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run", choices=["sg", "synth"])
    ap.add_argument("--jobs", type=int, default=8)
    ap.add_argument("--pack-mib", type=int, default=0, help="--blob-pack-mib (default: 32 sg, 256 synth)")
    ap.add_argument("--rounds", type=int, default=3, help="sg: load rounds (0: skip)")
    ap.add_argument("--small", type=int, default=20000)
    ap.add_argument("--medium", type=int, default=200)
    ap.add_argument("--large", type=int, default=8)
    ap.add_argument("--history", type=int, default=30)
    ap.add_argument("--windows", type=int, default=6)
    ap.add_argument("--rate-mbps", type=int, default=400,
                    help="synth: cap the mirror so that windows land while it runs (0: none)")
    a = ap.parse_args()
    a.pack_mib = a.pack_mib or (32 if a.run == "sg" else 256)
    res = run_sg(a) if a.run == "sg" else run_synth(a)
    res["binary_commit"] = subprocess.run(["git", "-C", ROOT, "rev-parse", "--short", "HEAD"],
                                          capture_output=True, text=True).stdout.strip()
    print(json.dumps(res, indent=2))
    os.makedirs(os.path.join(ROOT, "bench/results"), exist_ok=True)
    json.dump(res, open(os.path.join(ROOT, f"bench/results/separate-history-{a.run}.json"), "w"), indent=2)


if __name__ == "__main__":
    main()
