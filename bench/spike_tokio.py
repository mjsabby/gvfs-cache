#!/usr/bin/env python3
"""The edge's connection layer, A/B: the thread-per-request edge (`threads`, before the port),
the tokio edge (`tokio`), and the spike's variants (`examples/spike_tokio.rs`: `own`,
`hyper`, `own-blk`, `own-nice`, `own-yield`), on one store.

    python3 bench/spike_tokio.py check --dir ~/gvfs-bench/prof/sg7-sep
    python3 bench/spike_tokio.py bench --dir ~/gvfs-bench/prof/sg7-sep --rounds 3
    python3 bench/spike_tokio.py bench --dir ~/gvfs-bench/prof/sg7-sep \
        --variants threads,own,hyper,own-blk,own-nice,own-yield     # REPORT.md §14's spike

`check`: the tokio edge and each spike variant answer the same requests with the same
status, content type, length and body as the thread-per-request edge: config, blobs (GET), commits and trees (GET, whole
entries), unknown ids, up-to-date prefetches, cold and warm prefetches with and without
sizes and `.idx`, sizes batches.

`bench`: per workload, every variant in turn (the order rotating each round): edge CPU per
request, req/s, p50/p99 latency, allocations per request (`gvfs_alloc_post_seal_total`).
Then a GET load beside slow downloaders (clients reading a cold prefetch at 80 KB/s), and
beside idle keep-alive connections (with the edge's resident memory).

`--dir` holds `store/`, `up.git` and `blobs.txt` (bench/build_store.py). The edge runs on
SERVER_CPUS, loadgen on LOADGEN_CPUS, this script (slow and idle clients) on CLIENT_CPUS.
"""
import argparse
import collections
import hashlib
import json
import os
import random
import resource
import shutil
import socket
import statistics
import struct
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(__file__))
from gvfslib import clean_env, free_port, metrics, request, wait_http  # noqa: E402
from profile_bpf import LOADGEN_CPUS, OUT, SERVER_CPUS, cpu_seconds, loadgen_starter, wait_load  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# `threads`: the thread-per-request edge (before the tokio port, commit 5dada58); `tokio`: the
# edge as built now. Both run `gvfs-daemon serve` (env THREADS_BIN / TOKIO_BIN).
THREADS_BIN = os.environ.get("THREADS_BIN", os.path.expanduser("~/gvfs-bench/prof/bin/gvfs-daemon-threads"))
TOKIO_BIN = os.environ.get("TOKIO_BIN", os.path.join(REPO, "target/release/gvfs-daemon"))
SPIKE_BIN = os.path.join(REPO, "target/release/examples/spike_tokio")
DAEMONS = {"threads": THREADS_BIN, "tokio": TOKIO_BIN}
CLIENT_CPUS = os.environ.get("CLIENT_CPUS", "4-7,20-23")
GIT_UA = "git/2.53.0.vfs.0.0"
UP_TO_DATE = 9999999999


def cpus(spec):
    out = set()
    for part in spec.split(","):
        lo, _, hi = part.partition("-")
        out.update(range(int(lo), int(hi or lo) + 1))
    return out


def command(variant, store, port, cache, idle):
    bind = f"127.0.0.1:{port}"
    if variant in DAEMONS:
        cmd = [DAEMONS[variant], "serve", "--store", f"fs:{store}", "--bind", bind, "--cache-dir", cache,
               "--alloc-guard", "count", "--idle-secs", str(idle)]
        # The runtime's threads: one per edge CPU (the thread-per-request edge keeps its 256);
        # TOKIO_EXTRA: more flags (e.g. `--quantum-us 50`).
        extra = ["--workers", str(len(cpus(SERVER_CPUS)))] + os.environ.get("TOKIO_EXTRA", "").split()
        return cmd + (extra if variant == "tokio" else [])
    mode, _, heavy = variant.partition("-")
    cmd = [SPIKE_BIN, "--mode", mode, "--store", f"fs:{store}", "--bind", bind, "--cache-dir", cache,
           "--threads", str(len(cpus(SERVER_CPUS))), "--idle-secs", str(idle)]
    return cmd + (["--heavy", {"blk": "blocking", "nice": "nice", "yield": "yield"}[heavy]] if heavy else [])


def start(variant, store, idle=30):
    port = free_port()
    cache = os.path.join(OUT, f"spike-cache-{variant}")
    shutil.rmtree(cache, ignore_errors=True)
    log = open(os.path.join(OUT, f"spike-{variant}.log"), "wb")
    p = subprocess.Popen(["taskset", "-c", SERVER_CPUS] + command(variant, store, port, cache, idle),
                         stdout=subprocess.DEVNULL, stderr=log, env=clean_env())
    wait_http(port, "/healthz", timeout=1800, proc=p)
    return p, port


def stop(servers):
    for p, _ in servers.values():
        p.terminate()
    for p, _ in servers.values():
        p.wait()


# ------------------------------------------------------------------------------- check

def stream(port, path, headers):
    """Status, the headers that matter, length and SHA-256 of a (large) body, streamed."""
    import http.client
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=3600)
    try:
        c.request("GET", path, headers=headers)
        r = c.getresponse()
        h, n, first = hashlib.sha256(), 0, b""
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            if not first:
                first = chunk[:32]
            h.update(chunk)
            n += len(chunk)
        keep = ("content-type", "content-length", "x-gvfs-pack-start", "x-gvfs-pack-end", "x-gvfs-sizes")
        hdrs = {k.lower(): v for k, v in r.getheaders() if k.lower() in keep}
        return {"status": r.status, "headers": hdrs, "len": n, "sha256": h.hexdigest()}, first
    finally:
        c.close()


def check(a):
    store, up = os.path.join(a.dir, "store"), os.path.join(a.dir, "up.git")
    blobs = [x for x in open(os.path.join(a.dir, "blobs.txt")).read().split() if x]
    variants = ("threads", "tokio", "own", "hyper")
    servers = {v: start(v, store) for v in variants}
    rnd = random.Random(7)
    reqs = [("GET", "/gvfs/config", None, {}), ("GET", "/healthz", None, {})]
    for h in ({}, {"X-GVFS-Sizes": "1"}):
        reqs.append(("GET", f"/gvfs/prefetch?lastPackTimestamp={UP_TO_DATE}", None, h))
    reqs += [("GET", f"/gvfs/objects/{o}", None, {}) for o in rnd.sample(blobs, min(4000, len(blobs)))]
    commits = subprocess.run(["git", "--git-dir", up, "rev-list", "--first-parent", "-n", "300", "HEAD"],
                             capture_output=True, text=True, check=True, env=clean_env()).stdout.split()
    trees = subprocess.run(["git", "--git-dir", up, "rev-parse"] + [c + "^{tree}" for c in commits],
                           capture_output=True, text=True, check=True, env=clean_env()).stdout.split()
    reqs += [("GET", f"/gvfs/objects/{o}", None, {}) for o in commits + trees]
    reqs.append(("GET", "/gvfs/objects/" + "0" * 40, None, {}))
    for n in (1, 2, 17, 100, 1000, 4000, 4000):
        body = json.dumps([rnd.choice(blobs) for _ in range(n)]).encode()
        reqs.append(("POST", "/gvfs/sizes", body, {"Content-Type": "application/json"}))
    reqs.append(("POST", "/gvfs/sizes", json.dumps(commits[:50] + trees[:50]).encode(), {}))
    reqs.append(("POST", "/gvfs/sizes", json.dumps(blobs[:5] + ["1" * 40]).encode(), {}))
    stats = collections.Counter()
    bad = []
    try:
        for method, path, body, headers in reqs:
            rs = {v: request(port, method, path, body=body, headers=headers) for v, (_, port) in servers.items()}
            base = rs["threads"]
            for v in variants[1:]:
                r = rs[v]
                if r.status == 404 and b"delta: not in the spike" in r.body:
                    stats[f"{v} delta (not served by the spike)"] += 1
                    continue
                same = (r.status, r.headers.get("content-type"), r.body) == \
                    (base.status, base.headers.get("content-type"), base.body)
                stats[f"{v} {'same' if same else 'DIFFERENT'} {method} {base.status}"] += 1
                if not same:
                    bad.append((v, method, path, base.status, r.status, len(base.body), len(r.body)))
        # The tip's timestamp, from the cold pack's GPRE header.
        _, first = stream(servers["threads"][1], "/gvfs/prefetch?lastPackTimestamp=0", {"User-Agent": GIT_UA})
        tip = int.from_bytes(first[8:16], "little")
        git_sizes = {"User-Agent": GIT_UA, "X-GVFS-Sizes": "1"}
        streams = [(0, git_sizes), (0, {"User-Agent": GIT_UA}), (0, {"User-Agent": "GVFS/1.0"}),
                   (tip - 3600, git_sizes), (tip - 86400, git_sizes), (tip - 3 * 86400, {"User-Agent": GIT_UA}),
                   (tip - 7 * 86400, git_sizes)]
        for since, headers in streams:
            path = f"/gvfs/prefetch?lastPackTimestamp={since}"
            got = {v: stream(port, path, headers)[0] for v, (_, port) in servers.items()}
            for v in variants[1:]:
                same = got[v] == got["threads"]
                stats[f"{v} {'same' if same else 'DIFFERENT'} prefetch stream"] += 1
                if not same:
                    bad.append((v, "GET", path, got["threads"], got[v]))
            print(f"prefetch since {since} {headers}: {got['threads']['len']:,} bytes "
                  f"{got['threads']['sha256'][:16]}", flush=True)
    finally:
        stop(servers)
    for k, n in sorted(stats.items()):
        print(f"  {n:6d}  {k}")
    for b in bad[:20]:
        print("  MISMATCH", b)
    print("check:", "OK" if not bad else f"{len(bad)} mismatches")
    return 0 if not bad else 1


# ------------------------------------------------------------------------------- bench

def allocs(port):
    return metrics(port).get("gvfs_alloc_post_seal_total", float("nan"))


def rss_mb(pid, key="VmRSS:"):
    """Resident memory (`RssAnon:`: heap, stacks and buffers, not mapped store files)."""
    for line in open(f"/proc/{pid}/status"):
        if line.startswith(key):
            return int(line.split()[1]) / 1024
    return float("nan")


def run_loads(port, pid, loads, secs):
    """`loads` ([(name, loadgen args, conns)]) started in order, the first given a head start
    (sizes bodies take a moment to build); edge CPU and allocations over `secs` once busy."""
    dones = []
    for i, (name, args, conns) in enumerate(loads):
        extra = 6 * (len(loads) - 1 - i)
        dones.append((name, loadgen_starter(port, args, conns, secs + extra)()))
        if i + 1 < len(loads):
            wait_load(pid)
    wait_load(pid)
    a0, c0, t0 = allocs(port), cpu_seconds(pid), time.time()
    time.sleep(secs)
    c1, t1, a1 = cpu_seconds(pid), time.time(), allocs(port)
    out = {name: done() for name, done in dones}
    rps = sum(o["ok_rps"] for o in out.values() if o)
    mbps = sum(o["MBps"] for o in out.values() if o)
    cpu = (c1 - c0) / (t1 - t0)
    return {"loads": out, "edge_cpus": round(cpu, 2),
            "cpu_us_per_req": round(1e6 * cpu / max(rps, 1e-9), 2),
            "cpu_ms_per_gb": round(1e6 * cpu / max(mbps, 1e-9), 1),
            "allocs_per_req": round((a1 - a0) / max(rps * (t1 - t0), 1e-9), 2)}


def workloads(blobs):
    get = ["--workload", "get-object", "--oids", blobs]
    sizes = ["--workload", "sizes", "--oids", blobs, "--batch", "4000", "--bodies", "512"]
    up = ["--workload", "prefetch", "--ts", str(UP_TO_DATE)]
    cold = ["--workload", "prefetch", "--ts", "0", "--header", "X-GVFS-Sizes: 1", "--header", f"User-Agent: {GIT_UA}"]
    w = {}
    for c in (64, 256, 1024):
        w[f"config_{c}"] = [("config", ["--workload", "config"], c)]
        w[f"get_blob_{c}"] = [("get", get, c)]
        w[f"uptodate_{c}"] = [("uptodate", up, c)]
    w["cold_clone_8"] = [("cold", cold, 8)]
    w["sizes_4000_32"] = [("sizes", sizes, 32)]
    w["mixed_get64_sizes32"] = [("sizes", sizes, 32), ("get", get, 64)]
    # POST /gvfs/objects (not in the spike: `gvfs-daemon serve` only).
    post = ["--workload", "post-objects", "--oids", blobs, "--batch", "4000", "--bodies", "512"]
    w["post_4000_32"] = [("post", post, 32)]
    w["mixed_get64_post32"] = [("post", post, 32), ("get", get, 64)]
    return w


def runs(variant, work):
    """Whether `variant` serves workload `work`."""
    if work.startswith(("post", "mixed_get64_post")):
        return variant in DAEMONS
    return "-" not in variant or "sizes" in work


def slow_reader(port, stop_evt, started):
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16384)
    try:
        s.connect(("127.0.0.1", port))
        s.sendall(f"GET /gvfs/prefetch?lastPackTimestamp=0 HTTP/1.1\r\nHost: x\r\nUser-Agent: {GIT_UA}\r\n"
                  f"X-GVFS-Sizes: 1\r\n\r\n".encode())
        got = 0
        while not stop_evt.is_set():
            d = s.recv(4096)
            if not d:
                break
            if not got:
                started.append(1)
            got += len(d)
            time.sleep(0.05)
    except OSError:
        pass
    finally:
        s.close()


def slow_test(port, pid, get, secs, n):
    stop_evt, started = threading.Event(), []
    ts = [threading.Thread(target=slow_reader, args=(port, stop_evt, started), daemon=True) for _ in range(n)]
    for t in ts:
        t.start()
    time.sleep(5)
    res = run_loads(port, pid, [("get", get, 64)], secs)
    res["slow_receiving"] = len(started)
    stop_evt.set()
    for t in ts:
        t.join(timeout=10)
    return res


IDLE_RUNS = iter(range(1, 255))


def idle_test(port, pid, get, secs, n):
    net = next(IDLE_RUNS)  # fresh source addresses per run: 127.<run>.<k>.2
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    rss0, anon0 = rss_mb(pid), rss_mb(pid, "RssAnon:")
    socks = []
    try:
        for i in range(n):
            s = socket.socket()
            # The port is picked at connect (by the whole 4-tuple), not at bind: a bind to
            # port 0 only gets half the ephemeral range.
            s.setsockopt(socket.IPPROTO_IP, 24, 1)  # IP_BIND_ADDRESS_NO_PORT
            s.bind((f"127.{net}.{i // 10000}.2", 0))
            s.connect(("127.0.0.1", port))
            socks.append(s)
        for s in socks:
            s.sendall(b"GET /gvfs/config HTTP/1.1\r\nHost: x\r\n\r\n")
        for s in socks:
            buf = b""
            while b"\r\n\r\n" not in buf:
                buf += s.recv(4096)
            head, _, body = buf.partition(b"\r\n\r\n")
            length = int([x for x in head.split(b"\r\n") if x.lower().startswith(b"content-length")][0].split(b":")[1])
            while len(body) < length:
                body += s.recv(4096)
        time.sleep(2)
        rss1, anon1 = rss_mb(pid), rss_mb(pid, "RssAnon:")
        res = run_loads(port, pid, [("get", get, 64)], secs)
        res.update({"idle_conns": n, "rss_before_mb": round(rss0, 1), "rss_idle_mb": round(rss1, 1),
                    "anon_before_mb": round(anon0, 1), "anon_idle_mb": round(anon1, 1),
                    "kb_per_idle_conn": round(1024 * (anon1 - anon0) / n, 2)})
        return res
    finally:
        for s in socks:
            # A reset, not a FIN: no TIME_WAIT left holding the client's ports.
            s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            s.close()
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))


def bench(a):
    store, blobs = os.path.join(a.dir, "store"), os.path.join(a.dir, "blobs.txt")
    variants = a.variants.split(",")
    work = workloads(blobs)
    names = [n for n in work if not a.only or n in a.only.split(",")]
    results = []
    out_path = os.path.join(OUT, "spike.json")

    def record(r):
        results.append(r)
        print(json.dumps(r), flush=True)
        json.dump(results, open(out_path, "w"), indent=1)

    servers = {v: start(v, store, idle=600) for v in variants}
    try:
        for rnd in range(a.rounds):
            for n in names:
                k = rnd % len(variants)
                for v in variants[k:] + variants[:k]:
                    if not runs(v, n):
                        continue
                    p, port = servers[v]
                    res = run_loads(port, p.pid, work[n], a.secs)
                    record({"work": n, "variant": v, "round": rnd, "rss_mb": round(rss_mb(p.pid), 1),
                            "anon_mb": round(rss_mb(p.pid, "RssAnon:"), 1), **res})
        get = ["--workload", "get-object", "--oids", blobs]
        for rnd in range(a.extra_rounds):
            for v in [x for x in variants if "-" not in x]:
                p, port = servers[v]
                if a.slow:
                    record({"work": f"slow{a.slow}_get64", "variant": v, "round": rnd,
                            **slow_test(port, p.pid, get, a.secs, a.slow)})
                if a.idle:
                    record({"work": f"idle{a.idle}_get64", "variant": v, "round": rnd,
                            **idle_test(port, p.pid, get, a.secs, a.idle)})
    finally:
        stop(servers)
    summarize(results)


def summarize(results):
    by = collections.defaultdict(list)
    for r in results:
        by[(r["work"], r["variant"])].append(r)
    print(f"\n{'workload':22s} {'variant':9s} {'req/s':>11s} {'p50us':>7s} {'p99us':>8s} {'cpus':>5s} "
          f"{'cpu-us/req':>10s} {'allocs/req':>10s}  extra")
    for (w, v), xs in sorted(by.items()):
        def med(f):
            vals = [f(x) for x in xs if f(x) is not None]
            return statistics.median(vals) if vals else float("nan")
        main = "get" if any("get" in x["loads"] for x in xs) else next(iter(xs[0]["loads"]))

        def load(x, key):
            o = x["loads"].get(main)
            return o.get(key) if o else 0
        extra = ""
        for heavy in ("sizes", "post"):
            if heavy in xs[0]["loads"] and main != heavy:
                extra = f"{heavy} {med(lambda x: x['loads'][heavy]['ok_rps'] if x['loads'].get(heavy) else 0):.0f}/s " \
                        f"p99 {med(lambda x: x['loads'][heavy]['p99_us'] if x['loads'].get(heavy) else 0):.0f}us"
        if "kb_per_idle_conn" in xs[0]:
            extra = f"anon rss {med(lambda x: x['anon_before_mb']):.0f} -> {med(lambda x: x['anon_idle_mb']):.0f} MB, " \
                    f"{med(lambda x: x['kb_per_idle_conn']):.1f} KB/conn"
        if w.startswith("cold"):
            extra = f"{med(lambda x: x['cpu_ms_per_gb']):.0f} CPU-ms/GB, {med(lambda x: load(x, 'MBps')):.0f} MB/s"
        if "slow_receiving" in xs[0]:
            extra = f"slow clients receiving: {med(lambda x: x['slow_receiving']):.0f}"
        print(f"{w:22s} {v:9s} {med(lambda x: load(x, 'ok_rps')):11.0f} {med(lambda x: load(x, 'p50_us')):7.0f} "
              f"{med(lambda x: load(x, 'p99_us')):8.0f} {med(lambda x: x['edge_cpus']):5.2f} "
              f"{med(lambda x: x['cpu_us_per_req']):10.2f} {med(lambda x: x['allocs_per_req']):10.2f}  {extra}")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check")
    c.add_argument("--dir", required=True)
    b = sub.add_parser("bench")
    b.add_argument("--dir", required=True)
    b.add_argument("--variants", default="threads,tokio")
    b.add_argument("--rounds", type=int, default=3)
    b.add_argument("--secs", type=float, default=10)
    b.add_argument("--only", default=None)
    b.add_argument("--slow", type=int, default=400, help="slow downloaders (0: skip)")
    b.add_argument("--idle", type=int, default=30000, help="idle connections (0: skip)")
    b.add_argument("--extra-rounds", type=int, default=2)
    s = sub.add_parser("summary")
    s.add_argument("file")
    a = ap.parse_args()
    os.makedirs(OUT, exist_ok=True)
    os.sched_setaffinity(0, cpus(CLIENT_CPUS))
    if a.cmd == "check":
        sys.exit(check(a))
    if a.cmd == "summary":
        summarize(json.load(open(a.file)))
        return
    bench(a)


if __name__ == "__main__":
    main()
