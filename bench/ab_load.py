#!/usr/bin/env python3
"""Load A/B of two builds of opus on one store: `base` (env BASE_BIN) against `new` (env
NEW_BIN, by default this repository's release build).

    BASE_BIN=<an older build> python3 bench/ab_load.py bench --dir ~/gvfs-bench/prof/sg7-sep
    python3 bench/ab_load.py bench --dir ... --only get_blob_64,post_4000_32 --slow 0 --idle 0
    python3 bench/ab_load.py summary <results.json>

Per workload, each build in turn (the order rotating each round): edge CPU per request,
req/s, p50/p99 latency, allocations per request (`gvfs_alloc_post_seal_total`). Then a GET
load beside slow downloaders (clients reading a cold prefetch at 80 KB/s), and beside idle
keep-alive connections (with the edge's resident memory). The slot a build runs in biases
results by up to 0.3%: before calling a smaller difference, swap BASE_BIN and NEW_BIN.

`--dir` holds `store/`, `up.git` and `blobs.txt` (bench/build_store.py). The edge runs on
SERVER_CPUS, loadgen on LOADGEN_CPUS, this script (slow and idle clients) on CLIENT_CPUS;
SERVE_EXTRA adds flags to both builds' `serve` (e.g. `--quantum-us 50`).
"""
import argparse
import collections
import json
import os
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
from gvfslib import clean_env, free_port, metrics, wait_http  # noqa: E402
from profile_bpf import LOADGEN_CPUS, OUT, SERVER_CPUS, cpu_seconds, loadgen_starter, wait_load  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILDS = {"base": os.environ.get("BASE_BIN"),
          "new": os.environ.get("NEW_BIN", os.path.join(REPO, "target/release/gvfs-cache"))}
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
    """`serve` for build `variant`: one runtime thread per edge CPU, plus SERVE_EXTRA."""
    if not BUILDS.get(variant):
        sys.exit(f"no build for {variant!r}: set {variant.upper()}_BIN")
    return [BUILDS[variant], "serve", "--store", f"fs:{store}", "--bind", f"127.0.0.1:{port}",
            "--cache-dir", cache, "--alloc-guard", "count", "--idle-secs", str(idle),
            "--workers", str(len(cpus(SERVER_CPUS)))] + os.environ.get("SERVE_EXTRA", "").split()


def start(variant, store, idle=30):
    port = free_port()
    cache = os.path.join(OUT, f"ab-load-cache-{variant}")
    shutil.rmtree(cache, ignore_errors=True)
    log = open(os.path.join(OUT, f"ab-load-{variant}.log"), "wb")
    p = subprocess.Popen(["taskset", "-c", SERVER_CPUS] + command(variant, store, port, cache, idle),
                         stdout=subprocess.DEVNULL, stderr=log, env=clean_env())
    wait_http(port, "/healthz", timeout=1800, proc=p)
    return p, port


def stop(servers):
    for p, _ in servers.values():
        p.terminate()
    for p, _ in servers.values():
        p.wait()


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
    post = ["--workload", "post-objects", "--oids", blobs, "--batch", "4000", "--bodies", "512"]
    w["post_4000_32"] = [("post", post, 32)]
    w["mixed_get64_post32"] = [("post", post, 32), ("get", get, 64)]
    return w


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
    out_path = os.path.join(OUT, "ab-load.json")

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
                    p, port = servers[v]
                    res = run_loads(port, p.pid, work[n], a.secs)
                    record({"work": n, "variant": v, "round": rnd, "rss_mb": round(rss_mb(p.pid), 1),
                            "anon_mb": round(rss_mb(p.pid, "RssAnon:"), 1), **res})
        get = ["--workload", "get-object", "--oids", blobs]
        for rnd in range(a.extra_rounds):
            for v in variants:
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
    b = sub.add_parser("bench")
    b.add_argument("--dir", required=True)
    b.add_argument("--variants", default="base,new")
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
    if a.cmd == "summary":
        summarize(json.load(open(a.file)))
        return
    bench(a)


if __name__ == "__main__":
    main()
