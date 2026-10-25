"""Interleaved, CPU-pinned load benchmark (opus B-09, extended) over the SearchGold stores.

Server pinned to 4 cores / 8 threads of CCD0, loadgen to CCD1.  Each round visits the
implementations in a fresh random order (seeded), starts the server, runs every workload
(random order too), stops it.  Server CPU per request is read from /proc around the
measured window; RSS after each workload.
"""
import http.client
import json
import os
import random
import struct
import subprocess
import sys
import threading
import time

import impls
from gvfslib import clean_env, request

SG = "/home/muks/gvfs-bench/sg"
OIDS = os.path.join(SG, "oids")
LOADGEN = os.path.expanduser("~/gvfs-server-benchmark/target/loadgen/release/loadgen")
SERVER_CPUS = os.environ.get("SERVER_CPUS", "0-3,16-19")
LOADGEN_CPUS = os.environ.get("LOADGEN_CPUS", "8-15,24-31")
HZ = os.sysconf("SC_CLK_TCK")

SOL_SERVE = ["--max-objects", "16777216", "--max-links", "2147483647", "--pack-mib", "65536",
             "--object-mib", "128", "--delta-cache-mib", "512", "--arenas", "8",
             "--max-request-objects", "16384", "--max-ranges", "8388608"]
SERVE_EXTRA = {"sol": SOL_SERVE, "astra": ["--workers", "64"], "opus": [], "fable": []}


def workloads(cursor):
    W = [
        ("get_blob", ["--workload", "get-object", "--oids", f"{OIDS}/blobs.txt"], 64),
        ("get_tree", ["--workload", "get-object", "--oids", f"{OIDS}/trees.txt"], 64),
        ("get_commit", ["--workload", "get-object", "--oids", f"{OIDS}/commits.txt"], 64),
        ("post_64", ["--workload", "post-objects", "--oids", f"{OIDS}/noncommits.txt", "--batch", "64"], 64),
        ("post_4000", ["--workload", "post-objects", "--oids", f"{OIDS}/noncommits.txt", "--batch", "4000"], 32),
        ("sizes_4000", ["--workload", "sizes", "--oids", f"{OIDS}/oids.txt", "--batch", "4000"], 32),
        ("prefetch_uptodate", ["--workload", "prefetch", "--ts", str(cursor)], 64),
        ("prefetch_cold", ["--workload", "prefetch", "--ts", "0"], 8),
        ("closure", ["--workload", "post-objects", "--oids", f"{OIDS}/commits.txt", "--batch", "1",
                     "--depth", "1", "--bodies", "256"], 4),
    ]
    return W


def cpu_ticks(pid):
    with open(f"/proc/{pid}/stat") as f:
        parts = f.read().rsplit(")", 1)[1].split()
    return int(parts[11]) + int(parts[12])


def rss_kb(pid):
    out = {}
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith(("VmRSS", "VmHWM")):
                k, v = line.split(":")
                out[k] = int(v.split()[0])
    return out


def peek_cold_ts(port):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=600)
    c.request("GET", "/gvfs/prefetch?lastPackTimestamp=0")
    r = c.getresponse()
    head = r.read(32)
    c.close()
    n = struct.unpack_from("<H", head, 6)[0]
    return struct.unpack_from("<q", head, 8)[0] if n else 0


def run_workload(port, pid, name, args, conns, warmup=2.0, duration=10.0, out_dir=None):
    js = os.path.join(out_dir, f"{name}.json")
    cmd = ["taskset", "-c", LOADGEN_CPUS, LOADGEN, "--addr", f"127.0.0.1:{port}", "--conns", str(conns),
           "--warmup", str(warmup), "--duration", str(duration), "--threads", "16", "--timeout", "60",
           "--json", js] + args
    p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    time.sleep(warmup)
    c0, t0 = cpu_ticks(pid), time.time()
    time.sleep(duration)
    c1, t1 = cpu_ticks(pid), time.time()
    _, err = p.communicate(timeout=duration + 180)
    try:
        res = json.load(open(js))
    except Exception:
        res = {"error": err.decode(errors="replace")[-500:]}
    res["server_cpu_s"] = (c1 - c0) / HZ
    res["server_cores"] = res["server_cpu_s"] / (t1 - t0)
    ok = res.get("ok", 0) or 0
    res["cpu_us_per_ok"] = (res["server_cpu_s"] * 1e6 / ok) if ok else None
    res.update(rss_kb(pid))
    return res


def bench_impl(name, out_root, rnd, order_seed):
    work = os.path.join(SG, name)
    im = impls.ALL[name](work, extra_serve=SERVE_EXTRA.get(name, []))
    srv = im.serve(impls_upstream(name), poll_s=1, live=False, taskset=SERVER_CPUS, ready_timeout=1800)
    out_dir = os.path.join(out_root, f"r{rnd}", name)
    os.makedirs(out_dir, exist_ok=True)
    results = {"ready_s": srv.ready_secs, "rss_start": rss_kb(srv.proc.pid)}
    try:
        cursor = peek_cold_ts(srv.port)
        results["cursor"] = cursor
        W = workloads(cursor)
        random.Random(order_seed).shuffle(W)
        for wname, args, conns in W:
            r = run_workload(srv.port, srv.proc.pid, wname, args, conns, out_dir=out_dir)
            results[wname] = r
            print(f"  r{rnd} {name:6s} {wname:18s} ok/s={r.get('ok_rps', 0):>11.1f} MB/s={r.get('mbps', 0):>9.1f} "
                  f"p50={r.get('lat_us', {}).get('p50')}us p99={r.get('lat_us', {}).get('p99')}us "
                  f"cpu/req={r.get('cpu_us_per_ok') and round(r['cpu_us_per_ok'], 1)}us "
                  f"st={r.get('statuses')} rss={r.get('VmRSS', 0) // 1024}MiB", flush=True)
            if not srv.alive():
                results["crashed"] = srv.log_tail(20)
                print(f"  !! {name} died: {srv.log_tail(10)}")
                break
        results["metrics"] = srv.metrics()
    finally:
        srv.stop()
    with open(os.path.join(out_dir, "summary.json"), "w") as f:
        json.dump(results, f, indent=1)
    return results


def impls_upstream(name):
    return os.path.join(SG, "up.git")


def main():
    names = [a for a in sys.argv[1:] if a in impls.ALL] or ["opus", "fable", "sol", "astra"]
    rounds = int(os.environ.get("ROUNDS", "3"))
    out_root = os.environ.get("OUT", os.path.join(SG, "load"))
    os.makedirs(out_root, exist_ok=True)
    rng = random.Random(20261025)
    allres = {}
    for rnd in range(1, rounds + 1):
        order = names[:]
        rng.shuffle(order)
        print(f"round {rnd}: {order}", flush=True)
        for n in order:
            allres.setdefault(n, []).append(bench_impl(n, out_root, rnd, rng.randrange(1 << 30)))
            with open(os.path.join(out_root, "all.json"), "w") as f:
                json.dump(allres, f, indent=1)


if __name__ == "__main__":
    main()
