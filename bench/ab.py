"""A/B: frozen baseline binary vs candidate binary, alternating runs, same pinning.

usage: ab.py <impl> <candidate-binary> <workload> [<workload>...]
       (env ROUNDS, DUR, AB_BASE, AB_BASE_WORK, AB_CAND_WORK, AB_EXTRA_SERVE, AB_LOADGEN_CPUS)
"""
import json
import os
import statistics
import sys

import impls
import load_bench

SG = load_bench.SG
SERVER_CPUS = os.environ.get("SERVER_CPUS", "0-3,16-19")
LG = os.environ.get("AB_LOADGEN_CPUS", "4-7,20-23")


def one(name, binary, wl, dur, work=None):
    extra = load_bench.SERVE_EXTRA.get(name, []) + os.environ.get("AB_EXTRA_SERVE", "").split()
    im = impls.ALL[name](work or os.path.join(SG, name), binary=binary, extra_serve=extra)
    srv = im.serve(os.path.join(SG, "up.git"), live=False, taskset=SERVER_CPUS, ready_timeout=900)
    try:
        cursor = load_bench.peek_cold_ts(srv.port)
        spec = {w[0]: w for w in load_bench.workloads(cursor)}[wl]
        old = load_bench.LOADGEN_CPUS
        load_bench.LOADGEN_CPUS = LG
        out = os.path.join(SG, "ab")
        os.makedirs(out, exist_ok=True)
        r = load_bench.run_workload(srv.port, srv.proc.pid, wl, spec[1], spec[2], duration=dur, out_dir=out)
        load_bench.LOADGEN_CPUS = old
        r["alive"] = srv.alive()
        return r
    finally:
        srv.stop()


def main():
    name, cand, wls = sys.argv[1], sys.argv[2], sys.argv[3:]
    base = os.environ.get("AB_BASE") or os.path.join("/home/muks/gvfs-bench/frozen", f"{name}-gvfs-daemon")
    rounds = int(os.environ.get("ROUNDS", "2"))
    dur = float(os.environ.get("DUR", "8"))
    for wl in wls:
        res = {"A": [], "B": []}
        for _ in range(rounds):
            for tag, b, wk in (("A", base, os.environ.get("AB_BASE_WORK")),
                               ("B", cand, os.environ.get("AB_CAND_WORK"))):
                r = one(name, b, wl, dur, wk)
                res[tag].append(r)
        def med(tag, k):
            return statistics.median([x.get(k) or 0 for x in res[tag]])
        def lat(tag, q):
            return statistics.median([x.get("lat_us", {}).get(q) or 0 for x in res[tag]])
        a, b = med("A", "ok_rps"), med("B", "ok_rps")
        bad = {t: [x.get("statuses") for x in res[t]] for t in res}
        print(f"{name} {wl:18s} A={a:>12,.1f}/s B={b:>12,.1f}/s  B/A={b / a if a else 0:5.2f}  "
              f"cpu/req A={med('A', 'cpu_us_per_ok'):.1f}us B={med('B', 'cpu_us_per_ok'):.1f}us  "
              f"p99 A={lat('A', 'p99')} B={lat('B', 'p99')}  statuses {bad}", flush=True)


if __name__ == "__main__":
    main()
