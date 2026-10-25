"""perf profile of one implementation's server under one loadgen workload.

Server on SERVER_CPUS, loadgen on PROF_LOADGEN_CPUS; perf stat (AMD counters) + perf record
(flat, by symbol) over the steady-state window.
"""
import os
import subprocess
import sys
import time

import impls
import load_bench

SG = load_bench.SG
SERVER_CPUS = os.environ.get("SERVER_CPUS", "0-3,16-19")
LG_CPUS = os.environ.get("PROF_LOADGEN_CPUS", "4-7,20-23")
EVENTS = ("cycles,instructions,branch-misses,cache-misses,context-switches,cpu-migrations,"
          "page-faults,task-clock")


def profile(name, wl, out_dir, secs=8, callgraph=False):
    os.makedirs(out_dir, exist_ok=True)
    im = impls.ALL[name](os.path.join(SG, name), extra_serve=load_bench.SERVE_EXTRA.get(name, []))
    srv = im.serve(os.path.join(SG, "up.git"), live=False, taskset=SERVER_CPUS, ready_timeout=900)
    try:
        cursor = load_bench.peek_cold_ts(srv.port)
        spec = {w[0]: w for w in load_bench.workloads(cursor)}[wl]
        _, args, conns = spec
        lg = subprocess.Popen(["taskset", "-c", LG_CPUS, load_bench.LOADGEN, "--addr", f"127.0.0.1:{srv.port}",
                               "--conns", str(conns), "--warmup", "2", "--duration", str(secs + 4), "--threads", "8",
                               "--json", os.path.join(out_dir, f"{name}-{wl}-loadgen.json")] + args,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(3)
        pid = str(srv.proc.pid)
        stat_out = os.path.join(out_dir, f"{name}-{wl}-stat.txt")
        rec_out = os.path.join(out_dir, f"{name}-{wl}.data")
        st = subprocess.Popen(["perf", "stat", "-e", EVENTS, "-p", pid, "-o", stat_out, "--", "sleep", str(secs)])
        rec = ["perf", "record", "-F", "1999", "-p", pid, "-o", rec_out]
        if callgraph:
            rec += ["--call-graph", "dwarf,32768"]
        rec += ["--", "sleep", str(secs)]
        subprocess.run(rec, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        st.wait()
        lg.wait()
        rep = subprocess.run(["perf", "report", "-i", rec_out, "--no-children", "--sort", "dso,symbol",
                              "--stdio", "--percent-limit", "0.7"], capture_output=True, text=True).stdout
        with open(os.path.join(out_dir, f"{name}-{wl}-report.txt"), "w") as f:
            f.write(rep)
        return stat_out, rep
    finally:
        srv.stop()


if __name__ == "__main__":
    name = sys.argv[1]
    out = os.environ.get("OUT", os.path.join(SG, "prof"))
    for wl in sys.argv[2:]:
        s, rep = profile(name, wl, out, callgraph=bool(os.environ.get("CG")))
        print(f"===== {name} {wl}")
        print(open(s).read())
        print("\n".join(l for l in rep.splitlines() if l and not l.startswith("#"))[:3500])
