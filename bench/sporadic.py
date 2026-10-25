"""Open-loop sporadic keep-alive clients: CONNS connections share RATE GETs/s (each connection
sends one request every CONNS/RATE s); latency from the intended send time (loadgen --rate).

usage: sporadic.py <opus-binary> ["<extra serve flags>"]   (env CONNS, RATE, WL, LG)
"""
import os, sys
import impls, load_bench
binary = sys.argv[1]
extra = sys.argv[2].split() if len(sys.argv) > 2 else []
conns = int(os.environ.get("CONNS", "2048"))
rate = os.environ.get("RATE", "8000")
wl = os.environ.get("WL", "blobs")
SG = load_bench.SG
im = impls.ALL["opus"](os.path.join(SG, "opus2"), binary=binary, extra_serve=extra)
srv = im.serve(os.path.join(SG, "up.git"), live=False, taskset="0-3,16-19", ready_timeout=900)
load_bench.LOADGEN_CPUS = os.environ.get("LG", "4-7,20-23")
out = os.path.join(SG, "sporadic")
os.makedirs(out, exist_ok=True)
args = ["--workload", "get-object", "--oids", f"{load_bench.OIDS}/{wl}.txt", "--rate", rate]
try:
    r = load_bench.run_workload(srv.port, srv.proc.pid, "sporadic", args, conns, duration=10, out_dir=out)
finally:
    srv.stop()
lat = r.get("lat_us", {})
print(f"{' '.join(extra) or 'defaults':18s} conns={conns} rate={rate}: ok/s={r.get('ok_rps', 0):9.1f} "
      f"p50={lat.get('p50')}us p99={lat.get('p99')}us max={lat.get('max')}us timeouts={r.get('timeouts')} "
      f"connerr={r.get('conn_errors')} statuses={r.get('statuses')} server_cores={r.get('server_cores', 0):.2f}")
