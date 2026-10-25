"""Aggregate interleaved load rounds: median across rounds per (impl, workload)."""
import json
import statistics
import sys

path = sys.argv[1] if len(sys.argv) > 1 else "/home/muks/gvfs-bench/sg/load/all.json"
d = json.load(open(path))
W = ["get_blob", "get_tree", "get_commit", "post_64", "post_4000", "sizes_4000", "prefetch_uptodate",
     "prefetch_cold", "closure"]


def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def fmt(v, nd=0):
    if v is None:
        return "-"
    if nd == 0:
        return f"{v:,.0f}"
    return f"{v:,.{nd}f}"


rows = []
for w in W:
    for impl, rounds in d.items():
        rs = [r.get(w) for r in rounds if r.get(w)]
        if not rs:
            continue
        ok = med([r.get("ok_rps") for r in rs])
        mb = med([r.get("mbps") for r in rs])
        p50 = med([r.get("lat_us", {}).get("p50") for r in rs])
        p99 = med([r.get("lat_us", {}).get("p99") for r in rs])
        cpu = med([r.get("cpu_us_per_ok") for r in rs])
        cores = med([r.get("server_cores") for r in rs])
        rss = med([r.get("VmRSS", 0) / 1024 for r in rs])
        bad = sum(sum(v for k, v in (r.get("statuses") or {}).items() if k != "200") for r in rs)
        bad += sum((r.get("conn_errors") or 0) + (r.get("timeouts") or 0) for r in rs)
        spread = (max(r.get("ok_rps", 0) for r in rs) / max(1e-9, min(r.get("ok_rps", 0) for r in rs))) if rs else 0
        resp_kb = (mb * 1e3 / ok) if ok else None
        rows.append((w, impl, ok, mb, p50, p99, cpu, cores, rss, bad, spread, resp_kb, len(rs)))

print(f"{'workload':18s} {'impl':6s} {'ok/s':>11s} {'MB/s':>9s} {'KB/resp':>9s} {'p50us':>8s} {'p99us':>9s} "
      f"{'cpu us/req':>10s} {'cores':>5s} {'RSS MiB':>8s} {'errors':>6s} {'max/min':>7s} n")
last = None
for (w, impl, ok, mb, p50, p99, cpu, cores, rss, bad, spread, kb, n) in rows:
    if w != last:
        print()
        last = w
    print(f"{w:18s} {impl:6s} {fmt(ok, 1):>11s} {fmt(mb, 1):>9s} {fmt(kb, 1):>9s} {fmt(p50):>8s} {fmt(p99):>9s} "
          f"{fmt(cpu, 1):>10s} {fmt(cores, 2):>5s} {fmt(rss):>8s} {bad:>6} {spread:>7.2f} {n}")
