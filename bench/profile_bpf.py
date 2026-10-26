"""CPU and wait analysis of opus with BPF: where its threads run, and where they wait.

For each workload the edge (a frame-pointer build, pinned like the load benchmark) is driven
by loadgen, and three passes are taken over steady load:

  on-CPU   bpftrace `profile:hz:999` on the edge's pid, kernel and user stacks
  counters `perf stat` (cycles, instructions, cache and branch misses, stalls)
  off-CPU  bcc `offcputime` on the pid (a separate pass: it traces every context switch)

Each pass writes folded stacks under <out>/<workload>/; the summary keeps the top frames by
self and inclusive on-CPU samples and the top blocking stacks.

    python3 bench/profile_bpf.py serve [--stores sg7-sep,...] [--only get_blob,...] [--bin B]
    python3 bench/profile_bpf.py ab --base B1 --new B2 [--stores ...] [--only ...] [--rounds 3]
    python3 bench/profile_bpf.py pid <PID> <name> [--secs 10]     (any running process)

`ab` interleaves two builds (both edges up, the order alternating each round) and reports
throughput, latency and the edge's CPU time per request; no profiling.
"""
import argparse
import collections
import json
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from gvfslib import clean_env, free_port, wait_http  # noqa: E402

BENCH = os.path.expanduser("~/gvfs-bench")
FP_BIN = os.path.join(BENCH, "prof/target/release/gvfs-daemon")
LOADGEN = os.path.expanduser("~/gvfs-server-benchmark/target/loadgen/release/loadgen")
SERVER_CPUS = os.environ.get("SERVER_CPUS", "0-3,16-19")
LOADGEN_CPUS = os.environ.get("LOADGEN_CPUS", "8-15,24-31")
OUT = os.path.join(BENCH, "prof/out")
HASH = re.compile(r"::h[0-9a-f]{16}$")


def demangle(text):
    p = subprocess.run(["rustfilt"], input=text, capture_output=True, text=True)
    return p.stdout if p.returncode == 0 else text


def clean(sym):
    sym = HASH.sub("", sym)
    sym = re.sub(r"\+0x[0-9a-f]+$|\+\d+$", "", sym)
    return sym


def parse_bpftrace(text):
    """bpftrace map `@s[kstack, ustack]: n` -> [(kernel frames, user frames, n)], each list
    root first."""
    out = []
    for m in re.finditer(r"@s\[\n(.*?)\]: (\d+)", text, re.S):
        body = m.group(1)
        kpart, _, upart = body.partition("\n, \n") if "\n, \n" in body else body.partition(", \n")
        k = [clean(l.strip().split(" ", 1)[1]) for l in kpart.splitlines() if l.strip() and " " in l.strip()]
        u = []
        for l in upart.splitlines():
            l = l.strip()
            if not l or " " not in l:
                continue
            rest = l.split(" ", 1)[1]
            sym = rest[:rest.rfind(" (")] if " (" in rest else rest
            u.append(clean(sym))
        out.append((list(reversed(k)), list(reversed(u)), int(m.group(2))))
    return out


SYS = re.compile(r"__(?:x64|ia32)_sys_(\w+)")


def kernel_context(k):
    """Where kernel time goes: the syscall it serves, else what interrupted."""
    for f in k:
        m = SYS.match(f)
        if m:
            return "sys_" + m.group(1)
    names = " ".join(k)
    if "asm_exc_page_fault" in names:
        return "page fault"
    if "net_rx_action" in names or "do_softirq" in names or "handle_softirqs" in names:
        return "softirq (net rx)"
    if "irq" in names:
        return "irq"
    return "other kernel"


def summarize_oncpu(stacks, top=30):
    total = sum(n for _, _, n in stacks)
    self_c, incl_u, by_ctx, kernel = collections.Counter(), collections.Counter(), collections.Counter(), 0
    ksym = collections.Counter()
    for k, u, n in stacks:
        if k:
            kernel += n
            by_ctx[kernel_context(k)] += n
            ksym[k[-1]] += n
            self_c["[k] " + k[-1]] += n
        elif u:
            self_c[u[-1]] += n
            by_ctx["user"] += n
        for s in set(u):
            incl_u[s] += n
    pct = lambda c: round(100.0 * c / max(total, 1), 2)
    return {"samples": total, "kernel_pct": pct(kernel),
            "by_context": [(c, pct(v)) for c, v in by_ctx.most_common()],
            "self": [(s, pct(c)) for s, c in self_c.most_common(top)],
            "kernel_self": [(s, pct(c)) for s, c in ksym.most_common(top)],
            "inclusive_user": [(s, pct(c)) for s, c in incl_u.most_common(top * 3)]}


SCHED = {"finish_task_switch", "finish_task_switch.isra.0", "__schedule", "schedule", "schedule_timeout",
         "schedule_hrtimeout_range", "schedule_hrtimeout_range_clock", "__schedule_loop",
         "schedule_hrtimeout_range_noprof"}


def summarize_offcpu(text, top=20):
    """offcputime -f -d: `thread;user...;-;kernel... us`, both stacks root first."""
    total, rows, by_kind = 0, collections.Counter(), collections.Counter()
    for line in text.splitlines():
        if " " not in line:
            continue
        stack, us = line.rsplit(" ", 1)
        try:
            us = int(us)
        except ValueError:
            continue
        parts = stack.split(";")
        comm = parts[0]
        kind = re.sub(r"\d+$", "", comm)
        if "-" in parts:
            i = parts.index("-")
            user, kern = parts[1:i], parts[i + 1:]
        else:
            user, kern = parts[1:], []
        ours = [clean(p) for p in user if "gvfs_daemon" in p]
        where = ours[-1] if ours else (clean(user[-1]) if user else "?")
        sysc = next((("sys_" + m.group(1)) for m in (SYS.match(f) for f in kern) if m), None)
        reason = sysc or next((f for f in reversed(kern) if f not in SCHED), "?")
        total += us
        by_kind[kind] += us
        rows[(kind, where, reason)] += us
    return {"total_s": round(total / 1e6, 2),
            "by_thread_kind": [(k, round(v / 1e6, 2)) for k, v in by_kind.most_common()],
            "top": [(k, w, r, round(us / 1e6, 3), round(100.0 * us / max(total, 1), 1))
                    for (k, w, r), us in rows.most_common(top)]}


def perf_stat(pid, secs):
    p = subprocess.run(["sudo", "perf", "stat", "-x", ",", "-e",
                        "cycles,instructions,cache-references,cache-misses,branch-misses,"
                        "stalled-cycles-frontend,stalled-cycles-backend,context-switches",
                        "-p", str(pid), "--", "sleep", str(secs)], capture_output=True, text=True)
    vals = {}
    for line in p.stderr.splitlines():
        f = line.split(",")
        if len(f) > 2 and f[0].replace(".", "").isdigit():
            vals[f[2]] = float(f[0])
    out = {k: int(v) for k, v in vals.items()}
    if vals.get("cycles"):
        out["ipc"] = round(vals.get("instructions", 0) / vals["cycles"], 2)
    if vals.get("cache-references"):
        out["cache_miss_pct"] = round(100 * vals.get("cache-misses", 0) / vals["cache-references"], 1)
    return out


def oncpu(pid, secs, path):
    prog = (f"profile:hz:999 /pid == {pid}/ {{ @s[kstack(perf, 32), ustack(perf, 64)] = count(); }}"
            f" interval:s:{secs} {{ exit(); }}")
    p = subprocess.run(["sudo", "bpftrace", "-q", "-e", prog], capture_output=True, text=True)
    text = demangle(p.stdout)
    open(path, "w").write(text)
    return parse_bpftrace(text)


def offcpu(pid, secs, path):
    p = subprocess.run(["sudo", "offcputime-bpfcc", "-p", str(pid), "-f", "-d",
                        "--stack-storage-size", "131072", str(secs)], capture_output=True, text=True)
    text = demangle(p.stdout)
    open(path, "w").write(text)
    return text


def fold(stacks, path):
    with open(path, "w") as f:
        for k, u, n in stacks:
            f.write(";".join(u + ["[k]" + x for x in k]) + f" {n}\n")


def profile_pid(pid, name, secs, during=None):
    """The three passes on a running process; `during()` restarts the load for each pass."""
    d = os.path.join(OUT, name)
    os.makedirs(d, exist_ok=True)
    res = {}
    lg = during() if during else None
    wait_load(pid)
    import threading
    stat = {}
    t = threading.Thread(target=lambda: stat.update(perf_stat(pid, secs)))
    t.start()
    stacks = oncpu(pid, secs, os.path.join(d, "oncpu.bpftrace"))
    t.join()
    load_a = lg() if lg else None
    fold(stacks, os.path.join(d, "oncpu.folded"))
    res["oncpu"] = summarize_oncpu(stacks)
    res["counters"] = stat
    lg = during() if during else None
    wait_load(pid)
    res["offcpu"] = summarize_offcpu(offcpu(pid, secs, os.path.join(d, "offcpu.folded")))
    load_b = lg() if lg else None
    res["load"] = load_a
    res["load_during_offcpu"] = load_b
    json.dump(res, open(os.path.join(d, "summary.json"), "w"), indent=1)
    return res


def serve(store, log, binary=FP_BIN):
    port = free_port()
    p = subprocess.Popen(["taskset", "-c", SERVER_CPUS, binary, "serve", "--store", f"fs:{store}",
                          "--bind", f"127.0.0.1:{port}", "--cache-dir", os.path.join(BENCH, "prof/cache"),
                          "--alloc-guard", "strict"], stdout=subprocess.DEVNULL,
                         stderr=open(log, "wb"), env=clean_env())
    wait_http(port, "/healthz", timeout=1800, proc=p)
    return p, port


def loadgen_starter(port, args, conns, secs):
    def start():
        out = os.path.join(OUT, f"lg-{port}-{time.time():.0f}.json")
        p = subprocess.Popen(["taskset", "-c", LOADGEN_CPUS, LOADGEN, "--addr", f"127.0.0.1:{port}",
                              "--conns", str(conns), "--warmup", "1", "--duration", str(secs + 4),
                              "--threads", "16", "--timeout", "120", "--json", out] + args,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        def done():
            p.wait()
            try:
                r = json.load(open(out))
            except (OSError, ValueError):
                return None
            return {"ok_rps": round(r["ok_rps"], 1), "MBps": round(r["mbps"], 1),
                    "p50_us": r["lat_us"]["p50"], "p99_us": r["lat_us"]["p99"], "statuses": r["statuses"]}
        return done
    return start


def short(sym, n=110):
    sym = sym.replace("gvfs_daemon::", "")
    return sym if len(sym) <= n else sym[:n] + "..."


def report(name, res):
    print(f"\n=== {name}: load {res.get('load')}")
    c = res["counters"]
    print(f"  counters: ipc {c.get('ipc')} cache-miss {c.get('cache_miss_pct')}% "
          f"ctx-switches {c.get('context-switches')}")
    on = res["oncpu"]
    print(f"  on-CPU {on['samples']} samples; by context: "
          + ", ".join(f"{c} {p}%" for c, p in on["by_context"][:9]))
    print("  self (top):")
    for s, p in on["self"][:16]:
        print(f"    {p:6.2f}%  {short(s)}")
    print("  inclusive, ours:")
    for s, p in [x for x in on["inclusive_user"] if "gvfs_daemon" in x[0]][:16]:
        print(f"    {p:6.2f}%  {short(s)}")
    off = res["offcpu"]
    print(f"  off-CPU {off['total_s']} s; by thread kind: {off['by_thread_kind'][:6]}")
    for k, w, r, s, p in off["top"][:12]:
        print(f"    {p:5.1f}% {s:8.2f}s  {k:12s} {r:24s} {short(w, 90)}")


# name: (loadgen args, conns); {oids} is the store's blob list.
WORKLOADS = {
    "get_blob": (["--workload", "get-object", "--oids", "{oids}"], 64),
    "post_4000_blobs": (["--workload", "post-objects", "--oids", "{oids}", "--batch", "4000"], 32),
    "sizes_4000": (["--workload", "sizes", "--oids", "{oids}", "--batch", "4000"], 32),
    "prefetch_cold_sizes": (["--workload", "prefetch", "--ts", "0", "--header", "X-GVFS-Sizes: 1",
                             "--header", "User-Agent: git/2.53.0.vfs.0.0"], 8),
    # VFS for Git (not a git user agent): the pack's .idx comes after it.
    "prefetch_cold_idx": (["--workload", "prefetch", "--ts", "0", "--header", "User-Agent: GVFS/1.0"], 8),
}
# name: (store, blob ids)
STORES = {
    "sg-sep": (os.path.join(BENCH, "prof/sg-sep/store"), f"{BENCH}/sg/oids/blobs.txt"),
    "sg-inline": (os.path.join(BENCH, "sg/opus2/store"), f"{BENCH}/sg/oids/blobs.txt"),
    # SearchGold after a week of pipeline runs (bench/build_store.py): the live pyramid.
    "sg7-sep": (os.path.join(BENCH, "prof/sg7-sep/store"), f"{BENCH}/prof/sg7-sep/blobs.txt"),
    "sg7-inl": (os.path.join(BENCH, "prof/sg7-inl/store"), f"{BENCH}/prof/sg7-inl/blobs.txt"),
}


def args_for(work, store):
    args, conns = WORKLOADS[work]
    return [a.replace("{oids}", STORES[store][1]) for a in args], conns


def cpu_seconds(pid):
    """utime + stime of every thread of `pid`, in seconds."""
    f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    return (int(f[11]) + int(f[12])) / os.sysconf("SC_CLK_TCK")


def wait_load(pid, timeout=60.0):
    """Until the edge is busy: loadgen builds its request bodies (seconds, for 4,096 bodies
    of 4,000 ids) before it sends anything, so a fixed delay can measure an idle edge."""
    t_end = time.time() + timeout
    c0, t0 = cpu_seconds(pid), time.time()
    while time.time() < t_end:
        time.sleep(0.1)
        c1, t1 = cpu_seconds(pid), time.time()
        if (c1 - c0) / (t1 - t0) > 0.3:
            break
        c0, t0 = c1, t1
    time.sleep(1.0)


def measure(port, pid, args, conns, secs):
    start = loadgen_starter(port, args, conns, secs)
    done = start()
    wait_load(pid)
    c0, t0 = cpu_seconds(pid), time.time()
    time.sleep(secs)
    c1, t1 = cpu_seconds(pid), time.time()
    r = done()
    if r:
        r["edge_cpus"] = round((c1 - c0) / (t1 - t0), 2)
        r["cpu_us_per_req"] = round(1e6 * (c1 - c0) / (t1 - t0) / max(r["ok_rps"], 1e-9), 1)
    return r


def ab(a):
    names = [n for n in WORKLOADS if not a.only or n in a.only.split(",")]
    results = []
    for store in a.stores.split(","):
        servers = {}
        for tag, b in (("base", a.base), ("new", a.new)):
            servers[tag] = serve(STORES[store][0], os.path.join(OUT, f"ab-{store}-{tag}.log"), b)
        try:
            for r in range(a.rounds):
                for n in names:
                    args, conns = args_for(n, store)
                    order = ("base", "new") if r % 2 == 0 else ("new", "base")
                    for tag in order:
                        p, port = servers[tag]
                        res = measure(port, p.pid, args, conns, a.secs)
                        results.append({"store": store, "work": n, "build": tag, "round": r, **(res or {})})
                        print(json.dumps(results[-1]), flush=True)
        finally:
            for p, _ in servers.values():
                p.terminate()
                p.wait()
    json.dump(results, open(os.path.join(OUT, "ab.json"), "w"), indent=1)
    print("\nstore      workload              build  req/s(median)  p50us  p99us  edge-cpus  cpu-us/req")
    by = collections.defaultdict(list)
    for x in results:
        by[(x["store"], x["work"], x["build"])].append(x)
    for (st, w, b), xs in sorted(by.items()):
        xs = sorted(xs, key=lambda x: x.get("ok_rps", 0))
        m = xs[len(xs) // 2]
        print(f"{st:10s} {w:21s} {b:5s} {m.get('ok_rps', 0):13.1f} {m.get('p50_us', 0):6} {m.get('p99_us', 0):6} "
              f"{m.get('edge_cpus', 0):9} {m.get('cpu_us_per_req', 0):11}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["serve", "pid", "ab"])
    ap.add_argument("rest", nargs="*")
    ap.add_argument("--only", default="")
    ap.add_argument("--stores", default="sg7-sep")
    ap.add_argument("--secs", type=int, default=10)
    ap.add_argument("--bin", default=FP_BIN)
    ap.add_argument("--base", default=os.path.join(BENCH, "prof/bin/gvfs-daemon-base"))
    ap.add_argument("--new", default=os.path.join(BENCH, "prof/bin/gvfs-daemon-new"))
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--tag", default="", help="suffix for the output directories")
    a = ap.parse_args()
    os.makedirs(OUT, exist_ok=True)
    if a.mode == "pid":
        pid, name = int(a.rest[0]), a.rest[1]
        report(name, profile_pid(pid, name, a.secs))
        return
    if a.mode == "ab":
        ab(a)
        return
    names = [n for n in WORKLOADS if not a.only or n in a.only.split(",")]
    for store in a.stores.split(","):
        p, port = serve(STORES[store][0], os.path.join(OUT, f"serve-{store}.log"), a.bin)
        try:
            for n in names:
                args, conns = args_for(n, store)
                name = f"{store}-{n}{a.tag}"
                report(name, profile_pid(p.pid, name, a.secs, during=loadgen_starter(port, args, conns, a.secs)))
        finally:
            p.terminate()
            p.wait()


if __name__ == "__main__":
    main()
