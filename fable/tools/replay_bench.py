#!/usr/bin/env python3
"""Replay benchmark: bytes transferred and client pack-file counts for typical clients.

Replays the last N days of a real repository's first-parent history through the real
pipeline in 15-minute steps (a shared-object bare mirror whose `main` is advanced, the
pipeline run under GVFS_FAKE_NOW with the tier the wall clock dictates), then evaluates
simulated clients against the real prefetch selection via `gvfs-daemon plan`:

  agent-24x7    fetches every 15 minutes, around the clock (what a CI/agent host does)
  dev-diurnal   weekdays at 09:00, 11:00, 14:00, 17:00 UTC
  laptop-weekly Mondays at 09:00
  cold-clone    a fresh clone every hour (what a new agent VM downloads)
  stale         a client older than the retained weeklies, at the end

Each stateful client starts with a cold clone, then fetches incrementally. Local pack files
are tracked three ways:

  stock         keeps every pack it ever received (relies on git's multi-pack-index);
  hook          `gvfs-daemon client-prefetch`: unlinks a local pack when its interval
                (X-GVFS-Pack-Start/End) lies inside another local pack's;
  hook + budget additionally, when more than B packs remain, keeps the oldest B/2 and
                collapses the rest with one `compact=1` request (bytes counted separately
                as "compaction").

    tools/replay_bench.py --src /path/to/repo.git --days 14 --out /tmp/sim \
        --config never:h24 --config h6:h24 --config h24:h24 --config h168:h24 --config never:never

A config is `<leaf-tier>:<rollup-fetch-from>`. Configs run in parallel (own mirror + store).
"""
import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

BIN = os.environ.get("GVFS_DAEMON", os.path.join(os.path.dirname(__file__), "..", "target", "release", "gvfs-daemon"))
STEP = 900
HOOK_BUDGETS = [int(b) for b in os.environ.get("HOOK_BUDGETS", "20").split(",") if b]


def sh(args, env=None, cwd=None):
    r = subprocess.run(args, capture_output=True, text=True, env=env, cwd=cwd)
    if r.returncode != 0:
        raise RuntimeError(f"{' '.join(str(a) for a in args)}\n{r.stdout}\n{r.stderr}")
    return r.stdout


def git(mirror, *args):
    return sh(["git", f"--git-dir={mirror}", *args]).strip()


def history(src, days):
    lines = sh(["git", f"--git-dir={src}", "log", "--first-parent", f"--since={days + 1}.days", "--format=%ct %H", "--reverse"]).split()
    commits = [(int(lines[i]), lines[i + 1]) for i in range(0, len(lines), 2)]
    t_end = (commits[-1][0] // STEP + 1) * STEP
    t0 = t_end - days * 86400
    return commits, t0, t_end


def tier_for(t):
    d = dt.datetime.fromtimestamp(t, dt.timezone.utc)
    if d.minute != 0:
        return "m15"
    if d.hour == 0 and d.weekday() == 6:
        return "h168"
    if d.hour == 0:
        return "h24"
    if d.hour % 12 == 0:
        return "h12"
    if d.hour % 6 == 0:
        return "h6"
    return "h1"


class Hook:
    """Local pack set of a client running `gvfs-daemon client-prefetch` (the same rules as
    crates/gvfs-daemon/src/client.rs); `budget` None = never compact."""

    def __init__(self, budget):
        self.budget = budget
        self.local = []  # (start, end)
        self.peak = 0
        self.compactions = 0
        self.bytes = 0

    def add(self, start, end):
        self.local.append((start, end))
        # Containment only: a pack inside another local pack's interval is redundant.
        self.local = [p for p in self.local if not any(q != p and q[0] <= p[0] and p[1] <= q[1] for q in self.local)]
        self.peak = max(self.peak, len(self.local))

    def compact(self, store, env):
        if self.budget is None or len(self.local) <= self.budget:
            return
        anchors = sum(1 for (s, _) in self.local if s == 0)
        rest = sorted(p for p in self.local if p[0] > 0)
        keep = max(0, self.budget // 2 - anchors)
        if len(rest) - keep < 2:
            return
        since = rest[keep][0]
        p = plan(store, since, env, compact=True)
        if p["packs"] == 0:
            return
        self.bytes += p["pack_len"] + p["idx_len"] + 32
        self.compactions += 1
        self.add(p["start_ts"], p["end_ts"])


class Client:
    def __init__(self, name, due):
        self.name = name
        self.due = due  # fn(t) -> bool
        self.cursor = 0
        self.initial_bytes = 0
        self.bytes = 0
        self.fetches = 0
        self.max_fetch = 0
        self.max_fetch_slabs = 0
        self.files_nohook = 0
        self.leaf_fetches = 0
        self.hooks = [Hook(None)] + [Hook(b) for b in HOOK_BUDGETS]

    def apply(self, plan):
        if plan["packs"] == 0:
            return plan
        size = plan["pack_len"] + plan["idx_len"] + 32
        if self.cursor == 0:
            self.initial_bytes = size
        else:
            self.bytes += size
            self.max_fetch = max(self.max_fetch, size)
            self.max_fetch_slabs = max(self.max_fetch_slabs, len(plan["slabs"]))
        self.fetches += 1
        if plan.get("leaves", 0):
            self.leaf_fetches += 1
        self.files_nohook += 1
        for h in self.hooks:
            h.add(plan["start_ts"], plan["end_ts"])
        self.cursor = plan["end_ts"]
        return plan

    def compact(self, store, env):
        for h in self.hooks:
            h.compact(store, env)


def plan(store, since, env, compact=False):
    args = [BIN, "plan", "--store", store, "--since", str(since)]
    if compact:
        args.append("--compact")
    return json.loads(sh(args, env=env))


def run_config(cfg, src, days, out_dir, genesis_store, commits, t0, t_end, log):
    leaf, fetch_from = cfg.split(":")
    name = f"leaf-{leaf}_fetch-{fetch_from}"
    work = os.path.join(out_dir, name)
    mirror = os.path.join(work, "mirror.git")
    store = os.path.join(work, "store")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    sh(["git", "clone", "-q", "--bare", "--shared", src, mirror])
    sh(["cp", "-al", genesis_store, store])
    env = dict(os.environ)
    agent = Client("agent-24x7", lambda t: True)
    dev = Client("dev-diurnal", lambda t: dt.datetime.fromtimestamp(t, dt.timezone.utc).weekday() < 5 and dt.datetime.fromtimestamp(t, dt.timezone.utc).minute == 0 and dt.datetime.fromtimestamp(t, dt.timezone.utc).hour in (9, 11, 14, 17))
    laptop = Client("laptop-weekly", lambda t: dt.datetime.fromtimestamp(t, dt.timezone.utc).weekday() == 0 and dt.datetime.fromtimestamp(t, dt.timezone.utc).hour == 9 and dt.datetime.fromtimestamp(t, dt.timezone.utc).minute == 0)
    clients = [agent, dev, laptop]
    cold = []  # (bytes, slabs) per hourly cold clone
    upstream_bytes = 0
    pipeline_secs = 0.0
    rollups = {"splice": 0, "fetch": 0}
    ci = 0
    while ci < len(commits) and commits[ci][0] <= t0:
        ci += 1
    # A fresh mirror points at the source HEAD; start the branch where the genesis is.
    git(mirror, "update-ref", "refs/heads/main", commits[ci - 1][1])
    t = t0
    steps = 0
    while t < t_end:
        t += STEP
        steps += 1
        moved = False
        while ci < len(commits) and commits[ci][0] <= t:
            moved = True
            ci += 1
        if moved:
            git(mirror, "update-ref", "refs/heads/main", commits[ci - 1][1])
        tier = tier_for(t)
        env["GVFS_FAKE_NOW"] = str(t)
        t1 = time.time()
        out = sh([BIN, "pipeline", "--tier", tier, "--store", store, "--upstream", mirror, "--baseline", "splice", "--rollup-fetch-from", fetch_from, "--leaf-tier", leaf, "--workers", "8"], env=env)
        pipeline_secs += time.time() - t1
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("fetched "):
                upstream_bytes += int(line.split()[1])
            if line.startswith("rollup: splicing"):
                rollups["splice"] += 1
            if line.startswith("rollup: re-fetching"):
                rollups["fetch"] += 1
        for c in clients:
            if c.cursor == 0 or c.due(t):
                before = [len(h.local) for h in c.hooks]
                p = c.apply(plan(store, c.cursor, env))
                c.compact(store, env)
                if os.environ.get("REPLAY_DEBUG") == c.name and p and p["packs"]:
                    d = dt.datetime.fromtimestamp(t, dt.timezone.utc)
                    log(f"  {d:%a %H:%M} {c.name}: got [{p['start_ts']}..{p['end_ts']}] slabs={p['slabs']} leaves={p.get('leaves', 0)} files {before}->{[len(h.local) for h in c.hooks]}")
        if dt.datetime.fromtimestamp(t, dt.timezone.utc).minute == 0:
            p = plan(store, 0, env)
            cold.append((p["pack_len"] + p["idx_len"] + 32, len(p["slabs"])))
        if steps % 96 == 0:
            log(f"{name}: day {steps // 96} done ({pipeline_secs:.0f}s pipeline)")
    stale = plan(store, 1, env)
    insp = sh([BIN, "inspect", "--store", store], env=env)
    store_bytes = sum(os.path.getsize(os.path.join(store, "slabs", f)) for f in os.listdir(os.path.join(store, "slabs")))
    return {
        "config": cfg,
        "days": days,
        "steps": steps,
        "pipeline_secs": pipeline_secs,
        "upstream_bytes": upstream_bytes,
        "rollups": rollups,
        "store_bytes": store_bytes,
        "inspect": insp,
        "clients": {c.name: vars_of(c) for c in clients},
        "cold": {"n": len(cold), "avg_bytes": sum(b for b, _ in cold) / len(cold), "max_bytes": max(b for b, _ in cold), "min_bytes": min(b for b, _ in cold), "avg_slabs": sum(s for _, s in cold) / len(cold), "max_slabs": max(s for _, s in cold)},
        "stale_bytes": stale["pack_len"] + stale["idx_len"] + 32,
    }


def vars_of(c):
    hooks = [{"budget": h.budget, "peak": h.peak, "final": len(h.local), "compactions": h.compactions, "bytes": h.bytes} for h in c.hooks]
    return {k: v for k, v in vars(c).items() if k not in ("due", "hooks")} | {"hooks": hooks}


def mb(n):
    return f"{n / 1e6:.1f} MB"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", required=True, help="bare or non-bare repository to replay (its objects are shared, never modified)")
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--out", required=True)
    ap.add_argument("--config", action="append", default=[], help="<leaf-tier>:<rollup-fetch-from>")
    ap.add_argument("--jobs", type=int, default=4)
    args = ap.parse_args()
    configs = args.config or ["never:h24", "h6:h24", "h24:h24", "h168:h24", "never:never"]
    src = args.src if args.src.endswith(".git") or os.path.exists(os.path.join(args.src, "HEAD")) else os.path.join(args.src, ".git")
    os.makedirs(args.out, exist_ok=True)
    commits, t0, t_end = history(src, args.days)
    start_commit = [h for (ct, h) in commits if ct <= t0][-1]
    print(f"window {dt.datetime.fromtimestamp(t0, dt.timezone.utc)} .. {dt.datetime.fromtimestamp(t_end, dt.timezone.utc)}: {sum(1 for ct, _ in commits if t0 < ct <= t_end)} first-parent commits; genesis at {start_commit[:12]}", flush=True)
    genesis_store = os.path.join(args.out, "genesis-store")
    if not os.path.exists(os.path.join(genesis_store, "manifest.bin")):
        shutil.rmtree(genesis_store, ignore_errors=True)
        mirror = os.path.join(args.out, "genesis-mirror.git")
        shutil.rmtree(mirror, ignore_errors=True)
        sh(["git", "clone", "-q", "--bare", "--shared", src, mirror])
        git(mirror, "update-ref", "refs/heads/main", start_commit)
        env = dict(os.environ, GVFS_FAKE_NOW=str(t0))
        t1 = time.time()
        sh([BIN, "pipeline", "--tier", "m15", "--store", genesis_store, "--upstream", mirror, "--workers", "16"], env=env)
        print(f"genesis baseline built in {time.time() - t1:.0f}s", flush=True)
    results = []
    with ThreadPoolExecutor(max_workers=args.jobs) as ex:
        futs = {ex.submit(run_config, c, src, args.days, args.out, genesis_store, commits, t0, t_end, lambda m: print(m, flush=True)): c for c in configs}
        for f in futs:
            results.append(f.result())
    with open(os.path.join(args.out, "results.json"), "w") as fh:
        json.dump(results, fh, indent=1)
    # Tables.
    print()
    print(f"## {args.days}-day replay, {sum(1 for ct, _ in commits if t0 < ct <= t_end)} commits\n")
    print("| config (leaf-tier : rollup-fetch-from) | pipeline time | upstream bytes | rollups splice/fetch | store size |")
    print("|---|---|---|---|---|")
    for r in results:
        print(f"| {r['config']} | {r['pipeline_secs']:.0f} s | {mb(r['upstream_bytes'])} | {r['rollups']['splice']}/{r['rollups']['fetch']} | {mb(r['store_bytes'])} |")
    print()
    hook_cols = " | ".join(f"hook B={b}: compactions / bytes / peak / final" for b in HOOK_BUDGETS)
    print(f"| config | client | fetches w/ data | initial clone | incremental bytes | /day | max single fetch | leaf-only fetches | packs: stock | hook: peak / final | {hook_cols} |")
    print("|---" * (10 + len(HOOK_BUDGETS)) + "|")
    for r in results:
        for name, c in r["clients"].items():
            h0, hb = c["hooks"][0], c["hooks"][1:]
            cols = " | ".join(f"{h['compactions']} / {mb(h['bytes'])} / {h['peak']} / {h['final']}" for h in hb)
            print(f"| {r['config']} | {name} | {c['fetches']} | {mb(c['initial_bytes'])} | {mb(c['bytes'])} | {mb(c['bytes'] / r['days'])} | {mb(c['max_fetch'])} ({c['max_fetch_slabs']} slabs) | {c['leaf_fetches']} | {c['files_nohook']} | {h0['peak']} / {h0['final']} | {cols} |")
    print("\n(hook: unlink packs whose interval lies inside another local pack's; hook B: when more than B packs remain, keep the oldest B/2 and collapse the rest with one compact=1 request)")
    print()
    print("| config | hourly cold clones | bytes min / avg / max | slabs spliced avg / max | stale client (older than weeklies) |")
    print("|---|---|---|---|---|")
    for r in results:
        c = r["cold"]
        print(f"| {r['config']} | {c['n']} | {mb(c['min_bytes'])} / {mb(c['avg_bytes'])} / {mb(c['max_bytes'])} | {c['avg_slabs']:.1f} / {c['max_slabs']} | {mb(r['stale_bytes'])} |")


if __name__ == "__main__":
    main()
