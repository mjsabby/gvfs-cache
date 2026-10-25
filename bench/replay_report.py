"""Summarize replay results (result.json + responses.csv) into report tables."""
import csv
import json
import os
import statistics
import sys

R = "/home/muks/gvfs-bench/replay"
CLIENTS = ["agent-1m", "agent-15m", "agent-1h", "dev-hourly", "dev-3x", "dev-morning", "dev-vacation",
           "stale"]
LABEL = {"agent-1m": "agent, every minute", "agent-15m": "agent, every 15 min", "agent-1h": "agent, hourly",
         "dev-hourly": "laptop, hourly 08-18 weekdays", "dev-3x": "developer 09:00/13:00/17:30",
         "dev-morning": "developer, mornings", "dev-vacation": "back after 6 days off", "stale": "stale client (cursor 1)"}


def loc(n):
    """`name` (a run under R) or `label=<work dir>` (e.g. opus-final=~/gvfs-bench/replay-final/opus)."""
    if "=" in n:
        label, d = n.split("=", 1)
        return label, d
    return n, os.path.join(R, n)


def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * q))]


def main():
    names = sys.argv[1:] or ["opus", "fable", "sol", "astra"]
    res = {}
    for spec in names:
        n, d = loc(spec)
        p = os.path.join(d, "result.json")
        if not os.path.exists(p):
            continue
        r = json.load(open(p))
        rows = list(csv.DictReader(open(os.path.join(d, "responses.csv"))))
        res[n] = (r, rows)
    names = list(res)
    print("## failures / pipeline / freshness")
    for n, (r, rows) in res.items():
        pipe = r["pipeline"]
        tiers = {k: f"{v[0]}x avg {v[1] / max(1, v[0]):.2f}s max {v[2]:.1f}s" for k, v in pipe["by_tier"].items()}
        print(f"- {n}: failures {r['failures']} {r['failure_samples'][:2]} | pipeline runs {pipe['runs']} "
              f"total {sum(v[1] for v in pipe['by_tier'].values()):.0f}s max {pipe['max_s']:.1f}s {tiers} | "
              f"live wait p50 {r['live_wait_s']['p50'] and round(r['live_wait_s']['p50'], 2)}s "
              f"max {r['live_wait_s']['max'] and round(r['live_wait_s']['max'], 2)}s (n={r['live_wait_s']['n']}) | "
              f"metrics {r['metrics']} | elapsed {r['elapsed_s']:.0f}s")
    print("\n## warm clients: MB received (pack+idx) / ideal, packs, worst response")
    hdr = "| client | " + " | ".join(names) + " |"
    print(hdr)
    print("|---|" + "---|" * len(names))
    for c in CLIENTS:
        cells = []
        for n in names:
            if n not in res:
                cells.append("-")
                continue
            st = res[n][0]["clients"].get(c)
            rows = [x for x in res[n][1] if x["client"] == c]
            if not st:
                cells.append("-")
                continue
            got = (st["pack_bytes"] + st["idx_bytes"]) / 1e6
            ideal = st["ideal_bytes"] / 1e6
            ratio = (st["pack_bytes"] + st["idx_bytes"]) / st["ideal_bytes"] if st["ideal_bytes"] else 0
            ratio_p = st["pack_bytes"] / st["ideal_bytes"] if st["ideal_bytes"] else 0
            mx = max((int(x["pack_bytes"]) + int(x["idx_bytes"]) for x in rows), default=0) / 1e6
            cells.append(f"{got:.1f} / {ideal:.1f} MB = **{ratio:.2f}×** (pack only {ratio_p:.2f}×); "
                         f"{st['packs']} packs / {st['requests']} req; max {mx:.1f} MB; fails {st['failures']}")
        print(f"| {LABEL[c]} | " + " | ".join(cells) + " |")
    print("\n## latency of warm prefetch requests (client-side, ms)")
    for n, (r, rows) in res.items():
        secs = [float(x["secs"]) * 1000 for x in rows if x["client"] not in ("stale",)]
        nonempty = [float(x["secs"]) * 1000 for x in rows if x["packs"] == "1" and x["client"] != "stale"]
        print(f"- {n}: all p50 {pct(secs, .5):.2f} p99 {pct(secs, .99):.2f} max {max(secs):.1f} | "
              f"with data p50 {pct(nonempty, .5):.2f} p99 {pct(nonempty, .99):.2f} max {max(nonempty):.1f}")
    print("\n## cold clones")
    for n, (r, rows) in res.items():
        cold = r["cold"]
        if not cold:
            continue
        b = [c["bytes"] for c in cold]
        s = [c["secs"] for c in cold]
        ok = sum(1 for c in cold if c["ok"])
        print(f"- {n}: {len(cold)} clones, {ok} exact; bytes min/avg/max {min(b) / 1e9:.3f}/{statistics.mean(b) / 1e9:.3f}/"
              f"{max(b) / 1e9:.3f} GB; time p50 {pct(s, .5):.2f}s max {max(s):.2f}s")
    print("\n## resent objects (already held by the client) and non-advancing responses")
    for n, (r, rows) in res.items():
        tot = sum(st["resent_objects"] for st in r["clients"].values())
        na = sum(st["not_advancing"] for st in r["clients"].values())
        print(f"- {n}: resent objects {tot}, non-advancing responses {na}")


if __name__ == "__main__":
    main()


def prefix_compare(cutoff, names):
    """Bytes received vs git's ideal per client over [start, cutoff) for every implementation,
    from responses.csv and the shared ideal cache (identical schedules => identical tip pairs)."""
    import subprocess
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from gvfslib import clean_env
    H = json.load(open(os.path.join(R, "history-w7.json")))
    commits, seed = H["commits"], H["seed"]
    ideal = {}
    cache = os.path.join(R, "oracle.json")
    if os.path.exists(cache):
        ideal = {tuple(k.split(":")): v for k, v in json.load(open(cache)).get("ideal", {}).items()}

    def tip_at(t):
        best = seed
        for oid, eff, _ in commits:
            if eff <= t:
                best = oid
            else:
                break
        return best

    def ideal_bytes(a, b):
        if (a, b) not in ideal:
            p = subprocess.run(["git", "--git-dir=/home/muks/SearchGold/.git", "pack-objects", "--revs",
                                "--stdout", "-q"], input=f"{b}\n^{a}\n".encode(), capture_output=True,
                               env=clean_env())
            ideal[(a, b)] = len(p.stdout)
        return ideal[(a, b)]

    out = {}
    for spec in names:
        n, d = loc(spec)
        p = os.path.join(d, "responses.csv")
        if not os.path.exists(p):
            continue
        per = {}
        for row in csv.DictReader(open(p)):
            t = int(row["t"])
            if t >= cutoff or row["client"] == "stale":
                continue
            c = per.setdefault(row["client"], dict(rx=0, rxp=0, ideal=0, prev=seed, packs=0, req=0, maxb=0))
            c["req"] += 1
            if row["packs"] == "1":
                b = 32 + int(row["pack_bytes"]) + int(row["idx_bytes"])
                c["rx"] += b
                c["rxp"] += 32 + int(row["pack_bytes"])
                c["packs"] += 1
                c["maxb"] = max(c["maxb"], b)
            else:
                c["rx"] += 8
            tip = tip_at(t)
            if tip != c["prev"]:
                c["ideal"] += 32 + ideal_bytes(c["prev"], tip)
                c["prev"] = tip
        out[n] = per
    print(f"\n## same window for all: until {cutoff} — MB received / MB ideal (ratio), packs")
    print("| client | " + " | ".join(out) + " |")
    print("|---|" + "---|" * len(out))
    for c in CLIENTS:
        if c == "stale":
            continue
        cells = []
        for n in out:
            x = out[n].get(c)
            if not x:
                cells.append("-")
                continue
            cells.append(f"**{x['rx'] / max(1, x['ideal']):.2f}×** ({x['rxp'] / max(1, x['ideal']):.2f}× pack only), "
                         f"{x['rx'] / 1e6:.1f} MB, {x['packs']} packs")
        print(f"| {LABEL[c]} | " + " | ".join(cells) + " |")


if __name__ == "__main__" and os.environ.get("PREFIX_UNTIL"):
    prefix_compare(int(os.environ["PREFIX_UNTIL"]), sys.argv[1:] or ["opus", "fable", "sol", "astra"])
