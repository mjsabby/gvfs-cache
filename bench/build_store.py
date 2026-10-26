"""A steady-state store from a synthetic corpus (bench/corpus_gen.py): genesis (plus the
mirror, for `--blobs separate`), then the pipeline on the replay's cron (15-minute windows,
hourly to weekly rollups) over `--days` of pushes. No clients, no edge: the store is for
profiling the edge against the slab pyramid a real store carries.

    python3 bench/build_store.py --corpus ~/gvfs-bench/corpus-s --work ~/gvfs-bench/prof/cs-sep --blobs separate
    python3 bench/build_store.py --history ~/gvfs-bench/replay/history-w7.json \
        --objects ~/SearchGold/.git/objects --work ~/gvfs-bench/prof/sg7-sep --blobs separate
(`--history`: SearchGold's real week of first-parent pushes, as bench/replay.py replays it.)
Writes <work>/store, <work>/blobs.txt (the end tip's blob ids, shuffled) and <work>/build.json.
"""
import argparse
import json
import os
import random
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
import impls  # noqa: E402
from gvfslib import clean_env, init_bare  # noqa: E402
from replay_separate import cron_tier  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", default=os.path.expanduser("~/gvfs-bench/corpus-s"))
    ap.add_argument("--history", default=None, help="replay history JSON (start, end, seed, commits)")
    ap.add_argument("--objects", default=None, help="the history's object directory")
    ap.add_argument("--work", required=True)
    ap.add_argument("--blobs", choices=["inline", "separate"], default="separate")
    ap.add_argument("--days", type=float, default=14)
    ap.add_argument("--bin", default=None, help="gvfs-daemon binary (default: the release build)")
    a = ap.parse_args()
    if a.history:
        h = json.load(open(a.history))
        genesis, objects = h["seed"], a.objects
        S, end = h["start"], h["end"]
        pushes = [(c[0], c[1]) for c in h["commits"]]
    else:
        meta = json.load(open(os.path.join(a.corpus, "corpus.json")))
        commits, g = meta["commits"], meta["genesis_index"]
        genesis, objects = meta["genesis"], os.path.join(a.corpus, "up.git", "objects")
        S = commits[g][1] - commits[g][1] % 900 + 900
        end = commits[-1][1] + 1
        pushes = [(c[0], c[1]) for c in commits[g + 1:]]
    E = min(end, S + int(a.days * 86400))
    shutil.rmtree(a.work, ignore_errors=True)
    os.makedirs(a.work)
    im = impls.ALL["opus"](a.work, binary=a.bin, extra_pipeline=["--threads", "16"])
    up = os.path.join(a.work, "up.git")
    init_bare(up)
    with open(os.path.join(up, "objects", "info", "alternates"), "w") as f:
        f.write(objects + "\n")

    def set_tip(c):
        subprocess.run(["git", "--git-dir", up, "update-ref", "refs/heads/main", c], check=True, env=clean_env())

    set_tip(genesis)
    t0 = time.time()
    extra = ["--blobs", "separate"] if a.blobs == "separate" else []
    im.run(im.pipeline_cmd(up, "15m", S - 300) + extra)
    out = {"genesis_s": round(time.time() - t0, 1)}
    if a.blobs == "separate":
        t0 = time.time()
        im.run([im.bin, "mirror", "--store", f"fs:{im.store}", "--upstream", up, "--jobs", "16",
                "--threads", "16", "--work-dir", os.path.join(a.work, "mw")])
        out["mirror_s"] = round(time.time() - t0, 1)
    k, runs, t_runs, tip = 0, 0, 0.0, genesis
    for t in range(S + 900, E, 900):
        while k < len(pushes) and pushes[k][1] <= t:
            k += 1
        if k and pushes[k - 1][0] != tip:
            tip = pushes[k - 1][0]
            set_tip(tip)
        t0 = time.perf_counter()
        im.run(im.pipeline_cmd(up, cron_tier(t), t))
        t_runs += time.perf_counter() - t0
        runs += 1
    blobs = subprocess.run(["git", "--git-dir", up, "rev-list", "--objects", "--no-object-names",
                            "--filter=object:type=blob", "--filter-provided-objects", tip],
                           capture_output=True, text=True, check=True, env=clean_env()).stdout.split()
    random.Random(1).shuffle(blobs)
    open(os.path.join(a.work, "blobs.txt"), "w").write("\n".join(blobs) + "\n")
    slabs = sorted(f for f in os.listdir(os.path.join(im.store, "slabs")) if f.endswith(".mphf"))
    out.update(blobs=a.blobs, days=a.days, runs=runs, runs_s=round(t_runs, 1), tip=tip,
               tip_blobs=len(blobs), slabs=len(slabs), slab_names=slabs, store=im.store)
    json.dump(out, open(os.path.join(a.work, "build.json"), "w"), indent=1)
    print(json.dumps({k: v for k, v in out.items() if k != "slab_names"}))


if __name__ == "__main__":
    main()
