"""SearchGold-scale genesis for each implementation (sequential; wall/CPU/RSS via GNU time)."""
import json
import os
import re
import shutil
import subprocess
import sys
import time

import impls
from gvfslib import clean_env

SG = "/home/muks/gvfs-bench/sg"
UP = os.path.join(SG, "up.git")
SOL_CAP = ["--max-objects", "16777216", "--max-links", "2147483647", "--pack-mib", "65536",
           "--object-mib", "128", "--delta-cache-mib", "512"]


def make(name, work):
    extra = SOL_CAP if name == "sol" else []
    return impls.ALL[name](work, extra_pipeline=extra)


def timed(cmd, env, log, cwd):
    tv = log + ".time"
    with open(log, "wb") as f:
        p = subprocess.run(["/usr/bin/time", "-v", "-o", tv] + cmd, stdout=f, stderr=subprocess.STDOUT,
                           env=env, cwd=cwd)
    stats = {}
    for line in open(tv):
        line = line.strip()
        for k, rk in (("Elapsed (wall clock) time (h:mm:ss or m:ss)", "wall"),
                      ("Maximum resident set size (kbytes)", "maxrss_kb"),
                      ("User time (seconds)", "user_s"), ("System time (seconds)", "sys_s"),
                      ("File system outputs", "fs_out")):
            if line.startswith(k):
                stats[rk] = line.split(": ", 1)[1]
    w = stats.get("wall", "0:0")
    parts = [float(x) for x in w.split(":")]
    secs = parts[-1] + 60 * parts[-2] + (3600 * parts[-3] if len(parts) > 2 else 0)
    stats["wall_s"] = secs
    return p.returncode, stats


def main():
    names = sys.argv[1:] or ["opus", "fable", "sol", "astra"]
    results = {}
    for name in names:
        work = os.path.join(SG, name)
        shutil.rmtree(work, ignore_errors=True)
        os.makedirs(work)
        im = make(name, work)
        cmds = [im.pipeline_cmd(UP, "15m", None)]
        t0 = time.time()
        rc, st = timed(cmds[0], clean_env(im.pipeline_env(None)), os.path.join(work, "genesis.log"), work)
        st["rc"] = rc
        du = subprocess.run(["du", "-sb", im.store], capture_output=True, text=True).stdout.split()
        st["store_bytes"] = int(du[0]) if du else None
        results[name] = st
        print(name, json.dumps(st), flush=True)
        with open(os.path.join(SG, "genesis.json"), "w") as f:
            json.dump(results, f, indent=1)


if __name__ == "__main__":
    main()
