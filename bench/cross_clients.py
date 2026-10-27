"""Cross-testing clients x servers.

Clients: Microsoft git's gvfs-helper (what Scalar / VFS for Git agents and humans run),
fable's `client-prefetch` hook, astra's `sync` managed cache.  Each client is run against
every server, through the same push / live / seal / push sequence, and judged by stock git.
"""
import glob
import json
import os
import shutil
import subprocess
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

import corpus
import impls
from gvfslib import clean_env, git, git_out, init_bare, missing_objects, request, rev_list_objects

BASE = os.environ.get("BENCH_SCRATCH", "/home/muks/gvfs-bench/cross")
MSGIT = os.environ.get("GVFS_TEST_MSGIT", os.path.expanduser("~/git/bin-wrappers/git"))


def run(cmd, cwd=None, input=b"", env=None, timeout=300):
    p = subprocess.run(cmd, cwd=cwd, input=input, capture_output=True, timeout=timeout,
                       env=clean_env(env))
    return p.returncode, (p.stdout + p.stderr).decode(errors="replace")


def packs_in(d):
    return sorted(glob.glob(os.path.join(d, "*.pack")))


class Client:
    name = "?"

    def __init__(self, work, url):
        self.work, self.url = work, url

    def objects_repo(self):
        raise NotImplementedError


class GvfsHelper(Client):
    name = "gvfs-helper"

    def __init__(self, work, url):
        super().__init__(work, url)
        self.repo = os.path.join(work, "gh")
        git(None, "init", "-q", self.repo)
        git(os.path.join(self.repo, ".git"), "config", "remote.origin.url", url + "/org/repo")
        self.gitdir = os.path.join(self.repo, ".git")
        self.packdir = os.path.join(self.gitdir, "objects", "pack")

    def helper(self, cmd, stdin_oids=()):
        return run([MSGIT, "gvfs-helper", "--cache-server=disable", cmd], cwd=self.repo,
                   input=("\n".join(stdin_oids)).encode())

    def prefetch(self):
        return self.helper("prefetch")

    def objects_repo(self):
        return self.gitdir


class FableHook(Client):
    name = "fable-client-prefetch"

    def __init__(self, work, url):
        super().__init__(work, url)
        self.gitdir = init_bare(os.path.join(work, "fh.git"))
        self.packdir = os.path.join(self.gitdir, "objects", "pack")

    def prefetch(self):
        return run([impls.binary_path("fable"), "client-prefetch",
                    "--server", self.url + "/org/repo", "--repo", self.gitdir, "--midx"])

    def objects_repo(self):
        return self.gitdir


class AstraSync(Client):
    name = "astra-sync"

    def __init__(self, work, url):
        super().__init__(work, url)
        self.gitdir = os.path.join(work, "as-cache")
        self.packdir = os.path.join(self.gitdir, "objects", "pack")

    def prefetch(self):
        return run([impls.binary_path("astra"), "sync",
                    "--server", self.url + "/org/repo", "--cache-dir", self.gitdir])

    def objects_repo(self):
        return self.gitdir


CLIENTS = [GvfsHelper, FableHook, AstraSync]


def scenario(server_name, client_cls, src, oids):
    work = os.path.join(BASE, f"{server_name}--{client_cls.name}")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    res = {"server": server_name, "client": client_cls.name, "steps": []}
    srv = None

    def step(label, ok, detail=""):
        res["steps"].append((label, bool(ok), "" if ok else detail[-500:]))

    try:
        mysrc = os.path.join(work, "src.git")
        shutil.copytree(src, mysrc, symlinks=True)
        up = corpus.make_upstream(mysrc, os.path.join(work, "up.git"), oids["c10"])
        im = impls.ALL[server_name](work)
        im.step(up, "15m")
        srv = im.serve(up, poll_s=1)
        url = f"http://127.0.0.1:{srv.port}"
        os.makedirs(os.path.join(work, "client"), exist_ok=True)
        c = client_cls(os.path.join(work, "client"), url)
        rc, out = c.prefetch()
        step("cold:rc0", rc == 0, out)
        n_cold = len(packs_in(c.packdir))
        step("cold:one_pack", n_cold == 1, f"{n_cold} packs: {out}")
        miss = missing_objects(c.objects_repo(), oids["c10"])
        step("cold:complete", not miss, f"{len(miss)} missing")
        # live push
        corpus.move_main(up, oids["c15"])
        t0 = time.time()
        while time.time() - t0 < 30 and request(srv.port, "GET", f"/gvfs/objects/{oids['c15']}").status != 200:
            time.sleep(0.2)
        rc, out = c.prefetch()
        step("live:rc0", rc == 0, out)
        miss = missing_objects(c.objects_repo(), oids["c15"])
        step("live:complete", not miss, f"{len(miss)} missing; {out}")
        res["packs_after_live"] = len(packs_in(c.packdir))
        # repeated polls with nothing new must not add packs
        before = len(packs_in(c.packdir))
        for _ in range(3):
            c.prefetch()
        res["packs_after_idle_polls"] = len(packs_in(c.packdir)) - before
        step("idle_polls:no_new_packs", len(packs_in(c.packdir)) == before,
             f"{len(packs_in(c.packdir)) - before} extra packs")
        # seal, then poll
        im.step(up, "15m")
        time.sleep(2.6)
        rc, out = c.prefetch()
        step("sealed:rc0", rc == 0, out)
        miss = missing_objects(c.objects_repo(), oids["c15"])
        step("sealed:complete", not miss, f"{len(miss)} missing")
        # more pushes, a rollup, and a weekly; client keeps up
        for k, tier in ((20, "1h"), (25, "15m"), (30, "168h")):
            corpus.move_main(up, oids[f"c{k}"])
            im.step(up, tier)
            time.sleep(2.6)
            rc, out = c.prefetch()
            step(f"c{k}-{tier}:rc0", rc == 0, out)
            miss = missing_objects(c.objects_repo(), oids[f"c{k}"])
            step(f"c{k}-{tier}:complete", not miss, f"{len(miss)} missing; {out}")
        res["packs_final"] = len(packs_in(c.packdir))
        # gvfs-helper only: fault-in by GET and POST
        if client_cls is GvfsHelper:
            corpus.move_main(up, oids["c35"])
            t0 = time.time()
            while time.time() - t0 < 30 and request(srv.port, "GET", f"/gvfs/objects/{oids['c35']}").status != 200:
                time.sleep(0.2)
            new = [o for o in rev_list_objects(up, oids["c35"])
                   if git(c.gitdir, "cat-file", "-e", o, check=False).returncode != 0]
            half = len(new) // 2
            rc1, out1 = c.helper("get", new[:half])
            rc2, out2 = c.helper("post", new[half:])
            step("faultin:get_rc0", rc1 == 0, out1)
            step("faultin:post_rc0", rc2 == 0, out2)
            miss = missing_objects(c.gitdir, oids["c35"])
            step("faultin:complete", not miss, f"{len(miss)} missing")
        res["server_alive"] = srv.alive()
        step("server_alive", srv.alive(), srv.log_tail(5))
    except Exception as e:
        step("exception", False, f"{type(e).__name__}: {e}\n{traceback.format_exc()[-800:]}")
    finally:
        if srv:
            srv.stop()
    return res


def main():
    servers = [a for a in sys.argv[1:] if a in impls.ALL] or ["opus", "fable", "sol", "astra"]
    os.makedirs(BASE, exist_ok=True)
    src = os.path.join(BASE, "src.git")
    shutil.rmtree(src, ignore_errors=True)
    oids = corpus.build_source(src, n=40)
    jobs = [(s, c) for s in servers for c in CLIENTS]
    with ThreadPoolExecutor(8) as ex:
        out = list(ex.map(lambda j: scenario(j[0], j[1], src, oids), jobs))
    for r in out:
        fails = [s for s in r["steps"] if not s[1]]
        print(f"{r['server']:6s} x {r['client']:22s}: {len(r['steps']) - len(fails)}/{len(r['steps'])} ok"
              f"  packs final={r.get('packs_final')} idle-poll extra={r.get('packs_after_idle_polls')}")
        for f in fails[:6]:
            print("     FAIL", f[0], f[2][:300].replace("\n", " | "))
    with open(os.path.join(BASE, "summary.json"), "w") as f:
        json.dump(out, f, indent=1)


if __name__ == "__main__":
    main()
