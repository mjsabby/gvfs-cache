"""SearchGold-scale correctness: everything judged by stock git.

  * cold prefetch: exactly 1 pack; `git index-pack` succeeds; a sent .idx is byte-identical to git's;
    the pack's object set equals `rev-list --objects main` exactly; connectivity of main (rev-list
    --missing=print) inside a bare repo that holds only that pack
  * POST tip closure (commitDepth=1): every tree of the tip
  * GET 3,000 sampled objects (all types): loose format, hash, content == git
  * POST /gvfs/sizes 4,000 sampled ids == cat-file sizes; POST 4,000 non-commits -> exact objects
"""
import hashlib
import http.client
import json
import os
import random
import shutil
import struct
import subprocess
import sys
import time

import impls
import load_bench
from gvfslib import clean_env, git, git_out, request, check_loose, cat_batch, init_bare

SG = load_bench.SG
UP = os.path.join(SG, "up.git")
TIP = "4d12e2c7915577f43351ac82b9ca15bd7788be89"


def stream_to(port, method, path, body, out_prefix, headers=None):
    """Stream a GPRE or pack response to disk. Returns dict with files and timings."""
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=1800)
    t0 = time.perf_counter()
    h = dict(headers or {})
    if body is not None:
        body = json.dumps(body).encode()
        h["Content-Type"] = "application/json"
    c.request(method, path, body=body, headers=h)
    r = c.getresponse()
    status = r.status
    ttfb = time.perf_counter() - t0
    out = {"status": status, "ttfb_s": ttfb}
    if status != 200:
        out["body"] = r.read()[:300]
        return out
    if path.split("?")[0].endswith("/prefetch"):
        head = r.read(8)
        n = struct.unpack_from("<H", head, 6)[0]
        out["packs"] = n
        if n != 1:
            out["tail"] = r.read()[:100]
            return out
        ts, plen, ilen = struct.unpack("<qqq", r.read(24))
        out.update(ts=ts, pack_len=plen, idx_len=ilen)
        segs = [("pack", plen)] + ([("idx", ilen)] if ilen >= 0 else [])
    else:
        segs = [("pack", None)]
    for kind, n in segs:
        path_ = f"{out_prefix}.{kind}"
        sha = hashlib.sha1()
        got = 0
        with open(path_, "wb") as f:
            while n is None or got < n:
                chunk = r.read(min(8 << 20, (n - got)) if n is not None else 8 << 20)
                if not chunk:
                    break
                f.write(chunk)
                got += len(chunk)
        out[f"{kind}_file"], out[f"{kind}_bytes"] = path_, got
    rest = r.read()
    out["trailing"] = len(rest)
    out["total_s"] = time.perf_counter() - t0
    c.close()
    return out


def verify(name, out_root):
    work = os.path.join(SG, name)
    vdir = os.path.join(SG, "verify", name)
    shutil.rmtree(vdir, ignore_errors=True)
    os.makedirs(vdir)
    res = {"impl": name}
    im = impls.ALL[name](work, extra_serve=load_bench.SERVE_EXTRA.get(name, []))
    srv = im.serve(UP, live=False, ready_timeout=1800)
    res["ready_s"] = srv.ready_secs
    try:
        # ---------------- cold prefetch
        cp = stream_to(srv.port, "GET", "/org/repo/gvfs/prefetch?lastPackTimestamp=0", None,
                       os.path.join(vdir, "cold"))
        res["cold"] = {k: v for k, v in cp.items() if not k.endswith("_file")}
        if cp.get("packs") == 1 and cp.get("trailing") == 0:
            repo = init_bare(os.path.join(vdir, "cold.git"))
            pk = os.path.join(repo, "objects", "pack", "pack-cold.pack")
            os.rename(cp["pack_file"], pk)
            t0 = time.time()
            p = subprocess.run(["git", f"--git-dir={repo}", "index-pack", "--threads=16", pk],
                               capture_output=True, env=clean_env())
            res["index_pack_ok"] = p.returncode == 0
            res["index_pack_s"] = round(time.time() - t0, 1)
            if p.returncode != 0:
                res["index_pack_err"] = p.stderr.decode()[-500:]
            else:
                gidx = pk[:-5] + ".idx"
                if "idx_file" in cp:
                    res["idx_identical"] = open(cp["idx_file"], "rb").read() == open(gidx, "rb").read()
                n = struct.unpack(">I", open(pk, "rb").read(12)[8:12])[0]
                res["pack_objects"] = n
                # exact object set: show-index lists every object in git's idx
                with open(gidx, "rb") as f:
                    si = subprocess.run(["git", "show-index"], stdin=f, capture_output=True, env=clean_env())
                got = sorted(l.split()[1] for l in si.stdout.decode().splitlines())
                want = sorted(open(os.path.join(SG, "oids", "all-main.txt")).read().split())
                res["object_set_equal"] = got == want
                if got != want:
                    sg, sw = set(got), set(want)
                    res["missing_from_pack"] = len(sw - sg)
                    res["extra_in_pack"] = len(sg - sw)
                git(repo, "update-ref", "refs/heads/main", TIP)
                t0 = time.time()
                p = subprocess.run(["git", f"--git-dir={repo}", "rev-list", "--objects", "--missing=print",
                                    "main"], capture_output=True, env=clean_env())
                miss = [l for l in p.stdout.decode().splitlines() if l.startswith("?")]
                res["connectivity_ok"] = p.returncode == 0 and not miss
                res["connectivity_missing"] = len(miss)
                res["connectivity_s"] = round(time.time() - t0, 1)
            shutil.rmtree(repo, ignore_errors=True)
        for f in ("cold.pack", "cold.idx"):
            if os.path.exists(os.path.join(vdir, f)):
                os.remove(os.path.join(vdir, f))
        # ---------------- tip closure
        cl = stream_to(srv.port, "POST", "/org/repo/gvfs/objects", {"objectIds": [TIP], "commitDepth": 1},
                       os.path.join(vdir, "closure"))
        res["closure"] = {k: v for k, v in cl.items() if not k.endswith("_file")}
        if cl.get("status") == 200:
            repo = init_bare(os.path.join(vdir, "closure.git"))
            pk = os.path.join(repo, "objects", "pack", "pack-cl.pack")
            os.rename(cl["pack_file"], pk)
            p = subprocess.run(["git", f"--git-dir={repo}", "index-pack", "--threads=16", pk],
                               capture_output=True, env=clean_env())
            res["closure_index_ok"] = p.returncode == 0
            if p.returncode == 0:
                with open(pk[:-5] + ".idx", "rb") as f:
                    si = subprocess.run(["git", "show-index"], stdin=f, capture_output=True, env=clean_env())
                got = {l.split()[1] for l in si.stdout.decode().splitlines()}
                need_file = os.path.join(SG, "oids", "tip-closure.txt")
                if not os.path.exists(need_file):
                    out = git_out(UP, "rev-list", "--objects", "--no-walk", "--filter=blob:none", TIP)
                    open(need_file, "w").write("\n".join(l.split()[0] for l in out.splitlines()) + "\n")
                need = set(open(need_file).read().split())
                res["closure_complete"] = need <= got
                res["closure_missing"] = len(need - got)
                res["closure_extra"] = len(got - need)
            shutil.rmtree(repo, ignore_errors=True)
        # ---------------- sampled GET / sizes / POST
        rows = [l.split() for l in open(os.path.join(SG, "oids", "typed.txt"))]
        rng = random.Random(99)
        sample = rng.sample(rows, 3000)
        ref = cat_batch(UP, [o for o, _, _ in sample])
        bad = []
        t0 = time.time()
        for o, t, s in sample:
            r = request(srv.port, "GET", f"/org/repo/gvfs/objects/{o}")
            if r.status != 200:
                bad.append((o, t, r.status))
                continue
            try:
                if check_loose(o, r.body) != ref[o]:
                    bad.append((o, t, "content"))
            except Exception as e:
                bad.append((o, t, str(e)[:50]))
        res["get_sample_bad"] = bad[:10]
        res["get_sample_bad_n"] = len(bad)
        res["get_sample_s"] = round(time.time() - t0, 2)
        ids = [o for o, _, _ in rng.sample(rows, 4000)]
        sz = dict((o, int(s)) for o, _, s in rows)
        r = request(srv.port, "POST", "/gvfs/sizes", ids)
        try:
            js = json.loads(r.body)
            res["sizes_ok"] = js == [{"Id": o, "Size": sz[o]} for o in ids]
        except Exception as e:
            res["sizes_ok"] = f"{r.status} {e}"
        nonc = [o for o, t, _ in rng.sample(rows, 6000) if t != "commit"][:4000]
        r = request(srv.port, "POST", "/gvfs/objects", {"objectIds": nonc})
        if r.status == 200:
            repo = init_bare(os.path.join(vdir, "post.git"))
            p = subprocess.run(["git", f"--git-dir={repo}", "index-pack", "--stdin"], input=r.body,
                               capture_output=True, env=clean_env())
            if p.returncode == 0:
                pk = os.path.join(repo, "objects", "pack", f"pack-{p.stdout.decode().split()[-1]}.idx")
                with open(pk, "rb") as f:
                    si = subprocess.run(["git", "show-index"], stdin=f, capture_output=True, env=clean_env())
                got = {l.split()[1] for l in si.stdout.decode().splitlines()}
                res["post4000_complete"] = set(nonc) <= got
                res["post4000_extra"] = len(got - set(nonc))
            else:
                res["post4000_complete"] = "index-pack failed: " + p.stderr.decode()[-200:]
            shutil.rmtree(repo, ignore_errors=True)
        else:
            res["post4000_complete"] = f"status {r.status}"
        res["metrics"] = {k: v for k, v in srv.metrics().items() if "alloc" in k or "error" in k}
        res["alive"] = srv.alive()
    finally:
        srv.stop()
    print(json.dumps(res, indent=1, default=str), flush=True)
    with open(os.path.join(vdir, "result.json"), "w") as f:
        json.dump(res, f, indent=1, default=str)
    return res


if __name__ == "__main__":
    for n in sys.argv[1:]:
        verify(n, SG)
