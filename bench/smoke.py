"""Smoke triage: genesis, serve, cold prefetch completeness, GET/POST/sizes, for each impl."""
import json
import os
import shutil
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

import corpus
import impls
from gvfslib import (parse_gpre, index_pack, install_pack, missing_objects, init_bare, request,
                     check_loose, cat_batch, git_out, rev_list_objects)

BASE = os.environ.get("BENCH_SCRATCH", "/home/muks/gvfs-bench/smoke")


def smoke(name, src, oids):
    out = {"impl": name, "checks": {}}
    work = os.path.join(BASE, name)
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    up = corpus.make_upstream(src, os.path.join(work, "up.git"), oids["c5"])
    im = impls.ALL[name](work)
    srv = None

    def ck(k, ok, detail=""):
        out["checks"][k] = "PASS" if ok else f"FAIL {detail}"

    try:
        t = im.step(up, "15m")
        out["genesis_s"] = round(t, 3)
        srv = im.serve(up, poll_s=1)
        out["ready_s"] = round(srv.ready_secs, 3)
        r = request(srv.port, "GET", "/gvfs/config")
        ck("config", r.status == 200 and isinstance(json.loads(r.body), (dict, list)), r.status)
        r = request(srv.port, "GET", "/org/repo/gvfs/prefetch?lastPackTimestamp=0")
        packs = parse_gpre(r.body)
        ck("cold_one_pack", r.status == 200 and len(packs) == 1, f"status={r.status} packs={len(packs)}")
        ts, pack, idx = packs[0]
        out["cold_ts"] = ts
        out["cold_pack_bytes"] = len(pack)
        out["cold_idx"] = None if idx is None else len(idx)
        _, _, objs = index_pack(pack, os.path.join(work, "ip"), strict=False, verify_idx=idx)
        client = init_bare(os.path.join(work, "client.git"))
        install_pack(client, pack)
        miss = missing_objects(client, oids["c5"])
        ck("cold_complete", not miss, f"{len(miss)} missing e.g. {miss[:3]}")
        want = rev_list_objects(up, oids["c5"])
        got = {o[0] for o in objs}
        out["cold_objects"] = len(got)
        out["cold_extra"] = len(got - want)
        r = request(srv.port, "GET", f"/gvfs/prefetch?lastPackTimestamp={ts}")
        p2 = parse_gpre(r.body)
        ck("uptodate_zero", r.status == 200 and len(p2) == 0, f"{r.status} packs={len(p2)}")
        # GET every object of c5
        allo = sorted(want)
        ref = cat_batch(up, allo)
        bad = []
        for o in allo:
            r = request(srv.port, "GET", f"/org/repo/gvfs/objects/{o}")
            if r.status != 200:
                bad.append((o, r.status))
                continue
            try:
                t_, content = check_loose(o, r.body)
                if (t_, content) != ref[o]:
                    bad.append((o, "content"))
            except Exception as e:
                bad.append((o, str(e)[:80]))
        ck("get_all", not bad, f"{len(bad)} bad e.g. {bad[:3]}")
        r = request(srv.port, "GET", "/gvfs/objects/" + "f" * 40)
        ck("get_unknown_404", r.status == 404, r.status)
        # POST blobs+trees
        nonc = [o for o in allo if ref[o][0] != "commit"][:50]
        r = request(srv.port, "POST", "/gvfs/objects", {"objectIds": nonc, "commitDepth": 1},
                    headers={"Accept": "application/x-git-packfile"})
        if r.status == 200:
            _, _, pobjs = index_pack(r.body, os.path.join(work, "ip"))
            ck("post_exact", {o[0] for o in pobjs} == set(nonc), f"{len(pobjs)} vs {len(nonc)}")
        else:
            ck("post_exact", False, r.status)
        # commit closure
        r = request(srv.port, "POST", "/gvfs/objects", {"objectIds": [oids["c5"]], "commitDepth": 1})
        trees = set(git_out(up, "rev-list", "--objects", "--no-walk", "--filter=blob:none",
                            oids["c5"]).split())
        if r.status == 200:
            _, _, pobjs = index_pack(r.body, os.path.join(work, "ip"))
            gotc = {o[0] for o in pobjs}
            want_c = {l.split()[0] for l in git_out(up, "rev-list", "--objects", "--no-walk",
                                                     "--filter=blob:none", oids["c5"]).splitlines()}
            ck("closure", want_c <= gotc, f"missing {len(want_c - gotc)} extra {len(gotc - want_c)}")
            out["closure_extra"] = len(gotc - want_c)
        else:
            ck("closure", False, r.status)
        # sizes
        sample = allo[:40]
        r = request(srv.port, "POST", "/gvfs/sizes", sample)
        if r.status == 200:
            js = json.loads(r.body)
            exp = [{"Id": o, "Size": len(ref[o][1])} for o in sample]
            ck("sizes", js == exp, str(js[:2]))
        else:
            ck("sizes", False, r.status)
        m = srv.metrics()
        out["metrics_hot"] = {k: v for k, v in m.items() if "alloc" in k and "hot" in k}
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"[:3000]
        out["trace"] = traceback.format_exc()[-1500:]
    finally:
        if srv:
            srv.stop()
    return out


def main():
    names = sys.argv[1:] or list(impls.ALL)
    os.makedirs(BASE, exist_ok=True)
    src = os.path.join(BASE, "src.git")
    shutil.rmtree(src, ignore_errors=True)
    oids = corpus.build_source(src, n=40)
    with ThreadPoolExecutor(len(names)) as ex:
        res = list(ex.map(lambda n: smoke(n, src, oids), names))
    for r in res:
        print(json.dumps({k: v for k, v in r.items() if k != "trace"}, indent=1))
        if "trace" in r:
            print(r["trace"])


if __name__ == "__main__":
    main()
