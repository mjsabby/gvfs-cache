"""Black-box JSON request variants (from sol W07 / astra U-JSON) against every server.

Real clients differ: gvfs-helper sends compact {"commitDepth":1,"objectIds":[...]}, VFS for Git's
serializer {"objectIds":[...],"commitDepth":1}, others pretty-print.  Every variant below is valid
JSON and must be answered exactly (stock git checks the pack); malformed ones must be 4xx.
"""
import json
import os
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor

import corpus
import impls
from gvfslib import request, index_pack, cat_batch, rev_list_objects, git_out

BASE = "/home/muks/gvfs-bench/json"


def run(name, src, oids):
    work = os.path.join(BASE, name)
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    up = corpus.make_upstream(src, os.path.join(work, "up.git"), oids["c12"])
    im = impls.ALL[name](work)
    im.step(up, "15m")
    srv = im.serve(up, poll_s=1, live=False)
    res = {}
    try:
        allo = sorted(rev_list_objects(up, oids["c12"]))
        ref = cat_batch(up, allo)
        blobs = [o for o in allo if ref[o][0] == "blob"][:3]
        tree = [o for o in allo if ref[o][0] == "tree"][0]
        ids = blobs + [tree]
        q = lambda l: ", ".join(f'"{x}"' for x in l)
        valid = {
            "compact": '{"objectIds":[%s],"commitDepth":1}' % q(ids).replace(" ", ""),
            "depth_first": '{"commitDepth":1,"objectIds":[%s]}' % q(ids),
            "pretty": '{\n  "objectIds" : [\n    %s\n  ],\n  "commitDepth" : 1\n}' % ",\n    ".join(f'"{x}"' for x in ids),
            "no_depth": '{"objectIds":[%s]}' % q(ids),
            "unknown_fields": '{"x":{"y":[1,2,{"z":"q"}]},"objectIds":[%s],"commitDepth":1,"w":null}' % q(ids),
            "uppercase_hex": '{"objectIds":[%s],"commitDepth":1}' % q([x.upper() for x in ids]),
            "duplicate_ids": '{"objectIds":[%s],"commitDepth":1}' % q(ids + ids),
            "tabs_crlf": '{\t"objectIds":\r\n[%s],\r\n"commitDepth":\t1}' % q(ids),
        }
        for k, body in valid.items():
            r = request(srv.port, "POST", "/org/repo/gvfs/objects", body.encode(),
                        headers={"Content-Type": "application/json"})
            ok = False
            detail = r.status
            if r.status == 200:
                try:
                    _, _, objs = index_pack(r.body, os.path.join(work, "ip"))
                    got = {o[0] for o in objs}
                    ok = set(ids) <= got
                    detail = f"{len(got)} objects"
                except Exception as e:
                    detail = str(e)[:80]
            res[f"objects:{k}"] = "PASS" if ok else f"FAIL {detail}"
        sizes = {
            "compact": "[%s]" % q(ids).replace(" ", ""),
            "pretty": "[\n  %s\n]" % ",\n  ".join(f'"{x}"' for x in ids),
            "uppercase_hex": "[%s]" % q([x.upper() for x in ids]),
        }
        for k, body in sizes.items():
            r = request(srv.port, "POST", "/org/repo/gvfs/sizes", body.encode(),
                        headers={"Content-Type": "application/json"})
            ok = False
            try:
                js = json.loads(r.body)
                ok = r.status == 200 and [d["Size"] for d in js] == [len(ref[o][1]) for o in ids]
            except Exception:
                pass
            res[f"sizes:{k}"] = "PASS" if ok else f"FAIL {r.status} {r.body[:80]!r}"
        bad = {
            "trailing_garbage": '{"objectIds":["%s"]} xyz' % ids[0],
            "not_json": "objectIds=%s" % ids[0],
            "wrong_type": '{"objectIds":"%s"}' % ids[0],
            "short_oid": '{"objectIds":["%s"]}' % ids[0][:39],
        }
        for k, body in bad.items():
            r = request(srv.port, "POST", "/org/repo/gvfs/objects", body.encode(),
                        headers={"Content-Type": "application/json"})
            res[f"malformed:{k}"] = r.status
    finally:
        srv.stop()
    return name, res


def main():
    names = sys.argv[1:] or ["opus", "fable", "sol", "astra"]
    os.makedirs(BASE, exist_ok=True)
    src = os.path.join(BASE, "src.git")
    shutil.rmtree(src, ignore_errors=True)
    oids = corpus.build_source(src, n=20)
    with ThreadPoolExecutor(len(names)) as ex:
        out = dict(ex.map(lambda n: run(n, src, oids), names))
    keys = sorted({k for r in out.values() for k in r})
    print(f"{'case':28s} " + " ".join(f"{n:>10s}" for n in names))
    for k in keys:
        print(f"{k:28s} " + " ".join(f"{str(out[n].get(k))[:10]:>10s}" for n in names))
    json.dump(out, open(os.path.join(BASE, "summary.json"), "w"), indent=1)


if __name__ == "__main__":
    main()
