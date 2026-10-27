"""Two builds' edges on one store in object storage, the same requests to both: every
response must be byte-identical, and right by git. It covers what reads a blob pack that
stays in the store: loose blobs, packs (batches, and a large blob's pack of its own), and
sizes. Each runs with the edge's block cache at its default size, at a size that long
reads stream past, and off.

It builds its store in Azurite (`azurite-blob` on PATH) from a generated upstream: small
text, random binaries around the stored-block (1 MiB) and own-pack (16 MiB) thresholds,
and long compressible text (compressed entries of many MiB), over three commits.

    PATH=$HOME/.local/bin:$PATH python3 bench/ab_remote.py --base <old build> --new <new build>
"""
import argparse
import base64
import email.utils
import hashlib
import hmac
import json
import os
import random
import shutil
import subprocess
import sys
import time
import zlib

sys.path.insert(0, os.path.dirname(__file__))
from gvfslib import clean_env, free_port, metrics, request, wait_http  # noqa: E402

PACK = "application/x-git-packfile"
LOOSE = "application/x-git-loose-object"
ACCOUNT = "devstoreaccount1"
# Azurite's published development key.
KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
MIB = 1 << 20
# --blob-cache-mib: the default; small enough that reads over 12 MiB stream past it; off.
CACHES = (("default", []), ("small", ["--blob-cache-mib", "48"]), ("off", ["--blob-cache-mib", "0"]))


def text(n, seed):
    """`n` bytes of compressible text."""
    rnd = random.Random(seed)
    words = ["alpha", "beta", "gamma", "delta", "kernel", "search", "index", "query", "shard", "route"]
    out, size = [], 0
    while size < n:
        line = f"{size:09d} " + " ".join(rnd.choice(words) for _ in range(rnd.randint(4, 14))) + "\n"
        out.append(line)
        size += len(line)
    return "".join(out).encode()[:n]


def content(spec):
    kind, n, seed = spec
    return text(n, seed) if kind == "text" else random.Random(seed).randbytes(n)


def make_upstream(path):
    """Three commits on main; returns every blob's content by id."""
    files = {}
    for i in range(300):
        files[f"t/{i % 7}/f{i}.txt"] = ("text", 1000 + (i * 7919) % 60000, i)
    sizes = [64 << 10, 300 << 10, 900 << 10, MIB, MIB + 77, 2 * MIB, 3 * MIB + 5, 5 * MIB - 1]
    for i, n in enumerate(sizes):
        files[f"m/r{i}.bin"] = ("binary", n, 1000 + i)
    for i, n in enumerate([9 * MIB + 3, 16 * MIB, 17 * MIB + 11, 34 * MIB + 1]):
        files[f"l/r{i}.bin"] = ("binary", n, 2000 + i)
    for i, n in enumerate([1536 << 10, 6 * MIB + 9, 18 * MIB + 1, 30 * MIB]):
        files[f"c/t{i}.txt"] = ("text", n, 3000 + i)
    second = dict(files)
    for i in range(0, 300, 15):
        second[f"t/{i % 7}/f{i}.txt"] = ("text", 1000 + (i * 31) % 50000, 5000 + i)
    second["m/r6.bin"] = ("binary", 3 * MIB + 5, 6001)
    second["l/r2.bin"] = ("binary", 17 * MIB + 11, 6002)
    second["c/t2.txt"] = ("text", 18 * MIB + 1, 6003)
    third = dict(second)
    third["n/new.txt"] = ("text", 4000, 7000)
    third["l/r4.bin"] = ("binary", 20 * MIB, 7001)
    del third["m/r0.bin"]
    subprocess.run(["git", "init", "-q", "--bare", "-b", "main", path], check=True, env=clean_env())
    fi = subprocess.Popen(["git", "--git-dir", path, "fast-import", "--quiet"], stdin=subprocess.PIPE,
                          env=clean_env())
    marks, blobs = {}, {}
    for c, tree in enumerate((files, second, third)):
        for p, spec in sorted(tree.items()):
            if spec not in marks:
                data = content(spec)
                marks[spec] = len(marks) + 1
                head = f"blob {len(data)}\0".encode()
                blobs[hashlib.sha1(head + data).hexdigest()] = data
                fi.stdin.write(f"blob\nmark :{marks[spec]}\ndata {len(data)}\n".encode() + data + b"\n")
        msg = f"c{c}\n".encode()
        fi.stdin.write(f"commit refs/heads/main\ncommitter t <t@t> {1790812800 + c * 3600} +0000\n"
                       f"data {len(msg)}\n".encode() + msg + b"deleteall\n")
        for p, spec in sorted(tree.items()):
            fi.stdin.write(f"M 100644 :{marks[spec]} {p}\n".encode())
        fi.stdin.write(b"\n")
    fi.stdin.close()
    assert fi.wait() == 0
    return blobs


def azurite(root):
    """Azurite on disk under `root`, with a container `gvfs`; (process, connection string)."""
    port = free_port()
    p = subprocess.Popen(["azurite-blob", "--blobHost", "127.0.0.1", "--blobPort", str(port), "--silent",
                          "--skipApiVersionCheck", "--location", os.path.join(root, "azurite")],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    endpoint = f"http://127.0.0.1:{port}/{ACCOUNT}"
    deadline = time.time() + 30
    while True:
        try:
            date = email.utils.formatdate(usegmt=True)
            version = "2021-08-06"
            sts = ("PUT\n" + "\n" * 11 + f"x-ms-date:{date}\nx-ms-version:{version}\n"
                   + f"/{ACCOUNT}/{ACCOUNT}/gvfs\nrestype:container")
            sig = base64.b64encode(hmac.new(base64.b64decode(KEY), sts.encode(), hashlib.sha256).digest())
            r = request(port, "PUT", f"/{ACCOUNT}/gvfs?restype=container", body=b"",
                        headers={"x-ms-date": date, "x-ms-version": version,
                                 "Authorization": f"SharedKey {ACCOUNT}:{sig.decode()}"})
            assert r.status in (201, 409), r.status
            break
        except ConnectionRefusedError:
            assert time.time() < deadline and p.poll() is None, "Azurite did not start"
            time.sleep(0.2)
    cs = (f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT};AccountKey={KEY};"
          f"BlobEndpoint={endpoint};")
    return p, cs


def run(cmd, env):
    p = subprocess.run(cmd, capture_output=True, env=env)
    assert p.returncode == 0, (cmd[:3], p.stderr.decode(errors="replace")[-3000:])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True)
    ap.add_argument("--new", required=True)
    ap.add_argument("--work", default=os.path.expanduser("~/gvfs-bench/ab-remote"))
    a = ap.parse_args()
    shutil.rmtree(a.work, ignore_errors=True)
    os.makedirs(a.work)
    up = os.path.join(a.work, "up.git")
    t0 = time.time()
    blobs = make_upstream(up)
    print(f"upstream: {len(blobs)} blobs, {sum(map(len, blobs.values())) >> 20} MiB "
          f"({time.time() - t0:.1f}s)", flush=True)
    az, cs = azurite(a.work)
    env = clean_env({"AZURE_STORAGE_CONNECTION_STRING": cs})
    spec = "azure:gvfs/ab"
    servers = []
    try:
        run([a.new, "pipeline", "--store", spec, "--upstream", up, "--tier", "15m", "--threads", "4",
             "--work-dir", os.path.join(a.work, "pipeline-work"), "--blobs", "separate"], env)
        run([a.new, "mirror", "--store", spec, "--upstream", up, "--work-dir", os.path.join(a.work, "mirror-work"),
             "--publish-secs", "0", "--blob-pack-mib", "4", "--jobs", "4"], env)
        print(f"store built in Azurite ({time.time() - t0:.1f}s)", flush=True)
        ids = sorted(blobs)
        rnd = random.Random(7)
        large = [i for i in ids if len(blobs[i]) >= 16 * MIB]
        stats = {}
        for name, extra in CACHES:
            servers = []
            for t, binary in (("base", a.base), ("new", a.new)):
                port = free_port()
                cache = os.path.join(a.work, f"cache-{name}-{t}")
                cmd = [binary, "serve", "--store", spec, "--bind", f"127.0.0.1:{port}", "--cache-dir", cache,
                       "--alloc-guard", "strict", *extra]
                log = open(os.path.join(a.work, f"serve-{name}-{t}.log"), "wb")
                p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=log, env=env)
                servers.append((t, p, port))
                wait_http(port, "/healthz", timeout=120, proc=p)
            ports = {t: port for t, _, port in servers}
            n = {"identical": 0, "judged": 0}

            def both(method, path, body=None, headers=None):
                rs = {t: request(port, method, path, body=body, headers=headers or {}, timeout=600)
                      for t, port in ports.items()}
                b, x = rs["base"], rs["new"]
                assert (b.status, b.headers.get("content-type")) == (x.status, x.headers.get("content-type")), \
                    (name, path, b.status, x.status)
                assert b.body == x.body, (name, path, len(b.body), len(x.body))
                n["identical"] += 1
                return x

            for i in ids:
                r = both("GET", f"/gvfs/objects/{i}")
                assert r.status == 200 and r.headers.get("content-type") == LOOSE, (i, r.status)
                assert zlib.decompress(r.body) == f"blob {len(blobs[i])}\0".encode() + blobs[i], i
                n["judged"] += 1
            for i in large:
                for accept in (PACK, LOOSE, f"{PACK}, {LOOSE}"):
                    both("POST", "/gvfs/objects", body={"objectIds": [i], "commitDepth": 1},
                         headers={"Accept": accept})
            for k in (len(ids), 1, 2, 50, 200, 200):
                batch = ids if k == len(ids) else rnd.sample(ids, k)
                r = both("POST", "/gvfs/objects", body={"objectIds": batch, "commitDepth": 1},
                         headers={"Accept": PACK})
                assert r.status == 200
            r = both("POST", "/gvfs/sizes", body=ids)
            assert {e["Id"]: e["Size"] for e in json.loads(r.body)} == {i: len(blobs[i]) for i in ids}
            keys = ("gvfs_blob_cache_fetches_total", "gvfs_blob_cache_bypass_total",
                    "gvfs_blob_cache_hits_total", "gvfs_blob_cache_misses_total",
                    "gvfs_alloc_hot_path_total", "gvfs_errors_total")
            stats[name] = {}
            for t, port in ports.items():
                m = metrics(port)
                stats[name][t] = {k.replace("gvfs_blob_cache_", "").replace("gvfs_", ""): int(m.get(k, 0))
                                  for k in keys}
                assert stats[name][t]["alloc_hot_path_total"] == 0, (name, t, stats[name][t])
                assert stats[name][t]["errors_total"] == 0, (name, t, stats[name][t])
            print(f"{name}: {n} {json.dumps(stats[name])}", flush=True)
            for _, p, _ in servers:
                p.terminate()
                p.wait()
            servers = []
        print("OK", json.dumps(stats))
    finally:
        for _, p, _ in servers:
            p.terminate()
            p.wait()
        az.terminate()
        az.wait()


if __name__ == "__main__":
    main()
