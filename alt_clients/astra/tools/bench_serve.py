#!/usr/bin/env python3
"""Verify and measure the same OIDs and one-pack prefetch across built variants."""
import argparse
import hashlib
import http.client
import json
import mmap
import os
from pathlib import Path
import selectors
import struct
import subprocess
import time
import zlib

ROW = struct.Struct("<20sIQQQII")


def artifacts(root):
    files = list(root.glob("*.mphf"))
    if len(files) != 1:
        raise RuntimeError(f"expected a fresh single-baseline benchmark in {root}")
    return files[0]


def samples(path):
    with path.open("rb") as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as data:
        if data[:8] != b"GVFSMH02":
            raise RuntimeError("unknown MPHF format")
        count = struct.unpack_from("<I", data, 8)[0]
        pointer, head, history = None, None, []
        for i in range(0, count, max(1, count // 20000)):
            oid, size_kind, _, _, family, base, _ = ROW.unpack_from(data, 64 + i * ROW.size)
            kind, size = size_kind >> 29, size_kind & 0x1FFFFFFF
            if kind == 3 and 90 <= size <= 120 and pointer is None:
                pointer = oid.hex()
            if kind == 2 and family & (1 << 63) and head is None:
                head = oid.hex()
            if kind == 2 and base != 0xFFFFFFFF and len(history) < 64:
                history.append(oid.hex())
        if not pointer or not history:
            raise RuntimeError("missing representative objects")
        return pointer, head, history


def oid_set_digest(path):
    with path.open("rb") as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as data:
        if data[:8] != b"\xfftOc\x00\x00\x00\x02":
            raise RuntimeError("unknown Git index")
        count = struct.unpack_from(">I", data, 1028)[0]
        digest = hashlib.sha256()
        start, end = 1032, 1032 + count * 20
        while start < end:
            n = min(1 << 20, end - start)
            digest.update(data[start:start + n])
            start += n
        return count, digest.hexdigest()


def exact(response, n):
    data = response.read(n)
    if len(data) != n:
        raise RuntimeError("truncated HTTP response")
    return data


def stream_hash(response, n):
    digest = hashlib.sha1()
    buffer = bytearray(1 << 20)
    view = memoryview(buffer)
    while n:
        count = response.readinto(view[:min(len(buffer), n)])
        if not count:
            raise RuntimeError("truncated pack")
        digest.update(view[:count])
        n -= count
    return digest


def object_get(connection, oid):
    start = time.perf_counter_ns()
    connection.request("GET", f"/gvfs/objects/{oid}")
    response = connection.getresponse()
    body = response.read()
    elapsed = time.perf_counter_ns() - start
    if response.status != 200:
        raise RuntimeError(f"object status {response.status}: {oid}")
    if hashlib.sha1(zlib.decompress(body)).hexdigest() != oid:
        raise RuntimeError("incorrect loose object")
    return elapsed / 1000


def cpu_seconds(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().split()
    return (int(fields[13]) + int(fields[14])) / os.sysconf("SC_CLK_TCK")


def wait_ready(proc, limit=180):
    with selectors.DefaultSelector() as selector:
        selector.register(proc.stdout, selectors.EVENT_READ)
        if not selector.select(limit):
            raise RuntimeError("server readiness timed out")
        line = proc.stdout.readline().strip()
    if not line.startswith("listening http://"):
        raise RuntimeError(f"server failed before readiness: {line}")
    return line.removeprefix("listening http://")


def benchmark(args, mode, chosen, expected):
    root = args.store / mode
    mphf = artifacts(root)
    count, digest = oid_set_digest(mphf.with_suffix(".idx"))
    if expected and expected != (count, digest):
        raise RuntimeError("representations contain different object sets")
    command = [
        str(args.binary.resolve()), "serve", "--read-only", "--strict-alloc",
        "--store", str(root), "--work-dir", str(args.work / mode),
        "--bind", "127.0.0.1:0", "--workers", "4",
    ]
    log_path = args.output / f"{mode}-serve.log"
    with log_path.open("w") as log:
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=log, text=True)
        try:
            if hasattr(os, "sched_setaffinity"):
                allowed = sorted(os.sched_getaffinity(0))
                os.sched_setaffinity(proc.pid, set(allowed[:4]))
            boot = time.perf_counter()
            address = wait_ready(proc)
            boot_seconds = time.perf_counter() - boot
            host, port = address.rsplit(":", 1)
            connection = http.client.HTTPConnection(host, int(port), timeout=300)
            results = {}
            for label, oids in (("pointer", [chosen[0]]), ("head_tree", [chosen[1]] if chosen[1] else []), ("historical_tree", chosen[2])):
                if not oids:
                    continue
                for oid in oids:
                    object_get(connection, oid)
                values = [object_get(connection, oid) for _ in range(args.rounds) for oid in oids]
                values.sort()
                results[label] = {
                    "samples": len(values),
                    "p50_us": values[len(values) // 2],
                    "p99_us": values[min(len(values) - 1, int(len(values) * .99))],
                }
            before_cpu = cpu_seconds(proc.pid)
            start = time.perf_counter()
            connection.request("GET", "/gvfs/prefetch")
            response = connection.getresponse()
            if response.status != 200:
                raise RuntimeError(f"prefetch status {response.status}")
            prefix = exact(response, 32)
            if prefix[:8] != b"GPRE \x01\x01\x00":
                raise RuntimeError("prefetch must contain exactly one pack")
            _, pack_len, idx_len = struct.unpack_from("<qqq", prefix, 8)
            pack_hash = stream_hash(response, pack_len - 20)
            trailer = exact(response, 20)
            if pack_hash.digest() != trailer:
                raise RuntimeError("pack checksum mismatch")
            if idx_len > 0:
                idx_hash = stream_hash(response, idx_len - 40)
                last = exact(response, 40)
                idx_hash.update(last[:20])
                if last[:20] != trailer or idx_hash.digest() != last[20:]:
                    raise RuntimeError("Git index checksum mismatch")
            if response.read(1):
                raise RuntimeError("extra pack or trailing response data")
            elapsed = time.perf_counter() - start
            cpu = cpu_seconds(proc.pid) - before_cpu
            connection.request("GET", "/metrics")
            response = connection.getresponse()
            metrics = response.read().decode()
            if "gvfs_alloc_hot_path_total 0\n" not in metrics:
                raise RuntimeError(f"hot path allocated: {metrics}")
            connection.close()
            result = {
                "mode": mode, "objects": count, "oid_set_sha256": digest,
                "index_bytes": mphf.stat().st_size, "boot_seconds": boot_seconds,
                "get": results, "prefetch_packs": 1,
                "prefetch_bytes": 32 + pack_len + max(0, idx_len),
                "prefetch_verified_seconds": elapsed,
                "prefetch_verified_GB_per_second": (pack_len + max(0, idx_len)) / elapsed / 1e9,
                "prefetch_server_cpu_seconds": cpu, "hot_allocations": 0,
            }
            print(json.dumps(result), flush=True)
            return result, (count, digest)
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=Path("target/release/gvfs-daemon"))
    parser.add_argument("--store", type=Path, default=Path("target/searchgold-benchmark"))
    parser.add_argument("--work", type=Path, default=Path("target/searchgold-serve"))
    parser.add_argument("--output", type=Path, default=Path("benchmarks"))
    parser.add_argument("--rounds", type=int, default=40)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    chosen = samples(artifacts(args.store / "clustered"))
    expected, results = None, []
    for mode in ("delta", "clustered", "full"):
        result, expected = benchmark(args, mode, chosen, expected)
        results.append(result)
    (args.output / "searchgold-serving.json").write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
