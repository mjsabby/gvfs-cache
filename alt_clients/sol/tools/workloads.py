#!/usr/bin/env python3
"""Accelerated real-history GVFS/client-maintenance replay; no scheduler registration."""

import argparse
import collections
import csv
import datetime
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import tempfile
import time
import urllib.parse


class CountingSocket:
    def __init__(self, sock):
        self.sock = sock
        self.received = 0
        self.sent = 0

    def sendall(self, data):
        self.sent += len(data)
        self.sock.sendall(data)

    def makefile(self, mode):
        parent = self

        class Reader(io.RawIOBase):
            def readable(self):
                return True

            def readinto(self, target):
                count = parent.sock.recv_into(target)
                parent.received += count
                return count

        return io.BufferedReader(Reader())

    def close(self):
        self.sock.close()


class Connection(http.client.HTTPConnection):
    def connect(self):
        super().connect()
        self.sock = CountingSocket(self.sock)
        self.counter = self.sock


def command(args, env, data=None, capture=True):
    result = subprocess.run(args, input=data, env=env, stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
                            stderr=subprocess.PIPE)
    if result.returncode:
        raise RuntimeError(f"{args}: {result.returncode}\n{result.stderr.decode(errors='replace')}")
    return result.stdout


def git(repo, env, *args, data=None):
    return command(["git", "-C", str(repo), *args], env, data)


def isolated_environment(root):
    overrides = {"GIT_CONFIG_COUNT", "GIT_CONFIG_PARAMETERS", "GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR",
                 "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_TEMPLATE_DIR"}
    env = {key: value for key, value in os.environ.items()
           if key not in overrides and not key.startswith(("GIT_CONFIG_KEY_", "GIT_CONFIG_VALUE_"))}
    home = root / "environment"
    home.mkdir(exist_ok=True)
    # No Scalar register/config/start/reconfigure commands are used. Also isolate
    # config paths and the session bus so even an unexpected scheduler call cannot
    # activate the user's systemd manager.
    env.update(HOME=str(home), XDG_CONFIG_HOME=str(home / "xdg"), XDG_RUNTIME_DIR=str(home / "runtime"),
               GIT_CONFIG_GLOBAL=str(home / "gitconfig"), GIT_CONFIG_NOSYSTEM="1",
               DBUS_SESSION_BUS_ADDRESS="unix:path=" + str(home / "no-session-bus"))
    return env


def prefetch(url, cursor, destination=None):
    parsed = urllib.parse.urlsplit(url)
    connection = Connection(parsed.hostname, parsed.port, timeout=600)
    started = time.monotonic()
    connection.request("GET", f"/gvfs/prefetch?lastPackTimestamp={cursor}")
    response = connection.getresponse()
    if response.status != 200:
        raise RuntimeError((response.status, response.read()[:1000]))
    sock = connection.counter

    def exact(size):
        data = response.read(size)
        if len(data) != size:
            raise RuntimeError(f"truncated response: wanted {size}, got {len(data)}")
        return data

    file_header = exact(8)
    if file_header[:6] != b"GPRE \x01":
        raise RuntimeError("not GPRE")
    packs = struct.unpack("<H", file_header[6:])[0]
    if packs not in (0, 1):
        raise RuntimeError(f"more than one pack: {packs}")
    result = {"packs": packs, "body_bytes": 8, "pack_bytes": 0, "index_bytes": 0,
              "objects": 0, "timestamp": cursor, "pack_hash": ""}
    if packs:
        timestamp, pack_size, index_size = struct.unpack("<qqq", exact(24))
        if pack_size < 32 or (index_size != -1 and index_size < 1072):
            raise RuntimeError("invalid GPRE dimensions")
        result.update(timestamp=timestamp, pack_bytes=pack_size, index_bytes=max(index_size, 0),
                      body_bytes=32 + pack_size + max(index_size, 0))
        if destination is not None:
            destination.mkdir(parents=True, exist_ok=True)

        def stream(size, extension, pack=False):
            digest = hashlib.sha1()
            output = (destination / ("received." + extension)).open("wb") if destination is not None else None
            try:
                remaining = size - 20
                first = True
                footer = b""
                while remaining:
                    chunk = exact(min(4 << 20, remaining))
                    if pack and first:
                        if chunk[:8] != b"PACK\0\0\0\x02":
                            raise RuntimeError("invalid pack header")
                        result["objects"] = struct.unpack(">I", chunk[8:12])[0]
                    if not pack:
                        if first and (chunk[:8] != b"\xfftOc\0\0\0\x02" or
                                      struct.unpack(">I", chunk[1028:1032])[0] != result["objects"]):
                            raise RuntimeError("invalid index header/count")
                        footer = (footer + chunk[-20:])[-20:]
                    first = False
                    digest.update(chunk)
                    if output:
                        output.write(chunk)
                    remaining -= len(chunk)
                trailer = exact(20)
                if digest.digest() != trailer:
                    raise RuntimeError(extension + " checksum mismatch")
                if not pack and footer.hex() != result["pack_hash"]:
                    raise RuntimeError("index references a different pack")
                if output:
                    output.write(trailer)
                return trailer.hex()
            finally:
                if output:
                    output.close()

        result["pack_hash"] = stream(pack_size, "pack", True)
        if index_size > 0:
            stream(index_size, "idx")
        elif destination is not None:
            raise RuntimeError("optimized client installation requires the supplied index")
    if response.read(1):
        raise RuntimeError("extra GPRE bytes")
    result.update(http_received=sock.received, http_sent=sock.sent, seconds=time.monotonic() - started)
    response.close()
    connection.close()
    return result


def initialize_repo(path, env):
    command(["git", "init", "--quiet", "--initial-branch=main", str(path)], env)
    for key, value in [("gc.auto", "0"), ("gc.autoPackLimit", "0"), ("maintenance.auto", "false"),
                       ("maintenance.strategy", "incremental"), ("maintenance.gc.enabled", "false"),
                       ("core.multiPackIndex", "true"), ("pack.threads", "1")]:
        git(path, env, "config", "--local", key, value)
    return path


def install(received, repo, timestamp, sequence):
    directory = repo / ".git/objects/pack"
    name = f"prefetch-{timestamp}-{sequence:032x}"
    for extension in ["pack", "idx"]:
        os.link(received / ("received." + extension), directory / (name + "." + extension))


def pack_state(repo):
    files = list((repo / ".git/objects/pack").glob("*.pack"))
    return {str(path): path.stat().st_size for path in files}


def maintain_repo(repo, env, policy):
    if policy not in ["scalar", "midx", "capped-32m"]:
        raise ValueError("unknown maintenance policy: " + policy)
    peak = len(pack_state(repo))
    if policy == "scalar":
        # NOT `scalar run config`, which registers scheduled maintenance.
        command(["scalar", "run", "pack-files", str(repo)], env)
        return max(peak, len(pack_state(repo)))
    git(repo, env, "multi-pack-index", "write", "--no-progress")
    if policy == "capped-32m":
        git(repo, env, "multi-pack-index", "expire", "--no-progress")
        peak = max(peak, len(pack_state(repo)))
        git(repo, env, "multi-pack-index", "repack", "--no-progress", "--batch-size=33554432")
        peak = max(peak, len(pack_state(repo)))
        git(repo, env, "multi-pack-index", "expire", "--no-progress")
    return max(peak, len(pack_state(repo)))


class CacheGroup:
    def __init__(self, root, name, env, seed, cursor, tip):
        self.name = name
        self.env = env
        self.cursor = cursor
        self.before_cursor = cursor
        self.sequence = 0
        self.repos = {}
        self.peak = {}
        self.received_packs = 1
        self.tip = tip
        for policy in ["none", "midx", "scalar"]:
            repo = initialize_repo(root / name / policy, env)
            install(seed, repo, cursor, 0)
            git(repo, env, "update-ref", "refs/heads/main", tip)
            self.repos[policy] = repo
            self.peak[policy] = 1

    def receive(self, data, metrics, tip):
        if metrics["packs"]:
            self.sequence += 1
            self.received_packs += 1
            for repo in self.repos.values():
                install(data, repo, metrics["timestamp"], self.sequence)
            self.cursor = metrics["timestamp"]
        self.tip = tip
        for policy, repo in self.repos.items():
            # update-ref rejects a genuinely missing tip; no remote/promisor is
            # configured, so this cannot conceal a failed fetch by lazy fetching.
            git(repo, self.env, "update-ref", "refs/heads/main", tip)
            self.peak[policy] = max(self.peak[policy], len(pack_state(repo)))

    def maintain(self, stamp, writer):
        for policy in ["midx", "scalar"]:
            repo = self.repos[policy]
            old = pack_state(repo)
            started = time.monotonic()
            observed_peak = maintain_repo(repo, self.env, policy)
            new = pack_state(repo)
            self.peak[policy] = max(self.peak[policy], observed_peak, len(new))
            writer.writerow({"time": stamp, "scenario": self.name, "policy": policy, "before": len(old),
                             "after": len(new), "rewritten_bytes": sum(size for path, size in new.items() if path not in old),
                             "seconds": time.monotonic() - started})


def clone_store(source, target):
    target.mkdir()
    (target / "slabs").mkdir()
    (target / "manifest.bin").write_bytes((source / "manifest.bin").read_bytes())
    for path in (source / "slabs").iterdir():
        if path.is_file():
            os.link(path, target / "slabs" / path.name)


def verify_clients(groups, upstream, base, env, output):
    checked_indexes = set()
    # A fully synthesized cold pack is checked earlier, including the original
    # baseline's reused idx rows. Do not decode that same baseline nine times.
    for group in groups:
        baseline = next((group.repos["none"] / ".git/objects/pack").glob("prefetch-*-00000000000000000000000000000000.idx"))
        checked_indexes.add((baseline.stat().st_dev, baseline.stat().st_ino))
    results = []
    for group in groups:
        objects = command(["git", "--git-dir", str(upstream), "rev-list", "--objects", "--no-object-names",
                           group.tip, "^" + base], env)
        expected = command(["git", "--git-dir", str(upstream), "cat-file",
                            "--batch-check=%(objectname) %(objecttype) %(objectsize)"], env, objects)
        if b" missing\n" in expected:
            raise RuntimeError("source incremental closure is incomplete")
        for policy, repo in group.repos.items():
            actual = git(repo, env, "cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)", data=objects)
            if actual != expected:
                raise RuntimeError(f"{group.name}/{policy}: incremental objects differ from original Git")
            checks = 0
            for index in (repo / ".git/objects/pack").glob("*.idx"):
                identity = (index.stat().st_dev, index.stat().st_ino)
                if identity not in checked_indexes:
                    command(["git", "verify-pack", str(index)], env)
                    checked_indexes.add(identity)
                    checks += 1
            results.append({"scenario": group.name, "policy": policy, "incremental_objects": len(objects.splitlines()),
                            "new_native_pack_checks": checks, "tip": group.tip})
    (output / "native-validation.json").write_text(json.dumps(results, indent=2) + "\n")


def read_metrics(url):
    parsed = urllib.parse.urlsplit(url)
    connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=30)
    connection.request("GET", "/metrics")
    response = connection.getresponse()
    if response.status != 200:
        raise RuntimeError("metrics unavailable")
    result = {name: int(value) for name, value in (line.split() for line in response.read().decode().splitlines())}
    connection.close()
    return result


def replay_capped_maintenance(root):
    """Replay immutable downloads with bounded small-pack batches, not the baseline."""
    root = root.resolve()
    measured = json.loads((root / "summary.json").read_text())
    if not measured["complete"]:
        raise RuntimeError("capped maintenance requires a completed workload replay")
    history = json.loads((root / "history.json").read_text())
    env = isolated_environment(root)
    requests = list(csv.DictReader((root / "requests.csv").open()))
    tips = {row["time"]: row["tip"] for row in map(json.loads, (root / "pipeline.jsonl").open())}
    results = []
    maintenance_rows = []
    for name in ["shared-agents", "polling", "developer"]:
        source = root / "clients" / name / "none"
        target = root / "clients" / name / "capped-32m"
        if target.exists():
            raise RuntimeError("refusing to reinitialize " + str(target))
        initialize_repo(target, env)
        source_packs = source / ".git/objects/pack"
        target_packs = target / ".git/objects/pack"
        seed_name = f"prefetch-{history['start']}-{'0' * 32}"
        seed_pack = source_packs / (seed_name + ".pack")
        seed_identity = (seed_pack.stat().st_ino, seed_pack.stat().st_size)
        for extension in ["pack", "idx"]:
            os.link(source_packs / (seed_name + "." + extension), target_packs / (seed_name + "." + extension))
        git(target, env, "update-ref", "refs/heads/main", history["base"])
        rows = [row for row in requests if row["scenario"] == name and row["variant"] == "after"]
        boundaries = iter(range(history["start"] + 86400, history["end"] + 1, 86400))
        boundary = next(boundaries, None)
        sequence = 0
        peak = 1

        def maintain(stamp):
            nonlocal peak
            old = pack_state(target)
            started = time.monotonic()
            observed_peak = maintain_repo(target, env, "capped-32m")
            new = pack_state(target)
            rewritten = sum(size for path, size in new.items() if path not in old)
            if rewritten >= seed_identity[1]:
                raise RuntimeError("capped maintenance unexpectedly rewrote a baseline-sized pack")
            peak = max(peak, observed_peak, len(new))
            maintenance_rows.append({"time": stamp, "scenario": name, "policy": "capped-32m",
                                     "before": len(old), "after": len(new), "peak": observed_peak, "rewritten_bytes": rewritten,
                                     "seconds": time.monotonic() - started})

        for row in rows:
            stamp = int(row["time"])
            while boundary is not None and boundary < stamp:
                maintain(boundary)
                boundary = next(boundaries, None)
            if int(row["packs"]):
                sequence += 1
                filename = f"prefetch-{row['timestamp']}-{sequence:032x}"
                for extension in ["pack", "idx"]:
                    # Fresh copies preserve logical arrival order relative to
                    # newly repacked files. MIDX tie-breaking uses pack mtimes.
                    shutil.copyfile(source_packs / (filename + "." + extension),
                                    target_packs / (filename + "." + extension))
            git(target, env, "update-ref", "refs/heads/main", tips[stamp])
            peak = max(peak, len(pack_state(target)))
        while boundary is not None:
            maintain(boundary)
            boundary = next(boundaries, None)
        baseline = target_packs / (seed_name + ".pack")
        if (baseline.stat().st_ino, baseline.stat().st_size) != seed_identity:
            raise RuntimeError("original baseline was replaced")
        git(target, env, "multi-pack-index", "verify")
        if len(list(target_packs.glob("*.idx"))) != len(pack_state(target)):
            raise RuntimeError("capped cache has unpaired pack/index files")
        for index in target_packs.glob("*.idx"):
            if index.name != seed_name + ".idx":
                command(["git", "verify-pack", str(index)], env)
        tip = tips[int(rows[-1]["time"])]
        objects = command(["git", "--git-dir", str(root / "upstream.git"), "rev-list", "--objects",
                           "--no-object-names", tip, "^" + history["base"]], env)
        check = "--batch-check=%(objectname) %(objecttype) %(objectsize)"
        if git(target, env, "cat-file", check, data=objects) != git(source, env, "cat-file", check, data=objects):
            raise RuntimeError("capped cache lost incremental objects")
        results.append({"scenario": name, "policy": "capped-32m", "packs": len(pack_state(target)),
                        "peak_packs": peak, "pack_bytes": sum(pack_state(target).values()),
                        "baseline_unchanged": True, "incremental_objects": len(objects.splitlines())})
        print(json.dumps(results[-1]), flush=True)
    with (root / "capped-maintenance.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, ["time", "scenario", "policy", "before", "after", "peak", "rewritten_bytes", "seconds"])
        writer.writeheader()
        writer.writerows(maintenance_rows)
    (root / "capped-summary.json").write_text(json.dumps(results, indent=2) + "\n")


def run(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = isolated_environment(output)
    history = json.loads(args.history.read_text())
    start, end = history["start"], history["end"]
    (output / "history.json").write_text(json.dumps(history, indent=2) + "\n")
    store = output / "store"
    clone_store(args.baseline.resolve(), store)
    # A fresh private upstream avoids changing SearchGold or another benchmark.
    upstream = output / "upstream.git"
    command(["git", "clone", "--quiet", "--shared", "--bare", str(args.upstream.resolve()), str(upstream)], env)
    command(["git", "--git-dir", str(upstream), "update-ref", "refs/heads/main", history["base"]], env)
    log = (output / "server.log").open("wb")
    driver = subprocess.Popen([str(args.driver.resolve()), str(store), str(upstream), str(output / "snapshots")],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log, env=env, text=True)

    def control(line):
        driver.stdin.write(line + "\n")
        driver.stdin.flush()
        answer = driver.stdout.readline()
        if not answer:
            raise RuntimeError("workload server stopped; see " + str(output / "server.log"))
        return json.loads(answer)

    request_fields = ["time", "scenario", "variant", "packs", "body_bytes", "pack_bytes", "index_bytes", "objects",
                      "timestamp", "pack_hash", "http_received", "http_sent", "seconds"]
    maintenance_fields = ["time", "scenario", "policy", "before", "after", "rewritten_bytes", "seconds"]
    totals = collections.defaultdict(lambda: collections.Counter())
    groups = []
    cold_count = 0
    shared_clone_count = 0
    cold_native_checks = 0
    started = time.monotonic()
    try:
        ready = json.loads(driver.stdout.readline())
        if not ready.get("ready"):
            raise RuntimeError(ready)
        before, after = ready["before"], ready["after"]
        (output / "setup.json").write_text(json.dumps(ready, indent=2) + "\n")
        with tempfile.TemporaryDirectory(prefix="seed-", dir=output) as temporary:
            seed = Path(temporary)
            seed_metrics = prefetch(after, 0, seed)
            (output / "seed.json").write_text(json.dumps(seed_metrics, indent=2) + "\n")
            for name in ["shared-agents", "polling", "developer"]:
                groups.append(CacheGroup(output / "clients", name, env, seed, start, history["base"]))
        for group in groups:
            (output / "clients" / group.name).mkdir(exist_ok=True)
        events = collections.defaultdict(set)
        for tick in range(start + 900, end + 1, 900):
            events[tick].add("seal")
        for tick in range(start + 300, end, 900):
            events[tick].add("shared-agents")
        for tick in range(start + 600, end, 900):
            events[tick].add("polling")
        for tick in range(start + 420, end, 3600):
            events[tick].add("cold-agents")
        for day in range((end - start) // 86400):
            date = datetime.datetime.fromtimestamp(start + day * 86400, datetime.timezone.utc)
            if date.weekday() < 5:
                for hour in [8, 11, 14, 17]:
                    events[start + day * 86400 + hour * 3600].add("developer")
            events[start + (day + 1) * 86400].add("maintenance")
        # Observe Monday catch-up after the final weekend, freezing the server at
        # the end of the selected real-history window (no fabricated later pushes).
        events[end + 8 * 3600].add("developer")
        by_name = {group.name: group for group in groups}
        updates = iter(history["commits"])
        upcoming = next(updates, None)
        tip = history["base"]
        with (output / "requests.csv").open("w", newline="") as requests, \
                (output / "maintenance.csv").open("w", newline="") as maintenance, \
                (output / "pipeline.jsonl").open("w") as pipeline:
            rows = csv.DictWriter(requests, request_fields)
            rows.writeheader()
            maint = csv.DictWriter(maintenance, maintenance_fields)
            maint.writeheader()
            for number, (stamp, kinds) in enumerate(sorted(events.items())):
                if args.limit_ticks and number >= args.limit_ticks:
                    break
                while upcoming and upcoming[1] <= min(stamp, end - 1):
                    tip = upcoming[0]
                    upcoming = next(updates, None)
                reply = control(f"tick {stamp} {tip} {int('seal' in kinds)}")
                pipeline.write(json.dumps(reply) + "\n")
                pipeline.flush()
                if "cold-agents" in kinds:
                    with tempfile.TemporaryDirectory(prefix="cold-", dir=output) as temporary:
                        directory = Path(temporary)
                        data = directory / "download"
                        metrics = prefetch(after, 0, data)
                        repo = initialize_repo(directory / "client", env)
                        install(data, repo, metrics["timestamp"], 0)
                        git(repo, env, "update-ref", "refs/heads/main", tip)
                        if len(pack_state(repo)) != 1:
                            raise RuntimeError("cold clone did not install exactly one pack")
                        if cold_count == 1:
                            index = next((repo / ".git/objects/pack").glob("*.idx"))
                            command(["git", "verify-pack", str(index)], env)
                            cold_native_checks += 1
                        rows.writerow(dict(time=stamp, scenario="cold-agents", variant="after", **metrics))
                        for key in ["packs", "body_bytes", "http_received", "http_sent", "objects"]:
                            totals["cold-agents/after"][key] += metrics[key]
                        totals["cold-agents/after"]["requests"] += 1
                        cold_count += 1
                for name in ["shared-agents", "polling", "developer"]:
                    if name not in kinds:
                        continue
                    group = by_name[name]
                    legacy = prefetch(before, group.before_cursor)
                    group.before_cursor = legacy["timestamp"]
                    rows.writerow(dict(time=stamp, scenario=name, variant="before", **legacy))
                    with tempfile.TemporaryDirectory(prefix="warm-", dir=output) as temporary:
                        data = Path(temporary)
                        metrics = prefetch(after, group.cursor, data)
                        rows.writerow(dict(time=stamp, scenario=name, variant="after", **metrics))
                        group.receive(data, metrics, tip)
                    if name == "shared-agents":
                        with tempfile.TemporaryDirectory(prefix="shared-agent-", dir=output) as temporary:
                            agent = Path(temporary) / "agent.git"
                            command(["git", "clone", "--quiet", "--shared", "--bare", "--single-branch",
                                     "--branch", "main", str(group.repos["scalar"]), str(agent)], env)
                            command(["git", "--git-dir", str(agent), "cat-file", "-e", tip + "^{commit}"], env)
                            if list((agent / "objects/pack").glob("*.pack")):
                                raise RuntimeError("shared agent unexpectedly copied private packs")
                            shared_clone_count += 1
                    for variant, result in [("before", legacy), ("after", metrics)]:
                        for key in ["packs", "body_bytes", "http_received", "http_sent", "objects"]:
                            totals[f"{name}/{variant}"][key] += result[key]
                        totals[f"{name}/{variant}"]["requests"] += 1
                if "maintenance" in kinds:
                    for group in groups:
                        group.maintain(stamp, maint)
                if number % 32 == 0 or "maintenance" in kinds:
                    print(json.dumps({"event": number, "time": stamp, "cold_clones": cold_count,
                                      "elapsed": round(time.monotonic() - started, 1)}), flush=True)
                    requests.flush()
                    maintenance.flush()
        metrics = {"before": read_metrics(before), "after": read_metrics(after)}
        for values in metrics.values():
            if values["gvfs_hot_allocations_total"] or values["gvfs_errors_total"]:
                raise RuntimeError("server errors/allocations: " + str(metrics))
        (output / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
        if not args.limit_ticks:
            if cold_native_checks != 1:
                raise RuntimeError("missing synthesized-cold native verification")
            days = (end - start) // 86400
            weekdays = sum(datetime.datetime.fromtimestamp(start + day * 86400, datetime.timezone.utc).weekday() < 5
                           for day in range(days))
            expected = {"cold-agents": (end - start) // 3600, "shared-agents": (end - start) // 900,
                        "polling": (end - start) // 900, "developer": weekdays * 4 + 1}
            for name, count in expected.items():
                if totals[name + "/after"]["requests"] != count:
                    raise RuntimeError("incomplete workload: " + name)
            if shared_clone_count != expected["shared-agents"]:
                raise RuntimeError("missing shared-agent clones")
            verify_clients(groups, upstream, history["base"], env, output)
        summary = {"complete": not args.limit_ticks, "window": [start, end], "cold_clones": cold_count,
                   "shared_agent_clones": shared_clone_count,
                   "cold_native_checks": cold_native_checks,
                   "totals": {key: dict(value) for key, value in totals.items()}, "clients": [],
                   "elapsed_seconds": time.monotonic() - started}
        for group in groups:
            for policy, repo in group.repos.items():
                if policy != "none" and (repo / ".git/objects/pack/multi-pack-index").exists():
                    git(repo, env, "multi-pack-index", "verify")
                git(repo, env, "cat-file", "-e", group.tip + "^{commit}")
                indexes = len(list((repo / ".git/objects/pack").glob("*.idx")))
                if indexes != len(pack_state(repo)):
                    raise RuntimeError("unpaired pack/index files")
                summary["clients"].append({"scenario": group.name, "policy": policy, "indexes": indexes,
                                          "packs": len(pack_state(repo)), "peak_packs": group.peak[policy],
                                          "pack_bytes": sum(pack_state(repo).values()), "received_packs": group.received_packs})
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary, indent=2), flush=True)
    finally:
        if driver.poll() is None:
            driver.stdin.write("quit\n")
            driver.stdin.flush()
            try:
                driver.wait(timeout=30)
            except subprocess.TimeoutExpired:
                driver.terminate()
                driver.wait(timeout=30)
        log.close()
    if driver.returncode:
        raise RuntimeError("workload server failed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", type=Path)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--upstream", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--replay-maintenance", type=Path, help="replay a completed run's downloads using capped MIDX batches")
    parser.add_argument("--driver", type=Path, default=Path("target/release/examples/workload_server"))
    parser.add_argument("--limit-ticks", type=int, default=0, help="partial smoke replay only; summary is labeled incomplete")
    arguments = parser.parse_args()
    if arguments.replay_maintenance:
        replay_capped_maintenance(arguments.replay_maintenance)
    else:
        if any(value is None for value in [arguments.history, arguments.baseline, arguments.upstream, arguments.output]):
            parser.error("--history, --baseline, --upstream and --output are required for a server replay")
        run(arguments)
