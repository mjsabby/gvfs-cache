"""Adapters: one uniform interface over the gvfs-daemon CLIs: opus (this repository) and
the other implementations kept in `alt_clients/` for cross-testing.

Standard tiers: 15m 1h 6h 12h 24h 168h.  `step(tier)` performs one scheduled pipeline run
"ingest new commits, then roll up everything up to `tier`" in whatever way the
implementation's documentation prescribes.
"""
import os
import signal
import subprocess
import time

from gvfslib import clean_env, free_port, wait_http, metrics

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TIERS = ["15m", "1h", "6h", "12h", "24h", "168h"]


def binary_path(name):
    """The release build of implementation `name`: opus at the repository root, the others
    in `alt_clients/<name>/`."""
    base = ROOT if name == "opus" else os.path.join(ROOT, "alt_clients", name)
    return os.path.join(base, "target", "release", "gvfs-daemon")


class Server:
    def __init__(self, impl, proc, port, log_path):
        self.impl, self.proc, self.port, self.log_path = impl, proc, port, log_path

    def alive(self):
        return self.proc.poll() is None

    def metrics(self):
        return metrics(self.port)

    def stop(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(10)

    def log_tail(self, n=40):
        try:
            with open(self.log_path, "rb") as f:
                return b"\n".join(f.read().splitlines()[-n:]).decode(errors="replace")
        except OSError:
            return ""


FAKETIME_LIB = "/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1"


class Clock:
    """Harness-controlled wall clock shared by every process of one implementation (libfaketime
    offset file: time keeps running at real speed from the last set point; monotonic is real)."""

    def __init__(self, path, start):
        self.path = path
        self.set(start)

    def set(self, fake_now):
        self.now = int(fake_now)
        self.set_at = time.time()
        off = self.now - self.set_at
        with open(self.path + ".tmp", "w") as f:
            f.write(f"{int(round(off)):+d}s\n")
        os.replace(self.path + ".tmp", self.path)

    def current(self):
        return int(self.now + (time.time() - self.set_at))

    def env(self):
        return {"LD_PRELOAD": FAKETIME_LIB, "FAKETIME_TIMESTAMP_FILE": self.path,
                "FAKETIME_NO_CACHE": "1", "FAKETIME_DONT_FAKE_MONOTONIC": "1"}


class Impl:
    name = "?"
    clock = "wall"          # 'commits' (opus) | 'flag' | 'env' | 'wall'
    poll_unit = "s"

    def __init__(self, work, binary=None, extra_serve=(), extra_pipeline=(), env=None, clock=None):
        self.work = work
        os.makedirs(work, exist_ok=True)
        self.bin = binary or binary_path(self.name)
        self.store = os.path.join(work, "store")
        self.extra_serve = list(extra_serve)
        self.extra_pipeline = list(extra_pipeline)
        self.env = dict(env or {})
        self.fake = clock
        if clock is not None:
            self.env.update(clock.env())
        self.pipeline_log = os.path.join(work, "pipeline.log")
        self.runs = 0

    # -- subclass hooks
    def pipeline_cmd(self, upstream, tier, now):
        raise NotImplementedError

    def serve_cmd(self, upstream, port, poll_s, live):
        raise NotImplementedError

    def pipeline_env(self, now):
        return {}

    def steps_for(self, tier):
        """Pipeline invocations that realise one scheduled run at `tier`."""
        return [tier]

    # -- uniform API
    def run(self, cmd, env=None, timeout=3600):
        self.runs += 1
        t0 = time.perf_counter()
        with open(self.pipeline_log, "ab") as log:
            log.write(f"\n=== [{self.runs}] {' '.join(cmd)}\n".encode())
            log.flush()
            p = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT, timeout=timeout,
                               env=clean_env({**self.env, **(env or {})}), cwd=self.work)
        dt = time.perf_counter() - t0
        if p.returncode != 0:
            raise RuntimeError(f"{self.name}: `{' '.join(cmd)}` exited {p.returncode}; "
                               f"see {self.pipeline_log}:\n{self.tail()}")
        return dt

    def tail(self, n=25):
        with open(self.pipeline_log, "rb") as f:
            return b"\n".join(f.read().splitlines()[-n:]).decode(errors="replace")

    def step(self, upstream, tier, now=None):
        if now is None and self.fake is not None:
            now = self.fake.current()
        dt = 0.0
        for t in self.steps_for(tier):
            dt += self.run(self.pipeline_cmd(upstream, t, now), env=self.pipeline_env(now))
        return dt

    def serve(self, upstream, port=None, poll_s=1, live=True, ready_timeout=600, env=None, taskset=None):
        port = port or free_port()
        log_path = os.path.join(self.work, f"serve-{port}.log")
        cmd = self.serve_cmd(upstream, port, poll_s, live) + self.extra_serve
        if taskset:
            cmd = ["taskset", "-c", taskset] + cmd
        log = open(log_path, "ab")
        log.write(f"\n=== {' '.join(cmd)}\n".encode())
        log.flush()
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, cwd=self.work,
                                env=clean_env({**self.env, **(env or {})}), start_new_session=True)
        srv = Server(self, proc, port, log_path)
        try:
            srv.ready_secs = wait_http(port, timeout=ready_timeout, proc=proc)
        except Exception:
            srv.stop()
            raise RuntimeError(f"{self.name} serve failed to start:\n{srv.log_tail()}")
        return srv


class Opus(Impl):
    name = "opus"
    clock = "commits"
    poll_unit = "ms"

    def pipeline_cmd(self, upstream, tier, now):
        c = [self.bin, "pipeline", "--store", f"fs:{self.store}", "--upstream", upstream,
             "--tier", tier, "--branch", "main", "--work-dir", os.path.join(self.work, "pwork")]
        if now is not None:
            c += ["--now", str(int(now))]
        return c + self.extra_pipeline

    def serve_cmd(self, upstream, port, poll_s, live):
        c = [self.bin, "serve", "--store", f"fs:{self.store}", "--bind", f"127.0.0.1:{port}",
             "--poll-ms", str(int(poll_s * 1000)), "--cache-dir", os.path.join(self.work, "cache"),
             "--branch", "main"]
        if live and upstream:
            c += ["--upstream", upstream]
        return c


class Fable(Impl):
    name = "fable"
    clock = "env"
    TIER = {"15m": "m15", "1h": "h1", "6h": "h6", "12h": "h12", "24h": "h24", "168h": "h168"}

    def pipeline_cmd(self, upstream, tier, now):
        return [self.bin, "pipeline", "--tier", self.TIER[tier], "--store", self.store,
                "--upstream", upstream, "--branch", "main"] + self.extra_pipeline

    def pipeline_env(self, now):
        return {"GVFS_FAKE_NOW": str(int(now))} if now is not None else {}

    def serve_cmd(self, upstream, port, poll_s, live):
        c = [self.bin, "serve", "--bind", f"127.0.0.1:{port}", "--store", self.store,
             "--branch", "main", "--manifest-poll-secs", str(max(1, int(poll_s)))]
        if live and upstream:
            c += ["--upstream", upstream, "--poll-secs", str(max(1, int(poll_s)))]
        else:
            c += ["--poll-secs", "0"]
        return c


class Sol(Impl):
    name = "sol"
    clock = "flag"

    def pipeline_cmd(self, upstream, tier, now):
        c = [self.bin, "pipeline", "--storage", self.store, "--upstream", upstream,
             "--branch", "main", "--tier", tier, "--cache-dir", os.path.join(self.work, "pcache")]
        if now is not None:
            c += ["--now", str(int(now))]
        return c + self.extra_pipeline

    def steps_for(self, tier):
        # explicit 1h..168h only compact storage; ingest is a separate 15m run
        return ["15m"] if tier == "15m" else ["15m", tier]

    def serve_cmd(self, upstream, port, poll_s, live):
        c = [self.bin, "serve", "--storage", self.store, "--bind", f"127.0.0.1:{port}",
             "--branch", "main", "--cache-dir", os.path.join(self.work, "scache"),
             "--manifest-seconds", str(max(1, int(poll_s)))]
        if live and upstream:
            c += ["--upstream", upstream, "--poll-seconds", str(max(1, int(poll_s)))]
        else:
            c += ["--upstream", upstream or "/nonexistent", "--no-live"]
        return c


class Astra(Impl):
    name = "astra"
    clock = "flag"

    def pipeline_cmd(self, upstream, tier, now):
        c = [self.bin, "pipeline", "--upstream", upstream, "--branch", "main",
             "--store", self.store, "--work-dir", os.path.join(self.work, "pwork"), "--tier", tier]
        if now is not None:
            c += ["--timestamp", str(int(now))]
        return c + self.extra_pipeline

    def steps_for(self, tier):
        return ["15m"] if tier == "15m" else ["15m", tier]

    def serve_cmd(self, upstream, port, poll_s, live):
        c = [self.bin, "serve", "--store", self.store, "--bind", f"127.0.0.1:{port}",
             "--branch", "main", "--work-dir", os.path.join(self.work, "swork"),
             "--manifest-poll-seconds", str(max(1, int(poll_s)))]
        if live and upstream:
            c += ["--upstream", upstream, "--poll-seconds", str(max(1, int(poll_s)))]
        else:
            c += ["--read-only"]
        return c


ALL = {c.name: c for c in (Opus, Fable, Sol, Astra)}
