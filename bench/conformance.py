"""Implementation-neutral lifecycle conformance suite (black box: CLI + HTTP, judged by stock git).

Union of the scenarios in the five tests_plus_benchmarks_*.md docs:
  genesis, cold/warm/up-to-date prefetch, live (L0) tip, sealing hand-off, every rollup tier
  (1h 6h 12h 24h 168h, plus a Monday weekly for Monday-based weeks), a merge stamped 2001
  (older than its parents), a revert (re-introduced objects), a force-push rewrite, a server
  restart with unsealed live data, a branch the pipeline never saw, loose objects, object batches,
  commit closures, sizes, malformed input, concurrency, idle/slow connections, metrics.

Every prefetch response must be strictly valid GPRE with at most ONE pack; packs are installed
with `git index-pack --strict --stdin` into the client's own repository; a sent .idx must be
byte-identical to git's; after draining, the client must hold every object reachable from the
tip the server has sealed (`rev-list --objects --missing=print`).
"""
import concurrent.futures as cf
import datetime as dt
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import traceback

import corpus
import impls
from gvfslib import (parse_gpre, index_pack, missing_objects, init_bare, request, check_loose,
                     cat_batch, git, git_out, rev_list_objects, GpreError, clean_env)

BASE = os.environ.get("BENCH_SCRATCH", "/home/muks/gvfs-bench/conf")
PREFIX = "/org/repo"


def utc(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc)


def cron_tier(ts):
    d = utc(ts)
    if d.minute != 0:
        return "15m"
    if d.hour == 0 and d.weekday() == 6:
        return "168h"
    if d.hour == 0:
        return "24h"
    if d.hour % 12 == 0:
        return "12h"
    if d.hour % 6 == 0:
        return "6h"
    return "1h"


class Client:
    def __init__(self, name, repo):
        self.name, self.repo = name, repo
        self.cursor = 0
        self.bytes = 0
        self.packs = 0
        self.responses = 0
        self.nonadvancing = 0
        self.tip = None
        self.ideal = 0


VARIANTS = {  # name -> (adapter, binary)
    "opus-orig": ("opus", "/home/muks/gvfs-bench/frozen/opus-orig-gvfs-daemon"),
}


class Suite:
    def __init__(self, name, src, oids, base=BASE):
        self.name = name
        impl, binary = VARIANTS.get(name, (name, None))
        self.work = os.path.join(base, name)
        shutil.rmtree(self.work, ignore_errors=True)
        os.makedirs(self.work)
        # private copy: every suite mints its own extra commits
        self.src = os.path.join(self.work, "src.git")
        shutil.copytree(src, self.src, symlinks=True)
        self.oids = oids
        self.checks = []
        self.info = {}
        self.clock = impls.Clock(os.path.join(self.work, "faketime"), corpus.T0)
        self.im = impls.ALL[impl](self.work, clock=self.clock, binary=binary)
        self.up = None
        self.srv = None
        self.clients = {}
        self.sealed_tip = None
        self.live_lat = []
        self.extra_k = 0
        self.lock = threading.Lock()

    # ------------------------------------------------------------------ bookkeeping
    def check(self, cid, ok, detail=""):
        with self.lock:
            self.checks.append((cid, bool(ok), "" if ok else str(detail)[:400]))
        if not ok:
            print(f"[{self.name}] FAIL {cid}: {str(detail)[:300]}", flush=True)
        return ok

    def note(self, k, v):
        self.info.setdefault(k, []).append(v)

    # ------------------------------------------------------------------ helpers
    def newer_commit(self, when):
        self.extra_k += 1
        parent = git_out(self.up, "rev-parse", "main")
        return corpus.extra_commit(self.src, parent, when, self.extra_k)

    def latest_le(self, t):
        best = None
        for k in range(1, 41):
            if corpus.T0 + 600 * k <= t:
                best = self.oids[f"c{k}"]
        return best

    def push(self, oid):
        corpus.move_main(self.up, oid)

    def prefetch(self, cursor, prefix=PREFIX, cid="prefetch"):
        r = request(self.srv.port, "GET", f"{prefix}/gvfs/prefetch?lastPackTimestamp={cursor}")
        if not self.check(f"{cid}:status200", r.status == 200, f"status {r.status} {r.body[:200]!r}"):
            return r, None
        try:
            packs = parse_gpre(r.body)
        except GpreError as e:
            self.check(f"{cid}:gpre_valid", False, e)
            return r, None
        self.check(f"{cid}:at_most_one_pack", len(packs) <= 1, f"{len(packs)} packs")
        return r, packs

    def ideal_bytes(self, have, want):
        inp = f"{want}\n" + (f"^{have}\n" if have else "")
        p = subprocess.run(["git", f"--git-dir={self.up}", "pack-objects", "--revs", "--stdout", "-q"],
                           input=inp.encode(), capture_output=True, env=clean_env())
        return len(p.stdout)

    def drain(self, c, tip, cid, max_iter=4, required=True):
        """Prefetch from the client's cursor until 0 packs (or complete); validate everything."""
        got_any = False
        for i in range(max_iter):
            r, packs = self.prefetch(c.cursor, cid=f"{cid}")
            if packs is None:
                return False
            c.responses += 1
            if not packs:
                break
            ts, pack, idx = packs[0]
            got_any = True
            if ts <= c.cursor:
                c.nonadvancing += 1
            if idx is not None:
                try:
                    index_pack(pack, os.path.join(self.work, "ipx"), verify_idx=idx)
                    self.check(f"{cid}:idx_identical", True)
                except Exception as e:
                    self.check(f"{cid}:idx_identical", False, e)
            p = subprocess.run(["git", f"--git-dir={c.repo}", "index-pack", "--strict", "--stdin"],
                               input=pack, capture_output=True, env=clean_env())
            if not self.check(f"{cid}:index_pack_strict", p.returncode == 0,
                              p.stderr.decode(errors="replace")[-300:]):
                return False
            c.cursor = max(c.cursor, ts)
            c.bytes += len(pack) + (len(idx) if idx else 0)
            c.packs += 1
            if not missing_objects(c.repo, tip) and ts > 0:
                # complete; one more round must say "nothing new" (unless the server keeps
                # re-sending a live pack whose timestamp does not advance)
                r2, p2 = self.prefetch(c.cursor, cid=f"{cid}:settle")
                if p2:
                    c.nonadvancing += 1 if p2[0][0] <= c.cursor else 0
                    self.note("resend_after_complete", f"{cid} {len(p2[0][1])}B")
                break
        miss = missing_objects(c.repo, tip)
        if required:
            self.check(f"{cid}:complete", not miss, f"{len(miss)} missing of tip {tip[:10]}: {miss[:3]}")
        if got_any or not miss:
            if c.tip != tip and not miss:
                c.ideal += self.ideal_bytes(c.tip, tip)
                c.tip = tip
        return not miss

    def cold_clone(self, tip, cid):
        c = Client("cold", init_bare(os.path.join(self.work, "clients", f"cold-{time.time_ns()}")))
        r, packs = self.prefetch(0, cid=cid)
        if packs is None:
            return None
        self.check(f"{cid}:exactly_one_pack", len(packs) == 1, f"{len(packs)} packs")
        if not packs:
            return None
        ts, pack, idx = packs[0]
        if idx is not None:
            try:
                index_pack(pack, os.path.join(self.work, "ipx"), verify_idx=idx)
                self.check(f"{cid}:idx_identical", True)
            except Exception as e:
                self.check(f"{cid}:idx_identical", False, e)
        p = subprocess.run(["git", f"--git-dir={c.repo}", "index-pack", "--strict", "--stdin"],
                           input=pack, capture_output=True, env=clean_env())
        if not self.check(f"{cid}:index_pack_strict", p.returncode == 0, p.stderr.decode()[-300:]):
            return None
        miss = missing_objects(c.repo, tip)
        self.check(f"{cid}:complete", not miss, f"{len(miss)} missing, e.g. {miss[:3]}")
        c.cursor, c.bytes, c.packs, c.tip = ts, len(pack) + (len(idx) if idx else 0), 1, tip
        self.note("cold_bytes", len(pack))
        shutil.rmtree(c.repo, ignore_errors=True)
        return ts

    def new_client(self, name, tip):
        c = Client(name, init_bare(os.path.join(self.work, "clients", name)))
        self.clients[name] = c
        self.drain(c, tip, f"client-{name}-init")
        return c

    def wait_live(self, tip, timeout=30):
        t0 = time.time()
        while time.time() - t0 < timeout:
            r = request(self.srv.port, "GET", f"{PREFIX}/gvfs/objects/{tip}")
            if r.status == 200:
                lat = time.time() - t0
                self.live_lat.append(round(lat, 2))
                return True
            time.sleep(0.1)
        return False

    def settle(self):
        time.sleep(2.6)  # manifest poll is 1 s for every implementation

    def server_alive(self, cid):
        return self.check(f"{cid}:server_alive", self.srv.alive(), self.srv.log_tail(8))

    # ------------------------------------------------------------------ phases
    def basic(self, tip):
        port = self.srv.port
        r = request(port, "GET", f"{PREFIX}/gvfs/config")
        ok = r.status == 200
        try:
            json.loads(r.body)
        except Exception:
            ok = False
        self.check("config:json200", ok, r.status)
        ts = self.cold_clone(tip, "genesis:cold")
        if ts is not None:
            r, packs = self.prefetch(ts, cid="genesis:uptodate")
            self.check("genesis:uptodate_zero", packs == [], f"{packs and len(packs)}")
            self.check("genesis:uptodate_8_bytes", len(r.body) == 8, len(r.body))
            r1 = request(port, "GET", "/gvfs/prefetch?lastPackTimestamp=0")
            r2 = request(port, "GET", "/a/b/c/gvfs/prefetch?lastPackTimestamp=0")
            self.check("prefix:same_cold_pack", r1.body == r2.body, f"{len(r1.body)} vs {len(r2.body)}")
            r3 = request(port, "GET", "/gvfs/prefetch")
            self.check("prefetch:no_param_ok", r3.status == 200, r3.status)
            r4 = request(port, "GET", f"/gvfs/prefetch?lastPackTimestamp={4102444800}")
            try:
                self.check("prefetch:future_zero", parse_gpre(r4.body) == [], r4.status)
            except GpreError as e:
                self.check("prefetch:future_zero", False, e)

    def step(self, F, tier=None, label=""):
        self.clock.set(F)
        tier = tier or cron_tier(F)
        t = self.im.step(self.up, tier)
        self.note("step_s", (label or tier, round(t, 3)))
        self.settle()
        self.sealed_tip = git_out(self.up, "rev-parse", "main")
        return tier

    def check_clients(self, tag, which=None):
        tip = self.sealed_tip
        self.cold_clone(tip, f"{tag}:cold")
        for name, c in self.clients.items():
            if which is None or name in which:
                self.drain(c, tip, f"{tag}:{name}")
        self.server_alive(tag)

    def live_probe(self, F, tag, allow_extra=False):
        """Push commits due in the next half step; they must become GET-able via the live tip."""
        self.clock.set(F)
        tip = git_out(self.up, "rev-parse", "main")
        newtip = self.latest_le(F)
        if newtip is None or newtip == tip or git(self.up, "merge-base", "--is-ancestor", newtip, tip,
                                                  check=False).returncode == 0:
            if not allow_extra:
                return None
            newtip = self.newer_commit(F - 30)
        self.push(newtip)
        ok = self.wait_live(newtip)
        self.check(f"{tag}:live_get_tip", ok, "new tip not GET-able within 30 s")
        if ok and "poller" in self.clients:
            c = self.clients["poller"]
            got = self.drain(c, newtip, f"{tag}:poller-live", required=False)
            self.note("live_in_prefetch", bool(got))
            # whatever it got, it must still hold the sealed tip
            self.check(f"{tag}:poller-live:has_sealed", not missing_objects(c.repo, self.sealed_tip))
        return newtip

    def run(self):
        oids = self.oids
        F = ((corpus.T0 + 1200) // 900 + 1) * 900 + 5   # first 15-minute boundary after c2
        self.up = corpus.make_upstream(self.src, os.path.join(self.work, "up.git"), self.latest_le(F))
        self.clock.set(F)
        t = self.im.step(self.up, "15m")
        self.note("genesis_s", round(t, 3))
        self.sealed_tip = git_out(self.up, "rev-parse", "main")
        self.genesis_tip = self.sealed_tip
        self.srv = self.im.serve(self.up, poll_s=1)
        self.info["ready_s"] = round(self.srv.ready_secs, 3)
        self.basic(self.sealed_tip)
        for n in ("poller", "hourly", "dev", "late"):
            self.new_client(n, self.sealed_tip)
        # ---------------- regular 15-minute cadence over the corpus history
        j = 0
        last_commit_t = corpus.T0 + 600 * 40
        while F < last_commit_t + 900:
            j += 1
            self.live_probe(F + 450, f"s{j}")
            F += 900
            self.clock.set(F)
            tip = self.latest_le(F)
            cur = git_out(self.up, "rev-parse", "main")
            if git(self.up, "merge-base", "--is-ancestor", tip, cur, check=False).returncode != 0:
                self.push(tip)
            tier = self.step(F, label=f"s{j}")
            which = {"poller"}
            if utc(F).minute == 0:
                which.add("hourly")
            if j % 8 == 0:
                which.add("dev")
            self.check_clients(f"s{j}-{tier}", which)
        # ---------------- merge with a 2001 clock, then a revert (re-introduced objects)
        F += 900
        self.clock.set(F)
        self.push(oids["merge"])
        self.check("merge:live_get", self.wait_live(oids["merge"]))
        tier = self.step(F, label="merge")
        self.check_clients(f"merge-{tier}", {"poller", "dev"})
        F += 900
        self.clock.set(F)
        self.push(oids["revert"])
        tier = self.step(F, label="revert")
        self.check_clients(f"revert-{tier}", {"poller"})
        # ---------------- branch the pipeline never saw (informational)
        r = request(self.srv.port, "GET", f"{PREFIX}/gvfs/objects/{oids['feature']}")
        self.info["unseen_branch_get"] = r.status
        if r.status == 200:
            try:
                check_loose(oids["feature"], r.body)
                self.check("unseen_branch:valid", True)
            except Exception as e:
                self.check("unseen_branch:valid", False, e)
        # ---------------- restart with unsealed live data
        F += 450
        self.clock.set(F)
        x = self.newer_commit(F - 10)
        self.push(x)
        self.check("restart:live_before", self.wait_live(x))
        self.srv.stop()
        self.srv = self.im.serve(self.up, poll_s=1)
        self.check("restart:live_recovered", self.wait_live(x), "unsealed tip not served after restart")
        F += 450
        self.clock.set(F)
        tier = self.step(F, label="after-restart")
        self.check_clients(f"restart-{tier}", {"poller", "hourly"})
        # ---------------- jump to 12:00 (12h), force-push rewrite, Sunday weekly, Monday weekly
        day0 = (F // 86400) * 86400
        for label, when, action in (
                ("readd", day0 + 11 * 3600 + 45 * 60 + 5, "readd"),
                ("12h", day0 + 12 * 3600 + 5, "commit"),
                ("rewrite", day0 + 12 * 3600 + 900 + 5, "rewrite"),
                ("sun-weekly", day0 + 86400 + 5, "commit"),
                ("sun-15m", day0 + 86400 + 900 + 5, "commit"),
                ("mon-weekly", day0 + 2 * 86400 + 5, "commit"),
                ("mon-15m", day0 + 2 * 86400 + 900 + 5, "commit"),
                ("mon-1h", day0 + 2 * 86400 + 3600 + 5, "none")):
            F = when
            self.clock.set(F)
            if action == "commit":
                self.push(self.newer_commit(F - 60))
            elif action == "readd":
                # restore c1's tree: re-adds objects the baseline already holds that the
                # upstream re-sends (c1 is history, not a tip negotiation can name)
                x = corpus.restore_commit(self.src, git_out(self.up, "rev-parse", "main"),
                                          self.oids["c1"], F - 60)
                self.push(x)
            elif action == "rewrite":
                base = oids["c30"]
                self.extra_k += 1
                x = corpus.extra_commit(self.src, base, F - 60, self.extra_k)
                self.push(x)
            tier = "168h" if "weekly" in label else cron_tier(F)
            tier = self.step(F, tier=tier, label=label)
            self.check_clients(f"{label}-{tier}", None)
        # stale client: older than every retained week
        c = Client("stale", init_bare(os.path.join(self.work, "clients", "stale")))
        c.cursor = 1
        self.clients["stale"] = c
        self.drain(c, self.sealed_tip, "stale")
        self.final_sweep()
        self.robustness()
        m = self.srv.metrics()
        hot = [v for k, v in m.items() if "alloc" in k and "hot" in k and "byte" not in k]
        self.info["hot_allocs"] = hot
        self.check("metrics:hot_path_zero", all(v == 0 for v in hot), m)
        errs = {k: v for k, v in m.items() if "error" in k and v}
        self.info["metric_errors"] = errs
        self.server_alive("final")
        self.info["clients"] = {n: dict(bytes=c.bytes, ideal=c.ideal, packs=c.packs,
                                        responses=c.responses, nonadvancing=c.nonadvancing)
                                for n, c in self.clients.items()}
        self.info["live_latency_s"] = self.live_lat

    # ------------------------------------------------------------------ final sweep
    def final_sweep(self):
        port = self.srv.port
        tip = self.sealed_tip
        allo = sorted(rev_list_objects(self.up, tip))
        ref = cat_batch(self.up, allo)
        bad = []
        for o in allo:
            r = request(port, "GET", f"{PREFIX}/gvfs/objects/{o}")
            if r.status != 200:
                bad.append((o, ref[o][0], r.status))
                continue
            try:
                if check_loose(o, r.body) != ref[o]:
                    bad.append((o, ref[o][0], "content"))
            except Exception as e:
                bad.append((o, ref[o][0], str(e)[:60]))
            ct = r.headers.get("content-type", "")
            if "loose" not in ct:
                bad.append((o, "content-type", ct))
        self.check("sweep:get_every_object", not bad, f"{len(bad)}/{len(allo)} bad: {bad[:4]}")
        # sizes, in request order, exact format
        sizes_bad = []
        for i in range(0, len(allo), 500):
            chunk = allo[i:i + 500]
            r = request(port, "POST", f"{PREFIX}/gvfs/sizes", chunk)
            try:
                js = json.loads(r.body)
                exp = [{"Id": o, "Size": len(ref[o][1])} for o in chunk]
                if js != exp:
                    sizes_bad.append(next(((a, b) for a, b in zip(js, exp) if a != b), "len"))
            except Exception as e:
                sizes_bad.append((r.status, str(e)[:80]))
        self.check("sweep:sizes_exact", not sizes_bad, sizes_bad[:3])
        # object batches: every non-commit, exact set (superset recorded)
        nonc = [o for o in allo if ref[o][0] != "commit"]
        extra = 0
        miss_tot = 0
        for i in range(0, len(nonc), 300):
            chunk = nonc[i:i + 300]
            r = request(port, "POST", f"{PREFIX}/gvfs/objects", {"objectIds": chunk, "commitDepth": 1},
                        headers={"Accept": "application/x-git-packfile"})
            if r.status != 200:
                miss_tot += len(chunk)
                continue
            try:
                _, _, objs = index_pack(r.body, os.path.join(self.work, "ipx"))
                got = {x[0] for x in objs}
                miss_tot += len(set(chunk) - got)
                extra += len(got - set(chunk))
            except Exception as e:
                self.check("sweep:post_batch_valid", False, e)
                miss_tot += len(chunk)
        self.check("sweep:post_batches_complete", miss_tot == 0, f"{miss_tot} missing")
        self.info["post_extra_objects"] = extra
        # commit closures (protocol: commit + all trees; parents up to depth; no blobs)
        for depth in (1, 2, 3):
            r = request(port, "POST", f"{PREFIX}/gvfs/objects",
                        {"objectIds": [tip], "commitDepth": depth})
            if not self.check(f"closure:d{depth}:200", r.status == 200, r.status):
                continue
            try:
                _, _, objs = index_pack(r.body, os.path.join(self.work, "ipx"))
            except Exception as e:
                self.check(f"closure:d{depth}:valid", False, e)
                continue
            got = {x[0] for x in objs}
            commits = git_out(self.up, "rev-list", f"--max-count={depth}", "--first-parent", tip).split()
            need = set()
            for cmt in commits:
                need |= {l.split()[0] for l in git_out(self.up, "rev-list", "--objects", "--no-walk",
                                                       "--filter=blob:none", cmt).splitlines()}
            types = {x[1] for x in objs}
            self.check(f"closure:d{depth}:has_commits_and_trees", need <= got,
                       f"missing {len(need - got)}")
            self.info[f"closure_d{depth}"] = dict(objects=len(got), required=len(need),
                                                  blobs=sum(1 for x in objs if x[1] == "blob"))
        # unknown ids
        r = request(port, "GET", f"{PREFIX}/gvfs/objects/{'e' * 40}")
        self.check("unknown:get_404", r.status == 404, r.status)
        r = request(port, "POST", f"{PREFIX}/gvfs/objects", {"objectIds": [nonc[0], "e" * 40]})
        self.info["unknown_in_post"] = r.status
        r = request(port, "POST", f"{PREFIX}/gvfs/sizes", ["e" * 40])
        self.info["unknown_in_sizes"] = (r.status, r.body[:80].decode(errors="replace"))
        # loose-object accept on POST (opus T-07) - informational
        r = request(port, "POST", f"{PREFIX}/gvfs/objects", {"objectIds": [nonc[0]]},
                    headers={"Accept": "application/x-gvfs-loose-objects"})
        self.info["post_loose_accept"] = (r.status, r.headers.get("content-type"))

    # ------------------------------------------------------------------ robustness
    def robustness(self):
        port = self.srv.port
        cases = {
            "get_short_oid": ("GET", "/gvfs/objects/abc", None),
            "get_nonhex_oid": ("GET", "/gvfs/objects/" + "z" * 40, None),
            "get_uppercase_oid": ("GET", "/gvfs/objects/" + self.sealed_tip.upper(), None),
            "prefetch_bad_ts": ("GET", "/gvfs/prefetch?lastPackTimestamp=abc", None),
            "prefetch_negative_ts": ("GET", "/gvfs/prefetch?lastPackTimestamp=-5", None),
            "post_bad_json": ("POST", "/gvfs/objects", b"{not json"),
            "post_empty_ids": ("POST", "/gvfs/objects", b'{"objectIds":[]}'),
            "post_bad_oid": ("POST", "/gvfs/objects", b'{"objectIds":["xyz"]}'),
            "post_neg_depth": ("POST", "/gvfs/objects",
                               ('{"objectIds":["%s"],"commitDepth":-1}' % self.sealed_tip).encode()),
            "sizes_not_array": ("POST", "/gvfs/sizes", b'{"a":1}'),
            "sizes_empty": ("POST", "/gvfs/sizes", b"[]"),
            "unknown_path": ("GET", "/gvfs/nope", None),
            "big_body": ("POST", "/gvfs/sizes", b"[" + b",".join([b'"' + b"a" * 40 + b'"'] * 60000) + b"]"),
        }
        res = {}
        for k, (m, path, body) in cases.items():
            try:
                r = request(port, m, path, body=body, headers={"Content-Type": "application/json"},
                            timeout=60)
                res[k] = r.status
            except Exception as e:
                res[k] = f"EXC {type(e).__name__}"
        self.info["robustness_status"] = res
        five = {k: v for k, v in res.items()
                if k != "big_body" and (not isinstance(v, int) or v >= 500)}
        self.check("robust:no_5xx_or_drop", not five, five)
        self.server_alive("robust")
        # idle keep-alive connections (a fleet's connection pools) must not starve real requests
        for n_idle in (100, 300, 1000):
            socks = []
            opened = 0
            try:
                for _ in range(n_idle):
                    try:
                        socks.append(socket.create_connection(("127.0.0.1", port), timeout=2))
                        opened += 1
                    except OSError:
                        break
                t0 = time.time()
                try:
                    r = request(port, "GET", "/gvfs/config", timeout=10)
                    lat = time.time() - t0
                    ok = r.status == 200 and lat < 2
                except Exception as e:
                    lat, ok = None, False
                self.info[f"idle{n_idle}"] = dict(opened=opened, config_latency_s=lat and round(lat, 3))
                self.check(f"robust:idle{n_idle}_dont_starve", ok and opened == n_idle,
                           f"opened {opened}/{n_idle}, latency {lat}")
            finally:
                for s_ in socks:
                    s_.close()
            time.sleep(0.5)
        # largest /gvfs/sizes and POST /gvfs/objects batch accepted
        allo = sorted(rev_list_objects(self.up, self.sealed_tip))
        limits = {}
        for kind in ("sizes", "objects"):
            best = 0
            for n in (1000, 4000, 16000, 64000):
                ids = [allo[i % len(allo)] for i in range(n)]
                body = ids if kind == "sizes" else {"objectIds": ids}
                try:
                    r = request(port, "POST", f"/gvfs/{kind}", body, timeout=60)
                    if r.status != 200:
                        break
                    best = n
                except Exception:
                    break
            limits[kind] = best
        self.info["max_batch_ok"] = limits
        # a stalled reader must not block others
        s = socket.create_connection(("127.0.0.1", port))
        s.sendall(b"GET /gvfs/prefetch?lastPackTimestamp=0 HTTP/1.1\r\nHost: x\r\n\r\n")
        s.recv(100)
        t0 = time.time()
        try:
            r = request(port, "GET", f"/gvfs/objects/{self.sealed_tip}", timeout=20)
            self.check("robust:stalled_reader_no_block", r.status == 200 and time.time() - t0 < 2,
                       f"{time.time() - t0:.2f}s")
        except Exception as e:
            self.check("robust:stalled_reader_no_block", False, e)
        s.close()
        # concurrency: cold clones + GETs at once
        allo = sorted(rev_list_objects(self.up, self.sealed_tip))[:200]

        def worker(i):
            errs = 0
            r = request(port, "GET", "/gvfs/prefetch?lastPackTimestamp=0", timeout=120)
            try:
                if r.status != 200 or len(parse_gpre(r.body)) != 1:
                    errs += 1
            except GpreError:
                errs += 1
            for o in allo[i::8]:
                if request(port, "GET", f"/gvfs/objects/{o}", timeout=60).status != 200:
                    errs += 1
            return errs
        with cf.ThreadPoolExecutor(32) as ex:
            errs = sum(ex.map(worker, range(32)))
        self.check("robust:concurrent_mix", errs == 0, f"{errs} errors")
        self.server_alive("concurrency")


def run_one(name, src, oids):
    s = Suite(name, src, oids)
    try:
        s.run()
    except Exception as e:
        s.check("suite:no_exception", False, f"{type(e).__name__}: {e}")
        s.info["trace"] = traceback.format_exc()[-3000:]
    finally:
        if s.srv:
            s.srv.stop()
    passed = sum(1 for c in s.checks if c[1])
    failed = [c for c in s.checks if not c[1]]
    out = dict(impl=name, passed=passed, failed=len(failed), failures=failed[:60], info=s.info)
    with open(os.path.join(s.work, "result.json"), "w") as f:
        json.dump(out, f, indent=1, default=str)
    return out


def main():
    names = sys.argv[1:] or list(impls.ALL)
    names = [n for n in names if n in impls.ALL or n in VARIANTS]
    os.makedirs(BASE, exist_ok=True)
    src = os.path.join(BASE, "src.git")
    shutil.rmtree(src, ignore_errors=True)
    oids = corpus.build_source(src, n=40)
    with cf.ThreadPoolExecutor(len(names)) as ex:
        res = list(ex.map(lambda n: run_one(n, src, oids), names))
    for r in res:
        print(f"== {r['impl']}: {r['passed']} passed, {r['failed']} failed")
    with open(os.path.join(BASE, "summary.json"), "w") as f:
        json.dump(res, f, indent=1, default=str)


if __name__ == "__main__":
    main()
