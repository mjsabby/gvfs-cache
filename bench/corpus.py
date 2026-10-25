"""Deterministic synthetic GitHFS-style corpus, built with git fast-import.

Union of the fixtures the five implementations' own docs used (opus G, sol, astra, fable F2):
  * big.txt: 600 lines, ~6 change per commit -> real blob deltas upstream
  * pointer files d<i%4>/e<i%3>/f<k>-<i>.dat (LF `~~Git-hfs 1.0`, 64-hex ids)
  * a wide directory (300 files) edited one file per commit with CRLF / 66-hex (33-byte) ids
  * a blob containing NUL bytes, a symlink, an executable, .gitattributes, .gitmodules + a gitlink
    (mode 160000) pointing OUTSIDE the repository (must never be traversed)
  * a revert commit (re-introduces an older tree), a merge whose commit date is 2001 (older than
    its parents: catches timestamp folding along first parents), a feature branch the pipeline
    never sees, and deterministic dates so every OID is reproducible.
Main history: refs/tags/c<k> for k = 1..N.  Extra refs: refs/tags/feature, refs/tags/side.
"""
import os
import subprocess

from gvfslib import clean_env, git, git_out, init_bare

T0 = 1_791_000_000  # 2026-10-03, a Saturday (UTC)


def _ptr(blob_id_hex, size, crlf=False):
    nl = "\r\n" if crlf else "\n"
    return f"~~Git-hfs 1.0{nl}blob:{blob_id_hex}{nl}size:{size}{nl}".encode()


class FI:
    def __init__(self):
        self.out = []
        self.mark = 0

    def blob(self, data):
        self.mark += 1
        self.out.append(b"blob\nmark :%d\ndata %d\n" % (self.mark, len(data)) + data + b"\n")
        return self.mark

    def commit(self, ref, when, msg, parents, ops, author_when=None):
        self.mark += 1
        m = self.mark
        aw = author_when if author_when is not None else when
        msgb = msg.encode()
        s = [b"commit %s\nmark :%d\n" % (ref.encode(), m),
             b"author T <t@t> %d +0000\n" % aw,
             b"committer T <t@t> %d +0000\n" % when,
             b"data %d\n" % len(msgb), msgb, b"\n"]
        if parents:
            s.append(b"from %s\n" % parents[0].encode())
            for p in parents[1:]:
                s.append(b"merge %s\n" % p.encode())
        for op in ops:
            s.append(op.encode() if isinstance(op, str) else op)
            s.append(b"\n")
        self.out.append(b"".join(s) + b"\n")
        return m


def build_source(path, n=40, pointers_per_commit=6, wide=300, seed_commits=0):
    """Create the source bare repo. Returns dict name->oid ('c1'..'cN', 'feature', 'side')."""
    if os.path.exists(path):
        raise FileExistsError(path)
    init_bare(path)
    fi = FI()
    gitlink_target = "1" * 40
    # Commit 1 lays down the wide dir and fixed files.
    marks = {}
    prev = None
    for k in range(1, n + 1):
        ops = []
        when = T0 + 600 * k
        big = "".join(f"line {l}" + (f" edit {k}" if l % 97 == k % 97 else "") + "\n" for l in range(600))
        ops.append(f"M 100644 :{fi.blob(big.encode())} big.txt")
        for i in range(pointers_per_commit):
            bid = f"{k * 1000 + i:064X}"
            ops.append(f"M 100644 :{fi.blob(_ptr(bid, k * 7 + i))} d{i % 4}/e{i % 3}/f{k}-{i}.dat")
        if k == 1:
            for w in range(wide):
                bid = f"{w:064x}00"
                ops.append(f"M 100644 :{fi.blob(_ptr(bid, 4096 + w))} wide/file-{w:04d}")
            ops.append(f"M 100644 :{fi.blob(b'*.bin binary' + bytes([10]))} .gitattributes")
            ops.append(f"M 100644 :{fi.blob(b'[submodule \"sub\"]' + bytes([10]) + b'	path = sub' + bytes([10]) + b'	url = https://example.invalid/sub.git' + bytes([10]))} .gitmodules")
            ops.append(f"M 160000 {gitlink_target} sub")
            ops.append(f"M 120000 :{fi.blob(b'big.txt')} link-to-big")
            ops.append(f"M 100755 :{fi.blob(b'#!/bin/sh' + bytes([10]) + b'echo hi' + bytes([10]))} tools/run.sh")
        else:
            w = (k * 37) % wide
            bid = f"{(k << 16) | w:064x}00"
            ops.append(f"M 100644 :{fi.blob(_ptr(bid, 1234 + k, crlf=True))} wide/file-{w:04d}")
        if k % 5 == 1:
            ops.append(f"M 100644 :{fi.blob(bytes([0, 1, 2, k % 256, 0]) * 40)} bin/nul.bin")
        if k % 9 == 0:
            ops.append(f"D d{k % 4}")  # delete a whole directory now and then
        parents = [f":{prev}"] if prev else []
        m = fi.commit("refs/heads/main", when, f"c{k}", parents, ops)
        marks[f"c{k}"] = m
        prev = m
    # side branch with a 2001 clock, merged into main with a 2001 clock (opus T-13)
    side = fi.commit("refs/heads/side", 1_000_000_000, "side", [f":{marks[f'c{n}']}"],
                     [f"M 100644 :{fi.blob(b'side branch file' + bytes([10]))} side/s.txt"])
    marks["side"] = side
    # feature branch never seen by the pipeline
    feat = fi.commit("refs/heads/feature", T0 + 600 * (n + 5), "feature", [f":{marks[f'c{n}']}"],
                     [f"M 100644 :{fi.blob(_ptr('F' * 64, 99))} feature/x.dat",
                      f"M 100644 :{fi.blob(b'feature notes' + bytes([10]))} feature/notes.txt"])
    marks["feature"] = feat
    stream = b"".join(fi.out) + b"done\n"
    mfile = os.path.join(path, "marks.txt")
    p = subprocess.run(["git", f"--git-dir={path}", "fast-import", "--done", "--quiet",
                        f"--export-marks={mfile}"], input=stream, capture_output=True, env=clean_env())
    if p.returncode != 0:
        raise RuntimeError(p.stderr.decode())
    bymark = {}
    for line in open(mfile):
        mk, oid = line.split()
        bymark[int(mk[1:])] = oid
    oids = {name: bymark[m] for name, m in marks.items()}
    # merge commit (2001 clock) of main tip with side, and a revert of the last commit
    tip = oids[f"c{n}"]
    merge_tree = git_out(path, "rev-parse", f"{oids['side']}^{{tree}}")
    env01 = {"GIT_AUTHOR_DATE": "1000000000 +0000", "GIT_COMMITTER_DATE": "1000000000 +0000"}
    oids["merge"] = git_out(path, "commit-tree", merge_tree, "-p", tip, "-p", oids["side"],
                            "-m", "merge side (2001 clock)", env=env01)
    prev_tree = git_out(path, "rev-parse", f"{oids[f'c{n-1}']}^{{tree}}")
    envr = {"GIT_AUTHOR_DATE": f"{T0 + 600 * (n + 2)} +0000",
            "GIT_COMMITTER_DATE": f"{T0 + 600 * (n + 2)} +0000"}
    oids["revert"] = git_out(path, "commit-tree", prev_tree, "-p", oids["merge"],
                             "-m", "revert to c{n-1} tree", env=envr)
    for name, oid in oids.items():
        git(path, "update-ref", f"refs/tags/{name}", oid)
    git(path, "repack", "-adf", "-q", "--window=50", "--depth=30")
    return oids


def make_upstream(src, path, at):
    """Bare upstream whose objects come from src (alternates), refs/heads/main at `at`."""
    init_bare(path)
    with open(os.path.join(path, "objects", "info", "alternates"), "w") as f:
        f.write(os.path.join(os.path.abspath(src), "objects") + "\n")
    git(path, "update-ref", "refs/heads/main", at)
    return path


def move_main(up, oid):
    git(up, "update-ref", "refs/heads/main", oid)


def extra_commit(src, parent, when, k, pointers=4):
    """Deterministic commit on top of `parent` (any oid) dated `when`; returns its oid."""
    fi = FI()
    ops = []
    big = "".join(f"line {l}" + (f" xedit {k}" if l % 89 == k % 89 else "") + "\n" for l in range(600))
    ops.append(f"M 100644 :{fi.blob(big.encode())} big.txt")
    for i in range(pointers):
        bid = f"{(k << 20) | i:064x}00"
        ops.append(f"M 100644 :{fi.blob(_ptr(bid, k * 13 + i))} x{i % 3}/y{k % 5}/g{k}-{i}.dat")
    fi.commit(f"refs/extra/x{k}", when, f"extra {k}", [parent], ops)
    stream = b"".join(fi.out) + b"done\n"
    p = subprocess.run(["git", f"--git-dir={src}", "fast-import", "--done", "--quiet", "--force"],
                       input=stream, capture_output=True, env=clean_env())
    if p.returncode != 0:
        raise RuntimeError(p.stderr.decode())
    return git_out(src, "rev-parse", f"refs/extra/x{k}")


if __name__ == "__main__":
    import sys
    import json
    o = build_source(sys.argv[1], n=int(sys.argv[2]) if len(sys.argv) > 2 else 40)
    print(json.dumps(o, indent=1))
