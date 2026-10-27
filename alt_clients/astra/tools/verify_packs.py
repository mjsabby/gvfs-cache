#!/usr/bin/env python3
"""Native Git verification with an installed ODB for deferred attribute checks."""
import argparse
import os
from pathlib import Path
import subprocess
import tempfile

from bench_serve import oid_set_digest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--store", type=Path, default=Path("target/searchgold-benchmark"))
    args = parser.parse_args()
    store = args.store.resolve()
    expected = None
    with tempfile.TemporaryDirectory(prefix="gvfs-verify-", dir=store.parent) as directory:
        bare = Path(directory) / "objects.git"
        subprocess.run(["git", "init", "--bare", "--quiet", str(bare)], check=True)
        for mode in ("delta", "clustered", "full"):
            packs = list((store / mode).glob("*.pack"))
            if len(packs) != 1:
                raise RuntimeError(f"{mode}: expected one baseline pack")
            pack = packs[0]
            idx = pack.with_suffix(".idx")
            actual = oid_set_digest(idx)
            if expected is not None and actual != expected:
                raise RuntimeError("representations contain different objects")
            expected = actual
            with pack.open("rb") as source:
                source.seek(-20, os.SEEK_END)
                digest = source.read(20).hex()
            for extension, source in (("pack", pack), ("idx", idx)):
                destination = bare / "objects" / "pack" / f"pack-{digest}.{extension}"
                if not destination.exists():
                    os.link(source, destination)
            subprocess.run([
                "git", "--git-dir", str(bare), "-c", "pack.threads=8",
                "index-pack", "--strict", "--verify", str(pack),
            ], check=True)
            print(f"{mode}: native Git passed; {actual[0]} objects; OID-set SHA256 {actual[1]}", flush=True)


if __name__ == "__main__":
    main()
