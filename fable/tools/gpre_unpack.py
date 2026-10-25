#!/usr/bin/env python3
"""Split a GVFS `GPRE` prefetch stream into pack-<n>.pack / pack-<n>.idx files.

    curl -s 'http://127.0.0.1:8080/gvfs/prefetch?lastPackTimestamp=0' | tools/gpre_unpack.py outdir

Prints one line per pack (timestamp, pack bytes, idx bytes) and exits non-zero on a
malformed stream, so it doubles as a protocol check in tests.
"""
import struct
import sys
import os


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    out = sys.argv[1]
    os.makedirs(out, exist_ok=True)
    data = sys.stdin.buffer.read()
    if data[:6] != b"GPRE \x01":
        print(f"bad magic: {data[:6]!r}", file=sys.stderr)
        return 1
    (count,) = struct.unpack_from("<H", data, 6)
    at = 8
    for i in range(count):
        ts, pack_len, idx_len = struct.unpack_from("<qqq", data, at)
        at += 24
        pack = data[at:at + pack_len]
        at += pack_len
        idx = b""
        if idx_len > 0:
            idx = data[at:at + idx_len]
            at += idx_len
        if len(pack) != pack_len or len(idx) != max(idx_len, 0):
            print("truncated stream", file=sys.stderr)
            return 1
        with open(os.path.join(out, f"pack-{i}.pack"), "wb") as f:
            f.write(pack)
        if idx:
            with open(os.path.join(out, f"pack-{i}.idx"), "wb") as f:
                f.write(idx)
        print(f"pack {i}: timestamp {ts} pack {pack_len} bytes idx {idx_len} bytes")
    if at != len(data):
        print(f"{len(data) - at} trailing bytes", file=sys.stderr)
        return 1
    print(f"{count} pack(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
