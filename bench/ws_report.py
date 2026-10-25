#!/usr/bin/env python3
"""Tables for the client-lifecycle study: bytes per client pattern (replay result.json) and
packs on disk per client regime (workstation.json), with a projection to long horizons.

Usage: python3 ws_report.py opus fable fable@h24     (REPLAY_WORK)
"""
import json
import os
import sys

W = os.environ.get("REPLAY_WORK", "/home/muks/gvfs-bench/ws")
CLIENTS = ["agent-1m", "agent-15m", "agent-1h", "dev-hourly", "dev-3x", "dev-morning", "dev-vacation"]
REGIMES = ["stock", "scalar", "vfs", "geometric", "hook"]
HORIZONS = [("1 week", 7), ("1 month", 30), ("6 months", 182), ("2 years", 730)]


def slope(v, regime):
    """Steady growth in packs/day. Real-git regimes: the troughs right after each daily
    maintenance (incl. the run after the week), last four; hook: end-of-day counts."""
    if regime == "stock":
        return v["per_day"]
    if regime == "hook":
        xs = [n for _, n in sorted(v.get("end_of_day", {}).items())]
    else:
        xs = [m[3] for m in v.get("maint", [])] + ([v["after_next_maint"]] if "after_next_maint" in v else [])
    if len(xs) < 3:
        return 0.0
    tail = xs[-4:]
    g = (tail[-1] - tail[0]) / (len(tail) - 1)
    return g if g > 0.1 * max(1.0, v.get("per_day", 0) or 1.0) and g > 1.0 else 0.0


def main():
    impls = sys.argv[1:] or ["opus", "fable", "fable@h24"]
    ws = json.load(open(os.path.join(W, "workstation.json")))
    rep = {i: json.load(open(os.path.join(W, i, "result.json"))) for i in impls}

    print("Bytes received over the week (GPRE bytes; x = / git's ideal for the same have->want)\n")
    print(f"| client | " + " | ".join(impls) + " |")
    print("|---|" + "---|" * len(impls))
    for c in CLIENTS:
        cells = []
        for i in impls:
            st = rep[i]["clients"][c]
            ideal = st["ideal_bytes"] or 1
            cells.append(f"{st['gpre_bytes'] / 1e6:.1f} MB ({st['gpre_bytes'] / ideal:.2f}x), "
                         f"{st['packs']} packs")
        print(f"| {c} | " + " | ".join(cells) + " |")

    print("\nPacks on disk after the week (peak during the week) → after the next daily maintenance,"
          " seed clone included\n")
    for i in impls:
        print(f"\n{i}\n")
        print("| client | " + " | ".join(REGIMES) + " |")
        print("|---|" + "---|" * len(REGIMES))
        for c in CLIENTS:
            cells = []
            for r in REGIMES:
                v = ws[i][c].get(r)
                if not v:
                    cells.append("-")
                elif "error" in v:
                    cells.append("ERR")
                elif "after_next_maint" in v:
                    cells.append(f"{v['packs_end']} ({v['peak']}) → {v['after_next_maint']}")
                else:
                    cells.append(f"{v['packs_end']} ({v['peak']})")
            print(f"| {c} | " + " | ".join(cells) + " |")

    print("\nChecks: missing objects after maintenance, gvfs-helper cursor lost, objects lost by the hook\n")
    bad = 0
    for i in impls:
        for c in CLIENTS:
            for r in REGIMES:
                v = ws[i][c].get(r) or {}
                if v.get("error") or v.get("missing_objects") or v.get("cursor_lost") or v.get("objects_lost") \
                        or v.get("cursor_mismatch"):
                    bad += 1
                    print(f"  {i} {c} {r}: {json.dumps({k: v.get(k) for k in ('error', 'missing_objects', 'cursor_lost', 'objects_lost', 'cursor_mismatch')})}")
    print(f"  {bad} problems")

    print("\nMaintenance cost over the week (wall seconds incl. multi-pack-index write; MB of packs written)\n")
    print("| impl | client | " + " | ".join(r for r in REGIMES if r in ("scalar", "vfs", "geometric")) + " |")
    print("|---|---|---|---|---|")
    for i in impls:
        for c in ("agent-1m", "agent-15m", "dev-hourly"):
            cells = []
            for r in ("scalar", "vfs", "geometric"):
                v = ws[i][c].get(r) or {}
                cells.append(f"{v.get('maint_s', 0):.0f} s, {v.get('maint_written_mb', 0):.1f} MB" if v else "-")
            print(f"| {i} | {c} | " + " | ".join(cells) + " |")

    print("\nProjection (packs on disk). Growing regimes: linear in the growth of the troughs after each"
          " daily maintenance; bounded ones: ≤ the week's peak\n")
    for c in ("agent-1m", "agent-15m", "agent-1h", "dev-hourly", "dev-3x"):
        print(f"\n{c}\n")
        print("| impl / regime | packs/day | " + " | ".join(h for h, _ in HORIZONS) + " |")
        print("|---|---|" + "---|" * len(HORIZONS))
        for i in impls:
            for r in REGIMES:
                v = ws[i][c].get(r)
                if not v or "error" in v:
                    continue
                g = slope(v, r)
                base = v.get("after_next_maint", v["packs_end"]) if g else v["peak"]
                cells = [str(round(base + g * (d - 7))) if g else f"≤{base}" for _, d in HORIZONS]
                print(f"| {i} / {r} | {g:.1f} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
