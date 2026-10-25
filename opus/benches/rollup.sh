#!/usr/bin/env bash
# Rollup quality on a real repository: replays one week of first-parent history the way
# the pipeline sees it and compares, per day and for the week, the size of
#
#   concat     the 15-minute packs as fetched, concatenated (a plain splice)
#   repacked   gvfs-daemon's optimal splice of its 15-minute slabs (what is published)
#   one fetch  the upstream's own pack for the whole range (git pack-objects / ADO)
#
# and the weekly baseline: seeded at the week's start, grown by the repacked week, against
# a fresh full fetch at the week's end.
#
# usage: benches/rollup.sh <repo> <week-start YYYY-MM-DD> [out-dir]
# env:   BIN (default target/release/gvfs-daemon), DAYS (default 7), BASELINE=0 to skip
#        the full fetches (each is a complete pack of the repository).
set -euo pipefail

REPO=${1:?usage: rollup.sh <repo> <week-start YYYY-MM-DD> [out-dir]}
START=${2:?week start date}
OUT=${3:-/tmp/gvd-rollup}
BIN=${BIN:-$(dirname "$0")/../target/release/gvfs-daemon}
DAYS=${DAYS:-7}
BASELINE=${BASELINE:-1}
THREADS=${THREADS:-$(nproc)}

rm -rf "$OUT"
mkdir -p "$OUT"/{raw15,m15,day,dayone,week,base}

size() { stat -c %s "$1"/*.pack; }
human() { numfmt --to=iec-i --suffix=B --format=%.2f "$1"; }
# Runs `gvfs-daemon <args>` writing into <dir>, which must end up with one slab.
slab_into() {
    local dir=$1
    shift
    rm -rf "$dir"
    mkdir -p "$dir"
    "$BIN" "$@" --out "$dir" --threads "$THREADS" 2>>"$OUT/log.txt"
    ls "$dir"/*.mphf >/dev/null
}

# 15-minute buckets of first-parent history: "<day> <have> <want> <commits>" per line,
# plus the commit before the window (the seed) and the window's last commit.
python3 - "$REPO" "$START" "$DAYS" >"$OUT/buckets.txt" <<'EOF'
import datetime, subprocess, sys
repo, start, days = sys.argv[1], sys.argv[2], int(sys.argv[3])
t0 = int(datetime.datetime.fromisoformat(start).replace(tzinfo=datetime.timezone.utc).timestamp())
t1 = t0 + days * 86400
log = subprocess.run(["git", "-C", repo, "log", "--first-parent", "--format=%H %ct", "HEAD"], capture_output=True, text=True, check=True).stdout
commits = [(l.split()[0], int(l.split()[1])) for l in log.splitlines()][::-1]
i0 = next(i for i, (_, t) in enumerate(commits) if t >= t0)
seed = commits[i0 - 1][0]
print("seed", seed)
have, bucket, last = seed, None, None
group = []
for c, t in commits[i0:]:
    if t >= t1:
        break
    b = max(t // 900, bucket or 0)
    if bucket is not None and b != bucket and group:
        print((bucket * 900 - t0) // 86400, have, group[-1], len(group))
        have, group = group[-1], []
    bucket = b
    group.append(c)
if group:
    print((bucket * 900 - t0) // 86400, have, group[-1], len(group))
EOF
SEED=$(awk '$1 == "seed" {print $2}' "$OUT/buckets.txt")
LAST=$(awk '$1 != "seed" {w = $3} END {print w}' "$OUT/buckets.txt")
echo "window: $(grep -vc seed "$OUT/buckets.txt") non-empty 15-minute buckets, $(awk '$1 != "seed" {n += $4} END {print n}' "$OUT/buckets.txt") commits"

# The 15m tier: fetch each bucket once, publish it repacked.
n=0
while read -r day have want _; do
    [ "$day" = seed ] && continue
    n=$((n + 1))
    id=$(printf "%02d-%04d" "$day" "$n")
    slab_into "$OUT/raw15/$id" ingest --upstream "$REPO" --have "$have" --want "$want"
    slab_into "$OUT/m15/$id" repack --tier 15m "$OUT"/raw15/"$id"/*.mphf
done <"$OUT/buckets.txt"

printf "\n%-6s %8s %12s %12s %12s %12s\n" day commits concat 15m-repacked rollup "one fetch"
total_concat=0
for d in $(seq 0 $((DAYS - 1))); do
    dd=$(printf "%02d" "$d")
    ls -d "$OUT"/m15/"$dd"-* >/dev/null 2>&1 || continue
    commits=$(awk -v d="$d" '$1 == d {n += $4} END {print n}' "$OUT/buckets.txt")
    have=$(awk -v d="$d" '$1 == d {print $2; exit}' "$OUT/buckets.txt")
    want=$(awk -v d="$d" '$1 == d {w = $3} END {print w}' "$OUT/buckets.txt")
    concat=$(cat "$OUT"/raw15/"$dd"-*/*.pack | wc -c)
    m15=$(cat "$OUT"/m15/"$dd"-*/*.pack | wc -c)
    total_concat=$((total_concat + concat))
    slab_into "$OUT/day/$dd" repack --tier 24h $(ls "$OUT"/m15/"$dd"-*/*.mphf)
    slab_into "$OUT/dayone/$dd" ingest --upstream "$REPO" --have "$have" --want "$want"
    printf "%-6s %8s %12s %12s %12s %12s\n" "$d" "$commits" "$(human "$concat")" "$(human "$m15")" "$(human "$(size "$OUT/day/$dd")")" "$(human "$(size "$OUT/dayone/$dd")")"
done

slab_into "$OUT/week/repacked" repack --tier 168h $(ls "$OUT"/day/*/*.mphf)
slab_into "$OUT/week/one" ingest --upstream "$REPO" --have "$SEED" --want "$LAST"
printf "%-6s %8s %12s %12s %12s %12s\n" week "" "$(human "$total_concat")" "" "$(human "$(size "$OUT/week/repacked")")" "$(human "$(size "$OUT/week/one")")"
grep -h "repack: [0-9]* slabs" "$OUT/log.txt" | tail -1 | sed 's/^\[[0-9.]*\] /  last rollup: /'

if [ "$BASELINE" = 1 ]; then
    # The seed at the week's start, its deltas re-pointed; the week appended to it; and what
    # a fresh full fetch at the week's end would be.
    slab_into "$OUT/base/raw" ingest --upstream "$REPO" --want "$SEED"
    slab_into "$OUT/base/seed" repack --tier base --mode copy "$OUT"/base/raw/*.mphf
    rm -rf "$OUT/base/raw"
    slab_into "$OUT/base/next" repack --tier base --frozen "$(ls "$OUT"/base/seed/*.mphf)" "$OUT"/week/repacked/*.mphf
    slab_into "$OUT/base/fresh" ingest --upstream "$REPO" --want "$LAST"
    seed=$(size "$OUT/base/seed")
    next=$(size "$OUT/base/next")
    fresh=$(size "$OUT/base/fresh")
    echo
    echo "baseline at the week's start:   $(human "$seed")"
    echo "  + the repacked week:          $(human "$next") (grew $(human $((next - seed))))"
    echo "fresh full fetch at week's end: $(human "$fresh")"
    grep -h "repack: 1 slabs .* frozen" "$OUT/log.txt" | tail -1 | sed 's/^\[[0-9.]*\] /  append: /'
fi
