#!/usr/bin/env bash
# Load scenarios against a running `gvfs-daemon serve` (the README's benchmark table).
#
#   benches/load.sh <host:port> <upstream.git> [seconds-per-scenario]
#
# Object ids are sampled from <upstream.git> (main). Uses the HTTP load generator from
# gvfs-server-benchmark (LOADGEN=/path/to/loadgen). Start the server with
# `--alloc-guard strict` to have any hot-path heap allocation abort it; the last line
# prints the server's allocation counter, which must be 0. loadgen sends no User-Agent,
# which `--prefetch-idx auto` answers with the pack's .idx too: start the server with
# `--prefetch-idx never` to measure pack streaming alone (as in the README's table).
set -euo pipefail

ADDR=${1:?usage: load.sh <host:port> <upstream.git> [seconds]}
REPO=${2:?usage: load.sh <host:port> <upstream.git> [seconds]}
SECS=${3:-10}
LOADGEN=${LOADGEN:-$HOME/gvfs-server-benchmark/target/loadgen/release/loadgen}
OUT=${OUT:-$(mktemp -d -t gvd-load.XXXXXX)}

mkdir -p "$OUT"
echo "results in $OUT"
# A fixed seed keeps the sample reproducible. `--random-source=<(yes)` would too, but its
# sample is biased (no commits at all in 400,000 ids of SearchGold).
seeded() { openssl enc -aes-256-ctr -pass pass:"$1" -nosalt </dev/zero 2>/dev/null; }
git --git-dir "$REPO" rev-list --objects main | cut -c1-40 \
    | shuf -n 400000 --random-source=<(seeded gvfs-load) > "$OUT/oids.txt"
git --git-dir "$REPO" cat-file --batch-check='%(objectname) %(objecttype)' < "$OUT/oids.txt" > "$OUT/typed.txt"
awk '$2 == "blob" { print $1 }' "$OUT/typed.txt" > "$OUT/blobs.txt"
awk '$2 == "tree" { print $1 }' "$OUT/typed.txt" > "$OUT/trees.txt"
awk '$2 != "commit" { print $1 }' "$OUT/typed.txt" > "$OUT/noncommits.txt"

run() {
    local name=$1
    shift
    printf '%-18s ' "$name"
    "$LOADGEN" --addr "$ADDR" --warmup 2 --duration "$SECS" --json "$OUT/$name.json" "$@" | head -1
}

run get-blob       --workload get-object   --oids "$OUT/blobs.txt" --conns 64
run get-tree       --workload get-object   --oids "$OUT/trees.txt" --conns 64
run post-4000      --workload post-objects --oids "$OUT/noncommits.txt" --batch 4000 --conns 32
run sizes-4000     --workload sizes        --oids "$OUT/oids.txt" --batch 4000 --conns 32
run prefetch-fresh --workload prefetch     --ts 4102444800 --conns 64
run prefetch-cold  --workload prefetch     --ts 0 --conns 8
if [[ -n ${WARM_TS:-} ]]; then
    run prefetch-warm --workload prefetch --ts "$WARM_TS" --conns 64
fi

curl -s "http://$ADDR/metrics" | grep -E '^gvfs_(alloc_hot_path_total|errors_total) '
