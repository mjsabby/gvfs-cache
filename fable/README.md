# gvfs-daemon

One Rust binary that implements the **Unified Binary Architecture** for a GitHFS monorepo
(Azure DevOps repositories whose every file is a `~~Git-hfs 1.0` pointer): the stateless
GVFS edge / localhost proxy and the scheduled slab pipeline are the same code, built on
exactly two primitives.

```
   ┌────────────────────────────────────────────────────────────────────────────────┐
   │                      gvfs-daemon  —  2 primitives, every mode                  │
   ├────────────────────────────────────────┬───────────────────────────────────────┤
   │ ingest_git_delta(have, want) -> Slab   │ splice(slabs) -> one pack (+ one idx) │
   │  • one upload-pack fetch (v2)          │  • bodies concatenated by reference    │
   │  • index: scan + parallel delta resolve│  • idx: k-way merge of sorted tables   │
   │  • dedup against published slabs       │  • only CPU: SHA-1 (cached per client │
   │  • seal .pack + .idx + .mphf           │    selection)                          │
   └──────────┬─────────────────────────────┴──────────────┬────────────────────────┘
              │                                            │
   ┌──────────┴───────────┐                 ┌──────────────┴──────────────────────────┐
   │ pipeline --tier m15  │ writes slabs    │ serve  (localhost proxy or shared edge)  │
   │ pipeline --tier h1…  │ rolls them up   │  GET  /gvfs/prefetch  -> 1 spliced pack  │
   │ pipeline --tier h168 │ weekly delta +  │  POST /gvfs/objects   -> 1 pack          │
   │   (ADO Pipeline /    │ fresh baseline  │  GET  /gvfs/objects/{oid}, POST /sizes   │
   │    GitHub Action)    │                 │  live (<15 min) slab via ingest_git_delta│
   └──────────────────────┘                 └──────────────────────────────────────────┘
                 manifest.bin (compare-and-swap)  +  immutable slab files
```

A cold clone always receives **exactly one pack** (and its `.idx`), no matter how many
slabs exist — the whole repository history plus everything that happened since the last
weekly baseline, spliced on the wire. A warm client never re-downloads the baseline.

## Build, test, run

```sh
cargo build --release                 # Linux, Rust 1.89+
cargo test --workspace                # unit tests + an end-to-end test driving git and the binary
cargo clippy --workspace --all-targets
```

```sh
# Scheduled job (ADO Pipeline / GitHub Action cron). Genesis happens by itself.
export ADO_PAT=...
gvfs-daemon pipeline --tier m15  --store /data/gvfs --upstream https://dev.azure.com/org/proj/_git/repo --token-env ADO_PAT
gvfs-daemon pipeline --tier h1   --store /data/gvfs --upstream ...      # :00  (also does the m15 step)
gvfs-daemon pipeline --tier h24  --store /data/gvfs --upstream ...      # midnight (re-fetched as one window)
gvfs-daemon pipeline --tier h168 --store /data/gvfs --upstream ...      # Sunday: weekly delta + fresh baseline
#   --rollup-fetch-from h24   tiers re-fetched from the upstream instead of spliced (default h24; `never` = always splice)
#   --leaf-tier h24           keep leaves below this tier: warm clients inside a window get only what they lack (default never)

# Edge (or a workstation proxy on 127.0.0.1); the upstream is only used for the live slab.
gvfs-daemon serve --bind 0.0.0.0:8080 --store /data/gvfs --upstream https://dev.azure.com/org/proj/_git/repo --token-env ADO_PAT \
    --cache-server "westus=https://gvfs-westus.example/org/repo"

# Any repository on disk works as an upstream (tests, mirrors): protocol v2 over git upload-pack.
gvfs-daemon pipeline --tier m15 --store /tmp/store --upstream /home/me/SearchGold

# Workstation hook (cron / post-fetch): prefetch into the repo's pack dir (or its shared object
# cache), name packs by covered interval, unlink subsumed packs, optional multi-pack-index.
gvfs-daemon client-prefetch --server http://127.0.0.1:8080 --repo ~/src/repo --midx

# Tools
gvfs-daemon inspect --store /data/gvfs --verify          # manifest, slabs, every object checked
gvfs-daemon lookup  --store /data/gvfs <oid>...          # slab / offset / type
gvfs-daemon splice  --store /data/gvfs --out /tmp/cold   # the pack+idx a cold clone would get
gvfs-daemon plan    --store /data/gvfs --since 1792800000 [--compact]   # what a prefetch at that cursor would get (JSON)
gvfs-daemon ingest  --store /data/gvfs --upstream ... --tier baseline --publish   # by hand

# Replay benchmark: the last N days of a real repository through the pipeline in 15-minute
# steps (GVFS_FAKE_NOW drives the clock), simulated clients evaluated with `plan`.
tools/replay_bench.py --src ~/SearchGold --days 14 --out /tmp/sim --config never:h24 --config h24:h24
```

GitHub upstreams: `--token-user x-access-token --token-env GH_TOKEN`. Secrets are only ever
read from environment variables.

## How it works

### Slabs

A slab is an immutable triple under `<store>/slabs/`:

| file | content |
| --- | --- |
| `slab-<seq>.pack` | a stock Git pack v2: the upstream's bytes, verbatim when possible |
| `slab-<seq>.idx` | a stock idx v2 (byte-identical to `git index-pack`'s), so clients never index gigabytes |
| `slab-<seq>.mphf` | the O(1) index: PTHash-style minimal perfect hash, 16 bytes/object |

**Deltas.** Trees are 90% of the bytes and compress 15× only as deltas (SearchGold: 4.8M
deltified trees take 0.37 GB; stored whole they would take >12 GB), so slabs keep the
upstream's `OFS_DELTA` trees. Every delta's base lives in the same slab, which makes a slab
*translation invariant*: concatenating whole slab bodies keeps every relative offset valid,
so rollups and prefetch responses never rewrite a byte. Commits, tags and pointer blobs are
stored as full objects (they are tiny and deltas buy nothing; every point lookup is one hop);
the policy stops at `--full-max-bytes` (4 KiB) of inflated size, so a repository with real
source blobs keeps those deltas — on git/git (313 MB upstream pack) the slab is 332 MB,
where de-deltifying every blob would have made it 1.65 GB.

**Dedup.** Incremental fetches re-send objects the client already has (a revert
re-introduces last week's trees). `ingest_git_delta` drops every object already present in a
published slab; a kept delta whose base was dropped is de-deltified right there, while its
content is still on the resolver's stack; OFS distances are re-encoded for the compacted
layout. Slabs are therefore pairwise disjoint and any selection splices into a valid pack.

**The `.mphf` file.** `bucket = mix(oid[0..8])`, a 16-bit pilot per bucket picked at build
time so `position = mix(oid[8..16] ^ pilot)` is injective, positions ≥ n redirected through
a small remap table (load factor 0.97), and a 16-byte descriptor per slot:
`[offset:36 | len:28][fingerprint = oid[12..20]]`. A probe for an absent key is rejected with
probability 1 − 2⁻⁶⁴, so the edge can probe every slab for every requested id without ever
inflating anything. 9.2M keys build in ~2 s and take 153 MB on disk (mmap, pre-faulted).

### The manifest (two tracks)

`manifest.bin` is the only mutable state: generation (compare-and-swap under `flock`), the
branch tip, and three lists of slab refs.

* **Track A — cold clones** (`lastPackTimestamp` 0 or older than the retained weeklies):
  the baseline plus the current pyramid.
* **Track B — warm clients**: weekly "aha" deltas and pyramid slabs newer than the client's
  timestamp. Never the baseline.

Slab timestamps are strictly increasing, so a client's cursor can never straddle two slabs.
A rollup is sealed with a fresh `end_ts` (it legitimately covers up to the moment it was
built), so a client that is already current receives the superset exactly once, at the
tier boundary — that delivery is what lets it drop the small packs.

**Zero-GC workstations.** A rollup slab is a strict superset of the slabs it replaced, and
the response carries the interval it covers (`X-GVFS-Pack-Start` / `X-GVFS-Pack-End`).
`gvfs-daemon client-prefetch` is the hook: it streams the pack and idx straight into the
pack directory as `prefetch-<start>-<end>.pack`, unlinks every prefetch pack whose interval
lies inside another's, and — when a leaf-keeping server leaves it with more than
`--budget` packs — collapses the newest half with one `&compact=1` request (the selection
then ignores leaves and returns the covering rollups whole). The cursor is the newest
interval end on disk, so the hook is stateless; `--midx` runs `git multi-pack-index write`
for good measure. Never `git gc`, never `index-pack`. Stock clients ignore the headers and
can lean on Scalar's background `multi-pack-index` maintenance instead. Retained leaves (see
`--leaf-tier`) are listed in the manifest (format v2) and mapped by the edge for prefetch
only; they are never probed for point lookups.

### Pipeline

`pipeline --tier X` does, in order: ingest since `manifest.tip` (one `m15` slab, deduplicated),
cascade rollups for every tier ≤ X (each tier rolls the lower-tier suffix of the pyramid
into one slab), and for `h168`: roll the whole pyramid into a weekly delta, prune weeklies
past `--retain-weeks`, rebuild the baseline (`--baseline fetch`: a fresh full fetch —
defragmented and garbage-free; `--baseline splice`: old baseline + weekly, no upstream
traffic). Then one CAS publish and a sweep of unreferenced files older than two hours. A
crash anywhere leaves the manifest untouched; the next run spans the gap.

**Splice or re-fetch.** A spliced rollup is a concatenation, so it stays at 15-minute
granularity: a lone commit costs ~25 KB (its trees have nothing to delta against), while
git packs a whole day of the same repository at ~2.5 KB/commit. From `--rollup-fetch-from`
(default `h24`) a rollup is therefore re-fetched from the upstream as one window — `have` =
the tip before the window, `want` = the tip at its end, deduplicated against everything
outside it — keeping the members' start and tip; it falls back to a splice if the
upstream refuses the `want`. Intraday tiers keep splicing (zero upstream traffic).

**Superset or leaves.** Every rollup is a strict superset of the slabs it replaced, and a
client whose cursor falls inside its window receives it whole — redundant bytes, but the
response's `X-GVFS-Pack-Start`/`End` interval lets a client hook `unlink` the subsumed
local packs, so a 15-minute poller never holds more than the pyramid depth plus the
weeklies (17 + w files, measured). With `--leaf-tier T` the rollups below `T` keep their
leaves instead: a client inside such a window receives only the leaves it lacks (one
spliced pack), nothing when it holds them all, and the rollup itself only with `compact=1`.
The knob trades bandwidth for peak file count on a 24/7 poller (14-day replay of
SearchGold, 3,258 commits, 3.4 MB/day of net-new objects; see the benchmark below):

| `--leaf-tier` | bytes/day, hook only | peak pack files | with a 20-pack budget (compact=1 re-fetches) |
| --- | --- | --- | --- |
| `never` (default) | 9.8 MB (2.9×) | 17 | 0 compactions — same 9.8 MB |
| `h6` | 7.6 MB (2.2×) | 31 | +1.9 MB/day → 9.5 MB |
| `h24` | 4.3 MB (1.3×) | 92 | +5.3 MB/day → 9.6 MB |
| `h168` | 3.7 MB (1.1×) | 542 | +5.7 MB/day → 9.4 MB |

Forcing a tight pack budget re-downloads exactly what the server-side supersets would have
sent, so leaves only pay off for clients that tolerate more local packs (a multi-pack-index
makes 100 packs a non-issue); for everyone else the default is the right answer.

### Serve

Boot maps every slab (`mmap`, indexes pre-faulted), allocates the request arenas, seals the
allocator, and binds. Requests touch only mapped slabs and pre-allocated arenas:

| endpoint | what happens |
| --- | --- |
| `GET /gvfs/prefetch?lastPackTimestamp=T[&compact=1]` | two-track selection → `GPRE` stream with **one** pack: header, slab bodies by reference, SHA-1 trailer, then the merged `.idx` (OID/CRC tables as zero-copy runs, offsets rebased, large-offset indices renumbered). The pack and idx checksums of a selection are cached, and a warm thread computes the cold selection's ahead of time — at boot and after every catalog change — so even the first cold clone streams by reference only. A single slab is passed through untouched. `compact=1` ignores retained leaves (the hook's "collapse my small packs" request). |
| `POST /gvfs/objects` | zero-allocation JSON scan into the arena → O(1) lookups → plan (commits expand to their tree closure; `commitDepth` adds parents) → one pack. A delta stays a delta if its base is in the pack (distance re-encoded), otherwise it is resolved and deflated while streaming. |
| `GET /gvfs/objects/{oid}` | a loose object. Full entries are *spliced* (stored block + the entry's own deflate blocks + `adler32_combine`): no inflate, no deflate. Deltas are resolved in the arena. |
| `POST /gvfs/sizes` | `[{"Id":..,"Size":..}]` from entry headers (a delta's result size costs one tiny inflate). |
| `GET /gvfs/config`, `/healthz`, `/metrics` | static JSON; counters (requests, prefetch bytes, misses and miss-fetch rounds, live objects), including the allocation guard's. |

The live slab: a thread polls the upstream branch every `--poll-secs` and runs the same
`ingest_git_delta(tip, HEAD)` into RAM, splicing the result into the previous live slab;
it re-reads the manifest every `--manifest-poll-secs` and drops the live slab when the
pipeline publishes. Requests see immutable catalog snapshots (`Arc` swap).

**Misses.** An object no slab holds (a feature branch the pipeline never saw, a commit
pushed seconds ago) is not a 404 when an upstream is configured: the request queues the id
(a pre-sized list, no allocation), the live thread fetches the whole batch in one protocol
v2 round — `want`s next to the tip, `have`s = every tip, so only the missing closure comes
back; an id the upstream rejects makes the batch retry id by id — merges it into the live
slab, swaps the catalog and wakes the waiters, which look once more. Smart-HTTP upstreams
that refuse arbitrary `want`s fall back to their own `POST /gvfs/objects`. Measured on
SearchGold: a branch commit absent from the 9.2M-object store → `200` in 0.77 s (one
`git upload-pack` round, exactly the branch's 8 own objects fetched), 0.4 ms afterwards.

## Engineering invariants

1. **100% safe Rust.** `unsafe_code = "forbid"` for the daemon. Two tiny crates hold one
   audited `unsafe` block each: `gvfs-mmap` (`mmap(2)` of immutable slab files) and
   `gvfs-alloc-guard` (`unsafe impl GlobalAlloc`).
2. **Panic-free production code.** `unwrap_used`, `expect_used`, `panic`, `indexing_slicing`,
   `arithmetic_side_effects`, `todo`, `unimplemented`, `unreachable` are `deny`; all slicing
   goes through `.get()`/patterns, all arithmetic is checked/saturating/wrapping.
3. **Zero heap allocations on the hot path.** Arenas and staging buffers are allocated at
   boot; the allocator is sealed and every endpoint runs inside a hot-path region. The
   end-to-end test asserts `gvfs_alloc_hot_path_total == 0` after exercising every endpoint,
   and `--alloc-guard strict` aborts on a violation. Generated frames come from a ring of
   staging chunks that is written only where every earlier frame has already been sent
   (`crates/gvfs-daemon/src/frames.rs`), and objects that must be re-deflated are streamed
   in chunk-sized frames, so neither backpressure nor object size can force an allocation;
   a 136 MB de-deltified closure streams with zero allocations to a fast and to a
   throttled client alike. Should every chunk still be in flight the body yields
   (`gvfs_frame_ring_busy_total`) and is polled again after hyper flushed.
4. **Minimal dependencies.** tokio, hyper (+hyper-util), bytes, sha1 (SHA-NI), crc32fast,
   flate2 with zlib-rs, rustls/webpki-roots (HTTPS upstreams), memmap2, clap. Everything
   else — pkt-line, protocol v2 client, pack/idx/delta, MPHF, GPRE, JSON, HFS pointers,
   loose-object splicing, the HTTP/1.1 client — is in-house.

## Measured (SearchGold: 658K commits, 9.2M objects reachable from main, 2.6 GB pack; AMD 9950X, NVMe)

| step | result |
| --- | --- |
| `ingest --tier baseline` (full fetch from a local clone, 16 workers) | **82 s** end to end: 9,224,930 objects, 3.34M pointer blobs, 273K objects de-deltified (+3 MB), `.idx` 280 MB, `.mphf` 153 MB |
| `git index-pack --verify` of the sealed slab (16 threads) | 37 s — the slab and our idx are valid; the idx is byte-identical to git's |
| `inspect --verify` (every object measured, sequential) | 11 s |
| `pipeline --tier m15` for 1,000 commits | **0.15 s**: 9,338 objects fetched, 3 revert-duplicates dropped |
| smart HTTP against github.com/git/git (409,658 objects, 313 MB; no token) | genesis **9.2 s** including the download, 332 MB slab, verified by `git fsck`; genesis at `master~300` (non-tip `want`) then an incremental step: 7,225 objects, 10 MB, 4.8 s |
| h1 rollup of two 500-commit slabs, re-fetched vs. spliced | **0.155 s**; same 9,335 objects in 1.64 MB vs. 1.93 MB (−15% with just two members; ~10× at 15-minute granularity) |
| warm client inside that window (`--leaf-tier h24`) | receives only the leaf it lacks (4,734 objects), not the 9,335-object rollup |
| cold-clone prefetch, 2 slabs (2.6 GB + 1.6 MB), checksums not yet known | **1.24 s** (SHA-1 over 2.6 GB + merged 280 MB idx) — verified by `git index-pack` |
| same selection, checksums cached | **0.27 s** (≈10.7 GB/s loopback, pure zero-copy) |
| first cold clone after boot, warm thread | the thread hashes the selection in 1.05 s while the server is already up; the request itself takes **0.23 s** |
| `POST /gvfs/objects` HEAD commit closure (411K trees, 136 MB) | **0.85 s**, 0 allocations (also with a 2 MB/s reader) |
| loose pointer blob / sizes / 523-object batch | 3–8 ms end to end (curl included), 0 allocations |

### Replay benchmark: bytes and pack files per client behaviour

`tools/replay_bench.py` replays the last 14 days of SearchGold's first-parent history
(3,258 commits, 47.3 MB of net-new objects as ingested) through the real pipeline in
15-minute steps — one `pipeline` run per step with the tier the clock dictates — and
evaluates simulated clients against the real prefetch selection (`plan`); the hook columns
model exactly what `client-prefetch` does. All five configurations take ~35 s of pipeline
time each for the 14 days. Config = `leaf-tier : rollup-fetch-from`; every client starts
with a 2,875 MB cold clone (one pack).

| config | client | fetches w/ data | incremental bytes | /day | max single fetch | packs: stock | hook: peak / final | hook, 20-pack budget: compactions / bytes / peak / final |
|---|---|---|---|---|---|---|---|---|
| never:h24 | agent (every 15 min, 24×7) | 1130 | 137.2 MB | 9.8 MB | 2.9 MB | 1130 | **17 / 17** | 0 / 0 / 17 / 17 |
| never:h24 | developer (weekdays 9/11/14/17 UTC) | 41 | 55.6 MB | 4.0 MB | 4.7 MB | 41 | 9 / 9 | 0 / 0 / 9 / 9 |
| never:h24 | laptop (Mondays 9:00) | 3 | 7.3 MB | 0.5 MB | 4.7 MB | 3 | 3 / 3 | 0 / 0 / 3 / 3 |
| h6:h24 | agent | 1079 | 106.3 MB | 7.6 MB | 2.9 MB | 1079 | 31 / 28 | 47 / 27.2 MB / 21 / 18 |
| h24:h24 | agent | 1074 | 59.8 MB | 4.3 MB | 2.9 MB | 1074 | 92 / 67 | 73 / 74.7 MB / 21 / 14 |
| h24:h24 | developer | 41 | 37.3 MB | 2.7 MB | 4.7 MB | 41 | 10 / 10 | 0 / 0 / 10 / 10 |
| h168:h24 | agent | 1070 | 52.2 MB | 3.7 MB | 2.9 MB | 1070 | 542 / 511 | 79 / 80.4 MB / 21 / 12 |
| h168:h24 | developer | 41 | 38.3 MB | 2.7 MB | 4.7 MB | 41 | 22 / 22 | 2 / 8.4 MB / 21 / 9 |
| never:never | agent | 1130 | 193.2 MB | 13.8 MB | **24.0 MB** (spliced weekly) | 1130 | 17 / 17 | 0 / 0 / 17 / 17 |
| never:never | developer | 41 | 105.3 MB | 7.5 MB | 27.7 MB | 41 | 9 / 9 | 0 / 0 / 9 / 9 |
| never:never | laptop | 3 | 33.9 MB | 2.4 MB | 27.7 MB | 3 | 3 / 3 | 0 / 0 / 3 / 3 |

| config | hourly cold clones (336) | bytes min / avg / max | slabs spliced avg / max | client older than the retained weeklies |
|---|---|---|---|---|
| any `:h24` | 1 pack each | 2875.1 / 2880.1 / 2885.0 MB | 7.3 / 14 | 2884.6 MB, 1 pack |
| never:never | 1 pack each | 2875.1 / 2899.1 / 2923.2 MB | 7.3 / 14 | 2923.2 MB, 1 pack |

What the numbers say:

* **A cold clone is always one pack**, 2,875–2,885 MB, spliced from up to 14 slabs.
* **Pure supersets (the default) need no client logic beyond `unlink`**: a 15-minute poller
  holds 17 packs (baseline + 2 weeklies + ≤ 6 days + h12 + h6 + ≤ 5 h1 + ≤ 3 m15), a
  developer 9–10, with zero compaction traffic. The price is 2.9× the net-new bytes for
  the poller (9.8 MB/day) and 1.2× for the developer.
* **Leaf tiers cut the poller's bytes to 1.1–1.3× but move the file problem to the client**:
  92 (`h24`) or 542 (`h168`) packs before the daily/weekly superset collapses them. With a
  20-pack budget, the compaction re-fetches bring every configuration back to ~9.8 MB/day.
* **Fetch-built rollups matter for Track B**: a weekly delta is 2.9 MB re-fetched versus
  24 MB spliced; the developer's two weeks cost 56 MB instead of 105 MB.
* Weeklies accumulate on warm clients (one file per week) until a longer tier subsumes
  them — a monthly delta is the natural extension (not implemented).
* Under `GVFS_FAKE_NOW` unreferenced slab files are swept immediately (mtimes are
  wall-clock), so a replay store stays at one baseline (~3 GB).

Reproduce: `tools/replay_bench.py --src ~/SearchGold --days 14 --out /tmp/sim` (~90 s for
the genesis baseline, then ~40 s for five configurations in parallel; `HOOK_BUDGETS=20,100`
adds hook columns, `REPLAY_DEBUG=agent-24x7` traces one client).

## Layout

```
crates/gvfs-mmap           read-only maps of slab files (the one mmap unsafe block)
crates/gvfs-alloc-guard    SealedAllocator: hot-path allocation counting / strict mode
crates/gvfs-daemon/src
  oid, arena, pack, zlib, delta, hfs, pktline, idx, mphf     formats and primitives
  frames                   staging ring for generated response frames
  transport                protocol v2 over `git upload-pack` (local) and smart HTTP(S)
  indexer                  scan + parallel delta resolution + dedup + emit
  ingest                   ingest_git_delta
  splice                   whole-slab splicing and the merged idx stream
  manifest, store          two-track manifest, filesystem store with CAS
  pipeline                 the scheduled job
  serve/                   catalog, arenas, handlers, bodies, live sync + miss fetches
  client                   the workstation prefetch hook (client-prefetch)
  cli, main                subcommands
tools/gpre_unpack.py       split a GPRE stream into pack/idx files (tests)
tools/replay_bench.py      replay a repository's history through the pipeline; bytes/pack-files per client type
```

## Notes and extension points

* The store is a directory (local disk or a shared filesystem). Azure Blob / S3 need the
  same six operations (`get`, conditional `put`, `list`, `delete`, slab upload/download); an
  edge in front of one would mirror the referenced slab files locally before mapping them.
* TLS termination is left to the load balancer; HTTP/1.1 and h2c are served.
* Responses are written with `writev` from the mapped pages (kernel copies once);
  `vmsplice`/`splice` zero-copy sends are a possible further step.
* The `~~Git-hfs 1.0` parser validates pointer blobs during ingest and reports counts; a
  `(oid → blob id, size)` sidecar for CAS pre-warming would be a small sequential pass.
