# gvfs-daemon

One Rust binary that puts a fast [GVFS protocol](https://github.com/microsoft/VFSForGit/blob/master/Protocol.md)
layer in front of a Git repository and is built on two primitives:

* **`ingest_git_delta(have, want)`**: one fetch from the upstream git server becomes an
  immutable **slab**: a pack plus a `.mphf` index (minimal perfect hash + 40-byte object
  records) built in parallel, 15× faster than `git index-pack`. The upstream is asked for
  every commit once.
* **splice**: slabs become **one** pack. On the wire, slab bodies are concatenated
  verbatim. Into a new slab (`repack`, the optimal splice), trees are re-deltified into
  per-directory clusters, so a rollup built only from stored slabs is as small as one
  upstream fetch of its whole range.

The same binary runs in three roles:

* the scheduled **pipeline**: 15-minute slabs, rolled up through 1h / 6h / 12h / 24h / 168h
  tiers in object storage;
* the **server** (`serve`): a remote edge or a localhost proxy, with the same code. It keeps
  the minutes since the last sealed slab in RAM as L0 slabs (one per push), built by the very
  same `ingest_git_delta` and `repack`;
* the **mirror**, for stores whose blobs live in packs of their own (`--blobs separate`): it
  copies every blob of the history into the store, and repairs the store on demand.

Every prefetch response, cold clone included, is exactly **one pack**, as native Git
sends, with its `.idx` for VFS for Git (which installs it instead of indexing the pack).

```text
          Git server (upload-pack v2, v0 fallback; smart HTTP or local path)
             │ have/want: new commits only                 │ have/want: new commits only
             ▼                                             ▼
  gvfs-daemon pipeline (cron, every 15 min)      gvfs-daemon serve  (edge or 127.0.0.1 proxy)
   ingest_git_delta → repack → 15m slab           L0: one repacked slab in RAM
   rollup 1h/6h/12h/24h: repack from the store    mmap'd slabs + MPHF lookups
   168h: weekly delta + baseline append           /gvfs/prefetch  → ONE pack (+ .idx)
   CAS publish manifest.bin (If-Match)            /gvfs/objects   → loose / one pack
             │                                    /gvfs/sizes, /gvfs/config, /metrics
             ▼                                             ▲
        object storage: manifest.bin + slabs/<id>.pack + slabs/<id>.mphf
        (a directory, or Azure Blob Storage)
```

Also in this repository:

* [DESIGN.md](DESIGN.md): formats, algorithms and engineering rules;
* [REPORT.md](REPORT.md): why this implementation, opus, was chosen out of five, and every
  measurement and change since;
* [tests_plus_benchmarks_opus.md](tests_plus_benchmarks_opus.md): the tests and benchmarks
  of its first build;
* `alt_clients/`: three of the other implementations (fable, sol and astra), kept for the
  cross-client tests and the comparison. Each builds on its own, outside this workspace;
* `bench/`: the harness behind REPORT.md (Python; stock git judges every byte).

## Build and test

The toolchain is pinned in `rust-toolchain.toml` (Rust 1.99.1, edition 2024), and rustup
installs it on first use. The daemon runs on Linux, on x86-64 or ARM64.

```sh
cargo build --release            # target/release/gvfs-daemon
cargo test --workspace --release # unit tests + end-to-end tests (needs git on PATH)
cargo clippy --workspace --all-targets
```

To cross-build for ARM64 on x86-64, install an ARM64 C toolchain (Debian and Ubuntu:
`gcc-aarch64-linux-gnu`). To run the tests as ARM64 binaries, also install `qemu-user` and
`qemu-user-binfmt`; binfmt runs the daemons the tests start under qemu too.

```sh
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
       CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc
cargo build --release --target aarch64-unknown-linux-gnu
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUNNER="qemu-aarch64 -L /usr/aarch64-linux-gnu" \
QEMU_LD_PREFIX=/usr/aarch64-linux-gnu \
    cargo test --release --target aarch64-unknown-linux-gnu --workspace
```

The end-to-end tests (`tests/`) run the real binary against scratch repositories and
check every byte the server sends with git itself (`index-pack`, `fsck`, `cat-file`).
Some tests also use extra tools when present and skip otherwise:

| Test | Needs |
|---|---|
| `tests/e2e.rs` `gvfs_helper_against_the_daemon` | Microsoft git with `gvfs-helper` (`GVFS_TEST_MSGIT`, default `~/git/bin-wrappers/git`) |
| `tests/separate.rs` `stock_gvfs_helper_checks_out_a_separate_store` | Microsoft git with `gvfs-helper`, as above |
| `tests/separate.rs` `blob_over_4gib_end_to_end` | `#[ignore]`d: writes ~13 GB under `target/`; run with `cargo test --release --test separate -- --ignored blob_over_4gib` |
| `tests/azure.rs` | the Azurite emulator (`azurite-blob` on PATH via `npm install -g azurite`, or `GVFS_TEST_AZURITE`), or a real account: `GVFS_TEST_AZURE_CS_FILE` (file holding a connection string) + `GVFS_TEST_AZURE_CONTAINER` (existing container; the tests use and delete a unique prefix) |

## Usage

```sh
# First run on an empty store is the one-time seed: the whole branch becomes the baseline.
gvfs-daemon pipeline --store azure:gvfs/searchgold --tier 15m \
    --upstream https://dev.azure.com/org/proj/_git/SearchGold --token-env ADO_PAT

# The edge (or, with --bind 127.0.0.1:8080, the localhost proxy on an agent VM).
gvfs-daemon serve --store azure:gvfs/searchgold --bind 0.0.0.0:8080 \
    --upstream https://dev.azure.com/org/proj/_git/SearchGold --token-env ADO_PAT \
    --cache-dir /nvme/gvfs-cache

# Clients: point VFS for Git / gvfs-helper at http://edge:8080/<anything>/
git config remote.origin.url http://edge:8080/org/SearchGold
git gvfs-helper --cache-server=disable prefetch
```

`gvfs-daemon help` lists every flag. Stores are named by:

* `fs:<dir>`, or a plain path;
* `azure:<container>[/<prefix>]`, with credentials (an account key or a SAS connection
  string) in `AZURE_STORAGE_CONNECTION_STRING`, or in the variable `--store-auth-env` names.

### Scheduling the pipeline

Run one job every 15 minutes with the highest tier that is due:

* Runners need no state: everything comes from the store.
* A tier's rollup folds everything below it: `--tier 6h` also covers the 1h rollup, and
  `168h` folds the whole pyramid into the week's delta and appends it to the baseline.
* Only the new commits are fetched from the upstream; every rollup is repacked from slabs
  already in the store.
* Concurrent runs are safe: the manifest is published with compare-and-swap, and the loser
  starts over.

```sh
m=$(date -u +%M) h=$(date -u +%H) dow=$(date -u +%u)
tier=15m
[ "$m" = 00 ] && tier=1h
[ "$m" = 00 ] && [ $((10#$h % 6)) = 0 ] && tier=6h
[ "$m" = 00 ] && [ $((10#$h % 12)) = 0 ] && tier=12h
[ "$m" = 00 ] && [ "$h" = 00 ] && tier=24h
[ "$m" = 00 ] && [ "$h" = 00 ] && [ "$dow" = 7 ] && tier=168h
gvfs-daemon pipeline --store "$STORE" --upstream "$ADO_URL" --token-env ADO_PAT --tier $tier
```

A crashed or skipped run only makes the next 15m slab wider. Slab files that no manifest
references are deleted after `--gc-grace-secs` (default 2 h).

### Real blobs in their own packs (`--blobs separate`)

For repositories whose files are real content (no `Git-hfs` pointers), a store created
with `--blobs separate` keeps commits and trees in the slabs, each slab with a table of
the sizes of the blobs its trees introduce, and every blob in blob packs of their own.
Prefetch and cold clones then carry no blobs at all. See [DESIGN.md](DESIGN.md#stores-with-real-blobs---blobs-separate).

```sh
# Genesis: the history's commits and trees, and every blob's size, from the upstream.
gvfs-daemon pipeline --store "$STORE" --upstream "$ADO_URL" --token-env ADO_PAT \
    --tier 15m --blobs separate

# The seed of the blobs: every blob of the history (the tip's first), resumable,
# verified by id and size, published as it goes; edges report ready once it is done.
gvfs-daemon mirror --store "$STORE" --upstream "$ADO_URL" --token-env ADO_PAT \
    --jobs 16 [--rate-mbps 4000] [--max-items N]

# Then schedule the pipeline as below: every run copies its window's new blobs before
# publishing the trees that reference them (keep --work-dir, or --cache-dir, on a
# persistent disk: a run reuses the store objects the previous one downloaded). Edges
# serve blobs from the blob packs (mapped, or read by range from Azure) and proxy only
# the blobs of the commits the pipeline has not sealed yet.
gvfs-daemon serve --store "$STORE" --upstream "$ADO_URL" --token-env ADO_PAT ...
```

An edge of such a store:

* reports ready (`/healthz`) once the mirror has copied the whole history;
* works with stock gvfs-helper unchanged, which prefetches commits and trees and faults
  blobs in (`GET`/`POST /gvfs/objects`);
* sends sizes without blobs to clients that ask: a prefetch with `X-GVFS-Sizes: 1` gets a
  `GSZ1` section after the packs.

Nothing repairs such a store by itself, and nothing should need to:

* If storage loses a blob pack or a segment index, edges and pipeline runs carry on. They
  log a warning, and edges serve the blobs involved through the proxy.
* An edge whose sealed trees name a blob the store lacks counts it in
  `gvfs_blobs_missing_total`.
* Either is the signal to run `gvfs-daemon mirror` again. It takes what the store lost out
  of `blobs.bin` and copies whatever is not listed; on a whole store it copies nothing.

### Inspecting

```sh
gvfs-daemon inspect manifest --store azure:gvfs/searchgold
gvfs-daemon inspect slab /nvme/gvfs-cache/base-1792879257-83da3df7dce1a81c.mphf
#   9222096 objects (9222096 pack entries), 4503 partitions x 512 buckets, ...
#   657912 commits, 5221743 trees, 3342441 blobs, 0 tags
#   3506901 deltas (mean depth 23.3, max 50)
#   3341849 GitHFS pointers -> 255.81 TiB of content; 592 other blobs (12.79 MiB)
```

Two more tools work on slabs directly:

* `inspect slab --write-idx out.idx` writes the `.idx` a VFS for Git client would be sent
  with that pack;
* `gvfs-daemon repack --out <dir> [--frozen <base.mphf>] <slab.mphf>...` runs the optimal
  splice by hand.

`curl http://edge:8080/metrics` reports, in Prometheus text format:

* **requests:** `gvfs_requests_total` by endpoint, prefetch packs sent, 404s
  (`gvfs_not_found_total`), 503s for want of an arena or growth budget (`gvfs_busy_total`),
  errors, and the bytes and objects sent;
* **the store:** slabs, L0 slabs, L0 ingests, manifest loads and the manifest's generation;
* **allocations:** `gvfs_alloc_hot_path_total` (and its bytes), which must stay 0, and every
  allocation since startup, anywhere (`gvfs_alloc_post_seal_total`);
* **connections:** open and idle; idle ones closed after `--idle-secs`, ones closed because
  `--max-idle-conns` was reached, and how often `--max-conns` held new ones back. Also the
  shared budget left for oversized buffers (`gvfs_buffer_spare_bytes`), and the room arenas
  have grown into for commit expansions (`gvfs_grown_objects`, within `--grow-objects`);
* **separate-blob stores:** blobs served from the store, proxied, missing (should stay 0)
  and unavailable; the `blobs.bin` generation, and whether it is complete
  (`gvfs_blobs_complete`);
* **blob packs in object storage:** the edge's block cache
  (`gvfs_blob_cache_{hits,misses,fetches,bypass}_total`, `gvfs_blob_cache_bytes`); size it
  with `--blob-cache-mib` (default 32 GiB).

## Performance

Measured on SearchGold, a real GitHFS repository cloned from ADO: main has 9.22M objects
and a 2.42 GiB pack; the full clone has 12.08M objects and 3.04 GiB. Host: AMD Ryzen 9
9950X (16 cores / 32 threads), Linux 7.3, loopback.

| | |
|---|---|
| Index the full ADO pack (12.08M objects, REF_DELTA chains up to depth 50), verified identical to git's `.idx` | **7.0 s** (git `index-pack --threads=32`: 107 s) |
| Genesis index of main (9.22M objects) | 2.4 s + 0.35 s MPHF (the upstream takes 70 s to generate the pack) |
| 15m slab: 50 commits, 469 objects, ingest + repack + upload | 0.04 s |
| Repack a week of slabs (15,908 objects) into the weekly delta | 0.16 s |
| Append the week to the 2.42 GiB baseline (re-indexed and verified) | 4.3 s |
| Seed: re-point the ADO pack's 5.1M REF deltas as OFS (3.04 GiB → 2.96 GiB) | 9.9 s |
| `.idx` for the 9.22M-object baseline (byte-identical to git's; client `index-pack`: 40 s) | 4.3 s |
| L0: push to upstream → served by the edge | < 1 s (one poll interval) |
| Cold clone with `gvfs-helper` (one 2.42 GiB pack; client `index-pack` dominates) | 31 s (native `git clone`: 114 s) |
| Commit closure `POST` (all 505,605 trees of main) | 137.8 MB in 0.70 s |

Rollups (`benches/rollup.sh`: one SearchGold week, 1,656 commits in 526 15-minute
slabs, against the upstream's own pack for the same range):

| | 15m packs concatenated | repacked (published) | one upstream fetch |
|---|---|---|---|
| busiest day (290 commits) | 4.32 MiB | 819 KiB | 800 KiB |
| the week | 21.90 MiB | 2.63 MiB | 2.65 MiB |
| baseline growth for the week | | 2.22 MiB | 2.35 MiB (fresh full fetch) |

Client traffic (`examples/clients.rs`): 14 days of SearchGold (2026-10-10 to 10-24,
3,308 commits) replayed on a simulated clock through the pipeline (a run every 15
minutes, tiers as in the cron script above) and the edge's L0 logic (an update on every
push), with each client's prefetches answered by the server's own plan selection. Ideal:
one self-contained upstream pack of exactly the objects the client lacks.

| Client | prefetches | packs received | MiB received | MiB ideal | × ideal | `168h` | `never` |
|---|---:|---:|---:|---:|---:|---:|---:|
| CI agents: fresh clone every 30 min, 24×7 | 672 | 672 | 2.42 GiB each | +0.04 MiB each | 1.0000 | 1.0000 | 1.0000 |
| agent VM: prefetch every minute, 24×7 | 20,159 | 2,839 | 92.6 | 83.1 | 1.11 | 1.04 | 1.00 |
| agent VM: prefetch every 15 min, 24×7 | 1,343 | 1,053 | 55.3 | 46.1 | 1.20 | 1.07 | 1.01 |
| agent VM: hourly maintenance prefetch, 24×7 | 335 | 321 | 35.2 | 26.4 | 1.33 | 1.12 | 1.01 |
| dev laptop: hourly maintenance 08:00-18:00 weekdays | 109 | 105 | 17.6 | 13.1 | 1.34 | 1.15 | 1.01 |
| developer: fetch 09:00, 13:00, 17:30 weekdays | 29 | 29 | 12.8 | 8.4 | 1.53 | 1.24 | 1.02 |
| developer: fetch each weekday morning | 9 | 9 | 9.0 | 5.9 | 1.53 | 1.31 | 1.02 |
| developer: back after 8 days off | 3 | 3 | 5.0 | 4.3 | 1.17 | 1.09 | 1.02 |

Notes on this table:

* Warm rows exclude each client's initial clone (one pack, 1.00× ideal).
* Every response is one pack: the slabs from the client's cursor to the tip, repacked
  together when there are several.
* The default `--superset-tier 24h` also sends each sealed day and week to each client
  whole, once (0.65 MiB a day here), so that stock client maintenance can delete the small
  packs it subsumes. `168h` does it for weeks only, and `never` sends exact covers (the last
  two columns; DESIGN.md, "Prefetch plans").
* Before plans were exact covers (DESIGN.md, "Warm traffic"), warm clients got 2.2-2.5× the
  ideal.
* Keeping replaced slabs listed costs 277 slabs (29.5 MiB) and a 38 KB manifest.

Load (`benches/load.sh` against `f100038`): SearchGold after a week of pipeline runs (an
inline store, 12 live slabs), the edge on 8 CPUs (4 cores and their SMT siblings) with
`--alloc-guard strict` and `--prefetch-idx never`, loadgen on the other 16 CPUs. Ids are
400,000 sampled uniformly from main. There were 0 hot-path allocations and 0 errors.

| Workload | Throughput | p50 / p99 |
|---|---|---|
| `GET /gvfs/objects/<blob>` (loose) | 1.24M req/s | 49 / 100 µs |
| `GET /gvfs/objects/<tree>` (deltified: resolved, deflated) | 181K req/s, 0.72 GB/s | 328 / 853 µs |
| `POST /gvfs/objects`, 4000 ids → one pack (11.2 MB, delta bases included) | 369 req/s, 4.1 GB/s | 86 / 94 ms |
| `POST /gvfs/sizes`, 4000 ids | 17.0K req/s | 1.8 / 2.8 ms |
| prefetch, up to date | 1.29M req/s | 47 / 92 µs |
| prefetch, warm (265 KB: the last 3.3 hours of slabs) | 72K req/s, 19.1 GB/s | 0.88 / 1.57 ms |
| prefetch, cold (2.6 GB pack via `sendfile`) | 81 GB/s aggregate (8 conns) | 241 / 377 ms |

A uniform sample is mostly trees (57% of main's objects), most of them deltified: the
costliest objects to serve. With separate blobs, a POST of 4,000 blobs runs at 8,700 req/s,
and sizes of 4,000 at 20,100 (REPORT.md §16).

## Repository layout

```text
src/main.rs, cli.rs        command dispatch and flag parsing (no framework); lib.rs: the modules
src/ingest.rs              primitive 1: ingest_git_delta + the parallel pack indexer
src/splice.rs              primitive 2 on the wire: pack plans, object splices
src/repack.rs              primitive 2 into a slab: the optimal splice (tree clusters)
src/idx.rs                 the git .idx of a prefetch pack (merged per-slab sorted runs)
src/slab.rs, mphf.rs       slab index format and the partitioned PTHash MPHF
src/idfilter.rs            blocked Bloom filters that rule slabs out of a lookup
src/timeline.rs            commit-derived timestamps; which slabs answer a client's cursor
src/pipeline.rs            the scheduled job: genesis, 15m, rollups, weekly, GC
src/mirror.rs              --blobs separate: the resumable seed (and on-demand repair) of the blob packs
src/separate.rs            --blobs separate: what pipeline, mirror and edge share (sizes, blob index,
                           the history's blobs, lost-object checks)
src/blobs.rs, sizes.rs     blob packs, segments, blobs.bin; sizes tables and the GSZ1 section
src/blobstream.rs, blobcopy.rs   streaming whole-blob packs from upstream fetches; copy items, publish
src/manifest.rs, store.rs, azure.rs    manifest.bin (CAS) and the fs / Azure stores
src/serve/                 the server on tokio: view (prefetch plans), httpd, conn (a task per
                           connection), handlers, budget (yields), live L0 loop, blobs (blob
                           packs and the read-through proxy), blobcache
src/upstream.rs, pktline.rs   git protocol v2/v0 over smart HTTP or a local repo (async;
                           `Upstream` blocks on it for the pipeline, mirror and live loop)
src/aclient.rs             the async HTTP client (hyper, tokio-rustls): the upstream's requests
                           and the edge's Azure ranged reads
src/http.rs                the blocking HTTP client (the Azure store's other calls, tests)
src/pack.rs, zlib.rs, delta.rs, object.rs, hfs.rs   git formats (delta.rs: tree delta encoder)
src/arena.rs               per-request arenas (no allocation while serving)
src/inspect.rs             `gvfs-daemon inspect`: the manifest and slabs, readable
src/util.rs, fsutil.rs, b64.rs   errors, ids, hex, time, logging; durable writes; base64
crates/gvfs-alloc-guard    counting global allocator that enforces zero hot-path allocations
crates/gvfs-mmap           the one memory map
crates/gvfs-prefetch       the one cache prefetch hint (batched lookups; x86-64 and ARM64);
                           with the two above, the only `unsafe` in the workspace
tests/                     end-to-end tests (pipeline, serve, HTTP v2/v0, gvfs-helper, Azure,
                           separate blobs)
benches/load.sh            the load table above
benches/rollup.sh          the rollup table above (any repository, any week)
examples/clients.rs        the client traffic table above (simulated clock, any window)
rust-toolchain.toml        the pinned toolchain (Rust 1.99.1; x86-64 and ARM64 Linux targets)
DESIGN.md, REPORT.md       the design, and the evidence: the comparison and every change since
bench/                     the harness behind REPORT.md (conformance, cross-client, replays,
                           load, profiles, byte-identity A/Bs of two builds)
alt_clients/               fable, sol and astra: other implementations of the same design, each
                           a workspace of its own with its tests_plus_benchmarks_<name>.md
```
