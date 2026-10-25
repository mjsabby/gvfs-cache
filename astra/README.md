# gvfs-daemon

One Rust binary for a scheduled Git ingestion/compaction pipeline and a local or
remote GVFS cache. Every nonempty prefetch response contains **one Git pack**,
not one pack per storage tier; up-to-date clients receive zero packs. Original object IDs and reachable history are
preserved, including ordinary historical blobs alongside Git-HFS pointers.

## Run

```sh
cargo build --release

# Genesis, then incremental ingestion and any due rollups on subsequent runs.
target/release/gvfs-daemon pipeline \
  --upstream /home/muks/SearchGold --store data --work-dir work/pipeline

# The same serving implementation locally or remotely; edge/proxy are aliases.
target/release/gvfs-daemon serve \
  --upstream /home/muks/SearchGold --store data --work-dir work/serve \
  --bind 127.0.0.1:8080 --strict-alloc

# Standalone read-only edge, without contacting the original repository.
target/release/gvfs-daemon serve --read-only --store data \
  --work-dir work/readonly --bind 127.0.0.1:8080

# Stock Git uses the read-only smart-HTTP endpoint.
git clone --no-checkout http://127.0.0.1:8080 client

# Prefer one managed cache per host for repeated agent workspaces.
target/release/gvfs-daemon sync --server http://127.0.0.1:8080 \
  --cache-dir work/client-cache --strict-alloc
git clone --shared --no-checkout work/client-cache workspace

# Keep that cache current without repeatedly downloading an unchanged live pack.
target/release/gvfs-daemon sync --server http://127.0.0.1:8080 \
  --cache-dir work/client-cache --poll-seconds 60 --strict-alloc
```

Use distinct work directories for concurrently running processes. The work
directory is locked and contains the upstream transport cache, not authoritative
state. The source repository is only read; it is never repacked or modified.

`--upstream` also accepts GitHub, ADO, SSH and other Git transport URLs.
`--branch HEAD` is the default; use `--branch main` to select a named branch.
Native Git handles transport/authentication and keeps a negotiation ref in the
private cache. Rust parses, resolves, hashes, orders and writes the objects.
Loose local commits are imported through a temporary native Git pack.

Run `pipeline --tier auto` every 15 minutes. It ingests the missing interval and
rolls up closed UTC windows: **15m → 1h → 6h → 12h → 24h → 168h**.
Weeks start Monday UTC. Explicit `--tier 1h|6h|12h|24h|168h` jobs only read storage;
they do not contact Git. `--tier 15m` ingests without rolling up.

The server speaks HTTP/1.1, including keep-alive, chunked uploads and smart Git
protocol v0. Stock Git negotiates that protocol automatically. Put a TLS/HTTP/2
reverse proxy in front of a remote deployment; the binary does not terminate
incoming TLS or HTTP/2 itself. Use `--public-url` for the advertised cache URL.
`--blob-store-url` advertises an existing HFS payload service.
Inbound authentication, pushes and HFS payload hydration are outside this
read-only metadata service. Ordinary Git does not acquire GVFS prefetch support
merely by changing a URL: GVFS clients use the GVFS endpoints, while ordinary Git
uses `/info/refs` and `/git-upload-pack`.

## Managed client cache

`sync` keeps **one pinned baseline and one rolling incremental pack** after a
successful update. It does not rewrite the baseline or re-download complete
weekly rollups. It downloads the missing response, removes objects already
covered by the baseline, and splices the remaining objects with the previous
delta using the same pack engine. Required REF_DELTA bases stay in the pack.

Incoming indexes are computed by native Git, rather than trusted from the wire.
The merged delta is verified with native Git, and exact OID membership proves
that the baseline plus replacement covers **every old and downloaded object**.
Only then can the previous managed delta be unlinked. This also preserves
unsealed commits that disappear from the origin after a force push; timestamps
alone would not protect them.

Installation uses a checksummed journal, durable pack/index/metadata files and
atomic state publication. A crash can temporarily leave both delta generations;
recovery finishes installing the new one before retiring its predecessor.
Unmanaged packs and the baseline are never pruned. If an MIDX exists, native Git
rewrites it using an explicit pack list excluding the predecessor before
unlinking. No new MIDX is needed for the normal two-pack cache.

The cache reserves `refs/heads/gvfs-cache` and updates it from the response's
`X-GVFS-Tip`; existing user refs are not overwritten. A fresh cache's HEAD points
there, so `git clone --shared <cache> <workspace>` creates workspaces without
another network fetch or pack copy. Shared workspaces require the cache to remain
available. Use a fresh cache for the two-pack bound; existing unmanaged packs
remain additional files.

There is one sync writer per cache. `--poll-seconds` retains mappings and scratch
buffers between polls; one-shot invocations revalidate the cache at startup.
Use `--auth-env` for the server Authorization value. The cache is bound to its
server URL and rejects accidental reuse with another server. Local compaction
trades sequential delta-pack I/O for bounded file counts; it does not re-deltify
or recompress already received objects.

## Pack representations

`--pack-mode clustered` is the default. All three modes are implemented for
comparison; commits and blobs are always standalone.

| Mode | Baseline / long-term trees | Intraday objects |
|---|---|---|
| `full` | Standalone zlib entries | Standalone |
| `delta` | Retain existing upstream tree deltas where their bases are retained; normalize other entries | Standalone |
| `clustered` | Rebuild bounded directory-history tree clusters at genesis, 24h and weekly compaction | Standalone |

`delta` is the upstream-compression reference variant: splicing retains its
existing deltas but does not discover new ones during compaction. `clustered`
does discover new deltas from the undeltified intraday artifacts.

Clusters have at most **16 members**, a **1 MiB compressed span**, and
**depth-one** deltas against a standalone base. A tree larger than the byte
budget is standalone. Current-tip trees are marked as bases when a long-term
slab is built; older versions follow in newest-publication-first order.
An intervening revert can still reuse a historical delta before the next
long-term rebuild.

The encoder merges sorted tree entries into real Git COPY/INSERT instructions.
It handles scattered edits, additions and deletions, not just common prefixes
and suffixes. Directory fingerprints are compression hints only: an accidental
hint collision cannot change object identity or content.

Sealed deltas use **REF_DELTA**, not OFS_DELTA. This costs a 20-byte base OID but
lets the shared splice engine remove duplicates and select subsets without
corrupting offset distances. Required bases are included exactly once. Cluster
lookups coalesce the base and requested member into one bounded range, omitting
unrequested bytes from the outgoing pack. Whole-slab OFS concatenation is
translation-invariant, but deleting bytes inside a slab is not; no correctness
assumption depends on whole-slab concatenation.

## Shared engine and memory

* `ingest_git_delta` handles genesis, scheduled deltas and live catch-up.
* `Plan` performs exact deduplication with reusable bitsets and builds contiguous
  spans. `splice_slab_ranges` is shared by prefetch, object batches, live-slab
  merging and pipeline rollups.
* The CHD MPHF has exactly one slot per object. Fingerprints reject most misses,
  but **full 20-byte OIDs verify membership**. Fingerprints alone are never used
  to discard objects or satisfy requests.
* Packed locations retain the 16-bit fingerprint / 34-bit offset / 14-bit length
  layout. An escape table supports long entries **and offsets beyond 16 GiB**.
  Companion Git indexes use standard v2 large offsets.
* Fixed-capacity vectors, touched-word bitsets and prefaulted anonymous mmap
  arenas bound the object loop, delta cache, DFS queue and per-worker scratch.
  Capacity exhaustion is an error, never an implicit resize.
* Safe anonymous mappings hold admitted immutable source packs and indexes.
  This deliberately avoids unsafe file-mapping contracts if another process
  truncates a file. Served local packs remain in the filesystem page cache;
  whole-pack prefetch uses Linux `sendfile`. Remote packs are read through
  bounded, coalesced HTTP Range requests.
* SHA-1 and CRC use the libraries' hardware dispatch. HFS parsing and tree
  matching use safe byte slices; there are no application SIMD intrinsics.

The **allocation guarantee is scoped**: measured object-ingest loops, planning,
local serving and splicing allocate zero heap objects after their buffers are
created. Startup, artifact/index construction, snapshot publication, native Git
subprocesses and the pooled HTTP/TLS transport are not allocation-free.
`--strict-alloc` aborts on an allocation inside a guarded engine region;
`/metrics` exposes both hot-path and all-post-seal counters.

The exact-membership index is intentionally larger than the PDF's probabilistic
8.4-byte/key sketch: it also stores full OIDs, sizes/types, CRCs, publication
times, family hints and base ordinals. Do not budget only 40–80 MB for a
multi-million-object repository.

All application code forbids unsafe Rust and denies panic, unwrap, unchecked
indexing and arithmetic Clippy lints. The isolated allocator instrumentation
crate is the sole exception for `GlobalAlloc`. The tested wire codecs and
allocation guard were adapted from the supplied `gvfs-server/opus` reference.

### Bounds

`--max-objects` defaults to 16,777,216; `--max-object-mib` to 32;
`--delta-cache-mib` to 256; `--live-mib` to 128. These are explicit provisioning
limits, not guessed repository sizes. A request plan must fit all selected
source rows, including duplicate rows before deduplication.

`--workers` bounds accepted connections and scratch arenas. Idle reads/writes
time out after 15 seconds; `--request-timeout-seconds` also bounds a complete
request/response, so a trickling client cannot pin an arena indefinitely.
The server rejects requests while live/manifest synchronization is unhealthy.

## Publication, weekly catch-up and recovery

Artifacts are immutable `.pack`, `.idx` and `.mphf` triples. Keys include both
the pack digest and metadata digest: identical pack bytes can have different
publication-time metadata. The manifest is checksummed and conditionally
published only after every artifact is complete.

The filesystem backend uses durable temporary files, create-only installation,
directory fsync and a locked manifest compare-and-swap. Azure and S3 use
If-None-Match / If-Match. Large artifacts use bounded block/multipart uploads;
S3's HTTP-200-with-error completion response is explicitly rejected.

A weekly job publishes both a full baseline and a **net-new weekly delta**.
Cold clients receive the baseline plus newer data as one pack. Warm clients
receive only missed weekly/intraday data, also as one pack. Compaction removes
duplicate OIDs, not distinct historical trees needed by old commits.

The durable manifest alone advances `lastPackTimestamp`. Live objects are
included without advancing that cursor. Conditional clients use `ETag` /
`If-None-Match` to avoid re-downloading an unchanged response. Validators include
the cursor and immutable snapshot identity, so a live update or publication
invalidates them without hashing a multi-GB response. Clients that do not send
validators can still re-download L0. This preserves correctness across independent
edge clocks and pipeline hand-offs.

On restart the edge validates indexes and reconstructs the live gap before
opening the listener. On manifest hand-off it reconstructs any remaining gap
before atomically publishing a new reader snapshot. A pipeline crash leaves the
old manifest valid; a CAS loser retries from the winning manifest. Force pushes
do not discard historical objects.

Unreferenced artifacts are deliberately **not automatically deleted after an
arbitrary two-hour timeout**: a stale writer or pinned reader may still need
them. Reclaim orphaned/old-baseline objects offline after quiescing users of the
old manifests. Retained weekly deltas must not be expired by a blanket bucket
lifecycle rule. The manifest's 1,024-descriptor bound fails explicitly rather
than silently dropping long-lived clients' history.

One response creates at most one client pack; it cannot remove packs from
previous fetches on an unmodified client. Use the managed `sync` path above to
bound local packs; merely pointing an old client at the server does not enable
client-side compaction.

## Object storage

```sh
# SAS is the container query string, kept out of argv and logs.
gvfs-daemon pipeline --upstream https://dev.azure.com/org/project/_git/repo \
  --git-auth-env GIT_HTTP_HEADER \
  --store azure:https://account.blob.core.windows.net/container/prefix \
  --storage-query-env AZURE_SAS --work-dir work/pipeline

# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, optional AWS_SESSION_TOKEN,
# and AWS_REGION are read from the environment. Signature V4 is implemented
# without an AWS SDK. The endpoint may use virtual-hosted or path-style buckets.
gvfs-daemon pipeline --upstream https://github.com/org/repo.git \
  --git-auth-env GIT_HTTP_HEADER \
  --store s3:https://bucket.s3.us-west-2.amazonaws.com/prefix \
  --work-dir work/pipeline
```

`GIT_HTTP_HEADER` contains a complete header, such as `Authorization: Basic ...`.
For a generic HTTP object-store facade, `--storage-auth-env` supplies the
Authorization header value. Credentials are read at startup; rotation requires
a restart. Cloud protocols, conditional writes, exact ranges and multipart
failure handling are exercised against local mock services, not a live account.

## Endpoints

| Endpoint | Response |
|---|---|
| `GET /gvfs/config` | Cache configuration |
| `GET /gvfs/prefetch?lastPackTimestamp=N` | One GPRE-framed pack, zero packs when caught up, or `304` when its validator matches |
| `GET /gvfs/objects/<oid>` | Loose object; standalone entries are spliced without recompression |
| `POST /gvfs/objects` | One deduplicated pack; commits expand by `commitDepth`, direct tree/blob requests remain point requests |
| `POST /gvfs/sizes` | Git object sizes, **not** dereferenced HFS payload sizes |
| `GET /info/refs`, `POST /git-upload-pack` | Read-only smart Git transport |
| `GET /healthz`, `GET /metrics` | Readiness and allocation/request counters |

### Pack coverage headers

Prefetch responses include `ETag`; successful `200` responses also include
`X-GVFS-Tip`, even when no pack is needed. A `304` has no body or redundant
content-type/tip headers. Prebuilt wire indexes are omitted by default: the native Microsoft
helper rebuilds them anyway. `serve --prefetch-index` restores sending an index
when a complete prebuilt slab is returned. Persisted server `.idx` files are
unchanged.

Nonempty `/gvfs/prefetch` responses additionally send:

| Header | Meaning |
|---|---|
| `X-GVFS-Pack-Start` | Requested `lastPackTimestamp`, exclusive; `0` for a cold baseline |
| `X-GVFS-Pack-End` | Durable manifest timestamp, inclusive; matches the GPRE timestamp |
| `X-GVFS-Pack-Mode` | `baseline` or `incremental` |
| `X-GVFS-Pack-Contains-Live` | `true` when the pack also includes unsealed L0 objects |

These describe the sealed synchronization interval, **not exact birth-time
bounds for every object in the pack**. Required delta bases can precede Start;
live objects can extend beyond End. A live-only response can have Start = End
and a nonempty pack. Empty prefetches, object responses and errors do not carry
these pack-specific headers.

This is our extension; the public Microsoft `gvfs-helper` implementation checked
does not consume these header names. They do not trigger native pack deletion.
An `incremental` response assembled from a weekly slab is still timestamp-filtered,
not a complete weekly replacement. The managed client instead performs local
delta splicing and exact coverage verification; these headers alone never
authorize unlinking old packs.

The original variable-width `~~Git-hfs 1.0` bytes are retained. The parser accepts
LF/CRLF, 33-byte VSO BlobStore IDs and the explicit `InvalidBlobId` sentinel.
It does not rewrite them into the PDF's early fixed-width SHA-256 example.

## Reproduce measurements

```sh
RUSTFLAGS='-C target-cpu=native' cargo build --release
target/release/gvfs-daemon benchmark --upstream /home/muks/SearchGold \
  --store target/searchgold-benchmark --work-dir target/searchgold-work \
  --strict-alloc
python3 -B tools/bench_serve.py
python3 -B tools/verify_packs.py
cargo bench --bench engine
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Use fresh benchmark output directories. `benchmarks/searchgold.csv` contains the
recorded builds; `searchgold-serving.json` contains the initial, pre-optimization
loopback measurements (including wire indexes). The three variants contain the same **9,224,930 objects reachable
from the selected HEAD**. The source pack also contains other refs' objects, so
its 12,083,668-object size is not an apples-to-apples comparison.

| Representation | Pack bytes | Build seconds | Guarded allocations |
|---|---:|---:|---:|
| Preserved tree deltas | 2,673,226,062 | 98.369 | 0 |
| Bounded tree clusters | 4,405,522,013 | 167.353 | 0 |
| Full entries | 21,386,711,153 | 223.749 | 0 |

These are measurements, not the PDF's estimated 50–80 GiB or nanosecond claims.
The combined build run peaked at approximately 6.0 GiB RSS and used roughly one
CPU core. Prefetch throughput measurements include client-side pack/index
checksum verification. GET latency includes the loopback client; it is not an
isolated in-process lookup benchmark.

| Representation | Historical GET p50 / p99 | Verified prefetch | Prefetch server CPU |
|---|---:|---:|---:|
| Preserved tree deltas | 32.52 / 47.13 us | 2.60 GB/s | 0.11 s |
| Bounded tree clusters | 24.33 / 26.79 us | 2.23 GB/s | 0.18 s |
| Full entries | 23.14 / 27.89 us | 2.44 GB/s | 0.68 s |

Each historical GET run uses the same 64 OIDs, warmed once, for 2,560 samples.
Every prefetch contains one pack and reports zero guarded allocations.
All three complete packs and their indexes also pass native Git verification.
The verifier installs hard links in a temporary object database so Git's
deferred `.gitattributes` checks can read the objects being verified.
Git reports three `gitmodulesParse` warnings because those original `.gitmodules`
blobs are Git-HFS pointers. They are preserved, not rewritten or dropped.

### Clone and polling workload replay

`examples/workload_bench.rs` replays eight logical days on the published
SearchGold baseline. It creates real Git commits in a private repository:
the first change adds a 256-file Git-HFS directory, then one pointer changes
every five minutes. The pipeline runs every 15 minutes; the edge catches up
between publications. The original repository and seed artifacts are not changed.
`--diurnal-writes` limits changes to 09:00–17:00 UTC on weekdays.

```sh
RUSTFLAGS='-C target-cpu=native' cargo build --release --example workload_bench
target/release/examples/workload_bench
target/release/examples/workload_bench --diurnal-writes \
  --work target/workload-replay-diurnal-optimized \
  --output benchmarks/workloads-diurnal-optimized.csv
# Reproduce the earlier behavior for comparison:
target/release/examples/workload_bench --legacy \
  --work target/workload-replay-legacy --output benchmarks/workloads-legacy.csv
```

Use fresh work directories. The interval covers Monday through the following
Monday, including a weekend and six workdays. Developer schedules are 09:00,
12:00, 15:00 and 18:00, or just 09:00. Fresh isolated caches start every five
minutes with a two-minute offset from publication boundaries. All persistent
caches begin with one initial prefetch.

This is a **time-compressed traffic-accounting replay**, not a multi-day live
load test. It uses the real pipeline, live sync and response planner. By default,
warm responses go through the real managed client: it writes and verifies packs,
compacts deltas, commits state and unlinks predecessors. Conditional responses
use the same validator function as the HTTP server. Cold-clone byte totals are
calculated from actual encoded pack/index sizes instead of transferring TB:
the replay checks active-slab disjointness and cross-checks the cold calculation
against the production planner at daily boundaries.

CSV byte counts include Git packs, optional indexes and GPRE framing, but exclude
HTTP/TLS overhead and HFS payload hydration. Optimized retained-pack counts are
checked against the actual client cache directories. The baseline is validated
once for the client template and hard-linked into the replay's independent cache
fixtures; initial transfers are accounted for each cache. Each pack has a
corresponding `.idx`. `--legacy` instead models unpruned native
`prefetch-<timestamp>-<pack-hash>` identities and the old index-carrying responses.

Initial results are in `benchmarks/workloads.csv` and `benchmarks/workloads-diurnal.csv`;
optimized results use `workloads-optimized.csv` and `workloads-diurnal-optimized.csv`.
Run logs record the exact input dimensions and peak active-slab count. The replay
also exposed a hand-off bug: a newly published `have` commit need not have passed
through the edge's decoder cache. Ingestion now asks Git whether that commit
exists instead of unnecessarily fetching full history. These results include
that fix.

Pre-optimization eight-day results (decimal units; persistent-cache traffic below is **after**
the initial 4.680864 GB bootstrap):

| Persistent cache | Requests including bootstrap | Packs retained, continuous writes | Extra MB, continuous writes | Packs retained, diurnal writes | Extra MB, diurnal writes |
|---|---:|---:|---:|---:|---:|
| Poll every minute | 11,521 | 2,305 | 109.261 | 583 | 27.356 |
| Poll every 15 minutes | 769 | 769 | 17.690 | 199 | 4.485 |
| Poll hourly | 193 | 193 | 16.741 | 55 | 4.370 |
| Developer, four fetches/workday | 25 | 25 | 10.512 | 25 | 4.266 |
| Developer, one fetch/workday | 7 | 7 | 6.318 | 7 | 0.562 |
| Friday evening, then Monday morning | 3 | 3 | 4.252 | 3 | 1.130 |
| Unchanged repository, poll every minute | 11,521 | 1 | 0.092 | 1 | 0.092 |

The fresh-isolated-cache workload creates 2,304 caches, each receiving one pack.
Its accounted response-body traffic is 10.155746 TB with continuous writes and
10.211131 TB with diurnal writes. The slightly higher diurnal cold total is
primarily the old response-format asymmetry: an unchanged, pure-baseline
prefetch includes its `.idx`, whereas a spliced multi-slab prefetch omits it.

Continuous one-minute polling receives 8,449 nonempty responses and would retain
2,305 distinct native filenames: 6,144 responses repeat an unchanged L0 pack.
With diurnal writes these counts are 2,119 / 583 / 1,536. These are the baseline
costs addressed by conditional requests and managed local compaction.
Peak active storage was 17 sealed slabs plus L0 for continuous writes, or 13
sealed slabs plus L0 for diurnal writes; every nonempty client response is still
one pack.

### Optimized client results

Same eight-day workload, now with conditional requests and actual client
installation/compaction. The initial bootstrap is **4.405522 GB** without the
unused wire index. Traffic below is additional to that bootstrap.

| Persistent cache | Continuous packs, before → after | Continuous MB, before → after | Diurnal packs, before → after | Diurnal MB, before → after |
|---|---:|---:|---:|---:|
| Poll every minute | 2,305 → 2 | 109.261 → 33.268 | 583 → 2 | 27.356 → 8.349 |
| Poll every 15 minutes | 769 → 2 | 17.690 → 16.872 | 199 → 2 | 4.485 → 4.263 |
| Poll hourly | 193 → 2 | 16.741 → 16.348 | 55 → 2 | 4.370 → 4.254 |
| Developer, four fetches/workday | 25 → 2 | 10.512 → 10.512 | 25 → 2 | 4.266 → 4.252 |
| Developer, one fetch/workday | 7 → 2 | 6.318 → 6.318 | 7 → 2 | 0.562 → 0.553 |
| Friday evening, then Monday morning | 3 → 2 | 4.252 → 4.252 | 3 → 2 | 1.130 → 1.128 |
| Unchanged repository, poll every minute | 1 → 1 | 0.092 → 0.000008 | 1 → 1 | 0.092 → 0.000008 |

Exact repeated pack responses fell from **6,144 to zero** in the continuous
minute-polling case, and from **1,536 to zero** with diurnal writes. Conditional
polls still exchange HTTP headers, which are not counted in these body-byte
figures. Changing L0 responses may still contain overlapping objects; local
compaction removes their redundant storage, rather than pretending those network
bytes were never sent.

This trades local I/O for bounded pack count. Continuous minute polling wrote
18.778 GB of replacement **delta pack bytes** across eight days; the baseline
was never rewritten. That figure excludes index/state writes and verification
reads. The rolling delta can grow in bytes over time even though the number of
managed packs stays at two.

The real replay directories ended with two managed `.pack` files per active
persistent cache. Native Git also cloned shared workspaces from those caches with
zero workspace pack files and traversed their complete commit histories.
Fresh **isolated** caches still require a baseline each time; share a managed
cache per host to avoid repeatedly paying that cost.
