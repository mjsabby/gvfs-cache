# gvfs-daemon

One Rust binary for scheduled Git ingestion/compaction and a local or remote GVFS
read cache. The shared primitives are `ingest_git_delta` and
`splice_slab_ranges`. A nonempty prefetch response contains **exactly one
deduplicated Git pack**, regardless of the number of selected slabs.

```sh
cargo build --release --locked

# Bootstrap, then run every 15 minutes. "auto" ingests and performs due UTC rollups.
target/release/gvfs-daemon pipeline \
  --storage ./data --upstream /path/to/repo --tier auto

# The same server runs on a workstation or a remote edge; only --bind changes.
target/release/gvfs-daemon serve \
  --storage ./data --upstream /path/to/repo --bind 127.0.0.1:8080
```

`edge` and `proxy` are aliases for `serve`. Keep Git's origin unchanged and
configure a GVFS-capable client to use this cache. This is a GVFS metadata
service, **not** a stock-Git smart-HTTP remote, filesystem provider, or HFS
payload hydrator. Changing `url.insteadOf` alone does not make stock Git speak GVFS.
`--help` lists capacities, authentication, TLS and deployment options.

## Two comparable tree layouts

`--tree-layout full` keeps every entry standalone, with Merkle DFS bootstrap
ordering. `--tree-layout clustered` (default) groups directory versions by their
inferred path in **24h, weekly-delta and weekly-baseline** slabs. A cluster has
one newest full tree and at most 15 older, depth-1 OFS deltas. A new full base
starts when the family changes, the cluster fills, the byte-span bound is reached,
or a delta is not worthwhile. Prefix/suffix matching handles localized edits;
a linear sorted-entry merge preserves unchanged islands for scattered edits.
No rolling-hash matcher is needed.

**Commits, blobs and all L0/15m/1h/6h/12h entries remain standalone in both
modes.** Original full entries retain their original compressed bytes.
Ordinary blobs are preserved alongside actual `~~Git-hfs 1.0` pointers.
LF/CRLF, 32/33-byte BlobStore IDs and `InvalidBlobId` are supported without
rewriting pointer text or changing Git OIDs. `/gvfs/sizes` reports Git object
sizes, not dereferenced HFS payload sizes.
Malformed HFS-looking historical blobs are diagnosed and preserved as opaque Git
blobs, not silently discarded or rewritten. `--strict-hfs` rejects them instead.

Full-body concatenation preserves OFS distances only when nothing inside a
body changes. Arbitrary subsets/deduplication instead add the necessary bases
once, emit dependencies first, and rewrite affected OFS **headers only**.
Compressed delta payloads remain untouched. A stored historical tree's
`[full base .. requested member]` is contiguous and needs one range read.

Rollups preserve each object's publication epoch in a checksummed, range-bound
sidecar (8 bytes/object; no extra baseline sidecar). Warm prefetch filters objects
already covered by the cursor, rather than resending an entire intersecting
daily/weekly rollup. Required delta bases are still included. Legacy slabs without
epochs conservatively use the slab's end time. L0-to-sealed handoffs can still
resend objects: independently observed edge timestamps are not safe deletion or
coverage proofs.

Synthesized prefetch responses include a native Git idx v2, avoiding client-side
inflation/index reconstruction. Header rewrites update CRCs algebraically without
reading payloads. Cold-clone indexing reuses the complete baseline's sorted idx
and merges the small tail linearly, rather than sorting millions of OIDs.
`--coarse-prefetch --no-prefetch-index` selects the previous behavior for A/B
measurements.

## Allocation and memory contract

Application code forbids unsafe Rust and denies panic/unwrap/unchecked-index
and arithmetic lints. The small `gvfs-platform` crate isolates the forwarding
allocator and OS file-mapping boundary.

Ingest, JSON/OID parsing, lookup, planning, tree reconstruction and sealing
loops use fixed-capacity arrays, ordinal tables, resettable scratch and
prewarmed zlib states. Allocation-count tests fail if these loops allocate.
Response channels, worker plans and streaming buffers are allocated at startup
and reused. Exhaustion is an explicit error, not an allocating fallback.
HTTP admission, connection lifetimes and pinned catalog generations are bounded.
Generated indexes reserve approximately 68 additional bytes per configured
object per worker arena (sorted rows plus worst-case encoded idx capacity).

This is **not a claim that Tokio/Hyper/TLS or catalog publication never allocate**.
Transport/task and control-plane allocations are outside the synchronous hot
regions. `/metrics` exposes the measured hot-region allocation count.

Large construction/spool arenas use private, anonymous temporary-file mappings
in `--cache-dir`; the full-layout experiment can spill through the page cache
without keeping two expanded histories on the heap. Reloaded files are verified
into read-only private mappings, rather than unsafely trusting external inodes
not to be truncated. Published slices are immutable and sent by shared ownership.
The hot-path counter measures heap allocations, not OS page faults or disk I/O.

The MPHF uses CHD pilots, packed-u64 descriptors and ordinal handles. Its
fingerprint is only a fast rejection: **full 20-byte OIDs prove membership**.
Large lengths/offsets escape to the fixed metadata record, including offsets
beyond 16 GiB in full-layout experiments. Standard Git idx v2 files include
CRCs, SHA-1 trailers and 64-bit offsets.

Only commit-parent/root-tree and subtree edges are persisted, using bounded
depth-1 adjacency deltas. Raw construction lists live in mmap-backed scratch,
not giant heap vectors. Even structural history would otherwise dwarf the pack.
Full Merkle rebuilds read canonical trees; commit closures walk the sidecar
without zlib and include commits/trees, not every file blob. Explicit blob OIDs
are included.

## Pipeline, storage and recovery

Explicit `--tier 1h|6h|12h|24h|168h` jobs compact existing storage and never
contact Git. `15m` ingests; `auto` ingests and runs due rollups (weekly boundary:
Sunday 00:00 UTC). The weekly job publishes both a full baseline for cold clones
and a baseline-excluding net-new delta for warm clients. Expired warm cursors
fall back to the full baseline instead of silently missing history.

Artifacts are immutable. Local publication is create-new/link + fsync and an
atomic manifest rename under a process-shared lock. Local pipeline jobs serialize
publication and old-orphan cleanup. HTTP manifests use ETags and conditional PUT;
conflicting jobs fail explicitly and can be retried. A missing manifest is only
genesis; a corrupt manifest is an error.

Startup loads the manifest/indexes and reconstructs the unsealed tip before
listening. Live polling and the pipeline share ingestion. Manifest installation
and rebuilding any remaining live suffix precede one catalog swap, so a handoff
cannot discard newer objects. Old readers retain complete snapshots. Failed sync
keeps the previous snapshot readable and marks health unready. Live capacity is
configurable rather than expiring at 15 minutes during pipeline outages.

```sh
# Azure Blob artifacts: a container/prefix SAS URL supplied through an environment
# variable, or --storage-auth-env for a complete Bearer Authorization header.
STORAGE_BUCKET="$ARTIFACT_CONTAINER_URL" target/release/gvfs-daemon pipeline \
  --storage-kind azure --upstream https://dev.azure.com/org/project/_git/repo \
  --token-env ADO_PAT --tier auto

# S3: path-style bucket/prefix endpoint; standard AWS_ACCESS_KEY_ID,
# AWS_SECRET_ACCESS_KEY and optional AWS_SESSION_TOKEN are read at startup.
target/release/gvfs-daemon pipeline --storage https://s3.us-east-1.amazonaws.com/bucket/prefix \
  --storage-kind s3 --s3-region us-east-1 \
  --upstream https://github.com/org/repo.git --token-env GH_TOKEN --tier auto
```

Azure block uploads and S3 multipart uploads handle large packs. Remote serving
warms metadata only; packs stay remote and exact `206 Content-Range` reads are
coalesced. Pipeline rebuilds hydrate each source pack once instead of issuing
millions of point GETs. Local orphan cleanup has a two-hour grace period.
**Automatic cloud orphan deletion is deliberately disabled:** deleting immutable
keys concurrently with publication requires a provider-specific GC fencing
protocol. Cloud orphans are harmless and retained; do not apply an age-only
lifecycle rule to published slabs.

## Endpoints

| Request | Response |
| --- | --- |
| `GET /gvfs/config` | Advertised cache configuration |
| `GET /gvfs/prefetch?lastPackTimestamp=N` | Zero or one GPRE pack plus native idx; an unchanged response is the 8-byte zero-pack envelope |
| `POST /gvfs/objects` | One pack; JSON `{"commitDepth":1,"objectIds":["<oid>"]}` |
| `GET /gvfs/objects/<oid>` | Standard loose object; full entries reuse deflate blocks without recompression |
| `POST /gvfs/sizes` | JSON array request `["<oid>"]`, Git object sizes |
| `GET /healthz`, `GET /metrics` | Freshness/readiness and counters |

HTTP/1.1 and HTTP/2 use identical handlers, optionally TLS with ALPN.
Missing requested objects or required closure members produce 404.

## Same-input benchmarks

The completed real-repository comparison is in [BENCHMARKS.md](BENCHMARKS.md).

`bench` fetches/reads the input **once**, builds both variants, verifies each with
native Git (`verify-pack`, `fsck`), asserts one physical Git pack, and writes
`results.csv` plus independently servable stores under a unique run directory.
Timings include arena initialization, decoding, ordering and index construction,
but exclude network acquisition, artifact publication and native validation
(validation time is recorded separately). Lookup timings include timer overhead;
tree decode timings include OID verification. Range-size samples describe the
stored one-read span, not a measured cloud RTT.

```sh
target/release/gvfs-daemon bench \
  --storage target/bench-searchgold --cache-dir target/bench-input \
  --upstream /home/muks/SearchGold \
  --input-pack /home/muks/SearchGold/.git/objects/pack/pack-ec111cc94ed5a20453e303e45695428e4ac3a35b.pack \
  --max-objects 16777216 --max-links 2147483647 \
  --pack-mib 65536 --object-mib 128 --delta-cache-mib 512
```

These are explicit large-repository capacities, not laptop defaults. The input
repository is read-only; all generated artifacts stay in the output/cache
directories. Normal `pipeline` and `serve` accept the same layout/capacity flags,
so both layouts can also be benchmarked through identical HTTP requests.

## Accelerated real-history workload replay

`tools/workloads.py` drives the real pipeline, L0 and HTTP endpoints through the
`workload_server` example. It consumes a pre-window baseline and a first-parent
history JSON (`start`, `end`, `base`, and `commits` containing
`[oid, effective_unix_time, original_committer_time]`). Commit time is a replay
proxy for push time, not an observed ref-update trace. The retained SearchGold
inputs and results are described in [BENCHMARKS.md](BENCHMARKS.md).

```sh
cargo build --release --example workload_server --locked
python3 tools/workloads.py \
  --history target/workloads/run-14d/history.json \
  --baseline target/workloads/store \
  --upstream target/workloads/upstream.git \
  --output target/workloads/replay-14d
```

The runner creates its own upstream fork, store and client repositories. It
measures hourly isolated cold agents, shared-cache agents every 15 minutes,
continuous 15-minute polling, and weekday 08:00/11:00/14:00/17:00 UTC developers.
A frozen Monday catch-up measures the final developer weekend backlog. Cold,
shared and polling requests use different phases so they also exercise L0.

Warm caches start with the same baseline. Each actual download is installed into
three independent caches: no maintenance, daily `git multi-pack-index write`,
and daily manual `scalar run pack-files`. Maintenance controls share immutable
downloads, not object databases or MIDX files. No `scalar register`, `run config`,
`start`, or scheduler registration is performed. Git/HOME/XDG configuration and
the session bus are isolated.

Per-request CSVs record exact GPRE and HTTP byte counts, pack/object counts and
checksums; separate records capture maintenance, rewritten pack bytes and physical
pack peaks. Native Git validates synthesized packs/indexes and incremental object
coverage. This is an accelerated metadata replay over loopback, not a concurrent
fleet throughput test, wall-clock soak, or external HFS-content benchmark.
Scalar's incremental repack/expiry can rewrite packs even with `gc.auto=0`.
The daemon does not delete workstation packs or implement timestamp tombstoning.

An additional client-maintenance control can replay the retained downloads
without repeating server traffic:

```sh
python3 tools/workloads.py --replay-maintenance target/workloads/replay-14d
```

It uses daily 32 MiB MIDX batches and immediate MIDX-proven expiry. This tests
consolidating small packs while retaining the large original baseline; it is
**not stock Scalar's default policy** and does not install a workstation hook
or change scheduled maintenance.

```sh
cargo fmt --all --check
cargo test --workspace --all-targets --locked --message-format=short
cargo clippy --workspace --all-targets --locked --message-format=short -- -D warnings
```

Integration tests use Git, Python 3 (independent SigV4 verification), and OpenSSL
(a temporary local TLS identity). Cloud storage tests use a local protocol fixture,
not live credentials.
