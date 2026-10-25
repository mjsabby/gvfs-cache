# gvfs-daemon design

This is how the *Unified Binary Architecture* from the design PDF is built, what the code
guarantees, and where it departs from the PDF (with the measurements that motivated each
change). The README covers usage and benchmarks.

## Engineering rules

These are the rules of `gvfs-server/opus/DESIGN.md`, applied to this workspace:

1. **Safe Rust.** The main crate inherits `unsafe_code = "forbid"` from the workspace.
   Two operations cannot be written safely: a `GlobalAlloc` implementation and `mmap(2)`.
   Each lives in its own tiny crate (`crates/gvfs-alloc-guard`, `crates/gvfs-mmap`) with
   `undocumented_unsafe_blocks` denied.
2. **No panics.** `unwrap_used`, `expect_used`, `panic`, `indexing_slicing`,
   `arithmetic_side_effects`, `todo`, `unimplemented`, and `unreachable` are denied. Slices
   are read with `.get()` and slice patterns; arithmetic is checked, saturating, or
   wrapping where wrapping is the intent. (Tests may unwrap: `clippy.toml`.)
3. **No heap allocation while serving.** Everything a request touches is allocated before
   the server starts: per-worker I/O buffers and a pool of request arenas
   (`src/arena.rs`). The global allocator (`gvfs-alloc-guard`) is *sealed* after warmup and
   counts every allocation made inside a request (`gvfs_alloc_hot_path_total`).
   `--alloc-guard strict` aborts on one; the tests and benchmarks run in strict mode. A
   few rare paths are explicitly marked `cold_section` and may allocate: building a new
   prefetch plan and its `.idx` once per view, metrics text, and arena growth for commit
   closures.
4. **Few dependencies, in-house wire formats.** pkt-line, pack/idx/delta, the GVFS
   prefetch stream, HTTP/1.1 (client and server), JSON scanning, base64, the manifest, and
   the slab index are written here. Dependencies are only what would be unwise to rewrite:
   `zlib-rs` (SIMD inflate/deflate), `sha1` (SHA-NI), `ring` + `rustls` + `webpki-roots`
   (TLS, HMAC), `nix` (`sendfile`), `memmap2`. There is no async runtime, no CLI framework,
   and no serde.

Code style: plain structs, slices, and indices into arenas; explicit loops; no trait
hierarchies or generic plumbing.

## Data model

```text
store/
  manifest.bin                  the only mutable object (compare-and-swap)
  slabs/<tier>-<end_ts>-<sha16>.pack     a pack written by repack (src/repack.rs)
  slabs/<tier>-<end_ts>-<sha16>.mphf     its index
```

**Manifest** (`src/manifest.rs`): `generation`, the `tip` commit all sealed slabs reach,
`max_ts`, the `baseline`, up to `--keep-weeks` `weekly` deltas, and the `pyramid` (15m/1h/
6h/12h/24h slabs since the baseline, oldest first). Each slab entry records its tier,
`(start_ts, end_ts]`, the `(have, want]` commit range it was fetched for, its object count,
and its sizes and pack hash. The format is binary with a trailing SHA-256.

**Slab index** (`src/slab.rs`, `src/mphf.rs`): a 128-byte header, then a partitioned
PTHash minimal perfect hash (2048 keys per partition, about 4 keys per bucket, skewed
bucket assignment, `u16` pilots: about 1 byte per object), then one 40-byte record per
distinct object in MPHF slot order:

```text
[0..20)  object id
[20..28) entry offset (40 bits) | object type (3) | delta depth (8) | size high bits (8)
[28..32) entry length (header + zlib stream)
[32..36) object size, low 32 bits
[36..40) slot of the delta base, or NO_BASE
```

A lookup is one hash, one 40-byte read, and a full 20-byte compare. That costs 40.5 bytes
per object (373 MB for SearchGold's 9.22M objects, memory-mapped). In exchange:

* lookups are exact (no fingerprint false positives that would serve a wrong object for
  an unknown id);
* `/gvfs/sizes`, type checks, and delta-chain planning never inflate anything.

A 24-byte record (8-byte keyed fingerprint plus the same fields) would save about 150 MB
for SearchGold. The full id was kept: the saving is small next to the 2.4 GiB pack, and
it keeps answers exact.

Object ids are already uniform, so the MPHF needs no hashing beyond mixing id words.

## Primitive 1: `ingest_git_delta` (`src/ingest.rs`)

`ingest_git_delta(upstream, haves, want, tier, range, dest)` runs one protocol-v2 `fetch`
(`want` + `done`, a single round trip). It falls back to v0 for servers without v2. The
fetch always asks for a non-thin pack, so every delta base is inside the pack. The pack is
indexed and written as a slab, to a directory (pipeline) or kept in memory (L0). An
unchanged tip returns `None`.

The upstream is asked for every commit once: by the one-time seed, then by the 15-minute
increment (or the edge's L0 poll) that first sees it. Nothing is ever re-fetched to get a
better pack. What the upstream sends is raw material, and every slab that is served is
written by the optimal splice below.

The indexer is a faster `git index-pack`:

* **Pass 1, parallel speculative scan.** Entry boundaries are only known by inflating every
  stream, which is why git's first pass is sequential. Here the pack is cut into chunks
  (≥ 2 MiB, threads × 4). Each worker finds the first plausible entry in its chunk (valid
  header, valid zlib header, inflates to exactly the declared size, plausible next header)
  and decodes from there, hashing whole objects as it goes. A stitcher walks the true
  chain from offset 12 and adopts a chunk's results only from an offset it reached
  itself, so the output is identical to a sequential scan.
* **Pass 2, resolve families.** Deltas are linked to their bases. OFS deltas link by
  offset. REF deltas link through a sorted `(base id, entry)` waiter table with a 64K
  fanout, because ADO sends *only* REF_DELTA entries, chained up to depth 50. Workers take
  whole families from a shared counter and walk each depth first with one content buffer
  per depth.
* The index build drops duplicate entries (the first copy wins) and builds the MPHF
  partitions in parallel.

On SearchGold's 12.08M-object ADO pack this takes 7.0 s, against 107 s for
`git index-pack --threads=32`. Every id and offset was verified against git's `.idx`
(`ingest --verify-idx`).

## Primitive 2: splice

Splicing makes one pack out of many, two ways. On the wire, slab bodies are concatenated
as they are (`src/splice.rs`). Into a new slab, trees are re-deltified so the result is
as small as one upstream fetch of the whole range (`src/repack.rs`).

### On the wire (`src/splice.rs`)

A slab body (`pack[12..len-20]`) is self-contained: OFS distances are relative, and REF
bases are named by id. So:

* **Prefetch** concatenates whole slab bodies between a fresh 12-byte header (the summed
  entry count) and a SHA-1 trailer. Concatenation shifts every entry of a slab by the same
  constant, so OFS distances stay valid. Nothing is inflated, re-deflated, or dropped. The
  trailer is the only computed byte: each plan hashes it once, single-flight, and every
  client of that plan reuses it. A warmer thread precomputes the cold plan's trailer after
  each manifest change (0.9 s for 2.42 GiB). Bodies leave through `sendfile(2)`.
* **Object splice** (`POST /gvfs/objects`) takes the requested entries plus their delta
  base chains, in (slab, offset) order. Contiguous runs are copied verbatim. An OFS_DELTA
  whose base moved gets a re-encoded header in front of its untouched zlib stream. A delta
  whose base comes from another slab is re-headed as REF_DELTA.
* **Loose objects** (`GET /gvfs/objects/<id>`): for a whole entry, the loose object is
  assembled without recompression. It is a zlib header, a stored block holding
  `"<type> <size>\0"`, the entry's original deflate blocks, and an Adler-32 combined from
  the two parts. A deltified object is resolved, then deflated at level 1.

Duplicates across slabs (rare: an object re-introduced after the `have`) stay in
concatenated packs. Duplicate entries are valid in a pack and in its `.idx`.

### The optimal splice (`src/repack.rs`)

Every slab is fetched for its own commit range and must stand alone, because clients
index each pack by itself (`gvfs-helper` runs `index-pack` without `--fix-thin`). So each
slab stores whole the first version of every directory its range touched. Concatenating
a day of 15-minute slabs repeats the hot directories, such as the root tree, once per
slab. On SearchGold that is 4.3 MiB for a day that one fetch delivers in 800 KiB.
`repack` (`Repacker::repack(input, tier, range, commits, dest)`) gets that back without
the upstream:

1. **Objects:** the union of the input slabs, first copy wins. Objects of a `frozen`
   slab (the old baseline, copied byte for byte as the output's prefix) or of `exclude`
   slabs are left out.
2. **Paths:** the new commits are walked newest first, and each new tree gets the
   directory path (a 64-bit hash) it is first found at. The walk only descends into new
   trees: an unchanged subtree belongs to older history. Commits and trees are resolved
   once each into an arena; delta chains are memoized.
3. **Tree clusters:** the versions of one directory are written back to back, newest
   first. The newest is whole, or, when appending to `frozen`, a delta against the frozen
   slab's version, found by walking the frozen commits the new history builds on along
   changed paths only. Each older version is an OFS delta against its newer neighbour,
   and a chain starts over whole after depth 50 (git's default). Clusters are ordered by
   when their newest version was found, so the newest commit's directories come first, in
   depth-first order.
4. **Tree deltas** (`delta::tree_delta`) are git's delta format, computed the way trees
   allow. Entries are sorted by name, so one merge pass over both entry lists turns runs of
   identical entries into copy ops and inserts only changed or added entries. There is
   no rolling hash and no window search. A delta must be smaller than half the tree;
   otherwise the tree is stored whole.
5. **Everything else** (commits, `Git-hfs` pointer blobs, tags) keeps its compressed
   bytes. Deltas among them are re-pointed as OFS_DELTA (ADO sends REF_DELTA, 20 bytes of
   base id per entry), with their bases written first. An object whose base is not in the
   output is resolved and stored whole.
6. **Verification:** the output is indexed by the same indexer as an upstream pack,
   which recomputes every id from content, and it must hold exactly the expected objects.
   Repacking a repacked slab reproduces it byte for byte.

Output order: (frozen prefix), commits and tags, tree clusters, blobs. `Mode::Copy` skips
steps 2-4 for the seed: the whole-history pack from the upstream only gets its deltas
re-pointed. The upstream's tree deltas are already local to each directory, and
re-clustering all of history would buy little.

The cluster layout gives two properties: the newest version of every directory is one
whole entry (checkouts of recent commits resolve no deltas), and any older version is a
contiguous byte range from its cluster's head, which one HTTP range read could fetch.

Measured on SearchGold, one week (2026-10-17 to 10-23: 1,656 commits in 526 non-empty
15-minute windows), with `benches/rollup.sh`:

| | concatenated 15m packs | repacked rollup | one upstream fetch |
|---|---|---|---|
| busiest day (290 commits) | 4.32 MiB | 819 KiB | 800 KiB |
| each day | 2.2 - 4.4 MiB | 354 - 819 KiB | 323 - 800 KiB (rollup is 1-10% larger) |
| the week | 21.90 MiB | **2.63 MiB** | 2.65 MiB |
| the week appended to the 2.42 GiB baseline | | +2.22 MiB (4.3 s) | full fetch: +2.35 MiB |

A day stays 1-10% above one fetch for two reasons. The newest version of every directory
is stored whole, and git also deltas a directory's first version against a similar
directory at another path (per-locale data folders, for example). Over a week, chaining
each version to its neighbour wins that back. The 15-minute slabs themselves are within
2% of the upstream's packs. The seed's REF→OFS conversion shrinks SearchGold's 3.04 GiB
ADO pack by 90.7 MB (2.8%) in 9.9 s.

## The pipeline (`src/pipeline.rs`)

The pipeline runs as a scheduled ADO pipeline or GitHub Action. Runners are stateless:
each run reads the manifest, downloads the slabs it needs from the store, and publishes.
Each run is one compare-and-swap of `manifest.bin`:

1. **Genesis** (no manifest; the one-time seed): `ingest_git_delta(none, HEAD)`,
   repacked in copy mode (deltas re-pointed as OFS), becomes the baseline, published with
   `If-None-Match: *`.
2. **15m**: `ingest_git_delta(known tips, HEAD)`, repacked with trees re-deltified by
   directory, becomes a 15m slab. The haves are the manifest's tips, so a force push
   still negotiates.
3. **Rollup** to tier T: for each tier from 1h up to T, the run of slabs below that tier
   at the end of the pyramid is repacked into one slab of the tier, keeping the run's time
   and commit range. Rollups cascade, so every hour has a 1h slab and every six hours a 6h
   slab even when a higher tier is due. The slabs a rollup replaces stay listed as
   `retained`: 15m slabs for a day, 1h/6h/12h for a week, dailies for the warm window
   (`--keep-weeks`).
4. **Weekly** (`168h`): the pyramid is repacked into the week's delta slab (kept for warm
   clients; the pyramid's slabs are retained). The new baseline is the old one, byte for
   byte, plus the delta's objects, with each directory's newest version stored as a delta
   against the old baseline's (`frozen`). Old index records keep their offsets, and one
   MPHF is built over all ids.
5. Publish with `If-Match` (a lost race starts over, up to 3 times), then delete slab
   files no manifest references once they are older than `--gc-grace-secs`.

Crash recovery needs nothing: unpublished uploads are garbage, and the next run's range
starts at the manifest tip. The fs store implements CAS with `flock`; Azure uses ETags.

**Timestamps come from commits** (`src/timeline.rs`). A slab covers `(start_ts, end_ts]`:
the stamps of the commits it was fetched between. Commits are stamped like a Lamport clock
seeded with committer times, `max(committer time, 1 + the largest stamp of its parents)`,
so a commit is stamped above all of its ancestors. That includes a merge made by
`git pull`, whose first parent is a local commit and whose second is the previous tip. A
parent the slab does not hold is looked up among stamps both sides know: the manifest's
boundaries (every slab's tip) and, on an edge, the commits of its unsealed L0 slabs, which
the pipeline will fetch and stamp the same way. Any other parent is older sealed history
and counts as 0. The pipeline and every edge therefore give a commit the same stamp however
history was cut into slabs, and an L0 slab and the 15m slab later sealed at the same commit
end at the same stamp. The seed is stamped with its tip's committer time, and no stamp is
below the previous boundary + 1, which only matters after a force push. A client's cursor
(`lastPackTimestamp`, the newest stamp it was sent) is therefore a boundary every server
knows, whichever edge sent it.

The two sides can still disagree in two cases:

* A force push.
* An edge still on the previous manifest stamps a push that merges a branch forked inside
  the range the pipeline just sealed, and the commit clocks are so close that the `+1`s
  decide.

An edge whose L0 tip is sealed with a different stamp drops its L0 slabs and fetches them
again. A client holding the edge's stamp is answered from the closest boundary at or before
it, which is exact unless the next boundary falls between the two stamps, seconds apart.
Prefetch is an optimization: whatever a client lacks it fetches on demand from
`/gvfs/objects`.

## The server (`src/serve/`)

**Boot**, which is also crash recovery: read the manifest, then open its slabs (retained
ones included) in place (fs store) or download them into `--cache-dir` (Azure), and map
them. With `--upstream`, rebuild the live tip as an L0 slab. Then allocate all buffers and
arenas, seal the allocator, and listen.

**The live tip (L0).** The pipeline seals a slab every 15 minutes; L0 bridges the minutes
since the last one, with one heap slab per move of the branch. The live loop runs every
`--poll-ms`:

* A moved branch: `ingest_git_delta(newest boundary, HEAD)` fetches only the new commits,
  repacked into one more L0 slab and stamped from its commits. This is the same code the
  pipeline runs, and the pipeline later stamps the same tip the same way.
* A new manifest generation swaps in its slabs. L0 slabs it covers leave the live set
  (object lookups and cold plans) but stay in the timeline for 6 hours, so a client whose
  cursor is one of their boundaries is still answered exactly. If the manifest's tip falls
  inside an L0 slab, what follows the tip is repacked out of it. Nothing is fetched again
  unless the branch was rewritten.

Every change publishes a new immutable `View`; requests hold an `Arc` to the view they
started with. Object lookups go through the live L0 slabs, the pyramid, the weekly deltas
and the baseline, newest first; retained slabs hold nothing those do not.

**Prefetch plans.** Every response is one pack holding exactly what the client lacks:

* The timeline is every slab a plan may use: weekly deltas, pyramid, retained slabs and L0
  slabs. Rollups keep their parts' bounds, so boundaries nest like a segment tree.
* **Warm** (a cursor at or after the oldest weekly delta's start, or the baseline's end):
  the exact cover from the newest boundary at or before the cursor to the tip. At each
  step it takes the slab starting exactly there that reaches furthest
  (`timeline::cover`); where none starts there, the narrowest slab containing the
  position. The baseline is never part of it.
* **Cold** (no cursor, or an older one): the baseline plus the cover from its end.
* A cover of several slabs is repacked into one, so the response is about the size of one
  upstream fetch of exactly that range.
* An up-to-date client (cursor at or past the tip's stamp) gets an empty `GPRE` stream.

Plans are keyed by boundary: at most one per slab start per view, each built once (in a
`cold_section`, since it allocates) and shared. The warmer thread prepares the cold plan,
and the plan for the previous view's tip, which is what up-to-date clients ask for next.

**Warm traffic.** `examples/clients.rs` replays 14 days of SearchGold through the pipeline
and the edge on a simulated clock (README, "Client traffic"). Every client pattern, from
an agent prefetching every minute to a developer back after 8 days off, gets within 2% of
the ideal: a self-contained pack of exactly what it lacks, as one upstream fetch would
make it. Cold clones are 0.04 MiB over a fresh full fetch. Three changes got there from
2.2-2.5×:

* *Rollups used to replace finer slabs*, so a client whose cursor fell inside an hour, day
  or week was sent the whole rollup again. Replaced slabs now stay listed (`retained`),
  and rollups cascade so every level has a slab at every boundary below it. Without the
  cascade, the hour before each 6h/12h/24h boundary had no 1h slab: once its 15m slabs
  aged out, a developer back on Monday got all of Friday again (2.1× for that fetch).
* *L0 used to be re-sent*: whole after every change, then once more inside the sealed 15m
  slab, because it was stamped with the edge's clock. Commit-derived stamps make an L0
  boundary and the sealed slab that later ends there identical, and one L0 slab per push
  lets a poller take only the new pushes.
* *A cover of several slabs repeated the hot directories* (each slab stores its first
  versions whole). Repacking the cover into one slab per plan removes that. Plans are
  built once per boundary per view and shared. On SearchGold an hour of commits (4 slabs)
  builds in under 5 ms, a day (9 slabs, 1.09 MiB to 581 KiB) in 40 ms, and the whole
  pyramid since the last weekly (14 slabs, 3.96 MiB to 2.40 MiB) in 0.15 s.

**The `.idx` (`src/idx.rs`).** VFS for Git installs a server-sent index as is
(`gvfs.trust-pack-indexes`, default true) instead of running `index-pack`. gvfs-helper
always indexes packs itself. So with `--prefetch-idx auto` (the default) every client
except `User-Agent: git/...` gets the pack's `.idx` (v2). It is built once per plan and
shared, like the trailer:

* each slab sorts its ids and computes its entries' CRC-32s once (`SortedSlab`, kept with
  the open slab; 74 MB for SearchGold's baseline);
* a plan's index is one k-way merge of its slabs' sorted runs, with each slab's offsets
  shifted to where its body lands, then the 64-bit offset table, the pack checksum (the
  plan's trailer), and the index checksum.

The result is byte-identical to `git index-pack`'s output: verified on every prefetch in
the tests, and on SearchGold's 2.6 GB baseline (9.22M objects, 2.65M 64-bit offsets). That
index is 267 MiB, built in 4.3 s; `index-pack` on the client takes 40 s and 141 CPU-s. The
warmer prepares it for each new cold plan once any client has asked for one.

**Endpoints.**

* `GET /gvfs/config`.
* `GET /gvfs/prefetch`: one pack, plus its `.idx` for clients other than git.
* `GET /gvfs/objects/<id>`: a loose object. gvfs-helper requires loose for GET.
* `POST /gvfs/objects`: one pack. A single non-commit with a loose `Accept` is answered
  loose. A commit expands to `commitDepth` levels of parents plus every tree of each.
* `POST /gvfs/sizes`.
* `/healthz` and `/metrics`.

Any path prefix before `/gvfs/` is accepted.

**Threads and memory.** A fixed set of worker threads (`--workers`, 256) waits on one epoll
set that holds the listening socket (backlog raised to `net.core.somaxconn`; std's 128
overflows when a fleet connects at once) and every idle keep-alive connection
(`src/serve/conns.rs`). A worker keeps serving a connection while its requests keep coming
(it waits up to `--linger-ms`, 5, for the next one, so a client sending requests back to back
pays no epoll round trip per request), then parks it: an idle client costs a table slot
(`--max-idle-conns`, 65,536), not a thread, and is closed after `--idle-secs` (30) without a
request. The linger is short because every sporadic request (an agent polling once a minute)
holds its worker for the whole linger: at 100 ms, 256 workers absorb only ~2,500 such requests
a second before they queue (8,000/s over 2,048 connections: p50 145 ms, p99 2.5 s; at 5 ms:
16 µs and 23 µs). While
every worker is busy, a worker also parks its connection between requests once it has held it
for `--hold-ms` (5), so busy connections beyond the worker count take turns instead of waiting
for others to go idle. A response in progress holds its worker until it is written (blocking
writes, 120 s timeout each). Each worker has a 16 KiB input buffer and a 256 KiB output
buffer. POST bodies and their working sets live in a leased arena (default: 2 ×
cores arenas of 8 MiB and 2^18 ids). If all arenas are busy for 10 s, the request gets a
503. A commit closure larger than its arena grows it in a `cold_section`; `reset` shrinks
it back. The server speaks plain HTTP/1.1: put TLS and HTTP/2 on the load balancer in
front of an edge. The localhost proxy needs neither.

## Departures from the PDF

| PDF | Here | Why |
|---|---|---|
| Undeltify every slab entry; write objects in "Merkle DFS" order | Slabs keep deltas. Trees are re-deltified into per-directory clusters (newest whole, older versions chained); other objects keep their compressed bytes | SearchGold's 7.54M trees are 45.1 GiB raw but 2.42 GiB deltified. zlib alone cannot shrink trees, which are mostly 20-byte ids, so undeltified slabs would be about 10× larger. |
| Rollups splice and drop duplicate ids and "superseded intermediate trees" (`plan_deduplicated_range_splice`) | Rollups are optimal splices from the store (`repack`): the union of the slabs with trees re-deltified across them. Nothing reachable is dropped | Intermediate trees are what intermediate commits point to; dropping them breaks history. Plain splicing is 8× larger than one fetch (21.9 vs 2.65 MiB for a SearchGold week); the repacked week is 2.63 MiB. |
| Tree deltas only in 24h/168h slabs, in clusters capped at depth 15 with every member a delta against the cluster head (later advice) | Clusters at every tier, each version a delta against its newer neighbour, depth up to 50 | Deltas against the head grow with every change since the head; deltas against the neighbour stay one change wide. A depth of 50 needs fewer whole copies, and an older version is still one contiguous range from its cluster's head. Small intraday slabs repack in milliseconds. |
| 8-byte packed descriptors with a 16-bit fingerprint | 40-byte records with the full id | Exact answers (404 for unknown ids, never a wrong object). Sizes, types, and delta bases come without inflation. 24-byte records would save about 150 MB on SearchGold. |
| Edge range-reads slabs from S3/Blob (coalesced HTTP `Range`) | The edge hydrates slabs to local disk (`--cache-dir`) and maps them | Local reads cost microseconds: a deltified tree GET is 190 µs p50. The edge restarts from the store at any time. The cluster layout keeps range reads possible behind the same `Slab` type. |
| Pack timestamps are creation times; warm clients get every slab newer than their cursor; rollups replace the slabs they fold | Timestamps are derived from commits; warm clients get the exact cover from their cursor (retained finer slabs, cascading rollups), repacked into one pack | An edge's L0 slab and the sealed slab that later ends at the same commit get the same stamp, so nothing is sent twice. Measured over 14 days of SearchGold, every client pattern gets within 2% of one upstream fetch of exactly what it lacks, against 2.2-2.5× before. |
| Tokio/Axum/Hyper, `DashMap` memtable for L0 | std threads, in-house HTTP/1.1, `sendfile`; L0 = heap slabs (one per push) in the same format as sealed slabs | Fewer dependencies (rule 4) and zero hot-path allocation (rule 3). One code path serves every slab. |
| Parse every `Git-hfs 1.0` pointer during ingest; optional `.hfs` sidecar (content id + real size) | Ingest does not parse pointers. `inspect slab` uses the zero-allocation parser to report pointer coverage | No GVFS endpoint consumes real content sizes (`/gvfs/sizes` returns git object sizes), so the sidecar would be unused work on the critical path. |

## Protocol facts the implementation relies on

* `gvfs-helper` computes its prefetch cursor from the newest `prefetch-<ts>-*.pack`
  filename, in seconds; it indexes every received pack itself, discarding a server
  `.idx`; it accepts packs only from `POST`; and it identifies as `git/<version>`.
* VFS for Git (`GitObjects.cs`) writes a server-sent `.idx` next to the pack when
  `gvfs.trust-pack-indexes` is true (the default) and identifies as
  `<process name>/<version>`. Both clients send
  `Accept: application/x-gvfs-timestamped-packfiles-indexes`.
* git requires an index to list every pack entry (`num_objects` must equal the pack
  header's count); duplicate ids are allowed outside `--strict`.
* Git protocol v2 `fetch` with `done` returns no acknowledgments section.
* ADO packs use only REF_DELTA. Copilot-style agent shells may inject `GIT_CONFIG_*`
  environment variables; local `git upload-pack` children get them stripped.

## Not done

* Live Azure was not exercised. The store is tested against the Azurite emulator, and
  `tests/azure.rs` can target a real account.
* Serving TLS/HTTP2 directly, and range-read (non-hydrating) slab access.
* Reachability GC of the baseline: it only grows by appends, so objects orphaned by a
  force push stay in it.
* CAS pre-warming from pointer contents (the PDF's optional sidecar).
