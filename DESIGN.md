# gvfs-cache design

This is how the *Unified Binary Architecture* from the design PDF is built, what the code
guarantees, and where it departs from the PDF (with the measurements that motivated each
change). The README covers usage and benchmarks.

## Engineering rules

These rules come from the design of this daemon's predecessor (`opus/DESIGN.md` in the
gvfs-server repository), and they hold for the whole workspace:

1. **Safe Rust.** The main crate inherits `unsafe_code = "forbid"` from the workspace.
   Three operations cannot be written safely: a `GlobalAlloc` implementation, `mmap(2)`,
   and a cache prefetch hint. On x86-64 the hint is `_mm_prefetch`, a
   `#[target_feature(enable = "sse")]` function, so calling it from code that does not
   itself list `sse` is `unsafe`, although every x86-64 CPU has SSE. On ARM64 it is a
   `prfm pldl1keep` instruction, written with `asm!`. Each operation lives in its own tiny
   crate (`crates/gvfs-alloc-guard`, `crates/gvfs-mmap`, `crates/gvfs-prefetch`) with
   `undocumented_unsafe_blocks` denied.
2. **No panics.** `unwrap_used`, `expect_used`, `panic`, `indexing_slicing`,
   `arithmetic_side_effects`, `todo`, `unimplemented`, and `unreachable` are denied. Slices
   are read with `.get()` and slice patterns; arithmetic is checked, saturating, or
   wrapping where wrapping is the intent. (Tests may unwrap: `clippy.toml`.)
3. **No heap allocation while serving.** Everything a request touches is allocated before
   the server starts: a pool of connection buffers and a pool of request arenas
   (`src/arena.rs`). The global allocator (`gvfs-alloc-guard`) is *sealed* after warmup and
   counts every allocation made inside a request (`gvfs_alloc_hot_path_total`): the
   hot-path scope is entered for every poll of a connection's task. `--alloc-guard strict`
   aborts on one; the tests and benchmarks run in strict mode. A few rare paths are
   explicitly marked `cold_section` and may allocate: building a new prefetch plan and its
   `.idx` once per view, metrics text, arena growth for commit closures, fetches from the
   upstream or object storage, and tokio's own bookkeeping when a connection opens or
   closes (never per request).
4. **Few dependencies, in-house wire formats.** pkt-line, pack/idx/delta, the GVFS
   prefetch stream, HTTP/1.1 (client and server), JSON scanning, base64, the manifest, and
   the slab index are written here. Dependencies are only what would be unwise to rewrite:
   `zlib-rs` (SIMD inflate/deflate), `sha1` (the CPU's SHA instructions, found at run
   time: SHA-NI on x86-64, the SHA extension on ARMv8), `ring` + `rustls` + `webpki-roots`
   (TLS, HMAC), `nix` (`sendfile`), `memmap2`, and `tokio` (the edge's runtime: sockets,
   timers, its scheduler, child processes). There is no CLI framework and no serde. The
   edge's HTTP/1.1 server stays in-house on tokio's sockets: hyper's server copies every
   body (no `sendfile`) and allocates per request (REPORT.md §14). Its fetches while it
   serves (object storage, the blob proxy's upstream) use hyper's client with
   `tokio-rustls` (`src/aclient.rs`), and so does every request to the upstream: the
   pipeline, the mirror and the live loop block on theirs (`Upstream` runs them on a
   runtime of its own). Their object-store calls keep the blocking in-house client.

Code style: plain structs, slices, and indices into arenas; explicit loops; no trait
hierarchies or generic plumbing.

## Data model

```text
store/
  manifest.bin                  the one object readers follow (compare-and-swap)
  slabs/<tier>-<end_ts>-<sha16>.pack     a pack written by repack (src/repack.rs)
  slabs/<tier>-<end_ts>-<sha16>.mphf     its index
  slabs/base-<end_ts>-<sha16>.tail       a weekly baseline's bytes after the one before it
  gc.bin                        the pipeline's own: files no manifest lists, and since when
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

Object ids are already uniform, so the MPHF needs no hashing beyond mixing id words with a
seed the index records. The seed is a constant for any ordinary set of ids. Whoever can
push to the branch can make objects whose ids collide under a seed it knows, so when the
constants all fail (they never do otherwise) the seed is a digest of the ids themselves,
which cannot be aimed at.

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
  whole families from a shared counter and walk each depth first. A worker holds an
  object's content only while the object has deltas left to resolve, so a chain of any
  depth needs two buffers, and it frees what a family of large objects needed (beyond
  4 MiB) when the family is done. On SearchGold's baseline (9.25M objects, trees up to
  5.3 MB in chains 50 deep) the indexer holds 2.1 GiB of anonymous memory at 32 threads
  and 1.7 GiB at one, nearly all of it tables that grow with the object count; with a
  buffer per depth, kept, it held 8.1 GiB at 32 threads. An id that a pack holds more
  than once (git accepts that) has its REF deltas resolved once, by the first object
  with that id to be resolved, so the walk ends even where a delta makes its own base's
  id again. A buffer is sized for a delta's result by what the delta's ops write, never
  by the size its header declares, and a delta, its base or its result of more than
  1 GiB fails the pack, as it does in a blob pack.
* The index build drops duplicate entries (of an id's copies, the one with the shortest
  delta chain is kept, the first of them in the pack) and builds the MPHF partitions in
  parallel.

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
  the two parts. A deltified object is resolved, then deflated at level 1. Deflating is
  the larger part of such a request and buys little for a tree, which is mostly ids:
  `--loose-deltas stored-trees` sends rebuilt trees as they are, in stored blocks written
  by hand (zlib at level 0 still clears its 128 KB hash table per stream), and `stored`
  everything, for clients on the same host. Per request on SearchGold, CPU and answer:

  | | deflate (default) | stored |
  |---|---|---|
  | Deltified tree | 101 µs, 5.8 KB | 55 µs (−45%), 9.8 KB |
  | Any tree | 68 µs, 4.0 KB | 39 µs (−43%), 6.4 KB |
  | Deltified blob (text) | 117 µs, 3.6 KB | 95 µs (−19%), 56 KB |

  A stored tree costs 4 KB more to save 46 µs: it pays on links faster than about
  0.7 Gbit/s per core, a stored blob never over a network.

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
Each run is at most one compare-and-swap of `manifest.bin`:

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
   MPHF is built over all ids. A rebuild waits for its week: asked for before the last
   week's end is six days old by the run's clock (a job retried, a trigger that fired
   twice), the run does the lower tiers only. Each rebuild made a week of whatever the
   pyramid held, rewrote the baseline and dropped the oldest week beyond `--keep-weeks`:
   two such runs with `--keep-weeks 2` left two weeks of three objects each, and every
   client older than that was sent the cold pack. `--weekly always` rebuilds whenever
   asked.
   The new baseline's pack is the old one's with another count in its header, the week's
   entries after the old ones, and a new trailer; the run publishes those entries and the
   trailer beside it as its tail (`slabs/<id>.tail`, `src/tail.rs`: the old baseline's id,
   length and trailer, then the bytes). An edge of an object store that has the old pack
   makes the new one of the two, locally, and checks it as it checks a pack it downloads
   (its SHA-1 against the manifest's, which proves it byte for byte); if that fails, or
   it lacks the old pack, or the store has no tail, it downloads the pack. On a 512 MiB
   baseline in Azurite, a week of 2 MiB cost an edge 516 MiB before and 4.0 MiB after
   (the tail, the week's delta slab and the indexes); SearchGold's baseline is 2.6 GB a
   week, for every edge and every agent VM's proxy. The index is still downloaded
   (375 MB there). `gvfs_baselines_grown_total` counts the baselines an edge made so.
5. Publish with `If-Match` (a lost race starts over, up to 3 times), then collect
   garbage (below). A run that changed nothing (no new commits, no rollup due) publishes
   nothing: the manifest keeps its generation, so no edge builds a new view and hashes
   its cold pack again (1.3 s on SearchGold, its `.idx` beside it: the runs of a quiet
   branch would cost that 96 times a day). What counts as a change is everything but the
   generation, the `blobs.bin` generation the manifest names included. Such a run still
   collects garbage. Once its manifest is the store's, a run has succeeded: garbage
   collection that fails is logged, with the first file it could not delete (it goes on
   to the others) or the listing it could not make, and the run exits 0 (after an error
   exit a scheduler runs it again, a weekly a second time); a later run collects what
   it left.

Crash recovery needs nothing: unpublished uploads are garbage, and the next run's range
starts at the manifest tip. The fs store implements CAS with `flock`; Azure uses ETags.

**Garbage collection** (`src/gc.rs`). Runs take files out of the manifest: a retained
slab that has aged out on most runs, the old baseline every week. Readers of the
manifest before may still be fetching them: an edge that is hydrating, a run that
started from it. And a run that died left uploads that no manifest ever listed. Such a
file goes a grace period (`--gc-grace-secs`, default 2 h) after a run first found it
unlisted. Each run ends with a pass:

* It lists the store. A file that no manifest lists and that `gc.bin` does not have is
  noted there, with the run's clock and the file's version in the listing (an object
  store's ETag; in a directory the file's inode, length and time of last write). It
  stays.
* A file noted at least a grace period ago is deleted, if it is still the file that was
  noted: the listing gives it the noted version, and the delete is conditional on that
  version (Azure: `If-Match`, a 412 leaves the blob; a directory: the file is opened,
  looked at again and unlinked under the lock that uploads are renamed into place
  under, so far as the file system's locks reach).
* A file that is listed again, or gone, loses its note. One uploaded again since it was
  noted is another file, noted anew: its grace period starts over.

So a file goes with the first pass at least a grace period after the pass that first
saw it unlisted: with a run every 15 minutes, 2 h to 2 h 15 min after the manifest that
dropped it was replaced, and for a crashed run's upload that long after the next run.
Until then a reader of that manifest finds every file it lists. (The grace period was
the file's age since it was written. What a run dropped was older, and was deleted by
that run, seconds after the manifest that listed it was replaced: an edge still
hydrating did not start, and a run that had read that manifest failed.)

The grace period is also what a run has from an upload to the manifest that lists it
(a mirror, from a blob pack to its `blobs.bin`), counted from the first pass that sees
the upload, and it covers the seconds by which the manifest a pass goes by, the one its
run read or published, may be behind the store's: it must be longer than a run takes.
A pass's clock is read when it runs, not when its run started (`--now` fixes it for the
run); a note whose clock is ahead of the pass's counts from the pass.

The versions close a race between two runs. A rollup that a run uploaded before it died
is in the store, unlisted, and noted. A later run that makes the same rollup makes the
same id (ids are content-addressed), uploads it and publishes it. Another run's pass
meanwhile either listed the files before that upload and deletes after it, or finds
them, due, between the upload and the publication. An upload always makes a new object,
also where the key has one (a directory store renames the new file over the old, which
readers that have it mapped keep), so in the first case the delete is refused and in
the second the note is not this file's. (An upload to a directory left a file of that
name as it was, and a pass deleted by name: the slab was deleted, the manifest listed
it, and both runs had exited 0.)

`gc.bin` is `"GVDGCOL1" | version u32 | count u32 | (since u64 | key_len u16 | key |
version_len u16 | version)* | SHA-256`, about 100 bytes a file, for the files that are
unlisted and not yet deleted (`gvfs-cache inspect manifest` says how many). A pass costs
one read of it, one listing (two for a store with separate blobs), the deletes, and one
write if it is to hold something else: a compare-and-swap. A pass that loses the swap to
another run's, or cannot write, has deleted only what was due, and what it noted the
next pass notes: later, never sooner. A pass that cannot read `gc.bin` deletes nothing;
a `gc.bin` that does not decode counts as absent, and what is unlisted is noted anew. A
store from before `gc.bin` gets one at its first pass, which deletes nothing. Edges
never read it.

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

A force push the pipeline never saw (pushed and replaced between two runs) would make
them disagree: the edge's L0 slabs hold commits the branch no longer has, and what follows
can be stamped below the stamps they handed out (committer times are not monotonic: a
commit made before the abandoned one, pushed after it). The edge therefore checks each
move of the branch: a push descends from the newest live L0 tip. When one does not, the
edge drops the live L0 slabs the new head does not reach (their objects stop being
served), and fetches and stamps what the head has after the newest boundary it still
reaches, exactly as the next pipeline run will. The boundaries of the dropped slabs are
*retracted*: a client whose cursor is one of them holds commits the branch no longer has
and may lack ones stamped below its cursor, so it is answered from the boundary before
them (the edge remembers the last 1,024 such ranges; a cursor that is a boundary of the
current timeline is always taken as it is).

The two sides can still disagree when an edge still on the previous manifest stamps a push
that merges a branch forked inside the range the pipeline just sealed, and the commit
clocks are so close that the `+1`s decide. An edge whose L0 tip is sealed with a different
stamp, or whose live L0 slabs are not on the branch a new manifest has (a force push the
pipeline sealed before the edge saw it), drops its L0 slabs, retracts their boundaries,
and fetches again. A cursor the edge did not retract (another edge's, or from before a
restart) is answered from the closest boundary at or before it, which is exact unless the
next boundary falls between the two stamps. Prefetch is an optimization: whatever a client
lacks it fetches on demand from `/gvfs/objects`.

A commit dated in the future (a machine with a wrong clock) puts every stamp after it
ahead of the clock, one apart per commit, until the clock passes its date. The stamps
stay what they are: both sides must give a commit the same one, and neither knows the
other's clock. What goes by distances between stamps is bounded instead. The slabs kept
for warm clients, in the manifest and of an edge's L0, age against the clock when the
newest stamp is ahead of it (`min(newest stamp, now)`), and are bounded in number: four
times what a tier's retention holds at its own period in the manifest (384 15m slabs, 672
of an hour), 4,096 sealed L0 slabs on an edge. (By stamps alone, a commit dated two days
ahead dropped every retained slab in the run that sealed it, and one dated ten years
ahead then kept every slab made for ten years: 96 more a day in the manifest, and on
every edge.) The pipeline says so at each run while the newest stamp is more than a day
ahead.

## The server (`src/serve/`)

**Boot**, which is also crash recovery: read the manifest, then open its slabs (retained
ones included) in place (fs store) or download them into `--cache-dir` (Azure), and map
them. A slab's files are the slab only if they are the ones the manifest lists: the pack's
length and trailer, the index's length and the object count are compared with its entry
(a manifest names a slab by 8 bytes of its pack's SHA-1), and a pack that was just
downloaded is hashed, so that what an edge serves is the pack whose SHA-1 the pipeline
published (SearchGold's baseline: 2.4 GiB, about a second). Cached files that are not
the slab, or a sizes table that fails its checksum, are downloaded again, once: an edge
does not need its cache wiped by hand to start. An edge of an object store must be given
`--cache-dir`, a directory on local disk:
it holds the slabs (gigabytes) and the caches of blob packs, and the system's temp
directory, otherwise the default, is memory on many hosts (a memory filesystem is warned
about at startup). Whenever a manifest is applied, at boot too, the files of slabs it does
not list are deleted (a request on an older view keeps what it mapped), as are segment
indexes when `blobs.bin` changes, and downloads that died with their process: files named
as the edge names its downloads, and no others the directory may hold. With `--upstream`, rebuild the live tip as an L0 slab. Then allocate all buffers and
arenas, seal the allocator, and listen.

**The live tip (L0).** The pipeline seals a slab every 15 minutes; L0 bridges the minutes
since the last one, with one memory slab per move of the branch (its index on the heap,
its pack in a memfd once it is 16 KiB, so that it too is sent with `sendfile(2)`). Two
threads poll every `--poll-ms`, one the upstream and one the store:

* A moved branch: `ingest_git_delta(newest boundary, HEAD)` fetches only the new commits,
  repacked into one more L0 slab and stamped from its commits. This is the same code the
  pipeline runs, and the pipeline later stamps the same tip the same way. A forced push
  (the new head does not descend from the newest live L0 tip) drops the live L0 slabs the
  head does not reach and fetches again from the newest boundary it does (above).
* A new manifest generation swaps in its slabs. L0 slabs it covers leave the live set
  (object lookups and cold plans) but stay in the timeline for 6 hours, so a client whose
  cursor is one of their boundaries is still answered exactly. If the manifest's tip falls
  inside an L0 slab, what follows the tip is repacked out of it. Nothing is fetched again
  unless the branch was rewritten.

The store's thread reads `blobs.bin` and the manifest, downloads and opens what they list,
and hands it to the upstream's, which prunes its L0 slabs against the new slabs and makes
the view. A poll that finds nothing new reads nothing: in object storage it is two
requests with `If-None-Match` on a connection the thread keeps, each answered `304`
without a body; in a directory, each file is opened and looked at (device, inode, length,
times) and read only if it is not the file that was read, or was written in the last two
seconds. (Each poll was two GETs of the whole objects, each on a new TLS connection:
`blobs.bin` is about 120 bytes per blob pack, megabytes every `--poll-ms` for a store of
large blobs.) Neither thread waits for the other: while a weekly baseline downloads
(gigabytes), or a store does not answer, or a manifest cannot be opened, the edge
follows the branch on the manifest it serves; while a long fetch from the upstream runs,
the next manifest is downloaded.
A step that fails changes nothing: the slabs, the L0 slabs and what the tip
knows of them (the stamps of their commits, the boundaries a forced push took away) are
replaced together once everything that can fail has not, so a later view is never made
of one manifest's bounds and another's slabs. What failed is tried again after one poll
interval, then after twice as long each time, up to a minute, since an attempt fetches
the delta or downloads the slabs again; another head, or another manifest, is tried at
once. `gvfs_live_failures_total` counts the failures. A manifest whose generation is not
above the one served is not an older state of the store, whose generations only grow,
but a store made anew (or put back from a copy): the edge follows it, as a restarted edge
would, once it has stayed the store's manifest for 30 polls, which a read that returned
an older manifest for a moment does not. Ignored, it left the edge serving the store
that was gone, its L0 slabs growing.

Every change publishes a new immutable `View`; requests hold an `Arc` to the view they
started with. A view that differs from the last only in its `blobs.bin` (a running mirror
publishes one after another) keeps that view's slabs, filter and plans: its cold pack is
the same pack, and is not hashed again (`gvfs_prefetch_plans_total` counts the packs
that are). Object lookups go through the live L0 slabs, the pyramid, the weekly deltas
and the baseline, newest first; retained slabs hold nothing those do not. Most ids clients
ask for are only in the baseline, which comes last, so each view carries a blocked Bloom
filter of the ids of every slab before it (`src/idfilter.rs`, 16 bits per id, one cache
line per check): an id it rules out goes straight to the baseline. Each of those slabs has
its own filter too, for the ids that pass. Filters have no false negatives, so lookups
answer exactly as without them; they are built off the request path when a view is made
(once per slab). On a week-old SearchGold store (12 live slabs) they took 4,000-id sizes
requests from 6,049 to 11,190 a second and blob batches from 2,112 to 3,155. Batched lookups
(POST, sizes) also have the CPU start loading the record of the id 16 places ahead
(`gvfs-prefetch`; a hint, so answers cannot change), so a batch's cache misses overlap
instead of following one another: sizes 1.8-1.9× again, blob batches 1.1-1.9×. Plain loads
in its place made them 4-11% slower: a load the core must wait for stalls it. Asking for
the record means computing its slot, and the lookup, 16 ids later, computed it again (a
fifth of a sizes request's CPU): the loop keeps the 16 slots it has computed, and a
lookup that only the baseline can answer reads its record there. Sizes of 4,000 ids on
SearchGold: 726 to 634 µs of CPU (−13%).

A lookup reads one record at random in an index of hundreds of megabytes: on 4 KiB pages
that is a TLB miss besides the cache miss (51 ns a lookup against 34 on 2 MiB pages).
Indexes are mapped from their files, and the kernel maps a file 2 MiB at a time only
where the page cache happens to hold it so, which depends on how fragmented memory was
when the file was written: 88% of SearchGold's baseline index one day, none of it
another, none of RepoB's. `--index-pages huge` reads each index of 2 MiB or more
into anonymous memory that asks for huge pages (`MADV_HUGEPAGE`; no `unsafe`), which the
kernel gives wherever it has them: 99% of SearchGold's 358 MiB here, and sizes of 4,000
blobs went from 590 to 524 µs of CPU (−11%), on RepoB from 593 to 549 µs
(−7%); batches of objects, which copy more than they look up, did not change. The price
is that the indexes are the edge's own memory, not the page cache's: not shared with
another process, not evictable, and read once more at each start. It is off by default.
Either way the edge says at start how much of its indexes lies on 2 MiB pages, and
reports it (`gvfs_index_bytes`, `gvfs_index_huge_bytes`, `gvfs_index_copied_bytes`).

**Prefetch plans.** Every response is one pack holding what the client lacks (and, once
per sealed day and week, that rollup whole):

* The timeline is every slab a plan may use: weekly deltas, pyramid, retained slabs and L0
  slabs. Rollups keep their parts' bounds, so boundaries nest like a segment tree.
* **Warm** (a cursor at or after the oldest weekly delta's start, or the baseline's end):
  the exact cover from the newest boundary at or before the cursor to the tip. At each
  step it takes the slab starting exactly there that reaches furthest
  (`timeline::cover`); where none starts there, the narrowest slab containing the
  position. The baseline is never part of it.
* **Cold** (no cursor, or an older one): the baseline plus the cover from its end.
* **Superset deliveries** (`--superset-tier`, default `24h`). Once the tip has moved past
  a sealed daily rollup or weekly delta, a client whose cursor lies inside it, or exactly
  at its end, is answered from the rollup's start instead (`timeline::superset_start`, the
  widest such rollup). It receives the rollup whole plus what follows, once, since its
  cursor then lies past the rollup. Every pack it got inside that window is then
  redundant, and that is what stock client maintenance needs: `git multi-pack-index
  expire` deletes packs whose objects all have newer copies. With exact covers alone a
  client keeps one pack per prefetch that brought news. Scalar's default
  `incremental-repack` (batch size: the second-largest pack + 1) merges only a few of them
  a day: over a replayed SearchGold week an agent prefetching every minute kept 1,280 of
  its 1,405 packs, on its way to ~5,000 a month, where git pays ~3 ms for every lookup of
  an absent object (it rescans the pack directory; a multi-pack-index does not help). With
  superset deliveries it stays under two days of packs. VFS for Git's fixed 2 GB batch
  and Scalar's `geometric` strategy keep up either way. The price is one daily rollup per
  client per active day and one weekly delta per week (0.35-0.82 MiB and 1.9 MiB on
  SearchGold): 1.11-1.53× the ideal instead of 1.00-1.02×. `168h` delivers weekly deltas
  only (1.04-1.31×, packs bounded by a week's worth); `never` keeps exact covers.
* A cover of several slabs is repacked into one, so the response is about the size of one
  upstream fetch of exactly that range. The repacked pack is kept in a memfd (when it is
  16 KiB or more), so it leaves through `sendfile(2)` as a stored slab does. Written from
  the heap it cost 2.6 times the CPU: 392 µs against 151 µs for a 739 KB pack (SearchGold,
  a client one day behind), over half of it the kernel zeroing the socket pages the bytes
  were then copied into.
* Every response names the interval it covers (`X-GVFS-Pack-Start`/`X-GVFS-Pack-End`),
  so an interval hook (fable's `client-prefetch`) can unlink the local packs it subsumes.
* An up-to-date client (cursor at or past the tip's stamp) gets an empty `GPRE` stream.

Plans are keyed by boundary: at most one per slab start per view, each built once (in a
`cold_section`, since it allocates) and shared. The warmer thread prepares the cold plan,
and the plan for the previous view's tip, which is what up-to-date clients ask for next.
A request that needs a plan, or its trailer, `.idx` or sizes section, before the warmer
got to it has it built on one thread of the blocking pool, and waits for it as a parked
task, as does every request after it (`budget::Builds`). Each used to wait on a thread
of its own: 700 cold clones arriving with a new SearchGold snapshot took all 512 threads
of the pool for the 5.5 s the `.idx` then took, and a `/gvfs/config` sent meanwhile
waited those 5.5 s. It now takes 0.1 ms (14 ms at worst), on 13 threads.

**Warm traffic.** `examples/clients.rs` replays 14 days of SearchGold through the pipeline
and the edge on a simulated clock (README, "Client traffic"). With exact covers
(`--superset-tier never`) every client pattern, from an agent prefetching every minute to
a developer back after 8 days off, gets within 2% of the ideal: a self-contained pack of
exactly what it lacks, as one upstream fetch would make it. Superset deliveries add each
sealed day and week once (above). Cold clones are 0.04 MiB over a fresh full fetch. Three
changes got there from 2.2-2.5×:

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
shared, like the trailer, and moved into a memfd (as is a plan's `GSZ1` section) so that
responses send it with `sendfile(2)` like the pack: copied into the socket, each byte cost a
socket buffer page the kernel zeroes first plus the copy, ~9× a sendfile'd byte (a cold
prefetch's edge CPU: 87.8 ms with the `.idx`, 51.4 ms now). Building it:

* each slab sorts its ids and computes its entries' CRC-32s once (`SortedSlab`, kept with
  the open slab; 74 MB for SearchGold's baseline). The entries are read in slot order,
  which is no order of the pack's: a large slab's are hashed on 8 threads, each asking
  for the entry 16 places on while it hashes one (0.6 s for that baseline; one thread
  without the hint took 3.1 s);
* a plan's index is one k-way merge of its slabs' sorted runs, with each slab's offsets
  shifted to where its body lands, then the 64-bit offset table, the pack checksum (the
  plan's trailer), and the index checksum. The CRC-32s are kept in id order, the record
  16 places on in a run is asked for ahead, and a run's next entry takes the last one's
  place at the top of the heap: 0.5 s for SearchGold's cold plan (1.5 s when each entry
  cost two cache misses one after the other). The tables need no trailer, so for a large
  pack they are merged while another thread hashes it.

The result is byte-identical to `git index-pack`'s output: verified on every prefetch in
the tests, and on SearchGold's 2.6 GB baseline (9.22M objects, 2.65M 64-bit offsets). That
index is 267 MiB; `index-pack` on the client takes 40 s and 141 CPU-s. The
warmer prepares it for each new cold plan once any client has asked for one: pack and
index are ready 1.3 s after an edge starts on SearchGold, the time its trailer takes
(5.5 s before the changes above).

**Endpoints.**

* `GET /gvfs/config`.
* `GET /gvfs/prefetch`: one pack, plus its `.idx` for clients other than git.
* `GET /gvfs/objects/<id>`: a loose object. gvfs-helper requires loose for GET.
* `POST /gvfs/objects`: one pack. A single non-commit with a loose `Accept` is answered
  loose. A commit expands to `commitDepth` levels of parents plus every tree of each, for
  at most `--max-commit-depth` levels (16 by default; stock clients ask for 1). A level
  costs what its commit changed: 10 objects on SearchGold, about 500 trees and 15 ms of CPU
  on RepoB, whose directories are wide.
* `POST /gvfs/sizes`.
* `/healthz` and `/metrics`.

Any path prefix before `/gvfs/` is accepted.

**Threads and memory.** The edge runs on a tokio runtime, one worker thread per CPU
(`--workers`), one task per connection (`src/serve/conn.rs`); the listening socket's
backlog is raised to `net.core.somaxconn` (std's 128 overflows when a fleet connects at
once). Between requests a connection holds no buffer, only its parked task (about 3 KB) and
a descriptor: when its next request's bytes arrive it takes a 16 KiB head buffer and a
256 KiB output buffer from a pool, and gives them back once the response is out. A read
that leaves room in its buffer has drained the socket and clears its read readiness, as
tokio's own `poll_read` does on epoll, so a connection sleeps until its next request
instead of waking after each response to read nothing. A head that arrives in pieces waits
with the head buffer only: the output buffer goes back to a pool meanwhile (5,000 slow
heads took 62 MiB of memory, 1,278 MiB before). A connection is
closed after `--idle-secs` (30) without a request, or when it goes idle after a response
while `--max-idle-conns` (65,536) others are (its client reconnects; a new connection is
always served first). At most `--max-conns` connections are open at once (default: twice
`--max-idle-conns`, within the file descriptor limit, which the edge raises to its hard
limit, less a reserve of a sixteenth, at least 4,096, for its own files and connections):
past that, the edge stops accepting, and new clients wait in the listen queue until a
connection closes (`gvfs_connections_open`, `gvfs_connections_full_total`). A request must
arrive whole, head and body, within `--idle-secs` once begun, and each wait for a client to
take more of a response is bounded at 120 s. A response waiting on a slow client parks its
task: the worker serves other connections meanwhile, and `sendfile` resumes when the socket
drains (REPORT.md §14: 400 slow clones beside 1.2M GETs a second; with one thread per
request, 256 of them stopped the edge).

Handlers that compute for long (batches of ids, commit expansion, hashing a pack) yield
every 100 µs of work (`src/serve/budget.rs`, `--quantum-us`). tokio does not preempt:
without the yields, a 4,000-id sizes request held up the GETs queued behind it on its worker
(GET p50 59 µs → 1.3 ms). At 100 µs, heavy requests keep the thread-per-request edge's
throughput beside GETs, whose p99 is 4× better; a shorter quantum favors GETs further, at
the heavy requests' expense. Moving such work to tokio's blocking pool does not help much, even at a lower
priority, because its threads share the CPUs with the workers. A request's own work that
computes for long without a place to yield (a large object's delta chain) runs in a
`block_in_place` section, which hands the worker's other tasks to another thread; what
many requests wait for (a plan built once per view) is built on one thread while they
park. Fetches
from object storage or the upstream are awaited: a request waiting on them parks its task.

A POST's working set (its ids, dedupe sets, the splice plan) lives in a leased arena
(default: 2 × cores arenas of 8 MiB of buffers and room for 2^18 ids: about 31 MB each,
resident from the start, 2 GB on 32 cores), awaited without holding a thread; if all
arenas are busy for 10 s, the request gets a 503. Arenas are few, so a slow client must not
hold one: the body is read first, into the connection's own output buffer (bodies of up to
256 KiB fit it; a larger one, up to a quarter of `--arena-mib`, grows it as its bytes arrive,
from a budget shared by all connections, else 503), and only the whole request leases an
arena. A sizes answer is built in that buffer too, and a resolved loose object that fits is
copied there, so the arena is free before the client reads. A pack answer streams from its
plan in the arena, which a slow reader therefore still holds (put a load balancer that
buffers responses in front of an edge). A commit closure larger than its arena grows it in a
`cold_section`, by doubling, up to `--max-request-objects` (default 2^22; beyond: 413). Grown
arenas share a budget (`--grow-objects`, default as much again as all arenas hold, about 250
bytes an object; the `gvfs_grown_objects` gauge), charged as their room doubles. An
expansion past it gets a 503 at once rather than wait holding its arena, so expansions never
queue behind each other and never park arenas that blob batches need; a SearchGold tip
(431k objects) takes 2^18, so every arena can expand one at once. What an expansion costs
before it is refused is the walk so far, which `--max-commit-depth` keeps near one tip's
(0.9 s of CPU for RepoB's 838k objects). The lease's end shrinks
what grew, then returns its charge. The server speaks plain HTTP/1.1: put TLS and HTTP/2 on
the load balancer in front of an edge. The localhost proxy needs neither.

The same binary on `127.0.0.1` is the localhost proxy of an agent VM, and three of an
edge's defaults do not fit it: `--profile proxy` sets them otherwise (a flag given still
wins). Two arenas, since one client needs two: 73 MB of memory at the start where the
edge's two per core were 2 GB on 32 cores. A poll every 15 s, not every 2: a thousand
proxies each poll the upstream and the store. And `--loose-deltas stored`: its clients
inflate at once what it would deflate for them.

## Stores with real blobs (`--blobs separate`)

A store chooses at genesis how it keeps blobs. *Inline* (the default, everything above)
keeps them in the slabs, as SearchGold's `Git-hfs` pointers are. *Separate* keeps every
file as a real git blob in **blob packs** of their own, and only commits and trees in the
slabs, each slab with a **sizes table** for the blobs its trees introduce. The mode is
recorded in the manifest; every later run and every edge follows it. Inline stores are
untouched: they still write manifest version 2, byte for byte as before.

```text
store/
  manifest.bin            version 3: flags (separate) + the blobs.bin generation its
                          trees need
  slabs/<id>.pack|.mphf   commits and trees only
  slabs/<id>.sizes        "GVDSIZE1" | count u64 | (id[20] | size u64)* sorted | SHA-256
  blobs.bin               the blob packs and their segments (compare-and-swap), the
                          genesis commit, whether the history is mirrored
  blobs/<id>.pack         whole blobs (a stock git pack, never deltas)
  blobs/<id>.bidx         segment: PTHash MPHF + 48-byte records over one or more packs
  gc.bin                  as for every store: also what `blobs.bin` no longer lists
```

**Blob packs** (`src/blobs.rs`) hold whole entries only, so any entry is served by
splicing its own bytes. An entry keeps the upstream's zlib stream untouched, unless the
blob is at least 1 MiB and its first 256 KiB compress by less than 5%: then it is rewritten
as deflate *stored* blocks (65,535 content bytes each, 5-byte headers, 0.008% overhead). The
copier measures that itself: it deflates those 256 KiB at git's default level (about 4 ms
per blob of 1 MiB or more), so the choice depends on the blob's content alone, and a blob
is encoded alike however the upstream compressed it, whole or as a delta, and in whatever
pieces it arrived. A blob the upstream sent loose (Copying blobs) has no stream to keep:
unless it is stored, its content is deflated by the copier, at git's default level. A
**segment** record is `id | pack u32 | offset (48 bits) | header
length (8) | stored (1) | entry length u64 | size u64`: 64-bit lengths, so entries beyond
4 GiB are ordinary.

The stored encoding is **optional**: an optimization for clients, not part of the format.
Readers take either encoding, the edge serves both alike (it inflates neither), and a store
may mix them. The copier could stop writing it, or move its threshold, without migrating a
store or changing a client. What it buys is decoding: git inflates stored blocks at the
speed of a copy. A 256 MiB blob that compresses by about 3% reads 5× faster with git 2.53
(`cat-file` 0.60 s → 0.12 s, `index-pack` 1.29 s → 0.81 s), which saves about 1.9 s of CPU
per GiB each time a checkout, diff or index pass reads it. The cost is 3% more bytes in
storage, on the wire and on the client's disk. Content that does not compress at all
(archives, video, encrypted files) gains nothing: git's own zlib already writes it as
stored blocks. The saving lasts only where the client keeps the entry as sent: in packs
(POST batches, and a POST for one blob of 16 MiB or more, answered with that blob's own
pack file). Scalar's maintenance recompresses loose objects. The encoding was first chosen
so that byte ranges could be read in place, through an endpoint since removed (REPORT.md
§15, #13).

### The consistency rule

Once `blobs.bin` is complete, every blob of every tree in the store is in a blob pack it
lists (a manifest's trees' at the `blobs.bin` generation the manifest names). `gvfs-cache
mirror` makes that true for the history up to genesis; every pipeline run keeps it for its
window by copying, before it publishes its trees, the blobs that the trees in its pack
reference and the store lacks. That check is enough, whatever the upstream: an upstream
leaves out of a pack only objects its haves reach, and the haves are the store's own
earlier tips, so a tree it leaves out is in the store and so are its blobs. Plain git
leaves out the trees of the edge commits (an old side branch's base), an upstream with
reachability bitmaps everything the haves reach; either way a reused old version (a
revert, an old branch merged, a restored directory) is in the pack's trees or in the store
already. (The first version mirrored only the genesis tip: the haves then reached history
the store lacked, and a window could reference an old version that was neither in its
pack nor in the store.) Branches never merged into the followed branch are not in the
store at all, trees or blobs. The edge's L0 overlay is read through until the pipeline
seals it. Separately, every blob any tree references has its size in the sizes table of
the slab whose trees introduced it, so sizes are complete for every tree, before and after
the mirror.

* **Genesis** fetches `blob:none` (commits and trees), walks every tree for the blobs it
  references, asks the upstream for their sizes (protocol v2 `object-info`, or ADO's
  `POST /gvfs/sizes`, 4,000 ids per request, 8 requests in flight), writes the baseline's
  sizes table, records the genesis commit in `blobs.bin` (not mirrored yet), and flags the
  manifest `separate`.
* **A 15m run works blobs first** (`pipeline::window_separate`): a `blob:none` fetch of
  the window; the blobs the trees in its pack reference that no segment lists; their sizes
  from the upstream (so a large blob is fetched alone); the copy, each blob verified by
  id, published in `blobs.bin` as it goes (next item); only then, once `blobs.bin` lists
  them all, the slab, its sizes table (the sizes no earlier
  table has) and the manifest. Until the mirror has finished, a run copies only the blobs
  no earlier tree references (no sizes table has them): an unchanged file in a changed
  directory, or an old version brought back, is in the mirror's plan, and copying it too
  would race the mirror a whole directory at a time (measured: one window copied 1.7 GiB of
  untouched large files the mirror was copying). Every blob in a sizes table is then either
  in the plan of the mirror, whose last run checks them all, or new in the window that
  introduced it, which copied it. The new tip is the exception: the blobs of its tree the
  store lacks (an old version brought back) are copied too when they fit in one blob
  pack's bytes (`--blob-pack-mib`), so a tip the mirror has copied stays whole and a
  checkout of it never waits on the upstream. More than that is a tip the mirror has not
  copied yet, which stays in its plan. (Measured on Networking-Metadata, a store with its
  tip mirrored and the rest of the history not: after two windows, 59 of the tip's 523K
  blobs were old versions the store lacked; walking that tip's tree takes 1.25 s.) A run
  that dies in between leaves blobs no tree references yet, never a tree without its
  blobs. Pipeline runners keep the store objects a run reads (sizes tables, segment
  indexes, and the slabs the tip's tree is walked through) in `--cache-dir` across runs,
  pruned to what the store lists. Only those downloads are pruned, known by their names
  (`<slab id>.pack`, `.mphf`, `slabs_<slab id>.sizes`, `<segment id>.bidx`): any other
  file in the directory is left alone.
* **A window's copy is the mirror's loop** (`separate::copy_all`): `--jobs` copies at a
  time (default 4), each on an upstream connection of its own; finished packs published
  at least every `--publish-secs` (default 60); a rate limit waited out; an item tried
  three times. A run that stops before its last blob (its runner's time limit, an item
  that failed) keeps what it published: the next run copies what `blobs.bin` still
  lacks, then seals. The slab goes out only once `blobs.bin`, read again after the copy,
  lists every blob the window needs, whoever copied it. (The copy took one blob at a time
  and published once, after the last: a window that outlived its runner left every pack
  unlisted, and each run began again. A window of 32 blobs of 16 MiB from an upstream
  that sends each at 64 MiB/s: 8.8 s one at a time, 2.5 s with four jobs; a run killed
  after 16 of them left the next one all 32 to fetch, and now, at `--publish-secs 1`,
  the 17 to 20 it had not published.)
* **Runs that overlap share a window** (the next quarter's run starts while this one
  copies; both plan the same items). Each takes the items in an order of its own and
  reads `blobs.bin` again at each of its publishes, leaving out what the other has
  listed. A blob is copied twice only when both took it before either had published it
  and the other read that: what the runs copy in about two publish intervals and an
  item's time, a few times over the copy (the last items always). Two runs at once of a
  window of 256 blobs, half a second each, `--jobs 4 --publish-secs 1`: 299 to 303
  fetched in 20 s, where two serial runs fetched 512 in 134 s. A window that lasts no
  longer than those intervals is still copied by each (32 blobs in 2.5 s: 59 to 62
  fetched). In one order each run takes an item while the other is still copying it,
  to the last one: every blob twice, as measured, however often they publish. A blob
  listed twice is found once; its other copy is wasted bytes, which nothing reclaims
  (Limits). Neither run fails the other: `blobs.bin` is swapped with retries, a fold
  gives way to another writer, and of two manifests one wins: the other run starts
  over, finds the window sealed and publishes nothing.
* **The manifest names the `blobs.bin` generation** the run computed its copies against,
  or the one it read after its copy, which lists them all, even when it copied nothing:
  a run that died after publishing `blobs.bin` left the blobs the next run's trees rely
  on.
* **Rollups and the weekly rebuild** union their parts' tables. **GC** keeps the sizes
  tables of listed slabs and the packs and segments `blobs.bin` lists. Anything else in
  `slabs/` and `blobs/` (a crashed copy's upload, the segments a fold merged, the table
  of a slab the manifest dropped) is noted in `gc.bin` by the first run that finds it
  so, and deleted `--gc-grace-secs` later, as slab files are: an edge that has read the
  `blobs.bin` from before a fold still finds its segments.

### Copying blobs (`src/blobstream.rs`, `src/blobcopy.rs`)

`BlobSink` is the `Write` end of an upstream fetch (`POST /gvfs/objects` from a GVFS
upstream, protocol v2 `want`s otherwise). It parses packs as they arrive and writes one
whole-blob pack:

* a whole entry streams through in fixed memory: inflated once in 64 KiB pieces to verify
  its id (SHA-1 of `blob <size>\0` + content) and size, then written raw or, optionally,
  stored (see Blob packs);
* a delta is inflated in memory and applied to its base, read back from the output (a REF
  base may come later: the delta waits);
* a loose object in place of a pack streams through as a whole entry does. Azure DevOps
  answers a request for one object with `zlib("blob <size>\0" ++ content)`, whatever the
  request accepts, and every blob of 16 MiB or more is asked for alone. The client hands
  that answer on as it arrives; the sink inflates it once, in 64 KiB pieces, to verify
  the id and the size (the header must be the one git writes), and writes the content
  stored by the same rule, else deflated, once: the loose stream begins with the header,
  so it cannot be kept. The content is deflated 64 KiB at a time whatever pieces arrived,
  so one blob makes one entry, byte for byte (zlib's output goes by the pieces it is fed:
  3 to 20 bytes more or less for 32 MiB of text). (The client first made a pack of one
  out of the answer: inflated, deflated at level 6 and hashed, then inflated again by
  the sink, which wrote content that does not compress in stored blocks anyway. Blobs of
  32 MiB from a local upstream that answers so, per copy: 43–55 MiB/s before, at 17 to
  22 ms of CPU per MiB; now 380–405 MiB/s at 1.6 ms for content that does not compress,
  where the same blobs sent as packs pass at 334–351 MiB/s; and 56–57 MiB/s at 16.7 ms
  for text, which is the one deflate.)
* the output declares as many entries as blobs were asked for; any other object, a blob
  twice, a blob missing, or a bad input trailer fails the pack: a loose object is an
  entry like any other, refused at once when no blob is asked for any more, and at the
  first 64 KiB past the size it declares;
* so does a pack that declares more entries than blobs are still asked for, at its header
  (the deltas of one would wait for bases that never come, however many there are), and
  a delta whose ops do not make the result its header declares, before a buffer is sized
  for it;
* so does an entry, or a loose object, whose zlib stream is longer than twice its content
  plus 1 KiB. No encoder writes one (deflate's longest code is 15 bits): it is padded
  with empty blocks, which a hostile or broken upstream can send without end, and which
  the copy would hold in memory (while a large blob's encoding is undecided, and when a
  delta's base is read back) and write to the store.

Work is cut into items in the order given. A blob of 16 MiB or more is an item of its
own: one request whose single whole entry streams straight into the store (`fs`: a temp
file renamed into place; Azure: Put Block per 16 MiB, Put Block List at the end), with no
local disk. Smaller blobs are batched up to `--blob-pack-mib` (default 2 GiB) and spooled
to a local file, where deltas can read their bases back. Pack ids are fresh per copy, so a
published pack is never overwritten; segments are content-addressed; `blobs.bin` is
replaced by compare-and-swap. Its segments are kept a geometric progression by blob count
(each at least twice the next smaller, as git's `repack --geometric`): a publish merges only
the smallest ones, plus any bigger one they would not be half of, so there are O(log n)
segments and each record is rewritten (and downloaded again by edges) O(log n) times
(100,000 single-blob publishes: at most 18 segments). Lookups probe the biggest first.

Measured (`tests/separate.rs`, `blob_over_4gib_end_to_end`): a pipeline window with a
4,294,979,641-byte incompressible blob ran in 7.0 s at an 11 MiB peak RSS. The local git
upstream's `pack-objects` held 4.1 GB to send it: that cost is the upstream's.

### `gvfs-cache mirror` (`src/mirror.rs`)

The seed of the blobs: every blob of every tree in the store, planned from the store, never
from the upstream (ids from the slabs' trees, sizes from the sizes tables, minus what
`blobs.bin` lists). The plan is one parallel pass over every tree, each read once
(`separate::history_blobs`): the current tip's tree first, depth first in path order (a
checkout's blobs share packs and cache blocks), then the rest of the history by where the
first tree naming each blob is stored (an upstream writes trees newest commit first, each
commit's in path order). `--jobs` copies run in parallel, finished packs are published
every `--publish-secs` (`separate::copy_all`, the loop a pipeline run's window goes
through too), `--rate-mbps` paces all copies through one throttle, and
`--max-items` seeds in slices. Before each pack, blobs that pipeline runs copied meanwhile
are left out (the index is reloaded after each publish), and every copied blob's size is
checked against the sizes table clients were given. The run that copies the last item
checks that every blob is listed and records `blobs.bin` as mirrored: edges serve from
then on. A restart re-plans and skips what was published; a killed run's unpublished
uploads are never listed and GC removes them (tested by `SIGKILL` at several points). The
throttle is a token bucket, so `--rate-mbps` caps the rate, not the average since the
start: a pause (a 429 waited out) earns a quarter of a second of credit, not its length.

**Repair is on demand.** Nothing repairs a store by itself: once mirrored there is nothing
to repair unless storage loses data or a bug drops something, so no pipeline run pays for
a check. `mirror` is the repair. It lists `blobs/` first (after reading `blobs.bin`, so
everything listed had been written) and takes out of `blobs.bin` what the store no longer
holds: lost segments are dropped, segments that index a lost pack are refolded without
it. It then copies whatever the trees reference and `blobs.bin` does not list; on a whole
store it copies nothing and changes nothing. Meanwhile the store degrades instead of
failing: edges and pipeline runs leave out a lost segment, or a lost or truncated local
pack, with a warning; edges keep booting and proxy those blobs (counted in
`gvfs_blobs_missing_total`, the signal to run `mirror`), and a window copies again what
its own trees need. A lost pack in object storage shows only when it is read (a 5xx).

### Serving (`src/serve/blobs.rs`)

* **Where a blob comes from.** A request id is looked up in the segments of the loaded
  `blobs.bin` (biggest first) before the slabs, which hold only commits and trees: a blob
  id would otherwise probe every slab's index first (three times per POST). The segments
  after the biggest, up to 4 Mi blobs each, have an id filter as slabs have (built once
  per segment): a lookup that goes past the biggest (a new blob, in one of the small
  ones; an id that is no blob) costs a cache line in each segment that lacks the id, not
  a slot computation and a record read. On RepoB (5 segments) the sizes of
  4,000 ids of every kind went from 1,083 to 793 µs of CPU, of 4,000 blobs from 704 to
  607 µs. A POST notes
  each blob by segment, not by cloning its pack's `Arc` (every core incremented the same
  count, a quarter of a blob batch's cycles), and dedupes them in a set sized for a batch.
  Blobs go out in request order (the store's and the proxy's cached ones interleaved),
  then those fetched from the upstream for the request.
  A 4,000-blob POST went from 4.7 to 1.6 ms of CPU, a 4,000-id sizes request from 4.3 to
  0.7 ms (`bench/profile_bpf.py`). Packs of a directory store are
  mapped and leave through `sendfile(2)`. Packs in Azure stay there. The edge holds
  their segment indexes and reads the packs in 4 MiB blocks into a local cache
  (`src/serve/blobcache.rs`), at most 8 blocks per read at a time: a read fetches the
  blocks it lacks, each run of consecutive ones with a single ranged GET. A cached
  block is a file that stays open and mapped, and what the cache holds is read as a
  local pack is: an entry under 16 KiB is copied from the mapping into the response, a
  longer one leaves through `sendfile(2)` from the block's file (block by block, where
  it lies across several), with no wait, no allocation and no system call but those
  that send. Such a read does not take the cache's lock: a pack has a slot per block,
  read under a lock of its own, and a read leaves a stamp of the cache's clock in the
  block (the clock moves when blocks are fetched or evicted, so most reads write
  nothing). The slots are made when a pack's first block is cached: an edge lists every
  pack of its store and reads few, and the packs it has not read cost it their names
  (24 bytes a block from when a pack was listed came to 6 MB for each TiB of packs:
  403 MB and 221 ms to list 64 TiB, now 9 MB and 15 ms). A request keeps the block it
  read last, so blobs that are neighbours in a pack cost one lookup between them, and
  adds its hits to the cache's count when it ends. (A read of a cached block took the
  cache's lock twice and moved the block in
  an ordered map; it opened, read and closed the block's file, and passed the bytes on
  through a buffer allocated for it: seven allocations and three system calls a blob.
  Through the cache, every block cached: 3.8 µs for 300 bytes and 64 µs for 1 MiB,
  now 0.03 µs and 18 µs, 0.01 µs more than a copy from a mapping of the pack. An edge
  of a store in Azurite, its cache warm: a POST of 4,000 small blobs took 33.5 ms of
  CPU and 13,000 system calls, and takes 8.0 ms and 53, as from a directory store's
  mapped pack (7.7 ms, and as many calls); a GET of one blob took 30.0 µs, and takes
  14.3, as mapped.)
  Blocks are evicted least recently used beyond `--blob-cache-mib` (default 32 GiB),
  and beyond a number of blocks, since each holds a descriptor and a mapping: a
  quarter of the descriptors the edge keeps for itself, as the proxy's packs below,
  and a quarter of `vm.max_map_count`. That is 8,192 blocks under a limit of 524,288
  descriptors, which is what the default budget holds, and 1,024 (4 GiB) under 65,536:
  the edge says at start when its limits leave the cache fewer blocks than its budget
  holds. Eviction sorts the blocks by stamp when it needs room and takes them in that
  order until it is used up (O(log n) a block); a block read since has another stamp
  and is passed over. A read holds the block it is sending: one evicted meanwhile
  stays readable, and its file is removed, unmapped and closed with its last reader,
  so every read finishes whatever the bounds; a block that does not become whole (its
  fetch failed, or the request that fetched it went away) leaves no file.
  `gvfs_blob_cache_blocks` and `gvfs_blob_cache_bytes` report what is kept. A read
  of more than a quarter of the budget (one huge blob) streams straight from the store,
  32 MiB at a time, instead of evicting everything else (`--blob-cache-mib 0`: every read
  does). A read that fetches is of a whole range, or of all that is left of one, its
  pieces handed to an async sink as they arrive (a client's socket, the pack's hash, an
  inflater): no buffer between the store and the client, and the cache sees a read's
  length. A loose blob that is not cached is one read of its entry's zlib stream, sent as
  it arrives: the response begins when the stream's first two bytes are here (its
  header, which says whether it can be spliced), and its last four, the Adler-32, are
  kept for the trailer. (The header was a read of its own, before the body's: a request
  to the store for two bytes where there is no cache, and with one a block fetched for
  them, and kept, also of a blob the cache streams past. A blob of 9 MiB asked for
  first took two requests to the store and takes one, 34 MiB three and two; without a
  cache every loose blob took one more than it does.) The mirror writes the tip's blobs
  first, in path order, so a checkout's blobs share blocks; a block being fetched is
  fetched once (other readers' tasks wait for it, and fail with it if that fetch fails).
  The ranged GETs go through the async client, its connections pooled per account.
* **The proxy** serves blobs the store does not have, only for ids a sizes table names (a
  404 otherwise, without touching the upstream): the L0 overlay's, copied when the
  pipeline seals their trees, and blobs requested before the mirror has finished. A blob a
  sealed tree names that a complete store lacks is a fault: still proxied (a 503 without
  an upstream), but counted in `gvfs_blobs_missing_total` and logged. The proxy fetches
  each blob once per edge (requests for a blob being fetched wait for that fetch, and get
  its error if it fails, rather than ask the upstream one after another),
  verifies it as it streams in, and keeps it, one pack per fetch (oldest fetches go
  first; the newest stays even past the bounds, since the requests that waited for it
  read its blobs from the cache). A pack of at most 16 KiB is kept in memory, up to
  64 MiB and 65,536 of them: one blob asked for alone, a source file or a pointer, is
  such a pack, ranges that short are copied into the response rather than sent with
  `sendfile(2)`, and a file each would cost a descriptor and a mapping each. A larger
  pack is a file under `--cache-dir` (written, not synced: the directory is emptied at
  start), up to `--proxy-cache-mib` and `--proxy-cache-packs` of them: a quarter of the
  descriptors the edge keeps for itself, 256 under a limit of 1,024 and 8,192 under
  524,288. (The bytes alone bounded neither descriptors nor mappings; bounded in packs
  whatever their size, single-blob requests for more blobs than the bound evicted one
  another, each then a fetch from the upstream: 9 ms where a cached blob takes 70 µs.)
  When a new `blobs.bin` is loaded, the packs whose blobs it all lists go too (the
  pipeline has copied what a window sealed, and requests look in the store first). A
  request keeps the packs it already holds, so eviction never makes it start over, and
  lets go of them when it ends: an evicted pack's file goes with its last request, and a
  fetch that fails or is abandoned leaves none. `gvfs_proxy_cache_{packs,files,bytes}`
  report what is kept. The
  proxy keeps 8 upstream connections for its next fetches. An
  upstream failure is a 503, which clients retry; gvfs-helper treats a 404 as fatal. Its
  requests are async (`AsyncUpstream`: the async client for smart HTTP and GVFS
  upstreams, `tokio::process` for a local `git upload-pack`); what arrives is verified
  and written on tokio's blocking pool, fed through a bounded queue
  (`serve::blobs::fetch_into`), so the runtime's workers keep serving meanwhile. A loose
  answer (Azure DevOps, for one blob) goes through that queue as it arrived, like a
  pack's pieces. (The request's task made a pack of it first: 15 ms of CPU per MiB on a
  worker. With one worker and four blobs of 32 MiB being proxied, other requests waited
  up to 0.5 s, 24 to 74 of them more than 10 ms; now none waits more than 3.5 ms, which
  is what an idle edge's answers reach on this host.)
* **Loose objects** are spliced from the entry's deflate blocks between a stored block
  and the combined Adler-32, so nothing is inflated; a remote entry is planned from its
  first 2 and last 4 zlib bytes where the cache holds them, and is otherwise sent as it
  is read, its trailer made when its last bytes arrive. **POST packs** append whole
  blob entries after the slab splice and hash the trailer as the bytes go out, so a
  remote entry is read once. A POST of a single blob gets a loose object when the client
  accepts one, unless the blob is 16 MiB or more and the client also takes packs (as
  gvfs-helper does): a client's maintenance must inflate and deflate a loose object again
  (~15 s per GiB), never a pack entry. Such a blob was copied into a pack of its own, whose
  header and trailer were written then, so that file goes out as it is: as cheap for the
  edge as a loose object (0.01 s of CPU for 1 GiB; hashing a fresh trailer took 0.38 s).
* **Sizes.** `POST /gvfs/sizes` answers blobs from their segment records (the sizes
  tables for blobs the store lacks), exactly the blobs a GET serves: an id none of them
  has is a 404 without asking the upstream. A prefetch that
  sends `X-GVFS-Sizes: 1` gets, after its packs, `"GSZ1" | count u64 | (id[20] | size
  u64)*`: the union of the tables of the slabs in that response (a cold clone: the
  history's; a warm prefetch: what its window introduced). The section is built once per
  plan, and the response says `X-GVFS-Sizes: 1`. Without the header the stream is
  unchanged.
* **Consistency at the edge.** `blobs.bin` is polled with the manifest. A manifest is
  applied only once the `blobs.bin` generation it names is loaded. `/healthz` stays 503
  until `blobs.bin` is complete (the history mirrored). L0 fetches are `blob:none`, with
  the sizes their trees introduce asked of the upstream.

Store hits allocate nothing (the edge tests run `--alloc-guard strict`). Proxy fetches
and remote reads are cold sections (`budget::cold`: allocations while they are polled are
not the hot path's).

### Limits and open questions

* Mirroring the whole history costs what the history weighs (SearchGold's main: 3.34M
  pointers standing for 256 TiB, per `inspect slab`), once, before edges serve; the
  mirror is resumable, parallel and rate-capped, and ADO's serving rate decides how long
  it takes. ADO also caps the server CPU each identity may use: mirroring
  Networking-Metadata (small XML files, which ADO deltifies per batch request), 4 copies
  were slowed after 5 minutes and answered 429 (resource `ATCPU`) after 20, every
  request of that identity then blocked for minutes. Jobs that can wait (the pipeline,
  the mirror, the edge's live loop) wait out a 429 as its `Retry-After` asks.
* Azure DevOps speaks only protocol v0 (measured on dev.azure.com: it ignores
  `Git-Protocol: version=2`) and advertises `filter` there, so `blob:none` fetches go over
  v0. Without v2 there is no `object-info` and no blob `want`s: blobs by id and sizes come
  through its GVFS endpoints (`/gvfs/objects`, `/gvfs/sizes`), used wherever an upstream
  offers them. Nor is there a way to ask for one ref: the answer to a poll is the
  advertisement of every ref (10.2 MB for SearchGold's 82,779 refs, every `--poll-ms`).
  The edge reads it up to its branch's line and drops the connection: the first ref line
  is `HEAD`'s, and names the branch `HEAD` stands for (`symref=HEAD:refs/heads/main`), so
  the default branch's tip is in the first kilobyte; another branch is found at its own
  line. Measured against the SearchGold clone served over v0: 4.8 MB/s for an idle edge
  before, and after at most 159 KB per poll (what the edge's socket had taken in when it
  closed, on loopback), with a new connection per poll.
  `/gvfs/objects` sends one object asked for alone (each large blob) loose,
  whatever the request accepts: the copier takes it as it is (Copying blobs). One that
  compresses still costs a deflate at level 6: 16.7 ms of CPU per MiB of text, 57 MiB/s
  a copy, where a pack's entry of the same blob passes at 340.
* The edge's block cache lives in memory and on local disk and starts empty after a
  restart. Hot small-blob packs are not prefetched whole.
* A window run reads every segment index (48 B per blob) and the manifest's sizes tables:
  about 220 MB at SearchGold's scale. A runner that keeps `--cache-dir` downloads each
  once; an ephemeral runner, every run.
* Small blobs are stored whole (no deltas), trading storage on similar small files for
  entries that always splice alone. Chunk-level dedup (HFS) is gone: git stores whole
  files.
* A blob request first misses every metadata slab's index, then probes the segments.
* No reachability GC of blob packs (blobs of force-pushed-away commits stay, and so does
  the second copy of a blob that two overlapping runs both copied).
* A copy has `--gc-grace-secs` from the first garbage collection that sees one of its
  packs unlisted to the `blobs.bin` that lists it. The mirror and a window publish every
  `--publish-secs`, far inside it. A copy that could not publish for longer than that
  while other runs collect garbage loses its first packs, and the `blobs.bin` it then
  publishes lists packs the store lacks (edges proxy their blobs; `mirror` repairs).

## Departures from the PDF

| PDF | Here | Why |
|---|---|---|
| Undeltify every slab entry; write objects in "Merkle DFS" order | Slabs keep deltas. Trees are re-deltified into per-directory clusters (newest whole, older versions chained); other objects keep their compressed bytes | SearchGold's 7.54M trees are 45.1 GiB raw but 2.42 GiB deltified. zlib alone cannot shrink trees, which are mostly 20-byte ids, so undeltified slabs would be about 10× larger. |
| Rollups splice and drop duplicate ids and "superseded intermediate trees" (`plan_deduplicated_range_splice`) | Rollups are optimal splices from the store (`repack`): the union of the slabs with trees re-deltified across them. Nothing reachable is dropped | Intermediate trees are what intermediate commits point to; dropping them breaks history. Plain splicing is 8× larger than one fetch (21.9 vs 2.65 MiB for a SearchGold week); the repacked week is 2.63 MiB. |
| Tree deltas only in 24h/168h slabs, in clusters capped at depth 15 with every member a delta against the cluster head (later advice) | Clusters at every tier, each version a delta against its newer neighbour, depth up to 50 | Deltas against the head grow with every change since the head; deltas against the neighbour stay one change wide. A depth of 50 needs fewer whole copies, and an older version is still one contiguous range from its cluster's head. Small intraday slabs repack in milliseconds. |
| 8-byte packed descriptors with a 16-bit fingerprint | 40-byte records with the full id | Exact answers (404 for unknown ids, never a wrong object). Sizes, types, and delta bases come without inflation. 24-byte records would save about 150 MB on SearchGold. |
| Edge range-reads slabs from S3/Blob (coalesced HTTP `Range`) | The edge hydrates slabs to local disk (`--cache-dir`) and maps them | Local reads cost microseconds: a deltified tree GET is 190 µs p50. The edge restarts from the store at any time. The cluster layout keeps range reads possible behind the same `Slab` type. |
| Pack timestamps are creation times; warm clients get every slab newer than their cursor; rollups replace the slabs they fold | Timestamps are derived from commits; warm clients get the exact cover from their cursor (retained finer slabs, cascading rollups), repacked into one pack | An edge's L0 slab and the sealed slab that later ends at the same commit get the same stamp, so nothing is sent twice. Measured over 14 days of SearchGold, every client pattern gets within 2% of one upstream fetch of exactly what it lacks, against 2.2-2.5× before. |
| Superset tombstoning: every rollup reaches clients whole, and a client hook unlinks the packs it subsumes | Only sealed days and weeks reach a client whole, once (`--superset-tier`); the response carries its interval for a hook, and stock `git multi-pack-index expire` does the unlinking | Stock clients (gvfs-helper, VFS for Git, Scalar) run no hook, but their maintenance expires packs that newer packs duplicate. Days and weeks suffice for daily maintenance; finer supersets would re-send each hour up to five times (fable: 1.9-5.3× the ideal). |
| Tokio/Axum/Hyper, `DashMap` memtable for L0 | tokio with the in-house HTTP/1.1 and `sendfile` (no Axum or hyper); L0 = memory slabs (one per push) in the same format as sealed slabs | hyper cannot `sendfile`: a cold clone cost 20× the CPU per GB, plus 5-6 allocations per request (rule 3). tokio itself measured 24-38% less CPU per small request than thread-per-request, and slow clients hold no thread (REPORT.md §14). One code path serves every slab. |
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
* Serving TLS/HTTP2 directly, and range-read (non-hydrating) slab access. (Blob packs of
  `--blobs separate` stores are read by range; slabs are still hydrated.)
* Reachability GC of the baseline: it only grows by appends, so objects orphaned by a
  force push stay in it.
* CAS pre-warming from pointer contents (the PDF's optional sidecar).
