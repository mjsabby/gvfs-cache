# gvfs-daemon: standard tests and benchmarks

This lists every test and measurement run on **opus** (`/home/muks/gvfs-daemon/opus`) during its build. It is written so that **argon, astra, fable and sol** (`/home/muks/gvfs-daemon/<name>`) can go through the same suite. Each item has a stable ID, a setup, a procedure, and a pass criterion or metric, followed by opus's result. §9 is the cross-implementation results matrix with opus filled in.

## 0. Requirements under test

| Requirement (from the design PDF and the user) | Checked by |
|---|---|
| Exactly one pack per prefetch response, cold clones included | T-03, T-05, T-10 to T-16, S-03, S-07, B-11 |
| Everything served is packed as well as one upstream fetch of the same objects | B-06, B-11, B-12 |
| The upstream is contacted once for the seed, then only for new commits; rollups come from stored slabs | by construction in opus; see §10 for the recommended check |
| Pipeline runs are stateless (CI runners keep nothing between runs) | every T test: each pipeline step is a new process with an empty work directory |
| A client is never told it is up to date while it lacks objects | T-03, T-05, T-10 to T-14 |
| Request paths do not allocate after startup | T-18, B-09 |
| Byte-exact protocol output (packs, `.idx`, loose objects, sizes) | T-01 to T-09, S-01, S-02 |

## 1. Conventions

- **IDs.** There are three families:
  - `T-xx`: pass/fail scenarios on small generated repositories.
  - `S-xx`: pass/fail checks at SearchGold scale.
  - `B-xx`: measurements.
- **opus stages.** Each opus result says which stage it was measured on:
  - **P1, initial design.** Slabs were the upstream's packs, kept verbatim. Rollups re-fetched from the upstream, prefetch concatenated whole slabs, there was no `.idx`, and timestamps came from the wall clock.
  - **P2, optimal splice.** The pipeline only repacks stored slabs, with per-directory tree clusters. The seed's REF deltas are re-pointed as OFS deltas, and non-git clients are sent a `.idx`.
  - **P3, final** (the snapshot in `opus/`). Timestamps are derived from commits. Replaced slabs are retained and rollups cascade. Each push gets one L0 slab. Every prefetch is the exact cover from the client's cursor, repacked into one pack.

  The request paths (GET, POST, sizes, pack streaming) did not change materially after P1, but prefetch plan selection did. Re-measure P1/P2 numbers on P3 before ranking.
- **Units.** Sizes are in bytes, with KiB/MiB/GiB = 2^10/2^20/2^30. Throughput in GB/s is 10^9 B/s, as loadgen reports it. Times are wall clock.
- **Repeats.** opus numbers are single runs unless stated. The final test suite was also run 3 times at P1 with stable results. For comparisons, use 3 runs, report the median, and treat gaps under 10% as noise. This is the convention in `~/gvfs-server-benchmark/REPORT.md`.
- **Byte checks use stock git only:**
  - `git index-pack` (pack trailer and every object hash)
  - `git fsck --strict`
  - `git cat-file`
  - `git rev-list --objects --missing=print`

  Never trust the implementation's own reader.

## 2. Environment

| | |
|---|---|
| CPU | AMD Ryzen 9 9950X, 16 cores / 32 threads |
| RAM | 89 GiB |
| OS | Linux 7.3.0. CPU mitigations are compiled out. `init_on_alloc` is on, so the kernel zeroes every TCP send page; this shows in send-bound profiles. |
| Disk | NVMe with ext4 for `/home`. `/tmp` is a 45 GB tmpfs (RAM), so keep multi-GB scratch data under `/home`. |
| Toolchain | rustc 1.97.1. git 2.53.0 for upstreams and all validation. Microsoft git 2.56.0.vfs.0.2.5 at `~/git/bin-wrappers/git` for `gvfs-helper`. |
| Load generator | `~/gvfs-server-benchmark/target/loadgen/release/loadgen` (Rust/hyper, closed loop, HTTP/1.1, HDR histograms) |
| Azure | Azurite emulator (npm `azurite`): in-memory, Shared Key, path-style endpoint |
| Placement | Loopback only, nothing pinned. Server, load generator and upstream shared all 32 threads. |

## 3. Corpora and upstreams

### 3.1 G: generated scratch repositories (T tests)

G is an upstream bare repository (`git init --bare -b main`) plus a worktree that pushes to it. The k-th call to `commit(p)` does three things:

1. It rewrites `big.txt` as 600 lines `line <l>`, appending ` edit <k>` where `l % 97 == k % 97`. That changes 6-7 lines, so git makes real blob deltas.
2. It adds `p` pointer files at `d<i%4>/e<i%3>/f<k>-<i>.dat` with the content `~~Git-hfs 1.0\nblob:<k*1000+i as 64 uppercase hex digits>\nsize:<k*7+i>\n`.
3. It commits as `t <t@t>` and pushes `HEAD:main`.

Reference: `opus/tests/common/mod.rs` (`Upstream`).

### 3.2 SG: SearchGold

SG is a real GitHFS repository cloned from Azure DevOps, at `/home/muks/SearchGold`. It is only read, never written.

| | |
|---|---|
| `main` | `4d12e2c7915577f43351ac82b9ca15bd7788be89` (2026-10-24 17:15:10 UTC). 658,211 commits (657,963 first-parent); 9,224,930 objects reachable. |
| On-disk pack | `pack-ec111cc94ed5a20453e303e45695428e4ac3a35b`: 3,266,681,772 B (3.04 GiB), 12,083,668 objects (all refs). Made by ADO. |
| **W14** (B-11) | 2026-10-10T00:00Z to 2026-10-24T00:00Z: 3,308 first-parent commits. Seed `8ed3b8cfe7aef031f7a8c5b6c6af389fde8e5882`, last `acf19de22c5dfb7d04622d67bb3d30ec7d81672d`. Starts on a Saturday. |
| **W7** (B-06) | 2026-10-17T00:00Z to 2026-10-24T00:00Z: 1,656 first-parent commits in 526 non-empty 15-minute buckets. Seed `5c19c35b86c4a3627787733b281ddcb38ede2a19`. |

**Window rule.** Use the first-parent history of `main`, with commit times made non-decreasing. The seed is the last commit before the window starts.

**Seed snapshot.** opus's scale and load runs (S-01 to S-06, B-02 to B-10) used a seed snapshot of `main` with 9,222,096 objects, 0.03% below today's tip. Its exact commit was not recorded. New runs should seed at `4d12e2c7`.

### 3.3 Upstreams

| Name | What it is | Used by |
|---|---|---|
| U-local | A bare repository served by a locally spawned `git upload-pack --stateless-rpc` (protocol v2). For SG, the bare repository shares SearchGold's objects and has `main` at the seed. | T tests, S tests, B-02 to B-10 |
| U-http | A smart-HTTP server inside the test, in front of the same repository. It requires Basic auth with a test token, keeps connections alive, sends chunked responses, and speaks protocol v2 or v0 only. | T-16 |
| U-replay | A bare repository with `objects/info/alternates` pointing at SearchGold's objects. `refs/heads/main` is moved commit by commit along first-parent history, at each commit's time. | B-06, B-11 |

## 4. Harness contract

Each implementation must expose these capabilities. The last column shows how opus provides each one.

| Capability | Used by | opus |
|---|---|---|
| One pipeline run with a tier, an injected clock and a branch. The first run on an empty store is the seed. | T-10 to T-17, B-05, B-11 | `gvfs-daemon pipeline --store fs:<dir> --upstream <path or url> --tier <15m, 1h, 6h, 12h, 24h or 168h> [--now <unix-secs>] [--branch <b>] [--keep-weeks N] [--gc-grace-secs N] [--token-env VAR]` |
| A server that follows the store and, optionally, the upstream | T-03 to T-17, B-07 to B-10 | `gvfs-daemon serve --store … [--upstream …] --bind 127.0.0.1:<port> --poll-ms 100 --cache-dir <dir> --alloc-guard strict`. Tests also pass `--workers 8 --arenas 4`. |
| The GVFS protocol, under any path prefix (`/org/repo/gvfs/…`) | all server tests | `GET /gvfs/config`, `GET /gvfs/prefetch?lastPackTimestamp=T`, `GET /gvfs/objects/<id>`, `POST /gvfs/objects`, `POST /gvfs/sizes` |
| Published state: generation, tip, tip timestamp, slabs by tier | T-10 to T-14 | `gvfs-daemon inspect manifest --store …`; `/metrics` `gvfs_manifest_generation` |
| Live-tip state | T-05, T-10 to T-13 | `/metrics` `gvfs_l0_slabs` |
| Hot-path allocations and server errors | T-18, B-09 | `/metrics` `gvfs_alloc_hot_path_total` and `gvfs_errors_total`. A neutral substitute is `~/gvfs-server-benchmark/tools/allocshim` (LD_PRELOAD counter). |
| Offline: index a pack and compare it with git's `.idx` | T-01, S-01, B-02 | `gvfs-daemon ingest --pack <P> --out <D> --threads N --verify-idx <git.idx>` |
| Offline: fetch a commit range, and repack slabs | B-06, B-12 | `gvfs-daemon ingest --upstream <repo> [--have H] --want W --out D`; `gvfs-daemon repack --tier T --out D [--mode copy] [--frozen F] <slab.mphf>...` |
| Export a prefetch pack's `.idx` | S-02 | `gvfs-daemon inspect slab <S.mphf> --write-idx out.idx` |

- **Clock injection.** Since P3, opus derives every timestamp from commits; `--now` only drives garbage collection. An implementation without a `--now` flag can run its pipeline under libfaketime, with the harness writing the simulated time before each event. If it stamps from the server's clock, run the server that way too.
- **Content policy.** This must be declared. opus prefetch packs hold every object reachable from the tip: commits, trees and all blobs, including SG's 592 non-pointer blobs. An implementation that leaves blobs out should compute B-11's ideal with `--filter=blob:none`; it is then complete only for commits and trees.

## 5. Conformance tests (corpus G)

Unless noted, each test seeds with the pipeline, starts the server, and uses U-local. Terms used below:

- **install**: `git index-pack --stdin` into a fresh repository.
- **complete(tip)**: `git rev-list --objects --missing=print <tip>` prints no `?` line.
- **prefetch(T)**: `GET /org/repo/gvfs/prefetch?lastPackTimestamp=T`.
- **GPRE ok**: the response is 200 with `application/x-gvfs-timestamped-packfiles-indexes`, and the body parses exactly with no trailing bytes:
  - `GPRE ` followed by the byte `0x01`, then a u16 LE pack count;
  - per pack: u64 LE timestamp, u64 LE pack length, i64 LE `.idx` length (−1 = none);
  - then the pack bytes, then the `.idx` bytes.

Every server test ends with T-18. All T tests pass on opus P3. Each test's opus name is listed in Appendix A.

**T-00 Build hygiene**
- Run `cargo build --release` and the implementation's own suite, in release and in debug (debug enables overflow checks).
- Run clippy on lib and bin targets with `unwrap_used`, `expect_used`, `panic`, `indexing_slicing` and `arithmetic_side_effects` denied.
- `unsafe` is allowed only in isolated crates.
- opus:
  - 49 tests: 40 unit tests, 2 in the `unsafe` crates `gvfs-alloc-guard` and `gvfs-mmap`, 5 end-to-end and 2 Azure.
  - All pass. The 47 in the main crate were re-run in release and debug on P3.
  - Wall time in release: unit 0.23 s, e2e 1.0 s, Azure 1.1 s. clippy is clean.

**T-01 Index matches git**
1. Corpus G with 4 × `commit(10)`.
2. Build a pack with `git pack-objects --revs --stdout` for `main`, and index it with `git index-pack -o in.idx` as the reference.
3. Ingest the pack.

Pass:
- Every id and offset equals `in.idx`.
- A content scan reports 4 commits, 40 GitHFS pointers and 4 other blobs.

REF_DELTA chains are covered by a unit test here and at scale by S-01.

**T-02 Config.** `GET /org/repo/gvfs/config` returns 200 `application/json` with a JSON object.

**T-03 Cold prefetch, then up to date.** Corpus G with 3 × `commit(6)`, then seed.
- `prefetch(0)` is GPRE ok with 1 pack. Its timestamp equals the seed's boundary. Install it, then complete(tip).
- `prefetch(<that timestamp>)` returns 0 packs.

**T-04 `.idx` by client** (only if the implementation sends one). This applies to every prefetch made by every T test.
- With no `User-Agent`: the `.idx` length is at least 0. The `.idx` is byte-identical to `git index-pack -o` of the same pack. (VFS for Git sends `<process>/<version>` and trusts a server `.idx`.)
- With `User-Agent: git/2.45.2.vfs.0.0` (gvfs-helper): the length is −1 and the pack bytes are the same.
- opus: P2 and later, `--prefetch-idx auto`.

**T-05 Live tip**
1. Push 2 × `commit(5)`, giving tip2.
2. Wait until `GET /gvfs/objects/<tip2>` returns 200; the L0 count is at least 1.

Pass:
- `prefetch(ts1)` returns 1 pack with ts2 > ts1. Install it into the T-03 client, then complete(tip2).
- `prefetch(ts2)` returns 0 packs.
- `prefetch(0)` returns 1 pack with timestamp ts2; complete(tip2) holds in a fresh repository.

**T-06 Loose objects.** For every object reachable from tip2, `GET /org/repo/gvfs/objects/<id>` returns 200 `application/x-git-loose-object`.
- Written as loose files, they pass `git fsck --strict --no-dangling`.
- `cat-file --batch` output equals the upstream's.
- Requesting 40 zeros returns 404.

**T-07 Object batches.** `POST /gvfs/objects` with `{"objectIds":[every non-commit]}` and `Accept: application/x-git-packfile`.
- The response is 200 with a pack that installs without missing delta bases.
- The entry count equals the batch size, and the contents match the upstream's.
- One blob requested with `Accept: application/x-git-loose-object` comes back as a loose object.
- An unknown id returns 404.

**T-08 Commit closure.** `POST /gvfs/objects` with `{"commitDepth":2,"objectIds":["<tip2>"]}`. The pack must hold, with identical contents:
- tip2 and its parent;
- both commits' root trees;
- every subtree of both (`git ls-tree -r -d`).

**T-09 Sizes.** `POST /gvfs/sizes` with every id. The response must be exactly `[{"Id":"<id>","Size":<n>},…]` in request order. `n` is `git cat-file --batch-check='%(objectsize)'`: uncompressed and undeltified.

**T-10 Sealing at the edge's timestamp.** Run the pipeline at `15m`.
- The manifest has generation 2 and one 15m slab. Its tip is tip2, and its tip timestamp is ts2, exactly the edge's L0 timestamp.
- The server reaches generation 2, and its live L0 count is 0.
- `prefetch(ts2)` returns 0 packs: a client that took L0 has nothing to fetch.
- `prefetch(ts1)` returns 1 pack carrying the manifest's timestamp.
- A cold prefetch returns 1 pack, and complete(tip2) holds.

**T-11 One live slab per push**
1. Push tip3 (`commit(3)`) and wait for it to be live.
2. Push tip4 (`commit(3)`) and wait.

Pass:
- The L0 count is 2.
- prefetch(sealed boundary) returns 1 pack with ts4 > ts3 > boundary.
- complete(tip4).

**T-12 Pipeline behind the edge.** Run `pipeline --tier 15m --branch sealed`, where `sealed` = tip3.
- The manifest's tip is tip3, at ts3 (the edge's timestamp). The L0 count is 1.
- `prefetch(ts3)` returns 1 pack with timestamp ts4 and exactly as many entries as `git rev-list --objects tip4 ^tip3`.
- A cold prefetch gives complete(tip4).
- `prefetch(ts4)` returns 0 packs.

**T-13 Merge push with an older clock.**
1. From tip3, make a side commit with author and committer date `1000000000 +0000` (2001).
2. Run `merge --no-ff tip4` with the same date, and push the result as `main`.
3. Run the pipeline at `15m`.

Pass:
- Before sealing, `prefetch(ts4)` returns 1 pack with a timestamp above ts4, then complete(merge).
- After sealing, the manifest's tip is the merge, stamped with the edge's timestamp.
- `prefetch(<that timestamp>)` returns 0 packs, and a cold prefetch gives complete(merge).

This catches timestamps folded along first parents only.

**T-14 Pyramid lifecycle.** The server runs without an upstream, and each pipeline run advances `--now` by 900 s.

Setup: G with 2 × `commit(4)`, then seed. A client clones cold; its cursor ts0 is the baseline's end.

After every publication, check:
- the server reaches the new generation;
- a cold prefetch returns 1 pack with the manifest's timestamp, and is complete;
- the client gets 1 pack at its previous cursor (0 if that equals the manifest's timestamp), and is complete;
- prefetch(manifest timestamp) returns 0 packs.

Steps:
1. 3 × (commit, `15m`): three 15m slabs.
2. Commit, then `1h`: one 1h slab. Its `have` is the seed's tip and it starts at the seed's timestamp.
3. Commit tip_a, then `15m` (giving ts_a). Commit tip_b, then `1h`:
   - two 1h slabs;
   - the folded 15m slabs stay listed (opus `retained`);
   - `prefetch(ts_a)` returns 1 pack with exactly `rev-list --objects tip_b ^tip_a` entries.
4. `6h` with no new commits: one 6h slab with unchanged timestamps; the client is up to date.
5. Commit, then `24h`: one 24h slab.
6. Commit, then `168h`:
   - the pyramid is empty and there is one weekly slab from the seed;
   - the baseline holds every object of the tip;
   - a client still at ts0 gets 1 pack of exactly the weekly slab's objects, and is complete.
7. Commit, then `15m`. Commit, then `168h --keep-weeks 1 --gc-grace-secs 0`:
   - there is one weekly slab, starting at week 1's end;
   - a client at ts0, now older than the warm window, gets the cold pack, with exactly the baseline's objects;
   - the store holds exactly the slab files the manifest references.

**T-15 gvfs-helper**

Setup: G with 2 × `commit(8)`, seed, and a server with an upstream. Make a fresh client with `remote.origin.url=http://127.0.0.1:<port>/org/repo`, and run Microsoft git's `gvfs-helper --cache-server=disable` from it.

Pass:
1. `prefetch` leaves exactly 1 `.pack` in the client, and complete(tip).
2. Push, then `prefetch`: 2 packs in total, complete.
3. Push a `commit(8)`, `get` one new object, then `post` the rest: complete.

The test is skipped without Microsoft git (`GVFS_TEST_MSGIT`, default `~/git/bin-wrappers/git`).

**T-16 Upstream over smart HTTP, v2 and v0-only.** On U-http, once per protocol:
- The pipeline fails with 401 without credentials.
- With the token: seed; 2 commits, then `15m`; 1 commit, then `1h`. The 1h slab holds exactly `rev-list --objects <seed tip>..main`.
- A server using the same transport picks up a push. A cold prefetch returns 1 pack, and complete(tip).
- The upstream saw at least 4 upload-pack POSTs, all v2 if and only if v2 was offered.

**T-17 Azure Blob store.** Runs on Azurite, or on a real account under a unique prefix that is deleted afterwards.

Store checks:
- Compare-and-swap on `manifest.bin`:
  - creating it with If-None-Match succeeds, and a second create fails;
  - an update with If-Match(etag) succeeds, and one with a stale etag fails.
- A slab-sized file uploaded in parallel 16 MiB blocks and downloaded as parallel ranges is byte-identical.
- Listing returns the expected keys, with modification times within 5 minutes.
- A second prefix in the same container sees none of the first's keys.

Flow, entirely on Azure: seed, `15m`, a `1h` that repacks 2 slabs, weekly with GC, and a server.
- Every prefetch returns 1 pack and is complete.
- After GC, the store lists exactly the referenced slabs.

**T-18 Resource discipline.** The server runs in strict allocation mode: in opus, `--alloc-guard strict` aborts the process on any heap allocation inside a request. At the end of every server test, hot-path allocations and server errors must both be 0.

**Internal properties.** opus unit tests cover these; other implementations should have equivalents.

- **`mphf.rs`:** the MPHF is a bijection, rejects duplicate keys, and gives the same table for any build thread count.
- **`slab.rs` and `manifest.rs`:** slab records and manifests round-trip, and a flipped manifest byte fails the checksum.
- **`store.rs`:** fs-store conditional writes.
- **`delta.rs`:** git delta apply works; tree deltas round-trip and are small.
- **`zlib.rs`:** exact inflate; split deflate equals whole; a loose object spliced from a packed one is valid.
- **`hfs.rs`:** real GitHFS pointer variants parse, and non-pointers are rejected.
- **`idx.rs`:** a plan's `.idx` equals git's.
- **`repack.rs`, rollups:** rolling up ten 4-commit slabs gives exactly one fetch's objects. The result must be under 75% of their concatenation and under 110% of one fetch. The newest version of every tree is stored whole, older ones are chained to depth 2 or more, and repacking the output reproduces it byte for byte.
- **`repack.rs`, other cases:** appending to a frozen baseline keeps the baseline as a byte-identical prefix. Excluding slabs drops their objects and rebases the rest.
- **`timeline.rs`:** timestamps do not depend on where history is cut into slabs. A merge is stamped above every parent, the same way on edge and pipeline. Covers are exact and minimal.
- **`serve/mod.rs`:** only sealed L0 slabs age out.
- **HTTP and Azure:** HTTP/1.1 heads parse, chunked and length-delimited bodies work over a socket, and the Azure Shared Key string-to-sign matches the reference.

## 6. Scale checks (SG)

| ID | Check | Pass | opus |
|---|---|---|---|
| S-01 | Index the 3.04 GiB ADO pack (REF_DELTA only, chains up to depth 50) | Every id and offset equals git's `.idx` | ✅ 12,083,668 of 12,083,668 (P1) |
| S-02 | Build the `.idx` for the 2.42 GiB baseline: 9.22M objects, 2.65M of them past 2 GiB (64-bit offsets) | Byte-identical to `git index-pack` | ✅ (P2) |
| S-03 | Cold clone with gvfs-helper | 1 pack, every object reachable | ✅ 9,222,096 objects (P1). Repeated after a weekly baseline rebuild: 9,223,025 (P1). |
| S-04 | `POST` the closure of the tip commit | Installs; every tree of the tip present | ✅ all 505,605 trees, 137.8 MB (P1) |
| S-05 | The edge follows new manifests at scale | Generations 2→4 served, covered L0 dropped, cold prefetch still 1 valid pack | ✅ (P1) |
| S-06 | Every repacked output is re-indexed from content and its object set checked | Exactly the expected objects | ✅ weekly delta, baseline append, seed copy (P2) |
| S-07 | The 14-day replay (B-11) | Every response is at most 1 pack; no failures | ✅ 22,666 prefetches (P3) |

## 7. Benchmarks

### B-01 Corpus characterization (SG)

These are design-relevant facts. Any implementation's choices, such as keeping deltas or undeltifying trees, can be checked against them. Measured at P1.

| | Full ADO pack (all refs, 12,083,668 objects) | `main` (opus seed, 9,222,096 objects) |
|---|---|---|
| Commits | 1.03M (58K deltified) | 657,912 |
| Trees | 7.54M (4.78M deltified): **45.12 GiB raw, 2.42 GiB packed** | 5,221,743 (1.99M whole, 3.23M deltas) |
| Blobs | 3.52M (276K deltified) | 3,342,441: 3,341,849 GitHFS pointers (255.81 TiB of content behind them) and 592 other blobs (12.79 MiB) |
| Delta encoding | REF_DELTA only (5,116,445 REF, 0 OFS), often based on other deltas | 3,506,901 deltas, depth mean 23.3, p50 23, p90 45, max 50 |
| Delta-to-base distance | | median 50 B; 92.6% within 64 KiB |
| Tip trees stored whole | | 500,328 of 505,611 |
| One day, 251 commits | One fetch: 622 KB. As 76 self-contained 15-minute packs: 3.5 MB. | |

### B-02 Indexing an upstream pack

**Procedure.** Index the SG pack (12,083,668 objects) on all 32 threads. The reference is `git index-pack --threads=32` on the same file. Correctness is checked by S-01.

**opus (P1).**
- 7.0 s: scan 1.22 s, link REF deltas to their bases 0.38 s, resolve 5.42 s.
- Plus 0.5 s to build the MPHF and records.
- git takes **107 s**.

### B-03 Seed (genesis)

**Procedure.** Starting from an empty store, run `pipeline --tier 15m` against U-local at the seed commit, and time each phase.

**opus.**

| Phase | opus |
|---|---|
| Upstream generates the full pack | 70 s (P1) |
| Index 9,222,096 objects (2.42 GiB) | 2.4 s, plus 0.35 s for the MPHF (P1) |
| Re-point the ADO pack's 5.1M REF deltas as OFS | 9.9 s; 3.04 → 2.96 GiB, −90.7 MB (2.8%) (P2) |
| Build the baseline's `.idx` on first use | 4.3 s. The client's own `index-pack` of the same pack takes 40 s. (P2) |

### B-04 Index footprint

**Metric.** Bytes per object of the lookup index.

**opus (P1).** 373.5 MB for 9.22M objects, or 40.5 B/object. Each record is 40 bytes: id; offset, type, depth and size bits; entry length; low size bits; base slot. MPHF pilots make up the rest.

**Reference.** git's `.idx` for the full SG pack is 381.6 MB for 12.08M objects (31.6 B/object), plus a 48.3 MB `.rev`.

### B-05 Pipeline step cost

| Step | opus |
|---|---|
| 15m run with 50 commits and 469 objects: fetch, index, repack, upload (fs store) | 0.04 s (P2) |
| Weekly delta: repack W7's daily slabs (15,908 objects) | 0.16 s (P2) |
| Append the week to the 2.42 GiB baseline, then re-index and verify | 4.3 s (P2) |

### B-06 Rollup size (W7)

**Procedure** (opus: `benches/rollup.sh /home/muks/SearchGold 2026-10-17 <out>`):
1. Bucket W7's first-parent commits into 15-minute buckets (bucket = commit time / 900, non-decreasing). There are 526 non-empty ones.
2. For each bucket, fetch from the previous bucket's last commit to this bucket's last commit (non-thin). That raw pack becomes the implementation's 15m slab.
3. For each day, roll that day's 15m slabs into a 24h slab, and also fetch the whole day in one upstream request ("one fetch").
4. Roll the 7 daily slabs into the weekly slab, and fetch the whole week once.
5. Seed a baseline at W7's seed and append the week to it. Compare its growth with a fresh full fetch at the week's end minus the seed's full fetch.

For an implementation without an offline repack command, use a black-box variant instead. Drive its pipeline over W7 with a simulated clock: one `15m` run per bucket, plus the cron tiers. Then read the sizes of its published 24h and 168h artifacts.

**Metric.** Bytes of (a) the raw 15m packs concatenated, (b) the published rollup, and (c) one fetch.

**opus (P2).**

| | Concatenated 15m packs | Published rollup | One upstream fetch |
|---|---|---|---|
| Each day | 2.2 to 4.4 MiB | 354 to 819 KiB | 323 to 800 KiB (rollup 1-10% larger) |
| Busiest day (290 commits) | 4.32 MiB | 819 KiB | 800 KiB |
| The week (1,656 commits) | 21.90 MiB | **2.63 MiB** | 2.65 MiB |
| Baseline growth from the week | | +2.22 MiB | +2.35 MiB (fresh full fetch) |

The 15m slabs themselves are within 2% of the upstream's packs.

### B-07 Freshness

**Procedure.** Point the server at U-local, move `main` forward, and time how long it takes until:
- (a) `GET /gvfs/objects/<new tip>` returns 200;
- (b) a warm prefetch contains the new tip.

Report the poll interval with the result.

**opus.**
- Corpus G with a 100 ms poll: under 1 s in every T test (P3).
- SG: 50 commits (469 objects) were live 0.87 s after the ref moved, and the next warm prefetch took 11 ms and returned 1 pack (P1).

### B-08 Cold clone

**Procedure.** Use the SG seeded store with the server's cold pack already built. Measure three things:
- (a) Wall time of `gvfs-helper --cache-server=disable prefetch` into an empty repository. This includes the client's own `index-pack`.
- (b) A native `git clone --bare --no-local` from U-local, as the reference.
- (c) One `curl` of the cold prefetch response to `/dev/null`.

**opus (P1).**
- (a) **31 s**: 1 pack, 9,222,096 objects.
- (b) 1 min 53.9 s.
- (c) 2.6 GB in 0.17 s (~15 GB/s).
- The cold pack's trailer (SHA-1 over 2.42 GiB) is computed once per manifest: 0.97 s, and 0.9 s after a rebuild.

### B-09 Request load

**Procedure** (opus: `benches/load.sh <host:port> <upstream.git> 10`):
1. Sample ids with `git rev-list --objects main | cut -c1-40 | shuf -n 400000 --random-source=<(yes)`, then type them with `cat-file --batch-check`.
2. Run loadgen with `--warmup 2 --duration 10` for each workload below.
3. Read `/metrics` afterwards: allocations and errors must both be 0.

| Workload | loadgen arguments | Connections |
|---|---|---|
| GET blob | `--workload get-object --oids blobs.txt` | 64 |
| GET tree | `--workload get-object --oids trees.txt` | 64 |
| POST 4000 ids | `--workload post-objects --oids noncommits.txt --batch 4000` | 32 |
| sizes, 4000 ids | `--workload sizes --oids oids.txt --batch 4000` | 32 |
| prefetch, up to date | `--workload prefetch --ts 4102444800` | 64 |
| prefetch, warm | `--workload prefetch --ts <cursor just before an L0 slab>` | 64 |
| prefetch, cold | `--workload prefetch --ts 0` | 8 |

Server flags for opus: the defaults (256 workers, 64 arenas × 8 MiB) plus `--alloc-guard strict --prefetch-idx never`. loadgen sends no `User-Agent`, so `--prefetch-idx never` keeps the measurement to pack streaming alone.

For cross-implementation runs, also:
- use the bake-off's CPU pinning: the server on `taskset -c 0-3,16-19`, everything else on the other CCD;
- give every implementation the same concurrency;
- use a cursor the server returned rather than a far-future one.

**opus (P1).** Unpinned, on the ~9.22M-object SG baseline, in strict allocation mode, with 0 hot-path allocations and 0 errors.

| Workload | Throughput | p50 / p99 |
|---|---|---|
| GET blob (loose object spliced from the pack) | 1.02M req/s | 59 / 128 µs |
| GET tree (deltified: resolved and deflated) | 265K req/s, 1.3 GB/s | 190 / 853 µs |
| POST 4000 ids, answered with one pack | 1,106 req/s, 10.0 GB/s | 28 / 41 ms |
| POST /gvfs/sizes, 4000 ids | 19.2K req/s | 1.5 / 3.0 ms |
| prefetch, up to date | 1.09M req/s | 55 / 121 µs |
| prefetch, warm (266 KB L0 pack; earlier run, see Appendix B) | 71K req/s | 0.84 ms p50 |
| prefetch, cold (2.6 GB via `sendfile`) | 48.5 GB/s aggregate | 392 ms |

### B-10 Commit closure at scale

**Procedure.** One `POST /gvfs/objects` with the SG tip commit, which returns its whole tree.

**opus (P1).** 137.8 MB in 0.70 s for 505,605 trees. S-04 checks the content.

### B-11 Client traffic (W14)

**Question.** Over two weeks of real pushes, how many packs and bytes does each kind of client receive, compared with the least possible?

**Replay** (opus: `examples/clients.rs`). opus ran this in process: it calls the same code `pipeline` and `serve` run (`pipeline::run`, `serve::LiveTip`, `View::prefetch`), without HTTP. A black-box harness runs the same schedule against the real binaries.

**Upstream.** U-replay on W14, where each first-parent commit is pushed at its commit time.

**Pipeline schedule.** One run every 15 minutes, at t0 + k·900, with the tier chosen like the README's cron script:

| When (UTC) | Tier |
|---|---|
| Minute 0 of an hour divisible by 6 | `6h` |
| 12:00 | `12h` |
| 00:00 | `24h` |
| Sunday 00:00 | `168h` |
| Any other minute 0 | `1h` |
| Every other run | `15m` |

The more specific row wins. Pipeline flags: `--keep-weeks 8 --gc-grace-secs 7200`.

**Edge and ordering.**
- The edge picks up every push before the next event. opus calls `LiveTip::advance` per push. A black-box harness waits until `GET /gvfs/objects/<tip>` returns 200.
- Events at the same time run in this order: pushes, then the pipeline, then clients.

**Clients.** All times are UTC; W14 starts on a Saturday.

| Client | Requests at | Count |
|---|---|---|
| CI agents: fresh clone every 30 min (always cursor 0) | :07 and :37 of every hour | 672 |
| Agent VM: every minute | second 30 of every minute | 20,160 |
| Agent VM: every 15 min | 7.5 min into every quarter hour | 1,344 |
| Agent VM: hourly maintenance | :30 of every hour | 336 |
| Dev laptop: hourly, weekdays | 08:15, 09:15 … 18:15, Mon-Fri | 110 |
| Developer: 3 fetches a day | 09:00, 13:00, 17:30, Mon-Fri | 30 |
| Developer: mornings | 09:00, Mon-Fri | 10 |
| Developer: back after 8 days off | 09:00 Mon-Fri, skipping days 3-10 of the window | 4 |

**Client model.** A client sends its cursor, which is the timestamp of its last response, and adopts the server's tip as its own.

**Bytes per response.** The GPRE framing plus the packs: 8 bytes when empty, or 8 + 24 + the pack length for one pack. The `.idx` is excluded.

**Ideal per response.**
- 8 bytes if the client's tip is the server's tip.
- Otherwise 32 bytes plus one non-thin upstream fetch (protocol v2, `have` = client tip, `want` = server tip).
- For a cold clone, 32 bytes plus the seed pack, plus the fetch from the seed's tip to the server's tip minus 32.
- In opus's run, the seed term was opus's seed slab: the upstream's full pack at the seed, with deltas re-pointed.

**Reporting.**
- Warm rows exclude each client's first request, its initial clone.
- Per client: requests, responses that carried a pack, source slabs per pack (average and maximum), MiB received, MiB ideal, and the ratio.
- For cold clones: packs per response, source slabs (average and maximum), the size range, and mean and maximum overhead.
- Also: pipeline runs by tier, live-tip updates, extra storage, and wall time.

**Recommended guard** (not in opus's run, which compares totals only): flag every response smaller than its ideal. Such a response told a client it was up to date, or sent it too little. Completeness of individual answers is covered by T-05 and T-10 to T-14.

**opus (P3).**

| Client | Prefetches | Packs | Source slabs per pack (avg / max) | MiB received | MiB ideal | × ideal |
|---|---:|---:|---:|---:|---:|---:|
| CI agents: fresh clone every 30 min | 672 | 672 | 8.8 / 23 | 2,472.9 to 2,477.8 each | +0.04 mean, +0.14 max over ideal | 1.0000 |
| Agent VM: every minute | 20,159 | 2,839 | 1.2 / 8 | 83.5 | 83.1 | 1.00 |
| Agent VM: every 15 min | 1,343 | 1,053 | 2.9 / 31 | 46.4 | 46.1 | 1.01 |
| Agent VM: hourly maintenance | 335 | 321 | 3.3 / 4 | 26.5 | 26.4 | 1.01 |
| Dev laptop: hourly, weekdays | 109 | 105 | 3.9 / 12 | 13.2 | 13.1 | 1.01 |
| Developer: 3 fetches a day | 29 | 29 | 5.2 / 8 | 8.6 | 8.4 | 1.02 |
| Developer: mornings | 9 | 9 | 8.2 / 10 | 6.0 | 5.9 | 1.02 |
| Developer: back after 8 days off | 3 | 3 | 9.7 / 13 | 4.4 | 4.3 | 1.02 |

**Run facts (P3).**
- 1,344 pipeline runs: 1,008 × 15m, 280 × 1h, 28 × 6h, 14 × 12h, 12 × 24h, 2 × 168h.
- 3,308 live-tip updates.
- Wall time 5 min 50 s, of which the seed took about 80 s.
- End state: 277 retained slabs (74 × 15m, 156 × 1h, 27 × 6h, 13 × 12h, 7 × 24h), which is 29.5 MiB of extra storage, and a 38 KB manifest.
- The same table came out identically before and after the two review fixes in §8.

**Before P3 (P2).** Prefetch then sent every slab newer than the cursor, and rollups replaced their parts.

| Client | MiB received | × ideal | Packs |
|---|---:|---:|---:|
| Agent VM: every minute | 184.5 | 2.22 | 3,716 |
| Agent VM: every 15 min | 100.8 | 2.19 | 1,174 |
| Agent VM: hourly maintenance | 65.7 | 2.49 | 321 |
| Dev laptop: hourly, weekdays | 29.0 | 2.21 | 105 |
| Developer: 3 fetches a day | 21.4 | 2.54 | 29 |
| Developer: mornings | 12.8 | 2.18 | 9 |
| Developer: back after 8 days off | 6.7 | 1.57 | 3 |
| CI cold clones | +0.82 MiB mean, +1.96 max over ideal | 1.0003 | 672 |

**Diagnostic.** `CLIENTS_TRACE=<substring of a client name>` prints every response's cursor, bytes, ideal and source slabs. At P2, a breakdown by tier showed where the extra bytes came from. For the every-minute poller, 111.0 of 184.5 MiB came from re-sent L0 slabs. For hourly clients, most came from rollups that re-sent hours they already had.

### B-12 Prefetch plan build cost

**Procedure.** Starting from the W14 end state, build the one-pack answer for covers of N slabs. opus used the `repack` CLI over each cover's slabs.

| Cover | Input → output | opus (P3) |
|---|---|---|
| Last hour (4 slabs) | 99.8 KiB → 36.4 KiB | < 5 ms |
| Last day (9 slabs) | 1.09 MiB → 581 KiB | 40 ms |
| Everything since the last weekly (14 slabs) | 3.96 MiB → 2.40 MiB | 0.15 s |

Each plan is built once per (boundary, manifest generation) and shared by every client.

### B-13 Content scan

Counting types, delta depths and GitHFS pointer coverage of the SG baseline (`inspect slab`, 32 threads) takes 0.63 s in opus (P1).

## 8. Bugs these tests found in opus

| Found by | Bug | Fix |
|---|---|---|
| S-04 | The tip's commit closure returned 413 because request arenas had a fixed capacity | Arenas grow for commit expansion (outside the hot path) and shrink back after |
| B-09 with `--alloc-guard trace` | 4 hot-path allocations in `OidSet::shrink` | Fixed; strict mode clean afterwards |
| B-02 | REF_DELTA resolution ran in rounds: 51 rounds, 50 s | A waiter table keyed by base id brought it to 7.0 s |
| T-17 on Azurite | The Shared Key signature omitted the endpoint's path, which path-style endpoints such as Azurite need | The canonical resource now includes it |
| B-06 | Concatenated 15m slabs were 8.3× one fetch over a week | P2 repack: 2.63 vs 2.65 MiB |
| P2 rework of the live tip | An edge/pipeline ordering bug could make a client miss a sealed slab | Fixed in P2 |
| B-11 | Warm clients received 2.2-2.5× the ideal | P3 brought this to 1.00-1.02×. Changes: retained slabs, cascading rollups, commit-derived timestamps, one L0 slab per push, and a repack per plan. |
| Code review, now covered by T-13 | Timestamps were folded along first parents only, so a `git pull` merge could be stamped below the tip and a client told it was up to date | Lamport-style stamps over all parents, computed from information both sides share |
| Code review, now a unit test | Unsealed live slabs could age out of the timeline while the pipeline was away | Only sealed slabs age out |

## 9. Results matrix

Fill in one column per implementation. For opus, the stage is in parentheses. "—" means not yet run.

| ID | Metric | opus | argon | astra | fable | sol |
|---|---|---|---|---|---|---|
| T-00 | Own suite / clippy with the deny-lints / `unsafe` isolated | 49 pass / clean / 2 crates (P3) | — | — | — | — |
| T-00 to T-18 | Conformance scenarios passed (T-04 only if a `.idx` is sent) | 19 / 19 (P3) | — | — | — | — |
| S-01 | Full ADO pack index equals git's | ✅ 12,083,668 (P1) | — | — | — | — |
| S-02 | Baseline `.idx` byte-identical to git's | ✅ (P2) | — | — | — | — |
| S-03 | gvfs-helper cold clone: 1 pack, complete | ✅ (P1) | — | — | — | — |
| B-02 | Index the full ADO pack (git: 107 s) | 7.0 s (P1) | — | — | — | — |
| B-03 | Seed: index + MPHF for 9.22M objects | 2.4 s + 0.35 s (P1) | — | — | — | — |
| B-04 | Index bytes per object | 40.5 (P1) | — | — | — | — |
| B-05 | 15m run (469 objects) | 0.04 s (P2) | — | — | — | — |
| B-05 | Append a week to the baseline | 4.3 s (P2) | — | — | — | — |
| B-06 | Week rollup / one fetch | 2.63 / 2.65 MiB = 0.99× (P2) | — | — | — | — |
| B-06 | Daily rollup / one fetch | 1.01-1.10× (P2) | — | — | — | — |
| B-06 | Baseline growth for a week / fresh full fetch | 2.22 / 2.35 MiB (P2) | — | — | — | — |
| B-07 | Push → servable (SG, 469 objects) | 0.87 s (P1) | — | — | — | — |
| B-08 | gvfs-helper cold clone (native clone: 114 s) | 31 s (P1) | — | — | — | — |
| B-09 | GET blob: req/s, p50 | 1.02M, 59 µs (P1) | — | — | — | — |
| B-09 | GET tree: req/s, p50 | 265K, 190 µs (P1) | — | — | — | — |
| B-09 | POST 4000 ids: req/s, GB/s | 1,106, 10.0 (P1) | — | — | — | — |
| B-09 | sizes, 4000 ids: req/s | 19.2K (P1) | — | — | — | — |
| B-09 | Prefetch up to date: req/s | 1.09M (P1) | — | — | — | — |
| B-09 | Prefetch cold: aggregate GB/s (8 connections) | 48.5 (P1) | — | — | — | — |
| B-09 | Hot-path allocations under load | 0 (P1) | — | — | — | — |
| B-10 | Tip commit closure (SG) | 137.8 MB in 0.70 s (P1) | — | — | — | — |
| B-11 | Warm clients, × ideal (worst pattern) | 1.02 (P3) | — | — | — | — |
| B-11 | Every-minute poller, × ideal | 1.00 (P3) | — | — | — | — |
| B-11 | CI cold clone overhead over ideal | +0.04 MiB (P3) | — | — | — | — |
| B-11 | Packs per response (max) | 1 (P3) | — | — | — | — |
| B-11 | Extra storage for warm answers | 29.5 MiB, 277 slabs (P3) | — | — | — | — |
| B-12 | Plan for a day of commits | 40 ms (P3) | — | — | — | — |

## 10. Not covered

- **A live Azure account.** Only Azurite was tested.
- **The VFS for Git (C#) client itself.** Only its `.idx` contract was checked, as byte equality with git.
- **HTTP/2 and TLS.** opus serves plain HTTP/1.1 only.
- **Force pushes and rewritten history.** Timestamps and L0 handling in this case have no test.
- **An edge one manifest behind while a push merges a branch forked inside the just-sealed range.** This is a documented residual risk for timestamps.
- **Concurrent pipeline runs racing to publish.** CAS is tested only at the store level.
- **Crashes mid-run and partial uploads.** These are safe by construction (unpublished uploads are garbage) but untested.
- **Upstream economy.** Rollups make no fetch only by construction. Recommended check: at U-http, count upload-pack requests and objects per pipeline run. There should be at most one fetch per run, holding only objects newer than the manifest's tip, and live-tip fetches should start at the previous L0 tip.
- **Per-response completeness inside the B-11 replay.** See the recommended guard there.
- **The bake-off's resource metrics.** Memory (RSS), CPU µs per request, perf counters and CPU pinning (`~/gvfs-server-benchmark/REPORT.md`) were not measured in this session.
- **Load numbers on P2/P3.** B-09 is P1 only.
- **Long-running soak tests.**
- **The bake-off validator** (`~/gvfs-server-benchmark/tools/validate.py`) was not run. Its prefetch rule (no non-pointer blobs) differs from opus's content policy.

## Appendix A. Running opus's suite

opus test names for each T test:

| ID | opus test |
|---|---|
| T-01 | `ingest_cli_matches_git_index`; REF_DELTA chains: `index_matches_git_with_ref_deltas` |
| T-02 to T-13, T-18 | `serve_end_to_end_with_live_l0` |
| T-14 | `pyramid_rollups_and_weekly_rebuilds` |
| T-15 | `gvfs_helper_against_the_daemon` |
| T-16 | `smart_http_upstream_v2_and_v0` |
| T-17 | `tests/azure.rs`: `azure_store_operations`, `azure_pipeline_and_serve` |

```sh
cd /home/muks/gvfs-daemon/opus
cargo build --release
cargo test --workspace --release          # T-00..T-18. T-15 needs ~/git/bin-wrappers/git or GVFS_TEST_MSGIT.
cargo test --workspace                    # the same in debug (overflow checks)
GVFS_TEST_AZURITE=$(command -v azurite-blob) cargo test --release --test azure    # T-17
cargo clippy --workspace --all-targets

# S-01 / B-02
P=/home/muks/SearchGold/.git/objects/pack/pack-ec111cc94ed5a20453e303e45695428e4ac3a35b
time ./target/release/gvfs-daemon ingest --pack $P.pack --out /home/muks/scratch/ingest --threads 32 --verify-idx $P.idx
time git index-pack --threads=32 -o /home/muks/scratch/git.idx $P.pack

# Scale store (S-02..S-05, B-03, B-07..B-10): a bare repo sharing SearchGold's objects, main at the seed.
git clone -q --bare --shared /home/muks/SearchGold /home/muks/scratch/up.git
./target/release/gvfs-daemon pipeline --store fs:/home/muks/scratch/store --upstream /home/muks/scratch/up.git --tier 15m
./target/release/gvfs-daemon serve --store fs:/home/muks/scratch/store --upstream /home/muks/scratch/up.git \
    --bind 127.0.0.1:18081 --cache-dir /home/muks/scratch/cache --alloc-guard strict --prefetch-idx never
git init -q /home/muks/scratch/client && git -C /home/muks/scratch/client config remote.origin.url http://127.0.0.1:18081/org/repo
time ~/git/bin-wrappers/git -C /home/muks/scratch/client gvfs-helper --cache-server=disable prefetch     # B-08 (a)
time git clone -q --bare --no-local /home/muks/scratch/up.git /home/muks/scratch/native.git              # B-08 (b)
benches/load.sh 127.0.0.1:18081 /home/muks/scratch/up.git 10      # B-09; WARM_TS=<cursor> adds the warm row
./target/release/gvfs-daemon inspect slab /home/muks/scratch/store/slabs/base-*.mphf --write-idx /home/muks/scratch/base.idx  # S-02 (compare with git index-pack)

# B-06
BIN=target/release/gvfs-daemon benches/rollup.sh /home/muks/SearchGold 2026-10-17 /home/muks/scratch/rollup

# B-11 (about 6 minutes; leaves about 8.5 GB in --work)
cargo build --release --example clients
./target/release/examples/clients --repo /home/muks/SearchGold --from 2026-10-10 --days 14 \
    --work /home/muks/scratch/sim 2>sim.log >sim.md
```

## Appendix B. P1 measurements superseded later

These were measured on the P1 design, where slabs were the upstream's packs verbatim, rollups were re-fetched or concatenated, and prefetch concatenated whole slabs. They are kept as reference points for implementations with that design.

- **SG pipeline run.** `15m`, then `1h` (929 objects), then `168h`. The weekly baseline built by concatenation had 9,223,025 objects (2.42 GiB) and took 1.9 s. The edge followed generations 2 to 4. After the rebuild, a gvfs-helper cold clone took 31 s and returned 1 pack.
- **The cold pack.** P1's cold pack was the upstream's pack verbatim. It was byte-identical to the pack a native clone received (trailer `83da3df7…`).
- **First load run** (before `benches/load.sh`; GET drew random objects of every type):

  | Workload | Throughput | Latency |
  |---|---|---|
  | GET loose objects | 443K req/s | p50 118 µs |
  | GET deltified trees | 239K req/s | p50 222 µs |
  | POST 4000-object batches | 644 req/s, 7.4 GB/s | p50 24.7 ms |
  | sizes, 4000 ids | 18.8K req/s | |
  | prefetch, warm (266 KB L0 pack) | 71K req/s | p50 838 µs |
  | prefetch, up to date | 1.09M req/s | |
  | prefetch, cold, 8 connections | 50 GB/s aggregate | |

  The later `load.sh` run superseded the GET, POST and sizes rows. The warm-prefetch row in B-09 comes from this run.
