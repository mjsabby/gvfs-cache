# gvfs-daemon — tests and benchmarks (fable), as a standard for comparing implementations

This document lists every test and benchmark that was run on the `fable` implementation
(`/home/muks/gvfs-daemon/fable`), written so the same procedure can be applied to any other
implementation of the unified-binary GVFS architecture. Each item states **what** is checked,
**how** (exact commands or an equivalent an implementation must provide), the **pass criterion
or metric**, and fable's **result**. Section 9 is a blank reporting template.

Conventions used throughout:

* `BIN` = the implementation's binary. Where fable subcommands are named (`pipeline`, `serve`,
  `plan`, `inspect`, `lookup`, `client-prefetch`), another implementation needs an equivalent
  with the same observable behaviour; the required behaviour is spelled out each time.
* **Bytes of a prefetch response** = `pack_len + idx_len + 32` (8-byte GPRE header + one 24-byte
  per-pack header). A response with zero packs counts as 0 bytes and is not a "fetch with data".
* Pack validity is always decided by **stock git**, never by the implementation itself (see 3.0).
* Unless stated otherwise, timings are single runs on a warm page cache over loopback, measured
  with `curl -w '%{time_total}'` or `time`; they are not medians. Treat differences under ~20%
  as noise.

---

## 0. Environment

| item | value |
|---|---|
| host | `llm` — AMD Ryzen 9 9950X (16C/32T), 89 GiB RAM, NVMe; Linux 7.3.0 |
| toolchain | rustc/cargo 1.97.1 (crate `rust-version = 1.89`), git 2.53.0, Python 3.14 |
| note | `/tmp` on this host is a 45 GB tmpfs: multi-GB stores must live under `/home`, not `/tmp` (hard links across the two also fail) |
| note | the shell injects `GIT_CONFIG_*` (`safe.bareRepository=explicit`): drive bare repositories with `git --git-dir=…`, not `git -C` |

Lint gate every implementation was asked to satisfy (from `opus/DESIGN.md`):
`#![forbid(unsafe_code)]` in the daemon crate (any `unsafe` isolated in tiny audited crates), and
clippy `deny` of `unwrap_used, expect_used, panic, indexing_slicing, arithmetic_side_effects,
todo, unimplemented, unreachable`. Fable: passes `cargo clippy --workspace --all-targets` with
zero warnings; two `unsafe` blocks total (`gvfs-mmap`: `mmap(2)`; `gvfs-alloc-guard`:
`unsafe impl GlobalAlloc`).

---

## 1. Fixtures

| id | fixture | identity / size | used for |
|---|---|---|---|
| F1 | **SearchGold** (`/home/muks/SearchGold`, a real GitHFS monorepo clone) | `main` = `4d12e2c7915577f43351ac82b9ca15bd7788be89`, 658,211 commits on main; 12.0M objects in the clone's single pack (3.27 GB on disk), **9,224,930 reachable from main (≈2.6 GB packed)**; 7.5M trees of which 4.78M are deltified (0.37 GB as deltas, >12 GB if stored whole); 3.34M `~~Git-hfs 1.0` pointer blobs; 80,713 refs; only REF_DELTA entries in the upstream pack (ADO-served) | scale correctness, all performance numbers, replay benchmark |
| F2 | **synthetic repo** (built by the e2e test) | 12 dirs × 25 pointer-blob files + 1 real file, 16 commits of 4 edits, repacked; later commits add 3 edits each; a revert; a feature branch | automated lifecycle test |
| F3 | **github.com/git/git** over smart HTTP (anonymous) | `master` = `7e34d8174dcda3435d1404c9a30bef46afe6c13e` at test time: 409,658 objects, 313 MB upstream pack, 162,712 real (non-pointer) blobs | transport verification against a public server; non-HFS repository behaviour |
| F4 | **github.com/octocat/Hello-World** | 7 objects | smoke test of the HTTP client |

Reference values that other implementations must reproduce exactly (they are properties of F1,
not of fable): object count reachable from `main` = **9,224,930**; `git rev-list --objects main
| wc -l` is the oracle.

Repository activity profile of F1 (needed to interpret the replay numbers): ≈230 first-parent
commits/day on average, peak 519/day, peak 49/hour. Bytes of net-new objects per commit when
fetched as one window: ≈25 KB for a single commit (its trees have nothing to delta against),
≈2.4 KB/commit for a whole day, ≈1.6 KB/commit for a whole week.

---

## 2. Static quality gates

| id | check | command | fable |
|---|---|---|---|
| S1 | no warnings under the deny list | `cargo clippy --workspace --all-targets` | 0 warnings |
| S2 | unsafe isolation | `grep -rn "unsafe" crates/` | 2 blocks, both outside the daemon crate |
| S3 | dependency surface | `cargo tree -e normal --depth 1` | tokio, hyper(+util), bytes, sha1, crc32fast, flate2(zlib-rs), rustls+webpki-roots, memmap2, clap; everything else in-house (pkt-line, protocol v2 client, pack/idx/delta, MPHF, GPRE, JSON, HFS pointers, HTTP/1.1 client) |
| S4 | unit tests | `cargo test --workspace` | 37 lib tests + 2 integration tests + 1 probe, all green (~5 s) |

---

## 3. Automated correctness suite (synthetic repo F2; `cargo test -p gvfs-daemon --test e2e`)

One test drives the whole lifecycle against a real `git` and the real binary (`serve` is
spawned as a child process on an ephemeral port with `--alloc-guard count`). Pipeline config
under test: fetch-built rollups from `h1`, leaves kept below `h24`, GC grace 0.

### 3.0 The pack-validity oracle (used by every check below)

```
git index-pack --verify p.pack           # with the served .idx next to it: pack and idx consistent
git index-pack g.pack && cmp g.idx p.idx # the served idx is BYTE-IDENTICAL to git's own
git fsck --connectivity-only             # in a bare repo holding only the served pack(s), ref set to the tip
git rev-list --objects --all | wc -l     # equals the same count in the source repository
git verify-pack -v p.idx                 # lists exactly the expected objects (POST /gvfs/objects)
```

An implementation that ships its own `.idx` must pass the byte-identity check; one that does
not must at least pass `index-pack --verify` on the pack alone.

### 3.1 Checklist

| id | step | pass criterion | fable |
|---|---|---|---|
| C1 | genesis (`pipeline --tier m15` on an empty store) | one baseline slab whose object count = `rev-list --objects --all`; passes 3.0 | ✓ |
| C2 | three incremental runs, the third after `git revert` | the revert run reports duplicates dropped > 0 and keeps exactly **1** new object (the revert commit itself); slabs are pairwise disjoint | ✓ |
| C3 | `pipeline --tier h1` | the three m15 slabs become one h1 slab, re-fetched as one window (`have` = tip before the window); object count within 1 of the members' sum; its `end_ts` is **newer** than its last member's; members remain on disk as leaves; everything unreferenced is swept | ✓ |
| C4 | cold clone `GET /gvfs/prefetch?lastPackTimestamp=0` | **exactly 1 pack** in the GPRE stream; passes 3.0 including fsck connectivity and equal object count; `X-GVFS-Pack-Start: 0`, `X-GVFS-Pack-End: <latest>` | ✓ |
| C5 | warm client at the h1 window start | receives the h1 slab verbatim (byte-equal to the file: passthrough) | ✓ |
| C6 | warm client inside the window (holds leaf 1) | receives **only leaves 2+3**, spliced into one pack (object count = sum), `X-GVFS-Pack-Start` = leaf 2's start; passes 3.0 | ✓ |
| C7 | client at the latest timestamp / holding every leaf | empty GPRE (`GPRE\x01` + count 0) | ✓ |
| C8 | same client with `&compact=1` | receives the whole h1 rollup (byte-equal), headers = the rollup's interval | ✓ |
| C9 | `POST /gvfs/objects` {blob, subtree, unknown id} | 200; `verify-pack -v` lists exactly those 2 objects | ✓ |
| C10 | `POST /gvfs/objects` {HEAD, commitDepth 1} | pack holds commit + root tree + every subtree (`ls-tree -r -d` count + 2) | ✓ |
| C11 | `POST /gvfs/sizes` [blob, commit] | sizes equal `git cat-file -s` | ✓ |
| C12 | `GET /gvfs/objects/{oid}` for a blob, a tree, a commit | zlib-inflates to `"<type> <size>\0"+content` whose SHA-1 is the oid | ✓ |
| C13 | `GET /gvfs/objects/ffff…` | 404; no upstream fetch of objects (`gvfs_miss_fetched_objects_total` stays 0) | ✓ |
| C14 | objects on a **branch the pipeline never saw** (`GET` commit; `POST` its tree + unknown id) | 200 both; exactly the branch's own objects were fetched from the upstream (`rev-list --objects feature ^main` count); served from the live slab afterwards | ✓ |
| C15 | live sync: commit upstream, no pipeline run | within the poll interval the new commit is servable, the live slab appears as a pack for warm clients (passes 3.0); after a pipeline publish the server reloads the manifest and drops the live slab; a cold clone is still 1 pack and valid | ✓ |
| C16 | client hook cold clone into an empty bare repo (`client-prefetch --midx`) | exactly one `prefetch-0-<end>.pack` + `.idx`, a `multi-pack-index`, `fsck --connectivity-only` clean, object count = source `main` | ✓ |
| C17 | hook incrementals ×2, then again with nothing new | 3 packs on disk; "nothing new" costs 0 bytes | ✓ |
| C18 | hook over budget after an h1 rollup (`--budget 2`) | one `compact=1` re-fetch, the two m15 packs unlinked, exactly {cold pack, h1 pack} remain; fsck clean | ✓ |
| C19 | **allocation audit** after all of the above | `/metrics` `gvfs_alloc_hot_path_total == 0` (every endpoint ran inside a hot-path region with the allocator sealed) | ✓ 0 |
| C20 | `pipeline --tier h168` with `--baseline splice` | pyramid empty, one weekly, new baseline passes 3.0 and holds every object of `main` | ✓ |

Other automated tests: `tests/alloc_probe.rs` (a pre-promoted `BytesMut` can be split into
frames without allocating — the trick every zero-copy body relies on); unit tests for the
MPHF (build + lookup + miss), idx writer/reader, delta apply, zlib splice/`adler32_combine`,
HFS pointer parser (LF/CRLF, 64/66-hex), pkt-line, pack headers, manifest selection with
leaves, tombstoning/compaction rules of the client hook, the miss queue, the frame ring.

---

## 4. Correctness at scale (F1, F3 — run by hand)

| id | check | procedure | pass criterion | fable |
|---|---|---|---|---|
| R1 | full baseline of F1 | `BIN pipeline --tier m15 --store S --upstream ~/SearchGold --workers 16` on an empty store | object count = 9,224,930; `git index-pack --verify` of the slab with its idx succeeds; idx byte-identical to git's | ✓ (verify 37 s with 16 threads; `inspect --verify`, which measures every object, 11 s) |
| R2 | spliced cold clone of F1 | prefetch with 2 slabs (baseline + an m15 of 1,000 commits) → 2.9 GB stream | `git index-pack --verify` on the spliced pack + merged idx | ✓ |
| R3 | HEAD commit closure of F1 | `POST /gvfs/objects {HEAD, commitDepth 1}` → 136 MB, 411K trees | `git index-pack --stdin` succeeds | ✓ |
| R4 | miss path at scale | commit of a non-main ref absent from the store (`rev-list --objects <ref> ^main` = 8) | `GET` 200; `POST` closure (136 MB) index-packs; exactly 8 objects fetched | ✓ |
| R5 | loose delta object at scale | a tree stored as OFS_DELTA (`lookup` says so) via `GET /gvfs/objects/` | inflates and SHA-1s to its id | ✓ |
| R6 | smart HTTP against a public server (F3) | genesis from `https://github.com/git/git` with no token | pack + idx pass fsck connectivity in a bare repo; incremental step from a genesis pinned at `master~300` (a non-tip `want`) fetches only the delta | ✓ 409,658 objects, 9.2 s incl. download; incremental 7,225 objects / 10 MB in 4.8 s |
| R7 | smoke (F4) | genesis from Hello-World | 7 objects, 0.5 s | ✓ |

Not verified (no credentials available): Azure DevOps as an upstream — protocol v2 support and
whether ADO accepts non-tip `want`s (fable falls back to splicing / to ADO's own
`POST /gvfs/objects` if it refuses).

---

## 5. Performance benchmarks (F1 unless noted)

Procedure for the server-side numbers: start `BIN serve --bind 127.0.0.1:P --store S
[--upstream ~/SearchGold] --poll-secs 0 --alloc-guard count`, wait for `/healthz`, issue the
request with `curl -s -o /dev/null -w '%{http_code} %{time_total}s %{size_download}'`, read
`/metrics` afterwards. Report the first request and a repeat.

| id | benchmark | metric | fable |
|---|---|---|---|
| P1 | full ingest of F1 (fetch from local clone → sealed slab + idx + O(1) index) | wall time, outputs | **82 s** with 16 workers; 9,224,930 objects; 273K objects de-deltified (+3 MB); idx 280 MB; MPHF 153 MB (≈2 s of the total for 12M keys) |
| P2 | incremental ingest, 1,000 commits | wall time | **0.15 s**; 9,338 objects fetched, 3 revert duplicates dropped |
| P3 | h1 rollup of two 500-commit slabs: re-fetch vs splice | time; bytes | 0.155 s; same 9,335 objects in **1.64 MB** (fetched) vs 1.93 MB (spliced) |
| P4 | weekly delta: re-fetch vs splice (from the replay) | bytes of one week | **2.9 MB** fetched vs **24.0 MB** spliced (8×) |
| P5 | cold-clone prefetch, 2 slabs (2.6 GB + 1.6 MB), checksums unknown | time to last byte | **1.24 s** (SHA-1 over 2.6 GB + 280 MB merged idx while streaming) |
| P6 | same selection, checksums cached | time | **0.27 s** ≈ 10.7 GB/s over loopback (pure zero-copy `writev` from mapped pages) |
| P7 | first cold clone after boot (background pre-hash of the cold selection) | time of the request; background time | request **0.23 s**; warm thread 1.05 s for 2.88 GB while the server is already serving |
| P8 | HEAD commit closure `POST /gvfs/objects` (411K trees, 136 MB) | time; allocations | **0.85 s**; 0 hot-path allocations |
| P8b | same, throttled client (`curl --limit-rate 2M`, 5 s then abort) | allocations, ring stalls | 0 allocations; `gvfs_frame_ring_busy_total` 0 |
| P9 | loose pointer blob / `POST /gvfs/sizes` / 523-object batch | end-to-end time incl. curl | 3–8 ms; 0 allocations |
| P10 | miss fetch (branch commit absent from the store) | first request; repeat | **0.77 s** (one `git upload-pack` round, exactly the 8 missing objects); 0.4 ms afterwards |
| P11 | loose delta tree at scale (`GET /gvfs/objects/<tree stored as delta>`) | time | 0.27 ms |
| P12 | smart HTTP genesis of F3 with three de-deltification policies | slab size, time | default (de-deltify non-trees ≤ 4 KiB): **332 MB / 9.2 s**; de-deltify all non-trees: 1,648 MB / 15.5 s; keep all upstream deltas: 313 MB / 13.7 s |
| P13 | replay pipeline cost | pipeline CPU-time for 14 days of F1 in 15-minute steps (1,344 runs) | ≈27 s per configuration (≈20 ms/step); genesis baseline 85–90 s |

Interpretation notes an implementation should also report:

* P1 vs `git index-pack --verify` (37 s on the same data): the full ingest is ≈2.2× a bare
  verification pass.
* P5→P6: the difference is exactly the hashing; a design that must rewrite bytes (re-delta,
  re-deflate) cannot approach P6.
* P12: without a size cap, de-deltifying blobs blows a non-HFS repository up 5×; with the cap
  the slab is +6% over the upstream pack. On F1 (pointer blobs) the policy costs +3 MB on 2.6 GB.

---

## 6. Replay benchmark — bytes and pack files per client behaviour

### 6.1 What it answers

For each server configuration: how many bytes and how many pack files do typical clients
accumulate over N real days, is a cold clone always one pack, and how large is the pipeline's
own upstream traffic. It runs the **real** pipeline binary over the **real** history and
evaluates clients against the **real** prefetch selection, so it benchmarks the selection and
rollup policy, not a model of it.

### 6.2 Required implementation interface

An implementation needs three things to be replayable with the same harness
(`fable/tools/replay_bench.py`, 300 lines of Python, no dependencies):

1. `pipeline --tier <m15|h1|h6|h12|h24|h168> --store S --upstream <bare repo>` that can be
   invoked once per step and whose notion of "now" can be overridden (fable: `GVFS_FAKE_NOW=<unix
   seconds>`; under the fake clock unreferenced files are swept immediately since mtimes are
   wall-clock).
2. A **selection oracle**: `plan --store S --since <ts> [--compact]` printing JSON
   `{packs, pack_len, idx_len, slabs:[…], leaves, start_ts, end_ts}` — exactly what
   `GET /gvfs/prefetch?lastPackTimestamp=<ts>[&compact=1]` would stream and which interval
   headers it would carry, without streaming it (≈30 ms in fable).
3. Log lines the harness greps: `fetched <bytes>` per upstream fetch, and `rollup: splicing` /
   `rollup: re-fetching` per rollup (or adapt the three `startswith` checks).

### 6.3 Protocol

* **History**: `git log --first-parent --since=<N+1>.days --format='%ct %H' --reverse` of the
  source repository. `t_end` = the last commit's timestamp rounded **up** to the next 15-minute
  boundary; `t0 = t_end − N·86400`; genesis commit = the last commit with `ts ≤ t0`.
* **Genesis**: a `--shared` bare mirror with `main` pinned to the genesis commit;
  `pipeline --tier m15` at fake time `t0` builds the baseline (reused across configurations via
  `cp -al`). The per-configuration mirror's `main` is also pinned to the genesis commit before
  step 1 (a fresh mirror otherwise points at the source HEAD and the first step would ingest the
  whole window).
* **Steps**: every 900 s from `t0` to `t_end`: advance the mirror's `main` to the last commit
  with `ts ≤ t`; run `pipeline` with the tier dictated by the clock:
  `minute≠0 → m15; hour==0 ∧ Sunday → h168; hour==0 → h24; hour%12==0 → h12; hour%6==0 → h6;
  else h1` (each tier also performs the lower tiers' work).
* **Clients** (each starts with a cold clone at its first opportunity, then fetches when due,
  applying `plan(cursor)` and setting `cursor = end_ts` of what it received):
  `agent-24x7` every step; `dev-diurnal` weekdays at 09:00/11:00/14:00/17:00 UTC;
  `laptop-weekly` Mondays 09:00; `cold-clone` a fresh `plan(0)` every hour on the hour;
  `stale` one `plan(1)` at the end (a client older than every retained weekly).
* **Local pack models** per stateful client:
  *stock* — keeps every pack ever received;
  *hook* — on each receipt, unlink any local pack whose `[start,end]` lies inside another local
  pack's (fable's `client-prefetch` rule; a received superset tombstones what it subsumes);
  *hook + budget B* — additionally, when more than B packs remain, keep the oldest ⌊B/2⌋
  (counting the start-0 cold pack) and collapse the rest with one `plan(oldest remaining start,
  compact=1)`, only if that collapses ≥ 2 packs; bytes counted separately as "compaction".
* **Configurations**: `<leaf-tier>:<rollup-fetch-from>` — `never:h24` (fable default), `h6:h24`,
  `h24:h24`, `h168:h24`, `never:never` (pure splicing). An implementation without these knobs
  runs its single policy and reports it under its own label.
* **Reported metrics**: per configuration — pipeline wall time, upstream bytes fetched, rollups
  spliced/fetched, store size. Per client — fetches with data, initial clone bytes, incremental
  bytes (total and /day), max single fetch (bytes, slab count), leaf-only fetches, stock pack
  count, hook peak/final pack count, budget-hook compactions/bytes/peak/final. Cold clones —
  count, bytes min/avg/max, slabs spliced avg/max; stale-client bytes.
* **Command**: `tools/replay_bench.py --src ~/SearchGold --days 14 --out ~/sim --config never:h24
  --config h6:h24 --config h24:h24 --config h168:h24 --config never:never --jobs 5`
  (`HOOK_BUDGETS=20,100` adds budget columns; `REPLAY_DEBUG=agent-24x7` traces one client;
  `results.json` holds everything). Cost: ≈90 s genesis + ≈40 s for five configurations in
  parallel.

### 6.4 Fable results — 14 days of F1 (2026-10-10 17:30 → 10-24 17:30 UTC), 3,258 commits

| config | pipeline time | upstream bytes | rollups splice/fetch | store size |
|---|---|---|---|---|
| never:h24 | 27 s | 47.3 MB | 392 / 14 | 3.04 GB (one baseline) |
| h6:h24, h24:h24, h168:h24 | ≈27–35 s | 47.3 MB | 392 / 14 | same |
| never:never | ≈34 s | 47.3 MB | 406 / 0 | same |

| config | client | fetches w/ data | initial clone | incremental | /day | max single fetch | packs: stock | hook: peak / final | hook B=20: compactions / bytes / peak / final |
|---|---|---|---|---|---|---|---|---|---|
| never:h24 | agent-24x7 | 1130 | 2874.6 MB | 137.2 MB | 9.8 MB | 2.9 MB (1 slab) | 1130 | **17 / 17** | 0 / 0 / 17 / 17 |
| never:h24 | dev-diurnal | 41 | 2874.6 MB | 55.6 MB | 4.0 MB | 4.7 MB (6 slabs) | 41 | 9 / 9 | 0 / 0 / 9 / 9 |
| never:h24 | laptop-weekly | 3 | 2874.6 MB | 7.3 MB | 0.5 MB | 4.7 MB (6 slabs) | 3 | 3 / 3 | 0 / 0 / 3 / 3 |
| h6:h24 | agent-24x7 | 1079 | 2874.6 MB | 106.3 MB | 7.6 MB | 2.9 MB | 1079 | 31 / 28 | 47 / 27.2 MB / 21 / 18 |
| h24:h24 | agent-24x7 | 1074 | 2874.6 MB | 59.8 MB | 4.3 MB | 2.9 MB | 1074 | 92 / 67 | 73 / 74.7 MB / 21 / 14 |
| h24:h24 | dev-diurnal | 41 | 2874.6 MB | 37.3 MB | 2.7 MB | 4.7 MB | 41 | 10 / 10 | 0 / 0 / 10 / 10 |
| h168:h24 | agent-24x7 | 1070 | 2874.6 MB | 52.2 MB | 3.7 MB | 2.9 MB | 1070 | 542 / 511 | 79 / 80.4 MB / 21 / 12 |
| h168:h24 | dev-diurnal | 41 | 2874.6 MB | 38.3 MB | 2.7 MB | 4.7 MB (32 slabs) | 41 | 22 / 22 | 2 / 8.4 MB / 21 / 9 |
| never:never | agent-24x7 | 1130 | 2874.6 MB | 193.1 MB | 13.8 MB | **24.0 MB** (spliced weekly) | 1130 | 17 / 17 | 0 / 0 / 17 / 17 |
| never:never | dev-diurnal | 41 | 2874.6 MB | 105.3 MB | 7.5 MB | 27.7 MB | 41 | 9 / 9 | 0 / 0 / 9 / 9 |
| never:never | laptop-weekly | 3 | 2874.6 MB | 33.9 MB | 2.4 MB | 27.7 MB | 3 | 3 / 3 | 0 / 0 / 3 / 3 |

| config | hourly cold clones (336) | bytes min / avg / max | slabs spliced avg / max | stale client |
|---|---|---|---|---|
| any `:h24` | 1 pack each | 2875.1 / 2880.1 / 2885.0 MB | 7.3 / 14 | 2884.6 MB, 1 pack |
| never:never | 1 pack each | 2875.1 / 2899.1 / 2923.2 MB | 7.3 / 14 | 2923.2 MB, 1 pack |

Leaf-tier knob summary (agent-24x7, net-new objects ≈3.4 MB/day):

| `--leaf-tier` | bytes/day, hook only | peak packs | with a 20-pack budget |
|---|---|---|---|
| never (default) | 9.8 MB (2.9× net-new) | 17 | 0 compactions — 9.8 MB |
| h6 | 7.6 MB (2.2×) | 31 | +1.9 MB/day → 9.5 MB |
| h24 | 4.3 MB (1.3×) | 92 | +5.3 MB/day → 9.6 MB |
| h168 | 3.7 MB (1.1×) | 542 | +5.7 MB/day → 9.4 MB |

Invariants any implementation should satisfy on this benchmark: every cold clone and the stale
client receive **exactly 1 pack**; a 15-minute poller's hook pack count is bounded by the
pyramid depth plus retained weeklies (fable: 17) and **resets** at every tier boundary; a weekly
delta built by re-fetching is ≈8× smaller than a spliced one.

---

## 7. Allocation-discipline measurements

The daemon crate runs with a sealing global allocator: after boot, allocations made inside a
hot-path region are counted (`gvfs_alloc_hot_path_total`, `…_bytes_total`) or abort the process
(`--alloc-guard strict`). Every endpoint and every body `poll_frame` is a hot-path region; the
HTTP response object itself (hyper's header map) is built outside it. The metric is the oracle
for the following:

| id | measurement | method | fable |
|---|---|---|---|
| A1 | every endpoint, synthetic repo | e2e check C19 | 0 |
| A2 | 136 MB converted closure, fast reader | P8 + `/metrics` | 0 (was **58** with a single staging buffer, see 8.4) |
| A3 | same, 2 MB/s reader aborted after 5 s | P8b | 0; ring never busy |
| A4 | miss round + retry path | R4 + `/metrics` | 0 (the queue is a pre-sized list; the wait happens outside the hot region) |
| A5 | `BytesMut` wrap behaviour (micro-probe, guard allocator in a test binary) | 64 frames of 64 KiB from a 1 MiB promoted buffer while keeping the last *k* frames alive | k=0: 0 allocations; k≥1: **96 allocations** (every wrap reallocates while a frame is in flight) — the reason a ring of chunks, each written only when fully reclaimed, is required |
| A6 | pre-promoted buffer split | `tests/alloc_probe.rs` | 0 |

---

## 8. Defects the tests and benchmarks surfaced (checklist for other implementations)

These were real bugs in fable found only by the procedures above; each is worth probing in any
implementation.

1. **Supersets never reached current clients (replay, 6).** Rollups inherited their last
   member's `end_ts`, so a poller that was already up to date never received the h1/h24/h168
   that subsumed its packs; its pack count only fell at *quiet* boundaries (peak 43 instead of
   17, unbounded with leaf tiers). Fix: a rollup is sealed with a fresh `end_ts` (strictly after
   every published slab), so a current client receives the superset exactly once. Probe: run the
   replay's `agent-24x7` with `REPLAY_DEBUG` and check the pack count resets every hour/day/week.
2. **Side arena overflow on non-HFS repositories (R6).** A fixed per-worker buffer for
   de-deltified objects, sized for 100-byte pointer blobs, overflowed on git/git. Probe: ingest
   F3.
3. **De-deltifying every non-tree object (P12).** Correct on HFS pointers (+3 MB on 2.6 GB),
   5× blow-up on real blobs (313 MB → 1.65 GB). Probe: compare slab size with the upstream pack
   on F3.
4. **Staging-buffer allocation under backpressure (A2/A5).** A single `BytesMut` staging buffer
   reallocates on every wrap while hyper still holds an earlier frame: 58 allocations on a 136 MB
   response. Probe: P8 with `/metrics`, then P8b.
5. **Harness: fresh mirror at source HEAD.** Without pinning `main` to the genesis commit, a
   window whose first 15 minutes contain no commit makes step 1 ingest the whole window. Probe:
   the first step's `fetched` bytes must be small.
6. **Harness: GC by wall clock.** Under a fake clock superseded baselines never age; replay
   stores grew to 9.3 GB (three baselines). Fixed by sweeping immediately under the fake clock.
7. **Summary-line bug in the hook's output** hid the unlink count when the regular fetch had
   nothing new — caught by C18's string assertion. Assert on the structured facts (files on
   disk), not only on log text.

---

## 9. Reporting template (copy per implementation)

```
Implementation: ________   commit: ________   date: ________   host: llm (9950X) / other: ________

Static gates (S1–S4):      clippy warnings ___  unsafe blocks ___ (where) ___  deps ___  unit tests ___/___
Correctness suite (C1–C20): pass ___/20   failures: ___
Scale correctness:         R1 count ___ (must be 9,224,930)  index-pack --verify ___ s  idx byte-identical [y/n]
                           R2 [y/n]  R3 [y/n]  R4 [y/n]  R5 [y/n]  R6 (git/git) objects ___ fsck [y/n]
Performance (F1):          P1 ___ s (workers ___)   P2 ___ s   P3 ___ MB vs ___ MB   P4 ___ MB vs ___ MB
                           P5 ___ s   P6 ___ s   P7 ___ s   P8 ___ s / ___ allocs   P8b ___ allocs
                           P9 ___ ms   P10 ___ s / ___ ms   P12 slab ___ MB (upstream 313 MB)
Replay (14 d, F1):         per-config table, per-client table, cold-clone table as in 6.4
                           cold clones always 1 pack [y/n]   poller peak packs ___ (resets at tier boundaries [y/n])
Allocation discipline:     A1 ___  A2 ___  A3 ___  A4 ___   (hot-path allocations; 0 expected)
```

---

## 10. Not covered / caveats

* Azure DevOps as a live upstream (protocol v2, non-tip `want`, PAT auth): untested for lack of
  credentials; the smart-HTTP transport was verified against github.com only.
* All server numbers are loopback on one machine with a warm page cache; no multi-client
  concurrency storm, no TLS, no HTTP/2 client, no cold-cache (page-cache-evicted) numbers.
* Timings are single runs; P5–P8 were repeated 2–3 times informally and agreed within ~10%.
* CPU and memory profiles (perf/bpftrace) were not collected in this session; RSS of `serve`
  is essentially the mapped slabs plus `--arenas × --arena-mib`.
* The replay window is the last 14 days of F1 (one Sunday rollover, 3,258 commits); longer
  windows change only the weekly-count terms (one more file per week on warm clients until a
  monthly tier exists).
