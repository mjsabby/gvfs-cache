# gvfs-daemon: five implementations, one standard comparison

**Recommendation: opus**, with the fixes and optimizations made in this branch (`opus/`, commits
`cdda1ae`…`f7d9bef`). It is the only implementation that is correct on every check *and*
fastest or near-fastest on every workload that moves real bytes. Section 1 explains the call
and its trade-offs; sections 3–8 are the evidence; section 9 lists every defect found, per
implementation, including the opus defects this comparison found and fixed. Section 11
studies client-side pack lifecycles; section 12 adds a store mode for real blobs in their own
packs (`--blobs separate`), with sizes travelling beside the trees.

All numbers: AMD Ryzen 9 9950X (16C/32T), 89 GiB, Linux 7.3, rustc 1.97.1, git 2.53, loopback.
Each implementation built as shipped (`cargo build --release`, its own profile). The harness is
in `bench/` (Python, stock git as the only judge of correctness); raw results in `bench/results/`.

---

## 1. Verdict

| | **opus** (final) | sol | fable | astra | argon |
|---|---|---|---|---|---|
| Correct on every check (sections 3–6) | ✅ (after the fixes in §8) | ⚠️ 413s at scale unless tuned; 503 storms under POST load | ✅ | ❌ | ❌ |
| GET blob / commit (req/s, 8 server threads) · CPU per GET | 595K / 791K → **1.01M / 1.02M** · **4.9 µs** | **1.01M / 1.02M** · 5.3 µs | 115K / 117K · 65 µs | 535K / 533K · 8.3 µs | — |
| POST 64 ids / 4000 ids (req/s) | 27.6K / 334 → **29.6K / 352** | 3.5 / 1.5 (503s) | 2.3K / 33 | 20.6K / 309 | — |
| POST /gvfs/sizes, 4000 ids (req/s) | 9.6K → **12.3K** | 5.3K (503s) | 3.1K | 248 | — |
| Cold clones, 8 at a time: clones/s · server CPU per clone | **17–21 · 0.14 s** (50–60 GB/s, bandwidth-bound) | 5.3 · 1.31 s | 4.0 · 1.79 s | 14.1 (1.5× bigger pack) · 0.15 s | — |
| Tip commit closure (505,605 trees) | **0.69 s** | 1.06 s (413 by default) | 0.89 s | 1.55 s (+2.5M blobs, 391 MB) | — |
| Genesis of SearchGold (9.22M objects) | **79 s** | 202 s (after fix) | 83 s | 267 s | — |
| Peak serving RSS | 4.0 GiB | 11.9 GiB | **3.2 GiB** | 11.6 GiB | — |
| Warm-client bytes vs git's ideal (7-day replay, 7 client patterns) | **0.98–1.40×** with day/week supersets (§11); 0.94–1.04× exact (`--superset-tier never`) | 2.3–7.2× | 3.1–5.4× | 3.0–9.3×, then the pipeline aborts at its first weekly | — |
| Packs a client keeps under Scalar's default maintenance (1-min / 15-min agent, after a week) | **215 / 80**, bounded (exact covers: 1,247 / 482, growing ~170 / 65 a day) | not measured | 17 / 14 (13 / 13 with its own hook) | not measured | — |
| 7-day replay: missed objects / cold clones exact | **0 / 84 of 84** (3 builds) | 0 / 84 of 84 | 0 / 84 of 84 (1 non-canonical `.idx`) | 0 / — (aborted) | — |

**Why opus.** With this branch's changes it ties sol on the tiniest requests (GET of a commit or
small blob at ~1M req/s, 4.9 µs of CPU per request vs sol's 5.3) and is within 4 % of sol and
13 % of fable on up-to-date prefetch. On everything that costs real CPU or bandwidth — batched
POSTs, sizes, cold clones (`sendfile`, ~10× less CPU than sol/fable), commit closures, genesis
— it is the fastest, by 1.05× (genesis vs fable) to 2.3× (sizes vs sol) over the runner-up,
without a single error, at the lowest memory after fable. Its warm-client traffic is the closest
to git's own ideal of the five (§6). Correctness: it passes every conformance, cross-client,
JSON, scale and replay check after the fixes below. A later CPU and wait analysis (§13) made
it faster still on a store carrying a week of slabs, with byte-identical responses: blob batches
1.8× (inline) to 5.5× (separate blobs), sizes 3.2× to 11×, and cold prefetches 31-41% cheaper.
Its connection layer now runs on tokio (§14), with the same bytes on the wire.
* **Small requests:** 22–35% less CPU each, and p99 at 1,024 connections falls from 15.7 ms
  to 1.3 ms.
* **Slow downloaders:** it keeps serving beside them; they stalled the 256 worker threads.
* **hyper:** its server was ruled out (20× the CPU per cloned GB, no `sendfile`); its client
  carries the edge's fetches.

**What was fixed/optimized in opus** (each A/B-measured and re-verified; details in §8):

1. **Bug — duplicate objects in cold-clone packs.** Upstreams without reachability bitmaps
   (SearchGold's own clone; any repository with reverts or re-added files) re-send objects that
   older history already has; opus concatenated them after the baseline, so a SearchGold cold
   clone had 11 duplicate entries and **`git index-pack --strict` (i.e. `transfer.fsckObjects`)
   and `git verify-pack` reject it**. Fixed: the cold plan's tail is repacked excluding the
   baseline whenever they overlap.
2. **Robustness — connection handling.** The shipped server was thread-per-connection (256
   workers): 256 idle keep-alive clients (a fleet's connection pools) stalled every other client
   for up to 30 s; more continuously *busy* clients than workers (a proxy's pool, a build farm
   hydrating blobs) left the extra ones unanswered until others went idle (4 workers, 8 busy
   clients: 4 waited the whole 4 s run); and std's listen backlog of 128 made a fleet connecting
   at once wait for SYN retries (p99 0.7 s, max 2.1 s). Fixed: idle connections are parked in one
   epoll set after a 5 ms linger; while no worker is free, a worker parks its connection between
   requests after 5 ms (`--hold-ms`), so busy clients take turns; the backlog is the system
   limit. Now 1,000 idle connections cost a table slot each, every busy client is served, and 8,000
   sporadic requests a second from 2,048 keep-alive agents get p99 23 µs. No throughput cost
   (each A/B'd: 0.99–1.02× on GET, up-to-date prefetch, POST 64 and sizes).
3. **Faster:** head and body of small responses in one `writev`; `/gvfs/sizes` formatted
   without `core::fmt`; small non-tree deltas stored whole at repack (+0.1 % baseline size).
   Net, in the final interleaved run against the shipped binary: GET blob **1.69×**, GET commit
   1.29×, sizes 1.28×, up-to-date prefetch 1.23×, POSTs 1.05–1.07×; cold clones (memory-bandwidth
   bound) and closures (tree walks) unchanged.
4. **Design gap — packs piling up on clients (§11).** opus sent exact covers only: one small
   pack per prefetch that brought news, never a superset, so a client's maintenance had to
   rewrite them. Scalar's default maintenance merges only a few a day: a 1-minute agent kept
   1,247 of its 1,405 packs after a week, growing ~170 a day. At 5,000 packs every lookup
   of an absent object costs git ~3 ms. Fixed (`--superset-tier 24h`, the default): once
   the tip moves past a sealed day or week, a client inside it receives that rollup whole,
   once, and stock `multi-pack-index expire` deletes the packs it subsumes. It now keeps
   215 (15-minute agent: 80). This is fable's superset tombstoning at day/week granularity,
   for 0.98–1.40× git's ideal bytes instead of fable's 1.85–5.3×.

**Trade-offs of choosing opus** (none egregious; know them):

* **Thread-per-connection for active transfers.** Idle clients no longer hold threads, but a
  client *receiving* a response holds a worker until done (blocking `sendfile`, 120 s per write).
  256 simultaneous slow cold clones over WAN would occupy all default workers — raise
  `--workers` (threads are cheap) on a remote edge with many slow links (connections take turns
  between requests, but not in the middle of a response). Tail latency at saturation is higher
  than sol's (GET p99 0.3 ms vs 0.12 ms at 1M req/s).
* **POST responses carry delta bases** instead of resolving deltas: ~25 % more bytes than
  fable's exact packs (183 KB vs 145 KB per 64 ids) for ~12× less server CPU. Right for a
  localhost proxy or LAN edge; on a thin WAN link fable's choice costs fewer bytes.
* **First VFS-for-Git cold clone after each manifest change** waits ~4 s while the 280 MB `.idx`
  is built (gvfs-helper does not ask for it; later clones get the cached one).
* **Genesis peaks at ~12–17 GB RSS** (pipeline runner sizing), more than fable (7 GB).
* **Day/week supersets cost bytes**: each warm client receives every sealed day and week
  whole once (0.3–0.7 MB a day per client on SearchGold), 0.98–1.40× git's ideal instead
  of 0.94–1.04×. `--superset-tier 168h` (weeks only) or `never` (exact) trade that back for
  more packs on Scalar-default clients (§11). With fable's own client hook, fable keeps
  fewer packs (13 vs opus's 80–232 for agents) at 3.8–5.3× git's ideal bytes.

**Why not the others** (details §9):

* **sol** — as fast as opus on tiny requests, with the best p99 (tokio/hyper, 5.3 µs per GET),
  but unsafe to run with defaults at this scale: genesis from a git upstream failed (fixed: keep-alive packets),
  tip closures and even 4,000-id POSTs return 413 until `--max-ranges` is raised, a POST of 10
  objects returns **145 MB / 417,652 objects** (its planner sends large neighbourhoods), and
  under 64 concurrent POST clients it answers 99.999 % of requests with 503 (`HTTP arenas busy`).
  12 GB RSS, 2.6× slower genesis, 3–4× fewer cold clones per second at 9× the CPU each.
* **fable** — the most conservative and correct out of the box (0 failures, exact POST packs,
  best memory), but 4–13× slower than opus on GET/POST/sizes: every request clears a bitset sized
  to the whole store (63 % of GET CPU in `memset`) and allocates/zero-fills fresh response frames
  (~28 page faults per GET); warm clients receive 1.9–5.3× git's ideal bytes on SearchGold
  (superset rollups by design). That design has a real payoff that §6 did not credit: under
  stock Scalar maintenance its supersets let `multi-pack-index expire` keep a client's packs
  bounded, and with its own hook an agent keeps 13 packs (§11). opus now does the same at
  day/week granularity.
* **astra** — 6 conformance failures: clients with a cursor older than the store's genesis or
  ahead of it get errors forever (store rebuild/restore = permanent outage for every client);
  8 worker threads by default (8 idle clients stall it); malformed requests answered with 503
  (clients retry) or a dropped connection. Ingest aborted on any blob that merely looks like an
  HFS pointer (fixed to make progress). Commit closures include every blob (2.8× the bytes);
  sizes 49× slower than opus; default clustered cold pack 4.4 GB vs 2.6 GB; 12 GB RSS.
* **argon** — not a working server: `GET /gvfs/objects` returns 404 for every deltified tree,
  warm prefetch packs are thin (missing delta bases: `index-pack` fails), hot-path allocations,
  local-path upstream only. Dropped after the small-corpus suite.

---

## 2. Method (what "standard comparison" means here)

The five `tests_plus_benchmarks_*.md` documents were merged into one implementation-neutral
protocol, driven only through each binary's CLI and HTTP API (adapters in `bench/impls.py`) and
judged only by **stock git** (`index-pack [--strict]`, `verify-pack`, `rev-list --missing=print`,
`cat-file`) — never by an implementation's own reader.

| Suite | Script | What |
|---|---|---|
| Static gates | `cargo clippy` / each own test suite | deny-lints, own tests |
| Lifecycle conformance | `bench/conformance.py` | ~900 checks per implementation on a deterministic synthetic GitHFS corpus (fast-import; no bitmaps, like SearchGold): genesis, cold/warm/up-to-date prefetch, live tip, every rollup tier incl. Sunday and Monday weeklies, a merge stamped 2001, a revert, re-added objects, a force-push rewrite, server restart with unsealed data, a branch never pipelined, every object by GET/POST/sizes, closures depth 1–3, malformed input, idle/stalled/concurrent connections, hot-path allocations. Pipeline and server share a libfaketime clock moved 15 min per step. |
| Cross-client | `bench/cross_clients.py` | Microsoft `gvfs-helper` (prefetch/get/post), fable's `client-prefetch` hook, astra's `sync`, each against every server |
| JSON variants | `bench/json_variants.py` | compact/pretty/reordered/unknown-field/uppercase/duplicate requests (sol W07, astra U-JSON) |
| Scale correctness | `bench/sg_genesis.py`, `bench/sg_verify.py` | SearchGold `main` 4d12e2c7 (9,224,930 objects): genesis; cold clone streamed, `index-pack`'d, object set compared exactly with `rev-list --objects`, connectivity, `.idx` byte-identity; tip closure; 3,000 sampled GETs; sizes; 4,000-id POST |
| Load | `bench/load_bench.py`, `bench/load_report.py` | 9 workloads × 3 **interleaved rounds** (implementation order and workload order shuffled per round), server pinned to 4C/8T of CCD0, loadgen on CCD1, server CPU per request from `/proc`, RSS; frozen, uniformly sampled OID files (400K) |
| Usage patterns | `bench/replay.py` | 7 real SearchGold days (1,646 pushes), pipeline on its documented cron, live edge, 9 client behaviours (agents every 1/15/60 min, CI cold clones, developers, vacation, stale), every response validated, exact object-set oracle, bytes vs git's own pack |
| Profiles | `bench/profile.py`, `bench/ab.py` | perf + AMD counters; A/B of every optimization, alternating runs |

---

## 3. Static gates and own test suites

| | clippy (deny unwrap/expect/panic/indexing/arithmetic) | own tests | `unsafe` |
|---|---|---|---|
| opus | 0 warnings | 49/49 as shipped (incl. a gvfs-helper e2e); 53/53 with the 4 added here (`cargo test --workspace`) | 2 tiny crates (3 after §13: a prefetch hint) |
| fable | 0 | 40/40 | 2 tiny crates |
| sol | 0 | 29/29 | 1 crate |
| astra | 0 | 37/38 — two unit tests flaky (~50 %): an flock race in a multithreaded test, a mock-HTTP connection reset | 1 crate |
| argon | 0 | 12/12 | 1 crate |

## 4. Conformance (synthetic lifecycle) and cross-client

Bitmap-free corpus (like SearchGold's clone), ~900–920 checks each (the count varies with how
many prefetch rounds an implementation needs). Final matrix: all six binaries in one run
(`bench/results/conformance-final.json`). Byte ratios: total bytes a client received over
the lifecycle ÷ the sum of git's own packs for the same steps.

| | passed / failed | failures | poller | hourly | developer | late | 
|---|---|---|---|---|---|---|
| **opus final** | **918 / 0** | — | **1.24×** | **1.37×** | **1.45×** | **1.55×** |
| opus as shipped | 901 / 12 | duplicate entries in cold packs after a revert / re-added objects (10 checks: `index-pack --strict`, `verify-pack`); 300 and 1,000 idle connections starve it (2) | 1.21× | 1.32× | 1.39× | 1.47× |
| fable | 923 / 0 | — | 2.51× | 4.28× | 5.63× | 6.77× |
| sol | 918 / 0 (917 / 1 in another run: a 503 in the 32-way concurrent mix) | — | 1.47× | 2.85× | 3.50× | 4.62× |
| astra | 784 / 6 | future cursor → error; stale cursor → 503; malformed → 503/dropped; idle 100/300/1000 connections | 1.28× | 2.58× | 3.19× | 4.25× |
| argon | 490 / 115 (902 / 13 and 645 / 76 in earlier runs: failures cascade non-deterministically) | thin warm packs, 404 for deltified trees, hot-path allocations, 503s | 2.42× ² | 3.97× ² | 5.66× ² | 6.40× ² |

² argon's byte ratios are from its 902 / 13 run (its lifecycle stops early when failures cascade).
Final opus receives 2.5–5 % more bytes than the shipped binary on this corpus: rollups now carry
small non-tree objects whole instead of as deltas (`75271a2`), and this corpus is mostly tiny
blobs. On SearchGold the same change adds 0.1 % to the baseline and nothing to warm clients
(99,941,078 vs 99,942,202 bytes over the replayed week, §6).

Cross-client (each client × each server, push → live → seal → rollups → weekly, judged by
`rev-list --missing=print` in the client):

| client \ server | opus | fable | sol | astra |
|---|---|---|---|---|
| **Microsoft gvfs-helper** (prefetch, get, post) | ✅ 18/18 | ✅ 18/18 | ✅ 18/18 | ✅ 18/18 |
| fable `client-prefetch` (interval tombstoning + midx) | ✅ 15/15 | ⚠️ 12/15 (its `multi-pack-index write` fails after unlinking) | ✅ 15/15 | ❌ needs a server `.idx` |
| astra `sync` (managed cache) | ❌ needs astra's ETag | ❌ | ❌ | ✅ 15/15 |

No client accumulated packs from repeated idle polls on any server. JSON variants (compact,
pretty, reordered keys, unknown fields, uppercase hex, duplicates, tabs/CRLF): every server
answers every valid variant exactly. Malformed bodies: sol 400 (strict), opus/fable lenient
(extract ids), astra 503.

## 5. Scale correctness (SearchGold)

`main` = 4d12e2c7, 9,224,930 objects reachable (`rev-list --objects`), upstream = a single-ref
bare repository sharing SearchGold's objects (read-only), served by `git upload-pack`.

| | opus | fable | sol | astra |
|---|---|---|---|---|
| Genesis wall / user CPU / peak RSS | **79 s** / 149 s / 17.1 GB (final build: 91 s on 16 threads, 12.5 GB) | 83 s / 104 s / **7.3 GB** | 202 s / 192 s / 18.4 GB — *failed until fixed* (keep-alive) | 267 s / 314 s / 6.4 GB |
| Store on disk | 2.97 GB | 3.03 GB | 4.80 GB | 5.24 GB |
| Cold clone: 1 pack, `index-pack` OK, exact object set, connectivity | ✅ | ✅ | ✅ | ✅ |
| Cold pack / `.idx` sent | 2.60 GB / 280 MB (non-git UA) | 2.60 GB / 280 MB | **2.39 GB** / 261 MB | 4.40 GB / none |
| `.idx` byte-identical to git's | ✅ | ✅ | ✅ | n/a |
| Tip closure (505,605 trees): time / bytes / extra objects | **0.68 s** / 138 MB / 22,157 bases | 0.83 s / 137 MB / 0 | 0.73 s / 139 MB / 0 — **413 with defaults**, needs `--max-ranges 8388608` | 1.31 s / **384 MB** / 2,469,256 (blobs) |
| 3,000 sampled GETs (all types), sizes of 4,000 | ✅ / ✅ | ✅ / ✅ | ✅ / ✅ | ✅ / ✅ |
| POST 4,000 non-commits: complete / extra objects | ✅ / 38,891 bases | ✅ / **0** | ✅ (413 by default) / **657,911** | ✅ / 1,638 |
| Hot-path allocations | 0 | 0 | 0 | 0 |

sol's POST over-delivery is per object, not per batch: one 1 KB tree came back with 947 trees
(1.9 MB); 10 random objects with 417,652 objects (145 MB). opus, fable and astra return exactly
the requested object for single-object POSTs.

Final opus (`9b736e2`; also `70091b7`), re-checked with the store its own pipeline wrote: every
row ✅, cold clone 2.60 GB with git's `.idx`, closure 0.68 s / 138 MB / 22,157 bases, POST 4,000
complete with 36,075 bases, 0 errors, 0 hot-path allocations
(`bench/results/verify-sg-opus-final.json`).

The duplicate-entry bug in opus does not show at genesis (a single upstream pack); it appears
as soon as the timeline holds re-sent objects — see §6 and §8.

## 6. Usage patterns: 7-day SearchGold replay

Real history: SearchGold first-parent `main`, Tue 2026-10-13 00:00 → Tue 10-20 00:00 UTC, 1,646
pushes (weekdays, a weekend, a Sunday-00:00 and a Monday-00:00 weekly boundary). Each
implementation was seeded at the window start and then driven exactly as its README prescribes
(opus/fable: cron with explicit tiers; sol/astra: `--tier auto` every 15 min), pipeline and
server sharing a libfaketime clock; the edge followed the upstream live (1 s poll). Clients send
gvfs-helper's User-Agent. Every response was validated: strict GPRE, ≤ 1 pack, the pack
self-contained (`index-pack` in an empty repository), a sent `.idx` byte-identical to git's, and
**complete** against an exact object-set oracle (`objects(SEED)` loaded exactly, since
`rev-list A ^B` is not a set difference). "Ideal" = git's own non-thin pack for the same
have→want (`pack-objects --revs`). 84 CI cold clones were streamed, SHA-1-checked and their
object counts compared exactly.

**Correctness over the week**

| | opus (`50c4cc5`, `75271a2`, `70091b7`: three runs) | fable | sol | astra |
|---|---|---|---|---|
| Missed objects / told up-to-date while behind | 0 | 0 | 0 | 0 (until abort) |
| Cold clones exact (count, SHA-1) | 84 / 84 | 84 / 84 | 84 / 84 | — |
| Other | — | 1: merged `.idx` of the stale client's full pack not byte-identical (64-bit offset table in slab order; lookups correct, `git verify-pack` rejects it) | 14,763 errors counted in `/metrics` (404 probes) | **pipeline aborted at its first weekly (Mon 00:00): `configured capacity exhausted: record arena`** — also with 2× and 4× `--max-objects`; with `auto` every later run retries it, so the store stops advancing |

Each opus run is a separate seed and week; their warm-client bytes agree to 0.01 %, so the tables
below show one opus column. These three builds send exact covers; the final build (`f7d9bef`)
also delivers each sealed day and week whole once, and its own replay is in §11 (0 failures,
84/84 cold clones, 0.98–1.40×). The commit `9b736e2` (listen backlog, linger)
changes no path this replay exercises: its client opens one connection per request. The
original opus (before `50c4cc5`) failed this replay: from day 1 its cold clones carried 1–11
duplicate entries (§8).

**Bytes per client pattern, same window for all four** (Tue 00:00 → Mon 00:00, before astra's
abort): received ÷ git's ideal (bold), the same with only pack bytes, total, packs created on a
stock client (one per non-empty response).

| client | opus | fable | sol | astra |
|---|---|---|---|---|
| agent, every minute | **1.04×** (1.03× pack only), 30.6 MB, 998 packs | **5.39×** (5.24× pack only), 159.0 MB, 1988 packs | **2.27×** (2.20× pack only), 66.9 MB, 1293 packs | **9.28×** (9.28× pack only), 274.1 MB, 4117 packs |
| agent, every 15 min | **1.02×** (1.02× pack only), 17.0 MB, 374 packs | **3.71×** (3.61× pack only), 61.9 MB, 431 packs | **3.04×** (2.99× pack only), 50.8 MB, 418 packs | **2.98×** (2.98× pack only), 49.7 MB, 420 packs |
| agent, hourly | **1.00×** (1.00× pack only), 9.7 MB, 116 packs | **4.38×** (4.26× pack only), 42.7 MB, 116 packs | **3.57×** (3.53× pack only), 34.8 MB, 116 packs | **3.40×** (3.40× pack only), 33.2 MB, 116 packs |
| laptop, hourly 08-18 weekdays | **0.99×** (0.99× pack only), 5.4 MB, 44 packs | **4.48×** (4.36× pack only), 24.5 MB, 44 packs | **4.50×** (4.45× pack only), 24.7 MB, 44 packs | **4.45×** (4.45× pack only), 24.4 MB, 44 packs |
| developer 09:00/13:00/17:30 | **0.98×** (0.98× pack only), 3.5 MB, 12 packs | **5.09×** (4.96× pack only), 18.1 MB, 12 packs | **6.80×** (6.73× pack only), 24.1 MB, 12 packs | **6.73×** (6.73× pack only), 23.9 MB, 12 packs |
| developer, mornings | **0.97×** (0.97× pack only), 2.3 MB, 4 packs | **3.06×** (2.94× pack only), 7.4 MB, 4 packs | **5.93×** (5.84× pack only), 14.4 MB, 4 packs | **5.85×** (5.85× pack only), 14.2 MB, 4 packs |
| back after 6 days off | **1.00×** (1.00× pack only), 0.4 MB, 1 packs | **2.98×** (2.92× pack only), 1.2 MB, 1 packs | **7.15×** (7.08× pack only), 2.8 MB, 1 packs | **7.08×** (7.08× pack only), 2.8 MB, 1 packs |

**Full week** (opus, fable, sol): opus 0.94–1.03× for every pattern; fable 1.85–5.35×; sol
2.3–7.2× (tables: `bench/results/report-full.txt`; final opus builds: `report-final.txt`).

| | opus | fable | sol |
|---|---|---|---|
| Pipeline: 672 runs, total CPU-wall / weekly run | **24–25 s / 6.3–7.2 s** (weekly = repack of slabs) | 96 s / 81.5 s (weekly re-fetches the whole repo upstream, ~2.6 GB) | 2,791 s / 152 s (~4 s every 15 min) |
| Push → servable (p50 / max, 1 s poll) | 0.35–0.45 / 1.06 s | **0.01 / 0.05 s** (read-through fetch) | 0.40 / 1.54 s |
| Cold clone bytes / time (p50; the client SHA-1s every byte) | **2.60 GB / 1.42 s** in the four-way concurrent run, like sol's (final build alone: 1.16 s, like fable's) | 2.878 GB (pack + `.idx` gvfs-helper discards) / 1.90 s | 2.653 GB / 3.56 s |
| Warm prefetch latency p50 / p99 / max (ms) | 0.09 / 3.6 / 145 (first client after a change builds the plan) | 0.11 / 0.65 / 6.8 | 0.11 / 0.54 / 51 |
| Objects re-sent to clients that already had them | 2,523 | 256,125 | 23,416 |

What the patterns mean in practice:

* **Agents polling every minute** (the worst case for a server): opus sends each push once
  (1.04×); sol 2.3×, fable 5.4×, astra 9.3× — astra resends the live pack on every poll until the
  pipeline seals it, which also left that client with 4,117 packs in six days.
* **Humans** (hourly laptop, 3×/day, mornings, back from vacation): opus ≈ 1.0×; the others
  3–7× because a client inside a rolled-up window receives the whole rollup again.
* **Cold clones** (CI): one pack, every time, for all three that completed the week; opus's is
  the smallest it can be for its layout and streams via `sendfile`.
* **Pack count on stock clients** is one per non-empty response for every implementation
  (gvfs-helper keeps each prefetch pack until Scalar's maintenance consolidates them); opus
  creates the fewest because it never re-sends. What maintenance can then delete is a
  different matter, and it favours supersets: §11 measures it.

## 7. Load (median of 3 interleaved rounds)

SearchGold stores from §5 (9.22M objects), each server restarted per round with its default
flags (sol: the capacity flags it needs at this scale plus `--arenas 8`; astra `--workers 64`),
pinned to 4 cores / 8 threads of CCD0; the closed-loop load generator (Rust, keep-alive HTTP/1.1)
on the other CCD. Implementation order and workload order are shuffled every round; both opus
binaries ran in the same rounds. 400K-entry OID files, sampled uniformly once and frozen. Only
`200` responses count. Raw: `bench/results/load-final-all.json`, `load-final-medians.txt`.

| workload (connections) | **opus final** | opus as shipped | sol | fable | astra |
|---|---|---|---|---|---|
| GET blob (64): req/s · CPU/req · p99 | **1,006,808** · **4.9 µs** · 306 µs | 594,582 · 8.6 µs · 641 µs | 1,009,187 · 5.3 µs · **122 µs** | 115,010 · 65.5 µs · 1,073 µs | 534,765 · 8.3 µs · 407 µs |
| GET commit (64) | **1,024,256** · **4.8 µs** · 300 µs | 791,160 · 6.0 µs · 361 µs | 1,023,861 · 5.1 µs · **124 µs** | 117,417 · 64.1 µs · 1,051 µs | 532,610 · 8.3 µs · 403 µs |
| GET tree (64), 4 KB | **181,889** · 41.0 µs · 2,993 µs | 175,566 · 41.7 µs | 176,664 · 42.3 µs · **814 µs** · 154 × 503 | 48,957 · 156 µs | 179,985 · **39.9 µs** · 2,365 µs |
| POST 64 ids (64): req/s · KB/resp · CPU/req | **29,569** · 178 KB · **261 µs** | 27,630 · 183 KB · 278 µs | 3.5 · 195 **MB** · — (7.6M × 503) | 2,273 · **145 KB** · 3.5 ms | 20,588 · 158 KB · 280 µs |
| POST 4,000 ids (32) | **352** · 11.0 MB · 22 ms | 334 · 11.3 MB · 23 ms | 1.5 · 586 **MB** (573K × 503, 344K connection errors) | 32.9 · **9.1 MB** · 242 ms | 309 · 9.9 MB · **20 ms** |
| POST `/gvfs/sizes`, 4,000 ids (32): req/s · CPU/req | **12,268** · **640 µs** | 9,594 · 818 µs | 5,265 · 1,505 µs · 1,411 × 503 | 3,134 · 2,522 µs | 248 · 19,940 µs |
| Prefetch, up to date (64): req/s · CPU/req · p99 | 1,066,515 · 4.7 µs · 297 µs | 867,191 · 5.4 µs | 1,109,830 · 4.5 µs · 114 µs | **1,232,029** · **3.7 µs** · **101 µs** | 874,624 · 5.4 µs |
| Prefetch, cold (8): clones/s · GB/s · GB/clone · CPU/clone | 16.9 · 48.7 · 2.88 · 141 ms ¹ | **20.5** · 59.0 · 2.88 · 147 ms | 5.3 · 14.1 · **2.65** · 1,313 ms | 4.0 · 11.5 · 2.88 · 1,791 ms | 14.1 · **62.1** · 4.40 · 148 ms |
| Tip closure, 505,605 trees (4): s/closure · MB | **0.69 s** · 141 | 0.69 s · 141 | 1.06 s · 143 | 0.89 s · **140** | 1.55 s · 391 (all blobs too) |
| Peak RSS while serving (opus/fable: mostly the mmapped store) | 4.0 GiB | 4.0 GiB | 11.9 GiB | **3.2 GiB** | 11.6 GiB |

¹ Cold prefetch is bound by memory/fabric bandwidth on this box (≈ 50–60 GB/s through loopback;
the server used 2.3–3.2 of its 8 threads at ~145 ms CPU per clone), and the same binary varies
1.15–1.28× round to round. Isolated A/B with alternating runs, final vs as-shipped: **0.99×**
(4 rounds, exact load-run pairing), 1.01× (same store), 0.98× (same binary, both stores): no
change. opus and astra stream from the page cache with `sendfile`; sol and fable copy through
user space (~9–12× the CPU per clone).

Reading the table:

* **Tiny requests** (GET of a commit or small blob, up-to-date prefetch) run at ~1M req/s for
  opus, sol and fable — with 64 closed-loop connections that is a latency limit, not a CPU limit
  (opus, sol and astra use 4.4–5.3 of their 8 threads; fable is CPU-bound on GETs at 7.5), so CPU
  per request is the fairer comparison: final opus 4.7–4.9 µs, sol 4.5–5.3 µs, fable 3.7 µs on
  up-to-date but 64 µs on GETs, astra 5.4–8.3 µs.
  sol keeps the better p99 under saturation (tokio, 8 threads, vs opus's thread per active
  connection).
* **Requests that move objects** (POST batches, sizes, closures, cold clones): opus is the
  fastest or within noise of the fastest on every one, without errors. sol answers 99.999 % of
  concurrent POSTs with `503 arenas busy`, and the few it serves carry 50–1,100× opus's bytes.
  fable's packs are the most exact (no delta bases) at 12× opus's CPU.
* Net effect of this branch on opus (same run): GET blob **1.69×**, GET commit 1.29×, sizes
  1.28×, up-to-date prefetch 1.23×, POST 64 1.07×, POST 4,000 1.05×, GET tree 1.04×, closure
  1.00×, cold clone 0.99× (A/B).
* "opus final" in this table is `75271a2`. The two later commits (`70091b7`, `9b736e2`) change
  only connection scheduling; each measured 0.99–1.02× against its predecessor on GET
  blob/commit (and tree), up-to-date prefetch, POST 64 and sizes (3 alternating rounds each),
  with the same CPU per request and p99.

## 8. opus changes in this branch

All changes keep opus's invariants (`forbid(unsafe_code)`, the clippy deny list, zero hot-path
allocations — `/metrics` `gvfs_alloc_hot_path_total` stayed 0 in every run). Each was A/B'd
against the previous binary with alternating runs on the same pinned cores, then re-verified
with opus's own suite (53 tests; 55 with `f7d9bef`), the conformance suite (918/918), the
cross-client suite, and the SearchGold scale checks.

| Commit | Change | Why (evidence) | Effect |
|---|---|---|---|
| `50c4cc5` | Cold plan: repack the tail excluding the baseline when they share objects; keep the cover's end as the plan's timestamp | Replay: SearchGold cold clones carried 1–11 duplicate entries (`index-pack --strict`: "The same object … appears twice in the pack"). Reproduced on the synthetic corpus once upstream bitmaps are off (revert / restored tree) | Conformance 903/918 → 918/918; replay cold clones exact |
| `cdda1ae` | Idle keep-alive connections parked in one epoll set after a 100 ms linger (5 ms since `9b736e2`; `--linger-ms`, `--max-idle-conns`), generation-tagged slots, idle reaper, fd limit raised; mid-request reads switch to `--idle-secs` patience | 300+ idle connections: requests timed out (256 workers + 128 backlog). A first attempt that re-armed epoll per request serialized on the epoll mutex (0.73–0.81×, 43 % kernel lock time) — hence the linger | 1,000 idle connections: `/gvfs/config` answered in < 80 ms; throughput 0.98–1.03× (noise) |
| `70091b7` | While no worker is free, a worker parks its connection between requests after holding it `--hold-ms` (5); `Conns` counts workers waiting in `epoll_wait` | 4 workers, 8 busy keep-alive clients: 4 clients got no answer for the whole run (also in the shipped binary). New e2e test (2 workers, 6 clients, every body checked) fails without it: answers `[359744, 293721, 1, 1, 1, 1]` | Every client served (19–21K answers each, first within 11 ms), same total. A/B 256 workers / 64 conns: 1.00–1.02× on GET, up-to-date prefetch, POST, sizes; 32 workers / 64 conns: 0.99–1.01×, all 64 served |
| `9b736e2` | Listen backlog raised from std's 128 to `net.core.somaxconn`; keep-alive linger default 100 → 5 ms | 2,048 clients connecting at once: 14K listen overflows, SYN retries (p99 713 ms, max 2.1 s, server 4 % busy). Each sporadic request (one request, then idle) held its worker for the whole linger: 256 workers ≈ 2,500 such req/s before queueing; 8,000 req/s over 2,048 keep-alive clients: p50 145 ms, p99 2.5 s | No overflows; p50 16 µs, p99 23 µs. 512 clients at 20 ms think time: p99 8.9 ms → 18 µs (+0.01 core). Closed loop (back-to-back): 0.99–1.00×, same CPU and p99 |
| `cdda1ae` | `/gvfs/sizes` items built without `core::fmt` / UTF-8 validation | profile: 52 % `from_utf8` + 17 % fmt | 1.24–1.25× req/s, CPU/request 916 → 724 µs |
| `7e45558` | Response head and small body in one write (`Out::head`, `write_response` via `writev`) | two syscalls and two TCP segments per small request | GET blob 1.09×, GET commit 1.08×, up-to-date prefetch 1.15×, lower p99 |
| `f7d9bef` | Superset deliveries (`--superset-tier 24h`, default): once the tip moves past a sealed daily rollup or weekly delta, a client whose cursor lies inside it (or at its end) is answered from its start, so it receives it whole, once; `X-GVFS-Pack-Start/End` on every prefetch | §11: exact covers left Scalar-default clients ~every pack (1-minute agent: 1,247 of 1,405 after maintenance, +170/day); git pays ~3 ms per absent-object lookup at 5,000 packs | Scalar-default 1-minute agent: 215 packs (15-minute: 80), bounded; warm bytes 0.94–1.04× → 0.98–1.40× (14-day sim 1.11–1.53×; `168h` 1.04–1.31×). Replay 0 failures, 84/84; conformance 918/0; cross-client unchanged; 55 tests |
| `75271a2` | Repack stores non-tree objects ≤ 4 KiB whole (ADO deltifies pointer blobs and commits in chains up to 50 deep) | profile: ~31 % of GET-blob CPU inflating delta chains | GET-blob CPU/request 5.4 → 2.6 µs (perf; 1.12× req/s in an A/B whose load generator shared the server's CCD — §7 has the full-load effect); POST 64 ids 1.08×; baseline +2.5 MB (+0.1 %), 272,990 objects; warm-client bytes unchanged on SearchGold |

Tried and rejected: re-arming epoll on every request (above); hand-formatting the response head
(no measurable gain, reverted).

Not done (bigger changes; candidates for later): resolve-and-cache for deep ADO *tree* chains
(GET tree spends 32 % in deflate and ~25 % inflating chains of mean depth 23); a non-blocking
write path so slow downloaders do not hold workers; building the cold `.idx` eagerly.

## 9. Defects found, per implementation

Severity: **C** correctness (a client can end up wrong or rejected), **A** availability (a
realistic pattern makes it fail or stall), **P** performance, **H** harness-only.

**opus** — C: duplicate objects in cold-clone packs after re-sent objects (fixed). A: idle
keep-alive connections exhaust the 256 worker threads (fixed); more continuously busy
connections than workers starve the extra ones (fixed); listen backlog 128, so connection
bursts wait for SYN retries (fixed). The first version of the idle-connection fix lingered
100 ms per request, which capped sporadic keep-alive traffic at ~2,500 req/s (now 5 ms).
P: `/gvfs/sizes` formatting, two writes per small response, deltified small objects (all
fixed). Remaining by design: POST
responses include delta bases; workers held for the length of a response.

**sol** — A: rejected git upload-pack keep-alive packets, so genesis of any large repository
from a git upstream failed after 5 s (fixed in `sol/src/upstream.rs`, 1 hunk). A: needs many
capacity flags at scale; with defaults a tip closure or a 4,000-id POST returns 413. A: under 64
concurrent POST clients almost every request is 503 (`HTTP arenas busy`); 1 in ~800 requests
503s in a 32-way mixed burst on the small corpus. P/bandwidth: POST returns large neighbourhoods
(1 object → 947, 10 → 417,652 objects / 145 MB). P: cold clone 3–4× less throughput than opus,
12 GB RSS, 2.6× slower genesis.

**fable** — P: per-request `seen.fill(0)` over a bitset of every stored object (63 % of GET CPU)
and freshly allocated, zero-filled response frames (~28 page faults per GET): GET 8.8×, POST
11–13×, sizes 3.9× slower than final opus (a dirty-word fix gave 1.24–1.36× on GET; not kept,
to compare fable as shipped). C (minor): the `.idx` it merges for a > 2 GiB multi-slab pack
lists 64-bit offsets in slab order, not git's: lookups are correct, but `git verify-pack`
rejects it (the stale client's full pack in the replay). Bandwidth: superset rollups resend whole windows (2.6–6× ideal on the
lifecycle corpus). Client hook: `client-prefetch` against fable's own server fails `git
multi-pack-index write` after unlinking subsumed packs; it needs a server `.idx` (fails against
astra). Unknown ids in `/gvfs/sizes` get `Size: 0` with 200. Strengths: 0 conformance failures,
exact POST packs, read-through fetch of objects the pipeline never saw.

**astra** — A: in the SearchGold replay the pipeline aborted at its first weekly (`configured
capacity exhausted: record arena`, also with 2× and 4× `--max-objects`); with `--tier auto`
every later run retries it, so the store stops advancing. A: a cursor older than the store's
genesis → 503 forever; a cursor ahead of the
server → error (a store rebuild or restore breaks every existing client). A: `--workers 8`
default (100 idle connections: requests time out). A: malformed input → 503 (clients retry) or a
dropped connection. A: ingest aborted on any blob that starts like an HFS pointer but is another
variant (fixed in `astra/src/ingest.rs` to make progress; the parse only fed a counter). P: sizes
49× slower than opus; closures include every blob (2.8× bytes); clustered (default) cold pack
4.4 GB vs ~2.6 GB; 12 GB RSS; genesis 3.4× slower. Re-sends live packs whose timestamp does
not advance (24 non-advancing responses for the lifecycle poller). Its `sync` client requires an
astra-only ETag. Two flaky unit tests.

**argon** — C: `GET /gvfs/objects/<deltified tree>` → 404 (no delta path in the handler); warm
packs reference delta bases they do not contain (`index-pack`: "did not receive expected
object"); clients left incomplete. P: hot-path allocations (135 in the suite). A: local-path
upstream only, wall clock only, live sync holds the runtime's write lock for the whole ingest.

**Harness bugs found and fixed along the way** (H, listed so nobody repeats them):
`git rev-list --objects A ^B` is not a set difference (it lists deleted-then-re-added objects),
so a completeness oracle needs `objects(SEED)` exactly; `shuf --random-source=<(yes)` (the recipe
in one doc) gives a biased sample (0 commits in 400K); bare-repository `repack -a` writes
bitmaps, which hide the duplicate-object bug; Python 3.14's larger default write buffer; a shared
temp file in a multithreaded cache save.

## 10. Reproduce

```sh
cd bench
python3 smoke.py                         # 1 minute triage of all five
python3 conformance.py                   # lifecycle suite, all five in parallel (~10 min)
python3 conformance.py opus-orig opus    # original vs final opus
python3 cross_clients.py                 # gvfs-helper (~/git/bin-wrappers/git), fable hook, astra sync
python3 json_variants.py
python3 sg_genesis.py opus fable sol astra   # SearchGold stores under ~/gvfs-bench/sg
python3 sg_verify.py opus fable sol astra   # VERIFY_WORK=<store> VERIFY_TAG=-x: another store
ROUNDS=3 python3 load_bench.py opus opus-opt fable sol astra && python3 load_report.py
python3 replay.py --seed opus fable sol astra && python3 replay.py opus fable sol astra
(export REPLAY_WORK=~/gvfs-bench/replay-final FROZEN=<dir with opus-gvfs-daemon>   # another run
 python3 replay.py --seed opus && python3 replay.py opus)
python3 replay_report.py opus fable sol astra            # tables; label=<work dir> for other runs
python3 ab.py opus <candidate-binary> get_blob ...       # A/B any change (AB_EXTRA_SERVE=...)
python3 busy_conns.py <opus-binary> 4 8                  # busy clients beyond --workers
CONNS=2048 RATE=8000 python3 sporadic.py <opus-binary>   # sporadic keep-alive clients
python3 profile.py opus get_tree                         # perf stat + perf record
# §11: client-side pack lifecycles (REPLAY_WORK=~/gvfs-bench/ws; FROZEN: dirs of binaries)
REPLAY_KEEP_RX=1 python3 replay.py opus fable fable@h24  # after --seed of each; keeps every response
FROZEN=<dir with the superset build> python3 replay.py --seed opus@ss && \
  FROZEN=<same> REPLAY_KEEP_RX=1 python3 replay.py opus@ss
WS_REGIMES=scalar,vfs,geometric python3 workstation.py opus opus@ss fable fable@h24
python3 ws_report.py opus opus@ss fable fable@h24        # bytes, packs per regime, projection
PC_SIZES=0,100,1000,5000,20000 python3 packcount.py opus agent-1m   # what pack count costs git
cargo run --release --example clients -- --repo ~/SearchGold --from 2026-10-10 --days 14 \
  --work <scratch> --superset-tier 24h|168h|never      # (in opus/) opus's own 14-day simulator
# §12: --blobs separate
(cd ../opus && cargo test --release --test separate && \
  cargo test --release --test separate -- --ignored blob_over_4gib)  # ~13 GB under target/
python3 separate_seed.py --jobs 8                        # real-content seed, ~/gvfs-bench/sep-seed
gvfs-daemon pipeline --store fs:<dir> --upstream ~/SearchGold --branch main --tier 15m \
  --blobs separate                                       # SearchGold metadata in separate mode
# §14: the connection layer on tokio (first, in opus/: cargo build --release --bin gvfs-daemon
# --example spike_tokio; the store: build_store.py)
python3 spike_tokio.py check --dir ~/gvfs-bench/prof/sg7-sep   # spike responses = serve's
THREADS_BIN=<a build of 5dada58> python3 spike_tokio.py bench --dir ~/gvfs-bench/prof/sg7-sep
                                                         # threads vs tokio, ~1 h; prof/out/spike.json
```
Scratch data lives under `~/gvfs-bench` (SearchGold stores ~16 GB); `/tmp` is a tmpfs here.
The shell's injected `GIT_CONFIG_*` is stripped by `gvfslib.clean_env`.

## 11. Client-side pack lifecycle: exact covers vs superset tombstoning

**The question.** The design's "zero-GC workstation" fixes are: splice one pack per fetch
(Fix 1), let clients unlink packs that a superset rollup subsumes (Fix 2), and use
`multi-pack-index` instead of `git gc` (Fix 3). fable re-sends every rollup tier whole, so
that a client can tombstone. §6 counted that only as extra bytes. Does it buy something
that opus's exact covers lack?

**What each server sends.** Neither sends loose objects: every prefetch answer is one pack.

* *Inside the live window (< 15 min).* opus keeps one L0 slab per push, so a poller gets
  exactly the pushes since its cursor: a 1-minute agent gets e.g. 10 objects / 26 KB per poll
  that has news. fable keeps one growing live slab and re-sends it whole on each poll whose
  slab end moved: e.g. 46 objects / 140 KB, and 140 objects again when only the end stamp
  advanced. That is 5.3× git's ideal for a 1-minute agent, and 2× as many packs.
* *Beyond it.* opus answers with the exact cover from the cursor through retained finer
  slabs (1.0×). fable re-sends any rollup (1h…168h) whose window holds the cursor, whole
  (1.9–4.9×), and labels it `X-GVFS-Pack-Start/End` for its `client-prefetch` hook.

**Method.** The §6 week was replayed again keeping every response (`REPLAY_KEEP_RX=1`). It
covered opus as before, fable, fable with `--leaf-tier h24` (its knob: leaves below a day),
and opus with the superset deliveries below (`opus@ss`). `bench/workstation.py` then
installs each client's responses into real repositories with microsoft/git 2.56, the
client these servers serve, under five regimes:

* **stock** — gvfs-helper as shipped: `prefetch-<ts>-<sha>` per response, `.keep` on the
  newest only.
* **scalar** — stock plus Scalar's default `incremental` strategy, run daily:
  `incremental-repack`, i.e. `multi-pack-index write`, `expire`, and
  `repack --batch-size=<2nd largest + 1>`.
* **vfs** — stock plus VFS for Git's daily `PackfileMaintenanceStep`: `write`, `expire`,
  `repack --batch-size=2g`.
* **geometric** — stock plus `maintenance.strategy=geometric`, run daily.
* **hook** — fable's tombstoning rule: unlink every pack whose interval lies inside another's.

Every scalar/vfs/geometric client was checked complete with `rev-list --missing=print`. Every
object an unlinked pack held was found again, and the gvfs-helper cursor (the newest prefetch
pack) always survived: 0 problems in 112 client lifecycles.

**Bytes over the week** (÷ git's ideal for the same have→want):

| client | opus (exact) | **opus final** (`f7d9bef`, day/week supersets) | fable | fable `--leaf-tier h24` |
|---|---|---|---|---|
| agent, every minute | 43.3 MB (1.03×) | 48.1 MB (1.15×) | 222.6 MB (5.31×) | 184.2 MB (4.39×) |
| agent, every 15 min | 23.8 MB (1.02×) | 28.6 MB (1.22×) | 88.5 MB (3.78×) | 50.1 MB (2.14×) |
| agent, hourly | 13.7 MB (1.00×) | 18.2 MB (1.33×) | 61.9 MB (4.53×) | 29.2 MB (2.13×) |
| laptop, hourly 08-18 weekdays | 7.9 MB (0.99×) | 10.1 MB (1.27×) | 34.6 MB (4.32×) | 19.6 MB (2.45×) |
| developer 09:00/13:00/17:30 | 5.2 MB (0.98×) | 7.4 MB (1.40×) | 25.7 MB (4.87×) | 19.0 MB (3.60×) |
| developer, mornings | 3.5 MB (0.97×) | 5.1 MB (1.40×) | 11.5 MB (3.13×) | 11.5 MB (3.13×) |
| back after 6 days off | 2.6 MB (0.94×) | 2.8 MB (0.98×) | 5.2 MB (1.85×) | 5.2 MB (1.85×) |

**Packs on disk** after the week (peak) → after the next daily maintenance; seed clone included:

| client | regime | opus (exact) | **opus final** | fable | fable h24 |
|---|---|---|---|---|---|
| agent, every minute | stock | 1,405 | 1,405 | 2,734 | 2,707 |
| | scalar | **1,280 → 1,247 (+170/day)** | 395 (484) → 215 | 444 (542) → 17 | 506 (616) → 88 |
| | vfs | 396 (485) | 395 (484) | 444 (542) | 506 (616) |
| | geometric | 242 (281) | 241 (281) | 436 (527) | 435 (529) |
| | hook | 1,405 | 232 (272) | **13 (15)** | 86 (90) |
| agent, every 15 min | stock | 523 | 523 | 607 | 588 |
| | scalar | **493 → 482 (+65/day)** | 149 (171) → 80 | 101 (107) → 14 | 159 (180) → 86 |
| | vfs | 151 (172) | 149 (171) | 101 (107) | 159 (180) |
| | geometric | 82 (89) | 81 (89) | 94 (97) | 90 (96) |
| | hook | 523 | 80 (89) | **13 (14)** | 86 (89) |
| laptop, hourly 08-18 weekdays | scalar | 48 → 39 (+5/day) | 26 → 13 | 18 → 5 | 26 → 13 |
| | hook | 56 | 12 (14) | 4 (10) | 12 (14) |
| developer 09:00/13:00/17:30 | scalar | 13 → 11 (+1.3/day) | 10 → 5 | 10 → 5 | 10 → 5 |
| | hook | 16 | 4 (6) | 4 (6) | 4 (6) |

With no maintenance (stock) every server leaves one pack per response with news, and fable
leaves the most. With maintenance the bound comes from *what maintenance can delete*. A pack
whose objects all have newer copies is deleted by `multi-pack-index expire` for free.
Otherwise it must be repacked, and Scalar's default batch size (second-largest pack + 1)
repacks only a few packs a day. So with exact covers and Scalar defaults an agent's packs grow
without bound: ~5,000 after a month for a 1-minute agent, ~2,000 for a 15-minute one. That
was the one egregious usage pattern found for opus. Supersets fix it by making old packs
deletable, and VFS for Git's fixed 2 GB batch or Scalar's `geometric` strategy fix it by
repacking enough. fable's supersets did buy this, so its design was right about Scalar; its
own hook additionally keeps an agent at 13 packs. fable pays 2–5× the bytes for this,
because it re-sends every tier and the live slab.

**What pack count costs git** (`bench/packcount.py`: the seed clone plus N of the 1-minute
agent's real packs; median of 5; microsoft/git):

| packs | absent-object lookup (2,000 ids), per id | `rev-parse` (command start) | `log -n 500` | with a multi-pack-index: absent / start |
|---|---|---|---|---|
| 1 | 5 µs | 0.8 ms | 4 ms | 4 µs / 0.8 ms |
| 101 | 64 µs | 0.8 ms | 3 ms | 50 µs / 0.8 ms |
| 1,001 | 0.49 ms | 2.5 ms | 7 ms | 0.42 ms / 1.3 ms |
| 5,001 | 2.8 ms | 10 ms | 20 ms | 2.6 ms / 3.6 ms |
| 20,001 | 12 ms | 53 ms | 98 ms | 11.6 ms / 12.6 ms |

On a miss git rescans the pack directory, so a multi-pack-index does not help. GVFS clients
miss on purpose: a blob not yet downloaded is a miss. A few hundred packs are harmless, but
thousands add milliseconds to every miss.

**The change** (`f7d9bef`, `--superset-tier 24h`, the default). A sealed daily rollup or
weekly delta reaches each warm client whole, once. A client whose cursor lies inside one, or
exactly at its end, is answered from its start once the tip has moved past it. Its cursor
then lies past the rollup, so the rollup is not sent again. This is fable's superset
tombstoning restricted to days and weeks, which is all that daily maintenance needs. opus's
rollups are re-deltified and small: 0.35–0.82 MiB a day and 1.9 MiB a week on SearchGold.
Every prefetch now carries `X-GVFS-Pack-Start/End`, so fable's hook also works against opus,
with 0 objects lost.

Over opus's own 14-day simulator (`examples/clients.rs`), warm clients get:

| mode | warm bytes ÷ ideal |
|---|---|
| `24h` (default) | 1.11–1.53× |
| `168h` (weeks only; packs bounded by a week's worth) | 1.04–1.31× |
| `never` (exact covers, the old behaviour) | 1.00–1.02× |

Validation: unit and e2e tests (the e2e test fails without the change; 55/55 pass); the
replay (0 failures, 84/84 exact cold clones); conformance 918/0; cross-client unchanged;
and the lifecycles above.

**The design's checklist, as built:**

| design item | opus (final) | fable |
|---|---|---|
| `manifest.bin` with If-Match CAS | ✅ (fs: `flock`; Azure: ETag; the loser retries 3×) | ✅ (`flock`) |
| One-time bootstrap → one baseline slab | ✅ (79 s for SearchGold) | ✅ (83 s) |
| Sub-second tailing (L0) | ✅ each edge polls the upstream (`--poll-ms`, 2 s default); one L0 slab per push; push → servable p50 0.35–0.5 s. No S3 WAL: N edges poll ADO N times | ✅ read-through on a miss (p50 0.01 s) + one live slab |
| 15m → 1h → 6h → 12h → 24h → 168h | ✅ cascading; every rollup repacked, trees re-deltified | ✅ spliced intraday; re-fetched upstream from 24h |
| Drop "intermediate trees" in rollups | ✗ by design: intermediate commits reach them | ✗ same |
| Two tracks: cold = baseline + tail; warm never gets the baseline | ✅ | ✅ |
| Warm retention window | ✅ 8 weekly deltas; retained finer slabs (15m 1 day, 1h–12h 1 week, days 8 weeks) | ✅ (`--retain-weeks`) |
| Weekly re-bootstrap (fresh walk: defragments, drops garbage) | ⚠️ the baseline grows by appending the week (6–7 s, no upstream traffic); objects orphaned by a force push stay | ✅ `--baseline fetch` (re-fetches 2.6 GB, 81.5 s) or splice |
| Point lookups: L0 + this week's slabs + baseline MPHF | ✅ | ✅ |
| Fix 1: one pack per prefetch | ✅ (covers repacked into one) | ✅ (slab bodies concatenated) |
| Fix 2: superset tombstoning | ✅ days and weeks, once per client, + interval headers (`f7d9bef`) | ✅ every tier and the live slab, + its hook |
| Fix 3: client `multi-pack-index` instead of gc | ✅ client-side; bounded with VFS for Git, Scalar and its geometric strategy | ✅ (its hook's `--midx` failed after unlinking, §4) |
| Weekly "aha" delta for warm workstations | ✅ (6 days off: 1 pack, 0.98×) | ✅ |
| Cron jitter / overlapping runs | ✅ L0 bridges any gap; the next 15m slab gets wider; CAS | ✅ |
| Cold clone = 1 pack | ✅ (84/84 exact) | ✅ |

## 12. Real blobs in their own packs (`--blobs separate`)

**The decision.** The pointer design leaves the real content outside git, so nothing
in the GVFS protocol can see it: `/gvfs/sizes` reports pointer sizes, and every
consumer needs the HFS side channel. The alternative adopted here:
* every file is a real git blob in ADO;
* commits and trees travel in one set of packs, blobs in another;
* the blob sizes a client needs to project a tree travel with the trees, as a side table.

**What it is** (opus `5870cd0`…`60e9164`, whole-history mirror `aa475fa`…`37a4ded`;
formats and algorithms in `opus/DESIGN.md`, "Stores with real blobs"). A store chooses its mode at genesis
(`pipeline --blobs separate`); inline stores are unchanged, manifest byte for byte.

| piece | what it does |
|---|---|
| sizes tables | `slabs/<id>.sizes`: sorted `(id, size)` of the blobs a slab's trees introduce; rollups union them |
| blob packs + segments + `blobs.bin` | whole-blob git packs (upstream zlib kept, or stored blocks when incompressible: byte ranges in place); 48-byte records with 64-bit offsets and lengths; a CAS'd list whose segments stay a geometric progression by blob count (O(log n) segments, each record rewritten O(log n) times) |
| pipeline, blobs first | `blob:none` window; copy the blobs its new trees reference that the store lacks; publish `blobs.bin`; only then the slab and the manifest naming that `blobs.bin` generation |
| `gvfs-daemon mirror` | the seed of the blobs: every blob of the history, planned from the store's own trees and sizes in one parallel pass (the tip's tree first, in path order), resumable, parallel, rate-capped, never re-copying what windows copied meanwhile, sizes checked; records `blobs.bin` as mirrored. Also the repair, on demand |
| edge | blobs from the packs (mapped, or from Azure through a local 4 MiB block cache, never downloaded whole); a single-flight read-through proxy for the L0 overlay's blobs, only for ids the trees name (a sealed tree's blob that a complete store lacks is proxied too, but counted as `gvfs_blobs_missing_total`); `GSZ1` sizes section on opt-in; `GET /gvfs/blobs/<oid>?offset&length`; ready only once the history is mirrored |

**Gates** (stock git judges every byte):

| gate | result |
|---|---|
| Blob packs valid, sizes exact | every pack passes `index-pack --strict`; every table and every `/gvfs/sizes` answer equals `git cat-file -s` |
| Streaming copy: chunking, OFS/REF/forward deltas, stored vs raw, wrong/missing/extra objects, bad trailers | unit tests (`blobstream.rs`) |
| Pipeline blobs first | every blob of every window byte-exact in the store, including a revert to a never-copied version; the window's table holds exactly the new sizes; `blobs.bin` published before the manifest; rollups/weekly union; GC keeps exactly what is listed |
| Old versions reused after genesis | an old side branch merged (plain git leaves its base's subtrees out of the window's pack) and a revert against an upstream with reachability bitmaps: with the mirror run before the windows and with it run after them, every blob of every commit is in the store, listed once; an edge serves them all from the store, none through the proxy |
| Recovery after a dead run | a run that published `blobs.bin` and died before the manifest: the next run copies nothing and its manifest still names that generation |
| Mirror | every blob of the history byte-exact, each listed once; the first slice (`--max-items`) holds only tip blobs; segments end a geometric progression after folding; `--scope` is gone; `SIGKILL`ed at several points, two runs leaving unpublished uploads: completes, every blob once, GC removes exactly the orphans |
| A complete store missing blobs | `blobs.bin` loses a segment: an edge stays ready, serves those blobs through the proxy (GET and POST), counts each upstream fetch in `gvfs_blobs_missing_total`; `mirror` copies them back and the edge serves them from the store |
| Storage loses a pack and a segment | edges boot and serve every blob of the history byte-exact (the lost ones proxied, counted); pipeline runs go on; `mirror` drops the losses from `blobs.bin` and copies those blobs again, every blob listed once, every listed pack present |
| Edge | every history blob loose (`fsck --strict`), POST packs (`index-pack --strict`), ranges of stored and compressed blobs (pipelined requests too), `GSZ1` equal to git's history (cold) and to the window's new blobs (warm), L0 blobs through the proxy and then from the store once sealed; 0 hot-path allocations in strict mode |
| Inline stores unchanged | opus conformance re-run at `f7a34df`: 918 passed, 0 failed. Cross-client matrix: gvfs-helper 18/18 and fable's hook 15/15; astra's sync client 2/15 as before, since it needs astra's ETag (`bench/results/*-opus-sep.json`). The 7-day replay was not re-run |
| Caches never livelock | block cache: a read larger than the budget, a read spanning more blocks than the budget holds, 8 threads racing over a 2-block cache; proxy: a request needing a cached and an uncached proxied blob with room for one pack. The three single-request cases hung before the fix |
| Stock microsoft/git 2.56 | `gvfs-helper prefetch` (no blobs), a checkout faulting every blob byte for byte (16 MiB included), an old version (from the store: nothing proxied), a sparse cone that never fetches blobs outside it |
| Azure (Azurite) | streaming block uploads; an edge serving every blob loose and in packs by ranged reads, holding only segment indexes locally |
| Beyond 4 GiB | a 4,294,979,641-byte blob: copied by a pipeline window in 7.0 s at an **11 MiB** peak RSS; served loose (hashes to its id), in a pack (`index-pack --strict`), and by ranges past 4 GiB |

A code review of the separate-mode diff raised four findings. Three were bugs, now fixed,
each with a regression test that failed before the fix:
* a pipelined `/gvfs/blobs` request could be answered with the next request's range;
* the edge's block cache and its blob proxy could livelock when one request needed more than
  their budget;
* a manifest could name an older `blobs.bin` generation than its trees needed.

The fourth was a design gap, not a bug: a window copies only the blobs of trees in its pack,
and the upstream leaves out subtrees its haves reach. With only the genesis tip mirrored, the
haves reached history the store lacked, so a later commit reusing an old version (a revert,
an old branch merged) could reference a blob that was in neither. A first fix walked every
new commit's whole tree each run (and made Azure runners download the baseline); it was
reverted. The fix is the store's contents: the mirror now copies the whole history, so
whatever a pack leaves out is in the store already, for any upstream (plain git or
bitmaps), and a window needs no walk.

**Scale: SearchGold's history in separate mode** (commits and trees real; its blobs are the
3.34M pointer files, so this measures the metadata side):

| | inline (§5) | separate |
|---|---|---|
| genesis | 79 s | 99 s: 56 s `blob:none` fetch (git), 7 s tree walk, 27 s `object-info` for 3,342,769 sizes, 6 s repack |
| baseline | 2.60 GB pack + 374 MB index | 2.27 GB pack + 238 MB index + 93.6 MB sizes table |
| cold clone | 2.60 GB | 2.27 GB; 2.36 GB with `X-GVFS-Sizes: 1` (sizes of every history blob), 0.7 s on loopback |
| `POST /gvfs/sizes`, 4,000 blob ids | — | 1–1.5 ms per request, answered from the table |

**Whole-history validation** (`bench/separate_history.py`, results in
`bench/results/separate-history-{sg,synth}.json`; stock git judges every check):

* *SearchGold's main* (5.88M commits and trees, 3,342,769 blobs of pointer content, 319 MiB),
  mirrored into a copy of the separate genesis store from the local repository: **14.8 s**
  (planning 7.6 s, one parallel pass over 5.2M trees; 226K blobs/s, 8 jobs), peak RSS 3.0 GiB
  (mostly the mapped 2.1 GiB baseline). The blobs listed are exactly git's blobs of main: 0
  missing, 0 listed twice, every size git's; all 10 packs pass `index-pack --strict`. An edge
  with no upstream (nothing can be proxied), `--alloc-guard strict`: 3,000 sampled GETs, a
  4,000-blob POST and 4,000 sizes exact; 0 hot-path allocations, 0 proxied, 0 missing.
* *Folding at that scale:* the same mirror forced to publish after every pack (160 publishes)
  folded 87 times and ended with 2 segments (3,307,098 and 35,671 blobs), rewriting 4.9 records
  per blob in all. Simulated with equal publishes, the old policy (merge everything beyond 16
  segments) rewrites 156 records per blob at 5,000 publishes, the geometric one 6.2, with at
  most 12 segments.
* *Serving blobs from blob packs vs from slabs* (the same 145,326 SearchGold blobs, the same
  binary, CPU-pinned, median of 3 interleaved rounds):

  | | separate | inline |
  |---|---|---|
  | GET blob, 64 conns | 987,648 req/s, p50 54 µs, p99 306 µs | 989,168 req/s, p50 54 µs, p99 307 µs |
  | POST 4,000 blobs, 32 conns | 3,434 req/s (1,341 MB/s), p50 9.2 ms | 3,600 req/s (1,410 MB/s), p50 8.7 ms |

  Single GETs are equal; batches are 5% slower, since a blob id first misses every slab index
  before its segment answers.
* *Real content with history:* a synthetic repository (20,208 files: text, 1-8 MB binaries, 64-512
  MB incompressible files) and 30 commits of new versions before genesis: 26,871 blobs, 4.21 GB
  in main's history. The mirror, capped at 400 Mbps so that commits land while it runs, took
  80.3 s (52.4 MB/s) at a 55 MiB peak RSS (the daemon's; the local git upstream's `pack-objects` peaked at 498 MiB). Six 15-minute runs sealed commits during the mirror,
  one restoring a pre-genesis version of a large file, one restoring a whole old directory against
  a bitmap upstream. Afterwards the store lists exactly git's 26,871 blobs, each once, in 4.02 GB
  of packs (the content's size); every pack passes `index-pack --strict`; an edge with no upstream
  returned all 26,871 blobs by POST (4.02 GB in 17.7 s) byte for byte, and 8 ranges of the largest
  blob; 0 hot-path allocations.
* *A bug the synthetic run found* (fixed in `2b64c9b`): a window running before the mirror had
  finished copied every unlisted blob its new trees name, unchanged files of changed directories
  included. One window copied 1.71 GiB of untouched large files the mirror was copying at the same
  time (1,265 records twice, 1.65 GB of duplicate packs); at TB scale a window could miss its slot.
  Until the mirror is done, a window now copies only blobs no sizes table has. Re-run: each window
  copied its 100 new blobs in 0.1 s, and nothing was copied twice.
* *Inline stores unchanged:* opus conformance re-run with this binary, 918 passed, 0 failed.

**Client side: blob packs on stock clients** (`bench/blob_clients.py`,
`bench/results/blob-clients.json`). Sixteen microsoft/git 2.56 clients with Scalar's GVFS settings
(`core.gvfs`, `core.useGVFSHelper`, Scalar's recommended config, sparse cones) follow a separate-blob
edge for 14 days of a synthetic repository (`bench/corpus_gen.py` at scale 0.05: 9.0 GB over 2,150
commits, a 6.2 GB tip at genesis), the pipeline sealing every 15 minutes. Each sync is `gvfs-helper
prefetch` then `checkout --detach <tip>`, which faults the missing blobs of the cone in (POSTs of up to
4,000 ids, each a `vfs-<sha>.pack`; a single blob as a loose object). The developer also runs `log -p`
and `cat-file` on old versions every weekday. Maintenance runs at 00:00 UTC under four regimes: none,
Scalar's default (`incremental`: incremental-repack and loose-objects), VFS for Git's (2 GB batches),
and `geometric`. All 16 clients ended complete (every blob of the cone at their tip present) and clean;
0 failures; the edge made no hot-path allocation and missed no blob.

| client (blob bytes fetched in 14 days) | packs, no maintenance | Scalar default: packs (peak), written | VFS for Git: packs, written | geometric: written; downloaded by maintenance |
|---|---|---|---|---|
| agent every 15 min, text cone (287 MB) | 743 | 84 (162), 1.1 GB (3.8×) | 73, 2.8 GB (9.8×) | 38.5 GB (134×); **7.3 GB** |
| agent hourly, binary cone (2.6 GB) | 288 | 29 (59), 1.8 GB (0.7×) | 26, 1.9 GB (0.7×) | 38.5 GB (15×); **4.9 GB** |
| developer, 3 syncs a weekday + history (592 MB) | 61 | 11 (19), 1.8 GB (3.1×) | 8, 5.4 GB (9.1×) | 24.7 GB (42×); **7.0 GB** |
| full checkout, daily (7.9 GB) | 30 | 7 (9), 9.8 GB (1.2×) | 6, 11.2 GB (1.4×) | 23.9 GB (3.0×); 0 |

* *Scalar's default works with blob packs.* Packs stay bounded (84 instead of 743 for the 15-minute
  agent), at 0.7-3.8× the fetched bytes rewritten over two weeks; maintenance took 2-3 s a day for an
  agent and 11 s a day for the full checkout. Packs of 2 GB or more are never rewritten (the batch size
  is capped at 2 GB). VFS for Git's fixed batches rewrite more for the same bound.
* *The `geometric` strategy must not be used in a GVFS enlistment.* Its `pack-objects --stdin-packs`
  walks history for path hints, and every blob the walk touches that the client lacks is faulted in
  through gvfs-helper (traced: `pack.usePathWalk` on or off): the 15-minute agent, whose cone needed
  287 MB, had its maintenance download 7.3 GB and write 38 GB. That is the client, not the server: it
  happens against ADO too. Scalar sets `incremental`; `git gc` and `repack -a` traverse history as well.
* *A single large blob now comes as a pack* (`8ac4c46`). A checkout that needs one blob POSTs one id,
  and gvfs-helper installs a loose answer as is; Scalar's loose-objects task then inflates and deflates
  it into a pack: 14.9 s and another 1.07 GB written for a 1 GiB blob (SearchGold's largest file is
  94 GB). A pack costs gvfs-helper one `index-pack` (3.1 s) and is never deflated again, so the edge
  answers a single blob of 16 MiB or more with a pack when the client accepts both. "A pack" is only an
  envelope here: the same stored bytes, 16 bytes more on the wire (a 12-byte header, a 20-byte trailer).
  Every such blob already sits alone in a pack in the store, trailer included, so the edge sends that
  file as it is (`sendfile`): 0.01 s of edge CPU for 1 GiB, as for a loose object (computing a fresh
  trailer took 0.38 s). GETs (gvfs-helper's on-demand reads) stay loose, as gvfs-helper requires.
* *Without maintenance* a 15-minute agent gains ~50 packs a day, mostly prefetch packs (§11).

**Two weeks on a 170 GB repository** (`bench/corpus_gen.py` at scale 1.0, `bench/replay_separate.py`,
`bench/results/replay-separate.json`). The corpus: 170.5 GB of real content in 32,553 blobs (SearchGold's
size distribution: p50 12 KB, p99 49 MB, 20 blobs over 1 GB, the largest 4.6 GB, two over 4 GiB) and
2,360 commits, a 124 GB tip at genesis, ~3 GB of new content a weekday; reproducible (41 blobs
regenerated, up to 4.3 GiB, match) and `git fsck --full --strict` clean. Genesis took 1.8 s; the
mirror copied the history (126.7 GiB, 29,406 blobs) in **150.7 s** (~900 MB/s with 16 jobs, from a
local git upstream) at a 98.8 MiB peak RSS (git's `pack-objects` on the upstream side peaked at
4.4 GiB). Then 14 days of pushes were replayed on the pipeline's cron, with a live edge (L0 one per
push, its blobs read through) and the §6 clients, each with a sparse cone: every prefetch with
`X-GVFS-Sizes: 1`, then the cone's missing blobs in POSTs of up to 4,000 ids, every entry inflated and
hashed by the harness; CI cold clones every 2 hours fetch their cone (4 GB) from scratch.

* *0 failures in 21,612 client events.* Every prefetch complete (as in §6), never a blob in it; every
  GSZ1 size git's, and every file of every cone sized before its blob was fetched; every blob
  response exactly the ids asked for, each git's. All 168 cold clones exact: 3.4 MB of commits and
  trees in 0.01 s (median), then 5,610 blobs (4.0 GB) in 6.2 s (max 9.7 s); 686 GB to CI in all.
* *Edge:* 948,114 blobs from the store, 548 through the proxy (L0, before the pipeline sealed them), 0
  missing, 0 errors, 0 hot-path allocations; 1,406 L0 ingests.
* *Pipeline:* 1,340 runs; 15-minute windows 0.10 s on average, 2.4 s at most (a window copying the
  day's rebuilt binaries first).
* *Clients over the two weeks:*

  | client (cone) | requests | blob bytes | blob requests (max) | prefetch ÷ git's ideal |
  |---|---|---|---|---|
  | agent, every minute (src, 2 GB) | 20,102 | 0.16 GB | 128 (0.10 s) | 1.55× |
  | agent, every 15 min (services, lib) | 1,340 | 0.68 GB | 222 (0.72 s) | 1.56× |
  | agent, hourly (models, 27 GB) | 335 | 8.6 GB | 156 (1.9 s) | 1.62× |
  | developer, hourly (web, sdk, tests) | 110 | 2.0 GB | 94 (4.0 s) | 1.48× |
  | developer, 3× a day (bin, config) | 30 | 4.6 GB | 30 (1.0 s) | 1.57× |
  | developer, mornings (everything) | 10 | 31.4 GB | 10 (8.3 s) | 1.35× |
  | back after 6 days off | 2 | 5.5 GB | 1 (7.1 s) | 0.96× |
  | stale (cold cursor) | 1 | 0.78 GB | 1 (1.5 s) | 1.66× |

  Blob bytes equal the content's (text compressed, binaries stored). Prefetches carry megabytes over
  the two weeks, 1.35-1.66× git's own `blob:none` pack for the same range (the day supersets of §11),
  next to gigabytes of blobs.

**Seeding real content** (`bench/separate_seed.py`): a synthetic corpus shaped like
SearchGold (20,216 files, 5.99 GB of which 4.7 GB in 16 large incompressible files),
`mirror --jobs 8` copied it in 35.5 s (169 MB/s), at an 84 MiB peak RSS. perf: 95% of the CPU
was the local git upstream (`pack-objects`: SHA-1DC, zlib), 5% the daemon (mostly verifying
SHA-1s), so ADO's serving rate, not the daemon, will set the seed time.

**Trade-offs and open questions:**

* *Mirroring the whole history* costs what it weighs (SearchGold's main: 3.34M pointers
  standing for 256 TiB), once, before edges serve: resumable, parallel, rate-capped, the tip's
  blobs first. ADO's serving rate decides how long it takes.
* *Repair is on demand.* No run checks or repairs the store by itself. If storage loses a pack
  or a segment, edges and runs carry on (warnings; edges proxy those blobs and count them as
  missing), and `gvfs-daemon mirror`, run again, repairs `blobs.bin`. A lost pack in Azure shows
  only when it is read (5xx for its blobs until the repair).

* *ADO capabilities:* protocol v2 `filter blob:none`, `object-info` and fetching blobs by id (genesis,
  the 15-minute runs, L0, the mirror) are supported by ADO (confirmed, not exercised here: every run
  above used a local git upstream). `/gvfs/objects` and `/gvfs/sizes` are used when the upstream is
  GVFS.
* *Edges cache blob packs in object storage by 4 MiB blocks* (`--blob-cache-mib`): a read fetches
  each run of missing blocks with one ranged GET, so a checkout's blobs, which sit together in path
  order, cost a few requests. Repeat reads cost none (tested on Azurite). The cache starts cold
  after a restart.
* *Runner downloads:* a window run reads every segment index (48 B per blob) and the manifest's
  sizes tables: ~220 MB at SearchGold's scale, kept in `--cache-dir` across runs (an ephemeral
  runner downloads them every run).
* *Storage:* small blobs are stored whole (no deltas), so any blob leaves as its own bytes. Git
  stores whole files, so the chunk-level dedup HFS had is gone: a small edit to a 10 GB file
  costs 10 GB again, upstream and here.
* *Not done from the plan:*
  * a reference client for `GSZ1` and ranges (the replay's harness plays that part);
  * reachability GC of blob packs.

## 13. CPU and wait analysis (BPF, perf)

**Method** (`bench/profile_bpf.py`). The edge runs a frame-pointer build pinned to 8 CPUs (4 cores
and their SMT siblings; loadgen on the other CCD). Each workload is profiled several ways:

* on-CPU with bpftrace (kernel and user stacks), and NMI `perf record -e cycles` for exact
  attribution;
* off-CPU with bcc `offcputime`;
* run-queue latency from bpftrace on the sched tracepoints;
* syscalls per request, and uprobes on arena lease and hold.

The stores are SearchGold after a week of pipeline runs on the replay's cron
(`bench/build_store.py`: 12 live slabs, as in production; the one-slab store first profiled hid
most lookup costs), separate and inline, plus the synthetic corpus' large blobs. `ab` mode
interleaves two builds on the same CPUs (3 rounds, medians); `bench/ab_responses.py` requires
their responses to be byte-identical:

* 960 requests per store: batches of 1 to 20,000 ids with repeats, trees, commits and
  recent-slab objects;
* prefetches compared by SHA-256: cold with sizes, cold with `.idx`, warm (4.8 and 5.5 GB);
* a sample also judged by git (`index-pack`, sizes against `cat-file`);
* `--upstream`: both edges also serve a live tip 5 days ahead of a store (an upstream with
  `pack.threads=1`, so both edges' L0 slabs and proxy caches are the same). Its 1,869 new
  blobs are mixed in, fetched the first time and cached after, interleaved with the store's:
  1,032 responses, 4,312 proxied blobs, all byte-identical.

A code review of the first version found that a batch mixing store blobs with proxy-cached
ones sent them in a different order than before: the same objects, a valid pack, but not
byte-identical. Upstream mode reproduces it (21,000 entries, the first difference at entry
41). The fix keeps one list in request order.

End to end with the final build:
* opus's tests (59 + 3 + 7 + 9);
* conformance 918/0;
* the 14-day `--blobs separate` replay on the synthetic corpus (`bench/replay_separate.py
  --corpus corpus-s`, a live edge proxying the L0 overlay's blobs; results in
  `bench/results/replay-separate-corpus-s.json`): 21,954 client requests with 0 failures,
  168 of 168 cold clones exact, 56,631 blobs from the store and 766 proxied, 0 missing,
  0 errors, 0 hot-path allocations.

| Commit | Found | Change | Effect (8 CPUs; separate / inline store) |
|---|---|---|---|
| `5bb4df3` | Separate store: a blob id probed every slab's index first, three times per POST (commit check, plan, `plan_blobs`), and sizes binary-searched every slab's sizes table (50% of sizes CPU in `memcmp`) | Blob packs' segments first (slabs hold only commits and trees there) | POST 4,000 blobs 1,703 → 4,320 req/s; sizes 1,852 → 11,292 |
| `5bb4df3` | `lock incq` on one `Arc<BlobPack>` count per blob, from every core: 25% of POST cycles. Dedupe in the arena's 10 MB `seen` table: a cache miss per insert, 11% | Blobs noted by segment index; a dedupe set sized for a batch | POST → 5,037 req/s (2.96×), 4.7 → 1.6 ms CPU, p99 45 → 15 ms |
| `5bb4df3` | `ArenaPool::lease` reset the arena (clearing a 4,000-id set) under the pool lock: 22% of POST off-CPU time in futex waits | Reset after unlocking | (part of the above) |
| `5bb4df3` | Inline store: an id only the baseline has (most of them) paid an MPHF probe in each of 11 newer slabs; 63% of POST and 87% of sizes CPU in lookups | Blocked Bloom filters, one per view over all slabs before the baseline, one per such slab: no false negatives, same answers | Inline POST 2,112 → 3,155, sizes 6,049 → 11,190 (1.85×) |
| `5bb4df3` | `plan_objects` looked every id up twice (commit check, then plan): 22% of inline POST CPU | The check's lookups kept for the plan (up to 8,192 ids) | Inline POST → 3,357 req/s (1.59×), 3.75 → 2.36 ms CPU, p99 39 → 18 ms |
| `a4734cb` | A cold prefetch's `.idx` and `GSZ1` (165 MB and 93 MB on SearchGold) were copied into the socket: each byte also cost a socket buffer page zeroed by `init_on_alloc`. 4% of the bytes, 27% of the CPU | Sections of 1 MiB or more moved to a memfd once (mapped, no second copy) and sent with `sendfile` | Edge CPU per cold prefetch: with `.idx` 87.8 → 51.4 ms (27.0 → 34.9 req/s), with sizes 73.4 → 50.5 ms |
| `75fd11e` | Batched lookups waited for one DRAM read at a time: each id's record sits at a random place in an index far larger than the caches (the blob set's 160 MB, the baseline's 9M records). One load was 68% of a sizes request | Each batch loop has the CPU start loading the record of the id 16 places ahead, and the POST send loop the blob entries ahead, so the misses overlap. `_mm_prefetch` needs one `unsafe` block, isolated in `crates/gvfs-prefetch`; plain loads in its place (safe) were 4-11% slower | POST 4,000 blobs 4,824 → 9,035 req/s (1.87×), sizes 10,593 → 20,487 (1.93×); inline 1.14× and 1.77× |

All of it against the original build, interleaved (df4a1df → 75fd11e, 8 CPUs):

| | separate blobs | inline |
|---|---|---|
| POST 4,000 blobs | 1,634 → 9,040 req/s (5.5×), CPU 4.86 → 0.86 ms, p99 47 → 9 ms | 2,011 → 3,655 (1.8×) |
| sizes 4,000 | 1,805 → 20,588 (11.4×), CPU 4.40 → 0.38 ms | 5,770 → 18,381 (3.2×) |
| blob GET | 961K → 1.00M (+4.5%) | 983K → 1.02M (+3.3%) |

Blob GETs rose 7% (separate) and 4% (inline). Large-blob paths were already cheap and did not
change: about 40 µs of CPU per 1 MiB range at 48K ranges/s, and whole large blobs leave
through `sendfile`.

**What remains is mostly inherent:**

* *Blob batches* (separate store, 0.86 ms of CPU per 4,000 blobs):
  * the pack trailer's SHA-1, 28%. It is the pure-Rust `sha1` crate on the CPU's SHA
    instructions (no OpenSSL anywhere; `openssl speed` was only the yardstick: 2.94 GB/s per
    core, the crate 2.91; `ring` 0.79). One stream cannot go faster: each 64-byte block needs
    the previous block's result, a chain of dependent SHA instructions, and git checks the
    trailer, so it cannot be skipped (a single large blob's own stored pack is sent without
    hashing);
  * the lookups, ~35%: computing each id's slot, then its record. The prefetches now mostly
    wait for address translation (an L2 TLB miss every ~550 cycles, an estimated 7-15% of
    the batch);
  * parsing the ids from the JSON, 7%; the dedupe set, 7%; copying entries, 4%; the kernel
    zeroing socket buffer pages, 3%.
* *Small GETs* (1.07M req/s at 256 connections, all 8 CPUs busy):
  * the TCP stack takes about half the cycles and the scheduler 15-20%: one `recvfrom` and one
    `sendto` per request, and one context switch, since a worker blocks for the next request;
  * user code is ~10%;
  * PSI accounting is 2.3% (`psi=0` on the kernel command line removes it);
  * the receive timeout that implements the linger arms and cancels a timer per wait (~1.5%).
* *Cold prefetch:* page cache reads and socket buffers. `init_on_alloc`'s page zeroing is 14%
  of it, part of the hardening kept on this host.
* *Pipeline:* a 15-minute window takes 60 ms (max 114 ms) and a daily rollup 164 ms on
  SearchGold. Separate stores spend 44% of that verifying sizes tables' SHA-256 (the
  baseline's is 93 MB).
* *Mirror:* 85% of the CPU is the local git upstream (`pack-objects`), 8% the daemon (SHA-1 of
  what arrives).

**Not done (trade-offs):**

* An event-loop serving model would avoid the context switch per request, but only at
  saturation. It is a rewrite of the connection model (non-blocking sockets, partial writes,
  `sendfile` backpressure), for at most the scheduler's 15-20% of small-GET CPU. §14 then
  measured it and did it: the saving was larger (22–35%), at every concurrency, and slow
  clients stopped holding threads.
* A per-worker cached view would skip the `RwLock` and `Arc` atomics per request (≤3% of GET).
  It changes every handler's signature.
* Hashing the output buffer in large chunks would hash 100-byte entries at 2.9 GB/s instead of
  2.5 GB/s (~4% of a blob batch).
* Index files mapped with 2 MiB pages would make most of the remaining page walks go away
  (~7-15% of a blob batch, estimated). The page cache maps them in 4 KiB pages; huge pages
  would mean copying the indexes into anonymous memory, at the RAM that costs.

**Measurement pitfalls found on the way:**

* loadgen builds its 4,096 request bodies (4,000 ids each, ~4.5 s) before it sends anything, so
  a CPU window opened after a fixed delay measured an idle edge. That looked like a server
  stalling at half its CPUs. The harness now waits until the edge is busy.
* bpftrace's `profile:hz` (cpu-clock, an hrtimer) puts ~50% of small-GET samples in
  `finish_task_switch`: samples due while interrupts were off during a switch land when they
  come back on. NMI `cycles` sampling does not.
* bcc's `runqlat` does not compile on kernel 7.3, and `runqlen` overlays an outdated `cfs_rq`
  and always reads 0. The sched tracepoints in bpftrace work.
* `rev-list --filter=object:type=blob` still prints the commit it was given (unless
  `--filter-provided-objects`). In a POST mix, that one commit (which expands to every tree)
  took 20-30% of the CPU.

## 14. Connection layer: std threads vs tokio (spike, then the port)

**Question.** Today the edge runs one request per thread: 256 workers, with idle keep-alive
connections parked in an epoll set until their next request. Would an async runtime serve
better, and if so with hyper or with the edge's own HTTP/1 framing?

**Method.** `opus/examples/spike_tokio.rs` serves the same store on tokio 1.54 (8 workers) with
the edge's own building blocks: request parser and router, view lookups, loose-object splice
plans, prefetch plans, the sizes loop. It runs in one of two modes:

* *own*: tokio sockets with the edge's HTTP/1 framing. A connection takes buffers from a pool
  only while it has a request in progress. File ranges go out with `sendfile` whenever the
  socket is writable.
* *hyper*: hyper 1.12's HTTP/1 server. Bodies are `Bytes`: small pieces are copied, slab ranges
  are sliced from mmap-backed `Bytes` made once per slab.

Endpoints: config, GET of an object (blobs and whole slab entries), prefetch (up to date, cold,
warm, with sizes, with `.idx`) and POST sizes. Not covered: POST `/gvfs/objects`, deltas as
loose objects, blob ranges, the proxy and the live tip.

`bench/spike_tokio.py check` found every response identical to `gvfs-daemon serve`'s (status,
content type, body):

* 4,318 GETs;
* 9 sizes POSTs;
* 7 prefetch streams: cold (2.27 to 2.44 GB, with and without sizes and `.idx`) and warm.

`bench/spike_tokio.py bench` ran on the week-old SearchGold store with separate blobs, the
edge on 8 CPUs and loadgen on 16. It took medians of 3 interleaved rounds (round-to-round spread
within 2%), plus 2 rounds of the slow-client and idle-connection tests. Results are in
`bench/results/spike-tokio.json`.

Small requests (req/s · p99 µs · edge CPU µs per request):

| | threads (today) | tokio + own HTTP/1 | tokio + hyper |
|---|---|---|---|
| config, 64 connections | 1.08M · 298 · 4.59 | **1.29M · 142 · 3.31** | 1.22M · 176 · 3.70 |
| config, 256 | 1.14M · 920 · 4.82 | **1.54M · 332 · 3.06** | 1.44M · 353 · 3.45 |
| config, 1,024 | 1.15M · 15,647 · 4.79 | **1.75M · 1,165 · 2.97** | 1.59M · 1,299 · 3.37 |
| GET blob, 64 | 1.00M · 320 · 4.99 | **1.21M · 133 · 3.80** | 1.15M · 145 · 4.17 |
| GET blob, 256 | 1.07M · 983 · 5.25 | **1.40M · 379 · 3.58** | 1.32M · 393 · 3.91 |
| GET blob, 1,024 | 1.08M · 15,703 · 5.20 | **1.55M · 1,365 · 3.49** | 1.45M · 1,369 · 3.87 |
| prefetch up to date, 64 | 1.06M · 299 · 4.66 | **1.27M** · 178 · **3.42** | 1.21M · 123 · 3.80 |
| prefetch up to date, 256 | 1.12M · 930 · 4.93 | **1.51M · 343 · 3.17** | 1.41M · 358 · 3.55 |
| prefetch up to date, 1,024 | 1.13M · 15,671 · 4.89 | **1.71M · 1,217 · 3.07** | 1.56M · 1,291 · 3.49 |
| allocations per request | 0 | 0 | 5-6 |

Large and CPU-heavy requests:

| | threads (today) | tokio + own HTTP/1 | tokio + hyper |
|---|---|---|---|
| Cold clones, 8 at a time (2.36 GB each): edge CPU per GB · MB/s | 23 ms · 77,401 | 24 ms · 81,617 | 461 ms · 14,840 |
| Sizes of 4,000 ids, 32 connections: req/s · p99 · CPU per request | 20.6K · 3.95 ms · 376 µs | 20.4K · 2.68 ms · 380 µs | 20.5K · 2.89 ms · 379 µs |

GETs beside other load (GET req/s · p50 · p99 in µs):

| | threads (today) | own, sizes inline | own, sizes yield every 256 ids | own, sizes on `spawn_blocking` | same, those threads at nice 10 | hyper, sizes inline |
|---|---|---|---|---|---|---|
| 64 GET connections beside 32 sizes connections | 462K · 59 · 1,018 | 50K · 1,267 · 2,595 | **814K · 74 · 156** | 129K · 428 · 1,445 | 149K · 374 · 1,265 | 48K · 1,316 · 2,915 |
| … the sizes requests: req/s · p99 | 13.7K · 8.6 ms | 19.9K · 2.9 ms | 11.5K · 4.5 ms | 18.9K · 6.0 ms | 18.8K · 6.9 ms | 19.8K · 3.1 ms |

| | threads (today) | tokio + own HTTP/1 | tokio + hyper |
|---|---|---|---|
| 64 GET connections beside 400 cold clones read at 80 KB/s each: GET req/s · p99; clones progressing | **0** (every worker in `sendfile`); 256 of 400 | 1.19M · 234 µs; 400 | 1.15M · 135 µs; 400 |
| 64 GET connections beside 30,000 idle keep-alive connections: GET req/s · p99; resident memory per idle connection | 1.01M · 318 µs; ~0 KB | 1.21M · 154 µs; 2.2 KB | 1.17M · 112 µs; 7-12 KB |

**Findings:**

1. **tokio with our own HTTP/1 is cheaper per small request at every concurrency: 24-38% less
   CPU.** The context switch per request goes away, because a worker runs ready connections back
   to back. Its tail stays bounded at 1,024 connections: p99 1.2-1.4 ms against today's 15.7 ms.
   Today 1,024 connections share 256 workers, and a parked connection waits for one.
2. **hyper's server is out.** On small requests it costs 9-14% more CPU than our own framing
   and makes 5-6 allocations per request. It holds 3-5× the memory per idle connection, because
   it keeps a read buffer. The decisive cost is that it has no `sendfile`: every byte of a clone
   is copied into the socket, and the kernel first zeroes each socket buffer page
   (`init_on_alloc`). A cold clone costs 20× the CPU (461 vs 24 ms per GB) and runs at a fifth
   of the throughput. The edge's main job is streaming gigabytes of packs.
3. **CPU-heavy requests have to yield.**
   * Inline, a 380 µs sizes request holds its worker, and the GETs queued behind it wait: p50
     59 → 1,267 µs.
   * tokio's blocking pool helps little, because its threads share the CPUs with the workers as
     equals.
   * Lowering those threads' priority (nice 10, checked with `ps`) helps little more: a woken
     worker still waits out the running thread's time slice.
   * Yielding to other tasks every 256 ids (~20 µs of work) bounds the wait: GET p99 156 µs
     (6.5× better than today) at 1.76× today's GET throughput. Sizes requests get 16% less
     throughput than today (11.5K vs 13.7K/s) with half of today's p99. Every CPU-heavy loop
     in a port needs such yield points: blob-batch planning, the pack's SHA-1, delta
     resolution.
4. **Slow clients stall today's edge.** 256 slow downloads take every worker, and the edge
   serves nothing else: 0 GETs in 10 s, while 144 more downloads wait for a worker. On tokio a
   slow client parks only its own task, and GETs are unaffected.
5. **Idle connections:** today's epoll parking is the cheapest, at ~0 KB each. tokio's 2.2 KB
   is a parked task (its future and timer, no buffers): 220 MB per 100,000 idle clients.

### The port

The edge now runs this way (`opus/src/serve/`):

* **Connections:** a tokio runtime with one worker per CPU (`--workers`) and a task per
  connection (`conn.rs`). The edge's own HTTP/1.1 runs on tokio's sockets (`httpd.rs`): it
  uses `sendfile` whenever a socket is writable, and every wait has a deadline. Removed:
  `conns.rs`, the 256 threads, `--linger-ms`, `--hold-ms` and the reaper.
* **Heavy work:** handlers are async. CPU-heavy loops yield every `--quantum-us` (100) of work
  (`budget.rs`). Plans built once, and large delta chains, run in `block_in_place`. Request
  arenas sit behind a tokio semaphore.
* **Fetches** are async too:
  * `src/aclient.rs`: hyper's client with tokio-rustls, pooled connections, and one deadline
    over TCP, TLS and the HTTP handshake;
  * Azure ranged reads and the block cache;
  * the proxy (`AsyncUpstream` over smart HTTP, GVFS or a local `upload-pack`); what arrives
    is verified on tokio's blocking pool through a bounded queue.

  The live loop, the pipeline and the mirror keep the blocking client on their own threads.
* **Errors** are built in cold sections, crate-wide (`util::cold`). A failing request never
  counts as a hot-path allocation.

**Validation of the final build:**

* **Tests:** lib 57, Azure emulator 3, e2e 8, separate 10 (+1 ignored). Two of them are new:
  malformed requests and the idle cap; and the async upstream over local, smart-HTTP v2 and
  GVFS transports, with git as the oracle.
* **Byte-identical to the thread-per-request build** (`bench/ab_responses.py`): 1,920
  responses on the separate store, 1,920 inline, and 2,064 with a live upstream (8,623
  proxied blobs). This includes 4.8–5.5 GB of prefetch streams compared by SHA-256. 0
  hot-path allocations, 0 errors.
* **Conformance** 918/0. **Clients:** gvfs-helper 18/18, fable's hook 15/15, astra's sync
  2/15 (as before: it needs astra's ETag).
* **14-day `--blobs separate` replay** on the synthetic corpus: 21,635 events, 0 failures,
  168/168 cold clones exact, 56,632 blobs from the store and 765 proxied, 0 missing, 0
  hot-path allocations (`bench/results/replay-separate-corpus-s-tokio.json`).

**A code review found four problems, all fixed:**

* A malformed request head allocated its error inside the hot-path scope. Under
  `--alloc-guard strict`, one bad request aborted the edge.
  * The fix (building errors in cold sections) also covers an older case: an over-capacity
    POST batch.
* At the `--max-idle-conns` cap, a new connection was reset before its first request. The
  cap now applies only to a connection going idle after a response it has actually sent.
  "After the first wake" was not enough: tokio may wake a task with nothing to read, which
  made a new test fail one run in six.
* The async client's TLS handshake had no deadline.
* The block cache fsynced every 4 MiB block, and the proxy verified blobs on the runtime's
  workers. The cache is no longer fsynced (it starts empty after a restart anyway), and
  verification runs on the blocking pool.

**Performance problems found during the port:**

* **Batches of blobs:** the first port's 4,000-blob POST cost 24% more CPU. `Out::bytes` awaited
  once per piece, and each await stored its arguments into the task and read them back. That
  read waited on the store buffer: 17.7% of the cycles sat on one `movups`. Pieces that fit
  in the buffer are now copied by a synchronous `put`, which brought the overhead down to 6%.
* **Sizes:** the JSON item buffer lived in the async function's state, which turned its
  fixed-length copies into `memmove` calls (17% of the request). It is now built in a
  synchronous helper.
* **Idle connections:** a task's future is sized for its largest path, so every connection's
  task was 11.4 KB. The proxy's fetch chain, with a TLS handshake inline, set that size.
  Rare paths now go on the heap only when they are taken (`budget::cold`), and arenas are
  boxed. A task is now 2.8 KB. Measured with 30,000 idle clients, each costs 3.4 KB; it
  was 11.6 KB.
* **Buffer pool:** it kept the most buffers it had ever needed. It now keeps at most twice
  its startup count.

**Final A/B:** thread-per-request at `5dada58` against the tokio edge, sg7-sep, medians of 3
interleaved rounds.

| Small requests: req/s · p50 · p99 (µs) · CPU per request | thread per request | tokio |
|---|---|---|
| config, 64 / 256 / 1,024 connections | 1.09M · 50 · 297 · 4.56 µs / 1.14M · 208 · 919 · 4.81 / 1.15M · 215 · 15,647 · 4.76 | **1.26M · 48 · 129 · 3.41** / **1.50M · 164 · 336 · 3.19** / **1.69M** · 564 · **1,226 · 3.11** |
| GET blob, 64 / 256 / 1,024 | 1.01M · 53 · 319 · 4.98 / 1.07M · 224 · 961 · 5.25 / 1.08M · 230 · 15,703 · 5.21 | **1.19M · 51 · 130 · 3.88** / **1.38M · 178 · 369 · 3.65** / **1.52M** · 629 · **1,371 · 3.58** |
| prefetch up to date, 64 / 256 / 1,024 | 1.06M · 51 · 299 · 4.66 / 1.13M · 212 · 912 · 4.93 / 1.13M · 218 · 15,663 · 4.88 | **1.24M · 49 · 117 · 3.53** / **1.46M · 168 · 349 · 3.29** / **1.63M** · 585 · **1,320 · 3.21** |

| Heavy and mixed | thread per request | tokio |
|---|---|---|
| Cold clones, 8 at a time: edge CPU per GB · MB/s | 24 ms · 83,641 | 24 ms · 84,652 |
| POST 4,000 blobs, 32 connections: req/s · p99 · CPU | **9,087** · 9.1 ms · **860 µs** | 8,567 · **5.0 ms** · 913 µs |
| Sizes of 4,000, 32 connections: req/s · p99 · CPU | **20,501** · 4.0 ms · **378 µs** | 20,001 · **2.4 ms** · 388 µs |
| 64 GET connections beside 32 POST: GET req/s · p50 · p99; POST/s · p99 | 363K · 84 · 1,354 µs; 6,792 · 15.9 ms | **416K** · 152 · **295 µs**; 6,707 · **6.7 ms** |
| 64 GET connections beside 32 sizes: GET req/s · p50 · p99; sizes/s · p99 | **466K** · **40** · 1,081 µs; 13,289 · 9.1 ms | 401K · 157 · **308 µs**; **15,742 · 3.2 ms** |
| 64 GET connections beside 400 clones read at 80 KB/s: GET req/s · p99; clones progressing | **0**; 256 of 400 | **1.19M · 142 µs; 400** |
| 64 GET connections beside 30,000 idle ones: GET req/s · p99; memory per idle connection | 1.01M · 319 µs; ~0 | **1.21M · 94 µs**; 11.6 KB here, **3.4 KB** with the fix above (1 round) |

The quantum, measured with 1 round of 8 s each (GET on 64 connections beside 32
connections of POSTs or sizes):

| `--quantum-us` | GET req/s · p50 · p99 beside POSTs | POST/s | GET req/s · p50 · p99 beside sizes | sizes/s |
|---|---|---|---|---|
| 20 | 833K · 74 · 136 µs | 4,854 | 803K · 76 · 148 µs | 11,154 |
| 50 | 584K · 106 · 202 µs | 5,998 | 544K · 113 · 224 µs | 13,928 |
| **100** (default) | 418K · 151 · 293 µs | 6,681 | 397K · 158 · 312 µs | 15,553 |
| thread per request | 363K · 84 · 1,354 µs | 6,792 | 466K · 40 · 1,081 µs | 13,289 |

**Trade-offs:**

* **Fairness at 1,024 connections:** the median rises from 215–230 µs to 564–629 µs, and
  p99 falls from 15.7 ms to 1.2–1.4 ms. The thread server served 256 connections at a time
  while the other 768 waited. tokio serves all of them in turn.
* **GETs beside heavy requests, at the default quantum:** p99 is 4× better and heavy
  requests keep their throughput, but the GET median is higher (40–84 → 152–157 µs). The
  kernel used to give a woken GET thread the CPU ahead of a busy one; now a GET waits for the
  heavy requests ahead of it to reach a yield. A shorter quantum (20 µs) gives the GETs
  1.8–2.3× the throughput and p99 ~140 µs, at 16–29% fewer heavy requests.
* **Heavy requests alone** cost 3–6% more CPU (the yields' clock reads, the async state),
  and their p99 halves.
* **Idle connections** cost 3.4 KB each (a parked task): 340 MB per 100,000 idle clients.
  With threads, a parked connection cost almost nothing.
* **Dependencies added:** tokio, plus the client side of hyper, with hyper-util,
  http-body-util, bytes and tokio-rustls.

### Lessons

* **The benchmarks flattered the design.** Every load test used fast clients at no more than
  256 connections, which is the thread pool's sweet spot. Its two failure modes need slow
  clients or many clients to show:
  * 256 slow readers stall every worker, and then nothing else is served;
  * busy connections beyond the 256 workers wait 15 ms.

  DESIGN.md stated the limit ("a response in progress holds its worker until it is
  written"), but no benchmark tested it.
* **A measured cost was dismissed by argument.** §13 measured the scheduler at 15–20% of
  small-GET CPU, with one context switch per request. It then reasoned that an event loop
  would help "only at saturation". That was wrong: with 64 connections on 8 CPUs another
  connection is always ready, so tokio runs them back to back without sleeping. It saves
  22–35% at every concurrency tested. The spike took about an hour and settled it.
* **Workarounds pointed at the wrong model.** The linger, the hold, the epoll parking table,
  its reaper and its generation tokens all worked around thread-per-request, and none of
  them could fix blocking writes.
* **Medians hid the unfairness.** At 1,024 connections the thread server's median looked
  better than tokio's, while its p99 was 12× worse.
* **Async is not free in tight loops.** An await per small piece cost 17% of a blob batch,
  and a buffer kept in an async function's state turned fixed-length copies into `memmove`
  calls. Inner loops should stay synchronous and await only at I/O.
* **tokio does not preempt.** `spawn_blocking` and a lower priority did not protect latency;
  explicit yields did. Their quantum is a policy: small-request latency against
  heavy-request throughput.
* **A task's memory is that of its largest path.** One rarely taken path (a TLS handshake
  inline) made every connection five times larger until it was boxed.
* **The guard rails caught regressions within minutes.** They were the strict allocation
  guard, the byte-identity harness, the replay and an independent code review. Between them
  they caught:
  * tokio allocating as a socket is closed;
  * one malformed request able to abort the edge;
  * the idle cap resetting new connections;
  * a TLS handshake without a deadline.
