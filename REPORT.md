# gvfs-daemon: five implementations, one standard comparison

**Recommendation: opus**, with the fixes and optimizations made in this branch (`opus/`, commits
`cdda1ae`…`9b736e2`). It is the only implementation that is correct on every check *and*
fastest or near-fastest on every workload that moves real bytes. Section 1 explains the call
and its trade-offs; sections 3–8 are the evidence; section 9 lists every defect found, per
implementation, including the opus defects this comparison found and fixed.

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
| Warm-client bytes vs git's ideal (7-day replay, 7 client patterns) | **0.97–1.04×** | 2.3–7.2× | 3.1–5.4× | 3.0–9.3×, then the pipeline aborts at its first weekly | — |
| 7-day replay: missed objects / cold clones exact | **0 / 84 of 84** (3 builds) | 0 / 84 of 84 | 0 / 84 of 84 (1 non-canonical `.idx`) | 0 / — (aborted) | — |

**Why opus.** With this branch's changes it ties sol on the tiniest requests (GET of a commit or
small blob at ~1M req/s, 4.9 µs of CPU per request vs sol's 5.3) and is within 4 % of sol and
13 % of fable on up-to-date prefetch. On everything that costs real CPU or bandwidth — batched
POSTs, sizes, cold clones (`sendfile`, ~10× less CPU than sol/fable), commit closures, genesis
— it is the fastest, by 1.05× (genesis vs fable) to 2.3× (sizes vs sol) over the runner-up,
without a single error, at the lowest memory after fable. Its warm-client traffic is the closest
to git's own ideal of the five (§6). Correctness: it passes every conformance, cross-client,
JSON, scale and replay check after the fixes below.

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
  (~28 page faults per GET); warm clients receive 2.6–6× git's ideal bytes on the synthetic
  lifecycle (superset rollups by design, for its client-side tombstoning hook).
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
| opus | 0 warnings | 49/49 as shipped (incl. a gvfs-helper e2e); 53/53 with the 4 added here (`cargo test --workspace`) | 2 tiny crates |
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
below show one opus column. The last commit, `9b736e2` (listen backlog, linger),
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
  creates the fewest because it never re-sends. fable's own `client-prefetch` hook can tombstone
  subsumed packs instead (its superset rollups exist for that hook).

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
with opus's own suite (53 tests), the conformance suite (918/918), the cross-client suite,
and the SearchGold scale checks.

| Commit | Change | Why (evidence) | Effect |
|---|---|---|---|
| `50c4cc5` | Cold plan: repack the tail excluding the baseline when they share objects; keep the cover's end as the plan's timestamp | Replay: SearchGold cold clones carried 1–11 duplicate entries (`index-pack --strict`: "The same object … appears twice in the pack"). Reproduced on the synthetic corpus once upstream bitmaps are off (revert / restored tree) | Conformance 903/918 → 918/918; replay cold clones exact |
| `cdda1ae` | Idle keep-alive connections parked in one epoll set after a 100 ms linger (5 ms since `9b736e2`; `--linger-ms`, `--max-idle-conns`), generation-tagged slots, idle reaper, fd limit raised; mid-request reads switch to `--idle-secs` patience | 300+ idle connections: requests timed out (256 workers + 128 backlog). A first attempt that re-armed epoll per request serialized on the epoll mutex (0.73–0.81×, 43 % kernel lock time) — hence the linger | 1,000 idle connections: `/gvfs/config` answered in < 80 ms; throughput 0.98–1.03× (noise) |
| `70091b7` | While no worker is free, a worker parks its connection between requests after holding it `--hold-ms` (5); `Conns` counts workers waiting in `epoll_wait` | 4 workers, 8 busy keep-alive clients: 4 clients got no answer for the whole run (also in the shipped binary). New e2e test (2 workers, 6 clients, every body checked) fails without it: answers `[359744, 293721, 1, 1, 1, 1]` | Every client served (19–21K answers each, first within 11 ms), same total. A/B 256 workers / 64 conns: 1.00–1.02× on GET, up-to-date prefetch, POST, sizes; 32 workers / 64 conns: 0.99–1.01×, all 64 served |
| `9b736e2` | Listen backlog raised from std's 128 to `net.core.somaxconn`; keep-alive linger default 100 → 5 ms | 2,048 clients connecting at once: 14K listen overflows, SYN retries (p99 713 ms, max 2.1 s, server 4 % busy). Each sporadic request (one request, then idle) held its worker for the whole linger: 256 workers ≈ 2,500 such req/s before queueing; 8,000 req/s over 2,048 keep-alive clients: p50 145 ms, p99 2.5 s | No overflows; p50 16 µs, p99 23 µs. 512 clients at 20 ms think time: p99 8.9 ms → 18 µs (+0.01 core). Closed loop (back-to-back): 0.99–1.00×, same CPU and p99 |
| `cdda1ae` | `/gvfs/sizes` items built without `core::fmt` / UTF-8 validation | profile: 52 % `from_utf8` + 17 % fmt | 1.24–1.25× req/s, CPU/request 916 → 724 µs |
| `7e45558` | Response head and small body in one write (`Out::head`, `write_response` via `writev`) | two syscalls and two TCP segments per small request | GET blob 1.09×, GET commit 1.08×, up-to-date prefetch 1.15×, lower p99 |
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
```
Scratch data lives under `~/gvfs-bench` (SearchGold stores ~16 GB); `/tmp` is a tmpfs here.
The shell's injected `GIT_CONFIG_*` is stripped by `gvfslib.clean_env`.
