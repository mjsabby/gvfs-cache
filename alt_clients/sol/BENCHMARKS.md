# SearchGold: full versus clustered trees

Measured on an AMD Ryzen 9 9950X (32 logical CPUs), 89 GiB RAM, Linux 7.3.0,
Rust 1.97.1. Portable release build, thin LTO, one codegen unit; no CPU pinning
or `target-cpu=native`. These are single-run measurements, not statistical
performance ceilings.

Both variants read the **same original pack once** and select the same
**9,224,930 objects reachable from `refs/heads/main`** at
`4d12e2c7915577f43351ac82b9ca15bd7788be89`. The original pack contains
12,083,668 objects across all refs; that is not the output scope.

| Measurement | Full entries | Clustered trees |
| --- | ---: | ---: |
| Pack bytes | 21,386,711,153 | 2,394,341,455 |
| Pack GiB | 19.918 | 2.230 |
| Construction seconds | 226.690 | 166.808 |
| Tree deltas | 0 | 3,534,786 |
| Encoded adjacency bytes | 1,208,761,046 | 1,208,761,046 |
| OID lookup p50 / p99, ns | 220 / 670 | 210 / 620 |
| Tree decode + OID verification p99, us | 88.961 | 76.961 |
| Historical tree range p99, bytes | 23,610 | 25,060 |
| Measured hot-region allocations | **0** | **0** |
| Native pack/index + fsck validation seconds | 404.314 | 151.339 |
| Physical packs in verification repository | **1** | **1** |

Clustered trees use **88.80% fewer pack bytes (8.93x smaller)** and take
**26.42% less construction time**. Commits/blobs remain full entries; clusters
have one full tree and at most 15 direct-to-base deltas. The full variant
exercises real offsets beyond 16 GiB as well as standard idx 64-bit offsets.
Adjacency compression is independent of the experimental pack layout.

Construction includes arena initialization, decoding, ordering and all index/
adjacency construction. Input acquisition and validation are outside that timer.
Lookup samples include timer overhead; tree samples include content hashing.
Range sizes are stored one-read spans, not cloud RTT measurements.

## Actual daemon/HTTP verification

Each generated store was loaded by the final release binary with `serve
--no-live`, one worker arena, the same large-repository capacities and warm
filesystem caches. Both complete prefetch responses were streamed through HTTP,
not just inspected on disk.

| Check | Full entries | Clustered trees |
| --- | ---: | ---: |
| Startup/readiness seconds, 250 ms polling | 14.505 | 4.754 |
| Complete pack + idx transfer and client SHA-1 checking, seconds | 8.076 | 0.986 |
| GPRE pack count | **1** | **1** |
| Single-object POST pack count | **1** | **1** |
| Native Git verifies received POST pack | yes | yes |
| Loose blob/tree content hashes match original OIDs | yes | yes |
| `/metrics` hot allocation count | **0** | **0** |

These loopback transfer timings include Python client hashing and are not an
isolated HTTP-server throughput benchmark.

Native `fsck` exits successfully with three `gitmodulesParse` warnings.
Inspection confirms those original `.gitmodules` objects contain HFS pointers;
their bytes were not rewritten. One historical HFS-looking blob contains a
non-hash placeholder; it is diagnosed and treated as opaque Git data. The
original SearchGold repository remains unchanged with its original single pack.

## Reproduce / inspect

The command and capacity explanation are in [README.md](README.md#same-input-benchmarks).
Raw data and independently servable stores are retained under:

```
target/bench-searchgold/run-1792885933-2090136/
  results.csv
  http-smoke.json
  full/
  clustered/
```

Select `--tree-layout full|clustered` for normal pipeline jobs, or run `bench`
to generate and validate both on the same input. HTTP/1.1, HTTP/2, TLS/ALPN,
cloud protocol fixtures, CAS/recovery and allocation invariants are also covered
by the 29-test integration/unit suite.

## Accelerated fourteen-day workload replay

The completed replay covers **2026-10-05 00:00 through 2026-10-19 00:00 UTC**:
2,947 real first-parent commits, 1,064 active quarter-hour intervals, and 28,625
Git-declared incremental objects. Committer timestamps approximate ref-update
times; this is not a captured push-arrival trace. The pre-window tip is
`2ed14186d6cc83d8f612703c71c4275148e50f5f`, with a 9,182,331-object baseline;
the final tip is `478aa4241df5250724c658da8d3d6dbd82935a42`.
Git/Scalar 2.53.0 ran on the machine described above.

This used the actual pipeline, L0, pyramid rollups, HTTP responses, native Git
object databases and maintenance commands, not projected pack sizes. The
fourteen-day timeline was accelerated; the main run took **1,814.290 seconds
(30.24 minutes)** including its native checks.

### Traffic and final physical pack counts

| Workload | Requests | GPRE response bytes | No maintenance | MIDX only | Daily Scalar | Tuned MIDX |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Isolated cold agent hourly, 24x7 | 336 | 890,857,314,733 (829.676 GiB) | 1 per clone | n/a | n/a | n/a |
| New shared-cache agent every 15m | 1,344 | 117,250,751 (111.819 MiB) | 1,159 | 1,159 | 906 | **2** |
| Existing cache polling every 15m, 24x7 | 1,344 | 144,208,643 (137.528 MiB) | 1,204 | 1,204 | 832 | **2** |
| Weekday developer, four fetches/day | 41 | 54,294,631 (51.779 MiB) | 42 | 42 | 9 | **3** |

Bytes include the pack, supplied idx and GPRE envelope, but not HTTP/TCP/TLS
overhead. Exact HTTP request/response byte counts are also retained in the CSV
and summary JSON. Every nonempty response contained **one pack**, regardless of
how many slabs were selected; unchanged responses contained zero packs.

Warm caches were preseeded: their table traffic excludes the separately measured
**2,640,839,167-byte (2.459 GiB)** initial baseline response. Pack counts include
that baseline. Each warm fetch was downloaded once and installed into three
independent maintenance-control object databases; their network traffic is not
multiplied by three. The tuned control subsequently replayed the exact retained
downloads without another server transfer.

The shared-agent case also created 1,344 fresh native bare clones using the
Scalar cache through Git alternates: **zero private packs per agent**. Cold
clones installed one independent pack each and discarded their isolated cache
after validation. No working trees or external HFS payloads were hydrated.

Seals occur on quarter-hours; shared agents request at +5m, polling at +10m and
hourly cold agents at +7m, exercising unsealed L0 as well as stored slabs.
Developers request at 08:00/11:00/14:00/17:00 UTC on weekdays. Their 41 requests
include 40 scheduled fetches plus a Monday 08:00 catch-up on the frozen final
catalog, exposing the last weekend backlog. Shared/polling caches stop at their
last scheduled request before the window ends; their final coverage is checked
against that request's actual tip.

### What Scalar does, and the additional tuning

MIDX-only runs `git multi-pack-index write` daily; it does not remove packs.
Daily Scalar runs the actual `scalar run pack-files` command. **Neither policy
guarantees a small physical pack count under continuous 15-minute fetching.**
MIDX correctness was verified; this workload did not benchmark lookup latency.

The additional, explicitly non-default tuned policy runs daily:

```sh
git multi-pack-index write
git multi-pack-index expire
git multi-pack-index repack --batch-size=33554432
git multi-pack-index expire
```

It consolidates small packs and expires only packs that Git's MIDX proves
redundant, rather than inferring object coverage from timestamps. All three
original **2,380,735,219-byte baselines remained unchanged**, including their
original file identities. This policy was tested only in generated benchmark
caches; no workstation hook or scheduled maintenance was installed.

| Workload | Scalar peak packs | Tuned peak packs | Tuned new pack bytes across 14 maintenance runs | Tuned maintenance seconds |
| --- | ---: | ---: | ---: | ---: |
| Shared agents | 925 | 95 | 25,554,283 | 76.670 |
| Polling | 866 | 96 | 25,729,113 | 76.339 |
| Developer | 19 | 7 | 18,543,635 | 45.106 |

Peaks include pending daily inputs and repack/expiry transitions, not just the
post-maintenance state. The developer's final third pack is Monday catch-up
after the last scheduled maintenance. Small-pack rewriting is intentional;
there is no `git gc`, baseline re-download or baseline rewrite. These measured
fourteen-day results are **not a perpetual pack-count bound**.

### Server optimizations and measured A/B results

The compatibility control sends complete intersecting rollups and omits
synthesized indexes. The optimized default preserves object publication epochs
through rollups, filters already covered objects, and emits a native synthesized
idx. Required delta bases are always included.

| Workload | Previous response MiB | Optimized response MiB | Reduction |
| --- | ---: | ---: | ---: |
| Shared agents | 243.347 | 111.819 | **54.05%** |
| Polling | 268.063 | 137.528 | **48.70%** |
| Developer | 91.954 | 51.779 | **43.69%** |

These savings already include the generated-index bandwidth cost. CRC
composition handles rewritten OFS headers without payload reads; cold requests
reuse the complete baseline's sorted idx and linearly merge the small tail.
Reusable ingestion scratch and immutable slab mappings avoid repeated
large-arena initialization.

The initial run exposed a real weekly rebuild failure: Merkle-order selection
unnecessarily built an HTTP range plan and exhausted its bounded range arena.
Rebuilds now consume hydrated source entries without that unrelated plan;
visited-OID checks also avoid repeated MPHF lookups and read only the full
20-byte membership key. A one-range-budget regression test and a real-size
weekly retry validate the fix. Both weekly cycles completed in the final replay
(143.641 and 142.767 seconds for their complete pipeline ticks).

### Validation and retained evidence

Every response pack/index checksum was checked. Native Git verified a complete
synthesized cold pack and all distinct retained incremental packs, verified each
MIDX, and matched incremental object types/sizes against original Git for all
maintenance variants. The developer caches cover all 28,625 incremental objects;
shared/polling caches cover the 28,613 objects at their last scheduled tip.
The tuned caches passed the same coverage checks.

A separately retained rebuilt weekly baseline contains **9,194,275 objects**;
native `verify-pack` and full strict `fsck` passed. Its three existing
`gitmodulesParse` warnings are the unchanged HFS-pointer cases described above.
Final server counters report **zero errors and zero measured hot allocations**.
The original SearchGold HEAD, zero loose objects and single original pack remain
unchanged. Benchmark listeners shut down and maintenance timers are inactive.

This is serialized loopback metadata traffic, not a concurrent-fleet throughput
test, cloud RTT measurement, wall-clock soak or external BlobStore benchmark.
No automatic client tombstoning or cloud orphan deletion was introduced.

```text
target/workloads/run-14d/
  history.json, seed.json, setup.json
  requests.csv, maintenance.csv, pipeline.jsonl
  summary.json, metrics.json, native-validation.json
  capped-maintenance.csv, capped-summary.json
  store/, clients/, snapshots/
target/workloads/weekly-native/result.json
```

The initial failed attempt is retained separately as
`target/workloads/run-14d-initial-failed`; it is not included in the result
tables. Reproduction commands and safety boundaries are in
[README.md](README.md#accelerated-real-history-workload-replay).
