# Sol: testing record and cross-implementation benchmark protocol

Document date: 2026-10-25 UTC. Protocol identifier: `gvfs-comparison-sol-v1`.

This document records the tests and benchmarks actually executed during the
gvfs-daemon implementation session, with enough detail to reproduce equivalent
cases against other implementations. It is both an evidence inventory and a
proposed common comparison protocol, not a claim that other implementations
already implement this protocol.

**Status:** the final 29-test suite, native Git validations, full snapshot
comparison, fourteen-day replay, and tuned-maintenance replay passed. Failed
development attempts are identified separately. Writing this document did not
rerun the large benchmarks.

## 1. Comparison rules

For each implementation, preserve the same input pack, requested ref, history
trace, simulated event times, client policies, and correctness checks. Adapt
the implementation's CLI/API, not the workload or expected object coverage.

Use these result statuses:

| Status | Meaning |
| --- | --- |
| `PASS` | Executed and satisfied the stated checks. |
| `FAIL` | Executed and failed; retain the failure and partial results. |
| `FAIL_FIXED` | Earlier failure with a fix and a successful subsequent check. |
| `NOT_RUN` | Not measured or tested; do not infer a result. |
| `N/A` | A genuinely inapplicable internal mechanism, with an explanation. |

Separate externally observable requirements from implementation-specific tests.
For example, every implementation must reject absent OIDs correctly, but need
not use Sol's MPHF, sidecar layout, Rust types, or CRC-composition algorithm.
Full-versus-clustered layout comparisons and an old-behavior control are
capability-dependent experiments; the common workload must also run against
each implementation's normal production configuration.

The essential correctness gate is **native Git-readable data with complete
required object coverage**. Object counts, valid checksums, or a successful
HTTP response alone do not establish that.

In particular:

- A nonempty prefetch produces **one pack per response**, not one pack per
  selected slab. It does not promise one pack for the workstation's lifetime.
- An unchanged prefetch produces zero packs.
- Pack/idx checksums, delta dependencies, and canonical Git OIDs must remain
  valid after selection, deduplication, and compaction.
- No maintenance policy may remove objects needed by the client's fetched tip.
- Do not compare a main-only output against an all-refs input as if they
  represented the same object set.
- Keep server layout/transport improvements separate from client maintenance
  tuning. The tuned MIDX policy below is not stock Scalar's default.

## 2. Provenance, environment, and safety

### 2.1 Locations and revision

Current implementation root, abbreviated as `SOL` below:

```text
/home/muks/gvfs-daemon/alt_clients/sol
```

The work originally ran under `/home/muks/sol/gvfs-daemon`; some retained logs,
JSON fields, scripts, and Git alternates still contain that earlier absolute
path. For reproduction, create fresh generated upstream forks and output
directories instead of assuming old alternates are relocatable.

The checkout inspected for this document is:

```text
820249c82dfffd18687e695037592280187ca612
Initial commit, 2026-10-25T04:50:11Z
```

The measurements preceded that commit. Original per-run source/binary hashes
were not captured, and the snapshot and workload experiments were performed at
different stages of development. Do not retrospectively assign that commit or
the current release binary's hash to every historical timing. A standardized
rerun should freeze and record its source revision, dirty diff, binary hash,
dependency lockfile, and build flags before execution.

### 2.2 Recorded machine and build conditions

| Item | Session configuration |
| --- | --- |
| CPU | AMD Ryzen 9 9950X, 16 cores / 32 logical CPUs |
| Memory | Approximately 89 GiB RAM |
| OS | Linux 7.3.0 |
| Rust | rustc 1.97.1; Cargo 1.97.1 |
| Git and Scalar | 2.53.0 |
| Python, confirmed at documentation time | 3.14.4 |
| OpenSSL, confirmed at documentation time | 3.5.5 |
| Release profile | Thin LTO, one codegen unit, debug information level 1 |
| CPU-specific compilation | No `target-cpu=native` |
| CPU affinity | No CPU pinning |
| Storage | Local disk; generated arenas and caches under `target/` |
| HTTP benchmark transport | Loopback HTTP/1.1; separate synthetic HTTP/2/TLS tests |
| Repetitions | One successful full snapshot comparison and one successful full history replay; not repeated statistical trials |

There was no controlled cold-page-cache run, CPU governor/frequency study,
thermal study, peak-RSS series, CPU-time series, or block-I/O accounting.
Do not turn these results into hardware-independent performance ceilings.

For a new comparison, record those missing conditions where possible and run
at least several independent repetitions with randomized implementation order.
That is a recommendation for future comparisons, not something already done
in this session.

### 2.3 Data and scheduler safety

The original `/home/muks/SearchGold` repository was treated as read-only.
Only generated forks had their refs advanced. Its final checked state remained:

```text
HEAD:             4d12e2c7915577f43351ac82b9ca15bd7788be89
Loose objects:    0
Physical packs:   1
Packed objects:   12,083,668
```

The workload harness isolates `HOME`, `XDG_CONFIG_HOME`, `XDG_RUNTIME_DIR`,
`GIT_CONFIG_GLOBAL`, `GIT_CONFIG_NOSYSTEM`, and the session D-Bus address.
It removes Git repository/object-directory overrides and injected
`GIT_CONFIG_COUNT`, `GIT_CONFIG_KEY_*`, `GIT_CONFIG_VALUE_*`, and
`GIT_CONFIG_PARAMETERS` settings before spawning native Git.

Successful workload runs configure only generated repositories and invoke
maintenance manually. They do not call `scalar register`, `scalar run config`,
`scalar start`, or background-maintenance registration.

An earlier exploratory `scalar run config` probe unexpectedly created user
systemd maintenance timers despite an isolated global Git config. The exact
newly created units/links were removed, their timers stopped, and the service
instances checked: no scheduled maintenance job had started. Final checks
showed the hourly, daily, and weekly timers inactive. This is a reproducibility
hazard to avoid, not part of the benchmark protocol.

All benchmark listeners were stopped at completion. Do not run cleanup or
maintenance commands against the source repository or a user's real cache.

## 3. Common datasets and synthetic fixture

### 3.1 Real repository: frozen snapshot

| Field | Fixed value |
| --- | --- |
| Source | `/home/muks/SearchGold/.git` |
| Input pack | `objects/pack/pack-ec111cc94ed5a20453e303e45695428e4ac3a35b.pack` |
| Input pack bytes | 3,266,681,772 |
| Input pack version / objects | v2 / 12,083,668 |
| Input pack SHA-1 trailer | `ec111cc94ed5a20453e303e45695428e4ac3a35b` |
| Selected ref | `refs/heads/main` |
| Selected tip | `4d12e2c7915577f43351ac82b9ca15bd7788be89` |
| Objects in each measured snapshot output | 9,224,930 |

The input contains objects beyond the selected main history. Both measured
output layouts use the same selected scope. The input pack's size is therefore
not a fair same-object-set native-Git compression baseline.

This is a GitHFS repository, but its Git objects include ordinary blobs as well
as pointer text. Do not replace Git blobs with dereferenced file payloads, and
do not reject normal text because it is not an HFS pointer.

One real HFS-looking historical blob,
`7dfac37dd89f1ab8fc833b93e3cfa28f2fb2cee0`, contains a placeholder instead of a
valid BlobId. Sol preserves it as opaque Git data with a diagnostic by default;
an explicit strict mode rejects it. Preserve original OIDs and bytes in the
normal benchmark.

Native fsck reported the following pre-existing `gitmodulesParse` warnings,
because the original `.gitmodules` contents are HFS pointers:

```text
6079c2d73a8c0b48e2e476be1fba4881eb9ff421
d5804d9bb1cd0749db0b37727b7231e3a2259ab9
b7aad543dd1ce2c4aa30c8506882bfb7c1fb0a88
```

Retain these warnings and their successful exit status. Do not silently suppress
all fsck warnings or rewrite the original objects to make a test look cleaner.

### 3.2 Real repository: fourteen-day history

Use the retained trace rather than independently choosing a recent fortnight:

```text
SOL/target/workloads/run-14d/history.json
SHA-256: e7cb30583dc15220da0ab46056c0b7d5d05f3d2db19d6a034e4b4842c46d7160
```

| Field | Fixed value |
| --- | --- |
| Start, inclusive | 2026-10-05 00:00:00 UTC / `1791158400` |
| End, exclusive for new history | 2026-10-19 00:00:00 UTC / `1792368000` |
| Pre-window tip | `2ed14186d6cc83d8f612703c71c4275148e50f5f` |
| Final tip | `478aa4241df5250724c658da8d3d6dbd82935a42` |
| First-parent commits | 2,947 |
| Active 15-minute intervals | 1,064 of 1,344 |
| Commit timestamp backsteps in this trace | 0 |
| Git-declared incremental objects, final excluding base | 28,625 |
| Initial baseline objects | 9,182,331 |

Daily first-parent commit counts, October 5 through October 18:

```text
120, 141, 174, 220, 269, 258, 179, 230, 267, 140, 256, 322, 188, 183
```

Trace entries are `[oid, effective_unix_time, original_committer_time]`.
Committer time is a proxy for ref/push arrival time, not an observed arrival
trace. At each replay event, advance the private upstream to the latest
first-parent tip whose effective timestamp is due. Several commits may become
visible together at the next event; this is not a separate ingest operation for
every individual commit. Fetching that tip still includes its actual reachable
history, including merged history.

### 3.3 Synthetic Git fixture

The shared generator is `SOL/tests/common/mod.rs`. Port its data semantics, or
reuse the generated input pack for all implementations:

- 96 files initially containing actual `~~Git-hfs 1.0` pointer blobs.
- LF pointers initially; changed pointers use CRLF and 33-byte BlobStore IDs.
- Version `v` changes file `(v * 37) % 96`, creating scattered edits over time.
- Each version creates a `src` tree and a root tree with 32 aliases to an empty
  tree, an ordinary `.gitattributes` blob, a symlink blob, and a gitlink.
- Gitlinks deliberately refer outside the repository and must not be traversed
  as required local objects.
- Commits are parent-linked with fixed fixture author/committer timestamps.
- Native Git packs the fixture with `--delta-base-offset --window=64 --depth=50`.
- Individual tests use 2, 5, 20, 22, or 24 versions as indicated below.

Default small-test capacities are 4,096 objects, 16 MiB pack capacity, 1 MiB
object scratch, 131,072 graph-link capacity, 4 MiB delta cache, and a 64 KiB
cluster/read span setting. The large-source idx merge test raises objects to
8,192. These are test settings, not production defaults.

## 4. Automated validation actually executed

### 4.1 Build and static checks

These commands passed after the final code changes:

```sh
cargo fmt --all --check
cargo test --workspace --all-targets --locked --message-format=short
cargo clippy --workspace --all-targets --locked --message-format=short -- -D warnings
cargo build --release --locked --bin gvfs-daemon --example workload_server --message-format=short
```

The complete suite finished with **29 tests: 16 core, 8 workflow, 2 remote, and
3 library unit tests**. The binary, example, and platform crate had no additional
test cases. The same fmt/test/clippy commands are configured in
`.github/workflows/ci.yml`; no hosted CI run is being claimed here.

Main application code forbids unsafe Rust. Workspace clippy denies unwrap,
expect, panic, unchecked indexing/slicing, arithmetic side effects, todo,
unimplemented, and unreachable. Tests have explicit allowances; the small
platform crate isolates the allocation/mapping unsafe boundary.
For non-Rust implementations, record equivalent static checks rather than
pretending these specific Rust lints are universally applicable.

### 4.2 Core cases: `tests/core.rs`

All cases below passed. The IDs are proposed stable comparison IDs; the final
column gives the actual Sol test name for exact reproduction.

| ID | Input and acceptance condition | Sol test |
| --- | --- | --- |
| C01 | Parse 33-byte LF and 32-byte CRLF HFS IDs, zero size, and `InvalidBlobId`; ordinary text is not a pointer. Reject trailing size junk, u64 overflow, extra lines, wrong version, and malformed IDs. The instrumented parse allocates zero times. | `actual_hfs_format_is_strict_and_allocation_free` |
| C02 | A deterministic 200,000-byte base with insert/delete/replace edits at offsets 0, 65,536, 100,000, and 199,999. Real Git delta instructions reconstruct exact bytes with zero hot allocations. Invalid opcode/copy/varint programs fail. | `prefix_suffix_encoder_writes_real_git_delta_opcodes` |
| C03 | A 22-version fixture has identical OIDs/counts/sizes in full and clustered layouts; clustered is smaller. Decode/hash every tree, check structural adjacency, newest full cluster heads, direct full bases, at most 15 older members, and the 64 KiB span bound. Native Git verifies both complete packs and fscks their tips. | `full_and_clustered_layouts_have_identical_native_objects` |
| C04 | With clustered configuration, all entries at L0, 15m, 1h, 6h, and 12h remain undeltified. The test's name mentions both modes, but its explicit tier loop uses clustered configuration; do not report an independently executed two-mode matrix for this case. | `every_intraday_tier_remains_undeltified_in_both_modes` |
| C05 | 256 stored synthetic keys and 10,000 absent keys share the candidate fingerprint. No absent key may pass membership. A packed descriptor with a 1 TiB offset and 100,000-byte length uses the overflow representation. | `perfect_hash_never_treats_a_matching_fingerprint_as_membership` |
| C06 | A five-object pack places a REF_DELTA before its full blob base and an OFS_DELTA depending on the resolved REF_DELTA. All five objects reconstruct, native Git verifies the output, and the commit is connected. | `forward_ref_deltas_and_deferred_ofs_dependents_resolve` |
| C07 | Reject pack truncations at lengths 0, 1, 11, 12, 20, and one byte before EOF, an altered byte at offset 20, an object capacity of 2 for a larger pack, and an overflowing pack-entry header. Fail explicitly, not by panic or allocating fallback. | `invalid_input_and_capacity_fail_explicitly_without_panics` |
| C08 | Request tree versions 2, 7, and 2 from duplicate sources. Deduplicate objects/bases, preserve output offsets, rewrite OFS headers, and construct an allocation-free idx exactly equal to native `index-pack` output. Requested tree bytes match the source. | `subset_splicing_rewrites_distances_and_deduplicates_cluster_bases` |
| C09 | Constructing the upstream fetch future must not embed the packet scratch arena; its size is less than 65,536 bytes. This is Sol-specific stack-safety coverage. | `transport_future_does_not_embed_the_packet_arena` |
| C10 | In a canonical 200-entry tree, change OIDs at entries 1, 75, and 198. Reconstruction is exact, hot allocations are zero, and unchanged islands keep the delta below 200 bytes. | `scattered_tree_edits_copy_unchanged_middle_entries` |
| C11 | Preserve a placeholder-HFS blob as opaque native Git data, with one diagnostic and no HFS metadata. Explicit strict mode returns the malformed-HFS error. Normal ingest allocates zero times in its hot region. | `opaque_hfs_history_is_preserved_and_strict_policy_is_explicit` |
| C12 | Replacing short, long, or empty CRC prefixes agrees with CRC over actual bytes for payload lengths 0, 1, 127, 4,096, and 70,000, including prefix-width changes. Zero hot allocations. | `replacing_crc_prefix_matches_actual_bytes_even_when_header_width_changes` |
| C13 | Merge an entire 4,096-object sorted baseline index with a 16-object prefix/tail source. Generated offsets, CRCs, ordering, and bytes equal native Git's idx; planning/encoding allocate zero times. | `complete_source_index_merge_matches_native_git_without_allocating` |
| C14 | A legacy baseline with publication epoch zero is still included in cold prefetch at cursor zero. | `cold_prefetch_includes_legacy_zero_epoch_baselines` |
| C15 | Native idx offsets 12, 2^31-1, 2^31, and 2^40 round-trip. An invalid large-offset ordinal cannot read the checksum trailer as an offset. | `idx_large_offsets_round_trip_and_cannot_reference_the_trailer` |
| C16 | A complete 20-version clustered baseline rebuild succeeds with a one-entry HTTP-range budget, consumes no range-plan slots, allocates zero times during Merkle planning, and passes native Git validation after sealing. | `weekly_rebuild_does_not_consume_the_http_range_budget` |

Internal tests using synthetic OIDs, such as the fingerprint-collision test,
are not substituted for native Git hash correctness tests.

### 4.3 Workflow cases: `tests/workflow.rs`

| ID | Input and acceptance condition | Sol test |
| --- | --- | --- |
| W01 | Bootstrap at tip 5/time 100; ingest tip 11/time 1,000 and tip 17/time 2,000. Stale CAS fails. Daily rollup at 86,400 preserves object epochs and filters already covered objects while retaining dependencies. Weekly rollup at 604,800 emits a native-valid baseline and a baseline-excluding delta. Warm/cold/expired/unchanged cursor selection is correct. An unpublished orphan does not affect the manifest; a later ingest recovers. Rewind and no-op cases work. | `genesis_incremental_daily_weekly_and_cas_recovery` |
| W02 | Two refs can have the same pack ID with different time ranges. GC removes only the unreferenced range-specific time sidecar, retaining the live sidecar, shared pack artifacts, and an unrecognized filename. Missing required epoch data fails validation/loading instead of silently defaulting. | `publication_sidecars_are_required_and_gc_is_range_specific` |
| W03 | Bootstrap through version 5, recover live history to version 19, seal only through version 11, then install the new manifest. The remaining live suffix and old pinned snapshot both stay valid. Rebooting runtime state from the store reconstructs the current tip. | `restart_and_manifest_handoff_rebuild_only_the_unsealed_live_window` |
| W04 | Two concurrent conditional-genesis attempts yield one success. A manifest with one byte corrupted is an error, not a new empty store. | `conditional_genesis_and_corrupt_manifest_are_not_success_shaped_fallbacks` |
| W05 | HTTP/1.1 health/config/prefetch work over a populated three-stage store; cold GPRE has one pack, valid checksum, and supplied idx equal to native Git's. Twenty repeated commit-depth requests reuse arenas and produce valid object packs. Sizes report Git blob bytes; loose blob/tree/commit content hashes match. Current cursor returns the exact 8-byte zero-pack envelope; negative cursor and malformed JSON yield 400, absent OID yields 404. | `http_single_pack_prefetch_objects_sizes_loose_and_reused_arenas` |
| W06 | HTTP/2 prior-knowledge transport uses the same endpoint engine; its received pack is native-readable. | `http2_prior_knowledge_uses_the_same_endpoint_engine` |
| W07 | Accept escaped JSON keys, unknown nested fields, a valid surrogate pair, and sizes arrays with zero hot allocations. Reject whitespace-broken numbers, an unpaired surrogate, leading-zero depth, duplicate object-ID fields, invalid literals, and trailing garbage. | `json_protocol_arrays_escapes_unknown_fields_and_malformed_values` |
| W08 | A temporary OpenSSL-generated localhost server certificate is trusted by the test client, ALPN negotiates h2, and the TLS response contains one native-valid pack. This is local TLS integration, not production PKI/interoperability certification. | `tls_alpn_http2_serves_one_native_git_pack` |

Boot/reboot and orphan-publication tests are logical recovery checks. They are
not repeated power-loss tests, kernel-crash injection, or a distributed writer
stress test.

### 4.4 Remote/storage cases: `tests/remote.rs`

| ID | Input and acceptance condition | Sol test |
| --- | --- | --- |
| R01 | Against a local Azure-shaped HTTP fixture, publish/load an ETag manifest, allow identical immutable upload, reject stale CAS, and load metadata without downloading the pack. One exact range reconstructs a clustered historical tree with its correct OID. Reject a server returning 200 instead of 206, a wrong Content-Range start, or a truncated body. | `remote_conditional_manifest_indexes_and_one_cluster_range` |
| R02 | Upload a 67,112,960-byte payload (64 MiB + 4,096) through both Azure block upload and S3 multipart branches; each completes one multipart assembly and preserves every byte. | `azure_block_and_s3_multipart_uploads_preserve_all_bytes` |

These are local protocol-fixture tests. They do not use live Azure/S3 accounts,
real credentials, provider throttling, real cloud RTT, or a real ADO/GitHub
smart-HTTP origin.

### 4.5 Library unit cases

| ID | Input and acceptance condition | Source/test |
| --- | --- | --- |
| U01 | A stale cancellation notification from an old response is cleared when its reusable channel begins again; the next response sends and receives successfully. | `src/server/body.rs`: `canceled_response_does_not_poison_the_reused_channel` |
| U02 | A queued mapped-data frame keeps its generation permit after the response body is dropped; the permit is released only after the last frame is dropped. | `src/server/body.rs`: `queued_mapped_data_keeps_its_generation_after_the_body_is_dropped` |
| U03 | SigV4 authorization for a URI with escaped path/query, non-default port, and session token matches an independent Python hashlib/hmac/urllib implementation. Only fixture credentials are used. | `src/storage.rs`: `sigv4_matches_an_independent_standard_library_implementation` |

### 4.6 What the zero-allocation checks mean

`crates/gvfs-platform/src/lib.rs` forwards Rust global allocation calls and
counts `alloc`, `alloc_zeroed`, and `realloc` while a synchronous, thread-local
`HotRegion` is active. Startup allocates/prewarms arenas and codec state before
the measured region. A region must not cross an await or thread migration.

The tests and production checks instrument ingest, OID/JSON parsing, graph/tree
operations, splice planning, sealing loops, and synthesized-index work.
The real HTTP and replay metrics also reported zero accumulated hot allocations.

This does **not** mean the process, Tokio/Hyper/TLS, subprocesses, catalog
publication, OS mappings, or page faults are allocation-free. It is not a
foreign-allocator profiler, and it does not measure deallocation cost or RSS.
Other implementations should report their instrumentation boundary explicitly,
not compare a whole-process count to Sol's core-region count.

## 5. Measurement definitions

Use exact integer bytes as the primary data. MiB = 2^20 bytes; GiB = 2^30 bytes.
Record acquisition, construction, persistence, validation, transfer, installation,
and maintenance as separate phases rather than mixing different boundaries.

### 5.1 Wire format and byte accounting

The measured GVFS prefetch envelope is:

```text
8 bytes:  "GPRE \x01" followed by a little-endian u16 pack count
if count == 1:
  24 bytes: little-endian i64 timestamp, i64 pack length, i64 idx length
  pack bytes
  idx bytes, if idx length >= 0; -1 means omitted
```

A Git pack is a 12-byte PACK/v2/count header, object entries, and a 20-byte SHA-1
trailer. A supplied idx is native idx v2 with its own checksum and the correct
pack checksum.

For each response:

```text
body_bytes = 8                                      if no pack
body_bytes = 32 + pack_bytes + supplied_index_bytes if one pack
```

HTTP counters count application socket bytes sent/received, including HTTP
headers/framing. They exclude TCP/IP/Ethernet overhead, retransmission accounting,
and TLS overhead in this HTTP-only replay. The byte tables below are GPRE body
bytes unless explicitly labeled otherwise.

### 5.2 Client file and storage accounting

Count actual `*.pack` and paired `*.idx` files in each independent client's real
object database. Include the original baseline. Record final count and sampled
peak, not merely the number of server slabs or nonempty fetches.

`client_pack_bytes` is the sum of visible `.pack` file lengths. It excludes idx,
MIDX, refs, working trees, and filesystem metadata. Main maintenance controls
hardlink identical downloaded immutable data, so sums across controls are
logical sizes, not physical disk consumption.

`rewritten_bytes` in the retained maintenance CSV means the sum of sizes of
newly appearing final `.pack` paths in that maintenance invocation. It is not a
block-device write counter, write-amplification trace, or total temporary I/O.

For future comparisons, separately report full live server artifact bytes,
unreferenced retained artifacts, client idx/MIDX bytes, actual allocated disk
blocks, and peak RSS. These are not interchangeable.

## 6. B01: same-input full-versus-clustered snapshot benchmark

### 6.1 Procedure and timing boundary

Implementation: `src/benchmark.rs`.

Read/map the same input once before both timers; resolve the same selected tip.
Run `full`, then `clustered`, each with fresh construction arenas. Construction
time includes arena initialization, ingest/decode/order/compression, index and
adjacency construction, and sealing. It excludes input acquisition, artifact
publication, native validation, and the subsequent lookup microbenchmark.
The recorded decode subphase starts after arena/builder initialization.

Validate/publish each result and install its pack/idx into a bare native Git
repository. The snapshot runner invokes `git verify-pack -s` and
`git fsck --full --no-reflogs --no-progress`, and asserts one physical pack.
`verify-pack -s` alone is not full object verification; the independent full
fsck is material. Small fixture validation instead uses `verify-pack -v` and
strict full fsck.

Large input capacities:

```text
max objects       16,777,216
pack arena        65,536 MiB (64 GiB)
object scratch    128 MiB
max graph links   2,147,483,647
delta cache       512 MiB
```

These are upper bounds/preallocated capacities, not measured resident memory.

### 6.2 Lookup/tree sampling

After native validation has warmed data, perform 50,000 successful OID lookups,
choosing physical ordinals with seed `0x24102026` and Sol's deterministic
64-bit mix. Each step adds `0x9e3779b97f4a7c15` modulo 2^64 before mixing.
Up to 5,000 encountered tree samples are decoded and their canonical OIDs
recomputed; collect their contiguous base-to-member byte spans.

Lookup timing excludes retrieving the selected record before the timer.
Tree timing includes reconstruction and OID hashing. Both include clock-call
overhead. Sort samples and use:

```text
percentile_index = floor((sample_count - 1) * percentile / 100)
```

The same ordinal sequence does not select identical OIDs after a layout changes.
These are useful warm internal microbenchmarks, but not a rigorously matched-OID
cross-layout latency experiment. For a new standardized latency comparison,
freeze common OID query files independent of storage order, record their hashes,
and repeat the measurements. That stronger query-cohort experiment was not run.

### 6.3 Measured results

| Metric | Full entries | Clustered trees |
| --- | ---: | ---: |
| Output objects | 9,224,930 | 9,224,930 |
| Pack bytes | 21,386,711,153 | 2,394,341,455 |
| Construction seconds | 226.689708 | 166.808016 |
| Decode subphase seconds | 207.856932 | 156.108399 |
| Tree delta entries | 0 | 3,534,786 |
| Structural adjacency bytes | 1,208,761,046 | 1,208,761,046 |
| Lookup p50 / p99, ns | 220 / 670 | 210 / 620 |
| Tree decode + OID p99, ns | 88,961 | 76,961 |
| Historical tree span p99, bytes | 23,610 | 25,060 |
| Measured ingest hot allocations | 0 | 0 |
| Native validation seconds | 404.314159 | 151.339378 |
| Physical native-repository packs | 1 | 1 |

Clustered output uses 88.80% fewer pack bytes (8.93x smaller) and construction
took 26.42% less time in this single run. Do not assign statistical confidence
intervals to those one-run ratios.

For storage comparisons, include the other files rather than quoting pack size
as total server storage:

| Artifact bytes | Full | Clustered |
| --- | ---: | ---: |
| `.pack` | 21,386,711,153 | 2,394,341,455 |
| `.idx` | 304,050,864 | 261,310,488 |
| `.meta` | 664,195,012 | 664,195,012 |
| `.mphf` | 122,999,136 | 122,999,136 |
| `.links` | 1,208,761,046 | 1,208,761,046 |
| `.hfs` | 153,740,148 | 153,740,148 |
| Sum of these six immutable files | 23,840,457,359 | 4,805,347,285 |

The sums were calculated from retained file sizes while preparing this
document. They exclude the manifest, cache/spool files, duplicate verification
repositories, and filesystem overhead; no baseline `.times` file is needed.
This is a logical artifact-size calculation, not an RSS or physical-disk test.

The full output also exercises real pack offsets beyond 16 GiB and native idx
64-bit offsets.

## 7. B02: complete real-store HTTP smoke/transfer verification

Script retained in the session workspace:

```text
/home/muks/.copilot/session-state/51db17a1-4861-4b02-9901-c8eaeb51473e/files/verify_real_http.py
```

For each B01 store, start the release daemon with `--no-live`, one worker arena,
four snapshot slots, the large repository limits, and `127.0.0.1:0`.
Poll startup logs every 250 ms, then verify `/healthz` and advertised actual
assigned port. Filesystem caches are warm.

For each layout:

- GET loose blob `01aa5a4d70d5625e0ccb31d6f16595a3fe611398` and tree
  `06d7c654ea81d0ef707d518e1f74e836f27db25e`; decompress, hash canonical
  loose bytes, and match OIDs. Compare the blob bytes to original Git.
- POST the blob OID with `commitDepth=1` five times. Each response is a valid
  one-object pack; install with native `index-pack`, check one physical pack,
  and read the blob back through Git.
- GET cold prefetch. Fully consume the actual pack and idx over HTTP, checking
  both SHA-1 trailers against their stream and corresponding stored artifacts.
  Do not replace this with checking a Content-Length or reading files on disk.
- Check zero hot allocations in `/metrics`, then stop the daemon.

| Metric | Full | Clustered |
| --- | ---: | ---: |
| Startup/readiness seconds, 250 ms granularity | 14.505 | 4.754 |
| Prefetch pack bytes | 21,386,711,153 | 2,394,341,455 |
| Prefetch idx bytes | 304,050,864 | 261,310,488 |
| Total GPRE body bytes | 21,690,762,049 | 2,655,651,975 |
| Complete stream/hash seconds | 8.076 | 0.986 |
| GPRE pack count | 1 | 1 |
| Single-pointer POST pack bytes | 137 | 137 |
| POST pack/native readback | PASS | PASS |
| Loose OIDs checked | 2 | 2 |
| Measured hot allocations | 0 | 0 |

The stream timer starts after receiving the 32-byte GPRE envelope and includes
Python client hashing. It excludes startup and is not the same timing boundary
as the later replay's per-request timer. This is not an isolated server
throughput measurement. HTTP/2 and TLS correctness were checked in W06/W08,
not benchmarked with these multi-GiB stores.

## 8. B03: accelerated fourteen-day workload replay

### 8.1 Server adapter and initial state

`examples/workload_server.rs` embeds the real pipeline, live ingestion, and
HTTP server. `tools/workloads.py` supplies the deterministic clock and clients.
No fourteen-day wall-clock wait occurs.

Bootstrap a clustered baseline at the trace's pre-window tip:

```text
Pack SHA-1:       5d91a129a5b501b8d53e54ac2ef19231c30c36df
Objects:          9,182,331
Pack bytes:       2,380,735,219
Idx bytes:        260,103,916
GPRE body bytes:  2,640,839,167
Cursor:           1791158400
```

The initial response was actually downloaded and checked. It seeded all warm
controls but is excluded from their recurring traffic totals.

Replay server/pipeline limits:

| Setting | Value |
| --- | --- |
| Persistent object / pack limits | 16,777,216 / 64 GiB |
| Persistent object scratch / delta cache | 128 MiB / 128 MiB |
| Persistent graph-link capacity | 2,147,483,647 |
| Live object / pack limits | 65,536 / 256 MiB |
| Live object scratch / delta cache | 32 MiB / 32 MiB |
| Live graph-link capacity | 8,388,608 |
| HTTP worker arenas | 1 per server |
| Tokio worker threads | 4 |
| Read/coalescing range limit | 4 MiB |
| Coalescing gap | 4,096 bytes |
| Maximum planned ranges | 262,144 |
| Pinned snapshot slots | 8 |
| Weekly-delta retention | 14 days |
| Automatic real-time sync | Disabled; explicit replay ticks advance state |

Both measured HTTP variants share the same actual published catalogs:

| Variant | Selection / idx behavior |
| --- | --- |
| `before` | Whole intersecting rollups; omit synthesized idx, but direct stored-pack responses can still include their stored idx. Equivalent controls: `--coarse-prefetch --no-prefetch-index`. |
| `after` | Preserve/filter publication epochs; emit native synthesized idx; retain necessary delta bases. Normal optimized default. |

The optimized server alone drives live ingestion. The compatibility server
receives that same catalog rather than fetching a different history. This is
an A/B of selection/response behavior, not two unrelated data pipelines.

### 8.2 Exact event schedule

Let `S=1791158400` and `E=1792368000`, with all times in UTC seconds.

| Event | Times / count |
| --- | --- |
| Seal/auto pipeline | `S+900, S+1800, ..., E`; 1,344 opportunities |
| Shared-cache agent | `S+300+900*k < E`; 1,344 requests/clones |
| Continuous warm polling | `S+600+900*k < E`; 1,344 requests |
| Isolated cold agent | `S+420+3600*k < E`; 336 requests |
| Developer | 08:00, 11:00, 14:00, 17:00 on the ten weekdays; 40 requests |
| Daily maintenance | Each next midnight through `E`; 14 runs per maintained cache |
| Final developer catch-up | `E+28800`, using the frozen final history; one extra request and no extra maintenance |

At a shared event time, apply due source-tip changes and any scheduled seal,
refresh the live catalog, perform client requests, then run maintenance.
The implementation iterates cold agents first, then shared agents, polling,
and developer when applicable. Warm A/B requests execute `before` then `after`.
There is no simultaneous client-fleet load.

The final trace contains **4,369 control ticks**, of which **1,691 observed
nonempty live state**. Auto rollups exercise 15m -> 1h -> 6h -> 12h -> 24h,
and the weekly policy has its boundary at Sunday 00:00 UTC. The two weekly
ticks are `1791676800` and `1792281600`.

These phase offsets matter: aligning every fetch just after sealing would
avoid much of the live-window/handoff behavior and produce different traffic.

### 8.3 Client construction

Cold agents download from cursor zero, checksum the entire response, install
its supplied pack/idx into a fresh native Git repository, update the fetched
tip, assert exactly one physical pack, then discard the generated cache.
This is a metadata-cache clone, not stock `git clone` against a smart-HTTP
origin or a full working-tree checkout.

For each warm behavior, initialize separate `none`, `midx`, and `scalar`
repositories from the same seed. Download each optimized response once and
install its immutable pack/idx into each control using hardlinks with realistic
unique `prefetch-<timestamp>-<sequence>` names. The object databases and MIDX
files are independent. Do not triple-count network bytes merely because the
maintenance experiment has three controls.

Each shared-agent event also creates a fresh native
`git clone --shared --bare --single-branch --branch main` from its Scalar cache.
Check the tip through Git and assert zero private packs in the agent.
All **1,344 actual shared-agent clones** passed that check.

Clients have no promisor configuration masking missing data. Updating the ref
must fail if the fetched commit is absent. A successful commit lookup alone is
still followed by the broader final object-coverage checks below.

Local Git settings:

```ini
gc.auto = 0
gc.autoPackLimit = 0
maintenance.auto = false
maintenance.strategy = incremental
maintenance.gc.enabled = false
core.multiPackIndex = true
pack.threads = 1
```

Advance a client's cursor only from a successful nonempty response. An empty
response retains its prior cursor. The `before` and `after` clients maintain
their own protocol cursors.

### 8.4 Baseline client-maintenance policies

All commands run only against their generated client object database.

| Policy | Daily operation |
| --- | --- |
| `none` | No pack/index maintenance |
| `midx` | `git multi-pack-index write --no-progress` |
| `scalar` | Actual `scalar run pack-files <generated-repository>` |

The Scalar command invokes Git's incremental-repack task, including MIDX
write, expire, and an automatically chosen repack batch. It is not equivalent
to MIDX write alone and does not mean no pack rewriting. Do not replace it
with a full `gc`, or silently tune its batch, and still label it this policy.

### 8.5 Per-response checks and final native checks

Every fetched response is fully consumed, including no-change responses.
Check HTTP status, GPRE magic/count, lengths, PACK v2/count, pack SHA-1,
idx v2/count/checksum and referenced pack checksum when present, and no trailing
unexpected body bytes. Record raw pack/index/body bytes, object entries,
returned cursor, request/response socket byte counts, and duration.

The replay request duration starts before making the HTTP request and includes
client stream hashing and writing received files when a destination is used.
It excludes subsequent native Git installation, ref updates, clone creation,
and scheduled maintenance. The reported traffic reductions are not reductions
in this duration.

One complete synthesized cold response (the second cold clone) was also
native-verified. The main replay did not run full native pack decoding for
every one of the 336 multi-GiB clones.

After the trace:

- Native `verify-pack` checks all distinct retained incremental pack/index
  inodes and repacked files. Hardlinked identical inputs are verified once,
  not incorrectly counted as separate verification coverage.
- `rev-list --objects --no-object-names <client_tip> ^<base>` defines the
  incremental required set from original Git.
- `cat-file --batch-check` compares every required OID's type/size in every
  maintenance variant against the original repository; no missing object is
  accepted. Combined with native pack verification, this is stronger than
  counting pack headers.
- Existing MIDX files are verified with `git multi-pack-index verify`; each
  final repository has matching pack and idx counts.
- `/metrics` must report zero server errors and zero measured hot allocations.

Shared/polling clients finish at their last scheduled pre-end request:

```text
tip:              45da79c79734d113b16e6dd34a353d902229a7b9
incremental OIDs:  28,613
```

The final developer catch-up reaches the final trace tip and all 28,625
incremental OIDs. Do not incorrectly fail the earlier finishing clients for
not fetching later history; compare each to its actual fetched tip.

### 8.6 Traffic results

Warm totals exclude the initial 2,640,839,167-byte seed response. All figures
include supplied indexes and GPRE envelopes.

| Workload | Requests | Nonempty / empty | Optimized body bytes | Optimized body size |
| --- | ---: | ---: | ---: | ---: |
| Cold hourly agents | 336 | 336 / 0 | 890,857,314,733 | 829.676 GiB |
| Shared-cache agents | 1,344 | 1,158 / 186 | 117,250,751 | 111.819 MiB |
| Continuous polling | 1,344 | 1,203 / 141 | 144,208,643 | 137.528 MiB |
| Weekday developers + catch-up | 41 | 41 / 0 | 54,294,631 | 51.779 MiB |

Exact optimized transport components:

| Workload | Pack bytes | Idx bytes | HTTP received | HTTP sent |
| --- | ---: | ---: | ---: | ---: |
| Cold agents | 803,331,012,109 | 87,526,291,872 | 890,857,372,861 | 33,936 |
| Shared agents | 114,888,183 | 2,324,024 | 117,476,223 | 147,840 |
| Polling | 141,560,099 | 2,608,920 | 144,434,429 | 147,840 |
| Developer | 53,448,091 | 845,228 | 54,301,580 | 4,510 |

Warm A/B response comparison:

| Workload | Before bytes | After bytes | Reduction |
| --- | ---: | ---: | ---: |
| Shared agents | 255,167,591 | 117,250,751 | 54.05% |
| Polling | 281,084,743 | 144,208,643 | 48.70% |
| Developer | 96,420,977 | 54,294,631 | 43.69% |

Both warm variants produced the same numbers of nonempty responses in this
trace. These savings therefore reduce bytes, not the number of fetch-created
packs. The index bandwidth increase is already included.

Transmitted object-entry counts were 121,981 -> 38,666 for shared agents,
130,433 -> 47,118 for polling, and 71,121 -> 28,617 for developers.
These are sums of response pack entries, not a unique-OID coverage proof.
Delta bases and live/sealed handoffs can cause repeated delivery; required
coverage is assessed independently against original Git.

### 8.7 Final packs, sampled peaks, and pack-only sizes

Counts include the initial baseline. Each count has a paired idx.

| Workload | Policy | Final packs | Sampled peak | Final `.pack` bytes |
| --- | --- | ---: | ---: | ---: |
| Shared agents | none | 1,159 | 1,159 | 2,495,623,402 |
| Shared agents | midx | 1,159 | 1,159 | 2,495,623,402 |
| Shared agents | scalar | 906 | 925 | 2,481,044,658 |
| Polling | none | 1,204 | 1,204 | 2,522,295,318 |
| Polling | midx | 1,204 | 1,204 | 2,522,295,318 |
| Polling | scalar | 832 | 866 | 2,493,961,404 |
| Developer | none | 42 | 42 | 2,434,183,310 |
| Developer | midx | 42 | 42 | 2,434,183,310 |
| Developer | scalar | 9 | 19 | 2,395,498,767 |

Cold caches have one pack per clone. Shared agents themselves have zero
private packs; the shared cache has the counts in this table.

Fourteen daily maintenance invocations per case:

| Workload | Policy | Newly appearing pack bytes, summed | Maintenance wall seconds, summed |
| --- | --- | ---: | ---: |
| Shared agents | midx | 0 | 20.394 |
| Shared agents | scalar | 2,040,164 | 58.240 |
| Polling | midx | 0 | 20.395 |
| Polling | scalar | 2,106,829 | 58.729 |
| Developer | midx | 0 | 16.691 |
| Developer | scalar | 18,033,421 | 54.330 |

MIDX-only does not delete packs. Daily Scalar improved physical counts but
did not keep continuously fetching caches small in this measured trace.
No end-user lookup-latency benchmark was performed on these accumulated caches;
MIDX correctness is not a measured claim about `status` or `checkout` latency.

### 8.8 Pipeline and execution results

The main replay recorded `elapsed_seconds = 1814.2902722800209`, about
30.24 minutes. Its timer includes startup, seed transfer, pipeline work, HTTP
traffic, client work, scheduled maintenance, and the earlier final pack/object
checks. It is sampled before the summary loop's last MIDX checks and shutdown.
It excludes initial baseline construction, prior failed attempts, separate
weekly validation, the later tuned policy, and the rest of the session.

The two completed weekly pipeline/refresh ticks took 143.640650 and
142.766959 seconds. Their returned catalogs had empty intraday pyramids.
These are whole tick durations, not isolated compression times.

Final observed server counters:

```text
before: requests=2731, errors=0, remote_range_reads=0, hot_allocations=0
after:  requests=3068, errors=0, remote_range_reads=0, hot_allocations=0
manifest generation: 1129
```

Request counters can include metrics/probe requests; use the CSV for workload
request counts. Zero remote-range reads confirms this was a local-storage
replay, not an S3/Azure performance run.

## 9. B04: tuned small-pack maintenance on the same downloads

### 9.1 Additional policy

After B03 completed, `tools/workloads.py --replay-maintenance <run>` replayed
the retained optimized downloads into new `capped-32m` repositories:

```sh
git multi-pack-index write --no-progress
git multi-pack-index expire --no-progress
git multi-pack-index repack --no-progress --batch-size=33554432
git multi-pack-index expire --no-progress
```

Run these at the same fourteen daily boundaries, with the same original
baseline and request sequence. Do not download the large baseline again.
New incremental arrivals are copied with fresh mtimes in replay order: MIDX
tie-breaking uses pack mtimes, so blindly hardlinking all old arrivals after
creating a new repack could bias the experiment.

Expiry is justified by MIDX object coverage, not by time-range filenames.
The policy intentionally rewrites small packs while avoiding a baseline-sized
batch. It was tested only in generated caches and is **not** an installed
workstation hook, an automatic daemon feature, or stock Scalar configuration.

Measure peak counts after installs and intermediate maintenance steps, final
pack/index counts, newly appearing pack sizes, maintenance time, and baseline
file identity/length. Native-verify new packs, verify MIDX, and compare all
incremental object types/sizes to the untuned cache.

### 9.2 Results

| Workload | Final packs | Sampled peak | Final `.pack` bytes | Original baseline retained |
| --- | ---: | ---: | ---: | --- |
| Shared agents | 2 | 95 | 2,383,931,776 | yes |
| Polling | 2 | 96 | 2,383,891,466 | yes |
| Developer | 3 | 7 | 2,385,715,032 | yes |

| Workload | Daily runs | Newly appearing pack bytes, summed | Maintenance wall seconds, summed |
| --- | ---: | ---: | ---: |
| Shared agents | 14 | 25,554,283 | 76.670 |
| Polling | 14 | 25,729,113 | 76.339 |
| Developer | 14 | 18,543,635 | 45.106 |

All three original 2,380,735,219-byte baseline files retained their original
inode and length; no baseline-sized repack appeared. The final developer third
pack is its Monday catch-up after the last scheduled maintenance. Peak counts
include pending daily packs and sampled repack-before-expiry transitions; they
are not continuous filesystem-event traces.

All tuned caches passed native pack/MIDX and object-coverage checks.
There was no `git gc`, baseline re-download, or timestamp-only unlink policy.
These are fourteen-day observations, **not an everlasting two/three-pack bound**.

## 10. Additional probes, failures, fixes, and reruns

These were part of the session and should not disappear from an implementation
comparison merely because only successful results are summarized above.

| Case | What actually happened / validation |
| --- | --- |
| Expanded-history/adjacency capacity | Real SearchGold construction exposed very large expanded tree/structural data and exhausted an earlier adjacency budget. Mmap-backed construction scratch and compact depth-1 structural lists replaced giant adjacency vectors. The completed B01 runs are the successful evidence; earlier partial directories with only CSV headers were not counted. |
| Malformed historical HFS text | Real input initially exposed the placeholder BlobId. Opaque-byte preservation plus explicit strict mode was implemented and covered by C11 and both completed real layouts. |
| Upstream async stack | An inline packet buffer caused stack failure during development. Moving scratch out of the async future is covered by C09 and subsequent successful real ingestion. |
| Reusable response cancellation | A stale notification could poison the next reuse. U01 and repeated HTTP object requests cover the correction. |
| Mapped response lifetime | A queued frame could outlive the body that held the catalog pin. U02 checks pin ownership at the frame boundary. |
| Scalar tiny-pack probe | Three tiny native packs became four after one `scalar run pack-files`, then two after a second invocation: expiry of prior repacks can occur on a later run. Native Git accepted the realistic prefetch filenames. This was a behavior probe, not a throughput result. |
| Manual Scalar with a real large seed | A four-pack cache became five after one invocation, taking about 2.447 seconds. The trace showed MIDX write/expire/repack and a 67,356-byte auto batch. No scheduler config was created in this isolated manual probe. |
| B03 preliminary smoke | A 12-tick partial replay installed one cold cache and exercised shared/polling L0/M15 requests. It is labeled incomplete in `target/workloads/smoke-1/summary.json`; it did not replace the full fourteen-day run or independently cover every rollup boundary. |
| Initial fourteen-day attempt | The first Sunday weekly rebuild failed with `capacity exhausted: arena elements`. Merkle-order selection unnecessarily tried to create an HTTP range plan with bounded range slots. Failed data is retained separately; none of its partial totals are combined with B03. |
| Weekly range-plan fix | Hydrated full rebuilds no longer create an unrelated HTTP range plan. Visited-OID checks avoid redundant lookups. C16 passed with a one-range budget; a real-size weekly retry and the restarted complete B03 trace passed. |
| Generated idx / epoch filtering | New CRC-composition, sorted-merge, offset-bound, zero-epoch, required-sidecar, and epoch-preservation cases passed before trusting real replay results (C08, C12-C15, W01-W02, W05). |
| Tuned policy smoke | A real-seed four-pack cache became two while retaining its 2,380,735,219-byte baseline; the probe took about 5.601 seconds. A separate short labeled fixture exercised all three maintenance-replay paths before B04; temporary completed probe directories were removed. |
| Scheduler-registration hazard | The exploratory Scalar config call and its complete rollback are described in Section 2.3. Final workload commands avoided that path. |

### 10.1 Separate real-size weekly validation

An isolated weekly retry used the last durable pre-failure manifest and private
upstream, then completed auto pipeline work at `1791676800`:

```text
generation=483
tip=2961025eaa92e100526219635e8f2241dcbf7b57
new_objects=30
slabs_written=7
changed=true
```

The resulting full weekly baseline was independently installed into native Git:

| Metric | Result |
| --- | --- |
| Tip | `2961025eaa92e100526219635e8f2241dcbf7b57` |
| Objects | 9,194,275 |
| Pack bytes | 2,388,692,320 |
| Pack SHA-1 | `de552f3b4cf3a538cab2ce214da9b84ee4502750` |
| Native checks | `git verify-pack`, ref update, `git fsck --strict --full --no-reflogs --no-progress` |
| Outcome | PASS, with only the recorded existing gitmodules warnings |
| Combined native-validation seconds | 155.3769378879806 |

This validation was additional to the final replay and is not included in its
30.24-minute figure.

## 11. Reproduction recipes for Sol

Run in the implementation root. Use new output names; the harness deliberately
refuses to overwrite an existing run or tuned-policy repository. Keep source
refs fixed for B01. Use the environment-isolation rules from Section 2.3 for
native Git setup commands as well as the workload harness.

### 11.1 Automated suite and release build

```sh
cd /home/muks/gvfs-daemon/alt_clients/sol
cargo fmt --all --check
cargo test --workspace --all-targets --locked --message-format=short
cargo clippy --workspace --all-targets --locked --message-format=short -- -D warnings
cargo build --release --locked --bin gvfs-daemon --example workload_server --message-format=short
```

Dependencies used by the tests include native Git, Python 3, and OpenSSL.
No cloud account is required. Individual tests can be selected by the exact
names in Section 4.

### 11.2 Snapshot comparison

```sh
target/release/gvfs-daemon bench \
  --storage target/comparison-sol-snapshot \
  --cache-dir target/comparison-sol-snapshot-input \
  --upstream /home/muks/SearchGold \
  --input-pack /home/muks/SearchGold/.git/objects/pack/pack-ec111cc94ed5a20453e303e45695428e4ac3a35b.pack \
  --max-objects 16777216 --max-links 2147483647 \
  --pack-mib 65536 --object-mib 128 --delta-cache-mib 512
```

Before execution, verify the selected upstream main tip equals the B01 tip.
This command reads input once, produces both layouts, checks native Git
readability, and writes a unique run directory.

For B02, the retained `verify_real_http.py` contains the full procedure and
fixed test OIDs. Its old absolute `root` and `run` paths must be adjusted to
the current root and new snapshot run. It writes its result file, so do not
point a new measurement at the canonical historical run unless replacing that
evidence is intentional.

### 11.3 Prepare a new pre-window seed

The following paths are examples of a new, nonexisting output root. Do not
update `/home/muks/SearchGold` itself:

```sh
mkdir target/comparison-sol-history
git clone --quiet --shared --bare --single-branch --branch main \
  /home/muks/SearchGold target/comparison-sol-history/upstream.git
git --git-dir="$PWD/target/comparison-sol-history/upstream.git" update-ref \
  refs/heads/main 2ed14186d6cc83d8f612703c71c4275148e50f5f

target/release/gvfs-daemon pipeline \
  --storage target/comparison-sol-history/seed-store \
  --cache-dir target/comparison-sol-history/seed-cache \
  --upstream "$PWD/target/comparison-sol-history/upstream.git" \
  --input-pack /home/muks/SearchGold/.git/objects/pack/pack-ec111cc94ed5a20453e303e45695428e4ac3a35b.pack \
  --tier 15m --now 1791158400 --tree-layout clustered \
  --max-objects 16777216 --max-links 2147483647 \
  --pack-mib 65536 --object-mib 128 --delta-cache-mib 512
```

On an empty store, this bootstraps a full baseline even though the requested
entry-point tier is 15m. Check its tip/object coverage before using it as seed.
Allow enough disk space for the expanded full-layout experiment, artifacts,
temporary arenas, and verification clients; capacity settings are large.

### 11.4 Smoke, complete replay, and tuned replay

```sh
python3 tools/workloads.py \
  --history target/workloads/run-14d/history.json \
  --baseline target/comparison-sol-history/seed-store \
  --upstream target/comparison-sol-history/upstream.git \
  --output target/comparison-sol-history/smoke \
  --limit-ticks 12

python3 tools/workloads.py \
  --history target/workloads/run-14d/history.json \
  --baseline target/comparison-sol-history/seed-store \
  --upstream target/comparison-sol-history/upstream.git \
  --output target/comparison-sol-history/replay

python3 tools/workloads.py \
  --replay-maintenance target/comparison-sol-history/replay
```

The main harness creates its own further upstream fork and writable store;
the seed remains reusable. A limited run must remain labeled incomplete and
must not be used as the full benchmark.

The original fourteen-day cold-agent traffic alone was approximately
0.83 TiB over loopback. It was actually streamed, not multiplied from one
response. The tuned replay adds no server network traffic.

### 11.5 Adapting another implementation

Provide equivalent adapter operations:

```text
bootstrap(input_pack, fixed_tip, layout, capacity_settings) -> immutable store
set_private_upstream_tip(oid)
advance(simulated_epoch, should_seal) -> fully installed catalog
serve_catalog() -> GVFS base URL
stop() -> listeners/tasks terminated
```

An adapter may expose line-based control like Sol:

```text
tick <unix_seconds> <tip_oid> <0-or-1-seal>
quit
```

Do not require Sol's private manifest or sidecars. What must match is the event
ordering, visible history, pack contract, native client validation, and metrics
boundaries. The HTTP consumer, native Git client controls, and canonical trace
can be shared. If an implementation omits idx, measure native index construction
as a separate client cost rather than hiding it or excluding it from comparison.
Sol's existing optimized installation path requires a supplied idx, so such an
adapter also needs an explicit client-indexing path.

If normal upstream synchronization cannot be manually clocked, add a minimal
clock/control adapter or declare that the exact accelerated protocol is
unsupported. Do not silently substitute sleeps, a different push trace, or a
different rollup schedule and call the numbers equivalent.

## 12. Standard output records for other implementations

The following schema is a proposed normalization of the session's records.
Fields absent from Sol's historical output remain `NOT_RUN`/null; do not
backfill invented measurements.

### 12.1 Run metadata

```text
protocol_id, implementation, source_revision, dirty_diff_hash, binary_sha256
dependency_lock_sha256, compiler, compiler_flags, build_profile
machine_cpu, cpu_count, memory_limit, kernel, filesystem, tool_versions
cpu_affinity, governor, page_cache_policy, repetitions, execution_order
source_pack_id, source_pack_bytes, selected_tip, history_sha256
configured_capacities, worker_counts, transport, storage_backend
start_time, end_time, status, known_warnings, failure_log
```

### 12.2 Test records

```text
case_id, implementation, status, fixture_id, assertions_checked
native_validation_commands, exit_status, warning_allowlist, evidence_path
```

### 12.3 Snapshot and query records

Sol's actual `results.csv` columns are:

```text
layout,objects,pack_bytes,graph_bytes,build_seconds,decode_seconds,
tree_deltas,lookup_p50_ns,lookup_p99_ns,tree_decode_p99_ns,
tree_range_p99_bytes,hot_allocations,native_seconds
```

For new comparisons, add all sidecar bytes, total live artifact bytes, separately
timed publication, exact query-cohort hash/count, allocation boundary, RSS, and
CPU/I/O fields if measured.

### 12.4 Request records

Sol's actual `requests.csv` columns are:

```text
time,scenario,variant,packs,body_bytes,pack_bytes,index_bytes,objects,
timestamp,pack_hash,http_received,http_sent,seconds
```

Add implementation/run IDs, input cursor, fetched tip, explicit validation
status, and separately timed install/index work for a cross-implementation
harness. Preserve no-change rows instead of dropping them from averages.

### 12.5 Maintenance and final-client records

Sol's main maintenance columns are:

```text
time,scenario,policy,before,after,rewritten_bytes,seconds
```

The tuned CSV additionally records a sampled intermediate `peak`.
Final records contain scenario/policy, final/peak packs, paired indexes,
pack-only bytes, and baseline-preservation/coverage information.
Use policy names that encode cadence and batch size; never merge tuned and
default Scalar into a single result column.

## 13. Retained evidence

All paths below are relative to current `SOL` unless absolute.

| Evidence | Location |
| --- | --- |
| Final source/test inventory | `tests/core.rs`, `tests/workflow.rs`, `tests/remote.rs`, `src/server/body.rs`, `src/storage.rs` |
| Common fixture and native helpers | `tests/common/mod.rs` |
| Snapshot benchmark implementation | `src/benchmark.rs` |
| Workload client/maintenance harness | `tools/workloads.py` |
| Deterministic server adapter | `examples/workload_server.rs` |
| Allocation instrumentation | `crates/gvfs-platform/src/lib.rs` |
| Snapshot results and stores | `target/bench-searchgold/run-1792885933-2090136/` |
| Snapshot native stores | The run's `full/verify.git` and `clustered/verify.git` |
| Snapshot HTTP results | The run's `http-smoke.json`, `http-full.log`, `http-clustered.log` |
| Canonical initial history seed | `target/workloads/store/` |
| Completed fourteen-day run | `target/workloads/run-14d/` |
| Per-request and maintenance data | That run's `requests.csv`, `maintenance.csv`, `pipeline.jsonl` |
| Main summaries/validation | That run's `summary.json`, `metrics.json`, `native-validation.json` |
| Tuned policy evidence | That run's `capped-maintenance.csv`, `capped-summary.json` |
| Actual generated caches/catalogs | That run's `clients/`, `store/`, `snapshots/` |
| Completed main console log | `target/workloads/run-14d.console.log` |
| Initial failed full replay | `target/workloads/run-14d-initial-failed/` and its separate console log |
| Partial smoke | `target/workloads/smoke-1/` |
| Successful isolated weekly store | `target/workloads/weekly-probe/` |
| Strict native weekly validation | `target/workloads/weekly-native/result.json` and `native.log` |
| Earlier session checkpoint | `/home/muks/.copilot/session-state/51db17a1-4861-4b02-9901-c8eaeb51473e/checkpoints/001-building-real-history-workload.md` |

The checkpoint was written before final replay completion and contains
superseded in-progress status. Final JSON/CSV records and this completed
inventory take precedence for results.

SHA-256 identities computed from retained small records for this document:

```text
48ccec09f03427b0997d8aa4b5ea66ffad442031251e8280ed876ef47bac6f95  snapshot/results.csv
674613d54188ea10c4409746d325f13daada845f031077f4b8f50693c761cf00  snapshot/http-smoke.json
e7cb30583dc15220da0ab46056c0b7d5d05f3d2db19d6a034e4b4842c46d7160  replay/history.json
b72e1a145187752890e91d009106a8bcabc3f1cf7dfe9611a7aaf966a659d399  replay/summary.json
66a6dd20f51dffd077c8c0dd9d474510371240b13e2f08155520739775b92330  replay/capped-summary.json
36a569d2e2a8bc693d7ea281638b37feb16f9fc2926f67a442d4693b5838a000  replay/requests.csv
4142d4543d8425f24bc2a488aa726f3864ec7b447242e36b884d22a933631a87  replay/maintenance.csv
c33255c289a2748be63fecc904f32a21b2c332615ce241ab3544369c62c63bbc  replay/capped-maintenance.csv
fdeda477123f444cb371dc1d8f4cfc3a19f071f0d9988c99dff7aa4913fd26e5  replay/native-validation.json
68090fb43aaf9984606363b19e978678d113acacec8e6b4383274fdfd69b5a2a  replay/metrics.json
```

Here `snapshot` denotes `target/bench-searchgold/run-1792885933-2090136`, and
`replay` denotes `target/workloads/run-14d`.

## 14. Explicitly not established by this session

Mark these `NOT_RUN` or unsupported in any comparison that relies only on this
evidence:

- Production cloud storage/provider compatibility, sustained WAN throughput,
  cloud range-read latency/percentiles, or authenticated ADO/GitHub end-to-end
  smart-HTTP origin testing.
- External HFS/BlobStore payload transfer, virtual filesystem behavior, a full
  working-tree checkout/build, or Windows/macOS workstation behavior.
- Simultaneous fleet saturation, many-client fairness, p99 service latency
  under load, maximum QPS, or long-duration wall-clock soak.
- Measured `git status`, `checkout`, `log`, or object lookup latency for the
  accumulated client pack-count matrix.
- Peak RSS, total process/foreign allocations, allocation-byte volumes,
  page-fault rates, CPU time, perf profiles, syscall counts, actual block-device
  writes, or production file-descriptor exhaustion.
- Formal fault-injection at every fsync/publication step, physical power-loss
  durability, cross-process/cloud concurrent-writer stress, or a proof of safe
  concurrent cloud orphan deletion.
- Fuzzing, sanitizer campaigns, model checking, an independent security audit,
  a benchmark against every Git pack edge case, or guarantees for SHA-256 Git
  repositories.
- A full native fsck of every cold clone or every final client independently;
  the exact native-verification coverage is described in Sections 6-10.
- A separately isolated timing of generated-index CPU savings versus native
  client `index-pack`, or isolated contributions from each optimization.
- Automatically installed workstation maintenance/tombstoning, a two-year
  replay, or a perpetual upper bound on client pack counts.
- Confidence intervals, repeated cold/warm cache trials, or a portable
  matched-OID query benchmark.

The final comparison should therefore present correctness, body bytes, client
pack accumulation, maintenance work, construction time, and scoped hot
allocations as separate dimensions. Report missing dimensions honestly rather
than treating small packs, zero core allocations, or a successful local replay
as a proxy for all of them.
