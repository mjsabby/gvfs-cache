# GVFS implementation comparison: Astra tests and benchmarks

Session record and reproducible, implementation-neutral comparison protocol.

- Session: `df64b0ab-3ec1-4466-9f23-7cd38015f48d`.
- Work performed: 2026-10-24 through 2026-10-25, UTC.
- Report prepared: 2026-10-25.
- Current implementation directory: `/home/muks/gvfs-daemon/astra`.
- Original execution directory: `/home/muks/astra/gvfs-daemon`; it was subsequently moved into the comparison repository.
- Current packaging commit: `820249c82dfffd18687e695037592280187ca612`.
- Current `astra` Git tree: `9ba4a1ef1b286ba1e2721ff7d45aa57b884b1b12`.

Historical timings were collected at successive development stages, not all at the final source revision. This report distinguishes those stages. It was assembled from the session transcript, retained result files, and test/harness source; the expensive benchmarks were not rerun merely to write the report.

## 1. What was actually established

| Area | Evidence collected | Important boundary |
|---|---|---|
| Rust correctness/build gates | Final formatting, compilation, release build, strict Clippy, and 39 named tests passed | This is not exhaustive verification or a fuzzing campaign |
| Full SearchGold ingestion | Three representations of the same 9,224,930 HEAD-reachable objects | Not all 12,083,668 objects in the original source pack |
| Git compatibility | Native `index-pack`, index comparison, small-repository `fsck`, HTTP clones, shared-cache clones | Microsoft's native `gvfs-helper` and C# VFSForGit were inspected, not run as end-to-end clients |
| HTTP performance | Warm GET latency and one checksum-verified full prefetch per representation | One sequential loopback client; no saturation or cloud-latency benchmark |
| Workload traffic | Eight-day continuous-write and diurnal-write replays, before and after optimization | Logical time; no eight-day wall-clock run or TB-scale network transfer |
| Pack retention | Legacy filename model; optimized replay used real installed/compacted cache files | Two **managed** packs after successful sync, not a bound on unrelated packs or bytes |
| Memory discipline | Zero counted allocations in declared engine hot regions during the measured successful runs | Startup, publication, index construction, transport, and native Git are outside that guarantee |
| Cloud-store behavior | Local HTTP mocks for conditional publication, range reads, Azure blocks, S3 multipart | Not live Azure/S3 certification; the mock does not independently verify AWS signatures |
| Recovery | Edge restart, pipeline hand-off, client transaction fault injection, malformed/truncated input tests | Not actual machine power-loss, filesystem fault injection, or exhaustive crash-point testing |

**Do not turn this into a single "fastest implementation" score.** Correctness, pack size, latency, network bytes, retained files, local write amplification, and deployment constraints are distinct dimensions.

## 2. Rules for comparing another implementation fairly

Use the test and benchmark IDs below even if the other implementation has different binaries or APIs.

1. Freeze the same source commit and object scope. Check the object-set digest before comparing pack sizes or traffic.
2. Preserve original Git object bytes and OIDs, including ordinary historical blobs. Dropping objects, rewriting pointers, or omitting history is not compression.
3. Label the client path: stock Git smart HTTP, native GVFS, a modeled cache, or an actual implementation-specific managed cache.
4. Label the wire policy: pack only, or pack plus `.idx`. Include the index in transfer accounting when sent.
5. Distinguish server artifact count, packs returned, unique response identities, retained client packs, and total client files.
6. Distinguish measured network traffic from serialized bytes and calculated cold-clone totals.
7. Use the same workload event order, fixed logical clock, payload generator, and schedules.
8. Run timed comparisons sequentially on otherwise quiet, identically provisioned machines. Record affinity, cache state, CPU policy, versions, and compiler flags.
9. Count errors, capacity exhaustion, truncated responses, and failed verification. Do not exclude failures from latency/throughput summaries.
10. Report unsupported tests as `UNSUPPORTED` and unexecuted tests as `NOT_RUN`, not `PASS`.
11. Do not claim process-wide zero allocation from a counter that excludes transport/publication. Publish the measured region definition.
12. For new timing comparisons, collect multiple independent runs and raw samples. The historical Astra measurements below are single-run observations, not confidence intervals.

### Minimal adapter surface

An adapter for another implementation should provide these operations:

```text
build(configuration)
bootstrap(source_repository, wanted_commit, publication_time, pack_policy)
ingest(have_commit, wanted_commit, publication_time)
compact(tier, logical_time)                 # must work without an upstream
reopen_server_from_published_storage()
prefetch(cursor, optional_validator, wire_index_policy)
get_object(oid)
post_objects(oids, commit_depth)
post_sizes(oids)
sync_managed_cache(response)
reopen_managed_cache_after_interruption()
inspect_pack_files(), inspect_cursor(), inspect_tip()
```

The workload driver, logical fixture, request schedules, Git verification, and metric definitions should stay fixed. Adapt invocation and storage access, not the workload to favor an implementation.

## 3. Environment and source identity

### 3.1 Machine and toolchain

The following machine details were confirmed when preparing this report. The Rust and Git versions agree with the versions recorded during the session.

| Field | Value |
|---|---|
| CPU | AMD Ryzen 9 9950X, 16 physical cores / 32 logical CPUs, one socket |
| Architecture | x86-64 |
| Kernel | Linux 7.3.0 |
| RAM visible to OS | Approximately 89 GiB |
| Swap configured | 8 GiB |
| Benchmark storage | ext4 on `/dev/nvme0n1p2`, not `/tmp` |
| Base page size | 4,096 bytes |
| Rust | `rustc 1.97.1 (8bab26f4f 2026-07-14)` |
| Cargo | `cargo 1.97.1 (c980f4866 2026-06-30)` |
| Git | `git version 2.53.0` |
| Release profile | Thin LTO, one codegen unit, debug info level 1 |
| Optimized build flag | `RUSTFLAGS='-C target-cpu=native'` |
| Compression | `flate2` with `zlib-rs`; new full entries compressed at level 1 |
| Resource collector | GNU `/usr/bin/time -v` |

The machine was shared. CPU governor, thermal conditions, and page-cache state were not controlled for the historical runs. No privileged global page-cache dropping was performed. Some benchmark processes overlapped with other session work; the two optimized workload replays overlapped with each other. Do not use their elapsed times as isolated throughput rankings.

The HTTP benchmark requested affinity to the first four allowed **logical** CPUs immediately after server launch. It did not record those CPU IDs or pin the Python client. Four workers and requested four-CPU affinity do not establish four physical cores or a concurrency test.

### 3.2 Real repository

| Item | Recorded value |
|---|---|
| Repository | `/home/muks/SearchGold` |
| Selected commit | `4d12e2c7915577f43351ac82b9ca15bd7788be89` |
| Original pack | `pack-ec111cc94ed5a20453e303e45695428e4ac3a35b.pack` |
| Original pack bytes | `3266681772` |
| Original `.idx` bytes | `381637088` |
| Original `.rev` bytes | `48334724` |
| Original packed-object count | `12083668` |
| Original loose-object count | `0` |
| Output scope | All objects reached by the implementation's traversal from the selected HEAD; submodule gitlinks are not hydrated |
| Output object count, every representation | `9224930` |
| SHA-256 of sorted raw 20-byte output OIDs | `8ee9eddde1aa708d68c3dd9abab96d1b2ce2c7e46b8a4f25d5b91489318e17e3` |
| Original seed publication timestamp | `1792880752` |

The digest is SHA-256 over the concatenated, lexicographically sorted **binary** OIDs from the Git index, with no hexadecimal encoding or separators. It is not a hash of the entire `.idx`.

The source's 3.267 GB pack contains objects outside the selected output scope. It is **not** an apples-to-apples compression baseline for the 2.673 GB Astra delta pack.

The source repository was only read. Its original single pack was checked again after the benchmark work. Synthetic writes happened in private repositories, not SearchGold.

### 3.3 Pointer format

Use the actual variable-width format, not the PDF's early hypothetical fixed-width format:

```text
~~Git-hfs 1.0
blob:<66 hexadecimal characters: a 33-byte VSO BlobStore ID>
size:<decimal size>
```

Tests cover LF and CRLF and the explicit `InvalidBlobId` sentinel. Stored bytes and Git OIDs remain unchanged. `/gvfs/sizes` returns the size of the Git object, not the dereferenced HFS payload.

### 3.4 Representations compared

| ID | Astra option | Policy |
|---|---|---|
| P-FULL | `--pack-mode full` | Standalone zlib entries |
| P-RETAIN | `--pack-mode delta` | Preserve upstream tree deltas whose bases are retained; commits/blobs remain standalone |
| P-CLUSTER | `--pack-mode clustered` | Build directory-history tree clusters at genesis/daily/weekly packing; intraday objects remain standalone |

Clusters use at most 16 members, a 1 MiB compressed-span budget, and depth-one references to a standalone base. Sealed deltas use REF_DELTA so deduplicating/subsetting does not invalidate OFS distances. Current-tip trees are bases when a long-term slab is built.

P-RETAIN is a compression reference: its rollups do not discover new deltas among previously undeltified objects. P-CLUSTER is the final CLI default, but the session did **not** establish that it is universally optimal. Test other implementations' policies explicitly.

## 4. Correctness and safety test suite

Final executed gate:

```sh
cd /home/muks/gvfs-daemon/astra
cargo fmt --all
cargo test --workspace --lib --tests --examples --message-format=short --quiet
cargo clippy --workspace --all-targets --message-format=short --quiet -- -D warnings
RUSTFLAGS='-C target-cpu=native' cargo build --release \
  --bin gvfs-daemon --example workload_bench --message-format=short --quiet
```

Observed: 1 allocator test + 33 library tests + 4 integration tests + 1 replay-schedule test = **39 passed**. CLI targets also compiled. Earlier `cargo check`, library-only runs, targeted regressions, full workspace runs, and `--all-targets` runs were used during development. Zero-test/doc-test targets are not counted as test cases.

The application forbids unsafe Rust and denies `unwrap_used`, `expect_used`, `panic`, `indexing_slicing`, `arithmetic_side_effects`, `todo`, `unimplemented`, and `unreachable` in production. The allocator instrumentation crate contains the isolated `GlobalAlloc` unsafe implementation. Test code has deliberate assertion/fixture exceptions.

For routine CI, `cargo fmt --all -- --check` is the non-mutating equivalent formatting gate. Match semantics rather than Rust-specific lint names in other languages.

### 4.1 Codec, indexing, and allocation tests

All cases in this table passed. Source locations are relative to the current Astra directory.

| ID | Test names / source | Standardized assertions |
|---|---|---|
| U-ALLOC | `counts_hot_path_allocations_only_after_seal`; `crates/alloc-guard/src/lib.rs:279` | Unsealed allocations do not count as guarded failures; sealed cold allocations affect the total but not hot counter; a guarded allocation increments hot count/bytes |
| U-BUF | `decimal_roundtrip`, `bounded_writers_never_grow`; `src/buf.rs:265` | Decimal roundtrips including 0, boundaries, 0..99,999 and `u64::MAX`; reject overflow/negative input; exhausted fixed writers fail without capacity growth |
| U-OID | `hex_roundtrip_and_validation`, `branch_free_decoder_matches_the_table_for_every_byte`; `src/oid.rs:204` | Valid upper/lowercase OIDs, invalid lengths/characters, all 256 byte values at representative positions, scalar/SWAR encoder agreement |
| U-PACK | `entry_header_roundtrip`, `ofs_distance_roundtrip`, `pack_header_and_loose_header`; `src/pack.rs:267` | Type/size varints, partial header, sizes through `1<<40`, OFS distances through `(1<<50)+7`, invalid pack version, canonical loose header |
| U-IDX | `write_then_read`; `src/idx.rs:266` | 300 entries, fanout/search/CRC/offset equality, checksum, missing lookup, duplicate rejection, a 4 GiB offset |
| U-DELTA | `copy_and_insert`; `src/delta.rs:129` | Real Git copy/insert instructions, exact output, wrong base size, insufficient output, reserved zero opcode |
| U-TREEDELTA | `scattered_edits_insert_delete_and_large_copies`; `src/tree_delta.rs:191` | 4,000-entry tree; change every 123rd entry, delete entry 137, append `z-new`; encoded delta under 3,000 bytes, exact restoration, empty base, malformed tree rejection |
| U-MPHF | `exact_membership_and_large_offsets`; `src/mphf.rs:488` | 10,000 present OIDs and 10,000 absent OIDs; exact membership; duplicate rejection; long entries and offsets beyond 16 GiB through overflow records |
| U-HFS | `actual_format_and_strict_trailing_bytes`; `src/hfs.rs:68` | Real pointer parsing, CRLF sentinel, size extraction, trailing-junk rejection, decimal overflow rejection |
| U-OBJECT | `commit_parsing`, `tree_iteration_and_order`; `src/object.rs:180` | Commit tree/parents/time, signed header continuations, no parents from message body, borrowed tree parsing, Git tree ordering |
| U-JSON | `objects_request_compact_and_pretty`, `objects_request_rejects_bad_input`, `ado_refs_exact_name_match`, `sizes_roundtrip`, `string_escaping`; `src/json.rs:515` | Compact/pretty JSON, keys in either order, nested unknown fields, exact ref-name match, OID arrays, size results, escaping, malformed/truncated/deep/negative/trailing input, bounded sink |
| U-ZLIB | `adler_matches_reference_and_combines`, `inflate_deflate_roundtrip_exact`, `loose_splice_produces_valid_loose_object`; `src/zlib.rs:390` | Adler combination, multipart compression, exact consumption, wrong expected sizes, truncation, empty payload, loose-object splice at levels 0/1/6/9 and payloads up to 70,000 bytes |
| U-GPRE | `headers_roundtrip`; `src/gpre.rs:101` | GPRE magic/version, little-endian pack count, timestamp/length/index sentinel, multi-loose framing |
| U-SCHEDULE | `eight_day_schedules_include_the_weekend_and_next_monday`; `examples/workload_bench.rs:895` | Exact eight-day request counts and 576 weekday-only five-minute changes |

The MPHF/overflow test is synthetic. The full SearchGold P-FULL artifact additionally exercises real output beyond 16 GiB. The unit test does not establish a nanosecond lookup latency.

### 4.2 Small Git integration fixture

Reference: `tests/end_to_end.rs:27-179`.

- Create a private Git repository on branch `main`, set fixture identity, and disable automatic GC.
- Create 400 files `file-0000` through `file-0399` plus `ordinary-history`, whose bytes contain an embedded NUL.
- Initial HFS IDs are `SHA1(u32_le(file) || u32_le(version))`, padded to 33 bytes, encoded as 66 hex characters. The pointer declares `size:1234`.
- Make 34 commits using versions 1 through 34; each changes file `version % 400`.
- Run `git repack -a -d --window=50 --depth=30` to exercise upstream deltas.
- Test provisioning: 32,768 objects, 1 MiB maximum object, 4 MiB delta cache, 8 MiB live slab.
- HTTP fixture: localhost ephemeral port, two workers, one-second live/manifest polling, strict allocation guard.
- The publication constant is `1699920000`. Despite an old "Monday" source comment, this is Tuesday, 2023-11-14 UTC. Do not use that comment to define the workload calendar.
- Fixture commit wall-clock metadata is not frozen by this helper. For byte-identical cross-implementation testing, generate the fixture once and distribute its objects, or explicitly freeze Git author/committer timestamps.

### 4.3 Pipeline, protocol, and native-Git integration

These are four executable tests, containing the portable subcases below.

| ID | Source test | Assertions executed |
|---|---|---|
| I-PIPE | `unified_pipeline_protocol_recovery_and_allocation_contract`, `tests/end_to_end.rs:320` | Genesis in all three modes; identical OID sets; native Git pack/index validation; standalone commits/blobs; clustered base depth/span and HEAD-root base; zero guarded ingest/splice allocations |
| I-DEDUPE | Within I-PIPE | Selecting the same slab twice emits each OID once and reproduces the original pack bytes; exact missing OID does not select a fingerprint false positive |
| I-NOCHANGE | Within I-PIPE | Unchanged pipeline is a no-op; caught-up prefetch returns an 8-byte zero-pack GPRE response, not a new empty Git pack |
| I-ROLLUP | Within I-PIPE | New intraday entries are undeltified; daily/weekly compaction succeeds with no upstream configured; weekly output contains a baseline and net-new delta; both verify with native Git |
| I-WARM | Within I-PIPE | Warm prefetch skips baseline; response from inside a weekly interval contains only the missing tail, not an incorrectly advertised full-week replacement |
| I-PUBLISH | Within I-PIPE | Stale manifest ETag fails; unreferenced truncated artifact does not alter visible root; corrupted manifest checksum is rejected |
| I-LIVE | Within I-PIPE | Reconstruct an unsealed tip before listen; cold response includes live data; live-only response does not advance the durable cursor; restart reconstructs lost in-memory state |
| I-HANDOFF | Within I-PIPE | Pipeline publishes behind the already-live edge; remaining tip stays readable after manifest hand-off |
| I-HTTP | Within I-PIPE | Config version-object shape; exact loose-object bytes; POST commit closure contains expected blob; direct tree POST remains a point lookup; `/sizes` returns Git size; absent OID returns 404; metrics remain at zero hot allocations |
| I-HEADERS | Within I-PIPE | Start/End and baseline/incremental/live markers on nonempty prefetch; no coverage on empty/object response; Start can equal End for L0-only data |
| I-CLONE | Within I-PIPE | Stock Git smart-HTTP `clone --no-checkout`; correct HEAD, `git fsck --full --no-dangling`, exactly one `.pack` |
| I-REWIND | Within I-PIPE | Force-rewind the selected branch; tip moves back without deleting retained history; a zero-new-object publication is handled |
| I-TRANSPORT | `remote_git_transport_reuses_a_real_negotiation_ref`, `tests/end_to_end.rs:708` | `file://` exercises native transport; unchanged fetch does not grow decoded source entries; next fetch adds fewer than 20 entries; persisted `refs/gvfs/upstream` is correct |
| I-HAVE | `newly_published_have_need_not_be_in_the_edges_decoder_cache`, `tests/end_to_end.rs:746` | A newly published have commit exists in Git but not the edge decoder; next fetch still excludes it and adds fewer than 20 entries, rather than refetching all history |
| I-SYNC | `conditional_http_and_real_client_compaction_keep_two_managed_packs`, `tests/end_to_end.rs:781` | The conditional/managed-cache cases in the next paragraph |

I-SYNC includes:

- Default prefetch omits `.idx`; `--prefetch-index` supplies it for a whole prebuilt slab.
- Matching and weak/list-form ETags yield 304; 304 body is empty and has no Content-Length, Transfer-Encoding, or coverage assertion.
- Validator is scoped to the requested cursor; an old URI's tag does not incorrectly validate the next URI.
- A cursor ahead of the server is rejected even with `If-None-Match: *`.
- Real HTTP client sync installs baseline, reaches two managed packs, and suppresses unchanged live downloads.
- Existing MIDX remains valid through replacement.
- Two live commits are subsequently force-pushed away before pipeline publication; local compaction preserves them anyway.
- Baseline bytes are unchanged; every original/downloaded commit remains readable.
- `git clone --shared --no-checkout <cache> <workspace>` uses zero workspace packs and the correct tip.
- Closing/reopening cache and invoking the actual `sync` CLI preserves cursor/tip and the two-pack state.

### 4.4 Managed-cache unit/integration tests

Reference: `src/client/tests.rs`; real pack files and native Git are used, not just mocked filename counters.

| ID | Test | Required behavior |
|---|---|---|
| C-PIN | `pinned_baseline_and_two_pack_bound_with_unmanaged_data_and_midx` | Baseline byte-identical; old managed delta removed; unmanaged pack retained; all contents readable; MIDX verifies; all-already-covered download causes zero replacement writes; reopen works |
| C-NOOP | `unchanged_response_and_invalid_download_never_rewrite_or_prune` | 304 causes zero download/body writes and byte-identical state; truncated download cannot advance cursor or remove existing files |
| C-BASE | `local_splice_keeps_ref_bases_even_when_the_anchor_also_has_them` | A standalone anchor object that is also a REF_DELTA base is retained where needed to keep the delta pack self-contained; later merges still decode correctly |
| C-CRASH | `interrupted_install_is_recovered_before_retiring_the_predecessor` | Inject failure after journal, after pack installation, after state save, and before retirement; old pack survives until recovery; reopen completes replacement; two packs and all objects remain |
| C-READERS | `long_lived_git_readers_survive_managed_pack_replacement` | A persistent `git cat-file --batch` process, with and without MIDX, can read old contents after managed replacement |
| C-STATE | `corrupt_state_and_foreign_server_are_not_silently_reset` | Wrong server URL or damaged state produces an error, not a success-shaped reset; existing pack survives |
| C-IMIDX | `existing_incremental_midx_is_rebuilt_before_retirement` | Existing incremental MIDX is rebuilt/verified before removing its predecessor; two managed packs remain |
| C-OWNERSHIP | `untracked_reserved_names_are_not_adopted_or_deleted` | File using a future managed filename but not owned by state is not adopted or removed; cursor and old managed pack remain unchanged |

Recovery testing injects application errors at explicit stages and reopens the cache. It is not a proof of crash consistency under arbitrary storage failure. Concurrent **readers** were tested; arbitrary competing GC/repack writers and Windows unlink behavior were not.

### 4.5 HTTP object-store tests

Reference: `src/storage/tests.rs:187-266`.

| ID | Test | Cases |
|---|---|---|
| S-CAS-RANGE | `conditional_remote_publication_and_exact_ranges` | Missing manifest, create-only publication, stale create/update conflict, ETag readback, successful conditional update, repeated identical immutable upload, exact 5-byte range, reject full-body 200 for a range request, reject immutable-content collision |
| S-MULTIPART | `azure_blocks_and_s3_multipart_are_atomic_and_bounded` | 17,000 deterministic bytes with 8,192-byte test parts produce three parts and exact stored content, for both Azure block and S3 multipart paths |
| S-COMPLETE | Within S-MULTIPART | S3 HTTP 200 with `<Error>` is not success; abort is attempted and failed upload is not installed |

Mocks use localhost HTTP and synthetic credentials. Production part sizes are much larger; the test deliberately uses small parts. This validates the exercised control/data flow, not live service limits, TLS interoperability, IAM policies, retry storms, or signature correctness against AWS.

## 5. Native verification and exploratory checks

### N-FULL: every full SearchGold output

Executed against P-RETAIN, P-CLUSTER, and P-FULL:

```text
git --git-dir <temporary-bare-odb> -c pack.threads=8 \
    index-pack --strict --verify <output.pack>
```

Install hard links named `pack-<trailer-sha1>.pack` and `.idx` in the temporary ODB first. Git's deferred attribute checks need to be able to read the objects being verified. Other implementations must supply a valid ODB context too.

All three passed. Three `gitmodulesParse` warnings came from original `.gitmodules` blobs that contain Git-HFS pointers; those bytes were checked in SearchGold and preserved. See [native-verification.txt](gvfs-daemon/astra/benchmarks/native-verification.txt).

Reproduction helper: [tools/verify_packs.py](gvfs-daemon/astra/tools/verify_packs.py).

### Other executed checks

- Smoke-tested the verifier on a tiny real Git repository with all three representations: each contained nine objects and the same OID-set digest.
- Used native Git to confirm it accepts managed `gvfs-base-<hash>.pack/.idx` filenames.
- Verified `multi-pack-index write --stdin-packs` accepts explicit `.idx` filenames, followed by `multi-pack-index verify`.
- Verified the final weekly baseline and index from each original workload replay using native `index-pack --verify`.
- Opened the optimized real SearchGold client caches, counted two cache packs, cloned a shared workspace, counted zero workspace packs, and traversed full histories:
  - Continuous replay: `660515` commits.
  - Diurnal replay: `658787` commits.
- Verified optimized cache states' checksums, absence of pending transaction journals, and unchanged baseline inodes shared with the immutable client template.
- Checked CLI help after builds and cleanup; all temporary benchmark servers were stopped.
- Inspected Microsoft public client framing and searched for `X-GVFS-Pack-Start/End`. Those exact extension names were not found; the examined native helper uses GPRE timestamps and pack hashes. This was source inspection, not an executed Microsoft-client compatibility test.
- Before implementation, sampled 512 OIDs evenly across the source `.idx` using `cat-file --batch-check`: 316 trees, 146 blobs, 50 commits. This was a sizing probe, not a representative population estimate or a performance result.

### Failed attempts and corrections that matter when reproducing

| Observation | Resolution / interpretation |
|---|---|
| `pdftotext` and `/usr/bin/time` were initially missing | Installed `poppler-utils` for document reading and `time` for measurement; no successful result is attributed to the failed invocations |
| Early native verification outside a proper repository crashed Git; another attempt reported missing `.gitattributes` objects despite their presence in the generated index | Reran with the actual pack/index installed in an explicit temporary ODB; all three large packs passed |
| Reusing an existing read-only verification index caused a permission error | Used controlled temporary verification artifacts without overwriting unrelated files |
| First workload smoke run exhausted source capacity because an edge did not recognize a newly published have in its decoder cache | Added I-HAVE, first observed it fail, fixed negotiation to consult Git rather than decoder membership, then reran both small and full workloads |
| Reusing the client decoder across independently indexed incoming packs initially reused stale ordinal-cache entries | Clear cache keys when admitting a new Source; exercised by C-PIN and subsequent client tests |
| Some intermediate commands accidentally filtered out integration tests, or test functions were temporarily nested | Corrected the invocation/layout and ran the final unfiltered suite; zero selected tests was not treated as success coverage |

Intermediate compile/lint failures were fixed and rerun. They are not benchmark samples.

## 6. Benchmark B-INGEST: complete SearchGold build

### Method

Run once in this order: P-RETAIN, P-CLUSTER, P-FULL, each in a fresh output/work directory:

```sh
RUSTFLAGS='-C target-cpu=native' cargo build --release --message-format=short --quiet
/usr/bin/time -v -o <results>/searchgold-resources.txt \
  target/release/gvfs-daemon benchmark \
  --upstream /home/muks/SearchGold \
  --branch 4d12e2c7915577f43351ac82b9ca15bd7788be89 \
  --store <fresh-store> --work-dir <fresh-work> --strict-alloc
```

The actual initial invocation used the same repository's HEAD; the explicit `--branch` above freezes that recorded commit.

`build_seconds` measures `Engine::pipeline()` after initial Engine construction, not `cargo build`. It includes source admission, object resolution/writing, index construction, filesystem publication and reload. Some initial Engine setup/preallocation is outside this per-mode timer; the outer GNU time covers the whole three-mode process.

No filesystem-cache eviction separated the modes. Compression/layout and CPU order were not randomized. Compare the modes' size results directly; treat timing differences as provisional observations.

### Results

| Mode | Objects | Pack bytes | Git `.idx` bytes | Server `.mphf` bytes | Build seconds | Hot allocations |
|---|---:|---:|---:|---:|---:|---:|
| P-RETAIN | 9224930 | 2673226062 | 279897408 | 562800272 | 98.369 | 0 |
| P-CLUSTER | 9224930 | 4405522013 | 275342048 | 562904032 | 167.353 | 0 |
| P-FULL | 9224930 | 21386711153 | 304050864 | 582500592 | 223.749 | 0 |

Pack trailer SHA-1s:

```text
P-RETAIN  078b4b0620aef528cc3eb3515a12a7de81cb2db8
P-CLUSTER 19183bd0239e89e8a2ad56cade0f1b50ad5ef434
P-FULL    599084143ebe9e9c7c73ac31e2e30bd56ecb5376
```

`.mphf` here is the complete server index file with exact OIDs and metadata, not only the mathematical perfect-hash pilot table. It is not sent to a standard client.

Outer three-mode run: 489.95 s wall, 443.81 s user, 16.97 s system, max RSS 6,291,144 KiB. This RSS is a process high-water mark, not a per-mode average or the sum of all mode allocations.

Sources: [searchgold.csv](gvfs-daemon/astra/benchmarks/searchgold.csv), [searchgold-resources.txt](gvfs-daemon/astra/benchmarks/searchgold-resources.txt). Index byte sizes were additionally checked from retained files.

## 7. Benchmark B-HTTP: startup, warm reads, full prefetch

### Method

Driver: [tools/bench_serve.py](gvfs-daemon/astra/tools/bench_serve.py).

- Start one server for each mode: read-only, strict allocation guard, four workers, localhost ephemeral port.
- Request four logical CPUs via `sched_setaffinity` where available.
- Measure time from just after subprocess launch/affinity setup to the server's `listening` line. This is readiness/reload time, not ingestion time.
- Use one persistent HTTP/1.1 connection and sequential requests.
- Choose one pointer, one HEAD-marked tree, and 64 historical trees from the clustered reference index, then reuse those **same OIDs in all modes**. The exact list is in Appendix A.
- Warm each selected OID once, then repeat its group 40 times.
- Pointer/HEAD group: 40 measured requests each. Historical group: 2,560 measured requests.
- GET latency starts before sending and ends after reading the body. The driver's client-side inflate/SHA-1 correctness check occurs **after** that GET timer.
- Sort samples; p50 = element `n//2`; p99 = element `min(n-1, int(n*0.99))`.
- Stream one full prefetch, verifying its GPRE one-pack header, SHA-1 pack trailer, optional index checksum and EOF.
- Prefetch timing **includes** client checksum verification. Server CPU is the delta of `/proc/<pid>/stat` user+system ticks, not total fleet CPU.
- Read metrics and require `gvfs_alloc_hot_path_total 0`.

This sample is deliberately reproducible but not random: historical OIDs were the first 64 qualifying sampled clustered delta records. It may favor locality or a particular tree population. Forty samples give an especially weak estimate of a pointer/HEAD p99.

### Recorded latency

Units: microseconds. All values are the single recorded run.

| Mode | Pointer p50 / p99 (40) | HEAD tree p50 / p99 (40) | Historical tree p50 / p99 (2560) | Readiness seconds |
|---|---:|---:|---:|---:|
| P-RETAIN | 22.700 / 93.451 | 20.180 / 25.290 | 32.520 / 47.131 | 1.607532 |
| P-CLUSTER | 27.171 / 65.701 | 19.830 / 23.560 | 24.330 / 26.790 | 1.056134 |
| P-FULL | 31.840 / 75.521 | 23.420 / 29.981 | 23.140 / 27.891 | 0.984966 |

### Recorded prefetch

These measurements were taken **before wire-index omission became the default**. Each response included one pack and its prebuilt `.idx`.

| Mode | Response-body bytes | Verified seconds | Verified GB/s | Server CPU seconds | Packs | Hot allocations |
|---|---:|---:|---:|---:|---:|---:|
| P-RETAIN | 2953123502 | 1.134888 | 2.602127 | 0.11 | 1 | 0 |
| P-CLUSTER | 4680864093 | 2.098375 | 2.230710 | 0.18 | 1 | 0 |
| P-FULL | 21690762049 | 8.878943 | 2.442944 | 0.68 | 1 | 0 |

GB/s numerator is `pack_len + max(0, idx_len)`, excluding 32 GPRE bytes. Response-body byte totals include those 32 bytes. GB = 1,000,000,000 bytes.

Source: [searchgold-serving.json](gvfs-daemon/astra/benchmarks/searchgold-serving.json).

### Reproducing old versus current wire policy

The current driver launches the current server default, which omits the wire `.idx`. Running it now without adaptation is a **new pack-only benchmark**, not a reproduction of the table above.

For the historical pack-plus-index policy, have the server adapter append `--prefetch-index`. For example, an executable wrapper used only as the driver's `--binary` can contain:

```sh
#!/bin/sh
exec /home/muks/gvfs-daemon/astra/target/release/gvfs-daemon "$@" --prefetch-index
```

Then run the unchanged driver with a new output directory:

```sh
python3 -B tools/bench_serve.py \
  --binary <server-wrapper> --store <three-mode-store> \
  --work <fresh-serve-work> --output <fresh-results> --rounds 40
```

For other implementations, use the frozen OIDs and identical timing/verification loop rather than requiring Astra's `.mphf` format. Record both pack-only and pack-plus-index lanes where supported.

There was **no final post-optimization HTTP latency/throughput rerun** in this session. Later tests establish functional correctness, and later replays establish body-byte changes, not updated B-HTTP timings.

## 8. Benchmark B-HFS: parser microbenchmark

Source: [benches/engine.rs](gvfs-daemon/astra/benches/engine.rs).

```sh
cargo bench --bench engine --message-format=short --quiet
```

- Ten million parses of the same valid pointer containing the VSO ID from the sample README pointer and size 4167.
- `black_box` on input/output; allocation guard sealed; one timer around the loop.
- Result: **27.71 ns/object**, **0 hot allocations** in the optimized Cargo bench run.
- A later `cargo test --all-targets` also executed this custom `harness = false` binary in a debug profile and printed **1032.51 ns/object**. This was a debug-test side effect, **not** a valid optimized performance comparison.

No throughput distribution, invalid-input mix, multiple-pointer dataset, affinity control, SIMD disassembly, or separate repeat trials were collected.

## 9. Benchmark B-REPLAY: standardized workload definition

Source: [examples/workload_bench.rs](gvfs-daemon/astra/examples/workload_bench.rs).

### 9.1 Fixed logical input

| Setting | Value |
|---|---|
| Seed | Published P-CLUSTER SearchGold baseline, 9,224,930 objects |
| Seed publication timestamp | `1792880752` |
| Replay start | `1792972800` = Monday 2026-10-26 00:00:00 UTC |
| Replay duration | 8 logical days: minutes 1 through 11,520 inclusive |
| Calendar interpretation | Monday through the end of the following Monday; six weekdays plus a weekend |
| Source change cadence | Every 5 minutes |
| Continuous source activity | All eligible minutes; 2,304 commits |
| Diurnal source activity | Weekdays only, 09:00 inclusive to 17:00 exclusive UTC; 576 commits |
| Pipeline cadence | Every 15 minutes, `tier=auto`; daily and weekly rollups run |
| Edge behavior | Reload after publication; live-sync on change/publication before client requests |
| Object capacity | 16,777,216 |
| Maximum object | 32 MiB |
| Replay engine delta cache | 128 MiB per configured engine |
| Live capacity | 128 MiB |
| Range scratch | 1 MiB |
| Optimized captured-response scratch | 128 MiB |
| GC / client maintenance | Legacy: none modeled. Optimized: actual local delta splicing/pruning |

The seed timestamp matters: the driver computes the next Monday from it. A newly generated seed with today's timestamp changes synthetic commit dates, OIDs, and sometimes compressed lengths. Retain the original seed or use the explicit original timestamp for exact reproduction.

### 9.2 Synthetic changes

The fixture extends the real source root with `.gvfs-workload`, containing 256 regular files named `file-00000000` through `file-00000255`.

For file index `i` and revision `r`, pointer bytes are:

```text
~~Git-hfs 1.0\n
blob:<lowercase hex SHA256(u32_le(i) || u32_le(r))>00\n
size:4096\n
```

Revision starts at zero. On each change:

1. Increment revision.
2. Change file `revision % 256`; other pointer OIDs remain unchanged.
3. Write the workload directory tree and update the root tree.
4. Create a single-parent commit with the exact timestamp of this logical minute:

```text
tree <root-tree-oid>
parent <previous-tip-oid>
author Benchmark <bench@example.invalid> <timestamp> +0000
committer Benchmark <bench@example.invalid> <timestamp> +0000

workload <revision>
```

The first change adds the whole 256-file directory; later changes usually add a pointer, two trees, and a commit. This is a **narrow two-level edit workload**, not a measured trace of actual SearchGold authors, multi-directory changes, binary payloads, merges, or renames.

Generate these objects once and share them across implementations. "One file change" is not a sufficient standard if directory width/depth or pointer format differs.

### 9.3 Event order

At each logical minute `m`:

```text
if source activity is enabled and m is divisible by 5:
    generate and publish the next source commit
if m is divisible by 15:
    run pipeline auto at the logical time
    reload the edge manifest
if source changed or pipeline ran:
    catch up live edge to the source
for each due client:
    request/sync using that client's own cursor and validator
```

This gives boundary-aligned clients the just-published state. It does not simulate cloud pipeline launch jitter or nonzero ingestion lag.

### 9.4 Client schedules

Every persistent client performs one initial bootstrap before minute 1.

| Scenario ID in CSV | Schedule | Requests, including bootstrap |
|---|---|---:|
| `fresh_isolated_cache_every_5m` | New isolated cache when `minute % 5 == 2`; no initial shared bootstrap | 2304 |
| `warm_cache_poll_every_1m` | Every minute | 11521 |
| `warm_cache_poll_every_15m` | Minutes divisible by 15 | 769 |
| `warm_cache_poll_every_1h` | Minutes divisible by 60 | 193 |
| `developer_four_fetches_per_workday` | Weekdays at 09:00, 12:00, 15:00, 18:00 | 25 |
| `developer_one_fetch_per_workday` | Weekdays at 09:00 | 7 |
| `friday_evening_then_monday_morning` | Day 4 at 18:00, day 7 at 09:00, plus initial bootstrap | 3 |
| `idle_repo_poll_every_1m` | Every minute against a frozen seed catalog | 11521 |

The weekend scenario's total includes changes accumulated **before Friday**, not just the Friday-to-Monday gap. The CSV's `last_incremental_bytes` is the Monday request alone. Daily-09:00 clients stop receiving Monday changes after that day's 09:00 request even though the replay continues; equal duration does not imply equal final freshness.

### 9.5 Measurement modes

**Legacy replay:**

- Real Rust pipeline, live sync, rollups, response selection, compression, and warm-body serialization.
- Whole-slab responses include `.idx`; spliced responses do not.
- Persistent retained-pack count is modeled from distinct `(GPRE timestamp, pack SHA-1)` identities.
- No persistent client directories, client verification cost, or client deletion are included in that legacy model.

**Optimized replay:**

- Same source workload and schedules.
- Same validator function as HTTP serving; matching validators produce zero body bytes.
- Real `Cache::apply_response`: stage, native verification, exact coverage checks, local splice, installation, state journal, and retirement.
- Actual client directories are checked for the final pack count.
- A full baseline is installed/verified once as a template; immutable pack/index files are hard-linked into the seven independent persistent-cache fixtures, which re-open their own state. Each client's initial download is still accounted.
- This is in-process response delivery, **not a socket or TLS throughput test**.

**Fresh-clone traffic in both modes:**

- Aggregate bytes are computed from actual encoded lengths instead of transferring approximately 10 TB.
- Slab disjointness is checked, and daily cold totals are cross-checked against the production response planner.
- The test does not create 2,304 real isolated workspaces.
- It is valid size accounting for this fixture; it is not a measurement of completing 2,304 clones under load.

### 9.6 Metric definitions

| Field | Meaning |
|---|---|
| `requests` | Scheduled request count, including initial bootstrap where applicable |
| `cold_clones` | Number of independent initial cache populations |
| `pack_responses` | Successful nonempty responses, including repeat bodies |
| `unique_pack_files` | Legacy response-identity counter retained for comparison, even in optimized CSV; **not** the optimized final physical file count |
| `retained_packs_per_cache` | Legacy modeled count; optimized actual `.pack` count after successful sync |
| `gvfs_body_bytes` | Pack bytes + transmitted indexes + GPRE bytes; includes bootstrap |
| `after_initial_clone_bytes` | Additional body bytes after bootstrap for persistent clients |
| `duplicate_pack_responses` | Responses repeating an already seen `(timestamp, pack SHA-1)` identity; not all overlapping-object bytes |
| `largest_incremental_bytes` | Largest post-bootstrap body |
| `last_incremental_bytes` | Last scheduled post-bootstrap body; zero for an optimized final 304 |
| `not_modified` | Matching conditional responses; present in optimized CSV |
| `local_replacement_pack_bytes` | Sum of actual locally written replacement delta-pack lengths; excludes initial baseline, index/state writes, and verification reads |

An empty unmodified GPRE response is 8 bytes; a 304 has zero body bytes but still exchanges HTTP headers in real HTTP. A one-pack response is `8 + 24 + pack_len + max(idx_len, 0)` bytes.

All traffic excludes HTTP headers, TCP/TLS framing, retransmissions, HFS payload hydration, and upstream/object-store traffic. All pack counts refer to `.pack`; each normally also has `.idx` and may have auxiliary metadata.

## 10. B-REPLAY results

Decimal MB/GB/TB are used in summary tables. Exact integers are in Appendix B.

### 10.1 Initial bootstrap and fresh caches

| Result | Legacy | Optimized |
|---|---:|---:|
| Initial P-CLUSTER response bytes | 4680864093 | 4405522045 |
| Index bytes avoided in a pure-baseline response | 0 | 275342048 |
| Fresh isolated caches over 8 days | 2304 | 2304 |
| Packs per isolated cache | 1 | 1 |
| Continuous-write fresh-cache traffic, bytes | 10155745785023 | 10155194851867 |
| Diurnal-write fresh-cache traffic, bytes | 10211130518812 | 10151649644524 |

Removing an unused `.idx` reduces a pure-baseline response, but an isolated agent still downloads a multi-GB baseline. Use a per-host shared cache to change that cost model. The session verified a few real shared clones, not a concurrent 2,304-workspace shared-cache fleet.

### 10.2 Persistent clients: continuous writes

Extra traffic excludes that version's bootstrap.

| Client | Requests | Retained packs before / after | Extra MB before / after | Optimized 304s | Local replacement pack bytes |
|---|---:|---:|---:|---:|---:|
| Every minute | 11521 | 2305 / 2 | 109.260506 / 33.268078 | 8448 | 18777662609 |
| Every 15 minutes | 769 | 769 / 2 | 17.689770 / 16.871622 | 0 | 6483362999 |
| Hourly | 193 | 193 / 2 | 16.741390 / 16.347690 | 0 | 1585706212 |
| Four/workday | 25 | 25 / 2 | 10.512008 / 10.512008 | 0 | 131206099 |
| One/workday | 7 | 7 / 2 | 6.318018 / 6.318018 | 0 | 20224734 |
| Friday then Monday | 3 | 3 / 2 | 4.252174 / 4.252174 | 0 | 7045113 |
| Frozen repository, minute polling | 11521 | 1 / 1 | 0.092160 / 0.000008 | 11519 | 0 |

Minute polling: nonempty responses fell from 8,449 to 2,305; repeated response identities fell from 6,144 to zero. Changing L0 responses still overlap in objects. The optimization does not claim those overlapping network bytes disappeared.

### 10.3 Persistent clients: diurnal writes

| Client | Requests | Retained packs before / after | Extra MB before / after | Optimized 304s | Local replacement pack bytes |
|---|---:|---:|---:|---:|---:|
| Every minute | 11521 | 583 / 2 | 27.355921 / 8.348797 | 10726 | 1184886910 |
| Every 15 minutes | 769 | 199 / 2 | 4.484705 / 4.263309 | 550 | 424526433 |
| Hourly | 193 | 55 / 2 | 4.370081 / 4.254045 | 124 | 115747188 |
| Four/workday | 25 | 25 / 2 | 4.266257 / 4.252013 | 0 | 51946650 |
| One/workday | 7 | 7 / 2 | 0.561719 / 0.553395 | 0 | 1753682 |
| Friday then Monday | 3 | 3 / 2 | 1.129606 / 1.128422 | 0 | 2249598 |
| Frozen repository, minute polling | 11521 | 1 / 1 | 0.092160 / 0.000008 | 11519 | 0 |

Minute polling: nonempty responses fell from 2,119 to 583; repeated identities fell from 1,536 to zero.

### 10.4 Costs and limitations exposed

- Active storage peaked at **17 sealed slabs + L0** for continuous writes, **13 sealed slabs + L0** for diurnal writes. Client responses still contained at most one pack.
- All four successful full replays reported **0 hot allocations** in the instrumented engine regions.
- Optimized continuous minute polling wrote **18.778 GB** of replacement delta packs in eight days. The baseline was not rewritten. This is real local write amplification, not free compaction.
- Two managed packs is a file-count bound after success. The rolling delta's bytes, verification time, scratch needs, and rewrite cost can grow with retained history.
- Temporary generations/journals may add files during installation/recovery; unmanaged packs are deliberately additional.
- The optimization does not implement server full-week replacement plus timestamp-only unlinking. It verifies the locally merged OID coverage instead.

### 10.5 Resource observations

The outer GNU time includes process initialization/teardown and native Git children. "Loop seconds" is the driver's internal elapsed time and starts after substantial setup, including optimized template/cache construction.

| Run | Outer wall s | Loop s | User s | System s | Max RSS KiB | Major / minor faults |
|---|---:|---:|---:|---:|---:|---:|
| Legacy continuous | 110.07 | 108.0 | 88.38 | 11.73 | 19714460 | 272 / 7973732 |
| Legacy diurnal | 95.32 | 93.0 | 83.12 | 8.53 | 19697460 | 0 / 6175915 |
| Optimized continuous | 598.06 | 521.2 | 529.76 | 90.21 | 22415536 | 9 / 14519689 |
| Optimized diurnal | 206.51 | 128.6 | 231.98 | 61.51 | 22391620 | 257 / 8663127 |

These are **whole replay-harness** high-water values: simultaneous pipeline, edge, indexes, client fixtures, and benchmark scratch. They are not one production edge's RSS. Legacy does less work than optimized because it has no real client installs. The optimized runs overlapped and the host was shared, so do not derive isolated regression percentages from this table.

Full GNU time counters, including filesystem input/output operation counters, are retained in the four `*resources.txt` files. They must not be relabeled as network bytes or precise physical disk-write bytes.

## 11. Reproduction commands and smoke lanes

All commands assume `cd /home/muks/gvfs-daemon/astra`. Substitute paths for another implementation. Use fresh output directories; do not erase or mutate SearchGold to prepare a run.

### 11.1 Freeze a replay-compatible baseline

If the original `target/searchgold-benchmark/clustered` survives, reuse it read-only. Otherwise:

```sh
RUSTFLAGS='-C target-cpu=native' cargo build --release \
  --bin gvfs-daemon --example workload_bench

target/release/gvfs-daemon pipeline \
  --upstream /home/muks/SearchGold \
  --branch 4d12e2c7915577f43351ac82b9ca15bd7788be89 \
  --pack-mode clustered --tier 15m --timestamp 1792880752 \
  --store <fresh-seed-store> --work-dir <fresh-seed-work> --strict-alloc
```

For another implementation, use the same source commit/time and verify the same OID set; its compression is allowed to differ. Retained Astra seed artifacts are convenient fixtures, not a required internal storage format.

### 11.2 Run the four traffic lanes sequentially

```sh
/usr/bin/time -v -o <results>/legacy-continuous.resources.txt \
  target/release/examples/workload_bench --legacy \
  --seed <seed> --work <fresh-legacy-continuous> \
  --output <results>/legacy-continuous.csv \
  2> <results>/legacy-continuous.log

/usr/bin/time -v -o <results>/legacy-diurnal.resources.txt \
  target/release/examples/workload_bench --legacy --diurnal-writes \
  --seed <seed> --work <fresh-legacy-diurnal> \
  --output <results>/legacy-diurnal.csv \
  2> <results>/legacy-diurnal.log

/usr/bin/time -v -o <results>/optimized-continuous.resources.txt \
  target/release/examples/workload_bench \
  --seed <seed> --work <fresh-optimized-continuous> \
  --output <results>/optimized-continuous.csv \
  2> <results>/optimized-continuous.log

/usr/bin/time -v -o <results>/optimized-diurnal.resources.txt \
  target/release/examples/workload_bench --diurnal-writes \
  --seed <seed> --work <fresh-optimized-diurnal> \
  --output <results>/optimized-diurnal.csv \
  2> <results>/optimized-diurnal.log
```

These are command templates: replace angle-bracket placeholders with explicit paths. Current `--legacy` selects the old response/client-accounting policy while retaining the negotiation bug fix. Historical old runs predated the flag and therefore show different invocation text.

### 11.3 Small replay smoke tests that were run

Before large replays, bootstrap a one-file real Git repository at timestamp `1699920000`, then run:

- One logical day, 5-minute continuous changes, 16 fixture files, 4,096 object limit.
- Eight logical days, 60-minute continuous changes, 16 fixture files, 4,096 object limit.
- Repeat through the real optimized client after implementation.

Expected source changes: 288 and 192 respectively. The optimized smoke runs ended with at most two managed packs per active persistent cache and zero guarded allocations. Smoke datasets are not byte-volume substitutes for SearchGold.

### 11.4 Independent cache postchecks

For each optimized persistent cache:

```text
count actual objects/pack/*.pack
verify checksummed client state and no pending transaction
verify pinned baseline bytes/inode are unchanged
git --git-dir <cache> rev-parse HEAD
git clone --shared --no-checkout <cache> <fresh-workspace>
git -C <workspace> rev-parse HEAD
count <workspace>/.git/objects/pack/*.pack
git -C <workspace> rev-list --count HEAD
```

For clients scheduled to finish at the latest tip, compare their HEAD with the private source HEAD. For daily/Friday clients, compare with their **last requested** tip, not the final source tip. Keep all shared caches alive until dependent workspaces are finished.

## 12. Standard result contract for other implementations

Store one manifest per run plus raw CSV/JSON/log files. Suggested manifest:

```json
{
  "protocol_version": 1,
  "implementation": "name",
  "implementation_revision": "commit-or-content-hash",
  "benchmark_id": "B-INGEST | B-HTTP | B-HFS | B-REPLAY",
  "trial": 1,
  "source_tip": "4d12e2c7915577f43351ac82b9ca15bd7788be89",
  "source_scope": "HEAD-reachable",
  "source_oid_count": 9224930,
  "source_oid_set_sha256": "8ee9eddde1aa708d68c3dd9abab96d1b2ce2c7e46b8a4f25d5b91489318e17e3",
  "logical_start": 1792972800,
  "pack_policy": "full | retained-tree-delta | clustered-tree-delta | other",
  "wire_index_policy": "omitted | prebuilt-when-whole | always",
  "client_policy": "stock | modeled-native | managed-local-splice | server-rollup-replacement",
  "execution_kind": "http-loopback | serialized-counting | real-client-in-process | calculated",
  "cache_state": "explicitly-recorded",
  "workers": 4,
  "cpu_affinity": "record actual CPU IDs",
  "compiler_flags": "record verbatim",
  "hot_region_definition": "describe exclusions",
  "errors": 0,
  "test_status": "PASS | FAIL | UNSUPPORTED | NOT_RUN"
}
```

Also record language/runtime/toolchain, machine/kernel/filesystem, scratch capacities, index bytes, peak RSS, process/child CPU accounting, elapsed timer boundaries, HTTP version/TLS, source transport, and whether tests overlapped other workloads.

Required output categories:

- **Correctness:** each U/I/C/S/N test ID, pass/fail/unsupported, exact assertion failure when applicable.
- **B-INGEST:** object count/digest, pack/index sizes, policy, elapsed, CPU, RSS, guarded and all-allocation counters.
- **B-HTTP:** the frozen sample manifest, individual latency samples, concurrency, quantile definition, readiness, verified body bytes, server CPU, error count.
- **B-REPLAY:** the CSV fields defined above, plus actual/modelled label for each count and byte field.
- **Client cost:** retained packs, baseline rewrite bytes, delta replacement bytes, and, when measured, index/state writes and verification reads separately.

A standard should not demand the same internal MPHF/slab representation. It should demand equivalent content, explicit resource policies, trustworthy framing, and the same externally observable workload.

## 13. Known gaps: do not mark these as tested

- Live ADO/GitHub ingest performance, live Azure/S3 performance or authorization, TLS handshake throughput, HTTP/2 front-end behavior.
- End-to-end Microsoft `gvfs-helper`, C# VFSForGit, or ProjFS/FUSE hydration runs.
- HFS payload CAS downloads, checkout materialization, application builds, working-tree status/diff performance.
- Concurrent fleet saturation, QPS at load, p99.9 under contention, many slow clients, or multi-host capacity.
- Multi-year cache growth, a sustained months-long compaction run, or a guarantee that two packs bounds total bytes or rewrite cost.
- Server full-week replacement plus client timestamp-only tombstoning. The implemented optimization is local exact-coverage delta splicing.
- Power-loss durability, adversarial filesystem corruption at every operation, disk-full/memory-pressure fault matrices, or simultaneous external GC/repack writers.
- Windows, macOS, filesystem portability, Windows open-file deletion semantics.
- Dedicated fuzzing/property-based campaigns, sanitizer runs, penetration/security reviews.
- Raw packet capture of total wire bytes, cloud request-cost accounting, real WAN RTT effects, or physical device write amplification.
- Independent per-phase CPU profiles, precise allocation attribution outside hot regions, or hardware-vectorization measurements.
- Updated static B-HTTP performance after the final conditional/client changes.
- More than one trial per reported performance configuration, randomized trial order, confidence intervals, or statistically representative object sampling.

These are valuable next comparison lanes, but they are not results from this session.

## Appendix A. Frozen B-HTTP object sample

Use this list across implementations. It was recovered from the retained clustered benchmark artifact using the original sampling routine.

```json
{
  "pointer": "da94087c48c9fe6f39910005e2cff167ef217d7d",
  "head_tree": "a008032af39e1fb208238c305e6cf954a6c1cc4e",
  "historical_trees": [
    "9ae54453d6c8e2f7a03f298ebcfa21b111ce30d6",
    "60a7f16d03b8ea4bc5828e44425ca218ed18e429",
    "7b4ef905ac9a9991c8440338757aa278a7f12e57",
    "ed1ae9d45e848d09f2ef4f35eef8aaf108bb0002",
    "fe6e16c438ee769bf6b3bc0814ccf799bda0bbf7",
    "fbf0fd19193b384f83ca4cb64e0270aa057ec886",
    "9f0870cdce8deb8877cbdfff90a3ea784a7a7954",
    "24198e5a1e89373d081d34ab4504fb8b952e9f51",
    "738532a5c6cdef54c0d88593ecf9b46538939c8f",
    "ff2b891f1a4217e1b8037d6a01edb52406996c87",
    "0557fe6969c3da0c8d32d4e335e8e161290bf4ae",
    "fbdda7060f7cdd5a191ce0ed203b209438c53f31",
    "dfe5867f323e26fbbf8c0756f92f3c4614128ea3",
    "1d6f296e0a456c95d5cfceb0a9a06d05259237ef",
    "04543942f6aecaf3591b20e4f26b4673f38c9491",
    "0e6a7e184358b40d8102e8a41bf66f7257515da1",
    "96995f94a88d15b72dc65f0b7dc13fef6066f67b",
    "b51fb12eed59eb1d3aa5a61a9f6bbf182b555bf3",
    "ca25b906d7c6d6839c4c433f3dfd43bb1139e3d6",
    "6ecb57ae6d9914891e64d0006d976d7c0ee7cbfa",
    "abd4704de0ca34002f1427706467cbc41fdb4f96",
    "8e9028b746a5127e54df7bf764c52e58ef8c26c0",
    "0d9d17a82c6da6fdc081c7a745d60ee8910919f0",
    "90649f3adbe5239ca757e3fada1fe2da864ba9bd",
    "ef6493cce933e75f0a9be5ae658f3bf5cf9a4bc6",
    "89679263cea396a4e85e4d37ce9388e3bfbd65db",
    "77146ae4932282fd497aad771c6f5fb7c96f0a9f",
    "c009ee9073fe42cd4ff723df916649c06c1da08e",
    "3d32faa7a75838136e44f5bcaabb8e650ce28949",
    "bb512bf678482a1ab6e130075f408823dd834ab6",
    "b8c3fda624e5efcba84ec78d2cf024f22aa2bb48",
    "0b819a03c9336cefc1541ce885ab3015c0e90c6e",
    "cfdac65e5dbe7ed0849d4cd98f795d6108077f5e",
    "9110feb7eef314d941650bb748cd497abdc3e616",
    "ee331bad2f29255cf528ac3e568bd699edbca6f0",
    "94ba885dcfc866198d5a20ef29a6b7668d9940ec",
    "f053bd02781cbebdcdd4810f5dbf41846f837085",
    "6b2ef9a529443226e09ddd960902c2962be31176",
    "6d08d7ab9f0838f1890ebfaf45f90603f04827a6",
    "8cc2db16025d3d2cc5c4426160e54db6b304ff55",
    "f04c25d292480361303aeb4ad03009e454a4b353",
    "24d907b76a33d9289832a4209444726ae692d246",
    "4497374e0c3bfcf7f50ed1a399d52e54d143d766",
    "2adb7cf107cb3b2c51e14f5b48b697869ef432cc",
    "920e8c5fef293cb3f9f037ffd5ec21eec8b61cbb",
    "49258a9ac713e3096054885d43a45e1a423f45eb",
    "9b0a43b96e8d14e513a8c2178270fdf54861bc3d",
    "da6913d16c5fa710a683e484feb16a3096ac5494",
    "9d59f7e10e136b716da565fb9bbb399a72e954ea",
    "bf3e34f6764e132d13bdef16213aea0ea4434305",
    "97a7e82bcac74fd00f63f0353228ee9322c99d6b",
    "9e0c9909648b3c88737ec1c6dc30d505bc2e7816",
    "c5224e285befe6f071e757b0f215f1310d727513",
    "9a7c9b11a07aa9ee3ec79345823d779fa994b7e0",
    "4f7e058f680e528de025daaf68aa13db8a614328",
    "d947eace70c1056d57f9f0917eb45070dc4a544c",
    "bcf536ed07868b10d26578a96033585a1d4c39b0",
    "85788b1760f45418ad994814784eb4f695bc964b",
    "e22ad8d41e934e4d0277b48d9af73a0d4eb95a18",
    "b9ab2b0f950a6915c2764254406b020f3cbd1b89",
    "d6c0df902f25bc9f59a29e330d18a2f71202b2f4",
    "55f4e04fa1e3d88271d1fd8964c4a0b31f1735ee",
    "fe98fdb7d54023add9f7c8c3d6ba80045eea3605",
    "12e48fe406f2c9bccf677e12f222202cdc3894dd"
  ]
}
```

## Appendix B. Exact replay outputs

These blocks preserve the original integers, including counters not shown in the summary tables.

### Legacy continuous: `benchmarks/workloads.csv`

```csv
scenario,requests,cold_clones,pack_responses,unique_pack_files,retained_packs_per_cache,gvfs_body_bytes,after_initial_clone_bytes,duplicate_pack_responses,largest_incremental_bytes,last_incremental_bytes
fresh_isolated_cache_every_5m,2304,2304,2304,2304,1,10155745785023,0,0,0,0
warm_cache_poll_every_1m,11521,1,8449,2305,2305,4790124599,109260506,6144,55271,7975
warm_cache_poll_every_15m,769,1,769,769,769,4698553863,17689770,0,55271,7975
warm_cache_poll_every_1h,193,1,193,193,193,4697605483,16741390,0,122423,13130
developer_four_fetches_per_workday,25,1,25,25,25,4691376101,10512008,0,1459107,264320
developer_one_fetch_per_workday,7,1,7,7,7,4687182111,6318018,0,1571944,1571944
friday_evening_then_monday_morning,3,1,3,3,3,4685116267,4252174,0,2793067,1459107
idle_repo_poll_every_1m,11521,1,1,1,1,4680956253,92160,0,8,8
```

### Legacy diurnal: `benchmarks/workloads-diurnal.csv`

```csv
scenario,requests,cold_clones,pack_responses,unique_pack_files,retained_packs_per_cache,gvfs_body_bytes,after_initial_clone_bytes,duplicate_pack_responses,largest_incremental_bytes,last_incremental_bytes
fresh_isolated_cache_every_5m,2304,2304,2304,2304,1,10211130518812,0,0,0,0
warm_cache_poll_every_1m,11521,1,2119,583,583,4708220014,27355921,1536,40048,8
warm_cache_poll_every_15m,769,1,199,199,199,4685348798,4484705,0,40048,8
warm_cache_poll_every_1h,193,1,55,55,55,4685234174,4370081,0,90710,8
developer_four_fetches_per_workday,25,1,25,25,25,4685130350,4266257,0,264741,168685
developer_one_fetch_per_workday,7,1,7,7,7,4681425812,561719,0,104437,104437
friday_evening_then_monday_morning,3,1,3,3,3,4681993699,1129606,0,1121304,8302
idle_repo_poll_every_1m,11521,1,1,1,1,4680956253,92160,0,8,8
```

### Optimized continuous: `benchmarks/workloads-optimized.csv`

```csv
scenario,requests,cold_clones,pack_responses,unique_pack_files,retained_packs_per_cache,gvfs_body_bytes,after_initial_clone_bytes,duplicate_pack_responses,largest_incremental_bytes,last_incremental_bytes,not_modified,local_replacement_pack_bytes
fresh_isolated_cache_every_5m,2304,2304,2304,2304,1,10155194851867,0,0,0,0,0,0
warm_cache_poll_every_1m,11521,1,2305,2305,2,4438790123,33268078,0,46723,7975,8448,18777662609
warm_cache_poll_every_15m,769,1,769,769,2,4422393667,16871622,0,46723,7975,0,6483362999
warm_cache_poll_every_1h,193,1,193,193,2,4421869735,16347690,0,112867,13130,0,1585706212
developer_four_fetches_per_workday,25,1,25,25,2,4416034053,10512008,0,1459107,264320,0,131206099
developer_one_fetch_per_workday,7,1,7,7,2,4411840063,6318018,0,1571944,1571944,0,20224734
friday_evening_then_monday_morning,3,1,3,3,2,4409774219,4252174,0,2793067,1459107,0,7045113
idle_repo_poll_every_1m,11521,1,1,1,1,4405522053,8,0,8,0,11519,0
```

### Optimized diurnal: `benchmarks/workloads-diurnal-optimized.csv`

```csv
scenario,requests,cold_clones,pack_responses,unique_pack_files,retained_packs_per_cache,gvfs_body_bytes,after_initial_clone_bytes,duplicate_pack_responses,largest_incremental_bytes,last_incremental_bytes,not_modified,local_replacement_pack_bytes
fresh_isolated_cache_every_5m,2304,2304,2304,2304,1,10151649644524,0,0,0,0,0,0
warm_cache_poll_every_1m,11521,1,583,583,2,4413870842,8348797,0,31724,8,10726,1184886910
warm_cache_poll_every_15m,769,1,199,199,2,4409785354,4263309,0,31724,8,550,424526433
warm_cache_poll_every_1h,193,1,55,55,2,4409776090,4254045,0,88294,8,124,115747188
developer_four_fetches_per_workday,25,1,25,25,2,4409774058,4252013,0,264741,168685,0,51946650
developer_one_fetch_per_workday,7,1,7,7,2,4406075440,553395,0,104437,104437,0,1753682
friday_evening_then_monday_morning,3,1,3,3,2,4406650467,1128422,0,1121304,7118,0,2249598
idle_repo_poll_every_1m,11521,1,1,1,1,4405522053,8,0,8,0,11519,0
```

## Appendix C. Evidence inventory and fingerprints

All paths below are under `/home/muks/gvfs-daemon/astra`.

| Evidence | Files |
|---|---|
| Static build | `benchmarks/searchgold.csv`, `benchmarks/searchgold-resources.txt` |
| Static HTTP serving | `benchmarks/searchgold-serving.json` |
| Full Git validation | `benchmarks/native-verification.txt` |
| Legacy continuous | `benchmarks/workloads.csv`, `benchmarks/workload-run.log`, `benchmarks/workload-resources.txt` |
| Legacy diurnal | `benchmarks/workloads-diurnal.csv`, `benchmarks/workload-diurnal-run.log`, `benchmarks/workload-diurnal-resources.txt` |
| Optimized continuous | `benchmarks/workloads-optimized.csv`, `benchmarks/workload-optimized-run.log`, `benchmarks/workload-optimized-resources.txt` |
| Optimized diurnal | `benchmarks/workloads-diurnal-optimized.csv`, `benchmarks/workload-diurnal-optimized-run.log`, `benchmarks/workload-diurnal-optimized-resources.txt` |
| Native validation helper | `tools/verify_packs.py` |
| HTTP performance driver | `tools/bench_serve.py` |
| Workload generator/driver | `examples/workload_bench.rs` |
| HFS microbenchmark | `benches/engine.rs` |
| Main integration tests | `tests/end_to_end.rs` |
| Client ownership/recovery tests | `src/client/tests.rs` |
| Storage mock tests | `src/storage/tests.rs` |
| Full retained seed artifacts | `target/searchgold-benchmark/{delta,clustered,full}` |
| Full original replay artifacts | `target/workload-replay`, `target/workload-replay-diurnal` |
| Actual optimized cache fixtures | `target/workload-replay-optimized/clients`, `target/workload-replay-diurnal-optimized/clients` |

The unit/integration command outcomes and microbenchmark stdout are recorded in the session transcript; they were not separately saved as machine-readable test-result files. The result files above should be preserved when comparing implementations.

Source fingerprints at report time:

```text
77ed783931050cd3dc87f6cd1c8e7b7373744335d41ff549a99e629a5e3c15c5  Cargo.lock
0f5d677279e8b8a7c2350ada89d5da9c21677b2d4b243010fc8059619b5973b3  examples/workload_bench.rs
48cade33f907e852ac5402e003e8b0967f944326150d11f85a8446b5df6388c9  tools/bench_serve.py
dcc42006ee15999750d1b77d956cb6bb4c98565703983cd375d9398bc793cee0  tools/verify_packs.py
028270861927b0929a45c189021b2806f211e9cd4e271d51dcda584c4a4cc3f1  benches/engine.rs
```
