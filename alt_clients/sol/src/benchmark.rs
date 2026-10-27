//! Reproducible, same-input comparison. Network acquisition is outside both timers;
//! arena initialization, decoding, ordering, compression and all indexes are inside.

use std::io::Write;
use std::path::Path;
use std::process::Stdio;
use std::time::Instant;

use crate::arena::{push, reserve};
use crate::codec::{Kind, Oid, object_id};
use crate::ingest::IngestArena;
use crate::manifest::{Manifest, Tier};
use crate::pipeline::PipelineOptions;
use crate::slab::{Builder, Limits, NO_BASE, Slab};
use crate::storage::{Storage, StoreKind, artifact_key};
use crate::tree::{TreeLayout, TreeReader};
use crate::upstream::{Upstream, git_command};
use crate::{Error, Result};

const SAMPLES: usize = 50000;
const TREE_SAMPLES: usize = 5000;

pub struct Row {
    pub layout: &'static str,
    pub objects: u32,
    pub pack_bytes: u64,
    pub graph_bytes: u64,
    pub build_seconds: f64,
    pub decode_seconds: f64,
    pub tree_deltas: u64,
    pub lookup_p50_ns: u64,
    pub lookup_p99_ns: u64,
    pub tree_decode_p99_ns: u64,
    pub tree_range_p99_bytes: u64,
    pub hot_allocations: u64,
    pub native_seconds: f64,
}

pub async fn run(
    root: &str,
    cache: &Path,
    upstream: &mut Upstream,
    options: &PipelineOptions,
    range_bytes: usize,
) -> Result<()> {
    if root.starts_with("http://") || root.starts_with("https://") {
        return Err(Error::Usage(
            "bench --storage must be a local output directory".into(),
        ));
    }
    std::fs::create_dir_all(cache)?;
    let tip = upstream.head().await?;
    let input = match &options.input_pack {
        Some(path) => crate::upstream::input_pack(path, options.limits.pack_bytes)?,
        None => {
            upstream
                .fetch(Oid::ZERO, tip, cache, options.limits.pack_bytes)
                .await?
        }
    };
    let run = Path::new(root.strip_prefix("file://").unwrap_or(root)).join(format!(
        "run-{}-{}",
        crate::cli::now()?,
        std::process::id()
    ));
    std::fs::create_dir_all(&run)?;
    let mut report = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(run.join("results.csv"))?;
    writeln!(
        report,
        "layout,objects,pack_bytes,graph_bytes,build_seconds,decode_seconds,tree_deltas,lookup_p50_ns,lookup_p99_ns,tree_decode_p99_ns,tree_range_p99_bytes,hot_allocations,native_seconds"
    )?;
    println!(
        "layout       objects       pack_MiB    build_s  tree_deltas  lookup_p99_ns  tree_decode_p99_ns  range_p99_B"
    );
    for layout in [TreeLayout::Full, TreeLayout::Clustered] {
        let label = if layout == TreeLayout::Full {
            "full"
        } else {
            "clustered"
        };
        let directory = run.join(label);
        eprintln!("{label}: decoding and ordering input");
        let mut limits = options.limits;
        limits.tree_layout = layout;
        let started = Instant::now();
        let mut arena = IngestArena::mapped(limits, layout, range_bytes, cache)?;
        let mut builder = Builder::mapped(limits, cache)?;
        let decode_start = Instant::now();
        let hot = gvfs_platform::HotRegion::enter();
        let decoded = arena.decode(&input, tip, &[], &mut builder);
        let hot_allocations = hot.allocations();
        drop(hot);
        arena.diagnostics().report();
        if let Err(error) = decoded {
            eprintln!(
                "{label}: decode failed after {:.3}s: {error}",
                decode_start.elapsed().as_secs_f64()
            );
            return Err(error);
        }
        if hot_allocations != 0 {
            return Err(Error::Invalid("benchmark ingestion allocated"));
        }
        let decode_seconds = decode_start.elapsed().as_secs_f64();
        drop(arena);
        let slab = builder.finish(Tier::Baseline, 0, options.now.max(1), tip)?;
        let build_seconds = started.elapsed().as_secs_f64();
        slab.validate()?;
        eprintln!(
            "{label}: built {} objects, {} bytes in {:.3}s; publishing and verifying",
            slab.reference.count, slab.reference.pack_len, build_seconds
        );
        let storage = Storage::open(
            directory
                .to_str()
                .ok_or(Error::Invalid("benchmark path UTF-8"))?,
            directory.join("cache"),
            range_bytes,
            limits.pack_bytes,
            StoreKind::Generic,
            None,
            None,
            1,
        )?;
        storage.upload(&slab).await?;
        storage
            .publish_manifest(&Manifest::genesis(slab.reference)?, None)
            .await?;
        let native_started = Instant::now();
        native_verify(&directory, &slab, tip)?;
        let native_seconds = native_started.elapsed().as_secs_f64();
        let mut row = measure(&slab, limits)?;
        row.layout = label;
        row.build_seconds = build_seconds;
        row.decode_seconds = decode_seconds;
        row.hot_allocations = hot_allocations;
        row.native_seconds = native_seconds;
        println!(
            "{:<10} {:>10} {:>14.2} {:>10.3} {:>12} {:>14} {:>19} {:>12}",
            row.layout,
            row.objects,
            row.pack_bytes as f64 / 1048576.0,
            row.build_seconds,
            row.tree_deltas,
            row.lookup_p99_ns,
            row.tree_decode_p99_ns,
            row.tree_range_p99_bytes
        );
        writeln!(
            report,
            "{},{},{},{},{:.6},{:.6},{},{},{},{},{},{},{:.6}",
            row.layout,
            row.objects,
            row.pack_bytes,
            row.graph_bytes,
            row.build_seconds,
            row.decode_seconds,
            row.tree_deltas,
            row.lookup_p50_ns,
            row.lookup_p99_ns,
            row.tree_decode_p99_ns,
            row.tree_range_p99_bytes,
            row.hot_allocations,
            row.native_seconds
        )?;
        report.sync_all()?;
    }
    println!("verified artifacts and CSV: {}", run.display());
    Ok(())
}

fn measure(slab: &Slab, limits: Limits) -> Result<Row> {
    let mut lookups = reserve(SAMPLES)?;
    let mut tree_times = reserve(TREE_SAMPLES)?;
    let mut spans = reserve(TREE_SAMPLES)?;
    let mut reader = TreeReader::new(limits)?;
    let mut random = 0x2410_2026u64;
    let mut tree_deltas = 0u64;
    for rank in 0..slab.reference.count {
        if slab.record(rank)?.base != NO_BASE {
            tree_deltas = tree_deltas.saturating_add(1);
        }
    }
    let hot = gvfs_platform::HotRegion::enter();
    for _ in 0..SAMPLES {
        random = crate::mphf::mix(random.wrapping_add(0x9e37_79b9_7f4a_7c15));
        let rank = random
            .checked_rem(u64::from(slab.reference.count))
            .ok_or(Error::Invalid("empty benchmark pack"))? as u32;
        let record = slab.record(rank)?;
        let start = Instant::now();
        let found = std::hint::black_box(slab.lookup(std::hint::black_box(record.oid))?);
        let elapsed = u64::try_from(start.elapsed().as_nanos())
            .map_err(|_| Error::Invalid("benchmark clock"))?;
        if found != Some(rank) {
            return Err(Error::Invalid("benchmark OID lookup"));
        }
        push(&mut lookups, elapsed)?;
        if record.kind == Kind::Tree && tree_times.len() < TREE_SAMPLES {
            let start = Instant::now();
            let len = reader.read_local(slab, rank)?;
            if object_id(Kind::Tree, reader.content(len)?)? != record.oid {
                return Err(Error::Invalid("benchmark decoded tree OID"));
            }
            push(
                &mut tree_times,
                u64::try_from(start.elapsed().as_nanos())
                    .map_err(|_| Error::Invalid("benchmark clock"))?,
            )?;
            push(&mut spans, TreeReader::span(slab, rank)?.1 as u64)?;
        }
    }
    let allocations = hot.allocations();
    drop(hot);
    if allocations != 0 {
        return Err(Error::Invalid("benchmark lookup or tree decode allocated"));
    }
    lookups.sort_unstable();
    tree_times.sort_unstable();
    spans.sort_unstable();
    let percentile = |values: &[u64], percent: usize| -> u64 {
        let index = values.len().saturating_sub(1).saturating_mul(percent) / 100;
        values.get(index).copied().unwrap_or(0)
    };
    Ok(Row {
        layout: "",
        objects: slab.reference.count,
        pack_bytes: slab.reference.pack_len,
        graph_bytes: slab.links.len() as u64,
        build_seconds: 0.0,
        decode_seconds: 0.0,
        tree_deltas,
        lookup_p50_ns: percentile(&lookups, 50),
        lookup_p99_ns: percentile(&lookups, 99),
        tree_decode_p99_ns: percentile(&tree_times, 99),
        tree_range_p99_bytes: percentile(&spans, 99),
        hot_allocations: 0,
        native_seconds: 0.0,
    })
}

fn native_verify(root: &Path, slab: &Slab, tip: Oid) -> Result<()> {
    let git = root.join("verify.git");
    let status = git_command()
        .args(["init", "--quiet", "--bare", "--initial-branch=main"])
        .arg(&git)
        .status()?;
    if !status.success() {
        return Err(Error::Transport(format!("git init failed: {status}")));
    }
    for extension in ["pack", "idx"] {
        let source = root.join(artifact_key(slab.reference.id, extension));
        let destination = git
            .join("objects/pack")
            .join(format!("pack-{}.{}", slab.reference.id, extension));
        std::fs::hard_link(source, destination)?;
    }
    let index = root.join(artifact_key(slab.reference.id, "idx"));
    let status = git_command()
        .args(["verify-pack", "-s"])
        .arg(index)
        .stdout(Stdio::null())
        .status()?;
    if !status.success() {
        return Err(Error::Transport(format!(
            "native Git pack/index verification failed: {status}"
        )));
    }
    let status = git_command()
        .arg("--git-dir")
        .arg(&git)
        .args(["update-ref", "refs/heads/main", &tip.to_string()])
        .status()?;
    if !status.success() {
        return Err(Error::Transport(format!(
            "native Git update-ref failed: {status}"
        )));
    }
    let status = git_command()
        .arg("--git-dir")
        .arg(&git)
        .args(["fsck", "--full", "--no-reflogs", "--no-progress"])
        .stdout(Stdio::null())
        .status()?;
    if !status.success() {
        return Err(Error::Transport(format!(
            "native Git connectivity verification failed: {status}"
        )));
    }
    let count = std::fs::read_dir(git.join("objects/pack"))?.try_fold(
        0usize,
        |count, entry| -> Result<usize> {
            let entry = entry?;
            Ok(count.saturating_add(usize::from(
                entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "pack"),
            )))
        },
    )?;
    if count != 1 {
        return Err(Error::Invalid("benchmark must leave exactly one Git pack"));
    }
    Ok(())
}
