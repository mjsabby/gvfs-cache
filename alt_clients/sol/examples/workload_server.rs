//! Deterministic clock/control adapter around the real pipeline, L0 and HTTP server.
//! Client traffic and native maintenance are driven by tools/workloads.py.
#![forbid(unsafe_code)]

use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use gvfs_daemon::arena::reserve;
use gvfs_daemon::codec::Oid;
use gvfs_daemon::pipeline::{PipelineCache, PipelineOptions, run_pipeline_cached};
use gvfs_daemon::server::{ServerOptions, run_listener, runtime};
use gvfs_daemon::slab::Limits;
use gvfs_daemon::storage::{Storage, StoreKind};
use gvfs_daemon::tree::TreeLayout;
use gvfs_daemon::upstream::{Upstream, git_command};
use gvfs_daemon::{Error, Result};

#[tokio::main(worker_threads = 4)]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("workload-server: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let store = args
        .next()
        .ok_or_else(|| Error::Usage("workload_server STORE UPSTREAM OUTPUT".into()))?;
    let upstream = args
        .next()
        .ok_or_else(|| Error::Usage("missing upstream".into()))?;
    let output = PathBuf::from(
        args.next()
            .ok_or_else(|| Error::Usage("missing output".into()))?,
    );
    std::fs::create_dir_all(&output)?;
    let limits = Limits {
        objects: 1 << 24,
        pack_bytes: 64usize << 30,
        object_bytes: 128 << 20,
        links: 0x7fff_ffff,
        tree_layout: TreeLayout::Clustered,
        cache_bytes: 128 << 20,
        strict_hfs: false,
    };
    let storage = Arc::new(Storage::open(
        &store,
        output.join("cache"),
        4 << 20,
        limits.pack_bytes,
        StoreKind::Generic,
        None,
        None,
        2,
    )?);
    let archive = Storage::open(
        output
            .join("archive")
            .to_str()
            .ok_or(Error::Invalid("archive path"))?,
        output.join("archive-cache"),
        4 << 20,
        limits.pack_bytes,
        StoreKind::Generic,
        None,
        None,
        1,
    )?;
    let first = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let second = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let before_address = first.local_addr()?;
    let after_address = second.local_addr()?;
    let options = ServerOptions {
        bind: after_address,
        limits,
        live_limits: Limits {
            objects: 1 << 16,
            pack_bytes: 256 << 20,
            object_bytes: 32 << 20,
            links: 1 << 23,
            cache_bytes: 32 << 20,
            tree_layout: TreeLayout::Full,
            strict_hfs: false,
        },
        arenas: 1,
        snapshot_slots: 8,
        max_ranges: 1 << 18,
        automatic_sync: false,
        connection_seconds: 3600,
        ..ServerOptions::default()
    };
    let (after, mut live_upstream) = runtime::boot(
        Arc::clone(&storage),
        Some(Upstream::new(upstream.clone(), "main".into(), None, None)?),
        options.clone(),
    )
    .await?;
    let mut before_options = options;
    before_options.bind = before_address;
    before_options.live = false;
    before_options.coarse_prefetch = true;
    before_options.prefetch_indexes = false;
    let (before, _) = runtime::boot_with_slabs(
        Arc::clone(&storage),
        None,
        before_options,
        &after.snapshot()?.persistent,
    )
    .await?;
    let (stop_before, ended_before) = tokio::sync::oneshot::channel();
    let (stop_after, ended_after) = tokio::sync::oneshot::channel();
    let before_task = tokio::spawn(run_listener(first, Arc::clone(&before), None, async {
        ended_before
            .await
            .map_err(|_| Error::Invalid("before shutdown"))?;
        Ok(())
    }));
    let after_task = tokio::spawn(run_listener(second, Arc::clone(&after), None, async {
        ended_after
            .await
            .map_err(|_| Error::Invalid("after shutdown"))?;
        Ok(())
    }));
    let mut cache = PipelineCache::new()?;
    cache.loaded = after.snapshot()?.persistent.clone();
    let mut pipe_upstream = Upstream::new(upstream.clone(), "main".into(), None, None)?;
    let mut last_tip = after.snapshot()?.tip();
    let mut stdout = std::io::stdout().lock();
    writeln!(
        stdout,
        "{{\"ready\":true,\"before\":\"http://{before_address}\",\"after\":\"http://{after_address}\"}}"
    )?;
    stdout.flush()?;
    let result: Result<()> = async {
        for line in std::io::stdin().lock().lines() {
            let line = line?;
            let mut fields = line.split_ascii_whitespace();
            let Some(command) = fields.next() else {
                continue;
            };
            if command == "quit" {
                break;
            }
            if command != "tick" {
                return Err(Error::Invalid("workload command"));
            }
            let now = fields
                .next()
                .ok_or(Error::Invalid("tick time"))?
                .parse::<u64>()
                .map_err(|_| Error::Invalid("tick time"))?;
            let tip = Oid::parse(fields.next().ok_or(Error::Invalid("tick tip"))?.as_bytes())?;
            let seal = match fields.next() {
                Some("0") => false,
                Some("1") => true,
                _ => return Err(Error::Invalid("tick seal")),
            };
            if fields.next().is_some() {
                return Err(Error::Invalid("extra tick arguments"));
            }
            let started = Instant::now();
            if tip != last_tip {
                let status = git_command()
                    .arg("--git-dir")
                    .arg(&upstream)
                    .args(["update-ref", "refs/heads/main", &tip.to_string()])
                    .status()?;
                if !status.success() {
                    return Err(Error::Transport(format!("replay update-ref: {status}")));
                }
                last_tip = tip;
            }
            let mut writes = 0usize;
            if seal {
                let report = run_pipeline_cached(
                    &storage,
                    Some(&mut pipe_upstream),
                    &PipelineOptions {
                        tier: None,
                        now,
                        limits,
                        max_ranges: 1 << 18,
                        gap: 4096,
                        retention_secs: 14 * 86400,
                        input_pack: None,
                    },
                    &mut cache,
                )
                .await?;
                writes = report.artifacts;
            }
            runtime::refresh_at(&after, live_upstream.as_mut(), seal, now).await?;
            let snapshot = after.snapshot()?;
            let permit = Arc::clone(&before.generations)
                .try_acquire_owned()
                .map_err(|_| Error::Capacity("before replay snapshots"))?;
            *before
                .current
                .write()
                .map_err(|_| Error::Invalid("before catalog lock"))? = Arc::new(runtime::Catalog {
                manifest: snapshot.manifest.clone(),
                etag: snapshot.etag.clone(),
                persistent: snapshot.persistent.clone(),
                live: snapshot.live.clone(),
                _permit: permit,
            });
            std::fs::write(
                output.join(format!("{now}.manifest")),
                snapshot.manifest.encode()?,
            )?;
            if let Some(live) = &snapshot.live {
                archive.upload(live).await?;
                let mut reference = reserve(80)?;
                live.reference.encode(&mut reference)?;
                std::fs::write(output.join(format!("{now}.live")), reference)?;
            }
            let live_objects = snapshot.live.as_ref().map_or(0, |slab| slab.reference.count);
            let mut tiers = [0usize; 5];
            for reference in &snapshot.manifest.pyramid {
                let i = match reference.tier {
                    gvfs_daemon::manifest::Tier::M15 => 0,
                    gvfs_daemon::manifest::Tier::H1 => 1,
                    gvfs_daemon::manifest::Tier::H6 => 2,
                    gvfs_daemon::manifest::Tier::H12 => 3,
                    _ => 4,
                };
                if let Some(value) = tiers.get_mut(i) {
                    *value = value.saturating_add(1);
                }
            }
            writeln!(
                stdout,
                "{{\"time\":{now},\"tip\":\"{}\",\"cursor\":{},\"generation\":{},\"live_objects\":{live_objects},\"slabs_written\":{writes},\"seconds\":{:.6},\"tiers\":{:?}}}",
                snapshot.tip(),
                snapshot.cursor(),
                snapshot.manifest.generation,
                started.elapsed().as_secs_f64(),
                tiers
            )?;
            stdout.flush()?;
        }
        Ok(())
    }
    .await;
    let _ = stop_before.send(());
    let _ = stop_after.send(());
    for task in [before_task, after_task] {
        task.await
            .map_err(|error| Error::Transport(error.to_string()))??;
    }
    result
}
