//! Command implementations behind the clap entry point (`main.rs`).

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use crate::ingest::{Destination, IngestConfig, ingest_git_delta};
use crate::manifest::{Manifest, Selection, SlabRef, Tier};
use crate::oid::Oid;
use crate::pack::{DeltaBase, parse_entry_header};
use crate::pipeline::{haves_for, now_ts};
use crate::slab::{SealScratch, Slab, SlabSet};
use crate::splice::{Plan, SlabList, write_splice};
use crate::store::FsStore;
use crate::transport::{Auth, Upstream, open_upstream};

pub type CliError = Box<dyn std::error::Error + Send + Sync>;

fn err(msg: impl Into<String>) -> CliError {
    msg.into().into()
}

/// `--token-env`/`--token-user` into an `Authorization` scheme.
pub fn auth_from_args(token_env: Option<&str>, token_user: &str) -> Result<Auth, CliError> {
    Ok(match token_env {
        Some(var) => {
            let token = std::env::var(var)
                .map_err(|_| err(format!("environment variable {var} is not set")))?;
            Auth::Basic {
                user: token_user.to_string(),
                token,
            }
        }
        None => Auth::None,
    })
}

/// Builds the upstream from `--upstream`/`--token-env`/`--token-user`.
pub fn upstream_from_args(
    spec: &str,
    token_env: Option<&str>,
    token_user: &str,
) -> Result<Box<dyn Upstream + Send>, CliError> {
    Ok(open_upstream(spec, auth_from_args(token_env, token_user)?)?)
}

/// The client hook (`gvfs-daemon client-prefetch`).
pub fn cmd_client_prefetch(
    opts: &crate::client::Options<'_>,
    out: &mut dyn Write,
) -> Result<(), CliError> {
    let s = crate::client::client_prefetch(opts, out)?;
    match s.interval {
        Some((start, end)) => {
            let _ = writeln!(
                out,
                "prefetch from {}: {} pack(s), {} covering [{start}..{end}]",
                s.cursor,
                s.received,
                fmt_bytes(s.bytes)
            );
        }
        None => {
            let _ = writeln!(out, "prefetch from {}: nothing new", s.cursor);
        }
    }
    if s.compaction_bytes > 0 {
        let _ = writeln!(
            out,
            "compaction: {} re-fetched",
            fmt_bytes(s.compaction_bytes)
        );
    }
    let _ = writeln!(
        out,
        "{} subsumed pack(s) unlinked; {} prefetch pack(s) in {}",
        s.unlinked,
        s.local_packs,
        opts.pack_dir.display()
    );
    Ok(())
}

pub fn branch_ref(branch: &str) -> String {
    if branch.starts_with("refs/") {
        branch.to_string()
    } else {
        format!("refs/heads/{branch}")
    }
}

pub struct IngestArgs<'a> {
    pub store: &'a Path,
    pub upstream: &'a mut dyn Upstream,
    pub branch: String,
    pub want: Option<Oid>,
    pub haves: Vec<Oid>,
    pub tier: Tier,
    pub dedup: bool,
    pub publish: bool,
    pub ingest: IngestConfig,
}

/// One-shot slab build (`gvfs-daemon ingest`).
pub fn cmd_ingest(args: IngestArgs<'_>, out: &mut dyn Write) -> Result<(), CliError> {
    let store = FsStore::open(args.store)?;
    let mut manifest = store.load_manifest()?;
    let want = match args.want {
        Some(w) => w,
        None => args
            .upstream
            .ls_ref(&args.branch)?
            .ok_or_else(|| err(format!("branch {} not found upstream", args.branch)))?,
    };
    let haves: Vec<Oid> = if !args.haves.is_empty() {
        args.haves.clone()
    } else if args.tier == Tier::Baseline {
        Vec::new()
    } else {
        manifest.as_ref().map(haves_for).unwrap_or_default()
    };
    let dedup_set = if args.dedup && args.tier != Tier::Baseline {
        match &manifest {
            Some(m) => Some(SlabSet::open(&store, m.lookup_slabs())?),
            None => None,
        }
    } else {
        None
    };
    let dedup_fn = |oid: &Oid| dedup_set.as_ref().is_some_and(|s| s.contains(oid));
    let dedup: Option<&(dyn Fn(&Oid) -> bool + Sync)> = if dedup_set.is_some() {
        Some(&dedup_fn)
    } else {
        None
    };
    let seq = manifest.as_mut().map_or(0, Manifest::allocate_seq);
    let now = now_ts();
    let start_ts = if args.tier == Tier::Baseline {
        0
    } else {
        manifest.as_ref().map_or(0, Manifest::latest_ts)
    };
    let mut scratch = SealScratch::new();
    let t0 = std::time::Instant::now();
    let _ = writeln!(
        out,
        "fetching {want} from {} ({} haves) ...",
        args.upstream.describe(),
        haves.len()
    );
    let ing = ingest_git_delta(
        args.upstream,
        &[want],
        &haves,
        args.tier,
        start_ts,
        now,
        dedup,
        Destination::Store { store: &store, seq },
        &args.ingest,
        &mut scratch,
    )?;
    drop(dedup_set);
    let Some(ing) = ing else {
        let _ = writeln!(
            out,
            "nothing new (every fetched object was already published)"
        );
        return Ok(());
    };
    let _ = writeln!(
        out,
        "slab {seq} ({}) sealed in {:.1}s: fetched {} bytes / {} objects; kept {} ({} duplicates, {} de-deltified); {} pointer blobs, {} other blobs; pack {} B, idx {} B, mphf {} B",
        args.tier,
        t0.elapsed().as_secs_f64(),
        ing.fetched_bytes,
        ing.fetched_objects,
        ing.meta.object_count,
        ing.duplicates,
        ing.converted,
        ing.pointer_blobs,
        ing.other_blobs,
        ing.meta.pack_len,
        ing.meta.idx_len,
        ing.meta.mphf_len
    );
    if args.publish {
        let expected = manifest.as_ref().map(|m| m.generation);
        let mut m = manifest.unwrap_or_else(|| Manifest {
            next_seq: 1,
            ..Manifest::default()
        });
        match args.tier {
            Tier::Baseline => m.baseline = Some(ing.meta),
            Tier::Weekly => m.weekly.push(ing.meta),
            _ => m.push_pyramid(ing.meta, Vec::new()),
        }
        m.tip = want;
        store.store_manifest(&mut m, expected)?;
        let _ = writeln!(out, "published manifest generation {}", m.generation);
    }
    Ok(())
}

fn fmt_bytes(n: u64) -> String {
    if n >= 1 << 30 {
        format!("{:.2} GiB", n as f64 / (1u64 << 30) as f64)
    } else if n >= 1 << 20 {
        format!("{:.1} MiB", n as f64 / (1u64 << 20) as f64)
    } else if n >= 1 << 10 {
        format!("{:.1} KiB", n as f64 / 1024.0)
    } else {
        format!("{n} B")
    }
}

fn print_slab_row(out: &mut dyn Write, label: &str, s: &SlabRef) {
    let _ = writeln!(
        out,
        "  {label:<9} slab {:>6}  {:<8} objects {:>10}  pack {:>10}  idx {:>10}  mphf {:>9}  ts {}..{}  tip {}",
        s.seq,
        s.tier.as_str(),
        s.object_count,
        fmt_bytes(s.pack_len),
        fmt_bytes(s.idx_len),
        fmt_bytes(s.mphf_len),
        s.start_ts,
        s.end_ts,
        s.tip
    );
}

/// `gvfs-daemon inspect`: the manifest and, with `verify`, every slab's consistency.
pub fn cmd_inspect(store_dir: &Path, verify: bool, out: &mut dyn Write) -> Result<(), CliError> {
    let store = FsStore::open(store_dir)?;
    let Some(m) = store.load_manifest()? else {
        let _ = writeln!(
            out,
            "no manifest in {} (store not bootstrapped)",
            store_dir.display()
        );
        let seqs = store.list_slab_seqs()?;
        if !seqs.is_empty() {
            let _ = writeln!(out, "unreferenced slab files: {seqs:?}");
        }
        return Ok(());
    };
    let _ = writeln!(
        out,
        "manifest generation {} updated {} tip {} next_seq {}",
        m.generation, m.updated_ts, m.tip, m.next_seq
    );
    if let Some(b) = &m.baseline {
        print_slab_row(out, "baseline", b);
    }
    for w in &m.weekly {
        print_slab_row(out, "weekly", w);
    }
    for (i, p) in m.pyramid.iter().enumerate() {
        print_slab_row(out, "pyramid", p);
        for l in m.leaves_of(i) {
            print_slab_row(out, "  leaf", l);
        }
    }
    let total: u64 = m.all_slabs().map(|s| s.pack_len).sum();
    let objects: u64 = m.all_slabs().map(|s| u64::from(s.object_count)).sum();
    let _ = writeln!(
        out,
        "  total: {} slabs, {objects} objects, {} of packs",
        m.all_slabs().count(),
        fmt_bytes(total)
    );
    let orphans: Vec<u64> = store
        .list_slab_seqs()?
        .into_iter()
        .filter(|seq| !m.all_slabs().any(|s| s.seq == *seq))
        .collect();
    if !orphans.is_empty() {
        let _ = writeln!(out, "  unreferenced slab files (pending gc): {orphans:?}");
    }
    if verify {
        let mut scratch = vec![0u8; 64 << 10];
        for s in m.all_slabs() {
            let t0 = std::time::Instant::now();
            let slab = Slab::open(&store, *s, true)?;
            let idx = slab.idx().ok_or_else(|| err("idx unreadable"))?;
            let mphf = slab.mphf().ok_or_else(|| err("mphf unreadable"))?;
            let mut inflater = crate::zlib::Inflater::new();
            let mut bad = 0u64;
            for i in 0..idx.n {
                let oid = idx.oid(i).ok_or_else(|| err("idx entry"))?;
                let off = idx.offset(i).ok_or_else(|| err("idx offset"))?;
                match mphf.lookup(&oid) {
                    Some((_, loc)) if loc.offset == off => {
                        // The entry must parse and its zlib stream must end exactly at len.
                        let bytes = slab.entry(loc);
                        let ok = parse_entry_header(bytes).ok().and_then(|h| {
                            let z = bytes.get(h.header_len..)?;
                            let (consumed, produced) = inflater.measure(z, &mut scratch).ok()?;
                            Some(consumed == z.len() && produced as u64 == h.size)
                        });
                        if ok != Some(true) {
                            bad = bad.saturating_add(1);
                        }
                    }
                    _ => bad = bad.saturating_add(1),
                }
            }
            let _ = writeln!(
                out,
                "  verify slab {:>6}: {} objects, {} inconsistent, {:.1}s",
                s.seq,
                idx.n,
                bad,
                t0.elapsed().as_secs_f64()
            );
            if bad > 0 {
                return Err(err(format!(
                    "slab {} has {bad} inconsistent entries",
                    s.seq
                )));
            }
        }
    }
    Ok(())
}

/// `gvfs-daemon lookup`: where each OID lives.
pub fn cmd_lookup(store_dir: &Path, oids: &[Oid], out: &mut dyn Write) -> Result<(), CliError> {
    let store = FsStore::open(store_dir)?;
    let m = store
        .load_manifest()?
        .ok_or_else(|| err("store not bootstrapped"))?;
    let set = SlabSet::open(&store, m.lookup_slabs())?;
    for oid in oids {
        match set.find(oid) {
            Some((slab, loc)) => {
                let bytes = slab.entry(loc);
                let desc = match parse_entry_header(bytes) {
                    Ok(h) => match h.base {
                        DeltaBase::None => format!("{:?} size {}", h.kind, h.size),
                        DeltaBase::Ofs(d) => {
                            format!("OFS_DELTA (base at -{d}) delta size {}", h.size)
                        }
                        DeltaBase::Ref(o) => format!("REF_DELTA (base {o}) delta size {}", h.size),
                    },
                    Err(e) => format!("unparseable header: {e}"),
                };
                let _ = writeln!(
                    out,
                    "{oid}  slab {} ({})  offset {}  len {}  {desc}",
                    slab.meta.seq, slab.meta.tier, loc.offset, loc.len
                );
            }
            None => {
                let _ = writeln!(out, "{oid}  not found");
            }
        }
    }
    Ok(())
}

/// `gvfs-daemon plan`: what a client with cursor `since` would receive, as one JSON line
/// (no bytes are produced; used by replay benchmarks).
pub fn cmd_plan(
    store_dir: &Path,
    since: u64,
    compact: bool,
    out: &mut dyn Write,
) -> Result<(), CliError> {
    let store = FsStore::open(store_dir)?;
    let m = store
        .load_manifest()?
        .ok_or_else(|| err("store not bootstrapped"))?;
    let mut sel = Selection::new();
    if compact {
        m.select_compact(since, None, &mut sel);
    } else {
        m.select_for_prefetch(since, None, &mut sel);
    }
    let mut list = SlabList::new();
    let mut leaves = 0usize;
    let mut seqs = Vec::new();
    for pick in sel.as_slice() {
        if let Some(r) = m.slab(*pick) {
            if matches!(pick, crate::manifest::Pick::Leaf(..)) {
                leaves = leaves.saturating_add(1);
            }
            seqs.push(r.seq.to_string());
            list.push(Arc::new(Slab::open(&store, *r, false)?))?;
        }
    }
    if list.is_empty() {
        let _ = writeln!(out, "{{\"packs\":0,\"generation\":{}}}", m.generation);
        return Ok(());
    }
    let plan = Plan::new(list)?;
    let _ = writeln!(
        out,
        "{{\"packs\":1,\"pack_len\":{},\"idx_len\":{},\"objects\":{},\"start_ts\":{},\"end_ts\":{},\"slabs\":[{}],\"leaves\":{},\"generation\":{}}}",
        plan.pack_len,
        plan.idx_len,
        plan.object_count,
        plan.start_ts,
        plan.timestamp,
        seqs.join(","),
        leaves,
        m.generation
    );
    Ok(())
}

/// `gvfs-daemon splice`: write the pack + idx a client would get for `lastPackTimestamp`
/// (or an explicit slab list) to `<out>.pack` / `<out>.idx`.
pub fn cmd_splice(
    store_dir: &Path,
    out_prefix: &Path,
    since: Option<u64>,
    seqs: &[u64],
    out: &mut dyn Write,
) -> Result<(), CliError> {
    let store = FsStore::open(store_dir)?;
    let m = store
        .load_manifest()?
        .ok_or_else(|| err("store not bootstrapped"))?;
    let mut list = SlabList::new();
    if !seqs.is_empty() {
        for seq in seqs {
            let r = m
                .all_slabs()
                .find(|s| s.seq == *seq)
                .ok_or_else(|| err(format!("slab {seq} is not in the manifest")))?;
            list.push(Arc::new(Slab::open(&store, *r, false)?))?;
        }
    } else {
        let mut sel = Selection::new();
        m.select_for_prefetch(since.unwrap_or(0), None, &mut sel);
        for pick in sel.as_slice() {
            if let Some(r) = m.slab(*pick) {
                list.push(Arc::new(Slab::open(&store, *r, false)?))?;
            }
        }
    }
    if list.is_empty() {
        let _ = writeln!(out, "nothing to send for that timestamp");
        return Ok(());
    }
    let plan = Plan::new(list)?;
    let _ = writeln!(
        out,
        "splicing {} slabs: {} objects, pack {} B, idx {} B, timestamp {}",
        plan.slabs.len(),
        plan.object_count,
        plan.pack_len,
        plan.idx_len,
        plan.timestamp
    );
    let t0 = std::time::Instant::now();
    let mut pw =
        BufWriter::with_capacity(1 << 20, File::create(out_prefix.with_extension("pack"))?);
    let mut iw = BufWriter::with_capacity(1 << 20, File::create(out_prefix.with_extension("idx"))?);
    let (pack_sha, _) = write_splice(plan, &mut pw, &mut iw)?;
    pw.flush()?;
    iw.flush()?;
    let _ = writeln!(
        out,
        "wrote {} and .idx in {:.1}s (pack sha1 {})",
        out_prefix.with_extension("pack").display(),
        t0.elapsed().as_secs_f64(),
        Oid(pack_sha)
    );
    Ok(())
}
