//! Client traffic on a simulated clock: what clients receive from the edge, against the
//! ideal.
//!
//! Replays a repository's first-parent history (commit times as push times) through the
//! real pipeline (a run every 15 minutes, tiers as in the README's cron script), and
//! through the edge's live-tip logic (`serve::LiveTip`: L0 on every push). Each client
//! pattern sends its prefetches through the server's own plan selection
//! (`View::prefetch`). Per pattern it reports prefetches, pack files received (one per
//! response with news), slabs spliced into them, and bytes. The ideal is one self-contained
//! upstream pack of exactly the objects the client lacks (a non-thin `fetch` from the
//! client's tip). That is the least a GVFS client can be sent, since prefetch packs are
//! indexed on their own.
//!
//! ```sh
//! cargo run --release --example clients -- --repo ~/SearchGold --from 2026-10-10 \
//!     --days 14 --work /path/to/scratch 2>clients.log
//! ```
//!
//! `--superset-tier 24h|168h|never` as for `serve` (default 24h).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use gvfs_daemon::manifest::{MANIFEST_KEY, Manifest};
use gvfs_daemon::pipeline;
use gvfs_daemon::serve::view::{Prefetch, View};
use gvfs_daemon::serve::{LiveTip, retain_l0};
use gvfs_daemon::slab::{Slab, Tier};
use gvfs_daemon::store::Store;
use gvfs_daemon::upstream::{Auth, Upstream};
use gvfs_daemon::util::Oid;

const DAY: u64 = 86_400;
/// Bytes of an empty `GPRE` response, and the framing of a one-pack response.
const EMPTY: u64 = 8;
const FRAMING: u64 = 8 + 24;

/// Days since 1970-01-01 of a proleptic Gregorian date.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn parse_date(s: &str) -> u64 {
    let p: Vec<i64> = s.split('-').map(|x| x.parse().unwrap()).collect();
    (days_from_civil(p[0], p[1], p[2]) as u64) * DAY
}

/// 0 = Sunday.
fn weekday(t: u64) -> u64 {
    (t / DAY + 4) % 7
}

/// The tier the README's cron script runs at `t` (UTC).
fn tier_at(t: u64) -> Tier {
    let (m, h) = ((t / 60) % 60, (t / 3600) % 24);
    match (m, h) {
        (0, 0) if weekday(t) == 0 => Tier::Week,
        (0, 0) => Tier::H24,
        (0, 12) => Tier::H12,
        (0, h) if h % 6 == 0 => Tier::H6,
        (0, _) => Tier::H1,
        _ => Tier::M15,
    }
}

fn git(dir: &Path, args: &[&str]) -> String {
    let mut cmd = Command::new("git");
    cmd.args(args).current_dir(dir);
    gvfs_daemon::upstream::hermetic_git_env(&mut cmd);
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

/// The upstream: a bare repository borrowing the real one's objects, whose `main` is moved
/// commit by commit as the clock advances.
struct Origin {
    bare: PathBuf,
}

impl Origin {
    fn new(work: &Path, repo: &Path) -> Origin {
        let bare = work.join("upstream.git");
        git(
            work,
            &["init", "-q", "--bare", "-b", "main", bare.to_str().unwrap()],
        );
        let objects = if repo.join(".git").exists() {
            repo.join(".git/objects")
        } else {
            repo.join("objects")
        };
        std::fs::write(
            bare.join("objects/info/alternates"),
            format!("{}\n", objects.display()),
        )
        .unwrap();
        Origin { bare }
    }

    fn push(&self, commit: &Oid) {
        std::fs::write(self.bare.join("refs/heads/main"), format!("{commit}\n")).unwrap();
    }

    fn open(&self) -> Upstream {
        Upstream::open(self.bare.to_str().unwrap(), "main", &Auth::default()).unwrap()
    }
}

/// The edge: sealed slabs from the store plus the L0 slab, as `serve` keeps them.
struct Edge {
    store: Store,
    up: Upstream,
    live: LiveTip,
    opened: HashMap<String, Arc<Slab>>,
    manifest: Option<Manifest>,
    sealed: Vec<Arc<Slab>>,
    l0: Vec<Arc<Slab>>,
    view: Option<Arc<View>>,
    generation: u64,
    l0_updates: u64,
    supersets: Option<Tier>,
}

impl Edge {
    /// A new manifest: swap in its slabs, prune L0.
    fn reload(&mut self) {
        let (bytes, etag) = self.store.get(MANIFEST_KEY).unwrap().unwrap();
        let m = Manifest::decode(&bytes).unwrap();
        if self
            .manifest
            .as_ref()
            .is_some_and(|old| old.generation == m.generation)
        {
            return;
        }
        let mut opened = HashMap::new();
        self.sealed = m
            .slabs()
            .map(|meta| {
                let s = self.opened.get(&meta.id).cloned().unwrap_or_else(|| {
                    let pack = self.store.local_path(&meta.pack_key()).unwrap();
                    let index = self.store.local_path(&meta.index_key()).unwrap();
                    Slab::open(&pack, &index, meta.clone()).unwrap()
                });
                opened.insert(meta.id.clone(), s.clone());
                s
            })
            .collect();
        self.opened = opened;
        if let Some(old) = &self.manifest {
            self.l0 = self.live.prune(&self.l0, old, &m, &self.sealed).unwrap();
        }
        self.manifest = Some(m);
        self.publish(etag);
    }

    /// The upstream branch moved to `head` at `t`.
    fn push(&mut self, head: Oid) {
        let (Some(m), Some(v)) = (&self.manifest, &self.view) else {
            return;
        };
        if v.tip == head {
            return;
        }
        if let Some(l0) = self.live.advance(&mut self.up, m, &self.l0, head).unwrap() {
            self.l0.push(l0);
            retain_l0(&mut self.l0, m.max_ts);
            self.l0_updates += 1;
            let etag = self
                .view
                .as_ref()
                .map(|v| v.etag.clone())
                .unwrap_or_default();
            self.publish(etag);
        }
    }

    fn publish(&mut self, etag: String) {
        self.generation += 1;
        let m = self.manifest.clone().unwrap();
        self.view = Some(Arc::new(View::new(
            self.generation,
            m,
            etag,
            self.sealed.clone(),
            self.l0.clone(),
            self.supersets,
        )));
    }
}

/// What a client pattern received (warm clients: after their initial clone).
#[derive(Default, Clone)]
struct Tally {
    requests: u64,
    packs: u64,
    bytes: u64,
    ideal: u64,
    slabs_max: usize,
    slabs_sum: u64,
    /// Per request: (bytes, ideal, slabs).
    each: Vec<(u64, u64, usize)>,
}

struct Client {
    name: String,
    /// Request times, ascending.
    times: Vec<u64>,
    /// Every request is a fresh clone (stateless CI agents).
    stateless: bool,
    cursor: u64,
    tip: Option<Oid>,
    tally: Tally,
}

/// Exactly the objects a client at `have` lacks for `want`, as one non-thin upstream pack.
struct Ideal {
    up: Upstream,
    memo: HashMap<(Oid, Oid), u64>,
    /// The upstream's full pack at the seed, and the seed commit.
    seed: (u64, Oid),
}

impl Ideal {
    fn bytes(&mut self, have: Option<Oid>, want: Oid) -> u64 {
        match have {
            Some(h) if h == want => EMPTY,
            Some(h) => FRAMING + self.fetch(h, want),
            // A full clone: the seed's pack plus what came since, in one pack.
            None => FRAMING + self.seed.0 + self.fetch(self.seed.1, want).saturating_sub(32),
        }
    }

    fn fetch(&mut self, have: Oid, want: Oid) -> u64 {
        if have == want {
            return 0;
        }
        if let Some(&n) = self.memo.get(&(have, want)) {
            return n;
        }
        let mut pack = Vec::new();
        let n = self.up.fetch(&[have], &want, &mut pack).unwrap();
        self.memo.insert((have, want), n);
        n
    }
}

fn every(t0: u64, t1: u64, period: u64, offset: u64) -> Vec<u64> {
    (0..)
        .map(|k| t0 + offset + k * period)
        .take_while(|&t| t < t1)
        .collect()
}

/// Weekdays (Mon-Fri) at the given UTC times of day, skipping days `[skip.0, skip.1)`.
fn weekdays(t0: u64, t1: u64, at: &[u64], skip: (u64, u64)) -> Vec<u64> {
    let mut v = Vec::new();
    for day in 0..(t1 - t0) / DAY {
        let d0 = t0 + day * DAY;
        if (1..=5).contains(&weekday(d0)) && !(skip.0..skip.1).contains(&day) {
            v.extend(at.iter().map(|s| d0 + s));
        }
    }
    v
}

fn mib(b: u64) -> String {
    format!("{:.1}", b as f64 / 1048576.0)
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let f = gvfs_daemon::cli::Flags::parse(
        &args,
        &[
            "repo",
            "from",
            "days",
            "work",
            "branch",
            "threads",
            "superset-tier",
        ],
    )
    .unwrap();
    let repo = PathBuf::from(f.require("repo").unwrap());
    let t0 = parse_date(f.require("from").unwrap());
    let days: u64 = f.num("days", 14).unwrap();
    let t1 = t0 + days * DAY;
    let work = PathBuf::from(f.require("work").unwrap());
    let branch = f.get("branch").unwrap_or("HEAD").to_string();
    let threads: usize = f
        .num("threads", gvfs_daemon::util::default_threads())
        .unwrap();
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(work.join("pipeline")).unwrap();
    let started = Instant::now();

    // First-parent history: (push time, commit), times made monotonic.
    let log = git(
        &repo,
        &["log", "--first-parent", "--format=%H %ct", &branch],
    );
    let mut all: Vec<(u64, Oid)> = log
        .lines()
        .rev()
        .map(|l| {
            let (h, t) = l.split_once(' ').unwrap();
            (t.parse().unwrap(), Oid::from_hex(h.as_bytes()).unwrap())
        })
        .collect();
    let mut last = 0;
    for c in &mut all {
        c.0 = c.0.max(last);
        last = c.0;
    }
    let first = all
        .iter()
        .position(|c| c.0 >= t0)
        .expect("no commits after --from");
    let seed = all[first - 1].1;
    let commits: Vec<(u64, Oid)> = all[first..]
        .iter()
        .copied()
        .take_while(|c| c.0 < t1)
        .collect();
    eprintln!(
        "simulating {} commits over {days} days from {}",
        commits.len(),
        f.get("from").unwrap()
    );

    let origin = Origin::new(&work, &repo);
    origin.push(&seed);
    let store_spec = format!("fs:{}", work.join("store").display());
    let mut opts = pipeline::Options {
        store: Store::open(&store_spec, "").unwrap(),
        upstream: origin.open(),
        tier: Tier::M15,
        work_dir: work.join("pipeline"),
        now: t0,
        keep_weeks: 8,
        gc_grace: 7200,
        threads,
        blobs: None,
        blob_pack_bytes: gvfs_daemon::separate::ITEM_BYTES,
    };
    let mut edge = Edge {
        store: Store::open(&store_spec, "").unwrap(),
        up: origin.open(),
        live: LiveTip::default(),
        opened: HashMap::new(),
        manifest: None,
        sealed: Vec::new(),
        l0: Vec::new(),
        view: None,
        generation: 0,
        l0_updates: 0,
        supersets: match f.get("superset-tier").unwrap_or("24h") {
            "24h" => Some(Tier::H24),
            "168h" => Some(Tier::Week),
            _ => None,
        },
    };

    let h = 3600;
    let client = |name: &str, times: Vec<u64>, stateless: bool| Client {
        name: name.into(),
        times,
        stateless,
        cursor: 0,
        tip: None,
        tally: Tally::default(),
    };
    let mut clients = [
        client(
            "CI agents: fresh clone every 30 min, 24x7",
            every(t0, t1, 1800, 7 * 60),
            true,
        ),
        client(
            "agent VM: prefetch every minute, 24x7",
            every(t0, t1, 60, 30),
            false,
        ),
        client(
            "agent VM: prefetch every 15 min, 24x7",
            every(t0, t1, 900, 450),
            false,
        ),
        client(
            "agent VM: hourly maintenance prefetch, 24x7",
            every(t0, t1, h, h / 2),
            false,
        ),
        client(
            "dev laptop: hourly maintenance 08:00-18:00 weekdays",
            weekdays(
                t0,
                t1,
                &(8..=18).map(|x| x * h + h / 4).collect::<Vec<_>>(),
                (0, 0),
            ),
            false,
        ),
        client(
            "developer: fetch 09:00, 13:00, 17:30 weekdays",
            weekdays(t0, t1, &[9 * h, 13 * h, 17 * h + h / 2], (0, 0)),
            false,
        ),
        client(
            "developer: fetch each weekday morning",
            weekdays(t0, t1, &[9 * h], (0, 0)),
            false,
        ),
        client(
            "developer: back after 8 days off",
            weekdays(t0, t1, &[9 * h], (3, 11)),
            false,
        ),
    ];
    let mut first_clone: Vec<Option<(u64, u64)>> = vec![None; clients.len()];

    // Events in time order; at equal times pushes come first, then the pipeline, then clients.
    let mut events: Vec<(u64, u8, usize)> = Vec::new();
    events.extend(commits.iter().enumerate().map(|(i, c)| (c.0, 0, i)));
    events.extend(every(t0, t1, 900, 0).into_iter().map(|t| (t, 1, 0)));
    for (ci, c) in clients.iter().enumerate() {
        events.extend(c.times.iter().map(|&t| (t, 2, ci)));
    }
    events.sort();

    // CLIENTS_TRACE=<substring of a client name>: one line per prefetch of that client.
    let trace = std::env::var("CLIENTS_TRACE").ok();
    let mut ideal: Option<Ideal> = None;
    let mut tiers: HashMap<&'static str, u64> = HashMap::new();
    let mut cold_slabs_max = 0usize;
    for (t, kind, i) in events {
        match kind {
            0 => {
                let c = commits[i].1;
                origin.push(&c);
                edge.push(c);
            }
            1 => {
                opts.now = t;
                opts.tier = tier_at(t);
                pipeline::run(&mut opts).unwrap();
                *tiers.entry(opts.tier.name()).or_default() += 1;
                edge.reload();
                if ideal.is_none() {
                    let m = edge.manifest.as_ref().unwrap();
                    ideal = Some(Ideal {
                        up: origin.open(),
                        memo: HashMap::new(),
                        seed: (m.baseline.pack_len, m.tip),
                    });
                    eprintln!("seeded in {:.0}s", started.elapsed().as_secs_f64());
                }
            }
            _ => {
                let view = edge.view.clone().unwrap();
                let ideal = ideal.as_mut().unwrap();
                let c = &mut clients[i];
                if c.stateless {
                    c.cursor = 0;
                    c.tip = None;
                }
                let want = ideal.bytes(c.tip, view.tip);
                let cursor = c.cursor;
                let mut traced = String::new();
                let (bytes, slabs) = match view.prefetch(c.cursor) {
                    Prefetch::UpToDate => (EMPTY, 0),
                    Prefetch::Pack(plan, _) => {
                        c.cursor = plan.timestamp;
                        for s in &plan.slabs {
                            traced.push_str(&format!(
                                " [{} ({}, {}] {}..{} {}B]",
                                s.meta.tier.name(),
                                s.meta.start_ts,
                                s.meta.end_ts,
                                s.meta.have,
                                s.meta.want,
                                s.pack().len()
                            ));
                        }
                        (FRAMING + plan.len, plan.sources)
                    }
                };
                if trace.as_deref().is_some_and(|t| c.name.contains(t)) {
                    println!(
                        "trace t={t} dow={} cursor={cursor} client_tip={:?} bytes={bytes} ideal={want} sources={slabs}{traced}",
                        weekday(t),
                        c.tip.map(|o| o.to_string())
                    );
                }
                if c.stateless {
                    cold_slabs_max = cold_slabs_max.max(slabs);
                }
                let initial = c.tip.is_none() && !c.stateless;
                c.tip = Some(view.tip);
                if initial {
                    first_clone[i] = Some((bytes, want));
                    continue;
                }
                let tl = &mut c.tally;
                tl.requests += 1;
                tl.packs += u64::from(slabs > 0);
                tl.bytes += bytes;
                tl.ideal += want;
                tl.slabs_max = tl.slabs_max.max(slabs);
                tl.slabs_sum += slabs as u64;
                tl.each.push((bytes, want, slabs));
            }
        }
    }
    eprintln!(
        "simulated in {:.0}s: {} pipeline runs {:?}, {} L0 updates",
        started.elapsed().as_secs_f64(),
        tiers.values().sum::<u64>(),
        tiers,
        edge.l0_updates
    );

    println!(
        "\n{days} days of {} from {} ({} commits)\n",
        repo.display(),
        f.get("from").unwrap(),
        commits.len()
    );
    println!(
        "Warm clients, after their initial clone (one pack each, about 2.4 GiB, 1.00x ideal):\n"
    );
    println!(
        "| Client | prefetches | packs received | slabs merged per pack (avg / max) | MiB received | MiB ideal | x ideal |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|");
    for c in clients.iter().skip(1) {
        let t = &c.tally;
        let avg = if t.packs == 0 {
            0.0
        } else {
            t.slabs_sum as f64 / t.packs as f64
        };
        println!(
            "| {} | {} | {} | {avg:.1} / {} | {} | {} | {:.2} |",
            c.name,
            t.requests,
            t.packs,
            t.slabs_max,
            mib(t.bytes),
            mib(t.ideal),
            t.bytes as f64 / t.ideal.max(1) as f64
        );
    }
    for (c, first) in clients.iter().zip(&first_clone).skip(1) {
        if let Some((b, w)) = first {
            assert!(
                *b as f64 <= *w as f64 * 1.01,
                "{}: initial clone {b} vs ideal {w}",
                c.name
            );
        }
    }
    let ci = &clients[0].tally;
    let mut b: Vec<u64> = ci.each.iter().map(|e| e.0).collect();
    b.sort();
    let over: Vec<f64> = ci.each.iter().map(|e| e.0 as f64 - e.1 as f64).collect();
    let slabs: Vec<usize> = ci.each.iter().map(|e| e.2).collect();
    println!(
        "\nCold clones ({}): {} clones, {} packs (one each) made of {:.1} slabs on average, {} at most; {}-{} MiB each; over a fresh full fetch: {:.2} MiB on average, {:.2} MiB at most ({:.4}x)",
        clients[0].name,
        ci.requests,
        ci.packs,
        slabs.iter().sum::<usize>() as f64 / slabs.len() as f64,
        cold_slabs_max,
        mib(b[0]),
        mib(*b.last().unwrap()),
        over.iter().sum::<f64>() / over.len() as f64 / 1048576.0,
        over.iter().cloned().fold(f64::MIN, f64::max) / 1048576.0,
        ci.bytes as f64 / ci.ideal as f64
    );
}
