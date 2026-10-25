//! The timeline: when each slab's content became visible, and which slabs answer a client.
//!
//! **Timestamps come from commits.** Every commit is stamped like a Lamport clock seeded
//! with committer times: `max(committer time, 1 + the largest stamp of its parents)`, so a
//! commit's stamp is above every ancestor's. A slab's bounds are the stamps of the commits
//! it was fetched between. Parents a slab does not hold are looked up among the stamps
//! both sides know: the manifest's boundaries, plus (on an edge) the commits of its unsealed
//! L0 slabs; any other parent is older sealed history and counts as 0. The pipeline and
//! every edge therefore stamp a commit alike whichever slabs they fetched it in: an edge's
//! L0 slab for a push and the 15m slab the pipeline later seals end at the same stamp, and a
//! client's cursor (`lastPackTimestamp`, the newest stamp it was sent) is a boundary every
//! server knows. Any push but a forced one descends from the previous tip, so its stamp is
//! above every stamp handed out before it.
//!
//! **Answers are exact covers.** A slab covers `(start_ts, end_ts]`; a rollup keeps its
//! parts' bounds, so boundaries nest like a segment tree. A client at `T` is answered with
//! the chain of slabs from `T` to the newest boundary that at each step takes the slab
//! starting exactly there that reaches furthest: nothing it already has, in as few slabs as
//! the tiers allow. A cursor no slab starts at (its boundary aged out) is answered from the
//! closest boundary before it.

use std::collections::HashMap;

use crate::object::{commit_parents, commit_time};
use crate::pack::OBJ_COMMIT;
use crate::slab::Slab;
use crate::splice::Resolver;
use crate::util::{Oid, Result};

/// The timestamp of a whole-history seed tipped by `head` (in `slab`): its committer time.
pub fn seed_ts(slab: &Slab, resolver: &mut Resolver, head: Oid) -> Result<u64> {
    let slot = slab
        .find(&head)
        .ok_or_else(|| crate::err!("seed tip {head} is not in its slab"))?;
    let (_, commit) = resolver.resolve(slab, slot)?;
    Ok(u64::try_from(commit_time(commit).unwrap_or(1))
        .unwrap_or(1)
        .max(1))
}

/// Stamps `head` and every commit of `slab` it reaches, adding them to `stamps`, which
/// holds the stamps of commits outside the slab that both the pipeline and the edges know.
/// The result is at least `floor + 1` (the previous boundary's stamp + 1), which only
/// matters for a rewritten branch.
pub fn stamp(
    slab: &Slab,
    resolver: &mut Resolver,
    head: Oid,
    stamps: &mut HashMap<Oid, u64>,
    floor: u64,
) -> Result<u64> {
    // Depth first over in-slab parents; a commit is stamped once its parents are.
    let mut stack: Vec<(Oid, bool)> = vec![(head, false)];
    let mut parents: Vec<Oid> = Vec::new();
    while let Some(&(c, expanded)) = stack.last() {
        if stamps.contains_key(&c) {
            stack.pop();
            continue;
        }
        let Some(slot) = slab.find(&c) else {
            stack.pop();
            continue;
        };
        let (kind, commit) = resolver.resolve(slab, slot)?;
        if kind != OBJ_COMMIT {
            stack.pop();
            continue;
        }
        parents.clear();
        parents.extend(commit_parents(commit));
        let time = u64::try_from(commit_time(commit).unwrap_or(0)).unwrap_or(0);
        let pending: Vec<Oid> = parents
            .iter()
            .copied()
            .filter(|p| !stamps.contains_key(p) && slab.find(p).is_some())
            .collect();
        if !expanded && !pending.is_empty() {
            if let Some(top) = stack.last_mut() {
                top.1 = true;
            }
            stack.extend(pending.into_iter().map(|p| (p, false)));
            if stack.len() > 10_000_000 {
                crate::bail!("commit graph too deep to stamp");
            }
            continue;
        }
        let ts = parents.iter().fold(time, |ts, p| {
            ts.max(stamps.get(p).copied().unwrap_or(0).saturating_add(1))
        });
        stamps.insert(c, ts);
        stack.pop();
    }
    Ok(stamps
        .get(&head)
        .copied()
        .unwrap_or(0)
        .max(floor.saturating_add(1)))
}

/// The stamps of the manifest's boundaries: every slab's tip.
pub fn boundaries(m: &crate::manifest::Manifest) -> HashMap<Oid, u64> {
    let mut known: HashMap<Oid, u64> = m
        .slabs()
        .filter(|s| !s.want.is_zero())
        .map(|s| (s.want, s.end_ts))
        .collect();
    known.insert(m.tip, m.max_ts);
    known
}

/// The chain of `spans` (`(start, end]`, sorted by start then end) that takes a client at
/// `from` to `to`, as indexes into `spans`: at each step the span starting exactly at the
/// current position that reaches furthest, or, where none starts there, the narrowest span
/// containing it. False if the spans leave a gap before `to`.
pub fn cover(spans: &[(u64, u64)], from: u64, to: u64, out: &mut Vec<usize>) -> bool {
    out.clear();
    let mut at = from;
    while at < to {
        let lo = spans.partition_point(|s| s.0 < at);
        let hi = spans.partition_point(|s| s.0 <= at);
        let end = |i: &usize| spans.get(*i).map_or(0, |s| s.1);
        let exact = (lo..hi).filter(|i| end(i) > at).max_by_key(end);
        let width = |i: &usize| spans.get(*i).map_or(u64::MAX, |s| s.1.saturating_sub(s.0));
        let Some(next) = exact.or_else(|| (0..lo).filter(|i| end(i) > at).min_by_key(width)) else {
            return false;
        };
        out.push(next);
        at = end(&next);
    }
    true
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use super::*;
    use crate::ingest::{Indexer, memory_slab};
    use crate::slab::Tier;
    use std::process::{Command, Stdio};

    #[test]
    fn covers_are_exact_and_minimal() {
        // 15m slabs every 10 units, an hour rollup over the first four, a retained day.
        let mut spans: Vec<(u64, u64)> = (0..8).map(|i| (i * 10, i * 10 + 10)).collect();
        spans.push((0, 40));
        spans.push((40, 80));
        spans.sort();
        let mut out = Vec::new();
        let ends = |out: &[usize]| out.iter().map(|&i| spans[i]).collect::<Vec<_>>();
        // From a boundary: the furthest-reaching slab at each step.
        assert!(cover(&spans, 0, 80, &mut out));
        assert_eq!(ends(&out), vec![(0, 40), (40, 80)]);
        assert!(cover(&spans, 30, 80, &mut out));
        assert_eq!(ends(&out), vec![(30, 40), (40, 80)]);
        assert!(cover(&spans, 50, 70, &mut out));
        assert_eq!(ends(&out), vec![(50, 60), (60, 70)]);
        // Inside a slab: the narrowest one containing the cursor.
        assert!(cover(&spans, 35, 80, &mut out));
        assert_eq!(ends(&out), vec![(30, 40), (40, 80)]);
        // Up to date, and a gap.
        assert!(cover(&spans, 80, 80, &mut out) && out.is_empty());
        let gappy = vec![(0, 10), (20, 30)];
        assert!(!cover(&gappy, 0, 30, &mut out));
    }

    fn git(dir: &std::path::Path, args: &[&str], env: &[(&str, String)], stdin: &[u8]) -> String {
        let mut cmd = Command::new("git");
        cmd.args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null");
        crate::upstream::hermetic_git_env(&mut cmd);
        for (k, v) in env {
            cmd.env(k, v);
        }
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        std::io::Write::write_all(&mut child.stdin.take().unwrap(), stdin).unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success(), "git {args:?}");
        String::from_utf8(out.stdout).unwrap()
    }

    /// A scratch repository with commits at chosen committer times.
    struct Repo {
        dir: std::path::PathBuf,
    }

    impl Repo {
        fn new(name: &str) -> Repo {
            let dir =
                std::env::temp_dir().join(format!("gvd-timeline-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            git(&dir, &["init", "-q", "-b", "main"], &[], b"");
            Repo { dir }
        }

        /// A commit at `time` with the given parents (the first one is checked out).
        fn commit(&self, time: u64, parents: &[Oid], file: &str) -> Oid {
            if let Some(p) = parents.first() {
                git(
                    &self.dir,
                    &["checkout", "-q", "--detach", &p.to_string()],
                    &[],
                    b"",
                );
            }
            std::fs::write(self.dir.join(file), format!("{time}")).unwrap();
            git(&self.dir, &["add", "-A"], &[], b"");
            let tree = git(&self.dir, &["write-tree"], &[], b"");
            let mut args = vec![
                "commit-tree".to_string(),
                tree.trim().to_string(),
                "-m".into(),
                "c".into(),
            ];
            for p in parents {
                args.push("-p".into());
                args.push(p.to_string());
            }
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            let date = format!("{time} +0000");
            let env = [
                ("GIT_COMMITTER_DATE", date.clone()),
                ("GIT_AUTHOR_DATE", date),
                ("GIT_AUTHOR_NAME", "t".into()),
                ("GIT_AUTHOR_EMAIL", "t@t".into()),
                ("GIT_COMMITTER_NAME", "t".into()),
                ("GIT_COMMITTER_EMAIL", "t@t".into()),
            ];
            Oid::from_hex(git(&self.dir, &args, &env, b"").trim().as_bytes()).unwrap()
        }

        /// The slab a fetch of `want` with `haves` would make.
        fn slab(&self, ix: &mut Indexer, haves: &[Oid], want: Oid) -> std::sync::Arc<Slab> {
            let mut revs = format!("{want}\n");
            for h in haves {
                revs.push_str(&format!("^{h}\n"));
            }
            let mut cmd = Command::new("git");
            cmd.args(["pack-objects", "--revs", "--stdout", "-q"])
                .current_dir(&self.dir);
            crate::upstream::hermetic_git_env(&mut cmd);
            let mut child = cmd
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            std::io::Write::write_all(&mut child.stdin.take().unwrap(), revs.as_bytes()).unwrap();
            memory_slab(
                ix,
                child.wait_with_output().unwrap().stdout,
                Tier::M15,
                (0, 1),
                (Oid::ZERO, Oid::ZERO),
            )
            .unwrap()
            .slab
        }
    }

    impl Drop for Repo {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[test]
    fn stamps_do_not_depend_on_where_history_is_cut() {
        let repo = Repo::new("cut");
        // Committer times: increasing, two equal, one in the past.
        let b = 1_700_000_000u64;
        let mut ids = vec![repo.commit(b, &[], "f")];
        for t in [b + 1000, b + 1000, b + 500, b + 2000] {
            let parent = *ids.last().unwrap();
            ids.push(repo.commit(t, &[parent], "f"));
        }
        let (mut ix, mut r) = (Indexer::new(1), Resolver::default());
        let seed = repo.slab(&mut ix, &[], ids[0]);
        let t0 = seed_ts(&seed, &mut r, ids[0]).unwrap();
        assert_eq!(t0, b);
        // One slab over commits 1..4 (the pipeline), against one per commit (an edge).
        let mut sealed = HashMap::from([(ids[0], t0)]);
        let whole = stamp(
            &repo.slab(&mut ix, &[ids[0]], ids[4]),
            &mut r,
            ids[4],
            &mut sealed,
            t0,
        )
        .unwrap();
        let mut edge = HashMap::from([(ids[0], t0)]);
        let mut ts = t0;
        for i in 1..5 {
            ts = stamp(
                &repo.slab(&mut ix, &[ids[i - 1]], ids[i]),
                &mut r,
                ids[i],
                &mut edge,
                ts,
            )
            .unwrap();
            let expect = [0, b + 1000, b + 1001, b + 1002, b + 2000][i];
            assert_eq!(
                ts, expect,
                "commit {i}: equal times step by a second, older times never go back"
            );
        }
        assert_eq!((whole, ts), (b + 2000, b + 2000));
    }

    /// A pushed merge whose first parent is not the previous tip (`git pull`, then push).
    /// The edge saw the previous tip A on its own; the pipeline sees M..B in one slab. Both
    /// stamp B alike, above A.
    #[test]
    fn merges_are_stamped_above_every_parent() {
        let repo = Repo::new("merge");
        let b = 1_700_000_000u64;
        let m = repo.commit(b, &[], "f");
        let a = repo.commit(b + 100, &[m], "a");
        // L was committed on another machine with a clock behind A's.
        let l = repo.commit(b + 50, &[m], "l");
        let merge = repo.commit(b + 90, &[l, a], "l");
        let (mut ix, mut r) = (Indexer::new(1), Resolver::default());
        // Edge: A on its own, then A..B.
        let mut edge = HashMap::from([(m, b)]);
        let ts_a = stamp(&repo.slab(&mut ix, &[m], a), &mut r, a, &mut edge, b).unwrap();
        let ts_b = stamp(
            &repo.slab(&mut ix, &[a, m], merge),
            &mut r,
            merge,
            &mut edge,
            ts_a,
        )
        .unwrap();
        // Pipeline: M..B at once.
        let mut sealed = HashMap::from([(m, b)]);
        let sealed_b = stamp(
            &repo.slab(&mut ix, &[m], merge),
            &mut r,
            merge,
            &mut sealed,
            b,
        )
        .unwrap();
        assert_eq!(ts_a, b + 100);
        assert_eq!((ts_b, sealed_b), (b + 101, b + 101));
    }
}
