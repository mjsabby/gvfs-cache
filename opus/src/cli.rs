//! Command-line parsing: `gvfs-daemon <command> [--flag value]...`.
//!
//! Deliberately tiny (no framework): every command lists the flags it accepts, unknown
//! flags are errors, and values are parsed where they are used.

use crate::util::Result;

pub const USAGE: &str = "\
gvfs-daemon - unified GVFS monorepo acceleration layer (one binary, two primitives)

USAGE:
    gvfs-daemon pipeline --store <SPEC> --upstream <URL|PATH> --tier <TIER> [OPTIONS]
        Scheduled job (ADO pipeline / GitHub Action, every 15 minutes; runners keep no
        state): fetch the commits since the manifest's tip (the only time the upstream
        is asked for them), repack them into a 15m slab, then roll up every tier up to
        TIER by repacking slabs from the store. With no manifest yet: the one-time seed.
        TIER: 15m | 1h | 6h | 12h | 24h | 168h  (168h = weekly delta + new baseline)

    gvfs-daemon mirror --store <SPEC> --upstream <URL|PATH> [OPTIONS]
        Copies every blob of the history into a --blobs separate store, resumably:
        planned from the store's own trees and sizes tables (the tip's tree first),
        skipping blobs already copied, verified by id and size as they stream in,
        published to blobs.bin as it goes. When the last blob is in, blobs.bin is
        complete and edges serve. Run again at any time: it copies only what is missing.

    gvfs-daemon serve --store <SPEC> [--upstream <URL|PATH>] [--bind <ADDR>] [OPTIONS]
        GVFS server. Remote edge: --bind 0.0.0.0:8080. Localhost proxy: --bind
        127.0.0.1:8080 (the default). With --upstream it bridges the minutes since the
        last sealed slab: it polls the branch, fetches each new commit once, and keeps
        it as an L0 slab in RAM (one per push). Without it, it serves sealed slabs only.
        Every prefetch is one pack of exactly what the client lacks.

    gvfs-daemon ingest --pack <FILE> --out <DIR> [--threads N]
    gvfs-daemon ingest --upstream <URL|PATH> --want <OID|REF> [--have <OID>] --out <DIR>
        Run ingest_git_delta once and write <DIR>/<id>.pack + <id>.mphf (debugging,
        benchmarks).

    gvfs-daemon repack --out <DIR> [--frozen <FILE.mphf>] [--mode trees|copy] <FILE.mphf>...
        The optimal splice by hand: slabs (oldest first) become one slab with trees
        re-deltified by directory (--mode copy: deltas re-pointed only). --frozen appends
        to that slab, kept byte for byte, as the weekly baseline rebuild does.

    gvfs-daemon inspect manifest --store <SPEC>
    gvfs-daemon inspect slab <FILE.mphf> [--threads N] [--write-idx <FILE.idx>]
        The slab index header; with the .pack beside it, object counts, delta depths and
        GitHFS pointer coverage (how much content the pointers stand for). --write-idx
        writes the git .idx a VFS for Git client would be sent with this pack.

STORES (--store):
    fs:<DIR> or <DIR>          a directory (local disk or a shared filesystem)
    azure:<CONTAINER>[/<PREFIX>]   Azure Blob Storage; credentials from the environment
                               variable named by --store-auth-env (default
                               AZURE_STORAGE_CONNECTION_STRING): account key or SAS

UPSTREAM (--upstream):
    https://dev.azure.com/<org>/<project>/_git/<repo>  (or any smart-HTTP git server)
    <PATH>                     a local repository (runs git upload-pack)
    --branch <NAME>            branch to follow (default: main)
    --token-env <VAR>          env var with a PAT / token, sent as HTTP Basic auth
    --token-user <USER>        Basic auth user for --token-env (default: empty)
    --auth-header-env <VAR>    env var with a full Authorization header value

PIPELINE OPTIONS:
    --work-dir <DIR>           scratch space for slab files (default: system temp)
    --now <UNIX-SECONDS>       override the clock (tests)
    --keep-weeks <N>           weekly deltas kept for warm clients (default: 8)
    --gc-grace-secs <N>        orphaned slab files older than this are deleted (default: 7200)
    --threads <N>              indexing threads (default: all cores)
    --blobs <inline|separate>  at genesis: keep blobs in the slabs (inline, the default)
                               or every blob, real content, in blob packs with sizes
                               tables beside the slabs (separate); later runs follow
                               the store
    --blob-pack-mib <N>        small blobs per blob pack (default: 2048); blobs of 16 MiB
                               or more always get a pack of their own, streamed
    --cache-dir <DIR>          separate stores: store objects a run downloads (sizes
                               tables, segment indexes), kept for the next run and pruned
                               to what the store lists (default:
                               <work-dir>/gvfs-daemon-store-cache)

MIRROR OPTIONS:
    --jobs <N>                 parallel copies, one upstream connection each (default: 4)
    --blob-pack-mib <N>        as for the pipeline
    --publish-secs <N>         publish finished packs at least this often (default: 60)
    --rate-mbps <N>            cap the combined download rate (megabits per second)
    --max-items <N>            stop after N packs (seed in slices; run again to continue)
    --work-dir, --threads      as for the pipeline

SERVE OPTIONS:
    --bind <ADDR>              listen address (default: 127.0.0.1:8080)
    --cache-dir <DIR>          local copies of remote slabs (default: system temp)
    --poll-ms <N>              upstream + manifest poll interval (default: 2000)
    --workers <N>              connection threads (default: 256)
    --arenas <N>               request scratch arenas (default: 2 x cores)
    --arena-mib <N>            size of each arena (default: 8)
    --cache-server <NAME=URL>  advertised in /gvfs/config (repeatable)
    --alloc-guard <count|strict|trace>   hot-path allocation policy (default: count)
    --prefetch-idx <auto|always|never>   send each prefetch pack's .idx; auto (default):
                               to every client except git itself (gvfs-helper discards it)
    --proxy-cache-mib <N>      --blobs separate: blobs proxied from the upstream kept on
                               local disk (default: 10240)
    --blob-cache-mib <N>       --blobs separate: 4 MiB blocks of blob packs in object
                               storage kept on local disk (default: 32768)
    --superset-tier <24h|168h|never>     send each sealed day and week (24h, default) or
                               week (168h) to a warm client whole, once, so that client
                               maintenance can expire its smaller packs; never: exact covers

ENVIRONMENT:
    GVFS_DEBUG_INGEST=1        log pack indexing phase timings and entry statistics
";

/// Parsed `--flag value` pairs of one command.
pub struct Flags {
    pairs: Vec<(String, String)>,
    pub positional: Vec<String>,
}

impl Flags {
    /// Parses `args`, accepting only the flags in `allowed`.
    pub fn parse(args: &[String], allowed: &[&str]) -> Result<Flags> {
        let mut pairs = Vec::new();
        let mut positional = Vec::new();
        let mut it = args.iter();
        while let Some(a) = it.next() {
            if let Some(name) = a.strip_prefix("--") {
                let (name, value) = match name.split_once('=') {
                    Some((n, v)) => (n.to_string(), v.to_string()),
                    None => (
                        name.to_string(),
                        it.next()
                            .cloned()
                            .ok_or_else(|| crate::err!("--{name} needs a value"))?,
                    ),
                };
                if !allowed.contains(&name.as_str()) {
                    crate::bail!("unknown option --{name}");
                }
                pairs.push((name, value));
            } else {
                positional.push(a.clone());
            }
        }
        Ok(Flags { pairs, positional })
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.pairs
            .iter()
            .rev()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    pub fn all(&self, name: &str) -> impl Iterator<Item = &str> {
        self.pairs
            .iter()
            .filter(move |(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    pub fn require(&self, name: &str) -> Result<&str> {
        self.get(name)
            .ok_or_else(|| crate::err!("--{name} is required"))
    }

    /// Parses an optional value, with a default.
    pub fn num<T: std::str::FromStr>(&self, name: &str, default: T) -> Result<T> {
        match self.get(name) {
            None => Ok(default),
            Some(v) => v
                .parse()
                .map_err(|_| crate::err!("--{name}: invalid value {v:?}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn parses_pairs_and_rejects_unknown() {
        let f = Flags::parse(&s(&["--a", "1", "--b=two", "pos", "--a", "3"]), &["a", "b"]).unwrap();
        assert_eq!(f.get("a"), Some("3"));
        assert_eq!(f.all("a").collect::<Vec<_>>(), vec!["1", "3"]);
        assert_eq!(f.get("b"), Some("two"));
        assert_eq!(f.positional, vec!["pos"]);
        assert_eq!(f.num("a", 0u32).unwrap(), 3);
        assert!(f.num::<u32>("b", 0).is_err());
        assert!(Flags::parse(&s(&["--c", "1"]), &["a"]).is_err());
        assert!(Flags::parse(&s(&["--a"]), &["a"]).is_err());
    }
}
