//! `gvfs-daemon`: the unified GVFS monorepo acceleration layer.
//!
//! One binary, two primitives:
//!
//! * [`ingest::ingest_git_delta`] — one `have`/`want` fetch from the upstream git server
//!   becomes a *slab*: a pack plus a `.mphf` index (minimal perfect hash + fixed 40-byte
//!   object records). The upstream is asked for every commit once.
//! * splice — slabs become ONE pack. On the wire ([`splice`]) slab bodies are concatenated
//!   verbatim between a fresh 12-byte header and a SHA-1 trailer. Into a new slab
//!   ([`repack`], the optimal splice) trees are re-deltified by directory, so a rollup is
//!   as small as one upstream fetch of its whole range.
//!
//! The scheduled `pipeline` uses them to seal 15-minute slabs and roll them up through the
//! 1h / 6h / 12h / 24h / 168h pyramid in object storage; `serve` (remote edge or localhost
//! proxy, same code) uses them to answer the GVFS protocol, keeping the last few minutes in
//! RAM as one L0 slab built by the very same `ingest_git_delta` and `repack`.
#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

pub mod util;

pub mod b64;
pub mod http;
pub mod pktline;
pub mod upstream;

pub mod arena;
pub mod azure;
pub mod blobcopy;
pub mod blobs;
pub mod blobstream;
pub mod manifest;
pub mod store;

pub mod cli;
pub mod fsutil;

pub mod delta;
pub mod hfs;
pub mod idx;
pub mod ingest;
pub mod inspect;
pub mod mphf;
pub mod object;
pub mod pack;
pub mod pipeline;
pub mod repack;
pub mod separate;
pub mod serve;
pub mod sizes;
pub mod slab;
pub mod splice;
pub mod timeline;
pub mod zlib;
