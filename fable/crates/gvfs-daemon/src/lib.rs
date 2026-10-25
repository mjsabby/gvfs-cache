//! `gvfs-daemon`: the unified GVFS monorepo acceleration layer.
#![forbid(unsafe_code)]

pub mod arena;
pub mod cli;
pub mod client;
pub mod delta;
pub mod frames;
pub mod hfs;
pub mod idx;
pub mod indexer;
pub mod ingest;
pub mod manifest;
pub mod mphf;
pub mod oid;
pub mod pack;
pub mod pipeline;
pub mod pktline;
pub mod serve;
pub mod slab;
pub mod splice;
pub mod store;
pub mod transport;
pub mod zlib;

#[cfg(test)]
pub mod testutil;
