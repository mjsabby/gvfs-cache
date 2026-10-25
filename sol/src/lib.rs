#![forbid(unsafe_code)]

pub mod arena;
pub mod benchmark;
pub mod cli;
pub mod codec;
pub mod error;
pub mod graph;
pub mod hfs;
pub mod idx;
pub mod ingest;
pub mod manifest;
pub mod mphf;
pub mod pipeline;
pub mod server;
pub mod slab;
pub mod splice;
pub mod storage;
pub mod tree;
pub mod upstream;

#[global_allocator]
static ALLOCATOR: gvfs_platform::TrackingAllocator = gvfs_platform::TrackingAllocator;

pub use codec::Oid;
pub use error::{Error, Result};
