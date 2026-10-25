//! `gvfs-daemon` binary entry point: parse the command, dispatch, report.
#![forbid(unsafe_code)]

use gvfs_alloc_guard::SealedAllocator;
use gvfs_daemon::{cli, ingest, inspect, pipeline, repack, serve};

#[global_allocator]
static GLOBAL: SealedAllocator = SealedAllocator;

fn main() -> std::process::ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let Some((command, rest)) = argv.split_first() else {
        eprint!("{}", cli::USAGE);
        return std::process::ExitCode::from(2);
    };
    let result = match command.as_str() {
        "pipeline" => pipeline::main(rest),
        // `edge` and `proxy` are the same server; the bind address is the only difference.
        "serve" | "edge" | "proxy" => serve::main(rest),
        "ingest" => ingest::main(rest),
        "inspect" => inspect::main(rest),
        "repack" => repack::main(rest),
        "help" | "--help" | "-h" => {
            print!("{}", cli::USAGE);
            Ok(())
        }
        other => Err(gvfs_daemon::err!(
            "unknown command {other:?} (see `gvfs-daemon help`)"
        )),
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("gvfs-daemon {command}: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
