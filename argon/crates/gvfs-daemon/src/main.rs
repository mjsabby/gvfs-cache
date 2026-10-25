//! `gvfs-daemon` — Unified binary entry point (PDF pp. 145–153 & DESIGN.md).
#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use gvfs_alloc_guard::SealedAllocator;

#[global_allocator]
static GLOBAL: SealedAllocator = SealedAllocator;

fn main() -> std::process::ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    match gvfs_daemon::parse_cli_args(&argv) {
        Ok(None) => {
            print!("{}", gvfs_daemon::USAGE);
            std::process::ExitCode::SUCCESS
        }
        Ok(Some(cmd)) => match gvfs_daemon::execute_command(cmd) {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("gvfs-daemon: {e}");
                std::process::ExitCode::FAILURE
            }
        },
        Err(msg) => {
            eprintln!("gvfs-daemon: {msg}\n\n{}", gvfs_daemon::USAGE);
            std::process::ExitCode::from(2)
        }
    }
}
