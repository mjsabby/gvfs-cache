#![forbid(unsafe_code)]

use gvfs_daemon::Result;
use gvfs_daemon::cli::{Cli, HELP, Mode};

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("gvfs-daemon: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse(std::env::args().skip(1))?;
    match cli.mode {
        Mode::Help => {
            print!("{HELP}");
            return Ok(());
        }
        Mode::Version => {
            println!("gvfs-daemon {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        Mode::Pipeline | Mode::Serve | Mode::Benchmark => (),
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let storage = cli.storage_client()?;
        let mut upstream = cli.git_upstream()?;
        match cli.mode {
            Mode::Pipeline => {
                let report =
                    gvfs_daemon::pipeline::run_pipeline(&storage, upstream.as_mut(), &cli.pipeline)
                        .await?;
                println!(
                    "generation={} tip={} new_objects={} slabs_written={} changed={}",
                    report.generation, report.tip, report.objects, report.artifacts, report.changed
                );
                Ok(())
            }
            Mode::Serve => {
                gvfs_daemon::server::serve(std::sync::Arc::new(storage), upstream, cli.server).await
            }
            Mode::Benchmark => {
                gvfs_daemon::benchmark::run(
                    &cli.storage,
                    &cli.cache,
                    upstream.as_mut().ok_or_else(|| {
                        gvfs_daemon::Error::Usage("bench requires --upstream".into())
                    })?,
                    &cli.pipeline,
                    cli.server.range_bytes,
                )
                .await
            }
            Mode::Help | Mode::Version => Ok(()),
        }
    })
}
