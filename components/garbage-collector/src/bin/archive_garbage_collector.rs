//! Runs the archive garbage collector of a clp-s package until a shutdown signal arrives.

use std::convert::Infallible;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use clp_rust_utils::clp_config::package;
use clp_rust_utils::database::mysql::create_clp_db_mysql_pool;
use clp_rust_utils::serde::yaml;
use garbage_collector::ArchiveGarbageCollector;
use tokio::signal::unix::SignalKind;

/// Command-line arguments for the archive garbage collector.
#[derive(Debug, Parser)]
#[command(about = "Run the clp-s archive garbage collector.")]
struct Cli {
    /// Path to the configuration file.
    #[arg(short, long, value_name = "PATH")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    const SECS_PER_MINUTE: u64 = 60;

    let args = Cli::parse();

    let _guard = clp_rust_utils::logging::set_up_logging("archive_garbage_collector.log");

    let config: package::config::Config = yaml::from_path(args.config).inspect_err(|e| {
        tracing::error!(error = % e, "Failed to load the configuration file.");
    })?;

    let credentials = package::credentials::Database::from_env().inspect_err(|e| {
        tracing::error!(error = % e, "Failed to read the database credentials.");
    })?;

    let db_pool = create_clp_db_mysql_pool(&config.database, &credentials, 1)
        .await
        .inspect_err(|e| tracing::error!(error = % e, "Failed to create the database pool."))?;

    let garbage_collector = ArchiveGarbageCollector::from_config(&config, db_pool.clone())
        .await
        .inspect_err(|e| {
            tracing::error!(error = % e, "Failed to create the archive garbage collector.");
        })?;
    let sweep_interval = Duration::from_secs(
        config
            .garbage_collector
            .sweep_interval
            .archive_minutes
            .get()
            .saturating_mul(SECS_PER_MINUTE),
    );

    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())
        .inspect_err(|e| tracing::error!(error = % e, "Failed to listen for SIGTERM."))?;

    tracing::info!("Archive garbage collector started.");
    tokio::select! {
        _ = sigterm.recv() => tracing::info!("Received SIGTERM."),
        result = tokio::signal::ctrl_c() => {
            result.inspect_err(|e| tracing::error!(error = % e, "Failed to listen for ctrl-c."))?;
            tracing::info!("Received ctrl-c.");
        }
        never = run(&garbage_collector, sweep_interval) => match never {},
    }

    db_pool.close().await;
    tracing::info!("Archive garbage collector stopped.");
    Ok(())
}

/// Sweeps with `garbage_collector`, and then sleeps for `sweep_interval`, repeatedly.
///
/// # Returns
///
/// Never returns.
async fn run(garbage_collector: &ArchiveGarbageCollector, sweep_interval: Duration) -> Infallible {
    loop {
        if let Err(e) = garbage_collector.sweep().await {
            tracing::error!(error = % e, "Failed to sweep the archives.");
        }
        tokio::time::sleep(sweep_interval).await;
    }
}
