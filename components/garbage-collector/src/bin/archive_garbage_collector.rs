//! The archive garbage collector executable.

use std::path::PathBuf;

use clap::Parser;
use clp_rust_utils::clp_config::package::config::Config;
use clp_rust_utils::clp_config::package::config::StorageEngine;
use clp_rust_utils::clp_config::package::credentials;
use clp_rust_utils::database::mysql::create_clp_db_mysql_pool;
use clp_rust_utils::serde::yaml;
use garbage_collector::archive::ArchiveGarbageCollector;

/// Command-line arguments for the archive garbage collector.
#[derive(Debug, Parser)]
#[command(about = "Run the archive garbage collector.")]
struct Cli {
    /// Path to the configuration file.
    #[arg(short, long, value_name = "PATH")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    let _guard = clp_rust_utils::logging::set_up_logging("archive_garbage_collector.log");

    let config: Config = yaml::from_path(args.config).inspect_err(|e| {
        tracing::error!(error = % e, "Failed to load the configuration file.");
    })?;

    if StorageEngine::ClpS != config.package.storage_engine {
        tracing::error!("The archive garbage collector only supports the clp-s storage engine.");
        anyhow::bail!("the archive garbage collector only supports the clp-s storage engine");
    }

    let db_credentials = credentials::Database::from_env()?;
    let db_pool = create_clp_db_mysql_pool(&config.database, &db_credentials, 1)
        .await
        .inspect_err(|e| tracing::error!(error = % e, "Failed to create the database pool."))?;

    run_until_shutdown(&ArchiveGarbageCollector::new(config, db_pool)).await;
    Ok(())
}

/// Runs `garbage_collector` until a shutdown signal arrives.
///
/// # Panics
///
/// Panics if listening for `SIGTERM` fails.
async fn run_until_shutdown(garbage_collector: &ArchiveGarbageCollector) {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to listen for SIGTERM");

    tokio::select! {
        _ = sigterm.recv() => tracing::info!("Received SIGTERM."),
        result = tokio::signal::ctrl_c() => {
            if let Err(e) = result {
                tracing::error!(error = % e, "Failed to listen to ctrl-c.");
            }
            tracing::info!("Received ctrl-c.");
        }
        never = garbage_collector.run() => match never {},
    }

    tracing::info!("Archive garbage collector stopped.");
}
