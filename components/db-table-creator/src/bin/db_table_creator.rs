//! The DB table creator executable.

use std::path::PathBuf;

use clap::Parser;
use clp_metadata_db::MetadataTables;
use clp_rust_utils::clp_config::package::{self};
use clp_rust_utils::database::mysql::create_clp_db_mysql_pool;
use clp_rust_utils::serde::yaml;

/// Command-line arguments for the DB table creator.
#[derive(Debug, Parser)]
#[command(about = "Create the CLP DB's tables.")]
struct Cli {
    /// Path to the configuration file.
    #[arg(short, long, value_name = "PATH")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    let _guard = clp_rust_utils::logging::set_up_logging("db_table_creator.log");

    let config: package::config::Config = yaml::from_path(args.config).inspect_err(|e| {
        tracing::error!(error = % e, "Failed to load the configuration file.");
    })?;

    if package::config::StorageEngine::ClpS != config.package.storage_engine {
        tracing::error!("The DB table creator only supports the clp-s storage engine.");
        anyhow::bail!("the DB table creator only supports the clp-s storage engine");
    }

    let credentials = package::credentials::Database::from_env()?;

    let db_pool = create_clp_db_mysql_pool(&config.database, &credentials, 1)
        .await
        .inspect_err(|e| tracing::error!(error = % e, "Failed to create the database pool."))?;

    let mut db_connection = db_pool.acquire().await.inspect_err(|e| {
        tracing::error!(error = % e, "Failed to acquire a database connection.");
    })?;

    MetadataTables::new(&config.database)
        .create_tables(&mut db_connection)
        .await
        .inspect_err(|e| {
            tracing::error!(error = % e, "Failed to create the metadata tables.");
        })?;
    tracing::info!("Created the metadata tables.");

    Ok(())
}
