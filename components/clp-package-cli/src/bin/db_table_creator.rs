//! The table creator: sets up the clp-s metadata tables and the `default` dataset.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use clp_package_cli::package::FAILURE_EXIT_CODE;
use clp_package_cli::package::connect;
use clp_package_cli::package::database_credentials_from_env;
use clp_package_cli::package::load_config;
use clp_package_cli::package::set_up_logging;
use clp_package_cli::table_creator::DefaultDataset;
use clp_package_cli::table_creator::create_tables_and_default_dataset;

/// Command-line arguments of the table creator.
#[derive(Debug, Parser)]
#[command(about = "Sets up the clp-s metadata tables and the `default` dataset.")]
struct Cli {
    /// CLP configuration file.
    #[arg(short, long, value_name = "PATH")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    set_up_logging(false);
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = % format!("{e:#}"), "Failed to create the metadata tables.");
            ExitCode::from(FAILURE_EXIT_CODE)
        }
    }
}

/// Creates the metadata tables and the `default` dataset of the package that `cli` configures.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`load_config`]'s return values on failure.
/// * Forwards [`database_credentials_from_env`]'s return values on failure.
/// * Forwards [`connect`]'s return values on failure.
/// * Forwards [`create_tables_and_default_dataset`]'s return values on failure.
async fn run(cli: Cli) -> anyhow::Result<()> {
    let config = load_config(&cli.config)?;
    let credentials = database_credentials_from_env()?;
    let client = connect(&config, &credentials).await?;
    match create_tables_and_default_dataset(&client, &config.archive_output).await? {
        DefaultDataset::Ready { id } => tracing::info!(
            default_dataset_id = % id,
            "Created the metadata tables and the `default` dataset."
        ),
        DefaultDataset::Deleted | DefaultDataset::ConfigMismatch { .. } => {
            tracing::info!("Created the metadata tables.");
        }
    }
    Ok(())
}
