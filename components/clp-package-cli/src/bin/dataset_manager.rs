//! The dataset manager: lists datasets, and marks them as deleted.

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use clap::Subcommand;
use clp_package_cli::dataset_manager::DatasetsToDelete;
use clp_package_cli::dataset_manager::delete_datasets;
use clp_package_cli::dataset_manager::list_datasets;
use clp_package_cli::package::FAILURE_EXIT_CODE;
use clp_package_cli::package::connect;
use clp_package_cli::package::database_credentials_from_env;
use clp_package_cli::package::load_config;
use clp_package_cli::package::set_up_logging;
use clp_rust_utils::dataset::DatasetName;

/// Command-line arguments of the dataset manager.
#[derive(Debug, Parser)]
#[command(about = "List or delete datasets.")]
struct Cli {
    /// CLP configuration file.
    #[arg(short, long, value_name = "PATH")]
    config: PathBuf,

    /// Enable debug logging.
    #[arg(short, long)]
    verbose: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// List existing datasets.
    List,

    /// Delete datasets. A deleted dataset's archives and storage are kept, and its name can't be
    /// used again.
    Del {
        /// Datasets to delete.
        #[arg(required_unless_present = "all", conflicts_with = "all")]
        datasets: Vec<DatasetName>,

        /// Delete all existing datasets.
        #[arg(short, long)]
        all: bool,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    set_up_logging(cli.verbose);
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = % format!("{e:#}"), "Dataset manager failed.");
            ExitCode::from(FAILURE_EXIT_CODE)
        }
    }
}

/// Runs the command that `cli` names. A listed dataset is printed to stdout as its name.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`load_config`]'s return values on failure.
/// * Forwards [`database_credentials_from_env`]'s return values on failure.
/// * Forwards [`connect`]'s return values on failure.
/// * Forwards [`list_datasets`]'s return values on failure.
/// * Forwards [`delete_datasets`]'s return values on failure.
/// * [`std::io::Error`] if writing to stdout fails.
async fn run(cli: Cli) -> anyhow::Result<()> {
    let config = load_config(&cli.config)?;
    let credentials = database_credentials_from_env()?;
    let client = connect(&config, &credentials).await?;
    match cli.command {
        Command::List => {
            let datasets = list_datasets(&client).await?;
            tracing::info!(num_datasets = datasets.len(), "Found datasets.");
            let mut stdout = std::io::stdout().lock();
            for dataset in datasets {
                writeln!(stdout, "{}", dataset.name)?;
            }
        }
        Command::Del { datasets, all } => {
            let datasets = if all {
                DatasetsToDelete::All
            } else {
                DatasetsToDelete::Named(datasets)
            };
            delete_datasets(&client, datasets).await?;
        }
    }
    Ok(())
}
