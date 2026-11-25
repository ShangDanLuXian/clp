//! The archive manager: finds archives, and deletes them from the metadata database and storage.

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use clap::Subcommand;
use clp_package_cli::archive_manager::ArchiveSelection;
use clp_package_cli::archive_manager::ArchiveStorage;
use clp_package_cli::archive_manager::delete_archives;
use clp_package_cli::archive_manager::find_archives;
use clp_package_cli::archive_manager::preview_deletion;
use clp_package_cli::package::FAILURE_EXIT_CODE;
use clp_package_cli::package::connect;
use clp_package_cli::package::database_credentials_from_env;
use clp_package_cli::package::load_config;
use clp_package_cli::package::set_up_logging;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::job_config::TimeRange;

/// Command-line arguments of the archive manager.
#[derive(Debug, Parser)]
#[command(about = "View list of archive IDs or delete compressed archives.")]
struct Cli {
    /// CLP configuration file.
    #[arg(short, long, value_name = "PATH")]
    config: PathBuf,

    /// Enable debug logging.
    #[arg(short, long)]
    verbose: bool,

    /// The dataset that the archives belong to [default: default]. Not allowed when deleting by
    /// archive ID: an ID identifies an archive across datasets.
    #[arg(long)]
    dataset: Option<DatasetName>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// List the IDs and UUIDs of archives.
    Find {
        /// Time-range lower-bound (inclusive) as milliseconds from the UNIX epoch.
        #[arg(long, allow_negative_numbers = true)]
        begin_ts: Option<i64>,

        /// Time-range upper-bound (inclusive) as milliseconds from the UNIX epoch.
        #[arg(long, allow_negative_numbers = true)]
        end_ts: Option<i64>,
    },

    /// Delete archives from the database and storage.
    Del {
        /// Preview delete without making changes. Lists the archives to be deleted.
        #[arg(long)]
        dry_run: bool,

        #[command(subcommand)]
        command: DelCommand,
    },
}

#[derive(Debug, Subcommand)]
enum DelCommand {
    /// Delete archives by ID.
    ByIds {
        /// Take the arguments as UUIDs of archives of `--dataset` (compatibility mode).
        #[arg(long)]
        uuid: bool,

        /// List of archive IDs (or UUIDs, with `--uuid`) to delete.
        #[arg(required = true)]
        ids: Vec<String>,
    },

    /// Delete archives that fall within the specified time range.
    #[command(allow_negative_numbers = true)]
    ByFilter {
        /// Time-range lower-bound (inclusive) as milliseconds from the UNIX epoch.
        begin_ts: i64,

        /// Time-range upper-bound (inclusive) as milliseconds from the UNIX epoch.
        end_ts: i64,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    set_up_logging(cli.verbose);
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = % format!("{e:#}"), "Archive manager failed.");
            ExitCode::from(FAILURE_EXIT_CODE)
        }
    }
}

/// Runs the command that `cli` names. A found archive is printed to stdout as its ID and UUID,
/// separated by a tab.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`load_config`]'s return values on failure.
/// * Forwards [`database_credentials_from_env`]'s return values on failure.
/// * Forwards [`TimeRange::new`]'s return values on failure.
/// * Forwards [`ArchiveSelection::by_ids`]'s return values on failure.
/// * Forwards [`ArchiveSelection::by_uuids`]'s return values on failure.
/// * Forwards [`connect`]'s return values on failure.
/// * Forwards [`find_archives`]'s return values on failure.
/// * Forwards [`preview_deletion`]'s return values on failure.
/// * Forwards [`delete_archives`]'s return values on failure.
/// * [`std::io::Error`] if writing to stdout fails.
async fn run(cli: Cli) -> anyhow::Result<()> {
    let config = load_config(&cli.config)?;
    let credentials = database_credentials_from_env()?;
    match cli.command {
        Command::Find { begin_ts, end_ts } => {
            let time_range = TimeRange::new(begin_ts, end_ts)?;
            // `--dataset` defaults to the default dataset, as in the Python tool.
            let dataset = cli.dataset.unwrap_or_default();
            let client = connect(&config, &credentials).await?;
            let archives = find_archives(&client, &dataset, &time_range).await?;
            if archives.is_empty() {
                tracing::info!("No archives found within the specified time range.");
                return Ok(());
            }
            tracing::info!(
                dataset = % dataset,
                num_archives = archives.len(),
                "Found archives within the specified time range."
            );
            let mut stdout = std::io::stdout().lock();
            for archive in archives {
                writeln!(stdout, "{}\t{}", archive.id, archive.uuid)?;
            }
        }
        Command::Del { dry_run, command } => {
            let selection = match command {
                DelCommand::ByIds { uuid: false, ids } => {
                    ArchiveSelection::by_ids(cli.dataset.as_ref(), &ids)?
                }
                DelCommand::ByIds { uuid: true, ids } => {
                    ArchiveSelection::by_uuids(cli.dataset, &ids)?
                }
                DelCommand::ByFilter { begin_ts, end_ts } => ArchiveSelection::by_time_range(
                    cli.dataset,
                    TimeRange::new(Some(begin_ts), Some(end_ts))?,
                ),
            };
            let client = connect(&config, &credentials).await?;
            if dry_run {
                tracing::info!("Running in dry-run mode.");
                preview_deletion(&client, &selection).await?;
            } else {
                let storage = ArchiveStorage::from_config(&config.archive_output.storage).await;
                delete_archives(&client, &storage, &selection).await?;
            }
        }
    }
    Ok(())
}
