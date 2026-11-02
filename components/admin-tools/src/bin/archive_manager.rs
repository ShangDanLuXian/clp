use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::process::ExitCode;

use admin_tools::archive;
use admin_tools::archive::Archive;
use clap::Parser;
use clap::Subcommand;
use clp_rust_utils::clp_config::package::config::ArchiveOutputStorage;
use clp_rust_utils::clp_config::package::config::Config;
use clp_rust_utils::clp_config::package::config::Database as DatabaseConfig;
use clp_rust_utils::clp_config::package::config::StorageEngine;
use clp_rust_utils::clp_config::package::credentials;
use clp_rust_utils::database::mysql::create_clp_db_mysql_pool;
use clp_rust_utils::dataset::DatasetId;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::serde::yaml;
use clp_rust_utils::types::ArchiveId;

/// Command-line arguments for the archive manager.
#[derive(Debug, Parser)]
#[command(about = "View list of archive IDs or delete compressed archives.")]
struct Cli {
    /// CLP configuration file.
    #[arg(short, long, value_name = "PATH")]
    config: PathBuf,

    /// Enable debug logging.
    #[arg(short, long)]
    verbose: bool,

    /// The dataset that the archives belong to.
    #[arg(long, default_value = DatasetName::default_name())]
    dataset: DatasetName,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// List IDs of archives.
    Find {
        /// Time-range lower-bound (inclusive) as milliseconds from the UNIX epoch.
        #[arg(long, default_value_t = 0, allow_negative_numbers = true)]
        begin_ts: i64,

        /// Time-range upper-bound (inclusive) as milliseconds from the UNIX epoch.
        #[arg(long, allow_negative_numbers = true)]
        end_ts: Option<i64>,
    },

    /// Delete archives from the database and file system.
    Del {
        /// Preview delete without making changes. Lists errors and files to be deleted.
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
        /// List of archive IDs to delete
        #[arg(required = true)]
        ids: Vec<String>,
    },

    /// Deletes archives that fall within the specified time range.
    ByFilter {
        /// Time-range lower-bound (inclusive) as milliseconds from the UNIX epoch.
        #[arg(allow_negative_numbers = true)]
        begin_ts: i64,

        /// Time-range upper-bound (inclusive) as milliseconds from the UNIX epoch.
        #[arg(allow_negative_numbers = true)]
        end_ts: i64,
    },
}

/// The criteria that select the archives to delete.
enum DeleteCriteria {
    Ids(Vec<ArchiveId>),
    TimeRange { begin_ts: i64, end_ts: i64 },
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Cli::parse();

    let _guard = clp_rust_utils::logging::set_up_logging_with_default_level(
        "archive_manager.log",
        if args.verbose {
            tracing::Level::DEBUG
        } else {
            tracing::Level::INFO
        },
    );

    if run(args).await.is_ok() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(admin_tools::FAILURE_EXIT_CODE)
    }
}

/// Runs the command given by `args`.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if:
///   * The storage engine isn't clp-s.
///   * The archive storage isn't FS.
///   * The dataset doesn't exist.
///   * `archive_output.directory` doesn't exist.
///   * Some of the given archive IDs are not valid UUIDs.
/// * Forwards [`yaml::from_path`]'s return values on failure.
/// * Forwards [`credentials::Database::from_env`]'s return values on failure.
/// * Forwards [`create_clp_db_mysql_pool`]'s return values on failure.
/// * Forwards [`clp_rust_utils::database::mysql::dataset::get_id`]'s return values on failure.
/// * Forwards [`find_archives`]'s return values on failure.
/// * Forwards [`delete_archives`]'s return values on failure.
async fn run(args: Cli) -> anyhow::Result<()> {
    let config: Config = yaml::from_path(&args.config).inspect_err(|e| {
        tracing::error!(error = % e, "Failed to load the configuration file.");
    })?;

    if StorageEngine::ClpS != config.package.storage_engine {
        const ERROR_MESSAGE: &str = "The archive manager only supports the clp-s storage engine.";
        tracing::error!("{ERROR_MESSAGE}");
        anyhow::bail!(ERROR_MESSAGE);
    }

    let ArchiveOutputStorage::Fs { directory } = &config.archive_output.storage else {
        const ERROR_MESSAGE: &str = "The archive manager only supports FS archive storage.";
        tracing::error!("{ERROR_MESSAGE}");
        anyhow::bail!(ERROR_MESSAGE);
    };

    let db_credentials = credentials::Database::from_env()?;
    let db_pool = create_clp_db_mysql_pool(&config.database, &db_credentials, 1)
        .await
        .inspect_err(|e| tracing::error!(error = % e, "Failed to create the database pool."))?;

    let dataset = args.dataset;
    let Some(dataset_id) =
        clp_rust_utils::database::mysql::dataset::get_id(&db_pool, &config.database, &dataset)
            .await
            .inspect_err(|e| {
                tracing::error!(error = % e, "Failed to look up the dataset in the database.");
            })?
    else {
        tracing::error!("Dataset `{dataset}` doesn't exist.");
        anyhow::bail!("dataset `{dataset}` doesn't exist");
    };

    let archives_dir = Path::new(directory);
    if !archives_dir.exists() {
        const ERROR_MESSAGE: &str = "`archive_output.directory` doesn't exist.";
        tracing::error!("{ERROR_MESSAGE}");
        anyhow::bail!(ERROR_MESSAGE);
    }
    let archive_output_dir = archives_dir.join(dataset.as_str());

    match args.command {
        Command::Find { begin_ts, end_ts } => {
            find_archives(
                &db_pool,
                &config.database,
                &archive_output_dir,
                &dataset,
                dataset_id,
                begin_ts,
                end_ts,
            )
            .await
        }
        Command::Del { dry_run, command } => {
            let criteria = match command {
                DelCommand::ByIds { ids } => DeleteCriteria::Ids(
                    ids.iter()
                        .map(|id| id.parse::<ArchiveId>())
                        .collect::<Result<_, _>>()
                        .inspect_err(|e| {
                            tracing::error!(
                                error = % e,
                                "Some of the provided archive IDs are not valid UUIDs."
                            );
                        })?,
                ),
                DelCommand::ByFilter { begin_ts, end_ts } => {
                    DeleteCriteria::TimeRange { begin_ts, end_ts }
                }
            };
            delete_archives(
                &db_pool,
                &config.database,
                &archive_output_dir,
                &dataset,
                dataset_id,
                criteria,
                dry_run,
            )
            .await
        }
    }
}

/// Lists the archives of a dataset whose timestamp ranges begin at or after `begin_ts` and, if
/// `end_ts` is given, end at or before `end_ts`.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::Pool::acquire`]'s return values on failure.
/// * Forwards [`archive::find_by_time_range`]'s return values on failure.
async fn find_archives(
    db_pool: &sqlx::MySqlPool,
    database_config: &DatabaseConfig,
    archive_output_dir: &Path,
    dataset: &DatasetName,
    dataset_id: DatasetId,
    begin_ts: i64,
    end_ts: Option<i64>,
) -> anyhow::Result<()> {
    tracing::info!("Starting to find archives of dataset `{dataset}` from the database.");
    let archives = async {
        let mut connection = db_pool.acquire().await?;
        archive::find_by_time_range(
            &mut connection,
            database_config,
            dataset_id,
            begin_ts,
            end_ts,
        )
        .await
    }
    .await
    .inspect_err(|e| tracing::error!(error = % e, "Failed to find archives from the database."))?;
    if archives.is_empty() {
        tracing::info!("No archives found within specified time range.");
        return Ok(());
    }

    tracing::info!(
        "Found {} archives within the specified time range.",
        archives.len()
    );
    for Archive { uuid, .. } in archives {
        tracing::info!("{uuid}");
        if !archive_output_dir.join(uuid.to_string()).is_dir() {
            tracing::warn!("Archive {uuid} in database not found on disk.");
        }
    }

    tracing::info!("Finished finding archives from the database.");
    Ok(())
}

/// Deletes the archives of a dataset that `criteria` selects from the database and then from disk.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`delete_archives_from_database`]'s return values on failure.
/// * Forwards [`std::fs::remove_dir_all`]'s return values on failure.
async fn delete_archives(
    db_pool: &sqlx::MySqlPool,
    database_config: &DatabaseConfig,
    archive_output_dir: &Path,
    dataset: &DatasetName,
    dataset_id: DatasetId,
    criteria: DeleteCriteria,
    dry_run: bool,
) -> anyhow::Result<()> {
    tracing::info!("Starting to delete archives of dataset `{dataset}` from the database.");
    let Some(archives) =
        delete_archives_from_database(db_pool, database_config, dataset_id, criteria, dry_run)
            .await
            .inspect_err(|e| {
                tracing::error!(
                    error = % e,
                    "Failed to delete archives from the database. Aborting deletion."
                );
            })?
    else {
        return Ok(());
    };
    tracing::info!("Finished deleting archives from the database.");

    for Archive { uuid, .. } in archives {
        let archive_path = archive_output_dir.join(uuid.to_string());
        if !archive_path.is_dir() {
            tracing::warn!("Archive {uuid} is not a directory. Skipping deletion.");
            continue;
        }

        tracing::info!("Deleting archive {uuid} from disk.");
        std::fs::remove_dir_all(&archive_path).inspect_err(|e| {
            tracing::error!(error = % e, "Failed to delete archive {uuid} from disk.");
        })?;
    }

    tracing::info!("Finished deleting archives from disk.");
    Ok(())
}

/// Deletes the archives of a dataset that `criteria` selects from the database, in one
/// transaction that's rolled back if `dry_run` is set.
///
/// # Returns
///
/// The deleted archives on success, or `None` if no archives match `criteria` or `dry_run` is set.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::Pool::begin`]'s return values on failure.
/// * Forwards [`archive::find_by_uuids`]'s return values on failure.
/// * Forwards [`archive::find_by_time_range`]'s return values on failure.
/// * Forwards [`clp_rust_utils::database::mysql::archive::delete`]'s return values on failure.
/// * Forwards [`sqlx::Transaction::rollback`]'s return values on failure.
/// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
async fn delete_archives_from_database(
    db_pool: &sqlx::MySqlPool,
    database_config: &DatabaseConfig,
    dataset_id: DatasetId,
    criteria: DeleteCriteria,
    dry_run: bool,
) -> anyhow::Result<Option<Vec<Archive>>> {
    let mut tx = db_pool.begin().await?;
    if dry_run {
        tracing::info!("Running in dry-run mode.");
    }

    let archives = match criteria {
        DeleteCriteria::Ids(ids) => {
            let archives =
                archive::find_by_uuids(&mut tx, database_config, dataset_id, &ids).await?;
            if archives.is_empty() {
                tracing::info!("No archives found with matching IDs.");
                return Ok(None);
            }

            let not_found_ids = find_not_found_ids(ids, &archives);
            if !not_found_ids.is_empty() {
                tracing::warn!(
                    "Archives with the following IDs don't exist: {}.",
                    not_found_ids
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            archives
        }
        DeleteCriteria::TimeRange { begin_ts, end_ts } => {
            let archives = archive::find_by_time_range(
                &mut tx,
                database_config,
                dataset_id,
                begin_ts,
                Some(end_ts),
            )
            .await?;
            if archives.is_empty() {
                tracing::info!("No archives found within the specified time range.");
                return Ok(None);
            }
            archives
        }
    };

    let archive_ids: Vec<u64> = archives.iter().map(|archive| archive.id).collect();
    clp_rust_utils::database::mysql::archive::delete(
        &mut tx,
        database_config,
        dataset_id,
        &archive_ids,
    )
    .await?;
    for archive in &archives {
        tracing::info!("Deleted archive {} from the database.", archive.uuid);
    }

    if dry_run {
        tracing::info!("Dry-run finished.");
        tx.rollback().await?;
        return Ok(None);
    }

    tx.commit().await?;
    Ok(Some(archives))
}

/// # Returns
///
/// The IDs in `ids` that aren't the UUID of any archive in `archives`, without duplicates, in the
/// order that they first appear in `ids`.
fn find_not_found_ids(ids: Vec<ArchiveId>, archives: &[Archive]) -> Vec<ArchiveId> {
    let mut seen_ids: HashSet<ArchiveId> = archives.iter().map(|archive| archive.uuid).collect();
    ids.into_iter().filter(|id| seen_ids.insert(*id)).collect()
}

#[cfg(test)]
mod tests {
    use admin_tools::archive::Archive;
    use clp_rust_utils::types::ArchiveId;

    use super::find_not_found_ids;

    #[test]
    fn not_found_ids_exclude_found_ids_and_duplicates_in_order() {
        let ids: Vec<ArchiveId> = [
            "018e90e5-8b2a-4a61-a2fc-cac799936ca0",
            "018e90e5-8b2a-4a61-a2fc-cac799936ca1",
            "018e90e5-8b2a-4a61-a2fc-cac799936ca2",
            "018e90e5-8b2a-4a61-a2fc-cac799936ca3",
        ]
        .iter()
        .map(|id| id.parse().expect("valid UUID should parse"))
        .collect();
        let archives = [
            Archive {
                id: 1,
                uuid: ids[1],
            },
            Archive {
                id: 2,
                uuid: ids[3],
            },
        ];

        assert_eq!(
            find_not_found_ids(vec![ids[2], ids[1], ids[0], ids[2], ids[3]], &archives),
            vec![ids[2], ids[0]]
        );
    }
}
