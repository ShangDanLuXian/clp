use std::path::Path;
use std::path::PathBuf;
use std::process::ExitCode;

use admin_tools::dataset::Dataset;
use clap::Parser;
use clap::Subcommand;
use clp_rust_utils::aws::AWS_DEFAULT_REGION;
use clp_rust_utils::clp_config::S3Config;
use clp_rust_utils::clp_config::package::config::ArchiveOutputStorage;
use clp_rust_utils::clp_config::package::config::Config;
use clp_rust_utils::clp_config::package::config::StorageEngine;
use clp_rust_utils::database::mysql::create_clp_db_mysql_pool;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::serde::yaml;
use clp_rust_utils::types::non_empty_string::ExpectedNonEmpty;
use non_empty_string::NonEmptyString;

/// Command-line arguments for the dataset manager.
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

    /// Delete datasets from the database and file storage.
    Del {
        /// Datasets to delete.
        datasets: Vec<DatasetName>,

        /// Delete all existing datasets.
        #[arg(short = 'a', long = "all")]
        del_all: bool,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Cli::parse();

    let _guard = clp_rust_utils::logging::set_up_logging_with_default_level(
        "dataset_manager.log",
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
/// * [`anyhow::Error`] if the storage engine isn't clp-s.
/// * Forwards [`yaml::from_path`]'s return values on failure.
/// * Forwards [`admin_tools::read_db_credentials_from_env`]'s return values on failure.
/// * Forwards [`create_clp_db_mysql_pool`]'s return values on failure.
/// * Forwards [`admin_tools::dataset::fetch_existing`]'s return values on failure.
/// * Forwards [`delete_datasets`]'s return values on failure.
async fn run(args: Cli) -> anyhow::Result<()> {
    let config: Config = yaml::from_path(&args.config).inspect_err(|e| {
        tracing::error!(error = % e, "Failed to load the configuration file.");
    })?;

    if StorageEngine::ClpS != config.package.storage_engine {
        const ERROR_MESSAGE: &str = "The dataset manager only supports the clp-s storage engine.";
        tracing::error!("{ERROR_MESSAGE}");
        anyhow::bail!(ERROR_MESSAGE);
    }

    let db_credentials = admin_tools::read_db_credentials_from_env()?;
    let db_pool = create_clp_db_mysql_pool(&config.database, &db_credentials, 1)
        .await
        .inspect_err(|e| tracing::error!(error = % e, "Failed to create the database pool."))?;

    let existing_datasets = admin_tools::dataset::fetch_existing(&db_pool, &config.database)
        .await
        .inspect_err(|e| {
            tracing::error!(error = % e, "Failed to fetch datasets from the database.");
        })?;

    match args.command {
        Command::List => {
            tracing::info!("Found {} datasets.", existing_datasets.len());
            for dataset in &existing_datasets {
                tracing::info!("{}", dataset.name);
            }
            Ok(())
        }
        Command::Del { datasets, del_all } => {
            delete_datasets(&db_pool, &config, &existing_datasets, datasets, del_all).await
        }
    }
}

/// Deletes the given datasets, or every existing dataset if `del_all` is set.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if a dataset in `datasets` doesn't exist.
/// * Forwards [`delete_dataset`]'s return values on failure.
async fn delete_datasets(
    db_pool: &sqlx::MySqlPool,
    config: &Config,
    existing_datasets: &[Dataset],
    datasets: Vec<DatasetName>,
    del_all: bool,
) -> anyhow::Result<()> {
    if existing_datasets.is_empty() {
        tracing::warn!("No datasets exist.");
        return Ok(());
    }

    let datasets_to_delete: Vec<&Dataset> = if del_all {
        existing_datasets.iter().collect()
    } else {
        let mut datasets_to_delete = Vec::new();
        for name in datasets {
            let Some(dataset) = existing_datasets
                .iter()
                .find(|dataset| dataset.name == name)
            else {
                tracing::error!("Dataset `{name}` doesn't exist. Aborting deletion.");
                anyhow::bail!("dataset `{name}` doesn't exist");
            };
            if !datasets_to_delete.contains(&dataset) {
                datasets_to_delete.push(dataset);
            }
        }
        datasets_to_delete
    };

    for dataset in datasets_to_delete {
        delete_dataset(db_pool, config, dataset).await?;
    }

    Ok(())
}

/// Deletes a dataset's archives from the archive storage, and then marks the dataset as deleted.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`delete_archives_from_fs`]'s return values on failure.
/// * Forwards [`delete_archives_from_s3`]'s return values on failure.
/// * Forwards [`admin_tools::dataset::mark_deleted`]'s return values on failure.
async fn delete_dataset(
    db_pool: &sqlx::MySqlPool,
    config: &Config,
    dataset: &Dataset,
) -> anyhow::Result<()> {
    let name = &dataset.name;
    match &config.archive_output.storage {
        ArchiveOutputStorage::Fs { .. } => delete_archives_from_fs(&dataset.archive_storage_path),
        ArchiveOutputStorage::S3 { s3_config, .. } => {
            delete_archives_from_s3(s3_config, &dataset.archive_storage_path).await
        }
    }
    .inspect_err(|e| {
        tracing::error!(error = % e, "Failed to delete archives of dataset `{name}`.");
    })?;
    tracing::info!("Deleted archives of dataset `{name}`.");

    admin_tools::dataset::mark_deleted(db_pool, &config.database, dataset.id)
        .await
        .inspect_err(|e| {
            tracing::error!(
                error = % e,
                "Failed to delete dataset `{name}` from the metadata database."
            );
        })?;
    tracing::info!("Deleted dataset `{name}` from the metadata database.");

    Ok(())
}

/// Deletes the directory at `archive_storage_path`, if it's a directory.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`Path::canonicalize`]'s return values on failure.
/// * Forwards [`std::fs::remove_dir_all`]'s return values on failure.
fn delete_archives_from_fs(archive_storage_path: &str) -> anyhow::Result<()> {
    let archive_storage_path = Path::new(archive_storage_path);
    if !archive_storage_path.exists() {
        tracing::debug!("'{}' doesn't exist.", archive_storage_path.display());
        return Ok(());
    }
    if !archive_storage_path.is_dir() {
        tracing::debug!(
            "'{}' isn't a directory. Skipping deletion.",
            archive_storage_path.display()
        );
        return Ok(());
    }

    std::fs::remove_dir_all(archive_storage_path.canonicalize()?)?;
    Ok(())
}

/// Deletes every object under the archive storage path in the S3 bucket.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`clp_rust_utils::s3::delete_by_key_prefix`]'s return values on failure.
async fn delete_archives_from_s3(
    s3_config: &S3Config,
    archive_storage_path: &str,
) -> anyhow::Result<()> {
    let region = s3_config
        .region_code
        .as_ref()
        .map_or(AWS_DEFAULT_REGION, NonEmptyString::as_str);
    let client = clp_rust_utils::s3::create_new_client(
        region,
        s3_config.endpoint_url.as_ref(),
        &s3_config.aws_authentication,
    )
    .await;
    clp_rust_utils::s3::delete_by_key_prefix(
        &client,
        s3_config.bucket.as_str(),
        &archive_storage_key_prefix(archive_storage_path),
    )
    .await?;
    Ok(())
}

/// # Returns
///
/// `archive_storage_path` with a trailing `/`.
fn archive_storage_key_prefix(archive_storage_path: &str) -> NonEmptyString {
    if archive_storage_path.ends_with('/') {
        NonEmptyString::from_string(archive_storage_path.to_owned())
    } else {
        NonEmptyString::from_string(format!("{archive_storage_path}/"))
    }
}

#[cfg(test)]
mod tests {
    use super::archive_storage_key_prefix;

    #[test]
    fn archive_storage_key_prefix_appends_a_slash_only_if_missing() {
        assert_eq!(
            archive_storage_key_prefix("prefix/dataset").as_str(),
            "prefix/dataset/"
        );
        assert_eq!(
            archive_storage_key_prefix("prefix/dataset/").as_str(),
            "prefix/dataset/"
        );
    }
}
