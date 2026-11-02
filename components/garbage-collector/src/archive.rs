//! The archive garbage collector, which deletes the archives whose datasets' retention periods have
//! expired, and reclaims the metadata of deleted datasets.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use std::time::SystemTime;

use clp_rust_utils::aws::AWS_DEFAULT_REGION;
use clp_rust_utils::clp_config::S3Config;
use clp_rust_utils::clp_config::package::config::ArchiveOutput;
use clp_rust_utils::clp_config::package::config::ArchiveOutputStorage;
use clp_rust_utils::clp_config::package::config::Config;
use clp_rust_utils::clp_config::package::config::Database as DatabaseConfig;
use clp_rust_utils::database::mysql::archive;
use clp_rust_utils::database::mysql::archive::Archive;
use clp_rust_utils::database::mysql::dataset;
use clp_rust_utils::database::mysql::query_job;
use clp_rust_utils::dataset::DatasetId;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::job_config::QueryJobStatus;
use clp_rust_utils::s3::DeleteError;
use clp_rust_utils::types::ArchiveId;
use non_empty_string::NonEmptyString;

use crate::deletion_candidates::DeletionCandidatesBuffer;

/// The archive garbage collector.
pub struct ArchiveGarbageCollector {
    db_pool: sqlx::MySqlPool,
    database_config: DatabaseConfig,
    archive_output: ArchiveOutput,
    recovery_file_path: PathBuf,
    sweep_interval: Duration,
}

impl ArchiveGarbageCollector {
    /// Factory function.
    ///
    /// Creates an archive garbage collector of the archive storage and the CLP DB in `config`,
    /// connecting to the CLP DB through `db_pool`.
    ///
    /// # Returns
    ///
    /// A newly created archive garbage collector.
    #[must_use]
    pub fn new(config: Config, db_pool: sqlx::MySqlPool) -> Self {
        const RECOVERY_FILE_NAME: &str = "archive-garbage-collector.tmp";
        const SECS_PER_MINUTE: u64 = 60;

        Self {
            db_pool,
            database_config: config.database,
            archive_output: config.archive_output,
            recovery_file_path: Path::new(&config.tmp_directory).join(RECOVERY_FILE_NAME),
            sweep_interval: Duration::from_secs(
                config
                    .garbage_collector
                    .sweep_interval
                    .archive_minutes
                    .get()
                    .saturating_mul(SECS_PER_MINUTE),
            ),
        }
    }

    /// Sweeps the CLP DB and the archive storage, and then sleeps for the sweep interval,
    /// repeatedly.
    ///
    /// # Returns
    ///
    /// Never returns.
    pub async fn run(&self) -> Infallible {
        tracing::info!("Archive garbage collector started.");
        loop {
            if let Err(e) = self.sweep().await {
                tracing::error!(error = % e, "Archive garbage collector failed to sweep.");
            }
            tokio::time::sleep(self.sweep_interval).await;
        }
    }

    /// Deletes the expired archives, and then reclaims the metadata of deleted datasets.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::get_expiry_base_millis`]'s return values on failure.
    /// * Forwards [`Self::delete_expired_archives`]'s return values on failure.
    /// * Forwards [`Self::reclaim_deleted_datasets`]'s return values on failure.
    async fn sweep(&self) -> anyhow::Result<()> {
        let expiry_base_millis = self.get_expiry_base_millis().await?;
        self.delete_expired_archives(expiry_base_millis).await?;
        self.reclaim_deleted_datasets().await
    }

    /// Gets the time from which each dataset's retention period is subtracted to find the
    /// dataset's expired archives.
    ///
    /// # Returns
    ///
    /// The creation time of the earliest created query job that isn't in a terminal status if it's
    /// earlier than now, or now otherwise, in milliseconds since the UNIX epoch, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`SystemTime::duration_since`]'s return values on failure.
    /// * Forwards [`i64::try_from`]'s return values on failure.
    /// * Forwards [`query_job::get_earliest_creation_time_millis`]'s return values on failure.
    async fn get_expiry_base_millis(&self) -> anyhow::Result<i64> {
        const NON_TERMINAL_STATUSES: [QueryJobStatus; 4] = [
            QueryJobStatus::Pending,
            QueryJobStatus::Running,
            QueryJobStatus::CancelRequested,
            QueryJobStatus::Cancelling,
        ];

        let now_millis = i64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_millis(),
        )?;
        let earliest_job_creation_time_millis =
            query_job::get_earliest_creation_time_millis(&self.db_pool, &NON_TERMINAL_STATUSES)
                .await
                .inspect_err(|e| {
                    tracing::error!(error = % e, "Failed to look up the non-terminal query jobs.");
                })?;

        if let Some(creation_time_millis) = earliest_job_creation_time_millis
            && creation_time_millis < now_millis
        {
            tracing::debug!(
                "Discovered a non-terminal query job created at {creation_time_millis}."
            );
            tracing::debug!("Using adjusted expiry base {creation_time_millis}.");
            return Ok(creation_time_millis);
        }
        tracing::debug!("Using expiry base {now_millis}.");
        Ok(now_millis)
    }

    /// Deletes the metadata of the archives that expired before `expiry_base_millis`, and then
    /// deletes them, along with the candidates left in the recovery file, from the archive storage.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`DeletionCandidatesBuffer::new`]'s return values on failure.
    /// * Forwards [`Self::fetch_expired_archives`]'s return values on failure.
    /// * Forwards [`DeletionCandidatesBuffer::persist_new_candidates`]'s return values on failure.
    /// * Forwards [`Self::delete_archive_metadata`]'s return values on failure.
    /// * Forwards [`Self::delete_from_storage`]'s return values on failure.
    /// * Forwards [`DeletionCandidatesBuffer::clear`]'s return values on failure.
    async fn delete_expired_archives(&self, expiry_base_millis: i64) -> anyhow::Result<()> {
        let mut candidates_buffer = DeletionCandidatesBuffer::new(self.recovery_file_path.clone())
            .inspect_err(|e| {
                tracing::error!(error = % e, "Failed to read the recovery file.");
            })?;

        let expired_archives = self
            .fetch_expired_archives(expiry_base_millis)
            .await
            .inspect_err(|e| {
                tracing::error!(
                    error = % e,
                    "Failed to fetch the expired archives from the database."
                );
            })?;
        if !expired_archives.is_empty() {
            let mut archive_ids_by_dataset_id: BTreeMap<DatasetId, Vec<u64>> = BTreeMap::new();
            for expired_archive in expired_archives {
                candidates_buffer.add_candidate(
                    Candidate {
                        dataset: expired_archive.dataset_name,
                        archive_id: expired_archive.archive.uuid,
                    }
                    .to_string(),
                );
                archive_ids_by_dataset_id
                    .entry(expired_archive.dataset_id)
                    .or_default()
                    .push(expired_archive.archive.id);
            }
            candidates_buffer
                .persist_new_candidates()
                .inspect_err(|e| {
                    tracing::error!(error = % e, "Failed to write the recovery file.");
                })?;
            self.delete_archive_metadata(&archive_ids_by_dataset_id)
                .await
                .inspect_err(|e| {
                    tracing::error!(
                        error = % e,
                        "Failed to delete the expired archives from the database."
                    );
                })?;
        }

        if candidates_buffer.candidates().is_empty() {
            tracing::debug!(
                "No archives matched the expiry criteria: `creation_time_millis < \
                 {expiry_base_millis} - dataset's retention period`."
            );
            return Ok(());
        }
        let candidates: Vec<Candidate> = candidates_buffer
            .candidates()
            .iter()
            .filter_map(|candidate| {
                candidate
                    .parse()
                    .inspect_err(|e| {
                        tracing::warn!(
                            candidate = % candidate,
                            error = % e,
                            "Failed to parse a deletion candidate; dropping it."
                        );
                    })
                    .ok()
            })
            .collect();
        self.delete_from_storage(&candidates)
            .await
            .inspect_err(|e| {
                tracing::error!(
                    error = % e,
                    "Failed to delete the expired archives from the archive storage."
                );
            })?;
        tracing::info!(
            "Deleted {} archive(s): {:?}.",
            candidates.len(),
            candidates
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        );

        candidates_buffer.clear().inspect_err(|e| {
            tracing::error!(error = % e, "Failed to delete the recovery file.");
        })?;
        Ok(())
    }

    /// # Returns
    ///
    /// The archives of the datasets that aren't deleted and have a retention period, which were
    /// created earlier than `expiry_base_millis` minus their dataset's retention period, on
    /// success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryAs::fetch_all`]'s return values on failure.
    async fn fetch_expired_archives(
        &self,
        expiry_base_millis: i64,
    ) -> Result<Vec<ExpiredArchive>, sqlx::Error> {
        const MILLISECS_PER_MINUTE: i64 = 60 * 1000;

        let archives_table = self.database_config.unscoped_archives_table_name();
        let datasets_table = self.database_config.datasets_table_name();
        sqlx::query_as(&format!(
            "SELECT `archives`.`id`, `archives`.`uuid`, `archives`.`dataset_id`, \
             `datasets`.`name` AS `dataset_name` FROM `{archives_table}` AS `archives` JOIN \
             `{datasets_table}` AS `datasets` ON `archives`.`dataset_id` = `datasets`.`id` WHERE \
             `datasets`.`deleted_at_millis` = 0 AND `datasets`.`retention_period_minutes` IS NOT \
             NULL AND `archives`.`creation_time_millis` < ? - \
             CAST(`datasets`.`retention_period_minutes` AS SIGNED) * {MILLISECS_PER_MINUTE}"
        ))
        .bind(expiry_base_millis)
        .fetch_all(&self.db_pool)
        .await
    }

    /// Deletes the metadata of archives, grouped by the IDs of their datasets, in one transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::Pool::begin`]'s return values on failure.
    /// * Forwards [`archive::delete`]'s return values on failure.
    /// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
    async fn delete_archive_metadata(
        &self,
        archive_ids_by_dataset_id: &BTreeMap<DatasetId, Vec<u64>>,
    ) -> Result<(), clp_rust_utils::Error> {
        let mut tx = self.db_pool.begin().await?;
        for (dataset_id, archive_ids) in archive_ids_by_dataset_id {
            archive::delete(&mut tx, &self.database_config, *dataset_id, archive_ids).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Deletes `candidates` from the archive storage.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`delete_from_fs`]'s return values on failure.
    /// * Forwards [`delete_from_s3`]'s return values on failure.
    async fn delete_from_storage(&self, candidates: &[Candidate]) -> anyhow::Result<()> {
        match &self.archive_output.storage {
            ArchiveOutputStorage::Fs { .. } => {
                let paths: Vec<PathBuf> = candidates
                    .iter()
                    .map(|candidate| {
                        Path::new(
                            &self
                                .archive_output
                                .dataset_archive_storage_directory(&candidate.dataset),
                        )
                        .join(candidate.archive_id.to_string())
                    })
                    .collect();
                delete_from_fs(&paths).await?;
            }
            ArchiveOutputStorage::S3 { s3_config, .. } => {
                let keys: Vec<NonEmptyString> = candidates
                    .iter()
                    .map(|candidate| {
                        self.archive_output
                            .dataset_archive_object_key(&candidate.dataset, &candidate.archive_id)
                    })
                    .collect();
                delete_from_s3(s3_config, &keys).await?;
            }
        }
        Ok(())
    }

    /// Deletes the rows of every deleted dataset from the archives, long-span archives and
    /// column-metadata tables.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`dataset::get_deleted_ids`]'s return values on failure.
    /// * Forwards [`Self::delete_dataset_metadata`]'s return values on failure.
    async fn reclaim_deleted_datasets(&self) -> anyhow::Result<()> {
        let dataset_ids = dataset::get_deleted_ids(&self.db_pool, &self.database_config)
            .await
            .inspect_err(|e| {
                tracing::error!(
                    error = % e,
                    "Failed to fetch the deleted datasets from the database."
                );
            })?;
        for dataset_id in dataset_ids {
            let num_deleted_rows =
                self.delete_dataset_metadata(dataset_id)
                    .await
                    .inspect_err(|e| {
                        tracing::error!(
                            error = % e,
                            "Failed to reclaim the metadata of deleted dataset {dataset_id}."
                        );
                    })?;
            if 0 != num_deleted_rows {
                tracing::info!(
                    "Reclaimed {num_deleted_rows} metadata row(s) of deleted dataset {dataset_id}."
                );
            }
        }
        Ok(())
    }

    /// Deletes a dataset's rows from the archives, long-span archives and column-metadata tables in
    /// one transaction.
    ///
    /// # Returns
    ///
    /// The number of deleted rows on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::Pool::begin`]'s return values on failure.
    /// * Forwards [`dataset::delete_metadata`]'s return values on failure.
    /// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
    async fn delete_dataset_metadata(
        &self,
        dataset_id: DatasetId,
    ) -> Result<u64, clp_rust_utils::Error> {
        let mut tx = self.db_pool.begin().await?;
        let num_deleted_rows =
            dataset::delete_metadata(&mut tx, &self.database_config, dataset_id).await?;
        tx.commit().await?;
        Ok(num_deleted_rows)
    }
}

/// An archive that has outlived its dataset's retention period.
#[derive(sqlx::FromRow)]
struct ExpiredArchive {
    #[sqlx(flatten)]
    archive: Archive,
    dataset_id: DatasetId,
    dataset_name: DatasetName,
}

/// An archive to delete from the archive storage.
#[derive(Debug)]
struct Candidate {
    dataset: DatasetName,
    archive_id: ArchiveId,
}

impl Display for Candidate {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}/{}", self.dataset, self.archive_id)
    }
}

impl FromStr for Candidate {
    type Err = anyhow::Error;

    /// Parses a candidate formatted as `<dataset name>/<archive ID>`.
    ///
    /// # Returns
    ///
    /// The parsed candidate on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if `candidate` doesn't contain a `/`.
    /// * Forwards [`DatasetName::from_str`]'s return values on failure.
    /// * Forwards [`ArchiveId::from_str`]'s return values on failure.
    fn from_str(candidate: &str) -> Result<Self, Self::Err> {
        let Some((dataset, archive_id)) = candidate.split_once('/') else {
            anyhow::bail!("deletion candidate isn't formatted as `<dataset name>/<archive ID>`");
        };
        Ok(Self {
            dataset: dataset.parse()?,
            archive_id: archive_id.parse()?,
        })
    }
}

/// Deletes the file or directory at each of `paths`, if it exists.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`tokio::fs::metadata`]'s return values on failure, unless nothing exists at the
///   path.
/// * Forwards [`tokio::fs::remove_dir_all`]'s return values on failure.
/// * Forwards [`tokio::fs::remove_file`]'s return values on failure.
async fn delete_from_fs(paths: &[PathBuf]) -> std::io::Result<()> {
    for path in paths {
        let metadata = match tokio::fs::metadata(path).await {
            Ok(metadata) => metadata,
            Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {
                continue;
            }
            Err(e) => return Err(e),
        };
        if metadata.is_dir() {
            tokio::fs::remove_dir_all(path).await?;
        } else {
            tokio::fs::remove_file(path).await?;
        }
    }
    Ok(())
}

/// Deletes every object in `s3_config`'s bucket whose key is in `keys`.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`clp_rust_utils::s3::delete_by_keys`]'s return values on failure.
async fn delete_from_s3(s3_config: &S3Config, keys: &[NonEmptyString]) -> Result<(), DeleteError> {
    let client = clp_rust_utils::s3::create_new_client(
        s3_config
            .region_code
            .as_ref()
            .map_or(AWS_DEFAULT_REGION, NonEmptyString::as_str),
        s3_config.endpoint_url.as_ref(),
        &s3_config.aws_authentication,
    )
    .await;
    clp_rust_utils::s3::delete_by_keys(&client, s3_config.bucket.as_str(), keys).await
}

#[cfg(test)]
mod tests {
    use clp_rust_utils::clp_config::package::config::ArchiveOutput;
    use clp_rust_utils::clp_config::package::config::ArchiveOutputStorage;
    use clp_rust_utils::clp_config::package::config::Config;
    use clp_rust_utils::types::ArchiveId;

    use super::ArchiveGarbageCollector;
    use super::Candidate;

    const ARCHIVE_ID: &str = "018e90e5-8b2a-4a61-a2fc-cac799936caf";

    #[test]
    fn candidate_round_trips_through_its_dataset_name_and_archive_id() -> anyhow::Result<()> {
        let formatted_candidate = format!("logs/{ARCHIVE_ID}");

        let candidate: Candidate = formatted_candidate.parse()?;
        assert_eq!(candidate.dataset.as_str(), "logs");
        assert_eq!(candidate.archive_id, ARCHIVE_ID.parse::<ArchiveId>()?);
        assert_eq!(candidate.to_string(), formatted_candidate);
        Ok(())
    }

    #[test]
    fn candidate_without_a_dataset_name_is_rejected() {
        assert_eq!(
            ARCHIVE_ID
                .parse::<Candidate>()
                .expect_err("a candidate without a dataset name should be rejected")
                .to_string(),
            "deletion candidate isn't formatted as `<dataset name>/<archive ID>`"
        );
    }

    #[tokio::test]
    async fn fs_deletion_deletes_existing_files_and_directories_and_skips_missing_ones()
    -> anyhow::Result<()> {
        const OTHER_ARCHIVE_IDS: [&str; 4] = [
            "018e90e5-8b2a-4a61-a2fc-cac799936ca0",
            "018e90e5-8b2a-4a61-a2fc-cac799936ca1",
            "018e90e5-8b2a-4a61-a2fc-cac799936ca2",
            "018e90e5-8b2a-4a61-a2fc-cac799936ca3",
        ];

        let directory = tempfile::tempdir()?;
        let garbage_collector = ArchiveGarbageCollector::new(
            Config {
                archive_output: ArchiveOutput {
                    storage: ArchiveOutputStorage::Fs {
                        directory: directory.path().to_string_lossy().into_owned(),
                    },
                    ..ArchiveOutput::default()
                },
                ..Config::default()
            },
            sqlx::mysql::MySqlPoolOptions::new().connect_lazy("mysql://127.0.0.1:1/clp-db")?,
        );
        let dataset_directory = directory.path().join("logs");
        let directory_archive = dataset_directory.join(ARCHIVE_ID);
        std::fs::create_dir_all(directory_archive.join("nested"))?;
        std::fs::write(directory_archive.join("nested").join("segment"), b"data")?;
        let file_archive = dataset_directory.join(OTHER_ARCHIVE_IDS[0]);
        std::fs::write(&file_archive, b"data")?;
        let kept_archive = dataset_directory.join(OTHER_ARCHIVE_IDS[1]);
        std::fs::write(&kept_archive, b"data")?;
        std::fs::write(directory.path().join("file"), b"data")?;

        // The candidates that don't exist come first, so skipping them mustn't stop the deletion.
        garbage_collector
            .delete_from_storage(&[
                format!("file/{}", OTHER_ARCHIVE_IDS[2]).parse()?,
                format!("logs/{}", OTHER_ARCHIVE_IDS[3]).parse()?,
                format!("logs/{ARCHIVE_ID}").parse()?,
                format!("logs/{}", OTHER_ARCHIVE_IDS[0]).parse()?,
            ])
            .await?;
        assert!(!directory_archive.exists());
        assert!(!file_archive.exists());
        assert!(kept_archive.is_file());
        assert!(directory.path().join("file").is_file());
        Ok(())
    }
}
