//! The archive garbage collector, which deletes the archives whose datasets' retention periods have
//! expired, and reclaims the metadata of deleted datasets.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::SystemTime;

use clp_rust_utils::aws::AWS_DEFAULT_REGION;
use clp_rust_utils::clp_config::S3Config;
use clp_rust_utils::clp_config::package::config::ArchiveOutputStorage;
use clp_rust_utils::clp_config::package::config::Config;
use clp_rust_utils::clp_config::package::config::Database as DatabaseConfig;
use clp_rust_utils::database::mysql::archive;
use clp_rust_utils::database::mysql::dataset;
use clp_rust_utils::database::mysql::query_job;
use clp_rust_utils::dataset::DatasetId;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::job_config::QueryJobStatus;
use clp_rust_utils::types::ArchiveId;
use clp_rust_utils::types::UuidBytes;
use non_empty_string::NonEmptyString;

use crate::deletion_candidates::DeletionCandidatesBuffer;

/// The archive garbage collector.
pub struct ArchiveGarbageCollector {
    db_pool: sqlx::MySqlPool,
    database_config: DatabaseConfig,
    archive_storage: ArchiveOutputStorage,
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
            archive_storage: config.archive_output.storage,
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
    /// repeatedly until a sweep fails.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::sweep`]'s return values on failure.
    pub async fn run(&self) -> anyhow::Result<()> {
        tracing::info!("Archive garbage collector started.");
        loop {
            self.sweep().await?;
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
    /// The creation time of the earliest created running query job if it's earlier than now, or now
    /// otherwise, in milliseconds since the UNIX epoch, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`SystemTime::duration_since`]'s return values on failure.
    /// * Forwards [`i64::try_from`]'s return values on failure.
    /// * Forwards [`query_job::get_earliest_creation_time_millis`]'s return values on failure.
    async fn get_expiry_base_millis(&self) -> anyhow::Result<i64> {
        let now_millis = i64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_millis(),
        )?;
        let earliest_running_job_creation_time_millis =
            query_job::get_earliest_creation_time_millis(&self.db_pool, QueryJobStatus::Running)
                .await
                .inspect_err(|e| {
                    tracing::error!(error = % e, "Failed to look up the running query jobs.");
                })?;

        if let Some(creation_time_millis) = earliest_running_job_creation_time_millis
            && creation_time_millis < now_millis
        {
            tracing::debug!("Discovered a running query job created at {creation_time_millis}.");
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
            for archive in expired_archives {
                archive_ids_by_dataset_id
                    .entry(archive.dataset_id)
                    .or_default()
                    .push(archive.id);
                candidates_buffer.add_candidate(format!(
                    "{}/{}",
                    archive.dataset,
                    ArchiveId::from_bytes(archive.uuid)
                ));
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

        let candidates = candidates_buffer.candidates();
        if candidates.is_empty() {
            tracing::debug!(
                "No archives matched the expiry criteria: `creation_time_millis < \
                 {expiry_base_millis} - dataset's retention period`."
            );
            return Ok(());
        }
        self.delete_from_storage(candidates)
            .await
            .inspect_err(|e| {
                tracing::error!(
                    error = % e,
                    "Failed to delete the expired archives from the archive storage."
                );
            })?;
        tracing::info!("Deleted {} archive(s): {candidates:?}.", candidates.len());

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
             `datasets`.`name` AS `dataset` FROM `{archives_table}` AS `archives` JOIN \
             `{datasets_table}` AS `datasets` ON `archives`.`dataset_id` = `datasets`.`id` WHERE \
             `datasets`.`is_deleted` = FALSE AND `datasets`.`retention_period_minutes` IS NOT \
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
    async fn delete_from_storage(&self, candidates: &BTreeSet<String>) -> anyhow::Result<()> {
        match &self.archive_storage {
            ArchiveOutputStorage::Fs { directory } => {
                delete_from_fs(Path::new(directory), candidates)?;
            }
            ArchiveOutputStorage::S3 { s3_config, .. } => {
                delete_from_s3(s3_config, candidates).await?;
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

/// An archive whose dataset's retention period has expired.
#[derive(sqlx::FromRow)]
struct ExpiredArchive {
    id: u64,
    #[sqlx(try_from = "Vec<u8>")]
    uuid: UuidBytes,
    dataset_id: DatasetId,
    dataset: DatasetName,
}

/// Deletes the file or directory at each of `candidates`, relative to `directory`, if it exists.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`std::fs::remove_dir_all`]'s return values on failure.
/// * Forwards [`std::fs::remove_file`]'s return values on failure.
fn delete_from_fs(directory: &Path, candidates: &BTreeSet<String>) -> std::io::Result<()> {
    for candidate in candidates {
        let path = directory.join(candidate);
        if !path.exists() {
            continue;
        }
        if path.is_dir() {
            std::fs::remove_dir_all(&path)?;
        } else {
            std::fs::remove_file(&path)?;
        }
    }
    Ok(())
}

/// Deletes the objects whose keys are each of `candidates` prefixed by the S3 key prefix.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`clp_rust_utils::s3::delete_by_keys`]'s return values on failure.
async fn delete_from_s3(
    s3_config: &S3Config,
    candidates: &BTreeSet<String>,
) -> Result<(), clp_rust_utils::s3::DeleteError> {
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
    let keys: Vec<String> = candidates
        .iter()
        .map(|candidate| format!("{}{candidate}", s3_config.key_prefix))
        .collect();
    clp_rust_utils::s3::delete_by_keys(&client, s3_config.bucket.as_str(), &keys).await
}
