//! The archive garbage collector, which marks expired archives as deleted and cleans up the
//! datasets and archives marked as deleted.

use std::collections::HashMap;
use std::collections::HashSet;
use std::io::ErrorKind;
use std::path::Path;

use clp_metadata_db::ArchiveRowId;
use clp_metadata_db::DatasetId;
use clp_metadata_db::MarkedArchive;
use clp_metadata_db::MetadataTables;
use clp_rust_utils::aws::AWS_DEFAULT_REGION;
use clp_rust_utils::clp_config::package::config::ArchiveOutputStorage;
use clp_rust_utils::clp_config::package::config::Config;
use clp_rust_utils::clp_config::package::config::StorageEngine;
use clp_rust_utils::database::mysql::query_job;
use clp_rust_utils::job_config::QueryJobStatus;
use clp_rust_utils::types::non_empty_string::ExpectedNonEmpty;
use non_empty_string::NonEmptyString;
use sqlx::Connection;

use crate::Error;
use crate::error::StorageError;

/// The archive garbage collector of a clp-s package.
pub struct ArchiveGarbageCollector {
    db_pool: sqlx::MySqlPool,
    metadata_tables: MetadataTables,
    storage: Storage,
}

impl ArchiveGarbageCollector {
    /// Factory function.
    ///
    /// Creates an archive garbage collector of the metadata tables and the archive storage in
    /// `config`, connecting to the CLP DB through `db_pool`.
    ///
    /// # Returns
    ///
    /// A newly created archive garbage collector on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::UnsupportedStorageEngine`] if the package doesn't use the clp-s storage engine.
    pub async fn from_config(config: &Config, db_pool: sqlx::MySqlPool) -> Result<Self, Error> {
        if StorageEngine::ClpS != config.package.storage_engine {
            return Err(Error::UnsupportedStorageEngine);
        }
        Ok(Self {
            db_pool,
            metadata_tables: MetadataTables::new(&config.database),
            storage: Storage::from_config(&config.archive_output.storage).await,
        })
    }

    /// Runs one sweep: marks expired archives as deleted, then cleans up the marked datasets, and
    /// then the marked archives.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::Pool::acquire`]'s return values on failure.
    /// * Forwards [`Self::mark_expired_archives`]'s return values on failure.
    /// * Forwards [`Self::clean_up_marked_datasets`]'s return values on failure.
    /// * Forwards [`Self::clean_up_marked_archives`]'s return values on failure.
    pub async fn sweep(&self) -> Result<(), Error> {
        let mut conn = self.db_pool.acquire().await?;
        self.mark_expired_archives(&mut conn).await?;
        self.clean_up_marked_datasets(&mut conn).await?;
        self.clean_up_marked_archives(&mut conn).await
    }

    /// Marks expired archives as deleted, measuring retention periods back from the earlier of the
    /// DB's current time and the earliest non-terminal query job's creation.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`query_job::get_earliest_creation_timestamp_millisecs`]'s return values on
    ///   failure.
    /// * Forwards [`MetadataTables::mark_expired_archives_deleted`]'s return values on failure.
    async fn mark_expired_archives(&self, conn: &mut sqlx::MySqlConnection) -> Result<(), Error> {
        const NON_TERMINAL_STATUSES: [QueryJobStatus; 4] = [
            QueryJobStatus::Pending,
            QueryJobStatus::Running,
            QueryJobStatus::CancelRequested,
            QueryJobStatus::Cancelling,
        ];

        let earliest_query_job_creation_timestamp_millisecs =
            query_job::get_earliest_creation_timestamp_millisecs(conn, &NON_TERMINAL_STATUSES)
                .await?;
        let num_marked_archives = self
            .metadata_tables
            .mark_expired_archives_deleted(conn, earliest_query_job_creation_timestamp_millisecs)
            .await?;
        if 0 != num_marked_archives {
            tracing::info!(
                num_marked_archives = num_marked_archives,
                "Marked expired archives as deleted."
            );
        }
        Ok(())
    }

    /// Deletes each marked dataset from the archive storage, and then from the metadata tables.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`MetadataTables::get_marked_datasets`]'s return values on failure.
    /// * Forwards [`sqlx::Connection::begin`]'s return values on failure.
    /// * Forwards [`MetadataTables::delete_marked_dataset`]'s return values on failure.
    /// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
    async fn clean_up_marked_datasets(
        &self,
        conn: &mut sqlx::MySqlConnection,
    ) -> Result<(), Error> {
        for dataset in self.metadata_tables.get_marked_datasets(conn).await? {
            if let Err(e) = self
                .storage
                .delete_dataset(&dataset.archive_storage_path)
                .await
            {
                tracing::error!(
                    dataset_id = ? dataset.id,
                    archive_storage_path = % dataset.archive_storage_path,
                    error = % e,
                    "Failed to delete a marked dataset from the archive storage."
                );
                continue;
            }
            let mut tx = conn.begin().await?;
            self.metadata_tables
                .delete_marked_dataset(&mut tx, dataset.id)
                .await?;
            tx.commit().await?;
            tracing::info!(dataset_id = ? dataset.id, "Deleted a marked dataset.");
        }
        Ok(())
    }

    /// Deletes the marked archives of live datasets from the archive storage, and then from the
    /// metadata tables, in batches.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`MetadataTables::get_marked_archives`]'s return values on failure.
    /// * Forwards [`sqlx::Connection::begin`]'s return values on failure.
    /// * Forwards [`MetadataTables::delete_marked_archives`]'s return values on failure.
    /// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
    async fn clean_up_marked_archives(
        &self,
        conn: &mut sqlx::MySqlConnection,
    ) -> Result<(), Error> {
        const MAX_NUM_ARCHIVES_PER_BATCH: usize = 1000;

        let mut attempted_ids = HashSet::new();
        loop {
            let mut archives_by_dataset_id: HashMap<DatasetId, Vec<MarkedArchive>> = HashMap::new();
            for archive in self
                .metadata_tables
                .get_marked_archives(conn, MAX_NUM_ARCHIVES_PER_BATCH)
                .await?
            {
                if attempted_ids.insert(archive.id) {
                    archives_by_dataset_id
                        .entry(archive.dataset_id)
                        .or_default()
                        .push(archive);
                }
            }
            if archives_by_dataset_id.is_empty() {
                return Ok(());
            }

            for (dataset_id, archives) in archives_by_dataset_id {
                let deleted_ids = self.storage.delete_archives(archives).await;
                if deleted_ids.is_empty() {
                    continue;
                }
                let mut tx = conn.begin().await?;
                let num_deleted_archives = self
                    .metadata_tables
                    .delete_marked_archives(&mut tx, dataset_id, &deleted_ids)
                    .await?;
                tx.commit().await?;
                tracing::info!(
                    dataset_id = ? dataset_id,
                    num_deleted_archives = num_deleted_archives,
                    "Deleted marked archives."
                );
            }
        }
    }
}

/// The archive storage that datasets and archives are deleted from.
enum Storage {
    Fs,
    S3 {
        client: aws_sdk_s3::Client,
        bucket: NonEmptyString,
    },
}

impl Storage {
    /// Factory function.
    ///
    /// Creates the storage of the given archive output. For S3 storage, also creates the S3 client
    /// that deletes the objects.
    ///
    /// # Returns
    ///
    /// The newly created storage.
    async fn from_config(archive_output_storage: &ArchiveOutputStorage) -> Self {
        match archive_output_storage {
            ArchiveOutputStorage::Fs { .. } => Self::Fs,
            ArchiveOutputStorage::S3 { s3_config, .. } => Self::S3 {
                client: clp_rust_utils::s3::create_new_client(
                    s3_config
                        .region_code
                        .as_ref()
                        .map_or(AWS_DEFAULT_REGION, NonEmptyString::as_str),
                    s3_config.endpoint_url.as_ref(),
                    &s3_config.aws_authentication,
                )
                .await,
                bucket: s3_config.bucket.clone(),
            },
        }
    }

    /// Deletes everything under a dataset's archive storage path.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`remove_fs_path`]'s return values on failure.
    /// * Forwards [`clp_rust_utils::s3::delete_by_key_prefix`]'s return values on failure.
    async fn delete_dataset(&self, archive_storage_path: &str) -> Result<(), StorageError> {
        match self {
            Self::Fs => remove_fs_path(Path::new(archive_storage_path)).await?,
            Self::S3 { client, bucket } => {
                clp_rust_utils::s3::delete_by_key_prefix(
                    client,
                    bucket.as_str(),
                    &NonEmptyString::from_string(format!("{archive_storage_path}/")),
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Deletes a dataset's archives from the archive storage.
    ///
    /// # Returns
    ///
    /// The row IDs of the archives deleted from the archive storage.
    async fn delete_archives(&self, archives: Vec<MarkedArchive>) -> Vec<ArchiveRowId> {
        match self {
            Self::Fs => {
                let mut deleted_ids = Vec::new();
                for archive in archives {
                    let path =
                        Path::new(&archive.archive_storage_path).join(archive.uuid.to_string());
                    match remove_fs_path(&path).await {
                        Ok(()) => deleted_ids.push(archive.id),
                        Err(e) => tracing::error!(
                            dataset_id = ? archive.dataset_id,
                            archive_id = % archive.uuid,
                            path = ? path,
                            error = % e,
                            "Failed to delete a marked archive from the archive storage."
                        ),
                    }
                }
                deleted_ids
            }
            Self::S3 { client, bucket } => {
                let keys = archives
                    .iter()
                    .map(|archive| format!("{}/{}", archive.archive_storage_path, archive.uuid))
                    .collect();
                match clp_rust_utils::s3::delete_by_keys(client, bucket.as_str(), keys).await {
                    Ok(()) => archives.into_iter().map(|archive| archive.id).collect(),
                    Err(e) => {
                        tracing::error!(
                            archive_ids = ? archives
                                .iter()
                                .map(|archive| archive.uuid.to_string())
                                .collect::<Vec<_>>(),
                            error = % e,
                            "Failed to delete marked archives from the archive storage."
                        );
                        Vec::new()
                    }
                }
            }
        }
    }
}

/// Removes the directory or file at `path`, if anything exists there.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`tokio::fs::symlink_metadata`]'s return values on failure, unless nothing exists at
///   `path`.
/// * Forwards [`tokio::fs::remove_dir_all`]'s return values on failure, unless nothing exists at
///   `path`.
/// * Forwards [`tokio::fs::remove_file`]'s return values on failure, unless nothing exists at
///   `path`.
async fn remove_fs_path(path: &Path) -> std::io::Result<()> {
    let result = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.is_dir() => tokio::fs::remove_dir_all(path).await,
        Ok(_) => tokio::fs::remove_file(path).await,
        Err(e) => Err(e),
    };
    match result {
        Err(e) if ErrorKind::NotFound == e.kind() => Ok(()),
        result => result,
    }
}
