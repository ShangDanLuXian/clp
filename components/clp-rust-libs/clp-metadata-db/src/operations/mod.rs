//! The operation traits, grouped by entity and by whether they write.

mod archive;
mod dataset;
mod statistics;

use std::collections::HashMap;
use std::num::NonZeroU32;

use async_trait::async_trait;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::job_config::CompressionJobId;
use clp_rust_utils::job_config::TimeRange;
use clp_rust_utils::types::ArchiveId;
use clp_rust_utils::types::ArchiveUuid;
use clp_rust_utils::types::DatasetId;

use crate::error::Error;
use crate::types::ArchiveDescriptor;
use crate::types::ArchiveMetadata;
use crate::types::ArchiveTimestampRange;
use crate::types::ColumnType;
use crate::types::Dataset;
use crate::types::DatasetDescriptor;
use crate::types::QueryArchive;
use crate::types::SpaceSavings;

/// Reads of datasets. Every read skips datasets marked as deleted.
#[async_trait]
pub trait DatasetLookup {
    /// # Returns
    ///
    /// The live datasets, ordered by name, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::InvalidStoredRetentionPeriod`] if a dataset's stored retention period is zero.
    /// * [`Error::Sqlx`] if a statement fails or a row can't be decoded.
    async fn get_datasets(&mut self) -> Result<Vec<Dataset>, Error>;

    /// # Parameters
    ///
    /// * `name` - The name of the dataset to read.
    ///
    /// # Returns
    ///
    /// The live dataset named `name`, or `None` if there is none, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::InvalidStoredRetentionPeriod`] if the dataset's stored retention period is zero.
    /// * [`Error::Sqlx`] if a statement fails or a row can't be decoded.
    async fn get_dataset_by_name(&mut self, name: &DatasetName) -> Result<Option<Dataset>, Error>;

    /// # Parameters
    ///
    /// * `names` - The names of the datasets to resolve.
    ///
    /// # Returns
    ///
    /// The IDs of the live datasets among `names`, keyed by name, on success. Names that don't
    /// resolve are absent.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::Sqlx`] if a statement fails or a row can't be decoded.
    async fn get_dataset_ids_by_names(
        &mut self,
        names: &[DatasetName],
    ) -> Result<HashMap<DatasetName, DatasetId>, Error>;
}

/// Writes to datasets. Implemented only for [`crate::ClpDbTransaction`]: the guarantees below last
/// until that transaction ends.
#[async_trait]
pub trait DatasetManagement {
    /// Creates the dataset described by `descriptor`, or returns the existing live dataset with the
    /// same name. Concurrent callers with the same name all get the same ID. An existing dataset's
    /// settings must match the descriptor's.
    ///
    /// Until this transaction ends, no other transaction can mark the dataset as deleted.
    ///
    /// If a concurrent creator of the same new name rolls back while others wait on its insert, the
    /// server can pick this transaction as a deadlock victim (error 1213, as [`Error::Sqlx`]).
    /// Retrying the transaction is safe.
    ///
    /// # Parameters
    ///
    /// * `descriptor` - The name and settings of the dataset.
    ///
    /// # Returns
    ///
    /// The ID of the new or existing dataset on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::DatasetDeleted`] if the dataset with the descriptor's name is marked as deleted.
    /// * [`Error::DatasetConfigMismatch`] if the dataset exists with other settings than the
    ///   descriptor's.
    /// * [`Error::DatasetNotFound`] if a concurrent creator's row is gone by the time it's read
    ///   back. It can't happen while dataset rows are never removed.
    /// * [`Error::InvalidStoredRetentionPeriod`] if the existing dataset's stored retention period
    ///   is zero.
    /// * [`Error::Sqlx`] if a statement fails (including a deadlock, see above) or a row can't be
    ///   decoded.
    async fn create_or_get_dataset(
        &mut self,
        descriptor: &DatasetDescriptor,
    ) -> Result<DatasetId, Error>;

    /// Marks the dataset named `name` as deleted.
    ///
    /// Archive inserts into the dataset by other transactions wait until this transaction ends,
    /// then fail, so commit promptly. Don't call it in a transaction that has already used the same
    /// dataset in another operation: the lock upgrade can deadlock.
    ///
    /// # Parameters
    ///
    /// * `name` - The name of the dataset to mark.
    ///
    /// # Returns
    ///
    /// The marked dataset's ID on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::DatasetNotFound`] if no dataset is named `name`.
    /// * [`Error::DatasetDeleted`] if the dataset is already marked as deleted.
    /// * [`Error::Sqlx`] if a statement fails or a row can't be decoded.
    async fn mark_dataset_deleted(&mut self, name: &DatasetName) -> Result<DatasetId, Error>;
}

/// Reads of archives. Every read skips the archives of datasets marked as deleted.
#[async_trait]
pub trait ArchiveLookup {
    /// # Parameters
    ///
    /// * `dataset_id` - The dataset whose archives to select.
    /// * `time_range` - The query's time range.
    /// * `retention_period_minutes` - The package-wide retention period, if any.
    /// * `job_creation_timestamp_millisecs` - The query job's creation time, which the retention
    ///   period counts back from.
    ///
    /// # Returns
    ///
    /// The archives of the live dataset that overlap `time_range` and whose end timestamp is within
    /// `retention_period_minutes` of `job_creation_timestamp_millisecs` (no bound if `None`),
    /// ordered by descending end timestamp, on success. Archives without timestamps pass the
    /// retention bound and come last, as today.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::Sqlx`] if a statement fails or a row can't be decoded (e.g. a stored UUID that
    ///   isn't 16 bytes).
    async fn find_archives_for_query(
        &mut self,
        dataset_id: DatasetId,
        time_range: &TimeRange,
        retention_period_minutes: Option<NonZeroU32>,
        job_creation_timestamp_millisecs: i64,
    ) -> Result<Vec<QueryArchive>, Error>;

    /// # Parameters
    ///
    /// * `dataset_id` - The dataset whose archives to select.
    /// * `time_range` - The range that the selected archives' timestamp ranges lie within.
    ///
    /// # Returns
    ///
    /// The archives of the live dataset whose timestamp range lies within `time_range`, as
    /// `archive_manager.py` selects them, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::InvalidArchiveTimestampRange`] if a selected archive's stored begin timestamp is
    ///   after its end timestamp.
    /// * [`Error::Sqlx`] if a statement fails or a row can't be decoded (e.g. a stored UUID that
    ///   isn't 16 bytes).
    async fn find_archives_by_time_range(
        &mut self,
        dataset_id: DatasetId,
        time_range: &TimeRange,
    ) -> Result<Vec<ArchiveMetadata>, Error>;

    /// Looks up archives by ID, across datasets.
    ///
    /// # Parameters
    ///
    /// * `ids` - The IDs of the archives to look up.
    ///
    /// # Returns
    ///
    /// One entry per element of `ids`, in the same order, on success: the archive's metadata, or
    /// `None` if no archive of a live dataset has that ID.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::InvalidArchiveTimestampRange`] if a found archive's stored begin timestamp is
    ///   after its end timestamp.
    /// * [`Error::Sqlx`] if a statement fails or a row can't be decoded (e.g. a stored UUID that
    ///   isn't 16 bytes).
    async fn find_archives_by_ids(
        &mut self,
        ids: &[ArchiveId],
    ) -> Result<Vec<Option<ArchiveMetadata>>, Error>;

    /// Resolves archive UUIDs to archive IDs within one live dataset. It exists only for the
    /// archive management CLI's UUID mode, for compatibility; new APIs take archive IDs.
    ///
    /// # Parameters
    ///
    /// * `dataset_id` - The dataset that the archives belong to.
    /// * `uuids` - The UUIDs of the archives to resolve.
    ///
    /// # Returns
    ///
    /// One entry per element of `uuids`, in the same order, on success: the archive's ID, or `None`
    /// if no archive of the dataset has that UUID.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::AmbiguousArchiveUuid`] if more than one archive of the dataset has the same UUID.
    /// * [`Error::Sqlx`] if a statement fails or a row can't be decoded (e.g. a stored UUID that
    ///   isn't 16 bytes).
    async fn find_archive_ids_by_uuids(
        &mut self,
        dataset_id: DatasetId,
        uuids: &[ArchiveUuid],
    ) -> Result<Vec<Option<ArchiveId>>, Error>;
}

/// Writes to archives. Implemented only for [`crate::ClpDbTransaction`]: the guarantees below last
/// until that transaction ends.
#[async_trait]
pub trait ArchiveManagement {
    /// Inserts the archives with `creation_timestamp_millisecs` from the DB clock, then a
    /// `clp_long_span_archives` row for each archive spanning more than 24 hours.
    ///
    /// Until this transaction ends, no other transaction can mark the dataset as deleted: a
    /// concurrent [`DatasetManagement::mark_dataset_deleted`] waits for it.
    ///
    /// # Parameters
    ///
    /// * `dataset_id` - The dataset to insert the archives into.
    /// * `compression_job_id` - The compression job that produced the archives.
    /// * `archives` - The archives to insert.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::DatasetNotFound`] if the dataset doesn't exist.
    /// * [`Error::DatasetDeleted`] if the dataset was marked as deleted before this call.
    /// * [`Error::Sqlx`] if a statement fails or a row can't be decoded.
    async fn insert_archives(
        &mut self,
        dataset_id: DatasetId,
        compression_job_id: CompressionJobId,
        archives: &[ArchiveDescriptor],
    ) -> Result<(), Error>;

    /// Deletes the archives with the given IDs, across datasets: their `clp_long_span_archives`
    /// rows, then their `clp_archives` rows. The deletion takes effect when this transaction
    /// commits.
    ///
    /// # Parameters
    ///
    /// * `ids` - The IDs of the archives to delete.
    ///
    /// # Returns
    ///
    /// The IDs among `ids` that match no archive, in input order, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::InvalidArchiveTimestampRange`] if a matched archive's stored begin timestamp is
    ///   after its end timestamp.
    /// * [`Error::Sqlx`] if a statement fails or a row can't be decoded.
    async fn delete_archives(&mut self, ids: &[ArchiveId]) -> Result<Vec<ArchiveId>, Error>;
}

/// Aggregate reads of the metadata, for the `WebUI`. Every read skips datasets marked as deleted.
#[async_trait]
pub trait MetadataStatistics {
    /// # Parameters
    ///
    /// * `dataset_ids` - The datasets whose archives to cover.
    ///
    /// # Returns
    ///
    /// The range from the smallest begin timestamp to the largest end timestamp over the archives
    /// of the live datasets among `dataset_ids`, or `None` if there are none, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::InvalidArchiveTimestampRange`] if the smallest stored begin timestamp is after
    ///   the largest stored end timestamp, which only a reversed stored range can cause.
    /// * [`Error::Sqlx`] if a statement fails or a row can't be decoded.
    async fn get_timestamp_bounds(
        &mut self,
        dataset_ids: &[DatasetId],
    ) -> Result<Option<ArchiveTimestampRange>, Error>;

    /// # Returns
    ///
    /// The uncompressed and compressed sizes summed over the archives of every live dataset on
    /// success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::Sqlx`] if a statement fails or a row can't be decoded.
    async fn get_space_savings(&mut self) -> Result<SpaceSavings, Error>;

    /// # Parameters
    ///
    /// * `dataset_id` - The dataset whose columns to read.
    /// * `types` - The column types to select.
    ///
    /// # Returns
    ///
    /// The distinct names of the dataset's columns with one of `types`, ordered by name, on
    /// success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::Sqlx`] if a statement fails or a row can't be decoded (e.g. a stored name that
    ///   isn't UTF-8).
    async fn get_column_names_by_types(
        &mut self,
        dataset_id: DatasetId,
        types: &[ColumnType],
    ) -> Result<Vec<String>, Error>;
}

/// The maximum number of values in an `IN` list or rows in a multi-row insert.
const MAX_BATCH_SIZE: usize = 1000;
