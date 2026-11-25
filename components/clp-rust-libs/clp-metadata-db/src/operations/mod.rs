//! The operation traits, grouped by entity and by whether they write.

mod archive;
mod dataset;

use std::collections::HashMap;

use async_trait::async_trait;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::job_config::CompressionJobId;
use clp_rust_utils::types::DatasetId;

use crate::error::Error;
use crate::types::ArchiveDescriptor;
use crate::types::Dataset;
use crate::types::DatasetDescriptor;

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
}

/// Writes to archives. Implemented only for [`crate::ClpDbTransaction`]: the guarantees below last
/// until that transaction ends.
#[async_trait]
pub trait ArchiveManagement {
    /// Inserts the archives with `creation_timestamp_millisecs` from the DB clock, then a
    /// `clp_long_span_archives` row for each archive spanning more than 24 hours.
    ///
    /// Until this transaction ends, no other transaction can mark the dataset as deleted.
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
}

/// The maximum number of values in an `IN` list or rows in a multi-row insert.
const MAX_BATCH_SIZE: usize = 1000;
