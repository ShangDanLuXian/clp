//! The error type of every metadata database operation.

use clp_rust_utils::types::ArchiveUuid;
use clp_rust_utils::types::DatasetId;

use crate::types::DatasetConfig;
use crate::types::InvalidArchiveTimestampRangeError;

/// An error returned by a metadata database operation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The dataset is marked as deleted.
    #[error("the dataset is marked as deleted")]
    DatasetDeleted,

    /// No live dataset has the given ID or name.
    #[error("the dataset doesn't exist")]
    DatasetNotFound,

    /// A stored archive row has a begin timestamp after its end timestamp.
    #[error("invalid stored archive timestamp range: {0}")]
    InvalidArchiveTimestampRange(#[from] InvalidArchiveTimestampRangeError),

    /// A stored dataset row has a retention period of zero.
    #[error("dataset {dataset_id} has a stored retention period of zero")]
    InvalidStoredRetentionPeriod { dataset_id: DatasetId },

    /// [`crate::DatasetManagement::create_or_get_dataset`] found the dataset, but with other
    /// settings than requested.
    #[error("the dataset exists with other settings: {existing:?}")]
    DatasetConfigMismatch { existing: DatasetConfig },

    /// [`crate::ArchiveLookup::find_archive_ids_by_uuids`] found more than one archive with the
    /// same UUID.
    #[error("more than one archive has the UUID {uuid}")]
    AmbiguousArchiveUuid { uuid: ArchiveUuid },

    /// Forwarded `sqlx` errors, including row decode errors (e.g. a UUID that isn't 16 bytes).
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}
