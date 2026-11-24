//! The values that the metadata database operations take and return.

use std::num::NonZeroU32;

use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::types::DatasetId;

/// A dataset's configurable settings, stored in its `clp_datasets` row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetConfig {
    /// The directory or S3 key prefix that the dataset's archives are written to.
    pub archive_storage_path: String,

    /// How long archives are kept, by end timestamp. Always `None` in this release: the
    /// package-wide retention period applies.
    pub retention_period_minutes: Option<NonZeroU32>,
}

/// A dataset to create.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetDescriptor {
    pub name: DatasetName,
    pub config: DatasetConfig,
}

/// A live dataset, as stored.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Dataset {
    pub id: DatasetId,
    pub name: DatasetName,
    pub config: DatasetConfig,
}
