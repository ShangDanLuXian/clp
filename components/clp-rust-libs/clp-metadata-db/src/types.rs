//! Identifiers, column types, and the row and input types of the metadata table operations.

use std::num::NonZeroU32;

use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::types::ArchiveId;

/// A dataset's ID in the datasets table.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, sqlx::Type)]
#[sqlx(transparent)]
pub struct DatasetId(u16);

/// An archive's row ID in the archives table, distinct from its UUID ([`ArchiveId`]).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, sqlx::Type)]
#[sqlx(transparent)]
pub struct ArchiveRowId(u64);

/// An inclusive range of timestamps, in milliseconds since the Unix epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimestampRange {
    pub begin_timestamp_millisecs: i64,
    pub end_timestamp_millisecs: i64,
}

/// Mirror of `clp_s::NodeType` (`components/core/src/clp_s/SchemaTree.hpp`). Must be kept in sync.
///
/// A column's type in the column metadata table. Each variant's integer value is the value of the
/// C++ enumerator with the same name.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum ColumnType {
    Integer = 0,
    Float = 1,
    ClpString = 2,
    VarString = 3,
    Boolean = 4,
    Object = 5,
    UnstructuredArray = 6,
    NullValue = 7,
    DeprecatedDateString = 8,
    StructuredArray = 9,
    Metadata = 10,
    DeltaInteger = 11,
    FormattedFloat = 12,
    DictionaryFloat = 13,
    Timestamp = 14,
    Unknown = 255,
}

/// A dataset to create.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewDataset {
    pub name: DatasetName,

    /// The directory or S3 key prefix that the dataset's archives are written to.
    pub archive_storage_path: String,

    /// How long the dataset's archives are kept after their creation, or `None` to keep them
    /// forever.
    pub retention_period_minutes: Option<NonZeroU32>,
}

/// An archive, as the archive API lists it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Archive {
    pub id: ArchiveRowId,
    pub uuid: ArchiveId,

    /// The inclusive lower bound of the archive's timestamps, or `0` if it has no timestamps.
    pub begin_timestamp_millisecs: i64,

    /// The inclusive upper bound of the archive's timestamps, or `0` if it has no timestamps.
    pub end_timestamp_millisecs: i64,
}

/// An archive that a compression job produced, to insert into the archives table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewArchive {
    pub uuid: ArchiveId,

    /// The inclusive lower bound of the archive's timestamps, or `0` if it has no timestamps.
    pub begin_timestamp_millisecs: i64,

    /// The inclusive upper bound of the archive's timestamps, or `0` if it has no timestamps.
    pub end_timestamp_millisecs: i64,

    pub num_uncompressed_bytes: u64,
    pub num_compressed_bytes: u64,
}

/// The criteria that select the archives a query job searches.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryArchiveFilter {
    /// The inclusive lower bound of the query's time range, or `None` if it's unbounded.
    pub begin_timestamp_millisecs: Option<i64>,

    /// The inclusive upper bound of the query's time range, or `None` if it's unbounded.
    pub end_timestamp_millisecs: Option<i64>,

    /// The query job's creation time, which the dataset's retention period is measured back from.
    pub job_creation_timestamp_millisecs: i64,
}

/// An archive that a query job searches.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryArchive {
    pub uuid: ArchiveId,
    pub num_uncompressed_bytes: u64,

    /// The inclusive upper bound of the archive's timestamps, or `0` if it has no timestamps.
    pub end_timestamp_millisecs: i64,
}

/// A dataset marked as deleted, pending cleanup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarkedDataset {
    pub id: DatasetId,

    /// The directory or S3 key prefix that the dataset's archives were written to.
    pub archive_storage_path: String,
}

/// An archive marked as deleted, pending cleanup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarkedArchive {
    pub id: ArchiveRowId,
    pub uuid: ArchiveId,
    pub dataset_id: DatasetId,

    /// The directory or S3 key prefix that the archive's dataset writes its archives to.
    pub archive_storage_path: String,
}

/// The earliest begin timestamp and the latest end timestamp over a set of archives.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimestampBounds {
    pub begin_timestamp_millisecs: i64,
    pub end_timestamp_millisecs: i64,
}

/// The total uncompressed and compressed sizes over a set of archives.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpaceSavings {
    pub num_uncompressed_bytes: u64,
    pub num_compressed_bytes: u64,
}
