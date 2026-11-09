//! Error type for the metadata table operations.

use clp_rust_utils::dataset::DatasetName;

/// Errors returned by the metadata table operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A dataset with the same name already exists, possibly pending deletion.
    #[error("dataset {0} already exists")]
    DatasetAlreadyExists(DatasetName),

    /// An existing table doesn't have the shape of the shared metadata tables.
    #[error("table {table} has an incompatible shape")]
    IncompatibleTable { table: String },

    /// Forwarded `sqlx` errors, including row decode errors (e.g. a UUID that isn't 16 bytes).
    #[error("`sqlx::Error`: {0}")]
    Sqlx(#[from] sqlx::Error),
}
