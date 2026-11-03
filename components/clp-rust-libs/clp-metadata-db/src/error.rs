//! Error type for the operations on the clp-s metadata tables.

use clp_rust_utils::dataset::DatasetName;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("dataset already exists: {0}")]
    DatasetAlreadyExists(DatasetName),

    #[error("`sqlx::Error`: {0}")]
    Sqlx(#[from] sqlx::Error),
}
