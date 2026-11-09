//! Error types of the garbage collectors.

/// Errors returned by the archive garbage collector.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The package doesn't use the clp-s storage engine.
    #[error("the archive garbage collector only supports the clp-s storage engine")]
    UnsupportedStorageEngine,

    /// Forwarded `clp_metadata_db` errors.
    #[error("`clp_metadata_db::Error`: {0}")]
    MetadataDb(#[from] clp_metadata_db::Error),

    /// Forwarded `clp_rust_utils` errors.
    #[error("`clp_rust_utils::Error`: {0}")]
    ClpRustUtils(#[from] clp_rust_utils::Error),

    /// Forwarded `sqlx` errors.
    #[error("`sqlx::Error`: {0}")]
    Sqlx(#[from] sqlx::Error),
}

/// Errors from deleting datasets and archives from the archive storage.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Forwarded filesystem errors.
    #[error("`std::io::Error`: {0}")]
    Fs(#[from] std::io::Error),

    /// Forwarded S3 errors.
    #[error("`clp_rust_utils::Error`: {0}")]
    S3(#[from] clp_rust_utils::Error),
}
