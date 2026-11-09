//! Typed operations on the clp-s metadata tables that every dataset shares.
//!
//! Every operation runs on a caller-provided [`sqlx::MySqlConnection`], so callers can combine
//! operations inside their own transactions or run them on pooled connections.

mod archive;
mod column_metadata;
mod dataset;
mod error;
mod schema;
mod statistics;
mod types;

pub use error::Error;
pub use types::Archive;
pub use types::ArchiveRowId;
pub use types::ColumnType;
pub use types::DatasetId;
pub use types::MarkedArchive;
pub use types::MarkedDataset;
pub use types::NewArchive;
pub use types::NewDataset;
pub use types::QueryArchive;
pub use types::QueryArchiveFilter;
pub use types::SpaceSavings;
pub use types::TimestampBounds;
pub use types::TimestampRange;

/// The clp-s metadata tables that every dataset shares, identified by their names.
#[derive(Clone, Debug)]
#[expect(
    dead_code,
    reason = "the table names are read once the operations are implemented"
)]
pub struct MetadataTables {
    datasets: String,
    archives: String,
    long_span_archives: String,
    column_metadata: String,
}

impl MetadataTables {
    /// Factory function.
    ///
    /// Names the shared tables after the table prefix in the given database config.
    ///
    /// # Returns
    ///
    /// The shared metadata tables of the configured database.
    ///
    /// # Panics
    ///
    /// Always panics, since this function isn't implemented yet.
    #[must_use]
    pub fn new(_database_config: &clp_rust_utils::clp_config::package::config::Database) -> Self {
        todo!("not implemented")
    }
}
