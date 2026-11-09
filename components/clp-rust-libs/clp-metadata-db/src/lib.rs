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
    #[must_use]
    pub fn new(database_config: &clp_rust_utils::clp_config::package::config::Database) -> Self {
        Self {
            datasets: database_config.datasets_table_name(),
            archives: database_config.unscoped_archives_table_name(),
            long_span_archives: database_config.long_span_archives_table_name(),
            column_metadata: database_config.unscoped_column_metadata_table_name(),
        }
    }
}
