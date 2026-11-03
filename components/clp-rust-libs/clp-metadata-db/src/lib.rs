//! Operations on the clp-s metadata tables that every dataset shares in the CLP DB.

mod archive;
mod dataset;
mod error;
mod schema;

pub use archive::Archive;
pub use archive::ExpiredArchive;
use clp_rust_utils::clp_config::package::config::Database as DatabaseConfig;
pub use dataset::NewDataset;
pub use error::Error;

/// The clp-s metadata tables that every dataset shares in the CLP DB, identified by their names.
///
/// Each method runs its statements on the given connection, so the caller decides whether they run
/// in a transaction.
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
    /// Derives the names of the clp-s metadata tables from `database_config`.
    ///
    /// # Returns
    ///
    /// A newly created [`MetadataTables`].
    #[must_use]
    pub fn new(database_config: &DatabaseConfig) -> Self {
        Self {
            datasets: database_config.datasets_table_name(),
            archives: database_config.unscoped_archives_table_name(),
            long_span_archives: database_config.long_span_archives_table_name(),
            column_metadata: database_config.unscoped_column_metadata_table_name(),
        }
    }
}
