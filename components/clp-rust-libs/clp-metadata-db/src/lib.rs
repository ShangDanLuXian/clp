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

/// The clp-s metadata tables that every dataset shares in the CLP DB.
///
/// Each operation accepts a connection, so the caller owns transaction boundaries.
#[derive(Clone, Debug)]
pub struct MetadataTables {
    _private: (),
}

impl MetadataTables {
    /// Factory function.
    ///
    /// Derives the names of the clp-s metadata tables from the database configuration.
    ///
    /// # Returns
    ///
    /// A newly created [`MetadataTables`].
    ///
    /// # Panics
    ///
    /// Always panics until implemented.
    #[must_use]
    pub fn new(_database_config: &DatabaseConfig) -> Self {
        todo!("not implemented")
    }
}
