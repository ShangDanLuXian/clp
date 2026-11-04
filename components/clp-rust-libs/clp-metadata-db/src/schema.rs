//! The schema of the clp-s metadata tables, which every dataset shares.

use crate::Error;
use crate::MetadataTables;

impl MetadataTables {
    /// Creates the clp-s metadata tables that don't exist yet.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::Sqlx`] if the database operation fails.
    ///
    /// # Panics
    ///
    /// Always panics until implemented.
    pub async fn create_tables(
        &self,
        _connection: &mut sqlx::MySqlConnection,
    ) -> Result<(), Error> {
        todo!("not implemented")
    }
}
