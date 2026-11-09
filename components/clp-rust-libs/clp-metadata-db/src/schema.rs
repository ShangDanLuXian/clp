//! Creation of the shared metadata tables.

use crate::Error;
use crate::MetadataTables;

impl MetadataTables {
    /// Creates the shared metadata tables if they don't exist.
    ///
    /// This isn't a migration: an existing table with another shape is left as is.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::Sqlx`] if a database operation fails.
    ///
    /// # Panics
    ///
    /// Always panics, since this function isn't implemented yet.
    pub async fn create_tables(&self, _conn: &mut sqlx::MySqlConnection) -> Result<(), Error> {
        todo!("not implemented")
    }
}
