//! Reads of the column metadata table.

use crate::ColumnType;
use crate::DatasetId;
use crate::Error;
use crate::MetadataTables;

impl MetadataTables {
    /// Gets the names of a live dataset's columns that have any of `types`.
    ///
    /// # Returns
    ///
    /// The matching column names on success, or none if the dataset doesn't exist or is marked as
    /// deleted.
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
    pub async fn get_column_names_by_types(
        &self,
        _conn: &mut sqlx::MySqlConnection,
        _dataset_id: DatasetId,
        _types: &[ColumnType],
    ) -> Result<Vec<String>, Error> {
        todo!("not implemented")
    }
}
