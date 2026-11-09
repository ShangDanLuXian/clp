//! Aggregate reads over the archives table for the `WebUI` metadata routes.

use clp_rust_utils::types::ArchiveId;

use crate::DatasetId;
use crate::Error;
use crate::MetadataTables;
use crate::SpaceSavings;
use crate::TimestampBounds;

impl MetadataTables {
    /// Computes the timestamp bounds over the live archives of the live datasets among `ids`.
    ///
    /// # Returns
    ///
    /// The timestamp bounds on success, or `None` if there are no such archives.
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
    pub async fn get_timestamp_bounds(
        &self,
        _conn: &mut sqlx::MySqlConnection,
        _ids: &[DatasetId],
    ) -> Result<Option<TimestampBounds>, Error> {
        todo!("not implemented")
    }

    /// Computes the total sizes of the live archives of every live dataset.
    ///
    /// # Returns
    ///
    /// The total uncompressed and compressed sizes on success, both `0` if there are no such
    /// archives.
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
    pub async fn get_space_savings(
        &self,
        _conn: &mut sqlx::MySqlConnection,
    ) -> Result<SpaceSavings, Error> {
        todo!("not implemented")
    }

    /// Computes the total uncompressed size of the live archives among `uuids` that belong to the
    /// live datasets among `dataset_ids`.
    ///
    /// # Returns
    ///
    /// The total number of uncompressed bytes on success, or `0` if no archive matches.
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
    pub async fn get_uncompressed_bytes_by_uuids(
        &self,
        _conn: &mut sqlx::MySqlConnection,
        _dataset_ids: &[DatasetId],
        _uuids: &[ArchiveId],
    ) -> Result<u64, Error> {
        todo!("not implemented")
    }
}
