//! Operations on the clp-s archives tables that every dataset shares.

use clp_rust_utils::dataset::DatasetId;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::types::ArchiveId;

use crate::Error;
use crate::MetadataTables;

/// An archive in the archives table.
#[derive(Debug)]
pub struct Archive {
    /// The archive's ID in the archives table.
    pub id: u64,
    /// The archive's UUID.
    pub uuid: ArchiveId,
}

/// An archive that has outlived its dataset's retention period.
#[derive(Debug)]
pub struct ExpiredArchive {
    /// The archive.
    pub archive: Archive,
    /// The ID of the archive's dataset.
    pub dataset_id: DatasetId,
    /// The name of the archive's dataset.
    pub dataset_name: DatasetName,
}

impl MetadataTables {
    /// Finds a dataset's archives whose timestamp ranges begin at or after `_begin_ts` and, if
    /// `_end_ts` is given, end at or before `_end_ts`.
    ///
    /// # Returns
    ///
    /// The matching archives on success.
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
    pub async fn find_archives_by_time_range(
        &self,
        _connection: &mut sqlx::MySqlConnection,
        _dataset_id: DatasetId,
        _begin_ts: i64,
        _end_ts: Option<i64>,
    ) -> Result<Vec<Archive>, Error> {
        todo!("not implemented")
    }

    /// Finds a dataset's archives whose UUIDs are in `_uuids`.
    ///
    /// # Returns
    ///
    /// The matching archives on success.
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
    pub async fn find_archives_by_uuids(
        &self,
        _connection: &mut sqlx::MySqlConnection,
        _dataset_id: DatasetId,
        _uuids: &[ArchiveId],
    ) -> Result<Vec<Archive>, Error> {
        todo!("not implemented")
    }

    /// Deletes the metadata of a dataset's archives, identified by their IDs in the archives table,
    /// from the long-span archives table and the archives table.
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
    pub async fn delete_archives(
        &self,
        _connection: &mut sqlx::MySqlConnection,
        _dataset_id: DatasetId,
        _ids: &[u64],
    ) -> Result<(), Error> {
        todo!("not implemented")
    }

    /// # Returns
    ///
    /// The archives of the datasets that aren't deleted and have a retention period, which were
    /// created earlier than `_expiry_base_millis` minus their dataset's retention period, on
    /// success.
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
    pub async fn find_expired_archives(
        &self,
        _connection: &mut sqlx::MySqlConnection,
        _expiry_base_millis: i64,
    ) -> Result<Vec<ExpiredArchive>, Error> {
        todo!("not implemented")
    }
}
