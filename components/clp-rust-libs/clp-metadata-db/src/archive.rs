//! Operations on the archives table and the long-span archives table.

use clp_rust_utils::job_config::CompressionJobId;
use clp_rust_utils::types::ArchiveId;

use crate::Archive;
use crate::ArchiveRowId;
use crate::DatasetId;
use crate::Error;
use crate::MarkedArchive;
use crate::MetadataTables;
use crate::NewArchive;
use crate::QueryArchive;
use crate::QueryArchiveFilter;
use crate::TimestampRange;

impl MetadataTables {
    /// Finds a dataset's live archives whose timestamp ranges lie within `range`.
    ///
    /// Archives are selected the same way as `archive_manager.py` selects them.
    ///
    /// # Returns
    ///
    /// The matching archives on success.
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
    pub async fn find_archives_by_time_range(
        &self,
        _conn: &mut sqlx::MySqlConnection,
        _dataset_id: DatasetId,
        _range: &TimestampRange,
    ) -> Result<Vec<Archive>, Error> {
        todo!("not implemented")
    }

    /// Finds a dataset's live archives among `uuids`.
    ///
    /// # Returns
    ///
    /// The matching archives on success. UUIDs that don't match a live archive of the dataset are
    /// absent.
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
    pub async fn find_archives_by_uuids(
        &self,
        _conn: &mut sqlx::MySqlConnection,
        _dataset_id: DatasetId,
        _uuids: &[ArchiveId],
    ) -> Result<Vec<Archive>, Error> {
        todo!("not implemented")
    }

    /// Marks a dataset's live archives among `ids` as deleted.
    ///
    /// Once the mark commits, normal reads skip the archives. The caller is expected to run this in
    /// the same transaction as the lookup that selected `ids`.
    ///
    /// # Returns
    ///
    /// The number of archives marked on success.
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
    pub async fn mark_archives_deleted(
        &self,
        _conn: &mut sqlx::MySqlConnection,
        _dataset_id: DatasetId,
        _ids: &[ArchiveRowId],
    ) -> Result<u64, Error> {
        todo!("not implemented")
    }

    /// Inserts the archives that a compression job produced into a dataset.
    ///
    /// Each archive's creation time comes from the DB clock, and each archive whose timestamp range
    /// spans more than 24 hours also gets a long-span archive row. The caller is expected to run
    /// this in a transaction, after [`Self::lock_live_dataset`] and together with the job's status
    /// update.
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
    pub async fn insert_archives(
        &self,
        _conn: &mut sqlx::MySqlConnection,
        _dataset_id: DatasetId,
        _compression_job_id: CompressionJobId,
        _archives: &[NewArchive],
    ) -> Result<(), Error> {
        todo!("not implemented")
    }

    /// Finds the archives of a live dataset that a query job searches.
    ///
    /// An archive matches if it's live, its timestamp range overlaps the filter's time range
    /// (archives without timestamps always match), and it was created within the dataset's
    /// retention period before the job's creation.
    ///
    /// # Returns
    ///
    /// The matching archives on success, or none if the dataset doesn't exist or is marked as
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
    pub async fn find_archives_for_query(
        &self,
        _conn: &mut sqlx::MySqlConnection,
        _dataset_id: DatasetId,
        _filter: &QueryArchiveFilter,
    ) -> Result<Vec<QueryArchive>, Error> {
        todo!("not implemented")
    }

    /// Marks expired archives as deleted, across every dataset.
    ///
    /// The expiry base is the DB's current time, or
    /// `earliest_query_job_creation_timestamp_millisecs` if that is earlier. An archive expires if
    /// it's live, its dataset is live and has a retention period, and it was created before the
    /// expiry base minus that retention period.
    ///
    /// # Returns
    ///
    /// The number of archives marked on success.
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
    pub async fn mark_expired_archives_deleted(
        &self,
        _conn: &mut sqlx::MySqlConnection,
        _earliest_query_job_creation_timestamp_millisecs: Option<i64>,
    ) -> Result<u64, Error> {
        todo!("not implemented")
    }

    /// Gets archives marked as deleted whose datasets aren't marked, across every dataset.
    ///
    /// Archives of marked datasets are cleaned up with their datasets instead.
    ///
    /// # Returns
    ///
    /// Up to `limit` marked archives on success.
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
    pub async fn get_marked_archives(
        &self,
        _conn: &mut sqlx::MySqlConnection,
        _limit: usize,
    ) -> Result<Vec<MarkedArchive>, Error> {
        todo!("not implemented")
    }

    /// Removes a dataset's archives among `ids` that are marked as deleted, along with their
    /// long-span archive rows.
    ///
    /// Archives that aren't marked are left as is. The caller is expected to run this in a
    /// transaction.
    ///
    /// # Returns
    ///
    /// The number of archives removed on success.
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
    pub async fn delete_marked_archives(
        &self,
        _conn: &mut sqlx::MySqlConnection,
        _dataset_id: DatasetId,
        _ids: &[ArchiveRowId],
    ) -> Result<u64, Error> {
        todo!("not implemented")
    }
}
