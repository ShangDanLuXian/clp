//! Operations on the CLP DB's datasets table and on the dataset-keyed rows of the clp-s metadata
//! tables.

use std::num::NonZeroU32;

use clp_rust_utils::dataset::DatasetId;
use clp_rust_utils::dataset::DatasetName;

use crate::Error;
use crate::MetadataTables;

/// A dataset to create in the datasets table.
#[derive(Debug)]
pub struct NewDataset {
    /// The dataset's name.
    pub name: DatasetName,
    /// The path where the dataset's archives are stored.
    pub archive_storage_path: String,
    /// The dataset's retention period in minutes, if any.
    pub retention_period_minutes: Option<NonZeroU32>,
}

impl MetadataTables {
    /// Creates `_dataset` in the datasets table.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::DatasetAlreadyExists`] if a dataset with the same name exists and isn't deleted.
    /// * [`Error::Sqlx`] if the database operation fails.
    ///
    /// # Panics
    ///
    /// Always panics until implemented.
    pub async fn create_dataset(
        &self,
        _connection: &mut sqlx::MySqlConnection,
        _dataset: &NewDataset,
    ) -> Result<(), Error> {
        todo!("not implemented")
    }

    /// # Returns
    ///
    /// The names of the datasets that aren't deleted, ordered by name, on success.
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
    pub async fn get_dataset_names(
        &self,
        _connection: &mut sqlx::MySqlConnection,
    ) -> Result<Vec<DatasetName>, Error> {
        todo!("not implemented")
    }

    /// # Returns
    ///
    /// The IDs of the deleted datasets on success.
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
    pub async fn get_deleted_dataset_ids(
        &self,
        _connection: &mut sqlx::MySqlConnection,
    ) -> Result<Vec<DatasetId>, Error> {
        todo!("not implemented")
    }

    /// Looks up a dataset's ID by its name.
    ///
    /// # Returns
    ///
    /// The dataset's ID on success, or `None` if the dataset doesn't exist or is deleted.
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
    pub async fn get_dataset_id_by_name(
        &self,
        _connection: &mut sqlx::MySqlConnection,
        _name: &DatasetName,
    ) -> Result<Option<DatasetId>, Error> {
        todo!("not implemented")
    }

    /// Marks a dataset as deleted, unless it's already deleted.
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
    pub async fn mark_dataset_deleted(
        &self,
        _connection: &mut sqlx::MySqlConnection,
        _id: DatasetId,
    ) -> Result<(), Error> {
        todo!("not implemented")
    }

    /// Deletes a dataset's rows from the archives, long-span archives and column-metadata tables,
    /// keeping its row in the datasets table.
    ///
    /// # Returns
    ///
    /// The number of deleted rows on success.
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
    pub async fn delete_dataset_metadata(
        &self,
        _connection: &mut sqlx::MySqlConnection,
        _id: DatasetId,
    ) -> Result<u64, Error> {
        todo!("not implemented")
    }
}
