//! Operations on the datasets table.

use std::collections::HashMap;

use clp_rust_utils::dataset::DatasetName;

use crate::DatasetId;
use crate::Error;
use crate::MarkedDataset;
use crate::MetadataTables;
use crate::NewDataset;

impl MetadataTables {
    /// Creates a live dataset.
    ///
    /// # Returns
    ///
    /// The ID of the created dataset on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::DatasetAlreadyExists`] if a dataset with the same name exists, including one
    ///   marked as deleted.
    /// * [`Error::Sqlx`] if a database operation fails.
    ///
    /// # Panics
    ///
    /// Always panics, since this function isn't implemented yet.
    pub async fn create_dataset(
        &self,
        _conn: &mut sqlx::MySqlConnection,
        _dataset: &NewDataset,
    ) -> Result<DatasetId, Error> {
        todo!("not implemented")
    }

    /// Gets the names of every live dataset, i.e., every dataset that isn't marked as deleted.
    ///
    /// # Returns
    ///
    /// The names of the live datasets, ordered by name, on success.
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
    pub async fn get_dataset_names(
        &self,
        _conn: &mut sqlx::MySqlConnection,
    ) -> Result<Vec<DatasetName>, Error> {
        todo!("not implemented")
    }

    /// Looks up a live dataset by name.
    ///
    /// # Returns
    ///
    /// The dataset's ID on success, or `None` if no live dataset has the name, including when the
    /// dataset with the name is marked as deleted.
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
    pub async fn get_dataset_id_by_name(
        &self,
        _conn: &mut sqlx::MySqlConnection,
        _name: &DatasetName,
    ) -> Result<Option<DatasetId>, Error> {
        todo!("not implemented")
    }

    /// Marks a live dataset as deleted.
    ///
    /// Once the mark commits, normal reads skip the dataset and its archives, commits into the
    /// dataset fail, and its name can't be used to create a dataset until the dataset is reclaimed.
    ///
    /// # Returns
    ///
    /// Whether a live dataset was marked on success; `false` if the dataset doesn't exist or is
    /// already marked.
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
    pub async fn mark_dataset_deleted(
        &self,
        _conn: &mut sqlx::MySqlConnection,
        _id: DatasetId,
    ) -> Result<bool, Error> {
        todo!("not implemented")
    }

    /// Locks a dataset's row for shared access (`FOR SHARE`) and checks whether it's live.
    ///
    /// The lock serializes the caller's transaction with [`Self::mark_dataset_deleted`]. The caller
    /// is expected to run this in a transaction, before [`Self::insert_archives`].
    ///
    /// # Returns
    ///
    /// Whether the dataset exists and isn't marked as deleted on success.
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
    pub async fn lock_live_dataset(
        &self,
        _conn: &mut sqlx::MySqlConnection,
        _id: DatasetId,
    ) -> Result<bool, Error> {
        todo!("not implemented")
    }

    /// Looks up live datasets by name.
    ///
    /// # Returns
    ///
    /// A map from each name that resolves to a live dataset to the dataset's ID on success. Names
    /// that don't resolve, including those of datasets marked as deleted, are absent.
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
    pub async fn get_dataset_ids_by_names(
        &self,
        _conn: &mut sqlx::MySqlConnection,
        _names: &[DatasetName],
    ) -> Result<HashMap<DatasetName, DatasetId>, Error> {
        todo!("not implemented")
    }

    /// Gets every dataset marked as deleted.
    ///
    /// # Returns
    ///
    /// The marked datasets on success.
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
    pub async fn get_marked_datasets(
        &self,
        _conn: &mut sqlx::MySqlConnection,
    ) -> Result<Vec<MarkedDataset>, Error> {
        todo!("not implemented")
    }

    /// Removes a dataset marked as deleted from every shared table.
    ///
    /// Removes the dataset's long-span archive, archive and column metadata rows, then its dataset
    /// row. A dataset that isn't marked is left as is. The caller is expected to run this in a
    /// transaction.
    ///
    /// # Returns
    ///
    /// Whether the dataset row was removed on success.
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
    pub async fn delete_marked_dataset(
        &self,
        _conn: &mut sqlx::MySqlConnection,
        _id: DatasetId,
    ) -> Result<bool, Error> {
        todo!("not implemented")
    }
}
