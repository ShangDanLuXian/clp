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
    /// Creates `dataset` in the datasets table.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::DatasetAlreadyExists`] if a dataset with the same name exists and isn't deleted.
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    pub async fn create_dataset(
        &self,
        connection: &mut sqlx::MySqlConnection,
        dataset: &NewDataset,
    ) -> Result<(), Error> {
        let datasets_table = &self.datasets;
        sqlx::query(&format!(
            "INSERT INTO `{datasets_table}` (`name`, `archive_storage_path`, \
             `retention_period_minutes`) VALUES (?, ?, ?)"
        ))
        .bind(&dataset.name)
        .bind(dataset.archive_storage_path.as_str())
        .bind(dataset.retention_period_minutes.map(NonZeroU32::get))
        .execute(connection)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                Error::DatasetAlreadyExists(dataset.name.clone())
            }
            e => e.into(),
        })?;
        Ok(())
    }

    /// # Returns
    ///
    /// The names of the datasets that aren't deleted, ordered by name, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryScalar::fetch_all`]'s return values on failure.
    pub async fn get_dataset_names(
        &self,
        connection: &mut sqlx::MySqlConnection,
    ) -> Result<Vec<DatasetName>, Error> {
        let datasets_table = &self.datasets;
        Ok(sqlx::query_scalar(&format!(
            "SELECT `name` FROM `{datasets_table}` WHERE `deleted_at_millis` = 0 ORDER BY `name`"
        ))
        .fetch_all(connection)
        .await?)
    }

    /// # Returns
    ///
    /// The IDs of the deleted datasets on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryScalar::fetch_all`]'s return values on failure.
    pub async fn get_deleted_dataset_ids(
        &self,
        connection: &mut sqlx::MySqlConnection,
    ) -> Result<Vec<DatasetId>, Error> {
        let datasets_table = &self.datasets;
        Ok(sqlx::query_scalar(&format!(
            "SELECT `id` FROM `{datasets_table}` WHERE `deleted_at_millis` <> 0"
        ))
        .fetch_all(connection)
        .await?)
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
    /// * Forwards [`sqlx::query::QueryScalar::fetch_optional`]'s return values on failure.
    pub async fn get_dataset_id_by_name(
        &self,
        connection: &mut sqlx::MySqlConnection,
        name: &DatasetName,
    ) -> Result<Option<DatasetId>, Error> {
        let datasets_table = &self.datasets;
        Ok(sqlx::query_scalar(&format!(
            "SELECT `id` FROM `{datasets_table}` WHERE `name` = ? AND `deleted_at_millis` = 0"
        ))
        .bind(name)
        .fetch_optional(connection)
        .await?)
    }

    /// Marks a dataset as deleted, unless it's already deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    pub async fn mark_dataset_deleted(
        &self,
        connection: &mut sqlx::MySqlConnection,
        id: DatasetId,
    ) -> Result<(), Error> {
        let datasets_table = &self.datasets;
        sqlx::query(&format!(
            "UPDATE `{datasets_table}` SET `deleted_at_millis` = \
             CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED) WHERE `id` = ? AND \
             `deleted_at_millis` = 0"
        ))
        .bind(id)
        .execute(connection)
        .await?;
        Ok(())
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
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    pub async fn delete_dataset_metadata(
        &self,
        connection: &mut sqlx::MySqlConnection,
        id: DatasetId,
    ) -> Result<u64, Error> {
        let mut num_deleted_rows = 0;
        for table in [
            &self.long_span_archives,
            &self.archives,
            &self.column_metadata,
        ] {
            num_deleted_rows +=
                sqlx::query(&format!("DELETE FROM `{table}` WHERE `dataset_id` = ?"))
                    .bind(id)
                    .execute(&mut *connection)
                    .await?
                    .rows_affected();
        }
        Ok(num_deleted_rows)
    }
}
