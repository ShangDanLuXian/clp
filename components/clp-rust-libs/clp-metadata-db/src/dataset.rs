//! Operations on the datasets table.

use std::collections::HashMap;
use std::num::NonZeroU32;

use clp_rust_utils::dataset::DatasetName;
use sqlx::mysql::MySqlDatabaseError;

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
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if the ID of the created dataset doesn't fit in [`u16`], which the datasets table's
    /// `SMALLINT UNSIGNED` ID column precludes.
    pub async fn create_dataset(
        &self,
        conn: &mut sqlx::MySqlConnection,
        dataset: &NewDataset,
    ) -> Result<DatasetId, Error> {
        const MYSQL_ER_DUP_ENTRY: u16 = 1062;

        let query = format!(
            r"
INSERT INTO `{datasets}` (`name`, `archive_storage_path`, `retention_period_minutes`)
VALUES (?, ?, ?)",
            datasets = self.datasets,
        );
        let id = sqlx::query(&query)
            .bind(&dataset.name)
            .bind(&dataset.archive_storage_path)
            .bind(dataset.retention_period_minutes.map(NonZeroU32::get))
            .execute(&mut *conn)
            .await
            .map_err(|e| match e {
                sqlx::Error::Database(e)
                    if e.try_downcast_ref::<MySqlDatabaseError>()
                        .is_some_and(|mysql_err| mysql_err.number() == MYSQL_ER_DUP_ENTRY) =>
                {
                    Error::DatasetAlreadyExists(dataset.name.clone())
                }
                e => e.into(),
            })?
            .last_insert_id();
        Ok(DatasetId(u16::try_from(id).expect(
            "the ID of a `SMALLINT UNSIGNED` column should fit in `u16`",
        )))
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
    /// * Forwards [`sqlx::query::QueryScalar::fetch_all`]'s return values on failure.
    pub async fn get_dataset_names(
        &self,
        conn: &mut sqlx::MySqlConnection,
    ) -> Result<Vec<DatasetName>, Error> {
        // `sqlx` doesn't decode strings from `utf8mb4_bin` columns, which are reported as binary.
        let query = format!(
            r"
SELECT CONVERT(`name` USING utf8mb4) FROM `{datasets}`
WHERE `is_deleted` = FALSE
ORDER BY `name`",
            datasets = self.datasets,
        );
        Ok(sqlx::query_scalar(&query).fetch_all(&mut *conn).await?)
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
    /// * Forwards [`sqlx::query::QueryScalar::fetch_optional`]'s return values on failure.
    pub async fn get_dataset_id_by_name(
        &self,
        conn: &mut sqlx::MySqlConnection,
        name: &DatasetName,
    ) -> Result<Option<DatasetId>, Error> {
        let query = format!(
            r"
SELECT `id` FROM `{datasets}`
WHERE `name` = ? AND `is_deleted` = FALSE",
            datasets = self.datasets,
        );
        Ok(sqlx::query_scalar(&query)
            .bind(name)
            .fetch_optional(&mut *conn)
            .await?)
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
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    pub async fn mark_dataset_deleted(
        &self,
        conn: &mut sqlx::MySqlConnection,
        id: DatasetId,
    ) -> Result<bool, Error> {
        let query = format!(
            r"
UPDATE `{datasets}` SET `is_deleted` = TRUE
WHERE `id` = ? AND `is_deleted` = FALSE",
            datasets = self.datasets,
        );
        let result = sqlx::query(&query).bind(id).execute(&mut *conn).await?;
        Ok(result.rows_affected() == 1)
    }

    /// Locks a dataset's row for shared access (`LOCK IN SHARE MODE`) and checks whether it's live.
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
    /// * Forwards [`sqlx::query::QueryScalar::fetch_optional`]'s return values on failure.
    pub async fn lock_live_dataset(
        &self,
        conn: &mut sqlx::MySqlConnection,
        id: DatasetId,
    ) -> Result<bool, Error> {
        let query = format!(
            r"
SELECT `is_deleted` FROM `{datasets}`
WHERE `id` = ?
LOCK IN SHARE MODE",
            datasets = self.datasets,
        );
        let is_deleted: Option<bool> = sqlx::query_scalar(&query)
            .bind(id)
            .fetch_optional(&mut *conn)
            .await?;
        Ok(is_deleted == Some(false))
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
    /// * Forwards [`sqlx::query::QueryAs::fetch_all`]'s return values on failure.
    pub async fn get_dataset_ids_by_names(
        &self,
        conn: &mut sqlx::MySqlConnection,
        names: &[DatasetName],
    ) -> Result<HashMap<DatasetName, DatasetId>, Error> {
        const MAX_NUM_NAMES_PER_QUERY: usize = 1000;

        let mut ids = HashMap::new();
        for chunk in names.chunks(MAX_NUM_NAMES_PER_QUERY) {
            // `sqlx` doesn't decode strings from `utf8mb4_bin` columns, which are reported as
            // binary.
            let mut query_builder = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
                r"
SELECT CONVERT(`name` USING utf8mb4), `id` FROM `{datasets}`
WHERE `is_deleted` = FALSE AND `name` IN (",
                datasets = self.datasets,
            ));
            let mut separated_names = query_builder.separated(", ");
            for name in chunk {
                separated_names.push_bind(name);
            }
            query_builder.push(")");
            ids.extend(
                query_builder
                    .build_query_as::<(DatasetName, DatasetId)>()
                    .fetch_all(&mut *conn)
                    .await?,
            );
        }
        Ok(ids)
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
    /// * Forwards [`sqlx::query::QueryAs::fetch_all`]'s return values on failure.
    pub async fn get_marked_datasets(
        &self,
        conn: &mut sqlx::MySqlConnection,
    ) -> Result<Vec<MarkedDataset>, Error> {
        // `sqlx` doesn't decode strings from `utf8mb4_bin` columns, which are reported as binary.
        let query = format!(
            r"
SELECT `id`, CONVERT(`archive_storage_path` USING utf8mb4) FROM `{datasets}`
WHERE `is_deleted` = TRUE",
            datasets = self.datasets,
        );
        let rows: Vec<(DatasetId, String)> = sqlx::query_as(&query).fetch_all(&mut *conn).await?;
        Ok(rows
            .into_iter()
            .map(|(id, archive_storage_path)| MarkedDataset {
                id,
                archive_storage_path,
            })
            .collect())
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
    /// * Forwards [`sqlx::query::QueryScalar::fetch_optional`]'s return values on failure.
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    pub async fn delete_marked_dataset(
        &self,
        conn: &mut sqlx::MySqlConnection,
        id: DatasetId,
    ) -> Result<bool, Error> {
        let lock_query = format!(
            r"
SELECT `is_deleted` FROM `{datasets}`
WHERE `id` = ?
FOR UPDATE",
            datasets = self.datasets,
        );
        let Some(true) = sqlx::query_scalar::<_, bool>(&lock_query)
            .bind(id)
            .fetch_optional(&mut *conn)
            .await?
        else {
            return Ok(false);
        };

        for child_table in [
            &self.long_span_archives,
            &self.archives,
            &self.column_metadata,
        ] {
            sqlx::query(&format!(
                "DELETE FROM `{child_table}` WHERE `dataset_id` = ?"
            ))
            .bind(id)
            .execute(&mut *conn)
            .await?;
        }

        let dataset_deletion_query = format!(
            "DELETE FROM `{datasets}` WHERE `id` = ?",
            datasets = self.datasets,
        );
        let result = sqlx::query(&dataset_deletion_query)
            .bind(id)
            .execute(&mut *conn)
            .await?;
        Ok(result.rows_affected() == 1)
    }
}
