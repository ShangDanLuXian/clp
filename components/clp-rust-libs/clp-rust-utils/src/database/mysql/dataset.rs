//! Operations on the CLP DB's datasets table and on the dataset-keyed rows of the clp-s metadata
//! tables.

use crate::clp_config::package::config::Database as DatabaseConfig;
use crate::dataset::DatasetId;
use crate::dataset::DatasetName;

/// The ID and archive storage path of a dataset in the datasets table.
#[derive(Debug, sqlx::FromRow)]
pub struct Dataset {
    /// The dataset's ID.
    pub id: DatasetId,
    /// The path of the dataset's archives in the archive storage.
    pub archive_storage_path: String,
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
pub async fn get_names(
    db_pool: &sqlx::MySqlPool,
    database_config: &DatabaseConfig,
) -> Result<Vec<DatasetName>, crate::Error> {
    let datasets_table = database_config.datasets_table_name();
    Ok(sqlx::query_scalar(&format!(
        "SELECT `name` FROM `{datasets_table}` WHERE `is_deleted` = FALSE ORDER BY `name`"
    ))
    .fetch_all(db_pool)
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
pub async fn get_deleted_ids(
    db_pool: &sqlx::MySqlPool,
    database_config: &DatabaseConfig,
) -> Result<Vec<DatasetId>, crate::Error> {
    let datasets_table = database_config.datasets_table_name();
    Ok(sqlx::query_scalar(&format!(
        "SELECT `id` FROM `{datasets_table}` WHERE `is_deleted` = TRUE"
    ))
    .fetch_all(db_pool)
    .await?)
}

/// Looks up a dataset by its name.
///
/// # Returns
///
/// The dataset on success, or `None` if the dataset doesn't exist or is deleted.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::query::QueryAs::fetch_optional`]'s return values on failure.
pub async fn get_by_name(
    db_pool: &sqlx::MySqlPool,
    database_config: &DatabaseConfig,
    name: &DatasetName,
) -> Result<Option<Dataset>, crate::Error> {
    let datasets_table = database_config.datasets_table_name();
    Ok(sqlx::query_as(&format!(
        "SELECT `id`, `archive_storage_path` FROM `{datasets_table}` WHERE `name` = ? AND \
         `is_deleted` = FALSE"
    ))
    .bind(name)
    .fetch_optional(db_pool)
    .await?)
}

/// Marks a dataset as deleted.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
pub async fn mark_deleted(
    db_pool: &sqlx::MySqlPool,
    database_config: &DatabaseConfig,
    id: DatasetId,
) -> Result<(), crate::Error> {
    let datasets_table = database_config.datasets_table_name();
    sqlx::query(&format!(
        "UPDATE `{datasets_table}` SET `is_deleted` = TRUE WHERE `id` = ?"
    ))
    .bind(id)
    .execute(db_pool)
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
pub async fn delete_metadata(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    database_config: &DatabaseConfig,
    id: DatasetId,
) -> Result<u64, crate::Error> {
    let mut num_deleted_rows = 0;
    for table in [
        database_config.long_span_archives_table_name(),
        database_config.unscoped_archives_table_name(),
        database_config.unscoped_column_metadata_table_name(),
    ] {
        num_deleted_rows += sqlx::query(&format!("DELETE FROM `{table}` WHERE `dataset_id` = ?"))
            .bind(id)
            .execute(&mut **tx)
            .await?
            .rows_affected();
    }
    Ok(num_deleted_rows)
}
