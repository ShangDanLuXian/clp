//! Operations on the CLP DB's datasets table and on the dataset-keyed rows of the clp-s metadata
//! tables.

use crate::clp_config::package::config::Database as DatabaseConfig;
use crate::dataset::DatasetId;
use crate::dataset::DatasetName;

/// Looks up the ID of a dataset by its name.
///
/// # Returns
///
/// The ID of the dataset on success, or `None` if the dataset doesn't exist or is deleted.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::query::QueryScalar::fetch_optional`]'s return values on failure.
pub async fn get_id(
    executor: impl sqlx::MySqlExecutor<'_>,
    database_config: &DatabaseConfig,
    dataset: &DatasetName,
) -> Result<Option<DatasetId>, crate::Error> {
    let datasets_table = database_config.datasets_table_name();
    Ok(sqlx::query_scalar(&format!(
        "SELECT `id` FROM `{datasets_table}` WHERE `name` = ? AND `is_deleted` = FALSE"
    ))
    .bind(dataset)
    .fetch_optional(executor)
    .await?)
}

/// # Returns
///
/// The IDs of every deleted dataset on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::query::QueryScalar::fetch_all`]'s return values on failure.
pub async fn get_deleted_ids(
    executor: impl sqlx::MySqlExecutor<'_>,
    database_config: &DatabaseConfig,
) -> Result<Vec<DatasetId>, crate::Error> {
    let datasets_table = database_config.datasets_table_name();
    Ok(sqlx::query_scalar(&format!(
        "SELECT `id` FROM `{datasets_table}` WHERE `is_deleted` = TRUE"
    ))
    .fetch_all(executor)
    .await?)
}

/// Deletes a dataset's rows from the archives, long-span archives and column-metadata tables,
/// leaving its row in the datasets table.
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
    dataset_id: DatasetId,
) -> Result<u64, crate::Error> {
    let mut num_deleted_rows = 0;
    for table in [
        database_config.long_span_archives_table_name(),
        database_config.unscoped_archives_table_name(),
        database_config.unscoped_column_metadata_table_name(),
    ] {
        num_deleted_rows += sqlx::query(&format!("DELETE FROM `{table}` WHERE `dataset_id` = ?"))
            .bind(dataset_id)
            .execute(&mut **tx)
            .await?
            .rows_affected();
    }

    Ok(num_deleted_rows)
}
