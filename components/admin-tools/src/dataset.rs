//! Lookups and soft deletion of datasets in the datasets table.

use clp_rust_utils::clp_config::package::config::Database as DatabaseConfig;
use clp_rust_utils::dataset::DatasetName;

/// A dataset in the datasets table.
#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
pub struct Dataset {
    pub id: u16,
    pub name: DatasetName,
    pub archive_storage_path: String,
}

/// # Returns
///
/// Every dataset that isn't deleted on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::query::QueryAs::fetch_all`]'s return values on failure.
pub async fn fetch_existing(
    db_pool: &sqlx::MySqlPool,
    database_config: &DatabaseConfig,
) -> Result<Vec<Dataset>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT `id`, `name`, `archive_storage_path` FROM `{}` WHERE NOT `is_deleted`",
        database_config.datasets_table_name()
    ))
    .fetch_all(db_pool)
    .await
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
    dataset_id: u16,
) -> Result<(), sqlx::Error> {
    sqlx::query(&format!(
        "UPDATE `{}` SET `is_deleted` = TRUE WHERE `id` = ?",
        database_config.datasets_table_name()
    ))
    .bind(dataset_id)
    .execute(db_pool)
    .await?;
    Ok(())
}
