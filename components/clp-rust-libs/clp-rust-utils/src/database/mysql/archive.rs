//! Operations on the clp-s archives tables that every dataset shares.

use crate::clp_config::package::config::Database as DatabaseConfig;
use crate::dataset::DatasetId;

/// Deletes the metadata of a dataset's archives, identified by their IDs in the archives table,
/// from the long-span archives table and the archives table.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
pub async fn delete(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    database_config: &DatabaseConfig,
    dataset_id: DatasetId,
    archive_ids: &[u64],
) -> Result<(), crate::Error> {
    const BATCH_SIZE: usize = 1000;

    let statement_prefixes = [
        format!(
            "DELETE FROM `{}` WHERE `archive_id` IN (",
            database_config.long_span_archives_table_name()
        ),
        format!(
            "DELETE FROM `{}` WHERE `id` IN (",
            database_config.unscoped_archives_table_name()
        ),
    ];
    for archive_ids in archive_ids.chunks(BATCH_SIZE) {
        for statement_prefix in &statement_prefixes {
            let mut query_builder = sqlx::QueryBuilder::<sqlx::MySql>::new(statement_prefix);
            let mut separated = query_builder.separated(", ");
            for archive_id in archive_ids {
                separated.push_bind(*archive_id);
            }
            separated.push_unseparated(") AND `dataset_id` = ");
            query_builder.push_bind(dataset_id);
            query_builder.build().execute(&mut **tx).await?;
        }
    }

    Ok(())
}
