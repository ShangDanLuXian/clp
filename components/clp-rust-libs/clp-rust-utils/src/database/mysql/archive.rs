//! Operations on the clp-s archives tables that every dataset shares.

use sqlx::Row;
use sqlx::mysql::MySqlRow;

use crate::clp_config::package::config::Database as DatabaseConfig;
use crate::dataset::DatasetId;
use crate::types::ArchiveId;
use crate::types::UuidBytes;

/// An archive in the archives table.
#[derive(Debug)]
pub struct Archive {
    /// The archive's ID in the archives table.
    pub id: u64,
    /// The archive's UUID.
    pub uuid: ArchiveId,
}

impl sqlx::FromRow<'_, MySqlRow> for Archive {
    fn from_row(row: &MySqlRow) -> Result<Self, sqlx::Error> {
        let uuid: &[u8] = row.try_get("uuid")?;
        let uuid = UuidBytes::try_from(uuid).map_err(|e| sqlx::Error::ColumnDecode {
            index: "uuid".to_owned(),
            source: Box::new(e),
        })?;
        Ok(Self {
            id: row.try_get("id")?,
            uuid: ArchiveId::from_bytes(uuid),
        })
    }
}

/// Finds a dataset's archives whose timestamp ranges begin at or after `begin_ts` and, if `end_ts`
/// is given, end at or before `end_ts`.
///
/// # Returns
///
/// The matching archives on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::query::QueryAs::fetch_all`]'s return values on failure.
pub async fn find_by_time_range(
    connection: &mut sqlx::MySqlConnection,
    database_config: &DatabaseConfig,
    dataset_id: DatasetId,
    begin_ts: i64,
    end_ts: Option<i64>,
) -> Result<Vec<Archive>, crate::Error> {
    let mut query_builder = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
        "SELECT `id`, `uuid` FROM `{}` WHERE `dataset_id` = ",
        database_config.unscoped_archives_table_name()
    ));
    query_builder
        .push_bind(dataset_id)
        .push(" AND `timestamp_range_begin_millis` >= ")
        .push_bind(begin_ts);
    if let Some(end_ts) = end_ts {
        query_builder
            .push(" AND `timestamp_range_end_millis` <= ")
            .push_bind(end_ts);
    }
    Ok(query_builder.build_query_as().fetch_all(connection).await?)
}

/// Finds a dataset's archives whose UUIDs are in `uuids`.
///
/// # Returns
///
/// The matching archives on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::query::QueryAs::fetch_all`]'s return values on failure.
pub async fn find_by_uuids(
    connection: &mut sqlx::MySqlConnection,
    database_config: &DatabaseConfig,
    dataset_id: DatasetId,
    uuids: &[ArchiveId],
) -> Result<Vec<Archive>, crate::Error> {
    let archives_table = database_config.unscoped_archives_table_name();
    let mut archives = Vec::new();
    for uuids in uuids.chunks(MAX_NUM_IN_LIST_VALUES) {
        let mut query_builder = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
            "SELECT `id`, `uuid` FROM `{archives_table}` WHERE `uuid` IN ("
        ));
        let mut separated = query_builder.separated(", ");
        for uuid in uuids {
            separated.push_bind(uuid.as_bytes().as_slice());
        }
        separated.push_unseparated(") AND `dataset_id` = ");
        query_builder.push_bind(dataset_id);
        archives.extend(
            query_builder
                .build_query_as::<Archive>()
                .fetch_all(&mut *connection)
                .await?,
        );
    }
    Ok(archives)
}

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
    ids: &[u64],
) -> Result<(), crate::Error> {
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
    for ids in ids.chunks(MAX_NUM_IN_LIST_VALUES) {
        for statement_prefix in &statement_prefixes {
            let mut query_builder = sqlx::QueryBuilder::<sqlx::MySql>::new(statement_prefix);
            let mut separated = query_builder.separated(", ");
            for id in ids {
                separated.push_bind(*id);
            }
            separated.push_unseparated(") AND `dataset_id` = ");
            query_builder.push_bind(dataset_id);
            query_builder.build().execute(&mut **tx).await?;
        }
    }
    Ok(())
}

/// The maximum number of values in an `IN` list of a query.
const MAX_NUM_IN_LIST_VALUES: usize = 1000;
