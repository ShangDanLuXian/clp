//! Lookups of a dataset's archives in the clp-s archives table.

use clp_rust_utils::clp_config::package::config::Database as DatabaseConfig;
use clp_rust_utils::types::ArchiveId;
use clp_rust_utils::types::UuidBytes;
use sqlx::Row;
use sqlx::mysql::MySqlRow;

/// An archive in the archives table.
#[derive(Debug)]
pub struct Archive {
    pub id: u64,
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

/// Finds the archives of a dataset whose timestamp ranges begin at or after `begin_ts` and, if
/// `end_ts` is given, end at or before `end_ts`.
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
    dataset_id: u16,
    begin_ts: i64,
    end_ts: Option<i64>,
) -> Result<Vec<Archive>, sqlx::Error> {
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
    query_builder.build_query_as().fetch_all(connection).await
}

/// Finds the archives of a dataset whose UUIDs are in `uuids`.
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
    dataset_id: u16,
    uuids: &[ArchiveId],
) -> Result<Vec<Archive>, sqlx::Error> {
    const BATCH_SIZE: usize = 1000;

    let archives_table = database_config.unscoped_archives_table_name();
    let mut archives = Vec::new();
    for uuids in uuids.chunks(BATCH_SIZE) {
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
