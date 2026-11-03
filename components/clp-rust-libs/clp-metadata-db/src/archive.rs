//! Operations on the clp-s archives tables that every dataset shares.

use clp_rust_utils::dataset::DatasetId;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::types::ArchiveId;
use clp_rust_utils::types::UuidBytes;
use sqlx::Row;
use sqlx::mysql::MySqlRow;

use crate::Error;
use crate::MetadataTables;

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

/// An archive that has outlived its dataset's retention period.
#[derive(Debug, sqlx::FromRow)]
pub struct ExpiredArchive {
    /// The archive.
    #[sqlx(flatten)]
    pub archive: Archive,
    /// The ID of the archive's dataset.
    pub dataset_id: DatasetId,
    /// The name of the archive's dataset.
    pub dataset_name: DatasetName,
}

impl MetadataTables {
    /// Finds a dataset's archives whose timestamp ranges begin at or after `begin_ts` and, if
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
    pub async fn find_archives_by_time_range(
        &self,
        connection: &mut sqlx::MySqlConnection,
        dataset_id: DatasetId,
        begin_ts: i64,
        end_ts: Option<i64>,
    ) -> Result<Vec<Archive>, Error> {
        let mut query_builder = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
            "SELECT `id`, `uuid` FROM `{}` WHERE `dataset_id` = ",
            self.archives
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
    pub async fn find_archives_by_uuids(
        &self,
        connection: &mut sqlx::MySqlConnection,
        dataset_id: DatasetId,
        uuids: &[ArchiveId],
    ) -> Result<Vec<Archive>, Error> {
        let archives_table = &self.archives;
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
    pub async fn delete_archives(
        &self,
        connection: &mut sqlx::MySqlConnection,
        dataset_id: DatasetId,
        ids: &[u64],
    ) -> Result<(), Error> {
        let statement_prefixes = [
            format!(
                "DELETE FROM `{}` WHERE `archive_id` IN (",
                self.long_span_archives
            ),
            format!("DELETE FROM `{}` WHERE `id` IN (", self.archives),
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
                query_builder.build().execute(&mut *connection).await?;
            }
        }
        Ok(())
    }

    /// # Returns
    ///
    /// The archives of the datasets that aren't deleted and have a retention period, which were
    /// created earlier than `expiry_base_millis` minus their dataset's retention period, on
    /// success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryAs::fetch_all`]'s return values on failure.
    pub async fn find_expired_archives(
        &self,
        connection: &mut sqlx::MySqlConnection,
        expiry_base_millis: i64,
    ) -> Result<Vec<ExpiredArchive>, Error> {
        const MILLISECS_PER_MINUTE: i64 = 60 * 1000;

        let archives_table = &self.archives;
        let datasets_table = &self.datasets;
        Ok(sqlx::query_as(&format!(
            "SELECT `archives`.`id`, `archives`.`uuid`, `archives`.`dataset_id`, \
             `datasets`.`name` AS `dataset_name` FROM `{archives_table}` AS `archives` JOIN \
             `{datasets_table}` AS `datasets` ON `archives`.`dataset_id` = `datasets`.`id` WHERE \
             `datasets`.`deleted_at_millis` = 0 AND `datasets`.`retention_period_minutes` IS NOT \
             NULL AND `archives`.`creation_time_millis` < ? - \
             CAST(`datasets`.`retention_period_minutes` AS SIGNED) * {MILLISECS_PER_MINUTE}"
        ))
        .bind(expiry_base_millis)
        .fetch_all(connection)
        .await?)
    }
}

/// The maximum number of values in an `IN` list of a query.
const MAX_NUM_IN_LIST_VALUES: usize = 1000;
