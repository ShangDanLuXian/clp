//! Operations on the archives table and the long-span archives table.

use clp_rust_utils::job_config::CompressionJobId;
use clp_rust_utils::types::ArchiveId;
use clp_rust_utils::types::UuidBytes;
use sqlx::MySql;
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::mysql::MySqlTypeInfo;
use sqlx::mysql::MySqlValueRef;

use crate::Archive;
use crate::ArchiveRowId;
use crate::DatasetId;
use crate::Error;
use crate::MarkedArchive;
use crate::MetadataTables;
use crate::NewArchive;
use crate::QueryArchive;
use crate::QueryArchiveFilter;
use crate::TimestampRange;

/// The DB clock's current time, in milliseconds since the Unix epoch.
///
/// Unlike `UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))`, it doesn't depend on the session's time zone, so
/// it stays correct during a repeated daylight-saving hour.
const DB_NOW_MILLISECS: &str =
    "(TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(6)) DIV 1000)";

const MAX_NUM_ITEMS_PER_QUERY: usize = 1000;

const MILLISECS_PER_MINUTE: i64 = 60 * 1000;

impl MetadataTables {
    /// Finds a dataset's live archives whose timestamp ranges lie within `range`.
    ///
    /// Archives are selected the same way as `archive_manager.py` selects them.
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
        conn: &mut sqlx::MySqlConnection,
        dataset_id: DatasetId,
        range: &TimestampRange,
    ) -> Result<Vec<Archive>, Error> {
        let query = format!(
            r"
SELECT `id`, `uuid`, `begin_timestamp_millisecs`, `end_timestamp_millisecs` FROM `{archives}`
WHERE `dataset_id` = ?
    AND `is_deleted` = FALSE
    AND `begin_timestamp_millisecs` >= ?
    AND `end_timestamp_millisecs` <= ?",
            archives = self.archives,
        );
        let rows: Vec<(ArchiveRowId, BinaryArchiveId, i64, i64)> = sqlx::query_as(&query)
            .bind(dataset_id)
            .bind(range.begin_timestamp_millisecs)
            .bind(range.end_timestamp_millisecs)
            .fetch_all(&mut *conn)
            .await?;
        Ok(rows.into_iter().map(archive_from_row).collect())
    }

    /// Finds a dataset's live archives among `uuids`.
    ///
    /// # Returns
    ///
    /// The matching archives on success. UUIDs that don't match a live archive of the dataset are
    /// absent.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryAs::fetch_all`]'s return values on failure.
    pub async fn find_archives_by_uuids(
        &self,
        conn: &mut sqlx::MySqlConnection,
        dataset_id: DatasetId,
        uuids: &[ArchiveId],
    ) -> Result<Vec<Archive>, Error> {
        let mut archives = Vec::new();
        for chunk in uuids.chunks(MAX_NUM_ITEMS_PER_QUERY) {
            let mut query_builder = sqlx::QueryBuilder::<MySql>::new(format!(
                r"
SELECT `id`, `uuid`, `begin_timestamp_millisecs`, `end_timestamp_millisecs` FROM `{archives}`
WHERE `is_deleted` = FALSE AND `dataset_id` = ",
                archives = self.archives,
            ));
            query_builder.push_bind(dataset_id).push(" AND `uuid` IN (");
            let mut separated_uuids = query_builder.separated(", ");
            for uuid in chunk {
                separated_uuids.push_bind(BinaryArchiveId(*uuid));
            }
            query_builder.push(")");
            archives.extend(
                query_builder
                    .build_query_as::<(ArchiveRowId, BinaryArchiveId, i64, i64)>()
                    .fetch_all(&mut *conn)
                    .await?
                    .into_iter()
                    .map(archive_from_row),
            );
        }
        Ok(archives)
    }

    /// Marks a dataset's live archives among `ids` as deleted.
    ///
    /// Once the mark commits, normal reads skip the archives. The caller is expected to run this in
    /// the same transaction as the lookup that selected `ids`.
    ///
    /// # Returns
    ///
    /// The number of archives marked on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    pub async fn mark_archives_deleted(
        &self,
        conn: &mut sqlx::MySqlConnection,
        dataset_id: DatasetId,
        ids: &[ArchiveRowId],
    ) -> Result<u64, Error> {
        let mut num_marked_archives = 0;
        for chunk in ids.chunks(MAX_NUM_ITEMS_PER_QUERY) {
            let mut query_builder = sqlx::QueryBuilder::<MySql>::new(format!(
                r"
UPDATE `{archives}` SET `is_deleted` = TRUE
WHERE `is_deleted` = FALSE AND `dataset_id` = ",
                archives = self.archives,
            ));
            query_builder.push_bind(dataset_id).push(" AND `id` IN (");
            let mut separated_ids = query_builder.separated(", ");
            for id in chunk {
                separated_ids.push_bind(*id);
            }
            query_builder.push(")");
            num_marked_archives += query_builder
                .build()
                .execute(&mut *conn)
                .await?
                .rows_affected();
        }
        Ok(num_marked_archives)
    }

    /// Inserts the archives that a compression job produced into a dataset.
    ///
    /// Each archive's creation time comes from the DB clock, and each archive whose timestamp range
    /// spans more than 24 hours also gets a long-span archive row. The caller is expected to run
    /// this in a transaction, after [`Self::lock_live_dataset`] and together with the job's status
    /// update.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryScalar::fetch_one`]'s return values on failure.
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    pub async fn insert_archives(
        &self,
        conn: &mut sqlx::MySqlConnection,
        dataset_id: DatasetId,
        compression_job_id: CompressionJobId,
        archives: &[NewArchive],
    ) -> Result<(), Error> {
        const LONG_SPAN_THRESHOLD_MILLISECS: i64 = 24 * 60 * MILLISECS_PER_MINUTE;

        if archives.is_empty() {
            return Ok(());
        }

        let creation_timestamp_millisecs: i64 =
            sqlx::query_scalar(&format!("SELECT {DB_NOW_MILLISECS}"))
                .fetch_one(&mut *conn)
                .await?;

        for chunk in archives.chunks(MAX_NUM_ITEMS_PER_QUERY) {
            let mut query_builder = sqlx::QueryBuilder::<MySql>::new(format!(
                r"
INSERT INTO `{archives}` (
    `dataset_id`,
    `uuid`,
    `begin_timestamp_millisecs`,
    `end_timestamp_millisecs`,
    `num_uncompressed_bytes`,
    `num_compressed_bytes`,
    `creation_timestamp_millisecs`,
    `compression_job_id`
) ",
                archives = self.archives,
            ));
            query_builder.push_values(chunk, |mut row, archive| {
                row.push_bind(dataset_id)
                    .push_bind(BinaryArchiveId(archive.uuid))
                    .push_bind(archive.begin_timestamp_millisecs)
                    .push_bind(archive.end_timestamp_millisecs)
                    .push_bind(archive.num_uncompressed_bytes)
                    .push_bind(archive.num_compressed_bytes)
                    .push_bind(creation_timestamp_millisecs)
                    .push_bind(compression_job_id);
            });
            query_builder.build().execute(&mut *conn).await?;
        }

        // A multi-row insert's auto-increment IDs aren't necessarily consecutive, so the new rows
        // are looked up by UUID.
        let long_span_archives: Vec<&NewArchive> = archives
            .iter()
            .filter(|archive| {
                archive
                    .end_timestamp_millisecs
                    .saturating_sub(archive.begin_timestamp_millisecs)
                    > LONG_SPAN_THRESHOLD_MILLISECS
            })
            .collect();
        for chunk in long_span_archives.chunks(MAX_NUM_ITEMS_PER_QUERY) {
            let mut query_builder = sqlx::QueryBuilder::<MySql>::new(format!(
                r"
INSERT INTO `{long_span_archives}` (
    `dataset_id`, `begin_timestamp_millisecs`, `end_timestamp_millisecs`, `archive_id`
)
SELECT `dataset_id`, `begin_timestamp_millisecs`, `end_timestamp_millisecs`, `id`
FROM `{archives}`
WHERE `dataset_id` = ",
                long_span_archives = self.long_span_archives,
                archives = self.archives,
            ));
            query_builder.push_bind(dataset_id).push(" AND `uuid` IN (");
            let mut separated_uuids = query_builder.separated(", ");
            for archive in chunk {
                separated_uuids.push_bind(BinaryArchiveId(archive.uuid));
            }
            query_builder.push(")");
            query_builder.build().execute(&mut *conn).await?;
        }
        Ok(())
    }

    /// Finds the archives of a live dataset that a query job searches.
    ///
    /// An archive matches if it's live, its timestamp range overlaps the filter's time range, and
    /// it was created within the dataset's retention period before the job's creation.
    ///
    /// # Returns
    ///
    /// The matching archives on success, or none if the dataset doesn't exist or is marked as
    /// deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryAs::fetch_all`]'s return values on failure.
    pub async fn find_archives_for_query(
        &self,
        conn: &mut sqlx::MySqlConnection,
        dataset_id: DatasetId,
        filter: &QueryArchiveFilter,
    ) -> Result<Vec<QueryArchive>, Error> {
        // The retention period is cast to signed so that a bound before the Unix epoch doesn't
        // overflow unsigned arithmetic.
        let mut query_builder = sqlx::QueryBuilder::<MySql>::new(format!(
            r"
SELECT `a`.`uuid`, `a`.`num_uncompressed_bytes`, `a`.`end_timestamp_millisecs`
FROM `{archives}` AS `a`
JOIN `{datasets}` AS `d` ON `d`.`id` = `a`.`dataset_id`
WHERE `a`.`is_deleted` = FALSE AND `d`.`is_deleted` = FALSE AND `a`.`dataset_id` = ",
            archives = self.archives,
            datasets = self.datasets,
        ));
        query_builder
            .push_bind(dataset_id)
            .push(
                " AND (`d`.`retention_period_minutes` IS NULL OR \
                 `a`.`creation_timestamp_millisecs` >= ",
            )
            .push_bind(filter.job_creation_timestamp_millisecs)
            .push(format!(
                " - CAST(`d`.`retention_period_minutes` AS SIGNED) * {MILLISECS_PER_MINUTE})"
            ));
        if let Some(end_timestamp_millisecs) = filter.end_timestamp_millisecs {
            query_builder
                .push(" AND `a`.`begin_timestamp_millisecs` <= ")
                .push_bind(end_timestamp_millisecs);
        }
        if let Some(begin_timestamp_millisecs) = filter.begin_timestamp_millisecs {
            query_builder
                .push(" AND `a`.`end_timestamp_millisecs` >= ")
                .push_bind(begin_timestamp_millisecs);
        }

        Ok(query_builder
            .build_query_as::<(BinaryArchiveId, u64, i64)>()
            .fetch_all(&mut *conn)
            .await?
            .into_iter()
            .map(
                |(uuid, num_uncompressed_bytes, end_timestamp_millisecs)| QueryArchive {
                    uuid: uuid.0,
                    num_uncompressed_bytes,
                    end_timestamp_millisecs,
                },
            )
            .collect())
    }

    /// Marks expired archives as deleted, across every dataset.
    ///
    /// The expiry base is the DB's current time, or
    /// `earliest_query_job_creation_timestamp_millisecs` if that is earlier. An archive expires if
    /// it's live, its dataset is live and has a retention period, and it was created before the
    /// expiry base minus that retention period.
    ///
    /// # Returns
    ///
    /// The number of archives marked on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    pub async fn mark_expired_archives_deleted(
        &self,
        conn: &mut sqlx::MySqlConnection,
        earliest_query_job_creation_timestamp_millisecs: Option<i64>,
    ) -> Result<u64, Error> {
        // The retention period is cast to signed so that a bound before the Unix epoch doesn't
        // overflow unsigned arithmetic.
        let query = format!(
            r"
UPDATE `{archives}` AS `a`
JOIN `{datasets}` AS `d` ON `d`.`id` = `a`.`dataset_id`
SET `a`.`is_deleted` = TRUE
WHERE `a`.`is_deleted` = FALSE
    AND `d`.`is_deleted` = FALSE
    AND `a`.`creation_timestamp_millisecs`
        < LEAST({DB_NOW_MILLISECS}, COALESCE(?, {DB_NOW_MILLISECS}))
            - CAST(`d`.`retention_period_minutes` AS SIGNED) * {MILLISECS_PER_MINUTE}",
            archives = self.archives,
            datasets = self.datasets,
        );
        let result = sqlx::query(&query)
            .bind(earliest_query_job_creation_timestamp_millisecs)
            .execute(&mut *conn)
            .await?;
        Ok(result.rows_affected())
    }

    /// Gets archives marked as deleted whose datasets aren't marked, across every dataset.
    ///
    /// Archives of marked datasets are cleaned up with their datasets instead.
    ///
    /// # Returns
    ///
    /// Up to `limit` marked archives on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryAs::fetch_all`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if `limit` doesn't fit in [`u64`].
    pub async fn get_marked_archives(
        &self,
        conn: &mut sqlx::MySqlConnection,
        limit: usize,
    ) -> Result<Vec<MarkedArchive>, Error> {
        // `sqlx` doesn't decode strings from `utf8mb4_bin` columns, which are reported as binary.
        let query = format!(
            r"
SELECT
    `a`.`id`,
    `a`.`uuid`,
    `a`.`dataset_id`,
    CONVERT(`d`.`archive_storage_path` USING utf8mb4)
FROM `{archives}` AS `a`
JOIN `{datasets}` AS `d` ON `d`.`id` = `a`.`dataset_id`
WHERE `a`.`is_deleted` = TRUE AND `d`.`is_deleted` = FALSE
LIMIT ?",
            archives = self.archives,
            datasets = self.datasets,
        );
        let rows: Vec<(ArchiveRowId, BinaryArchiveId, DatasetId, String)> = sqlx::query_as(&query)
            .bind(u64::try_from(limit).expect("`usize` should fit in `u64`"))
            .fetch_all(&mut *conn)
            .await?;
        Ok(rows
            .into_iter()
            .map(
                |(id, uuid, dataset_id, archive_storage_path)| MarkedArchive {
                    id,
                    uuid: uuid.0,
                    dataset_id,
                    archive_storage_path,
                },
            )
            .collect())
    }

    /// Removes a dataset's archives among `ids` that are marked as deleted, along with their
    /// long-span archive rows.
    ///
    /// Archives that aren't marked are left as is. The caller is expected to run this in a
    /// transaction.
    ///
    /// # Returns
    ///
    /// The number of archives removed on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    pub async fn delete_marked_archives(
        &self,
        conn: &mut sqlx::MySqlConnection,
        dataset_id: DatasetId,
        ids: &[ArchiveRowId],
    ) -> Result<u64, Error> {
        let mut num_deleted_archives = 0;
        for chunk in ids.chunks(MAX_NUM_ITEMS_PER_QUERY) {
            let mut long_span_query_builder = sqlx::QueryBuilder::<MySql>::new(format!(
                r"
DELETE `l` FROM `{long_span_archives}` AS `l`
JOIN `{archives}` AS `a` ON `a`.`id` = `l`.`archive_id`
WHERE `a`.`is_deleted` = TRUE AND `l`.`dataset_id` = ",
                long_span_archives = self.long_span_archives,
                archives = self.archives,
            ));
            long_span_query_builder
                .push_bind(dataset_id)
                .push(" AND `a`.`dataset_id` = ")
                .push_bind(dataset_id)
                .push(" AND `a`.`id` IN (");
            let mut separated_ids = long_span_query_builder.separated(", ");
            for id in chunk {
                separated_ids.push_bind(*id);
            }
            long_span_query_builder.push(")");
            long_span_query_builder.build().execute(&mut *conn).await?;

            let mut archive_query_builder = sqlx::QueryBuilder::<MySql>::new(format!(
                r"
DELETE FROM `{archives}`
WHERE `is_deleted` = TRUE AND `dataset_id` = ",
                archives = self.archives,
            ));
            archive_query_builder
                .push_bind(dataset_id)
                .push(" AND `id` IN (");
            let mut separated_ids = archive_query_builder.separated(", ");
            for id in chunk {
                separated_ids.push_bind(*id);
            }
            archive_query_builder.push(")");
            num_deleted_archives += archive_query_builder
                .build()
                .execute(&mut *conn)
                .await?
                .rows_affected();
        }
        Ok(num_deleted_archives)
    }
}

/// An [`ArchiveId`] stored as a `BINARY(16)` column value.
struct BinaryArchiveId(ArchiveId);

impl sqlx::Type<MySql> for BinaryArchiveId {
    fn type_info() -> MySqlTypeInfo {
        <[u8] as sqlx::Type<MySql>>::type_info()
    }

    fn compatible(ty: &MySqlTypeInfo) -> bool {
        <[u8] as sqlx::Type<MySql>>::compatible(ty)
    }
}

impl sqlx::Encode<'_, MySql> for BinaryArchiveId {
    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        <&[u8] as sqlx::Encode<MySql>>::encode(self.0.as_bytes().as_slice(), buf)
    }
}

impl<'r> sqlx::Decode<'r, MySql> for BinaryArchiveId {
    fn decode(value: MySqlValueRef<'r>) -> Result<Self, BoxDynError> {
        let bytes = <&[u8] as sqlx::Decode<MySql>>::decode(value)?;
        Ok(Self(ArchiveId::from_bytes(UuidBytes::try_from(bytes)?)))
    }
}

/// # Returns
///
/// The archive in a row of `id`, `uuid`, `begin_timestamp_millisecs` and
/// `end_timestamp_millisecs`.
const fn archive_from_row(
    (id, uuid, begin_timestamp_millisecs, end_timestamp_millisecs): (
        ArchiveRowId,
        BinaryArchiveId,
        i64,
        i64,
    ),
) -> Archive {
    Archive {
        id,
        uuid: uuid.0,
        begin_timestamp_millisecs,
        end_timestamp_millisecs,
    }
}
