//! The archive operations: [`ArchiveLookup`] and [`ArchiveManagement`].

use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroU32;

use async_trait::async_trait;
use clp_rust_utils::clp_config::package::config::MetadataTables;
use clp_rust_utils::job_config::CompressionJobId;
use clp_rust_utils::job_config::TimeRange;
use clp_rust_utils::types::ArchiveId;
use clp_rust_utils::types::ArchiveUuid;
use clp_rust_utils::types::DatasetId;
use sqlx::MySql;
use sqlx::MySqlConnection;
use sqlx::QueryBuilder;

use super::ArchiveLookup;
use super::ArchiveManagement;
use super::MAX_BATCH_SIZE;
use crate::client::ClpDbTransaction;
use crate::client::Handle;
use crate::client::HandleParts;
use crate::error::Error;
use crate::stored::StoredArchiveUuid;
use crate::types::ArchiveDescriptor;
use crate::types::ArchiveMetadata;
use crate::types::ArchiveTimestampRange;
use crate::types::InvalidArchiveTimestampRangeError;
use crate::types::QueryArchive;

/// The span above which an archive also gets a `clp_long_span_archives` row (24 hours).
const MAX_SPAN_MILLISECS: u64 = 24 * 60 * 60 * 1000;

/// The columns that [`ArchiveMetadataRow`] projects from the archives table aliased `a`, in a
/// `SELECT` list.
const ARCHIVE_METADATA_COLUMNS: &str =
    "a.`id`, a.`dataset_id`, a.`uuid`, a.`begin_timestamp_millisecs`, a.`end_timestamp_millisecs`";

#[async_trait]
impl<HandleType: Handle> ArchiveLookup for HandleType {
    async fn find_archives_for_query(
        &mut self,
        dataset_id: DatasetId,
        time_range: &TimeRange,
        retention_period_minutes: Option<NonZeroU32>,
        job_creation_timestamp_millisecs: i64,
    ) -> Result<Vec<QueryArchive>, Error> {
        const MILLISECS_PER_MINUTE: u64 = 60 * 1000;

        let HandleParts { tables, connection } = self.parts();
        let mut query_builder = QueryBuilder::<MySql>::new(format!(
            "SELECT a.`id`, a.`uuid`, a.`num_uncompressed_bytes`, a.`end_timestamp_millisecs` \
             FROM `{}` a JOIN `{}` d ON d.`id` = a.`dataset_id` WHERE d.`is_deleted` = FALSE AND \
             a.`dataset_id` = ",
            tables.archives(),
            tables.datasets()
        ));
        query_builder.push_bind(dataset_id);
        if let Some(end_timestamp) = time_range.end_timestamp_millisecs() {
            query_builder
                .push(" AND a.`begin_timestamp_millisecs` <= ")
                .push_bind(end_timestamp);
        }
        if let Some(begin_timestamp) = time_range.begin_timestamp_millisecs() {
            query_builder
                .push(" AND a.`end_timestamp_millisecs` >= ")
                .push_bind(begin_timestamp);
        }
        if let Some(retention_period_minutes) = retention_period_minutes {
            let retention_cutoff_millisecs = job_creation_timestamp_millisecs
                .saturating_sub_unsigned(
                    u64::from(retention_period_minutes.get()).saturating_mul(MILLISECS_PER_MINUTE),
                );
            query_builder
                .push(" AND (a.`end_timestamp_millisecs` >= ")
                .push_bind(retention_cutoff_millisecs)
                .push(" OR a.`end_timestamp_millisecs` = ")
                .push_bind(ArchiveTimestampRange::WITHOUT_TIMESTAMPS.end_timestamp_millisecs())
                .push(")");
        }
        query_builder.push(" ORDER BY a.`end_timestamp_millisecs` DESC");

        Ok(query_builder
            .build_query_as::<QueryArchiveRow>()
            .fetch_all(connection)
            .await?
            .into_iter()
            .map(QueryArchive::from)
            .collect())
    }

    async fn find_archives_by_time_range(
        &mut self,
        dataset_id: DatasetId,
        time_range: &TimeRange,
    ) -> Result<Vec<ArchiveMetadata>, Error> {
        let HandleParts { tables, connection } = self.parts();
        let mut query_builder = QueryBuilder::<MySql>::new(format!(
            "SELECT {ARCHIVE_METADATA_COLUMNS} FROM `{}` a JOIN `{}` d ON d.`id` = a.`dataset_id` \
             WHERE d.`is_deleted` = FALSE AND a.`dataset_id` = ",
            tables.archives(),
            tables.datasets()
        ));
        query_builder.push_bind(dataset_id);
        if let Some(begin_timestamp) = time_range.begin_timestamp_millisecs() {
            query_builder
                .push(" AND a.`begin_timestamp_millisecs` >= ")
                .push_bind(begin_timestamp);
        }
        if let Some(end_timestamp) = time_range.end_timestamp_millisecs() {
            query_builder
                .push(" AND a.`end_timestamp_millisecs` <= ")
                .push_bind(end_timestamp);
        }
        query_builder.push(" ORDER BY a.`begin_timestamp_millisecs`, a.`id`");

        query_builder
            .build_query_as::<ArchiveMetadataRow>()
            .fetch_all(connection)
            .await?
            .into_iter()
            .map(|row| Ok(ArchiveMetadata::try_from(row)?))
            .collect()
    }

    async fn find_archives_by_ids(
        &mut self,
        ids: &[ArchiveId],
    ) -> Result<Vec<Option<ArchiveMetadata>>, Error> {
        let HandleParts { tables, connection } = self.parts();
        let mut archives = HashMap::new();
        for batch in ids.chunks(MAX_BATCH_SIZE) {
            let mut query_builder = QueryBuilder::<MySql>::new(format!(
                "SELECT {ARCHIVE_METADATA_COLUMNS} FROM `{}` a JOIN `{}` d ON d.`id` = \
                 a.`dataset_id` WHERE d.`is_deleted` = FALSE AND a.`id` IN (",
                tables.archives(),
                tables.datasets()
            ));
            let mut separated_ids = query_builder.separated(", ");
            for id in batch {
                separated_ids.push_bind(id);
            }
            query_builder.push(")");
            for row in query_builder
                .build_query_as::<ArchiveMetadataRow>()
                .fetch_all(&mut *connection)
                .await?
            {
                let archive = ArchiveMetadata::try_from(row)?;
                archives.insert(archive.id, archive);
            }
        }
        Ok(ids.iter().map(|id| archives.get(id).copied()).collect())
    }

    async fn find_archive_ids_by_uuids(
        &mut self,
        dataset_id: DatasetId,
        uuids: &[ArchiveUuid],
    ) -> Result<Vec<Option<ArchiveId>>, Error> {
        let HandleParts { tables, connection } = self.parts();
        let mut ids = HashMap::new();
        for batch in uuids.chunks(MAX_BATCH_SIZE) {
            let mut query_builder = QueryBuilder::<MySql>::new(format!(
                "SELECT a.`id`, a.`uuid` FROM `{}` a JOIN `{}` d ON d.`id` = a.`dataset_id` WHERE \
                 d.`is_deleted` = FALSE AND a.`dataset_id` = ",
                tables.archives(),
                tables.datasets()
            ));
            query_builder
                .push_bind(dataset_id)
                .push(" AND a.`uuid` IN (");
            let mut separated_uuids = query_builder.separated(", ");
            for uuid in batch {
                separated_uuids.push_bind(uuid.as_bytes().as_slice());
            }
            query_builder.push(")");
            for row in query_builder
                .build_query_as::<ArchiveIdRow>()
                .fetch_all(&mut *connection)
                .await?
            {
                let uuid = row.uuid.value;
                // The same row comes back again when `uuids` repeats a UUID across batches.
                if ids
                    .insert(uuid, row.id)
                    .is_some_and(|other_id| other_id != row.id)
                {
                    return Err(Error::AmbiguousArchiveUuid { uuid });
                }
            }
        }
        Ok(uuids.iter().map(|uuid| ids.get(uuid).copied()).collect())
    }
}

#[async_trait]
impl ArchiveManagement for ClpDbTransaction<'_> {
    async fn insert_archives(
        &mut self,
        dataset_id: DatasetId,
        compression_job_id: CompressionJobId,
        archives: &[ArchiveDescriptor],
    ) -> Result<(), Error> {
        let HandleParts { tables, connection } = self.parts();

        // The shared lock makes a concurrent `mark_dataset_deleted` wait until this transaction
        // ends, and the locking read sees a mark committed after this transaction's snapshot, so no
        // archive is inserted into a dataset once its mark commits.
        let lock_query = format!(
            "SELECT `is_deleted` FROM `{}` WHERE `id` = ? LOCK IN SHARE MODE",
            tables.datasets()
        );
        let is_deleted = sqlx::query_scalar::<_, bool>(&lock_query)
            .bind(dataset_id)
            .fetch_optional(&mut *connection)
            .await?
            .ok_or(Error::DatasetNotFound)?;
        if is_deleted {
            return Err(Error::DatasetDeleted);
        }

        // A long-span archive is inserted on its own, so that `LAST_INSERT_ID()` is its ID. The IDs
        // of a multi-row insert aren't guaranteed to be consecutive.
        let mut insertion = ArchiveInsertion {
            tables,
            connection: &mut *connection,
            dataset_id,
            compression_job_id,
        };
        let mut long_span_archives = Vec::new();
        let mut batch = Vec::with_capacity(MAX_BATCH_SIZE.min(archives.len()));
        for archive in archives {
            if archive.timestamp_range.span_millisecs() <= MAX_SPAN_MILLISECS {
                batch.push(archive);
                if batch.len() == MAX_BATCH_SIZE {
                    insertion.insert_rows(&batch).await?;
                    batch.clear();
                }
                continue;
            }
            if !batch.is_empty() {
                insertion.insert_rows(&batch).await?;
                batch.clear();
            }
            let id = insertion.insert_rows(&[archive]).await?;
            long_span_archives.push(LongSpanArchive {
                id,
                timestamp_range: archive.timestamp_range,
            });
        }
        if !batch.is_empty() {
            insertion.insert_rows(&batch).await?;
        }

        for long_span_batch in long_span_archives.chunks(MAX_BATCH_SIZE) {
            let mut query_builder = QueryBuilder::<MySql>::new(format!(
                "INSERT INTO `{}` (`dataset_id`, `begin_timestamp_millisecs`, \
                 `end_timestamp_millisecs`, `archive_id`) ",
                tables.long_span_archives()
            ));
            query_builder.push_values(long_span_batch, |mut row, archive| {
                row.push_bind(dataset_id)
                    .push_bind(archive.timestamp_range.begin_timestamp_millisecs())
                    .push_bind(archive.timestamp_range.end_timestamp_millisecs())
                    .push_bind(archive.id);
            });
            query_builder.build().execute(&mut *connection).await?;
        }
        Ok(())
    }

    async fn delete_archives(&mut self, ids: &[ArchiveId]) -> Result<Vec<ArchiveId>, Error> {
        let HandleParts { tables, connection } = self.parts();

        let mut found_ids = HashSet::new();
        for batch in ids.chunks(MAX_BATCH_SIZE) {
            // The exclusive locks keep the rows from changing until they're deleted below, and the
            // read gives the full primary keys of both tables' rows.
            let mut select_builder = QueryBuilder::<MySql>::new(format!(
                "SELECT `id`, `dataset_id`, `begin_timestamp_millisecs`, \
                 `end_timestamp_millisecs` FROM `{}` WHERE `id` IN (",
                tables.archives()
            ));
            let mut separated_ids = select_builder.separated(", ");
            for id in batch {
                separated_ids.push_bind(id);
            }
            select_builder.push(") FOR UPDATE");
            let archives = select_builder
                .build_query_as::<ArchiveKeyRow>()
                .fetch_all(&mut *connection)
                .await?
                .into_iter()
                .map(ArchiveKey::try_from)
                .collect::<Result<Vec<_>, _>>()?;
            if archives.is_empty() {
                continue;
            }

            let long_span_archives: Vec<&ArchiveKey> = archives
                .iter()
                .filter(|archive| archive.timestamp_range.span_millisecs() > MAX_SPAN_MILLISECS)
                .collect();
            if !long_span_archives.is_empty() {
                let mut long_span_builder = QueryBuilder::<MySql>::new(format!(
                    "DELETE FROM `{}` WHERE (`dataset_id`, `end_timestamp_millisecs`, \
                     `archive_id`) IN ",
                    tables.long_span_archives()
                ));
                long_span_builder.push_tuples(long_span_archives, |mut key, archive| {
                    key.push_bind(archive.dataset_id)
                        .push_bind(archive.timestamp_range.end_timestamp_millisecs())
                        .push_bind(archive.id);
                });
                long_span_builder.build().execute(&mut *connection).await?;
            }

            let mut archives_builder = QueryBuilder::<MySql>::new(format!(
                "DELETE FROM `{}` WHERE (`dataset_id`, `begin_timestamp_millisecs`, `id`) IN ",
                tables.archives()
            ));
            archives_builder.push_tuples(&archives, |mut key, archive| {
                key.push_bind(archive.dataset_id)
                    .push_bind(archive.timestamp_range.begin_timestamp_millisecs())
                    .push_bind(archive.id);
            });
            archives_builder.build().execute(&mut *connection).await?;

            found_ids.extend(archives.iter().map(|archive| archive.id));
        }
        Ok(ids
            .iter()
            .copied()
            .filter(|id| !found_ids.contains(id))
            .collect())
    }
}

/// The target of the archives rows that [`ArchiveManagement::insert_archives`] inserts.
///
/// # Type Parameters
///
/// * `'handle` - The lifetime of the borrowed transaction.
struct ArchiveInsertion<'handle> {
    tables: &'handle MetadataTables,
    connection: &'handle mut MySqlConnection,
    dataset_id: DatasetId,
    compression_job_id: CompressionJobId,
}

impl ArchiveInsertion<'_> {
    /// Inserts `archives` into the archives table in one statement, with the DB clock as their
    /// creation time.
    ///
    /// # Returns
    ///
    /// The ID of the first inserted archive on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if `archives` is empty.
    async fn insert_rows(&mut self, archives: &[&ArchiveDescriptor]) -> Result<ArchiveId, Error> {
        assert!(
            !archives.is_empty(),
            "at least one archive must be inserted"
        );
        let mut query_builder = QueryBuilder::<MySql>::new(format!(
            "INSERT INTO `{}` (`dataset_id`, `uuid`, `begin_timestamp_millisecs`, \
             `end_timestamp_millisecs`, `num_uncompressed_bytes`, `num_compressed_bytes`, \
             `creation_timestamp_millisecs`, `compression_job_id`) ",
            self.tables.archives()
        ));
        query_builder.push_values(archives, |mut row, archive| {
            row.push_bind(self.dataset_id)
                .push_bind(archive.uuid.as_bytes().as_slice())
                .push_bind(archive.timestamp_range.begin_timestamp_millisecs())
                .push_bind(archive.timestamp_range.end_timestamp_millisecs())
                .push_bind(archive.num_uncompressed_bytes)
                .push_bind(archive.num_compressed_bytes)
                .push("UNIX_TIMESTAMP(NOW(3)) * 1000")
                .push_bind(self.compression_job_id);
        });
        let result = query_builder.build().execute(&mut *self.connection).await?;
        Ok(ArchiveId::from(result.last_insert_id()))
    }
}

/// The columns of an archives row that a query selects.
#[derive(sqlx::FromRow)]
struct QueryArchiveRow {
    id: ArchiveId,
    #[sqlx(try_from = "Vec<u8>")]
    uuid: StoredArchiveUuid,
    num_uncompressed_bytes: u64,
    end_timestamp_millisecs: i64,
}

impl From<QueryArchiveRow> for QueryArchive {
    fn from(row: QueryArchiveRow) -> Self {
        Self {
            id: row.id,
            uuid: row.uuid.value,
            num_uncompressed_bytes: row.num_uncompressed_bytes,
            end_timestamp_millisecs: row.end_timestamp_millisecs,
        }
    }
}

/// The columns of an archives row that [`ARCHIVE_METADATA_COLUMNS`] lists.
#[derive(sqlx::FromRow)]
struct ArchiveMetadataRow {
    id: ArchiveId,
    dataset_id: DatasetId,
    #[sqlx(try_from = "Vec<u8>")]
    uuid: StoredArchiveUuid,
    begin_timestamp_millisecs: i64,
    end_timestamp_millisecs: i64,
}

impl TryFrom<ArchiveMetadataRow> for ArchiveMetadata {
    type Error = InvalidArchiveTimestampRangeError;

    fn try_from(row: ArchiveMetadataRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            dataset_id: row.dataset_id,
            uuid: row.uuid.value,
            timestamp_range: ArchiveTimestampRange::new(
                row.begin_timestamp_millisecs,
                row.end_timestamp_millisecs,
            )?,
        })
    }
}

/// The ID and UUID of an archives row.
#[derive(sqlx::FromRow)]
struct ArchiveIdRow {
    id: ArchiveId,
    #[sqlx(try_from = "Vec<u8>")]
    uuid: StoredArchiveUuid,
}

/// The columns of an archives row that the primary keys of its archives and long-span archives rows
/// consist of.
#[derive(sqlx::FromRow)]
struct ArchiveKeyRow {
    id: ArchiveId,
    dataset_id: DatasetId,
    begin_timestamp_millisecs: i64,
    end_timestamp_millisecs: i64,
}

/// The validated primary-key columns of an archives row.
struct ArchiveKey {
    id: ArchiveId,
    dataset_id: DatasetId,
    timestamp_range: ArchiveTimestampRange,
}

impl TryFrom<ArchiveKeyRow> for ArchiveKey {
    type Error = InvalidArchiveTimestampRangeError;

    fn try_from(row: ArchiveKeyRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            dataset_id: row.dataset_id,
            timestamp_range: ArchiveTimestampRange::new(
                row.begin_timestamp_millisecs,
                row.end_timestamp_millisecs,
            )?,
        })
    }
}

/// An inserted archive that needs a long-span archives row.
struct LongSpanArchive {
    id: ArchiveId,
    timestamp_range: ArchiveTimestampRange,
}
