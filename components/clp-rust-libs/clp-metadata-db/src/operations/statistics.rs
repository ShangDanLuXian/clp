//! The aggregate reads: [`MetadataStatistics`].

use async_trait::async_trait;
use clp_rust_utils::types::DatasetId;
use sqlx::MySql;
use sqlx::QueryBuilder;

use super::MAX_BATCH_SIZE;
use super::MetadataStatistics;
use crate::client::Handle;
use crate::client::HandleParts;
use crate::error::Error;
use crate::stored::StoredString;
use crate::types::ArchiveTimestampRange;
use crate::types::ColumnType;
use crate::types::SpaceSavings;

#[async_trait]
impl<HandleType: Handle> MetadataStatistics for HandleType {
    async fn get_timestamp_bounds(
        &mut self,
        dataset_ids: &[DatasetId],
    ) -> Result<Option<ArchiveTimestampRange>, Error> {
        let HandleParts { tables, connection } = self.parts();
        let mut begin_timestamp_millisecs = None;
        let mut end_timestamp_millisecs = None;
        for batch in dataset_ids.chunks(MAX_BATCH_SIZE) {
            let mut query_builder = QueryBuilder::<MySql>::new(format!(
                "SELECT MIN(a.`begin_timestamp_millisecs`) AS `begin_timestamp_millisecs`, \
                 MAX(a.`end_timestamp_millisecs`) AS `end_timestamp_millisecs` FROM `{}` a JOIN \
                 `{}` d ON d.`id` = a.`dataset_id` WHERE d.`is_deleted` = FALSE AND \
                 a.`dataset_id` IN (",
                tables.archives(),
                tables.datasets()
            ));
            let mut separated_ids = query_builder.separated(", ");
            for dataset_id in batch {
                separated_ids.push_bind(dataset_id);
            }
            query_builder.push(")");
            let bounds = query_builder
                .build_query_as::<TimestampBoundsRow>()
                .fetch_one(&mut *connection)
                .await?;
            begin_timestamp_millisecs =
                [begin_timestamp_millisecs, bounds.begin_timestamp_millisecs]
                    .into_iter()
                    .flatten()
                    .min();
            end_timestamp_millisecs = [end_timestamp_millisecs, bounds.end_timestamp_millisecs]
                .into_iter()
                .flatten()
                .max();
        }
        // The bounds are both `NULL` or both set, since the columns are `NOT NULL`.
        Ok(begin_timestamp_millisecs
            .zip(end_timestamp_millisecs)
            .map(|(begin, end)| ArchiveTimestampRange::new(begin, end))
            .transpose()?)
    }

    async fn get_space_savings(&mut self) -> Result<SpaceSavings, Error> {
        let HandleParts { tables, connection } = self.parts();
        let query = format!(
            "SELECT CAST(COALESCE(SUM(a.`num_uncompressed_bytes`), 0) AS UNSIGNED) AS \
             `num_uncompressed_bytes`, CAST(COALESCE(SUM(a.`num_compressed_bytes`), 0) AS \
             UNSIGNED) AS `num_compressed_bytes` FROM `{}` a JOIN `{}` d ON d.`id` = \
             a.`dataset_id` WHERE d.`is_deleted` = FALSE",
            tables.archives(),
            tables.datasets()
        );
        let row = sqlx::query_as::<_, SpaceSavingsRow>(&query)
            .fetch_one(connection)
            .await?;
        Ok(SpaceSavings {
            num_uncompressed_bytes: row.num_uncompressed_bytes,
            num_compressed_bytes: row.num_compressed_bytes,
        })
    }

    async fn get_column_names_by_types(
        &mut self,
        dataset_id: DatasetId,
        types: &[ColumnType],
    ) -> Result<Vec<String>, Error> {
        // Deduplicated so that the `IN` list never exceeds the number of column types, which is
        // below `MAX_BATCH_SIZE`.
        let mut distinct_types: Vec<ColumnType> = Vec::new();
        for column_type in types {
            if !distinct_types.contains(column_type) {
                distinct_types.push(*column_type);
            }
        }
        if distinct_types.is_empty() {
            return Ok(Vec::new());
        }

        let HandleParts { tables, connection } = self.parts();
        let mut query_builder = QueryBuilder::<MySql>::new(format!(
            "SELECT DISTINCT c.`name` FROM `{}` c JOIN `{}` d ON d.`id` = c.`dataset_id` WHERE \
             d.`is_deleted` = FALSE AND c.`dataset_id` = ",
            tables.column_metadata(),
            tables.datasets()
        ));
        query_builder
            .push_bind(dataset_id)
            .push(" AND c.`type` IN (");
        let mut separated_types = query_builder.separated(", ");
        for column_type in distinct_types {
            separated_types.push_bind(column_type);
        }
        query_builder.push(") ORDER BY c.`name`");
        Ok(query_builder
            .build_query_as::<ColumnNameRow>()
            .fetch_all(connection)
            .await?
            .into_iter()
            .map(|row| row.name.value)
            .collect())
    }
}

/// The smallest begin timestamp and the largest end timestamp over a set of archives rows, both
/// `NULL` if the set is empty.
#[derive(sqlx::FromRow)]
struct TimestampBoundsRow {
    begin_timestamp_millisecs: Option<i64>,
    end_timestamp_millisecs: Option<i64>,
}

/// The sizes summed over a set of archives rows.
#[derive(sqlx::FromRow)]
struct SpaceSavingsRow {
    num_uncompressed_bytes: u64,
    num_compressed_bytes: u64,
}

/// The name of a column-metadata row.
#[derive(sqlx::FromRow)]
struct ColumnNameRow {
    #[sqlx(try_from = "Vec<u8>")]
    name: StoredString,
}
