//! Aggregate reads over the archives table for the `WebUI` metadata routes.

use clp_rust_utils::types::ArchiveId;

use crate::DatasetId;
use crate::Error;
use crate::MetadataTables;
use crate::SpaceSavings;
use crate::TimestampBounds;
use crate::types::BinaryArchiveId;

impl MetadataTables {
    /// Computes the timestamp bounds over the live archives of the live datasets among `ids`.
    ///
    /// # Returns
    ///
    /// The timestamp bounds on success, or `None` if there are no such archives.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryAs::fetch_one`]'s return values on failure.
    pub async fn get_timestamp_bounds(
        &self,
        conn: &mut sqlx::MySqlConnection,
        ids: &[DatasetId],
    ) -> Result<Option<TimestampBounds>, Error> {
        if ids.is_empty() {
            return Ok(None);
        }

        let mut query_builder = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
            r"
SELECT MIN(`a`.`begin_timestamp_millisecs`), MAX(`a`.`end_timestamp_millisecs`)
FROM `{archives}` AS `a`
JOIN `{datasets}` AS `d` ON `d`.`id` = `a`.`dataset_id`
WHERE `a`.`is_deleted` = FALSE AND `d`.`is_deleted` = FALSE AND `a`.`dataset_id` IN (",
            archives = self.archives,
            datasets = self.datasets,
        ));
        let mut separated_ids = query_builder.separated(", ");
        for id in ids {
            separated_ids.push_bind(*id);
        }
        query_builder.push(")");

        let (Some(begin_timestamp_millisecs), Some(end_timestamp_millisecs)) = query_builder
            .build_query_as::<(Option<i64>, Option<i64>)>()
            .fetch_one(&mut *conn)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(TimestampBounds {
            begin_timestamp_millisecs,
            end_timestamp_millisecs,
        }))
    }

    /// Computes the total sizes of the live archives of every live dataset.
    ///
    /// # Returns
    ///
    /// The total uncompressed and compressed sizes on success, both `0` if there are no such
    /// archives.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryAs::fetch_one`]'s return values on failure.
    pub async fn get_space_savings(
        &self,
        conn: &mut sqlx::MySqlConnection,
    ) -> Result<SpaceSavings, Error> {
        let query = format!(
            r"
SELECT
    CAST(COALESCE(SUM(`a`.`num_uncompressed_bytes`), 0) AS UNSIGNED),
    CAST(COALESCE(SUM(`a`.`num_compressed_bytes`), 0) AS UNSIGNED)
FROM `{archives}` AS `a`
JOIN `{datasets}` AS `d` ON `d`.`id` = `a`.`dataset_id`
WHERE `a`.`is_deleted` = FALSE AND `d`.`is_deleted` = FALSE",
            archives = self.archives,
            datasets = self.datasets,
        );
        let (num_uncompressed_bytes, num_compressed_bytes): (u64, u64) =
            sqlx::query_as(&query).fetch_one(&mut *conn).await?;
        Ok(SpaceSavings {
            num_uncompressed_bytes,
            num_compressed_bytes,
        })
    }

    /// Computes the total uncompressed size of the live archives among `uuids` that belong to the
    /// live datasets among `dataset_ids`.
    ///
    /// # Returns
    ///
    /// The total number of uncompressed bytes on success, or `0` if no archive matches.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryScalar::fetch_one`]'s return values on failure.
    pub async fn get_uncompressed_bytes_by_uuids(
        &self,
        conn: &mut sqlx::MySqlConnection,
        dataset_ids: &[DatasetId],
        uuids: &[ArchiveId],
    ) -> Result<u64, Error> {
        const MAX_NUM_UUIDS_PER_QUERY: usize = 1000;

        if dataset_ids.is_empty() {
            return Ok(0);
        }

        let mut num_uncompressed_bytes = 0;
        for chunk in uuids.chunks(MAX_NUM_UUIDS_PER_QUERY) {
            let mut query_builder = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
                r"
SELECT CAST(COALESCE(SUM(`a`.`num_uncompressed_bytes`), 0) AS UNSIGNED)
FROM `{archives}` AS `a`
JOIN `{datasets}` AS `d` ON `d`.`id` = `a`.`dataset_id`
WHERE `a`.`is_deleted` = FALSE AND `d`.`is_deleted` = FALSE AND `a`.`dataset_id` IN (",
                archives = self.archives,
                datasets = self.datasets,
            ));
            let mut separated_ids = query_builder.separated(", ");
            for id in dataset_ids {
                separated_ids.push_bind(*id);
            }
            query_builder.push(") AND `a`.`uuid` IN (");
            let mut separated_uuids = query_builder.separated(", ");
            for uuid in chunk {
                separated_uuids.push_bind(BinaryArchiveId(*uuid));
            }
            query_builder.push(")");
            num_uncompressed_bytes += query_builder
                .build_query_scalar::<u64>()
                .fetch_one(&mut *conn)
                .await?;
        }
        Ok(num_uncompressed_bytes)
    }
}
