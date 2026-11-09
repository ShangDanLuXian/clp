//! Creation of the shared metadata tables.

use crate::Error;
use crate::MetadataTables;

impl MetadataTables {
    /// Creates the shared metadata tables if they don't exist.
    ///
    /// This isn't a migration: an existing table with another shape is left as is.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::IncompatibleTable`] if the existing datasets table doesn't have the shape of the
    ///   shared datasets table.
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    /// * Forwards [`sqlx::query::Query::fetch_optional`]'s return values on failure.
    pub async fn create_tables(&self, conn: &mut sqlx::MySqlConnection) -> Result<(), Error> {
        const DATASETS_ID_COLUMN_QUERY: &str = r"
SELECT 1 FROM `information_schema`.`COLUMNS`
WHERE `TABLE_SCHEMA` = DATABASE() AND `TABLE_NAME` = ? AND `COLUMN_NAME` = 'id'";

        let datasets_table_creation_query = format!(
            r"
CREATE TABLE IF NOT EXISTS `{datasets}` (
    `id`                       SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `name`                     VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    `archive_storage_path`     VARCHAR(4096) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    `is_deleted`               BOOLEAN NOT NULL DEFAULT FALSE,
    `retention_period_minutes` INT UNSIGNED DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `dataset_name` (`name`) USING BTREE
)",
            datasets = self.datasets,
        );
        sqlx::query(&datasets_table_creation_query)
            .execute(&mut *conn)
            .await?;

        if sqlx::query(DATASETS_ID_COLUMN_QUERY)
            .bind(&self.datasets)
            .fetch_optional(&mut *conn)
            .await?
            .is_none()
        {
            return Err(Error::IncompatibleTable {
                table: self.datasets.clone(),
            });
        }

        let archives_table_creation_query = format!(
            r"
CREATE TABLE IF NOT EXISTS `{archives}` (
    `id`                           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `dataset_id`                   SMALLINT UNSIGNED NOT NULL,
    `uuid`                         BINARY(16) NOT NULL,
    `begin_timestamp_millisecs`    BIGINT NOT NULL,
    `end_timestamp_millisecs`      BIGINT NOT NULL,
    `num_uncompressed_bytes`       BIGINT UNSIGNED NOT NULL,
    `num_compressed_bytes`         BIGINT UNSIGNED NOT NULL,
    `creation_timestamp_millisecs` BIGINT NOT NULL,
    `compression_job_id`           INT NOT NULL,
    `pack_id`                      BIGINT UNSIGNED DEFAULT NULL,
    `is_deleted`                   BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (`dataset_id`, `begin_timestamp_millisecs`, `id`),
    KEY `auto_inc` (`id`),
    KEY `archive_uuid` (`dataset_id`, `uuid`),
    KEY `archives_gc_order` (`dataset_id`, `creation_timestamp_millisecs`)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8
PARTITION BY RANGE (`begin_timestamp_millisecs`) (
    PARTITION `p_future` VALUES LESS THAN MAXVALUE
)",
            archives = self.archives,
        );
        sqlx::query(&archives_table_creation_query)
            .execute(&mut *conn)
            .await?;

        let long_span_archives_table_creation_query = format!(
            r"
CREATE TABLE IF NOT EXISTS `{long_span_archives}` (
    `dataset_id`                SMALLINT UNSIGNED NOT NULL,
    `begin_timestamp_millisecs` BIGINT NOT NULL,
    `end_timestamp_millisecs`   BIGINT NOT NULL,
    `archive_id`                BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (`dataset_id`, `end_timestamp_millisecs`, `archive_id`),
    KEY `by_begin` (`dataset_id`, `begin_timestamp_millisecs`)
) ENGINE=InnoDB",
            long_span_archives = self.long_span_archives,
        );
        sqlx::query(&long_span_archives_table_creation_query)
            .execute(&mut *conn)
            .await?;

        let column_metadata_table_creation_query = format!(
            r"
CREATE TABLE IF NOT EXISTS `{column_metadata}` (
    `dataset_id` SMALLINT UNSIGNED NOT NULL,
    `name`       VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    `type`       TINYINT NOT NULL,
    PRIMARY KEY (`dataset_id`, `name`, `type`)
)",
            column_metadata = self.column_metadata,
        );
        sqlx::query(&column_metadata_table_creation_query)
            .execute(&mut *conn)
            .await?;

        Ok(())
    }
}
