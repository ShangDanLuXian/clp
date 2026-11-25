//! The `CREATE TABLE` statements of the shared metadata tables.

use clp_rust_utils::clp_config::package::config::MetadataTables;

/// # Returns
///
/// The statements that create the shared metadata tables that don't exist yet, in dependency order.
pub fn create_table_statements(tables: &MetadataTables) -> [String; 4] {
    let datasets = tables.datasets();
    let archives = tables.archives();
    let long_span_archives = tables.long_span_archives();
    let column_metadata = tables.column_metadata();
    [
        format!(
            "CREATE TABLE IF NOT EXISTS `{datasets}` (
                `id` SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
                `name` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                `archive_storage_path` VARCHAR(4096) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT \
             NULL,
                `is_deleted` BOOLEAN NOT NULL DEFAULT FALSE,
                `retention_period_minutes` INT UNSIGNED DEFAULT NULL,
                PRIMARY KEY (`id`),
                UNIQUE KEY `dataset_name` (`name`) USING BTREE
            )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS `{archives}` (
                `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                `dataset_id` SMALLINT UNSIGNED NOT NULL,
                `uuid` BINARY(16) NOT NULL,
                `begin_timestamp_millisecs` BIGINT NOT NULL,
                `end_timestamp_millisecs` BIGINT NOT NULL,
                `num_uncompressed_bytes` BIGINT UNSIGNED NOT NULL,
                `num_compressed_bytes` BIGINT UNSIGNED NOT NULL,
                `creation_timestamp_millisecs` BIGINT NOT NULL,
                `compression_job_id` INT NOT NULL,
                `pack_id` BIGINT UNSIGNED DEFAULT NULL,
                PRIMARY KEY (`dataset_id`, `begin_timestamp_millisecs`, `id`),
                KEY `auto_inc` (`id`),
                KEY `archive_uuid` (`dataset_id`, `uuid`),
                KEY `archives_gc_order` (`dataset_id`, `creation_timestamp_millisecs`)
            ) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8
            PARTITION BY RANGE (`begin_timestamp_millisecs`) (
                PARTITION `p_future` VALUES LESS THAN MAXVALUE
            )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS `{long_span_archives}` (
                `dataset_id` SMALLINT UNSIGNED NOT NULL,
                `begin_timestamp_millisecs` BIGINT NOT NULL,
                `end_timestamp_millisecs` BIGINT NOT NULL,
                `archive_id` BIGINT UNSIGNED NOT NULL,
                PRIMARY KEY (`dataset_id`, `end_timestamp_millisecs`, `archive_id`),
                KEY `by_begin` (`dataset_id`, `begin_timestamp_millisecs`)
            ) ENGINE=InnoDB"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS `{column_metadata}` (
                `dataset_id` SMALLINT UNSIGNED NOT NULL,
                `name` VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                `type` TINYINT NOT NULL,
                PRIMARY KEY (`dataset_id`, `name`, `type`)
            )"
        ),
    ]
}
