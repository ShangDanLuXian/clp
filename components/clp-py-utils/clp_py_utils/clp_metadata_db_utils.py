from __future__ import annotations

from pathlib import Path

from clp_py_utils.clp_config import ArchiveOutput, StorageType

# Constants
MYSQL_TABLE_NAME_MAX_LEN = 64

ARCHIVES_TABLE_SUFFIX = "archives"
COLUMN_METADATA_TABLE_SUFFIX = "column_metadata"
DATASETS_TABLE_SUFFIX = "datasets"
FILTER_PACKS_TABLE_SUFFIX = "filter_packs"
FILES_TABLE_SUFFIX = "files"

TABLE_SUFFIX_MAX_LEN = max(
    len(ARCHIVES_TABLE_SUFFIX),
    len(COLUMN_METADATA_TABLE_SUFFIX),
    len(DATASETS_TABLE_SUFFIX),
    len(FILTER_PACKS_TABLE_SUFFIX),
    len(FILES_TABLE_SUFFIX),
)


def _create_archives_table(
    db_cursor, archives_table_name: str, filter_packs_table_name: str
) -> None:
    db_cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{archives_table_name}` (
            `pagination_id` BIGINT unsigned NOT NULL AUTO_INCREMENT,
            `id` VARCHAR(64) NOT NULL,
            `begin_timestamp` BIGINT NOT NULL,
            `end_timestamp` BIGINT NOT NULL,
            `uncompressed_size` BIGINT NOT NULL,
            `size` BIGINT NOT NULL,
            `creator_id` VARCHAR(64) NOT NULL,
            `creation_ix` INT NOT NULL,
            `filter_pack_id` BIGINT unsigned NULL,
            KEY `archives_creation_order` (`creator_id`,`creation_ix`) USING BTREE,
            KEY `archives_filter_pack_id` (`filter_pack_id`) USING BTREE,
            FOREIGN KEY (`filter_pack_id`) REFERENCES `{filter_packs_table_name}` (`id`),
            UNIQUE KEY `archive_id` (`id`) USING BTREE,
            PRIMARY KEY (`pagination_id`)
        )
        """
    )


def _create_files_table(db_cursor, table_prefix: str, dataset: str | None) -> None:
    db_cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{get_files_table_name(table_prefix, dataset)}` (
            `id` VARCHAR(64) NOT NULL,
            `orig_file_id` VARCHAR(64) NOT NULL,
            `path` VARCHAR(12288) NOT NULL,
            `begin_timestamp` BIGINT NOT NULL,
            `end_timestamp` BIGINT NOT NULL,
            `num_uncompressed_bytes` BIGINT NOT NULL,
            `begin_message_ix` BIGINT NOT NULL,
            `num_messages` BIGINT NOT NULL,
            `archive_id` VARCHAR(64) NOT NULL,
            KEY `files_path` (path(768)) USING BTREE,
            KEY `files_archive_id` (`archive_id`) USING BTREE,
            PRIMARY KEY (`id`)
        ) ROW_FORMAT=DYNAMIC
        """
    )


def _create_column_metadata_table(db_cursor, table_prefix: str, dataset: str) -> None:
    db_cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{get_column_metadata_table_name(table_prefix, dataset)}` (
            `name` VARCHAR(512) NOT NULL,
            `type` TINYINT NOT NULL,
            PRIMARY KEY (`name`, `type`)
        )
        """
    )


def _create_filter_packs_table(db_cursor, table_prefix: str, dataset: str | None) -> None:
    db_cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{get_filter_packs_table_name(table_prefix, dataset)}` (
            `id` BIGINT unsigned NOT NULL AUTO_INCREMENT,
            `storage_path` VARCHAR(4096) NOT NULL,
            `size` BIGINT NOT NULL,
            `num_filters` INT NOT NULL,
            PRIMARY KEY (`id`)
        )
        """
    )


def _get_table_name(prefix: str, suffix: str, dataset: str | None) -> str:
    """
    :param prefix:
    :param suffix:
    :param dataset:
    :return: The table name in the form of "<prefix>[<dataset>_]<suffix>".
    """
    table_name = prefix
    if dataset is not None:
        table_name += f"{dataset}_"
    table_name += suffix
    return table_name


def create_datasets_table(db_cursor, table_prefix: str) -> None:
    """
    Creates the datasets information table.

    :param db_cursor: The database cursor to execute the table creation.
    :param table_prefix: A string to prepend to the table name.
    """
    # For a description of the table, see
    # `../../../docs/src/dev-docs/design-metadata-db.md`
    db_cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{get_datasets_table_name(table_prefix)}` (
            `name` VARCHAR(255) NOT NULL,
            `archive_storage_directory` VARCHAR(4096) NOT NULL,
            PRIMARY KEY (`name`)
        )
        """
    )


def add_dataset(
    db_conn,
    db_cursor,
    table_prefix: str,
    dataset_name: str,
    archive_output: ArchiveOutput,
) -> None:
    """
    Inserts a new dataset into the `datasets` table and creates the corresponding standard set of
    tables for CLP's metadata.

    :param db_conn:
    :param db_cursor: The database cursor to execute the table row insertion.
    :param table_prefix: A string to prepend to the table name.
    :param dataset_name:
    :param archive_output:
    """
    archive_storage_directory: Path
    if StorageType.S3 == archive_output.storage.type:
        s3_config = archive_output.storage.s3_config
        archive_storage_directory = Path(s3_config.key_prefix)
    else:
        archive_storage_directory = archive_output.get_directory()

    query = f"""INSERT INTO `{get_datasets_table_name(table_prefix)}`
                (name, archive_storage_directory)
                VALUES (%s, %s)
                """
    db_cursor.execute(
        query,
        (dataset_name, str(archive_storage_directory / dataset_name)),
    )
    create_metadata_db_tables(db_cursor, table_prefix, dataset_name)
    db_conn.commit()


def fetch_existing_datasets(
    db_cursor,
    table_prefix: str,
) -> set[str]:
    """
    Gets the names of all existing datasets.

    :param db_cursor:
    :param table_prefix:
    """
    db_cursor.execute(f"SELECT name FROM `{get_datasets_table_name(table_prefix)}`")
    rows = db_cursor.fetchall()
    return {row["name"] for row in rows}


def create_metadata_db_tables(db_cursor, table_prefix: str, dataset: str | None = None) -> None:
    """
    Creates the standard set of tables for CLP's metadata.

    :param db_cursor: The database cursor to execute the table creations.
    :param table_prefix: A string to prepend to all table names.
    :param dataset: If set, all tables will be named in a dataset-specific manner.
    """
    if dataset is not None:
        _create_column_metadata_table(db_cursor, table_prefix, dataset)

    _create_filter_packs_table(db_cursor, table_prefix, dataset)

    archives_table_name = get_archives_table_name(table_prefix, dataset)
    filter_packs_table_name = get_filter_packs_table_name(table_prefix, dataset)
    _create_archives_table(db_cursor, archives_table_name, filter_packs_table_name)
    _create_files_table(db_cursor, table_prefix, dataset)


def delete_archives_from_metadata_db(
    db_cursor, archive_ids: list[str], table_prefix: str, dataset: str | None
) -> None:
    """
    Deletes archives from the metadata database specified by a list of IDs. It also deletes
    the associated entries from the `files` table that reference these archives.

    The order of deletion follows the foreign key constraints, ensuring no violations occur during
    the process.

    :param db_cursor:
    :param archive_ids: The list of archive to delete.
    :param table_prefix:
    :param dataset:
    """
    ids_list_string = ", ".join(["%s"] * len(archive_ids))

    db_cursor.execute(
        f"""
        DELETE FROM `{get_files_table_name(table_prefix, dataset)}`
        WHERE archive_id in ({ids_list_string})
        """,
        archive_ids,
    )

    db_cursor.execute(
        f"""
        DELETE FROM `{get_archives_table_name(table_prefix, dataset)}`
        WHERE id in ({ids_list_string})
        """,
        archive_ids,
    )


def delete_dataset_from_metadata_db(db_cursor, table_prefix: str, dataset: str) -> None:
    """
    Deletes all tables associated with `dataset` from the metadata database.

    :param db_cursor:
    :param table_prefix:
    :param dataset:
    """
    # Drop tables in an order such that no foreign key constraint is violated.
    tables_in_removal_order = [
        get_column_metadata_table_name(table_prefix, dataset),
        get_files_table_name(table_prefix, dataset),
        get_archives_table_name(table_prefix, dataset),
        get_filter_packs_table_name(table_prefix, dataset),
    ]

    for table in tables_in_removal_order:
        db_cursor.execute(f"DROP TABLE IF EXISTS `{table}`")

    # Remove the dataset row from the datasets table
    db_cursor.execute(
        f"""
        DELETE FROM `{get_datasets_table_name(table_prefix)}`
        WHERE name = %s
        """,
        (dataset,),
    )


def insert_filter_pack(
    db_cursor,
    table_prefix: str,
    dataset: str | None,
    storage_path: str,
    size: int,
    num_filters: int,
) -> int:
    """
    Inserts a filter pack and returns the auto-generated pack id.

    :param db_cursor:
    :param table_prefix:
    :param dataset:
    :param storage_path:
    :param size:
    :param num_filters:
    :return: pack id
    """
    table_name = get_filter_packs_table_name(table_prefix, dataset)
    db_cursor.execute(
        f"""
        INSERT INTO `{table_name}` (storage_path, size, num_filters)
        VALUES (%s, %s, %s)
        """,
        (storage_path, size, num_filters),
    )
    return db_cursor.lastrowid


def set_archives_filter_pack_id(
    db_cursor,
    table_prefix: str,
    dataset: str | None,
    archive_ids: list[str],
    pack_id: int,
    only_if_null: bool = True,
) -> None:
    """
    Updates the archives table to point to the given filter pack id.

    :param db_cursor:
    :param table_prefix:
    :param dataset:
    :param archive_ids:
    :param pack_id:
    :param only_if_null:
    """
    if len(archive_ids) == 0:
        return
    ids_list_string = ", ".join(["%s"] * len(archive_ids))
    archives_table = get_archives_table_name(table_prefix, dataset)
    null_clause = " AND filter_pack_id IS NULL" if only_if_null else ""
    db_cursor.execute(
        f"""
        UPDATE `{archives_table}`
        SET filter_pack_id = %s
        WHERE id IN ({ids_list_string}){null_clause}
        """,
        [pack_id, *archive_ids],
    )


def fetch_filter_pack_paths(
    db_cursor,
    table_prefix: str,
    dataset: str | None,
    pack_ids: list[int],
) -> dict[int, str]:
    if len(pack_ids) == 0:
        return {}
    table_name = get_filter_packs_table_name(table_prefix, dataset)
    placeholders = ", ".join(["%s"] * len(pack_ids))
    db_cursor.execute(
        f"""
        SELECT id, storage_path
        FROM `{table_name}`
        WHERE id IN ({placeholders})
        """,
        pack_ids,
    )
    rows = db_cursor.fetchall()
    return {int(row["id"]): row["storage_path"] for row in rows}


def get_archives_table_name(table_prefix: str, dataset: str | None) -> str:
    return _get_table_name(table_prefix, ARCHIVES_TABLE_SUFFIX, dataset)


def get_column_metadata_table_name(table_prefix: str, dataset: str | None) -> str:
    return _get_table_name(table_prefix, COLUMN_METADATA_TABLE_SUFFIX, dataset)


def get_datasets_table_name(table_prefix: str) -> str:
    return _get_table_name(table_prefix, DATASETS_TABLE_SUFFIX, None)


def get_files_table_name(table_prefix: str, dataset: str | None) -> str:
    return _get_table_name(table_prefix, FILES_TABLE_SUFFIX, dataset)


def get_filter_packs_table_name(table_prefix: str, dataset: str | None) -> str:
    return _get_table_name(table_prefix, FILTER_PACKS_TABLE_SUFFIX, dataset)
