from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from clp_py_utils.clp_config import ArchiveOutput, StorageType

# Constants
MYSQL_TABLE_NAME_MAX_LEN = 64

ARCHIVES_TABLE_SUFFIX = "archives"
COLUMN_METADATA_TABLE_SUFFIX = "column_metadata"
DATASETS_TABLE_SUFFIX = "datasets"
FILES_TABLE_SUFFIX = "files"
# Left out of `TABLE_SUFFIX_MAX_LEN`: it names only a shared table, never a per-dataset one.
LONG_SPAN_ARCHIVES_TABLE_SUFFIX = "long_span_archives"

TABLE_SUFFIX_MAX_LEN = max(
    len(ARCHIVES_TABLE_SUFFIX),
    len(COLUMN_METADATA_TABLE_SUFFIX),
    len(DATASETS_TABLE_SUFFIX),
    len(FILES_TABLE_SUFFIX),
)


# The span above which a clp-s archive also has a row in the long-span archives table (24 hours).
# Mirror of `MAX_SPAN_MILLISECS` in `clp-rust-libs/clp-metadata-db/src/operations/archive.rs`; the
# two must be kept in sync.
_MAX_SPAN_MILLISECS = 24 * 60 * 60 * 1000


@dataclass(frozen=True)
class SharedArchiveKey:
    """
    The values, besides its dataset ID, that make up a clp-s archive's primary keys in the shared
    archives table and the long-span archives table.
    """

    id: int
    begin_timestamp_millisecs: int
    end_timestamp_millisecs: int


@dataclass(frozen=True)
class LiveDataset:
    """A clp-s dataset that isn't marked as deleted, as stored in the shared datasets table."""

    id: int
    name: str
    archive_storage_path: str


def _create_archives_table(db_cursor, archives_table_name: str) -> None:
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
            KEY `archives_creation_order` (`creator_id`,`creation_ix`) USING BTREE,
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


def _delete_by_primary_keys(
    db_cursor, table_name: str, key_columns: tuple[str, ...], keys: list[tuple[int, ...]]
) -> None:
    """
    Deletes the rows with the given full primary keys from a table. The function performs no action
    if `keys` is empty.

    :param db_cursor:
    :param table_name:
    :param key_columns: The table's primary key columns, in the order of each key's values.
    :param keys:
    """
    query = f"""
        DELETE `{table_name}` FROM `{table_name}` FORCE INDEX (PRIMARY)
        WHERE {" AND ".join(f"`{column}` = %s" for column in key_columns)}
        """
    for key in keys:
        db_cursor.execute(query, key)


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


def fetch_live_datasets(db_cursor, table_prefix: str) -> list[LiveDataset]:
    """
    Gets every clp-s dataset that isn't marked as deleted from the shared datasets table.

    :param db_cursor: A cursor created with `dictionary=True`.
    :param table_prefix:
    :return: The live datasets, ordered by ID.
    """
    db_cursor.execute(
        f"""
        SELECT id, name, archive_storage_path
        FROM `{get_datasets_table_name(table_prefix)}`
        WHERE is_deleted = FALSE
        ORDER BY id
        """
    )
    return [
        LiveDataset(
            id=row["id"],
            name=row["name"],
            archive_storage_path=row["archive_storage_path"],
        )
        for row in db_cursor.fetchall()
    ]


def delete_archives_from_shared_metadata_tables(
    db_cursor, table_prefix: str, dataset_id: int, archives: list[SharedArchiveKey]
) -> None:
    """
    Deletes clp-s archives of one dataset from the shared metadata tables, together with their
    long-span references. The caller owns the transaction, so both deletions commit together.

    Like the commit of new archives, it deletes the archives' rows first, then their long-span rows.
    Each row is deleted by its own statement, by its full primary key, so the statements lock only
    the deleted rows, not the gaps that a concurrent commit into the dataset inserts into.

    :param db_cursor:
    :param table_prefix:
    :param dataset_id:
    :param archives: The archives' primary key values, as read from the archives table.
    """
    _delete_by_primary_keys(
        db_cursor,
        get_archives_table_name(table_prefix, None),
        ("dataset_id", "begin_timestamp_millisecs", "id"),
        [(dataset_id, archive.begin_timestamp_millisecs, archive.id) for archive in archives],
    )
    # Only archives spanning more than `_MAX_SPAN_MILLISECS` have a long-span row. Deleting a key
    # that doesn't exist would lock the gap around it.
    _delete_by_primary_keys(
        db_cursor,
        get_long_span_archives_table_name(table_prefix),
        ("dataset_id", "end_timestamp_millisecs", "archive_id"),
        [
            (dataset_id, archive.end_timestamp_millisecs, archive.id)
            for archive in archives
            if archive.end_timestamp_millisecs - archive.begin_timestamp_millisecs
            > _MAX_SPAN_MILLISECS
        ],
    )


def create_metadata_db_tables(db_cursor, table_prefix: str, dataset: str | None = None) -> None:
    """
    Creates the standard set of tables for CLP's metadata.

    :param db_cursor: The database cursor to execute the table creations.
    :param table_prefix: A string to prepend to all table names.
    :param dataset: If set, all tables will be named in a dataset-specific manner.
    """
    if dataset is not None:
        _create_column_metadata_table(db_cursor, table_prefix, dataset)

    archives_table_name = get_archives_table_name(table_prefix, dataset)

    _create_archives_table(db_cursor, archives_table_name)
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


def get_archives_table_name(table_prefix: str, dataset: str | None) -> str:
    return _get_table_name(table_prefix, ARCHIVES_TABLE_SUFFIX, dataset)


def get_column_metadata_table_name(table_prefix: str, dataset: str | None) -> str:
    return _get_table_name(table_prefix, COLUMN_METADATA_TABLE_SUFFIX, dataset)


def get_datasets_table_name(table_prefix: str) -> str:
    return _get_table_name(table_prefix, DATASETS_TABLE_SUFFIX, None)


def get_files_table_name(table_prefix: str, dataset: str | None) -> str:
    return _get_table_name(table_prefix, FILES_TABLE_SUFFIX, dataset)


def get_long_span_archives_table_name(table_prefix: str) -> str:
    return _get_table_name(table_prefix, LONG_SPAN_ARCHIVES_TABLE_SUFFIX, None)
