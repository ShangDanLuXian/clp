import argparse
import logging
import pathlib
import sys
import uuid
from contextlib import closing

from clp_py_utils.clp_config import (
    CLP_DEFAULT_CONFIG_FILE_RELATIVE_PATH,
    CLP_DEFAULT_DATASET_NAME,
    ClpConfig,
    StorageEngine,
    StorageType,
)
from clp_py_utils.clp_metadata_db_utils import (
    get_archives_table_name,
    get_filter_packs_table_name,
)
from clp_py_utils.filter_pack import build_filter_pack
from clp_py_utils.s3_utils import s3_put
from clp_py_utils.sql_adapter import SqlAdapter

from clp_package_utils.general import get_clp_home, load_config_file
from clp_package_utils.scripts.native.utils import validate_dataset_exists

logger = logging.getLogger(__file__)

FILTER_SUFFIX = ".var.dict.filter"


def _iter_archives_to_pack(db_cursor, table_prefix: str, dataset: str):
    archives_table = get_archives_table_name(table_prefix, dataset)
    query = f"""
        SELECT id
        FROM `{archives_table}`
        WHERE filter_pack_id IS NULL
        ORDER BY begin_timestamp ASC, end_timestamp ASC, pagination_id ASC
    """
    db_cursor.execute(query)
    for row in db_cursor.fetchall():
        yield row["id"]


def _collect_filters(
    staging_dir: pathlib.Path, archive_ids: list[str]
) -> list[tuple[str, pathlib.Path]]:
    filters: list[tuple[str, pathlib.Path]] = []
    for archive_id in archive_ids:
        filter_path = staging_dir / f"{archive_id}{FILTER_SUFFIX}"
        if filter_path.exists():
            filters.append((archive_id, filter_path))
        else:
            logger.debug("Missing filter for archive %s", archive_id)
    return filters


def _split_by_size(
    filters: list[tuple[str, pathlib.Path]], max_pack_size: int
) -> list[list[tuple[str, pathlib.Path]]]:
    groups: list[list[tuple[str, pathlib.Path]]] = []
    current: list[tuple[str, pathlib.Path]] = []
    current_size = 0
    for archive_id, path in filters:
        size = path.stat().st_size
        if current and current_size + size > max_pack_size:
            groups.append(current)
            current = []
            current_size = 0
        current.append((archive_id, path))
        current_size += size
    if current:
        groups.append(current)
    return groups


def _insert_pack_and_update_archives(
    db_conn,
    db_cursor,
    table_prefix: str,
    dataset: str,
    storage_path: str,
    pack_size: int,
    archive_ids: list[str],
):
    filter_packs_table = get_filter_packs_table_name(table_prefix, dataset)
    archives_table = get_archives_table_name(table_prefix, dataset)

    db_cursor.execute(
        f"""
        INSERT INTO `{filter_packs_table}` (storage_path, size, num_filters)
        VALUES (%s, %s, %s)
        """,
        (storage_path, pack_size, len(archive_ids)),
    )
    pack_id = db_cursor.lastrowid

    placeholders = ", ".join(["%s"] * len(archive_ids))
    db_cursor.execute(
        f"""
        UPDATE `{archives_table}`
        SET filter_pack_id = %s
        WHERE id IN ({placeholders}) AND filter_pack_id IS NULL
        """,
        [pack_id, *archive_ids],
    )
    if db_cursor.rowcount != len(archive_ids):
        logger.warning(
            "Updated %s/%s archives for pack %s (some may already be packed).",
            db_cursor.rowcount,
            len(archive_ids),
            pack_id,
        )

    db_conn.commit()


def _build_pack_path_fs(
    archive_output_dir: pathlib.Path, dataset: str, pack_id: str
) -> pathlib.Path:
    return archive_output_dir / "_filter_packs" / dataset / f"{pack_id}.clpf"


def _build_pack_paths_s3(
    key_prefix: str, dataset: str, pack_id: str
) -> tuple[str, str]:
    rel_path = f"_filter_packs/{dataset}/{pack_id}.clpf"
    full_key = str(pathlib.PurePosixPath(key_prefix) / rel_path)
    return rel_path, full_key


def pack_filters_for_dataset(
    clp_config: ClpConfig,
    dataset: str,
    max_pack_size_bytes: int,
    dry_run: bool = False,
) -> bool:
    if clp_config.package.storage_engine != StorageEngine.CLP_S:
        logger.info("Filter packing is only supported for clp-s.")
        return False

    validate_dataset_exists(clp_config.database, dataset)

    staging_dir = pathlib.Path(clp_config.filter_staging_directory) / dataset
    if not staging_dir.exists():
        logger.info("Filter staging directory does not exist: %s", staging_dir)
        return True

    sql_adapter = SqlAdapter(clp_config.database)
    clp_db_connection_params = clp_config.database.get_clp_connection_params_and_type(True)
    table_prefix = clp_db_connection_params["table_prefix"]

    with (
        closing(sql_adapter.create_connection(True)) as db_conn,
        closing(db_conn.cursor(dictionary=True)) as db_cursor,
    ):
        archive_ids = list(_iter_archives_to_pack(db_cursor, table_prefix, dataset))
        if not archive_ids:
            logger.info("No archives eligible for packing.")
            return True

        filters = _collect_filters(staging_dir, archive_ids)
        if not filters:
            logger.info("No filter files found to pack.")
            return True

        groups = _split_by_size(filters, max_pack_size_bytes)
        logger.info("Packing %s filters into %s packs.", len(filters), len(groups))

        for group in groups:
            pack_id = uuid.uuid4().hex
            archive_ids_in_pack = [archive_id for archive_id, _ in group]

            if dry_run:
                logger.info(
                    "Dry run: would pack %s filters into pack %s.",
                    len(archive_ids_in_pack),
                    pack_id,
                )
                continue

            if clp_config.archive_output.storage.type == StorageType.FS:
                pack_path = _build_pack_path_fs(
                    clp_config.archive_output.get_directory(), dataset, pack_id
                )
                pack_path.parent.mkdir(parents=True, exist_ok=True)
                result = build_filter_pack(pack_path, group)
                storage_path = str(pack_path)
            else:
                s3_config = clp_config.archive_output.storage.s3_config
                local_tmp_dir = pathlib.Path(clp_config.tmp_directory) / "filter-packs" / dataset
                local_tmp_dir.mkdir(parents=True, exist_ok=True)
                local_pack_path = local_tmp_dir / f"{pack_id}.clpf"
                result = build_filter_pack(local_pack_path, group)
                rel_path, storage_path = _build_pack_paths_s3(
                    s3_config.key_prefix, dataset, pack_id
                )
                s3_put(s3_config, local_pack_path, rel_path)
                local_pack_path.unlink()

            _insert_pack_and_update_archives(
                db_conn,
                db_cursor,
                table_prefix,
                dataset,
                storage_path,
                result.size,
                archive_ids_in_pack,
            )

    return True


def main(argv):
    clp_home = get_clp_home()
    default_config_file_path = clp_home / CLP_DEFAULT_CONFIG_FILE_RELATIVE_PATH

    args_parser = argparse.ArgumentParser(description="Packs per-archive filter files.")
    args_parser.add_argument(
        "--config",
        "-c",
        default=str(default_config_file_path),
        help="CLP package configuration file.",
    )
    args_parser.add_argument(
        "--dataset",
        type=str,
        default=CLP_DEFAULT_DATASET_NAME,
        help="Dataset whose filters should be packed.",
    )
    args_parser.add_argument(
        "--max-pack-size-bytes",
        type=int,
        default=64 * 1024 * 1024,
        help="Maximum pack size in bytes.",
    )
    args_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    args_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be packed without writing packs or updating the DB.",
    )

    parsed_args = args_parser.parse_args(argv[1:])
    logger.setLevel(logging.DEBUG if parsed_args.verbose else logging.INFO)

    try:
        config_file_path = pathlib.Path(parsed_args.config)
        clp_config = load_config_file(config_file_path)
        clp_config.validate_logs_dir()
        clp_config.database.load_credentials_from_env()
    except Exception:
        logger.exception("Failed to load config.")
        return -1

    try:
        pack_filters_for_dataset(
            clp_config,
            parsed_args.dataset,
            parsed_args.max_pack_size_bytes,
            parsed_args.dry_run,
        )
    except Exception:
        logger.exception("Filter packing failed.")
        return -1

    return 0


if "__main__" == __name__:
    sys.exit(main(sys.argv))
