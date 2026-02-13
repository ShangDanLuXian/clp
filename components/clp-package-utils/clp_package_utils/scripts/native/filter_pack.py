import argparse
import json
import logging
import pathlib
import subprocess
import sys
import tempfile
import uuid
from contextlib import closing

from clp_py_utils.clp_config import (
    CLP_DEFAULT_CONFIG_FILE_RELATIVE_PATH,
    CLP_DEFAULT_ARCHIVES_DIRECTORY_PATH,
    CLP_DEFAULT_DATASET_NAME,
    ClpConfig,
    StorageEngine,
    StorageType,
)
from clp_py_utils.clp_metadata_db_utils import (
    get_archives_table_name,
    insert_filter_pack,
    set_archives_filter_pack_id,
)
from clp_py_utils.s3_utils import s3_put
from clp_py_utils.sql_adapter import SqlAdapter

from clp_package_utils.general import get_clp_home, load_config_file
from clp_package_utils.scripts.native.utils import validate_dataset_exists

logger = logging.getLogger(__name__)

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


def _build_filter_pack_with_clp_filter(
    clp_home: pathlib.Path,
    output_path: pathlib.Path,
    group: list[tuple[str, pathlib.Path]],
) -> dict:
    clp_filter = clp_home / "bin" / "clp-filter"
    if not clp_filter.exists():
        raise FileNotFoundError(f"clp-filter not found at {clp_filter}")

    with tempfile.TemporaryDirectory(prefix="clp-filter-pack-") as tmp_dir:
        tmp_dir_path = pathlib.Path(tmp_dir)
        manifest_path = tmp_dir_path / "manifest.tsv"
        output_json_path = tmp_dir_path / "result.json"

        with manifest_path.open("w", encoding="utf-8") as manifest_file:
            for archive_id, filter_path in group:
                manifest_file.write(f"{archive_id}\t{filter_path}\n")

        cmd = [
            str(clp_filter),
            "pack",
            "--output",
            str(output_path),
            "--manifest",
            str(manifest_path),
            "--output-json",
            str(output_json_path),
        ]
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(
                f"clp-filter pack failed with code {proc.returncode}: {proc.stderr}"
            )

        with output_json_path.open("r", encoding="utf-8") as output_file:
            return json.load(output_file)


def _insert_pack_and_update_archives(
    db_conn,
    db_cursor,
    table_prefix: str,
    dataset: str,
    storage_path: str,
    pack_size: int,
    archive_ids: list[str],
):
    pack_id = insert_filter_pack(
        db_cursor,
        table_prefix,
        dataset,
        storage_path,
        pack_size,
        len(archive_ids),
    )
    set_archives_filter_pack_id(
        db_cursor,
        table_prefix,
        dataset,
        archive_ids,
        pack_id,
        only_if_null=True,
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


def _resolve_pack_storage_path_fs(
    archive_output_dir: pathlib.Path, pack_path: pathlib.Path
) -> str:
    """
    Store a container-visible path for filter packs so query-side containers can access them.
    """
    try:
        rel_path = pack_path.relative_to(archive_output_dir)
    except ValueError:
        logger.warning(
            "Filter pack path %s is not under archive output dir %s; storing host path.",
            pack_path,
            archive_output_dir,
        )
        return str(pack_path)

    container_archive_dir = pathlib.Path("/") / CLP_DEFAULT_ARCHIVES_DIRECTORY_PATH
    return str(container_archive_dir / rel_path)


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
        logger.info(
            "Filter packing skipped for dataset %s: storage engine %s is not clp-s.",
            dataset,
            clp_config.package.storage_engine,
        )
        return False

    validate_dataset_exists(clp_config.database, dataset)
    clp_home = get_clp_home()
    logger.info(
        "Starting filter packing for dataset %s (max_pack_size_bytes=%s, dry_run=%s).",
        dataset,
        max_pack_size_bytes,
        dry_run,
    )
    logger.info(
        "Filter staging dir: %s; archive output dir: %s; storage type: %s.",
        clp_config.filter_staging_directory,
        clp_config.archive_output.get_directory()
        if clp_config.archive_output.storage.type == StorageType.FS
        else "s3",
        clp_config.archive_output.storage.type,
    )

    staging_dir = pathlib.Path(clp_config.filter_staging_directory) / dataset
    if not staging_dir.exists():
        logger.info(
            "Filter staging directory does not exist for dataset %s: %s",
            dataset,
            staging_dir,
        )
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
            logger.info("No archives eligible for packing for dataset %s.", dataset)
            return True

        filters = _collect_filters(staging_dir, archive_ids)
        if not filters:
            logger.info(
                "No filter files found to pack for dataset %s (archives=%s, staging_dir=%s).",
                dataset,
                len(archive_ids),
                staging_dir,
            )
            return True

        groups = _split_by_size(filters, max_pack_size_bytes)
        logger.info(
            "Packing %s filters into %s packs for dataset %s.",
            len(filters),
            len(groups),
            dataset,
        )

        for group in groups:
            pack_id = uuid.uuid4().hex
            archive_ids_in_pack = [archive_id for archive_id, _ in group]
            total_bytes = sum(path.stat().st_size for _, path in group)
            logger.info(
                "Building filter pack %s for dataset %s (filters=%s, bytes=%s).",
                pack_id,
                dataset,
                len(group),
                total_bytes,
            )

            if dry_run:
                logger.info(
                    "Dry run: would pack %s filters into pack %s.",
                    len(archive_ids_in_pack),
                    pack_id,
                )
                continue

            if clp_config.archive_output.storage.type == StorageType.FS:
                archive_output_dir = clp_config.archive_output.get_directory()
                pack_path = _build_pack_path_fs(archive_output_dir, dataset, pack_id)
                pack_path.parent.mkdir(parents=True, exist_ok=True)
                result = _build_filter_pack_with_clp_filter(clp_home, pack_path, group)
                storage_path = _resolve_pack_storage_path_fs(archive_output_dir, pack_path)
            else:
                s3_config = clp_config.archive_output.storage.s3_config
                local_tmp_dir = pathlib.Path(clp_config.tmp_directory) / "filter-packs" / dataset
                local_tmp_dir.mkdir(parents=True, exist_ok=True)
                local_pack_path = local_tmp_dir / f"{pack_id}.clpf"
                result = _build_filter_pack_with_clp_filter(clp_home, local_pack_path, group)
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
                int(result["size"]),
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
