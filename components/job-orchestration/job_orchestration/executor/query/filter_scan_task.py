import os
from pathlib import Path
from celery.app.task import Task
from celery.utils.log import get_task_logger
from clp_py_utils.clp_config import StorageType, WorkerConfig
from clp_py_utils.clp_logging import set_logging_level
from clp_py_utils.filter_file import filter_might_contain, read_filter_file_bytes
from clp_py_utils.filter_pack import (
    read_filter_pack_footer_from_bytes,
    read_filter_pack_index_from_bytes,
)

from job_orchestration.executor.query.celery import app
from job_orchestration.executor.utils import load_worker_config

logger = get_task_logger(__name__)


def _load_pack_bytes(storage_path: str, worker_config: WorkerConfig) -> bytes | None:
    storage = worker_config.archive_output.storage
    if StorageType.FS != storage.type:
        logger.warning("Filter scan currently supports only FS storage, skipping pack %s.", storage_path)
        return None

    pack_path = Path(storage_path)
    if not pack_path.is_absolute():
        pack_path = worker_config.archive_output.get_directory() / pack_path
    try:
        return pack_path.read_bytes()
    except OSError:
        logger.exception("Failed to read filter pack from %s", pack_path)
        return None


@app.task(bind=True)
def filter_scan(
    self: Task,
    job_id: str,
    pack_id: int,
    pack_storage_path: str,
    archive_ids: list[str],
    filter_terms: list[str],
    ignore_case: bool,
) -> list[str]:
    task_name = "filter_scan"

    clp_logging_level = os.getenv("CLP_LOGGING_LEVEL")
    set_logging_level(logger, clp_logging_level)

    logger.info(
        "Started %s task for job %s pack %s (%s archives)",
        task_name,
        job_id,
        pack_id,
        len(archive_ids),
    )

    if not filter_terms:
        return archive_ids

    clp_config_path = Path(os.getenv("CLP_CONFIG_PATH"))
    worker_config = load_worker_config(clp_config_path, logger)
    if worker_config is None:
        return archive_ids

    pack_bytes = _load_pack_bytes(pack_storage_path, worker_config)
    if pack_bytes is None:
        return archive_ids

    try:
        footer = read_filter_pack_footer_from_bytes(pack_bytes)
        entries = read_filter_pack_index_from_bytes(pack_bytes, footer)
    except Exception:
        logger.exception("Failed to parse filter pack %s", pack_storage_path)
        return archive_ids

    entry_map = {entry.archive_id: entry for entry in entries}
    matched: list[str] = []

    for archive_id in archive_ids:
        entry = entry_map.get(archive_id)
        if entry is None:
            matched.append(archive_id)
            continue

        start = footer.body_offset + entry.offset
        end = start + entry.size
        if start < 0 or end > len(pack_bytes):
            logger.debug(
                "Filter entry for archive %s is out of range (offset=%s size=%s)",
                archive_id,
                entry.offset,
                entry.size,
            )
            matched.append(archive_id)
            continue

        filter_bytes = pack_bytes[start:end]
        try:
            filter_data = read_filter_file_bytes(filter_bytes)
        except Exception:
            logger.debug("Failed to parse filter for archive %s", archive_id, exc_info=True)
            matched.append(archive_id)
            continue

        if filter_might_contain(filter_data, filter_terms, ignore_case):
            matched.append(archive_id)

    return matched
