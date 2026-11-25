import asyncio
import pathlib
import posixpath
import time
import uuid
from contextlib import closing

from clp_py_utils.clp_config import (
    ArchiveOutput,
    ClpConfig,
    Database,
    QUERY_JOBS_TABLE_NAME,
    StorageEngine,
)
from clp_py_utils.clp_logging import configure_logging, get_logger
from clp_py_utils.clp_metadata_db_utils import (
    delete_archives_from_metadata_db,
    delete_archives_from_shared_metadata_tables,
    fetch_existing_datasets,
    get_archives_table_name,
    LiveDataset,
    SharedArchiveKey,
)
from clp_py_utils.sql_adapter import SqlAdapter

from job_orchestration.garbage_collector.constants import (
    ARCHIVE_GARBAGE_COLLECTOR_NAME,
    MIN_TO_SECONDS,
    SECOND_TO_MILLISECOND,
)
from job_orchestration.garbage_collector.utils import (
    DeletionCandidatesBuffer,
    execute_deletion,
    execute_deletion_by_storage_paths,
    validate_storage_path,
    validate_storage_type,
)
from job_orchestration.scheduler.constants import QueryJobStatus

logger = get_logger(ARCHIVE_GARBAGE_COLLECTOR_NAME)


def _delete_expired_archives(
    db_conn,
    db_cursor,
    table_prefix: str,
    archive_expiry_epoch_secs: int,
    candidates_buffer: DeletionCandidatesBuffer,
    archive_output_config: ArchiveOutput,
    dataset: str | None,
) -> None:
    archives_table = get_archives_table_name(table_prefix, dataset)
    archive_end_ts_upper_bound = archive_expiry_epoch_secs * SECOND_TO_MILLISECOND

    db_cursor.execute(
        f"""
        SELECT id FROM `{archives_table}`
        WHERE end_timestamp < %s
        AND end_timestamp != 0
        """,
        [archive_end_ts_upper_bound],
    )

    results = db_cursor.fetchall()
    archive_ids = [result["id"] for result in results]
    if len(archive_ids) != 0:
        delete_archives_from_metadata_db(db_cursor, archive_ids, table_prefix, dataset)

        for candidate in archive_ids:
            if dataset is not None:
                candidate = f"{dataset}/{candidate}"
            candidates_buffer.add_candidate(candidate)

        candidates_buffer.persist_new_candidates()
        db_conn.commit()

    candidates_to_delete = candidates_buffer.get_candidates()
    num_candidates_to_delete = len(candidates_to_delete)
    if 0 == num_candidates_to_delete:
        logger.debug(
            f"No archives matched the expiry criteria: `end_ts < {archive_end_ts_upper_bound}`."
        )
        return

    execute_deletion(archive_output_config, candidates_to_delete)

    # Prepare the log message
    dataset_msg: str
    deleted_candidates: list[str]
    if dataset is not None:
        dataset_log_msg = f" from dataset `{dataset}`"
        # Note: If dataset is not None, candidates are expected to be in the format
        # `<dataset>/<archive_id>`
        deleted_candidates = [candidate.split("/")[1] for candidate in candidates_to_delete]
    else:
        dataset_log_msg = ""
        deleted_candidates = list(candidates_to_delete)

    candidates_buffer.clear()
    logger.info(
        f"Deleted {num_candidates_to_delete} archive(s){dataset_log_msg}: {deleted_candidates}"
    )


def _delete_expired_clp_s_archives(
    db_conn,
    db_cursor,
    table_prefix: str,
    archive_expiry_epoch_secs: int,
    candidates_buffer: DeletionCandidatesBuffer,
    archive_output_config: ArchiveOutput,
    dataset: LiveDataset,
) -> None:
    """
    Deletes the archives of one clp-s dataset whose end timestamp is before the expiry, first from
    the shared metadata tables, then from storage under the dataset's archive storage path.

    The candidates are persisted to the recovery file, as full storage paths, before the metadata
    deletion commits, so that their storage can still be deleted after a failure.

    :param db_conn:
    :param db_cursor:
    :param table_prefix:
    :param archive_expiry_epoch_secs:
    :param candidates_buffer:
    :param archive_output_config:
    :param dataset:
    :raises: Propagates `validate_storage_path`'s exceptions.
    :raises: Propagates `delete_archives_from_shared_metadata_tables`'s exceptions.
    :raises: Propagates `execute_deletion_by_storage_paths`'s exceptions.
    """
    # Validate before deleting any metadata: a path that fails validation after the commit would
    # be persisted in the recovery file and fail every later sweep.
    validate_storage_path(archive_output_config, dataset.archive_storage_path)

    archive_end_ts_upper_bound = archive_expiry_epoch_secs * SECOND_TO_MILLISECOND

    # Archives without timestamps store 0 as their end timestamp and are never expired.
    db_cursor.execute(
        f"""
        SELECT id, uuid, begin_timestamp_millisecs, end_timestamp_millisecs
        FROM `{get_archives_table_name(table_prefix, None)}`
        WHERE dataset_id = %s
        AND end_timestamp_millisecs < %s
        AND end_timestamp_millisecs != 0
        """,
        [dataset.id, archive_end_ts_upper_bound],
    )

    results = db_cursor.fetchall()
    if len(results) != 0:
        # Storage keys use the UUID's hyphenated text form, not the stored `BINARY(16)`.
        storage_paths = [
            posixpath.join(dataset.archive_storage_path, str(uuid.UUID(bytes=result["uuid"])))
            for result in results
        ]
        archives = [
            SharedArchiveKey(
                id=result["id"],
                begin_timestamp_millisecs=result["begin_timestamp_millisecs"],
                end_timestamp_millisecs=result["end_timestamp_millisecs"],
            )
            for result in results
        ]
        delete_archives_from_shared_metadata_tables(db_cursor, table_prefix, dataset.id, archives)

        for storage_path in storage_paths:
            candidates_buffer.add_candidate(storage_path)

        candidates_buffer.persist_new_candidates()
        db_conn.commit()

    candidates_to_delete = candidates_buffer.get_candidates()
    num_candidates_to_delete = len(candidates_to_delete)
    if 0 == num_candidates_to_delete:
        logger.debug(
            f"No archives in dataset `{dataset.name}` matched the expiry criteria:"
            f" `end_ts < {archive_end_ts_upper_bound}`."
        )
        return

    execute_deletion_by_storage_paths(archive_output_config, candidates_to_delete)

    deleted_candidates = list(candidates_to_delete)
    candidates_buffer.clear()
    logger.info(
        f"Deleted {num_candidates_to_delete} archive(s) while collecting dataset `{dataset.name}`:"
        f" {deleted_candidates}"
    )


def _delete_recovered_clp_s_archives(
    candidates_buffer: DeletionCandidatesBuffer,
    archive_output_config: ArchiveOutput,
    recovery_file: pathlib.Path,
) -> None:
    """
    Deletes the storage of clp-s archives left in the recovery file by a previous failed sweep.
    These are full storage paths, independent of any dataset, so they're deleted even if no dataset
    is live.

    :param candidates_buffer:
    :param archive_output_config:
    :param recovery_file: The file `candidates_buffer` was read from.
    :raises ValueError: If the storage type is FS and a candidate isn't an absolute path, e.g., if
        the recovery file was written by a release before v0.15.0, which wrote paths relative to the
        configured archive directory.
    :raises: Propagates `execute_deletion_by_storage_paths`'s exceptions.
    """
    candidates_to_delete = candidates_buffer.get_candidates()
    num_candidates_to_delete = len(candidates_to_delete)
    if 0 == num_candidates_to_delete:
        return

    try:
        for candidate in candidates_to_delete:
            validate_storage_path(archive_output_config, candidate)
    except ValueError as e:
        raise ValueError(
            f"Recovery file `{recovery_file}` holds an invalid archive storage path. A recovery"
            " file written by a release before v0.15.0 holds relative paths and must be removed"
            " after deleting the archives it lists."
        ) from e

    execute_deletion_by_storage_paths(archive_output_config, candidates_to_delete)

    deleted_candidates = list(candidates_to_delete)
    candidates_buffer.clear()
    logger.info(
        f"Deleted {num_candidates_to_delete} archive(s) left by a previous sweep:"
        f" {deleted_candidates}"
    )


def _get_archive_safe_expiry_epoch(
    db_cursor,
    retention_period_minutes: int,
) -> int:
    """
    Calculates a safe expiration timestamp such that archives with `end_ts` less than this value are
    guaranteed not to be searched by any running query jobs.

    If no query jobs are running, the expiry time is set to `current_time - retention_period`.
    If a query job is running and was created at `creation_time`, the query scheduler guarantees
    that it will not search any archive whose end_ts < (creation_time - retention_period).
    In this case, the expiry time can be safely adjusted to `creation_time - retention_period`.

    Note: This function does not consider query jobs that started before
    `current_time - retention_period`, as such long-running jobs are likely hanging. Including them
    would prevent the expiry time from advancing.

    :param db_cursor: Database cursor object
    :param retention_period_minutes: Retention window in minutes
    :return: Epoch timestamp indicating the safe expiration time (in seconds)
    """
    retention_period_secs = retention_period_minutes * MIN_TO_SECONDS
    current_epoch_secs = time.time()
    archive_expiry_epoch: int

    db_cursor.execute(
        f"""
        SELECT id, creation_time
        FROM `{QUERY_JOBS_TABLE_NAME}`
        WHERE {QUERY_JOBS_TABLE_NAME}.status = {QueryJobStatus.RUNNING}
        AND {QUERY_JOBS_TABLE_NAME}.creation_time < FROM_UNIXTIME(%s)
        ORDER BY creation_time ASC
        LIMIT 1
        """,
        [current_epoch_secs],
    )

    row = db_cursor.fetchone()
    if row is not None:
        job_creation_time = row.get("creation_time")
        archive_expiry_epoch = int(job_creation_time.timestamp()) - retention_period_secs
        logger.debug(f"Discovered running query job created at {job_creation_time}.")
        logger.debug(f"Using adjusted archive_expiry_epoch=`{archive_expiry_epoch}`.")
    else:
        archive_expiry_epoch = int(current_epoch_secs) - retention_period_secs
        logger.debug(f"Using archive_expiry_epoch=`{archive_expiry_epoch}`.")

    return archive_expiry_epoch


def _collect_and_sweep_expired_archives(
    archive_output_config: ArchiveOutput,
    storage_engine: str,
    database_config: Database,
    recovery_file: pathlib.Path,
) -> None:
    candidates_buffer = DeletionCandidatesBuffer(recovery_file)

    clp_connection_param = database_config.get_clp_connection_params_and_type()
    table_prefix = clp_connection_param["table_prefix"]
    sql_adapter = SqlAdapter(database_config)
    with (
        closing(sql_adapter.create_connection(True)) as db_conn,
        closing(db_conn.cursor(dictionary=True)) as db_cursor,
    ):
        archive_expiry_epoch = _get_archive_safe_expiry_epoch(
            db_cursor,
            archive_output_config.retention_period,
        )
        if StorageEngine.CLP_S == storage_engine:
            datasets = fetch_existing_datasets(db_cursor, table_prefix)
            for dataset in datasets:
                logger.debug(f"Running garbage collection on dataset `{dataset}`")
                _delete_expired_archives(
                    db_conn,
                    db_cursor,
                    table_prefix,
                    archive_expiry_epoch,
                    candidates_buffer,
                    archive_output_config,
                    dataset,
                )
        elif StorageEngine.CLP == storage_engine:
            _delete_expired_archives(
                db_conn,
                db_cursor,
                table_prefix,
                archive_expiry_epoch,
                candidates_buffer,
                archive_output_config,
                None,
            )
        else:
            raise ValueError(f"Unsupported Storage engine: {storage_engine}.")


async def archive_garbage_collector(clp_config: ClpConfig) -> None:
    configure_logging(logger, ARCHIVE_GARBAGE_COLLECTOR_NAME)

    archive_output_config = clp_config.archive_output
    storage_engine = clp_config.package.storage_engine
    validate_storage_type(archive_output_config, storage_engine)

    sweep_interval_secs = clp_config.garbage_collector.sweep_interval.archive * MIN_TO_SECONDS
    recovery_file = clp_config.tmp_directory / f"{ARCHIVE_GARBAGE_COLLECTOR_NAME}.tmp"

    logger.info(f"{ARCHIVE_GARBAGE_COLLECTOR_NAME} started.")
    try:
        while True:
            _collect_and_sweep_expired_archives(
                archive_output_config, storage_engine, clp_config.database, recovery_file
            )
            await asyncio.sleep(sweep_interval_secs)
    except Exception:
        logger.exception(f"{ARCHIVE_GARBAGE_COLLECTOR_NAME} exited with failure.")
        raise
