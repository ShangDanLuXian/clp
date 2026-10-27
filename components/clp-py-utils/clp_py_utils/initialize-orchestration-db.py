#!/usr/bin/env python3
import argparse
import logging
import pathlib
import sys
from contextlib import closing

from job_orchestration.scheduler.constants import (
    CompressionJobStatus,
    CompressionTaskStatus,
    QueryJobStatus,
    QueryTaskStatus,
)
from mysql.connector.errorcode import ER_DUP_KEYNAME
from pydantic import ValidationError

from clp_py_utils.clp_config import (
    ClpConfig,
    COMPRESSION_JOBS_TABLE_NAME,
    COMPRESSION_TASKS_TABLE_NAME,
    QUERY_JOBS_TABLE_NAME,
    QUERY_TASKS_TABLE_NAME,
)
from clp_py_utils.core import read_yaml_config_file
from clp_py_utils.sql_adapter import SqlAdapter

# Setup logging
# Create logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
# Setup console logging
logging_console_handler = logging.StreamHandler()
logging_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
logging_console_handler.setFormatter(logging_formatter)
logger.addHandler(logging_console_handler)


def _upgrade_query_job_id_columns(db_cursor) -> None:
    """
    Upgrades `query_jobs.id` and `query_tasks.job_id` to `BIGINT UNSIGNED` in databases created
    before they were. MariaDB can't change a column that a foreign key uses, so the foreign key is
    dropped before the change and added back after it. Each step runs only if it's still needed, so
    an upgrade that fails partway through is completed by the next run.

    :param db_cursor:
    """
    columns_to_upgrade = [
        (table_name, column_name, column_definition)
        for table_name, column_name, column_definition in (
            (QUERY_JOBS_TABLE_NAME, "id", "BIGINT UNSIGNED NOT NULL AUTO_INCREMENT"),
            (QUERY_TASKS_TABLE_NAME, "job_id", "BIGINT UNSIGNED NOT NULL"),
        )
        if not _is_bigint_unsigned_column(db_cursor, table_name, column_name)
    ]
    has_foreign_key = _has_foreign_key(db_cursor, QUERY_TASKS_TABLE_NAME, QUERY_TASKS_TABLE_NAME)
    if len(columns_to_upgrade) > 0:
        if has_foreign_key:
            db_cursor.execute(
                f"""
                ALTER TABLE `{QUERY_TASKS_TABLE_NAME}`
                DROP FOREIGN KEY `{QUERY_TASKS_TABLE_NAME}`
                """
            )
            has_foreign_key = False
        for table_name, column_name, column_definition in columns_to_upgrade:
            logger.info("Upgrading `%s`.`%s` to BIGINT UNSIGNED.", table_name, column_name)
            db_cursor.execute(
                f"ALTER TABLE `{table_name}` MODIFY `{column_name}` {column_definition}"
            )
    if not has_foreign_key:
        db_cursor.execute(
            f"""
            ALTER TABLE `{QUERY_TASKS_TABLE_NAME}`
            ADD CONSTRAINT `{QUERY_TASKS_TABLE_NAME}` FOREIGN KEY (`job_id`)
            REFERENCES `{QUERY_JOBS_TABLE_NAME}` (`id`)
            ON UPDATE NO ACTION ON DELETE NO ACTION
            """
        )


def _is_bigint_unsigned_column(db_cursor, table_name: str, column_name: str) -> bool:
    """
    :param db_cursor:
    :param table_name:
    :param column_name:
    :return: Whether the column exists and is a `BIGINT UNSIGNED`.
    """
    db_cursor.execute(
        """
        SELECT COUNT(*) AS `num_matches`
        FROM `information_schema`.`COLUMNS`
        WHERE `TABLE_SCHEMA` = DATABASE()
            AND `TABLE_NAME` = %s
            AND `COLUMN_NAME` = %s
            AND `DATA_TYPE` = 'bigint'
            AND LOCATE('unsigned', `COLUMN_TYPE`) > 0
        """,
        (table_name, column_name),
    )
    return db_cursor.fetchone()["num_matches"] > 0


def _has_foreign_key(db_cursor, table_name: str, constraint_name: str) -> bool:
    """
    :param db_cursor:
    :param table_name:
    :param constraint_name:
    :return: Whether the table has a foreign key with the given name.
    """
    db_cursor.execute(
        """
        SELECT COUNT(*) AS `num_matches`
        FROM `information_schema`.`TABLE_CONSTRAINTS`
        WHERE `CONSTRAINT_SCHEMA` = DATABASE()
            AND `TABLE_NAME` = %s
            AND `CONSTRAINT_NAME` = %s
            AND `CONSTRAINT_TYPE` = 'FOREIGN KEY'
        """,
        (table_name, constraint_name),
    )
    return db_cursor.fetchone()["num_matches"] > 0


def main(argv):
    args_parser = argparse.ArgumentParser(
        description="Sets up metadata tables for job orchestration."
    )
    args_parser.add_argument("--config", "-c", required=True, help="CLP configuration file.")
    parsed_args = args_parser.parse_args(argv[1:])

    # Load configuration
    config_path = pathlib.Path(parsed_args.config)
    try:
        clp_config = ClpConfig.model_validate(read_yaml_config_file(config_path))
        clp_config.database.load_credentials_from_env()
    except (ValidationError, ValueError) as err:
        logger.error(err)
        return -1
    except Exception:
        logger.exception("Failed to load CLP configuration.")
        return -1

    try:
        sql_adapter = SqlAdapter(clp_config.database)
        with (
            closing(sql_adapter.create_connection(True)) as scheduling_db,
            closing(scheduling_db.cursor(dictionary=True)) as scheduling_db_cursor,
        ):
            scheduling_db_cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{COMPRESSION_JOBS_TABLE_NAME}` (
                    `id` INT NOT NULL AUTO_INCREMENT,
                    `status` INT NOT NULL DEFAULT '{CompressionJobStatus.PENDING}',
                    `status_msg` VARCHAR(512) NOT NULL DEFAULT '',
                    `creation_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                    `start_time` DATETIME(3) NULL DEFAULT NULL,
                    `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP()
                        ON UPDATE CURRENT_TIMESTAMP(),
                    `duration` FLOAT NULL DEFAULT NULL,
                    `original_size` BIGINT NOT NULL DEFAULT '0',
                    `uncompressed_size` BIGINT NOT NULL DEFAULT '0',
                    `compressed_size` BIGINT NOT NULL DEFAULT '0',
                    `num_tasks` INT NOT NULL DEFAULT '0',
                    `num_tasks_completed` INT NOT NULL DEFAULT '0',
                    `clp_binary_version` INT NULL DEFAULT NULL,
                    `spider_id` BIGINT UNSIGNED NULL DEFAULT NULL,
                    `dispatch_time` DATETIME NULL DEFAULT NULL,
                    `clp_config` MEDIUMBLOB NOT NULL,
                    PRIMARY KEY (`id`) USING BTREE,
                    INDEX `JOB_STATUS` (`status`) USING BTREE,
                    INDEX `JOB_UPDATE_TIME` (`update_time`) USING BTREE,
                    INDEX `JOB_START_TIME_STATUS` (`start_time`, `status`) USING BTREE,
                    INDEX `JOB_SPIDER_ID` (`spider_id`) USING BTREE
                ) ROW_FORMAT=DYNAMIC
                """
            )

            # Add index to existing tables that were created before this index
            # was added to the CREATE TABLE statement. Ignoring duplicate-key
            # errors makes this idempotent for databases that already have it.
            try:
                scheduling_db_cursor.execute(
                    f"""
                    ALTER TABLE `{COMPRESSION_JOBS_TABLE_NAME}`
                    ADD INDEX `JOB_START_TIME_STATUS`
                    (`start_time`, `status`) USING BTREE
                    """
                )
            except Exception as err:
                if not (hasattr(err, "errno") and err.errno == ER_DUP_KEYNAME):
                    raise

            scheduling_db_cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{COMPRESSION_TASKS_TABLE_NAME}` (
                    `id` BIGINT NOT NULL AUTO_INCREMENT,
                    `status` INT NOT NULL DEFAULT '{CompressionTaskStatus.PENDING}',
                    `creation_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                    `start_time` DATETIME(3) NULL DEFAULT NULL,
                    `duration` FLOAT NULL DEFAULT NULL,
                    `job_id` INT NOT NULL,
                    `clp_paths_to_compress` MEDIUMBLOB NOT NULL,
                    `partition_original_size` BIGINT NOT NULL,
                    `partition_uncompressed_size` BIGINT NULL DEFAULT NULL,
                    `partition_compressed_size` BIGINT NULL DEFAULT NULL,
                    PRIMARY KEY (`id`) USING BTREE,
                    INDEX `job_id` (`job_id`) USING BTREE,
                    INDEX `TASK_STATUS` (`status`) USING BTREE,
                    INDEX `TASK_START_TIME` (`start_time`) USING BTREE,
                    CONSTRAINT `{COMPRESSION_TASKS_TABLE_NAME}` FOREIGN KEY (`job_id`) 
                    REFERENCES `{COMPRESSION_JOBS_TABLE_NAME}` (`id`)
                    ON UPDATE NO ACTION ON DELETE NO ACTION
                ) ROW_FORMAT=DYNAMIC
                """
            )

            scheduling_db_cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{QUERY_JOBS_TABLE_NAME}` (
                    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    `type` INT NOT NULL,
                    `status` INT NOT NULL DEFAULT '{QueryJobStatus.PENDING}',
                    `status_msg` TEXT NULL DEFAULT NULL,
                    `creation_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                    `num_tasks` INT NOT NULL DEFAULT '0',
                    `num_tasks_completed` INT NOT NULL DEFAULT '0',
                    `start_time` DATETIME(3) NULL DEFAULT NULL,
                    `duration` FLOAT NULL DEFAULT NULL,
                    `job_config` MEDIUMBLOB NOT NULL,
                    `spider_id` BIGINT UNSIGNED NULL DEFAULT NULL,
                    `dispatch_time` DATETIME NULL DEFAULT NULL,
                    PRIMARY KEY (`id`) USING BTREE,
                    INDEX `CREATION_TIME` (`creation_time`) USING BTREE,
                    INDEX `JOB_STATUS` (`status`) USING BTREE,
                    INDEX `JOB_SPIDER_ID` (`spider_id`) USING BTREE
                ) ROW_FORMAT=DYNAMIC
                """
            )

            scheduling_db_cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{QUERY_TASKS_TABLE_NAME}` (
                    `id` BIGINT NOT NULL AUTO_INCREMENT,
                    `status` INT NOT NULL DEFAULT '{QueryTaskStatus.PENDING}',
                    `creation_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                    `start_time` DATETIME(3) NULL DEFAULT NULL,
                    `duration` FLOAT NULL DEFAULT NULL,
                    `job_id` BIGINT UNSIGNED NOT NULL,
                    `archive_id` VARCHAR(255) NULL DEFAULT NULL,
                    PRIMARY KEY (`id`) USING BTREE,
                    INDEX `job_id` (`job_id`) USING BTREE,
                    INDEX `TASK_STATUS` (`status`) USING BTREE,
                    INDEX `TASK_START_TIME` (`start_time`) USING BTREE,
                    CONSTRAINT `{QUERY_TASKS_TABLE_NAME}` FOREIGN KEY (`job_id`) 
                    REFERENCES `{QUERY_JOBS_TABLE_NAME}` (`id`)
                    ON UPDATE NO ACTION ON DELETE NO ACTION
                ) ROW_FORMAT=DYNAMIC
                """
            )

            _upgrade_query_job_id_columns(scheduling_db_cursor)

            scheduling_db.commit()
    except:
        logger.exception("Failed to create scheduling tables.")
        return -1

    return 0


if "__main__" == __name__:
    sys.exit(main(sys.argv))
