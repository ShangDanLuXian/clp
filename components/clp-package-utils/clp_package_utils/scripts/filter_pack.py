import argparse
import logging
import pathlib
import shlex
import subprocess
import sys

from clp_py_utils.clp_config import (
    ARCHIVE_MANAGER_ACTION_NAME,
    CLP_DB_PASS_ENV_VAR_NAME,
    CLP_DB_USER_ENV_VAR_NAME,
    CLP_DEFAULT_CONFIG_FILE_RELATIVE_PATH,
    CLP_DEFAULT_DATASET_NAME,
    ClpDbUserType,
    StorageEngine,
)
from clp_py_utils.core import resolve_host_path_in_container

from clp_package_utils.general import (
    dump_container_config,
    generate_container_config,
    generate_container_name,
    generate_container_start_cmd,
    get_clp_home,
    get_container_config_filename,
    JobType,
    load_config_file,
    validate_and_load_db_credentials_file,
    validate_dataset_name,
)
from clp_py_utils.s3_utils import generate_container_auth_options

logger = logging.getLogger(__name__)


def _generate_filter_pack_cmd(
    parsed_args: argparse.Namespace,
    dataset: str,
    config_path: pathlib.Path,
) -> list[str]:
    # fmt: off
    cmd = [
        "python3",
        "-m", "clp_package_utils.scripts.native.filter_pack",
        "--config", str(config_path),
        "--dataset", dataset,
        "--max-pack-size-bytes", str(parsed_args.max_pack_size_bytes),
    ]
    # fmt: on
    if parsed_args.verbose:
        cmd.append("--verbose")
    if parsed_args.dry_run:
        cmd.append("--dry-run")
    if parsed_args.job_id is not None:
        cmd.append("--job-id")
        cmd.append(str(parsed_args.job_id))
    return cmd


def main(argv):
    clp_home = get_clp_home()
    default_config_file_path = clp_home / CLP_DEFAULT_CONFIG_FILE_RELATIVE_PATH

    args_parser = argparse.ArgumentParser(description="Packs per-archive filters into pack files.")
    args_parser.add_argument(
        "--config",
        "-c",
        default=str(default_config_file_path),
        help="CLP package configuration file.",
    )
    args_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging.",
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
        "--job-id",
        type=int,
        default=None,
        help="Only pack filters produced by the given compression job id.",
    )
    args_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be packed without writing packs or updating the DB.",
    )

    parsed_args = args_parser.parse_args(argv[1:])
    logger.setLevel(logging.DEBUG if parsed_args.verbose else logging.INFO)

    # Validate and load config file
    try:
        config_file_path = pathlib.Path(parsed_args.config)
        clp_config = load_config_file(resolve_host_path_in_container(config_file_path))
        clp_config.validate_logs_dir(True)
        validate_and_load_db_credentials_file(clp_config, clp_home, False)
    except Exception:
        logger.exception("Failed to load config.")
        return -1

    storage_engine: StorageEngine = clp_config.package.storage_engine
    if StorageEngine.CLP_S != storage_engine:
        logger.error(f"Filter packing requires storage engine: {StorageEngine.CLP_S}.")
        return -1

    dataset = parsed_args.dataset
    try:
        clp_db_connection_params = clp_config.database.get_clp_connection_params_and_type(True)
        validate_dataset_name(clp_db_connection_params["table_prefix"], dataset)
    except Exception as e:
        logger.error(e)
        return -1

    container_name = generate_container_name(str(JobType.FILTER_PACKING))
    container_clp_config, mounts = generate_container_config(clp_config, clp_home)
    generated_config_path_on_container, generated_config_path_on_host = dump_container_config(
        container_clp_config, clp_config, get_container_config_filename(container_name)
    )

    necessary_mounts = [
        mounts.data_dir,
        mounts.logs_dir,
        mounts.archives_output_dir,
        mounts.filter_staging_dir,
    ]

    aws_mount, aws_env_vars = generate_container_auth_options(
        clp_config, ARCHIVE_MANAGER_ACTION_NAME
    )
    if aws_mount:
        necessary_mounts.append(mounts.aws_config_dir)

    credentials = clp_config.database.credentials
    extra_env_vars = {
        CLP_DB_PASS_ENV_VAR_NAME: credentials[ClpDbUserType.CLP].password,
        CLP_DB_USER_ENV_VAR_NAME: credentials[ClpDbUserType.CLP].username,
    }
    container_start_cmd = generate_container_start_cmd(
        container_name, necessary_mounts, clp_config.container_image_ref, extra_env_vars
    )
    filter_pack_cmd = _generate_filter_pack_cmd(
        parsed_args, dataset, generated_config_path_on_container
    )

    if len(aws_env_vars) != 0:
        for aws_env_var in aws_env_vars:
            container_start_cmd.extend(["-e", aws_env_var])

    cmd = container_start_cmd + filter_pack_cmd
    proc = subprocess.run(cmd, check=False)
    ret_code = proc.returncode
    if ret_code != 0:
        logger.error("Filter packing failed.")
        logger.debug(f"Docker command failed: {shlex.join(cmd)}")

    resolved_generated_config_path_on_host = resolve_host_path_in_container(
        generated_config_path_on_host
    )
    resolved_generated_config_path_on_host.unlink()

    return ret_code


if "__main__" == __name__:
    sys.exit(main(sys.argv))
