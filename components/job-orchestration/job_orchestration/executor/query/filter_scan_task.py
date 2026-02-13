import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from celery.app.task import Task
from celery.utils.log import get_task_logger

from job_orchestration.executor.query.celery import app

# Setup logging
logger = get_task_logger(__name__)


@app.task(bind=True)
def filter_scan(
    self: Task,
    pack_path: str,
    archive_ids: list[str],
    query: str,
    job_id: str | None = None,
) -> dict[str, Any]:
    if not archive_ids:
        return {"ok": True, "supported": True, "passed": [], "total": 0, "skipped": 0}

    clp_home = os.getenv("CLP_HOME")
    if not clp_home:
        return {"ok": False, "error": "CLP_HOME not set"}

    clp_filter_path = Path(clp_home) / "bin" / "clp-filter"
    if not clp_filter_path.exists():
        return {"ok": False, "error": f"clp-filter not found at {clp_filter_path}"}

    fd, output_json_path = tempfile.mkstemp(prefix="clp-filter-", suffix=".json")
    os.close(fd)

    cmd = [
        str(clp_filter_path),
        "scan",
        "--pack-path",
        pack_path,
        "--archives",
        ",".join(archive_ids),
        "--query",
        query,
        "--output-json",
        output_json_path,
    ]
    logger.info("Filter scan task job_id=%s pack_path=%s archives=%s", job_id, pack_path, len(archive_ids))

    try:
        proc = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception as exc:
        logger.exception("Filter scan task failed to start clp-filter (job_id=%s)", job_id)
        try:
            os.unlink(output_json_path)
        except OSError:
            logger.warning("Failed to remove filter scan output file %s", output_json_path)
        return {"ok": False, "error": f"failed to run clp-filter: {exc}"}

    if proc.returncode != 0:
        logger.error(
            "Filter scan task failed (job_id=%s) return_code=%s stderr=%s",
            job_id,
            proc.returncode,
            proc.stderr,
        )
        try:
            os.unlink(output_json_path)
        except OSError:
            logger.warning("Failed to remove filter scan output file %s", output_json_path)
        return {"ok": False, "error": "clp-filter scan failed", "stderr": proc.stderr}

    try:
        with open(output_json_path, "r", encoding="utf-8") as output_file:
            output = json.load(output_file)
    except Exception as exc:
        logger.error(
            "Filter scan task failed to parse output (job_id=%s): %s",
            job_id,
            exc,
        )
        return {"ok": False, "error": f"invalid clp-filter output: {exc}"}
    finally:
        try:
            os.unlink(output_json_path)
        except OSError:
            logger.warning("Failed to remove filter scan output file %s", output_json_path)

    output["ok"] = True
    return output
