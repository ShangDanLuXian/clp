import json
import os
import subprocess
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

    clp_s_path = Path(clp_home) / "bin" / "clp-s"
    if not clp_s_path.exists():
        return {"ok": False, "error": f"clp-s not found at {clp_s_path}"}

    cmd = [
        str(clp_s_path),
        "f",
        "--pack-path",
        pack_path,
        "--archives",
        ",".join(archive_ids),
        "--query",
        query,
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
        logger.exception("Filter scan task failed to start clp-s (job_id=%s)", job_id)
        return {"ok": False, "error": f"failed to run clp-s: {exc}"}

    if proc.returncode != 0:
        logger.error(
            "Filter scan task failed (job_id=%s) return_code=%s stderr=%s",
            job_id,
            proc.returncode,
            proc.stderr,
        )
        return {"ok": False, "error": "clp-s filter scan failed", "stderr": proc.stderr}

    try:
        output = json.loads(proc.stdout.strip())
    except Exception as exc:
        logger.error(
            "Filter scan task failed to parse output (job_id=%s): %s",
            job_id,
            proc.stdout,
        )
        return {"ok": False, "error": f"invalid clp-s output: {exc}", "stdout": proc.stdout}

    output["ok"] = True
    return output
