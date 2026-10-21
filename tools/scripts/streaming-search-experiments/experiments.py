#!/usr/bin/env python3
"""Runs the streaming search experiments against a running CLP package in Spider mode.

The driver needs only the Python standard library, Docker, and a started package (see README.md).
Every phase writes its results as JSON and text files into a results directory that can be
committed and pushed for analysis.

Phases:
  preflight    Check Docker, the compose project, the API server, the database, and the archives.
  gen-logs     Generate synthetic JSONL logs for the experiments.
  compress     Compress the generated (or given) logs into the package, one job per file.
  correctness  Compare the results of `POST /query/stream`, the results-cache path, and clp-s.
  latency      Time to first result: stream vs results cache vs `sbin/streaming-search.sh`.
  disconnect   Measure how fast a client disconnect marks the job CANCELLING.
  fault        SIGKILL every running clp-s while the client stalls; check nothing is lost.
  stall        Stall the client longer than Spider's hard timeout; record what happens.
  collect      Dump versions, config, archives, and service logs.
  all          Everything above, in that order.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import http.client
import json
import os
import random
import re
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

QUERY_JOB_STATUS = {
    0: "PENDING",
    1: "RUNNING",
    2: "SUCCEEDED",
    3: "FAILED",
    4: "CANCELLING",
    5: "CANCELLED",
    6: "KILLED",
}
TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "CANCELLED", "KILLED"}

# Mirrors `compute_query_task_execution_policy` in query-coordinator/src/job_handle.rs.
SOFT_TIMEOUT_BASE_SECS = 10.0
SOFT_TIMEOUT_SECS_PER_MIB = 0.020
HARD_TIMEOUT_BASE_SECS = 60.0
HARD_TIMEOUT_SECS_PER_MIB = 0.500

LOG_SERVICES = [
    "api-server",
    "query-coordinator",
    "spider-scheduler",
    "spider-storage",
    "spider-worker",
    "spider-worker-compression",
]
INTERESTING_LOG_PATTERN = re.compile(
    r"timeout|timed out|kill|retry|retried|fail|cancel|protocol|duplicate|dropped|stale|unknown session",
    re.IGNORECASE,
)


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_of_lines(lines: list[str]) -> str:
    digest = hashlib.sha256()
    for line in lines:
        digest.update(line.encode())
        digest.update(b"\n")
    return digest.hexdigest()


class Ctx:
    """Everything the phases share: paths, the compose project, credentials, and logging."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.package_dir = Path(args.package_dir).resolve()
        self.results_dir = Path(args.results_dir).resolve()
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = open(self.results_dir / "experiments.log", "a")
        self.api_host = args.api_host
        self.api_port = args.api_port
        self.dataset = args.dataset
        self.query = args.query
        self.project: str | None = None
        self.db_user = self.db_pass = self.db_name = None

    # ----- logging -----
    def log(self, msg: str) -> None:
        line = f"[{utc_now_iso()}] {msg}"
        print(line, flush=True)
        self.log_file.write(line + "\n")
        self.log_file.flush()

    def save(self, name: str, data) -> Path:
        path = self.results_dir / name
        if isinstance(data, (dict, list)):
            path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
        else:
            path.write_text(str(data))
        self.log(f"wrote {path}")
        return path

    # ----- processes -----
    def run(self, cmd: list[str], check: bool = True, timeout: float | None = None, **kw) -> subprocess.CompletedProcess:
        self.log("$ " + " ".join(cmd))
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, **kw)
        if proc.returncode != 0:
            self.log(f"  rc={proc.returncode} stderr={proc.stderr.strip()[-2000:]}")
            if check:
                raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(cmd)}")
        return proc

    def compose(self, *args: str, check: bool = True, timeout: float | None = 600) -> subprocess.CompletedProcess:
        assert self.project, "compose project not discovered; run preflight first"
        return self.run(["docker", "compose", "--project-name", self.project, *args], check=check, timeout=timeout, cwd=self.package_dir)

    # ----- package discovery -----
    def discover_project(self) -> str:
        if self.args.project:
            self.project = self.args.project
            return self.project
        proc = self.run(["docker", "compose", "ls", "--format", "json"])
        projects = [p for p in json.loads(proc.stdout or "[]") if p.get("Name", "").startswith("clp-package")]
        running = [p for p in projects if "running" in p.get("Status", "")]
        chosen = running or projects
        if not chosen:
            raise RuntimeError("no running `clp-package-*` compose project found; start the package first")
        if len(chosen) > 1:
            self.log(f"several clp-package projects found, using the first: {[p['Name'] for p in chosen]}")
        self.project = chosen[0]["Name"]
        return self.project

    def load_credentials(self) -> None:
        creds = read_simple_yaml(self.package_dir / "etc" / "credentials.yaml")
        self.db_user = creds.get("database", {}).get("username", "clp-user")
        self.db_pass = creds.get("database", {}).get("password")
        if not self.db_pass:
            raise RuntimeError("could not read database.password from etc/credentials.yaml")
        config = read_simple_yaml(self.package_dir / "etc" / "clp-config.yaml")
        self.db_name = config.get("database", {}).get("names", {}).get("clp", "clp-db")

    # ----- database -----
    def db_rows(self, sql: str) -> list[list[str]]:
        """Runs `sql` through the database container's client; returns tab-separated rows."""
        for client in ("mariadb", "mysql"):
            proc = self.compose(
                "exec", "-T", "database", client, "-N", "-B",
                f"-u{self.db_user}", f"-p{self.db_pass}", self.db_name, "-e", sql,
                check=False, timeout=60,
            )
            if proc.returncode == 0:
                return [row.split("\t") for row in proc.stdout.splitlines() if row]
        raise RuntimeError(f"database query failed: {sql}")

    def job_status(self, job_id: int) -> str:
        rows = self.db_rows(f"SELECT status FROM query_jobs WHERE id = {int(job_id)}")
        if not rows:
            return "MISSING"
        return QUERY_JOB_STATUS.get(int(rows[0][0]), rows[0][0])

    def archives(self) -> list[dict]:
        tables = [r[0] for r in self.db_rows("SHOW TABLES LIKE '%archives'")]
        tables = [t for t in tables if not t.endswith("_files")]
        out = []
        for table in tables:
            for row in self.db_rows(f"SELECT id, uncompressed_size, size FROM `{table}`"):
                out.append({"table": table, "id": row[0], "uncompressed_size": int(row[1]), "size": int(row[2])})
        return out

    def archives_dir(self) -> Path:
        base = self.package_dir / "var" / "data" / "archives"
        with_dataset = base / self.dataset
        return with_dataset if with_dataset.is_dir() else base

    def clp_s_bin(self) -> Path:
        path = self.package_dir / "bin" / "clp-s"
        if not path.exists():
            raise RuntimeError(f"clp-s binary not found at {path}")
        return path

    def worker_containers(self) -> list[str]:
        proc = self.run([
            "docker", "ps", "-q",
            "--filter", f"label=com.docker.compose.project={self.project}",
            "--filter", "label=com.docker.compose.service=spider-worker",
        ])
        return proc.stdout.split()

    def timeouts(self) -> dict:
        """The soft/hard timeouts Spider enforces for the largest and smallest archive."""
        sizes_mib = [a["uncompressed_size"] // (1024 * 1024) for a in self.archives()]
        if not sizes_mib:
            return {}
        mx, mn = max(sizes_mib), min(sizes_mib)
        return {
            "num_archives": len(sizes_mib),
            "max_archive_mib": mx,
            "min_archive_mib": mn,
            "soft_timeout_secs_max": SOFT_TIMEOUT_BASE_SECS + SOFT_TIMEOUT_SECS_PER_MIB * mx,
            "hard_timeout_secs_max": HARD_TIMEOUT_BASE_SECS + HARD_TIMEOUT_SECS_PER_MIB * mx,
            "hard_timeout_secs_min": HARD_TIMEOUT_BASE_SECS + HARD_TIMEOUT_SECS_PER_MIB * mn,
        }

    def service_logs(self, since_iso: str, name: str) -> Path:
        """Saves the services' logs since `since_iso` and the interesting lines among them."""
        path = self.results_dir / f"{name}.logs.txt"
        interesting = self.results_dir / f"{name}.logs.interesting.txt"
        with open(path, "w") as out, open(interesting, "w") as hits:
            for service in LOG_SERVICES:
                proc = self.compose("logs", "--no-color", "--timestamps", "--since", since_iso, service, check=False, timeout=300)
                out.write(f"===== {service} (rc={proc.returncode}) =====\n{proc.stdout}\n{proc.stderr}\n")
                for line in proc.stdout.splitlines():
                    if INTERESTING_LOG_PATTERN.search(line):
                        hits.write(f"{service}: {line}\n")
        self.log(f"wrote {path} and {interesting}")
        return path


def read_simple_yaml(path: Path) -> dict:
    """Reads the subset of YAML the package's config and credentials use: nested maps of scalars."""
    root: dict = {}
    stack: list[tuple[int, dict]] = [(-1, root)]
    if not path.exists():
        return root
    for raw in path.read_text().splitlines():
        line = raw.split("#", 1)[0].rstrip() if not raw.lstrip().startswith("#") else ""
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        key, _, value = line.strip().partition(":")
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        value = value.strip()
        if value == "":
            child: dict = {}
            parent[key.strip()] = child
            stack.append((indent, child))
        else:
            if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
                value = value[1:-1]
            parent[key.strip()] = value
    return root


# ----------------------------------------------------------------------------------------------
# HTTP / SSE
# ----------------------------------------------------------------------------------------------
class SseStream:
    """An SSE response from the API server, read event by event."""

    def __init__(self, ctx: Ctx, method: str, path: str, body: dict | None = None, timeout: float = 3600.0) -> None:
        self.ctx = ctx
        self.conn = http.client.HTTPConnection(ctx.api_host, ctx.api_port, timeout=timeout)
        headers = {"Accept": "text/event-stream"}
        payload = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            payload = json.dumps(body)
        self.t_request = time.monotonic()
        self.conn.request(method, path, body=payload, headers=headers)
        self.resp = self.conn.getresponse()
        self.t_response = time.monotonic()
        self.status = self.resp.status
        if self.status != 200:
            self.error_body = self.resp.read().decode(errors="replace")
        else:
            self.error_body = None

    def events(self):
        """Yields `(event_name, data)`; `event_name` is None for unnamed events."""
        name, data = None, []
        while True:
            line = self.resp.readline()
            if not line:
                if data:
                    yield name, "\n".join(data)
                return
            line = line.rstrip(b"\r\n").decode(errors="replace")
            if line == "":
                if data or name:
                    yield name, "\n".join(data)
                name, data = None, []
                continue
            if line.startswith(":"):
                continue  # keep-alive comment
            if line.startswith("event:"):
                name = line[len("event:"):].strip()
            elif line.startswith("data:"):
                data.append(line[len("data:"):].lstrip(" "))

    def shutdown(self) -> None:
        """Closes the TCP connection in a way the server notices at once."""
        sock = self.conn.sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.conn.close()

    def close(self) -> None:
        self.conn.close()


def stream_query_body(ctx: Ctx) -> dict:
    return {"query_string": ctx.query, "datasets": [ctx.dataset], "ignore_case": False, "max_num_results": 0}


def results_cache_query_body(ctx: Ctx) -> dict:
    return {
        "query_string": ctx.query,
        "datasets": [ctx.dataset],
        "ignore_case": False,
        "buffer_results_in_mongodb": True,
        "max_num_results": ctx.args.results_cache_limit,
    }


def read_stream_fully(ctx: Ctx, stream: SseStream, on_first=None) -> dict:
    """Consumes a `POST /query/stream` response; returns the job id, messages, end/error event, timings."""
    out = {"job_id": None, "messages": [], "end": None, "error": None, "t_first_result": None, "t_end": None}
    for name, data in stream.events():
        if name == "job":
            out["job_id"] = json.loads(data)["query_job_id"]
        elif name == "end":
            out["end"] = json.loads(data)
        elif name == "error":
            out["error"] = json.loads(data)
        elif name is None:
            if out["t_first_result"] is None:
                out["t_first_result"] = time.monotonic()
                if on_first is not None:
                    on_first(out)
            out["messages"].append(json.loads(data)["message"].rstrip("\n"))
    out["t_end"] = time.monotonic()
    return out


def read_results_cache(ctx: Ctx, job_id: int) -> dict:
    stream = SseStream(ctx, "GET", f"/query_results/{job_id}")
    if stream.status != 200:
        raise RuntimeError(f"GET /query_results/{job_id} -> {stream.status}: {stream.error_body}")
    out = {"messages": [], "t_first_result": None, "t_end": None, "t_request": stream.t_request}
    for name, data in stream.events():
        if name is None:
            if out["t_first_result"] is None:
                out["t_first_result"] = time.monotonic()
            out["messages"].append(json.loads(data)["message"].rstrip("\n"))
    out["t_end"] = time.monotonic()
    stream.close()
    return out


def submit_results_cache_query(ctx: Ctx) -> tuple[int, float]:
    conn = http.client.HTTPConnection(ctx.api_host, ctx.api_port, timeout=120)
    t0 = time.monotonic()
    conn.request("POST", "/query", body=json.dumps(results_cache_query_body(ctx)), headers={"Content-Type": "application/json"})
    resp = conn.getresponse()
    body = resp.read().decode()
    conn.close()
    if resp.status != 200:
        raise RuntimeError(f"POST /query -> {resp.status}: {body}")
    uri = json.loads(body)["query_results_uri"]
    return int(uri.rsplit("/", 1)[-1]), t0


class StatusPoller(threading.Thread):
    """Records every change of a query job's DB status, with timestamps relative to `t0`."""

    def __init__(self, ctx: Ctx, job_id: int, t0: float, interval: float = 1.0) -> None:
        super().__init__(daemon=True)
        self.ctx, self.job_id, self.t0, self.interval = ctx, job_id, t0, interval
        self.timeline: list[dict] = []
        self.stop_event = threading.Event()

    def run(self) -> None:
        last = None
        while not self.stop_event.is_set():
            try:
                status = self.ctx.job_status(self.job_id)
            except Exception as e:  # noqa: BLE001
                status = f"ERROR: {e}"
            if status != last:
                self.timeline.append({"t_secs": round(time.monotonic() - self.t0, 3), "status": status})
                last = status
            if status in TERMINAL_STATUSES:
                return
            self.stop_event.wait(self.interval)

    def stop(self) -> None:
        self.stop_event.set()
        self.join(timeout=30)


def reference_messages(ctx: Ctx) -> list[str]:
    """clp-s's own results for the query over the package's archives, sorted."""
    proc = ctx.run([str(ctx.clp_s_bin()), "s", str(ctx.archives_dir()), ctx.query], timeout=3600)
    return sorted(line.rstrip("\n") for line in proc.stdout.splitlines() if line.strip())


def compare(name: str, got: list[str], ref: list[str]) -> dict:
    got_s, ref_s = sorted(got), sorted(ref)
    result = {
        "name": name,
        "count": len(got_s),
        "reference_count": len(ref_s),
        "sha256": sha256_of_lines(got_s),
        "reference_sha256": sha256_of_lines(ref_s),
        "identical_multiset": got_s == ref_s,
    }
    if not result["identical_multiset"]:
        from collections import Counter

        missing = Counter(ref_s) - Counter(got_s)
        extra = Counter(got_s) - Counter(ref_s)
        result["num_missing"] = sum(missing.values())
        result["num_extra"] = sum(extra.values())
        result["missing_sample"] = list(missing.elements())[:5]
        result["extra_sample"] = list(extra.elements())[:5]
    return result


# ----------------------------------------------------------------------------------------------
# Phases
# ----------------------------------------------------------------------------------------------
def phase_preflight(ctx: Ctx) -> dict:
    out = {"package_dir": str(ctx.package_dir), "results_dir": str(ctx.results_dir)}
    ctx.run(["docker", "version", "--format", "{{.Server.Version}}"])
    out["project"] = ctx.discover_project()
    ctx.load_credentials()
    out["db_name"] = ctx.db_name
    ps = ctx.compose("ps", "--format", "json", check=False)
    out["compose_ps"] = ps.stdout
    conn = http.client.HTTPConnection(ctx.api_host, ctx.api_port, timeout=10)
    conn.request("GET", "/health")
    resp = conn.getresponse()
    out["api_health_status"] = resp.status
    resp.read()
    conn.close()
    if resp.status != 200:
        raise RuntimeError(f"GET /health -> {resp.status}; is the API server up on {ctx.api_host}:{ctx.api_port}?")
    out["query_jobs_count"] = int(ctx.db_rows("SELECT COUNT(*) FROM query_jobs")[0][0])
    out["archives"] = ctx.archives()
    out["timeouts"] = ctx.timeouts()
    out["worker_containers"] = ctx.worker_containers()
    out["clp_s_bin"] = str(ctx.package_dir / "bin" / "clp-s")
    out["clp_s_bin_exists"] = (ctx.package_dir / "bin" / "clp-s").exists()
    out["git_head"] = ctx.run(["git", "rev-parse", "HEAD"], check=False, cwd=Path(__file__).resolve().parents[3]).stdout.strip()
    ctx.save("preflight.json", out)
    return out


def phase_gen_logs(ctx: Ctx) -> dict:
    logs_dir = Path(ctx.args.logs_dir).resolve()
    logs_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(ctx.args.seed)
    files = []
    ts = 1_700_000_000_000
    seq = 0
    actions = ["Transmitted"] * 3 + ["Received"] * 5 + ["Dropped"] * 2  # 30% match `*Transmitted*`
    for file_index in range(ctx.args.num_files):
        path = logs_dir / f"experiment-{file_index:02d}.jsonl"
        target = ctx.args.file_size_mb * 1024 * 1024
        with open(path, "w") as f:
            written = 0
            while written < target:
                action = rng.choice(actions)
                record = {
                    "ts": ts,
                    "level": rng.choice(["INFO", "INFO", "INFO", "WARN", "ERROR"]),
                    "host": f"host-{rng.randrange(16):02d}",
                    "msg": f"{action} block {seq} on channel {rng.randrange(64)} payload {rng.getrandbits(128):032x}",
                    "seq": seq,
                }
                line = json.dumps(record, separators=(",", ":")) + "\n"
                f.write(line)
                written += len(line)
                ts += rng.randrange(1, 50)
                seq += 1
        files.append({"path": str(path), "bytes": path.stat().st_size})
        ctx.log(f"generated {path} ({path.stat().st_size} bytes)")
    out = {"logs_dir": str(logs_dir), "files": files, "total_records": seq}
    ctx.save("gen-logs.json", out)
    return out


def phase_compress(ctx: Ctx) -> dict:
    logs_dir = Path(ctx.args.logs_dir).resolve()
    paths = sorted(p for p in logs_dir.iterdir() if p.is_file())
    if not paths:
        raise RuntimeError(f"no files to compress in {logs_dir}; run gen-logs or pass --logs-dir")
    before = {a["id"] for a in ctx.archives()}
    jobs = []
    for path in paths:
        t0 = time.monotonic()
        cmd = [str(ctx.package_dir / "sbin" / "compress.sh"), "--dataset", ctx.dataset, "--no-progress-reporting"]
        if ctx.args.timestamp_key:
            cmd += ["--timestamp-key", ctx.args.timestamp_key]
        cmd.append(str(path))
        proc = ctx.run(cmd, check=False, timeout=3600)
        jobs.append({"path": str(path), "rc": proc.returncode, "secs": round(time.monotonic() - t0, 2), "stderr_tail": proc.stderr[-500:]})
        if proc.returncode != 0:
            raise RuntimeError(f"compress.sh failed for {path}")
    after = ctx.archives()
    out = {"jobs": jobs, "new_archives": [a for a in after if a["id"] not in before], "archives": after, "timeouts": ctx.timeouts()}
    ctx.save("compress.json", out)
    return out


def phase_correctness(ctx: Ctx) -> dict:
    ref = reference_messages(ctx)
    ctx.log(f"reference: {len(ref)} results from clp-s")
    out = {"query": ctx.query, "reference_count": len(ref), "reference_sha256": sha256_of_lines(ref), "runs": []}
    for run_index in range(ctx.args.runs):
        stream = SseStream(ctx, "POST", "/query/stream", stream_query_body(ctx))
        if stream.status != 200:
            raise RuntimeError(f"POST /query/stream -> {stream.status}: {stream.error_body}")
        got = read_stream_fully(ctx, stream)
        stream.close()
        run = {
            "run": run_index,
            "job_id": got["job_id"],
            "end": got["end"],
            "error": got["error"],
            "final_db_status": ctx.job_status(got["job_id"]) if got["job_id"] else None,
            "stream_vs_clp_s": compare("stream", got["messages"], ref),
            "archive_ids_seen": None,
        }
        out["runs"].append(run)
        ctx.log(f"run {run_index}: stream {len(got['messages'])} results, end={got['end']}, db={run['final_db_status']}")
        time.sleep(2)
    # Results-cache path, once.
    job_id, _ = submit_results_cache_query(ctx)
    cached = read_results_cache(ctx, job_id)
    out["results_cache"] = {
        "job_id": job_id,
        "final_db_status": ctx.job_status(job_id),
        "vs_clp_s": compare("results-cache", cached["messages"], ref),
    }
    ctx.log(f"results cache: {len(cached['messages'])} results, db={out['results_cache']['final_db_status']}")
    out["all_identical"] = all(r["stream_vs_clp_s"]["identical_multiset"] for r in out["runs"]) and out["results_cache"]["vs_clp_s"]["identical_multiset"]
    ctx.save("correctness.json", out)
    return out


def phase_latency(ctx: Ctx) -> dict:
    out = {"query": ctx.query, "runs": ctx.args.runs, "stream": [], "results_cache": [], "cli": []}
    cli = ctx.package_dir / "sbin" / "streaming-search.sh"
    for i in range(ctx.args.runs):
        # A: POST /query/stream
        stream = SseStream(ctx, "POST", "/query/stream", stream_query_body(ctx))
        if stream.status != 200:
            raise RuntimeError(f"POST /query/stream -> {stream.status}: {stream.error_body}")
        got = read_stream_fully(ctx, stream)
        stream.close()
        out["stream"].append({
            "job_id": got["job_id"],
            "ttfr_ms": round((got["t_first_result"] - stream.t_request) * 1000, 1) if got["t_first_result"] else None,
            "total_ms": round((got["t_end"] - stream.t_request) * 1000, 1),
            "count": len(got["messages"]),
            "end": got["end"],
        })
        time.sleep(2)
        # B: POST /query + GET /query_results
        job_id, t0 = submit_results_cache_query(ctx)
        cached = read_results_cache(ctx, job_id)
        out["results_cache"].append({
            "job_id": job_id,
            "ttfr_ms": round((cached["t_first_result"] - t0) * 1000, 1) if cached["t_first_result"] else None,
            "total_ms": round((cached["t_end"] - t0) * 1000, 1),
            "count": len(cached["messages"]),
        })
        time.sleep(2)
        # C: sbin/streaming-search.sh --raw
        if cli.exists():
            t0 = time.monotonic()
            proc = subprocess.Popen([str(cli), "--raw", "--dataset", ctx.dataset, "--", ctx.query], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=ctx.package_dir)
            first = None
            count = 0
            for line in proc.stdout:
                if first is None:
                    first = time.monotonic()
                count += 1
            proc.wait(timeout=3600)
            out["cli"].append({
                "rc": proc.returncode,
                "ttfb_ms": round((first - t0) * 1000, 1) if first else None,
                "total_ms": round((time.monotonic() - t0) * 1000, 1),
                "count": count,
                "stderr_tail": proc.stderr.read()[-800:],
            })
            time.sleep(2)
        ctx.log(f"latency run {i}: stream={out['stream'][-1]} cache={out['results_cache'][-1]}")

    def median(values):
        values = sorted(v for v in values if v is not None)
        return values[len(values) // 2] if values else None

    out["median_ttfr_ms"] = {
        "stream": median(r["ttfr_ms"] for r in out["stream"]),
        "results_cache": median(r["ttfr_ms"] for r in out["results_cache"]),
        "cli_ttfb": median(r["ttfb_ms"] for r in out["cli"]),
    }
    out["median_total_ms"] = {
        "stream": median(r["total_ms"] for r in out["stream"]),
        "results_cache": median(r["total_ms"] for r in out["results_cache"]),
        "cli": median(r["total_ms"] for r in out["cli"]),
    }
    ctx.save("latency.json", out)
    return out


def phase_disconnect(ctx: Ctx) -> dict:
    out = {"runs": []}
    for i in range(ctx.args.runs):
        since = utc_now_iso()
        stream = SseStream(ctx, "POST", "/query/stream", stream_query_body(ctx))
        if stream.status != 200:
            raise RuntimeError(f"POST /query/stream -> {stream.status}: {stream.error_body}")
        job_id = None
        first_seen = False
        for name, data in stream.events():
            if name == "job":
                job_id = json.loads(data)["query_job_id"]
            elif name is None:
                first_seen = True
                break
            elif name in ("end", "error"):
                break
        t_shutdown = time.monotonic()
        stream.shutdown()
        poller = StatusPoller(ctx, job_id, t_shutdown, interval=0.2)
        poller.start()
        deadline = time.monotonic() + ctx.args.disconnect_wait_secs
        while time.monotonic() < deadline and not (poller.timeline and poller.timeline[-1]["status"] in TERMINAL_STATUSES):
            time.sleep(0.5)
        poller.stop()
        cancelling = next((t["t_secs"] for t in poller.timeline if t["status"] == "CANCELLING"), None)
        run = {
            "run": i,
            "job_id": job_id,
            "disconnected_after_first_result": first_seen,
            "secs_to_cancelling_after_shutdown": cancelling,
            "status_timeline": poller.timeline,
            "final_db_status": ctx.job_status(job_id) if job_id else None,
        }
        out["runs"].append(run)
        ctx.log(f"disconnect run {i}: {run}")
        time.sleep(3)
    ctx.service_logs(since, "disconnect")
    ctx.save("disconnect.json", out)
    return out


def kill_all_clp_s(ctx: Ctx) -> dict:
    """SIGKILLs every clp-s process in every Spider worker container; returns what was killed."""
    script = (
        "killed=0; for d in /proc/[0-9]*; do "
        "if [ \"$(cat $d/comm 2>/dev/null)\" = clp-s ]; then kill -9 ${d#/proc/} && killed=$((killed+1)); fi; "
        "done; echo $killed"
    )
    killed = {}
    for container in ctx.worker_containers():
        proc = ctx.run(["docker", "exec", container, "sh", "-c", script], check=False, timeout=30)
        killed[container] = proc.stdout.strip()
    return killed


def phase_fault(ctx: Ctx) -> dict:
    ref = reference_messages(ctx)
    since = utc_now_iso()
    t0 = time.monotonic()
    stream = SseStream(ctx, "POST", "/query/stream", stream_query_body(ctx))
    if stream.status != 200:
        raise RuntimeError(f"POST /query/stream -> {stream.status}: {stream.error_body}")
    state = {"job_id": None, "messages": [], "end": None, "error": None, "kills": [], "t_first": None}
    events = stream.events()
    # Read up to the first result, then stop reading so the tasks block in send().
    for name, data in events:
        if name == "job":
            state["job_id"] = json.loads(data)["query_job_id"]
        elif name is None:
            state["t_first"] = round(time.monotonic() - t0, 3)
            state["messages"].append(json.loads(data)["message"].rstrip("\n"))
            break
        elif name in ("end", "error"):
            raise RuntimeError(f"job ended before the first result: {name} {data}")
    poller = StatusPoller(ctx, state["job_id"], t0)
    poller.start()
    for k in range(ctx.args.fault_kills):
        time.sleep(ctx.args.fault_stall_secs)
        killed = kill_all_clp_s(ctx)
        state["kills"].append({"t_secs": round(time.monotonic() - t0, 3), "killed_per_container": killed})
        ctx.log(f"kill {k}: {killed}")
    # Resume reading to the end.
    for name, data in events:
        if name == "end":
            state["end"] = json.loads(data)
        elif name == "error":
            state["error"] = json.loads(data)
        elif name is None:
            state["messages"].append(json.loads(data)["message"].rstrip("\n"))
    stream.close()
    poller.stop()
    out = {
        "job_id": state["job_id"],
        "t_first_result_secs": state["t_first"],
        "kills": state["kills"],
        "end": state["end"],
        "error": state["error"],
        "status_timeline": poller.timeline,
        "final_db_status": ctx.job_status(state["job_id"]),
        "stream_vs_clp_s": compare("fault-stream", state["messages"], ref),
        "total_secs": round(time.monotonic() - t0, 3),
    }
    ctx.service_logs(since, "fault")
    ctx.save("fault.json", out)
    return out


def phase_stall(ctx: Ctx) -> dict:
    ref = reference_messages(ctx)
    timeouts = ctx.timeouts()
    stall = ctx.args.stall_secs or (timeouts.get("hard_timeout_secs_max", 60.0) + 45.0)
    since = utc_now_iso()
    t0 = time.monotonic()
    stream = SseStream(ctx, "POST", "/query/stream", stream_query_body(ctx))
    if stream.status != 200:
        raise RuntimeError(f"POST /query/stream -> {stream.status}: {stream.error_body}")
    state = {"job_id": None, "messages": [], "end": None, "error": None, "t_first": None}
    events = stream.events()
    for name, data in events:
        if name == "job":
            state["job_id"] = json.loads(data)["query_job_id"]
        elif name is None:
            state["t_first"] = round(time.monotonic() - t0, 3)
            state["messages"].append(json.loads(data)["message"].rstrip("\n"))
            break
        elif name in ("end", "error"):
            raise RuntimeError(f"job ended before the first result: {name} {data}")
    poller = StatusPoller(ctx, state["job_id"], t0)
    poller.start()
    ctx.log(f"stalling the client for {stall:.0f} s (hard timeout max {timeouts.get('hard_timeout_secs_max')} s)")
    time.sleep(stall)
    t_resume = round(time.monotonic() - t0, 3)
    ctx.log("resuming reads")
    eof = True
    try:
        for name, data in events:
            if name == "end":
                state["end"] = json.loads(data)
            elif name == "error":
                state["error"] = json.loads(data)
            elif name is None:
                state["messages"].append(json.loads(data)["message"].rstrip("\n"))
    except (OSError, http.client.HTTPException) as e:
        eof = False
        state["read_error"] = repr(e)
    stream.close()
    # Let the DB status settle.
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and ctx.job_status(state["job_id"]) not in TERMINAL_STATUSES:
        time.sleep(1)
    poller.stop()
    out = {
        "job_id": state["job_id"],
        "timeouts": timeouts,
        "stall_secs": stall,
        "t_first_result_secs": state["t_first"],
        "t_resume_secs": t_resume,
        "end": state["end"],
        "error": state["error"],
        "stream_closed_cleanly": eof,
        "read_error": state.get("read_error"),
        "status_timeline": poller.timeline,
        "final_db_status": ctx.job_status(state["job_id"]),
        "stream_vs_clp_s": compare("stall-stream", state["messages"], ref),
        "total_secs": round(time.monotonic() - t0, 3),
    }
    ctx.service_logs(since, "stall")
    ctx.save("stall.json", out)
    return out


def phase_collect(ctx: Ctx) -> dict:
    out = {"git_head": None, "images": {}, "docker_server_version": None}
    out["git_head"] = ctx.run(["git", "rev-parse", "HEAD"], check=False, cwd=Path(__file__).resolve().parents[3]).stdout.strip()
    out["docker_server_version"] = ctx.run(["docker", "version", "--format", "{{.Server.Version}}"], check=False).stdout.strip()
    ps = ctx.compose("ps", "--format", "json", check=False)
    out["compose_ps"] = ps.stdout
    out["worker_containers"] = ctx.worker_containers()
    config = (ctx.package_dir / "etc" / "clp-config.yaml").read_text()
    config = re.sub(r"(password\s*:\s*).*", r"\1<masked>", config, flags=re.IGNORECASE)
    ctx.save("clp-config.yaml.txt", config)
    out["archives"] = ctx.archives()
    out["timeouts"] = ctx.timeouts()
    out["query_jobs_tail"] = ctx.db_rows("SELECT id, status, creation_time, start_time, duration, num_tasks, num_tasks_completed FROM query_jobs ORDER BY id DESC LIMIT 40")
    ctx.service_logs("24h", "collect")
    ctx.save("collect.json", out)
    return out


PHASES = {
    "preflight": phase_preflight,
    "gen-logs": phase_gen_logs,
    "compress": phase_compress,
    "correctness": phase_correctness,
    "latency": phase_latency,
    "disconnect": phase_disconnect,
    "fault": phase_fault,
    "stall": phase_stall,
    "collect": phase_collect,
}
ALL_ORDER = ["preflight", "gen-logs", "compress", "correctness", "latency", "disconnect", "fault", "stall", "collect"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("phases", nargs="+", choices=[*PHASES, "all"])
    parser.add_argument("--package-dir", default="build/clp-package", help="The built package (default: build/clp-package).")
    parser.add_argument("--results-dir", default=None, help="Where to write results (default: <this dir>/results/<UTC timestamp>).")
    parser.add_argument("--project", default=None, help="Compose project name (default: discovered).")
    parser.add_argument("--api-host", default="127.0.0.1")
    parser.add_argument("--api-port", type=int, default=3001)
    parser.add_argument("--dataset", default="default")
    parser.add_argument("--query", default="*Transmitted*")
    parser.add_argument("--runs", type=int, default=3, help="Repetitions for correctness, latency and disconnect.")
    parser.add_argument("--logs-dir", default=None, help="Logs to compress (default: <results dir>/logs, generated).")
    parser.add_argument("--timestamp-key", default="ts", help="Timestamp key for compress.sh; empty to omit.")
    parser.add_argument("--num-files", type=int, default=8, help="gen-logs: number of files (one compression job each).")
    parser.add_argument("--file-size-mb", type=int, default=40, help="gen-logs: size of each file.")
    parser.add_argument("--seed", type=int, default=20261021)
    parser.add_argument("--results-cache-limit", type=int, default=1_000_000, help="max_num_results for the results-cache path.")
    parser.add_argument("--disconnect-wait-secs", type=float, default=180.0)
    parser.add_argument("--fault-stall-secs", type=float, default=8.0, help="Seconds the client stalls before each kill.")
    parser.add_argument("--fault-kills", type=int, default=2)
    parser.add_argument("--stall-secs", type=float, default=None, help="Default: the largest archive's hard timeout + 45 s.")
    parser.add_argument("--skip-compress-if-archives", action="store_true", help="With `all`: skip gen-logs/compress when archives already exist.")
    args = parser.parse_args()

    if args.results_dir is None:
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        args.results_dir = str(Path(__file__).resolve().parent / "results" / stamp)
    ctx = Ctx(args)
    if args.logs_dir is None:
        args.logs_dir = str(ctx.results_dir / "logs")

    phases = ALL_ORDER if "all" in args.phases else args.phases
    summary = {"started": utc_now_iso(), "phases": {}, "args": vars(args)}
    ctx.save("summary.json", summary)
    for phase in phases:
        if phase != "preflight" and ctx.project is None:
            ctx.discover_project()
            ctx.load_credentials()
        if phase in ("gen-logs", "compress") and args.skip_compress_if_archives and ctx.archives():
            ctx.log(f"skipping {phase}: archives already exist")
            continue
        ctx.log(f"===== phase {phase} =====")
        t0 = time.monotonic()
        try:
            result = PHASES[phase](ctx)
            summary["phases"][phase] = {"ok": True, "secs": round(time.monotonic() - t0, 1)}
            if phase in ("correctness", "fault", "stall", "disconnect", "latency"):
                summary["phases"][phase]["highlights"] = highlights(phase, result)
        except Exception as e:  # noqa: BLE001
            ctx.log(f"phase {phase} FAILED: {e!r}")
            summary["phases"][phase] = {"ok": False, "error": repr(e), "secs": round(time.monotonic() - t0, 1)}
            summary["finished"] = utc_now_iso()
            ctx.save("summary.json", summary)
            return 1
        summary["finished"] = utc_now_iso()
        ctx.save("summary.json", summary)
    ctx.log("done; commit and push the results directory")
    return 0


def highlights(phase: str, result: dict) -> dict:
    if phase == "correctness":
        return {"all_identical": result["all_identical"], "reference_count": result["reference_count"]}
    if phase == "latency":
        return {"median_ttfr_ms": result["median_ttfr_ms"], "median_total_ms": result["median_total_ms"]}
    if phase == "disconnect":
        return {"secs_to_cancelling": [r["secs_to_cancelling_after_shutdown"] for r in result["runs"]], "final": [r["final_db_status"] for r in result["runs"]]}
    if phase in ("fault", "stall"):
        return {
            "end": result["end"],
            "error": result["error"],
            "final_db_status": result["final_db_status"],
            "identical": result["stream_vs_clp_s"]["identical_multiset"],
            "status_timeline": result["status_timeline"],
        }
    return {}


if __name__ == "__main__":
    sys.exit(main())
