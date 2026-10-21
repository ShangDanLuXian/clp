# Streaming search experiments: results of 2026-10-21 (local WSL2 machine)

All times are UTC. Job IDs are `query_jobs.id`. Every number below comes from the JSON files and
service logs in this directory.

## 1. Environment

| What | Value |
|---|---|
| Machine | Intel Core i7-13700K, 24 logical CPUs, 23.5 GiB RAM (WSL2) |
| Kernel | `6.18.33.1-microsoft-standard-WSL2` |
| Docker | Docker Desktop, server 29.5.3 |
| `git rev-parse HEAD` | `1abbda7e` for `preflight`, `gen-logs`, `compress`; `18e47190` for `correctness` through `collect` (harness fixes only between them, see section 7) |
| Package image | `clp-package:dev-xuhuichen-855a` (`sha256:855a67c0f552…`) |
| Spider worker image | `clp-spider-worker:dev-xuhuichen-b7d4` (`sha256:b7d447407903…`), tagged as `ghcr.io/y-scope/clp/clp-spider-worker:main` |
| `clp-s` | the same binary on the host (`build/core/clp-s`), in the package image and in the worker image (sha256 `5b36fde9…`) |
| Compose project | `clp-package-51d911fb-5143-47a0-bd98-3c911d9e28c9` |

Config block appended to `build/clp-package/etc/clp-config.yaml` (masked copy:
`clp-config.yaml.txt`):

```yaml
package:
  storage_engine: "clp-s"
  scheduler: "spider"
api_server:
  host: "localhost"
  port: 3001
  search_result_listener:
    port: 3003
spider:
  host: "localhost"
  port: 50051
  storage: {log_level: "INFO"}
  scheduler: {log_level: "INFO", policy: "resource_group_round_robin"}
  worker:
    log_level: "INFO"
    pools:
      general: {replicas: 4}
      compression: {replicas: 1}
compression_coordinator:
  logging_level: "INFO"
query_coordinator:
  logging_level: "INFO"
  query_task_max_retry: 1
database:
  port: 3307   # the host already runs a MariaDB on 127.0.0.1:3306; see section 7
```

Dataset: harness defaults, `gen-logs` with seed 20261021, 8 files of 40 MiB (335,545,014 bytes
in total, 2,196,937 records), one compression job per file, dataset `default`, timestamp key `ts`.
Query `*Transmitted*`: 659,677 results (30.0%).

Archives (`compress.json`): 8, each with 41,943,046 to 41,943,182 bytes uncompressed (40 MiB) and
7,533,976 to 7,845,582 bytes compressed. Derived timeouts (`preflight.json`, `compress.json`):
soft 10.8 s, hard 80.0 s for every archive (all archives are the same size).

## 2. Correctness

Reference: `clp-s s` over the 8 archives, 659,677 lines, sha256 `9c92d9a6…acc2ef`.

| Run | Job | Count | `identical_multiset` | `end` event | Final DB status |
|---|---|---|---|---|---|
| stream 0 | 5 | 659,677 | true | `Succeeded`, emitted 659,677, duplicates 0, protocol errors 0 | SUCCEEDED |
| stream 1 | 6 | 659,677 | true | same | SUCCEEDED |
| stream 2 | 7 | 659,677 | true | same | SUCCEEDED |
| results cache | 8 | 659,677 | true | (no `end` event on this path) | SUCCEEDED |

`all_identical: true`. No differences, so there is no `missing_sample` or `extra_sample`.

## 3. Latency

Medians over 3 runs (`latency.json`), time from sending the request:

| Path | First result (ms) | Last result (ms) |
|---|---|---|
| `POST /query/stream` | **85.3** | 3,549.8 |
| `POST /query` + `GET /query_results` | 3,112.1 | 5,395.7 |
| `sbin/streaming-search.sh --raw` (first stdout line) | 862.1 | 2,684.9 |

Per run:

| Run | stream TTFR / total | results cache TTFR / total | CLI TTFB / total |
|---|---|---|---|
| 0 | 119.3 / 3,528.4 (job 9) | 3,110.5 / 5,377.2 (job 10) | 862.1 / 2,699.1 |
| 1 | 77.3 / 3,558.6 (job 12) | 3,113.0 / 5,395.7 (job 13) | 839.1 / 2,673.1 |
| 2 | 85.3 / 3,549.8 (job 15) | 3,112.1 / 5,396.5 (job 16) | 866.0 / 2,684.9 |

All 9 runs returned 659,677 results; the CLI exited 0 every time. The CLI's TTFB includes about
0.8 s of `docker compose run` start-up for the `clp-runtime` container (its stderr shows the
container being created). The stream's total (3.5 s) is slower than the CLI's (2.7 s) because it is
the Python SSE client parsing 659,677 JSON events. The results cache gets its first result only
after the whole job has finished (about 3.1 s).

## 4. Disconnect

The client reads one result, then shuts down the socket (`disconnect.json`):

| Run | Job | s to CANCELLING | s to CANCELLED | Final status |
|---|---|---|---|---|
| 0 | 18 | 0.121 | 0.432 | CANCELLED |
| 1 | 19 | 0.144 | 0.446 | CANCELLED |
| 2 | 20 | 0.132 | 0.458 | CANCELLED |

So the job terminated 0.31 to 0.33 s after the cancel. With this dataset, cancelling stopped no
work early. For job 20, the API server's log says the tasks ran to the end and their results were
discarded:

```
01:31:33.620 api-server  A streaming search was dropped before its end; cancelling its query job. query_job_id=20
01:31:33.622 api-server  Marked the query job as cancelling. query_job_id=20
01:31:33.902 query-coordinator  The query job completed, but it had already been requested to be cancelled. query_job_id=20
01:31:33.929 api-server  The query job of a dropped streaming search terminated. status=Cancelled num_results_emitted=358 num_results_discarded=659319
```

(`disconnect.logs.*.txt` covers only run 2 because the harness resets its `since` time on every
run; see section 7.)

## 5. Fault

Job 21 (`fault.json`). The client read the first result at 0.089 s, then stopped reading. At
8.44 s and again at 16.78 s the harness SIGKILLed every `clp-s` in the 4 `spider-worker`
containers: **1 per container each time, 8 kills in total** (`56b104247783`, `98ad8dae87b5`,
`a77b3aa6aa6e`, `1cb5000673e6`). Then the client read to the end.

* `end`: `status: Succeeded`, `num_results_emitted: 659677`, **`num_duplicates_dropped: 280778`**,
  **`num_protocol_errors: 0`**. The kills hit tasks mid-stream, and their retries re-sent results
  that were already delivered.
* `identical_multiset: true` (659,677 against 659,677).
* Status timeline: RUNNING at 0.206 s, SUCCEEDED at 20.263 s. Total 20.6 s.
* Spider retrying (`fault.logs.interesting.txt`). Each kill is a failed task instance; the
  failed instances are re-registered behind the tasks that had not started:

```
01:31:46.787 spider-worker-1  clp-s exited on failure. task_id=Index(0) elapsed_us=8155142
01:31:46.788 spider-storage   Task instance failed. task_id=Index(0) task_instance_id=162 job_state=Running
01:31:46.863 spider-storage   Task instance failed. task_id=Index(1) task_instance_id=161
01:31:46.943 spider-storage   Task instance failed. task_id=Index(3) task_instance_id=163
01:31:47.031 spider-storage   Task instance failed. task_id=Index(2) task_instance_id=164
01:31:56.588 spider-storage   Task instance failed. task_id=Index(4) task_instance_id=165
01:31:56.660 spider-storage   Task instance failed. task_id=Index(5) task_instance_id=166
01:31:56.748 spider-storage   Task instance failed. task_id=Index(6) task_instance_id=167
01:31:56.800 spider-storage   Task instance failed. task_id=Index(7) task_instance_id=168
01:31:57.310 spider-storage   WARN The request was issued from a stale cache state. error="the number of living task instances has reached the upper limit"
01:31:57.312 spider-storage   WARN The request was issued from a stale cache state. error="task already in terminal state Succeeded"
```

  Each task was killed once, so with `query_task_max_retry: 1` the job had used all its retries
  and still succeeded. The listener saw each killed connection as a truncated frame (8 ×
  `Connection ended abnormally … the connection closed in the middle of a frame`, at
  01:31:57.13 to 57.65, task indexes 0 to 7). Those warnings only appeared once the client
  resumed reading at 01:31:57. The listener does not read the task sockets while the SSE
  client is stalled, which is the backpressure the stall phase relies on.

## 6. Stall

Job 22 (`stall.json`, `stall.logs.*.txt`). Stall **125 s** (the default: the 80.0 s hard timeout
+ 45 s). The soft timeout was 10.8 s. `query_task_max_retry: 1`.

* Status timeline: RUNNING at 0.196 s, **SUCCEEDED** at 127.69 s. The job never went to FAILED.
* `end`: `status: Succeeded`, `num_results_emitted: 659677`, **`num_duplicates_dropped: 328619`**,
  `num_protocol_errors: 0`. `error`: none. `stream_closed_cleanly: true`, `read_error: null`.
* `identical_multiset: true` (659,677 against 659,677) after the resume.
* First result at 0.081 s, resume at 125.089 s, total 129.1 s.

What happened, from the worker and storage logs (job 22 started 01:32:02.26, 8 tasks, 4 slots;
the client stalled from about 01:32:02.3 until it resumed at about 01:34:07.3):

```
01:32:02.286 spider-worker-2/4/1/3  clp-s query task started. task 0, 2, 3, 1
01:32:02.746 spider-worker-3  clp-s query task completed successfully. task_id=1 (elapsed 0.46 s; its results fitted in the buffers)
01:32:02.747 spider-worker-3  clp-s query task started. task_id=4
01:33:26.651 spider-worker-4  Executor time out triggered. executor_id=0
01:33:26.652 spider-worker-4  WARN Task hit the hard timeout. task_id=Index(2) hard_timeout_ms=80000
01:33:26.653 spider-worker-1  WARN Task hit the hard timeout. task_id=Index(3) hard_timeout_ms=80000
01:33:26.653 spider-worker-2  WARN Task hit the hard timeout. task_id=Index(0) hard_timeout_ms=80000
01:33:26.652 spider-storage   Task instance failed. task_id=Index(2) task_instance_id=183
01:33:26.655 spider-worker-4/1/2  clp-s query task started. task 5, 6, 7 (first attempts)
01:33:27.113 spider-worker-3  WARN Task hit the hard timeout. task_id=Index(4) hard_timeout_ms=80000
01:33:27.115 spider-worker-3  clp-s query task started. task_id=0 task_instance_id=189 (retry)
01:34:13.777 spider-worker-1  clp-s query task completed successfully. task_id=6 (elapsed 45.7 s)
01:34:13.777 spider-worker-1  clp-s query task started. task_id=2 task_instance_id=190 (retry)
01:34:13.851 spider-worker-2  clp-s query task started. task_id=3 task_instance_id=191 (retry)
01:34:14.160 spider-worker-4  clp-s query task started. task_id=4 task_instance_id=192 (retry)
01:34:14.488 spider-worker-3  clp-s query task completed successfully. task_id=0 task_instance_id=189 (elapsed 45.9 s)
01:34:14.489 spider-worker-3  WARN Storage rejected task registration. Dropping the assignment. task 0, 2, 3, 4, 5, 6, 7 ("task already in terminal state Succeeded" / "the number of living task instances has reached the upper limit")
01:34:15.222 spider-storage   Task instance succeeded. task_id=Index(4) task_instance_id=192 job_state=Succeeded
01:34:15.237 query-coordinator  Query job reached a terminal Spider state. query_job_id=22 outcome=Succeeded
```

Findings:

1. **The suspected flaw is real.** The 4 tasks that were running when the client stalled
   (0, 2, 3, 4) were killed by Spider's hard timeout although nothing was wrong with them. Each
   used up its one retry. The kill came 84.37 s after each task started (80 s configured): task
   2 started at 01:32:02.286 and timed out at 01:33:26.651, task 4 at 01:32:02.747 and
   01:33:27.113.
2. **It did not fail the job here, for a reason specific to this setup.** A failed task goes to
   the back of the queue, behind the tasks that had not started (8 tasks on 4 slots). So the
   second attempts started late. Task 0's retry started at 85 s, and tasks 2, 3 and 4 only
   retried after the client resumed, at 131.5 to 131.9 s. The first attempts of tasks 5, 6, 7
   and task 0's retry started at 84.4 to 85 s, so their own 80 s timers would have fired at
   about 169 s. The client resumed at 125 s, before that. A 125 s stall is shorter than the
   hard timeout of these second attempts plus the time they waited, so the default stall
   (hard + 45 s) cannot exhaust `query_task_max_retry: 1` in this layout. Extrapolating from
   these timestamps (not measured), a stall longer than about 170 s would have hard-killed
   task 0's retry and failed the job.
3. No soft-timeout re-dispatch: no log line mentions the soft timeout, and no task had two live
   instances while the client stalled. No `UnknownSession` and no protocol errors.
4. After a hard timeout the worker logs `Executor time out triggered` and respawns the executor,
   but logs no `clp-s exited on failure`. That line does appear for every SIGKILL in the fault
   phase. The API server logged no `Connection ended abnormally` for job 22 either, although
   four `clp-s` were stopped mid-stream. Possibly the timed-out `clp-s` processes were not killed
   with the executor and finished their streams after the resume, which would also fit the
   328,619 duplicates (about 4 archives' worth, 4 × 82,460). The logs cannot show which.
   Follow-up B samples the processes during a stall to settle it.
5. Task 1 finished in 0.46 s while the client was already stalled. The buffers between `clp-s`
   and the SSE client (socket buffers plus the listener's channel) held about one archive's
   results (82,000 results, about 15 MB) before backpressure reached `clp-s`.

## 7. Anything unexpected

Harness fixes, each committed separately:

* `1abbda7e`: `task package` puts `clp-s` only into the image, so `build/clp-package/bin/clp-s`
  does not exist and every phase that computes the reference failed. The harness now falls back
  to `build/core/clp-s`, the identical binary.
* `7d17f9f2`: `GET /query_results` sends each result's log line itself as the SSE data, not a
  `{"message": …}` object. `correctness` failed with `KeyError('message')` on the results-cache
  path, after all three stream runs had passed.
* `18e47190`: re-running phases into the same directory overwrote `summary.json` and lost the
  earlier phases. It now keeps them (the first invocation is under `previous_invocations`).
* `d183158b`: `experiments.log` recorded every `mariadb -p<password>` command with the database
  password in clear (169 times). The harness now masks it, and the password was replaced with
  `<masked>` in this directory's `experiments.log` before committing. No other file contained a
  password.

Phases re-run: the first `all` invocation completed `preflight`, `gen-logs` and `compress` and
failed in `correctness` (results-cache `KeyError`). `correctness latency disconnect fault stall
collect` were then re-run into the same directory against the same archives. Jobs 1 to 4 are from
the failed invocation.

Package and setup:

* `start-clp.sh` failed twice. (1) The host's own MariaDB holds 127.0.0.1:3306, so the package's
  database was moved to 3307. The harness reaches the database through `docker compose exec`,
  so this does not affect it. (2) `docker compose up --wait` fails because
  `compression-coordinator` exits with `The compression coordinator only supports S3 logs
  inputs.` (`logs_input` is `fs`, as the handoff says to keep it). Without it the Spider
  `compression` resource group is never registered, so `spider-worker-compression` exits with
  `failed to register with storage: resource group authentication failed`. Neither is used here:
  `sbin/compress.sh` ran on the Celery compression workers (8 jobs, about 2 s each). The other
  services were up, `/health` returned 200, and all 4 `spider-worker` replicas registered.
* **The vendored Spider compose file sets `pull_policy: ${SPIDER_PULL_POLICY:-always}`.**
  `start-clp.sh` therefore pulled `ghcr.io/y-scope/clp/clp-spider-worker:main` over the local
  tag from the handoff's `docker tag` step. The workers first started on the upstream image
  (`7c7d3f03c3c8`, without this branch's `clp-s`). Fix used: re-tag, then run the same compose
  command with the generated `.env`, `SPIDER_PULL_POLICY=missing docker compose --project-name
  <project> --file docker-compose-spider.yaml up --detach --wait`, which recreated the 5 worker
  containers on `b7d447407903`. The runtime container that `start-clp.sh` uses does not forward
  `SPIDER_PULL_POLICY`, so the handoff's tag step alone is not enough on a machine that can reach
  ghcr.io. Every experiment ran on the right image: `clp-s` in the workers has the same sha256
  as `build/core/clp-s`.
* `query_jobs.duration` is NULL and `num_tasks_completed` is 0 for every job in Spider mode
  (`collect.json`, `query_jobs_tail`), including SUCCEEDED ones.
* Warnings in the service logs (`collect.logs.txt`, 24 h): 12 × `The request was issued from a
  stale cache state.` and 12 × `Storage rejected task registration. Dropping the assignment.`
  (after retries, shown above; harmless here); 8 × `clp-s exited on failure.` and
  8 × `Connection ended abnormally.` (the fault kills); 4 × `Task hit the hard timeout.` and
  4 × `Executor time out triggered.` (the stall); 4 × `Failed to load external credentials …
  Continue without a pinned resource group` (each general-pool worker at start-up, which has no
  `SPIDER_EXTERNAL_RESOURCE_GROUP_*` variables, so it is not pinned to a group) and 8 ×
  `Invalid resource group. … external resource group 'compression' does not exist` (the
  compression worker's start-up attempts, see above).
* `disconnect.logs.*.txt` covers only the last disconnect run, because `phase_disconnect`
  resets `since` on every run. Not fixed, to keep the committed harness the one that produced
  these numbers.
* Slot use: 8 tasks on 4 general-pool slots. In the stall, all 4 slots stayed busy with blocked
  tasks for the whole stall, so no other query could have run. That is the slot starvation
  noted in the design review. `docker stats` was not sampled.
* Results directory size: 22 MB, of which `collect.logs.txt` is 15 MB and `stall.logs.txt` is
  5.2 MB (debug-level Spider storage logs).
