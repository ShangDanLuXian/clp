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

What happened, from the worker and storage logs. Job 22 started at 01:32:02.26, with 8 tasks on 4
slots. The client stalled from 01:32:02 until it resumed at 01:34:13 by the log clock. That is
125 s on the monotonic clock: log timestamps run about 5% fast on this host (see the clock note
in section 7).

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
   used up its one retry. By the log clock the kill came 84.37 s after each task started (task 2
   started at 01:32:02.286 and timed out at 01:33:26.651, task 4 at 01:32:02.747 and
   01:33:27.113). That is the configured 80 s on the monotonic clock.
2. **It did not fail the job here, because of how the retries were queued.** A failed task goes
   to the back of the queue, behind the tasks that had not started (8 tasks on 4 slots). So the
   second attempts started late. Task 0's retry started one hard timeout into the job, and the
   retries of tasks 2, 3 and 4 started only after the client resumed. The second attempts would
   have timed out about 2 × 80 = 160 s (monotonic) into the job. The client resumed at 125 s.
   Follow-ups B and C (section 8) measure where the job actually fails: it depends on whether one
   task finished from the buffers, which leaves a free slot for a retry in the first wave.
3. No soft-timeout re-dispatch: no log line mentions the soft timeout, and no task had two live
   instances while the client stalled. No `UnknownSession` and no protocol errors.
4. **A hard timeout does not kill `clp-s`.** The worker logs `Executor time out triggered` and
   respawns the executor, but no `clp-s exited on failure`, which every SIGKILL in the fault
   phase produces. The API server logged no `Connection ended abnormally` for job 22 either.
   Process sampling in follow-up B confirms it. The timed-out `clp-s` is reparented to the
   worker's PID 1 (`spider-execution-manager`) and keeps its socket. It keeps streaming after
   the client resumes and then stays a zombie, because PID 1 does not reap it. The 4 zombies
   present when follow-up B started sampling are the 4 `clp-s` from job 22. The 328,619
   duplicates are these orphans and the retries both sending the same 4 archives (4 × 82,460 ≈
   329,840).
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
* **Clock note.** On this WSL2 host, wall-clock time (the timestamps in `experiments.log` and in
  the container logs) advances about 5% faster than the monotonic clock that the harness's
  `time.sleep` and Spider's timeouts use. Stalls of 40, 125, 200 and 270 s (monotonic) span 43,
  131, 210 and 284 s between the harness's `stalling`/`resuming` log lines. The 80 s hard
  timeout shows up 84.37 to 84.46 s after the task start in the log timestamps. All the
  `*_secs` values in the JSON files are monotonic, as are the stall and timeout lengths, so the
  stall-versus-timeout comparisons are consistent. Only log-timestamp differences carry the ×1.05.

## 8. Follow-ups (section 3.8 of the handoff, plus two longer stalls)

Each follow-up is in its own directory under `results/`, with its own `RESULTS.md`. They ran
against the same package and the same 8 archives as this run. Stall lengths and times are on the
monotonic clock.

| Directory | Varied | Job | Hard timeouts (tasks × count) | Final status | `end` | Duplicates dropped | Identical |
|---|---|---|---|---|---|---|---|
| `stall-control-40s` | stall 40 s | 23 | none | SUCCEEDED at 43.5 s | Succeeded | 0 | yes |
| `stall-200s` | stall 200 s; `clp-s` sampled | 24 | 8 tasks × 1 | SUCCEEDED | Succeeded | 659,677 | yes |
| `stall-270s` | stall 270 s; `clp-s` sampled | 25 | tasks 1-4 × 2, tasks 5-7 × 1 | **FAILED at 160.9 s**: `hard timeout (80000 ms) exceeded` | **Failed**, 659,677 emitted | 328,083 | yes (sic) |
| `fault-4-kills` | `--fault-kills 4 --fault-stall-secs 5` | 26 | SIGKILLs, not timeouts | **FAILED at 16.9 s** | Failed, 167,875 emitted | 129,865 | no, 491,802 missing |
| `stall-retry3-125s` | `query_task_max_retry: 3`, stall 125 s | 27 | 4 tasks × 1 | SUCCEEDED at 127.7 s | Succeeded | 327,527 | yes |
| `stall-retry3-270s` | `query_task_max_retry: 3`, stall 270 s | 28 | tasks 0-3 × 2, tasks 4-7 × 1 | SUCCEEDED at 273.7 s | Succeeded | 987,204 | yes |

What they add to section 6:

* **A stalled client does fail the job, through the hard timeout alone.** With
  `query_task_max_retry: 1`, the 270 s stall failed job 25 after 160.9 s, which is 2 × 80 s. In
  that run one task (0) finished from the buffers at once, which left a free slot. The retry of
  task 1 then started the moment the first wave timed out, and timed out again 80 s later. In
  the 200 s run no task finished early. All 8 first attempts timed out, in two waves (80 s and
  160 s). Every retry started a full wave later, so the earliest a second timeout could come was
  240 s, after that client resumed at 200 s. The stall that fails a job is therefore about
  (1 + waves before the retry starts) × the hard timeout. Here that was 160 s or 240 s,
  depending on whether a single task's results fit into the buffers.
* **The job is FAILED but the client receives everything.** In job 25 the client, resuming at
  270 s, still received all 659,677 results, identical to the reference, followed by
  `end: {"status": "Failed"}`. The orphaned `clp-s` processes delivered them, after the job was
  already over. Spider kept running the 3 queued retries of the failed job until their own hard
  timeout (`Failed to report task outcome to storage. Dropping the report. err=CacheStale("cache
  stale: job no longer running")` × 3).
* **Orphans and zombies accumulate.** Peak `clp-s` processes in the 4 worker containers: 8
  orphans + 4 live children in `stall-200s`, 11 + 4 in `stall-270s`, 12 + 4 in
  `stall-retry3-270s`. After each stall all of them became zombies: 4 → 12 in `stall-200s`, 12 →
  23 in `stall-270s`. A restart clears them (0 at the start of `stall-retry3-125s`). Each
  timed-out task also holds a listener connection and a socket buffer until its orphan finishes.
* **`query_task_max_retry: 3` survives the 270 s stall** (12 hard timeouts, tasks 0-3 killed
  twice) and finishes 3.7 s after the resume. The default 125 s stall needed no extra retry: 4
  timeouts, SUCCEEDED 2.6 s after the resume, as with 1 retry.
* `fault-4-kills`: 4 kills at 5 s intervals exhaust `query_task_max_retry: 1`, and the job FAILS
  at 16.9 s with `end: Failed`. That is the expected outcome, and the stream reports it instead
  of ending silently short. The Spider error the query coordinator logged contains undecoded
  bytes: `in-task failure: ��TaskError��ExecutionError�5clp-s exited on error with
  status=signal: 9 (SIGKILL)`. The harness's per-container kill counts (5 to 7) include the 23
  zombies left by the earlier stalls (`kill -9` on a zombie succeeds), so they are not task
  kills.
* `stall-200s` has no SUCCEEDED entry in its `status_timeline`: the poller stopped before its
  next sample. The DB status and the query coordinator (`Query job reached a terminal Spider
  state … outcome=Succeeded` at 01:43:39.662) both say SUCCEEDED.

Not done: a run on the mongod logs in `~/mongodb_data/mongodb` (planned: 8 × 256 MiB files as
dataset `mongodb`, query `*Slow query*`, hard timeout ≈ 188 s). The session's permission check
refused the command that set up that data and the package restart that restored
`query_task_max_retry: 1`. **The running package is still configured with
`query_task_max_retry: 3`** from the last follow-up.
