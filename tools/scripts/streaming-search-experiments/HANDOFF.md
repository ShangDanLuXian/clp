# Handoff: run the streaming search experiments on a local WSL machine

This document is for an agent (or a person) working in a local clone of this repository on a
WSL2 machine with Docker. The cloud session that wrote it cannot reach Docker, so the
package-level experiments have to run here. Everything needed is on the branch
`query-coordinator/streaming-search`; the job is to build and start the package in Spider mode,
run `experiments.py all`, write up what happened, and push the results directory.

## 1. Context in five lines

* Streaming search sends `clp-s` search results from Spider tasks straight to a listener
  (the API server's `POST /query/stream`, or the `sbin/streaming-search.sh` CLI) instead of
  the results cache, deduplicating across Spider retries with a per-task atomic cursor.
* The design, the review's issues, and the analysis of the main suspected flaw are in
  `docs/src/dev-docs/design-streaming-search/` (`issues.md`, `backpressure.md`).
* The suspected flaw: a slow client makes `clp-s` block in `send()`, that blocked time counts
  against Spider's per-task hard timeout (60 s + 0.5 s per MiB of the archive), Spider kills
  the task, retries once, kills again, and the job FAILs for no reason. Nobody has tested a
  stall longer than 60 s yet. The `stall` phase of the harness does exactly that.
* The prototype's own reports (CLI benchmark, API latency, fault injection) were reproduced
  here only as far as the unit/integration tests go; see
  `docs/src/dev-docs/design-streaming-search/reproduction.md`.
* The harness, `tools/scripts/streaming-search-experiments/experiments.py`, was smoke-tested
  against a fake API server, never against a real package. Expect small breakages and fix
  them in place.

## 2. Ground rules

* Work on branch `query-coordinator/streaming-search`. Commit as `Xuhui Chen`
  (`git -c user.name="Xuhui Chen" -c user.email="<the owner's email>" commit ...`, or set
  the repo config once). Never force-push, never rebase, do not open a pull request.
* Commit only measurements: the results directory (JSON, `*.logs.*.txt`, `RESULTS.md`) and
  any fix to the harness. Never commit `results/*/logs/` (git-ignored already),
  `build/`, `etc/credentials.yaml`, or anything with a password in it. `collect` masks
  passwords in the config copy it saves; check the diff before committing anyway.
* If the harness needs a change, make the smallest change that unblocks you, commit it
  separately with a message saying what was wrong, and keep running.
* Do not change the experiment semantics silently (stall length, kill count, dataset size,
  `query_task_max_retry`). If you must, say so in `RESULTS.md`.
* **Do not run `task package` or `task clean` after the data is compressed.** `task package`
  starts with `rm -rf build/clp-package`, which deletes the archives and the database. If the
  package must be rebuilt, stop CLP first and expect to compress again.
* If you are stuck for more than about an hour, commit what you have plus a `STATUS.md` in
  the results directory describing where you are stuck (commands, errors, what you tried),
  push, and stop.

## 3. Runbook

Verify every step before moving on. Estimated wall time: 1-2 h for the build the first time,
20-30 min for the experiments.

### 3.1 Prerequisites

```shell
python3 --version        # >= 3.10
docker info >/dev/null && echo docker-ok
task --version           # >= 3.48
uv --version             # >= 0.8
```

If `task` or `uv` is missing, follow `docs/src/dev-docs/building-package.md`. Then, from the
repository root:

```shell
git fetch origin
git checkout query-coordinator/streaming-search
git pull --ff-only
tools/scripts/deps-download/init.sh
components/core/tools/scripts/lib_install/ubuntu-jammy/install-all.sh   # needs sudo; works on 24.04 too
```

### 3.2 Build

```shell
task package                       # -> build/clp-package, and the clp-package:dev-* image
task docker-images:spider-worker   # -> clp-spider-worker:dev-<user>-<id>
docker images clp-spider-worker    # note the newest tag
docker tag clp-spider-worker:dev-<user>-<id> ghcr.io/y-scope/clp/clp-spider-worker:main
```

Check: `ls build/clp-package/bin/clp-s build/clp-package/sbin/streaming-search.sh` and
`cat build/clp-package/clp-package-image.id` all succeed. The tag step matters: the Spider
compose file defaults to the `ghcr.io/...:main` reference and would pull a worker image that
lacks this branch's `clp-s`.

### 3.3 Configure Spider mode

Edit `build/clp-package/etc/clp-config.yaml` and add the block from `README.md` step 3
(`package.scheduler: "spider"`, `api_server.search_result_listener`, `spider` with the worker
pools, `compression_coordinator`, `query_coordinator` with `query_task_max_retry: 1`). The
three Spider sections must not be `null`. Keep `logs_input` at its default (`fs`, `/`).

### 3.4 Start

```shell
build/clp-package/sbin/start-clp.sh
docker compose ls                                          # clp-package-<id> running
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:3001/health   # 200
docker compose -p "$(docker compose ls --format json | python3 -c 'import json,sys;print([p["Name"] for p in json.load(sys.stdin) if p["Name"].startswith("clp-package")][0])')" ps
```

The `ps` output must show `query-coordinator`, `api-server`, `spider-scheduler`,
`spider-storage`, `spider-worker` (several replicas) and `spider-worker-compression` up.
If `start-clp.sh` fails, read its message: the usual causes are a `null` Spider section, a
missing worker image (the tag step), or a port already in use.

### 3.5 Run the experiments

```shell
python3 tools/scripts/streaming-search-experiments/experiments.py all 2>&1 | tee /tmp/experiments-console.txt
```

The driver prints each phase and writes to
`tools/scripts/streaming-search-experiments/results/<UTC timestamp>/`. `summary.json` is
updated after every phase. The `stall` phase sits silent for a few minutes by design.

If a phase fails, read `experiments.log` in the results directory, fix the cause, and re-run
from that phase into the same directory, for example:

```shell
python3 tools/scripts/streaming-search-experiments/experiments.py fault stall collect \
    --results-dir tools/scripts/streaming-search-experiments/results/<timestamp>
```

`gen-logs` and `compress` do not need to be repeated once `preflight.json` lists archives
(`--skip-compress-if-archives` for `all`).

### 3.6 Write RESULTS.md

Create `RESULTS.md` in the results directory with these sections, filled from the JSON files
and the `*.logs.interesting.txt` files. Short and factual; numbers over adjectives.

1. **Environment**: CPU count, RAM, WSL kernel, Docker version, `git rev-parse HEAD`, the
   worker image tag, the config block used, dataset size and the archives (count, sizes,
   the derived soft/hard timeouts from `preflight.json`).
2. **Correctness**: for each run, `identical_multiset`, counts, the `end` event, the final DB
   status. Any difference: the `missing_sample`/`extra_sample`.
3. **Latency**: the medians from `latency.json` (stream vs results cache vs CLI), plus the
   full per-run numbers.
4. **Disconnect**: seconds to CANCELLING per run, the final status, how long the job took to
   terminate after the cancel.
5. **Fault**: how many `clp-s` were killed per container and when, the `end` statistics
   (`num_duplicates_dropped` should be > 0 if the kills hit tasks mid-stream, and
   `num_protocol_errors`), `identical_multiset`, the status timeline, and the lines from
   `fault.logs.interesting.txt` that show Spider retrying.
6. **Stall** (the important one): the stall length vs the hard timeout, the status timeline
   (did the job go RUNNING -> FAILED, or stay RUNNING and SUCCEED after the resume?), the
   `end`/`error` event, whether the stream closed cleanly, `identical_multiset` after the
   resume, and the log lines from `stall.logs.interesting.txt` that show soft-timeout
   re-dispatch, hard-timeout kills, retries, `UnknownSession`, or protocol errors. Quote the
   lines with their timestamps.
7. **Anything unexpected**: harness fixes you had to make, phases you re-ran, warnings in the
   service logs, slot starvation you noticed in `docker stats`, and so on.

### 3.7 Push

```shell
git add tools/scripts/streaming-search-experiments/results tools/scripts/streaming-search-experiments/*.py
git status            # no logs/, no credentials
git commit -m "experiments(streaming-search): Add results from <machine> on <date>."
git push origin query-coordinator/streaming-search
```

### 3.8 Optional follow-ups, only if everything above is done

Each goes into its own results directory (`--results-dir .../results/<name>`), with a line in
its `RESULTS.md` saying what was varied.

* `stall` with a stall shorter than the smallest hard timeout (`--stall-secs 40`): the job
  should SUCCEED and the stream should be identical. This is the control.
* `stall` with `query_task_max_retry: 3` in `query_coordinator` (edit the config, restart
  with `stop-clp.sh`/`start-clp.sh`): does the job survive with more retries, and how long
  does it take?
* `fault` with `--fault-kills 4 --fault-stall-secs 5`.

## 4. Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `task package` fails in the C++ build | Missing core dependencies; re-run the `lib_install` script, then `task deps:core` and look at the first error. |
| `start-clp.sh`: validation error about `spider`/`query_coordinator` | A section is still `null` or mis-indented. Compare with `README.md` step 3. |
| `start-clp.sh` pulls `ghcr.io/y-scope/clp/clp-spider-worker:main` or fails to | The `docker tag` step was skipped or tagged the wrong image. |
| `preflight` finds no compose project | The package is not running, or `docker compose ls` shows it under a different name; pass `--project <name>`. |
| `GET /health` is not 200 | The API server is still starting (wait 30 s) or the port is not 3001 (`--api-port`). |
| `POST /query/stream` returns 501 | `package.scheduler` is not `spider` in the config the package was started with. |
| `POST /query/stream` returns 400 | Request shape changed; see the `error_body` in `experiments.log` and adjust `stream_query_body`. |
| `compress.sh` fails | Look at `experiments.log` `stderr_tail`; typical causes are the `logs_input` directory not covering the logs path, or the dataset name. |
| `clp-s binary not found` | `build/clp-package/bin/clp-s` is missing; the package build did not finish. |
| The DB query helper fails | `etc/credentials.yaml` was not generated (start the package once), or the DB container's client is neither `mariadb` nor `mysql`; run `docker compose -p <project> exec database ls /usr/bin | grep -i -E 'mariadb|mysql'` and adjust `Ctx.db_rows`. |
| `fault` kills nothing (`killed_per_container` all 0) | The tasks finished before the kill; lower `--fault-stall-secs` or raise `--file-size-mb` so tasks are still streaming when the kill lands. |
| `stall` stream ends with `error` instead of `end` | That is a result, not a bug in the harness: record it, with the API server's log lines around that time. |
| A phase hangs | Each SSE read has a 1 h socket timeout; `Ctrl-C`, check `experiments.log`, and re-run that phase. |

## 5. Where things are

| What | Path |
|---|---|
| Harness and README | `tools/scripts/streaming-search-experiments/` |
| Design, issues, backpressure analysis, reproduction | `docs/src/dev-docs/design-streaming-search/` |
| Listener library and its tests (incl. the real-clp-s test) | `components/search-result-listener/` |
| `clp-streaming-search` CLI | `components/clp-package-cli/` |
| `POST /query/stream` | `components/api-server/src/streaming_search.rs`, `routes.rs` |
| `clp-s` network output handler (protocol v1) | `components/core/src/clp_s/OutputHandlerImpl.cpp` |
| Spider task timeouts | `components/query-coordinator/src/job_handle.rs`, `compute_query_task_execution_policy` |
| Package config template (Spider sections) | `components/package-template/src/etc/clp-config.template.json.yaml` |
