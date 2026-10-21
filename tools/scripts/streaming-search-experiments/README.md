# Streaming search experiments

A driver that runs the streaming search experiments against a CLP package deployed in Spider
mode and collects everything needed to analyse them into a results directory. It needs only
Python 3.10+, Docker, and the started package. See
`docs/src/dev-docs/design-streaming-search/` for the design, the issues under review, and why
each experiment exists.

## What it runs

| Phase         | What it does                                                                            | What to look at |
|---------------|-----------------------------------------------------------------------------------------|-----------------|
| `preflight`   | Finds the compose project, reads the DB credentials, checks `GET /health`, lists the archives and the soft/hard timeouts Spider derives from their sizes. | `preflight.json` |
| `gen-logs`    | Writes synthetic JSONL logs (default 8 files x 40 MB; 30% of records match `*Transmitted*`). | `gen-logs.json` |
| `compress`    | Runs `sbin/compress.sh` once per file, so each file becomes its own archive.           | `compress.json` |
| `correctness` | Runs `POST /query/stream` N times and the results-cache path once, and compares every result set with `clp-s s` on the archives (byte-identical multisets). | `correctness.json` |
| `latency`     | Time to first result: `POST /query/stream` vs `POST /query` + `GET /query_results` vs `sbin/streaming-search.sh --raw`. | `latency.json` |
| `disconnect`  | Reads one result, closes the socket, and times how long the job takes to become CANCELLING. | `disconnect.json`, `disconnect.logs.*` |
| `fault`       | Reads one result, stalls, SIGKILLs every `clp-s` in every worker container (twice), then reads to the end. Checks nothing is lost or duplicated and reads the `end` statistics. | `fault.json`, `fault.logs.*` |
| `stall`       | Reads one result, then stops reading for longer than the largest archive's hard timeout (default: hard timeout + 45 s), then resumes. Records the DB status timeline, the `end`/`error` event, and the services' logs. This is the experiment for issue 1 (backpressure vs. Spider's timeouts). | `stall.json`, `stall.logs.interesting.txt` |
| `collect`     | Versions, masked config, archives, the last 40 query jobs, and 24 h of service logs.   | `collect.json`, `collect.logs.txt` |

`all` runs them in that order. Expect about 15-25 minutes with the default dataset.

## One-time setup (WSL2, Docker running)

1. Prerequisites, as in `docs/src/dev-docs/building-package.md`: `task`, `uv`, Python 3.10+,
   and CLP core's build dependencies:

   ```shell
   tools/scripts/deps-download/init.sh
   components/core/tools/scripts/lib_install/ubuntu-jammy/install-all.sh
   ```

2. Check out this branch and build the package and the Spider worker image:

   ```shell
   git checkout query-coordinator/streaming-search
   task package                       # build/clp-package + the clp-package image
   task docker-images:spider-worker   # clp-spider-worker:dev-<user>-<id>
   ```

   The package's compose files reference `ghcr.io/y-scope/clp/clp-spider-worker:main`, and the
   vendored Spider compose file sets `pull_policy: ${SPIDER_PULL_POLICY:-always}`, so a plain
   start pulls the upstream worker image (without this branch's `clp-s`) even when a local image
   carries that tag. Tag the image just built and, after `start-clp.sh` (step 4), recreate the
   workers with the pull policy overridden:

   ```shell
   docker tag "$(docker images clp-spider-worker --format '{{.Repository}}:{{.Tag}}' | head -1)" \
       ghcr.io/y-scope/clp/clp-spider-worker:main
   ```

3. Put the package in Spider mode. In `build/clp-package/etc/clp-config.yaml`, add (or
   uncomment and edit) these sections; `spider`, `compression_coordinator` and
   `query_coordinator` must all be non-null:

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
     storage:
       log_level: "INFO"
     scheduler:
       log_level: "INFO"
       policy: "resource_group_round_robin"
     worker:
       log_level: "INFO"
       pools:
         general:
           replicas: 4      # the query tasks run here; one task per replica at a time
         compression:
           replicas: 1

   compression_coordinator:
     logging_level: "INFO"

   query_coordinator:
     logging_level: "INFO"
     query_task_max_retry: 1
   ```

   `logs_input` can stay at its default (`type: fs`, `directory: /`), which is what
   `sbin/compress.sh` uses. Compression in Spider mode still runs on the Celery compression
   workers, which the Spider compose file includes. The `compression-coordinator` service only
   supports S3 input and exits with `fs`, which also keeps `spider-worker-compression` from
   registering; neither is needed for these experiments, but it makes `start-clp.sh`'s
   `docker compose up --wait` report a failure (see step 4). If the host already runs a
   database on 3306, set `database.port` to another port in the same config.

4. Start the package and check it is up:

   ```shell
   build/clp-package/sbin/start-clp.sh    # may end with a --wait failure from compression-coordinator; that is expected with fs input
   docker compose ls                 # a running clp-package-* project
   PROJECT=$(docker compose ls --format json | python3 -c 'import json,sys;print([p["Name"] for p in json.load(sys.stdin) if p["Name"].startswith("clp-package")][0])')
   (cd build/clp-package && SPIDER_PULL_POLICY=missing docker compose --project-name "$PROJECT" --file docker-compose-spider.yaml up --detach)
   docker compose -p "$PROJECT" ps   # query-coordinator, api-server, spider-scheduler, spider-storage, spider-worker x N up
   curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:3001/health   # 200
   ```

   The second `up` reuses the `.env` that `start-clp.sh` generated and recreates the worker
   containers on the local image instead of the pulled one. Check with
   `docker inspect --format '{{.Image}}' <a spider-worker container>` that the image ID is the
   one built in step 2. The credentials file `etc/credentials.yaml` is generated on the first
   start.

## Run

From the repository root:

```shell
python3 tools/scripts/streaming-search-experiments/experiments.py all
```

Results go to `tools/scripts/streaming-search-experiments/results/<UTC timestamp>/`. The
generated logs land in `<results>/logs/` and are git-ignored.

Useful options:

* `--logs-dir DIR --timestamp-key KEY`: compress your own JSON logs instead of generating
  (`--timestamp-key ""` to omit the key). One compression job per file.
* `--runs N`: repetitions for `correctness`, `latency` and `disconnect` (default 3).
* `--stall-secs S`: the stall length (default: largest archive's hard timeout + 45 s).
* `--skip-compress-if-archives`: with `all`, skip `gen-logs`/`compress` when the package
  already has archives (for a second run against the same data).
* `--query`, `--dataset`, `--api-port`, `--package-dir`, `--project`: defaults are
  `*Transmitted*`, `default`, `3001`, `build/clp-package`, discovered.

If a phase fails, fix the cause and re-run only the phases you need into the same directory:

```shell
python3 tools/scripts/streaming-search-experiments/experiments.py stall collect \
    --results-dir tools/scripts/streaming-search-experiments/results/<timestamp>
```

`experiments.log` in the results directory has every command the driver ran and its errors.

## Push the results

```shell
git add tools/scripts/streaming-search-experiments/results
git commit -m "experiments(streaming-search): Add results from <host> on <date>."
git push
```

The results directory is a few MB (the service logs dominate). `summary.json` has the
highlights of every phase; the per-phase JSON files have the details.

## Stop

```shell
build/clp-package/sbin/stop-clp.sh
```

## Notes

* The `stall` phase deliberately pushes every search task past its hard timeout. Spider is
  expected to kill the tasks and retry them once (`query_task_max_retry: 1`); the question the
  experiment answers is what the job's final status is and what the `end` event says. Both
  outcomes are informative, so do not treat a FAILED job here as a broken setup.
* `fault` kills `clp-s` with SIGKILL inside the worker containers through `/proc`, so it does
  not need `procps` in the image.
* All HTTP goes to `127.0.0.1:3001` on the host; the DB is queried through
  `docker compose exec database`, so nothing has to be installed on the host for that.
