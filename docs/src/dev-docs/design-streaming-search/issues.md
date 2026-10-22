# Issues

Every issue found while reviewing the streaming search design and the prototype, ranked by
severity. Each entry gives the problem, the evidence (file references are to the prototype code
on this branch), and a proposed direction. The core compare-and-swap deduplication is sound; the
issues are in the assumptions around it and in the cases it does not cover.

Severity:

* **P0**: can produce wrong results, or fail a job for no good reason.
* **P1**: an architectural trade-off that should be decided before the design is final.
* **P2**: details.

## Status after the 2026-10-20/21 design revision and the Rust-reader report

The design doc was revised twice after this review started, and a third report
("rust-search-integration-and-scaling-report", branch `query-coordinator/streaming-search-rust-reader`
at `e10364cc`, **not pushed**; all of its changes were uncommitted) added three pieces of work:
protocol v2 (a per-task trailer and a listener acknowledgement before a task may succeed),
encoding moved into the listener's connection tasks (the "funnel fix"), and V3, a Rust
reimplementation of the clp-s reader running inside the Spider task executor instead of spawning
`clp-s`. None of that code is on this branch yet; the notes below are from the documents.

| Issue | Status after the revision |
|---|---|
| 2 (no acknowledgement, silent loss) | **Closed by protocol v2.** The report found the same hole independently: a client throttled to 40 MB/s plus two killed workers lost 8 to 15,764 results per run with `end: Succeeded`, because the tail was still in the dead worker's kernel send buffer after the task had reported success. v2 makes the task wait for the listener's acknowledgement of `0..N-1` plus a trailer count. |
| 10 (no per-task end frame) | **Closed by the v2 trailer.** A SUCCEEDED job now implies every task's full output reached the session. Incomplete connections are counted (`num_incomplete_connections`). |
| 20 (orphaned `clp-s`) | **Gone under V3, unchanged under V2.** V3 has no subprocess, so the executor's SIGKILL on a hard timeout kills the reader too. V2 (C++ `clp-s`) is still the default engine (`CLP_SPIDER_SEARCH_ENGINE` unset), and compression tasks still spawn subprocesses. |
| 1 and 4 (backpressure vs. hard timeout, slot starvation) | **Not addressed, and sharpened by v2**: see issue 21. |
| 3 (determinism precondition) | **A concrete trigger appeared with V3**: see issue 22. |
| 6 (status polling) | Unchanged; the report lists the 100 ms poll as the end-of-stream floor (about 50 ms). |
| 5, 7, 9, 11, 12 | Unchanged. |
| 9 (extensibility) | v2 adds a 1-element trailer told apart by array length and a fixed 10-byte acknowledgement; still positional. |

The report's own open issues that this review had not listed: memory under backpressure is
O(blocked connections) and 2.2-2.5x the old build after the funnel fix (its acceptance criterion
failed, but v2 bounds the blocked connections by the worker slots, see issue 21); the listener's
accept backlog is mio's 128 and bursts of task connections overflow it (about 1 s of SYN
retransmits); Spider's dispatch rate (3.8 ms per task at 16 workers) limits tiny-task queries;
the results-cache path cannot request unlimited results, polls with a backoff of up to 5 s, reads
back on one stream at about 400k docs/s, and inserts synchronously; all scaling numbers are capped
by the WSL2 host's per-core slowdown at 16 busy threads and need multi-node validation.

## P0

### 1. Backpressure conflicts with Spider's task timeouts

The design lets a slow consumer block connection tasks on the bounded channel, which stops them
reading their sockets, which makes TCP block `clp-s` in `send()`. Spider, however, bounds a task's
wall-clock time by the archive's size (`query-coordinator/src/job_handle.rs`,
`compute_query_task_execution_policy`): soft timeout 10 s + 20 ms/MiB, after which Spider
re-dispatches the task; hard timeout 60 s + 500 ms/MiB, after which Spider kills it;
`query_task_max_retry` defaults to 1.

With backpressure, a task's wall-clock time is no longer a function of the archive but of the
consumer's speed and the task's output size, shared across every concurrent task of the session. A
small archive with many matches and a slow client exceeds its hard timeout, is killed, is retried
under the same conditions, is killed again, and the job is FAILED although nothing is wrong with
the data or the cluster. The results-cache path has no such coupling, so this is a failure mode
that streaming introduces. See [backpressure](backpressure.md) for the full analysis and the options.

**Confirmed by measurement (2026-10-21, WSL2, 8 archives of 40 MiB, hard timeout 80 s, 4 worker
slots; `tools/scripts/streaming-search-experiments/results/`).** A client that reads one result
and then stops reading:

| Stall  | `query_task_max_retry` | Hard timeouts | Job                          | Results after the resume |
|--------|------------------------|---------------|------------------------------|--------------------------|
| 40 s   | 1                      | 0             | SUCCEEDED                    | identical, 0 duplicates  |
| 125 s  | 1                      | 4             | SUCCEEDED                    | identical, 328,619 duplicates dropped |
| 200 s  | 1                      | 8             | SUCCEEDED                    | identical, 659,677 duplicates dropped |
| 270 s  | 1                      | 11            | **FAILED at 160.9 s** ("hard timeout (80000 ms) exceeded") | identical, `end: Failed` |
| 270 s  | 3                      | 12            | SUCCEEDED                    | identical, 987,204 duplicates dropped |

Every task that was streaming when the client stalled hit the hard timeout 80 s after it
started; nothing was wrong with any of them. The job fails once a task's retry also times out,
which is about `(1 + waves of unstarted tasks ahead of the retry) × hard timeout` into the job:
160 s here when a slot was free, 240 s when none was. The API report's refutation was wrong
because no test had stalled longer than the smallest hard timeout. See
[backpressure](backpressure.md) for the timelines and for a second finding from the same runs:
Spider's hard timeout frees the slot but does not kill `clp-s` (issue 20).

**Direction.** Cut the backpressure chain before it reaches the worker: a bounded buffer at the
listener, after which the job is cancelled and the client told why (option A in the backpressure
analysis). This requires real job cancellation (issue 5).

### 2. The protocol is one-directional, so "task succeeded but results were lost" is possible

`clp-s`'s `send()` returns once the bytes are in the kernel's send buffer; it then exits 0. If the
listener subsequently closes the connection (rejected handshake, index gap, unknown session), the
results are lost but the task has succeeded, Spider does not retry it, the job is SUCCEEDED, and
the CLI exits 0. The prototype report (§2.5) acknowledges this and notes it happens only on a bug or
once the job is already over. The CLI logs `num_protocol_errors > 0` at debug level only and still
exits 0, which hides the one signal of this case.

**Direction.** Make the protocol bidirectional in v1 rather than later: the listener answers the
handshake with `[next_result_index]`, and `clp-s` waits for a final acknowledgement before exiting
0. One change closes three gaps at once: silent loss, re-sending the whole prefix on retry (the
"resume from the cursor" future work), and telling `clp-s` to stop. Changing the wire protocol
later requires a coordinated `clp-s` and listener upgrade, which is the rolling-upgrade problem the
design doc itself raises.

### 3. Deduplication rests on `clp-s` determinism, and the protocol discards the idempotency key that exists

The results-cache path deduplicates by `_id = {archive_id, log_event_idx}`
(`core/src/clp_s/OutputHandlerImpl.cpp`, `ResultsCacheOutputHandler::finish`). That key does not
depend on determinism. The network handler explicitly ignores it
(`NetworkOutputHandler::write`, `[[maybe_unused]] int64_t log_event_idx`).

Determinism is a property of another component that is not part of any contract and has no test.
A future `clp-s` change (parallel search, a different schema traversal order, a per-archive top-K
with unstable tie-breaking, a rolling upgrade running two versions in one job) turns exactly-once
into "one duplicate and one loss" silently: no error is raised anywhere.

**Direction**, preferably (a) and (c) together:

* (a) Add `log_event_idx` to the result frame. The listener records the `log_event_idx` of every
  emitted result per task (a `Vec<i64>`; about 800 KB for 100,000 results). When a duplicate is
  dropped, its `log_event_idx` must match the recorded one; a mismatch is a determinism violation
  and fails the job loudly. The hot path stays one CAS.
* (b) Deduplicate by `(archive_id, log_event_idx)` and drop the determinism precondition entirely.
  Costs a per-task set (a roaring bitmap is cheap) and turns the hot path into a set insert.
* (c) Add a contract test to `clp-s`: searching one archive twice produces byte-identical output.

Carrying `log_event_idx` also makes it possible to attach the web UI's "view in context" feature to
streamed results later.

### 4. One client that stops reading holds the whole cluster

The API report measured it: "one stalled stream held all 8 worker slots, so another job waited".
This is the other face of issue 1. Once backpressure reaches the workers, the scheduler's capacity
is handed to the slowest client. The API server is multi-tenant, so this will happen.

*Measured 2026-10-21:* during the 125 s stall all 4 general-pool slots were held by blocked tasks
for the whole stall; after the hard timeout the slots went to the retries of the same job, which
blocked in turn. Because the timed-out `clp-s` processes are not killed (issue 20), the worker's
CPU and memory stay with the orphans as well.

**Direction.** Per-session quotas: a memory cap and a no-progress timeout, after which the job is
cancelled and the client receives `error`.

### 5. Cancellation does not exist, but the design depends on it

* Ctrl-C on the CLI exits the CLI; the job keeps running.
* An API client disconnect marks the job CANCELLING, but the coordinator "runs the job to
  completion" and then marks it CANCELLED. No work is stopped.
* `max_num_results` is ignored for network output (API report trap #2): the TDL never passes it for
  a network output, and a per-archive limit is not a global one. A broad query streams everything
  and nothing can stop it.
* Coordinator gaps recorded by the API report: a disconnect before dispatch leaves the job
  CANCELLING forever with a session that polls the DB every 100 ms (#16); a cancel in the ~17 ms
  window between the coordinator's fetch of a PENDING job and its PENDING→RUNNING transition leaves
  the Spider job in `Ready` forever (#17).

*Measured 2026-10-21:* after the client closed its socket, the job was CANCELLING within
0.12-0.14 s and CANCELLED 0.3 s later, but every task ran to the end; the API server discarded
659,319 of the 659,677 results (`num_results_discarded`). Cancelling stopped no work.

**Direction.** Promote "the coordinator actually cancels the Spider job" from future work to a
prerequisite. Implement a global `max_num_results` in the listener: cancel the job once N results
have been emitted.

### 6. The status source gives up on the first failure, and per-session polling does not scale

* One failed poll ends the session: the CLI exits with an error and the API server cancels the job,
  although the job is fine. The API report leaves this as a recorded decision; it should be
  reversed. Retry with backoff and give up only after a threshold of consecutive failures.
* Every session polls the DB every 100 ms. With 100 concurrent streams that is 1,000 queries per
  second for status alone. The API report already observed all 5 pool connections busy with no
  requests in flight.

**Direction.** One shared poller per listener (`SELECT id, status FROM query_jobs WHERE id IN
(...)`), or better, have the coordinator notify terminal status (it already knows).

### 20. Spider's hard timeout frees the slot but does not kill `clp-s`

Found by the stall experiments (`results/*/stall.clp-s-processes.txt`, sampled every 2 s inside
the worker containers). When a task hits the hard timeout, the worker logs `Executor time out
triggered` and `Task hit the hard timeout`, respawns the executor, and reports the task instance
as failed. The `clp-s` child is not killed: it is reparented to the container's PID 1
(`spider-execution-manager`), keeps its socket to the listener, keeps streaming once the client
resumes, and then stays a zombie because PID 1 never reaps it. No `clp-s exited on failure` and
no `Connection ended abnormally` was logged for any hard timeout, whereas every SIGKILL in the
fault runs produced both. Peak counts: 8 orphans + 4 live children after a 200 s stall, 12 + 4
after a 270 s stall; zombies grew 4 → 12 → 23 across runs and were cleared only by a restart.

Consequences:

* **Every hard timeout creates the "stale attempt overlapping its retry" case** that the design
  treats as a rare race and that the prototype could never reproduce. It is the normal outcome.
  The cursor handled it correctly in every run (identical result sets, 0 protocol errors), but
  the orphan and the retry both stream the whole archive, so the duplicates dropped equal one
  archive's results per timeout (328,619 ≈ 4 × 82,000; 987,204 ≈ 12 × 82,000).
* **A FAILED job still delivers all its results.** In the 270 s run the job failed at 160.9 s,
  yet the client, resuming at 270 s, received all 659,677 results from the orphans, followed by
  `end: {"status": "Failed"}`. The job status and the stream disagree about what happened.
* **Resources leak.** Each orphan holds a worker process, its memory (the 113 MiB buffer the API
  report measured), a listener connection, and socket buffers until the client drains it;
  zombies accumulate until the container restarts. Spider also keeps dispatching the queued
  retries of an already-failed job until they time out (`Failed to report task outcome to
  storage. Dropping the report. err=CacheStale("cache stale: job no longer running")`).
* The listener's drain grace is what eventually cuts an orphan off after the job ends, so the
  design's "stale attempt" handling is load-bearing, not defensive.

**Where it comes from (verified in the sources, not in the prototype).** Three pre-existing
pieces combine:

* Spider's execution manager (`components/spider-execution-manager/src/process_pool.rs` at
  `278d06f`) spawns each executor process with `tokio::process::Command` and
  `kill_on_drop(true)`, in the manager's own process group. On a hard timeout it drops the
  handle, which SIGKILLs the executor process only (`Outcome::Timeout`: "The pool has
  `SIGKILL`-ed the process"). Nothing kills the executor's children. Spider's docs say the task
  "will be terminated"; the implementation terminates the executor. Upstream Spider `main`
  (`6e85c90`, 2026-10) has the same code.
* `clp-tdl-package`'s `run_clp_s_search` (`src/task/query/search.rs`, from #2512 on `main`,
  before the prototype) spawns `clp-s` with `std::process::Command::spawn()` and `wait()`: no
  process group, no `PR_SET_PDEATHSIG`, no cleanup. The prototype only changed the arguments.
  The compression tasks spawn `clp-s`, the log converter and the indexer the same way
  (`src/task/compression/compress.rs`), so a compression hard timeout orphans them too.
* The worker container runs `spider-execution-manager` as PID 1 (no `tini`, no `init: true`),
  and the manager never `waitpid`s children it did not spawn, so the orphans become zombies.

Nobody saw it before because with results-cache output an orphaned `clp-s` just finishes its
MongoDB upserts (deduplicated by `_id`) and exits; only streaming gives the orphan a connection
to keep, and no earlier test stalled past the hard timeout.

**Correctness impact.** None on the result data itself: the cursor's argument (deterministic
output, TCP order within a connection, one atomic swap per index) does not care whether the
competing attempt is a retry or an orphan, and the measurements agree (5 stall runs, up to
987,204 duplicates dropped, 0 protocol errors, every result set byte-identical to `clp-s`). An
orphan cannot reach another job either: tokens are random per session, and once a session is
unregistered a late orphan's handshake is rejected and its next `send()` fails.

What breaks is the **meaning of the job status**:

* A FAILED job can deliver a complete result set (observed: job 25 failed at 160.9 s, the client
  received all 659,677 results, then `end: Failed`).
* A SUCCEEDED job can, in a narrow case, deliver an incomplete one. During the drain, a
  connection is cut once it has spent more than the grace period (5 s) reading from its socket
  without claiming a result (`connection.rs`, `read_time_since_last_claim`). With an orphan that
  ran ahead, the succeeded attempt's connection reads only duplicates, so it is the one that
  accumulates read time; if it is cut before it reaches the unclaimed tail, and the orphan is
  cut too (idle for more than 5 s while scanning a sparse region) or already gone, the tail is
  lost and nothing reports it. It needs a slow worker-to-listener network on top of the orphan,
  so it was not observed, but the listener could not detect it if it happened.
* Aggregation would be wrong: a `reducer` output has no `_id` deduplication, so an orphan and its
  retry would both add their counts. Not reachable today, because the coordinator rejects
  aggregation jobs on Spider, but it must be settled before aggregation moves there.

Both status problems have the same root: the listener never learns how many results a task
produced (no per-task end frame, no acknowledgement; issues 2 and 10), so it cannot say
"complete" independently of Spider's verdict. The `end` event should carry a completeness flag
computed from per-task totals, and the CLI's exit code should follow it rather than the job
status.

**Fix.** In Spider: spawn executors in their own process group (`Command::process_group`) and
kill the group on timeout or crash, and reap reparented children (or run the worker under an
init). Defensively in `clp-tdl-package`, independent of Spider: `pre_exec` with
`prctl(PR_SET_PDEATHSIG, SIGKILL)` on every spawned `clp-s`, so it dies with its executor. Until
one of these lands, the streaming design must assume that a timed-out attempt keeps running.
Neither fix changes issue 1: the hard timeout still kills a task that is merely blocked.

Also noted in the same runs: no soft-timeout re-dispatch was observed in any stall (no log line
mentions the soft timeout, and no task had two live instances before its hard timeout), although
the soft timeout (10.8 s) was exceeded by every stalled task. Whether Spider's
`resource_group_round_robin` policy implements soft-timeout re-dispatch at this revision should be
checked before the design relies on it either way.

### 21. Protocol v2 makes a slow client fail the job sooner and more surely

Under v2 a task succeeds only after the listener acknowledges that its results have been handed
to the session's channel, so a task holds its worker slot until the consumer has drained what it
sent. The revised design doc states this as a memory win ("the job now lasts as long as the
stream"; API-server peak memory down 2.4-5x, open listener connections from 64-282 down to about
10) and relies on "Spider's task timeouts bound everything else". Those timeouts are what fail
the job:

* Under v1 a fast task could finish into the kernel buffers and succeed while the client was
  stalled (the measured 125 s and 200 s stalls succeeded only because of that and of the queued
  retries). Under v2 no task with results can finish until the client reads them, so every first
  attempt times out together, every retry times out one hard timeout later, and with
  `query_task_max_retry: 1` the job fails whenever the client stalls for more than about twice the
  hard timeout.
* A client that is merely slow fails the job too. A task's wall time becomes the results queued
  ahead of it divided by the client's rate, shared across the slots. With 4 slots holding about
  15 MB of results each and an 80 s hard timeout, a client slower than roughly 1 MB/s cannot
  acknowledge a wave in time. That is an ordinary rate for a client on a mobile or cross-region
  link.
* The memory the funnel fix moved into blocked connections is now bounded because the pressure
  moved into worker slots instead (issue 4): one slow client holds every slot its job has.

The stall experiments in `tools/scripts/streaming-search-experiments/` ran against v1. They
should be re-run against v2 with the same harness, plus a throttled (not stalled) client
(`curl --limit-rate`), before the design's backpressure section is finalised. The expectation is
FAILED at 2 x hard timeout for any stall longer than that, independent of the queueing nuance
measured under v1.

### 22. V2 and V3 engines in one job break the determinism precondition

V3 selects the search engine per worker container through `CLP_SPIDER_SEARCH_ENGINE`. The
Rust-reader report records 3 of 225 fixture cases whose wire output differs from C++ only in float
formatting (divergence DV-01). If one attempt of a task runs on a V2 worker and its retry on a V3
worker, the same `result_index` can carry a different message, and position-based deduplication
turns that into one duplicate and one loss with no error (issue 3's failure mode, with a concrete
trigger; it is also the design doc's own "rolling upgrade" risk). The engine must be uniform
within a job (in practice, per deployment), or the listener must verify duplicates against a
per-result identity such as `log_event_idx`.

### 23. Protocol v2 breaks `sbin/search.sh` in Celery mode

Under v2 `clp-s` waits for the listener's acknowledgement before exiting 0. The Celery path's
Python CLI (`native/search.py`), which the prototype had adapted to read v1 frames, never
acknowledges, so every Celery search with the new `clp-s` fails. The revised design doc lists this
under its assumptions ("a Celery search through `sbin/search.sh` fails"). A package deployed in
Celery mode with this `clp-s` therefore loses search entirely; either the Celery CLI acknowledges
too, or the package must refuse Celery mode with a v2 `clp-s`.

## P1

### 7. The push model requires workers to reach the client; the CLI should not host a listener

The CLI relies on `docker run --network host` and auto-detecting the first non-loopback IPv4
address so that workers can connect back. This does not work on Helm/Kubernetes (the API report:
501), behind NAT, or from a laptop.

**Direction.** Make `streaming-search.sh` an SSE client of the API server's `POST /query/stream`.
This removes the Python wrapper (and open question 2 of the design doc), the DB credentials in the
CLI, the advertised-host problem, and about 825 ms of launch overhead (the reports measured
1021 ms to the first result for the CLI against 208 ms through the API). The listener library then
has one host instead of two.

### 8. The "authentication" goal is not met

* The session token is stored in plain text in `query_jobs.config`, appears on the worker's command
  line (`ps`, and `--verbose` logs the command line; the report notes it logs `CLP_DB_PASS` too).
* With the token, anyone can inject results under any `task_index`: the archive-ID check applies
  only to a task index that already has a cursor, so an unused index bypasses it.
* The listener binds `0.0.0.0`.

This is no weaker than the Celery path, so it is not a regression, but the design doc should state
the threat model honestly. Cheap improvements: pass the token to `clp-s` through an environment
variable; have the listener check `task_index < num_tasks`.

### 9. The protocol is not extensible and drops fields

* Fixed-arity positional arrays: adding a field requires a version bump and a coordinated upgrade.
* The old protocol carried `log_event_idx` (see issue 3); the new one does not. Neither carries the
  dataset, so a multi-dataset query's consumer cannot tell where a result came from.

**Direction.** Let the listener ignore trailing elements in a result frame, or negotiate
capabilities in the handshake. Add `log_event_idx` at least.

### 10. No per-task end frame

Without a trailer, the listener cannot distinguish "task finished cleanly with N results" from "task
died after N results". Consequences: no progress reporting (on the Spider path
`num_tasks_completed` already stays at 0), no completeness check, no safe cursor release. An
`[END, total]` frame is sent once per connection that had results, so it is cheap. Do it together
with the acknowledgement in issue 2.

### 11. Dead peers are not detected

The listener sets no TCP keepalive. A peer that vanishes without FIN or RST (prototype job 14: a
killed worker's connection hung with no FIN) is detected only by TCP's retransmission timeout,
about 15 minutes. An application-level idle timeout during streaming would be wrong, because
`clp-s` legitimately sends nothing while scanning a large archive between matches; `SO_KEEPALIVE`
with a short interval probes the peer, not the application.

### 12. An alternative architecture was not discussed

Tasks could write results to shared storage, keyed by task so that reruns overwrite idempotently,
and the consumer could pull. Benefits: fault tolerance for free, recovery after a listener crash
(currently a non-goal), no backpressure into workers (issues 1 and 4), replay, multiple consumers.
Costs: an extra hop of latency and a storage dependency. The design should at least say why it is
not chosen.

## P2

| #  | Issue                                                                                                 |
|----|-------------------------------------------------------------------------------------------------------|
| 13 | What is sent as the timestamp for an event without a timestamp column? The design does not say.      |
| 14 | Output formats differ: the CLI keeps the message's trailing newline, the API strips it; `end.status` uses `Succeeded` while the DB uses `SUCCEEDED`. Suggest NDJSON by default for the CLI, with `--raw` printing only the message. |
| 15 | A FAILED job prints a pile of partial results and then exits non-zero. The docs should say the output may be partial. *Measured:* 4 SIGKILLs with one retry failed the job at 16.9 s after 167,875 of 659,677 results, then `end: Failed`; conversely a job failed by hard timeouts delivered all results (issue 20). The stream's `end.status` is the only reliable signal. |
| 16 | An index gap closes the connection, which fails the task, which is retried, which hits the same gap. A bug is amplified into N retries. The second identical gap should fail the job directly. |
| 17 | Every job with a stale attempt pays up to the 5 s drain grace at the end.                             |
| 18 | An unsupported version closes the connection with no way for `clp-s` to fall back. Acceptable, but record it. |
| 19 | Two overlapping attempts can emit one task's results out of index order. Accepted; not to be fixed.  |

## Which issues affect correctness

Of the 23 issues, these are the ones whose failure mode is wrong or incomplete results, or a
wrong "complete" signal, as opposed to a job that fails, hangs, or is slow.

| # | Failure mode the client sees | Trigger | Status |
|---|---|---|---|
| 2 | Results silently missing while the job and `end` say `Succeeded`: a task "succeeds" once its bytes are in its own kernel buffer, and if the listener never reads them nobody retries. | A worker dies with a backpressured tail (the Rust-reader report measured 8 to 15,764 of 1.88M results missing per run); or the listener closes after clp-s's last `send()` (reproduced on this branch: a small archive exits 0 although the listener dropped it). | Confirmed. Closed by protocol v2 (trailer and acknowledgement), not on this branch yet. |
| 10 | No loss is detectable, because the listener cannot tell "task finished with N results" from "task died after N". Concrete path: during the drain, a connection reading only duplicates (an orphan ran ahead) is cut after the grace period; if the orphan is gone too, the unclaimed tail is lost under `SUCCEEDED`. | An orphan ahead of the retry plus a slow worker-to-listener network. Narrow; not observed. | Closed by v2's trailer and acknowledgement. |
| 3 | One duplicate and one loss, silently: position-based deduplication assumes every attempt of a task produces identical output in identical order, and nothing enforces or tests it. | Any clp-s change to scan order, threading, or top-K tie-breaking; a rolling upgrade; issue 22. | Open. v2 verifies count and order per connection but not content, so it does not address this. Mitigation: carry `log_event_idx` and verify duplicates against it, or a contract test on clp-s. |
| 22 | Same as issue 3, with a known trigger: 3 of 225 fixtures differ in float formatting between the C++ and Rust readers, so a retry on the other engine yields a different message at the same index. | A job whose tasks run on workers with different `CLP_SPIDER_SEARCH_ENGINE` settings. | Open. Needs one engine per job, or the issue 3 check. |
| 20 (a) | A `reducer` output would double-count: an orphan and its retry both send their counts, and the reducer has no `_id` deduplication. | Aggregation on Spider, which the coordinator rejects today. | Open, latent. v2 does not fix it: both attempts would be acknowledged. |
| 20 (b) | A FAILED job can deliver a complete result set, so the status no longer says whether the results are complete. The data itself is intact. | Any hard timeout under the V2 engine. | Status-level only. |
| 8 | Fabricated results delivered as genuine: the session token is in the DB config and on worker command lines, and the archive-ID check only covers task indices that already have a cursor, so an unused index accepts anything. | An attacker with DB read access or `ps` on a worker while the session is open. | Open. Integrity under attack, not a bug in normal operation. |
| 13 | A wrong time attributed to a result: what clp-s sends for an event without a timestamp is unspecified. | Records lacking the timestamp key. | Open, unverified; metadata only. |
| 14 | Messages with meaningful leading or trailing whitespace differ by path: the API strips one trailing newline, the results-cache path trims, the CLI keeps the bytes. | Any such message. | Cosmetic but byte-visible; needs one rule. |

Two that are not correctness issues but get mistaken for them:

* Issue 19, out-of-order results within a task when attempts overlap: no order is promised, so it
  is only wrong for a consumer that assumes per-archive order. The API docs should say so.
* Issues 1, 21 and 15, a job that fails because of backpressure and delivers partial (or, with
  orphans, complete) results followed by `end: Failed`: the data is never wrong and the client is
  told. These are availability issues.

After v2 lands, the remaining correctness exposure is issues 3 and 22 (determinism, now with a
real trigger), 20 (a) (aggregation on Spider, latent) and 8 (adversarial). Issue 3 is the one to
fix first: v2's acknowledgement verifies count and order per connection but cannot detect that two
attempts disagree about the content at the same index.

## Protocol v2 sketch

Addresses issues 2, 3, 9, and 10 together:

```text
clp-s -> listener   HELLO    [ver, token, task_index, archive_id]
listener -> clp-s   WELCOME  [ver, next_result_index]                      # confirms the handshake; resumes from the cursor
clp-s -> listener   RESULT   [result_index, timestamp, log_event_idx, message]   # xN, starting at next_result_index
clp-s -> listener   END      [total_results]
listener -> clp-s   ACK      [total_results]                               # clp-s exits 0 only after this
```

Listener side: the cursor also records the emitted results' `log_event_idx` for verification;
`SO_KEEPALIVE` on every connection; one shared status poller; per-session quotas.

## Suggested order of work

1. Issues 1 and 4, the backpressure policy: it defines the contract between workers and consumers
   and influences whether the protocol needs a pause/stop message.
2. Issue 7, making the CLI an API client: it decides how many hosts the listener has and whether
   the Python layer stays.
3. Protocol v2 (issues 2, 3, 9, 10): cheapest to change now.
4. Issue 5, cancellation: coordinator-side prerequisite.
5. The rest.

## Pre-existing issues seen by the prototype (not part of this design)

* Rust `Package::default().storage_engine` is `Clp`; Python's default is `CLP_S`.
* `QueryJobPollingConfig` field names do not match Python's `*_ms` names, so the API server ignores
  those values.
* On Spider: `query_jobs.duration` stays NULL; a default `POST /query` (write to file) FAILs;
  `--verbose` logs `CLP_DB_PASS` on a failed run.
* `start_clp` exits 1 because compression-coordinator rejects an fs `logs_input`.
* The `impl_sqlx_type!` doctest fails; CI's nextest does not run doctests.
