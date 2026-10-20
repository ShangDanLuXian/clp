# Issues

Every issue found while reviewing the streaming search design and the prototype, ranked by
severity. Each entry gives the problem, the evidence (file references are to the prototype code
on this branch), and a proposed direction. The core compare-and-swap deduplication is sound; the
issues are in the assumptions around it and in the cases it does not cover.

Severity:

* **P0**: can produce wrong results, or fail a job for no good reason.
* **P1**: an architectural trade-off that should be decided before the design is final.
* **P2**: details.

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

The API report records "a stalled client makes the job FAIL by pushing tasks past their hard
timeout" as refuted, without a reason. No test in either report stalls longer than the smallest
hard timeout (60 s): the longest stalls were 25 s (prototype job 15) and 5 s (API disconnect test).
The refutation should be re-checked with a 90 s stall on a small archive.

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
| 15 | A FAILED job prints a pile of partial results and then exits non-zero. The docs should say the output may be partial. |
| 16 | An index gap closes the connection, which fails the task, which is retried, which hits the same gap. A bug is amplified into N retries. The second identical gap should fail the job directly. |
| 17 | Every job with a stale attempt pays up to the 5 s drain grace at the end.                             |
| 18 | An unsupported version closes the connection with no way for `clp-s` to fall back. Acceptable, but record it. |
| 19 | Two overlapping attempts can emit one task's results out of index order. Accepted; not to be fixed.  |

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
