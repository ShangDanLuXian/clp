# Design summary

This is a condensed description of the streaming search design as it was prototyped. Where the
prototype corrected the original design doc, the corrected version is given and marked
*(prototype)*.

## Scope

* **Spider only.** Streaming runs on the Spider query path: the Rust `query-coordinator` dispatches
  one search task per archive to Spider workers. The design originally dropped compatibility with
  the Celery path, but the prototype found that this broke every Celery search (not only streaming),
  so the Celery path was adapted to the new job config as well *(prototype)*.
* **New entry point.** `sbin/streaming-search.sh` runs a Rust CLI, `clp-streaming-search`.
  `sbin/search.sh` is left as it is.
* **No aggregation.** `--count` and `--count-by-time` stay on the results-cache path.

## The problem being solved

The Celery path streams results from each `clp-s` straight to a TCP server in the CLI, but it has
no fault tolerance (a retried task streams its results again), exits 0 on failure, can truncate
results, has no authentication, and ships no timestamp or archive ID.

The Spider path writes only to the results cache. Spider also reruns failed tasks. The results
cache tolerates reruns because its writes are deduplicated by `{archive_id, log_event_idx}`; a raw
network stream does not.

So the core problem is: stream through a retrying scheduler without duplicating results.

## Exactly-once delivery

Every result a task streams carries a `result_index`, numbered from 0 in the order `clp-s`
produces results. The listener keeps one `TaskCursor` per task index, holding an `AtomicU64`
`next_result_index`. A connection that receives result `r` claims it with one compare-and-swap
from `r` to `r + 1`:

| Outcome                   | Meaning                                 | Action                |
|---------------------------|-----------------------------------------|-----------------------|
| Swap succeeds             | First to deliver `r`                    | Emit                  |
| Swap fails, cursor > `r`  | Another attempt already emitted `r`     | Drop as duplicate     |
| Swap fails, cursor < `r`  | Index gap; impossible for a valid stream| Close the connection  |

This holds however attempts of one task overlap, because TCP preserves order within a connection
and the swap is atomic. It depends on a **precondition**: every attempt of a task produces the same
results in the same order. `clp-s` meets this today because it visits schemas in metadata order,
reads tables row by row, and searches on one thread. See [issues](issues.md) for why relying on
this is a problem.

Known limits: a task that exhausts its retries leaves its emitted results emitted; two overlapping
attempts can emit one task's results out of index order; a retry re-sends everything below the
cursor only for the listener to drop it.

## Wire protocol v1

Back-to-back msgpack values with no length prefix, one direction only (`clp-s` never reads):

1. **Handshake**, once: `[version: u8, session_token: str, task_index: u64, archive_id: str]`.
2. **Result**, per result: `[result_index: u64, timestamp: i64, message: str]`.
3. **EOF**: `clp-s` closes the socket. There is no trailer.

`clp-s` connects lazily, on its first result, so an archive with no results never connects and
never gets a cursor. The listener closes a connection on an unsupported version, an invalid UUID,
an unknown session token, an archive ID that differs from the cursor's, an index gap, or an
undecodable frame. A closed connection fails `clp-s`'s next `send()`, which fails the task, which
Spider reruns.

*(prototype)* msgpack encodes integers in their smallest form, so the listener decodes every
integer generically and range-checks it. `clp-s` rejects `network` output with more than one input
archive, because each archive would otherwise connect under the same task index.

## Listener library (`search-result-listener`)

* `ResultListener`: one per process; owns the `TcpListener`, the accept loop, and a
  `DashMap<SessionToken, Arc<Session>>`. Serves many concurrent jobs (the API server) or one (the
  CLI).
* `Session`: one per query job; owns a `DashMap<TaskIndex, Arc<TaskCursor>>`, the sender of a
  bounded mpsc channel of `SearchResult`s (default capacity 1024), a `CancellationToken`, and a
  `TaskTracker` of its connection tasks.
* `TaskCursor`: the `AtomicU64` plus the task's `ArchiveId`.
* `JobStatusSource`: a trait polled for the job's status. The MariaDB implementation polls
  `query_jobs.status` every 100 ms.

Handshake: two map lookups (session by token, cursor by task index, created on first use), then an
archive-ID check. The hot path per result is one msgpack decode, one CAS, and one channel send.

### Shutdown *(prototype)*

1. **Accept barrier.** Accept everything still queued in the kernel and wait, at most for the
   handshake timeout (10 s), for those handshakes. Without this, a succeeded task's connection
   still in the accept queue would be rejected as an unknown session.
2. **Unregister and cancel.** Remove the session from the registry; late connections are rejected.
3. **Drain.** Each open connection reads until EOF or until its grace budget runs out. The budget is
   the time spent waiting on the socket since the connection last claimed a result (default 5 s).
   A wall-clock deadline would drop a succeeded attempt's unread data; a per-read idle timeout would
   never stop a stale attempt that keeps resending duplicates.
4. **Close the result channel.**
5. **Resolve the outcome**: terminal status plus statistics (emitted, duplicates dropped, protocol
   errors; the API branch adds results discarded).

The grace timeout wraps only the socket read, never the channel send of an already-claimed result,
because no other connection can claim that index again.

### Backpressure

The channel is bounded. A slow consumer blocks connection tasks in `send`, which stop reading
their sockets, which makes TCP push back on `clp-s`'s blocking `send()`. See
[backpressure](backpressure.md) for why this conflicts with Spider's task timeouts.

## Changes outside the listener

* Job config: `network_address` is replaced by `network_output: Option<NetworkOutput {host, port,
  session_token}>`; the token is serialized as a hyphenated UUID string *(prototype)*.
* `OutputHandle::Network { host, port, session_token }`.
* `query-coordinator`: picks the output handle from each job's own config; appends the task index
  (its position in `archives_to_search`) as a per-task input. Spider persists the graph, so reruns
  reuse the index. The index is declared as `int64` in TDL *(prototype)*.
* TDL search task: builds `clp-s s ... network --host --port --session-token --task-index`.
* `clp-s` `network` handler: requires the token and task index, connects lazily, forces metadata
  output for timestamps, numbers results from 0. `try_send` now retries short writes and passes
  `MSG_NOSIGNAL`; `clp-s.cpp` catches exceptions from the output handler and exits 1
  *(prototype)*.
* Job submission moves from the API server into `clp-rust-utils`.

## `streaming-search.sh`

Three layers: the shell script runs `clp_package_utils.scripts.streaming_search` in the
`clp-runtime` container; the Python wrapper validates the config, refuses to run unless
`package.scheduler` is `spider`, generates the container config, and runs the Rust binary with
`docker run --network host`; `clp-streaming-search` binds the listener, submits the job with
`max_num_results: 0`, prints each result (`--raw` prints only the message), and exits 0 only if
the job succeeded. *(prototype)* Logs go to stderr; SIGINT and SIGTERM exit 130 and 143 (the CLI is
PID 1 in its container, and the kernel drops signals to a PID 1 without a handler); the job is left
running.

## API server integration *(prototype, second branch)*

`POST /query/stream` takes the `QueryConfig` body and answers with Server-Sent Events: a `job`
event, one unnamed event per result, then `end` (status and statistics) or `error`. The API server
binds one listener at startup, only in Spider mode (port 3003; other modes answer 501). When the
client disconnects before `end`, the job is marked CANCELLING (median 3 ms) and the listener keeps
reading and discarding results until the job terminates.

## Measured results (from the reports)

* CLI vs `sbin/search.sh`, 607 archives, 8 slots: 3.805 s vs 10.958 s median wall time (2.88x);
  identical result sets in 48 runs; fault injection (SIGKILL `clp-s`, kill a worker) lost and
  duplicated nothing.
* API `POST /query/stream`: 207.8 ms median to the first result, 15x faster than
  `POST /query` + `GET /query_results` and 4.9x faster than the CLI. 71% of that is one `clp-s`
  process (about 60 ms zero-filling a 113 MiB buffer, about 70 ms decompressing a whole packed
  stream before evaluating any row).
