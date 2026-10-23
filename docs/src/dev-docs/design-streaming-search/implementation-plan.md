# Streaming search: implementation plan

Tracking PR title:

```text
feat(clp-package)!: Stream search results from Spider query tasks to the API server and the CLI.
```

Timeline: two weeks, Monday 2026-10-26 to Friday 2026-11-06.

# Description

## Summary

Today a query job on Spider writes its results to the results cache (MongoDB), and a client can
read them only after the whole job has finished. This work adds **streaming search**: the job's
query tasks send their results over TCP to a listener in the caller as they find them, and the
caller forwards them to the client immediately.

Two callers use it:

* the API server, through a new `POST /query/stream` endpoint that answers with Server-Sent
  Events;
* the `sbin/streaming-search.sh` command-line tool.

Spider reruns failed tasks, so a task's results can arrive more than once. The listener delivers
each result to the client exactly once, and a task counts as successful only after the listener
confirms that it has received all of the task's results.

## Scope

In scope:

* the `network` output of `clp-s`, which each query task uses to send its results;
* the `query-coordinator` and the Spider query task (`query::clp_s_search`), which pass the network
  destination to each task;
* a new Rust library, `search-result-listener`, that receives and deduplicates the results;
* `POST /query/stream` in the API server, including cancelling the job when the client
  disconnects;
* `sbin/streaming-search.sh`;
* user docs and the package config template.

Out of scope:

* aggregations (`--count`, `--count-by-time`), which stay on the results-cache path;
* result limits (`max_num_results`) for streaming searches: a streaming search returns every
  result;
* the `query-scheduler` path, which is being deprecated, and its `sbin/search.sh`;
* the Helm deployment, which doesn't run the `query-coordinator`; there the endpoint answers 501.

## How it works

```text
client
  │  POST /query/stream (SSE), or sbin/streaming-search.sh
  ▼
API server ─ ResultListener (one per process)
               └─ Session (one per job): a cursor per task, a bounded queue of results
  ▲  TCP: handshake → results → trailer, then the listener's acknowledgement
  │
Spider query task (one per archive) ─ clp-s s <archive> <query> network --host --port
  ▲                                        --session-token --task-index
query-coordinator: builds one task per archive and gives each task a fixed index
```

**Submitting a job.** The caller opens a listener session first, which creates a random session
token. It then inserts the query job with a network output: the listener's address and the
token. The `query-coordinator` creates one Spider task per archive and gives each task a fixed
index (its position in the list of archives). Spider stores the task graph, so a rerun of a task
gets the same index.

**Sending results.** Each task runs `clp-s` with a `network` output. `clp-s` connects to the
listener when it finds its first result, so an archive without results never opens a connection.
It sends four kinds of messages, each a msgpack array:

| Message | Sent by | Content |
|---|---|---|
| Handshake | `clp-s`, once | `[protocol_version, session_token, task_index, archive_id]` |
| Result | `clp-s`, once per result | `[result_index, timestamp, message]`, with `result_index` counting from 0 |
| Trailer | `clp-s`, once after the last result | `[num_results]` |
| Acknowledgement | the listener, once | `[num_results]`, always 10 bytes: `0x91 0xcf` and the count as a big-endian u64 |

After the trailer, `clp-s` stops writing and waits for the acknowledgement. It exits successfully
only if the acknowledgement arrives and its count matches; otherwise it exits with an error, the
task fails, and Spider reruns it.

**Exactly once.** For each task, the listener keeps a cursor: the index of the next result it
hasn't delivered yet. A result with index `r` is delivered only if the cursor is exactly `r`, and
delivering it moves the cursor to `r + 1` in one atomic step. If the cursor is already past `r`,
another attempt of the same task has delivered that result, and this copy is dropped. Each
connection must send its results in order (0, 1, 2, ...), which the listener checks. This relies
on `clp-s` producing a task's results in the same order on every attempt, which it does: it
searches on one thread, visits schemas in the order stored in the archive, and reads rows in
order.

**Acknowledging.** The listener acknowledges a connection only after it has decoded the trailer,
the connection has sent indices `0` to `N - 1` in order, `N` matches the trailer, and every
result the connection delivered has been handed to the session's queue. So once every task has
succeeded, the client has every result.

**Delivering to the client.** Each connection converts the results it delivers into the client's
final format (SSE events for the API server, output lines for the CLI) and passes them on in
batches, one batch per socket read. The session's queue is bounded, so a slow client slows the
connections down instead of growing memory.

**Finishing.** The session polls the job's status every 100 ms. When the job reaches a final
status, the session stops accepting new connections (after accepting any that are already
waiting), lets open connections finish within a short grace period, closes the queue, and reports
the final status and statistics: results delivered, duplicates dropped, connections closed for
protocol errors, and connections that ended before their trailer. If the client disconnects, the
session keeps reading the tasks' results and drops them, so the tasks still succeed, and the
caller cancels the job.

## Work breakdown

The work lands as fifteen small pull requests against `main`. Each one:

* has a single purpose and makes sense on its own;
* builds, passes the repository's checks, and leaves `main` working;
* carries unit tests for the code it adds;
* updates the docs for anything user-facing it adds.

Sizes are rough estimates including tests, and exclude generated files.

### `clp-s`

#### PR 1. `fix(clp): Retry partial and interrupted sends, and don't raise SIGPIPE when the peer has closed the connection.`

Depends on: nothing. Size: about 60 lines.

* `clp::networking::try_send` loops until the whole buffer is written, retries `EINTR`, and passes
  `MSG_NOSIGNAL` so a closed connection returns an error instead of killing the process. The
  reducer and `clo` use the same function and benefit too.
* Unit tests over a socket pair.

#### PR 2. `feat(clp-s)!: Send network search results with a handshake, result indices, timestamps, a trailer and an acknowledgement.`

Depends on: PR 1. Size: about 550 lines, about 350 of them tests.

* `clp-s s ... network` requires `--session-token` and `--task-index`, and accepts a single
  archive.
* Connect when the first result is found; send the handshake and the results as described above;
  always include each result's timestamp.
* In the output handler's `finish()` step, which runs after the search, send the trailer, stop
  writing, wait for the acknowledgement, check it, and close. Any failure makes the search fail
  and `clp-s` exit with an error.
* Catch2 unit tests over a socket pair: the messages, the trailer, a correct, malformed or
  mismatched acknowledgement, and a peer that closes early.

### Shared Rust types

#### PR 3. `feat(clp-rust-utils): Add the network output of a query job.`

Depends on: nothing. Size: about 250 lines.

* `NetworkOutput { host, port, session_token }`, and a `network_output` field in
  `QueryJobConfig` (`SearchJobConfig` until #2590 is merged) replacing the unused
  `network_address`. The token is a UUID stored as a string, so the Python mirror of the job
  config and the database see a readable value. Update the Python mirror in `job_orchestration`.
* `OutputHandle::Network { host, port, session_token }`, the task's output destination.
* Unit tests: serialization round trips and the field names the Python mirror expects.

#### PR 4. `refactor(clp-rust-utils): Move query job submission and the cancellation request out of the API server.`

Depends on: nothing. Size: about 200 lines.

* Move `INSERT INTO query_jobs` and the cancellation request behind `Client::cancel_query_job` (a
  pending or running job becomes `CancelRequested`) from the API server into `clp-rust-utils`, so
  the streaming endpoint and the CLI can reuse them.
* No behavior change: `DELETE /query/{query_job_id}` keeps its `404` and `409` responses, and the
  API server's existing tests cover it.

### `query-coordinator` and the Spider query task

#### PR 5. `feat(query-coordinator): Pass each query task its index within the job.`

Depends on: nothing. Size: about 250 lines.

* Add each task's index (its position in the list of archives) to the Spider task graph as a
  per-task input, and accept it in the Spider query task (`query::clp_s_search`). The coordinator
  and the task change together, because the task's input list must match the graph.
* Spider stores the graph, so a rerun of a task gets the same index.
* Unit tests: the graph's per-task inputs.

#### PR 6. `feat(query-coordinator): Send a query job's results to its network output.`

Depends on: PR 2, PR 3 and PR 5. Size: about 350 lines.

* Choose each job's output from its config: the network output if set, otherwise the results
  cache. Reject a job that sets both a network output and `max_num_results`.
* In the Spider query task, build the `clp-s` command line for the network output.
* Unit tests: the output selection, the rejection, and the command line.

### `search-result-listener`

#### PR 7. `feat(search-result-listener): Add the crate with the wire protocol.`

Depends on: PR 3. Size: about 600 lines.

* A new crate, `components/search-result-listener`, with no I/O yet: decoding the handshake,
  result and trailer messages, encoding the acknowledgement, and the protocol errors (unsupported
  version, malformed message, unexpected result index, count mismatch, data after the trailer).
* Workspace member, CI path filter.
* Unit tests, including cut-off messages and out-of-range values.

#### PR 8. `feat(search-result-listener): Add the per-task result cursor and the result encodings.`

Depends on: PR 7. Size: about 300 lines.

* `TaskCursor`: the next undelivered index of a task, advanced with one atomic compare-and-swap.
* `ResultEncoding` and `ResultEncoder`, the pluggable output format, and `SearchResultEncoding`,
  which yields plain results.
* Unit tests: claiming and dropping duplicates from concurrent claimers; encoding batches.

#### PR 9. `feat(search-result-listener): Add the connection handler.`

Depends on: PR 8. Size: about 650 lines, about 350 of them tests.

* The per-connection logic: read the handshake, check the archive ID against the task's cursor,
  claim and encode the results of each read, send one batch per read, check the trailer, and
  acknowledge after the last batch has been handed on.
* Written against `AsyncRead + AsyncWrite`, so the unit tests drive it through an in-memory stream
  (`tokio::io::duplex`): two attempts of one task, a rerun, each protocol error, a connection that
  ends before its trailer, and a slow consumer.

#### PR 10. `feat(search-result-listener): Add the TCP listener and its session registry.`

Depends on: PR 9. Size: about 450 lines.

* `ResultListener`: binds the socket, accepts connections, gives each one 10 s to send its
  handshake, and routes it to its session by token. Unknown tokens are rejected.
* `Session` creation and registration: a new session gets a random token and a bounded batch
  queue, and is registered before its job is submitted.
* Unit tests: routing, unknown tokens, the handshake time limit, several sessions on one listener.

#### PR 11. `feat(search-result-listener): Finish a session once its query job terminates.`

Depends on: PR 10. Size: about 550 lines.

* `JobStatusSource`, a trait that reports a job's status, and its database implementation, polled
  every 100 ms.
* The shutdown sequence: accept connections that are already waiting, unregister the session,
  let open connections finish within the grace period, close the queue, and report the final
  status and statistics. When the client has gone, keep reading and drop the results so tasks
  still succeed.
* Unit tests with a fake status source: each step of the shutdown, the grace period, the
  statistics, and a dropped result stream.

### API server

#### PR 12. `feat(api-server): Add a Server-Sent Events encoding for streamed results.`

Depends on: PR 8. Size: about 400 lines.

* `SseEncoding`, which turns results into ready-to-send `data:` events, and a response type that
  forwards the batches and sends a keep-alive comment after 15 s without an event.
* Unit tests: the events match axum's own SSE rendering byte for byte, including escaping; the
  keep-alive timing.

#### PR 13. `feat(api-server): Validate streaming query requests.`

Depends on: PR 3. Size: about 250 lines.

* Turn a `QueryConfig` into a streaming query job config, rejecting what streaming doesn't
  support: an empty query, an invalid time range, `max_num_results`, MongoDB buffering,
  count-by-time, invalid dataset names.
* Unit tests for each rule.

#### PR 14. `feat(api-server): Add POST /query/stream to run a query and stream its results as Server-Sent Events.`

Depends on: PR 4, PR 6, PR 11, PR 12 and PR 13. Size: about 700 lines.

* Config: `api_server.search_result_listener` with the listener's `port` (default 3003) and
  `advertised_host`, in Rust, in Python, and in the config template. The advertised host defaults
  to the API server's container hostname, so workers reach it without extra configuration.
* Start the listener with the API server when the package runs the query coordinator (its config
  has a `query_coordinator` section); otherwise the endpoint answers 501.
* The handler: validate, open a session, submit the job, and stream a `job` event, one event per
  result, and a final `end` event (status and statistics) or `error` event. If the client
  disconnects before `end`, request the job's cancellation.
* Regenerate the OpenAPI document and the web UI's API client; document the endpoint in the API
  server guide.
* Unit tests with an in-memory query job table: the events, 501, validation errors, and
  cancellation on disconnect.

### CLI

#### PR 15. `feat(clp-package): Add sbin/streaming-search.sh.`

Depends on: PR 14. Size: about 300 lines.

* A small Rust command-line client of `POST /query/stream`. It takes the same arguments as
  `sbin/search.sh` without the aggregation options, prints each result (`--raw` prints only the
  message; otherwise the archive ID and timestamp come first), and exits with 0 only if the job
  succeeded. Pressing Ctrl-C closes the stream, which cancels the job.
* Document the tool.
* Unit tests: argument parsing and output formatting.

## Order of work

PR 1, PR 3, PR 4 and PR 5 depend on nothing and start on day 1. The longest chain runs through
the listener:

```text
PR 3 → PR 7 → PR 8 → PR 9 → PR 10 → PR 11 → PR 14 → PR 15
```

Everything else runs alongside it: `clp-s` (PR 1 → PR 2) and the coordinator (PR 5 → PR 6) feed
into PR 14 through PR 6; PR 12 needs only PR 8; PR 13 needs only PR 3.

## Schedule

| Day | Date | Engineer A: listener | Engineer B: `clp-s`, coordinator, API server | Milestone |
|---|---|---|---|---|
| 1 | Mon 10-26 | Settle the open questions. PR 3, PR 7. | PR 1, PR 5. | |
| 2 | Tue 10-27 | PR 8. | PR 2, PR 4. | PR 3 merged |
| 3 | Wed 10-28 | PR 9. | PR 2 review, PR 13. | |
| 4 | Thu 10-29 | PR 9 review. | PR 6, PR 12. | `clp-s` merged |
| 5 | Fri 10-30 | PR 10. | PR 6 and PR 12 review. | Coordinator merged |
| 6 | Mon 11-02 | PR 10 review, PR 11. | PR 14. | |
| 7 | Tue 11-03 | PR 11 review. | PR 14. | Listener merged |
| 8 | Wed 11-04 | Review PR 14. | PR 14 review, PR 15. | API merged |
| 9 | Thu 11-05 | Review PR 15. | PR 15 review. | Feature complete |
| 10 | Fri 11-06 | Spare day for review follow-ups and release notes. | | Done |

A PR named on a day is opened that day; it merges once reviewed, usually the next day. Each PR
is opened as soon as the PR it depends on is up for review, and rebased when that one merges. The
schedule assumes a reviewer available every day.

## Breaking changes

* The `network` output of `clp-s` uses the new protocol and waits for an acknowledgement, so
  programs that read the old format can no longer use it. This includes `sbin/search.sh` on the
  `query-scheduler` path. PR 2's title is marked with `!`.
* `QueryJobConfig.network_address` is replaced by `network_output`. It is an internal field that
  nothing reads today.

## Risks

| Risk | Mitigation |
|---|---|
| Fifteen PRs to review in two weeks. | Each PR is small and single-purpose; open them early and stack them; reserve a reviewer slot every day. |
| Other work on the `query-coordinator` and the Spider query task conflicts with PR 5 and PR 6. | Land both in the first week. |
| #2590 (renaming `SearchJobConfig` to `QueryJobConfig`) is still open and touches the same files as PR 3, PR 6 and PR 13. | Base PR 3 on it, or rebase once it merges; the rename is mechanical. |
| The C++ build makes PR 1 and PR 2 slow to iterate on. | Start them on day 1, in parallel with the Rust work. |

## Open questions

1. **Shape of `sbin/streaming-search.sh`.** The plan above makes it a client of the API server.
   The alternative is a tool that runs its own listener, which needs database credentials, a
   network route from the workers back to the machine running the tool, and the Python wrapper
   that the other `sbin` scripts use. Settle on day 1.
2. **Timestamp format in the CLI output.** Epoch milliseconds, as the API returns them, or a
   formatted date and time.

# Checklist

* [ ] The PRs satisfy the [contribution guidelines][yscope-contrib-guidelines].
* [ ] Breaking changes are indicated in the PR titles (PR 2).
* [ ] Each user-facing PR updates its docs (PR 14, PR 15).
* [ ] The open questions are answered in the tracking issue.

# Validation performed

Planned. Each PR is validated by the repository's checks (`task lint:check-rust`,
`cargo nextest run --workspace`, and the C++ build and `clp_s` unit tests for PR 1 and PR 2) and by
the unit tests it adds. No integration or end-to-end tests are planned.

[yscope-contrib-guidelines]: https://docs.yscope.com/dev-guide/contrib-guides-overview.html
