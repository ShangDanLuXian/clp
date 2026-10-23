# Streaming search: implementation plan

## Work breakdown

The work lands as fourteen small pull requests against `main`. Each one:

* has a single purpose and makes sense on its own;
* builds, passes the repository's checks, and leaves `main` working;
* carries unit tests for the code it adds;
* updates the docs for anything user-facing it adds.

Each PR depends only on lower-numbered PRs. Sizes are rough estimates including tests, and exclude
generated files.

### `clp-s`

#### PR 1. `feat(clp-s)!: Send network search results with a handshake, result indices, timestamps, a trailer and an acknowledgement.`

Depends on: nothing. Size: about 600 lines, about 350 of them tests.

`clp-s` sends its results to a listener (PRs 7 to 11). A connection carries four kinds of
messages, each a msgpack array:

| Message | Sent by | Content |
|---|---|---|
| Handshake | `clp-s`, once | `[protocol_version, session_token, task_index, archive_id]` |
| Result | `clp-s`, once per result | `[result_index, timestamp, message]` |
| Trailer | `clp-s`, once after the last result | `[num_results]` |
| Acknowledgement | the listener, once | `[num_results]`, always 10 bytes: `0x91 0xcf` and the count as a big-endian u64 |

`session_token` and `archive_id` are strings, and `timestamp` is an integer in epoch milliseconds.
`result_index` counts the results from 0, in the order `clp-s` finds them. The other numbers are
unsigned integers.

* `clp-s s ... network` requires `--session-token` and `--task-index`, and accepts a single
  archive.
* Connect when the first result is found, so an archive without results never opens a
  connection; then send the handshake and one result message per result.
* In the output handler's `finish()` step, which runs after the search, send the trailer, shut
  down the write side of the socket, wait for the acknowledgement, and check that its count equals
  the number of results sent. Any failure makes the search fail and `clp-s` exit with an error, so
  the task fails and Spider reruns it, up to `query_task_max_retry` times.
* Make the shared `clp::networking::try_send`, which `clo` and the reducer also use, reliable:
  loop until the whole buffer is written (a send interrupted part-way otherwise drops the rest of a
  message), retry `EINTR`, and pass `MSG_NOSIGNAL` so a closed connection returns an error instead
  of killing the process with `SIGPIPE`.
* Breaking: `sbin/search.sh` on the `query-scheduler` path runs the `network` output without the
  new options, so it no longer works with `clp-s`.
* Catch2 unit tests over a socket pair: the messages, the trailer, a correct, malformed or
  mismatched acknowledgement, and a peer that closes early.

### Shared Rust code

#### PR 2. `feat(clp-rust-utils): Add the network output of a query job.`

Depends on: nothing. Size: about 210 lines.

* `NetworkOutput { host, port, session_token }`, and a `network_output` field in
  `QueryJobConfig` (`SearchJobConfig` until #2590 is merged) replacing `network_address`, which
  only the Python search path sets and no Rust code reads. The token is a UUID, serialized as a
  string: the form `clp-s` receives on its command line and sends back in the handshake.
* `OutputHandle::Network { host, port, session_token }`, the task's output destination.
* Unit tests: serialization round trips.

#### PR 3. `refactor(clp-rust-utils): Move query job submission and the cancellation request out of the API server.`

Depends on: nothing. Size: about 200 lines.

* Move the job insertion of `Client::submit_query` and the cancellation request of
  `Client::cancel_query_job` (a pending or running job becomes `CancelRequested`) into
  `clp-rust-utils`, as functions that take a database pool, so the streaming endpoint and the CLI
  can reuse them.
* No behavior change: `DELETE /query/{query_job_id}` keeps its `404` and `409` responses, and the
  API server's existing tests cover it.

#### PR 4. `feat(clp-rust-utils): Validate streaming query job configs.`

Depends on: PR 2. Size: about 150 lines.

* One check, used by both the API server and the CLI, that a query job config with a network
  output can be submitted: the query isn't empty, the time range isn't reversed,
  `max_num_results` isn't set, and the job has no aggregation.
* Unit tests for each rule.

### `query-coordinator` and the Spider query task

#### PR 5. `feat(query-coordinator): Pass each query task its index within the job.`

Depends on: nothing. Size: about 250 lines.

* Add each task's index (its position in the list of archives) to the Spider task graph as a
  per-task input, and accept it in the Spider query task (`query::clp_s_search`, in
  `components/clp-tdl-package`). The coordinator and the task change together, because the task's
  input list must match the graph.
* Spider stores the graph, so a rerun of a task gets the same index.
* Unit tests: the graph's per-task inputs.

#### PR 6. `feat(query-coordinator): Send a query job's results to its network output.`

Depends on: PR 1, PR 2 and PR 5. Size: about 350 lines.

* Choose each job's output from its config: the network output if set, otherwise the results
  cache. Fail a job that sets both a network output and `max_num_results` before creating its
  tasks.
* In the Spider query task, build the `clp-s` command line for the network output.
* Unit tests: the output selection, the failure, and the command line.

### `search-result-listener`

#### PR 7. `feat(search-result-listener): Add the crate with the wire protocol.`

Depends on: nothing. Size: about 600 lines.

* A new crate, `components/search-result-listener`, with no I/O yet: decoding the handshake,
  result and trailer messages of PR 1, encoding the acknowledgement, and the protocol errors:
  unsupported version, malformed message, result index out of order, an archive ID that differs
  from the task's earlier attempts, count mismatch, and data after the trailer.
* Workspace member, CI path filter.
* Unit tests, including cut-off messages and out-of-range values.

#### PR 8. `feat(search-result-listener): Add the per-task result cursor and the result encodings.`

Depends on: PR 7. Size: about 300 lines.

* `TaskCursor`, one per task index: the archive ID from the task's first handshake, and the index
  of the next result not yet delivered. A connection delivers result `r` only by moving the cursor
  from `r` to `r + 1` with one atomic compare-and-swap. If the cursor is already past `r`, another
  attempt of the task has delivered that result, and this copy is dropped as a duplicate. Every
  connection sends its indices in order from 0, so a result is never ahead of the cursor. This
  relies on `clp-s` producing a task's results in the same order on every attempt.
* `ResultEncoding` and `ResultEncoder`, the pluggable output format, and `SearchResultEncoding`,
  which yields plain results.
* Unit tests: claiming and dropping duplicates from concurrent claimers; encoding batches.

#### PR 9. `feat(search-result-listener): Add the connection handler.`

Depends on: PR 8. Size: about 700 lines, about 380 of them tests.

* The per-connection logic: read the handshake, check the archive ID against the task's cursor,
  claim and encode the results of each socket read (up to 64 KiB), and send them as one batch into
  the session's queue, a bounded `tokio::sync::mpsc` channel. Then check the trailer, and
  acknowledge only after the last batch is in the queue. A full queue blocks the handler, which
  stops reading, so TCP slows `clp-s` down.
* If the queue's receiver is gone because the client left, keep reading, claim and drop the
  results, and still acknowledge, so the tasks succeed.
* Written against `AsyncRead + AsyncWrite`, so the unit tests drive it through an in-memory stream
  (`tokio::io::duplex`): two attempts of one task, a rerun, each protocol error, a connection that
  ends before its trailer, a slow consumer, and a dropped receiver.

#### PR 10. `feat(search-result-listener): Add the TCP listener and its session registry.`

Depends on: PR 2 and PR 9. Size: about 450 lines.

* `ResultListener`: binds the socket (`0.0.0.0:0` by default, so the OS picks the port), accepts
  connections, gives each one 10 s to send its handshake, and routes it to its session by token.
  Unknown tokens are rejected. Unless the caller gives a host, it advertises the machine's first
  non-loopback IPv4 address.
* `Session` creation and registration: a new session gets a random token and its queue (16
  batches by default), and is registered before its job is submitted, so no connection arrives
  before its session exists. The session gives the caller the `NetworkOutput` (PR 2) to put in the
  job config.
* Unit tests: routing, unknown tokens, the handshake time limit, several sessions on one listener.

#### PR 11. `feat(search-result-listener): Finish a session once its query job terminates.`

Depends on: PR 10. Size: about 500 lines.

* `JobStatusSource`, a trait that reports a job's status, and its database implementation, which
  reads `query_jobs.status` every 100 ms.
* Once the job's status is final:
  1. unregister the session, so a later connection with its token is closed at the handshake;
  2. let open connections finish within a grace period: zero if the job succeeded, 5 s by default
     otherwise. The grace period limits only reading from sockets; a claimed batch is always
     delivered;
  3. close the queue;
  4. report the final status and the statistics `num_results_emitted`, `num_duplicates_dropped`,
     `num_protocol_errors` and `num_incomplete_connections` (connections that ended before their
     trailer).
* The final status is enough to finish on. A task succeeds only after its acknowledgement, which
  follows its last batch, so once the job has succeeded every result is already in the queue.
* Unit tests with a fake status source: each step of the shutdown, the grace period, and the
  statistics.

### API server

#### PR 12. `feat(api-server): Add a Server-Sent Events encoding for streamed results.`

Depends on: PR 8. Size: about 400 lines.

* `SseEncoding`, which turns each result into a ready-to-send `data:` event carrying
  `{archive_id, timestamp, message}`, with the message's trailing newline removed. Also a response
  type that forwards the batches and sends a keep-alive comment after 15 s without an event.
* Unit tests: the events match axum's own SSE rendering byte for byte, including escaping; the
  keep-alive timing.

#### PR 13. `feat(api-server): Add POST /query/stream to run a query and stream its results as Server-Sent Events.`

Depends on: PR 3, PR 4, PR 6, PR 11 and PR 12. Size: about 600 lines.

* Config: `api_server.search_result_listener` in the Rust config, with the listener's `port`
  (default 3003) and `advertised_host` (default: the API server's container hostname, which the
  workers can reach). The package generates the API server's config without this section, so the
  package uses the defaults.
* Start the listener with the API server when its config has a `query_coordinator` section;
  otherwise the endpoint answers 501.
* The handler rejects requests that ask for MongoDB buffering or count-by-time, builds the job
  config and checks it (PR 4), opens a session, submits the job, and streams these events:
  * `job`, first: `{query_job_id}`;
  * one unnamed event per result (PR 12);
  * `end`, last: `{status}` and the statistics of PR 11;
  * `error`, last instead of `end` if the job's status can't be read: `{message}`.
* If the client disconnects before `end`, request the job's cancellation (PR 3).
* Regenerate the OpenAPI document and the web UI's API client; document the endpoint and its
  events in the API server guide.
* Unit tests in the style of the existing route tests, which need no database: 501 without the
  listener, each rejected request, the events of a session driven by a fake status source
  (PR 11), and the cancellation request when the response is dropped before `end`.

### CLI

#### PR 14. `feat(clp-package): Add sbin/streaming-search.sh, backed by the clp-streaming-search tool.`

Depends on: PR 3, PR 4, PR 6 and PR 11. Size: about 600 lines, about 200 of them tests.

* A new crate, `components/clp-package-cli`, for the Rust command-line tools shipped in the
  package, starting with `clp-streaming-search`. It is separate from the listener crate so the
  API server doesn't depend on a command-line crate. Workspace member, CI path filter.
* `clp-streaming-search` takes the arguments of `sbin/search.sh` except `--count`,
  `--count-by-time` and `--file-path`: the query, `--config`, `--dataset`, `--begin-time`,
  `--end-time`, `--ignore-case`, `--raw` and `--verbose`. It also takes `--advertised-host`, to
  override the address the query tasks connect to.
* It builds the job config and checks it (PR 4), binds its own listener on a port the OS picks,
  opens a session, submits the job with the session's network output, and runs the session. It
  prints each result as it arrives: `--raw` prints only the message; otherwise the archive ID and
  timestamp come first. It exits with 0 only if the job succeeded, and logs the final status
  otherwise. Ctrl-C requests the job's cancellation.
* It reads the package config (`etc/clp-config.yaml` by default) itself, and refuses to run unless
  the config has a `query_coordinator` section. It also reads `database.username` and
  `database.password` from the credentials file (`etc/credentials.yaml` by default), which no Rust
  code reads yet.
* `sbin/streaming-search.sh` runs `clp-streaming-search` in the package's runtime container,
  which uses the host's network. So the database address in the config works as written, and the
  query tasks connect back to the machine running the command.
* Ship the binary in the package image; document the command.
* Unit tests: argument parsing, loading the config and the credentials, output formatting, and
  the exit code for each final status.

## Open questions

1. **Default output format.** Should `sbin/streaming-search.sh` print each result's timestamp as
   the raw epoch integer, as the API returns it, or formatted as a date and time?
2. **Loading the config.** The plan has `clp-streaming-search` read the package config and the
   credentials itself. The alternative is a Python wrapper, like the one behind `sbin/search.sh`,
   that generates a container config for the tool, so that config handling stays in one place.
   The plan avoids the wrapper because the tool needs only the database address and credentials,
   and the runtime container uses the host's network, so the user's config works as written.
