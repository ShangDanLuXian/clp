# Streaming search: implementation plan

## Work breakdown

The work lands as fourteen small pull requests against `main`. Each one:

* has a single purpose and makes sense on its own;
* builds, passes the repository's checks, and leaves `main` working;
* carries unit tests for the code it adds;
* updates the docs for anything user-facing it adds.

Sizes are rough estimates including tests, and exclude generated files.

### `clp-s`

#### PR 1. `feat(clp-s)!: Send network search results with a handshake, result indices, timestamps, a trailer and an acknowledgement.`

Depends on: nothing. Size: about 600 lines, about 350 of them tests.

A connection carries four kinds of messages, each a msgpack array:

| Message | Sent by | Content |
|---|---|---|
| Handshake | `clp-s`, once | `[protocol_version, session_token, task_index, archive_id]` |
| Result | `clp-s`, once per result | `[result_index, timestamp, message]`, with `result_index` counting from 0 |
| Trailer | `clp-s`, once after the last result | `[num_results]` |
| Acknowledgement | the listener, once | `[num_results]`, always 10 bytes: `0x91 0xcf` and the count as a big-endian u64 |

* `clp-s s ... network` requires `--session-token` and `--task-index`, and accepts a single
  archive.
* Connect when the first result is found, so an archive without results never opens a
  connection; then send the handshake and one result message per result, always with the
  result's timestamp.
* In the output handler's `finish()` step, which runs after the search, send the trailer, stop
  writing, wait for the acknowledgement, check its count, and close. Any failure makes the search
  fail and `clp-s` exit with an error, so the task fails and Spider reruns it.
* Send through `clp::networking::try_send`, and make it reliable: loop until the whole buffer is
  written (a send interrupted part-way otherwise drops the rest of a message), retry `EINTR`, and
  pass `MSG_NOSIGNAL` so a closed connection returns an error instead of killing `clp-s` with
  `SIGPIPE`.
* Catch2 unit tests over a socket pair: the messages, the trailer, a correct, malformed or
  mismatched acknowledgement, and a peer that closes early.

### Shared Rust types

#### PR 2. `feat(clp-rust-utils): Add the network output of a query job.`

Depends on: nothing. Size: about 210 lines.

* `NetworkOutput { host, port, session_token }`, and a `network_output` field in
  `QueryJobConfig` (`SearchJobConfig` until #2590 is merged) replacing the unused
  `network_address`. The token is a UUID, serialized as a string: the form `clp-s` receives on its
  command line and sends back in the handshake.
* `OutputHandle::Network { host, port, session_token }`, the task's output destination.
* Unit tests: serialization round trips.

#### PR 3. `refactor(clp-rust-utils): Move query job submission and the cancellation request out of the API server.`

Depends on: nothing. Size: about 200 lines.

* Move `INSERT INTO query_jobs` and the cancellation request behind `Client::cancel_query_job` (a
  pending or running job becomes `CancelRequested`) from the API server into `clp-rust-utils`, so
  the streaming endpoint and the CLI can reuse them.
* No behavior change: `DELETE /query/{query_job_id}` keeps its `404` and `409` responses, and the
  API server's existing tests cover it.

### `query-coordinator` and the Spider query task

#### PR 4. `feat(query-coordinator): Pass each query task its index within the job.`

Depends on: nothing. Size: about 250 lines.

* Add each task's index (its position in the list of archives) to the Spider task graph as a
  per-task input, and accept it in the Spider query task (`query::clp_s_search`). The coordinator
  and the task change together, because the task's input list must match the graph.
* Spider stores the graph, so a rerun of a task gets the same index.
* Unit tests: the graph's per-task inputs.

#### PR 5. `feat(query-coordinator): Send a query job's results to its network output.`

Depends on: PR 1, PR 2 and PR 4. Size: about 350 lines.

* Choose each job's output from its config: the network output if set, otherwise the results
  cache. Reject a job that sets both a network output and `max_num_results`.
* In the Spider query task, build the `clp-s` command line for the network output.
* Unit tests: the output selection, the rejection, and the command line.

### `search-result-listener`

#### PR 6. `feat(search-result-listener): Add the crate with the wire protocol.`

Depends on: PR 2. Size: about 600 lines.

* A new crate, `components/search-result-listener`, with no I/O yet: decoding the handshake,
  result and trailer messages, encoding the acknowledgement, and the protocol errors (unsupported
  version, malformed message, unexpected result index, count mismatch, data after the trailer).
* Workspace member, CI path filter.
* Unit tests, including cut-off messages and out-of-range values.

#### PR 7. `feat(search-result-listener): Add the per-task result cursor and the result encodings.`

Depends on: PR 6. Size: about 300 lines.

* `TaskCursor`: the next undelivered index of a task, advanced with one atomic compare-and-swap.
* `ResultEncoding` and `ResultEncoder`, the pluggable output format, and `SearchResultEncoding`,
  which yields plain results.
* Unit tests: claiming and dropping duplicates from concurrent claimers; encoding batches.

#### PR 8. `feat(search-result-listener): Add the connection handler.`

Depends on: PR 7. Size: about 650 lines, about 350 of them tests.

* The per-connection logic: read the handshake, check the archive ID against the task's cursor,
  claim and encode the results of each read, send one batch per read, check the trailer, and
  acknowledge after the last batch has been handed on.
* Written against `AsyncRead + AsyncWrite`, so the unit tests drive it through an in-memory stream
  (`tokio::io::duplex`): two attempts of one task, a rerun, each protocol error, a connection that
  ends before its trailer, and a slow consumer.

#### PR 9. `feat(search-result-listener): Add the TCP listener and its session registry.`

Depends on: PR 8. Size: about 450 lines.

* `ResultListener`: binds the socket, accepts connections, gives each one 10 s to send its
  handshake, and routes it to its session by token. Unknown tokens are rejected.
* `Session` creation and registration: a new session gets a random token and a bounded batch
  queue, and is registered before its job is submitted.
* Unit tests: routing, unknown tokens, the handshake time limit, several sessions on one listener.

#### PR 10. `feat(search-result-listener): Finish a session once its query job terminates.`

Depends on: PR 9. Size: about 550 lines.

* `JobStatusSource`, a trait that reports a job's status, and its database implementation, polled
  every 100 ms.
* The shutdown sequence: accept connections that are already waiting, unregister the session,
  let open connections finish within a short grace period, close the queue, and report the final
  status and statistics. When the client has gone, keep reading and drop the results so tasks
  still succeed.
* Unit tests with a fake status source: each step of the shutdown, the grace period, the
  statistics, and a dropped result stream.

### API server

#### PR 11. `feat(api-server): Add a Server-Sent Events encoding for streamed results.`

Depends on: PR 7. Size: about 400 lines.

* `SseEncoding`, which turns results into ready-to-send `data:` events, and a response type that
  forwards the batches and sends a keep-alive comment after 15 s without an event.
* Unit tests: the events match axum's own SSE rendering byte for byte, including escaping; the
  keep-alive timing.

#### PR 12. `feat(api-server): Validate streaming query requests.`

Depends on: PR 2. Size: about 250 lines.

* Turn a `QueryConfig` into a streaming query job config, rejecting what streaming doesn't
  support: an empty query, an invalid time range, `max_num_results`, MongoDB buffering,
  count-by-time, invalid dataset names.
* Unit tests for each rule.

#### PR 13. `feat(api-server): Add POST /query/stream to run a query and stream its results as Server-Sent Events.`

Depends on: PR 3, PR 5, PR 10, PR 11 and PR 12. Size: about 690 lines.

* Config: `api_server.search_result_listener` with the listener's `port` (default 3003) and
  `advertised_host`, in the Rust config and in the config template. The advertised host defaults
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

#### PR 14. `feat(clp-package): Add sbin/streaming-search.sh, backed by the clp-streaming-search tool.`

Depends on: PR 3, PR 5 and PR 10. Size: about 550 lines, about 200 of them tests.

* A new crate, `components/clp-package-cli`, for the Rust command-line tools shipped in the
  package, starting with `clp-streaming-search`. It is separate from the listener crate so the
  API server doesn't depend on a command-line crate.
* `clp-streaming-search` takes the same arguments as `sbin/search.sh` without the aggregation
  options: the query, `--dataset`, `--begin-time`, `--end-time`, `--ignore-case` and `--raw`. It
  also takes `--config`, and `--advertised-host` to override the address the query tasks connect
  to.
* It binds its own listener, opens a session, submits the job with the session's network output,
  and runs the session, polling the job's status every 100 ms. It prints each result as it
  arrives: `--raw` prints only the message; otherwise the archive ID and timestamp come first. It
  exits with 0 only if the job succeeded, and logs the final status otherwise. Ctrl-C requests the
  job's cancellation.
* It reads the package config (`etc/clp-config.yaml` by default) and the database credentials
  (`etc/credentials.yaml`) itself, and refuses to run unless the config has a `query_coordinator`
  section.
* `sbin/streaming-search.sh` runs `clp-streaming-search` in the package's runtime container,
  which uses the host's network. So the database address in the config works as written, and the
  query tasks connect back to the machine running the command, at its first non-loopback IPv4
  address unless `--advertised-host` is given.
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
