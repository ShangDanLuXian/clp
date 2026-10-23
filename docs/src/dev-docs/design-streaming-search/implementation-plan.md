# Streaming search: implementation plan

Tracking PR title:

```text
feat(clp-package)!: Stream search results from Spider search tasks to the API server and the CLI.
```

Timeline: two weeks, Monday 2026-10-26 to Friday 2026-11-06.

# Description

## Summary

Today a search job on Spider writes its results to the results cache (MongoDB), and a client can
read them only after the whole job has finished. This work adds **streaming search**: the job's
search tasks send their results over TCP to a listener in the caller as they find them, and the
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

* the `network` output of `clp-s`, which each search task uses to send its results;
* the `query-coordinator` and the Spider search task, which pass the network destination to each
  task;
* a new Rust library, `search-result-listener`, that receives and deduplicates the results;
* `POST /query/stream` in the API server, including cancelling the job when the client
  disconnects;
* `sbin/streaming-search.sh`;
* user docs and the package config template.

Out of scope:

* aggregations (`--count`, `--count-by-time`), which stay on the results-cache path;
* result limits (`max_num_results`) for streaming searches: a streaming search returns every
  result;
* the Celery query path and its `sbin/search.sh`;
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
Spider search task (one per archive) ─ clp-s s <archive> <query> network --host --port
  ▲                                         --session-token --task-index
query-coordinator: builds one task per archive and gives each task a fixed index
```

**Submitting a job.** The caller opens a listener session first, which creates a random session
token. It then inserts the search job with a network output: the listener's address and the
token. The `query-coordinator` creates one Spider task per archive and gives each task a fixed
index (its position in the list of archives). Spider stores the task graph, so a rerun of a task
gets the same index.

**Sending results.** Each task runs `clp-s` with a `network` output. `clp-s` connects to the
listener when it finds its first result, so an archive without results never opens a connection.
It sends four kinds of messages, each a msgpack array:

| Message | Sent by | Content |
|---|---|---|
| Handshake | `clp-s`, once | `[2, session_token, task_index, archive_id]` (2 is the protocol version) |
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

The work lands as seven pull requests against `main`. Sizes are rough estimates, including tests.

### PR 1. `feat(clp-rust-utils): Add network output types for streaming search; share query job submission and cancellation requests.`

Depends on: nothing. Size: about 400 lines.

* `NetworkOutput { host, port, session_token }`, and a `network_output` field in
  `SearchJobConfig` replacing the unused `network_address`. The token is a UUID stored as a string,
  so the Python mirror of the job config and the database see a readable value. Update the Python
  mirror in `job_orchestration` so the existing search paths keep reading the job config.
* `OutputHandle::Network { host, port, session_token }`, the task's output destination.
* Move query job submission (`INSERT INTO query_jobs`) from the API server into
  `clp-rust-utils`, so the API server and the CLI share it.
* A shared helper that requests cancellation of a pending or running job (`CancelRequested`, the
  state the coordinator's cancellation monitor from #2573 acts on).
* Package config: `package.scheduler` in the Rust config, and
  `api_server.search_result_listener` with the listener's `port` (default 3003) and
  `advertised_host`, in Rust, in Python, and in the config template.

### PR 2. `feat(clp-s)!: Send network search results with a handshake, result indices, timestamps, a trailer and an acknowledgement.`

Depends on: nothing; runs in parallel with PR 1. Size: about 550 lines, including a Catch2 test.

* `clp-s s ... network` requires `--session-token` and `--task-index`, and accepts a single
  archive.
* Connect when the first result is found; send the handshake and the results as described above;
  always include each result's timestamp.
* In the output handler's `finish()` step, which runs after the search, send the trailer, stop
  writing, wait for the acknowledgement, check it, and close. Any failure makes the search fail
  and `clp-s` exit with an error.
* Make sending robust: retry partial writes and interrupted calls, and don't let a closed
  connection kill the process with `SIGPIPE`.
* A Catch2 test with a local socket covering the messages, the trailer, a correct, malformed or
  mismatched acknowledgement, and a listener that closes the connection early.

### PR 3. `feat(query-coordinator): Send search results to a job's network output through the Spider search tasks.`

Depends on: PR 1. Size: about 600 lines.

* Choose each job's output from its config: the network output if set, otherwise the results
  cache.
* Reject a job that sets both a network output and `max_num_results`.
* Add each task's index to the Spider task graph as a per-task input.
* In the Spider search task, accept the network output and the index, and build the `clp-s`
  command line for it.
* Tests: the command line, the task inputs, and the output selection.

### PR 4a. `feat(search-result-listener): Add the wire protocol decoder and the per-task result cursor.`

Depends on: PR 1. Size: about 800 lines.

* A new crate, `components/search-result-listener`, starting with the parts that do no I/O:
  decoding the handshake, result and trailer messages, encoding the acknowledgement, and the
  per-task cursor.
* Clear protocol errors: unknown session, archive ID that doesn't match the task, unexpected
  result index, count mismatch, data after the trailer, unsupported version, malformed message.
* Unit tests for the decoder and the cursor, including cut-off messages and out-of-range values.

### PR 4b. `feat(search-result-listener): Add the listener that delivers each search result exactly once.`

Depends on: PR 4a. Size: about 3,500 lines, about 2,000 of them tests.

* `ResultListener` (the TCP listener, shared by all jobs in a process), `Session` (one per job),
  and the per-connection task.
* A pluggable output format (`ResultEncoding`), so each caller receives ready-to-send batches.
* The acknowledgement rules, the shutdown sequence described under "How it works", and a 10 s
  limit for a new connection to send its handshake.
* A job-status source that polls the database, behind a trait so callers and tests can supply
  their own.
* Integration tests over real TCP: overlapping attempts of one task, reruns, protocol errors,
  slow clients, shutdown, client disconnects and the acknowledgement cases; plus a test against a
  real `clp-s` binary that runs when `CLP_S_BINARY` is set.

### PR 5. `feat(api-server): Add POST /query/stream to run a search and stream its results as Server-Sent Events.`

Depends on: PR 1 and PR 4b. Size: about 1,800 lines, most of them tests.

* Start the listener with the API server when the package runs the Spider scheduler; otherwise the
  endpoint answers 501.
* Check the request before creating a job: a non-empty query, a valid time range, no
  `max_num_results`, no MongoDB buffering, no count-by-time, valid dataset names.
* The response is an SSE stream: a `job` event with the job ID, one event per result
  (`archive_id`, `timestamp`, `message`), and a final `end` event with the status and the
  statistics, or an `error` event if the job's status can't be determined.
* If the client disconnects before the `end` event, request the job's cancellation.
* Default the listener's advertised host to the API server's container hostname, so workers can
  reach it without extra configuration.
* Regenerate the OpenAPI document and the web UI's API client.

### PR 6. `feat(clp-package): Add sbin/streaming-search.sh.`

Depends on: PR 5. Size: about 300 lines.

* A small Rust command-line client of `POST /query/stream`. It takes the same arguments as
  `sbin/search.sh` without the aggregation options, prints each result (`--raw` prints only the
  message; otherwise the archive ID and timestamp come first), and exits with 0 only if the job
  succeeded.
* Pressing Ctrl-C closes the stream, which cancels the job.

### PR 7. `docs: Document streaming search.`

Depends on: PR 5 and PR 6.

* The API server guide: the request, the events, and what streaming search doesn't support.
* The CLI usage.
* Comments for `api_server.search_result_listener` in the config template.

## Order of work

```text
PR 1 ──┬── PR 3
       └── PR 4a ── PR 4b ── PR 5 ── PR 6 ── PR 7
PR 2 (in parallel)
```

The longest chain is PR 1 → PR 4a → PR 4b → PR 5 → PR 6. PR 2 and PR 3 run alongside it and must
be merged before the end-to-end testing in week 2.

## Schedule

| Day | Date | Rust track | C++ and coordinator track | Milestone |
|---|---|---|---|---|
| 1 | Mon 10-26 | Settle the open questions. Open PR 1. Start PR 4a. | Start PR 2. | |
| 2 | Tue 10-27 | Review PR 1. Open PR 4a. | Finish PR 2. | |
| 3 | Wed 10-28 | Merge PR 1. Start PR 4b. | Open PR 2 and PR 3. | PR 1 merged |
| 4 | Thu 10-29 | Open PR 4b. | Review PR 2 and PR 3. | |
| 5 | Fri 10-30 | Merge PR 4a. Start PR 5. | Merge PR 2 and PR 3. Build a package with all open PRs and run a first end-to-end check. | Task side merged |
| 6 | Mon 11-02 | Review PR 4b. Open PR 5. | Prepare the end-to-end tests. | |
| 7 | Tue 11-03 | Merge PR 4b. Open PR 6. | Run the end-to-end tests on the combined branch. | Listener merged |
| 8 | Wed 11-04 | Review and merge PR 5. | Fix what the tests found. | API merged |
| 9 | Thu 11-05 | Merge PR 6. Write PR 7. | Run the end-to-end tests on `main`. | Feature complete |
| 10 | Fri 11-06 | Spare day for review follow-ups and release notes. | | Done |

The schedule assumes two engineers and a reviewer available every day. Each PR is opened as soon
as the PR it depends on is up for review, and rebased when that one merges.

## Breaking changes

* The `network` output of `clp-s` uses the new protocol and waits for an acknowledgement, so
  programs that read the old format can no longer use it. This includes `sbin/search.sh` on the
  Celery query path. PR 2's title is marked with `!`.
* `SearchJobConfig.network_address` is replaced by `network_output`. It is an internal field that
  nothing reads today.

## Risks

| Risk | Mitigation |
|---|---|
| About 7,000 to 8,000 lines to review in two weeks. | Split the listener into two PRs; open PRs early and stack them; reserve a reviewer slot every day. |
| Other work on the `query-coordinator` and the Spider search task conflicts with PR 3. | Land PR 3 in the first week and keep it small. |
| The C++ build and tests make PR 2 slow to iterate on. | Start PR 2 on day 1, in parallel with the Rust work. |

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
* [ ] Necessary docs have been updated (PR 7).
* [ ] The open questions are answered in the tracking issue.
* [ ] Every end-to-end test below passes on `main` after the last merge.

# Validation performed

Planned.

Each PR runs the repository's checks: `task lint:check-rust`, `cargo nextest run --workspace`,
and, for PR 2, the C++ build and the `clp_s` unit tests. Before PR 4b and PR 5 merge, the
listener's test against a real `clp-s` runs locally.

End-to-end tests, run on a package deployed with the Spider scheduler, first on a branch that
combines all PRs (day 7) and then on `main` (day 9):

| Test | Passes when |
|---|---|
| The same query through `POST /query/stream` (3 times), through the results cache, and with `clp-s` directly | All five result sets are identical |
| Kill the running `clp-s` processes twice during a streaming search | The results are identical to `clp-s`'s own results, some duplicates were dropped, and the job succeeds |
| A slow client (`curl --limit-rate 40M`) while a worker is killed | No results are missing in 3 runs, and the job succeeds |
| The client disconnects after the first result | The job is cancelled within about a second, and its Spider tasks stop |
| The client disconnects before the job starts | The job ends as cancelled |
| Time to the first result | Recorded; well below the time to the first result through the results cache |

[yscope-contrib-guidelines]: https://docs.yscope.com/dev-guide/contrib-guides-overview.html
