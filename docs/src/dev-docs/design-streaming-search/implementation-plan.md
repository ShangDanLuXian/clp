# Implementation plan: streaming search on Spider

Suggested tracking PR title:

```text
feat(query-coordinator)!: Stream search results from Spider search tasks to the API server and the CLI (implementation plan).
```

Target: two weeks, Monday 2026-10-26 to Friday 2026-11-06.

# Description

## Summary

Ship streaming search on the Spider query path. A search job's tasks stream their results over
TCP to a listener in the API server (`POST /query/stream`, Server-Sent Events) or in the
`sbin/streaming-search.sh` tool. Each result is delivered exactly once per task, even when Spider
reruns tasks. A task succeeds only after the listener acknowledges that it holds all of the task's
results (protocol v2).

The work lands as seven PRs against `y-scope/clp` `main`.

A complete reference implementation exists: `LinZhihao-723/clp` branch
`query-coordinator/streaming-search-api` at `63927f0`, which now includes protocol v2 (`63927f0`)
and the batch encoding (`4cc49f1`), about 8,700 lines including tests. It is a **reference, not a
port source**: it sits on the pre-squash base of #2554 (`ab2e560`), so it predates `main`'s
#2561, #2565, #2567 and #2573 and the error-code `OutputHandler` interface, and its code structure
is not the one the PRs should have. Each PR below names the reference files to read and the
places where `main` differs. This branch carries the reference's protocol v1 state merged onto
`main` (see [reproduction](reproduction.md)), which shows how those differences resolve.

References:

* The design doc (revised 2026-10-21: protocol v2 and result encoding) and its summary,
  [design-summary](design-summary.md).
* The review, [issues](issues.md), and the measurements behind it,
  [backpressure](backpressure.md) and [reproduction](reproduction.md).
* The prototype reports (CLI, API server integration, Rust reader integration and scaling).

## Scope

In scope:

* Protocol v2 in the C++ `clp-s` `network` output handler: session handshake, per-task result
  indices, timestamps, lazy connect, trailer, acknowledgement.
* Network output through the `query-coordinator` and the TDL search task, with a persisted task
  index per task.
* The `search-result-listener` library: exactly-once delivery, acknowledgement, batch encoding,
  shutdown and drain, discard mode.
* `POST /query/stream` in the API server, with cancellation on client disconnect through main's
  `CancelRequested` (#2573).
* `sbin/streaming-search.sh` (its shape is decision D1).
* Docs and the package config template.

Out of scope, by decision:

* **Celery.** Python is being deprecated. The Celery `sbin/search.sh` network output stops working
  with the new `clp-s` (see "Breaking changes"). Celery results-cache searches keep working.
* **Aggregation** (`--count`, `--count-by-time`) on the streaming path.
* **A retry policy for the job-status poll** (issue 6): a status-source error ends the session and
  cancels the job. Documented, not fixed.
* **Backpressure redesign** (issues 1, 4 and 21): documented as a known limitation.
* **Adversarial clients** (issue 8): the session token is not a security boundary.
* **The Rust in-process reader (V3).** This milestone streams from the C++ `clp-s`; V3 can follow
  on the same wire protocol.
* **Helm.** The Helm chart deploys no `query-coordinator`, so the endpoint answers 501 there.

## Assumptions

* `clp-s` is correct and deterministic: single-threaded, schemas in metadata order, rows in
  order. Position-based deduplication relies on this (issue 3); it is a precondition, not
  something this work verifies.
* One search engine per deployment (issue 22).
* The internal network between workers and the listener is trusted.

## Architecture recap

```text
caller: API server (POST /query/stream) or streaming-search tool
  ResultListener (one per process) ── Session (one per job: cursors, bounded batch channel)
        ▲  TCP, protocol v2: handshake → results → trailer ← acknowledgement
  Spider search task ── clp-s s ... network --host --port --session-token --task-index
        ▲
  query-coordinator: one task per archive, task_index = position in archives_to_search,
                     OutputHandle::Network from the job's network_output
```

## Work breakdown

Sizes are measured on the reference at `63927f0`. "Reference" names the files to read; "new"
marks work the reference does not cover.

Where the reference predates `main` (every PR must follow `main`, not the reference):

| Reference (`63927f0`) | `main` |
|---|---|
| `cancel_query_job` sets `Cancelling` directly | #2573: the API requests `CancelRequested`; the coordinator's cancellation monitor cancels the Spider job and moves the job to `Cancelling` and `Cancelled` |
| `QueryConfig.max_num_results: u32`, 0 meaning "all" | #2567: `Option<NonZeroU32>`; streaming requires it to be omitted |
| `ClpSQueryOption` built field by field in the job handle | #2561: `ClpSQueryOption::try_from(&SearchJobConfig)` |
| Coordinator and job-handle code from the pre-squash #2552/#2553/#2564 | The squashed versions on `main`, plus #2565 (unsupported jobs fail) and #2573 (cancellation monitor, a `cancel_query_job` submitter method) |
| `OutputHandler::write` returns `void` and the network handler throws | `write` returns `ErrorCode`; the network handler must return `ErrorCodeFailureNetwork` instead of throwing |
| The advertised host defaults through Python's `transform_for_container` | Python is being deprecated (decision D5) |

### PR 1. `feat(clp-rust-utils): Add network output types for streaming search; share query job submission and cancellation requests.`

Depends on: nothing. Reference: `clp-rust-utils/src/job_config/search.rs`, `task_io/query.rs`,
`database/mysql.rs`, `clp_config/package/config.rs`.

* `job_config::NetworkOutput { host, port, session_token }` and `SessionToken` (a `Uuid`
  serialized as a hyphenated string, so Python and the database see a string).
* `SearchJobConfig.network_address` → `network_output: Option<NetworkOutput>`. Mirror the rename
  in `job_orchestration/scheduler/job_config.py` and in the Celery task that reads the field, so
  Celery results-cache searches keep working.
* `task_io::query::OutputHandle::Network { host, port, session_token }` and `QueryTaskIndex = u64`.
* `database::mysql::submit_query_job`, moved from `api-server/src/client.rs`.
* `database::mysql::request_query_job_cancellation`: PENDING/RUNNING → `CancelRequested`, shared by
  `DELETE /query/{id}` and the streaming disconnect path (new; the reference's `cancel_query_job`
  predates #2573).
* Package config: `Package::scheduler` in the Rust mirror (not on `main` yet), and
  `ApiServer::search_result_listener { port: 3003, advertised_host }`, with the Python mirror and
  the commented template entry.
* Tests: serde round trips (msgpack, and against the Python mirror's field names), config defaults.
* Size: about +350/−20 Rust, +40 Python.

### PR 2. `feat(clp-s)!: Stream network search results with protocol v2 (session handshake, result indices, timestamps, trailer and acknowledgement).`

Depends on: nothing; runs in parallel with PR 1. Reference:
`core/src/clp_s/OutputHandlerImpl.{hpp,cpp}`, `clp-s.cpp`, `CommandLineArguments.cpp`,
`clp/networking/socket_utils.cpp`, and the Catch2 test
`core/src/clp_s/tests/test-clp_s-network_output_handler.cpp`.

* `--session-token` and `--task-index` required for `network` output; more than one input archive
  rejected.
* Lazy connect on the first result; handshake `[2, token, task_index, archive_id]`; results
  `[result_index, timestamp, message]`; metadata output always on.
* The trailer and acknowledgement live in `NetworkOutputHandler::finish()`, which
  `Output::filter()` calls after the scan: if a connection was opened, send `[num_results]`,
  `shutdown(SHUT_WR)`, read exactly 10 bytes (retrying `EINTR`), check `0x91 0xcf` and the count,
  close. Any failure returns `ErrorCodeFailureNetwork`, so `filter()` fails and `clp-s` exits
  non-zero. An archive without results never connects, so `finish()` returns success at once.
* `clp::networking::try_send`: short writes, `EINTR`, `MSG_NOSIGNAL` (shared with the reducer and
  `clo`).
* Follow `main`'s error-code `OutputHandler` interface: the reference's `write()` still throws and
  its `finish()` catches `OperationFailed` from `send_buffer()`; on `main` both return error codes
  (this branch's port of the v1 handler shows the shape).
* Tests: the reference's Catch2 test (frames, trailer, correct, malformed, short and mismatched
  acknowledgements, a peer that closes). It adds `OutputHandlerImpl`, `MongoDBUtils` and
  `ResultsCacheUtils` to the `clp_s` unit-test sources, which links `mongocxx` into the test
  binary; a reviewer may prefer splitting the network handler into its own translation unit
  first. The listener crate's real-`clp-s` test (PR 4b) covers the end-to-end path.
* Size: about +200/−40 C++ plus about 330 lines of tests.
* **Breaking:** `sbin/search.sh` in Celery mode can no longer read `clp-s`'s network output.

### PR 3. `feat(query-coordinator): Stream search results to a job's network output through the Spider search tasks.`

Depends on: PR 1. Reference: `query-coordinator/src/{coordination.rs,job_handle.rs}`,
`query_job_submitter/spider.rs`, `clp-tdl-package/src/task/query/search.rs`. The reference's
coordinator is the pre-squash version; apply its change (the output handle per job) to `main`'s
coordinator, as this branch's merge did.

* Coordinator: derive the output handle per job from its config (network output or results
  cache); move the results-cache URI into `QueryJobHandleContext`; fail a job that sets both a
  network output and `max_num_results` as `UnsupportedJobConfig` (new).
* Task graph: append each task's index (its position in `archives_to_search`) as a per-task input,
  declared `int64` in the TDL. Spider persists it, so reruns and a re-attached coordinator reuse it.
* TDL `query::clp_s_search`: accept `OutputHandle::Network` and the index; build the `clp-s`
  `network` argv.
* Optional (decision D4, new): `PR_SET_PDEATHSIG(SIGKILL)` on the spawned `clp-s`, so a hard
  timeout no longer leaves an orphaned producer (issue 20). Under v2 an orphan also blocks in
  `finish()` waiting for an acknowledgement until the listener closes its connection.
* Tests: argv building, input order, handle selection, rejection of `max_num_results`.
* Size: about +400/−100 coordinator, +200/−35 TDL.

### PR 4a. `feat(search-result-listener): Add the protocol v2 decoder and the per-task result cursor.`

Depends on: PR 1. Reference: `search-result-listener/src/{protocol.rs,cursor.rs,error.rs}`.

* The new crate's pure parts, no I/O: handshake, result and trailer frame decoding (generic
  integer reads with range checks; version checked before arity; a 1-element array is the
  trailer; 64-byte cap on handshake strings), the 10-byte acknowledgement encoding, and
  `TaskCursor` (one compare-and-swap per result; `claim` is infallible and asserts, because each
  connection checks its own order first).
* Error taxonomy as in the reference: protocol errors `UnexpectedResultIndex`,
  `ResultCountMismatch`, `DataAfterTrailer`, `ArchiveIdMismatch`, `UnknownSession`,
  `UnsupportedVersion`, `MalformedFrame`, invalid UUIDs; connection outcomes `MissingTrailer`,
  `TruncatedFrame`, `DrainTimedOut`, `HandshakeTimeout`, I/O.
* Workspace member, `dashmap` workspace dependency, CI path filter.
* Tests: the protocol and cursor unit tests, including truncated frames and out-of-range integers.
* Size: about 760 lines including the in-file tests (`protocol.rs` 615, `cursor.rs` 147).

### PR 4b. `feat(search-result-listener): Add the listener that delivers each streamed search result exactly once and acknowledges complete tasks.`

Depends on: PR 4a. Reference: `search-result-listener/src/{connection.rs,session.rs,listener.rs,
encoding.rs,job_status.rs}` and `tests/{listener.rs,clp_s.rs}`.

* `ResultListener`, `Session`, the connection task, and `JobStatusSource` with its MariaDB
  implementation.
* `ResultEncoding` / `ResultEncoder` and `SearchResultEncoding`. Connections encode the results
  they claim and send one batch per socket read (reads capped at 64 KiB) on a channel bounded in
  batches (default 16).
* Per socket read: claim the buffered frames, send the pending batch, *then* surface a protocol
  error from that read, so results claimed before the error are delivered.
* Acknowledgement: sent only when the read after the trailer returns EOF (the producer's
  half-close), which is after the connection's last batch was sent or discarded; so "nothing
  after the trailer" and "last batch delivered" hold by construction. Otherwise the connection
  closes without an ack. `MissingTrailer`, `TruncatedFrame`, `DrainTimedOut` and I/O errors count
  in `num_incomplete_connections`.
* Shutdown: poll to a terminal status, accept barrier, unregister, drain (zero grace once the job
  has succeeded, since every task was acknowledged; otherwise grace counted as socket-wait time
  since the last claim), close the stream, resolve the outcome. Discard mode after the stream is
  dropped. 10 s handshake timeout.
* Tests: the TCP integration suite (overlapping attempts, retries, unexpected indices, malformed
  frames, unknown sessions, slow consumers, the drain rules, discard mode, the acknowledgement
  cases) and the real-`clp-s` test (ignored unless `CLP_S_BINARY` is set).
* Size: about 1,500 lines of source and 2,000 lines of tests.

### PR 5. `feat(api-server): Add POST /query/stream to submit a search and stream its results as Server-Sent Events.`

Depends on: PR 1, PR 4b. Reference: `api-server/src/{sse.rs,streaming_search.rs,routes.rs,
client.rs}`.

* Bind the listener at startup in Spider mode only; answer 501 otherwise.
* `SseEncoding` and `SseResponse`: pre-encoded `data:` events byte-identical to axum's rendering,
  15 s keep-alive. The reference's oracle tests compare against axum itself.
* Validation before any insert: non-empty query, `begin <= end`, `max_num_results` omitted, no
  MongoDB buffering, no count-by-time, valid dataset names.
* Run the insert and the session in a spawned task (a disconnect during the insert must not orphan
  the job). Events: `job`, results, then `end` (status, emitted, duplicates dropped, protocol
  errors, incomplete connections) or `error`.
* Request cancellation (`CancelRequested`, new) when the stream is dropped before `end`, or when
  the outcome is an error. `DELETE /query/{id}` uses `main`'s transactional cancel; the reference's
  direct `Cancelling` update must not come back.
* `max_num_results` must be omitted (`main`'s `Option<NonZeroU32>`), not "0" as in the reference.
* Advertised host (decision D5) without the Python `transform_for_container` step.
* Regenerate the OpenAPI document and the web UI's TypeScript client schema.
* Tests: the route tests (validation, 501, events, disconnect → cancel, keep-alive, byte identity
  with axum).
* Size: `sse.rs` about 300 lines, `streaming_search.rs` about 330, the route handler and tests
  about 1,200.

### PR 6. `feat(clp-package): Add sbin/streaming-search.sh.`

Depends on: decision D1.

* Option A (recommended): a small Rust client of `POST /query/stream`. It needs no listener, no
  database credentials and no host networking, works wherever the API server is reachable, and
  Ctrl-C cancels the job by closing the stream. About 250 lines of Rust plus the shell script.
  Depends on PR 5.
* Option B: the reference's `clp-streaming-search` (`clp-package-cli`, about 430 lines, with its
  own `OutputEncoding`), which embeds the listener and is started by a Python wrapper (about 250
  lines). It keeps a Python layer that is being deprecated, and needs database credentials and
  host networking. Depends on PR 4b.
* Either way: `--raw` prints the message only; the default prefixes the archive ID and timestamp
  (decision D6 for the timestamp format); exit 0 only if the job succeeded.

### PR 7. `docs: Document streaming search.`

Depends on: PR 5, PR 6.

* User guide: the API section (from the prototype), the CLI, and the known limitations below.
* Config template comments for `api_server.search_result_listener`.
* Dev docs: the design, with this branch's review findings that were accepted as limitations.

## Dependency graph and critical path

```text
PR 1 ──┬── PR 3
       ├── PR 4a ── PR 4b ──┬── PR 5 ── PR 6 (option A) ── PR 7
       │                    └── PR 6 (option B)
PR 2 ──┘ (parallel; needed for package validation)
```

Critical path: PR 1 → PR 4a → PR 4b → PR 5 → PR 6 → validation. PR 2 and PR 3 run in parallel
and must be merged before the package validation in week 2.

## Schedule

| Day | Date | Track A (Rust) | Track B (C++ / coordinator) | Milestone |
|---|---|---|---|---|
| 1 | Mon 10-26 | Decisions D1 and D3-D6. Open PR 1. Start PR 4a. | Start PR 2. | Decisions recorded |
| 2 | Tue 10-27 | PR 1 review. Open PR 4a. | PR 2: trailer and ack. | |
| 3 | Wed 10-28 | PR 1 merged. Start PR 4b. | Open PR 2. Open PR 3. | PR 1 merged |
| 4 | Thu 10-29 | Open PR 4b. | PR 2 and PR 3 review. | |
| 5 | Fri 10-30 | PR 4a merged. Start PR 5. | PR 2 and PR 3 merged. Build a package from the full stack; harness smoke run. | Producer side merged |
| 6 | Mon 11-02 | PR 4b review. Open PR 5. | Harness updates (v2 statistics, throttled client). | |
| 7 | Tue 11-03 | PR 4b merged. Open PR 6. | Full harness run on a Docker host (stack branch). | Listener merged |
| 8 | Wed 11-04 | PR 5 review and merge. | Fixes from validation. | API merged |
| 9 | Thu 11-05 | PR 6 merged. PR 7. | Full harness run on `main`. | Feature complete |
| 10 | Fri 11-06 | Buffer: review follow-ups, release notes. | Buffer. | Done |

The schedule assumes two engineers (or one engineer plus the prototype author for review) and a
reviewer slot every day. PRs are opened as soon as their dependency is up for review, stacked on
it, and rebased when it merges.

## Known limitations shipped (documented, not fixed)

1. **Slow or stalled clients can fail the job.** Under v2 a task holds its worker slot until the
   listener has acknowledged its results, so its run time depends on the client's read rate.
   Spider's hard timeout (60 s + 0.5 s per MiB of the archive) then fails the task, and the job
   once retries run out. Measured under v1: a 270 s stall failed a job at 160.9 s. Issues 1 and 21.
2. **A failed job-status poll ends the stream.** The client receives `error` and the job is
   cancelled. Issue 6.
3. **With the C++ engine, a hard timeout orphans `clp-s`** unless D4 adds `PR_SET_PDEATHSIG`. The
   orphan keeps streaming; deduplication keeps the results exact; zombies accumulate until the
   worker restarts. Issue 20.
4. No `max_num_results` for streaming searches; no aggregation; Spider only; no Helm; no ordering
   across archives, nor within an archive when two attempts overlap; a listener crash is not
   survived.
5. The session token is not a security boundary.

## Breaking changes

* `clp-s`'s `network` output speaks protocol v2 and waits for an acknowledgement. The Celery
  `sbin/search.sh` (Python) can no longer read it. PR 2's title carries `!`.
* `SearchJobConfig.network_address` is replaced by `network_output` (internal job config).

## Risks

| Risk | Mitigation |
|---|---|
| Review bandwidth: about 8,700 lines including tests in seven to eight PRs in two weeks (the reference's size). | Split the listener (4a/4b); follow the reference's tested behaviour; open PRs early and stacked; a daily review slot. |
| The reference predates `main` (cancellation, result limits, `ClpSQueryOption`, the error-code `OutputHandler`), so copying it reintroduces old behaviour. | The table under "Work breakdown" lists each difference; reviewers check against it. This branch's merge onto `main` already resolved each one for protocol v1. |
| The reference's Catch2 test pulls `mongocxx` into the `clp_s` unit-test binary. | Agree on the test layout in PR 2's review; splitting the network handler into its own translation unit removes the dependency. |
| Upstream churn in the coordinator and the TDL (cancellation, V3). | Land PR 3 in week 1; keep it small. |
| C++ build and CI time for PR 2. | Start PR 2 on day 1 in parallel. |
| Validation only on a single WSL2 host. | The gates are correctness checks, not absolute timings. |

## Decisions needed on day 1

| # | Decision | Recommendation |
|---|---|---|
| D1 | Shape of `sbin/streaming-search.sh`: an SSE client of the API server, or the embedded listener with a Python wrapper. | The SSE client (Python is being deprecated; no host networking or credentials in the CLI). |
| D2 | Source of the v2 and encoding code. | Resolved: the reference at `63927f0` includes both; it is a behavioural reference, and each PR is written against `main`. |
| D3 | Engine for this milestone. | The C++ `clp-s`; V3 later on the same protocol (its adapter needs a v2 network sink). |
| D4 | `PR_SET_PDEATHSIG` on the spawned `clp-s` in PR 3. | Yes: a few lines, and it removes the orphaned producers. |
| D5 | Default advertised host for the API server's listener without the Python config transform. | Default to the API server's container hostname (`api_server`) in the Rust config when running in the package. |
| D6 | CLI timestamp format (design open question 1). | Epoch milliseconds, as the API returns. |

# Checklist

* [ ] The PRs satisfy the [contribution guidelines][yscope-contrib-guidelines].
* [ ] Breaking changes are indicated in the PR titles (PR 2), OR the PR isn't breaking.
* [ ] Necessary docs have been updated (PR 7), OR no docs need to be updated.
* [ ] Decisions D1 and D3-D6 are recorded in the tracking issue.
* [ ] Every gate under "Validation performed" passes on `main` after the last merge.

# Validation performed

Planned. Each PR runs the repository's own checks: `task lint:check-rust`, `cargo nextest run
--workspace`, the C++ build and the `clp_s` unit tests (including the network handler's Catch2
test) for PR 2, and the Python lint for the mirror changes. Before PR 4b and PR 5 merge, the
real-`clp-s` listener test runs locally with `CLP_S_BINARY` set.

Prior evidence from the reference: its fault test (client throttled to 40 MB/s, 16 workers, two
workers killed between 1 and 8 s) delivered exactly 1,875,605 results and SUCCEEDED in all 18
runs under v2, against 19,747 to 26,272 missing results per run under v1; the acknowledgement cost
about 40 µs per task. The gates below repeat the fault test on the PRs' code.

Package-level validation runs the harness in `tools/scripts/streaming-search-experiments/` on a
Docker host, first on the stacked branch (day 7) and then on `main` (day 9). Gates:

| Check | Gate |
|---|---|
| `correctness`: 3 streaming runs and 1 results-cache run against `clp-s`'s own results | Identical result multisets (sha256) in every run |
| `fault`: SIGKILL every running `clp-s` twice during a stall | Identical results, duplicates dropped > 0, 0 protocol errors, job SUCCEEDED |
| Throttled client (`curl --limit-rate 40M`) plus a killed worker (the recipe that found the v1 silent loss) | No missing results in 3 runs; job SUCCEEDED; `num_incomplete_connections` > 0 for the killed attempts |
| `disconnect`: client closes after the first result | `CancelRequested` within 1 s, then `Cancelled`; the Spider job stops and the workers go idle |
| Client disconnects before the job is dispatched | The job ends `Cancelled` (regression check for the API report's stuck-`CANCELLING` case) |
| `latency` | Time to first result recorded; well below the results-cache path |
| `stall` at 40 s and at more than twice the hard timeout | Recorded for the docs; not a gate (known limitation 1) |

Harness changes needed (day 6): read `num_incomplete_connections` from the `end` event, add a
throttled-client mode, and check that the Spider job is cancelled after a disconnect.

[yscope-contrib-guidelines]: https://docs.yscope.com/dev-guide/contrib-guides-overview.html
