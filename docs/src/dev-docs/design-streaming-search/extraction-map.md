# Streaming search: extraction map (internal)

This is our working map from the prototype to the PRs of the
[implementation plan](implementation-plan.md). It isn't meant for reviewers.

* **Source:** `LinZhihao-723/clp`, branch `query-coordinator/streaming-search-api`, at `63927f0`.
  The streaming work is `ab2e5600..63927f0`. Paths and line numbers below are at `63927f0`.
* **Target:** upstream `main`. Each PR branch is cut from the branches it depends on, so the
  reference branch is a merge of the leaf branches.
* **The prototype's base isn't an ancestor of `main`.** Move code, not commits, and adapt it to
  `main` as it is today.

## Rules for every PR

* **Method.** Start from the prototype's own text: apply its changes to `main` (for example
  `git diff ab2e5600 63927f0 -- <files> | git apply -3`), resolve conflicts by keeping both sides
  where they don't overlap, then make only the changes an adjustment below calls for. Don't
  reword, restyle or add code beyond that. Add a test only where an adjustment changes behavior.
  Check the result by diffing it against the prototype's changes applied to `main`.
* **The style guide is stricter than the prototype.** Where Lin's Rust guide
  (`LinZhihao-723/claude-instruction`, `rust/coding-style.md`) disagrees with the prototype's
  text, the guide wins. In particular:
  * test only hand-written logic, once, where it's defined; no tests of derived serde behavior;
  * a new test must fail when the behavior it covers is deliberately broken;
  * a hand-written serde adapter (for example a `with` attribute) is covered by a test, without a
    comment explaining it;
  * Rust code a PR adds or changes doesn't refer to Python, not even in docstrings; existing
    "Mirror of `job_orchestration…`" docstrings the PR doesn't otherwise touch stay as they are; a
    type whose mirror docstring a PR removes gets a short docstring of its own;
  * don't add docstrings or comments that explain how the new feature behaves unless asked; only
    update docs that the change makes stale.
* **Don't edit a PR branch.** Put review fixes and any other change for an open PR on a separate
  reference branch, as new commits on top of the PR's head; the PR's author applies them. Don't
  revert anything.
* **Force-push only when asked,** and only to a reference branch or to a PR branch that nobody
  except the agent reviewer has reviewed. Never force-push a PR branch that someone has left a
  review on.
* **PR text.** Don't hard-wrap PR descriptions or issue bodies: one line per paragraph or bullet.
* **Commits.** Each PR is two commits, the implementation and then its tests. Each one builds and
  passes the checks on its own. The author and committer are Xuhui Chen
  `<xuhui.chen@yscope.com>`, and there are no Claude lines in the messages.
* **Naming.** `search-result-listener` → `query-result-listener`, `search_result_listener` →
  `query_result_listener`, `SearchResult*` → `QueryResult*`, `StreamingSearch*` →
  `StreamingQuery*`, and "search task" → "query task" in docs and logs.
* **Protocol version 1** (the prototype uses 2).
* **No Python.** Leave out every `.py` change and the config template change:
  `fs_search_task.py`, `job_config.py`, `native/search.py`, `streaming_search.py`,
  `clp_config.py` and `clp-config.template.json.yaml`. The one exception is PR 3a's change to the
  query tables in `initialize-orchestration-db.py`, which no Rust code can make instead.
* **Breaking changes are fine** for now, including databases that must be recreated.
* **Ignore Celery.** Treat the Celery query path as if it didn't exist: don't design around it,
  and don't qualify names with "Spider" to tell the two paths apart.
* **Adapt to `main`:**
  * `SearchJobConfig` (until #2590), `DatasetName`, and `max_num_results: Option<NonZeroU32>`;
  * `QueryJobStatus` with `CancelRequested`, and cancellation through PR 3's function;
  * the `OutputHandler` methods of `clp-s` returning `ErrorCode`;
  * `ClpSQueryOption`.
* **Left out everywhere:**
  * the accept barrier;
  * the per-task index input;
  * the `Package.scheduler` mirror (gate on `query_coordinator` instead);
  * `credentials::Database::from_env`;
  * the real-`clp-s` test, `tests/clp_s.rs`. We still run it ourselves on the reference branch.
* **Unrelated edits to leave out:** the lowercased `HandlerError` messages, the `cancel_search_job`
  rewrite, and the `.expect` added to `Client::submit_query`.
* **Tests.** `search-result-listener/tests/listener.rs` is split across PRs 7 to 9, as listed
  below. Shared fakes go into the listener's `test-utils` feature instead of being copied into the
  API server.

## Per PR

### PR 1. `clp-s` network protocol

* **Take:**
  * `components/core/src/clp_s/OutputHandlerImpl.{hpp,cpp}`: the network handler.
    * `.hpp:88-102`;
    * `.cpp:47-50`, the acknowledgement constants;
    * `.cpp:62-81`, `receive_exact`;
    * `.cpp:102`, metadata always on;
    * `.cpp:118-131`, the handshake and the results;
    * `.cpp:139-174`, `finish()`;
    * `.cpp:231-257`, the acknowledgement check.
  * `CommandLineArguments.{hpp,cpp}:820-829, 913-915, 947-959, 1172-1190`.
  * `clp-s.cpp:401-406` (keep it only if something can still throw).
  * `clp/networking/socket_utils.{hpp,cpp}:19-25, 65-85`.
  * `clp_s/CMakeLists.txt:561-573`.
  * `clp_s/tests/test-clp_s-network_output_handler.cpp`.
* **Adjust:**
  * the `ErrorCode` returns, replacing `OperationFailed`;
  * version 1;
  * sticky failure;
  * `SO_NOSIGPIPE` where `MSG_NOSIGNAL` is missing, including in the test;
  * golden bytes instead of expected bytes built with the same packer;
  * a failed-connection test.
* **Status:** redone by the method on branch `streaming-search/clp-s-network-output`, commits
  `1161c27` (implementation) and `2e5eb58` (tests). The old own-words branch,
  `streaming-search/clp-s-network-protocol`, is superseded. `SO_NOSIGPIPE` is set in
  `connect_to_server`; the prototype's `try`/`catch` around `Output::filter` is left out, since
  the handler no longer throws. To build locally: symlink the worktree's `build/deps` to the
  shared deps, set `PKG_CONFIG_PATH` to the static LibLZMA's `pkgconfig` directory, and configure
  with `CLP_USE_STATIC_LIBS=OFF` (the system's static `libarchive` doesn't link here).

### PR 2. Network output types

* **Take** the prototype's changes to `clp-rust-utils/src/job_config/search.rs` and
  `task_io/query.rs`.
* **Adjust:**
  * `SessionToken` becomes a `#[serde(transparent)]` newtype in `types.rs`, next to `ArchiveId`
    and serialized like it, instead of a `Uuid` alias. It has `From<uuid::Uuid>` and `Display`, so
    the prototype's later code (`session_token.to_string()`, `*session_token`) works unchanged.
    Later PRs import it from `clp_rust_utils::types`, and code that builds a token from a `Uuid`
    (PRs 8 and 10) wraps it with `SessionToken::from`. The listener PR adds `FromStr` for the
    handshake, following `ArchiveId`.
  * `OutputHandle::Network(NetworkOutput)` instead of the struct variant, so the prototype's
    `From<NetworkOutput> for OutputHandle` goes away: the coordinator (PR 10) uses
    `network_output.map(OutputHandle::Network)` where the prototype has `OutputHandle::from`. The
    msgpack layout is unchanged.
  * Leave out `QueryTaskIndex`.
  * Docs: no Python `NetworkOutput` mirror, "query tasks", and `SearchJobConfig`'s Python mirror
    docstring replaced by its own.
  * Tests: none; the prototype's round-trip tests only check derived serde behavior.
* **Status:** y-scope/clp#2613, branch `streaming-search/network-output`. Review addressed in
  `5996665`, `09e7bd6`, `7630da7` and `2a14922`; approved by the reviewer
  session. Lin's review: `deny_unknown_fields` on `NetworkOutput` (`a142a7a`, his suggestion);
  the docstring and the `SessionToken` change are `3c3444f` ("Address PR comments."). The output
  enum is a follow-up issue. Merged into `main` as `095e90ee`.

### PR 3a. Query job IDs as `u64`

* **New code**, not from the prototype: `QueryJobId = u64`, `query_jobs.id` and
  `query_tasks.job_id` as `BIGINT UNSIGNED`, the coordinator's job ID task input declared as
  `int64`, the WebUI query-speed route's `value_type = u64`, and the API server using
  `QueryJobId` (taken from PR 3's former first commit, `7936db5`, with `u64`).
* **Why:** sqlx returns an inserted row's ID as a `u64` and decodes `u64` only from unsigned
  columns, so with `u64` everywhere no code converts query job IDs. Spider has no unsigned types.
* **No upgrade of existing databases:** `query_tasks` is the Celery path's table, and Celery is
  treated as removed. The upgrade written in `df0c9a0` was reverted in `37a33d1`.
* **Status:** branch `streaming-search/query-job-id-u64`: `4df8ff7` (types and schema), `df0c9a0`
  and its revert `37a33d1`, and `5cfeeb8` (the API server). The OpenAPI document is unchanged.
  Whichever of it and PR 3 merges second needs a small merge in the API server's `client.rs`.
  Checked by hand on MariaDB 10.11: the new tables and their foreign key, `last_insert_id` as
  `QueryJobId`, decoding `id` as `u64`, an ID above `u32::MAX`; on the old `INT` columns, the
  coordinator's reads fail to decode `id` as `u64`, so it can't start.

### PR 3. Shared query-job operations

* **Take:**
  * `clp-rust-utils/src/database/mysql.rs:97-112`, the insert;
  * `mysql.rs:127-143`, the cancellation;
  * the status read from the API server's `Client::get_status`.
* **Adjust:** cancellation with upstream's semantics (`CancelRequested`, not found, already
  terminated), not the prototype's direct `Cancelling`. The functions live in
  `database::mysql::query_job`, and `Cancellability` in `job_config`. They take the API server's
  `u64` IDs, and the API server keeps `main`'s ID types, so nothing converts IDs; the ID type is
  PR 3a's.
* **Status:** y-scope/clp#2617, branch `streaming-search/shared-query-job-operations`: `7936db5`
  (API server IDs) and `c3c3703` (the move), `main` merged in (`524c4ff`), `3424554` and `5120a9b`
  (`u64`, by mistake), Lin's review in `4735be1` (`request_cancellation`, `get_status` returning
  `None` and the API server answering `404`, the `Cancellability` test), and `main` merged in
  again (`1bfb16b`). On the reference branch `streaming-search/shared-query-job-operations-review`
  and not yet on the PR: `6929c1d` restores the test imports the second merge broke. `e43ecde`
  after it moves the API server to `QueryJobId` and isn't to be applied, since the API server keeps
  `main`'s `u64`.

### PR 4. Streaming job config validation

* **Take** the job-config rules of `api-server/src/client.rs:251-311`
  (`into_streaming_search_job_config`):
  * an empty query;
  * begin after end;
  * `max_num_results`;
  * aggregation.
  
  The CLI's own check of begin after end (`clp_streaming_search.rs:169-178`) is replaced by this.
* **Leave for PR 14:** the API-only rules, MongoDB buffering and count-by-time. Dataset names
  are already checked by `DatasetName`.
* **Adjust:** the rules move from the API server's `QueryConfig` into `clp-rust-utils`:
  * `SearchJobConfig::validate`, shared with the query coordinator: no aggregation, no
    `write_to_file`, a non-empty query, and begin <= end, in the coordinator's order and with its
    messages. `QueryJobHandle::new` calls it, and its errors map to the coordinator's old `Error`
    variants.
  * `StreamingQueryJobConfig::validate_and_create` adds `max_num_results` must be unset (`main`'s
    `Option<NonZeroU32>`), and `into_search_job_config` sets the network output after validation.
* **Status:** y-scope/clp#2627, branch `streaming-search/streaming-job-config-validation`:
  `a9d468e` and `63fb355`, then review fixes `57b80be` and `7e3a6b6`, and the rename `8b7116a`.

### PR 5. Listener crate and wire protocol

* **Take:**
  * `search-result-listener/src/protocol.rs`, the whole file: incremental decoding at `:193-220`
    and `:241-255`, the version checked first at `:51-55`, and the string caps at `:104` and
    `:176`;
  * the protocol errors of `src/error.rs:32-72`;
  * `lib.rs` and `Cargo.toml`;
  * the workspace members and the `bytes` and `rmp` dependencies in the root `Cargo.toml`;
  * the CI path filter.
* **`test-utils`:** the encoders of `tests/listener.rs:324-386`: `ack`, `encode_handshake`,
  `encode_result(s)` and `encode_trailer`.
* **Tests:** the 10 unit tests of `protocol.rs`.
* **Adjust:** rename the crate; version 1; only the error variants this PR needs.
* **Status:** branch `streaming-search/query-result-listener-protocol` (`dd9ecd1`) was written
  fresh, not moved. Redo it by the method above.

### PR 6. Cursor and encodings

* **Take** `src/cursor.rs` and `src/encoding.rs`, the whole files, with their 5 unit tests.
* **Adjust:** `SearchResult*` → `QueryResult*`.

### PR 7. Connection handler

* **Take** from `src/connection.rs`:
  * the read loop of `run`, `:214-280`, without the draining branch at `:241-259`;
  * claiming and batching, `:333-351`;
  * the trailer and the acknowledgement, `:264-273` and `:312-318`;
  * discard mode, `:358-371`;
  * the statistics in `Drop`, `:374-378`.
  
  Also the slice of the session's state the handler uses: the cursors, the batch sender and the
  counters.
* **Adjust:**
  * make it generic over `AsyncRead + AsyncWrite`; the prototype uses `TcpStream` at `:39`, `:217`
    and `:397`;
  * remove its use of the listener's pending-handshake counter (`:23`).
* **Tests,** moved from `tests/listener.rs` and rewritten over `tokio::io::duplex`:
  * single attempt and ordering: `:432`, `:463`;
  * retries and overlapping attempts: `:498`, `:535`, `:566`;
  * protocol errors: `:603`, `:641`, `:672`, `:702`, `:742`, `:766`, `:885`, `:917`, `:946`;
  * batching and slow consumers: `:972`, `:1004`;
  * discard mode: `:1426`, `:1471`.

### PR 8. TCP listener and session registry

* **Take:**
  * from `src/listener.rs`: `bind`, `:89-135`, without the dup'd file descriptor at `:93-94`; the
    accept loop, `:253-276`, without the barrier requests; and the advertised host, `:328-345`;
  * from `src/session.rs`: the registry, `:35`; `Registration`, `:356-360`; `open_session`; and
    `State`, `:263-270`, made `pub(crate)`;
  * from `src/connection.rs`: `serve`, `:38-117`, the handshake read and the join, `:167-177`.
* **Leave out:** `AcceptBarrier` (`listener.rs:186-199`), `PendingHandshake`, the
  pending-handshake `watch`, and `accept_queued_connections` (`:279-301`).
* **`test-utils`:** `FakeClpS` from `tests/listener.rs:178-322`.
* **Tests:**
  * `:805` and `:826`: the version test now sends an unsupported version instead of version 1;
  * `:860`, `:1291`, `:1361`, `:1375`.

### PR 9. Finishing a session

* **Take:**
  * from `src/session.rs`: `run`, `:142`; `drive`, `:379-407`, without the barrier wait at `:388`;
    the status polling, `:424-430`; the statistics, `:319-330`; and the outcome types;
  * from `src/connection.rs`: draining, `:241-259`, and the zero grace period after success,
    `:290-296`;
  * `src/job_status.rs`, with its query replaced by PR 3's status read.
* **`test-utils`:** `FakeJobStatusSource` from `tests/common/mod.rs`.
* **Tests:**
  * `:1070`, `:1104`, `:1163`, `:1186`, `:1230`, `:1307`, `:1321`, `:1337`, `:1516`;
  * `:1271` tests the barrier and is left out.
* **Optional trim:** the debug-only flags `has_joined_connection` and `has_claimed_result`
  (`session.rs:276-277, 303-316`).

### PR 10. Coordinator and Spider query task

* **Take:**
  * from `query-coordinator/src/job_handle.rs`: the output selection, `:148-153`, and its tests,
    `:914` and `:936`;
  * from `clp-tdl-package/src/task/query/search.rs`: the match on the output handle, `:98-119`;
    the network arguments, `:231-252`; the shared argument builder, `:260-291`; and the tests,
    `:625` and `:668`.
* **Adjust:**
  * take the task index from `ctx.task_id` (`TaskId::Index`);
  * fail a network output that also sets `max_num_results` in the job handle (the prototype passes
    it through at `:142`);
  * follow upstream's current shapes of `job_handle.rs` and `coordination.rs`. Upstream still
    passes `output_handle` into the handle; the prototype moved the results-cache URI into the
    context instead (`coordination.rs:143`).
* **Leave out:**
  * the per-task input in `query_job_submitter/spider.rs:148, 165-167, 184` and its test at
    `:207`;
  * `ArchiveToSearch` and the new task parameter in the TDL's `mod.rs`.

### PR 11. CLI crate and config loading

* **New code.** The prototype reads the config that Python generates.
* **Reference:**
  * Python's `load_config_file`;
  * `resolve_host_path_in_container`, in `clp_py_utils/core.py:79-143`;
  * the credentials file format.
* **Note:** the Rust `Config` can't read a user's file directly. `Package.storage_engine` defaults
  to `clp` (Python defaults to `clp-s`), and `Spider` requires `host` and `port`. So use a small
  struct of the fields the CLI needs.

### PR 12. `clp-streaming-search` and `sbin/streaming-search.sh`

* **Take:**
  * `clp-package-cli/Cargo.toml`;
  * from `src/bin/clp_streaming_search.rs`: the arguments, `:41-73`; the output encoding,
    `:85-111`; the exit codes, `:133-145`; the search, `:169-230`; the output loop, `:282-289`; and
    the tests, `:306-434`;
  * `package-template/src/sbin/streaming-search.sh`;
  * `tools/docker-images/clp-package/Dockerfile:44`.
* **Adjust:**
  * load the config with PR 11 instead of a generated config;
  * check the job config with PR 4;
  * `DatasetName`, and `max_num_results: None`;
  * cancel the job on `SIGINT`, `SIGTERM` and a closed output (the prototype only exits, at
    `:138-145`);
  * write stdout asynchronously;
  * the `sbin` script runs the binary directly in `clp-runtime`.

### PR 13. SSE encoding

* **Take:**
  * `api-server/src/sse.rs`, the whole file: the encoder, `:38-89`; the response, `:111-131`;
    `encode_event`, `:151-162`; and the keep-alive, `:164-219`, with its tests at `:248` and
    `:273`;
  * the result event struct, `routes.rs:253-264`;
  * the byte-compatibility tests `routes.rs:1584` and `:1627`, with their helpers at `:1532-1583`.
* **Adjust:** run the keep-alive tests on paused time.

### PR 14. Streaming query layer

* **Take:**
  * `api-server/src/streaming_search.rs`, the whole file: `QueryJobTable`, `:29-70`; the database
    implementation, `:72-110`, rebuilt on PR 3; `submit`, `:154-183`; the stream and its `Drop`,
    `:259-284`; and `cancel_and_wait`, `:295-333`;
  * the API-only rules of `client.rs:251-311`;
  * `FakeQueryJobTable`, `routes.rs:991-1213`.
* **Adjust:**
  * `QueryJobTable::cancel` returns PR 3's result instead of a `bool`;
  * the tests run on the stream instead of through the router.
* **Tests:**
  * the happy path, `routes.rs:1782`;
  * drops and status-read failures: `:1877`, `:1918`, `:1944`, `:1969`, `:1997`;
  * validation, `:1717`;
  * `:1899` is covered by PR 7's discard-mode tests, so it is left out.

### PR 15. `POST /query/stream`

* **Take:**
  * from `routes.rs`: the handler, `:330-385`; the event structs, `:253-297`; and the route;
  * from `client.rs`: the listener startup, `:373-396`, moved out of `Client` into the API
    server's startup;
  * from `clp-rust-utils/src/clp_config/package/config.rs`: `ApiServer.search_result_listener`,
    `:225-247`, renamed;
  * the OpenAPI document and `schema.ts`, regenerated;
  * the API server guide section.
* **Adjust:**
  * gate on `query_coordinator` instead of `Package.scheduler` (`config.rs:277-308`, left out);
  * the advertised host defaults to the container's hostname.
* **Tests:**
  * the response body byte for byte, `routes.rs:1659`;
  * 501, `:1768`, through `from_client`;
  * the closed client connection, `:2036`.
