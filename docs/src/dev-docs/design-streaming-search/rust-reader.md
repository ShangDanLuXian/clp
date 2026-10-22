# The Rust reader prototype and V3

This page is the discussion basis for the Rust-reader report's V3 ("Spider + Rust in-process
reader"). It records what of that work is on this branch, what is not, where it would attach to
the streaming path, and the points the streaming design has to settle before adopting it.

## What is on this branch

Merged from `LinZhihao-723/clp` branch `clp-s-rust-reader-prototype` (4 commits on upstream
`39b8fa3`, 720 new files, all under `components/core/rust/`):

* `e1459b5` the Rust prototype of the clp-s archive reader (search and unordered decompression of
  single-file archives);
* `fa5e4c2` the docs split into design, prototyping-and-benchmark, and API overview;
* `487717c` a Log Archival Bench query harness with C++ vs Rust results on the MongoDB dataset;
* `509e12b` the full read-path architecture in the design doc.

It is a **standalone Cargo workspace** (`components/core/rust/Cargo.toml`, 18 crates), not a
member of the repository workspace, and it depends on `clp-rust-utils` by path. Its own docs are
`components/core/rust/README.md`, `docs/design.md` (architecture, prototype design, search
semantics and the divergence registry), `docs/prototyping-and-benchmark.md` (conformance against
the C++ `clp-s`, every gate green as of 2026-10-18) and `docs/api-overview.md`.

Scope, as the README states it: single-file archives of format 0.5.x only; no kv-IR search;
unordered extraction and search; outputs are JSON lines, msgpack file/network tuples, and the
MongoDB results cache; archives from local files, memory or S3; and an in-process Spider search
task.

The pieces that matter for streaming:

| Crate | What it holds | Relevance |
|---|---|---|
| `clp-s-reader`, `clp-s-exec`, `clp-s-search`, `clp-s-table`, ... | The reader: planning, pruning, vectorized filters, sessions and the sink protocol | The engine V3 runs in-process |
| `clp-s-spider-adapter` | `search(ctx, config, query_job_id, clp_s_query_option, archive_id, dataset, output_handle)`: an in-process Spider search task with process-wide resources (`OnceLock`), `catch_unwind` at the task boundary, a memory-budget admission wait, S3 or staging-directory archive resolution, and a fallback that spawns the C++ `clp-s` on `UnsupportedArchive` before any write | The task V3 dispatches to. **It accepts only `OutputHandle::ResultsCache`** (`SearchTask::new` bails on anything else). |
| `clp-s-sinks` | JSON lines, an in-memory collector, and `MsgpackResultSink`: the **old** 5-tuple `(timestamp, message, orig_file_path, archive_id, log_event_idx)` over one connection per session | Not the streaming protocol; the template a v1 sink would follow |
| `clp-s-wire` | `SearchResultTuple` (the 5-tuple), results-cache field names, `epoch_ns_to_wire_ms` | Byte compatibility with the C++ outputs |
| `clp-s-results-cache` | The MongoDB sink with the session-wide top-k | The adapter's only output today |
| `clp-s-cli` | `clp-s-rs search`/`extract` with `--output network --host --port` (5-tuple) | Reference and differential testing |

## What is not on this branch

Everything the report's V3 and the revised design doc add on top of the reader is on
`query-coordinator/streaming-search-rust-reader`, which is **not pushed** (the report says all of
it was uncommitted at `e10364cc`):

* `NetworkV1Sink` in `clp-s-sinks`: lazy connect on the first non-empty batch, the v1 handshake
  `[1, token, task_index, archive_id]`, results `[result_index, timestamp_ms, message + "\n"]`
  numbered from 0 per connection, one `write_all` per batch of 4,096 rows or 256 KiB;
* the engine switch in `clp-tdl-package`: `CLP_SPIDER_SEARCH_ENGINE` read once per executor into
  a `OnceLock`, `query::clp_s_search` dispatching to `clp_s_spider_adapter::search` for `rust`,
  and `package_init` installing the Tokio runtime, the `ReaderContext` and the CLP home;
* the `OutputHandle::Network` arm in the adapter, and its `--session-token`/`--task-index`
  equivalents;
* protocol v2 (trailer and acknowledgement) in both `clp-s` and the listener;
* the funnel fix (`ResultEncoding`/`ResultEncoder`, batches in the listener's channel, the API
  server's `SseEncoding`);
* the `compose.clp-spider.yaml` pass-through of `CLP_SPIDER_SEARCH_ENGINE`;
* the worker image that bundles the adapter into `libclp_tdl_package.so`.

Until that branch is pushed, V3 can be discussed from the reader's code and docs, but not run.

## Where V3 attaches to this branch

1. `components/clp-tdl-package/src/task/query/search.rs`, `clp_s_search`: today it builds the
   `clp-s` argv for the `ResultsCache` or `Network` output handle and spawns the binary
   (`run_clp_s_search`). V3 adds a match on the engine before that: `rust` calls
   `clp_s_spider_adapter::search` with the same inputs; errors stay `TdlError::ExecutionError`, so
   Spider retries as before.
2. `clp-s-spider-adapter`: `SearchTask::new` must accept `OutputHandle::Network { host, port,
   session_token }` and build the v1 sink with the task index as the cursor key. The adapter
   already has the per-task inputs (`archive_id`, the dataset, `ClpSQueryOption`), the task
   context, and the fallback logic; the fallback is safe only because of lazy connect (nothing
   reaches the listener before `UnsupportedArchive` is known).
3. `clp-s-sinks`: a `NetworkV1Sink` next to `MsgpackResultSink`. The reader's `Sink`/`PartialSink`
   protocol delivers `RecordBatch`es per partition; one task is one archive, so one partition and
   one `result_index` counter.
4. The listener, the coordinator and the job config need no change for v1: the wire bytes are the
   same. (v2 changes the listener and both producers.)
5. `clp-rust-utils` is a path dependency of the reader workspace, and the reader workspace is not
   in `.github/workflows/clp-rust-checks.yaml`, so nothing in CI builds it against this branch's
   `ClpSQueryOption` or `OutputHandle`. The adapter's `SearchTask::new` already matches on
   `OutputHandle`, so a new variant is a compile error there, which is the right failure.

## Discussion points for the streaming design

* **Determinism (issues 3 and 22).** The reader argues it the same way the design doc does for
  C++: one scan per calling thread, streams in table-directory order, rows ascending, batch size
  irrelevant to order. The report verified identical per-archive order on 607 archives. The
  registered divergence DV-01 (floats: C++ prints with `%f`, lossy; Rust prints the shortest
  round-trip) means the *content* at an index can differ between engines, which is why a job must
  not mix engines. The reader's own risk RK9 ("oversubscription and nondeterminism in hosts",
  mitigated by a `CurrentThread` default, one context per process, `Scan` order and a total-order
  key) is the same precondition stated from the other side; the streaming design should cite it
  rather than restate it.
* **Orphans (issue 20).** With the reader in-process, Spider's hard-timeout SIGKILL of the
  executor ends the scan too: no orphaned producer, no zombie, no duplicate stream from a
  timed-out attempt. This removes the V2-only part of issue 20 for search; compression tasks still
  spawn subprocesses.
* **Memory and admission vs. Spider's timeouts (issues 1 and 21).** The adapter waits for room in
  the process-wide memory budget ("60 s base admission timeout" in the report; `wait_for_room` in
  `admission.rs`), and the reader's buffer pool can hold up to about 1 GiB per executor by the
  code's bound (not measured in the report). Time spent waiting for admission counts against the
  same hard timeout as time spent blocked on the listener. With v2's acknowledgement, a task's slot
  is held until the consumer drains it, so admission, backpressure and the timeout all meet in one
  executor; the budget and the timeout policy need to be set together.
* **Panics.** `catch_unwind` at the task boundary turns a reader panic into a task failure and a
  Spider retry, instead of taking the execution manager down. The streaming listener sees it as an
  incomplete connection (EOF without a trailer under v2), which is the intended path.
* **Archive format.** The reader reads only single-file archives. The report's benchmark archives
  were compressed with `use_single_file_archive=True` forced through a bind-mounted override; the
  package default must be checked before V3 is the default engine, or the fallback to C++ runs on
  every archive.
* **Timestamps without a value (issue 13).** DV-34: a record without a timestamp is `None` in the
  reader and `0` on the wire, which is also what C++ sends. The streaming protocol should say so.
* **Trailing newline (issue 14).** DV-21/22: the message is encoded as the text followed by `\n`,
  like C++, so the API server's stripping stays necessary and the CLI's output stays byte-identical
  across engines.
* **Session-wide top-k (DV-25, Q6).** The results-cache sink applies a session-wide top-k; a
  streaming search has no limit (`max_num_results` must be omitted), so this does not apply, but
  it is one more reason a per-archive limit cannot be reintroduced later without breaking
  determinism (issue 3).
* **Fallback to C++ inside a V3 worker.** `UnsupportedArchive` before any write spawns `clp-s`,
  which reintroduces the subprocess (and issue 20) for exactly those archives, and runs the other
  engine's formatting (DV-01) under the same task index if the task is later retried in-process.
  The fallback should be all-or-nothing per job, or the retry must use the same path.
* **What to re-measure once the V3 branch is pushed.** The stall and throttled-client experiments
  (`tools/scripts/streaming-search-experiments/`) with `CLP_SPIDER_SEARCH_ENGINE=rust`, to confirm
  the orphan finding disappears and to measure issue 21 under v2; and the fault test with 4 kills,
  which under V3 kills the executor rather than a child.
