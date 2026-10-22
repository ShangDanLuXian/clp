# Experiment reproduction

This page records an independent re-run of the checks and experiments from the prototype reports,
on the merged tree of this branch (`query-coordinator/streaming-search`), and compares the results
with the reports' numbers.

## Environment

| Item            | Value                                                                          |
|-----------------|--------------------------------------------------------------------------------|
| Tree            | This branch after merging `query-coordinator/streaming-search-api` (`7365387b`) |
| Host            | 4 vCPUs, 15 GiB RAM, Ubuntu 24.04, Linux 6.18                                   |
| Rust            | stable 1.97.0 for `cargo check`/tests; the repo's pinned nightly for lint        |
| cargo-nextest   | 0.9.148                                                                         |
| C++             | GCC 13.3 / Clang 18, CMake 3.28, deps built from source with `task deps:core`   |
| Docker          | client only; no daemon                                                          |

Because there is no Docker daemon, the package-level experiments of the reports (the 607-archive
benchmark, the fault injection with `docker kill`, the API server benchmark, and the disconnect
timings) cannot be reproduced here. They need Docker Compose, Spider, and the 211 GB dataset. The
rows below are marked accordingly.

## Results

| Check (from the reports)                                      | Reported            | Reproduced here |
|---------------------------------------------------------------|---------------------|-----------------|
| Merged workspace type-checks with the merged lockfile (`cargo check --workspace --all-targets --locked`) | n/a | **passes** (4 m 35 s cold) |
| `cargo +nightly fmt --check` and `clippy -D warnings` (`task lint:check-rust`) | clean | **clean** |
| `cargo nextest run --workspace`                               | 140 passed, 6 skipped | **140 passed, 6 skipped** (see note 1) |
| `stream_query_tests` repeated 20 times                        | 10/10 passed every time | **10/10 passed in all 20 runs** |
| `cargo nextest run -p search-result-listener --run-ignored all` without `CLP_S_BINARY` | the real-clp-s test skips | **36 passed, 0 skipped**; the clp-s test logs "Skipping the test since `CLP_S_BINARY` isn't set." and passes |
| Real-clp-s listener test (`CLP_S_BINARY` set)                 | 143 results, 286 duplicates dropped, 0 protocol errors | **passes** (see note 4); the full listener suite with the binary: 36 passed, 0 skipped |
| clp-s and clo build from the merged tree                      | build                | **build** (Release, shared libs; `clp-s` in 2 m 28 s on 4 cores, see note 5) |
| Wire protocol v1 from the built clp-s (not in the reports)    | n/a                  | **as designed** (see note 6) |
| `impl_sqlx_type!` doctest (`cargo test --doc -p clp-rust-utils`) | known failure, pre-existing | **fails** the same way: "cannot find macro `impl_sqlx_type` in this scope" |
| Package correctness, fault injection, benchmarks              | see reports          | not reproducible here (no Docker daemon) |

## Notes

1. The first `cargo nextest run --workspace` stopped at one failure,
   `log-ingestor::test_compression_listener`, which asserts that a 100 ms timeout-based submission
   has happened 102 ms after the senders finish. It ran while the C++ dependency build had the 4
   CPUs at a load average above 8, and it passed on the next run with `--no-fail-fast` (140/140).
   The merge does not touch `log-ingestor`; the test is load-sensitive and unrelated to streaming
   search.
2. The 20 repeated `stream_query_tests` runs each took about 0.42 s. All 20 reported
   `10 tests run: 10 passed, 15 skipped` (the 15 are the other api-server tests filtered out by
   the test-name filter).
3. The session's outbound proxy returns HTTP 403 for GitHub source archives
   (`github.com/<owner>/<repo>/archive/...`) of repositories not attached to the session, while
   `releases/download` assets and git clones are served. 14 of the C++ dependencies use archive
   URLs, so `task deps:core` fails at the first of them (`utfcpp`). To build clp-s here, each of
   those dependencies was cloned at its pinned tag or commit with git, packed into a local tarball
   with `git archive`, and the deps taskfile was pointed at the local tarballs for the duration of
   the build (an uncommitted, local-only change). The ANTLR C++ runtime's CMake configuration
   additionally downloads googletest from a GitHub archive when `ANTLR_BUILD_CPP_TESTS` is on (its
   default), which fails the same way, so that option was switched off locally for the build. The
   other 11 dependencies built unchanged.
4. `search-result-listener::clp_s::clp_s_attempts_of_one_task_emit_each_result_once` compresses
   1,000 JSON records (every 7th says "Transmitted"), searches `*Transmitted*` to stdout as the
   reference, then streams the same search to a listener three times concurrently under task index
   0, plus a no-result query under task index 1, plus one more attempt under task index 0 after the
   first three finish. It asserts that the listener emits exactly the reference messages with
   `num_results_emitted = N`, `num_duplicates_dropped = 2N`, `num_protocol_errors = 0`. The
   reference has 143 messages (1,000 / 7 rounded up), so the passing assertion is the report's
   "143 results, 286 duplicates dropped, 0 protocol errors". The run took 0.12 s. Running the same
   data through the built binary by hand also gives 143 lines on stdout.
5. The core build used `-DCMAKE_BUILD_TYPE=Release -DCLP_USE_STATIC_LIBS=OFF` with the system
   libarchive, libcurl, MariaDB client and OpenSSL, and the dependency build's libraries for the
   rest. The repo's `FindLibLZMA` module takes the version from pkg-config, so
   `PKG_CONFIG_PATH` had to point at the dependency build's `liblzma.pc`; otherwise the system
   5.4.5 header is reported against the 5.8.1 library and configuration fails. Only the `clp-s`
   and `clo` targets were built, not `unitTest`.
6. A small msgpack decoder was put in front of `clp-s s ... network --host 127.0.0.1 --port P
   --session-token T --task-index 0` on the archive from note 4:
   * The first frame is `[1, "<T>", 0, "<archive id>"]`: version 1, the session token and the task
     index exactly as given, and the archive ID of the archive searched.
   * It is followed by 143 result frames `[result_index, timestamp, message]`, with
     `result_index` contiguous from 0 to 142, each `timestamp` equal to the record's `ts`, and
     each `message` the record's JSON with a trailing newline (the API report's reason for
     stripping it).
   * A query with no results (`msg: absent`) under task index 1 makes no connection at all within a
     5 s accept timeout and `clp-s` exits 0, which is the lazy-connect behaviour the design
     specifies.

## Package-level results (local WSL2 machine, 2026-10-21)

The package-level experiments were then run on a WSL2 machine with Docker (i7-13700K, 24
threads) by the harness in `tools/scripts/streaming-search-experiments/`, against this branch's
package in Spider mode with 8 synthetic archives of 40 MiB and the query `*Transmitted*`
(659,677 results). Full write-ups: `tools/scripts/streaming-search-experiments/results/`.

| Check                                            | Reported (607 archives, 1,071 results) | Measured here (8 archives, 659,677 results) |
|--------------------------------------------------|----------------------------------------|---------------------------------------------|
| Stream vs results cache vs `clp-s` result sets   | byte-identical                          | **byte-identical**, 3 stream runs + 1 results-cache run |
| Time to first result, `POST /query/stream`       | 207.8 ms                                | **85.3 ms** median                          |
| Time to first result, `POST /query` + `GET /query_results` | 3,115 ms                     | **3,112 ms** (the whole job must finish first) |
| Time to first byte, `sbin/streaming-search.sh`   | 1,021 ms                                | **862 ms** (about 0.8 s is the runtime container start) |
| Disconnect → CANCELLING                          | 3.2 ms                                  | **0.12-0.14 s** (the harness polls the DB through `docker exec` every 0.2 s) |
| SIGKILL `clp-s` mid-stream, one retry            | nothing lost or duplicated              | **identical**; 8 kills, 280,778 duplicates dropped, 0 protocol errors |
| 4 SIGKILLs per task with one retry               | not run                                 | job FAILED at 16.9 s, `end: Failed`, 167,875 results delivered |
| Client stall longer than the hard timeout        | not run ("refuted")                     | **job FAILED** at 160.9 s for a 270 s stall; see [backpressure](backpressure.md) |

## Not reproduced

* The reports' exact numbers on their 607-archive, 211 GB dataset: the benchmark (3.805 s vs
  10.958 s), the `docker kill` of a worker, the API server's waterfall, and the concurrency
  run. The ratios above come from a smaller dataset on a faster machine.
* Mutation testing (19/19 and 18/18 injected defects caught). The injected defects are not
  recorded in the reports, so there is nothing to re-run.
* The report's end-to-end handler script (26/26) and the clang-format check; the script is not
  on the branch.
## After merging upstream `main` (2026-10-22)

The branch was then merged with y-scope/clp `main` at `616b07d`, which adds query job
cancellation (#2573, with the `CancelRequested` state and the coordinator's cancellation monitor),
`ClpSQueryOption` with count-by-time (#2561), unlimited result limits when omitted (#2567), failing
unsupported jobs (#2565), and an `OutputHandler` interface that returns error codes instead of
throwing. The prototype's clp-s `network` handler was ported to that interface (protocol v1
unchanged), the shared `cancel_query_job` now requests cancellation the way the coordinator
expects, and streaming searches require `max_num_results` to be omitted.

| Check                                                             | Result |
|-------------------------------------------------------------------|--------|
| `cargo check --workspace --all-targets`                           | **passes** |
| `cargo nextest run --workspace`                                   | **150 passed, 6 skipped** (upstream added 10 tests) |
| `task lint:check-rust`                                            | see below |
| clp-s builds with the ported handler                              | **builds** |
| Real-clp-s listener test with the rebuilt binary                  | **passes** (36 passed, 0 skipped in the listener crate with `--run-ignored all`) |
| Wire capture of the rebuilt clp-s                                 | handshake `[1, token, 0, archive id]`, 143 contiguous results, timestamps match |
| clp-s with no listener on the port                                | logs `Failed to write search result, error=20` (`ErrorCodeFailureNetwork`) and **exits 1**, so the task fails and Spider retries |
| Listener that accepts, reads a few bytes, and closes              | clp-s **exits 0**: all 143 results of this small archive fit in the kernel send buffer before the peer's reset arrives. This is the v1 gap of issue 2 (no acknowledgement), reproduced on purpose; protocol v2's trailer and acknowledgement in the revised design close it. |

## Conclusion

Every check that can run without a deployment reproduces the reports' results exactly: the merged
tree lints clean, all 140 workspace tests pass, the route tests are stable over 20 runs, the
pre-existing doctest failure is still there, `clp-s` builds, and the real-clp-s listener test
passes with the expected statistics. The built `clp-s` speaks protocol v1 as designed, including
lazy connect. The package-level runs on a local machine reproduce the reports' qualitative
results (identical result sets, a first result tens of times sooner than the results-cache path,
exactly-once delivery across SIGKILLs) and add the two findings the reports lacked: a stalled
client fails the job through Spider's hard timeout, and the hard timeout does not kill `clp-s`.
