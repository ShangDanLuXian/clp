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
| Real-clp-s listener test (`CLP_S_BINARY` set)                 | 143 results, 286 duplicates dropped, 0 protocol errors | TBD |
| clp-s builds from the merged tree                             | builds               | TBD |
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
