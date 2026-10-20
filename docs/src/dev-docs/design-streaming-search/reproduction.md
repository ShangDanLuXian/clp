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
| Merged workspace type-checks with the merged lockfile (`cargo check --workspace --all-targets --locked`) | n/a | TBD |
| `cargo +nightly fmt --check` and `clippy -D warnings` (`task lint:check-rust`) | clean | TBD |
| `cargo nextest run --workspace`                               | 140 passed, 6 skipped | TBD |
| `stream_query_tests` repeated 20 times                        | 10/10 passed every time | TBD |
| Real-clp-s listener test (`CLP_S_BINARY` set)                 | 143 results, 286 duplicates dropped, 0 protocol errors | TBD |
| clp-s builds from the merged tree                             | builds               | TBD |
| `impl_sqlx_type!` doctest                                     | known failure, pre-existing | TBD |
| Package correctness, fault injection, benchmarks              | see reports          | not reproducible here (no Docker daemon) |

## Notes

TBD
