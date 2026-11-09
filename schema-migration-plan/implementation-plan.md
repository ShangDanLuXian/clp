# clp-s shared metadata tables: implementation plan

Companion to `interface-plan.md`, which defines the tables, the deletion lifecycle, and the
`clp-metadata-db` interface.

## Strategy

* Minimal replacement first. The goal of this plan is the switch: clp-s moves from per-dataset
  tables to the shared tables, and every non-Celery path on `main` keeps working. Later design-doc
  features build on it afterward.
* The current per-dataset path keeps working until the switch. Before it, new code merges without
  being wired in: new routes stay unregistered, new binaries stay undeployed, and no existing
  runtime path changes. There is no feature flag; clp-s and clp-text are told apart by the
  configured storage engine, as today.
* Interface first: define `clp-metadata-db` with `todo!()` bodies and have Lin review the surface
  before any SQL is written.
* clp-text (`clp` storage engine) stays untouched throughout.

## Phase 1: Define the interface

1. Create `components/clp-rust-libs/clp-metadata-db` with the layout, types, errors and every
   operation in the interface plan, each body `todo!()`.
2. Move `get_earliest_creation_timestamp_millisecs` in `clp_rust_utils::database::mysql::query_job`
   onto `&mut MySqlConnection`.
3. Interface review with Lin. Nothing calls the crate yet.

Exit: the crate compiles, the workspace checks pass, and Lin has approved the signatures and the
deletion lifecycle.

## Phase 2: Implement `clp-metadata-db`

One commit per module, in dependency order, each replacing reviewed `todo!()` bodies with SQL.

| Step | Content | Supporting changes |
|---|---|---|
| 2.1 | `MetadataTables::new`, `schema::create_tables` | Shared-table name helpers in `clp-rust-utils` |
| 2.2 | `dataset.rs` | — |
| 2.3 | `archive.rs`, including marking, cleanup reads and reclamation | `ArchiveId::from_bytes` / `as_bytes` in `clp-rust-utils` |
| 2.4 | `column_metadata.rs` | `ColumnType` ↔ `NodeType` mapping, kept in sync with `SchemaTree.hpp` |
| 2.5 | `statistics.rs` | — |
| 2.6 | `query_job::get_earliest_creation_timestamp_millisecs` | — |

Every step checks the interface plan's rules: `dataset_id` scoping, skipping marked rows in normal
reads, hard deletes guarded by `is_deleted = TRUE`, DB-clock timestamps, and batches of 1000.

## Phase 3: Pre-switch consumers (dormant)

Each step is self-contained and leaves runtime behavior unchanged.

| Step | Content | Kept dormant by |
|---|---|---|
| 3.1 | `credentials::Database::from_env` | — (helper only) |
| 3.2 | `db-table-creator` binary: parse the config, reject non-clp-s, create the tables, fail on an old-shaped `clp_datasets` | Not invoked by the package |
| 3.3 | `clp_rust_utils::s3::{delete_by_key_prefix, delete_by_keys}` (paginated, batched at 1000, per-object errors surfaced) | — (helper only) |
| 3.4 | api-server dataset routes: `GET`/`POST /datasets`, `DELETE /datasets/{name}` (marks only) | `datasets::router()` not merged into `routes::from_app_state`; OpenAPI unchanged |
| 3.5 | api-server archive routes: `GET /datasets/{name}/archives`, `POST /datasets/{name}/archives/delete` (by UUIDs or time range; marks only) | Same as 3.4 |
| 3.6 | `garbage-collector` library: one sweep of retention marking, dataset cleanup, archive cleanup, on FS and S3 | Library only |
| 3.7 | `archive-garbage-collector` binary (clp-s only, sweep loop, SIGTERM/ctrl-c) | Not in Docker/compose/Helm |

Steps 3.6 and 3.7 need `garbage_collector.sweep_interval.archive` in the Rust config mirror, added
with the Python default (60 minutes).

## Phase 4: The switch

One change that wires everything at once.

1. Merge `main`.
2. Compression coordinator: replace `upsert_metadata_tables` with `get_dataset_id_by_name`; fail the
   job if the dataset is missing or marked. Pass the `DatasetId` to the compression and commit tasks
   through the task inputs.
3. Commit task: replace `register_dataset` and the per-dataset insert with `lock_live_dataset` and
   `insert_archives` (with the job's `compression_job_id`), in the same transaction as the job's
   status update. Fail the job if the dataset is gone or marked.
4. C++ indexer: take `--dataset-id`, write `clp_column_metadata`, drop `--create-table`. The
   compression task passes the ID. Reviewed against the C++ guides.
5. Query coordinator: `get_dataset_ids_by_names` + `find_archives_for_query`; drop the global
   `archive_retention_period_millisecs`.
6. api-server WebUI metadata routes: switch the clp-s branches to the library (interface plan
   table); the clp-text branches stay.
7. Register `datasets::router()`; regenerate `docs/src/_static/generated/api-server-openapi.json`
   and `components/webui/packages/api-client/openapi.json`.
8. Remove the per-dataset table-name helpers and Rust's `archive_output.retention_period`.
9. Deployment:
   * `create-db-tables.py` runs `db-table-creator` for clp-s; clp-text keeps the Python
     initializer.
   * Add the `archive-garbage-collector` compose service and Helm deployment, with the archive
     storage mount and AWS config it needs, and its environment in `controller.py`.
   * The Python garbage collector skips archive GC for clp-s; search-result GC stays.
   * The Dockerfile copies the new binaries.
   * Remove `sbin/admin-tools/*.sh` (dataset/archive management moves to the API).
10. Python: remove the clp-s branches of `initialize-clp-metadata-db.py` and the archive GC, the
    native dataset and archive managers, and the clp-s `validate_datasets_exist` call in the
    Python search script if the Rust search CLI (submitting through the api-server) hasn't
    replaced it yet.
11. Docs and scripts: datasets must be created with `POST /datasets` before compression, including
    `default`; update the user docs, `compress.sh`, and the GC/retention docs (per-dataset
    retention by creation time).

Before the switch merges, validate end to end on FS and S3:

* Create a dataset, compress, search (with and without a time range), check every WebUI metadata
  route, and see the column metadata the indexer wrote.
* Compressing to a missing dataset fails; compressing to a dataset marked mid-job fails at commit
  and inserts nothing.
* Deleting archives (by UUID and time range) and a dataset hides them immediately; the next GC sweep
  removes the storage and the rows; the dataset name can then be recreated.
* Kill the GC between storage cleanup and reclamation, and between marking and cleanup; the next
  sweep finishes the job and deletes nothing unmarked.
* Retention marks only archives older than the protected expiry base; a non-terminal query job holds
  it back.
* clp-text behaves as before.

## Phase 5: After the switch

* Partition maintenance for `clp_archives`, and reading `clp_long_span_archives`.
* The known deletion limitations in the interface plan: waiting for in-flight queries before
  cleanup, reclaiming ingestion leftovers, and an immutable storage location for compression.
* Shared files table, then the top-level-filtering tables.
* Authentication for the destructive routes.
* Relaxing the dataset-name length limit derived from per-dataset table names (WebUI
  `SqlTableSuffix`, `DatasetName` validation).

## Workflow for every step

1. A subagent implements the step after reading Lin's Rust `coding-style.md` (C++ steps follow the
   C++ guide).
2. The diff is reviewed line by line against `coding-style.md` and `pr-review.md`.
3. Every added test is mutation-checked: break the behavior, confirm the test fails, restore it.
   Tests cover hand-written logic once, add no test-only seams, return `anyhow::Result`, and use
   `expect` rather than `unwrap`.
4. Run the checks with CI's pinned toolchain from the repo root: `cargo fmt --all -- --check`,
   `cargo clippy --all-targets --all-features -- -D warnings`,
   `cargo nextest run --workspace --all-features`, and `cargo fetch --locked` when Cargo files
   change.
5. SQL steps are validated against a local MariaDB; S3 steps against a local moto server.
6. Pre-switch checkpoints: `datasets::router()` isn't registered, the generated OpenAPI documents
   are unchanged, neither new binary is referenced by Docker/compose/Helm/Python, and no Python,
   C++, compression-coordinator, query-coordinator or WebUI files changed.

## Open process questions

1. Database tests: CI has no MySQL service, so SQL is validated manually against MariaDB. Add a
   MySQL/MariaDB service to the Rust CI and `sqlx`-backed tests, or keep manual validation?
2. Delivery: one PR per phase, or one PR per step?
