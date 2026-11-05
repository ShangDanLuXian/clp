# clp-s shared metadata tables: implementation plan

Companion to `interface-plan.md`, which defines the `clp-metadata-db` interface.

## Strategy

* The old per-dataset path keeps working until one switch step. Everything before the switch is
  merged without being wired in: new routes stay unregistered, new binaries stay undeployed, and no
  existing runtime path changes. There is no feature flag.
* Interface first: define `clp-metadata-db` with `todo!()` bodies and review the surface before SQL
  implementation. Deletion DDL and signatures remain provisional until the interface plan's
  "Deletion lifecycle (review required)" review is complete; other interfaces can proceed separately.
* clp-text (`clp` storage engine) stays untouched throughout.
* Generate UUIDv7 IDs for new clp-s archives independently of the shared-table rollout. Existing
  UUIDv4 IDs remain supported; changing UUID generation does not change database storage encoding.

## Phase 1: Define the interface

1. Settle the open questions in the interface plan with Lin. Review the deletion lifecycle before
   freezing deletion DDL or interfaces: durable intent, in-flight work, stable storage targets,
   rollback/replay safety, name reuse, and metadata reclamation. Choose the representation and
   transaction boundaries without assuming either existing deletion order is sufficient.
2. Define the full surface in the interface plan, with `todo!()` bodies:
   * Types: `DatasetId` / `ArchiveRowId` newtypes, `TimestampRange`, `QueryArchiveFilter`,
     `ColumnType`, `NewArchive`, `QueryArchive`, `TimestampBounds`, `SpaceSavings`.
   * Operations: every operation listed in the interface plan, plus the intent/retry/completion
     operations settled by deletion review. Candidate hard-delete primitives are final reclamation
     helpers, not complete request workflows.
   * `get_earliest_creation_timestamp_millisecs` takes a connection.
   * `MetadataTables` holds its four table names; any additional cleanup-record table names and
     types depend on deletion review.
3. Interface review with Lin. Nothing calls the crate yet.

Exit: the crate compiles, the workspace's checks pass, and Lin has approved the signatures. Deletion
approval must cover the lifecycle and its schema; placeholder signatures alone do not close it.

## Phase 2: Implement `clp-metadata-db`

One commit per module, in dependency order. Each replaces reviewed `todo!()` bodies with SQL.
Implement deletion SQL only after its lifecycle and interfaces have been approved.

| Step | Content | Supporting changes |
|---|---|---|
| 2.1 | `MetadataTables::new`, `schema::create_tables` (`utf8mb4` with `utf8mb4_bin`, `compression_job_id`, partition `p_future`; deletion columns/keys and any cleanup records require lifecycle approval) | `long_span_archives_table_name`, `unscoped_column_metadata_table_name` in `clp-rust-utils` |
| 2.2 | `dataset.rs` | — |
| 2.3 | `archive.rs` | `ArchiveId::from_bytes` / `as_bytes` in `clp-rust-utils` |
| 2.4 | `column_metadata.rs` | `ColumnType` ↔ `NodeType` mapping, kept in sync with `SchemaTree.hpp` |
| 2.5 | `statistics.rs` | — |
| 2.6 | `query_job::get_earliest_creation_timestamp_millisecs` | — |

Implementation rules that every step checks:

* Every archive/column query and deletion includes `dataset_id`.
* `IN` lists and multi-row inserts are batched at 1000.
* Final reclamation deletes long-span rows before archive rows, after storage cleanup and the
  reviewed concurrency prerequisites.
* Normal reads exclude logically deleted datasets and archives; cleanup queries explicitly read
  pending-deletion state. The proposed live-dataset predicate is `deletion_timestamp_millisecs = 0`.
* Logical deletion and durable cleanup targets commit atomically. Retries use immutable identities
  and original storage locations, and cannot delete live data after a rolled-back intent transaction.
* Times that must agree across components (`creation_timestamp_millisecs`,
  `deletion_timestamp_millisecs`) come from the DB clock.
* Timestamps use the `_timestamp_millisecs` suffix in columns and Rust fields.

## Phase 3: Pre-switch consumers (dormant)

Each step is self-contained and leaves runtime behavior unchanged.

| Step | Content | Kept dormant by |
|---|---|---|
| 3.1 | `credentials::Database::from_env` | — (helper only) |
| 3.2 | `db-table-creator` binary: parse config, reject non-clp-s, create the tables | Not invoked by the package |
| 3.3 | `clp_rust_utils::s3::{delete_by_key_prefix, delete_by_keys}` (paginated, batched at 1000, per-object errors surfaced) | — (helper only) |
| 3.4 | api-server dataset routes: `GET`/`POST /datasets`, `DELETE /datasets/{name}`; deletion follows the reviewed logical-delete, durable-cleanup, and reclamation lifecycle | `datasets::router()` not merged into `routes::from_app_state`; OpenAPI unchanged |
| 3.5 | api-server archive routes: `GET /datasets/{name}/archives`, `POST /datasets/{name}/archives/delete`; manual deletion has durable retry ownership and the reviewed in-flight query/writer protection | Same as 3.4 |
| 3.6 | `garbage-collector` crate: reviewed cleanup recovery/replay support; a recovery-file buffer alone is insufficient | Library only |
| 3.7 | `ArchiveGarbageCollector`: expiry base from all non-terminal query jobs; use the same reviewed deletion invariants as API deletion; reclaim deleted-dataset metadata only after cleanup and coordination | Library only |
| 3.8 | `archive-garbage-collector` binary (clp-s only, SIGTERM/ctrl-c) | Not in Docker/compose/Helm |

Package config additions needed by 3.7/3.8 (`garbage_collector.sweep_interval.archive`,
`tmp_directory`) are added additively to the Rust config mirror with Python-compatible defaults.

## Phase 4: The switch (one step)

Gate: deletion lifecycle review and failure validation below must be complete before runtime wiring.
Manual deletion and retention GC must both have durable cleanup/retry ownership; orphan recording
for failed deletion is not deferred until after the switch.

1. Merge `main`.
2. Compression coordinator: replace `upsert_metadata_tables` with a live-dataset lookup; fail the job
   if the dataset is missing or deleted. Bind work to the original dataset ID or generation and
   enforce the reviewed deletion protocol through compression, indexing, publication, and commit.
3. Commit task: drop `register_dataset`; validate the job's original dataset identity and live state,
   rather than resolving a reused name, and call `insert_archives` with the job's `compression_job_id`
   in the same transaction as the job's status update. Coordinate with deletion using the reviewed
   fencing protocol.
4. C++ indexer (kept until the Rust clp-s rewrite): port the column-metadata write to the shared
   table, using the job's original dataset identity and reviewed deletion coordination; drop
   `--create-table`. Reviewed against the C++ `pr-review.md`.
5. Query coordinator: `get_dataset_ids_by_names` + `find_archives_for_query`; drop the global
   `archive_retention_period_millisecs`.
6. api-server WebUI metadata routes: the clp-s branches call `get_dataset_names`, `statistics::*`
   and `get_column_names_by_types`; the clp-text branches stay as they are. The ingestion-details
   route is out of scope and isn't migrated.
7. Register `datasets::router()` and regenerate
   `docs/src/_static/generated/api-server-openapi.json` and
   `components/webui/packages/api-client/openapi.json`.
8. Remove the per-dataset table-name helpers and Rust's `archive_output.retention_period`.
9. Deployment: `create-db-tables.py` runs `db-table-creator` for clp-s; add the
   `archive-garbage-collector` compose service and Helm deployment (plus `controller.py` env); the
   Python GC skips archive GC for clp-s; the api-server mounts the archives directory (rw) and the AWS
   config; the Dockerfile copies the new binaries; remove `sbin/admin-tools/*.sh`.
10. Delete the now-unused clp-s Python (native archive/dataset managers, the clp-s branches of the
    Python archive GC and `initialize-clp-metadata-db.py`) and update docs.
11. Users create datasets through `POST /datasets` before compressing; `compress.sh` is updated
    accordingly.

## Phase 5: After the switch

* WebUI: move the front end off `/api/archive-metadata/sql` (raw SQL against the per-dataset
  tables) onto the api-server routes, unless it's folded into the switch.
* Shared files table, then the top-level-filtering tables.
* Partition maintenance for `clp_archives`; reading `clp_long_span_archives`.
* Reconciliation of storage objects orphaned by ingestion failures, if needed. Recovery from failed
  deletion is required before the switch and is not part of this follow-up.
* Authentication for the destructive routes.

## Workflow for every step

1. A subagent implements the step after reading Lin's Rust `coding-style.md`
   (C++ steps follow the C++ guide).
2. The diff is reviewed line by line against `coding-style.md` and `pr-review.md`.
3. Every added test is mutation-checked: break the behavior, confirm the test fails, restore it.
   Tests cover hand-written logic once, add no test-only seams, return `anyhow::Result`, and use
   `expect` rather than `unwrap`.
4. Checks with CI's pinned toolchain from the repo root (after copying the lint configs):
   `cargo +nightly-2026-10-31 fmt --all -- --check`,
   `cargo +nightly-2026-10-31 clippy --all-targets --all-features -- -D warnings`,
   `cargo nextest run --workspace --all-features`, and `cargo fetch --locked` when Cargo files change.
5. SQL steps are validated against a local MariaDB; S3 steps against a local moto server.
   Deletion validation must cover crashes after committed intent, partial storage failure,
   persisted candidates followed by DB rollback, repeated requests and restart replay, concurrent
   deletion with queries/writers/indexers/commit, and same-name recreation. Include queries that
   selected archive IDs before logical deletion and stale cleanup tasks after name reuse. Confirm
   that targets remain correct across config changes, live data is never removed by replay, and
   pending work remains discoverable until completion.
6. Pre-switch checkpoints: `datasets::router()` isn't registered, the generated OpenAPI documents
   are unchanged, neither new binary is referenced by Docker/compose/Helm/Python, and no Python,
   C++, compression-coordinator, query-coordinator or WebUI files changed.

## Open process questions

1. Database tests: CI has no MySQL service today, so SQL is validated manually against MariaDB.
   Add a MySQL/MariaDB service to the Rust CI and `sqlx`-backed tests, or keep manual validation?
2. Delivery: one PR per phase, or one PR per step?
3. ~~Stream-file extraction~~ Decided: out of scope; accepted as broken. The WebUI log viewer's
   `POST /stream_files/extract` submits an `ExtractJson` query job that only the Python query
   scheduler handles, and the Spider deployment (`tools/deployment/package/docker-compose-spider.yaml`)
   doesn't run that scheduler, so extraction is already broken independently of this migration. It
   gets fixed separately when extraction moves to the Rust query coordinator, against the shared
   tables.
