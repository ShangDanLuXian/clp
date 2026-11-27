# v0.15.0 schema migration: PR plan, testing scope and follow-up work

Review outcome of Xuhui's "Implementation plan (v0.15.0 schema migration)"
(`42404e4d9e6b814daa0bc66edf4d2fb1`), which builds on prototype report v3
(`42404e4d9e6b8113912ce0817837ff3e`, branch `schema-migration-v015-v3`). It implements the spec
"Schema migration plan for release v0.15.0" (`41f04e4d9e6b807d91b7ff9c5c5726f2`). Where this file
and the implementation plan disagree, this file wins.

The prototype branch is local to Xuhui's machine, so this plan is derived from the spec's trait
section and report v3's commit contents and coupling notes (C1-C5, N3), not from the code.

## 1. PR plan

The library is split by trait method. After the core PR, every library PR is independent of the
others, and every consumer PR depends only on the library methods it calls.

| #  | PR                                                                         | Depends on    |
|----|----------------------------------------------------------------------------|---------------|
| 1  | Rename `ArchiveId` to `ArchiveUuid`                                        | -             |
| 2  | Core (see below)                                                           | 1             |
| 3  | `clp-package-cli` scaffold: config, credentials, client construction       | 2             |
| L1 | `DatasetLookup`                                                            | 2             |
| L2 | `DatasetManagement::create_or_get_dataset`                                 | 2             |
| L3 | `DatasetManagement::mark_dataset_deleted`                                  | 2             |
| L4 | `ArchiveManagement::insert_archives`                                       | 2             |
| L5 | `ArchiveLookup::find_archives_for_query`                                   | 2             |
| L6 | `ArchiveLookup::{find_archives_by_ids, find_archive_ids_by_uuids, find_archives_by_time_range}` | 2 |
| L7 | `ArchiveManagement::delete_archives`                                       | 2             |
| L8 | `MetadataStatistics`                                                       | 2             |
| G  | Python GC functions for the shared tables                                  | 2 (DDL only)  |
| C1 | `db-table-creator`                                                         | 3, L2         |
| C2 | tdl `commit_into_shared_tables`                                            | L4            |
| C3 | Query coordinator shared-table archive selector                            | L1, L5        |
| C4 | api-server `ClpSMetadataReader`                                            | L1, L8        |
| C5 | Rust `archive-manager`                                                     | 3, L1, L6, L7 |
| C6 | Rust `dataset-manager`                                                     | 3, L1, L3     |
| C7 | Staged compression coordinator (optional)                                  | L2            |
| S  | Switch (`!`)                                                               | all above     |

PR 2 (core) contains:

- The `clp-metadata-db` crate: `ClpDbClient`, `ClpDbConnection`, `ClpDbTransaction`.
- Every type: `DatasetId`, the integer `ArchiveId`, `UuidBytes` and the `ArchiveUuid` byte
  conversions, `ArchiveTimestampRange`, the descriptor and row structs, `ColumnType`,
  `SpaceSavings`, `DatasetConfig`, `DatasetDescriptor`, `Dataset`.
- The full `Error` enum.
- The helpers shared by several methods (`StoredArchiveUuid`, the `time_range` helper), so no two
  leaves add the same block (report v3 C2).
- The binary-collation decode of `DatasetName`.
- `schema.rs` with the final DDL of the 4 tables (including D1's long-span columns) and
  `ClpDbClient::create_tables`.
- `MetadataTables` and `Database::metadata_tables` in `clp-rust-utils`.

Notes:

- Critical path: 1, 2, L6 or L7, C5, S. The implementation plan's PR 9 (+2260) becomes L6, L7 and
  C5.
- The archive and dataset managers don't depend on the table creator's logic. They only need the
  `clp-package-cli` crate, which PR 3 creates.
- A trait's definition can't land ahead of its impls, since an impl must cover every method. Each
  library PR adds its methods to the trait, implements them for the handles, and adds its tests.
  Each PR appends its methods in the spec's order, to keep the rebases trivial. Every rebase reruns
  the gate.
- Library PRs need no `expect(dead_code)`: their items are public in a library crate. Only staged
  consumer code uses the MOD-9 expects, and the switch removes them.
- Merge rule: merge all methods of a trait before any consumer whose tests mock that trait.
  Consumers can be written and reviewed in parallel, stacked on the library PRs they use, so no
  consumer carries another consumer's mock stubs (report v3 C1).
- Small neighbours (L2 and L3, L1 and L8) may be combined if reviewers prefer fewer PRs.
  `create_or_get_dataset`, `delete_archives` and `find_archives_for_query` stay in their own PRs.
- OQ1 (column-name collation) must be decided before PR 2, which fixes the DDL.

## 2. Testing scope of this series

No test in this series runs against a database, and CI gets no database.

| What                       | Tests                                                                |
|----------------------------|----------------------------------------------------------------------|
| Core types                 | Unit tests: `ArchiveTimestampRange`, UUID bytes, `ColumnType`, `DatasetName` decode |
| Library methods            | Unit tests of the pure logic, factored into plain functions: input alignment, missing IDs, the long-span threshold, batching. The SQL itself isn't tested. |
| Services (C3, C4, C7)      | Unit tests against mocks of the traits they're bounded on            |
| tdl commit (C2)            | None. It runs statements on `sqlx_transaction()`, so a mock can't stand in. |
| CLIs (C1, C5, C6)          | None (thin CLIs). Validated manually in the package.                 |
| Switch                     | Docker Compose e2e that reaches every library method (below)         |

Prototype tests that use the test database have to become mock-based or move to the follow-up work
in section 3. Likely candidates: the tdl deleted-dataset test, the table creator's `Deleted` arm,
and the CLI `tests/commands.rs`.

The repo's SQL is checked only at run time, so the switch e2e is the first time the library's SQL
runs. It must reach every method at least once:

| Library method                                                 | e2e flow at the switch              |
|----------------------------------------------------------------|-------------------------------------|
| `create_or_get_dataset`, `insert_archives`                     | Compression, including two jobs that create the same new dataset at once |
| `get_dataset_ids_by_names`, `find_archives_for_query`          | Search within and across datasets   |
| `get_datasets`, `get_dataset_by_name`, `MetadataStatistics`    | Every WebUI metadata route          |
| `find_archives_by_time_range`, `find_archives_by_ids`, `delete_archives` | `archive-manager find`, `del by-ids`, `del by-filter` |
| `find_archive_ids_by_uuids`                                    | `archive-manager del by-ids --uuid` (run it explicitly) |
| `mark_dataset_deleted`                                         | `dataset-manager del`, then compression into that dataset must fail |

## 3. Follow-up work: MariaDB/MySQL integration

Planned after this series, not part of v0.15.0's PRs. Both items need a MariaDB/MySQL server in CI,
which this series doesn't set up.

### 3.1 Unit tests against a database, including concurrent access

- A MariaDB test container and its CI path (the prototype has both, behind
  `CLP_METADATA_DB_TEST_SERVER_URL`).
- A `TestDb` fixture that creates a fresh database per test and seeds datasets and archives with
  plain SQL, so a method's tests don't depend on other library methods. One test asserts that
  `insert_archives` and the seeding write identical rows (long-span rows for archives spanning more
  than 24 hours, `creation_timestamp_millisecs`, `0/0` for archives without timestamps).
- A test per library method for its SQL and its documented errors.
- Concurrent-access tests for the guarantees the spec's "Locking" section states. Run them on a
  multi-threaded runtime with a barrier, many callers and many iterations (TEST-4):
  - Concurrent `create_or_get_dataset` calls with the same name all get the same ID and use up no
    extra auto-increment values.
  - `insert_archives` and `mark_dataset_deleted` on the same dataset never interleave: once the mark
    commits, no insert succeeds.
  - Concurrent `delete_archives` calls on overlapping IDs don't deadlock (the per-primary-key
    deletes).
  - `IN` lists and multi-row inserts of more than 1000 items.
- The prototype tests that this series removes or turns into mock tests (section 2) come back here.

### 3.2 sqlx static check (`query!`)

Use sqlx's compile-time checked macros (`query!`, `query_as!`, `query_scalar!`), so that SQL
syntax, table and column names, bind-parameter counts and result types are checked at build time.

- `sqlx-macros` is already in `Cargo.lock` through sqlx's default features; no new dependency.
- Offline mode: commit the `.sqlx/` query cache, so normal builds need no database. A CI job creates
  the schema on MariaDB/MySQL and runs `cargo sqlx prepare --check`.
- The macros take only a string literal, not `format!` or `formatcp!` output. The table names have
  to be written as literals. `table_prefix` is fixed to `clp_`, so `MetadataTables`' runtime names
  can be removed then.
- Custom types need overrides in the SQL (`SELECT id AS "id: DatasetId"`, and the same for
  `ArchiveId`, `ArchiveUuid`, `DatasetName`). Aggregates (`SUM`, `MIN`, `MAX`) need `"x!"` or
  `"x?"` overrides for their nullability.
- Statements built with `QueryBuilder` stay checked at run time: the batched `IN (...)` lookups,
  `delete_archives`' locking read, and the multi-row `insert_archives`.
- Order:
  1. `clp-metadata-db`.
  2. The job-table SQL in the coordinators, tdl and api-server, which build statements with
     `formatcp!` constants. This part is mechanical.
- First step: classify every statement in the prototype as checkable as is, checkable after
  inlining table names, or runtime-only. Then try one crate under `cargo sqlx prepare` against
  MariaDB to confirm the type inference. This needs the prototype branch.

## 4. Open items

1. Xuhui pushes the v3 stack and the `v3/*` leaf branches, to confirm which prototype tests use the
   database and to classify the statements for 3.2.
2. OQ1 (column-name collation), before PR 2.

## 5. Review notes (Xuhui, checked against the v3 stack `3364442a`)

Sections 1-4 above are the plan as received. This section records the outcome of checking it
against the v3 code.

### 5.1 Confirmed

- The consumer dependencies in section 1 match the library methods each consumer calls in v3.
- No library method calls another, so the per-method split works.
- PR 3 depends on PR 2: the scaffold's `connect` builds a `ClpDbClient`.
- `sqlx-macros` is in `Cargo.lock`, and `table_prefix` is fixed to `clp_`.

### 5.2 Corrections

1. **Testing scope (section 2).** The DB tests aren't dropped: they run locally against MariaDB for
   every PR and aren't added to CI until 3.1 sets up the infrastructure. Suggested wording for
   section 2: "DB tests run locally and aren't added to CI until the follow-up." The prototype's DB
   tests are kept on a branch so that 3.1 can bring them back.
2. **Auto-increment assertion (3.1).** Concurrent `create_or_get_dataset` calls for the same new
   name can use up auto-increment values: each insert that loses the race uses one (the spec's
   "Dataset ID width" note). Only calls for an existing name use none. The concurrent test asserts
   the same ID, plus no values used for an existing name. This doesn't change any behavior.
3. **The long-span query branch needs a test (L5).** v3's `find_archives_for_query` has no lower
   bound, so its test doesn't reach the long-span branch. The `begin >= lower - 24h` bound, the
   long-span branch and D1's `uuid` and `num_uncompressed_bytes` columns are new code. L5 adds a
   local DB test in which an archive spanning more than 24 hours is found only through the
   long-span branch, with the right `uuid` and `num_uncompressed_bytes`. Insert and delete already
   cover the long-span rows in v3's tests.
4. **sqlx classification (3.2).**
   - `delete_archives`' locking read isn't built with `QueryBuilder`: it's a plain
     `SELECT ... WHERE id = ? FOR UPDATE` per ID, so `query!` can check it.
   - The runtime-only statements include `find_archives_for_query` and
     `find_archives_by_time_range`, whose bounds are optional.
   - Those bounds stay built at run time. Writing them as `(? IS NULL OR begin >= ?)` to fit
     `query!` would keep range analysis from getting a constant range, which brings back the
     partition planning cost measured in the partitioning benchmark.

### 5.3 Open item 1: prototype tests that use the database

| Location                                         | DB tests |
|--------------------------------------------------|----------|
| `clp-metadata-db/tests/metadata_db.rs`           | 26       |
| `clp-package-cli/tests/commands.rs`              | 8        |
| tdl `task/compression/commit.rs`                 | 3        |
| compression-coordinator `job_handle.rs`          | 3        |

All of them read `CLP_METADATA_DB_TEST_SERVER_URL`. In v3, `taskfiles/tests/main.yaml` starts a
`mariadb:10.11.16` container for them. The query-coordinator archive selector (3 tests) and the
api-server metadata reader (14 tests) already use mocks.
