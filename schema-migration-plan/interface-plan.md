# clp-s shared metadata tables: interface plan

## Goal

clp-s archive metadata moves from per-dataset tables (`clp_<dataset>_archives`,
`clp_<dataset>_column_metadata`) to tables that every dataset shares, keyed by dataset ID. Every
database operation on these tables goes through one Rust library, `clp-metadata-db`
(`components/clp-rust-libs/clp-metadata-db`): typed Rust functions implemented with `sqlx`
underneath. Every function takes an arbitrary `&mut sqlx::MySqlConnection`, so a caller can run any
combination of operations inside its own transaction (`&mut *tx`) or on a plain pooled connection.
The library never creates pools or transactions.

The library covers both the operations of the pre-switch components (API management routes, archive
GC) and the ones the switch moves onto the shared tables (compression, commit, query planning, WebUI
metadata). One exception: the C++ indexer keeps writing column metadata itself until clp-s is
rewritten in Rust (see "The indexer").

Out of scope: clp-text (`clp` storage engine) tables, the query/compression job tables, files tables,
top-level-filtering tables, and the WebUI ingestion-details route (its file and message counts need a
files table, which clp-s doesn't populate).

## Conventions

* Timestamps carry their unit as a `_timestamp_millisecs` suffix, in both columns and Rust fields
  (e.g. `begin_timestamp_millisecs`). Durations keep their own unit suffix
  (`retention_period_minutes`).
* String columns are `CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`, so dataset and column names are
  case-sensitive (`Logs` and `logs` are different datasets). Known limitation: `utf8mb4_bin` pads
  with spaces when comparing, so column names that differ only in trailing spaces (`"a"` vs `"a "`)
  still collide. Dataset names can't contain spaces.
* Times that several components must agree on (archive creation, dataset deletion) come from the DB
  clock.

## DB table changes

The Before/After tables compare the current per-dataset schema with this plan. The "Diff from
previous doc" notes compare this plan with
[Metadata database design and schema](https://app.notion.com/p/yscope/Metadata-database-design-and-schema-3c804e4d9e6b809ebe63cb3926c0e0ff).

### `clp_datasets` (changed)

| Before | After | Notes |
|---|---|---|
| — | `id SMALLINT UNSIGNED AUTO_INCREMENT` (primary key) | Every shared table refers to a dataset by this ID |
| `name VARCHAR(255)` (primary key) | `name VARCHAR(255)` | No longer the key; case-sensitive |
| `archive_storage_directory VARCHAR(4096)` | `archive_storage_path VARCHAR(4096)` | Written at creation; never read (open question 2) |
| — | `retention_period_minutes INT UNSIGNED NULL` | Per-dataset retention; `NULL` keeps archives forever. Replaces the package-wide `archive_output.retention_period` |
| — | Proposed: `deletion_timestamp_millisecs BIGINT NOT NULL DEFAULT 0` | `0` = live; otherwise the soft-deletion time |
| Unique: `name` | Proposed: `(name, deletion_timestamp_millisecs)` | Name reuse requires safe storage isolation and retry handling; the key alone is insufficient |

Datasets are no longer registered implicitly by compression; they must be created through the API
first. The deletion timestamp, unique key, and deleted-row retention policy are proposals pending
the deletion lifecycle review below.

**Diff from previous doc**

* Propose replacing `is_deleted BOOLEAN` with `deletion_timestamp_millisecs BIGINT NOT NULL DEFAULT 0`,
  recording deletion time using the DB clock. The deletion representation remains under review.
* Propose replacing the unique key on `name` with `(name, deletion_timestamp_millisecs)`. This permits
  name reuse in the DB, but does not make storage reuse safe. The key and deleted-row retention
  policy remain under review.
* Specify `utf8mb4_bin` collation for string columns; the previous doc specifies only `utf8mb4`.
* Keep `id`, `archive_storage_path`, and `retention_period_minutes`, which already exist in the
  previous doc. The current proposal derives archive locations from package config; the location
  source and safe deletion targets must be settled in open question 2 below.

### `clp_<dataset>_archives` → `clp_archives` (one table for every dataset)

| Before | After | Notes |
|---|---|---|
| `pagination_id BIGINT UNSIGNED AUTO_INCREMENT` (primary key) | `id BIGINT UNSIGNED AUTO_INCREMENT` | Surrogate key, referenced by `clp_long_span_archives` |
| `id VARCHAR(64)` (unique) | `uuid BINARY(16)` (indexed with `dataset_id`, not unique) | A partitioned table's unique keys must include the partitioning column |
| — | `dataset_id SMALLINT UNSIGNED` | Replaces the dataset in the table name |
| `begin_timestamp BIGINT` | `begin_timestamp_millisecs BIGINT` | |
| `end_timestamp BIGINT` | `end_timestamp_millisecs BIGINT` | |
| `uncompressed_size BIGINT` | `num_uncompressed_bytes BIGINT UNSIGNED` | |
| `size BIGINT` | `num_compressed_bytes BIGINT UNSIGNED` | |
| `creator_id VARCHAR(64)`, `creation_ix INT` | — | Never set by clp-s |
| — | `creation_timestamp_millisecs BIGINT` | DB clock at commit; drives per-dataset retention |
| — | `compression_job_id INT` | The job that produced the archive |
| — | `pack_id BIGINT UNSIGNED NULL` | Reserved for archive packing |
| — | `is_deleted BOOLEAN NOT NULL DEFAULT FALSE` (review baseline) | Preserve the previous doc's logical-deletion state until a durable alternative is approved |

Keys: primary key `(dataset_id, begin_timestamp_millisecs, id)`; indexes on `id`,
`(dataset_id, uuid)` and `(dataset_id, creation_timestamp_millisecs)` (for the GC).
`ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8`. Range-partitioned by `begin_timestamp_millisecs`, with a
single `p_future` partition until partition maintenance exists.

New archive UUIDs are generated as UUIDv7 during clp-s ingestion, including after archive splits.
Existing UUIDv4 archives retain their IDs; `ArchiveId` continues accepting both versions. UUID
generation changes independently of this schema migration. Both versions occupy 16 bytes in
`uuid BINARY(16)`.

The UUIDv7 timestamp comes from the ingestion host's clock and does not define archive retention.
`creation_timestamp_millisecs` still comes from the DB clock at commit and drives retention.

**Diff from previous doc**

* Keep the previous doc's `is_deleted` as the review baseline. Its proposed removal is withdrawn
  pending approval of an equivalent durable deletion mechanism. A flag alone does not define
  cleanup ownership, concurrent-job protection, or recovery.
* Add `compression_job_id INT` to identify the job that produced each archive.
* Rename `timestamp_range_begin_millis`, `timestamp_range_end_millis`, and `creation_time_millis`
  to `begin_timestamp_millisecs`, `end_timestamp_millisecs`, and `creation_timestamp_millisecs`.
  The primary key and indexes use the renamed columns with the same structure.
* Set creation time from the DB clock at commit. The previous doc describes archive seal time from
  "our own clock" without requiring the DB clock.
* Create only `p_future` initially. The previous doc describes `p_floor`, time-window partitions,
  and their maintenance; those steps and the associated query pruning are deferred here.
* Keep `uuid BINARY(16)`, UUIDv7 for new archives, and nullable `pack_id`, matching the previous
  doc. `ArchiveId` continues accepting existing UUIDv4 IDs.

### `clp_<dataset>_column_metadata` → `clp_column_metadata`

| Before | After | Notes |
|---|---|---|
| — | `dataset_id SMALLINT UNSIGNED` | Replaces the dataset in the table name |
| `name VARCHAR(512)` | `name VARCHAR(512)` | Case-sensitive |
| `type TINYINT` | `type TINYINT` | `clp_s::NodeType` |
| Primary key `(name, type)` | Primary key `(dataset_id, name, type)` | |

**Diff from previous doc**

* Specify `utf8mb4_bin` collation for `name`; the previous doc specifies only `utf8mb4`.
* Keep the previous doc's columns, types, 512-character name limit, and primary key
  `(dataset_id, name, type)`. Replacing the union-of-columns model is outside this step.
* Define a Rust read interface while retaining the C++ indexer as the writer until the Rust clp-s
  rewrite; the previous doc does not specify this implementation split.

### `clp_<dataset>_files` (no longer created for clp-s)

clp-s never wrote rows to it. A shared files table is future work.

**Diff from previous doc**

* Defer both shared files/file-splits alternatives described in the previous doc. This migration
  creates neither and stops creating the unused per-dataset files table for clp-s.
* File-to-archive mappings, the WebUI ingestion-details file/message counts, and stream extraction
  are outside this plan's scope.

### New: `clp_long_span_archives`

A second reference to every archive whose timestamp range spans more than 24 hours, so a future
partition-pruned search can still find them. Unpartitioned. Written now; read once partition
maintenance exists.

| Column | Notes |
|---|---|
| `dataset_id SMALLINT UNSIGNED` | |
| `begin_timestamp_millisecs BIGINT` | |
| `end_timestamp_millisecs BIGINT` | |
| `archive_id BIGINT UNSIGNED` | `clp_archives.id` |

Keys: primary key `(dataset_id, end_timestamp_millisecs, archive_id)`; index on
`(dataset_id, begin_timestamp_millisecs)`.

**Diff from previous doc**

* This table already exists in the previous doc; "New" refers to its addition to the current
  per-dataset schema.
* Rename `timestamp_range_begin_millis` and `timestamp_range_end_millis` to
  `begin_timestamp_millisecs` and `end_timestamp_millisecs`. Keep the columns, primary-key/index
  structure, and unpartitioned layout otherwise unchanged.
* Fix the long-span threshold at 24 hours; the previous doc leaves it as `MAX_SPAN`.
* Write the additional references now, but defer reading them until partition maintenance exists.
  The previous doc describes scanning this table on every time-ranged query. Until then, queries
  read overlapping archives from `clp_archives` without the `MAX_SPAN` lower-bound prune.

## Deletion lifecycle (review required)

The previous doc provides logical-deletion flags, but does not specify a complete concurrency or
recovery protocol. The earlier storage-first dataset deletion and metadata-first archive deletion
sequences were inherited from existing implementations; neither is an approved protocol for this
migration. Extracting SQLx interfaces does not require preserving those sequences.

Deletion-related DDL and interfaces below are provisional. Review must settle the following
requirements before those interfaces are frozen or their SQL is implemented:

1. **Durable intent.** Commit logical deletion and durable cleanup targets atomically in the DB,
   using tombstones or deletion-job records whose representation is still to be decided. Retain
   enough state to discover and retry unfinished storage cleanup after a process restart.
2. **Concurrent work.** Prevent new queries and ingestion from selecting logically deleted data.
   Define whether existing queries finish or are cancelled, and how compression, indexing, storage
   publication, and metadata commits are drained or fenced. A live-dataset check at job submission
   alone cannot prevent a later write after deletion begins. Jobs must remain bound to the original
   dataset ID or generation rather than resolve a reused name at commit time. Storage cleanup must
   wait for the chosen coordination condition, including queries that already hold archive IDs.
3. **Stable cleanup targets.** Persist the immutable dataset/archive identity and exact original
   storage location needed by each retry. Do not derive retry targets from a reused dataset name
   or the current package config. Dataset-prefix deletion must not overlap a new dataset's files.
4. **Safe retries.** Cleanup must be idempotent and recover from partial storage failures, repeated
   requests, and crashes. A recovery-file entry or candidate persisted before a DB transaction
   commits must not authorize physical deletion if that transaction rolls back. Define recovery
   ownership and replay semantics for both manual API deletion and retention GC; the GC's local
   recovery file alone does not establish those guarantees.
5. **Name reuse.** Permit recreation only after cleanup is durably complete and stale cleanup or
   writer tasks cannot affect the reused location, or after storage and in-flight work are safely
   isolated by immutable identity. The proposed timestamp-based unique key is not a concurrency
   fence, and must also handle deletion-timestamp collisions.
6. **Final reclamation.** Hard-delete archive and dependent metadata only after physical cleanup
   and the concurrency requirements are satisfied. Decide when dataset rows and deletion records
   can be reclaimed without losing retry ownership or allowing stale work to affect new data.

The review must choose the deletion state and transaction boundaries, the storage-location source,
the handling of in-flight jobs, and the dataset-row retention/ID-lifetime policy. This plan does not
choose between a timestamp, a boolean tombstone, and a separate deletion-job record. Crash-safe
deletion recovery is required before the runtime switch, rather than deferred follow-up work.

## The library

### Layout

The layout and `MetadataTables` below show the four shared metadata tables. Any additional cleanup
record types, table names, or modules depend on deletion review.

```text
clp-metadata-db/src/
├── lib.rs              MetadataTables, re-exports
├── error.rs            Error
├── types.rs            DatasetId, ArchiveRowId, TimestampRange, ColumnType
├── schema.rs           create_tables
├── dataset.rs          dataset operations
├── archive.rs          archive operations
├── column_metadata.rs  column-metadata reads
└── statistics.rs       aggregate reads for the WebUI metadata routes
```

### Entry point

```rust
/// The clp-s metadata tables that every dataset shares, identified by their names.
#[derive(Clone, Debug)]
pub struct MetadataTables {
    datasets: String,
    archives: String,
    long_span_archives: String,
    column_metadata: String,
}

impl MetadataTables {
    pub fn new(database_config: &clp_rust_utils::clp_config::package::config::Database) -> Self;
}
```

### Types

| Type | Definition | Why |
|---|---|---|
| `DatasetId` | `struct DatasetId(u16)`, `#[sqlx(transparent)]`; replaces the current `u16` alias in `clp-rust-utils` | Can't be mixed up with other integers |
| `ArchiveRowId` | `struct ArchiveRowId(u64)`, `#[sqlx(transparent)]` | `clp_archives.id`; distinct from the archive's UUID |
| `ArchiveId` | existing UUID type in `clp-rust-utils`, plus `from_bytes` / `as_bytes` | Stored as `BINARY(16)`; the text SQL encoding stays for other tables |
| `TimestampRange` | `{ begin_timestamp_millisecs: i64, end_timestamp_millisecs: Option<i64> }` | Named bounds instead of two adjacent `i64`s |
| `ColumnType` | `#[repr(u8)]` enum mirroring `clp_s::NodeType` (`components/core/src/clp_s/SchemaTree.hpp`) | Typed `type` column; replaces the magic numbers in `webui_client/metadata.rs` |
| `CompressionJobId`, `QueryJobStatus` | existing types in `clp-rust-utils` | |

### Errors

```rust
pub enum Error {
    /// A live dataset with the same name already exists.
    DatasetAlreadyExists(DatasetName),
    /// Forwarded `sqlx` errors, including row decode errors (e.g. a UUID that isn't 16 bytes).
    Sqlx(sqlx::Error),
}
```

### Rules every operation follows

* Every archive and column-metadata statement is scoped by `dataset_id`, unless it's explicitly
  cross-dataset.
* Normal dataset reads exclude logically deleted datasets; the proposed timestamp representation
  uses `deletion_timestamp_millisecs = 0`. Archive selection and archive aggregates also exclude
  logically deleted archives. Cleanup queries explicitly opt into pending-deletion state.
* `IN` lists and multi-row inserts are batched at 1000.
* During final metadata reclamation, remove `clp_long_span_archives` rows before `clp_archives` rows.
  This ordering does not replace the deletion lifecycle requirements above.

## New interfaces, by the Python code they replace

"Tx: yes" marks operations the caller is expected to wrap in a transaction with other operations.
Each operation's signature is given where it first appears. Deletion signatures and their mappings
are provisional: they show candidate DB primitives, not a complete deletion-request API. The review
must define how callers persist intent, claim cleanup work, retry it, and record completion.

### `clp_py_utils/clp_metadata_db_utils.py`, `clp_py_utils/initialize-clp-metadata-db.py`

| Python | What it did | New interface | Rust caller |
|---|---|---|---|
| `create_datasets_table`, `create_metadata_db_tables` | Created the datasets table and each dataset's archives/files/column-metadata tables | `create_tables` | `db-table-creator` |
| `add_dataset` | Inserted a dataset and created its tables, implicitly on first compression | `create_dataset` | API `POST /datasets` |
| `fetch_existing_datasets` | Listed dataset names | `get_dataset_names` | API, WebUI routes |
| `delete_archives_from_metadata_db` | Deleted archives (and their files rows) by ID | Deletion-request interface pending review; `delete_archives` is final reclamation only | API, archive GC |
| `delete_dataset_from_metadata_db` | Dropped the dataset's tables and its datasets row | Deletion-request interface pending review; `mark_dataset_deleted` and `delete_dataset_metadata` are candidate primitives | API, archive GC |
| `get_*_table_name` | Built per-dataset table names | `MetadataTables::new` | All |

```rust
impl MetadataTables {
    /// Creates the shared tables if they don't exist. Not a migration: an existing table with another
    /// shape is left as is.
    pub async fn create_tables(&self, conn: &mut MySqlConnection) -> Result<(), Error>;

    /// Errors with `DatasetAlreadyExists` on a unique-key violation.
    pub async fn create_dataset(
        &self,
        conn: &mut MySqlConnection,
        dataset: &NewDataset,
    ) -> Result<DatasetId, Error>;

    /// Live datasets, ordered by name.
    pub async fn get_dataset_names(&self, conn: &mut MySqlConnection)
        -> Result<Vec<DatasetName>, Error>;

    /// Provisional logical-deletion primitive; returns whether a live dataset was marked using DB
    /// time. Must participate in the reviewed intent transaction; not a complete deletion request.
    pub async fn mark_dataset_deleted(&self, conn: &mut MySqlConnection, id: DatasetId)
        -> Result<bool, Error>;
}

pub struct NewDataset {
    pub name: DatasetName,
    pub archive_storage_path: String,
    pub retention_period_minutes: Option<NonZeroU32>,
}
```

`delete_archives` is listed under `archive_manager.py`, `delete_dataset_metadata` under
`archive_garbage_collector.py`.

### `clp_package_utils/scripts/native/dataset_manager.py`

Replaced by the API's `GET /datasets` and `DELETE /datasets/{name}`. The deletion request must follow
the reviewed lifecycle: commit logical deletion with durable cleanup intent, coordinate in-flight
work, clean the original storage location with retries, then reclaim eligible metadata. Deleting
storage while the dataset is still live is not the proposed workflow. Response/completion semantics
and the required DB interfaces remain to be reviewed.

| Python | What it did | New interface |
|---|---|---|
| `_get_dataset_info` | Listed names and storage directories | `get_dataset_names` |
| `_delete_dataset`, `_delete_dataset_from_database` | Deleted storage (location from the datasets table), then dropped the tables | Resolve the original dataset identity; deletion-request interface pending review |

```rust
impl MetadataTables {
    /// Live dataset only.
    pub async fn get_dataset_id_by_name(
        &self,
        conn: &mut MySqlConnection,
        name: &DatasetName,
    ) -> Result<Option<DatasetId>, Error>;
}
```

### `clp_package_utils/scripts/native/archive_manager.py`

Replaced by the API's `GET /datasets/{name}/archives` and `POST /datasets/{name}/archives/delete`.
Deletion must preserve logical-deletion state and durable cleanup targets through storage retries.
The lookup, concurrency protection, intent transaction, and eventual reclamation follow the
reviewed lifecycle; deleting metadata and then attempting storage cleanup is not a complete protocol.
No dry-run.

| Python | What it did | New interface |
|---|---|---|
| `_find_archives` | Archive IDs with `begin_timestamp >= begin` and `end_timestamp <= end` | `find_archives_by_time_range` |
| `_delete_archives` (by IDs) | Looked up then deleted archives by ID | `find_archives_by_uuids`; deletion-request interface pending review |
| `_delete_archives` (by filter) | Looked up then deleted archives in a time range | `find_archives_by_time_range`; deletion-request interface pending review |

```rust
impl MetadataTables {
    /// Archives whose timestamp range lies within `range`.
    pub async fn find_archives_by_time_range(
        &self,
        conn: &mut MySqlConnection,
        dataset_id: DatasetId,
        range: &TimestampRange,
    ) -> Result<Vec<Archive>, Error>;

    pub async fn find_archives_by_uuids(
        &self,
        conn: &mut MySqlConnection,
        dataset_id: DatasetId,
        uuids: &[ArchiveId],
    ) -> Result<Vec<Archive>, Error>;

    /// Provisional final metadata reclamation after storage cleanup and concurrency protection.
    /// Removes long-span rows before archive rows. Tx: yes; not a deletion-request interface.
    pub async fn delete_archives(
        &self,
        conn: &mut MySqlConnection,
        dataset_id: DatasetId,
        ids: &[ArchiveRowId],
    ) -> Result<(), Error>;
}

pub struct Archive {
    pub id: ArchiveRowId,
    pub uuid: ArchiveId,
}
```

### `clp_package_utils/scripts/native/utils.py`

| Python | What it did | New interface |
|---|---|---|
| `validate_dataset_exists` (via `fetch_existing_datasets`) | Checked that datasets given to the CLI scripts exist | `get_dataset_ids_by_names` (the check moves to the components that run the job) |

### `job_orchestration/scheduler/compress/compression_scheduler.py`

The Rust compression coordinator already replaces it.

| Python | What it did | New interface |
|---|---|---|
| `fetch_existing_datasets` + `add_dataset` | Registered the job's dataset if it didn't exist | `get_dataset_id_by_name`; the job fails if the dataset is missing or deleted |

### `job_orchestration/executor/compress/compression_task.py`

The Rust compress and commit tasks already replace it.

| Python | What it did | New interface |
|---|---|---|
| `update_archive_metadata` | Inserted one archive's stats from `clp-s --print-archive-stats` | `insert_archives`, in the commit's transaction |
| Running the `indexer` | Wrote the archive's columns to the column-metadata table | Unchanged: the C++ indexer (see "The indexer") |

```rust
impl MetadataTables {
    /// Inserts the archives with `creation_timestamp_millisecs` from the DB clock, then a
    /// `clp_long_span_archives` row for each archive spanning more than 24 hours. Tx: yes, with the
    /// job's status update.
    pub async fn insert_archives(
        &self,
        conn: &mut MySqlConnection,
        dataset_id: DatasetId,
        compression_job_id: CompressionJobId,
        archives: &[NewArchive],
    ) -> Result<(), Error>;
}

pub struct NewArchive {
    pub uuid: ArchiveId,
    pub begin_timestamp_millisecs: i64,
    pub end_timestamp_millisecs: i64,
    pub num_uncompressed_bytes: u64,
    pub num_compressed_bytes: u64,
}
```

### `job_orchestration/scheduler/query/query_scheduler.py`

The Rust query coordinator already replaces search scheduling.

| Python | What it did | New interface |
|---|---|---|
| `fetch_existing_datasets` | Validated the job's datasets | `get_dataset_ids_by_names` |
| `get_archives_for_search` | Per-dataset `UNION ALL` of archives overlapping the query's time range, with a package-wide retention bound on `end_timestamp` | `find_archives_for_query`, with per-dataset retention by creation time |
| `archive_exists` | Checked the archive of a stream extraction | — (stream extraction is out of scope) |
| `get_archive_and_file_split_ids` | Files-table lookup for IR extraction | — (clp-text only) |

```rust
impl MetadataTables {
    /// Live datasets only; names that don't resolve are absent.
    pub async fn get_dataset_ids_by_names(
        &self,
        conn: &mut MySqlConnection,
        names: &[DatasetName],
    ) -> Result<HashMap<DatasetName, DatasetId>, Error>;

    /// Live archives in a live dataset, overlapping the query's time range, within its retention
    /// relative to the job's creation
    /// (`creation_timestamp_millisecs >= job_creation_timestamp_millisecs - retention`).
    pub async fn find_archives_for_query(
        &self,
        conn: &mut MySqlConnection,
        dataset_id: DatasetId,
        filter: &QueryArchiveFilter,
    ) -> Result<Vec<QueryArchive>, Error>;
}

pub struct QueryArchiveFilter {
    /// Inclusive bounds of the query's time range; `None` is unbounded on that side.
    pub begin_timestamp_millisecs: Option<i64>,
    pub end_timestamp_millisecs: Option<i64>,
    pub job_creation_timestamp_millisecs: i64,
}

pub struct QueryArchive {
    pub uuid: ArchiveId,
    pub num_uncompressed_bytes: u64,
    pub end_timestamp_millisecs: i64,
}
```

### `job_orchestration/garbage_collector/archive_garbage_collector.py`

Replaced by the standalone Rust `archive-garbage-collector`. Search-result GC stays in Python.

| Python | What it did | New interface |
|---|---|---|
| `_get_archive_safe_expiry_epoch` | Earliest creation time of `RUNNING` query jobs | `query_job::get_earliest_creation_timestamp_millisecs` over every non-terminal status (`PENDING`, `RUNNING`, `CANCEL_REQUESTED`, `CANCELLING`) |
| `_delete_expired_archives` | Per dataset, archives with `end_timestamp` before the package-wide retention, then `delete_archives_from_metadata_db` | `find_expired_archives` (per-dataset retention by creation time), then the reviewed deletion lifecycle |
| — | — | `get_deleted_dataset_ids` discovers candidates; `delete_dataset_metadata` only after cleanup and concurrency prerequisites |

```rust
impl MetadataTables {
    /// Live archives of live datasets with a retention period, created before
    /// `expiry_base_timestamp_millisecs - retention`. Cross-dataset.
    pub async fn find_expired_archives(
        &self,
        conn: &mut MySqlConnection,
        expiry_base_timestamp_millisecs: i64,
    ) -> Result<Vec<ExpiredArchive>, Error>;

    /// Candidate discovery only; does not establish that reclamation is safe.
    pub async fn get_deleted_dataset_ids(&self, conn: &mut MySqlConnection)
        -> Result<Vec<DatasetId>, Error>;

    /// Provisional final reclamation of `clp_long_span_archives`, `clp_archives` and
    /// `clp_column_metadata` after storage cleanup and concurrency protection.
    /// Dataset-row retention is pending review. Returns the number of deleted rows. Tx: yes.
    pub async fn delete_dataset_metadata(&self, conn: &mut MySqlConnection, id: DatasetId)
        -> Result<u64, Error>;
}

pub struct ExpiredArchive {
    pub archive: Archive,
    pub dataset_id: DatasetId,
    pub dataset_name: DatasetName,
}
```

The query-job tables aren't metadata tables, so this helper stays in
`clp_rust_utils::database::mysql::query_job`, but it takes a connection like the rest:

```rust
pub async fn get_earliest_creation_timestamp_millisecs(
    conn: &mut MySqlConnection,
    statuses: &[QueryJobStatus],
) -> Result<Option<i64>, crate::Error>;
```

### Not from Python: the WebUI's metadata SQL

The WebUI client builds raw SQL against the per-dataset tables
(`components/webui/packages/client/src/pages/**/sql.ts`, sent through `/api/archive-metadata/sql`).
The api-server's WebUI metadata routes (`components/api-server/src/webui_client/metadata.rs`) replace
it; their clp-s branches use:

| Route | New interface |
|---|---|
| Dataset list | `get_dataset_names` |
| Time range | `get_timestamp_bounds` |
| Space savings | `get_space_savings` |
| Query speed | `get_uncompressed_bytes_by_uuids` |
| Timestamp column names | `get_column_names_by_types` |

```rust
impl MetadataTables {
    pub async fn get_timestamp_bounds(&self, conn: &mut MySqlConnection, ids: &[DatasetId])
        -> Result<Option<TimestampBounds>, Error>;

    /// Over live archives in live datasets.
    pub async fn get_space_savings(&self, conn: &mut MySqlConnection)
        -> Result<SpaceSavings, Error>;

    pub async fn get_uncompressed_bytes_by_uuids(
        &self,
        conn: &mut MySqlConnection,
        dataset_ids: &[DatasetId],
        uuids: &[ArchiveId],
    ) -> Result<u64, Error>;

    pub async fn get_column_names_by_types(
        &self,
        conn: &mut MySqlConnection,
        dataset_id: DatasetId,
        types: &[ColumnType],
    ) -> Result<Vec<String>, Error>;
}

pub struct TimestampBounds {
    pub begin_timestamp_millisecs: i64,
    pub end_timestamp_millisecs: i64,
}

pub struct SpaceSavings {
    pub num_uncompressed_bytes: u64,
    pub num_compressed_bytes: u64,
}
```

The query-speed route currently joins `query_jobs` / `query_tasks` with the archives table in one
statement. `query_tasks.archive_id` (text UUID) doesn't match `clp_archives.uuid` (bytes), so the
route reads the job's archive IDs from the job tables and passes them to
`get_uncompressed_bytes_by_uuids` instead.

## The indexer

`components/core/src/clp_s/indexer` is a separate C++ executable that the compress task runs on each
archive. It's the only C++ code that writes to the database: it opens the archive, walks the schema
tree, and `INSERT IGNORE`s `(path, NodeType)` into the dataset's column-metadata table.

Decision (Devin): keep the C++ indexer until clp-s is rewritten in Rust. At the switch, its SQL is
ported to the shared `clp_column_metadata` table. The write must remain bound to the ingestion job's
original dataset ID or generation and follow the reviewed deletion fencing protocol. Resolving a
dataset name afresh at indexing time could write into a replacement dataset; a live-state predicate
alone does not coordinate an already-running indexer with deletion.

`--create-table` and the per-dataset table name go away. When the Rust rewrite replaces the indexer,
it must reproduce the key escaping (`escape_key_name`) and the skipped node types
(`StructuredArray`, `Object`, `Unknown`) exactly, and write through a new `insert_column_metadata`.

## Open questions

1. `DatasetId` is `SMALLINT UNSIGNED`; the proposed indefinite retention of deleted rows limits the
   lifetime to 65 535 datasets. Reclaiming rows alone does not reset an exhausted auto-increment
   counter. Review the ID width and retention/reuse policy, including stale-job safety.
2. The current proposal writes `archive_storage_path` at creation but derives archive locations
   from the current package config. Config changes leave existing archives at their old location.
   Decide the authoritative location source and immutable namespace before deletion is implemented;
   retries must retain exact original targets regardless of later config changes or name reuse.
3. Complete the deletion lifecycle review above before freezing deletion DDL and signatures. The
   timestamp/composite unique key, archive tombstone representation, request/completion semantics,
   in-flight work protection, and cleanup recovery ownership are not settled by these snippets.
