# clp-s shared metadata tables: interface plan

## Goal

Replace clp-s's per-dataset metadata tables (`clp_<dataset>_archives`,
`clp_<dataset>_column_metadata`, `clp_<dataset>_files`) with the shared tables from
[Metadata database design and schema][design-doc] (the "design doc"), keyed by dataset ID.

This first step is a minimal replacement: when the runtime switches to the new tables, every
non-Celery path on `main` keeps working, with the behavior described here. Features the design doc
describes beyond that (partition maintenance, files tables, top-level-filtering tables, packs) are
added afterward.

Every operation on the shared tables goes through one Rust library, `clp-metadata-db`
(`components/clp-rust-libs/clp-metadata-db`): typed Rust functions with SQL underneath, written with
`sqlx`. Each function takes an arbitrary `&mut sqlx::MySqlConnection`, so a caller can combine
operations inside its own transaction (`&mut *tx`) or run them on a pooled connection. The library
never creates pools or transactions. The one exception is the C++ indexer, which keeps writing
column metadata itself until clp-s is rewritten in Rust (see "The indexer").

## Scope

In scope, for the clp-s storage engine only:

* The four shared tables: `clp_datasets`, `clp_archives`, `clp_long_span_archives`,
  `clp_column_metadata`.
* Every current non-Celery reader and writer of the per-dataset tables: the compression coordinator,
  the Spider compression and commit tasks, the C++ indexer, the query coordinator, the api-server
  WebUI metadata routes, table creation, archive GC, and the dataset/archive management scripts.

Out of scope:

* clp-text (`clp` storage engine). Its tables and code paths stay as they are.
* The Celery compression scheduler and workers, which the compression coordinator replaces.
* The query/compression job tables.
* Files tables and top-level-filtering tables.
* Migrating data from existing per-dataset tables (see open question 1).

## Conventions

* Timestamps carry their unit as a `_timestamp_millisecs` suffix in both columns and Rust fields
  (e.g. `begin_timestamp_millisecs`). Durations keep their own unit suffix
  (`retention_period_minutes`).
* String columns are `CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`, so dataset and column names are
  case-sensitive. `utf8mb4_bin` ignores trailing spaces when comparing, so column names that differ
  only in trailing spaces still collide; dataset names can't contain spaces.
* Times that several components must agree on (archive creation, retention expiry) come from the
  DB clock.
* Table names are the configured table prefix (default `clp_`) followed by the names below.

## Tables

### `clp_datasets`

```sql
CREATE TABLE IF NOT EXISTS `clp_datasets` (
    `id`                       SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `name`                     VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    `archive_storage_path`     VARCHAR(4096) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    `is_deleted`               BOOLEAN NOT NULL DEFAULT FALSE,
    `retention_period_minutes` INT UNSIGNED DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `dataset_name` (`name`) USING BTREE
);
```

* Datasets are created only through `POST /datasets`. Compression to a missing or deleted dataset
  fails; nothing registers datasets implicitly and no `default` dataset is pre-provisioned.
* `archive_storage_path` is set at creation from the package's archive output config (the directory
  or S3 key prefix that compression writes the dataset's archives to). Deletion uses it.
* `retention_period_minutes` is set at creation; `NULL` keeps archives forever. It replaces the
  package-wide `archive_output.retention_period` for clp-s.
* `is_deleted` marks a dataset whose deletion is pending (see "Deletion"). The row is removed once
  cleanup finishes, which frees the name for reuse.

Compared with the current table: `name` is no longer the primary key, `archive_storage_directory`
is renamed to `archive_storage_path`, and `id`, `is_deleted` and `retention_period_minutes` are new.

Diff from the design doc: adds the `utf8mb4_bin` collation; defines `is_deleted` as "deletion
pending" and removes the row after cleanup.

### `clp_archives`

```sql
CREATE TABLE IF NOT EXISTS `clp_archives` (
    `id`                           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `dataset_id`                   SMALLINT UNSIGNED NOT NULL,
    `uuid`                         BINARY(16) NOT NULL,
    `begin_timestamp_millisecs`    BIGINT NOT NULL,
    `end_timestamp_millisecs`      BIGINT NOT NULL,
    `num_uncompressed_bytes`       BIGINT UNSIGNED NOT NULL,
    `num_compressed_bytes`         BIGINT UNSIGNED NOT NULL,
    `creation_timestamp_millisecs` BIGINT NOT NULL,
    `compression_job_id`           INT NOT NULL,
    `pack_id`                      BIGINT UNSIGNED DEFAULT NULL,
    `is_deleted`                   BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (`dataset_id`, `begin_timestamp_millisecs`, `id`),
    KEY `auto_inc` (`id`),
    KEY `archive_uuid` (`dataset_id`, `uuid`),
    KEY `archives_gc_order` (`dataset_id`, `creation_timestamp_millisecs`)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8
PARTITION BY RANGE (`begin_timestamp_millisecs`) (
    PARTITION `p_future` VALUES LESS THAN MAXVALUE
);
```

| Current column | New column | Notes |
|---|---|---|
| `pagination_id` (primary key) | `id` | Surrogate key, referenced by `clp_long_span_archives` |
| `id VARCHAR(64)` (unique) | `uuid BINARY(16)` | Indexed with `dataset_id`, not unique: a partitioned table's unique keys must include the partitioning column |
| — | `dataset_id` | Replaces the dataset in the table name |
| `begin_timestamp`, `end_timestamp` | `begin_timestamp_millisecs`, `end_timestamp_millisecs` | Archives without timestamps keep storing `0` for both |
| `uncompressed_size`, `size` | `num_uncompressed_bytes`, `num_compressed_bytes` | |
| `creator_id`, `creation_ix` | — | Never set by clp-s |
| — | `creation_timestamp_millisecs` | DB clock when the commit inserts the row; drives retention |
| — | `compression_job_id` | The job that produced the archive |
| — | `pack_id` | Reserved for archive packing; always `NULL` for now |
| — | `is_deleted` | Deletion pending (see "Deletion") |

New archive IDs are UUIDv7; existing UUIDv4 IDs stay valid. Both are 16 bytes. The UUIDv7 timestamp
comes from the ingestion host and doesn't affect retention.

Diff from the design doc:

* Renames `timestamp_range_begin_millis`, `timestamp_range_end_millis` and `creation_time_millis`
  to `begin_timestamp_millisecs`, `end_timestamp_millisecs` and `creation_timestamp_millisecs`; keys
  keep the same structure.
* Adds `compression_job_id` (agreed in the design doc's comments).
* Sets the creation time from the DB clock at insertion rather than the archive's seal time (agreed
  in the design doc's comments).
* Creates only `p_future`. `p_floor`, per-window partitions, their maintenance, and the
  `MAX_SPAN`-based lower-bound pruning come later.

### `clp_long_span_archives`

```sql
CREATE TABLE IF NOT EXISTS `clp_long_span_archives` (
    `dataset_id`                SMALLINT UNSIGNED NOT NULL,
    `begin_timestamp_millisecs` BIGINT NOT NULL,
    `end_timestamp_millisecs`   BIGINT NOT NULL,
    `archive_id`                BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (`dataset_id`, `end_timestamp_millisecs`, `archive_id`),
    KEY `by_begin` (`dataset_id`, `begin_timestamp_millisecs`)
) ENGINE=InnoDB;
```

A second reference to each archive whose timestamp range spans more than 24 hours (`MAX_SPAN`).
Written at insertion and removed with the archive, but not read until partition maintenance exists;
until then, queries find every overlapping archive in `clp_archives` directly.

Diff from the design doc: the same timestamp renames; `MAX_SPAN` fixed at 24 hours; reading is
deferred.

### `clp_column_metadata`

```sql
CREATE TABLE IF NOT EXISTS `clp_column_metadata` (
    `dataset_id` SMALLINT UNSIGNED NOT NULL,
    `name`       VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    `type`       TINYINT NOT NULL,
    PRIMARY KEY (`dataset_id`, `name`, `type`)
);
```

`type` is a `clp_s::NodeType`. Diff from the design doc: adds the `utf8mb4_bin` collation.

### `clp_<dataset>_files`

No longer created for clp-s. clp-s never wrote rows to it; a shared files table comes later.

## Deletion

`is_deleted` on datasets and archives is the durable record of a pending deletion. Every deletion,
whether manual (API) or by retention, has the same three steps:

1. **Mark.** One transaction sets `is_deleted = TRUE`. From then on, normal reads skip the row, a
   commit into the dataset fails, and its name can't be recreated.
2. **Clean up storage.** Delete the objects at the location derived from the dataset's
   `archive_storage_path`: the whole directory or key prefix (with a trailing `/`) for a dataset,
   `<archive_storage_path>/<uuid>` for an archive. Objects that are already gone aren't errors, so
   this step can be repeated.
3. **Reclaim.** One transaction hard-deletes the rows, only where `is_deleted = TRUE`:
   * an archive: its `clp_long_span_archives` rows, then its `clp_archives` row;
   * a dataset: its `clp_long_span_archives`, `clp_archives` and `clp_column_metadata` rows, then
     its `clp_datasets` row.

Ownership:

* The API and the retention check only mark. The `archive-garbage-collector` service runs steps 2
  and 3 for every marked row on each sweep, so a crash or failure anywhere leaves the row marked and
  the next sweep retries it. No storage is deleted for unmarked rows, and there is no local recovery
  file.
* Archives in a marked dataset are cleaned up with the dataset.

Coordination with writers:

* The commit transaction reads the job's dataset row with `FOR SHARE` and fails unless it exists
  and isn't marked. Marking takes an exclusive lock on the same row, so a commit and a dataset
  deletion are serialized: once the mark commits, no archive can be inserted into the dataset.
* Compression and commit carry the dataset ID resolved when the job started, not its name, so a
  later dataset with the same name is never written to by an old job.

Retention:

* Each sweep computes an expiry base: the DB's current time, or the creation time of the oldest
  non-terminal query job (`PENDING`, `RUNNING`, `CANCEL_REQUESTED`, `CANCELLING`) if that is
  earlier. It marks live archives of live datasets with a retention period whose
  `creation_timestamp_millisecs` is before `expiry base - retention`.
* The query coordinator only selects archives with
  `creation_timestamp_millisecs >= job creation - retention`, so no non-terminal query can be
  reading an archive that retention marks.

Known limitations, unchanged from `main` and left for later:

* A manual deletion doesn't wait for queries that already selected the archives; those queries may
  fail to read them.
* Archives that an in-flight compression writes to a dataset after it's marked are never committed,
  so nothing references them. Like other ingestion-failure leftovers, they aren't reclaimed.
* An indexer that finishes after its dataset is reclaimed can leave `clp_column_metadata` rows for
  a dataset ID that no longer exists. IDs aren't reused, so nothing reads them.
* Compression writes to the location derived from the current package config, while deletion uses
  `archive_storage_path` recorded at creation. They differ only if the config changes.
* A recreated dataset name maps to the same storage location, which may still hold the unreferenced
  objects above.

## The library

### Layout

```text
clp-metadata-db/src/
├── lib.rs              MetadataTables, re-exports
├── error.rs            Error
├── types.rs            DatasetId, ArchiveRowId, TimestampRange, ColumnType, row structs
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

| Type | Definition | Notes |
|---|---|---|
| `DatasetId` | `struct DatasetId(u16)`, `#[sqlx(transparent)]` | `clp_datasets.id` |
| `ArchiveRowId` | `struct ArchiveRowId(u64)`, `#[sqlx(transparent)]` | `clp_archives.id`; distinct from the UUID |
| `ArchiveId` | Existing UUID type in `clp-rust-utils`, plus `from_bytes` / `as_bytes` | Stored as `BINARY(16)` |
| `TimestampRange` | `{ begin_timestamp_millisecs: i64, end_timestamp_millisecs: i64 }` | Inclusive bounds |
| `ColumnType` | `#[repr(u8)]` enum mirroring `clp_s::NodeType` (`components/core/src/clp_s/SchemaTree.hpp`) | Replaces the magic numbers in `webui_client/metadata.rs` |
| `DatasetName`, `CompressionJobId`, `QueryJobStatus` | Existing types in `clp-rust-utils` | |

### Errors

```rust
pub enum Error {
    /// A dataset with the same name already exists, possibly pending deletion.
    DatasetAlreadyExists(DatasetName),
    /// Forwarded `sqlx` errors, including row decode errors (e.g. a UUID that isn't 16 bytes).
    Sqlx(sqlx::Error),
}
```

### Rules every operation follows

* Archive and column-metadata statements are scoped by `dataset_id`, unless documented as
  cross-dataset.
* Normal reads skip marked datasets and marked archives. Only the GC's cleanup reads opt into
  marked rows.
* Hard deletes only remove marked rows.
* `IN` lists and multi-row inserts are batched at 1000.

## Operations, by consumer

"Tx" marks an operation the caller is expected to run in a transaction together with other
statements.

### Table creation (`db-table-creator`, replacing `initialize-clp-metadata-db.py` for clp-s)

```rust
impl MetadataTables {
    /// Creates the shared tables if they don't exist. Not a migration: an existing table with
    /// another shape is left as is.
    pub async fn create_tables(&self, conn: &mut MySqlConnection) -> Result<(), Error>;
}
```

### Dataset API (`GET`/`POST /datasets`, `DELETE /datasets/{name}`, replacing `dataset_manager.py`)

```rust
impl MetadataTables {
    /// Errors with `DatasetAlreadyExists` on a name conflict, including with a dataset pending
    /// deletion.
    pub async fn create_dataset(
        &self,
        conn: &mut MySqlConnection,
        dataset: &NewDataset,
    ) -> Result<DatasetId, Error>;

    /// Live datasets, ordered by name.
    pub async fn get_dataset_names(&self, conn: &mut MySqlConnection)
        -> Result<Vec<DatasetName>, Error>;

    /// Live dataset only.
    pub async fn get_dataset_id_by_name(
        &self,
        conn: &mut MySqlConnection,
        name: &DatasetName,
    ) -> Result<Option<DatasetId>, Error>;

    /// Marks a live dataset as deleted. Returns whether a live dataset was marked.
    pub async fn mark_dataset_deleted(&self, conn: &mut MySqlConnection, id: DatasetId)
        -> Result<bool, Error>;
}

pub struct NewDataset {
    pub name: DatasetName,
    pub archive_storage_path: String,
    pub retention_period_minutes: Option<NonZeroU32>,
}
```

### Archive API (`GET /datasets/{name}/archives`, `POST /datasets/{name}/archives/delete`, replacing `archive_manager.py`)

Deletion selects archives by UUID or by time range, then marks them in the same transaction. There
is no dry run.

```rust
impl MetadataTables {
    /// Live archives whose timestamp range lies within `range`, as `archive_manager.py` selected
    /// them.
    pub async fn find_archives_by_time_range(
        &self,
        conn: &mut MySqlConnection,
        dataset_id: DatasetId,
        range: &TimestampRange,
    ) -> Result<Vec<Archive>, Error>;

    /// Live archives among `uuids`; UUIDs that don't match are absent.
    pub async fn find_archives_by_uuids(
        &self,
        conn: &mut MySqlConnection,
        dataset_id: DatasetId,
        uuids: &[ArchiveId],
    ) -> Result<Vec<Archive>, Error>;

    /// Returns the number of archives marked. Tx: yes, with the lookup.
    pub async fn mark_archives_deleted(
        &self,
        conn: &mut MySqlConnection,
        dataset_id: DatasetId,
        ids: &[ArchiveRowId],
    ) -> Result<u64, Error>;
}

pub struct Archive {
    pub id: ArchiveRowId,
    pub uuid: ArchiveId,
    pub begin_timestamp_millisecs: i64,
    pub end_timestamp_millisecs: i64,
}
```

### Compression coordinator (replacing `upsert_metadata_tables`)

Uses `get_dataset_id_by_name` when it starts a job; the job fails if the dataset is missing or
marked. The resolved `DatasetId` is passed to the compression and commit tasks.

### Spider commit task (replacing `register_dataset` and the per-dataset archive insert)

```rust
impl MetadataTables {
    /// Locks the dataset row (`FOR SHARE`) and returns whether it exists and isn't marked.
    /// Tx: yes, before `insert_archives`.
    pub async fn lock_live_dataset(&self, conn: &mut MySqlConnection, id: DatasetId)
        -> Result<bool, Error>;

    /// Inserts the archives with `creation_timestamp_millisecs` from the DB clock, then a
    /// `clp_long_span_archives` row for each archive spanning more than 24 hours.
    /// Tx: yes, with `lock_live_dataset` and the job's status update.
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

### Query coordinator (replacing the per-dataset archive selection)

```rust
impl MetadataTables {
    /// Live datasets only; names that don't resolve are absent.
    pub async fn get_dataset_ids_by_names(
        &self,
        conn: &mut MySqlConnection,
        names: &[DatasetName],
    ) -> Result<HashMap<DatasetName, DatasetId>, Error>;

    /// Live archives of a live dataset that overlap the filter's time range (archives without
    /// timestamps always match), within the dataset's retention relative to the job's creation.
    pub async fn find_archives_for_query(
        &self,
        conn: &mut MySqlConnection,
        dataset_id: DatasetId,
        filter: &QueryArchiveFilter,
    ) -> Result<Vec<QueryArchive>, Error>;
}

pub struct QueryArchiveFilter {
    /// Inclusive bounds; `None` is unbounded on that side.
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

The global `archive_retention_period_millisecs` goes away; retention comes from each dataset.

### `archive-garbage-collector` (replacing the clp-s part of `archive_garbage_collector.py`)

A sweep runs, in order: retention marking, dataset cleanup, archive cleanup.

```rust
impl MetadataTables {
    /// Marks live archives of live datasets that have a retention period and were created before
    /// `min(DB now, earliest_query_job_creation_timestamp_millisecs) - retention`. Cross-dataset.
    /// Returns the number of archives marked.
    pub async fn mark_expired_archives_deleted(
        &self,
        conn: &mut MySqlConnection,
        earliest_query_job_creation_timestamp_millisecs: Option<i64>,
    ) -> Result<u64, Error>;

    /// Marked datasets.
    pub async fn get_marked_datasets(&self, conn: &mut MySqlConnection)
        -> Result<Vec<MarkedDataset>, Error>;

    /// Removes a marked dataset's rows from every shared table. Returns whether the dataset row was
    /// removed. Tx: yes.
    pub async fn delete_marked_dataset(&self, conn: &mut MySqlConnection, id: DatasetId)
        -> Result<bool, Error>;

    /// Up to `limit` marked archives of datasets that aren't marked. Cross-dataset.
    pub async fn get_marked_archives(&self, conn: &mut MySqlConnection, limit: usize)
        -> Result<Vec<MarkedArchive>, Error>;

    /// Removes marked archives and their long-span rows. Returns the number of archives removed.
    /// Tx: yes.
    pub async fn delete_marked_archives(
        &self,
        conn: &mut MySqlConnection,
        dataset_id: DatasetId,
        ids: &[ArchiveRowId],
    ) -> Result<u64, Error>;
}

pub struct MarkedDataset {
    pub id: DatasetId,
    pub archive_storage_path: String,
}

pub struct MarkedArchive {
    pub id: ArchiveRowId,
    pub uuid: ArchiveId,
    pub dataset_id: DatasetId,
    pub archive_storage_path: String,
}
```

The query-job tables aren't metadata tables, so this helper stays in
`clp_rust_utils::database::mysql::query_job` but takes a connection like the rest:

```rust
/// Earliest `creation_time` among jobs with one of `statuses`, in milliseconds.
pub async fn get_earliest_creation_timestamp_millisecs(
    conn: &mut MySqlConnection,
    statuses: &[QueryJobStatus],
) -> Result<Option<i64>, crate::Error>;
```

### api-server WebUI metadata routes (`webui_client/metadata.rs`, clp-s branches)

The clp-text branches stay as they are.

| Route | New interface |
|---|---|
| `/metadata/datasets` | `get_dataset_names` |
| `/metadata/time_range` | `get_timestamp_bounds` |
| `/metadata/space_savings` | `get_space_savings` |
| `/metadata/ingestion_details` | `get_timestamp_bounds` over every live dataset; file and message counts are `0`, as they are today because clp-s never fills the files tables |
| `/metadata/query_speed` | `get_uncompressed_bytes_by_uuids` |
| `/metadata/column_metadata/{dataset_name}/timestamp` | `get_column_names_by_types` |

```rust
impl MetadataTables {
    /// Over live archives of the given datasets. `None` if there are none.
    pub async fn get_timestamp_bounds(&self, conn: &mut MySqlConnection, ids: &[DatasetId])
        -> Result<Option<TimestampBounds>, Error>;

    /// Over live archives of live datasets.
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
route reads the job's archive IDs from the job tables, then calls `get_uncompressed_bytes_by_uuids`.

### Python CLI dataset checks

`validate_datasets_exist` reads the datasets table directly. For clp-s, no Python caller needs a
replacement:

| Script | Replacement |
|---|---|
| `search.sh` | The Rust search CLI, which submits through the api-server. The query coordinator already fails jobs whose datasets don't exist (`ensure_all_required_datasets_exist`), so the CLI doesn't check the DB itself |
| `decompress.sh extract-json` | — (stream extraction is out of scope; only the Python query scheduler handles `EXTRACT_JSON` jobs, and the Spider deployment doesn't run it) |
| `admin-tools/archive-manager.sh` | Removed at the switch; replaced by the archive API |

## The indexer

`components/core/src/clp_s/indexer` is a separate C++ executable that the compression task runs on
each archive. It opens the archive, walks the schema tree, and `INSERT IGNORE`s `(name, type)` into
the dataset's column-metadata table.

Decision (Devin): keep the C++ indexer until clp-s is rewritten in Rust. At the switch:

* It takes the dataset ID from the compression task instead of a dataset name, and writes
  `(dataset_id, name, type)` into `clp_column_metadata`.
* `--create-table` and the per-dataset table name go away.

When the Rust rewrite replaces the indexer, it must reproduce the key escaping (`escape_key_name`)
and the skipped node types (`StructuredArray`, `Object`, `Unknown`) exactly, and write through a new
`insert_column_metadata`.

## Open questions

1. **Existing deployments.** No data moves from the per-dataset tables. The new `clp_datasets` has
   the same name as the current one but a different shape, and `create_tables` leaves an existing
   table alone. Proposal: clp-s requires a fresh metadata database after the switch, and
   `db-table-creator` fails with a clear error if it finds the old `clp_datasets` shape.
2. **Search CLI ordering.** If the Rust search CLI lands before the switch, the switch doesn't touch
   the Python search script. Otherwise, the switch removes its clp-s `validate_datasets_exist` call
   and relies on the query coordinator's check, so no Python reads the shared tables either way.
3. **Manual deletion latency.** The API only marks, so storage is reclaimed on the next GC sweep
   (60 minutes by default). Is that acceptable, or should the API wake the GC?
4. **Dataset ID width.** `SMALLINT UNSIGNED` allows 65,535 datasets over the lifetime of a database:
   the auto-increment counter persists, and reclaiming rows doesn't reset it.
5. **Default dataset.** The CLI scripts, WebUI compression page, and MCP server default to the
   `default` dataset, which must now be created first. Proposal: their errors say so, and creation
   stays an API call.

[design-doc]: https://app.notion.com/p/yscope/Metadata-database-design-and-schema-3c804e4d9e6b809ebe63cb3926c0e0ff
