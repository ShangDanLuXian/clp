# clp-s shared metadata tables: interface plan

## Goal

Every database operation on the clp-s metadata tables goes through one Rust library,
`clp-metadata-db` (`components/clp-rust-libs/clp-metadata-db`). Each operation is a typed Rust
function implemented with `sqlx` underneath. Every function takes an arbitrary
`&mut sqlx::MySqlConnection`, so a caller can run any combination of operations inside a transaction
of its own (`&mut *tx`) or on a plain pooled connection. The library never creates pools or
transactions.

This plan covers the full interface needed by the end state, i.e., both the operations the pre-switch
components use (API management routes, archive GC) and the operations the switch moves onto the
shared tables (compression, commit, query planning, WebUI metadata).

One exception: the C++ indexer keeps writing column metadata itself until clp-s is rewritten in
Rust (see "The indexer").

Out of scope: clp-text (`clp` storage engine) tables, the query/compression job tables, files tables,
top-level-filtering tables, and the WebUI ingestion-details route (its file and message counts need a
files table, which clp-s doesn't populate).

## Tables

| Table | Key | Notes |
|---|---|---|
| `clp_datasets` | `id SMALLINT UNSIGNED` | `name`, `archive_storage_path`, `retention_period_minutes` (NULL = keep forever), `deleted_at_millis` (0 = live); unique `(name, deleted_at_millis)` |
| `clp_archives` | `(dataset_id, timestamp_range_begin_millis, id)` | `id BIGINT UNSIGNED` surrogate, `uuid BINARY(16)`, timestamp range, byte counts, `creation_time_millis` (DB clock), `compression_job_id`, `pack_id`; range-partitioned by `timestamp_range_begin_millis` (single `p_future` for now) |
| `clp_long_span_archives` | `(dataset_id, timestamp_range_end_millis, archive_id)` | Second reference to archives spanning more than 24 hours; unpartitioned |
| `clp_column_metadata` | `(dataset_id, name, type)` | Union of the columns of a dataset's archives |

All string columns are `CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`, so dataset and column names are
case-sensitive (`Logs` and `logs` are different datasets). Known limitation: `utf8mb4_bin` pads with
spaces when comparing, so column names that differ only in trailing spaces (`"a"` vs `"a "`) still
collide. Dataset names can't contain spaces.

## Crate layout

```text
clp-metadata-db/src/
├── lib.rs              MetadataTables, re-exports
├── error.rs            Error
├── types.rs            DatasetId, ArchiveRowId, TimestampRange, ColumnType (see "Types")
├── schema.rs           create_tables
├── dataset.rs          dataset operations
├── archive.rs          archive operations
├── column_metadata.rs  column metadata operations
└── statistics.rs       aggregate reads for the WebUI metadata routes
```

## Entry point

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

Table names come from the package config's `Database` (`datasets_table_name`,
`unscoped_archives_table_name`, plus two new helpers `long_span_archives_table_name` and
`unscoped_column_metadata_table_name`).

## Types

| Type | Definition | Why |
|---|---|---|
| `DatasetId` | newtype over `u16` (`#[sqlx(transparent)]`), replacing the current `u16` alias in `clp-rust-utils` | Keeps dataset IDs from mixing with other integers |
| `ArchiveRowId` | newtype over `u64` | The archives table's surrogate key; distinct from the archive's UUID (`ArchiveId`) |
| `ArchiveId` | existing UUID type in `clp-rust-utils`, plus `from_bytes` / `as_bytes` | Stored as `BINARY(16)` in the shared table; the legacy text SQL encoding stays for other tables |
| `TimestampRange` | `{ begin_millis: i64, end_millis: Option<i64> }` | Replaces the bare `begin_ts` / `end_ts` pairs |
| `ColumnType` | `#[repr(u8)]` enum mirroring `clp_s::NodeType` (`components/core/src/clp_s/SchemaTree.hpp`) | Typed `type` column; replaces the magic numbers in `webui_client/metadata.rs` |
| `CompressionJobId` | existing `i32` alias in `clp-rust-utils` | Written to `compression_job_id` |

`NewDataset`, `Archive`, `ExpiredArchive`, `QueryArchive`, `NewArchive`, `TimestampBounds` and
`SpaceSavings` are plain structs defined next to the operations that use them.

## Errors

```rust
pub enum Error {
    /// A live dataset with the same name already exists.
    DatasetAlreadyExists(DatasetName),
    /// Forwarded `sqlx` errors, including row decode errors (e.g. a UUID that isn't 16 bytes).
    Sqlx(sqlx::Error),
}
```

Callers map the error to their own domain errors, e.g. `DatasetAlreadyExists` → HTTP 409.

## Operations

"Tx" says whether the caller is expected to wrap the call in a transaction with other operations.
Every operation scopes its rows by `dataset_id` unless it is explicitly cross-dataset.

### Schema (`schema.rs`)

| Function | Returns | Behavior | Callers | Tx |
|---|---|---|---|---|
| `create_tables(conn)` | `()` | `CREATE TABLE IF NOT EXISTS` for the four tables. Not a migration: an existing table with another shape is left as is | `db-table-creator` | No |

### Datasets (`dataset.rs`)

```rust
pub struct NewDataset {
    pub name: DatasetName,
    pub archive_storage_path: String,
    pub retention_period_minutes: Option<NonZeroU32>,
}
```

| Function | Returns | Behavior | Callers | Tx |
|---|---|---|---|---|
| `create_dataset(conn, &NewDataset)` | `DatasetId` | Inserts a live dataset; a unique-key violation becomes `Error::DatasetAlreadyExists` | API `POST /datasets` | No |
| `get_dataset_names(conn)` | `Vec<DatasetName>` | Live datasets, ordered by name | API `GET /datasets`, WebUI dataset list | No |
| `get_dataset_id_by_name(conn, &DatasetName)` | `Option<DatasetId>` | Live dataset only | API routes, compression coordinator, commit task | Optional |
| `get_dataset_ids_by_names(conn, &[DatasetName])` | `HashMap<DatasetName, DatasetId>` | Live datasets only; names that don't resolve are absent. Batches of 1000 | Query coordinator, WebUI metadata routes | No |
| `mark_dataset_deleted(conn, DatasetId)` | `bool` (whether a live row was marked) | Sets `deleted_at_millis` to the DB clock's current time if the dataset is live | API `DELETE /datasets/{name}` | No |
| `get_deleted_dataset_ids(conn)` | `Vec<DatasetId>` | `deleted_at_millis <> 0` | Archive GC | No |
| `delete_dataset_metadata(conn, DatasetId)` | `u64` (rows deleted) | Deletes the dataset's rows from the long-span archives, archives and column-metadata tables; keeps the datasets row | Archive GC | Yes |

### Archives (`archive.rs`)

```rust
pub struct NewArchive {
    pub uuid: ArchiveId,
    pub begin_timestamp_millis: i64,
    pub end_timestamp_millis: i64,
    pub num_uncompressed_bytes: u64,
    pub num_compressed_bytes: u64,
}

pub struct Archive {
    pub id: ArchiveRowId,
    pub uuid: ArchiveId,
}

pub struct ExpiredArchive {
    pub archive: Archive,
    pub dataset_id: DatasetId,
    pub dataset_name: DatasetName,
}

pub struct QueryArchive {
    pub uuid: ArchiveId,
    pub num_uncompressed_bytes: u64,
    pub end_timestamp_millis: i64,
}
```

| Function | Returns | Behavior | Callers | Tx |
|---|---|---|---|---|
| `insert_archives(conn, DatasetId, CompressionJobId, &[NewArchive])` | `()` | Inserts the archives with `creation_time_millis` from the DB clock, then a long-span row for each archive whose span exceeds 24 hours. Batches of 1000 | Commit task (switch) | Yes, with the job's status update |
| `find_archives_by_time_range(conn, DatasetId, TimestampRange)` | `Vec<Archive>` | Archives whose timestamp range lies within the range (begin ≥ `begin_millis`, end ≤ `end_millis` if given) | API archive list/delete | Yes, for deletion |
| `find_archives_by_uuids(conn, DatasetId, &[ArchiveId])` | `Vec<Archive>` | Batches of 1000 | API archive delete | Yes |
| `delete_archives(conn, DatasetId, &[ArchiveRowId])` | `()` | Deletes long-span rows first, then archive rows. Batches of 1000 | API archive delete, archive GC | Yes |
| `find_expired_archives(conn, expiry_base_millis: i64)` | `Vec<ExpiredArchive>` | Archives of live datasets with a retention period, created before `expiry_base_millis − retention`. Cross-dataset | Archive GC | No |
| `find_archives_for_query(conn, DatasetId, &QueryArchiveFilter)` | `Vec<QueryArchive>` | Archives overlapping the query's time range, excluding ones outside the dataset's retention relative to the job's creation time (`creation_time_millis ≥ job_creation_millis − retention`); dataset must be live | Query coordinator (switch) | No |

```rust
pub struct QueryArchiveFilter {
    /// Inclusive bounds of the query's time range; `None` means unbounded on that side.
    pub begin_timestamp_millis: Option<i64>,
    pub end_timestamp_millis: Option<i64>,
    pub job_creation_time_millis: i64,
}
```

### Column metadata (`column_metadata.rs`)

| Function | Returns | Behavior | Callers | Tx |
|---|---|---|---|---|
| `get_column_names_by_types(conn, DatasetId, &[ColumnType])` | `Vec<String>` | Distinct names ordered by name | WebUI timestamp-column route (switch) | No |

The library only reads this table; the C++ indexer writes it (see "The indexer"). An
`insert_column_metadata` operation is added when the Rust clp-s rewrite replaces the indexer.

### Statistics (`statistics.rs`)

Aggregate reads that back the api-server's WebUI `/metadata` routes. They replace the per-dataset
`UNION ALL` queries in `components/api-server/src/webui_client/metadata.rs`.

| Function | Returns | Behavior |
|---|---|---|
| `get_timestamp_bounds(conn, &[DatasetId])` | `Option<TimestampBounds>` | `MIN(begin)` / `MAX(end)` over the given datasets |
| `get_space_savings(conn)` | `SpaceSavings` | Total uncompressed / compressed bytes over live datasets |
| `get_uncompressed_bytes_by_uuids(conn, &[DatasetId], &[ArchiveId])` | `u64` | Sum used for the query-speed route, which then combines it with the query job's duration |

The query-speed route currently joins `query_jobs` / `query_tasks` with the archives table in one
statement. With the shared table, `query_tasks.archive_id` (text UUID) no longer matches
`clp_archives.uuid` (bytes), so the route reads the job's archive IDs from the job tables and passes
them here instead of joining across both.

### Query-job helper (not in this crate)

The archive GC also needs the earliest creation time of the non-terminal query jobs. The query-job
tables aren't metadata tables, so the helper stays in `clp_rust_utils::database::mysql::query_job`,
but it takes a connection like the rest of this interface:

```rust
pub async fn get_earliest_creation_time_millis(
    conn: &mut sqlx::MySqlConnection,
    statuses: &[QueryJobStatus],
) -> Result<Option<i64>, crate::Error>;
```

## The indexer

`components/core/src/clp_s/indexer` is a separate C++ executable that the compress task runs on each
archive. It's the only C++ code that writes to the database: it opens the archive, walks the schema
tree, and `INSERT IGNORE`s `(path, NodeType)` into the dataset's column-metadata table.

Decision (Devin): keep the C++ indexer until clp-s is rewritten in Rust. At the switch, its SQL is
ported to the shared table:

```sql
INSERT IGNORE INTO clp_column_metadata (name, type, dataset_id)
SELECT ?, ?, id FROM clp_datasets WHERE name = ? AND deleted_at_millis = 0
```

`--create-table` and the per-dataset table name go away. When the Rust rewrite replaces the indexer,
it must reproduce the key escaping (`escape_key_name`) and the skipped node types
(`StructuredArray`, `Object`, `Unknown`) exactly, and write through `insert_column_metadata`.

## Open questions

1. `DatasetId` is `SMALLINT UNSIGNED` and deleted rows keep their IDs, so at most 65 535 datasets can
   ever be created. Enough?
2. `archive_storage_path` is written at dataset creation but never read: every component (compression,
   search, the API's deletions, the archive GC) derives archive locations from the current package
   config. If the config's storage location changes, existing archives stay at the old location,
   which only this column records. Options: keep it write-only, drop it, or make it the source of
   archive locations.
