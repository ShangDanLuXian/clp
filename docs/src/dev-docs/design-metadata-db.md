# Metadata database

:::{warning}
🚧 This section is still under construction.
:::

## clp-s tables

The `clp-s` storage engine stores the metadata of every dataset in the shared tables listed in
[Table 1](#table-1). Each table name starts with the configured table prefix (`clp_` by default).

(table-1)=
:::{card}

| Table name               | Description                                                                        |
|--------------------------|------------------------------------------------------------------------------------|
| `clp_datasets`           | Each dataset stored in CLP, with its settings.                                     |
| `clp_archives`           | Each archive, with the dataset it belongs to and its timestamp range.              |
| `clp_long_span_archives` | A second reference to each archive whose timestamp range spans more than 24 hours. |
| `clp_column_metadata`    | The columns (name and type) of each dataset.                                       |

+++
**Table 1**: CLP's shared metadata tables for `clp-s`.
:::

:::{note}
The indexer still writes each dataset's columns to a per-dataset table,
`clp_<dataset>_column_metadata`, instead of `clp_column_metadata`.
:::

### Datasets table

CLP's datasets table describes each dataset stored in CLP and can be used by external tools (e.g.,
a CLP Presto connector) to locate archives that belong to a dataset. [Table 2](#table-2) describes
the high-level schema of the datasets table. Each string column has a length that should
accommodate all expected values.

(table-2)=
:::{card}

| Column name              | Type              | Description                                                                         |
|--------------------------|-------------------|-------------------------------------------------------------------------------------|
| id                       | SMALLINT UNSIGNED | The dataset's ID, which the other tables use to refer to the dataset.               |
| name                     | VARCHAR(255)      | The *unique* name of the dataset.                                                   |
| archive_storage_path     | VARCHAR(4096)     | The directory (relative to the storage type, e.g., `s3`) where archives are stored. |
| is_deleted               | BOOLEAN           | Whether the dataset has been deleted.                                               |
| retention_period_minutes | INT UNSIGNED      | The dataset's retention period. Currently always `NULL`.                            |

+++
**Table 2**: The high-level schema of CLP's datasets table.
:::

:::{note}
CLP typically runs in containers, so when storing archives on a local filesystem, the path it uses
for archives is the path inside the container. However, for external tools to locate these archives,
`archive_storage_path` must be the path on the host filesystem.
:::

### Archives table

[Table 3](#table-3) describes the high-level schema of the archives table.

(table-3)=
:::{card}

| Column name                  | Type              | Description                                                   |
|------------------------------|-------------------|---------------------------------------------------------------|
| id                           | BIGINT UNSIGNED   | The archive's ID.                                             |
| dataset_id                   | SMALLINT UNSIGNED | The ID of the dataset that the archive belongs to.            |
| uuid                         | BINARY(16)        | The archive's UUID, which names the archive in storage.       |
| begin_timestamp_millisecs    | BIGINT            | The archive's earliest timestamp, in milliseconds.            |
| end_timestamp_millisecs      | BIGINT            | The archive's latest timestamp, in milliseconds.              |
| num_uncompressed_bytes       | BIGINT UNSIGNED   | The size of the archive's logs before compression, in bytes.  |
| num_compressed_bytes         | BIGINT UNSIGNED   | The size of the archive, in bytes.                            |
| creation_timestamp_millisecs | BIGINT            | When the archive was committed to the table, in milliseconds. |
| compression_job_id           | INT               | The ID of the compression job that created the archive.       |
| pack_id                      | BIGINT UNSIGNED   | Reserved for archive packing. Currently always `NULL`.        |

+++
**Table 3**: The high-level schema of CLP's archives table.
:::
