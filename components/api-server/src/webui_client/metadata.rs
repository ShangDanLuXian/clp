//! Read-only metadata about the CLP package's datasets, archives, and jobs.

use std::collections::HashSet;

use chrono::DateTime;
use chrono::Utc;
use clp_metadata_db::ArchiveTimestampRange;
use clp_metadata_db::ColumnType;
use clp_metadata_db::DatasetLookup;
use clp_metadata_db::MetadataStatistics;
use clp_rust_utils::clp_config::package::config::StorageEngine;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::job_config::COMPRESSION_JOBS_TABLE_NAME;
use clp_rust_utils::job_config::CompressionJobId;
use clp_rust_utils::job_config::QUERY_JOBS_TABLE_NAME;
use clp_rust_utils::job_config::QUERY_TASKS_TABLE_NAME;
use clp_rust_utils::job_config::QueryJobId;
use clp_rust_utils::serde::ZstdMsgpack;
use clp_rust_utils::types::DatasetId;
use serde::Deserialize;
use serde::Serialize;
use utoipa::ToSchema;

use crate::error::ClientError;
use crate::webui_client::WebuiClient;

/// Earliest and latest log entry timestamps across the selected datasets.
#[derive(Debug, PartialEq, Eq, Serialize, ToSchema)]
pub struct TimeRange {
    /// Earliest log entry timestamp (epoch milliseconds).
    pub begin_timestamp: i64,
    /// Latest log entry timestamp (epoch milliseconds).
    pub end_timestamp: i64,
}

impl From<ArchiveTimestampRange> for TimeRange {
    fn from(range: ArchiveTimestampRange) -> Self {
        Self {
            begin_timestamp: range.begin_timestamp_millisecs(),
            end_timestamp: range.end_timestamp_millisecs(),
        }
    }
}

/// Aggregated space-savings statistics.
#[derive(Debug, PartialEq, Eq, Serialize, sqlx::FromRow, ToSchema)]
pub struct SpaceSavings {
    /// Total uncompressed size of all archives, in bytes.
    pub total_uncompressed_size: i64,
    /// Total compressed size of all archives, in bytes.
    pub total_compressed_size: i64,
}

impl TryFrom<clp_metadata_db::SpaceSavings> for SpaceSavings {
    type Error = ClientError;

    fn try_from(space_savings: clp_metadata_db::SpaceSavings) -> Result<Self, Self::Error> {
        let to_signed =
            |size: u64| i64::try_from(size).map_err(|_| ClientError::ArchiveSizeOutOfRange(size));
        Ok(Self {
            total_uncompressed_size: to_signed(space_savings.num_uncompressed_bytes)?,
            total_compressed_size: to_signed(space_savings.num_compressed_bytes)?,
        })
    }
}

/// Ingestion details statistics.
#[derive(Debug, PartialEq, Eq, Serialize, ToSchema)]
pub struct IngestionDetails {
    /// Earliest log entry timestamp (epoch milliseconds).
    pub begin_timestamp: i64,
    /// Latest log entry timestamp (epoch milliseconds).
    pub end_timestamp: i64,
    /// Number of distinct ingested files.
    pub num_files: i64,
    /// Total number of ingested messages.
    pub num_messages: i64,
}

/// Query-speed statistics for a query job.
#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct QuerySpeed {
    /// Total uncompressed size of the archives the job scanned, in bytes.
    pub bytes: f64,
    /// Wall-clock duration the job ran, in seconds.
    pub duration: f32,
}

/// The metadata of a compression job, with the display fields of its config.
#[derive(Debug, Serialize, ToSchema)]
pub struct CompressionMetadata {
    /// The compression job's ID. Named `_id` to match the `WebUI`'s existing JSON contract.
    #[serde(rename = "_id")]
    #[schema(value_type = i32)]
    pub id: CompressionJobId,
    /// Current status of the job, as the integer value of a
    /// [`clp_rust_utils::job_config::CompressionJobStatus`].
    pub status: i32,
    /// Status message for the job.
    pub status_msg: String,
    /// Time the job started executing (RFC 3339). `null` if the job hasn't started.
    pub start_time: Option<DateTime<Utc>>,
    /// Time the job was last updated (RFC 3339).
    pub update_time: DateTime<Utc>,
    /// Wall-clock duration the job ran, in seconds. `null` if the job did not complete.
    pub duration: Option<f32>,
    /// Total uncompressed size of input files, in bytes.
    pub uncompressed_size: i64,
    /// Total compressed archive size, in bytes.
    pub compressed_size: i64,
    /// The display fields of the job's stored config. All other fields, including storage
    /// credentials, are dropped.
    pub clp_config: CompressionMetadataDisplayConfig,
}

impl TryFrom<CompressionMetadataRowProjection> for CompressionMetadata {
    type Error = ClientError;

    fn try_from(row: CompressionMetadataRowProjection) -> Result<Self, Self::Error> {
        let clp_config = ZstdMsgpack::deserialize(&row.clp_config)?;
        Ok(Self {
            id: row.id,
            status: row.status,
            status_msg: row.status_msg,
            start_time: row.start_time,
            update_time: row.update_time,
            duration: row.duration,
            uncompressed_size: row.uncompressed_size,
            compressed_size: row.compressed_size,
            clp_config,
        })
    }
}

/// The display subset of a stored compression job config, as returned in
/// [`CompressionMetadata::clp_config`].
///
/// Decoding a stored config into this type keeps only the display fields: all other fields,
/// including storage credentials, are dropped.
#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct CompressionMetadataDisplayConfig {
    /// The display fields of the config's `input` section. Empty if the stored config has no
    /// `input` section.
    #[serde(default)]
    #[schema(required = true)]
    pub input: CompressionMetadataDisplayInput,
    /// The display fields of the config's `output` section. Empty if the stored config has no
    /// `output` section.
    #[serde(default)]
    #[schema(required = true)]
    pub output: CompressionMetadataDisplayOutput,
}

/// The display fields of a stored config's `input` section. Older compression jobs may have stored
/// only some of them, and a field stored as `null` is omitted.
#[derive(Debug, Default, Deserialize, Serialize, ToSchema)]
pub struct CompressionMetadataDisplayInput {
    /// The input type (e.g., `fs` or `s3`).
    // The `WebUI`'s `ClpIoPartialConfigSchema` accepts a missing key but rejects `null` for most
    // display fields, so every display field here and in `CompressionMetadataDisplayOutput` omits
    // `None` instead of serializing it as `null`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// The dataset the logs were compressed into.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset: Option<String>,
    /// The filesystem paths to compress.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paths_to_compress: Option<Vec<String>>,
    /// The prefix removed from the compressed file paths.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path_prefix_to_remove: Option<String>,
    /// The S3 object keys to compress.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<String>>,
    /// The key of the log events' timestamp field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_key: Option<String>,
    /// Whether the logs are unstructured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unstructured: Option<bool>,
}

/// The display fields of a stored config's `output` section. Older compression jobs may have stored
/// only some of them, and a field stored as `null` is omitted.
#[derive(Debug, Default, Deserialize, Serialize, ToSchema)]
pub struct CompressionMetadataDisplayOutput {
    /// The compression level.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_level: Option<u8>,
    /// The target archive size, in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_archive_size: Option<u64>,
    /// The target dictionaries size, in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_dictionaries_size: Option<u64>,
    /// The target encoded file size, in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_encoded_file_size: Option<u64>,
    /// The target segment size, in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_segment_size: Option<u64>,
}

impl WebuiClient {
    /// Fetches all dataset names from the datasets table.
    ///
    /// The CLP storage engine has no datasets table, so no dataset names are returned for it.
    ///
    /// # Returns
    ///
    /// The dataset names, ordered by name, on success, or an empty list for the CLP storage engine.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryScalar::fetch_all`]'s return values on failure.
    pub async fn get_dataset_names(&self) -> Result<Vec<DatasetName>, ClientError> {
        if self.config.package.storage_engine == StorageEngine::Clp {
            return Ok(Vec::new());
        }
        let table = self.config.database.datasets_table_name();
        let names = sqlx::query_scalar(&format!("SELECT name FROM `{table}` ORDER BY name"))
            .fetch_all(&self.sql_pool)
            .await?;
        Ok(names)
    }

    /// Fetches the earliest and latest log entry timestamps across the given datasets.
    ///
    /// For the CLP storage engine, `datasets` is ignored and the single archives table is queried.
    /// For CLP-S, the union of the per-dataset archives tables is queried.
    ///
    /// # Returns
    ///
    /// The earliest and latest log entry timestamps on success, or `None` when the datasets have no
    /// archives (including, for CLP-S, when `datasets` is empty).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::DatasetNotFound`] if, for CLP-S, any of `datasets` doesn't exist. The first
    ///   missing dataset in `datasets` is reported.
    /// * Forwards [`sqlx::query::QueryAs::fetch_one`]'s return values on any other failure.
    /// * Forwards [`sqlx::query::QueryScalar::fetch_all`]'s return values if looking up the missing
    ///   dataset fails.
    pub async fn get_time_range(
        &self,
        datasets: &[DatasetName],
    ) -> Result<Option<TimeRange>, ClientError> {
        let database = &self.config.database;
        let sql = match self.config.package.storage_engine {
            StorageEngine::Clp => format!(
                "SELECT MIN(begin_timestamp) AS begin_timestamp, MAX(end_timestamp) AS \
                 end_timestamp FROM `{}`",
                database.unscoped_archives_table_name()
            ),
            StorageEngine::ClpS => {
                if datasets.is_empty() {
                    return Ok(None);
                }
                let union = Self::union_all(datasets.iter().map(|dataset| {
                    format!(
                        "SELECT MIN(begin_timestamp) AS begin_timestamp, MAX(end_timestamp) AS \
                         end_timestamp FROM `{}`",
                        database.archives_table_name(dataset)
                    )
                }));
                format!(
                    "SELECT MIN(begin_timestamp) AS begin_timestamp, MAX(end_timestamp) AS \
                     end_timestamp FROM ({union}) AS combined"
                )
            }
        };
        let row = match sqlx::query_as::<_, TimeRangeRowProjection>(&sql)
            .fetch_one(&self.sql_pool)
            .await
        {
            Ok(row) => row,
            Err(err) => return Err(self.resolve_dataset_query_error(datasets, err).await),
        };
        Ok(row.into())
    }

    /// Fetches aggregated space-savings statistics across all datasets.
    ///
    /// # Returns
    ///
    /// The total uncompressed and compressed sizes on success; both are `0` when no data has been
    /// ingested yet.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::get_dataset_names`]'s return values on failure.
    /// * Forwards [`sqlx::query::QueryAs::fetch_one`]'s return values on failure, including when
    ///   the tables of a dataset that's being deleted no longer exist.
    pub async fn get_space_savings(&self) -> Result<SpaceSavings, ClientError> {
        let database = &self.config.database;
        let sql = match self.config.package.storage_engine {
            StorageEngine::Clp => format!(
                "SELECT CAST(COALESCE(SUM(uncompressed_size), 0) AS SIGNED) AS \
                 total_uncompressed_size, CAST(COALESCE(SUM(size), 0) AS SIGNED) AS \
                 total_compressed_size FROM `{}`",
                database.unscoped_archives_table_name()
            ),
            StorageEngine::ClpS => {
                let datasets = self.get_dataset_names().await?;
                if datasets.is_empty() {
                    return Ok(SpaceSavings {
                        total_uncompressed_size: 0,
                        total_compressed_size: 0,
                    });
                }
                let union = Self::union_all(datasets.iter().map(|dataset| {
                    format!(
                        "SELECT SUM(uncompressed_size) AS uncompressed_size, SUM(size) AS size \
                         FROM `{}`",
                        database.archives_table_name(dataset)
                    )
                }));
                format!(
                    "SELECT CAST(COALESCE(SUM(uncompressed_size), 0) AS SIGNED) AS \
                     total_uncompressed_size, CAST(COALESCE(SUM(size), 0) AS SIGNED) AS \
                     total_compressed_size FROM ({union}) AS archives_combined"
                )
            }
        };
        let space_savings = sqlx::query_as::<_, SpaceSavings>(&sql)
            .fetch_one(&self.sql_pool)
            .await?;
        Ok(space_savings)
    }

    /// Fetches ingestion details (timestamp range, file count, message count) across all datasets.
    ///
    /// # Returns
    ///
    /// The ingestion details on success, or `None` when no data has been ingested yet.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::get_dataset_names`]'s return values on failure.
    /// * Forwards [`sqlx::query::QueryAs::fetch_one`]'s return values on failure, including when
    ///   the tables of a dataset that's being deleted no longer exist.
    pub async fn get_ingestion_details(&self) -> Result<Option<IngestionDetails>, ClientError> {
        let database = &self.config.database;
        let sql = match self.config.package.storage_engine {
            StorageEngine::Clp => {
                let archives = database.unscoped_archives_table_name();
                let files = database.unscoped_files_table_name();
                format!(
                    "SELECT a.begin_timestamp, a.end_timestamp, f.num_files, f.num_messages FROM \
                     (SELECT MIN(begin_timestamp) AS begin_timestamp, MAX(end_timestamp) AS \
                     end_timestamp FROM `{archives}`) AS a, (SELECT COUNT(DISTINCT orig_file_id) \
                     AS num_files, CAST(COALESCE(SUM(num_messages), 0) AS SIGNED) AS num_messages \
                     FROM `{files}`) AS f"
                )
            }
            StorageEngine::ClpS => {
                let datasets = self.get_dataset_names().await?;
                if datasets.is_empty() {
                    return Ok(None);
                }
                let archives_union = Self::union_all(datasets.iter().map(|dataset| {
                    format!(
                        "SELECT MIN(begin_timestamp) AS begin_timestamp, MAX(end_timestamp) AS \
                         end_timestamp FROM `{}`",
                        database.archives_table_name(dataset)
                    )
                }));
                let files_union = Self::union_all(datasets.iter().map(|dataset| {
                    format!(
                        "SELECT COUNT(DISTINCT orig_file_id) AS num_files, \
                         CAST(COALESCE(SUM(num_messages), 0) AS SIGNED) AS num_messages FROM `{}`",
                        database.files_table_name(dataset)
                    )
                }));
                format!(
                    "SELECT a.begin_timestamp, a.end_timestamp, f.num_files, f.num_messages FROM \
                     (SELECT MIN(begin_timestamp) AS begin_timestamp, MAX(end_timestamp) AS \
                     end_timestamp FROM ({archives_union}) AS archives_combined) AS a, (SELECT \
                     CAST(SUM(num_files) AS SIGNED) AS num_files, CAST(SUM(num_messages) AS \
                     SIGNED) AS num_messages FROM ({files_union}) AS files_combined) AS f"
                )
            }
        };
        let row = sqlx::query_as::<_, IngestionDetailsRowProjection>(&sql)
            .fetch_one(&self.sql_pool)
            .await?;
        Ok(row.into())
    }

    /// Fetches the query speed (total uncompressed bytes scanned and job duration) for a query job
    /// across the given datasets.
    ///
    /// For the CLP storage engine, `datasets` is ignored.
    ///
    /// # Returns
    ///
    /// The query-speed statistics on success, or `None` when:
    ///
    /// * the job doesn't exist;
    /// * the job has no tasks over the archives of the given datasets;
    /// * the job hasn't finished yet (its duration is `NULL`);
    /// * for CLP-S, `datasets` is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::DatasetNotFound`] if, for CLP-S, any of `datasets` doesn't exist. The first
    ///   missing dataset in `datasets` is reported.
    /// * Forwards [`sqlx::query::QueryAs::fetch_optional`]'s return values on any other failure.
    /// * Forwards [`sqlx::query::QueryScalar::fetch_all`]'s return values if looking up the missing
    ///   dataset fails.
    pub async fn get_query_speed(
        &self,
        datasets: &[DatasetName],
        query_job_id: QueryJobId,
    ) -> Result<Option<QuerySpeed>, ClientError> {
        let database = &self.config.database;
        let archives_subquery = match self.config.package.storage_engine {
            StorageEngine::Clp => format!(
                "SELECT id, uncompressed_size FROM `{}`",
                database.unscoped_archives_table_name()
            ),
            StorageEngine::ClpS => {
                if datasets.is_empty() {
                    return Ok(None);
                }
                Self::union_all(datasets.iter().map(|dataset| {
                    format!(
                        "SELECT id, uncompressed_size FROM `{}`",
                        database.archives_table_name(dataset)
                    )
                }))
            }
        };
        let sql = format!(
            "SELECT CAST(SUM(ca.uncompressed_size) AS DOUBLE) AS bytes, qj.duration AS duration \
             FROM `{QUERY_JOBS_TABLE_NAME}` qj JOIN `{QUERY_TASKS_TABLE_NAME}` qt ON qt.job_id = \
             qj.id JOIN ({archives_subquery}) ca ON qt.archive_id = ca.id WHERE qj.id = ? GROUP \
             BY qj.id, qj.duration"
        );
        let row = match sqlx::query_as::<_, QuerySpeedRowProjection>(&sql)
            .bind(query_job_id)
            .fetch_optional(&self.sql_pool)
            .await
        {
            Ok(row) => row,
            Err(err) => return Err(self.resolve_dataset_query_error(datasets, err).await),
        };
        Ok(row.and_then(Into::into))
    }

    /// Fetches the timestamp column names for a given dataset (CLP-S only).
    ///
    /// # Returns
    ///
    /// The distinct timestamp column names, ordered by name, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::DatasetNotFound`] if the dataset's column metadata table doesn't exist.
    /// * Forwards [`sqlx::query::QueryScalar::fetch_all`]'s return values on failure.
    pub async fn get_timestamp_column_names(
        &self,
        dataset: &DatasetName,
    ) -> Result<Vec<String>, ClientError> {
        // Must be kept in sync with `NodeType::Timestamp` and `NodeType::DeprecatedDateString` in
        // `components/core/src/clp_s/SchemaTree.hpp`.
        const TIMESTAMP_NODE_TYPE: i8 = 14;
        const DEPRECATED_DATE_STRING_NODE_TYPE: i8 = 8;

        let table_name = self.config.database.column_metadata_table_name(dataset);
        let names: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT DISTINCT name FROM `{table_name}` WHERE type IN (?, ?) ORDER BY name"
        ))
        .bind(TIMESTAMP_NODE_TYPE)
        .bind(DEPRECATED_DATE_STRING_NODE_TYPE)
        .fetch_all(&self.sql_pool)
        .await
        .map_err(|err| {
            if Self::is_table_not_found(&err) {
                return ClientError::DatasetNotFound(dataset.clone());
            }
            err.into()
        })?;
        Ok(names)
    }

    /// Fetches the metadata of the most recent compression jobs, with the display fields of each
    /// job's config.
    ///
    /// # Returns
    ///
    /// The metadata of the 1000 most recent compression jobs, most recent first, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::TaskJoin`] if the task decoding the rows panics.
    /// * Forwards [`sqlx::query::QueryAs::fetch_all`]'s return values on failure.
    /// * Forwards [`CompressionMetadata::try_from`]'s return values on failure, so the whole call
    ///   fails if any of the jobs' stored configs can't be decoded into a
    ///   [`CompressionMetadataDisplayConfig`].
    pub async fn get_compression_metadata(&self) -> Result<Vec<CompressionMetadata>, ClientError> {
        const QUERY_LIMIT: i64 = 1000;

        let rows = sqlx::query_as::<_, CompressionMetadataRowProjection>(&format!(
            "SELECT id, status, status_msg, start_time, update_time, duration, uncompressed_size, \
             compressed_size, clp_config FROM `{COMPRESSION_JOBS_TABLE_NAME}` ORDER BY id DESC \
             LIMIT ?"
        ))
        .bind(QUERY_LIMIT)
        .fetch_all(&self.sql_pool)
        .await?;
        tokio::task::spawn_blocking(move || {
            rows.into_iter()
                .map(CompressionMetadata::try_from)
                .collect()
        })
        .await?
    }

    /// Converts the failure of a query over the given datasets' tables into a [`ClientError`].
    ///
    /// # Returns
    ///
    /// * [`ClientError::DatasetNotFound`] for the first of `datasets` that isn't in the datasets
    ///   table, if `error` is a "table doesn't exist" error and the storage engine is CLP-S.
    /// * The lookup's error, if looking up `datasets` in the datasets table fails.
    /// * `error` otherwise, including when every one of `datasets` is in the datasets table.
    async fn resolve_dataset_query_error(
        &self,
        datasets: &[DatasetName],
        error: sqlx::Error,
    ) -> ClientError {
        if self.config.package.storage_engine != StorageEngine::ClpS
            || datasets.is_empty()
            || !Self::is_table_not_found(&error)
        {
            return error.into();
        }
        let table = self.config.database.datasets_table_name();
        let placeholders = vec!["?"; datasets.len()].join(", ");
        let sql = format!("SELECT name FROM `{table}` WHERE name IN ({placeholders})");
        let mut query = sqlx::query_scalar::<_, DatasetName>(&sql);
        for dataset in datasets {
            query = query.bind(dataset);
        }
        let existing: HashSet<DatasetName> = match query.fetch_all(&self.sql_pool).await {
            Ok(names) => names.into_iter().collect(),
            Err(lookup_error) => return lookup_error.into(),
        };
        datasets
            .iter()
            .find(|dataset| !existing.contains(*dataset))
            .map_or_else(
                || error.into(),
                |dataset| ClientError::DatasetNotFound(dataset.clone()),
            )
    }

    /// Combines `SELECT` statements with `UNION ALL`.
    ///
    /// # Returns
    ///
    /// The combined statement.
    fn union_all(selects: impl Iterator<Item = String>) -> String {
        selects.collect::<Vec<_>>().join("\nUNION ALL\n")
    }

    /// # Returns
    ///
    /// Whether `error` is a `MySQL` "table doesn't exist" error.
    fn is_table_not_found(error: &sqlx::Error) -> bool {
        // MySQL error number for "Table doesn't exist".
        const MYSQL_TABLE_NOT_FOUND: u16 = 1146;

        if let sqlx::Error::Database(db_err) = error
            && let Some(mysql_err) = db_err.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
        {
            return mysql_err.number() == MYSQL_TABLE_NOT_FOUND;
        }
        false
    }
}

/// Reads the clp-s metadata behind the `WebUI`'s metadata routes.
///
/// # Type Parameters
///
/// * `'metadata_db_lifetime` - The lifetime of the metadata database handle.
/// * `MetadataDbType` - The type of the metadata database handle that the metadata is read from.
struct ClpSMetadataReader<'metadata_db_lifetime, MetadataDbType: DatasetLookup + MetadataStatistics>
{
    metadata_db: &'metadata_db_lifetime mut MetadataDbType,
}

impl<'metadata_db_lifetime, MetadataDbType: DatasetLookup + MetadataStatistics>
    ClpSMetadataReader<'metadata_db_lifetime, MetadataDbType>
{
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created [`ClpSMetadataReader`] that reads from `metadata_db`.
    const fn new(metadata_db: &'metadata_db_lifetime mut MetadataDbType) -> Self {
        Self { metadata_db }
    }

    /// # Returns
    ///
    /// The names of the live datasets, ordered by name, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`DatasetLookup::get_datasets`]'s return values on failure.
    async fn get_dataset_names(&mut self) -> Result<Vec<DatasetName>, ClientError> {
        Ok(self
            .metadata_db
            .get_datasets()
            .await?
            .into_iter()
            .map(|dataset| dataset.name)
            .collect())
    }

    /// # Returns
    ///
    /// The earliest and latest log entry timestamps across the archives of `datasets`, or `None`
    /// when they have no archives, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::get_dataset_ids`]'s return values on failure.
    /// * Forwards [`MetadataStatistics::get_timestamp_bounds`]'s return values on failure.
    async fn get_time_range(
        &mut self,
        datasets: &[DatasetName],
    ) -> Result<Option<TimeRange>, ClientError> {
        let dataset_ids = self.get_dataset_ids(datasets).await?;
        Ok(self
            .metadata_db
            .get_timestamp_bounds(&dataset_ids)
            .await?
            .map(TimeRange::from))
    }

    /// # Returns
    ///
    /// The total uncompressed and compressed sizes of the archives of every live dataset on
    /// success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`MetadataStatistics::get_space_savings`]'s return values on failure.
    /// * Forwards [`SpaceSavings::try_from`]'s return values on failure.
    async fn get_space_savings(&mut self) -> Result<SpaceSavings, ClientError> {
        SpaceSavings::try_from(self.metadata_db.get_space_savings().await?)
    }

    /// # Returns
    ///
    /// The ingestion details across the archives of every live dataset, or `None` when there are no
    /// archives, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`DatasetLookup::get_datasets`]'s return values on failure.
    /// * Forwards [`MetadataStatistics::get_timestamp_bounds`]'s return values on failure.
    async fn get_ingestion_details(&mut self) -> Result<Option<IngestionDetails>, ClientError> {
        let dataset_ids: Vec<DatasetId> = self
            .metadata_db
            .get_datasets()
            .await?
            .into_iter()
            .map(|dataset| dataset.id)
            .collect();
        let bounds = self.metadata_db.get_timestamp_bounds(&dataset_ids).await?;
        // clp-s has no files table, so it reports no files or messages, as it did when it had
        // per-dataset files tables that it never filled.
        Ok(bounds.map(|range| IngestionDetails {
            begin_timestamp: range.begin_timestamp_millisecs(),
            end_timestamp: range.end_timestamp_millisecs(),
            num_files: 0,
            num_messages: 0,
        }))
    }

    /// # Returns
    ///
    /// The distinct names of the timestamp columns of the dataset named `dataset`, ordered by name,
    /// on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::DatasetNotFound`] if `dataset` doesn't exist.
    /// * Forwards [`DatasetLookup::get_dataset_by_name`]'s return values on failure.
    /// * Forwards [`MetadataStatistics::get_column_names_by_types`]'s return values on failure.
    async fn get_timestamp_column_names(
        &mut self,
        dataset: &DatasetName,
    ) -> Result<Vec<String>, ClientError> {
        let dataset_id = self
            .metadata_db
            .get_dataset_by_name(dataset)
            .await?
            .ok_or_else(|| ClientError::DatasetNotFound(dataset.clone()))?
            .id;
        Ok(self
            .metadata_db
            .get_column_names_by_types(
                dataset_id,
                &[ColumnType::Timestamp, ColumnType::DeprecatedDateString],
            )
            .await?)
    }

    /// # Returns
    ///
    /// The IDs of the datasets named in `datasets`, in the same order, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::DatasetNotFound`] if any of `datasets` doesn't exist. The first missing
    ///   dataset in `datasets` is reported.
    /// * Forwards [`DatasetLookup::get_dataset_ids_by_names`]'s return values on failure.
    async fn get_dataset_ids(
        &mut self,
        datasets: &[DatasetName],
    ) -> Result<Vec<DatasetId>, ClientError> {
        let dataset_ids = self.metadata_db.get_dataset_ids_by_names(datasets).await?;
        datasets
            .iter()
            .map(|dataset| {
                dataset_ids
                    .get(dataset)
                    .copied()
                    .ok_or_else(|| ClientError::DatasetNotFound(dataset.clone()))
            })
            .collect()
    }
}

/// A projection of the earliest and latest log entry timestamps aggregated over archives. Both are
/// `NULL` when there are no archives to aggregate.
#[derive(Debug, sqlx::FromRow)]
struct TimeRangeRowProjection {
    begin_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
}

impl From<TimeRangeRowProjection> for Option<TimeRange> {
    fn from(row: TimeRangeRowProjection) -> Self {
        let (Some(begin_timestamp), Some(end_timestamp)) = (row.begin_timestamp, row.end_timestamp)
        else {
            return None;
        };
        Some(TimeRange {
            begin_timestamp,
            end_timestamp,
        })
    }
}

/// A projection of the ingestion details aggregated over archives and files. The timestamps are
/// `NULL` when there are no archives to aggregate.
#[derive(Debug, sqlx::FromRow)]
struct IngestionDetailsRowProjection {
    begin_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
    num_files: i64,
    num_messages: i64,
}

impl From<IngestionDetailsRowProjection> for Option<IngestionDetails> {
    fn from(row: IngestionDetailsRowProjection) -> Self {
        let (Some(begin_timestamp), Some(end_timestamp)) = (row.begin_timestamp, row.end_timestamp)
        else {
            return None;
        };
        Some(IngestionDetails {
            begin_timestamp,
            end_timestamp,
            num_files: row.num_files,
            num_messages: row.num_messages,
        })
    }
}

/// A projection of a query job's scanned bytes and duration. The duration is `NULL` until the job
/// finishes.
#[derive(Debug, sqlx::FromRow)]
struct QuerySpeedRowProjection {
    bytes: f64,
    duration: Option<f32>,
}

impl From<QuerySpeedRowProjection> for Option<QuerySpeed> {
    fn from(row: QuerySpeedRowProjection) -> Self {
        Some(QuerySpeed {
            bytes: row.bytes,
            duration: row.duration?,
        })
    }
}

/// A projection of a compression job row, with its config still serialized.
#[derive(Debug, sqlx::FromRow)]
struct CompressionMetadataRowProjection {
    id: CompressionJobId,
    status: i32,
    status_msg: String,
    start_time: Option<DateTime<Utc>>,
    update_time: DateTime<Utc>,
    duration: Option<f32>,
    uncompressed_size: i64,
    compressed_size: i64,
    clp_config: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use async_trait::async_trait;
    use chrono::DateTime;
    use clp_metadata_db::ArchiveTimestampRange;
    use clp_metadata_db::ColumnType;
    use clp_metadata_db::Dataset;
    use clp_metadata_db::DatasetConfig;
    use clp_metadata_db::DatasetLookup;
    use clp_metadata_db::MetadataStatistics;
    use clp_rust_utils::clp_config::AwsAuthentication;
    use clp_rust_utils::clp_config::AwsCredentials;
    use clp_rust_utils::clp_config::S3Config;
    use clp_rust_utils::clp_config::package::config::Config;
    use clp_rust_utils::clp_config::package::config::Package;
    use clp_rust_utils::clp_config::package::config::StorageEngine;
    use clp_rust_utils::dataset::DatasetName;
    use clp_rust_utils::job_config::ClpIoConfig;
    use clp_rust_utils::job_config::FsInputConfig;
    use clp_rust_utils::job_config::InputConfig;
    use clp_rust_utils::job_config::OutputConfig;
    use clp_rust_utils::job_config::S3InputConfig;
    use clp_rust_utils::serde::ZstdMsgpack;
    use clp_rust_utils::types::DatasetId;
    use non_empty_string::NonEmptyString;

    use super::ClpSMetadataReader;
    use super::CompressionMetadata;
    use super::CompressionMetadataRowProjection;
    use super::IngestionDetails;
    use super::IngestionDetailsRowProjection;
    use super::QuerySpeed;
    use super::QuerySpeedRowProjection;
    use super::SpaceSavings;
    use super::TimeRange;
    use super::TimeRangeRowProjection;
    use crate::error::ClientError;
    use crate::webui_client::WebuiClient;

    /// A column-metadata row.
    struct StoredColumn {
        dataset_id: DatasetId,
        name: &'static str,
        column_type: ColumnType,
    }

    /// A metadata database holding the given live datasets, their archives' timestamp bounds and
    /// their columns, which records the datasets it receives timestamp-bound reads for.
    #[derive(Default)]
    struct MockMetadataDb {
        /// The live datasets, ordered by name as the real lookup guarantees.
        datasets: Vec<Dataset>,

        timestamp_bounds: HashMap<DatasetId, ArchiveTimestampRange>,

        columns: Vec<StoredColumn>,

        timestamp_bounds_reads: Vec<Vec<DatasetId>>,
    }

    impl MockMetadataDb {
        /// Factory function.
        ///
        /// # Returns
        ///
        /// A metadata database holding the live datasets named `names`, with IDs from `1` in the
        /// order of `names`.
        ///
        /// # Panics
        ///
        /// Panics if `names` isn't ordered, or if any of them isn't a valid dataset name.
        fn with_datasets(names: &[&str]) -> Self {
            assert!(names.is_sorted(), "the datasets should be ordered by name");
            let datasets = (1..)
                .zip(names)
                .map(|(id, name)| Dataset {
                    id: DatasetId::from(id),
                    name: name.parse().expect("the dataset name should be valid"),
                    config: DatasetConfig {
                        archive_storage_path: format!("/var/data/archives/{name}"),
                        retention_period_minutes: None,
                    },
                })
                .collect();
            Self {
                datasets,
                ..Self::default()
            }
        }
    }

    #[async_trait]
    impl DatasetLookup for MockMetadataDb {
        async fn get_datasets(&mut self) -> Result<Vec<Dataset>, clp_metadata_db::Error> {
            Ok(self.datasets.clone())
        }

        async fn get_dataset_by_name(
            &mut self,
            name: &DatasetName,
        ) -> Result<Option<Dataset>, clp_metadata_db::Error> {
            Ok(self
                .datasets
                .iter()
                .find(|dataset| dataset.name == *name)
                .cloned())
        }

        async fn get_dataset_ids_by_names(
            &mut self,
            names: &[DatasetName],
        ) -> Result<HashMap<DatasetName, DatasetId>, clp_metadata_db::Error> {
            Ok(self
                .datasets
                .iter()
                .filter(|dataset| names.contains(&dataset.name))
                .map(|dataset| (dataset.name.clone(), dataset.id))
                .collect())
        }
    }

    #[async_trait]
    impl MetadataStatistics for MockMetadataDb {
        async fn get_timestamp_bounds(
            &mut self,
            dataset_ids: &[DatasetId],
        ) -> Result<Option<ArchiveTimestampRange>, clp_metadata_db::Error> {
            self.timestamp_bounds_reads.push(dataset_ids.to_vec());
            let ranges: Vec<&ArchiveTimestampRange> = dataset_ids
                .iter()
                .filter_map(|dataset_id| self.timestamp_bounds.get(dataset_id))
                .collect();
            let begin = ranges
                .iter()
                .map(|range| range.begin_timestamp_millisecs())
                .min();
            let end = ranges
                .iter()
                .map(|range| range.end_timestamp_millisecs())
                .max();
            Ok(begin
                .zip(end)
                .map(|(begin, end)| ArchiveTimestampRange::new(begin, end))
                .transpose()?)
        }

        async fn get_space_savings(
            &mut self,
        ) -> Result<clp_metadata_db::SpaceSavings, clp_metadata_db::Error> {
            unreachable!("the space savings are converted without reading the metadata database")
        }

        async fn get_column_names_by_types(
            &mut self,
            dataset_id: DatasetId,
            types: &[ColumnType],
        ) -> Result<Vec<String>, clp_metadata_db::Error> {
            let mut names: Vec<String> = self
                .columns
                .iter()
                .filter(|column| {
                    column.dataset_id == dataset_id && types.contains(&column.column_type)
                })
                .map(|column| column.name.to_owned())
                .collect();
            names.sort();
            names.dedup();
            Ok(names)
        }
    }

    fn non_empty(value: &str) -> NonEmptyString {
        NonEmptyString::new(value.to_owned()).expect("the test value should be non-empty")
    }

    #[test]
    fn time_range_is_none_unless_both_timestamps_are_present() {
        for (begin_timestamp, end_timestamp) in [(None, None), (Some(1), None), (None, Some(2))] {
            let row = TimeRangeRowProjection {
                begin_timestamp,
                end_timestamp,
            };
            assert_eq!(Option::<TimeRange>::from(row), None);
        }
        let row = TimeRangeRowProjection {
            begin_timestamp: Some(1),
            end_timestamp: Some(2),
        };
        assert_eq!(
            Option::<TimeRange>::from(row),
            Some(TimeRange {
                begin_timestamp: 1,
                end_timestamp: 2,
            })
        );
    }

    #[test]
    fn ingestion_details_is_none_unless_both_timestamps_are_present() {
        for (begin_timestamp, end_timestamp) in [(None, None), (Some(1), None), (None, Some(2))] {
            let row = IngestionDetailsRowProjection {
                begin_timestamp,
                end_timestamp,
                num_files: 0,
                num_messages: 0,
            };
            assert_eq!(Option::<IngestionDetails>::from(row), None);
        }
        let row = IngestionDetailsRowProjection {
            begin_timestamp: Some(1),
            end_timestamp: Some(2),
            num_files: 3,
            num_messages: 4,
        };
        assert_eq!(
            Option::<IngestionDetails>::from(row),
            Some(IngestionDetails {
                begin_timestamp: 1,
                end_timestamp: 2,
                num_files: 3,
                num_messages: 4,
            })
        );
    }

    #[test]
    fn query_speed_is_none_until_the_duration_is_recorded() {
        let row = QuerySpeedRowProjection {
            bytes: 1024.0,
            duration: None,
        };
        assert_eq!(Option::<QuerySpeed>::from(row), None);
        let row = QuerySpeedRowProjection {
            bytes: 1024.0,
            duration: Some(2.0),
        };
        assert_eq!(
            Option::<QuerySpeed>::from(row),
            Some(QuerySpeed {
                bytes: 1024.0,
                duration: 2.0,
            })
        );
    }

    #[tokio::test]
    async fn clp_dataset_names_are_empty() -> anyhow::Result<()> {
        let config = Config {
            package: Package {
                storage_engine: StorageEngine::Clp,
            },
            ..Config::default()
        };
        let sql_pool = sqlx::mysql::MySqlPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_secs(1))
            .connect_lazy("mysql://localhost:1/clp-db")?;
        let mongodb_client = mongodb::Client::with_uri_str("mongodb://localhost:1").await?;
        let client = WebuiClient::new(config, sql_pool, mongodb_client).await;
        assert_eq!(client.get_dataset_names().await?, Vec::<DatasetName>::new());
        Ok(())
    }

    #[tokio::test]
    async fn clp_s_query_speed_without_datasets_is_none() -> anyhow::Result<()> {
        let config = Config {
            package: Package {
                storage_engine: StorageEngine::ClpS,
            },
            ..Config::default()
        };
        let sql_pool = sqlx::mysql::MySqlPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_secs(1))
            .connect_lazy("mysql://localhost:1/clp-db")?;
        let mongodb_client = mongodb::Client::with_uri_str("mongodb://localhost:1").await?;
        let client = WebuiClient::new(config, sql_pool, mongodb_client).await;
        assert_eq!(client.get_query_speed(&[], 1).await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn clp_s_time_range_without_datasets_is_none() -> anyhow::Result<()> {
        let config = Config {
            package: Package {
                storage_engine: StorageEngine::ClpS,
            },
            ..Config::default()
        };
        let sql_pool = sqlx::mysql::MySqlPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_secs(1))
            .connect_lazy("mysql://localhost:1/clp-db")?;
        let mongodb_client = mongodb::Client::with_uri_str("mongodb://localhost:1").await?;
        let client = WebuiClient::new(config, sql_pool, mongodb_client).await;
        assert_eq!(client.get_time_range(&[]).await?, None);
        Ok(())
    }

    #[test]
    fn compression_metadata_keeps_display_fields_and_omits_storage_credentials()
    -> anyhow::Result<()> {
        let config = ClpIoConfig {
            input: InputConfig::S3InputConfig {
                config: S3InputConfig {
                    s3_config: S3Config {
                        bucket: non_empty("the-bucket"),
                        region_code: Some(non_empty("us-east-2")),
                        key_prefix: non_empty("logs/"),
                        endpoint_url: None,
                        aws_authentication: AwsAuthentication::Credentials {
                            credentials: AwsCredentials {
                                access_key_id: "the-access-key".to_owned(),
                                secret_access_key: "the-secret-key".to_owned(),
                                session_token: Some("the-session-token".to_owned()),
                            },
                        },
                    },
                    keys: Some(vec![non_empty("logs/app.log")]),
                    dataset: "logs".parse()?,
                    timestamp_key: Some(non_empty("ts")),
                    unstructured: false,
                },
            },
            output: OutputConfig {
                target_archive_size: 1,
                target_dictionaries_size: 2,
                target_encoded_file_size: 3,
                target_segment_size: 4,
                compression_level: 5,
            },
        };
        let row = CompressionMetadataRowProjection {
            id: 7,
            status: 2,
            status_msg: "done".to_owned(),
            start_time: Some(DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp")),
            update_time: DateTime::from_timestamp(1_700_000_060, 0).expect("valid timestamp"),
            duration: Some(60.0),
            uncompressed_size: 100,
            compressed_size: 10,
            clp_config: ZstdMsgpack::serialize(&config)?,
        };

        let metadata = CompressionMetadata::try_from(row)?;

        assert_eq!(metadata.id, 7);
        assert_eq!(metadata.status, 2);
        assert_eq!(metadata.status_msg, "done");
        assert_eq!(
            metadata.start_time,
            Some(DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp"))
        );
        assert_eq!(
            metadata.update_time,
            DateTime::from_timestamp(1_700_000_060, 0).expect("valid timestamp")
        );
        assert_eq!(metadata.duration, Some(60.0));
        assert_eq!(metadata.uncompressed_size, 100);
        assert_eq!(metadata.compressed_size, 10);
        assert_eq!(
            serde_json::to_value(metadata.clp_config)?,
            serde_json::json!({
                "input": {
                    "type": "s3",
                    "dataset": "logs",
                    "keys": ["logs/app.log"],
                    "timestamp_key": "ts",
                    "unstructured": false
                },
                "output": {
                    "target_archive_size": 1,
                    "target_dictionaries_size": 2,
                    "target_encoded_file_size": 3,
                    "target_segment_size": 4,
                    "compression_level": 5
                }
            })
        );
        Ok(())
    }

    #[test]
    fn compression_metadata_keeps_filesystem_display_fields() -> anyhow::Result<()> {
        let config = ClpIoConfig {
            input: InputConfig::FsInputConfig {
                config: FsInputConfig {
                    dataset: "logs".parse()?,
                    paths_to_compress: vec![non_empty("/mnt/logs/app.log")],
                    path_prefix_to_remove: Some(non_empty("/mnt/logs")),
                    timestamp_key: None,
                    unstructured: true,
                },
            },
            output: OutputConfig {
                target_archive_size: 1,
                target_dictionaries_size: 2,
                target_encoded_file_size: 3,
                target_segment_size: 4,
                compression_level: 5,
            },
        };
        let row = CompressionMetadataRowProjection {
            id: 7,
            status: 0,
            status_msg: String::new(),
            start_time: None,
            update_time: DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp"),
            duration: None,
            uncompressed_size: 0,
            compressed_size: 0,
            clp_config: ZstdMsgpack::serialize(&config)?,
        };

        assert_eq!(
            serde_json::to_value(CompressionMetadata::try_from(row)?.clp_config)?,
            serde_json::json!({
                "input": {
                    "type": "fs",
                    "dataset": "logs",
                    "paths_to_compress": ["/mnt/logs/app.log"],
                    "path_prefix_to_remove": "/mnt/logs",
                    "unstructured": true
                },
                "output": {
                    "target_archive_size": 1,
                    "target_dictionaries_size": 2,
                    "target_encoded_file_size": 3,
                    "target_segment_size": 4,
                    "compression_level": 5
                }
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn clp_s_time_range_reports_the_first_missing_dataset() -> anyhow::Result<()> {
        let mut metadata_db = MockMetadataDb::with_datasets(&["logs", "other"]);
        let datasets: Vec<DatasetName> = vec![
            "logs".parse()?,
            "missing_first".parse()?,
            "other".parse()?,
            "missing_second".parse()?,
        ];

        let error = ClpSMetadataReader::new(&mut metadata_db)
            .get_time_range(&datasets)
            .await
            .expect_err("a missing dataset should fail the read");

        let ClientError::DatasetNotFound(missing_dataset) = error else {
            panic!("unexpected error: {error:?}");
        };
        assert_eq!(missing_dataset, "missing_first".parse::<DatasetName>()?);
        assert_eq!(
            metadata_db.timestamp_bounds_reads,
            Vec::<Vec<DatasetId>>::new()
        );
        Ok(())
    }

    #[tokio::test]
    async fn clp_s_time_range_covers_only_the_given_datasets() -> anyhow::Result<()> {
        let mut metadata_db = MockMetadataDb::with_datasets(&["first", "second", "third"]);
        metadata_db.timestamp_bounds = HashMap::from([
            (DatasetId::from(1), ArchiveTimestampRange::new(10, 20)?),
            (DatasetId::from(2), ArchiveTimestampRange::new(1, 5)?),
            (DatasetId::from(3), ArchiveTimestampRange::new(15, 30)?),
        ]);

        let time_range = ClpSMetadataReader::new(&mut metadata_db)
            .get_time_range(&["third".parse()?, "first".parse()?])
            .await?;

        assert_eq!(
            time_range,
            Some(TimeRange {
                begin_timestamp: 10,
                end_timestamp: 30,
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn clp_s_ingestion_details_cover_every_live_dataset_without_files_or_messages()
    -> anyhow::Result<()> {
        let mut metadata_db = MockMetadataDb::with_datasets(&["first", "second"]);
        assert_eq!(
            ClpSMetadataReader::new(&mut metadata_db)
                .get_ingestion_details()
                .await?,
            None
        );

        metadata_db.timestamp_bounds = HashMap::from([
            (DatasetId::from(1), ArchiveTimestampRange::new(10, 20)?),
            (DatasetId::from(2), ArchiveTimestampRange::new(1, 5)?),
        ]);
        assert_eq!(
            ClpSMetadataReader::new(&mut metadata_db)
                .get_ingestion_details()
                .await?,
            Some(IngestionDetails {
                begin_timestamp: 1,
                end_timestamp: 20,
                num_files: 0,
                num_messages: 0,
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn clp_s_timestamp_column_names_are_the_timestamp_columns_of_the_dataset()
    -> anyhow::Result<()> {
        let mut metadata_db = MockMetadataDb::with_datasets(&["logs", "other"]);
        metadata_db.columns = vec![
            StoredColumn {
                dataset_id: DatasetId::from(1),
                name: "ts",
                column_type: ColumnType::Timestamp,
            },
            StoredColumn {
                dataset_id: DatasetId::from(1),
                name: "date",
                column_type: ColumnType::DeprecatedDateString,
            },
            StoredColumn {
                dataset_id: DatasetId::from(1),
                name: "count",
                column_type: ColumnType::Integer,
            },
            StoredColumn {
                dataset_id: DatasetId::from(2),
                name: "other_ts",
                column_type: ColumnType::Timestamp,
            },
        ];

        assert_eq!(
            ClpSMetadataReader::new(&mut metadata_db)
                .get_timestamp_column_names(&"logs".parse()?)
                .await?,
            vec!["date".to_owned(), "ts".to_owned()]
        );

        let error = ClpSMetadataReader::new(&mut metadata_db)
            .get_timestamp_column_names(&"missing".parse()?)
            .await
            .expect_err("a missing dataset should fail the read");
        let ClientError::DatasetNotFound(missing_dataset) = error else {
            panic!("unexpected error: {error:?}");
        };
        assert_eq!(missing_dataset, "missing".parse::<DatasetName>()?);
        Ok(())
    }

    #[test]
    fn clp_s_space_savings_reject_sizes_above_i64_max() -> anyhow::Result<()> {
        assert_eq!(
            SpaceSavings::try_from(clp_metadata_db::SpaceSavings {
                num_uncompressed_bytes: 100,
                num_compressed_bytes: 10,
            })?,
            SpaceSavings {
                total_uncompressed_size: 100,
                total_compressed_size: 10,
            }
        );

        let out_of_range_size = u64::try_from(i64::MAX)? + 1;
        for space_savings in [
            clp_metadata_db::SpaceSavings {
                num_uncompressed_bytes: out_of_range_size,
                num_compressed_bytes: 10,
            },
            clp_metadata_db::SpaceSavings {
                num_uncompressed_bytes: 100,
                num_compressed_bytes: out_of_range_size,
            },
        ] {
            let error = SpaceSavings::try_from(space_savings)
                .expect_err("a size above `i64::MAX` should be rejected");
            assert!(
                matches!(error, ClientError::ArchiveSizeOutOfRange(size) if size == out_of_range_size),
                "unexpected error: {error:?}"
            );
        }
        Ok(())
    }
}
