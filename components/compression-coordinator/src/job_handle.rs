//! Handle for driving a single compression job to completion.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use clp_metadata_db::MetadataTables;
use clp_rust_utils::dataset::DatasetId;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::job_config::COMPRESSION_JOBS_TABLE_NAME;
use clp_rust_utils::job_config::ClpIoConfig;
use clp_rust_utils::job_config::CompressionJobId;
use clp_rust_utils::job_config::CompressionJobStatus;
use clp_rust_utils::job_config::FsInputConfig;
use clp_rust_utils::job_config::InputConfig;
use clp_rust_utils::job_config::S3ObjectMetadataInputConfig;
use clp_rust_utils::logs_input::default_mount_point;
use clp_rust_utils::s3::ObjectMetadata;
use clp_rust_utils::s3::S3ObjectMetadataId;
use clp_rust_utils::task_io::compression::ClpSCompressionOption;
use clp_rust_utils::task_io::compression::Dataset;
use clp_rust_utils::task_io::compression::FsInputSource;
use clp_rust_utils::task_io::compression::S3InputSource;
use const_format::formatcp;
use non_empty_string::NonEmptyString;
use spider_core::task::ExecutionPolicy;
use spider_core::task::TimeoutPolicy;
use spider_core::types::id::JobId as SpiderJobId;
use spider_core::types::id::ResourceGroupId;
use sqlx::MySqlPool;

use crate::Error;
use crate::compression_job_submitter::CompressionJobOutcome;
use crate::compression_job_submitter::CompressionJobSubmitter;
use crate::compression_job_submitter::CompressionManifest;
use crate::input::discover_fs_entries;
use crate::partition::CompressionInputBuilder;
use crate::partition::FsSource;

/// Options for a compression job running in Spider.
pub struct SpiderOption {
    pub compression_task_max_retry: u32,
    pub commit_task_execution_policy: ExecutionPolicy,
    pub initial_poll_backoff: Duration,
    pub max_poll_backoff: Duration,
}

/// Resources and execution options shared by compression job handles.
pub struct CompressionJobHandleContext {
    pub db_pool: MySqlPool,
    pub metadata_tables: MetadataTables,
    pub spider_option: SpiderOption,
}

/// Handles the asynchronous submission of a compression job and the retrieval of its result.
///
/// # Type Parameters
///
/// * `SubmitterType` - The type of the job submitter for Spider job submission.
pub struct CompressionJobHandle<SubmitterType: CompressionJobSubmitter> {
    context: Arc<CompressionJobHandleContext>,
    compression_job_id: CompressionJobId,
    job_submitter: SubmitterType,
    resource_group_id: ResourceGroupId,

    input_config: InputConfig,
    clp_s_compression_option: ClpSCompressionOption,
    dataset: DatasetName,
    target_archive_size: u64,
}

impl<SubmitterType: CompressionJobSubmitter> CompressionJobHandle<SubmitterType> {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created [`CompressionJobHandle`] for the given compression job, with the `clp-s`
    /// compression options derived from `clp_io_config`'s output config.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::UnsupportedInputConfig`] if `clp_io_config` requests direct S3 discovery.
    pub fn new(
        context: Arc<CompressionJobHandleContext>,
        compression_job_id: CompressionJobId,
        job_submitter: SubmitterType,
        resource_group_id: ResourceGroupId,
        clp_io_config: ClpIoConfig,
    ) -> Result<Self, Error> {
        let input_config = clp_io_config.input;
        let (dataset, timestamp_key, unstructured) = match &input_config {
            InputConfig::S3ObjectMetadataInputConfig { config } => (
                config.dataset.clone(),
                config.timestamp_key.clone(),
                config.unstructured,
            ),
            InputConfig::FsInputConfig { config } => (
                config.dataset.clone(),
                config.timestamp_key.clone(),
                config.unstructured,
            ),
            InputConfig::S3InputConfig { .. } => return Err(Error::UnsupportedInputConfig),
        };

        let output_config = clp_io_config.output;
        let clp_s_compression_option = ClpSCompressionOption {
            target_encoded_size: output_config.target_segment_size
                + output_config.target_dictionaries_size,
            compression_level: output_config.compression_level,
            timestamp_key: timestamp_key.map(String::from),
            unstructured,
        };

        Ok(Self {
            context,
            compression_job_id,
            job_submitter,
            resource_group_id,
            input_config,
            clp_s_compression_option,
            dataset,
            target_archive_size: output_config.target_archive_size,
        })
    }

    /// Submits the compression job to Spider and drives it to completion.
    ///
    /// This method prepares the compression tasks' inputs, looks up the ID of the job's dataset,
    /// submits the job, persists the Spider job ID it was assigned, and then waits for the job to
    /// reach a terminal state. On any failure, the compression job is marked as
    /// [`CompressionJobStatus::Failed`] before the error is returned.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::submit_and_wait`]'s return values on failure.
    pub async fn run(self) -> Result<(), Error> {
        tracing::info!(compression_job_id = % self.compression_job_id, "Starting compression job.");

        let result = self.submit_and_wait().await;
        if let Err(err) = &result {
            self.report_failure(err).await;
        }

        result
    }

    /// Resumes a compression job that was already submitted to Spider.
    ///
    /// This method skips submission and waits for the Spider job identified by `spider_job_id` to
    /// reach a terminal state. On failure, the compression job is marked as
    /// [`CompressionJobStatus::Failed`] before the error is returned.
    ///
    /// NOTE: It's the caller's responsibility to ensure that the given Spider job ID is associated
    /// with the compression job.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::to_completion`]'s return values on failure.
    pub async fn recover(self, spider_job_id: SpiderJobId) -> Result<(), Error> {
        tracing::info!(
            compression_job_id = % self.compression_job_id,
            spider_job_id = % spider_job_id,
            "Recovering compression job.",
        );
        match self.to_completion(spider_job_id).await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.report_failure(&err).await;
                Err(err)
            }
        }
    }

    /// Submits the compression job to Spider and waits for it to reach a terminal state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::prepare_s3_task_inputs`]'s return values on failure.
    /// * Forwards [`Self::prepare_fs_task_inputs`]'s return values on failure.
    /// * Forwards [`Self::submit`]'s return values on failure.
    /// * Forwards [`Self::to_completion`]'s return values on failure.
    async fn submit_and_wait(&self) -> Result<(), Error> {
        let spider_job_id = match &self.input_config {
            InputConfig::S3ObjectMetadataInputConfig { config } => {
                self.submit(self.prepare_s3_task_inputs(config).await?)
                    .await?
            }
            InputConfig::FsInputConfig { config } => {
                self.submit(self.prepare_fs_task_inputs(config).await?)
                    .await?
            }
            InputConfig::S3InputConfig { .. } => {
                unreachable!("the input config is validated in the factory")
            }
        };
        self.to_completion(spider_job_id).await
    }

    /// Submits the compression job and persists its Spider association.
    ///
    /// # Type Parameters
    ///
    /// * `ManifestType` - The concrete input manifest consumed by each compression task.
    ///
    /// # Returns
    ///
    /// The submitted Spider job ID on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::get_dataset_id`]'s return values on failure.
    /// * Forwards [`CompressionJobSubmitter::submit_compression_job`]'s return values on failure.
    /// * Forwards [`Self::persist_spider_job_id`]'s return values on failure.
    async fn submit<ManifestType: CompressionManifest>(
        &self,
        input_sources: Vec<(ManifestType, ExecutionPolicy)>,
    ) -> Result<SpiderJobId, Error> {
        let num_tasks = input_sources.len();

        let dataset = Dataset {
            id: self.get_dataset_id().await?,
            name: self.dataset.clone(),
        };

        let spider_job_id = self
            .job_submitter
            .submit_compression_job(
                self.compression_job_id,
                self.resource_group_id,
                self.clp_s_compression_option.clone(),
                dataset,
                input_sources,
                self.context
                    .spider_option
                    .commit_task_execution_policy
                    .clone(),
            )
            .await?;
        tracing::info!(
            compression_job_id = % self.compression_job_id,
            spider_job_id = % spider_job_id,
            "Compression job submitted.",
        );

        self.persist_spider_job_id(spider_job_id, num_tasks).await?;
        tracing::info!(
            compression_job_id = % self.compression_job_id,
            spider_job_id = % spider_job_id,
            "Compression job submission persisted.",
        );

        Ok(spider_job_id)
    }

    /// Reports a compression job failure.
    ///
    /// This method logs the original error and attempts to mark the compression job as
    /// [`CompressionJobStatus::Failed`] in the CLP database. The stored status message includes the
    /// original error message.
    ///
    /// If updating the job status fails, the status-update error is logged for observability and
    /// otherwise ignored.
    async fn report_failure(&self, err: &Error) {
        tracing::error!(
            compression_job_id = % self.compression_job_id,
            error = % err,
            "Compression job failed.",
        );
        let status_message = format!("Compression job failed: {err}");
        if let Err(e) = self
            .update_job_status(CompressionJobStatus::Failed, Some(status_message))
            .await
        {
            tracing::error!(
                compression_job_id = % self.compression_job_id,
                error = % e,
                "Failed to update job status on a job failure.",
            );
        }
    }

    /// Prepares the task inputs for the compression job.
    ///
    /// This method retrieves object metadata from the S3 object metadata table in the CLP database,
    /// partitions the objects into compression task inputs, and attaches the configured execution
    /// policy to each task.
    ///
    /// # Returns
    ///
    /// A vector of tuples on success, where each tuple contains:
    ///
    /// * An [`S3InputSource`] representing the input to a single compression task.
    /// * The [`ExecutionPolicy`] for that task.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::NoS3ObjectMetadata`] if the job doesn't request any object metadata IDs.
    /// * [`Error::DuplicateS3ObjectMetadata`] if the requested IDs contain a duplicate.
    /// * [`Error::MissingS3ObjectMetadata`] if a requested ID does not exist in the metadata table.
    /// * [`Error::EmptyS3ObjectMetadataField`] if a requested bucket or key is empty.
    /// * [`Error::NoTaskInputs`] if no compression task inputs are produced.
    /// * Forwards [`CompressionInputBuilder::add`]'s return values on failure.
    /// * Forwards [`sqlx::query::QueryAs::fetch_all`]'s return values on failure.
    async fn prepare_s3_task_inputs(
        &self,
        config: &S3ObjectMetadataInputConfig,
    ) -> Result<Vec<(S3InputSource, ExecutionPolicy)>, Error> {
        // NOTE: We batch the metadata fetch to avoid hitting the maximum placeholder limit of
        // MySQL.
        const FETCH_CHUNK_SIZE: usize = 1000;

        if config.s3_object_metadata_ids.is_empty() {
            return Err(Error::NoS3ObjectMetadata(config.ingestion_job_id));
        }

        let mut sorted_metadata_ids = config.s3_object_metadata_ids.clone();
        sorted_metadata_ids.sort_unstable();
        let mut duplicate_ids: Vec<S3ObjectMetadataId> = Vec::new();
        sorted_metadata_ids.dedup_by(|a, b| {
            if *a != *b {
                return false;
            }
            if duplicate_ids.last().copied() != Some(*b) {
                duplicate_ids.push(*b);
            }
            true
        });
        if !duplicate_ids.is_empty() {
            return Err(Error::DuplicateS3ObjectMetadata {
                ingestion_job_id: config.ingestion_job_id,
                ids: duplicate_ids,
            });
        }

        let mut input_builder =
            CompressionInputBuilder::new(config.s3_config.clone(), self.target_archive_size);
        for chunk in sorted_metadata_ids.chunks(FETCH_CHUNK_SIZE) {
            let mut query_builder = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
                "SELECT `id`, `bucket`, `key`, `size` FROM \
                 `{INGESTED_S3_OBJECT_METADATA_TABLE_NAME}` WHERE `id` IN ("
            ));
            let mut separated_ids = query_builder.separated(", ");
            for metadata_id in chunk {
                separated_ids.push_bind(metadata_id);
            }
            query_builder
                .push(") AND `ingestion_job_id` = ")
                .push_bind(config.ingestion_job_id);
            query_builder.push(" ORDER BY `id` ASC");

            let metadata_rows = query_builder
                .build_query_as::<S3ObjectMetadataRow>()
                .fetch_all(&self.context.db_pool)
                .await?;

            let mut row_iter = metadata_rows.into_iter().peekable();
            for &expected_id in chunk {
                if row_iter.peek().is_none_or(|row| row.id != expected_id) {
                    return Err(Error::MissingS3ObjectMetadata {
                        ingestion_job_id: config.ingestion_job_id,
                        id: expected_id,
                    });
                }
                let row = row_iter
                    .next()
                    .expect("row should have been validated by the previous peek");
                let bucket = NonEmptyString::new(row.bucket).map_err(|_| {
                    Error::EmptyS3ObjectMetadataField {
                        id: row.id,
                        field: "bucket",
                    }
                })?;
                let key = NonEmptyString::new(row.key).map_err(|_| {
                    Error::EmptyS3ObjectMetadataField {
                        id: row.id,
                        field: "key",
                    }
                })?;
                input_builder.add(ObjectMetadata {
                    bucket,
                    key,
                    size: row.size,
                })?;
            }
        }

        let partitions = input_builder.build();
        if partitions.is_empty() {
            return Err(Error::NoTaskInputs);
        }

        Ok(partitions
            .into_iter()
            .map(|partition| {
                let execution_policy = self.s3_compression_task_execution_policy(
                    partition.estimated_uncompressed_size,
                    partition.input.object_keys.len() as u64,
                );
                (partition.input, execution_policy)
            })
            .collect())
    }

    /// Discovers and partitions filesystem inputs and attaches each task's execution policy.
    ///
    /// # Returns
    ///
    /// Regular-file manifests paired with their execution policies on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::NoFsInputPaths`] if no input paths are requested.
    /// * [`Error::FsDiscoveryJoin`] if the blocking discovery task cannot be joined.
    /// * [`Error::NoTaskInputs`] if the selection contains no regular files.
    /// * Forwards [`default_mount_point`]'s return values on failure.
    /// * Forwards [`discover_fs_entries`]'s return values on failure.
    /// * Forwards [`CompressionInputBuilder::add`]'s return values on failure.
    async fn prepare_fs_task_inputs(
        &self,
        config: &FsInputConfig,
    ) -> Result<Vec<(FsInputSource, ExecutionPolicy)>, Error> {
        if config.paths_to_compress.is_empty() {
            return Err(Error::NoFsInputPaths);
        }
        let paths = config.paths_to_compress.clone();
        let entries = tokio::task::spawn_blocking(move || {
            discover_fs_entries(&paths, default_mount_point()?)
        })
        .await??;
        let mut input_builder = CompressionInputBuilder::new(FsSource, self.target_archive_size);
        for entry in entries {
            input_builder.add(entry)?;
        }
        let partitions = input_builder.build();
        if partitions.is_empty() {
            return Err(Error::NoTaskInputs);
        }
        Ok(partitions
            .into_iter()
            .map(|partition| {
                let execution_policy = self.fs_compression_task_execution_policy(
                    partition.estimated_uncompressed_size,
                    partition.input.file_paths.len() as u64,
                );
                (partition.input, execution_policy)
            })
            .collect())
    }

    /// Derives an S3 compression task's retry and timeout policy from its input size and download
    /// input count.
    ///
    /// # Returns
    ///
    /// The configured retry policy with compression and upload timeouts based on estimated sizes
    /// and per-input download latency.
    fn s3_compression_task_execution_policy(
        &self,
        estimated_uncompressed_size: u64,
        num_download_inputs: u64,
    ) -> ExecutionPolicy {
        const FACTORS: CompressionTimeoutFactors = CompressionTimeoutFactors {
            download_input_latency_ms: 1000,
            soft_compression_throughput_bytes_per_sec: NonZeroU64::new(40 * 1024 * 1024)
                .expect("40 MiB/s is nonzero"),
            hard_compression_throughput_bytes_per_sec: NonZeroU64::new(8 * 1024 * 1024)
                .expect("8 MiB/s is nonzero"),
            output_compression_ratio_estimate: NonZeroU64::new(15).expect("15 is nonzero"),
            soft_upload_throughput_bytes_per_sec: NonZeroU64::new(40 * 1024 * 1024)
                .expect("40 MiB/s is nonzero"),
            hard_upload_throughput_bytes_per_sec: NonZeroU64::new(8 * 1024 * 1024)
                .expect("8 MiB/s is nonzero"),
        };

        ExecutionPolicy {
            max_num_retry: self.context.spider_option.compression_task_max_retry,
            timeout_policy: compression_timeout_policy(
                &FACTORS,
                estimated_uncompressed_size,
                num_download_inputs,
            ),
            ..ExecutionPolicy::default()
        }
    }

    /// Derives a filesystem compression task's retry and timeout policy from its input size and
    /// download input count.
    ///
    /// # Returns
    ///
    /// The configured retry policy with compression and upload timeouts based on estimated sizes
    /// and per-input download latency.
    fn fs_compression_task_execution_policy(
        &self,
        estimated_uncompressed_size: u64,
        num_download_inputs: u64,
    ) -> ExecutionPolicy {
        const FACTORS: CompressionTimeoutFactors = CompressionTimeoutFactors {
            download_input_latency_ms: 1000,
            soft_compression_throughput_bytes_per_sec: NonZeroU64::new(40 * 1024 * 1024)
                .expect("40 MiB/s is nonzero"),
            hard_compression_throughput_bytes_per_sec: NonZeroU64::new(8 * 1024 * 1024)
                .expect("8 MiB/s is nonzero"),
            output_compression_ratio_estimate: NonZeroU64::new(15).expect("15 is nonzero"),
            soft_upload_throughput_bytes_per_sec: NonZeroU64::new(40 * 1024 * 1024)
                .expect("40 MiB/s is nonzero"),
            hard_upload_throughput_bytes_per_sec: NonZeroU64::new(8 * 1024 * 1024)
                .expect("8 MiB/s is nonzero"),
        };

        ExecutionPolicy {
            max_num_retry: self.context.spider_option.compression_task_max_retry,
            timeout_policy: compression_timeout_policy(
                &FACTORS,
                estimated_uncompressed_size,
                num_download_inputs,
            ),
            ..ExecutionPolicy::default()
        }
    }

    /// Looks up the ID of the job's dataset.
    ///
    /// # Returns
    ///
    /// The ID of the job's dataset on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::DatasetNotFound`] if the dataset doesn't exist or is marked as deleted.
    /// * Forwards [`sqlx::Pool::acquire`]'s return values on failure.
    /// * Forwards [`MetadataTables::get_dataset_id_by_name`]'s return values on failure.
    async fn get_dataset_id(&self) -> Result<DatasetId, Error> {
        let mut conn = self.context.db_pool.acquire().await?;
        self.context
            .metadata_tables
            .get_dataset_id_by_name(&mut conn, &self.dataset)
            .await?
            .ok_or_else(|| Error::DatasetNotFound(self.dataset.clone()))
    }

    /// Persists the Spider job ID and marks the compression job as running.
    ///
    /// This method associates the given Spider job ID with the compression job in the CLP database
    /// and updates the compression job status to [`CompressionJobStatus::Running`].
    ///
    /// This method also ensures that the job has a valid `dispatch_time`, which the coordinator
    /// uses to mark jobs as dispatched. A coordinator restart may occur before the marker is
    /// persisted, leaving the Spider job running without a valid `dispatch_time`. Therefore, this
    /// method sets the field as part of row update if it has not already been set by the
    /// coordinator.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::TooManyCompressionTasks`] if `num_tasks` exceeds `i32`'s range.
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    async fn persist_spider_job_id(
        &self,
        spider_job_id: SpiderJobId,
        num_tasks: usize,
    ) -> Result<(), Error> {
        let num_tasks =
            i32::try_from(num_tasks).map_err(|_| Error::TooManyCompressionTasks(num_tasks))?;
        sqlx::query(formatcp!(
            "UPDATE `{COMPRESSION_JOBS_TABLE_NAME}` SET `spider_id` = ?, `status` = ?, \
             `num_tasks` = ?, `start_time` = CURRENT_TIMESTAMP(3), `dispatch_time` = \
             COALESCE(`dispatch_time`, CURRENT_TIMESTAMP()) WHERE `id` = ?"
        ))
        .bind(spider_job_id.get())
        .bind(CompressionJobStatus::Running)
        .bind(num_tasks)
        .bind(self.compression_job_id)
        .execute(&self.context.db_pool)
        .await?;

        Ok(())
    }

    /// Waits for the associated Spider job to complete and finalizes the compression job.
    ///
    /// This method monitors the specified Spider job until it reaches a terminal state, then
    /// updates the compression job according to the Spider job's result.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`CompressionJobSubmitter::run_compression_job_to_completion`]'s return values on
    ///   failure.
    /// * Forwards [`Self::update_job_status`]'s return values on failure.
    /// * Forwards [`Self::get_job_status`]'s return values on failure.
    async fn to_completion(&self, spider_job_id: SpiderJobId) -> Result<(), Error> {
        let outcome = self
            .job_submitter
            .run_compression_job_to_completion(
                spider_job_id,
                self.context.spider_option.initial_poll_backoff,
                self.context.spider_option.max_poll_backoff,
            )
            .await?;
        tracing::info!(
            compression_job_id = % self.compression_job_id,
            spider_job_id = % spider_job_id,
            outcome = ? outcome,
            "Compression job reached a terminal state.",
        );

        match outcome {
            // The commit task records the successful CLP job status and publishes the archives in
            // the same transaction.
            CompressionJobOutcome::Succeeded => Ok(()),
            CompressionJobOutcome::Failed { error_message } => {
                if CompressionJobStatus::Succeeded == self.get_job_status().await? {
                    // NOTE: The commit task may successfully proceed but fail to report to Spider's
                    // control unit. In that case, the compression outcome has already committed to
                    // CLP DB, and thus the job should be considered `Succeeded`.
                    return Ok(());
                }
                self.update_job_status(
                    CompressionJobStatus::Failed,
                    Some(format!(
                        "The Spider compression job failed: {error_message}"
                    )),
                )
                .await
            }
            CompressionJobOutcome::Cancelled => {
                if CompressionJobStatus::Succeeded == self.get_job_status().await? {
                    // NOTE: The commit task may successfully proceed but fail to report to Spider's
                    // control unit. In that case, the compression outcome has already committed to
                    // CLP DB, and thus the job should be considered `Succeeded`.
                    return Ok(());
                }
                self.update_job_status(
                    CompressionJobStatus::Failed,
                    Some("The Spider compression job was cancelled.".to_owned()),
                )
                .await
            }
        }
    }

    /// Reads the current status of the compression job from the CLP database.
    ///
    /// # Returns
    ///
    /// The current [`CompressionJobStatus`] of the compression job on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryScalar::fetch_one`]'s return values on failure.
    async fn get_job_status(&self) -> Result<CompressionJobStatus, Error> {
        let job_status: CompressionJobStatus = sqlx::query_scalar(formatcp!(
            "SELECT `status` FROM `{COMPRESSION_JOBS_TABLE_NAME}` WHERE `id` = ?"
        ))
        .bind(self.compression_job_id)
        .fetch_one(&self.context.db_pool)
        .await?;

        Ok(job_status)
    }

    /// Updates the compression job status in the CLP database.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    async fn update_job_status(
        &self,
        job_status: CompressionJobStatus,
        status_message: Option<String>,
    ) -> Result<(), Error> {
        let status_message = status_message.as_ref().map_or("", String::as_str);
        sqlx::query(formatcp!(
            "UPDATE `{COMPRESSION_JOBS_TABLE_NAME}` SET `status` = ?, `status_msg` = ? WHERE `id` \
             = ?"
        ))
        .bind(job_status)
        .bind(status_message)
        .bind(self.compression_job_id)
        .execute(&self.context.db_pool)
        .await?;

        Ok(())
    }
}

const INGESTED_S3_OBJECT_METADATA_TABLE_NAME: &str = "ingested_s3_object_metadata";

/// A projection of the columns read from an ingested S3 object metadata row.
#[derive(Debug, sqlx::FromRow)]
struct S3ObjectMetadataRow {
    id: S3ObjectMetadataId,
    bucket: String,
    key: String,
    size: u64,
}

/// Throughput and latency assumptions for estimating a compression task's timeouts.
///
/// A task's timeout is its input download allowance plus the time to compress its estimated
/// uncompressed size and to upload its estimated compressed size. The soft timeout uses the
/// expected throughputs, and the hard timeout uses the pessimistic ones.
struct CompressionTimeoutFactors {
    /// The allowance for downloading each input file or object.
    download_input_latency_ms: u64,

    /// The expected compression throughput, used for the soft timeout.
    soft_compression_throughput_bytes_per_sec: NonZeroU64,

    /// The pessimistic compression throughput, used for the hard timeout.
    hard_compression_throughput_bytes_per_sec: NonZeroU64,

    /// The estimated ratio of uncompressed input size to archive size, used to estimate the
    /// archive size to upload.
    output_compression_ratio_estimate: NonZeroU64,

    /// The expected archive upload throughput, used for the soft timeout.
    soft_upload_throughput_bytes_per_sec: NonZeroU64,

    /// The pessimistic archive upload throughput, used for the hard timeout.
    hard_upload_throughput_bytes_per_sec: NonZeroU64,
}

/// Estimates task timeouts from the input size and count using the supplied factors.
///
/// # Returns
///
/// A [`TimeoutPolicy`] covering download, compression, and upload within Spider's timeout limits.
fn compression_timeout_policy(
    factors: &CompressionTimeoutFactors,
    estimated_uncompressed_size: u64,
    num_download_inputs: u64,
) -> TimeoutPolicy {
    const MIN_SOFT_TIMEOUT_MS: u64 = 1000;
    const MIN_HARD_TIMEOUT_MS: u64 = 10_000;
    const MAX_TIMEOUT_MS: u64 = 24 * 60 * 60 * 1000;

    let download_latency_ms = num_download_inputs.saturating_mul(factors.download_input_latency_ms);
    let estimated_compressed_size =
        estimated_uncompressed_size.div_ceil(factors.output_compression_ratio_estimate.get());
    let timeout_ms = |compression_throughput: NonZeroU64, upload_throughput: NonZeroU64| {
        let compression_ms = estimated_uncompressed_size
            .saturating_mul(1000)
            .div_ceil(compression_throughput.get());
        let upload_ms = estimated_compressed_size
            .saturating_mul(1000)
            .div_ceil(upload_throughput.get());
        download_latency_ms
            .saturating_add(compression_ms)
            .saturating_add(upload_ms)
    };
    let soft_timeout_ms = timeout_ms(
        factors.soft_compression_throughput_bytes_per_sec,
        factors.soft_upload_throughput_bytes_per_sec,
    )
    .clamp(MIN_SOFT_TIMEOUT_MS, MAX_TIMEOUT_MS - 1);
    let hard_timeout_ms = timeout_ms(
        factors.hard_compression_throughput_bytes_per_sec,
        factors.hard_upload_throughput_bytes_per_sec,
    )
    .clamp(MIN_HARD_TIMEOUT_MS.max(soft_timeout_ms + 1), MAX_TIMEOUT_MS);
    TimeoutPolicy {
        soft_timeout_ms,
        hard_timeout_ms,
    }
}

#[cfg(test)]
mod tests {
    use clp_rust_utils::clp_config::package::config::Database;
    use clp_rust_utils::job_config::ingestion::JobId as IngestionJobId;
    use clp_rust_utils::logs_input::LogsInputPath;
    use spider_core::task::TaskGraph;
    use spider_core::task::TdlContext;
    use spider_core::task::TerminationTaskDescriptor;

    use super::*;
    use crate::partition::FsEntry;
    use crate::partition::FsSource;

    const TEST_MAX_RETRY: u32 = 7;

    #[derive(Clone)]
    struct UnreachableSubmitter;

    #[async_trait::async_trait]
    impl CompressionJobSubmitter for UnreachableSubmitter {
        async fn submit_compression_job<ManifestType: CompressionManifest>(
            &self,
            _compression_job_id: CompressionJobId,
            _resource_group_id: ResourceGroupId,
            _clp_s_option: ClpSCompressionOption,
            _dataset: Dataset,
            _input_sources: Vec<(ManifestType, ExecutionPolicy)>,
            _commit_task_execution_policy: ExecutionPolicy,
        ) -> Result<SpiderJobId, Error> {
            unreachable!("policy tests do not submit compression jobs")
        }

        async fn run_compression_job_to_completion(
            &self,
            _spider_job_id: SpiderJobId,
            _initial_poll_backoff: Duration,
            _max_poll_backoff: Duration,
        ) -> Result<CompressionJobOutcome, Error> {
            unreachable!("policy tests do not run compression jobs")
        }
    }

    impl CompressionJobHandle<UnreachableSubmitter> {
        /// # Returns
        ///
        /// A compression job handle for policy tests, with a database pool that connects lazily.
        ///
        /// # Panics
        ///
        /// Panics if the fixture's database URL or filesystem job configuration is invalid.
        fn new_for_policy_tests() -> Self {
            use clp_rust_utils::clp_config::package::config::ArchiveOutput;
            use clp_rust_utils::types::non_empty_string::ExpectedNonEmpty;

            Self::new(
                Arc::new(CompressionJobHandleContext {
                    db_pool: MySqlPool::connect_lazy("mysql://localhost/clp-db")
                        .expect("the fixture database URL is valid"),
                    metadata_tables: MetadataTables::new(&Database::default()),
                    spider_option: SpiderOption {
                        compression_task_max_retry: TEST_MAX_RETRY,
                        commit_task_execution_policy: ExecutionPolicy::default(),
                        initial_poll_backoff: Duration::from_secs(1),
                        max_poll_backoff: Duration::from_secs(1),
                    },
                }),
                1,
                UnreachableSubmitter,
                ResourceGroupId::from(1),
                ClpIoConfig {
                    input: InputConfig::FsInputConfig {
                        config: FsInputConfig {
                            dataset: DatasetName::default(),
                            paths_to_compress: vec![NonEmptyString::from_static_str("/mnt/logs")],
                            path_prefix_to_remove: None,
                            timestamp_key: None,
                            unstructured: false,
                        },
                    },
                    output: (&ArchiveOutput::default()).into(),
                },
            )
            .expect("the fixture uses a supported filesystem input configuration")
        }
    }

    #[tokio::test]
    async fn compression_timeouts_include_download_compression_and_upload() -> anyhow::Result<()> {
        let handle = CompressionJobHandle::new_for_policy_tests();
        for (size, num_download_inputs, soft_timeout_ms, hard_timeout_ms) in [
            (0, 0, 1000, 10_000),
            (1, 0, 1000, 10_000),
            (0, 1, 1000, 10_000),
            (1, 1, 1002, 10_000),
            (0, 9, 9000, 10_000),
            (0, 10, 10_000, 10_001),
            (40 * 1024 * 1024, 2, 3067, 10_000),
            (15 * 40 * 1024 * 1024 + 1, 1, 17_002, 81_002),
            (1024 * 1024 * 1024 * 1024, 1, 27_963_027, 86_400_000),
            (u64::MAX, 1, 86_399_999, 86_400_000),
            (0, 4096, 4_096_000, 4_096_001),
        ] {
            for policy in [
                handle.s3_compression_task_execution_policy(size, num_download_inputs),
                handle.fs_compression_task_execution_policy(size, num_download_inputs),
            ] {
                assert_eq!(policy.max_num_retry, TEST_MAX_RETRY);
                assert_eq!(
                    policy.timeout_policy,
                    TimeoutPolicy {
                        soft_timeout_ms,
                        hard_timeout_ms
                    }
                );
                TaskGraph::new(
                    Some(TerminationTaskDescriptor {
                        tdl_context: TdlContext {
                            package: "clp".to_owned(),
                            task_func: "compression::commit".to_owned(),
                        },
                        execution_policy: Some(policy),
                    }),
                    None,
                )?;
            }
        }
        Ok(())
    }

    #[test]
    fn timeout_factors_control_each_budget_and_round_up_output_size() {
        const FACTORS: CompressionTimeoutFactors = CompressionTimeoutFactors {
            download_input_latency_ms: 250,
            soft_compression_throughput_bytes_per_sec: NonZeroU64::new(20)
                .expect("20 bytes/s is nonzero"),
            hard_compression_throughput_bytes_per_sec: NonZeroU64::new(10)
                .expect("10 bytes/s is nonzero"),
            output_compression_ratio_estimate: NonZeroU64::new(4).expect("4 is nonzero"),
            soft_upload_throughput_bytes_per_sec: NonZeroU64::new(5).expect("5 bytes/s is nonzero"),
            hard_upload_throughput_bytes_per_sec: NonZeroU64::new(2).expect("2 bytes/s is nonzero"),
        };
        assert_eq!(
            compression_timeout_policy(&FACTORS, 41, 2),
            TimeoutPolicy {
                soft_timeout_ms: 4750,
                hard_timeout_ms: 10_100,
            }
        );
    }

    #[tokio::test]
    async fn full_filesystem_partition_has_valid_size_based_timeouts() -> anyhow::Result<()> {
        const TARGET_ARCHIVE_SIZE: u64 = 256 * 1024 * 1024;
        const NUM_FILES: u64 = 4096;

        let mut builder = CompressionInputBuilder::new(FsSource, TARGET_ARCHIVE_SIZE);
        for index in 0..NUM_FILES {
            builder.add(FsEntry {
                path: LogsInputPath::try_from(format!("/logs/{index}"))?,
                size: TARGET_ARCHIVE_SIZE / NUM_FILES,
            })?;
        }
        let partitions = builder.build();
        assert_eq!(partitions.len(), 1);
        let partition = &partitions[0];
        assert_eq!(partition.input.file_paths.len() as u64, NUM_FILES);
        let handle = CompressionJobHandle::new_for_policy_tests();
        let policy = handle.fs_compression_task_execution_policy(
            partition.estimated_uncompressed_size,
            partition.input.file_paths.len() as u64,
        );
        assert_eq!(
            policy.timeout_policy,
            TimeoutPolicy {
                soft_timeout_ms: 4_102_827,
                hard_timeout_ms: 4_130_134,
            }
        );
        TaskGraph::new(
            Some(TerminationTaskDescriptor {
                tdl_context: TdlContext {
                    package: "clp".to_owned(),
                    task_func: "compression::commit".to_owned(),
                },
                execution_policy: Some(policy),
            }),
            None,
        )?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
    async fn run_submits_the_id_of_the_live_dataset() -> anyhow::Result<()> {
        let test_db = TestDatabase::create().await;
        let dataset_id = test_db.create_dataset().await;
        let job_id = test_db.insert_job().await;
        let submitter = RecordingSubmitter::default();

        test_db
            .create_job_handle(job_id, submitter.clone())
            .run()
            .await?;

        assert_eq!(
            submitter.submitted_datasets(),
            vec![Dataset {
                id: dataset_id,
                name: TestDatabase::dataset(),
            }]
        );
        assert_eq!(
            test_db.get_job_status(job_id).await,
            (CompressionJobStatus::Running, String::new())
        );

        test_db.drop_database().await;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
    async fn run_fails_the_job_without_submitting_it_if_the_dataset_is_missing()
    -> anyhow::Result<()> {
        let test_db = TestDatabase::create().await;
        let job_id = test_db.insert_job().await;
        let submitter = RecordingSubmitter::default();

        let error = test_db
            .create_job_handle(job_id, submitter.clone())
            .run()
            .await
            .expect_err("a job whose dataset is missing must fail");
        let Error::DatasetNotFound(dataset) = &error else {
            panic!("unexpected error: {error}");
        };
        assert_eq!(*dataset, TestDatabase::dataset());

        assert_eq!(submitter.submitted_datasets(), vec![]);
        assert_eq!(
            test_db.get_job_status(job_id).await,
            (
                CompressionJobStatus::Failed,
                format!("Compression job failed: {error}")
            )
        );

        test_db.drop_database().await;
        Ok(())
    }

    /// A submitter that records the dataset of every submitted job, whose jobs succeed.
    #[derive(Clone, Default)]
    struct RecordingSubmitter {
        submitted_datasets: Arc<std::sync::Mutex<Vec<Dataset>>>,
    }

    impl RecordingSubmitter {
        /// # Returns
        ///
        /// The datasets of the submitted jobs, in submission order.
        ///
        /// # Panics
        ///
        /// Panics if the recorded datasets' lock is poisoned.
        fn submitted_datasets(&self) -> Vec<Dataset> {
            self.submitted_datasets
                .lock()
                .expect("the recorded datasets' lock should not be poisoned")
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl CompressionJobSubmitter for RecordingSubmitter {
        async fn submit_compression_job<ManifestType: CompressionManifest>(
            &self,
            _compression_job_id: CompressionJobId,
            _resource_group_id: ResourceGroupId,
            _clp_s_option: ClpSCompressionOption,
            dataset: Dataset,
            _input_sources: Vec<(ManifestType, ExecutionPolicy)>,
            _commit_task_execution_policy: ExecutionPolicy,
        ) -> Result<SpiderJobId, Error> {
            self.submitted_datasets
                .lock()
                .expect("the recorded datasets' lock should not be poisoned")
                .push(dataset);
            Ok(SpiderJobId::random())
        }

        async fn run_compression_job_to_completion(
            &self,
            _spider_job_id: SpiderJobId,
            _initial_poll_backoff: Duration,
            _max_poll_backoff: Duration,
        ) -> Result<CompressionJobOutcome, Error> {
            Ok(CompressionJobOutcome::Succeeded)
        }
    }

    /// A uniquely named database on the test server, used by a single test, with the shared
    /// metadata tables, the columns of the compression jobs table that job handles use, and the
    /// ingested S3 object metadata of a single object.
    struct TestDatabase {
        pool: MySqlPool,
        metadata_tables: MetadataTables,
        name: String,
    }

    impl TestDatabase {
        const INGESTION_JOB_ID: IngestionJobId = 1;
        const S3_OBJECT_METADATA_ID: S3ObjectMetadataId = 1;

        /// Factory function.
        ///
        /// Creates a uniquely named database on the server at `CLP_METADATA_DB_TEST_URL` (e.g.
        /// `mysql://root:password@127.0.0.1:3306`), connects to it, and creates its tables.
        ///
        /// # Returns
        ///
        /// The newly created test database.
        ///
        /// # Panics
        ///
        /// Panics if:
        ///
        /// * `CLP_METADATA_DB_TEST_URL` isn't set or isn't a valid MySQL URL.
        /// * The database or its tables can't be created, or the database can't be connected to.
        async fn create() -> Self {
            use std::str::FromStr;

            use sqlx::mysql::MySqlConnectOptions;

            let server_url = std::env::var("CLP_METADATA_DB_TEST_URL").expect(
                "`CLP_METADATA_DB_TEST_URL` should be set to a MariaDB or MySQL server URL",
            );
            let server_options = MySqlConnectOptions::from_str(&server_url)
                .expect("`CLP_METADATA_DB_TEST_URL` should be a valid MySQL URL");
            let name = format!(
                "compression_coordinator_test_{}",
                uuid::Uuid::new_v4().simple()
            );
            let server_pool = MySqlPool::connect_with(server_options.clone())
                .await
                .expect("failed to connect to the test server");
            sqlx::query(&format!("CREATE DATABASE `{name}`"))
                .execute(&server_pool)
                .await
                .expect("failed to create the test database");
            server_pool.close().await;

            let pool = MySqlPool::connect_with(server_options.database(&name))
                .await
                .expect("failed to connect to the test database");
            let metadata_tables = MetadataTables::new(&Database::default());
            metadata_tables
                .create_tables(&mut pool.acquire().await.expect("failed to get a connection"))
                .await
                .expect("failed to create the shared metadata tables");
            for query in [
                format!(
                    r"
CREATE TABLE `{COMPRESSION_JOBS_TABLE_NAME}` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `status` INT NOT NULL,
    `status_msg` VARCHAR(512) NOT NULL DEFAULT '',
    `start_time` DATETIME(3) NULL DEFAULT NULL,
    `num_tasks` INT NOT NULL DEFAULT '0',
    `spider_id` BIGINT UNSIGNED NULL DEFAULT NULL,
    `dispatch_time` DATETIME NULL DEFAULT NULL,
    PRIMARY KEY (`id`)
)"
                ),
                format!(
                    r"
CREATE TABLE `{INGESTED_S3_OBJECT_METADATA_TABLE_NAME}` (
    `id` BIGINT UNSIGNED NOT NULL,
    `ingestion_job_id` BIGINT UNSIGNED NOT NULL,
    `bucket` VARCHAR(64) NOT NULL,
    `key` VARCHAR(1024) NOT NULL,
    `size` BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (`id`)
)"
                ),
            ] {
                sqlx::query(&query)
                    .execute(&pool)
                    .await
                    .expect("failed to create a table");
            }
            sqlx::query(&format!(
                "INSERT INTO `{INGESTED_S3_OBJECT_METADATA_TABLE_NAME}` (`id`, \
                 `ingestion_job_id`, `bucket`, `key`, `size`) VALUES (?, ?, 'logs', \
                 'logs/a.json', 100)"
            ))
            .bind(Self::S3_OBJECT_METADATA_ID)
            .bind(Self::INGESTION_JOB_ID)
            .execute(&pool)
            .await
            .expect("failed to insert the S3 object metadata");

            Self {
                pool,
                metadata_tables,
                name,
            }
        }

        /// # Returns
        ///
        /// The name of the dataset that the test jobs compress into.
        fn dataset() -> DatasetName {
            "logs".parse().expect("test dataset name should be valid")
        }

        /// Creates the dataset that the test jobs compress into.
        ///
        /// # Returns
        ///
        /// The created dataset's ID.
        ///
        /// # Panics
        ///
        /// Panics if the dataset can't be created.
        async fn create_dataset(&self) -> DatasetId {
            self.metadata_tables
                .create_dataset(
                    &mut self
                        .pool
                        .acquire()
                        .await
                        .expect("failed to get a connection"),
                    &clp_metadata_db::NewDataset {
                        name: Self::dataset(),
                        archive_storage_path: "/var/data/archives/logs".to_owned(),
                        retention_period_minutes: None,
                    },
                )
                .await
                .expect("failed to create the dataset")
        }

        /// Inserts a pending compression job.
        ///
        /// # Returns
        ///
        /// The inserted job's ID.
        ///
        /// # Panics
        ///
        /// Panics if the job can't be inserted.
        async fn insert_job(&self) -> CompressionJobId {
            let id = sqlx::query(formatcp!(
                "INSERT INTO `{COMPRESSION_JOBS_TABLE_NAME}` (`status`) VALUES (?)"
            ))
            .bind(CompressionJobStatus::Pending)
            .execute(&self.pool)
            .await
            .expect("failed to insert the compression job")
            .last_insert_id();
            CompressionJobId::try_from(id).expect("the test job's ID should fit in `i32`")
        }

        /// # Returns
        ///
        /// A handle for the given job that compresses the test S3 object into the test dataset.
        ///
        /// # Panics
        ///
        /// Panics if the handle can't be created.
        fn create_job_handle(
            &self,
            job_id: CompressionJobId,
            submitter: RecordingSubmitter,
        ) -> CompressionJobHandle<RecordingSubmitter> {
            use clp_rust_utils::clp_config::AwsAuthentication;
            use clp_rust_utils::clp_config::S3Config;
            use clp_rust_utils::clp_config::package::config::ArchiveOutput;
            use clp_rust_utils::types::non_empty_string::ExpectedNonEmpty;

            CompressionJobHandle::new(
                Arc::new(CompressionJobHandleContext {
                    db_pool: self.pool.clone(),
                    metadata_tables: self.metadata_tables.clone(),
                    spider_option: SpiderOption {
                        compression_task_max_retry: TEST_MAX_RETRY,
                        commit_task_execution_policy: ExecutionPolicy::default(),
                        initial_poll_backoff: Duration::from_secs(1),
                        max_poll_backoff: Duration::from_secs(1),
                    },
                }),
                job_id,
                submitter,
                ResourceGroupId::from(1),
                ClpIoConfig {
                    input: InputConfig::S3ObjectMetadataInputConfig {
                        config: S3ObjectMetadataInputConfig {
                            s3_config: S3Config {
                                bucket: NonEmptyString::from_static_str("logs"),
                                region_code: None,
                                key_prefix: NonEmptyString::from_static_str("logs/"),
                                endpoint_url: None,
                                aws_authentication: AwsAuthentication::Default,
                            },
                            ingestion_job_id: Self::INGESTION_JOB_ID,
                            s3_object_metadata_ids: vec![Self::S3_OBJECT_METADATA_ID],
                            dataset: Self::dataset(),
                            timestamp_key: None,
                            unstructured: false,
                        },
                    },
                    output: (&ArchiveOutput::default()).into(),
                },
            )
            .expect("the test job uses a supported S3 object metadata input configuration")
        }

        /// # Returns
        ///
        /// The status and status message of a compression job.
        ///
        /// # Panics
        ///
        /// Panics if the job can't be read.
        async fn get_job_status(&self, id: CompressionJobId) -> (CompressionJobStatus, String) {
            sqlx::query_as(formatcp!(
                "SELECT `status`, `status_msg` FROM `{COMPRESSION_JOBS_TABLE_NAME}` WHERE `id` = ?"
            ))
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .expect("failed to read the compression job")
        }

        /// Drops the test database and closes the connections to it.
        ///
        /// # Panics
        ///
        /// Panics if the database can't be dropped.
        async fn drop_database(self) {
            sqlx::query(&format!("DROP DATABASE `{}`", self.name))
                .execute(&self.pool)
                .await
                .expect("failed to drop the test database");
            self.pool.close().await;
        }
    }
}
