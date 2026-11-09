//! Handle for driving a single query job to completion.

use std::cmp::Reverse;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use clp_metadata_db::MetadataTables;
use clp_metadata_db::QueryArchiveFilter;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::job_config::QUERY_JOBS_TABLE_NAME;
use clp_rust_utils::job_config::QueryJobConfig;
use clp_rust_utils::job_config::QueryJobId;
use clp_rust_utils::job_config::QueryJobStatus;
use clp_rust_utils::job_config::QueryOutput;
use clp_rust_utils::job_config::ResultsCacheOutput;
use const_format::formatcp;
use spider_core::task::ExecutionPolicy;
use spider_core::task::TimeoutPolicy;
use spider_core::types::id::JobId as SpiderJobId;
use spider_core::types::id::ResourceGroupId;
use sqlx::MySql;
use sqlx::MySqlPool;
use sqlx::Transaction;

use crate::Error;
use crate::query_job_submitter::ArchiveMetadata;
use crate::query_job_submitter::QueryJobOutcome;
use crate::query_job_submitter::QueryJobSubmitter;

/// Options for a query job running in Spider.
pub struct SpiderOption {
    pub poll_interval: Duration,
    pub query_task_max_retry: u32,
}

/// Options for selecting the archives to search.
pub struct ArchiveSelectionOptions {
    pub max_datasets_per_query: Option<NonZeroUsize>,
}

/// Resources shared by query job handles created by the coordinator.
pub struct QueryJobHandleContext {
    pub db_pool: MySqlPool,
    pub metadata_tables: MetadataTables,
    pub archive_selection_options: ArchiveSelectionOptions,
    pub spider_option: SpiderOption,
}

/// Handles the asynchronous submission of a query job and the retrieval of its result.
///
/// # Type Parameters
///
/// * `SubmitterType` - The type of the job submitter for Spider job submission.
pub struct QueryJobHandle<SubmitterType: QueryJobSubmitter> {
    context: Arc<QueryJobHandleContext>,
    query_job_id: QueryJobId,
    job_submitter: SubmitterType,
    resource_group_id: ResourceGroupId,
    query_job_config: QueryJobConfig,
    job_creation_timestamp_millisecs: i64,
}

impl<SubmitterType: QueryJobSubmitter> QueryJobHandle<SubmitterType> {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created [`QueryJobHandle`] for the given query job.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::UnsupportedJobConfig`] if:
    ///   * The job carries an aggregation.
    ///   * The job writes its results to files.
    pub fn new(
        context: Arc<QueryJobHandleContext>,
        query_job_id: QueryJobId,
        job_submitter: SubmitterType,
        resource_group_id: ResourceGroupId,
        query_job_config: QueryJobConfig,
        job_creation_timestamp_millisecs: i64,
    ) -> Result<Self, Error> {
        match &query_job_config.output {
            QueryOutput::ResultsCache(ResultsCacheOutput {
                aggregation: None, ..
            })
            | QueryOutput::Network(_) => {}
            QueryOutput::ResultsCache(ResultsCacheOutput {
                aggregation: Some(_),
                ..
            }) => {
                return Err(Error::UnsupportedJobConfig(
                    "aggregation jobs are not supported".to_owned(),
                ));
            }
            QueryOutput::File {} => {
                return Err(Error::UnsupportedJobConfig(
                    "writing query results to files is not supported".to_owned(),
                ));
            }
        }

        Ok(Self {
            context,
            query_job_id,
            job_submitter,
            resource_group_id,
            query_job_config,
            job_creation_timestamp_millisecs,
        })
    }

    /// Submits the query job to Spider and drives it to completion.
    ///
    /// This method prepares the query tasks' inputs, submits the job, persists the Spider job ID it
    /// was assigned, and then waits for the job to reach a terminal state. On failure, it attempts
    /// to persist a terminal status for the query job before the error is returned.
    ///
    /// If no archives are selected, it marks the query job as succeeded without submitting it.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::plan`]'s return values on failure.
    /// * Forwards [`Self::terminate`]'s return values on failure for an empty plan.
    /// * Forwards [`Self::submit`]'s return values on failure.
    /// * Forwards [`Self::to_completion`]'s return values on failure.
    pub async fn run(self) -> Result<(), Error> {
        tracing::info!(query_job_id = % self.query_job_id, "Starting query job.");

        let result = async {
            let archives_to_search = self.plan().await?;
            if archives_to_search.is_empty() {
                return self.terminate(QueryJobOutcome::Succeeded).await;
            }

            let spider_job_id = self.submit(archives_to_search).await?;
            self.to_completion(spider_job_id).await
        }
        .await;
        if let Err(error) = &result {
            self.finalize_on_error(error).await;
        }
        result
    }

    /// Resumes a query job that was already submitted to Spider.
    ///
    /// This method skips submission and waits for the Spider job identified by `spider_job_id` to
    /// reach a terminal state. On failure, it attempts to persist a terminal status for the query
    /// job before the error is returned.
    ///
    /// NOTE: It's the caller's responsibility to ensure that the given Spider job ID is associated
    /// with the query job.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::to_completion`]'s return values on failure.
    pub async fn recover(self, spider_job_id: SpiderJobId) -> Result<(), Error> {
        tracing::info!(
            query_job_id = % self.query_job_id,
            spider_job_id = % spider_job_id,
            "Recovering query job.",
        );

        let result = self.to_completion(spider_job_id).await;
        if let Err(error) = &result {
            self.finalize_on_error(error).await;
        }
        result
    }

    /// Submits the query job to Spider and persists its running state.
    ///
    /// # Returns
    ///
    /// The submitted Spider job ID on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::TooManyQueryTasks`] if the number of query tasks exceeds `i32`'s range.
    /// * Forwards [`QueryJobSubmitter::submit_query_job`]'s return values on failure.
    /// * Forwards [`Self::start`]'s return values on failure.
    async fn submit(
        &self,
        archives_to_search: Vec<(ArchiveMetadata, ExecutionPolicy)>,
    ) -> Result<SpiderJobId, Error> {
        let num_tasks = archives_to_search.len();
        let persisted_num_tasks =
            i32::try_from(num_tasks).map_err(|_| Error::TooManyQueryTasks(num_tasks))?;
        let spider_job_id = self
            .job_submitter
            .submit_query_job(
                self.query_job_id,
                self.resource_group_id,
                self.query_job_config.query.clone(),
                self.query_job_config.output.clone(),
                archives_to_search,
            )
            .await?;

        tracing::info!(
            query_job_id = % self.query_job_id,
            spider_job_id = % spider_job_id,
            num_tasks,
            "Query job submitted.",
        );

        if let Err(start_error) = self.start(spider_job_id, persisted_num_tasks).await {
            let _ = self
                .job_submitter
                .cancel_query_job(spider_job_id)
                .await
                .inspect_err(|e| {
                    tracing::warn!(
                        query_job_id = % self.query_job_id,
                        spider_job_id = % spider_job_id,
                        error = % e,
                        "Failed to cancel the Spider job after failing to start the query job.",
                    );
                });
            return Err(start_error);
        }
        Ok(spider_job_id)
    }

    /// Prepares the task inputs for the query job.
    ///
    /// # Returns
    ///
    /// A vector of tuples on success, where each tuple contains:
    ///
    /// * The [`ArchiveMetadata`] identifying the archive searched by a single query task.
    /// * The [`ExecutionPolicy`] for that task.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ArchiveSelector::select`]'s return values on failure.
    async fn plan(&self) -> Result<Vec<(ArchiveMetadata, ExecutionPolicy)>, Error> {
        ArchiveSelector {
            context: &self.context,
            query_job_config: &self.query_job_config,
            job_creation_timestamp_millisecs: self.job_creation_timestamp_millisecs,
        }
        .select()
        .await
    }

    /// Persists the Spider job ID and marks the query job as running.
    ///
    /// This method associates the given Spider job ID with the query job in the CLP database and
    /// updates the query job status from [`QueryJobStatus::Pending`] to
    /// [`QueryJobStatus::Running`], in a transaction that locks the query job's row.
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
    /// * [`Error::QueryJobCancelled`] if the query job is in [`QueryJobStatus::CancelRequested`].
    /// * [`Error::InvalidQueryJobStatusTransition`] if the query job is in any other status than
    ///   [`QueryJobStatus::Pending`].
    /// * Forwards [`sqlx::Pool::begin`]'s return values on failure.
    /// * Forwards [`Self::lock_and_get_status`]'s return values on failure.
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    /// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
    async fn start(&self, spider_job_id: SpiderJobId, num_tasks: i32) -> Result<(), Error> {
        const UPDATE_QUERY: &str = formatcp!(
            "UPDATE `{QUERY_JOBS_TABLE_NAME}` SET `spider_id` = ?, `status` = ?, `num_tasks` = ?, \
             `start_time` = CURRENT_TIMESTAMP(3), `dispatch_time` = COALESCE(`dispatch_time`, \
             CURRENT_TIMESTAMP()) WHERE `id` = ?"
        );

        let mut tx = self.context.db_pool.begin().await?;
        match self.lock_and_get_status(&mut tx).await? {
            QueryJobStatus::Pending => {}
            QueryJobStatus::CancelRequested => return Err(Error::QueryJobCancelled),
            from => {
                return Err(Error::InvalidQueryJobStatusTransition {
                    from,
                    to: QueryJobStatus::Running,
                });
            }
        }

        sqlx::query(UPDATE_QUERY)
            .bind(spider_job_id.get())
            .bind(QueryJobStatus::Running)
            .bind(num_tasks)
            .bind(self.query_job_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        Ok(())
    }

    /// Waits for the associated Spider job to complete and finalizes the query job.
    ///
    /// This method monitors the specified Spider job until it reaches a terminal state, then
    /// updates the query job according to the Spider job's result.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`QueryJobSubmitter::run_query_job_to_completion`]'s return values on failure.
    /// * Forwards [`Self::terminate`]'s return values on failure.
    async fn to_completion(&self, spider_job_id: SpiderJobId) -> Result<(), Error> {
        let outcome = self
            .job_submitter
            .run_query_job_to_completion(spider_job_id, self.context.spider_option.poll_interval)
            .await?;

        tracing::info!(
            query_job_id = % self.query_job_id,
            spider_job_id = % spider_job_id,
            outcome = ? outcome,
            "Query job reached a terminal Spider state.",
        );

        self.terminate(outcome).await
    }

    /// Finalizes the query job after the handle failed to drive it to completion.
    ///
    /// This method logs `error` as a job-handle failure and then attempts to terminate the query
    /// job with the terminal status implied by `error`. Nothing is persisted if `error` shows that
    /// the job's row is gone or that the job already reached a terminal status through another
    /// path.
    ///
    /// A failed attempt is logged and otherwise ignored, so that it can't re-enter this path.
    async fn finalize_on_error(&self, error: &Error) {
        tracing::error!(
            query_job_id = % self.query_job_id,
            error = % error,
            "Query job failed.",
        );

        let outcome = match error {
            Error::QueryJobMetadataCorrupted(_) => return,
            Error::InvalidQueryJobStatusTransition { from, .. } if from.is_terminal() => return,
            Error::QueryJobCancelled => QueryJobOutcome::Cancelled,
            error => QueryJobOutcome::Failed {
                error_message: format!("Query job failed: {error}"),
            },
        };

        let Err(terminate_error) = self.terminate(outcome).await else {
            return;
        };
        match terminate_error {
            Error::InvalidQueryJobStatusTransition { from, .. } if from.is_terminal() => {
                tracing::warn!(
                    query_job_id = % self.query_job_id,
                    from = ? from,
                    "Query job already reached a terminal status; skipping the status update.",
                );
            }
            terminate_error => {
                tracing::error!(
                    query_job_id = % self.query_job_id,
                    error = % terminate_error,
                    "Failed to update job status on a job failure.",
                );
            }
        }
    }

    /// Terminates the query job with the given outcome in the CLP database.
    ///
    /// This method runs in a transaction that locks the query job's row, and resolves the status to
    /// persist from the locked status as follows:
    ///
    /// * [`QueryJobOutcome::Succeeded`] is permitted from [`QueryJobStatus::Pending`] and
    ///   [`QueryJobStatus::Running`]. From [`QueryJobStatus::CancelRequested`] and
    ///   [`QueryJobStatus::Cancelling`], the job is terminated as [`QueryJobStatus::Cancelled`]
    ///   instead, since a cancellation wins over a success.
    /// * [`QueryJobOutcome::Cancelled`] is permitted only from [`QueryJobStatus::CancelRequested`]
    ///   and [`QueryJobStatus::Cancelling`]. From [`QueryJobStatus::Running`], the job is
    ///   terminated as [`QueryJobStatus::Failed`] instead.
    /// * [`QueryJobOutcome::Failed`] is permitted from any non-terminal status, since a failure
    ///   wins over both a success and a cancellation.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::InvalidQueryJobStatusTransition`] if the query job's current status doesn't
    ///   permit a transition to `outcome`'s status.
    /// * Forwards [`sqlx::Pool::begin`]'s return values on failure.
    /// * Forwards [`Self::lock_and_get_status`]'s return values on failure.
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    /// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
    async fn terminate(&self, outcome: QueryJobOutcome) -> Result<(), Error> {
        const UPDATE_QUERY: &str = formatcp!(
            "UPDATE `{QUERY_JOBS_TABLE_NAME}` SET `status` = ?, `status_msg` = ? WHERE `id` = ?"
        );
        const CANCELLED_AFTER_COMPLETION_MESSAGE: &str =
            "The query job completed, but it had already been requested to be cancelled.";
        const CANCELLED_MESSAGE: &str = "The query job was cancelled.";
        const CANCELLED_UNEXPECTED_MESSAGE: &str =
            "The query job was cancelled unexpectedly without a cancellation request.";

        let mut tx = self.context.db_pool.begin().await?;
        let current_status = self.lock_and_get_status(&mut tx).await?;

        let to = QueryJobStatus::from(&outcome);
        let (status_to_persist, status_message) = match (outcome, current_status) {
            (QueryJobOutcome::Succeeded, QueryJobStatus::Pending | QueryJobStatus::Running) => {
                (to, None)
            }
            (
                QueryJobOutcome::Cancelled,
                QueryJobStatus::CancelRequested | QueryJobStatus::Cancelling,
            ) => (to, Some(CANCELLED_MESSAGE.to_owned())),
            (
                QueryJobOutcome::Succeeded,
                QueryJobStatus::CancelRequested | QueryJobStatus::Cancelling,
            ) => {
                tracing::info!(
                    query_job_id = % self.query_job_id,
                    "{CANCELLED_AFTER_COMPLETION_MESSAGE}",
                );
                (
                    QueryJobStatus::Cancelled,
                    Some(CANCELLED_AFTER_COMPLETION_MESSAGE.to_owned()),
                )
            }
            (QueryJobOutcome::Cancelled, QueryJobStatus::Running) => {
                tracing::error!(
                    query_job_id = % self.query_job_id,
                    "{CANCELLED_UNEXPECTED_MESSAGE}",
                );
                (
                    QueryJobStatus::Failed,
                    Some(CANCELLED_UNEXPECTED_MESSAGE.to_owned()),
                )
            }
            (QueryJobOutcome::Failed { error_message }, from) if !from.is_terminal() => {
                (to, Some(error_message))
            }
            (_, from) => return Err(Error::InvalidQueryJobStatusTransition { from, to }),
        };

        sqlx::query(UPDATE_QUERY)
            .bind(status_to_persist)
            .bind(status_message)
            .bind(self.query_job_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        Ok(())
    }

    /// Reads the query job's current status, locking its row until `tx` ends.
    ///
    /// # Returns
    ///
    /// The query job's current status on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::QueryJobMetadataCorrupted`] if the query job's row no longer exists.
    /// * Forwards [`sqlx::query::QueryScalar::fetch_optional`]'s return values on failure.
    async fn lock_and_get_status(
        &self,
        tx: &mut Transaction<'_, MySql>,
    ) -> Result<QueryJobStatus, Error> {
        const SELECT_QUERY: &str =
            formatcp!("SELECT `status` FROM `{QUERY_JOBS_TABLE_NAME}` WHERE `id` = ? FOR UPDATE");

        sqlx::query_scalar::<_, QueryJobStatus>(SELECT_QUERY)
            .bind(self.query_job_id)
            .fetch_optional(&mut **tx)
            .await?
            .ok_or(Error::QueryJobMetadataCorrupted(self.query_job_id))
    }
}

impl From<&QueryJobOutcome> for QueryJobStatus {
    fn from(outcome: &QueryJobOutcome) -> Self {
        match outcome {
            QueryJobOutcome::Succeeded => Self::Succeeded,
            QueryJobOutcome::Failed { .. } => Self::Failed,
            QueryJobOutcome::Cancelled => Self::Cancelled,
        }
    }
}

/// Selects the archives a query job should search, and derives each one's task execution policy.
///
/// # Lifetimes
///
/// * `'job_handle_lifetime` - The lifetime of the [`QueryJobHandle`] state borrowed by the
///   selector.
struct ArchiveSelector<'job_handle_lifetime> {
    context: &'job_handle_lifetime QueryJobHandleContext,
    query_job_config: &'job_handle_lifetime QueryJobConfig,
    job_creation_timestamp_millisecs: i64,
}

impl ArchiveSelector<'_> {
    /// Prepares the task inputs for the query job.
    ///
    /// This method validates and deduplicates the job's requested datasets, then retrieves from the
    /// CLP database the metadata of the archives matching the query, ordered by descending archive
    /// end timestamp, and attaches an execution policy derived from each archive's uncompressed
    /// size to its task. Archives created before each dataset's retention period, measured back
    /// from the job's creation, are excluded.
    ///
    /// # Returns
    ///
    /// A vector of tuples on success, where each tuple contains:
    ///
    /// * The [`ArchiveMetadata`] identifying the archive searched by a single query task.
    /// * The [`ExecutionPolicy`] for that task.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::InvalidQueryJobConfig`] if any requested dataset doesn't exist or is marked as
    ///   deleted.
    /// * Forwards [`Self::deduplicate_and_validate_requested_datasets`]'s return values on failure.
    /// * Forwards [`sqlx::Pool::acquire`]'s return values on failure.
    /// * Forwards [`MetadataTables::get_dataset_ids_by_names`]'s return values on failure.
    /// * Forwards [`MetadataTables::find_archives_for_query`]'s return values on failure.
    async fn select(&self) -> Result<Vec<(ArchiveMetadata, ExecutionPolicy)>, Error> {
        let datasets: Vec<DatasetName> = self
            .deduplicate_and_validate_requested_datasets()?
            .into_iter()
            .collect();

        let metadata_tables = &self.context.metadata_tables;
        let mut conn = self.context.db_pool.acquire().await?;
        let dataset_ids = metadata_tables
            .get_dataset_ids_by_names(&mut conn, &datasets)
            .await?;
        if dataset_ids.len() != datasets.len() {
            return Err(Error::InvalidQueryJobConfig(
                "one or more requested datasets don't exist".to_owned(),
            ));
        }

        let time_range = self.query_job_config.query.time_range;
        let filter = QueryArchiveFilter {
            begin_timestamp_millisecs: time_range.begin_timestamp_millisecs(),
            end_timestamp_millisecs: time_range.end_timestamp_millisecs(),
            job_creation_timestamp_millisecs: self.job_creation_timestamp_millisecs,
        };
        let mut selected_archives = Vec::new();
        for (dataset, dataset_id) in dataset_ids {
            selected_archives.extend(
                metadata_tables
                    .find_archives_for_query(&mut conn, dataset_id, &filter)
                    .await?
                    .into_iter()
                    .map(|archive| ArchiveMetadata {
                        id: archive.uuid,
                        dataset: dataset.clone(),
                        uncompressed_size: archive.num_uncompressed_bytes,
                        end_timestamp: archive.end_timestamp_millisecs,
                    }),
            );
        }
        selected_archives.sort_by_key(|archive| Reverse(archive.end_timestamp));

        Ok(selected_archives
            .into_iter()
            .map(|archive| {
                let execution_policy =
                    self.compute_query_task_execution_policy(archive.uncompressed_size);
                (archive, execution_policy)
            })
            .collect())
    }

    /// Validates and deduplicates the datasets requested by a query job.
    ///
    /// # Returns
    ///
    /// The distinct requested datasets on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::InvalidQueryJobConfig`] if:
    ///   * The job's requested datasets are empty.
    ///   * The number of distinct datasets exceeds
    ///     [`ArchiveSelectionOptions::max_datasets_per_query`].
    fn deduplicate_and_validate_requested_datasets(&self) -> Result<HashSet<DatasetName>, Error> {
        let requested_datasets = &self.query_job_config.datasets;
        if requested_datasets.is_empty() {
            return Err(Error::InvalidQueryJobConfig(
                "the datasets list must not be empty".to_owned(),
            ));
        }

        let datasets: HashSet<DatasetName> = requested_datasets.iter().cloned().collect();

        if let Some(max_datasets_per_query) = self
            .context
            .archive_selection_options
            .max_datasets_per_query
            && datasets.len() > max_datasets_per_query.get()
        {
            return Err(Error::InvalidQueryJobConfig(format!(
                "the number of requested datasets ({}) exceeds `max_datasets_per_query` \
                 ({max_datasets_per_query})",
                datasets.len()
            )));
        }

        Ok(datasets)
    }

    /// Derives the execution policy of the query task that searches an archive whose uncompressed
    /// size is `archive_uncompressed_size` bytes.
    ///
    /// The soft timeout is an aggressive estimate of the task's execution time, whereas the hard
    /// timeout is a conservative deadline that assumes the entire archive is extracted.
    ///
    /// # Returns
    ///
    /// The query task's execution policy.
    fn compute_query_task_execution_policy(
        &self,
        archive_uncompressed_size: u64,
    ) -> ExecutionPolicy {
        const BYTES_PER_MIB: u64 = 1024 * 1024;
        const SOFT_TIMEOUT_MILLISECS_PER_MIB: u64 = 10;
        const HARD_TIMEOUT_MILLISECS_PER_MIB: u64 = 2 * 1000;

        /// The minimum timeout accepted by Spider, in milliseconds.
        const MIN_TIMEOUT_MILLISECS: u64 = 100;

        /// The maximum timeout accepted by Spider, in milliseconds (24 hours).
        const MAX_TIMEOUT_MILLISECS: u64 = 1000 * 60 * 60 * 24;

        let hard_timeout_ms = (archive_uncompressed_size
            .saturating_mul(HARD_TIMEOUT_MILLISECS_PER_MIB)
            / BYTES_PER_MIB)
            .clamp(MIN_TIMEOUT_MILLISECS + 1, MAX_TIMEOUT_MILLISECS);
        let soft_timeout_ms = (archive_uncompressed_size
            .saturating_mul(SOFT_TIMEOUT_MILLISECS_PER_MIB)
            / BYTES_PER_MIB)
            .clamp(MIN_TIMEOUT_MILLISECS, hard_timeout_ms - 1);

        ExecutionPolicy {
            max_num_retry: self.context.spider_option.query_task_max_retry,
            timeout_policy: TimeoutPolicy {
                soft_timeout_ms,
                hard_timeout_ms,
            },
            ..ExecutionPolicy::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::num::NonZeroU16;
    use std::num::NonZeroU32;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use clp_metadata_db::DatasetId;
    use clp_metadata_db::MetadataTables;
    use clp_metadata_db::NewArchive;
    use clp_metadata_db::NewDataset;
    use clp_rust_utils::clp_config::package::config::Database;
    use clp_rust_utils::dataset::DatasetName;
    use clp_rust_utils::job_config::Aggregation;
    use clp_rust_utils::job_config::NetworkOutput;
    use clp_rust_utils::job_config::QueryJobConfig;
    use clp_rust_utils::job_config::QueryJobId;
    use clp_rust_utils::job_config::QueryOutput;
    use clp_rust_utils::job_config::ResultsCacheOutput;
    use clp_rust_utils::job_config::SearchQuery;
    use clp_rust_utils::job_config::TimeRange;
    use clp_rust_utils::types::ArchiveId;
    use clp_rust_utils::types::PositiveI64;
    use clp_rust_utils::types::SessionToken;
    use clp_rust_utils::types::non_empty_string::ExpectedNonEmpty;
    use non_empty_string::NonEmptyString;
    use spider_core::task::ExecutionPolicy;
    use spider_core::types::id::JobId as SpiderJobId;
    use spider_core::types::id::ResourceGroupId;
    use sqlx::MySqlPool;
    use sqlx::mysql::MySqlPoolOptions;

    use super::ArchiveSelectionOptions;
    use super::ArchiveSelector;
    use super::QueryJobHandle;
    use super::QueryJobHandleContext;
    use super::SpiderOption;
    use crate::Error;
    use crate::query_job_submitter::ArchiveMetadata;
    use crate::query_job_submitter::QueryJobOutcome;
    use crate::query_job_submitter::QueryJobSubmitter;

    /// A submitter for tests that never reach Spider.
    #[derive(Clone)]
    struct UnreachableSubmitter;

    #[async_trait]
    impl QueryJobSubmitter for UnreachableSubmitter {
        async fn submit_query_job(
            &self,
            _query_job_id: QueryJobId,
            _resource_group_id: ResourceGroupId,
            _query: SearchQuery,
            _output: QueryOutput,
            _archives_to_search: Vec<(ArchiveMetadata, ExecutionPolicy)>,
        ) -> Result<SpiderJobId, Error> {
            unreachable!("the test doesn't submit query jobs")
        }

        async fn run_query_job_to_completion(
            &self,
            _spider_job_id: SpiderJobId,
            _poll_interval: Duration,
        ) -> Result<QueryJobOutcome, Error> {
            unreachable!("the test doesn't run query jobs")
        }

        async fn cancel_query_job(&self, _spider_job_id: SpiderJobId) -> Result<(), Error> {
            unreachable!("the test doesn't cancel query jobs")
        }
    }

    /// A uniquely named database on the test server, used by a single test, with the shared
    /// metadata tables.
    struct TestDatabase {
        pool: MySqlPool,
        metadata_tables: MetadataTables,
        name: String,
    }

    impl TestDatabase {
        /// Factory function.
        ///
        /// Creates a uniquely named database on the server at `CLP_METADATA_DB_TEST_URL` (e.g.
        /// `mysql://root:password@127.0.0.1:3306`), connects to it, and creates the shared
        /// metadata tables.
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
            let name = format!("query_coordinator_test_{}", uuid::Uuid::new_v4().simple());
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
            Self {
                pool,
                metadata_tables,
                name,
            }
        }

        /// Creates a dataset.
        ///
        /// # Returns
        ///
        /// The created dataset's ID.
        ///
        /// # Panics
        ///
        /// Panics if the dataset can't be created.
        async fn create_dataset(
            &self,
            name: &str,
            retention_period_minutes: Option<NonZeroU32>,
        ) -> DatasetId {
            self.metadata_tables
                .create_dataset(
                    &mut self
                        .pool
                        .acquire()
                        .await
                        .expect("failed to get a connection"),
                    &NewDataset {
                        name: name.parse().expect("test dataset name should be valid"),
                        archive_storage_path: format!("/var/data/archives/{name}"),
                        retention_period_minutes,
                    },
                )
                .await
                .expect("failed to create the dataset")
        }

        /// Inserts an archive into a dataset for each `(begin timestamp, end timestamp,
        /// uncompressed size)` in `archives`.
        ///
        /// # Returns
        ///
        /// The inserted archives' IDs, in the order of `archives`.
        ///
        /// # Panics
        ///
        /// Panics if the archives can't be inserted.
        async fn insert_archives<const NUM_ARCHIVES: usize>(
            &self,
            dataset_id: DatasetId,
            archives: [(i64, i64, u64); NUM_ARCHIVES],
        ) -> [ArchiveId; NUM_ARCHIVES] {
            let archives = archives.map(
                |(begin_timestamp_millisecs, end_timestamp_millisecs, num_uncompressed_bytes)| {
                    NewArchive {
                        uuid: ArchiveId::from_bytes(*uuid::Uuid::new_v4().as_bytes()),
                        begin_timestamp_millisecs,
                        end_timestamp_millisecs,
                        num_uncompressed_bytes,
                        num_compressed_bytes: 1,
                    }
                },
            );
            self.metadata_tables
                .insert_archives(
                    &mut self
                        .pool
                        .acquire()
                        .await
                        .expect("failed to get a connection"),
                    dataset_id,
                    1,
                    &archives,
                )
                .await
                .expect("failed to insert the archives");
            archives.map(|archive| archive.uuid)
        }

        /// # Returns
        ///
        /// The test database's current time, in milliseconds since the Unix epoch.
        ///
        /// # Panics
        ///
        /// Panics if the time can't be read.
        async fn now_millisecs(&self) -> i64 {
            sqlx::query_scalar("SELECT CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS SIGNED)")
                .fetch_one(&self.pool)
                .await
                .expect("failed to read the current time")
        }

        /// Plans a query job that searches `datasets` over `time_range`, created at
        /// `job_creation_timestamp_millisecs`.
        ///
        /// # Returns
        ///
        /// The archives selected for the job's tasks on success.
        ///
        /// # Errors
        ///
        /// Returns an error if:
        ///
        /// * Forwards [`QueryJobHandle::plan`]'s return values on failure.
        ///
        /// # Panics
        ///
        /// Panics if the job handle can't be created.
        async fn plan(
            &self,
            datasets: &[&str],
            time_range: TimeRange,
            job_creation_timestamp_millisecs: i64,
        ) -> Result<Vec<ArchiveMetadata>, Error> {
            let base_query_job_config = base_query_job_config();
            let job_handle = QueryJobHandle::new(
                Arc::new(QueryJobHandleContext {
                    db_pool: self.pool.clone(),
                    ..context(None)
                }),
                1,
                UnreachableSubmitter,
                ResourceGroupId::random(),
                QueryJobConfig {
                    datasets: datasets
                        .iter()
                        .map(|dataset| dataset.parse().expect("test dataset name should be valid"))
                        .collect(),
                    query: SearchQuery {
                        time_range,
                        ..base_query_job_config.query
                    },
                    ..base_query_job_config
                },
                job_creation_timestamp_millisecs,
            )
            .expect("the test job's config should be supported");
            Ok(job_handle
                .plan()
                .await?
                .into_iter()
                .map(|(archive, _)| archive)
                .collect())
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

    #[tokio::test]
    async fn query_job_handle_rejects_aggregation_and_file_outputs() {
        let context = Arc::new(context(None));
        let results_cache_output = ResultsCacheOutput {
            max_num_results: NonZeroU32::new(7),
            aggregation: None,
        };
        let new_job_handle = |output| {
            QueryJobHandle::new(
                context.clone(),
                1,
                UnreachableSubmitter,
                ResourceGroupId::random(),
                QueryJobConfig {
                    output,
                    ..base_query_job_config()
                },
                0,
            )
        };

        new_job_handle(QueryOutput::ResultsCache(results_cache_output.clone()))
            .expect("a results cache output without aggregation should be supported");
        new_job_handle(QueryOutput::Network(NetworkOutput {
            host: NonEmptyString::from_static_str("10.0.0.5"),
            port: NonZeroU16::new(3003).expect("3003 is nonzero"),
            session_token: SessionToken::from(uuid::Uuid::nil()),
        }))
        .expect("a network output should be supported");

        for (output, expected_message) in [
            (
                QueryOutput::ResultsCache(ResultsCacheOutput {
                    aggregation: Some(Aggregation::CountByTime {
                        bucket_size_millisecs: PositiveI64::new(60_000)
                            .expect("60,000 is positive"),
                    }),
                    ..results_cache_output
                }),
                "aggregation",
            ),
            (QueryOutput::File {}, "files"),
        ] {
            let Err(error) = new_job_handle(output.clone()) else {
                panic!("output should be unsupported: {output:?}");
            };
            assert!(
                matches!(
                    &error,
                    Error::UnsupportedJobConfig(message) if message.contains(expected_message)
                ),
                "unexpected error for {output:?}: {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn duplicate_requested_datasets_count_once_toward_max_datasets_per_query() {
        let context = context(NonZeroUsize::new(1));
        let other_dataset: DatasetName = "other"
            .parse()
            .expect("name should be a valid dataset name");
        let select_datasets = |datasets| {
            let query_job_config = QueryJobConfig {
                datasets,
                ..base_query_job_config()
            };
            ArchiveSelector {
                context: &context,
                query_job_config: &query_job_config,
                job_creation_timestamp_millisecs: 0,
            }
            .deduplicate_and_validate_requested_datasets()
        };

        assert_eq!(
            select_datasets(vec![DatasetName::default(), DatasetName::default()])
                .expect("duplicate datasets should count once"),
            HashSet::from([DatasetName::default()])
        );
        let error = select_datasets(vec![DatasetName::default(), other_dataset])
            .expect_err("two distinct datasets should exceed the limit");
        assert!(
            matches!(
                &error,
                Error::InvalidQueryJobConfig(message) if message.contains("max_datasets_per_query")
            ),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    #[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
    async fn plan_rejects_jobs_whose_datasets_do_not_all_exist() -> anyhow::Result<()> {
        let test_db = TestDatabase::create().await;
        test_db.create_dataset("logs", None).await;

        let error = test_db
            .plan(&["logs", "missing"], TimeRange::default(), 0)
            .await
            .expect_err("a job with a missing dataset should be rejected");
        assert!(
            matches!(
                &error,
                Error::InvalidQueryJobConfig(message) if message.contains("don't exist")
            ),
            "unexpected error: {error:?}"
        );

        test_db.drop_database().await;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
    async fn plan_selects_the_requested_datasets_archives_within_the_time_range_and_retention()
    -> anyhow::Result<()> {
        let test_db = TestDatabase::create().await;
        let logs_id = test_db.create_dataset("logs", None).await;
        let metrics_id = test_db.create_dataset("metrics", NonZeroU32::new(1)).await;
        let other_id = test_db.create_dataset("other", None).await;
        let [_, in_range, late_in_range, _] = test_db
            .insert_archives(
                logs_id,
                [(10, 100, 1), (100, 200, 2), (300, 400, 3), (1000, 2000, 4)],
            )
            .await;
        let [metrics_in_range] = test_db.insert_archives(metrics_id, [(150, 250, 5)]).await;
        test_db.insert_archives(other_id, [(100, 200, 6)]).await;

        let time_range = TimeRange::new(Some(150), Some(500))?;
        let now = test_db.now_millisecs().await;
        let archive = |id, dataset: &str, uncompressed_size, end_timestamp| ArchiveMetadata {
            id,
            dataset: dataset.parse().expect("test dataset name should be valid"),
            uncompressed_size,
            end_timestamp,
        };
        assert_eq!(
            test_db.plan(&["logs", "metrics"], time_range, now).await?,
            vec![
                archive(late_in_range, "logs", 3, 400),
                archive(metrics_in_range, "metrics", 5, 250),
                archive(in_range, "logs", 2, 200),
            ]
        );
        assert_eq!(
            test_db
                .plan(&["logs", "metrics"], time_range, now + 60 * 60 * 1000)
                .await?,
            vec![
                archive(late_in_range, "logs", 3, 400),
                archive(in_range, "logs", 2, 200),
            ]
        );

        test_db.drop_database().await;
        Ok(())
    }

    /// # Returns
    ///
    /// A query job handle context whose database pool connects lazily, so tests that don't query
    /// the database need no database.
    ///
    /// # Panics
    ///
    /// Panics if the database URL is invalid.
    fn context(max_datasets_per_query: Option<NonZeroUsize>) -> QueryJobHandleContext {
        QueryJobHandleContext {
            db_pool: MySqlPoolOptions::new()
                .connect_lazy("mysql://localhost/clp-db")
                .expect("the database URL should be valid"),
            metadata_tables: MetadataTables::new(&Database::default()),
            archive_selection_options: ArchiveSelectionOptions {
                max_datasets_per_query,
            },
            spider_option: SpiderOption {
                poll_interval: Duration::from_secs(1),
                query_task_max_retry: 0,
            },
        }
    }

    /// # Returns
    ///
    /// A query job config that searches the default dataset and buffers its results in the
    /// results cache.
    fn base_query_job_config() -> QueryJobConfig {
        QueryJobConfig {
            datasets: vec![DatasetName::default()],
            query: SearchQuery {
                query_string: NonEmptyString::from_static_str("*"),
                time_range: TimeRange::default(),
                ignore_case: false,
            },
            output: QueryOutput::ResultsCache(ResultsCacheOutput {
                max_num_results: None,
                aggregation: None,
            }),
        }
    }
}
