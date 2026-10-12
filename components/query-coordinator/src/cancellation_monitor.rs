//! The monitor that relays query job cancellations requested through the API server to Spider.

use std::time::Duration;

use clp_rust_utils::job_config::QUERY_JOBS_TABLE_NAME;
use clp_rust_utils::job_config::QueryJobId;
use clp_rust_utils::job_config::QueryJobStatus;
use clp_rust_utils::job_config::QueryJobType;
use const_format::formatcp;
use spider_core::types::id::JobId as SpiderJobId;
use sqlx::MySqlPool;

use crate::Error;
use crate::query_job_submitter::QueryJobSubmitter;

/// Relays the cancellation of query jobs in [`QueryJobStatus::CancelRequested`] to Spider.
///
/// # Type Parameters
///
/// * `SubmitterType` - The type of the job submitter used to cancel Spider jobs.
pub struct CancellationMonitor<SubmitterType: QueryJobSubmitter> {
    db_pool: MySqlPool,
    job_submitter: SubmitterType,
    polling_interval: Duration,
}

impl<SubmitterType: QueryJobSubmitter> CancellationMonitor<SubmitterType> {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created [`CancellationMonitor`] that polls for cancellation requests every
    /// `polling_interval` while it has none to relay.
    pub const fn new(
        db_pool: MySqlPool,
        job_submitter: SubmitterType,
        polling_interval: Duration,
    ) -> Self {
        Self {
            db_pool,
            job_submitter,
            polling_interval,
        }
    }

    /// Fetches the oldest query job in [`QueryJobStatus::CancelRequested`].
    ///
    /// This is a non-locking read used only to discover a candidate; every status transition
    /// re-reads the job's status under its row lock.
    ///
    /// # Returns
    ///
    /// The oldest job whose cancellation was requested, or `None` if there is none, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryAs::fetch_optional`]'s return values on failure.
    async fn fetch_next_cancel_requested_job(
        &self,
    ) -> Result<Option<CancelRequestedJobRowProjection>, Error> {
        const QUERY: &str = formatcp!(
            "SELECT `id`, `spider_id` FROM `{QUERY_JOBS_TABLE_NAME}` WHERE `type` = ? AND \
             `status` = ? ORDER BY `id` ASC LIMIT 1"
        );

        Ok(sqlx::query_as::<_, CancelRequestedJobRowProjection>(QUERY)
            .bind(QueryJobType::SearchOrAggregation)
            .bind(QueryJobStatus::CancelRequested)
            .fetch_optional(&self.db_pool)
            .await?)
    }

    /// Transitions the query job identified by `query_job_id` from
    /// [`QueryJobStatus::CancelRequested`] to `to`, in a transaction that locks the job's row.
    ///
    /// If the job is no longer in [`QueryJobStatus::CancelRequested`], for example because its job
    /// handle has already terminated it, nothing is persisted.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::QueryJobMetadataCorrupted`] if the query job's row no longer exists.
    /// * Forwards [`sqlx::Pool::begin`]'s return values on failure.
    /// * Forwards [`sqlx::query::QueryScalar::fetch_optional`]'s return values on failure.
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    /// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
    async fn transition_from_cancel_requested(
        &self,
        query_job_id: QueryJobId,
        to: QueryJobStatus,
        status_msg: Option<&str>,
    ) -> Result<(), Error> {
        const SELECT_QUERY: &str =
            formatcp!("SELECT `status` FROM `{QUERY_JOBS_TABLE_NAME}` WHERE `id` = ? FOR UPDATE");
        const UPDATE_QUERY: &str = formatcp!(
            "UPDATE `{QUERY_JOBS_TABLE_NAME}` SET `status` = ?, `status_msg` = ? WHERE `id` = ?"
        );

        let mut tx = self.db_pool.begin().await?;
        let observed_status = sqlx::query_scalar::<_, QueryJobStatus>(SELECT_QUERY)
            .bind(query_job_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::QueryJobMetadataCorrupted(query_job_id))?;
        if QueryJobStatus::CancelRequested != observed_status {
            tracing::info!(
                query_job_id = % query_job_id,
                observed_status = ? observed_status,
                to = ? to,
                "Skipping the cancellation transition: the query job is no longer in \
                 `CancelRequested`.",
            );
            return Ok(());
        }

        sqlx::query(UPDATE_QUERY)
            .bind(to)
            .bind(status_msg)
            .bind(query_job_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        Ok(())
    }
}

/// A projection of the columns read from a [`QueryJobStatus::CancelRequested`] query job row.
#[derive(Debug, sqlx::FromRow)]
struct CancelRequestedJobRowProjection {
    id: QueryJobId,
    #[sqlx(rename = "spider_id")]
    spider_job_id: Option<SpiderJobId>,
}
