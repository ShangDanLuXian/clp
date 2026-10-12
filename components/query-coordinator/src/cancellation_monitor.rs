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
}

/// A projection of the columns read from a [`QueryJobStatus::CancelRequested`] query job row.
#[derive(Debug, sqlx::FromRow)]
struct CancelRequestedJobRowProjection {
    id: QueryJobId,
    #[sqlx(rename = "spider_id")]
    spider_job_id: Option<SpiderJobId>,
}
