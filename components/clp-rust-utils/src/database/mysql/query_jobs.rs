//! Operations on query jobs in the CLP DB's query jobs table.

use sqlx::Row;

use crate::job_config::QUERY_JOBS_TABLE_NAME;
use crate::job_config::QueryJobStatus;
use crate::job_config::QueryJobType;
use crate::job_config::SearchJobConfig;

/// The cancellability of a query job, derived from its status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cancellability {
    /// The job is pending or running, so its cancellation can be requested.
    Cancellable,

    /// The job's cancellation has already been requested, or the job has already been cancelled.
    CancellationRequested,

    /// The job has terminated without being cancelled.
    NotCancellable,
}

impl Cancellability {
    /// # Returns
    ///
    /// The cancellability of a query job in the given status.
    #[must_use]
    pub const fn from_status(status: QueryJobStatus) -> Self {
        match status {
            QueryJobStatus::Pending | QueryJobStatus::Running => Self::Cancellable,
            QueryJobStatus::CancelRequested
            | QueryJobStatus::Cancelling
            | QueryJobStatus::Cancelled => Self::CancellationRequested,
            QueryJobStatus::Succeeded | QueryJobStatus::Failed | QueryJobStatus::Killed => {
                Self::NotCancellable
            }
        }
    }
}

/// Inserts a search or aggregation query job into the query jobs table.
///
/// # Returns
///
/// The unique ID of the newly created query job on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`rmp_serde::to_vec_named`]'s return values on failure.
/// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
pub async fn insert_query_job(
    db_pool: &sqlx::MySqlPool,
    query_job_config: &SearchJobConfig,
) -> Result<u64, crate::Error> {
    let query_job_type_i32: i32 = QueryJobType::SearchOrAggregation.into();
    let query_result = sqlx::query(&format!(
        "INSERT INTO `{QUERY_JOBS_TABLE_NAME}` (`job_config`, `type`) VALUES (?, ?)"
    ))
    .bind(rmp_serde::to_vec_named(query_job_config)?)
    .bind(query_job_type_i32)
    .execute(db_pool)
    .await?;

    let query_job_id = query_result.last_insert_id();
    Ok(query_job_id)
}

/// Requests the cancellation of a query job by setting its status to
/// [`QueryJobStatus::CancelRequested`], if the job is pending or running.
///
/// This function is idempotent: if a cancellation has already been requested for the job, it's a
/// no-op that returns `Ok(())`.
///
/// # Returns
///
/// `Ok(())` on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`crate::Error::QueryJobNotFound`] if the job doesn't exist.
/// * [`crate::Error::QueryJobAlreadyTerminated`] if the job has already succeeded, failed, or been
///   killed.
/// * Forwards [`sqlx::Pool::begin`]'s return values on failure.
/// * Forwards [`sqlx::query::QueryScalar::fetch_optional`]'s return values on failure.
/// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
/// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
pub async fn request_query_job_cancellation(
    db_pool: &sqlx::MySqlPool,
    query_job_id: u64,
) -> Result<(), crate::Error> {
    let mut tx = db_pool.begin().await?;
    let Some(status) = sqlx::query_scalar::<_, QueryJobStatus>(&format!(
        "SELECT status FROM `{QUERY_JOBS_TABLE_NAME}` WHERE id = ? FOR UPDATE"
    ))
    .bind(query_job_id)
    .fetch_optional(&mut *tx)
    .await?
    else {
        return Err(crate::Error::QueryJobNotFound(query_job_id));
    };

    match Cancellability::from_status(status) {
        Cancellability::Cancellable => {
            sqlx::query(&format!(
                "UPDATE `{QUERY_JOBS_TABLE_NAME}` SET status = ? WHERE id = ?"
            ))
            .bind(QueryJobStatus::CancelRequested)
            .bind(query_job_id)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(())
        }
        Cancellability::CancellationRequested => Ok(()),
        Cancellability::NotCancellable => {
            Err(crate::Error::QueryJobAlreadyTerminated(query_job_id))
        }
    }
}

/// Reads the status of a query job.
///
/// # Returns
///
/// The current status of the job on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`crate::Error::InvalidQueryJobStatus`] if the stored status isn't a valid [`QueryJobStatus`].
/// * Forwards [`sqlx::query::Query::fetch_one`]'s return values on failure.
/// * Forwards [`sqlx::Row::try_get`]'s return values on failure.
pub async fn get_query_job_status(
    db_pool: &sqlx::MySqlPool,
    query_job_id: u64,
) -> Result<QueryJobStatus, crate::Error> {
    let row = sqlx::query(&format!(
        "SELECT status FROM `{QUERY_JOBS_TABLE_NAME}` WHERE id = ?"
    ))
    .bind(query_job_id)
    .fetch_one(db_pool)
    .await?;
    let status: i32 = row.try_get("status")?;
    Ok(QueryJobStatus::try_from(status)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellability_from_status() {
        let expected_cancellabilities = [
            (QueryJobStatus::Pending, Cancellability::Cancellable),
            (QueryJobStatus::Running, Cancellability::Cancellable),
            (QueryJobStatus::Succeeded, Cancellability::NotCancellable),
            (QueryJobStatus::Failed, Cancellability::NotCancellable),
            (
                QueryJobStatus::CancelRequested,
                Cancellability::CancellationRequested,
            ),
            (
                QueryJobStatus::Cancelling,
                Cancellability::CancellationRequested,
            ),
            (
                QueryJobStatus::Cancelled,
                Cancellability::CancellationRequested,
            ),
            (QueryJobStatus::Killed, Cancellability::NotCancellable),
        ];
        for (status, expected) in expected_cancellabilities {
            assert_eq!(
                Cancellability::from_status(status),
                expected,
                "unexpected cancellability for {status:?}"
            );
        }
    }
}
