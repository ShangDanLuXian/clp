//! Streamed queries, each of which submits a query job whose tasks stream their results to the API
//! server, and streams the results back while the job runs.

use std::sync::Arc;
use std::time::Duration;

use async_stream::stream;
use async_trait::async_trait;
use clp_rust_utils::job_config::QueryJobConfig;
use clp_rust_utils::job_config::QueryJobStatus;
use futures::Stream;
use futures::StreamExt;
use query_result_listener::encoding::ResultEncoding;
use query_result_listener::listener::ResultListener;
use query_result_listener::session::ResultStream;
use query_result_listener::session::Session;
use query_result_listener::session::SessionConfig;
use query_result_listener::session::SessionStats;

use crate::client::ClientError;
use crate::client::QueryConfig;

/// The operations that a streamed query performs on the query jobs table.
#[async_trait]
pub trait QueryJobTable: Send + Sync + 'static {
    /// Submits a query job.
    ///
    /// # Parameters
    ///
    /// * `query_job_config` - The config of the query job.
    ///
    /// # Returns
    ///
    /// The ID of the submitted query job on success.
    ///
    /// # Errors
    ///
    /// Implementations must document their error conditions.
    async fn submit(&self, query_job_config: &QueryJobConfig) -> Result<u64, ClientError>;

    /// Reads the status of a query job.
    ///
    /// # Parameters
    ///
    /// * `query_job_id` - The ID of the query job.
    ///
    /// # Returns
    ///
    /// The current status of the query job on success, or `None` if the query job doesn't exist.
    ///
    /// # Errors
    ///
    /// Implementations must document their error conditions.
    async fn read_status(&self, query_job_id: u64) -> Result<Option<QueryJobStatus>, ClientError>;

    /// Requests the cancellation of a query job.
    ///
    /// # Parameters
    ///
    /// * `query_job_id` - The ID of the query job.
    ///
    /// # Errors
    ///
    /// Implementations must document their error conditions.
    async fn request_cancellation(&self, query_job_id: u64) -> Result<(), ClientError>;
}

/// Runs streamed queries, receiving the results of every query through one listener.
///
/// # Type Parameters
///
/// * `QueryJobTableType` - The query jobs table that the queries' jobs are submitted to.
/// * `EncodingType` - The encoding of the queries' results.
pub struct QueryStreamer<QueryJobTableType: QueryJobTable, EncodingType: ResultEncoding> {
    listener: ResultListener<EncodingType>,
    query_job_table: Arc<QueryJobTableType>,
    session_config: SessionConfig,
}

impl<QueryJobTableType: QueryJobTable, EncodingType: ResultEncoding>
    QueryStreamer<QueryJobTableType, EncodingType>
{
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created streamer that receives the queries' results through `listener`, in sessions
    /// configured by `session_config`, and submits the queries' jobs to `query_job_table`.
    #[must_use]
    pub fn new(
        listener: ResultListener<EncodingType>,
        query_job_table: QueryJobTableType,
        session_config: SessionConfig,
    ) -> Self {
        Self {
            listener,
            query_job_table: Arc::new(query_job_table),
            session_config,
        }
    }

    /// Submits a query job whose tasks stream their results to the streamer's listener.
    ///
    /// The stream of the returned query's events yields the batches of the query's results as they
    /// arrive, while it reads the job's status every [`QUERY_JOB_STATUS_POLL_INTERVAL`]. Once the
    /// job has terminated, the stream ends the query's session, yields the batches that the
    /// session's connections had already claimed until the session's result stream ends, and then
    /// yields a [`QueryEvent::End`]. Instead, if the session fails, the job's status can't be read,
    /// or the job doesn't exist, the stream requests the job's cancellation and yields a
    /// [`QueryEvent::Error`].
    ///
    /// Dropping the stream ends the query's session. If the stream hasn't yet found the job
    /// terminated or yielded an error, dropping it also requests the job's cancellation. Dropping
    /// this future once it has been polled doesn't stop the job's submission: the submitted job's
    /// cancellation is requested instead.
    ///
    /// # Returns
    ///
    /// The submitted query on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::TaskJoin`] if the task that submits the query job panics.
    /// * Forwards [`ResultListener::open_session`]'s return values on failure.
    /// * Forwards [`QueryConfig::into_streamed_query_job_config`]'s return values on failure.
    /// * Forwards [`QueryJobTable::submit`]'s return values on failure.
    pub async fn submit(
        &self,
        query_config: QueryConfig,
    ) -> Result<
        StreamedQuery<
            impl Stream<Item = QueryEvent<EncodingType::Batch>>
            + Send
            + use<QueryJobTableType, EncodingType>,
        >,
        ClientError,
    > {
        let (session, results) = self.listener.open_session(self.session_config)?;
        let query_job_config =
            query_config.into_streamed_query_job_config(session.network_output().clone())?;
        let query_job_table = Arc::clone(&self.query_job_table);
        // Submits from a task, so that if this future is dropped mid-submission, the task's output
        // is dropped instead, which requests the submitted job's cancellation.
        tokio::spawn(async move {
            let query_job_id = query_job_table.submit(&query_job_config).await?;
            Ok(StreamedQuery {
                query_job_id,
                events: QueryJob::new(query_job_id, query_job_table).into_events(session, results),
            })
        })
        .await?
    }
}

/// A streamed query whose job has been submitted.
///
/// # Type Parameters
///
/// * `EventStreamType` - The type of the stream of the query's events.
pub struct StreamedQuery<EventStreamType> {
    /// The ID of the query's job.
    pub query_job_id: u64,

    /// The stream of the query's events, as [`QueryStreamer::submit`] describes.
    pub events: EventStreamType,
}

/// An item of a streamed query's stream.
///
/// # Type Parameters
///
/// * `BatchType` - The type of a batch of encoded results.
#[derive(Debug)]
pub enum QueryEvent<BatchType> {
    /// A batch of the query's results.
    Results(BatchType),

    /// The end of the query, once its job has terminated and its session's result stream has ended.
    End(QueryEnd),

    /// The end of the query because its session failed, its job's status couldn't be read, or its
    /// job doesn't exist. The job's cancellation has been requested.
    Error(ClientError),
}

/// The end of a streamed query whose job has terminated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryEnd {
    /// The terminal status of the query's job.
    pub status: QueryJobStatus,

    /// The statistics of the query's session.
    pub stats: SessionStats,
}

/// The interval between the end of one read of a streamed query's job status and the start of the
/// next.
const QUERY_JOB_STATUS_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The submitted job of a streamed query, whose cancellation is requested if it's dropped before
/// it's marked as terminated.
///
/// # Type Parameters
///
/// * `QueryJobTableType` - The query jobs table that the job was submitted to.
struct QueryJob<QueryJobTableType: QueryJobTable> {
    id: u64,
    table: Arc<QueryJobTableType>,
    cancel_on_drop: bool,
}

impl<QueryJobTableType: QueryJobTable> QueryJob<QueryJobTableType> {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The job `id` submitted to `table`, whose cancellation is requested if it's dropped.
    const fn new(id: u64, table: Arc<QueryJobTableType>) -> Self {
        Self {
            id,
            table,
            cancel_on_drop: true,
        }
    }

    /// Streams the events of the job's query, as [`QueryStreamer::submit`] describes, from
    /// `session`, which the job's tasks stream their results to, and its `results`.
    ///
    /// # Type Parameters
    ///
    /// * `EncodingType` - The encoding of the query's results.
    ///
    /// # Returns
    ///
    /// The stream of the query's events.
    ///
    /// # Panics
    ///
    /// The returned stream panics if `results` ends while `session` is open, which the listener
    /// never lets it do.
    fn into_events<EncodingType: ResultEncoding>(
        mut self,
        session: Session<EncodingType>,
        mut results: ResultStream<EncodingType::Batch>,
    ) -> impl Stream<Item = QueryEvent<EncodingType::Batch>> + Send {
        stream! {
            let end = {
                let termination = self.wait_for_termination();
                tokio::pin!(termination);
                loop {
                    // `biased`, so that a session failure isn't missed because the job's status was
                    // seen first, and a steady flow of batches doesn't keep the job's status from
                    // being noticed.
                    tokio::select! {
                        biased;
                        error = session.failed() => {
                            break Err(ClientError::QueryResultEncoding(error.to_string()));
                        }
                        status = &mut termination => break status,
                        batch = results.next() => yield QueryEvent::Results(batch.expect(
                            "a session's result stream shouldn't end while the session is open",
                        )),
                    }
                }
            };
            drop(session);
            match end {
                Ok(status) => {
                    self.mark_terminated();
                    while let Some(batch) = results.next().await {
                        yield QueryEvent::Results(batch);
                    }
                    yield QueryEvent::End(QueryEnd {
                        status,
                        stats: results.stats(),
                    });
                }
                Err(error) => {
                    drop(results);
                    tracing::error!(
                        query_job_id = self.id,
                        error = % error,
                        "A streamed query failed; requesting the cancellation of its job."
                    );
                    self.request_cancellation();
                    yield QueryEvent::Error(error);
                }
            }
        }
    }

    /// Reads the job's status every [`QUERY_JOB_STATUS_POLL_INTERVAL`] until it's terminal.
    ///
    /// # Returns
    ///
    /// The job's terminal status on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::QueryJobNotFound`] if the job doesn't exist.
    /// * Forwards [`QueryJobTable::read_status`]'s return values on failure.
    async fn wait_for_termination(&self) -> Result<QueryJobStatus, ClientError> {
        loop {
            tokio::time::sleep(QUERY_JOB_STATUS_POLL_INTERVAL).await;
            let status = self
                .table
                .read_status(self.id)
                .await?
                .ok_or(ClientError::QueryJobNotFound(self.id))?;
            if status.is_terminal() {
                return Ok(status);
            }
        }
    }

    /// Marks the job as terminated, so that dropping it doesn't request its cancellation.
    const fn mark_terminated(&mut self) {
        self.cancel_on_drop = false;
    }

    /// Requests the job's cancellation in a background task, which logs the request's outcome.
    /// Outside a Tokio runtime, only logs that the cancellation can't be requested.
    fn request_cancellation(&mut self) {
        self.cancel_on_drop = false;
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(
                query_job_id = self.id,
                "Can't request the cancellation of a streamed query's job outside a Tokio runtime."
            );
            return;
        };
        let id = self.id;
        let table = Arc::clone(&self.table);
        runtime.spawn(async move {
            match table.request_cancellation(id).await {
                Ok(()) => tracing::info!(
                    query_job_id = id,
                    "Requested the cancellation of a streamed query's job."
                ),
                Err(e) => tracing::warn!(
                    query_job_id = id,
                    error = % e,
                    "Failed to request the cancellation of a streamed query's job."
                ),
            }
        });
    }
}

impl<QueryJobTableType: QueryJobTable> Drop for QueryJob<QueryJobTableType> {
    fn drop(&mut self) {
        if self.cancel_on_drop {
            self.request_cancellation();
        }
    }
}
