//! Streamed queries, each of which submits a query job whose tasks stream their results to the API
//! server, and streams the results back to its caller while the job runs.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::ready;
use std::time::Duration;

use async_trait::async_trait;
use clp_rust_utils::job_config::QueryJobConfig;
use clp_rust_utils::job_config::QueryJobStatus;
use futures::Stream;
use query_result_listener::encoding::ResultEncoding;
use query_result_listener::listener::ResultListener;
use query_result_listener::session::ResultStream;
use query_result_listener::session::Session;
use query_result_listener::session::SessionConfig;
use query_result_listener::session::SessionStats;
use tokio::time::Sleep;

use crate::client::ClientError;
use crate::client::QueryConfig;

/// The interval between the end of one read of a streamed query's job status and the start of the
/// next.
pub const QUERY_JOB_STATUS_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The operations a streamed query performs on the query jobs table.
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
    /// The current status of the query job on success.
    ///
    /// # Errors
    ///
    /// Implementations must return an error if the query job doesn't exist, and document their
    /// other error conditions.
    async fn read_status(&self, query_job_id: u64) -> Result<QueryJobStatus, ClientError>;

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

/// Streams queries' results back to their callers while the queries' jobs run, receiving the
/// results of every query through one listener.
///
/// # Type Parameters
///
/// * `QueryJobTableType` - The query jobs table that the queries' jobs are submitted to.
/// * `EncodingType` - The encoding of the results streamed back.
pub struct QueryStreamer<QueryJobTableType: QueryJobTable, EncodingType: ResultEncoding> {
    listener: ResultListener<EncodingType>,
    query_job_table: Arc<QueryJobTableType>,
}

impl<QueryJobTableType: QueryJobTable, EncodingType: ResultEncoding>
    QueryStreamer<QueryJobTableType, EncodingType>
{
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created streamer that receives the queries' results through `listener`, and submits
    /// their jobs to `query_job_table`.
    #[must_use]
    pub fn new(listener: ResultListener<EncodingType>, query_job_table: QueryJobTableType) -> Self {
        Self {
            listener,
            query_job_table: Arc::new(query_job_table),
        }
    }

    /// Submits a query job whose tasks stream their results to the streamer's listener.
    ///
    /// Dropping the returned future once it has been polled doesn't stop the job's submission:
    /// the submitted job's cancellation is requested instead.
    ///
    /// # Returns
    ///
    /// The stream of the query's events on success.
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
    ) -> Result<QueryStream<QueryJobTableType, EncodingType>, ClientError> {
        let (session, results) = self.listener.open_session(SessionConfig::default())?;
        let query_job_config =
            query_config.into_streamed_query_job_config(session.network_output().clone())?;
        let query_job_table = Arc::clone(&self.query_job_table);
        // Submits from a task, so that a submission whose caller has gone away still ends in a
        // stream, whose drop requests the job's cancellation.
        tokio::spawn(async move {
            let query_job_id = query_job_table.submit(&query_job_config).await?;
            tracing::debug!(
                query_job_id,
                session_token = % session.network_output().session_token,
                "Submitted the job of a streamed query."
            );
            Ok(QueryStream {
                query_job: QueryJob {
                    id: query_job_id,
                    table: query_job_table,
                },
                results,
                state: State::Running {
                    _session: session,
                    status_poll: StatusPoll::new(),
                },
            })
        })
        .await?
    }
}

/// The events of a streamed query: each batch of its results as it arrives, then its end.
///
/// While the query's job runs, the stream reads the job's status every
/// [`QUERY_JOB_STATUS_POLL_INTERVAL`]. Once the job has terminated, or its status can't be read,
/// the stream ends the query's session, forwards the batches that the session's connections had
/// already claimed, and then ends.
///
/// Dropping the stream before its job has terminated requests the job's cancellation.
///
/// # Type Parameters
///
/// * `QueryJobTableType` - The query jobs table that the query's job was submitted to.
/// * `EncodingType` - The encoding of the query's results.
pub struct QueryStream<QueryJobTableType: QueryJobTable, EncodingType: ResultEncoding> {
    query_job: QueryJob<QueryJobTableType>,
    results: ResultStream<EncodingType::Batch>,
    state: State<EncodingType>,
}

impl<QueryJobTableType: QueryJobTable, EncodingType: ResultEncoding>
    QueryStream<QueryJobTableType, EncodingType>
{
    /// # Returns
    ///
    /// The ID of the query's job.
    #[must_use]
    pub const fn query_job_id(&self) -> u64 {
        self.query_job.id
    }
}

impl<QueryJobTableType: QueryJobTable, EncodingType: ResultEncoding> Stream
    for QueryStream<QueryJobTableType, EncodingType>
{
    type Item = QueryEvent<EncodingType::Batch>;

    /// # Panics
    ///
    /// Panics if the query's result stream ends while its session is open, which the listener
    /// never lets it do.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Polls the job's status before the results, so that a steady flow of results can't keep
        // the stream from noticing that the job has terminated.
        if let State::Running { status_poll, .. } = &mut this.state
            && let Poll::Ready(end) = status_poll.poll(cx, &this.query_job)
        {
            match &end {
                Ok(status) => tracing::debug!(
                    query_job_id = this.query_job.id,
                    status = ? status,
                    "The job of a streamed query has terminated; ending its session."
                ),
                Err(e) => {
                    tracing::error!(
                        query_job_id = this.query_job.id,
                        error = % e,
                        "Failed to read the status of a streamed query's job; requesting its \
                         cancellation."
                    );
                    this.query_job.spawn_cancellation();
                }
            }
            this.state = State::Draining { end };
        }
        if matches!(this.state, State::Ended) {
            return Poll::Ready(None);
        }
        if let Some(batch) = ready!(Pin::new(&mut this.results).poll_next(cx)) {
            return Poll::Ready(Some(QueryEvent::Results(batch)));
        }
        let State::Draining { end } = std::mem::replace(&mut this.state, State::Ended) else {
            panic!("a result stream shouldn't end while its session is open");
        };
        Poll::Ready(Some(match end {
            Ok(status) => QueryEvent::End(QueryEnd {
                status,
                stats: this.results.stats(),
            }),
            Err(e) => QueryEvent::Error(e),
        }))
    }
}

impl<QueryJobTableType: QueryJobTable, EncodingType: ResultEncoding> Drop
    for QueryStream<QueryJobTableType, EncodingType>
{
    fn drop(&mut self) {
        if !matches!(self.state, State::Running { .. }) {
            return;
        }
        tracing::info!(
            query_job_id = self.query_job.id,
            "A streamed query was dropped while its job was running; requesting its cancellation."
        );
        self.query_job.spawn_cancellation();
    }
}

/// An event of a streamed query.
///
/// # Type Parameters
///
/// * `BatchType` - The type of a batch of encoded results.
#[derive(Debug)]
pub enum QueryEvent<BatchType> {
    /// A batch of the query's results.
    Results(BatchType),

    /// The end of the query, once its job has terminated and every result its session received
    /// has been streamed.
    End(QueryEnd),

    /// The end of the query because its job's status couldn't be read, once every result its
    /// session received has been streamed. The job's cancellation has been requested.
    Error(ClientError),
}

/// The end of a streamed query whose job has terminated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryEnd {
    /// The terminal status of the query's job.
    pub status: QueryJobStatus,

    /// The final statistics of the query's session.
    pub stats: SessionStats,
}

/// A read of a query job's status.
type StatusRead = Pin<Box<dyn Future<Output = Result<QueryJobStatus, ClientError>> + Send>>;

/// A submitted query job.
///
/// # Type Parameters
///
/// * `QueryJobTableType` - The query jobs table that the job was submitted to.
struct QueryJob<QueryJobTableType: QueryJobTable> {
    id: u64,
    table: Arc<QueryJobTableType>,
}

impl<QueryJobTableType: QueryJobTable> QueryJob<QueryJobTableType> {
    /// # Returns
    ///
    /// A read of the job's status, which doesn't borrow the job.
    fn read_status(&self) -> StatusRead {
        let table = Arc::clone(&self.table);
        let id = self.id;
        Box::pin(async move { table.read_status(id).await })
    }

    /// Requests the job's cancellation in a background task, which logs the request's outcome.
    ///
    /// Outside a Tokio runtime, only logs that the cancellation can't be requested.
    fn spawn_cancellation(&self) {
        let table = Arc::clone(&self.table);
        let id = self.id;
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(
                query_job_id = id,
                "Can't request the cancellation of a streamed query's job outside a runtime."
            );
            return;
        };
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

/// The state of a [`QueryStream`].
///
/// # Type Parameters
///
/// * `EncodingType` - The encoding of the query's results.
enum State<EncodingType: ResultEncoding> {
    /// The query's job is running: its session accepts connections, and its status is polled.
    Running {
        _session: Session<EncodingType>,
        status_poll: StatusPoll,
    },

    /// The query's session has been dropped, because its job has terminated with the status in
    /// `end` or because its status couldn't be read with the error in `end`. The stream forwards
    /// the batches that the session's connections had already claimed.
    Draining {
        end: Result<QueryJobStatus, ClientError>,
    },

    /// The stream has yielded its end.
    Ended,
}

/// The reads of a running query job's status, each starting [`QUERY_JOB_STATUS_POLL_INTERVAL`]
/// after the previous one finished.
struct StatusPoll {
    timer: Pin<Box<Sleep>>,
    read: Option<StatusRead>,
}

impl StatusPoll {
    /// Factory function.
    ///
    /// Starts the interval before the first read.
    ///
    /// # Returns
    ///
    /// The newly created poll.
    fn new() -> Self {
        Self {
            timer: Box::pin(tokio::time::sleep(QUERY_JOB_STATUS_POLL_INTERVAL)),
            read: None,
        }
    }

    /// Drives the reads of `query_job`'s status until one of them ends the poll.
    ///
    /// # Type Parameters
    ///
    /// * `QueryJobTableType` - The query jobs table that the job was submitted to.
    ///
    /// # Returns
    ///
    /// * [`Poll::Ready`] with the job's status once a read finds that it is terminal, or with the
    ///   error of a read that failed.
    /// * [`Poll::Pending`] otherwise, once the timer or the read in flight is registered to wake
    ///   `cx`.
    fn poll<QueryJobTableType: QueryJobTable>(
        &mut self,
        cx: &mut Context<'_>,
        query_job: &QueryJob<QueryJobTableType>,
    ) -> Poll<Result<QueryJobStatus, ClientError>> {
        use tokio::time::Instant;

        loop {
            if let Some(read) = self.read.as_mut() {
                let status = ready!(read.as_mut().poll(cx));
                self.read = None;
                match status {
                    Ok(status) if !status.is_terminal() => self
                        .timer
                        .as_mut()
                        .reset(Instant::now() + QUERY_JOB_STATUS_POLL_INTERVAL),
                    end => return Poll::Ready(end),
                }
            }
            ready!(self.timer.as_mut().poll(cx));
            self.read = Some(query_job.read_status());
        }
    }
}
