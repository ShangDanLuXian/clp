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

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::MutexGuard;
    use std::time::Duration;

    use anyhow::Result;
    use async_trait::async_trait;
    use clp_rust_utils::job_config::NetworkOutput;
    use clp_rust_utils::job_config::QueryJobConfig;
    use clp_rust_utils::job_config::QueryJobStatus;
    use futures::Stream;
    use futures::StreamExt;
    use query_result_listener::encoding::ResultEncoding;
    use query_result_listener::session::SessionConfig;
    use query_result_listener::session::SessionStats;
    use query_result_listener::test_utils::ARCHIVE_ID;
    use query_result_listener::test_utils::QueryResult;
    use query_result_listener::test_utils::QueryResultEncoding;
    use query_result_listener::test_utils::TIMEOUT;
    use query_result_listener::test_utils::expected_results;
    use tokio::net::TcpStream;
    use tokio::sync::Notify;
    use tokio::sync::Semaphore;
    use tokio::time::sleep;
    use tokio::time::timeout;

    use super::QUERY_JOB_STATUS_POLL_INTERVAL;
    use super::QueryEnd;
    use super::QueryEvent;
    use super::QueryJobTable;
    use super::QueryStreamer;
    use crate::client::ClientError;
    use crate::client::QueryConfig;

    /// The ID that [`MockQueryJobTable`] gives the only query job submitted to it.
    const QUERY_JOB_ID: u64 = 42;

    /// The index of the query task whose results the tests' attempts stream.
    const TASK_INDEX: usize = 0;

    /// How long a test waits for the listener to read what an attempt has sent, or for a background
    /// task to do what it was spawned for.
    const SETTLE_DELAY: Duration = Duration::from_millis(100);

    /// The error message of a failed read of a [`MockQueryJobTable`]'s job status.
    const STATUS_READ_ERROR: &str = "the status read failed";

    type TestEvent = QueryEvent<Vec<QueryResult>>;

    /// A [`QueryJobTable`] that keeps the only query job submitted to it in memory, and whose job's
    /// status the test sets.
    #[derive(Clone, Default)]
    struct MockQueryJobTable {
        state: Arc<Mutex<MockQueryJobTableState>>,
        changed: Arc<Notify>,
    }

    impl MockQueryJobTable {
        /// # Returns
        ///
        /// The network output that the submitted query job streams its results to.
        ///
        /// # Panics
        ///
        /// Panics if no query job has been submitted, or its output isn't a network output.
        fn network_output(&self) -> NetworkOutput {
            use clp_rust_utils::job_config::QueryOutput;

            let Some(QueryJobConfig {
                output: QueryOutput::Network(network_output),
                ..
            }) = &self.lock().query_job_config
            else {
                panic!("a query job streaming its results to a network output should be submitted");
            };
            network_output.clone()
        }

        /// # Returns
        ///
        /// The number of reads of the query job's status so far.
        fn num_status_reads(&self) -> usize {
            self.lock().num_status_reads
        }

        /// Sets the status of the submitted query job.
        ///
        /// # Panics
        ///
        /// Panics if no query job has been submitted.
        fn set_status(&self, status: QueryJobStatus) {
            let mut state = self.lock();
            assert!(
                state.status.is_some(),
                "a query job should be submitted before its status is set"
            );
            state.status = Some(status);
        }

        /// Deletes the submitted query job, so that reading its status finds no query job.
        fn delete_query_job(&self) {
            self.lock().status = None;
        }

        /// Makes every later read of the query job's status fail with [`STATUS_READ_ERROR`].
        fn fail_status_reads(&self) {
            self.lock().fails_status_reads = true;
        }

        /// Makes every later submission insert its query job, then wait for a permit of the
        /// returned semaphore before returning the job's ID.
        ///
        /// # Returns
        ///
        /// The semaphore that releases the held submissions, which has no permits.
        fn hold_submissions(&self) -> Arc<Semaphore> {
            let submission_gate = Arc::new(Semaphore::new(0));
            self.lock().submission_gate = Some(Arc::clone(&submission_gate));
            submission_gate
        }

        /// Waits until a query job has been inserted.
        ///
        /// # Panics
        ///
        /// Panics if no query job is inserted within [`TIMEOUT`].
        async fn wait_for_submission(&self) {
            self.wait_until(|state| state.query_job_config.is_some())
                .await;
        }

        /// Waits until the cancellation of the query job has been requested.
        ///
        /// # Panics
        ///
        /// Panics if the cancellation isn't requested within [`TIMEOUT`].
        async fn wait_for_cancellation_request(&self) {
            self.wait_until(|state| !state.cancellation_requests.is_empty())
                .await;
        }

        /// Waits until the cancellation of the query job has been requested, then waits
        /// [`SETTLE_DELAY`] for any further request.
        ///
        /// # Panics
        ///
        /// Panics if the cancellation isn't requested within [`TIMEOUT`], or is requested more than
        /// once.
        async fn expect_single_cancellation_request(&self) {
            self.wait_for_cancellation_request().await;
            sleep(SETTLE_DELAY).await;
            assert_eq!(self.lock().cancellation_requests, [QUERY_JOB_ID]);
        }

        /// Waits [`SETTLE_DELAY`] for any cancellation request.
        ///
        /// # Panics
        ///
        /// Panics if the cancellation of any query job has been requested.
        async fn expect_no_cancellation_request(&self) {
            sleep(SETTLE_DELAY).await;
            assert_eq!(self.lock().cancellation_requests, [] as [u64; 0]);
        }

        /// Waits until `condition` holds for the mock's state.
        ///
        /// # Panics
        ///
        /// Panics if `condition` doesn't hold within [`TIMEOUT`].
        async fn wait_until(&self, condition: impl Fn(&MockQueryJobTableState) -> bool) {
            timeout(TIMEOUT, async {
                loop {
                    // Created before the check, so that no change between the check and the wait is
                    // missed.
                    let changed = self.changed.notified();
                    if condition(&self.lock()) {
                        return;
                    }
                    changed.await;
                }
            })
            .await
            .expect("the mock query jobs table should reach the awaited state");
        }

        /// # Returns
        ///
        /// The guard of the mock's state.
        ///
        /// # Panics
        ///
        /// Panics if the state's lock is poisoned.
        fn lock(&self) -> MutexGuard<'_, MockQueryJobTableState> {
            self.state
                .lock()
                .expect("the state's lock shouldn't be poisoned")
        }
    }

    #[async_trait]
    impl QueryJobTable for MockQueryJobTable {
        async fn submit(&self, query_job_config: &QueryJobConfig) -> Result<u64, ClientError> {
            let submission_gate = {
                let mut state = self.lock();
                assert!(
                    state.query_job_config.is_none(),
                    "a test should submit a single query job"
                );
                state.query_job_config = Some(query_job_config.clone());
                state.status = Some(QueryJobStatus::Pending);
                state.submission_gate.clone()
            };
            self.changed.notify_waiters();
            if let Some(submission_gate) = submission_gate {
                let _permit = submission_gate
                    .acquire()
                    .await
                    .expect("the submission gate shouldn't be closed");
            }
            Ok(QUERY_JOB_ID)
        }

        async fn read_status(
            &self,
            query_job_id: u64,
        ) -> Result<Option<QueryJobStatus>, ClientError> {
            assert_eq!(query_job_id, QUERY_JOB_ID);
            let mut state = self.lock();
            state.num_status_reads += 1;
            if state.fails_status_reads {
                return Err(ClientError::Io(std::io::Error::other(STATUS_READ_ERROR)));
            }
            Ok(state.status)
        }

        async fn request_cancellation(&self, query_job_id: u64) -> Result<(), ClientError> {
            self.lock().cancellation_requests.push(query_job_id);
            self.changed.notify_waiters();
            Ok(())
        }
    }

    #[derive(Default)]
    struct MockQueryJobTableState {
        query_job_config: Option<QueryJobConfig>,
        status: Option<QueryJobStatus>,
        num_status_reads: usize,
        fails_status_reads: bool,
        submission_gate: Option<Arc<Semaphore>>,
        cancellation_requests: Vec<u64>,
    }

    /// A simulated attempt of a `clp-s` query task, streaming the results of task [`TASK_INDEX`] of
    /// archive [`ARCHIVE_ID`] over TCP.
    struct FakeClpS {
        stream: TcpStream,
    }

    impl FakeClpS {
        /// Connects to the listener that `network_output` names, and sends the handshake of task
        /// [`TASK_INDEX`] of archive [`ARCHIVE_ID`] for its session.
        ///
        /// # Returns
        ///
        /// The connected attempt.
        ///
        /// # Panics
        ///
        /// Panics if connecting or sending the handshake fails.
        async fn connect(network_output: &NetworkOutput) -> Self {
            use query_result_listener::protocol::PROTOCOL_VERSION;
            use query_result_listener::test_utils::encode_handshake;

            let stream =
                TcpStream::connect((network_output.host.as_str(), network_output.port.get()))
                    .await
                    .expect("connecting to the listener should succeed");
            let mut attempt = Self { stream };
            attempt
                .send(&encode_handshake(
                    PROTOCOL_VERSION,
                    &network_output.session_token.to_string(),
                    u64::try_from(TASK_INDEX).expect("the test task index fits in `u64`"),
                    ARCHIVE_ID,
                ))
                .await;
            attempt
        }

        /// Connects to the listener that `network_output` names, and sends the results at `0..2`,
        /// then those at `2..4` in a separate read, so that the first batch fills a session's
        /// channel of capacity 1 that nothing consumes, and the attempt waits to send the second.
        ///
        /// # Returns
        ///
        /// The connected attempt.
        ///
        /// # Panics
        ///
        /// Panics if connecting or sending fails.
        async fn connect_blocked_on_its_second_batch(network_output: &NetworkOutput) -> Self {
            let mut attempt = Self::connect(network_output).await;
            attempt.send_results(0..2).await;
            sleep(SETTLE_DELAY).await;
            attempt.send_results(2..4).await;
            sleep(SETTLE_DELAY).await;
            attempt
        }

        /// Sends `bytes` as they are.
        ///
        /// # Panics
        ///
        /// Panics if the write fails.
        async fn send(&mut self, bytes: &[u8]) {
            use tokio::io::AsyncWriteExt;

            self.stream
                .write_all(bytes)
                .await
                .expect("sending to the listener should succeed");
        }

        /// Sends the results at `result_indices` in a single write.
        ///
        /// # Panics
        ///
        /// Panics if the write fails.
        async fn send_results(&mut self, result_indices: impl IntoIterator<Item = u64>) {
            use query_result_listener::test_utils::encode_results;

            self.send(&encode_results(TASK_INDEX, result_indices)).await;
        }

        /// Ends the stream the way `clp-s` does: sends a trailer counting `num_results` results,
        /// then EOF, and waits until the listener closes the connection.
        ///
        /// # Returns
        ///
        /// The bytes the listener sent back.
        ///
        /// # Panics
        ///
        /// Panics if sending fails, or the listener doesn't close the connection within
        /// [`TIMEOUT`].
        async fn finish(mut self, num_results: u64) -> Vec<u8> {
            use query_result_listener::test_utils::encode_trailer;
            use tokio::io::AsyncWriteExt;

            self.send(&encode_trailer(num_results)).await;
            self.stream
                .shutdown()
                .await
                .expect("sending EOF to the listener should succeed");
            self.read_until_closed().await
        }

        /// Waits until the listener closes the connection without sending anything.
        ///
        /// # Panics
        ///
        /// Panics if the listener sends anything, or doesn't close the connection within
        /// [`TIMEOUT`].
        async fn expect_closed_by_listener(self) {
            assert_eq!(
                self.read_until_closed().await,
                [] as [u8; 0],
                "the listener shouldn't acknowledge the connection"
            );
        }

        /// Waits until the listener closes the connection, either cleanly or with a reset.
        ///
        /// # Returns
        ///
        /// The bytes the listener sent before closing.
        ///
        /// # Panics
        ///
        /// Panics if the listener doesn't close the connection within [`TIMEOUT`], or reading fails
        /// with an error other than a reset.
        async fn read_until_closed(mut self) -> Vec<u8> {
            use tokio::io::AsyncReadExt;

            let mut received = Vec::new();
            match timeout(TIMEOUT, self.stream.read_to_end(&mut received))
                .await
                .expect("the listener should close the connection")
            {
                Ok(_) => received,
                // The listener resets the connection if it closes it before reading every byte
                // sent.
                Err(e) if ErrorKind::ConnectionReset == e.kind() => received,
                Err(e) => panic!("reading from the listener failed: {e}"),
            }
        }
    }

    /// The items that a streamed query's stream yields from some point until its last item.
    struct Ending {
        results: Vec<QueryResult>,
        last_event: TestEvent,
    }

    impl Ending {
        /// Receives the items of `query_events` until its end or error, without polling it further.
        ///
        /// # Returns
        ///
        /// The results of the received batches, and the stream's last item.
        ///
        /// # Panics
        ///
        /// Panics if the stream doesn't yield its end or error within [`TIMEOUT`], or ends without
        /// one.
        async fn receive(query_events: &mut (impl Stream<Item = TestEvent> + Unpin)) -> Self {
            let mut results = Vec::new();
            let last_event = loop {
                match next_event(query_events)
                    .await
                    .expect("the query stream shouldn't end without an end or an error")
                {
                    QueryEvent::Results(batch) => results.extend(batch),
                    last_event => break last_event,
                }
            };
            Self {
                results,
                last_event,
            }
        }

        /// # Returns
        ///
        /// The end that the stream's last item carries.
        ///
        /// # Panics
        ///
        /// Panics if the stream's last item isn't an end.
        fn end(&self) -> QueryEnd {
            let QueryEvent::End(end) = &self.last_event else {
                panic!(
                    "the query stream should end with an end: {:?}",
                    self.last_event
                );
            };
            *end
        }

        /// # Returns
        ///
        /// The error that the stream's last item carries.
        ///
        /// # Panics
        ///
        /// Panics if the stream's last item isn't an error.
        fn error(&self) -> &ClientError {
            let QueryEvent::Error(error) = &self.last_event else {
                panic!(
                    "the query stream should end with an error: {:?}",
                    self.last_event
                );
            };
            error
        }
    }

    /// # Type Parameters
    ///
    /// * `EncodingType` - The encoding of the queries' results.
    ///
    /// # Returns
    ///
    /// A streamer whose loopback listener's handshake timeout outlasts [`TIMEOUT`], whose sessions
    /// are configured by `session_config`, and whose query jobs are submitted to `query_job_table`.
    ///
    /// # Panics
    ///
    /// Panics if the advertised host fixture is empty, or the listener can't be bound.
    fn start_streamer<EncodingType: ResultEncoding>(
        query_job_table: &MockQueryJobTable,
        session_config: SessionConfig,
    ) -> QueryStreamer<MockQueryJobTable, EncodingType> {
        use std::net::Ipv4Addr;
        use std::net::SocketAddr;
        use std::num::NonZeroU64;

        use non_empty_string::NonEmptyString;
        use query_result_listener::listener::ListenerConfig;
        use query_result_listener::listener::ResultListener;

        let listener = ResultListener::bind(ListenerConfig {
            bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            advertised_host: NonEmptyString::new("127.0.0.1".to_owned())
                .expect("the advertised host fixture should be non-empty"),
            handshake_timeout_sec: NonZeroU64::new(6 * TIMEOUT.as_secs())
                .expect("the handshake timeout should be nonzero"),
        })
        .expect("binding a loopback listener should succeed");
        QueryStreamer::new(listener, query_job_table.clone(), session_config)
    }

    /// # Returns
    ///
    /// The config of a streamed query of every log event.
    fn query_config() -> QueryConfig {
        QueryConfig {
            query_string: "*".to_owned(),
            datasets: None,
            max_num_results: None,
            time_range_begin_millisecs: None,
            time_range_end_millisecs: None,
            ignore_case: false,
            buffer_results_in_mongodb: false,
            count_by_time_bucket_size_millisecs: None,
        }
    }

    /// Polls `query_events` once, which starts the interval before the first read of its job's
    /// status.
    ///
    /// # Panics
    ///
    /// Panics if the stream yields anything or ends.
    fn expect_pending(query_events: &mut (impl Stream<Item = TestEvent> + Unpin)) {
        use futures::FutureExt;

        let polled = query_events.next().now_or_never();
        assert!(
            polled.is_none(),
            "the query stream shouldn't yield anything yet: {polled:?}"
        );
    }

    /// # Returns
    ///
    /// The next item of `query_events`, or `None` if it has ended.
    ///
    /// # Panics
    ///
    /// Panics if neither happens within [`TIMEOUT`].
    async fn next_event(
        query_events: &mut (impl Stream<Item = TestEvent> + Unpin),
    ) -> Option<TestEvent> {
        timeout(TIMEOUT, query_events.next())
            .await
            .expect("the query stream should yield an item or end")
    }

    /// Receives batches from `query_events` until they hold `num_results` results.
    ///
    /// # Returns
    ///
    /// The received results.
    ///
    /// # Panics
    ///
    /// Panics if the stream yields anything but batches, or the results don't arrive within
    /// [`TIMEOUT`].
    async fn receive_results(
        query_events: &mut (impl Stream<Item = TestEvent> + Unpin),
        num_results: usize,
    ) -> Vec<QueryResult> {
        let mut received = Vec::new();
        while received.len() < num_results {
            match next_event(query_events).await {
                Some(QueryEvent::Results(batch)) => received.extend(batch),
                event => panic!("the query stream should yield a batch: {event:?}"),
            }
        }
        received
    }

    #[tokio::test]
    async fn results_are_forwarded_while_the_job_runs_and_the_end_carries_its_status_and_stats()
    -> Result<()> {
        let query_job_table = MockQueryJobTable::default();
        let streamer =
            start_streamer::<QueryResultEncoding>(&query_job_table, SessionConfig::default());
        let streamed_query = streamer.submit(query_config()).await?;
        let mut query_events = Box::pin(streamed_query.events);
        let network_output = query_job_table.network_output();
        let mut first_attempt = FakeClpS::connect(&network_output).await;
        first_attempt.send_results(0..3).await;
        let mut received = receive_results(&mut query_events, 3).await;
        first_attempt.finish(3).await;
        let mut retry = FakeClpS::connect(&network_output).await;
        retry.send_results(0..5).await;
        received.extend(receive_results(&mut query_events, 2).await);
        retry.finish(5).await;

        query_job_table.set_status(QueryJobStatus::Succeeded);
        let ending = Ending::receive(&mut query_events).await;

        assert_eq!(streamed_query.query_job_id, QUERY_JOB_ID);
        assert_eq!(received, expected_results(TASK_INDEX, ARCHIVE_ID, 0..5));
        assert_eq!(ending.results, [] as [QueryResult; 0]);
        assert_eq!(
            ending.end(),
            QueryEnd {
                status: QueryJobStatus::Succeeded,
                stats: SessionStats {
                    num_results_emitted: 5,
                    num_duplicates_dropped: 3,
                },
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn end_comes_only_after_a_batch_claimed_before_the_job_terminated_is_forwarded()
    -> Result<()> {
        use std::num::NonZeroUsize;

        let query_job_table = MockQueryJobTable::default();
        let streamer = start_streamer::<QueryResultEncoding>(
            &query_job_table,
            SessionConfig {
                channel_capacity: NonZeroUsize::MIN,
            },
        );
        let mut query_events = Box::pin(streamer.submit(query_config()).await?.events);
        expect_pending(&mut query_events);
        let _attempt =
            FakeClpS::connect_blocked_on_its_second_batch(&query_job_table.network_output()).await;
        query_job_table.set_status(QueryJobStatus::Succeeded);
        // Lets the first status read become due, so that the next poll finds the terminal status
        // while the attempt still waits to send its second batch.
        sleep(QUERY_JOB_STATUS_POLL_INTERVAL).await;

        let ending = Ending::receive(&mut query_events).await;

        assert_eq!(
            ending.results,
            expected_results(TASK_INDEX, ARCHIVE_ID, 0..4)
        );
        assert_eq!(
            ending.end(),
            QueryEnd {
                status: QueryJobStatus::Succeeded,
                stats: SessionStats {
                    num_results_emitted: 4,
                    num_duplicates_dropped: 0,
                },
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn job_that_did_not_succeed_ends_the_session_and_its_open_connection_is_cut_off()
    -> Result<()> {
        let query_job_table = MockQueryJobTable::default();
        let streamer =
            start_streamer::<QueryResultEncoding>(&query_job_table, SessionConfig::default());
        let mut query_events = Box::pin(streamer.submit(query_config()).await?.events);
        let attempt = FakeClpS::connect(&query_job_table.network_output()).await;
        // Lets the listener join the attempt to the session while the job runs.
        sleep(SETTLE_DELAY).await;

        query_job_table.set_status(QueryJobStatus::Failed);
        let ending = Ending::receive(&mut query_events).await;
        attempt.expect_closed_by_listener().await;

        assert_eq!(
            ending.end(),
            QueryEnd {
                status: QueryJobStatus::Failed,
                stats: SessionStats::default(),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn session_failure_takes_priority_over_a_terminal_status_and_requests_cancellation_once()
    -> Result<()> {
        use clp_rust_utils::types::ArchiveId;
        use query_result_listener::test_utils::EncodingError;
        use query_result_listener::test_utils::FallibleQueryResultEncoding;
        use query_result_listener::test_utils::UNENCODABLE_MESSAGE;
        use query_result_listener::test_utils::encode_result;
        use query_result_listener::test_utils::timestamp;

        let query_job_table = MockQueryJobTable::default();
        let streamer = start_streamer::<FallibleQueryResultEncoding>(
            &query_job_table,
            SessionConfig::default(),
        );
        let mut query_events = Box::pin(streamer.submit(query_config()).await?.events);
        expect_pending(&mut query_events);
        let mut failing_attempt = FakeClpS::connect(&query_job_table.network_output()).await;
        failing_attempt
            .send(&encode_result(0, timestamp(0), UNENCODABLE_MESSAGE))
            .await;
        // The listener closes the connection only once it has failed the session.
        failing_attempt.expect_closed_by_listener().await;
        query_job_table.set_status(QueryJobStatus::Succeeded);
        // Lets the first status read become due, so that the terminal status is ready at the next
        // poll as well.
        sleep(QUERY_JOB_STATUS_POLL_INTERVAL).await;

        let ending = Ending::receive(&mut query_events).await;

        let expected_error = EncodingError::UnencodableMessage {
            archive_id: ARCHIVE_ID.parse::<ArchiveId>()?,
            timestamp_millisecs: timestamp(0),
        };
        assert!(
            matches!(
                ending.error(),
                ClientError::QueryResultEncoding(message) if expected_error.to_string() == *message
            ),
            "unexpected error: {:?}",
            ending.error()
        );
        query_job_table.wait_for_cancellation_request().await;
        drop(query_events);
        query_job_table.expect_single_cancellation_request().await;
        Ok(())
    }

    #[tokio::test]
    async fn failed_status_read_ends_the_stream_with_its_error_and_requests_cancellation_once()
    -> Result<()> {
        use std::num::NonZeroUsize;

        let query_job_table = MockQueryJobTable::default();
        let streamer = start_streamer::<QueryResultEncoding>(
            &query_job_table,
            SessionConfig {
                channel_capacity: NonZeroUsize::MIN,
            },
        );
        let mut query_events = Box::pin(streamer.submit(query_config()).await?.events);
        expect_pending(&mut query_events);
        let attempt =
            FakeClpS::connect_blocked_on_its_second_batch(&query_job_table.network_output()).await;
        query_job_table.fail_status_reads();
        // Lets the first status read become due, so that it fails at the next poll.
        sleep(QUERY_JOB_STATUS_POLL_INTERVAL).await;

        let ending = Ending::receive(&mut query_events).await;
        attempt.expect_closed_by_listener().await;

        assert_eq!(ending.results, [] as [QueryResult; 0]);
        assert!(
            matches!(
                ending.error(),
                ClientError::Io(error) if STATUS_READ_ERROR == error.to_string()
            ),
            "unexpected error: {:?}",
            ending.error()
        );
        query_job_table.wait_for_cancellation_request().await;
        drop(query_events);
        query_job_table.expect_single_cancellation_request().await;
        Ok(())
    }

    #[tokio::test]
    async fn missing_job_ends_the_stream_with_query_job_not_found_and_requests_cancellation_once()
    -> Result<()> {
        let query_job_table = MockQueryJobTable::default();
        let streamer =
            start_streamer::<QueryResultEncoding>(&query_job_table, SessionConfig::default());
        let mut query_events = Box::pin(streamer.submit(query_config()).await?.events);

        query_job_table.delete_query_job();
        let ending = Ending::receive(&mut query_events).await;

        assert!(
            matches!(ending.error(), ClientError::QueryJobNotFound(QUERY_JOB_ID)),
            "unexpected error: {:?}",
            ending.error()
        );
        query_job_table.wait_for_cancellation_request().await;
        drop(query_events);
        query_job_table.expect_single_cancellation_request().await;
        Ok(())
    }

    #[tokio::test]
    async fn dropping_the_stream_before_its_end_requests_cancellation_once_and_ends_the_session()
    -> Result<()> {
        let query_job_table = MockQueryJobTable::default();
        let streamer =
            start_streamer::<QueryResultEncoding>(&query_job_table, SessionConfig::default());
        let mut query_events = Box::pin(streamer.submit(query_config()).await?.events);
        let attempt = FakeClpS::connect(&query_job_table.network_output()).await;
        // Lets the listener join the attempt to the session while the job runs.
        sleep(SETTLE_DELAY).await;
        expect_pending(&mut query_events);

        drop(query_events);
        attempt.expect_closed_by_listener().await;
        query_job_table.expect_single_cancellation_request().await;
        Ok(())
    }

    #[tokio::test]
    async fn dropping_the_stream_while_it_drains_after_the_job_terminated_requests_nothing()
    -> Result<()> {
        use std::num::NonZeroUsize;

        let query_job_table = MockQueryJobTable::default();
        let streamer = start_streamer::<QueryResultEncoding>(
            &query_job_table,
            SessionConfig {
                channel_capacity: NonZeroUsize::MIN,
            },
        );
        let mut query_events = Box::pin(streamer.submit(query_config()).await?.events);
        expect_pending(&mut query_events);
        let _attempt =
            FakeClpS::connect_blocked_on_its_second_batch(&query_job_table.network_output()).await;
        query_job_table.set_status(QueryJobStatus::Succeeded);
        // Lets the first status read become due, so that the next poll finds the terminal status
        // before it forwards the first batch, and the stream then waits for the second.
        sleep(QUERY_JOB_STATUS_POLL_INTERVAL).await;
        receive_results(&mut query_events, 2).await;

        drop(query_events);
        query_job_table.expect_no_cancellation_request().await;
        Ok(())
    }

    #[test]
    fn dropping_the_stream_outside_a_runtime_does_not_panic() -> Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let query_job_table = MockQueryJobTable::default();
        let streamer = {
            let _runtime_context = runtime.enter();
            start_streamer::<QueryResultEncoding>(&query_job_table, SessionConfig::default())
        };
        let streamed_query = runtime.block_on(streamer.submit(query_config()))?;
        drop(runtime);

        drop(streamed_query);

        assert_eq!(query_job_table.lock().cancellation_requests, [] as [u64; 0]);
        Ok(())
    }

    #[tokio::test]
    async fn dropping_the_submission_while_it_inserts_the_job_requests_the_jobs_cancellation()
    -> Result<()> {
        let query_job_table = MockQueryJobTable::default();
        let streamer =
            start_streamer::<QueryResultEncoding>(&query_job_table, SessionConfig::default());
        let submission_gate = query_job_table.hold_submissions();
        let mut submission = Box::pin(streamer.submit(query_config()));
        tokio::select! {
            _ = &mut submission => anyhow::bail!("the submission should wait for its gate"),
            () = query_job_table.wait_for_submission() => {}
        }

        drop(submission);
        submission_gate.add_permits(1);
        query_job_table.expect_single_cancellation_request().await;
        Ok(())
    }

    #[tokio::test]
    async fn status_is_read_repeatedly_but_at_most_once_per_poll_interval_while_the_job_runs()
    -> Result<()> {
        use std::time::Instant;

        let query_job_table = MockQueryJobTable::default();
        let streamer =
            start_streamer::<QueryResultEncoding>(&query_job_table, SessionConfig::default());
        let mut query_events = Box::pin(streamer.submit(query_config()).await?.events);

        let polling_started_at = Instant::now();
        let event_while_running =
            timeout(10 * QUERY_JOB_STATUS_POLL_INTERVAL, query_events.next()).await;
        let num_status_reads = query_job_table.num_status_reads();
        let elapsed = polling_started_at.elapsed();

        assert!(
            event_while_running.is_err(),
            "the query stream shouldn't yield anything while its job runs: {event_while_running:?}"
        );
        assert!(
            num_status_reads >= 2,
            "the status should be read repeatedly, not {num_status_reads} times"
        );
        // Each read starts at least one interval after the previous one, and the first at least one
        // interval after the stream's first poll.
        assert!(
            QUERY_JOB_STATUS_POLL_INTERVAL * u32::try_from(num_status_reads)? <= elapsed,
            "{num_status_reads} reads in {elapsed:?} are more than one per poll interval"
        );
        Ok(())
    }
}
