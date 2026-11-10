use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use anyhow::Result;
use api_server::client::ClientError;
use api_server::client::QueryConfig;
use api_server::query_stream::QUERY_JOB_STATUS_POLL_INTERVAL;
use api_server::query_stream::QueryEnd;
use api_server::query_stream::QueryEvent;
use api_server::query_stream::QueryJobTable;
use api_server::query_stream::QueryStream;
use api_server::query_stream::QueryStreamer;
use async_trait::async_trait;
use clp_rust_utils::job_config::NetworkOutput;
use clp_rust_utils::job_config::QueryJobConfig;
use clp_rust_utils::job_config::QueryJobStatus;
use clp_rust_utils::types::SessionToken;
use futures::StreamExt;
use query_result_listener::protocol::Ack;
use query_result_listener::protocol::PROTOCOL_VERSION;
use query_result_listener::session::SessionStats;
use query_result_listener::test_utils::ARCHIVE_ID;
use query_result_listener::test_utils::QueryResult;
use query_result_listener::test_utils::QueryResultEncoding;
use query_result_listener::test_utils::TIMEOUT;
use query_result_listener::test_utils::encode_handshake;
use query_result_listener::test_utils::encode_results;
use query_result_listener::test_utils::encode_trailer;
use query_result_listener::test_utils::expected_results;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tokio::time::timeout;

/// The index of the query task that the tests' attempts stream the results of.
const TASK_INDEX: usize = 0;

type TestQueryStreamer = QueryStreamer<FakeQueryJobTable, QueryResultEncoding>;

type TestQueryStream = QueryStream<FakeQueryJobTable, QueryResultEncoding>;

/// A [`QueryJobTable`] that keeps its query jobs in memory, and whose jobs' statuses the test sets.
#[derive(Clone, Default)]
struct FakeQueryJobTable {
    state: Arc<Mutex<FakeQueryJobTableState>>,
    changed: Arc<Notify>,
}

impl FakeQueryJobTable {
    /// # Returns
    ///
    /// The configs of the submitted query jobs, ordered by their IDs.
    fn query_job_configs(&self) -> Vec<QueryJobConfig> {
        self.lock()
            .query_jobs
            .values()
            .map(|query_job| query_job.config.clone())
            .collect()
    }

    /// # Returns
    ///
    /// The IDs of the query jobs whose cancellation was requested, in request order.
    fn cancellation_requests(&self) -> Vec<u64> {
        self.lock().cancellation_requests.clone()
    }

    /// # Returns
    ///
    /// The number of reads of a query job's status so far.
    fn num_status_reads(&self) -> usize {
        self.lock().num_status_reads
    }

    /// Sets the status of the query job `query_job_id`.
    ///
    /// # Panics
    ///
    /// Panics if the query job doesn't exist.
    fn set_status(&self, query_job_id: u64, status: QueryJobStatus) {
        self.lock()
            .query_jobs
            .get_mut(&query_job_id)
            .expect("the query job should exist")
            .status = status;
    }

    /// Makes every later read of a query job's status fail.
    fn fail_status_reads(&self) {
        self.lock().fails_status_reads = true;
    }

    /// Makes every later submission insert its query job, then wait for a permit of the returned
    /// semaphore before returning the job's ID.
    ///
    /// # Returns
    ///
    /// The semaphore that releases the held submissions, which has no permits.
    fn hold_submissions(&self) -> Arc<Semaphore> {
        let submission_gate = Arc::new(Semaphore::new(0));
        self.lock().submission_gate = Some(Arc::clone(&submission_gate));
        submission_gate
    }

    /// Waits until `num_requests` cancellation requests have been made.
    ///
    /// # Returns
    ///
    /// The IDs of the query jobs whose cancellation was requested, in request order.
    ///
    /// # Panics
    ///
    /// Panics if the requests aren't made within [`TIMEOUT`].
    async fn wait_for_cancellation_requests(&self, num_requests: usize) -> Vec<u64> {
        self.wait_until(|state| {
            (state.cancellation_requests.len() >= num_requests)
                .then(|| state.cancellation_requests.clone())
        })
        .await
    }

    /// Waits until `num_query_jobs` query jobs have been inserted.
    ///
    /// # Panics
    ///
    /// Panics if the query jobs aren't inserted within [`TIMEOUT`].
    async fn wait_for_query_jobs(&self, num_query_jobs: usize) {
        self.wait_until(|state| (state.query_jobs.len() >= num_query_jobs).then_some(()))
            .await;
    }

    /// Waits until `condition` holds a value for the fake's state.
    ///
    /// # Type Parameters
    ///
    /// * `ValueType` - The type of the value that `condition` holds.
    ///
    /// # Returns
    ///
    /// The value that `condition` holds.
    ///
    /// # Panics
    ///
    /// Panics if `condition` holds no value within [`TIMEOUT`].
    async fn wait_until<ValueType>(
        &self,
        condition: impl Fn(&FakeQueryJobTableState) -> Option<ValueType>,
    ) -> ValueType {
        timeout(TIMEOUT, async {
            loop {
                // Created before the check, so that no change between the check and the wait is
                // missed.
                let changed = self.changed.notified();
                let value = condition(&self.lock());
                if let Some(value) = value {
                    return value;
                }
                changed.await;
            }
        })
        .await
        .expect("the fake query jobs table should reach the awaited state")
    }

    /// # Returns
    ///
    /// The guard of the fake's state.
    ///
    /// # Panics
    ///
    /// Panics if the state's lock is poisoned.
    fn lock(&self) -> MutexGuard<'_, FakeQueryJobTableState> {
        self.state
            .lock()
            .expect("the state's lock shouldn't be poisoned")
    }
}

#[async_trait]
impl QueryJobTable for FakeQueryJobTable {
    async fn submit(&self, query_job_config: &QueryJobConfig) -> Result<u64, ClientError> {
        let (query_job_id, submission_gate) = {
            let mut state = self.lock();
            let query_job_id = u64::try_from(state.query_jobs.len() + 1)
                .expect("the number of test query jobs should fit in `u64`");
            state.query_jobs.insert(
                query_job_id,
                FakeQueryJob {
                    config: query_job_config.clone(),
                    status: QueryJobStatus::Pending,
                },
            );
            (query_job_id, state.submission_gate.clone())
        };
        self.changed.notify_waiters();
        if let Some(submission_gate) = submission_gate {
            let _permit = submission_gate
                .acquire()
                .await
                .expect("the submission gate shouldn't be closed");
        }
        Ok(query_job_id)
    }

    async fn read_status(&self, query_job_id: u64) -> Result<QueryJobStatus, ClientError> {
        let mut state = self.lock();
        state.num_status_reads += 1;
        if state.fails_status_reads {
            return Err(ClientError::Io(std::io::Error::other(
                "the status read failed",
            )));
        }
        state
            .query_jobs
            .get(&query_job_id)
            .map(|query_job| query_job.status)
            .ok_or(ClientError::QueryJobNotFound(query_job_id))
    }

    async fn request_cancellation(&self, query_job_id: u64) -> Result<(), ClientError> {
        self.lock().cancellation_requests.push(query_job_id);
        self.changed.notify_waiters();
        Ok(())
    }
}

#[derive(Default)]
struct FakeQueryJobTableState {
    query_jobs: BTreeMap<u64, FakeQueryJob>,
    cancellation_requests: Vec<u64>,
    num_status_reads: usize,
    fails_status_reads: bool,
    submission_gate: Option<Arc<Semaphore>>,
}

struct FakeQueryJob {
    config: QueryJobConfig,
    status: QueryJobStatus,
}

/// A simulated attempt of a `clp-s` query task, streaming the results of task [`TASK_INDEX`] over
/// TCP.
struct FakeClpS {
    stream: TcpStream,
    session_token: SessionToken,
}

impl FakeClpS {
    /// Connects to the listener that `network_output` names, as an attempt for its session, and
    /// sends the handshake of task [`TASK_INDEX`] of archive [`ARCHIVE_ID`].
    ///
    /// # Returns
    ///
    /// The connected attempt.
    ///
    /// # Panics
    ///
    /// Panics if connecting or sending the handshake fails.
    async fn connect(network_output: &NetworkOutput) -> Self {
        let stream = TcpStream::connect((network_output.host.as_str(), network_output.port.get()))
            .await
            .expect("connecting to the listener should succeed");
        let mut attempt = Self {
            stream,
            session_token: network_output.session_token,
        };
        attempt
            .send(&encode_handshake(
                PROTOCOL_VERSION,
                &attempt.session_token.to_string(),
                u64::try_from(TASK_INDEX).expect("the test task index fits in `u64`"),
                ARCHIVE_ID,
            ))
            .await;
        attempt
    }

    /// Sends the results at `result_indices` in a single write.
    ///
    /// # Panics
    ///
    /// Panics if the write fails.
    async fn send_results(&mut self, result_indices: impl IntoIterator<Item = u64>) {
        self.send(&encode_results(TASK_INDEX, result_indices)).await;
    }

    /// Ends the stream the way `clp-s` does: sends a trailer counting `num_results` results, then
    /// EOF, and waits until the listener closes the connection.
    ///
    /// # Returns
    ///
    /// The bytes the listener sent back.
    ///
    /// # Panics
    ///
    /// Panics if sending fails, or the listener doesn't close the connection within [`TIMEOUT`].
    async fn finish(mut self, num_results: u64) -> Vec<u8> {
        self.send(&encode_trailer(num_results)).await;
        self.stream
            .shutdown()
            .await
            .expect("sending EOF to the listener should succeed");
        self.read_until_closed().await
    }

    /// Waits until the listener closes the connection without acknowledging it.
    ///
    /// # Panics
    ///
    /// Panics if the listener sends anything, or doesn't close the connection within [`TIMEOUT`].
    async fn expect_closed_by_listener(self) {
        assert_eq!(
            self.read_until_closed().await,
            [] as [u8; 0],
            "the listener shouldn't acknowledge the connection"
        );
    }

    /// Sends `bytes` as they are.
    ///
    /// # Panics
    ///
    /// Panics if the write fails.
    async fn send(&mut self, bytes: &[u8]) {
        self.stream
            .write_all(bytes)
            .await
            .expect("sending to the listener should succeed");
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
        let mut received = Vec::new();
        match timeout(TIMEOUT, self.stream.read_to_end(&mut received))
            .await
            .expect("the listener should close the connection")
        {
            Ok(_) => received,
            // The listener resets the connection if it closes it before reading every byte sent.
            Err(e) if ErrorKind::ConnectionReset == e.kind() => received,
            Err(e) => panic!("reading from the listener failed: {e}"),
        }
    }
}

/// The events a query stream yields from some point until it ends.
struct Ending {
    results: Vec<QueryResult>,
    last_event: QueryEvent<Vec<QueryResult>>,
}

impl Ending {
    /// Receives `query_stream`'s events until it ends.
    ///
    /// # Returns
    ///
    /// The results of the stream's batches, and the stream's last event.
    ///
    /// # Panics
    ///
    /// Panics if the stream doesn't end within [`TIMEOUT`], ends without an end or an error, or
    /// yields anything after one.
    async fn receive(query_stream: &mut TestQueryStream) -> Self {
        let mut results = Vec::new();
        let last_event = loop {
            match next_event(query_stream)
                .await
                .expect("the query stream shouldn't end without an end or an error")
            {
                QueryEvent::Results(batch) => results.extend(batch),
                last_event => break last_event,
            }
        };
        assert!(
            next_event(query_stream).await.is_none(),
            "the query stream should end after its last event"
        );
        Self {
            results,
            last_event,
        }
    }

    /// # Returns
    ///
    /// The end that the stream's last event carries.
    ///
    /// # Panics
    ///
    /// Panics if the stream's last event isn't an end.
    fn end(&self) -> QueryEnd {
        let QueryEvent::End(end) = &self.last_event else {
            panic!(
                "the query stream should end with an end: {:?}",
                self.last_event
            );
        };
        *end
    }
}

/// # Returns
///
/// A streamer whose loopback listener's handshake timeout outlasts [`TIMEOUT`], and whose query
/// jobs are submitted to `query_job_table`.
///
/// # Panics
///
/// Panics if the advertised host fixture is empty, or the listener can't be bound.
fn start_streamer(query_job_table: &FakeQueryJobTable) -> TestQueryStreamer {
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
    QueryStreamer::new(listener, query_job_table.clone())
}

/// # Returns
///
/// A query config that streams every result.
fn base_query_config() -> QueryConfig {
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

/// # Returns
///
/// The network output that the only submitted query job streams its results to.
///
/// # Panics
///
/// Panics if a single query job hasn't been submitted, or its output isn't a network output.
fn network_output_of_the_only_query_job(query_job_table: &FakeQueryJobTable) -> NetworkOutput {
    use clp_rust_utils::job_config::QueryOutput;

    let [query_job_config] = query_job_table
        .query_job_configs()
        .try_into()
        .expect("a single query job should have been submitted");
    let QueryOutput::Network(network_output) = query_job_config.output else {
        panic!("the query job should stream its results to a network output");
    };
    network_output
}

/// # Returns
///
/// The next event of `query_stream`, or `None` if it has ended.
///
/// # Panics
///
/// Panics if neither arrives within [`TIMEOUT`].
async fn next_event(query_stream: &mut TestQueryStream) -> Option<QueryEvent<Vec<QueryResult>>> {
    timeout(TIMEOUT, query_stream.next())
        .await
        .expect("the query stream should yield an event or end")
}

/// Receives batches from `query_stream` until they hold `num_results` results.
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
    query_stream: &mut TestQueryStream,
    num_results: usize,
) -> Vec<QueryResult> {
    let mut received = Vec::new();
    while received.len() < num_results {
        match next_event(query_stream).await {
            Some(QueryEvent::Results(batch)) => received.extend(batch),
            event => panic!("the query stream should yield a batch: {event:?}"),
        }
    }
    received
}

#[tokio::test]
async fn results_stream_until_the_job_succeeds_and_the_end_carries_its_status_and_statistics()
-> Result<()> {
    use clp_rust_utils::job_config::QueryOutput;

    let query_job_table = FakeQueryJobTable::default();
    let streamer = start_streamer(&query_job_table);
    let mut query_stream = streamer.submit(base_query_config()).await?;
    let network_output = network_output_of_the_only_query_job(&query_job_table);
    let mut first_attempt = FakeClpS::connect(&network_output).await;
    first_attempt.send_results(0..3).await;
    let first_acknowledgement = first_attempt.finish(3).await;
    let mut retry = FakeClpS::connect(&network_output).await;
    retry.send_results(0..3).await;
    let retry_acknowledgement = retry.finish(3).await;

    query_job_table.set_status(query_stream.query_job_id(), QueryJobStatus::Succeeded);
    // Lets the next poll find the terminal status while the batches are still buffered.
    sleep(2 * QUERY_JOB_STATUS_POLL_INTERVAL).await;
    let ending = Ending::receive(&mut query_stream).await;

    assert_eq!(
        query_job_table.query_job_configs(),
        [QueryJobConfig {
            output: QueryOutput::Network(network_output),
            ..QueryJobConfig::try_from(base_query_config())?
        }]
    );
    assert_eq!(first_acknowledgement, Ack { num_results: 3 }.encode());
    assert_eq!(retry_acknowledgement, Ack { num_results: 3 }.encode());
    assert_eq!(
        ending.results,
        expected_results(TASK_INDEX, ARCHIVE_ID, 0..3)
    );
    assert_eq!(
        ending.end(),
        QueryEnd {
            status: QueryJobStatus::Succeeded,
            stats: SessionStats {
                num_results_emitted: 3,
                num_duplicates_dropped: 3,
            },
        }
    );
    Ok(())
}

#[tokio::test]
async fn failed_job_ends_its_session_and_cuts_off_its_open_connection() -> Result<()> {
    let query_job_table = FakeQueryJobTable::default();
    let streamer = start_streamer(&query_job_table);
    let mut query_stream = streamer.submit(base_query_config()).await?;
    let mut attempt =
        FakeClpS::connect(&network_output_of_the_only_query_job(&query_job_table)).await;
    attempt.send_results(0..2).await;
    let received = receive_results(&mut query_stream, 2).await;

    query_job_table.set_status(query_stream.query_job_id(), QueryJobStatus::Failed);
    let ending = Ending::receive(&mut query_stream).await;
    attempt.expect_closed_by_listener().await;

    assert_eq!(received, expected_results(TASK_INDEX, ARCHIVE_ID, 0..2));
    assert_eq!(ending.results, [] as [QueryResult; 0]);
    assert_eq!(
        ending.end(),
        QueryEnd {
            status: QueryJobStatus::Failed,
            stats: SessionStats {
                num_results_emitted: 2,
                num_duplicates_dropped: 0,
            },
        }
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn status_is_read_every_poll_interval_until_the_job_terminates() -> Result<()> {
    const NUM_READS: u32 = 10;

    let query_job_table = FakeQueryJobTable::default();
    let streamer = start_streamer(&query_job_table);
    let mut query_stream = streamer.submit(base_query_config()).await?;
    query_job_table.set_status(query_stream.query_job_id(), QueryJobStatus::Running);

    let event_while_running = timeout(
        NUM_READS * QUERY_JOB_STATUS_POLL_INTERVAL + QUERY_JOB_STATUS_POLL_INTERVAL / 2,
        query_stream.next(),
    )
    .await;
    let num_reads_while_running = query_job_table.num_status_reads();
    query_job_table.set_status(query_stream.query_job_id(), QueryJobStatus::Cancelled);
    let ending = Ending::receive(&mut query_stream).await;

    assert!(
        event_while_running.is_err(),
        "the query stream shouldn't yield anything while its job runs: {event_while_running:?}"
    );
    assert_eq!(num_reads_while_running, usize::try_from(NUM_READS)?);
    assert_eq!(ending.end().status, QueryJobStatus::Cancelled);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn failed_status_read_ends_the_stream_with_its_error_and_requests_cancellation_once()
-> Result<()> {
    let query_job_table = FakeQueryJobTable::default();
    let streamer = start_streamer(&query_job_table);
    let mut query_stream = streamer.submit(base_query_config()).await?;
    let query_job_id = query_stream.query_job_id();

    query_job_table.fail_status_reads();
    let ending = Ending::receive(&mut query_stream).await;
    let cancellation_requests = query_job_table.wait_for_cancellation_requests(1).await;
    drop(query_stream);
    sleep(10 * QUERY_JOB_STATUS_POLL_INTERVAL).await;

    assert!(
        matches!(ending.last_event, QueryEvent::Error(ClientError::Io(_))),
        "the query stream should end with the read's error: {:?}",
        ending.last_event
    );
    assert_eq!(cancellation_requests, [query_job_id]);
    assert_eq!(query_job_table.cancellation_requests(), [query_job_id]);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dropping_the_stream_while_its_job_runs_requests_cancellation_once() -> Result<()> {
    let query_job_table = FakeQueryJobTable::default();
    let streamer = start_streamer(&query_job_table);
    let query_stream = streamer.submit(base_query_config()).await?;
    let query_job_id = query_stream.query_job_id();
    query_job_table.set_status(query_job_id, QueryJobStatus::Running);

    drop(query_stream);
    let cancellation_requests = query_job_table.wait_for_cancellation_requests(1).await;
    sleep(10 * QUERY_JOB_STATUS_POLL_INTERVAL).await;

    assert_eq!(cancellation_requests, [query_job_id]);
    assert_eq!(query_job_table.cancellation_requests(), [query_job_id]);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dropping_the_stream_after_its_end_does_not_request_cancellation() -> Result<()> {
    let query_job_table = FakeQueryJobTable::default();
    let streamer = start_streamer(&query_job_table);
    let mut query_stream = streamer.submit(base_query_config()).await?;
    query_job_table.set_status(query_stream.query_job_id(), QueryJobStatus::Succeeded);
    Ending::receive(&mut query_stream).await;

    drop(query_stream);
    sleep(10 * QUERY_JOB_STATUS_POLL_INTERVAL).await;

    assert_eq!(query_job_table.cancellation_requests(), [] as [u64; 0]);
    Ok(())
}

#[tokio::test]
async fn dropping_the_stream_while_it_drains_a_terminated_job_does_not_request_cancellation()
-> Result<()> {
    use futures::FutureExt;

    let query_job_table = FakeQueryJobTable::default();
    let streamer = start_streamer(&query_job_table);
    let mut query_stream = streamer.submit(base_query_config()).await?;
    let mut attempt =
        FakeClpS::connect(&network_output_of_the_only_query_job(&query_job_table)).await;
    attempt.send_results(0..2).await;
    receive_results(&mut query_stream, 2).await;
    query_job_table.set_status(query_stream.query_job_id(), QueryJobStatus::Succeeded);
    sleep(2 * QUERY_JOB_STATUS_POLL_INTERVAL).await;

    // On this single-threaded runtime, the open connection can't finish while the stream is polled,
    // so the poll that finds the terminal status leaves the stream forwarding the connection's last
    // results.
    let polled = query_stream.next().now_or_never();
    drop(query_stream);
    attempt.expect_closed_by_listener().await;
    sleep(QUERY_JOB_STATUS_POLL_INTERVAL).await;

    assert!(
        polled.is_none(),
        "the stream shouldn't yield anything yet: {polled:?}"
    );
    assert_eq!(query_job_table.cancellation_requests(), [] as [u64; 0]);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dropping_a_started_submission_requests_the_submitted_jobs_cancellation() -> Result<()> {
    let query_job_table = FakeQueryJobTable::default();
    let streamer = Arc::new(start_streamer(&query_job_table));
    let submission_gate = query_job_table.hold_submissions();
    let submission = tokio::spawn({
        let streamer = Arc::clone(&streamer);
        async move { streamer.submit(base_query_config()).await.map(drop) }
    });
    query_job_table.wait_for_query_jobs(1).await;

    submission.abort();
    let submission_error = submission
        .await
        .expect_err("the submission should be held until it is aborted");
    submission_gate.add_permits(1);
    let cancellation_requests = query_job_table.wait_for_cancellation_requests(1).await;

    assert!(submission_error.is_cancelled());
    assert_eq!(cancellation_requests, [1]);
    Ok(())
}

#[tokio::test]
async fn query_config_with_an_option_that_does_not_apply_to_streamed_results_is_rejected()
-> Result<()> {
    use std::num::NonZeroU32;

    let query_job_table = FakeQueryJobTable::default();
    let streamer = start_streamer(&query_job_table);

    for (query_config, rejected_option) in [
        (
            QueryConfig {
                max_num_results: NonZeroU32::new(10),
                ..base_query_config()
            },
            "max_num_results",
        ),
        (
            QueryConfig {
                buffer_results_in_mongodb: true,
                ..base_query_config()
            },
            "buffer_results_in_mongodb",
        ),
        (
            QueryConfig {
                count_by_time_bucket_size_millisecs: Some(1000),
                ..base_query_config()
            },
            "count_by_time_bucket_size_millisecs",
        ),
    ] {
        let Err(error) = streamer.submit(query_config).await else {
            anyhow::bail!("a query config setting `{rejected_option}` should be rejected");
        };
        assert!(
            matches!(
                &error,
                ClientError::InvalidInput(message) if message.contains(rejected_option)
            ),
            "unexpected error for `{rejected_option}`: {error:?}"
        );
    }
    assert_eq!(
        query_job_table.query_job_configs(),
        [] as [QueryJobConfig; 0]
    );
    Ok(())
}
