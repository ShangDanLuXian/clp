//! End-to-end tests for query job cancellation against a running CLP package in Spider mode.
//!
//! NOTE: These tests are for manual verification only and are not meant to be merged.
//!
//! The tests drive a real deployment: the package's own `query-coordinator`, Spider, and MariaDB.
//! They submit and cancel query jobs through the API server's client library (the same code the
//! HTTP API runs), read job rows from the CLP database, and check Spider job states through
//! `spider-storage`.
//!
//! # Setup
//!
//! 1. Build the package from this branch and start it in Spider mode (`sbin/start-clp.sh`).
//! 2. Compress enough logs into the dataset under test that a broad query runs for at least a few
//!    seconds; the tests need to cancel a job while it's still running.
//!
//! # Environment variables
//!
//! * `CLP_DB_USER`, `CLP_DB_PASS` (required) - the CLP database credentials (see
//!   `etc/credentials.yaml`).
//! * `CLP_TEST_DB_HOST` (default `127.0.0.1`), `CLP_TEST_DB_PORT` (default `3306`),
//!   `CLP_TEST_DB_NAME` (default `clp-db`) - the CLP database.
//! * `CLP_TEST_SPIDER_ENDPOINT` (default `http://127.0.0.1:50051`) - `spider-storage`.
//! * `CLP_TEST_DATASET` (default `default`) - the dataset to search.
//! * `CLP_TEST_SLOW_QUERY` (default `*`) - a query that takes several seconds to finish.
//! * `CLP_TEST_FAST_QUERY` (default `clp-cancel-test-no-match-0d1f`) - a query that finishes
//!   quickly.
//! * `CLP_TEST_TIMEOUT_SECS` (default `120`) - how long to wait for each expected state.
//! * `CLP_TEST_COORDINATOR_CONTAINER` (optional) - the `query-coordinator` container's name, e.g.
//!   from `docker ps --filter name=query-coordinator`. The restart tests are skipped without it.
//!
//! # Running
//!
//! ```bash
//! cargo test -p query-coordinator --test spider_cancellation -- --ignored --test-threads=1 \
//!     --nocapture
//! ```
//!
//! The tests must run one at a time: the restart tests stop the shared coordinator.

use std::process::Command;
use std::time::Duration;

use api_server::client::Client;
use api_server::client::QueryConfig;
use clp_rust_utils::clp_config::package::config::ApiServer;
use clp_rust_utils::clp_config::package::config::Config;
use clp_rust_utils::clp_config::package::config::Database;
use clp_rust_utils::clp_config::package::config::Package;
use clp_rust_utils::clp_config::package::config::StorageEngine;
use clp_rust_utils::clp_config::package::credentials;
use clp_rust_utils::job_config::QueryJobStatus;
use spider_client::SpiderClient;
use spider_core::job::JobState;
use spider_core::types::id::JobId as SpiderJobId;
use sqlx::MySqlPool;
use tonic::transport::Endpoint;

const POLL_INTERVAL: Duration = Duration::from_millis(20);
const MAX_NUM_RESULTS: u32 = 100_000_000;

const CANCELLED_MESSAGE: &str = "The query job was cancelled.";
const CANCELLED_BEFORE_SUBMISSION_MESSAGE: &str =
    "The query job was cancelled before it was submitted.";
const CANCELLED_AFTER_COMPLETION_MESSAGE: &str =
    "The query job completed, but it had already been requested to be cancelled.";

/// Clients for the deployment under test.
struct Deployment {
    api: Client,
    db: MySqlPool,
    spider: SpiderClient,
    dataset: String,
    timeout: Duration,
}

/// A query job row's status columns.
#[derive(Debug, sqlx::FromRow)]
struct JobRow {
    status: QueryJobStatus,
    status_msg: Option<String>,
    spider_id: Option<u64>,
}

impl Deployment {
    async fn connect() -> anyhow::Result<Self> {
        let mut database = Database {
            host: env_or("CLP_TEST_DB_HOST", "127.0.0.1"),
            port: env_or("CLP_TEST_DB_PORT", "3306").parse()?,
            ..Database::default()
        };
        database.names.clp = env_or("CLP_TEST_DB_NAME", "clp-db");
        let credentials = credentials::Credentials {
            database: credentials::Database {
                user: std::env::var("CLP_DB_USER").expect("`CLP_DB_USER` must be set"),
                password: secrecy::SecretString::new(
                    std::env::var("CLP_DB_PASS")
                        .expect("`CLP_DB_PASS` must be set")
                        .into_boxed_str(),
                ),
            },
        };
        let config = Config {
            package: Package {
                storage_engine: StorageEngine::ClpS,
            },
            database: database.clone(),
            api_server: Some(ApiServer::default()),
            ..Config::default()
        };

        let api = Client::connect(&config, &credentials).await?;
        let db = clp_rust_utils::database::mysql::create_clp_db_mysql_pool(
            &database,
            &credentials.database,
            2,
        )
        .await?;
        let spider = SpiderClient::builder(Endpoint::from_shared(env_or(
            "CLP_TEST_SPIDER_ENDPOINT",
            "http://127.0.0.1:50051",
        ))?)
        .connect()
        .await?;

        Ok(Self {
            api,
            db,
            spider,
            dataset: env_or("CLP_TEST_DATASET", "default"),
            timeout: Duration::from_secs(env_or("CLP_TEST_TIMEOUT_SECS", "120").parse()?),
        })
    }

    async fn submit(&self, query_string: &str) -> anyhow::Result<u64> {
        let job_id = self
            .api
            .submit_query(QueryConfig {
                query_string: query_string.to_owned(),
                datasets: Some(vec![self.dataset.clone()]),
                max_num_results: MAX_NUM_RESULTS,
                time_range_begin_millisecs: None,
                time_range_end_millisecs: None,
                ignore_case: false,
                buffer_results_in_mongodb: true,
                count_by_time_bucket_size_millisecs: None,
            })
            .await?;
        println!("Submitted query job {job_id} (`{query_string}`).");
        Ok(job_id)
    }

    async fn submit_slow(&self) -> anyhow::Result<u64> {
        self.submit(&env_or("CLP_TEST_SLOW_QUERY", "*")).await
    }

    async fn row(&self, job_id: u64) -> anyhow::Result<JobRow> {
        Ok(sqlx::query_as::<_, JobRow>(
            "SELECT `status`, `status_msg`, `spider_id` FROM `query_jobs` WHERE `id` = ?",
        )
        .bind(job_id)
        .fetch_one(&self.db)
        .await?)
    }

    /// Polls the job's row until `predicate` holds, printing every status it passes through.
    async fn wait_for_row(
        &self,
        job_id: u64,
        description: &str,
        predicate: impl Fn(&JobRow) -> bool,
    ) -> anyhow::Result<JobRow> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        let mut last_status = None;
        loop {
            let row = self.row(job_id).await?;
            if last_status != Some(row.status) {
                println!("  job {job_id}: {:?}", row.status);
                last_status = Some(row.status);
            }
            if predicate(&row) {
                return Ok(row);
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for job {job_id} to be {description}; last row: {row:?}"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Waits until the job is running in Spider.
    ///
    /// # Returns
    ///
    /// The job's Spider job ID on success.
    async fn wait_until_running(&self, job_id: u64) -> anyhow::Result<SpiderJobId> {
        let row = self
            .wait_for_row(job_id, "running", |row| {
                row.status == QueryJobStatus::Running || row.status.is_terminal()
            })
            .await?;
        anyhow::ensure!(
            row.status == QueryJobStatus::Running,
            "job {job_id} finished as {:?} before it could be observed running; use a slower \
             `CLP_TEST_SLOW_QUERY` or a larger dataset",
            row.status
        );
        Ok(SpiderJobId::from(
            row.spider_id.expect("a running job has a Spider job ID"),
        ))
    }

    async fn wait_until_terminal(&self, job_id: u64) -> anyhow::Result<JobRow> {
        self.wait_for_row(job_id, "terminal", |row| row.status.is_terminal())
            .await
    }

    /// Cancels the job through the API server, failing with a clear message if the job finished
    /// before the cancellation reached it.
    async fn cancel(&self, job_id: u64) -> anyhow::Result<()> {
        self.api.cancel_search_job(job_id).await.map_err(|e| {
            anyhow::anyhow!(
                "cancelling job {job_id} failed ({e}); if it already finished, use a slower \
                 `CLP_TEST_SLOW_QUERY` or a larger dataset"
            )
        })?;
        println!("Requested the cancellation of query job {job_id}.");
        Ok(())
    }

    async fn spider_state(&self, spider_job_id: SpiderJobId) -> anyhow::Result<JobState> {
        Ok(self.spider.get_job_state(spider_job_id).await?)
    }
}

/// # Returns
///
/// The value of the environment variable `name`, or `default` if it isn't set.
fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

/// Asserts that a cancelled job ended as `Cancelled`, with one of the messages the cancellation
/// path can produce (the Spider job may finish on its own just before the cancel reaches it).
fn assert_cancelled(row: &JobRow) {
    assert_eq!(row.status, QueryJobStatus::Cancelled, "{row:?}");
    assert!(
        matches!(
            row.status_msg.as_deref(),
            Some(CANCELLED_MESSAGE | CANCELLED_AFTER_COMPLETION_MESSAGE)
        ),
        "{row:?}"
    );
}

/// The `query-coordinator` container, for the restart tests.
struct CoordinatorContainer {
    name: String,
}

impl CoordinatorContainer {
    /// # Returns
    ///
    /// The container named by `CLP_TEST_COORDINATOR_CONTAINER`, or `None` if it isn't set.
    fn from_env() -> Option<Self> {
        let name = std::env::var("CLP_TEST_COORDINATOR_CONTAINER").ok()?;
        Some(Self { name })
    }

    fn stop(&self) {
        self.docker("stop");
    }

    fn start(&self) {
        self.docker("start");
    }

    fn docker(&self, action: &str) {
        let status = Command::new("docker")
            .args([action, &self.name])
            .status()
            .expect("failed to run `docker`");
        assert!(status.success(), "`docker {action} {}` failed", self.name);
        println!("`docker {action} {}` done.", self.name);
    }
}

/// Starts the coordinator again if a restart test fails while it's stopped.
struct StartOnDrop<'container>(&'container CoordinatorContainer);

impl Drop for StartOnDrop<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.start();
        }
    }
}

/// A running job is cancelled in Spider, then terminated as `Cancelled`.
#[tokio::test]
#[ignore = "requires a running CLP package in Spider mode"]
async fn cancel_running_job() -> anyhow::Result<()> {
    let deployment = Deployment::connect().await?;
    let job_id = deployment.submit_slow().await?;
    let spider_job_id = deployment.wait_until_running(job_id).await?;

    deployment.cancel(job_id).await?;
    let row = deployment.wait_until_terminal(job_id).await?;

    assert_cancelled(&row);
    let spider_state = deployment.spider_state(spider_job_id).await?;
    assert!(
        matches!(spider_state, JobState::Cancelled | JobState::Succeeded),
        "Spider job {spider_job_id} is {spider_state:?}"
    );
    Ok(())
}

/// A repeated cancellation of a job whose cancellation is in progress is accepted.
#[tokio::test]
#[ignore = "requires a running CLP package in Spider mode"]
async fn repeated_cancel_is_accepted() -> anyhow::Result<()> {
    let deployment = Deployment::connect().await?;
    let job_id = deployment.submit_slow().await?;
    deployment.wait_until_running(job_id).await?;

    deployment.cancel(job_id).await?;
    let status = deployment.row(job_id).await?.status;
    if status.is_terminal() {
        println!("Job {job_id} terminated before the repeated cancel; nothing left to check.");
    } else {
        deployment.cancel(job_id).await?;
    }

    assert_cancelled(&deployment.wait_until_terminal(job_id).await?);
    Ok(())
}

/// Cancelling a job that already finished is rejected, and its status is left alone.
#[tokio::test]
#[ignore = "requires a running CLP package in Spider mode"]
async fn cancel_finished_job_is_rejected() -> anyhow::Result<()> {
    let deployment = Deployment::connect().await?;
    let job_id = deployment
        .submit(&env_or(
            "CLP_TEST_FAST_QUERY",
            "clp-cancel-test-no-match-0d1f",
        ))
        .await?;
    let row = deployment.wait_until_terminal(job_id).await?;
    assert_eq!(row.status, QueryJobStatus::Succeeded, "{row:?}");

    let result = deployment.api.cancel_search_job(job_id).await;
    assert!(
        matches!(&result, Err(e) if e.to_string().starts_with("Search job not found")),
        "{result:?}"
    );
    assert_eq!(
        deployment.row(job_id).await?.status,
        QueryJobStatus::Succeeded
    );
    Ok(())
}

/// A job whose Spider job is cancelled directly in Spider, without a cancellation request, fails.
#[tokio::test]
#[ignore = "requires a running CLP package in Spider mode"]
async fn job_cancelled_in_spider_directly_fails() -> anyhow::Result<()> {
    let deployment = Deployment::connect().await?;
    let job_id = deployment.submit_slow().await?;
    let spider_job_id = deployment.wait_until_running(job_id).await?;

    deployment.spider.cancel_job(spider_job_id).await?;
    println!("Cancelled Spider job {spider_job_id} directly.");
    let row = deployment.wait_until_terminal(job_id).await?;

    assert_eq!(row.status, QueryJobStatus::Failed, "{row:?}");
    println!("  status_msg: {:?}", row.status_msg);
    Ok(())
}

/// A job cancelled while the coordinator is down, before it was submitted to Spider, is cancelled
/// by the startup sweep without ever reaching Spider.
#[tokio::test]
#[ignore = "requires a running CLP package in Spider mode"]
async fn restart_sweeps_unsubmitted_cancelled_job() -> anyhow::Result<()> {
    let Some(coordinator) = CoordinatorContainer::from_env() else {
        println!("`CLP_TEST_COORDINATOR_CONTAINER` isn't set; skipping.");
        return Ok(());
    };
    let deployment = Deployment::connect().await?;

    coordinator.stop();
    let _guard = StartOnDrop(&coordinator);
    let job_id = deployment.submit_slow().await?;
    deployment.cancel(job_id).await?;
    let row = deployment.row(job_id).await?;
    assert_eq!(row.status, QueryJobStatus::CancelRequested, "{row:?}");
    assert_eq!(row.spider_id, None);
    coordinator.start();

    let row = deployment.wait_until_terminal(job_id).await?;
    assert_eq!(row.status, QueryJobStatus::Cancelled, "{row:?}");
    assert_eq!(
        row.status_msg.as_deref(),
        Some(CANCELLED_BEFORE_SUBMISSION_MESSAGE)
    );
    assert_eq!(
        row.spider_id, None,
        "the job must never be submitted to Spider"
    );
    Ok(())
}

/// A running job cancelled while the coordinator is down is recovered after the restart, and its
/// cancellation is relayed to Spider.
#[tokio::test]
#[ignore = "requires a running CLP package in Spider mode"]
async fn restart_recovers_cancel_requested_running_job() -> anyhow::Result<()> {
    let Some(coordinator) = CoordinatorContainer::from_env() else {
        println!("`CLP_TEST_COORDINATOR_CONTAINER` isn't set; skipping.");
        return Ok(());
    };
    let deployment = Deployment::connect().await?;
    let job_id = deployment.submit_slow().await?;
    let spider_job_id = deployment.wait_until_running(job_id).await?;

    coordinator.stop();
    let _guard = StartOnDrop(&coordinator);
    deployment.cancel(job_id).await?;
    assert_eq!(
        deployment.row(job_id).await?.status,
        QueryJobStatus::CancelRequested
    );
    coordinator.start();

    let row = deployment.wait_until_terminal(job_id).await?;
    assert_cancelled(&row);
    let spider_state = deployment.spider_state(spider_job_id).await?;
    assert!(
        matches!(spider_state, JobState::Cancelled | JobState::Succeeded),
        "Spider job {spider_job_id} is {spider_state:?}"
    );
    Ok(())
}
