use std::str::FromStr;

use anyhow::Result;
use clp_rust_utils::database::mysql::query_job::get_earliest_creation_timestamp_millisecs;
use clp_rust_utils::job_config::QUERY_JOBS_TABLE_NAME;
use clp_rust_utils::job_config::QueryJobStatus;
use sqlx::Connection;
use sqlx::MySqlConnection;
use sqlx::mysql::MySqlConnectOptions;

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_earliest_creation_timestamp_millisecs_returns_the_earliest_among_the_statuses()
-> Result<()> {
    // 2026-01-01T00:00:01.500Z.
    const EARLIEST_MATCHING_CREATION_TIMESTAMP_MILLISECS: i64 = 1_767_225_601_500;

    let mut test_db = TestDatabase::create().await;
    for (status, creation_time) in [
        (QueryJobStatus::Pending, "2026-01-01 00:00:02.000"),
        (QueryJobStatus::Running, "2026-01-01 00:00:01.500"),
        (QueryJobStatus::Succeeded, "2026-01-01 00:00:00.000"),
    ] {
        test_db.insert_query_job(status, creation_time).await;
    }

    assert_eq!(
        get_earliest_creation_timestamp_millisecs(
            &mut test_db.conn,
            &[QueryJobStatus::Pending, QueryJobStatus::Running],
        )
        .await?,
        Some(EARLIEST_MATCHING_CREATION_TIMESTAMP_MILLISECS)
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_earliest_creation_timestamp_millisecs_returns_none_without_matching_jobs_or_statuses()
-> Result<()> {
    let mut test_db = TestDatabase::create().await;
    test_db
        .insert_query_job(QueryJobStatus::Succeeded, "2026-01-01 00:00:00.000")
        .await;

    for statuses in [vec![QueryJobStatus::Pending], Vec::new()] {
        assert_eq!(
            get_earliest_creation_timestamp_millisecs(&mut test_db.conn, &statuses).await?,
            None,
            "statuses {statuses:?}"
        );
    }

    test_db.drop_database().await;
    Ok(())
}

/// A uniquely named database with a query jobs table on the test server, used by a single test.
struct TestDatabase {
    conn: MySqlConnection,
    name: String,
}

impl TestDatabase {
    /// Factory function.
    ///
    /// Creates a uniquely named database on the server at `CLP_METADATA_DB_TEST_URL` (e.g.
    /// `mysql://root:password@127.0.0.1:3306`), connects to it in the UTC time zone, and creates
    /// the query jobs table's columns that the tests use.
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
    /// * The database or its query jobs table can't be created, or the database can't be connected
    ///   to.
    async fn create() -> Self {
        let server_url = std::env::var("CLP_METADATA_DB_TEST_URL")
            .expect("`CLP_METADATA_DB_TEST_URL` should be set to a MariaDB or MySQL server URL");
        let server_options = MySqlConnectOptions::from_str(&server_url)
            .expect("`CLP_METADATA_DB_TEST_URL` should be a valid MySQL URL");
        let name = format!("clp_rust_utils_test_{}", uuid::Uuid::new_v4().simple());

        let mut server_conn = MySqlConnection::connect_with(&server_options)
            .await
            .expect("failed to connect to the test server");
        sqlx::query(&format!("CREATE DATABASE `{name}`"))
            .execute(&mut server_conn)
            .await
            .expect("failed to create the test database");
        server_conn
            .close()
            .await
            .expect("failed to close the test server connection");

        let mut conn = MySqlConnection::connect_with(
            &server_options
                .database(&name)
                .timezone(Some("+00:00".to_owned())),
        )
        .await
        .expect("failed to connect to the test database");
        sqlx::query(&format!(
            r"
CREATE TABLE `{QUERY_JOBS_TABLE_NAME}` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `status` INT NOT NULL,
    `creation_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`)
)"
        ))
        .execute(&mut conn)
        .await
        .expect("failed to create the query jobs table");
        Self { conn, name }
    }

    /// Inserts a query job with the given status and creation time, in UTC.
    ///
    /// # Panics
    ///
    /// Panics if the query job can't be inserted.
    async fn insert_query_job(&mut self, status: QueryJobStatus, creation_time: &str) {
        sqlx::query(&format!(
            "INSERT INTO `{QUERY_JOBS_TABLE_NAME}` (`status`, `creation_time`) VALUES (?, ?)"
        ))
        .bind(status)
        .bind(creation_time)
        .execute(&mut self.conn)
        .await
        .expect("failed to insert a query job");
    }

    /// Drops the test database and closes the connection to it.
    ///
    /// # Panics
    ///
    /// Panics if the database can't be dropped or the connection can't be closed.
    async fn drop_database(mut self) {
        sqlx::query(&format!("DROP DATABASE `{}`", self.name))
            .execute(&mut self.conn)
            .await
            .expect("failed to drop the test database");
        self.conn
            .close()
            .await
            .expect("failed to close the test database connection");
    }
}
