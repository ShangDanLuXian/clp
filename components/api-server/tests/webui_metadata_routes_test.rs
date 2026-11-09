use std::str::FromStr;

use anyhow::Result;
use api_server::client::Client;
use api_server::client::ClientError;
use api_server::routes::AppState;
use api_server::webui_client::WebuiClient;
use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use clp_metadata_db::ColumnType;
use clp_metadata_db::DatasetId;
use clp_metadata_db::MetadataTables;
use clp_metadata_db::NewArchive;
use clp_metadata_db::NewDataset;
use clp_rust_utils::clp_config::package::config::Config;
use clp_rust_utils::clp_config::package::config::Package;
use clp_rust_utils::clp_config::package::config::StorageEngine;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::job_config::QUERY_JOBS_TABLE_NAME;
use clp_rust_utils::job_config::QUERY_TASKS_TABLE_NAME;
use clp_rust_utils::types::ArchiveId;
use http_body_util::BodyExt;
use serde_json::Value;
use serde_json::json;
use sqlx::Connection;
use sqlx::MySqlConnection;
use sqlx::mysql::MySqlConnectOptions;
use sqlx::mysql::MySqlPoolOptions;
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn time_range_covers_the_given_datasets_and_rejects_unknown_ones() -> Result<()> {
    let mut app = TestApp::create().await;
    let logs_id = app.create_dataset("logs").await;
    let metrics_id = app.create_dataset("metrics").await;
    app.insert_archives(logs_id, &[(100, 200, 1), (300, 400, 1)])
        .await;
    app.insert_archives(metrics_id, &[(50, 150, 1)]).await;

    assert_eq!(
        app.get("/metadata/time_range?datasets=logs,metrics")
            .await?,
        (
            StatusCode::OK,
            json!({"begin_timestamp": 50, "end_timestamp": 400})
        )
    );
    assert_eq!(
        app.get("/metadata/time_range?datasets=logs").await?,
        (
            StatusCode::OK,
            json!({"begin_timestamp": 100, "end_timestamp": 400})
        )
    );
    assert_eq!(
        app.get("/metadata/time_range?datasets=logs,missing")
            .await?,
        (StatusCode::NOT_FOUND, Value::Null)
    );

    let error = app
        .webui_client
        .get_time_range(&[
            parse_dataset_name("logs"),
            parse_dataset_name("second"),
            parse_dataset_name("first"),
        ])
        .await
        .expect_err("unknown datasets should be rejected");
    assert!(
        matches!(&error, ClientError::DatasetNotFound(dataset) if dataset.as_str() == "second"),
        "unexpected error: {error:?}"
    );

    app.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn query_speed_sums_the_given_datasets_archives_that_the_job_searched() -> Result<()> {
    let mut app = TestApp::create().await;
    let logs_id = app.create_dataset("logs").await;
    let metrics_id = app.create_dataset("metrics").await;
    let other_id = app.create_dataset("other").await;
    let logs_archive_ids = app
        .insert_archives(logs_id, &[(0, 0, 1000), (0, 0, 2000), (0, 0, 16_000)])
        .await;
    let metrics_archive_ids = app.insert_archives(metrics_id, &[(0, 0, 4000)]).await;
    let other_archive_ids = app.insert_archives(other_id, &[(0, 0, 8000)]).await;

    let finished_job_id = app
        .insert_query_job(
            Some(2.0),
            &[
                Some(logs_archive_ids[0].to_string()),
                Some(logs_archive_ids[1].to_string()),
                Some(metrics_archive_ids[0].to_string()),
                Some(other_archive_ids[0].to_string()),
                Some("not-a-uuid".to_owned()),
                None,
            ],
        )
        .await;
    let unfinished_job_id = app
        .insert_query_job(None, &[Some(logs_archive_ids[0].to_string())])
        .await;
    let other_dataset_job_id = app
        .insert_query_job(Some(1.0), &[Some(other_archive_ids[0].to_string())])
        .await;
    app.insert_query_job(Some(1.0), &[Some(logs_archive_ids[2].to_string())])
        .await;

    let query_speed = |datasets: &str, query_job_id: u64| {
        format!("/metadata/query_speed?datasets={datasets}&query_job_id={query_job_id}")
    };
    assert_eq!(
        app.get(&query_speed("logs,metrics", finished_job_id))
            .await?,
        (StatusCode::OK, json!({"bytes": 7000.0, "duration": 2.0}))
    );
    assert_eq!(
        app.get(&query_speed("logs", finished_job_id)).await?,
        (StatusCode::OK, json!({"bytes": 3000.0, "duration": 2.0}))
    );
    for (datasets, query_job_id) in [
        ("logs", unfinished_job_id),
        ("logs", other_dataset_job_id),
        ("logs", 1_000_000),
    ] {
        assert_eq!(
            app.get(&query_speed(datasets, query_job_id)).await?,
            (StatusCode::OK, Value::Null),
            "datasets: {datasets}, query job ID: {query_job_id}"
        );
    }
    assert_eq!(
        app.get(&query_speed("logs,missing", finished_job_id))
            .await?,
        (StatusCode::NOT_FOUND, Value::Null)
    );

    app.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn dataset_statistics_cover_the_live_datasets() -> Result<()> {
    let mut app = TestApp::create().await;
    assert_eq!(
        app.get("/metadata/ingestion_details").await?,
        (StatusCode::OK, Value::Null)
    );

    let logs_id = app.create_dataset("logs").await;
    let metrics_id = app.create_dataset("metrics").await;
    let deleted_id = app.create_dataset("deleted").await;
    app.insert_archives(logs_id, &[(100, 200, 1000)]).await;
    app.insert_archives(metrics_id, &[(50, 150, 2000)]).await;
    app.insert_archives(deleted_id, &[(0, 1000, 4000)]).await;
    app.tables
        .mark_dataset_deleted(&mut app.conn, deleted_id)
        .await?;

    assert_eq!(
        app.get("/metadata/datasets").await?,
        (StatusCode::OK, json!(["logs", "metrics"]))
    );
    assert_eq!(
        app.get("/metadata/ingestion_details").await?,
        (
            StatusCode::OK,
            json!({
                "begin_timestamp": 50,
                "end_timestamp": 200,
                "num_files": 0,
                "num_messages": 0,
            })
        )
    );
    assert_eq!(
        app.get("/metadata/space_savings").await?,
        (
            StatusCode::OK,
            json!({"total_uncompressed_size": 3000, "total_compressed_size": 30})
        )
    );

    app.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn timestamp_column_names_are_the_datasets_timestamp_columns() -> Result<()> {
    let mut app = TestApp::create().await;
    let logs_id = app.create_dataset("logs").await;
    let metrics_id = app.create_dataset("metrics").await;
    app.insert_column_metadata(logs_id, "ts", ColumnType::Timestamp)
        .await;
    app.insert_column_metadata(logs_id, "date", ColumnType::DeprecatedDateString)
        .await;
    app.insert_column_metadata(logs_id, "message", ColumnType::ClpString)
        .await;
    app.insert_column_metadata(metrics_id, "time", ColumnType::Timestamp)
        .await;

    assert_eq!(
        app.get("/metadata/column_metadata/logs/timestamp").await?,
        (StatusCode::OK, json!(["date", "ts"]))
    );
    assert_eq!(
        app.get("/metadata/column_metadata/missing/timestamp")
            .await?,
        (StatusCode::NOT_FOUND, Value::Null)
    );

    app.drop_database().await;
    Ok(())
}

/// The API server's routes on a uniquely named test database with the shared metadata tables and
/// the columns of the query job tables that the `WebUI` metadata routes read.
struct TestApp {
    router: axum::Router,
    webui_client: WebuiClient,
    conn: MySqlConnection,
    tables: MetadataTables,
    name: String,
}

impl TestApp {
    /// Factory function.
    ///
    /// Creates a uniquely named database on the server at `CLP_METADATA_DB_TEST_URL` (e.g.
    /// `mysql://root:password@127.0.0.1:3306`), creates its tables, and builds the API server's
    /// routes on it with a clp-s package config.
    ///
    /// # Returns
    ///
    /// The newly created test app.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// * `CLP_METADATA_DB_TEST_URL` isn't set or isn't a valid MySQL URL.
    /// * The database or its tables can't be created, or the database can't be connected to.
    /// * The `MongoDB` client or the routes can't be created.
    async fn create() -> Self {
        let server_url = std::env::var("CLP_METADATA_DB_TEST_URL")
            .expect("`CLP_METADATA_DB_TEST_URL` should be set to a MariaDB or MySQL server URL");
        let server_options = MySqlConnectOptions::from_str(&server_url)
            .expect("`CLP_METADATA_DB_TEST_URL` should be a valid MySQL URL");
        let name = format!("api_server_webui_metadata_test_{}", Uuid::new_v4().simple());

        let mut conn = MySqlConnection::connect_with(&server_options)
            .await
            .expect("failed to connect to the test server");
        sqlx::query(&format!("CREATE DATABASE `{name}`"))
            .execute(&mut conn)
            .await
            .expect("failed to create the test database");
        let options = server_options.database(&name);
        let mut conn = MySqlConnection::connect_with(&options)
            .await
            .expect("failed to connect to the test database");

        let config = Config {
            package: Package {
                storage_engine: StorageEngine::ClpS,
            },
            ..Config::default()
        };
        let tables = MetadataTables::new(&config.database);
        tables
            .create_tables(&mut conn)
            .await
            .expect("failed to create the shared metadata tables");
        for query in [
            format!(
                "CREATE TABLE `{QUERY_JOBS_TABLE_NAME}` (`id` INT NOT NULL AUTO_INCREMENT, \
                 `duration` FLOAT NULL DEFAULT NULL, PRIMARY KEY (`id`))"
            ),
            format!(
                "CREATE TABLE `{QUERY_TASKS_TABLE_NAME}` (`id` BIGINT NOT NULL AUTO_INCREMENT, \
                 `job_id` INT NOT NULL, `archive_id` VARCHAR(255) NULL DEFAULT NULL, PRIMARY KEY \
                 (`id`))"
            ),
        ] {
            sqlx::query(&query)
                .execute(&mut conn)
                .await
                .expect("failed to create a query job table");
        }

        let sql_pool = MySqlPoolOptions::new()
            .connect_with(options)
            .await
            .expect("failed to connect to the test database");
        let mongodb_client =
            mongodb::Client::with_uri_str("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=100")
                .await
                .expect("the MongoDB URI should be valid");
        let webui_client = Box::pin(WebuiClient::new(
            config.clone(),
            sql_pool.clone(),
            mongodb_client.clone(),
        ))
        .await;
        let router = api_server::routes::from_app_state(AppState {
            client: Client::new(config, mongodb_client, sql_pool),
            webui_client: webui_client.clone(),
        })
        .expect("failed to build the API server's routes");
        Self {
            router,
            webui_client,
            conn,
            tables,
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
    async fn create_dataset(&mut self, name: &str) -> DatasetId {
        self.tables
            .create_dataset(
                &mut self.conn,
                &NewDataset {
                    name: parse_dataset_name(name),
                    archive_storage_path: format!("/var/data/archives/{name}"),
                    retention_period_minutes: None,
                },
            )
            .await
            .expect("failed to create the dataset")
    }

    /// Inserts an archive into a dataset for each `(begin timestamp, end timestamp, uncompressed
    /// size)` in `archives`, with a compressed size of a hundredth of its uncompressed size.
    ///
    /// # Returns
    ///
    /// The inserted archives' IDs, in the order of `archives`.
    ///
    /// # Panics
    ///
    /// Panics if the archives can't be inserted.
    async fn insert_archives(
        &mut self,
        dataset_id: DatasetId,
        archives: &[(i64, i64, u64)],
    ) -> Vec<ArchiveId> {
        let archives: Vec<_> = archives
            .iter()
            .map(
                |&(begin_timestamp_millisecs, end_timestamp_millisecs, num_uncompressed_bytes)| {
                    NewArchive {
                        uuid: ArchiveId::from_bytes(*Uuid::new_v4().as_bytes()),
                        begin_timestamp_millisecs,
                        end_timestamp_millisecs,
                        num_uncompressed_bytes,
                        num_compressed_bytes: num_uncompressed_bytes / 100,
                    }
                },
            )
            .collect();
        self.tables
            .insert_archives(&mut self.conn, dataset_id, 1, &archives)
            .await
            .expect("failed to insert the archives");
        archives.into_iter().map(|archive| archive.uuid).collect()
    }

    /// Inserts a column into a dataset's column metadata.
    ///
    /// # Panics
    ///
    /// Panics if the column can't be inserted.
    async fn insert_column_metadata(
        &mut self,
        dataset_id: DatasetId,
        name: &str,
        column_type: ColumnType,
    ) {
        sqlx::query(&format!(
            "INSERT INTO `{}` (`dataset_id`, `name`, `type`) VALUES (?, ?, ?)",
            Config::default()
                .database
                .unscoped_column_metadata_table_name()
        ))
        .bind(dataset_id)
        .bind(name)
        .bind(column_type as u8)
        .execute(&mut self.conn)
        .await
        .expect("failed to insert the column metadata");
    }

    /// Inserts a query job with the given duration, and a query task for each of `archive_ids`.
    ///
    /// # Returns
    ///
    /// The inserted job's ID.
    ///
    /// # Panics
    ///
    /// Panics if the job or its tasks can't be inserted.
    async fn insert_query_job(
        &mut self,
        duration: Option<f32>,
        archive_ids: &[Option<String>],
    ) -> u64 {
        let job_id = sqlx::query(&format!(
            "INSERT INTO `{QUERY_JOBS_TABLE_NAME}` (`duration`) VALUES (?)"
        ))
        .bind(duration)
        .execute(&mut self.conn)
        .await
        .expect("failed to insert the query job")
        .last_insert_id();
        for archive_id in archive_ids {
            sqlx::query(&format!(
                "INSERT INTO `{QUERY_TASKS_TABLE_NAME}` (`job_id`, `archive_id`) VALUES (?, ?)"
            ))
            .bind(job_id)
            .bind(archive_id)
            .execute(&mut self.conn)
            .await
            .expect("failed to insert the query task");
        }
        job_id
    }

    /// Sends a `GET` request to the test app's router.
    ///
    /// # Returns
    ///
    /// The response's status and body on success. The body is parsed as JSON, or is `null` if it's
    /// empty.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`axum::http::request::Builder::body`]'s return values on failure.
    /// * Forwards [`tower::ServiceExt::oneshot`]'s return values on failure.
    /// * Forwards [`http_body_util::BodyExt::collect`]'s return values on failure.
    /// * Forwards [`serde_json::from_slice`]'s return values on failure.
    async fn get(&self, uri: &str) -> Result<(StatusCode, Value)> {
        let response = self
            .router
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty())?)
            .await?;
        let status = response.status();
        let bytes = response.into_body().collect().await?.to_bytes();
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)?
        };
        Ok((status, body))
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

/// # Returns
///
/// The dataset name `name`.
///
/// # Panics
///
/// Panics if `name` isn't a valid dataset name.
fn parse_dataset_name(name: &str) -> DatasetName {
    name.parse().expect("test dataset name should be valid")
}
