use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use api_server::client::Client;
use api_server::routes::AppState;
use api_server::webui_client::WebuiClient;
use axum::body::Body;
use axum::http::Method;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use clp_metadata_db::MetadataTables;
use clp_metadata_db::NewArchive;
use clp_rust_utils::clp_config::package::config::ArchiveOutput;
use clp_rust_utils::clp_config::package::config::ArchiveOutputStorage;
use clp_rust_utils::clp_config::package::config::Config;
use clp_rust_utils::clp_config::package::config::Package;
use clp_rust_utils::clp_config::package::config::StorageEngine;
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

const ARCHIVE_ID: &str = "3f1c1a5e-5b6e-4a43-9d6b-0f2b2f5a8c11";

#[tokio::test]
async fn dataset_routes_reject_non_clp_s_storage_engine() -> Result<()> {
    let router = TestApp::lazy_router(Config {
        package: Package {
            storage_engine: StorageEngine::Clp,
        },
        ..clp_s_config()
    })
    .await;

    for (method, uri, body) in [
        (Method::GET, "/datasets", None),
        (Method::POST, "/datasets", Some(json!({"name": "logs"}))),
        (Method::DELETE, "/datasets/logs", None),
        (Method::GET, "/datasets/logs/archives", None),
        (
            Method::POST,
            "/datasets/logs/archives/delete",
            Some(json!({"archive_ids": [ARCHIVE_ID]})),
        ),
    ] {
        let (status, body) = send(&router, method.clone(), uri, body).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {uri}");
        assert!(
            body.as_str()
                .is_some_and(|body| body.contains("requires the clp-s storage engine")),
            "{method} {uri}: unexpected body: {body}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn dataset_routes_reject_invalid_dataset_names() -> Result<()> {
    let router = TestApp::lazy_router(clp_s_config()).await;

    let (status, _) = send(
        &router,
        Method::POST,
        "/datasets",
        Some(json!({"name": "bad-name"})),
    )
    .await?;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    let (status, _) = send(&router, Method::DELETE, "/datasets/bad-name", None).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn archive_routes_reject_begin_after_end() -> Result<()> {
    let router = TestApp::lazy_router(clp_s_config()).await;

    let (status, _) = send(
        &router,
        Method::GET,
        "/datasets/logs/archives?begin_timestamp_millisecs=2&end_timestamp_millisecs=1",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = send(
        &router,
        Method::POST,
        "/datasets/logs/archives/delete",
        Some(json!({"begin_timestamp_millisecs": 2, "end_timestamp_millisecs": 1})),
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn delete_archives_rejects_bodies_without_exactly_one_selection() -> Result<()> {
    let router = TestApp::lazy_router(clp_s_config()).await;

    for body in [
        json!({}),
        json!({"begin_timestamp_millisecs": 1}),
        json!({
            "archive_ids": [ARCHIVE_ID],
            "begin_timestamp_millisecs": 1,
            "end_timestamp_millisecs": 2,
        }),
    ] {
        let (status, _) = send(
            &router,
            Method::POST,
            "/datasets/logs/archives/delete",
            Some(body.clone()),
        )
        .await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "body: {body}");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn create_dataset_stores_the_dataset_with_its_storage_path_and_retention() -> Result<()> {
    let mut app = TestApp::create().await;

    for body in [
        json!({"name": "logs"}),
        json!({"name": "metrics", "retention_period_minutes": 60}),
    ] {
        let (status, _) = app.send(Method::POST, "/datasets", Some(body)).await?;
        assert_eq!(status, StatusCode::CREATED);
    }

    assert_eq!(
        app.send(Method::GET, "/datasets", None).await?,
        (StatusCode::OK, json!(["logs", "metrics"]))
    );
    let datasets: Vec<(String, String, Option<u32>)> = sqlx::query_as(
        "SELECT CONVERT(`name` USING utf8mb4), CONVERT(`archive_storage_path` USING utf8mb4), \
         `retention_period_minutes` FROM `clp_datasets` ORDER BY `name`",
    )
    .fetch_all(&mut app.conn)
    .await?;
    assert_eq!(
        datasets,
        vec![
            (
                "logs".to_owned(),
                "/var/data/archives/logs".to_owned(),
                None
            ),
            (
                "metrics".to_owned(),
                "/var/data/archives/metrics".to_owned(),
                Some(60)
            ),
        ]
    );

    app.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn create_dataset_returns_conflict_if_the_name_is_live_or_pending_deletion() -> Result<()> {
    let app = TestApp::create().await;
    let creation = json!({"name": "logs"});

    let (status, _) = app
        .send(Method::POST, "/datasets", Some(creation.clone()))
        .await?;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = app
        .send(Method::POST, "/datasets", Some(creation.clone()))
        .await?;
    assert_eq!(status, StatusCode::CONFLICT);

    let (status, _) = app.send(Method::DELETE, "/datasets/logs", None).await?;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (status, _) = app.send(Method::POST, "/datasets", Some(creation)).await?;
    assert_eq!(status, StatusCode::CONFLICT);

    app.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn delete_dataset_marks_only_a_live_dataset() -> Result<()> {
    let app = TestApp::create().await;
    app.create_dataset("logs").await;
    app.create_dataset("metrics").await;

    let (status, _) = app.send(Method::DELETE, "/datasets/logs", None).await?;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(
        app.send(Method::GET, "/datasets", None).await?,
        (StatusCode::OK, json!(["metrics"]))
    );

    for uri in ["/datasets/logs", "/datasets/missing"] {
        let (status, _) = app.send(Method::DELETE, uri, None).await?;
        assert_eq!(status, StatusCode::NOT_FOUND, "uri: {uri}");
    }

    app.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn list_archives_returns_the_archives_within_the_time_range() -> Result<()> {
    let mut app = TestApp::create().await;
    app.create_dataset("logs").await;
    let archive_ranges = [(-100, -50), (0, 0), (100, 200), (300, 400)];
    let archive_ids = app.insert_archives("logs", &archive_ranges).await;
    let expected_archives = |indices: &[usize]| {
        indices
            .iter()
            .map(|&idx| {
                json!({
                    "archive_id": archive_ids[idx].to_string(),
                    "begin_timestamp_millisecs": archive_ranges[idx].0,
                    "end_timestamp_millisecs": archive_ranges[idx].1,
                })
            })
            .collect::<Vec<_>>()
    };

    for (query, expected_indices) in [
        ("", vec![0, 1, 2, 3]),
        ("?begin_timestamp_millisecs=0", vec![1, 2, 3]),
        ("?end_timestamp_millisecs=200", vec![0, 1, 2]),
        (
            "?begin_timestamp_millisecs=100&end_timestamp_millisecs=200",
            vec![2],
        ),
    ] {
        let (status, body) = app
            .send(
                Method::GET,
                &format!("/datasets/logs/archives{query}"),
                None,
            )
            .await?;
        assert_eq!(status, StatusCode::OK, "query: {query}");
        assert_eq!(
            sorted_by_begin_timestamp(body),
            expected_archives(&expected_indices),
            "query: {query}"
        );
    }

    app.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn archive_routes_return_not_found_for_a_missing_or_marked_dataset() -> Result<()> {
    let app = TestApp::create().await;
    app.create_dataset("marked").await;
    let (status, _) = app.send(Method::DELETE, "/datasets/marked", None).await?;
    assert_eq!(status, StatusCode::ACCEPTED);

    for dataset in ["missing", "marked"] {
        let (status, body) = app
            .send(Method::GET, &format!("/datasets/{dataset}/archives"), None)
            .await?;
        assert_eq!(
            (status, body),
            (StatusCode::NOT_FOUND, Value::Null),
            "dataset: {dataset}"
        );
        let (status, body) = app
            .send(
                Method::POST,
                &format!("/datasets/{dataset}/archives/delete"),
                Some(json!({"archive_ids": [ARCHIVE_ID]})),
            )
            .await?;
        assert_eq!(
            (status, body),
            (StatusCode::NOT_FOUND, Value::Null),
            "dataset: {dataset}"
        );
    }

    app.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn delete_archives_by_ids_marks_the_archives() -> Result<()> {
    let mut app = TestApp::create().await;
    app.create_dataset("logs").await;
    let archive_ids = app
        .insert_archives("logs", &[(100, 200), (300, 400), (500, 600)])
        .await;

    let (status, body) = app
        .send(
            Method::POST,
            "/datasets/logs/archives/delete",
            Some(json!({"archive_ids": [archive_ids[0], archive_ids[1]]})),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        sorted_archive_ids(&body["archive_ids"]),
        sorted_archive_ids(&json!([archive_ids[0], archive_ids[1]]))
    );
    assert_eq!(
        app.list_archive_ids("logs").await?,
        vec![archive_ids[2].to_string()]
    );

    app.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn delete_archives_by_ids_marks_nothing_if_an_id_is_missing() -> Result<()> {
    let mut app = TestApp::create().await;
    app.create_dataset("logs").await;
    let archive_ids = app.insert_archives("logs", &[(100, 200), (300, 400)]).await;

    let (status, body) = app
        .send(
            Method::POST,
            "/datasets/logs/archives/delete",
            Some(json!({"archive_ids": [archive_ids[0], ARCHIVE_ID]})),
        )
        .await?;
    assert_eq!(
        (status, body),
        (StatusCode::NOT_FOUND, json!({"archive_ids": [ARCHIVE_ID]}))
    );
    let mut expected_ids: Vec<_> = archive_ids.iter().map(ToString::to_string).collect();
    expected_ids.sort();
    assert_eq!(app.list_archive_ids("logs").await?, expected_ids);

    app.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn delete_archives_by_time_range_marks_the_archives_within_it() -> Result<()> {
    let mut app = TestApp::create().await;
    app.create_dataset("logs").await;
    let archive_ids = app
        .insert_archives("logs", &[(100, 200), (150, 250), (300, 400)])
        .await;

    let (status, body) = app
        .send(
            Method::POST,
            "/datasets/logs/archives/delete",
            Some(json!({"begin_timestamp_millisecs": 100, "end_timestamp_millisecs": 250})),
        )
        .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        sorted_archive_ids(&body["archive_ids"]),
        sorted_archive_ids(&json!([archive_ids[0], archive_ids[1]]))
    );
    assert_eq!(
        app.list_archive_ids("logs").await?,
        vec![archive_ids[2].to_string()]
    );

    app.drop_database().await;
    Ok(())
}

/// The API server's routes on a uniquely named test database with the shared metadata tables.
struct TestApp {
    router: axum::Router,
    conn: MySqlConnection,
    tables: MetadataTables,
    name: String,
}

impl TestApp {
    /// Factory function.
    ///
    /// Creates a uniquely named database on the server at `CLP_METADATA_DB_TEST_URL` (e.g.
    /// `mysql://root:password@127.0.0.1:3306`), creates the shared metadata tables in it, and
    /// builds the API server's routes on it with [`clp_s_config`].
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
    async fn create() -> Self {
        let server_url = std::env::var("CLP_METADATA_DB_TEST_URL")
            .expect("`CLP_METADATA_DB_TEST_URL` should be set to a MariaDB or MySQL server URL");
        let server_options = MySqlConnectOptions::from_str(&server_url)
            .expect("`CLP_METADATA_DB_TEST_URL` should be a valid MySQL URL");
        let name = format!("api_server_datasets_test_{}", Uuid::new_v4().simple());

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

        let config = clp_s_config();
        let tables = MetadataTables::new(&config.database);
        tables
            .create_tables(&mut conn)
            .await
            .expect("failed to create the shared metadata tables");
        let sql_pool = MySqlPoolOptions::new()
            .connect_with(options)
            .await
            .expect("failed to connect to the test database");
        Self {
            router: Self::router(config, sql_pool).await,
            conn,
            tables,
            name,
        }
    }

    /// Builds the API server's routes with a `MySQL` connection pool that only connects on first
    /// use.
    ///
    /// # Returns
    ///
    /// The router.
    ///
    /// # Panics
    ///
    /// Panics if the connection pool can't be created.
    async fn lazy_router(config: Config) -> axum::Router {
        let sql_pool = MySqlPoolOptions::new()
            .acquire_timeout(Duration::from_secs(1))
            .connect_lazy("mysql://127.0.0.1:1/clp-db")
            .expect("the MySQL URI should be valid");
        Self::router(config, sql_pool).await
    }

    /// Sends a request to the test app's router.
    ///
    /// # Returns
    ///
    /// Forwards [`send`]'s return values.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`send`]'s return values on failure.
    async fn send(
        &self,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> Result<(StatusCode, Value)> {
        send(&self.router, method, uri, body).await
    }

    /// Creates a dataset through the dataset creation route.
    ///
    /// # Panics
    ///
    /// Panics if the dataset can't be created.
    async fn create_dataset(&self, name: &str) {
        let (status, body) = self
            .send(Method::POST, "/datasets", Some(json!({"name": name})))
            .await
            .expect("failed to send the dataset creation request");
        assert_eq!(status, StatusCode::CREATED, "body: {body}");
    }

    /// Inserts an archive into a dataset for each of the given timestamp ranges.
    ///
    /// # Returns
    ///
    /// The inserted archives' IDs, in the order of `timestamp_ranges`.
    ///
    /// # Panics
    ///
    /// Panics if the dataset doesn't exist or the archives can't be inserted.
    async fn insert_archives(
        &mut self,
        dataset: &str,
        timestamp_ranges: &[(i64, i64)],
    ) -> Vec<ArchiveId> {
        let dataset_id = self
            .tables
            .get_dataset_id_by_name(
                &mut self.conn,
                &dataset.parse().expect("dataset name should be valid"),
            )
            .await
            .expect("failed to get the dataset ID")
            .expect("the dataset should exist");
        let archives: Vec<_> = timestamp_ranges
            .iter()
            .map(
                |&(begin_timestamp_millisecs, end_timestamp_millisecs)| NewArchive {
                    uuid: ArchiveId::from_bytes(*Uuid::new_v4().as_bytes()),
                    begin_timestamp_millisecs,
                    end_timestamp_millisecs,
                    num_uncompressed_bytes: 1000,
                    num_compressed_bytes: 100,
                },
            )
            .collect();
        self.tables
            .insert_archives(&mut self.conn, dataset_id, 1, &archives)
            .await
            .expect("failed to insert the archives");
        archives.into_iter().map(|archive| archive.uuid).collect()
    }

    /// Lists a dataset's archives through the archive listing route.
    ///
    /// # Returns
    ///
    /// The archives' IDs, sorted, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::send`]'s return values on failure.
    async fn list_archive_ids(&self, dataset: &str) -> Result<Vec<String>> {
        let (status, body) = self
            .send(Method::GET, &format!("/datasets/{dataset}/archives"), None)
            .await?;
        assert_eq!(status, StatusCode::OK, "body: {body}");
        let archive_ids: Vec<_> = body
            .as_array()
            .expect("the archive listing should be an array")
            .iter()
            .map(|archive| archive["archive_id"].clone())
            .collect();
        Ok(sorted_archive_ids(&Value::Array(archive_ids)))
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

    /// Builds the API server's routes on the given `MySQL` connection pool.
    ///
    /// # Returns
    ///
    /// The router.
    ///
    /// # Panics
    ///
    /// Panics if the `MongoDB` client or the routes can't be created.
    async fn router(config: Config, sql_pool: sqlx::MySqlPool) -> axum::Router {
        let mongodb_client =
            mongodb::Client::with_uri_str("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=100")
                .await
                .expect("the MongoDB URI should be valid");
        api_server::routes::from_app_state(AppState {
            client: Client::new(config.clone(), mongodb_client.clone(), sql_pool.clone()),
            webui_client: Box::pin(WebuiClient::new(config, sql_pool, mongodb_client)).await,
        })
        .expect("failed to build the API server's routes")
    }
}

/// # Returns
///
/// A clp-s package config whose archives are stored under `/var/data/archives`.
fn clp_s_config() -> Config {
    Config {
        package: Package {
            storage_engine: StorageEngine::ClpS,
        },
        archive_output: ArchiveOutput {
            storage: ArchiveOutputStorage::Fs {
                directory: "/var/data/archives".to_owned(),
            },
            ..ArchiveOutput::default()
        },
        ..Config::default()
    }
}

/// Sends a request to `router`, with `body` as its JSON body if given.
///
/// # Returns
///
/// The response's status and body on success. The body is parsed as JSON, or kept as a JSON string
/// if it isn't JSON, or is `null` if it's empty.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`axum::http::request::Builder::body`]'s return values on failure.
/// * Forwards [`tower::ServiceExt::oneshot`]'s return values on failure.
/// * Forwards [`http_body_util::BodyExt::collect`]'s return values on failure.
async fn send(
    router: &axum::Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> Result<(StatusCode, Value)> {
    let request = Request::builder().method(method).uri(uri);
    let request = match body {
        Some(body) => request
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))?,
        None => request.body(Body::empty())?,
    };
    let response = router.clone().oneshot(request).await?;
    let status = response.status();
    let bytes = response.into_body().collect().await?.to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
    };
    Ok((status, body))
}

/// # Returns
///
/// The archive listing in `body`, sorted by the archives' begin timestamps.
///
/// # Panics
///
/// Panics if `body` isn't an array of archives.
fn sorted_by_begin_timestamp(body: Value) -> Vec<Value> {
    let Value::Array(mut archives) = body else {
        panic!("the archive listing should be an array, got {body}");
    };
    archives.sort_by_key(|archive| {
        archive["begin_timestamp_millisecs"]
            .as_i64()
            .expect("an archive should have an integer begin timestamp")
    });
    archives
}

/// # Returns
///
/// The archive IDs in `archive_ids`, sorted.
///
/// # Panics
///
/// Panics if `archive_ids` isn't an array of strings.
fn sorted_archive_ids(archive_ids: &Value) -> Vec<String> {
    let mut archive_ids: Vec<String> = archive_ids
        .as_array()
        .expect("the archive IDs should be an array")
        .iter()
        .map(|id| {
            id.as_str()
                .expect("an archive ID should be a string")
                .to_owned()
        })
        .collect();
    archive_ids.sort();
    archive_ids
}
