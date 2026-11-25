//! Tests of the metadata database operations against a MariaDB or MySQL server.
//!
//! The tests are ignored by default. To run them, set `CLP_METADATA_DB_TEST_SERVER_URL` to the URL
//! of a server without a database (e.g. `mysql://user:password@127.0.0.1:3306`) whose user can
//! create and drop databases, and run `cargo test -p clp-metadata-db -- --ignored`. Each test
//! recreates its own database.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use clp_metadata_db::ClpDbClient;
use clp_metadata_db::Dataset;
use clp_metadata_db::DatasetConfig;
use clp_metadata_db::DatasetDescriptor;
use clp_metadata_db::DatasetLookup;
use clp_metadata_db::DatasetManagement;
use clp_metadata_db::Error;
use clp_rust_utils::clp_config::package::config::Database;
use clp_rust_utils::clp_config::package::config::MetadataTables;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::types::DatasetId;
use sqlx::MySqlPool;
use sqlx::mysql::MySqlPoolOptions;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

/// A freshly created database with the shared metadata tables, and a client of it.
struct TestDb {
    client: ClpDbClient,
    pool: MySqlPool,
    tables: MetadataTables,
}

impl TestDb {
    /// Recreates the database `clp_metadata_db_test_<name>` and creates the shared metadata tables
    /// in it.
    ///
    /// # Returns
    ///
    /// The database's client, pool and table names.
    ///
    /// # Panics
    ///
    /// Panics if `CLP_METADATA_DB_TEST_SERVER_URL` isn't set, or the database can't be recreated.
    async fn create(name: &str) -> Self {
        const MAX_CONNECTIONS: u32 = 32;

        let server_url = std::env::var("CLP_METADATA_DB_TEST_SERVER_URL")
            .expect("`CLP_METADATA_DB_TEST_SERVER_URL` should be set");
        let database = format!("clp_metadata_db_test_{name}");
        let server_pool = MySqlPoolOptions::new()
            .max_connections(1)
            .connect(&server_url)
            .await
            .expect("the test server should be reachable");
        for statement in [
            format!("DROP DATABASE IF EXISTS `{database}`"),
            format!("CREATE DATABASE `{database}`"),
        ] {
            sqlx::query(&statement)
                .execute(&server_pool)
                .await
                .expect("the test database should be recreated");
        }
        server_pool.close().await;

        let pool = MySqlPoolOptions::new()
            .max_connections(MAX_CONNECTIONS)
            .connect(&format!("{server_url}/{database}"))
            .await
            .expect("the test database should be reachable");
        let config = Database::default();
        let client = ClpDbClient::new(pool.clone(), &config);
        client
            .create_tables()
            .await
            .expect("the shared metadata tables should be created");
        Self {
            client,
            pool,
            tables: config.metadata_tables(),
        }
    }

    /// Creates the dataset described by `descriptor` in its own transaction.
    ///
    /// # Returns
    ///
    /// The dataset's ID.
    ///
    /// # Panics
    ///
    /// Panics if the dataset can't be created.
    async fn create_dataset(&self, descriptor: &DatasetDescriptor) -> DatasetId {
        let mut transaction = self
            .client
            .begin()
            .await
            .expect("a transaction should begin");
        let id = transaction
            .create_or_get_dataset(descriptor)
            .await
            .expect("the dataset should be created");
        transaction
            .commit()
            .await
            .expect("the transaction should commit");
        id
    }

    /// Marks the dataset named `name` as deleted in its own transaction.
    ///
    /// # Panics
    ///
    /// Panics if the dataset can't be marked.
    async fn mark_dataset_deleted(&self, name: &DatasetName) {
        let result = sqlx::query(&format!(
            "UPDATE `{}` SET `is_deleted` = TRUE WHERE `name` = ?",
            self.tables.datasets()
        ))
        .bind(name)
        .execute(&self.pool)
        .await
        .expect("the dataset should be marked as deleted");
        assert_eq!(result.rows_affected(), 1);
    }
}

/// # Returns
///
/// The dataset name `name`.
///
/// # Panics
///
/// Panics if `name` isn't a valid dataset name.
fn dataset_name(name: &str) -> DatasetName {
    name.parse()
        .expect("the name should be a valid dataset name")
}

/// # Returns
///
/// A descriptor of the dataset `name` with a storage path derived from the name and no retention
/// period.
///
/// # Panics
///
/// Panics if `name` isn't a valid dataset name.
fn dataset_descriptor(name: &str) -> DatasetDescriptor {
    DatasetDescriptor {
        name: dataset_name(name),
        config: DatasetConfig {
            archive_storage_path: format!("/var/data/archives/{name}"),
            retention_period_minutes: None,
        },
    }
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn create_tables_is_idempotent_and_leaves_the_tables_empty() -> anyhow::Result<()> {
    let test_db = TestDb::create("create_tables").await;
    test_db.client.create_tables().await?;
    assert_eq!(test_db.client.acquire().await?.get_datasets().await?, []);
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn create_or_get_dataset_returns_the_existing_id_without_using_up_ids() -> anyhow::Result<()>
{
    let test_db = TestDb::create("create_or_get_dataset_existing").await;
    let logs = dataset_descriptor("logs");
    let logs_id = test_db.create_dataset(&logs).await;
    for _ in 0..3 {
        assert_eq!(test_db.create_dataset(&logs).await, logs_id);
    }

    let metrics = dataset_descriptor("metrics");
    let metrics_id = test_db.create_dataset(&metrics).await;
    assert_eq!(metrics_id, DatasetId::from(2));
    assert_eq!(logs_id, DatasetId::from(1));

    let mut connection = test_db.client.acquire().await?;
    let expected_logs = Dataset {
        id: logs_id,
        name: logs.name.clone(),
        config: logs.config.clone(),
    };
    assert_eq!(
        connection.get_dataset_by_name(&logs.name).await?,
        Some(expected_logs.clone())
    );
    assert_eq!(
        connection.get_datasets().await?,
        [
            expected_logs,
            Dataset {
                id: metrics_id,
                name: metrics.name,
                config: metrics.config,
            },
        ]
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn create_or_get_dataset_stores_and_compares_every_setting() -> anyhow::Result<()> {
    let test_db = TestDb::create("create_or_get_dataset_mismatch").await;
    let stored = DatasetDescriptor {
        config: DatasetConfig {
            archive_storage_path: "s3-prefix/logs".to_owned(),
            retention_period_minutes: NonZeroU32::new(60),
        },
        ..dataset_descriptor("logs")
    };
    let id = test_db.create_dataset(&stored).await;

    for config in [
        DatasetConfig {
            archive_storage_path: "s3-prefix/other".to_owned(),
            ..stored.config.clone()
        },
        DatasetConfig {
            retention_period_minutes: None,
            ..stored.config.clone()
        },
        DatasetConfig {
            retention_period_minutes: NonZeroU32::new(61),
            ..stored.config.clone()
        },
    ] {
        let mut transaction = test_db.client.begin().await?;
        let error = transaction
            .create_or_get_dataset(&DatasetDescriptor {
                name: stored.name.clone(),
                config,
            })
            .await
            .expect_err("other settings should be rejected");
        assert!(
            matches!(
                &error,
                Error::DatasetConfigMismatch { existing } if *existing == stored.config
            ),
            "unexpected error: {error:?}"
        );
    }

    assert_eq!(
        test_db
            .client
            .acquire()
            .await?
            .get_dataset_by_name(&stored.name)
            .await?,
        Some(Dataset {
            id,
            name: stored.name,
            config: stored.config,
        })
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn create_or_get_dataset_rejects_a_deleted_dataset() -> anyhow::Result<()> {
    let test_db = TestDb::create("create_or_get_dataset_deleted").await;
    let logs = dataset_descriptor("logs");
    test_db.create_dataset(&logs).await;
    test_db.mark_dataset_deleted(&logs.name).await;

    let mut transaction = test_db.client.begin().await?;
    let error = transaction
        .create_or_get_dataset(&logs)
        .await
        .expect_err("a deleted dataset should be rejected");
    assert!(
        matches!(error, Error::DatasetDeleted),
        "unexpected error: {error:?}"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn create_or_get_dataset_sees_a_dataset_committed_after_the_transaction_snapshot()
-> anyhow::Result<()> {
    let test_db = TestDb::create("create_or_get_dataset_snapshot").await;
    let logs = dataset_descriptor("logs");

    let mut transaction = test_db.client.begin().await?;
    assert_eq!(transaction.get_dataset_by_name(&logs.name).await?, None);
    let id = test_db.create_dataset(&logs).await;
    assert_eq!(transaction.create_or_get_dataset(&logs).await?, id);
    transaction.commit().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn concurrent_create_or_get_dataset_converges_to_one_id() -> anyhow::Result<()> {
    const NUM_CONCURRENT_CALLS: usize = 16;
    const NUM_ITERATIONS: usize = 10;
    const JOIN_TIMEOUT: Duration = Duration::from_secs(30);

    let test_db = TestDb::create("concurrent_create_or_get_dataset").await;
    let client = Arc::new(test_db.client);
    for iteration in 0..NUM_ITERATIONS {
        let descriptor = Arc::new(dataset_descriptor(&format!("logs_{iteration}")));
        let barrier = Arc::new(Barrier::new(NUM_CONCURRENT_CALLS));
        let mut tasks = JoinSet::new();
        for _ in 0..NUM_CONCURRENT_CALLS {
            let (client, descriptor, barrier) = (
                Arc::clone(&client),
                Arc::clone(&descriptor),
                Arc::clone(&barrier),
            );
            tasks.spawn(async move {
                barrier.wait().await;
                let mut transaction = client.begin().await?;
                let id = transaction.create_or_get_dataset(&descriptor).await?;
                transaction.commit().await?;
                Ok::<_, Error>(id)
            });
        }
        let ids = tokio::time::timeout(JOIN_TIMEOUT, tasks.join_all())
            .await
            .expect("the concurrent creators should finish")
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        let expected_id = client
            .acquire()
            .await?
            .get_dataset_by_name(&descriptor.name)
            .await?
            .expect("the dataset should exist")
            .id;
        assert_eq!(
            ids,
            vec![expected_id; NUM_CONCURRENT_CALLS],
            "iteration {iteration}"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn lookups_and_statistics_skip_deleted_datasets() -> anyhow::Result<()> {
    let test_db = TestDb::create("lookups_skip_deleted_datasets").await;
    let live = dataset_descriptor("live");
    let deleted = dataset_descriptor("deleted");
    let live_id = test_db.create_dataset(&live).await;
    test_db.create_dataset(&deleted).await;
    test_db.mark_dataset_deleted(&deleted.name).await;

    let mut connection = test_db.client.acquire().await?;
    assert_eq!(
        connection
            .get_datasets()
            .await?
            .into_iter()
            .map(|dataset| dataset.id)
            .collect::<Vec<_>>(),
        [live_id]
    );
    assert_eq!(connection.get_dataset_by_name(&deleted.name).await?, None);
    assert_eq!(
        connection
            .get_dataset_ids_by_names(&[live.name.clone(), deleted.name.clone()])
            .await?,
        HashMap::from([(live.name, live_id)])
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn get_dataset_ids_by_names_resolves_names_across_batches() -> anyhow::Result<()> {
    const NUM_NAMES: usize = 2500;

    let test_db = TestDb::create("get_dataset_ids_by_names").await;
    let names: Vec<DatasetName> = (0..NUM_NAMES)
        .map(|index| dataset_name(&format!("dataset_{index}")))
        .collect();
    let mut expected_ids = HashMap::new();
    for index in [0, 999, 1000, NUM_NAMES - 1] {
        let id = test_db
            .create_dataset(&dataset_descriptor(names[index].as_str()))
            .await;
        expected_ids.insert(names[index].clone(), id);
    }

    assert_eq!(
        test_db
            .client
            .acquire()
            .await?
            .get_dataset_ids_by_names(&names)
            .await?,
        expected_ids
    );
    assert_eq!(
        test_db
            .client
            .acquire()
            .await?
            .get_dataset_ids_by_names(&[])
            .await?,
        HashMap::new()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn stored_dataset_rows_are_revalidated() -> anyhow::Result<()> {
    let test_db = TestDb::create("stored_dataset_rows_revalidated").await;
    let descriptor = dataset_descriptor("logs");
    let dataset_id = test_db.create_dataset(&descriptor).await;
    sqlx::query(&format!(
        "UPDATE `{}` SET `retention_period_minutes` = 0 WHERE `id` = ?",
        test_db.tables.datasets()
    ))
    .bind(dataset_id)
    .execute(&test_db.pool)
    .await?;

    let mut connection = test_db.client.acquire().await?;
    let error = connection
        .get_datasets()
        .await
        .expect_err("a zero stored retention period should be rejected");
    assert!(
        matches!(error, Error::InvalidStoredRetentionPeriod { dataset_id: id } if id == dataset_id),
        "unexpected error: {error:?}"
    );
    let error = connection
        .get_dataset_by_name(&descriptor.name)
        .await
        .expect_err("a zero stored retention period should be rejected");
    assert!(
        matches!(error, Error::InvalidStoredRetentionPeriod { dataset_id: id } if id == dataset_id),
        "unexpected error: {error:?}"
    );
    let mut transaction = test_db.client.begin().await?;
    let error = transaction
        .create_or_get_dataset(&descriptor)
        .await
        .expect_err("a zero stored retention period should be rejected");
    assert!(
        matches!(error, Error::InvalidStoredRetentionPeriod { dataset_id: id } if id == dataset_id),
        "unexpected error: {error:?}"
    );
    Ok(())
}
