//! Tests of the metadata database operations against a MariaDB or MySQL server.
//!
//! The tests are ignored by default. To run them, set `CLP_METADATA_DB_TEST_SERVER_URL` to the URL
//! of a server without a database (e.g. `mysql://user:password@127.0.0.1:3306`) whose user can
//! create and drop databases, and run `cargo test -p clp-metadata-db -- --ignored`. Each test
//! recreates its own database.

use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use clp_metadata_db::ArchiveDescriptor;
use clp_metadata_db::ArchiveLookup;
use clp_metadata_db::ArchiveManagement;
use clp_metadata_db::ArchiveMetadata;
use clp_metadata_db::ArchiveTimestampRange;
use clp_metadata_db::ClpDbClient;
use clp_metadata_db::ColumnType;
use clp_metadata_db::Dataset;
use clp_metadata_db::DatasetConfig;
use clp_metadata_db::DatasetDescriptor;
use clp_metadata_db::DatasetLookup;
use clp_metadata_db::DatasetManagement;
use clp_metadata_db::Error;
use clp_metadata_db::MetadataStatistics;
use clp_metadata_db::QueryArchive;
use clp_metadata_db::SpaceSavings;
use clp_rust_utils::clp_config::package::config::Database;
use clp_rust_utils::clp_config::package::config::MetadataTables;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::job_config::CompressionJobId;
use clp_rust_utils::job_config::TimeRange;
use clp_rust_utils::types::ArchiveId;
use clp_rust_utils::types::ArchiveUuid;
use clp_rust_utils::types::DatasetId;
use sqlx::MySqlPool;
use sqlx::mysql::MySqlDatabaseError;
use sqlx::mysql::MySqlPoolOptions;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

const BASE_ARCHIVE: ArchiveDescriptor = ArchiveDescriptor {
    uuid: ArchiveUuid::from_bytes([0; 16]),
    timestamp_range: ArchiveTimestampRange::WITHOUT_TIMESTAMPS,
    num_uncompressed_bytes: 1000,
    num_compressed_bytes: 100,
};

const COMPRESSION_JOB_ID: CompressionJobId = 7;

const DAY_MILLISECS: i64 = 24 * 60 * 60 * 1000;

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

    /// Inserts `archives` into the dataset `dataset_id` in their own transaction.
    ///
    /// # Returns
    ///
    /// The IDs of the archives, in input order.
    ///
    /// # Panics
    ///
    /// Panics if the archives can't be inserted, or their UUIDs don't resolve to one ID each.
    async fn insert_archives(
        &self,
        dataset_id: DatasetId,
        archives: &[ArchiveDescriptor],
    ) -> Vec<ArchiveId> {
        let mut transaction = self
            .client
            .begin()
            .await
            .expect("a transaction should begin");
        transaction
            .insert_archives(dataset_id, COMPRESSION_JOB_ID, archives)
            .await
            .expect("the archives should be inserted");
        transaction
            .commit()
            .await
            .expect("the transaction should commit");
        let uuids: Vec<ArchiveUuid> = archives.iter().map(|archive| archive.uuid).collect();
        self.client
            .acquire()
            .await
            .expect("a connection should be acquired")
            .find_archive_ids_by_uuids(dataset_id, &uuids)
            .await
            .expect("the archive UUIDs should resolve")
            .into_iter()
            .map(|id| id.expect("every inserted archive should have an ID"))
            .collect()
    }

    /// Marks the dataset named `name` as deleted in its own transaction.
    ///
    /// # Panics
    ///
    /// Panics if the dataset can't be marked.
    async fn mark_dataset_deleted(&self, name: &DatasetName) {
        let mut transaction = self
            .client
            .begin()
            .await
            .expect("a transaction should begin");
        transaction
            .mark_dataset_deleted(name)
            .await
            .expect("the dataset should be marked as deleted");
        transaction
            .commit()
            .await
            .expect("the transaction should commit");
    }

    /// # Returns
    ///
    /// The long-span archives rows of the dataset `dataset_id`, ordered by archive ID.
    ///
    /// # Panics
    ///
    /// Panics if the rows can't be read.
    async fn long_span_archives(&self, dataset_id: DatasetId) -> Vec<LongSpanArchiveRow> {
        sqlx::query_as::<_, LongSpanArchiveRow>(&format!(
            "SELECT `archive_id`, `begin_timestamp_millisecs`, `end_timestamp_millisecs` FROM \
             `{}` WHERE `dataset_id` = ? ORDER BY `archive_id`",
            self.tables.long_span_archives()
        ))
        .bind(dataset_id)
        .fetch_all(&self.pool)
        .await
        .expect("the long-span archives rows should be read")
    }

    /// # Returns
    ///
    /// The number of archives rows of the dataset `dataset_id`.
    ///
    /// # Panics
    ///
    /// Panics if the rows can't be counted.
    async fn num_archives(&self, dataset_id: DatasetId) -> i64 {
        sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM `{}` WHERE `dataset_id` = ?",
            self.tables.archives()
        ))
        .bind(dataset_id)
        .fetch_one(&self.pool)
        .await
        .expect("the archives rows should be counted")
    }
}

/// A row of the long-span archives table.
#[derive(Debug, Eq, Hash, PartialEq, sqlx::FromRow)]
struct LongSpanArchiveRow {
    archive_id: ArchiveId,
    begin_timestamp_millisecs: i64,
    end_timestamp_millisecs: i64,
}

/// The columns of an archives row that the lookups don't return.
#[derive(sqlx::FromRow)]
struct StoredArchiveRow {
    num_uncompressed_bytes: u64,
    num_compressed_bytes: u64,
    compression_job_id: CompressionJobId,
    creation_timestamp_millisecs: i64,
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

/// # Returns
///
/// The archive UUID whose bytes encode `seed`.
fn archive_uuid(seed: u64) -> ArchiveUuid {
    ArchiveUuid::from_bytes(u128::from(seed).to_be_bytes())
}

/// # Returns
///
/// The archive timestamp range `[begin, end]`.
///
/// # Panics
///
/// Panics if `begin` exceeds `end`.
fn timestamp_range(begin: i64, end: i64) -> ArchiveTimestampRange {
    ArchiveTimestampRange::new(begin, end).expect("the range should be valid")
}

/// # Returns
///
/// The time range `[begin, end]`, where `None` leaves a side unbounded.
///
/// # Panics
///
/// Panics if `begin` exceeds `end`.
fn time_range(begin: Option<i64>, end: Option<i64>) -> TimeRange {
    TimeRange::new(begin, end).expect("the time range should be valid")
}

/// # Returns
///
/// Whether `error` is a MySQL error with the error number `number`.
fn is_mysql_error(error: &Error, number: u16) -> bool {
    matches!(
        error,
        Error::Sqlx(sqlx::Error::Database(database_error))
            if database_error
                .try_downcast_ref::<MySqlDatabaseError>()
                .is_some_and(|mysql_error| mysql_error.number() == number)
    )
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
async fn mark_dataset_deleted_reports_missing_and_already_deleted_datasets() -> anyhow::Result<()> {
    let test_db = TestDb::create("mark_dataset_deleted").await;
    let logs = dataset_descriptor("logs");
    let id = test_db.create_dataset(&logs).await;

    let mut transaction = test_db.client.begin().await?;
    let error = transaction
        .mark_dataset_deleted(&dataset_name("missing"))
        .await
        .expect_err("a missing dataset should be reported");
    assert!(
        matches!(error, Error::DatasetNotFound),
        "unexpected error: {error:?}"
    );
    assert_eq!(transaction.mark_dataset_deleted(&logs.name).await?, id);
    transaction.commit().await?;

    let mut transaction = test_db.client.begin().await?;
    let error = transaction
        .mark_dataset_deleted(&logs.name)
        .await
        .expect_err("a deleted dataset should be reported");
    assert!(
        matches!(error, Error::DatasetDeleted),
        "unexpected error: {error:?}"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn mark_dataset_deleted_sees_a_mark_committed_after_the_transaction_snapshot()
-> anyhow::Result<()> {
    let test_db = TestDb::create("mark_dataset_deleted_snapshot").await;
    let logs = dataset_descriptor("logs");
    test_db.create_dataset(&logs).await;

    let mut transaction = test_db.client.begin().await?;
    assert!(transaction.get_dataset_by_name(&logs.name).await?.is_some());
    test_db.mark_dataset_deleted(&logs.name).await;
    let error = transaction
        .mark_dataset_deleted(&logs.name)
        .await
        .expect_err("a dataset marked after the snapshot should be reported");
    assert!(
        matches!(error, Error::DatasetDeleted),
        "unexpected error: {error:?}"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn lookups_and_statistics_skip_deleted_datasets() -> anyhow::Result<()> {
    let test_db = TestDb::create("lookups_skip_deleted_datasets").await;
    let live = dataset_descriptor("live");
    let deleted = dataset_descriptor("deleted");
    let live_id = test_db.create_dataset(&live).await;
    let deleted_id = test_db.create_dataset(&deleted).await;
    let live_archive = ArchiveDescriptor {
        uuid: archive_uuid(1),
        timestamp_range: timestamp_range(10, 20),
        ..BASE_ARCHIVE
    };
    let deleted_archive = ArchiveDescriptor {
        uuid: archive_uuid(2),
        timestamp_range: timestamp_range(0, 30),
        ..BASE_ARCHIVE
    };
    let live_archive_ids = test_db
        .insert_archives(live_id, std::slice::from_ref(&live_archive))
        .await;
    let deleted_archive_ids = test_db
        .insert_archives(deleted_id, std::slice::from_ref(&deleted_archive))
        .await;
    sqlx::query(&format!(
        "INSERT INTO `{}` (`dataset_id`, `name`, `type`) VALUES (?, 'ts', ?)",
        test_db.tables.column_metadata()
    ))
    .bind(deleted_id)
    .bind(ColumnType::Timestamp)
    .execute(&test_db.pool)
    .await?;
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
    assert_eq!(
        connection
            .find_archives_for_query(deleted_id, &TimeRange::default(), None, 0)
            .await?,
        []
    );
    assert_eq!(
        connection
            .find_archives_by_time_range(deleted_id, &TimeRange::default())
            .await?,
        []
    );
    assert_eq!(
        connection
            .find_archives_by_ids(&[deleted_archive_ids[0], live_archive_ids[0]])
            .await?
            .into_iter()
            .map(|archive| archive.map(|archive| archive.id))
            .collect::<Vec<_>>(),
        [None, Some(live_archive_ids[0])]
    );
    assert_eq!(
        connection
            .find_archive_ids_by_uuids(deleted_id, &[deleted_archive.uuid])
            .await?,
        [None]
    );
    assert_eq!(
        connection
            .get_timestamp_bounds(&[live_id, deleted_id])
            .await?,
        Some(live_archive.timestamp_range)
    );
    assert_eq!(
        connection.get_space_savings().await?,
        SpaceSavings {
            num_uncompressed_bytes: BASE_ARCHIVE.num_uncompressed_bytes,
            num_compressed_bytes: BASE_ARCHIVE.num_compressed_bytes,
        }
    );
    assert_eq!(
        connection
            .get_column_names_by_types(deleted_id, &[ColumnType::Timestamp])
            .await?,
        Vec::<String>::new()
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
async fn insert_archives_stores_the_archives_and_their_long_span_references() -> anyhow::Result<()>
{
    let test_db = TestDb::create("insert_archives").await;
    let dataset_id = test_db.create_dataset(&dataset_descriptor("logs")).await;
    let archives = [
        ArchiveDescriptor {
            uuid: archive_uuid(1),
            timestamp_range: timestamp_range(-5, 5),
            num_uncompressed_bytes: u64::MAX,
            num_compressed_bytes: 1,
        },
        ArchiveDescriptor {
            uuid: archive_uuid(2),
            ..BASE_ARCHIVE
        },
        ArchiveDescriptor {
            uuid: archive_uuid(3),
            timestamp_range: timestamp_range(0, DAY_MILLISECS),
            ..BASE_ARCHIVE
        },
        ArchiveDescriptor {
            uuid: archive_uuid(4),
            timestamp_range: timestamp_range(0, DAY_MILLISECS + 1),
            ..BASE_ARCHIVE
        },
        ArchiveDescriptor {
            uuid: archive_uuid(5),
            timestamp_range: timestamp_range(i64::MIN, i64::MAX),
            ..BASE_ARCHIVE
        },
    ];
    let db_clock_query = "SELECT CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS SIGNED)";
    let before_millisecs: i64 = sqlx::query_scalar(db_clock_query)
        .fetch_one(&test_db.pool)
        .await?;
    let ids = test_db.insert_archives(dataset_id, &archives).await;
    let after_millisecs: i64 = sqlx::query_scalar(db_clock_query)
        .fetch_one(&test_db.pool)
        .await?;

    let mut connection = test_db.client.acquire().await?;
    let expected_archives: Vec<Option<ArchiveMetadata>> = ids
        .iter()
        .zip(&archives)
        .map(|(id, archive)| {
            Some(ArchiveMetadata {
                id: *id,
                dataset_id,
                uuid: archive.uuid,
                timestamp_range: archive.timestamp_range,
            })
        })
        .collect();
    assert_eq!(
        connection.find_archives_by_ids(&ids).await?,
        expected_archives
    );

    let stored_row = sqlx::query_as::<_, StoredArchiveRow>(&format!(
        "SELECT `num_uncompressed_bytes`, `num_compressed_bytes`, `compression_job_id`, \
         `creation_timestamp_millisecs` FROM `{}` WHERE `id` = ?",
        test_db.tables.archives()
    ))
    .bind(ids[0])
    .fetch_one(&test_db.pool)
    .await?;
    assert_eq!(stored_row.num_uncompressed_bytes, u64::MAX);
    assert_eq!(stored_row.num_compressed_bytes, 1);
    assert_eq!(stored_row.compression_job_id, COMPRESSION_JOB_ID);
    assert!(
        (before_millisecs..=after_millisecs).contains(&stored_row.creation_timestamp_millisecs),
        "the creation time {} should be within [{before_millisecs}, {after_millisecs}]",
        stored_row.creation_timestamp_millisecs
    );

    assert_eq!(
        test_db.long_span_archives(dataset_id).await,
        [
            LongSpanArchiveRow {
                archive_id: ids[3],
                begin_timestamp_millisecs: 0,
                end_timestamp_millisecs: DAY_MILLISECS + 1,
            },
            LongSpanArchiveRow {
                archive_id: ids[4],
                begin_timestamp_millisecs: i64::MIN,
                end_timestamp_millisecs: i64::MAX,
            },
        ]
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn insert_archives_keeps_long_span_references_aligned_across_batches() -> anyhow::Result<()> {
    const NUM_ARCHIVES: u64 = 2500;
    const LONG_SPAN_INDICES: [usize; 6] = [0, 998, 999, 1000, 2001, 2499];

    let test_db = TestDb::create("insert_archives_batches").await;
    let dataset_id = test_db.create_dataset(&dataset_descriptor("logs")).await;
    let archives: Vec<ArchiveDescriptor> = (0..NUM_ARCHIVES)
        .map(|seed| ArchiveDescriptor {
            uuid: archive_uuid(seed),
            timestamp_range: timestamp_range(0, 1),
            ..BASE_ARCHIVE
        })
        .enumerate()
        .map(|(index, archive)| {
            if LONG_SPAN_INDICES.contains(&index) {
                ArchiveDescriptor {
                    timestamp_range: timestamp_range(0, 2 * DAY_MILLISECS),
                    ..archive
                }
            } else {
                archive
            }
        })
        .collect();
    let ids = test_db.insert_archives(dataset_id, &archives).await;

    assert!(ids.is_sorted(), "archive IDs should follow the input order");
    assert_eq!(
        test_db.num_archives(dataset_id).await,
        i64::try_from(NUM_ARCHIVES)?
    );
    let expected_long_span_archives: HashSet<LongSpanArchiveRow> = LONG_SPAN_INDICES
        .iter()
        .map(|index| LongSpanArchiveRow {
            archive_id: ids[*index],
            begin_timestamp_millisecs: 0,
            end_timestamp_millisecs: 2 * DAY_MILLISECS,
        })
        .collect();
    let long_span_archives = test_db.long_span_archives(dataset_id).await;
    assert_eq!(long_span_archives.len(), LONG_SPAN_INDICES.len());
    let long_span_archives: HashSet<LongSpanArchiveRow> = long_span_archives.into_iter().collect();
    assert_eq!(long_span_archives, expected_long_span_archives);
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn insert_archives_rejects_missing_and_deleted_datasets() -> anyhow::Result<()> {
    let test_db = TestDb::create("insert_archives_rejected").await;
    let logs = dataset_descriptor("logs");
    let id = test_db.create_dataset(&logs).await;
    test_db.mark_dataset_deleted(&logs.name).await;
    let archive = ArchiveDescriptor {
        uuid: archive_uuid(1),
        ..BASE_ARCHIVE
    };

    let mut transaction = test_db.client.begin().await?;
    let error = transaction
        .insert_archives(
            DatasetId::from(42),
            COMPRESSION_JOB_ID,
            std::slice::from_ref(&archive),
        )
        .await
        .expect_err("a missing dataset should be rejected");
    assert!(
        matches!(error, Error::DatasetNotFound),
        "unexpected error: {error:?}"
    );
    let error = transaction
        .insert_archives(id, COMPRESSION_JOB_ID, &[archive])
        .await
        .expect_err("a deleted dataset should be rejected");
    assert!(
        matches!(error, Error::DatasetDeleted),
        "unexpected error: {error:?}"
    );
    transaction.commit().await?;

    assert_eq!(test_db.num_archives(id).await, 0);
    assert_eq!(test_db.num_archives(DatasetId::from(42)).await, 0);
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn insert_archives_sees_a_mark_committed_after_its_snapshot() -> anyhow::Result<()> {
    let test_db = TestDb::create("insert_archives_snapshot").await;
    let logs = dataset_descriptor("logs");
    let id = test_db.create_dataset(&logs).await;

    let mut transaction = test_db.client.begin().await?;
    assert!(transaction.get_dataset_by_name(&logs.name).await?.is_some());
    test_db.mark_dataset_deleted(&logs.name).await;
    let error = transaction
        .insert_archives(
            id,
            COMPRESSION_JOB_ID,
            &[ArchiveDescriptor {
                uuid: archive_uuid(1),
                ..BASE_ARCHIVE
            }],
        )
        .await
        .expect_err("a dataset marked after the snapshot should be rejected");
    assert!(
        matches!(error, Error::DatasetDeleted),
        "unexpected error: {error:?}"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn insert_archives_holds_off_mark_dataset_deleted_until_the_transaction_ends()
-> anyhow::Result<()> {
    const MYSQL_ER_LOCK_WAIT_TIMEOUT: u16 = 1205;

    let test_db = TestDb::create("insert_archives_lock").await;
    let logs = dataset_descriptor("logs");
    let id = test_db.create_dataset(&logs).await;
    let archive = ArchiveDescriptor {
        uuid: archive_uuid(1),
        ..BASE_ARCHIVE
    };

    let mut inserting = test_db.client.begin().await?;
    inserting
        .insert_archives(id, COMPRESSION_JOB_ID, std::slice::from_ref(&archive))
        .await?;

    let mut marking = test_db.client.begin().await?;
    sqlx::query("SET SESSION innodb_lock_wait_timeout = 1")
        .execute(&mut **marking.sqlx_transaction())
        .await?;
    let error = marking
        .mark_dataset_deleted(&logs.name)
        .await
        .expect_err("the mark should wait for the inserting transaction");
    assert!(
        is_mysql_error(&error, MYSQL_ER_LOCK_WAIT_TIMEOUT),
        "unexpected error: {error:?}"
    );
    marking.rollback().await?;

    inserting.commit().await?;
    test_db.mark_dataset_deleted(&logs.name).await;
    assert_eq!(test_db.num_archives(id).await, 1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn find_archives_for_query_applies_the_time_and_retention_filters() -> anyhow::Result<()> {
    const JOB_CREATION_TIMESTAMP_MILLISECS: i64 = 10 * 60 * 1000;

    let test_db = TestDb::create("find_archives_for_query").await;
    let dataset_id = test_db.create_dataset(&dataset_descriptor("logs")).await;
    let other_dataset_id = test_db.create_dataset(&dataset_descriptor("other")).await;
    let minute = 60 * 1000;
    let archives = [
        ArchiveDescriptor {
            uuid: archive_uuid(1),
            timestamp_range: timestamp_range(minute, 2 * minute),
            ..BASE_ARCHIVE
        },
        ArchiveDescriptor {
            uuid: archive_uuid(2),
            timestamp_range: timestamp_range(3 * minute, 5 * minute),
            num_uncompressed_bytes: 7,
            ..BASE_ARCHIVE
        },
        ArchiveDescriptor {
            uuid: archive_uuid(3),
            ..BASE_ARCHIVE
        },
        ArchiveDescriptor {
            uuid: archive_uuid(4),
            timestamp_range: timestamp_range(6 * minute, 9 * minute),
            ..BASE_ARCHIVE
        },
    ];
    let ids = test_db.insert_archives(dataset_id, &archives).await;
    test_db
        .insert_archives(
            other_dataset_id,
            &[ArchiveDescriptor {
                uuid: archive_uuid(5),
                timestamp_range: timestamp_range(minute, 9 * minute),
                ..BASE_ARCHIVE
            }],
        )
        .await;
    let query_archive = |index: usize| QueryArchive {
        id: ids[index],
        uuid: archives[index].uuid,
        num_uncompressed_bytes: archives[index].num_uncompressed_bytes,
        end_timestamp_millisecs: archives[index].timestamp_range.end_timestamp_millisecs(),
    };

    let mut connection = test_db.client.acquire().await?;
    for (time_range, retention_period_minutes, expected_indices) in [
        (TimeRange::default(), None, vec![3, 1, 0, 2]),
        (time_range(Some(5 * minute), None), None, vec![3, 1]),
        (time_range(None, Some(3 * minute)), None, vec![1, 0, 2]),
        (
            time_range(Some(2 * minute), Some(6 * minute)),
            None,
            vec![3, 1, 0],
        ),
        (time_range(Some(-minute), Some(0)), None, vec![2]),
        (TimeRange::default(), NonZeroU32::new(5), vec![3, 1, 2]),
        (TimeRange::default(), NonZeroU32::new(4), vec![3, 2]),
        (
            TimeRange::default(),
            NonZeroU32::new(u32::MAX),
            vec![3, 1, 0, 2],
        ),
    ] {
        assert_eq!(
            connection
                .find_archives_for_query(
                    dataset_id,
                    &time_range,
                    retention_period_minutes,
                    JOB_CREATION_TIMESTAMP_MILLISECS,
                )
                .await?,
            expected_indices
                .iter()
                .map(|index| query_archive(*index))
                .collect::<Vec<_>>(),
            "time range {time_range:?}, retention period {retention_period_minutes:?}"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn find_archives_by_time_range_selects_the_archives_within_the_range() -> anyhow::Result<()> {
    let test_db = TestDb::create("find_archives_by_time_range").await;
    let dataset_id = test_db.create_dataset(&dataset_descriptor("logs")).await;
    let other_dataset_id = test_db.create_dataset(&dataset_descriptor("other")).await;
    let archives = [
        ArchiveDescriptor {
            uuid: archive_uuid(1),
            timestamp_range: timestamp_range(10, 20),
            ..BASE_ARCHIVE
        },
        ArchiveDescriptor {
            uuid: archive_uuid(2),
            ..BASE_ARCHIVE
        },
        ArchiveDescriptor {
            uuid: archive_uuid(3),
            timestamp_range: timestamp_range(15, 30),
            ..BASE_ARCHIVE
        },
    ];
    let ids = test_db.insert_archives(dataset_id, &archives).await;
    test_db
        .insert_archives(
            other_dataset_id,
            &[ArchiveDescriptor {
                uuid: archive_uuid(4),
                timestamp_range: timestamp_range(10, 20),
                ..BASE_ARCHIVE
            }],
        )
        .await;
    let archive_metadata = |index: usize| ArchiveMetadata {
        id: ids[index],
        dataset_id,
        uuid: archives[index].uuid,
        timestamp_range: archives[index].timestamp_range,
    };

    let mut connection = test_db.client.acquire().await?;
    for (time_range, expected_indices) in [
        (TimeRange::default(), vec![1, 0, 2]),
        (time_range(Some(10), None), vec![0, 2]),
        (time_range(None, Some(20)), vec![1, 0]),
        (time_range(Some(10), Some(20)), vec![0]),
        (time_range(Some(0), Some(0)), vec![1]),
        (time_range(Some(11), Some(30)), vec![2]),
    ] {
        assert_eq!(
            connection
                .find_archives_by_time_range(dataset_id, &time_range)
                .await?,
            expected_indices
                .iter()
                .map(|index| archive_metadata(*index))
                .collect::<Vec<_>>(),
            "time range {time_range:?}"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn find_archives_by_ids_aligns_the_results_with_the_input_across_batches()
-> anyhow::Result<()> {
    const NUM_ARCHIVES: u64 = 1500;

    let test_db = TestDb::create("find_archives_by_ids").await;
    let first_dataset_id = test_db.create_dataset(&dataset_descriptor("first")).await;
    let second_dataset_id = test_db.create_dataset(&dataset_descriptor("second")).await;
    let archives: Vec<ArchiveDescriptor> = (0..NUM_ARCHIVES)
        .map(|seed| ArchiveDescriptor {
            uuid: archive_uuid(seed),
            timestamp_range: timestamp_range(0, i64::try_from(seed).expect("seed should fit")),
            ..BASE_ARCHIVE
        })
        .collect();
    let first_ids = test_db
        .insert_archives(first_dataset_id, &archives[..1])
        .await;
    let second_ids = test_db
        .insert_archives(second_dataset_id, &archives[1..])
        .await;
    let missing_id = ArchiveId::from(u64::MAX);

    let mut ids = vec![second_ids[1200], missing_id, first_ids[0], second_ids[1200]];
    ids.extend(&second_ids);
    ids.push(first_ids[0]);
    let expected: Vec<Option<ArchiveMetadata>> = ids
        .iter()
        .map(|id| {
            if *id == first_ids[0] {
                Some(ArchiveMetadata {
                    id: *id,
                    dataset_id: first_dataset_id,
                    uuid: archives[0].uuid,
                    timestamp_range: archives[0].timestamp_range,
                })
            } else {
                second_ids
                    .iter()
                    .position(|second_id| second_id == id)
                    .map(|index| ArchiveMetadata {
                        id: *id,
                        dataset_id: second_dataset_id,
                        uuid: archives[index + 1].uuid,
                        timestamp_range: archives[index + 1].timestamp_range,
                    })
            }
        })
        .collect();
    assert_eq!(expected[1], None);

    let mut connection = test_db.client.acquire().await?;
    assert_eq!(connection.find_archives_by_ids(&ids).await?, expected);
    assert_eq!(connection.find_archives_by_ids(&[]).await?, []);
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn find_archive_ids_by_uuids_aligns_the_results_and_rejects_ambiguous_uuids()
-> anyhow::Result<()> {
    let test_db = TestDb::create("find_archive_ids_by_uuids").await;
    let dataset_id = test_db.create_dataset(&dataset_descriptor("logs")).await;
    let other_dataset_id = test_db.create_dataset(&dataset_descriptor("other")).await;
    let archive = ArchiveDescriptor {
        uuid: archive_uuid(1),
        ..BASE_ARCHIVE
    };
    let other_archive = ArchiveDescriptor {
        uuid: archive_uuid(2),
        ..BASE_ARCHIVE
    };
    let ids = test_db
        .insert_archives(dataset_id, &[archive.clone(), other_archive.clone()])
        .await;
    test_db
        .insert_archives(other_dataset_id, std::slice::from_ref(&archive))
        .await;

    let mut connection = test_db.client.acquire().await?;
    let mut uuids = vec![other_archive.uuid, archive_uuid(3), archive.uuid];
    uuids.extend((10..1010).map(archive_uuid));
    uuids.push(other_archive.uuid);
    let mut expected = vec![Some(ids[1]), None, Some(ids[0])];
    expected.extend([None; 1000]);
    expected.push(Some(ids[1]));
    assert_eq!(
        connection
            .find_archive_ids_by_uuids(dataset_id, &uuids)
            .await?,
        expected
    );

    let mut transaction = test_db.client.begin().await?;
    transaction
        .insert_archives(
            dataset_id,
            COMPRESSION_JOB_ID,
            std::slice::from_ref(&archive),
        )
        .await?;
    let error = transaction
        .find_archive_ids_by_uuids(dataset_id, &[archive.uuid])
        .await
        .expect_err("a UUID of two archives should be rejected");
    assert!(
        matches!(error, Error::AmbiguousArchiveUuid { uuid } if uuid == archive.uuid),
        "unexpected error: {error:?}"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn delete_archives_removes_the_rows_and_reports_missing_ids_across_batches()
-> anyhow::Result<()> {
    const NUM_ARCHIVES: u64 = 1500;

    let test_db = TestDb::create("delete_archives").await;
    let dataset_id = test_db.create_dataset(&dataset_descriptor("logs")).await;
    let other_dataset_id = test_db.create_dataset(&dataset_descriptor("other")).await;
    let archives: Vec<ArchiveDescriptor> = (0..NUM_ARCHIVES)
        .map(|seed| ArchiveDescriptor {
            uuid: archive_uuid(seed),
            timestamp_range: if seed % 500 == 0 {
                timestamp_range(-DAY_MILLISECS, DAY_MILLISECS)
            } else {
                timestamp_range(0, 1)
            },
            ..BASE_ARCHIVE
        })
        .collect();
    let ids = test_db.insert_archives(dataset_id, &archives).await;
    let other_ids = test_db
        .insert_archives(
            other_dataset_id,
            &[ArchiveDescriptor {
                uuid: archive_uuid(NUM_ARCHIVES),
                timestamp_range: timestamp_range(0, 2 * DAY_MILLISECS),
                ..BASE_ARCHIVE
            }],
        )
        .await;
    let kept_id = ids[1];
    let missing_id = ArchiveId::from(u64::MAX);

    let mut deleted_ids: Vec<ArchiveId> = ids.iter().copied().filter(|id| *id != kept_id).collect();
    deleted_ids.insert(1, missing_id);
    deleted_ids.push(ids[0]);
    deleted_ids.push(missing_id);
    let mut transaction = test_db.client.begin().await?;
    assert_eq!(
        transaction.delete_archives(&deleted_ids).await?,
        [missing_id, missing_id]
    );
    transaction.commit().await?;

    assert_eq!(test_db.num_archives(dataset_id).await, 1);
    assert_eq!(test_db.long_span_archives(dataset_id).await, []);
    let mut connection = test_db.client.acquire().await?;
    assert_eq!(
        connection
            .find_archives_by_ids(&[kept_id, ids[0], other_ids[0]])
            .await?
            .into_iter()
            .map(|archive| archive.map(|archive| archive.id))
            .collect::<Vec<_>>(),
        [Some(kept_id), None, Some(other_ids[0])]
    );
    assert_eq!(test_db.long_span_archives(other_dataset_id).await.len(), 1);

    let mut transaction = test_db.client.begin().await?;
    assert_eq!(transaction.delete_archives(&[]).await?, []);
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn stored_archive_rows_are_revalidated() -> anyhow::Result<()> {
    let test_db = TestDb::create("stored_archive_rows_revalidated").await;
    let dataset_id = test_db.create_dataset(&dataset_descriptor("logs")).await;
    let ids = test_db
        .insert_archives(
            dataset_id,
            &[ArchiveDescriptor {
                uuid: archive_uuid(1),
                timestamp_range: timestamp_range(1, 2),
                ..BASE_ARCHIVE
            }],
        )
        .await;
    sqlx::query(&format!(
        "UPDATE `{}` SET `begin_timestamp_millisecs` = 3 WHERE `id` = ?",
        test_db.tables.archives()
    ))
    .bind(ids[0])
    .execute(&test_db.pool)
    .await?;

    let mut connection = test_db.client.acquire().await?;
    let error = connection
        .find_archives_by_ids(&ids)
        .await
        .expect_err("a reversed stored range should be rejected");
    assert!(
        matches!(&error, Error::InvalidArchiveTimestampRange(source)
            if *source == timestamp_range_error(3, 2)),
        "unexpected error: {error:?}"
    );
    let mut transaction = test_db.client.begin().await?;
    let error = transaction
        .delete_archives(&ids)
        .await
        .expect_err("a reversed stored range should be rejected");
    assert!(
        matches!(error, Error::InvalidArchiveTimestampRange(_)),
        "unexpected error: {error:?}"
    );
    Ok(())
}

/// # Returns
///
/// The error that [`ArchiveTimestampRange::new`] returns for `[begin, end]`.
///
/// # Panics
///
/// Panics if `[begin, end]` is a valid range.
fn timestamp_range_error(
    begin: i64,
    end: i64,
) -> clp_metadata_db::InvalidArchiveTimestampRangeError {
    ArchiveTimestampRange::new(begin, end).expect_err("the range should be invalid")
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

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn get_timestamp_bounds_covers_the_given_datasets_across_batches() -> anyhow::Result<()> {
    let test_db = TestDb::create("get_timestamp_bounds").await;
    let first_dataset_id = test_db.create_dataset(&dataset_descriptor("first")).await;
    let second_dataset_id = test_db.create_dataset(&dataset_descriptor("second")).await;
    let third_dataset_id = test_db.create_dataset(&dataset_descriptor("third")).await;
    test_db
        .insert_archives(
            first_dataset_id,
            &[
                ArchiveDescriptor {
                    uuid: archive_uuid(1),
                    timestamp_range: timestamp_range(10, 20),
                    ..BASE_ARCHIVE
                },
                ArchiveDescriptor {
                    uuid: archive_uuid(2),
                    timestamp_range: timestamp_range(15, 25),
                    ..BASE_ARCHIVE
                },
            ],
        )
        .await;
    test_db
        .insert_archives(
            second_dataset_id,
            &[ArchiveDescriptor {
                uuid: archive_uuid(3),
                timestamp_range: timestamp_range(30, 40),
                ..BASE_ARCHIVE
            }],
        )
        .await;
    test_db
        .insert_archives(
            third_dataset_id,
            &[ArchiveDescriptor {
                uuid: archive_uuid(4),
                ..BASE_ARCHIVE
            }],
        )
        .await;

    let mut connection = test_db.client.acquire().await?;
    assert_eq!(connection.get_timestamp_bounds(&[]).await?, None);
    assert_eq!(
        connection
            .get_timestamp_bounds(&[DatasetId::from(100)])
            .await?,
        None
    );
    assert_eq!(
        connection.get_timestamp_bounds(&[first_dataset_id]).await?,
        Some(timestamp_range(10, 25))
    );
    assert_eq!(
        connection
            .get_timestamp_bounds(&[first_dataset_id, third_dataset_id])
            .await?,
        Some(timestamp_range(0, 25))
    );

    for (first_index, second_index) in [(999, 1000), (1000, 999)] {
        let mut dataset_ids: Vec<DatasetId> = (100..2100).map(DatasetId::from).collect();
        dataset_ids[first_index] = first_dataset_id;
        dataset_ids[second_index] = second_dataset_id;
        assert_eq!(
            connection.get_timestamp_bounds(&dataset_ids).await?,
            Some(timestamp_range(10, 40))
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn get_space_savings_sums_the_sizes_of_every_dataset() -> anyhow::Result<()> {
    let test_db = TestDb::create("get_space_savings").await;
    let first_dataset_id = test_db.create_dataset(&dataset_descriptor("first")).await;
    let second_dataset_id = test_db.create_dataset(&dataset_descriptor("second")).await;

    let mut connection = test_db.client.acquire().await?;
    assert_eq!(
        connection.get_space_savings().await?,
        SpaceSavings {
            num_uncompressed_bytes: 0,
            num_compressed_bytes: 0,
        }
    );

    test_db
        .insert_archives(
            first_dataset_id,
            &[ArchiveDescriptor {
                uuid: archive_uuid(1),
                num_uncompressed_bytes: 1000,
                num_compressed_bytes: 10,
                ..BASE_ARCHIVE
            }],
        )
        .await;
    test_db
        .insert_archives(
            second_dataset_id,
            &[ArchiveDescriptor {
                uuid: archive_uuid(2),
                num_uncompressed_bytes: 3000,
                num_compressed_bytes: 30,
                ..BASE_ARCHIVE
            }],
        )
        .await;
    assert_eq!(
        connection.get_space_savings().await?,
        SpaceSavings {
            num_uncompressed_bytes: 4000,
            num_compressed_bytes: 40,
        }
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn get_column_names_by_types_selects_the_distinct_names_of_the_dataset() -> anyhow::Result<()>
{
    let test_db = TestDb::create("get_column_names_by_types").await;
    let dataset_id = test_db.create_dataset(&dataset_descriptor("logs")).await;
    let other_dataset_id = test_db.create_dataset(&dataset_descriptor("other")).await;
    for (id, name, column_type) in [
        (dataset_id, "z_ts", ColumnType::Timestamp),
        (dataset_id, "ts", ColumnType::Timestamp),
        (dataset_id, "ts", ColumnType::DeprecatedDateString),
        (dataset_id, "TS", ColumnType::DeprecatedDateString),
        (dataset_id, "count", ColumnType::Integer),
        (other_dataset_id, "other_ts", ColumnType::Timestamp),
    ] {
        sqlx::query(&format!(
            "INSERT INTO `{}` (`dataset_id`, `name`, `type`) VALUES (?, ?, ?)",
            test_db.tables.column_metadata()
        ))
        .bind(id)
        .bind(name)
        .bind(column_type)
        .execute(&test_db.pool)
        .await?;
    }

    let mut connection = test_db.client.acquire().await?;
    assert_eq!(
        connection
            .get_column_names_by_types(
                dataset_id,
                &[
                    ColumnType::Timestamp,
                    ColumnType::DeprecatedDateString,
                    ColumnType::Timestamp,
                ],
            )
            .await?,
        ["TS", "ts", "z_ts"]
    );
    assert_eq!(
        connection
            .get_column_names_by_types(dataset_id, &[ColumnType::Integer])
            .await?,
        ["count"]
    );
    assert_eq!(
        connection
            .get_column_names_by_types(dataset_id, &[])
            .await?,
        Vec::<String>::new()
    );
    Ok(())
}
