use std::collections::HashMap;
use std::num::NonZeroU32;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use aws_sdk_s3::Client;
use clp_metadata_db::DatasetId;
use clp_metadata_db::MetadataTables;
use clp_metadata_db::NewArchive;
use clp_metadata_db::NewDataset;
use clp_rust_utils::clp_config::AwsAuthentication;
use clp_rust_utils::clp_config::AwsCredentials;
use clp_rust_utils::clp_config::S3Config;
use clp_rust_utils::clp_config::package::config::ArchiveOutput;
use clp_rust_utils::clp_config::package::config::ArchiveOutputStorage;
use clp_rust_utils::clp_config::package::config::Config;
use clp_rust_utils::clp_config::package::config::Package;
use clp_rust_utils::clp_config::package::config::StorageEngine;
use clp_rust_utils::job_config::QUERY_JOBS_TABLE_NAME;
use clp_rust_utils::job_config::QueryJobStatus;
use clp_rust_utils::types::ArchiveId;
use clp_rust_utils::types::non_empty_string::ExpectedNonEmpty;
use garbage_collector::ArchiveGarbageCollector;
use garbage_collector::Error;
use non_empty_string::NonEmptyString;
use sqlx::Connection;
use sqlx::MySqlConnection;
use sqlx::mysql::MySqlConnectOptions;
use sqlx::mysql::MySqlPoolOptions;
use uuid::Uuid;

#[tokio::test]
async fn from_config_rejects_storage_engines_other_than_clp_s() -> Result<()> {
    let db_pool = MySqlPoolOptions::new().connect_lazy("mysql://127.0.0.1:1/clp-db")?;
    let config = Config {
        package: Package {
            storage_engine: StorageEngine::Clp,
        },
        ..Config::default()
    };

    let result = ArchiveGarbageCollector::from_config(&config, db_pool).await;

    assert!(
        matches!(result, Err(Error::UnsupportedStorageEngine)),
        "expected `Error::UnsupportedStorageEngine`"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn sweep_cleans_up_expired_archives_and_keeps_the_others() -> Result<()> {
    let archive_directory = tempfile::tempdir()?;
    let mut env = TestEnv::create(fs_storage(archive_directory.path())).await;
    let retained = NewDataset {
        retention_period_minutes: NonZeroU32::new(60),
        ..env.new_dataset("retained")
    };
    let retained_id = env.create_dataset(&retained).await;
    let unlimited = env.new_dataset("unlimited");
    let unlimited_id = env.create_dataset(&unlimited).await;
    let expired = env.insert_archive(retained_id).await;
    env.backdate_archive(expired, 90).await;
    let unexpired = env.insert_archive(retained_id).await;
    env.backdate_archive(unexpired, 30).await;
    let unlimited_archive = env.insert_archive(unlimited_id).await;
    env.backdate_archive(unlimited_archive, 90).await;
    for (dataset, uuid) in [
        (&retained, expired),
        (&retained, unexpired),
        (&unlimited, unlimited_archive),
    ] {
        create_directory_archive(&archive_path(dataset, uuid));
    }

    env.sweep().await?;

    assert_eq!(
        env.get_archive_states().await,
        HashMap::from([(unexpired, false), (unlimited_archive, false)])
    );
    assert!(!archive_path(&retained, expired).exists());
    assert!(archive_path(&retained, unexpired).exists());
    assert!(archive_path(&unlimited, unlimited_archive).exists());

    env.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn non_terminal_query_jobs_hold_back_expiry() -> Result<()> {
    let archive_directory = tempfile::tempdir()?;
    let mut env = TestEnv::create(fs_storage(archive_directory.path())).await;
    let dataset = NewDataset {
        retention_period_minutes: NonZeroU32::new(60),
        ..env.new_dataset("logs")
    };
    let dataset_id = env.create_dataset(&dataset).await;
    let archive = env.insert_archive(dataset_id).await;
    env.backdate_archive(archive, 90).await;
    create_directory_archive(&archive_path(&dataset, archive));
    let query_job_id = env.insert_query_job(QueryJobStatus::Pending, 45).await;

    for status in [
        QueryJobStatus::Pending,
        QueryJobStatus::Running,
        QueryJobStatus::CancelRequested,
        QueryJobStatus::Cancelling,
    ] {
        env.set_query_job_status(query_job_id, status).await;

        env.sweep().await?;

        assert_eq!(
            env.get_archive_states().await,
            HashMap::from([(archive, false)]),
            "status {status:?}"
        );
        assert!(
            archive_path(&dataset, archive).exists(),
            "status {status:?}"
        );
    }

    env.set_query_job_status(query_job_id, QueryJobStatus::Succeeded)
        .await;
    env.sweep().await?;

    assert_eq!(env.get_archive_states().await, HashMap::new());
    assert!(!archive_path(&dataset, archive).exists());

    env.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn sweep_cleans_up_marked_archives_stored_as_directories_or_files() -> Result<()> {
    let archive_directory = tempfile::tempdir()?;
    let mut env = TestEnv::create(fs_storage(archive_directory.path())).await;
    let dataset = env.new_dataset("logs");
    let dataset_id = env.create_dataset(&dataset).await;
    let directory_archive = env.insert_archive(dataset_id).await;
    create_directory_archive(&archive_path(&dataset, directory_archive));
    let file_archive = env.insert_archive(dataset_id).await;
    create_file_archive(&archive_path(&dataset, file_archive));
    let live_archive = env.insert_archive(dataset_id).await;
    create_directory_archive(&archive_path(&dataset, live_archive));
    env.mark_archive(dataset_id, directory_archive).await;
    env.mark_archive(dataset_id, file_archive).await;

    env.sweep().await?;

    assert_eq!(
        env.get_archive_states().await,
        HashMap::from([(live_archive, false)])
    );
    assert!(!archive_path(&dataset, directory_archive).exists());
    assert!(!archive_path(&dataset, file_archive).exists());
    assert!(archive_path(&dataset, live_archive).exists());

    env.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn sweep_cleans_up_a_marked_dataset_and_frees_its_name() -> Result<()> {
    let archive_directory = tempfile::tempdir()?;
    let mut env = TestEnv::create(fs_storage(archive_directory.path())).await;
    let marked = env.new_dataset("logs");
    let marked_id = env.create_dataset(&marked).await;
    let marked_archive = env.insert_archive(marked_id).await;
    create_directory_archive(&archive_path(&marked, marked_archive));
    let live = env.new_dataset("logs_other");
    let live_id = env.create_dataset(&live).await;
    let live_archive = env.insert_archive(live_id).await;
    create_directory_archive(&archive_path(&live, live_archive));
    env.mark_dataset(marked_id).await;

    env.sweep().await?;

    assert_eq!(
        env.get_dataset_states().await,
        HashMap::from([("logs_other".to_owned(), false)])
    );
    assert_eq!(
        env.get_archive_states().await,
        HashMap::from([(live_archive, false)])
    );
    assert!(!Path::new(&marked.archive_storage_path).exists());
    assert!(archive_path(&live, live_archive).exists());
    env.create_dataset(&marked).await;

    env.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn sweep_reclaims_marked_rows_whose_storage_is_already_gone() -> Result<()> {
    let archive_directory = tempfile::tempdir()?;
    let mut env = TestEnv::create(fs_storage(archive_directory.path())).await;
    let marked = env.new_dataset("marked");
    let marked_id = env.create_dataset(&marked).await;
    env.insert_archive(marked_id).await;
    env.mark_dataset(marked_id).await;
    let live = env.new_dataset("live");
    let live_id = env.create_dataset(&live).await;
    let marked_archive = env.insert_archive(live_id).await;
    env.mark_archive(live_id, marked_archive).await;

    env.sweep().await?;

    assert_eq!(
        env.get_dataset_states().await,
        HashMap::from([("live".to_owned(), false)])
    );
    assert_eq!(env.get_archive_states().await, HashMap::new());

    env.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn storage_failures_leave_rows_marked_without_blocking_the_others() -> Result<()> {
    let archive_directory = tempfile::tempdir()?;
    let mut env = TestEnv::create(fs_storage(archive_directory.path())).await;
    let file = archive_directory.path().join("file");
    std::fs::write(&file, b"content")?;
    let unreachable_dataset = NewDataset {
        archive_storage_path: file.join("unreachable").to_string_lossy().into_owned(),
        ..env.new_dataset("unreachable")
    };
    let unreachable_dataset_id = env.create_dataset(&unreachable_dataset).await;
    env.mark_dataset(unreachable_dataset_id).await;
    let unreachable_archives = NewDataset {
        archive_storage_path: file
            .join("unreachable_archives")
            .to_string_lossy()
            .into_owned(),
        ..env.new_dataset("unreachable_archives")
    };
    let unreachable_archives_id = env.create_dataset(&unreachable_archives).await;
    let unreachable_archive = env.insert_archive(unreachable_archives_id).await;
    env.mark_archive(unreachable_archives_id, unreachable_archive)
        .await;
    let reachable_dataset = env.new_dataset("reachable");
    let reachable_dataset_id = env.create_dataset(&reachable_dataset).await;
    let reachable_dataset_archive = env.insert_archive(reachable_dataset_id).await;
    create_directory_archive(&archive_path(&reachable_dataset, reachable_dataset_archive));
    env.mark_dataset(reachable_dataset_id).await;
    let reachable_archives = env.new_dataset("reachable_archives");
    let reachable_archives_id = env.create_dataset(&reachable_archives).await;
    let reachable_archive = env.insert_archive(reachable_archives_id).await;
    create_directory_archive(&archive_path(&reachable_archives, reachable_archive));
    env.mark_archive(reachable_archives_id, reachable_archive)
        .await;

    env.sweep().await?;

    assert_eq!(
        env.get_dataset_states().await,
        HashMap::from([
            ("unreachable".to_owned(), true),
            ("unreachable_archives".to_owned(), false),
            ("reachable_archives".to_owned(), false),
        ])
    );
    assert_eq!(
        env.get_archive_states().await,
        HashMap::from([(unreachable_archive, true)])
    );
    assert!(!Path::new(&reachable_dataset.archive_storage_path).exists());
    assert!(!archive_path(&reachable_archives, reachable_archive).exists());

    env.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server and LocalStack or AWS environment; set \
            CLP_METADATA_DB_TEST_URL and AWS_ENDPOINT_URL"]
async fn sweep_cleans_up_marked_datasets_and_archives_on_s3() -> Result<()> {
    let bucket = TestBucket::create().await;
    let mut env = TestEnv::create(ArchiveOutputStorage::S3 {
        staging_directory: "var/data/staged-archives".to_owned(),
        s3_config: bucket.s3_config.clone(),
    })
    .await;
    let marked = env.new_dataset("ds");
    let marked_id = env.create_dataset(&marked).await;
    let marked_dataset_archive = env.insert_archive(marked_id).await;
    env.mark_dataset(marked_id).await;
    let live = env.new_dataset("ds_other");
    let live_id = env.create_dataset(&live).await;
    let marked_archive = env.insert_archive(live_id).await;
    env.mark_archive(live_id, marked_archive).await;
    let live_archive = env.insert_archive(live_id).await;
    let kept_keys = vec![
        object_key(&live, live_archive),
        format!("{}-unrelated", marked.archive_storage_path),
    ];
    bucket
        .put_objects(
            [
                object_key(&marked, marked_dataset_archive),
                format!("{}/leftover", marked.archive_storage_path),
                object_key(&live, marked_archive),
            ]
            .into_iter()
            .chain(kept_keys.iter().cloned()),
        )
        .await;

    env.sweep().await?;

    let mut expected_keys = kept_keys;
    expected_keys.sort();
    assert_eq!(bucket.list_keys().await, expected_keys);
    assert_eq!(
        env.get_dataset_states().await,
        HashMap::from([("ds_other".to_owned(), false)])
    );
    assert_eq!(
        env.get_archive_states().await,
        HashMap::from([(live_archive, false)])
    );

    env.drop_database().await;
    Ok(())
}

/// A uniquely named database on the test server with the shared metadata tables and a query jobs
/// table, and an archive garbage collector of it, used by a single test.
struct TestEnv {
    garbage_collector: ArchiveGarbageCollector,
    conn: MySqlConnection,
    tables: MetadataTables,
    config: Config,
    name: String,
}

impl TestEnv {
    /// Factory function.
    ///
    /// Creates a uniquely named database on the server at `CLP_METADATA_DB_TEST_URL` (e.g.
    /// `mysql://root:password@127.0.0.1:3306`), creates the shared metadata tables and the query
    /// jobs table's columns that the tests use in it, and creates an archive garbage collector of
    /// it and of `archive_output_storage`.
    ///
    /// # Returns
    ///
    /// The newly created test environment.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// * `CLP_METADATA_DB_TEST_URL` isn't set or isn't a valid MySQL URL.
    /// * The database or its tables can't be created, or the database can't be connected to.
    /// * The archive garbage collector can't be created.
    async fn create(archive_output_storage: ArchiveOutputStorage) -> Self {
        let server_url = std::env::var("CLP_METADATA_DB_TEST_URL")
            .expect("`CLP_METADATA_DB_TEST_URL` should be set to a MariaDB or MySQL server URL");
        let server_options = MySqlConnectOptions::from_str(&server_url)
            .expect("`CLP_METADATA_DB_TEST_URL` should be a valid MySQL URL");
        let name = format!("garbage_collector_test_{}", Uuid::new_v4().simple());

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

        let options = server_options.database(&name);
        let mut conn = MySqlConnection::connect_with(&options)
            .await
            .expect("failed to connect to the test database");
        let config = Config {
            package: Package {
                storage_engine: StorageEngine::ClpS,
            },
            archive_output: ArchiveOutput {
                storage: archive_output_storage,
                ..ArchiveOutput::default()
            },
            ..Config::default()
        };
        let tables = MetadataTables::new(&config.database);
        tables
            .create_tables(&mut conn)
            .await
            .expect("failed to create the shared metadata tables");
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

        let db_pool = MySqlPoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("failed to connect to the test database");
        let garbage_collector = ArchiveGarbageCollector::from_config(&config, db_pool)
            .await
            .expect("failed to create the archive garbage collector");
        Self {
            garbage_collector,
            conn,
            tables,
            config,
            name,
        }
    }

    /// Runs one sweep of the archive garbage collector.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ArchiveGarbageCollector::sweep`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if the sweep doesn't finish within 30 seconds.
    async fn sweep(&self) -> Result<(), Error> {
        tokio::time::timeout(Duration::from_secs(30), self.garbage_collector.sweep())
            .await
            .expect("the sweep should finish within 30 seconds")
    }

    /// # Returns
    ///
    /// A dataset to create with the given name, stored where the package config stores it, and
    /// without a retention period.
    ///
    /// # Panics
    ///
    /// Panics if `name` isn't a valid dataset name.
    fn new_dataset(&self, name: &str) -> NewDataset {
        let name = name.parse().expect("name should be a valid dataset name");
        NewDataset {
            archive_storage_path: self
                .config
                .archive_output
                .dataset_archive_storage_directory(&name),
            name,
            retention_period_minutes: None,
        }
    }

    /// Creates a dataset.
    ///
    /// # Returns
    ///
    /// The ID of the created dataset.
    ///
    /// # Panics
    ///
    /// Panics if the dataset can't be created.
    async fn create_dataset(&mut self, dataset: &NewDataset) -> DatasetId {
        self.tables
            .create_dataset(&mut self.conn, dataset)
            .await
            .expect("failed to create a dataset")
    }

    /// Marks a dataset as deleted.
    ///
    /// # Panics
    ///
    /// Panics if the dataset isn't live or can't be marked.
    async fn mark_dataset(&mut self, dataset_id: DatasetId) {
        assert!(
            self.tables
                .mark_dataset_deleted(&mut self.conn, dataset_id)
                .await
                .expect("failed to mark a dataset as deleted"),
            "the dataset should be live"
        );
    }

    /// Inserts an archive with a random UUID into a dataset.
    ///
    /// # Returns
    ///
    /// The archive's UUID.
    ///
    /// # Panics
    ///
    /// Panics if the archive can't be inserted.
    async fn insert_archive(&mut self, dataset_id: DatasetId) -> ArchiveId {
        let uuid = ArchiveId::from_bytes(*Uuid::new_v4().as_bytes());
        self.tables
            .insert_archives(
                &mut self.conn,
                dataset_id,
                1,
                &[NewArchive {
                    uuid,
                    begin_timestamp_millisecs: 0,
                    end_timestamp_millisecs: 0,
                    num_uncompressed_bytes: 1000,
                    num_compressed_bytes: 100,
                }],
            )
            .await
            .expect("failed to insert an archive");
        uuid
    }

    /// Moves an archive's creation time back by `minutes`.
    ///
    /// # Panics
    ///
    /// Panics if the archive's row can't be updated.
    async fn backdate_archive(&mut self, uuid: ArchiveId, minutes: i64) {
        sqlx::query(&format!(
            r"
UPDATE `{archives}`
SET `creation_timestamp_millisecs` = `creation_timestamp_millisecs` - ? * 60 * 1000
WHERE `uuid` = ?",
            archives = self.config.database.unscoped_archives_table_name(),
        ))
        .bind(minutes)
        .bind(uuid.as_bytes().as_slice())
        .execute(&mut self.conn)
        .await
        .expect("failed to backdate an archive");
    }

    /// Marks a dataset's live archive as deleted.
    ///
    /// # Panics
    ///
    /// Panics if the archive isn't a live archive of the dataset or can't be marked.
    async fn mark_archive(&mut self, dataset_id: DatasetId, uuid: ArchiveId) {
        let ids: Vec<_> = self
            .tables
            .find_archives_by_uuids(&mut self.conn, dataset_id, &[uuid])
            .await
            .expect("failed to find an archive")
            .into_iter()
            .map(|archive| archive.id)
            .collect();
        assert_eq!(
            self.tables
                .mark_archives_deleted(&mut self.conn, dataset_id, &ids)
                .await
                .expect("failed to mark an archive as deleted"),
            1
        );
    }

    /// Inserts a query job with the given status, created `age_minutes` before the DB's current
    /// time.
    ///
    /// # Returns
    ///
    /// The query job's ID.
    ///
    /// # Panics
    ///
    /// Panics if the query job can't be inserted.
    async fn insert_query_job(&mut self, status: QueryJobStatus, age_minutes: i64) -> u64 {
        sqlx::query(&format!(
            "INSERT INTO `{QUERY_JOBS_TABLE_NAME}` (`status`, `creation_time`) VALUES (?, \
             UTC_TIMESTAMP(3) - INTERVAL ? MINUTE)"
        ))
        .bind(status)
        .bind(age_minutes)
        .execute(&mut self.conn)
        .await
        .expect("failed to insert a query job")
        .last_insert_id()
    }

    /// Sets a query job's status.
    ///
    /// # Panics
    ///
    /// Panics if the query job's row can't be updated.
    async fn set_query_job_status(&mut self, id: u64, status: QueryJobStatus) {
        sqlx::query(&format!(
            "UPDATE `{QUERY_JOBS_TABLE_NAME}` SET `status` = ? WHERE `id` = ?"
        ))
        .bind(status)
        .bind(id)
        .execute(&mut self.conn)
        .await
        .expect("failed to set a query job's status");
    }

    /// # Returns
    ///
    /// A map from each dataset's name to whether it's marked as deleted.
    ///
    /// # Panics
    ///
    /// Panics if the datasets table can't be read.
    async fn get_dataset_states(&mut self) -> HashMap<String, bool> {
        sqlx::query_as(&format!(
            "SELECT CONVERT(`name` USING utf8mb4), `is_deleted` FROM `{datasets}`",
            datasets = self.config.database.datasets_table_name(),
        ))
        .fetch_all(&mut self.conn)
        .await
        .expect("failed to read the datasets table")
        .into_iter()
        .collect()
    }

    /// # Returns
    ///
    /// A map from each archive's UUID to whether it's marked as deleted.
    ///
    /// # Panics
    ///
    /// Panics if the archives table can't be read or holds a UUID that isn't 16 bytes.
    async fn get_archive_states(&mut self) -> HashMap<ArchiveId, bool> {
        let rows: Vec<(Vec<u8>, bool)> = sqlx::query_as(&format!(
            "SELECT `uuid`, `is_deleted` FROM `{archives}`",
            archives = self.config.database.unscoped_archives_table_name(),
        ))
        .fetch_all(&mut self.conn)
        .await
        .expect("failed to read the archives table");
        rows.into_iter()
            .map(|(uuid, is_deleted)| {
                (
                    ArchiveId::from_bytes(
                        uuid.try_into()
                            .expect("an archive's UUID should be 16 bytes"),
                    ),
                    is_deleted,
                )
            })
            .collect()
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

/// A uniquely named bucket created for a single test, with the S3 config of an archive storage in
/// it.
struct TestBucket {
    client: Client,
    s3_config: S3Config,
}

impl TestBucket {
    /// Creates a uniquely named bucket on the S3 endpoint configured by the following environment
    /// variables:
    ///
    /// * `AWS_ENDPOINT_URL`: Defaults to `http://127.0.0.1:4566` if not set.
    /// * `AWS_ACCESS_KEY_ID`: Defaults to `test` if not set.
    /// * `AWS_SECRET_ACCESS_KEY`: Defaults to `test` if not set.
    /// * `AWS_REGION`: Defaults to `us-east-1` if not set.
    ///
    /// # Returns
    ///
    /// The created bucket.
    ///
    /// # Panics
    ///
    /// Panics if the bucket can't be created.
    async fn create() -> Self {
        let env_var_or =
            |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_owned());
        let s3_config = S3Config {
            bucket: NonEmptyString::from_string(format!(
                "clp-garbage-collector-test-{}",
                Uuid::new_v4().simple()
            )),
            region_code: Some(NonEmptyString::from_string(env_var_or(
                "AWS_REGION",
                "us-east-1",
            ))),
            key_prefix: NonEmptyString::from_static_str("archives/"),
            endpoint_url: Some(NonEmptyString::from_string(env_var_or(
                "AWS_ENDPOINT_URL",
                "http://127.0.0.1:4566",
            ))),
            aws_authentication: AwsAuthentication::Credentials {
                credentials: AwsCredentials {
                    access_key_id: env_var_or("AWS_ACCESS_KEY_ID", "test"),
                    secret_access_key: env_var_or("AWS_SECRET_ACCESS_KEY", "test"),
                    session_token: None,
                },
            },
        };
        let client = clp_rust_utils::s3::create_new_client(
            s3_config
                .region_code
                .as_ref()
                .expect("the region code should be set")
                .as_str(),
            s3_config.endpoint_url.as_ref(),
            &s3_config.aws_authentication,
        )
        .await;
        client
            .create_bucket()
            .bucket(s3_config.bucket.as_str())
            .send()
            .await
            .expect("failed to create the test bucket");
        Self { client, s3_config }
    }

    /// Creates an object for each of the given keys.
    ///
    /// # Panics
    ///
    /// Panics if an object can't be created.
    async fn put_objects(&self, keys: impl Iterator<Item = String>) {
        for key in keys {
            self.client
                .put_object()
                .bucket(self.s3_config.bucket.as_str())
                .key(key)
                .body(Vec::from(*b"content").into())
                .send()
                .await
                .expect("failed to create a test object");
        }
    }

    /// # Returns
    ///
    /// The keys of every object in the bucket, in lexicographic order.
    ///
    /// # Panics
    ///
    /// Panics if the objects can't be listed.
    async fn list_keys(&self) -> Vec<String> {
        self.client
            .list_objects_v2()
            .bucket(self.s3_config.bucket.as_str())
            .into_paginator()
            .send()
            .try_collect()
            .await
            .expect("failed to list the test objects")
            .into_iter()
            .flat_map(|page| page.contents.unwrap_or_default())
            .filter_map(|object| object.key)
            .collect()
    }
}

/// # Returns
///
/// An archive storage on the local filesystem under `directory`.
fn fs_storage(directory: &Path) -> ArchiveOutputStorage {
    ArchiveOutputStorage::Fs {
        directory: directory.to_string_lossy().into_owned(),
    }
}

/// # Returns
///
/// The path of an archive of `dataset` on the local filesystem.
fn archive_path(dataset: &NewDataset, uuid: ArchiveId) -> PathBuf {
    Path::new(&dataset.archive_storage_path).join(uuid.to_string())
}

/// # Returns
///
/// The S3 object key of an archive of `dataset`.
fn object_key(dataset: &NewDataset, uuid: ArchiveId) -> String {
    format!("{}/{uuid}", dataset.archive_storage_path)
}

/// Creates an archive stored as a directory with a nested file at `path`.
///
/// # Panics
///
/// Panics if the directory or the file can't be created.
fn create_directory_archive(path: &Path) {
    std::fs::create_dir_all(path.join("nested")).expect("failed to create an archive directory");
    std::fs::write(path.join("nested").join("segment"), b"content")
        .expect("failed to create an archive file");
}

/// Creates an archive stored as a single file at `path`.
///
/// # Panics
///
/// Panics if the file or its parent directory can't be created.
fn create_file_archive(path: &Path) {
    std::fs::create_dir_all(
        path.parent()
            .expect("an archive's path should have a parent directory"),
    )
    .expect("failed to create a dataset directory");
    std::fs::write(path, b"content").expect("failed to create an archive file");
}
