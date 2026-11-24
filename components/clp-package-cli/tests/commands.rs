//! Tests of the tools' commands against a MariaDB or MySQL server and a filesystem archive storage.
//!
//! The tests are ignored by default. To run them, set `CLP_METADATA_DB_TEST_SERVER_URL` to the URL
//! of a server without a database (e.g. `mysql://user:password@127.0.0.1:3306`) whose user can
//! create and drop databases, and run `cargo test -p clp-package-cli -- --ignored`. Each test
//! recreates its own database. The storage-failure test needs a user whose directory permissions
//! are enforced, so it skips itself when run as root.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use clp_metadata_db::ArchiveDescriptor;
use clp_metadata_db::ArchiveLookup;
use clp_metadata_db::ArchiveManagement;
use clp_metadata_db::ArchiveTimestampRange;
use clp_metadata_db::ClpDbClient;
use clp_metadata_db::Dataset;
use clp_metadata_db::DatasetConfig;
use clp_metadata_db::DatasetDescriptor;
use clp_metadata_db::DatasetLookup;
use clp_metadata_db::DatasetManagement;
use clp_package_cli::archive_manager::ArchiveReference;
use clp_package_cli::archive_manager::ArchiveSelection;
use clp_package_cli::archive_manager::ArchiveStorage;
use clp_package_cli::archive_manager::delete_archives;
use clp_package_cli::archive_manager::preview_deletion;
use clp_package_cli::dataset_manager::DatasetsToDelete;
use clp_package_cli::dataset_manager::delete_datasets;
use clp_package_cli::dataset_manager::list_datasets;
use clp_package_cli::table_creator::DefaultDataset;
use clp_package_cli::table_creator::create_tables_and_default_dataset;
use clp_rust_utils::clp_config::package::config::ArchiveOutput;
use clp_rust_utils::clp_config::package::config::ArchiveOutputStorage;
use clp_rust_utils::clp_config::package::config::Database;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::job_config::TimeRange;
use clp_rust_utils::types::ArchiveId;
use clp_rust_utils::types::ArchiveUuid;
use clp_rust_utils::types::DatasetId;
use sqlx::mysql::MySqlPoolOptions;

/// An archive inserted by [`TestDb::insert_archive`].
#[derive(Clone, Copy, Debug)]
struct TestArchive {
    id: ArchiveId,
    uuid: ArchiveUuid,
}

/// A freshly created database, and a client of it.
struct TestDb {
    client: ClpDbClient,
}

impl TestDb {
    /// Recreates the database `clp_package_cli_test_<name>`, without tables.
    ///
    /// # Returns
    ///
    /// A client of the database.
    ///
    /// # Panics
    ///
    /// Panics if `CLP_METADATA_DB_TEST_SERVER_URL` isn't set, or the database can't be recreated.
    async fn create_empty(name: &str) -> Self {
        let server_url = std::env::var("CLP_METADATA_DB_TEST_SERVER_URL")
            .expect("`CLP_METADATA_DB_TEST_SERVER_URL` should be set");
        let database = format!("clp_package_cli_test_{name}");
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
            .max_connections(2)
            .connect(&format!("{server_url}/{database}"))
            .await
            .expect("the test database should be reachable");
        Self {
            client: ClpDbClient::new(pool, &Database::default()),
        }
    }

    /// Recreates the database `clp_package_cli_test_<name>` with the shared metadata tables.
    ///
    /// # Returns
    ///
    /// A client of the database.
    ///
    /// # Panics
    ///
    /// Panics if the database or its tables can't be created.
    async fn create(name: &str) -> Self {
        let test_db = Self::create_empty(name).await;
        test_db
            .client
            .create_tables()
            .await
            .expect("the shared metadata tables should be created");
        test_db
    }

    /// Creates the dataset `name`, whose archives are stored under `archive_storage_path`.
    ///
    /// # Returns
    ///
    /// The dataset's ID.
    ///
    /// # Panics
    ///
    /// Panics if the dataset can't be created.
    async fn create_dataset(&self, name: &str, archive_storage_path: &Path) -> DatasetId {
        let descriptor = DatasetDescriptor {
            name: name.parse().expect("the dataset name should be valid"),
            config: DatasetConfig {
                archive_storage_path: archive_storage_path.to_string_lossy().into_owned(),
                retention_period_minutes: None,
            },
        };
        let mut transaction = self
            .client
            .begin()
            .await
            .expect("a transaction should begin");
        let id = transaction
            .create_or_get_dataset(&descriptor)
            .await
            .expect("the dataset should be created");
        transaction
            .commit()
            .await
            .expect("the transaction should commit");
        id
    }

    /// Inserts an archive with the UUID made of `uuid_byte` and `timestamp_range` into the dataset
    /// `dataset_id`.
    ///
    /// # Returns
    ///
    /// The inserted archive.
    ///
    /// # Panics
    ///
    /// Panics if the archive can't be inserted.
    async fn insert_archive(
        &self,
        dataset_id: DatasetId,
        uuid_byte: u8,
        timestamp_range: ArchiveTimestampRange,
    ) -> TestArchive {
        let uuid = ArchiveUuid::from_bytes([uuid_byte; 16]);
        let mut transaction = self
            .client
            .begin()
            .await
            .expect("a transaction should begin");
        transaction
            .insert_archives(
                dataset_id,
                1,
                &[ArchiveDescriptor {
                    uuid,
                    timestamp_range,
                    num_uncompressed_bytes: 1000,
                    num_compressed_bytes: 100,
                }],
            )
            .await
            .expect("the archive should be inserted");
        let ids = transaction
            .find_archive_ids_by_uuids(dataset_id, &[uuid])
            .await
            .expect("the archive should be found");
        transaction
            .commit()
            .await
            .expect("the transaction should commit");
        TestArchive {
            id: ids
                .into_iter()
                .next()
                .flatten()
                .expect("the inserted archive should have an ID"),
            uuid,
        }
    }

    /// # Returns
    ///
    /// The IDs of the archives of the dataset `dataset_id`, ordered by begin timestamp and ID.
    ///
    /// # Panics
    ///
    /// Panics if the archives can't be read.
    async fn archive_ids(&self, dataset_id: DatasetId) -> Vec<ArchiveId> {
        let mut connection = self
            .client
            .acquire()
            .await
            .expect("a connection should be acquired");
        connection
            .find_archives_by_time_range(dataset_id, &TimeRange::default())
            .await
            .expect("the archives should be read")
            .into_iter()
            .map(|archive| archive.id)
            .collect()
    }
}

/// Creates an archive directory with one file, named after `archive.uuid`, under `directory`.
///
/// # Panics
///
/// Panics if the directory can't be created.
fn store_archive_directory(directory: &Path, archive: TestArchive) {
    let archive_directory = directory.join(archive.uuid.to_string());
    std::fs::create_dir_all(&archive_directory).expect("the archive directory should be created");
    std::fs::write(archive_directory.join("0"), b"archive")
        .expect("the archive file should be written");
}

/// # Returns
///
/// Whether `archive` is stored under `directory`.
fn is_stored(directory: &Path, archive: TestArchive) -> bool {
    directory.join(archive.uuid.to_string()).exists()
}

#[tokio::test]
#[ignore = "requires a MariaDB or MySQL server"]
async fn table_creator_gives_default_the_first_id_and_reruns_harmlessly() -> anyhow::Result<()> {
    let test_db = TestDb::create_empty("table_creator").await;
    let archive_output = ArchiveOutput {
        storage: ArchiveOutputStorage::Fs {
            directory: "/var/data/archives".to_owned(),
        },
        ..ArchiveOutput::default()
    };

    let expected_id = DatasetId::from(1);
    assert_eq!(
        create_tables_and_default_dataset(&test_db.client, &archive_output).await?,
        DefaultDataset::Ready { id: expected_id }
    );
    assert_eq!(
        create_tables_and_default_dataset(&test_db.client, &archive_output).await?,
        DefaultDataset::Ready { id: expected_id }
    );

    let mut connection = test_db.client.acquire().await?;
    assert_eq!(
        connection.get_datasets().await?,
        vec![Dataset {
            id: expected_id,
            name: DatasetName::default(),
            config: DatasetConfig {
                archive_storage_path: "/var/data/archives/default".to_owned(),
                retention_period_minutes: None,
            },
        }]
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a MariaDB or MySQL server"]
async fn table_creator_succeeds_if_default_is_deleted_or_configured_differently()
-> anyhow::Result<()> {
    let test_db = TestDb::create_empty("table_creator_unusable_default").await;
    let archive_output_in = |directory: &str| ArchiveOutput {
        storage: ArchiveOutputStorage::Fs {
            directory: directory.to_owned(),
        },
        ..ArchiveOutput::default()
    };
    let stored_config = DatasetConfig {
        archive_storage_path: "/var/data/archives/default".to_owned(),
        retention_period_minutes: None,
    };
    create_tables_and_default_dataset(&test_db.client, &archive_output_in("/var/data/archives"))
        .await?;

    assert_eq!(
        create_tables_and_default_dataset(&test_db.client, &archive_output_in("/mnt/archives"))
            .await?,
        DefaultDataset::ConfigMismatch {
            existing: stored_config.clone(),
        }
    );
    assert_eq!(
        list_datasets(&test_db.client)
            .await?
            .into_iter()
            .map(|dataset| dataset.config)
            .collect::<Vec<_>>(),
        vec![stored_config]
    );

    delete_datasets(
        &test_db.client,
        DatasetsToDelete::Named(vec![DatasetName::default()]),
    )
    .await?;
    assert_eq!(
        create_tables_and_default_dataset(
            &test_db.client,
            &archive_output_in("/var/data/archives")
        )
        .await?,
        DefaultDataset::Deleted
    );
    assert_eq!(list_datasets(&test_db.client).await?, vec![]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a MariaDB or MySQL server"]
async fn deleting_by_ids_spans_datasets_and_deletes_storage_under_each_dataset()
-> anyhow::Result<()> {
    let test_db = TestDb::create("delete_by_ids").await;
    let storage = tempfile::tempdir()?;
    let logs_directory = storage.path().join("logs");
    let metrics_directory = storage.path().join("metrics");
    let logs_id = test_db.create_dataset("logs", &logs_directory).await;
    let metrics_id = test_db.create_dataset("metrics", &metrics_directory).await;

    let deleted_logs_archive = test_db
        .insert_archive(logs_id, 1, ArchiveTimestampRange::new(10, 20)?)
        .await;
    let kept_logs_archive = test_db
        .insert_archive(logs_id, 2, ArchiveTimestampRange::new(30, 40)?)
        .await;
    let single_file_metrics_archive = test_db
        .insert_archive(metrics_id, 3, ArchiveTimestampRange::WITHOUT_TIMESTAMPS)
        .await;
    let unstored_metrics_archive = test_db
        .insert_archive(metrics_id, 4, ArchiveTimestampRange::new(50, 60)?)
        .await;
    store_archive_directory(&logs_directory, deleted_logs_archive);
    store_archive_directory(&logs_directory, kept_logs_archive);
    std::fs::create_dir_all(&metrics_directory)?;
    std::fs::write(
        metrics_directory.join(single_file_metrics_archive.uuid.to_string()),
        b"archive",
    )?;

    let unknown_id = ArchiveId::from(1_000_000);
    let selection = ArchiveSelection::by_ids(
        None,
        &[
            deleted_logs_archive.id,
            single_file_metrics_archive.id,
            unknown_id,
            unstored_metrics_archive.id,
            deleted_logs_archive.id,
        ]
        .map(|id| id.to_string()),
    )?;
    let deleted = delete_archives(&test_db.client, &ArchiveStorage::Fs, &selection).await?;

    assert_eq!(
        deleted
            .archives
            .iter()
            .map(|archive| (archive.metadata.id, archive.dataset_name.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (deleted_logs_archive.id, "logs"),
            (single_file_metrics_archive.id, "metrics"),
            (unstored_metrics_archive.id, "metrics"),
        ]
    );
    assert_eq!(deleted.unknown, vec![ArchiveReference::Id(unknown_id)]);
    assert_eq!(
        test_db.archive_ids(logs_id).await,
        vec![kept_logs_archive.id]
    );
    assert_eq!(test_db.archive_ids(metrics_id).await, vec![]);
    assert!(!is_stored(&logs_directory, deleted_logs_archive));
    assert!(!is_stored(&metrics_directory, single_file_metrics_archive));
    assert!(is_stored(&logs_directory, kept_logs_archive));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a MariaDB or MySQL server"]
async fn deleting_by_uuids_selects_only_archives_of_the_dataset() -> anyhow::Result<()> {
    let test_db = TestDb::create("delete_by_uuids").await;
    let storage = tempfile::tempdir()?;
    let logs_directory = storage.path().join("logs");
    let metrics_directory = storage.path().join("metrics");
    let logs_id = test_db.create_dataset("logs", &logs_directory).await;
    let metrics_id = test_db.create_dataset("metrics", &metrics_directory).await;

    let logs_archive = test_db
        .insert_archive(logs_id, 1, ArchiveTimestampRange::new(10, 20)?)
        .await;
    let metrics_archive = test_db
        .insert_archive(metrics_id, 2, ArchiveTimestampRange::new(10, 20)?)
        .await;
    store_archive_directory(&logs_directory, logs_archive);
    store_archive_directory(&metrics_directory, metrics_archive);

    let unknown_uuid = ArchiveUuid::from_bytes([9; 16]);
    let selection = ArchiveSelection::by_uuids(
        Some("logs".parse()?),
        &[logs_archive.uuid, metrics_archive.uuid, unknown_uuid].map(|uuid| uuid.to_string()),
    )?;
    let deleted = delete_archives(&test_db.client, &ArchiveStorage::Fs, &selection).await?;

    assert_eq!(
        deleted
            .archives
            .iter()
            .map(|archive| archive.metadata.id)
            .collect::<Vec<_>>(),
        vec![logs_archive.id]
    );
    assert_eq!(
        deleted.unknown,
        vec![
            ArchiveReference::Uuid(metrics_archive.uuid),
            ArchiveReference::Uuid(unknown_uuid),
        ]
    );
    assert_eq!(test_db.archive_ids(logs_id).await, vec![]);
    assert_eq!(
        test_db.archive_ids(metrics_id).await,
        vec![metrics_archive.id]
    );
    assert!(!is_stored(&logs_directory, logs_archive));
    assert!(is_stored(&metrics_directory, metrics_archive));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a MariaDB or MySQL server"]
async fn previewing_a_deletion_changes_nothing() -> anyhow::Result<()> {
    let test_db = TestDb::create("preview_deletion").await;
    let storage = tempfile::tempdir()?;
    let dataset_id = test_db.create_dataset("logs", storage.path()).await;
    let archive_within = test_db
        .insert_archive(dataset_id, 1, ArchiveTimestampRange::new(100, 200)?)
        .await;
    let archive_beyond = test_db
        .insert_archive(dataset_id, 2, ArchiveTimestampRange::new(100, 5000)?)
        .await;
    store_archive_directory(storage.path(), archive_within);

    let selection = ArchiveSelection::by_time_range(
        Some("logs".parse()?),
        TimeRange::new(Some(50), Some(1000))?,
    );
    let selected = preview_deletion(&test_db.client, &selection).await?;

    assert_eq!(
        selected
            .archives
            .iter()
            .map(|archive| archive.metadata.id)
            .collect::<Vec<_>>(),
        vec![archive_within.id]
    );
    assert_eq!(selected.unknown, vec![]);
    assert_eq!(
        test_db.archive_ids(dataset_id).await,
        vec![archive_within.id, archive_beyond.id]
    );
    assert!(is_stored(storage.path(), archive_within));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a MariaDB or MySQL server"]
async fn a_storage_failure_fails_the_deletion_after_trying_every_archive() -> anyhow::Result<()> {
    let test_db = TestDb::create("storage_failure").await;
    let storage = tempfile::tempdir()?;
    let read_only_directory = storage.path().join("read_only");
    let writable_directory = storage.path().join("writable");
    let read_only_id = test_db
        .create_dataset("read_only", &read_only_directory)
        .await;
    let writable_id = test_db
        .create_dataset("writable", &writable_directory)
        .await;
    let undeletable_archive = test_db
        .insert_archive(read_only_id, 1, ArchiveTimestampRange::new(10, 20)?)
        .await;
    let deletable_archive = test_db
        .insert_archive(writable_id, 2, ArchiveTimestampRange::new(10, 20)?)
        .await;
    store_archive_directory(&read_only_directory, undeletable_archive);
    store_archive_directory(&writable_directory, deletable_archive);
    std::fs::set_permissions(&read_only_directory, std::fs::Permissions::from_mode(0o555))?;
    if std::fs::File::create(read_only_directory.join("probe")).is_ok() {
        // The user (e.g. root) bypasses the permissions, so the archive's storage can't be made
        // undeletable.
        eprintln!("Skipping the test: directory permissions aren't enforced for this user.");
        return Ok(());
    }

    let selection = ArchiveSelection::by_ids(
        None,
        &[undeletable_archive.id, deletable_archive.id].map(|id| id.to_string()),
    )?;
    let result = delete_archives(&test_db.client, &ArchiveStorage::Fs, &selection).await;
    std::fs::set_permissions(&read_only_directory, std::fs::Permissions::from_mode(0o755))?;

    let error =
        result.expect_err("the deletion should fail when an archive's storage can't be deleted");
    assert_eq!(
        error.to_string(),
        "failed to delete 1 archive(s) from storage"
    );
    assert_eq!(test_db.archive_ids(read_only_id).await, vec![]);
    assert_eq!(test_db.archive_ids(writable_id).await, vec![]);
    assert!(is_stored(&read_only_directory, undeletable_archive));
    assert!(!is_stored(&writable_directory, deletable_archive));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a MariaDB or MySQL server"]
async fn deleting_named_datasets_marks_none_if_a_name_is_unknown() -> anyhow::Result<()> {
    let test_db = TestDb::create("delete_named_datasets").await;
    let storage = tempfile::tempdir()?;
    for name in ["logs", "metrics", "traces"] {
        test_db.create_dataset(name, storage.path()).await;
    }
    let logs: DatasetName = "logs".parse()?;
    let dataset_names = async || -> anyhow::Result<Vec<DatasetName>> {
        Ok(list_datasets(&test_db.client)
            .await?
            .into_iter()
            .map(|dataset| dataset.name)
            .collect())
    };

    let error = delete_datasets(
        &test_db.client,
        DatasetsToDelete::Named(vec![logs.clone(), "unknown".parse()?]),
    )
    .await
    .expect_err("an unknown dataset name should abort the deletion");
    assert_eq!(
        error.to_string(),
        "dataset `unknown` doesn't exist; aborting deletion"
    );
    assert_eq!(
        dataset_names().await?,
        vec![logs.clone(), "metrics".parse()?, "traces".parse()?]
    );

    assert_eq!(
        delete_datasets(
            &test_db.client,
            DatasetsToDelete::Named(vec![logs.clone(), logs.clone()]),
        )
        .await?,
        vec![logs]
    );
    assert_eq!(
        dataset_names().await?,
        vec!["metrics".parse()?, "traces".parse()?]
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a MariaDB or MySQL server"]
async fn deleting_all_datasets_marks_every_live_dataset() -> anyhow::Result<()> {
    let test_db = TestDb::create("delete_all_datasets").await;
    let storage = tempfile::tempdir()?;
    for name in ["logs", "metrics"] {
        test_db.create_dataset(name, storage.path()).await;
    }

    assert_eq!(
        delete_datasets(&test_db.client, DatasetsToDelete::All).await?,
        vec!["logs".parse()?, "metrics".parse()?]
    );
    assert_eq!(list_datasets(&test_db.client).await?, vec![]);
    assert_eq!(
        delete_datasets(&test_db.client, DatasetsToDelete::All).await?,
        vec![]
    );
    Ok(())
}
