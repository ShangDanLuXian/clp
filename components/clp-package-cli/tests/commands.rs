//! Tests of the tools' commands against a MariaDB or MySQL server.
//!
//! The tests are ignored by default. To run them, set `CLP_METADATA_DB_TEST_SERVER_URL` to the URL
//! of a server without a database (e.g. `mysql://user:password@127.0.0.1:3306`) whose user can
//! create and drop databases, and run `cargo test -p clp-package-cli -- --ignored`. Each test
//! recreates its own database.

use clp_metadata_db::ClpDbClient;
use clp_metadata_db::Dataset;
use clp_metadata_db::DatasetConfig;
use clp_metadata_db::DatasetLookup;
use clp_package_cli::table_creator::DefaultDataset;
use clp_package_cli::table_creator::create_tables_and_default_dataset;
use clp_rust_utils::clp_config::package::config::ArchiveOutput;
use clp_rust_utils::clp_config::package::config::ArchiveOutputStorage;
use clp_rust_utils::clp_config::package::config::Database;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::types::DatasetId;
use sqlx::mysql::MySqlPoolOptions;

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
async fn table_creator_succeeds_if_default_is_configured_differently() -> anyhow::Result<()> {
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
    let mut connection = test_db.client.acquire().await?;
    assert_eq!(
        connection
            .get_datasets()
            .await?
            .into_iter()
            .map(|dataset| dataset.config)
            .collect::<Vec<_>>(),
        vec![stored_config]
    );
    Ok(())
}
