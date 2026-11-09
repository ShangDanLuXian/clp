use std::collections::HashMap;
use std::num::NonZeroU32;

use anyhow::Result;
use clp_metadata_db::DatasetId;
use clp_metadata_db::Error;
use clp_metadata_db::MarkedDataset;
use clp_metadata_db::NewDataset;
use clp_rust_utils::dataset::DatasetName;
use sqlx::Connection;
use sqlx::mysql::MySqlDatabaseError;
use uuid::Uuid;

use super::test_utils::TestDatabase;

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn create_dataset_stores_the_dataset_and_returns_its_id() -> Result<()> {
    let mut test_db = create_test_database().await;
    let first_dataset = new_dataset("first");
    let second_dataset = NewDataset {
        retention_period_minutes: NonZeroU32::new(60),
        ..new_dataset("second")
    };

    let first_id = test_db
        .tables
        .create_dataset(&mut test_db.conn, &first_dataset)
        .await?;
    let second_id = test_db
        .tables
        .create_dataset(&mut test_db.conn, &second_dataset)
        .await?;

    let rows: Vec<(DatasetId, DatasetName, String, Option<u32>)> = sqlx::query_as(&format!(
        r"
SELECT
    `id`,
    CONVERT(`name` USING utf8mb4),
    CONVERT(`archive_storage_path` USING utf8mb4),
    `retention_period_minutes`
FROM `{datasets}`
ORDER BY `id`",
        datasets = test_db.database_config.datasets_table_name(),
    ))
    .fetch_all(&mut test_db.conn)
    .await?;
    assert_eq!(
        rows,
        [(first_id, first_dataset), (second_id, second_dataset)]
            .map(|(id, dataset)| (
                id,
                dataset.name,
                dataset.archive_storage_path,
                dataset.retention_period_minutes.map(NonZeroU32::get),
            ))
            .to_vec()
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn create_dataset_returns_dataset_already_exists_if_live_or_marked_dataset_has_name()
-> Result<()> {
    let mut test_db = create_test_database().await;
    let live_dataset = new_dataset("live");
    let marked_dataset = new_dataset("marked");
    test_db
        .tables
        .create_dataset(&mut test_db.conn, &live_dataset)
        .await?;
    let marked_id = test_db
        .tables
        .create_dataset(&mut test_db.conn, &marked_dataset)
        .await?;
    test_db
        .tables
        .mark_dataset_deleted(&mut test_db.conn, marked_id)
        .await?;

    for dataset in [live_dataset, marked_dataset] {
        let result = test_db
            .tables
            .create_dataset(&mut test_db.conn, &dataset)
            .await;
        let Err(Error::DatasetAlreadyExists(name)) = result else {
            anyhow::bail!("expected a dataset-already-exists error, got {result:?}");
        };
        assert_eq!(name, dataset.name);
    }

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_dataset_names_returns_live_names_ordered_by_name() -> Result<()> {
    let mut test_db = create_test_database().await;
    for name in ["zeta", "alpha", "marked"] {
        let id = test_db
            .tables
            .create_dataset(&mut test_db.conn, &new_dataset(name))
            .await?;
        if name == "marked" {
            test_db
                .tables
                .mark_dataset_deleted(&mut test_db.conn, id)
                .await?;
        }
    }

    assert_eq!(
        test_db.tables.get_dataset_names(&mut test_db.conn).await?,
        vec![dataset_name("alpha"), dataset_name("zeta")]
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_dataset_id_by_name_returns_only_live_datasets() -> Result<()> {
    let mut test_db = create_test_database().await;
    let live_id = test_db
        .tables
        .create_dataset(&mut test_db.conn, &new_dataset("live"))
        .await?;
    let marked_id = test_db
        .tables
        .create_dataset(&mut test_db.conn, &new_dataset("marked"))
        .await?;
    test_db
        .tables
        .mark_dataset_deleted(&mut test_db.conn, marked_id)
        .await?;

    for (name, expected_id) in [("live", Some(live_id)), ("marked", None), ("unknown", None)] {
        assert_eq!(
            test_db
                .tables
                .get_dataset_id_by_name(&mut test_db.conn, &dataset_name(name))
                .await?,
            expected_id,
            "dataset {name}"
        );
    }

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_dataset_ids_by_names_omits_unknown_and_marked_names() -> Result<()> {
    let mut test_db = create_test_database().await;
    let mut ids = HashMap::new();
    for name in ["first", "second", "marked"] {
        let id = test_db
            .tables
            .create_dataset(&mut test_db.conn, &new_dataset(name))
            .await?;
        ids.insert(dataset_name(name), id);
    }
    let marked_id = ids
        .remove(&dataset_name("marked"))
        .expect("the marked dataset should have been created");
    test_db
        .tables
        .mark_dataset_deleted(&mut test_db.conn, marked_id)
        .await?;

    let names = ["first", "marked", "unknown", "second"].map(dataset_name);
    assert_eq!(
        test_db
            .tables
            .get_dataset_ids_by_names(&mut test_db.conn, &names)
            .await?,
        ids
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_dataset_ids_by_names_resolves_names_beyond_the_first_1000() -> Result<()> {
    let mut test_db = create_test_database().await;
    let mut ids = HashMap::new();
    for name in ["first", "last"] {
        let id = test_db
            .tables
            .create_dataset(&mut test_db.conn, &new_dataset(name))
            .await?;
        ids.insert(dataset_name(name), id);
    }

    let names: Vec<DatasetName> = std::iter::once(dataset_name("first"))
        .chain((0..1000).map(|index| dataset_name(&format!("unknown_{index}"))))
        .chain(std::iter::once(dataset_name("last")))
        .collect();
    assert_eq!(
        test_db
            .tables
            .get_dataset_ids_by_names(&mut test_db.conn, &names)
            .await?,
        ids
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_dataset_ids_by_names_returns_empty_map_for_no_names() -> Result<()> {
    let mut test_db = create_test_database().await;

    assert_eq!(
        test_db
            .tables
            .get_dataset_ids_by_names(&mut test_db.conn, &[])
            .await?,
        HashMap::new()
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn mark_dataset_deleted_marks_a_live_dataset_only_once() -> Result<()> {
    let mut test_db = create_test_database().await;
    let id = test_db
        .tables
        .create_dataset(&mut test_db.conn, &new_dataset("dataset"))
        .await?;

    assert!(
        test_db
            .tables
            .mark_dataset_deleted(&mut test_db.conn, id)
            .await?
    );
    assert!(
        !test_db
            .tables
            .mark_dataset_deleted(&mut test_db.conn, id)
            .await?
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn lock_live_dataset_returns_whether_dataset_is_live() -> Result<()> {
    let mut test_db = create_test_database().await;
    let live_id = test_db
        .tables
        .create_dataset(&mut test_db.conn, &new_dataset("live"))
        .await?;
    let marked_id = test_db
        .tables
        .create_dataset(&mut test_db.conn, &new_dataset("marked"))
        .await?;
    test_db
        .tables
        .mark_dataset_deleted(&mut test_db.conn, marked_id)
        .await?;

    assert!(
        test_db
            .tables
            .lock_live_dataset(&mut test_db.conn, live_id)
            .await?
    );
    assert!(
        !test_db
            .tables
            .lock_live_dataset(&mut test_db.conn, marked_id)
            .await?
    );
    test_db
        .tables
        .delete_marked_dataset(&mut test_db.conn, marked_id)
        .await?;
    assert!(
        !test_db
            .tables
            .lock_live_dataset(&mut test_db.conn, marked_id)
            .await?
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn lock_live_dataset_blocks_concurrent_mark_dataset_deleted() -> Result<()> {
    // MySQL error number for "Lock wait timeout exceeded".
    const MYSQL_ER_LOCK_WAIT_TIMEOUT: u16 = 1205;

    let mut test_db = create_test_database().await;
    let id = test_db
        .tables
        .create_dataset(&mut test_db.conn, &new_dataset("dataset"))
        .await?;
    let mut other_conn = test_db.connect().await;
    sqlx::query("SET SESSION innodb_lock_wait_timeout = 1")
        .execute(&mut other_conn)
        .await?;

    let mut tx = test_db.conn.begin().await?;
    assert!(test_db.tables.lock_live_dataset(&mut tx, id).await?);
    let result = test_db
        .tables
        .mark_dataset_deleted(&mut other_conn, id)
        .await;
    let Err(Error::Sqlx(sqlx::Error::Database(db_err))) = &result else {
        anyhow::bail!("expected a lock-wait timeout, got {result:?}");
    };
    assert_eq!(
        db_err
            .try_downcast_ref::<MySqlDatabaseError>()
            .map(MySqlDatabaseError::number),
        Some(MYSQL_ER_LOCK_WAIT_TIMEOUT)
    );
    tx.rollback().await?;

    other_conn.close().await?;
    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_marked_datasets_returns_only_marked_datasets() -> Result<()> {
    let mut test_db = create_test_database().await;
    let marked_dataset = new_dataset("marked");
    let marked_id = test_db
        .tables
        .create_dataset(&mut test_db.conn, &marked_dataset)
        .await?;
    test_db
        .tables
        .create_dataset(&mut test_db.conn, &new_dataset("live"))
        .await?;
    test_db
        .tables
        .mark_dataset_deleted(&mut test_db.conn, marked_id)
        .await?;

    assert_eq!(
        test_db
            .tables
            .get_marked_datasets(&mut test_db.conn)
            .await?,
        vec![MarkedDataset {
            id: marked_id,
            archive_storage_path: marked_dataset.archive_storage_path,
        }]
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn delete_marked_dataset_removes_its_rows_and_keeps_other_datasets() -> Result<()> {
    let mut test_db = create_test_database().await;
    let marked_id = test_db
        .tables
        .create_dataset(&mut test_db.conn, &new_dataset("marked"))
        .await?;
    let other_id = test_db
        .tables
        .create_dataset(&mut test_db.conn, &new_dataset("other"))
        .await?;
    seed_child_rows(&mut test_db, marked_id).await;
    seed_child_rows(&mut test_db, other_id).await;
    test_db
        .tables
        .mark_dataset_deleted(&mut test_db.conn, marked_id)
        .await?;

    assert!(
        test_db
            .tables
            .delete_marked_dataset(&mut test_db.conn, marked_id)
            .await?
    );
    for (table, ids) in get_dataset_ids_in_each_table(&mut test_db).await {
        assert_eq!(ids, vec![other_id], "table {table}");
    }

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn delete_marked_dataset_leaves_an_unmarked_dataset_alone() -> Result<()> {
    let mut test_db = create_test_database().await;
    let id = test_db
        .tables
        .create_dataset(&mut test_db.conn, &new_dataset("live"))
        .await?;
    seed_child_rows(&mut test_db, id).await;
    let ids_by_table = get_dataset_ids_in_each_table(&mut test_db).await;

    assert!(
        !test_db
            .tables
            .delete_marked_dataset(&mut test_db.conn, id)
            .await?
    );
    assert_eq!(
        get_dataset_ids_in_each_table(&mut test_db).await,
        ids_by_table
    );

    test_db.drop_database().await;
    Ok(())
}

/// Creates a test database with the shared metadata tables.
///
/// # Returns
///
/// The newly created test database.
///
/// # Panics
///
/// Panics if the test database or the shared metadata tables can't be created.
async fn create_test_database() -> TestDatabase {
    let mut test_db = TestDatabase::create().await;
    test_db
        .tables
        .create_tables(&mut test_db.conn)
        .await
        .expect("failed to create the shared metadata tables");
    test_db
}

/// # Returns
///
/// The dataset name `name`.
///
/// # Panics
///
/// Panics if `name` isn't a valid dataset name.
fn dataset_name(name: &str) -> DatasetName {
    name.parse().expect("name should be a valid dataset name")
}

/// # Returns
///
/// A dataset to create with the given name, an archive storage path derived from the name, and no
/// retention period.
///
/// # Panics
///
/// Panics if `name` isn't a valid dataset name.
fn new_dataset(name: &str) -> NewDataset {
    NewDataset {
        name: dataset_name(name),
        archive_storage_path: format!("/var/data/archives/{name}"),
        retention_period_minutes: None,
    }
}

/// Inserts one row for the given dataset into each of the archives, long-span archives and column
/// metadata tables.
///
/// # Panics
///
/// Panics if a row can't be inserted.
async fn seed_child_rows(test_db: &mut TestDatabase, dataset_id: DatasetId) {
    let archive_id = sqlx::query(&format!(
        r"
INSERT INTO `{archives}` (
    `dataset_id`,
    `uuid`,
    `begin_timestamp_millisecs`,
    `end_timestamp_millisecs`,
    `num_uncompressed_bytes`,
    `num_compressed_bytes`,
    `creation_timestamp_millisecs`,
    `compression_job_id`
)
VALUES (?, ?, 0, 0, 0, 0, 0, 0)",
        archives = test_db.database_config.unscoped_archives_table_name(),
    ))
    .bind(dataset_id)
    .bind(Uuid::new_v4().as_bytes().as_slice())
    .execute(&mut test_db.conn)
    .await
    .expect("failed to insert an archive row")
    .last_insert_id();

    sqlx::query(&format!(
        r"
INSERT INTO `{long_span_archives}` (
    `dataset_id`, `begin_timestamp_millisecs`, `end_timestamp_millisecs`, `archive_id`
)
VALUES (?, 0, 0, ?)",
        long_span_archives = test_db.database_config.long_span_archives_table_name(),
    ))
    .bind(dataset_id)
    .bind(archive_id)
    .execute(&mut test_db.conn)
    .await
    .expect("failed to insert a long-span archive row");

    sqlx::query(&format!(
        r"
INSERT INTO `{column_metadata}` (`dataset_id`, `name`, `type`)
VALUES (?, 'field', 0)",
        column_metadata = test_db
            .database_config
            .unscoped_column_metadata_table_name(),
    ))
    .bind(dataset_id)
    .execute(&mut test_db.conn)
    .await
    .expect("failed to insert a column metadata row");
}

/// # Returns
///
/// A map from each shared metadata table's name to the dataset IDs of its rows, in ascending order.
///
/// # Panics
///
/// Panics if a table can't be read.
async fn get_dataset_ids_in_each_table(
    test_db: &mut TestDatabase,
) -> HashMap<String, Vec<DatasetId>> {
    let config = &test_db.database_config;
    let tables = [
        (config.datasets_table_name(), "id"),
        (config.unscoped_archives_table_name(), "dataset_id"),
        (config.long_span_archives_table_name(), "dataset_id"),
        (config.unscoped_column_metadata_table_name(), "dataset_id"),
    ];

    let mut ids_by_table = HashMap::new();
    for (table, column) in tables {
        let ids = sqlx::query_scalar(&format!(
            "SELECT `{column}` FROM `{table}` ORDER BY `{column}`"
        ))
        .fetch_all(&mut test_db.conn)
        .await
        .expect("failed to read the dataset IDs of a table");
        ids_by_table.insert(table, ids);
    }
    ids_by_table
}
