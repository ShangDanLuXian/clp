use anyhow::Result;
use clp_metadata_db::ColumnType;
use clp_metadata_db::DatasetId;
use clp_metadata_db::NewDataset;

use super::test_utils::TestDatabase;

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_column_names_by_types_returns_the_datasets_distinct_names_of_the_types_by_name()
-> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset").await;
    let other_dataset_id = create_dataset(&mut test_db, "other").await;
    for (name, column_type) in [
        ("ts_b", ColumnType::Timestamp as i8),
        ("ts_a", ColumnType::Timestamp as i8),
        ("ts_a", ColumnType::DeprecatedDateString as i8),
        ("Ts_c", ColumnType::DeprecatedDateString as i8),
        ("count", ColumnType::Integer as i8),
        ("signed_unknown", -1),
    ] {
        insert_column(&mut test_db, dataset_id, name, column_type).await;
    }
    insert_column(
        &mut test_db,
        other_dataset_id,
        "other_ts",
        ColumnType::Timestamp as i8,
    )
    .await;

    for (types, expected_names) in [
        (
            vec![ColumnType::Timestamp, ColumnType::DeprecatedDateString],
            vec!["Ts_c", "ts_a", "ts_b"],
        ),
        (vec![ColumnType::Unknown], Vec::new()),
    ] {
        assert_eq!(
            test_db
                .tables
                .get_column_names_by_types(&mut test_db.conn, dataset_id, &types)
                .await?,
            expected_names,
            "types {types:?}"
        );
    }

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_column_names_by_types_returns_nothing_for_a_marked_dataset() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset").await;
    insert_column(&mut test_db, dataset_id, "ts", ColumnType::Timestamp as i8).await;
    test_db
        .tables
        .mark_dataset_deleted(&mut test_db.conn, dataset_id)
        .await?;

    assert_eq!(
        test_db
            .tables
            .get_column_names_by_types(&mut test_db.conn, dataset_id, &[ColumnType::Timestamp])
            .await?,
        Vec::<String>::new()
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_column_names_by_types_returns_nothing_for_no_types() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset").await;
    insert_column(&mut test_db, dataset_id, "ts", ColumnType::Timestamp as i8).await;

    assert_eq!(
        test_db
            .tables
            .get_column_names_by_types(&mut test_db.conn, dataset_id, &[])
            .await?,
        Vec::<String>::new()
    );

    test_db.drop_database().await;
    Ok(())
}

/// Creates a dataset with the given name.
///
/// # Returns
///
/// The ID of the created dataset.
///
/// # Panics
///
/// Panics if the dataset can't be created.
async fn create_dataset(test_db: &mut TestDatabase, name: &str) -> DatasetId {
    test_db
        .tables
        .create_dataset(
            &mut test_db.conn,
            &NewDataset {
                name: name.parse().expect("name should be a valid dataset name"),
                archive_storage_path: format!("/var/data/archives/{name}"),
                retention_period_minutes: None,
            },
        )
        .await
        .expect("failed to create a dataset")
}

/// Inserts a column metadata row, as the clp-s indexer does.
///
/// # Panics
///
/// Panics if the row can't be inserted.
async fn insert_column(
    test_db: &mut TestDatabase,
    dataset_id: DatasetId,
    name: &str,
    column_type: i8,
) {
    sqlx::query(&format!(
        "INSERT INTO `{column_metadata}` (`dataset_id`, `name`, `type`) VALUES (?, ?, ?)",
        column_metadata = test_db
            .database_config
            .unscoped_column_metadata_table_name(),
    ))
    .bind(dataset_id)
    .bind(name)
    .bind(column_type)
    .execute(&mut test_db.conn)
    .await
    .expect("failed to insert a column metadata row");
}
