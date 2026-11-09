use anyhow::Result;
use clp_metadata_db::Error;

use super::test_utils::TestDatabase;

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn create_tables_creates_every_shared_table_and_is_idempotent() -> Result<()> {
    const TABLE_NAMES_QUERY: &str = r"
SELECT `TABLE_NAME` FROM `information_schema`.`TABLES`
WHERE `TABLE_SCHEMA` = DATABASE()
ORDER BY `TABLE_NAME`";

    let mut test_db = TestDatabase::create().await;

    test_db.tables.create_tables(&mut test_db.conn).await?;
    test_db.tables.create_tables(&mut test_db.conn).await?;

    let table_names: Vec<String> = sqlx::query_scalar(TABLE_NAMES_QUERY)
        .fetch_all(&mut test_db.conn)
        .await?;
    let mut expected_table_names = vec![
        test_db.database_config.datasets_table_name(),
        test_db.database_config.unscoped_archives_table_name(),
        test_db.database_config.long_span_archives_table_name(),
        test_db
            .database_config
            .unscoped_column_metadata_table_name(),
    ];
    expected_table_names.sort();
    assert_eq!(table_names, expected_table_names);

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn create_tables_returns_incompatible_table_if_datasets_table_has_old_shape() -> Result<()> {
    let mut test_db = TestDatabase::create().await;
    let datasets_table = test_db.database_config.datasets_table_name();
    sqlx::query(&format!(
        r"
CREATE TABLE `{datasets_table}` (
    `name` VARCHAR(255) NOT NULL,
    `archive_storage_directory` VARCHAR(4096) NOT NULL,
    PRIMARY KEY (`name`)
)"
    ))
    .execute(&mut test_db.conn)
    .await?;

    let result = test_db.tables.create_tables(&mut test_db.conn).await;

    let Err(Error::IncompatibleTable { table }) = result else {
        anyhow::bail!("expected an incompatible-table error, got {result:?}");
    };
    assert_eq!(table, datasets_table);

    test_db.drop_database().await;
    Ok(())
}
