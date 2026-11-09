use anyhow::Result;
use clp_metadata_db::DatasetId;
use clp_metadata_db::NewArchive;
use clp_metadata_db::NewDataset;
use clp_metadata_db::SpaceSavings;
use clp_metadata_db::TimestampBounds;
use clp_rust_utils::types::ArchiveId;

use super::test_utils::TestDatabase;
use super::test_utils::new_archive;

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_timestamp_bounds_covers_live_archives_of_the_live_datasets_among_ids() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset").await;
    let second_dataset_id = create_dataset(&mut test_db, "second").await;
    let unrequested_dataset_id = create_dataset(&mut test_db, "unrequested").await;
    let marked_dataset_id = create_dataset(&mut test_db, "marked").await;
    let marked = new_archive(10, 900);
    insert_archives(
        &mut test_db,
        dataset_id,
        &[new_archive(100, 200), new_archive(0, 0), marked.clone()],
    )
    .await;
    insert_archives(&mut test_db, second_dataset_id, &[new_archive(150, 300)]).await;
    insert_archives(
        &mut test_db,
        unrequested_dataset_id,
        &[new_archive(5, 1000)],
    )
    .await;
    insert_archives(&mut test_db, marked_dataset_id, &[new_archive(1, 2000)]).await;
    mark_archive(&mut test_db, dataset_id, &marked).await;
    test_db
        .tables
        .mark_dataset_deleted(&mut test_db.conn, marked_dataset_id)
        .await?;

    assert_eq!(
        test_db
            .tables
            .get_timestamp_bounds(
                &mut test_db.conn,
                &[dataset_id, second_dataset_id, marked_dataset_id],
            )
            .await?,
        Some(TimestampBounds {
            begin_timestamp_millisecs: 0,
            end_timestamp_millisecs: 300,
        })
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_timestamp_bounds_returns_none_without_archives_or_ids() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset").await;

    for ids in [vec![dataset_id], Vec::new()] {
        assert_eq!(
            test_db
                .tables
                .get_timestamp_bounds(&mut test_db.conn, &ids)
                .await?,
            None,
            "ids {ids:?}"
        );
    }

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_space_savings_sums_live_archives_of_live_datasets() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset").await;
    let marked_dataset_id = create_dataset(&mut test_db, "marked").await;
    let marked = sized_archive(4, 400);
    insert_archives(
        &mut test_db,
        dataset_id,
        &[sized_archive(1, 100), sized_archive(2, 200), marked.clone()],
    )
    .await;
    insert_archives(&mut test_db, marked_dataset_id, &[sized_archive(8, 800)]).await;
    mark_archive(&mut test_db, dataset_id, &marked).await;
    test_db
        .tables
        .mark_dataset_deleted(&mut test_db.conn, marked_dataset_id)
        .await?;

    assert_eq!(
        test_db.tables.get_space_savings(&mut test_db.conn).await?,
        SpaceSavings {
            num_uncompressed_bytes: 300,
            num_compressed_bytes: 3,
        }
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_space_savings_returns_zeros_without_archives() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    create_dataset(&mut test_db, "dataset").await;

    assert_eq!(
        test_db.tables.get_space_savings(&mut test_db.conn).await?,
        SpaceSavings {
            num_uncompressed_bytes: 0,
            num_compressed_bytes: 0,
        }
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_uncompressed_bytes_by_uuids_sums_live_archives_of_live_datasets_among_uuids()
-> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset").await;
    let second_dataset_id = create_dataset(&mut test_db, "second").await;
    let unrequested_dataset_id = create_dataset(&mut test_db, "unrequested").await;
    let marked_dataset_id = create_dataset(&mut test_db, "marked").await;
    let requested = sized_archive(0, 1);
    let marked = sized_archive(0, 4);
    let in_second_dataset = sized_archive(0, 8);
    let in_unrequested_dataset = sized_archive(0, 16);
    let in_marked_dataset = sized_archive(0, 32);
    insert_archives(
        &mut test_db,
        dataset_id,
        &[requested.clone(), sized_archive(0, 2), marked.clone()],
    )
    .await;
    insert_archives(
        &mut test_db,
        second_dataset_id,
        std::slice::from_ref(&in_second_dataset),
    )
    .await;
    insert_archives(
        &mut test_db,
        unrequested_dataset_id,
        std::slice::from_ref(&in_unrequested_dataset),
    )
    .await;
    insert_archives(
        &mut test_db,
        marked_dataset_id,
        std::slice::from_ref(&in_marked_dataset),
    )
    .await;
    mark_archive(&mut test_db, dataset_id, &marked).await;
    test_db
        .tables
        .mark_dataset_deleted(&mut test_db.conn, marked_dataset_id)
        .await?;

    let uuids = [
        requested.uuid,
        marked.uuid,
        in_second_dataset.uuid,
        in_unrequested_dataset.uuid,
        in_marked_dataset.uuid,
        new_archive(0, 0).uuid,
    ];
    assert_eq!(
        test_db
            .tables
            .get_uncompressed_bytes_by_uuids(
                &mut test_db.conn,
                &[dataset_id, second_dataset_id, marked_dataset_id],
                &uuids,
            )
            .await?,
        9
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_uncompressed_bytes_by_uuids_sums_uuids_beyond_the_first_1000() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset").await;
    let first = sized_archive(0, 1);
    let last = sized_archive(0, 2);
    insert_archives(&mut test_db, dataset_id, &[first.clone(), last.clone()]).await;

    let uuids: Vec<ArchiveId> = std::iter::once(first.uuid)
        .chain((0..1000).map(|_| new_archive(0, 0).uuid))
        .chain(std::iter::once(last.uuid))
        .collect();
    assert_eq!(
        test_db
            .tables
            .get_uncompressed_bytes_by_uuids(&mut test_db.conn, &[dataset_id], &uuids)
            .await?,
        3
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_uncompressed_bytes_by_uuids_returns_zero_for_no_datasets_or_uuids() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset").await;
    let archive = sized_archive(0, 1);
    insert_archives(&mut test_db, dataset_id, std::slice::from_ref(&archive)).await;

    for (dataset_ids, uuids) in [
        (Vec::new(), vec![archive.uuid]),
        (vec![dataset_id], Vec::new()),
    ] {
        assert_eq!(
            test_db
                .tables
                .get_uncompressed_bytes_by_uuids(&mut test_db.conn, &dataset_ids, &uuids)
                .await?,
            0,
            "dataset IDs {dataset_ids:?}, UUIDs {uuids:?}"
        );
    }

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

/// Inserts archives into a dataset.
///
/// # Panics
///
/// Panics if the archives can't be inserted.
async fn insert_archives(
    test_db: &mut TestDatabase,
    dataset_id: DatasetId,
    archives: &[NewArchive],
) {
    test_db
        .tables
        .insert_archives(&mut test_db.conn, dataset_id, 1, archives)
        .await
        .expect("failed to insert archives");
}

/// Marks a dataset's archive as deleted.
///
/// # Panics
///
/// Panics if the archive isn't a live archive of the dataset.
async fn mark_archive(test_db: &mut TestDatabase, dataset_id: DatasetId, archive: &NewArchive) {
    let found_archives = test_db
        .tables
        .find_archives_by_uuids(&mut test_db.conn, dataset_id, &[archive.uuid])
        .await
        .expect("failed to find an archive");
    let [found_archive] = found_archives.as_slice() else {
        panic!("the archive should be a live archive of the dataset");
    };
    assert_eq!(
        test_db
            .tables
            .mark_archives_deleted(&mut test_db.conn, dataset_id, &[found_archive.id])
            .await
            .expect("failed to mark an archive as deleted"),
        1
    );
}

/// # Returns
///
/// An archive to insert without timestamps and with the given sizes.
fn sized_archive(num_compressed_bytes: u64, num_uncompressed_bytes: u64) -> NewArchive {
    NewArchive {
        num_uncompressed_bytes,
        num_compressed_bytes,
        ..new_archive(0, 0)
    }
}
