use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroU32;

use anyhow::Result;
use clp_metadata_db::Archive;
use clp_metadata_db::ArchiveRowId;
use clp_metadata_db::DatasetId;
use clp_metadata_db::MarkedArchive;
use clp_metadata_db::NewArchive;
use clp_metadata_db::NewDataset;
use clp_metadata_db::QueryArchive;
use clp_metadata_db::QueryArchiveFilter;
use clp_metadata_db::TimestampRange;
use clp_rust_utils::job_config::CompressionJobId;
use clp_rust_utils::types::ArchiveId;

use super::test_utils::TestDatabase;
use super::test_utils::new_archive;

const DAY_IN_MILLISECS: i64 = 24 * 60 * 60 * 1000;
const MINUTE_IN_MILLISECS: i64 = 60 * 1000;

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn insert_archives_sets_creation_time_from_the_db_clock() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset", None).await;

    let before = get_db_now_millisecs(&mut test_db).await;
    insert_archives(
        &mut test_db,
        dataset_id,
        &[new_archive(0, 0), new_archive(0, 0)],
    )
    .await;
    let after = get_db_now_millisecs(&mut test_db).await;

    let creation_timestamps: Vec<i64> = get_archive_rows(&mut test_db)
        .await
        .into_values()
        .map(|row| row.creation_timestamp_millisecs)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let [creation_timestamp] = creation_timestamps.as_slice() else {
        anyhow::bail!("expected one creation time, got {creation_timestamps:?}");
    };
    assert!(
        (before..=after).contains(creation_timestamp),
        "creation time {creation_timestamp} should be within [{before}, {after}]"
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn insert_archives_stores_archives_and_long_span_rows_beyond_the_first_1000() -> Result<()> {
    const COMPRESSION_JOB_ID: CompressionJobId = 7;

    #[derive(Debug, Eq, Hash, PartialEq, sqlx::FromRow)]
    struct StoredArchive {
        uuid: Vec<u8>,
        dataset_id: DatasetId,
        begin_timestamp_millisecs: i64,
        end_timestamp_millisecs: i64,
        num_uncompressed_bytes: u64,
        num_compressed_bytes: u64,
        compression_job_id: CompressionJobId,
    }

    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset", None).await;
    let archives: Vec<NewArchive> = (0..1001)
        .map(|index| NewArchive {
            num_uncompressed_bytes: 1000 + index,
            num_compressed_bytes: index,
            ..new_archive(
                index.cast_signed(),
                index.cast_signed() + DAY_IN_MILLISECS + 1,
            )
        })
        .collect();

    test_db
        .tables
        .insert_archives(&mut test_db.conn, dataset_id, COMPRESSION_JOB_ID, &archives)
        .await?;

    let stored_archives: HashSet<StoredArchive> = sqlx::query_as::<_, StoredArchive>(&format!(
        r"
SELECT
    `uuid`,
    `dataset_id`,
    `begin_timestamp_millisecs`,
    `end_timestamp_millisecs`,
    `num_uncompressed_bytes`,
    `num_compressed_bytes`,
    `compression_job_id`
FROM `{archives}`",
        archives = test_db.database_config.unscoped_archives_table_name(),
    ))
    .fetch_all(&mut test_db.conn)
    .await?
    .into_iter()
    .collect();
    let expected_archives: HashSet<StoredArchive> = archives
        .iter()
        .map(|archive| StoredArchive {
            uuid: archive.uuid.as_bytes().to_vec(),
            dataset_id,
            begin_timestamp_millisecs: archive.begin_timestamp_millisecs,
            end_timestamp_millisecs: archive.end_timestamp_millisecs,
            num_uncompressed_bytes: archive.num_uncompressed_bytes,
            num_compressed_bytes: archive.num_compressed_bytes,
            compression_job_id: COMPRESSION_JOB_ID,
        })
        .collect();
    assert_eq!(stored_archives, expected_archives);

    let archive_rows = get_archive_rows(&mut test_db).await;
    let expected_long_span_rows: HashSet<_> = archives
        .iter()
        .map(|archive| {
            (
                dataset_id,
                archive.begin_timestamp_millisecs,
                archive.end_timestamp_millisecs,
                archive_rows[&archive.uuid].id,
            )
        })
        .collect();
    assert_eq!(
        get_long_span_rows(&mut test_db).await,
        expected_long_span_rows
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn insert_archives_adds_long_span_rows_only_for_archives_spanning_more_than_24_hours()
-> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset", None).await;
    let long_span_archive = new_archive(1000, 1000 + DAY_IN_MILLISECS + 1);

    insert_archives(
        &mut test_db,
        dataset_id,
        &[
            new_archive(0, 0),
            new_archive(1000, 1000 + DAY_IN_MILLISECS),
            long_span_archive.clone(),
        ],
    )
    .await;

    let archive_rows = get_archive_rows(&mut test_db).await;
    assert_eq!(
        get_long_span_rows(&mut test_db).await,
        HashSet::from([(
            dataset_id,
            long_span_archive.begin_timestamp_millisecs,
            long_span_archive.end_timestamp_millisecs,
            archive_rows[&long_span_archive.uuid].id,
        )])
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn insert_archives_does_nothing_for_no_archives() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset", None).await;

    test_db
        .tables
        .insert_archives(&mut test_db.conn, dataset_id, 1, &[])
        .await?;

    assert_eq!(get_archive_rows(&mut test_db).await, HashMap::new());

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn find_archives_by_time_range_returns_live_archives_of_the_dataset_within_the_range()
-> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset", None).await;
    let other_dataset_id = create_dataset(&mut test_db, "other", None).await;
    let within = new_archive(100, 200);
    let marked = new_archive(120, 180);
    insert_archives(
        &mut test_db,
        dataset_id,
        &[
            within.clone(),
            new_archive(99, 150),
            new_archive(150, 201),
            marked.clone(),
        ],
    )
    .await;
    insert_archives(&mut test_db, other_dataset_id, &[new_archive(100, 200)]).await;
    mark_archives(&mut test_db, dataset_id, &[&marked]).await;

    let archive_rows = get_archive_rows(&mut test_db).await;
    assert_eq!(
        test_db
            .tables
            .find_archives_by_time_range(
                &mut test_db.conn,
                dataset_id,
                &TimestampRange {
                    begin_timestamp_millisecs: 100,
                    end_timestamp_millisecs: 200,
                },
            )
            .await?,
        vec![expected_archive(&within, &archive_rows)]
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn find_archives_by_uuids_returns_live_archives_of_the_dataset_among_uuids() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset", None).await;
    let other_dataset_id = create_dataset(&mut test_db, "other", None).await;
    let requested = new_archive(0, 0);
    let marked = new_archive(0, 0);
    let other = new_archive(0, 0);
    insert_archives(
        &mut test_db,
        dataset_id,
        &[requested.clone(), new_archive(0, 0), marked.clone()],
    )
    .await;
    insert_archives(&mut test_db, other_dataset_id, std::slice::from_ref(&other)).await;
    mark_archives(&mut test_db, dataset_id, &[&marked]).await;

    let archive_rows = get_archive_rows(&mut test_db).await;
    let uuids = [
        requested.uuid,
        marked.uuid,
        other.uuid,
        new_archive(0, 0).uuid,
    ];
    assert_eq!(
        test_db
            .tables
            .find_archives_by_uuids(&mut test_db.conn, dataset_id, &uuids)
            .await?,
        vec![expected_archive(&requested, &archive_rows)]
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn find_archives_by_uuids_finds_uuids_beyond_the_first_1000() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset", None).await;
    let first = new_archive(0, 0);
    let last = new_archive(0, 0);
    insert_archives(&mut test_db, dataset_id, &[first.clone(), last.clone()]).await;

    let archive_rows = get_archive_rows(&mut test_db).await;
    let uuids: Vec<ArchiveId> = std::iter::once(first.uuid)
        .chain((0..1000).map(|_| new_archive(0, 0).uuid))
        .chain(std::iter::once(last.uuid))
        .collect();
    let mut archives = test_db
        .tables
        .find_archives_by_uuids(&mut test_db.conn, dataset_id, &uuids)
        .await?;
    archives.sort_by_key(|archive| archive.uuid.to_string());
    let mut expected_archives = vec![
        expected_archive(&first, &archive_rows),
        expected_archive(&last, &archive_rows),
    ];
    expected_archives.sort_by_key(|archive| archive.uuid.to_string());
    assert_eq!(archives, expected_archives);

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn find_archives_by_uuids_returns_nothing_for_no_uuids() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset", None).await;

    assert_eq!(
        test_db
            .tables
            .find_archives_by_uuids(&mut test_db.conn, dataset_id, &[])
            .await?,
        Vec::new()
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn mark_archives_deleted_marks_only_the_datasets_live_archives_among_ids() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset", None).await;
    let other_dataset_id = create_dataset(&mut test_db, "other", None).await;
    let target = new_archive(0, 0);
    let unrequested = new_archive(0, 0);
    let already_marked = new_archive(0, 0);
    let other = new_archive(0, 0);
    insert_archives(
        &mut test_db,
        dataset_id,
        &[target.clone(), unrequested.clone(), already_marked.clone()],
    )
    .await;
    insert_archives(&mut test_db, other_dataset_id, std::slice::from_ref(&other)).await;
    mark_archives(&mut test_db, dataset_id, &[&already_marked]).await;

    let archive_rows = get_archive_rows(&mut test_db).await;
    let ids = [&target, &already_marked, &other].map(|archive| archive_rows[&archive.uuid].id);
    assert_eq!(
        test_db
            .tables
            .mark_archives_deleted(&mut test_db.conn, dataset_id, &ids)
            .await?,
        1
    );
    assert_eq!(
        get_deletion_states(&mut test_db).await,
        HashMap::from([
            (target.uuid, true),
            (unrequested.uuid, false),
            (already_marked.uuid, true),
            (other.uuid, false),
        ])
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn mark_archives_deleted_marks_archives_beyond_the_first_1000() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset", None).await;
    let archives: Vec<NewArchive> = (0..1001).map(|_| new_archive(0, 0)).collect();
    insert_archives(&mut test_db, dataset_id, &archives).await;

    let ids: Vec<ArchiveRowId> = get_archive_rows(&mut test_db)
        .await
        .into_values()
        .map(|row| row.id)
        .collect();
    assert_eq!(
        test_db
            .tables
            .mark_archives_deleted(&mut test_db.conn, dataset_id, &ids)
            .await?,
        1001
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn find_archives_for_query_selects_live_archives_overlapping_the_time_range() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset", None).await;
    let other_dataset_id = create_dataset(&mut test_db, "other", None).await;
    let before = new_archive(10, 99);
    let after = new_archive(201, 300);
    let ends_at_begin = new_archive(50, 100);
    let begins_at_end = new_archive(200, 250);
    let without_timestamps = new_archive(0, 0);
    let marked = new_archive(120, 180);
    insert_archives(
        &mut test_db,
        dataset_id,
        &[
            before.clone(),
            after.clone(),
            ends_at_begin.clone(),
            begins_at_end.clone(),
            without_timestamps.clone(),
            marked.clone(),
        ],
    )
    .await;
    insert_archives(&mut test_db, other_dataset_id, &[new_archive(120, 180)]).await;
    mark_archives(&mut test_db, dataset_id, &[&marked]).await;

    let bounded_filter = QueryArchiveFilter {
        begin_timestamp_millisecs: Some(100),
        end_timestamp_millisecs: Some(200),
        job_creation_timestamp_millisecs: 0,
    };
    let end_bounded_filter = QueryArchiveFilter {
        begin_timestamp_millisecs: None,
        ..bounded_filter
    };
    let unbounded_filter = QueryArchiveFilter {
        begin_timestamp_millisecs: None,
        end_timestamp_millisecs: None,
        ..bounded_filter
    };
    for (filter, expected_archives) in [
        (&bounded_filter, vec![&ends_at_begin, &begins_at_end]),
        (
            &end_bounded_filter,
            vec![&before, &ends_at_begin, &begins_at_end, &without_timestamps],
        ),
        (
            &unbounded_filter,
            vec![
                &before,
                &after,
                &ends_at_begin,
                &begins_at_end,
                &without_timestamps,
            ],
        ),
    ] {
        assert_eq!(
            find_archives_for_query(&mut test_db, dataset_id, filter).await,
            sorted_query_archives(expected_archives.into_iter().map(expected_query_archive)),
            "filter {filter:?}"
        );
    }

    test_db
        .tables
        .mark_dataset_deleted(&mut test_db.conn, dataset_id)
        .await?;
    assert_eq!(
        find_archives_for_query(&mut test_db, dataset_id, &unbounded_filter).await,
        Vec::new()
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn find_archives_for_query_selects_archives_created_within_retention() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    for (name, retention_period_minutes, job_creation_offsets_and_matches) in [
        (
            "retained",
            Some(10),
            vec![
                (10 * MINUTE_IN_MILLISECS, true),
                (10 * MINUTE_IN_MILLISECS + 1, false),
            ],
        ),
        ("forever", None, vec![(3650 * DAY_IN_MILLISECS, true)]),
        ("longest", Some(u32::MAX), vec![(0, true)]),
    ] {
        let dataset_id = create_dataset(&mut test_db, name, retention_period_minutes).await;
        let archive = new_archive(0, 0);
        insert_archives(&mut test_db, dataset_id, std::slice::from_ref(&archive)).await;
        let creation_timestamp_millisecs =
            get_archive_rows(&mut test_db).await[&archive.uuid].creation_timestamp_millisecs;
        for (offset, matches) in job_creation_offsets_and_matches {
            let filter = QueryArchiveFilter {
                begin_timestamp_millisecs: None,
                end_timestamp_millisecs: None,
                job_creation_timestamp_millisecs: creation_timestamp_millisecs + offset,
            };
            let expected_archives = if matches {
                vec![expected_query_archive(&archive)]
            } else {
                Vec::new()
            };
            assert_eq!(
                find_archives_for_query(&mut test_db, dataset_id, &filter).await,
                expected_archives,
                "dataset {name}, job created {offset} ms after the archive"
            );
        }
    }

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn mark_expired_archives_deleted_marks_archives_older_than_retention_before_db_now()
-> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let retained_dataset_id = create_dataset(&mut test_db, "retained", Some(10)).await;
    let forever_dataset_id = create_dataset(&mut test_db, "forever", None).await;
    let marked_dataset_id = create_dataset(&mut test_db, "marked", Some(10)).await;
    let longest_dataset_id = create_dataset(&mut test_db, "longest", Some(u32::MAX)).await;
    let expired = new_archive(0, 0);
    let recent = new_archive(0, 0);
    let already_marked = new_archive(0, 0);
    let kept_forever = new_archive(0, 0);
    let in_marked_dataset = new_archive(0, 0);
    let kept_longest = new_archive(0, 0);
    insert_archives(
        &mut test_db,
        retained_dataset_id,
        &[expired.clone(), recent.clone(), already_marked.clone()],
    )
    .await;
    insert_archives(
        &mut test_db,
        forever_dataset_id,
        std::slice::from_ref(&kept_forever),
    )
    .await;
    insert_archives(
        &mut test_db,
        marked_dataset_id,
        std::slice::from_ref(&in_marked_dataset),
    )
    .await;
    insert_archives(
        &mut test_db,
        longest_dataset_id,
        std::slice::from_ref(&kept_longest),
    )
    .await;
    mark_archives(&mut test_db, retained_dataset_id, &[&already_marked]).await;
    test_db
        .tables
        .mark_dataset_deleted(&mut test_db.conn, marked_dataset_id)
        .await?;

    let now = get_db_now_millisecs(&mut test_db).await;
    for (archive, age) in [
        (&expired, 11 * MINUTE_IN_MILLISECS),
        (&recent, 9 * MINUTE_IN_MILLISECS),
        (&already_marked, 11 * MINUTE_IN_MILLISECS),
        (&kept_forever, 365 * DAY_IN_MILLISECS),
        (&in_marked_dataset, 11 * MINUTE_IN_MILLISECS),
        (&kept_longest, 11 * MINUTE_IN_MILLISECS),
    ] {
        set_creation_timestamp(&mut test_db, archive, now - age).await;
    }

    assert_eq!(
        test_db
            .tables
            .mark_expired_archives_deleted(&mut test_db.conn, None)
            .await?,
        1
    );
    assert_eq!(
        get_deletion_states(&mut test_db).await,
        HashMap::from([
            (expired.uuid, true),
            (recent.uuid, false),
            (already_marked.uuid, true),
            (kept_forever.uuid, false),
            (in_marked_dataset.uuid, false),
            (kept_longest.uuid, false),
        ])
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn mark_expired_archives_deleted_uses_the_earlier_of_db_now_and_the_query_job_bound()
-> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset", Some(10)).await;
    let older = new_archive(0, 0);
    let newer = new_archive(0, 0);
    insert_archives(&mut test_db, dataset_id, &[older.clone(), newer.clone()]).await;
    let now = get_db_now_millisecs(&mut test_db).await;
    set_creation_timestamp(&mut test_db, &older, now - 11 * MINUTE_IN_MILLISECS).await;
    set_creation_timestamp(&mut test_db, &newer, now - 5 * MINUTE_IN_MILLISECS).await;

    for (earliest_query_job_creation_timestamp_millisecs, num_expected_marked_archives) in [
        (now - 5 * MINUTE_IN_MILLISECS, 0),
        (now + 60 * MINUTE_IN_MILLISECS, 1),
    ] {
        assert_eq!(
            test_db
                .tables
                .mark_expired_archives_deleted(
                    &mut test_db.conn,
                    Some(earliest_query_job_creation_timestamp_millisecs),
                )
                .await?,
            num_expected_marked_archives,
            "bound {earliest_query_job_creation_timestamp_millisecs}, DB now about {now}"
        );
    }
    assert_eq!(
        get_deletion_states(&mut test_db).await,
        HashMap::from([(older.uuid, true), (newer.uuid, false)])
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn get_marked_archives_returns_marked_archives_of_live_datasets_up_to_limit() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset = NewDataset {
        name: "dataset".parse()?,
        archive_storage_path: "/var/data/archives/dataset".to_owned(),
        retention_period_minutes: None,
    };
    let dataset_id = test_db
        .tables
        .create_dataset(&mut test_db.conn, &dataset)
        .await?;
    let marked_dataset_id = create_dataset(&mut test_db, "marked", None).await;
    let first_marked = new_archive(0, 0);
    let second_marked = new_archive(0, 0);
    let in_marked_dataset = new_archive(0, 0);
    insert_archives(
        &mut test_db,
        dataset_id,
        &[
            first_marked.clone(),
            second_marked.clone(),
            new_archive(0, 0),
        ],
    )
    .await;
    insert_archives(
        &mut test_db,
        marked_dataset_id,
        std::slice::from_ref(&in_marked_dataset),
    )
    .await;
    mark_archives(&mut test_db, dataset_id, &[&first_marked, &second_marked]).await;
    mark_archives(&mut test_db, marked_dataset_id, &[&in_marked_dataset]).await;
    test_db
        .tables
        .mark_dataset_deleted(&mut test_db.conn, marked_dataset_id)
        .await?;

    let archive_rows = get_archive_rows(&mut test_db).await;
    let mut expected_archives: Vec<MarkedArchive> = [&first_marked, &second_marked]
        .into_iter()
        .map(|archive| MarkedArchive {
            id: archive_rows[&archive.uuid].id,
            uuid: archive.uuid,
            dataset_id,
            archive_storage_path: dataset.archive_storage_path.clone(),
        })
        .collect();
    expected_archives.sort_by_key(|archive| archive.uuid.to_string());

    let mut archives = test_db
        .tables
        .get_marked_archives(&mut test_db.conn, 10)
        .await?;
    archives.sort_by_key(|archive| archive.uuid.to_string());
    assert_eq!(archives, expected_archives);

    let limited_archives = test_db
        .tables
        .get_marked_archives(&mut test_db.conn, 1)
        .await?;
    assert_eq!(limited_archives.len(), 1);
    assert!(
        expected_archives.contains(&limited_archives[0]),
        "unexpected archive {limited_archives:?}"
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn delete_marked_archives_removes_only_the_datasets_marked_archives_among_ids() -> Result<()>
{
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset", None).await;
    let other_dataset_id = create_dataset(&mut test_db, "other", None).await;
    let target = new_archive(0, 2 * DAY_IN_MILLISECS);
    let unmarked = new_archive(0, 2 * DAY_IN_MILLISECS);
    let unrequested = new_archive(0, 2 * DAY_IN_MILLISECS);
    let other = new_archive(0, 2 * DAY_IN_MILLISECS);
    insert_archives(
        &mut test_db,
        dataset_id,
        &[target.clone(), unmarked.clone(), unrequested.clone()],
    )
    .await;
    insert_archives(&mut test_db, other_dataset_id, std::slice::from_ref(&other)).await;
    mark_archives(&mut test_db, dataset_id, &[&target, &unrequested]).await;
    mark_archives(&mut test_db, other_dataset_id, &[&other]).await;

    let archive_rows = get_archive_rows(&mut test_db).await;
    let ids = [&target, &unmarked, &other].map(|archive| archive_rows[&archive.uuid].id);
    assert_eq!(
        test_db
            .tables
            .delete_marked_archives(&mut test_db.conn, dataset_id, &ids)
            .await?,
        1
    );

    let remaining_archives = [&unmarked, &unrequested, &other];
    assert_eq!(
        get_archive_rows(&mut test_db)
            .await
            .into_keys()
            .collect::<HashSet<_>>(),
        HashSet::from(remaining_archives.map(|archive| archive.uuid))
    );
    assert_eq!(
        get_long_span_rows(&mut test_db)
            .await
            .into_iter()
            .map(|(_, _, _, archive_id)| archive_id)
            .collect::<HashSet<_>>(),
        HashSet::from(remaining_archives.map(|archive| archive_rows[&archive.uuid].id))
    );

    test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
async fn delete_marked_archives_removes_archives_beyond_the_first_1000() -> Result<()> {
    let mut test_db = TestDatabase::create_with_tables().await;
    let dataset_id = create_dataset(&mut test_db, "dataset", None).await;
    let archives: Vec<NewArchive> = (0..1001)
        .map(|_| new_archive(0, 2 * DAY_IN_MILLISECS))
        .collect();
    insert_archives(&mut test_db, dataset_id, &archives).await;
    let ids: Vec<ArchiveRowId> = get_archive_rows(&mut test_db)
        .await
        .into_values()
        .map(|row| row.id)
        .collect();
    test_db
        .tables
        .mark_archives_deleted(&mut test_db.conn, dataset_id, &ids)
        .await?;

    assert_eq!(
        test_db
            .tables
            .delete_marked_archives(&mut test_db.conn, dataset_id, &ids)
            .await?,
        1001
    );
    assert_eq!(get_archive_rows(&mut test_db).await, HashMap::new());
    assert_eq!(get_long_span_rows(&mut test_db).await, HashSet::new());

    test_db.drop_database().await;
    Ok(())
}

/// An archive's row in the archives table, as the tests inspect it.
#[derive(Debug, PartialEq)]
struct ArchiveRow {
    id: ArchiveRowId,
    creation_timestamp_millisecs: i64,
    is_deleted: bool,
}

/// Creates a dataset with the given name and retention period.
///
/// # Returns
///
/// The ID of the created dataset.
///
/// # Panics
///
/// Panics if the dataset can't be created.
async fn create_dataset(
    test_db: &mut TestDatabase,
    name: &str,
    retention_period_minutes: Option<u32>,
) -> DatasetId {
    test_db
        .tables
        .create_dataset(
            &mut test_db.conn,
            &NewDataset {
                name: name.parse().expect("name should be a valid dataset name"),
                archive_storage_path: format!("/var/data/archives/{name}"),
                retention_period_minutes: retention_period_minutes.and_then(NonZeroU32::new),
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

/// Marks a dataset's archives as deleted.
///
/// # Panics
///
/// Panics if an archive doesn't exist or can't be marked.
async fn mark_archives(
    test_db: &mut TestDatabase,
    dataset_id: DatasetId,
    archives: &[&NewArchive],
) {
    let archive_rows = get_archive_rows(test_db).await;
    let ids: Vec<ArchiveRowId> = archives
        .iter()
        .map(|archive| archive_rows[&archive.uuid].id)
        .collect();
    test_db
        .tables
        .mark_archives_deleted(&mut test_db.conn, dataset_id, &ids)
        .await
        .expect("failed to mark archives as deleted");
}

/// Sets an archive's creation time.
///
/// # Panics
///
/// Panics if the archive's row can't be updated.
async fn set_creation_timestamp(
    test_db: &mut TestDatabase,
    archive: &NewArchive,
    creation_timestamp_millisecs: i64,
) {
    sqlx::query(&format!(
        "UPDATE `{archives}` SET `creation_timestamp_millisecs` = ? WHERE `uuid` = ?",
        archives = test_db.database_config.unscoped_archives_table_name(),
    ))
    .bind(creation_timestamp_millisecs)
    .bind(archive.uuid.as_bytes().as_slice())
    .execute(&mut test_db.conn)
    .await
    .expect("failed to set an archive's creation time");
}

/// # Returns
///
/// The DB clock's current time, in milliseconds since the Unix epoch.
///
/// # Panics
///
/// Panics if the DB clock can't be read.
async fn get_db_now_millisecs(test_db: &mut TestDatabase) -> i64 {
    sqlx::query_scalar("SELECT CAST(FLOOR(UNIX_TIMESTAMP(NOW(6)) * 1000) AS SIGNED)")
        .fetch_one(&mut test_db.conn)
        .await
        .expect("failed to read the DB clock")
}

/// # Returns
///
/// A map from each archive's UUID to its row in the archives table.
///
/// # Panics
///
/// Panics if the archives table can't be read.
async fn get_archive_rows(test_db: &mut TestDatabase) -> HashMap<ArchiveId, ArchiveRow> {
    let rows: Vec<(Vec<u8>, ArchiveRowId, i64, bool)> = sqlx::query_as(&format!(
        "SELECT `uuid`, `id`, `creation_timestamp_millisecs`, `is_deleted` FROM `{archives}`",
        archives = test_db.database_config.unscoped_archives_table_name(),
    ))
    .fetch_all(&mut test_db.conn)
    .await
    .expect("failed to read the archives table");
    rows.into_iter()
        .map(|(uuid, id, creation_timestamp_millisecs, is_deleted)| {
            (
                archive_id_from_bytes(&uuid),
                ArchiveRow {
                    id,
                    creation_timestamp_millisecs,
                    is_deleted,
                },
            )
        })
        .collect()
}

/// # Returns
///
/// A map from each archive's UUID to whether it's marked as deleted.
///
/// # Panics
///
/// Panics if the archives table can't be read.
async fn get_deletion_states(test_db: &mut TestDatabase) -> HashMap<ArchiveId, bool> {
    get_archive_rows(test_db)
        .await
        .into_iter()
        .map(|(uuid, row)| (uuid, row.is_deleted))
        .collect()
}

/// # Returns
///
/// The rows of the long-span archives table, as `(dataset_id, begin_timestamp_millisecs,
/// end_timestamp_millisecs, archive_id)`.
///
/// # Panics
///
/// Panics if the long-span archives table can't be read.
async fn get_long_span_rows(
    test_db: &mut TestDatabase,
) -> HashSet<(DatasetId, i64, i64, ArchiveRowId)> {
    let rows: Vec<(DatasetId, i64, i64, ArchiveRowId)> = sqlx::query_as(&format!(
        r"
SELECT `dataset_id`, `begin_timestamp_millisecs`, `end_timestamp_millisecs`, `archive_id`
FROM `{long_span_archives}`",
        long_span_archives = test_db.database_config.long_span_archives_table_name(),
    ))
    .fetch_all(&mut test_db.conn)
    .await
    .expect("failed to read the long-span archives table");
    rows.into_iter().collect()
}

/// # Returns
///
/// The archives that [`clp_metadata_db::MetadataTables::find_archives_for_query`] selects, ordered
/// by UUID.
///
/// # Panics
///
/// Panics if the archives can't be selected.
async fn find_archives_for_query(
    test_db: &mut TestDatabase,
    dataset_id: DatasetId,
    filter: &QueryArchiveFilter,
) -> Vec<QueryArchive> {
    sorted_query_archives(
        test_db
            .tables
            .find_archives_for_query(&mut test_db.conn, dataset_id, filter)
            .await
            .expect("failed to select archives for a query"),
    )
}

/// # Returns
///
/// The archive ID with the UUID bytes stored in the archives table.
///
/// # Panics
///
/// Panics if `bytes` isn't 16 bytes long.
fn archive_id_from_bytes(bytes: &[u8]) -> ArchiveId {
    ArchiveId::from_bytes(bytes.try_into().expect("a stored UUID should be 16 bytes"))
}

/// # Returns
///
/// The [`Archive`] that the archive operations return for an inserted archive.
///
/// # Panics
///
/// Panics if the archive isn't among `archive_rows`.
fn expected_archive(
    archive: &NewArchive,
    archive_rows: &HashMap<ArchiveId, ArchiveRow>,
) -> Archive {
    Archive {
        id: archive_rows[&archive.uuid].id,
        uuid: archive.uuid,
        begin_timestamp_millisecs: archive.begin_timestamp_millisecs,
        end_timestamp_millisecs: archive.end_timestamp_millisecs,
    }
}

/// # Returns
///
/// The [`QueryArchive`] that [`clp_metadata_db::MetadataTables::find_archives_for_query`] returns
/// for an inserted archive.
const fn expected_query_archive(archive: &NewArchive) -> QueryArchive {
    QueryArchive {
        uuid: archive.uuid,
        num_uncompressed_bytes: archive.num_uncompressed_bytes,
        end_timestamp_millisecs: archive.end_timestamp_millisecs,
    }
}

/// # Returns
///
/// The archives ordered by UUID.
fn sorted_query_archives(archives: impl IntoIterator<Item = QueryArchive>) -> Vec<QueryArchive> {
    let mut archives: Vec<QueryArchive> = archives.into_iter().collect();
    archives.sort_by_key(|archive| archive.uuid.to_string());
    archives
}
