//! The commit worker that publishes a compression job's archives to CLP's metadata store.

use anyhow::Context;
use clp_metadata_db::ArchiveDescriptor;
use clp_metadata_db::ArchiveManagement;
use clp_metadata_db::ArchiveTimestampRange;
use clp_metadata_db::ClpDbClient;
use clp_rust_utils::database::mysql::create_clp_db_mysql_pool;
use clp_rust_utils::job_config::COMPRESSION_JOBS_TABLE_NAME;
use clp_rust_utils::job_config::CompressionJobId;
use clp_rust_utils::job_config::CompressionJobStatus;
use clp_rust_utils::task_io::compression::ArchiveMetadata;
use clp_rust_utils::task_io::compression::CompressionTaskOutput;
use clp_rust_utils::types::DatasetId;
use const_format::formatcp;
use spider_core::types::id::JobId;
use tokio::sync::OnceCell;

use crate::common::spider_task_executor_config;
use crate::task::utils::db_credentials_from_env;

/// # Returns
///
/// The task executor process's metadata database client, created on first use, on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`OnceCell::get_or_try_init`]'s return values on failure.
/// * Forwards [`db_credentials_from_env`]'s return values on failure.
/// * Forwards [`create_clp_db_mysql_pool`]'s return values on failure.
pub(super) async fn clp_db_client() -> anyhow::Result<&'static ClpDbClient> {
    // A task executor process runs one task at a time.
    const MAX_CONNECTIONS: u32 = 1;

    static CLIENT: OnceCell<ClpDbClient> = OnceCell::const_new();

    CLIENT
        .get_or_try_init(|| async {
            let config = spider_task_executor_config();
            let pool = create_clp_db_mysql_pool(
                &config.database,
                &db_credentials_from_env()?,
                MAX_CONNECTIONS,
            )
            .await
            .context("failed to create the CLP DB connection pool")?;
            Ok(ClpDbClient::new(pool, &config.database))
        })
        .await
}

/// Publishes a compression job's archives into the dataset `dataset_id` and marks the job
/// succeeded.
///
/// Inserts the archives together with the job's final sizes, completed task count, and duration in
/// one transaction. A no-op if the job is already [`CompressionJobStatus::Succeeded`].
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if the job's status is neither [`CompressionJobStatus::Running`] nor
///   [`CompressionJobStatus::Succeeded`].
/// * Forwards [`archive_descriptor`]'s return values on failure.
/// * Forwards [`ClpDbClient::begin`]'s return values on failure.
/// * Forwards [`lock_compression_job`]'s return values on failure.
/// * Forwards [`ArchiveManagement::insert_archives`]'s return values on failure.
/// * Forwards [`mark_job_succeeded`]'s return values on failure.
/// * Forwards [`clp_metadata_db::ClpDbTransaction::commit`]'s return values on failure.
pub(super) async fn commit(
    client: &ClpDbClient,
    spider_job_id: JobId,
    dataset_id: DatasetId,
    archives: Vec<ArchiveMetadata>,
) -> anyhow::Result<()> {
    tracing::info!(
        spider_job_id = spider_job_id.get(),
        "CLP compression commit task started."
    );
    let total_uncompressed_size: i64 = archives.iter().map(|a| a.uncompressed_size).sum();
    let total_compressed_size: i64 = archives.iter().map(|a| a.size).sum();
    let archives = archives
        .iter()
        .map(archive_descriptor)
        .collect::<anyhow::Result<Vec<_>>>()?;

    let mut transaction = client
        .begin()
        .await
        .context("failed to begin the commit transaction")?;

    let job = lock_compression_job(transaction.sqlx_transaction(), spider_job_id).await?;
    if job.status == CompressionJobStatus::Succeeded {
        tracing::info!(
            job_id = job.id,
            spider_job_id = spider_job_id.get(),
            "CLP compression job already committed; nothing to do."
        );
        return Ok(());
    }
    if job.status != CompressionJobStatus::Running {
        anyhow::bail!(
            "CLP compression job {} is no longer running; refusing to commit",
            job.id
        );
    }

    transaction
        .insert_archives(dataset_id, job.id, &archives)
        .await
        .with_context(|| format!("failed to insert archives into dataset {dataset_id}"))?;
    mark_job_succeeded(
        transaction.sqlx_transaction(),
        job.id,
        total_uncompressed_size,
        total_compressed_size,
    )
    .await?;

    transaction
        .commit()
        .await
        .context("failed to commit the transaction")?;
    tracing::info!(
        job_id = job.id,
        num_archives = archives.len(),
        "CLP compression commit task completed successfully."
    );
    Ok(())
}

/// Combines compression outputs belonging to one dataset.
///
/// # Returns
///
/// The combined output on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if outputs are missing or belong to different datasets.
pub(super) fn combine_outputs(
    outputs: Vec<CompressionTaskOutput>,
) -> anyhow::Result<CompressionTaskOutput> {
    let mut outputs = outputs.into_iter();
    let mut combined = outputs
        .next()
        .ok_or_else(|| anyhow::anyhow!("commit received no compression task outputs"))?;
    for output in outputs {
        if output.dataset_id != combined.dataset_id {
            anyhow::bail!("compression task outputs belong to more than one dataset");
        }
        combined.archives.extend(output.archives);
    }
    Ok(combined)
}

/// A compression job, as the commit task locks it.
#[derive(sqlx::FromRow)]
struct CompressionJob {
    id: CompressionJobId,
    status: CompressionJobStatus,
}

/// Converts the metadata `clp-s` reports for an archive into the fields the archive is inserted
/// with.
///
/// # Returns
///
/// The archive's descriptor on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if the archive's compressed or uncompressed size is negative.
/// * Forwards [`ArchiveTimestampRange::new`]'s return values on failure.
fn archive_descriptor(archive: &ArchiveMetadata) -> anyhow::Result<ArchiveDescriptor> {
    let ArchiveMetadata {
        uuid,
        begin_timestamp,
        end_timestamp,
        size,
        uncompressed_size,
    } = *archive;
    let timestamp_range = ArchiveTimestampRange::new(begin_timestamp, end_timestamp)
        .with_context(|| format!("archive {uuid} has an invalid timestamp range"))?;
    let num_uncompressed_bytes = u64::try_from(uncompressed_size).with_context(|| {
        format!("archive {uuid} has a negative uncompressed size {uncompressed_size}")
    })?;
    let num_compressed_bytes = u64::try_from(size)
        .with_context(|| format!("archive {uuid} has a negative compressed size {size}"))?;
    Ok(ArchiveDescriptor {
        uuid,
        timestamp_range,
        num_uncompressed_bytes,
        num_compressed_bytes,
    })
}

/// Locks the compression job that the Spider job `spider_job_id` runs.
///
/// # Returns
///
/// The locked job on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if no CLP compression job exists for `spider_job_id`.
/// * Forwards [`sqlx::query::QueryAs::fetch_optional`]'s return values on failure.
async fn lock_compression_job(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    spider_job_id: JobId,
) -> anyhow::Result<CompressionJob> {
    const QUERY: &str = formatcp!(
        "SELECT id, status FROM {COMPRESSION_JOBS_TABLE_NAME} WHERE spider_id = ? FOR UPDATE"
    );

    sqlx::query_as(QUERY)
        .bind(spider_job_id.get())
        .fetch_optional(&mut **tx)
        .await
        .context("failed to look up the CLP compression job")?
        .with_context(|| {
            format!(
                "no CLP compression job found for spider job {}",
                spider_job_id.get()
            )
        })
}

/// Marks a running compression job succeeded with its final sizes, completed task count, and
/// duration.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if the update does not affect exactly one running job.
/// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
async fn mark_job_succeeded(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    id: CompressionJobId,
    total_uncompressed_size: i64,
    total_compressed_size: i64,
) -> anyhow::Result<()> {
    // NOTE: `duration` (seconds) is derived from the DB clock so it stays consistent with the DB's
    // time.
    const QUERY: &str = formatcp!(
        "UPDATE {COMPRESSION_JOBS_TABLE_NAME} SET status = ?, uncompressed_size = ?, \
         compressed_size = ?, num_tasks_completed = num_tasks, duration = \
         TIMESTAMPDIFF(MICROSECOND, start_time, CURRENT_TIMESTAMP(3)) / 1000000 WHERE id = ? AND \
         status = ?"
    );
    let query_result = sqlx::query(QUERY)
        .bind(CompressionJobStatus::Succeeded)
        .bind(total_uncompressed_size)
        .bind(total_compressed_size)
        .bind(id)
        .bind(CompressionJobStatus::Running)
        .execute(&mut **tx)
        .await
        .context("failed to mark the CLP compression job succeeded")?;

    if query_result.rows_affected() != 1 {
        anyhow::bail!(
            "failed to mark CLP compression job {} succeeded; expected 1 row to be updated, got {}",
            id,
            query_result.rows_affected()
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    //! Tests of the compression commit: the outputs it accepts, the archives it inserts, and its
    //! transaction against a MariaDB or MySQL server.
    //!
    //! The database tests are ignored by default. To run them, set
    //! `CLP_METADATA_DB_TEST_SERVER_URL` to the URL of a server without a database (e.g.
    //! `mysql://user:password@127.0.0.1:3306`) whose user can create and drop databases, and run
    //! `cargo test -p clp-tdl-package -- --ignored`.

    use clp_metadata_db::ArchiveDescriptor;
    use clp_metadata_db::ArchiveTimestampRange;
    use clp_metadata_db::ClpDbClient;
    use clp_metadata_db::DatasetConfig;
    use clp_metadata_db::DatasetDescriptor;
    use clp_metadata_db::DatasetManagement;
    use clp_metadata_db::Error;
    use clp_rust_utils::clp_config::package::config::Database;
    use clp_rust_utils::job_config::COMPRESSION_JOBS_TABLE_NAME;
    use clp_rust_utils::job_config::CompressionJobId;
    use clp_rust_utils::job_config::CompressionJobStatus;
    use clp_rust_utils::task_io::compression::ArchiveMetadata;
    use clp_rust_utils::task_io::compression::CompressionTaskOutput;
    use clp_rust_utils::types::ArchiveUuid;
    use clp_rust_utils::types::DatasetId;
    use const_format::formatcp;
    use spider_core::types::id::JobId;
    use sqlx::MySqlPool;
    use sqlx::mysql::MySqlPoolOptions;

    use super::archive_descriptor;
    use super::combine_outputs;
    use super::commit;

    const BASE_ARCHIVE: ArchiveMetadata = ArchiveMetadata {
        uuid: ArchiveUuid::from_bytes([1; 16]),
        begin_timestamp: 1,
        end_timestamp: 2,
        size: 10,
        uncompressed_size: 100,
    };

    /// Recreates the database `clp_tdl_package_test_<name>` with the shared metadata tables and a
    /// compression jobs table.
    ///
    /// # Returns
    ///
    /// The database's pool, and a metadata database client on it.
    ///
    /// # Panics
    ///
    /// Panics if `CLP_METADATA_DB_TEST_SERVER_URL` isn't set, or the database can't be recreated.
    async fn create_test_db(name: &str) -> (MySqlPool, ClpDbClient) {
        const CREATE_COMPRESSION_JOBS_TABLE: &str = formatcp!(
            "CREATE TABLE `{COMPRESSION_JOBS_TABLE_NAME}` (`id` INT NOT NULL AUTO_INCREMENT, \
             `status` INT NOT NULL, `start_time` DATETIME(3) NULL DEFAULT NULL, `duration` FLOAT \
             NULL DEFAULT NULL, `uncompressed_size` BIGINT NOT NULL DEFAULT '0', \
             `compressed_size` BIGINT NOT NULL DEFAULT '0', `num_tasks` INT NOT NULL DEFAULT '0', \
             `num_tasks_completed` INT NOT NULL DEFAULT '0', `spider_id` BIGINT UNSIGNED NULL \
             DEFAULT NULL, PRIMARY KEY (`id`))"
        );

        let server_url = std::env::var("CLP_METADATA_DB_TEST_SERVER_URL")
            .expect("`CLP_METADATA_DB_TEST_SERVER_URL` should be set");
        let database = format!("clp_tdl_package_test_{name}");
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
        let client = ClpDbClient::new(pool.clone(), &Database::default());
        client
            .create_tables()
            .await
            .expect("the shared metadata tables should be created");
        sqlx::query(CREATE_COMPRESSION_JOBS_TABLE)
            .execute(&pool)
            .await
            .expect("the compression jobs table should be created");
        (pool, client)
    }

    /// Creates the dataset `name` in its own transaction.
    ///
    /// # Returns
    ///
    /// The dataset's ID.
    ///
    /// # Panics
    ///
    /// Panics if the dataset can't be created.
    async fn create_dataset(client: &ClpDbClient, name: &str) -> DatasetId {
        let mut transaction = client.begin().await.expect("a transaction should begin");
        let id = transaction
            .create_or_get_dataset(&DatasetDescriptor {
                name: name.parse().expect("the test dataset name should be valid"),
                config: DatasetConfig {
                    archive_storage_path: format!("/archives/{name}"),
                    retention_period_minutes: None,
                },
            })
            .await
            .expect("the dataset should be created");
        transaction
            .commit()
            .await
            .expect("the dataset should be committed");
        id
    }

    /// Inserts a compression job with the status `status` and two tasks, run by the Spider job
    /// `spider_job_id`.
    ///
    /// # Returns
    ///
    /// The job's ID.
    ///
    /// # Panics
    ///
    /// Panics if the job can't be inserted.
    async fn insert_job(
        pool: &MySqlPool,
        spider_job_id: JobId,
        status: CompressionJobStatus,
    ) -> CompressionJobId {
        const QUERY: &str = formatcp!(
            "INSERT INTO `{COMPRESSION_JOBS_TABLE_NAME}` (`status`, `start_time`, `num_tasks`, \
             `spider_id`) VALUES (?, CURRENT_TIMESTAMP(3), 2, ?)"
        );

        let result = sqlx::query(QUERY)
            .bind(status)
            .bind(spider_job_id.get())
            .execute(pool)
            .await
            .expect("the compression job should be inserted");
        CompressionJobId::try_from(result.last_insert_id())
            .expect("the compression job ID should fit")
    }

    /// # Returns
    ///
    /// The status, uncompressed size, compressed size and number of completed tasks of the
    /// compression job `id`.
    ///
    /// # Panics
    ///
    /// Panics if the job can't be read.
    async fn read_job(
        pool: &MySqlPool,
        id: CompressionJobId,
    ) -> (CompressionJobStatus, i64, i64, i32) {
        const QUERY: &str = formatcp!(
            "SELECT `status`, `uncompressed_size`, `compressed_size`, `num_tasks_completed` FROM \
             `{COMPRESSION_JOBS_TABLE_NAME}` WHERE `id` = ?"
        );

        sqlx::query_as(QUERY)
            .bind(id)
            .fetch_one(pool)
            .await
            .expect("the compression job should be readable")
    }

    /// # Returns
    ///
    /// The UUIDs of the dataset's archives, ordered by descending end timestamp.
    ///
    /// # Panics
    ///
    /// Panics if the archives can't be read.
    async fn archive_uuids(client: &ClpDbClient, dataset_id: DatasetId) -> Vec<ArchiveUuid> {
        let query = format!(
            "SELECT `uuid` FROM `{}` WHERE `dataset_id` = ? ORDER BY `end_timestamp_millisecs` \
             DESC",
            Database::default().metadata_tables().archives()
        );
        let mut transaction = client.begin().await.expect("a transaction should begin");
        sqlx::query_scalar::<_, Vec<u8>>(&query)
            .bind(dataset_id)
            .fetch_all(&mut **transaction.sqlx_transaction())
            .await
            .expect("the archives should be readable")
            .into_iter()
            .map(|uuid| {
                ArchiveUuid::from_bytes(
                    uuid.try_into()
                        .expect("a stored archive UUID should be 16 bytes"),
                )
            })
            .collect()
    }

    #[test]
    fn combine_outputs_rejects_missing_outputs() {
        let error = combine_outputs(vec![]).expect_err("missing task outputs must be rejected");
        assert!(error.to_string().contains("no compression task outputs"));
    }

    #[test]
    fn combine_outputs_rejects_mixed_datasets() {
        let error = combine_outputs(vec![
            CompressionTaskOutput {
                dataset_id: DatasetId::from(1),
                archives: vec![BASE_ARCHIVE],
            },
            CompressionTaskOutput {
                dataset_id: DatasetId::from(2),
                archives: vec![BASE_ARCHIVE],
            },
        ])
        .expect_err("task outputs from different datasets must not be committed together");
        assert!(error.to_string().contains("more than one dataset"));
    }

    #[test]
    fn archive_descriptor_maps_the_sizes_and_rejects_negative_ones() -> anyhow::Result<()> {
        let ArchiveDescriptor {
            uuid,
            timestamp_range,
            num_uncompressed_bytes,
            num_compressed_bytes,
        } = archive_descriptor(&BASE_ARCHIVE)?;
        assert_eq!(uuid, BASE_ARCHIVE.uuid);
        assert_eq!(timestamp_range, ArchiveTimestampRange::new(1, 2)?);
        assert_eq!(num_uncompressed_bytes, 100);
        assert_eq!(num_compressed_bytes, 10);

        for (archive, expected_message) in [
            (
                ArchiveMetadata {
                    size: -1,
                    ..BASE_ARCHIVE
                },
                format!(
                    "archive {} has a negative compressed size -1",
                    BASE_ARCHIVE.uuid
                ),
            ),
            (
                ArchiveMetadata {
                    uncompressed_size: -1,
                    ..BASE_ARCHIVE
                },
                format!(
                    "archive {} has a negative uncompressed size -1",
                    BASE_ARCHIVE.uuid
                ),
            ),
        ] {
            let error = archive_descriptor(&archive).expect_err("a negative size must be rejected");
            assert_eq!(error.to_string(), expected_message);
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a MariaDB or MySQL server"]
    async fn commit_inserts_the_archives_and_marks_the_job_succeeded() -> anyhow::Result<()> {
        let (pool, client) = create_test_db("commit_succeeds").await;
        let other_dataset_id = create_dataset(&client, "other").await;
        let dataset_id = create_dataset(&client, "logs").await;
        let spider_job_id = JobId::from(1);
        let job_id = insert_job(&pool, spider_job_id, CompressionJobStatus::Running).await;
        let other_archive = ArchiveMetadata {
            uuid: ArchiveUuid::from_bytes([2; 16]),
            begin_timestamp: 0,
            end_timestamp: 0,
            size: 20,
            uncompressed_size: 200,
        };

        commit(
            &client,
            spider_job_id,
            dataset_id,
            vec![BASE_ARCHIVE, other_archive.clone()],
        )
        .await?;
        assert_eq!(
            archive_uuids(&client, dataset_id).await,
            vec![BASE_ARCHIVE.uuid, other_archive.uuid]
        );
        assert_eq!(archive_uuids(&client, other_dataset_id).await, vec![]);
        assert_eq!(
            read_job(&pool, job_id).await,
            (CompressionJobStatus::Succeeded, 300, 30, 2)
        );

        // A retried commit of a committed job changes nothing.
        commit(&client, spider_job_id, dataset_id, vec![BASE_ARCHIVE]).await?;
        assert_eq!(
            archive_uuids(&client, dataset_id).await,
            vec![BASE_ARCHIVE.uuid, other_archive.uuid]
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a MariaDB or MySQL server"]
    async fn commit_into_a_deleted_dataset_fails_and_leaves_the_job_running() -> anyhow::Result<()>
    {
        let (pool, client) = create_test_db("commit_into_deleted_dataset").await;
        let dataset_id = create_dataset(&client, "logs").await;
        sqlx::query(&format!(
            "UPDATE `{}` SET `is_deleted` = TRUE WHERE `name` = ?",
            Database::default().metadata_tables().datasets()
        ))
        .bind("logs")
        .execute(&pool)
        .await?;
        let spider_job_id = JobId::from(1);
        let job_id = insert_job(&pool, spider_job_id, CompressionJobStatus::Running).await;

        let error = commit(&client, spider_job_id, dataset_id, vec![BASE_ARCHIVE])
            .await
            .expect_err("a commit into a deleted dataset must fail");
        assert!(matches!(
            error.downcast_ref::<Error>(),
            Some(Error::DatasetDeleted)
        ));
        assert_eq!(
            read_job(&pool, job_id).await,
            (CompressionJobStatus::Running, 0, 0, 0)
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a MariaDB or MySQL server"]
    async fn commit_refuses_a_job_that_isnt_running_or_doesnt_exist() -> anyhow::Result<()> {
        let (pool, client) = create_test_db("commit_refuses_job").await;
        let dataset_id = create_dataset(&client, "logs").await;
        let failed_spider_job_id = JobId::from(1);
        let failed_job_id =
            insert_job(&pool, failed_spider_job_id, CompressionJobStatus::Failed).await;

        let error = commit(
            &client,
            failed_spider_job_id,
            dataset_id,
            vec![BASE_ARCHIVE],
        )
        .await
        .expect_err("a commit of a failed job must fail");
        assert_eq!(
            error.to_string(),
            format!("CLP compression job {failed_job_id} is no longer running; refusing to commit")
        );
        assert_eq!(
            read_job(&pool, failed_job_id).await,
            (CompressionJobStatus::Failed, 0, 0, 0)
        );

        let error = commit(&client, JobId::from(2), dataset_id, vec![BASE_ARCHIVE])
            .await
            .expect_err("a commit without a compression job must fail");
        assert_eq!(
            error.to_string(),
            "no CLP compression job found for spider job 2"
        );

        assert_eq!(archive_uuids(&client, dataset_id).await, vec![]);
        Ok(())
    }
}
