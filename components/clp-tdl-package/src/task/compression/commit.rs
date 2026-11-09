//! The commit worker that publishes a compression job's archives to CLP's metadata store.

use anyhow::Context;
use clp_metadata_db::MetadataTables;
use clp_metadata_db::NewArchive;
use clp_rust_utils::database::mysql::create_clp_db_mysql_pool;
use clp_rust_utils::dataset::DatasetId;
use clp_rust_utils::job_config::COMPRESSION_JOBS_TABLE_NAME;
use clp_rust_utils::job_config::CompressionJobId;
use clp_rust_utils::job_config::CompressionJobStatus;
use clp_rust_utils::task_io::compression::ArchiveMetadata;
use clp_rust_utils::task_io::compression::CompressionTaskOutput;
use const_format::formatcp;
use spider_core::types::id::JobId;

use crate::common::spider_task_executor_config;
use crate::task::utils::db_credentials_from_env;

/// Publishes a compression job's archives and marks it succeeded.
///
/// Publishes the archives into the job's dataset together with the job's final state in one
/// transaction. A no-op if the job is already [`CompressionJobStatus::Succeeded`].
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`db_credentials_from_env`]'s return values on failure.
/// * Forwards [`create_clp_db_mysql_pool`]'s return values on failure.
/// * Forwards [`sqlx::Pool::begin`]'s return values on failure.
/// * Forwards [`publish_archives`]'s return values on failure.
/// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
pub(super) async fn commit(
    spider_job_id: JobId,
    dataset_id: DatasetId,
    archives: Vec<ArchiveMetadata>,
) -> anyhow::Result<()> {
    tracing::info!(
        spider_job_id = spider_job_id.get(),
        "CLP compression commit task started."
    );

    let config = spider_task_executor_config();
    let pool = create_clp_db_mysql_pool(&config.database, &db_credentials_from_env()?, 1)
        .await
        .context("failed to create the CLP DB connection pool")?;
    let mut tx = pool
        .begin()
        .await
        .context("failed to begin the commit transaction")?;

    let Some(id) = publish_archives(
        &mut tx,
        &MetadataTables::new(&config.database),
        spider_job_id,
        dataset_id,
        &archives,
    )
    .await?
    else {
        return Ok(());
    };

    tx.commit()
        .await
        .context("failed to commit the transaction")?;
    tracing::info!(
        job_id = id,
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

/// Publishes a compression job's archives into its dataset and marks the job succeeded.
///
/// Inserts the archives together with the job's final sizes, completed task count, and duration. A
/// no-op if the job is already [`CompressionJobStatus::Succeeded`].
///
/// # Returns
///
/// The CLP compression job's ID on success, or `None` if the job is already
/// [`CompressionJobStatus::Succeeded`].
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if:
///   * No CLP compression job exists for `spider_job_id`.
///   * The job's status is neither [`CompressionJobStatus::Running`] nor
///     [`CompressionJobStatus::Succeeded`].
///   * The dataset doesn't exist or is marked as deleted.
/// * Forwards [`sqlx::query::QueryAs::fetch_optional`]'s return values on failure.
/// * Forwards [`MetadataTables::lock_live_dataset`]'s return values on failure.
/// * Forwards [`MetadataTables::insert_archives`]'s return values on failure.
/// * Forwards [`mark_job_succeeded`]'s return values on failure.
async fn publish_archives(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    metadata_tables: &MetadataTables,
    spider_job_id: JobId,
    dataset_id: DatasetId,
    archives: &[ArchiveMetadata],
) -> anyhow::Result<Option<CompressionJobId>> {
    const QUERY: &str = formatcp!(
        "SELECT id, status FROM {COMPRESSION_JOBS_TABLE_NAME} WHERE spider_id = ? FOR UPDATE"
    );

    let job: Option<(CompressionJobId, CompressionJobStatus)> = sqlx::query_as(QUERY)
        .bind(spider_job_id.get())
        .fetch_optional(&mut **tx)
        .await
        .context("failed to look up the CLP compression job")?;
    let Some((id, status)) = job else {
        anyhow::bail!(
            "no CLP compression job found for spider job {}",
            spider_job_id.get()
        );
    };
    if status == CompressionJobStatus::Succeeded {
        tracing::info!(
            job_id = id,
            spider_job_id = spider_job_id.get(),
            "CLP compression job already committed; nothing to do."
        );
        return Ok(None);
    }
    if status != CompressionJobStatus::Running {
        anyhow::bail!("CLP compression job {id} is no longer running; refusing to commit");
    }

    if !metadata_tables
        .lock_live_dataset(tx, dataset_id)
        .await
        .context("failed to lock the dataset")?
    {
        anyhow::bail!(
            "the dataset of CLP compression job {id} doesn't exist or is pending deletion; \
             refusing to commit"
        );
    }
    let new_archives: Vec<NewArchive> = archives
        .iter()
        .map(|archive| NewArchive {
            uuid: archive.id,
            begin_timestamp_millisecs: archive.begin_timestamp,
            end_timestamp_millisecs: archive.end_timestamp,
            num_uncompressed_bytes: archive.uncompressed_size,
            num_compressed_bytes: archive.size,
        })
        .collect();
    metadata_tables
        .insert_archives(tx, dataset_id, id, &new_archives)
        .await
        .context("failed to insert the archives")?;
    let total_uncompressed_size: u64 = archives.iter().map(|a| a.uncompressed_size).sum();
    let total_compressed_size: u64 = archives.iter().map(|a| a.size).sum();
    mark_job_succeeded(tx, id, total_uncompressed_size, total_compressed_size).await?;

    Ok(Some(id))
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
    total_uncompressed_size: u64,
    total_compressed_size: u64,
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
    //! Validation of the compression task outputs accepted for commit, and the publication of a
    //! job's archives.

    use std::str::FromStr;

    use clp_metadata_db::MetadataTables;
    use clp_metadata_db::NewDataset;
    use clp_rust_utils::clp_config::package::config::Database;
    use clp_rust_utils::dataset::DatasetId;
    use clp_rust_utils::job_config::COMPRESSION_JOBS_TABLE_NAME;
    use clp_rust_utils::job_config::CompressionJobId;
    use clp_rust_utils::job_config::CompressionJobStatus;
    use clp_rust_utils::task_io::compression::ArchiveMetadata;
    use clp_rust_utils::task_io::compression::CompressionTaskOutput;
    use spider_core::types::id::JobId;
    use sqlx::Connection;
    use sqlx::MySqlConnection;
    use sqlx::mysql::MySqlConnectOptions;

    use super::combine_outputs;
    use super::publish_archives;

    const ARCHIVE: ArchiveMetadata = ArchiveMetadata {
        id: clp_rust_utils::types::ArchiveId::from_bytes([1; 16]),
        begin_timestamp: 1,
        end_timestamp: 2,
        size: 10,
        uncompressed_size: 100,
    };

    #[test]
    fn combine_outputs_rejects_missing_outputs() {
        let error = combine_outputs(vec![]).expect_err("missing task outputs must be rejected");
        assert!(error.to_string().contains("no compression task outputs"));
    }

    #[test]
    fn combine_outputs_rejects_mixed_datasets() {
        let error = combine_outputs(vec![
            CompressionTaskOutput {
                dataset_id: DatasetId::new(1),
                archives: vec![ARCHIVE],
            },
            CompressionTaskOutput {
                dataset_id: DatasetId::new(2),
                archives: vec![ARCHIVE],
            },
        ])
        .expect_err("task outputs from different datasets must not be committed together");
        assert!(error.to_string().contains("more than one dataset"));
    }

    #[tokio::test]
    #[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
    async fn publish_archives_inserts_archives_into_a_live_dataset_and_marks_the_job_succeeded()
    -> anyhow::Result<()> {
        let mut test_db = TestDatabase::create().await;
        let dataset_id = test_db.create_dataset().await;
        let spider_job_id = JobId::random();
        let job_id = test_db
            .insert_job(spider_job_id, CompressionJobStatus::Running)
            .await;
        let archives = [
            ArchiveMetadata {
                id: clp_rust_utils::types::ArchiveId::from_bytes([2; 16]),
                begin_timestamp: 1000,
                end_timestamp: 2000,
                size: 30,
                uncompressed_size: 400,
            },
            ArchiveMetadata {
                id: clp_rust_utils::types::ArchiveId::from_bytes([3; 16]),
                begin_timestamp: 5000,
                end_timestamp: 9000,
                size: 7,
                uncompressed_size: 80,
            },
        ];

        let mut tx = test_db.conn.begin().await?;
        assert_eq!(
            publish_archives(
                &mut tx,
                &test_db.tables,
                spider_job_id,
                dataset_id,
                &archives
            )
            .await?,
            Some(job_id)
        );
        tx.commit().await?;

        assert_eq!(
            get_archive_rows(&mut test_db.conn).await,
            archives
                .iter()
                .map(|archive| ArchiveRow {
                    dataset_id,
                    uuid: archive.id.as_bytes().to_vec(),
                    begin_timestamp_millisecs: archive.begin_timestamp,
                    end_timestamp_millisecs: archive.end_timestamp,
                    num_uncompressed_bytes: archive.uncompressed_size,
                    num_compressed_bytes: archive.size,
                    compression_job_id: job_id,
                })
                .collect::<Vec<_>>()
        );
        assert_eq!(
            test_db.get_job(job_id).await,
            (CompressionJobStatus::Succeeded, 480, 37)
        );

        test_db.drop_database().await;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
    async fn publish_archives_rejects_a_dataset_marked_as_deleted() -> anyhow::Result<()> {
        let mut test_db = TestDatabase::create().await;
        let dataset_id = test_db.create_dataset().await;
        test_db.mark_dataset_deleted(dataset_id).await;
        let spider_job_id = JobId::random();
        let job_id = test_db
            .insert_job(spider_job_id, CompressionJobStatus::Running)
            .await;

        let mut tx = test_db.conn.begin().await?;
        let error = publish_archives(
            &mut tx,
            &test_db.tables,
            spider_job_id,
            dataset_id,
            &[ARCHIVE],
        )
        .await
        .expect_err("archives must not be published into a dataset marked as deleted");
        assert!(
            error
                .to_string()
                .contains("doesn't exist or is pending deletion"),
            "unexpected error: {error}"
        );
        assert_eq!(get_archive_rows(&mut tx).await, vec![]);
        tx.rollback().await?;

        assert_eq!(
            test_db.get_job(job_id).await,
            (CompressionJobStatus::Running, 0, 0)
        );

        test_db.drop_database().await;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Requires a MariaDB or MySQL server; set CLP_METADATA_DB_TEST_URL"]
    async fn publish_archives_is_a_no_op_for_a_succeeded_job_even_if_its_dataset_is_marked()
    -> anyhow::Result<()> {
        let mut test_db = TestDatabase::create().await;
        let dataset_id = test_db.create_dataset().await;
        test_db.mark_dataset_deleted(dataset_id).await;
        let spider_job_id = JobId::random();
        test_db
            .insert_job(spider_job_id, CompressionJobStatus::Succeeded)
            .await;

        let mut tx = test_db.conn.begin().await?;
        assert_eq!(
            publish_archives(
                &mut tx,
                &test_db.tables,
                spider_job_id,
                dataset_id,
                &[ARCHIVE]
            )
            .await?,
            None
        );
        assert_eq!(get_archive_rows(&mut tx).await, vec![]);
        tx.rollback().await?;

        test_db.drop_database().await;
        Ok(())
    }

    /// An archive row as the tests read it back from the archives table.
    #[derive(Debug, Eq, PartialEq, sqlx::FromRow)]
    struct ArchiveRow {
        dataset_id: DatasetId,
        uuid: Vec<u8>,
        begin_timestamp_millisecs: i64,
        end_timestamp_millisecs: i64,
        num_uncompressed_bytes: u64,
        num_compressed_bytes: u64,
        compression_job_id: CompressionJobId,
    }

    /// A uniquely named database with the shared metadata tables and a compression jobs table on
    /// the test server, used by a single test.
    struct TestDatabase {
        conn: MySqlConnection,
        tables: MetadataTables,
        name: String,
    }

    impl TestDatabase {
        /// Factory function.
        ///
        /// Creates a uniquely named database on the server at `CLP_METADATA_DB_TEST_URL` (e.g.
        /// `mysql://root:password@127.0.0.1:3306`), connects to it, and creates the shared metadata
        /// tables and the compression jobs table's columns that the commit task uses.
        ///
        /// # Returns
        ///
        /// The newly created test database.
        ///
        /// # Panics
        ///
        /// Panics if:
        ///
        /// * `CLP_METADATA_DB_TEST_URL` isn't set or isn't a valid MySQL URL.
        /// * The database or its tables can't be created, or the database can't be connected to.
        async fn create() -> Self {
            let server_url = std::env::var("CLP_METADATA_DB_TEST_URL").expect(
                "`CLP_METADATA_DB_TEST_URL` should be set to a MariaDB or MySQL server URL",
            );
            let server_options = MySqlConnectOptions::from_str(&server_url)
                .expect("`CLP_METADATA_DB_TEST_URL` should be a valid MySQL URL");
            let name = format!("clp_tdl_package_test_{}", uuid::Uuid::new_v4().simple());

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

            let mut conn = MySqlConnection::connect_with(&server_options.database(&name))
                .await
                .expect("failed to connect to the test database");
            let tables = MetadataTables::new(&Database::default());
            tables
                .create_tables(&mut conn)
                .await
                .expect("failed to create the shared metadata tables");
            sqlx::query(&format!(
                r"
CREATE TABLE `{COMPRESSION_JOBS_TABLE_NAME}` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `status` INT NOT NULL,
    `start_time` DATETIME(3) NULL DEFAULT NULL,
    `duration` FLOAT NULL DEFAULT NULL,
    `uncompressed_size` BIGINT NOT NULL DEFAULT '0',
    `compressed_size` BIGINT NOT NULL DEFAULT '0',
    `num_tasks` INT NOT NULL DEFAULT '0',
    `num_tasks_completed` INT NOT NULL DEFAULT '0',
    `spider_id` BIGINT UNSIGNED NULL DEFAULT NULL,
    PRIMARY KEY (`id`)
)"
            ))
            .execute(&mut conn)
            .await
            .expect("failed to create the compression jobs table");
            Self { conn, tables, name }
        }

        /// Creates a live dataset.
        ///
        /// # Returns
        ///
        /// The created dataset's ID.
        ///
        /// # Panics
        ///
        /// Panics if the dataset can't be created.
        async fn create_dataset(&mut self) -> DatasetId {
            self.tables
                .create_dataset(
                    &mut self.conn,
                    &NewDataset {
                        name: "logs".parse().expect("test dataset name should be valid"),
                        archive_storage_path: "/var/data/archives/logs".to_owned(),
                        retention_period_minutes: None,
                    },
                )
                .await
                .expect("failed to create the dataset")
        }

        /// Marks a live dataset as deleted.
        ///
        /// # Panics
        ///
        /// Panics if the dataset can't be marked.
        async fn mark_dataset_deleted(&mut self, id: DatasetId) {
            assert!(
                self.tables
                    .mark_dataset_deleted(&mut self.conn, id)
                    .await
                    .expect("failed to mark the dataset as deleted")
            );
        }

        /// Inserts a compression job that was submitted to Spider as `spider_job_id`.
        ///
        /// # Returns
        ///
        /// The inserted job's ID.
        ///
        /// # Panics
        ///
        /// Panics if the job can't be inserted.
        async fn insert_job(
            &mut self,
            spider_job_id: JobId,
            status: CompressionJobStatus,
        ) -> CompressionJobId {
            let id = sqlx::query(&format!(
                "INSERT INTO `{COMPRESSION_JOBS_TABLE_NAME}` (`status`, `spider_id`, `num_tasks`, \
                 `start_time`) VALUES (?, ?, 1, CURRENT_TIMESTAMP(3))"
            ))
            .bind(status)
            .bind(spider_job_id.get())
            .execute(&mut self.conn)
            .await
            .expect("failed to insert the compression job")
            .last_insert_id();
            CompressionJobId::try_from(id).expect("the test job's ID should fit in `i32`")
        }

        /// # Returns
        ///
        /// The status, uncompressed size and compressed size of a compression job.
        ///
        /// # Panics
        ///
        /// Panics if the job can't be read.
        async fn get_job(&mut self, id: CompressionJobId) -> (CompressionJobStatus, i64, i64) {
            sqlx::query_as(&format!(
                "SELECT `status`, `uncompressed_size`, `compressed_size` FROM \
                 `{COMPRESSION_JOBS_TABLE_NAME}` WHERE `id` = ?"
            ))
            .bind(id)
            .fetch_one(&mut self.conn)
            .await
            .expect("failed to read the compression job")
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

    /// # Returns
    ///
    /// Every row in the archives table, ordered by begin timestamp.
    ///
    /// # Panics
    ///
    /// Panics if the archives table can't be read.
    async fn get_archive_rows(conn: &mut MySqlConnection) -> Vec<ArchiveRow> {
        sqlx::query_as(&format!(
            r"
SELECT
    `dataset_id`,
    `uuid`,
    `begin_timestamp_millisecs`,
    `end_timestamp_millisecs`,
    `num_uncompressed_bytes`,
    `num_compressed_bytes`,
    `compression_job_id`
FROM `{archives}`
ORDER BY `begin_timestamp_millisecs`",
            archives = Database::default().unscoped_archives_table_name(),
        ))
        .fetch_all(conn)
        .await
        .expect("failed to read the archives table")
    }
}
