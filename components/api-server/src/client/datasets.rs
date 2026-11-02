//! Management of the clp-s storage engine's datasets and their archives.

use std::collections::HashSet;
use std::io::ErrorKind;
use std::num::NonZeroU32;
use std::path::Path;

use clp_rust_utils::aws::AWS_DEFAULT_REGION;
use clp_rust_utils::clp_config::S3Config;
use clp_rust_utils::clp_config::package::config::ArchiveOutputStorage;
use clp_rust_utils::clp_config::package::config::StorageEngine;
use clp_rust_utils::database::mysql::archive;
use clp_rust_utils::database::mysql::archive::Archive;
use clp_rust_utils::database::mysql::dataset;
use clp_rust_utils::dataset::DatasetId;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::s3::DeleteError;
use clp_rust_utils::types::ArchiveId;
use clp_rust_utils::types::non_empty_string::ExpectedNonEmpty;
use non_empty_string::NonEmptyString;
use serde::Deserialize;
use serde::Serialize;
use utoipa::ToSchema;

use crate::client::Client;
use crate::error::ClientError;

/// Request body for the dataset creation endpoint.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetCreation {
    /// The dataset's name.
    pub name: String,

    /// The dataset's retention period in minutes. Must be positive. Omit or set to `null` to keep
    /// the dataset's archives forever.
    #[schema(minimum = 1)]
    pub retention_period_minutes: Option<u32>,
}

/// Request body for the archive deletion endpoint, which selects the archives to delete.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum ArchiveDeletion {
    /// The IDs of the archives to delete. Must not be empty.
    Ids(Vec<String>),

    /// The time range that the timestamp ranges of the archives to delete lie within.
    TimeRange {
        /// The time range's lower bound (inclusive), in epoch milliseconds.
        begin_ts: i64,
        /// The time range's upper bound (inclusive), in epoch milliseconds.
        end_ts: i64,
    },
}

/// Response body of the archive deletion endpoint.
#[derive(Debug, Serialize, ToSchema)]
pub struct DeletedArchives {
    /// The IDs of the deleted archives.
    #[schema(value_type = Vec<String>)]
    pub deleted_ids: Vec<ArchiveId>,
    /// The requested IDs that aren't the IDs of any of the dataset's archives. Always empty when
    /// deleting by time range.
    #[schema(value_type = Vec<String>)]
    pub not_found_ids: Vec<ArchiveId>,
}

impl Client {
    /// # Returns
    ///
    /// The names of the datasets that aren't deleted, ordered by name, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::check_clp_s_storage_engine`]'s return values on failure.
    /// * Forwards [`dataset::get_names`]'s return values on failure.
    pub async fn get_dataset_names(&self) -> Result<Vec<DatasetName>, ClientError> {
        self.check_clp_s_storage_engine("dataset listing")?;
        Ok(dataset::get_names(&self.sql_pool, &self.config.database).await?)
    }

    /// Creates a dataset, whose archives will be stored in its directory in the archive storage.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::DatasetAlreadyExists`] if a dataset with the same name exists and isn't
    ///   deleted.
    /// * Forwards [`Self::check_clp_s_storage_engine`]'s return values on failure.
    /// * Forwards [`NewDataset::try_from`]'s return values on failure.
    /// * Forwards [`dataset::get_id_by_name`]'s return values on failure.
    /// * Forwards [`dataset::create`]'s return values on failure.
    pub async fn create_dataset(&self, creation: &DatasetCreation) -> Result<(), ClientError> {
        self.check_clp_s_storage_engine("dataset creation")?;
        let NewDataset {
            name,
            retention_period_minutes,
        } = NewDataset::try_from(creation)?;
        if dataset::get_id_by_name(&self.sql_pool, &self.config.database, &name)
            .await?
            .is_some()
        {
            return Err(ClientError::DatasetAlreadyExists(name));
        }
        dataset::create(
            &self.sql_pool,
            &self.config.database,
            &name,
            &self
                .config
                .archive_output
                .dataset_archive_storage_directory(&name),
            retention_period_minutes,
        )
        .await
        .map_err(|e| match e {
            clp_rust_utils::Error::Sqlx(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                ClientError::DatasetAlreadyExists(name)
            }
            e => e.into(),
        })
    }

    /// Deletes a dataset's archives from the archive storage, and then marks the dataset as
    /// deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::check_clp_s_storage_engine`]'s return values on failure.
    /// * Forwards [`Self::get_dataset_id`]'s return values on failure.
    /// * Forwards [`delete_directory`]'s return values on failure.
    /// * Forwards [`delete_archives_from_s3`]'s return values on failure.
    /// * Forwards [`dataset::mark_deleted`]'s return values on failure.
    pub async fn delete_dataset(&self, name: &DatasetName) -> Result<(), ClientError> {
        self.check_clp_s_storage_engine("dataset deletion")?;
        let dataset_id = self.get_dataset_id(name).await?;
        let archive_storage_directory = self
            .config
            .archive_output
            .dataset_archive_storage_directory(name);
        match &self.config.archive_output.storage {
            ArchiveOutputStorage::Fs { .. } => {
                delete_directory(Path::new(&archive_storage_directory)).await?;
            }
            ArchiveOutputStorage::S3 { s3_config, .. } => {
                delete_archives_from_s3(s3_config, &archive_storage_directory).await?;
            }
        }
        dataset::mark_deleted(&self.sql_pool, &self.config.database, dataset_id).await?;
        Ok(())
    }

    /// # Returns
    ///
    /// The IDs of the dataset's archives whose timestamp ranges begin at or after `begin_ts` and,
    /// if `end_ts` is given, end at or before `end_ts`, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::check_clp_s_storage_engine`]'s return values on failure.
    /// * Forwards [`check_time_range`]'s return values on failure.
    /// * Forwards [`Self::get_dataset_id`]'s return values on failure.
    /// * Forwards [`sqlx::Pool::acquire`]'s return values on failure.
    /// * Forwards [`archive::find_by_time_range`]'s return values on failure.
    pub async fn get_archive_ids(
        &self,
        dataset_name: &DatasetName,
        begin_ts: i64,
        end_ts: Option<i64>,
    ) -> Result<Vec<ArchiveId>, ClientError> {
        self.check_clp_s_storage_engine("archive listing")?;
        check_time_range(begin_ts, end_ts)?;
        let dataset_id = self.get_dataset_id(dataset_name).await?;
        let archives = archive::find_by_time_range(
            &mut *self.sql_pool.acquire().await?,
            &self.config.database,
            dataset_id,
            begin_ts,
            end_ts,
        )
        .await?;
        Ok(archives.into_iter().map(|archive| archive.uuid).collect())
    }

    /// Deletes the dataset's archives that `deletion` selects: deletes their metadata in one
    /// transaction, and then deletes them from the archive storage.
    ///
    /// # Returns
    ///
    /// The IDs of the deleted archives and, when deleting by IDs, the requested IDs that aren't the
    /// IDs of any of the dataset's archives, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::check_clp_s_storage_engine`]'s return values on failure.
    /// * Forwards [`ArchiveSelection::try_from`]'s return values on failure.
    /// * Forwards [`Self::get_dataset_id`]'s return values on failure.
    /// * Forwards [`sqlx::Pool::begin`]'s return values on failure.
    /// * Forwards [`archive::find_by_uuids`]'s return values on failure.
    /// * Forwards [`archive::find_by_time_range`]'s return values on failure.
    /// * Forwards [`archive::delete`]'s return values on failure.
    /// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
    /// * Forwards [`delete_directory`]'s return values on failure.
    /// * Forwards [`delete_archive_objects_from_s3`]'s return values on failure.
    pub async fn delete_archives(
        &self,
        dataset_name: &DatasetName,
        deletion: ArchiveDeletion,
    ) -> Result<DeletedArchives, ClientError> {
        self.check_clp_s_storage_engine("archive deletion")?;
        let selection = ArchiveSelection::try_from(deletion)?;
        let dataset_id = self.get_dataset_id(dataset_name).await?;

        let mut tx = self.sql_pool.begin().await?;
        let (archives, not_found_ids) = match selection {
            ArchiveSelection::Ids(ids) => {
                let archives =
                    archive::find_by_uuids(&mut tx, &self.config.database, dataset_id, &ids)
                        .await?;
                let not_found_ids = find_not_found_ids(ids, &archives);
                (archives, not_found_ids)
            }
            ArchiveSelection::TimeRange { begin_ts, end_ts } => {
                let archives = archive::find_by_time_range(
                    &mut tx,
                    &self.config.database,
                    dataset_id,
                    begin_ts,
                    Some(end_ts),
                )
                .await?;
                (archives, Vec::new())
            }
        };
        let ids: Vec<u64> = archives.iter().map(|archive| archive.id).collect();
        archive::delete(&mut tx, &self.config.database, dataset_id, &ids).await?;
        tx.commit().await?;

        let deleted_ids: Vec<ArchiveId> =
            archives.into_iter().map(|archive| archive.uuid).collect();
        match &self.config.archive_output.storage {
            ArchiveOutputStorage::Fs { .. } => {
                let archive_storage_directory = self
                    .config
                    .archive_output
                    .dataset_archive_storage_directory(dataset_name);
                for id in &deleted_ids {
                    delete_directory(&Path::new(&archive_storage_directory).join(id.to_string()))
                        .await?;
                }
            }
            ArchiveOutputStorage::S3 { s3_config, .. } => {
                let keys: Vec<NonEmptyString> = deleted_ids
                    .iter()
                    .map(|id| {
                        self.config
                            .archive_output
                            .dataset_archive_object_key(dataset_name, id)
                    })
                    .collect();
                delete_archive_objects_from_s3(s3_config, &keys).await?;
            }
        }
        Ok(DeletedArchives {
            deleted_ids,
            not_found_ids,
        })
    }

    /// Checks that the package's storage engine is clp-s.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::InvalidInput`] if the package's storage engine isn't clp-s.
    fn check_clp_s_storage_engine(&self, operation: &str) -> Result<(), ClientError> {
        if self.config.package.storage_engine != StorageEngine::ClpS {
            return Err(ClientError::InvalidInput(format!(
                "{operation} requires the clp-s storage engine"
            )));
        }
        Ok(())
    }

    /// Looks up a dataset's ID by its name.
    ///
    /// # Returns
    ///
    /// The dataset's ID on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::DatasetNotFound`] if the dataset doesn't exist or is deleted.
    /// * Forwards [`dataset::get_id_by_name`]'s return values on failure.
    async fn get_dataset_id(&self, name: &DatasetName) -> Result<DatasetId, ClientError> {
        dataset::get_id_by_name(&self.sql_pool, &self.config.database, name)
            .await?
            .ok_or_else(|| ClientError::DatasetNotFound(name.clone()))
    }
}

/// The dataset that a [`DatasetCreation`] describes, validated.
#[derive(Debug, Eq, PartialEq)]
struct NewDataset {
    name: DatasetName,
    retention_period_minutes: Option<NonZeroU32>,
}

impl TryFrom<&DatasetCreation> for NewDataset {
    type Error = ClientError;

    /// Validates `value` and parses its name.
    ///
    /// # Returns
    ///
    /// The validated dataset on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::InvalidInput`] if:
    ///   * `value`'s name isn't a valid dataset name.
    ///   * `value`'s retention period is 0.
    fn try_from(value: &DatasetCreation) -> Result<Self, Self::Error> {
        let name = value
            .name
            .parse::<DatasetName>()
            .map_err(|e| ClientError::InvalidInput(e.to_string()))?;
        let retention_period_minutes = value
            .retention_period_minutes
            .map(|minutes| {
                NonZeroU32::new(minutes).ok_or_else(|| {
                    ClientError::InvalidInput("retention_period_minutes must be > 0".to_owned())
                })
            })
            .transpose()?;
        Ok(Self {
            name,
            retention_period_minutes,
        })
    }
}

/// The archives that an [`ArchiveDeletion`] selects, validated.
#[derive(Debug, Eq, PartialEq)]
enum ArchiveSelection {
    Ids(Vec<ArchiveId>),
    TimeRange { begin_ts: i64, end_ts: i64 },
}

impl TryFrom<ArchiveDeletion> for ArchiveSelection {
    type Error = ClientError;

    /// Validates `value` and parses its IDs.
    ///
    /// # Returns
    ///
    /// The validated selection on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::InvalidInput`] if:
    ///   * `value` selects archives by IDs, and the IDs are empty.
    ///   * `value` selects archives by IDs, and an ID isn't a valid UUID.
    /// * Forwards [`check_time_range`]'s return values on failure.
    fn try_from(value: ArchiveDeletion) -> Result<Self, Self::Error> {
        match value {
            ArchiveDeletion::Ids(ids) => {
                if ids.is_empty() {
                    return Err(ClientError::InvalidInput(
                        "ids must not be empty".to_owned(),
                    ));
                }
                let ids = ids
                    .iter()
                    .map(|id| id.parse::<ArchiveId>())
                    .collect::<Result<_, _>>()
                    .map_err(|e| ClientError::InvalidInput(e.to_string()))?;
                Ok(Self::Ids(ids))
            }
            ArchiveDeletion::TimeRange { begin_ts, end_ts } => {
                check_time_range(begin_ts, Some(end_ts))?;
                Ok(Self::TimeRange { begin_ts, end_ts })
            }
        }
    }
}

/// Checks that the time range from `begin_ts` to `end_ts` (both inclusive) isn't empty.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`ClientError::InvalidInput`] if `end_ts` is less than `begin_ts`.
fn check_time_range(begin_ts: i64, end_ts: Option<i64>) -> Result<(), ClientError> {
    if end_ts.is_some_and(|end_ts| end_ts < begin_ts) {
        return Err(ClientError::InvalidInput(
            "begin_ts must be <= end_ts".to_owned(),
        ));
    }
    Ok(())
}

/// # Returns
///
/// The IDs in `ids` that aren't the UUID of any archive in `archives`, without duplicates, in the
/// order that they first appear in `ids`.
fn find_not_found_ids(ids: Vec<ArchiveId>, archives: &[Archive]) -> Vec<ArchiveId> {
    let mut seen_ids: HashSet<ArchiveId> = archives.iter().map(|archive| archive.uuid).collect();
    ids.into_iter().filter(|id| seen_ids.insert(*id)).collect()
}

/// Deletes the directory that `path` resolves to, if it's a directory.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`tokio::fs::metadata`]'s return values on failure, unless nothing exists at `path`.
/// * Forwards [`tokio::fs::canonicalize`]'s return values on failure.
/// * Forwards [`tokio::fs::remove_dir_all`]'s return values on failure.
async fn delete_directory(path: &Path) -> std::io::Result<()> {
    let metadata = match tokio::fs::metadata(path).await {
        Ok(metadata) => metadata,
        Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    if !metadata.is_dir() {
        return Ok(());
    }
    tokio::fs::remove_dir_all(tokio::fs::canonicalize(path).await?).await
}

/// Deletes every object under `archive_storage_directory` in `s3_config`'s bucket.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`clp_rust_utils::s3::delete_by_key_prefix`]'s return values on failure.
async fn delete_archives_from_s3(
    s3_config: &S3Config,
    archive_storage_directory: &str,
) -> Result<(), DeleteError> {
    let client = clp_rust_utils::s3::create_new_client(
        s3_config
            .region_code
            .as_ref()
            .map_or(AWS_DEFAULT_REGION, NonEmptyString::as_str),
        s3_config.endpoint_url.as_ref(),
        &s3_config.aws_authentication,
    )
    .await;
    clp_rust_utils::s3::delete_by_key_prefix(
        &client,
        s3_config.bucket.as_str(),
        &NonEmptyString::from_string(format!("{archive_storage_directory}/")),
    )
    .await
}

/// Deletes every object in `s3_config`'s bucket whose key is in `keys`.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`clp_rust_utils::s3::delete_by_keys`]'s return values on failure.
async fn delete_archive_objects_from_s3(
    s3_config: &S3Config,
    keys: &[NonEmptyString],
) -> Result<(), DeleteError> {
    let client = clp_rust_utils::s3::create_new_client(
        s3_config
            .region_code
            .as_ref()
            .map_or(AWS_DEFAULT_REGION, NonEmptyString::as_str),
        s3_config.endpoint_url.as_ref(),
        &s3_config.aws_authentication,
    )
    .await;
    clp_rust_utils::s3::delete_by_keys(&client, s3_config.bucket.as_str(), keys).await
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use clp_rust_utils::clp_config::package::config::Config;
    use clp_rust_utils::clp_config::package::config::Package;
    use clp_rust_utils::clp_config::package::config::StorageEngine;
    use clp_rust_utils::database::mysql::archive::Archive;
    use clp_rust_utils::dataset::DatasetName;
    use clp_rust_utils::types::ArchiveId;

    use super::ArchiveDeletion;
    use super::ArchiveSelection;
    use super::DatasetCreation;
    use super::NewDataset;
    use super::check_time_range;
    use super::delete_directory;
    use super::find_not_found_ids;
    use crate::client::Client;

    const ARCHIVE_ID: &str = "018e90e5-8b2a-4a61-a2fc-cac799936caf";

    #[tokio::test]
    async fn directory_deletion_only_deletes_a_directory_or_a_symlinked_one() -> anyhow::Result<()>
    {
        let root = tempfile::tempdir()?;
        let directory = root.path().join("directory");
        std::fs::create_dir_all(directory.join("archive"))?;
        std::fs::write(directory.join("archive").join("segment"), b"data")?;
        let symlinked_directory = root.path().join("symlinked_directory");
        std::fs::create_dir(&symlinked_directory)?;
        let symlink = root.path().join("symlink");
        std::os::unix::fs::symlink(&symlinked_directory, &symlink)?;
        let file = root.path().join("file");
        std::fs::write(&file, b"data")?;

        for path in [&directory, &symlink] {
            delete_directory(path).await?;
        }
        assert!(!directory.exists());
        assert!(!symlinked_directory.exists());

        for path in [&file, &file.join("child"), &root.path().join("missing")] {
            delete_directory(path).await?;
        }
        assert!(file.is_file());
        Ok(())
    }

    #[test]
    fn time_range_check_only_rejects_an_end_before_the_begin() {
        for (begin_ts, end_ts) in [(1, None), (1, Some(1))] {
            check_time_range(begin_ts, end_ts).expect("the time range should be accepted");
        }
        assert_eq!(
            check_time_range(2, Some(1))
                .expect_err("the time range should be rejected")
                .to_string(),
            "Invalid input: begin_ts must be <= end_ts"
        );
    }

    #[test]
    fn archive_selection_parses_ids_and_keeps_the_time_range() -> anyhow::Result<()> {
        const OTHER_ARCHIVE_ID: &str = "018e90e5-8b2a-4a61-a2fc-cac799936ca0";

        assert_eq!(
            ArchiveSelection::try_from(ArchiveDeletion::Ids(vec![
                ARCHIVE_ID.to_owned(),
                OTHER_ARCHIVE_ID.to_owned(),
            ]))?,
            ArchiveSelection::Ids(vec![ARCHIVE_ID.parse()?, OTHER_ARCHIVE_ID.parse()?])
        );
        assert_eq!(
            ArchiveSelection::try_from(ArchiveDeletion::TimeRange {
                begin_ts: 1,
                end_ts: 2,
            })?,
            ArchiveSelection::TimeRange {
                begin_ts: 1,
                end_ts: 2,
            }
        );
        Ok(())
    }

    #[test]
    fn archive_selection_rejects_empty_or_invalid_ids_and_a_reversed_time_range() {
        const INVALID_ARCHIVE_ID: &str = "not-a-uuid";

        let parse_error = INVALID_ARCHIVE_ID
            .parse::<ArchiveId>()
            .expect_err("the archive ID should be invalid");
        for (deletion, expected_error) in [
            (
                ArchiveDeletion::Ids(Vec::new()),
                "Invalid input: ids must not be empty".to_owned(),
            ),
            (
                ArchiveDeletion::Ids(vec![ARCHIVE_ID.to_owned(), INVALID_ARCHIVE_ID.to_owned()]),
                format!("Invalid input: {parse_error}"),
            ),
            (
                ArchiveDeletion::TimeRange {
                    begin_ts: 2,
                    end_ts: 1,
                },
                "Invalid input: begin_ts must be <= end_ts".to_owned(),
            ),
        ] {
            assert_eq!(
                ArchiveSelection::try_from(deletion)
                    .expect_err("the deletion should be rejected")
                    .to_string(),
                expected_error
            );
        }
    }

    #[test]
    fn new_dataset_parses_the_name_and_keeps_the_retention_period() -> anyhow::Result<()> {
        for retention_period_minutes in [None, Some(NonZeroU32::MIN)] {
            assert_eq!(
                NewDataset::try_from(&DatasetCreation {
                    name: "logs".to_owned(),
                    retention_period_minutes: retention_period_minutes.map(NonZeroU32::get),
                })?,
                NewDataset {
                    name: "logs".parse()?,
                    retention_period_minutes,
                }
            );
        }
        Ok(())
    }

    #[test]
    fn new_dataset_rejects_an_invalid_name_and_a_zero_retention_period() {
        const INVALID_DATASET_NAME: &str = "bad-name";

        let parse_error = INVALID_DATASET_NAME
            .parse::<DatasetName>()
            .expect_err("the dataset name should be invalid");
        for (creation, expected_error) in [
            (
                DatasetCreation {
                    name: INVALID_DATASET_NAME.to_owned(),
                    retention_period_minutes: Some(1),
                },
                format!("Invalid input: {parse_error}"),
            ),
            (
                DatasetCreation {
                    name: "logs".to_owned(),
                    retention_period_minutes: Some(0),
                },
                "Invalid input: retention_period_minutes must be > 0".to_owned(),
            ),
        ] {
            assert_eq!(
                NewDataset::try_from(&creation)
                    .expect_err("the creation should be rejected")
                    .to_string(),
                expected_error
            );
        }
    }

    #[test]
    fn not_found_ids_exclude_found_ids_and_duplicates_in_request_order() -> anyhow::Result<()> {
        let ids: Vec<ArchiveId> = [
            "018e90e5-8b2a-4a61-a2fc-cac799936ca0",
            "018e90e5-8b2a-4a61-a2fc-cac799936ca1",
            "018e90e5-8b2a-4a61-a2fc-cac799936ca2",
            "018e90e5-8b2a-4a61-a2fc-cac799936ca3",
        ]
        .iter()
        .map(|id| id.parse())
        .collect::<Result<_, _>>()?;
        let archives = [
            Archive {
                id: 1,
                uuid: ids[1],
            },
            Archive {
                id: 2,
                uuid: ids[3],
            },
        ];

        assert_eq!(
            find_not_found_ids(vec![ids[2], ids[1], ids[0], ids[2], ids[3]], &archives),
            vec![ids[2], ids[0]]
        );
        Ok(())
    }

    #[tokio::test]
    async fn dataset_management_is_rejected_under_the_clp_storage_engine() -> anyhow::Result<()> {
        let config = Config {
            package: Package {
                storage_engine: StorageEngine::Clp,
            },
            ..Config::default()
        };
        let sql_pool =
            sqlx::mysql::MySqlPoolOptions::new().connect_lazy("mysql://127.0.0.1:1/clp-db")?;
        let mongodb_client =
            mongodb::Client::with_uri_str("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=100")
                .await?;
        let client = Client::new(config, mongodb_client, sql_pool);
        let dataset_name: DatasetName = "logs".parse()?;

        assert_eq!(
            client
                .get_dataset_names()
                .await
                .expect_err("the listing should be rejected")
                .to_string(),
            "Invalid input: dataset listing requires the clp-s storage engine"
        );
        assert_eq!(
            client
                .create_dataset(&DatasetCreation {
                    name: dataset_name.to_string(),
                    retention_period_minutes: None,
                })
                .await
                .expect_err("the creation should be rejected")
                .to_string(),
            "Invalid input: dataset creation requires the clp-s storage engine"
        );
        assert_eq!(
            client
                .delete_dataset(&dataset_name)
                .await
                .expect_err("the deletion should be rejected")
                .to_string(),
            "Invalid input: dataset deletion requires the clp-s storage engine"
        );
        assert_eq!(
            client
                .get_archive_ids(&dataset_name, 0, None)
                .await
                .expect_err("the archive listing should be rejected")
                .to_string(),
            "Invalid input: archive listing requires the clp-s storage engine"
        );
        assert_eq!(
            client
                .delete_archives(
                    &dataset_name,
                    ArchiveDeletion::Ids(vec![ARCHIVE_ID.to_owned()])
                )
                .await
                .expect_err("the archive deletion should be rejected")
                .to_string(),
            "Invalid input: archive deletion requires the clp-s storage engine"
        );
        Ok(())
    }
}
