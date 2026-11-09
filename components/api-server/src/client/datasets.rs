//! Management of the clp-s storage engine's datasets and their archives.

use std::collections::HashSet;
use std::num::NonZeroU32;

use clp_metadata_db::Archive;
use clp_metadata_db::NewDataset;
use clp_metadata_db::TimestampRange;
use clp_rust_utils::clp_config::package::config::StorageEngine;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::types::ArchiveId;
use serde::Deserialize;
use serde::Serialize;
use utoipa::IntoParams;
use utoipa::ToSchema;

use crate::client::Client;
use crate::error::ClientError;

/// Request body for the dataset creation endpoint.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetCreation {
    /// The dataset's name.
    #[schema(value_type = String)]
    pub name: DatasetName,

    /// How long the dataset's archives are kept after their creation, in minutes. Must be
    /// positive. Omit or set to `null` to keep them forever.
    #[serde(default)]
    #[schema(value_type = Option<u32>, minimum = 1)]
    pub retention_period_minutes: Option<NonZeroU32>,
}

/// Query parameters for the archive listing endpoint.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
pub struct ArchiveTimeRange {
    /// The time range's lower bound (inclusive), in epoch milliseconds. Omit for no lower bound.
    pub begin_timestamp_millisecs: Option<i64>,

    /// The time range's upper bound (inclusive), in epoch milliseconds. Omit for no upper bound.
    pub end_timestamp_millisecs: Option<i64>,
}

/// An archive of a dataset.
#[derive(Debug, Serialize, ToSchema)]
pub struct ArchiveMetadata {
    /// The archive's ID.
    #[schema(value_type = String)]
    pub archive_id: ArchiveId,

    /// The inclusive lower bound of the archive's timestamps, in epoch milliseconds, or `0` if it
    /// has no timestamps.
    pub begin_timestamp_millisecs: i64,

    /// The inclusive upper bound of the archive's timestamps, in epoch milliseconds, or `0` if it
    /// has no timestamps.
    pub end_timestamp_millisecs: i64,
}

impl From<Archive> for ArchiveMetadata {
    fn from(archive: Archive) -> Self {
        Self {
            archive_id: archive.uuid,
            begin_timestamp_millisecs: archive.begin_timestamp_millisecs,
            end_timestamp_millisecs: archive.end_timestamp_millisecs,
        }
    }
}

/// A set of archive IDs.
#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ArchiveIds {
    /// The archives' IDs.
    #[schema(value_type = Vec<String>)]
    pub archive_ids: Vec<ArchiveId>,
}

/// A time range that archives' timestamp ranges lie within.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ArchiveTimeRangeSelection {
    /// The time range's lower bound (inclusive), in epoch milliseconds.
    pub begin_timestamp_millisecs: i64,

    /// The time range's upper bound (inclusive), in epoch milliseconds.
    pub end_timestamp_millisecs: i64,
}

/// Request body for the archive deletion endpoint, which selects the archives to delete either by
/// their IDs or by a time range.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum ArchiveSelection {
    ByIds(ArchiveIds),
    ByTimeRange(ArchiveTimeRangeSelection),
}

impl Client {
    /// # Returns
    ///
    /// The names of the live datasets, ordered by name, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::check_clp_s_storage_engine`]'s return values on failure.
    /// * Forwards [`sqlx::Pool::acquire`]'s return values on failure.
    /// * Forwards [`clp_metadata_db::MetadataTables::get_dataset_names`]'s return values on
    ///   failure.
    pub async fn get_dataset_names(&self) -> Result<Vec<DatasetName>, ClientError> {
        self.check_clp_s_storage_engine("dataset listing")?;
        let mut conn = self.sql_pool.acquire().await?;
        Ok(self.metadata_tables.get_dataset_names(&mut conn).await?)
    }

    /// Creates a dataset whose archives are stored at its location in the package's archive
    /// output.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::check_clp_s_storage_engine`]'s return values on failure.
    /// * Forwards [`sqlx::Pool::acquire`]'s return values on failure.
    /// * Forwards [`clp_metadata_db::MetadataTables::create_dataset`]'s return values on failure.
    pub async fn create_dataset(&self, creation: DatasetCreation) -> Result<(), ClientError> {
        self.check_clp_s_storage_engine("dataset creation")?;
        let DatasetCreation {
            name,
            retention_period_minutes,
        } = creation;
        let dataset = NewDataset {
            archive_storage_path: self
                .config
                .archive_output
                .dataset_archive_storage_directory(&name),
            name,
            retention_period_minutes,
        };
        let mut conn = self.sql_pool.acquire().await?;
        self.metadata_tables
            .create_dataset(&mut conn, &dataset)
            .await?;
        Ok(())
    }

    /// Marks a live dataset as deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::DatasetNotFound`] if there's no live dataset named `name`.
    /// * Forwards [`Self::check_clp_s_storage_engine`]'s return values on failure.
    /// * Forwards [`sqlx::Pool::begin`]'s return values on failure.
    /// * Forwards [`Self::get_live_dataset_id`]'s return values on failure.
    /// * Forwards [`clp_metadata_db::MetadataTables::mark_dataset_deleted`]'s return values on
    ///   failure.
    /// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
    pub async fn mark_dataset_deleted(&self, name: &DatasetName) -> Result<(), ClientError> {
        self.check_clp_s_storage_engine("dataset deletion")?;
        let mut tx = self.sql_pool.begin().await?;
        let dataset_id = self.get_live_dataset_id(&mut tx, name).await?;
        if !self
            .metadata_tables
            .mark_dataset_deleted(&mut tx, dataset_id)
            .await?
        {
            return Err(ClientError::DatasetNotFound(name.clone()));
        }
        tx.commit().await?;
        Ok(())
    }

    /// Gets a live dataset's live archives whose timestamp ranges lie within `time_range`.
    ///
    /// # Returns
    ///
    /// The matching archives, in no particular order, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::check_clp_s_storage_engine`]'s return values on failure.
    /// * Forwards [`validate_timestamp_range`]'s return values on failure.
    /// * Forwards [`sqlx::Pool::acquire`]'s return values on failure.
    /// * Forwards [`Self::get_live_dataset_id`]'s return values on failure.
    /// * Forwards [`clp_metadata_db::MetadataTables::find_archives_by_time_range`]'s return values
    ///   on failure.
    pub async fn get_dataset_archives(
        &self,
        name: &DatasetName,
        time_range: ArchiveTimeRange,
    ) -> Result<Vec<ArchiveMetadata>, ClientError> {
        self.check_clp_s_storage_engine("archive listing")?;
        let range = validate_timestamp_range(
            time_range.begin_timestamp_millisecs.unwrap_or(i64::MIN),
            time_range.end_timestamp_millisecs.unwrap_or(i64::MAX),
        )?;
        let mut conn = self.sql_pool.acquire().await?;
        let dataset_id = self.get_live_dataset_id(&mut conn, name).await?;
        Ok(self
            .metadata_tables
            .find_archives_by_time_range(&mut conn, dataset_id, &range)
            .await?
            .into_iter()
            .map(ArchiveMetadata::from)
            .collect())
    }

    /// Marks a live dataset's selected live archives as deleted.
    ///
    /// When selecting by IDs, either every requested archive is marked, or none is.
    ///
    /// # Returns
    ///
    /// The IDs of the marked archives on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::ArchivesNotFound`] if any of the requested IDs isn't the ID of a live
    ///   archive of the dataset.
    /// * Forwards [`Self::check_clp_s_storage_engine`]'s return values on failure.
    /// * Forwards [`ValidatedArchiveSelection::try_from`]'s return values on failure.
    /// * Forwards [`sqlx::Pool::begin`]'s return values on failure.
    /// * Forwards [`Self::get_live_dataset_id`]'s return values on failure.
    /// * Forwards [`clp_metadata_db::MetadataTables::find_archives_by_uuids`]'s return values on
    ///   failure.
    /// * Forwards [`clp_metadata_db::MetadataTables::find_archives_by_time_range`]'s return values
    ///   on failure.
    /// * Forwards [`clp_metadata_db::MetadataTables::mark_archives_deleted`]'s return values on
    ///   failure.
    /// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
    pub async fn mark_dataset_archives_deleted(
        &self,
        name: &DatasetName,
        selection: ArchiveSelection,
    ) -> Result<ArchiveIds, ClientError> {
        self.check_clp_s_storage_engine("archive deletion")?;
        let selection = ValidatedArchiveSelection::try_from(selection)?;
        let mut tx = self.sql_pool.begin().await?;
        let dataset_id = self.get_live_dataset_id(&mut tx, name).await?;
        let archives = match selection {
            ValidatedArchiveSelection::ByIds(archive_ids) => {
                let archives = self
                    .metadata_tables
                    .find_archives_by_uuids(&mut tx, dataset_id, &archive_ids)
                    .await?;
                let found_ids: HashSet<ArchiveId> =
                    archives.iter().map(|archive| archive.uuid).collect();
                let missing_ids: Vec<ArchiveId> = archive_ids
                    .into_iter()
                    .filter(|id| !found_ids.contains(id))
                    .collect();
                if !missing_ids.is_empty() {
                    return Err(ClientError::ArchivesNotFound(missing_ids));
                }
                archives
            }
            ValidatedArchiveSelection::ByTimeRange(range) => {
                self.metadata_tables
                    .find_archives_by_time_range(&mut tx, dataset_id, &range)
                    .await?
            }
        };
        let row_ids: Vec<_> = archives.iter().map(|archive| archive.id).collect();
        self.metadata_tables
            .mark_archives_deleted(&mut tx, dataset_id, &row_ids)
            .await?;
        tx.commit().await?;
        Ok(ArchiveIds {
            archive_ids: archives.into_iter().map(|archive| archive.uuid).collect(),
        })
    }

    /// Checks that the package uses the clp-s storage engine, whose datasets this client manages.
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

    /// # Returns
    ///
    /// The ID of the live dataset named `name` on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::DatasetNotFound`] if there's no live dataset named `name`.
    /// * Forwards [`clp_metadata_db::MetadataTables::get_dataset_id_by_name`]'s return values on
    ///   failure.
    async fn get_live_dataset_id(
        &self,
        conn: &mut sqlx::MySqlConnection,
        name: &DatasetName,
    ) -> Result<clp_metadata_db::DatasetId, ClientError> {
        self.metadata_tables
            .get_dataset_id_by_name(conn, name)
            .await?
            .ok_or_else(|| ClientError::DatasetNotFound(name.clone()))
    }
}

/// An [`ArchiveSelection`] whose time range, if any, is valid.
enum ValidatedArchiveSelection {
    ByIds(Vec<ArchiveId>),
    ByTimeRange(TimestampRange),
}

impl TryFrom<ArchiveSelection> for ValidatedArchiveSelection {
    type Error = ClientError;

    /// Validates the selection's time range, if any.
    ///
    /// # Returns
    ///
    /// The validated selection on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`validate_timestamp_range`]'s return values on failure.
    fn try_from(selection: ArchiveSelection) -> Result<Self, Self::Error> {
        Ok(match selection {
            ArchiveSelection::ByIds(ArchiveIds { archive_ids }) => Self::ByIds(archive_ids),
            ArchiveSelection::ByTimeRange(ArchiveTimeRangeSelection {
                begin_timestamp_millisecs,
                end_timestamp_millisecs,
            }) => Self::ByTimeRange(validate_timestamp_range(
                begin_timestamp_millisecs,
                end_timestamp_millisecs,
            )?),
        })
    }
}

/// # Returns
///
/// The inclusive time range between the given bounds on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`ClientError::InvalidInput`] if `begin_timestamp_millisecs` is greater than
///   `end_timestamp_millisecs`.
fn validate_timestamp_range(
    begin_timestamp_millisecs: i64,
    end_timestamp_millisecs: i64,
) -> Result<TimestampRange, ClientError> {
    if begin_timestamp_millisecs > end_timestamp_millisecs {
        return Err(ClientError::InvalidInput(
            "begin_timestamp_millisecs must be <= end_timestamp_millisecs".to_owned(),
        ));
    }
    Ok(TimestampRange {
        begin_timestamp_millisecs,
        end_timestamp_millisecs,
    })
}
