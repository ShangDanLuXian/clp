//! The dataset operations: [`DatasetLookup`] and [`DatasetManagement`].

use std::collections::HashMap;
use std::num::NonZeroU32;

use async_trait::async_trait;
use clp_rust_utils::clp_config::package::config::MetadataTables;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::types::DatasetId;
use sqlx::MySql;
use sqlx::MySqlConnection;
use sqlx::QueryBuilder;

use super::DatasetLookup;
use super::DatasetManagement;
use super::MAX_BATCH_SIZE;
use crate::client::ClpDbTransaction;
use crate::client::Handle;
use crate::client::HandleParts;
use crate::error::Error;
use crate::stored::StoredString;
use crate::types::Dataset;
use crate::types::DatasetConfig;
use crate::types::DatasetDescriptor;

/// The columns that [`DatasetRow`] projects, in a `SELECT` list.
const DATASET_COLUMNS: &str = "`id`, `name`, `archive_storage_path`, `retention_period_minutes`";

#[async_trait]
impl<HandleType: Handle> DatasetLookup for HandleType {
    async fn get_datasets(&mut self) -> Result<Vec<Dataset>, Error> {
        let HandleParts { tables, connection } = self.parts();
        let query = format!(
            "SELECT {DATASET_COLUMNS} FROM `{}` WHERE `is_deleted` = FALSE ORDER BY `name`",
            tables.datasets()
        );
        sqlx::query_as::<_, DatasetRow>(&query)
            .fetch_all(connection)
            .await?
            .into_iter()
            .map(DatasetRow::into_dataset)
            .collect()
    }

    async fn get_dataset_by_name(&mut self, name: &DatasetName) -> Result<Option<Dataset>, Error> {
        let HandleParts { tables, connection } = self.parts();
        let query = format!(
            "SELECT {DATASET_COLUMNS} FROM `{}` WHERE `name` = ? AND `is_deleted` = FALSE",
            tables.datasets()
        );
        sqlx::query_as::<_, DatasetRow>(&query)
            .bind(name)
            .fetch_optional(connection)
            .await?
            .map(DatasetRow::into_dataset)
            .transpose()
    }

    async fn get_dataset_ids_by_names(
        &mut self,
        names: &[DatasetName],
    ) -> Result<HashMap<DatasetName, DatasetId>, Error> {
        let HandleParts { tables, connection } = self.parts();
        let mut ids = HashMap::new();
        for batch in names.chunks(MAX_BATCH_SIZE) {
            let mut query_builder = QueryBuilder::<MySql>::new(format!(
                "SELECT `id`, `name` FROM `{}` WHERE `is_deleted` = FALSE AND `name` IN (",
                tables.datasets()
            ));
            let mut separated_names = query_builder.separated(", ");
            for name in batch {
                separated_names.push_bind(name);
            }
            query_builder.push(")");
            let rows = query_builder
                .build_query_as::<(DatasetId, DatasetName)>()
                .fetch_all(&mut *connection)
                .await?;
            ids.extend(rows.into_iter().map(|(id, name)| (name, id)));
        }
        Ok(ids)
    }
}

#[async_trait]
impl DatasetManagement for ClpDbTransaction<'_> {
    async fn create_or_get_dataset(
        &mut self,
        descriptor: &DatasetDescriptor,
    ) -> Result<DatasetId, Error> {
        let HandleParts { tables, connection } = self.parts();
        let mut creation = DatasetCreation { tables, connection };

        // Only a dataset that already exists is read with a lock: a locking read of a missing row
        // takes a gap lock, and two creators that both hold one deadlock on their inserts. The
        // consistent read takes no lock.
        if !creation.name_exists(&descriptor.name).await? {
            let query = format!(
                "INSERT INTO `{}` (`name`, `archive_storage_path`, `retention_period_minutes`) \
                 VALUES (?, ?, ?)",
                creation.tables.datasets()
            );
            let insert_result = sqlx::query(&query)
                .bind(&descriptor.name)
                .bind(&descriptor.config.archive_storage_path)
                .bind(
                    descriptor
                        .config
                        .retention_period_minutes
                        .map(NonZeroU32::get),
                )
                .execute(&mut *creation.connection)
                .await;
            match insert_result {
                Ok(result) => {
                    let id = u16::try_from(result.last_insert_id())
                        .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
                    return Ok(DatasetId::from(id));
                }
                // A concurrent creator committed the dataset first.
                Err(sqlx::Error::Database(error)) if error.is_unique_violation() => {}
                Err(error) => return Err(error.into()),
            }
        }

        // Rows are never removed in this release, so a dataset that exists, or that a concurrent
        // creator inserted, is still there.
        creation
            .read_for_share(&descriptor.name)
            .await?
            .ok_or(Error::DatasetNotFound)?
            .into_matching_dataset_id(descriptor)
    }

    async fn mark_dataset_deleted(&mut self, name: &DatasetName) -> Result<DatasetId, Error> {
        let HandleParts { tables, connection } = self.parts();

        // The exclusive lock waits for the transactions that hold a shared lock on the row
        // (`insert_archives`, `create_or_get_dataset`), and makes theirs wait for this one, so an
        // archive insert and the mark never interleave.
        let select_query = format!(
            "SELECT `id`, `is_deleted` FROM `{}` WHERE `name` = ? FOR UPDATE",
            tables.datasets()
        );
        let Some((id, is_deleted)) = sqlx::query_as::<_, (DatasetId, bool)>(&select_query)
            .bind(name)
            .fetch_optional(&mut *connection)
            .await?
        else {
            return Err(Error::DatasetNotFound);
        };
        if is_deleted {
            return Err(Error::DatasetDeleted);
        }

        let update_query = format!(
            "UPDATE `{}` SET `is_deleted` = TRUE WHERE `id` = ?",
            tables.datasets()
        );
        sqlx::query(&update_query)
            .bind(id)
            .execute(connection)
            .await?;
        Ok(id)
    }
}

/// The table names and transaction connection that [`DatasetManagement::create_or_get_dataset`]
/// runs its statements on.
///
/// # Type Parameters
///
/// * `'handle` - The lifetime of the borrowed transaction.
struct DatasetCreation<'handle> {
    tables: &'handle MetadataTables,
    connection: &'handle mut MySqlConnection,
}

impl DatasetCreation<'_> {
    /// # Returns
    ///
    /// Whether the transaction's snapshot has a dataset named `name`, deleted or not, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryScalar::fetch_one`]'s return values on failure.
    async fn name_exists(&mut self, name: &DatasetName) -> Result<bool, Error> {
        let query = format!(
            "SELECT EXISTS (SELECT 1 FROM `{}` WHERE `name` = ?)",
            self.tables.datasets()
        );
        Ok(sqlx::query_scalar::<_, bool>(&query)
            .bind(name)
            .fetch_one(&mut *self.connection)
            .await?)
    }

    /// Reads the latest committed row of the dataset named `name`, deleted or not, and takes a
    /// shared lock on it until the transaction ends.
    ///
    /// # Returns
    ///
    /// The dataset's row, or `None` if no dataset is named `name`, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryAs::fetch_optional`]'s return values on failure.
    async fn read_for_share(
        &mut self,
        name: &DatasetName,
    ) -> Result<Option<LockedDatasetRow>, Error> {
        // A locking read, unlike a consistent read, sees rows committed after the transaction's
        // snapshot, such as a concurrent creator's. The shared lock keeps `mark_dataset_deleted`
        // waiting until the caller's transaction ends.
        let query = format!(
            "SELECT {DATASET_COLUMNS}, `is_deleted` FROM `{}` WHERE `name` = ? LOCK IN SHARE MODE",
            self.tables.datasets()
        );
        Ok(sqlx::query_as::<_, LockedDatasetRow>(&query)
            .bind(name)
            .fetch_optional(&mut *self.connection)
            .await?)
    }
}

/// The columns of a `clp_datasets` row that describe a dataset.
#[derive(sqlx::FromRow)]
struct DatasetRow {
    id: DatasetId,
    name: DatasetName,
    #[sqlx(try_from = "Vec<u8>")]
    archive_storage_path: StoredString,
    retention_period_minutes: Option<u32>,
}

impl DatasetRow {
    /// Validates the row's settings.
    ///
    /// # Returns
    ///
    /// The dataset that the row describes on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::InvalidStoredRetentionPeriod`] if the stored retention period is zero.
    fn into_dataset(self) -> Result<Dataset, Error> {
        let retention_period_minutes = self
            .retention_period_minutes
            .map(|minutes| {
                NonZeroU32::new(minutes).ok_or(Error::InvalidStoredRetentionPeriod {
                    dataset_id: self.id,
                })
            })
            .transpose()?;
        Ok(Dataset {
            id: self.id,
            name: self.name,
            config: DatasetConfig {
                archive_storage_path: self.archive_storage_path.value,
                retention_period_minutes,
            },
        })
    }
}

/// A `clp_datasets` row read with a lock, including datasets marked as deleted.
#[derive(sqlx::FromRow)]
struct LockedDatasetRow {
    #[sqlx(flatten)]
    dataset: DatasetRow,
    is_deleted: bool,
}

impl LockedDatasetRow {
    /// # Returns
    ///
    /// The dataset's ID on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::DatasetDeleted`] if the dataset is marked as deleted.
    /// * [`Error::DatasetConfigMismatch`] if the dataset's settings differ from `descriptor`'s.
    /// * Forwards [`DatasetRow::into_dataset`]'s return values on failure.
    fn into_matching_dataset_id(self, descriptor: &DatasetDescriptor) -> Result<DatasetId, Error> {
        if self.is_deleted {
            return Err(Error::DatasetDeleted);
        }
        let existing = self.dataset.into_dataset()?;
        if existing.config != descriptor.config {
            return Err(Error::DatasetConfigMismatch {
                existing: existing.config,
            });
        }
        Ok(existing.id)
    }
}
