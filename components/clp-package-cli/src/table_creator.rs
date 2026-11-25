//! The table creator, which sets up the shared clp-s metadata tables at package start.

use anyhow::Context;
use clp_metadata_db::ClpDbClient;
use clp_metadata_db::DatasetConfig;
use clp_metadata_db::DatasetDescriptor;
use clp_metadata_db::DatasetManagement;
use clp_metadata_db::Error;
use clp_rust_utils::clp_config::package::config::ArchiveOutput;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::types::DatasetId;

/// The state of the `default` dataset after the table creator ran.
#[derive(Debug, Eq, PartialEq)]
pub enum DefaultDataset {
    /// The `default` dataset is live, with the configured settings.
    Ready { id: DatasetId },

    /// The `default` dataset is marked as deleted, so compression into it fails.
    Deleted,

    /// The `default` dataset exists with other settings than the configured ones, so compression
    /// into it fails.
    ConfigMismatch { existing: DatasetConfig },
}

/// Creates the shared metadata tables and the `default` dataset, if they don't exist yet.
///
/// The `default` dataset gets the package-wide defaults, and, on a fresh database, the first
/// dataset ID. Rerunning it changes nothing.
///
/// The package can't start until this succeeds, so a `default` dataset that is marked as deleted,
/// or whose settings differ from the configured ones, doesn't fail it: it only fails compression
/// into `default`, and is logged as a warning.
///
/// # Returns
///
/// The state of the `default` dataset on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`ClpDbClient::create_tables`]'s return values on failure.
/// * Forwards [`ClpDbClient::begin`]'s return values on failure.
/// * Forwards [`DatasetManagement::create_or_get_dataset`]'s return values on failure, except for
///   [`Error::DatasetDeleted`] and [`Error::DatasetConfigMismatch`].
/// * Forwards [`clp_metadata_db::ClpDbTransaction::commit`]'s return values on failure.
/// * Forwards [`clp_metadata_db::ClpDbTransaction::rollback`]'s return values on failure.
pub async fn create_tables_and_default_dataset(
    client: &ClpDbClient,
    archive_output: &ArchiveOutput,
) -> anyhow::Result<DefaultDataset> {
    client
        .create_tables()
        .await
        .context("failed to create the metadata tables")?;

    let name = DatasetName::default();
    // The compression coordinator must fill the descriptors of the datasets it creates the same
    // way: if the settings differ, every compression job into `default` fails with a settings
    // mismatch.
    let descriptor = DatasetDescriptor {
        config: DatasetConfig {
            archive_storage_path: archive_output.dataset_archive_storage_directory(&name),
            retention_period_minutes: None,
        },
        name,
    };
    let mut transaction = client.begin().await?;
    match transaction.create_or_get_dataset(&descriptor).await {
        Ok(id) => {
            transaction.commit().await?;
            Ok(DefaultDataset::Ready { id })
        }
        Err(Error::DatasetDeleted) => {
            transaction.rollback().await?;
            tracing::warn!(
                "The `default` dataset is marked as deleted, so compression into it will fail."
            );
            Ok(DefaultDataset::Deleted)
        }
        Err(Error::DatasetConfigMismatch { existing }) => {
            transaction.rollback().await?;
            tracing::warn!(
                stored_archive_storage_path = % existing.archive_storage_path,
                configured_archive_storage_path = % descriptor.config.archive_storage_path,
                "The `default` dataset's stored settings differ from the configured ones, so \
                 compression into it will fail."
            );
            Ok(DefaultDataset::ConfigMismatch { existing })
        }
        Err(e) => Err(e).context("failed to create the `default` dataset"),
    }
}
