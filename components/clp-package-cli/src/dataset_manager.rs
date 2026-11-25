//! The dataset manager, which lists datasets and marks them as deleted.

use std::collections::HashSet;

use anyhow::Context;
use clp_metadata_db::ClpDbClient;
use clp_metadata_db::Dataset;
use clp_metadata_db::DatasetLookup;
use clp_metadata_db::DatasetManagement;
use clp_metadata_db::Error;
use clp_rust_utils::dataset::DatasetName;

/// The datasets to delete.
#[derive(Debug)]
pub enum DatasetsToDelete {
    /// Every live dataset.
    All,

    /// The named datasets, each of which must be live.
    Named(Vec<DatasetName>),
}

/// # Returns
///
/// The live datasets, ordered by name, on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`ClpDbClient::acquire`]'s return values on failure.
/// * Forwards [`DatasetLookup::get_datasets`]'s return values on failure.
pub async fn list_datasets(client: &ClpDbClient) -> anyhow::Result<Vec<Dataset>> {
    let mut connection = client.acquire().await?;
    connection
        .get_datasets()
        .await
        .context("failed to read the datasets")
}

/// Marks datasets as deleted, each in its own transaction, which commits right away since archive
/// commits into the dataset wait on it. The datasets' archives, storage and rows stay.
///
/// Checks that every named dataset is live before marking any, so an unknown name deletes nothing.
/// A dataset that is already marked by the time its turn comes, by another caller or because it's
/// named twice, is skipped.
///
/// # Returns
///
/// The names of the datasets this call marked, in order, on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if a named dataset isn't live.
/// * Forwards [`list_datasets`]'s return values on failure.
/// * Forwards [`ClpDbClient::begin`]'s return values on failure.
/// * Forwards [`DatasetManagement::mark_dataset_deleted`]'s return values on failure, except for
///   [`Error::DatasetDeleted`].
/// * Forwards [`clp_metadata_db::ClpDbTransaction::commit`]'s return values on failure.
pub async fn delete_datasets(
    client: &ClpDbClient,
    datasets: DatasetsToDelete,
) -> anyhow::Result<Vec<DatasetName>> {
    let live_datasets = list_datasets(client).await?;
    let names = match datasets {
        DatasetsToDelete::All => {
            if live_datasets.is_empty() {
                tracing::warn!("No datasets exist.");
            }
            live_datasets
                .into_iter()
                .map(|dataset| dataset.name)
                .collect()
        }
        DatasetsToDelete::Named(names) => {
            let live_names: HashSet<&DatasetName> =
                live_datasets.iter().map(|dataset| &dataset.name).collect();
            if let Some(unknown_name) = names.iter().find(|name| !live_names.contains(name)) {
                anyhow::bail!("dataset `{unknown_name}` doesn't exist; aborting deletion");
            }
            names
        }
    };

    let mut marked_names = Vec::with_capacity(names.len());
    for name in names {
        let mut transaction = client.begin().await?;
        match transaction.mark_dataset_deleted(&name).await {
            Ok(_) => {}
            Err(Error::DatasetDeleted) => {
                tracing::warn!(dataset = % name, "Dataset was already deleted.");
                continue;
            }
            Err(e) => {
                return Err(e).with_context(|| format!("failed to delete dataset `{name}`"));
            }
        }
        transaction.commit().await?;
        tracing::info!(dataset = % name, "Marked dataset as deleted.");
        marked_names.push(name);
    }
    Ok(marked_names)
}
