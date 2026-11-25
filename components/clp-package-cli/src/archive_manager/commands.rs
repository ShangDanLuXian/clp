//! The archive manager's commands: finding archives, and deleting them or previewing a deletion.

use std::collections::HashMap;
use std::collections::HashSet;

use anyhow::Context;
use clp_metadata_db::ArchiveLookup;
use clp_metadata_db::ArchiveManagement;
use clp_metadata_db::ArchiveMetadata;
use clp_metadata_db::ClpDbClient;
use clp_metadata_db::Dataset;
use clp_metadata_db::DatasetLookup;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::job_config::TimeRange;
use clp_rust_utils::types::ArchiveId;
use clp_rust_utils::types::DatasetId;

use crate::archive_manager::selection::ArchiveReference;
use crate::archive_manager::selection::ArchiveSelection;
use crate::archive_manager::storage::ArchiveStorage;
use crate::archive_manager::storage::StorageDeletion;

/// An archive that a deletion selects, with its dataset.
#[derive(Debug)]
pub struct SelectedArchive {
    pub metadata: ArchiveMetadata,
    pub dataset_name: DatasetName,

    /// The archive storage path of the archive's dataset, which the archive is stored under.
    pub archive_storage_path: String,
}

/// The archives that a deletion selects.
#[derive(Debug)]
pub struct SelectedArchives {
    /// The selected archives, each once, in selection order.
    pub archives: Vec<SelectedArchive>,

    /// The archives that the selection names but that don't exist, in selection order.
    pub unknown: Vec<ArchiveReference>,
}

/// Finds the archives of `dataset` whose timestamp range lies within `time_range`.
///
/// # Returns
///
/// The archives, ordered by begin timestamp, on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`ClpDbClient::acquire`]'s return values on failure.
/// * Forwards `get_live_dataset`'s return values on failure.
/// * Forwards [`ArchiveLookup::find_archives_by_time_range`]'s return values on failure.
pub async fn find_archives(
    client: &ClpDbClient,
    dataset: &DatasetName,
    time_range: &TimeRange,
) -> anyhow::Result<Vec<ArchiveMetadata>> {
    let mut connection = client.acquire().await?;
    let dataset = get_live_dataset(&mut connection, dataset).await?;
    connection
        .find_archives_by_time_range(dataset.id, time_range)
        .await
        .context("failed to find the archives")
}

/// Finds the archives that `selection` selects, without deleting them. Logs each archive that a
/// deletion would delete, and each archive the selection names that doesn't exist.
///
/// # Returns
///
/// The selected archives on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`ClpDbClient::begin`]'s return values on failure.
/// * Forwards `select`'s return values on failure.
/// * Forwards [`clp_metadata_db::ClpDbTransaction::rollback`]'s return values on failure.
pub async fn preview_deletion(
    client: &ClpDbClient,
    selection: &ArchiveSelection,
) -> anyhow::Result<SelectedArchives> {
    // One transaction gives every lookup the same snapshot.
    let mut transaction = client.begin().await?;
    let selected = select(&mut transaction, selection).await?;
    for archive in &selected.archives {
        tracing::info!(
            archive_id = % archive.metadata.id,
            archive_uuid = % archive.metadata.uuid,
            dataset = % archive.dataset_name,
            "Archive would be deleted."
        );
    }
    transaction.rollback().await?;
    tracing::info!("Dry-run finished.");
    Ok(selected)
}

/// Deletes the archives that `selection` selects: their rows in one transaction, then, once it
/// commits, their storage. Logs each archive the selection names that doesn't exist.
///
/// It takes no lock against running queries, which may fail to read the deleted archives. An
/// archive whose rows another caller deletes in the meantime is skipped. An archive missing from
/// storage is skipped too, and a failure to delete one from storage doesn't stop the others: its
/// rows are already gone, so its storage is left orphaned.
///
/// # Returns
///
/// The archives that this call deleted, and the unknown archives that the selection names, on
/// success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if an archive's storage can't be deleted, after trying every archive.
/// * Forwards [`ClpDbClient::begin`]'s return values on failure.
/// * Forwards `select`'s return values on failure.
/// * Forwards [`ArchiveManagement::delete_archives`]'s return values on failure.
/// * Forwards [`clp_metadata_db::ClpDbTransaction::commit`]'s return values on failure.
/// * Forwards [`clp_metadata_db::ClpDbTransaction::rollback`]'s return values on failure.
pub async fn delete_archives(
    client: &ClpDbClient,
    storage: &ArchiveStorage,
    selection: &ArchiveSelection,
) -> anyhow::Result<SelectedArchives> {
    let mut transaction = client.begin().await?;
    let SelectedArchives { archives, unknown } = select(&mut transaction, selection).await?;
    if archives.is_empty() {
        transaction.rollback().await?;
        return Ok(SelectedArchives { archives, unknown });
    }

    let ids: Vec<ArchiveId> = archives.iter().map(|archive| archive.metadata.id).collect();
    let concurrently_deleted_ids: HashSet<ArchiveId> = transaction
        .delete_archives(&ids)
        .await
        .context("failed to delete the archives from the database")?
        .into_iter()
        .collect();
    transaction
        .commit()
        .await
        .context("failed to commit the deletion of the archives from the database")?;

    let mut deleted_archives = Vec::with_capacity(archives.len());
    for archive in archives {
        if concurrently_deleted_ids.contains(&archive.metadata.id) {
            tracing::warn!(
                archive_id = % archive.metadata.id,
                "Archive was deleted by another caller. Skipping its storage deletion."
            );
            continue;
        }
        tracing::info!(
            archive_id = % archive.metadata.id,
            archive_uuid = % archive.metadata.uuid,
            dataset = % archive.dataset_name,
            "Deleted archive from the database."
        );
        deleted_archives.push(archive);
    }
    tracing::info!("Finished deleting archives from the database.");

    let mut num_storage_failures = 0_usize;
    for archive in &deleted_archives {
        match storage
            .delete_archive(&archive.archive_storage_path, &archive.metadata.uuid)
            .await
        {
            Ok(StorageDeletion::Deleted) => {
                tracing::info!(
                    archive_id = % archive.metadata.id,
                    "Deleted archive from storage."
                );
            }
            Ok(StorageDeletion::NotFound) => {
                tracing::warn!(
                    archive_id = % archive.metadata.id,
                    "Archive not found in storage. Skipping its deletion."
                );
            }
            Err(e) => {
                tracing::error!(
                    archive_id = % archive.metadata.id,
                    error = % format!("{e:#}"),
                    "Failed to delete archive from storage; its metadata has already been deleted."
                );
                num_storage_failures += 1;
            }
        }
    }
    if num_storage_failures != 0 {
        anyhow::bail!("failed to delete {num_storage_failures} archive(s) from storage");
    }
    tracing::info!("Finished deleting archives from storage.");
    Ok(SelectedArchives {
        archives: deleted_archives,
        unknown,
    })
}

/// Finds the archives that `selection` selects, with their datasets. Logs each archive the
/// selection names that doesn't exist, or that nothing matches.
///
/// # Type Parameters
///
/// * `HandleType` - The handle that the lookups run on.
///
/// # Returns
///
/// The selected archives on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if a selected archive's dataset isn't among the live datasets, which only
///   lookups on different snapshots can cause.
/// * Forwards [`get_live_dataset`]'s return values on failure.
/// * Forwards [`ArchiveLookup::find_archives_by_ids`]'s return values on failure.
/// * Forwards [`ArchiveLookup::find_archive_ids_by_uuids`]'s return values on failure.
/// * Forwards [`ArchiveLookup::find_archives_by_time_range`]'s return values on failure.
/// * Forwards [`DatasetLookup::get_datasets`]'s return values on failure.
async fn select<HandleType: ArchiveLookup + DatasetLookup + Send>(
    db: &mut HandleType,
    selection: &ArchiveSelection,
) -> anyhow::Result<SelectedArchives> {
    let (found_archives, unknown) = match selection {
        ArchiveSelection::Ids { ids } => {
            let archives = db
                .find_archives_by_ids(ids)
                .await
                .context("failed to find the archives by ID")?;
            split_found(
                ids.iter().copied().map(ArchiveReference::Id).collect(),
                archives,
            )
        }
        ArchiveSelection::Uuids { dataset, uuids } => {
            let dataset = get_live_dataset(db, dataset).await?;
            let ids = db
                .find_archive_ids_by_uuids(dataset.id, uuids)
                .await
                .context("failed to find the archives by UUID")?;
            let mut unknown = Vec::new();
            let mut found_references = Vec::new();
            let mut found_ids = Vec::new();
            for (uuid, id) in uuids.iter().zip(ids) {
                match id {
                    Some(id) => {
                        found_references.push(ArchiveReference::Uuid(*uuid));
                        found_ids.push(id);
                    }
                    None => unknown.push(ArchiveReference::Uuid(*uuid)),
                }
            }
            let archives = db
                .find_archives_by_ids(&found_ids)
                .await
                .context("failed to find the archives by ID")?;
            let (found_archives, unknown_after_lookup) = split_found(found_references, archives);
            unknown.extend(unknown_after_lookup);
            (found_archives, unknown)
        }
        ArchiveSelection::TimeRange {
            dataset,
            time_range,
        } => {
            let dataset = get_live_dataset(db, dataset).await?;
            let archives = db
                .find_archives_by_time_range(dataset.id, time_range)
                .await
                .context("failed to find the archives by time range")?;
            (archives, Vec::new())
        }
    };

    if !unknown.is_empty() {
        tracing::warn!(
            archives = % join_references(&unknown),
            "The following archives don't exist."
        );
    }
    if found_archives.is_empty() {
        match selection {
            ArchiveSelection::Ids { .. } | ArchiveSelection::Uuids { .. } => {
                tracing::info!("No archives found with matching IDs.");
            }
            ArchiveSelection::TimeRange { .. } => {
                tracing::info!("No archives found within the specified time range.");
            }
        }
        return Ok(SelectedArchives {
            archives: Vec::new(),
            unknown,
        });
    }

    let datasets: HashMap<DatasetId, Dataset> = db
        .get_datasets()
        .await
        .context("failed to read the datasets")?
        .into_iter()
        .map(|dataset| (dataset.id, dataset))
        .collect();
    let mut selected_ids = HashSet::new();
    let archives = found_archives
        .into_iter()
        .filter(|archive| selected_ids.insert(archive.id))
        .map(|archive| {
            let dataset = datasets.get(&archive.dataset_id).with_context(|| {
                format!(
                    "archive {}'s dataset {} isn't a live dataset",
                    archive.id, archive.dataset_id
                )
            })?;
            Ok(SelectedArchive {
                metadata: archive,
                dataset_name: dataset.name.clone(),
                archive_storage_path: dataset.config.archive_storage_path.clone(),
            })
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(SelectedArchives { archives, unknown })
}

/// # Type Parameters
///
/// * `HandleType` - The handle that the lookup runs on.
///
/// # Returns
///
/// The live dataset named `name` on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if no live dataset is named `name`.
/// * Forwards [`DatasetLookup::get_dataset_by_name`]'s return values on failure.
async fn get_live_dataset<HandleType: DatasetLookup + Send>(
    db: &mut HandleType,
    name: &DatasetName,
) -> anyhow::Result<Dataset> {
    db.get_dataset_by_name(name)
        .await
        .with_context(|| format!("failed to read dataset `{name}`"))?
        .with_context(|| format!("dataset `{name}` doesn't exist"))
}

/// Splits the results of a lookup into the archives it found and the references it found nothing
/// for.
///
/// # Returns
///
/// A tuple containing:
///
/// * The found archives, in the order of `references`.
/// * The references whose lookup result is `None`, in order.
fn split_found(
    references: Vec<ArchiveReference>,
    archives: Vec<Option<ArchiveMetadata>>,
) -> (Vec<ArchiveMetadata>, Vec<ArchiveReference>) {
    let mut found_archives = Vec::with_capacity(archives.len());
    let mut unknown = Vec::new();
    for (reference, archive) in references.into_iter().zip(archives) {
        match archive {
            Some(archive) => found_archives.push(archive),
            None => unknown.push(reference),
        }
    }
    (found_archives, unknown)
}

/// # Returns
///
/// The references, separated by `, `.
fn join_references(references: &[ArchiveReference]) -> String {
    references
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}
