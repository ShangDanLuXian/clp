/// The default dataset name (mirror of `clp_py_utils.clp_config.CLP_DEFAULT_DATASET_NAME`).
pub const CLP_DEFAULT_DATASET_NAME: &str = "default";

/// # Returns
///
/// `dataset` when set, otherwise the `CLP_S` default dataset name [`CLP_DEFAULT_DATASET_NAME`].
#[must_use]
pub fn resolve_dataset_name(dataset: Option<&str>) -> &str {
    dataset.unwrap_or(CLP_DEFAULT_DATASET_NAME)
}

/// Idempotently registers `dataset` (defaulting a missing one to the `CLP_S` default) in
/// `datasets_table`, recording `archive_storage_path` as its archive storage path.
///
/// # Returns
///
/// The dataset's ID.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
pub async fn register_dataset(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    datasets_table: &str,
    archive_storage_path: &str,
    dataset: Option<&str>,
) -> Result<u64, sqlx::Error> {
    // NOTE: `LAST_INSERT_ID(id)` sets the statement's insert ID to the existing row's ID when the
    // dataset is already registered, so the ID can be read back without a second query.
    let query_result = sqlx::query(&format!(
        "INSERT INTO `{datasets_table}` (name, archive_storage_path) VALUES (?, ?) ON DUPLICATE \
         KEY UPDATE id = LAST_INSERT_ID(id), archive_storage_path = VALUES(archive_storage_path)"
    ))
    .bind(resolve_dataset_name(dataset))
    .bind(archive_storage_path)
    .execute(&mut **tx)
    .await?;
    Ok(query_result.last_insert_id())
}

#[cfg(test)]
mod tests {
    use super::CLP_DEFAULT_DATASET_NAME;
    use super::resolve_dataset_name;

    #[test]
    fn resolve_dataset_name_passes_through_some() {
        assert_eq!(resolve_dataset_name(Some("mydataset")), "mydataset");
    }

    #[test]
    fn resolve_dataset_name_defaults_none_to_default() {
        assert_eq!(resolve_dataset_name(None), CLP_DEFAULT_DATASET_NAME);
    }
}
