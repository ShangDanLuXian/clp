//! Archive selection for search queries.

use std::cmp::Reverse;

use clp_metadata_db::ArchiveLookup;
use clp_metadata_db::DatasetLookup;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::dataset::NonEmptyDatasetNames;
use clp_rust_utils::job_config::SearchQuery;
use spider_core::task::ExecutionPolicy;

use crate::Error;
use crate::job_handle::ArchiveMetadata;
use crate::job_handle::ArchiveSelectionOptions;
use crate::job_handle::archive::NonEmptyDatasetNameSet;
use crate::job_handle::archive::SEARCH_TIMEOUT_RATES;
use crate::job_handle::archive::task_execution_policy;

/// Selects the archives a query job should search, and derives each one's task execution policy.
///
/// # Type Parameters
///
/// * `'context_lifetime` - The lifetime of the metadata database handle and selection options.
/// * `MetadataDbType` - The type of the metadata database handle that the archives are selected
///   from.
pub(super) struct ArchiveSelector<'context_lifetime, MetadataDbType: DatasetLookup + ArchiveLookup>
{
    metadata_db: &'context_lifetime mut MetadataDbType,
    options: &'context_lifetime ArchiveSelectionOptions,
}

impl<'context_lifetime, MetadataDbType: DatasetLookup + ArchiveLookup>
    ArchiveSelector<'context_lifetime, MetadataDbType>
{
    /// Creates an archive selector with the given metadata database and selection options.
    ///
    /// # Returns
    ///
    /// A newly created [`ArchiveSelector`].
    pub(super) const fn new(
        metadata_db: &'context_lifetime mut MetadataDbType,
        options: &'context_lifetime ArchiveSelectionOptions,
    ) -> Self {
        Self {
            metadata_db,
            options,
        }
    }

    /// Prepares the task inputs for the query job.
    ///
    /// This method validates and deduplicates the job's requested datasets, then retrieves from the
    /// CLP metadata database the metadata of the archives matching the query, ordered by descending
    /// archive end timestamp, and attaches an execution policy derived from each archive's
    /// uncompressed size to its task. The retention filter excludes archives that end before the
    /// cutoff, except archives with an end timestamp of zero.
    ///
    /// # Returns
    ///
    /// A vector of tuples on success, where each tuple contains:
    ///
    /// * The [`ArchiveMetadata`] identifying the archive searched by a single query task.
    /// * The [`ExecutionPolicy`] for that task.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::InvalidQueryJobConfig`] if any requested dataset doesn't exist.
    /// * Forwards [`Self::deduplicate_and_validate_requested_datasets`]'s return values on failure.
    /// * Forwards [`DatasetLookup::get_dataset_ids_by_names`]'s return values on failure.
    /// * Forwards [`ArchiveLookup::find_archives_for_query`]'s return values on failure.
    pub(super) async fn select(
        &mut self,
        query: &SearchQuery,
        job_creation_timestamp_millisecs: i64,
        max_num_retry: u32,
    ) -> Result<Vec<(ArchiveMetadata, ExecutionPolicy)>, Error> {
        let datasets: Vec<DatasetName> = self
            .deduplicate_and_validate_requested_datasets(&query.datasets)?
            .iter()
            .cloned()
            .collect();
        let dataset_ids = self.metadata_db.get_dataset_ids_by_names(&datasets).await?;
        if dataset_ids.len() != datasets.len() {
            return Err(Error::InvalidQueryJobConfig(
                "one or more requested datasets don't exist".to_owned(),
            ));
        }

        let mut selected_archives = Vec::new();
        for (dataset, dataset_id) in dataset_ids {
            let archives = self
                .metadata_db
                .find_archives_for_query(
                    dataset_id,
                    &query.time_range,
                    self.options.archive_retention_period_minutes,
                    job_creation_timestamp_millisecs,
                )
                .await?;
            selected_archives.extend(archives.into_iter().map(|archive| ArchiveMetadata {
                uuid: archive.uuid,
                dataset: dataset.clone(),
                uncompressed_size: archive.num_uncompressed_bytes,
                end_timestamp: archive.end_timestamp_millisecs,
            }));
        }
        selected_archives.sort_by_key(|archive| Reverse(archive.end_timestamp));

        Ok(selected_archives
            .into_iter()
            .map(|archive| {
                let execution_policy = task_execution_policy(
                    archive.uncompressed_size,
                    &SEARCH_TIMEOUT_RATES,
                    max_num_retry,
                );
                (archive, execution_policy)
            })
            .collect())
    }

    /// Validates and deduplicates the datasets requested by a query job.
    ///
    /// # Type Parameters
    ///
    /// * `'dataset_lifetime` - The lifetime of the requested dataset names.
    ///
    /// # Returns
    ///
    /// The distinct requested datasets on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::InvalidQueryJobConfig`] if the number of distinct datasets exceeds
    ///   [`ArchiveSelectionOptions::max_datasets_per_query`].
    fn deduplicate_and_validate_requested_datasets<'dataset_lifetime>(
        &self,
        requested_datasets: &'dataset_lifetime NonEmptyDatasetNames,
    ) -> Result<NonEmptyDatasetNameSet<'dataset_lifetime>, Error> {
        let datasets = NonEmptyDatasetNameSet::from(requested_datasets);

        if let Some(max_datasets_per_query) = self.options.max_datasets_per_query
            && datasets.len() > max_datasets_per_query.get()
        {
            return Err(Error::InvalidQueryJobConfig(format!(
                "the number of requested datasets ({}) exceeds `max_datasets_per_query` \
                 ({max_datasets_per_query})",
                datasets.len()
            )));
        }

        Ok(datasets)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroU32;
    use std::num::NonZeroUsize;

    use async_trait::async_trait;
    use clp_metadata_db::ArchiveLookup;
    use clp_metadata_db::Dataset;
    use clp_metadata_db::DatasetLookup;
    use clp_metadata_db::QueryArchive;
    use clp_rust_utils::dataset::DatasetName;
    use clp_rust_utils::dataset::NonEmptyDatasetNames;
    use clp_rust_utils::job_config::SearchQuery;
    use clp_rust_utils::job_config::TimeRange;
    use clp_rust_utils::types::ArchiveId;
    use clp_rust_utils::types::DatasetId;
    use clp_rust_utils::types::non_empty_string::ExpectedNonEmpty;
    use non_empty_string::NonEmptyString;

    use super::ArchiveSelector;
    use crate::Error;
    use crate::job_handle::ArchiveMetadata;
    use crate::job_handle::ArchiveSelectionOptions;
    use crate::job_handle::archive::SEARCH_TIMEOUT_RATES;
    use crate::job_handle::archive::task_execution_policy;

    /// The arguments of a [`ArchiveLookup::find_archives_for_query`] call.
    #[derive(Debug, Eq, PartialEq)]
    struct ArchiveQuery {
        dataset_id: DatasetId,
        time_range: TimeRange,
        retention_period_minutes: Option<NonZeroU32>,
        job_creation_timestamp_millisecs: i64,
    }

    /// A metadata database holding the given live datasets and their archives, which records the
    /// archive queries it receives.
    #[derive(Default)]
    struct MockMetadataDb {
        dataset_ids: HashMap<DatasetName, DatasetId>,

        /// Each dataset's archives, in the order of descending end timestamp that the real lookup
        /// guarantees.
        archives: HashMap<DatasetId, Vec<QueryArchive>>,

        archive_queries: Vec<ArchiveQuery>,
    }

    #[async_trait]
    impl DatasetLookup for MockMetadataDb {
        async fn get_datasets(&mut self) -> Result<Vec<Dataset>, clp_metadata_db::Error> {
            unreachable!("archive selection doesn't list datasets")
        }

        async fn get_dataset_by_name(
            &mut self,
            _name: &DatasetName,
        ) -> Result<Option<Dataset>, clp_metadata_db::Error> {
            unreachable!("archive selection doesn't read datasets by name")
        }

        async fn get_dataset_ids_by_names(
            &mut self,
            names: &[DatasetName],
        ) -> Result<HashMap<DatasetName, DatasetId>, clp_metadata_db::Error> {
            Ok(names
                .iter()
                .filter_map(|name| Some((name.clone(), *self.dataset_ids.get(name)?)))
                .collect())
        }
    }

    #[async_trait]
    impl ArchiveLookup for MockMetadataDb {
        async fn find_archives_for_query(
            &mut self,
            dataset_id: DatasetId,
            time_range: &TimeRange,
            retention_period_minutes: Option<NonZeroU32>,
            job_creation_timestamp_millisecs: i64,
        ) -> Result<Vec<QueryArchive>, clp_metadata_db::Error> {
            self.archive_queries.push(ArchiveQuery {
                dataset_id,
                time_range: *time_range,
                retention_period_minutes,
                job_creation_timestamp_millisecs,
            });
            Ok(self.archives.get(&dataset_id).cloned().unwrap_or_default())
        }
    }

    /// # Returns
    ///
    /// The dataset name `name`.
    ///
    /// # Panics
    ///
    /// Panics if `name` isn't a valid dataset name.
    fn dataset_name(name: &str) -> DatasetName {
        name.parse().expect("name should be a valid dataset name")
    }

    /// # Returns
    ///
    /// A search query over `datasets` for every log event in the time range `[100, 300]`.
    ///
    /// # Panics
    ///
    /// Panics if `datasets` is empty.
    fn search_query(datasets: Vec<DatasetName>) -> SearchQuery {
        SearchQuery {
            datasets: NonEmptyDatasetNames::try_from(datasets)
                .expect("dataset names should be non-empty"),
            query_string: NonEmptyString::from_static_str("*"),
            time_range: TimeRange::new(Some(100), Some(300)).expect("time range should be valid"),
            ignore_case: false,
        }
    }

    /// # Returns
    ///
    /// The archive with the ID `seed`, a UUID derived from `seed`, and the given size and end
    /// timestamp.
    ///
    /// # Panics
    ///
    /// Panics if `seed` has more than 12 digits, so the derived UUID is invalid.
    fn query_archive(seed: u64, num_uncompressed_bytes: u64, end_timestamp: i64) -> QueryArchive {
        QueryArchive {
            id: ArchiveId::from(seed),
            uuid: format!("00000000-0000-0000-0000-{seed:012}")
                .parse()
                .expect("the UUID should be valid"),
            num_uncompressed_bytes,
            end_timestamp_millisecs: end_timestamp,
        }
    }

    #[test]
    fn duplicate_requested_datasets_count_once_toward_max_datasets_per_query() {
        let mut metadata_db = MockMetadataDb::default();
        let options = ArchiveSelectionOptions {
            archive_retention_period_minutes: None,
            max_datasets_per_query: NonZeroUsize::new(1),
        };
        let selector = ArchiveSelector::new(&mut metadata_db, &options);
        let default_dataset = DatasetName::default();
        let duplicates =
            NonEmptyDatasetNames::try_from(vec![default_dataset.clone(), default_dataset.clone()])
                .expect("dataset names should be non-empty");
        assert_eq!(
            selector
                .deduplicate_and_validate_requested_datasets(&duplicates)
                .expect("duplicate datasets should count once")
                .iter()
                .collect::<Vec<_>>(),
            [&default_dataset]
        );
        let other_dataset: DatasetName = "other"
            .parse()
            .expect("name should be a valid dataset name");
        let distinct = NonEmptyDatasetNames::try_from(vec![default_dataset, other_dataset])
            .expect("dataset names should be non-empty");
        let error = selector
            .deduplicate_and_validate_requested_datasets(&distinct)
            .expect_err("two distinct datasets should exceed the limit");
        assert!(
            matches!(
                &error,
                Error::InvalidQueryJobConfig(message)
                    if message == "the number of requested datasets (2) exceeds \
                                   `max_datasets_per_query` (1)"
            ),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn selection_fails_without_querying_archives_if_any_dataset_does_not_exist()
    -> anyhow::Result<()> {
        let mut metadata_db = MockMetadataDb {
            dataset_ids: HashMap::from([(DatasetName::default(), DatasetId::from(1))]),
            archives: HashMap::from([(DatasetId::from(1), vec![query_archive(1, 1, 200)])]),
            ..MockMetadataDb::default()
        };
        let options = ArchiveSelectionOptions {
            archive_retention_period_minutes: None,
            max_datasets_per_query: None,
        };

        let error = ArchiveSelector::new(&mut metadata_db, &options)
            .select(
                &search_query(vec![DatasetName::default(), dataset_name("missing")]),
                0,
                0,
            )
            .await
            .expect_err("a missing dataset should fail the selection");
        assert!(
            matches!(
                &error,
                Error::InvalidQueryJobConfig(message)
                    if message == "one or more requested datasets don't exist"
            ),
            "unexpected error: {error:?}"
        );
        assert_eq!(metadata_db.archive_queries, []);
        Ok(())
    }

    #[tokio::test]
    async fn selection_merges_the_datasets_by_descending_end_timestamp() -> anyhow::Result<()> {
        const JOB_CREATION_TIMESTAMP_MILLISECS: i64 = 1_000_000;
        const MAX_NUM_RETRY: u32 = 2;

        let logs = dataset_name("logs");
        let other = dataset_name("other");
        let logs_id = DatasetId::from(1);
        let other_id = DatasetId::from(2);
        let mut metadata_db = MockMetadataDb {
            dataset_ids: HashMap::from([(logs.clone(), logs_id), (other.clone(), other_id)]),
            archives: HashMap::from([
                (
                    logs_id,
                    vec![query_archive(1, 10, 300), query_archive(2, 20, 100)],
                ),
                (
                    other_id,
                    vec![query_archive(3, 30, 200), query_archive(4, 40, 0)],
                ),
            ]),
            ..MockMetadataDb::default()
        };
        let options = ArchiveSelectionOptions {
            archive_retention_period_minutes: NonZeroU32::new(60),
            max_datasets_per_query: None,
        };
        let query = search_query(vec![logs.clone(), other.clone()]);

        let selected = ArchiveSelector::new(&mut metadata_db, &options)
            .select(&query, JOB_CREATION_TIMESTAMP_MILLISECS, MAX_NUM_RETRY)
            .await?;

        let expected: Vec<_> = [
            (1, &logs, 10, 300),
            (3, &other, 30, 200),
            (2, &logs, 20, 100),
            (4, &other, 40, 0),
        ]
        .into_iter()
        .map(|(seed, dataset, uncompressed_size, end_timestamp)| {
            (
                ArchiveMetadata {
                    uuid: query_archive(seed, uncompressed_size, end_timestamp).uuid,
                    dataset: dataset.clone(),
                    uncompressed_size,
                    end_timestamp,
                },
                task_execution_policy(uncompressed_size, &SEARCH_TIMEOUT_RATES, MAX_NUM_RETRY),
            )
        })
        .collect();
        assert_eq!(selected, expected);

        assert_eq!(metadata_db.archive_queries.len(), 2);
        for dataset_id in [logs_id, other_id] {
            let expected_query = ArchiveQuery {
                dataset_id,
                time_range: query.time_range,
                retention_period_minutes: options.archive_retention_period_minutes,
                job_creation_timestamp_millisecs: JOB_CREATION_TIMESTAMP_MILLISECS,
            };
            assert!(
                metadata_db.archive_queries.contains(&expected_query),
                "missing {expected_query:?} in {:?}",
                metadata_db.archive_queries
            );
        }
        Ok(())
    }
}
