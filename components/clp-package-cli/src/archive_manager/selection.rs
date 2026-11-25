//! The archives that a deletion selects, parsed from the command's arguments.

use std::fmt::Display;
use std::fmt::Formatter;
use std::num::ParseIntError;

use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::job_config::TimeRange;
use clp_rust_utils::types::ArchiveId;
use clp_rust_utils::types::ArchiveUuid;
use clp_rust_utils::types::ParseArchiveUuidError;

/// The archives that a deletion selects.
#[derive(Debug, Eq, PartialEq)]
pub enum ArchiveSelection {
    /// Archives by ID, across datasets.
    Ids { ids: Vec<ArchiveId> },

    /// Archives of one dataset by UUID. Kept only for compatibility with workflows built on UUIDs.
    Uuids {
        dataset: DatasetName,
        uuids: Vec<ArchiveUuid>,
    },

    /// The archives of one dataset whose timestamp range lies within `time_range`.
    TimeRange {
        dataset: DatasetName,
        time_range: TimeRange,
    },
}

impl ArchiveSelection {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A selection of the archives with the given IDs on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SelectionError::DatasetWithArchiveIds`] if `dataset` is given: an archive ID identifies
    ///   an archive across datasets.
    /// * [`SelectionError::InvalidArchiveId`] if an element of `ids` isn't an archive ID.
    pub fn by_ids(dataset: Option<&DatasetName>, ids: &[String]) -> Result<Self, SelectionError> {
        if dataset.is_some() {
            return Err(SelectionError::DatasetWithArchiveIds);
        }
        let ids = ids
            .iter()
            .map(|id| {
                id.parse::<u64>().map(ArchiveId::from).map_err(|source| {
                    SelectionError::InvalidArchiveId {
                        value: id.clone(),
                        source,
                    }
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(Self::Ids { ids })
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// A selection of the archives of `dataset` (the default dataset if `None`) with the given
    /// UUIDs on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SelectionError::InvalidArchiveUuid`] if an element of `uuids` isn't a UUID.
    pub fn by_uuids(
        dataset: Option<DatasetName>,
        uuids: &[String],
    ) -> Result<Self, SelectionError> {
        let uuids = uuids
            .iter()
            .map(|uuid| {
                uuid.parse::<ArchiveUuid>()
                    .map_err(SelectionError::InvalidArchiveUuid)
            })
            .collect::<Result<_, _>>()?;
        Ok(Self::Uuids {
            dataset: dataset.unwrap_or_default(),
            uuids,
        })
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// A selection of the archives of `dataset` (the default dataset if `None`) whose timestamp
    /// range lies within `time_range`.
    #[must_use]
    pub fn by_time_range(dataset: Option<DatasetName>, time_range: TimeRange) -> Self {
        Self::TimeRange {
            dataset: dataset.unwrap_or_default(),
            time_range,
        }
    }
}

/// A reference to an archive, as the command's arguments name it.
#[derive(Debug, Eq, PartialEq)]
pub enum ArchiveReference {
    /// An archive ID, which identifies an archive across datasets.
    Id(ArchiveId),

    /// An archive UUID, which identifies an archive within its dataset.
    Uuid(ArchiveUuid),
}

impl Display for ArchiveReference {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Id(id) => id.fmt(formatter),
            Self::Uuid(uuid) => uuid.fmt(formatter),
        }
    }
}

/// An error returned when a deletion's arguments don't select archives.
#[derive(Debug, thiserror::Error)]
pub enum SelectionError {
    /// `--dataset` was given with archive IDs, without `--uuid`.
    #[error(
        "`--dataset` can't be used with archive IDs, which identify archives across datasets; to \
         delete the archives of a dataset by UUID, pass `--uuid`"
    )]
    DatasetWithArchiveIds,

    /// An argument isn't a valid archive ID.
    #[error("invalid archive ID {value:?} (to delete archives by UUID, pass `--uuid`)")]
    InvalidArchiveId {
        value: String,
        source: ParseIntError,
    },

    /// An argument isn't a valid archive UUID.
    #[error(transparent)]
    InvalidArchiveUuid(ParseArchiveUuidError),
}

#[cfg(test)]
mod tests {
    use clp_rust_utils::dataset::DatasetName;
    use clp_rust_utils::types::ArchiveId;

    use super::ArchiveSelection;
    use super::SelectionError;

    #[test]
    fn by_ids_rejects_a_dataset_and_non_numeric_ids() -> anyhow::Result<()> {
        const UUID: &str = "018e90e5-8b2a-4a61-a2fc-cac799936caf";

        assert_eq!(
            ArchiveSelection::by_ids(None, &["1".to_owned(), "42".to_owned()])?,
            ArchiveSelection::Ids {
                ids: vec![ArchiveId::from(1), ArchiveId::from(42)],
            }
        );

        let error = ArchiveSelection::by_ids(Some(&DatasetName::default()), &["1".to_owned()])
            .expect_err("a dataset should be rejected with archive IDs");
        assert!(
            matches!(error, SelectionError::DatasetWithArchiveIds),
            "unexpected error: {error:?}"
        );

        let error = ArchiveSelection::by_ids(None, &["1".to_owned(), UUID.to_owned()])
            .expect_err("a UUID should be rejected as an archive ID");
        assert!(
            matches!(&error, SelectionError::InvalidArchiveId { value, .. } if value == UUID),
            "unexpected error: {error:?}"
        );
        Ok(())
    }

    #[test]
    fn by_uuids_defaults_to_the_default_dataset() -> anyhow::Result<()> {
        const UUID: &str = "018e90e5-8b2a-4a61-a2fc-cac799936caf";

        let dataset: DatasetName = "logs".parse()?;
        assert_eq!(
            ArchiveSelection::by_uuids(Some(dataset.clone()), &[UUID.to_owned()])?,
            ArchiveSelection::Uuids {
                dataset,
                uuids: vec![UUID.parse()?],
            }
        );
        assert_eq!(
            ArchiveSelection::by_uuids(None, &[UUID.to_owned()])?,
            ArchiveSelection::Uuids {
                dataset: DatasetName::default(),
                uuids: vec![UUID.parse()?],
            }
        );

        let error = ArchiveSelection::by_uuids(None, &["1".to_owned()])
            .expect_err("an archive ID should be rejected as a UUID");
        assert!(
            matches!(error, SelectionError::InvalidArchiveUuid(_)),
            "unexpected error: {error:?}"
        );
        Ok(())
    }
}
