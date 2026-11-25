//! Options for selecting the archives that a query job searches from the shared metadata tables.

use std::num::NonZeroU32;
use std::num::NonZeroUsize;

use clp_rust_utils::clp_config::package::config::ArchiveOutput as ArchiveOutputConfig;
use clp_rust_utils::clp_config::package::config::QueryCoordinator as CoordinatorConfig;

use crate::Error;

/// Options for selecting the archives to search.
pub struct ArchiveSelectionOptions {
    pub archive_retention_period_minutes: Option<NonZeroU32>,
    pub max_datasets_per_query: Option<NonZeroUsize>,
}

impl ArchiveSelectionOptions {
    /// Factory function.
    ///
    /// Takes the package-wide archive retention period from `archive_output_config` and the dataset
    /// limit from `coordinator_config`.
    ///
    /// # Returns
    ///
    /// The archive selection options on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::InvalidConfiguration`] if the archive retention period exceeds `u32::MAX`
    ///   minutes.
    pub fn from_config(
        coordinator_config: &CoordinatorConfig,
        archive_output_config: &ArchiveOutputConfig,
    ) -> Result<Self, Error> {
        let archive_retention_period_minutes = archive_output_config
            .retention_period
            .map(|period| {
                NonZeroU32::try_from(period).map_err(|_| {
                    Error::InvalidConfiguration(format!(
                        "`archive_output.retention_period` must not exceed {} minutes, got \
                         {period}",
                        u32::MAX
                    ))
                })
            })
            .transpose()?;
        Ok(Self {
            archive_retention_period_minutes,
            max_datasets_per_query: coordinator_config.max_datasets_per_query,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::num::NonZeroU64;

    use clp_rust_utils::clp_config::package::config::ArchiveOutput as ArchiveOutputConfig;
    use clp_rust_utils::clp_config::package::config::QueryCoordinator as CoordinatorConfig;

    use super::ArchiveSelectionOptions;
    use crate::Error;

    #[test]
    fn archive_retention_periods_beyond_u32_minutes_are_rejected() -> anyhow::Result<()> {
        let coordinator_config = CoordinatorConfig::default();
        let largest_period = ArchiveOutputConfig {
            retention_period: Some(NonZeroU64::from(NonZeroU32::MAX)),
            ..ArchiveOutputConfig::default()
        };
        assert_eq!(
            ArchiveSelectionOptions::from_config(&coordinator_config, &largest_period)?
                .archive_retention_period_minutes,
            Some(NonZeroU32::MAX)
        );

        let oversized_period = ArchiveOutputConfig {
            retention_period: NonZeroU64::from(NonZeroU32::MAX).checked_add(1),
            ..ArchiveOutputConfig::default()
        };
        let error = ArchiveSelectionOptions::from_config(&coordinator_config, &oversized_period)
            .err()
            .expect("a retention period beyond `u32::MAX` minutes should be rejected");
        assert!(
            matches!(
                &error,
                Error::InvalidConfiguration(message)
                    if message == "`archive_output.retention_period` must not exceed 4294967295 \
                                   minutes, got 4294967296"
            ),
            "unexpected error: {error:?}"
        );
        Ok(())
    }
}
