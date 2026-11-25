//! Shared helpers for archive planning.

use std::collections::HashSet;

use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::dataset::NonEmptyDatasetNames;
use spider_core::task::ExecutionPolicy;
use spider_core::task::TimeoutPolicy;

pub(super) const SEARCH_TIMEOUT_RATES: ArchiveTaskTimeoutRates = ArchiveTaskTimeoutRates {
    soft_millisecs_per_mib: 10,
    hard_millisecs_per_mib: 2000,
};

/// Timeout rates in milliseconds per MiB of uncompressed archive data.
pub(super) struct ArchiveTaskTimeoutRates {
    soft_millisecs_per_mib: u64,
    hard_millisecs_per_mib: u64,
}

/// A non-empty set of distinct dataset names.
///
/// # Type Parameters
///
/// * `'dataset_lifetime` - The lifetime of the borrowed dataset names.
#[derive(Debug)]
pub(super) struct NonEmptyDatasetNameSet<'dataset_lifetime> {
    names: HashSet<&'dataset_lifetime DatasetName>,
}

impl<'dataset_lifetime> NonEmptyDatasetNameSet<'dataset_lifetime> {
    /// # Returns
    ///
    /// An iterator over the distinct dataset names.
    pub(super) fn iter(&self) -> impl Iterator<Item = &'dataset_lifetime DatasetName> + '_ {
        self.names.iter().copied()
    }

    /// # Returns
    ///
    /// The number of distinct dataset names.
    pub(super) fn len(&self) -> usize {
        self.names.len()
    }
}

impl<'dataset_lifetime> From<&'dataset_lifetime NonEmptyDatasetNames>
    for NonEmptyDatasetNameSet<'dataset_lifetime>
{
    fn from(names: &'dataset_lifetime NonEmptyDatasetNames) -> Self {
        Self {
            names: names.as_slice().iter().collect(),
        }
    }
}

/// Derives a task's execution policy from the size of the archive it processes.
///
/// The soft timeout is an aggressive estimate of the task's execution time, whereas the hard
/// timeout is a conservative deadline that assumes the entire archive is extracted.
///
/// # Returns
///
/// The task's execution policy.
pub(super) fn task_execution_policy(
    archive_uncompressed_size: u64,
    rates: &ArchiveTaskTimeoutRates,
    max_num_retry: u32,
) -> ExecutionPolicy {
    const BYTES_PER_MIB: u64 = 1024 * 1024;
    const MIN_SOFT_TIMEOUT_MILLISECS: u64 = 1000;
    const MIN_HARD_TIMEOUT_MILLISECS: u64 = 10_000;

    /// The maximum timeout accepted by Spider, in milliseconds (24 hours).
    const MAX_TIMEOUT_MILLISECS: u64 = 1000 * 60 * 60 * 24;

    const _: () = assert!(MIN_SOFT_TIMEOUT_MILLISECS < MIN_HARD_TIMEOUT_MILLISECS);
    const _: () = assert!(MIN_HARD_TIMEOUT_MILLISECS <= MAX_TIMEOUT_MILLISECS);

    let hard_timeout_ms = (archive_uncompressed_size.saturating_mul(rates.hard_millisecs_per_mib)
        / BYTES_PER_MIB)
        .clamp(MIN_HARD_TIMEOUT_MILLISECS, MAX_TIMEOUT_MILLISECS);
    let soft_timeout_ms = (archive_uncompressed_size.saturating_mul(rates.soft_millisecs_per_mib)
        / BYTES_PER_MIB)
        .clamp(MIN_SOFT_TIMEOUT_MILLISECS, hard_timeout_ms - 1);

    ExecutionPolicy {
        max_num_retry,
        timeout_policy: TimeoutPolicy {
            soft_timeout_ms,
            hard_timeout_ms,
        },
        ..ExecutionPolicy::default()
    }
}

#[cfg(test)]
mod tests {
    use super::SEARCH_TIMEOUT_RATES;
    use super::task_execution_policy;

    #[test]
    fn search_task_timeouts_scale_with_archive_size_and_respect_limits() {
        const MIB: u64 = 1024 * 1024;
        const MAX_NUM_RETRY: u32 = 3;

        for (archive_size, expected_soft_timeout_ms, expected_hard_timeout_ms) in [
            (1, 1_000, 10_000),
            (1024 * MIB + MIB / 2, 10_245, 2_049_000),
            (u64::MAX, 86_399_999, 86_400_000),
        ] {
            let policy = task_execution_policy(archive_size, &SEARCH_TIMEOUT_RATES, MAX_NUM_RETRY);
            assert_eq!(
                policy.timeout_policy.soft_timeout_ms, expected_soft_timeout_ms,
                "soft timeout for {archive_size} bytes"
            );
            assert_eq!(
                policy.timeout_policy.hard_timeout_ms, expected_hard_timeout_ms,
                "hard timeout for {archive_size} bytes"
            );
            assert_eq!(policy.max_num_retry, MAX_NUM_RETRY);
        }
    }
}
