//! The values that the metadata database operations take and return.

use std::num::NonZeroU32;

use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::types::ArchiveId;
use clp_rust_utils::types::ArchiveUuid;
use clp_rust_utils::types::DatasetId;
use serde::Deserialize;
use serde::Serialize;

/// An archive's inclusive range of timestamps, in Unix epoch milliseconds.
///
/// The begin timestamp never exceeds the end timestamp. An archive without timestamps has the range
/// [`ArchiveTimestampRange::WITHOUT_TIMESTAMPS`].
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "UncheckedArchiveTimestampRange")]
pub struct ArchiveTimestampRange {
    begin_timestamp_millisecs: i64,
    end_timestamp_millisecs: i64,
}

impl ArchiveTimestampRange {
    /// The range of an archive without timestamps.
    pub const WITHOUT_TIMESTAMPS: Self = Self {
        begin_timestamp_millisecs: 0,
        end_timestamp_millisecs: 0,
    };

    /// Factory function.
    ///
    /// # Returns
    ///
    /// The range between the given inclusive timestamps on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InvalidArchiveTimestampRangeError`] if `begin_timestamp_millisecs` exceeds
    ///   `end_timestamp_millisecs`.
    pub const fn new(
        begin_timestamp_millisecs: i64,
        end_timestamp_millisecs: i64,
    ) -> Result<Self, InvalidArchiveTimestampRangeError> {
        if begin_timestamp_millisecs > end_timestamp_millisecs {
            return Err(InvalidArchiveTimestampRangeError {
                begin_timestamp_millisecs,
                end_timestamp_millisecs,
            });
        }
        Ok(Self {
            begin_timestamp_millisecs,
            end_timestamp_millisecs,
        })
    }

    #[must_use]
    pub const fn begin_timestamp_millisecs(&self) -> i64 {
        self.begin_timestamp_millisecs
    }

    #[must_use]
    pub const fn end_timestamp_millisecs(&self) -> i64 {
        self.end_timestamp_millisecs
    }

    /// # Returns
    ///
    /// Whether the range isn't [`Self::WITHOUT_TIMESTAMPS`].
    #[must_use]
    pub const fn has_timestamps(&self) -> bool {
        self.begin_timestamp_millisecs != Self::WITHOUT_TIMESTAMPS.begin_timestamp_millisecs
            || self.end_timestamp_millisecs != Self::WITHOUT_TIMESTAMPS.end_timestamp_millisecs
    }

    /// # Returns
    ///
    /// The span of the range, `end - begin`, computed with [`i64::abs_diff`]. It can't overflow,
    /// because the span of any valid range fits in a `u64`.
    #[must_use]
    pub const fn span_millisecs(&self) -> u64 {
        self.end_timestamp_millisecs
            .abs_diff(self.begin_timestamp_millisecs)
    }
}

impl TryFrom<UncheckedArchiveTimestampRange> for ArchiveTimestampRange {
    type Error = InvalidArchiveTimestampRangeError;

    fn try_from(value: UncheckedArchiveTimestampRange) -> Result<Self, Self::Error> {
        Self::new(
            value.begin_timestamp_millisecs,
            value.end_timestamp_millisecs,
        )
    }
}

/// An error returned when an archive's begin timestamp exceeds its end timestamp.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
#[error(
    "archive begin timestamp {begin_timestamp_millisecs} is greater than end timestamp \
     {end_timestamp_millisecs}"
)]
pub struct InvalidArchiveTimestampRangeError {
    begin_timestamp_millisecs: i64,
    end_timestamp_millisecs: i64,
}

/// The type of a dataset's column.
///
/// Mirror of `clp_s::NodeType` (`components/core/src/clp_s/SchemaTree.hpp`). Must be kept in sync.
#[derive(Clone, Copy, Debug, Eq, PartialEq, sqlx::Type)]
#[repr(u8)]
pub enum ColumnType {
    Integer = 0,
    Float = 1,
    ClpString = 2,
    VarString = 3,
    Boolean = 4,
    Object = 5,
    UnstructuredArray = 6,
    NullValue = 7,
    DeprecatedDateString = 8,
    StructurizedArray = 9,
    Metadata = 10,
    DeltaInteger = 11,
    FormattedFloat = 12,
    DictionaryFloat = 13,
    Timestamp = 14,
    StructurizedText = 15,
    ParentRule = 16,
    UnstructurizedText = 17,
    Unknown = 18,
}

/// A dataset's configurable settings, stored in its `clp_datasets` row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetConfig {
    /// The directory or S3 key prefix that the dataset's archives are written to.
    pub archive_storage_path: String,

    /// How long archives are kept, by end timestamp. Always `None` in this release: the
    /// package-wide retention period applies.
    pub retention_period_minutes: Option<NonZeroU32>,
}

/// A dataset to create.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetDescriptor {
    pub name: DatasetName,
    pub config: DatasetConfig,
}

/// A live dataset, as stored.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Dataset {
    pub id: DatasetId,
    pub name: DatasetName,
    pub config: DatasetConfig,
}

/// The fields an archive is inserted with. The DB assigns its ID and creation time.
#[derive(Clone, Debug)]
pub struct ArchiveDescriptor {
    pub uuid: ArchiveUuid,
    pub timestamp_range: ArchiveTimestampRange,
    pub num_uncompressed_bytes: u64,
    pub num_compressed_bytes: u64,
}

/// A stored archive, as manual deletion selects it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArchiveMetadata {
    pub id: ArchiveId,
    pub dataset_id: DatasetId,
    pub uuid: ArchiveUuid,
    pub timestamp_range: ArchiveTimestampRange,
}

/// A stored archive, as a query selects it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryArchive {
    pub id: ArchiveId,
    pub uuid: ArchiveUuid,
    pub num_uncompressed_bytes: u64,
    pub end_timestamp_millisecs: i64,
}

/// The sizes of a set of archives.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpaceSavings {
    pub num_uncompressed_bytes: u64,
    pub num_compressed_bytes: u64,
}

/// The fields of an [`ArchiveTimestampRange`] before its invariant is checked.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedArchiveTimestampRange {
    begin_timestamp_millisecs: i64,
    end_timestamp_millisecs: i64,
}

#[cfg(test)]
mod tests {
    use super::ArchiveTimestampRange;
    use super::InvalidArchiveTimestampRangeError;

    #[test]
    fn archive_timestamp_range_rejects_only_a_begin_after_the_end() -> anyhow::Result<()> {
        for (begin, end) in [(0, 0), (-1, 1), (5, 5), (i64::MIN, i64::MAX)] {
            let range = ArchiveTimestampRange::new(begin, end)?;
            assert_eq!(range.begin_timestamp_millisecs(), begin);
            assert_eq!(range.end_timestamp_millisecs(), end);
        }
        for (begin, end) in [(1, 0), (i64::MAX, i64::MIN)] {
            assert_eq!(
                ArchiveTimestampRange::new(begin, end),
                Err(InvalidArchiveTimestampRangeError {
                    begin_timestamp_millisecs: begin,
                    end_timestamp_millisecs: end,
                })
            );
        }
        Ok(())
    }

    #[test]
    fn archive_timestamp_range_deserialization_rejects_a_begin_after_the_end() -> anyhow::Result<()>
    {
        let range: ArchiveTimestampRange = serde_json::from_value(serde_json::json!({
            "begin_timestamp_millisecs": 1,
            "end_timestamp_millisecs": 2,
        }))?;
        assert_eq!(range, ArchiveTimestampRange::new(1, 2)?);

        let error = serde_json::from_value::<ArchiveTimestampRange>(serde_json::json!({
            "begin_timestamp_millisecs": 2,
            "end_timestamp_millisecs": 1,
        }))
        .expect_err("a reversed range should be rejected");
        // `serde_json::Error` keeps a custom error only as its message, so the message is the only
        // way to check that the range's own error caused the rejection.
        assert_eq!(
            error.to_string(),
            ArchiveTimestampRange::new(2, 1)
                .expect_err("a reversed range should be rejected")
                .to_string()
        );
        Ok(())
    }

    #[test]
    fn has_timestamps_is_false_only_for_the_range_without_timestamps() -> anyhow::Result<()> {
        assert!(!ArchiveTimestampRange::WITHOUT_TIMESTAMPS.has_timestamps());
        assert!(!ArchiveTimestampRange::new(0, 0)?.has_timestamps());
        for (begin, end) in [(0, 1), (-1, 0), (1, 1), (-1, -1)] {
            assert!(ArchiveTimestampRange::new(begin, end)?.has_timestamps());
        }
        Ok(())
    }

    #[test]
    fn span_millisecs_covers_every_valid_range_without_overflow() -> anyhow::Result<()> {
        for (begin, end, span) in [
            (0, 0, 0),
            (-5, 5, 10),
            (i64::MIN, i64::MIN, 0),
            (i64::MIN, 0, i64::MIN.unsigned_abs()),
            (0, i64::MAX, i64::MAX.cast_unsigned()),
            (i64::MIN, i64::MAX, u64::MAX),
        ] {
            assert_eq!(
                ArchiveTimestampRange::new(begin, end)?.span_millisecs(),
                span
            );
        }
        Ok(())
    }
}
