//! Decoders of column values that `sqlx` can't decode into their Rust types directly.

use std::array::TryFromSliceError;
use std::string::FromUtf8Error;

use clp_rust_utils::types::ArchiveUuid;

/// An archive UUID as stored in a `BINARY(16)` column.
pub struct StoredArchiveUuid {
    pub value: ArchiveUuid,
}

impl TryFrom<Vec<u8>> for StoredArchiveUuid {
    type Error = TryFromSliceError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self {
            value: ArchiveUuid::from_bytes(bytes.as_slice().try_into()?),
        })
    }
}

/// A string as stored in a `utf8mb4_bin` column. `sqlx` doesn't decode such a column as a
/// [`String`], since the server reports it with the `BINARY` flag.
pub struct StoredString {
    pub value: String,
}

impl TryFrom<Vec<u8>> for StoredString {
    type Error = FromUtf8Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self {
            value: String::from_utf8(bytes)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::StoredArchiveUuid;

    #[test]
    fn stored_archive_uuid_accepts_only_sixteen_bytes() -> anyhow::Result<()> {
        let bytes: Vec<u8> = (0..16).collect();
        let stored = StoredArchiveUuid::try_from(bytes.clone())?;
        assert_eq!(stored.value.as_bytes().as_slice(), bytes.as_slice());

        // `TryFromSliceError` has no payload to compare, so `is_err` is the exact outcome.
        for length in [0, 15, 17, 36] {
            assert!(
                StoredArchiveUuid::try_from(vec![0; length]).is_err(),
                "a {length}-byte UUID should be rejected"
            );
        }
        Ok(())
    }
}
