//! Decoders of column values that `sqlx` can't decode into their Rust types directly.

use std::string::FromUtf8Error;

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
