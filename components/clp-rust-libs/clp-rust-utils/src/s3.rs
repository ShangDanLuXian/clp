mod client;
mod delete;
mod url;

pub use client::create_new_client;
pub use delete::DeleteError;
pub use delete::delete_by_key_prefix;
use non_empty_string::NonEmptyString;
pub use url::generate_s3_url;

/// Represents the unique identifier for an S3 object metadata entry in CLP DB.
pub type S3ObjectMetadataId = u64;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectMetadata {
    pub bucket: NonEmptyString,
    pub key: NonEmptyString,
    pub size: u64,
}
