//! The storage that archives are deleted from: a filesystem or an S3 bucket.

use std::path::Path;

use anyhow::Context;
use clp_rust_utils::aws::AWS_DEFAULT_REGION;
use clp_rust_utils::clp_config::package::config::ArchiveOutputStorage;
use clp_rust_utils::s3::create_new_client;
use clp_rust_utils::types::ArchiveUuid;
use non_empty_string::NonEmptyString;

/// The storage of the package's archives.
///
/// A dataset's archives are stored under the dataset's archive storage path, a directory on the
/// filesystem or a key prefix in the S3 bucket, each in an entry named after its UUID.
pub enum ArchiveStorage {
    /// The local filesystem, where an archive storage path is a directory.
    Fs,

    /// An S3 bucket, where an archive storage path is a key prefix.
    S3 {
        /// The client that deletes the archives' objects.
        client: aws_sdk_s3::Client,

        /// The bucket that the archives are stored in.
        bucket: NonEmptyString,
    },
}

impl ArchiveStorage {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The storage that `storage` configures. An S3 client authenticates as configured, in the
    /// configured region or [`AWS_DEFAULT_REGION`] if there is none.
    pub async fn from_config(storage: &ArchiveOutputStorage) -> Self {
        match storage {
            ArchiveOutputStorage::Fs { .. } => Self::Fs,
            ArchiveOutputStorage::S3 { s3_config, .. } => {
                let region = s3_config
                    .region_code
                    .as_ref()
                    .map_or(AWS_DEFAULT_REGION, NonEmptyString::as_str);
                Self::S3 {
                    client: create_new_client(
                        region,
                        s3_config.endpoint_url.as_ref(),
                        &s3_config.aws_authentication,
                    )
                    .await,
                    bucket: s3_config.bucket.clone(),
                }
            }
        }
    }

    /// Deletes the archive `uuid` stored under `archive_storage_path`.
    ///
    /// # Returns
    ///
    /// On success:
    ///
    /// * [`StorageDeletion::Deleted`] if the archive was deleted, or isn't in the S3 bucket (S3
    ///   doesn't report missing objects).
    /// * [`StorageDeletion::NotFound`] if the archive isn't on the filesystem.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`tokio::fs::symlink_metadata`]'s return values on failure, except for
    ///   [`std::io::ErrorKind::NotFound`].
    /// * Forwards [`tokio::fs::remove_dir_all`]'s return values on failure.
    /// * Forwards [`tokio::fs::remove_file`]'s return values on failure.
    /// * Forwards [`DeleteObjectFluentBuilder::send`]'s return values on failure.
    ///
    /// [`DeleteObjectFluentBuilder::send`]:
    ///     aws_sdk_s3::operation::delete_object::builders::DeleteObjectFluentBuilder::send
    pub(crate) async fn delete_archive(
        &self,
        archive_storage_path: &str,
        uuid: &ArchiveUuid,
    ) -> anyhow::Result<StorageDeletion> {
        match self {
            Self::Fs => {
                let path = Path::new(archive_storage_path).join(uuid.to_string());
                let metadata = match tokio::fs::symlink_metadata(&path).await {
                    Ok(metadata) => metadata,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        return Ok(StorageDeletion::NotFound);
                    }
                    Err(e) => {
                        return Err(e)
                            .with_context(|| format!("failed to inspect {}", path.display()));
                    }
                };
                // A clp-s archive is a directory, or a single file if it was compressed into one.
                if metadata.is_dir() {
                    tokio::fs::remove_dir_all(&path).await
                } else {
                    tokio::fs::remove_file(&path).await
                }
                .with_context(|| format!("failed to delete {}", path.display()))?;
            }
            Self::S3 { client, bucket } => {
                let key = format!("{archive_storage_path}/{uuid}");
                client
                    .delete_object()
                    .bucket(bucket.as_str())
                    .key(&key)
                    .send()
                    .await
                    .with_context(|| format!("failed to delete s3://{bucket}/{key}"))?;
            }
        }
        Ok(StorageDeletion::Deleted)
    }
}

/// The outcome of deleting an archive from storage.
pub enum StorageDeletion {
    /// The archive was deleted from storage.
    Deleted,

    /// The archive wasn't in storage.
    NotFound,
}
