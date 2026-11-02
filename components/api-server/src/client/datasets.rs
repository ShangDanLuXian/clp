//! Management of the clp-s storage engine's datasets.

use std::io::ErrorKind;

use clp_rust_utils::aws::AWS_DEFAULT_REGION;
use clp_rust_utils::clp_config::S3Config;
use clp_rust_utils::clp_config::package::config::ArchiveOutputStorage;
use clp_rust_utils::clp_config::package::config::StorageEngine;
use clp_rust_utils::database::mysql::dataset;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::s3::DeleteError;
use clp_rust_utils::types::non_empty_string::ExpectedNonEmpty;
use non_empty_string::NonEmptyString;

use crate::client::Client;
use crate::error::ClientError;

impl Client {
    /// # Returns
    ///
    /// The names of the datasets that aren't deleted, ordered by name, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::check_clp_s_storage_engine`]'s return values on failure.
    /// * Forwards [`dataset::get_names`]'s return values on failure.
    pub async fn get_dataset_names(&self) -> Result<Vec<DatasetName>, ClientError> {
        self.check_clp_s_storage_engine("dataset listing")?;
        Ok(dataset::get_names(&self.sql_pool, &self.config.database).await?)
    }

    /// Deletes a dataset's archives from the archive storage, and then marks the dataset as
    /// deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::DatasetNotFound`] if the dataset doesn't exist or is deleted.
    /// * Forwards [`Self::check_clp_s_storage_engine`]'s return values on failure.
    /// * Forwards [`dataset::get_by_name`]'s return values on failure.
    /// * Forwards [`delete_archives_from_fs`]'s return values on failure.
    /// * Forwards [`delete_archives_from_s3`]'s return values on failure.
    /// * Forwards [`dataset::mark_deleted`]'s return values on failure.
    pub async fn delete_dataset(&self, name: &DatasetName) -> Result<(), ClientError> {
        self.check_clp_s_storage_engine("dataset deletion")?;
        let dataset = dataset::get_by_name(&self.sql_pool, &self.config.database, name)
            .await?
            .ok_or_else(|| ClientError::DatasetNotFound(name.clone()))?;
        match &self.config.archive_output.storage {
            ArchiveOutputStorage::Fs { .. } => {
                delete_archives_from_fs(&dataset.archive_storage_path).await?;
            }
            ArchiveOutputStorage::S3 { s3_config, .. } => {
                delete_archives_from_s3(s3_config, &dataset.archive_storage_path).await?;
            }
        }
        dataset::mark_deleted(&self.sql_pool, &self.config.database, dataset.id).await?;
        Ok(())
    }

    /// Checks that the package's storage engine is clp-s.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::InvalidInput`] if the package's storage engine isn't clp-s.
    fn check_clp_s_storage_engine(&self, operation: &str) -> Result<(), ClientError> {
        if self.config.package.storage_engine != StorageEngine::ClpS {
            return Err(ClientError::InvalidInput(format!(
                "{operation} requires the clp-s storage engine"
            )));
        }
        Ok(())
    }
}

/// Deletes the directory that `archive_storage_path` resolves to, if it's a directory.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`tokio::fs::metadata`]'s return values on failure, unless nothing exists at
///   `archive_storage_path`.
/// * Forwards [`tokio::fs::canonicalize`]'s return values on failure.
/// * Forwards [`tokio::fs::remove_dir_all`]'s return values on failure.
async fn delete_archives_from_fs(archive_storage_path: &str) -> std::io::Result<()> {
    let metadata = match tokio::fs::metadata(archive_storage_path).await {
        Ok(metadata) => metadata,
        Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    if !metadata.is_dir() {
        return Ok(());
    }
    tokio::fs::remove_dir_all(tokio::fs::canonicalize(archive_storage_path).await?).await
}

/// Deletes every object under `archive_storage_path` in `s3_config`'s bucket.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`clp_rust_utils::s3::delete_by_key_prefix`]'s return values on failure.
async fn delete_archives_from_s3(
    s3_config: &S3Config,
    archive_storage_path: &str,
) -> Result<(), DeleteError> {
    let client = clp_rust_utils::s3::create_new_client(
        s3_config
            .region_code
            .as_ref()
            .map_or(AWS_DEFAULT_REGION, NonEmptyString::as_str),
        s3_config.endpoint_url.as_ref(),
        &s3_config.aws_authentication,
    )
    .await;
    clp_rust_utils::s3::delete_by_key_prefix(
        &client,
        s3_config.bucket.as_str(),
        &archive_storage_key_prefix(archive_storage_path),
    )
    .await
}

/// # Returns
///
/// `archive_storage_path` with a trailing `/`.
fn archive_storage_key_prefix(archive_storage_path: &str) -> NonEmptyString {
    if archive_storage_path.ends_with('/') {
        NonEmptyString::from_string(archive_storage_path.to_owned())
    } else {
        NonEmptyString::from_string(format!("{archive_storage_path}/"))
    }
}

#[cfg(test)]
mod tests {
    use clp_rust_utils::clp_config::package::config::Config;
    use clp_rust_utils::clp_config::package::config::Package;
    use clp_rust_utils::clp_config::package::config::StorageEngine;

    use super::archive_storage_key_prefix;
    use super::delete_archives_from_fs;
    use crate::client::Client;

    #[tokio::test]
    async fn fs_archive_deletion_only_deletes_a_directory_or_a_symlinked_one() -> anyhow::Result<()>
    {
        let root = tempfile::tempdir()?;
        let directory = root.path().join("directory");
        std::fs::create_dir_all(directory.join("archive"))?;
        std::fs::write(directory.join("archive").join("segment"), b"data")?;
        let symlinked_directory = root.path().join("symlinked_directory");
        std::fs::create_dir(&symlinked_directory)?;
        let symlink = root.path().join("symlink");
        std::os::unix::fs::symlink(&symlinked_directory, &symlink)?;
        let file = root.path().join("file");
        std::fs::write(&file, b"data")?;

        for path in [&directory, &symlink] {
            delete_archives_from_fs(path.to_str().expect("the path should be UTF-8")).await?;
        }
        assert!(!directory.exists());
        assert!(!symlinked_directory.exists());

        for path in [&file, &file.join("child"), &root.path().join("missing")] {
            delete_archives_from_fs(path.to_str().expect("the path should be UTF-8")).await?;
        }
        assert!(file.is_file());
        Ok(())
    }

    #[test]
    fn archive_storage_key_prefix_appends_a_slash_only_if_missing() {
        assert_eq!(
            archive_storage_key_prefix("prefix/dataset").as_str(),
            "prefix/dataset/"
        );
        assert_eq!(
            archive_storage_key_prefix("prefix/dataset/").as_str(),
            "prefix/dataset/"
        );
    }

    #[tokio::test]
    async fn dataset_management_is_rejected_under_the_clp_storage_engine() -> anyhow::Result<()> {
        let config = Config {
            package: Package {
                storage_engine: StorageEngine::Clp,
            },
            ..Config::default()
        };
        let sql_pool =
            sqlx::mysql::MySqlPoolOptions::new().connect_lazy("mysql://127.0.0.1:1/clp-db")?;
        let mongodb_client =
            mongodb::Client::with_uri_str("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=100")
                .await?;
        let client = Client::new(config, mongodb_client, sql_pool);

        assert_eq!(
            client
                .get_dataset_names()
                .await
                .expect_err("the listing should be rejected")
                .to_string(),
            "Invalid input: dataset listing requires the clp-s storage engine"
        );
        assert_eq!(
            client
                .delete_dataset(&"logs".parse()?)
                .await
                .expect_err("the deletion should be rejected")
                .to_string(),
            "Invalid input: dataset deletion requires the clp-s storage engine"
        );
        Ok(())
    }
}
