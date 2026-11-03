//! Deletion of S3 objects.

use aws_sdk_s3::Client;
use aws_sdk_s3::error::DisplayErrorContext;
use aws_sdk_s3::types::Delete;
use aws_sdk_s3::types::ObjectIdentifier;
use non_empty_string::NonEmptyString;

/// An error returned when deleting S3 objects.
#[derive(Debug, thiserror::Error)]
pub enum DeleteError {
    #[error("`aws_sdk_s3::Error`: {}", DisplayErrorContext(.0))]
    Sdk(Box<aws_sdk_s3::Error>),

    #[error("failed to delete S3 object: {0:?}")]
    ObjectNotDeleted(Box<aws_sdk_s3::types::Error>),
}

/// Deletes every object in `bucket` whose key starts with `key_prefix`.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`DeleteError::Sdk`] if
///   [`aws_sdk_s3::operation::list_objects_v2::paginator::ListObjectsV2Paginator::send`]'s stream
///   yields an error.
/// * Forwards [`delete_objects`]'s return values on failure.
pub async fn delete_by_key_prefix(
    client: &Client,
    bucket: &str,
    key_prefix: &NonEmptyString,
) -> Result<(), DeleteError> {
    let mut pages = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(key_prefix.as_str())
        .max_keys(i32::from(MAX_NUM_KEYS_PER_REQUEST))
        .into_paginator()
        .send();
    while let Some(page) = pages.next().await {
        let page = page.map_err(|e| DeleteError::Sdk(Box::new(e.into())))?;
        let keys: Vec<&str> = page
            .contents()
            .iter()
            .filter_map(|object| object.key())
            .collect();
        if keys.is_empty() {
            continue;
        }

        delete_objects(client, bucket, &keys).await?;
    }

    Ok(())
}

/// Deletes every object in `bucket` whose key is in `keys`.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`delete_objects`]'s return values on failure.
pub async fn delete_by_keys(
    client: &Client,
    bucket: &str,
    keys: &[NonEmptyString],
) -> Result<(), DeleteError> {
    for keys in keys.chunks(usize::from(MAX_NUM_KEYS_PER_REQUEST)) {
        let keys: Vec<&str> = keys.iter().map(NonEmptyString::as_str).collect();
        delete_objects(client, bucket, &keys).await?;
    }

    Ok(())
}

/// The maximum number of keys that a `ListObjectsV2` or `DeleteObjects` request handles.
const MAX_NUM_KEYS_PER_REQUEST: u16 = 1000;

/// Deletes the objects in `bucket` whose keys are in `keys`, in one request.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`DeleteError::Sdk`] if:
///   * [`aws_sdk_s3::types::builders::ObjectIdentifierBuilder::build`] fails.
///   * [`aws_sdk_s3::types::builders::DeleteBuilder::build`] fails.
///   * [`aws_sdk_s3::operation::delete_objects::builders::DeleteObjectsFluentBuilder::send`] fails.
/// * [`DeleteError::ObjectNotDeleted`] if S3 fails to delete an object.
async fn delete_objects(client: &Client, bucket: &str, keys: &[&str]) -> Result<(), DeleteError> {
    let objects = keys
        .iter()
        .map(|key| ObjectIdentifier::builder().key(*key).build())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| DeleteError::Sdk(Box::new(e.into())))?;
    let delete = Delete::builder()
        .set_objects(Some(objects))
        .quiet(true)
        .build()
        .map_err(|e| DeleteError::Sdk(Box::new(e.into())))?;
    let output = client
        .delete_objects()
        .bucket(bucket)
        .delete(delete)
        .send()
        .await
        .map_err(|e| DeleteError::Sdk(Box::new(e.into())))?;
    if let Some(error) = output.errors().first() {
        return Err(DeleteError::ObjectNotDeleted(Box::new(error.clone())));
    }

    Ok(())
}
