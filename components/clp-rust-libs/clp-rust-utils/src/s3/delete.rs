//! Deletion of S3 objects.

use aws_sdk_s3::Client;
use aws_sdk_s3::types::Delete;
use aws_sdk_s3::types::ObjectIdentifier;
use non_empty_string::NonEmptyString;

use crate::Error;

/// Deletes every object in `bucket` whose key starts with `key_prefix`.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`Error::S3`] if
///   [`aws_sdk_s3::operation::list_objects_v2::paginator::ListObjectsV2Paginator::send`]'s stream
///   yields an error.
/// * Forwards [`delete_by_keys`]'s return values on failure.
pub async fn delete_by_key_prefix(
    client: &Client,
    bucket: &str,
    key_prefix: &NonEmptyString,
) -> Result<(), Error> {
    let mut pages = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(key_prefix.as_str())
        .into_paginator()
        .send();
    while let Some(page) = pages.next().await {
        let keys = page
            .map_err(|e| Error::S3(Box::new(e.into())))?
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|object| object.key)
            .collect();
        delete_by_keys(client, bucket, keys).await?;
    }
    Ok(())
}

/// Deletes the objects in `bucket` whose keys are in `keys`.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`Error::S3`] if:
///   * [`aws_sdk_s3::types::builders::ObjectIdentifierBuilder::build`] fails.
///   * [`aws_sdk_s3::types::builders::DeleteBuilder::build`] fails.
///   * [`aws_sdk_s3::operation::delete_objects::builders::DeleteObjectsFluentBuilder::send`] fails.
/// * [`Error::S3ObjectsNotDeleted`] if S3 fails to delete any of the objects.
pub async fn delete_by_keys(client: &Client, bucket: &str, keys: Vec<String>) -> Result<(), Error> {
    const MAX_NUM_KEYS_PER_REQUEST: usize = 1000;

    let mut keys = keys.into_iter().peekable();
    while keys.peek().is_some() {
        let objects = keys
            .by_ref()
            .take(MAX_NUM_KEYS_PER_REQUEST)
            .map(|key| ObjectIdentifier::builder().key(key).build())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| Error::S3(Box::new(e.into())))?;
        let delete = Delete::builder()
            .set_objects(Some(objects))
            .quiet(true)
            .build()
            .map_err(|e| Error::S3(Box::new(e.into())))?;
        let output = client
            .delete_objects()
            .bucket(bucket)
            .delete(delete)
            .send()
            .await
            .map_err(|e| Error::S3(Box::new(e.into())))?;
        if let Some(errors) = output.errors
            && !errors.is_empty()
        {
            return Err(Error::S3ObjectsNotDeleted(errors));
        }
    }
    Ok(())
}
