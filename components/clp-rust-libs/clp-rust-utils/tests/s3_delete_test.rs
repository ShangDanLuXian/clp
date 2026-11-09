use std::sync::Arc;
use std::sync::Mutex;

use anyhow::Result;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::ConfigBag;
use aws_sdk_s3::config::Intercept;
use aws_sdk_s3::config::RuntimeComponents;
use aws_sdk_s3::config::interceptors::BeforeDeserializationInterceptorContextMut;
use aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef;
use aws_sdk_s3::error::BoxError;
use aws_sdk_s3::primitives::SdkBody;
use clp_rust_utils::Error;
use clp_rust_utils::clp_config::AwsAuthentication;
use clp_rust_utils::clp_config::AwsCredentials;
use clp_rust_utils::s3::delete_by_key_prefix;
use clp_rust_utils::s3::delete_by_keys;
use clp_rust_utils::types::non_empty_string::ExpectedNonEmpty;
use futures::StreamExt;
use futures::TryStreamExt;
use non_empty_string::NonEmptyString;

#[tokio::test]
#[ignore = "Requires LocalStack or AWS environment"]
async fn delete_by_key_prefix_deletes_every_page_and_keeps_other_objects() -> Result<()> {
    const NUM_OBJECTS_UNDER_PREFIX: usize = 1050;
    const KEY_PREFIX: &str = "dataset/";

    let bucket = TestBucket::create().await;
    let objects_outside_prefix = vec!["dataset-sibling/object".to_owned(), "object".to_owned()];
    bucket
        .put_objects(
            (0..NUM_OBJECTS_UNDER_PREFIX)
                .map(|idx| format!("{KEY_PREFIX}{idx:05}"))
                .chain(objects_outside_prefix.iter().cloned()),
        )
        .await;

    delete_by_key_prefix(
        &bucket.client,
        &bucket.name,
        &NonEmptyString::from_static_str(KEY_PREFIX),
    )
    .await?;

    assert_eq!(bucket.list_keys().await, objects_outside_prefix);
    Ok(())
}

#[tokio::test]
#[ignore = "Requires LocalStack or AWS environment"]
async fn delete_by_keys_deletes_keys_in_batches_of_1000() -> Result<()> {
    const MAX_NUM_KEYS_PER_REQUEST: usize = 1000;
    const NUM_OBJECTS_TO_DELETE: usize = 1050;

    let bucket = TestBucket::create().await;
    let keys_to_delete: Vec<_> = (0..NUM_OBJECTS_TO_DELETE)
        .map(|idx| format!("deleted/{idx:05}"))
        .collect();
    let kept_keys = vec!["kept/object".to_owned()];
    bucket
        .put_objects(keys_to_delete.iter().chain(&kept_keys).cloned())
        .await;
    let recorder = DeleteRequestRecorder::default();
    let recording_client = Client::from_conf(
        bucket
            .client
            .config()
            .to_builder()
            .interceptor(recorder.clone())
            .build(),
    );

    delete_by_keys(&recording_client, &bucket.name, keys_to_delete).await?;

    assert_eq!(bucket.list_keys().await, kept_keys);
    assert_eq!(
        recorder.num_objects_per_request(),
        vec![
            MAX_NUM_KEYS_PER_REQUEST,
            NUM_OBJECTS_TO_DELETE - MAX_NUM_KEYS_PER_REQUEST
        ]
    );
    Ok(())
}

#[tokio::test]
#[ignore = "Requires LocalStack or AWS environment"]
async fn deleting_missing_keys_and_prefixes_succeeds() -> Result<()> {
    let bucket = TestBucket::create().await;
    let kept_keys = vec!["kept/object".to_owned()];
    bucket.put_objects(kept_keys.iter().cloned()).await;

    delete_by_keys(
        &bucket.client,
        &bucket.name,
        vec!["missing/object".to_owned()],
    )
    .await?;
    delete_by_keys(&bucket.client, &bucket.name, Vec::new()).await?;
    delete_by_key_prefix(
        &bucket.client,
        &bucket.name,
        &NonEmptyString::from_static_str("missing/"),
    )
    .await?;

    assert_eq!(bucket.list_keys().await, kept_keys);
    Ok(())
}

#[tokio::test]
#[ignore = "Requires LocalStack or AWS environment"]
async fn delete_by_keys_returns_per_object_errors() -> Result<()> {
    const KEY: &str = "object";

    let bucket = TestBucket::create().await;
    let failing_client = Client::from_conf(
        bucket
            .client
            .config()
            .to_builder()
            .interceptor(DeleteErrorInjector { key: KEY })
            .build(),
    );

    let result = delete_by_keys(&failing_client, &bucket.name, vec![KEY.to_owned()]).await;

    let Err(Error::S3ObjectsNotDeleted(errors)) = result else {
        panic!("expected `Error::S3ObjectsNotDeleted`, got {result:?}");
    };
    assert_eq!(
        errors
            .iter()
            .map(aws_sdk_s3::types::Error::key)
            .collect::<Vec<_>>(),
        vec![Some(KEY)]
    );
    Ok(())
}

/// A uniquely named bucket created for a single test.
struct TestBucket {
    client: Client,
    name: String,
}

impl TestBucket {
    /// Creates a uniquely named bucket on the S3 endpoint configured by the following environment
    /// variables:
    ///
    /// * `AWS_ENDPOINT_URL`: Defaults to `http://127.0.0.1:4566` if not set.
    /// * `AWS_ACCESS_KEY_ID`: Defaults to `test` if not set.
    /// * `AWS_SECRET_ACCESS_KEY`: Defaults to `test` if not set.
    /// * `AWS_REGION`: Defaults to `us-east-1` if not set.
    ///
    /// # Returns
    ///
    /// The created bucket.
    ///
    /// # Panics
    ///
    /// Panics if the bucket can't be created.
    async fn create() -> Self {
        let env_var_or =
            |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_owned());
        let endpoint =
            NonEmptyString::from_string(env_var_or("AWS_ENDPOINT_URL", "http://127.0.0.1:4566"));
        let aws_authentication = AwsAuthentication::Credentials {
            credentials: AwsCredentials {
                access_key_id: env_var_or("AWS_ACCESS_KEY_ID", "test"),
                secret_access_key: env_var_or("AWS_SECRET_ACCESS_KEY", "test"),
                session_token: None,
            },
        };
        let client = clp_rust_utils::s3::create_new_client(
            env_var_or("AWS_REGION", "us-east-1").as_str(),
            Some(&endpoint),
            &aws_authentication,
        )
        .await;

        let name = format!("clp-s3-delete-test-{}", uuid::Uuid::new_v4().simple());
        client
            .create_bucket()
            .bucket(&name)
            .send()
            .await
            .expect("failed to create the test bucket");
        Self { client, name }
    }

    /// Creates an object for each of the given keys.
    ///
    /// # Panics
    ///
    /// Panics if an object can't be created.
    async fn put_objects(&self, keys: impl Iterator<Item = String>) {
        const MAX_NUM_CONCURRENT_REQUESTS: usize = 16;

        futures::stream::iter(keys)
            .map(|key| {
                self.client
                    .put_object()
                    .bucket(&self.name)
                    .key(key)
                    .body(Vec::from(*b"content").into())
                    .send()
            })
            .buffer_unordered(MAX_NUM_CONCURRENT_REQUESTS)
            .try_collect::<Vec<_>>()
            .await
            .expect("failed to create the test objects");
    }

    /// # Returns
    ///
    /// The keys of every object in the bucket, in lexicographic order.
    ///
    /// # Panics
    ///
    /// Panics if the objects can't be listed.
    async fn list_keys(&self) -> Vec<String> {
        self.client
            .list_objects_v2()
            .bucket(&self.name)
            .into_paginator()
            .send()
            .try_collect()
            .await
            .expect("failed to list the test objects")
            .into_iter()
            .flat_map(|page| page.contents.unwrap_or_default())
            .filter_map(|object| object.key)
            .collect()
    }
}

/// An interceptor that records the number of objects in every `DeleteObjects` request.
#[derive(Clone, Debug, Default)]
struct DeleteRequestRecorder {
    num_objects_per_request: Arc<Mutex<Vec<usize>>>,
}

impl DeleteRequestRecorder {
    /// # Returns
    ///
    /// The number of objects in each recorded request, in request order.
    ///
    /// # Panics
    ///
    /// Panics if the recorder lock is poisoned.
    fn num_objects_per_request(&self) -> Vec<usize> {
        self.num_objects_per_request
            .lock()
            .expect("recorder lock must not be poisoned")
            .clone()
    }
}

impl Intercept for DeleteRequestRecorder {
    fn name(&self) -> &'static str {
        "DeleteRequestRecorder"
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let body = context
            .request()
            .body()
            .bytes()
            .ok_or("the `DeleteObjects` request body must be in memory")?;
        let num_objects = std::str::from_utf8(body)?.matches("<Object>").count();
        self.num_objects_per_request
            .lock()
            .expect("recorder lock must not be poisoned")
            .push(num_objects);
        Ok(())
    }
}

/// An interceptor that replaces every response body with a `DeleteObjects` result that reports a
/// failure to delete `key`.
#[derive(Debug)]
struct DeleteErrorInjector {
    key: &'static str,
}

impl Intercept for DeleteErrorInjector {
    fn name(&self) -> &'static str {
        "DeleteErrorInjector"
    }

    fn modify_before_deserialization(
        &self,
        context: &mut BeforeDeserializationInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        *context.response_mut().body_mut() = SdkBody::from(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult \
             xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Error><Key>{}</Key>\
             <Code>AccessDenied</Code><Message>Access Denied</Message></Error></DeleteResult>",
            self.key
        ));
        Ok(())
    }
}
