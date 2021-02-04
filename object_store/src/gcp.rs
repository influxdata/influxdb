//! This module contains the IOx implementation for using Google Cloud Storage
//! as the object store.
use crate::{path::cloud::CloudPath, ListResult, ObjectStoreApi};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    stream::{self, BoxStream},
    Stream, StreamExt, TryStreamExt,
};
use snafu::{ensure, futures::TryStreamExt as _, ResultExt, Snafu};
use std::io;

/// A specialized `Result` for Google Cloud Storage object store-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A specialized `Error` for Google Cloud Storage object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Expected streamed data to have length {}, got {}", expected, actual))]
    DataDoesNotMatchLength { expected: usize, actual: usize },

    #[snafu(display(
        "Unable to PUT data. Bucket: {}, Location: {}, Error: {}",
        bucket,
        location,
        source
    ))]
    UnableToPutData {
        source: cloud_storage::Error,
        bucket: String,
        location: String,
    },

    #[snafu(display("Unable to list data. Bucket: {}, Error: {}", bucket, source,))]
    UnableToListData {
        source: cloud_storage::Error,
        bucket: String,
    },

    #[snafu(display("Unable to stream list data. Bucket: {}, Error: {}", bucket, source,))]
    UnableToStreamListData {
        source: cloud_storage::Error,
        bucket: String,
    },

    #[snafu(display(
        "Unable to DELETE data. Bucket: {}, Location: {}, Error: {}",
        bucket,
        location,
        source,
    ))]
    UnableToDeleteData {
        source: cloud_storage::Error,
        bucket: String,
        location: String,
    },

    #[snafu(display(
        "Unable to GET data. Bucket: {}, Location: {}, Error: {}",
        bucket,
        location,
        source,
    ))]
    UnableToGetData {
        source: cloud_storage::Error,
        bucket: String,
        location: String,
    },
}

/// Configuration for connecting to [Google Cloud Storage](https://cloud.google.com/storage/).
#[derive(Debug)]
pub struct GoogleCloudStorage {
    bucket_name: String,
}

#[async_trait]
impl ObjectStoreApi for GoogleCloudStorage {
    type Path = CloudPath;
    type Error = Error;

    fn new_path(&self) -> Self::Path {
        CloudPath::default()
    }

    async fn put<S>(&self, location: &Self::Path, bytes: S, length: usize) -> Result<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        let temporary_non_streaming = bytes
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .expect("Should have been able to collect streaming data")
            .to_vec();

        ensure!(
            temporary_non_streaming.len() == length,
            DataDoesNotMatchLength {
                actual: temporary_non_streaming.len(),
                expected: length,
            }
        );

        let location = location.to_raw();
        let location_copy = location.clone();
        let bucket_name = self.bucket_name.clone();

        cloud_storage::Object::create(
            &bucket_name,
            temporary_non_streaming,
            &location_copy,
            "application/octet-stream",
        )
        .await
        .context(UnableToPutData {
            bucket: &self.bucket_name,
            location,
        })?;

        Ok(())
    }

    async fn get(&self, location: &Self::Path) -> Result<BoxStream<'static, Result<Bytes>>> {
        let location = location.to_raw();
        let location_copy = location.clone();
        let bucket_name = self.bucket_name.clone();

        let bytes = cloud_storage::Object::download(&bucket_name, &location_copy)
            .await
            .context(UnableToGetData {
                bucket: &self.bucket_name,
                location,
            })?;

        Ok(futures::stream::once(async move { Ok(bytes.into()) }).boxed())
    }

    async fn delete(&self, location: &Self::Path) -> Result<()> {
        let location = location.to_raw();
        let location_copy = location.clone();
        let bucket_name = self.bucket_name.clone();

        cloud_storage::Object::delete(&bucket_name, &location_copy)
            .await
            .context(UnableToDeleteData {
                bucket: &self.bucket_name,
                location: location.clone(),
            })?;

        Ok(())
    }

    async fn list<'a>(
        &'a self,
        prefix: Option<&'a Self::Path>,
    ) -> Result<BoxStream<'a, Result<Vec<Self::Path>>>> {
        let objects = match prefix {
            Some(prefix) => {
                let cloud_prefix = prefix.to_raw();
                let list = cloud_storage::Object::list_prefix(&self.bucket_name, &cloud_prefix)
                    .await
                    .context(UnableToListData {
                        bucket: &self.bucket_name,
                    })?;

                // TODO: Remove collect when the path no longer needs
                // to be converted into an owned object that would be
                // dropped too early.
                stream::iter(list.collect::<Vec<_>>().await).left_stream()
            }
            None => cloud_storage::Object::list(&self.bucket_name)
                .await
                .context(UnableToListData {
                    bucket: &self.bucket_name,
                })?
                .right_stream(),
        };

        let objects = objects
            .map_ok(|list| {
                list.into_iter()
                    .map(|o| CloudPath::raw(o.name))
                    .collect::<Vec<_>>()
            })
            .context(UnableToStreamListData {
                bucket: &self.bucket_name,
            });

        Ok(objects.boxed())
    }

    async fn list_with_delimiter(&self, _prefix: &Self::Path) -> Result<ListResult<Self::Path>> {
        unimplemented!();
    }
}

impl GoogleCloudStorage {
    /// Configure a connection to Google Cloud Storage.
    pub fn new(bucket_name: impl Into<String>) -> Self {
        Self {
            bucket_name: bucket_name.into(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        tests::{get_nonexistent_object, put_get_delete_list},
        GoogleCloudStorage, ObjectStoreApi, ObjectStorePath,
    };
    use bytes::Bytes;
    use std::env;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = TestError> = std::result::Result<T, E>;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    // Helper macro to skip tests if the GCP environment variables are not set.
    // Skips become hard errors if TEST_INTEGRATION is set.
    macro_rules! maybe_skip_integration {
        () => {
            dotenv::dotenv().ok();

            let bucket_name = env::var("GCS_BUCKET_NAME");
            let force = std::env::var("TEST_INTEGRATION");

            match (bucket_name.is_ok(), force.is_ok()) {
                (false, true) => {
                    panic!("TEST_INTEGRATION is set, but GCS_BUCKET_NAME is not")
                }
                (false, false) => {
                    eprintln!("skipping integration test - set GCS_BUCKET_NAME to run");
                    return Ok(());
                }
                _ => {}
            }
        };
    }

    fn bucket_name() -> Result<String> {
        Ok(env::var("GCS_BUCKET_NAME")
            .map_err(|_| "The environment variable GCS_BUCKET_NAME must be set")?)
    }

    #[tokio::test]
    async fn gcs_test() -> Result<()> {
        maybe_skip_integration!();
        let bucket_name = bucket_name()?;

        let integration = GoogleCloudStorage::new(&bucket_name);
        put_get_delete_list(&integration).await?;
        Ok(())
    }

    #[tokio::test]
    async fn gcs_test_get_nonexistent_location() -> Result<()> {
        maybe_skip_integration!();
        let bucket_name = bucket_name()?;
        let integration = GoogleCloudStorage::new(&bucket_name);

        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let result = get_nonexistent_object(&integration, Some(location)).await?;

        assert_eq!(
            result,
            Bytes::from(format!(
                "No such object: {}/{}",
                bucket_name, NON_EXISTENT_NAME
            ))
        );

        Ok(())
    }

    #[tokio::test]
    async fn gcs_test_get_nonexistent_bucket() -> Result<()> {
        maybe_skip_integration!();
        let bucket_name = NON_EXISTENT_NAME;
        let integration = GoogleCloudStorage::new(bucket_name);
        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let result = get_nonexistent_object(&integration, Some(location)).await?;

        assert_eq!(result, Bytes::from("Not Found"));

        Ok(())
    }

    #[tokio::test]
    async fn gcs_test_delete_nonexistent_location() -> Result<()> {
        maybe_skip_integration!();
        let bucket_name = bucket_name()?;
        let integration = GoogleCloudStorage::new(&bucket_name);

        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let err = integration.delete(&location).await.unwrap_err();

        if let Error::UnableToDeleteData {
            source,
            bucket,
            location,
        } = err
        {
            assert!(matches!(source, cloud_storage::Error::Google(_)));
            assert_eq!(bucket, bucket_name);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type")
        }

        Ok(())
    }

    #[tokio::test]
    async fn gcs_test_delete_nonexistent_bucket() -> Result<()> {
        maybe_skip_integration!();
        let bucket_name = NON_EXISTENT_NAME;
        let integration = GoogleCloudStorage::new(bucket_name);

        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let err = integration.delete(&location).await.unwrap_err();

        if let Error::UnableToDeleteData {
            source,
            bucket,
            location,
        } = err
        {
            assert!(matches!(source, cloud_storage::Error::Google(_)));
            assert_eq!(bucket, bucket_name);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type")
        }

        Ok(())
    }

    #[tokio::test]
    async fn gcs_test_put_nonexistent_bucket() -> Result<()> {
        maybe_skip_integration!();
        let bucket_name = NON_EXISTENT_NAME;
        let integration = GoogleCloudStorage::new(bucket_name);
        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let data = Bytes::from("arbitrary data");
        let stream_data = std::io::Result::Ok(data.clone());

        let result = integration
            .put(
                &location,
                futures::stream::once(async move { stream_data }),
                data.len(),
            )
            .await;
        assert!(result.is_ok());

        Ok(())
    }
}
