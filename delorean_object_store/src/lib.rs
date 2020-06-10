#![deny(rust_2018_idioms)]
#![warn(missing_docs)]

//! # delorean_object_store
//!
//! This crate provides APIs for interacting with object storage services. It currently supports
//! PUT, GET, DELETE, and list for Google Cloud Storage, Amazon S3, and in-memory storage.
//!
//! Future compatibility will include Azure Blob Storage, Minio, and Ceph.

use bytes::Bytes;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use rusoto_credential::ChainProvider;
use rusoto_s3::S3;
use snafu::{futures::TryStreamExt as _, OptionExt, ResultExt, Snafu};
use std::fmt;

/// Universal interface to multiple object store services.
#[derive(Debug)]
pub struct ObjectStore(ObjectStoreIntegration);

impl ObjectStore {
    /// Configure a connection to Amazon S3.
    pub fn new_amazon_s3(s3: AmazonS3) -> Self {
        ObjectStore(ObjectStoreIntegration::AmazonS3(s3))
    }

    /// Configure a connection to Google Cloud Storage.
    pub fn new_google_cloud_storage(gcs: GoogleCloudStorage) -> Self {
        ObjectStore(ObjectStoreIntegration::GoogleCloudStorage(gcs))
    }

    /// Save the provided bytes to the specified location.
    pub async fn put<S>(&self, location: &str, bytes: S) -> Result<()>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
    {
        match &self.0 {
            ObjectStoreIntegration::AmazonS3(s3) => s3.put(location, bytes).await?,
            ObjectStoreIntegration::GoogleCloudStorage(gcs) => gcs.put(location, bytes).await?,
        }

        Ok(())
    }

    /// Return the bytes that are stored at the specified location.
    pub async fn get(&self, location: &str) -> Result<impl Stream<Item = Result<Bytes>>> {
        Ok(match &self.0 {
            ObjectStoreIntegration::AmazonS3(s3) => s3.get(location).await?.boxed(),
            ObjectStoreIntegration::GoogleCloudStorage(gcs) => gcs.get(location).await?.boxed(),
        })
    }

    /// Delete the object at the specified location.
    pub async fn delete(&self, location: &str) -> Result<()> {
        match &self.0 {
            ObjectStoreIntegration::AmazonS3(s3) => s3.delete(location).await?,
            ObjectStoreIntegration::GoogleCloudStorage(gcs) => gcs.delete(location).await?,
        }

        Ok(())
    }

    /// List all the objects with the given prefix.
    pub async fn list<'a>(
        &'a self,
        prefix: Option<&'a str>,
    ) -> Result<impl Stream<Item = Result<Vec<String>>> + 'a> {
        Ok(match &self.0 {
            ObjectStoreIntegration::AmazonS3(s3) => s3.list(prefix).await?.err_into().boxed(),
            ObjectStoreIntegration::GoogleCloudStorage(gcs) => {
                gcs.list(prefix).await?.err_into().boxed()
            }
        })
    }
}

/// All supported object storage integrations
#[derive(Debug)]
enum ObjectStoreIntegration {
    GoogleCloudStorage(GoogleCloudStorage),
    AmazonS3(AmazonS3),
}

/// Configuration for connecting to [Google Cloud Storage](https://cloud.google.com/storage/).
#[derive(Debug)]
pub struct GoogleCloudStorage {
    bucket_name: String,
}

impl GoogleCloudStorage {
    /// Configure a connection to Google Cloud Storage.
    pub fn new(bucket_name: impl Into<String>) -> Self {
        GoogleCloudStorage {
            bucket_name: bucket_name.into(),
        }
    }

    /// Save the provided bytes to the specified location.
    async fn put<S>(&self, location: &str, bytes: S) -> InternalResult<()>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
    {
        let temporary_non_streaming = bytes
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .expect("Should have been able to collect streaming data")
            .to_vec();

        let location = location.to_string();
        let bucket_name = self.bucket_name.clone();

        let _ = tokio::task::spawn_blocking(move || {
            cloud_storage::Object::create(
                &bucket_name,
                &temporary_non_streaming,
                &location,
                "application/octet-stream",
            )
        })
        .await
        .context(UnableToPutDataToGcs)?;

        Ok(())
    }

    async fn get(&self, location: &str) -> InternalResult<impl Stream<Item = Result<Bytes>>> {
        let location = location.to_string();
        let bucket_name = self.bucket_name.clone();

        let bytes = tokio::task::spawn_blocking(move || {
            cloud_storage::Object::download(&bucket_name, &location)
        })
        .await
        .context(UnableToGetDataFromGcs)?
        .context(UnableToGetDataFromGcs2)?;

        Ok(futures::stream::once(async move { Ok(bytes.into()) }))
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &str) -> InternalResult<()> {
        let location = location.to_string();
        let bucket_name = self.bucket_name.clone();

        tokio::task::spawn_blocking(move || cloud_storage::Object::delete(&bucket_name, &location))
            .await
            .context(UnableToDeleteDataFromGcs)?
            .context(UnableToDeleteDataFromGcs2)?;

        Ok(())
    }

    /// List all the objects with the given prefix.
    async fn list<'a>(
        &'a self,
        prefix: Option<&'a str>,
    ) -> InternalResult<impl Stream<Item = InternalResult<Vec<String>>> + 'a> {
        Ok(stream::empty())
    }
}

/// Configuration for connecting to [Amazon S3](https://aws.amazon.com/s3/).
pub struct AmazonS3 {
    client: rusoto_s3::S3Client,
    bucket_name: String,
}

impl fmt::Debug for AmazonS3 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AmazonS3")
            .field("client", &"rusoto_s3::S3Client")
            .field("bucket_name", &self.bucket_name)
            .finish()
    }
}

impl AmazonS3 {
    /// Configure a connection to Amazon S3 in the specified Amazon region and bucket. Uses
    /// [`rusoto_credential::ChainProvider`](cp) to check for credentials in:
    ///
    /// 1. Environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
    /// 2. `credential_process` command in the AWS config file, usually located at `~/.aws/config`.
    /// 3. AWS credentials file. Usually located at `~/.aws/credentials`.
    /// 4. IAM instance profile. Will only work if running on an EC2 instance with an instance
    ///    profile/role.
    ///
    /// [cp]: https://docs.rs/rusoto_credential/0.43.0/rusoto_credential/struct.ChainProvider.html
    pub fn new(region: rusoto_core::Region, bucket_name: impl Into<String>) -> Self {
        let http_client = rusoto_core::request::HttpClient::new()
            .expect("Current implementation of rusoto_core has no way for this to fail");
        let credentials_provider = ChainProvider::new();
        AmazonS3 {
            client: rusoto_s3::S3Client::new_with(http_client, credentials_provider, region),
            bucket_name: bucket_name.into(),
        }
    }

    /// Save the provided bytes to the specified location.
    async fn put<S>(&self, location: &str, bytes: S) -> InternalResult<()>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
    {
        // Rusoto theoretically supports streaming, but won't actually until this is fixed:
        // https://github.com/rusoto/rusoto/issues/1752
        let temporary_non_streaming = bytes
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .expect("Should have been able to collect streaming data")
            .to_vec();

        let put_request = rusoto_s3::PutObjectRequest {
            bucket: self.bucket_name.clone(),
            key: location.to_string(),
            body: Some(temporary_non_streaming.into()),
            ..Default::default()
        };

        self.client.put_object(put_request).await?;
        Ok(())
    }

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &str) -> InternalResult<impl Stream<Item = Result<Bytes>>> {
        let get_request = rusoto_s3::GetObjectRequest {
            bucket: self.bucket_name.clone(),
            key: location.to_string(),
            ..Default::default()
        };
        Ok(self
            .client
            .get_object(get_request)
            .await?
            .body
            .context(NoDataFromS3)?
            .context(UnableToGetPieceOfDataFromS3)
            .err_into())
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &str) -> InternalResult<()> {
        let delete_request = rusoto_s3::DeleteObjectRequest {
            bucket: self.bucket_name.clone(),
            key: location.to_string(),
            ..Default::default()
        };

        self.client.delete_object(delete_request).await?;
        Ok(())
    }

    /// List all the objects with the given prefix.
    async fn list<'a>(
        &'a self,
        prefix: Option<&'a str>,
    ) -> InternalResult<impl Stream<Item = InternalResult<Vec<String>>> + 'a> {
        #[derive(Clone)]
        enum ListState {
            Start,
            HasMore(String),
            Done,
        }
        use ListState::*;

        Ok(stream::unfold(ListState::Start, move |state| async move {
            let mut list_request = rusoto_s3::ListObjectsV2Request {
                bucket: self.bucket_name.clone(),
                prefix: prefix.map(ToString::to_string),
                ..Default::default()
            };

            match state.clone() {
                HasMore(continuation_token) => {
                    list_request.continuation_token = Some(continuation_token);
                }
                Done => {
                    return None;
                }
                // If this is the first request we've made, we don't need to make any modifications
                // to the request
                Start => {}
            }

            let resp = match self.client.list_objects_v2(list_request).await {
                Ok(resp) => resp,
                Err(e) => return Some((Err(e.into()), state)),
            };

            let contents = resp.contents.unwrap_or_default();
            let names = contents.into_iter().flat_map(|object| object.key).collect();

            // The AWS response contains a field named `is_truncated` as well as
            // `next_continuation_token`, and we're assuming that `next_continuation_token` is only
            // set when `is_truncated` is true (and therefore not checking `is_truncated`).
            let next_state = if let Some(next_continuation_token) = resp.next_continuation_token {
                ListState::HasMore(next_continuation_token)
            } else {
                ListState::Done
            };

            Some((Ok(names), next_state))
        }))
    }
}

/// A specialized `Result` for object store-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;
type InternalResult<T, E = InternalError> = std::result::Result<T, E>;

/// Opaque public `Error` type
#[derive(Debug, Snafu)]
pub struct Error(InternalError);

impl Error {
    #[cfg(test)]
    fn s3_error_due_to_credentials(&self) -> bool {
        use rusoto_core::RusotoError;
        use InternalError::*;

        match self.0 {
            UnableToPutDataToS3 {
                source: RusotoError::Credentials(_),
            } => true,
            UnableToGetDataFromS3 {
                source: RusotoError::Credentials(_),
            } => true,
            UnableToDeleteDataFromS3 {
                source: RusotoError::Credentials(_),
            } => true,
            UnableToListDataFromS3 {
                source: RusotoError::Credentials(_),
            } => true,
            _ => false,
        }
    }
}

#[derive(Debug, Snafu)]
enum InternalError {
    UnableToPutDataToGcs {
        source: tokio::task::JoinError,
    },
    UnableToListDataFromGcs {
        source: tokio::task::JoinError,
    },
    UnableToListDataFromGcs2 {
        source: cloud_storage::Error,
    },
    UnableToDeleteDataFromGcs {
        source: tokio::task::JoinError,
    },
    UnableToDeleteDataFromGcs2 {
        source: cloud_storage::Error,
    },
    UnableToGetDataFromGcs {
        source: tokio::task::JoinError,
    },
    UnableToGetDataFromGcs2 {
        source: cloud_storage::Error,
    },

    #[snafu(context(false))]
    UnableToPutDataToS3 {
        source: rusoto_core::RusotoError<rusoto_s3::PutObjectError>,
    },
    #[snafu(context(false))]
    UnableToGetDataFromS3 {
        source: rusoto_core::RusotoError<rusoto_s3::GetObjectError>,
    },
    #[snafu(context(false))]
    UnableToDeleteDataFromS3 {
        source: rusoto_core::RusotoError<rusoto_s3::DeleteObjectError>,
    },
    NoDataFromS3,
    UnableToReadBytesFromS3 {
        source: std::io::Error,
    },
    UnableToGetPieceOfDataFromS3 {
        source: std::io::Error,
    },

    #[snafu(context(false))]
    UnableToListDataFromS3 {
        source: rusoto_core::RusotoError<rusoto_s3::ListObjectsV2Error>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    async fn flatten_list_stream(
        storage: &ObjectStore,
        prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        storage
            .list(prefix)
            .await?
            .map_ok(|v| stream::iter(v).map(Ok))
            .try_flatten()
            .try_collect()
            .await
    }

    async fn put_get_delete_list(storage: &ObjectStore) -> Result<()> {
        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());

        let data = Bytes::from("arbitrary data");
        let location = "test_file";

        let stream_data = std::io::Result::Ok(data.clone());
        storage
            .put(location, futures::stream::once(async move { stream_data }))
            .await?;

        // List everything
        let content_list = flatten_list_stream(storage, None).await?;
        assert_eq!(content_list, &[location]);

        // List everything starting with a prefix that should return results
        let content_list = flatten_list_stream(storage, Some("test")).await?;
        assert_eq!(content_list, &[location]);

        // List everything starting with a prefix that shouldn't return results
        let content_list = flatten_list_stream(storage, Some("something")).await?;
        assert!(content_list.is_empty());

        let read_data = storage
            .get(location)
            .await?
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await?;
        assert_eq!(&*read_data, data);

        storage.delete(location).await?;

        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());

        Ok(())
    }

    // Tests TODO:
    // GET nonexisting location
    // DELETE nonexisting location
    // PUT overwriting

    mod google_cloud_storage {
        use std::env;

        use super::*;

        fn bucket_name() -> Result<String> {
            dotenv::dotenv().ok();
            let bucket_name = env::var("GCS_BUCKET_NAME")
                .map_err(|_| "The environment variable GCS_BUCKET_NAME must be set")?;

            Ok(bucket_name)
        }

        #[tokio::test]
        async fn gcs_test() -> Result<()> {
            let bucket_name = bucket_name()?;

            let integration =
                ObjectStore::new_google_cloud_storage(GoogleCloudStorage::new(&bucket_name));
            put_get_delete_list(&integration).await?;
            Ok(())
        }
    }

    mod amazon_s3 {
        use std::env;

        use super::*;

        #[tokio::test]
        async fn s3_test() -> Result<()> {
            let (region, bucket_name) = region_and_bucket_name()?;

            let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, &bucket_name));
            check_credentials(put_get_delete_list(&integration).await)?;

            Ok(())
        }

        fn region_and_bucket_name() -> Result<(rusoto_core::Region, String)> {
            dotenv::dotenv().ok();

            let region = env::var("AWS_DEFAULT_REGION")
                .map_err(|_| "The environment variable AWS_DEFAULT_REGION must be set to a value like `us-east-2`")?;
            let bucket_name = env::var("AWS_S3_BUCKET_NAME")
                .map_err(|_| "The environment variable AWS_S3_BUCKET_NAME must be set")?;

            Ok((region.parse()?, bucket_name))
        }

        fn check_credentials<T>(r: Result<T>) -> Result<T> {
            if let Err(e) = &r {
                let e = &**e;
                if let Some(e) = e.downcast_ref::<crate::Error>() {
                    if e.s3_error_due_to_credentials() {
                        eprintln!("Try setting the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables");
                    }
                }
            }

            r
        }
    }
}
