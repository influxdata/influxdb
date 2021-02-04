//! This module contains the IOx implementation for using S3 as the object
//! store.
use crate::{
    path::{cloud::CloudPath, DELIMITER},
    ListResult, ObjectMeta, ObjectStoreApi,
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{
    stream::{self, BoxStream},
    Stream, StreamExt, TryStreamExt,
};
use rusoto_core::ByteStream;
use rusoto_credential::ChainProvider;
use rusoto_s3::S3;
use snafu::{futures::TryStreamExt as _, OptionExt, ResultExt, Snafu};
use std::convert::TryFrom;
use std::{fmt, io};

/// A specialized `Result` for object store-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Expected streamed data to have length {}, got {}", expected, actual))]
    DataDoesNotMatchLength { expected: usize, actual: usize },

    #[snafu(display("Did not receive any data. Bucket: {}, Location: {}", bucket, location))]
    NoData { bucket: String, location: String },

    #[snafu(display(
        "Unable to DELETE data. Bucket: {}, Location: {}, Error: {}",
        bucket,
        location,
        source,
    ))]
    UnableToDeleteData {
        source: rusoto_core::RusotoError<rusoto_s3::DeleteObjectError>,
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
        source: rusoto_core::RusotoError<rusoto_s3::GetObjectError>,
        bucket: String,
        location: String,
    },

    #[snafu(display(
        "Unable to GET part of the data. Bucket: {}, Location: {}, Error: {}",
        bucket,
        location,
        source,
    ))]
    UnableToGetPieceOfData {
        source: std::io::Error,
        bucket: String,
        location: String,
    },

    #[snafu(display(
        "Unable to PUT data. Bucket: {}, Location: {}, Error: {}",
        bucket,
        location,
        source,
    ))]
    UnableToPutData {
        source: rusoto_core::RusotoError<rusoto_s3::PutObjectError>,
        bucket: String,
        location: String,
    },

    #[snafu(display("Unable to list data. Bucket: {}, Error: {}", bucket, source))]
    UnableToListData {
        source: rusoto_core::RusotoError<rusoto_s3::ListObjectsV2Error>,
        bucket: String,
    },
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

#[async_trait]
impl ObjectStoreApi for AmazonS3 {
    type Path = CloudPath;
    type Error = Error;

    fn new_path(&self) -> Self::Path {
        CloudPath::default()
    }

    async fn put<S>(&self, location: &Self::Path, bytes: S, length: usize) -> Result<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        let bytes = ByteStream::new_with_size(bytes, length);

        let put_request = rusoto_s3::PutObjectRequest {
            bucket: self.bucket_name.clone(),
            key: location.to_raw(),
            body: Some(bytes),
            ..Default::default()
        };

        self.client
            .put_object(put_request)
            .await
            .context(UnableToPutData {
                bucket: &self.bucket_name,
                location: location.to_raw(),
            })?;
        Ok(())
    }

    async fn get(&self, location: &Self::Path) -> Result<BoxStream<'static, Result<Bytes>>> {
        let key = location.to_raw();
        let get_request = rusoto_s3::GetObjectRequest {
            bucket: self.bucket_name.clone(),
            key: key.clone(),
            ..Default::default()
        };
        Ok(self
            .client
            .get_object(get_request)
            .await
            .context(UnableToGetData {
                bucket: self.bucket_name.to_owned(),
                location: key.clone(),
            })?
            .body
            .context(NoData {
                bucket: self.bucket_name.to_owned(),
                location: key.clone(),
            })?
            .context(UnableToGetPieceOfData {
                bucket: self.bucket_name.to_owned(),
                location: key,
            })
            .err_into()
            .boxed())
    }

    async fn delete(&self, location: &Self::Path) -> Result<()> {
        let key = location.to_raw();
        let delete_request = rusoto_s3::DeleteObjectRequest {
            bucket: self.bucket_name.clone(),
            key: key.clone(),
            ..Default::default()
        };

        self.client
            .delete_object(delete_request)
            .await
            .context(UnableToDeleteData {
                bucket: self.bucket_name.to_owned(),
                location: key,
            })?;
        Ok(())
    }

    async fn list<'a>(
        &'a self,
        prefix: Option<&'a Self::Path>,
    ) -> Result<BoxStream<'a, Result<Vec<Self::Path>>>> {
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
                prefix: prefix.map(|p| p.to_raw()),
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
                Err(e) => {
                    return Some((
                        Err(Error::UnableToListData {
                            source: e,
                            bucket: self.bucket_name.clone(),
                        }),
                        state,
                    ))
                }
            };

            let contents = resp.contents.unwrap_or_default();
            let names = contents
                .into_iter()
                .flat_map(|object| object.key.map(CloudPath::raw))
                .collect();

            // The AWS response contains a field named `is_truncated` as well as
            // `next_continuation_token`, and we're assuming that `next_continuation_token`
            // is only set when `is_truncated` is true (and therefore not
            // checking `is_truncated`).
            let next_state = if let Some(next_continuation_token) = resp.next_continuation_token {
                ListState::HasMore(next_continuation_token)
            } else {
                ListState::Done
            };

            Some((Ok(names), next_state))
        })
        .boxed())
    }

    async fn list_with_delimiter(&self, prefix: &Self::Path) -> Result<ListResult<Self::Path>> {
        self.list_with_delimiter_and_token(prefix, &None).await
    }
}

impl AmazonS3 {
    /// Configure a connection to Amazon S3 in the specified Amazon region and
    /// bucket. Uses [`rusoto_credential::ChainProvider`][cp] to check for
    /// credentials in:
    ///
    /// 1. Environment variables: `AWS_ACCESS_KEY_ID` and
    ///    `AWS_SECRET_ACCESS_KEY`
    /// 2. `credential_process` command in the AWS config file, usually located
    ///    at `~/.aws/config`.
    /// 3. AWS credentials file. Usually located at `~/.aws/credentials`.
    /// 4. IAM instance profile. Will only work if running on an EC2 instance
    ///    with an instance profile/role.
    ///
    /// [cp]: https://docs.rs/rusoto_credential/0.43.0/rusoto_credential/struct.ChainProvider.html
    pub fn new(region: rusoto_core::Region, bucket_name: impl Into<String>) -> Self {
        let http_client = rusoto_core::request::HttpClient::new()
            .expect("Current implementation of rusoto_core has no way for this to fail");
        let credentials_provider = ChainProvider::new();
        Self {
            client: rusoto_s3::S3Client::new_with(http_client, credentials_provider, region),
            bucket_name: bucket_name.into(),
        }
    }

    /// List objects with the given prefix and a set delimiter of `/`. Returns
    /// common prefixes (directories) in addition to object metadata. Optionally
    /// takes a continuation token for paging.
    pub async fn list_with_delimiter_and_token<'a>(
        &'a self,
        prefix: &'a CloudPath,
        next_token: &Option<String>,
    ) -> Result<ListResult<CloudPath>> {
        let converted_prefix = prefix.to_raw();

        let mut list_request = rusoto_s3::ListObjectsV2Request {
            bucket: self.bucket_name.clone(),
            prefix: Some(converted_prefix),
            delimiter: Some(DELIMITER.to_string()),
            ..Default::default()
        };

        if let Some(t) = next_token {
            list_request.continuation_token = Some(t.clone());
        }

        let resp = match self.client.list_objects_v2(list_request).await {
            Ok(resp) => resp,
            Err(e) => {
                return Err(Error::UnableToListData {
                    source: e,
                    bucket: self.bucket_name.clone(),
                })
            }
        };

        let contents = resp.contents.unwrap_or_default();

        let objects: Vec<_> = contents
            .into_iter()
            .map(|object| {
                let location =
                    CloudPath::raw(object.key.expect("object doesn't exist without a key"));
                let last_modified = match object.last_modified {
                    Some(lm) => {
                        DateTime::parse_from_rfc3339(&lm)
                            .unwrap()
                            .with_timezone(&Utc)
                        // match dt {
                        //     Err(err) => return
                        // Err(Error::UnableToParseLastModifiedTime{value: lm,
                        // err})     Ok(dt) =>
                        // dt.with_timezone(&Utc), }
                    }
                    None => Utc::now(),
                };
                let size = usize::try_from(object.size.unwrap_or(0))
                    .expect("unsupported size on this platform");

                ObjectMeta {
                    location,
                    last_modified,
                    size,
                }
            })
            .collect();

        let common_prefixes = resp
            .common_prefixes
            .unwrap_or_default()
            .into_iter()
            .map(|p| CloudPath::raw(p.prefix.expect("can't have a prefix without a value")))
            .collect();

        let result = ListResult {
            objects,
            common_prefixes,
            next_token: resp.next_continuation_token,
        };

        Ok(result)
    }
}

impl Error {
    #[cfg(test)]
    fn s3_error_due_to_credentials(&self) -> bool {
        use rusoto_core::RusotoError;
        use Error::*;

        matches! (self,
             UnableToPutData {
                 source: RusotoError::Credentials(_),
                 bucket: _,
                 location: _,
             } |
             UnableToGetData {
                 source: RusotoError::Credentials(_),
                 bucket: _,
                 location: _,
             } |
             UnableToDeleteData {
                 source: RusotoError::Credentials(_),
                 bucket: _,
                 location: _,
             } |
             UnableToListData {
                 source: RusotoError::Credentials(_),
                 bucket: _,
             }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        tests::{get_nonexistent_object, list_with_delimiter, put_get_delete_list},
        AmazonS3, ObjectStoreApi, ObjectStorePath,
    };
    use bytes::Bytes;
    use std::env;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = TestError> = std::result::Result<T, E>;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    // Helper macro to skip tests if the AWS environment variables are not set.
    // Skips become hard errors if TEST_INTEGRATION is set.
    macro_rules! maybe_skip_integration {
        () => {
            dotenv::dotenv().ok();

            let region = env::var("AWS_DEFAULT_REGION");
            let bucket_name = env::var("AWS_S3_BUCKET_NAME");
            let force = std::env::var("TEST_INTEGRATION");

            match (region.is_ok(), bucket_name.is_ok(), force.is_ok()) {
                (false, false, true) => {
                    panic!(
                        "TEST_INTEGRATION is set, \
                            but AWS_DEFAULT_REGION and AWS_S3_BUCKET_NAME are not"
                    )
                }
                (false, true, true) => {
                    panic!("TEST_INTEGRATION is set, but AWS_DEFAULT_REGION is not")
                }
                (true, false, true) => {
                    panic!("TEST_INTEGRATION is set, but AWS_S3_BUCKET_NAME is not")
                }
                (false, false, false) => {
                    eprintln!(
                        "skipping integration test - set \
                               AWS_DEFAULT_REGION and AWS_S3_BUCKET_NAME to run"
                    );
                    return Ok(());
                }
                (false, true, false) => {
                    eprintln!("skipping integration test - set AWS_DEFAULT_REGION to run");
                    return Ok(());
                }
                (true, false, false) => {
                    eprintln!("skipping integration test - set AWS_S3_BUCKET_NAME to run");
                    return Ok(());
                }
                _ => {}
            }
        };
    }

    // Helper to get region and bucket from environment variables. Call the
    // `maybe_skip_integration!` macro before calling this to skip the test if these
    // aren't set; if you don't call that macro, the tests will fail if
    // these env vars aren't set.
    //
    // `AWS_DEFAULT_REGION` should be a value like `us-east-2`.
    fn region_and_bucket_name() -> Result<(rusoto_core::Region, String)> {
        let region = env::var("AWS_DEFAULT_REGION").map_err(|_| {
            "The environment variable AWS_DEFAULT_REGION must be set \
                 to a value like `us-east-2`"
        })?;
        let bucket_name = env::var("AWS_S3_BUCKET_NAME")
            .map_err(|_| "The environment variable AWS_S3_BUCKET_NAME must be set")?;

        Ok((region.parse()?, bucket_name))
    }

    fn check_credentials<T>(r: Result<T>) -> Result<T> {
        if let Err(e) = &r {
            let e = &**e;
            if let Some(e) = e.downcast_ref::<Error>() {
                if e.s3_error_due_to_credentials() {
                    eprintln!(
                        "Try setting the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY \
                               environment variables"
                    );
                }
            }
        }

        r
    }

    #[tokio::test]
    async fn s3_test() -> Result<()> {
        maybe_skip_integration!();
        let (region, bucket_name) = region_and_bucket_name()?;

        let integration = AmazonS3::new(region, &bucket_name);
        check_credentials(put_get_delete_list(&integration).await)?;

        check_credentials(list_with_delimiter(&integration).await).unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn s3_test_get_nonexistent_region() -> Result<()> {
        maybe_skip_integration!();
        // Assumes environment variables do not provide credentials to AWS US West 1
        let (_, bucket_name) = region_and_bucket_name()?;
        let region = rusoto_core::Region::UsWest1;
        let integration = AmazonS3::new(region, &bucket_name);
        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();
        if let Some(Error::UnableToListData { source, bucket }) = err.downcast_ref::<Error>() {
            assert!(matches!(source, rusoto_core::RusotoError::Unknown(_)));
            assert_eq!(bucket, &bucket_name);
        } else {
            panic!("unexpected error type")
        }

        Ok(())
    }

    #[tokio::test]
    async fn s3_test_get_nonexistent_location() -> Result<()> {
        maybe_skip_integration!();
        let (region, bucket_name) = region_and_bucket_name()?;
        let integration = AmazonS3::new(region, &bucket_name);
        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();
        if let Some(Error::UnableToGetData {
            source,
            bucket,
            location,
        }) = err.downcast_ref::<Error>()
        {
            assert!(matches!(
                source,
                rusoto_core::RusotoError::Service(rusoto_s3::GetObjectError::NoSuchKey(_))
            ));
            assert_eq!(bucket, &bucket_name);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type")
        }

        Ok(())
    }

    #[tokio::test]
    async fn s3_test_get_nonexistent_bucket() -> Result<()> {
        maybe_skip_integration!();
        let (region, _) = region_and_bucket_name()?;
        let bucket_name = NON_EXISTENT_NAME;
        let integration = AmazonS3::new(region, bucket_name);
        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();
        if let Some(Error::UnableToListData { source, bucket }) = err.downcast_ref::<Error>() {
            assert!(matches!(
                source,
                rusoto_core::RusotoError::Service(rusoto_s3::ListObjectsV2Error::NoSuchBucket(_))
            ));
            assert_eq!(bucket, bucket_name);
        } else {
            panic!("unexpected error type")
        }

        Ok(())
    }

    #[tokio::test]
    async fn s3_test_put_nonexistent_region() -> Result<()> {
        maybe_skip_integration!();
        // Assumes environment variables do not provide credentials to AWS US West 1
        let (_, bucket_name) = region_and_bucket_name()?;
        let region = rusoto_core::Region::UsWest1;
        let integration = AmazonS3::new(region, &bucket_name);
        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);
        let data = Bytes::from("arbitrary data");
        let stream_data = std::io::Result::Ok(data.clone());

        let err = integration
            .put(
                &location,
                futures::stream::once(async move { stream_data }),
                data.len(),
            )
            .await
            .unwrap_err();

        if let Error::UnableToPutData {
            source,
            bucket,
            location,
        } = err
        {
            assert!(matches!(source, rusoto_core::RusotoError::Unknown(_)));
            assert_eq!(bucket, bucket_name);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type")
        }

        Ok(())
    }

    #[tokio::test]
    async fn s3_test_put_nonexistent_bucket() -> Result<()> {
        maybe_skip_integration!();
        let (region, _) = region_and_bucket_name()?;
        let bucket_name = NON_EXISTENT_NAME;
        let integration = AmazonS3::new(region, bucket_name);
        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);
        let data = Bytes::from("arbitrary data");
        let stream_data = std::io::Result::Ok(data.clone());

        let err = integration
            .put(
                &location,
                futures::stream::once(async move { stream_data }),
                data.len(),
            )
            .await
            .unwrap_err();

        if let Error::UnableToPutData {
            source,
            bucket,
            location,
        } = err
        {
            assert!(matches!(source, rusoto_core::RusotoError::Unknown(_)));
            assert_eq!(bucket, bucket_name);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type")
        }

        Ok(())
    }

    #[tokio::test]
    async fn s3_test_delete_nonexistent_location() -> Result<()> {
        maybe_skip_integration!();
        let (region, bucket_name) = region_and_bucket_name()?;
        let integration = AmazonS3::new(region, &bucket_name);
        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let result = integration.delete(&location).await;

        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn s3_test_delete_nonexistent_region() -> Result<()> {
        maybe_skip_integration!();
        // Assumes environment variables do not provide credentials to AWS US West 1
        let (_, bucket_name) = region_and_bucket_name()?;
        let region = rusoto_core::Region::UsWest1;
        let integration = AmazonS3::new(region, &bucket_name);
        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let err = integration.delete(&location).await.unwrap_err();
        if let Error::UnableToDeleteData {
            source,
            bucket,
            location,
        } = err
        {
            assert!(matches!(source, rusoto_core::RusotoError::Unknown(_)));
            assert_eq!(bucket, bucket_name);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type")
        }

        Ok(())
    }

    #[tokio::test]
    async fn s3_test_delete_nonexistent_bucket() -> Result<()> {
        maybe_skip_integration!();
        let (region, _) = region_and_bucket_name()?;
        let bucket_name = NON_EXISTENT_NAME;
        let integration = AmazonS3::new(region, bucket_name);
        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let err = integration.delete(&location).await.unwrap_err();
        if let Error::UnableToDeleteData {
            source,
            bucket,
            location,
        } = err
        {
            assert!(matches!(source, rusoto_core::RusotoError::Unknown(_)));
            assert_eq!(bucket, bucket_name);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type")
        }

        Ok(())
    }
}
