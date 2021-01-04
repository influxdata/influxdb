//! This module contains the IOx implementation for using S3 as the object
//! store.
use crate::{
    path::{CloudConverter, ObjectStorePath, DELIMITER},
    Error, ListResult, NoDataFromS3, ObjectMeta, Result, UnableToDeleteDataFromS3,
    UnableToGetDataFromS3, UnableToGetPieceOfDataFromS3, UnableToPutDataToS3,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{stream, Stream, TryStreamExt};
use rusoto_core::ByteStream;
use rusoto_credential::ChainProvider;
use rusoto_s3::S3;
use snafu::{futures::TryStreamExt as _, OptionExt, ResultExt};
use std::convert::TryFrom;
use std::{fmt, io};

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

    /// Save the provided bytes to the specified location.
    pub async fn put<S>(&self, location: &ObjectStorePath, bytes: S, length: usize) -> Result<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        let bytes = ByteStream::new_with_size(bytes, length);

        let put_request = rusoto_s3::PutObjectRequest {
            bucket: self.bucket_name.clone(),
            key: CloudConverter::convert(&location),
            body: Some(bytes),
            ..Default::default()
        };

        self.client
            .put_object(put_request)
            .await
            .context(UnableToPutDataToS3 {
                bucket: &self.bucket_name,
                location: CloudConverter::convert(&location),
            })?;
        Ok(())
    }

    /// Return the bytes that are stored at the specified location.
    pub async fn get(
        &self,
        location: &ObjectStorePath,
    ) -> Result<impl Stream<Item = Result<Bytes>>> {
        let key = CloudConverter::convert(&location);
        let get_request = rusoto_s3::GetObjectRequest {
            bucket: self.bucket_name.clone(),
            key: key.clone(),
            ..Default::default()
        };
        Ok(self
            .client
            .get_object(get_request)
            .await
            .context(UnableToGetDataFromS3 {
                bucket: self.bucket_name.to_owned(),
                location: key.clone(),
            })?
            .body
            .context(NoDataFromS3 {
                bucket: self.bucket_name.to_owned(),
                location: key.clone(),
            })?
            .context(UnableToGetPieceOfDataFromS3 {
                bucket: self.bucket_name.to_owned(),
                location: key,
            })
            .err_into())
    }

    /// Delete the object at the specified location.
    pub async fn delete(&self, location: &ObjectStorePath) -> Result<()> {
        let key = CloudConverter::convert(&location);
        let delete_request = rusoto_s3::DeleteObjectRequest {
            bucket: self.bucket_name.clone(),
            key: key.clone(),
            ..Default::default()
        };

        self.client
            .delete_object(delete_request)
            .await
            .context(UnableToDeleteDataFromS3 {
                bucket: self.bucket_name.to_owned(),
                location: key,
            })?;
        Ok(())
    }

    /// List all the objects with the given prefix.
    pub async fn list<'a>(
        &'a self,
        prefix: Option<&'a ObjectStorePath>,
    ) -> Result<impl Stream<Item = Result<Vec<ObjectStorePath>>> + 'a> {
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
                prefix: prefix.map(CloudConverter::convert),
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
                        Err(Error::UnableToListDataFromS3 {
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
                .flat_map(|object| object.key.map(ObjectStorePath::from_cloud_unchecked))
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
        }))
    }

    /// List objects with the given prefix and a set delimiter of `/`. Returns
    /// common prefixes (directories) in addition to object metadata.
    pub async fn list_with_delimiter<'a>(
        &'a self,
        prefix: &'a ObjectStorePath,
        next_token: &Option<String>,
    ) -> Result<ListResult> {
        dbg!(&prefix);
        let converted_prefix = CloudConverter::convert(prefix);
        dbg!(&converted_prefix);
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
                return Err(Error::UnableToListDataFromS3 {
                    source: e,
                    bucket: self.bucket_name.clone(),
                })
            }
        };

        let contents = resp.contents.unwrap_or_default();
        dbg!(&contents);
        let objects: Vec<_> = contents
            .into_iter()
            .map(|object| {
                let location = ObjectStorePath::from_cloud_unchecked(
                    object.key.expect("object doesn't exist without a key"),
                );
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

        dbg!(&resp.common_prefixes);

        let common_prefixes = resp
            .common_prefixes
            .unwrap_or_default()
            .into_iter()
            .map(|p| {
                ObjectStorePath::from_cloud_unchecked(
                    p.prefix.expect("can't have a prefix without a value"),
                )
            })
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
        use crate::Error::*;
        use rusoto_core::RusotoError;

        matches! (self,
             UnableToPutDataToS3 {
                 source: RusotoError::Credentials(_),
                 bucket: _,
                 location: _,
             } |
             UnableToGetDataFromS3 {
                 source: RusotoError::Credentials(_),
                 bucket: _,
                 location: _,
             } |
             UnableToDeleteDataFromS3 {
                 source: RusotoError::Credentials(_),
                 bucket: _,
                 location: _,
             } |
             UnableToListDataFromS3 {
                 source: RusotoError::Credentials(_),
                 bucket: _,
             }
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        path::ObjectStorePath,
        tests::{get_nonexistent_object, list_with_delimiter, put_get_delete_list},
        AmazonS3, Error, ObjectStore,
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
            if let Some(e) = e.downcast_ref::<crate::Error>() {
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

        let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, &bucket_name));
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
        let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, &bucket_name));
        let location_name = ObjectStorePath::from_cloud_unchecked(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&integration, Some(location_name))
            .await
            .unwrap_err();
        if let Some(Error::UnableToListDataFromS3 { source, bucket }) =
            err.downcast_ref::<crate::Error>()
        {
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
        let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, &bucket_name));
        let location_name = ObjectStorePath::from_cloud_unchecked(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&integration, Some(location_name))
            .await
            .unwrap_err();
        if let Some(Error::UnableToGetDataFromS3 {
            source,
            bucket,
            location,
        }) = err.downcast_ref::<crate::Error>()
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
        let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, bucket_name));
        let location_name = ObjectStorePath::from_cloud_unchecked(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&integration, Some(location_name))
            .await
            .unwrap_err();
        if let Some(Error::UnableToListDataFromS3 { source, bucket }) =
            err.downcast_ref::<crate::Error>()
        {
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
        let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, &bucket_name));
        let location_name = ObjectStorePath::from_cloud_unchecked(NON_EXISTENT_NAME);
        let data = Bytes::from("arbitrary data");
        let stream_data = std::io::Result::Ok(data.clone());

        let err = integration
            .put(
                &location_name,
                futures::stream::once(async move { stream_data }),
                data.len(),
            )
            .await
            .unwrap_err();

        if let Error::UnableToPutDataToS3 {
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
        let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, bucket_name));
        let location_name = ObjectStorePath::from_cloud_unchecked(NON_EXISTENT_NAME);
        let data = Bytes::from("arbitrary data");
        let stream_data = std::io::Result::Ok(data.clone());

        let err = integration
            .put(
                &location_name,
                futures::stream::once(async move { stream_data }),
                data.len(),
            )
            .await
            .unwrap_err();

        if let Error::UnableToPutDataToS3 {
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
        let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, &bucket_name));
        let location_name = ObjectStorePath::from_cloud_unchecked(NON_EXISTENT_NAME);

        let result = integration.delete(&location_name).await;

        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn s3_test_delete_nonexistent_region() -> Result<()> {
        maybe_skip_integration!();
        // Assumes environment variables do not provide credentials to AWS US West 1
        let (_, bucket_name) = region_and_bucket_name()?;
        let region = rusoto_core::Region::UsWest1;
        let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, &bucket_name));
        let location_name = ObjectStorePath::from_cloud_unchecked(NON_EXISTENT_NAME);

        let err = integration.delete(&location_name).await.unwrap_err();
        if let Error::UnableToDeleteDataFromS3 {
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
        let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, bucket_name));
        let location_name = ObjectStorePath::from_cloud_unchecked(NON_EXISTENT_NAME);

        let err = integration.delete(&location_name).await.unwrap_err();
        if let Error::UnableToDeleteDataFromS3 {
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
