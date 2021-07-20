//! This module contains the IOx implementation for using S3 as the object
//! store.
use crate::{
    buffer::slurp_stream_tempfile,
    cache::{Cache, LocalFSCache},
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
use rusoto_credential::{InstanceMetadataProvider, StaticProvider};
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

    #[snafu(display(
        "Unable to parse last modified date. Bucket: {}, Error: {}",
        bucket,
        source
    ))]
    UnableToParseLastModified {
        source: chrono::ParseError,
        bucket: String,
    },

    #[snafu(display("Unable to buffer data into temporary file, Error: {}", source))]
    UnableToBufferStream { source: std::io::Error },

    #[snafu(display(
        "Could not parse `{}` as an AWS region. Regions should look like `us-east-2`. {:?}",
        region,
        source
    ))]
    InvalidRegion {
        region: String,
        source: rusoto_core::region::ParseRegionError,
    },

    #[snafu(display("Missing aws-access-key"))]
    MissingAccessKey,

    #[snafu(display("Missing aws-secret-access-key"))]
    MissingSecretAccessKey,
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

    async fn put<S>(&self, location: &Self::Path, bytes: S, length: Option<usize>) -> Result<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        let bytes = match length {
            Some(length) => ByteStream::new_with_size(bytes, length),
            None => {
                let bytes = slurp_stream_tempfile(bytes)
                    .await
                    .context(UnableToBufferStream)?;
                let length = bytes.size();
                ByteStream::new_with_size(bytes, length)
            }
        };

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

            let resp = self
                .client
                .list_objects_v2(list_request)
                .await
                .context(UnableToListData {
                    bucket: &self.bucket_name,
                });
            let resp = match resp {
                Ok(resp) => resp,
                Err(e) => return Some((Err(e), state)),
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

    fn cache(&self) -> Option<&dyn Cache> {
        todo!()
    }
}

/// Configure a connection to Amazon S3 using the specified credentials in
/// the specified Amazon region and bucket.
///
/// Note do not expose the AmazonS3::new() function to allow it to be
/// swapped out when the aws feature is not enabled
pub(crate) fn new_s3(
    access_key_id: Option<impl Into<String>>,
    secret_access_key: Option<impl Into<String>>,
    region: impl Into<String>,
    bucket_name: impl Into<String>,
    endpoint: Option<impl Into<String>>,
    session_token: Option<impl Into<String>>,
) -> Result<AmazonS3> {
    let region = region.into();
    let region: rusoto_core::Region = match endpoint {
        None => region.parse().context(InvalidRegion { region })?,
        Some(endpoint) => rusoto_core::Region::Custom {
            name: region,
            endpoint: endpoint.into(),
        },
    };

    let http_client = rusoto_core::request::HttpClient::new()
        .expect("Current implementation of rusoto_core has no way for this to fail");

    let client = match (access_key_id, secret_access_key, session_token) {
        (Some(access_key_id), Some(secret_access_key), Some(session_token)) => {
            let credentials_provider = StaticProvider::new(
                access_key_id.into(),
                secret_access_key.into(),
                Some(session_token.into()),
                None,
            );
            rusoto_s3::S3Client::new_with(http_client, credentials_provider, region)
        }
        (Some(access_key_id), Some(secret_access_key), None) => {
            let credentials_provider =
                StaticProvider::new_minimal(access_key_id.into(), secret_access_key.into());
            rusoto_s3::S3Client::new_with(http_client, credentials_provider, region)
        }
        (None, Some(_), _) => return Err(Error::MissingAccessKey),
        (Some(_), None, _) => return Err(Error::MissingSecretAccessKey),
        _ => {
            let credentials_provider = InstanceMetadataProvider::new();
            rusoto_s3::S3Client::new_with(http_client, credentials_provider, region)
        }
    };

    Ok(AmazonS3 {
        client,
        bucket_name: bucket_name.into(),
    })
}

pub(crate) fn new_failing_s3() -> Result<AmazonS3> {
    new_s3(
        Some("foo"),
        Some("bar"),
        "us-east-1",
        "bucket",
        None as Option<&str>,
        None as Option<&str>,
    )
}

impl AmazonS3 {
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

        let resp = self
            .client
            .list_objects_v2(list_request)
            .await
            .context(UnableToListData {
                bucket: &self.bucket_name,
            })?;

        let contents = resp.contents.unwrap_or_default();

        let objects = contents
            .into_iter()
            .map(|object| {
                let location =
                    CloudPath::raw(object.key.expect("object doesn't exist without a key"));
                let last_modified = match object.last_modified {
                    Some(lm) => DateTime::parse_from_rfc3339(&lm)
                        .context(UnableToParseLastModified {
                            bucket: &self.bucket_name,
                        })?
                        .with_timezone(&Utc),
                    None => Utc::now(),
                };
                let size = usize::try_from(object.size.unwrap_or(0))
                    .expect("unsupported size on this platform");

                Ok(ObjectMeta {
                    location,
                    last_modified,
                    size,
                })
            })
            .collect::<Result<Vec<_>>>()?;

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

        matches!(
            self,
            UnableToPutData {
                source: RusotoError::Credentials(_),
                bucket: _,
                location: _,
            } | UnableToGetData {
                source: RusotoError::Credentials(_),
                bucket: _,
                location: _,
            } | UnableToDeleteData {
                source: RusotoError::Credentials(_),
                bucket: _,
                location: _,
            } | UnableToListData {
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
        Error as ObjectStoreError, ObjectStore, ObjectStoreApi, ObjectStorePath,
    };
    use bytes::Bytes;
    use std::env;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = TestError> = std::result::Result<T, E>;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[derive(Debug)]
    struct AwsConfig {
        access_key_id: String,
        secret_access_key: String,
        region: String,
        bucket: String,
        endpoint: Option<String>,
        token: Option<String>,
    }

    // Helper macro to skip tests if TEST_INTEGRATION and the AWS environment variables are not set.
    macro_rules! maybe_skip_integration {
        () => {{
            dotenv::dotenv().ok();

            let required_vars = [
                "AWS_DEFAULT_REGION",
                "INFLUXDB_IOX_BUCKET",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
            ];
            let unset_vars: Vec<_> = required_vars
                .iter()
                .filter_map(|&name| match env::var(name) {
                    Ok(_) => None,
                    Err(_) => Some(name),
                })
                .collect();
            let unset_var_names = unset_vars.join(", ");

            let force = env::var("TEST_INTEGRATION");

            if force.is_ok() && !unset_var_names.is_empty() {
                panic!(
                    "TEST_INTEGRATION is set, \
                            but variable(s) {} need to be set",
                    unset_var_names
                );
            } else if force.is_err() {
                eprintln!(
                    "skipping AWS integration test - set {}TEST_INTEGRATION to run",
                    if unset_var_names.is_empty() {
                        String::new()
                    } else {
                        format!("{} and ", unset_var_names)
                    }
                );
                return;
            } else {
                AwsConfig {
                    access_key_id: env::var("AWS_ACCESS_KEY_ID")
                        .expect("already checked AWS_ACCESS_KEY_ID"),
                    secret_access_key: env::var("AWS_SECRET_ACCESS_KEY")
                        .expect("already checked AWS_SECRET_ACCESS_KEY"),
                    region: env::var("AWS_DEFAULT_REGION")
                        .expect("already checked AWS_DEFAULT_REGION"),
                    bucket: env::var("INFLUXDB_IOX_BUCKET")
                        .expect("already checked INFLUXDB_IOX_BUCKET"),
                    endpoint: env::var("AWS_ENDPOINT").ok(),
                    token: env::var("AWS_SESSION_TOKEN").ok(),
                }
            }
        }};
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
    async fn s3_test() {
        let config = maybe_skip_integration!();
        let integration = ObjectStore::new_amazon_s3(
            Some(config.access_key_id),
            Some(config.secret_access_key),
            config.region,
            config.bucket,
            config.endpoint,
            config.token,
        )
        .expect("Valid S3 config");

        check_credentials(put_get_delete_list(&integration).await).unwrap();
        check_credentials(list_with_delimiter(&integration).await).unwrap();
    }

    #[tokio::test]
    async fn s3_test_get_nonexistent_region() {
        let mut config = maybe_skip_integration!();
        // Assumes environment variables do not provide credentials to AWS US West 1
        config.region = "us-west-1".into();

        let integration = ObjectStore::new_amazon_s3(
            Some(config.access_key_id),
            Some(config.secret_access_key),
            config.region,
            &config.bucket,
            config.endpoint,
            config.token,
        )
        .expect("Valid S3 config");

        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();
        if let Some(ObjectStoreError::AwsObjectStoreError {
            source: Error::UnableToListData { source, bucket },
        }) = err.downcast_ref::<ObjectStoreError>()
        {
            assert!(matches!(source, rusoto_core::RusotoError::Unknown(_)));
            assert_eq!(bucket, &config.bucket);
        } else {
            panic!("unexpected error type: {:?}", err);
        }
    }

    #[tokio::test]
    async fn s3_test_get_nonexistent_location() {
        let config = maybe_skip_integration!();
        let integration = ObjectStore::new_amazon_s3(
            Some(config.access_key_id),
            Some(config.secret_access_key),
            config.region,
            &config.bucket,
            config.endpoint,
            config.token,
        )
        .expect("Valid S3 config");

        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();
        if let Some(ObjectStoreError::AwsObjectStoreError {
            source:
                Error::UnableToGetData {
                    source,
                    bucket,
                    location,
                },
        }) = err.downcast_ref::<ObjectStoreError>()
        {
            assert!(matches!(
                source,
                rusoto_core::RusotoError::Service(rusoto_s3::GetObjectError::NoSuchKey(_))
            ));
            assert_eq!(bucket, &config.bucket);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type: {:?}", err);
        }
    }

    #[tokio::test]
    async fn s3_test_get_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();

        let integration = ObjectStore::new_amazon_s3(
            Some(config.access_key_id),
            Some(config.secret_access_key),
            config.region,
            &config.bucket,
            config.endpoint,
            config.token,
        )
        .expect("Valid S3 config");

        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();
        if let Some(ObjectStoreError::AwsObjectStoreError {
            source: Error::UnableToListData { source, bucket },
        }) = err.downcast_ref::<ObjectStoreError>()
        {
            assert!(matches!(
                source,
                rusoto_core::RusotoError::Service(rusoto_s3::ListObjectsV2Error::NoSuchBucket(_))
            ));
            assert_eq!(bucket, &config.bucket);
        } else {
            panic!("unexpected error type: {:?}", err);
        }
    }

    #[tokio::test]
    async fn s3_test_put_nonexistent_region() {
        let mut config = maybe_skip_integration!();
        // Assumes environment variables do not provide credentials to AWS US West 1
        config.region = "us-west-1".into();

        let integration = ObjectStore::new_amazon_s3(
            Some(config.access_key_id),
            Some(config.secret_access_key),
            config.region,
            &config.bucket,
            config.endpoint,
            config.token,
        )
        .expect("Valid S3 config");

        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);
        let data = Bytes::from("arbitrary data");
        let stream_data = std::io::Result::Ok(data.clone());

        let err = integration
            .put(
                &location,
                futures::stream::once(async move { stream_data }),
                Some(data.len()),
            )
            .await
            .unwrap_err();

        if let ObjectStoreError::AwsObjectStoreError {
            source:
                Error::UnableToPutData {
                    source,
                    bucket,
                    location,
                },
        } = err
        {
            assert!(matches!(source, rusoto_core::RusotoError::Unknown(_)));
            assert_eq!(bucket, config.bucket);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type: {:?}", err);
        }
    }

    #[tokio::test]
    async fn s3_test_put_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();

        let integration = ObjectStore::new_amazon_s3(
            Some(config.access_key_id),
            Some(config.secret_access_key),
            config.region,
            &config.bucket,
            config.endpoint,
            config.token,
        )
        .expect("Valid S3 config");

        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);
        let data = Bytes::from("arbitrary data");
        let stream_data = std::io::Result::Ok(data.clone());

        let err = integration
            .put(
                &location,
                futures::stream::once(async move { stream_data }),
                Some(data.len()),
            )
            .await
            .unwrap_err();

        if let ObjectStoreError::AwsObjectStoreError {
            source:
                Error::UnableToPutData {
                    source,
                    bucket,
                    location,
                },
        } = err
        {
            assert!(matches!(source, rusoto_core::RusotoError::Unknown(_)));
            assert_eq!(bucket, config.bucket);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type: {:?}", err);
        }
    }

    #[tokio::test]
    async fn s3_test_delete_nonexistent_location() {
        let config = maybe_skip_integration!();
        let integration = ObjectStore::new_amazon_s3(
            Some(config.access_key_id),
            Some(config.secret_access_key),
            config.region,
            config.bucket,
            config.endpoint,
            config.token,
        )
        .expect("Valid S3 config");

        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let result = integration.delete(&location).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn s3_test_delete_nonexistent_region() {
        let mut config = maybe_skip_integration!();
        // Assumes environment variables do not provide credentials to AWS US West 1
        config.region = "us-west-1".into();

        let integration = ObjectStore::new_amazon_s3(
            Some(config.access_key_id),
            Some(config.secret_access_key),
            config.region,
            &config.bucket,
            config.endpoint,
            config.token,
        )
        .expect("Valid S3 config");

        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let err = integration.delete(&location).await.unwrap_err();
        if let ObjectStoreError::AwsObjectStoreError {
            source:
                Error::UnableToDeleteData {
                    source,
                    bucket,
                    location,
                },
        } = err
        {
            assert!(matches!(source, rusoto_core::RusotoError::Unknown(_)));
            assert_eq!(bucket, config.bucket);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type: {:?}", err);
        }
    }

    #[tokio::test]
    async fn s3_test_delete_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();

        let integration = ObjectStore::new_amazon_s3(
            Some(config.access_key_id),
            Some(config.secret_access_key),
            config.region,
            &config.bucket,
            config.endpoint,
            config.token,
        )
        .expect("Valid S3 config");

        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let err = integration.delete(&location).await.unwrap_err();
        if let ObjectStoreError::AwsObjectStoreError {
            source:
                Error::UnableToDeleteData {
                    source,
                    bucket,
                    location,
                },
        } = err
        {
            assert!(matches!(source, rusoto_core::RusotoError::Unknown(_)));
            assert_eq!(bucket, config.bucket);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type: {:?}", err);
        }
    }
}
