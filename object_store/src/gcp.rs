//! This module contains the IOx implementation for using Google Cloud Storage
//! as the object store.
use crate::{
    path::{cloud::CloudPath, DELIMITER},
    ListResult, ObjectMeta, ObjectStoreApi,
};
use async_trait::async_trait;
use bytes::Bytes;
use cloud_storage::Client;
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use snafu::{ensure, ResultExt, Snafu};
use std::{convert::TryFrom, env, io};

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
    client: Client,
    bucket_name: String,
}

#[async_trait]
impl ObjectStoreApi for GoogleCloudStorage {
    type Path = CloudPath;
    type Error = Error;

    fn new_path(&self) -> Self::Path {
        CloudPath::default()
    }

    async fn put<S>(&self, location: &Self::Path, bytes: S, length: Option<usize>) -> Result<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        let temporary_non_streaming = bytes
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .expect("Should have been able to collect streaming data")
            .to_vec();

        if let Some(length) = length {
            ensure!(
                temporary_non_streaming.len() == length,
                DataDoesNotMatchLength {
                    actual: temporary_non_streaming.len(),
                    expected: length,
                }
            );
        }

        let location = location.to_raw();
        let location_copy = location.clone();
        let bucket_name = self.bucket_name.clone();

        self.client
            .object()
            .create(
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

        let bytes = self
            .client
            .object()
            .download(&bucket_name, &location_copy)
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

        self.client
            .object()
            .delete(&bucket_name, &location_copy)
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
        let converted_prefix = prefix.map(|p| p.to_raw());
        let list_request = cloud_storage::ListRequest {
            prefix: converted_prefix,
            ..Default::default()
        };
        let object_lists = self
            .client
            .object()
            .list(&self.bucket_name, list_request)
            .await
            .context(UnableToListData {
                bucket: &self.bucket_name,
            })?;

        let bucket_name = self.bucket_name.clone();
        let objects = object_lists
            .map_ok(|list| {
                list.items
                    .into_iter()
                    .map(|o| CloudPath::raw(o.name))
                    .collect::<Vec<_>>()
            })
            .map_err(move |source| Error::UnableToStreamListData {
                source,
                bucket: bucket_name.clone(),
            });

        Ok(objects.boxed())
    }

    async fn list_with_delimiter(&self, prefix: &Self::Path) -> Result<ListResult<Self::Path>> {
        let converted_prefix = prefix.to_raw();
        let list_request = cloud_storage::ListRequest {
            prefix: Some(converted_prefix),
            delimiter: Some(DELIMITER.to_string()),
            ..Default::default()
        };

        let mut object_lists = Box::pin(
            self.client
                .object()
                .list(&self.bucket_name, list_request)
                .await
                .context(UnableToListData {
                    bucket: &self.bucket_name,
                })?,
        );

        let result = match object_lists.next().await {
            None => ListResult {
                objects: vec![],
                common_prefixes: vec![],
                next_token: None,
            },
            Some(list_response) => {
                let list_response = list_response.context(UnableToStreamListData {
                    bucket: &self.bucket_name,
                })?;

                ListResult {
                    objects: list_response
                        .items
                        .iter()
                        .map(|object| {
                            let location = CloudPath::raw(&object.name);
                            let last_modified = object.updated;
                            let size = usize::try_from(object.size)
                                .expect("unsupported size on this platform");

                            ObjectMeta {
                                location,
                                last_modified,
                                size,
                            }
                        })
                        .collect(),
                    common_prefixes: list_response.prefixes.iter().map(CloudPath::raw).collect(),
                    next_token: list_response.next_page_token,
                }
            }
        };

        Ok(result)
    }
}

/// Configure a connection to Google Cloud Storage.
pub fn new_gcs(
    service_account_path: impl AsRef<std::ffi::OsStr>,
    bucket_name: impl Into<String>,
) -> Result<GoogleCloudStorage> {
    // The cloud storage crate currently only supports authentication via
    // environment variables. Set the environment variable explicitly so
    // that we can optionally accept command line arguments instead.
    env::set_var("SERVICE_ACCOUNT", service_account_path);
    Ok(GoogleCloudStorage {
        client: Default::default(),
        bucket_name: bucket_name.into(),
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        tests::{get_nonexistent_object, list_with_delimiter, put_get_delete_list},
        Error as ObjectStoreError, ObjectStore, ObjectStoreApi, ObjectStorePath,
    };
    use bytes::Bytes;
    use std::env;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[derive(Debug)]
    struct GoogleCloudConfig {
        bucket: String,
        service_account: String,
    }

    // Helper macro to skip tests if TEST_INTEGRATION and the GCP environment variables are not set.
    macro_rules! maybe_skip_integration {
        () => {{
            dotenv::dotenv().ok();

            let required_vars = ["INFLUXDB_IOX_BUCKET", "GOOGLE_SERVICE_ACCOUNT"];
            let unset_vars: Vec<_> = required_vars
                .iter()
                .filter_map(|&name| match env::var(name) {
                    Ok(_) => None,
                    Err(_) => Some(name),
                })
                .collect();
            let unset_var_names = unset_vars.join(", ");

            let force = std::env::var("TEST_INTEGRATION");

            if force.is_ok() && !unset_var_names.is_empty() {
                panic!(
                    "TEST_INTEGRATION is set, \
                            but variable(s) {} need to be set",
                    unset_var_names
                )
            } else if force.is_err() {
                eprintln!(
                    "skipping Google Cloud integration test - set {}TEST_INTEGRATION to run",
                    if unset_var_names.is_empty() {
                        String::new()
                    } else {
                        format!("{} and ", unset_var_names)
                    }
                );
                return;
            } else {
                GoogleCloudConfig {
                    bucket: env::var("INFLUXDB_IOX_BUCKET")
                        .expect("already checked INFLUXDB_IOX_BUCKET"),
                    service_account: env::var("GOOGLE_SERVICE_ACCOUNT")
                        .expect("already checked GOOGLE_SERVICE_ACCOUNT"),
                }
            }
        }};
    }

    #[tokio::test]
    async fn gcs_test() {
        let config = maybe_skip_integration!();
        let integration =
            ObjectStore::new_google_cloud_storage(config.service_account, config.bucket).unwrap();

        put_get_delete_list(&integration).await.unwrap();
        list_with_delimiter(&integration).await.unwrap();
    }

    #[tokio::test]
    async fn gcs_test_get_nonexistent_location() {
        let config = maybe_skip_integration!();
        let integration =
            ObjectStore::new_google_cloud_storage(config.service_account, &config.bucket).unwrap();

        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();

        if let Some(ObjectStoreError::GcsObjectStoreError {
            source:
                Error::UnableToGetData {
                    source,
                    bucket,
                    location,
                },
        }) = err.downcast_ref::<ObjectStoreError>()
        {
            assert!(matches!(source, cloud_storage::Error::Reqwest(_)));
            assert_eq!(bucket, &config.bucket);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type: {:?}", err)
        }
    }

    #[tokio::test]
    async fn gcs_test_get_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();
        let integration =
            ObjectStore::new_google_cloud_storage(config.service_account, &config.bucket).unwrap();

        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();

        if let Some(ObjectStoreError::GcsObjectStoreError {
            source: Error::UnableToStreamListData { source, bucket },
        }) = err.downcast_ref::<ObjectStoreError>()
        {
            assert!(matches!(source, cloud_storage::Error::Google(_)));
            assert_eq!(bucket, &config.bucket);
        } else {
            panic!("unexpected error type: {:?}", err);
        }
    }

    #[tokio::test]
    async fn gcs_test_delete_nonexistent_location() {
        let config = maybe_skip_integration!();
        let integration =
            ObjectStore::new_google_cloud_storage(config.service_account, &config.bucket).unwrap();

        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let err = integration.delete(&location).await.unwrap_err();

        if let ObjectStoreError::GcsObjectStoreError {
            source:
                Error::UnableToDeleteData {
                    source,
                    bucket,
                    location,
                },
        } = err
        {
            assert!(matches!(source, cloud_storage::Error::Google(_)));
            assert_eq!(bucket, config.bucket);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type: {:?}", err)
        }
    }

    #[tokio::test]
    async fn gcs_test_delete_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();
        let integration =
            ObjectStore::new_google_cloud_storage(config.service_account, &config.bucket).unwrap();

        let mut location = integration.new_path();
        location.set_file_name(NON_EXISTENT_NAME);

        let err = integration.delete(&location).await.unwrap_err();

        if let ObjectStoreError::GcsObjectStoreError {
            source:
                Error::UnableToDeleteData {
                    source,
                    bucket,
                    location,
                },
        } = err
        {
            assert!(matches!(source, cloud_storage::Error::Google(_)));
            assert_eq!(bucket, config.bucket);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type: {:?}", err)
        }
    }

    #[tokio::test]
    async fn gcs_test_put_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();
        let integration =
            ObjectStore::new_google_cloud_storage(config.service_account, &config.bucket).unwrap();

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

        if let ObjectStoreError::GcsObjectStoreError {
            source:
                Error::UnableToPutData {
                    source,
                    bucket,
                    location,
                },
        } = err
        {
            assert!(matches!(source, cloud_storage::Error::Other(_)));
            assert_eq!(bucket, config.bucket);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type: {:?}", err);
        }
    }
}
