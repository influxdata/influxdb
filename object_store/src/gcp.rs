//! This module contains the IOx implementation for using Google Cloud Storage
//! as the object store.
use crate::{
    path::{Path, DELIMITER},
    GetResult, ListResult, ObjectMeta, ObjectStoreApi, Result,
};
use async_trait::async_trait;
use bytes::Bytes;
use cloud_storage::Client;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use snafu::{ResultExt, Snafu};
use std::{convert::TryFrom, env};

/// A specialized `Error` for Google Cloud Storage object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Expected streamed data to have length {}, got {}", expected, actual))]
    DataDoesNotMatchLength { expected: usize, actual: usize },

    #[snafu(display(
        "Unable to PUT data. Bucket: {}, Location: {}, Error: {}",
        bucket,
        path,
        source
    ))]
    UnableToPutData {
        source: cloud_storage::Error,
        bucket: String,
        path: String,
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
        path,
        source,
    ))]
    UnableToDeleteData {
        source: cloud_storage::Error,
        bucket: String,
        path: String,
    },

    #[snafu(display(
        "Unable to GET data. Bucket: {}, Location: {}, Error: {}",
        bucket,
        path,
        source,
    ))]
    UnableToGetData {
        source: cloud_storage::Error,
        bucket: String,
        path: String,
    },

    NotFound {
        path: String,
        source: cloud_storage::Error,
    },
}

impl From<Error> for super::Error {
    fn from(source: Error) -> Self {
        match source {
            Error::NotFound { path, source } => Self::NotFound {
                path,
                source: source.into(),
            },
            _ => Self::Generic {
                store: "GCS",
                source: Box::new(source),
            },
        }
    }
}

/// Configuration for connecting to [Google Cloud Storage](https://cloud.google.com/storage/).
#[derive(Debug)]
pub struct GoogleCloudStorage {
    client: Client,
    bucket_name: String,
}

impl std::fmt::Display for GoogleCloudStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GoogleCloudStorage({})", self.bucket_name)
    }
}

#[async_trait]
impl ObjectStoreApi for GoogleCloudStorage {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let location = location.to_raw();
        let bucket_name = self.bucket_name.clone();

        self.client
            .object()
            .create(
                &bucket_name,
                bytes.to_vec(),
                location,
                "application/octet-stream",
            )
            .await
            .context(UnableToPutDataSnafu {
                bucket: &self.bucket_name,
                path: location,
            })?;

        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let location = location.to_raw();
        let bucket_name = self.bucket_name.clone();

        let bytes = self
            .client
            .object()
            .download(&bucket_name, location)
            .await
            .map_err(|e| match e {
                cloud_storage::Error::Other(ref text) if text.starts_with("No such object") => {
                    Error::NotFound {
                        path: location.to_string(),
                        source: e,
                    }
                }
                _ => Error::UnableToGetData {
                    bucket: bucket_name.clone(),
                    path: location.to_string(),
                    source: e,
                },
            })?;

        let s = futures::stream::once(async move { Ok(bytes.into()) }).boxed();
        Ok(GetResult::Stream(s))
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let location = location.to_raw();
        let bucket_name = self.bucket_name.clone();

        self.client
            .object()
            .delete(&bucket_name, location)
            .await
            .context(UnableToDeleteDataSnafu {
                bucket: &self.bucket_name,
                path: location,
            })?;

        Ok(())
    }

    async fn list<'a>(
        &'a self,
        prefix: Option<&'a Path>,
    ) -> Result<BoxStream<'a, Result<Vec<Path>>>> {
        let converted_prefix = prefix.map(|p| format!("{}{}", p.to_raw(), DELIMITER));
        let list_request = cloud_storage::ListRequest {
            prefix: converted_prefix,
            ..Default::default()
        };
        let object_lists = self
            .client
            .object()
            .list(&self.bucket_name, list_request)
            .await
            .context(UnableToListDataSnafu {
                bucket: &self.bucket_name,
            })?;

        let bucket_name = self.bucket_name.clone();
        let objects = object_lists
            .map_ok(move |list| {
                list.items
                    .into_iter()
                    .map(|o| Path::from_raw(o.name))
                    .collect::<Vec<_>>()
            })
            .map_err(move |source| {
                Error::UnableToStreamListData {
                    source,
                    bucket: bucket_name.clone(),
                }
                .into()
            });

        Ok(objects.boxed())
    }

    async fn list_with_delimiter(&self, prefix: &Path) -> Result<ListResult> {
        let converted_prefix = format!("{}{}", prefix, DELIMITER);
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
                .context(UnableToListDataSnafu {
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
                let list_response = list_response.context(UnableToStreamListDataSnafu {
                    bucket: &self.bucket_name,
                })?;

                ListResult {
                    objects: list_response
                        .items
                        .iter()
                        .map(|object| {
                            let location = Path::from_raw(&object.name);
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
                    common_prefixes: list_response.prefixes.iter().map(Path::from_raw).collect(),
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
        tests::{
            get_nonexistent_object, list_uses_directories_correctly, list_with_delimiter,
            put_get_delete_list,
        },
        Error as ObjectStoreError, ObjectStoreApi,
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
        let integration = new_gcs(config.service_account, config.bucket).unwrap();

        put_get_delete_list(&integration).await.unwrap();
        list_uses_directories_correctly(&integration).await.unwrap();
        list_with_delimiter(&integration).await.unwrap();
    }

    #[tokio::test]
    async fn gcs_test_get_nonexistent_location() {
        let config = maybe_skip_integration!();
        let integration = new_gcs(config.service_account, &config.bucket).unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();

        if let Some(ObjectStoreError::NotFound { path, source }) =
            err.downcast_ref::<ObjectStoreError>()
        {
            let source_variant = source.downcast_ref::<cloud_storage::Error>();
            assert!(
                matches!(source_variant, Some(cloud_storage::Error::Other(_))),
                "got: {:?}",
                source_variant
            );
            assert_eq!(path, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type: {:?}", err)
        }
    }

    #[tokio::test]
    async fn gcs_test_get_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();
        let integration = new_gcs(config.service_account, &config.bucket).unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("Unable to stream list data"), "{}", err)
    }

    #[tokio::test]
    async fn gcs_test_delete_nonexistent_location() {
        let config = maybe_skip_integration!();
        let integration = new_gcs(config.service_account, &config.bucket).unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.delete(&location).await.unwrap_err().to_string();
        assert!(err.contains("Unable to DELETE data"), "{}", err)
    }

    #[tokio::test]
    async fn gcs_test_delete_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();
        let integration = new_gcs(config.service_account, &config.bucket).unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.delete(&location).await.unwrap_err().to_string();
        assert!(err.contains("Unable to DELETE data"), "{}", err)
    }

    #[tokio::test]
    async fn gcs_test_put_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();
        let integration = new_gcs(config.service_account, &config.bucket).unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);
        let data = Bytes::from("arbitrary data");

        let err = integration
            .put(&location, data)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("Unable to PUT data"), "{}", err)
    }
}
