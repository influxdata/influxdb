//! This module contains the IOx implementation for using Azure Blob storage as
//! the object store.
use crate::{
    path::{cloud::CloudPath, DELIMITER},
    ListResult, ObjectMeta, ObjectStoreApi,
};
use async_trait::async_trait;
use azure_core::prelude::*;
use azure_storage::{
    clients::{
        AsBlobClient, AsContainerClient, AsStorageClient, ContainerClient, StorageAccountClient,
    },
    DeleteSnapshotsMethod,
};
use bytes::Bytes;
use futures::{
    stream::{self, BoxStream},
    FutureExt, Stream, StreamExt, TryStreamExt,
};
use snafu::{ensure, ResultExt, Snafu};
use std::sync::Arc;
use std::{convert::TryInto, io};

/// A specialized `Result` for Azure object store-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A specialized `Error` for Azure object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Expected streamed data to have length {}, got {}", expected, actual))]
    DataDoesNotMatchLength { expected: usize, actual: usize },

    #[snafu(display("Unable to DELETE data. Location: {}, Error: {}", location, source,))]
    UnableToDeleteData {
        source: Box<dyn std::error::Error + Send + Sync>,
        location: String,
    },

    #[snafu(display("Unable to GET data. Location: {}, Error: {}", location, source,))]
    UnableToGetData {
        source: Box<dyn std::error::Error + Send + Sync>,
        location: String,
    },

    #[snafu(display("Unable to PUT data. Location: {}, Error: {}", location, source,))]
    UnableToPutData {
        source: Box<dyn std::error::Error + Send + Sync>,
        location: String,
    },

    #[snafu(display("Unable to list data. Error: {}", source))]
    UnableToListData {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Configuration for connecting to [Microsoft Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/).
#[derive(Debug)]
pub struct MicrosoftAzure {
    container_client: Arc<ContainerClient>,
    container_name: String,
}

#[async_trait]
impl ObjectStoreApi for MicrosoftAzure {
    type Path = CloudPath;
    type Error = Error;

    fn new_path(&self) -> Self::Path {
        CloudPath::default()
    }

    async fn put<S>(&self, location: &Self::Path, bytes: S, length: Option<usize>) -> Result<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        let location = location.to_raw();
        let temporary_non_streaming = bytes
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .expect("Should have been able to collect streaming data");

        if let Some(length) = length {
            ensure!(
                temporary_non_streaming.len() == length,
                DataDoesNotMatchLength {
                    actual: temporary_non_streaming.len(),
                    expected: length,
                }
            );
        }

        self.container_client
            .as_blob_client(&location)
            .put_block_blob(temporary_non_streaming)
            .execute()
            .await
            .context(UnableToPutData {
                location: location.to_owned(),
            })?;

        Ok(())
    }

    async fn get(&self, location: &Self::Path) -> Result<BoxStream<'static, Result<Bytes>>> {
        let container_client = Arc::clone(&self.container_client);
        let location = location.to_raw();
        Ok(async move {
            container_client
                .as_blob_client(&location)
                .get()
                .execute()
                .await
                .map(|blob| blob.data.into())
                .context(UnableToGetData {
                    location: location.to_owned(),
                })
        }
        .into_stream()
        .boxed())
    }

    async fn delete(&self, location: &Self::Path) -> Result<()> {
        let location = location.to_raw();
        self.container_client
            .as_blob_client(&location)
            .delete()
            .delete_snapshots_method(DeleteSnapshotsMethod::Include)
            .execute()
            .await
            .context(UnableToDeleteData {
                location: location.to_owned(),
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

        Ok(stream::unfold(ListState::Start, move |state| async move {
            let mut request = self.container_client.list_blobs();

            let prefix = prefix.map(|p| p.to_raw());
            if let Some(ref p) = prefix {
                request = request.prefix(p as &str);
            }

            match state {
                ListState::HasMore(ref marker) => {
                    request = request.next_marker(marker as &str);
                }
                ListState::Done => {
                    return None;
                }
                ListState::Start => {}
            }

            let resp = match request.execute().await.context(UnableToListData) {
                Ok(resp) => resp,
                Err(err) => return Some((Err(err), state)),
            };

            let next_state = if let Some(marker) = resp.next_marker {
                ListState::HasMore(marker.as_str().to_string())
            } else {
                ListState::Done
            };

            let names = resp
                .blobs
                .blobs
                .into_iter()
                .map(|blob| CloudPath::raw(blob.name))
                .collect();

            Some((Ok(names), next_state))
        })
        .boxed())
    }

    async fn list_with_delimiter(&self, prefix: &Self::Path) -> Result<ListResult<Self::Path>> {
        let mut request = self.container_client.list_blobs();

        let prefix = prefix.to_raw();

        request = request.delimiter(Delimiter::new(DELIMITER));
        request = request.prefix(&*prefix);

        let resp = request.execute().await.context(UnableToListData)?;

        let next_token = resp.next_marker.as_ref().map(|m| m.as_str().to_string());

        let common_prefixes = resp
            .blobs
            .blob_prefix
            .map(|prefixes| {
                prefixes
                    .iter()
                    .map(|prefix| CloudPath::raw(&prefix.name))
                    .collect()
            })
            .unwrap_or_else(Vec::new);

        let objects = resp
            .blobs
            .blobs
            .into_iter()
            .map(|blob| {
                let location = CloudPath::raw(blob.name);
                let last_modified = blob.properties.last_modified;
                let size = blob
                    .properties
                    .content_length
                    .try_into()
                    .expect("unsupported size on this platform");

                ObjectMeta {
                    location,
                    last_modified,
                    size,
                }
            })
            .collect();

        Ok(ListResult {
            next_token,
            common_prefixes,
            objects,
        })
    }
}

impl MicrosoftAzure {
    /// Configure a connection to container with given name on Microsoft Azure
    /// Blob store.
    ///
    /// The credentials `account` and `master_key` must provide access to the
    /// store.
    pub fn new(account: String, master_key: String, container_name: impl Into<String>) -> Self {
        // From https://github.com/Azure/azure-sdk-for-rust/blob/master/sdk/storage/examples/blob_00.rs#L29
        let http_client: Arc<Box<dyn HttpClient>> = Arc::new(Box::new(reqwest::Client::new()));

        let storage_account_client =
            StorageAccountClient::new_access_key(Arc::clone(&http_client), &account, &master_key);

        let storage_client = storage_account_client.as_storage_client();

        let container_name = container_name.into();

        let container_client = storage_client.as_container_client(&container_name);

        Self {
            container_client,
            container_name,
        }
    }

    /// Configure a connection to container with given name on Microsoft Azure
    /// Blob store.
    ///
    /// The credentials `account` and `master_key` must be set via the
    /// environment variables `AZURE_STORAGE_ACCOUNT` and
    /// `AZURE_STORAGE_MASTER_KEY` respectively.
    pub fn new_from_env(container_name: impl Into<String>) -> Self {
        let account = std::env::var("AZURE_STORAGE_ACCOUNT")
            .expect("Set env variable AZURE_STORAGE_ACCOUNT first!");
        let master_key = std::env::var("AZURE_STORAGE_MASTER_KEY")
            .expect("Set env variable AZURE_STORAGE_MASTER_KEY first!");

        Self::new(account, master_key, container_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{list_with_delimiter, put_get_delete_list};
    use std::env;

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    // Helper macro to skip tests if the GCP environment variables are not set.
    // Skips become hard errors if TEST_INTEGRATION is set.
    macro_rules! maybe_skip_integration {
        () => {
            dotenv::dotenv().ok();

            let required_vars = [
                "AZURE_STORAGE_ACCOUNT",
                "AZURE_STORAGE_CONTAINER",
                "AZURE_STORAGE_MASTER_KEY",
            ];
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
            } else if force.is_err() && !unset_var_names.is_empty() {
                eprintln!(
                    "skipping Azure integration test - set \
                           {} to run",
                    unset_var_names
                );
                return Ok(());
            }
        };
    }

    #[tokio::test]
    async fn azure_blob_test() -> Result<()> {
        maybe_skip_integration!();

        let container_name = env::var("AZURE_STORAGE_CONTAINER")
            .map_err(|_| "The environment variable AZURE_STORAGE_CONTAINER must be set")?;
        let integration = MicrosoftAzure::new_from_env(container_name);

        put_get_delete_list(&integration).await?;
        list_with_delimiter(&integration).await?;

        Ok(())
    }
}
