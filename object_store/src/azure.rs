//! This module contains the IOx implementation for using Azure Blob storage as
//! the object store.
use crate::{
    path::{Path, DELIMITER},
    GetResult, ListResult, ObjectMeta, ObjectStoreApi, Result,
};
use async_trait::async_trait;
use azure_core::{prelude::*, HttpClient};
use azure_storage::core::prelude::*;
use azure_storage_blobs::blob::Blob;
use azure_storage_blobs::{
    prelude::{AsBlobClient, AsContainerClient, ContainerClient},
    DeleteSnapshotsMethod,
};
use bytes::Bytes;
use futures::{
    stream::{self, BoxStream},
    FutureExt, StreamExt, TryStreamExt,
};
use snafu::{ResultExt, Snafu};
use std::{convert::TryInto, sync::Arc};

/// A specialized `Error` for Azure object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
enum Error {
    #[snafu(display("Unable to DELETE data. Location: {}, Error: {}", path, source,))]
    Delete {
        source: Box<dyn std::error::Error + Send + Sync>,
        path: String,
    },

    #[snafu(display("Unable to GET data. Location: {}, Error: {}", path, source,))]
    Get {
        source: Box<dyn std::error::Error + Send + Sync>,
        path: String,
    },

    #[snafu(display("Unable to PUT data. Location: {}, Error: {}", path, source,))]
    Put {
        source: Box<dyn std::error::Error + Send + Sync>,
        path: String,
    },

    #[snafu(display("Unable to list data. Error: {}", source))]
    List {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[cfg(not(feature = "azure_test"))]
    #[snafu(display(
        "Azurite (azure emulator) support not compiled in, please add `azure_test` feature"
    ))]
    NoEmulatorFeature,
}

impl From<Error> for super::Error {
    fn from(source: Error) -> Self {
        Self::Generic {
            store: "Azure Blob Storage",
            source: Box::new(source),
        }
    }
}

/// Configuration for connecting to [Microsoft Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/).
#[derive(Debug)]
pub struct MicrosoftAzure {
    container_client: Arc<ContainerClient>,
    #[allow(dead_code)]
    container_name: String,
}

impl std::fmt::Display for MicrosoftAzure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MicrosoftAzure({})", self.container_name)
    }
}

#[async_trait]
impl ObjectStoreApi for MicrosoftAzure {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let location = location.to_raw();

        let bytes = bytes::BytesMut::from(&*bytes);

        self.container_client
            .as_blob_client(location)
            .put_block_blob(bytes)
            .execute()
            .await
            .context(PutSnafu {
                path: location.to_owned(),
            })?;

        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let container_client = Arc::clone(&self.container_client);
        let location = location.to_raw().to_string();
        let s = async move {
            let bytes = container_client
                .as_blob_client(&location)
                .get()
                .execute()
                .await
                .map(|blob| blob.data)
                .context(GetSnafu { path: location })?;

            Ok(bytes)
        }
        .into_stream()
        .boxed();

        Ok(GetResult::Stream(s))
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let location = location.to_raw();
        self.container_client
            .as_blob_client(location)
            .delete()
            .delete_snapshots_method(DeleteSnapshotsMethod::Include)
            .execute()
            .await
            .context(DeleteSnafu {
                path: location.to_owned(),
            })?;

        Ok(())
    }

    async fn list<'a>(
        &'a self,
        prefix: Option<&'a Path>,
    ) -> Result<BoxStream<'a, Result<ObjectMeta>>> {
        #[derive(Clone)]
        enum ListState {
            Start,
            HasMore(String),
            Done,
        }

        Ok(stream::unfold(ListState::Start, move |state| async move {
            let mut request = self.container_client.list_blobs();

            let prefix_raw = prefix.map(|p| format!("{}{}", p.to_raw(), DELIMITER));
            if let Some(ref p) = prefix_raw {
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

            let resp = match request.execute().await.context(ListSnafu) {
                Ok(resp) => resp,
                Err(err) => return Some((Err(crate::Error::from(err)), state)),
            };

            let next_state = if let Some(marker) = resp.next_marker {
                ListState::HasMore(marker.as_str().to_string())
            } else {
                ListState::Done
            };

            let names = resp.blobs.blobs.into_iter().map(convert_object_meta);
            Some((Ok(futures::stream::iter(names)), next_state))
        })
        .try_flatten()
        .boxed())
    }

    async fn list_with_delimiter(&self, prefix: &Path) -> Result<ListResult> {
        let mut request = self.container_client.list_blobs();

        let prefix_raw = format!("{}{}", prefix, DELIMITER);

        request = request.delimiter(Delimiter::new(DELIMITER));
        request = request.prefix(&*prefix_raw);

        let resp = request.execute().await.context(ListSnafu)?;

        let next_token = resp.next_marker.as_ref().map(|m| m.as_str().to_string());

        let common_prefixes = resp
            .blobs
            .blob_prefix
            .map(|prefixes| {
                prefixes
                    .iter()
                    .map(|prefix| Path::from_raw(&prefix.name))
                    .collect()
            })
            .unwrap_or_else(Vec::new);

        let objects = resp
            .blobs
            .blobs
            .into_iter()
            .map(convert_object_meta)
            .collect::<Result<_>>()?;

        Ok(ListResult {
            next_token,
            common_prefixes,
            objects,
        })
    }
}

fn convert_object_meta(blob: Blob) -> Result<ObjectMeta> {
    let location = Path::from_raw(blob.name);
    let last_modified = blob.properties.last_modified;
    let size = blob
        .properties
        .content_length
        .try_into()
        .expect("unsupported size on this platform");

    Ok(ObjectMeta {
        location,
        last_modified,
        size,
    })
}

#[cfg(feature = "azure_test")]
fn check_if_emulator_works() -> Result<()> {
    Ok(())
}

#[cfg(not(feature = "azure_test"))]
fn check_if_emulator_works() -> Result<()> {
    Err(Error::NoEmulatorFeature.into())
}

/// Configure a connection to container with given name on Microsoft Azure
/// Blob store.
///
/// The credentials `account` and `access_key` must provide access to the
/// store.
pub fn new_azure(
    account: impl Into<String>,
    access_key: impl Into<String>,
    container_name: impl Into<String>,
    use_emulator: bool,
) -> Result<MicrosoftAzure> {
    let account = account.into();
    let access_key = access_key.into();
    let http_client: Arc<dyn HttpClient> = Arc::new(reqwest::Client::new());

    let storage_account_client = if use_emulator {
        check_if_emulator_works()?;
        StorageAccountClient::new_emulator_default()
    } else {
        StorageAccountClient::new_access_key(Arc::clone(&http_client), &account, &access_key)
    };

    let storage_client = storage_account_client.as_storage_client();

    let container_name = container_name.into();

    let container_client = storage_client.as_container_client(&container_name);

    Ok(MicrosoftAzure {
        container_client,
        container_name,
    })
}

#[cfg(test)]
mod tests {
    use crate::azure::new_azure;
    use crate::tests::{list_uses_directories_correctly, list_with_delimiter, put_get_delete_list};
    use std::env;

    #[derive(Debug)]
    struct AzureConfig {
        storage_account: String,
        access_key: String,
        bucket: String,
        use_emulator: bool,
    }

    // Helper macro to skip tests if TEST_INTEGRATION and the Azure environment
    // variables are not set.
    macro_rules! maybe_skip_integration {
        () => {{
            dotenv::dotenv().ok();

            let use_emulator = std::env::var("AZURE_USE_EMULATOR").is_ok();

            let mut required_vars = vec!["OBJECT_STORE_BUCKET"];
            if !use_emulator {
                required_vars.push("AZURE_STORAGE_ACCOUNT");
                required_vars.push("AZURE_STORAGE_ACCESS_KEY");
            }
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
                    "skipping Azure integration test - set {}TEST_INTEGRATION to run",
                    if unset_var_names.is_empty() {
                        String::new()
                    } else {
                        format!("{} and ", unset_var_names)
                    }
                );
                return;
            } else {
                AzureConfig {
                    storage_account: env::var("AZURE_STORAGE_ACCOUNT").unwrap_or_default(),
                    access_key: env::var("AZURE_STORAGE_ACCESS_KEY").unwrap_or_default(),
                    bucket: env::var("OBJECT_STORE_BUCKET")
                        .expect("already checked OBJECT_STORE_BUCKET"),
                    use_emulator,
                }
            }
        }};
    }

    #[tokio::test]
    async fn azure_blob_test() {
        let config = maybe_skip_integration!();
        let integration = new_azure(
            config.storage_account,
            config.access_key,
            config.bucket,
            config.use_emulator,
        )
        .unwrap();

        put_get_delete_list(&integration).await.unwrap();
        list_uses_directories_correctly(&integration).await.unwrap();
        list_with_delimiter(&integration).await.unwrap();
    }
}
