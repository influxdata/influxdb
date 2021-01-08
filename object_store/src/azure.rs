//! This module contains the IOx implementation for using Azure Blob storage as
//! the object store.
use crate::{
    path::{CloudConverter, ObjectStorePath},
    DataDoesNotMatchLength, Result, UnableToDeleteDataFromAzure, UnableToGetDataFromAzure,
    UnableToListDataFromAzure, UnableToPutDataToAzure,
};
use azure_sdk_core::prelude::*;
use azure_sdk_storage_blob::prelude::*;
use bytes::Bytes;
use futures::{stream, FutureExt, Stream, TryStreamExt};
use snafu::{ensure, ResultExt};
use std::io;
use std::sync::Arc;

/// Configuration for connecting to [Microsoft Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/).
#[derive(Debug)]
pub struct MicrosoftAzure {
    client: Arc<azure_sdk_storage_core::key_client::KeyClient>,
    container_name: String,
}

impl MicrosoftAzure {
    /// Configure a connection to container with given name on Microsoft Azure
    /// Blob store.
    ///
    /// The credentials `account` and `master_key` must provide access to the
    /// store.
    pub fn new(account: String, master_key: String, container_name: impl Into<String>) -> Self {
        Self {
            client: Arc::new(azure_sdk_storage_core::client::with_access_key(
                &account,
                &master_key,
            )),
            container_name: container_name.into(),
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

    /// Save the provided bytes to the specified location.
    pub async fn put<S>(&self, location: &ObjectStorePath, bytes: S, length: usize) -> Result<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        let location = CloudConverter::convert(&location);
        let temporary_non_streaming = bytes
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .expect("Should have been able to collect streaming data");

        ensure!(
            temporary_non_streaming.len() == length,
            DataDoesNotMatchLength {
                actual: temporary_non_streaming.len(),
                expected: length,
            }
        );

        self.client
            .put_block_blob()
            .with_container_name(&self.container_name)
            .with_blob_name(&location)
            .with_body(&temporary_non_streaming)
            .finalize()
            .await
            .context(UnableToPutDataToAzure {
                location: location.to_owned(),
            })?;

        Ok(())
    }

    /// Return the bytes that are stored at the specified location.
    pub async fn get(
        &self,
        location: &ObjectStorePath,
    ) -> Result<impl Stream<Item = Result<Bytes>>> {
        let client = self.client.clone();
        let container_name = self.container_name.clone();
        let location = CloudConverter::convert(&location);
        Ok(async move {
            client
                .get_blob()
                .with_container_name(&container_name)
                .with_blob_name(&location)
                .finalize()
                .await
                .map(|blob| blob.data.into())
                .context(UnableToGetDataFromAzure {
                    location: location.to_owned(),
                })
        }
        .into_stream())
    }

    /// Delete the object at the specified location.
    pub async fn delete(&self, location: &ObjectStorePath) -> Result<()> {
        let location = CloudConverter::convert(&location);
        self.client
            .delete_blob()
            .with_container_name(&self.container_name)
            .with_blob_name(&location)
            .with_delete_snapshots_method(DeleteSnapshotsMethod::Include)
            .finalize()
            .await
            .context(UnableToDeleteDataFromAzure {
                location: location.to_owned(),
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

        Ok(stream::unfold(ListState::Start, move |state| async move {
            let mut request = self
                .client
                .list_blobs()
                .with_container_name(&self.container_name);

            let prefix = prefix.map(CloudConverter::convert);
            if let Some(ref p) = prefix {
                request = request.with_prefix(p);
            }

            match state {
                ListState::HasMore(ref token) => {
                    request = request.with_next_marker(token);
                }
                ListState::Done => {
                    return None;
                }
                ListState::Start => {}
            }

            let resp = match request.finalize().await.context(UnableToListDataFromAzure) {
                Ok(resp) => resp,
                Err(err) => return Some((Err(err), state)),
            };

            let next_state = if let Some(token) = resp.incomplete_vector.token() {
                ListState::HasMore(token.to_string())
            } else {
                ListState::Done
            };

            let names = resp
                .incomplete_vector
                .vector
                .into_iter()
                .map(|blob| ObjectStorePath::from_cloud_unchecked(blob.name))
                .collect();

            Some((Ok(names), next_state))
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{tests::put_get_delete_list, ObjectStore};
    use std::env;

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    // Helper macro to skip tests if the GCP environment variables are not set.
    // Skips become hard errors if TEST_INTEGRATION is set.
    macro_rules! maybe_skip_integration {
        () => {
            dotenv::dotenv().ok();

            let account = env::var("AZURE_STORAGE_ACCOUNT");
            let container = env::var("AZURE_STORAGE_CONTAINER");
            let force = std::env::var("TEST_INTEGRATION");

            match (account.is_ok(), container.is_ok(), force.is_ok()) {
                (false, false, true) => {
                    panic!(
                        "TEST_INTEGRATION is set, \
                            but AZURE_STROAGE_ACCOUNT and AZURE_STORAGE_CONTAINER are not"
                    )
                }
                (false, true, true) => {
                    panic!("TEST_INTEGRATION is set, but AZURE_STORAGE_ACCOUNT is not")
                }
                (true, false, true) => {
                    panic!("TEST_INTEGRATION is set, but AZURE_STORAGE_CONTAINER is not")
                }
                (false, false, false) => {
                    eprintln!(
                        "skipping integration test - set \
                               AZURE_STROAGE_ACCOUNT and AZURE_STORAGE_CONTAINER to run"
                    );
                    return Ok(());
                }
                (false, true, false) => {
                    eprintln!("skipping integration test - set AZURE_STORAGE_ACCOUNT to run");
                    return Ok(());
                }
                (true, false, false) => {
                    eprintln!("skipping integration test - set AZURE_STROAGE_CONTAINER to run");
                    return Ok(());
                }
                _ => {}
            }
        };
    }

    #[tokio::test]
    async fn azure_blob_test() -> Result<()> {
        maybe_skip_integration!();

        let container_name = env::var("AZURE_STORAGE_CONTAINER")
            .map_err(|_| "The environment variable AZURE_STORAGE_CONTAINER must be set")?;
        let azure = MicrosoftAzure::new_from_env(container_name);

        let integration = ObjectStore::new_microsoft_azure(azure);
        put_get_delete_list(&integration).await?;

        Ok(())
    }
}
