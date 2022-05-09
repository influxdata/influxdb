//! This module contains the IOx implementation for using memory as the object
//! store.
use crate::{path::Path, GetResult, ListResult, ObjectMeta, ObjectStoreApi, Result};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::{stream::BoxStream, StreamExt};
use snafu::{OptionExt, Snafu};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use tokio::sync::RwLock;

/// A specialized `Error` for in-memory object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
enum Error {
    #[snafu(display("No data in memory found. Location: {path}"))]
    NoDataInMemory { path: String },
}

impl From<Error> for super::Error {
    fn from(source: Error) -> Self {
        match source {
            Error::NoDataInMemory { ref path } => Self::NotFound {
                path: path.into(),
                source: source.into(),
            },
            // currently "not found" is the only error that can happen with the in-memory store
        }
    }
}

/// In-memory storage suitable for testing or for opting out of using a cloud
/// storage provider.
#[derive(Debug, Default)]
pub struct InMemory {
    storage: RwLock<BTreeMap<Path, Bytes>>,
}

impl std::fmt::Display for InMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InMemory")
    }
}

#[async_trait]
impl ObjectStoreApi for InMemory {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.storage.write().await.insert(location.clone(), bytes);
        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let data =
            self.storage
                .read()
                .await
                .get(location)
                .cloned()
                .context(NoDataInMemorySnafu {
                    path: location.to_string(),
                })?;

        Ok(GetResult::Stream(
            futures::stream::once(async move { Ok(data) }).boxed(),
        ))
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.storage.write().await.remove(location);
        Ok(())
    }

    async fn list<'a>(
        &'a self,
        prefix: Option<&'a Path>,
    ) -> Result<BoxStream<'a, Result<Vec<Path>>>> {
        let list = if let Some(prefix) = &prefix {
            self.storage
                .read()
                .await
                .keys()
                .filter(|k| k.prefix_matches(prefix))
                .cloned()
                .collect()
        } else {
            self.storage.read().await.keys().cloned().collect()
        };

        Ok(futures::stream::once(async move { Ok(list) }).boxed())
    }

    /// The memory implementation returns all results, as opposed to the cloud
    /// versions which limit their results to 1k or more because of API
    /// limitations.
    async fn list_with_delimiter(&self, prefix: &Path) -> Result<ListResult> {
        let mut common_prefixes = BTreeSet::new();
        let last_modified = Utc::now();

        // Only objects in this base level should be returned in the
        // response. Otherwise, we just collect the common prefixes.
        let mut objects = vec![];
        for (k, v) in self.storage.read().await.range((prefix)..) {
            let mut parts = match k.prefix_match(prefix) {
                Some(parts) => parts,
                None => break,
            };

            // Pop first element
            let common_prefix = match parts.next() {
                Some(p) => p,
                None => continue,
            };

            if parts.next().is_some() {
                common_prefixes.insert(prefix.child(common_prefix));
            } else {
                let object = ObjectMeta {
                    location: k.clone(),
                    last_modified,
                    size: v.len(),
                };
                objects.push(object);
            }
        }

        Ok(ListResult {
            objects,
            common_prefixes: common_prefixes.into_iter().collect(),
            next_token: None,
        })
    }
}

impl InMemory {
    /// Create new in-memory storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a clone of the store
    pub async fn clone(&self) -> Self {
        let storage = self.storage.read().await;
        let storage = storage.clone();

        Self {
            storage: RwLock::new(storage),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        tests::{
            get_nonexistent_object, list_uses_directories_correctly, list_with_delimiter,
            put_get_delete_list,
        },
        Error as ObjectStoreError, ObjectStoreApi,
    };

    #[tokio::test]
    async fn in_memory_test() {
        let integration = InMemory::new();

        put_get_delete_list(&integration).await.unwrap();
        list_uses_directories_correctly(&integration).await.unwrap();
        list_with_delimiter(&integration).await.unwrap();
    }

    #[tokio::test]
    async fn unknown_length() {
        let integration = InMemory::new();

        let location = Path::from_raw("some_file");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();

        integration.put(&location, data).await.unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&*read_data, expected_data);
    }

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[tokio::test]
    async fn nonexistent_location() {
        let integration = InMemory::new();

        let location = Path::from_raw(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();
        if let Some(ObjectStoreError::NotFound { path, source }) =
            err.downcast_ref::<ObjectStoreError>()
        {
            let source_variant = source.downcast_ref::<Error>();
            assert!(
                matches!(source_variant, Some(Error::NoDataInMemory { .. }),),
                "got: {:?}",
                source_variant
            );
            assert_eq!(path, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type: {:?}", err);
        }
    }
}
