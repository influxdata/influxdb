//! This module contains the IOx implementation for using memory as the object
//! store.
use crate::cache::LocalFSCache;
use crate::{
    path::parsed::DirsAndFileName, ListResult, ObjectMeta, ObjectStoreApi, ObjectStorePath,
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::collections::BTreeSet;
use std::{collections::BTreeMap, io};
use tokio::sync::RwLock;

/// A specialized `Result` for in-memory object store-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A specialized `Error` for in-memory object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Expected streamed data to have length {}, got {}", expected, actual))]
    DataDoesNotMatchLength { expected: usize, actual: usize },

    #[snafu(display("Unable to stream data from the request into memory: {}", source))]
    UnableToStreamDataIntoMemory { source: std::io::Error },

    #[snafu(display("No data in memory found. Location: {}", location))]
    NoDataInMemory { location: String },
}

/// In-memory storage suitable for testing or for opting out of using a cloud
/// storage provider.
#[derive(Debug, Default)]
pub struct InMemory {
    storage: RwLock<BTreeMap<DirsAndFileName, Bytes>>,
}

#[async_trait]
impl ObjectStoreApi for InMemory {
    type Cache = LocalFSCache;
    type Path = DirsAndFileName;
    type Error = Error;

    fn new_path(&self) -> Self::Path {
        DirsAndFileName::default()
    }

    async fn put<S>(&self, location: &Self::Path, bytes: S, length: Option<usize>) -> Result<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        let content = bytes
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .context(UnableToStreamDataIntoMemory)?;

        if let Some(length) = length {
            ensure!(
                content.len() == length,
                DataDoesNotMatchLength {
                    actual: content.len(),
                    expected: length,
                }
            );
        }

        let content = content.freeze();

        self.storage
            .write()
            .await
            .insert(location.to_owned(), content);
        Ok(())
    }

    async fn get(&self, location: &Self::Path) -> Result<BoxStream<'static, Result<Bytes>>> {
        let data = self
            .storage
            .read()
            .await
            .get(location)
            .cloned()
            .context(NoDataInMemory {
                location: location.display(),
            })?;

        Ok(futures::stream::once(async move { Ok(data) }).boxed())
    }

    async fn delete(&self, location: &Self::Path) -> Result<()> {
        self.storage.write().await.remove(&location);
        Ok(())
    }

    async fn list<'a>(
        &'a self,
        prefix: Option<&'a Self::Path>,
    ) -> Result<BoxStream<'a, Result<Vec<Self::Path>>>> {
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
    async fn list_with_delimiter(&self, prefix: &Self::Path) -> Result<ListResult<Self::Path>> {
        let mut common_prefixes = BTreeSet::new();
        let last_modified = Utc::now();

        // Only objects in this base level should be returned in the
        // response. Otherwise, we just collect the common prefixes.
        let mut objects = vec![];
        for (k, v) in self
            .storage
            .read()
            .await
            .range((prefix)..)
            .take_while(|(k, _)| k.prefix_matches(prefix))
        {
            let parts = k
                .parts_after_prefix(prefix)
                .expect("must have prefix if in range");

            if parts.len() >= 2 {
                let mut full_prefix = prefix.to_owned();
                full_prefix.push_part_as_dir(&parts[0]);
                common_prefixes.insert(full_prefix);
            } else {
                let object = ObjectMeta {
                    location: k.to_owned(),
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

    fn cache(&self) -> Option<&Self::Cache> {
        todo!()
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
        tests::{list_with_delimiter, put_get_delete_list},
        Error as ObjectStoreError, ObjectStore, ObjectStoreApi, ObjectStorePath,
    };
    use futures::stream;

    #[tokio::test]
    async fn in_memory_test() {
        let integration = ObjectStore::new_in_memory();

        put_get_delete_list(&integration).await.unwrap();
        list_with_delimiter(&integration).await.unwrap();
    }

    #[tokio::test]
    async fn length_mismatch_is_an_error() {
        let integration = ObjectStore::new_in_memory();

        let bytes = stream::once(async { Ok(Bytes::from("hello world")) });
        let mut location = integration.new_path();
        location.set_file_name("junk");
        let res = integration.put(&location, bytes, Some(0)).await;

        assert!(matches!(
            res.err().unwrap(),
            ObjectStoreError::InMemoryObjectStoreError {
                source: Error::DataDoesNotMatchLength {
                    expected: 0,
                    actual: 11,
                }
            }
        ));
    }

    #[tokio::test]
    async fn unknown_length() {
        let integration = ObjectStore::new_in_memory();

        let data = Bytes::from("arbitrary data");
        let stream_data = std::io::Result::Ok(data.clone());

        let mut location = integration.new_path();
        location.set_file_name("some_file");
        integration
            .put(
                &location,
                futures::stream::once(async move { stream_data }),
                None,
            )
            .await
            .unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .unwrap();
        assert_eq!(&*read_data, data);
    }
}
