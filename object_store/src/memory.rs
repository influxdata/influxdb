//! This module contains the IOx implementation for using memory as the object
//! store.
use crate::{
    path::{cloud::CloudPath, parsed::DirsAndFileName},
    ListResult, ObjectMeta, ObjectStoreApi,
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::{stream::BoxStream, StreamExt};
use snafu::{OptionExt, Snafu};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use tokio::sync::RwLock;

/// A specialized `Result` for in-memory object store-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A specialized `Error` for in-memory object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
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
    type Path = DirsAndFileName;
    type Error = Error;

    fn new_path(&self) -> Self::Path {
        DirsAndFileName::default()
    }

    fn path_from_raw(&self, raw: &str) -> Self::Path {
        // Reuse the CloudPath parsing logic even though the in-memory storage isn't considered
        // to be cloud storage, necessarily
        let cloud_path = CloudPath::raw(raw);
        cloud_path.into()
    }

    async fn put(&self, location: &Self::Path, bytes: Bytes) -> Result<()> {
        self.storage
            .write()
            .await
            .insert(location.to_owned(), bytes);
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
                location: location.to_string(),
            })?;

        Ok(futures::stream::once(async move { Ok(data) }).boxed())
    }

    async fn delete(&self, location: &Self::Path) -> Result<()> {
        self.storage.write().await.remove(location);
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
            let parts = match k.parts_after_prefix(prefix) {
                Some(parts) => parts,
                None => continue,
            };

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
        ObjectStore, ObjectStoreApi, ObjectStorePath,
    };
    use futures::TryStreamExt;

    #[tokio::test]
    async fn in_memory_test() {
        let integration = ObjectStore::new_in_memory();

        put_get_delete_list(&integration).await.unwrap();
        list_with_delimiter(&integration).await.unwrap();
    }

    #[tokio::test]
    async fn unknown_length() {
        let integration = ObjectStore::new_in_memory();

        let mut location = integration.new_path();
        location.set_file_name("some_file");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();

        integration.put(&location, data).await.unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .unwrap();
        assert_eq!(&*read_data, expected_data);
    }
}
