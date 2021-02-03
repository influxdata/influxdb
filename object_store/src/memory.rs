//! This module contains the IOx implementation for using memory as the object
//! store.
use crate::{
    path::parsed::DirsAndFileName, DataDoesNotMatchLength, ListResult, NoDataInMemory, ObjectMeta,
    ObjectStoreApi, Result, UnableToPutDataInMemory,
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use snafu::{ensure, OptionExt, ResultExt};
use std::collections::BTreeSet;
use std::{collections::BTreeMap, io};
use tokio::sync::RwLock;

/// In-memory storage suitable for testing or for opting out of using a cloud
/// storage provider.
#[derive(Debug, Default)]
pub struct InMemory {
    storage: RwLock<BTreeMap<DirsAndFileName, Bytes>>,
}

#[async_trait]
impl ObjectStoreApi for InMemory {
    type Path = DirsAndFileName;

    fn new_path(&self) -> Self::Path {
        DirsAndFileName::default()
    }

    async fn put<S>(&self, location: &Self::Path, bytes: S, length: usize) -> Result<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        let content = bytes
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .context(UnableToPutDataInMemory)?;

        ensure!(
            content.len() == length,
            DataDoesNotMatchLength {
                actual: content.len(),
                expected: length,
            }
        );

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
            .context(NoDataInMemory)?;

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

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = TestError> = std::result::Result<T, E>;

    use crate::{
        tests::{list_with_delimiter, put_get_delete_list},
        Error, ObjectStoreApi, ObjectStorePath,
    };
    use futures::stream;

    #[tokio::test]
    async fn in_memory_test() -> Result<()> {
        let integration = InMemory::new();

        put_get_delete_list(&integration).await?;

        list_with_delimiter(&integration).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn length_mismatch_is_an_error() -> Result<()> {
        let integration = InMemory::new();

        let bytes = stream::once(async { Ok(Bytes::from("hello world")) });
        let mut location = integration.new_path();
        location.set_file_name("junk");
        let res = integration.put(&location, bytes, 0).await;

        assert!(matches!(
            res.err().unwrap(),
            Error::DataDoesNotMatchLength {
                expected: 0,
                actual: 11,
            }
        ));

        Ok(())
    }
}
