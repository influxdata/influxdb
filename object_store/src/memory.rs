//! This module contains the IOx implementation for using memory as the object
//! store.
use crate::{
    path::parsed::DirsAndFileName, DataDoesNotMatchLength, ListResult, NoDataInMemory, ObjectMeta,
    Result, UnableToPutDataInMemory,
};
use bytes::Bytes;
use chrono::Utc;
use futures::{Stream, TryStreamExt};
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

    /// Return a new location path appropriate for this object storage
    pub fn new_path(&self) -> DirsAndFileName {
        DirsAndFileName::default()
    }

    /// Save the provided bytes to the specified location.
    pub async fn put<S>(&self, location: &DirsAndFileName, bytes: S, length: usize) -> Result<()>
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

    /// Return the bytes that are stored at the specified location.
    pub async fn get(
        &self,
        location: &DirsAndFileName,
    ) -> Result<impl Stream<Item = Result<Bytes>>> {
        let data = self
            .storage
            .read()
            .await
            .get(location)
            .cloned()
            .context(NoDataInMemory)?;

        Ok(futures::stream::once(async move { Ok(data) }))
    }

    /// Delete the object at the specified location.
    pub async fn delete(&self, location: &DirsAndFileName) -> Result<()> {
        self.storage.write().await.remove(location);
        Ok(())
    }

    /// List all the objects with the given prefix.
    pub async fn list<'a>(
        &'a self,
        prefix: Option<&'a DirsAndFileName>,
    ) -> Result<impl Stream<Item = Result<Vec<DirsAndFileName>>> + 'a> {
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

        Ok(futures::stream::once(async move { Ok(list) }))
    }

    /// List objects with the given prefix and a set delimiter of `/`. Returns
    /// common prefixes (directories) in addition to object metadata. The
    /// memory implementation returns all results, as opposed to the cloud
    /// versions which limit their results to 1k or more because of API
    /// limitations.
    pub async fn list_with_delimiter<'a>(
        &'a self,
        prefix: &'a DirsAndFileName,
    ) -> Result<ListResult<DirsAndFileName>> {
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

#[cfg(test)]
mod tests {
    use super::*;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = TestError> = std::result::Result<T, E>;

    use crate::{
        tests::{list_with_delimiter, put_get_delete_list},
        Error, ObjectStore, ObjectStorePath,
    };
    use futures::stream;

    #[tokio::test]
    async fn in_memory_test() -> Result<()> {
        let integration = ObjectStore::new_in_memory(InMemory::new());

        put_get_delete_list(&integration).await?;

        list_with_delimiter(&integration).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn length_mismatch_is_an_error() -> Result<()> {
        let integration = ObjectStore::new_in_memory(InMemory::new());

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
