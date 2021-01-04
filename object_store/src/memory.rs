//! This module contains the IOx implementation for using memory as the object
//! store.
use crate::{
    DataDoesNotMatchLength, ListResult, NoDataInMemory, ObjectMeta, Result, UnableToPutDataInMemory,
};
use bytes::Bytes;
use chrono::Utc;
use futures::{Stream, TryStreamExt};
use snafu::{ensure, OptionExt, ResultExt};
use std::collections::BTreeSet;
use std::{collections::BTreeMap, io};
use tokio::sync::RwLock;

// The delimiter to separate object namespaces, creating a directory structure.
const DELIMITER: &str = "/";

/// In-memory storage suitable for testing or for opting out of using a cloud
/// storage provider.
#[derive(Debug, Default)]
pub struct InMemory {
    storage: RwLock<BTreeMap<String, Bytes>>,
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

    /// Save the provided bytes to the specified location.
    pub async fn put<S>(&self, location: &str, bytes: S, length: usize) -> Result<()>
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
            .insert(location.to_string(), content);
        Ok(())
    }

    /// Return the bytes that are stored at the specified location.
    pub async fn get(&self, location: &str) -> Result<impl Stream<Item = Result<Bytes>>> {
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
    pub async fn delete(&self, location: &str) -> Result<()> {
        self.storage.write().await.remove(location);
        Ok(())
    }

    /// List all the objects with the given prefix.
    pub async fn list<'a>(
        &'a self,
        prefix: Option<&'a str>,
    ) -> Result<impl Stream<Item = Result<Vec<String>>> + 'a> {
        let list = if let Some(prefix) = prefix {
            self.storage
                .read()
                .await
                .keys()
                .filter(|k| k.starts_with(prefix))
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
        prefix: &'a str,
        _next_token: &Option<String>,
    ) -> Result<ListResult> {
        let mut common_prefixes = BTreeSet::new();
        let last_modified = Utc::now();

        // first ensure the prefix ends with the delimiter
        let prefix = if prefix.ends_with(DELIMITER) {
            prefix.to_string()
        } else {
            prefix.to_string() + DELIMITER
        };

        // set the end prefix so we pull back everything that starts with
        // the passed in prefix
        let mut end_prefix = prefix.clone();
        end_prefix.pop();
        end_prefix.push('0');

        // Only objects in this base level should be returned in the
        // response. Otherwise, we just collect the common prefixes.
        let mut objects = vec![];
        for (k, v) in self.storage.read().await.range(prefix.clone()..end_prefix) {
            let parts: Vec<_> = k
                .strip_prefix(&prefix)
                .expect("must have prefix if in range")
                .split(DELIMITER)
                .collect();

            if parts.len() >= 2 {
                let full_prefix = prefix.clone() + parts[0] + DELIMITER;
                common_prefixes.insert(full_prefix);
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

#[cfg(test)]
mod tests {
    use super::*;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = TestError> = std::result::Result<T, E>;

    use crate::{
        tests::{list_with_delimiter, put_get_delete_list},
        Error, ObjectStore,
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
        let res = integration.put("junk", bytes, 0).await;

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
