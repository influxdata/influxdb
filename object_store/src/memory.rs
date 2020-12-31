//! This module contains the IOx implementation for using memory as the object
//! store.
use crate::{DataDoesNotMatchLength, NoDataInMemory, Result, UnableToPutDataInMemory};
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use snafu::{ensure, OptionExt, ResultExt};
use std::{collections::BTreeMap, io};
use tokio::sync::RwLock;

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
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = TestError> = std::result::Result<T, E>;

    mod in_memory {
        use super::*;
        use crate::{tests::put_get_delete_list, Error, ObjectStore};
        use futures::stream;

        #[tokio::test]
        async fn in_memory_test() -> Result<()> {
            let integration = ObjectStore::new_in_memory(InMemory::new());

            put_get_delete_list(&integration).await?;
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
}
