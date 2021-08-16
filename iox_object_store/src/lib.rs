#![deny(broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

//! Wraps the object_store crate with IOx-specific semantics.

// TODO: Create an IoxPath type and only take/return paths of those types, and wrap in the
// database's root path before sending to the underlying object_store.

use bytes::Bytes;
use data_types::{server_id::ServerId, DatabaseName};
use futures::{stream::BoxStream, Stream, StreamExt};
use object_store::{
    path::{parsed::DirsAndFileName, ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi, Result,
};
use std::{io, sync::Arc};
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;

/// Handles persistence of data for a particular database. Writes within its directory/prefix.
///
/// This wrapper on top of an `ObjectStore` maps IOx specific concepts to ObjectStore locations
#[derive(Debug)]
pub struct IoxObjectStore {
    inner: Arc<ObjectStore>,
    server_id: ServerId,
    database_name: String, // TODO: use data_types DatabaseName?
    root_path: RootPath,
}

impl IoxObjectStore {
    /// Create a database-specific wrapper. Takes all the information needed to create the
    /// root directory of a database.
    pub fn new(
        inner: Arc<ObjectStore>,
        server_id: ServerId,
        database_name: &DatabaseName<'_>,
    ) -> Self {
        let root_path = RootPath::new(inner.new_path(), server_id, database_name);
        Self {
            inner,
            server_id,
            database_name: database_name.into(),
            root_path,
        }
    }

    /// The name of the database this object store is for.
    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    /// Path where transactions are stored.
    ///
    /// The format is:
    ///
    /// ```text
    /// <server_id>/<db_name>/transactions/
    /// ```
    // TODO: avoid leaking this outside this crate
    pub fn catalog_path(&self) -> Path {
        let mut path = self.inner.new_path();
        path.push_dir(self.server_id.to_string());
        path.push_dir(&self.database_name);
        path.push_dir("transactions");
        path
    }

    /// Location where parquet data goes to.
    ///
    /// Schema currently is:
    ///
    /// ```text
    /// <server_id>/<db_name>/data/
    /// ```
    // TODO: avoid leaking this outside this crate
    pub fn data_path(&self) -> Path {
        let mut path = self.inner.new_path();
        path.push_dir(self.server_id.to_string());
        path.push_dir(&self.database_name);
        path.push_dir("data");
        path
    }

    /// Store this data in this database's object store.
    pub async fn put<S>(&self, location: &Path, bytes: S, length: Option<usize>) -> Result<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        self.inner.put(location, bytes, length).await
    }

    /// List all the catalog transaction files in object storage for this database.
    pub async fn catalog_transaction_files(&self) -> Result<BoxStream<'static, Result<Vec<Path>>>> {
        Ok(self.list(Some(&self.catalog_path())).await?.boxed())
    }

    /// List the relative paths in this database's object store.
    pub async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Result<BoxStream<'static, Result<Vec<Path>>>> {
        let (tx, rx) = channel(4);
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();

        // This is necessary because of the lifetime restrictions on the ObjectStoreApi trait's
        // methods, which might not actually be necessary but fixing it involves changes to the
        // cloud_storage crate that are longer term.
        tokio::spawn(async move {
            match inner.list(prefix.as_ref()).await {
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                }
                Ok(mut stream) => {
                    while let Some(list) = stream.next().await {
                        let _ = tx.send(list).await;
                    }
                }
            }
        });

        Ok(ReceiverStream::new(rx).boxed())
    }

    /// Get the data in this relative path in this database's object store.
    pub async fn get(&self, location: &Path) -> Result<BoxStream<'static, Result<Bytes>>> {
        self.inner.get(location).await
    }

    /// Delete the relative paths in this database's object store.
    pub async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    /// Create implementation-specific path from parsed representation.
    /// This might not be needed eventually
    pub fn path_from_dirs_and_filename(&self, path: DirsAndFileName) -> Path {
        self.inner.path_from_dirs_and_filename(path)
    }
}

/// A database-specific object store path that all `IoxPath`s should be within.
#[derive(Debug, Clone)]
struct RootPath {
    root: Path,
}

impl RootPath {
    /// How the root of a database is defined in object storage.
    fn new(mut root: Path, server_id: ServerId, database_name: &DatabaseName<'_>) -> Self {
        root.push_dir(server_id.to_string());
        root.push_dir(database_name.as_str());
        Self { root }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;

    /// Creates new test server ID
    fn make_server_id() -> ServerId {
        ServerId::new(NonZeroU32::new(1).unwrap())
    }

    /// Creates a new in-memory object store. These tests rely on the `Path`s being of type
    /// `DirsAndFileName` and thus using object_store::path::DELIMITER as the separator
    fn make_object_store() -> Arc<ObjectStore> {
        Arc::new(ObjectStore::new_in_memory())
    }

    #[test]
    fn catalog_path_is_relative_to_db_root() {
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let iox_object_store = IoxObjectStore::new(make_object_store(), server_id, &database_name);
        assert_eq!(
            iox_object_store.catalog_path().to_string(),
            "mem:1/clouds/transactions/"
        );
    }

    #[test]
    fn data_path_is_relative_to_db_root() {
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let iox_object_store = IoxObjectStore::new(make_object_store(), server_id, &database_name);
        assert_eq!(
            iox_object_store.data_path().to_string(),
            "mem:1/clouds/data/"
        );
    }
}
