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
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use object_store::{
    path::{parsed::DirsAndFileName, ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi, Result,
};
use std::{io, sync::Arc};
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;

mod paths;
use paths::{DataPath, RootPath};
pub use paths::{ParquetFilePath, ParquetFilePathParseError};

/// Handles persistence of data for a particular database. Writes within its directory/prefix.
///
/// This wrapper on top of an `ObjectStore` maps IOx specific concepts to ObjectStore locations
#[derive(Debug)]
pub struct IoxObjectStore {
    inner: Arc<ObjectStore>,
    server_id: ServerId,
    database_name: String, // TODO: use data_types DatabaseName?
    root_path: RootPath,
    data_path: DataPath,
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
        let data_path = DataPath::new(&root_path);
        Self {
            inner,
            server_id,
            database_name: database_name.into(),
            root_path,
            data_path,
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

    /// List all parquet file paths in object storage for this database.
    pub async fn parquet_files(&self) -> Result<BoxStream<'static, Result<Vec<ParquetFilePath>>>> {
        Ok(self
            .list(Some(&self.data_path.inner))
            .await?
            .map_ok(move |list| {
                list.into_iter()
                    // This `flat_map` ignores any filename in the data_path we couldn't parse as
                    // a ParquetFilePath
                    .flat_map(ParquetFilePath::from_absolute)
                    .collect()
            })
            .boxed())
    }

    /// Get the data in this relative path in this database's object store.
    pub async fn get_parquet_file(
        &self,
        location: &ParquetFilePath,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        let full_path = self.data_path.join(location);

        self.inner.get(&full_path).await
    }

    /// Store the data for this parquet file in this database's object store.
    pub async fn put_parquet_file<S>(
        &self,
        location: &ParquetFilePath,
        bytes: S,
        length: Option<usize>,
    ) -> Result<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        let full_path = self.data_path.join(location);

        self.inner.put(&full_path, bytes, length).await
    }

    /// Remove the data for this parquet file from this database's object store
    pub async fn delete_parquet_file(&self, location: &ParquetFilePath) -> Result<()> {
        let full_path = self.data_path.join(location);

        self.inner.delete(&full_path).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::chunk_metadata::ChunkAddr;
    use object_store::{ObjectStore, ObjectStoreApi};
    use std::num::NonZeroU32;
    use uuid::Uuid;

    /// Creates new test server ID
    fn make_server_id() -> ServerId {
        ServerId::new(NonZeroU32::new(1).unwrap())
    }

    /// Creates a new in-memory object store
    fn make_object_store() -> Arc<ObjectStore> {
        Arc::new(ObjectStore::new_in_memory())
    }

    async fn add_file(object_store: &ObjectStore, location: &Path) {
        let data = Bytes::from("arbitrary data");
        let stream_data = std::io::Result::Ok(data.clone());

        object_store
            .put(
                location,
                futures::stream::once(async move { stream_data }),
                None,
            )
            .await
            .unwrap();
    }

    async fn parquet_files(iox_object_store: &IoxObjectStore) -> Vec<ParquetFilePath> {
        iox_object_store
            .parquet_files()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect()
    }

    async fn add_parquet_file(iox_object_store: &IoxObjectStore, location: &ParquetFilePath) {
        let data = Bytes::from("arbitrary data");
        let stream_data = std::io::Result::Ok(data.clone());

        iox_object_store
            .put_parquet_file(
                location,
                futures::stream::once(async move { stream_data }),
                None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn only_lists_relevant_parquet_files() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let iox_object_store =
            IoxObjectStore::new(Arc::clone(&object_store), server_id, &database_name);
        let uuid = Uuid::new_v4();

        // Put a non-database file in
        let mut path = object_store.new_path();
        path.push_dir("foo");
        add_file(&object_store, &path).await;

        // Put a file for some other server in
        let mut path = object_store.new_path();
        path.push_dir("12345");
        add_file(&object_store, &path).await;

        // Put a file for some other database in
        let mut path = object_store.new_path();
        path.push_dir(server_id.to_string());
        path.push_dir("thunder");
        add_file(&object_store, &path).await;

        // Put a file in the database dir but not the data dir
        let mut path = object_store.new_path();
        path.push_dir(server_id.to_string());
        path.push_dir(database_name.to_string());
        path.set_file_name(&format!("111.{}.parquet", uuid));
        add_file(&object_store, &path).await;

        // Put files in the data dir whose names are in the wrong format
        let mut path = object_store.new_path();
        path.push_dir(server_id.to_string());
        path.push_dir(database_name.to_string());
        path.set_file_name("111.parquet");
        add_file(&object_store, &path).await;
        path.set_file_name(&format!("111.{}.xls", uuid));
        add_file(&object_store, &path).await;

        // Parquet files should be empty
        let pf = parquet_files(&iox_object_store).await;
        assert!(pf.is_empty(), "{:?}", pf);

        // Add a real parquet file
        let chunk_addr = ChunkAddr {
            db_name: "clouds".into(),
            table_name: "my_table".into(),
            partition_key: "my_partition".into(),
            chunk_id: 13,
        };
        let p1 = ParquetFilePath::new(&chunk_addr);
        add_parquet_file(&iox_object_store, &p1).await;

        // Only the real file should be returned
        let pf = parquet_files(&iox_object_store).await;
        assert_eq!(&pf, &[p1]);
    }
}
