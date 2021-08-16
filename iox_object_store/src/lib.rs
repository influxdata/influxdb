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

use bytes::{Bytes, BytesMut};
use data_types::{server_id::ServerId, DatabaseName};
use futures::{
    stream::{self, BoxStream},
    Stream, StreamExt, TryStreamExt,
};
use object_store::{path::Path, ObjectStore, ObjectStoreApi, Result};
use std::{io, sync::Arc};
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;

mod paths;
pub use paths::{
    parquet_file::{Path as ParquetFilePath, PathParseError as ParquetFilePathParseError},
    transaction_file::Path as TransactionFilePath,
};
use paths::{DataPath, RootPath, TransactionsPath};

const DB_RULES_FILE_NAME: &str = "rules.pb";

/// Handles persistence of data for a particular database. Writes within its directory/prefix.
///
/// This wrapper on top of an `ObjectStore` maps IOx specific concepts to ObjectStore locations
#[derive(Debug)]
pub struct IoxObjectStore {
    inner: Arc<ObjectStore>,
    server_id: ServerId,
    database_name: DatabaseName<'static>,
    root_path: RootPath,
    data_path: DataPath,
    transactions_path: TransactionsPath,
}

impl IoxObjectStore {
    /// Create a database-specific wrapper. Takes all the information needed to create the
    /// root directory of a database.
    pub fn new(
        inner: Arc<ObjectStore>,
        server_id: ServerId,
        database_name: &DatabaseName<'static>,
    ) -> Self {
        let root_path = RootPath::new(inner.new_path(), server_id, database_name);
        let data_path = DataPath::new(&root_path);
        let transactions_path = TransactionsPath::new(&root_path);
        Self {
            inner,
            server_id,
            database_name: database_name.to_owned(),
            root_path,
            data_path,
            transactions_path,
        }
    }

    /// The name of the database this object store is for.
    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    // Catalog transaction file methods ===========================================================

    /// List all the catalog transaction files in object storage for this database.
    pub async fn catalog_transaction_files(
        &self,
    ) -> Result<BoxStream<'static, Result<Vec<TransactionFilePath>>>> {
        Ok(self
            .list(Some(&self.transactions_path.inner))
            .await?
            .map_ok(move |list| {
                list.into_iter()
                    // This `flat_map` ignores any filename in the transactions_path we couldn't
                    // parse as a TransactionFilePath
                    .flat_map(TransactionFilePath::from_absolute)
                    .collect()
            })
            .boxed())
    }

    /// Get the catalog transaction data in this relative path in this database's object store.
    pub async fn get_catalog_transaction_file(
        &self,
        location: &TransactionFilePath,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        let full_path = self.transactions_path.join(location);

        self.inner.get(&full_path).await
    }

    /// Store the data for this parquet file in this database's object store.
    pub async fn put_catalog_transaction_file<S>(
        &self,
        location: &TransactionFilePath,
        bytes: S,
        length: Option<usize>,
    ) -> Result<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        let full_path = self.transactions_path.join(location);

        self.inner.put(&full_path, bytes, length).await
    }

    /// Delete all catalog transaction files for this database.
    pub async fn wipe_catalog(&self) -> Result<()> {
        let mut stream = self.catalog_transaction_files().await?;

        while let Some(transaction_file_list) = stream.try_next().await? {
            for transaction_file_path in &transaction_file_list {
                self.delete_catalog_transaction_file(transaction_file_path)
                    .await?;
            }
        }

        Ok(())
    }

    /// Remove the data for this catalog transaction file from this database's object store
    pub async fn delete_catalog_transaction_file(
        &self,
        location: &TransactionFilePath,
    ) -> Result<()> {
        let full_path = self.transactions_path.join(location);

        self.inner.delete(&full_path).await
    }

    // Parquet file methods =======================================================================

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

    /// Get the parquet file data in this relative path in this database's object store.
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

    // Database rule file methods =================================================================

    // Deliberately private; this should not leak outside this crate
    fn db_rules_path(&self) -> Path {
        self.root_path.join(DB_RULES_FILE_NAME)
    }

    /// Get the data for the database rules
    pub async fn get_database_rules_file(&self) -> Result<bytes::Bytes> {
        let mut stream = self.inner.get(&self.db_rules_path()).await?;
        let mut bytes = BytesMut::new();

        while let Some(buf) = stream.next().await {
            bytes.extend(buf?);
        }

        Ok(bytes.freeze())
    }

    /// Store the data for the database rules
    pub async fn put_database_rules_file(&self, bytes: bytes::Bytes) -> Result<()> {
        let len = bytes.len();
        let stream = stream::once(async move { Ok(bytes) });

        self.inner
            .put(&self.db_rules_path(), stream, Some(len))
            .await
    }

    /// Delete the data for the database rules
    pub async fn delete_database_rules_file(&self) -> Result<()> {
        self.inner.delete(&self.db_rules_path()).await
    }

    /// List the relative paths in this database's object store.
    ///
    /// Deliberately private!
    /// All outside calls should go to one of the more specific listing methods.
    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'static, Result<Vec<Path>>>> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::chunk_metadata::ChunkAddr;
    use object_store::{path::ObjectStorePath, ObjectStore, ObjectStoreApi};
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

    async fn catalog_transaction_files(
        iox_object_store: &IoxObjectStore,
    ) -> Vec<TransactionFilePath> {
        iox_object_store
            .catalog_transaction_files()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect()
    }

    async fn add_catalog_transaction_file(
        iox_object_store: &IoxObjectStore,
        location: &TransactionFilePath,
    ) {
        let data = Bytes::from("arbitrary data");
        let stream_data = std::io::Result::Ok(data.clone());

        iox_object_store
            .put_catalog_transaction_file(
                location,
                futures::stream::once(async move { stream_data }),
                None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn only_lists_relevant_catalog_transaction_files() {
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

        // Put a file in the database dir but not the transactions dir
        let mut path = object_store.new_path();
        path.push_dir(server_id.to_string());
        path.push_dir(database_name.to_string());
        path.set_file_name(&format!("{}.txn", uuid));
        add_file(&object_store, &path).await;

        // Put files in the transactions dir whose names are in the wrong format
        let mut path = object_store.new_path();
        path.push_dir(server_id.to_string());
        path.push_dir(database_name.to_string());
        path.set_file_name("111.parquet");
        add_file(&object_store, &path).await;
        path.set_file_name(&format!("{}.xls", uuid));
        add_file(&object_store, &path).await;

        // Catalog transaction files should be empty
        let ctf = catalog_transaction_files(&iox_object_store).await;
        assert!(ctf.is_empty(), "{:?}", ctf);

        // Add a real transaction file
        let t1 = TransactionFilePath::new_transaction(123, uuid);
        add_catalog_transaction_file(&iox_object_store, &t1).await;
        // Add a real checkpoint file
        let t2 = TransactionFilePath::new_checkpoint(123, uuid);
        add_catalog_transaction_file(&iox_object_store, &t2).await;

        // Only the real files should be returned
        let ctf = catalog_transaction_files(&iox_object_store).await;
        assert_eq!(ctf.len(), 2);
        assert!(ctf.contains(&t1));
        assert!(ctf.contains(&t2));
    }
}
