//! Wraps the object_store crate with IOx-specific semantics. The main responsibility of this crate
//! is to be the single source of truth for the paths of files in object storage. There is a
//! specific path type for each IOx-specific reason an object storage file exists. Content of the
//! files is managed outside of this crate.

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use data_types::server_id::ServerId;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{path::Path, ObjectStore, ObjectStoreApi, Result};
use observability_deps::tracing::warn;
use snafu::{ensure, ResultExt, Snafu};
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

mod paths;
pub use paths::{
    parquet_file::{ParquetFilePath, ParquetFilePathParseError},
    transaction_file::TransactionFilePath,
};
use paths::{DataPath, RootPath, TombstonePath, TransactionsPath};

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum IoxObjectStoreError {
    #[snafu(display("{}", source))]
    UnderlyingObjectStoreError { source: object_store::Error },

    #[snafu(display("Cannot create database with UUID `{}`; it already exists", uuid))]
    DatabaseAlreadyExists { uuid: Uuid },

    #[snafu(display(
        "Cannot restore; there is already an active database with UUID `{}`",
        uuid
    ))]
    ActiveDatabaseAlreadyExists { uuid: Uuid },

    #[snafu(display("No rules found to load at {}", root_path))]
    NoRulesFound { root_path: RootPath },

    #[snafu(display("Database at {} is marked as deleted", root_path))]
    DatabaseDeleted { root_path: RootPath },

    #[snafu(display("Could not restore database with UUID `{}`: {}", uuid, source))]
    RestoreFailed {
        uuid: Uuid,
        source: object_store::Error,
    },
}

/// Handles persistence of data for a particular database. Writes within its directory/prefix.
///
/// This wrapper on top of an `ObjectStore` maps IOx specific concepts to ObjectStore locations
#[derive(Debug)]
pub struct IoxObjectStore {
    inner: Arc<ObjectStore>,
    server_id: ServerId,
    root_path: RootPath,
    tombstone_path: TombstonePath,
    data_path: DataPath,
    transactions_path: TransactionsPath,
}

impl IoxObjectStore {
    /// Get the data for the server config to determine the names and locations of the databases
    /// that this server owns.
    pub async fn get_server_config_file(inner: &ObjectStore, server_id: ServerId) -> Result<Bytes> {
        let path = paths::server_config_path(inner, server_id);
        let mut stream = inner.get(&path).await?;
        let mut bytes = BytesMut::new();

        while let Some(buf) = stream.next().await {
            bytes.extend(buf?);
        }

        Ok(bytes.freeze())
    }

    /// Store the data for the server config with the names and locations of the databases
    /// that this server owns.
    pub async fn put_server_config_file(
        inner: &ObjectStore,
        server_id: ServerId,
        bytes: Bytes,
    ) -> Result<()> {
        let path = paths::server_config_path(inner, server_id);
        inner.put(&path, bytes).await
    }

    /// Return the path to the server config file to be used in database ownership information to
    /// identify the current server that a database thinks is its owner.
    pub fn server_config_path(inner: &ObjectStore, server_id: ServerId) -> Path {
        paths::server_config_path(inner, server_id)
    }

    /// Returns what the root path would be for a given database. Does not check existence or
    /// validity of the path in object storage.
    pub fn root_path_for(inner: &ObjectStore, server_id: ServerId, uuid: Uuid) -> RootPath {
        RootPath::new(inner, server_id, uuid)
    }

    // Private function to check if a given database has been deleted or not. If the database is
    // active, this returns `None`. If the database has been deleted, this returns
    // `Some(deleted_at)` where `deleted_at` is the time at which the database was deleted.
    async fn check_deleted(
        inner: &ObjectStore,
        root_path: &RootPath,
    ) -> Result<Option<DateTime<Utc>>> {
        let list_result = inner.list_with_delimiter(&root_path.inner).await?;

        let tombstone_file = root_path.tombstone_path();
        let deleted_at = list_result
            .objects
            .into_iter()
            .find(|object| object.location == tombstone_file.inner)
            .map(|object| object.last_modified);

        Ok(deleted_at)
    }

    /// Create a database-specific wrapper. Takes all the information needed to create a new
    /// root directory of a database. Checks that there isn't already anything in this database's
    /// directory in object storage.
    ///
    /// Caller *MUST* ensure there is at most 1 concurrent call of this function with the same
    /// parameters; this function does *NOT* do any locking.
    pub async fn create(
        inner: Arc<ObjectStore>,
        server_id: ServerId,
        uuid: Uuid,
    ) -> Result<Self, IoxObjectStoreError> {
        let root_path = Self::root_path_for(&inner, server_id, uuid);

        let list_result = inner
            .list_with_delimiter(&root_path.inner)
            .await
            .context(UnderlyingObjectStoreError)?;

        ensure!(
            list_result.objects.is_empty(),
            DatabaseAlreadyExists { uuid }
        );

        Ok(Self::existing(inner, server_id, root_path))
    }

    /// Look in object storage for an existing, active database with this UUID.
    pub async fn load(
        inner: Arc<ObjectStore>,
        server_id: ServerId,
        uuid: Uuid,
    ) -> Result<Self, IoxObjectStoreError> {
        let root_path = Self::root_path_for(&inner, server_id, uuid);

        Self::find(inner, server_id, root_path).await
    }

    /// Look in object storage for an existing database with this name and the given root path
    /// that was retrieved from a server config
    pub async fn load_at_root_path(
        inner: Arc<ObjectStore>,
        server_id: ServerId,
        root_path_str: &str,
    ) -> Result<Self, IoxObjectStoreError> {
        let root_path = RootPath::from_str(&inner, root_path_str);

        Self::find(inner, server_id, root_path).await
    }

    async fn find(
        inner: Arc<ObjectStore>,
        server_id: ServerId,
        root_path: RootPath,
    ) -> Result<Self, IoxObjectStoreError> {
        let list_result = inner
            .list_with_delimiter(&root_path.inner)
            .await
            .context(UnderlyingObjectStoreError)?;

        let rules_file = root_path.rules_path();
        let rules_exists = list_result
            .objects
            .iter()
            .any(|object| object.location == rules_file.inner);

        ensure!(rules_exists, NoRulesFound { root_path });

        let tombstone_file = root_path.tombstone_path();
        let tombstone_exists = list_result
            .objects
            .iter()
            .any(|object| object.location == tombstone_file.inner);

        ensure!(!tombstone_exists, DatabaseDeleted { root_path });

        Ok(Self::existing(inner, server_id, root_path))
    }

    /// Access the database-specific object storage files for an existing database that has
    /// already been located and verified to be active. Does not check object storage.
    fn existing(inner: Arc<ObjectStore>, server_id: ServerId, root_path: RootPath) -> Self {
        let tombstone_path = root_path.tombstone_path();
        let data_path = root_path.data_path();
        let transactions_path = root_path.transactions_path();

        Self {
            inner,
            server_id,
            root_path,
            tombstone_path,
            data_path,
            transactions_path,
        }
    }

    /// In the database's root directory, write out a file pointing to the server's config. This
    /// data can serve as an extra check on which server owns this database.
    pub async fn put_owner_file(&self, bytes: Bytes) -> Result<()> {
        let owner_path = self.root_path.owner_path();

        self.inner.put(&owner_path, bytes).await
    }

    /// Return the contents of the owner file in the database's root directory that provides
    /// information on the server that owns this database.
    pub async fn get_owner_file(&self) -> Result<Bytes> {
        let owner_path = self.root_path.owner_path();

        let mut stream = self.inner.get(&owner_path).await?;
        let mut bytes = BytesMut::new();

        while let Some(buf) = stream.next().await {
            bytes.extend(buf?);
        }

        Ok(bytes.freeze())
    }

    /// The location in object storage for all files for this database, suitable for logging or
    /// debugging purposes only. Do not parse this, as its format is subject to change!
    pub fn debug_database_path(&self) -> String {
        self.root_path.inner.to_string()
    }

    /// The possibly valid location in object storage for this database. Suitable for serialization
    /// to use during initial database load, but not parsing for semantic meaning, as its format is
    /// subject to change!
    pub fn root_path(&self) -> String {
        self.root_path.to_string()
    }

    // Deliberately private; this should not leak outside this crate
    // so assumptions about the object store organization are confined
    // (and can be changed) in this crate
    fn tombstone_path(&self) -> Path {
        self.root_path.tombstone_path().inner
    }

    /// Write the file in the database directory that indicates this database is marked as deleted,
    /// without yet actually deleting this directory or any files it contains in object storage.
    pub async fn write_tombstone(&self) -> Result<()> {
        self.inner.put(&self.tombstone_path(), Bytes::new()).await
    }

    /// Remove the tombstone file to restore a database. Will return an error if this database is
    /// already active. Returns the reactivated IoxObjectStore.
    pub async fn restore_database(
        inner: Arc<ObjectStore>,
        server_id: ServerId,
        uuid: Uuid,
    ) -> Result<Self, IoxObjectStoreError> {
        let root_path = Self::root_path_for(&inner, server_id, uuid);

        let deleted_at = Self::check_deleted(&inner, &root_path)
            .await
            .context(UnderlyingObjectStoreError)?;

        ensure!(deleted_at.is_some(), ActiveDatabaseAlreadyExists { uuid });

        let tombstone_path = root_path.tombstone_path();

        inner
            .delete(&tombstone_path.inner)
            .await
            .context(RestoreFailed { uuid })?;

        Ok(Self::existing(inner, server_id, root_path))
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
    pub async fn put_catalog_transaction_file(
        &self,
        location: &TransactionFilePath,
        bytes: Bytes,
    ) -> Result<()> {
        let full_path = self.transactions_path.join(location);

        self.inner.put(&full_path, bytes).await
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
    pub async fn put_parquet_file(&self, location: &ParquetFilePath, bytes: Bytes) -> Result<()> {
        let full_path = self.data_path.join(location);

        self.inner.put(&full_path, bytes).await
    }

    /// Remove the data for this parquet file from this database's object store
    pub async fn delete_parquet_file(&self, location: &ParquetFilePath) -> Result<()> {
        let full_path = self.data_path.join(location);

        self.inner.delete(&full_path).await
    }

    // Database rule file methods =================================================================

    // Deliberately private; this should not leak outside this crate
    // so assumptions about the object store organization are confined
    // (and can be changed) in this crate
    fn db_rules_path(&self) -> Path {
        self.root_path.rules_path().inner
    }

    /// Get the data for the database rules
    pub async fn get_database_rules_file(&self) -> Result<Bytes> {
        let mut stream = self.inner.get(&self.db_rules_path()).await?;
        let mut bytes = BytesMut::new();

        while let Some(buf) = stream.next().await {
            bytes.extend(buf?);
        }

        Ok(bytes.freeze())
    }

    /// Store the data for the database rules
    pub async fn put_database_rules_file(&self, bytes: Bytes) -> Result<()> {
        self.inner.put(&self.db_rules_path(), bytes).await
    }

    /// Delete the data for the database rules
    pub async fn delete_database_rules_file(&self) -> Result<()> {
        self.inner.delete(&self.db_rules_path()).await
    }

    /// List the relative paths in this database's object store.
    ///
    // Deliberately private; this should not leak outside this crate
    // so assumptions about the object store organization are confined
    // (and can be changed) in this crate
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
    use data_types::chunk_metadata::{ChunkAddr, ChunkId};
    use object_store::{parsed_path, path::ObjectStorePath, ObjectStore, ObjectStoreApi};
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

        object_store.put(location, data).await.unwrap();
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

        iox_object_store
            .put_parquet_file(location, data)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn only_lists_relevant_parquet_files() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let server_id_string = server_id.to_string();
        let server_id_str = server_id_string.as_str();
        let uuid = Uuid::new_v4();
        let uuid_string = uuid.to_string();
        let uuid_str = uuid_string.as_str();
        let iox_object_store = IoxObjectStore::create(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap();
        let parquet_uuid = Uuid::new_v4();
        let good_filename = format!("111.{}.parquet", parquet_uuid);
        let good_filename_str = good_filename.as_str();

        // Put a non-database file in
        let path = object_store.path_from_dirs_and_filename(parsed_path!(["foo"]));
        add_file(&object_store, &path).await;

        // Put a file for some other server in
        let path = object_store.path_from_dirs_and_filename(parsed_path!(["12345"]));
        add_file(&object_store, &path).await;

        // Put a file for some other database in
        let other_db_uuid = Uuid::new_v4().to_string();
        let path = object_store
            .path_from_dirs_and_filename(parsed_path!([server_id_str, other_db_uuid.as_str()]));
        add_file(&object_store, &path).await;

        // Put a file in the database dir but not the data dir
        let path = object_store.path_from_dirs_and_filename(parsed_path!(
            [server_id_str, uuid_str],
            good_filename_str
        ));
        add_file(&object_store, &path).await;

        // Put files in the data dir whose names are in the wrong format
        let mut path = object_store.path_from_dirs_and_filename(parsed_path!(
            [server_id_str, uuid_str, "data"],
            "111.parquet"
        ));
        add_file(&object_store, &path).await;
        path.set_file_name(&format!("111.{}.xls", parquet_uuid));
        add_file(&object_store, &path).await;

        // Parquet files should be empty
        let pf = parquet_files(&iox_object_store).await;
        assert!(pf.is_empty(), "{:?}", pf);

        // Add a real parquet file
        let chunk_addr = ChunkAddr {
            db_name: "clouds".into(),
            table_name: "my_table".into(),
            partition_key: "my_partition".into(),
            chunk_id: ChunkId::new_test(13),
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

        iox_object_store
            .put_catalog_transaction_file(location, data)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn only_lists_relevant_catalog_transaction_files() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let server_id_string = server_id.to_string();
        let server_id_str = server_id_string.as_str();
        let uuid = Uuid::new_v4();
        let uuid_string = uuid.to_string();
        let uuid_str = uuid_string.as_str();
        let iox_object_store = IoxObjectStore::create(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap();
        let txn_uuid = Uuid::new_v4();
        let good_txn_filename = format!("{}.txn", txn_uuid);
        let good_txn_filename_str = good_txn_filename.as_str();

        // Put a non-database file in
        let path = object_store.path_from_dirs_and_filename(parsed_path!(["foo"]));
        add_file(&object_store, &path).await;

        // Put a file for some other server in
        let path = object_store.path_from_dirs_and_filename(parsed_path!(["12345"]));
        add_file(&object_store, &path).await;

        // Put a file for some other database in
        let other_db_uuid = Uuid::new_v4().to_string();
        let path = object_store
            .path_from_dirs_and_filename(parsed_path!([server_id_str, other_db_uuid.as_str()]));
        add_file(&object_store, &path).await;

        // Put a file in the database dir but not the transactions dir
        let path = object_store.path_from_dirs_and_filename(parsed_path!(
            [server_id_str, uuid_str],
            good_txn_filename_str
        ));
        add_file(&object_store, &path).await;

        // Put files in the transactions dir whose names are in the wrong format
        let mut path = object_store
            .path_from_dirs_and_filename(parsed_path!([server_id_str, uuid_str], "111.parquet"));
        add_file(&object_store, &path).await;
        path.set_file_name(&format!("{}.xls", txn_uuid));
        add_file(&object_store, &path).await;

        // Catalog transaction files should be empty
        let ctf = catalog_transaction_files(&iox_object_store).await;
        assert!(ctf.is_empty(), "{:?}", ctf);

        // Add a real transaction file
        let t1 = TransactionFilePath::new_transaction(123, txn_uuid);
        add_catalog_transaction_file(&iox_object_store, &t1).await;
        // Add a real checkpoint file
        let t2 = TransactionFilePath::new_checkpoint(123, txn_uuid);
        add_catalog_transaction_file(&iox_object_store, &t2).await;

        // Only the real files should be returned
        let ctf = catalog_transaction_files(&iox_object_store).await;
        assert_eq!(ctf.len(), 2);
        assert!(ctf.contains(&t1));
        assert!(ctf.contains(&t2));
    }

    fn make_db_rules_path(object_store: &ObjectStore, server_id: ServerId, uuid: Uuid) -> Path {
        let mut p = object_store.new_path();
        p.push_all_dirs(&[server_id.to_string().as_str(), uuid.to_string().as_str()]);
        p.set_file_name("rules.pb");
        p
    }

    #[tokio::test]
    async fn db_rules_should_be_a_file() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let uuid = Uuid::new_v4();
        let rules_path = make_db_rules_path(&object_store, server_id, uuid);
        let iox_object_store = IoxObjectStore::create(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap();

        // PUT
        let original_file_content = Bytes::from("hello world");
        iox_object_store
            .put_database_rules_file(original_file_content.clone())
            .await
            .unwrap();

        let actual_content = object_store
            .get(&rules_path)
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap();

        assert_eq!(original_file_content, actual_content);

        // GET
        let updated_file_content = Bytes::from("goodbye moon");
        let expected_content = updated_file_content.clone();

        object_store
            .put(&rules_path, updated_file_content)
            .await
            .unwrap();

        let actual_content = iox_object_store.get_database_rules_file().await.unwrap();

        assert_eq!(expected_content, actual_content);

        // DELETE
        iox_object_store.delete_database_rules_file().await.unwrap();

        let file_count = object_store
            .list(None)
            .await
            .unwrap()
            .try_fold(0, |a, paths| async move { Ok(a + paths.len()) })
            .await
            .unwrap();

        assert_eq!(file_count, 0);
    }

    fn make_owner_path(object_store: &ObjectStore, server_id: ServerId, uuid: Uuid) -> Path {
        let mut p = object_store.new_path();
        p.push_all_dirs(&[server_id.to_string().as_str(), uuid.to_string().as_str()]);
        p.set_file_name("owner.pb");
        p
    }

    #[tokio::test]
    async fn owner_should_be_a_file() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let uuid = Uuid::new_v4();
        let owner_path = make_owner_path(&object_store, server_id, uuid);
        let iox_object_store = IoxObjectStore::create(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap();

        // PUT
        let original_file_content = Bytes::from("hello world");
        iox_object_store
            .put_owner_file(original_file_content.clone())
            .await
            .unwrap();

        let actual_content = object_store
            .get(&owner_path)
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap();

        assert_eq!(original_file_content, actual_content);

        // GET
        let updated_file_content = Bytes::from("goodbye moon");
        let expected_content = updated_file_content.clone();

        object_store
            .put(&owner_path, updated_file_content)
            .await
            .unwrap();

        let actual_content = iox_object_store.get_owner_file().await.unwrap();

        assert_eq!(expected_content, actual_content);
    }

    #[tokio::test]
    async fn write_tombstone_twice_is_fine() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let uuid = Uuid::new_v4();
        let iox_object_store = IoxObjectStore::create(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap();

        // tombstone file should not exist
        let mut tombstone = object_store.new_path();
        tombstone.push_all_dirs([server_id.to_string().as_str(), uuid.to_string().as_str()]);
        tombstone.set_file_name("DELETED");

        object_store.get(&tombstone).await.err().unwrap();

        iox_object_store.write_tombstone().await.unwrap();

        // tombstone file should exist
        object_store.get(&tombstone).await.unwrap();

        // deleting again should still succeed
        iox_object_store.write_tombstone().await.unwrap();

        // tombstone file should still exist
        object_store.get(&tombstone).await.unwrap();
    }

    #[tokio::test]
    async fn create_new_with_same_uuid_errors() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let uuid = Uuid::new_v4();

        let iox_object_store = IoxObjectStore::create(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap();

        iox_object_store
            .put_database_rules_file(Bytes::new())
            .await
            .unwrap();

        let err = IoxObjectStore::create(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                IoxObjectStoreError::DatabaseAlreadyExists { uuid: err_uuid } if err_uuid == uuid),
            "got: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn create_new_with_any_files_under_uuid_errors() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let uuid = Uuid::new_v4();

        let mut not_rules_path = object_store.new_path();
        not_rules_path.push_all_dirs(&[server_id.to_string().as_str(), uuid.to_string().as_str()]);
        not_rules_path.set_file_name("not_rules.txt");
        object_store
            .put(&not_rules_path, Bytes::new())
            .await
            .unwrap();

        let err = IoxObjectStore::create(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                IoxObjectStoreError::DatabaseAlreadyExists { uuid: err_uuid } if err_uuid == uuid),
            "got: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn delete_then_create_new_with_same_uuid_errors() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let uuid = Uuid::new_v4();

        let iox_object_store = IoxObjectStore::create(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap();

        iox_object_store
            .put_database_rules_file(Bytes::new())
            .await
            .unwrap();
        iox_object_store.write_tombstone().await.unwrap();

        let err = IoxObjectStore::create(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                IoxObjectStoreError::DatabaseAlreadyExists { uuid: err_uuid } if err_uuid == uuid),
            "got: {:?}",
            err
        );
    }

    async fn create_database(
        object_store: Arc<ObjectStore>,
        server_id: ServerId,
        uuid: Uuid,
    ) -> IoxObjectStore {
        let iox_object_store = IoxObjectStore::create(Arc::clone(&object_store), server_id, uuid)
            .await
            .unwrap();

        iox_object_store
            .put_database_rules_file(Bytes::new())
            .await
            .unwrap();

        iox_object_store
    }

    async fn delete_database(iox_object_store: &IoxObjectStore) {
        iox_object_store.write_tombstone().await.unwrap();
    }

    async fn restore_database(
        object_store: Arc<ObjectStore>,
        server_id: ServerId,
        uuid: Uuid,
    ) -> Result<IoxObjectStore, IoxObjectStoreError> {
        IoxObjectStore::restore_database(Arc::clone(&object_store), server_id, uuid).await
    }

    #[tokio::test]
    async fn restore_deleted_database() {
        let object_store = make_object_store();
        let server_id = make_server_id();

        // Create a database
        let db = Uuid::new_v4();
        let db_iox_store = create_database(Arc::clone(&object_store), server_id, db).await;

        // Delete the database
        delete_database(&db_iox_store).await;

        assert!(
            IoxObjectStore::check_deleted(&object_store, &db_iox_store.root_path)
                .await
                .unwrap()
                .is_some()
        );

        // Restore the database
        restore_database(Arc::clone(&object_store), server_id, db)
            .await
            .unwrap();

        assert!(
            IoxObjectStore::check_deleted(&object_store, &db_iox_store.root_path)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_load() {
        let object_store = make_object_store();
        let server_id = make_server_id();

        // Load can't find nonexistent database
        let nonexistent = Uuid::new_v4();
        let returned = IoxObjectStore::load(Arc::clone(&object_store), server_id, nonexistent)
            .await
            .unwrap_err();
        assert!(
            matches!(returned, IoxObjectStoreError::NoRulesFound { .. }),
            "got: {:?}",
            returned
        );

        // Create a database
        let db = Uuid::new_v4();
        let db_iox_store = create_database(Arc::clone(&object_store), server_id, db).await;

        // Load should return that database
        let returned = IoxObjectStore::load(Arc::clone(&object_store), server_id, db)
            .await
            .unwrap();
        assert_eq!(returned.root_path(), format!("{}/{}/", server_id, db));

        // Delete a database
        delete_database(&db_iox_store).await;

        // Load can't find deleted database
        let returned = IoxObjectStore::load(Arc::clone(&object_store), server_id, db)
            .await
            .unwrap_err();
        assert!(
            matches!(returned, IoxObjectStoreError::DatabaseDeleted { .. }),
            "got: {:?}",
            returned
        );
    }

    #[tokio::test]
    async fn round_trip_through_object_store_root_path() {
        let object_store = make_object_store();
        let server_id = make_server_id();

        // Create a new iox object store that doesn't exist yet
        let uuid = Uuid::new_v4();
        let db_iox_store = create_database(Arc::clone(&object_store), server_id, uuid).await;

        // Save its root path as the server config would
        let saved_root_path = db_iox_store.root_path();

        // Simulate server restarting and reading the server config to construct iox object stores,
        // the database files in object storage should be found in the same root
        let restarted_iox_store = IoxObjectStore::load_at_root_path(
            Arc::clone(&object_store),
            server_id,
            &saved_root_path,
        )
        .await
        .unwrap();
        assert_eq!(db_iox_store.root_path(), restarted_iox_store.root_path());

        // This should also equal root_path_for, which can be constructed even if a database
        // hasn't been fully initialized yet
        let alternate = IoxObjectStore::root_path_for(&object_store, server_id, uuid).to_string();
        assert_eq!(alternate, saved_root_path);
    }
}
