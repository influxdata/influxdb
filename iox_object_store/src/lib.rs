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

use bytes::Bytes;
use data_types::server_id::ServerId;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{path::Path, GetResult, ObjectStore, ObjectStoreApi, Result};
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
use paths::{DataPath, RootPath, TransactionsPath};

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum IoxObjectStoreError {
    #[snafu(display("{}", source))]
    UnderlyingObjectStoreError { source: object_store::Error },

    #[snafu(display("Cannot create database with UUID `{}`; it already exists", uuid))]
    DatabaseAlreadyExists { uuid: Uuid },

    #[snafu(display("No rules found to load at {}", root_path))]
    NoRulesFound { root_path: RootPath },
}

/// Handles persistence of data for a particular database. Writes within its directory/prefix.
///
/// This wrapper on top of an `ObjectStore` maps IOx specific concepts to ObjectStore locations
#[derive(Debug)]
pub struct IoxObjectStore {
    inner: Arc<ObjectStore>,
    root_path: RootPath,
    data_path: DataPath,
    transactions_path: TransactionsPath,
}

impl IoxObjectStore {
    /// Get the data for the server config to determine the names and locations of the databases
    /// that this server owns.
    ///
    /// TEMPORARY: Server config used to be at the top level instead of beneath `/nodes/`. Until
    /// all deployments have transitioned, check both locations before reporting that the server
    /// config is not found.
    pub async fn get_server_config_file(inner: &ObjectStore, server_id: ServerId) -> Result<Bytes> {
        let path = paths::server_config_path(inner, server_id);
        let result = match inner.get(&path).await {
            Err(object_store::Error::NotFound { .. }) => {
                use object_store::path::ObjectStorePath;
                let mut legacy_path = inner.new_path();
                legacy_path.push_dir(server_id.to_string());
                legacy_path.set_file_name(paths::SERVER_CONFIG_FILE_NAME);

                inner.get(&legacy_path).await
            }
            other => other,
        }?;

        Ok(result.bytes().await?.into())
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
    pub fn root_path_for(inner: &ObjectStore, uuid: Uuid) -> RootPath {
        RootPath::new(inner, uuid)
    }

    /// Create a database-specific wrapper. Takes all the information needed to create a new
    /// root directory of a database. Checks that there isn't already anything in this database's
    /// directory in object storage.
    ///
    /// Caller *MUST* ensure there is at most 1 concurrent call of this function with the same
    /// parameters; this function does *NOT* do any locking.
    pub async fn create(inner: Arc<ObjectStore>, uuid: Uuid) -> Result<Self, IoxObjectStoreError> {
        let root_path = Self::root_path_for(&inner, uuid);

        let list_result = inner
            .list_with_delimiter(&root_path.inner)
            .await
            .context(UnderlyingObjectStoreSnafu)?;

        ensure!(
            list_result.objects.is_empty(),
            DatabaseAlreadyExistsSnafu { uuid }
        );

        Ok(Self::existing(inner, root_path))
    }

    /// Look in object storage for an existing, active database with this UUID.
    pub async fn load(inner: Arc<ObjectStore>, uuid: Uuid) -> Result<Self, IoxObjectStoreError> {
        let root_path = Self::root_path_for(&inner, uuid);

        Self::find(inner, root_path).await
    }

    /// Look in object storage for an existing database with this name and the given root path
    /// that was retrieved from a server config
    pub async fn load_at_root_path(
        inner: Arc<ObjectStore>,
        root_path_str: &str,
    ) -> Result<Self, IoxObjectStoreError> {
        let root_path = RootPath::from_str(&inner, root_path_str);

        Self::find(inner, root_path).await
    }

    async fn find(
        inner: Arc<ObjectStore>,
        root_path: RootPath,
    ) -> Result<Self, IoxObjectStoreError> {
        let list_result = inner
            .list_with_delimiter(&root_path.inner)
            .await
            .context(UnderlyingObjectStoreSnafu)?;

        let rules_file = root_path.rules_path();
        let rules_exists = list_result
            .objects
            .iter()
            .any(|object| object.location == rules_file.inner);

        ensure!(rules_exists, NoRulesFoundSnafu { root_path });

        Ok(Self::existing(inner, root_path))
    }

    /// Access the database-specific object storage files for an existing database that has
    /// already been located and verified to be active. Does not check object storage.
    fn existing(inner: Arc<ObjectStore>, root_path: RootPath) -> Self {
        let data_path = root_path.data_path();
        let transactions_path = root_path.transactions_path();

        Self {
            inner,
            root_path,
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

        Ok(self.inner.get(&owner_path).await?.bytes().await?.into())
    }

    /// Delete owner file for testing
    pub async fn delete_owner_file_for_testing(&self) -> Result<()> {
        let owner_path = self.root_path.owner_path();

        self.inner.delete(&owner_path).await
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
    ) -> Result<GetResult<object_store::Error>> {
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
    ) -> Result<GetResult<object_store::Error>> {
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
        let path = &self.db_rules_path();
        Ok(self.inner.get(path).await?.bytes().await?.into())
    }

    /// Return the database rules file content without creating an IoxObjectStore instance. Useful
    /// when restoring a database given a UUID to check existence of the specified database and
    /// get information such as the database name from the rules before proceeding with restoring
    /// and initializing the database.
    pub async fn load_database_rules(inner: Arc<ObjectStore>, uuid: Uuid) -> Result<Bytes> {
        let root_path = Self::root_path_for(&inner, uuid);
        let db_rules_path = root_path.rules_path().inner;

        Ok(inner.get(&db_rules_path).await?.bytes().await?.into())
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
    use crate::paths::ALL_DATABASES_DIRECTORY;
    use data_types::chunk_metadata::{ChunkAddr, ChunkId};
    use object_store::{parsed_path, path::ObjectStorePath, ObjectStore, ObjectStoreApi};
    use test_helpers::assert_error;
    use uuid::Uuid;

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
        let uuid = Uuid::new_v4();
        let uuid_string = uuid.to_string();
        let uuid_str = uuid_string.as_str();
        let iox_object_store = IoxObjectStore::create(Arc::clone(&object_store), uuid)
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
        let path = object_store.path_from_dirs_and_filename(parsed_path!([
            ALL_DATABASES_DIRECTORY,
            other_db_uuid.as_str()
        ]));
        add_file(&object_store, &path).await;

        // Put a file in the database dir but not the data dir
        let path = object_store.path_from_dirs_and_filename(parsed_path!(
            [ALL_DATABASES_DIRECTORY, uuid_str],
            good_filename_str
        ));
        add_file(&object_store, &path).await;

        // Put files in the data dir whose names are in the wrong format
        let mut path = object_store.path_from_dirs_and_filename(parsed_path!(
            [ALL_DATABASES_DIRECTORY, uuid_str, "data"],
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
        let uuid = Uuid::new_v4();
        let uuid_string = uuid.to_string();
        let uuid_str = uuid_string.as_str();
        let iox_object_store = IoxObjectStore::create(Arc::clone(&object_store), uuid)
            .await
            .unwrap();
        let txn_uuid = Uuid::new_v4();
        let good_txn_filename = format!("{}.txn", txn_uuid);
        let good_txn_filename_str = good_txn_filename.as_str();

        // Put a non-database file in
        let path = object_store.path_from_dirs_and_filename(parsed_path!(["foo"]));
        add_file(&object_store, &path).await;

        // Put a file in a directory other than the databases directory
        let path = object_store.path_from_dirs_and_filename(parsed_path!(["12345"]));
        add_file(&object_store, &path).await;

        // Put a file for some other database in
        let other_db_uuid = Uuid::new_v4().to_string();
        let path = object_store.path_from_dirs_and_filename(parsed_path!([
            ALL_DATABASES_DIRECTORY,
            other_db_uuid.as_str()
        ]));
        add_file(&object_store, &path).await;

        // Put a file in the database dir but not the transactions dir
        let path = object_store.path_from_dirs_and_filename(parsed_path!(
            [ALL_DATABASES_DIRECTORY, uuid_str],
            good_txn_filename_str
        ));
        add_file(&object_store, &path).await;

        // Put files in the transactions dir whose names are in the wrong format
        let mut path = object_store.path_from_dirs_and_filename(parsed_path!(
            [ALL_DATABASES_DIRECTORY, uuid_str],
            "111.parquet"
        ));
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

    fn make_db_rules_path(object_store: &ObjectStore, uuid: Uuid) -> Path {
        let mut p = object_store.new_path();
        p.push_all_dirs(&[ALL_DATABASES_DIRECTORY, uuid.to_string().as_str()]);
        p.set_file_name("rules.pb");
        p
    }

    #[tokio::test]
    async fn db_rules_should_be_a_file() {
        let object_store = make_object_store();
        let uuid = Uuid::new_v4();
        let rules_path = make_db_rules_path(&object_store, uuid);
        let iox_object_store = IoxObjectStore::create(Arc::clone(&object_store), uuid)
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
            .bytes()
            .await
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

    fn make_owner_path(object_store: &ObjectStore, uuid: Uuid) -> Path {
        let mut p = object_store.new_path();
        p.push_all_dirs(&[ALL_DATABASES_DIRECTORY, uuid.to_string().as_str()]);
        p.set_file_name("owner.pb");
        p
    }

    #[tokio::test]
    async fn owner_should_be_a_file() {
        let object_store = make_object_store();
        let uuid = Uuid::new_v4();
        let owner_path = make_owner_path(&object_store, uuid);
        let iox_object_store = IoxObjectStore::create(Arc::clone(&object_store), uuid)
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
            .bytes()
            .await
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
    async fn create_new_with_same_uuid_errors() {
        let object_store = make_object_store();
        let uuid = Uuid::new_v4();

        let iox_object_store = IoxObjectStore::create(Arc::clone(&object_store), uuid)
            .await
            .unwrap();

        iox_object_store
            .put_database_rules_file(Bytes::new())
            .await
            .unwrap();

        assert_error!(
            IoxObjectStore::create(Arc::clone(&object_store), uuid).await,
            IoxObjectStoreError::DatabaseAlreadyExists { uuid: err_uuid } if err_uuid == uuid,
        );
    }

    #[tokio::test]
    async fn create_new_with_any_files_under_uuid_errors() {
        let object_store = make_object_store();
        let uuid = Uuid::new_v4();

        let mut not_rules_path = object_store.new_path();
        not_rules_path.push_all_dirs(&[ALL_DATABASES_DIRECTORY, uuid.to_string().as_str()]);
        not_rules_path.set_file_name("not_rules.txt");
        object_store
            .put(&not_rules_path, Bytes::new())
            .await
            .unwrap();

        assert_error!(
            IoxObjectStore::create(Arc::clone(&object_store), uuid).await,
            IoxObjectStoreError::DatabaseAlreadyExists { uuid: err_uuid } if err_uuid == uuid,
        );
    }

    async fn create_database(object_store: Arc<ObjectStore>, uuid: Uuid) -> IoxObjectStore {
        let iox_object_store = IoxObjectStore::create(Arc::clone(&object_store), uuid)
            .await
            .unwrap();

        iox_object_store
            .put_database_rules_file(Bytes::new())
            .await
            .unwrap();

        iox_object_store
    }

    #[tokio::test]
    async fn cant_read_rules_if_no_rules_exist() {
        let object_store = make_object_store();

        // Create a uuid but don't create a corresponding database
        let db = Uuid::new_v4();

        // This fails, there are no rules to read
        assert_error!(
            IoxObjectStore::load_database_rules(object_store, db).await,
            object_store::Error::NotFound { .. },
        );
    }

    #[tokio::test]
    async fn test_load() {
        let object_store = make_object_store();

        // Load can't find nonexistent database
        let nonexistent = Uuid::new_v4();
        assert_error!(
            IoxObjectStore::load(Arc::clone(&object_store), nonexistent).await,
            IoxObjectStoreError::NoRulesFound { .. },
        );

        // Create a database
        let db = Uuid::new_v4();
        create_database(Arc::clone(&object_store), db).await;

        // Load should return that database
        let returned = IoxObjectStore::load(Arc::clone(&object_store), db)
            .await
            .unwrap();
        assert_eq!(
            returned.root_path(),
            format!("{}/{}/", ALL_DATABASES_DIRECTORY, db)
        );
    }

    #[tokio::test]
    async fn round_trip_through_object_store_root_path() {
        let object_store = make_object_store();

        // Create a new iox object store that doesn't exist yet
        let uuid = Uuid::new_v4();
        let db_iox_store = create_database(Arc::clone(&object_store), uuid).await;

        // Save its root path as the server config would
        let saved_root_path = db_iox_store.root_path();

        // Simulate server restarting and reading the server config to construct iox object stores,
        // the database files in object storage should be found in the same root
        let restarted_iox_store =
            IoxObjectStore::load_at_root_path(Arc::clone(&object_store), &saved_root_path)
                .await
                .unwrap();
        assert_eq!(db_iox_store.root_path(), restarted_iox_store.root_path());

        // This should also equal root_path_for, which can be constructed even if a database
        // hasn't been fully initialized yet
        let alternate = IoxObjectStore::root_path_for(&object_store, uuid).to_string();
        assert_eq!(alternate, saved_root_path);
    }
}
