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

//! Wraps the object_store crate with IOx-specific semantics. The main responsibility of this crate
//! is to be the single source of truth for the paths of files in object storage. There is a
//! specific path type for each IOx-specific reason an object storage file exists. Content of the
//! files is managed outside of this crate.

use bytes::{Bytes, BytesMut};
use data_types::{error::ErrorLogger, server_id::ServerId, DatabaseName};
use futures::{
    stream::{self, BoxStream},
    Stream, StreamExt, TryStreamExt,
};
use object_store::{
    path::{parsed::DirsAndFileName, ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi, Result,
};
use snafu::{ensure, ResultExt, Snafu};
use std::{io, sync::Arc};
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;

mod paths;
pub use paths::{
    parquet_file::{ParquetFilePath, ParquetFilePathParseError},
    transaction_file::TransactionFilePath,
};
use paths::{DataPath, GenerationPath, RootPath, TransactionsPath};

const DB_RULES_FILE_NAME: &str = "rules.pb";

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum IoxObjectStoreError {
    #[snafu(display("{}", source))]
    UnderlyingObjectStoreError { source: object_store::Error },

    #[snafu(display(
        "Cannot create database `{}`; it already exists with generation {}",
        name,
        generation
    ))]
    DatabaseAlreadyExists { name: String, generation: usize },

    #[snafu(display("Multiple active databases found in object storage"))]
    MultipleActiveDatabasesFound,
}

/// Handles persistence of data for a particular database. Writes within its directory/prefix.
///
/// This wrapper on top of an `ObjectStore` maps IOx specific concepts to ObjectStore locations
#[derive(Debug)]
pub struct IoxObjectStore {
    inner: Arc<ObjectStore>,
    server_id: ServerId,
    database_name: DatabaseName<'static>,
    generation_id: usize,
    generation_path: GenerationPath,
    data_path: DataPath,
    transactions_path: TransactionsPath,
}

#[derive(Debug, Copy, Clone)]
struct Generation {
    id: usize,
    deleted: bool,
}

impl Generation {
    fn is_active(&self) -> bool {
        !self.deleted
    }
}

impl IoxObjectStore {
    /// List database names in object storage that need to be further checked for generations
    /// and whether they're marked as deleted or not.
    pub async fn list_possible_databases(
        inner: &ObjectStore,
        server_id: ServerId,
    ) -> Result<Vec<DatabaseName<'static>>> {
        let path = paths::all_databases_path(inner, server_id);

        let list_result = inner.list_with_delimiter(&path).await?;

        Ok(list_result
            .common_prefixes
            .into_iter()
            .filter_map(|prefix| {
                let prefix_parsed: DirsAndFileName = prefix.into();
                let last = prefix_parsed
                    .directories
                    .last()
                    .expect("path can't be empty");
                let db_name = DatabaseName::new(last.encoded().to_string())
                    .log_if_error("invalid database directory")
                    .ok()?;

                Some(db_name)
            })
            .collect())
    }

    // TODO: Add a list_deleted_databases API that returns the names and generations of databases
    // that contain tombstone files
    // See: https://github.com/influxdata/influxdb_iox/issues/2198

    async fn list_generations(
        inner: &ObjectStore,
        root_path: &RootPath,
    ) -> Result<Vec<Generation>> {
        let list_result = inner.list_with_delimiter(&root_path.inner).await?;

        let mut generations = Vec::with_capacity(list_result.common_prefixes.len());

        for prefix in list_result.common_prefixes {
            let prefix_parsed: DirsAndFileName = prefix.clone().into();
            let id = prefix_parsed
                .directories
                .last()
                .expect("path can't be empty");

            // Deliberately ignoring errors with parsing; if the directory isn't a usize, it's
            // not a valid database generation directory and we should skip it.
            if let Ok(id) = id.to_string().parse() {
                // TODO: Check for tombstone file once we add a way to create the tombstone file
                // However, if we can't list the contents of a database directory, we can't
                // know if it's deleted or not, so this should be an error.
                // let generation_list_result = inner.list_with_delimiter(&prefix).await?;
                // prefix.push_dir(TOMBSTONE_FILE_NAME);
                // let deleted = generation_list_result
                //     .objects
                //     .into_iter()
                //     .any(|object| object.location == prefix);

                generations.push(Generation { id, deleted: false });
            }
        }

        Ok(generations)
    }

    /// Create a database-specific wrapper. Takes all the information needed to create a new
    /// root directory of a database. Checks that there isn't already an active database
    /// with this name in object storage.
    pub async fn new(
        inner: Arc<ObjectStore>,
        server_id: ServerId,
        database_name: &DatabaseName<'static>,
    ) -> Result<Self, IoxObjectStoreError> {
        let root_path = RootPath::new(&inner, server_id, database_name);

        let generations = Self::list_generations(&inner, &root_path)
            .await
            .context(UnderlyingObjectStoreError)?;

        let active = generations.iter().find(|g| g.is_active());
        if let Some(active) = active {
            return DatabaseAlreadyExists {
                name: database_name,
                generation: active.id,
            }
            .fail();
        }

        let next_generation = generations
            .iter()
            .map(|g| g.id)
            .max()
            .map(|max| max + 1)
            .unwrap_or(0);

        let generation_path = root_path.generation_path(next_generation);
        let data_path = generation_path.data_path();
        let transactions_path = generation_path.transactions_path();

        Ok(Self {
            inner,
            server_id,
            database_name: database_name.to_owned(),
            generation_id: next_generation,
            generation_path,
            data_path,
            transactions_path,
        })
    }

    /// Look in object storage for an existing database with this name and a non-deleted
    /// generation, and error if none is found.
    pub async fn find_existing(
        inner: Arc<ObjectStore>,
        server_id: ServerId,
        database_name: &DatabaseName<'static>,
    ) -> Result<Option<Self>, IoxObjectStoreError> {
        let root_path = RootPath::new(&inner, server_id, database_name);

        let generations = Self::list_generations(&inner, &root_path)
            .await
            .context(UnderlyingObjectStoreError)?;

        let mut active_generations = generations.iter().filter(|g| g.is_active());

        let active = match active_generations.next() {
            Some(a) => a,
            None => return Ok(None),
        };

        ensure!(
            active_generations.next().is_none(),
            MultipleActiveDatabasesFound
        );

        Ok(Some(Self::existing(
            inner,
            server_id,
            database_name,
            active.id,
        )))
    }

    /// Access the database-specific object storage files for an existing database that has
    /// already been located and verified to be active. Does not check object storage.
    pub fn existing(
        inner: Arc<ObjectStore>,
        server_id: ServerId,
        database_name: &DatabaseName<'static>,
        generation_id: usize,
    ) -> Self {
        let root_path = RootPath::new(&inner, server_id, database_name);
        let generation_path = GenerationPath::new(&root_path, generation_id);
        let data_path = DataPath::new(&generation_path);
        let transactions_path = TransactionsPath::new(&generation_path);

        Self {
            inner,
            server_id,
            database_name: database_name.to_owned(),
            generation_id,
            generation_path,
            data_path,
            transactions_path,
        }
    }

    /// The name of the database this object store is for.
    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    /// The location in object storage for all files for this database, suitable for logging
    /// or debugging purposes only. Do not parse this, as its format is subject to change!
    pub fn debug_database_path(&self) -> String {
        self.generation_path.inner.to_string()
    }

    /// The generation id of this database instance.
    pub fn generation_id(&self) -> usize {
        self.generation_id
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
    // so assumptions about the object store organization are confined
    // (and can be changed) in this crate
    fn db_rules_path(&self) -> Path {
        let mut path = self.generation_path.inner.clone();
        path.set_file_name(DB_RULES_FILE_NAME);
        path
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
    use data_types::chunk_metadata::ChunkAddr;
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
        let server_id_string = server_id.to_string();
        let server_id_str = server_id_string.as_str();
        let database_name = DatabaseName::new("clouds").unwrap();
        let database_name_str = database_name.as_str();
        let iox_object_store =
            IoxObjectStore::new(Arc::clone(&object_store), server_id, &database_name)
                .await
                .unwrap();
        let uuid = Uuid::new_v4();
        let good_filename = format!("111.{}.parquet", uuid);
        let good_filename_str = good_filename.as_str();

        // Put a non-database file in
        let path = object_store.path_from_dirs_and_filename(parsed_path!(["foo"]));
        add_file(&object_store, &path).await;

        // Put a file for some other server in
        let path = object_store.path_from_dirs_and_filename(parsed_path!(["12345"]));
        add_file(&object_store, &path).await;

        // Put a file for some other database in
        let path =
            object_store.path_from_dirs_and_filename(parsed_path!([server_id_str, "thunder"]));
        add_file(&object_store, &path).await;

        // Put a file in the database dir but not the generation dir
        let path = object_store.path_from_dirs_and_filename(parsed_path!(
            [server_id_str, database_name_str],
            good_filename_str
        ));
        add_file(&object_store, &path).await;

        // Put a file in the generation dir but not the data dir
        let path = object_store.path_from_dirs_and_filename(parsed_path!(
            [server_id_str, database_name_str, "0"],
            good_filename_str
        ));
        add_file(&object_store, &path).await;

        // Put files in the data dir whose names are in the wrong format
        let mut path = object_store.path_from_dirs_and_filename(parsed_path!(
            [server_id_str, database_name_str, "0", "data"],
            "111.parquet"
        ));
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
        let server_id_string = server_id.to_string();
        let server_id_str = server_id_string.as_str();
        let database_name = DatabaseName::new("clouds").unwrap();
        let database_name_str = database_name.as_str();
        let iox_object_store =
            IoxObjectStore::new(Arc::clone(&object_store), server_id, &database_name)
                .await
                .unwrap();
        let uuid = Uuid::new_v4();
        let good_txn_filename = format!("{}.txn", uuid);
        let good_txn_filename_str = good_txn_filename.as_str();

        // Put a non-database file in
        let path = object_store.path_from_dirs_and_filename(parsed_path!(["foo"]));
        add_file(&object_store, &path).await;

        // Put a file for some other server in
        let path = object_store.path_from_dirs_and_filename(parsed_path!(["12345"]));
        add_file(&object_store, &path).await;

        // Put a file for some other database in
        let path =
            object_store.path_from_dirs_and_filename(parsed_path!([server_id_str, "thunder"]));
        add_file(&object_store, &path).await;

        // Put a file in the database dir but not the generation dir
        let path = object_store.path_from_dirs_and_filename(parsed_path!(
            [server_id_str, database_name_str],
            good_txn_filename_str
        ));
        add_file(&object_store, &path).await;

        // Put a file in the generation dir but not the transactions dir
        let path = object_store.path_from_dirs_and_filename(parsed_path!(
            [server_id_str, database_name_str, "0"],
            good_txn_filename_str
        ));
        add_file(&object_store, &path).await;

        // Put files in the transactions dir whose names are in the wrong format
        let mut path = object_store.path_from_dirs_and_filename(parsed_path!(
            [server_id_str, database_name_str, "0"],
            "111.parquet"
        ));
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

    fn make_db_rules_path(object_store: &ObjectStore, server_id: ServerId, db_name: &str) -> Path {
        let mut p = object_store.new_path();
        p.push_all_dirs(&[&server_id.to_string(), db_name, "0"]);
        p.set_file_name("rules.pb");
        p
    }

    #[tokio::test]
    async fn db_rules_should_be_a_file() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let rules_path = make_db_rules_path(&object_store, server_id, "clouds");
        let iox_object_store =
            IoxObjectStore::new(Arc::clone(&object_store), server_id, &database_name)
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
        let updated_file_stream = stream::once({
            let bytes = updated_file_content.clone();
            async move { Ok(bytes) }
        });
        object_store
            .put(&rules_path, updated_file_stream, None)
            .await
            .unwrap();

        let actual_content = iox_object_store.get_database_rules_file().await.unwrap();

        assert_eq!(updated_file_content, actual_content);

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
}
