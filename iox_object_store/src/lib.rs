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

//! Wraps the object_store crate with IOx-specific semantics. The main responsibility of this crate
//! is to be the single source of truth for the paths of files in object storage. There is a
//! specific path type for each IOx-specific reason an object storage file exists. Content of the
//! files is managed outside of this crate.

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use data_types::{
    detailed_database::{DetailedDatabase, GenerationId},
    error::ErrorLogger,
    server_id::ServerId,
    DatabaseName,
};
use futures::{
    stream::{self, BoxStream},
    Stream, StreamExt, TryStreamExt,
};
use object_store::{
    path::{parsed::DirsAndFileName, ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi, Result,
};
use observability_deps::tracing::warn;
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{collections::BTreeMap, io, sync::Arc};
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;

mod paths;
pub use paths::{
    parquet_file::{ParquetFilePath, ParquetFilePathParseError},
    transaction_file::TransactionFilePath,
};
use paths::{DataPath, GenerationPath, RootPath, TombstonePath, TransactionsPath};

const DB_RULES_FILE_NAME: &str = "rules.pb";

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum IoxObjectStoreError {
    #[snafu(display("{}", source))]
    UnderlyingObjectStoreError { source: object_store::Error },

    #[snafu(display("Cannot create database `{}`; it already exists", name,))]
    DatabaseAlreadyExists { name: String },

    #[snafu(display("Multiple active databases found in object storage"))]
    MultipleActiveDatabasesFound,

    #[snafu(display("Cannot restore; there is already an active database named `{}`", name))]
    ActiveDatabaseAlreadyExists { name: String },

    #[snafu(display("Generation `{}` not found for database `{}`", generation_id, name))]
    GenerationNotFound {
        generation_id: GenerationId,
        name: String,
    },

    #[snafu(display(
        "Could not restore generation `{}` of database `{}`: {}",
        generation_id,
        name,
        source
    ))]
    RestoreFailed {
        generation_id: GenerationId,
        name: String,
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
    database_name: DatabaseName<'static>,
    generation_path: GenerationPath,
    data_path: DataPath,
    transactions_path: TransactionsPath,
}

/// Private information about a database's generation.
#[derive(Debug, Copy, Clone, PartialEq)]
struct Generation {
    id: GenerationId,
    deleted_at: Option<DateTime<Utc>>,
}

impl Generation {
    fn active(id: usize) -> Self {
        Self {
            id: GenerationId { inner: id },
            deleted_at: None,
        }
    }

    fn is_active(&self) -> bool {
        self.deleted_at.is_none()
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

    /// List databases marked as deleted in in object storage along with their generation IDs and
    /// when they were deleted. Enables a user to choose a generation for a database that they
    /// would want to restore or would want to delete permanently.
    pub async fn list_deleted_databases(
        inner: &ObjectStore,
        server_id: ServerId,
    ) -> Result<Vec<DetailedDatabase>> {
        Ok(Self::list_all_databases(inner, server_id)
            .await?
            .into_iter()
            .flat_map(|(name, generations)| {
                let name = Arc::new(name);
                generations.into_iter().filter_map(move |gen| {
                    let name = Arc::clone(&name);
                    gen.deleted_at.map(|_| DetailedDatabase {
                        name: (*name).clone(),
                        generation_id: gen.id,
                        deleted_at: gen.deleted_at,
                    })
                })
            })
            .collect())
    }

    /// List all databases in in object storage along with their generation IDs and if/when they
    /// were deleted. Useful for visibility into object storage and finding databases to restore or
    /// permanently delete.
    pub async fn list_detailed_databases(
        inner: &ObjectStore,
        server_id: ServerId,
    ) -> Result<Vec<DetailedDatabase>> {
        Ok(Self::list_all_databases(inner, server_id)
            .await?
            .into_iter()
            .flat_map(|(name, generations)| {
                let name = Arc::new(name);
                generations.into_iter().map(move |gen| {
                    let name = Arc::clone(&name);
                    DetailedDatabase {
                        name: (*name).clone(),
                        generation_id: gen.id,
                        deleted_at: gen.deleted_at,
                    }
                })
            })
            .collect())
    }

    /// List database names in object storage along with all existing generations for each database
    /// and whether the generations are marked as deleted or not. Useful for finding candidates
    /// to restore or to permanently delete. Makes many more calls to object storage than
    /// [`IoxObjectStore::list_possible_databases`].
    async fn list_all_databases(
        inner: &ObjectStore,
        server_id: ServerId,
    ) -> Result<BTreeMap<DatabaseName<'static>, Vec<Generation>>> {
        let path = paths::all_databases_path(inner, server_id);

        let list_result = inner.list_with_delimiter(&path).await?;

        let mut all_dbs = BTreeMap::new();

        for prefix in list_result.common_prefixes {
            let prefix_parsed: DirsAndFileName = prefix.into();
            let last = prefix_parsed
                .directories
                .last()
                .expect("path can't be empty");

            if let Ok(db_name) = DatabaseName::new(last.encoded().to_string())
                .log_if_error("invalid database directory")
            {
                let root_path = RootPath::new(inner, server_id, &db_name);
                let generations = Self::list_generations(inner, &root_path).await?;

                all_dbs.insert(db_name, generations);
            }
        }

        Ok(all_dbs)
    }

    // Private function to list all generation directories in object storage for a particular
    // database.
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

            if let Ok(id) = id.to_string().parse() {
                // If we can't list the contents of a database directory, we can't
                // know if it's deleted or not, so this should be an error.
                let generation_list_result = inner.list_with_delimiter(&prefix).await?;
                let tombstone_file = TombstonePath::new_from_object_store_path(&prefix);

                let deleted_at = generation_list_result
                    .objects
                    .into_iter()
                    .find(|object| object.location == tombstone_file.inner)
                    .map(|object| object.last_modified);

                generations.push(Generation { id, deleted_at });
            } else {
                // Deliberately ignoring errors with parsing; if the directory isn't a usize, it's
                // not a valid database generation directory and we should skip it.
                warn!("invalid generation directory found: {}", id);
            }
        }

        Ok(generations)
    }

    /// Create a database-specific wrapper. Takes all the information needed to create a new
    /// root directory of a database. Checks that there isn't already an active database
    /// with this name in object storage.
    ///
    /// Caller *MUST* ensure there is at most 1 concurrent call of this function with the same
    /// parameters; this function does *NOT* do any locking.
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

        ensure!(
            active.is_none(),
            DatabaseAlreadyExists {
                name: database_name
            }
        );

        let next_generation_id = generations
            .iter()
            .max_by_key(|g| g.id)
            .map(|max| max.id.inner + 1)
            .unwrap_or(0);

        Ok(Self::existing(
            inner,
            server_id,
            database_name,
            Generation::active(next_generation_id),
            root_path,
        ))
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
            Some(a) => *a,
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
            active,
            root_path,
        )))
    }

    /// Access the database-specific object storage files for an existing database that has
    /// already been located and verified to be active. Does not check object storage.
    fn existing(
        inner: Arc<ObjectStore>,
        server_id: ServerId,
        database_name: &DatabaseName<'static>,
        generation: Generation,
        root_path: RootPath,
    ) -> Self {
        let generation_path = root_path.generation_path(generation);
        let data_path = generation_path.data_path();
        let transactions_path = generation_path.transactions_path();

        Self {
            inner,
            server_id,
            database_name: database_name.to_owned(),
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

    // Deliberately private; this should not leak outside this crate
    // so assumptions about the object store organization are confined
    // (and can be changed) in this crate
    fn tombstone_path(&self) -> Path {
        TombstonePath::new(&self.generation_path).inner
    }

    /// Write the file in the database directory that indicates this database is marked as deleted,
    /// without yet actually deleting this directory or any files it contains in object storage.
    pub async fn write_tombstone(&self) -> Result<()> {
        let stream = stream::once(async move { Ok(Bytes::new()) });
        let len = 0;

        self.inner
            .put(&self.tombstone_path(), stream, Some(len))
            .await
    }

    /// Remove the tombstone file to restore a database generation. Will return an error if this
    /// generation is already active or if there is another database generation already active for
    /// this database name. Returns the reactivated IoxObjectStore.
    pub async fn restore_database(
        inner: Arc<ObjectStore>,
        server_id: ServerId,
        database_name: &DatabaseName<'static>,
        generation_id: GenerationId,
    ) -> Result<Self, IoxObjectStoreError> {
        let root_path = RootPath::new(&inner, server_id, database_name);

        let generations = Self::list_generations(&inner, &root_path)
            .await
            .context(UnderlyingObjectStoreError)?;

        let active = generations.iter().find(|g| g.is_active());

        ensure!(
            active.is_none(),
            ActiveDatabaseAlreadyExists {
                name: database_name
            }
        );

        let restore_candidate = generations
            .iter()
            .find(|g| g.id == generation_id && !g.is_active())
            .context(GenerationNotFound {
                generation_id,
                name: database_name.as_str(),
            })?;

        let generation_path = root_path.generation_path(*restore_candidate);
        let tombstone_path = TombstonePath::new(&generation_path);

        inner
            .delete(&tombstone_path.inner)
            .await
            .context(RestoreFailed {
                generation_id,
                name: database_name.as_str(),
            })?;

        Ok(Self::existing(
            inner,
            server_id,
            database_name,
            Generation::active(generation_id.inner),
            root_path,
        ))
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

    #[tokio::test]
    async fn write_tombstone_twice_is_fine() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let iox_object_store =
            IoxObjectStore::new(Arc::clone(&object_store), server_id, &database_name)
                .await
                .unwrap();

        // tombstone file should not exist
        let mut tombstone = object_store.new_path();
        tombstone.push_all_dirs([
            server_id.to_string().as_str(),
            database_name.to_string().as_str(),
            "0",
        ]);
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
    async fn delete_then_create_new_with_same_name() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let root_path = RootPath::new(&object_store, server_id, &database_name);

        let iox_object_store =
            IoxObjectStore::new(Arc::clone(&object_store), server_id, &database_name)
                .await
                .unwrap();

        iox_object_store
            .put_database_rules_file(Bytes::new())
            .await
            .unwrap();

        let generations = IoxObjectStore::list_generations(&object_store, &root_path)
            .await
            .unwrap();
        assert_eq!(generations.len(), 1);
        assert_eq!(
            generations[0],
            Generation {
                id: GenerationId { inner: 0 },
                deleted_at: None,
            }
        );

        iox_object_store.write_tombstone().await.unwrap();

        let generations = IoxObjectStore::list_generations(&object_store, &root_path)
            .await
            .unwrap();
        assert_eq!(generations.len(), 1);
        assert_eq!(generations[0].id, GenerationId { inner: 0 });
        assert!(!generations[0].is_active());

        let iox_object_store =
            IoxObjectStore::new(Arc::clone(&object_store), server_id, &database_name)
                .await
                .unwrap();

        iox_object_store
            .put_database_rules_file(Bytes::new())
            .await
            .unwrap();

        let mut generations = IoxObjectStore::list_generations(&object_store, &root_path)
            .await
            .unwrap();
        assert_eq!(generations.len(), 2);
        generations.sort_by_key(|g| g.id);
        assert_eq!(generations[0].id, GenerationId { inner: 0 });
        assert!(!generations[0].is_active());
        assert_eq!(
            generations[1],
            Generation {
                id: GenerationId { inner: 1 },
                deleted_at: None,
            }
        );
    }

    async fn create_database(
        object_store: Arc<ObjectStore>,
        server_id: ServerId,
        database_name: &DatabaseName<'static>,
    ) -> IoxObjectStore {
        let iox_object_store =
            IoxObjectStore::new(Arc::clone(&object_store), server_id, database_name)
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

    #[tokio::test]
    async fn list_possible_databases_returns_all_potential_databases() {
        let object_store = make_object_store();
        let server_id = make_server_id();

        // Create a normal database, will be in the list
        let db_normal = DatabaseName::new("db_normal").unwrap();
        create_database(Arc::clone(&object_store), server_id, &db_normal).await;

        // Create a database, then delete it - will still be in the list
        let db_deleted = DatabaseName::new("db_deleted").unwrap();
        let db_deleted_iox_store =
            create_database(Arc::clone(&object_store), server_id, &db_deleted).await;
        delete_database(&db_deleted_iox_store).await;

        // Put a file in a directory that looks like a database directory but has no rules,
        // will still be in the list
        let not_a_db = DatabaseName::new("not_a_db").unwrap();
        let mut not_rules_path = object_store.new_path();
        not_rules_path.push_all_dirs(&[&server_id.to_string(), not_a_db.as_str(), "0"]);
        not_rules_path.set_file_name("not_rules.txt");
        object_store
            .put(
                &not_rules_path,
                stream::once(async move { Ok(Bytes::new()) }),
                None,
            )
            .await
            .unwrap();

        // Put a file in a directory that's an invalid database name - this WON'T be in the list
        let invalid_db_name = ("a".repeat(65)).to_string();
        let mut invalid_db_name_rules_path = object_store.new_path();
        invalid_db_name_rules_path.push_all_dirs(&[&server_id.to_string(), &invalid_db_name, "0"]);
        invalid_db_name_rules_path.set_file_name("rules.pb");
        object_store
            .put(
                &invalid_db_name_rules_path,
                stream::once(async move { Ok(Bytes::new()) }),
                None,
            )
            .await
            .unwrap();

        let mut possible = IoxObjectStore::list_possible_databases(&object_store, server_id)
            .await
            .unwrap();
        possible.sort();
        assert_eq!(possible, vec![db_deleted, db_normal, not_a_db]);
    }

    #[tokio::test]
    async fn list_all_databases_returns_generation_info() {
        let object_store = make_object_store();
        let server_id = make_server_id();

        // Create a normal database, will be in the list with one active generation
        let db_normal = DatabaseName::new("db_normal").unwrap();
        create_database(Arc::clone(&object_store), server_id, &db_normal).await;

        // Create a database, then delete it - will be in the list with one inactive generation
        let db_deleted = DatabaseName::new("db_deleted").unwrap();
        let db_deleted_iox_store =
            create_database(Arc::clone(&object_store), server_id, &db_deleted).await;
        delete_database(&db_deleted_iox_store).await;

        // Create, delete, create - will be in the list with one active and one inactive generation
        let db_reincarnated = DatabaseName::new("db_reincarnated").unwrap();
        let db_reincarnated_iox_store =
            create_database(Arc::clone(&object_store), server_id, &db_reincarnated).await;
        delete_database(&db_reincarnated_iox_store).await;
        create_database(Arc::clone(&object_store), server_id, &db_reincarnated).await;

        // Put a file in a directory that looks like a database directory but has no rules,
        // will still be in the list with one active generation
        let not_a_db = DatabaseName::new("not_a_db").unwrap();
        let mut not_rules_path = object_store.new_path();
        not_rules_path.push_all_dirs(&[&server_id.to_string(), not_a_db.as_str(), "0"]);
        not_rules_path.set_file_name("not_rules.txt");
        object_store
            .put(
                &not_rules_path,
                stream::once(async move { Ok(Bytes::new()) }),
                None,
            )
            .await
            .unwrap();

        // Put a file in a directory that's an invalid database name - this WON'T be in the list
        let invalid_db_name = ("a".repeat(65)).to_string();
        let mut invalid_db_name_rules_path = object_store.new_path();
        invalid_db_name_rules_path.push_all_dirs(&[&server_id.to_string(), &invalid_db_name, "0"]);
        invalid_db_name_rules_path.set_file_name("rules.pb");
        object_store
            .put(
                &invalid_db_name_rules_path,
                stream::once(async move { Ok(Bytes::new()) }),
                None,
            )
            .await
            .unwrap();

        // Put a file in a directory that looks like a database name, but doesn't look like a
        // generation directory - the database will be in the list but the generations will
        // be empty.
        let no_generations = DatabaseName::new("no_generations").unwrap();
        let mut no_generations_path = object_store.new_path();
        no_generations_path.push_all_dirs(&[
            &server_id.to_string(),
            no_generations.as_str(),
            "not-a-generation",
        ]);
        no_generations_path.set_file_name("not_rules.txt");
        object_store
            .put(
                &no_generations_path,
                stream::once(async move { Ok(Bytes::new()) }),
                None,
            )
            .await
            .unwrap();

        let all_dbs = IoxObjectStore::list_all_databases(&object_store, server_id)
            .await
            .unwrap();

        let db_names: Vec<_> = all_dbs.keys().collect();
        assert_eq!(
            db_names,
            vec![
                &db_deleted,
                &db_normal,
                &db_reincarnated,
                &no_generations,
                &not_a_db
            ]
        );

        let db_normal_generations = all_dbs.get(&db_normal).unwrap();
        assert_eq!(db_normal_generations.len(), 1);
        assert_eq!(db_normal_generations[0].id, GenerationId { inner: 0 });
        assert!(db_normal_generations[0].is_active());

        let db_deleted_generations = all_dbs.get(&db_deleted).unwrap();
        assert_eq!(db_deleted_generations.len(), 1);
        assert_eq!(db_deleted_generations[0].id, GenerationId { inner: 0 });
        assert!(!db_deleted_generations[0].is_active());

        let mut db_reincarnated_generations = all_dbs.get(&db_reincarnated).unwrap().clone();
        db_reincarnated_generations.sort_by_key(|g| g.id);
        assert_eq!(db_reincarnated_generations.len(), 2);
        assert_eq!(db_reincarnated_generations[0].id, GenerationId { inner: 0 });
        assert!(!db_reincarnated_generations[0].is_active());
        assert_eq!(db_reincarnated_generations[1].id, GenerationId { inner: 1 });
        assert!(db_reincarnated_generations[1].is_active());

        // There is a database-looking directory with a generation-looking directory inside it
        // that doesn't have a tombstone file, so as far as we know at this point this is a
        // database. Actually using this database and finding out it's invalid is out of scope.
        let not_a_db_generations = all_dbs.get(&not_a_db).unwrap();
        assert_eq!(not_a_db_generations.len(), 1);
        assert_eq!(not_a_db_generations[0].id, GenerationId { inner: 0 });
        assert!(not_a_db_generations[0].is_active());

        // There are files in this database directory, but none of them look like generation
        // directories, so generations is empty.
        let no_generations_generations = all_dbs.get(&no_generations).unwrap();
        assert!(
            no_generations_generations.is_empty(),
            "got {:?}",
            no_generations_generations
        );
    }

    #[tokio::test]
    async fn list_deleted_databases_metadata() {
        let object_store = make_object_store();
        let server_id = make_server_id();

        // Create a normal database, will NOT be in the list of deleted databases
        let db_normal = DatabaseName::new("db_normal").unwrap();
        create_database(Arc::clone(&object_store), server_id, &db_normal).await;

        // Create a database, then delete it - will be in the list once
        let db_deleted = DatabaseName::new("db_deleted").unwrap();
        let db_deleted_iox_store =
            create_database(Arc::clone(&object_store), server_id, &db_deleted).await;
        delete_database(&db_deleted_iox_store).await;

        // Create, delete, create - will be in the list once
        let db_reincarnated = DatabaseName::new("db_reincarnated").unwrap();
        let db_reincarnated_iox_store =
            create_database(Arc::clone(&object_store), server_id, &db_reincarnated).await;
        delete_database(&db_reincarnated_iox_store).await;
        create_database(Arc::clone(&object_store), server_id, &db_reincarnated).await;

        // Create, delete, create, delete - will be in the list twice
        let db_deleted_twice = DatabaseName::new("db_deleted_twice").unwrap();
        let db_deleted_twice_iox_store =
            create_database(Arc::clone(&object_store), server_id, &db_deleted_twice).await;
        delete_database(&db_deleted_twice_iox_store).await;
        let db_deleted_twice_iox_store =
            create_database(Arc::clone(&object_store), server_id, &db_deleted_twice).await;
        delete_database(&db_deleted_twice_iox_store).await;

        // Put a file in a directory that looks like a database directory but has no rules,
        // won't be in the list because there's no tombstone file
        let not_a_db = DatabaseName::new("not_a_db").unwrap();
        let mut not_rules_path = object_store.new_path();
        not_rules_path.push_all_dirs(&[&server_id.to_string(), not_a_db.as_str(), "0"]);
        not_rules_path.set_file_name("not_rules.txt");
        object_store
            .put(
                &not_rules_path,
                stream::once(async move { Ok(Bytes::new()) }),
                None,
            )
            .await
            .unwrap();

        // Put a file in a directory that's an invalid database name - won't be in the list
        let invalid_db_name = ("a".repeat(65)).to_string();
        let mut invalid_db_name_rules_path = object_store.new_path();
        invalid_db_name_rules_path.push_all_dirs(&[&server_id.to_string(), &invalid_db_name, "0"]);
        invalid_db_name_rules_path.set_file_name("rules.pb");
        object_store
            .put(
                &invalid_db_name_rules_path,
                stream::once(async move { Ok(Bytes::new()) }),
                None,
            )
            .await
            .unwrap();

        // Put a file in a directory that looks like a database name, but doesn't look like a
        // generation directory - won't be in the list
        let no_generations = DatabaseName::new("no_generations").unwrap();
        let mut no_generations_path = object_store.new_path();
        no_generations_path.push_all_dirs(&[
            &server_id.to_string(),
            no_generations.as_str(),
            "not-a-generation",
        ]);
        no_generations_path.set_file_name("not_rules.txt");
        object_store
            .put(
                &no_generations_path,
                stream::once(async move { Ok(Bytes::new()) }),
                None,
            )
            .await
            .unwrap();

        let mut deleted_dbs = IoxObjectStore::list_deleted_databases(&object_store, server_id)
            .await
            .unwrap();

        deleted_dbs.sort_by_key(|d| (d.name.clone(), d.generation_id));

        assert_eq!(deleted_dbs.len(), 4);

        assert_eq!(deleted_dbs[0].name, db_deleted);
        assert_eq!(deleted_dbs[0].generation_id.inner, 0);

        assert_eq!(deleted_dbs[1].name, db_deleted_twice);
        assert_eq!(deleted_dbs[1].generation_id.inner, 0);
        assert_eq!(deleted_dbs[2].name, db_deleted_twice);
        assert_eq!(deleted_dbs[2].generation_id.inner, 1);

        assert_eq!(deleted_dbs[3].name, db_reincarnated);
        assert_eq!(deleted_dbs[3].generation_id.inner, 0);
    }

    async fn restore_database(
        object_store: Arc<ObjectStore>,
        server_id: ServerId,
        database_name: &DatabaseName<'static>,
        generation_id: GenerationId,
    ) -> Result<IoxObjectStore, IoxObjectStoreError> {
        IoxObjectStore::restore_database(
            Arc::clone(&object_store),
            server_id,
            database_name,
            generation_id,
        )
        .await
    }

    #[tokio::test]
    async fn restore_deleted_database() {
        let object_store = make_object_store();
        let server_id = make_server_id();

        // Create a database
        let db = DatabaseName::new("db").unwrap();
        let db_iox_store = create_database(Arc::clone(&object_store), server_id, &db).await;

        // Delete the database
        delete_database(&db_iox_store).await;

        // Create and delete it again so there are two deleted generations
        let db_iox_store = create_database(Arc::clone(&object_store), server_id, &db).await;
        delete_database(&db_iox_store).await;

        // Get one generation ID from the list of deleted databases
        let deleted_dbs = IoxObjectStore::list_deleted_databases(&object_store, server_id)
            .await
            .unwrap();
        assert_eq!(deleted_dbs.len(), 2);
        let deleted_db = deleted_dbs.iter().find(|d| d.name == db).unwrap();

        // Restore the generation
        restore_database(
            Arc::clone(&object_store),
            server_id,
            &db,
            deleted_db.generation_id,
        )
        .await
        .unwrap();

        // The database should be in the list of all databases again
        let all_dbs = IoxObjectStore::list_all_databases(&object_store, server_id)
            .await
            .unwrap();
        assert_eq!(all_dbs.len(), 1);

        // The other deleted generation should be the only item in the deleted list
        let deleted_dbs = IoxObjectStore::list_deleted_databases(&object_store, server_id)
            .await
            .unwrap();
        assert_eq!(deleted_dbs.len(), 1);

        // Try to restore the other deleted database
        let deleted_db = deleted_dbs.iter().find(|d| d.name == db).unwrap();
        let err = restore_database(
            Arc::clone(&object_store),
            server_id,
            &db,
            deleted_db.generation_id,
        )
        .await
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Cannot restore; there is already an active database named `db`"
        );
    }
}
