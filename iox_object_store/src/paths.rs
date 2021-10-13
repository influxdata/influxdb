//! Paths for specific types of files within a database's object storage.

use super::Generation;
use data_types::{server_id::ServerId, DatabaseName};
use object_store::{
    path::{ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi,
};

pub mod parquet_file;
use parquet_file::ParquetFilePath;

pub mod transaction_file;
use transaction_file::TransactionFilePath;

const SERVER_CONFIG_FILE_NAME: &str = "config.pb";

/// The path to the server file containing the list of databases this server owns.
// TODO: this is in the process of replacing all_databases_path for the floating databases design
pub(crate) fn server_config_path(object_store: &ObjectStore, server_id: ServerId) -> Path {
    let mut path = object_store.new_path();
    path.push_dir(server_id.to_string());
    path.set_file_name(SERVER_CONFIG_FILE_NAME);
    path
}

/// The path all database root paths should be in. Used for listing all databases and building
/// database `RootPath`s in the same way. Not its own type because it's only needed ephemerally.
// TODO: this is in the process of being deprecated in favor of server_config_path
pub(crate) fn all_databases_path(object_store: &ObjectStore, server_id: ServerId) -> Path {
    let mut path = object_store.new_path();
    path.push_dir(server_id.to_string());
    path
}

/// A database-specific object store path that all `IoxPath`s should be within.
/// This should not be leaked outside this crate.
#[derive(Debug, Clone)]
pub(crate) struct RootPath {
    pub(crate) inner: Path,
}

impl RootPath {
    /// How the root of a database is defined in object storage.
    pub(crate) fn new(
        object_store: &ObjectStore,
        server_id: ServerId,
        database_name: &DatabaseName<'_>,
    ) -> Self {
        let mut inner = all_databases_path(object_store, server_id);
        inner.push_dir(database_name.as_str());
        Self { inner }
    }

    fn join(&self, dir: &str) -> Path {
        let mut result = self.inner.clone();
        result.push_dir(dir);
        result
    }

    pub(crate) fn generation_path(&self, generation: Generation) -> GenerationPath {
        GenerationPath::new(self, generation)
    }
}

/// A database- and generation-specific object store path that all `IoxPath`s should be within.
/// This should not be leaked outside this crate.
#[derive(Debug, Clone)]
pub(crate) struct GenerationPath {
    pub(crate) inner: Path,
    generation: Generation,
}

impl GenerationPath {
    /// How the generation path of a database is defined in object storage.
    pub(crate) fn new(root_path: &RootPath, generation: Generation) -> Self {
        Self {
            inner: root_path.join(&generation.id.to_string()),
            generation,
        }
    }

    fn join(&self, dir: &str) -> Path {
        let mut result = self.inner.clone();
        result.push_dir(dir);
        result
    }

    pub(crate) fn data_path(&self) -> DataPath {
        DataPath::new(self)
    }

    pub(crate) fn transactions_path(&self) -> TransactionsPath {
        TransactionsPath::new(self)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TombstonePath {
    pub(crate) inner: Path,
}

impl TombstonePath {
    const TOMBSTONE_FILE_NAME: &'static str = "DELETED";

    /// How the tombstone path of a database is defined in object storage in terms of the
    /// generation path.
    pub(crate) fn new(generation_path: &GenerationPath) -> Self {
        Self::new_from_object_store_path(&generation_path.inner)
    }

    /// Creating a potential tombstone file location given an object storage path received from
    /// an object storage list operation.
    pub(crate) fn new_from_object_store_path(path: &Path) -> Self {
        let mut inner = path.clone();
        inner.set_file_name(Self::TOMBSTONE_FILE_NAME);
        Self { inner }
    }
}

/// A database-specific object store path for all catalog transaction files. This should not be
/// leaked outside this crate.
#[derive(Debug, Clone)]
pub(crate) struct TransactionsPath {
    pub(crate) inner: Path,
}

impl TransactionsPath {
    pub(crate) fn new(generation_path: &GenerationPath) -> Self {
        Self {
            inner: generation_path.join("transactions"),
        }
    }

    pub(crate) fn join(&self, transaction_file_path: &TransactionFilePath) -> Path {
        let mut result = self.inner.clone();
        let relative = transaction_file_path.relative_dirs_and_file_name();
        for part in relative.directories {
            result.push_dir(part.to_string());
        }
        result.set_file_name(
            relative
                .file_name
                .expect("Transaction file paths have filenames")
                .to_string(),
        );
        result
    }
}

/// A database-specific object store path for all data files. This should not be leaked outside
/// this crate.
#[derive(Debug, Clone)]
pub(crate) struct DataPath {
    pub(crate) inner: Path,
}

impl DataPath {
    pub(crate) fn new(generation_path: &GenerationPath) -> Self {
        Self {
            inner: generation_path.join("data"),
        }
    }

    pub(crate) fn join(&self, parquet_file_path: &ParquetFilePath) -> Path {
        let mut result = self.inner.clone();
        let relative = parquet_file_path.relative_dirs_and_file_name();
        for part in relative.directories {
            result.push_dir(part.to_string());
        }
        result.set_file_name(
            relative
                .file_name
                .expect("Parquet file paths have filenames")
                .to_string(),
        );
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IoxObjectStore;
    use object_store::ObjectStore;
    use std::{num::NonZeroU32, sync::Arc};

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
    fn root_path_contains_server_id_and_db_name() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let root_path = RootPath::new(&object_store, server_id, &database_name);

        assert_eq!(root_path.inner.to_string(), "mem:1/clouds/")
    }

    #[test]
    fn root_path_join_concatenates() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let root_path = RootPath::new(&object_store, server_id, &database_name);

        let path = root_path.join("foo");
        assert_eq!(path.to_string(), "mem:1/clouds/foo/");
    }

    #[test]
    fn generation_path_is_relative_to_root_path() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let generation = Generation::active(3);
        let root_path = RootPath::new(&object_store, server_id, &database_name);
        let iox_object_store = IoxObjectStore::existing(
            Arc::clone(&object_store),
            server_id,
            &database_name,
            generation,
            root_path,
        );
        assert_eq!(
            iox_object_store.generation_path.inner.to_string(),
            "mem:1/clouds/3/"
        );
    }

    #[test]
    fn transactions_path_is_relative_to_generation_path() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let generation = Generation::active(3);
        let root_path = RootPath::new(&object_store, server_id, &database_name);
        let iox_object_store = IoxObjectStore::existing(
            Arc::clone(&object_store),
            server_id,
            &database_name,
            generation,
            root_path,
        );
        assert_eq!(
            iox_object_store.transactions_path.inner.to_string(),
            "mem:1/clouds/3/transactions/"
        );
    }

    #[test]
    fn data_path_is_relative_to_generation_path() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let generation = Generation::active(3);
        let root_path = RootPath::new(&object_store, server_id, &database_name);
        let iox_object_store = IoxObjectStore::existing(
            Arc::clone(&object_store),
            server_id,
            &database_name,
            generation,
            root_path,
        );
        assert_eq!(
            iox_object_store.data_path.inner.to_string(),
            "mem:1/clouds/3/data/"
        );
    }
}
