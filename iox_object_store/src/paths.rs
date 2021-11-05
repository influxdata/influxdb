//! Paths for specific types of files within a database's object storage.

use data_types::server_id::ServerId;
use object_store::{
    path::{ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi,
};
use std::fmt;
use uuid::Uuid;

pub mod parquet_file;
use parquet_file::ParquetFilePath;

pub mod transaction_file;
use transaction_file::TransactionFilePath;

pub(crate) const ALL_DATABASES_DIRECTORY: &str = "dbs";
const SERVER_CONFIG_FILE_NAME: &str = "config.pb";
const DATABASE_OWNER_FILE_NAME: &str = "owner.pb";

/// The path to the server file containing the list of databases this server owns.
// TODO: this is in the process of replacing all_databases_path for the floating databases design
pub(crate) fn server_config_path(object_store: &ObjectStore, server_id: ServerId) -> Path {
    let mut path = object_store.new_path();
    path.push_dir(server_id.to_string());
    path.set_file_name(SERVER_CONFIG_FILE_NAME);
    path
}

/// A database-specific object store path that all `IoxObjectStore` `Path`s should be within.
/// This can be serialized to facilitate initial loading of a database from object storage, but
/// the path should not be parsed into its component parts as the format might change.
#[derive(Debug, Clone, PartialEq)]
pub struct RootPath {
    pub(crate) inner: Path,
}

impl RootPath {
    /// How the root of a database is defined in object storage.
    pub(crate) fn new(object_store: &ObjectStore, uuid: Uuid) -> Self {
        let mut inner = object_store.new_path();
        inner.push_dir(ALL_DATABASES_DIRECTORY);
        inner.push_dir(uuid.to_string());
        Self { inner }
    }

    pub(crate) fn from_str(object_store: &ObjectStore, raw: &str) -> Self {
        Self {
            inner: object_store.path_from_raw(raw),
        }
    }

    fn join(&self, dir: &str) -> Path {
        let mut result = self.inner.clone();
        result.push_dir(dir);
        result
    }

    pub(crate) fn owner_path(&self) -> Path {
        let mut result = self.inner.clone();
        result.set_file_name(DATABASE_OWNER_FILE_NAME);
        result
    }

    pub(crate) fn rules_path(&self) -> RulesPath {
        RulesPath::new(self)
    }

    pub(crate) fn data_path(&self) -> DataPath {
        DataPath::new(self)
    }

    pub(crate) fn transactions_path(&self) -> TransactionsPath {
        TransactionsPath::new(self)
    }

    pub(crate) fn tombstone_path(&self) -> TombstonePath {
        TombstonePath::new(self)
    }
}

impl fmt::Display for RootPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner.to_raw())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RulesPath {
    pub(crate) inner: Path,
}

impl RulesPath {
    const DB_RULES_FILE_NAME: &'static str = "rules.pb";

    /// How the rules path of a database is defined in object storage in terms of the
    /// root path.
    pub(crate) fn new(root_path: &RootPath) -> Self {
        Self::new_from_object_store_path(&root_path.inner)
    }

    /// Creating a potential rules file location given an object storage path received from
    /// an object storage list operation.
    pub(crate) fn new_from_object_store_path(path: &Path) -> Self {
        let mut inner = path.clone();
        inner.set_file_name(Self::DB_RULES_FILE_NAME);
        Self { inner }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TombstonePath {
    pub(crate) inner: Path,
}

impl TombstonePath {
    const TOMBSTONE_FILE_NAME: &'static str = "DELETED";

    /// How the tombstone path of a database is defined in object storage in terms of the
    /// root path.
    pub(crate) fn new(root_path: &RootPath) -> Self {
        Self::new_from_object_store_path(&root_path.inner)
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
    /// How the transactions path of a database is defined in object storage in terms of the
    /// root path.
    pub(crate) fn new(root_path: &RootPath) -> Self {
        Self {
            inner: root_path.join("transactions"),
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
    /// How the data path of a database is defined in object storage in terms of the root path.
    pub(crate) fn new(root_path: &RootPath) -> Self {
        Self {
            inner: root_path.join("data"),
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
    use std::sync::Arc;

    /// Creates a new in-memory object store. These tests rely on the `Path`s being of type
    /// `DirsAndFileName` and thus using object_store::path::DELIMITER as the separator
    fn make_object_store() -> Arc<ObjectStore> {
        Arc::new(ObjectStore::new_in_memory())
    }

    #[test]
    fn root_path_contains_dbs_and_db_uuid() {
        let object_store = make_object_store();
        let uuid = Uuid::new_v4();
        let root_path = RootPath::new(&object_store, uuid);

        assert_eq!(
            root_path.inner.to_string(),
            format!("mem:{}/{}/", ALL_DATABASES_DIRECTORY, uuid)
        );
    }

    #[test]
    fn root_path_join_concatenates() {
        let object_store = make_object_store();
        let uuid = Uuid::new_v4();
        let root_path = RootPath::new(&object_store, uuid);

        let path = root_path.join("foo");
        assert_eq!(
            path.to_string(),
            format!("mem:{}/{}/foo/", ALL_DATABASES_DIRECTORY, uuid)
        );
    }

    #[test]
    fn transactions_path_is_relative_to_root_path() {
        let object_store = make_object_store();
        let uuid = Uuid::new_v4();
        let root_path = RootPath::new(&object_store, uuid);
        let iox_object_store = IoxObjectStore::existing(Arc::clone(&object_store), root_path);
        assert_eq!(
            iox_object_store.transactions_path.inner.to_string(),
            format!("mem:{}/{}/transactions/", ALL_DATABASES_DIRECTORY, uuid)
        );
    }

    #[test]
    fn data_path_is_relative_to_root_path() {
        let object_store = make_object_store();
        let uuid = Uuid::new_v4();
        let root_path = RootPath::new(&object_store, uuid);
        let iox_object_store = IoxObjectStore::existing(Arc::clone(&object_store), root_path);
        assert_eq!(
            iox_object_store.data_path.inner.to_string(),
            format!("mem:{}/{}/data/", ALL_DATABASES_DIRECTORY, uuid)
        );
    }
}
