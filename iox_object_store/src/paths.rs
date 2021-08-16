//! Paths for specific types of files within a database's object storage.

use data_types::{server_id::ServerId, DatabaseName};
use object_store::path::{ObjectStorePath, Path};

pub mod parquet_file;
use parquet_file::Path as ParquetFilePath;

/// A database-specific object store path that all `IoxPath`s should be within.
/// This should not be leaked outside this crate.
#[derive(Debug, Clone)]
pub struct RootPath {
    inner: Path,
}

impl RootPath {
    /// How the root of a database is defined in object storage.
    pub fn new(mut root: Path, server_id: ServerId, database_name: &DatabaseName<'_>) -> Self {
        root.push_dir(server_id.to_string());
        root.push_dir(database_name.as_str());
        Self { inner: root }
    }

    pub fn join(&self, dir: &str) -> Path {
        let mut result = self.inner.clone();
        result.push_dir(dir);
        result
    }
}

/// A database-specific object store path for all catalog transaction files. This should not be
/// leaked outside this crate.
#[derive(Debug, Clone)]
pub struct TransactionsPath {
    pub inner: Path,
}

impl TransactionsPath {
    pub fn new(root_path: &RootPath) -> Self {
        Self {
            inner: root_path.join("transactions"),
        }
    }
}

/// A database-specific object store path for all data files. This should not be leaked outside
/// this crate.
#[derive(Debug, Clone)]
pub struct DataPath {
    pub inner: Path,
}

impl DataPath {
    pub fn new(root_path: &RootPath) -> Self {
        Self {
            inner: root_path.join("data"),
        }
    }

    pub fn join(&self, parquet_file_path: &ParquetFilePath) -> Path {
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
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let iox_object_store = IoxObjectStore::new(make_object_store(), server_id, &database_name);

        assert_eq!(iox_object_store.root_path.inner.to_string(), "1/clouds/")
    }

    #[test]
    fn root_path_join_concatenates() {
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let iox_object_store = IoxObjectStore::new(make_object_store(), server_id, &database_name);

        let path = iox_object_store.root_path.join("foo");
        assert_eq!(path.to_string(), "1/clouds/foo/");
    }

    #[test]
    fn transactions_path_is_relative_to_db_root() {
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let iox_object_store = IoxObjectStore::new(make_object_store(), server_id, &database_name);
        assert_eq!(
            iox_object_store.transactions_path.inner.to_string(),
            "1/clouds/transactions/"
        );
    }

    #[test]
    fn data_path_is_relative_to_db_root() {
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let iox_object_store = IoxObjectStore::new(make_object_store(), server_id, &database_name);
        assert_eq!(
            iox_object_store.data_path.inner.to_string(),
            "1/clouds/data/"
        );
    }
}
