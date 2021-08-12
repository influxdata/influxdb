//! Paths for specific types of files within a database's object storage.

use data_types::{chunk_metadata::ChunkAddr, server_id::ServerId, DatabaseName};
use object_store::{
    path::{parsed::DirsAndFileName, ObjectStorePath, Path},
    Result,
};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::sync::Arc;
use uuid::Uuid;

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

/// Location of a Parquet file within a database's object store.
/// The exact format is an implementation detail and is subject to change.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ParquetFilePath {
    table_name: Arc<str>,
    partition_key: Arc<str>,
    chunk_id: u32,
    uuid: Uuid,
}

impl ParquetFilePath {
    /// Create a location for this chunk's parquet file. Calling this twice on the same `ChunkAddr`
    /// will return different `ParquetFilePaths`.
    pub fn new(chunk_addr: &ChunkAddr) -> Self {
        Self {
            table_name: Arc::clone(&chunk_addr.table_name),
            partition_key: Arc::clone(&chunk_addr.partition_key),
            chunk_id: chunk_addr.chunk_id,
            // generate random UUID so that files are unique and never overwritten
            uuid: Uuid::new_v4(),
        }
    }

    /// Turn this into directories and file names to be added to a root path or to be serialized
    /// in protobuf.
    pub fn relative_dirs_and_file_name(&self) -> DirsAndFileName {
        let mut result = DirsAndFileName::default();
        result.push_all_dirs(&[self.table_name.as_ref(), self.partition_key.as_ref()]);
        result.set_file_name(format!("{}.{}.parquet", self.chunk_id, self.uuid));
        result
    }

    /// Create from serialized protobuf strings.
    pub fn from_relative_dirs_and_file_name(
        dirs_and_file_name: &DirsAndFileName,
    ) -> Result<Self, ParquetFilePathParseError> {
        let mut directories = dirs_and_file_name.directories.iter();
        let table_name = directories
            .next()
            .context(MissingTableName)?
            .to_string()
            .into();
        let partition_key = directories
            .next()
            .context(MissingPartitionKey)?
            .to_string()
            .into();

        ensure!(directories.next().is_none(), UnexpectedDirectory);

        let file_name = dirs_and_file_name
            .file_name
            .as_ref()
            .context(MissingChunkId)?
            .to_string();
        let mut parts = file_name.split('.');
        let chunk_id = parts
            .next()
            .context(MissingChunkId)?
            .parse()
            .context(InvalidChunkId)?;
        let uuid = parts
            .next()
            .context(MissingUuid)?
            .parse()
            .context(InvalidUuid)?;
        let ext = parts.next().context(MissingExtension)?;
        ensure!(ext == "parquet", InvalidExtension { ext });
        ensure!(parts.next().is_none(), UnexpectedExtension);

        Ok(Self {
            table_name,
            partition_key,
            chunk_id,
            uuid,
        })
    }

    // Deliberately pub(crate); this transformation should only happen within this crate
    pub(crate) fn from_absolute(absolute_path: Path) -> Result<Self, ParquetFilePathParseError> {
        let absolute_path: DirsAndFileName = absolute_path.into();

        let mut absolute_dirs = absolute_path.directories.into_iter().fuse();

        // The number of `next`s here needs to match the total number of directories in
        // iox_object_store data_paths
        absolute_dirs.next(); // server id
        absolute_dirs.next(); // database name
        absolute_dirs.next(); // "data"

        let remaining = DirsAndFileName {
            directories: absolute_dirs.collect(),
            file_name: absolute_path.file_name,
        };

        Self::from_relative_dirs_and_file_name(&remaining)
    }
}

impl From<&Self> for ParquetFilePath {
    fn from(borrowed: &Self) -> Self {
        borrowed.clone()
    }
}

#[derive(Snafu, Debug, PartialEq)]
#[allow(missing_docs)]
pub enum ParquetFilePathParseError {
    #[snafu(display("Could not find required table name"))]
    MissingTableName,

    #[snafu(display("Could not find required partition key"))]
    MissingPartitionKey,

    #[snafu(display("Too many directories found"))]
    UnexpectedDirectory,

    #[snafu(display("Could not find required chunk id"))]
    MissingChunkId,

    #[snafu(display("Could not parse chunk id: {}", source))]
    InvalidChunkId { source: std::num::ParseIntError },

    #[snafu(display("Could not find required UUID"))]
    MissingUuid,

    #[snafu(display("Could not parse UUID: {}", source))]
    InvalidUuid { source: uuid::Error },

    #[snafu(display("Could not find required file extension"))]
    MissingExtension,

    #[snafu(display("Extension should have been `parquet`, instead found `{}`", ext))]
    InvalidExtension { ext: String },

    #[snafu(display("Too many extensions found"))]
    UnexpectedExtension,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IoxObjectStore;
    use object_store::{ObjectStore, ObjectStoreApi};
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
    fn catalog_path_is_relative_to_db_root() {
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let iox_object_store = IoxObjectStore::new(make_object_store(), server_id, &database_name);
        assert_eq!(
            iox_object_store.catalog_path().to_string(),
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

    #[test]
    fn test_parquet_file_paths_are_unique() {
        let chunk_addr = ChunkAddr {
            db_name: "clouds".into(),
            table_name: "my_table".into(),
            partition_key: "my_partition".into(),
            chunk_id: 13,
        };

        let p1 = ParquetFilePath::new(&chunk_addr);
        let p2 = ParquetFilePath::new(&chunk_addr);
        assert_ne!(p1, p2);
    }

    #[test]
    fn test_parquet_file_path_deserialization() {
        // Error cases
        use ParquetFilePathParseError::*;

        let mut df = DirsAndFileName::default();
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df);
        assert!(matches!(result, Err(MissingTableName)), "got {:?}", result);

        df.push_dir("foo");
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(MissingPartitionKey)),
            "got {:?}",
            result
        );

        df.push_dir("bar");
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(MissingChunkId { .. })),
            "got {:?}",
            result
        );

        let mut extra = df.clone();
        extra.push_dir("nope");
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&extra);
        assert!(
            matches!(result, Err(UnexpectedDirectory)),
            "got {:?}",
            result
        );

        df.set_file_name("bleh");
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(InvalidChunkId { .. })),
            "got {:?}",
            result
        );

        df.set_file_name("3");
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(MissingUuid { .. })),
            "got {:?}",
            result
        );

        df.set_file_name("3.nope");
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(InvalidUuid { .. })),
            "got {:?}",
            result
        );

        let uuid = Uuid::new_v4();
        df.set_file_name(&format!("3.{}", uuid));
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df);
        assert!(matches!(result, Err(MissingExtension)), "got {:?}", result);

        df.set_file_name(&format!("3.{}.exe", uuid));
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(InvalidExtension { .. })),
            "got {:?}",
            result
        );

        df.set_file_name(&format!("3.{}.parquet.v6", uuid));
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(UnexpectedExtension)),
            "got {:?}",
            result
        );

        // Success case
        df.set_file_name(&format!("3.{}.parquet", uuid));
        let result = ParquetFilePath::from_relative_dirs_and_file_name(&df).unwrap();
        assert_eq!(
            result,
            ParquetFilePath {
                table_name: "foo".into(),
                partition_key: "bar".into(),
                chunk_id: 3,
                uuid
            }
        );
        let round_trip = result.relative_dirs_and_file_name();
        assert_eq!(round_trip, df);
    }

    #[test]
    fn parquet_file_from_absolute() {
        let object_store = make_object_store();

        // Error cases
        use ParquetFilePathParseError::*;

        let mut path = object_store.new_path();
        // incorrect directories are fine, we're assuming that list(data_path) scoped to the
        // right directories so we don't check again on the way out
        path.push_all_dirs(&["foo", "bar", "baz", "}*", "aoeu"]);
        path.set_file_name("rules.pb");
        let result = ParquetFilePath::from_absolute(path);
        assert!(
            matches!(result, Err(InvalidChunkId { .. })),
            "got: {:?}",
            result
        );

        let mut path = object_store.new_path();
        path.push_all_dirs(&["foo", "bar", "baz", "}*", "aoeu"]);
        // missing file name
        let result = ParquetFilePath::from_absolute(path);
        assert!(matches!(result, Err(MissingChunkId)), "got: {:?}", result);

        // Success case
        let uuid = Uuid::new_v4();
        let mut path = object_store.new_path();
        path.push_all_dirs(&["foo", "bar", "baz", "}*", "aoeu"]);
        path.set_file_name(&format!("10.{}.parquet", uuid));
        let result = ParquetFilePath::from_absolute(path);
        assert_eq!(
            result.unwrap(),
            ParquetFilePath {
                table_name: "}*".into(),
                partition_key: "aoeu".into(),
                chunk_id: 10,
                uuid
            }
        );
    }

    #[test]
    fn parquet_file_relative_dirs_and_file_path() {
        let uuid = Uuid::new_v4();
        let pfp = ParquetFilePath {
            table_name: "}*".into(),
            partition_key: "aoeu".into(),
            chunk_id: 10,
            uuid,
        };
        let dirs_and_file_name = pfp.relative_dirs_and_file_name();
        assert_eq!(
            dirs_and_file_name.to_string(),
            format!("%7D%2A/aoeu/10.{}.parquet", uuid)
        );
        let round_trip =
            ParquetFilePath::from_relative_dirs_and_file_name(&dirs_and_file_name).unwrap();
        assert_eq!(pfp, round_trip);
    }

    #[test]
    fn data_path_join_with_parquet_file_path() {
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let object_store = make_object_store();
        let iox_object_store =
            IoxObjectStore::new(Arc::clone(&object_store), server_id, &database_name);

        let uuid = Uuid::new_v4();
        let pfp = ParquetFilePath {
            table_name: "}*".into(),
            partition_key: "aoeu".into(),
            chunk_id: 10,
            uuid,
        };

        let path = iox_object_store.data_path.join(&pfp);

        let mut expected_path = object_store.new_path();
        expected_path.push_all_dirs(&[
            &server_id.to_string(),
            database_name.as_str(),
            "data",
            "}*",
            "aoeu",
        ]);
        expected_path.set_file_name(&format!("10.{}.parquet", uuid));

        assert_eq!(path, expected_path);
    }
}
