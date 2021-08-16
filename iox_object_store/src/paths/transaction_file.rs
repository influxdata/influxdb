use object_store::{
    path::{parsed::DirsAndFileName, ObjectStorePath, Path as ObjStoPath},
    Result,
};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::str::FromStr;
use uuid::Uuid;

/// File suffix for transaction files in object store.
const TRANSACTION_FILE_SUFFIX: &str = "txn";

/// File suffix for checkpoint files in object store.
const CHECKPOINT_FILE_SUFFIX: &str = "ckpt";

/// Location of a catalog transaction file within a database's object store.
/// The exact format is an implementation detail and is subject to change.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Path {
    /// Transaction revision
    pub revision_counter: u64,
    /// Transaction identifier
    pub uuid: Uuid,
    suffix: TransactionFileSuffix,
}

impl Path {
    /// Create a new file path to store transaction info.
    pub fn new_transaction(revision_counter: u64, uuid: Uuid) -> Self {
        Self {
            revision_counter,
            uuid,
            suffix: TransactionFileSuffix::Transaction,
        }
    }

    /// Create a new file path to store checkpoint info.
    pub fn new_checkpoint(revision_counter: u64, uuid: Uuid) -> Self {
        Self {
            revision_counter,
            uuid,
            suffix: TransactionFileSuffix::Checkpoint,
        }
    }

    /// Returns true if this path is to a checkpoint file; false otherwise.
    pub fn is_checkpoint(&self) -> bool {
        self.suffix == TransactionFileSuffix::Checkpoint
    }

    /// Turn this into directories and file names to be added to a root path
    pub fn relative_dirs_and_file_name(&self) -> DirsAndFileName {
        let mut result = DirsAndFileName::default();

        // pad number: `u64::MAX.to_string().len()` is 20
        result.push_dir(format!("{:0>20}", self.revision_counter));

        let file_name = format!("{}.{}", self.uuid, self.suffix.as_str());
        result.set_file_name(file_name);
        result
    }

    /// Create from serialized protobuf strings.
    pub fn from_relative_dirs_and_file_name(
        dirs_and_file_name: &DirsAndFileName,
    ) -> Result<Self, PathParseError> {
        let mut directories = dirs_and_file_name.directories.iter();

        let revision_counter = directories
            .next()
            .context(MissingRevisionCounter)?
            .to_string()
            .parse()
            .context(InvalidRevisionCounter)?;

        ensure!(directories.next().is_none(), UnexpectedDirectory);

        let file_name = dirs_and_file_name
            .file_name
            .as_ref()
            .context(MissingFileName)?
            .to_string();
        let mut parts = file_name.split('.');

        let uuid = parts
            .next()
            .context(MissingUuid)?
            .parse()
            .context(InvalidUuid)?;

        let suffix = parts
            .next()
            .context(MissingSuffix)?
            .parse()
            .context(InvalidSuffix)?;

        ensure!(parts.next().is_none(), UnexpectedExtension);

        Ok(Self {
            revision_counter,
            uuid,
            suffix,
        })
    }

    // Deliberately pub(crate); this transformation should only happen within this crate
    pub(crate) fn from_absolute(absolute_path: ObjStoPath) -> Result<Self, PathParseError> {
        let absolute_path: DirsAndFileName = absolute_path.into();

        let mut absolute_dirs = absolute_path.directories.into_iter().fuse();

        // The number of `next`s here needs to match the total number of directories in
        // iox_object_store transactions_path
        absolute_dirs.next(); // server id
        absolute_dirs.next(); // database name
        absolute_dirs.next(); // "transactions"

        let remaining = DirsAndFileName {
            directories: absolute_dirs.collect(),
            file_name: absolute_path.file_name,
        };

        Self::from_relative_dirs_and_file_name(&remaining)
    }
}

#[derive(Snafu, Debug, PartialEq)]
#[allow(missing_docs)]
pub enum PathParseError {
    #[snafu(display("Could not find required revision counter"))]
    MissingRevisionCounter,

    #[snafu(display("Could not parse revision counter: {}", source))]
    InvalidRevisionCounter { source: std::num::ParseIntError },

    #[snafu(display("Too many directories found"))]
    UnexpectedDirectory,

    #[snafu(display("Could not find required file name"))]
    MissingFileName,

    #[snafu(display("Could not find required UUID"))]
    MissingUuid,

    #[snafu(display("Could not parse UUID: {}", source))]
    InvalidUuid { source: uuid::Error },

    #[snafu(display("Could not find required suffix"))]
    MissingSuffix,

    #[snafu(display("Invalid suffix: {}", source))]
    InvalidSuffix {
        source: TransactionFileSuffixParseError,
    },

    #[snafu(display("Too many extensions found"))]
    UnexpectedExtension,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum TransactionFileSuffix {
    Transaction,
    Checkpoint,
}

impl TransactionFileSuffix {
    fn as_str(&self) -> &'static str {
        match self {
            TransactionFileSuffix::Transaction => TRANSACTION_FILE_SUFFIX,
            TransactionFileSuffix::Checkpoint => CHECKPOINT_FILE_SUFFIX,
        }
    }
}

#[derive(Snafu, Debug, PartialEq)]
#[allow(missing_docs)]
pub enum TransactionFileSuffixParseError {
    #[snafu(display("Unknown suffix: {}", suffix))]
    UnknownSuffix { suffix: String },
}

impl FromStr for TransactionFileSuffix {
    type Err = TransactionFileSuffixParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            TRANSACTION_FILE_SUFFIX => Ok(Self::Transaction),
            CHECKPOINT_FILE_SUFFIX => Ok(Self::Checkpoint),
            suffix => UnknownSuffix { suffix }.fail(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IoxObjectStore;
    use data_types::{server_id::ServerId, DatabaseName};
    use object_store::{ObjectStore, ObjectStoreApi};
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
    fn is_checkpoint_works() {
        let uuid = Uuid::new_v4();

        let transaction = Path::new_transaction(0, uuid);
        assert!(!transaction.is_checkpoint());

        let checkpoint = Path::new_checkpoint(0, uuid);
        assert!(checkpoint.is_checkpoint());
    }

    #[test]
    fn test_transaction_file_path_deserialization() {
        // Error cases
        use PathParseError::*;

        let mut df = DirsAndFileName::default();
        let result = Path::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(MissingRevisionCounter)),
            "got {:?}",
            result
        );

        df.push_dir("foo");
        let result = Path::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(InvalidRevisionCounter { .. })),
            "got {:?}",
            result
        );

        let mut df = DirsAndFileName::default();
        df.push_dir("00000000000000000123");
        df.push_dir("foo");
        let result = Path::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(UnexpectedDirectory)),
            "got {:?}",
            result
        );

        let mut df = DirsAndFileName::default();
        df.push_dir("00000000000000000123");
        let result = Path::from_relative_dirs_and_file_name(&df);
        assert!(matches!(result, Err(MissingFileName)), "got {:?}", result);

        df.set_file_name("foo");
        let result = Path::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(InvalidUuid { .. })),
            "got {:?}",
            result
        );

        let uuid = Uuid::new_v4();

        df.set_file_name(&format!("{}", uuid));
        let result = Path::from_relative_dirs_and_file_name(&df);
        assert!(matches!(result, Err(MissingSuffix)), "got {:?}", result);

        df.set_file_name(&format!("{}.exe", uuid));
        let result = Path::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(InvalidSuffix { .. })),
            "got {:?}",
            result
        );

        df.set_file_name(&format!("{}.{}.foo", uuid, TRANSACTION_FILE_SUFFIX));
        let result = Path::from_relative_dirs_and_file_name(&df);
        assert!(
            matches!(result, Err(UnexpectedExtension)),
            "got {:?}",
            result
        );

        // Success case
        df.set_file_name(&format!("{}.{}", uuid, TRANSACTION_FILE_SUFFIX));
        let result = Path::from_relative_dirs_and_file_name(&df).unwrap();
        assert_eq!(
            result,
            Path {
                revision_counter: 123,
                uuid,
                suffix: TransactionFileSuffix::Transaction,
            }
        );
        let round_trip = result.relative_dirs_and_file_name();
        assert_eq!(round_trip, df);
    }

    #[test]
    fn transaction_file_from_absolute() {
        let object_store = make_object_store();

        // Error cases
        use PathParseError::*;

        let mut path = object_store.new_path();
        // incorrect directories are fine, we're assuming that list(transactions_path) scoped to the
        // right directories so we don't check again on the way out
        path.push_all_dirs(&["foo", "bar", "baz", "}*", "aoeu"]);
        path.set_file_name("rules.pb");
        let result = Path::from_absolute(path);
        assert!(
            matches!(result, Err(InvalidRevisionCounter { .. })),
            "got: {:?}",
            result
        );

        let mut path = object_store.new_path();
        path.push_all_dirs(&["foo", "bar", "baz", "00000000000000000123"]);
        // missing file name
        let result = Path::from_absolute(path);
        assert!(matches!(result, Err(MissingFileName)), "got: {:?}", result);

        // Success case
        let uuid = Uuid::new_v4();
        let mut path = object_store.new_path();
        path.push_all_dirs(&["foo", "bar", "baz", "00000000000000000123"]);
        path.set_file_name(&format!("{}.{}", uuid, CHECKPOINT_FILE_SUFFIX));
        let result = Path::from_absolute(path);
        assert_eq!(
            result.unwrap(),
            Path {
                revision_counter: 123,
                uuid,
                suffix: TransactionFileSuffix::Checkpoint,
            }
        );
    }

    #[test]
    fn transaction_file_relative_dirs_and_file_path() {
        let uuid = Uuid::new_v4();
        let tfp = Path {
            revision_counter: 555,
            uuid,
            suffix: TransactionFileSuffix::Transaction,
        };
        let dirs_and_file_name = tfp.relative_dirs_and_file_name();
        assert_eq!(
            dirs_and_file_name.to_string(),
            format!("00000000000000000555/{}.{}", uuid, TRANSACTION_FILE_SUFFIX)
        );
        let round_trip = Path::from_relative_dirs_and_file_name(&dirs_and_file_name).unwrap();
        assert_eq!(tfp, round_trip);
    }

    #[test]
    fn transactions_path_join_with_parquet_file_path() {
        let server_id = make_server_id();
        let database_name = DatabaseName::new("clouds").unwrap();
        let object_store = make_object_store();
        let iox_object_store =
            IoxObjectStore::new(Arc::clone(&object_store), server_id, &database_name);

        let uuid = Uuid::new_v4();
        let tfp = Path {
            revision_counter: 555,
            uuid,
            suffix: TransactionFileSuffix::Checkpoint,
        };

        let path = iox_object_store.transactions_path.join(&tfp);

        let mut expected_path = object_store.new_path();
        expected_path.push_all_dirs(&[
            &server_id.to_string(),
            database_name.as_str(),
            "transactions",
            "00000000000000000555",
        ]);
        expected_path.set_file_name(&format!("{}.{}", uuid, CHECKPOINT_FILE_SUFFIX));

        assert_eq!(path, expected_path);
    }
}
