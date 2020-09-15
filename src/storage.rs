//! This module defines the traits by which the rest of Delorean
//! interacts with the storage system. The goal is to define a clear
//! interface as well as being able to test other parts of Delorean
//! using mockups that conform to these traits

use std::convert::TryFrom;

pub mod block;
pub mod database;
pub mod memdb;
pub mod partitioned_store;
pub mod predicate;
pub mod remote_partition;
pub mod s3_partition;
pub mod write_buffer_database;

use std::{fmt::Debug, sync::Arc};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use delorean_line_parser::ParsedLine;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ReadPoint<T: Clone> {
    pub time: i64,
    pub value: T,
}

impl<T: Clone> From<&'_ crate::line_parser::Point<T>> for ReadPoint<T> {
    fn from(other: &'_ crate::line_parser::Point<T>) -> Self {
        let crate::line_parser::Point { time, value, .. } = other;
        Self {
            time: *time,
            value: value.clone(),
        }
    }
}

// The values for these enum variants have no real meaning, but they
// are serialized to disk. Revisit these whenever it's time to decide
// on an on-disk format.
#[repr(u8)]
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SeriesDataType {
    I64 = 0,
    F64 = 1,
    String = 2,
    Bool = 3,
    //    U64,
}

impl From<SeriesDataType> for u8 {
    fn from(other: SeriesDataType) -> Self {
        other as Self
    }
}

impl TryFrom<u8> for SeriesDataType {
    type Error = u8;

    fn try_from(other: u8) -> Result<Self, Self::Error> {
        use SeriesDataType::*;

        match other {
            v if v == I64 as u8 => Ok(I64),
            v if v == F64 as u8 => Ok(F64),
            v if v == String as u8 => Ok(String),
            v if v == Bool as u8 => Ok(Bool),
            _ => Err(other),
        }
    }
}

#[async_trait]
/// A `Database` stores data and provides an interface to query that data.
pub trait Database: Debug + Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// writes parsed lines into this database
    async fn write_lines(&self, lines: &[ParsedLine<'_>]) -> Result<(), Self::Error>;

    /// Execute the specified query and return arrow record batches with the result
    async fn query(&self, query: &str) -> Result<Vec<RecordBatch>, Self::Error>;

    /// Fetch the specified table names and columns as Arrow RecordBatches
    async fn table_to_arrow(
        &self,
        table_name: &str,
        columns: &[&str],
    ) -> Result<Vec<RecordBatch>, Self::Error>;
}

#[async_trait]
/// Storage for `Databases` which can be retrieved by name
pub trait DatabaseStore: Debug + Send + Sync {
    /// The type of database that is stored by this DatabaseStore
    type Database: Database;

    /// The type of error this DataBase store generates
    type Error: std::error::Error + Send + Sync + 'static;

    /// Retrieve the database specified by `name` returning None if no
    /// such database exists
    async fn db(&self, name: &str) -> Option<Arc<Self::Database>>;

    /// Retrieve the database specified by `name`, creating it if it
    /// doesn't exist.
    async fn db_or_create(&self, name: &str) -> Result<Arc<Self::Database>, Self::Error>;
}

/// Compatibility: return the database name to use for the specified
/// org and bucket name.
///
/// TODO move to somewhere else / change the traits to take the database name directly
pub fn org_and_bucket_to_database(org: &str, bucket: &str) -> String {
    org.to_owned() + "_" + bucket
}

/// In testing config, the `test_fixtures` module provides a simple
/// reference implementaton of DatabaseSource and Database for use in
/// testing

// Note: I would like to compile this module only in the 'test' cfg,
// but when I do so then other modules can not find them. For example:
//
// error[E0432]: unresolved import `delorean::storage::test_fixtures`
//    --> src/server/write_buffer_routes.rs:340:9
//     |
// 340 |         test_fixtures::TestDatabaseStore
//     |         ^^^^^^^^^^^^^ could not find `test_fixtures` in `storage`
//
//#[cfg(test)]
pub mod test_fixtures {
    use super::{Database, DatabaseStore};
    use arrow::record_batch::RecordBatch;
    use delorean_line_parser::ParsedLine;

    use snafu::Snafu;
    use std::{collections::BTreeMap, sync::Arc};
    use tonic::async_trait;

    use tokio::sync::Mutex;

    #[derive(Debug)]
    pub struct TestDatabase {
        // lines which have been written to this database, in order
        saved_lines: Mutex<Vec<String>>,
    }

    #[derive(Snafu, Debug, Clone, Copy)]
    pub enum TestError {}

    impl Default for TestDatabase {
        fn default() -> Self {
            Self {
                saved_lines: Mutex::new(Vec::new()),
            }
        }
    }

    impl TestDatabase {
        pub fn new() -> Self {
            Self::default()
        }

        /// Get all lines written to this database
        pub async fn get_lines(&self) -> Vec<String> {
            self.saved_lines.lock().await.clone()
        }
    }

    #[async_trait]
    impl Database for TestDatabase {
        type Error = TestError;

        /// writes parsed lines into this database
        async fn write_lines(&self, lines: &[ParsedLine<'_>]) -> Result<(), Self::Error> {
            let mut saved_lines = self.saved_lines.lock().await;
            for line in lines {
                saved_lines.push(line.to_string())
            }
            Ok(())
        }

        /// Execute the specified query and return arrow record batches with the result
        async fn query(&self, _query: &str) -> Result<Vec<RecordBatch>, Self::Error> {
            unimplemented!("query Not yet implemented");
        }

        /// Fetch the specified table names and columns as Arrow RecordBatches
        async fn table_to_arrow(
            &self,
            _table_name: &str,
            _columns: &[&str],
        ) -> Result<Vec<RecordBatch>, Self::Error> {
            unimplemented!("table_to_arrow Not yet implemented");
        }
    }

    #[derive(Debug)]
    pub struct TestDatabaseStore {
        databases: Mutex<BTreeMap<String, Arc<TestDatabase>>>,
    }

    impl TestDatabaseStore {
        pub fn new() -> Self {
            Self::default()
        }
    }

    impl Default for TestDatabaseStore {
        fn default() -> Self {
            Self {
                databases: Mutex::new(BTreeMap::new()),
            }
        }
    }

    #[async_trait]
    impl DatabaseStore for TestDatabaseStore {
        type Database = TestDatabase;
        type Error = TestError;
        /// Retrieve the database specified name
        async fn db(&self, name: &str) -> Option<Arc<Self::Database>> {
            let databases = self.databases.lock().await;

            databases.get(name).cloned()
        }

        /// Retrieve the database specified by name, creating it if it
        /// doesn't exist.
        async fn db_or_create(&self, name: &str) -> Result<Arc<Self::Database>, Self::Error> {
            let mut databases = self.databases.lock().await;

            if let Some(db) = databases.get(name) {
                Ok(db.clone())
            } else {
                let new_db = Arc::new(TestDatabase::new());
                databases.insert(name.to_string(), new_db.clone());
                Ok(new_db)
            }
        }
    }
}
