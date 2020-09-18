#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use delorean_line_parser::ParsedLine;
use std::collections::BTreeSet;

use std::{fmt::Debug, sync::Arc};

pub mod id;

/// Specifies a continuous range of nanosecond timestamps. Timestamp
/// predicates are so common and critical to performance of timeseries
/// databases in general, and delorean in particular, they handled specially
#[derive(Clone, PartialEq, Copy, Debug)]
pub struct TimestampRange {
    /// Start defines the inclusive lower bound.
    pub start: i64,

    /// End defines the exclusive upper bound.
    pub end: i64,
}

#[async_trait]
/// A `Database` stores data and provides an interface to query that data.
pub trait Database: Debug + Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// writes parsed lines into this database
    async fn write_lines(&self, lines: &[ParsedLine<'_>]) -> Result<(), Self::Error>;

    /// Execute the specified query and return arrow record batches with the result
    async fn query(&self, query: &str) -> Result<Vec<RecordBatch>, Self::Error>;

    /// Returns the list of table names in this database. If a
    /// timestamp range is specified, only tables which have data in
    /// the specified time range are returned.
    async fn table_names(
        &self,
        range: Option<TimestampRange>,
    ) -> Result<Arc<BTreeSet<String>>, Self::Error>;

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
pub fn org_and_bucket_to_database(org: impl Into<String>, bucket: &str) -> String {
    org.into() + "_" + bucket
}

// Note: I would like to compile this module only in the 'test' cfg,
// but when I do so then other modules can not find them. For example:
//
// error[E0433]: failed to resolve: could not find `test` in `delorean`
//   --> src/server/write_buffer_routes.rs:353:19
//     |
// 353 |     use delorean_storage_interface::test::TestDatabaseStore;
//     |                                     ^^^^ could not find `test` in `delorean_storage_interface`

//
//#[cfg(test)]
pub mod test;
