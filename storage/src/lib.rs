#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

use arrow_deps::arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use data_types::data::ReplicatedWrite;
use exec::{FieldListPlan, GroupedSeriesSetPlans, SeriesSetPlans, StringSetPlan};
use influxdb_line_protocol::ParsedLine;

use std::{fmt::Debug, sync::Arc};

pub mod exec;
pub mod id;
pub mod predicate;
pub mod util;

use self::predicate::{Predicate, TimestampRange};

#[async_trait]

/// A `Database` stores data and provides an interface to query that data.
///
/// It is like a relational database table where each column has a
/// datatype as well as being in one of the following categories:
///
/// Tag (always string columns)
/// Field (Float64, Int64, UInt64, String, or Bool)
/// Time (Float64)
///
/// While the underlying storage is the same for all columns and they
/// can retrieved as Arrow columns, the field type is used to select
/// certain columns in some query types.
pub trait Database: Debug + Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// writes parsed lines into this database
    async fn write_lines(&self, lines: &[ParsedLine<'_>]) -> Result<(), Self::Error>;

    /// Stores the replicated write in the write buffer and, if enabled, the write ahead log.
    async fn store_replicated_write(&self, write: &ReplicatedWrite) -> Result<(), Self::Error>;

    /// Execute the specified query and return arrow record batches with the result
    async fn query(&self, query: &str) -> Result<Vec<RecordBatch>, Self::Error>;

    /// Returns a plan that lists the names of tables in this
    /// database that have at least one row that matches the
    /// conditions listed on `predicate`
    async fn table_names(&self, predicate: Predicate) -> Result<StringSetPlan, Self::Error>;

    /// Returns a plan that produces the names of "tag" columns (as
    /// defined in the data written via `write_lines`)) names in this
    /// database, and have more than zero rows which pass the
    /// conditions specified by `predicate`.
    async fn tag_column_names(&self, predicate: Predicate) -> Result<StringSetPlan, Self::Error>;

    /// Returns a plan that produces a list of column names in this
    /// database which store fields (as defined in the data written
    /// via `write_lines`), and which have at least one row which
    /// matches the conditions listed on `predicate`.
    async fn field_columns(&self, predicate: Predicate) -> Result<FieldListPlan, Self::Error>;

    /// Returns a plan which finds the distinct values in the
    /// `column_name` column of this database which pass the
    /// conditions specified by `predicate`.
    async fn column_values(
        &self,
        column_name: &str,
        predicate: Predicate,
    ) -> Result<StringSetPlan, Self::Error>;

    /// Returns a plan that finds all rows rows which pass the
    /// conditions specified by `predicate` in the form of logical
    /// time series.
    ///
    /// A time series is defined by the unique values in a set of
    /// "tag_columns" for each field in the "field_columns", orderd by
    /// the time column.
    async fn query_series(&self, predicate: Predicate) -> Result<SeriesSetPlans, Self::Error>;

    /// Returns a plan that finds sets of rows which pass the
    /// conditions specified by `predicate` and which form groups of
    /// logical time series.
    ///
    /// Each time series is defined by the unique values in a set of
    /// "tag_columns" for each field in the "field_columns". Each
    /// group is is defined by unique combinations of the columns
    /// in `group_columns`
    async fn query_groups(
        &self,
        predicate: Predicate,
        group_columns: Vec<String>,
    ) -> Result<GroupedSeriesSetPlans, Self::Error>;

    /// Fetch the specified table names and columns as Arrow
    /// RecordBatches. Columns are returned in the order specified.
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
// 353 |     use storage::test::TestDatabaseStore;
//     |                  ^^^^ could not find `test` in `delorean_storage`

//
//#[cfg(test)]
pub mod test;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_range_contains() {
        let range = TimestampRange::new(100, 200);
        assert!(!range.contains(99));
        assert!(range.contains(100));
        assert!(range.contains(101));
        assert!(range.contains(199));
        assert!(!range.contains(200));
        assert!(!range.contains(201));
    }

    #[test]
    fn test_timestamp_range_contains_opt() {
        let range = TimestampRange::new(100, 200);
        assert!(!range.contains_opt(Some(99)));
        assert!(range.contains_opt(Some(100)));
        assert!(range.contains_opt(Some(101)));
        assert!(range.contains_opt(Some(199)));
        assert!(!range.contains_opt(Some(200)));
        assert!(!range.contains_opt(Some(201)));

        assert!(!range.contains_opt(None));
    }
}
