#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

use async_trait::async_trait;
use delorean_arrow::{arrow::record_batch::RecordBatch, datafusion::logical_plan::Expr};
use delorean_data_types::data::ReplicatedWrite;
use delorean_line_parser::ParsedLine;
use exec::{GroupedSeriesSetPlans, SeriesSetPlans, StringSetPlan};

use std::{fmt::Debug, sync::Arc};

pub mod exec;
pub mod id;
pub mod util;

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

impl TimestampRange {
    pub fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    #[inline]
    /// Returns true if this range contains the value v
    pub fn contains(&self, v: i64) -> bool {
        self.start <= v && v < self.end
    }

    #[inline]
    /// Returns true if this range contains the value v
    pub fn contains_opt(&self, v: Option<i64>) -> bool {
        Some(true) == v.map(|ts| self.contains(ts))
    }
}

/// Represents a general purpose predicate for evaluation
#[derive(Clone, Debug)]
pub struct Predicate {
    /// An expresson using the DataFusion expression operations.
    pub expr: Expr,
}

#[async_trait]
/// A `Database` stores data and provides an interface to query that data.
pub trait Database: Debug + Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// writes parsed lines into this database
    async fn write_lines(&self, lines: &[ParsedLine<'_>]) -> Result<(), Self::Error>;

    /// Stores the replicated write in the write buffer and, if enabled, the write ahead log.
    async fn store_replicated_write(&self, write: &ReplicatedWrite) -> Result<(), Self::Error>;

    /// Execute the specified query and return arrow record batches with the result
    async fn query(&self, query: &str) -> Result<Vec<RecordBatch>, Self::Error>;

    /// Returns the list of table names in this database.
    ///
    /// If `range` is specified, only tables which have data in the
    /// specified timestamp range are included.
    ///
    /// TODO: change this to return a StringSetPlan
    async fn table_names(
        &self,
        range: Option<TimestampRange>,
    ) -> Result<StringSetPlan, Self::Error>;

    /// Returns the list of column names in this database which store
    /// tags (as defined in the ParsedLines when written), and which
    /// have rows that match optional predicates.
    ///
    /// If `table` is specified, then only columns from the
    /// specified database which match other predictes are included.
    ///
    /// If `range` is specified, only columns which have data in the
    /// specified timestamp range which match other predictes are
    /// included.
    ///
    /// If `predicate` is specified, then only columns which have at
    /// least one non-null value in any row that matches the predicate
    /// are returned
    async fn tag_column_names(
        &self,
        table: Option<String>,
        range: Option<TimestampRange>,
        predicate: Option<Predicate>,
    ) -> Result<StringSetPlan, Self::Error>;

    /// Returns a plan which can find the distinct values in the
    /// `column_name` column of this database for rows that match
    /// optional predicates.
    ///
    /// If `table` is specified, then only values from the
    /// specified database which match other predictes are included.
    ///
    /// If `range` is specified, only values which have data in the
    /// specified timestamp range which match other predictes are
    /// included.
    ///
    /// If `predicate` is specified, then only values which have at
    /// least one non-null value in any row that matches the predicate
    /// are returned
    async fn column_values(
        &self,
        column_name: &str,
        table: Option<String>,
        range: Option<TimestampRange>,
        predicate: Option<Predicate>,
    ) -> Result<StringSetPlan, Self::Error>;

    /// Returns a plan that finds sets of rows which form logical time
    /// series. Each series is defined by the unique values in a set
    /// of "tag_columns" for each field in the "field_columns"
    ///
    /// If `range` is specified, only rows which have data in the
    /// specified timestamp range which match other predictes are
    /// included.
    ///
    /// If `predicate` is specified, then only rows which have at
    /// least one non-null value in any row that matches the predicate
    /// are returned
    async fn query_series(
        &self,
        range: Option<TimestampRange>,
        predicate: Option<Predicate>,
    ) -> Result<SeriesSetPlans, Self::Error>;

    /// Returns a plan that finds sets of rows which form groups of
    /// logical time series. Each series is defined by the unique
    /// values in a set of "tag_columns" for each field in the
    /// "field_columns". Each group is is defined by unique
    /// combinations of the columns described
    ///
    /// If `range` is specified, only rows which have data in the
    /// specified timestamp range which match other predictes are
    /// included.
    ///
    /// If `predicate` is specified, then only rows which have at
    /// least one non-null value in any row that matches the predicate
    /// are returned
    async fn query_groups(
        &self,
        range: Option<TimestampRange>,
        predicate: Option<Predicate>,
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
// 353 |     use delorean_storage::test::TestDatabaseStore;
//     |                           ^^^^ could not find `test` in `delorean_storage`

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
