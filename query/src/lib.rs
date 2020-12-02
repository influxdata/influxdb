#![deny(rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

use arrow_deps::arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use data_types::data::ReplicatedWrite;
use exec::{FieldListPlan, SeriesSetPlans, StringSetPlan};
use influxdb_line_protocol::ParsedLine;

use std::{fmt::Debug, sync::Arc};

pub mod exec;
pub mod group_by;
pub mod id;
pub mod predicate;
pub mod util;
pub mod window;

use self::group_by::GroupByAndAggregate;
use self::predicate::{Predicate, TimestampRange};

/// A `TSDatabase` describes something that Timeseries data using the
/// InfluxDB Line Protocol data model (`ParsedLine` structures) and
/// provides an interface to query that data. The query methods on
/// this trait such as `tag_columns are specific to this data model.
///
/// The IOx storage engine implements this trait to provide Timeseries
/// specific queries, but also provides more generic access to the same
/// underlying data via other interfaces (e.g. SQL).
///
/// The InfluxDB Timeseries data model can can be thought of as a
/// relational database table where each column has both a type as
/// well as one of the following categories:
///
/// * Tag (always String type)
/// * Field (Float64, Int64, UInt64, String, or Bool)
/// * Time (Int64)
///
/// While the underlying storage is the same for columns in different
/// categories with the same data type, columns of different
/// categories are treated differently in the different query types.
#[async_trait]
pub trait TSDatabase: Debug + Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// writes parsed lines into this database
    async fn write_lines(&self, lines: &[ParsedLine<'_>]) -> Result<(), Self::Error>;

    /// Stores the replicated write in the write buffer and, if enabled, the write ahead log.
    async fn store_replicated_write(&self, write: &ReplicatedWrite) -> Result<(), Self::Error>;

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
    async fn field_column_names(&self, predicate: Predicate) -> Result<FieldListPlan, Self::Error>;

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

    /// Returns a plan that finds rows which pass the conditions
    /// specified by `predicate` and have been logically grouped and
    /// aggregate according to `gby_agg`.
    ///
    /// Each time series is defined by the unique values in a set of
    /// tag columns, and each field in the set of field columns. Each
    /// group is is defined by unique combinations of the columns
    /// in `group_columns` or an optional time window.
    async fn query_groups(
        &self,
        predicate: Predicate,
        gby_agg: GroupByAndAggregate,
    ) -> Result<SeriesSetPlans, Self::Error>;
}

#[async_trait]
pub trait SQLDatabase: Debug + Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Execute the specified query and return arrow record batches with the result
    async fn query(&self, query: &str) -> Result<Vec<RecordBatch>, Self::Error>;
}

#[async_trait]
/// Storage for `Databases` which can be retrieved by name
pub trait DatabaseStore: Debug + Send + Sync {
    /// The type of database that is stored by this DatabaseStore
    type Database: TSDatabase + SQLDatabase;

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
// error[E0433]: failed to resolve: could not find `test` in `storage`
//   --> src/server/write_buffer_routes.rs:353:19
//     |
// 353 |     use query::test::TestDatabaseStore;
//     |                ^^^^ could not find `test` in `query`

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
