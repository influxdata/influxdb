#![deny(rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

use arrow_deps::arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use data_types::{data::ReplicatedWrite, partition_metadata::Table as TableStats};
use exec::{Executor, FieldListPlan, SeriesSetPlans, StringSetPlan};

use std::{fmt::Debug, sync::Arc};

pub mod exec;
pub mod frontend;
pub mod func;
pub mod group_by;
pub mod predicate;
pub mod util;

use self::group_by::GroupByAndAggregate;
use self::predicate::{Predicate, TimestampRange};

/// A `Database` is the main trait implemented by the IOx subsystems
/// that store actual data.
///
/// Databases store data organized by partitions and each partition stores
/// data in Chunks.
///
/// TODO: Move all Query and Line Protocol specific things out of this
/// trait and into the various query planners.
#[async_trait]
pub trait Database: Debug + Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    type Chunk: PartitionChunk;

    /// Stores the replicated write into the database.
    async fn store_replicated_write(&self, write: &ReplicatedWrite) -> Result<(), Self::Error>;

    /// Return the partition keys for data in this DB
    async fn partition_keys(&self) -> Result<Vec<String>, Self::Error>;

    /// Returns a covering set of chunks in the specified partition. A
    /// covering set means that together the chunks make up a single
    /// complete copy of the data being queried.
    async fn chunks(&self, partition_key: &str) -> Vec<Arc<Self::Chunk>>;

    // ----------
    // The functions below are slated for removal (migration into a gRPC query
    // frontend) ---------

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

/// Collection of data that shares the same partition key
pub trait PartitionChunk: Debug + Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// returns the Id of this chunk. Ids are unique within a
    /// particular partition.
    fn id(&self) -> u32;

    /// returns the partition metadata stats for every table in the partition
    fn table_stats(&self) -> Result<Vec<TableStats>, Self::Error>;

    /// converts the table to an Arrow RecordBatch and writes to dst
    /// TODO turn this into a streaming interface
    fn table_to_arrow(
        &self,
        dst: &mut Vec<RecordBatch>,
        table_name: &str,
        columns: &[&str],
    ) -> Result<(), Self::Error>;
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

    /// Provide a query executor to use for running queries on
    /// databases in this `DatabaseStore`
    fn executor(&self) -> Arc<Executor>;
}

// Note: I would like to compile this module only in the 'test' cfg,
// but when I do so then other modules can not find them. For example:
//
// error[E0433]: failed to resolve: could not find `test` in `storage`
//   --> src/server/mutable_buffer_routes.rs:353:19
//     |
// 353 |     use query::test::TestDatabaseStore;
//     |                ^^^^ could not find `test` in `query`

//
//#[cfg(test)]
pub mod test;
