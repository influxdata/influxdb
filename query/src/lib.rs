#![deny(rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use arrow_deps::datafusion::physical_plan::SendableRecordBatchStream;
use async_trait::async_trait;
use data_types::{
    data::ReplicatedWrite, partition_metadata::TableSummary, schema::Schema, selection::Selection,
};
use exec::{stringset::StringSet, Executor};
use plan::seriesset::SeriesSetPlans;

use std::{fmt::Debug, sync::Arc};

pub mod exec;
pub mod frontend;
pub mod func;
pub mod group_by;
pub mod plan;
pub mod predicate;
pub mod provider;
pub mod util;

use self::{group_by::GroupByAndAggregate, predicate::Predicate};

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
    fn partition_keys(&self) -> Result<Vec<String>, Self::Error>;

    /// Returns a covering set of chunks in the specified partition. A
    /// covering set means that together the chunks make up a single
    /// complete copy of the data being queried.
    fn chunks(&self, partition_key: &str) -> Vec<Arc<Self::Chunk>>;

    // ----------
    // The functions below are slated for removal (migration into a gRPC query
    // frontend) ---------

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
#[async_trait]
pub trait PartitionChunk: Debug + Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// returns the Id of this chunk. Ids are unique within a
    /// particular partition.
    fn id(&self) -> u32;

    /// returns the partition metadata stats for every table in the partition
    fn table_stats(&self) -> Result<Vec<TableSummary>, Self::Error>;

    /// Returns true if this chunk *might* have data that passes the
    /// predicate. If false is returned, this chunk can be
    /// skipped entirely. If true is returned, there still may not be
    /// rows that match.
    ///
    /// This is used during query planning to skip including entire chunks
    fn could_pass_predicate(&self, _predicate: &Predicate) -> Result<bool, Self::Error> {
        Ok(true)
    }

    /// Returns true if this chunk contains data for the specified table
    fn has_table(&self, table_name: &str) -> bool;

    /// Returns all table names from this chunk that have at least one
    /// row that matches the `predicate` and are not already in `known_tables`.
    ///
    /// If the predicate cannot be evaluated (e.g it has predicates
    /// that cannot be directly evaluated in the chunk), `None` is
    /// returned.
    ///
    /// `known_tables` is a list of table names already known to be in
    /// other chunks from the same partition. It may be empty or
    /// contain `table_names` not in this chunk.
    async fn table_names(
        &self,
        predicate: &Predicate,
        known_tables: &StringSet,
    ) -> Result<Option<StringSet>, Self::Error>;

    /// Returns a set of Strings with column names from the specified
    /// table that have at least one row that matches `predicate`, if
    /// the predicate can be evaluated entirely on the metadata of
    /// this Chunk. Returns `None` otherwise
    async fn column_names(
        &self,
        table_name: &str,
        predicate: &Predicate,
        columns: Selection<'_>,
    ) -> Result<Option<StringSet>, Self::Error>;

    /// Return a set of Strings containing the distinct values in the
    /// specified columns. If the predicate can be evaluated entirely
    /// on the metadata of this Chunk. Returns `None` otherwise
    ///
    /// The requested columns must all have String type.
    async fn column_values(
        &self,
        table_name: &str,
        column_name: &str,
        predicate: &Predicate,
    ) -> Result<Option<StringSet>, Self::Error>;

    /// Returns the Schema for a table in this chunk, with the
    /// specified column selection. An error is returned if the
    /// selection refers to columns that do not exist.
    async fn table_schema(
        &self,
        table_name: &str,
        selection: Selection<'_>,
    ) -> Result<Schema, Self::Error>;

    /// Provides access to raw `PartitionChunk` data as an
    /// asynchronous stream of `RecordBatch`es filtered by a *required*
    /// predicate. Note that not all chunks can evaluate all types of
    /// predicates and this function will return an error
    /// if requested to evaluate with a predicate that is not supported
    ///
    /// This is the analog of the `TableProvider` in DataFusion
    ///
    /// The reason we can't simply use the `TableProvider` trait
    /// directly is that the data for a particular Table lives in
    /// several chunks within a partition, so there needs to be an
    /// implementation of `TableProvider` that stitches together the
    /// streams from several different `PartitionChunks`.
    async fn read_filter(
        &self,
        table_name: &str,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, Self::Error>;
}

#[async_trait]
/// Storage for `Databases` which can be retrieved by name
pub trait DatabaseStore: Debug + Send + Sync {
    /// The type of database that is stored by this DatabaseStore
    type Database: Database;

    /// The type of error this DataBase store generates
    type Error: std::error::Error + Send + Sync + 'static;

    /// List the database names.
    async fn db_names_sorted(&self) -> Vec<String>;

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
