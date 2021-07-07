#![deny(broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]

use async_trait::async_trait;
use data_types::{chunk_metadata::ChunkSummary, partition_metadata::TableSummary};
use datafusion::physical_plan::SendableRecordBatchStream;
use exec::{stringset::StringSet, Executor};
use internal_types::{schema::Schema, selection::Selection};
use predicate::PredicateMatch;

use std::{fmt::Debug, sync::Arc};

pub mod exec;
pub mod frontend;
pub mod func;
pub mod group_by;
pub mod plan;
pub mod predicate;
pub mod provider;
pub mod pruning;
pub mod util;

pub use exec::context::{DEFAULT_CATALOG, DEFAULT_SCHEMA};

use self::predicate::Predicate;

/// Trait for an object (designed to be a Chunk) which can provide
/// metadata
pub trait QueryChunkMeta: Sized {
    /// Return a reference to the summary of the data
    fn summary(&self) -> &TableSummary;

    /// return a reference to the summary of the data held in this chunk
    fn schema(&self) -> Arc<Schema>;
}

/// A `Database` is the main trait implemented by the IOx subsystems
/// that store actual data.
///
/// Databases store data organized by partitions and each partition stores
/// data in Chunks.
///
/// TODO: Move all Query and Line Protocol specific things out of this
/// trait and into the various query planners.
pub trait QueryDatabase: Debug + Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    type Chunk: QueryChunk;

    /// Return the partition keys for data in this DB
    fn partition_keys(&self) -> Result<Vec<String>, Self::Error>;

    /// Schema for a specific table if the table exists.
    fn table_schema(&self, table_name: &str) -> Option<Schema>;

    /// Returns a set of chunks within the partition with data that may match
    /// the provided predicate. If possible, chunks which have no rows that can
    /// possibly match the predicate may be omitted.
    fn chunks(&self, predicate: &Predicate) -> Vec<Arc<Self::Chunk>>;

    /// Return a summary of all chunks in this database, in all partitions
    fn chunk_summaries(&self) -> Result<Vec<ChunkSummary>, Self::Error>;
}

/// Collection of data that shares the same partition key
pub trait QueryChunk: QueryChunkMeta + Debug + Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// returns the Id of this chunk. Ids are unique within a
    /// particular partition.
    fn id(&self) -> u32;

    /// Returns the name of the table stored in this chunk
    fn table_name(&self) -> &str;

    /// Returns true if the chunk may contain a duplicate "primary
    /// key" within itself
    fn may_contain_pk_duplicates(&self) -> bool;

    /// Returns the result of applying the `predicate` to the chunk
    /// using an efficient, but inexact method, based on metadata.
    ///
    /// NOTE: This method is suitable for calling during planning, and
    /// may return PredicateMatch::Unknown for certain types of
    /// predicates.
    fn apply_predicate_to_metadata(
        &self,
        predicate: &Predicate,
    ) -> Result<PredicateMatch, Self::Error>;

    /// Returns a set of Strings with column names from the specified
    /// table that have at least one row that matches `predicate`, if
    /// the predicate can be evaluated entirely on the metadata of
    /// this Chunk. Returns `None` otherwise
    fn column_names(
        &self,
        predicate: &Predicate,
        columns: Selection<'_>,
    ) -> Result<Option<StringSet>, Self::Error>;

    /// Return a set of Strings containing the distinct values in the
    /// specified columns. If the predicate can be evaluated entirely
    /// on the metadata of this Chunk. Returns `None` otherwise
    ///
    /// The requested columns must all have String type.
    fn column_values(
        &self,
        column_name: &str,
        predicate: &Predicate,
    ) -> Result<Option<StringSet>, Self::Error>;

    /// Provides access to raw `QueryChunk` data as an
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
    /// streams from several different `QueryChunk`s.
    fn read_filter(
        &self,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, Self::Error>;

    /// Returns true if data of this chunk is sorted
    fn is_sorted_on_pk(&self) -> bool;
}

#[async_trait]
/// Storage for `Databases` which can be retrieved by name
pub trait DatabaseStore: Debug + Send + Sync {
    /// The type of database that is stored by this DatabaseStore
    type Database: QueryDatabase;

    /// The type of error this DataBase store generates
    type Error: std::error::Error + Send + Sync + 'static;

    /// List the database names.
    fn db_names_sorted(&self) -> Vec<String>;

    /// Retrieve the database specified by `name` returning None if no
    /// such database exists
    fn db(&self, name: &str) -> Option<Arc<Self::Database>>;

    /// Retrieve the database specified by `name`, creating it if it
    /// doesn't exist.
    async fn db_or_create(&self, name: &str) -> Result<Arc<Self::Database>, Self::Error>;

    /// Provide a query executor to use for running queries on
    /// databases in this `DatabaseStore`
    fn executor(&self) -> Arc<Executor>;
}

/// Implement ChunkMeta for something wrapped in an Arc (like Chunks often are)
impl<P> QueryChunkMeta for Arc<P>
where
    P: QueryChunkMeta,
{
    fn summary(&self) -> &TableSummary {
        self.as_ref().summary()
    }

    fn schema(&self) -> Arc<Schema> {
        self.as_ref().schema()
    }
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
