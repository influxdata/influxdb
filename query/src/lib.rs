//! Contains the IOx query engine
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]

use async_trait::async_trait;
use data_types::{
    chunk_metadata::{ChunkAddr, ChunkId, ChunkOrder},
    delete_predicate::DeletePredicate,
    partition_metadata::{InfluxDbType, TableSummary},
};
use datafusion::physical_plan::SendableRecordBatchStream;
use exec::{stringset::StringSet, IOxSessionContext};
use observability_deps::tracing::{debug, trace};
use predicate::{rpc_predicate::QueryDatabaseMeta, Predicate, PredicateMatch};
use schema::selection::Selection;
use schema::{sort::SortKey, Schema, TIME_COLUMN_NAME};

use hashbrown::HashMap;
use std::{collections::BTreeSet, fmt::Debug, iter::FromIterator, sync::Arc};

pub mod exec;
pub mod frontend;
pub mod func;
pub mod group_by;
pub mod plan;
pub mod provider;
pub mod pruning;
pub mod statistics;
pub mod util;

pub use exec::context::{DEFAULT_CATALOG, DEFAULT_SCHEMA};
use schema::sort::SortKeyBuilder;

/// Trait for an object (designed to be a Chunk) which can provide
/// metadata
pub trait QueryChunkMeta {
    /// Return a reference to the summary of the data
    fn summary(&self) -> Option<&TableSummary>;

    /// return a reference to the summary of the data held in this chunk
    fn schema(&self) -> Arc<Schema>;

    /// return a reference to the sort key if any
    fn sort_key(&self) -> Option<&SortKey>;

    /// return a reference to delete predicates of the chunk
    fn delete_predicates(&self) -> &[Arc<DeletePredicate>];

    /// return true if the chunk has delete predicates
    fn has_delete_predicates(&self) -> bool {
        !self.delete_predicates().is_empty()
    }

    /// return column names participating in the all delete predicates
    /// in lexicographical order with one exception that time column is last
    /// This order is to be consistent with Schema::primary_key
    fn delete_predicate_columns(&self) -> Vec<&str> {
        // get all column names but time
        let mut col_names = BTreeSet::new();
        for pred in self.delete_predicates() {
            for expr in &pred.exprs {
                if expr.column != schema::TIME_COLUMN_NAME {
                    col_names.insert(expr.column.as_str());
                }
            }
        }

        // convert to vector
        let mut column_names = Vec::from_iter(col_names);

        // Now add time column to the end of the vector
        // Since time range is a must in the delete predicate, time column must be in this list
        column_names.push(TIME_COLUMN_NAME);

        column_names
    }
}

/// A `QueryCompletedToken` is returned by `record_query` implementations of
/// a `QueryDatabase`. It is used to trigger side-effects (such as query timing)
/// on query completion.
///
pub struct QueryCompletedToken {
    /// If this query completed successfully
    success: bool,

    /// Function invoked when the token is dropped. It is passed the
    /// vaue of `self.success`
    f: Option<Box<dyn FnOnce(bool) + Send>>,
}

impl Debug for QueryCompletedToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryCompletedToken")
            .field("success", &self.success)
            .finish()
    }
}

impl QueryCompletedToken {
    pub fn new(f: impl FnOnce(bool) + Send + 'static) -> Self {
        Self {
            success: false,
            f: Some(Box::new(f)),
        }
    }

    /// Record that this query completed successfully
    pub fn set_success(&mut self) {
        self.success = true;
    }
}

impl Drop for QueryCompletedToken {
    fn drop(&mut self) {
        if let Some(f) = self.f.take() {
            (f)(self.success)
        }
    }
}

/// Boxed description of a query that knows how to render to a string
///
/// This avoids storing potentially large strings
pub type QueryText = Box<dyn std::fmt::Display + Send + Sync>;

/// A `Database` is the main trait implemented by the IOx subsystems
/// that store actual data.
///
/// Databases store data organized by partitions and each partition stores
/// data in Chunks.
#[async_trait]
pub trait QueryDatabase: QueryDatabaseMeta + Debug + Send + Sync {
    /// Returns a set of chunks within the partition with data that may match
    /// the provided predicate. If possible, chunks which have no rows that can
    /// possibly match the predicate may be omitted.
    async fn chunks(&self, table_name: &str, predicate: &Predicate) -> Vec<Arc<dyn QueryChunk>>;

    /// Record that particular type of query was run / planned
    fn record_query(
        &self,
        ctx: &IOxSessionContext,
        query_type: &str,
        query_text: QueryText,
    ) -> QueryCompletedToken;

    /// Upcast to [`QueryDatabaseMeta`].
    ///
    /// This is required until <https://github.com/rust-lang/rust/issues/65991> is fixed.
    fn as_meta(&self) -> &dyn QueryDatabaseMeta;
}

/// Error type for [`QueryChunk`] operations.
pub type QueryChunkError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Collection of data that shares the same partition key
pub trait QueryChunk: QueryChunkMeta + Debug + Send + Sync + 'static {
    /// returns the Id of this chunk. Ids are unique within a
    /// particular partition.
    fn id(&self) -> ChunkId;

    /// Returns the ChunkAddr of this chunk
    fn addr(&self) -> ChunkAddr;

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
    ) -> Result<PredicateMatch, QueryChunkError>;

    /// Returns a set of Strings with column names from the specified
    /// table that have at least one row that matches `predicate`, if
    /// the predicate can be evaluated entirely on the metadata of
    /// this Chunk. Returns `None` otherwise
    fn column_names(
        &self,
        ctx: IOxSessionContext,
        predicate: &Predicate,
        columns: Selection<'_>,
    ) -> Result<Option<StringSet>, QueryChunkError>;

    /// Return a set of Strings containing the distinct values in the
    /// specified columns. If the predicate can be evaluated entirely
    /// on the metadata of this Chunk. Returns `None` otherwise
    ///
    /// The requested columns must all have String type.
    fn column_values(
        &self,
        ctx: IOxSessionContext,
        column_name: &str,
        predicate: &Predicate,
    ) -> Result<Option<StringSet>, QueryChunkError>;

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
        ctx: IOxSessionContext,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, QueryChunkError>;

    /// Returns chunk type which is either MUB, RUB, OS
    fn chunk_type(&self) -> &str;

    /// Order of this chunk relative to other overlapping chunks.
    fn order(&self) -> ChunkOrder;
}

/// Implement ChunkMeta for something wrapped in an Arc (like Chunks often are)
impl<P> QueryChunkMeta for Arc<P>
where
    P: QueryChunkMeta,
{
    fn summary(&self) -> Option<&TableSummary> {
        self.as_ref().summary()
    }

    fn schema(&self) -> Arc<Schema> {
        self.as_ref().schema()
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.as_ref().sort_key()
    }

    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        let pred = self.as_ref().delete_predicates();
        debug!(?pred, "Delete predicate in QueryChunkMeta");
        pred
    }
}

/// return true if all the chunks inlcude statistics
pub fn chunks_have_stats(chunks: &[Arc<dyn QueryChunk>]) -> bool {
    // If at least one of the provided chunk cannot provide stats,
    // do not need to compute potential duplicates. We will treat
    // as all of them have duplicates
    chunks.iter().all(|c| c.summary().is_some())
}

pub fn compute_sort_key_for_chunks(schema: &Schema, chunks: &[Arc<dyn QueryChunk>]) -> SortKey {
    if !chunks_have_stats(chunks) {
        // chunks have not enough stats, return its pk that is
        // sorted lexicographically but time column always last
        SortKey::from_columns(schema.primary_key())
    } else {
        let summaries = chunks
            .iter()
            .map(|x| x.summary().expect("Chunk should have summary"));
        compute_sort_key(summaries)
    }
}

/// Compute a sort key that orders lower cardinality columns first
///
/// In the absence of more precise information, this should yield a
/// good ordering for RLE compression
pub fn compute_sort_key<'a>(summaries: impl Iterator<Item = &'a TableSummary>) -> SortKey {
    let mut cardinalities: HashMap<&str, u64> = Default::default();
    for summary in summaries {
        for column in &summary.columns {
            if column.influxdb_type != Some(InfluxDbType::Tag) {
                continue;
            }

            let mut cnt = 0;
            if let Some(count) = column.stats.distinct_count() {
                cnt = count.get();
            }
            *cardinalities.entry(column.name.as_str()).or_default() += cnt;
        }
    }

    trace!(cardinalities=?cardinalities, "cardinalities of of columns to compute sort key");

    let mut cardinalities: Vec<_> = cardinalities.into_iter().collect();
    // Sort by (cardinality, column_name) to have deterministic order if same cardinality
    cardinalities.sort_by_key(|x| (x.1, x.0));

    let mut builder = SortKeyBuilder::with_capacity(cardinalities.len() + 1);
    for (col, _) in cardinalities {
        builder = builder.with_col(col)
    }
    builder = builder.with_col(TIME_COLUMN_NAME);

    let key = builder.build();

    trace!(computed_sort_key=?key, "Value of sort key from compute_sort_key");

    key
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
