//! Contains the IOx query engine
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send,
    clippy::todo,
    clippy::dbg_macro
)]

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use data_types::{ChunkId, ChunkOrder, DeletePredicate, InfluxDbType, PartitionId, TableSummary};
use datafusion::{error::DataFusionError, prelude::SessionContext};
use exec::{stringset::StringSet, IOxSessionContext};
use hashbrown::HashMap;
use observability_deps::tracing::{debug, trace};
use parquet_file::storage::ParquetExecInput;
use predicate::{rpc_predicate::QueryNamespaceMeta, Predicate, PredicateMatch};
use schema::{
    sort::{SortKey, SortKeyBuilder},
    Projection, Schema, TIME_COLUMN_NAME,
};
use std::{any::Any, collections::BTreeSet, fmt::Debug, iter::FromIterator, sync::Arc};

pub mod exec;
pub mod frontend;
pub mod logical_optimizer;
pub mod plan;
pub mod provider;
pub mod pruning;
pub mod statistics;
pub mod util;

pub use frontend::common::ScanPlanBuilder;
pub use query_functions::group_by::{Aggregate, WindowDuration};

/// Trait for an object (designed to be a Chunk) which can provide
/// metadata
pub trait QueryChunkMeta {
    /// Return a summary of the data
    fn summary(&self) -> Arc<TableSummary>;

    /// return a reference to the summary of the data held in this chunk
    fn schema(&self) -> &Schema;

    /// Return a reference to the chunk's partition sort key if any.
    /// Only persisted chunk has its partition sort key
    fn partition_sort_key(&self) -> Option<&SortKey>;

    /// Return partition id for this chunk
    fn partition_id(&self) -> PartitionId;

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
/// a `QueryNamespace`. It is used to trigger side-effects (such as query timing)
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

/// `QueryNamespace` is the main trait implemented by the IOx subsystems that store actual data.
///
/// Namespaces store data organized by partitions and each partition stores data in Chunks.
#[async_trait]
pub trait QueryNamespace: QueryNamespaceMeta + Debug + Send + Sync {
    /// Returns a set of chunks within the partition with data that may match the provided
    /// predicate.
    ///
    /// If possible, chunks which have no rows that can possibly match the predicate may be omitted.
    ///
    /// If projection is `None`, returned chunks will include all columns of its original data.
    /// Otherwise, returned chunks will include PK columns (tags and time) and columns specified in
    /// the projection. Projecting chunks here is optional and a mere optimization. The query
    /// subsystem does NOT rely on it.
    async fn chunks(
        &self,
        table_name: &str,
        predicate: &Predicate,
        projection: Option<&Vec<usize>>,
        ctx: IOxSessionContext,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError>;

    /// Record that particular type of query was run / planned
    fn record_query(
        &self,
        ctx: &IOxSessionContext,
        query_type: &str,
        query_text: QueryText,
    ) -> QueryCompletedToken;

    /// Upcast to [`QueryNamespaceMeta`].
    ///
    /// This is required until <https://github.com/rust-lang/rust/issues/65991> is fixed.
    fn as_meta(&self) -> &dyn QueryNamespaceMeta;
}

/// Raw data of a [`QueryChunk`].
#[derive(Debug)]
pub enum QueryChunkData {
    /// In-memory record batches.
    ///
    /// **IMPORTANT: All batches MUST have the schema that the [chunk reports](QueryChunkMeta::schema).**
    RecordBatches(Vec<RecordBatch>),

    /// Parquet file.
    ///
    /// See [`ParquetExecInput`] for details.
    Parquet(ParquetExecInput),
}

impl QueryChunkData {
    /// Read data into [`RecordBatch`]es. This is mostly meant for testing!
    pub async fn read_to_batches(
        self,
        schema: &Schema,
        session_ctx: &SessionContext,
    ) -> Vec<RecordBatch> {
        match self {
            Self::RecordBatches(batches) => batches,
            Self::Parquet(exec_input) => exec_input
                .read_to_batches(schema.as_arrow(), Projection::All, session_ctx)
                .await
                .unwrap(),
        }
    }
}

/// Collection of data that shares the same partition key
pub trait QueryChunk: QueryChunkMeta + Debug + Send + Sync + 'static {
    /// returns the Id of this chunk. Ids are unique within a
    /// particular partition.
    fn id(&self) -> ChunkId;

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
    ) -> Result<PredicateMatch, DataFusionError> {
        Ok(predicate.apply_to_table_summary(&self.summary(), self.schema().as_arrow()))
    }

    /// Returns a set of Strings with column names from the specified
    /// table that have at least one row that matches `predicate`, if
    /// the predicate can be evaluated entirely on the metadata of
    /// this Chunk. Returns `None` otherwise
    fn column_names(
        &self,
        ctx: IOxSessionContext,
        predicate: &Predicate,
        columns: Projection<'_>,
    ) -> Result<Option<StringSet>, DataFusionError>;

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
    ) -> Result<Option<StringSet>, DataFusionError>;

    /// Provides access to raw [`QueryChunk`] data.
    ///
    /// The engine assume that minimal work shall be performed to gather the `QueryChunkData`.
    fn data(&self) -> QueryChunkData;

    /// Returns chunk type. Useful in tests and debug logs.
    fn chunk_type(&self) -> &str;

    /// Order of this chunk relative to other overlapping chunks.
    fn order(&self) -> ChunkOrder;

    /// Return backend as [`Any`] which can be used to downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
}

/// Implement ChunkMeta for something wrapped in an Arc (like Chunks often are)
impl<P> QueryChunkMeta for Arc<P>
where
    P: QueryChunkMeta,
{
    fn summary(&self) -> Arc<TableSummary> {
        self.as_ref().summary()
    }

    fn schema(&self) -> &Schema {
        self.as_ref().schema()
    }

    fn partition_id(&self) -> PartitionId {
        self.as_ref().partition_id()
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.as_ref().sort_key()
    }

    fn partition_sort_key(&self) -> Option<&SortKey> {
        self.as_ref().partition_sort_key()
    }

    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        let pred = self.as_ref().delete_predicates();
        debug!(?pred, "Delete predicate in QueryChunkMeta");
        pred
    }
}

/// Implement `ChunkMeta` for `Arc<dyn QueryChunk>`
impl QueryChunkMeta for Arc<dyn QueryChunk> {
    fn summary(&self) -> Arc<TableSummary> {
        self.as_ref().summary()
    }

    fn schema(&self) -> &Schema {
        self.as_ref().schema()
    }

    fn partition_id(&self) -> PartitionId {
        self.as_ref().partition_id()
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.as_ref().sort_key()
    }

    fn partition_sort_key(&self) -> Option<&SortKey> {
        self.as_ref().partition_sort_key()
    }

    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        let pred = self.as_ref().delete_predicates();
        debug!(?pred, "Delete predicate in QueryChunkMeta");
        pred
    }
}

/// return true if all the chunks include distinct counts for all columns.
pub fn chunks_have_distinct_counts<'a>(
    chunks: impl IntoIterator<Item = &'a Arc<dyn QueryChunk>>,
) -> bool {
    // If at least one of the provided chunk cannot provide stats,
    // do not need to compute potential duplicates. We will treat
    // as all of them have duplicates
    chunks.into_iter().all(|chunk| {
        chunk
            .summary()
            .columns
            .iter()
            .all(|col| col.stats.distinct_count().is_some())
    })
}

pub fn compute_sort_key_for_chunks<'a>(
    schema: &Schema,
    chunks: impl Copy + IntoIterator<Item = &'a Arc<dyn QueryChunk>>,
) -> SortKey {
    if !chunks_have_distinct_counts(chunks) {
        // chunks have not enough stats, return its pk that is
        // sorted lexicographically but time column always last
        SortKey::from_columns(schema.primary_key())
    } else {
        let summaries = chunks.into_iter().map(|x| x.summary());
        compute_sort_key(summaries)
    }
}

/// Compute a sort key that orders lower _estimated_ cardinality columns first
///
/// In the absence of more precise information, this should yield a
/// good ordering for RLE compression.
///
/// The cardinality is estimated by the sum of unique counts over all summaries. This may overestimate cardinality since
/// it does not account for shared/repeated values.
fn compute_sort_key(summaries: impl Iterator<Item = Arc<TableSummary>>) -> SortKey {
    let mut cardinalities: HashMap<String, u64> = Default::default();
    for summary in summaries {
        for column in &summary.columns {
            if column.influxdb_type != InfluxDbType::Tag {
                continue;
            }

            let mut cnt = 0;
            if let Some(count) = column.stats.distinct_count() {
                cnt = count.get();
            }
            *cardinalities.entry_ref(column.name.as_str()).or_default() += cnt;
        }
    }

    trace!(cardinalities=?cardinalities, "cardinalities of of columns to compute sort key");

    let mut cardinalities: Vec<_> = cardinalities.into_iter().collect();
    // Sort by (cardinality, column_name) to have deterministic order if same cardinality
    cardinalities
        .sort_by(|(name_1, card_1), (name_2, card_2)| (card_1, name_1).cmp(&(card_2, name_2)));

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
// 353 |     use iox_query::test::TestDatabaseStore;
//     |                ^^^^ could not find `test` in `query`

//
//#[cfg(test)]
pub mod test;
