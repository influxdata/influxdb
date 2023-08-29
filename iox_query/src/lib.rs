//! Contains the IOx query engine
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use arrow::{
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use data_types::{ChunkId, ChunkOrder, TransitionPartitionId};
use datafusion::{
    error::DataFusionError,
    physical_plan::Statistics,
    prelude::{Expr, SessionContext},
};
use exec::IOxSessionContext;
use once_cell::sync::Lazy;
use parquet_file::storage::ParquetExecInput;
use schema::{sort::SortKey, Projection, Schema};
use std::{any::Any, fmt::Debug, sync::Arc};

pub mod chunk_statistics;
pub mod config;
pub mod exec;
pub mod frontend;
pub mod logical_optimizer;
pub mod physical_optimizer;
pub mod plan;
pub mod provider;
pub mod pruning;
pub mod statistics;
pub mod util;

pub use query_functions::group_by::{Aggregate, WindowDuration};

/// The name of the virtual column that represents the chunk order.
pub const CHUNK_ORDER_COLUMN_NAME: &str = "__chunk_order";

static CHUNK_ORDER_FIELD: Lazy<Arc<Field>> =
    Lazy::new(|| Arc::new(Field::new(CHUNK_ORDER_COLUMN_NAME, DataType::Int64, false)));

/// Generate [`Field`] for [chunk order column](CHUNK_ORDER_COLUMN_NAME).
pub fn chunk_order_field() -> Arc<Field> {
    Arc::clone(&CHUNK_ORDER_FIELD)
}

/// A single chunk of data.
pub trait QueryChunk: Debug + Send + Sync + 'static {
    /// Return a statistics of the data
    fn stats(&self) -> Arc<Statistics>;

    /// return a reference to the summary of the data held in this chunk
    fn schema(&self) -> &Schema;

    /// Return partition identifier for this chunk
    fn partition_id(&self) -> &TransitionPartitionId;

    /// return a reference to the sort key if any
    fn sort_key(&self) -> Option<&SortKey>;

    /// returns the Id of this chunk. Ids are unique within a
    /// particular partition.
    fn id(&self) -> ChunkId;

    /// Returns true if the chunk may contain a duplicate "primary
    /// key" within itself
    fn may_contain_pk_duplicates(&self) -> bool;

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
pub trait QueryNamespace: Debug + Send + Sync {
    /// Returns a set of chunks within the partition with data that may match the provided
    /// filter expression.
    ///
    /// If possible, chunks which have no rows that can possibly match the filter may be omitted.
    ///
    /// If projection is `None`, returned chunks will include all columns of its original data.
    /// Otherwise, returned chunks will include PK columns (tags and time) and columns specified in
    /// the projection. Projecting chunks here is optional and a mere optimization. The query
    /// subsystem does NOT rely on it.
    async fn chunks(
        &self,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: IOxSessionContext,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError>;

    /// Retention cutoff time.
    ///
    /// This gives the timestamp (NOT the duration) at which data should be cut off. This should result in an additional
    /// filter of the following form:
    ///
    /// ```text
    /// time >= retention_time_ns
    /// ```
    ///
    /// Returns `None` if now retention policy was defined.
    fn retention_time_ns(&self) -> Option<i64>;

    /// Record that particular type of query was run / planned
    fn record_query(
        &self,
        ctx: &IOxSessionContext,
        query_type: &str,
        query_text: QueryText,
    ) -> QueryCompletedToken;

    /// Returns a new execution context suitable for running queries
    fn new_query_context(&self, span_ctx: Option<trace::ctx::SpanContext>) -> IOxSessionContext;
}

/// Raw data of a [`QueryChunk`].
#[derive(Debug, Clone)]
pub enum QueryChunkData {
    /// In-memory record batches.
    ///
    /// **IMPORTANT: All batches MUST have the schema that the [chunk reports](QueryChunk::schema).**
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

    /// Extract [record batches](Self::RecordBatches) variant.
    pub fn into_record_batches(self) -> Option<Vec<RecordBatch>> {
        match self {
            Self::RecordBatches(batches) => Some(batches),
            Self::Parquet(_) => None,
        }
    }
}

impl<P> QueryChunk for Arc<P>
where
    P: QueryChunk,
{
    fn stats(&self) -> Arc<Statistics> {
        self.as_ref().stats()
    }

    fn schema(&self) -> &Schema {
        self.as_ref().schema()
    }

    fn partition_id(&self) -> &TransitionPartitionId {
        self.as_ref().partition_id()
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.as_ref().sort_key()
    }

    fn id(&self) -> ChunkId {
        self.as_ref().id()
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        self.as_ref().may_contain_pk_duplicates()
    }

    fn data(&self) -> QueryChunkData {
        self.as_ref().data()
    }

    fn chunk_type(&self) -> &str {
        self.as_ref().chunk_type()
    }

    fn order(&self) -> ChunkOrder {
        self.as_ref().order()
    }

    fn as_any(&self) -> &dyn Any {
        // present the underlying implementation, not the wrapper
        self.as_ref().as_any()
    }
}

impl QueryChunk for Arc<dyn QueryChunk> {
    fn stats(&self) -> Arc<Statistics> {
        self.as_ref().stats()
    }

    fn schema(&self) -> &Schema {
        self.as_ref().schema()
    }

    fn partition_id(&self) -> &TransitionPartitionId {
        self.as_ref().partition_id()
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.as_ref().sort_key()
    }

    fn id(&self) -> ChunkId {
        self.as_ref().id()
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        self.as_ref().may_contain_pk_duplicates()
    }

    fn data(&self) -> QueryChunkData {
        self.as_ref().data()
    }

    fn chunk_type(&self) -> &str {
        self.as_ref().chunk_type()
    }

    fn order(&self) -> ChunkOrder {
        self.as_ref().order()
    }

    fn as_any(&self) -> &dyn Any {
        // present the underlying implementation, not the wrapper
        self.as_ref().as_any()
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
        let Some(col_stats) = &chunk.stats().column_statistics else {
            return false;
        };
        col_stats.iter().all(|col| col.distinct_count.is_some())
    })
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
