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
#![allow(unreachable_pub)]

use datafusion_util::MemoryStream;
use futures::TryStreamExt;
use query_log::{QueryCompletedToken, QueryText, StateReceived};
use trace::{ctx::SpanContext, span::Span};

use tracker::InstrumentedAsyncOwnedSemaphorePermit;
// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use arrow::{
    datatypes::{DataType, Field, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use data_types::{ChunkId, ChunkOrder, TransitionPartitionId};
use datafusion::{
    error::DataFusionError,
    physical_plan::{SendableRecordBatchStream, Statistics},
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
pub mod query_log;
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
        span_ctx: Option<&SpanContext>,
        query_type: &'static str,
        query_text: QueryText,
    ) -> QueryCompletedToken<StateReceived>;

    /// Returns a new execution context suitable for running queries
    fn new_query_context(&self, span_ctx: Option<SpanContext>) -> IOxSessionContext;
}

/// Trait that allows the query engine (which includes flight and storage/InfluxRPC) to access a
/// virtual set of namespaces.
///
/// This is the only entry point for the query engine. This trait and the traits reachable by it (e.g.
/// [`QueryNamespace`]) are the only wait to access the catalog and payload data.
#[async_trait]
pub trait QueryNamespaceProvider: std::fmt::Debug + Send + Sync + 'static {
    /// Get namespace if it exists.
    ///
    /// System tables may contain debug information depending on `include_debug_info_tables`.
    async fn db(
        &self,
        name: &str,
        span: Option<Span>,
        include_debug_info_tables: bool,
    ) -> Option<Arc<dyn QueryNamespace>>;

    /// Acquire concurrency-limiting sempahore
    async fn acquire_semaphore(&self, span: Option<Span>) -> InstrumentedAsyncOwnedSemaphorePermit;
}

/// Raw data of a [`QueryChunk`].
pub enum QueryChunkData {
    /// Record batches.
    RecordBatches(SendableRecordBatchStream),

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
            Self::RecordBatches(batches) => batches.try_collect::<Vec<_>>().await.unwrap(),
            Self::Parquet(exec_input) => exec_input
                .read_to_batches(schema.as_arrow(), Projection::All, session_ctx)
                .await
                .unwrap(),
        }
    }

    /// Create data based on batches and schema.
    pub fn in_mem(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        let s = MemoryStream::new_with_schema(batches, schema);
        let s: SendableRecordBatchStream = Box::pin(s);
        Self::RecordBatches(s)
    }
}

impl std::fmt::Debug for QueryChunkData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RecordBatches(_) => f.debug_tuple("RecordBatches").field(&"<stream>").finish(),
            Self::Parquet(input) => f.debug_tuple("Parquet").field(input).finish(),
        }
    }
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
