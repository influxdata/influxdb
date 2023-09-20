use self::test_util::MockIngesterConnection;
use crate::cache::{namespace::CachedTable, CatalogCache};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use data_types::{ChunkId, ChunkOrder, NamespaceId, TransitionPartitionId};
use datafusion::{physical_plan::Statistics, prelude::Expr};
use iox_query::{
    chunk_statistics::{create_chunk_statistics, ColumnRanges},
    util::compute_timenanosecond_min_max,
    QueryChunk, QueryChunkData,
};
use observability_deps::tracing::trace;
use schema::{sort::SortKey, Schema};
use std::{any::Any, sync::Arc};
use trace::span::Span;
use uuid::Uuid;

pub(crate) mod test_util;
mod v1;

/// Create a new set of connections given ingester configurations
pub fn create_ingester_connections(
    ingester_addresses: Vec<Arc<str>>,
    catalog_cache: Arc<CatalogCache>,
    open_circuit_after_n_errors: u64,
    trace_context_header_name: &str,
) -> Arc<dyn IngesterConnection> {
    v1::create_ingester_connections(
        ingester_addresses,
        catalog_cache,
        open_circuit_after_n_errors,
        trace_context_header_name,
    )
}

/// Create a new ingester suitable for testing
pub fn create_ingester_connection_for_testing() -> Arc<dyn IngesterConnection> {
    Arc::new(MockIngesterConnection::new())
}

/// Dynamic error type that is used throughout the stack.
pub type DynError = Box<dyn std::error::Error + Send + Sync>;

/// Handles communicating with the ingester(s) to retrieve data that is not yet persisted
#[async_trait]
pub trait IngesterConnection: std::fmt::Debug + Send + Sync + 'static {
    /// Returns all partitions ingester(s) know about for the specified table.
    async fn partitions(
        &self,
        namespace_id: NamespaceId,
        cached_table: Arc<CachedTable>,
        columns: Vec<String>,
        filters: &[Expr],
        span: Option<Span>,
    ) -> Result<Vec<IngesterPartition>, DynError>;

    /// Return backend as [`Any`] which can be used to downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
}

/// A wrapper around the unpersisted data in a partition returned by
/// the ingester that (will) implement the `QueryChunk` interface
///
/// Given the catalog hierarchy:
///
/// ```text
/// (Catalog) Table --> (Catalog) Partition
/// ```
///
/// An IngesterPartition contains the unpersisted data for a catalog partition. Thus, there can be
/// more than one IngesterPartition for each table the ingester knows about.
#[derive(Debug, Clone)]
pub struct IngesterPartition {
    /// The ingester UUID that identifies whether this ingester has restarted since the last time
    /// it was queried or not, which affects whether we can compare the
    /// `completed_persistence_count` with a previous count for this ingester to know if we need
    /// to refresh the catalog cache or not.
    ingester_uuid: Uuid,

    /// The partition identifier.
    partition_id: TransitionPartitionId,

    /// The number of Parquet files this ingester UUID has persisted for this partition.
    completed_persistence_count: u64,

    chunks: Vec<IngesterChunk>,
}

impl IngesterPartition {
    /// Creates a new IngesterPartition, translating the passed `RecordBatches` into the correct
    /// types
    pub fn new(
        ingester_uuid: Uuid,
        partition_id: TransitionPartitionId,
        completed_persistence_count: u64,
    ) -> Self {
        Self {
            ingester_uuid,
            partition_id,
            completed_persistence_count,
            chunks: vec![],
        }
    }

    pub(crate) fn push_chunk(
        mut self,
        chunk_id: ChunkId,
        schema: Schema,
        batches: Vec<RecordBatch>,
    ) -> Self {
        let chunk = IngesterChunk {
            chunk_id,
            partition_id: self.partition_id.clone(),
            schema,
            batches,
            stats: None,
        };

        self.chunks.push(chunk);

        self
    }

    pub(crate) fn set_partition_column_ranges(&mut self, partition_column_ranges: &ColumnRanges) {
        for chunk in &mut self.chunks {
            // TODO: may want to ask the Ingester to send this value instead of computing it here.
            let ts_min_max =
                compute_timenanosecond_min_max(&chunk.batches).expect("Should have time range");

            let row_count = chunk
                .batches
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>() as u64;
            let stats = Arc::new(create_chunk_statistics(
                row_count,
                &chunk.schema,
                Some(ts_min_max),
                partition_column_ranges,
            ));
            chunk.stats = Some(stats);
        }
    }

    pub(crate) fn partition_id(&self) -> TransitionPartitionId {
        self.partition_id.clone()
    }

    pub(crate) fn ingester_uuid(&self) -> Uuid {
        self.ingester_uuid
    }

    pub(crate) fn completed_persistence_count(&self) -> u64 {
        self.completed_persistence_count
    }

    pub(crate) fn chunks(&self) -> &[IngesterChunk] {
        &self.chunks
    }

    pub(crate) fn into_chunks(self) -> Vec<IngesterChunk> {
        self.chunks
    }
}

#[derive(Debug, Clone)]
pub struct IngesterChunk {
    chunk_id: ChunkId,

    partition_id: TransitionPartitionId,

    schema: Schema,

    /// The raw table data
    batches: Vec<RecordBatch>,

    /// Summary Statistics
    ///
    /// Set to `None` if not calculated yet.
    stats: Option<Arc<Statistics>>,
}

impl IngesterChunk {
    pub(crate) fn estimate_size(&self) -> usize {
        self.batches
            .iter()
            .map(|batch| {
                batch
                    .columns()
                    .iter()
                    .map(|array| array.get_array_memory_size())
                    .sum::<usize>()
            })
            .sum::<usize>()
    }

    pub(crate) fn rows(&self) -> usize {
        self.batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>()
    }
}

impl QueryChunk for IngesterChunk {
    fn stats(&self) -> Arc<Statistics> {
        Arc::clone(self.stats.as_ref().expect("chunk stats set"))
    }

    fn schema(&self) -> &Schema {
        trace!(schema=?self.schema, "IngesterChunk schema");
        &self.schema
    }

    fn partition_id(&self) -> &TransitionPartitionId {
        &self.partition_id
    }

    fn sort_key(&self) -> Option<&SortKey> {
        // Data is not sorted
        None
    }

    fn id(&self) -> ChunkId {
        self.chunk_id
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        // ingester just dumps data, may contain duplicates!
        true
    }

    fn data(&self) -> QueryChunkData {
        QueryChunkData::in_mem(self.batches.clone(), Arc::clone(self.schema.inner()))
    }

    fn chunk_type(&self) -> &str {
        "IngesterPartition"
    }

    fn order(&self) -> ChunkOrder {
        // since this is always the 'most recent' chunk for this
        // partition, put it at the end
        ChunkOrder::new(i64::MAX)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
