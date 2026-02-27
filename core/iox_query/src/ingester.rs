use std::{
    any::Any,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use data_types::{ChunkId, ChunkOrder, PartitionHashId};
use datafusion::{
    common::Statistics,
    error::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream},
    physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, MetricValue, Time},
};
use futures::Stream;
use parking_lot::Mutex;
use schema::{Schema, sort::SortKey};
use tracing::trace;

use crate::{QueryChunk, QueryChunkData};

#[derive(Debug, Default, Clone)]
pub struct IngesterQueryMetrics {
    state: Arc<Mutex<IngesterQueryMetricsState>>,
}

impl IngesterQueryMetrics {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(IngesterQueryMetricsState::default())),
        }
    }

    pub fn update_metrics(&self, metrics: IngesterQueryMetricsState) {
        let mut state = self.state.lock();
        *state = metrics;
    }

    pub fn get_metrics(&self) -> IngesterQueryMetricsState {
        let state = self.state.lock();
        *state
    }
}

#[derive(Debug, Default)]
struct DFIngesterQueryMetrics {
    response_rows: Count,
    response_size: Count,
    partition_count: Count,
}

pub struct MetricDecoratorStream {
    input: SendableRecordBatchStream,

    ingester_metrics: IngesterQueryMetrics,

    /// execution plan metrics, to be updated each poll from the ingester_query_metrics
    execution_plan_metrics: ExecutionPlanMetricsSet,

    metrics: DFIngesterQueryMetrics,
}

impl std::fmt::Debug for MetricDecoratorStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricDecoratorStream")
            .field("input", &"(OPAQUE STREAM)")
            .field("ingester_metrics", &self.ingester_metrics)
            .field("execution_plan_metrics", &self.execution_plan_metrics)
            .field("metrics", &self.metrics)
            .finish()
    }
}

impl MetricDecoratorStream {
    pub fn new(
        input: SendableRecordBatchStream,
        ingester_metrics: IngesterQueryMetrics,
        execution_plan_metrics: ExecutionPlanMetricsSet,
    ) -> Self {
        let response_rows =
            MetricBuilder::new(&execution_plan_metrics).global_counter("response_rows");
        let response_size =
            MetricBuilder::new(&execution_plan_metrics).global_counter("response_size");
        let partition_count =
            MetricBuilder::new(&execution_plan_metrics).global_counter("partition_count");

        Self {
            input,
            ingester_metrics,
            execution_plan_metrics,
            metrics: DFIngesterQueryMetrics {
                response_rows,
                response_size,
                partition_count,
            },
        }
    }

    fn record_poll_ingester_query_metrics(
        &self,
        poll: &Poll<Option<Result<RecordBatch, DataFusionError>>>,
    ) {
        if let Poll::Ready(Some(Ok(record_batch))) = poll {
            let mem_size = record_batch.get_array_memory_size();
            let num_rows = record_batch.num_rows();

            self.metrics.response_rows.add(num_rows);
            self.metrics.response_size.add(mem_size);

            let IngesterQueryMetricsState {
                latency_to_plan,
                latency_to_full_data,
                partition_count,
            } = self.ingester_metrics.get_metrics();

            self.metrics.partition_count.add(partition_count);
            let ltp = Time::new();
            ltp.add_duration(latency_to_plan);
            let ltfd = Time::new();
            ltfd.add_duration(latency_to_full_data);

            MetricBuilder::new(&self.execution_plan_metrics).build(MetricValue::Time {
                name: "latency_to_plan".into(),
                time: ltp,
            });
            MetricBuilder::new(&self.execution_plan_metrics).build(MetricValue::Time {
                name: "latency_to_full_data".into(),
                time: ltfd,
            });
        }
    }
}

impl RecordBatchStream for MetricDecoratorStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

impl Stream for MetricDecoratorStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.input.as_mut().poll_next(ctx);
        self.record_poll_ingester_query_metrics(&poll);
        poll
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct IngesterQueryMetricsState {
    pub latency_to_plan: Duration,
    pub latency_to_full_data: Duration,
    pub partition_count: usize,
}

#[derive(Clone)]
pub enum IngesterChunkData {
    /// All batches are fetched already.
    Eager(Vec<RecordBatch>),

    /// Batches are streamed.
    Stream(Arc<dyn Fn() -> SendableRecordBatchStream + Send + Sync>),
}

impl IngesterChunkData {
    pub fn eager_ref(&self) -> &[RecordBatch] {
        match self {
            Self::Eager(batches) => batches,
            Self::Stream(_) => panic!("data is backed by a stream"),
        }
    }
}

impl std::fmt::Debug for IngesterChunkData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eager(arg0) => f.debug_tuple("Eager").field(arg0).finish(),
            Self::Stream(_) => f.debug_tuple("Stream").field(&"<stream>").finish(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IngesterChunk {
    /// Chunk ID.
    pub chunk_id: ChunkId,

    /// Partition ID.
    pub partition_id: PartitionHashId,

    /// Chunk schema.
    ///
    /// This may be a subset of the table and partition schema.
    pub schema: Schema,

    /// Data.
    pub data: IngesterChunkData,

    /// Summary Statistics
    pub stats: Arc<Statistics>,

    /// Metrics specific to Ingester communication
    pub metrics: IngesterQueryMetrics,
}

impl IngesterChunk {
    pub fn metrics(&self) -> IngesterQueryMetrics {
        self.metrics.clone()
    }
}

impl QueryChunk for IngesterChunk {
    fn stats(&self) -> Arc<Statistics> {
        Arc::clone(&self.stats)
    }

    fn schema(&self) -> &Schema {
        trace!(schema=?self.schema, "IngesterChunk schema");
        &self.schema
    }

    fn partition_id(&self) -> &PartitionHashId {
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
        match &self.data {
            IngesterChunkData::Eager(batches) => {
                QueryChunkData::in_mem(batches.clone(), Arc::clone(self.schema.inner()))
            }
            IngesterChunkData::Stream(f) => QueryChunkData::RecordBatches(f()),
        }
    }

    fn chunk_type(&self) -> &str {
        "ingester"
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
