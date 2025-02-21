use arrow::array::RecordBatch;
use data_types::{ChunkId, ChunkOrder, TransitionPartitionId};
use datafusion::common::Statistics;
use iox_query::chunk_statistics::ChunkStatistics;
use iox_query::{QueryChunk, QueryChunkData};
use parquet_file::storage::ParquetExecInput;
use schema::Schema;
use schema::sort::SortKey;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct BufferChunk {
    pub batches: Vec<RecordBatch>,
    pub schema: Schema,
    pub stats: Arc<ChunkStatistics>,
    pub partition_id: data_types::partition::TransitionPartitionId,
    pub sort_key: Option<SortKey>,
    pub id: data_types::ChunkId,
    pub chunk_order: data_types::ChunkOrder,
}

impl QueryChunk for BufferChunk {
    fn stats(&self) -> Arc<Statistics> {
        Arc::clone(&self.stats.statistics())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn partition_id(&self) -> &data_types::partition::TransitionPartitionId {
        &self.partition_id
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.sort_key.as_ref()
    }

    fn id(&self) -> data_types::ChunkId {
        self.id
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        true
    }

    fn data(&self) -> QueryChunkData {
        QueryChunkData::in_mem(self.batches.clone(), Arc::clone(self.schema.inner()))
    }

    fn chunk_type(&self) -> &str {
        "BufferChunk"
    }

    fn order(&self) -> data_types::ChunkOrder {
        self.chunk_order
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct ParquetChunk {
    pub schema: Schema,
    pub stats: Arc<ChunkStatistics>,
    pub partition_id: TransitionPartitionId,
    pub sort_key: Option<SortKey>,
    pub id: ChunkId,
    pub chunk_order: ChunkOrder,
    pub parquet_exec: ParquetExecInput,
}

impl QueryChunk for ParquetChunk {
    fn stats(&self) -> Arc<Statistics> {
        Arc::clone(&self.stats.statistics())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn partition_id(&self) -> &TransitionPartitionId {
        &self.partition_id
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.sort_key.as_ref()
    }

    fn id(&self) -> ChunkId {
        self.id
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        false
    }

    fn data(&self) -> QueryChunkData {
        QueryChunkData::Parquet(self.parquet_exec.clone())
    }

    fn chunk_type(&self) -> &str {
        "Parquet"
    }

    fn order(&self) -> ChunkOrder {
        self.chunk_order
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
