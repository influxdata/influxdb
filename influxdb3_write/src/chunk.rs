use arrow::array::RecordBatch;
use data_types::{ChunkId, ChunkOrder, PartitionHashId};
use datafusion::common::Statistics;
use iox_query::chunk_statistics::ChunkStatistics;
use iox_query::{QueryChunk, QueryChunkData};
use parquet_file::storage::DataSourceExecInput;
use schema::Schema;
use schema::sort::SortKey;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct BufferChunk {
    pub batches: Vec<RecordBatch>,
    pub schema: Schema,
    pub stats: Arc<ChunkStatistics>,
    pub partition_id: PartitionHashId,
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

    fn partition_id(&self) -> &PartitionHashId {
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

    fn row_order_range(&self) -> Option<std::ops::RangeInclusive<i64>> {
        let row_count = self
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>();
        if row_count == 0 {
            return None;
        }
        let offset = i64::try_from(row_count - 1).expect("buffer row count must fit in i64");
        let end = self.chunk_order.get();
        let start = end
            .checked_sub(offset)
            .expect("buffer row-order range must fit in i64");
        Some(start..=end)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct ParquetChunk {
    pub schema: Schema,
    pub stats: Arc<ChunkStatistics>,
    pub partition_id: PartitionHashId,
    pub sort_key: Option<SortKey>,
    pub id: ChunkId,
    pub chunk_order: ChunkOrder,
    pub parquet_exec: DataSourceExecInput,
}

impl QueryChunk for ParquetChunk {
    fn stats(&self) -> Arc<Statistics> {
        Arc::clone(&self.stats.statistics())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn partition_id(&self) -> &PartitionHashId {
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
