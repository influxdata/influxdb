use crate::parquet::QuerierParquetChunk;
use data_types::{ChunkId, ChunkOrder, TransitionPartitionId};
use datafusion::physical_plan::Statistics;
use iox_query::{QueryChunk, QueryChunkData};
use schema::{sort::SortKey, Schema};
use std::{any::Any, sync::Arc};

impl QueryChunk for QuerierParquetChunk {
    fn stats(&self) -> Arc<Statistics> {
        Arc::clone(&self.stats)
    }

    fn schema(&self) -> &Schema {
        self.parquet_chunk.schema()
    }

    fn partition_id(&self) -> &TransitionPartitionId {
        self.meta().partition_id()
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.meta().sort_key()
    }

    fn id(&self) -> ChunkId {
        self.meta().chunk_id
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        false
    }

    fn data(&self) -> QueryChunkData {
        QueryChunkData::Parquet(self.parquet_chunk.parquet_exec_input())
    }

    fn chunk_type(&self) -> &str {
        "parquet"
    }

    fn order(&self) -> ChunkOrder {
        self.meta().order()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
