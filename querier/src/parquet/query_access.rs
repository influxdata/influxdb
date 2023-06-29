use crate::parquet::QuerierParquetChunk;
use data_types::{ChunkId, ChunkOrder, PartitionId};
use datafusion::{error::DataFusionError, physical_plan::Statistics};
use iox_query::{
    exec::{stringset::StringSet, IOxSessionContext},
    QueryChunk, QueryChunkData,
};
use predicate::Predicate;
use schema::{sort::SortKey, Schema};
use std::{any::Any, sync::Arc};

impl QueryChunk for QuerierParquetChunk {
    fn stats(&self) -> Arc<Statistics> {
        Arc::clone(&self.stats)
    }

    fn schema(&self) -> &Schema {
        self.parquet_chunk.schema()
    }

    fn partition_id(&self) -> PartitionId {
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

    fn column_values(
        &self,
        mut ctx: IOxSessionContext,
        column_name: &str,
        predicate: &Predicate,
    ) -> Result<Option<StringSet>, DataFusionError> {
        ctx.set_metadata("column_name", column_name.to_string());
        ctx.set_metadata("predicate", format!("{}", &predicate));
        ctx.set_metadata("storage", "parquet");

        // Since DataFusion can read Parquet, there is no advantage to
        // manually implementing this vs just letting DataFusion do its thing
        Ok(None)
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
