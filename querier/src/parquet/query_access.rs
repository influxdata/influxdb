use crate::parquet::QuerierParquetChunk;
use data_types::{ChunkId, ChunkOrder, DeletePredicate, PartitionId, TableSummary};
use datafusion::error::DataFusionError;
use iox_query::{
    exec::{stringset::StringSet, IOxSessionContext},
    QueryChunk, QueryChunkData, QueryChunkMeta,
};
use predicate::Predicate;
use schema::{sort::SortKey, Projection, Schema};
use std::{any::Any, sync::Arc};

impl QueryChunkMeta for QuerierParquetChunk {
    fn summary(&self) -> Arc<TableSummary> {
        Arc::clone(&self.table_summary)
    }

    fn schema(&self) -> &Schema {
        self.parquet_chunk.schema()
    }

    fn partition_sort_key(&self) -> Option<&SortKey> {
        self.partition_sort_key.as_ref().map(|sk| sk.as_ref())
    }

    fn partition_id(&self) -> PartitionId {
        self.meta().partition_id()
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.meta().sort_key()
    }

    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        &self.delete_predicates
    }
}

impl QueryChunk for QuerierParquetChunk {
    fn id(&self) -> ChunkId {
        self.meta().chunk_id
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        false
    }

    fn column_names(
        &self,
        mut ctx: IOxSessionContext,
        predicate: &Predicate,
        columns: Projection<'_>,
    ) -> Result<Option<StringSet>, DataFusionError> {
        ctx.set_metadata("projection", format!("{columns}"));
        ctx.set_metadata("predicate", format!("{}", &predicate));
        ctx.set_metadata("storage", "parquet");

        if !predicate.is_empty() {
            // if there is anything in the predicate, bail for now and force a full plan
            return Ok(None);
        }

        Ok(self.parquet_chunk.column_names(columns))
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
