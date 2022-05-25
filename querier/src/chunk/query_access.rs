use crate::chunk::QuerierParquetChunk;
use data_types::{
    ChunkId, ChunkOrder, DeletePredicate, PartitionId, TableSummary, TimestampMinMax,
};
use iox_query::{QueryChunk, QueryChunkError, QueryChunkMeta};
use observability_deps::tracing::debug;
use predicate::PredicateMatch;
use schema::{sort::SortKey, Schema};
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Parquet File Error in chunk {}: {}", chunk_id, source))]
    ParquetFileChunkError {
        source: parquet_file::storage::ReadError,
        chunk_id: ChunkId,
    },
}

impl QueryChunkMeta for QuerierParquetChunk {
    fn summary(&self) -> Option<&TableSummary> {
        Some(self.parquet_chunk.table_summary().as_ref())
    }

    fn schema(&self) -> Arc<Schema> {
        self.parquet_chunk.schema()
    }

    fn partition_sort_key(&self) -> Option<&SortKey> {
        self.partition_sort_key()
    }

    fn partition_id(&self) -> Option<PartitionId> {
        Some(self.meta.partition_id())
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.meta().sort_key()
    }

    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        &self.delete_predicates
    }

    fn timestamp_min_max(&self) -> Option<TimestampMinMax> {
        self.timestamp_min_max()
    }
}

impl QueryChunk for QuerierParquetChunk {
    fn id(&self) -> ChunkId {
        self.meta().chunk_id
    }

    fn table_name(&self) -> &str {
        self.meta().table_name.as_ref()
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        false
    }

    fn apply_predicate_to_metadata(
        &self,
        predicate: &predicate::Predicate,
    ) -> Result<predicate::PredicateMatch, QueryChunkError> {
        let pred_result = if predicate.has_exprs()
            || self.parquet_chunk.has_timerange(predicate.range.as_ref())
        {
            PredicateMatch::Unknown
        } else {
            PredicateMatch::Zero
        };

        Ok(pred_result)
    }

    fn column_names(
        &self,
        _ctx: iox_query::exec::IOxSessionContext,
        predicate: &predicate::Predicate,
        columns: schema::selection::Selection<'_>,
    ) -> Result<Option<iox_query::exec::stringset::StringSet>, QueryChunkError> {
        if !predicate.is_empty() {
            // if there is anything in the predicate, bail for now and force a full plan
            return Ok(None);
        }
        Ok(self.parquet_chunk.column_names(columns))
    }

    fn column_values(
        &self,
        _ctx: iox_query::exec::IOxSessionContext,
        _column_name: &str,
        _predicate: &predicate::Predicate,
    ) -> Result<Option<iox_query::exec::stringset::StringSet>, QueryChunkError> {
        // Since DataFusion can read Parquet, there is no advantage to
        // manually implementing this vs just letting DataFusion do its thing
        Ok(None)
    }

    fn read_filter(
        &self,
        mut ctx: iox_query::exec::IOxSessionContext,
        predicate: &predicate::Predicate,
        selection: schema::selection::Selection<'_>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream, QueryChunkError> {
        let delete_predicates: Vec<_> = self
            .delete_predicates()
            .iter()
            .map(|pred| Arc::new(pred.as_ref().clone().into()))
            .collect();
        ctx.set_metadata("delete_predicates", delete_predicates.len() as i64);

        // merge the negated delete predicates into the select predicate
        let mut pred_with_deleted_exprs = predicate.clone();
        pred_with_deleted_exprs.merge_delete_predicates(&delete_predicates);
        debug!(?pred_with_deleted_exprs, "Merged negated predicate");

        ctx.set_metadata("predicate", format!("{}", &pred_with_deleted_exprs));
        self.parquet_chunk
            .read_filter(&pred_with_deleted_exprs, selection)
            .context(ParquetFileChunkSnafu {
                chunk_id: self.id(),
            })
            .map_err(|e| Box::new(e) as _)
    }

    fn chunk_type(&self) -> &str {
        "parquet"
    }

    fn order(&self) -> ChunkOrder {
        self.meta().order()
    }
}
