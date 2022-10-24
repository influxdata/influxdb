use crate::chunk::QuerierChunk;
use data_types::{
    ChunkId, ChunkOrder, DeletePredicate, PartitionId, TableSummary, TimestampMinMax,
};
use datafusion::{error::DataFusionError, physical_plan::SendableRecordBatchStream};
use iox_query::{
    exec::{stringset::StringSet, IOxSessionContext},
    QueryChunk, QueryChunkMeta,
};
use observability_deps::tracing::debug;
use predicate::Predicate;
use schema::{selection::Selection, sort::SortKey, Schema};
use snafu::{ResultExt, Snafu};
use std::{any::Any, sync::Arc};
use trace::span::SpanRecorder;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Parquet File Error in chunk {}: {}", chunk_id, source))]
    ParquetFileChunk {
        source: Box<parquet_file::storage::ReadError>,
        chunk_id: ChunkId,
    },

    #[snafu(display(
        "Could not find column name '{}' in read buffer column_values results for chunk {}",
        column_name,
        chunk_id,
    ))]
    ColumnNameNotFound {
        column_name: String,
        chunk_id: ChunkId,
    },
}

impl QueryChunkMeta for QuerierChunk {
    fn summary(&self) -> Option<Arc<TableSummary>> {
        Some(Arc::clone(&self.table_summary))
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn partition_sort_key(&self) -> Option<&SortKey> {
        self.partition_sort_key.as_ref().as_ref()
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

    fn timestamp_min_max(&self) -> Option<TimestampMinMax> {
        Some(self.timestamp_min_max)
    }
}

impl QueryChunk for QuerierChunk {
    fn id(&self) -> ChunkId {
        self.meta().chunk_id
    }

    fn table_name(&self) -> &str {
        self.meta().table_name.as_ref()
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        false
    }

    fn column_names(
        &self,
        mut ctx: IOxSessionContext,
        predicate: &Predicate,
        columns: Selection<'_>,
    ) -> Result<Option<StringSet>, DataFusionError> {
        ctx.set_metadata("projection", format!("{}", columns));
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

    fn read_filter(
        &self,
        mut ctx: IOxSessionContext,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let span_recorder = SpanRecorder::new(
            ctx.span()
                .map(|span| span.child("QuerierChunk::read_filter")),
        );
        let delete_predicates: Vec<_> = self
            .delete_predicates()
            .iter()
            .map(|pred| Arc::new(pred.as_ref().clone().into()))
            .collect();
        ctx.set_metadata("delete_predicates", delete_predicates.len() as i64);

        // merge the negated delete predicates into the select predicate
        let pred_with_deleted_exprs = predicate.clone().with_delete_predicates(&delete_predicates);
        debug!(?pred_with_deleted_exprs, "Merged negated predicate");

        ctx.set_metadata("predicate", format!("{}", &pred_with_deleted_exprs));
        ctx.set_metadata("projection", format!("{}", selection));
        ctx.set_metadata("storage", "parquet");

        let chunk_id = self.id();
        debug!(?predicate, "parquet read_filter");

        // TODO(marco): propagate span all the way down to the object store cache access
        let _span_recorder = span_recorder;

        self.parquet_chunk
            .read_filter(&pred_with_deleted_exprs, selection, ctx.inner())
            .map_err(Box::new)
            .context(ParquetFileChunkSnafu { chunk_id })
            .map_err(|e| DataFusionError::External(Box::new(e)))
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
