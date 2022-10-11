//! Module to handle query on Ingester's data

use std::{any::Any, sync::Arc};

use arrow::record_batch::RecordBatch;
use arrow_util::util::ensure_schema;
use data_types::{
    ChunkId, ChunkOrder, DeletePredicate, PartitionId, SequenceNumber, TableSummary,
    TimestampMinMax,
};
use datafusion::{
    error::DataFusionError,
    physical_plan::{
        common::SizedRecordBatchStream,
        metrics::{ExecutionPlanMetricsSet, MemTrackingMetrics},
        SendableRecordBatchStream,
    },
};
use iox_query::{
    exec::{stringset::StringSet, IOxSessionContext},
    QueryChunk, QueryChunkMeta,
};
use observability_deps::tracing::trace;
use predicate::Predicate;
use schema::{merge::merge_record_batch_schemas, selection::Selection, sort::SortKey, Schema};
use snafu::{ResultExt, Snafu};

use crate::data::{partition::SnapshotBatch, table::TableName};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Internal error concatenating record batches {}", source))]
    Schema { source: schema::Error },

    #[snafu(display("Internal error concatenating record batches {}", source))]
    ConcatBatches { source: arrow::error::ArrowError },

    #[snafu(display("Internal error filtering columns from a record batch {}", source))]
    FilterColumns { source: crate::data::Error },

    #[snafu(display("Internal error filtering record batch: {}", source))]
    FilterBatch { source: arrow::error::ArrowError },
}

/// A specialized `Error` for Ingester's Query errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Queryable data used for both query and persistence
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct QueryableBatch {
    /// data
    pub(crate) data: Vec<Arc<SnapshotBatch>>,

    /// This is needed to return a reference for a trait function
    pub(crate) table_name: TableName,

    /// Partition ID
    pub(crate) partition_id: PartitionId,
}

impl QueryableBatch {
    /// Initilaize a QueryableBatch
    pub(crate) fn new(
        table_name: TableName,
        partition_id: PartitionId,
        data: Vec<Arc<SnapshotBatch>>,
    ) -> Self {
        Self {
            data,
            table_name,
            partition_id,
        }
    }

    /// Add snapshots to this batch
    pub(crate) fn with_data(mut self, mut data: Vec<Arc<SnapshotBatch>>) -> Self {
        self.data.append(&mut data);
        self
    }

    /// return min and max of all the snapshots
    pub(crate) fn min_max_sequence_numbers(&self) -> (SequenceNumber, SequenceNumber) {
        let min = self
            .data
            .first()
            .expect("The Queryable Batch should not empty")
            .min_sequence_number;

        let max = self
            .data
            .first()
            .expect("The Queryable Batch should not empty")
            .max_sequence_number;

        assert!(min <= max);

        (min, max)
    }
}

impl QueryChunkMeta for QueryableBatch {
    fn summary(&self) -> Option<Arc<TableSummary>> {
        None
    }

    fn schema(&self) -> Arc<Schema> {
        // TODO: may want store this schema as a field of QueryableBatch and
        // only do this schema merge the first time it is called

        // Merge schema of all RecordBatches of the PerstingBatch
        let batches: Vec<Arc<RecordBatch>> =
            self.data.iter().map(|s| Arc::clone(&s.data)).collect();
        merge_record_batch_schemas(&batches)
    }

    fn partition_sort_key(&self) -> Option<&SortKey> {
        None // Ingester data has not persisted yet and should not be attached to any partition
    }

    fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    fn sort_key(&self) -> Option<&SortKey> {
        None // Ingester data is not sorted
    }

    fn timestamp_min_max(&self) -> Option<TimestampMinMax> {
        // Note: we need to consider which option we want to go with
        //  . Return None here and avoid taking time to compute time's min max of RecordBacthes (current choice)
        //  . Compute time's min max here and avoid compacting non-overlapped QueryableBatches in the Ingester
        None
    }

    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        &[]
    }
}

impl QueryChunk for QueryableBatch {
    // This function should not be used in QueryBatch context
    fn id(&self) -> ChunkId {
        // To return a value for debugging and make it consistent with ChunkId created in Compactor,
        // use Uuid for this
        ChunkId::new()
    }

    /// Returns the name of the table stored in this chunk
    fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Returns true if the chunk may contain a duplicate "primary
    /// key" within itself
    fn may_contain_pk_duplicates(&self) -> bool {
        // always true because they are not deduplicated yet
        true
    }

    /// Returns a set of Strings with column names from the specified
    /// table that have at least one row that matches `predicate`, if
    /// the predicate can be evaluated entirely on the metadata of
    /// this Chunk. Returns `None` otherwise
    fn column_names(
        &self,
        _ctx: IOxSessionContext,
        _predicate: &Predicate,
        _columns: Selection<'_>,
    ) -> Result<Option<StringSet>, DataFusionError> {
        Ok(None)
    }

    /// Return a set of Strings containing the distinct values in the
    /// specified columns. If the predicate can be evaluated entirely
    /// on the metadata of this Chunk. Returns `None` otherwise
    ///
    /// The requested columns must all have String type.
    fn column_values(
        &self,
        _ctx: IOxSessionContext,
        _column_name: &str,
        _predicate: &Predicate,
    ) -> Result<Option<StringSet>, DataFusionError> {
        Ok(None)
    }

    /// Provides access to raw `QueryChunk` data as an
    /// asynchronous stream of `RecordBatch`es
    fn read_filter(
        &self,
        mut ctx: IOxSessionContext,
        _predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        ctx.set_metadata("storage", "ingester");
        ctx.set_metadata("projection", format!("{}", selection));
        trace!(?selection, "selection");

        let schema = self
            .schema()
            .select(selection)
            .context(SchemaSnafu)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Get all record batches from their snapshots
        let batches = self
            .data
            .iter()
            .filter_map(|snapshot| {
                let batch = snapshot
                    // Only return columns in the selection
                    .scan(selection)
                    .context(FilterColumnsSnafu {})
                    .transpose()?
                    // ensure batch has desired schema
                    .and_then(|batch| {
                        ensure_schema(&schema.as_arrow(), &batch).context(ConcatBatchesSnafu {})
                    })
                    .map(Arc::new);
                Some(batch)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Return stream of data
        let dummy_metrics = ExecutionPlanMetricsSet::new();
        let mem_metrics = MemTrackingMetrics::new(&dummy_metrics, 0);
        let stream = SizedRecordBatchStream::new(schema.as_arrow(), batches, mem_metrics);
        Ok(Box::pin(stream))
    }

    /// Returns chunk type
    fn chunk_type(&self) -> &str {
        "PersistingBatch"
    }

    // This function should not be used in PersistingBatch context
    fn order(&self) -> ChunkOrder {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
