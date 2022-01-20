//! This module is responsible for compacting Ingester's data

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use data_types::{
    chunk_metadata::{ChunkAddr, ChunkId, ChunkOrder},
    delete_predicate::DeletePredicate,
    partition_metadata::TableSummary,
};
use datafusion::{
    error::DataFusionError,
    physical_plan::{common::SizedRecordBatchStream, SendableRecordBatchStream},
};
use predicate::{
    delete_predicate::parse_delete_predicate,
    predicate::{Predicate, PredicateMatch},
};
use query::{
    exec::{stringset::StringSet, Executor, ExecutorType},
    frontend::reorg::ReorgPlanner,
    QueryChunk, QueryChunkMeta,
};
use schema::{merge::SchemaMerger, selection::Selection, sort::SortKey, Schema};
use snafu::{ResultExt, Snafu};

use crate::data::PersistingBatch;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Error while building logical plan for Ingester's compaction"))]
    LogicalPlan {
        source: query::frontend::reorg::Error,
    },

    #[snafu(display("Error while building physical plan for Ingester's compaction"))]
    PhysicalPlan { source: DataFusionError },

    #[snafu(display("Error while executing Ingester's compaction"))]
    ExecutePlan { source: DataFusionError },

    #[snafu(display("Error while building delete predicate from start time, {}, stop time, {}, and serialized predicate, {}", min, max, predicate))]
    DeletePredicate {
        source: predicate::delete_predicate::Error,
        min: String,
        max: String,
        predicate: String,
    },
}

/// A specialized `Error` for Ingester's Compact errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Compact the given Ingester's data
/// Note: the given `executor` should be created  with the IngesterServer
pub async fn compact(
    executor: &Executor,
    data: &'static PersistingBatch,
) -> Result<SendableRecordBatchStream> {
    let chunk = CompactingChunk::new(data);

    // Build logical plan for compaction
    let ctx = executor.new_context(ExecutorType::Reorg);
    let logical_plan = ReorgPlanner::new()
        .scan_single_chunk_plan(chunk.schema(), Arc::new(chunk))
        .context(LogicalPlanSnafu {})?;

    // Build physical plan
    let physical_plan = ctx
        .prepare_plan(&logical_plan)
        .await
        .context(PhysicalPlanSnafu {})?;

    // Execute the plan and return the compacted stream
    let output_stream = ctx
        .execute_stream(physical_plan)
        .await
        .context(ExecutePlanSnafu {})?;
    Ok(output_stream)
}

// todo: move this function to a more appropriate crate
/// Return the merged schema for RecordBatches
///
/// This is infallable because the schemas of chunks within a
/// partition are assumed to be compatible because that schema was
/// enforced as part of writing into the partition
pub fn merge_record_batch_schemas(batches: &[Arc<RecordBatch>]) -> Arc<Schema> {
    let mut merger = SchemaMerger::new();
    for batch in batches {
        let schema = Schema::try_from(batch.schema()).expect("Schema conversion error");
        merger = merger.merge(&schema).expect("schemas compatible");
    }
    Arc::new(merger.build())
}

// ------------------------------------------------------------
/// CompactingChunk is a wrapper of a PersistingBatch that implements
/// QueryChunk and QueryChunkMeta needed to build a query plan to compact its data
#[derive(Debug)]
pub struct CompactingChunk<'a> {
    /// Provided ingest data
    pub data: &'a PersistingBatch,

    /// Statistic of this data which is currently empty as it may not needed
    pub summary: TableSummary,

    /// Delete predicates to be applied while copacting this
    pub delete_predicates: Vec<Arc<DeletePredicate>>,
}

impl<'a> CompactingChunk<'a> {
    /// Create a PersistingChunk for a given PesistingBatch
    pub fn new(data: &'a PersistingBatch) -> Self {
        // Empty table summary because PersistingBatch does not keep stats
        // Todo - note to self: the TableSummary is used to get stastistics
        // while planning the scan. This empty column stats may
        // cause some issues during planning. Will verify if it is the case.
        let summary = TableSummary {
            // todo: do not have table name to provide here.
            //   Either accept this ID as the name (used mostly for debug/log) while
            //   running compaction or store table name with the PersistingBatch to
            //   avoid reading the catalog for it
            name: data.table_id.get().to_string(),
            columns: vec![],
        };

        let mut delete_predicates = vec![];
        for delete in &data.deletes {
            let delete_predicate = Arc::new(
                parse_delete_predicate(
                    &delete.min_time.get().to_string(),
                    &delete.max_time.get().to_string(),
                    &delete.serialized_predicate,
                )
                .expect("Error building delete predicate"),
            );

            delete_predicates.push(delete_predicate);
        }

        Self {
            data,
            summary,
            delete_predicates,
        }
    }
}

impl QueryChunkMeta for CompactingChunk<'_> {
    fn summary(&self) -> &TableSummary {
        &self.summary
    }

    fn schema(&self) -> Arc<Schema> {
        // Merge schema of all RecordBatches of the PerstingBatch
        let batches: Vec<Arc<RecordBatch>> =
            self.data.data.iter().map(|s| Arc::clone(&s.data)).collect();
        merge_record_batch_schemas(&batches)
    }

    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        self.delete_predicates.as_ref()
    }
}

impl QueryChunk for CompactingChunk<'_> {
    type Error = Error;

    // This function should not be used in PersistingBatch context
    fn id(&self) -> ChunkId {
        unimplemented!()
    }

    // This function should not be used in PersistingBatch context
    fn addr(&self) -> ChunkAddr {
        unimplemented!()
    }

    /// Returns the name of the table stored in this chunk
    fn table_name(&self) -> &str {
        &self.summary.name
    }

    /// Returns true if the chunk may contain a duplicate "primary
    /// key" within itself
    fn may_contain_pk_duplicates(&self) -> bool {
        // always true because they are not deduplicated yet
        true
    }

    /// Returns the result of applying the `predicate` to the chunk
    /// using an efficient, but inexact method, based on metadata.
    ///
    /// NOTE: This method is suitable for calling during planning, and
    /// may return PredicateMatch::Unknown for certain types of
    /// predicates.
    fn apply_predicate_to_metadata(
        &self,
        _predicate: &Predicate,
    ) -> Result<PredicateMatch, Self::Error> {
        Ok(PredicateMatch::Unknown)
    }

    /// Returns a set of Strings with column names from the specified
    /// table that have at least one row that matches `predicate`, if
    /// the predicate can be evaluated entirely on the metadata of
    /// this Chunk. Returns `None` otherwise
    fn column_names(
        &self,
        _predicate: &Predicate,
        _columns: Selection<'_>,
    ) -> Result<Option<StringSet>, Self::Error> {
        Ok(None)
    }

    /// Return a set of Strings containing the distinct values in the
    /// specified columns. If the predicate can be evaluated entirely
    /// on the metadata of this Chunk. Returns `None` otherwise
    ///
    /// The requested columns must all have String type.
    fn column_values(
        &self,
        _column_name: &str,
        _predicate: &Predicate,
    ) -> Result<Option<StringSet>, Self::Error> {
        Ok(None)
    }

    /// Provides access to raw `QueryChunk` data as an
    /// asynchronous stream of `RecordBatch`es filtered by a *required*
    /// predicate. Note that not all chunks can evaluate all types of
    /// predicates and this function will return an error
    /// if requested to evaluate with a predicate that is not supported
    ///
    /// This is the analog of the `TableProvider` in DataFusion
    ///
    /// The reason we can't simply use the `TableProvider` trait
    /// directly is that the data for a particular Table lives in
    /// several chunks within a partition, so there needs to be an
    /// implementation of `TableProvider` that stitches together the
    /// streams from several different `QueryChunk`s.
    fn read_filter(
        &self,
        _predicate: &Predicate,
        _selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, Self::Error> {
        let batches: Vec<_> = self.data.data.iter().map(|s| Arc::clone(&s.data)).collect();
        let stream = SizedRecordBatchStream::new(self.schema().as_arrow(), batches);
        Ok(Box::pin(stream))
    }

    /// Returns true if data of this chunk is sorted
    fn is_sorted_on_pk(&self) -> bool {
        false
    }

    /// Returns the sort key of the chunk if any
    fn sort_key(&self) -> Option<SortKey<'_>> {
        None
    }

    /// Returns chunk type
    fn chunk_type(&self) -> &str {
        "PersistingBatch"
    }

    // This function should not be used in PersistingBatch context
    fn order(&self) -> ChunkOrder {
        unimplemented!()
    }
}
