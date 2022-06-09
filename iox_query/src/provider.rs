//! Implementation of a DataFusion `TableProvider` in terms of `QueryChunk`s

use async_trait::async_trait;
use hashbrown::HashMap;
use std::sync::Arc;

use arrow::{datatypes::SchemaRef as ArrowSchemaRef, error::ArrowError};
use datafusion::{
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    logical_plan::Expr,
    physical_plan::{
        expressions::{col as physical_col, PhysicalSortExpr},
        filter::FilterExec,
        projection::ProjectionExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
        ExecutionPlan,
    },
};
use observability_deps::tracing::{debug, trace, warn};
use predicate::Predicate;
use schema::{merge::SchemaMerger, sort::SortKey, InfluxColumnType, Schema};

use crate::{
    compute_sort_key_for_chunks,
    exec::IOxSessionContext,
    util::{arrow_sort_key_exprs, df_physical_expr},
    QueryChunk,
};

use snafu::{ResultExt, Snafu};

mod adapter;
mod deduplicate;
pub mod overlap;
mod physical;
use self::overlap::group_potential_duplicates;
pub(crate) use deduplicate::DeduplicateExec;
pub use deduplicate::RecordBatchDeduplicator;
pub(crate) use physical::IOxReadFilterNode;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Internal error: no chunk pruner provided to builder for {}",
        table_name,
    ))]
    InternalNoChunkPruner { table_name: String },

    #[snafu(display("Internal error: Cannot create projection select expr '{}'", source,))]
    InternalSelectExpr {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Internal error adding sort operator '{}'", source,))]
    InternalSort {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Internal error adding filter operator '{}'", source,))]
    InternalFilter {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Internal error adding projection operator '{}'", source,))]
    InternalProjection {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Internal error: Can not group chunks '{}'", source,))]
    InternalChunkGrouping { source: self::overlap::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for ArrowError {
    // Wrap an error into an arrow error
    fn from(e: Error) -> Self {
        Self::ExternalError(Box::new(e))
    }
}

impl From<Error> for DataFusionError {
    // Wrap an error into a datafusion error
    fn from(e: Error) -> Self {
        Self::ArrowError(e.into())
    }
}

/// Something that can prune chunks based on their metadata
pub trait ChunkPruner: Sync + Send + std::fmt::Debug {
    /// prune `chunks`, if possible, based on predicate.
    fn prune_chunks(
        &self,
        table_name: &str,
        table_schema: Arc<Schema>,
        chunks: Vec<Arc<dyn QueryChunk>>,
        predicate: &Predicate,
    ) -> Vec<Arc<dyn QueryChunk>>;
}

/// Builds a `ChunkTableProvider` from a series of `QueryChunk`s
/// and ensures the schema across the chunks is compatible and
/// consistent.
#[derive(Debug)]
pub struct ProviderBuilder {
    table_name: Arc<str>,
    schema: Arc<Schema>,
    chunk_pruner: Option<Arc<dyn ChunkPruner>>,
    chunks: Vec<Arc<dyn QueryChunk>>,
    sort_key: Option<SortKey>,

    // execution context used for tracing
    ctx: IOxSessionContext,
}

impl ProviderBuilder {
    pub fn new(table_name: impl AsRef<str>, schema: Arc<Schema>) -> Self {
        Self {
            table_name: Arc::from(table_name.as_ref()),
            schema,
            chunk_pruner: None,
            chunks: Vec::new(),
            sort_key: None,
            ctx: IOxSessionContext::default(),
        }
    }

    pub fn with_execution_context(self, ctx: IOxSessionContext) -> Self {
        Self { ctx, ..self }
    }

    /// Produce sorted output
    pub fn with_sort_key(self, sort_key: SortKey) -> Self {
        Self {
            sort_key: Some(sort_key),
            ..self
        }
    }

    /// Add a new chunk to this provider
    pub fn add_chunk(mut self, chunk: Arc<dyn QueryChunk>) -> Self {
        self.chunks.push(chunk);
        self
    }

    /// Specify a `ChunkPruner` for the provider that will apply
    /// additional chunk level pruning based on pushed down predicates
    pub fn add_pruner(mut self, chunk_pruner: Arc<dyn ChunkPruner>) -> Self {
        assert!(
            self.chunk_pruner.is_none(),
            "Chunk pruner already specified"
        );
        self.chunk_pruner = Some(chunk_pruner);
        self
    }

    /// Specify a `ChunkPruner` for the provider that does no
    /// additional pruning based on pushed down predicates.
    ///
    /// Some planners, such as InfluxRPC which apply all predicates
    /// when they get the initial list of chunks, do not need an
    /// additional pass.
    pub fn add_no_op_pruner(self) -> Self {
        let chunk_pruner = Arc::new(NoOpPruner {});
        self.add_pruner(chunk_pruner)
    }

    /// Create the Provider
    pub fn build(self) -> Result<ChunkTableProvider> {
        let chunk_pruner = match self.chunk_pruner {
            Some(chunk_pruner) => chunk_pruner,
            None => {
                return InternalNoChunkPrunerSnafu {
                    table_name: self.table_name.as_ref(),
                }
                .fail()
            }
        };

        Ok(ChunkTableProvider {
            iox_schema: self.schema,
            chunk_pruner,
            table_name: self.table_name,
            chunks: self.chunks,
            sort_key: self.sort_key,
            ctx: self.ctx,
        })
    }
}

/// Implementation of a DataFusion TableProvider in terms of QueryChunks
///
/// This allows DataFusion to see data from Chunks as a single table, as well as
/// push predicates and selections down to chunks
#[derive(Debug)]
pub struct ChunkTableProvider {
    table_name: Arc<str>,
    /// The IOx schema (wrapper around Arrow Schemaref) for this table
    iox_schema: Arc<Schema>,
    /// Something that can prune chunks
    chunk_pruner: Arc<dyn ChunkPruner>,
    /// The chunks
    chunks: Vec<Arc<dyn QueryChunk>>,
    /// The sort key if any
    sort_key: Option<SortKey>,

    // execution context
    ctx: IOxSessionContext,
}

impl ChunkTableProvider {
    /// Return the IOx schema view for the data provided by this provider
    pub fn iox_schema(&self) -> Arc<Schema> {
        Arc::clone(&self.iox_schema)
    }

    /// Return the Arrow schema view for the data provided by this provider
    pub fn arrow_schema(&self) -> ArrowSchemaRef {
        self.iox_schema.as_arrow()
    }

    /// Return the table name
    pub fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }
}

#[async_trait]
impl TableProvider for ChunkTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// Schema with all available columns across all chunks
    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema()
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        trace!("Create a scan node for ChunkTableProvider");

        // Note that `filters` don't actually need to be evaluated in
        // the scan for the plans to be correct, they are an extra
        // optimization for providers which can offer them
        let predicate = Predicate::default().with_pushdown_exprs(filters);

        // Now we have a second attempt to prune out chunks based on
        // metadata using the pushed down predicate (e.g. in SQL).
        let chunks: Vec<Arc<dyn QueryChunk>> = self.chunks.to_vec();
        let num_initial_chunks = chunks.len();
        let chunks = self.chunk_pruner.prune_chunks(
            self.table_name(),
            self.iox_schema(),
            chunks,
            &predicate,
        );
        debug!(%predicate, num_initial_chunks, num_final_chunks=chunks.len(), "pruned with pushed down predicates");

        // Figure out the schema of the requested output
        let scan_schema = match projection {
            Some(indices) => Arc::new(self.iox_schema.select_by_indices(indices)),
            None => Arc::clone(&self.iox_schema),
        };

        // This debug shows the self.arrow_schema() includes all columns in all chunks
        // which means the schema of all chunks are merged before invoking this scan
        debug!(schema=?self.arrow_schema(), "All chunks schema");
        // However, the schema of each chunk is still in its original form which does not
        // include the merged columns of other chunks. The code below (put in comments on purpose) proves it
        // for chunk in chunks.clone() {
        //     trace!("Schema of chunk {}: {:#?}", chunk.id(), chunk.schema());
        // }

        let mut deduplicate =
            Deduplicater::new().with_execution_context(self.ctx.child_ctx("deduplicator"));
        let plan = deduplicate.build_scan_plan(
            Arc::clone(&self.table_name),
            scan_schema,
            chunks,
            predicate,
            self.sort_key.clone(),
        )?;

        Ok(plan)
    }

    /// Filter pushdown specification
    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> DataFusionResult<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

#[derive(Debug)]
/// A deduplicater that deduplicate the duplicated data during scan execution
pub(crate) struct Deduplicater {
    /// a vector of a vector of overlapped chunks
    pub overlapped_chunks_set: Vec<Vec<Arc<dyn QueryChunk>>>,

    /// a vector of non-overlapped chunks each have duplicates in itself
    pub in_chunk_duplicates_chunks: Vec<Arc<dyn QueryChunk>>,

    /// a vector of non-overlapped and non-duplicates chunks
    pub no_duplicates_chunks: Vec<Arc<dyn QueryChunk>>,

    // execution context
    ctx: IOxSessionContext,
}

impl Deduplicater {
    pub(crate) fn new() -> Self {
        Self {
            overlapped_chunks_set: vec![],
            in_chunk_duplicates_chunks: vec![],
            no_duplicates_chunks: vec![],
            ctx: IOxSessionContext::default(),
        }
    }

    pub(crate) fn with_execution_context(self, ctx: IOxSessionContext) -> Self {
        Self { ctx, ..self }
    }

    /// The IOx scan process needs to deduplicate data if there are duplicates. Hence it will look
    /// like below.
    ///
    /// If `sort_key` is `Some`, the output data of plan will be sorted by that key. If `sort_key` is
    /// `None` the output will not (necessarily) be sorted.
    ///
    /// In the case of sorted plan, plan will include 2 extra operators: the final SortPreservingMergeExec on top and the SortExec
    ///   on top of Chunk 4's IOxReadFilterNode. Detail:
    /// In this example, there are 4 chunks and should be read bottom up as follows:
    ///  . Chunks 1 and 2 overlap and need to get deduplicated. This includes these main steps:
    ///     i. Read/scan/steam the chunk: IOxReadFilterNode.
    ///     ii. Sort each chunk if they are not sorted yet: SortExec.
    ///     iii. Merge the sorted chunks into one stream: SortPreservingMergeExc.
    ///     iv. Deduplicate the sorted stream: DeduplicateExec
    ///     Output data of this branch will be sorted as the result of the deduplication.
    ///  . Chunk 3 does not overlap with others but has duplicates in it self, hence it only needs to get
    ///      sorted if needed, then deduplicated.
    ///     Output data of this branch will be sorted as the result of the deduplication.
    ///  . Chunk 4 neither overlaps with other chunks nor has duplicates in itself, hence it does not
    ///      need any extra besides chunk reading.
    ///     Output data of this branch may NOT be sorted and usually in its input order.
    /// The final UnionExec on top (just below the top SortPreservingMergeExec) is to union the streams below.
    ///   If there is only one stream, UnionExec will not be added into the plan.
    /// In the case the parameter sort_output is true, the output of the plan must be sorted. This is done by
    ///   adding 2 operators: SortExec on top of chunk 4 to sort that chunk, and the top SortPreservingMergeExec
    ///   to merge all four already sorted streams.
    /// ```text
    ///                                      ┌───────────────────────┐
    ///                                      │SortPreservingMergeExec│   <-- This is added if sort_output = true
    ///                                      └───────────────────────┘
    ///                                               ▲
    ///                                               │
    ///                                      ┌─────────────────┐
    ///                                      │    UnionExec    │
    ///                                      │                 │
    ///                                      └─────────────────┘
    ///                                               ▲
    ///                                               │
    ///                        ┌──────────────────────┴───────────┬─────────────────────┐
    ///                        │                                  │                     │
    ///                        │                                  │                     │
    ///               ┌─────────────────┐                ┌─────────────────┐   ┌─────────────────┐
    ///               │ DeduplicateExec │                │ DeduplicateExec │   │     SortExec    │  <-- This is added if output_sort_key.is_some()
    ///               └─────────────────┘                └─────────────────┘   │    (Optional)   │
    ///                        ▲                                  ▲            └─────────────────┘
    ///                        │                                  │                     ▲
    ///            ┌───────────────────────┐                      │                     │
    ///            │SortPreservingMergeExec│                      │             ┌─────────────────┐
    ///            └───────────────────────┘                      │             │IOxReadFilterNode│
    ///                        ▲                                  │             │    (Chunk 4)    │
    ///                        │                                  │             └─────────────────┘
    ///            ┌───────────────────────┐                      │
    ///            │       UnionExec       │                      │
    ///            └───────────────────────┘                      │
    ///                       ▲                                   |
    ///                       │                                   |
    ///           ┌───────────┴───────────┐                       │
    ///           │                       │                       │
    ///  ┌─────────────────┐     ┌─────────────────┐    ┌─────────────────┐
    ///  │    SortExec     │     │    SortExec     │    │    SortExec     │
    ///  │   (optional)    │     │   (optional)    │    │   (optional)    │
    ///  └─────────────────┘     └─────────────────┘    └─────────────────┘
    ///           ▲                       ▲                      ▲
    ///           │                       │                      │
    ///           │                       │                      │
    ///  ┌─────────────────┐     ┌─────────────────┐    ┌─────────────────┐
    ///  │IOxReadFilterNode│     │IOxReadFilterNode│    │IOxReadFilterNode│
    ///  │    (Chunk 1)    │     │    (Chunk 2)    │    │    (Chunk 3)    │
    ///  └─────────────────┘     └─────────────────┘    └─────────────────┘
    ///
    /// # Panic
    ///
    /// Panics if output_sort_key is `Some` and doesn't contain all primary key columns
    ///
    ///```
    pub(crate) fn build_scan_plan(
        &mut self,
        table_name: Arc<str>,
        output_schema: Arc<Schema>,
        chunks: Vec<Arc<dyn QueryChunk>>,
        predicate: Predicate,
        output_sort_key: Option<SortKey>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // find overlapped chunks and put them into the right group
        self.split_overlapped_chunks(chunks.to_vec())?;

        // Building plans
        let mut plans: Vec<Arc<dyn ExecutionPlan>> = vec![];
        if self.no_duplicates() {
            // Neither overlaps nor duplicates, no deduplicating needed
            debug!("All chunks neither overlap nor duplicate. Build only one scan node for all of them.");
            let mut non_duplicate_plans = Self::build_plans_for_non_duplicates_chunks(
                self.ctx.child_ctx("build_plans_for_non_duplicates_chunks"),
                Arc::clone(&table_name),
                Arc::clone(&output_schema),
                chunks,
                predicate,
                output_sort_key.as_ref(),
            )?;
            plans.append(&mut non_duplicate_plans);
        } else {
            let pk_schema = Self::compute_pk_schema(&chunks);
            debug!(overlapped_chunks=?self.overlapped_chunks_set.len(),
                   in_chunk_duplicates=?self.in_chunk_duplicates_chunks.len(),
                   no_duplicates_chunks=?self.no_duplicates_chunks.len(),
                   "Chunks after classifying");

            // Verify that output_sort_key must cover PK
            if let Some(sort_key) = &output_sort_key {
                debug!(%sort_key, "output_sort_key is provided for building deduplicate plan");
                // Technically we only require that the sort order is prefixed by
                // the primary key, in order for deduplication to work correctly
                assert!(
                    pk_schema.len() <= sort_key.len(),
                    "output_sort_key ({:?}) must be at least as long as the primary key ({:?})",
                    sort_key.to_columns(),
                    pk_schema,
                );
                assert!(
                    pk_schema.is_sorted_on_pk(sort_key),
                    "output_sort_key must contain primary key"
                );
            } else {
                debug!("output_sort_key is not provided for building deduplicate plan");
            }

            // This sort key is only used when the chunks are not sorted and output_sort_key is not provided
            let dedup_sort_key_for_unsorted_chunks =
                compute_sort_key_for_chunks(&pk_schema, chunks.as_ref());

            // Go over overlapped set, build deduplicate plan for each vector of overlapped chunks
            for overlapped_chunks in self.overlapped_chunks_set.iter().cloned() {
                let chunks_dedup_sort_key = Self::chunks_dedup_sort_key(
                    &output_sort_key,
                    &overlapped_chunks,
                    &dedup_sort_key_for_unsorted_chunks,
                );

                debug!(
                    ?chunks_dedup_sort_key,
                    ?output_sort_key,
                    ?pk_schema,
                    "Sort keys while building build_deduplicate_plan_for_overlapped_chunks"
                );

                plans.push(Self::build_deduplicate_plan_for_overlapped_chunks(
                    self.ctx
                        .child_ctx("build_deduplicate_plan_for_overlapped_chunks"),
                    Arc::clone(&table_name),
                    Arc::clone(&output_schema),
                    overlapped_chunks,
                    predicate.clone(),
                    &chunks_dedup_sort_key,
                )?);
            }

            // Go over each in_chunk_duplicates_chunks, build deduplicate plan for each
            for chunk_with_duplicates in self.in_chunk_duplicates_chunks.iter().cloned() {
                // Set dedup sort key
                let chunk_dedup_sort_key = Self::chunks_dedup_sort_key(
                    &output_sort_key,
                    &vec![Arc::clone(&chunk_with_duplicates)],
                    &dedup_sort_key_for_unsorted_chunks,
                );

                debug!(
                    ?chunk_dedup_sort_key,
                    ?output_sort_key,
                    ?pk_schema,
                    "Sort keys while building build_deduplicate_plan_for_chunk_with_duplicate"
                );

                plans.push(Self::build_deduplicate_plan_for_chunk_with_duplicates(
                    self.ctx
                        .child_ctx("build_deduplicate_plan_for_chunk_with_duplicates"),
                    Arc::clone(&table_name),
                    Arc::clone(&output_schema),
                    chunk_with_duplicates,
                    predicate.clone(),
                    &chunk_dedup_sort_key,
                )?);
            }

            // Go over non_duplicates_chunks, build a plan for it
            if !self.no_duplicates_chunks.is_empty() {
                debug!(
                    ?output_sort_key,
                    ?pk_schema,
                    "Build one scan node for the rest of neither-duplicated-nor-overlapped chunks."
                );
                let mut non_duplicate_plans = Self::build_plans_for_non_duplicates_chunks(
                    self.ctx.child_ctx("build_plans_for_non_duplicates_chunks"),
                    Arc::clone(&table_name),
                    Arc::clone(&output_schema),
                    self.no_duplicates_chunks.to_vec(),
                    predicate,
                    output_sort_key.as_ref(),
                )?;
                plans.append(&mut non_duplicate_plans);
            }
        }

        if plans.is_empty() {
            // No plan generated. Something must go wrong
            // Even if the chunks are empty, IOxReadFilterNode is still created
            panic!("Internal error generating deduplicate plan");
        }

        let mut plan = match plans.len() {
            // One child plan, no need to add Union
            1 => plans.remove(0),
            // many child plans, add Union
            _ => Arc::new(UnionExec::new(plans)),
        };

        if let Some(sort_key) = &output_sort_key {
            // Sort preserving merge the sorted plans
            // Note that even if the plan is a single plan (aka no UnionExec on top),
            // we still need to add this SortPreservingMergeExec because:
            //    1. It will provide a sorted signal(through Datafusion's Distribution::UnspecifiedDistribution)
            //    2. And it will not do anything extra if the input is one partition so won't affect performance
            let sort_exprs = arrow_sort_key_exprs(sort_key, &plan.schema());
            plan = Arc::new(SortPreservingMergeExec::new(sort_exprs, plan));
        }

        Ok(plan)
    }

    // Return sort key for overlapped chunks
    fn chunks_dedup_sort_key(
        output_sort_key: &Option<SortKey>,
        chunks: &Vec<Arc<dyn QueryChunk>>,
        dedup_sort_key_for_unsorted_chunks: &SortKey,
    ) -> SortKey {
        let chunks_dedup_sort_key = match &output_sort_key {
            // Use output _sort_key if provided
            Some(sort_key) => sort_key.clone(),
            None => {
                // Use the chunk's partition key if they were persisted chunks
                if let Some(sort_key) = if chunks.len() == 1 {
                    chunks[0].sort_key()
                } else {
                    Self::sort_key_of_overlapped_chunks(chunks)
                } {
                    sort_key.clone()
                } else {
                    // This happens either:
                    //   . In the Ingester to compact ingesting data that is not sorted and not
                    //     deduplicated yet
                    //   . In the Querier that also includes data sent from Ingester that is also
                    //     not yet sorted.
                    //        Note: Data sent from Ingester is already deduplicated but if it
                    //        overlaps with other chunks, it may include duplicated data with those
                    //        chunks
                    debug!(
                        "Sort key is computed during planning for deduplicating overlapped chunks."
                    );
                    dedup_sort_key_for_unsorted_chunks.clone()
                }
            }
        };
        chunks_dedup_sort_key
    }

    // Return sort key of overlapped chunks
    // The input chunks must be in the same partition
    fn sort_key_of_overlapped_chunks(chunks: &Vec<Arc<dyn QueryChunk>>) -> Option<&SortKey> {
        if chunks.is_empty() {
            return None;
        }

        // At least one of the overlapped chunks are not sorted
        // Either Ingesting data or data sent from Ingester to Querier will
        // fall into this case
        if chunks.iter().any(|c| c.sort_key() == None) {
            debug!("At least one of the overlapped chunks are not sorted");
            return None;
        }

        // All overlapped chunks must be sorted
        // --> they must come from persisted parquet files of the same partition
        // Get their partition's sort_key
        let partition_sort_key = chunks[0].partition_sort_key();

        // The chunk must have partition sort key
        let partition_sort_key =
            partition_sort_key.expect("Sorted/persisted chunk without partition id");

        // Verify if the partition sort key covers all columns of the chunks sort keys in the same order
        for c in chunks {
            let chunk_sort_key = c.sort_key().expect("Chunk should have sort key");
            if !Self::sort_key_cover_and_same_order(partition_sort_key, chunk_sort_key) {
                panic!(
                    "Partition sort key {} does not cover or is sorted on the same order of the chunk sort key {}",
                    partition_sort_key, chunk_sort_key
                );
            }
        }

        Some(partition_sort_key)
    }

    // return true if the super_sort_key covers sort_key and same column order
    fn sort_key_cover_and_same_order(super_sort_key: &SortKey, sort_key: &SortKey) -> bool {
        if super_sort_key == sort_key {
            return true;
        }

        if let Some(merge_key) = SortKey::try_merge_key(super_sort_key, sort_key) {
            if merge_key == super_sort_key {
                return true;
            }
        }
        false
    }

    /// discover overlaps and split them into three groups:
    ///  1. vector of vector of overlapped chunks
    ///  2. vector of non-overlapped chunks, each have duplicates in itself
    ///  3. vectors of non-overlapped chunks without duplicates
    fn split_overlapped_chunks(&mut self, chunks: Vec<Arc<dyn QueryChunk>>) -> Result<()> {
        trace!("split_overlapped_chunks");

        // -------------------------------
        // Group chunks by partition first
        // Chunks in different partition are guarantee not to ovelap

        // Chunks without assigned partition id should be treated overlapped
        // This is the case of ingester data
        if chunks.iter().any(|c| c.partition_id().is_none()) {
            self.overlapped_chunks_set.push(chunks);
            return Ok(());
        }

        // Group chunks by partition
        let mut partition_groups = HashMap::with_capacity(chunks.len());
        for chunk in chunks {
            let chunks = partition_groups
                .entry(chunk.partition_id().expect("Chunk must have partition id"))
                .or_insert_with(Vec::new);
            chunks.push(chunk);
        }

        // -------------------------------
        // Find all overlapped groups for each partition-group based on their time range
        for (_, chunks) in partition_groups {
            let groups = group_potential_duplicates(chunks).context(InternalChunkGroupingSnafu)?;
            for mut group in groups {
                if group.len() == 1 {
                    if group[0].may_contain_pk_duplicates() {
                        self.in_chunk_duplicates_chunks.append(&mut group);
                    } else {
                        self.no_duplicates_chunks.append(&mut group);
                    }
                } else {
                    self.overlapped_chunks_set.push(group)
                }
            }
        }

        Ok(())
    }

    /// Return true if all chunks neither overlap nor have duplicates in itself
    fn no_duplicates(&self) -> bool {
        self.overlapped_chunks_set.is_empty() && self.in_chunk_duplicates_chunks.is_empty()
    }

    /// Return deduplicate plan for the given overlapped chunks
    ///
    /// The plan will look like this
    ///
    /// ```text
    ///               ┌─────────────────┐
    ///               │ ProjectionExec  │
    ///               │  (optional)     │
    ///               └─────────────────┘
    ///                        ▲
    ///                        │
    ///               ┌─────────────────┐
    ///               │ DeduplicateExec │
    ///               └─────────────────┘
    ///                        ▲
    ///                        │
    ///            ┌───────────────────────┐
    ///            │SortPreservingMergeExec│
    ///            └───────────────────────┘
    ///                        ▲
    ///                        │
    ///            ┌───────────────────────┐
    ///            │       UnionExec       │
    ///            └───────────────────────┘
    ///                       ▲
    ///                       │
    ///           ┌───────────┴───────────┐
    ///           │                       │
    ///  ┌─────────────────┐        ┌─────────────────┐
    ///  │    SortExec     │ ...    │    SortExec     │
    ///  │   (optional)    │        │   (optional)    │
    ///  └─────────────────┘        └─────────────────┘
    ///           ▲                          ▲
    ///           │          ...             │
    ///           │                          │
    ///  ┌─────────────────┐        ┌─────────────────┐
    ///  │IOxReadFilterNode│        │IOxReadFilterNode│
    ///  │    (Chunk 1)    │ ...    │    (Chunk n)    │
    ///  └─────────────────┘        └─────────────────┘
    ///```
    fn build_deduplicate_plan_for_overlapped_chunks(
        ctx: IOxSessionContext,
        table_name: Arc<str>,
        output_schema: Arc<Schema>,
        chunks: Vec<Arc<dyn QueryChunk>>, // These chunks are identified overlapped
        predicate: Predicate,
        sort_key: &SortKey,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Note that we may need to sort/deduplicate based on tag
        // columns which do not appear in the output

        // We need to sort chunks before creating the execution plan. For that, the chunk order is used. Since the order
        // only sorts overlapping chunks, we also use the chunk ID for deterministic outputs.
        let chunks = {
            let mut chunks = chunks;
            chunks.sort_unstable_by_key(|c| (c.order(), c.id()));
            chunks
        };

        let pk_schema = Self::compute_pk_schema(&chunks);
        let input_schema = Self::compute_input_schema(&output_schema, &pk_schema);

        debug!(
            ?output_schema,
            ?pk_schema,
            ?input_schema,
            "creating deduplicate plan for overlapped chunks"
        );

        // Build sort plan for each chunk
        let sorted_chunk_plans: Result<Vec<Arc<dyn ExecutionPlan>>> = chunks
            .iter()
            .map(|chunk| {
                Self::build_sort_plan_for_read_filter(
                    ctx.child_ctx("build_sort_plan_for_read_filter"),
                    Arc::clone(&table_name),
                    Arc::clone(&input_schema),
                    Arc::clone(chunk),
                    predicate.clone(),
                    Some(sort_key),
                )
            })
            .collect();

        // Union the plans
        // The UnionExec operator only streams all chunks (aka partitions in Datafusion) and
        // keep them in separate chunks which exactly what we need here
        let plan = UnionExec::new(sorted_chunk_plans?);

        // Now (sort) merge the already sorted chunks
        let sort_exprs = arrow_sort_key_exprs(sort_key, &plan.schema());

        let plan = Arc::new(SortPreservingMergeExec::new(
            sort_exprs.clone(),
            Arc::new(plan),
        ));

        // Add DeduplicateExc
        let plan = Self::add_deduplicate_node(sort_exprs, plan);

        // select back to the requested output schema
        Self::add_projection_node_if_needed(output_schema, plan)
    }

    /// Return deduplicate plan for a given chunk with duplicates
    /// The plan will look like this
    /// ```text
    ///                ┌─────────────────┐
    ///                │ ProjectionExec  │
    ///                │  (optional)     │
    ///                └─────────────────┘
    ///                        ▲
    ///                        │
    ///                ┌─────────────────┐
    ///                │ DeduplicateExec │
    ///                └─────────────────┘
    ///                        ▲
    ///                        │
    ///                ┌─────────────────┐
    ///                │    SortExec     │
    ///                │   (optional)    │
    ///                └─────────────────┘
    ///                        ▲
    ///                        │
    ///                ┌─────────────────┐
    ///                │IOxReadFilterNode│
    ///                │    (Chunk)      │
    ///                └─────────────────┘
    ///```
    fn build_deduplicate_plan_for_chunk_with_duplicates(
        ctx: IOxSessionContext,
        table_name: Arc<str>,
        output_schema: Arc<Schema>,
        chunk: Arc<dyn QueryChunk>, // This chunk is identified having duplicates
        predicate: Predicate,
        sort_key: &SortKey,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let pk_schema = Self::compute_pk_schema(&[Arc::clone(&chunk)]);
        let input_schema = Self::compute_input_schema(&output_schema, &pk_schema);

        debug!(
            ?output_schema,
            ?pk_schema,
            ?input_schema,
            "creating deduplicate plan for a chunk with duplicates"
        );

        // Compute the output sort key for this chunk
        let chunks = vec![chunk];

        // Create the 2 bottom nodes IOxReadFilterNode and SortExec
        let plan = Self::build_sort_plan_for_read_filter(
            ctx.child_ctx("build_sort_plan_for_read_filter"),
            table_name,
            Arc::clone(&input_schema),
            Arc::clone(&chunks[0]),
            predicate,
            Some(sort_key),
        )?;

        // Add DeduplicateExec
        // Sort exprs for the deduplication
        let sort_exprs = arrow_sort_key_exprs(sort_key, &plan.schema());
        debug!(?sort_exprs, chunk_id=?chunks[0].id(), "Sort Expression for the deduplicate node of chunk");
        let plan = Self::add_deduplicate_node(sort_exprs, plan);

        // select back to the requested output schema
        Self::add_projection_node_if_needed(output_schema, plan)
    }

    /// Hooks DeduplicateExec on top of the given input plan
    fn add_deduplicate_node(
        sort_exprs: Vec<PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(DeduplicateExec::new(input, sort_exprs))
    }

    /// Creates a plan that produces output_schema given a plan that
    /// produces the input schema
    ///
    /// ```text
    /// ┌─────────────────┐
    /// │ ProjectionExec  │
    /// │  (optional)     │
    /// └─────────────────┘
    ///```

    fn add_projection_node_if_needed(
        output_schema: Arc<Schema>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_schema = input.schema();
        let output_schema = output_schema.as_arrow();

        // If columns are the same, nothing to do
        let same_columns = input_schema.fields().len() == output_schema.fields().len()
            && input_schema
                .fields()
                .iter()
                .zip(output_schema.fields())
                .all(|(a, b)| a.name() == b.name());

        if same_columns {
            return Ok(input);
        }

        // build select exprs for the requested fields
        let select_exprs = output_schema
            .fields()
            .iter()
            .map(|f| {
                let field_name = f.name();
                let physical_expr =
                    physical_col(field_name, &input_schema).context(InternalSelectExprSnafu)?;
                Ok((physical_expr, field_name.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;

        let plan = ProjectionExec::try_new(select_exprs, input).context(InternalProjectionSnafu)?;
        Ok(Arc::new(plan))
    }

    /// Return a sort plan for for a given chunk
    /// This plan is applied for every chunk to read data from chunk
    /// The plan will look like this. Reading bottom up:
    ///
    ///   1. First we scan the data in IOxReadFilterNode which represents a custom implemented scan
    ///      of the chunk. Both Select Predicate of the query and Delete Predicates of the chunk is
    ///      pushed down here to eliminate as much data as early as possible but it is not
    ///      guaranteed all filters are applied because only certain expressions work at this low
    ///      chunk scan level. Delete Predicates are tombstone of deleted data that will be
    ///      eliminated at read time.
    ///   2. If the chunk has Delete Predicates, the FilterExec will be added to filter data out.
    ///      We apply delete predicate filter at this low level because the Delete Predicates are
    ///      chunk specific.
    ///   3. Then SortExec is added if there is a request to sort this chunk at this stage.
    ///      See the description of function build_scan_plan to see why the sort may be needed.
    ///
    /// ```text
    ///                ┌─────────────────┐
    ///                │ ProjectionExec  │
    ///                │  (optional)     │
    ///                └─────────────────┘
    ///                         ▲
    ///                         │
    ///                ┌─────────────────┐
    ///                │    SortExec     │
    ///                │   (optional)    │
    ///                └─────────────────┘
    ///                          ▲
    ///                          │
    ///                          │
    ///                ┌─────────────────────────┐
    ///                │ FilterExec (optional)   │
    ///                | To apply delete preds   │
    ///                │    (Chunk)              │
    ///                └─────────────────────────┘
    ///                          ▲
    ///                          │
    ///                          │
    ///                ┌─────────────────┐
    ///                │IOxReadFilterNode│
    ///                │    (Chunk)      │
    ///                └─────────────────┘
    ///```
    fn build_sort_plan_for_read_filter(
        ctx: IOxSessionContext,
        table_name: Arc<str>,
        output_schema: Arc<Schema>,
        chunk: Arc<dyn QueryChunk>,
        predicate: Predicate, // This is the select predicate of the query
        sort_key: Option<&SortKey>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Add columns of sort key and delete predicates in the schema of to-be-scanned IOxReadFilterNode
        // This is needed because columns in select query may not include them yet

        // Construct a schema to pass to IOxReadFilterNode that contains:
        //
        // 1. all columns present in the output schema
        // 2. all columns present in the sort key that are present in the chunk
        // 3. all columns present in any delete predicates on the chunk
        //
        // Any columns present in the schema but not in the chunk, will be padded with NULLs
        // by IOxReadFilterNode
        //
        // 1. ensures that the schema post-projection matches output_schema
        // 2. ensures that all columns necessary to perform the sort are present
        // 3. ensures that all columns necessary to evaluate the delete predicates are present
        trace!("Build sort plan for a single chunk. Sort node won't be added if the plan is already sorted");
        let mut schema_merger = SchemaMerger::new().merge(&output_schema).unwrap();
        let chunk_schema = chunk.schema();
        trace!(?chunk_schema, "chunk schema");

        // Cols of sort key
        if let Some(key) = sort_key {
            for (t, field) in chunk_schema.iter() {
                // Ignore columns present in sort key but not in chunk
                if key.get(field.name()).is_some() {
                    schema_merger.merge_field(field, t).unwrap();
                }
            }
        }

        // Cols of delete predicates
        if chunk.has_delete_predicates() {
            for col in chunk.delete_predicate_columns() {
                let idx = chunk_schema
                    .find_index_of(col)
                    .expect("delete predicate missing column");

                let (t, field) = chunk_schema.field(idx);
                schema_merger.merge_field(field, t).unwrap();
            }
        }

        let input_schema = schema_merger.build();

        // Create the bottom node IOxReadFilterNode for this chunk
        let mut input: Arc<dyn ExecutionPlan> = Arc::new(IOxReadFilterNode::new(
            ctx,
            Arc::clone(&table_name),
            Arc::new(input_schema),
            vec![Arc::clone(&chunk)],
            predicate,
        ));

        // Add Filter operator, FilterExec, if the chunk has delete predicates
        let del_preds = chunk.delete_predicates();
        let del_preds: Vec<Arc<Predicate>> = del_preds
            .iter()
            .map(|pred| Arc::new(pred.as_ref().clone().into()))
            .collect();

        trace!(?del_preds, "Chunk delete predicates");
        let negated_del_expr_val = Predicate::negated_expr(&del_preds[..]);
        if let Some(negated_del_expr) = negated_del_expr_val {
            debug!(?negated_del_expr, "Logical negated expressions");

            let negated_physical_del_expr =
                df_physical_expr(&*input, negated_del_expr).context(InternalFilterSnafu)?;
            debug!(?negated_physical_del_expr, "Physical negated expressions");

            input = Arc::new(
                FilterExec::try_new(negated_physical_del_expr, input)
                    .context(InternalFilterSnafu)?,
            );
        }

        // Add the sort operator, SortExec, if needed
        if let Some(sort_key) = sort_key {
            let mut add_sort_op = true;
            if let Some(chunk_sort_key) = chunk.sort_key() {
                if Self::sort_key_cover_and_same_order(sort_key, chunk_sort_key) {
                    // the chunk is already sorted
                    add_sort_op = false;
                }
            }
            if add_sort_op {
                input = Self::build_sort_plan(chunk, input, sort_key)?
            }
        }

        // Add a projection operator to return only schema of the operator above this in the plan
        // This is needed for matching column index of that operator
        Self::add_projection_node_if_needed(output_schema, input)
    }

    /// Add SortExec operator on top of the input plan of the given chunk
    /// The plan will be sorted on the chunk's primary key
    fn build_sort_plan(
        chunk: Arc<dyn QueryChunk>,
        input: Arc<dyn ExecutionPlan>,
        output_sort_key: &SortKey,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // output_sort_key cannot be empty
        if output_sort_key.is_empty() {
            panic!("Super sort key is empty");
        }

        debug!(output_sort_key=?output_sort_key, "Super sort key input to build_sort_plan");

        // Check to see if the plan is sorted on the subset of the output_sort_key
        let sort_key = chunk.sort_key();
        if let Some(chunk_sort_key) = sort_key {
            if let Some(merge_key) = SortKey::try_merge_key(output_sort_key, chunk_sort_key) {
                if merge_key == output_sort_key {
                    // the chunk is already sorted on the subset of the o_sort_key,
                    // no need to resort it
                    debug!(ChunkID=?chunk.id(), "Chunk is sorted and no need the sort operator");
                    return Ok(input);
                }
            } else {
                // The chunk is sorted but not on different order with super sort key.
                // Log it for investigating data set to improve performance further
                // This behavior should not happen when ingester, compactor and querier use the sort key
                warn!(chunk_type=?chunk.chunk_type(),
                    chunk_ID=?chunk.id(),
                    chunk_current_sort_order=?chunk_sort_key,
                    chunk_super_sort_key=?output_sort_key,
                    "Chunk will get resorted in build_sort_plan because it was sorted on different sort key");
            }
        } else {
            debug!(chunk_type=?chunk.chunk_type(),
                chunk_ID=?chunk.id(),
                "Chunk is not yet sorted and will get sorted in build_sort_plan");
        }

        // Build arrow sort expression for the chunk sort key
        let input_schema = input.schema();
        let sort_exprs = arrow_sort_key_exprs(output_sort_key, &input_schema);

        debug!(?sort_exprs, chunk_id=?chunk.id(), "Sort Expression for the sort operator of chunk");

        // Create SortExec operator
        Ok(Arc::new(
            SortExec::try_new(sort_exprs, input).context(InternalSortSnafu)?,
        ))
    }

    /// Return the simplest IOx scan plan of a given chunk which is IOxReadFilterNode
    // And some optional operators on top such as applying delete predicates or sort the chunk
    fn build_plan_for_non_duplicates_chunk(
        ctx: IOxSessionContext,
        table_name: Arc<str>,
        output_schema: Arc<Schema>,
        chunk: Arc<dyn QueryChunk>, // This chunk is identified having no duplicates
        predicate: Predicate,
        sort_key: Option<&SortKey>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Self::build_sort_plan_for_read_filter(
            ctx,
            table_name,
            output_schema,
            chunk,
            predicate,
            sort_key,
        )
    }

    /// Return either
    ///  The simplest IOx scan plan for chunks without delete predicates
    ///  and no need to sort is IOxReadFilterNode:
    /// ```text
    ///                ┌─────────────────┐
    ///                │IOxReadFilterNode│
    ///                │ (No Chunks)     │
    ///                └─────────────────┘
    /// ```
    /// Or, many plans, one for each chunk, like this:
    /// ```text
    ///   ┌─────────────────┐                   ┌─────────────────┐
    ///   │    SortExec     │                   │    SortExec     │
    ///   │   (optional)    │                   │   (optional)    │
    ///   └─────────────────┘                   └─────────────────┘
    ///            ▲                                     ▲
    ///            │                                     │
    ///            │
    /// ┌─────────────────────────┐          ┌─────────────────────────┐
    /// │ FilterExec (optional)   │          │ FilterExec (optional)   │
    /// | To apply delete preds   │   .....  | To apply delete preds   │
    /// │    (Chunk 1)            │          │    (Chunk n)            │
    /// └─────────────────────────┘          └─────────────────────────┘
    ///            ▲                                     ▲
    ///            │                                     │
    ///            │                                     │
    ///   ┌─────────────────┐                   ┌─────────────────┐
    ///   │IOxReadFilterNode│                   │IOxReadFilterNode│
    ///   │    (Chunk 1)    │                   │    (Chunk n)    │
    ///   └─────────────────┘                   └─────────────────┘
    ///```
    fn build_plans_for_non_duplicates_chunks(
        ctx: IOxSessionContext,
        table_name: Arc<str>,
        output_schema: Arc<Schema>,
        chunks: Vec<Arc<dyn QueryChunk>>, // These chunks is identified having no duplicates
        predicate: Predicate,
        output_sort_key: Option<&SortKey>,
    ) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
        let mut plans: Vec<Arc<dyn ExecutionPlan>> = vec![];

        // Only chunks without delete predicates should be in this one IOxReadFilterNode
        // if there is no chunk, we still need to return a plan
        if (output_sort_key.is_none() && Self::no_delete_predicates(&chunks)) || chunks.is_empty() {
            debug!("Build one scan IOxReadFilterNode for all non duplicated chunks even if empty");
            plans.push(Arc::new(IOxReadFilterNode::new(
                ctx,
                Arc::clone(&table_name),
                output_schema,
                chunks,
                predicate,
            )));

            return Ok(plans);
        }

        // Build sorted plans, one for each chunk
        debug!("Consider to add a sort node if needed for every non duplicated chunk");
        let sorted_chunk_plans: Result<Vec<Arc<dyn ExecutionPlan>>> = chunks
            .iter()
            .map(|chunk| {
                Self::build_plan_for_non_duplicates_chunk(
                    ctx.child_ctx("build_plan_for_non_duplicates_chunk"),
                    Arc::clone(&table_name),
                    Arc::clone(&output_schema),
                    Arc::clone(chunk),
                    predicate.clone(),
                    output_sort_key,
                )
            })
            .collect();

        sorted_chunk_plans
    }

    fn no_delete_predicates(chunks: &[Arc<dyn QueryChunk>]) -> bool {
        chunks
            .iter()
            .all(|chunk| chunk.delete_predicates().is_empty())
    }

    /// Find the columns needed in chunks' primary keys across schemas
    fn compute_pk_schema(chunks: &[Arc<dyn QueryChunk>]) -> Arc<Schema> {
        let mut schema_merger = SchemaMerger::new();
        for chunk in chunks {
            let chunk_schema = chunk.schema();
            for (column_type, field) in chunk_schema.iter() {
                if matches!(
                    column_type,
                    Some(InfluxColumnType::Tag | InfluxColumnType::Timestamp)
                ) {
                    schema_merger
                        .merge_field(field, column_type)
                        .expect("schema mismatch");
                }
            }
        }

        Arc::new(schema_merger.build())
    }

    /// Find columns required to read from each scan: the output columns + the
    /// primary key columns
    fn compute_input_schema(output_schema: &Schema, pk_schema: &Schema) -> Arc<Schema> {
        let input_schema = SchemaMerger::new()
            .merge(output_schema)
            .unwrap()
            .merge(pk_schema)
            .unwrap()
            .build();
        Arc::new(input_schema)
    }
}

#[derive(Debug)]
/// A pruner that does not do pruning (suitable if no additional pruning is possible)
struct NoOpPruner {}
impl ChunkPruner for NoOpPruner {
    fn prune_chunks(
        &self,
        _table_name: &str,
        _table_schema: Arc<Schema>,
        chunks: Vec<Arc<dyn QueryChunk>>,
        _predicate: &Predicate,
    ) -> Vec<Arc<dyn QueryChunk>> {
        chunks
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::{raw_data, TestChunk};
    use arrow::datatypes::DataType;
    use arrow_util::{assert_batches_eq, assert_batches_sorted_eq};
    use datafusion::physical_plan::displayable;
    use datafusion_util::test_collect;
    use schema::{builder::SchemaBuilder, TIME_COLUMN_NAME};
    use std::num::NonZeroU64;

    #[test]
    fn chunk_grouping() {
        // This test just ensures that all the plumbing is connected
        // for chunk grouping. The logic of the grouping is tested
        // in the duplicate module

        // c1: no overlaps
        let c1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_partition_id(10)
                .with_time_column_with_stats(Some(1), Some(10)),
        );

        // c2: over lap with c3
        let c2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_partition_id(10)
                .with_time_column_with_stats(Some(15), Some(20)),
        );

        // c3: overlap with c2
        let c3 = Arc::new(
            TestChunk::new("t")
                .with_id(3)
                .with_partition_id(10)
                .with_time_column_with_stats(Some(17), Some(23)),
        );

        // c4: self overlap
        let c4 = Arc::new(
            TestChunk::new("t")
                .with_id(4)
                .with_partition_id(10)
                .with_time_column_with_stats(Some(30), Some(40))
                .with_may_contain_pk_duplicates(true),
        );

        let mut deduplicator = Deduplicater::new();
        deduplicator
            .split_overlapped_chunks(vec![c1, c2, c3, c4])
            .expect("split chunks");

        assert_eq!(
            chunk_group_ids(&deduplicator.overlapped_chunks_set),
            vec!["Group 0: 00000000-0000-0000-0000-000000000002, 00000000-0000-0000-0000-000000000003"]
        );
        assert_eq!(
            chunk_ids(&deduplicator.in_chunk_duplicates_chunks),
            "00000000-0000-0000-0000-000000000004"
        );
        assert_eq!(
            chunk_ids(&deduplicator.no_duplicates_chunks),
            "00000000-0000-0000-0000-000000000001"
        );
    }

    #[test]
    fn chunk_grouping_no_partition() {
        // At least one chunk without assigned partition, all chunks are considered overlapped even if their time ranges do not

        // c1: no time-range overlaps
        let c1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_partition_id(10)
                .with_time_column_with_stats(Some(1), Some(10)),
        );

        // c2: time-range overlap with c3
        let c2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_partition_id(10)
                .with_time_column_with_stats(Some(15), Some(20)),
        );

        // c3: time-range overlap with c2
        // no partition provided
        let c3 = Arc::new(
            TestChunk::new("t")
                .with_id(3)
                .with_time_column_with_stats(Some(17), Some(23)),
        );

        // c4: time-range self overlap
        let c4 = Arc::new(
            TestChunk::new("t")
                .with_id(4)
                .with_partition_id(10)
                .with_time_column_with_stats(Some(30), Some(40))
                .with_may_contain_pk_duplicates(true),
        );

        let mut deduplicator = Deduplicater::new();
        deduplicator
            .split_overlapped_chunks(vec![c1, c2, c3, c4])
            .expect("split chunks");

        assert_eq!(
            chunk_group_ids(&deduplicator.overlapped_chunks_set),
            vec!["Group 0: 00000000-0000-0000-0000-000000000001, 00000000-0000-0000-0000-000000000002, 00000000-0000-0000-0000-000000000003, 00000000-0000-0000-0000-000000000004"]
        );
    }

    #[tokio::test]
    async fn sort_planning_one_tag_with_time() {
        test_helpers::maybe_start_logging();

        // Chunk 1 with 5 rows of data
        let chunk = Arc::new(
            TestChunk::new("t")
                .with_time_column()
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        let sort_key = SortKey::from_columns(vec!["tag1", TIME_COLUMN_NAME]);

        // IOx scan operator
        let input: Arc<dyn ExecutionPlan> = Arc::new(IOxReadFilterNode::new(
            IOxSessionContext::default(),
            Arc::from("t"),
            chunk.schema(),
            vec![Arc::clone(&chunk)],
            Predicate::default(),
        ));

        // plan should not have sort operator
        let plan = format!("{}", displayable(input.as_ref()).indent());
        assert!(!plan.contains("SortExec"));

        let batch = test_collect(Arc::clone(&input)).await;
        // data in its original non-sorted form
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);

        // Add Sort operator on top of IOx scan
        let sort_plan = Deduplicater::build_sort_plan(chunk, input, &sort_key).unwrap();

        // Plan should have sort operator
        let plan = format!("{}", displayable(sort_plan.as_ref()).indent());
        assert!(plan.contains("SortExec"));

        let batch = test_collect(sort_plan).await;
        // data is sorted on (tag1, time)
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn sort_planning_two_tags_with_time() {
        test_helpers::maybe_start_logging();

        // Chunk 1 with 5 rows of data
        let chunk = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_full_stats(
                    Some(5),
                    Some(7000),
                    5,
                    Some(NonZeroU64::new(5).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("AL"),
                    Some("MT"),
                    5,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag2",
                    Some("AL"),
                    Some("MA"),
                    5,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        let sort_key = SortKey::from_columns(vec!["tag1", "tag2", "tag3", TIME_COLUMN_NAME]);

        // IOx scan operator
        let input: Arc<dyn ExecutionPlan> = Arc::new(IOxReadFilterNode::new(
            IOxSessionContext::default(),
            Arc::from("t"),
            chunk.schema(),
            vec![Arc::clone(&chunk)],
            Predicate::default(),
        ));
        let batch = test_collect(Arc::clone(&input)).await;
        // data in its original non-sorted form
        let expected = vec![
            "+-----------+------+------+--------------------------------+",
            "| field_int | tag1 | tag2 | time                           |",
            "+-----------+------+------+--------------------------------+",
            "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | AL   | 1970-01-01T00:00:00.000005Z    |",
            "+-----------+------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);

        // Add Sort operator on top of IOx scan
        let sort_plan = Deduplicater::build_sort_plan(chunk, input, &sort_key).unwrap();
        let batch = test_collect(sort_plan).await;
        // with the provider stats, data is sorted on: (tag1, tag2, time)
        let expected = vec![
            "+-----------+------+------+--------------------------------+",
            "| field_int | tag1 | tag2 | time                           |",
            "+-----------+------+------+--------------------------------+",
            "| 100       | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 5         | MT   | AL   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
            "+-----------+------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn sort_read_filter_plan_for_two_tags_with_time() {
        test_helpers::maybe_start_logging();

        // Chunk 1 with 5 rows of data
        let chunk = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_full_stats(
                    Some(5),
                    Some(7000),
                    5,
                    Some(NonZeroU64::new(5).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("AL"),
                    Some("MT"),
                    5,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag2",
                    Some("AL"),
                    Some("MA"),
                    5,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        let sort_key = SortKey::from_columns(vec!["tag1", "tag2", TIME_COLUMN_NAME]);

        // Datafusion schema of the chunk
        let schema = chunk.schema();

        let sort_plan = Deduplicater::build_sort_plan_for_read_filter(
            IOxSessionContext::default(),
            Arc::from("t"),
            Arc::clone(&schema),
            Arc::clone(&chunk),
            Predicate::default(),
            Some(&sort_key.clone()),
        )
        .unwrap();

        // Plan should have sort operator because the chunk is not sorted
        let plan = format!("{}", displayable(sort_plan.as_ref()).indent());

        assert!(plan.contains("SortExec"));

        let batch = test_collect(sort_plan).await;
        // with provided stats, data is sorted on (tag1, tag2, time)
        let expected = vec![
            "+-----------+------+------+--------------------------------+",
            "| field_int | tag1 | tag2 | time                           |",
            "+-----------+------+------+--------------------------------+",
            "| 100       | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 5         | MT   | AL   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
            "+-----------+------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);

        // -----------------------------------------
        // Create a sorted chunk
        let chunk = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_i64_field_column("field_int")
                .with_sort_key(sort_key.clone()), // signal the chunk is sorted
        ) as Arc<dyn QueryChunk>;

        let sort_plan = Deduplicater::build_sort_plan_for_read_filter(
            IOxSessionContext::default(),
            Arc::from("t"),
            schema,
            Arc::clone(&chunk),
            Predicate::default(),
            Some(&sort_key),
        )
        .unwrap();

        // Plan should NOT have sort operator because the chunk is already sorted on the same sort key
        let plan = format!("{}", displayable(sort_plan.as_ref()).indent());
        assert!(!plan.contains("SortExec"));
    }

    #[tokio::test]
    async fn test_build_deduplicate_plan_for_chunk_with_duplicates_explain() {
        let sort_key = SortKey::from_columns(vec!["tag1", "tag2", TIME_COLUMN_NAME]);

        // -----------------------------------------
        // Create a sorted chunk
        let chunk = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_i64_field_column("field_int")
                .with_sort_key(sort_key.clone()), // signal the chunk is sorted
        ) as Arc<dyn QueryChunk>;

        // Datafusion schema of the chunk
        let schema = chunk.schema();

        let plan = Deduplicater::build_deduplicate_plan_for_chunk_with_duplicates(
            IOxSessionContext::default(),
            Arc::from("t"),
            schema,
            Arc::clone(&chunk),
            Predicate::default(),
            &sort_key,
        )
        .unwrap();

        let plan = format!("{}", displayable(plan.as_ref()).indent());
        // Plan should NOT have sort operator because the chunk is already sorted on the same sort key
        assert!(!plan.contains("SortExec"));
        // Should have DeduplicateExec
        assert!(plan.contains("DeduplicateExec"));

        // -----------------------------------------
        // Create a non-sorted chunk
        let chunk = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_i64_field_column("field_int"),
        ) as Arc<dyn QueryChunk>;

        // Datafusion schema of the chunk
        let schema = chunk.schema();

        let plan = Deduplicater::build_deduplicate_plan_for_chunk_with_duplicates(
            IOxSessionContext::default(),
            Arc::from("t"),
            schema,
            Arc::clone(&chunk),
            Predicate::default(),
            &sort_key,
        )
        .unwrap();

        let plan = format!("{}", displayable(plan.as_ref()).indent());
        // Plan should have sort operator
        assert!(plan.contains("SortExec"));
        // Should have DeduplicateExec
        assert!(plan.contains("DeduplicateExec"));
    }

    #[tokio::test]
    async fn test_build_plans_for_non_duplicates_chunks_explain() {
        let sort_key = SortKey::from_columns(vec!["tag1", "tag2", TIME_COLUMN_NAME]);

        // Sorted Chunk 1 with 5 rows of data on 2 tags
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_i64_field_column("field_int")
                .with_sort_key(sort_key.clone()),
        ) as Arc<dyn QueryChunk>;

        // Non-sorted Chunk 2
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_time_column()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_i64_field_column("field_int"),
        ) as Arc<dyn QueryChunk>;

        // Datafusion schema of the chunk
        let schema = chunk1.schema();

        // All chunks in one single scan
        let plans = Deduplicater::build_plans_for_non_duplicates_chunks(
            IOxSessionContext::default(),
            Arc::from("t"),
            Arc::clone(&schema),
            vec![Arc::clone(&chunk1), Arc::clone(&chunk2)],
            Predicate::default(),
            None, // not ask to sort the output of the plan
        )
        .unwrap();

        // should have only one output plan
        assert_eq!(plans.len(), 1);
        let plan = format!("{}", displayable(plans[0].as_ref()).indent());
        // Plan should NOT have sort operator
        assert!(!plan.contains("SortExec"));
        // Should not have DeduplicateExec
        assert!(!plan.contains("DeduplicateExec"));

        // -----------------------------------
        // Each chunk in its own plan becasue sorting on each chunk is asked
        let plans = Deduplicater::build_plans_for_non_duplicates_chunks(
            IOxSessionContext::default(),
            Arc::from("t"),
            schema,
            vec![Arc::clone(&chunk1), Arc::clone(&chunk2)],
            Predicate::default(),
            Some(&sort_key), // sort output on this sort_key
        )
        .unwrap();

        // should have two output plans
        assert_eq!(plans.len(), 2);
        // First plan should NOT have sort operator because already sorted
        let plan = format!("{}", displayable(plans[0].as_ref()).indent());
        assert!(!plan.contains("SortExec"));
        // Second plan should have sort operator
        let plan = format!("{}", displayable(plans[1].as_ref()).indent());
        assert!(plan.contains("SortExec"));
        // Should not have DeduplicateExec
        assert!(!plan.contains("DeduplicateExec"));
    }

    #[tokio::test]
    async fn test_sort_key_of_overlapped_chunks() {
        // Empty chunks
        let chunks = vec![];
        let result = Deduplicater::sort_key_of_overlapped_chunks(&chunks);
        assert!(result.is_none());

        // One not-sorted chunk
        let chunk1 = Arc::new(TestChunk::new("t")) as Arc<dyn QueryChunk>;
        let chunks = vec![chunk1];
        let result = Deduplicater::sort_key_of_overlapped_chunks(&chunks);
        assert!(result.is_none());

        // One sorted chunk whose sort key and partition sort key are the same
        let sort_key = SortKey::from_columns(vec!["tag1", TIME_COLUMN_NAME]);
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_sort_key(sort_key.clone())
                .with_partition_sort_key(sort_key.clone()),
        ) as Arc<dyn QueryChunk>;
        let chunks = vec![chunk1];
        let result = Deduplicater::sort_key_of_overlapped_chunks(&chunks).unwrap();
        assert_eq!(*result, sort_key);

        // partition sort key is a super key of the sort key
        let sort_key = SortKey::from_columns(vec!["tag1", TIME_COLUMN_NAME]);
        let partition_sort_key = SortKey::from_columns(vec!["tag1", "tag2", TIME_COLUMN_NAME]);
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_sort_key(sort_key)
                .with_partition_sort_key(partition_sort_key.clone()),
        ) as Arc<dyn QueryChunk>;
        let chunks = vec![chunk1];
        let result = Deduplicater::sort_key_of_overlapped_chunks(&chunks).unwrap();
        assert_eq!(*result, partition_sort_key);
    }

    #[tokio::test]
    async fn test_sort_key_of_overlapped_chunks_many_chunks() {
        // One not-sorted chunk, one sorted
        let sort_key = SortKey::from_columns(vec!["tag1", TIME_COLUMN_NAME]);
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_sort_key(sort_key.clone())
                .with_partition_sort_key(sort_key),
        ) as Arc<dyn QueryChunk>;
        let chunk2 = Arc::new(TestChunk::new("t")) as Arc<dyn QueryChunk>;
        let chunks = vec![chunk1, chunk2];
        let result = Deduplicater::sort_key_of_overlapped_chunks(&chunks);
        assert!(result.is_none());

        // Two sorted chunks
        let partition_sort_key = SortKey::from_columns(vec!["tag2", "tag1", TIME_COLUMN_NAME]);
        let sort_key_1 = SortKey::from_columns(vec!["tag1", TIME_COLUMN_NAME]);
        let sort_key_2 = SortKey::from_columns(vec!["tag2", TIME_COLUMN_NAME]);
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_sort_key(sort_key_1)
                .with_partition_sort_key(partition_sort_key.clone()),
        ) as Arc<dyn QueryChunk>;
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_sort_key(sort_key_2)
                .with_partition_sort_key(partition_sort_key.clone()),
        ) as Arc<dyn QueryChunk>;
        let chunks = vec![chunk1, chunk2];
        let result = Deduplicater::sort_key_of_overlapped_chunks(&chunks).unwrap();
        assert_eq!(*result, partition_sort_key);
    }

    #[should_panic(
        expected = "Partition sort key tag2, tag1, time, does not cover or is sorted on the same order of the chunk sort key tag3, time,"
    )]
    #[tokio::test]
    async fn test_sort_key_of_overlapped_chunks_negative() {
        // Two sorted chunks but partition sort key does not cover the sort key
        let partition_sort_key = SortKey::from_columns(vec!["tag2", "tag1", TIME_COLUMN_NAME]);
        let sort_key_1 = SortKey::from_columns(vec!["tag3", TIME_COLUMN_NAME]); // tag3 is notincluded in partition_sort_key
        let sort_key_2 = SortKey::from_columns(vec!["tag2", TIME_COLUMN_NAME]);
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_sort_key(sort_key_1)
                .with_partition_sort_key(partition_sort_key.clone()),
        ) as Arc<dyn QueryChunk>;
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_sort_key(sort_key_2)
                .with_partition_sort_key(partition_sort_key),
        ) as Arc<dyn QueryChunk>;
        // will panic
        Deduplicater::sort_key_of_overlapped_chunks(&vec![chunk1, chunk2]).unwrap();
    }

    #[tokio::test]
    async fn deduplicate_plan_for_overlapped_chunks() {
        test_helpers::maybe_start_logging();

        // Chunk 1 with 5 rows of data on 2 tags
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        // Chunk 2 exactly the same with Chunk 1
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_time_column()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;
        // Datafusion schema of the chunk
        // the same for 2 chunks
        let schema = chunk1.schema();
        let chunks = vec![chunk1, chunk2];

        // data in its original form
        let expected = vec![
            "+-----------+------+------+--------------------------------+",
            "| field_int | tag1 | tag2 | time                           |",
            "+-----------+------+------+--------------------------------+",
            "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | AL   | 1970-01-01T00:00:00.000005Z    |",
            "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | AL   | 1970-01-01T00:00:00.000005Z    |",
            "+-----------+------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        let output_sort_key = SortKey::from_columns(vec!["tag1", "tag2", "time"]);
        let sort_plan = Deduplicater::build_deduplicate_plan_for_overlapped_chunks(
            IOxSessionContext::default(), //TODO(edd): address this.
            Arc::from("t"),
            Arc::clone(&schema),
            chunks,
            Predicate::default(),
            &output_sort_key,
        )
        .unwrap();

        // Check the plan
        let plan = format!("{}", displayable(sort_plan.as_ref()).indent());
        // Plan should have 2 SortExec, one for each chunk, because they are not yet sorted
        assert!(plan.contains("SortExec"));
        // Plan should include the final SortPreservingMergeExec to merge 2 sorted inputs
        assert!(plan.contains("SortPreservingMergeExec"));
        // Should have DeduplicateExec
        assert!(plan.contains("DeduplicateExec"));

        let batch = test_collect(sort_plan).await;
        // data is sorted on primary key(tag1, tag2, time)
        let expected = vec![
            "+-----------+------+------+--------------------------------+",
            "| field_int | tag1 | tag2 | time                           |",
            "+-----------+------+------+--------------------------------+",
            "| 100       | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 5         | MT   | AL   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
            "+-----------+------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);

        // -------------------------------------------------------------
        // Build plan for sorted chunks
        // Sorted Chunk
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_i64_field_column("field_int")
                .with_five_rows_of_data()
                .with_sort_key(output_sort_key.clone()),
        ) as Arc<dyn QueryChunk>;

        // Chunk 2 exactly the same with Chunk 1
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_time_column()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_i64_field_column("field_int")
                .with_five_rows_of_data()
                .with_sort_key(output_sort_key.clone()),
        ) as Arc<dyn QueryChunk>;

        let sort_plan = Deduplicater::build_deduplicate_plan_for_overlapped_chunks(
            IOxSessionContext::default(),
            Arc::from("t"),
            schema,
            vec![chunk1, chunk2],
            Predicate::default(),
            &output_sort_key,
        )
        .unwrap();

        // Check the plan
        let plan = format!("{}", displayable(sort_plan.as_ref()).indent());
        // Plan should NOT have SortExec becasue both chunks are sorted
        assert!(!plan.contains("SortExec"));
        // Plan should include the final SortPreservingMergeExec to merge 2 sorted inputs
        assert!(plan.contains("SortPreservingMergeExec"));
        // Should have DeduplicateExec
        assert!(plan.contains("DeduplicateExec"));
    }

    #[tokio::test]
    async fn deduplicate_plan_for_overlapped_chunks_subset() {
        test_helpers::maybe_start_logging();

        // Same two chunks but only select the field and timestamp, not the tag values
        // Chunk 1 with 5 rows of data on 2 tags
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        // Chunk 2 exactly the same with Chunk 1
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_time_column()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;
        let chunks = vec![chunk1, chunk2];

        // data in its original form
        let expected = vec![
            "+-----------+------+------+--------------------------------+",
            "| field_int | tag1 | tag2 | time                           |",
            "+-----------+------+------+--------------------------------+",
            "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | AL   | 1970-01-01T00:00:00.000005Z    |",
            "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | AL   | 1970-01-01T00:00:00.000005Z    |",
            "+-----------+------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        // request just the field and timestamp
        let schema = SchemaBuilder::new()
            .field("field_int", DataType::Int64)
            .timestamp()
            .build()
            .unwrap();

        let output_sort_key = SortKey::from_columns(vec!["tag1", "tag2", "time"]);
        let sort_plan = Deduplicater::build_deduplicate_plan_for_overlapped_chunks(
            IOxSessionContext::default(), //TODO(edd): address this.
            Arc::from("t"),
            Arc::new(schema),
            chunks,
            Predicate::default(),
            &output_sort_key,
        )
        .unwrap();
        let batch = test_collect(sort_plan).await;
        // expect only 5 values, with "f1" and "timestamp" (even though input has 10)
        let expected = vec![
            "+-----------+--------------------------------+",
            "| field_int | time                           |",
            "+-----------+--------------------------------+",
            "| 100       | 1970-01-01T00:00:00.000000050Z |",
            "| 70        | 1970-01-01T00:00:00.000000100Z |",
            "| 5         | 1970-01-01T00:00:00.000005Z    |",
            "| 10        | 1970-01-01T00:00:00.000007Z    |",
            "| 1000      | 1970-01-01T00:00:00.000001Z    |",
            "+-----------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn deduplicate_plan_for_overlapped_chunks_subset_different_fields() {
        test_helpers::maybe_start_logging();

        // Chunks with different fields / tags, and select a subset
        // Chunk 1 with 5 rows of data on 2 tags
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        // Chunk 2 same tags, but different fields
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_time_column()
                .with_tag_column("tag1")
                .with_i64_field_column("other_field_int")
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        // Chunk 3 exactly the same with Chunk 2
        let chunk3 = Arc::new(
            TestChunk::new("t")
                .with_id(3)
                .with_time_column()
                .with_tag_column("tag1")
                .with_i64_field_column("other_field_int")
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        let chunks = vec![chunk1, chunk2, chunk3];
        // data in its original form
        let expected = vec![
            "+-----------+------+--------------------------------+--------------------------------+",
            "| field_int | tag1 | tag2                           | time                           |",
            "+-----------+------+--------------------------------+--------------------------------+",
            "| 1000      | MT   | CT                             | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | AL                             | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | CT                             | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | MA                             | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | AL                             | 1970-01-01T00:00:00.000005Z    |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |                                |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |                                |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |                                |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |                                |",
            "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |                                |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |                                |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |                                |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |                                |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |                                |",
            "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |                                |",
            "+-----------+------+--------------------------------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        // request just the fields
        let schema = SchemaBuilder::new()
            .field("field_int", DataType::Int64)
            .field("other_field_int", DataType::Int64)
            .build()
            .unwrap();

        let output_sort_key = SortKey::from_columns(vec!["tag2", "tag1", "time"]);
        let sort_plan = Deduplicater::build_deduplicate_plan_for_overlapped_chunks(
            IOxSessionContext::default(), //TODO(edd): address this.
            Arc::from("t"),
            Arc::new(schema),
            chunks,
            Predicate::default(),
            &output_sort_key,
        )
        .unwrap();
        let batch = test_collect(sort_plan).await;

        let expected = vec![
            "+-----------+-----------------+",
            "| field_int | other_field_int |",
            "+-----------+-----------------+",
            "|           | 100             |",
            "|           | 70              |",
            "|           | 1000            |",
            "|           | 5               |",
            "|           | 10              |",
            "| 5         |                 |",
            "| 10        |                 |",
            "| 70        |                 |",
            "| 1000      |                 |",
            "| 100       |                 |",
            "+-----------+-----------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn deduplicate_plan_for_overlapped_chunks_with_different_schemas() {
        test_helpers::maybe_start_logging();

        // Chunk 1 with 5 rows of data on 2 tags
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        // Chunk 2 has two different tags
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_time_column()
                .with_tag_column("tag3")
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        // Chunk 3 has just tag3
        let chunk3 = Arc::new(
            TestChunk::new("t")
                .with_id(3)
                .with_time_column()
                .with_tag_column("tag3")
                .with_i64_field_column("field_int")
                .with_i64_field_column("field_int2")
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        // With provided stats, the computed key will be (tag2, tag1, tag3, time)
        // Requested output schema == the schema for all three
        let schema = SchemaMerger::new()
            .merge(chunk1.schema().as_ref())
            .unwrap()
            .merge(chunk2.schema().as_ref())
            .unwrap()
            .merge(chunk3.schema().as_ref())
            .unwrap()
            .build();

        let chunks = vec![chunk1, chunk2, chunk3];
        // data in its original form
        let expected = vec![
            "+-----------+------+------+--------------------------------+",
            "| field_int | tag1 | tag2 | time                           |",
            "+-----------+------+------+--------------------------------+",
            "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | AL   | 1970-01-01T00:00:00.000005Z    |",
            "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | AL   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | MT   | 1970-01-01T00:00:00.000005Z    |",
            "| 1000      | 1000 | CT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | 10   | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | 70   | AL   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | 100  | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | 5    | MT   | 1970-01-01T00:00:00.000005Z    |",
            "+-----------+------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        let output_sort_key = SortKey::from_columns(vec!["tag2", "tag1", "time"]);
        let sort_plan = Deduplicater::build_deduplicate_plan_for_overlapped_chunks(
            IOxSessionContext::default(), //TODO(edd): address this.
            Arc::from("t"),
            Arc::new(schema),
            chunks,
            Predicate::default(),
            &output_sort_key,
        )
        .unwrap();
        let batch = test_collect(sort_plan).await;
        // with provided stats, data is sorted on (tag2, tag1, tag3, time)
        let expected = vec![
            "+-----------+------------+------+------+------+--------------------------------+",
            "| field_int | field_int2 | tag1 | tag2 | tag3 | time                           |",
            "+-----------+------------+------+------+------+--------------------------------+",
            "| 100       | 100        |      |      | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        | 70         |      |      | AL   | 1970-01-01T00:00:00.000000100Z |",
            "| 1000      | 1000       |      |      | CT   | 1970-01-01T00:00:00.000001Z    |",
            "| 5         | 5          |      |      | MT   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        | 10         |      |      | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 100       |            | AL   |      | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        |            | CT   |      | AL   | 1970-01-01T00:00:00.000000100Z |",
            "| 1000      |            | MT   |      | CT   | 1970-01-01T00:00:00.000001Z    |",
            "| 5         |            | MT   |      | MT   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        |            | MT   |      | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 5         |            | MT   | AL   |      | 1970-01-01T00:00:00.000005Z    |",
            "| 10        |            | MT   | AL   |      | 1970-01-01T00:00:00.000007Z    |",
            "| 70        |            | CT   | CT   |      | 1970-01-01T00:00:00.000000100Z |",
            "| 1000      |            | MT   | CT   |      | 1970-01-01T00:00:00.000001Z    |",
            "| 100       |            | AL   | MA   |      | 1970-01-01T00:00:00.000000050Z |",
            "+-----------+------------+------+------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn scan_plan_with_one_chunk_no_duplicates() {
        test_helpers::maybe_start_logging();

        // Test no duplicate at all
        let chunk = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_full_stats(
                    Some(5),
                    Some(7000),
                    5,
                    Some(NonZeroU64::new(5).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("AL"),
                    Some("MT"),
                    5,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        // Datafusion schema of the chunk
        let schema = chunk.schema();
        let chunks = vec![chunk];

        // data in its original form
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &raw_data(&chunks).await);

        let mut deduplicator = Deduplicater::new();
        let plan = deduplicator
            .build_scan_plan(Arc::from("t"), schema, chunks, Predicate::default(), None)
            .unwrap();
        let batch = test_collect(plan).await;
        // No duplicates so no sort at all. The data will stay in their original order
        assert_batches_sorted_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn scan_plan_with_one_chunk_with_duplicates() {
        test_helpers::maybe_start_logging();

        // Test one chunk with duplicate within
        let chunk = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_full_stats(
                    Some(5),
                    Some(7000),
                    10,
                    Some(NonZeroU64::new(7).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("AL"),
                    Some("MT"),
                    10,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_may_contain_pk_duplicates(true)
                .with_ten_rows_of_data_some_duplicates(),
        ) as Arc<dyn QueryChunk>;

        // Datafusion schema of the chunk
        let schema = chunk.schema();
        let chunks = vec![chunk];

        // data in its original form
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
            "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        let mut deduplicator = Deduplicater::new();
        let plan = deduplicator
            .build_scan_plan(Arc::from("t"), schema, chunks, Predicate::default(), None)
            .unwrap();
        let batch = test_collect(plan).await;
        // Data must be sorted on (tag1, time) and duplicates removed
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
            "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn scan_plan_with_one_chunk_with_duplicates_subset() {
        test_helpers::maybe_start_logging();

        // Test one chunk with duplicate within
        let chunk = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_full_stats(
                    Some(5),
                    Some(7000),
                    10,
                    Some(NonZeroU64::new(7).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("AL"),
                    Some("MT"),
                    10,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_may_contain_pk_duplicates(true)
                .with_ten_rows_of_data_some_duplicates(),
        ) as Arc<dyn QueryChunk>;

        let chunks = vec![chunk];
        // data in its original form
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
            "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        // request just the field and timestamp
        let schema = SchemaBuilder::new()
            .field("field_int", DataType::Int64)
            .timestamp()
            .build()
            .unwrap();

        let mut deduplicator = Deduplicater::new();
        let plan = deduplicator
            .build_scan_plan(
                Arc::from("t"),
                Arc::new(schema),
                chunks,
                Predicate::default(),
                None,
            )
            .unwrap();
        let batch = test_collect(plan).await;

        // expect just the 7 rows of de-duplicated data
        let expected = vec![
            "+-----------+--------------------------------+",
            "| field_int | time                           |",
            "+-----------+--------------------------------+",
            "| 10        | 1970-01-01T00:00:00.000000050Z |",
            "| 70        | 1970-01-01T00:00:00.000000100Z |",
            "| 70        | 1970-01-01T00:00:00.000000500Z |",
            "| 30        | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | 1970-01-01T00:00:00.000001Z    |",
            "| 1000      | 1970-01-01T00:00:00.000002Z    |",
            "| 20        | 1970-01-01T00:00:00.000007Z    |",
            "+-----------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn scan_plan_with_two_overlapped_chunks_with_duplicates() {
        test_helpers::maybe_start_logging();

        // test overlapped chunks
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_full_stats(
                    Some(5),
                    Some(7000),
                    10,
                    Some(NonZeroU64::new(7).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("AL"),
                    Some("MT"),
                    10,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_ten_rows_of_data_some_duplicates(),
        ) as Arc<dyn QueryChunk>;

        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_full_stats(
                    Some(5),
                    Some(7000),
                    5,
                    Some(NonZeroU64::new(5).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("AL"),
                    Some("MT"),
                    5,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        // Datafusion schema of the chunk
        let schema = chunk1.schema();
        let chunks = vec![chunk1, chunk2];

        // data in its original form
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
            "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        let mut deduplicator = Deduplicater::new();
        let plan = deduplicator
            .build_scan_plan(Arc::from("t"), schema, chunks, Predicate::default(), None)
            .unwrap();
        let batch = test_collect(plan).await;
        // Two overlapped chunks will be sort merged on (tag1, time) with duplicates removed
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
            "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn non_sorted_scan_plan_with_four_chunks() {
        test_helpers::maybe_start_logging();

        // This test covers all kind of chunks: overlap, non-overlap without duplicates within, non-overlap with duplicates within
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column_with_full_stats(
                    Some(5),
                    Some(7000),
                    10,
                    Some(NonZeroU64::new(7).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("AL"),
                    Some("MT"),
                    10,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_partition_id(1) // signal the chunk in partition 1 while grouping overlaps
                .with_ten_rows_of_data_some_duplicates(),
        ) as Arc<dyn QueryChunk>;

        // chunk2 overlaps with chunk 1
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_time_column_with_full_stats(
                    Some(5),
                    Some(7000),
                    5,
                    Some(NonZeroU64::new(5).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("AL"),
                    Some("MT"),
                    5,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_partition_id(1) // signal the chunk in partition 1 while grouping overlaps
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        // chunk3 no overlap, no duplicates within
        let chunk3 = Arc::new(
            TestChunk::new("t")
                .with_id(3)
                .with_time_column_with_full_stats(
                    Some(8000),
                    Some(20000),
                    3,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("UT"),
                    Some("WA"),
                    3,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_partition_id(1) // signal the chunk in partition 1 while grouping overlaps
                .with_three_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        // chunk4 no overlap, duplicates within
        let chunk4 = Arc::new(
            TestChunk::new("t")
                .with_id(4)
                .with_time_column_with_full_stats(
                    Some(28000),
                    Some(220000),
                    4,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("UT"),
                    Some("WA"),
                    4,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_may_contain_pk_duplicates(true)
                .with_partition_id(1) // signal the chunk in partition 1 while grouping overlaps
                .with_four_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        // Datafusion schema of the chunk
        let schema = chunk1.schema();
        let chunks = vec![chunk1, chunk2, chunk3, chunk4];

        // data in its original form
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
            "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
            "| 1000      | WA   | 1970-01-01T00:00:00.000008Z    |",
            "| 10        | VT   | 1970-01-01T00:00:00.000010Z    |",
            "| 70        | UT   | 1970-01-01T00:00:00.000020Z    |",
            "| 1000      | WA   | 1970-01-01T00:00:00.000028Z    |",
            "| 10        | VT   | 1970-01-01T00:00:00.000210Z    |",
            "| 70        | UT   | 1970-01-01T00:00:00.000220Z    |",
            "| 50        | VT   | 1970-01-01T00:00:00.000210Z    |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        // Create scan plan whose output data is only partially sorted
        let mut deduplicator = Deduplicater::new();
        let plan = deduplicator
            .build_scan_plan(Arc::from("t"), schema, chunks, Predicate::default(), None)
            .unwrap();

        // plan should include SortExec because chunks are not yet sorted
        let plan_str = format!("{}", displayable(plan.as_ref()).indent());
        // println!("{}", plan_str);
        // The plan should look like this
        // UnionExec
        //     DeduplicateExec: [tag1@1 ASC,time@2 ASC]
        //         SortPreservingMergeExec: [tag1@1 ASC,time@2 ASC]
        //             UnionExec
        //                 SortExec: [tag1@1 ASC,time@2 ASC]                   <-- needed for deduplication
        //                     IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //                 SortExec: [tag1@1 ASC,time@2 ASC]                   <-- needed for deduplication
        //                     IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //     DeduplicateExec: [tag1@1 ASC,time@2 ASC]
        //         SortExec: [tag1@1 ASC,time@2 ASC]                           <-- needed for deduplication
        //             IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //     IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate   <-- no sort above this because it
        //                                                                         does not need to get deduplicated
        //                                                                         and the plan output is not sorted

        //  Verify 3 SortExec
        let count = plan_str.matches("SortExec").count();
        assert_eq!(count, 3);

        // Verify 4 IOxReadFilterNode
        let count = plan_str.matches("IOxReadFilterNode").count();
        assert_eq!(count, 4);

        // Verify 2 DeduplicateExec
        let count = plan_str.matches("DeduplicateExec").count();
        assert_eq!(count, 2);

        // Verify 2 UnionExec
        let count = plan_str.matches("UnionExec").count();
        assert_eq!(count, 2);

        // Verify 1 SortPreservingMergeExec
        let count = plan_str.matches("SortPreservingMergeExec").count();
        assert_eq!(count, 1);

        let batch = test_collect(plan).await;
        // Final data is partially sorted with duplicates removed. Detailed:
        //   . chunk1 and chunk2 will be sorted merged and deduplicated (rows 7-14)
        //   . chunk3 will stay in its original (rows 1-3)
        //   . chunk4 will be sorted and deduplicated (rows 4-6)
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 1000      | WA   | 1970-01-01T00:00:00.000008Z    |",
            "| 10        | VT   | 1970-01-01T00:00:00.000010Z    |",
            "| 70        | UT   | 1970-01-01T00:00:00.000020Z    |",
            "| 70        | UT   | 1970-01-01T00:00:00.000220Z    |",
            "| 50        | VT   | 1970-01-01T00:00:00.000210Z    |",
            "| 1000      | WA   | 1970-01-01T00:00:00.000028Z    |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
            "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "+-----------+------+--------------------------------+",
        ];
        // Since output is partially sorted, allow order to vary and
        // test to still pass
        assert_batches_sorted_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn sorted_scan_plan_with_four_chunks() {
        test_helpers::maybe_start_logging();

        // This test covers all kind of chunks: overlap, non-overlap without duplicates within, non-overlap with duplicates within
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column_with_full_stats(
                    Some(5),
                    Some(7000),
                    10,
                    Some(NonZeroU64::new(7).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("AL"),
                    Some("MT"),
                    10,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_partition_id(1) // signal the chunk in partition 1 while grouping overlaps
                .with_ten_rows_of_data_some_duplicates(),
        ) as Arc<dyn QueryChunk>;

        // chunk2 overlaps with chunk 1
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_time_column_with_full_stats(
                    Some(5),
                    Some(7000),
                    5,
                    Some(NonZeroU64::new(5).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("AL"),
                    Some("MT"),
                    5,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_partition_id(1) // signal the chunk in partition 1 while grouping overlaps
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        // chunk3 no overlap, no duplicates within
        let chunk3 = Arc::new(
            TestChunk::new("t")
                .with_id(3)
                .with_time_column_with_full_stats(
                    Some(8000),
                    Some(20000),
                    3,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("UT"),
                    Some("WA"),
                    3,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_partition_id(1) // signal the chunk in partition 1 while grouping overlaps
                .with_three_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        // chunk3 no overlap, duplicates within
        let chunk4 = Arc::new(
            TestChunk::new("t")
                .with_id(4)
                .with_time_column_with_full_stats(
                    Some(28000),
                    Some(220000),
                    4,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("UT"),
                    Some("WA"),
                    4,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_may_contain_pk_duplicates(true)
                .with_partition_id(1) // signal the chunk in partition 1 while grouping overlaps
                .with_four_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        // Datafusion schema of the chunk
        let schema = chunk1.schema();
        let chunks = vec![chunk1, chunk2, chunk3, chunk4];

        // data in its original form
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
            "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
            "| 1000      | WA   | 1970-01-01T00:00:00.000008Z    |",
            "| 10        | VT   | 1970-01-01T00:00:00.000010Z    |",
            "| 70        | UT   | 1970-01-01T00:00:00.000020Z    |",
            "| 1000      | WA   | 1970-01-01T00:00:00.000028Z    |",
            "| 10        | VT   | 1970-01-01T00:00:00.000210Z    |",
            "| 70        | UT   | 1970-01-01T00:00:00.000220Z    |",
            "| 50        | VT   | 1970-01-01T00:00:00.000210Z    |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        let sort_key = compute_sort_key_for_chunks(&schema, &chunks);
        let mut deduplicator = Deduplicater::new();
        let plan = deduplicator
            .build_scan_plan(
                Arc::from("t"),
                schema,
                chunks,
                Predicate::default(),
                Some(sort_key.clone()), // Ask to sort the plan output
            )
            .unwrap();

        // plan should include SortExec because chunks are not yet sorted on the specified sort_key
        let plan_str = format!("{}", displayable(plan.as_ref()).indent());
        // println!("{}", plan_str);
        // The plan should look like this
        // SortPreservingMergeExec: [tag1@1 ASC,time@2 ASC]
        //     UnionExec
        //         DeduplicateExec: [tag1@1 ASC,time@2 ASC]
        //             SortPreservingMergeExec: [tag1@1 ASC,time@2 ASC]
        //                 UnionExec
        //                     SortExec: [tag1@1 ASC,time@2 ASC]  <-- needed for deduplication
        //                         IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //                     SortExec: [tag1@1 ASC,time@2 ASC]  <-- needed for deduplication
        //                         IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //         DeduplicateExec: [tag1@1 ASC,time@2 ASC]
        //             SortExec: [tag1@1 ASC,time@2 ASC]         <-- needed for deduplication
        //                 IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //         SortExec: [tag1@1 ASC,time@2 ASC]              <-- needed because the plan output is asked to get sorted
        //            IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate

        //  Verify 4 SortExec
        let count = plan_str.matches("SortExec").count();
        assert_eq!(count, 4);

        // Verify 4 IOxReadFilterNode
        let count = plan_str.matches("IOxReadFilterNode").count();
        assert_eq!(count, 4);

        // Verify 2 DeduplicateExec
        let count = plan_str.matches("DeduplicateExec").count();
        assert_eq!(count, 2);

        // Verify 2 UnionExec
        let count = plan_str.matches("UnionExec").count();
        assert_eq!(count, 2);

        // Verify 2 SortPreservingMergeExec
        let count = plan_str.matches("SortPreservingMergeExec").count();
        assert_eq!(count, 2);

        let batch = test_collect(plan).await;
        // Final data must be sorted
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
            "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | UT   | 1970-01-01T00:00:00.000020Z    |",
            "| 70        | UT   | 1970-01-01T00:00:00.000220Z    |",
            "| 10        | VT   | 1970-01-01T00:00:00.000010Z    |",
            "| 50        | VT   | 1970-01-01T00:00:00.000210Z    |",
            "| 1000      | WA   | 1970-01-01T00:00:00.000008Z    |",
            "| 1000      | WA   | 1970-01-01T00:00:00.000028Z    |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn alread_sorted_scan_plan_with_four_chunks_explain() {
        test_helpers::maybe_start_logging();

        // This test covers all kind of SORTED chunks: overlap, non-overlap without duplicates within, non-overlap with duplicates within

        let sort_key = SortKey::from_columns(vec!["tag1", TIME_COLUMN_NAME]);

        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column()
                .with_timestamp_min_max(5, 7000)
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_partition_id(1) // signal the chunk in partition 1 while grouping overlaps
                .with_sort_key(sort_key.clone()), // signal the chunk is sorted
        ) as Arc<dyn QueryChunk>;

        // chunk2 overlaps with chunk 1
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_time_column()
                .with_timestamp_min_max(5, 7000)
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_partition_id(1) // signal the chunk in partition 1 while grouping overlaps
                .with_sort_key(sort_key.clone()), // signal the chunk is sorted
        ) as Arc<dyn QueryChunk>;

        // chunk3 no overlap, no duplicates within
        let chunk3 = Arc::new(
            TestChunk::new("t")
                .with_id(3)
                .with_time_column()
                .with_timestamp_min_max(8000, 20000)
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_partition_id(1) // signal the chunk in partition 1 while grouping overlaps
                .with_sort_key(sort_key.clone()), // signal the chunk is sorted
        ) as Arc<dyn QueryChunk>;

        // chunk3 no overlap, duplicates within
        let chunk4 = Arc::new(
            TestChunk::new("t")
                .with_id(4)
                .with_time_column()
                .with_timestamp_min_max(28000, 220000)
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_partition_id(1) // signal the chunk in partition 1 while grouping overlaps
                .with_may_contain_pk_duplicates(true) // signal having duplicates within this chunk
                .with_sort_key(sort_key.clone()), // signal the chunk is sorted
        ) as Arc<dyn QueryChunk>;

        let schema = chunk1.schema();
        let chunks = vec![chunk1, chunk2, chunk3, chunk4];
        let mut deduplicator = Deduplicater::new();
        let plan = deduplicator
            .build_scan_plan(
                Arc::from("t"),
                schema,
                chunks,
                Predicate::default(),
                Some(sort_key),
            )
            .unwrap();

        // The plan should look like this. No SortExec at all because
        // all chunks are already sorted on the same requested sort key
        //
        // SortPreservingMergeExec: [tag1@1 ASC,time@2 ASC]
        //     UnionExec
        //         DeduplicateExec: [tag1@1 ASC,time@2 ASC]
        //             SortPreservingMergeExec: [tag1@1 ASC,time@2 ASC]
        //                 UnionExec
        //                     IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //                     IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //         DeduplicateExec: [tag1@1 ASC,time@2 ASC]
        //             IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //         IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        let plan_str = format!("{}", displayable(plan.as_ref()).indent());
        //println!("{}", plan_str);

        //  Verify no SortExec because chunks are already sorted on the specified sort_key
        assert!(!plan_str.contains("SortExec"));

        // Verify 4 IOxReadFilterNode
        let count = plan_str.matches("IOxReadFilterNode").count();
        assert_eq!(count, 4);

        // Verify 2 DeduplicateExec
        let count = plan_str.matches("DeduplicateExec").count();
        assert_eq!(count, 2);

        // Verify 2 UnionExec
        let count = plan_str.matches("UnionExec").count();
        assert_eq!(count, 2);

        // Verify 2 SortPreservingMergeExec
        let count = plan_str.matches("SortPreservingMergeExec").count();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn alread_sorted_scan_plan_with_two_partitions_explain() {
        test_helpers::maybe_start_logging();

        // ------------
        // Partition 1: covers all kind of SORTED chunks: overlap, non-overlap without duplicates within, non-overlap with duplicates within

        let sort_key = SortKey::from_columns(vec!["tag1", TIME_COLUMN_NAME]);

        let chunk1_1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column()
                .with_timestamp_min_max(5, 7000)
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_partition_id(1) // signal the chunk in partition 1 while grouping overlaps
                .with_sort_key(sort_key.clone()), // signal the chunk is sorted
        ) as Arc<dyn QueryChunk>;

        // chunk2 overlaps with chunk 1
        let chunk1_2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_time_column()
                .with_timestamp_min_max(5, 7000)
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_partition_id(1) // signal the chunk in partition 1 while grouping overlaps
                .with_sort_key(sort_key.clone()), // signal the chunk is sorted
        ) as Arc<dyn QueryChunk>;

        // chunk3 no overlap, no duplicates within
        let chunk1_3 = Arc::new(
            TestChunk::new("t")
                .with_id(3)
                .with_time_column()
                .with_timestamp_min_max(8000, 20000)
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_partition_id(1) // signal the chunk in partition 1 while grouping overlaps
                .with_sort_key(sort_key.clone()), // signal the chunk is sorted
        ) as Arc<dyn QueryChunk>;

        // chunk3 no overlap, duplicates within
        let chunk1_4 = Arc::new(
            TestChunk::new("t")
                .with_id(4)
                .with_time_column()
                .with_timestamp_min_max(28000, 220000)
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_partition_id(1) // signal the chunk in partition 1 while grouping overlaps
                .with_may_contain_pk_duplicates(true) // signal having duplicates within this chunk
                .with_sort_key(sort_key.clone()), // signal the chunk is sorted
        ) as Arc<dyn QueryChunk>;

        // ------------
        // Partition 2: Same 4 kinds of chunks on 6 chunks.
        // The non-overlap without duplicates within and non-overlap with duplicates within, each now
        // include 2 chunks for more test coverage

        let chunk2_1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column()
                .with_timestamp_min_max(5, 7000)
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_partition_id(2) // signal the chunk in partition 2
                .with_sort_key(sort_key.clone()), // signal the chunk is sorted
        ) as Arc<dyn QueryChunk>;

        // chunk2_2 overlaps with chunk 2_1
        let chunk2_2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_time_column()
                .with_timestamp_min_max(5, 7000)
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_partition_id(2) // signal the chunk in partition 2
                .with_sort_key(sort_key.clone()), // signal the chunk is sorted
        ) as Arc<dyn QueryChunk>;

        // chunk2_2 no overlap, no duplicates within
        let chunk2_3 = Arc::new(
            TestChunk::new("t")
                .with_id(3)
                .with_time_column()
                .with_timestamp_min_max(8000, 20000)
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_partition_id(2) // signal the chunk in partition 2
                .with_sort_key(sort_key.clone()), // signal the chunk is sorted
        ) as Arc<dyn QueryChunk>;

        // chunk2_4 no overlap, no duplicates within
        let chunk2_4 = Arc::new(
            TestChunk::new("t")
                .with_id(3)
                .with_time_column()
                .with_timestamp_min_max(21000, 27000)
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_partition_id(2) // signal the chunk in partition 2
                .with_sort_key(sort_key.clone()), // signal the chunk is sorted
        ) as Arc<dyn QueryChunk>;

        // chunk2_5 no overlap, duplicates within
        let chunk2_5 = Arc::new(
            TestChunk::new("t")
                .with_id(4)
                .with_time_column()
                .with_timestamp_min_max(28000, 30000)
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_partition_id(2) // signal the chunk in partition 2 while grouping overlaps
                .with_may_contain_pk_duplicates(true) // signal having duplicates within this chunk
                .with_sort_key(sort_key.clone()), // signal the chunk is sorted
        ) as Arc<dyn QueryChunk>;

        // chunk2_6 no overlap, duplicates within
        let chunk2_6 = Arc::new(
            TestChunk::new("t")
                .with_id(4)
                .with_time_column()
                .with_timestamp_min_max(38000, 40000)
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_partition_id(2) // signal the chunk in partition 2 while grouping overlaps
                .with_may_contain_pk_duplicates(true) // signal having duplicates within this chunk
                .with_sort_key(sort_key.clone()), // signal the chunk is sorted
        ) as Arc<dyn QueryChunk>;

        let schema = chunk1_1.schema();
        let chunks = vec![
            chunk1_1, chunk1_2, chunk1_3, chunk1_4, chunk2_1, chunk2_2, chunk2_3, chunk2_4,
            chunk2_5, chunk2_6,
        ];
        let mut deduplicator = Deduplicater::new();
        let plan = deduplicator
            .build_scan_plan(
                Arc::from("t"),
                schema,
                chunks,
                Predicate::default(),
                Some(sort_key),
            )
            .unwrap();

        // The plan should look like this. No SortExec at all because
        // all chunks are already sorted on the same requested sort key
        //
        // SortPreservingMergeExec: [tag1@1 ASC,time@2 ASC]
        //     UnionExec
        //     DeduplicateExec: [tag1@1 ASC,time@2 ASC]
        //         SortPreservingMergeExec: [tag1@1 ASC,time@2 ASC]
        //             UnionExec
        //                 IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //                 IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //     DeduplicateExec: [tag1@1 ASC,time@2 ASC]
        //         SortPreservingMergeExec: [tag1@1 ASC,time@2 ASC]
        //             UnionExec
        //                 IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //                 IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //     DeduplicateExec: [tag1@1 ASC,time@2 ASC]
        //         IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //     DeduplicateExec: [tag1@1 ASC,time@2 ASC]
        //         IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //     DeduplicateExec: [tag1@1 ASC,time@2 ASC]
        //         IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //     IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //     IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        //     IOxReadFilterNode: table_name=t, chunks=1 predicate=Predicate
        let plan_str = format!("{}", displayable(plan.as_ref()).indent());
        println!("{}", plan_str);

        //  Verify no SortExec because chunks are already sorted on the specified sort_key
        assert!(!plan_str.contains("SortExec"));

        // Verify 10 IOxReadFilterNode
        let count = plan_str.matches("IOxReadFilterNode").count();
        assert_eq!(count, 10);

        // Verify 5 DeduplicateExec
        let count = plan_str.matches("DeduplicateExec").count();
        assert_eq!(count, 5);

        // Verify 3 UnionExec
        let count = plan_str.matches("UnionExec").count();
        assert_eq!(count, 3);

        // Verify 3 SortPreservingMergeExec
        let count = plan_str.matches("SortPreservingMergeExec").count();
        assert_eq!(count, 3);
    }

    fn chunk_ids(group: &[Arc<dyn QueryChunk>]) -> String {
        let ids = group
            .iter()
            .map(|c| c.id().get().to_string())
            .collect::<Vec<_>>();
        ids.join(", ")
    }

    fn chunk_group_ids(groups: &[Vec<Arc<dyn QueryChunk>>]) -> Vec<String> {
        groups
            .iter()
            .enumerate()
            .map(|(idx, group)| format!("Group {}: {}", idx, chunk_ids(group)))
            .collect()
    }
}
