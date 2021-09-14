//! Implementation of a DataFusion `TableProvider` in terms of `QueryChunk`s

use std::sync::Arc;

use arrow::{datatypes::SchemaRef as ArrowSchemaRef, error::ArrowError};
use datafusion::{
    datasource::{datasource::TableProviderFilterPushDown, TableProvider},
    error::{DataFusionError, Result as DataFusionResult},
    logical_plan::Expr,
    physical_plan::{
        expressions::{col as physical_col, PhysicalSortExpr},
        projection::ProjectionExec,
        sort::SortExec,
        sort_preserving_merge::SortPreservingMergeExec,
        union::UnionExec,
        ExecutionPlan,
    },
};
use internal_types::schema::{merge::SchemaMerger, sort::SortKey, Schema};
use observability_deps::tracing::{debug, info, trace};
use predicate::predicate::{Predicate, PredicateBuilder};

use crate::{compute_sort_key, util::arrow_sort_key_exprs, QueryChunk};

use snafu::{ResultExt, Snafu};

mod adapter;
mod deduplicate;
mod overlap;
mod physical;
use self::overlap::group_potential_duplicates;
pub(crate) use deduplicate::DeduplicateExec;
pub(crate) use physical::IOxReadFilterNode;

// TODO(edd): temp experiment - should wire in `_batch_size` in the
// table provider.
pub const BATCH_SIZE: usize = 1025 * 25;

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
pub trait ChunkPruner<C: QueryChunk>: Sync + Send + std::fmt::Debug {
    /// prune `chunks`, if possible, based on predicate.
    fn prune_chunks(&self, chunks: Vec<Arc<C>>, predicate: &Predicate) -> Vec<Arc<C>>;
}

/// Builds a `ChunkTableProvider` from a series of `QueryChunk`s
/// and ensures the schema across the chunks is compatible and
/// consistent.
#[derive(Debug)]
pub struct ProviderBuilder<C: QueryChunk + 'static> {
    table_name: Arc<str>,
    schema: Arc<Schema>,
    chunk_pruner: Option<Arc<dyn ChunkPruner<C>>>,
    chunks: Vec<Arc<C>>,
    /// ensure the output is sorted on the pk columns (in an optimal order computed based on their cardinality)
    ensure_pk_sort: bool,
}

impl<C: QueryChunk> ProviderBuilder<C> {
    pub fn new(table_name: impl AsRef<str>, schema: Arc<Schema>) -> Self {
        Self {
            table_name: Arc::from(table_name.as_ref()),
            schema,
            chunk_pruner: None,
            chunks: Vec::new(),
            ensure_pk_sort: false, // never sort the output unless explicitly specified
        }
    }

    /// Requests the output of the scan sorted
    pub fn ensure_pk_sort(&mut self) {
        self.ensure_pk_sort = true;
    }

    /// Add a new chunk to this provider
    pub fn add_chunk(mut self, chunk: Arc<C>) -> Self {
        self.chunks.push(chunk);
        self
    }

    /// Specify a `ChunkPruner` for the provider that will apply
    /// additional chunk level pruning based on pushed down predicates
    pub fn add_pruner(mut self, chunk_pruner: Arc<dyn ChunkPruner<C>>) -> Self {
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
    pub fn build(self) -> Result<ChunkTableProvider<C>> {
        let chunk_pruner = match self.chunk_pruner {
            Some(chunk_pruner) => chunk_pruner,
            None => {
                return InternalNoChunkPruner {
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
            ensure_pk_sort: self.ensure_pk_sort,
        })
    }
}

/// Implementation of a DataFusion TableProvider in terms of QueryChunks
///
/// This allows DataFusion to see data from Chunks as a single table, as well as
/// push predicates and selections down to chunks
#[derive(Debug)]
pub struct ChunkTableProvider<C: QueryChunk + 'static> {
    table_name: Arc<str>,
    /// The IOx schema (wrapper around Arrow Schemaref) for this table
    iox_schema: Arc<Schema>,
    /// Something that can prune chunks
    chunk_pruner: Arc<dyn ChunkPruner<C>>,
    // The chunks
    chunks: Vec<Arc<C>>,
    /// ensure the output is sorted on the pk columns (in an optimal order computed based on their cardinality)
    ensure_pk_sort: bool,
}

impl<C: QueryChunk + 'static> ChunkTableProvider<C> {
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

    /// Requests the output of the scan sorted
    pub fn ensure_pk_sort(&mut self) {
        self.ensure_pk_sort = true;
    }
}

impl<C: QueryChunk + 'static> TableProvider for ChunkTableProvider<C> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// Schema with all available columns across all chunks
    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Note that `filters` don't actually need to be evaluated in
        // the scan for the plans to be correct, they are an extra
        // optimization for providers which can offer them
        let predicate = PredicateBuilder::default()
            .add_pushdown_exprs(filters)
            .build();

        // Now we have a second attempt to prune out chunks based on
        // metadata using the pushed down predicate (e.g. in SQL).
        let chunks: Vec<Arc<C>> = self.chunks.to_vec();
        let num_initial_chunks = chunks.len();
        let chunks = self.chunk_pruner.prune_chunks(chunks, &predicate);
        debug!(%predicate, num_initial_chunks, num_final_chunks=chunks.len(), "pruned with pushed down predicates");

        // Figure out the schema of the requested output
        let scan_schema = match projection {
            Some(indicies) => Arc::new(self.iox_schema.select_by_indices(indicies)),
            None => Arc::clone(&self.iox_schema),
        };

        // This debug shows the self.arrow_schema() includes all columns in all chunks
        // which means the schema of all chunks are merged before invoking this scan
        trace!("all chunks schema: {:#?}", self.arrow_schema());
        // However, the schema of each chunk is still in its original form which does not
        // include the merged columns of other chunks. The code below (put in comments on purpose) proves it
        // for chunk in chunks.clone() {
        //     trace!("Schema of chunk {}: {:#?}", chunk.id(), chunk.schema());
        // }

        let mut deduplicate = Deduplicater::new();
        let plan = deduplicate.build_scan_plan(
            Arc::clone(&self.table_name),
            scan_schema,
            chunks,
            predicate,
            self.ensure_pk_sort,
        )?;

        Ok(plan)
    }

    /// Filter pushdown specificiation
    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> DataFusionResult<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }
}

#[derive(Clone, Debug, Default)]
/// A deduplicater that deduplicate the duplicated data during scan execution
pub(crate) struct Deduplicater<C: QueryChunk + 'static> {
    // a vector of a vector of overlapped chunks
    pub overlapped_chunks_set: Vec<Vec<Arc<C>>>,

    // a vector of non-overlapped chunks each have duplicates in itself
    pub in_chunk_duplicates_chunks: Vec<Arc<C>>,

    // a vector of non-overlapped and non-duplicates chunks
    pub no_duplicates_chunks: Vec<Arc<C>>,
}

impl<C: QueryChunk + 'static> Deduplicater<C> {
    pub(crate) fn new() -> Self {
        Self {
            overlapped_chunks_set: vec![],
            in_chunk_duplicates_chunks: vec![],
            no_duplicates_chunks: vec![],
        }
    }

    /// The IOx scan process needs to deduplicate data if there are duplicates. Hence it will look
    /// like below.
    /// Depending on the parameter, sort_output, the output data of plan will be either sorted or not sorted.
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
    ///               │ DeduplicateExec │                │ DeduplicateExec │   │     SortExec    │  <-- This is added if sort_output = true
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
    ///```
    pub(crate) fn build_scan_plan(
        &mut self,
        table_name: Arc<str>,
        output_schema: Arc<Schema>,
        chunks: Vec<Arc<C>>,
        predicate: Predicate,
        sort_output: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Initialize an empty sort key
        let mut output_sort_key = SortKey::with_capacity(0);
        if sort_output {
            // Compute the output sort key which is the super key of chunks' keys base on their data cardinality
            output_sort_key = compute_sort_key(chunks.iter().map(|x| x.summary()));
        }

        // find overlapped chunks and put them into the right group
        self.split_overlapped_chunks(chunks.to_vec())?;

        // Building plans
        let mut plans: Vec<Arc<dyn ExecutionPlan>> = vec![];
        if self.no_duplicates() {
            // Neither overlaps nor duplicates, no deduplicating needed
            let mut non_duplicate_plans = Self::build_plans_for_non_duplicates_chunks(
                Arc::clone(&table_name),
                Arc::clone(&output_schema),
                chunks.to_owned(),
                predicate,
                &output_sort_key,
            )?;
            plans.append(&mut non_duplicate_plans);
        } else {
            trace!(overlapped_chunks=?self.overlapped_chunks_set.len(),
                in_chunk_duplicates=?self.in_chunk_duplicates_chunks.len(),
                no_duplicates_chunks=?self.no_duplicates_chunks.len(),
                "Chunks after classifying: ");

            // Go over overlapped set, build deduplicate plan for each vector of overlapped chunks
            for overlapped_chunks in self.overlapped_chunks_set.to_vec() {
                plans.push(Self::build_deduplicate_plan_for_overlapped_chunks(
                    Arc::clone(&table_name),
                    Arc::clone(&output_schema),
                    overlapped_chunks.to_owned(),
                    predicate.clone(),
                    &output_sort_key,
                )?);
            }

            // Go over each in_chunk_duplicates_chunks, build deduplicate plan for each
            for chunk_with_duplicates in self.in_chunk_duplicates_chunks.to_vec() {
                plans.push(Self::build_deduplicate_plan_for_chunk_with_duplicates(
                    Arc::clone(&table_name),
                    Arc::clone(&output_schema),
                    chunk_with_duplicates.to_owned(),
                    predicate.clone(),
                    &output_sort_key,
                )?);
            }

            // Go over non_duplicates_chunks, build a plan for it
            for no_duplicates_chunk in self.no_duplicates_chunks.to_vec() {
                plans.push(Self::build_plan_for_non_duplicates_chunk(
                    Arc::clone(&table_name),
                    Arc::clone(&output_schema),
                    no_duplicates_chunk.to_owned(),
                    predicate.clone(),
                    &output_sort_key,
                )?);
            }
        }

        if plans.is_empty() {
            // No plan generated. Something must go wrong
            // Even if the chunks are empty, IOxReadFilterNode is still created
            panic!("Internal error generating deduplicate plan");
        }

        let mut plan = match plans.len() {
            //One child plan, no need to add Union
            1 => plans.remove(0),
            // many child plans, add Union
            _ => Arc::new(UnionExec::new(plans)),
        };

        if sort_output {
            // Sort preserving merge the sorted plans
            // Note that even if the plan is a single plan (aka no UnionExec on top),
            // we still need to add this SortPreservingMergeExec because:
            //    1. It will provide a sorted signal(through Datafusion's Distribution::UnspecifiedDistribution)
            //    2. And it will not do anything extra if the input is one partition so won't affect performance
            let sort_exprs = arrow_sort_key_exprs(&output_sort_key, &plan.schema());
            plan = Arc::new(SortPreservingMergeExec::new(sort_exprs, plan, BATCH_SIZE));
        }

        Ok(plan)
    }

    /// discover overlaps and split them into three groups:
    ///  1. vector of vector of overlapped chunks
    ///  2. vector of non-overlapped chunks, each have duplicates in itself
    ///  3. vectors of non-overlapped chunks without duplicates
    fn split_overlapped_chunks(&mut self, chunks: Vec<Arc<C>>) -> Result<()> {
        // Find all groups based on statstics
        let groups = group_potential_duplicates(chunks).context(InternalChunkGrouping)?;

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
        table_name: Arc<str>,
        output_schema: Arc<Schema>,
        chunks: Vec<Arc<C>>, // These chunks are identified overlapped
        predicate: Predicate,
        output_sort_key: &SortKey<'_>,
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

        // Compute the output sort key for these chunks
        let sort_key = if !output_sort_key.is_empty() {
            output_sort_key.to_owned()
        } else {
            compute_sort_key(chunks.iter().map(|x| x.summary()))
        };
        trace!(sort_key=?sort_key, "sort key for the input chunks");

        trace!(
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
                    Arc::clone(&table_name),
                    Arc::clone(&input_schema),
                    Arc::clone(chunk),
                    predicate.clone(),
                    &sort_key,
                )
            })
            .collect();

        // Union the plans
        // The UnionExec operator only streams all chunks (aka partitions in Datafusion) and
        // keep them in separate chunks which exactly what we need here
        let plan = UnionExec::new(sorted_chunk_plans?);

        // Now (sort) merge the already sorted chunks
        let sort_exprs = arrow_sort_key_exprs(&sort_key, &plan.schema());

        let plan = Arc::new(SortPreservingMergeExec::new(
            sort_exprs.clone(),
            Arc::new(plan),
            BATCH_SIZE,
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
        table_name: Arc<str>,
        output_schema: Arc<Schema>,
        chunk: Arc<C>, // This chunk is identified having duplicates
        predicate: Predicate,
        output_sort_key: &SortKey<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let pk_schema = Self::compute_pk_schema(&[Arc::clone(&chunk)]);
        let input_schema = Self::compute_input_schema(&output_schema, &pk_schema);

        // Compute the output sort key for this chunk
        let mut sort_key = if !output_sort_key.is_empty() {
            output_sort_key.to_owned()
        } else {
            compute_sort_key(vec![chunk.summary()].into_iter())
        };
        trace!(sort_key=?sort_key,chunk_id=?chunk.id(), "Computed the sort key for the input chunk");

        // Create the 2 bottom nodes IOxReadFilterNode and SortExec
        let plan = Self::build_sort_plan_for_read_filter(
            table_name,
            Arc::clone(&input_schema),
            Arc::clone(&chunk),
            predicate,
            &sort_key,
        )?;

        // The sort key of this chunk might only the subset of the super sort key
        if !output_sort_key.is_empty() {
            // First get the chunk pk columns
            let schema = chunk.schema();
            let key_columns = schema.primary_key();

            // Now get the key subset of the super key that includes the chunk's pk columns
            sort_key = output_sort_key.selected_sort_key(key_columns.clone());
        }

        // Add DeduplicateExc
        // Sort exprs for the deduplication
        let sort_exprs = arrow_sort_key_exprs(&sort_key, &plan.schema());
        trace!(Sort_Exprs=?sort_exprs, chunk_ID=?chunk.id(), "Sort Expression for the deduplicate node of chunk");
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

        // If the schemas are the same, nothing to do
        if input_schema == output_schema {
            return Ok(input);
        }

        // build select exprs for the requested fields
        let select_exprs = output_schema
            .fields()
            .iter()
            .map(|f| {
                let field_name = f.name();
                let physical_expr =
                    physical_col(field_name, &input_schema).context(InternalSelectExpr)?;
                Ok((physical_expr, field_name.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;

        let plan = ProjectionExec::try_new(select_exprs, input).context(InternalProjection)?;
        Ok(Arc::new(plan))
    }

    /// Return a sort plan for for a given chunk
    /// The plan will look like this
    /// ```text
    ///                ┌─────────────────┐
    ///                │    SortExec     │
    ///                │   (optional)    │
    ///                └─────────────────┘
    ///                          ▲
    ///                          │
    ///                          │
    ///                ┌─────────────────┐
    ///                │IOxReadFilterNode│
    ///                │    (Chunk)      │
    ///                └─────────────────┘
    ///```
    fn build_sort_plan_for_read_filter(
        table_name: Arc<str>,
        output_schema: Arc<Schema>,
        chunk: Arc<C>, // This chunk is identified having duplicates
        predicate: Predicate,
        output_sort_key: &SortKey<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Create the bottom node IOxReadFilterNode for this chunk
        let input: Arc<dyn ExecutionPlan> = Arc::new(IOxReadFilterNode::new(
            Arc::clone(&table_name),
            output_schema,
            vec![Arc::clone(&chunk)],
            predicate,
        ));

        // Add the sort operator, SortExec, if needed
        if !output_sort_key.is_empty() {
            Self::build_sort_plan(chunk, input, output_sort_key)
        } else {
            Ok(input)
        }
    }

    /// Add SortExec operator on top of the input plan of the given chunk
    /// The plan will be sorted on the chunk's primary key
    fn build_sort_plan(
        chunk: Arc<C>,
        input: Arc<dyn ExecutionPlan>,
        output_sort_key: &SortKey<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // output_sort_key cannot be empty
        if output_sort_key.is_empty() {
            panic!("Super sort key is empty");
        }

        trace!(output_sort_key=?output_sort_key, "Super sort key input to build_sort_plan");

        // Check to see if the plan is sorted on the subset of the output_sort_key
        let sort_key = chunk.sort_key();
        if let Some(chunk_sort_key) = sort_key {
            if let Some(merge_key) = SortKey::try_merge_key(output_sort_key, &chunk_sort_key) {
                if merge_key == *output_sort_key {
                    // the chunk is already sorted on the subset of the o_sort_key,
                    // no need to resort it
                    trace!(ChunkID=?chunk.id(), "Chunk is sorted and no need the sort operator");
                    return Ok(input);
                }
            } else {
                // The chunk is sorted but not on different order with super sort key.
                // Log it for investigating data set to improve performance further
                info!(chunk_type=?chunk.chunk_type(),
                    chunk_ID=?chunk.id(),
                    chunk_current_sort_order=?chunk_sort_key,
                    chunk_super_sort_key=?output_sort_key,
                    "Chunk will get resorted in build_sort_plan due to new cardinality rate between key columns");
            }
        } else {
            info!(chunk_type=?chunk.chunk_type(),
                chunk_ID=?chunk.id(),
                "Chunk is not yet sorted and will get sorted in build_sort_plan");
        }

        // Build the chunk's sort key that is a subset of the output_sort_key
        //
        // First get the chunk pk columns
        let schema = chunk.schema();
        let key_columns = schema.primary_key();

        // Now get the key subset of the super key that includes the chunk's pk columns
        let chunk_sort_key = output_sort_key.selected_sort_key(key_columns.clone());

        info!(chunk_type=?chunk.chunk_type(),
            chunk_ID=?chunk.id(),
            pk_columns=?key_columns,
            sort_key=?chunk_sort_key,
             "Chunk is getting sorted");

        // Build arrow sort expression for the chunk sort key
        let input_schema = input.schema();
        let sort_exprs = arrow_sort_key_exprs(&chunk_sort_key, &input_schema);

        trace!(Sort_Exprs=?sort_exprs, Chunk_ID=?chunk.id(), "Sort Expression for the sort operator of chunk");

        // Create SortExec operator
        Ok(Arc::new(
            SortExec::try_new(sort_exprs, input).context(InternalSort)?,
        ))
    }

    /// Return the simplest IOx scan plan of a given chunk which is IOxReadFilterNode
    /// ```text
    ///                ┌─────────────────┐
    ///                │    SortExec     │
    ///                │   (optional)    │   <-- Only added if the input output_sort_key is not empty
    ///                └─────────────────┘
    ///                          ▲
    ///                          │
    ///                          │
    ///                ┌─────────────────┐
    ///                │IOxReadFilterNode│
    ///                │    (Chunk)      │
    ///                └─────────────────┘
    ///```
    fn build_plan_for_non_duplicates_chunk(
        table_name: Arc<str>,
        output_schema: Arc<Schema>,
        chunk: Arc<C>, // This chunk is identified having no duplicates
        predicate: Predicate,
        output_sort_key: &SortKey<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Self::build_sort_plan_for_read_filter(
            table_name,
            output_schema,
            chunk,
            predicate,
            output_sort_key,
        )
    }

    /// Return either:
    ///   the simplest IOx scan plan for many chunks which is IOxReadFilterNode
    ///   if the input output_sort_key is empty
    /// ```text
    ///                ┌─────────────────┐
    ///                │IOxReadFilterNode│
    ///                │ (Many Chunks)   │
    ///                └─────────────────┘
    ///```
    ///
    /// Otherwise, many plans like this
    ///
    /// ```text
    ///   ┌─────────────────┐             ┌─────────────────┐
    ///   │    SortExec     │             │    SortExec     │
    ///   │   (optional)    │             │   (optional)    │
    ///   └─────────────────┘             └─────────────────┘
    ///            ▲                               ▲
    ///            │            .....              │
    ///            │                               │
    ///   ┌─────────────────┐             ┌─────────────────┐
    ///   │IOxReadFilterNode│             │IOxReadFilterNode│
    ///   │    (Chunk 1)    │             │    (Chunk n)    │
    ///   └─────────────────┘             └─────────────────┘
    ///```
    fn build_plans_for_non_duplicates_chunks(
        table_name: Arc<str>,
        output_schema: Arc<Schema>,
        chunks: Vec<Arc<C>>, // These chunks is identified having no duplicates
        predicate: Predicate,
        output_sort_key: &SortKey<'_>,
    ) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
        let mut plans: Vec<Arc<dyn ExecutionPlan>> = vec![];

        // output is not required to be sorted or no chunks provided, only create a read filter for all chunks
        if output_sort_key.is_empty() || chunks.is_empty() {
            plans.push(Arc::new(IOxReadFilterNode::new(
                Arc::clone(&table_name),
                output_schema,
                chunks,
                predicate,
            )));

            return Ok(plans);
        }

        // Build sorted plans, one for each chunk
        let sorted_chunk_plans: Result<Vec<Arc<dyn ExecutionPlan>>> = chunks
            .iter()
            .map(|chunk| {
                Self::build_plan_for_non_duplicates_chunk(
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

    /// Find the columns needed in the primary key across schemas
    ///
    /// Note by the time we get down here, we have already checked
    /// the chunks for compatible schema, so we use unwrap (perhaps
    /// famous last words, but true at time of writing)
    fn compute_pk_schema(chunks: &[Arc<C>]) -> Arc<Schema> {
        let mut pk_schema_merger = SchemaMerger::new();
        for chunk in chunks {
            let chunk_schema = chunk.schema();
            let chunk_pk = chunk_schema.primary_key();
            let chunk_pk_schema = chunk_schema.select_by_names(&chunk_pk).unwrap();
            pk_schema_merger = pk_schema_merger.merge(&chunk_pk_schema).unwrap();
        }
        let pk_schema = pk_schema_merger.build();
        Arc::new(pk_schema)
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
impl<C: QueryChunk> ChunkPruner<C> for NoOpPruner {
    fn prune_chunks(&self, chunks: Vec<Arc<C>>, _predicate: &Predicate) -> Vec<Arc<C>> {
        chunks
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroU64;

    use arrow::datatypes::DataType;
    use arrow_util::assert_batches_eq;
    use datafusion::physical_plan::collect;
    use internal_types::schema::{builder::SchemaBuilder, TIME_COLUMN_NAME};

    use crate::{
        test::{raw_data, TestChunk},
        QueryChunkMeta,
    };

    use super::*;

    #[test]
    fn chunk_grouping() {
        // This test just ensures that all the plumbing is connected
        // for chunk grouping. The logic of the grouping is tested
        // in the duplicate module

        // c1: no overlaps
        let c1 = Arc::new(TestChunk::new("t").with_id(1).with_tag_column_with_stats(
            "tag1",
            Some("a"),
            Some("b"),
        ));

        // c2: over lap with c3
        let c2 = Arc::new(TestChunk::new("t").with_id(2).with_tag_column_with_stats(
            "tag1",
            Some("c"),
            Some("d"),
        ));

        // c3: overlap with c2
        let c3 = Arc::new(TestChunk::new("t").with_id(3).with_tag_column_with_stats(
            "tag1",
            Some("c"),
            Some("d"),
        ));

        // c4: self overlap
        let c4 = Arc::new(
            TestChunk::new("t")
                .with_id(4)
                .with_tag_column_with_stats("tag1", Some("e"), Some("f"))
                .with_may_contain_pk_duplicates(true),
        );

        let mut deduplicator = Deduplicater::new();
        deduplicator
            .split_overlapped_chunks(vec![c1, c2, c3, c4])
            .expect("split chunks");

        assert_eq!(
            chunk_group_ids(&deduplicator.overlapped_chunks_set),
            vec!["Group 0: 2, 3"]
        );
        assert_eq!(chunk_ids(&deduplicator.in_chunk_duplicates_chunks), "4");
        assert_eq!(chunk_ids(&deduplicator.no_duplicates_chunks), "1");
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
        );

        let mut sort_key = SortKey::with_capacity(2);
        sort_key.with_col("tag1");
        sort_key.with_col(TIME_COLUMN_NAME);

        // IOx scan operator
        let input: Arc<dyn ExecutionPlan> = Arc::new(IOxReadFilterNode::new(
            Arc::from("t"),
            chunk.schema(),
            vec![Arc::clone(&chunk)],
            Predicate::default(),
        ));
        let batch = collect(Arc::clone(&input)).await.unwrap();
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
        let sort_plan = Deduplicater::build_sort_plan(chunk, input, &sort_key);
        let batch = collect(sort_plan.unwrap()).await.unwrap();
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
        );

        let mut sort_key = SortKey::with_capacity(3);
        sort_key.with_col("tag1");
        sort_key.with_col("tag2");
        sort_key.with_col("tag3");
        sort_key.with_col(TIME_COLUMN_NAME);

        // IOx scan operator
        let input: Arc<dyn ExecutionPlan> = Arc::new(IOxReadFilterNode::new(
            Arc::from("t"),
            chunk.schema(),
            vec![Arc::clone(&chunk)],
            Predicate::default(),
        ));
        let batch = collect(Arc::clone(&input)).await.unwrap();
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
        let sort_plan = Deduplicater::build_sort_plan(chunk, input, &sort_key);
        let batch = collect(sort_plan.unwrap()).await.unwrap();
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
        );

        let mut sort_key = SortKey::with_capacity(3);
        sort_key.with_col("tag1");
        sort_key.with_col("tag2");
        sort_key.with_col(TIME_COLUMN_NAME);

        // Datafusion schema of the chunk
        let schema = chunk.schema();

        let sort_plan = Deduplicater::build_sort_plan_for_read_filter(
            Arc::from("t"),
            schema,
            Arc::clone(&chunk),
            Predicate::default(),
            &sort_key,
        );
        let batch = collect(sort_plan.unwrap()).await.unwrap();
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
    }

    #[tokio::test]
    async fn deduplicate_plan_for_overlapped_chunks() {
        test_helpers::maybe_start_logging();

        // Chunk 1 with 5 rows of data on 2 tags
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
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
        );

        // Chunk 2 exactly the same with Chunk 1
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
                .with_tag_column_with_full_stats(
                    "tag2",
                    Some("AL"),
                    Some("MA"),
                    5,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        );
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

        let output_sort_key = SortKey::with_capacity(0);
        let sort_plan = Deduplicater::build_deduplicate_plan_for_overlapped_chunks(
            Arc::from("t"),
            schema,
            chunks,
            Predicate::default(),
            &output_sort_key,
        );
        let batch = collect(sort_plan.unwrap()).await.unwrap();
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
    }

    #[tokio::test]
    async fn deduplicate_plan_for_overlapped_chunks_subset() {
        test_helpers::maybe_start_logging();

        // Same two chunks but only select the field and timestamp, not the tag values
        // Chunk 1 with 5 rows of data on 2 tags
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
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
        );

        // Chunk 2 exactly the same with Chunk 1
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
                .with_tag_column_with_full_stats(
                    "tag2",
                    Some("AL"),
                    Some("MA"),
                    5,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        );
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

        // With the provided stats, the computed sort key will be (tag1, tag2, time)
        let output_sort_key = SortKey::with_capacity(0);
        let sort_plan = Deduplicater::build_deduplicate_plan_for_overlapped_chunks(
            Arc::from("t"),
            Arc::new(schema),
            chunks,
            Predicate::default(),
            &output_sort_key,
        );
        let batch = collect(sort_plan.unwrap()).await.unwrap();
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
        );

        // Chunk 2 same tags, but different fields
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
                .with_i64_field_column("other_field_int")
                .with_five_rows_of_data(),
        );

        // Chunk 3 exactly the same with Chunk 2
        let chunk3 = Arc::new(
            TestChunk::new("t")
                .with_id(3)
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
                .with_i64_field_column("other_field_int")
                .with_five_rows_of_data(),
        );

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

        // With the provided stats, the computed sort key will be (tag2, tag1, time)
        let output_sort_key = SortKey::with_capacity(0);
        let sort_plan = Deduplicater::build_deduplicate_plan_for_overlapped_chunks(
            Arc::from("t"),
            Arc::new(schema),
            chunks,
            Predicate::default(),
            &output_sort_key,
        );
        let batch = collect(sort_plan.unwrap()).await.unwrap();

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
        );

        // Chunk 2 has two different tags
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
                    "tag3",
                    Some("AL"),
                    Some("MT"),
                    5,
                    Some(NonZeroU64::new(3).unwrap()),
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
        );

        // Chunk 3 has just tag3
        let chunk3 = Arc::new(
            TestChunk::new("t")
                .with_id(3)
                .with_time_column_with_full_stats(
                    Some(5),
                    Some(7000),
                    5,
                    Some(NonZeroU64::new(5).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag3",
                    Some("AL"),
                    Some("MT"),
                    5,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_i64_field_column("field_int2")
                .with_five_rows_of_data(),
        );

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

        let output_sort_key = SortKey::with_capacity(0);
        let sort_plan = Deduplicater::build_deduplicate_plan_for_overlapped_chunks(
            Arc::from("t"),
            Arc::new(schema),
            chunks,
            Predicate::default(),
            &output_sort_key,
        );
        let batch = collect(sort_plan.unwrap()).await.unwrap();
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
        );

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
            "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        let mut deduplicator = Deduplicater::new();
        let plan = deduplicator.build_scan_plan(
            Arc::from("t"),
            schema,
            chunks,
            Predicate::default(),
            false,
        );
        let batch = collect(plan.unwrap()).await.unwrap();
        // No duplicates so no sort at all. The data will stay in their original order
        assert_batches_eq!(&expected, &batch);
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
        );

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
        let plan = deduplicator.build_scan_plan(
            Arc::from("t"),
            schema,
            chunks,
            Predicate::default(),
            false,
        );
        let batch = collect(plan.unwrap()).await.unwrap();
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
        );

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
        let plan = deduplicator.build_scan_plan(
            Arc::from("t"),
            Arc::new(schema),
            chunks,
            Predicate::default(),
            false,
        );
        let batch = collect(plan.unwrap()).await.unwrap();

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
        );

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
        );

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
        let plan = deduplicator.build_scan_plan(
            Arc::from("t"),
            schema,
            chunks,
            Predicate::default(),
            false,
        );
        let batch = collect(plan.unwrap()).await.unwrap();
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
                .with_ten_rows_of_data_some_duplicates(),
        );

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
                .with_five_rows_of_data(),
        );

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
                .with_three_rows_of_data(),
        );

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
                .with_four_rows_of_data(),
        );

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
        let plan = deduplicator.build_scan_plan(
            Arc::from("t"),
            schema,
            chunks,
            Predicate::default(),
            false,
        );
        let batch = collect(plan.unwrap()).await.unwrap();
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
        assert_batches_eq!(&expected, &batch);
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
                .with_ten_rows_of_data_some_duplicates(),
        );

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
                .with_five_rows_of_data(),
        );

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
                .with_three_rows_of_data(),
        );

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
                .with_four_rows_of_data(),
        );

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

        let mut deduplicator = Deduplicater::new();
        let plan = deduplicator.build_scan_plan(
            Arc::from("t"),
            schema,
            chunks,
            Predicate::default(),
            true,
        );
        let batch = collect(plan.unwrap()).await.unwrap();
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

    fn chunk_ids(group: &[Arc<TestChunk>]) -> String {
        let ids = group.iter().map(|c| c.id().to_string()).collect::<Vec<_>>();
        ids.join(", ")
    }

    fn chunk_group_ids(groups: &[Vec<Arc<TestChunk>>]) -> Vec<String> {
        groups
            .iter()
            .enumerate()
            .map(|(idx, group)| format!("Group {}: {}", idx, chunk_ids(group)))
            .collect()
    }
}
