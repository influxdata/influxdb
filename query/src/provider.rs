//! Implementation of a DataFusion `TableProvider` in terms of `QueryChunk`s

use std::sync::Arc;

use arrow::{datatypes::SchemaRef as ArrowSchemaRef, error::ArrowError};
use datafusion::{
    datasource::{
        datasource::{Statistics, TableProviderFilterPushDown},
        TableProvider,
    },
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
use internal_types::schema::{merge::SchemaMerger, Schema};
use observability_deps::tracing::{debug, trace};

use crate::{
    predicate::{Predicate, PredicateBuilder},
    util::arrow_pk_sort_exprs,
    QueryChunk,
};

use snafu::{ResultExt, Snafu};

mod adapter;
mod deduplicate;
mod overlap;
mod physical;
use self::{
    deduplicate::DeduplicateExec, overlap::group_potential_duplicates, physical::IOxReadFilterNode,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Chunk schema not compatible for table '{}': {}", table_name, source))]
    ChunkSchemaNotCompatible {
        table_name: String,
        source: internal_types::schema::merge::Error,
    },

    #[snafu(display(
        "Internal error: no chunk pruner provided to builder for {}",
        table_name,
    ))]
    InternalNoChunkPruner { table_name: String },

    #[snafu(display("Internal error: No rows found in table '{}'", table_name))]
    InternalNoRowsInTable { table_name: String },

    #[snafu(display("Internal error: Cannot verify the push-down predicate '{}'", source,))]
    InternalPushdownPredicate {
        source: datafusion::error::DataFusionError,
    },

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
    schema_merger: SchemaMerger,
    chunk_pruner: Option<Arc<dyn ChunkPruner<C>>>,
    chunks: Vec<Arc<C>>,
}

impl<C: QueryChunk> ProviderBuilder<C> {
    pub fn new(table_name: impl AsRef<str>) -> Self {
        Self {
            table_name: Arc::from(table_name.as_ref()),
            schema_merger: SchemaMerger::new(),
            chunk_pruner: None,
            chunks: Vec::new(),
        }
    }

    /// Add a new chunk to this provider
    pub fn add_chunk(mut self, chunk: Arc<C>) -> Result<Self> {
        let chunk_table_schema = chunk.schema();

        self.schema_merger = self
            .schema_merger
            .merge(&chunk_table_schema.as_ref())
            .context(ChunkSchemaNotCompatible {
                table_name: self.table_name.as_ref(),
            })?;

        self.chunks.push(chunk);

        Ok(self)
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
        let iox_schema = self.schema_merger.build();

        // if the table was reported to exist, it should not be empty
        if self.chunks.is_empty() {
            return InternalNoRowsInTable {
                table_name: self.table_name.as_ref(),
            }
            .fail();
        }

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
            iox_schema,
            chunk_pruner,
            table_name: self.table_name,
            chunks: self.chunks,
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
    iox_schema: Schema,
    /// Something that can prune chunks
    chunk_pruner: Arc<dyn ChunkPruner<C>>,
    // The chunks
    chunks: Vec<Arc<C>>,
}

impl<C: QueryChunk + 'static> ChunkTableProvider<C> {
    /// Return the IOx schema view for the data provided by this provider
    pub fn iox_schema(&self) -> Schema {
        self.iox_schema.clone()
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
            Some(indicies) => self.iox_schema.select_by_indices(indicies),
            None => self.iox_schema.clone(),
        };

        // This debug shows the self.arrow_schema() includes all columns in all chunks
        // which means the schema of all chunks are merged before invoking this scan
        trace!("all chunks schema: {:#?}", self.arrow_schema());
        // However, the schema of each chunk is still in its original form which does not
        // include the merged columns of other chunks. The code below proves it
        for chunk in chunks.clone() {
            trace!("Schema of chunk {}: {:#?}", chunk.id(), chunk.schema());
        }

        let mut deduplicate = Deduplicater::new();
        let plan = deduplicate.build_scan_plan(
            Arc::clone(&self.table_name),
            Arc::new(scan_schema),
            chunks,
            predicate,
        )?;

        Ok(plan)
    }

    fn statistics(&self) -> Statistics {
        // TODO translate IOx stats to DataFusion statistics
        Statistics::default()
    }

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
    fn new() -> Self {
        Self {
            overlapped_chunks_set: vec![],
            in_chunk_duplicates_chunks: vec![],
            no_duplicates_chunks: vec![],
        }
    }

    /// The IOx scan process needs to deduplicate data if there are duplicates. Hence it will look
    /// like this. In this example, there are 4 chunks.
    ///  . Chunks 1 and 2 overlap and need to get deduplicated. This includes these main steps:
    ///     i. Read/scan/steam the chunk: IOxReadFilterNode.
    ///     ii. Sort each chunk if they are not sorted yet: SortExec.
    ///     iii. Merge the sorted chunks into one stream: SortPreservingMergeExc.
    ///     iv. Deduplicate the sorted stream: DeduplicateExec
    ///  . Chunk 3 does not overlap with others but has duplicates in it self, hence it only needs to get
    ///      sorted if needed, then deduplicated.
    ///  . Chunk 4 neither overlaps with other chunks nor has duplicates in itself, hence it does not
    ///      need any extra besides chunk reading.
    /// The final UnionExec on top is to union the streams below. If there is only one stream, UnionExec
    ///   will not be added into the plan.
    /// ```text
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
    ///               │ DeduplicateExec │                │ DeduplicateExec │   │IOxReadFilterNode│
    ///               └─────────────────┘                └─────────────────┘   │    (Chunk 4)    │
    ///                        ▲                                  ▲            └─────────────────┘
    ///                        │                                  │
    ///            ┌───────────────────────┐                      │
    ///            │SortPreservingMergeExec│                      │
    ///            └───────────────────────┘                      │
    ///                        ▲                                  │
    ///                        │                                  │
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

    fn build_scan_plan(
        &mut self,
        table_name: Arc<str>,
        output_schema: Arc<Schema>,
        chunks: Vec<Arc<C>>,
        predicate: Predicate,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // find overlapped chunks and put them into the right group
        self.split_overlapped_chunks(chunks.to_vec())?;

        // Building plans
        let mut plans = vec![];
        if self.no_duplicates() {
            // Neither overlaps nor duplicates, no deduplicating needed
            let plan = Self::build_plans_for_non_duplicates_chunk(
                Arc::clone(&table_name),
                Arc::clone(&output_schema),
                chunks,
                predicate,
            );
            plans.push(plan);
        } else {
            // Go over overlapped set, build deduplicate plan for each vector of overlapped chunks
            for overlapped_chunks in self.overlapped_chunks_set.to_vec() {
                plans.push(Self::build_deduplicate_plan_for_overlapped_chunks(
                    Arc::clone(&table_name),
                    Arc::clone(&output_schema),
                    overlapped_chunks.to_owned(),
                    predicate.clone(),
                )?);
            }

            // Go over each in_chunk_duplicates_chunks, build deduplicate plan for each
            for chunk_with_duplicates in self.in_chunk_duplicates_chunks.to_vec() {
                plans.push(Self::build_deduplicate_plan_for_chunk_with_duplicates(
                    Arc::clone(&table_name),
                    Arc::clone(&output_schema),
                    chunk_with_duplicates.to_owned(),
                    predicate.clone(),
                )?);
            }

            // Go over non_duplicates_chunks, build a plan for it
            for no_duplicates_chunk in self.no_duplicates_chunks.to_vec() {
                plans.push(Self::build_plan_for_non_duplicates_chunk(
                    Arc::clone(&table_name),
                    Arc::clone(&output_schema),
                    no_duplicates_chunk.to_owned(),
                    predicate.clone(),
                ));
            }
        }

        match plans.len() {
            // No plan generated. Something must go wrong
            // Even if the chunks are empty, IOxReadFilterNode is still created
            0 => panic!("Internal error generating deduplicate plan"),
            // Only one plan, no need to add union node
            // Return the plan itself
            1 => Ok(plans.remove(0)),
            // Has many plans and need to union them
            _ => Ok(Arc::new(UnionExec::new(plans))),
        }
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Note that we may need to sort/deduplicate based on tag
        // columns which do not appear in the output

        let pk_schema = Self::compute_pk_schema(&chunks);
        let input_schema = Self::compute_input_schema(&output_schema, &pk_schema);

        trace!(
            ?output_schema,
            ?pk_schema,
            ?input_schema,
            "creating deduplicate plan for overlapped chunks"
        );

        // Build sort plan for each chunk
        // todo: compute the chunk sort key and make sure:
        //       the key columns are the same and if they are sort keys are on different order, need to use the most popular one to reduce resort
        //       and need to log if they are different
        let sorted_chunk_plans: Result<Vec<Arc<dyn ExecutionPlan>>> = chunks
            .iter()
            .map(|chunk| {
                Self::build_sort_plan_for_read_filter(
                    Arc::clone(&table_name),
                    Arc::clone(&input_schema),
                    Arc::clone(&chunk),
                    predicate.clone(),
                )
            })
            .collect();

        // Union the plans
        // The UnionExec operator only streams all chunks (aka partitions in Datafusion) and
        // keep them in separate chunks which exactly what we need here
        let plan = UnionExec::new(sorted_chunk_plans?);

        // Now (sort) merge the already sorted chunks
        let sort_exprs = arrow_pk_sort_exprs(pk_schema.primary_key(), &plan.schema());
        let plan = Arc::new(SortPreservingMergeExec::new(
            sort_exprs.clone(),
            Arc::new(plan),
            1024,
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let pk_schema = Self::compute_pk_schema(&[Arc::clone(&chunk)]);
        let input_schema = Self::compute_input_schema(&output_schema, &pk_schema);

        // Create the 2 bottom nodes IOxReadFilterNode and SortExec
        let plan = Self::build_sort_plan_for_read_filter(
            table_name,
            Arc::clone(&input_schema),
            Arc::clone(&chunk),
            predicate,
        )?;

        // Add DeduplicateExc
        // Sort exprs for the deduplication
        // todo (see todo below)
        let sort_exprs = arrow_pk_sort_exprs(pk_schema.primary_key(), &plan.schema());
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Create the bottom node IOxReadFilterNode for this chunk
        let input: Arc<dyn ExecutionPlan> = Arc::new(IOxReadFilterNode::new(
            Arc::clone(&table_name),
            output_schema.as_arrow(),
            vec![Arc::clone(&chunk)],
            predicate,
        ));

        // Add the sort operator, SortExec, if needed
        Self::build_sort_plan(chunk, input)
    }

    /// Add SortExec operator on top of the input plan of the given chunk
    /// The plan will be sorted on the chunk's primary key
    fn build_sort_plan(
        chunk: Arc<C>,
        input: Arc<dyn ExecutionPlan>,
        // todo: add a sortkey
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Todo: check there is sort key and it matches with the given one
        //let sort_key = schema.sort_key();
        if chunk.is_sorted_on_pk() {
            return Ok(input);
        }

        let schema = chunk.schema();
        // todo:
        // If the param sortkey available, use it. Also need to validate it with the chunk's compute_sort_key
        let sort_exprs = arrow_pk_sort_exprs(schema.primary_key(), &input.schema());

        // Create SortExec operator
        Ok(Arc::new(
            SortExec::try_new(sort_exprs, input).context(InternalSort)?,
        ))
    }

    /// Return the simplest IOx scan plan of a given chunk which is IOxReadFilterNode
    /// ```text
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
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(IOxReadFilterNode::new(
            Arc::clone(&table_name),
            output_schema.as_arrow(),
            vec![chunk],
            predicate,
        ))
    }

    /// Return the simplest IOx scan plan for many chunks which is IOxReadFilterNode
    /// ```text
    ///                ┌─────────────────┐
    ///                │IOxReadFilterNode│
    ///                │ (Many Chunks)   │
    ///                └─────────────────┘
    ///```
    fn build_plans_for_non_duplicates_chunk(
        table_name: Arc<str>,
        output_schema: Arc<Schema>,
        chunks: Vec<Arc<C>>, // This chunk is identified having no duplicates
        predicate: Predicate,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(IOxReadFilterNode::new(
            Arc::clone(&table_name),
            output_schema.as_arrow(),
            chunks,
            predicate,
        ))
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
            .merge(&output_schema)
            .unwrap()
            .merge(&pk_schema)
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
    use arrow::datatypes::DataType;
    use arrow_util::assert_batches_eq;
    use datafusion::physical_plan::collect;
    use internal_types::schema::builder::SchemaBuilder;

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
        let c1 = Arc::new(TestChunk::new(1).with_tag_column_with_stats("t", "tag1", "a", "b"));

        // c2: over lap with c3
        let c2 = Arc::new(TestChunk::new(2).with_tag_column_with_stats("t", "tag1", "c", "d"));

        // c3: overlap with c2
        let c3 = Arc::new(TestChunk::new(3).with_tag_column_with_stats("t", "tag1", "c", "d"));

        // c4: self overlap
        let c4 = Arc::new(
            TestChunk::new(4)
                .with_tag_column_with_stats("t", "tag1", "e", "f")
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
        // Chunk 1 with 5 rows of data
        let chunk = Arc::new(
            TestChunk::new(1)
                .with_time_column("t")
                .with_tag_column("t", "tag1")
                .with_int_field_column("t", "field_int")
                .with_five_rows_of_data("t"),
        );

        // Datafusion schema of the chunk
        let schema = chunk.schema().as_arrow();

        // IOx scan operator
        let input: Arc<dyn ExecutionPlan> = Arc::new(IOxReadFilterNode::new(
            Arc::from("t"),
            schema,
            vec![Arc::clone(&chunk)],
            Predicate::default(),
        ));
        let batch = collect(Arc::clone(&input)).await.unwrap();
        // data in its original non-sorted form
        let expected = vec![
            "+-----------+------+-------------------------------+",
            "| field_int | tag1 | time                          |",
            "+-----------+------+-------------------------------+",
            "| 1000      | MT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | 1970-01-01 00:00:00.000005    |",
            "+-----------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);

        // Add Sort operator on top of IOx scan
        let sort_plan = Deduplicater::build_sort_plan(chunk, input);
        let batch = collect(sort_plan.unwrap()).await.unwrap();
        // data is not sorted on primary key(tag1, tag2, time)
        let expected = vec![
            "+-----------+------+-------------------------------+",
            "| field_int | tag1 | time                          |",
            "+-----------+------+-------------------------------+",
            "| 100       | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 1000      | MT   | 1970-01-01 00:00:00.000001    |",
            "| 5         | MT   | 1970-01-01 00:00:00.000005    |",
            "| 10        | MT   | 1970-01-01 00:00:00.000007    |",
            "+-----------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn sort_planning_two_tags_with_time() {
        // Chunk 1 with 5 rows of data
        let chunk = Arc::new(
            TestChunk::new(1)
                .with_time_column("t")
                .with_tag_column("t", "tag1")
                .with_tag_column("t", "tag2")
                .with_int_field_column("t", "field_int")
                .with_five_rows_of_data("t"),
        );

        // Datafusion schema of the chunk
        let schema = chunk.schema().as_arrow();

        // IOx scan operator
        let input: Arc<dyn ExecutionPlan> = Arc::new(IOxReadFilterNode::new(
            Arc::from("t"),
            schema,
            vec![Arc::clone(&chunk)],
            Predicate::default(),
        ));
        let batch = collect(Arc::clone(&input)).await.unwrap();
        // data in its original non-sorted form
        let expected = vec![
            "+-----------+------+------+-------------------------------+",
            "| field_int | tag1 | tag2 | time                          |",
            "+-----------+------+------+-------------------------------+",
            "| 1000      | MT   | CT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | AL   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | MA   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | AL   | 1970-01-01 00:00:00.000005    |",
            "+-----------+------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);

        // Add Sort operator on top of IOx scan
        let sort_plan = Deduplicater::build_sort_plan(chunk, input);
        let batch = collect(sort_plan.unwrap()).await.unwrap();
        // data is not sorted on primary key(tag1, tag2, time)
        let expected = vec![
            "+-----------+------+------+-------------------------------+",
            "| field_int | tag1 | tag2 | time                          |",
            "+-----------+------+------+-------------------------------+",
            "| 100       | AL   | MA   | 1970-01-01 00:00:00.000000050 |",
            "| 70        | CT   | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 5         | MT   | AL   | 1970-01-01 00:00:00.000005    |",
            "| 10        | MT   | AL   | 1970-01-01 00:00:00.000007    |",
            "| 1000      | MT   | CT   | 1970-01-01 00:00:00.000001    |",
            "+-----------+------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn sort_read_filter_plan_for_two_tags_with_time() {
        // Chunk 1 with 5 rows of data
        let chunk = Arc::new(
            TestChunk::new(1)
                .with_time_column("t")
                .with_tag_column("t", "tag1")
                .with_tag_column("t", "tag2")
                .with_int_field_column("t", "field_int")
                .with_five_rows_of_data("t"),
        );

        // Datafusion schema of the chunk
        let schema = chunk.schema();

        let sort_plan = Deduplicater::build_sort_plan_for_read_filter(
            Arc::from("t"),
            schema,
            Arc::clone(&chunk),
            Predicate::default(),
        );
        let batch = collect(sort_plan.unwrap()).await.unwrap();
        // data is not sorted on primary key(tag1, tag2, time)
        let expected = vec![
            "+-----------+------+------+-------------------------------+",
            "| field_int | tag1 | tag2 | time                          |",
            "+-----------+------+------+-------------------------------+",
            "| 100       | AL   | MA   | 1970-01-01 00:00:00.000000050 |",
            "| 70        | CT   | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 5         | MT   | AL   | 1970-01-01 00:00:00.000005    |",
            "| 10        | MT   | AL   | 1970-01-01 00:00:00.000007    |",
            "| 1000      | MT   | CT   | 1970-01-01 00:00:00.000001    |",
            "+-----------+------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn deduplicate_plan_for_overlapped_chunks() {
        // Chunk 1 with 5 rows of data on 2 tags
        let chunk1 = Arc::new(
            TestChunk::new(1)
                .with_time_column("t")
                .with_tag_column("t", "tag1")
                .with_tag_column("t", "tag2")
                .with_int_field_column("t", "field_int")
                .with_five_rows_of_data("t"),
        );

        // Chunk 2 exactly the same with Chunk 1
        let chunk2 = Arc::new(
            TestChunk::new(2)
                .with_time_column("t")
                .with_tag_column("t", "tag1")
                .with_tag_column("t", "tag2")
                .with_int_field_column("t", "field_int")
                .with_five_rows_of_data("t"),
        );
        // Datafusion schema of the chunk
        // the same for 2 chunks
        let schema = chunk1.schema();
        let chunks = vec![chunk1, chunk2];

        // data in its original form
        let expected = vec![
            "+-----------+------+------+-------------------------------+",
            "| field_int | tag1 | tag2 | time                          |",
            "+-----------+------+------+-------------------------------+",
            "| 1000      | MT   | CT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | AL   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | MA   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | AL   | 1970-01-01 00:00:00.000005    |",
            "| 1000      | MT   | CT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | AL   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | MA   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | AL   | 1970-01-01 00:00:00.000005    |",
            "+-----------+------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        let sort_plan = Deduplicater::build_deduplicate_plan_for_overlapped_chunks(
            Arc::from("t"),
            schema,
            chunks,
            Predicate::default(),
        );
        let batch = collect(sort_plan.unwrap()).await.unwrap();
        // data is sorted on primary key(tag1, tag2, time)
        let expected = vec![
            "+-----------+------+------+-------------------------------+",
            "| field_int | tag1 | tag2 | time                          |",
            "+-----------+------+------+-------------------------------+",
            "| 100       | AL   | MA   | 1970-01-01 00:00:00.000000050 |",
            "| 70        | CT   | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 5         | MT   | AL   | 1970-01-01 00:00:00.000005    |",
            "| 10        | MT   | AL   | 1970-01-01 00:00:00.000007    |",
            "| 1000      | MT   | CT   | 1970-01-01 00:00:00.000001    |",
            "+-----------+------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn deduplicate_plan_for_overlapped_chunks_subset() {
        // Same two chunks but only select the field and timestamp, not the tag values
        // Chunk 1 with 5 rows of data on 2 tags
        let chunk1 = Arc::new(
            TestChunk::new(1)
                .with_time_column("t")
                .with_tag_column("t", "tag1")
                .with_tag_column("t", "tag2")
                .with_int_field_column("t", "field_int")
                .with_five_rows_of_data("t"),
        );

        // Chunk 2 exactly the same with Chunk 1
        let chunk2 = Arc::new(
            TestChunk::new(2)
                .with_time_column("t")
                .with_tag_column("t", "tag1")
                .with_tag_column("t", "tag2")
                .with_int_field_column("t", "field_int")
                .with_five_rows_of_data("t"),
        );
        let chunks = vec![chunk1, chunk2];

        // data in its original form
        let expected = vec![
            "+-----------+------+------+-------------------------------+",
            "| field_int | tag1 | tag2 | time                          |",
            "+-----------+------+------+-------------------------------+",
            "| 1000      | MT   | CT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | AL   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | MA   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | AL   | 1970-01-01 00:00:00.000005    |",
            "| 1000      | MT   | CT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | AL   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | MA   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | AL   | 1970-01-01 00:00:00.000005    |",
            "+-----------+------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        // request just the field and timestamp
        let schema = SchemaBuilder::new()
            .field("field_int", DataType::Int64)
            .timestamp()
            .build()
            .unwrap();

        let sort_plan = Deduplicater::build_deduplicate_plan_for_overlapped_chunks(
            Arc::from("t"),
            Arc::new(schema),
            chunks,
            Predicate::default(),
        );
        let batch = collect(sort_plan.unwrap()).await.unwrap();
        // expect onlt only 5 values, with "f1" and "timestamp" (even though input has 10)
        let expected = vec![
            "+-----------+-------------------------------+",
            "| field_int | time                          |",
            "+-----------+-------------------------------+",
            "| 100       | 1970-01-01 00:00:00.000000050 |",
            "| 70        | 1970-01-01 00:00:00.000000100 |",
            "| 5         | 1970-01-01 00:00:00.000005    |",
            "| 10        | 1970-01-01 00:00:00.000007    |",
            "| 1000      | 1970-01-01 00:00:00.000001    |",
            "+-----------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn deduplicate_plan_for_overlapped_chunks_subset_different_fields() {
        // Chunks with different fields / tags, and select a subset
        // Chunk 1 with 5 rows of data on 2 tags
        let chunk1 = Arc::new(
            TestChunk::new(1)
                .with_time_column("t")
                .with_tag_column("t", "tag1")
                .with_tag_column("t", "tag2")
                .with_int_field_column("t", "field_int")
                .with_five_rows_of_data("t"),
        );

        // Chunk 2 same tags, but different fields
        let chunk2 = Arc::new(
            TestChunk::new(2)
                .with_time_column("t")
                .with_tag_column("t", "tag1")
                .with_int_field_column("t", "other_field_int")
                .with_five_rows_of_data("t"),
        );

        // Chunk 3 exactly the same with Chunk 2
        let chunk3 = Arc::new(
            TestChunk::new(3)
                .with_time_column("t")
                .with_tag_column("t", "tag1")
                .with_int_field_column("t", "other_field_int")
                .with_five_rows_of_data("t"),
        );

        let chunks = vec![chunk1, chunk2, chunk3];
        // data in its original form
        let expected = vec![
            "+-----------+------+-------------------------------+-------------------------------+",
            "| field_int | tag1 | tag2                          | time                          |",
            "+-----------+------+-------------------------------+-------------------------------+",
            "| 1000      | MT   | CT                            | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | AL                            | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | CT                            | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | MA                            | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | AL                            | 1970-01-01 00:00:00.000005    |",
            "| 1000      | MT   | 1970-01-01 00:00:00.000001    |                               |",
            "| 10        | MT   | 1970-01-01 00:00:00.000007    |                               |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000100 |                               |",
            "| 100       | AL   | 1970-01-01 00:00:00.000000050 |                               |",
            "| 5         | MT   | 1970-01-01 00:00:00.000005    |                               |",
            "| 1000      | MT   | 1970-01-01 00:00:00.000001    |                               |",
            "| 10        | MT   | 1970-01-01 00:00:00.000007    |                               |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000100 |                               |",
            "| 100       | AL   | 1970-01-01 00:00:00.000000050 |                               |",
            "| 5         | MT   | 1970-01-01 00:00:00.000005    |                               |",
            "+-----------+------+-------------------------------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        // request just the fields
        let schema = SchemaBuilder::new()
            .field("field_int", DataType::Int64)
            .field("other_field_int", DataType::Int64)
            .build()
            .unwrap();

        let sort_plan = Deduplicater::build_deduplicate_plan_for_overlapped_chunks(
            Arc::from("t"),
            Arc::new(schema),
            chunks,
            Predicate::default(),
        );
        let batch = collect(sort_plan.unwrap()).await.unwrap();

        let expected = vec![
            "+-----------+-----------------+",
            "| field_int | other_field_int |",
            "+-----------+-----------------+",
            "| 100       |                 |",
            "|           | 100             |",
            "| 70        |                 |",
            "|           | 70              |",
            "| 5         |                 |",
            "| 10        |                 |",
            "| 1000      |                 |",
            "|           | 1000            |",
            "|           | 5               |",
            "|           | 10              |",
            "+-----------+-----------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn deduplicate_plan_for_overlapped_chunks_with_different_schemas() {
        // Chunk 1 with 5 rows of data on 2 tags
        let chunk1 = Arc::new(
            TestChunk::new(1)
                .with_time_column("t")
                .with_tag_column("t", "tag1")
                .with_tag_column("t", "tag2")
                .with_int_field_column("t", "field_int")
                .with_five_rows_of_data("t"),
        );

        // Chunk 2 has two different tags
        let chunk2 = Arc::new(
            TestChunk::new(2)
                .with_time_column("t")
                .with_tag_column("t", "tag3")
                .with_tag_column("t", "tag1")
                .with_int_field_column("t", "field_int")
                .with_five_rows_of_data("t"),
        );

        // Chunk 3 has just tag3
        let chunk3 = Arc::new(
            TestChunk::new(3)
                .with_time_column("t")
                .with_tag_column("t", "tag3")
                .with_int_field_column("t", "field_int")
                .with_int_field_column("t", "field_int2")
                .with_five_rows_of_data("t"),
        );

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
            "+-----------+------+------+-------------------------------+",
            "| field_int | tag1 | tag2 | time                          |",
            "+-----------+------+------+-------------------------------+",
            "| 1000      | MT   | CT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | AL   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | MA   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | AL   | 1970-01-01 00:00:00.000005    |",
            "| 1000      | MT   | CT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | AL   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | MT   | 1970-01-01 00:00:00.000005    |",
            "| 1000      | 1000 | CT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | 10   | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | 70   | AL   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | 100  | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | 5    | MT   | 1970-01-01 00:00:00.000005    |",
            "+-----------+------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        let sort_plan = Deduplicater::build_deduplicate_plan_for_overlapped_chunks(
            Arc::from("t"),
            Arc::new(schema),
            chunks,
            Predicate::default(),
        );
        let batch = collect(sort_plan.unwrap()).await.unwrap();
        // data is sorted on primary key(tag1, tag2, tag3, time)
        let expected = vec![
            "+-----------+------------+------+------+------+-------------------------------+",
            "| field_int | field_int2 | tag1 | tag2 | tag3 | time                          |",
            "+-----------+------------+------+------+------+-------------------------------+",
            "| 100       |            | AL   | MA   |      | 1970-01-01 00:00:00.000000050 |",
            "| 100       |            | AL   |      | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 70        |            | CT   | CT   |      | 1970-01-01 00:00:00.000000100 |",
            "| 70        |            | CT   |      | AL   | 1970-01-01 00:00:00.000000100 |",
            "| 5         |            | MT   | AL   |      | 1970-01-01 00:00:00.000005    |",
            "| 10        |            | MT   | AL   |      | 1970-01-01 00:00:00.000007    |",
            "| 1000      |            | MT   | CT   |      | 1970-01-01 00:00:00.000001    |",
            "| 1000      |            | MT   |      | CT   | 1970-01-01 00:00:00.000001    |",
            "| 5         |            | MT   |      | MT   | 1970-01-01 00:00:00.000005    |",
            "| 10        |            | MT   |      | MT   | 1970-01-01 00:00:00.000007    |",
            "| 100       | 100        |      |      | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 70        | 70         |      |      | AL   | 1970-01-01 00:00:00.000000100 |",
            "| 1000      | 1000       |      |      | CT   | 1970-01-01 00:00:00.000001    |",
            "| 5         | 5          |      |      | MT   | 1970-01-01 00:00:00.000005    |",
            "| 10        | 10         |      |      | MT   | 1970-01-01 00:00:00.000007    |",
            "+-----------+------------+------+------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn scan_plan_with_one_chunk_no_duplicates() {
        // Test no duplicate at all
        let chunk = Arc::new(
            TestChunk::new(1)
                .with_time_column_with_stats("t", 5, 7000)
                .with_tag_column_with_stats("t", "tag1", "AL", "MT")
                .with_int_field_column("t", "field_int")
                .with_five_rows_of_data("t"),
        );

        // Datafusion schema of the chunk
        let schema = chunk.schema();
        let chunks = vec![chunk];

        // data in its original form
        let expected = vec![
            "+-----------+------+-------------------------------+",
            "| field_int | tag1 | time                          |",
            "+-----------+------+-------------------------------+",
            "| 1000      | MT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | 1970-01-01 00:00:00.000005    |",
            "+-----------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        let mut deduplicator = Deduplicater::new();
        let plan =
            deduplicator.build_scan_plan(Arc::from("t"), schema, chunks, Predicate::default());
        let batch = collect(plan.unwrap()).await.unwrap();
        // No duplicates so no sort at all. The data will stay in their original order
        let expected = vec![
            "+-----------+------+-------------------------------+",
            "| field_int | tag1 | time                          |",
            "+-----------+------+-------------------------------+",
            "| 1000      | MT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | 1970-01-01 00:00:00.000005    |",
            "+-----------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn scan_plan_with_one_chunk_with_duplicates() {
        // Test one chunk with duplicate within
        let chunk = Arc::new(
            TestChunk::new(1)
                .with_time_column_with_stats("t", 5, 7000)
                .with_tag_column_with_stats("t", "tag1", "AL", "MT")
                .with_int_field_column("t", "field_int")
                .with_may_contain_pk_duplicates(true)
                .with_ten_rows_of_data_some_duplicates("t"),
        );

        // Datafusion schema of the chunk
        let schema = chunk.schema();
        let chunks = vec![chunk];

        // data in its original form
        let expected = vec![
            "+-----------+------+-------------------------------+",
            "| field_int | tag1 | time                          |",
            "+-----------+------+-------------------------------+",
            "| 1000      | MT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | 1970-01-01 00:00:00.000000005 |",
            "| 1000      | MT   | 1970-01-01 00:00:00.000002    |",
            "| 20        | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000500 |",
            "| 10        | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 30        | MT   | 1970-01-01 00:00:00.000000005 |",
            "+-----------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        let mut deduplicator = Deduplicater::new();
        let plan =
            deduplicator.build_scan_plan(Arc::from("t"), schema, chunks, Predicate::default());
        let batch = collect(plan.unwrap()).await.unwrap();
        // Data must be sorted and duplicates removed
        let expected = vec![
            "+-----------+------+-------------------------------+",
            "| field_int | tag1 | time                          |",
            "+-----------+------+-------------------------------+",
            "| 10        | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000500 |",
            "| 30        | MT   | 1970-01-01 00:00:00.000000005 |",
            "| 1000      | MT   | 1970-01-01 00:00:00.000001    |",
            "| 1000      | MT   | 1970-01-01 00:00:00.000002    |",
            "| 20        | MT   | 1970-01-01 00:00:00.000007    |",
            "+-----------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn scan_plan_with_one_chunk_with_duplicates_subset() {
        // Test one chunk with duplicate within
        let chunk = Arc::new(
            TestChunk::new(1)
                .with_time_column_with_stats("t", 5, 7000)
                .with_tag_column_with_stats("t", "tag1", "AL", "MT")
                .with_int_field_column("t", "field_int")
                .with_may_contain_pk_duplicates(true)
                .with_ten_rows_of_data_some_duplicates("t"),
        );

        let chunks = vec![chunk];
        // data in its original form
        let expected = vec![
            "+-----------+------+-------------------------------+",
            "| field_int | tag1 | time                          |",
            "+-----------+------+-------------------------------+",
            "| 1000      | MT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | 1970-01-01 00:00:00.000000005 |",
            "| 1000      | MT   | 1970-01-01 00:00:00.000002    |",
            "| 20        | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000500 |",
            "| 10        | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 30        | MT   | 1970-01-01 00:00:00.000000005 |",
            "+-----------+------+-------------------------------+",
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
        );
        let batch = collect(plan.unwrap()).await.unwrap();

        // expect just the 7 rows of de-duplicated data
        let expected = vec![
            "+-----------+-------------------------------+",
            "| field_int | time                          |",
            "+-----------+-------------------------------+",
            "| 10        | 1970-01-01 00:00:00.000000050 |",
            "| 70        | 1970-01-01 00:00:00.000000100 |",
            "| 70        | 1970-01-01 00:00:00.000000500 |",
            "| 30        | 1970-01-01 00:00:00.000000005 |",
            "| 1000      | 1970-01-01 00:00:00.000001    |",
            "| 1000      | 1970-01-01 00:00:00.000002    |",
            "| 20        | 1970-01-01 00:00:00.000007    |",
            "+-----------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn scan_plan_with_two_overlapped_chunks_with_duplicates() {
        // test overlapped chunks
        let chunk1 = Arc::new(
            TestChunk::new(1)
                .with_time_column_with_stats("t", 5, 7000)
                .with_tag_column_with_stats("t", "tag1", "AL", "MT")
                .with_int_field_column("t", "field_int")
                .with_ten_rows_of_data_some_duplicates("t"),
        );

        let chunk2 = Arc::new(
            TestChunk::new(1)
                .with_time_column_with_stats("t", 5, 7000)
                .with_tag_column_with_stats("t", "tag1", "AL", "MT")
                .with_int_field_column("t", "field_int")
                .with_five_rows_of_data("t"),
        );

        // Datafusion schema of the chunk
        let schema = chunk1.schema();
        let chunks = vec![chunk1, chunk2];

        // data in its original form
        let expected = vec![
            "+-----------+------+-------------------------------+",
            "| field_int | tag1 | time                          |",
            "+-----------+------+-------------------------------+",
            "| 1000      | MT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | 1970-01-01 00:00:00.000000005 |",
            "| 1000      | MT   | 1970-01-01 00:00:00.000002    |",
            "| 20        | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000500 |",
            "| 10        | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 30        | MT   | 1970-01-01 00:00:00.000000005 |",
            "| 1000      | MT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | 1970-01-01 00:00:00.000005    |",
            "+-----------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        let mut deduplicator = Deduplicater::new();
        let plan =
            deduplicator.build_scan_plan(Arc::from("t"), schema, chunks, Predicate::default());
        let batch = collect(plan.unwrap()).await.unwrap();
        // Two overlapped chunks will be sort merged with dupplicates removed
        let expected = vec![
            "+-----------+------+-------------------------------+",
            "| field_int | tag1 | time                          |",
            "+-----------+------+-------------------------------+",
            "| 100       | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000500 |",
            "| 30        | MT   | 1970-01-01 00:00:00.000000005 |",
            "| 1000      | MT   | 1970-01-01 00:00:00.000001    |",
            "| 1000      | MT   | 1970-01-01 00:00:00.000002    |",
            "| 5         | MT   | 1970-01-01 00:00:00.000005    |",
            "| 10        | MT   | 1970-01-01 00:00:00.000007    |",
            "+-----------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn scan_plan_with_four_chunks() {
        // This test covers all kind of chunks: overlap, non-overlap without duplicates within, non-overlap with duplicates within
        let chunk1 = Arc::new(
            TestChunk::new(1)
                .with_time_column_with_stats("t", 5, 7000)
                .with_tag_column_with_stats("t", "tag1", "AL", "MT")
                .with_int_field_column("t", "field_int")
                .with_ten_rows_of_data_some_duplicates("t"),
        );

        // chunk2 overlaps with chunk 1
        let chunk2 = Arc::new(
            TestChunk::new(1)
                .with_time_column_with_stats("t", 5, 7000)
                .with_tag_column_with_stats("t", "tag1", "AL", "MT")
                .with_int_field_column("t", "field_int")
                .with_five_rows_of_data("t"),
        );

        // chunk3 no overlap, no duplicates within
        let chunk3 = Arc::new(
            TestChunk::new(1)
                .with_time_column_with_stats("t", 8000, 20000)
                .with_tag_column_with_stats("t", "tag1", "UT", "WA")
                .with_int_field_column("t", "field_int")
                .with_three_rows_of_data("t"),
        );

        // chunk3 no overlap, duplicates within
        let chunk4 = Arc::new(
            TestChunk::new(1)
                .with_time_column_with_stats("t", 28000, 220000)
                .with_tag_column_with_stats("t", "tag1", "UT", "WA")
                .with_int_field_column("t", "field_int")
                .with_may_contain_pk_duplicates(true)
                .with_four_rows_of_data("t"),
        );

        // Datafusion schema of the chunk
        let schema = chunk1.schema();
        let chunks = vec![chunk1, chunk2, chunk3, chunk4];

        // data in its original form
        let expected = vec![
            "+-----------+------+-------------------------------+",
            "| field_int | tag1 | time                          |",
            "+-----------+------+-------------------------------+",
            "| 1000      | MT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | 1970-01-01 00:00:00.000000005 |",
            "| 1000      | MT   | 1970-01-01 00:00:00.000002    |",
            "| 20        | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000500 |",
            "| 10        | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 30        | MT   | 1970-01-01 00:00:00.000000005 |",
            "| 1000      | MT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | 1970-01-01 00:00:00.000005    |",
            "| 1000      | WA   | 1970-01-01 00:00:00.000008    |",
            "| 10        | VT   | 1970-01-01 00:00:00.000010    |",
            "| 70        | UT   | 1970-01-01 00:00:00.000020    |",
            "| 1000      | WA   | 1970-01-01 00:00:00.000008    |",
            "| 10        | VT   | 1970-01-01 00:00:00.000010    |",
            "| 70        | UT   | 1970-01-01 00:00:00.000020    |",
            "| 50        | VT   | 1970-01-01 00:00:00.000010    |",
            "+-----------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&chunks).await);

        let mut deduplicator = Deduplicater::new();
        let plan =
            deduplicator.build_scan_plan(Arc::from("t"), schema, chunks, Predicate::default());
        let batch = collect(plan.unwrap()).await.unwrap();
        // Final data is partially sorted with duplicates removed. Detailed:
        //   . chunk1 and chunk2 will be sorted merged and deduplicated (rows 8-32)
        //   . chunk3 will stay in its original (rows 1-3)
        //   . chunk4 will be sorted and deduplicated (rows 4-7)
        let expected = vec![
            "+-----------+------+-------------------------------+",
            "| field_int | tag1 | time                          |",
            "+-----------+------+-------------------------------+",
            "| 1000      | WA   | 1970-01-01 00:00:00.000008    |",
            "| 10        | VT   | 1970-01-01 00:00:00.000010    |",
            "| 70        | UT   | 1970-01-01 00:00:00.000020    |",
            "| 70        | UT   | 1970-01-01 00:00:00.000020    |",
            "| 50        | VT   | 1970-01-01 00:00:00.000010    |",
            "| 1000      | WA   | 1970-01-01 00:00:00.000008    |",
            "| 100       | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000500 |",
            "| 30        | MT   | 1970-01-01 00:00:00.000000005 |",
            "| 1000      | MT   | 1970-01-01 00:00:00.000001    |",
            "| 1000      | MT   | 1970-01-01 00:00:00.000002    |",
            "| 5         | MT   | 1970-01-01 00:00:00.000005    |",
            "| 10        | MT   | 1970-01-01 00:00:00.000007    |",
            "+-----------+------+-------------------------------+",
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
