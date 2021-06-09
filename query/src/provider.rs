//! Implementation of a DataFusion `TableProvider` in terms of `PartitionChunk`s

use std::sync::Arc;

use arrow::{compute::SortOptions, datatypes::SchemaRef as ArrowSchemaRef, error::ArrowError};
use datafusion::{
    datasource::{
        datasource::{Statistics, TableProviderFilterPushDown},
        TableProvider,
    },
    error::{DataFusionError, Result as DataFusionResult},
    logical_plan::Expr,
    physical_plan::{
        expressions::{col, PhysicalSortExpr},
        sort::SortExec,
        ExecutionPlan,
    },
};
use internal_types::schema::{builder::SchemaMerger, Schema};
use observability_deps::tracing::debug;

use crate::{
    duplicate::group_potential_duplicates,
    predicate::{Predicate, PredicateBuilder},
    util::project_schema,
    PartitionChunk,
};

use snafu::{ResultExt, Snafu};

mod adapter;
mod physical;
use self::physical::IOxReadFilterNode;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Chunk schema not compatible for table '{}': {}", table_name, source))]
    ChunkSchemaNotCompatible {
        table_name: String,
        source: internal_types::schema::builder::Error,
    },

    #[snafu(display(
        "Internal error: no chunks found in builder for table '{}': {}",
        table_name,
        source,
    ))]
    InternalNoChunks {
        table_name: String,
        source: internal_types::schema::builder::Error,
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

    #[snafu(display("Internal error adding sort operator '{}'", source,))]
    InternalSort {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Internal error: Can not group chunks '{}'", source,))]
    InternalChunkGrouping { source: crate::duplicate::Error },
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
pub trait ChunkPruner<C: PartitionChunk>: Sync + Send + std::fmt::Debug {
    /// prune `chunks`, if possible, based on predicate.
    fn prune_chunks(&self, chunks: Vec<Arc<C>>, predicate: &Predicate) -> Vec<Arc<C>>;
}

/// Builds a `ChunkTableProvider` from a series of `PartitionChunk`s
/// and ensures the schema across the chunks is compatible and
/// consistent.
#[derive(Debug)]
pub struct ProviderBuilder<C: PartitionChunk + 'static> {
    table_name: Arc<str>,
    schema_merger: SchemaMerger,
    chunk_pruner: Option<Arc<dyn ChunkPruner<C>>>,
    chunks: Vec<Arc<C>>,
}

impl<C: PartitionChunk> ProviderBuilder<C> {
    pub fn new(table_name: impl AsRef<str>) -> Self {
        Self {
            table_name: Arc::from(table_name.as_ref()),
            schema_merger: SchemaMerger::new(),
            chunk_pruner: None,
            chunks: Vec::new(),
        }
    }

    /// Add a new chunk to this provider
    pub fn add_chunk(self, chunk: Arc<C>, chunk_table_schema: Schema) -> Result<Self> {
        let Self {
            table_name,
            schema_merger,
            chunk_pruner,
            mut chunks,
        } = self;

        let schema_merger =
            schema_merger
                .merge(chunk_table_schema)
                .context(ChunkSchemaNotCompatible {
                    table_name: table_name.as_ref(),
                })?;

        chunks.push(chunk);

        Ok(Self {
            table_name,
            schema_merger,
            chunk_pruner,
            chunks,
        })
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
        let Self {
            table_name,
            schema_merger,
            chunk_pruner,
            chunks,
        } = self;

        let iox_schema = schema_merger
            .build()
            .context(InternalNoChunks {
                table_name: table_name.as_ref(),
            })?
            // sort so the columns are always in a consistent order
            .sort_fields_by_name();

        // if the table was reported to exist, it should not be empty
        if chunks.is_empty() {
            return InternalNoRowsInTable {
                table_name: table_name.as_ref(),
            }
            .fail();
        }

        let chunk_pruner = match chunk_pruner {
            Some(chunk_pruner) => chunk_pruner,
            None => {
                return InternalNoChunkPruner {
                    table_name: table_name.as_ref(),
                }
                .fail()
            }
        };

        Ok(ChunkTableProvider {
            table_name,
            iox_schema,
            chunk_pruner,
            chunks,
        })
    }
}

/// Implementation of a DataFusion TableProvider in terms of PartitionChunks
///
/// This allows DataFusion to see data from Chunks as a single table, as well as
/// push predicates and selections down to chunks
#[derive(Debug)]
pub struct ChunkTableProvider<C: PartitionChunk + 'static> {
    table_name: Arc<str>,
    /// The IOx schema (wrapper around Arrow Schemaref) for this table
    iox_schema: Schema,
    /// Something that can prune chunks
    chunk_pruner: Arc<dyn ChunkPruner<C>>,
    // The chunks
    chunks: Vec<Arc<C>>,
}

impl<C: PartitionChunk + 'static> ChunkTableProvider<C> {
    /// Return the IOx schema view for the data provided by this provider
    pub fn iox_schema(&self) -> Schema {
        self.iox_schema.clone()
    }

    /// Return the Arrow schema view for the data provided by this provider
    pub fn arrow_schema(&self) -> ArrowSchemaRef {
        self.iox_schema.as_arrow()
    }
}

impl<C: PartitionChunk + 'static> TableProvider for ChunkTableProvider<C> {
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
        debug!(?filters, "Input Filters to Scan");

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
        let scan_schema = project_schema(self.arrow_schema(), projection);

        let mut deduplicate = Deduplicater::new();
        let plan = deduplicate.build_scan_plan(
            Arc::clone(&self.table_name),
            scan_schema,
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
pub(crate) struct Deduplicater<C: PartitionChunk + 'static> {
    // a vector of a vector of overlapped chunks
    pub overlapped_chunks_set: Vec<Vec<Arc<C>>>,

    // a vector of non-overlapped chunks each have duplicates in itself
    pub in_chunk_duplicates_chunks: Vec<Arc<C>>,

    // a vector of non-overlapped and non-duplicates chunks
    pub no_duplicates_chunks: Vec<Arc<C>>,
}

impl<C: PartitionChunk + 'static> Deduplicater<C> {
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
        schema: ArrowSchemaRef,
        chunks: Vec<Arc<C>>,
        predicate: Predicate,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // findi overlapped chunks and put them into the right group
        self.split_overlapped_chunks(chunks.to_vec())?;

        // TEMP until the rest of this module's code is complete:
        // merge all plans into the same
        self.no_duplicates_chunks
            .append(&mut self.in_chunk_duplicates_chunks);
        for mut group in &mut self.overlapped_chunks_set {
            self.no_duplicates_chunks.append(&mut group);
        }
        self.overlapped_chunks_set.clear();

        // Building plans
        let mut plans = vec![];
        if self.no_duplicates() {
            // Neither overlaps nor duplicates, no deduplicating needed
            let plan = Self::build_plans_for_non_duplicates_chunk(
                Arc::clone(&table_name),
                Arc::clone(&schema),
                chunks,
                predicate,
            );
            plans.push(plan);
        } else {
            // Go over overlapped set, build deduplicate plan for each vector of overlapped chunks
            for overlapped_chunks in self.overlapped_chunks_set.to_vec() {
                plans.push(Self::build_deduplicate_plan_for_overlapped_chunks(
                    Arc::clone(&table_name),
                    Arc::clone(&schema),
                    overlapped_chunks.to_owned(),
                    predicate.clone(),
                ));
            }

            // Go over each in_chunk_duplicates_chunks, build deduplicate plan for each
            for chunk_with_duplicates in self.in_chunk_duplicates_chunks.to_vec() {
                plans.push(Self::build_deduplicate_plan_for_chunk_with_duplicates(
                    Arc::clone(&table_name),
                    Arc::clone(&schema),
                    chunk_with_duplicates.to_owned(),
                    predicate.clone(),
                )?);
            }

            // Go over non_duplicates_chunks, build a plan for it
            for no_duplicates_chunk in self.no_duplicates_chunks.to_vec() {
                plans.push(Self::build_plan_for_non_duplicates_chunk(
                    Arc::clone(&table_name),
                    Arc::clone(&schema),
                    no_duplicates_chunk.to_owned(),
                    predicate.clone(),
                ));
            }
        }

        let final_plan = plans.remove(0);

        // TODO
        // There are still plan, add UnionExec
        if !plans.is_empty() {
            // final_plan = union_plan
            panic!("Unexpected error: There should be only one output for scan plan, but there were: {:#?}", plans);
        }

        Ok(final_plan)
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

    /// Return true if all chunks are neither overlap nor has duplicates in itself
    fn no_duplicates(&self) -> bool {
        self.overlapped_chunks_set.is_empty() && self.in_chunk_duplicates_chunks.is_empty()
    }

    /// Return deduplicate plan for the given overlapped chunks
    /// The plan will look like this
    /// ```text
    ///               ┌─────────────────┐       
    ///               │ DeduplicateExec │         
    ///               └─────────────────┘        
    ///                        ▲                            
    ///                        │                                                                 
    ///            ┌───────────────────────┐                                                   
    ///            │SortPreservingMergeExec│                                                     
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
        schema: ArrowSchemaRef,
        chunks: Vec<Arc<C>>, // These chunks are identified overlapped
        predicate: Predicate,
    ) -> Arc<dyn ExecutionPlan> {
        // TODO
        // Currently return just like there are no overlaps, no duplicates
        Arc::new(IOxReadFilterNode::new(
            Arc::clone(&table_name),
            schema,
            chunks,
            predicate,
        ))
    }

    /// Return deduplicate plan for a given chunk with duplicates
    /// The plan will look like this
    /// ```text
    ///                ┌─────────────────┐   
    ///                │ DeduplicateExec │   
    ///                └─────────────────┘   
    ///                        ▲             
    ///                        │             
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
    fn build_deduplicate_plan_for_chunk_with_duplicates(
        table_name: Arc<str>,
        schema: ArrowSchemaRef,
        chunk: Arc<C>, // This chunk is identified having duplicates
        predicate: Predicate,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Create the bottom node IOxReadFilterNode for this chunk
        let input: Arc<dyn ExecutionPlan> = Arc::new(IOxReadFilterNode::new(
            Arc::clone(&table_name),
            schema,
            vec![Arc::clone(&chunk)],
            predicate,
        ));

        // Add the sort operator, SortExec, if needed
        //let plan = Self::build_sort_plan(chunk, input);
        Self::build_sort_plan(chunk, input)

        // Create DeduplicateExc
        // TODO: Add DeuplicateExec here when it is implemented in https://github.com/influxdata/influxdb_iox/issues/1646
        //plan = add_deduplicate_exec(plan);

        //plan
    }

    /// Add SortExec operator on top of the input plan of the given chunk
    /// The plan will be sorted on the chunk's primary key
    fn build_sort_plan(
        chunk: Arc<C>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if chunk.is_sorted_on_pk() {
            return Ok(input);
        }

        // Sort the chunk on pk
        let key_summaries = chunk.summary().primary_key_columns();

        // build sort expression
        let mut sort_exprs = vec![];
        for key in key_summaries {
            sort_exprs.push(PhysicalSortExpr {
                expr: col(key.name.as_str()),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            });
        }

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
        schema: ArrowSchemaRef,
        chunk: Arc<C>, // This chunk is identified having no duplicates
        predicate: Predicate,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(IOxReadFilterNode::new(
            Arc::clone(&table_name),
            schema,
            vec![chunk],
            predicate,
        ))
    }

    /// Return the simplest IOx scan plan for many chunks which is IOxReadFilterNode
    /// ```text
    ///                ┌─────────────────┐   
    ///                │IOxReadFilterNode│   
    ///                │    (Chunk)      │   
    ///                └─────────────────┘   
    ///```
    fn build_plans_for_non_duplicates_chunk(
        table_name: Arc<str>,
        schema: ArrowSchemaRef,
        chunks: Vec<Arc<C>>, // This chunk is identified having no duplicates
        predicate: Predicate,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(IOxReadFilterNode::new(
            Arc::clone(&table_name),
            schema,
            chunks,
            predicate,
        ))
    }
}

#[derive(Debug)]
/// A pruner that does not do pruning (suitable if no additional pruning is possible)
struct NoOpPruner {}
impl<C: PartitionChunk> ChunkPruner<C> for NoOpPruner {
    fn prune_chunks(&self, chunks: Vec<Arc<C>>, _predicate: &Predicate) -> Vec<Arc<C>> {
        chunks
    }
}

#[cfg(test)]
mod test {
    use arrow_util::assert_batches_eq;
    use datafusion::physical_plan::collect;
    use internal_types::selection::Selection;

    use crate::test::TestChunk;

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
        let schema = chunk.table_schema(Selection::All).unwrap().as_arrow();

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
            "+------+-----------+-------------------------------+",
            "| tag1 | field_int | time                          |",
            "+------+-----------+-------------------------------+",
            "| MT   | 1000      | 1970-01-01 00:00:00.000001    |",
            "| MT   | 10        | 1970-01-01 00:00:00.000007    |",
            "| CT   | 70        | 1970-01-01 00:00:00.000000100 |",
            "| AL   | 100       | 1970-01-01 00:00:00.000000050 |",
            "| MT   | 5         | 1970-01-01 00:00:00.000005    |",
            "+------+-----------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);

        // Add Sort operator on top of IOx scan
        let sort_plan = Deduplicater::build_sort_plan(chunk, input);
        let batch = collect(sort_plan.unwrap()).await.unwrap();
        // data is not sorted on primary key(tag1, tag2, time)
        let expected = vec![
            "+------+-----------+-------------------------------+",
            "| tag1 | field_int | time                          |",
            "+------+-----------+-------------------------------+",
            "| AL   | 100       | 1970-01-01 00:00:00.000000050 |",
            "| CT   | 70        | 1970-01-01 00:00:00.000000100 |",
            "| MT   | 1000      | 1970-01-01 00:00:00.000001    |",
            "| MT   | 5         | 1970-01-01 00:00:00.000005    |",
            "| MT   | 10        | 1970-01-01 00:00:00.000007    |",
            "+------+-----------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);
    }

    #[tokio::test]
    async fn sort_planning_two_tags_with_time() {
        // Chunk 1 with 5 rows of data
        let chunk = Arc::new(
            TestChunk::new(1)
                .with_tag_column("t", "tag1")
                .with_tag_column("t", "tag2")
                .with_int_field_column("t", "field_int")
                .with_time_column("t")
                .with_five_rows_of_data("t"),
        );

        // Datafusion schema of the chunk
        let schema = chunk.table_schema(Selection::All).unwrap().as_arrow();

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
            "+------+------+-----------+-------------------------------+",
            "| tag1 | tag2 | field_int | time                          |",
            "+------+------+-----------+-------------------------------+",
            "| MT   | CT   | 1000      | 1970-01-01 00:00:00.000001    |",
            "| MT   | AL   | 10        | 1970-01-01 00:00:00.000007    |",
            "| CT   | CT   | 70        | 1970-01-01 00:00:00.000000100 |",
            "| AL   | MA   | 100       | 1970-01-01 00:00:00.000000050 |",
            "| MT   | AL   | 5         | 1970-01-01 00:00:00.000005    |",
            "+------+------+-----------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &batch);

        // Add Sort operator on top of IOx scan
        let sort_plan = Deduplicater::build_sort_plan(chunk, input);
        let batch = collect(sort_plan.unwrap()).await.unwrap();
        // data is not sorted on primary key(tag1, tag2, time)
        let expected = vec![
            "+------+------+-----------+-------------------------------+",
            "| tag1 | tag2 | field_int | time                          |",
            "+------+------+-----------+-------------------------------+",
            "| AL   | MA   | 100       | 1970-01-01 00:00:00.000000050 |",
            "| CT   | CT   | 70        | 1970-01-01 00:00:00.000000100 |",
            "| MT   | AL   | 5         | 1970-01-01 00:00:00.000005    |",
            "| MT   | AL   | 10        | 1970-01-01 00:00:00.000007    |",
            "| MT   | CT   | 1000      | 1970-01-01 00:00:00.000001    |",
            "+------+------+-----------+-------------------------------+",
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
