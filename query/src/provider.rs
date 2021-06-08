//! Implementation of a DataFusion `TableProvider` in terms of `PartitionChunk`s

use std::sync::Arc;

use arrow::{compute::SortOptions, datatypes::SchemaRef as ArrowSchemaRef};
// use data_types::partition_metadata::ColumnSummary;
use datafusion::{datasource::{
        datasource::{Statistics, TableProviderFilterPushDown},
        TableProvider,
    }, error::{DataFusionError, Result as DataFusionResult}, logical_plan::Expr, physical_plan::{ExecutionPlan, expressions::{PhysicalSortExpr, col}, sort::SortExec}};
use internal_types::schema::{builder::SchemaMerger, Schema};
use observability_deps::tracing::debug;

use crate::{
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
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

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
        );

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
    ) -> Arc<dyn ExecutionPlan> {
        //finding overlapped chunks and put them into the right group
        self.split_overlapped_chunks(chunks.to_vec());

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
                ));
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
            panic!("Unexpected error: There should be only one output for scan plan");
        }

        final_plan
    }

    /// discover overlaps and split them into three groups:
    ///  1. vector of vector of overlapped chunks
    ///  2. vector of non-overlapped chunks, each have duplicates in itself
    ///  3. vectors of non-overlapped chunks without duplicates
    fn split_overlapped_chunks(&mut self, chunks: Vec<Arc<C>>) {
        // TODO: need to discover overlaps and split them
        // The current behavior is just like neither overlaps nor having duplicates in its own chunk
        //self.overlapped_chunks_set = vec![];
        //self.in_chunk_duplicates_chunks = vec![];
        self.no_duplicates_chunks.append(&mut chunks.to_vec());
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
    ) -> Arc<dyn ExecutionPlan> {

        // Create the bottom node IOxRedFilterNode for this chunk
        let mut input: Arc<dyn ExecutionPlan>  = Arc::new(IOxReadFilterNode::new(
            Arc::clone(&table_name),
            schema,
            vec![Arc::clone(&chunk)],
            predicate,
        ));
        

        // Add the sort operator, SortExec, if needed
        if !chunk.is_sorted() {

            let key_summaries = chunk.primary_key_columns();

            // build sort expression
            let mut sort_exprs = vec![];
            for key in key_summaries {
                sort_exprs.push(
                    PhysicalSortExpr{
                        expr: col(key.name.as_str()),
                        options: SortOptions {
                            descending: false,
                            nulls_first: false,
                        }
                    }
                );
            }

            // Create SortExec operator on top of the IOxReadFilterNode
            let sort_exec = SortExec::try_new(sort_exprs, input);
            let sort_exec = match sort_exec {
                Ok(plan) => plan,
                Err(e) => panic!("Internal error while adding SortExec: {}", e) // This should never happens 
            };
            input = Arc::new(sort_exec);
        }
    
        // Create DeduplicateExc
        // TODO: Add DeuplicateExec here when it is implemented in https://github.com/influxdata/influxdb_iox/issues/1646

        input
    }


    //////////

                    // let sort_expr = expr
                //     .iter()
                //     .map(|e| match e {
                //         Expr::Sort {
                //             expr,
                //             asc,
                //             nulls_first,
                //         } => self.create_physical_sort_expr(
                //             expr,
                //             &input_schema,
                //             SortOptions {
                //                 descending: !*asc,
                //                 nulls_first: *nulls_first,
                //             },
                //             ctx_state,
                //         ),
                //         _ => Err(DataFusionError::Plan(
                //             "Sort only accepts sort expressions".to_string(),
                //         )),
                //     })
                //     .collect::<Result<Vec<_>>>()?;

                // Ok(Arc::new(SortExec::try_new(sort_expr, input)?))

    ///////////
    /////// TEST: create SortExec plan
    // #[tokio::test]
    // async fn test_lex_sort_by_float() -> Result<()> {
    //     let schema = Arc::new(Schema::new(vec![
    //         Field::new("a", DataType::Float32, true),
    //         Field::new("b", DataType::Float64, true),
    //     ]));

    //     // define data.
    //     let batch = RecordBatch::try_new(
    //         schema.clone(),
    //         vec![
    //             Arc::new(Float32Array::from(vec![
    //                 Some(f32::NAN),
    //                 None,
    //                 None,
    //                 Some(f32::NAN),
    //                 Some(1.0_f32),
    //                 Some(1.0_f32),
    //                 Some(2.0_f32),
    //                 Some(3.0_f32),
    //             ])),
    //             Arc::new(Float64Array::from(vec![
    //                 Some(200.0_f64),
    //                 Some(20.0_f64),
    //                 Some(10.0_f64),
    //                 Some(100.0_f64),
    //                 Some(f64::NAN),
    //                 None,
    //                 None,
    //                 Some(f64::NAN),
    //             ])),
    //         ],
    //     )?;

    //     let sort_exec = Arc::new(SortExec::try_new(
    //         vec![
    //             PhysicalSortExpr {
    //                 expr: col("a"),
    //                 options: SortOptions {
    //                     descending: true,
    //                     nulls_first: true,
    //                 },
    //             },
    //             PhysicalSortExpr {
    //                 expr: col("b"),
    //                 options: SortOptions {
    //                     descending: false,
    //                     nulls_first: false,
    //                 },
    //             },
    //         ],
    //         Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None)?),
    //     )?);

    //     assert_eq!(DataType::Float32, *sort_exec.schema().field(0).data_type());
    //     assert_eq!(DataType::Float64, *sort_exec.schema().field(1).data_type());

    //     let result: Vec<RecordBatch> = collect(sort_exec.clone()).await?;
    //     assert!(sort_exec.metrics().get("sortTime").unwrap().value() > 0);
    //     assert_eq!(sort_exec.metrics().get("outputRows").unwrap().value(), 8);
    //     assert_eq!(result.len(), 1);

    //     let columns = result[0].columns();

    //     assert_eq!(DataType::Float32, *columns[0].data_type());
    //     assert_eq!(DataType::Float64, *columns[1].data_type());

    //     let a = as_primitive_array::<Float32Type>(&columns[0]);
    //     let b = as_primitive_array::<Float64Type>(&columns[1]);

    //     // convert result to strings to allow comparing to expected result containing NaN
    //     let result: Vec<(Option<String>, Option<String>)> = (0..result[0].num_rows())
    //         .map(|i| {
    //             let aval = if a.is_valid(i) {
    //                 Some(a.value(i).to_string())
    //             } else {
    //                 None
    //             };
    //             let bval = if b.is_valid(i) {
    //                 Some(b.value(i).to_string())
    //             } else {
    //                 None
    //             };
    //             (aval, bval)
    //         })
    //         .collect();

    //     let expected: Vec<(Option<String>, Option<String>)> = vec![
    //         (None, Some("10".to_owned())),
    //         (None, Some("20".to_owned())),
    //         (Some("NaN".to_owned()), Some("100".to_owned())),
    //         (Some("NaN".to_owned()), Some("200".to_owned())),
    //         (Some("3".to_owned()), Some("NaN".to_owned())),
    //         (Some("2".to_owned()), None),
    //         (Some("1".to_owned()), Some("NaN".to_owned())),
    //         (Some("1".to_owned()), None),
    //     ];

    //     assert_eq!(expected, result);

    //     Ok(())
    // }
    /////////////

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


#[tokio::test]
async fn test_sort() {
    
}