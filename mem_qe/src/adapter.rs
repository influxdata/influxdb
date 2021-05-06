//! Code for interfacing and running queries in DataFusion

// use crate::Store;
// use arrow::{
//     datatypes::{Schema, SchemaRef},
//     record_batch::{RecordBatch, RecordBatchReader},
//     util::pretty,
// };
// use datafusion::prelude::*;
// use datafusion::{
//     datasource::TableProvider,
//     execution::{
//         context::ExecutionContextState,
//         physical_plan::{common::RecordBatchIterator, ExecutionPlan,
// Partition},     },
//     logicalplan::{make_logical_plan_node, Expr, LogicalPlan},
//     lp::LogicalPlanNode,
//     optimizer::utils,
// };

// use crate::column;
// use std::{
//     fmt,
//     sync::{Arc, Mutex},
// };

// Wrapper to adapt a Store to a DataFusion "TableProvider" --
// eventually we could also implement this directly on Store
// pub struct StoreTableSource {
//     store: Arc<Store>,
// }

// impl<'a> StoreTableSource {
//     pub fn new(store: Arc<Store>) -> Self {
//         Self { store }
//     }
// }

// impl TableProvider for StoreTableSource {
//     /// Get a reference to the schema for this table
//     fn schema(&self) -> SchemaRef {
//         self.store.schema()
//     }

//     /// Perform a scan of a table and return a sequence of iterators over the
// data (one     /// iterator per partition)
//     fn scan(
//         &self,
//         _projection: &Option<Vec<usize>>,
//         _batch_size: usize,
//     ) -> datafusion::error::Result<Vec<Arc<dyn Partition>>> {
//         unimplemented!("scan not yet implemented");
//     }
// }

// /// Prototype of how a InfluxDB IOx query engine, built on top of
// /// DataFusion, but using specialized column store operators might
// /// look like.
// ///
// /// Data from the Segments in the `store` are visible in DataFusion
// /// as a table ("measurement") in this prototype.
// pub struct IOxQueryEngine {
// ctx: ExecutionContext,
// store: Arc<Store>,
// }

// impl IOxQueryEngine {
// pub fn new(store: Arc<Store>) -> Self {
// let start = std::time::Instant::now();
// let mut ctx = ExecutionContext::new();
// let source = StoreTableSource::new(store.clone());
// let source = Box::new(source);
// ctx.register_table("measurement", source);
// println!("Completed setup in {:?}", start.elapsed());
// IOxQueryEngine { ctx, store }
// }

//     // Run the specified SQL and return the number of records matched
//     pub fn run_sql(&mut self, sql: &str) -> usize {
//         let plan = self
//             .ctx
//             .create_logical_plan(sql)
//             .expect("Creating the logical plan");

//         //println!("Created logical plan:\n{:?}", plan);
//         let plan = self.rewrite_to_segment_scan(&plan);
//         //println!("Rewritten logical plan:\n{:?}", plan);

//         match self.ctx.collect_plan(&plan) {
//             Err(err) => {
//                 println!("Error running query: {:?}", err);
//                 0
//             }
//             Ok(results) => {
//                 if results.is_empty() {
//                     //println!("Empty result returned");
//                     0
//                 } else {
//                     pretty::print_batches(&results).expect("printing");
//                     results.iter().map(|b| b.num_rows()).sum()
//                 }
//             }
//         }
//     }

//     /// Specialized optimizer pass that combines a `TableScan` and a `Filter`
//     /// together into a SegementStore with the predicates.
//     ///
//     /// For example, given this input:
//     ///
//     /// Projection: #env, #method, #host, #counter, #time
//     ///   Filter: #time GtEq Int64(1590036110000000)
//     ///     TableScan: measurement projection=None
//     ///
//     /// The following plan would be produced
//     /// Projection: #env, #method, #host, #counter, #time
//     ///   SegmentScan: measurement projection=None predicate=: #time GtEq
// Int64(1590036110000000)     ///
//     fn rewrite_to_segment_scan(&self, plan: &LogicalPlan) -> LogicalPlan {
//         if let LogicalPlan::Filter { predicate, input } = plan {
//             // see if the input is a TableScan
//             if let LogicalPlan::TableScan { .. } = **input {
//                 return make_logical_plan_node(Box::new(SegmentScan::new(
//                     self.store.clone(),
//                     predicate.clone(),
//                 )));
//             }
//         }

//         // otherwise recursively apply
//         let optimized_inputs = utils::inputs(&plan)
//             .iter()
//             .map(|input| self.rewrite_to_segment_scan(input))
//             .collect();

//         return utils::from_plan(plan, &utils::expressions(plan),
// &optimized_inputs)             .expect("Created plan");
//     }
// }

// /// LogicalPlan node that serves as a scan of the segment store with optional
// predicates struct SegmentScan {
//     /// The underlying Store
//     store: Arc<Store>,

//     schema: SchemaRef,

//     /// The predicate to apply during the scan
//     predicate: Expr,
// }

// impl<'a> SegmentScan {
//     fn new(store: Arc<Store>, predicate: Expr) -> Self {
//         let schema = store.schema().clone();

//         SegmentScan {
//             store,
//             schema,
//             predicate,
//         }
//     }
// }

// impl LogicalPlanNode for SegmentScan {
//     /// Return  a reference to the logical plan's inputs
//     fn inputs(&self) -> Vec<&LogicalPlan> {
//         Vec::new()
//     }

//     /// Get a reference to the logical plan's schema
//     fn schema(&self) -> &Schema {
//         self.schema.as_ref()
//     }

//     /// returns all expressions (non-recursively) in the current logical plan
// node.     fn expressions(&self) -> Vec<Expr> {
//         // The predicate expression gets absorbed by this node As
//         // there are no inputs, there are no exprs that operate on
//         // inputs
//         Vec::new()
//     }

//     /// Write a single line human readable string to `f` for use in explain
// plan     fn format_for_explain(&self, f: &mut fmt::Formatter<'_>) ->
// fmt::Result {         write!(
//             f,
//             "SegmentScan: {:?} predicate {:?}",
//             self.store.as_ref() as *const Store,
//             self.predicate
//         )
//     }

//     /// Create a clone of this node.
//     ///
//     /// Note std::Clone needs a Sized type, so we must implement a
//     /// clone that creates a node with a known Size (i.e. Box)
//     //
//     fn dyn_clone(&self) -> Box<dyn LogicalPlanNode> {
//         Box::new(SegmentScan::new(self.store.clone(),
// self.predicate.clone()))     }

//     /// Create a clone of this LogicalPlanNode with inputs and expressions
// replaced.     ///
//     /// Note that exprs and inputs are in the same order as the result
//     /// of self.inputs and self.exprs.
//     ///
//     /// So, clone_from_template(exprs).exprs() == exprs
//     fn clone_from_template(
//         &self,
//         exprs: &Vec<Expr>,
//         inputs: &Vec<LogicalPlan>,
//     ) -> Box<dyn LogicalPlanNode> {
//         assert_eq!(exprs.len(), 0, "no exprs expected");
//         assert_eq!(inputs.len(), 0, "no inputs expected");
//         Box::new(SegmentScan::new(self.store.clone(),
// self.predicate.clone()))     }

//     /// Create the corresponding physical scheplan for this node
//     fn create_physical_plan(
//         &self,
//         input_physical_plans: Vec<Arc<dyn ExecutionPlan>>,
//         _ctx_state: Arc<Mutex<ExecutionContextState>>,
//     ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
//         assert_eq!(input_physical_plans.len(), 0, "Can not have inputs");

//         // If this were real code, we would now progrmatically
//         // transform the DataFusion Expr into the specific form needed
//         // by the Segment. However, to save prototype time we just
//         // hard code it here instead
//         assert_eq!(
//             format!("{:?}", self.predicate),
//             "CAST(#time AS Int64) GtEq Int64(1590036110000000) And CAST(#time
// AS Int64) Lt Int64(1590040770000000) And #env Eq
// Utf8(\"prod01-eu-central-1\")"         );

//         let time_range = (1590036110000000, 1590040770000000);
//         let string_predicate = StringPredicate {
//             col_name: "env".into(),
//             value: "prod01-eu-central-1".into(),
//         };

//         Ok(Arc::new(SegmentScanExec::new(
//             self.store.clone(),
//             time_range,
//             string_predicate,
//         )))
//     }
// }

// #[derive(Debug, Clone)]
// struct StringPredicate {
//     col_name: String,
//     value: String,
// }

// /// StoreScan execution node
// #[derive(Debug)]
// pub struct SegmentScanExec {
//     store: Arc<Store>,

//     // Specialized predicates to apply
//     time_range: (i64, i64),
//     string_predicate: StringPredicate,
// }

// impl SegmentScanExec {
//     fn new(store: Arc<Store>, time_range: (i64, i64), string_predicate:
// StringPredicate) -> Self {         SegmentScanExec {
//             store,
//             time_range,
//             string_predicate,
//         }
//     }
// }

// impl ExecutionPlan for SegmentScanExec {
//     fn schema(&self) -> SchemaRef {
//         self.store.schema()
//     }

//     fn partitions(&self) -> datafusion::error::Result<Vec<Arc<dyn
// Partitioning>>> {         let store = self.store.clone();
//         Ok(vec![Arc::new(SegmentPartition {
//             store,
//             time_range: self.time_range,
//             string_predicate: self.string_predicate.clone(),
//         })])
//     }
// }

// #[derive(Debug)]
// struct SegmentPartition {
//     store: Arc<Store>,
//     time_range: (i64, i64),
//     string_predicate: StringPredicate,
// }

// impl Partition for SegmentPartition {
//     fn execute(
//         &self,
//     ) -> datafusion::error::Result<Arc<Mutex<dyn
// RecordBatchReader + Send + Sync>>>     {
//         let combined_results: Vec<Arc<RecordBatch>> = vec![];

//         let segments = self.store.segments();

//         // prepare the string predicates in the manner Segments want them
//         let col_name = &self.string_predicate.col_name;
//         let scalar = column::Scalar::String(&self.string_predicate.value);

//         // Here
//         let _columns = segments.read_filter_eq(
//             self.time_range,
//             &[(col_name, Some(scalar))],
//             vec![
//                 "env".to_string(),
//                 "method".to_string(),
//                 "host".to_string(),
//                 "counter".to_string(),
//                 "time".to_string(),
//             ],
//         );

//         // If we were implementing this for real, we would not convert
//         // `columns` into RecordBatches and feed them back out

//         Ok(Arc::new(Mutex::new(RecordBatchIterator::new(
//             self.store.schema().clone(),
//             combined_results,
//         ))))
//     }
// }
