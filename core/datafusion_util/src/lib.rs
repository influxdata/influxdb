//! This module contains various DataFusion utility functions.
//!
//! Almost everything for manipulating DataFusion `Expr`s IOx should be in DataFusion already
//! (or if not it should be upstreamed).
//!
//! For example, check out
//! [datafusion_optimizer::utils](https://docs.rs/datafusion-optimizer/13.0.0/datafusion_optimizer/utils/index.html)
//! for expression manipulation functions.

use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchemaRef, Result};
use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use datafusion::logical_expr::{Distinct, Extension, LogicalPlan, UserDefinedLogicalNode};
use std::cmp::Ordering;
use std::collections::HashSet;
// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod config;
pub mod sender;
pub mod watch;

use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, Fields};
use datafusion::common::stats::Precision;
use datafusion::common::{DataFusionError, ToDFSchema};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::utils::inspect_expr_pre;
use datafusion::logical_expr::{SortExpr, expr::Sort};
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::{EmptyRecordBatchStream, ExecutionPlan, collect};
use datafusion::prelude::{Column, Expr, SessionContext, lit};
use datafusion::{
    arrow::{
        datatypes::{Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    physical_plan::{RecordBatchStream, SendableRecordBatchStream},
    scalar::ScalarValue,
};
use futures::{Stream, StreamExt};
use schema::TIME_DATA_TIMEZONE;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver};
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use watch::WatchedTask;

/// Traits to help creating DataFusion [`Expr`]s
pub trait AsExpr {
    /// Creates a DataFusion expr
    fn as_expr(&self) -> Expr;

    /// creates a DataFusion SortExpr
    fn as_sort_expr(&self) -> SortExpr {
        Sort::new(
            self.as_expr(),
            true, // Sort ASCENDING
            true,
        )
    }
}

impl AsExpr for Arc<str> {
    fn as_expr(&self) -> Expr {
        self.as_ref().as_expr()
    }
}

impl AsExpr for str {
    fn as_expr(&self) -> Expr {
        // note using `col(<ident>)` will parse identifiers and try to
        // split them on `.`.
        //
        // So it would treat 'foo.bar' as table 'foo', column 'bar'
        //
        // This is not correct for influxrpc, so instead treat it
        // like the column "foo.bar"
        Expr::Column(Column {
            relation: None,
            name: self.into(),
            spans: Default::default(),
        })
    }
}

impl AsExpr for Expr {
    fn as_expr(&self) -> Expr {
        self.clone()
    }
}

/// Creates an `Expr` that represents a Dictionary encoded string (e.g
/// the type of constant that a tag would be compared to)
pub fn lit_dict(value: impl Into<String>) -> Expr {
    lit(dict(value))
}

/// Creates an `ScalarValue` that represents a Dictionary encoded string (e.g
/// the type of constant that a tag would be compared to)
pub fn dict(value: impl Into<String>) -> ScalarValue {
    // expr has been type coerced
    ScalarValue::Dictionary(
        Box::new(DataType::Int32),
        Box::new(ScalarValue::new_utf8(value)),
    )
}

/// Creates expression like:
/// start <= time && time < end
pub fn make_range_expr(start: i64, end: i64, time: impl AsRef<str>) -> Expr {
    // We need to cast the start and end values to timestamps
    // the equivalent of:
    let ts_start = timestamptz_nano(start);
    let ts_end = timestamptz_nano(end);

    let time_col = time.as_ref().as_expr();
    let ts_low = lit(ts_start).lt_eq(time_col.clone());
    let ts_high = time_col.lt(lit(ts_end));

    ts_low.and(ts_high)
}

/// Ensures all columns referred to in `filters` are in the `projection`, if
/// any, adding them if necessary.
pub fn extend_projection_for_filters(
    schema: &Schema,
    filters: &[Expr],
    projection: Option<&Vec<usize>>,
) -> Result<Option<Vec<usize>>, DataFusionError> {
    let Some(mut projection) = projection.cloned() else {
        return Ok(None);
    };

    let mut seen_cols: HashSet<usize> = projection.iter().cloned().collect();
    for filter in filters {
        inspect_expr_pre(filter, |expr| {
            if let Expr::Column(c) = expr {
                let idx = schema.index_of(&c.name)?;
                // if haven't seen this column before, add it to the list
                if seen_cols.insert(idx) {
                    projection.push(idx);
                }
            }
            Ok(()) as Result<(), DataFusionError>
        })?;
    }
    Ok(Some(projection))
}

// TODO port this upstream to datafusion (maybe as From<Option> for Precision)
/// Maps `Option::Some(T)` to `Precision::Exact(T)` and `Option::None` to
/// `Precision::Absent`
pub fn option_to_precision<T: std::fmt::Debug + Clone + PartialEq + Eq + PartialOrd>(
    option: Option<T>,
) -> Precision<T> {
    match option {
        Some(value) => Precision::Exact(value),
        None => Precision::Absent,
    }
}

/// A RecordBatchStream created from in-memory RecordBatches.
#[derive(Debug)]
pub struct MemoryStream {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl MemoryStream {
    /// Create new stream.
    ///
    /// Must at least pass one record batch!
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        assert!(!batches.is_empty(), "must at least pass one record batch");
        Self {
            schema: batches[0].schema(),
            batches,
        }
    }

    /// Create new stream with provided schema.
    pub fn new_with_schema(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        Self { schema, batches }
    }
}

impl RecordBatchStream for MemoryStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for MemoryStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.batches.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Ready(Some(Ok(self.batches.remove(0))))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.batches.len(), Some(self.batches.len()))
    }
}

#[derive(Debug)]
/// Implements a [`SendableRecordBatchStream`] to help create DataFusion outputs
/// from tokio channels.
///
/// It sends streams of RecordBatches from a tokio channel *and* crucially knows
/// up front the schema each batch will have be used.
pub struct AdapterStream<T> {
    /// Schema
    schema: SchemaRef,
    /// channel for getting deduplicated batches
    inner: T,

    /// Optional join handles of underlying tasks.
    #[expect(dead_code)]
    task: Arc<WatchedTask>,
}

impl AdapterStream<ReceiverStream<Result<RecordBatch, DataFusionError>>> {
    /// Create a new stream which wraps the `inner` channel which produces
    /// [`RecordBatch`]es that each have the specified schema
    ///
    /// Not called `new` because it returns a pinned reference rather than the
    /// object itself.
    pub fn adapt(
        schema: SchemaRef,
        rx: Receiver<Result<RecordBatch, DataFusionError>>,
        task: Arc<WatchedTask>,
    ) -> SendableRecordBatchStream {
        let inner = ReceiverStream::new(rx);
        Box::pin(Self {
            schema,
            inner,
            task,
        })
    }
}

impl AdapterStream<UnboundedReceiverStream<Result<RecordBatch, DataFusionError>>> {
    /// Create a new stream which wraps the `inner` unbounded channel which
    /// produces [`RecordBatch`]es that each have the specified schema
    ///
    /// Not called `new` because it returns a pinned reference rather than the
    /// object itself.
    pub fn adapt_unbounded(
        schema: SchemaRef,
        rx: UnboundedReceiver<Result<RecordBatch, DataFusionError>>,
        task: Arc<WatchedTask>,
    ) -> SendableRecordBatchStream {
        let inner = UnboundedReceiverStream::new(rx);
        Box::pin(Self {
            schema,
            inner,
            task,
        })
    }
}

impl<T> Stream for AdapterStream<T>
where
    T: Stream<Item = Result<RecordBatch, DataFusionError>> + Unpin,
{
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl<T> RecordBatchStream for AdapterStream<T>
where
    T: Stream<Item = Result<RecordBatch, DataFusionError>> + Unpin,
{
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Create a SendableRecordBatchStream a RecordBatch
pub fn stream_from_batch(schema: SchemaRef, batch: RecordBatch) -> SendableRecordBatchStream {
    stream_from_batches(schema, vec![batch])
}

/// Create a SendableRecordBatchStream from Vec of RecordBatches with the same schema
pub fn stream_from_batches(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> SendableRecordBatchStream {
    if batches.is_empty() {
        return Box::pin(EmptyRecordBatchStream::new(schema));
    }
    Box::pin(MemoryStream::new_with_schema(batches, schema))
}

/// Helper trait for implementing `partial_cmp` for nested types.
///
/// Example
/// ```rust
/// use std::cmp::Ordering;
/// use datafusion_util::ThenWithOpt;
///
/// // Struct has two fields, lets pretend one can't be compared
/// #[derive(Debug, PartialEq)]
/// struct Foo {
///    a: i32,
///    b: i32, // pretend this can't be compared
///    c: i32,
/// }
///
/// impl PartialOrd for Foo {
///    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
///       self.a.partial_cmp(&other.a)
///          // only compare c if a is equal
///          .then_with_opt(|| self.c.partial_cmp(&other.c))
///   }
/// }
///
/// let foo1 = Foo { a: 1, b: 2, c: 3 };
/// let foo2 = Foo { a: 1, b: 4, c: 5 };
/// let foo3 = Foo { a: 1, b: 4, c: 6 };
/// let foo4 = Foo { a: 2, b: 4, c: 6 };
///
/// assert_eq!(foo1.partial_cmp(&foo1), Some(Ordering::Equal));
/// assert_eq!(foo1.partial_cmp(&foo2), Some(Ordering::Less));
/// assert_eq!(foo2.partial_cmp(&foo3), Some(Ordering::Less));
/// assert_eq!(foo4.partial_cmp(&foo1), Some(Ordering::Greater));
/// ```
pub trait ThenWithOpt {
    /// Invoke the closure if the ordering is equal, otherwise return the ordering.
    fn then_with_opt<F: FnOnce() -> Self>(self, f: F) -> Self;
}

impl ThenWithOpt for Option<Ordering> {
    fn then_with_opt<F: FnOnce() -> Self>(self, f: F) -> Self {
        match self {
            Some(Ordering::Equal) => f(),
            other => other,
        }
    }
}

/// Execute the [ExecutionPlan] with a default [SessionContext] and
/// collect the results in memory.
///
/// # Panics
/// If an an error occurs
pub async fn test_collect(plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    let session_ctx = SessionContext::new();
    let task_ctx = Arc::new(TaskContext::from(&session_ctx));
    collect(plan, task_ctx).await.unwrap()
}

/// Execute the specified partition of the [ExecutionPlan] with a
/// default [SessionContext] returning the resulting stream.
///
/// # Panics
/// If an an error occurs
pub async fn test_execute_partition(
    plan: Arc<dyn ExecutionPlan>,
    partition: usize,
) -> SendableRecordBatchStream {
    let session_ctx = SessionContext::new();
    let task_ctx = Arc::new(TaskContext::from(&session_ctx));
    plan.execute(partition, task_ctx).unwrap()
}

/// Execute the specified partition of the [ExecutionPlan] with a
/// default [SessionContext] and collect the results in memory.
///
/// # Panics
/// If an an error occurs
pub async fn test_collect_partition(
    plan: Arc<dyn ExecutionPlan>,
    partition: usize,
) -> Vec<RecordBatch> {
    let stream = test_execute_partition(plan, partition).await;
    datafusion::physical_plan::common::collect(stream)
        .await
        .unwrap()
}

/// Filter data from RecordBatch
///
/// Borrowed from DF's <https://github.com/apache/arrow-datafusion/blob/ecd0081bde98e9031b81aa6e9ae2a4f309fcec12/datafusion/src/physical_plan/filter.rs#L186>.
// TODO: if we make DF batch_filter public, we can call that function directly
pub fn batch_filter(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
) -> Result<RecordBatch, DataFusionError> {
    predicate
        .evaluate(batch)
        .and_then(|v| v.into_array(batch.num_rows()))
        .and_then(|array| {
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Filter predicate evaluated to non-boolean value".to_string(),
                    )
                })
                // apply filter array to record batch
                .and_then(|filter_array| {
                    filter_record_batch(batch, filter_array)
                        .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))
                })
        })
}

/// Returns a new schema where all the fields are nullable
pub fn nullable_schema(schema: SchemaRef) -> SchemaRef {
    // they are all already nullable
    if schema.fields().iter().all(|f| f.is_nullable()) {
        schema
    } else {
        // make a new schema with all nullable fields
        let new_fields: Fields = schema
            .fields()
            .iter()
            .map(|f| {
                // make a copy of the field, but allow it to be nullable
                f.as_ref().clone().with_nullable(true)
            })
            .collect();

        Arc::new(Schema::new_with_metadata(
            new_fields,
            schema.metadata().clone(),
        ))
    }
}

/// Returns a [`PhysicalExpr`] from the logical [`Expr`] and Arrow [`SchemaRef`]
pub fn create_physical_expr_from_schema(
    props: &ExecutionProps,
    expr: &Expr,
    schema: &SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let df_schema = Arc::clone(schema).to_dfschema_ref()?;
    create_physical_expr(expr, df_schema.as_ref(), props)
}

/// Returns a [`PruningPredicate`] from the logical [`Expr`] and Arrow [`SchemaRef`]
pub fn create_pruning_predicate(
    props: &ExecutionProps,
    expr: &Expr,
    schema: &SchemaRef,
) -> Result<PruningPredicate, DataFusionError> {
    let expr = create_physical_expr_from_schema(props, expr, schema)?;
    PruningPredicate::try_new(expr, Arc::clone(schema))
}

/// Create a memory pool that has no limit
pub fn unbounded_memory_pool() -> Arc<dyn MemoryPool> {
    Arc::new(UnboundedMemoryPool::default())
}

/// Create a timestamp literal for the given UTC nanosecond offset in
/// the timezone specified by [TIME_DATA_TIMEZONE].
///
/// N.B. If [TIME_DATA_TIMEZONE] specifies the None timezone then this
/// function behaves identially to [datafusion::prelude::lit_timestamp_nano].
pub fn lit_timestamptz_nano(ns: i64) -> Expr {
    lit(timestamptz_nano(ns))
}

/// Create a scalar timestamp value for the given UTC nanosecond offset
/// in the timezone specified by [TIME_DATA_TIMEZONE].
pub fn timestamptz_nano(ns: i64) -> ScalarValue {
    ScalarValue::TimestampNanosecond(Some(ns), TIME_DATA_TIMEZONE())
}

/// Transform the schema of a single [`LogicalPlan`] node by applying a function to its schema.
///
/// This function applies the transformation function `f` to the schema field of the given
/// plan node, without recursing into child nodes. It returns a [`Transformed`] result
/// indicating whether the plan was modified and how tree traversal should proceed when
/// used with DataFusion's tree traversal APIs.
///
/// # Parameters
///
/// * `plan` - The logical plan node to transform
/// * `e` - A mutable closure that handles extension nodes, receiving the
///   user-defined logical node and a mutable reference to the schema transformation
///   function `f`. It should return a [`Transformed`] result with the potentially modified
///   [`UserDefinedLogicalNode`].
/// * `f` - A mutable closure that transforms a [`DFSchemaRef`] and returns a [`Transformed`]
///   result indicating whether the schema was modified
///
/// # Returns
///
/// Returns a [`Transformed<LogicalPlan>`] that contains:
/// - The plan node with its schema potentially modified
/// - A boolean indicating whether any transformation occurred
/// - A [`TreeNodeRecursion`](datafusion::common::tree_node::TreeNodeRecursion)
///   value controlling tree traversal (typically `Continue`, or `Jump`
///   for extension nodes to avoid recursing into their internals)
///
/// # Supported Plan Nodes
///
/// This function handles schema transformation for the following plan node types:
/// - `Projection`, `Window`, `Aggregate`, `Join`, `Union` - Core query operators
/// - `TableScan`, `EmptyRelation`, `SubqueryAlias`, `Values` - Data sources
/// - `Explain`, `Analyze` - Query analysis nodes
/// - `Dml`, `Copy`, `DescribeTable` - Data manipulation and metadata nodes
/// - `Unnest` - Array expansion operator
/// - `Distinct::On` - Distinct operator with ON clause
///
/// Extension plan nodes are handled via the provided closure `e`.
///
/// Plan nodes without schemas (`Filter`, `Sort`, `Limit`, etc.) return unchanged with
/// `Transformed::no(plan)`.
///
/// # Errors
///
/// Returns an error if:
/// - The transformation function `f` returns an error
/// - The extension handler `e` returns an error
///
/// # Example
///
/// ```ignore
/// use datafusion::common::tree_node::Transformed;
///
/// // Transform the schema of the first node with an attached schema
/// let plan = ...; // some LogicalPlan
/// let plan = plan.transform_down(|plan| {
///     transform_plan_schema(
///         plan,
///         |node, _| Ok(Transformed::no(node)),
///         |schema| {
///             let qualified_fields = schema.iter().map(|(q, f)| (q.cloned(), Arc::clone(f))).collect();
///             let mut md = schema.metadata().clone();
///             md.insert("key".to_string(), "value".to_string());
///             let new_schema = DFSchema::new_with_metadata(qualified_fields, md)?;
///             Ok(Transformed::new(Arc::new(new_schema), true, TreeNodeRecursion::Jump))
///         },
///     )
/// })?;
/// ```
///
/// N.B. This function makes no attempt to validate the correctness of
/// the modified schema. The intended use-case is for modifying schema
/// metadata. Changes to the schema information used by DataFusion may
/// result in incorrect query plans or runtime errors.
pub fn transform_plan_schema<E, F>(
    plan: LogicalPlan,
    mut e: E,
    mut f: F,
) -> Result<Transformed<LogicalPlan>>
where
    E: FnMut(
        Arc<dyn UserDefinedLogicalNode>,
        &mut dyn FnMut(DFSchemaRef) -> Result<Transformed<DFSchemaRef>>,
    ) -> Result<Transformed<Arc<dyn UserDefinedLogicalNode>>>,
    F: FnMut(DFSchemaRef) -> Result<Transformed<DFSchemaRef>>,
{
    match plan {
        LogicalPlan::Projection(mut projection) => {
            f(Arc::clone(&projection.schema))?.map_data(|data| {
                projection.schema = data;
                Ok(LogicalPlan::Projection(projection))
            })
        }
        LogicalPlan::Window(mut window) => f(Arc::clone(&window.schema))?.map_data(|data| {
            window.schema = data;
            Ok(LogicalPlan::Window(window))
        }),
        LogicalPlan::Aggregate(mut aggregate) => {
            f(Arc::clone(&aggregate.schema))?.map_data(|data| {
                aggregate.schema = data;
                Ok(LogicalPlan::Aggregate(aggregate))
            })
        }
        LogicalPlan::Join(mut join) => f(Arc::clone(&join.schema))?.map_data(|data| {
            join.schema = data;
            Ok(LogicalPlan::Join(join))
        }),
        LogicalPlan::Union(mut union) => f(Arc::clone(&union.schema))?.map_data(|data| {
            union.schema = data;
            Ok(LogicalPlan::Union(union))
        }),
        LogicalPlan::TableScan(mut table_scan) => f(Arc::clone(&table_scan.projected_schema))?
            .map_data(|data| {
                table_scan.projected_schema = data;
                Ok(LogicalPlan::TableScan(table_scan))
            }),
        LogicalPlan::EmptyRelation(mut empty_relation) => f(Arc::clone(&empty_relation.schema))?
            .map_data(|data| {
                empty_relation.schema = data;
                Ok(LogicalPlan::EmptyRelation(empty_relation))
            }),
        LogicalPlan::SubqueryAlias(mut subquery_alias) => f(Arc::clone(&subquery_alias.schema))?
            .map_data(|data| {
                subquery_alias.schema = data;
                Ok(LogicalPlan::SubqueryAlias(subquery_alias))
            }),
        LogicalPlan::Values(mut values) => f(Arc::clone(&values.schema))?.map_data(|data| {
            values.schema = data;
            Ok(LogicalPlan::Values(values))
        }),
        LogicalPlan::Explain(mut explain) => f(Arc::clone(&explain.schema))?.map_data(|data| {
            explain.schema = data;
            Ok(LogicalPlan::Explain(explain))
        }),
        LogicalPlan::Analyze(mut analyze) => f(Arc::clone(&analyze.schema))?.map_data(|data| {
            analyze.schema = data;
            Ok(LogicalPlan::Analyze(analyze))
        }),
        LogicalPlan::Extension(Extension { node }) => {
            e(node, &mut f)?.map_data(|node| Ok(LogicalPlan::Extension(Extension { node })))
        }
        LogicalPlan::Distinct(Distinct::On(mut distinct_on)) => f(Arc::clone(&distinct_on.schema))?
            .map_data(|data| {
                distinct_on.schema = data;
                Ok(LogicalPlan::Distinct(Distinct::On(distinct_on)))
            }),
        LogicalPlan::Dml(mut dml_statement) => f(Arc::clone(&dml_statement.output_schema))?
            .map_data(|data| {
                dml_statement.output_schema = data;
                Ok(LogicalPlan::Dml(dml_statement))
            }),
        LogicalPlan::Copy(mut copy_to) => f(Arc::clone(&copy_to.output_schema))?.map_data(|data| {
            copy_to.output_schema = data;
            Ok(LogicalPlan::Copy(copy_to))
        }),
        LogicalPlan::DescribeTable(mut describe_table) => {
            f(Arc::clone(&describe_table.output_schema))?.map_data(|data| {
                describe_table.output_schema = data;
                Ok(LogicalPlan::DescribeTable(describe_table))
            })
        }
        LogicalPlan::Unnest(mut unnest) => f(Arc::clone(&unnest.schema))?.map_data(|data| {
            unnest.schema = data;
            Ok(LogicalPlan::Unnest(unnest))
        }),

        // Plans that do not contain a schema are unchanged.
        plan @ (LogicalPlan::Filter(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Statement(_)
        | LogicalPlan::Distinct(Distinct::All(_))
        | LogicalPlan::Ddl(_)
        | LogicalPlan::RecursiveQuery(_)) => Ok(Transformed::no(plan)),
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::DFSchema;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::functions_window::row_number::row_number_udwf;
    use datafusion::logical_expr::expr::WindowFunction;
    use datafusion::logical_expr::test::function_stub::count;
    use datafusion::logical_expr::{
        Expr, LogicalPlanBuilder, LogicalTableSource, Projection, WindowFunctionDefinition, col,
        lit,
    };
    use schema::builder::SchemaBuilder;

    use super::*;

    #[test]
    fn test_make_range_expr() {
        // Test that the generated predicate is correct

        let ts_predicate_expr = make_range_expr(101, 202, "time");
        let expected_timezone = match TIME_DATA_TIMEZONE() {
            Some(tz) => format!("Some(\"{tz}\")"),
            None => "None".into(),
        };
        let expected_string = format!(
            "TimestampNanosecond(101, {expected_timezone}) <= time AND time < TimestampNanosecond(202, {expected_timezone})"
        );
        let actual_string = format!("{ts_predicate_expr}");

        assert_eq!(actual_string, expected_string);
    }

    #[test]
    fn test_nullable_schema_nullable() {
        // schema is all nullable
        let schema = Arc::new(Schema::new(vec![
            Field::new("foo", DataType::Int32, true),
            Field::new("bar", DataType::Utf8, true),
        ]));

        assert_eq!(schema, nullable_schema(Arc::clone(&schema)))
    }

    #[test]
    fn test_nullable_schema_non_nullable() {
        // schema has one nullable column
        let schema = Arc::new(Schema::new(vec![
            Field::new("foo", DataType::Int32, false),
            Field::new("bar", DataType::Utf8, true),
        ]));

        let expected_schema = Arc::new(Schema::new(vec![
            Field::new("foo", DataType::Int32, true),
            Field::new("bar", DataType::Utf8, true),
        ]));

        assert_eq!(expected_schema, nullable_schema(schema))
    }

    #[tokio::test]
    async fn test_adapter_stream_panic_handling() {
        let schema = SchemaBuilder::new().timestamp().build().unwrap().as_arrow();
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let tx_captured = tx.clone();
        let fut = async move {
            let _tx = tx_captured;
            if true {
                panic!("epic fail");
            }

            Ok(())
        };
        let join_handle = WatchedTask::new(fut, vec![tx], "test");
        let stream = AdapterStream::adapt(schema, rx, join_handle);
        datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap_err();
    }

    /// Helper function to create a simple test input.
    fn create_test_source() -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        LogicalPlanBuilder::scan("test", Arc::new(LogicalTableSource::new(schema)), None)
            .unwrap()
            .build()
            .unwrap()
    }

    /// Helper function to create a modified schema with different metadata
    fn add_metadata_to_schema(schema: DFSchemaRef, key: &str, value: &str) -> Result<DFSchemaRef> {
        let mut metadata = schema.metadata().clone();
        metadata.insert(key.to_string(), value.to_string());
        Ok(Arc::new(DFSchema::new_with_metadata(
            schema
                .iter()
                .map(|(q, f)| (q.cloned(), Arc::clone(f)))
                .collect(),
            metadata,
        )?))
    }

    /// Helper function to apply schema transformation to a plan and return the transformed plan
    fn do_transform_plan_schema(plan: LogicalPlan) -> LogicalPlan {
        let Transformed {
            data, transformed, ..
        } = plan
            .transform_down(|plan| {
                transform_plan_schema(
                    plan,
                    |n, _| Ok(Transformed::no(n)),
                    |s| {
                        let new_schema = add_metadata_to_schema(s, "transformed", "true")?;
                        Ok(Transformed::new(new_schema, true, TreeNodeRecursion::Stop))
                    },
                )
            })
            .unwrap();
        assert_eq!(
            data.schema().as_arrow().metadata().get("transformed"),
            Some(&"true".to_string())
        );
        assert!(transformed);
        data
    }

    #[test]
    fn test_transform_projection() {
        let input = create_test_source();
        let schema = Arc::clone(input.schema());
        // Create expressions that match the schema fields
        let exprs = vec![
            datafusion::logical_expr::col("id"),
            datafusion::logical_expr::col("name"),
        ];
        let projection = LogicalPlan::Projection(
            Projection::try_new_with_schema(exprs, Arc::new(input), schema).unwrap(),
        );
        assert_matches!(
            do_transform_plan_schema(projection),
            LogicalPlan::Projection(_)
        );
    }

    #[test]
    fn test_transform_empty_relation() {
        assert_matches!(
            do_transform_plan_schema(LogicalPlanBuilder::empty(false).build().unwrap()),
            LogicalPlan::EmptyRelation(_)
        );
    }

    #[test]
    fn test_transform_filter() {
        // Create a simple plan with a filter node (which has no schema)
        let input = create_test_source();
        let filter = LogicalPlanBuilder::from(input)
            .filter(lit(true))
            .unwrap()
            .build()
            .unwrap();
        assert_matches!(do_transform_plan_schema(filter), LogicalPlan::Filter(_));
    }

    #[test]
    fn test_transform_sort() {
        let input = create_test_source();
        let sort = LogicalPlanBuilder::from(input)
            .sort([col("id").sort(true, false)])
            .unwrap()
            .build()
            .unwrap();
        assert_matches!(do_transform_plan_schema(sort), LogicalPlan::Sort(_));
    }

    #[test]
    fn test_transform_limit() {
        let input = create_test_source();
        let limit = LogicalPlanBuilder::from(input)
            .limit(0, Some(10))
            .unwrap()
            .build()
            .unwrap();
        assert_matches!(do_transform_plan_schema(limit), LogicalPlan::Limit(_));
    }

    #[test]
    fn test_transform_aggregate() {
        let input = create_test_source();

        // Create a simple aggregate plan
        let aggregate = LogicalPlanBuilder::from(input)
            .aggregate(vec![col("id")], vec![count(col("name"))])
            .unwrap()
            .build()
            .unwrap();
        assert_matches!(
            do_transform_plan_schema(aggregate),
            LogicalPlan::Aggregate(_)
        );
    }

    #[test]
    fn test_transform_union() {
        let input1 = create_test_source();
        let input2 = LogicalPlanBuilder::scan(
            "test2",
            Arc::new(LogicalTableSource::new(Arc::new(
                input1.schema().as_arrow().clone(),
            ))),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        // Create a union plan
        let union = datafusion::logical_expr::union(input1, input2).unwrap();
        assert_matches!(do_transform_plan_schema(union), LogicalPlan::Union(_));
    }

    #[test]
    fn test_transform_window() {
        let input = create_test_source();

        // Create a window plan
        let window_expr = Expr::WindowFunction(Box::new(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(row_number_udwf()),
            vec![],
        )));

        let window = LogicalPlanBuilder::from(input)
            .window(vec![window_expr])
            .unwrap()
            .build()
            .unwrap();

        assert_matches!(do_transform_plan_schema(window), LogicalPlan::Window(_));
    }

    #[test]
    fn test_transform_join() {
        let left = create_test_source();
        let right = LogicalPlanBuilder::scan(
            "test2",
            Arc::new(LogicalTableSource::new(Arc::new(
                left.schema().as_arrow().clone(),
            ))),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        // Create a cross join (no join keys needed)
        let join = LogicalPlanBuilder::from(left)
            .cross_join(right)
            .unwrap()
            .build()
            .unwrap();

        assert_matches!(do_transform_plan_schema(join), LogicalPlan::Join(_));
    }

    #[test]
    fn test_transform_subquery_alias() {
        let input = create_test_source();
        // Create a subquery alias
        let subquery = LogicalPlanBuilder::from(input)
            .alias("subquery")
            .unwrap()
            .build()
            .unwrap();

        assert_matches!(
            do_transform_plan_schema(subquery),
            LogicalPlan::SubqueryAlias(_)
        );
    }

    #[test]
    fn test_transform_values() {
        // Create a values plan
        let values = LogicalPlanBuilder::values(vec![vec![lit(1), lit("test")]])
            .unwrap()
            .build()
            .unwrap();
        assert_matches!(do_transform_plan_schema(values), LogicalPlan::Values(_));
    }

    #[test]
    fn test_transform_distinct_on() {
        let input = create_test_source();

        // Create a distinct on plan
        let distinct = LogicalPlanBuilder::from(input)
            .distinct_on(vec![col("id")], vec![], None)
            .unwrap()
            .build()
            .unwrap();
        assert_matches!(do_transform_plan_schema(distinct), LogicalPlan::Distinct(_));
    }

    #[test]
    fn test_transform_repartition() {
        use datafusion::logical_expr::Partitioning;

        let input = create_test_source();

        // Create a repartition plan
        let repartition = LogicalPlanBuilder::from(input)
            .repartition(Partitioning::RoundRobinBatch(4))
            .unwrap()
            .build()
            .unwrap();
        assert_matches!(
            do_transform_plan_schema(repartition),
            LogicalPlan::Repartition(_)
        );
    }

    #[test]
    fn test_transform_distinct_all() {
        let input = create_test_source();

        // Create a distinct all plan
        let distinct = LogicalPlanBuilder::from(input)
            .distinct()
            .unwrap()
            .build()
            .unwrap();

        assert_matches!(
            do_transform_plan_schema(distinct),
            LogicalPlan::Distinct(Distinct::All(_))
        );
    }

    #[test]
    fn test_transform_unnest() {
        // Create a schema with an array column
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]));
        let input =
            LogicalPlanBuilder::scan("test", Arc::new(LogicalTableSource::new(schema)), None)
                .unwrap()
                .build()
                .unwrap();

        // Create an unnest plan on the array column
        let unnest = LogicalPlanBuilder::from(input)
            .unnest_column("tags")
            .unwrap()
            .build()
            .unwrap();

        assert_matches!(do_transform_plan_schema(unnest), LogicalPlan::Unnest(_));
    }

    #[test]
    fn test_transform_table_scan() {
        let input = create_test_source();
        assert_matches!(do_transform_plan_schema(input), LogicalPlan::TableScan(_));
    }

    #[test]
    fn test_transform_explain() {
        let input = create_test_source();

        // Create an explain plan
        let explain = LogicalPlanBuilder::from(input)
            .explain(false, false)
            .unwrap()
            .build()
            .unwrap();

        assert_matches!(do_transform_plan_schema(explain), LogicalPlan::Explain(_));
    }

    #[test]
    fn test_transform_analyze() {
        let input = create_test_source();

        // Create an analyze plan
        let analyze = LogicalPlanBuilder::from(input)
            .explain(false, true)
            .unwrap()
            .build()
            .unwrap();

        assert_matches!(do_transform_plan_schema(analyze), LogicalPlan::Analyze(_));
    }

    #[test]
    fn test_transform_subquery() {
        use datafusion::common::Spans;
        use datafusion::logical_expr::{Subquery, expr::InSubquery};

        let input = create_test_source();

        // Create a filter with a subquery (Subquery node has no schema, so transform_down recurses to child)
        let subquery = Subquery {
            subquery: Arc::new(input),
            outer_ref_columns: vec![],
            spans: Spans::new(),
        };

        let filter_plan = LogicalPlanBuilder::from(create_test_source())
            .filter(Expr::InSubquery(InSubquery::new(
                Box::new(col("id")),
                subquery,
                false,
            )))
            .unwrap()
            .build()
            .unwrap();

        // The transformation should recurse through Subquery and transform the child TableScan
        let transformed = do_transform_plan_schema(filter_plan);
        assert_matches!(transformed, LogicalPlan::Filter(_));
    }

    #[test]
    fn test_transform_dml() {
        use datafusion::logical_expr::dml::{DmlStatement, InsertOp, WriteOp};

        let input = create_test_source();

        // Create a DML statement
        let dml = LogicalPlan::Dml(DmlStatement::new(
            "test".into(),
            Arc::new(LogicalTableSource::new(Arc::new(
                input.schema().as_arrow().clone(),
            ))),
            WriteOp::Insert(InsertOp::Append),
            Arc::new(input),
        ));

        assert_matches!(do_transform_plan_schema(dml), LogicalPlan::Dml(_));
    }

    #[test]
    fn test_transform_copy() {
        use datafusion::datasource::file_format::format_as_file_type;
        use datafusion::datasource::file_format::parquet::ParquetFormatFactory;
        use datafusion::logical_expr::dml::CopyTo;
        use std::collections::HashMap;

        let input = create_test_source();

        // Create a Copy plan
        let copy = LogicalPlan::Copy(CopyTo::new(
            Arc::new(input),
            "file:///tmp/output.parquet".to_string(),
            vec![],
            format_as_file_type(Arc::new(ParquetFormatFactory::new())),
            HashMap::new(),
        ));

        assert_matches!(do_transform_plan_schema(copy), LogicalPlan::Copy(_));
    }

    #[test]
    fn test_transform_describe_table() {
        use datafusion::logical_expr::DescribeTable;

        let input = create_test_source();
        let arrow_schema = Arc::new(input.schema().as_arrow().clone());

        // Create a DescribeTable plan with a simple output schema
        let output_fields = vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
        ];
        let output_schema = Arc::new(Schema::new(output_fields));
        let output_df_schema =
            DFSchema::try_from_qualified_schema("describe", &output_schema).unwrap();

        let describe = LogicalPlan::DescribeTable(DescribeTable {
            schema: arrow_schema,
            output_schema: Arc::new(output_df_schema),
        });

        assert_matches!(
            do_transform_plan_schema(describe),
            LogicalPlan::DescribeTable(_)
        );
    }

    #[test]
    fn test_transform_statement() {
        // Create a Statement plan (no schema, so transform_down recurses to child)
        // Use SetVariable as a simple Statement variant
        let statement = LogicalPlan::Statement(datafusion::logical_expr::Statement::SetVariable(
            datafusion::logical_expr::SetVariable {
                variable: "test_var".to_string(),
                value: "test_value".to_string(),
            },
        ));

        // Statement nodes don't have schemas, so the transformation returns them unchanged
        let Transformed {
            data, transformed, ..
        } = statement
            .transform_down(|plan| {
                transform_plan_schema(
                    plan,
                    |e, _| Ok(Transformed::no(e)),
                    |s| {
                        let new_schema = add_metadata_to_schema(s, "transformed", "true")?;
                        Ok(Transformed::new(new_schema, true, TreeNodeRecursion::Stop))
                    },
                )
            })
            .unwrap();

        // Statement itself has no schema to transform, so transformed should be false
        assert!(!transformed);
        assert_matches!(data, LogicalPlan::Statement(_));
    }

    #[test]
    fn test_transform_ddl() {
        use datafusion::common::Constraints;
        use datafusion::logical_expr::{CreateMemoryTable, DdlStatement};

        let input = create_test_source();

        // Create a DDL plan (no schema, so transform_down recurses to child)
        let ddl = LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
            name: "test_table".into(),
            constraints: Constraints::new_unverified(vec![]),
            input: Arc::new(input),
            if_not_exists: false,
            or_replace: false,
            column_defaults: vec![],
            temporary: false,
        }));

        // The transformation should recurse through DDL and transform the child TableScan
        let transformed = do_transform_plan_schema(ddl);
        assert_matches!(transformed, LogicalPlan::Ddl(_));
    }

    #[test]
    fn test_transform_recursive_query() {
        use datafusion::logical_expr::RecursiveQuery;

        let input = create_test_source();

        // Create a RecursiveQuery plan (no schema field in transform, so transform_down recurses to children)
        let recursive = LogicalPlan::RecursiveQuery(RecursiveQuery {
            name: "recursive_table".to_string(),
            static_term: Arc::new(input.clone()),
            recursive_term: Arc::new(input),
            is_distinct: false,
        });

        // The transformation should recurse through RecursiveQuery and transform the child TableScans
        let transformed = do_transform_plan_schema(recursive);
        assert_matches!(transformed, LogicalPlan::RecursiveQuery(_));
    }
}
