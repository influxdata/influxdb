//! Physical executor for the series limit operation.

use std::{
    collections::BTreeMap,
    ops::Range,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Datum, PrimitiveArray, RecordBatch, Scalar, UInt64Builder,
        new_null_array,
    },
    compute::partition,
    datatypes::{SchemaRef, UInt64Type},
    error::ArrowError,
};
use datafusion::{
    common::tree_node::{TreeNode, TreeNodeRecursion},
    error::{DataFusionError, Result},
    execution::{
        RecordBatchStream, SendableRecordBatchStream,
        context::TaskContext,
        memory_pool::{MemoryConsumer, MemoryReservation},
    },
    physical_expr::{
        EquivalenceProperties, LexOrdering, LexRequirement, OrderingRequirements, PhysicalExprRef,
        PhysicalSortExpr, PhysicalSortRequirement,
    },
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
        PlanProperties, SendableRecordBatchStream as SendableStream, Statistics,
        expressions::Column,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
    },
    scalar::ScalarValue,
};
use futures::{Stream, StreamExt, ready};

#[derive(Debug, Clone)]
pub struct PhysicalLimitExpr {
    /// The expression to evaluate for the limit. This must be a Column.
    expr: PhysicalExprRef,

    /// Whether to ignore null values in the limit calculation.
    ignore_nulls: bool,

    /// The default value to use when a row is filtered out of a time
    /// series, but required in an output batch. Typically this is NULL
    /// of the same type as the expression, however it could be 0 or another
    /// value. When using with InfluxQL this should be the value specified by
    /// the FILL clause mapped to an appropriate type for the column.
    default_value: ScalarValue,
}

impl PhysicalLimitExpr {
    /// Create a new PhysicalLimitExpr.
    pub fn new(expr: PhysicalExprRef, ignore_nulls: bool, default_value: ScalarValue) -> Self {
        Self {
            expr,
            ignore_nulls,
            default_value,
        }
    }
}

impl std::fmt::Display for PhysicalLimitExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}NULLS (default: {})",
            self.expr,
            if self.ignore_nulls {
                "IGNORE "
            } else {
                "RESPECT "
            },
            self.default_value
        )
    }
}

/// Physical execution plan for per-series LIMIT and OFFSET operations.
///
/// This operator implements InfluxQL-style series limiting, which applies
/// LIMIT and OFFSET constraints independently to each time series. Unlike
/// standard SQL LIMIT/OFFSET which apply globally to the result set, this
/// operator applies them separately to each group of rows sharing the same
/// series key (typically tag values).
///
/// # Purpose
///
/// This execution plan is designed to support InfluxQL queries where LIMIT
/// and OFFSET need to apply per-series rather than globally. This is a key
/// semantic difference from SQL that makes InfluxQL suitable for time series
/// data analysis where users want to limit the number of points returned
/// from each individual series independently.
///
/// # Query Semantics
///
/// For a query like:
/// ```sql
/// SELECT value FROM measurement WHERE time > now() - 1h GROUP BY tag LIMIT 10 OFFSET 5
/// ```
///
/// Standard SQL would return 10 rows total across all series. This operator
/// returns up to 10 rows **per series** (after skipping the first 5 rows of
/// each series).
///
/// # Execution Flow
///
/// 1. **Input Requirements**: The input must be pre-sorted by series expressions
///    followed by order expressions (enforced via `required_input_ordering`).
///
/// 2. **Partitioning**: If series expressions are present, the operator requires
///    hash partitioning on those expressions to ensure all rows for a given series
///    are processed by the same partition.
///
/// 3. **Stream Processing**: Each partition creates a [`SeriesLimitStream`] that:
///    - Identifies series boundaries in the sorted input
///    - Assigns row numbers within each series
///    - Filters rows based on skip/fetch values
///    - Maintains state across batch boundaries
///
/// 4. **Output**: Produces a stream of record batches containing only the rows
///    that fall within the LIMIT/OFFSET window for each series.
///
/// # NULL Handling
///
/// The `limit_expr` field supports both RESPECT NULLS and IGNORE NULLS modes:
///
/// - **RESPECT NULLS**: NULL values count toward the row limit
/// - **IGNORE NULLS**: NULL values are skipped and don't count toward the limit
///
/// This is controlled by the `ignore_nulls` field in [`PhysicalLimitExpr`].
///
/// # Default Values
///
/// Each limited expression can specify a default value (in [`PhysicalLimitExpr`])
/// that is used when a row is filtered out. This supports queries where all
/// series need to be time-aligned even when some series have no data for certain
/// timestamps.
///
/// # Examples
///
/// ## Basic LIMIT
/// ```text
/// Input (2 series, 4 rows each):
/// tag | time | value
/// ----|------|------
///  a  |  1   | 10
///  a  |  2   | 20
///  a  |  3   | 30
///  a  |  4   | 40
///  b  |  1   | 50
///  b  |  2   | 60
///  b  |  3   | 70
///  b  |  4   | 80
///
/// With LIMIT 2 (skip=0, fetch=Some(2)):
/// Output:
/// tag | time | value
/// ----|------|------
///  a  |  1   | 10
///  a  |  2   | 20
///  b  |  1   | 50
///  b  |  2   | 60
/// ```
///
/// ## LIMIT with OFFSET
/// ```text
/// Same input as above.
///
/// With LIMIT 2 OFFSET 1 (skip=1, fetch=Some(2)):
/// Output:
/// tag | time | value
/// ----|------|------
///  a  |  2   | 20
///  a  |  3   | 30
///  b  |  2   | 60
///  b  |  3   | 70
/// ```
///
/// ## Only OFFSET
/// ```text
/// Same input as above.
///
/// With OFFSET 2 (skip=2, fetch=None):
/// Output:
/// tag | time | value
/// ----|------|------
///  a  |  3   | 30
///  a  |  4   | 40
///  b  |  3   | 70
///  b  |  4   | 80
/// ```
///
/// ## Respecting NULLs
/// ```text
/// Input (4 series, 4 rows each):
/// tag | time | value1 | value2
/// ----|------|--------|-------
///  a  |  1   | 10     | <NULL>
///  a  |  2   | <NULL> | 20
///  a  |  3   | 30     | 30
///  a  |  4   | 40     | 40
///  b  |  1   | <NULL> | <NULL>
///  b  |  2   | <NULL> | 60
///  b  |  3   | 70     | <NULL>
///  b  |  4   | 80     | 80
///
/// With LIMIT 2 OFFSET 1 on both value1 and value2, respecting NULLs, default value NULL:
/// Output:
/// tag | time | value1 | value2
/// ----|------|--------|-------
///  a  |  2   | <NULL> | 20
///  a  |  3   | 30     | 30
///  b  |  2   | <NULL> | 60
///  b  |  3   | 70     | <NULL>
/// ```
///
/// ## Ignoring NULLs
/// ```text
/// Input (4 series):
/// tag | time | value1 | value2
/// ----|------|--------|-------
///  a  |  1   | 10     | <NULL>
///  a  |  2   | <NULL> | 20
///  a  |  3   | 30     | 30
///  a  |  4   | 40     | 40
///  b  |  1   | <NULL> | <NULL>
///  b  |  2   | <NULL> | 60
///  b  |  3   | 70     | 70
///  b  |  4   | 80     | 80
///  b  |  5   | 90     | 90
///
/// With LIMIT 2 OFFSET 1 on both value1 and value2, ignoring NULLs, default value NULL:
/// Output:
/// tag | time | value1 | value2
/// ----|------|--------|-------
///  a  |  3   | 30     | 30
///  a  |  4   | 40     | 40
///  b  |  4   | <NULL> | 70
///  b  |  4   | 80     | 80
///  b  |  5   | 90     | <NULL>
/// ```
///
/// # Performance Considerations
///
/// - **Memory**: Maintains minimal state (current series key + row counts)
///   across batches, making it suitable for large datasets.
///
/// - **Streaming**: Processes data in a streaming fashion without materializing
///   entire series in memory.
///
/// - **Early Termination**: Once a series exceeds its limit, subsequent rows
///   for that series can be efficiently filtered without full evaluation.
pub struct SeriesLimitExec {
    /// The input execution plan to apply series limiting to.
    input: Arc<dyn ExecutionPlan>,

    /// Expressions that define the series grouping.
    ///
    /// Rows with the same values for these expressions belong to the same series.
    /// Typically these are tag columns in InfluxQL queries.
    series_expr: Vec<PhysicalExprRef>,

    /// The expressions used for sorting within each series.
    ///
    /// Each series is sorted by this expression (typically ascending timestamp)
    /// before applying LIMIT and OFFSET operations.
    order_expr: Vec<PhysicalSortExpr>,

    /// Dynamic limit expressions that can evaluate to per-series limits.
    ///
    /// These expressions are evaluated for each series and can provide
    /// different limit values based on series characteristics.
    limit_expr: Vec<PhysicalLimitExpr>,

    /// Number of rows to skip at the beginning of each series (OFFSET).
    ///
    /// A value of 0 means no rows are skipped.
    skip: usize,

    /// Maximum number of rows to return from each series (LIMIT).
    ///
    /// `None` means no limit is applied (return all remaining rows after skip).
    /// `Some(n)` limits each series to at most `n` rows.
    fetch: Option<usize>,

    /// `limit_expr` modfied for use when processing batches.
    limited: Arc<BTreeMap<usize, LimitParams>>,

    /// Metrics tracking execution statistics for this plan node.
    ///
    /// Collects metrics like elapsed time, number of output rows, etc.
    metrics: ExecutionPlanMetricsSet,

    /// Cached plan properties for efficient access.
    ///
    /// Contains schema, partitioning, execution mode, and sort order information
    /// that are computed once and reused across multiple accesses.
    cache: PlanProperties,

    /// The required ordering for the input to this plan.
    required_ordering: Option<OrderingRequirements>,
}

impl SeriesLimitExec {
    /// Create a new SeriesLimitExec.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        series_expr: Vec<PhysicalExprRef>,
        order_expr: Vec<PhysicalSortExpr>,
        limit_expr: Vec<PhysicalLimitExpr>,
        skip: usize,
        fetch: Option<usize>,
    ) -> Result<Self> {
        let input_schema = input.schema();

        let mut limited = BTreeMap::new();
        for le in &limit_expr {
            let mut index = None;
            le.expr.apply(|pe| {
                if let Some(column) = pe.as_any().downcast_ref::<Column>() {
                    match index {
                        None => index = Some(column.index()),
                        Some(idx) if idx == column.index() => {}
                        Some(_) => {
                            return Err(DataFusionError::Plan(
                                "PhysicalLimitExpr requires a single Column expression".to_string(),
                            ));
                        }
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            let index = index.ok_or(DataFusionError::Plan(
                "PhysicalLimitExpr requires a Column expression".to_string(),
            ))?;
            if limited.insert(index, LimitParams::try_from(le)?).is_some() {
                return Err(DataFusionError::Plan(
                    "SeriesLimitExec limit expressions must refer to distinct columns".to_string(),
                ));
            }
        }

        // The output schema is the same as the input scheme except for columns
        // referenced in limit_expr, these are potentially renamed and may have
        // their nullability changed.
        let fields = input_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| match limited.get(&idx) {
                Some(params) => {
                    let field = params.expr.return_field(input_schema.as_ref())?;
                    let nullable = field.is_nullable();
                    Ok(Arc::new(
                        Arc::unwrap_or_clone(field).with_nullable(params.is_nullable(nullable)),
                    ))
                }
                None => Ok(Arc::clone(field)),
            })
            .collect::<Result<Vec<_>>>()?;

        let schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ));

        let limited = Arc::new(limited);
        let required_ordering = Self::compute_ordering(&series_expr, &order_expr);
        let cache = Self::compute_properties(&input, schema, &limited);

        Ok(Self {
            input,
            series_expr,
            order_expr,
            limit_expr,
            skip,
            fetch,
            limited,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
            required_ordering,
        })
    }

    /// This function creates the cache object that stores the plan properties
    /// such as equivalence properties, partitioning, ordering, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        limited: &BTreeMap<usize, LimitParams>,
    ) -> PlanProperties {
        // The output ordering is the same as the input ordering so long as
        // it does not depend on any of the limited columns. Iterate through the
        // input ordering stopping at the first ordering expression that depends on a
        // limited column.
        let ordering = input.output_ordering().and_then(|ordering| {
            LexOrdering::new(
                ordering
                    .iter()
                    .take_while(|pse| {
                        !pse.expr
                            .exists(|pe| {
                                Ok(if let Some(col) = pe.as_any().downcast_ref::<Column>() {
                                    limited.contains_key(&col.index())
                                } else {
                                    false
                                })
                            })
                            .expect("cannot error")
                    })
                    .cloned(),
            )
        });

        let eq_properties = if let Some(ordering) = ordering {
            EquivalenceProperties::new_with_orderings(schema, std::iter::once(ordering))
        } else {
            EquivalenceProperties::new(Arc::clone(&schema))
        };

        PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        )
    }

    fn compute_ordering(
        series_expr: &[PhysicalExprRef],
        order_expr: &[PhysicalSortExpr],
    ) -> Option<OrderingRequirements> {
        let sort_requirements = series_expr
            .iter()
            .map(|expr| PhysicalSortRequirement {
                expr: Arc::clone(expr),
                options: None,
            })
            .chain(order_expr.iter().map(|se| PhysicalSortRequirement {
                expr: Arc::clone(&se.expr),
                options: Some(se.options),
            }));

        LexRequirement::new(sort_requirements).map(OrderingRequirements::new)
    }
}

impl std::fmt::Debug for SeriesLimitExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeriesLimitExec")
            .field("series_expr", &self.series_expr)
            .field("order_expr", &self.order_expr)
            .field("limit_expr", &self.limit_expr)
            .field("skip", &self.skip)
            .field("fetch", &self.fetch)
            .finish_non_exhaustive()
    }
}

impl ExecutionPlan for SeriesLimitExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.as_slice() {
            [child] => Ok(Arc::new(Self::try_new(
                Arc::clone(child),
                self.series_expr.clone(),
                self.order_expr.clone(),
                self.limit_expr.clone(),
                self.skip,
                self.fetch,
            )?)),
            _ => Err(DataFusionError::Internal(format!(
                "SeriesLimitExec wrong number of children: expected 1, found {}",
                children.len()
            ))),
        }
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableStream> {
        if partition
            >= self
                .input
                .properties()
                .output_partitioning()
                .partition_count()
        {
            return Err(DataFusionError::Internal(format!(
                "SeriesLimitExec invalid partition {partition}"
            )));
        }

        let input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let reservation = MemoryConsumer::new(format!("SeriesLimitExec[{partition}]"))
            .register(context.memory_pool());

        let series_expr = self.series_expr.clone();
        let limited = Arc::clone(&self.limited);

        let stream = SeriesLimitStream::try_new(
            input_stream,
            self.schema(),
            baseline_metrics,
            reservation,
            series_expr,
            limited,
            self.skip as u64,
            self.fetch.map(|f| f as u64),
        )?;

        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![if self.series_expr.is_empty() {
            Distribution::SinglePartition
        } else {
            Distribution::HashPartitioned(self.series_expr.iter().map(Arc::clone).collect())
        }]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![self.required_ordering.clone()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
}

impl DisplayAs for SeriesLimitExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                let series_expr = self
                    .series_expr
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");

                let order_expr = self
                    .order_expr
                    .iter()
                    .map(|se| se.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");

                let limit_expr = self
                    .limit_expr
                    .iter()
                    .map(|le| le.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");

                write!(
                    f,
                    "SeriesLimitExec: series=[{}], order=[{}], limit_expr=[{}]",
                    series_expr, order_expr, limit_expr
                )?;

                if self.skip > 0 {
                    write!(f, ", skip={}", self.skip)?;
                }

                if let Some(fetch) = self.fetch {
                    write!(f, ", fetch={}", fetch)?;
                }

                Ok(())
            }
        }
    }
}

/// A streaming implementation of per-series LIMIT and OFFSET operations.
///
/// This stream processes incoming record batches and applies LIMIT and OFFSET
/// constraints independently to each time series (group of rows with the same
/// series key values). It maintains state across batches to correctly handle
/// series that span multiple batches.
///
/// # Behavior
///
/// For each incoming batch, the stream:
/// 1. Evaluates series expressions to determine series boundaries
/// 2. Detects series changes and resets row counters accordingly
/// 3. Assigns row numbers within each series using [`row_number`]
/// 4. Filters rows based on LIMIT (fetch) and OFFSET (skip) constraints
/// 5. Replaces filtered-out values with default values
/// 6. Tracks state for series that continue into subsequent batches
///
/// # Series Continuation
///
/// When a series spans multiple batches, the stream maintains:
/// - The current series key values in `current_series`
/// - Row counts for each limited expression in `counts`
///
/// This allows row numbering to continue correctly across batch boundaries.
/// For example, if a series has 100 rows split across 3 batches, LIMIT 10 OFFSET 5
/// will correctly skip the first 5 rows (even if they're in the first batch) and
/// return the next 10 rows (even if they span multiple batches).
///
/// # Memory Management
///
/// The stream tracks memory usage via `reservation` and grows/shrinks it as the
/// `current_series` state is updated. This ensures proper memory accounting in
/// DataFusion's memory pool system.
///
/// # Example
///
/// Given input with two series (tag='a' and tag='b'), each with 4 rows:
/// ```text
/// Input:
/// tag | time | value
/// ----|------|------
///  a  |  1   | 10
///  a  |  2   | 20
///  a  |  3   | 30
///  a  |  4   | 40
///  b  |  1   | 50
///  b  |  2   | 60
///  b  |  3   | 70
///  b  |  4   | 80
///
/// With LIMIT 2 OFFSET 1:
/// Output:
/// tag | time | value
/// ----|------|------
///  a  |  2   | 20    (skipped row 1, included rows 2-3)
///  a  |  3   | 30
///  b  |  2   | 60    (skipped row 1, included rows 2-3)
///  b  |  3   | 70
/// ```
///
/// # Default Values
///
/// For rows that are filtered out but whose timestamps appear in other series,
/// the stream can emit default values (typically NULL or 0) to maintain time
/// alignment across series. This is controlled by the `limited` field's default
/// value component.
struct SeriesLimitStream {
    /// The stream of input batches.
    input: SendableRecordBatchStream,

    /// The schema of the output batches.
    schema: SchemaRef,

    /// Metrics for tracking execution statistics.
    metrics: BaselineMetrics,

    /// Memory reservation for this stream.
    reservation: MemoryReservation,

    /// Physical expressions that define the series grouping.
    ///
    /// Rows with the same values for these expressions belong to the same series.
    /// Typically these are tag columns in InfluxQL queries.
    series_expr: Vec<PhysicalExprRef>,

    /// Limited expressions with their null handling and default values.
    ///
    /// Each tuple contains:
    /// - `PhysicalExprRef`: The expression to evaluate (typically a value column)
    /// - `bool`: Whether to ignore nulls (true = IGNORE NULLS, false = RESPECT NULLS)
    /// - `Scalar<ArrayRef>`: Default value to use for filtered-out rows
    limited: Arc<BTreeMap<usize, LimitParams>>,

    /// Range of row numbers to allow through the filter. This is a
    /// half-open interval of the form (lower, upper]. Rows are numbered
    /// from 1 so a lower bound of 0 will allow all rows up to upper. An
    /// upper value of u64::MAX is used to mean there is effectively no
    /// limit.
    lower: Scalar<PrimitiveArray<UInt64Type>>,
    upper: Scalar<PrimitiveArray<UInt64Type>>,

    /// The current series key being processed.
    current_series: Vec<Scalar<ArrayRef>>,

    /// Row counts for each limited expression in the current series.
    counts: BTreeMap<usize, u64>,
}

impl SeriesLimitStream {
    #[expect(clippy::too_many_arguments)]
    fn try_new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        metrics: BaselineMetrics,
        mut reservation: MemoryReservation,
        series_expr: Vec<PhysicalExprRef>,
        limited: Arc<BTreeMap<usize, LimitParams>>,
        skip: u64,
        fetch: Option<u64>,
    ) -> Result<Self> {
        // Set the initial series to be all nulls.
        let current_series = series_expr
            .iter()
            .map(|expr| expr.data_type(input.schema().as_ref()))
            .map(|data_type| data_type.map(|data_type| new_null_array(&data_type, 1)))
            .collect::<Result<Vec<_>>>()?;
        // Set the initial memory size.
        reservation.resize(
            current_series
                .iter()
                .map(|arr| arr.get_array_memory_size())
                .sum::<usize>(),
        );
        let current_series = current_series.into_iter().map(Scalar::new).collect();
        let counts = limited.keys().map(|idx| (*idx, 0u64)).collect();
        let lower = PrimitiveArray::<UInt64Type>::new_scalar(skip);
        let upper =
            PrimitiveArray::<UInt64Type>::new_scalar(fetch.map(|n| n + skip).unwrap_or(u64::MAX));
        Ok(Self {
            input,
            schema,
            metrics,
            reservation,
            series_expr,
            limited,
            lower,
            upper,
            current_series,
            counts,
        })
    }

    fn process_batch(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }

        let series_arrs = self
            .series_expr
            .iter()
            .map(|pe| pe.evaluate(&batch))
            .map(|res| res.and_then(|cv| cv.to_array(num_rows)))
            .collect::<Result<Vec<_>>>()?;

        // Check if the series has changed compared to the current series.
        // Short-circuit on first mismatch to avoid unnecessary comparisons.
        let mut series_changed = false;
        for (arr, current) in series_arrs.iter().zip(self.current_series.iter()) {
            let first_value = Scalar::new(arr.slice(0, 1));

            if !arrow::compute::kernels::cmp::eq(&first_value, current)?.value(0) {
                series_changed = true;
                break;
            }
        }

        if series_changed {
            // Series has changed, reset counts.
            for count in &mut self.counts.values_mut() {
                *count = 0;
            }
        }

        // Partition the series.
        let ranges = if series_arrs.is_empty() {
            #[expect(clippy::single_range_in_vec_init)]
            Vec::from([(0..num_rows)])
        } else {
            partition(&series_arrs)?.ranges()
        };

        // All columns that have ignore_nulls as false will produce the
        // same filter, remember it to avoid recomputing.
        let mut respect_nulls_cache: Option<(Arc<BooleanArray>, u64)> = None;
        let mut limited_arrs: BTreeMap<usize, ArrayRef> = BTreeMap::default();
        let mut filters = Vec::with_capacity(self.limited.len());

        for (idx, params) in self.limited.iter() {
            let LimitParams {
                expr,
                ignore_nulls,
                default_value,
            } = params;
            let arr = expr.evaluate(&batch)?.into_array(num_rows)?;

            let (filter, count) = match (*ignore_nulls, &respect_nulls_cache) {
                (true, _) => {
                    let (arr, count) = row_number(&arr, self.counts[idx], true, &ranges);
                    let filter = arrow::compute::and(
                        &arrow::compute::kernels::cmp::gt(&arr, &self.lower)?,
                        &arrow::compute::kernels::cmp::lt_eq(&arr, &self.upper)?,
                    )?;
                    (Arc::new(filter), count)
                }
                (false, Some((filter, count))) => (Arc::clone(filter), *count),
                (false, None) => {
                    let (arr, count) = row_number(&arr, self.counts[idx], false, &ranges);
                    let filter = Arc::new(arrow::compute::and(
                        &arrow::compute::kernels::cmp::gt(&arr, &self.lower)?,
                        &arrow::compute::kernels::cmp::lt_eq(&arr, &self.upper)?,
                    )?);
                    respect_nulls_cache = Some((Arc::clone(&filter), count));
                    (filter, count)
                }
            };
            limited_arrs.insert(
                *idx,
                arrow::compute::kernels::zip::zip(&filter, &arr, default_value)?,
            );
            filters.push(filter);
            self.counts.insert(*idx, count);
        }

        // Compute the batch filter efficiently by building it in one pass.
        // Instead of folding with or_kleene (which creates N-1 intermediate arrays),
        // we build the result directly by checking if any filter is true at each position.
        let batch_filter = if filters.is_empty() {
            BooleanArray::new_null(num_rows)
        } else if filters.len() == 1 {
            // Fast path: single filter, no need to combine
            Arc::unwrap_or_clone(Arc::clone(&filters[0]))
        } else {
            // Multiple filters: combine them efficiently
            let mut batch_filter_builder = arrow::array::BooleanBuilder::with_capacity(num_rows);

            for row_idx in 0..num_rows {
                // Check if any filter is true for this row
                let any_true = filters.iter().any(|filter| filter.value(row_idx));
                batch_filter_builder.append_value(any_true);
            }

            batch_filter_builder.finish()
        };

        let output_arrs = batch
            .into_parts()
            .1
            .iter()
            .enumerate()
            .map(|(idx, arr)| {
                if let Some(limited_arr) = limited_arrs.get(&idx) {
                    limited_arr
                } else {
                    arr
                }
            })
            .map(|arr| arrow::compute::filter(arr, &batch_filter))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        // Store the current series. Tracking the memory use.
        for (idx, arr) in series_arrs.iter().enumerate() {
            let arr = arr.slice(num_rows - 1, 1);
            self.reservation.try_grow(arr.get_array_memory_size())?;
            let mut value = Scalar::new(arr);
            std::mem::swap(&mut self.current_series[idx], &mut value);
            let arr = value.into_inner();
            self.reservation.shrink(arr.get_array_memory_size());
        }

        Ok(RecordBatch::try_new(Arc::clone(&self.schema), output_arrs)?)
    }
}

impl RecordBatchStream for SeriesLimitStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for SeriesLimitStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Poll the input stream for the next batch
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                // Process the batch through our series limiting logic
                let elapsed_compute = self.metrics.elapsed_compute().clone();
                let result = {
                    let _timer = elapsed_compute.timer();
                    self.process_batch(batch)
                };
                match result {
                    Ok(output_batch) => {
                        // Record the number of output rows
                        self.metrics.record_output(output_batch.num_rows());
                        Poll::Ready(Some(Ok(output_batch)))
                    }
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

/// Parameters defining how to process a limited column.
///
/// `LimitParams` encapsulates the processing rules for a single value column that
/// has per-series LIMIT/OFFSET constraints applied to it. During query execution,
/// these parameters control how rows are numbered, filtered, and replaced with
/// default values.
struct LimitParams {
    /// The expression for the limited column.
    expr: PhysicalExprRef,

    /// Whether to ignore nulls in the limit calculation.
    ignore_nulls: bool,

    /// The default value to use for filtered-out rows.
    default_value: Scalar<ArrayRef>,
}

impl LimitParams {
    /// Determine if the limited column can be nullable in the output.
    fn is_nullable(&self, input_nullable: bool) -> bool {
        let default_nullable = self.default_value.get().0.is_nullable();
        if self.ignore_nulls {
            // Any nulls will be replace by the default value
            default_nullable
        } else {
            // Respect nulls, so nullable if input is nullable or
            // default is nullable
            input_nullable || default_nullable
        }
    }
}

impl TryFrom<&PhysicalLimitExpr> for LimitParams {
    type Error = DataFusionError;

    fn try_from(value: &PhysicalLimitExpr) -> Result<Self> {
        let default_value = value.default_value.to_scalar().map_err(|e| {
            DataFusionError::Plan(format!(
                "PhysicalLimitExpr failed to convert default value to scalar: {}",
                e
            ))
        })?;
        Ok(Self {
            expr: Arc::clone(&value.expr),
            ignore_nulls: value.ignore_nulls,
            default_value,
        })
    }
}

/// Assigns row numbers to elements in an array, respecting partition boundaries.
///
/// This function generates sequential row numbers for each element in the input array,
/// with special handling for partitions and null values. Row numbers restart at 1 for
/// each new partition.
///
/// # Arguments
///
/// * `arr` - The input array for which to generate row numbers. This is typically a
///   value column, and is used only to check for null values when `ignore_nulls` is true.
/// * `start` - The starting row number for the first partition. Subsequent partitions
///   always start at 1. This allows continuing numbering across multiple batches within
///   the same partition.
/// * `ignore_nulls` - Controls null handling behavior:
///   - `false` (RESPECT NULLS): Null values receive row numbers like any other value
///   - `true` (IGNORE NULLS): Null values are skipped and assigned null row numbers
/// * `ranges` - Defines the partition boundaries within the array. Each range
///   represents a distinct group (e.g., time series) where row numbering should restart.
///
/// # Returns
///
/// Returns a tuple of:
/// * `PrimitiveArray<UInt64Type>` - An array of row numbers corresponding to each element
///   in the input array. Elements may be null if `ignore_nulls` is true and the corresponding
///   input element is null.
/// * `u64` - The final row number assigned in the last partition. This can be used as the
///   `start` value for subsequent calls to continue numbering within the same partition.
///
/// # Examples
///
/// ```text
/// // Single partition, no nulls, starting from 0:
/// arr = [10, 20, 30]
/// partitions = single partition covering all elements
/// result = ([1, 2, 3], 3)
///
/// // Single partition with RESPECT NULLS:
/// arr = [10, null, 30]
/// ignore_nulls = false
/// result = ([1, 2, 3], 3)
///
/// // Single partition with IGNORE NULLS:
/// arr = [10, null, 30]
/// ignore_nulls = true
/// result = ([1, null, 2], 2)
///
/// // Multiple partitions (e.g., two different series):
/// arr = [1, 2, 3, 4, 5, 6]
/// partitions = [0..3, 3..6] (two partitions)
/// result = ([1, 2, 3, 1, 2, 3], 3)
/// // Note: row numbering resets for second partition
///
/// // Continuing numbering across batches:
/// // Batch 1:
/// arr1 = [10, 20]
/// (result1, last1) = row_number(arr1, 0, false, single_partition)
/// // result1 = [1, 2], last1 = 2
///
/// // Batch 2 (same partition continues):
/// arr2 = [30, 40]
/// (result2, last2) = row_number(arr2, last1, false, single_partition)
/// // result2 = [3, 4], last2 = 4
/// ```
fn row_number(
    arr: &ArrayRef,
    start: u64,
    ignore_nulls: bool,
    ranges: &[Range<usize>],
) -> (PrimitiveArray<UInt64Type>, u64) {
    let mut builder = UInt64Builder::with_capacity(arr.len());
    let mut row_number = start;

    for (idx, range) in ranges.iter().enumerate() {
        if idx > 0 {
            row_number = 0;
        }
        for idx in range.start..range.end {
            if ignore_nulls && arr.is_null(idx) {
                builder.append_null();
                continue;
            }
            row_number += 1;
            builder.append_value(row_number);
        }
    }

    (builder.finish(), row_number)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use datafusion::physical_expr::expressions::Column;
    use insta::assert_snapshot;

    mod physical_limit_expr_tests {
        use super::*;

        #[test]
        fn test_new() {
            let expr = Arc::new(Column::new("value", 2)) as PhysicalExprRef;
            let default_value = ScalarValue::Float64(Some(0.0));
            let limit_expr = PhysicalLimitExpr::new(expr, true, default_value.clone());

            assert!(limit_expr.ignore_nulls);
            assert_eq!(limit_expr.default_value, default_value);
        }

        #[test]
        fn test_display_ignore_nulls() {
            let expr = Arc::new(Column::new("value", 2)) as PhysicalExprRef;
            let default_value = ScalarValue::Float64(Some(0.0));
            let limit_expr = PhysicalLimitExpr::new(expr, true, default_value);

            let display_str = format!("{}", limit_expr);
            assert!(display_str.contains("IGNORE NULLS"));
            assert!(display_str.contains("default:"));
            assert!(display_str.contains("0"));
        }

        #[test]
        fn test_display_respect_nulls() {
            let expr = Arc::new(Column::new("value", 2)) as PhysicalExprRef;
            let default_value = ScalarValue::Float64(Some(99.9));
            let limit_expr = PhysicalLimitExpr::new(expr, false, default_value);

            let display_str = format!("{}", limit_expr);
            assert!(display_str.contains("RESPECT NULLS"));
            assert!(display_str.contains("default:"));
            assert!(display_str.contains("99.9"));
        }

        #[test]
        fn test_clone() {
            let expr = Arc::new(Column::new("value", 2)) as PhysicalExprRef;
            let default_value = ScalarValue::Float64(Some(0.0));
            let limit_expr = PhysicalLimitExpr::new(expr, true, default_value.clone());

            let cloned = limit_expr.clone();
            assert_eq!(cloned.ignore_nulls, limit_expr.ignore_nulls);
            assert_eq!(cloned.default_value, limit_expr.default_value);
        }

        #[test]
        fn test_debug() {
            let expr = Arc::new(Column::new("value", 2)) as PhysicalExprRef;
            let default_value = ScalarValue::Float64(Some(0.0));
            let limit_expr = PhysicalLimitExpr::new(expr, true, default_value);

            let debug_str = format!("{:?}", limit_expr);
            assert!(!debug_str.is_empty());
            assert!(debug_str.contains("PhysicalLimitExpr"));
        }
    }

    mod series_limit_exec_tests {
        use super::*;
        use arrow::array::Float64Array;
        use arrow::compute::SortOptions;
        use arrow::datatypes::{Field, Schema};
        use datafusion::common::test_util::batches_to_string;
        use datafusion::physical_expr::LexOrdering;
        use datafusion::physical_plan::display::DisplayableExecutionPlan;
        use datafusion::{
            datasource::{memory::MemorySourceConfig, source::DataSourceExec},
            execution::context::SessionContext,
            physical_plan::sorts::sort::SortExec,
        };
        use futures::StreamExt;

        fn string_array<I, O, S>(vals: I) -> ArrayRef
        where
            I: IntoIterator<Item = O>,
            O: Into<Option<S>>,
            S: AsRef<str>,
        {
            Arc::new(StringArray::from_iter(vals.into_iter().map(|v| v.into())))
        }

        fn int_array(vals: impl IntoIterator<Item = impl Into<Option<i64>>>) -> ArrayRef {
            Arc::new(Int64Array::from_iter(vals.into_iter().map(|v| v.into())))
        }

        fn float_array(vals: impl IntoIterator<Item = impl Into<Option<f64>>>) -> ArrayRef {
            Arc::new(Float64Array::from_iter(vals.into_iter().map(|v| v.into())))
        }

        fn input_plan(
            arrs: impl IntoIterator<Item = (impl Into<String>, impl Into<ArrayRef>)>,
            sort: impl IntoIterator<Item = (impl Into<String>, impl Into<Option<SortOptions>>)>,
        ) -> Arc<dyn ExecutionPlan> {
            let columns: Vec<(String, ArrayRef)> = arrs
                .into_iter()
                .map(|(name, arr)| (name.into(), arr.into()))
                .collect();
            let fields = columns
                .iter()
                .map(|(name, arr)| Field::new(name, arr.data_type().clone(), arr.null_count() > 0))
                .collect::<Vec<_>>();
            let schema = Arc::new(Schema::new(fields));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                columns.into_iter().map(|(_, arr)| arr).collect(),
            )
            .unwrap();
            let empty = batch.num_rows() == 0;
            let mut plan: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&[vec![batch]], schema, None).unwrap(),
            )));

            let mut sort_it = sort.into_iter().peekable();
            if !empty && sort_it.peek().is_some() {
                let sort_expr = LexOrdering::new(sort_it.map(|(name, opts)| {
                    let name = name.into();
                    PhysicalSortExpr::new(
                        Arc::new(Column::new(&name, plan.schema().index_of(&name).unwrap())),
                        opts.into().unwrap_or_default(),
                    )
                }))
                .unwrap();
                plan = Arc::new(SortExec::new(sort_expr, plan));
            }

            plan
        }

        fn test_input_plan<I, O, S>(
            tag: I,
            time: impl IntoIterator<Item = impl Into<Option<i64>>>,
            value: impl IntoIterator<Item = impl Into<Option<f64>>>,
        ) -> Arc<dyn ExecutionPlan>
        where
            I: IntoIterator<Item = O>,
            O: Into<Option<S>>,
            S: AsRef<str>,
        {
            input_plan(
                [
                    ("tag", string_array(tag)),
                    ("time", int_array(time)),
                    ("value", float_array(value)),
                ],
                [("tag", None), ("time", None)],
            )
        }

        /// Helper to collect all batches from a stream
        async fn collect_stream(
            mut stream: Pin<Box<dyn RecordBatchStream + Send>>,
        ) -> Result<Vec<RecordBatch>> {
            let mut batches = vec![];
            while let Some(batch) = stream.next().await {
                batches.push(batch?);
            }
            Ok(batches)
        }

        #[tokio::test]
        async fn test_basic_limit() {
            // Test basic LIMIT functionality - limit 2 rows per series
            let input = test_input_plan(
                ["a", "a", "a", "a", "b", "b", "b", "b"],
                [1, 2, 3, 4, 1, 2, 3, 4],
                [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(2))
                    .unwrap();

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let batches = collect_stream(stream).await.unwrap();

            assert_snapshot!(batches_to_string(&batches), @r#"
            +-----+------+-------+
            | tag | time | value |
            +-----+------+-------+
            | a   | 1    | 1.0   |
            | a   | 2    | 2.0   |
            | b   | 1    | 5.0   |
            | b   | 2    | 6.0   |
            +-----+------+-------+
            "#);
        }

        #[tokio::test]
        async fn test_basic_offset() {
            // Test basic OFFSET functionality - skip first 2 rows per series
            let input = test_input_plan(["a", "a", "a", "a"], [1, 2, 3, 4], [1.0, 2.0, 3.0, 4.0]);

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 2, None)
                    .unwrap();

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let batches = collect_stream(stream).await.unwrap();

            assert_snapshot!(batches_to_string(&batches), @r#"
            +-----+------+-------+
            | tag | time | value |
            +-----+------+-------+
            | a   | 3    | 3.0   |
            | a   | 4    | 4.0   |
            +-----+------+-------+
            "#);
        }

        #[tokio::test]
        async fn test_limit_and_offset() {
            // Test combined LIMIT and OFFSET
            let input = test_input_plan(
                ["a", "a", "a", "a", "a", "a"],
                [1, 2, 3, 4, 5, 6],
                [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            // Skip 2, take 2
            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 2, Some(2))
                    .unwrap();

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let batches = collect_stream(stream).await.unwrap();

            assert_snapshot!(batches_to_string(&batches), @r#"
            +-----+------+-------+
            | tag | time | value |
            +-----+------+-------+
            | a   | 3    | 3.0   |
            | a   | 4    | 4.0   |
            +-----+------+-------+
            "#);
        }

        #[tokio::test]
        async fn test_multiple_series() {
            // Test that limits apply independently to each series
            let input = test_input_plan(
                ["a", "a", "a", "b", "b", "b", "c", "c", "c"],
                [1, 2, 3, 1, 2, 3, 1, 2, 3],
                [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            // Limit 1 row per series
            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(1))
                    .unwrap();

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let batches = collect_stream(stream).await.unwrap();

            assert_snapshot!(batches_to_string(&batches), @r#"
            +-----+------+-------+
            | tag | time | value |
            +-----+------+-------+
            | a   | 1    | 1.0   |
            | b   | 1    | 4.0   |
            | c   | 1    | 7.0   |
            +-----+------+-------+
            "#);
        }

        #[tokio::test]
        async fn test_empty_batch() {
            // Test handling of empty batches
            let input = test_input_plan(Vec::<String>::new(), Vec::<i64>::new(), Vec::<f64>::new());

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(10))
                    .unwrap();

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let batches = collect_stream(stream).await.unwrap();

            assert_snapshot!(batches_to_string(&batches), @r#"
            +-----+------+-------+
            | tag | time | value |
            +-----+------+-------+
            +-----+------+-------+
            "#);
        }

        #[tokio::test]
        async fn test_with_nulls() {
            // Test handling of null values with ignore_nulls = false
            let input = test_input_plan(
                ["a", "a", "a", "a"],
                [1, 2, 3, 4],
                [Some(1.0), None, Some(3.0), Some(4.0)],
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false, // RESPECT NULLS
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(2))
                    .unwrap();

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let batches = collect_stream(stream).await.unwrap();

            assert_snapshot!(batches_to_string(&batches), @r#"
            +-----+------+-------+
            | tag | time | value |
            +-----+------+-------+
            | a   | 1    | 1.0   |
            | a   | 2    |       |
            +-----+------+-------+
            "#);
        }

        #[tokio::test]
        async fn test_with_nulls_ignore() {
            // Test handling of null values with ignore_nulls = true
            let input = test_input_plan(
                ["a", "a", "a", "a", "a"],
                [1, 2, 3, 4, 5],
                [Some(1.0), None, Some(3.0), None, Some(5.0)],
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                true, // IGNORE NULLS
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(2))
                    .unwrap();

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let batches = collect_stream(stream).await.unwrap();

            assert_snapshot!(batches_to_string(&batches), @r#"
            +-----+------+-------+
            | tag | time | value |
            +-----+------+-------+
            | a   | 1    | 1.0   |
            | a   | 3    | 3.0   |
            +-----+------+-------+
            "#);
        }

        #[test]
        fn test_execute_invalid_partition() {
            // Test that execute returns an error for invalid partition number
            let input = test_input_plan(["a"], [1], [1.0]);

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, None)
                    .unwrap();

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();

            // Try to execute with invalid partition number (only partition 0 exists)
            let result = exec.execute(999, task_ctx);
            assert!(result.is_err());
            let err_string = result.err().unwrap().to_string();
            assert!(err_string.contains("invalid partition"));
        }

        #[test]
        fn test_with_new_children_wrong_count_zero() {
            // Test with_new_children with 0 children
            let input = test_input_plan(["a"], [1], [1.0]);

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec = Arc::new(
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, None)
                    .unwrap(),
            );

            // Try with 0 children
            let result = exec.with_new_children(vec![]);
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("wrong number of children")
            );
        }

        #[test]
        fn test_with_new_children_wrong_count_two() {
            // Test with_new_children with 2 children
            let input1 = test_input_plan(["a"], [1], [1.0]);
            let input2 = Arc::clone(&input1);

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec = Arc::new(
                SeriesLimitExec::try_new(input1, series_expr, order_expr, limit_expr, 0, None)
                    .unwrap(),
            );

            let input3 =
                test_input_plan(Vec::<String>::new(), Vec::<i64>::new(), Vec::<f64>::new());
            // Try with 2 children
            let result = exec.with_new_children(vec![input2, input3]);
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("wrong number of children")
            );
        }

        #[tokio::test]
        async fn test_preserve_schema() {
            // Test that limits apply independently to each series
            let input = input_plan(
                [
                    (
                        "value1",
                        float_array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
                    ),
                    (
                        "tag1",
                        string_array(["a", "a", "a", "b", "b", "b", "c", "c", "c"]),
                    ),
                    (
                        "value2",
                        float_array([10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0]),
                    ),
                    ("time", int_array([1, 2, 3, 1, 2, 3, 1, 2, 3])),
                    (
                        "tag2",
                        string_array(["A", "A", "B", "B", "B", "C", "C", "C", "D"]),
                    ),
                ],
                [("tag1", None), ("tag2", None), ("time", None)],
            );

            let series_expr = vec![
                Arc::new(Column::new("tag1", 1)) as PhysicalExprRef,
                Arc::new(Column::new("tag2", 4)) as PhysicalExprRef,
            ];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 3)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            let limit_expr = vec![
                PhysicalLimitExpr::new(
                    Arc::new(Column::new("value1", 0)) as PhysicalExprRef,
                    false,
                    ScalarValue::Float64(None),
                ),
                PhysicalLimitExpr::new(
                    Arc::new(Column::new("value2", 2)) as PhysicalExprRef,
                    false,
                    ScalarValue::Float64(None),
                ),
            ];

            // Limit 1 row per series
            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(1))
                    .unwrap();

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let batches = collect_stream(stream).await.unwrap();

            assert_snapshot!(batches_to_string(&batches), @r"
            +--------+------+--------+------+------+
            | value1 | tag1 | value2 | time | tag2 |
            +--------+------+--------+------+------+
            | 1.0    | a    | 10.0   | 1    | A    |
            | 3.0    | a    | 30.0   | 3    | B    |
            | 4.0    | b    | 40.0   | 1    | B    |
            | 6.0    | b    | 60.0   | 3    | C    |
            | 7.0    | c    | 70.0   | 1    | C    |
            | 9.0    | c    | 90.0   | 3    | D    |
            +--------+------+--------+------+------+
            ");
        }

        #[test]
        fn test_display_as() {
            // Test DisplayAs formatting
            let input = test_input_plan(["a"], [1], [1.0]);

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 5, Some(10))
                    .unwrap();

            assert_snapshot!(DisplayableExecutionPlan::new(&exec).indent(false), @r"
            SeriesLimitExec: series=[tag@0], order=[time@1 ASC], limit_expr=[value@2 RESPECT NULLS (default: 0)], skip=5, fetch=10
              SortExec: expr=[tag@0 ASC, time@1 ASC], preserve_partitioning=[false]
                DataSourceExec: partitions=1, partition_sizes=[1]
            ");
        }

        #[test]
        fn test_empty_series_expr_distribution() {
            // Test UnspecifiedDistribution when series_expr is empty
            let input = test_input_plan(["a"], [1], [1.0]);

            let series_expr = vec![]; // Empty!
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(10))
                    .unwrap();

            let distributions = exec.required_input_distribution();
            assert_eq!(distributions.len(), 1);
            matches!(distributions[0], Distribution::UnspecifiedDistribution);
        }

        #[tokio::test]
        async fn test_descending_time_ordering() {
            // Test with descending time ordering
            let input = input_plan(
                [
                    ("tag", string_array(["a", "a", "a"])),
                    ("time", int_array([3, 2, 1])), // Descending time
                    ("value", float_array([30.0, 20.0, 10.0])),
                ],
                [
                    ("tag", None),
                    (
                        "time",
                        Some(SortOptions {
                            descending: true,
                            nulls_first: true,
                        }),
                    ),
                ],
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                options: SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            }];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(2))
                    .unwrap();

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let batches = collect_stream(stream).await.unwrap();

            assert_snapshot!(batches_to_string(&batches), @r#"
            +-----+------+-------+
            | tag | time | value |
            +-----+------+-------+
            | a   | 3    | 30.0  |
            | a   | 2    | 20.0  |
            +-----+------+-------+
            "#);
        }

        #[tokio::test]
        async fn test_multiple_limited_expressions() {
            let input = input_plan(
                [
                    ("tag", string_array(["a", "a", "a", "a"])),
                    ("time", int_array([1, 2, 3, 4])),
                    ("value1", float_array([1.0, 2.0, 3.0, 4.0])),
                    ("value2", float_array([10.0, 20.0, 30.0, 40.0])),
                ],
                [("tag", None), ("time", None)],
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            // TWO limited expressions
            let limit_expr = vec![
                PhysicalLimitExpr::new(
                    Arc::new(Column::new("value1", 2)) as PhysicalExprRef,
                    false,
                    ScalarValue::Float64(Some(0.0)),
                ),
                PhysicalLimitExpr::new(
                    Arc::new(Column::new("value2", 3)) as PhysicalExprRef,
                    false,
                    ScalarValue::Float64(Some(0.0)),
                ),
            ];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(2))
                    .unwrap();

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let batches = collect_stream(stream).await.unwrap();

            assert_snapshot!(batches_to_string(&batches), @r#"
            +-----+------+--------+--------+
            | tag | time | value1 | value2 |
            +-----+------+--------+--------+
            | a   | 1    | 1.0    | 10.0   |
            | a   | 2    | 2.0    | 20.0   |
            +-----+------+--------+--------+
            "#);
        }

        #[tokio::test]
        async fn test_multiple_limited_expressions_ignore_nulls() {
            // Test with multiple limited expressions ignoring nulls
            let input = input_plan(
                [
                    ("tag", string_array(["a", "a", "a", "a"])),
                    ("time", int_array([1, 2, 3, 4])),
                    (
                        "value1",
                        float_array([Some(1.0), None, Some(3.0), Some(4.0)]),
                    ),
                    (
                        "value2",
                        float_array([Some(10.0), Some(20.0), None, Some(40.0)]),
                    ),
                ],
                [("tag", None), ("time", None)],
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            // TWO limited expressions
            let limit_expr = vec![
                PhysicalLimitExpr::new(
                    Arc::new(Column::new("value1", 2)) as PhysicalExprRef,
                    true,
                    ScalarValue::Float64(Some(0.0)),
                ),
                PhysicalLimitExpr::new(
                    Arc::new(Column::new("value2", 3)) as PhysicalExprRef,
                    true,
                    ScalarValue::Float64(Some(0.0)),
                ),
            ];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(2))
                    .unwrap();

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let batches = collect_stream(stream).await.unwrap();

            assert_snapshot!(batches_to_string(&batches), @r"
            +-----+------+--------+--------+
            | tag | time | value1 | value2 |
            +-----+------+--------+--------+
            | a   | 1    | 1.0    | 10.0   |
            | a   | 2    | 0.0    | 20.0   |
            | a   | 3    | 3.0    | 0.0    |
            +-----+------+--------+--------+
            ");
        }

        #[tokio::test]
        async fn test_multiple_limited_expressions_ignore_nulls_with_offset() {
            // Test with multiple limited expressions ignoring nulls and with offset
            let input = input_plan(
                [
                    ("tag", string_array(["a", "a", "a", "a"])),
                    ("time", int_array([1, 2, 3, 4])),
                    (
                        "value1",
                        float_array([Some(1.0), None, Some(3.0), Some(4.0)]),
                    ),
                    (
                        "value2",
                        float_array([Some(10.0), Some(20.0), None, Some(40.0)]),
                    ),
                ],
                [("tag", None), ("time", None)],
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            // TWO limited expressions
            let limit_expr = vec![
                PhysicalLimitExpr::new(
                    Arc::new(Column::new("value1", 2)) as PhysicalExprRef,
                    true,
                    ScalarValue::Float64(Some(0.0)),
                ),
                PhysicalLimitExpr::new(
                    Arc::new(Column::new("value2", 3)) as PhysicalExprRef,
                    true,
                    ScalarValue::Float64(Some(0.0)),
                ),
            ];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 1, Some(2))
                    .unwrap();

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let batches = collect_stream(stream).await.unwrap();

            assert_snapshot!(batches_to_string(&batches), @r"
            +-----+------+--------+--------+
            | tag | time | value1 | value2 |
            +-----+------+--------+--------+
            | a   | 2    | 0.0    | 20.0   |
            | a   | 3    | 3.0    | 0.0    |
            | a   | 4    | 4.0    | 40.0   |
            +-----+------+--------+--------+
            ");
        }

        #[tokio::test]
        async fn test_multiple_order_expressions() {
            // Test with multiple order expressions (e.g., ORDER BY time, value)
            let input = input_plan(
                [
                    ("tag", string_array(["a", "a", "a", "a", "a", "a"])),
                    ("time", int_array([1, 1, 1, 2, 2, 2])), // Same time values to test secondary ordering
                    ("tag2", string_array(["c", "c", "c", "c", "c", "c"])),
                    ("value", float_array([30.0, 20.0, 10.0, 60.0, 50.0, 40.0])),
                ],
                [
                    ("tag", None),
                    ("time", None),
                    ("tag2", None),
                    (
                        "value",
                        Some(SortOptions {
                            descending: true,
                            nulls_first: false,
                        }),
                    ),
                ],
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                    options: SortOptions::default(),
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("tag2", 2)) as PhysicalExprRef,
                    options: SortOptions {
                        descending: true,
                        nulls_first: false,
                    },
                },
            ];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 3)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(2))
                    .unwrap();

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let batches = collect_stream(stream).await.unwrap();

            assert_snapshot!(batches_to_string(&batches), @r"
            +-----+------+------+-------+
            | tag | time | tag2 | value |
            +-----+------+------+-------+
            | a   | 1    | c    | 30.0  |
            | a   | 1    | c    | 20.0  |
            +-----+------+------+-------+
            ");
        }

        #[tokio::test]
        async fn test_multiple_order_with_multiple_series() {
            // Test multiple order expressions with multiple series
            let input = input_plan(
                [
                    (
                        "tag",
                        string_array(["a", "a", "a", "a", "b", "b", "b", "b"]),
                    ),
                    ("time", int_array([1, 1, 2, 2, 1, 1, 2, 2])),
                    (
                        "tag2",
                        string_array(["c", "c", "c", "c", "c", "c", "c", "c"]),
                    ),
                    (
                        "value",
                        float_array([20.0, 10.0, 40.0, 30.0, 70.0, 60.0, 90.0, 80.0]),
                    ),
                ],
                [("tag", None), ("time", None), ("tag2", None)],
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                    options: SortOptions::default(),
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("tag2", 2)) as PhysicalExprRef,
                    options: SortOptions::default(),
                },
            ];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 3)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            // Limit to 3 rows per series
            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(3))
                    .unwrap();

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let batches = collect_stream(stream).await.unwrap();

            assert_snapshot!(batches_to_string(&batches), @r"
            +-----+------+------+-------+
            | tag | time | tag2 | value |
            +-----+------+------+-------+
            | a   | 1    | c    | 20.0  |
            | a   | 1    | c    | 10.0  |
            | a   | 2    | c    | 40.0  |
            | b   | 1    | c    | 70.0  |
            | b   | 1    | c    | 60.0  |
            | b   | 2    | c    | 90.0  |
            +-----+------+------+-------+
            ");
        }

        #[tokio::test]
        async fn test_multiple_order_with_offset() {
            // Test multiple order expressions with OFFSET
            let input = input_plan(
                [
                    ("tag", string_array(["a", "a", "a", "a", "a"])),
                    ("time", int_array([1, 1, 2, 2, 3])),
                    ("tag2", string_array(["c", "c", "c", "c", "c"])),
                    ("value", float_array([10.0, 20.0, 30.0, 40.0, 50.0])),
                ],
                [("tag", None), ("time", None), ("tag2", None)],
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                    options: SortOptions::default(),
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("tag2", 2)) as PhysicalExprRef,
                    options: SortOptions::default(),
                },
            ];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 3)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            // Skip 2, take 2
            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 2, Some(2))
                    .unwrap();

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let batches = collect_stream(stream).await.unwrap();

            assert_snapshot!(batches_to_string(&batches), @r"
            +-----+------+------+-------+
            | tag | time | tag2 | value |
            +-----+------+------+-------+
            | a   | 2    | c    | 30.0  |
            | a   | 2    | c    | 40.0  |
            +-----+------+------+-------+
            ");
        }

        #[test]
        fn test_required_ordering_multiple_order() {
            // Test that required_input_ordering includes all order expressions
            let input = test_input_plan(["a"], [1], [1.0]);

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                    options: SortOptions::default(),
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                    options: SortOptions {
                        descending: true,
                        nulls_first: false,
                    },
                },
            ];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(10))
                    .unwrap();

            let expect = OrderingRequirements::new(
                LexRequirement::new(vec![
                    PhysicalSortRequirement {
                        expr: Arc::new(Column::new("tag", 0)),
                        options: None,
                    },
                    PhysicalSortRequirement {
                        expr: Arc::new(Column::new("time", 1)),
                        options: Some(SortOptions::default()),
                    },
                    PhysicalSortRequirement {
                        expr: Arc::new(Column::new("value", 2)),
                        options: Some(SortOptions {
                            descending: true,
                            nulls_first: false,
                        }),
                    },
                ])
                .unwrap(),
            );

            let required_ordering = exec.required_input_ordering();
            assert_eq!(required_ordering, vec![Some(expect)]);
        }

        #[test]
        fn test_display_as_multiple_order() {
            // Test DisplayAs formatting with multiple order expressions
            let input = test_input_plan(["a"], [1], [1.0]);

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                    options: SortOptions::default(),
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                    options: SortOptions {
                        descending: true,
                        nulls_first: false,
                    },
                },
            ];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 5, Some(10))
                    .unwrap();

            assert_snapshot!(DisplayableExecutionPlan::new(&exec).indent(false), @r"
            SeriesLimitExec: series=[tag@0], order=[time@1 ASC, value@2 DESC NULLS LAST], limit_expr=[value@2 RESPECT NULLS (default: 0)], skip=5, fetch=10
              SortExec: expr=[tag@0 ASC, time@1 ASC], preserve_partitioning=[false]
                DataSourceExec: partitions=1, partition_sizes=[1]
            ");
        }

        #[test]
        fn test_output_ordering_preserved_when_limited_column_not_in_ordering() {
            // Test that output ordering is fully preserved when the limited column
            // doesn't appear in the input ordering
            let input = input_plan(
                [
                    ("tag", string_array(["a"])),
                    ("time", int_array([1])),
                    ("value", float_array([1.0])),
                    ("other", float_array([2.0])),
                ],
                [("tag", None), ("time", None), ("other", None)],
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                    options: SortOptions::default(),
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("other", 3)) as PhysicalExprRef,
                    options: SortOptions::default(),
                },
            ];
            // Limit "value" column which is NOT in the input ordering
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(10))
                    .unwrap();

            // Output ordering should be fully preserved: tag, time, other
            assert_snapshot!(
                exec.properties().output_ordering().unwrap(),
                @"tag@0 ASC, time@1 ASC, other@3 ASC",
            );
        }

        #[test]
        fn test_output_ordering_truncated_when_limited_column_in_ordering() {
            // Test that output ordering is truncated when a limited column appears
            // in the input ordering
            let input = input_plan(
                [
                    ("tag", string_array(["a"])),
                    ("time", int_array([1])),
                    ("value", float_array([1.0])),
                ],
                [("tag", None), ("time", None), ("value", None)],
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                    options: SortOptions::default(),
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                    options: SortOptions::default(),
                },
            ];
            // Limit "value" column which IS in the input ordering
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(10))
                    .unwrap();

            // Output ordering should be truncated before "value": only tag, time
            assert_snapshot!(
                exec.properties().output_ordering().unwrap(),
                @"tag@0 ASC, time@1 ASC",
            );
        }

        #[test]
        fn test_output_ordering_empty_when_first_column_limited() {
            // Test that output ordering becomes empty when the first order column
            // is a limited column
            let input = input_plan(
                [
                    ("tag", string_array(["a"])),
                    ("time", int_array([1])),
                    ("value", float_array([1.0])),
                ],
                [("tag", None), ("value", None)],
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            // Limit "value" column which is the FIRST in the input ordering
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(10))
                    .unwrap();

            // Output ordering should only have tag (series expr), not value
            assert_snapshot!(
                exec.properties().output_ordering().unwrap(),
                @"tag@0 ASC",
            );
        }

        #[test]
        fn test_output_ordering_with_no_input_ordering() {
            // Test behavior when input has no ordering
            let input = input_plan(
                [
                    ("tag", string_array(["a"])),
                    ("time", int_array([1])),
                    ("value", float_array([1.0])),
                ],
                Vec::<(&str, Option<SortOptions>)>::new(), // No sorting
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                options: SortOptions::default(),
            }];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(10))
                    .unwrap();

            // Output ordering should be None since input has no ordering
            assert!(exec.properties().output_ordering().is_none());
        }

        #[test]
        fn test_output_ordering_with_multiple_limited_columns() {
            // Test ordering when multiple columns are limited
            let input = input_plan(
                [
                    ("tag", string_array(["a"])),
                    ("time", int_array([1])),
                    ("value1", float_array([1.0])),
                    ("value2", float_array([2.0])),
                    ("value3", float_array([3.0])),
                ],
                [
                    ("tag", None),
                    ("time", None),
                    ("value2", None),
                    ("value3", None),
                ],
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                    options: SortOptions::default(),
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("value2", 3)) as PhysicalExprRef,
                    options: SortOptions::default(),
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("value3", 4)) as PhysicalExprRef,
                    options: SortOptions::default(),
                },
            ];
            // Limit multiple columns
            let limit_expr = vec![
                PhysicalLimitExpr::new(
                    Arc::new(Column::new("value1", 2)) as PhysicalExprRef,
                    false,
                    ScalarValue::Float64(Some(0.0)),
                ),
                PhysicalLimitExpr::new(
                    Arc::new(Column::new("value2", 3)) as PhysicalExprRef,
                    false,
                    ScalarValue::Float64(Some(0.0)),
                ),
            ];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(10))
                    .unwrap();

            // Output ordering should be truncated right before value2 (first limited column in ordering)
            // Should include: tag, time (but not value2 or value3)
            assert_snapshot!(
                exec.properties().output_ordering().unwrap(),
                @"tag@0 ASC, time@1 ASC",
            );
        }

        #[test]
        fn test_output_ordering_preserves_sort_options() {
            // Test that sort options (descending, nulls_first) are preserved
            let input = input_plan(
                [
                    ("tag", string_array(["a"])),
                    ("time", int_array([1])),
                    ("value", float_array([1.0])),
                    ("other", float_array([2.0])),
                ],
                [
                    (
                        "tag",
                        Some(SortOptions {
                            descending: false,
                            nulls_first: true,
                        }),
                    ),
                    (
                        "time",
                        Some(SortOptions {
                            descending: true,
                            nulls_first: false,
                        }),
                    ),
                    (
                        "other",
                        Some(SortOptions {
                            descending: false,
                            nulls_first: false,
                        }),
                    ),
                ],
            );

            let series_expr = vec![Arc::new(Column::new("tag", 0)) as PhysicalExprRef];
            let order_expr = vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("time", 1)) as PhysicalExprRef,
                    options: SortOptions {
                        descending: true,
                        nulls_first: false,
                    },
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("other", 3)) as PhysicalExprRef,
                    options: SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                },
            ];
            let limit_expr = vec![PhysicalLimitExpr::new(
                Arc::new(Column::new("value", 2)) as PhysicalExprRef,
                false,
                ScalarValue::Float64(Some(0.0)),
            )];

            let exec =
                SeriesLimitExec::try_new(input, series_expr, order_expr, limit_expr, 0, Some(10))
                    .unwrap();

            // Output ordering should preserve sort options
            assert_snapshot!(exec.properties().output_ordering().unwrap(),
                @"tag@0 ASC, time@1 DESC NULLS LAST, other@3 ASC NULLS LAST",
            );
        }
    }

    mod row_number_tests {
        use super::*;

        #[test]
        fn test_row_number_simple() {
            // Single partition - all rows in same group
            let group_arr = Arc::new(StringArray::from(vec!["a", "a", "a"])) as ArrayRef;
            let value_arr = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
            let ranges = partition(&[group_arr]).unwrap().ranges();

            let (result, final_count) = row_number(&value_arr, 0, false, &ranges);

            assert_eq!(result, PrimitiveArray::from_iter_values([1_u64, 2, 3]));
            assert_eq!(final_count, 3);
        }

        #[test]
        fn test_row_number_with_start() {
            let group_arr = Arc::new(StringArray::from(vec!["a", "a", "a"])) as ArrayRef;
            let value_arr = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
            let ranges = partition(&[group_arr]).unwrap().ranges();

            let (result, final_count) = row_number(&value_arr, 10, false, &ranges);

            assert_eq!(result, PrimitiveArray::from_iter_values([11_u64, 12, 13]));
            assert_eq!(final_count, 13);
        }

        #[test]
        fn test_row_number_with_nulls_respect() {
            let group_arr = Arc::new(StringArray::from(vec!["a", "a", "a"])) as ArrayRef;
            let value_arr = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
            let ranges = partition(&[group_arr]).unwrap().ranges();

            let (result, final_count) = row_number(&value_arr, 0, false, &ranges);

            // Respect nulls means nulls still get row numbers
            assert_eq!(result, PrimitiveArray::from_iter_values([1_u64, 2, 3]));
            assert_eq!(final_count, 3);
        }

        #[test]
        fn test_row_number_with_nulls_ignore() {
            let group_arr = Arc::new(StringArray::from(vec!["a", "a", "a"])) as ArrayRef;
            let value_arr = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
            let ranges = partition(&[group_arr]).unwrap().ranges();

            let (result, final_count) = row_number(&value_arr, 0, true, &ranges);

            // Ignore nulls means nulls are skipped in numbering
            assert_eq!(
                result,
                PrimitiveArray::from_iter(vec![Some(1u64), None, Some(2u64)])
            );
            assert_eq!(final_count, 2);
        }

        #[test]
        fn test_row_number_multiple_partitions() {
            // Two partitions - rows grouped by "a" and "b"
            let group_arr =
                Arc::new(StringArray::from(vec!["a", "a", "a", "b", "b", "b"])) as ArrayRef;
            let value_arr = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef;
            let ranges = partition(&[group_arr]).unwrap().ranges();

            let (result, final_count) = row_number(&value_arr, 0, false, &ranges);

            // Numbers start at one for each partition
            assert_eq!(
                result,
                PrimitiveArray::from_iter_values([1_u64, 2, 3, 1, 2, 3])
            );
            assert_eq!(final_count, 3);
        }

        #[test]
        fn test_row_number_empty() {
            let group_arr = Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;
            let value_arr = Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef;
            let ranges = partition(&[group_arr]).unwrap().ranges();

            let (result, final_count) = row_number(&value_arr, 0, false, &ranges);

            assert_eq!(result, PrimitiveArray::<UInt64Type>::from_iter_values([]));
            assert_eq!(final_count, 0);
        }

        #[test]
        fn test_row_number_single_partition_with_start_and_nulls() {
            let group_arr = Arc::new(StringArray::from(vec!["a", "a", "a", "a", "a"])) as ArrayRef;
            let value_arr = Arc::new(Int64Array::from(vec![
                Some(1),
                None,
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef;
            let ranges = partition(&[group_arr]).unwrap().ranges();

            let (result, final_count) = row_number(&value_arr, 5, true, &ranges);

            assert_eq!(
                result,
                PrimitiveArray::from_iter([Some(6_u64), None, Some(7), None, Some(8)])
            );
            assert_eq!(final_count, 8);
        }
    }
}
