pub(crate) mod metadata;
mod select;
mod source_field_names;
mod union;

use crate::aggregate::{MODE, PERCENTILE, SPREAD};
use crate::error;
use crate::plan::ir::{DataSource, Field, Interval, Select, SelectQuery};
use crate::plan::planner::metadata::{expr_is_influxql_filled, schema_with_influxql_filled};
use crate::plan::planner::select::{
    ProjectionInfo, Selector, SelectorWindowOrderBy, fields_to_exprs_no_nulls,
    make_tag_key_column_meta, plan_with_sort,
};
use crate::plan::planner::source_field_names::SourceFieldNamesVisitor;
use crate::plan::planner_time_range_expression::time_range_to_df_expr;
use crate::plan::rewriter::{ProjectionType, find_table_names, rewrite_statement};
use crate::plan::udf::{
    INTEGRAL_UDF_NAME, cumulative_sum, derivative, difference, elapsed, find_integral_udfs,
    find_window_udfs, integral, is_integral_udf, moving_average, non_negative_derivative,
    non_negative_difference,
};
use crate::plan::util::{
    IQLSchema, binary_operator_to_df_operator, find_aggregate_exprs, rebase_expr,
};
use crate::plan::var_ref::var_ref_data_type_to_data_type;
use crate::plan::{planner_rewrite_expression, udf};
use crate::window::{
    CUMULATIVE_SUM, DERIVATIVE, DIFFERENCE, ELAPSED, INTEGRAL_WINDOW, MOVING_AVERAGE,
    NON_NEGATIVE_DERIVATIVE, NON_NEGATIVE_DIFFERENCE, PERCENT_ROW_NUMBER,
};
use arrow::array::{
    BooleanArray, DictionaryArray, Int32Array, Int64Array, StringArray, StringBuilder,
    StringDictionaryBuilder,
};
use arrow::datatypes::{
    ArrowNativeType, DataType, Field as ArrowField, Int32Type, Schema as ArrowSchema,
};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{
    DFSchema, DFSchemaRef, DataFusionError, ExprSchema, Result, ScalarValue, SchemaError,
    ToDFSchema,
};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::execution::FunctionRegistry;
use datafusion::functions::datetime::date_bin::DateBinFunc;
use datafusion::functions::{core::expr_ext::FieldAccessor, expr_fn::now};
use datafusion::functions_aggregate::count::count;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::functions_aggregate::{
    average::avg_udaf, count::count_udaf, median::median_udaf, stddev::stddev_udaf, sum::sum_udaf,
};
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::expr::{
    AggregateFunctionParams, Alias, FieldMetadata, ScalarFunction, WindowFunctionParams,
};
use datafusion::logical_expr::expr_rewriter::normalize_col;
use datafusion::logical_expr::logical_plan::Analyze;
use datafusion::logical_expr::utils::{disjunction, expr_as_column_expr};
use datafusion::logical_expr::{
    AggregateUDF, EmptyRelation, Explain, Expr, ExprSchemable, Extension, LogicalPlan,
    LogicalPlanBuilder, Operator, PlanType, Projection, ScalarUDF, TableSource, ToStringifiedPlan,
    Union, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition, binary_expr,
    col, expr, expr::WindowFunction, lit, lit_with_metadata,
};
use datafusion::logical_expr::{ExplainFormat, Filter, SortExpr};
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::prelude::{Column, ExprFunctionExt, cast, lit_timestamp_nano, make_array, when};
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::ast::NullTreatment;
use datafusion_util::{AsExpr, lit_dict};
use generated_types::influxdata::iox::querier::v1::InfluxQlMetadata;
use influxdb_influxql_parser::common::{LimitClause, OffsetClause, OrderByClause};
use influxdb_influxql_parser::explain::{ExplainOption, ExplainStatement};
use influxdb_influxql_parser::expression::walk::{Expression, walk_expr, walk_expression};
use influxdb_influxql_parser::expression::{
    Binary, Call, ConditionalBinary, ConditionalExpression, ConditionalOperator, VarRef,
    VarRefDataType,
};
use influxdb_influxql_parser::functions::{
    is_aggregate_function, is_now_function, is_scalar_math_function,
};
use influxdb_influxql_parser::literal::Number;
use influxdb_influxql_parser::parameter::{BindParameterError, replace_bind_params_with_values};
use influxdb_influxql_parser::select::{FillClause, GroupByClause};
use influxdb_influxql_parser::show_field_keys::ShowFieldKeysStatement;
use influxdb_influxql_parser::show_measurements::{
    ShowMeasurementsStatement, WithMeasurementClause,
};
use influxdb_influxql_parser::show_retention_policies::ShowRetentionPoliciesStatement;
use influxdb_influxql_parser::show_tag_keys::ShowTagKeysStatement;
use influxdb_influxql_parser::show_tag_values::{ShowTagValuesStatement, WithKeyClause};
use influxdb_influxql_parser::simple_from_clause::ShowFromClause;
use influxdb_influxql_parser::time_range::{ReduceContext, TimeRange, split_cond};
use influxdb_influxql_parser::timestamp::Timestamp;
use influxdb_influxql_parser::visit::Visitable;
use influxdb_influxql_parser::{
    common::{MeasurementName, WhereClause},
    expression::Expr as IQLExpr,
    literal::Literal,
    select::SelectStatement,
    statement::Statement,
};
use iox_query::analyzer::default_return_value_for_aggr_fn;
use iox_query::config::{IoxConfigExt, MetadataCutoff};
use iox_query::exec::IOxSessionContext;
use iox_query::exec::gapfill::{FillExpr, FillStrategy, GapFill};
use iox_query::exec::series_limit::LimitExpr;
use iox_query::{LogicalPlanBuilderExt, transform_plan_schema};
use iox_query_params::StatementParams;
use itertools::Itertools;
use metadata::FieldExt;
use query_functions::date_bin_wallclock::DateBinWallclockUDF;
use query_functions::date_bin_wallclock::expr_fn::date_bin_wallclock;
use query_functions::{
    clean_non_meta_escapes,
    selectors::{selector_first, selector_last, selector_max, selector_min},
    tz::TZ_UDF,
};
use schema::{
    INFLUXQL_MEASUREMENT_COLUMN_NAME, INFLUXQL_METADATA_KEY, InfluxColumnType, InfluxFieldType,
    Schema, TIME_DATA_TIMEZONE,
};
use std::collections::{BTreeSet, HashMap, HashSet, hash_map::Entry};
use std::fmt::Debug;
use std::iter;
use std::ops::{Bound, ControlFlow, Deref, Range};
use std::sync::Arc;
use tracing::debug;
use union::union_and_coerce;

use super::parse_regex;
use super::util::{contains_expr, number_to_scalar};

/// The column index of the measurement column.
const MEASUREMENT_COLUMN_INDEX: u32 = 0;

/// The `SchemaProvider` trait allows the InfluxQL query planner to obtain
/// meta-data about tables referenced in InfluxQL statements.
pub trait SchemaProvider {
    /// Getter for a datasource
    fn get_table_provider(&self, name: &str) -> Result<Arc<dyn TableSource>>;

    /// Getter for a UDF description
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>>;

    /// Getter for a UDAF description
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>>;

    /// The collection of tables for this schema.
    fn table_names(&self) -> Vec<&'_ str>;

    /// Test if a table with the specified `name` exists.
    fn table_exists(&self, name: &str) -> bool {
        self.table_names().contains(&name)
    }

    /// Get the schema for the specified `table`.
    fn table_schema(&self, name: &str) -> Option<Schema>;

    fn execution_props(&self) -> &ExecutionProps;
}

/// Informs the planner which rules should be applied when transforming
/// an InfluxQL expression.
///
/// Specifically, the scope of available functions is narrowed to mathematical scalar functions
/// when processing the `WHERE` clause.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
enum ExprScope {
    /// Signals that expressions should be transformed in the context of
    /// the `WHERE` clause.
    #[default]
    Where,
    /// Signals that expressions should be transformed in the context of
    /// the `SELECT` projection list.
    Projection,
}

/// State used to inform the planner, which is derived for the
/// root `SELECT` and subqueries.
#[derive(Debug, Default, Clone)]
struct Context<'a> {
    /// The parent context, if this is a subquery.
    parent: Option<&'a Self>,

    /// The name of the table used as the data source for the current query.
    table_name: &'a str,
    projection_type: ProjectionType,
    tz: Option<Arc<str>>,

    order_by: OrderByClause,

    /// The column alias for the `time` column.
    ///
    /// # NOTE
    ///
    /// The time column can only be aliased for the root query.
    time_alias: &'a str,

    /// The filter predicate for the query, without `time`.
    condition: Option<&'a ConditionalExpression>,

    /// The time range of the query
    time_range: TimeRange,

    // GROUP BY information
    group_by: Option<&'a GroupByClause>,
    fill: Option<FillClause>,

    /// Interval of the `TIME` function found in the `GROUP BY` clause.
    interval: Option<Interval>,

    /// How many additional window intervals must be retrieved, when grouping
    /// by time, to ensure window functions like `difference` have sufficient
    /// data to for the first window of the `time_range`.
    extra_intervals: usize,

    /// The set of tags specified in the top-level `SELECT` statement
    /// which represent the tag set used for grouping output.
    root_group_by_tags: &'a [&'a str],
}

impl<'a> Context<'a> {
    fn new_root(
        table_name: &'a str,
        select: &'a Select,
        root_group_by_tags: &'a [&'a str],
    ) -> Self {
        Self {
            parent: None,
            table_name,
            projection_type: select.projection_type,
            tz: select.timezone.map(|tz| Arc::from(tz.name())),
            order_by: select.order_by.unwrap_or_default(),
            time_alias: &select.fields[0].name,
            condition: select.condition.as_ref(),
            time_range: select.time_range,
            group_by: select.group_by.as_ref(),
            fill: select.fill,
            interval: select.interval,
            extra_intervals: select.extra_intervals,
            root_group_by_tags,
        }
    }

    /// Create a new context for the select statement that is
    /// a subquery of the current context.
    fn subquery(&'a self, select: &'a Select) -> Self {
        Self {
            parent: Some(self),
            table_name: self.table_name,
            projection_type: select.projection_type,
            tz: select.timezone.map(|tz| Arc::from(tz.name())),
            order_by: select.order_by.unwrap_or_default(),
            // time is never aliased in subqueries
            time_alias: "time",
            condition: select.condition.as_ref(),
            // Subqueries should be restricted by the time range of the parent
            //
            // See: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/iterator.go#L716-L721
            time_range: select.time_range.intersected(self.time_range),
            group_by: select.group_by.as_ref(),
            fill: select.fill,
            interval: select.interval,
            extra_intervals: select.extra_intervals,
            root_group_by_tags: self.root_group_by_tags,
        }
    }

    /// Return a [`SortExpr`] expression for the `time` column.
    fn time_sort_expr(&self) -> SortExpr {
        self.time_alias.as_expr().sort(
            match self.order_by {
                OrderByClause::Ascending => true,
                OrderByClause::Descending => false,
            },
            false,
        )
    }

    /// Calculate the time range that would have been stored in the
    /// shard group created when the query was prepared in InfluxQL OG
    /// ([source][1]).
    ///
    /// ```go
    /// // Modify the time range if there are extra intervals and an interval.
    /// if !c.Interval.IsZero() && c.ExtraIntervals > 0 {
    ///     if c.Ascending {
    ///         newTime := timeRange.Min.Add(time.Duration(-c.ExtraIntervals) * c.Interval.Duration)
    ///         if !newTime.Before(time.Unix(0, influxql.MinTime).UTC()) {
    ///             timeRange.Min = newTime
    /// ```
    ///
    /// This time range is stored in the shard group and any iterators
    /// that are created from the group will have the time bound by this
    /// range ([source][2]).
    ///
    /// ```go
    /// // Override the time constraints if they don't match each other.
    /// if !a.MinTime.IsZero() && opt.StartTime < a.MinTime.UnixNano() {
    ///     opt.StartTime = a.MinTime.UnixNano()
    /// }
    /// if !a.MaxTime.IsZero() && opt.EndTime > a.MaxTime.UnixNano() {
    ///     opt.EndTime = a.MaxTime.UnixNano()
    /// }
    /// ```
    /// [1]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/compile.go#L1153-L1158
    /// [2]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/coordinator/shard_mapper.go#L177-L183
    fn shard_group_time_range(&self) -> TimeRange {
        if let Some(parent) = &self.parent {
            return parent.shard_group_time_range();
        }
        match (self.extra_intervals, self.interval) {
            (count @ 1.., Some(interval)) => {
                if self.order_by.is_ascending() {
                    TimeRange {
                        lower: self
                            .time_range
                            .lower
                            .map(|v| v - (count as i64 * interval.duration)),
                        upper: self.time_range.upper,
                    }
                } else {
                    TimeRange {
                        lower: self.time_range.lower,
                        upper: self
                            .time_range
                            .upper
                            .map(|v| v + (count as i64 * interval.duration)),
                    }
                }
            }
            _ => self.time_range,
        }
    }

    /// Return the time range of the context, including any
    /// additional intervals required for window functions like
    /// `difference` or `moving_average`, when the query contains a
    /// `GROUP BY TIME` clause.
    ///
    /// # NOTE
    ///
    /// This function accounts for a bug in InfluxQL OG that only reads
    /// a single interval, rather than the number required based on the
    /// window function.
    ///
    /// ## EXPECTED
    ///
    /// For InfluxQL OG, the likely intended behaviour of the extra intervals
    /// was to ensure a minimum number of windows were calculated to ensure
    /// there was sufficient data for the lower time bound specified
    /// in the `WHERE` clause, or upper time bound when ordering by `time`
    /// in descending order.
    ///
    /// For example, the following InfluxQL query calculates the `moving_average`
    /// of the `mean` of the `writes` field over 3 intervals. The interval
    /// is 10 seconds, as specified by the `GROUP BY time(10s)` clause.
    ///
    /// ```sql
    /// SELECT moving_average(mean(writes), 3)
    /// FROM diskio
    /// WHERE time >= '2020-06-11T16:53:00Z' AND time < '2020-06-11T16:55:00Z'
    /// GROUP BY time(10s)
    /// ```
    ///
    /// The intended output was supposed to include the first window of the time
    /// bounds, or `'2020-06-11T16:53:00Z'`:
    ///
    /// ```text
    /// name: diskio
    /// time                 moving_average
    /// ----                 --------------
    /// 2020-06-11T16:53:00Z 5592529.333333333
    /// 2020-06-11T16:53:10Z 5592677.333333333
    /// ...
    /// 2020-06-11T16:54:10Z 5593513.333333333
    /// 2020-06-11T16:54:20Z 5593612.333333333
    /// ```
    /// however, the actual output starts at `2020-06-11T16:53:10Z`.
    ///
    /// ## BUG
    ///
    /// During compilation of the query, InfluxQL OG determines the `ExtraIntervals`
    /// required for the `moving_average` function, which in the example is `3` ([source][1]):
    ///
    /// ```go
    /// if c.global.ExtraIntervals < int(arg1.Val) {
    ///     c.global.ExtraIntervals = int(arg1.Val)
    /// }
    /// ```
    ///
    /// `arg1.Val` is the second argument from the example InfluxQL query, or `3`.
    ///
    /// When preparing the query for execution, the time range is adjusted by the
    /// `ExtraIntervals` determined during compilation ([source][2]):
    ///
    /// ```go
    /// // Modify the time range if there are extra intervals and an interval.
    /// if !c.Interval.IsZero() && c.ExtraIntervals > 0 {
    ///     if c.Ascending {
    ///         newTime := timeRange.Min.Add(time.Duration(-c.ExtraIntervals) * c.Interval.Duration)
    ///         if !newTime.Before(time.Unix(0, influxql.MinTime).UTC()) {
    ///             timeRange.Min = newTime
    /// ```
    ///
    /// In this case `timeRange.Min` will be adjusted from `2020-06-11T16:53:00Z` to
    /// `2020-06-11T16:52:30Z`, as `ExtraIntervals` is `3` and `Interval.Duration` is `10s`.
    ///
    /// The first issue is that the adjusted `timeRange` is only used to determine which
    /// shards to read per the following ([source][3]):
    ///
    /// ```go
    /// // Create an iterator creator based on the shards in the cluster.
    /// shards, err := shardMapper.MapShards(c.stmt.Sources, timeRange, sopt)
    /// ```
    ///
    /// The options used to configure query execution, constructed later in the function,
    /// use the time range from the compiled statement ([source][4]):
    ///
    /// ```go
    /// opt.StartTime, opt.EndTime = c.TimeRange.MinTimeNano(), c.TimeRange.MaxTimeNano()
    /// ```
    ///
    /// Specifically, `opt.StartTime` would be `2020-06-11T16:53:00Z` (`1591894380000000000`).
    ///
    /// Finally, when construction the physical operator to compute the `moving_average`,
    /// the `StartTime`, or `EndTime` for descending queries, is adjusted by the single
    /// interval of `10s` ([source][5]):
    ///
    /// ```go
    /// if !opt.Interval.IsZero() {
    ///     if opt.Ascending {
    ///         opt.StartTime -= int64(opt.Interval.Duration)
    /// ```
    ///
    /// before creating the iterator over the adjusted time range ([source][6]):
    ///
    /// ```go
    /// input, err := buildExprIterator(ctx, expr.Args[0], b.ic, b.sources, opt, b.selector, false)
    /// ```
    ///
    /// and despite the time range being adjusted correctly later in the switch statement ([source][7]):
    ///
    /// ```go
    /// case "moving_average":
    ///     n := expr.Args[1].(*influxql.IntegerLiteral)
    ///     if n.Val > 1 && !opt.Interval.IsZero() {
    ///         if opt.Ascending {
    ///             opt.StartTime -= int64(opt.Interval.Duration) * (n.Val - 1)
    /// ```
    /// this is not used by the `moving_average` iterator ([source][8]):
    ///
    /// ```go
    /// return newMovingAverageIterator(input, int(n.Val), opt)
    /// ```
    ///
    /// # Note
    ///
    /// This function also ensures that the time range cannot extend
    /// outside of the time range that InfluxQL OG would have passed to
    /// the shard mapper when preparing the query, as calculated by
    /// [`Self::shard_group_time_range`] ([source][2]).
    ///
    /// ```go
    /// // Modify the time range if there are extra intervals and an interval.
    /// if !c.Interval.IsZero() && c.ExtraIntervals > 0 {
    ///     if c.Ascending {
    ///         newTime := timeRange.Min.Add(time.Duration(-c.ExtraIntervals) * c.Interval.Duration)
    ///         if !newTime.Before(time.Unix(0, influxql.MinTime).UTC()) {
    ///             timeRange.Min = newTime
    /// ```
    ///
    /// This time range is stored in the shard group and all iterators
    /// created from the shard group will be restricted to this time
    /// range ([source][9]).
    ///
    /// ```go
    /// // Override the time constraints if they don't match each other.
    /// if !a.MinTime.IsZero() && opt.StartTime < a.MinTime.UnixNano() {
    ///     opt.StartTime = a.MinTime.UnixNano()
    /// }
    /// if !a.MaxTime.IsZero() && opt.EndTime > a.MaxTime.UnixNano() {
    ///     opt.EndTime = a.MaxTime.UnixNano()
    /// }
    /// ```
    ///
    /// For top-level queries this will have no effect as the shard
    /// group time range will always be the same, or wider than this
    /// extended time range.
    ///
    /// For subqueries this might narrow the time range that would have
    /// been used to read extra intervals, if the top-level query had
    /// no need for those extra intervals. This should not further
    /// restrict the requested time range, as that is restricted to
    /// requested time range of the parent query in [`Self::subquery`].
    ///
    /// [1]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/compile.go#L592-L594
    /// [2]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/compile.go#L1153-L1158
    /// [3]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/compile.go#L1172-L1173
    /// [4]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/compile.go#L1198
    /// [5]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/select.go#L259-L261
    /// [6]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/select.go#L268-L267
    /// [7]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/select.go#L286-L290
    /// [8]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/select.go#L295
    /// [9]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/coordinator/shard_mapper.go#L177-L183
    fn extended_time_range(&self) -> TimeRange {
        // As described in the function docs, extra_intervals is either
        // 1 or 0 to match InfluxQL OG behaviour.
        match (self.extra_intervals.min(1), self.interval) {
            (count @ 1.., Some(interval)) => {
                if self.order_by.is_ascending() {
                    TimeRange {
                        lower: self
                            .time_range
                            .lower
                            .map(|v| v - (count as i64 * interval.duration)),
                        upper: self.time_range.upper,
                    }
                } else {
                    TimeRange {
                        lower: self.time_range.lower,
                        upper: self
                            .time_range
                            .upper
                            .map(|v| v + (count as i64 * interval.duration)),
                    }
                }
            }
            _ => self.time_range,
        }
        // The time range cannot be any wider than the shard group time
        // range.
        .intersected(self.shard_group_time_range())
    }

    /// Calculate the time range of the expected result set. This is the
    /// time range of the query, adjusted for any `GROUP BY TIME`
    /// interval and offset.
    fn result_time_range(&self) -> TimeRange {
        if let Some(interval) = self.interval {
            TimeRange {
                lower: self.time_range.lower.map(|v| {
                    v - i64::rem_euclid(v - interval.offset.unwrap_or(0), interval.duration)
                }),
                upper: self.time_range.upper.map(|v| {
                    v - i64::rem_euclid(v - interval.offset.unwrap_or(0), interval.duration)
                        + (interval.duration - 1) // stop just before the beginning of the following interval
                }),
            }
        } else {
            self.time_range
        }
    }

    /// Returns the combined `GROUP BY` tags clause from the root
    /// and current statement. The list is sorted and guaranteed to be unique.
    fn group_by_tags(&self) -> Vec<&str> {
        match (self.root_group_by_tags.is_empty(), self.group_by) {
            (true, None) => vec![],
            (false, None) => self.root_group_by_tags.to_vec(),
            (_, Some(group_by)) => group_by
                .tag_names()
                .map(|ident| ident.as_str())
                .chain(self.root_group_by_tags.iter().copied())
                .sorted()
                .dedup()
                .collect(),
        }
    }

    fn fill(&self) -> Option<FillClause> {
        self.fill
    }

    /// Apply a projection to the input plan to ensure
    /// that the time column is in the correct timezone.
    fn project_timezone(&self, builder: LogicalPlanBuilder) -> Result<LogicalPlanBuilder> {
        let Some(otz) = &self.tz else {
            return Ok(builder);
        };

        let schema = builder.schema();
        let expr = schema
            .columns()
            .into_iter()
            .map(
                |column| match schema.field_from_column(&column).unwrap().data_type() {
                    DataType::Timestamp(_, itz) => {
                        if itz.as_ref().is_some_and(|itz| itz == otz) {
                            Expr::Column(column)
                        } else {
                            let name = column.name().to_string();
                            Expr::ScalarFunction(ScalarFunction::new_udf(
                                Arc::clone(&TZ_UDF),
                                vec![
                                    Expr::Column(column),
                                    Expr::Literal(ScalarValue::Utf8(Some(otz.to_string())), None),
                                ],
                            ))
                            .alias(name)
                        }
                    }
                    _ => Expr::Column(column),
                },
            )
            .collect::<Vec<_>>();
        builder.project(expr)
    }

    fn default_timestamp(&self) -> Expr {
        Expr::Literal(
            ScalarValue::TimestampNanosecond(
                // Issue #12482: The time column is set to the lower bound of
                // the time range, if present.
                self.time_range.lower.or(Some(0)),
                self.tz.clone().or_else(TIME_DATA_TIMEZONE),
            ),
            None,
        )
    }
}

/// This struct specifies how to handle non-existent columns when querying
/// with the `FILL(Number)` clause. The gap-filling value is used to fill
/// in the missing columns.
///
/// ## Fields
///
/// `fill_clause`:
/// Contains the FILL clause specification (e.g., `FILL(3.14)` or `FILL(NULL)`)
/// that determines what value to use for non-existent columns in result sets.
///
/// `data_type`:
/// Specifies the expected data type for the field in the output result.
/// This is especially important when the gap-filling value needs type conversion
/// to match the target column's data type.
///
/// ## Note: Cross-Table Data Type Handling
///
/// When querying multiple tables, a field may exist in one table but not another.
/// In such cases, the gap-filling value from `FILL(Number)` must be converted
/// to match the expected data type of the field in the result set. This struct
/// provides the information needed for proper type conversion.
///
/// [`ProjectionInfo::fields`] stores the data type of the field in the output result.
///
/// ## Example
///
/// Consider two tables (`cpu` and `disk`) with different field availability:
///
/// | Field Name    | Data Type | Available in `cpu` | Available in `disk` |
/// |---------------|-----------|-------------------|---------------------|
/// | `usage_idle`  | Float     | ✅               | ❌                  |
/// | `bytes_free`  | Integer   | ❌               | ✅                  |
///
/// For the query:
///
/// ```sql
/// SELECT usage_idle, bytes_free FROM cpu, disk FILL(3.14);
/// +------------------+----------------------+------------+------------+
/// | iox::measurement | time                 | usage_idle | bytes_free |
/// +------------------+----------------------+------------+------------+
/// | cpu              | 2022-10-31T02:00:00Z | 2.98       | 3          |
/// | cpu              | 2022-10-31T02:00:00Z | 0.98       | 3          |
/// | cpu              | 2022-10-31T02:00:00Z | 1.98       | 3          |
/// | cpu              | 2022-10-31T02:00:10Z | 2.99       | 3          |
/// | cpu              | 2022-10-31T02:00:10Z | 0.99       | 3          |
/// | cpu              | 2022-10-31T02:00:10Z | 1.99       | 3          |
/// | disk             | 2022-10-31T02:00:00Z | 3.14       | 1234       |
/// | disk             | 2022-10-31T02:00:00Z | 3.14       | 2234       |
/// | disk             | 2022-10-31T02:00:00Z | 3.14       | 3234       |
/// | disk             | 2022-10-31T02:00:10Z | 3.14       | 1239       |
/// | disk             | 2022-10-31T02:00:10Z | 3.14       | 2239       |
/// | disk             | 2022-10-31T02:00:10Z | 3.14       | 3239       |
/// +------------------+----------------------+------------+------------+
/// ```
///
/// This struct enables the planner to:
/// - Use `3.14` (Float) for missing `usage_idle` values in the `disk` table
/// - Convert `3.14` to `3` (Integer) for missing `bytes_free` values in the `cpu` table
///
/// It also handles aggregation queries with time-based grouping:
///
/// ```sql
/// SELECT COUNT(usage_idle), COUNT(bytes_free) FROM cpu, disk
/// WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z'
/// GROUP BY TIME(30s) FILL(3.14)
/// +------------------+----------------------+-------+---------+
/// | iox::measurement | time                 | count | count_1 |
/// +------------------+----------------------+-------+---------+
/// | cpu              | 2022-10-31T02:00:00Z | 6     | 3       |
/// | cpu              | 2022-10-31T02:00:30Z | 3     | 3       |
/// | cpu              | 2022-10-31T02:01:00Z | 3     | 3       |
/// | cpu              | 2022-10-31T02:01:30Z | 3     | 3       |
/// | disk             | 2022-10-31T02:00:00Z | 3     | 6       |
/// | disk             | 2022-10-31T02:00:30Z | 3     | 3       |
/// | disk             | 2022-10-31T02:01:00Z | 3     | 3       |
/// | disk             | 2022-10-31T02:01:30Z | 3     | 3       |
/// +------------------+----------------------+-------+---------+
/// ```
///
/// This struct allows the planner to:
/// - Convert `3.14` (Float) to `3` (Integer) for missing `COUNT(usage_idle)` values
///   in the `disk` table, and `COUNT(bytes_free)` values in the `cpu` table
struct VirtualColumnFillConfig {
    fill_clause: Option<FillClause>,
    data_type: Option<InfluxColumnType>,
}

#[expect(missing_debug_implementations)]
/// InfluxQL query planner
pub struct InfluxQLToLogicalPlan<'a> {
    s: &'a dyn SchemaProvider,
    iox_ctx: IOxSessionContext,
}

impl<'a> InfluxQLToLogicalPlan<'a> {
    pub fn new(s: &'a dyn SchemaProvider, iox_ctx: &'a IOxSessionContext) -> Self {
        Self {
            s,
            iox_ctx: iox_ctx.child_ctx("InfluxQLToLogicalPlan"),
        }
    }

    pub fn statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        match statement {
            Statement::CreateDatabase(_) => error::not_implemented("CREATE DATABASE"),
            Statement::Delete(_) => error::not_implemented("DELETE"),
            Statement::DropMeasurement(_) => error::not_implemented("DROP MEASUREMENT"),
            Statement::Explain(explain) => self.explain_statement_to_plan(*explain),
            Statement::Select(select) => self.select_query_to_plan(
                &self
                    .rewrite_select_statement(*select)
                    .map_err(|e| e.context("rewriting statement"))?,
            ),
            Statement::ShowDatabases(_) => error::not_implemented("SHOW DATABASES"),
            Statement::ShowMeasurements(show_measurements) => {
                self.show_measurements_to_plan(*show_measurements)
            }
            Statement::ShowRetentionPolicies(show_retention_policies) => {
                self.show_retention_policies_to_plan(*show_retention_policies)
            }
            Statement::ShowTagKeys(show_tag_keys) => self.show_tag_keys_to_plan(*show_tag_keys),
            Statement::ShowTagValues(show_tag_values) => {
                self.show_tag_values_to_plan(*show_tag_values)
            }
            Statement::ShowFieldKeys(show_field_keys) => {
                self.show_field_keys_to_plan(*show_field_keys)
            }
        }
    }

    pub fn statement_to_plan_with_params(
        &self,
        mut statement: Statement,
        params: StatementParams,
    ) -> Result<LogicalPlan> {
        if !params.is_empty() {
            statement = replace_bind_params_with_values(statement, params)
                .or_else(|e| error::params(e.to_string()))?;
        }
        self.statement_to_plan(statement)
    }

    fn explain_statement_to_plan(&self, explain: ExplainStatement) -> Result<LogicalPlan> {
        let plan = self.statement_to_plan(*explain.statement)?;
        let plan = Arc::new(plan);
        let schema = LogicalPlan::explain_schema();
        let schema = schema.to_dfschema_ref()?;

        // We'll specify the `plan_type` column as the "measurement name", so that it may be
        // grouped into tables in the output when formatted as InfluxQL tabular format.
        let measurement_column_index = schema
            .index_of_column_by_name(None, "plan_type")
            .ok_or_else(|| error::map::internal("unable to find plan_type column"))?
            as u32;

        let (analyze, verbose) = match explain.options {
            Some(ExplainOption::AnalyzeVerbose) => (true, true),
            Some(ExplainOption::Analyze) => (true, false),
            Some(ExplainOption::Verbose) => (false, true),
            None => (false, false),
        };

        let plan = if analyze {
            LogicalPlan::Analyze(Analyze {
                verbose,
                input: plan,
                schema,
            })
        } else {
            let stringified_plans = vec![plan.to_stringified(PlanType::InitialLogicalPlan)];
            LogicalPlan::Explain(Explain {
                verbose,
                explain_format: ExplainFormat::Indent,
                plan,
                stringified_plans,
                schema,
                logical_optimization_succeeded: false,
            })
        };

        plan_with_metadata(
            plan,
            &InfluxQlMetadata {
                measurement_column_index,
                tag_key_columns: vec![],
            },
        )
    }

    fn rewrite_select_statement(&self, select: SelectStatement) -> Result<SelectQuery> {
        rewrite_statement(self.s, &select)
    }

    /// Create a [`LogicalPlan`] from the specified InfluxQL `SELECT` statement.
    fn select_query_to_plan(&self, query: &SelectQuery) -> Result<LogicalPlan> {
        let select = &query.select;

        let group_by_tags = if let Some(group_by) = select.group_by.as_ref() {
            group_by
                .tag_names()
                .map(|ident| ident.as_str())
                .sorted()
                .dedup()
                .collect()
        } else {
            vec![]
        };

        let ProjectionInfo {
            fields,
            group_by_tag_set,
            projection_tag_set,
            is_projected,
        } = ProjectionInfo::new(&select.fields, &group_by_tags);

        let order_by = select.order_by.unwrap_or_default();
        let time_alias = fields[0].name.as_str();
        let time_sort_expr = time_alias.as_expr().sort(
            match order_by {
                OrderByClause::Ascending => true,
                OrderByClause::Descending => false,
            },
            false,
        );

        let table_names = find_table_names(select);
        let sort_by_measurement = table_names.len() > 1;
        let mut plans = Vec::new();
        for table_name in table_names {
            let ctx = Context::new_root(table_name, select, &group_by_tags);

            let Some(plan) = self.union_from(&ctx, select)? else {
                continue;
            };

            let plan = self.project_select(&ctx, plan, &fields, &group_by_tag_set)?;

            // TODO(sgc): Handle FILL(N) and FILL(previous)
            //
            // See: https://github.com/influxdata/influxdb_iox/issues/8042

            let plan = self.limit(
                plan,
                select.offset,
                select.limit,
                &ctx.fill.unwrap_or_default(),
                vec![time_sort_expr.clone()],
                &group_by_tag_set,
                &projection_tag_set,
            )?;

            plans.push((table_name, plan));
        }

        let plan = {
            fn project_with_measurement(
                table_name: &str,
                input: LogicalPlan,
            ) -> Result<LogicalPlan> {
                if let LogicalPlan::Projection(Projection { expr, input, .. }) = input {
                    // Rewrite the existing projection with the measurement name column first
                    project(
                        input.deref().clone(),
                        iter::once(lit_dict(table_name).alias(INFLUXQL_MEASUREMENT_COLUMN_NAME))
                            .chain(expr),
                    )
                } else {
                    project(
                        input.clone(),
                        iter::once(lit_dict(table_name).alias(INFLUXQL_MEASUREMENT_COLUMN_NAME))
                            .chain(input.schema().iter().map(|(_qualifier, field)| {
                                Expr::Column(datafusion::common::Column::new_unqualified(
                                    field.name(),
                                ))
                            })),
                    )
                }
            }

            let mut iter = plans.into_iter();
            let plan = match iter.next() {
                Some((table_name, plan)) => project_with_measurement(table_name, plan),
                None => {
                    // empty result, but let's at least have all the strictly necessary metadata
                    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
                        INFLUXQL_MEASUREMENT_COLUMN_NAME,
                        (&InfluxColumnType::Tag).into(),
                        false,
                    )]));
                    let plan = LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: schema.to_dfschema_ref()?,
                    });
                    let plan = plan_with_metadata(
                        plan,
                        &InfluxQlMetadata {
                            measurement_column_index: MEASUREMENT_COLUMN_INDEX,
                            tag_key_columns: vec![],
                        },
                    )?;
                    return Ok(plan);
                }
            }?;

            iter.try_fold(plan, |prev, (table_name, input)| {
                let next = project_with_measurement(table_name, input)?;
                union_and_coerce(prev, next)
            })?
        };

        let plan = plan_with_metadata(
            plan,
            &InfluxQlMetadata {
                measurement_column_index: MEASUREMENT_COLUMN_INDEX,
                tag_key_columns: make_tag_key_column_meta(
                    &fields,
                    &group_by_tag_set,
                    &is_projected,
                ),
            },
        )?;

        plan_with_sort(
            plan,
            vec![time_sort_expr],
            sort_by_measurement,
            &group_by_tag_set,
            &projection_tag_set,
        )
    }

    fn subquery_to_plan(&self, ctx: &Context<'_>, select: &Select) -> Result<Option<LogicalPlan>> {
        let ctx = ctx.subquery(select);

        let Some(plan) = self.union_from(&ctx, select)? else {
            return Ok(None);
        };

        let group_by_tags = ctx.group_by_tags();
        let ProjectionInfo {
            fields,
            group_by_tag_set,
            projection_tag_set,
            ..
        } = ProjectionInfo::new(&select.fields, &group_by_tags);

        let plan = self.project_select(&ctx, plan, &fields, &group_by_tag_set)?;

        // the sort planner node must refer to the time column using
        // the alias that was specified
        let time_alias = fields[0].name.as_str();

        let time_sort_expr = time_alias.as_expr().sort(
            match ctx.order_by {
                OrderByClause::Ascending => true,
                OrderByClause::Descending => false,
            },
            false,
        );

        let plan = self.limit(
            plan,
            select.offset,
            select.limit,
            &ctx.fill.unwrap_or_default(),
            vec![time_sort_expr.clone()],
            &group_by_tag_set,
            &projection_tag_set,
        )?;

        plan_with_sort(
            plan,
            vec![time_sort_expr],
            false,
            &group_by_tag_set,
            &projection_tag_set,
        )
        .map(Some)
    }

    /// Returns a `LogicalPlan` that combines the `FROM` clause as a `UNION ALL`.
    fn union_from(&self, ctx: &Context<'_>, select: &Select) -> Result<Option<LogicalPlan>> {
        let mut plans = Vec::new();
        for ds in &select.from {
            let Some(plan) = self.plan_from_data_source(ctx, ds, &select.fields)? else {
                continue;
            };

            let schema = IQLSchema::new_from_ds_schema(plan.schema(), ds.schema(self.s)?)?;
            let plan = self.plan_condition(&ctx.tz, ctx.condition, plan, &schema)?;
            plans.push((plan, schema));
        }

        Ok(match plans.len() {
            0 | 1 => plans.pop().map(|(plan, _)| plan),
            2.. => {
                // find all the columns referenced in the `SELECT`
                let var_refs = find_var_refs(select);

                // Collect metadata from all source plans for all var refs
                // We need metadata to be consistent across all branches for DataFusion's aggregate
                // physical schema check
                let mut column_metadata: HashMap<String, HashMap<String, String>> = HashMap::new();
                for (plan, _) in &plans {
                    let schema = plan.schema();
                    for vr in &var_refs {
                        let name = vr.name.as_str();
                        if let Ok(field) = schema.field_with_unqualified_name(name)
                            && !field.metadata().is_empty()
                            && !column_metadata.contains_key(name)
                        {
                            column_metadata.insert(name.to_string(), field.metadata().clone());
                        }
                    }
                }

                let mut tags = HashMap::new();
                let plans = plans
                    .into_iter()
                    .map(|(plan, ds_schema)| {
                        let schema = plan.schema();
                        let select_exprs = var_refs.iter().map(|vr| {
                            // If the variable reference is a tag in one of the sources,
                            // but not in the other, then we need to produce an error.
                            let name = vr.name.as_str();
                            match (ds_schema.is_projected_tag_field(name), tags.get(name)) {
                                (v, None) => { tags.insert(name, v); Ok(()) },
                                (prev, Some(cur)) if &prev != cur => error::not_implemented(
                                        format!(
                                            "cannot mix tag and field columns with the same name: {name}"
                                        )),
                                _ => Ok(()),

                            }?;

                            if schema.has_column_with_unqualified_name(name) {
                                // Column exists - keep it as is
                                Ok(name.as_expr().alias(name))
                            } else {
                                // Column doesn't exist - create NULL literal with collected metadata
                                // This ensures consistent metadata across union branches
                                if let Some(metadata) = column_metadata.get(name) {
                                    Ok(lit_with_metadata(
                                        ScalarValue::Null,
                                        Some(FieldMetadata::from(metadata.clone()))
                                    ).alias(name))
                                } else {
                                    Ok(lit(ScalarValue::Null).alias(name))
                                }
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;

                        project(plan.clone(), select_exprs)
                    })
                    .collect::<Result<Vec<_>>>()?;

                let plan = {
                    let mut iter = plans.into_iter();
                    let plan = iter
                        .next()
                        .ok_or_else(|| error::map::internal("expected plan"))?;
                    iter.try_fold(plan, union_and_coerce)?
                };
                Some(plan)
            }
        })
    }

    fn project_select(
        &self,
        ctx: &Context<'_>,
        input: LogicalPlan,
        fields: &[Field],
        group_by_tag_set: &[&str],
    ) -> Result<LogicalPlan> {
        match ctx.projection_type {
            ProjectionType::Raw => self.project_select_raw(ctx, input, fields),
            ProjectionType::RawDistinct => self.project_select_raw_distinct(ctx, input, fields),
            ProjectionType::Aggregate => {
                self.project_select_aggregate(ctx, input, fields, group_by_tag_set)
            }
            ProjectionType::Window => {
                self.project_select_window(ctx, input, fields, group_by_tag_set)
            }
            ProjectionType::WindowAggregate => {
                self.project_select_window_aggregate(ctx, input, fields, group_by_tag_set)
            }
            ProjectionType::WindowAggregateMixed => {
                self.project_select_window_aggregate_mixed(ctx, input, fields, group_by_tag_set)
            }
            ProjectionType::Selector { .. } => {
                self.project_select_selector(ctx, input, fields, group_by_tag_set)
            }
            ProjectionType::TopBottomSelector => {
                self.project_select_top_bottom_selector(ctx, input, fields, group_by_tag_set)
            }
        }
    }

    /// Plan "Raw" SELECT queriers, These are queries that have no grouping
    /// and call only scalar functions.
    fn project_select_raw(
        &self,
        ctx: &Context<'_>,
        input: LogicalPlan,
        fields: &[Field],
    ) -> Result<LogicalPlan> {
        let schema = IQLSchema::new_from_fields(input.schema(), fields)?;

        // Transform InfluxQL AST field expressions to a list of DataFusion expressions.
        let select_exprs = self.field_list_to_exprs(&ctx.tz, &ctx.fill, &input, fields, &schema)?;

        // Wrap the plan in a `LogicalPlan::Projection` from the select expressions
        project(input, select_exprs)
    }

    /// Plan "RawDistinct" SELECT queriers, These are queries that have no grouping
    /// and call only scalar functions, but output only distinct rows.
    fn project_select_raw_distinct(
        &self,
        ctx: &Context<'_>,
        input: LogicalPlan,
        fields: &[Field],
    ) -> Result<LogicalPlan> {
        let schema = IQLSchema::new_from_fields(input.schema(), fields)?;

        // Transform InfluxQL AST field expressions to a list of DataFusion expressions.
        let mut select_exprs =
            self.field_list_to_exprs(&ctx.tz, &ctx.fill, &input, fields, &schema)?;

        // This is a special case, where exactly one column can be projected with a `DISTINCT`
        // clause or the `distinct` function.
        //
        // In addition, the time column is projected as the Unix epoch.

        let Some(time_column_index) = find_time_column_index(fields) else {
            return error::internal("unable to find time column");
        };

        // Take ownership of the alias, so we don't reallocate, and temporarily place a literal
        // `NULL` in its place.
        let Expr::Alias(Alias { name: alias, .. }) =
            std::mem::replace(&mut select_exprs[time_column_index], lit(ScalarValue::Null))
        else {
            return error::internal("time column is not an alias");
        };

        select_exprs[time_column_index] = if let Some(i) = ctx.interval {
            let stride = lit(ScalarValue::new_interval_mdn(0, 0, i.duration));
            let offset = i.offset.unwrap_or_default();

            date_bin_wallclock(stride, "time".as_expr(), lit_timestamp_nano(offset)).alias(alias)
        } else {
            ctx.default_timestamp().alias(alias)
        };

        // Wrap the plan in a `LogicalPlan::Projection` from the select expressions
        let plan = project(input, select_exprs)?;
        LogicalPlanBuilder::from(plan).distinct()?.build()
    }

    /// Plan "Aggregate" SELECT queries. These are queries that use one or
    /// more aggregate (but not window) functions.
    fn project_select_aggregate(
        &self,
        ctx: &Context<'_>,
        input: LogicalPlan,
        fields: &[Field],
        group_by_tag_set: &[&str],
    ) -> Result<LogicalPlan> {
        let schema = IQLSchema::new_from_fields(input.schema(), fields)?;
        // Transform InfluxQL AST field expressions to a list of DataFusion expressions.
        let select_exprs = self.field_list_to_exprs(&ctx.tz, &ctx.fill, &input, fields, &schema)?;

        let (plan, select_exprs) =
            self.select_aggregate(ctx, input, fields, select_exprs, group_by_tag_set)?;

        // Wrap the plan in a `LogicalPlan::Projection` from the select expressions
        project(plan, select_exprs).map_err(remove_aggr_count_from_error)
    }

    /// Plan "Window" SELECT queries. These are queries that use one or
    /// more window functions.
    fn project_select_window(
        &self,
        ctx: &Context<'_>,
        input: LogicalPlan,
        fields: &[Field],
        group_by_tag_set: &[&str],
    ) -> Result<LogicalPlan> {
        let schema = IQLSchema::new_from_fields(input.schema(), fields)?;

        // Transform InfluxQL AST field expressions to a list of DataFusion expressions.
        let select_exprs = self.field_list_to_exprs(&ctx.tz, &ctx.fill, &input, fields, &schema)?;

        let (plan, select_exprs) = Self::select_window(ctx, input, select_exprs, group_by_tag_set)?;
        let plan = filter_window_nulls(plan)?;

        // Wrap the plan in a `LogicalPlan::Projection` from the select expressions
        project(plan, select_exprs)
    }

    /// Plan "WindowAggregate" SELECT queries. These are queries that use
    /// a combination of window and nested aggregate functions.
    fn project_select_window_aggregate(
        &self,
        ctx: &Context<'_>,
        input: LogicalPlan,
        fields: &[Field],
        group_by_tag_set: &[&str],
    ) -> Result<LogicalPlan> {
        let schema = IQLSchema::new_from_fields(input.schema(), fields)?;

        // Transform InfluxQL AST field expressions to a list of DataFusion expressions.
        let select_exprs = self.field_list_to_exprs(&ctx.tz, &ctx.fill, &input, fields, &schema)?;

        let (plan, select_exprs) =
            self.select_aggregate(ctx, input, fields, select_exprs, group_by_tag_set)?;

        let (plan, select_exprs) = Self::select_window(ctx, plan, select_exprs, group_by_tag_set)?;
        let plan = filter_window_nulls(plan)?;

        // Wrap the plan in a `LogicalPlan::Projection` from the select expressions
        project(plan, select_exprs)
    }

    /// Plan "WindowAggregateMixed" SELECT queries. These are queries that use
    /// a combination of window and nested aggregate functions, along with
    /// additional aggregate functions.
    fn project_select_window_aggregate_mixed(
        &self,
        ctx: &Context<'_>,
        input: LogicalPlan,
        fields: &[Field],
        group_by_tag_set: &[&str],
    ) -> Result<LogicalPlan> {
        let schema = IQLSchema::new_from_fields(input.schema(), fields)?;

        // Transform InfluxQL AST field expressions to a list of DataFusion expressions.
        let select_exprs = self.field_list_to_exprs(&ctx.tz, &ctx.fill, &input, fields, &schema)?;

        let (plan, select_exprs) =
            self.select_aggregate(ctx, input, fields, select_exprs, group_by_tag_set)?;

        let (plan, select_exprs) = Self::select_window(ctx, plan, select_exprs, group_by_tag_set)?;

        // Filter on the originally requested time range.
        let plan = filter_requested_time_range(ctx, plan, fields, &select_exprs)?;

        // Wrap the plan in a `LogicalPlan::Projection` from the select expressions
        project(plan, select_exprs)
    }

    /// Plan the execution of SELECT queries that have the Selector projection
    /// type. These a queries that include a single FIRST, LAST, MAX, MIN,
    /// PERCENTILE, or SAMPLE function call, possibly requesting additional
    /// tags or fields.
    ///
    /// N.B SAMPLE is not yet implemented.
    fn project_select_selector(
        &self,
        ctx: &Context<'_>,
        input: LogicalPlan,
        fields: &[Field],
        group_by_tag_set: &[&str],
    ) -> Result<LogicalPlan> {
        let schema = IQLSchema::new_from_fields(input.schema(), fields)?;

        let (selector_index, field_key, plan) = match Selector::find_enumerated(fields)? {
            (_, Selector::First { .. })
            | (_, Selector::Last { .. })
            | (_, Selector::Max { .. })
            | (_, Selector::Min { .. }) => {
                // The FIRST, LAST, MAX & MIN selectors are implmented as specialised
                // forms of the equivilent aggregate implementaiion.
                return self.project_select_aggregate(ctx, input, fields, group_by_tag_set);
            }
            (idx, Selector::Percentile { field_key, n }) => {
                let window_perc_row = Expr::WindowFunction(Box::new(WindowFunction::new(
                    PERCENT_ROW_NUMBER.clone(),
                    vec![lit(n)],
                )))
                .partition_by(window_partition_by(ctx, input.schema(), group_by_tag_set))
                .order_by(vec![
                    field_key.as_expr().sort(true, false),
                    ctx.time_sort_expr(),
                ])
                .window_frame(WindowFrame::new_bounds(
                    WindowFrameUnits::Rows,
                    WindowFrameBound::Preceding(ScalarValue::Null),
                    WindowFrameBound::Following(ScalarValue::Null),
                ))
                .build()?;
                let perc_row_column_name = window_perc_row.schema_name().to_string();

                let window_row = Expr::WindowFunction(Box::new(WindowFunction::new(
                    WindowFunctionDefinition::WindowUDF(row_number_udwf()),
                    vec![],
                )))
                .partition_by(window_partition_by(ctx, input.schema(), group_by_tag_set))
                .order_by(vec![
                    field_key.as_expr().sort(true, false),
                    ctx.time_sort_expr(),
                ])
                .window_frame(WindowFrame::new_bounds(
                    WindowFrameUnits::Rows,
                    WindowFrameBound::Preceding(ScalarValue::Null),
                    WindowFrameBound::Following(ScalarValue::Null),
                ))
                .build()?;
                let row_column_name = window_row.schema_name().to_string();

                let filter_expr = binary_expr(
                    col(perc_row_column_name.clone()),
                    Operator::Eq,
                    col(row_column_name.clone()),
                );
                let plan = LogicalPlanBuilder::from(input)
                    .filter(field_key.as_expr().is_not_null())?
                    .window(vec![
                        window_perc_row.alias(perc_row_column_name),
                        window_row.alias(row_column_name),
                    ])?
                    .filter(filter_expr)?
                    .build()?;

                (idx, field_key, plan)
            }
            (_, Selector::Sample { field_key: _, n: _ }) => {
                return error::not_implemented("sample selector function");
            }

            (_, s) => {
                return error::internal(format!(
                    "unsupported selector function for ProjectionSelector {s}"
                ));
            }
        };

        let mut fields_vec = fields.to_vec();
        fields_vec[selector_index].expr = IQLExpr::VarRef(VarRef {
            name: field_key.clone(),
            data_type: None,
        });

        // Transform InfluxQL AST field expressions to a list of DataFusion expressions.
        let select_exprs =
            self.field_list_to_exprs(&ctx.tz, &ctx.fill, &plan, fields_vec.as_slice(), &schema)?;

        // Wrap the plan in a `LogicalPlan::Projection` from the select expressions
        project(plan, select_exprs)
    }

    /// Plan the execution of "TopBottomSelector" SELECT queries. These are
    /// queries that use the TOP or BOTTOM functions to select a number of
    /// rows from the ends of a partition..
    fn project_select_top_bottom_selector(
        &self,
        ctx: &Context<'_>,
        input: LogicalPlan,
        fields: &[Field],
        group_by_tag_set: &[&str],
    ) -> Result<LogicalPlan> {
        let schema = IQLSchema::new_from_fields(input.schema(), fields)?;

        let (selector_index, is_bottom, field_key, tag_keys, narg) =
            match Selector::find_enumerated(fields)? {
                (
                    idx,
                    Selector::Bottom {
                        field_key,
                        tag_keys,
                        n,
                    },
                ) => (idx, true, field_key, tag_keys, n),
                (
                    idx,
                    Selector::Top {
                        field_key,
                        tag_keys,
                        n,
                    },
                ) => (idx, false, field_key, tag_keys, n),
                (_, s) => {
                    return error::internal(format!(
                        "ProjectionTopBottomSelector used with unexpected selector function: {s}"
                    ));
                }
            };

        let mut fields_vec = fields.to_vec();
        fields_vec[selector_index].expr = IQLExpr::VarRef(VarRef {
            name: field_key.clone(),
            data_type: None,
        });
        let order_by = if is_bottom {
            SelectorWindowOrderBy::FieldAsc(field_key)
        } else {
            SelectorWindowOrderBy::FieldDesc(field_key)
        };

        let mut internal_group_by = group_by_tag_set.to_vec();
        for (i, tag_key) in tag_keys.iter().enumerate() {
            fields_vec.insert(
                selector_index + i + 1,
                Field {
                    expr: IQLExpr::VarRef(VarRef {
                        name: (*tag_key).clone(),
                        data_type: Some(VarRefDataType::Tag),
                    }),
                    name: (*tag_key).clone().take(),
                    data_type: None,
                },
            );
            internal_group_by.push(*tag_key);
        }

        // Transform InfluxQL AST field expressions to a list of DataFusion expressions.
        let select_exprs =
            self.field_list_to_exprs(&ctx.tz, &ctx.fill, &input, fields_vec.as_slice(), &schema)?;

        let plan = if !tag_keys.is_empty() {
            self.select_first(ctx, input, order_by, internal_group_by.as_slice(), 1)?
        } else {
            input
        };

        let plan = self.select_first(ctx, plan, order_by, group_by_tag_set, narg)?;

        // Wrap the plan in a `LogicalPlan::Projection` from the select expressions
        project(plan, select_exprs)
    }

    fn select_aggregate(
        &self,
        ctx: &Context<'_>,
        mut input: LogicalPlan,
        fields: &[Field],
        mut select_exprs: Vec<Expr>,
        group_by_tag_set: &[&str],
    ) -> Result<(LogicalPlan, Vec<Expr>)> {
        // If SELECT contains INTEGRAL, go ahead and process it first.
        // See select_integral_window for details
        let num_integrals = find_integral_udfs(&select_exprs).len();
        if num_integrals > 0 {
            (input, select_exprs) =
                self.select_integral_window(ctx, input, select_exprs, group_by_tag_set)?
        };
        // Find a list of unique aggregate expressions from the projection.
        //
        // For example, a projection such as:
        //
        // SELECT SUM(foo), SUM(foo) / COUNT(foo) ..
        //
        // will produce two aggregate expressions:
        //
        // [SUM(foo), COUNT(foo)]
        //
        // NOTE:
        //
        // It is possible this vector is empty, when all the fields in the
        // projection refer to columns that do not exist in the current
        // table.
        let (mut aggr_exprs, aggr_count) = find_aggregate_exprs(&select_exprs);

        // gather some time-related metadata
        let Some(time_column_index) = find_time_column_index(fields) else {
            return error::internal("unable to find time column");
        };

        let mut should_fill_expr = fields.iter().map(is_aggregate_field).collect::<Vec<_>>();

        // if there's only a single selector, wrap non-aggregated fields into that selector
        if let [selector] = aggr_exprs.as_slice() {
            let selector = selector.clone();

            if let Expr::AggregateFunction(mut agg) = selector.clone()
                && agg.func.name().starts_with("selector_")
                && aggr_count == 1
            {
                let selector_index = select_exprs
                    .iter()
                    .enumerate()
                    .find(|(_i, expr)| contains_expr(expr, &selector))
                    .map(|(i, _expr)| i)
                    .ok_or_else(|| error::map::internal("cannot find selector expression"))?;

                let group_by_tag_set = group_by_tag_set.iter().copied().collect::<HashSet<_>>();

                let mut additional_args = vec![];
                let mut fields_to_extract = vec![];
                for (idx, expr) in select_exprs.iter().enumerate() {
                    if (idx == time_column_index) || (idx == selector_index) {
                        continue;
                    }
                    let (expr, out_name) = match expr.clone() {
                        Expr::Alias(Alias {
                            expr,
                            relation: None,
                            name: out_name,
                            metadata: _,
                        }) => (*expr, out_name),
                        _ => {
                            return error::internal("other field is not aliased");
                        }
                    };
                    if group_by_tag_set.contains(&out_name.as_str()) {
                        continue;
                    }
                    additional_args.push(expr);
                    fields_to_extract.push((
                        idx,
                        format!("other_{}", additional_args.len()),
                        out_name,
                    ));
                }

                agg.params.args.append(&mut additional_args);
                let selector_new = Expr::AggregateFunction(agg);
                select_exprs[selector_index] = select_exprs[selector_index]
                    .clone()
                    .transform_up(&|expr| {
                        if expr == selector {
                            Ok(Transformed::yes(selector_new.clone()))
                        } else {
                            Ok(Transformed::no(expr))
                        }
                    })
                    .map(|t| t.data)
                    .expect("cannot fail");
                aggr_exprs[0] = selector_new.clone();

                for (idx, struct_name, out_alias) in fields_to_extract {
                    select_exprs[idx] = selector_new.clone().field(struct_name).alias(out_alias);
                    should_fill_expr[idx] = true;
                }
            }
        }

        // This block identifies the time column index and updates the time expression
        // based on the semantics of the projection.
        let time_column = {
            // Take ownership of the alias, so we don't reallocate, and temporarily place a literal
            // `NULL` in its place.
            let Expr::Alias(Alias { name: alias, .. }) =
                std::mem::replace(&mut select_exprs[time_column_index], lit(ScalarValue::Null))
            else {
                return error::internal("time column is not an alias");
            };

            // Rewrite the `time` column projection based on a series of rules in the following
            // order. If the query:
            //
            // 1. is binning by time, project the column using the `DATE_BIN` function,
            // 2. is a single-selector query, project the `time` field of the selector aggregate,
            // 3. otherwise, project the Unix epoch (0)
            select_exprs[time_column_index] = if let Some(i) = ctx.interval {
                let stride = lit(ScalarValue::new_interval_mdn(0, 0, i.duration));
                let offset = i.offset.unwrap_or_default();

                date_bin_wallclock(stride, "time".as_expr(), lit_timestamp_nano(offset))
            } else if let ProjectionType::Selector { has_fields: _ } = ctx.projection_type {
                let selector = match aggr_exprs.as_slice() {
                    [sel] => sel.clone(),
                    // Should have been validated by `select_statement_info`
                    _ => {
                        return error::internal(format!(
                            "internal: expected 1 selector expression, got {}",
                            aggr_exprs.len()
                        ));
                    }
                };

                selector.field("time")
            } else {
                ctx.default_timestamp()
            }
            .alias(alias);

            &select_exprs[time_column_index]
        };

        let aggr_group_by_exprs = {
            let schema = input.schema();

            let mut group_by_exprs = Vec::new();

            if ctx.group_by.and_then(|v| v.time_dimension()).is_some() {
                // Include the GROUP BY TIME(..) expression
                group_by_exprs.push(time_column.clone());
            }

            group_by_exprs.extend(group_by_tag_set.iter().filter_map(|name| {
                if schema.has_column_with_unqualified_name(name) {
                    Some(name.as_expr())
                } else {
                    None
                }
            }));

            group_by_exprs
        };

        if aggr_exprs.is_empty() && aggr_group_by_exprs.is_empty() {
            // If there are no aggregate expressions in the projection, because
            // they all referred to non-existent columns in the table, and there
            // is no GROUP BY, the result set is a single row.
            //
            // This is required for InfluxQL compatibility.
            return Ok((LogicalPlanBuilder::empty(true).build()?, select_exprs));
        }

        let schema = Arc::clone(input.schema());

        let plan = if aggr_group_by_exprs.is_empty() {
            // If there are no group by expressions then filter out
            // the case where there are no input rows at all.
            let mut aggr_exprs_with_count = aggr_exprs.clone();
            aggr_exprs_with_count.push(count(lit(1_i64)).alias("__aggr_count"));
            LogicalPlanBuilder::from(input)
                .aggregate(aggr_group_by_exprs.clone(), aggr_exprs_with_count)?
                .filter(col("__aggr_count").gt(lit(0_i64)))?
                .build()?
        } else {
            LogicalPlanBuilder::from(input)
                .aggregate(aggr_group_by_exprs.clone(), aggr_exprs.clone())?
                .build()?
        };

        let fill_option = ctx.fill();

        let num_exprs = aggr_exprs.len();
        // this specifies how we should handle the empty values in the gapfill that we need to add
        // to handle the GROUP BY clause below (if one exists)
        //
        // InfluxQL docs state that INTEGRAL does not support FILL. However, the behavior in 1.x is
        // more complex depending on the choice of fill option and the presence of other aggregates
        // in the query.
        let fill_strategy = match fill_option {
            // When the query contains all INTEGRAL aggregates, the query behaves as if the fill
            // clause is `fill(none)` regardless of what the fill clause actually specifies.
            _ if num_integrals == num_exprs => None,
            // If the user specified that nulls should be filled with a specific value, then we
            // need to take the value they gave us (`val`) and convert it to the correct type to
            // give to the gapfill
            Some(FillClause::Value(val)) => Some(
                aggr_exprs
                    .iter()
                    .map(|expr| {
                        expr_as_column_expr(expr, &plan)
                            .and_then(|expr| expr.get_type(plan.schema()))
                            .and_then(|ty| number_to_scalar(&val, &ty))
                            .map(FillStrategy::Default)
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
            // InfluxQL spec states that not including a fill clause should act exactly the same as
            // including a `FILL(NULL)`. FILL(NULL) fills all missing values with `NULL`s, except
            // for in the case of `COUNT`, and `COUNT` alone. missing `COUNT` values get
            // zero-filled.
            //
            // fill:
            // https://docs.influxdata.com/influxdb/v1/query_language/explore-data/#group-by-time-intervals-and-fill
            //
            // count:
            // https://docs.influxdata.com/influxdb/v1/query_language/functions/#common-issues-with-count
            Some(FillClause::Null) | None => Some(
                aggr_exprs
                    .iter()
                    .map(|expr| default_return_value_for_aggr_fn(expr, &schema, None))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .map(FillStrategy::Default)
                    .collect(),
            ),
            // with these, we just fill everything in the same way.
            Some(FillClause::Previous) => Some(vec![FillStrategy::PrevNullAsMissing; num_exprs]),
            // When INTEGRAL is used with other aggregates and `fill(linear)`, InfluxDB 1.0 fills
            // the integral column with nulls instead of calculating linear interpolation.
            Some(FillClause::Linear) if num_integrals > 0 => Some(
                aggr_exprs
                    .iter()
                    .map(|expr| {
                        Ok(if is_integral_aggr_expr(expr) {
                            FillStrategy::Default(default_return_value_for_aggr_fn(
                                expr, &schema, None,
                            )?)
                        } else {
                            FillStrategy::LinearInterpolate
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
            Some(FillClause::Linear) => Some(vec![FillStrategy::LinearInterpolate; num_exprs]),
            // If they specified Fill(NONE), then we don't want to gapfill any values because the
            // `NONE` specifies that all rows with null values should be removed.
            Some(FillClause::None) => None,
        };

        // Wrap the plan in a GapFill operator if the statement specifies a `GROUP BY TIME` clause and
        // the FILL option is one of
        //
        // * `null`
        // * `previous`
        // * `literal` value
        // * `linear`
        //
        let plan = if let (Some(_), Some(fill_strategy)) = (
            ctx.group_by.and_then(|gb| gb.time_dimension()),
            fill_strategy,
        ) {
            build_gap_fill_node(plan, fill_strategy, ctx.extended_time_range(), &ctx.tz)?
        } else {
            plan
        };

        // Combine the aggregate columns and group by expressions, which represents
        // the final projection from the aggregate operator.
        let aggr_projection_exprs = [aggr_group_by_exprs, aggr_exprs.clone()].concat();

        let fill_if_null = match fill_option {
            Some(FillClause::Value(v)) => Some(v),
            _ => None,
        };

        // Rewrite the aggregate columns from the projection, so that the expressions
        // refer to the columns from the aggregate projection
        let select_exprs_post_aggr = select_exprs
            .iter()
            .zip(should_fill_expr)
            .map(|(expr, should_fill)| {
                let fill_if_null = should_fill.then_some(fill_if_null).flatten();

                // the `build_gap_fill_node` function call above only handles the aggregate
                // expressions, such as `COUNT()` or `MEAN()`. InfluxQL, however, has the behavior
                rebase_expr(expr, &aggr_projection_exprs, &fill_if_null, &plan).map(|t| t.data)
            })
            .collect::<Result<Vec<Expr>>>()?;

        // the integral window function produces null values to adhere to DataFusion's RecordBatch
        // size invariant. To match 1.x behavior, we need to strip these nulls out when the query
        // has a `GROUP BY time` and there are no other aggregate functions.
        let plan = if num_integrals > 0
            && num_integrals == num_exprs
            && ctx.group_by.and_then(|gb| gb.time_dimension()).is_some()
            && let Some(filter_nulls) = disjunction(
                aggr_exprs
                    .into_iter()
                    .map(|expr| Ok(expr_as_column_expr(&expr, &plan)?.is_not_null()))
                    .collect::<Result<Vec<Expr>>>()?,
            ) {
            LogicalPlanBuilder::from(plan)
                .filter(filter_nulls)?
                .build()?
        } else {
            plan
        };

        // Fix the influxql filled metadata.
        let plan = plan
            .transform_down_up(
                |plan| {
                    if let LogicalPlan::Aggregate(mut aggregate) = plan {
                        let filled = aggregate
                            .group_expr
                            .iter()
                            .map(|_| false)
                            .chain(aggregate.aggr_expr.iter().map(|_| true))
                            .collect::<Vec<_>>();
                        aggregate.schema = schema_with_influxql_filled(aggregate.schema, &filled)?;
                        Ok(Transformed::new(
                            LogicalPlan::Aggregate(aggregate),
                            true,
                            TreeNodeRecursion::Jump,
                        ))
                    } else {
                        Ok(Transformed::no(plan))
                    }
                },
                |plan| {
                    if let LogicalPlan::Extension(Extension { node }) = plan {
                        if let Some(gap_fill) = node.as_any().downcast_ref::<GapFill>() {
                            // For GapFill copy the filled metadata from the input.
                            let filled = gap_fill
                                .input
                                .schema()
                                .fields()
                                .iter()
                                .map(|f| f.is_influxql_filled())
                                .collect::<Vec<_>>();

                            let new_schema =
                                schema_with_influxql_filled(Arc::clone(&gap_fill.schema), &filled)?;
                            let new_gap_fill = GapFill {
                                schema: new_schema,
                                ..gap_fill.clone()
                            };
                            Ok(Transformed::new(
                                LogicalPlan::Extension(Extension {
                                    node: Arc::new(new_gap_fill),
                                }),
                                true,
                                TreeNodeRecursion::Jump,
                            ))
                        } else {
                            Ok(Transformed::no(LogicalPlan::Extension(Extension { node })))
                        }
                    } else {
                        Ok(Transformed::no(plan))
                    }
                },
            )
            .map(|t| t.data)?;

        Ok((plan, select_exprs_post_aggr))
    }

    /// Generate a plan for any window functions, such as `moving_average` or `difference`.
    fn select_window(
        ctx: &Context<'_>,
        input: LogicalPlan,
        select_exprs: Vec<Expr>,
        group_by_tag_set: &[&str],
    ) -> Result<(LogicalPlan, Vec<Expr>)> {
        let udfs = find_window_udfs(&select_exprs);

        if udfs.is_empty() {
            return Ok((input, select_exprs));
        }

        let order_by = vec![ctx.time_sort_expr()];
        let partition_by =
            fields_to_exprs_no_nulls(input.schema(), group_by_tag_set).collect::<Vec<_>>();

        let window_func_exprs = udfs
            .clone()
            .into_iter()
            .map(|e| Self::udf_to_expr(ctx, e, partition_by.clone(), order_by.clone()))
            .collect::<Result<Vec<_>>>()?;

        // Recursively handle any nested window functions.
        let mut input_exprs: HashSet<Expr> = Default::default();
        for f in &window_func_exprs {
            f.apply(|e| {
                if let Expr::WindowFunction(windowfun) = e {
                    let WindowFunction { params, .. } = windowfun.as_ref();
                    input_exprs.extend(params.args.clone());
                    Ok(TreeNodeRecursion::Jump)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            })?;
        }
        let input_exprs = input_exprs.into_iter().collect::<Vec<_>>();
        let (input, modified_input_exprs) =
            Self::select_window(ctx, input, input_exprs.clone(), group_by_tag_set)?;
        let input_expr_map = input_exprs
            .into_iter()
            .zip(modified_input_exprs)
            .collect::<HashMap<_, _>>();
        let window_func_exprs = window_func_exprs
            .into_iter()
            .map(|e| {
                e.transform_down(|e| {
                    if let Expr::WindowFunction(windowfun) = e {
                        let WindowFunction { fun, params } = *windowfun;
                        let args = params
                            .args
                            .iter()
                            .map(|arg| {
                                input_expr_map
                                    .get(arg)
                                    .expect("window function inputs have been selected")
                                    .clone()
                            })
                            .collect();
                        let params = WindowFunctionParams { args, ..params };
                        Ok(Transformed::new(
                            Expr::WindowFunction(Box::new(WindowFunction { fun, params })),
                            true,
                            TreeNodeRecursion::Jump,
                        ))
                    } else {
                        Ok(Transformed::no(e))
                    }
                })
                .map(|t| t.data)
            })
            .collect::<Result<Vec<_>>>()?;

        let influxql_filled = input
            .schema()
            .fields()
            .iter()
            .map(|f| f.is_influxql_filled())
            .chain(std::iter::repeat_n(false, window_func_exprs.len()))
            .collect::<Vec<_>>();

        let plan = LogicalPlanBuilder::from(input)
            .window(window_func_exprs)?
            .build()?;

        let plan = plan
            .transform_down(|plan| {
                transform_plan_schema(plan, |schema| {
                    let schema = schema_with_influxql_filled(schema, &influxql_filled)?;
                    Ok(Transformed::new(schema, true, TreeNodeRecursion::Stop))
                })
            })
            .map(|t| t.data)?;

        // Rewrite the window columns from the projection, so that the expressions
        // refer to the columns from the window projection.
        let select_exprs = select_exprs
            .iter()
            .map(|expr| {
                expr.clone().transform_up(&|udf_expr| {
                    Ok(if udfs.contains(&udf_expr) {
                        Transformed::yes(expr_as_column_expr(&udf_expr, &plan)?)
                    } else {
                        Transformed::no(udf_expr)
                    })
                })
            })
            .map(|t| t.data())
            .collect::<Result<Vec<Expr>>>()?;

        Ok((plan, select_exprs))
    }

    /// Generate a plan to select the first n rows from each partition in
    /// the input data, optionally sorted by the requested field.
    fn select_first(
        &self,
        ctx: &Context<'_>,
        input: LogicalPlan,
        order_by: SelectorWindowOrderBy<'_>,
        group_by_tags: &[&str],
        count: i64,
    ) -> Result<LogicalPlan> {
        let order_by_exprs = match order_by {
            SelectorWindowOrderBy::FieldAsc(id) => {
                vec![id.as_expr().sort(true, false), ctx.time_sort_expr()]
            }
            SelectorWindowOrderBy::FieldDesc(id) => {
                vec![id.as_expr().sort(false, false), ctx.time_sort_expr()]
            }
        };

        let window_expr = Expr::WindowFunction(Box::new(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(row_number_udwf()),
            Vec::<Expr>::new(),
        )))
        .partition_by(window_partition_by(ctx, input.schema(), group_by_tags))
        .order_by(order_by_exprs)
        .window_frame(WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::Null),
            WindowFrameBound::CurrentRow,
        ))
        .build()?;
        let column_name = window_expr.schema_name().to_string();
        let filter_expr = binary_expr(col(column_name.clone()), Operator::LtEq, lit(count));
        LogicalPlanBuilder::from(input)
            .window(vec![window_expr.alias(column_name)])?
            .filter(filter_expr)?
            .build()
    }

    /// Transform a UDF to a window expression.
    fn udf_to_expr(
        ctx: &Context<'_>,
        e: Expr,
        partition_by: Vec<Expr>,
        order_by: Vec<SortExpr>,
    ) -> Result<Expr> {
        let alias = e.schema_name().to_string();

        let Expr::ScalarFunction(ScalarFunction { func, args }) = e else {
            return error::internal(format!("udf_to_expr: unexpected expression: {e}"));
        };

        fn derivative_unit(ctx: &Context<'_>, args: &[Expr]) -> Result<ScalarValue> {
            if args.len() > 1 {
                if let Expr::Literal(v, _) = &args[1] {
                    Ok(v.clone())
                } else {
                    error::internal(format!("udf_to_expr: unexpected expression: {}", args[1]))
                }
            } else if let Some(interval) = ctx.interval {
                Ok(ScalarValue::new_interval_mdn(0, 0, interval.duration))
            } else {
                Ok(ScalarValue::new_interval_mdn(0, 0, 1_000_000_000)) // 1s
            }
        }

        fn elapsed_unit(args: &[Expr]) -> Result<ScalarValue> {
            if args.len() > 1 {
                if let Expr::Literal(v, _) = &args[1] {
                    Ok(v.clone())
                } else {
                    error::internal(format!("udf_to_expr: unexpected expression: {}", args[1]))
                }
            } else {
                Ok(ScalarValue::new_interval_mdn(0, 0, 1))
            }
        }

        match udf::WindowFunction::try_from_scalar_udf(Arc::clone(&func)) {
            Some(udf::WindowFunction::MovingAverage) => {
                Ok(Expr::WindowFunction(Box::new(WindowFunction {
                    fun: MOVING_AVERAGE.clone(),
                    params: WindowFunctionParams {
                        args,
                        partition_by,
                        order_by,
                        window_frame: WindowFrame::new_bounds(
                            WindowFrameUnits::Rows,
                            WindowFrameBound::Preceding(ScalarValue::Null),
                            WindowFrameBound::Following(ScalarValue::Null),
                        ),
                        filter: None,
                        null_treatment: None,
                        distinct: false,
                    },
                }))
                .alias(alias))
            }
            Some(udf::WindowFunction::Difference) => {
                Ok(Expr::WindowFunction(Box::new(WindowFunction {
                    fun: DIFFERENCE.clone(),
                    params: WindowFunctionParams {
                        args,
                        partition_by,
                        order_by,
                        window_frame: WindowFrame::new_bounds(
                            WindowFrameUnits::Rows,
                            WindowFrameBound::Preceding(ScalarValue::Null),
                            WindowFrameBound::Following(ScalarValue::Null),
                        ),
                        filter: None,
                        null_treatment: None,
                        distinct: false,
                    },
                }))
                .alias(alias))
            }
            Some(udf::WindowFunction::Elapsed) => {
                Ok(Expr::WindowFunction(Box::new(WindowFunction {
                    fun: ELAPSED.clone(),
                    params: WindowFunctionParams {
                        args: vec![args[0].clone(), lit(elapsed_unit(&args)?), "time".as_expr()],
                        partition_by,
                        order_by,
                        window_frame: WindowFrame::new_bounds(
                            WindowFrameUnits::Rows,
                            WindowFrameBound::Preceding(ScalarValue::Null),
                            WindowFrameBound::Following(ScalarValue::Null),
                        ),
                        filter: None,
                        null_treatment: None,
                        distinct: false,
                    },
                }))
                .alias(alias))
            }
            Some(udf::WindowFunction::NonNegativeDifference) => {
                Ok(Expr::WindowFunction(Box::new(WindowFunction {
                    fun: NON_NEGATIVE_DIFFERENCE.clone(),
                    params: WindowFunctionParams {
                        args,
                        partition_by,
                        order_by,
                        window_frame: WindowFrame::new_bounds(
                            WindowFrameUnits::Rows,
                            WindowFrameBound::Preceding(ScalarValue::Null),
                            WindowFrameBound::Following(ScalarValue::Null),
                        ),
                        filter: None,
                        null_treatment: None,
                        distinct: false,
                    },
                }))
                .alias(alias))
            }
            Some(udf::WindowFunction::Derivative) => {
                Ok(Expr::WindowFunction(Box::new(WindowFunction {
                    fun: DERIVATIVE.clone(),
                    params: WindowFunctionParams {
                        args: vec![
                            args[0].clone(),
                            lit(derivative_unit(ctx, &args)?),
                            "time".as_expr(),
                        ],
                        partition_by,
                        order_by,
                        window_frame: WindowFrame::new_bounds(
                            WindowFrameUnits::Rows,
                            WindowFrameBound::Preceding(ScalarValue::Null),
                            WindowFrameBound::Following(ScalarValue::Null),
                        ),
                        filter: None,
                        null_treatment: None,
                        distinct: false,
                    },
                }))
                .alias(alias))
            }
            Some(udf::WindowFunction::NonNegativeDerivative) => {
                Ok(Expr::WindowFunction(Box::new(WindowFunction {
                    fun: NON_NEGATIVE_DERIVATIVE.clone(),
                    params: WindowFunctionParams {
                        args: vec![
                            args[0].clone(),
                            lit(derivative_unit(ctx, &args)?),
                            "time".as_expr(),
                        ],
                        partition_by,
                        order_by,
                        window_frame: WindowFrame::new_bounds(
                            WindowFrameUnits::Rows,
                            WindowFrameBound::Preceding(ScalarValue::Null),
                            WindowFrameBound::Following(ScalarValue::Null),
                        ),
                        filter: None,
                        null_treatment: None,
                        distinct: false,
                    },
                }))
                .alias(alias))
            }
            Some(udf::WindowFunction::CumulativeSum) => {
                Ok(Expr::WindowFunction(Box::new(WindowFunction {
                    fun: CUMULATIVE_SUM.clone(),
                    params: WindowFunctionParams {
                        args,
                        partition_by,
                        order_by,
                        window_frame: WindowFrame::new_bounds(
                            WindowFrameUnits::Rows,
                            WindowFrameBound::Preceding(ScalarValue::Null),
                            WindowFrameBound::Following(ScalarValue::Null),
                        ),
                        filter: None,
                        null_treatment: None,
                        distinct: false,
                    },
                }))
                .alias(alias))
            }
            None => error::internal(format!(
                "unexpected user-defined window function: {}",
                func.name()
            )),
        }
    }

    /// INTEGRAL is semantically an aggregate function, but has window-like properties due to the
    /// need to calculate adjacent points in a sliding window. To implement this in DataFusion,
    /// INTEGRAL is represented as a SUM aggregate that wraps an internal window function, as in
    /// the following pseudocode expression:
    ///      SUM(INTEGRAL(input_column) as integral_column)
    ///
    /// This function is similar to [`Self::select_window`] but specialized for integrals:
    ///
    /// - a Window` node is created in the input plan that calls the [`INTEGRAL`] UDWF for each
    ///   placeholder ScalarUDF that we find in `input_exprs`.
    ///
    /// - The list of `input_exprs` is transformed so that each of the placeholder ScalarUDFs are
    ///   substituted with `sum(integral_column)` where `integral_column` references the
    ///   output of one of the corresponding Window UDWF calls in the `Window` node.
    fn select_integral_window(
        &self,
        ctx: &Context<'_>,
        input: LogicalPlan,
        input_exprs: Vec<Expr>,
        group_by_tag_set: &[&str],
    ) -> Result<(LogicalPlan, Vec<Expr>)> {
        fn integral_unit(args: &[Expr]) -> Result<ScalarValue> {
            if args.len() > 1 {
                if let Expr::Literal(v, _) = &args[1] {
                    Ok(v.clone())
                } else {
                    error::internal(format!("udf_to_expr: unexpected expression: {}", args[1]))
                }
            } else {
                Ok(ScalarValue::new_interval_mdn(0, 0, 1_000_000_000)) // 1s
            }
        }

        let integral_udfs = find_integral_udfs(&input_exprs);

        let order_by = vec![ctx.time_sort_expr()];
        let partition_by =
            fields_to_exprs_no_nulls(input.schema(), group_by_tag_set).collect::<Vec<_>>();

        // Information about the `GROUP BY time` interval to pass into the INTEGRAL functions
        let (duration, offset) = match ctx.interval {
            None => (lit_duration_nano(0_i64), lit_duration_nano(0_i64)),
            Some(i) => (
                lit_duration_nano(i.duration),
                lit_duration_nano(i.offset.unwrap_or_default()),
            ),
        };
        let window_exprs = integral_udfs
            .iter()
            .map(|e| match e {
                Expr::ScalarFunction(ScalarFunction { args, .. }) if is_integral_udf(e) => {
                    let alias = e.schema_name().to_string();
                    Ok(Expr::WindowFunction(Box::new(WindowFunction {
                        fun: INTEGRAL_WINDOW.clone(),
                        params: WindowFunctionParams {
                            args: vec![
                                args[0].clone(),
                                lit(integral_unit(args)?),
                                "time".as_expr(),
                                duration.clone(),
                                offset.clone(),
                            ],
                            partition_by: partition_by.clone(),
                            order_by: order_by.clone(),
                            window_frame: WindowFrame::new_bounds(
                                WindowFrameUnits::Rows,
                                WindowFrameBound::Preceding(ScalarValue::Null),
                                WindowFrameBound::Following(ScalarValue::Null),
                            ),
                            filter: None,
                            null_treatment: None,
                            distinct: false,
                        },
                    }))
                    .alias(alias))
                }
                _ => error::internal(format!("unexpected expression: {e}",)),
            })
            .collect::<Result<Vec<_>>>()?;

        let plan = LogicalPlanBuilder::from(input)
            .window(window_exprs)?
            .build()?;

        // Rewrite the INTEGRAL calls from the projection, so that the expressions are a SUM that
        // refer to the columns from the window projection.
        let select_exprs = input_exprs
            .iter()
            .map(|expr| {
                expr.clone()
                    .transform_up(&|udf_expr| {
                        Ok(if integral_udfs.contains(&udf_expr) {
                            Transformed::yes(sum(expr_as_column_expr(&udf_expr, &plan)?))
                        } else {
                            Transformed::no(udf_expr)
                        })
                    })
                    .data()
            })
            .collect::<Result<Vec<Expr>>>()?;

        Ok((plan, select_exprs))
    }

    /// Generate a plan that partitions the input data into groups, first omitting a specified
    /// number of rows, followed by restricting the quantity of rows within each group.
    ///
    /// ## Arguments
    ///
    /// - `input`: The plan to apply the limit to.
    /// - `offset`: The number of input rows to skip.
    /// - `limit`: The maximum number of rows to return in the output plan per group.
    /// - `time_sort_expr`: An `Expr::Sort` referring to the `time` column of the input.
    /// - `group_by_tag_set`: Tag columns from the `input` plan that should be used to partition
    ///   the `input` plan and sort the `output` plan.
    /// - `projection_tag_set`: Additional tag columns that should be used to sort the `output`
    ///   plan.
    #[expect(clippy::too_many_arguments)]
    fn limit(
        &self,
        input: LogicalPlan,
        offset: Option<OffsetClause>,
        limit: Option<LimitClause>,
        fill_clause: &FillClause,
        sort_exprs: Vec<SortExpr>,
        group_by_tag_set: &[&str],
        projection_tag_set: &[&str],
    ) -> Result<LogicalPlan> {
        if offset.is_none() && limit.is_none() && !matches!(fill_clause, FillClause::Value(_)) {
            return Ok(input);
        }

        // If the query includes a GROUP BY tag[, tag, ...], the LIMIT and OFFSET clauses
        // are applied to each unique group. To accomplish this use a SeriesLimit operator.

        let order_by = sort_exprs
            .iter()
            .cloned()
            .chain(
                fields_to_exprs_no_nulls(input.schema(), projection_tag_set)
                    .map(|e| e.sort(true, false)),
            )
            .collect::<Vec<_>>();

        let partition_by =
            fields_to_exprs_no_nulls(input.schema(), group_by_tag_set).collect::<Vec<_>>();

        let limit_expr = input
            .schema()
            .fields()
            .iter()
            .filter(|f| {
                matches!(
                    f.data_type(),
                    DataType::Float64
                        | DataType::Int64
                        | DataType::UInt64
                        | DataType::Utf8
                        | DataType::Boolean
                )
            })
            .map(|f| {
                Ok(LimitExpr {
                    expr: Expr::Column(Column::new_unqualified(f.name())),
                    null_treatment: if f.is_influxql_filled() {
                        NullTreatment::RespectNulls
                    } else {
                        NullTreatment::IgnoreNulls
                    },
                    default_value: match fill_clause {
                        FillClause::Value(n) => number_to_scalar(n, f.data_type())
                            .or_else(|_| f.data_type().try_into())
                            .map(|v| Expr::Literal(v, None))?,
                        _ => f.data_type().try_into().map(|v| Expr::Literal(v, None))?,
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let limit = limit
            .map(|v| <u64 as TryInto<i64>>::try_into(*v))
            .transpose()
            .map_err(|_| error::map::query("limit out of range"))?
            .map(|v| v as usize);
        let offset = offset
            .map(|v| <u64 as TryInto<i64>>::try_into(*v))
            .transpose()
            .map_err(|_| error::map::query("offset out of range".to_owned()))?
            .map(|v| v as usize)
            .unwrap_or_default();

        LogicalPlanBuilder::from(input)
            .series_limit(partition_by, order_by, limit_expr, offset, limit)?
            .build()
    }

    /// Map the InfluxQL `SELECT` projection list into a list of DataFusion expressions.
    fn field_list_to_exprs(
        &self,
        tz: &Option<Arc<str>>,
        fill: &Option<FillClause>,
        plan: &LogicalPlan,
        fields: &[Field],
        schema: &IQLSchema<'_>,
    ) -> Result<Vec<Expr>> {
        let mut names: HashMap<&str, usize> = HashMap::new();
        fields
            .iter()
            .map(|field| {
                let mut new_field = field.clone();
                new_field.name = match names.entry(field.name.as_str()) {
                    Entry::Vacant(v) => {
                        v.insert(0);
                        field.name.clone()
                    }
                    Entry::Occupied(mut e) => {
                        let count = e.get_mut();
                        *count += 1;
                        format!("{}_{}", field.name, *count)
                    }
                };
                new_field
            })
            .map(|field| self.field_to_df_expr(tz, fill, &field, plan, schema))
            .collect()
    }

    /// Map an InfluxQL [`Field`] to a DataFusion [`Expr`].
    ///
    /// A [`Field`] is analogous to a column in a SQL `SELECT` projection.
    fn field_to_df_expr(
        &self,
        tz: &Option<Arc<str>>,
        fill: &Option<FillClause>,
        field: &Field,
        plan: &LogicalPlan,
        schema: &IQLSchema<'_>,
    ) -> Result<Expr> {
        let expr = self.expr_to_df_expr(
            tz,
            &Some(VirtualColumnFillConfig {
                fill_clause: *fill,
                data_type: field.data_type,
            }),
            // fill,
            // &field.data_type,
            ExprScope::Projection,
            &field.expr,
            schema,
        )?;
        let expr = planner_rewrite_expression::rewrite_field_expr(expr, schema)?;
        normalize_col(expr.data.alias(&field.name), plan)
    }

    /// Map an InfluxQL [`ConditionalExpression`] to a DataFusion [`Expr`].
    fn conditional_to_df_expr(
        &self,
        tz: &Option<Arc<str>>,
        iql: &ConditionalExpression,
        schema: &IQLSchema<'_>,
    ) -> Result<Expr> {
        match iql {
            ConditionalExpression::Expr(expr) => {
                self.expr_to_df_expr(tz, &None, ExprScope::Where, expr, schema)
            }
            ConditionalExpression::Binary(expr) => {
                self.binary_conditional_to_df_expr(tz, expr, schema)
            }
            ConditionalExpression::Grouped(e) => self.conditional_to_df_expr(tz, e, schema),
        }
    }

    /// Map an InfluxQL binary conditional expression to a DataFusion [`Expr`].
    fn binary_conditional_to_df_expr(
        &self,
        tz: &Option<Arc<str>>,
        expr: &ConditionalBinary,
        schema: &IQLSchema<'_>,
    ) -> Result<Expr> {
        let ConditionalBinary { lhs, op, rhs } = expr;

        Ok(binary_expr(
            self.conditional_to_df_expr(tz, lhs, schema)?,
            conditional_op_to_operator(*op)?,
            self.conditional_to_df_expr(tz, rhs, schema)?,
        ))
    }

    /// Map an InfluxQL [`IQLExpr`] to a DataFusion [`Expr`].
    fn expr_to_df_expr(
        &self,
        tz: &Option<Arc<str>>,
        fill_config: &Option<VirtualColumnFillConfig>,
        scope: ExprScope,
        iql: &IQLExpr,
        schema: &IQLSchema<'_>,
    ) -> Result<Expr> {
        match iql {
            // rewriter is expected to expand wildcard expressions
            IQLExpr::Wildcard(_) => error::internal("unexpected wildcard in projection"),
            IQLExpr::VarRef(varref) => self.varref_to_df_expr(fill_config, scope, varref, schema),
            IQLExpr::BindParameter(id) => {
                let err = BindParameterError::NotDefined(id.to_string());
                error::params(err.to_string())
            }
            IQLExpr::Literal(val) => match val {
                Literal::Integer(v) => Ok(lit(*v)),
                Literal::Unsigned(v) => Ok(lit(*v)),
                Literal::Float(v) => Ok(lit(*v)),
                Literal::String(v) => Ok(lit(v)),
                Literal::Boolean(v) => Ok(lit(*v)),
                Literal::Timestamp(v) => v
                    .timestamp_nanos_opt()
                    .ok_or_else(|| error::map::query("timestamp out of range"))
                    .map(|v| lit(ScalarValue::TimestampNanosecond(Some(v), tz.clone()))),
                Literal::Duration(v) => Ok(lit(ScalarValue::new_interval_mdn(0, 0, **v))),
                Literal::Regex(re) => match scope {
                    // a regular expression in a projection list is unexpected,
                    // as it should have been expanded by the rewriter.
                    ExprScope::Projection => {
                        error::internal("unexpected regular expression found in projection")
                    }
                    ExprScope::Where => Ok(lit(clean_non_meta_escapes(re.as_str()))),
                },
            },
            // A DISTINCT <ident> clause should have been replaced by `rewrite_statement`.
            IQLExpr::Distinct(_) => error::internal("distinct expression"),
            IQLExpr::Call(call) => self.call_to_df_expr(tz, fill_config, scope, call, schema),
            IQLExpr::Binary(expr) => {
                self.arithmetic_expr_to_df_expr(tz, fill_config, scope, expr, schema)
            }
            IQLExpr::Nested(e) => self.expr_to_df_expr(tz, fill_config, scope, e, schema),
        }
    }

    /// Map an InfluxQL variable reference to a DataFusion expression.
    fn varref_to_df_expr(
        &self,
        fill_config: &Option<VirtualColumnFillConfig>,
        scope: ExprScope,
        varref: &VarRef,
        schema: &IQLSchema<'_>,
    ) -> Result<Expr> {
        let df_schema = &schema.df_schema;
        let VarRef {
            name,
            data_type: opt_dst_type,
        } = varref;
        Ok(match (scope, name.as_str()) {
            // Per the Go implementation, the time column is case-insensitive in the
            // `WHERE` clause and disregards any postfix type cast operator.
            //
            // See: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L5751-L5753
            (ExprScope::Where, name) if name.eq_ignore_ascii_case("time") => "time".as_expr(),
            (ExprScope::Projection, "time") => "time".as_expr(),
            (_, name) => match df_schema
                .fields_with_unqualified_name(name)
                .first()
                .map(|f| f.data_type().clone())
            {
                Some(src_type) => {
                    let column = name.as_expr();

                    match opt_dst_type.and_then(var_ref_data_type_to_data_type) {
                        Some(dst_type) => {
                            fn is_numeric(dt: &DataType) -> bool {
                                matches!(dt, DataType::Int64 | DataType::Float64 | DataType::UInt64)
                            }

                            if src_type == dst_type {
                                column
                            } else if is_numeric(&src_type) && is_numeric(&dst_type) {
                                // InfluxQL only allows casting between numeric types,
                                // and it is safe to unconditionally unwrap, as the
                                // `is_numeric_type` call guarantees it can be mapped to
                                // an Arrow DataType
                                column.cast_to(&dst_type, &schema.df_schema)?
                            } else {
                                // If the cast is incompatible, evaluates to NULL
                                Expr::Literal(ScalarValue::Null, None)
                            }
                        }
                        None => column,
                    }
                }
                _ => {
                    // For non-existent columns, we need to check if the user specified a gap-filling value.
                    // See [`VirtualColumnFillConfig`] for more details.
                    match fill_config {
                        Some(VirtualColumnFillConfig {
                            fill_clause: Some(FillClause::Value(n)),
                            data_type,
                        }) => {
                            // The user specified a gap-filling value
                            match data_type {
                                Some(InfluxColumnType::Field(InfluxFieldType::Integer)) => {
                                    Expr::Literal(number_to_scalar(n, &DataType::Int64)?, None)
                                }
                                Some(InfluxColumnType::Field(InfluxFieldType::Float)) => {
                                    Expr::Literal(number_to_scalar(n, &DataType::Float64)?, None)
                                }
                                Some(InfluxColumnType::Tag) => {
                                    // Do not gap-fill tags
                                    Expr::Literal(ScalarValue::Null, None)
                                }
                                _ => {
                                    match n {
                                        // Default to the data type of the gap-filling value
                                        Number::Integer(_) => Expr::Literal(
                                            number_to_scalar(n, &DataType::Int64)?,
                                            None,
                                        ),
                                        Number::Float(_) => Expr::Literal(
                                            number_to_scalar(n, &DataType::Float64)?,
                                            None,
                                        ),
                                    }
                                }
                            }
                        }
                        _ => {
                            // No gap-filling config or value, return NULL
                            Expr::Literal(ScalarValue::Null, None)
                        }
                    }
                }
            },
        })
    }

    /// Map an InfluxQL function call to a DataFusion expression.
    ///
    /// A full list of supported functions available via the [InfluxQL documentation][docs].
    ///
    /// > **Note**
    /// >
    /// > These are not necessarily implemented, and are tracked by the following
    /// > issues:
    /// >
    /// > * <https://github.com/influxdata/influxdb_iox/issues/6934>
    /// > * <https://github.com/influxdata/influxdb_iox/issues/6935>
    /// > * <https://github.com/influxdata/influxdb_iox/issues/6937>
    /// > * <https://github.com/influxdata/influxdb_iox/issues/6938>
    /// > * <https://github.com/influxdata/influxdb_iox/issues/6939>
    ///
    /// [docs]: https://docs.influxdata.com/influxdb/v1.8/query_language/functions/
    fn call_to_df_expr(
        &self,
        tz: &Option<Arc<str>>,
        fill_config: &Option<VirtualColumnFillConfig>,
        scope: ExprScope,
        call: &Call,
        schema: &IQLSchema<'_>,
    ) -> Result<Expr> {
        if is_scalar_math_function(call.name.as_str()) {
            return self.scalar_math_func_to_df_expr(tz, fill_config, scope, call, schema);
        }

        match scope {
            ExprScope::Where => {
                if is_now_function(&call.name) {
                    error::not_implemented("now")
                } else {
                    let name = &call.name;
                    error::query(format!("invalid function call in condition: {name}"))
                }
            }
            ExprScope::Projection => self.function_to_df_expr(tz, fill_config, scope, call, schema),
        }
    }

    fn function_to_df_expr(
        &self,
        tz: &Option<Arc<str>>,
        fill_config: &Option<VirtualColumnFillConfig>,
        scope: ExprScope,
        call: &Call,
        schema: &IQLSchema<'_>,
    ) -> Result<Expr> {
        fn check_arg_count(name: &str, args: &[IQLExpr], count: usize) -> Result<()> {
            let got = args.len();
            if got != count {
                error::query(format!(
                    "invalid number of arguments for {name}: expected {count}, got {got}"
                ))
            } else {
                Ok(())
            }
        }

        fn check_arg_count_range(
            name: &str,
            args: &[IQLExpr],
            min: usize,
            max: usize,
        ) -> Result<()> {
            let got = args.len();
            if got < min || got > max {
                error::query(format!(
                    "invalid number of arguments for {name}: expected between {min} and {max}, got {got}"
                ))
            } else {
                Ok(())
            }
        }

        fn is_literal_null_or_number(expr: &Expr) -> bool {
            matches!(
                expr,
                Expr::Literal(
                    ScalarValue::Null | ScalarValue::Int64(_) | ScalarValue::Float64(_),
                    _
                )
            )
        }

        let Call { name, args } = call;

        match name.as_str() {
            // The DISTINCT function is handled as a `ProjectionType::RawDistinct`
            // query, so the planner only needs to project the single column
            // argument.
            "distinct" => self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema),
            "count" => {
                let count = count_udaf();
                let (expr, distinct) = match &args[0] {
                    IQLExpr::Call(c) if c.name == "distinct" => (
                        self.expr_to_df_expr(tz, fill_config, scope, &c.args[0], schema)?,
                        true,
                    ),
                    expr => (
                        self.expr_to_df_expr(tz, fill_config, scope, expr, schema)?,
                        false,
                    ),
                };

                // If the expression is a Null or a literal number, we can return it directly
                // without needing to create an aggregate function.
                //
                // So the query plan will look like:
                //  Int64(NULL)
                //
                // Without this check, the query plan will look like:
                //  coalesce_struct(count(Int64(NULL)), Int64(X))
                if is_literal_null_or_number(&expr) {
                    return Ok(expr);
                }

                check_arg_count("count", args, 1)?;
                Ok(Expr::AggregateFunction(expr::AggregateFunction {
                    func: count,
                    params: AggregateFunctionParams {
                        args: vec![expr],
                        distinct,
                        filter: None,
                        order_by: vec![],
                        null_treatment: None,
                    },
                }))
            }
            "sum" => {
                let sum = sum_udaf();
                let expr = self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema)?;

                // If the expression is a Null or a literal number, we can return it directly
                // without needing to create an aggregate function.
                //
                // So the query plan will look like:
                //  Int64(NULL)
                //
                // Without this check, the query plan will look like:
                //  coalesce_struct(sum(Int64(NULL)), Int64(X))
                if is_literal_null_or_number(&expr) {
                    return Ok(expr);
                }

                check_arg_count(name, args, 1)?;
                Ok(Expr::AggregateFunction(expr::AggregateFunction {
                    func: sum,
                    params: AggregateFunctionParams {
                        args: vec![expr],
                        distinct: false,
                        filter: None,
                        order_by: vec![],
                        null_treatment: None,
                    },
                }))
            }
            "stddev" => {
                let stddev = stddev_udaf();
                let expr = self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema)?;

                // If the expression is a Null or a literal number, we can return it directly
                // without needing to create an aggregate function.
                //
                // So the query plan will look like:
                //  Int64(NULL)
                //
                // Without this check, the query plan will look like:
                //  coalesce_struct(stddev(Int64(NULL)), Int64(X))
                if is_literal_null_or_number(&expr) {
                    return Ok(expr);
                }

                check_arg_count(name, args, 1)?;
                Ok(Expr::AggregateFunction(expr::AggregateFunction {
                    func: stddev,
                    params: AggregateFunctionParams {
                        args: vec![expr],
                        distinct: false,
                        filter: None,
                        order_by: vec![],
                        null_treatment: None,
                    },
                }))
            }
            "mean" => {
                let mean = avg_udaf();
                let expr = self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema)?;

                // If the expression is a Null or a literal number, we can return it directly
                // without needing to create an aggregate function.
                //
                // So the query plan will look like:
                //  Int64(NULL)
                //
                // Without this check, the query plan will look like:
                //  coalesce_struct(mean(Int64(NULL)), Int64(X))
                if is_literal_null_or_number(&expr) {
                    return Ok(expr);
                }

                check_arg_count(name, args, 1)?;
                Ok(Expr::AggregateFunction(expr::AggregateFunction {
                    func: mean,
                    params: AggregateFunctionParams {
                        args: vec![expr],
                        distinct: false,
                        filter: None,
                        order_by: vec![],
                        null_treatment: None,
                    },
                }))
            }
            "median" => {
                let median = median_udaf();
                let expr = self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema)?;

                // If the expression is a Null or a literal number, we can return it directly
                // without needing to create an aggregate function.
                //
                // So the query plan will look like:
                //  Int64(NULL)
                //
                // Without this check, the query plan will look like:
                //  coalesce_struct(median(Int64(NULL)), Int64(X))
                if is_literal_null_or_number(&expr) {
                    return Ok(expr);
                }

                check_arg_count(name, args, 1)?;
                Ok(Expr::AggregateFunction(expr::AggregateFunction {
                    func: median,
                    params: AggregateFunctionParams {
                        args: vec![expr],
                        distinct: false,
                        filter: None,
                        order_by: vec![],
                        null_treatment: None,
                    },
                }))
            }
            "mode" => {
                let expr = self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema)?;

                // If the expression is a Null or a literal number, we can return it directly
                // without needing to create an aggregate function.
                //
                // So the query plan will look like:
                //  Int64(NULL)
                //
                // Without this check, the query plan will look like:
                //  coalesce_struct(mode(Int64(NULL)), Int64(X))
                if is_literal_null_or_number(&expr) {
                    return Ok(expr);
                }

                check_arg_count(name, args, 1)?;
                Ok(Expr::AggregateFunction(expr::AggregateFunction {
                    func: MODE.clone(),
                    params: AggregateFunctionParams {
                        args: vec![expr, "time".as_expr()],
                        distinct: false,
                        filter: None,
                        order_by: vec![],
                        null_treatment: None,
                    },
                }))
            }
            "percentile" => {
                let expr = self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema)?;

                // If the expression is a Null or a literal number, we can return it directly
                // without needing to create an aggregate function.
                //
                // So the query plan will look like:
                //  Int64(NULL)
                //
                // Without this check, the query plan will look like:
                //  coalesce_struct(percentile(Int64(NULL)), Int64(X))
                if is_literal_null_or_number(&expr) {
                    return Ok(expr);
                }

                check_arg_count(name, args, 2)?;
                let nexpr = self.expr_to_df_expr(tz, fill_config, scope, &args[1], schema)?;
                Ok(Expr::AggregateFunction(expr::AggregateFunction {
                    func: PERCENTILE.clone(),
                    params: AggregateFunctionParams {
                        args: vec![expr, nexpr],
                        distinct: false,
                        filter: None,
                        order_by: vec![],
                        null_treatment: None,
                    },
                }))
            }
            "spread" => {
                let expr = self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema)?;

                // If the expression is a Null or a literal number, we can return it directly
                // without needing to create an aggregate function.
                //
                // So the query plan will look like:
                //  Int64(NULL)
                //
                // Without this check, the query plan will look like:
                //  coalesce_struct(spread(Int64(NULL)), Int64(X))
                if is_literal_null_or_number(&expr) {
                    return Ok(expr);
                }

                check_arg_count(name, args, 1)?;
                Ok(Expr::AggregateFunction(expr::AggregateFunction {
                    func: SPREAD.clone(),
                    params: AggregateFunctionParams {
                        args: vec![expr],
                        distinct: false,
                        filter: None,
                        order_by: vec![],
                        null_treatment: None,
                    },
                }))
            }
            name @ ("first" | "last" | "min" | "max") => {
                let expr = self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema)?;

                // If the expression is a Null or a literal number, we can return it directly
                // without needing to create an aggregate function.
                //
                // So the query plan will look like:
                //  Int64(NULL)
                //
                // Without this check, the query plan will look like:
                //  coalesce_struct(first(Int64(NULL)), Int64(X))
                if is_literal_null_or_number(&expr) {
                    return Ok(expr);
                }

                let selector_udf = match name {
                    "first" => selector_first(),
                    "last" => selector_last(),
                    "max" => selector_max(),
                    "min" => selector_min(),
                    _ => unreachable!(),
                }
                .call(vec![expr, "time".as_expr()]);

                Ok(selector_udf.field("value"))
            }
            "difference" => {
                check_arg_count(name, args, 1)?;

                // arg0 should be a column or function
                let arg0 = self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema)?;

                // If the expression is a Null or a literal number, we can return it directly
                // without needing to create an aggregate function.
                //
                // So the query plan will look like:
                //  Int64(NULL)
                //
                // Without this check, the query plan will look like:
                //  coalesce_struct(difference(Int64(NULL)), Int64(X))
                if is_literal_null_or_number(&arg0) {
                    return Ok(arg0);
                }

                Ok(difference(vec![arg0]))
            }
            "elapsed" => {
                check_arg_count_range(name, args, 1, 2)?;

                // arg0 should be a column or function
                let arg0 = self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema)?;

                // If the expression is a Null or a literal number, we can return it directly
                // without needing to create an aggregate function.
                //
                // So the query plan will look like:
                //  Int64(NULL)
                //
                // Without this check, the query plan will look like:
                //  coalesce_struct(elapsed(Int64(NULL)), Int64(X))
                if is_literal_null_or_number(&arg0) {
                    return Ok(arg0);
                }

                let mut eargs = vec![arg0];
                if args.len() > 1 {
                    let arg1 = self.expr_to_df_expr(tz, fill_config, scope, &args[1], schema)?;
                    eargs.push(arg1);
                }

                Ok(elapsed(eargs))
            }
            "non_negative_difference" => {
                check_arg_count(name, args, 1)?;

                // arg0 should be a column or function
                let arg0 = self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema)?;

                // If the expression is a Null or a literal number, we can return it directly
                // without needing to create an aggregate function.
                //
                // So the query plan will look like:
                //  Int64(NULL)
                //
                // Without this check, the query plan will look like:
                //  coalesce_struct(non_negative_difference(Int64(NULL)), Int64(X))
                if is_literal_null_or_number(&arg0) {
                    return Ok(arg0);
                }

                Ok(non_negative_difference(vec![arg0]))
            }
            "moving_average" => {
                check_arg_count(name, args, 2)?;

                // arg0 should be a column or function
                let arg0 = self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema)?;

                // If the expression is a Null or a literal number, we can return it directly
                // without needing to create an aggregate function.
                //
                // So the query plan will look like:
                //  Int64(NULL)
                //
                // Without this check, the query plan will look like:
                //  coalesce_struct(moving_average(Int64(NULL)), Int64(X))
                if is_literal_null_or_number(&arg0) {
                    return Ok(arg0);
                }

                // arg1 should be an integer.
                let arg1 = ScalarValue::Int64(Some(
                    match self.expr_to_df_expr(tz, fill_config, scope, &args[1], schema)? {
                        Expr::Literal(ScalarValue::Int64(Some(v)), _) => v,
                        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => v as i64,
                        _ => {
                            return error::query(
                                "moving_average expects number for second argument",
                            );
                        }
                    },
                ));

                Ok(moving_average(vec![arg0, lit(arg1)]))
            }
            "derivative" => {
                check_arg_count_range(name, args, 1, 2)?;

                // arg0 should be a column or function
                let arg0 = self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema)?;

                // If the expression is a Null or a literal number, we can return it directly
                // without needing to create an aggregate function.
                //
                // So the query plan will look like:
                //  Int64(NULL)
                //
                // Without this check, the query plan will look like:
                //  coalesce_struct(derivative(Int64(NULL)), Int64(X))
                if is_literal_null_or_number(&arg0) {
                    return Ok(arg0);
                }

                let mut eargs = vec![arg0];
                if args.len() > 1 {
                    let arg1 = self.expr_to_df_expr(tz, fill_config, scope, &args[1], schema)?;
                    eargs.push(arg1);
                }

                Ok(derivative(eargs))
            }
            "integral" => {
                check_arg_count_range(name, args, 1, 2)?;

                // arg0 should be a column or function
                let arg0 = self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema)?;

                // If the expression is a Null or a literal number, we can return it directly
                // without needing to create an aggregate function.
                //
                // So the query plan will look like:
                //  Int64(NULL)
                //
                // Without this check, the query plan will look like:
                //  coalesce_struct(derivative(Int64(NULL)), Int64(X))
                if is_literal_null_or_number(&arg0) {
                    return Ok(arg0);
                }

                let mut eargs = vec![arg0];
                if args.len() > 1 {
                    let arg1 = self.expr_to_df_expr(tz, fill_config, scope, &args[1], schema)?;
                    eargs.push(arg1);
                }

                Ok(integral(eargs))
            }
            "non_negative_derivative" => {
                check_arg_count_range(name, args, 1, 2)?;

                // arg0 should be a column or function
                let arg0 = self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema)?;

                // If the expression is a Null or a literal number, we can return it directly
                // without needing to create an aggregate function.
                //
                // So the query plan will look like:
                //  Int64(NULL)
                //
                // Without this check, the query plan will look like:
                //  coalesce_struct(non_negative_derivative(Int64(NULL)), Int64(X))
                if is_literal_null_or_number(&arg0) {
                    return Ok(arg0);
                }

                let mut eargs = vec![arg0];
                if args.len() > 1 {
                    let arg1 = self.expr_to_df_expr(tz, fill_config, scope, &args[1], schema)?;
                    eargs.push(arg1);
                }

                Ok(non_negative_derivative(eargs))
            }
            "cumulative_sum" => {
                check_arg_count(name, args, 1)?;

                // arg0 should be a column or function
                let arg0 = self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema)?;

                // If the expression is a Null or a literal number, we can return it directly
                // without needing to create an aggregate function.
                //
                // So the query plan will look like:
                //  Int64(NULL)
                //
                // Without this check, the query plan will look like:
                //  coalesce_struct(cumulative_sum(Int64(NULL)), Int64(X))
                if is_literal_null_or_number(&arg0) {
                    return Ok(arg0);
                }

                Ok(cumulative_sum(vec![arg0]))
            }
            // The TOP/BOTTOM function is handled as a `ProjectionType::TopBottomSelector`
            // query, so the planner only needs to project the single column
            // argument.
            "top" | "bottom" => self.expr_to_df_expr(tz, fill_config, scope, &args[0], schema),

            _ => error::query(format!("Invalid function '{name}'")),
        }
    }

    /// Map the InfluxQL scalar function call to a DataFusion scalar function expression.
    fn scalar_math_func_to_df_expr(
        &self,
        tz: &Option<Arc<str>>,
        fill_config: &Option<VirtualColumnFillConfig>,
        scope: ExprScope,
        call: &Call,
        schema: &IQLSchema<'a>,
    ) -> Result<Expr> {
        let mut args = call
            .args
            .iter()
            .map(|e| self.expr_to_df_expr(tz, fill_config, scope, e, schema))
            .collect::<Result<Vec<Expr>>>()?;

        let func = self
            .iox_ctx
            .inner()
            .state()
            .udf(call.name.as_str())
            .map_err(|_| {
                DataFusionError::Plan(format!(
                    "There is no UDF function named {}",
                    call.name.as_str()
                ))
            })?;

        /// Ensure that any literal parameter is a Float64, otherwise
        /// type coercion might choose to coerce to a Float32 instead.
        fn f64_lit(e: Expr) -> Expr {
            match e {
                Expr::Literal(ScalarValue::Int64(v), md) => {
                    Expr::Literal(ScalarValue::Float64(v.map(|v| v as f64)), md)
                }
                Expr::Literal(ScalarValue::UInt64(v), md) => {
                    Expr::Literal(ScalarValue::Float64(v.map(|v| v as f64)), md)
                }
                e => e,
            }
        }

        match call.name.as_str() {
            // log function
            //
            // Note in InfluxQL log() params order is different than that in DataFusion.
            //   DataFusion supports log(base, x)
            //   InfluxQL supports log(x, base)
            //   e.g in DataFusion log(base, x) == in InfluxQL log(x, base)
            //
            // https://github.com/apache/datafusion/blob/5d4468579481b36e53333c62bfd2440af1de1155/datafusion/functions/src/math/log.rs#L90
            "log" => {
                if args.len() != 2 {
                    error::query(format!(
                        "invalid number of arguments for log, expected 2, got {}",
                        args.len()
                    ))
                } else {
                    let arg1 = f64_lit(args.pop().unwrap());
                    let arg0 = args.pop().unwrap();
                    // reverse args
                    Ok(datafusion::prelude::log(arg1, arg0))
                }
            }
            "atan2" => {
                if args.len() != 2 {
                    error::query(format!(
                        "invalid number of arguments for atan2, expected 2, got {}",
                        args.len()
                    ))
                } else {
                    let arg1 = f64_lit(args.pop().unwrap());
                    let arg0 = f64_lit(args.pop().unwrap());
                    Ok(datafusion::prelude::atan2(arg0, arg1))
                }
            }
            _ => Ok(Expr::ScalarFunction(ScalarFunction { func, args })),
        }
    }

    /// Map an InfluxQL arithmetic expression to a DataFusion [`Expr`].
    fn arithmetic_expr_to_df_expr(
        &self,
        tz: &Option<Arc<str>>,
        fill_config: &Option<VirtualColumnFillConfig>,
        scope: ExprScope,
        expr: &Binary,
        schema: &IQLSchema<'_>,
    ) -> Result<Expr> {
        Ok(binary_expr(
            self.expr_to_df_expr(tz, fill_config, scope, &expr.lhs, schema)?,
            binary_operator_to_df_operator(expr.op),
            self.expr_to_df_expr(tz, fill_config, scope, &expr.rhs, schema)?,
        ))
    }

    fn plan_condition(
        &self,
        tz: &Option<Arc<str>>,
        condition: Option<&ConditionalExpression>,
        plan: LogicalPlan,
        schema: &IQLSchema<'a>,
    ) -> Result<LogicalPlan> {
        let filter_expr = condition
            .map(|condition| {
                let filter_expr = self.conditional_to_df_expr(tz, condition, schema)?;
                planner_rewrite_expression::rewrite_conditional_expr(
                    self.s.execution_props(),
                    filter_expr,
                    schema,
                )
            })
            .transpose()?;

        match (filter_expr, plan) {
            // Add to an existing filter, if possible.
            (Some(expr), LogicalPlan::Filter(filter)) => {
                let Filter {
                    predicate, input, ..
                } = filter;
                Ok(LogicalPlan::Filter(Filter::try_new(
                    predicate.and(expr),
                    input,
                )?))
            }
            (Some(expr), plan) => LogicalPlanBuilder::from(plan).filter(expr)?.build(),
            (None, plan) => Ok(plan),
        }
    }

    fn plan_time_range(
        &self,
        tz: &Option<Arc<str>>,
        time_range: TimeRange,
        plan: LogicalPlan,
    ) -> Result<LogicalPlan> {
        let time_expr = time_range_to_df_expr("time", tz, time_range);
        if let Some(expr) = time_expr {
            LogicalPlanBuilder::from(plan).filter(expr)?.build()
        } else {
            Ok(plan)
        }
    }

    /// Generate a logical plan that filters the existing plan based on the
    /// InfluxQL [`WhereClause`] of a `SHOW` statement.
    fn plan_where_clause(
        &self,
        plan: LogicalPlan,
        condition: &Option<WhereClause>,
        cutoff: MetadataCutoff,
        schema: &IQLSchema<'_>,
    ) -> Result<LogicalPlan> {
        let start_time = Timestamp::from(self.s.execution_props().query_execution_start_time);

        let (cond, time_range) = condition
            .as_ref()
            .map(|where_clause| {
                let rc = ReduceContext {
                    now: Some(start_time),
                    tz: None,
                };

                split_cond(&rc, where_clause).map_err(error::map::expr_error)
            })
            .transpose()?
            .unwrap_or_default();

        // Add time restriction to logical plan if there isn't any.
        let time_range = if time_range.is_unbounded() {
            TimeRange {
                lower: Some(match cutoff {
                    MetadataCutoff::Absolute(dt) => dt
                        .timestamp_nanos_opt()
                        .ok_or_else(|| error::map::query("timestamp out of range"))?,
                    MetadataCutoff::Relative(delta) => {
                        start_time
                            .timestamp_nanos_opt()
                            .ok_or_else(|| error::map::query("timestamp out of range"))?
                            - delta.as_nanos() as i64
                    }
                }),
                upper: None,
            }
        } else {
            time_range
        };

        let plan = self.plan_time_range(&None, time_range, plan)?;
        self.plan_condition(&None, cond.as_ref(), plan, schema)
    }

    /// Generate a logical plan for the specified `DataSource`. If the data source is a table then
    /// the plan will include a filter to ensure that at least one of the fields used in the query is
    /// not NULL. This ensures that the output is compatible with older versions of InfluxDB.
    fn plan_from_data_source(
        &self,
        ctx: &Context<'_>,
        ds: &DataSource,
        fields: &Vec<Field>,
    ) -> Result<Option<LogicalPlan>> {
        match ds {
            DataSource::Table(table_name) if table_name == ctx.table_name => {
                // `rewrite_statement` guarantees the table should exist
                let source = self.s.get_table_provider(table_name)?;
                let table_ref = TableReference::bare(table_name.to_owned());
                let mut builder = LogicalPlanBuilder::scan(table_ref, Arc::clone(&source), None)?;

                let mut field_set = BTreeSet::new();
                for field in fields {
                    field.expr.accept(SourceFieldNamesVisitor(&mut field_set))?;
                }
                let expr = disjunction(
                    field_set
                        .iter()
                        .filter(|&name| source.schema().field_with_name(name).is_ok())
                        .map(|name| Expr::Column(Column::from_name(name)).is_not_null()),
                );
                if let Some(expr) = expr {
                    builder = builder.filter(expr)?;
                }
                builder = ctx.project_timezone(builder)?;
                let plan =
                    self.plan_time_range(&ctx.tz, ctx.extended_time_range(), builder.build()?)?;
                Ok(Some(plan))
            }
            DataSource::Table(_) => Ok(None),
            DataSource::Subquery(select) => self
                .subquery_to_plan(ctx, select)?
                .map(|plan| {
                    ctx.project_timezone(LogicalPlanBuilder::from(plan))
                        .and_then(LogicalPlanBuilder::build)
                })
                .transpose(),
        }
    }

    /// Create a [LogicalPlan] that refers to the specified `table_name`.
    ///
    /// Normally, this functions will not return a `None`, as tables have been matched]
    /// by the [`rewrite_statement`] function.
    fn create_table_ref(&self, table_name: &str) -> Result<Option<(LogicalPlan, Vec<Expr>)>> {
        Ok(if let Ok(source) = self.s.get_table_provider(table_name) {
            let table_ref = TableReference::bare(table_name.to_owned());
            Some((
                LogicalPlanBuilder::scan(table_ref, source, None)?.build()?,
                vec![lit_dict(table_name).alias(INFLUXQL_MEASUREMENT_COLUMN_NAME)],
            ))
        } else {
            None
        })
    }

    /// Expand tables from `FROM` clause in metadata queries.
    fn expand_show_from_clause(&self, from: Option<ShowFromClause>) -> Result<Vec<String>> {
        match from {
            None => {
                let mut tables = self
                    .s
                    .table_names()
                    .into_iter()
                    .map(|s| s.to_owned())
                    .collect::<Vec<_>>();
                tables.sort();
                Ok(tables)
            }
            Some(from) => {
                let all_tables = self.s.table_names().into_iter().collect::<HashSet<_>>();
                let mut out = HashSet::new();
                for qualified_name in &*from {
                    if qualified_name.database.is_some() {
                        return error::not_implemented("database name in from clause");
                    }
                    if qualified_name.retention_policy.is_some() {
                        return error::not_implemented("retention policy in from clause");
                    }
                    match &qualified_name.name {
                        MeasurementName::Name(name) => {
                            let name = name.as_str();
                            if all_tables.contains(name) {
                                out.insert(name);
                            }
                        }
                        MeasurementName::Regex(regex) => {
                            let regex = parse_regex(regex)?;
                            for name in &all_tables {
                                if regex.is_match(name) {
                                    out.insert(name);
                                }
                            }
                        }
                    }
                }

                let mut out = out.into_iter().map(|s| s.to_owned()).collect::<Vec<_>>();
                out.sort();
                Ok(out)
            }
        }
    }

    fn expand_with_measurement_clause(
        &self,
        with_measurement: Option<WithMeasurementClause>,
    ) -> Result<Vec<String>> {
        match with_measurement {
            Some(
                WithMeasurementClause::Equals(qualified_name)
                | WithMeasurementClause::Regex(qualified_name),
            ) if qualified_name.database.is_some() => {
                error::not_implemented("database name in from clause")
            }
            Some(
                WithMeasurementClause::Equals(qualified_name)
                | WithMeasurementClause::Regex(qualified_name),
            ) if qualified_name.retention_policy.is_some() => {
                error::not_implemented("retention policy in from clause")
            }
            Some(WithMeasurementClause::Equals(qualified_name)) => match qualified_name.name {
                MeasurementName::Name(n) => {
                    let names = self.s.table_names();
                    let tables = if names.into_iter().any(|table| table == n.as_str()) {
                        vec![n.as_str().to_owned()]
                    } else {
                        vec![]
                    };
                    Ok(tables)
                }
                MeasurementName::Regex(_) => error::query("expected string but got regex"),
            },
            Some(WithMeasurementClause::Regex(qualified_name)) => match &qualified_name.name {
                MeasurementName::Name(_) => error::query("expected regex but got string"),
                MeasurementName::Regex(regex) => {
                    let regex = parse_regex(regex)?;
                    let mut tables = self
                        .s
                        .table_names()
                        .into_iter()
                        .filter(|s| regex.is_match(s))
                        .map(|s| s.to_owned())
                        .collect::<Vec<_>>();
                    tables.sort();
                    Ok(tables)
                }
            },
            None => {
                let mut tables = self
                    .s
                    .table_names()
                    .into_iter()
                    .map(|s| s.to_owned())
                    .collect::<Vec<_>>();
                tables.sort();
                Ok(tables)
            }
        }
    }

    fn show_tag_keys_to_plan(&self, show_tag_keys: ShowTagKeysStatement) -> Result<LogicalPlan> {
        if show_tag_keys.database.is_some() {
            // How do we handle this? Do we need to perform cross-namespace queries here?
            return error::not_implemented("SHOW TAG KEYS ON <database>");
        }

        let tag_key_col = "tagKey";
        let output_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                INFLUXQL_MEASUREMENT_COLUMN_NAME,
                (&InfluxColumnType::Tag).into(),
                false,
            ),
            ArrowField::new(tag_key_col, (&InfluxColumnType::Tag).into(), false),
        ]));

        let tables = self.expand_show_from_clause(show_tag_keys.from)?;

        let mut builder = match show_tag_keys.condition {
            Some(condition) => {
                debug!("`SHOW TAG KEYS` w/ WHERE-clause, use data scan plan",);

                let condition = Some(condition);
                let metadata_cutoff = self.metadata_cutoff();

                let mut union_plan = None;
                for table in tables {
                    let Some(table_schema) = self.s.table_schema(&table) else {
                        continue;
                    };
                    let Some((plan, measurement_expr)) = self.create_table_ref(&table)? else {
                        continue;
                    };

                    let ds = DataSource::Table(table.clone());
                    let schema = IQLSchema::new_from_ds_schema(plan.schema(), ds.schema(self.s)?)?;
                    let plan =
                        self.plan_where_clause(plan, &condition, metadata_cutoff, &schema)?;

                    let tags = table_schema
                        .iter()
                        .filter(|(t, _f)| matches!(t, InfluxColumnType::Tag))
                        .map(|(_t, f)| f.name().as_str())
                        .collect::<Vec<_>>();

                    // We want to find all tag columns that had non-null values and create a row for each of them. SQL
                    // (and DataFusion) don't have a real pivot/transpose operation, but we can work around this by
                    // using some `make_array`+`unnest` trickery.
                    let tag_key_df_col = Column::from_name(tag_key_col);
                    let tag_key_col_expr = Expr::Column(tag_key_df_col.clone());
                    let plan = LogicalPlanBuilder::from(plan)
                        // aggregate `SUM(tag IS NOT NULL)` for all tags in one go
                        //
                        // we have a single row afterwards because the group expression is empty.
                        .aggregate(
                            [] as [Expr; 0],
                            tags.iter().map(|tag| {
                                let tag_col = Expr::Column(Column::from_name(*tag));

                                sum(cast(tag_col.is_not_null(), DataType::UInt64)).alias(*tag)
                            }),
                        )?
                        // create array of tag names, where every name is:
                        // - null if it had no non-null values
                        // - not null if it had any non-null values
                        //
                        // note that since we only have a single row, this is efficient
                        .project([make_array(
                            tags.iter()
                                .map(|tag| {
                                    let tag_col = Expr::Column(Column::from_name(*tag));

                                    when(tag_col.gt(lit(0)), lit(*tag)).end()
                                })
                                .collect::<Result<Vec<_>, _>>()?,
                        )
                        .alias(tag_key_col)])?
                        // roll our single array row into one row per tag key
                        .unnest_column(tag_key_df_col)?
                        // filter out tags that had no none-null values
                        .filter(tag_key_col_expr.clone().is_not_null())?
                        // build proper output
                        .project(measurement_expr.into_iter().chain([tag_key_col_expr]))?
                        .build()?;

                    union_plan = match union_plan {
                        Some(union_plan) => Some(union_and_coerce(union_plan, plan)?),
                        None => Some(plan),
                    };
                }

                let plan = match union_plan {
                    Some(plan) => plan,
                    None => LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: output_schema.to_dfschema_ref()?,
                    }),
                };

                LogicalPlanBuilder::from(plan).sort([
                    Expr::Column(Column::new_unqualified(INFLUXQL_MEASUREMENT_COLUMN_NAME))
                        .sort(true, false),
                    Expr::Column(Column::new_unqualified(tag_key_col)).sort(true, false),
                ])?
            }
            None => {
                debug!("`SHOW TAG KEYS` w/o WHERE-clause, use cheap metadata scan",);

                let mut measurement_names_builder = StringDictionaryBuilder::<Int32Type>::new();
                let mut tag_key_builder = StringDictionaryBuilder::<Int32Type>::new();
                for table in tables {
                    let Some(table_schema) = self.s.table_schema(&table) else {
                        continue;
                    };
                    for (t, f) in table_schema.iter() {
                        match t {
                            InfluxColumnType::Tag => {}
                            InfluxColumnType::Field(_) | InfluxColumnType::Timestamp => {
                                continue;
                            }
                        }
                        measurement_names_builder.append_value(&table);
                        tag_key_builder.append_value(f.name());
                    }
                }
                LogicalPlanBuilder::scan(
                    "tag_keys",
                    provider_as_source(Arc::new(MemTable::try_new(
                        Arc::clone(&output_schema),
                        vec![vec![RecordBatch::try_new(
                            Arc::clone(&output_schema),
                            vec![
                                Arc::new(measurement_names_builder.finish()),
                                Arc::new(tag_key_builder.finish()),
                            ],
                        )?]],
                    )?)),
                    None,
                )?
            }
        };

        if show_tag_keys.offset.is_some() || show_tag_keys.limit.is_some() {
            builder = builder.series_limit(
                [Expr::Column(Column::new_unqualified(
                    INFLUXQL_MEASUREMENT_COLUMN_NAME,
                ))],
                [Expr::Column(Column::new_unqualified(tag_key_col)).sort(true, false)],
                [LimitExpr {
                    expr: Expr::Column(Column::new_unqualified(tag_key_col)),
                    null_treatment: NullTreatment::RespectNulls,
                    default_value: lit(ScalarValue::Dictionary(
                        Box::new(DataType::Int32),
                        Box::new(ScalarValue::Utf8(None)),
                    )),
                }],
                show_tag_keys
                    .offset
                    .map(|offset| offset.as_usize())
                    .unwrap_or_default(),
                show_tag_keys.limit.map(|limit| limit.as_usize()),
            )?;
        }

        let plan = plan_with_metadata(
            builder.build()?,
            &InfluxQlMetadata {
                measurement_column_index: MEASUREMENT_COLUMN_INDEX,
                tag_key_columns: vec![],
            },
        )?;

        Ok(plan)
    }

    fn show_field_keys_to_plan(
        &self,
        show_field_keys: ShowFieldKeysStatement,
    ) -> Result<LogicalPlan> {
        if show_field_keys.database.is_some() {
            // How do we handle this? Do we need to perform cross-namespace queries here?
            return error::not_implemented("SHOW FIELD KEYS ON <database>");
        }

        let field_key_col = "fieldKey";
        let output_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(INFLUXQL_MEASUREMENT_COLUMN_NAME, DataType::Utf8, false),
            ArrowField::new(field_key_col, DataType::Utf8, false),
            ArrowField::new("fieldType", DataType::Utf8, false),
        ]));

        let tables = self.expand_show_from_clause(show_field_keys.from)?;

        let mut measurement_names_builder = StringBuilder::new();
        let mut field_key_builder = StringBuilder::new();
        let mut field_type_builder = StringBuilder::new();
        for table in tables {
            let Some(table_schema) = self.s.table_schema(&table) else {
                continue;
            };
            for (t, f) in table_schema.iter() {
                let t = match t {
                    InfluxColumnType::Field(t) => t,
                    InfluxColumnType::Tag | InfluxColumnType::Timestamp => {
                        continue;
                    }
                };
                let t = match t {
                    InfluxFieldType::Float => "float",
                    InfluxFieldType::Integer => "integer",
                    InfluxFieldType::UInteger => "unsigned",
                    InfluxFieldType::String => "string",
                    InfluxFieldType::Boolean => "boolean",
                };
                measurement_names_builder.append_value(&table);
                field_key_builder.append_value(f.name());
                field_type_builder.append_value(t);
            }
        }
        let mut builder = LogicalPlanBuilder::scan(
            "field_keys",
            provider_as_source(Arc::new(MemTable::try_new(
                Arc::clone(&output_schema),
                vec![vec![RecordBatch::try_new(
                    Arc::clone(&output_schema),
                    vec![
                        Arc::new(measurement_names_builder.finish()),
                        Arc::new(field_key_builder.finish()),
                        Arc::new(field_type_builder.finish()),
                    ],
                )?]],
            )?)),
            None,
        )?;

        if show_field_keys.offset.is_some() || show_field_keys.limit.is_some() {
            builder = builder.series_limit(
                [Expr::Column(Column::new_unqualified(
                    INFLUXQL_MEASUREMENT_COLUMN_NAME,
                ))],
                [Expr::Column(Column::new_unqualified(field_key_col)).sort(true, false)],
                [LimitExpr {
                    expr: Expr::Column(Column::new_unqualified(field_key_col)),
                    null_treatment: NullTreatment::RespectNulls,
                    default_value: lit(ScalarValue::Utf8(None)),
                }],
                show_field_keys
                    .offset
                    .map(|offset| offset.as_usize())
                    .unwrap_or_default(),
                show_field_keys.limit.map(|limit| limit.as_usize()),
            )?;
        }

        let plan = plan_with_metadata(
            builder.build()?,
            &InfluxQlMetadata {
                measurement_column_index: MEASUREMENT_COLUMN_INDEX,
                tag_key_columns: vec![],
            },
        )?;

        Ok(plan)
    }

    fn show_tag_values_to_plan(
        &self,
        show_tag_values: ShowTagValuesStatement,
    ) -> Result<LogicalPlan> {
        if show_tag_values.database.is_some() {
            // How do we handle this? Do we need to perform cross-namespace queries here?
            return error::not_implemented("SHOW TAG VALUES ON <database>");
        }

        let key_col = "key";
        let value_col = "value";
        let output_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(INFLUXQL_MEASUREMENT_COLUMN_NAME, DataType::Utf8, false),
            ArrowField::new(key_col, DataType::Utf8, false),
            ArrowField::new(value_col, DataType::Utf8, false),
        ]));

        let tables = self.expand_show_from_clause(show_tag_values.from)?;
        let metadata_cutoff = self.metadata_cutoff();

        let mut union_plan = None;
        for table in tables {
            let Some(schema) = self.s.table_schema(&table) else {
                continue;
            };

            let keys = eval_with_key_clause(
                schema.tags_iter().map(|field| field.name().as_str()),
                &show_tag_values.with_key,
            )?;
            if keys.is_empty() {
                // don't bother to create a plan for this table
                continue;
            }

            let Some((plan, measurement_expr)) = self.create_table_ref(&table)? else {
                continue;
            };

            let ds = DataSource::Table(table.clone());
            let schema = IQLSchema::new_from_ds_schema(plan.schema(), ds.schema(self.s)?)?;
            let plan =
                self.plan_where_clause(plan, &show_tag_values.condition, metadata_cutoff, &schema)?;

            for key in keys {
                let idx = plan
                    .schema()
                    .index_of_column_by_name(None, key)
                    .expect("where is the key?");

                let plan = LogicalPlanBuilder::from(plan.clone())
                    .select([idx])?
                    .distinct()?
                    .project(measurement_expr.iter().cloned().chain([
                        lit_dict(key).alias(key_col),
                        Expr::Column(Column::from_name(key)).alias(value_col),
                    ]))?
                    .build()?;

                union_plan = match union_plan {
                    Some(union_plan) => match union_plan {
                        LogicalPlan::Union(Union { inputs, schema }) => {
                            // Flattens nested Unions to prevent a recursion stack overflow
                            // later during DataFusion optimization.
                            let mut inputs = inputs
                                .iter()
                                .flat_map(|plan| match plan.as_ref() {
                                    LogicalPlan::Union(Union { inputs, .. }) => inputs.to_vec(),
                                    _ => {
                                        vec![Arc::clone(plan)]
                                    }
                                })
                                .collect::<Vec<_>>();
                            inputs.push(Arc::new(plan));

                            Some(LogicalPlan::Union(Union { inputs, schema }))
                        }
                        _ => Some(union_and_coerce(union_plan, plan)?),
                    },
                    None => Some(plan),
                };
            }
        }

        let plan = match union_plan {
            Some(plan) => plan,
            None => LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: output_schema.to_dfschema_ref()?,
            }),
        };
        let mut builder = LogicalPlanBuilder::from(plan).sort([
            Expr::Column(Column::new_unqualified(INFLUXQL_MEASUREMENT_COLUMN_NAME))
                .sort(true, false),
            Expr::Column(Column::new_unqualified(key_col)).sort(true, false),
            Expr::Column(Column::new_unqualified(value_col)).sort(true, false),
        ])?;

        if show_tag_values.offset.is_some() || show_tag_values.limit.is_some() {
            builder = builder.series_limit(
                [
                    Expr::Column(Column::new_unqualified(INFLUXQL_MEASUREMENT_COLUMN_NAME)),
                    Expr::Column(Column::new_unqualified(key_col)),
                ],
                [Expr::Column(Column::new_unqualified(value_col)).sort(true, false)],
                [LimitExpr {
                    expr: Expr::Column(Column::new_unqualified(value_col)),
                    null_treatment: NullTreatment::RespectNulls,
                    default_value: lit(ScalarValue::Dictionary(
                        Box::new(DataType::Int32),
                        Box::new(ScalarValue::Utf8(None)),
                    )),
                }],
                show_tag_values
                    .offset
                    .map(|offset| offset.as_usize())
                    .unwrap_or_default(),
                show_tag_values.limit.map(|limit| limit.as_usize()),
            )?;
        }

        let plan = plan_with_metadata(
            builder.build()?,
            &InfluxQlMetadata {
                measurement_column_index: MEASUREMENT_COLUMN_INDEX,
                tag_key_columns: vec![],
            },
        )?;

        Ok(plan)
    }

    fn show_measurements_to_plan(
        &self,
        show_measurements: ShowMeasurementsStatement,
    ) -> Result<LogicalPlan> {
        if show_measurements.on.is_some() {
            // How do we handle this? Do we need to perform cross-namespace queries here?
            return error::not_implemented("SHOW MEASUREMENTS ON <database>");
        }

        let tables = self.expand_with_measurement_clause(show_measurements.with_measurement)?;

        let name_col = "name";
        let output_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                INFLUXQL_MEASUREMENT_COLUMN_NAME,
                (&InfluxColumnType::Tag).into(),
                false,
            ),
            ArrowField::new(name_col, (&InfluxColumnType::Tag).into(), false),
        ]));
        let dummy_measurement_name = "measurements";

        let mut builder = match show_measurements.condition {
            Some(condition) => {
                debug!("`SHOW MEASUREMENTS` w/ WHERE-clause, use data scan plan",);

                let condition = Some(condition);
                let metadata_cutoff = self.metadata_cutoff();

                let mut union_plan = None;
                for table in tables {
                    let Some((plan, _measurement_expr)) = self.create_table_ref(&table)? else {
                        continue;
                    };

                    let ds = DataSource::Table(table.clone());
                    let schema = IQLSchema::new_from_ds_schema(plan.schema(), ds.schema(self.s)?)?;
                    let plan =
                        self.plan_where_clause(plan, &condition, metadata_cutoff, &schema)?;

                    let plan = LogicalPlanBuilder::from(plan)
                        .limit(0, Some(1))?
                        .project([
                            lit_dict(dummy_measurement_name)
                                .alias(INFLUXQL_MEASUREMENT_COLUMN_NAME),
                            lit_dict(&table).alias(name_col),
                        ])?
                        .build()?;

                    union_plan = match union_plan {
                        Some(union_plan) => Some(union_and_coerce(union_plan, plan)?),
                        None => Some(plan),
                    };
                }

                let plan = match union_plan {
                    Some(plan) => plan,
                    None => LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: output_schema.to_dfschema_ref()?,
                    }),
                };
                LogicalPlanBuilder::from(plan).sort([
                    Expr::Column(Column::new_unqualified(INFLUXQL_MEASUREMENT_COLUMN_NAME))
                        .sort(true, false),
                    Expr::Column(Column::new_unqualified(name_col)).sort(true, false),
                ])?
            }
            None => {
                debug!("`SHOW MEASUREMENTS` w/o WHERE-clause, use cheap metadata scan",);

                let mut dummy_measurement_names_builder =
                    StringDictionaryBuilder::<Int32Type>::new();
                let mut name_builder = StringDictionaryBuilder::<Int32Type>::new();
                for table in tables {
                    dummy_measurement_names_builder.append_value(dummy_measurement_name);
                    name_builder.append_value(table);
                }
                LogicalPlanBuilder::scan(
                    "measurements",
                    provider_as_source(Arc::new(MemTable::try_new(
                        Arc::clone(&output_schema),
                        vec![vec![RecordBatch::try_new(
                            Arc::clone(&output_schema),
                            vec![
                                Arc::new(dummy_measurement_names_builder.finish()),
                                Arc::new(name_builder.finish()),
                            ],
                        )?]],
                    )?)),
                    None,
                )?
            }
        };

        if show_measurements.offset.is_some() || show_measurements.limit.is_some() {
            builder = builder.limit(
                show_measurements
                    .offset
                    .map(|offset| offset.as_usize())
                    .unwrap_or_default(),
                show_measurements.limit.map(|limit| limit.as_usize()),
            )?;
        }

        let plan = plan_with_metadata(
            builder.build()?,
            &InfluxQlMetadata {
                measurement_column_index: MEASUREMENT_COLUMN_INDEX,
                tag_key_columns: vec![],
            },
        )?;

        Ok(plan)
    }

    /// A limited implementation of SHOW RETENTION POLICIES that assumes
    /// any database has a single, default, retention policy.
    fn show_retention_policies_to_plan(
        &self,
        show_retention_policies: ShowRetentionPoliciesStatement,
    ) -> Result<LogicalPlan> {
        if show_retention_policies.database.is_some() {
            // This syntax is not yet handled.
            return error::not_implemented("SHOW RETENTION POLICIES ON <database>");
        }

        let output_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                INFLUXQL_MEASUREMENT_COLUMN_NAME,
                (&InfluxColumnType::Tag).into(),
                false,
            ),
            ArrowField::new(
                "name",
                (&InfluxColumnType::Field(InfluxFieldType::String)).into(),
                false,
            ),
            ArrowField::new(
                "duration",
                (&InfluxColumnType::Field(InfluxFieldType::String)).into(),
                false,
            ),
            ArrowField::new(
                "shardGroupDuration",
                (&InfluxColumnType::Field(InfluxFieldType::String)).into(),
                false,
            ),
            ArrowField::new(
                "replicaN",
                (&InfluxColumnType::Field(InfluxFieldType::Integer)).into(),
                false,
            ),
            ArrowField::new(
                "default",
                (&InfluxColumnType::Field(InfluxFieldType::Boolean)).into(),
                false,
            ),
        ]));
        let record_batch = RecordBatch::try_new(
            Arc::clone(&output_schema),
            vec![
                Arc::new(DictionaryArray::try_new(
                    Int32Array::from(vec![0]),
                    Arc::new(StringArray::from(vec![Some("retention_policies")])),
                )?),
                Arc::new(StringArray::from(vec![Some("autogen")])),
                Arc::new(StringArray::from(vec![Some("0s")])),
                Arc::new(StringArray::from(vec![Some("168h0m0s")])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(BooleanArray::from(vec![true])),
            ],
        )?;
        let table = Arc::new(MemTable::try_new(output_schema, vec![vec![record_batch]])?);
        let plan = LogicalPlanBuilder::scan("retention policies", provider_as_source(table), None)?
            .build()?;
        let plan = plan_with_metadata(
            plan,
            &InfluxQlMetadata {
                measurement_column_index: MEASUREMENT_COLUMN_INDEX,
                tag_key_columns: vec![],
            },
        )?;
        Ok(plan)
    }

    fn metadata_cutoff(&self) -> MetadataCutoff {
        self.iox_ctx
            .inner()
            .state()
            .config()
            .options()
            .extensions
            .get::<IoxConfigExt>()
            .cloned()
            .unwrap_or_default()
            .influxql_metadata_cutoff
    }
}

/// Returns a [`LogicalPlan`] that performs gap-filling for the `input` plan.
///
/// # Arguments
///
/// * `input` - A plan which requires gap-filling, it is required that
///   the input plan includes an Aggregate node.
/// * `fill_strategy` - The strategy used to fill gaps in the data.
///   Should be equal in length to `input.aggr_exprs`, where
///   fill_strategy\[n\] is the strategy for aggr_exprs\[n\].
/// * `time_range` - The range to fill gaps between.
/// * `tz` - The optional time zone for the query.
fn build_gap_fill_node(
    input: LogicalPlan,
    fill_strategy: Vec<FillStrategy>,
    time_range: TimeRange,
    tz: &Option<Arc<str>>,
) -> Result<LogicalPlan> {
    let mut aggr = None;
    input.apply(|expr| {
        if let LogicalPlan::Aggregate(a) = expr {
            aggr = Some(a.clone());
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    let Some(aggr) = aggr else {
        return error::internal("GapFill requires an Aggregate ancestor");
    };

    let group_expr = aggr.group_expr;

    // Extract the DATE_BIN expression from the aggregate's group
    // expressions.
    let (time_column_idx, time_column_alias, date_bin_udf, date_bin_args) = match group_expr
        .iter()
        .enumerate()
        .filter_map(|(idx, expr)| match expr {
            Expr::Alias(alias) => {
                if let Expr::ScalarFunction(fun) = alias.expr.as_ref() {
                    if fun.func.inner().as_any().is::<DateBinFunc>()
                        || fun.func.inner().as_any().is::<DateBinWallclockUDF>()
                    {
                        Some((
                            idx,
                            alias.name.clone(),
                            Arc::clone(&fun.func),
                            fun.args.clone(),
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect::<Vec<_>>()
        .as_slice()
    {
        [(idx, alias, udf, args)] => (*idx, alias.to_owned(), Arc::clone(udf), args.to_owned()),
        _ => {
            return error::internal("expected exactly one DATE_BIN in Aggregate group expressions");
        }
    };

    let ([stride, _] | [stride, _, _]) = date_bin_args.as_slice() else {
        // This is an internal error as the date_bin function is added by the planner and should
        // always contain the correct number of arguments.
        return error::internal(format!(
            "DATE_BIN expects 2 or 3 arguments, got {}",
            date_bin_args.len()
        ));
    };
    let origin = date_bin_args.get(2).cloned();

    // Ensure that a time range was specified and is valid for gap filling
    let time_range = {
        let TimeRange { lower, upper } = time_range;
        Range {
            start: lower
                .map(|n| {
                    Bound::Included(Expr::Literal(
                        ScalarValue::TimestampNanosecond(Some(n), tz.clone()),
                        None,
                    ))
                })
                .unwrap_or(Bound::Unbounded),
            end: upper
                .map(|n| {
                    Bound::Included(Expr::Literal(
                        ScalarValue::TimestampNanosecond(Some(n), tz.clone()),
                        None,
                    ))
                })
                // Follow the InfluxQL behaviour to use an upper bound of `now` when
                // not found:
                //
                // See: https://github.com/influxdata/influxdb/blob/98361e207349a3643bcc332d54b009818fe7585f/query/compile.go#L172-L176
                .unwrap_or(Bound::Excluded(now())),
        }
    };

    let LogicalPlan::Aggregate(aggr) = &input else {
        return Err(DataFusionError::Internal(format!(
            "Expected Aggregate plan, got {}",
            input.display()
        )));
    };
    let mut new_group_expr: Vec<_> = aggr
        .schema
        .iter()
        .map(|(qualifier, field)| Expr::Column(Column::from((qualifier, field.as_ref()))))
        .collect();
    let aggr_expr = new_group_expr.split_off(aggr.group_expr.len());

    // The fill strategy for InfluxQL is specified at the query level
    let fill_expr = aggr_expr
        .iter()
        .cloned()
        .zip(fill_strategy)
        .map(|(e, s)| FillExpr {
            expr: e,
            strategy: s,
        })
        .collect();

    let series_expr = group_expr
        .iter()
        .enumerate()
        .filter_map(|(i, e)| {
            if i != time_column_idx {
                Some(e.clone())
            } else {
                None
            }
        })
        .collect();

    let time_expr = Expr::ScalarFunction(ScalarFunction {
        func: date_bin_udf,
        args: vec![stride.clone(), col(time_column_alias)]
            .into_iter()
            .chain(origin.clone())
            .collect(),
    });

    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(GapFill::try_new(
            Arc::new(input),
            series_expr,
            time_expr,
            fill_expr,
            time_range,
        )?),
    }))
}

/// Adds [`InfluxQlMetadata`] to the `plan`.
fn plan_with_metadata(plan: LogicalPlan, metadata: &InfluxQlMetadata) -> Result<LogicalPlan> {
    let data = serde_json::to_string(metadata).map_err(|err| {
        error::map::internal(format!("error serializing InfluxQL metadata: {err}"))
    })?;

    plan.transform_down(|plan| {
        transform_plan_schema(plan, |schema| {
            let mut md = schema.as_arrow().metadata().clone();
            md.insert(INFLUXQL_METADATA_KEY.to_owned(), data.clone());

            let qualified_fields = schema
                .iter()
                .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
                .collect();
            Ok(Transformed::new(
                Arc::new(DFSchema::new_with_metadata(qualified_fields, md)?),
                true,
                TreeNodeRecursion::Jump,
            ))
        })
    })
    .map(|t| t.data)
}

/// A utility function that checks whether `f` is an
/// aggregate field or not. An aggregate field is one that contains at least one
/// call to an aggregate function.
fn is_aggregate_field(f: &Field) -> bool {
    walk_expr(&f.expr, &mut |e| match e {
        IQLExpr::Call(Call { name, .. }) if is_aggregate_function(name) => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

fn conditional_op_to_operator(op: ConditionalOperator) -> Result<Operator> {
    match op {
        ConditionalOperator::Eq => Ok(Operator::Eq),
        ConditionalOperator::NotEq => Ok(Operator::NotEq),
        ConditionalOperator::EqRegex => Ok(Operator::RegexMatch),
        ConditionalOperator::NotEqRegex => Ok(Operator::RegexNotMatch),
        ConditionalOperator::Lt => Ok(Operator::Lt),
        ConditionalOperator::LtEq => Ok(Operator::LtEq),
        ConditionalOperator::Gt => Ok(Operator::Gt),
        ConditionalOperator::GtEq => Ok(Operator::GtEq),
        ConditionalOperator::And => Ok(Operator::And),
        ConditionalOperator::Or => Ok(Operator::Or),
        // NOTE: This is not supported by InfluxQL SELECT expressions, so it is unexpected
        ConditionalOperator::In => error::internal("unexpected binary operator: IN"),
    }
}

/// Find the index of the time column in the fields list.
///
/// > **Note**
/// >
/// > To match InfluxQL, the `time` column must not exist as part of a
/// > complex expression.
fn find_time_column_index(fields: &[Field]) -> Option<usize> {
    fields
        .iter()
        .find_position(|f| matches!(f.data_type, Some(InfluxColumnType::Timestamp)))
        .map(|(i, _)| i)
}

/// Evaluate [`WithKeyClause`] on the given list of keys.
///
/// This may fail if the clause contains an invalid regex.
fn eval_with_key_clause<'a>(
    keys: impl IntoIterator<Item = &'a str>,
    clause: &WithKeyClause,
) -> Result<Vec<&'a str>> {
    match clause {
        WithKeyClause::Eq(ident) => {
            let ident = ident.as_str();
            Ok(keys.into_iter().filter(|key| ident == *key).collect())
        }
        WithKeyClause::NotEq(ident) => {
            let ident = ident.as_str();
            Ok(keys.into_iter().filter(|key| ident != *key).collect())
        }
        WithKeyClause::EqRegex(regex) => {
            let regex = parse_regex(regex)?;
            Ok(keys.into_iter().filter(|key| regex.is_match(key)).collect())
        }
        WithKeyClause::NotEqRegex(regex) => {
            let regex = parse_regex(regex)?;
            Ok(keys
                .into_iter()
                .filter(|key| !regex.is_match(key))
                .collect())
        }
        WithKeyClause::In(idents) => {
            let idents = idents
                .iter()
                .map(|ident| ident.as_str())
                .collect::<HashSet<_>>();
            Ok(keys
                .into_iter()
                .filter(|key| idents.contains(key))
                .collect())
        }
    }
}

/// Find distinct occurrences of `Expr::VarRef` expressions for
/// the `select`.
fn find_var_refs(select: &Select) -> BTreeSet<&VarRef> {
    let mut var_refs = BTreeSet::new();

    for f in &select.fields {
        let _ = walk_expr(&f.expr, &mut |e| {
            if let IQLExpr::VarRef(vr) = e {
                var_refs.insert(vr);
            }
            ControlFlow::<()>::Continue(())
        });
    }

    if let Some(condition) = &select.condition {
        let _ = walk_expression(condition, &mut |e| match e {
            Expression::Arithmetic(e) => walk_expr(e, &mut |e| {
                if let IQLExpr::VarRef(vr) = e {
                    var_refs.insert(vr);
                }
                ControlFlow::<()>::Continue(())
            }),
            _ => ControlFlow::<()>::Continue(()),
        });
    }

    if let Some(group_by) = &select.group_by {
        for vr in group_by.tags() {
            var_refs.insert(vr);
        }
    }

    var_refs
}

/// Calculate the partitioning for window functions.
fn window_partition_by(
    ctx: &Context<'_>,
    schema: &DFSchemaRef,
    group_by_tags: &[&str],
) -> Vec<Expr> {
    let mut parition_by = fields_to_exprs_no_nulls(schema, group_by_tags).collect::<Vec<_>>();
    if let Some(i) = ctx.interval {
        let stride = lit(ScalarValue::new_interval_mdn(0, 0, i.duration));
        let offset = i.offset.unwrap_or_default();

        parition_by.push(date_bin_wallclock(
            stride,
            "time".as_expr(),
            lit_timestamp_nano(offset),
        ));
    }
    parition_by
}

fn filter_window_nulls(plan: LogicalPlan) -> Result<LogicalPlan> {
    if let LogicalPlan::Window(window) = &plan {
        let filter = disjunction(window.window_expr.iter().map(|expr| {
            Expr::Column(Column::from_name(expr.schema_name().to_string())).is_not_null()
        }));
        if let Some(filter) = filter {
            LogicalPlanBuilder::from(plan).filter(filter)?.build()
        } else {
            Ok(plan)
        }
    } else {
        Ok(plan)
    }
}

/// Apply a time range filter to the `plan` based on the original
/// specified time filters (expanded to account for grouping). This will
/// remove any rows from the extended time range that were used to
/// compute window-like function values.
fn filter_requested_time_range(
    ctx: &Context<'_>,
    plan: LogicalPlan,
    fields: &[Field],
    select_exprs: &[Expr],
) -> Result<LogicalPlan> {
    let Some(time_column_index) = find_time_column_index(fields) else {
        return error::internal("unable to find time column");
    };
    let plan = if let Some(filter) = time_range_to_df_expr(
        &select_exprs[time_column_index],
        &ctx.tz,
        ctx.result_time_range(),
    ) {
        LogicalPlanBuilder::from(plan).filter(filter)?.build()?
    } else {
        plan
    };
    Ok(plan)
}

fn lit_duration_nano(t: i64) -> Expr {
    lit(ScalarValue::DurationNanosecond(Some(t)))
}

fn remove_aggr_count_from_error(error: DataFusionError) -> DataFusionError {
    // Strip "__aggr_count" from the SchemaError
    // since it is just an internal field
    match error {
        DataFusionError::SchemaError(schema_error, e) => match *schema_error {
            SchemaError::FieldNotFound {
                field,
                valid_fields,
            } => {
                let new_fields = valid_fields
                    .into_iter()
                    .filter(|x| x.name != "__aggr_count")
                    .collect();

                DataFusionError::SchemaError(
                    Box::new(SchemaError::FieldNotFound {
                        field,
                        valid_fields: new_fields,
                    }),
                    e,
                )
            }
            schema_error => DataFusionError::SchemaError(Box::new(schema_error), e),
        },
        _ => error,
    }
}

fn is_integral_aggr_expr(expr: &Expr) -> bool {
    if let Expr::AggregateFunction(expr::AggregateFunction { func, params }) = expr
        && func.name() == "sum"
        && let [Expr::Column(Column { name, .. })] = params.args.as_slice()
    {
        return name.starts_with(&format!("{INTEGRAL_UDF_NAME}(")) && name.ends_with(")");
    };
    false
}

/// Wrapper around DataFusion's `project` that preserves InfluxQL fill
/// information in the schema metadata.
fn project(
    input: LogicalPlan,
    expr: impl IntoIterator<Item = impl Into<Expr>>,
) -> Result<LogicalPlan> {
    let (expr, filled) = expr
        .into_iter()
        .map(|e| {
            let expr = e.into();
            let filled = expr_is_influxql_filled(&expr, input.schema());
            (expr, filled)
        })
        .unzip::<_, _, Vec<_>, Vec<_>>();
    let plan = datafusion::logical_expr::logical_plan::builder::project(input, expr)?;
    plan.transform_down(|plan| {
        transform_plan_schema(plan, |s| {
            schema_with_influxql_filled(s, &filled)
                .map(|s| Transformed::new(s, true, TreeNodeRecursion::Stop))
        })
    })
    .map(|t| t.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::test_utils::{MockSchemaProvider, parse_select};
    use influxdb_influxql_parser::parse_statements;
    use insta::assert_snapshot;
    use schema::SchemaBuilder;

    fn logical_plan(sql: &str) -> Result<LogicalPlan> {
        let mut statements = parse_statements(sql).unwrap();
        let mut sp = MockSchemaProvider::default();
        sp.add_schemas(vec![
            SchemaBuilder::new()
                .measurement("data")
                .timestamp()
                .tag("foo")
                .tag("bar")
                .influx_field("f64_field", InfluxFieldType::Float)
                .influx_field("mixedCase", InfluxFieldType::Float)
                .influx_field("with space", InfluxFieldType::Float)
                .influx_field("i64_field", InfluxFieldType::Integer)
                .influx_field("str_field", InfluxFieldType::String)
                .influx_field("bool_field", InfluxFieldType::Boolean)
                // InfluxQL is case sensitive
                .influx_field("TIME", InfluxFieldType::Boolean)
                .build()
                .unwrap(),
            // Table with tags and all field types
            SchemaBuilder::new()
                .measurement("all_types")
                .timestamp()
                .tag("tag0")
                .tag("tag1")
                .influx_field("f64_field", InfluxFieldType::Float)
                .influx_field("i64_field", InfluxFieldType::Integer)
                .influx_field("str_field", InfluxFieldType::String)
                .influx_field("bool_field", InfluxFieldType::Boolean)
                .influx_field("u64_field", InfluxFieldType::UInteger)
                .build()
                .unwrap(),
            // table w/ name clashes
            SchemaBuilder::new()
                .measurement("name_clash")
                .timestamp()
                .tag("first")
                .influx_field("f", InfluxFieldType::Float)
                .build()
                .unwrap(),
        ]);

        // Create multiple measurements with the same tag
        // for show tag values test
        for i in 0..5 {
            sp.add_schema(
                SchemaBuilder::new()
                    .measurement(format!("m{i}"))
                    .timestamp()
                    .tag("many_measurements")
                    .influx_field("f64_field", InfluxFieldType::Float)
                    .build()
                    .unwrap(),
            );
        }

        let iox_ctx = IOxSessionContext::with_testing();
        let planner = InfluxQLToLogicalPlan::new(&sp, &iox_ctx);

        planner.statement_to_plan(statements.pop().unwrap())
    }

    fn metadata(sql: &str) -> Option<InfluxQlMetadata> {
        logical_plan(sql)
            .unwrap()
            .schema()
            .as_arrow()
            .metadata()
            .get(INFLUXQL_METADATA_KEY)
            .map(|s| serde_json::from_str(s).unwrap())
    }

    fn plan(sql: impl Into<String>) -> String {
        let result = logical_plan(&sql.into());
        match result {
            Ok(res) => res.display_indent_schema().to_string(),
            Err(err) => err.to_string(),
        }
    }

    #[test]
    fn test_find_var_refs() {
        use influxdb_influxql_parser::expression::VarRefDataType::*;

        macro_rules! var_ref {
            ($NAME: literal) => {
                VarRef {
                    name: $NAME.into(),
                    data_type: None,
                }
            };

            ($NAME: literal, $TYPE: ident) => {
                VarRef {
                    name: $NAME.into(),
                    data_type: Some($TYPE),
                }
            };
        }

        fn find_var_refs(s: &dyn SchemaProvider, q: &str) -> Vec<VarRef> {
            let sel = parse_select(q);
            let select = rewrite_statement(s, &sel).unwrap();
            super::find_var_refs(&select.select)
                .into_iter()
                .cloned()
                .collect()
        }

        let sp = MockSchemaProvider::default();

        let got = find_var_refs(
            &sp,
            "SELECT cpu, usage_idle FROM cpu WHERE usage_user = 3 AND usage_idle > 1 GROUP BY cpu",
        );
        assert_eq!(
            &got,
            &[
                var_ref!("cpu", Tag),
                var_ref!("time", Timestamp),
                var_ref!("usage_idle", Float),
                var_ref!("usage_user", Float),
            ]
        );

        let got = find_var_refs(&sp, "SELECT non_existent, usage_idle FROM cpu");
        assert_eq!(
            &got,
            &[
                var_ref!("non_existent"),
                var_ref!("time", Timestamp),
                var_ref!("usage_idle", Float),
            ]
        );

        let got = find_var_refs(
            &sp,
            "SELECT non_existent, usage_idle FROM (SELECT cpu as non_existent, usage_idle FROM cpu) GROUP BY cpu",
        );
        assert_eq!(
            &got,
            &[
                var_ref!("cpu", Tag),
                var_ref!("non_existent", Tag),
                var_ref!("time", Timestamp),
                var_ref!("usage_idle", Float),
            ]
        );
    }

    /// Verify the list of unsupported statements.
    ///
    /// It is expected certain statements will be unsupported, indefinitely.
    #[test]
    fn test_unsupported_statements() {
        assert_snapshot!(plan("CREATE DATABASE foo"));
        assert_snapshot!(plan("DELETE FROM foo"));
        assert_snapshot!(plan("DROP MEASUREMENT foo"));
        assert_snapshot!(plan("SHOW DATABASES"));
    }

    mod metadata_queries {
        use super::*;

        #[test]
        fn test_show_field_keys() {
            assert_snapshot!(plan("SHOW FIELD KEYS"));
            assert_snapshot!(plan("SHOW FIELD KEYS LIMIT 1 OFFSET 2"));
        }

        #[test]
        fn test_show_measurements() {
            assert_snapshot!(plan("SHOW MEASUREMENTS"));
            assert_snapshot!(plan("SHOW MEASUREMENTS LIMIT 1 OFFSET 2"));
            assert_snapshot!(plan("SHOW MEASUREMENTS WHERE foo = 'some_foo'"));
            assert_snapshot!(plan("SHOW MEASUREMENTS WHERE time > 1337"));
        }

        #[test]
        fn test_show_tag_keys_1() {
            assert_snapshot!(plan("SHOW TAG KEYS"));
            assert_snapshot!(plan("SHOW TAG KEYS LIMIT 1 OFFSET 2"));
        }

        #[test]
        fn test_show_tag_keys_2() {
            assert_snapshot!(plan("SHOW TAG KEYS WHERE foo = 'some_foo'"));
        }

        #[test]
        fn test_show_tag_keys_3() {
            assert_snapshot!(plan("SHOW TAG KEYS WHERE time > 1337"));
        }

        #[test]
        fn test_show_tag_values_1() {
            assert_snapshot!(plan("SHOW TAG VALUES WITH KEY = bar"));
        }

        #[test]
        fn test_show_tag_values_2() {
            assert_snapshot!(plan("SHOW TAG VALUES WITH KEY = bar LIMIT 1 OFFSET 2"));
        }

        #[test]
        fn test_show_tag_values_3() {
            assert_snapshot!(plan(
                "SHOW TAG VALUES WITH KEY = bar WHERE foo = 'some_foo'"
            ));
        }

        #[test]
        fn test_show_tag_values_4() {
            assert_snapshot!(plan("SHOW TAG VALUES WITH KEY = bar WHERE time > 1337"));
        }

        #[test]
        fn test_show_tag_values_many_measurements() {
            assert_snapshot!(plan("SHOW TAG VALUES WITH KEY = many_measurements"));
        }

        #[test]
        fn test_show_retention_policies() {
            assert_snapshot!(plan("SHOW RETENTION POLICIES"));
            assert_snapshot!(plan("SHOW RETENTION POLICIES ON my_db"));
        }
    }

    /// Tests to validate InfluxQL `SELECT` statements, where the projections do not matter,
    /// such as the WHERE clause.
    mod select {
        use super::*;

        mod subqueries {
            use super::*;

            /// Projecting subqueries that do not use aggregate or selector functions.
            #[test]
            fn raw() {
                // project an aliased column
                assert_snapshot!(plan(
                    "SELECT value FROM (SELECT usage_idle AS value FROM cpu)"
                ));

                // project a wildcard
                assert_snapshot!(plan(
                    "SELECT * FROM (SELECT usage_idle, usage_system AS value FROM cpu)"
                ));
            }

            /// Projecting subqueries that do not use aggregate or selector functions.
            #[test]
            fn aggregates() {
                // project an aggregate
                assert_snapshot!(plan(
                    "SELECT value FROM (SELECT mean(usage_idle) AS value FROM cpu)"
                ));

                assert_snapshot!(plan(
                    "SELECT value FROM (SELECT mean(usage_idle) AS value FROM cpu GROUP BY TIME(10s))"
                ));
            }

            /// Projecting subqueries that use the `DISTINCT` function / operator.
            #[test]
            fn distinct() {
                // Subquery is a DISTINCT
                assert_snapshot!(plan(
                    "SELECT value FROM (SELECT DISTINCT(usage_idle) AS value FROM cpu)"
                ));

                // Outer query projects subquery with binary expressions
                assert_snapshot!(plan(
                    "SELECT value * 0.99 FROM (SELECT DISTINCT(usage_idle) AS value FROM cpu)"
                ));

                // Outer query groups by the `cpu` tag, which should be pushed all the way to inner-most subquery
                assert_snapshot!(plan(
                    "SELECT * FROM (SELECT MAX(value) FROM (SELECT DISTINCT(usage_idle) AS value FROM cpu)) GROUP BY cpu"
                ));
            }

            /// Projecting subqueries that use different `ORDER BY` sorting than parent
            #[test]
            fn order_by() {
                // Subquery is a time descending; parent is ascending
                assert_snapshot!(plan(
                    "SELECT value FROM (SELECT usage_idle AS value FROM cpu ORDER BY TIME DESC) ORDER BY TIME ASC"
                ));

                // Subquery is a time ascending (default); parent is descending
                assert_snapshot!(plan(
                    "SELECT value FROM (SELECT usage_idle AS value FROM cpu) ORDER BY TIME DESC"
                ));
            }

            #[test]
            fn aggregate_around_window_aggregate() {
                assert_snapshot!(plan(
                    r#"
                        SELECT sum(*)
                        FROM (
                            SELECT non_negative_difference(sum(/usage_.*/))
                            FROM cpu
                            WHERE time >= 0s AND time < 10m
                            GROUP BY time(1m), cpu
                            OFFSET 1
                        )
                        WHERE time >= 0s AND time < 10m
                        GROUP BY time(1m), cpu
                    "#
                ));
            }
        }

        /// Validate plans for multiple data sources in the `FROM` clause, including subqueries
        #[test]
        fn multiple_data_sources() {
            // same table for each subquery
            //
            // ⚠️ Important
            // The aggregate must be applied to the UNION of all instances of the cpu table
            assert_snapshot!(plan(
                "SELECT last(a) / last(b) FROM (SELECT mean(usage_idle) AS a FROM cpu), (SELECT mean(usage_user) AS b FROM cpu)"
            ));

            // selector with repeated table
            //
            // ⚠️ Important
            // The selector must be applied to the UNION of all instances of the cpu table
            assert_snapshot!(plan("SELECT last(usage_idle) FROM cpu, cpu"));

            // different tables for each subquery
            //
            // ⚠️ Important
            // The selector must be applied independently for each unique table
            assert_snapshot!(plan(
                "SELECT last(value) FROM (SELECT usage_idle AS value FROM cpu), (SELECT bytes_free AS value FROM disk)"
            ));
        }

        #[test]
        fn test_time_column() {
            // validate time column is explicitly projected
            assert_snapshot!(plan("SELECT usage_idle, time FROM cpu"));

            // validate time column may be aliased
            assert_snapshot!(plan("SELECT usage_idle, time AS timestamp FROM cpu"));
        }

        mod window_functions {
            use super::*;

            #[test]
            fn test_difference() {
                // no aggregates
                assert_snapshot!(plan("SELECT DIFFERENCE(usage_idle) FROM cpu"));

                // aggregate
                assert_snapshot!(plan(
                    "SELECT DIFFERENCE(MEAN(usage_idle)) FROM cpu GROUP BY TIME(10s)"
                ));
            }

            #[test]
            fn test_non_negative_difference() {
                // no aggregates
                assert_snapshot!(plan("SELECT NON_NEGATIVE_DIFFERENCE(usage_idle) FROM cpu"));

                // aggregate
                assert_snapshot!(plan(
                    "SELECT NON_NEGATIVE_DIFFERENCE(MEAN(usage_idle)) FROM cpu GROUP BY TIME(10s)"
                ));

                // aggregate SUM regex
                assert_snapshot!(plan(
                    "SELECT NON_NEGATIVE_DIFFERENCE(SUM(/usage_.*/)) FROM cpu GROUP BY time(10s)"
                ));
            }

            #[test]
            fn test_moving_average() {
                // no aggregates
                assert_snapshot!(plan("SELECT MOVING_AVERAGE(usage_idle, 3) FROM cpu"));

                // aggregate
                assert_snapshot!(plan(
                    "SELECT MOVING_AVERAGE(MEAN(usage_idle), 3) FROM cpu GROUP BY TIME(10s)"
                ));

                // Invariant: second argument is always a constant
                assert_snapshot!(plan(
                    "SELECT MOVING_AVERAGE(MEAN(usage_idle), usage_system) FROM cpu GROUP BY TIME(10s)"
                ));
            }

            #[test]
            fn test_derivative() {
                // no aggregates
                assert_snapshot!(plan("SELECT DERIVATIVE(usage_idle) FROM cpu"));

                // aggregate
                assert_snapshot!(plan(
                    "SELECT DERIVATIVE(MEAN(usage_idle)) FROM cpu GROUP BY TIME(10s)"
                ));
            }

            #[test]
            fn test_non_negative_derivative() {
                // no aggregates
                assert_snapshot!(plan("SELECT NON_NEGATIVE_DERIVATIVE(usage_idle) FROM cpu"));

                // aggregate
                assert_snapshot!(plan(
                    "SELECT NON_NEGATIVE_DERIVATIVE(MEAN(usage_idle)) FROM cpu GROUP BY TIME(10s)"
                ));

                // selector
                assert_snapshot!(plan(
                    "SELECT NON_NEGATIVE_DERIVATIVE(LAST(usage_idle)) FROM cpu GROUP BY TIME(10s)"
                ));
            }

            #[test]
            fn test_cumulative_sum() {
                // no aggregates
                assert_snapshot!(plan("SELECT CUMULATIVE_SUM(usage_idle) FROM cpu"));

                // aggregate
                assert_snapshot!(plan(
                    "SELECT CUMULATIVE_SUM(MEAN(usage_idle)) FROM cpu GROUP BY TIME(10s)"
                ));
            }

            #[test]
            fn test_mixed_aggregate() {
                assert_snapshot!(plan(
                    "SELECT DIFFERENCE(MEAN(usage_idle)), MEAN(usage_idle) FROM cpu GROUP BY TIME(10s)"
                ));
                assert_snapshot!(plan(
                    "SELECT DIFFERENCE(MEAN(usage_idle)), MEAN(usage_idle) FROM cpu WHERE time >= '2020-01-01T00:00:00Z' AND time < '2021-01-01T00:00:00Z' GROUP BY TIME(10s)"
                ));
            }

            #[test]
            fn test_elapsed() {
                assert_snapshot!(plan("SELECT ELAPSED(usage_idle) FROM cpu"));
            }

            #[test]
            fn test_elapsed_with_unit() {
                assert_snapshot!(plan("SELECT ELAPSED(usage_idle, 1s) FROM cpu"));
            }
        }

        /// Tests for the `DISTINCT` clause and `DISTINCT` function
        #[test]
        fn test_distinct() {
            assert_snapshot!(plan("SELECT DISTINCT usage_idle FROM cpu"));
            assert_snapshot!(plan("SELECT DISTINCT(usage_idle) FROM cpu"));
            assert_snapshot!(plan("SELECT DISTINCT usage_idle FROM cpu GROUP BY cpu"));
            assert_snapshot!(plan("SELECT count(DISTINCT usage_idle) FROM cpu"));
            assert_snapshot!(plan(
                "SELECT DISTINCT(usage_idle) FROM cpu GROUP BY time(1s)"
            ));
            assert_snapshot!(plan(
                "SELECT DISTINCT(usage_idle) FROM cpu GROUP BY time(1s), cpu"
            ));

            // fallible
            assert_snapshot!(plan(
                "SELECT DISTINCT(usage_idle), DISTINCT(usage_system) FROM cpu"
            ));
            assert_snapshot!(plan("SELECT DISTINCT(usage_idle), usage_system FROM cpu"));
        }

        mod functions {
            use super::*;

            #[test]
            fn test_selectors_query() {
                // single-selector query
                assert_snapshot!(plan("SELECT LAST(usage_idle) FROM cpu"));
            }
            #[test]
            fn test_selectors_query_grouping() {
                // single-selector, grouping by tags
                assert_snapshot!(plan("SELECT LAST(usage_idle) FROM cpu GROUP BY cpu"));
            }
            #[test]
            fn test_selectors_aggregate_gby_time() {
                // aggregate query, as we're grouping by time
                assert_snapshot!(plan("SELECT LAST(usage_idle) FROM cpu GROUP BY TIME(5s)"));
            }
            #[test]
            fn test_selectors_aggregate_gby_time_gapfill() {
                // aggregate query, grouping by time with gap filling
                assert_snapshot!(plan(
                    "SELECT FIRST(usage_idle) FROM cpu GROUP BY TIME(5s) FILL(0)"
                ));
            }
            #[test]
            fn test_selectors_aggregate_multi_selectors() {
                // aggregate query, as we're specifying multiple selectors or aggregates
                assert_snapshot!(plan("SELECT LAST(usage_idle), FIRST(usage_idle) FROM cpu"));
            }
            #[test]
            fn test_selectors_and_aggregate() {
                assert_snapshot!(plan("SELECT LAST(usage_idle), count(usage_idle) FROM cpu"));
            }
            #[test]
            fn test_selectors_additional_fields() {
                // additional fields
                assert_snapshot!(plan("SELECT LAST(usage_idle), usage_system FROM cpu"));
            }
            #[test]
            fn test_selectors_additional_fields_2() {
                assert_snapshot!(plan(
                    "SELECT LAST(usage_idle), usage_system FROM cpu GROUP BY TIME(5s)"
                ));
            }
            #[test]
            fn test_selectors_additional_fields_3() {
                assert_snapshot!(plan(
                    "SELECT LAST(usage_idle), usage_system FROM cpu GROUP BY TIME(5s) FILL(0)"
                ));
            }
            #[test]
            fn test_selectors_additional_fields_4() {
                assert_snapshot!(plan("SELECT FIRST(f), first FROM name_clash"));
            }
            #[test]
            fn test_selectors_query_other_other_selectors_1() {
                // Validate we can call the remaining supported selector functions
                assert_snapshot!(plan("SELECT FIRST(usage_idle) FROM cpu"));
            }
            #[test]
            fn test_selectors_query_other_other_selectors_2() {
                assert_snapshot!(plan("SELECT MAX(usage_idle) FROM cpu"));
            }
            #[test]
            fn test_selectors_query_other_other_selectors_3() {
                assert_snapshot!(plan("SELECT MIN(usage_idle) FROM cpu"));
            }

            #[test]
            fn test_selectors_invalid_arguments_3() {
                // Invalid number of arguments
                assert_snapshot!(plan("SELECT MIN(usage_idle, usage_idle) FROM cpu"));
            }

            #[test]
            fn issue_15698() {
                assert_snapshot!(plan(
                    "SELECT time, LAST(usage_idle) AS last_idle, DIFFERENCE(LAST(usage_idle)) AS delta FROM cpu GROUP BY cpu, time(20s) FILL(none)"
                ))
            }
        }

        #[test]
        fn test_percentile() {
            assert_snapshot!(plan(
                "SELECT percentile(usage_idle,50),usage_system FROM cpu"
            ));

            assert_snapshot!(plan(
                "SELECT percentile(usage_idle,50),usage_system FROM cpu WHERE time >= 0 AND time < 60000000000 GROUP BY cpu"
            ));

            assert_snapshot!(plan(
                "SELECT percentile(usage_idle,50), percentile(usage_idle,90) FROM cpu"
            ));

            assert_snapshot!(plan(
                "SELECT percentile(usage_idle,50), percentile(usage_idle,90) FROM cpu GROUP BY cpu"
            ));

            assert_snapshot!(plan(
                "SELECT percentile(usage_idle,50), percentile(usage_idle,90) FROM cpu GROUP BY cpu"
            ));

            assert_snapshot!(plan(
                "SELECT percentile(usage_idle,50), percentile(usage_idle,90) FROM cpu WHERE time >= 0 AND time < 60000000000 GROUP BY time(10s), cpu"
            ));
        }

        #[test]
        fn test_top() {
            assert_snapshot!(plan("SELECT top(usage_idle,10) FROM cpu"));

            assert_snapshot!(plan("SELECT top(usage_idle,10),cpu FROM cpu"));

            assert_snapshot!(plan("SELECT top(usage_idle,10) FROM cpu GROUP BY cpu"));

            assert_snapshot!(plan("SELECT top(usage_idle,cpu,10) FROM cpu"));
        }

        #[test]
        fn test_bottom() {
            assert_snapshot!(plan("SELECT bottom(usage_idle,10) FROM cpu"));

            assert_snapshot!(plan("SELECT bottom(usage_idle,10),cpu FROM cpu"));

            assert_snapshot!(plan("SELECT bottom(usage_idle,10) FROM cpu GROUP BY cpu"));

            assert_snapshot!(plan("SELECT bottom(usage_idle,cpu,10) FROM cpu"));
        }

        /// Test InfluxQL-specific behaviour of scalar functions that differ
        /// from DataFusion
        #[test]
        fn test_scalar_functions() {
            // LOG requires two arguments, and first argument is field
            assert_snapshot!(plan("SELECT LOG(usage_idle, 8) FROM cpu"));

            // LOG should coerce Int64 to Float64.
            assert_snapshot!(plan("SELECT LOG(i64_field, 8) FROM all_types"));
            // LOG should coerce UInt64 to Float64.
            assert_snapshot!(plan("SELECT LOG(u64_field, 8) FROM all_types"));

            // Fallible

            // LOG requires two arguments
            assert_snapshot!(plan("SELECT LOG(usage_idle) FROM cpu"));
        }

        /// Validate the metadata is correctly encoded in the schema.
        ///
        /// Properties that are tested:
        ///
        /// * only tag keys listed in a `GROUP BY` clause are included in the `tag_key_columns` vector
        /// * `tag_key_columns` is order by `tag_key`
        #[test]
        fn test_metadata_in_schema() {
            macro_rules! assert_tag_keys {
                ($MD:expr $(,($KEY:literal, $VAL:literal, $PROJ:literal))+) => {
                    assert_eq!(
                        $MD.tag_key_columns.clone().into_iter().map(|v| (v.tag_key, v.column_index, v.is_projected)).collect::<Vec<_>>(),
                        vec![$(($KEY.to_owned(), $VAL, $PROJ),)*],
                        "tag keys don't match"
                    );

                    let keys = $MD.tag_key_columns.into_iter().map(|v| v.tag_key).collect::<Vec<_>>();
                    let mut sorted = keys.clone();
                    sorted.sort_unstable();
                    assert_eq!(keys, sorted, "tag keys are not sorted");
                };
            }

            // validate metadata is empty when there is no group by
            let md = metadata("SELECT bytes_free FROM disk").unwrap();
            assert_eq!(md.measurement_column_index, 0);
            assert!(md.tag_key_columns.is_empty());
            let md = metadata("SELECT bytes_free FROM disk, cpu").unwrap();
            assert_eq!(md.measurement_column_index, 0);
            assert!(md.tag_key_columns.is_empty());

            let md = metadata("SELECT bytes_free FROM disk GROUP BY device").unwrap();
            assert_eq!(md.measurement_column_index, 0);
            assert_tag_keys!(md, ("device", 2, false));

            // validate tag in projection is not included in metadata
            let md = metadata("SELECT cpu, usage_idle, bytes_free FROM cpu, disk GROUP BY device")
                .unwrap();
            assert_eq!(md.measurement_column_index, 0);
            assert_tag_keys!(md, ("device", 2, false));

            // validate multiple tags from different measurements
            let md = metadata("SELECT usage_idle, bytes_free FROM cpu, disk GROUP BY cpu, device")
                .unwrap();
            assert_eq!(md.measurement_column_index, 0);
            assert_tag_keys!(md, ("cpu", 2, false), ("device", 3, false));

            // validate multiple tags from different measurements, and key order is maintained
            let md = metadata("SELECT usage_idle, bytes_free FROM cpu, disk GROUP BY device, cpu")
                .unwrap();
            assert_eq!(md.measurement_column_index, 0);
            assert_tag_keys!(md, ("cpu", 2, false), ("device", 3, false));

            // validate that with cpu tag explicitly listed in project, tag-key order is maintained and column index
            // is valid
            let md =
                metadata("SELECT usage_idle, bytes_free, cpu FROM cpu, disk GROUP BY cpu, device")
                    .unwrap();
            assert_eq!(md.measurement_column_index, 0);
            assert_tag_keys!(md, ("cpu", 5, true), ("device", 2, false));

            // validate region tag, shared by both measurements, is still correctly handled
            let md = metadata(
                "SELECT region, usage_idle, bytes_free, cpu FROM cpu, disk GROUP BY region, cpu, device",
            )
            .unwrap();
            assert_eq!(md.measurement_column_index, 0);
            assert_tag_keys!(
                md,
                ("cpu", 6, true),
                ("device", 2, false),
                ("region", 3, true)
            );
        }

        /// Verify the behaviour of the `FROM` clause when selecting from zero to many measurements.
        #[test]
        fn test_from_zero_to_many() {
            assert_snapshot!(plan(
                "SELECT host, cpu, device, usage_idle, bytes_used FROM cpu, disk"
            ));

            // nonexistent
            assert_snapshot!(plan("SELECT host, usage_idle FROM non_existent"));
            assert_snapshot!(plan("SELECT host, usage_idle FROM cpu, non_existent"));

            // multiple of same measurement
            assert_snapshot!(plan("SELECT host, usage_idle FROM cpu, cpu"));
        }

        #[test]
        fn test_time_range_in_where() {
            assert_snapshot!(plan(
                "SELECT foo, f64_field FROM data where time > now() - 10s"
            ));
            assert_snapshot!(plan(
                "SELECT foo, f64_field FROM data where time > '2004-04-09T02:33:45Z'"
            ));
            assert_snapshot!(plan(
                "SELECT foo, f64_field FROM data where time > '2004-04-09T'"
            ));

            // time on the right-hand side
            assert_snapshot!(plan(
                "SELECT foo, f64_field FROM data where  now() - 10s < time"
            ));

            // fallible

            // Unsupported operator
            assert_snapshot!(plan("SELECT foo, f64_field FROM data where time != 0"));

            // invalid timestamp
            // regression test for https://github.com/influxdata/influxdb_iox/issues/13535
            assert_snapshot!(plan(
                "SELECT foo, f64_field FROM data where time >= '0001-01-01T00:00:00Z' and time < '0001-01-01T00:00:00Z'"
            ));
        }

        #[test]
        fn test_regex_in_where() {
            test_helpers::maybe_start_logging();
            // Regular expression equality tests

            assert_snapshot!(plan("SELECT foo, f64_field FROM data where foo =~ /f/"));

            // regular expression for a numeric field is rewritten to `false`
            assert_snapshot!(plan(
                "SELECT foo, f64_field FROM data where f64_field =~ /f/"
            ));

            // regular expression for a non-existent field is rewritten to `false`
            assert_snapshot!(plan(
                "SELECT foo, f64_field FROM data where non_existent =~ /f/"
            ));

            // Regular expression inequality tests

            assert_snapshot!(plan("SELECT foo, f64_field FROM data where foo !~ /f/"));

            // regular expression for a numeric field is rewritten to `false`
            assert_snapshot!(plan(
                "SELECT foo, f64_field FROM data where f64_field !~ /f/"
            ));

            // regular expression for a non-existent field is rewritten to `false`
            assert_snapshot!(plan(
                "SELECT foo, f64_field FROM data where non_existent !~ /f/"
            ));
        }

        #[test]
        fn test_column_matching_rules() {
            // Cast between numeric types
            assert_snapshot!(plan("SELECT f64_field::integer FROM data"));
            assert_snapshot!(plan("SELECT i64_field::float FROM data"));

            // use field selector
            assert_snapshot!(plan("SELECT bool_field::field FROM data"));

            // invalid column reference
            assert_snapshot!(plan("SELECT not_exists::tag FROM data"));
            assert_snapshot!(plan("SELECT not_exists::field FROM data"));

            // Returns NULL for invalid casts
            assert_snapshot!(plan("SELECT f64_field::string FROM data"));
            assert_snapshot!(plan("SELECT f64_field::boolean FROM data"));
            assert_snapshot!(plan("SELECT str_field::boolean FROM data"));
        }

        #[test]
        fn test_explain() {
            assert_snapshot!(plan("EXPLAIN SELECT foo, f64_field FROM data"));
            assert_snapshot!(plan("EXPLAIN VERBOSE SELECT foo, f64_field FROM data"));
            assert_snapshot!(plan("EXPLAIN ANALYZE SELECT foo, f64_field FROM data"));
            assert_snapshot!(plan(
                "EXPLAIN ANALYZE VERBOSE SELECT foo, f64_field FROM data"
            ));
            assert_snapshot!(plan("EXPLAIN SHOW MEASUREMENTS"));
            assert_snapshot!(plan("EXPLAIN SHOW TAG KEYS"));

            assert_snapshot!(plan("EXPLAIN SHOW FIELD KEYS"));

            assert_snapshot!(plan("EXPLAIN SHOW RETENTION POLICIES"));

            assert_snapshot!(plan("EXPLAIN SHOW DATABASES"));
            assert_snapshot!(plan("EXPLAIN EXPLAIN SELECT f64_field::string FROM data"));
        }

        #[test]
        fn test_select_cast_postfix_operator() {
            // Float casting
            assert_snapshot!(plan("SELECT f64_field::float FROM all_types"));
            assert_snapshot!(plan("SELECT f64_field::unsigned FROM all_types"));
            assert_snapshot!(plan("SELECT f64_field::integer FROM all_types"));
            assert_snapshot!(plan("SELECT f64_field::string FROM all_types"));
            assert_snapshot!(plan("SELECT f64_field::boolean FROM all_types"));

            // Integer casting
            assert_snapshot!(plan("SELECT i64_field::float FROM all_types"));
            assert_snapshot!(plan("SELECT i64_field::unsigned FROM all_types"));
            assert_snapshot!(plan("SELECT i64_field::integer FROM all_types"));
            assert_snapshot!(plan("SELECT i64_field::string FROM all_types"));
            assert_snapshot!(plan("SELECT i64_field::boolean FROM all_types"));

            // Unsigned casting
            assert_snapshot!(plan("SELECT u64_field::float FROM all_types"));
            assert_snapshot!(plan("SELECT u64_field::unsigned FROM all_types"));
            assert_snapshot!(plan("SELECT u64_field::integer FROM all_types"));
            assert_snapshot!(plan("SELECT u64_field::string FROM all_types"));
            assert_snapshot!(plan("SELECT u64_field::boolean FROM all_types"));

            // String casting
            assert_snapshot!(plan("SELECT str_field::float FROM all_types"));
            assert_snapshot!(plan("SELECT str_field::unsigned FROM all_types"));
            assert_snapshot!(plan("SELECT str_field::integer FROM all_types"));
            assert_snapshot!(plan("SELECT str_field::string FROM all_types"));
            assert_snapshot!(plan("SELECT str_field::boolean FROM all_types"));

            // Boolean casting
            assert_snapshot!(plan("SELECT bool_field::float FROM all_types"));
            assert_snapshot!(plan("SELECT bool_field::unsigned FROM all_types"));
            assert_snapshot!(plan("SELECT bool_field::integer FROM all_types"));
            assert_snapshot!(plan("SELECT bool_field::string FROM all_types"));
            assert_snapshot!(plan("SELECT bool_field::boolean FROM all_types"));

            // Validate various projection expressions with casts

            assert_snapshot!(plan(
                "SELECT f64_field::integer + i64_field + u64_field::integer FROM all_types"
            ));

            assert_snapshot!(plan(
                "SELECT f64_field::integer + i64_field + str_field::integer FROM all_types"
            ));
        }

        /// Fix for https://github.com/influxdata/influxdb_iox/issues/8168
        #[test]
        fn test_integer_division_float_promotion() {
            assert_snapshot!(plan("SELECT i64_field / i64_field FROM all_types"));
            assert_snapshot!(plan(
                "SELECT sum(i64_field) / sum(i64_field) FROM all_types"
            ));
        }

        /// See <https://github.com/influxdata/influxdb_iox/issues/9175>
        #[test]
        fn test_true_and_time_pred() {
            assert_snapshot!(plan(
                "SELECT f64_field FROM data WHERE true AND time < '2022-10-31T02:02:00Z'"
            ));
        }

        #[test]
        fn test_spread() {
            assert_snapshot!(plan("SELECT SPREAD(usage_idle) as cpu_usage_idle FROM cpu"));

            assert_snapshot!(plan("SELECT SPREAD(*) FROM cpu"));
        }

        #[test]
        fn test_mode() {
            assert_snapshot!(plan("SELECT MODE(usage_idle) as cpu_usage_idle FROM cpu"));

            assert_snapshot!(plan("SELECT MODE(*) FROM cpu"));
        }

        /// Tests for timezone (TZ) clause
        #[test]
        fn test_timezone_clause() {
            // Basic timezone
            assert_snapshot!(plan(
                "SELECT f64_field FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' TZ('America/Los_Angeles')"
            ));

            // Timezone with GROUP BY TIME
            assert_snapshot!(plan(
                "SELECT MEAN(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s) TZ('Europe/London')"
            ));

            // Timezone with different zones
            assert_snapshot!(plan(
                "SELECT f64_field FROM data WHERE time >= '2022-10-31T02:00:00Z' TZ('UTC')"
            ));

            assert_snapshot!(plan(
                "SELECT f64_field FROM data WHERE time >= '2022-10-31T02:00:00Z' TZ('Asia/Tokyo')"
            ));
        }

        /// Tests for ORDER BY DESC
        #[test]
        fn test_order_by_desc() {
            assert_snapshot!(plan("SELECT f64_field FROM data ORDER BY TIME DESC"));

            assert_snapshot!(plan(
                "SELECT MEAN(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s) ORDER BY TIME DESC"
            ));

            assert_snapshot!(plan(
                "SELECT LAST(usage_idle) FROM cpu GROUP BY cpu ORDER BY TIME DESC"
            ));
        }

        /// Tests for wildcard expressions in different contexts
        #[test]
        fn test_wildcard_expressions() {
            // Wildcard with aggregates
            assert_snapshot!(plan("SELECT MEAN(*) FROM data"));
            assert_snapshot!(plan("SELECT SUM(*) FROM data"));
            assert_snapshot!(plan("SELECT COUNT(*) FROM data"));

            // Wildcard with selectors
            assert_snapshot!(plan("SELECT FIRST(*) FROM data"));
            assert_snapshot!(plan("SELECT LAST(*) FROM data"));

            // Regex matching fields
            assert_snapshot!(plan("SELECT MEAN(/^f64/) FROM data"));
            assert_snapshot!(plan("SELECT SUM(/field$/) FROM data"));
        }

        /// Tests for complex nested subqueries
        #[test]
        fn test_complex_subqueries() {
            // Subquery with window function over aggregate
            assert_snapshot!(plan(
                "SELECT DIFFERENCE(mean_value) FROM (SELECT MEAN(f64_field) AS mean_value FROM data GROUP BY TIME(10s))"
            ));

            // Multiple levels of nesting
            assert_snapshot!(plan(
                "SELECT * FROM (SELECT MEAN(value) AS mean_value FROM (SELECT f64_field AS value FROM data))"
            ));

            // Subquery with FILL
            assert_snapshot!(plan(
                "SELECT value FROM (SELECT MEAN(f64_field) AS value FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s) FILL(0))"
            ));
        }

        /// Tests for error cases and edge conditions
        #[test]
        fn test_error_cases() {
            // Non-numeric field with numeric aggregate
            assert_snapshot!(plan("SELECT MEAN(str_field) FROM data"));
            assert_snapshot!(plan("SELECT SUM(bool_field) FROM data"));

            // Aggregate on tag
            assert_snapshot!(plan("SELECT MEAN(foo) FROM data"));

            // Invalid function argument counts
            assert_snapshot!(plan("SELECT MEAN() FROM data"));
            assert_snapshot!(plan("SELECT MEAN(f64_field, i64_field) FROM data"));

            // Window function without GROUP BY TIME
            assert_snapshot!(plan("SELECT MOVING_AVERAGE(f64_field, 3) FROM data"));
        }

        /// Tests for queries across multiple measurements with schema differences
        #[test]
        fn test_multiple_measurements_schema_differences() {
            // Fields with same name but different types
            assert_snapshot!(plan(
                "SELECT usage_idle, bytes_free FROM cpu, disk WHERE time >= '2022-10-31T02:00:00Z'"
            ));

            // Aggregate across multiple measurements
            assert_snapshot!(plan(
                "SELECT MEAN(usage_idle), MEAN(bytes_free) FROM cpu, disk GROUP BY TIME(10s)"
            ));

            // Mixed tag sets
            assert_snapshot!(plan("SELECT * FROM cpu, disk GROUP BY cpu, device"));
        }
    }

    /// Tests to validate InfluxQL `SELECT` statements that project aggregate functions, such as `COUNT` or `SUM`.
    mod select_aggregate {
        use super::*;

        mod single_measurement {
            use super::*;

            #[test]
            fn no_group_by() {
                assert_snapshot!(plan("SELECT count(f64_field) FROM data"));
            }

            #[test]
            fn group_by_non_existent() {
                assert_snapshot!(plan(
                    "SELECT count(f64_field) FROM data GROUP BY non_existent"
                ));
            }

            #[test]
            fn group_by_foo() {
                assert_snapshot!(plan("SELECT count(f64_field) FROM data GROUP BY foo"));
            }

            #[test]
            fn multi_count_no_group_by() {
                // The `count(f64_field)` aggregate is only projected ones in the Aggregate and reused in the projection
                assert_snapshot!(plan(
                    "SELECT count(f64_field), count(f64_field) + count(f64_field), count(f64_field) * 3 FROM data"
                ));
            }

            #[test]
            fn group_by_foo_non_existent() {
                // non-existent tags are excluded from the Aggregate groupBy and Sort operators
                assert_snapshot!(plan(
                    "SELECT count(f64_field) FROM data GROUP BY foo, non_existent"
                ));
            }

            #[test]
            fn group_by_non_reused_in_projection() {
                // Aggregate expression is projected once and reused in final projection
                assert_snapshot!(plan(
                    "SELECT count(f64_field),  count(f64_field) * 2 FROM data"
                ));
            }

            #[test]
            fn aggregate_non_existent_field() {
                // Aggregate expression selecting non-existent field
                assert_snapshot!(plan(
                    "SELECT MEAN(f64_field) + MEAN(non_existent) FROM data"
                ));
            }

            #[test]
            fn aggregate_non_existent_field_and_group_by() {
                // Aggregate expression with GROUP BY and non-existent field
                assert_snapshot!(plan(
                    "SELECT MEAN(f64_field) + MEAN(non_existent) FROM data GROUP BY foo"
                ));
            }

            #[test]
            fn no_group_by_aggregate_tag() {
                // Aggregate expression selecting tag, should treat as non-existent
                assert_snapshot!(plan(
                    "SELECT MEAN(f64_field), MEAN(f64_field) + MEAN(non_existent) FROM data"
                ));
            }

            #[test]
            fn no_group_by_aggregate_error() {
                // Fallible

                // Cannot combine aggregate and non-aggregate columns in the projection
                assert_snapshot!(plan("SELECT count(f64_field), f64_field FROM data"));
                assert_snapshot!(plan("SELECT count(f64_field) + f64_field FROM data"));
            }

            #[test]
            fn group_by_time() {
                assert_snapshot!(plan(
                    "SELECT count(f64_field) FROM data GROUP BY TIME(10s) FILL(none)"
                ));

                // supports offset parameter
                assert_snapshot!(plan(
                    "SELECT count(f64_field) FROM data GROUP BY TIME(10s, 5s) FILL(none)"
                ));
            }

            #[test]
            fn group_by_time_gapfill_no_bounds() {
                // No time bounds
                assert_snapshot!(plan("SELECT count(f64_field) FROM data GROUP BY TIME(10s)"));
            }

            #[test]
            fn group_by_time_gapfill_no_lower_time_bounds() {
                // No lower time bounds
                assert_snapshot!(plan(
                    "SELECT count(f64_field) FROM data WHERE time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s)"
                ));
            }

            #[test]
            fn group_by_time_gapfill_no_upper_time_bounds() {
                // No upper time bounds
                assert_snapshot!(plan(
                    "SELECT count(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' GROUP BY TIME(10s)"
                ));
            }

            #[test]
            fn group_by_time_gapfill_defaul_is_fill_null1() {
                // Default is FILL(null)
                assert_snapshot!(plan(
                    "SELECT count(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s)"
                ));
            }

            #[test]
            fn group_by_time_gapfill_default_is_fill_null1() {
                assert_snapshot!(plan("SELECT count(f64_field) FROM data GROUP BY TIME(10s)"));
            }

            #[test]
            fn group_by_time_gapfill_default_is_fill_null2() {
                assert_snapshot!(plan(
                    "SELECT count(f64_field) FROM data GROUP BY TIME(10s) FILL(null)"
                ));
            }

            #[test]
            fn group_by_time_gapfill_default_is_fill_null3() {
                assert_snapshot!(plan(
                    "SELECT count(f64_field) FROM data GROUP BY TIME(10s) FILL(previous)"
                ));
            }

            #[test]
            fn group_by_time_gapfill_default_is_fill_null4() {
                assert_snapshot!(plan(
                    "SELECT count(f64_field) FROM data GROUP BY TIME(10s) FILL(0)"
                ));
            }

            #[test]
            fn group_by_time_gapfill_default_is_fill_null5() {
                assert_snapshot!(plan(
                    "SELECT count(f64_field) FROM data GROUP BY TIME(10s) FILL(linear)"
                ));
            }

            #[test]
            fn group_by_time_gapfill_plus_math() {
                // these plans should be semantically identical
                // See https://github.com/influxdata/EAR/issues/5343

                assert_snapshot!(plan(
                    "SELECT first(f64_field) FROM data GROUP BY TIME(10s) FILL(null)"
                ));

                assert_snapshot!(plan(
                    "SELECT first(f64_field) * 1 FROM data GROUP BY TIME(10s) FILL(null)"
                ));

                assert_snapshot!(plan(
                    "SELECT first(f64_field) / 1 FROM data GROUP BY TIME(10s) FILL(null)"
                ));
            }

            #[test]
            fn group_by_time_gapfill_coalesces_the_fill_value() {
                // Coalesces the fill value, which is a float, to the matching type of a `count` aggregate.
                assert_snapshot!(plan(
                    "SELECT count(f64_field) FROM data GROUP BY TIME(10s) FILL(3.2)"
                ));
            }

            #[test]
            fn group_by_time_gapfill_aggregates_part_of_binary_expression() {
                // Aggregates as part of a binary expression
                assert_snapshot!(plan(
                    "SELECT count(f64_field) + MEAN(f64_field) FROM data GROUP BY TIME(10s) FILL(3.2)"
                ));
            }

            #[test]
            fn with_limit_or_offset() {
                assert_snapshot!(plan(
                    "SELECT count(f64_field) FROM data GROUP BY foo LIMIT 1"
                ));
            }

            #[test]
            fn with_limit_or_offset2() {
                assert_snapshot!(plan(
                    "SELECT count(f64_field) FROM data GROUP BY foo OFFSET 1"
                ));
            }

            #[test]
            fn with_limit_or_offset3() {
                assert_snapshot!(plan(
                    "SELECT count(f64_field) FROM data GROUP BY foo LIMIT 2 OFFSET 3"
                ));
            }

            #[test]
            fn with_limit_or_offset_errors() {
                // Fallible

                // returns an error if LIMIT or OFFSET values exceed i64::MAX
                let max = (i64::MAX as u64) + 1;
                assert_snapshot!(
                    plan(format!(
                        "SELECT count(f64_field) FROM data GROUP BY foo LIMIT {max}"
                    )),
                    "with_limit_or_offset_errors-35"
                );
                assert_snapshot!(
                    plan(format!(
                        "SELECT count(f64_field) FROM data GROUP BY foo OFFSET {max}"
                    )),
                    "with_limit_or_offset_errors-36"
                );
            }

            /// Tests for INTEGRAL aggregate function
            #[test]
            fn test_integral() {
                // Basic integral
                assert_snapshot!(plan("SELECT INTEGRAL(f64_field) FROM data"));

                // Integral with unit
                assert_snapshot!(plan("SELECT INTEGRAL(f64_field, 1s) FROM data"));
                assert_snapshot!(plan("SELECT INTEGRAL(f64_field, 1m) FROM data"));

                // Integral with GROUP BY TIME
                assert_snapshot!(plan(
                    "SELECT INTEGRAL(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s)"
                ));

                // Integral with FILL(none) - should behave as fill(none) regardless
                assert_snapshot!(plan(
                    "SELECT INTEGRAL(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s) FILL(0)"
                ));

                // Integral mixed with other aggregates and FILL(linear)
                assert_snapshot!(plan(
                    "SELECT INTEGRAL(f64_field), MEAN(i64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s) FILL(linear)"
                ));

                // Multiple integrals
                assert_snapshot!(plan(
                    "SELECT INTEGRAL(f64_field), INTEGRAL(i64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s)"
                ));
            }

            /// Tests for STDDEV aggregate function
            #[test]
            fn test_stddev() {
                assert_snapshot!(plan("SELECT STDDEV(f64_field) FROM data"));
                assert_snapshot!(plan("SELECT STDDEV(i64_field) FROM data"));
                assert_snapshot!(plan("SELECT STDDEV(*) FROM data"));
                assert_snapshot!(plan("SELECT STDDEV(f64_field) FROM data GROUP BY foo"));
                assert_snapshot!(plan(
                    "SELECT STDDEV(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s)"
                ));
            }

            /// Tests for MEDIAN aggregate function
            #[test]
            fn test_median() {
                assert_snapshot!(plan("SELECT MEDIAN(f64_field) FROM data"));
                assert_snapshot!(plan("SELECT MEDIAN(i64_field) FROM data"));
                assert_snapshot!(plan("SELECT MEDIAN(*) FROM data"));
                assert_snapshot!(plan("SELECT MEDIAN(f64_field) FROM data GROUP BY foo"));
                assert_snapshot!(plan(
                    "SELECT MEDIAN(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s)"
                ));
            }

            /// Tests for MEAN aggregate function
            #[test]
            fn test_mean() {
                assert_snapshot!(plan("SELECT MEAN(f64_field) FROM data"));
                assert_snapshot!(plan("SELECT MEAN(i64_field) FROM data"));
                assert_snapshot!(plan("SELECT MEAN(u64_field) FROM all_types"));
                assert_snapshot!(plan("SELECT MEAN(*) FROM data"));
                assert_snapshot!(plan("SELECT MEAN(f64_field) FROM data GROUP BY foo"));
                assert_snapshot!(plan(
                    "SELECT MEAN(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s)"
                ));
                assert_snapshot!(plan(
                    "SELECT MEAN(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s) FILL(0)"
                ));
            }

            /// Tests for SUM aggregate function
            #[test]
            fn test_sum() {
                assert_snapshot!(plan("SELECT SUM(f64_field) FROM data"));
                assert_snapshot!(plan("SELECT SUM(i64_field) FROM data"));
                assert_snapshot!(plan("SELECT SUM(u64_field) FROM all_types"));
                assert_snapshot!(plan("SELECT SUM(*) FROM data"));
                assert_snapshot!(plan("SELECT SUM(f64_field) FROM data GROUP BY foo"));
                assert_snapshot!(plan(
                    "SELECT SUM(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s)"
                ));
                assert_snapshot!(plan(
                    "SELECT SUM(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s) FILL(previous)"
                ));
            }

            /// Tests for various FILL clause combinations
            #[test]
            fn test_fill_clause_variations() {
                // FILL with different aggregate functions
                assert_snapshot!(plan(
                    "SELECT MEAN(f64_field), SUM(i64_field), COUNT(str_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s) FILL(0)"
                ));

                // FILL(previous) with multiple aggregates
                assert_snapshot!(plan(
                    "SELECT MEAN(f64_field), MEDIAN(i64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s) FILL(previous)"
                ));

                // FILL with negative value
                assert_snapshot!(plan(
                    "SELECT COUNT(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s) FILL(-1)"
                ));

                // FILL with float value for integer aggregate
                assert_snapshot!(plan(
                    "SELECT COUNT(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s) FILL(3.14)"
                ));
            }

            /// These tests validate the planner returns an error when using features that
            /// are not implemented or supported.
            mod not_implemented {
                use super::*;

                /// Tracked by <https://github.com/influxdata/influxdb_iox/issues/7204>
                #[test]
                fn group_by_time_precision() {
                    assert_snapshot!(plan(
                        "SELECT count(f64_field) FROM data GROUP BY TIME(10u) FILL(none)"
                    ));
                }
            }
        }
    }

    /// Tests to validate InfluxQL `SELECT` statements that project columns without specifying
    /// aggregates or `GROUP BY time()` with gap filling.
    mod select_raw {
        use super::*;

        /// Select data from a single measurement
        #[test]
        fn test_single_measurement() {
            assert_snapshot!(plan("SELECT f64_field FROM data"));
            assert_snapshot!(plan("SELECT time, f64_field FROM data"));
            assert_snapshot!(plan("SELECT time as timestamp, f64_field FROM data"));
            assert_snapshot!(plan("SELECT foo, f64_field FROM data"));
            assert_snapshot!(plan("SELECT foo, f64_field, i64_field FROM data"));
            assert_snapshot!(plan("SELECT /^f/ FROM data"));
            assert_snapshot!(plan("SELECT * FROM data"));
            assert_snapshot!(plan("SELECT TIME FROM data")); // TIME is a field
        }

        /// Arithmetic expressions in the projection list
        #[test]
        fn test_simple_arithmetic_in_projection() {
            assert_snapshot!(plan("SELECT foo, f64_field + f64_field FROM data"));
            assert_snapshot!(plan("SELECT foo, sin(f64_field) FROM data"));
            assert_snapshot!(plan("SELECT foo, atan2(f64_field, 2) FROM data"));
            assert_snapshot!(plan("SELECT foo, atan2(i64_field, 2) FROM data"));
            assert_snapshot!(plan("SELECT tag0, atan2(u64_field, 2) FROM all_types"));
            assert_snapshot!(plan("SELECT foo, f64_field + 0.5 FROM data"));
        }

        #[test]
        fn test_select_single_measurement_group_by() {
            // Sort should be cpu, time
            assert_snapshot!(plan("SELECT usage_idle FROM cpu GROUP BY cpu"));

            // Sort should be cpu, time
            assert_snapshot!(plan("SELECT cpu, usage_idle FROM cpu GROUP BY cpu"));

            // Sort should be cpu, region, time
            assert_snapshot!(plan("SELECT usage_idle FROM cpu GROUP BY cpu, region"));

            // Sort should be cpu, region, time
            assert_snapshot!(plan("SELECT usage_idle FROM cpu GROUP BY region, cpu"));

            // Sort should be cpu, time, region
            assert_snapshot!(plan("SELECT region, usage_idle FROM cpu GROUP BY cpu"));

            // If a tag specified in a GROUP BY does not exist in the measurement, it should be omitted from the sort
            assert_snapshot!(plan(
                "SELECT usage_idle FROM cpu GROUP BY cpu, non_existent"
            ));

            // If a tag specified in a projection does not exist in the measurement, it should be omitted from the sort
            assert_snapshot!(plan(
                "SELECT usage_idle, cpu, non_existent FROM cpu GROUP BY cpu"
            ));

            // If a non-existent field is included in the GROUP BY and projection, it should not be duplicated
            assert_snapshot!(plan(
                "SELECT usage_idle, non_existent FROM cpu GROUP BY cpu, non_existent"
            ));
        }

        #[test]
        fn test_select_multiple_measurements_group_by() {
            // Sort should be iox::measurement, cpu, time
            assert_snapshot!(plan(
                "SELECT usage_idle, bytes_free FROM cpu, disk GROUP BY cpu"
            ));

            // Sort should be iox::measurement, cpu, device, time
            assert_snapshot!(plan(
                "SELECT usage_idle, bytes_free FROM cpu, disk GROUP BY device, cpu"
            ));

            // Sort should be iox::measurement, cpu, time, device
            assert_snapshot!(plan(
                "SELECT device, usage_idle, bytes_free FROM cpu, disk GROUP BY cpu"
            ));

            // Sort should be iox::measurement, cpu, device, time
            assert_snapshot!(plan(
                "SELECT cpu, usage_idle, bytes_free FROM cpu, disk GROUP BY cpu, device"
            ));

            // Sort should be iox::measurement, device, time, cpu
            assert_snapshot!(plan(
                "SELECT cpu, usage_idle, bytes_free FROM cpu, disk GROUP BY device"
            ));

            // If a tag specified in a GROUP BY does not exist across all measurements, it should be omitted from the sort
            assert_snapshot!(plan(
                "SELECT cpu, usage_idle, bytes_free FROM cpu, disk GROUP BY device, non_existent"
            ));

            // If a tag specified in a projection does not exist across all measurements, it should be omitted from the sort
            assert_snapshot!(plan(
                "SELECT cpu, usage_idle, bytes_free, non_existent FROM cpu, disk GROUP BY device"
            ));
        }

        #[test]
        fn test_select_group_by_limit_offset() {
            assert_snapshot!(plan("SELECT usage_idle FROM cpu GROUP BY cpu LIMIT 1"));
            assert_snapshot!(plan("SELECT usage_idle FROM cpu GROUP BY cpu OFFSET 1"));
            assert_snapshot!(plan(
                "SELECT usage_idle FROM cpu GROUP BY cpu LIMIT 1 OFFSET 1"
            ));
        }

        #[test]
        fn test_select_function_tag_column() {
            assert_snapshot!(plan(
                "SELECT last(foo) as foo, first(usage_idle) from cpu group by foo"
            ));
            assert_snapshot!(plan(
                "SELECT count(foo) as foo, first(usage_idle) from cpu group by foo"
            ));
        }

        /// Tests for field matching rules
        #[test]
        fn test_field_matching_rules() {
            // Correctly matches mixed case
            assert_snapshot!(plan("SELECT mixedCase FROM data"));
            assert_snapshot!(plan("SELECT \"mixedCase\" FROM data"));

            // Does not match when case differs
            assert_snapshot!(plan("SELECT MixedCase FROM data"));

            // Matches those that require quotes
            assert_snapshot!(plan("SELECT \"with space\" FROM data"));
            assert_snapshot!(plan("SELECT /(with|f64)/ FROM data"));
        }

        /// Tests for queries against measurements that don't exist
        #[test]
        fn test_measurement_doesnt_exist() {
            assert_snapshot!(plan("SELECT f64_field FROM data_1"));
            assert_snapshot!(plan("SELECT foo, f64_field FROM data_1"));
            assert_snapshot!(plan("SELECT /^f/ FROM data_1"));
            assert_snapshot!(plan("SELECT * FROM data_1"));
        }

        /// Tests for measurement that exists, with mixture of fields that do and don't exist
        #[test]
        fn test_mixture_of_existing_and_missing_fields() {
            assert_snapshot!(plan("SELECT f64_field, missing FROM data"));
            assert_snapshot!(plan("SELECT foo, missing FROM data"));
        }

        /// Tests for mathematical scalar functions in the projection list, including
        /// those in arithmetic expressions.
        #[test]
        fn test_mathematical_scalar_functions() {
            assert_snapshot!(plan("SELECT abs(f64_field) FROM data"));
            assert_snapshot!(plan("SELECT ceil(f64_field) FROM data"));
            assert_snapshot!(plan("SELECT floor(f64_field) FROM data"));
            assert_snapshot!(plan("SELECT pow(f64_field, 3) FROM data"));
            assert_snapshot!(plan("SELECT pow(i64_field, 3) FROM data"));
        }

        /// Tests for WHERE clause with conditional expressions for tag and field references
        #[test]
        fn test_where_clause_conditional_expressions() {
            // Arithmetic expressions in WHERE
            assert_snapshot!(plan(
                "SELECT f64_field FROM data WHERE f64_field + 10 > 100"
            ));
            assert_snapshot!(plan("SELECT f64_field FROM data WHERE i64_field * 2 = 20"));

            // Regular expressions for tags and fields
            assert_snapshot!(plan("SELECT f64_field FROM data WHERE foo =~ /^host/"));
            assert_snapshot!(plan("SELECT f64_field FROM data WHERE foo !~ /^host/"));
        }

        /// Tests for mathematical expressions in the WHERE clause
        #[test]
        fn test_where_clause_mathematical_expressions() {
            assert_snapshot!(plan("SELECT f64_field FROM data WHERE abs(f64_field) > 10"));
            assert_snapshot!(plan("SELECT f64_field FROM data WHERE ceil(f64_field) = 5"));
            assert_snapshot!(plan(
                "SELECT f64_field FROM data WHERE floor(f64_field) < 3"
            ));
            assert_snapshot!(plan(
                "SELECT f64_field FROM data WHERE pow(f64_field, 2) > 100"
            ));
        }
    }

    /// This module contains esoteric features of InfluxQL that are identified during
    /// the development of other features, and require additional work to implement or resolve.
    ///
    /// These tests are all ignored and will be promoted to the `test` module when resolved.
    ///
    /// By containing them in a submodule, they appear neatly grouped together in test output.
    mod issues {
        use super::*;

        /// **Issue:**
        /// Fails InfluxQL type coercion rules
        /// **Expected:**
        /// Succeeds and returns null values for the expression
        /// **Actual:**
        /// Error during planning: 'Float64 + Utf8' can't be evaluated because there isn't a common type to coerce the types to
        #[test]
        #[ignore]
        fn test_select_coercion_from_str() {
            assert_snapshot!(plan("SELECT f64_field + str_field::float FROM data"));
        }

        /// **Issue:**
        /// InfluxQL identifiers are case-sensitive and query fails to ignore unknown identifiers
        /// **Expected:**
        /// Succeeds and plans the query, returning null values for unknown columns
        /// **Actual:**
        /// Schema error: No field named 'TIME'. Valid fields are 'data'.'bar', 'data'.'bool_field', 'data'.'f64_field', 'data'.'foo', 'data'.'i64_field', 'data'.'mixedCase', 'data'.'str_field', 'data'.'time', 'data'.'with space'.
        #[test]
        #[ignore]
        fn test_select_case_sensitivity() {
            // should return no results
            assert_snapshot!(plan("SELECT TIME, f64_Field FROM data"));

            // should bind to time and f64_field, and i64_Field should return NULL values
            assert_snapshot!(plan("SELECT time, f64_field, i64_Field FROM data"));
        }
    }
}
