mod select;

use crate::aggregate::PERCENTILE;
use crate::error;
use crate::plan::ir::{DataSource, Field, Interval, Select, SelectQuery};
use crate::plan::planner::select::{
    fields_to_exprs_no_nulls, make_tag_key_column_meta, plan_with_sort, ProjectionInfo, Selector,
    SelectorWindowOrderBy,
};
use crate::plan::planner_time_range_expression::time_range_to_df_expr;
use crate::plan::rewriter::{find_table_names, rewrite_statement, ProjectionType};
use crate::plan::udf::{
    cumulative_sum, derivative, difference, find_window_udfs, moving_average,
    non_negative_derivative, non_negative_difference,
};
use crate::plan::util::{binary_operator_to_df_operator, rebase_expr, IQLSchema};
use crate::plan::var_ref::var_ref_data_type_to_data_type;
use crate::plan::{planner_rewrite_expression, udf};
use crate::window::{
    CUMULATIVE_SUM, DERIVATIVE, DIFFERENCE, MOVING_AVERAGE, NON_NEGATIVE_DERIVATIVE,
    NON_NEGATIVE_DIFFERENCE, PERCENT_ROW_NUMBER,
};
use arrow::array::{
    BooleanArray, DictionaryArray, Int32Array, Int64Array, StringArray, StringBuilder,
    StringDictionaryBuilder,
};
use arrow::datatypes::{DataType, Field as ArrowField, Int32Type, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use chrono_tz::Tz;
use datafusion::catalog::TableReference;
use datafusion::common::tree_node::{Transformed, TreeNode, VisitRecursion};
use datafusion::common::{DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue, ToDFSchema};
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::expr::{Alias, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::normalize_col;
use datafusion::logical_expr::logical_plan::builder::project;
use datafusion::logical_expr::logical_plan::Analyze;
use datafusion::logical_expr::utils::{expr_as_column_expr, find_aggregate_exprs};
use datafusion::logical_expr::{
    binary_expr, col, date_bin, expr, expr::WindowFunction, lit, lit_timestamp_nano, now, union,
    window_function, AggregateFunction, AggregateUDF, Between, BuiltInWindowFunction,
    BuiltinScalarFunction, EmptyRelation, Explain, Expr, ExprSchemable, Extension, LogicalPlan,
    LogicalPlanBuilder, Operator, PlanType, Projection, ScalarUDF, TableSource, ToStringifiedPlan,
    WindowFrame, WindowFrameBound, WindowFrameUnits,
};
use datafusion::optimizer::utils::conjunction;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::prelude::{cast, sum, when, Column};
use datafusion_util::{lit_dict, AsExpr};
use generated_types::influxdata::iox::querier::v1::InfluxQlMetadata;
use influxdb_influxql_parser::common::{LimitClause, OffsetClause, OrderByClause};
use influxdb_influxql_parser::explain::{ExplainOption, ExplainStatement};
use influxdb_influxql_parser::expression::walk::{walk_expr, walk_expression, Expression};
use influxdb_influxql_parser::expression::{
    Binary, Call, ConditionalBinary, ConditionalExpression, ConditionalOperator, VarRef,
    VarRefDataType,
};
use influxdb_influxql_parser::functions::{
    is_aggregate_function, is_now_function, is_scalar_math_function,
};
use influxdb_influxql_parser::select::{FillClause, GroupByClause};
use influxdb_influxql_parser::show_field_keys::ShowFieldKeysStatement;
use influxdb_influxql_parser::show_measurements::{
    ShowMeasurementsStatement, WithMeasurementClause,
};
use influxdb_influxql_parser::show_retention_policies::ShowRetentionPoliciesStatement;
use influxdb_influxql_parser::show_tag_keys::ShowTagKeysStatement;
use influxdb_influxql_parser::show_tag_values::{ShowTagValuesStatement, WithKeyClause};
use influxdb_influxql_parser::simple_from_clause::ShowFromClause;
use influxdb_influxql_parser::time_range::{split_cond, ReduceContext, TimeRange};
use influxdb_influxql_parser::timestamp::Timestamp;
use influxdb_influxql_parser::{
    common::{MeasurementName, WhereClause},
    expression::Expr as IQLExpr,
    literal::Literal,
    select::SelectStatement,
    statement::Statement,
};
use iox_query::config::{IoxConfigExt, MetadataCutoff};
use iox_query::exec::gapfill::{FillStrategy, GapFill, GapFillParams};
use iox_query::exec::IOxSessionContext;
use iox_query::logical_optimizer::range_predicate::find_time_range;
use itertools::Itertools;
use observability_deps::tracing::debug;
use query_functions::{
    clean_non_meta_escapes,
    selectors::{selector_first, selector_last, selector_max, selector_min},
};
use schema::{
    InfluxColumnType, InfluxFieldType, Schema, INFLUXQL_MEASUREMENT_COLUMN_NAME,
    INFLUXQL_METADATA_KEY,
};
use std::collections::{hash_map::Entry, BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::iter;
use std::ops::{Bound, ControlFlow, Deref, Not, Range};
use std::str::FromStr;
use std::sync::Arc;

use super::parse_regex;
use super::util::contains_expr;

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
#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
struct Context<'a> {
    /// The name of the table used as the data source for the current query.
    table_name: &'a str,
    projection_type: ProjectionType,
    tz: Option<Tz>,

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
            table_name,
            projection_type: select.projection_type,
            tz: select.timezone,
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
    fn subquery(&self, select: &'a Select) -> Self {
        Self {
            table_name: self.table_name,
            projection_type: select.projection_type,
            tz: select.timezone,
            order_by: self.order_by,
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

    /// Return a [`Expr::Sort`] expression for the `time` column.
    #[allow(dead_code)]
    fn time_sort_expr(&self) -> Expr {
        self.time_alias.as_expr().sort(
            match self.order_by {
                OrderByClause::Ascending => true,
                OrderByClause::Descending => false,
            },
            false,
        )
    }

    /// Returns true if the current context has an extended
    /// time range to provide leading data for window functions
    /// to produce the result for the first window.
    #[allow(dead_code)]
    fn has_extended_time_range(&self) -> bool {
        self.extra_intervals > 0 && self.interval.is_some()
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
    /// # EXPECTED
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
    /// # BUG
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
    /// [1]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/compile.go#L592-L594
    /// [2]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/compile.go#L1153-L1158
    /// [3]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/compile.go#L1172-L1173
    /// [4]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/compile.go#L1198
    /// [5]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/select.go#L259-L261
    /// [6]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/select.go#L268-L267
    /// [7]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/select.go#L286-L290
    /// [8]: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/select.go#L295
    #[allow(dead_code)]
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

    fn fill(&self) -> FillClause {
        self.fill.unwrap_or_default()
    }
}

#[allow(missing_debug_implementations)]
/// InfluxQL query planner
pub struct InfluxQLToLogicalPlan<'a> {
    s: &'a dyn SchemaProvider,
    iox_ctx: &'a IOxSessionContext,
}

impl<'a> InfluxQLToLogicalPlan<'a> {
    pub fn new(s: &'a dyn SchemaProvider, iox_ctx: &'a IOxSessionContext) -> Self {
        Self { s, iox_ctx }
    }

    pub fn statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        match statement {
            Statement::CreateDatabase(_) => error::not_implemented("CREATE DATABASE"),
            Statement::Delete(_) => error::not_implemented("DELETE"),
            Statement::DropMeasurement(_) => error::not_implemented("DROP MEASUREMENT"),
            Statement::Explain(explain) => self.explain_statement_to_plan(*explain),
            Statement::Select(select) => {
                self.select_query_to_plan(&self.rewrite_select_statement(*select)?)
            }
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

    fn explain_statement_to_plan(&self, explain: ExplainStatement) -> Result<LogicalPlan> {
        let plan = self.select_query_to_plan(&self.rewrite_select_statement(*explain.select)?)?;
        let plan = Arc::new(plan);
        let schema = LogicalPlan::explain_schema();
        let schema = schema.to_dfschema_ref()?;

        // We'll specify the `plan_type` column as the "measurement name", so that it may be
        // grouped into tables in the output when formatted as InfluxQL tabular format.
        let measurement_column_index = schema
            .index_of_column_by_name(None, "plan_type")?
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
                            .chain(
                                input
                                    .schema()
                                    .fields()
                                    .iter()
                                    .map(|expr| Expr::Column(expr.unqualified_column())),
                            ),
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
                union(prev, next)
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

        let time_sort_expr = time_alias.as_expr().sort(
            match order_by {
                OrderByClause::Ascending => true,
                OrderByClause::Descending => false,
            },
            false,
        );

        let plan = plan_with_sort(
            plan,
            vec![time_sort_expr.clone()],
            sort_by_measurement,
            &group_by_tag_set,
            &projection_tag_set,
        )?;

        self.limit(
            plan,
            select.offset,
            select.limit,
            vec![time_sort_expr],
            sort_by_measurement,
            &group_by_tag_set,
            &projection_tag_set,
        )
    }

    fn subquery_to_plan(&self, ctx: &Context<'_>, select: &Select) -> Result<Option<LogicalPlan>> {
        let ctx = ctx.subquery(select);

        let Some(plan) = self.union_from(&ctx, select)? else {
            return Ok(None)
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

        let plan = plan_with_sort(
            plan,
            vec![time_sort_expr.clone()],
            false,
            &group_by_tag_set,
            &projection_tag_set,
        )?;

        Ok(Some(self.limit(
            plan,
            select.offset,
            select.limit,
            vec![time_sort_expr],
            false,
            &group_by_tag_set,
            &projection_tag_set,
        )?))
    }

    /// Returns a `LogicalPlan` that combines the `FROM` clause as a `UNION ALL`.
    fn union_from(&self, ctx: &Context<'_>, select: &Select) -> Result<Option<LogicalPlan>> {
        let mut plans = Vec::new();
        for ds in &select.from {
            let Some(plan) = self.plan_from_data_source(ctx, ds)? else {
                continue;
            };

            let schema = IQLSchema::new_from_ds_schema(plan.schema(), ds.schema(self.s)?)?;
            let plan = self.plan_condition_time_range(
                ctx.condition,
                ctx.extended_time_range(),
                plan,
                &schema,
            )?;
            plans.push((plan, schema));
        }

        Ok(match plans.len() {
            0 => None,
            1 => plans.pop().map(|(plan, _)| plan),
            _ => {
                // find all the columns referenced in the `SELECT`
                let var_refs = find_var_refs(select);

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
                                            "cannot mix tag and field columns with the same name: {}",
                                            name
                                        )),
                                _ => Ok(()),

                            }?;

                            if schema.has_column_with_unqualified_name(name) {
                                Ok(name.as_expr().alias(name))
                            } else {
                                Ok(lit(ScalarValue::Null).alias(name))
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
                    iter.try_fold(plan, union)?
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
            ProjectionType::Raw => self.project_select_raw(input, fields),
            ProjectionType::RawDistinct => self.project_select_raw_distinct(input, fields),
            ProjectionType::Aggregate   => self.project_select_aggregate(ctx, input, fields, group_by_tag_set),
            ProjectionType::Window => self.project_select_window(ctx, input, fields, group_by_tag_set),
            ProjectionType::WindowAggregate => self.project_select_window_aggregate(ctx, input, fields, group_by_tag_set),
            ProjectionType::WindowAggregateMixed => error::not_implemented("mixed window-aggregate and aggregate columns, such as DIFFERENCE(MEAN(col)), MEAN(col)"),
            ProjectionType::Selector{..} => self.project_select_selector(ctx, input, fields, group_by_tag_set),
            ProjectionType::TopBottomSelector => self.project_select_top_bottom_selector(ctx, input, fields, group_by_tag_set),
        }
    }

    /// Plan "Raw" SELECT queriers, These are queries that have no grouping
    /// and call only scalar functions.
    fn project_select_raw(&self, input: LogicalPlan, fields: &[Field]) -> Result<LogicalPlan> {
        let schema = IQLSchema::new_from_fields(input.schema(), fields)?;

        // Transform InfluxQL AST field expressions to a list of DataFusion expressions.
        let select_exprs = self.field_list_to_exprs(&input, fields, &schema)?;

        // Wrap the plan in a `LogicalPlan::Projection` from the select expressions
        project(input, select_exprs)
    }

    /// Plan "RawDistinct" SELECT queriers, These are queries that have no grouping
    /// and call only scalar functions, but output only distinct rows.
    fn project_select_raw_distinct(
        &self,
        input: LogicalPlan,
        fields: &[Field],
    ) -> Result<LogicalPlan> {
        let schema = IQLSchema::new_from_fields(input.schema(), fields)?;

        // Transform InfluxQL AST field expressions to a list of DataFusion expressions.
        let mut select_exprs = self.field_list_to_exprs(&input, fields, &schema)?;

        // This is a special case, where exactly one column can be projected with a `DISTINCT`
        // clause or the `distinct` function.
        //
        // In addition, the time column is projected as the Unix epoch.

        let Some(time_column_index) = find_time_column_index(fields) else {
                return error::internal("unable to find time column")
            };

        // Take ownership of the alias, so we don't reallocate, and temporarily place a literal
        // `NULL` in its place.
        let Expr::Alias(Alias{name: alias, ..}) = std::mem::replace(&mut select_exprs[time_column_index], lit(ScalarValue::Null)) else {
                return error::internal("time column is not an alias")
            };

        select_exprs[time_column_index] = lit_timestamp_nano(0).alias(alias);

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
        let select_exprs = self.field_list_to_exprs(&input, fields, &schema)?;

        let (plan, select_exprs) =
            self.select_aggregate(ctx, input, fields, select_exprs, group_by_tag_set)?;

        // Wrap the plan in a `LogicalPlan::Projection` from the select expressions
        project(plan, select_exprs)
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
        let select_exprs = self.field_list_to_exprs(&input, fields, &schema)?;

        let (plan, select_exprs) =
            self.select_window(ctx, input, select_exprs, group_by_tag_set)?;

        // Wrap the plan in a `LogicalPlan::Projection` from the select expressions
        let plan = project(plan, select_exprs)?;

        // InfluxQL OG physical operators for

        // generate a predicate to filter rows where all field values of the row are `NULL`,
        // like:
        //
        //   NOT (field1 IS NULL AND field2 IS NULL AND ...)
        match conjunction(fields.iter().filter_map(|f| {
            if matches!(f.data_type, Some(InfluxColumnType::Field(_))) {
                Some(f.name.as_expr().is_null())
            } else {
                None
            }
        })) {
            Some(expr) => LogicalPlanBuilder::from(plan).filter(expr.not())?.build(),
            None => Ok(plan),
        }
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
        let select_exprs = self.field_list_to_exprs(&input, fields, &schema)?;

        let (plan, select_exprs) =
            self.select_aggregate(ctx, input, fields, select_exprs, group_by_tag_set)?;

        let (plan, select_exprs) = self.select_window(ctx, plan, select_exprs, group_by_tag_set)?;

        // Wrap the plan in a `LogicalPlan::Projection` from the select expressions
        let plan = project(plan, select_exprs)?;

        // InfluxQL OG physical operators for

        // generate a predicate to filter rows where all field values of the row are `NULL`,
        // like:
        //
        //   NOT (field1 IS NULL AND field2 IS NULL AND ...)
        match conjunction(fields.iter().filter_map(|f| {
            if matches!(f.data_type, Some(InfluxColumnType::Field(_))) {
                Some(f.name.as_expr().is_null())
            } else {
                None
            }
        })) {
            Some(expr) => LogicalPlanBuilder::from(plan).filter(expr.not())?.build(),
            None => Ok(plan),
        }
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
                let window_perc_row = Expr::WindowFunction(WindowFunction::new(
                    PERCENT_ROW_NUMBER.clone(),
                    vec![lit(n)],
                    window_partition_by(ctx, input.schema(), group_by_tag_set),
                    vec![field_key.as_expr().sort(true, false), ctx.time_sort_expr()],
                    WindowFrame {
                        units: WindowFrameUnits::Rows,
                        start_bound: WindowFrameBound::Preceding(ScalarValue::Null),
                        end_bound: WindowFrameBound::Following(ScalarValue::Null),
                    },
                ));
                let perc_row_column_name = window_perc_row.display_name()?;

                let window_row = Expr::WindowFunction(WindowFunction::new(
                    window_function::WindowFunction::BuiltInWindowFunction(
                        window_function::BuiltInWindowFunction::RowNumber,
                    ),
                    vec![],
                    window_partition_by(ctx, input.schema(), group_by_tag_set),
                    vec![field_key.as_expr().sort(true, false), ctx.time_sort_expr()],
                    WindowFrame {
                        units: WindowFrameUnits::Rows,
                        start_bound: WindowFrameBound::Preceding(ScalarValue::Null),
                        end_bound: WindowFrameBound::Following(ScalarValue::Null),
                    },
                ));
                let row_column_name = window_row.display_name()?;

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
                return error::not_implemented("sample selector function")
            }

            (_, s) => {
                return error::internal(format!(
                    "unsupported selector function for ProjectionSelector {s}"
                ))
            }
        };

        let mut fields_vec = fields.to_vec();
        fields_vec[selector_index].expr = IQLExpr::VarRef(VarRef {
            name: field_key.clone(),
            data_type: None,
        });

        // Transform InfluxQL AST field expressions to a list of DataFusion expressions.
        let select_exprs = self.field_list_to_exprs(&plan, fields_vec.as_slice(), &schema)?;

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
                    ))
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
        let select_exprs = self.field_list_to_exprs(&input, fields_vec.as_slice(), &schema)?;

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
        input: LogicalPlan,
        fields: &[Field],
        mut select_exprs: Vec<Expr>,
        group_by_tag_set: &[&str],
    ) -> Result<(LogicalPlan, Vec<Expr>)> {
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
        let mut aggr_exprs = find_aggregate_exprs(&select_exprs);

        // gather some time-related metadata
        let Some(time_column_index) = find_time_column_index(fields) else {
            return error::internal("unable to find time column")
        };

        // if there's only a single selector, wrap non-aggregated fields into that selector
        let mut should_fill_expr = fields.iter().map(is_aggregate_field).collect::<Vec<_>>();
        if aggr_exprs.len() == 1 {
            let selector = aggr_exprs[0].clone();

            if let Expr::AggregateUDF(mut udf) = selector.clone() {
                if udf.fun.name.starts_with("selector_") {
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
                                name: out_name,
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

                    udf.args.append(&mut additional_args);
                    let selector_new = Expr::AggregateUDF(udf);
                    select_exprs[selector_index] = select_exprs[selector_index]
                        .clone()
                        .transform_up(&|expr| {
                            if expr == selector {
                                Ok(Transformed::Yes(selector_new.clone()))
                            } else {
                                Ok(Transformed::No(expr))
                            }
                        })
                        .expect("cannot fail");
                    aggr_exprs[0] = selector_new.clone();

                    for (idx, struct_name, out_alias) in fields_to_extract {
                        select_exprs[idx] =
                            selector_new.clone().field(struct_name).alias(out_alias);
                        should_fill_expr[idx] = true;
                    }
                }
            }
        }

        // This block identifies the time column index and updates the time expression
        // based on the semantics of the projection.
        let time_column = {
            // Take ownership of the alias, so we don't reallocate, and temporarily place a literal
            // `NULL` in its place.
            let Expr::Alias(Alias{name: alias, ..}) = std::mem::replace(&mut select_exprs[time_column_index], lit(ScalarValue::Null)) else {
                return error::internal("time column is not an alias")
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

                date_bin(
                    stride,
                    "time".as_expr(),
                    lit(ScalarValue::TimestampNanosecond(Some(offset), None)),
                )
            } else if let ProjectionType::Selector { has_fields: _ } = ctx.projection_type {
                let selector = match aggr_exprs.len() {
                    1 => aggr_exprs[0].clone(),
                    len => {
                        // Should have been validated by `select_statement_info`
                        return error::internal(format!(
                            "internal: expected 1 selector expression, got {len}"
                        ));
                    }
                };

                selector.field("time")
            } else {
                lit_timestamp_nano(0)
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

        let plan = LogicalPlanBuilder::from(input)
            .aggregate(aggr_group_by_exprs.clone(), aggr_exprs.clone())?
            .build()?;

        let fill_option = ctx.fill();

        // Wrap the plan in a GapFill operator if the statement specifies a `GROUP BY TIME` clause and
        // the FILL option is one of
        //
        // * `null`
        // * `previous`
        // * `literal` value
        // * `linear`
        //
        let plan = if ctx.group_by.and_then(|gb| gb.time_dimension()).is_some()
            && fill_option != FillClause::None
        {
            let fill_strategy = match fill_option {
                FillClause::Null | FillClause::Value(_) => FillStrategy::Null,
                FillClause::Previous => FillStrategy::PrevNullAsMissing,
                FillClause::Linear => FillStrategy::LinearInterpolate,
                FillClause::None => unreachable!(),
            };

            build_gap_fill_node(plan, time_column, fill_strategy)?
        } else {
            plan
        };

        // Combine the aggregate columns and group by expressions, which represents
        // the final projection from the aggregate operator.
        let aggr_projection_exprs = [aggr_group_by_exprs, aggr_exprs].concat();

        // Create a literal expression for `value` if the strategy
        // is `FILL(<value>)`
        let fill_if_null = match fill_option {
            FillClause::Value(v) => Some(v),
            _ => None,
        };

        // Some aggregates, such as COUNT, should be filled with zero by default
        // rather than NULL.
        let should_zero_fill_expr = fields
            .iter()
            .map(is_zero_filled_aggregate_field)
            .collect::<Vec<_>>();

        // Rewrite the aggregate columns from the projection, so that the expressions
        // refer to the columns from the aggregate projection
        let select_exprs_post_aggr = select_exprs
            .iter()
            .zip(should_fill_expr.iter().zip(should_zero_fill_expr))
            .map(|(expr, (should_fill, should_zero_fill))| {
                // This implements the `FILL(<value>)` strategy, by coalescing any aggregate
                // expressions to `<value>` when they are `NULL`.
                let fill_if_null = match (fill_if_null, should_fill, should_zero_fill) {
                    (Some(_), true, _) => fill_if_null,
                    (None, true, true) => Some(0.into()),
                    _ => None,
                };

                rebase_expr(expr, &aggr_projection_exprs, &fill_if_null, &plan)
            })
            .collect::<Result<Vec<Expr>>>()?;

        Ok((plan, select_exprs_post_aggr))
    }

    /// Generate a plan for any window functions, such as `moving_average` or `difference`.
    fn select_window(
        &self,
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

        let plan = LogicalPlanBuilder::from(input)
            .window(window_func_exprs)?
            .build()?;

        // Rewrite the window columns from the projection, so that the expressions
        // refer to the columns from the window projection.
        let select_exprs = select_exprs
            .iter()
            .map(|expr| {
                expr.clone().transform_up(&|udf_expr| {
                    Ok(if udfs.contains(&udf_expr) {
                        Transformed::Yes(expr_as_column_expr(&udf_expr, &plan)?)
                    } else {
                        Transformed::No(udf_expr)
                    })
                })
            })
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

        let window_expr = Expr::WindowFunction(WindowFunction::new(
            window_function::WindowFunction::BuiltInWindowFunction(
                window_function::BuiltInWindowFunction::RowNumber,
            ),
            Vec::<Expr>::new(),
            window_partition_by(ctx, input.schema(), group_by_tags),
            order_by_exprs,
            WindowFrame {
                units: WindowFrameUnits::Rows,
                start_bound: WindowFrameBound::Preceding(ScalarValue::Null),
                end_bound: WindowFrameBound::CurrentRow,
            },
        ));
        let column_name = window_expr.display_name()?;
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
        order_by: Vec<Expr>,
    ) -> Result<Expr> {
        let alias = e
            .display_name()
            // display_name is known only to fail with Expr::Sort and Expr::QualifiedWildcard,
            // neither of which should be passed to udf_to_expr
            .map_err(|err| error::map::internal(format!("display_name: {err}")))?;

        let Expr::ScalarUDF(expr::ScalarUDF { fun, args }) = e else {
            return error::internal(format!("udf_to_expr: unexpected expression: {e}"))
        };

        fn derivative_unit(ctx: &Context<'_>, args: &Vec<Expr>) -> Result<ScalarValue> {
            if args.len() > 1 {
                if let Expr::Literal(v) = &args[1] {
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

        match udf::WindowFunction::try_from_scalar_udf(Arc::clone(&fun)) {
            Some(udf::WindowFunction::MovingAverage) => Ok(Expr::WindowFunction(WindowFunction {
                fun: MOVING_AVERAGE.clone(),
                args,
                partition_by,
                order_by,
                window_frame: WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start_bound: WindowFrameBound::Preceding(ScalarValue::Null),
                    end_bound: WindowFrameBound::Following(ScalarValue::Null),
                },
            })
            .alias(alias)),
            Some(udf::WindowFunction::Difference) => Ok(Expr::WindowFunction(WindowFunction {
                fun: DIFFERENCE.clone(),
                args,
                partition_by,
                order_by,
                window_frame: WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start_bound: WindowFrameBound::Preceding(ScalarValue::Null),
                    end_bound: WindowFrameBound::Following(ScalarValue::Null),
                },
            })
            .alias(alias)),
            Some(udf::WindowFunction::NonNegativeDifference) => {
                Ok(Expr::WindowFunction(WindowFunction {
                    fun: NON_NEGATIVE_DIFFERENCE.clone(),
                    args,
                    partition_by,
                    order_by,
                    window_frame: WindowFrame {
                        units: WindowFrameUnits::Rows,
                        start_bound: WindowFrameBound::Preceding(ScalarValue::Null),
                        end_bound: WindowFrameBound::Following(ScalarValue::Null),
                    },
                })
                .alias(alias))
            }
            Some(udf::WindowFunction::Derivative) => Ok(Expr::WindowFunction(WindowFunction {
                fun: DERIVATIVE.clone(),
                args: vec![
                    args[0].clone(),
                    lit(derivative_unit(ctx, &args)?),
                    "time".as_expr(),
                ],
                partition_by,
                order_by,
                window_frame: WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start_bound: WindowFrameBound::Preceding(ScalarValue::Null),
                    end_bound: WindowFrameBound::Following(ScalarValue::Null),
                },
            })
            .alias(alias)),
            Some(udf::WindowFunction::NonNegativeDerivative) => {
                Ok(Expr::WindowFunction(WindowFunction {
                    fun: NON_NEGATIVE_DERIVATIVE.clone(),
                    args: vec![
                        args[0].clone(),
                        lit(derivative_unit(ctx, &args)?),
                        "time".as_expr(),
                    ],
                    partition_by,
                    order_by,
                    window_frame: WindowFrame {
                        units: WindowFrameUnits::Rows,
                        start_bound: WindowFrameBound::Preceding(ScalarValue::Null),
                        end_bound: WindowFrameBound::Following(ScalarValue::Null),
                    },
                })
                .alias(alias))
            }
            Some(udf::WindowFunction::CumulativeSum) => Ok(Expr::WindowFunction(WindowFunction {
                fun: CUMULATIVE_SUM.clone(),
                args,
                partition_by,
                order_by,
                window_frame: WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start_bound: WindowFrameBound::Preceding(ScalarValue::Null),
                    end_bound: WindowFrameBound::Following(ScalarValue::Null),
                },
            })
            .alias(alias)),
            None => error::internal(format!(
                "unexpected user-defined window function: {}",
                fun.name
            )),
        }
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
    /// - `sort_by_measurement`: `true` if the `input` must be sorted by the measurement column.
    /// - `group_by_tag_set`: Tag columns from the `input` plan that should be used to partition
    ///   the `input` plan and sort the `output` plan.
    /// - `projection_tag_set`: Additional tag columns that should be used to sort the `output`
    ///   plan.
    #[allow(clippy::too_many_arguments)]
    fn limit(
        &self,
        input: LogicalPlan,
        offset: Option<OffsetClause>,
        limit: Option<LimitClause>,
        sort_exprs: Vec<Expr>,
        sort_by_measurement: bool,
        group_by_tag_set: &[&str],
        projection_tag_set: &[&str],
    ) -> Result<LogicalPlan> {
        if offset.is_none() && limit.is_none() {
            return Ok(input);
        }

        if group_by_tag_set.is_empty() && !sort_by_measurement {
            // If the query is not grouping by tags, and is a single measurement, the DataFusion
            // Limit operator is sufficient.
            let skip = offset.map_or(0, |v| *v as usize);
            let fetch = limit.map(|v| *v as usize);

            LogicalPlanBuilder::from(input).limit(skip, fetch)?.build()
        } else {
            // If the query includes a GROUP BY tag[, tag, ...], the LIMIT and OFFSET clauses
            // are applied to each unique group. To accomplish this, construct a plan which uses
            // the ROW_NUMBER windowing function.

            // The name of the ROW_NUMBER window expression
            const IOX_ROW_ALIAS: &str = "iox::row";

            // Construct a ROW_NUMBER window expression:
            //
            // ROW_NUMBER() OVER (
            //   PARTITION BY [iox::measurement, group_by_tag_set]
            //   ORDER BY time [ASC | DESC]
            //   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            // ) AS iox::row
            let order_by = sort_exprs.clone();

            let partition_by = if sort_by_measurement {
                iter::once(INFLUXQL_MEASUREMENT_COLUMN_NAME.as_expr())
                    .chain(fields_to_exprs_no_nulls(input.schema(), group_by_tag_set))
                    .collect::<Vec<_>>()
            } else {
                fields_to_exprs_no_nulls(input.schema(), group_by_tag_set).collect::<Vec<_>>()
            };

            let window_func_exprs = vec![Expr::WindowFunction(WindowFunction {
                fun: window_function::WindowFunction::BuiltInWindowFunction(
                    BuiltInWindowFunction::RowNumber,
                ),
                args: vec![],
                partition_by,
                order_by,
                window_frame: WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start_bound: WindowFrameBound::Preceding(ScalarValue::Null),
                    end_bound: WindowFrameBound::CurrentRow,
                },
            })
            .alias(IOX_ROW_ALIAS)];

            // Prepare new projection.
            let proj_exprs = input
                .schema()
                .fields()
                .iter()
                .map(|expr| Expr::Column(expr.unqualified_column()))
                .collect::<Vec<_>>();

            let plan = LogicalPlanBuilder::from(input)
                .window(window_func_exprs)?
                .build()?;

            let limit = limit
                .map(|v| <u64 as TryInto<i64>>::try_into(*v))
                .transpose()
                .map_err(|_| error::map::query("limit out of range"))?;
            let offset = offset
                .map(|v| <u64 as TryInto<i64>>::try_into(*v))
                .transpose()
                .map_err(|_| error::map::query("offset out of range".to_owned()))?;

            // a reference to the ROW_NUMBER column.
            let row_alias = IOX_ROW_ALIAS.as_expr();

            let row_filter_expr = match (limit, offset) {
                // WHERE "iox::row" BETWEEN OFFSET + 1 AND OFFSET + LIMIT
                (Some(limit), Some(offset)) => {
                    let low = offset + 1;
                    let high = offset + limit;

                    Expr::Between(Between {
                        expr: Box::new(row_alias),
                        negated: false,
                        low: Box::new(lit(low)),
                        high: Box::new(lit(high)),
                    })
                }

                // WHERE "iox::row" <= LIMIT
                (Some(limit), None) => row_alias.lt_eq(lit(limit)),

                // WHERE "iox::row" > OFFSET
                (None, Some(offset)) => row_alias.gt(lit(offset)),
                (None, None) => unreachable!("limit and offset cannot not be None"),
            };

            let plan = LogicalPlanBuilder::from(plan)
                // Filter by the LIMIT and OFFSET clause
                .filter(row_filter_expr)?
                // Project the output without the IOX_ROW_ALIAS column
                .project(proj_exprs)?
                .build()?;

            // For consistency with InfluxQL, the final results must be sorted by
            // the tag set from the GROUP BY
            plan_with_sort(
                plan,
                sort_exprs,
                sort_by_measurement,
                group_by_tag_set,
                projection_tag_set,
            )
        }
    }

    /// Map the InfluxQL `SELECT` projection list into a list of DataFusion expressions.
    fn field_list_to_exprs(
        &self,
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
            .map(|field| self.field_to_df_expr(&field, plan, schema))
            .collect()
    }

    /// Map an InfluxQL [`Field`] to a DataFusion [`Expr`].
    ///
    /// A [`Field`] is analogous to a column in a SQL `SELECT` projection.
    fn field_to_df_expr(
        &self,
        field: &Field,
        plan: &LogicalPlan,
        schema: &IQLSchema<'_>,
    ) -> Result<Expr> {
        let expr = self.expr_to_df_expr(ExprScope::Projection, &field.expr, schema)?;
        let expr = planner_rewrite_expression::rewrite_field_expr(expr, schema)?;
        normalize_col(expr.alias(&field.name), plan)
    }

    /// Map an InfluxQL [`ConditionalExpression`] to a DataFusion [`Expr`].
    fn conditional_to_df_expr(
        &self,
        iql: &ConditionalExpression,
        schema: &IQLSchema<'_>,
    ) -> Result<Expr> {
        match iql {
            ConditionalExpression::Expr(expr) => {
                self.expr_to_df_expr(ExprScope::Where, expr, schema)
            }
            ConditionalExpression::Binary(expr) => self.binary_conditional_to_df_expr(expr, schema),
            ConditionalExpression::Grouped(e) => self.conditional_to_df_expr(e, schema),
        }
    }

    /// Map an InfluxQL binary conditional expression to a DataFusion [`Expr`].
    fn binary_conditional_to_df_expr(
        &self,
        expr: &ConditionalBinary,
        schema: &IQLSchema<'_>,
    ) -> Result<Expr> {
        let ConditionalBinary { lhs, op, rhs } = expr;

        Ok(binary_expr(
            self.conditional_to_df_expr(lhs, schema)?,
            conditional_op_to_operator(*op)?,
            self.conditional_to_df_expr(rhs, schema)?,
        ))
    }

    /// Map an InfluxQL [`IQLExpr`] to a DataFusion [`Expr`].
    fn expr_to_df_expr(
        &self,
        scope: ExprScope,
        iql: &IQLExpr,
        schema: &IQLSchema<'_>,
    ) -> Result<Expr> {
        let df_schema = &schema.df_schema;
        match iql {
            // rewriter is expected to expand wildcard expressions
            IQLExpr::Wildcard(_) => error::internal("unexpected wildcard in projection"),
            IQLExpr::VarRef(VarRef {
                name,
                data_type: opt_dst_type,
            }) => {
                Ok(match (scope, name.as_str()) {
                    // Per the Go implementation, the time column is case-insensitive in the
                    // `WHERE` clause and disregards any postfix type cast operator.
                    //
                    // See: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L5751-L5753
                    (ExprScope::Where, name) if name.eq_ignore_ascii_case("time") => {
                        "time".as_expr()
                    }
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
                                        matches!(
                                            dt,
                                            DataType::Int64 | DataType::Float64 | DataType::UInt64
                                        )
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
                                        Expr::Literal(ScalarValue::Null)
                                    }
                                }
                                None => column,
                            }
                        }
                        _ => Expr::Literal(ScalarValue::Null),
                    },
                })
            }
            IQLExpr::BindParameter(_) => error::not_implemented("parameter"),
            IQLExpr::Literal(val) => match val {
                Literal::Integer(v) => Ok(lit(*v)),
                Literal::Unsigned(v) => Ok(lit(*v)),
                Literal::Float(v) => Ok(lit(*v)),
                Literal::String(v) => Ok(lit(v)),
                Literal::Boolean(v) => Ok(lit(*v)),
                Literal::Timestamp(v) => Ok(lit(ScalarValue::TimestampNanosecond(
                    Some(v.timestamp_nanos()),
                    None,
                ))),
                Literal::Duration(v) => {
                    Ok(lit(ScalarValue::IntervalMonthDayNano(Some((**v).into()))))
                }
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
            IQLExpr::Call(call) => self.call_to_df_expr(scope, call, schema),
            IQLExpr::Binary(expr) => self.arithmetic_expr_to_df_expr(scope, expr, schema),
            IQLExpr::Nested(e) => self.expr_to_df_expr(scope, e, schema),
        }
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
        scope: ExprScope,
        call: &Call,
        schema: &IQLSchema<'_>,
    ) -> Result<Expr> {
        if is_scalar_math_function(call.name.as_str()) {
            return self.scalar_math_func_to_df_expr(scope, call, schema);
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
            ExprScope::Projection => self.function_to_df_expr(scope, call, schema),
        }
    }

    fn function_to_df_expr(
        &self,
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

        let Call { name, args } = call;

        match name.as_str() {
            // The DISTINCT function is handled as a `ProjectionType::RawDistinct`
            // query, so the planner only needs to project the single column
            // argument.
            "distinct" => self.expr_to_df_expr(scope, &args[0], schema),
            "count" => {
                let (expr, distinct) = match &args[0] {
                    IQLExpr::Call(c) if c.name == "distinct" => {
                        (self.expr_to_df_expr(scope, &c.args[0], schema)?, true)
                    }
                    expr => (self.expr_to_df_expr(scope, expr, schema)?, false),
                };
                if let Expr::Literal(ScalarValue::Null) = expr {
                    return Ok(expr);
                }

                check_arg_count("count", args, 1)?;
                Ok(Expr::AggregateFunction(expr::AggregateFunction::new(
                    AggregateFunction::Count,
                    vec![expr],
                    distinct,
                    None,
                    None,
                )))
            }
            "sum" | "stddev" | "mean" | "median" => {
                let expr = self.expr_to_df_expr(scope, &args[0], schema)?;
                if let Expr::Literal(ScalarValue::Null) = expr {
                    return Ok(expr);
                }

                check_arg_count(name, args, 1)?;
                Ok(Expr::AggregateFunction(expr::AggregateFunction::new(
                    AggregateFunction::from_str(name)?,
                    vec![expr],
                    false,
                    None,
                    None,
                )))
            }
            "percentile" => {
                let expr = self.expr_to_df_expr(scope, &args[0], schema)?;
                if let Expr::Literal(ScalarValue::Null) = expr {
                    return Ok(expr);
                }

                check_arg_count(name, args, 2)?;
                let nexpr = self.expr_to_df_expr(scope, &args[1], schema)?;
                Ok(Expr::AggregateUDF(expr::AggregateUDF::new(
                    PERCENTILE.clone(),
                    vec![expr, nexpr],
                    None,
                    None,
                )))
            }
            name @ ("first" | "last" | "min" | "max") => {
                let expr = self.expr_to_df_expr(scope, &args[0], schema)?;
                if let Expr::Literal(ScalarValue::Null) = expr {
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
                let arg0 = self.expr_to_df_expr(scope, &args[0], schema)?;
                if let Expr::Literal(ScalarValue::Null) = arg0 {
                    return Ok(arg0);
                }

                Ok(difference(vec![arg0]))
            }
            "non_negative_difference" => {
                check_arg_count(name, args, 1)?;

                // arg0 should be a column or function
                let arg0 = self.expr_to_df_expr(scope, &args[0], schema)?;
                if let Expr::Literal(ScalarValue::Null) = arg0 {
                    return Ok(arg0);
                }

                Ok(non_negative_difference(vec![arg0]))
            }
            "moving_average" => {
                check_arg_count(name, args, 2)?;

                // arg0 should be a column or function
                let arg0 = self.expr_to_df_expr(scope, &args[0], schema)?;
                if let Expr::Literal(ScalarValue::Null) = arg0 {
                    return Ok(arg0);
                }

                // arg1 should be an integer.
                let arg1 = ScalarValue::Int64(Some(
                    match self.expr_to_df_expr(scope, &args[1], schema)? {
                        Expr::Literal(ScalarValue::Int64(Some(v))) => v,
                        Expr::Literal(ScalarValue::UInt64(Some(v))) => v as i64,
                        _ => {
                            return error::query(
                                "moving_average expects number for second argument",
                            )
                        }
                    },
                ));

                Ok(moving_average(vec![arg0, lit(arg1)]))
            }
            "derivative" => {
                check_arg_count_range(name, args, 1, 2)?;

                // arg0 should be a column or function
                let arg0 = self.expr_to_df_expr(scope, &args[0], schema)?;
                if let Expr::Literal(ScalarValue::Null) = arg0 {
                    return Ok(arg0);
                }
                let mut eargs = vec![arg0];
                if args.len() > 1 {
                    let arg1 = self.expr_to_df_expr(scope, &args[1], schema)?;
                    eargs.push(arg1);
                }

                Ok(derivative(eargs))
            }
            "non_negative_derivative" => {
                check_arg_count_range(name, args, 1, 2)?;

                // arg0 should be a column or function
                let arg0 = self.expr_to_df_expr(scope, &args[0], schema)?;
                if let Expr::Literal(ScalarValue::Null) = arg0 {
                    return Ok(arg0);
                }
                let mut eargs = vec![arg0];
                if args.len() > 1 {
                    let arg1 = self.expr_to_df_expr(scope, &args[1], schema)?;
                    eargs.push(arg1);
                }

                Ok(non_negative_derivative(eargs))
            }
            "cumulative_sum" => {
                check_arg_count(name, args, 1)?;

                // arg0 should be a column or function
                let arg0 = self.expr_to_df_expr(scope, &args[0], schema)?;
                if let Expr::Literal(ScalarValue::Null) = arg0 {
                    return Ok(arg0);
                }

                Ok(cumulative_sum(vec![arg0]))
            }
            // The TOP/BOTTOM function is handled as a `ProjectionType::TopBottomSelector`
            // query, so the planner only needs to project the single column
            // argument.
            "top" | "bottom" => self.expr_to_df_expr(scope, &args[0], schema),

            _ => error::query(format!("Invalid function '{name}'")),
        }
    }

    /// Map the InfluxQL scalar function call to a DataFusion scalar function expression.
    fn scalar_math_func_to_df_expr(
        &self,
        scope: ExprScope,
        call: &Call,
        schema: &IQLSchema<'a>,
    ) -> Result<Expr> {
        let args = call
            .args
            .iter()
            .map(|e| self.expr_to_df_expr(scope, e, schema))
            .collect::<Result<Vec<Expr>>>()?;

        match BuiltinScalarFunction::from_str(call.name.as_str())? {
            BuiltinScalarFunction::Log => {
                if args.len() != 2 {
                    error::query("invalid number of arguments for log, expected 2, got 1")
                } else {
                    Ok(Expr::ScalarFunction(ScalarFunction {
                        fun: BuiltinScalarFunction::Log,
                        args: args.into_iter().rev().collect(),
                    }))
                }
            }
            fun => Ok(Expr::ScalarFunction(ScalarFunction { fun, args })),
        }
    }

    /// Map an InfluxQL arithmetic expression to a DataFusion [`Expr`].
    fn arithmetic_expr_to_df_expr(
        &self,
        scope: ExprScope,
        expr: &Binary,
        schema: &IQLSchema<'_>,
    ) -> Result<Expr> {
        Ok(binary_expr(
            self.expr_to_df_expr(scope, &expr.lhs, schema)?,
            binary_operator_to_df_operator(expr.op),
            self.expr_to_df_expr(scope, &expr.rhs, schema)?,
        ))
    }

    fn plan_condition_time_range(
        &self,
        condition: Option<&ConditionalExpression>,
        time_range: TimeRange,
        plan: LogicalPlan,
        schema: &IQLSchema<'a>,
    ) -> Result<LogicalPlan> {
        let filter_expr = condition
            .map(|condition| {
                let filter_expr = self.conditional_to_df_expr(condition, schema)?;
                planner_rewrite_expression::rewrite_conditional_expr(
                    self.s.execution_props(),
                    filter_expr,
                    schema,
                )
            })
            .transpose()?;

        let time_expr = time_range_to_df_expr(time_range);

        let pb = LogicalPlanBuilder::from(plan);
        match (time_expr, filter_expr) {
            (Some(lhs), Some(rhs)) => pb.filter(lhs.and(rhs))?,
            (Some(expr), None) | (None, Some(expr)) => pb.filter(expr)?,
            (None, None) => pb,
        }
        .build()
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
                    MetadataCutoff::Absolute(dt) => dt.timestamp_nanos(),
                    MetadataCutoff::Relative(delta) => {
                        start_time.timestamp_nanos() - delta.as_nanos() as i64
                    }
                }),
                upper: None,
            }
        } else {
            time_range
        };

        self.plan_condition_time_range(cond.as_ref(), time_range, plan, schema)
    }

    /// Generate a logical plan for the specified `DataSource`.
    fn plan_from_data_source(
        &self,
        ctx: &Context<'_>,
        ds: &DataSource,
    ) -> Result<Option<LogicalPlan>> {
        match ds {
            DataSource::Table(table_name) if table_name == ctx.table_name => {
                // `rewrite_statement` guarantees the table should exist
                let source = self.s.get_table_provider(table_name)?;
                let table_ref = TableReference::bare(table_name.to_owned());
                Ok(Some(
                    LogicalPlanBuilder::scan(table_ref, source, None)?.build()?,
                ))
            }
            DataSource::Table(_) => Ok(None),
            DataSource::Subquery(select) => Ok(self.subquery_to_plan(ctx, select)?),
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
                for qualified_name in from.iter() {
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

        let plan = match show_tag_keys.condition {
            Some(condition) => {
                debug!("`SHOW TAG KEYS` w/ WHERE-clause, use data scan plan",);

                let condition = Some(condition);
                let metadata_cutoff = self.metadata_cutoff();

                let mut union_plan = None;
                for table in tables {
                    let Some(table_schema) = self.s.table_schema(&table) else {continue};
                    let Some((plan, measurement_expr)) = self.create_table_ref(&table)? else {continue;};

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
                        .project([Expr::ScalarFunction(ScalarFunction {
                            fun: BuiltinScalarFunction::MakeArray,
                            args: tags
                                .iter()
                                .map(|tag| {
                                    let tag_col = Expr::Column(Column::from_name(*tag));

                                    when(tag_col.gt(lit(0)), lit(*tag)).end()
                                })
                                .collect::<Result<Vec<_>, _>>()?,
                        })
                        .alias(tag_key_col)])?
                        // roll our single array row into one row per tag key
                        .unnest_column(tag_key_df_col)?
                        // filter out tags that had no none-null values
                        .filter(tag_key_col_expr.clone().is_not_null())?
                        // build proper output
                        .project(measurement_expr.into_iter().chain([tag_key_col_expr]))?
                        .build()?;

                    union_plan = match union_plan {
                        Some(union_plan) => {
                            Some(LogicalPlanBuilder::from(union_plan).union(plan)?.build()?)
                        }
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

                LogicalPlanBuilder::from(plan)
                    .sort([
                        Expr::Column(Column::new_unqualified(INFLUXQL_MEASUREMENT_COLUMN_NAME))
                            .sort(true, false),
                        Expr::Column(Column::new_unqualified(tag_key_col)).sort(true, false),
                    ])?
                    .build()?
            }
            None => {
                debug!("`SHOW TAG KEYS` w/o WHERE-clause, use cheap metadata scan",);

                let mut measurement_names_builder = StringDictionaryBuilder::<Int32Type>::new();
                let mut tag_key_builder = StringDictionaryBuilder::<Int32Type>::new();
                for table in tables {
                    let Some(table_schema) = self.s.table_schema(&table) else {continue};
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
                .build()?
            }
        };

        let plan = plan_with_metadata(
            plan,
            &InfluxQlMetadata {
                measurement_column_index: MEASUREMENT_COLUMN_INDEX,
                tag_key_columns: vec![],
            },
        )?;

        let plan = self.limit(
            plan,
            show_tag_keys.offset,
            show_tag_keys.limit,
            vec![Expr::Column(Column::new_unqualified(tag_key_col)).sort(true, false)],
            true,
            &[],
            &[],
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
            let Some(table_schema) = self.s.table_schema(&table) else {continue};
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
        let plan = LogicalPlanBuilder::scan(
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
        )?
        .build()?;
        let plan = plan_with_metadata(
            plan,
            &InfluxQlMetadata {
                measurement_column_index: MEASUREMENT_COLUMN_INDEX,
                tag_key_columns: vec![],
            },
        )?;
        let plan = self.limit(
            plan,
            show_field_keys.offset,
            show_field_keys.limit,
            vec![Expr::Column(Column::new_unqualified(field_key_col)).sort(true, false)],
            true,
            &[],
            &[],
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
            let Some(schema) = self.s.table_schema(&table) else {continue;};

            let keys = eval_with_key_clause(
                schema.tags_iter().map(|field| field.name().as_str()),
                &show_tag_values.with_key,
            )?;
            if keys.is_empty() {
                // don't bother to create a plan for this table
                continue;
            }

            let Some((plan, measurement_expr)) = self.create_table_ref(&table)? else {continue;};

            let ds = DataSource::Table(table.clone());
            let schema = IQLSchema::new_from_ds_schema(plan.schema(), ds.schema(self.s)?)?;
            let plan =
                self.plan_where_clause(plan, &show_tag_values.condition, metadata_cutoff, &schema)?;

            for key in keys {
                let idx = plan
                    .schema()
                    .index_of_column_by_name(None, key)?
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
                    Some(union_plan) => {
                        Some(LogicalPlanBuilder::from(union_plan).union(plan)?.build()?)
                    }
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
        let plan = LogicalPlanBuilder::from(plan)
            .sort([
                Expr::Column(Column::new_unqualified(INFLUXQL_MEASUREMENT_COLUMN_NAME))
                    .sort(true, false),
                Expr::Column(Column::new_unqualified(key_col)).sort(true, false),
                Expr::Column(Column::new_unqualified(value_col)).sort(true, false),
            ])?
            .build()?;
        let plan = plan_with_metadata(
            plan,
            &InfluxQlMetadata {
                measurement_column_index: MEASUREMENT_COLUMN_INDEX,
                tag_key_columns: vec![],
            },
        )?;
        let plan = self.limit(
            plan,
            show_tag_values.offset,
            show_tag_values.limit,
            vec![
                Expr::Column(Column::new_unqualified(key_col)).sort(true, false),
                Expr::Column(Column::new_unqualified(value_col)).sort(true, false),
            ],
            true,
            &[],
            &[],
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

        let plan = match show_measurements.condition {
            Some(condition) => {
                debug!("`SHOW MEASUREMENTS` w/ WHERE-clause, use data scan plan",);

                let condition = Some(condition);
                let metadata_cutoff = self.metadata_cutoff();

                let mut union_plan = None;
                for table in tables {
                    let Some((plan, _measurement_expr)) = self.create_table_ref(&table)? else {continue;};

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
                        Some(union_plan) => {
                            Some(LogicalPlanBuilder::from(union_plan).union(plan)?.build()?)
                        }
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
                LogicalPlanBuilder::from(plan)
                    .sort([
                        Expr::Column(Column::new_unqualified(INFLUXQL_MEASUREMENT_COLUMN_NAME))
                            .sort(true, false),
                        Expr::Column(Column::new_unqualified(name_col)).sort(true, false),
                    ])?
                    .build()?
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
                .build()?
            }
        };

        let plan = plan_with_metadata(
            plan,
            &InfluxQlMetadata {
                measurement_column_index: MEASUREMENT_COLUMN_INDEX,
                tag_key_columns: vec![],
            },
        )?;
        let plan = self.limit(
            plan,
            show_measurements.offset,
            show_measurements.limit,
            vec![Expr::Column(Column::new_unqualified(name_col)).sort(true, false)],
            true,
            &[],
            &[],
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
/// * `input` - An aggregate plan which requires gap-filling.
/// * `time_column` - The `date_bin` expression.
/// * `fill_strategy` - The strategy used to fill gaps in the data.
fn build_gap_fill_node(
    input: LogicalPlan,
    time_column: &Expr,
    fill_strategy: FillStrategy,
) -> Result<LogicalPlan> {
    let (expr, alias) = match time_column {
        Expr::Alias(Alias { expr, name: alias }) => (expr.as_ref(), alias),
        _ => return error::internal("expected time column to have an alias function"),
    };

    let date_bin_args = match expr {
        Expr::ScalarFunction(ScalarFunction {
            fun: BuiltinScalarFunction::DateBin,
            args,
        }) => args,
        _ => {
            // The InfluxQL planner adds the `date_bin` function,
            // so this condition represents an internal failure.
            return error::internal("expected DATE_BIN function");
        }
    };

    // Extract the gap-fill parameters from the arguments to the `DATE_BIN` function.
    // Any unexpected conditions represents an internal error, as the `DATE_BIN` function is
    // added by the planner.
    let (stride, time_range, origin) = match date_bin_args.len() {
        nargs @ 2..=3 => {
            let time_col = date_bin_args[1].try_into_col().map_err(|_| {
                error::map::internal("DATE_BIN requires a column as the source argument")
            })?;

            // Ensure that a time range was specified and is valid for gap filling
            let time_range = {
                // TODO(sgc): Fix via https://github.com/influxdata/influxdb_iox/issues/7829

                // This is a stop gap, until #7929 is fixed, as `find_time_range` does not look
                // beyond Union operators. We are working around the limitation by traversing the
                // tree until we find the first operator which specifies a filter predicate.
                let mut time_range: Option<Range<Bound<Expr>>> = None;
                _ = input.apply(&mut |n| match n {
                    plan @ (LogicalPlan::Filter(_) | LogicalPlan::TableScan(_)) => {
                        time_range = Some(match find_time_range(plan, &time_col)? {
                            // Follow the InfluxQL behaviour to use an upper bound of `now` when
                            // not found:
                            //
                            // See: https://github.com/influxdata/influxdb/blob/98361e207349a3643bcc332d54b009818fe7585f/query/compile.go#L172-L176
                            Range {
                                start,
                                end: Bound::Unbounded,
                            } => Range {
                                start,
                                end: Bound::Excluded(now()),
                            },
                            time_range => time_range,
                        });
                        Ok(VisitRecursion::Stop)
                    }
                    _ => Ok(VisitRecursion::Continue),
                });
                time_range
                    .ok_or_else(|| error::map::internal("expected to find a Filter or TableScan"))
            }?;

            let origin = (nargs == 3).then_some(date_bin_args[2].clone());

            (date_bin_args[0].clone(), time_range, origin)
        }
        nargs => {
            // This is an internal error as the date_bin function is added by the planner and should
            // always contain the correct number of arguments.
            return error::internal(format!("DATE_BIN expects 2 or 3 arguments, got {nargs}"));
        }
    };

    let LogicalPlan::Aggregate(aggr) = &input else {
        return Err(DataFusionError::Internal(format!("Expected Aggregate plan, got {}", input.display())));
    };
    let mut new_group_expr: Vec<_> = aggr
        .schema
        .fields()
        .iter()
        .map(|f| Expr::Column(f.qualified_column()))
        .collect();
    let aggr_expr = new_group_expr.split_off(aggr.group_expr.len());

    // The fill strategy for InfluxQL is specified at the query level
    let fill_strategy = aggr_expr
        .iter()
        .cloned()
        .map(|e| (e, fill_strategy.clone()))
        .collect();

    let time_column = col(input
        .schema()
        .field_with_unqualified_name(alias)
        .map(|f| f.qualified_column())?);

    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(GapFill::try_new(
            Arc::new(input),
            new_group_expr,
            aggr_expr,
            GapFillParams {
                stride,
                time_column,
                origin,
                time_range,
                fill_strategy,
            },
        )?),
    }))
}

/// Adds [`InfluxQlMetadata`] to the `plan`.
fn plan_with_metadata(plan: LogicalPlan, metadata: &InfluxQlMetadata) -> Result<LogicalPlan> {
    fn make_schema(schema: DFSchemaRef, metadata: &InfluxQlMetadata) -> Result<DFSchemaRef> {
        let data = serde_json::to_string(metadata).map_err(|err| {
            error::map::internal(format!("error serializing InfluxQL metadata: {err}"))
        })?;

        let mut md = schema.metadata().clone();
        md.insert(INFLUXQL_METADATA_KEY.to_owned(), data);

        Ok(Arc::new(DFSchema::new_with_metadata(
            schema.fields().clone(),
            md,
        )?))
    }

    // Reconstruct the plan, altering the first node which defines the output schema
    fn set_schema(input: &LogicalPlan, metadata: &InfluxQlMetadata) -> Result<LogicalPlan> {
        Ok(match input {
            LogicalPlan::Projection(src) => {
                let mut v = src.clone();
                v.schema = make_schema(Arc::clone(&src.schema), metadata)?;
                LogicalPlan::Projection(v)
            }
            LogicalPlan::Filter(src) => {
                let mut v = src.clone();
                v.input = Arc::new(set_schema(&src.input, metadata)?);
                LogicalPlan::Filter(v)
            }
            LogicalPlan::Window(src) => {
                let mut v = src.clone();
                v.schema = make_schema(Arc::clone(&src.schema), metadata)?;
                LogicalPlan::Window(v)
            }
            LogicalPlan::Aggregate(src) => {
                let mut v = src.clone();
                v.schema = make_schema(Arc::clone(&src.schema), metadata)?;
                LogicalPlan::Aggregate(v)
            }
            LogicalPlan::Sort(src) => {
                let mut v = src.clone();
                v.input = Arc::new(set_schema(&src.input, metadata)?);
                LogicalPlan::Sort(v)
            }
            LogicalPlan::Join(src) => {
                let mut v = src.clone();
                v.schema = make_schema(Arc::clone(&src.schema), metadata)?;
                LogicalPlan::Join(v)
            }
            LogicalPlan::CrossJoin(src) => {
                let mut v = src.clone();
                v.schema = make_schema(Arc::clone(&src.schema), metadata)?;
                LogicalPlan::CrossJoin(v)
            }
            LogicalPlan::Repartition(src) => {
                let mut v = src.clone();
                v.input = Arc::new(set_schema(&src.input, metadata)?);
                LogicalPlan::Repartition(v)
            }
            LogicalPlan::Union(src) => {
                let mut v = src.clone();
                v.schema = make_schema(Arc::clone(&src.schema), metadata)?;
                LogicalPlan::Union(v)
            }
            LogicalPlan::EmptyRelation(src) => {
                let mut v = src.clone();
                v.schema = make_schema(Arc::clone(&src.schema), metadata)?;
                LogicalPlan::EmptyRelation(v)
            }
            LogicalPlan::SubqueryAlias(src) => {
                let mut v = src.clone();
                v.schema = make_schema(Arc::clone(&src.schema), metadata)?;
                LogicalPlan::SubqueryAlias(v)
            }
            LogicalPlan::Limit(src) => {
                let mut v = src.clone();
                v.input = Arc::new(set_schema(&src.input, metadata)?);
                LogicalPlan::Limit(v)
            }
            LogicalPlan::Values(src) => {
                let mut v = src.clone();
                v.schema = make_schema(Arc::clone(&src.schema), metadata)?;
                LogicalPlan::Values(v)
            }
            LogicalPlan::Explain(src) => {
                let mut v = src.clone();
                v.schema = make_schema(Arc::clone(&src.schema), metadata)?;
                LogicalPlan::Explain(v)
            }
            LogicalPlan::Analyze(src) => {
                let mut v = src.clone();
                v.schema = make_schema(Arc::clone(&src.schema), metadata)?;
                LogicalPlan::Analyze(v)
            }
            LogicalPlan::Distinct(src) => {
                let mut v = src.clone();
                v.input = Arc::new(set_schema(&src.input, metadata)?);
                LogicalPlan::Distinct(v)
            }
            LogicalPlan::Unnest(src) => {
                let mut v = src.clone();
                v.schema = make_schema(Arc::clone(&src.schema), metadata)?;
                LogicalPlan::Unnest(v)
            }
            LogicalPlan::TableScan(src) => {
                let mut t = src.clone();
                t.projected_schema = make_schema(Arc::clone(&src.projected_schema), metadata)?;
                LogicalPlan::TableScan(t)
            }
            _ => return error::internal(format!("unexpected LogicalPlan: {}", input.display())),
        })
    }

    set_schema(&plan, metadata)
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

/// A utility function that checks whether `f` is an aggregate field
/// that should be filled with a 0 rather than an NULL.
fn is_zero_filled_aggregate_field(f: &Field) -> bool {
    walk_expr(&f.expr, &mut |e| match e {
        IQLExpr::Call(Call { name, .. }) if name == "count" => ControlFlow::Break(()),
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
        walk_expr(&f.expr, &mut |e| {
            if let IQLExpr::VarRef(vr) = e {
                var_refs.insert(vr);
            }
            ControlFlow::<()>::Continue(())
        });
    }

    if let Some(condition) = &select.condition {
        walk_expression(condition, &mut |e| match e {
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

        parition_by.push(date_bin(
            stride,
            "time".as_expr(),
            lit(ScalarValue::TimestampNanosecond(Some(offset), None)),
        ));
    }
    parition_by
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::plan::test_utils::{parse_select, MockSchemaProvider};
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

        let iox_ctx = IOxSessionContext::with_testing();
        let planner = InfluxQLToLogicalPlan::new(&sp, &iox_ctx);

        planner.statement_to_plan(statements.pop().unwrap())
    }

    fn metadata(sql: &str) -> Option<InfluxQlMetadata> {
        logical_plan(sql)
            .unwrap()
            .schema()
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

        let got = find_var_refs(&sp, "SELECT non_existent, usage_idle FROM (SELECT cpu as non_existent, usage_idle FROM cpu) GROUP BY cpu");
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
        assert_snapshot!(plan("CREATE DATABASE foo"), @"This feature is not implemented: CREATE DATABASE");
        assert_snapshot!(plan("DELETE FROM foo"), @"This feature is not implemented: DELETE");
        assert_snapshot!(plan("DROP MEASUREMENT foo"), @"This feature is not implemented: DROP MEASUREMENT");
        assert_snapshot!(plan("SHOW DATABASES"), @"This feature is not implemented: SHOW DATABASES");
    }

    mod metadata_queries {
        use super::*;

        #[test]
        fn test_show_field_keys() {
            assert_snapshot!(plan("SHOW FIELD KEYS"), @"TableScan: field_keys [iox::measurement:Utf8, fieldKey:Utf8, fieldType:Utf8]");
            assert_snapshot!(plan("SHOW FIELD KEYS LIMIT 1 OFFSET 2"), @r###"
            Sort: field_keys.iox::measurement ASC NULLS LAST, field_keys.fieldKey ASC NULLS LAST [iox::measurement:Utf8, fieldKey:Utf8, fieldType:Utf8]
              Projection: field_keys.iox::measurement, field_keys.fieldKey, field_keys.fieldType [iox::measurement:Utf8, fieldKey:Utf8, fieldType:Utf8]
                Filter: iox::row BETWEEN Int64(3) AND Int64(3) [iox::measurement:Utf8, fieldKey:Utf8, fieldType:Utf8, iox::row:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [field_keys.iox::measurement] ORDER BY [field_keys.fieldKey ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS iox::row]] [iox::measurement:Utf8, fieldKey:Utf8, fieldType:Utf8, iox::row:UInt64;N]
                    TableScan: field_keys [iox::measurement:Utf8, fieldKey:Utf8, fieldType:Utf8]
            "###);
        }

        #[test]
        fn test_snow_measurements() {
            assert_snapshot!(plan("SHOW MEASUREMENTS"), @"TableScan: measurements [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]");
            assert_snapshot!(plan("SHOW MEASUREMENTS LIMIT 1 OFFSET 2"), @r###"
            Sort: measurements.iox::measurement ASC NULLS LAST, measurements.name ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
              Projection: measurements.iox::measurement, measurements.name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                Filter: iox::row BETWEEN Int64(3) AND Int64(3) [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8), iox::row:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [measurements.iox::measurement] ORDER BY [measurements.name ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS iox::row]] [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8), iox::row:UInt64;N]
                    TableScan: measurements [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
            "###);
            assert_snapshot!(plan("SHOW MEASUREMENTS WHERE foo = 'some_foo'"), @r###"
            Sort: iox::measurement ASC NULLS LAST, name ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
              Union [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("all_types")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
                    Filter: all_types.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
                      TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("cpu")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                    Filter: cpu.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("data")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                    Filter: data.time >= TimestampNanosecond(1672444800000000000, None) AND data.foo = Dictionary(Int32, Utf8("some_foo")) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                      TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("disk")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                    Filter: disk.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                      TableScan: disk [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("diskio")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [bytes_read:Int64;N, bytes_written:Int64;N, host:Dictionary(Int32, Utf8);N, is_local:Boolean;N, read_utilization:Float64;N, region:Dictionary(Int32, Utf8);N, status:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), write_utilization:Float64;N]
                    Filter: diskio.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [bytes_read:Int64;N, bytes_written:Int64;N, host:Dictionary(Int32, Utf8);N, is_local:Boolean;N, read_utilization:Float64;N, region:Dictionary(Int32, Utf8);N, status:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), write_utilization:Float64;N]
                      TableScan: diskio [bytes_read:Int64;N, bytes_written:Int64;N, host:Dictionary(Int32, Utf8);N, is_local:Boolean;N, read_utilization:Float64;N, region:Dictionary(Int32, Utf8);N, status:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), write_utilization:Float64;N]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("merge_00")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [col0:Dictionary(Int32, Utf8);N, col1:Float64;N, col2:Boolean;N, col3:Utf8;N, time:Timestamp(Nanosecond, None)]
                    Filter: merge_00.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [col0:Dictionary(Int32, Utf8);N, col1:Float64;N, col2:Boolean;N, col3:Utf8;N, time:Timestamp(Nanosecond, None)]
                      TableScan: merge_00 [col0:Dictionary(Int32, Utf8);N, col1:Float64;N, col2:Boolean;N, col3:Utf8;N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("merge_01")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [col0:Float64;N, col1:Dictionary(Int32, Utf8);N, col2:Utf8;N, col3:Boolean;N, time:Timestamp(Nanosecond, None)]
                    Filter: merge_01.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [col0:Float64;N, col1:Dictionary(Int32, Utf8);N, col2:Utf8;N, col3:Boolean;N, time:Timestamp(Nanosecond, None)]
                      TableScan: merge_01 [col0:Float64;N, col1:Dictionary(Int32, Utf8);N, col2:Utf8;N, col3:Boolean;N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("name_clash")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [f:Float64;N, first:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                    Filter: name_clash.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [f:Float64;N, first:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                      TableScan: name_clash [f:Float64;N, first:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("temp_01")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [field_f64:Float64;N, field_i64:Int64;N, field_str:Utf8;N, field_u64:UInt64;N, shared_field0:Float64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                    Filter: temp_01.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [field_f64:Float64;N, field_i64:Int64;N, field_str:Utf8;N, field_u64:UInt64;N, shared_field0:Float64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                      TableScan: temp_01 [field_f64:Float64;N, field_i64:Int64;N, field_str:Utf8;N, field_u64:UInt64;N, shared_field0:Float64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("temp_02")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [shared_field0:Int64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                    Filter: temp_02.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [shared_field0:Int64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                      TableScan: temp_02 [shared_field0:Int64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("temp_03")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [shared_field0:Utf8;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                    Filter: temp_03.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [shared_field0:Utf8;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                      TableScan: temp_03 [shared_field0:Utf8;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
            "###);
            assert_snapshot!(plan("SHOW MEASUREMENTS WHERE time > 1337"), @r###"
            Sort: iox::measurement ASC NULLS LAST, name ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
              Union [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("all_types")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
                    Filter: all_types.time >= TimestampNanosecond(1338, None) [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
                      TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("cpu")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                    Filter: cpu.time >= TimestampNanosecond(1338, None) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("data")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                    Filter: data.time >= TimestampNanosecond(1338, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                      TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("disk")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                    Filter: disk.time >= TimestampNanosecond(1338, None) [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                      TableScan: disk [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("diskio")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [bytes_read:Int64;N, bytes_written:Int64;N, host:Dictionary(Int32, Utf8);N, is_local:Boolean;N, read_utilization:Float64;N, region:Dictionary(Int32, Utf8);N, status:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), write_utilization:Float64;N]
                    Filter: diskio.time >= TimestampNanosecond(1338, None) [bytes_read:Int64;N, bytes_written:Int64;N, host:Dictionary(Int32, Utf8);N, is_local:Boolean;N, read_utilization:Float64;N, region:Dictionary(Int32, Utf8);N, status:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), write_utilization:Float64;N]
                      TableScan: diskio [bytes_read:Int64;N, bytes_written:Int64;N, host:Dictionary(Int32, Utf8);N, is_local:Boolean;N, read_utilization:Float64;N, region:Dictionary(Int32, Utf8);N, status:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), write_utilization:Float64;N]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("merge_00")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [col0:Dictionary(Int32, Utf8);N, col1:Float64;N, col2:Boolean;N, col3:Utf8;N, time:Timestamp(Nanosecond, None)]
                    Filter: merge_00.time >= TimestampNanosecond(1338, None) [col0:Dictionary(Int32, Utf8);N, col1:Float64;N, col2:Boolean;N, col3:Utf8;N, time:Timestamp(Nanosecond, None)]
                      TableScan: merge_00 [col0:Dictionary(Int32, Utf8);N, col1:Float64;N, col2:Boolean;N, col3:Utf8;N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("merge_01")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [col0:Float64;N, col1:Dictionary(Int32, Utf8);N, col2:Utf8;N, col3:Boolean;N, time:Timestamp(Nanosecond, None)]
                    Filter: merge_01.time >= TimestampNanosecond(1338, None) [col0:Float64;N, col1:Dictionary(Int32, Utf8);N, col2:Utf8;N, col3:Boolean;N, time:Timestamp(Nanosecond, None)]
                      TableScan: merge_01 [col0:Float64;N, col1:Dictionary(Int32, Utf8);N, col2:Utf8;N, col3:Boolean;N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("name_clash")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [f:Float64;N, first:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                    Filter: name_clash.time >= TimestampNanosecond(1338, None) [f:Float64;N, first:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                      TableScan: name_clash [f:Float64;N, first:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("temp_01")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [field_f64:Float64;N, field_i64:Int64;N, field_str:Utf8;N, field_u64:UInt64;N, shared_field0:Float64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                    Filter: temp_01.time >= TimestampNanosecond(1338, None) [field_f64:Float64;N, field_i64:Int64;N, field_str:Utf8;N, field_u64:UInt64;N, shared_field0:Float64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                      TableScan: temp_01 [field_f64:Float64;N, field_i64:Int64;N, field_str:Utf8;N, field_u64:UInt64;N, shared_field0:Float64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("temp_02")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [shared_field0:Int64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                    Filter: temp_02.time >= TimestampNanosecond(1338, None) [shared_field0:Int64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                      TableScan: temp_02 [shared_field0:Int64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("measurements")) AS iox::measurement, Dictionary(Int32, Utf8("temp_03")) AS name [iox::measurement:Dictionary(Int32, Utf8), name:Dictionary(Int32, Utf8)]
                  Limit: skip=0, fetch=1 [shared_field0:Utf8;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                    Filter: temp_03.time >= TimestampNanosecond(1338, None) [shared_field0:Utf8;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                      TableScan: temp_03 [shared_field0:Utf8;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
            "###);
        }

        #[test]
        fn test_show_tag_keys_1() {
            assert_snapshot!(plan("SHOW TAG KEYS"), @"TableScan: tag_keys [iox::measurement:Dictionary(Int32, Utf8), tagKey:Dictionary(Int32, Utf8)]");
            assert_snapshot!(plan("SHOW TAG KEYS LIMIT 1 OFFSET 2"), @r###"
            Sort: tag_keys.iox::measurement ASC NULLS LAST, tag_keys.tagKey ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), tagKey:Dictionary(Int32, Utf8)]
              Projection: tag_keys.iox::measurement, tag_keys.tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Dictionary(Int32, Utf8)]
                Filter: iox::row BETWEEN Int64(3) AND Int64(3) [iox::measurement:Dictionary(Int32, Utf8), tagKey:Dictionary(Int32, Utf8), iox::row:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [tag_keys.iox::measurement] ORDER BY [tag_keys.tagKey ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS iox::row]] [iox::measurement:Dictionary(Int32, Utf8), tagKey:Dictionary(Int32, Utf8), iox::row:UInt64;N]
                    TableScan: tag_keys [iox::measurement:Dictionary(Int32, Utf8), tagKey:Dictionary(Int32, Utf8)]
            "###);
        }

        #[test]
        fn test_show_tag_keys_2() {
            assert_snapshot!(plan("SHOW TAG KEYS WHERE foo = 'some_foo'"), @r###"
            Sort: iox::measurement ASC NULLS LAST, tagKey ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
              Union [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN tag0 > Int32(0) THEN Utf8("tag0") END, CASE WHEN tag1 > Int32(0) THEN Utf8("tag1") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(all_types.tag0 IS NOT NULL AS UInt64)) AS tag0, SUM(CAST(all_types.tag1 IS NOT NULL AS UInt64)) AS tag1]] [tag0:UInt64;N, tag1:UInt64;N]
                          Filter: all_types.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
                            TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN cpu > Int32(0) THEN Utf8("cpu") END, CASE WHEN host > Int32(0) THEN Utf8("host") END, CASE WHEN region > Int32(0) THEN Utf8("region") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(cpu.cpu IS NOT NULL AS UInt64)) AS cpu, SUM(CAST(cpu.host IS NOT NULL AS UInt64)) AS host, SUM(CAST(cpu.region IS NOT NULL AS UInt64)) AS region]] [cpu:UInt64;N, host:UInt64;N, region:UInt64;N]
                          Filter: cpu.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                            TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN bar > Int32(0) THEN Utf8("bar") END, CASE WHEN foo > Int32(0) THEN Utf8("foo") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(data.bar IS NOT NULL AS UInt64)) AS bar, SUM(CAST(data.foo IS NOT NULL AS UInt64)) AS foo]] [bar:UInt64;N, foo:UInt64;N]
                          Filter: data.time >= TimestampNanosecond(1672444800000000000, None) AND data.foo = Dictionary(Int32, Utf8("some_foo")) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                            TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                Projection: Dictionary(Int32, Utf8("disk")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN device > Int32(0) THEN Utf8("device") END, CASE WHEN host > Int32(0) THEN Utf8("host") END, CASE WHEN region > Int32(0) THEN Utf8("region") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(disk.device IS NOT NULL AS UInt64)) AS device, SUM(CAST(disk.host IS NOT NULL AS UInt64)) AS host, SUM(CAST(disk.region IS NOT NULL AS UInt64)) AS region]] [device:UInt64;N, host:UInt64;N, region:UInt64;N]
                          Filter: disk.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                            TableScan: disk [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("diskio")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN host > Int32(0) THEN Utf8("host") END, CASE WHEN region > Int32(0) THEN Utf8("region") END, CASE WHEN status > Int32(0) THEN Utf8("status") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(diskio.host IS NOT NULL AS UInt64)) AS host, SUM(CAST(diskio.region IS NOT NULL AS UInt64)) AS region, SUM(CAST(diskio.status IS NOT NULL AS UInt64)) AS status]] [host:UInt64;N, region:UInt64;N, status:UInt64;N]
                          Filter: diskio.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [bytes_read:Int64;N, bytes_written:Int64;N, host:Dictionary(Int32, Utf8);N, is_local:Boolean;N, read_utilization:Float64;N, region:Dictionary(Int32, Utf8);N, status:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), write_utilization:Float64;N]
                            TableScan: diskio [bytes_read:Int64;N, bytes_written:Int64;N, host:Dictionary(Int32, Utf8);N, is_local:Boolean;N, read_utilization:Float64;N, region:Dictionary(Int32, Utf8);N, status:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), write_utilization:Float64;N]
                Projection: Dictionary(Int32, Utf8("merge_00")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN col0 > Int32(0) THEN Utf8("col0") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(merge_00.col0 IS NOT NULL AS UInt64)) AS col0]] [col0:UInt64;N]
                          Filter: merge_00.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [col0:Dictionary(Int32, Utf8);N, col1:Float64;N, col2:Boolean;N, col3:Utf8;N, time:Timestamp(Nanosecond, None)]
                            TableScan: merge_00 [col0:Dictionary(Int32, Utf8);N, col1:Float64;N, col2:Boolean;N, col3:Utf8;N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("merge_01")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN col1 > Int32(0) THEN Utf8("col1") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(merge_01.col1 IS NOT NULL AS UInt64)) AS col1]] [col1:UInt64;N]
                          Filter: merge_01.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [col0:Float64;N, col1:Dictionary(Int32, Utf8);N, col2:Utf8;N, col3:Boolean;N, time:Timestamp(Nanosecond, None)]
                            TableScan: merge_01 [col0:Float64;N, col1:Dictionary(Int32, Utf8);N, col2:Utf8;N, col3:Boolean;N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("name_clash")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN first > Int32(0) THEN Utf8("first") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(name_clash.first IS NOT NULL AS UInt64)) AS first]] [first:UInt64;N]
                          Filter: name_clash.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [f:Float64;N, first:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                            TableScan: name_clash [f:Float64;N, first:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("temp_01")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN shared_tag0 > Int32(0) THEN Utf8("shared_tag0") END, CASE WHEN shared_tag1 > Int32(0) THEN Utf8("shared_tag1") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(temp_01.shared_tag0 IS NOT NULL AS UInt64)) AS shared_tag0, SUM(CAST(temp_01.shared_tag1 IS NOT NULL AS UInt64)) AS shared_tag1]] [shared_tag0:UInt64;N, shared_tag1:UInt64;N]
                          Filter: temp_01.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [field_f64:Float64;N, field_i64:Int64;N, field_str:Utf8;N, field_u64:UInt64;N, shared_field0:Float64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                            TableScan: temp_01 [field_f64:Float64;N, field_i64:Int64;N, field_str:Utf8;N, field_u64:UInt64;N, shared_field0:Float64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("temp_02")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN shared_tag0 > Int32(0) THEN Utf8("shared_tag0") END, CASE WHEN shared_tag1 > Int32(0) THEN Utf8("shared_tag1") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(temp_02.shared_tag0 IS NOT NULL AS UInt64)) AS shared_tag0, SUM(CAST(temp_02.shared_tag1 IS NOT NULL AS UInt64)) AS shared_tag1]] [shared_tag0:UInt64;N, shared_tag1:UInt64;N]
                          Filter: temp_02.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [shared_field0:Int64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                            TableScan: temp_02 [shared_field0:Int64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("temp_03")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN shared_tag0 > Int32(0) THEN Utf8("shared_tag0") END, CASE WHEN shared_tag1 > Int32(0) THEN Utf8("shared_tag1") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(temp_03.shared_tag0 IS NOT NULL AS UInt64)) AS shared_tag0, SUM(CAST(temp_03.shared_tag1 IS NOT NULL AS UInt64)) AS shared_tag1]] [shared_tag0:UInt64;N, shared_tag1:UInt64;N]
                          Filter: temp_03.time >= TimestampNanosecond(1672444800000000000, None) AND Boolean(false) [shared_field0:Utf8;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                            TableScan: temp_03 [shared_field0:Utf8;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
            "###);
        }

        #[test]
        fn test_show_tag_keys_3() {
            assert_snapshot!(plan("SHOW TAG KEYS WHERE time > 1337"), @r###"
            Sort: iox::measurement ASC NULLS LAST, tagKey ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
              Union [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN tag0 > Int32(0) THEN Utf8("tag0") END, CASE WHEN tag1 > Int32(0) THEN Utf8("tag1") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(all_types.tag0 IS NOT NULL AS UInt64)) AS tag0, SUM(CAST(all_types.tag1 IS NOT NULL AS UInt64)) AS tag1]] [tag0:UInt64;N, tag1:UInt64;N]
                          Filter: all_types.time >= TimestampNanosecond(1338, None) [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
                            TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN cpu > Int32(0) THEN Utf8("cpu") END, CASE WHEN host > Int32(0) THEN Utf8("host") END, CASE WHEN region > Int32(0) THEN Utf8("region") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(cpu.cpu IS NOT NULL AS UInt64)) AS cpu, SUM(CAST(cpu.host IS NOT NULL AS UInt64)) AS host, SUM(CAST(cpu.region IS NOT NULL AS UInt64)) AS region]] [cpu:UInt64;N, host:UInt64;N, region:UInt64;N]
                          Filter: cpu.time >= TimestampNanosecond(1338, None) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                            TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN bar > Int32(0) THEN Utf8("bar") END, CASE WHEN foo > Int32(0) THEN Utf8("foo") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(data.bar IS NOT NULL AS UInt64)) AS bar, SUM(CAST(data.foo IS NOT NULL AS UInt64)) AS foo]] [bar:UInt64;N, foo:UInt64;N]
                          Filter: data.time >= TimestampNanosecond(1338, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                            TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                Projection: Dictionary(Int32, Utf8("disk")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN device > Int32(0) THEN Utf8("device") END, CASE WHEN host > Int32(0) THEN Utf8("host") END, CASE WHEN region > Int32(0) THEN Utf8("region") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(disk.device IS NOT NULL AS UInt64)) AS device, SUM(CAST(disk.host IS NOT NULL AS UInt64)) AS host, SUM(CAST(disk.region IS NOT NULL AS UInt64)) AS region]] [device:UInt64;N, host:UInt64;N, region:UInt64;N]
                          Filter: disk.time >= TimestampNanosecond(1338, None) [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                            TableScan: disk [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("diskio")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN host > Int32(0) THEN Utf8("host") END, CASE WHEN region > Int32(0) THEN Utf8("region") END, CASE WHEN status > Int32(0) THEN Utf8("status") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(diskio.host IS NOT NULL AS UInt64)) AS host, SUM(CAST(diskio.region IS NOT NULL AS UInt64)) AS region, SUM(CAST(diskio.status IS NOT NULL AS UInt64)) AS status]] [host:UInt64;N, region:UInt64;N, status:UInt64;N]
                          Filter: diskio.time >= TimestampNanosecond(1338, None) [bytes_read:Int64;N, bytes_written:Int64;N, host:Dictionary(Int32, Utf8);N, is_local:Boolean;N, read_utilization:Float64;N, region:Dictionary(Int32, Utf8);N, status:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), write_utilization:Float64;N]
                            TableScan: diskio [bytes_read:Int64;N, bytes_written:Int64;N, host:Dictionary(Int32, Utf8);N, is_local:Boolean;N, read_utilization:Float64;N, region:Dictionary(Int32, Utf8);N, status:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), write_utilization:Float64;N]
                Projection: Dictionary(Int32, Utf8("merge_00")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN col0 > Int32(0) THEN Utf8("col0") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(merge_00.col0 IS NOT NULL AS UInt64)) AS col0]] [col0:UInt64;N]
                          Filter: merge_00.time >= TimestampNanosecond(1338, None) [col0:Dictionary(Int32, Utf8);N, col1:Float64;N, col2:Boolean;N, col3:Utf8;N, time:Timestamp(Nanosecond, None)]
                            TableScan: merge_00 [col0:Dictionary(Int32, Utf8);N, col1:Float64;N, col2:Boolean;N, col3:Utf8;N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("merge_01")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN col1 > Int32(0) THEN Utf8("col1") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(merge_01.col1 IS NOT NULL AS UInt64)) AS col1]] [col1:UInt64;N]
                          Filter: merge_01.time >= TimestampNanosecond(1338, None) [col0:Float64;N, col1:Dictionary(Int32, Utf8);N, col2:Utf8;N, col3:Boolean;N, time:Timestamp(Nanosecond, None)]
                            TableScan: merge_01 [col0:Float64;N, col1:Dictionary(Int32, Utf8);N, col2:Utf8;N, col3:Boolean;N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("name_clash")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN first > Int32(0) THEN Utf8("first") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(name_clash.first IS NOT NULL AS UInt64)) AS first]] [first:UInt64;N]
                          Filter: name_clash.time >= TimestampNanosecond(1338, None) [f:Float64;N, first:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                            TableScan: name_clash [f:Float64;N, first:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("temp_01")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN shared_tag0 > Int32(0) THEN Utf8("shared_tag0") END, CASE WHEN shared_tag1 > Int32(0) THEN Utf8("shared_tag1") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(temp_01.shared_tag0 IS NOT NULL AS UInt64)) AS shared_tag0, SUM(CAST(temp_01.shared_tag1 IS NOT NULL AS UInt64)) AS shared_tag1]] [shared_tag0:UInt64;N, shared_tag1:UInt64;N]
                          Filter: temp_01.time >= TimestampNanosecond(1338, None) [field_f64:Float64;N, field_i64:Int64;N, field_str:Utf8;N, field_u64:UInt64;N, shared_field0:Float64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                            TableScan: temp_01 [field_f64:Float64;N, field_i64:Int64;N, field_str:Utf8;N, field_u64:UInt64;N, shared_field0:Float64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("temp_02")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN shared_tag0 > Int32(0) THEN Utf8("shared_tag0") END, CASE WHEN shared_tag1 > Int32(0) THEN Utf8("shared_tag1") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(temp_02.shared_tag0 IS NOT NULL AS UInt64)) AS shared_tag0, SUM(CAST(temp_02.shared_tag1 IS NOT NULL AS UInt64)) AS shared_tag1]] [shared_tag0:UInt64;N, shared_tag1:UInt64;N]
                          Filter: temp_02.time >= TimestampNanosecond(1338, None) [shared_field0:Int64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                            TableScan: temp_02 [shared_field0:Int64;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("temp_03")) AS iox::measurement, tagKey [iox::measurement:Dictionary(Int32, Utf8), tagKey:Utf8;N]
                  Filter: tagKey IS NOT NULL [tagKey:Utf8;N]
                    Unnest: tagKey [tagKey:Utf8;N]
                      Projection: make_array(CASE WHEN shared_tag0 > Int32(0) THEN Utf8("shared_tag0") END, CASE WHEN shared_tag1 > Int32(0) THEN Utf8("shared_tag1") END) AS tagKey [tagKey:List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} });N]
                        Aggregate: groupBy=[[]], aggr=[[SUM(CAST(temp_03.shared_tag0 IS NOT NULL AS UInt64)) AS shared_tag0, SUM(CAST(temp_03.shared_tag1 IS NOT NULL AS UInt64)) AS shared_tag1]] [shared_tag0:UInt64;N, shared_tag1:UInt64;N]
                          Filter: temp_03.time >= TimestampNanosecond(1338, None) [shared_field0:Utf8;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                            TableScan: temp_03 [shared_field0:Utf8;N, shared_tag0:Dictionary(Int32, Utf8);N, shared_tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
            "###);
        }

        #[test]
        fn test_show_tag_values_1() {
            assert_snapshot!(plan("SHOW TAG VALUES WITH KEY = bar"), @r###"
            Sort: iox::measurement ASC NULLS LAST, key ASC NULLS LAST, value ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, Dictionary(Int32, Utf8("bar")) AS key, data.bar AS value [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N]
                Distinct: [bar:Dictionary(Int32, Utf8);N]
                  Projection: data.bar [bar:Dictionary(Int32, Utf8);N]
                    Filter: data.time >= TimestampNanosecond(1672444800000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                      TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
        }

        #[test]
        fn test_show_tag_values_2() {
            assert_snapshot!(plan("SHOW TAG VALUES WITH KEY = bar LIMIT 1 OFFSET 2"), @r###"
            Sort: iox::measurement ASC NULLS LAST, key ASC NULLS LAST, value ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N]
              Projection: iox::measurement, key, value [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N]
                Filter: iox::row BETWEEN Int64(3) AND Int64(3) [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N, iox::row:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [iox::measurement] ORDER BY [key ASC NULLS LAST, value ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS iox::row]] [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N, iox::row:UInt64;N]
                    Sort: iox::measurement ASC NULLS LAST, key ASC NULLS LAST, value ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N]
                      Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, Dictionary(Int32, Utf8("bar")) AS key, data.bar AS value [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N]
                        Distinct: [bar:Dictionary(Int32, Utf8);N]
                          Projection: data.bar [bar:Dictionary(Int32, Utf8);N]
                            Filter: data.time >= TimestampNanosecond(1672444800000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                              TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
        }

        #[test]
        fn test_show_tag_values_3() {
            assert_snapshot!(plan("SHOW TAG VALUES WITH KEY = bar WHERE foo = 'some_foo'"), @r###"
            Sort: iox::measurement ASC NULLS LAST, key ASC NULLS LAST, value ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, Dictionary(Int32, Utf8("bar")) AS key, data.bar AS value [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N]
                Distinct: [bar:Dictionary(Int32, Utf8);N]
                  Projection: data.bar [bar:Dictionary(Int32, Utf8);N]
                    Filter: data.time >= TimestampNanosecond(1672444800000000000, None) AND data.foo = Dictionary(Int32, Utf8("some_foo")) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                      TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
        }

        #[test]
        fn test_show_tag_values_4() {
            assert_snapshot!(plan("SHOW TAG VALUES WITH KEY = bar WHERE time > 1337"), @r###"
            Sort: iox::measurement ASC NULLS LAST, key ASC NULLS LAST, value ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, Dictionary(Int32, Utf8("bar")) AS key, data.bar AS value [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N]
                Distinct: [bar:Dictionary(Int32, Utf8);N]
                  Projection: data.bar [bar:Dictionary(Int32, Utf8);N]
                    Filter: data.time >= TimestampNanosecond(1338, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                      TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
        }

        #[test]
        fn test_show_retention_policies() {
            assert_snapshot!(plan("SHOW RETENTION POLICIES"), @r###"
            TableScan: retention policies [iox::measurement:Dictionary(Int32, Utf8), name:Utf8, duration:Utf8, shardGroupDuration:Utf8, replicaN:Int64, default:Boolean]
            "###);
            assert_snapshot!(plan("SHOW RETENTION POLICIES ON my_db"), @r###"
            This feature is not implemented: SHOW RETENTION POLICIES ON <database>
            "###);
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
                assert_snapshot!(plan("SELECT value FROM (SELECT usage_idle AS value FROM cpu)"), @r###"
                Sort: time AS time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), value:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time AS time, value AS value [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), value:Float64;N]
                    Sort: time ASC NULLS LAST [time:Timestamp(Nanosecond, None), value:Float64;N]
                      Projection: cpu.time AS time, cpu.usage_idle AS value [time:Timestamp(Nanosecond, None), value:Float64;N]
                        TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

                // project a wildcard
                assert_snapshot!(plan("SELECT * FROM (SELECT usage_idle, usage_system AS value FROM cpu)"), @r###"
                Sort: time AS time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), usage_idle:Float64;N, value:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time AS time, usage_idle AS usage_idle, value AS value [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), usage_idle:Float64;N, value:Float64;N]
                    Sort: time ASC NULLS LAST [time:Timestamp(Nanosecond, None), usage_idle:Float64;N, value:Float64;N]
                      Projection: cpu.time AS time, cpu.usage_idle AS usage_idle, cpu.usage_system AS value [time:Timestamp(Nanosecond, None), usage_idle:Float64;N, value:Float64;N]
                        TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }

            /// Projecting subqueries that do not use aggregate or selector functions.
            #[test]
            fn aggregates() {
                // project an aggregate
                assert_snapshot!(plan("SELECT value FROM (SELECT mean(usage_idle) AS value FROM cpu)"), @r###"
                Sort: time AS time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), value:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time AS time, value AS value [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), value:Float64;N]
                    Sort: time ASC NULLS LAST [time:Timestamp(Nanosecond, None), value:Float64;N]
                      Projection: TimestampNanosecond(0, None) AS time, AVG(cpu.usage_idle) AS value [time:Timestamp(Nanosecond, None), value:Float64;N]
                        Aggregate: groupBy=[[]], aggr=[[AVG(cpu.usage_idle)]] [AVG(cpu.usage_idle):Float64;N]
                          TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

                assert_snapshot!(plan("SELECT value FROM (SELECT mean(usage_idle) AS value FROM cpu GROUP BY TIME(10s))"), @r###"
                Sort: time AS time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, value:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time AS time, value AS value [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, value:Float64;N]
                    Sort: time ASC NULLS LAST [time:Timestamp(Nanosecond, None);N, value:Float64;N]
                      Projection: time, AVG(cpu.usage_idle) AS value [time:Timestamp(Nanosecond, None);N, value:Float64;N]
                        Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), cpu.time, TimestampNanosecond(0, None)) AS time]], aggr=[[AVG(cpu.usage_idle)]] [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N]
                          Filter: cpu.time <= TimestampNanosecond(1672531200000000000, None) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                            TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }

            /// Projecting subqueries that use the `DISTINCT` function / operator.
            #[test]
            fn distinct() {
                // Subquery is a DISTINCT
                assert_snapshot!(plan("SELECT value FROM (SELECT DISTINCT(usage_idle) AS value FROM cpu)"), @r###"
                Sort: time AS time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), value:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time AS time, value AS value [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), value:Float64;N]
                    Sort: time ASC NULLS LAST [time:Timestamp(Nanosecond, None), value:Float64;N]
                      Distinct: [time:Timestamp(Nanosecond, None), value:Float64;N]
                        Projection: TimestampNanosecond(0, None) AS time, cpu.usage_idle AS value [time:Timestamp(Nanosecond, None), value:Float64;N]
                          TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

                // Outer query projects subquery with binary expressions
                assert_snapshot!(plan("SELECT value * 0.99 FROM (SELECT DISTINCT(usage_idle) AS value FROM cpu)"), @r###"
                Sort: time AS time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), value:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time AS time, value * Float64(0.99) AS value [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), value:Float64;N]
                    Sort: time ASC NULLS LAST [time:Timestamp(Nanosecond, None), value:Float64;N]
                      Distinct: [time:Timestamp(Nanosecond, None), value:Float64;N]
                        Projection: TimestampNanosecond(0, None) AS time, cpu.usage_idle AS value [time:Timestamp(Nanosecond, None), value:Float64;N]
                          TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

                // Outer query groups by the `cpu` tag, which should be pushed all the way to inner-most subquery
                assert_snapshot!(plan("SELECT * FROM (SELECT MAX(value) FROM (SELECT DISTINCT(usage_idle) AS value FROM cpu)) GROUP BY cpu"), @r###"
                Sort: cpu AS cpu ASC NULLS LAST, time AS time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, cpu:Dictionary(Int32, Utf8);N, max:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time AS time, cpu AS cpu, max AS max [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, cpu:Dictionary(Int32, Utf8);N, max:Float64;N]
                    Sort: cpu AS cpu ASC NULLS LAST, time ASC NULLS LAST [time:Timestamp(Nanosecond, None);N, cpu:Dictionary(Int32, Utf8);N, max:Float64;N]
                      Projection: (selector_max(value,time))[time] AS time, cpu AS cpu, (selector_max(value,time))[value] AS max [time:Timestamp(Nanosecond, None);N, cpu:Dictionary(Int32, Utf8);N, max:Float64;N]
                        Aggregate: groupBy=[[cpu]], aggr=[[selector_max(value, time)]] [cpu:Dictionary(Int32, Utf8);N, selector_max(value,time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                          Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, value:Float64;N]
                            Distinct: [time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, value:Float64;N]
                              Projection: TimestampNanosecond(0, None) AS time, cpu.cpu AS cpu, cpu.usage_idle AS value [time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, value:Float64;N]
                                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }
        }

        /// Validate plans for multiple data sources in the `FROM` clause, including subqueries
        #[test]
        fn multiple_data_sources() {
            // same table for each subquery
            //
            // ⚠️ Important
            // The aggregate must be applied to the UNION of all instances of the cpu table
            assert_snapshot!(plan("SELECT last(a) / last(b) FROM (SELECT mean(usage_idle) AS a FROM cpu), (SELECT mean(usage_user) AS b FROM cpu)"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), last_last:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, TimestampNanosecond(0, None) AS time, coalesce((selector_last(a,time))[value] / (selector_last(b,time))[value], Float64(0)) AS last_last [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), last_last:Float64;N]
                Aggregate: groupBy=[[]], aggr=[[selector_last(a, time), selector_last(b, time)]] [selector_last(a,time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N, selector_last(b,time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                  Union [a:Float64;N, b:Float64;N, time:Timestamp(Nanosecond, None)]
                    Projection: a AS a, CAST(NULL AS Float64) AS b, time AS time [a:Float64;N, b:Float64;N, time:Timestamp(Nanosecond, None)]
                      Sort: time ASC NULLS LAST [time:Timestamp(Nanosecond, None), a:Float64;N]
                        Projection: TimestampNanosecond(0, None) AS time, AVG(cpu.usage_idle) AS a [time:Timestamp(Nanosecond, None), a:Float64;N]
                          Aggregate: groupBy=[[]], aggr=[[AVG(cpu.usage_idle)]] [AVG(cpu.usage_idle):Float64;N]
                            TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                    Projection: CAST(NULL AS Float64) AS a, b AS b, time AS time [a:Float64;N, b:Float64;N, time:Timestamp(Nanosecond, None)]
                      Sort: time ASC NULLS LAST [time:Timestamp(Nanosecond, None), b:Float64;N]
                        Projection: TimestampNanosecond(0, None) AS time, AVG(cpu.usage_user) AS b [time:Timestamp(Nanosecond, None), b:Float64;N]
                          Aggregate: groupBy=[[]], aggr=[[AVG(cpu.usage_user)]] [AVG(cpu.usage_user):Float64;N]
                            TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            // selector with repeated table
            //
            // ⚠️ Important
            // The selector must be applied to the UNION of all instances of the cpu table
            assert_snapshot!(plan("SELECT last(usage_idle) FROM cpu, cpu"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, (selector_last(usage_idle,time))[time] AS time, (selector_last(usage_idle,time))[value] AS last [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N]
                Aggregate: groupBy=[[]], aggr=[[selector_last(usage_idle, time)]] [selector_last(usage_idle,time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                  Union [time:Timestamp(Nanosecond, None), usage_idle:Float64;N]
                    Projection: cpu.time AS time, cpu.usage_idle AS usage_idle [time:Timestamp(Nanosecond, None), usage_idle:Float64;N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                    Projection: cpu.time AS time, cpu.usage_idle AS usage_idle [time:Timestamp(Nanosecond, None), usage_idle:Float64;N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            // different tables for each subquery
            //
            // ⚠️ Important
            // The selector must be applied independently for each unique table
            assert_snapshot!(plan("SELECT last(value) FROM (SELECT usage_idle AS value FROM cpu), (SELECT bytes_free AS value FROM disk)"), @r###"
            Sort: iox::measurement ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N]
              Union [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, (selector_last(value,time))[time] AS time, (selector_last(value,time))[value] AS last [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N]
                  Aggregate: groupBy=[[]], aggr=[[selector_last(value, time)]] [selector_last(value,time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                    Sort: time ASC NULLS LAST [time:Timestamp(Nanosecond, None), value:Float64;N]
                      Projection: cpu.time AS time, cpu.usage_idle AS value [time:Timestamp(Nanosecond, None), value:Float64;N]
                        TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                Projection: Dictionary(Int32, Utf8("disk")) AS iox::measurement, (selector_last(value,time))[time] AS time, (selector_last(value,time))[value] AS last [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N]
                  Aggregate: groupBy=[[]], aggr=[[selector_last(CAST(value AS Float64), time)]] [selector_last(value,time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                    Sort: time ASC NULLS LAST [time:Timestamp(Nanosecond, None), value:Int64;N]
                      Projection: disk.time AS time, disk.bytes_free AS value [time:Timestamp(Nanosecond, None), value:Int64;N]
                        TableScan: disk [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
            "###);
        }

        #[test]
        fn test_time_column() {
            // validate time column is explicitly projected
            assert_snapshot!(plan("SELECT usage_idle, time FROM cpu"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), usage_idle:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), usage_idle:Float64;N]
                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            // validate time column may be aliased
            assert_snapshot!(plan("SELECT usage_idle, time AS timestamp FROM cpu"), @r###"
            Sort: timestamp ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), timestamp:Timestamp(Nanosecond, None), usage_idle:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS timestamp, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), timestamp:Timestamp(Nanosecond, None), usage_idle:Float64;N]
                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
        }

        mod window_functions {
            use super::*;

            #[test]
            fn test_difference() {
                // no aggregates
                assert_snapshot!(plan("SELECT DIFFERENCE(usage_idle) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), difference:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, difference [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), difference:Float64;N]
                    Filter: NOT difference IS NULL [time:Timestamp(Nanosecond, None), difference:Float64;N]
                      Projection: cpu.time AS time, difference(cpu.usage_idle) AS difference [time:Timestamp(Nanosecond, None), difference:Float64;N]
                        WindowAggr: windowExpr=[[difference(cpu.usage_idle) ORDER BY [cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS difference(cpu.usage_idle)]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, difference(cpu.usage_idle):Float64;N]
                          TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

                // aggregate
                assert_snapshot!(plan("SELECT DIFFERENCE(MEAN(usage_idle)) FROM cpu GROUP BY TIME(10s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, difference:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, difference [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, difference:Float64;N]
                    Filter: NOT difference IS NULL [time:Timestamp(Nanosecond, None);N, difference:Float64;N]
                      Projection: time, difference(AVG(cpu.usage_idle)) AS difference [time:Timestamp(Nanosecond, None);N, difference:Float64;N]
                        WindowAggr: windowExpr=[[difference(AVG(cpu.usage_idle)) ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS difference(AVG(cpu.usage_idle))]] [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N, difference(AVG(cpu.usage_idle)):Float64;N]
                          GapFill: groupBy=[time], aggr=[[AVG(cpu.usage_idle)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N]
                            Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), cpu.time, TimestampNanosecond(0, None)) AS time]], aggr=[[AVG(cpu.usage_idle)]] [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N]
                              Filter: cpu.time <= TimestampNanosecond(1672531200000000000, None) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }

            #[test]
            fn test_non_negative_difference() {
                // no aggregates
                assert_snapshot!(plan("SELECT NON_NEGATIVE_DIFFERENCE(usage_idle) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), non_negative_difference:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, non_negative_difference [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), non_negative_difference:Float64;N]
                    Filter: NOT non_negative_difference IS NULL [time:Timestamp(Nanosecond, None), non_negative_difference:Float64;N]
                      Projection: cpu.time AS time, non_negative_difference(cpu.usage_idle) AS non_negative_difference [time:Timestamp(Nanosecond, None), non_negative_difference:Float64;N]
                        WindowAggr: windowExpr=[[non_negative_difference(cpu.usage_idle) ORDER BY [cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS non_negative_difference(cpu.usage_idle)]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, non_negative_difference(cpu.usage_idle):Float64;N]
                          TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

                // aggregate
                assert_snapshot!(plan("SELECT NON_NEGATIVE_DIFFERENCE(MEAN(usage_idle)) FROM cpu GROUP BY TIME(10s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, non_negative_difference:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, non_negative_difference [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, non_negative_difference:Float64;N]
                    Filter: NOT non_negative_difference IS NULL [time:Timestamp(Nanosecond, None);N, non_negative_difference:Float64;N]
                      Projection: time, non_negative_difference(AVG(cpu.usage_idle)) AS non_negative_difference [time:Timestamp(Nanosecond, None);N, non_negative_difference:Float64;N]
                        WindowAggr: windowExpr=[[non_negative_difference(AVG(cpu.usage_idle)) ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS non_negative_difference(AVG(cpu.usage_idle))]] [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N, non_negative_difference(AVG(cpu.usage_idle)):Float64;N]
                          GapFill: groupBy=[time], aggr=[[AVG(cpu.usage_idle)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N]
                            Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), cpu.time, TimestampNanosecond(0, None)) AS time]], aggr=[[AVG(cpu.usage_idle)]] [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N]
                              Filter: cpu.time <= TimestampNanosecond(1672531200000000000, None) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }

            #[test]
            fn test_moving_average() {
                // no aggregates
                assert_snapshot!(plan("SELECT MOVING_AVERAGE(usage_idle, 3) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), moving_average:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, moving_average [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), moving_average:Float64;N]
                    Filter: NOT moving_average IS NULL [time:Timestamp(Nanosecond, None), moving_average:Float64;N]
                      Projection: cpu.time AS time, moving_average(cpu.usage_idle,Int64(3)) AS moving_average [time:Timestamp(Nanosecond, None), moving_average:Float64;N]
                        WindowAggr: windowExpr=[[moving_average(cpu.usage_idle, Int64(3)) ORDER BY [cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS moving_average(cpu.usage_idle,Int64(3))]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, moving_average(cpu.usage_idle,Int64(3)):Float64;N]
                          TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

                // aggregate
                assert_snapshot!(plan("SELECT MOVING_AVERAGE(MEAN(usage_idle), 3) FROM cpu GROUP BY TIME(10s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, moving_average:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, moving_average [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, moving_average:Float64;N]
                    Filter: NOT moving_average IS NULL [time:Timestamp(Nanosecond, None);N, moving_average:Float64;N]
                      Projection: time, moving_average(AVG(cpu.usage_idle),Int64(3)) AS moving_average [time:Timestamp(Nanosecond, None);N, moving_average:Float64;N]
                        WindowAggr: windowExpr=[[moving_average(AVG(cpu.usage_idle), Int64(3)) ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS moving_average(AVG(cpu.usage_idle),Int64(3))]] [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N, moving_average(AVG(cpu.usage_idle),Int64(3)):Float64;N]
                          GapFill: groupBy=[time], aggr=[[AVG(cpu.usage_idle)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N]
                            Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), cpu.time, TimestampNanosecond(0, None)) AS time]], aggr=[[AVG(cpu.usage_idle)]] [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N]
                              Filter: cpu.time <= TimestampNanosecond(1672531200000000000, None) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

                // Invariant: second argument is always a constant
                assert_snapshot!(plan("SELECT MOVING_AVERAGE(MEAN(usage_idle), usage_system) FROM cpu GROUP BY TIME(10s)"), @"Error during planning: expected integer argument in moving_average()");
            }

            #[test]
            fn test_derivative() {
                // no aggregates
                assert_snapshot!(plan("SELECT DERIVATIVE(usage_idle) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), derivative:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, derivative [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), derivative:Float64;N]
                    Filter: NOT derivative IS NULL [time:Timestamp(Nanosecond, None), derivative:Float64;N]
                      Projection: cpu.time AS time, derivative(cpu.usage_idle) AS derivative [time:Timestamp(Nanosecond, None), derivative:Float64;N]
                        WindowAggr: windowExpr=[[derivative(cpu.usage_idle, IntervalMonthDayNano("1000000000"), cpu.time) ORDER BY [cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS derivative(cpu.usage_idle)]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, derivative(cpu.usage_idle):Float64;N]
                          TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

                // aggregate
                assert_snapshot!(plan("SELECT DERIVATIVE(MEAN(usage_idle)) FROM cpu GROUP BY TIME(10s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, derivative:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, derivative [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, derivative:Float64;N]
                    Filter: NOT derivative IS NULL [time:Timestamp(Nanosecond, None);N, derivative:Float64;N]
                      Projection: time, derivative(AVG(cpu.usage_idle)) AS derivative [time:Timestamp(Nanosecond, None);N, derivative:Float64;N]
                        WindowAggr: windowExpr=[[derivative(AVG(cpu.usage_idle), IntervalMonthDayNano("10000000000"), time) ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS derivative(AVG(cpu.usage_idle))]] [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N, derivative(AVG(cpu.usage_idle)):Float64;N]
                          GapFill: groupBy=[time], aggr=[[AVG(cpu.usage_idle)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N]
                            Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), cpu.time, TimestampNanosecond(0, None)) AS time]], aggr=[[AVG(cpu.usage_idle)]] [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N]
                              Filter: cpu.time <= TimestampNanosecond(1672531200000000000, None) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }

            #[test]
            fn test_non_negative_derivative() {
                // no aggregates
                assert_snapshot!(plan("SELECT NON_NEGATIVE_DERIVATIVE(usage_idle) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), non_negative_derivative:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, non_negative_derivative [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), non_negative_derivative:Float64;N]
                    Filter: NOT non_negative_derivative IS NULL [time:Timestamp(Nanosecond, None), non_negative_derivative:Float64;N]
                      Projection: cpu.time AS time, non_negative_derivative(cpu.usage_idle) AS non_negative_derivative [time:Timestamp(Nanosecond, None), non_negative_derivative:Float64;N]
                        WindowAggr: windowExpr=[[non_negative_derivative(cpu.usage_idle, IntervalMonthDayNano("1000000000"), cpu.time) ORDER BY [cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS non_negative_derivative(cpu.usage_idle)]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, non_negative_derivative(cpu.usage_idle):Float64;N]
                          TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

                // aggregate
                assert_snapshot!(plan("SELECT NON_NEGATIVE_DERIVATIVE(MEAN(usage_idle)) FROM cpu GROUP BY TIME(10s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, non_negative_derivative:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, non_negative_derivative [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, non_negative_derivative:Float64;N]
                    Filter: NOT non_negative_derivative IS NULL [time:Timestamp(Nanosecond, None);N, non_negative_derivative:Float64;N]
                      Projection: time, non_negative_derivative(AVG(cpu.usage_idle)) AS non_negative_derivative [time:Timestamp(Nanosecond, None);N, non_negative_derivative:Float64;N]
                        WindowAggr: windowExpr=[[non_negative_derivative(AVG(cpu.usage_idle), IntervalMonthDayNano("10000000000"), time) ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS non_negative_derivative(AVG(cpu.usage_idle))]] [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N, non_negative_derivative(AVG(cpu.usage_idle)):Float64;N]
                          GapFill: groupBy=[time], aggr=[[AVG(cpu.usage_idle)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N]
                            Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), cpu.time, TimestampNanosecond(0, None)) AS time]], aggr=[[AVG(cpu.usage_idle)]] [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N]
                              Filter: cpu.time <= TimestampNanosecond(1672531200000000000, None) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

                // selector
                assert_snapshot!(plan("SELECT NON_NEGATIVE_DERIVATIVE(LAST(usage_idle)) FROM cpu GROUP BY TIME(10s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, non_negative_derivative:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, non_negative_derivative [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, non_negative_derivative:Float64;N]
                    Filter: NOT non_negative_derivative IS NULL [time:Timestamp(Nanosecond, None);N, non_negative_derivative:Float64;N]
                      Projection: time, non_negative_derivative(selector_last(cpu.usage_idle,cpu.time)[value]) AS non_negative_derivative [time:Timestamp(Nanosecond, None);N, non_negative_derivative:Float64;N]
                        WindowAggr: windowExpr=[[non_negative_derivative((selector_last(cpu.usage_idle,cpu.time))[value], IntervalMonthDayNano("10000000000"), time) ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS non_negative_derivative(selector_last(cpu.usage_idle,cpu.time)[value])]] [time:Timestamp(Nanosecond, None);N, selector_last(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N, non_negative_derivative(selector_last(cpu.usage_idle,cpu.time)[value]):Float64;N]
                          GapFill: groupBy=[time], aggr=[[selector_last(cpu.usage_idle,cpu.time)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, selector_last(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                            Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), cpu.time, TimestampNanosecond(0, None)) AS time]], aggr=[[selector_last(cpu.usage_idle, cpu.time)]] [time:Timestamp(Nanosecond, None);N, selector_last(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                              Filter: cpu.time <= TimestampNanosecond(1672531200000000000, None) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }

            #[test]
            fn test_cumulative_sum() {
                // no aggregates
                assert_snapshot!(plan("SELECT CUMULATIVE_SUM(usage_idle) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cumulative_sum:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, cumulative_sum [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cumulative_sum:Float64;N]
                    Filter: NOT cumulative_sum IS NULL [time:Timestamp(Nanosecond, None), cumulative_sum:Float64;N]
                      Projection: cpu.time AS time, cumulative_sum(cpu.usage_idle) AS cumulative_sum [time:Timestamp(Nanosecond, None), cumulative_sum:Float64;N]
                        WindowAggr: windowExpr=[[cumumlative_sum(cpu.usage_idle) ORDER BY [cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS cumulative_sum(cpu.usage_idle)]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, cumulative_sum(cpu.usage_idle):Float64;N]
                          TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

                // aggregate
                assert_snapshot!(plan("SELECT CUMULATIVE_SUM(MEAN(usage_idle)) FROM cpu GROUP BY TIME(10s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, cumulative_sum:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, cumulative_sum [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, cumulative_sum:Float64;N]
                    Filter: NOT cumulative_sum IS NULL [time:Timestamp(Nanosecond, None);N, cumulative_sum:Float64;N]
                      Projection: time, cumulative_sum(AVG(cpu.usage_idle)) AS cumulative_sum [time:Timestamp(Nanosecond, None);N, cumulative_sum:Float64;N]
                        WindowAggr: windowExpr=[[cumumlative_sum(AVG(cpu.usage_idle)) ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS cumulative_sum(AVG(cpu.usage_idle))]] [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N, cumulative_sum(AVG(cpu.usage_idle)):Float64;N]
                          GapFill: groupBy=[time], aggr=[[AVG(cpu.usage_idle)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N]
                            Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), cpu.time, TimestampNanosecond(0, None)) AS time]], aggr=[[AVG(cpu.usage_idle)]] [time:Timestamp(Nanosecond, None);N, AVG(cpu.usage_idle):Float64;N]
                              Filter: cpu.time <= TimestampNanosecond(1672531200000000000, None) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }

            #[test]
            fn test_not_implemented() {
                assert_snapshot!(plan("SELECT DIFFERENCE(MEAN(usage_idle)), MEAN(usage_idle) FROM cpu GROUP BY TIME(10s)"), @"This feature is not implemented: mixed window-aggregate and aggregate columns, such as DIFFERENCE(MEAN(col)), MEAN(col)");
            }
        }

        /// Tests for the `DISTINCT` clause and `DISTINCT` function
        #[test]
        fn test_distinct() {
            assert_snapshot!(plan("SELECT DISTINCT usage_idle FROM cpu"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), distinct:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, distinct [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), distinct:Float64;N]
                Distinct: [time:Timestamp(Nanosecond, None), distinct:Float64;N]
                  Projection: TimestampNanosecond(0, None) AS time, cpu.usage_idle AS distinct [time:Timestamp(Nanosecond, None), distinct:Float64;N]
                    TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT DISTINCT(usage_idle) FROM cpu"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), distinct:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, distinct [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), distinct:Float64;N]
                Distinct: [time:Timestamp(Nanosecond, None), distinct:Float64;N]
                  Projection: TimestampNanosecond(0, None) AS time, cpu.usage_idle AS distinct [time:Timestamp(Nanosecond, None), distinct:Float64;N]
                    TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT DISTINCT usage_idle FROM cpu GROUP BY cpu"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, distinct:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, cpu, distinct [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, distinct:Float64;N]
                Distinct: [time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, distinct:Float64;N]
                  Projection: TimestampNanosecond(0, None) AS time, cpu.cpu AS cpu, cpu.usage_idle AS distinct [time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, distinct:Float64;N]
                    TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT COUNT(DISTINCT usage_idle) FROM cpu"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), count:Int64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, TimestampNanosecond(0, None) AS time, coalesce_struct(COUNT(DISTINCT cpu.usage_idle), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), count:Int64;N]
                Aggregate: groupBy=[[]], aggr=[[COUNT(DISTINCT cpu.usage_idle)]] [COUNT(DISTINCT cpu.usage_idle):Int64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            // fallible
            assert_snapshot!(plan("SELECT DISTINCT(usage_idle), DISTINCT(usage_system) FROM cpu"), @"Error during planning: aggregate function distinct() cannot be combined with other functions or fields");
            assert_snapshot!(plan("SELECT DISTINCT(usage_idle), usage_system FROM cpu"), @"Error during planning: aggregate function distinct() cannot be combined with other functions or fields");
        }

        mod functions {
            use super::*;

            #[test]
            fn test_selectors_query() {
                // single-selector query
                assert_snapshot!(plan("SELECT LAST(usage_idle) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, (selector_last(cpu.usage_idle,cpu.time))[time] AS time, (selector_last(cpu.usage_idle,cpu.time))[value] AS last [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N]
                    Aggregate: groupBy=[[]], aggr=[[selector_last(cpu.usage_idle, cpu.time)]] [selector_last(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }
            #[test]
            fn test_selectors_query_grouping() {
                // single-selector, grouping by tags
                assert_snapshot!(plan("SELECT LAST(usage_idle) FROM cpu GROUP BY cpu"), @r###"
                Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, cpu:Dictionary(Int32, Utf8);N, last:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, (selector_last(cpu.usage_idle,cpu.time))[time] AS time, cpu.cpu AS cpu, (selector_last(cpu.usage_idle,cpu.time))[value] AS last [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, cpu:Dictionary(Int32, Utf8);N, last:Float64;N]
                    Aggregate: groupBy=[[cpu.cpu]], aggr=[[selector_last(cpu.usage_idle, cpu.time)]] [cpu:Dictionary(Int32, Utf8);N, selector_last(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }
            #[test]
            fn test_selectors_aggregate_gby_time() {
                // aggregate query, as we're grouping by time
                assert_snapshot!(plan("SELECT LAST(usage_idle) FROM cpu GROUP BY TIME(5s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, (selector_last(cpu.usage_idle,cpu.time))[value] AS last [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N]
                    GapFill: groupBy=[time], aggr=[[selector_last(cpu.usage_idle,cpu.time)]], time_column=time, stride=IntervalMonthDayNano("5000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, selector_last(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                      Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("5000000000"), cpu.time, TimestampNanosecond(0, None)) AS time]], aggr=[[selector_last(cpu.usage_idle, cpu.time)]] [time:Timestamp(Nanosecond, None);N, selector_last(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                        Filter: cpu.time <= TimestampNanosecond(1672531200000000000, None) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                          TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }
            #[test]
            fn test_selectors_aggregate_gby_time_gapfill() {
                // aggregate query, grouping by time with gap filling
                assert_snapshot!(plan("SELECT FIRST(usage_idle) FROM cpu GROUP BY TIME(5s) FILL(0)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, first:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, (coalesce_struct(selector_first(cpu.usage_idle,cpu.time), Struct({value:Float64(0),time:TimestampNanosecond(0, None)})))[value] AS first [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, first:Float64;N]
                    GapFill: groupBy=[time], aggr=[[selector_first(cpu.usage_idle,cpu.time)]], time_column=time, stride=IntervalMonthDayNano("5000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, selector_first(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                      Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("5000000000"), cpu.time, TimestampNanosecond(0, None)) AS time]], aggr=[[selector_first(cpu.usage_idle, cpu.time)]] [time:Timestamp(Nanosecond, None);N, selector_first(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                        Filter: cpu.time <= TimestampNanosecond(1672531200000000000, None) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                          TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }
            #[test]
            fn test_selectors_aggregate_multi_selectors() {
                // aggregate query, as we're specifying multiple selectors or aggregates
                assert_snapshot!(plan("SELECT LAST(usage_idle), FIRST(usage_idle) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), last:Float64;N, first:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, TimestampNanosecond(0, None) AS time, (selector_last(cpu.usage_idle,cpu.time))[value] AS last, (selector_first(cpu.usage_idle,cpu.time))[value] AS first [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), last:Float64;N, first:Float64;N]
                    Aggregate: groupBy=[[]], aggr=[[selector_last(cpu.usage_idle, cpu.time), selector_first(cpu.usage_idle, cpu.time)]] [selector_last(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N, selector_first(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }
            #[test]
            fn test_selectors_and_aggregate() {
                assert_snapshot!(plan("SELECT LAST(usage_idle), COUNT(usage_idle) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), last:Float64;N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, TimestampNanosecond(0, None) AS time, (selector_last(cpu.usage_idle,cpu.time))[value] AS last, coalesce_struct(COUNT(cpu.usage_idle), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), last:Float64;N, count:Int64;N]
                    Aggregate: groupBy=[[]], aggr=[[selector_last(cpu.usage_idle, cpu.time), COUNT(cpu.usage_idle)]] [selector_last(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N, COUNT(cpu.usage_idle):Int64;N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }
            #[test]
            fn test_selectors_additional_fields() {
                // additional fields
                assert_snapshot!(plan("SELECT LAST(usage_idle), usage_system FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N, usage_system:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, (selector_last(cpu.usage_idle,cpu.time,cpu.usage_system))[time] AS time, (selector_last(cpu.usage_idle,cpu.time,cpu.usage_system))[value] AS last, (selector_last(cpu.usage_idle,cpu.time,cpu.usage_system))[other_1] AS usage_system [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N, usage_system:Float64;N]
                    Aggregate: groupBy=[[]], aggr=[[selector_last(cpu.usage_idle, cpu.time, cpu.usage_system)]] [selector_last(cpu.usage_idle,cpu.time,cpu.usage_system):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "other_1", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }
            #[test]
            fn test_selectors_additional_fields_2() {
                assert_snapshot!(plan("SELECT LAST(usage_idle), usage_system FROM cpu GROUP BY TIME(5s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N, usage_system:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, (selector_last(cpu.usage_idle,cpu.time,cpu.usage_system))[value] AS last, (selector_last(cpu.usage_idle,cpu.time,cpu.usage_system))[other_1] AS usage_system [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N, usage_system:Float64;N]
                    GapFill: groupBy=[time], aggr=[[selector_last(cpu.usage_idle,cpu.time,cpu.usage_system)]], time_column=time, stride=IntervalMonthDayNano("5000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, selector_last(cpu.usage_idle,cpu.time,cpu.usage_system):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "other_1", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                      Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("5000000000"), cpu.time, TimestampNanosecond(0, None)) AS time]], aggr=[[selector_last(cpu.usage_idle, cpu.time, cpu.usage_system)]] [time:Timestamp(Nanosecond, None);N, selector_last(cpu.usage_idle,cpu.time,cpu.usage_system):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "other_1", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                        Filter: cpu.time <= TimestampNanosecond(1672531200000000000, None) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                          TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }
            #[test]
            fn test_selectors_additional_fields_3() {
                assert_snapshot!(plan("SELECT LAST(usage_idle), usage_system FROM cpu GROUP BY TIME(5s) FILL(0)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N, usage_system:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, (coalesce_struct(selector_last(cpu.usage_idle,cpu.time,cpu.usage_system), Struct({value:Float64(0),time:TimestampNanosecond(0, None),other_1:Float64(0)})))[value] AS last, (coalesce_struct(selector_last(cpu.usage_idle,cpu.time,cpu.usage_system), Struct({value:Float64(0),time:TimestampNanosecond(0, None),other_1:Float64(0)})))[other_1] AS usage_system [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N, usage_system:Float64;N]
                    GapFill: groupBy=[time], aggr=[[selector_last(cpu.usage_idle,cpu.time,cpu.usage_system)]], time_column=time, stride=IntervalMonthDayNano("5000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, selector_last(cpu.usage_idle,cpu.time,cpu.usage_system):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "other_1", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                      Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("5000000000"), cpu.time, TimestampNanosecond(0, None)) AS time]], aggr=[[selector_last(cpu.usage_idle, cpu.time, cpu.usage_system)]] [time:Timestamp(Nanosecond, None);N, selector_last(cpu.usage_idle,cpu.time,cpu.usage_system):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "other_1", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                        Filter: cpu.time <= TimestampNanosecond(1672531200000000000, None) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                          TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }
            #[test]
            fn test_selectors_additional_fields_4() {
                assert_snapshot!(plan("SELECT FIRST(f), first FROM name_clash"), @r###"
                Sort: time ASC NULLS LAST, first_1 ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, first:Float64;N, first_1:Dictionary(Int32, Utf8);N]
                  Projection: Dictionary(Int32, Utf8("name_clash")) AS iox::measurement, (selector_first(name_clash.f,name_clash.time,name_clash.first))[time] AS time, (selector_first(name_clash.f,name_clash.time,name_clash.first))[value] AS first, (selector_first(name_clash.f,name_clash.time,name_clash.first))[other_1] AS first_1 [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, first:Float64;N, first_1:Dictionary(Int32, Utf8);N]
                    Aggregate: groupBy=[[]], aggr=[[selector_first(name_clash.f, name_clash.time, name_clash.first)]] [selector_first(name_clash.f,name_clash.time,name_clash.first):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "other_1", data_type: Dictionary(Int32, Utf8), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                      TableScan: name_clash [f:Float64;N, first:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                "###);
            }
            #[test]
            fn test_selectors_query_other_other_selectors_1() {
                // Validate we can call the remaining supported selector functions
                assert_snapshot!(plan("SELECT FIRST(usage_idle) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, first:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, (selector_first(cpu.usage_idle,cpu.time))[time] AS time, (selector_first(cpu.usage_idle,cpu.time))[value] AS first [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, first:Float64;N]
                    Aggregate: groupBy=[[]], aggr=[[selector_first(cpu.usage_idle, cpu.time)]] [selector_first(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }
            #[test]
            fn test_selectors_query_other_other_selectors_2() {
                assert_snapshot!(plan("SELECT MAX(usage_idle) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, max:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, (selector_max(cpu.usage_idle,cpu.time))[time] AS time, (selector_max(cpu.usage_idle,cpu.time))[value] AS max [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, max:Float64;N]
                    Aggregate: groupBy=[[]], aggr=[[selector_max(cpu.usage_idle, cpu.time)]] [selector_max(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }
            #[test]
            fn test_selectors_query_other_other_selectors_3() {
                assert_snapshot!(plan("SELECT MIN(usage_idle) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, min:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, (selector_min(cpu.usage_idle,cpu.time))[time] AS time, (selector_min(cpu.usage_idle,cpu.time))[value] AS min [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, min:Float64;N]
                    Aggregate: groupBy=[[]], aggr=[[selector_min(cpu.usage_idle, cpu.time)]] [selector_min(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }

            #[test]
            fn test_selectors_invalid_arguments_3() {
                // Invalid number of arguments
                assert_snapshot!(plan("SELECT MIN(usage_idle, usage_idle) FROM cpu"), @"Error during planning: invalid number of arguments for min, expected 1, got 2");
            }
        }

        #[test]
        fn test_percentile() {
            assert_snapshot!(plan("SELECT percentile(usage_idle,50),usage_system FROM cpu"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), percentile:Float64;N, usage_system:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.usage_idle AS percentile, cpu.usage_system AS usage_system [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), percentile:Float64;N, usage_system:Float64;N]
                Filter: percent_row_number(Float64(50)) ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING = ROW_NUMBER() ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, percent_row_number(Float64(50)) ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING:UInt64;N, ROW_NUMBER() ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING:UInt64;N]
                  WindowAggr: windowExpr=[[percent_row_number(Float64(50)) ORDER BY [cpu.usage_idle ASC NULLS LAST, cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS percent_row_number(Float64(50)) ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, ROW_NUMBER() ORDER BY [cpu.usage_idle ASC NULLS LAST, cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS ROW_NUMBER() ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, percent_row_number(Float64(50)) ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING:UInt64;N, ROW_NUMBER() ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING:UInt64;N]
                    Filter: cpu.usage_idle IS NOT NULL [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            assert_snapshot!(plan("SELECT percentile(usage_idle,50),usage_system FROM cpu WHERE time >= 0 AND time < 60000000000 GROUP BY cpu"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, percentile:Float64;N, usage_system:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.cpu AS cpu, cpu.usage_idle AS percentile, cpu.usage_system AS usage_system [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, percentile:Float64;N, usage_system:Float64;N]
                Filter: percent_row_number(Float64(50)) PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING = ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, percent_row_number(Float64(50)) PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING:UInt64;N, ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING:UInt64;N]
                  WindowAggr: windowExpr=[[percent_row_number(Float64(50)) PARTITION BY [cpu.cpu] ORDER BY [cpu.usage_idle ASC NULLS LAST, cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS percent_row_number(Float64(50)) PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, ROW_NUMBER() PARTITION BY [cpu.cpu] ORDER BY [cpu.usage_idle ASC NULLS LAST, cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, percent_row_number(Float64(50)) PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING:UInt64;N, ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING:UInt64;N]
                    Filter: cpu.usage_idle IS NOT NULL [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                      Filter: cpu.time >= TimestampNanosecond(0, None) AND cpu.time <= TimestampNanosecond(59999999999, None) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                        TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            assert_snapshot!(plan("SELECT percentile(usage_idle,50), percentile(usage_idle,90) FROM cpu"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), percentile:Float64;N, percentile_1:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, TimestampNanosecond(0, None) AS time, percentile(cpu.usage_idle,Int64(50)) AS percentile, percentile(cpu.usage_idle,Int64(90)) AS percentile_1 [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), percentile:Float64;N, percentile_1:Float64;N]
                Aggregate: groupBy=[[]], aggr=[[percentile(cpu.usage_idle, Int64(50)), percentile(cpu.usage_idle, Int64(90))]] [percentile(cpu.usage_idle,Int64(50)):Float64;N, percentile(cpu.usage_idle,Int64(90)):Float64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            assert_snapshot!(plan("SELECT percentile(usage_idle,50), percentile(usage_idle,90) FROM cpu GROUP BY cpu"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, percentile:Float64;N, percentile_1:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, TimestampNanosecond(0, None) AS time, cpu.cpu AS cpu, percentile(cpu.usage_idle,Int64(50)) AS percentile, percentile(cpu.usage_idle,Int64(90)) AS percentile_1 [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, percentile:Float64;N, percentile_1:Float64;N]
                Aggregate: groupBy=[[cpu.cpu]], aggr=[[percentile(cpu.usage_idle, Int64(50)), percentile(cpu.usage_idle, Int64(90))]] [cpu:Dictionary(Int32, Utf8);N, percentile(cpu.usage_idle,Int64(50)):Float64;N, percentile(cpu.usage_idle,Int64(90)):Float64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            assert_snapshot!(plan("SELECT percentile(usage_idle,50), percentile(usage_idle,90) FROM cpu GROUP BY cpu"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, percentile:Float64;N, percentile_1:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, TimestampNanosecond(0, None) AS time, cpu.cpu AS cpu, percentile(cpu.usage_idle,Int64(50)) AS percentile, percentile(cpu.usage_idle,Int64(90)) AS percentile_1 [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, percentile:Float64;N, percentile_1:Float64;N]
                Aggregate: groupBy=[[cpu.cpu]], aggr=[[percentile(cpu.usage_idle, Int64(50)), percentile(cpu.usage_idle, Int64(90))]] [cpu:Dictionary(Int32, Utf8);N, percentile(cpu.usage_idle,Int64(50)):Float64;N, percentile(cpu.usage_idle,Int64(90)):Float64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            assert_snapshot!(plan("SELECT percentile(usage_idle,50), percentile(usage_idle,90) FROM cpu WHERE time >= 0 AND time < 60000000000 GROUP BY time(10s), cpu"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, cpu:Dictionary(Int32, Utf8);N, percentile:Float64;N, percentile_1:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, cpu.cpu AS cpu, percentile(cpu.usage_idle,Int64(50)) AS percentile, percentile(cpu.usage_idle,Int64(90)) AS percentile_1 [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, cpu:Dictionary(Int32, Utf8);N, percentile:Float64;N, percentile_1:Float64;N]
                GapFill: groupBy=[time, cpu.cpu], aggr=[[percentile(cpu.usage_idle,Int64(50)), percentile(cpu.usage_idle,Int64(90))]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Included(Literal(TimestampNanosecond(0, None)))..Included(Literal(TimestampNanosecond(59999999999, None))) [time:Timestamp(Nanosecond, None);N, cpu:Dictionary(Int32, Utf8);N, percentile(cpu.usage_idle,Int64(50)):Float64;N, percentile(cpu.usage_idle,Int64(90)):Float64;N]
                  Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), cpu.time, TimestampNanosecond(0, None)) AS time, cpu.cpu]], aggr=[[percentile(cpu.usage_idle, Int64(50)), percentile(cpu.usage_idle, Int64(90))]] [time:Timestamp(Nanosecond, None);N, cpu:Dictionary(Int32, Utf8);N, percentile(cpu.usage_idle,Int64(50)):Float64;N, percentile(cpu.usage_idle,Int64(90)):Float64;N]
                    Filter: cpu.time >= TimestampNanosecond(0, None) AND cpu.time <= TimestampNanosecond(59999999999, None) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
        }

        #[test]
        fn test_top() {
            assert_snapshot!(plan("SELECT top(usage_idle,10) FROM cpu"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), top:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.usage_idle AS top [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), top:Float64;N]
                Filter: ROW_NUMBER() ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW <= Int64(10) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() ORDER BY [cpu.usage_idle DESC NULLS LAST, cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS ROW_NUMBER() ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                    TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

            assert_snapshot!(plan("SELECT top(usage_idle,10),cpu FROM cpu"), @r###"
            Sort: time ASC NULLS LAST, cpu ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), top:Float64;N, cpu:Dictionary(Int32, Utf8);N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.usage_idle AS top, cpu.cpu AS cpu [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), top:Float64;N, cpu:Dictionary(Int32, Utf8);N]
                Filter: ROW_NUMBER() ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW <= Int64(10) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() ORDER BY [cpu.usage_idle DESC NULLS LAST, cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS ROW_NUMBER() ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                    TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

            assert_snapshot!(plan("SELECT top(usage_idle,10) FROM cpu GROUP BY cpu"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, top:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.cpu AS cpu, cpu.usage_idle AS top [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, top:Float64;N]
                Filter: ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW <= Int64(10) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [cpu.cpu] ORDER BY [cpu.usage_idle DESC NULLS LAST, cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                    TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

            assert_snapshot!(plan("SELECT top(usage_idle,cpu,10) FROM cpu"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), top:Float64;N, cpu:Dictionary(Int32, Utf8);N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.usage_idle AS top, cpu.cpu AS cpu [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), top:Float64;N, cpu:Dictionary(Int32, Utf8);N]
                Filter: ROW_NUMBER() ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW <= Int64(10) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N, ROW_NUMBER() ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() ORDER BY [cpu.usage_idle DESC NULLS LAST, cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS ROW_NUMBER() ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N, ROW_NUMBER() ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                    Filter: ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW <= Int64(1) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                      WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [cpu.cpu] ORDER BY [cpu.usage_idle DESC NULLS LAST, cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle DESC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                        TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
        }

        #[test]
        fn test_bottom() {
            assert_snapshot!(plan("SELECT bottom(usage_idle,10) FROM cpu"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bottom:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.usage_idle AS bottom [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bottom:Float64;N]
                Filter: ROW_NUMBER() ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW <= Int64(10) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() ORDER BY [cpu.usage_idle ASC NULLS LAST, cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS ROW_NUMBER() ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                    TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

            assert_snapshot!(plan("SELECT bottom(usage_idle,10),cpu FROM cpu"), @r###"
            Sort: time ASC NULLS LAST, cpu ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bottom:Float64;N, cpu:Dictionary(Int32, Utf8);N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.usage_idle AS bottom, cpu.cpu AS cpu [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bottom:Float64;N, cpu:Dictionary(Int32, Utf8);N]
                Filter: ROW_NUMBER() ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW <= Int64(10) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() ORDER BY [cpu.usage_idle ASC NULLS LAST, cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS ROW_NUMBER() ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                    TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

            assert_snapshot!(plan("SELECT bottom(usage_idle,10) FROM cpu GROUP BY cpu"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, bottom:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.cpu AS cpu, cpu.usage_idle AS bottom [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, bottom:Float64;N]
                Filter: ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW <= Int64(10) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [cpu.cpu] ORDER BY [cpu.usage_idle ASC NULLS LAST, cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                    TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

            assert_snapshot!(plan("SELECT bottom(usage_idle,cpu,10) FROM cpu"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bottom:Float64;N, cpu:Dictionary(Int32, Utf8);N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.usage_idle AS bottom, cpu.cpu AS cpu [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bottom:Float64;N, cpu:Dictionary(Int32, Utf8);N]
                Filter: ROW_NUMBER() ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW <= Int64(10) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N, ROW_NUMBER() ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() ORDER BY [cpu.usage_idle ASC NULLS LAST, cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS ROW_NUMBER() ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N, ROW_NUMBER() ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                    Filter: ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW <= Int64(1) [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                      WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [cpu.cpu] ORDER BY [cpu.usage_idle ASC NULLS LAST, cpu.time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]] [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N, ROW_NUMBER() PARTITION BY [cpu] ORDER BY [usage_idle ASC NULLS LAST, time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64;N]
                        TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
        }

        /// Test InfluxQL-specific behaviour of scalar functions that differ
        /// from DataFusion
        #[test]
        fn test_scalar_functions() {
            // LOG requires two arguments, and first argument is field
            assert_snapshot!(plan("SELECT LOG(usage_idle, 8) FROM cpu"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), log:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, log(Int64(8), cpu.usage_idle) AS log [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), log:Float64;N]
                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            // Fallible

            // LOG requires two arguments
            assert_snapshot!(plan("SELECT LOG(usage_idle) FROM cpu"), @"Error during planning: invalid number of arguments for log, expected 2, got 1");
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
            assert_snapshot!(plan("SELECT host, cpu, device, usage_idle, bytes_used FROM cpu, disk"), @r###"
            Sort: iox::measurement ASC NULLS LAST, time ASC NULLS LAST, cpu ASC NULLS LAST, device ASC NULLS LAST, host ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, cpu:Utf8;N, device:Utf8;N, usage_idle:Float64;N, bytes_used:Int64;N]
              Union [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, cpu:Utf8;N, device:Utf8;N, usage_idle:Float64;N, bytes_used:Int64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.host AS host, CAST(cpu.cpu AS Utf8) AS cpu, CAST(NULL AS Utf8) AS device, cpu.usage_idle AS usage_idle, CAST(NULL AS Int64) AS bytes_used [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, cpu:Utf8;N, device:Utf8;N, usage_idle:Float64;N, bytes_used:Int64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                Projection: Dictionary(Int32, Utf8("disk")) AS iox::measurement, disk.time AS time, disk.host AS host, CAST(NULL AS Utf8) AS cpu, CAST(disk.device AS Utf8) AS device, CAST(NULL AS Float64) AS usage_idle, disk.bytes_used AS bytes_used [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, cpu:Utf8;N, device:Utf8;N, usage_idle:Float64;N, bytes_used:Int64;N]
                  TableScan: disk [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
            "###);

            // nonexistent
            assert_snapshot!(plan("SELECT host, usage_idle FROM non_existent"), @"EmptyRelation [iox::measurement:Dictionary(Int32, Utf8)]");
            assert_snapshot!(plan("SELECT host, usage_idle FROM cpu, non_existent"), @r###"
            Sort: time ASC NULLS LAST, host ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.host AS host, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            // multiple of same measurement
            assert_snapshot!(plan("SELECT host, usage_idle FROM cpu, cpu"), @r###"
            Sort: time AS time ASC NULLS LAST, host AS host ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time AS time, host AS host, usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                Union [host:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N]
                  Projection: cpu.host AS host, cpu.time AS time, cpu.usage_idle AS usage_idle [host:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N]
                    TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                  Projection: cpu.host AS host, cpu.time AS time, cpu.usage_idle AS usage_idle [host:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N]
                    TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
        }

        #[test]
        fn test_time_range_in_where() {
            assert_snapshot!(
                plan("SELECT foo, f64_field FROM data where time > now() - 10s"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: data.time >= TimestampNanosecond(1672531190000000001, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###
            );
            assert_snapshot!(
                plan("SELECT foo, f64_field FROM data where time > '2004-04-09T02:33:45Z'"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: data.time >= TimestampNanosecond(1081478025000000001, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###
            );
            assert_snapshot!(
                plan("SELECT foo, f64_field FROM data where time > '2004-04-09T'"), @r###"Error during planning: invalid expression "'2004-04-09T'": '2004-04-09T' is not a valid timestamp"###
            );

            // time on the right-hand side
            assert_snapshot!(
                plan("SELECT foo, f64_field FROM data where  now() - 10s < time"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: data.time >= TimestampNanosecond(1672531190000000001, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###
            );

            // fallible

            // Unsupported operator
            assert_snapshot!(plan("SELECT foo, f64_field FROM data where time != 0"), @"Error during planning: invalid time comparison operator: !=")
        }

        #[test]
        fn test_regex_in_where() {
            test_helpers::maybe_start_logging();
            // Regular expression equality tests

            assert_snapshot!(plan("SELECT foo, f64_field FROM data where foo =~ /f/"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: CAST(data.foo AS Utf8) LIKE Utf8("%f%") [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

            // regular expression for a numeric field is rewritten to `false`
            assert_snapshot!(plan("SELECT foo, f64_field FROM data where f64_field =~ /f/"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: Boolean(false) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

            // regular expression for a non-existent field is rewritten to `false`
            assert_snapshot!(
                plan("SELECT foo, f64_field FROM data where non_existent =~ /f/"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: Boolean(false) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###
            );

            // Regular expression inequality tests

            assert_snapshot!(plan("SELECT foo, f64_field FROM data where foo !~ /f/"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: data.foo IS NULL OR CAST(data.foo AS Utf8) NOT LIKE Utf8("%f%") [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

            // regular expression for a numeric field is rewritten to `false`
            assert_snapshot!(plan("SELECT foo, f64_field FROM data where f64_field !~ /f/"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: Boolean(false) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

            // regular expression for a non-existent field is rewritten to `false`
            assert_snapshot!(
                plan("SELECT foo, f64_field FROM data where non_existent !~ /f/"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: Boolean(false) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###
            );
        }

        #[test]
        fn test_column_matching_rules() {
            // Cast between numeric types
            assert_snapshot!(plan("SELECT f64_field::integer FROM data"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Int64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, CAST(data.f64_field AS Int64) AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Int64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT i64_field::float FROM data"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, CAST(data.i64_field AS Float64) AS i64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Float64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

            // use field selector
            assert_snapshot!(plan("SELECT bool_field::field FROM data"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Boolean;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.bool_field AS bool_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Boolean;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

            // invalid column reference
            assert_snapshot!(plan("SELECT not_exists::tag FROM data"), @"EmptyRelation [iox::measurement:Dictionary(Int32, Utf8)]");
            assert_snapshot!(plan("SELECT not_exists::field FROM data"), @"EmptyRelation [iox::measurement:Dictionary(Int32, Utf8)]");

            // Returns NULL for invalid casts
            assert_snapshot!(plan("SELECT f64_field::string FROM data"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Null;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, NULL AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Null;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT f64_field::boolean FROM data"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Null;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, NULL AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Null;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT str_field::boolean FROM data"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, NULL AS str_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
        }

        #[test]
        fn test_explain() {
            assert_snapshot!(plan("EXPLAIN SELECT foo, f64_field FROM data"), @r###"
            Explain [plan_type:Utf8, plan:Utf8]
              Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("EXPLAIN VERBOSE SELECT foo, f64_field FROM data"), @r###"
            Explain [plan_type:Utf8, plan:Utf8]
              Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("EXPLAIN ANALYZE SELECT foo, f64_field FROM data"), @r###"
            Analyze [plan_type:Utf8, plan:Utf8]
              Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("EXPLAIN ANALYZE VERBOSE SELECT foo, f64_field FROM data"), @r###"
            Analyze [plan_type:Utf8, plan:Utf8]
              Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
        }

        #[test]
        fn test_select_cast_postfix_operator() {
            // Float casting
            assert_snapshot!(plan("SELECT f64_field::float FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, all_types.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Float64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT f64_field::unsigned FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:UInt64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, CAST(all_types.f64_field AS UInt64) AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:UInt64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT f64_field::integer FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Int64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, CAST(all_types.f64_field AS Int64) AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Int64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT f64_field::string FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, NULL AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT f64_field::boolean FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, NULL AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);

            // Integer casting
            assert_snapshot!(plan("SELECT i64_field::float FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, CAST(all_types.i64_field AS Float64) AS i64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Float64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT i64_field::unsigned FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:UInt64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, CAST(all_types.i64_field AS UInt64) AS i64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:UInt64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT i64_field::integer FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Int64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, all_types.i64_field AS i64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Int64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT i64_field::string FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, NULL AS i64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT i64_field::boolean FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, NULL AS i64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);

            // Unsigned casting
            assert_snapshot!(plan("SELECT u64_field::float FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, CAST(all_types.u64_field AS Float64) AS u64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:Float64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT u64_field::unsigned FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, all_types.u64_field AS u64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT u64_field::integer FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:Int64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, CAST(all_types.u64_field AS Int64) AS u64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:Int64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT u64_field::string FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, NULL AS u64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT u64_field::boolean FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, NULL AS u64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);

            // String casting
            assert_snapshot!(plan("SELECT str_field::float FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, NULL AS str_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT str_field::unsigned FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, NULL AS str_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT str_field::integer FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, NULL AS str_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT str_field::string FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Utf8;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, all_types.str_field AS str_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Utf8;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT str_field::boolean FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, NULL AS str_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);

            // Boolean casting
            assert_snapshot!(plan("SELECT bool_field::float FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, NULL AS bool_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT bool_field::unsigned FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, NULL AS bool_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT bool_field::integer FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, NULL AS bool_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT bool_field::string FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, NULL AS bool_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT bool_field::boolean FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Boolean;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, all_types.bool_field AS bool_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Boolean;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);

            // Validate various projection expressions with casts

            assert_snapshot!(plan("SELECT f64_field::integer + i64_field + u64_field::integer FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field_i64_field_u64_field:Int64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, CAST(all_types.f64_field AS Int64) + all_types.i64_field + CAST(all_types.u64_field AS Int64) AS f64_field_i64_field_u64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field_i64_field_u64_field:Int64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);

            assert_snapshot!(plan("SELECT f64_field::integer + i64_field + str_field::integer FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field_i64_field_str_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, NULL AS f64_field_i64_field_str_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field_i64_field_str_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
        }

        /// Fix for https://github.com/influxdata/influxdb_iox/issues/8168
        #[test]
        fn test_integer_division_float_promotion() {
            assert_snapshot!(plan("SELECT i64_field / i64_field FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field_i64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time AS time, coalesce(CAST(all_types.i64_field AS Float64) / CAST(all_types.i64_field AS Float64), Float64(0)) AS i64_field_i64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field_i64_field:Float64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT sum(i64_field) / sum(i64_field) FROM all_types"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), sum_sum:Float64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, TimestampNanosecond(0, None) AS time, coalesce(CAST(SUM(all_types.i64_field) AS Float64) / CAST(SUM(all_types.i64_field) AS Float64), Float64(0)) AS sum_sum [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), sum_sum:Float64;N]
                Aggregate: groupBy=[[]], aggr=[[SUM(all_types.i64_field)]] [SUM(all_types.i64_field):Int64;N]
                  TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
        }
    }

    /// Tests to validate InfluxQL `SELECT` statements that project aggregate functions, such as `COUNT` or `SUM`.
    mod select_aggregate {
        use super::*;

        mod single_measurement {
            use super::*;

            #[test]
            fn no_group_by() {
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), count:Int64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), count:Int64;N]
                Aggregate: groupBy=[[]], aggr=[[COUNT(data.f64_field)]] [COUNT(data.f64_field):Int64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY non_existent"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), non_existent:Null;N, count:Int64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, NULL AS non_existent, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), non_existent:Null;N, count:Int64;N]
                Aggregate: groupBy=[[]], aggr=[[COUNT(data.f64_field)]] [COUNT(data.f64_field):Int64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY foo"), @r###"
            Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, data.foo AS foo, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                Aggregate: groupBy=[[data.foo]], aggr=[[COUNT(data.f64_field)]] [foo:Dictionary(Int32, Utf8);N, COUNT(data.f64_field):Int64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

                // The `COUNT(f64_field)` aggregate is only projected ones in the Aggregate and reused in the projection
                assert_snapshot!(plan("SELECT COUNT(f64_field), COUNT(f64_field) + COUNT(f64_field), COUNT(f64_field) * 3 FROM data"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), count:Int64;N, count_count:Int64;N, count_1:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count, coalesce_struct(COUNT(data.f64_field), Int64(0)) + coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count_count, coalesce_struct(COUNT(data.f64_field), Int64(0)) * Int64(3) AS count_1 [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), count:Int64;N, count_count:Int64;N, count_1:Int64;N]
                    Aggregate: groupBy=[[]], aggr=[[COUNT(data.f64_field)]] [COUNT(data.f64_field):Int64;N]
                      TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);

                // non-existent tags are excluded from the Aggregate groupBy and Sort operators
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY foo, non_existent"), @r###"
            Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, non_existent:Null;N, count:Int64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, data.foo AS foo, NULL AS non_existent, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, non_existent:Null;N, count:Int64;N]
                Aggregate: groupBy=[[data.foo]], aggr=[[COUNT(data.f64_field)]] [foo:Dictionary(Int32, Utf8);N, COUNT(data.f64_field):Int64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

                // Aggregate expression is projected once and reused in final projection
                assert_snapshot!(plan("SELECT COUNT(f64_field),  COUNT(f64_field) * 2 FROM data"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), count:Int64;N, count_1:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count, coalesce_struct(COUNT(data.f64_field), Int64(0)) * Int64(2) AS count_1 [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), count:Int64;N, count_1:Int64;N]
                    Aggregate: groupBy=[[]], aggr=[[COUNT(data.f64_field)]] [COUNT(data.f64_field):Int64;N]
                      TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);

                // Aggregate expression selecting non-existent field
                assert_snapshot!(plan("SELECT MEAN(f64_field) + MEAN(non_existent) FROM data"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), mean_mean:Null;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, NULL AS mean_mean [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), mean_mean:Null;N]
                    EmptyRelation []
                "###);

                // Aggregate expression with GROUP BY and non-existent field
                assert_snapshot!(plan("SELECT MEAN(f64_field) + MEAN(non_existent) FROM data GROUP BY foo"), @r###"
                Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, mean_mean:Null;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, data.foo AS foo, NULL AS mean_mean [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, mean_mean:Null;N]
                    Aggregate: groupBy=[[data.foo]], aggr=[[]] [foo:Dictionary(Int32, Utf8);N]
                      TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);

                // Aggregate expression selecting tag, should treat as non-existent
                assert_snapshot!(plan("SELECT MEAN(f64_field), MEAN(f64_field) + MEAN(non_existent) FROM data"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), mean:Float64;N, mean_mean:Null;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, AVG(data.f64_field) AS mean, NULL AS mean_mean [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), mean:Float64;N, mean_mean:Null;N]
                    Aggregate: groupBy=[[]], aggr=[[AVG(data.f64_field)]] [AVG(data.f64_field):Float64;N]
                      TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);

                // Fallible

                // Cannot combine aggregate and non-aggregate columns in the projection
                assert_snapshot!(plan("SELECT COUNT(f64_field), f64_field FROM data"), @"Error during planning: mixing aggregate and non-aggregate columns is not supported");
                assert_snapshot!(plan("SELECT COUNT(f64_field) + f64_field FROM data"), @"Error during planning: mixing aggregate and non-aggregate columns is not supported");
            }

            #[test]
            fn group_by_time() {
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10s) FILL(none)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Filter: data.time <= TimestampNanosecond(1672531200000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                        TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);

                // supports offset parameter
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10s, 5s) FILL(none)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(5000000000, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Filter: data.time <= TimestampNanosecond(1672531200000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                        TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
            }

            #[test]
            fn group_by_time_gapfill_no_bounds() {
                // No time bounds
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[time], aggr=[[COUNT(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        Filter: data.time <= TimestampNanosecond(1672531200000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                          TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
            }

            #[test]
            fn group_by_time_gapfill_no_lower_time_bounds() {
                // No lower time bounds
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data WHERE time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[time], aggr=[[COUNT(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1667181719999999999, None))) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        Filter: data.time <= TimestampNanosecond(1667181719999999999, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                          TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
            }

            #[test]
            fn group_by_time_gapfill_no_upper_time_bounds() {
                // No upper time bounds
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' GROUP BY TIME(10s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[time], aggr=[[COUNT(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Included(Literal(TimestampNanosecond(1667181600000000000, None)))..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        Filter: data.time >= TimestampNanosecond(1667181600000000000, None) AND data.time <= TimestampNanosecond(1672531200000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                          TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
            }

            #[test]
            fn group_by_time_gapfill_defaul_is_fill_null1() {
                // Default is FILL(null)
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[time], aggr=[[COUNT(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Included(Literal(TimestampNanosecond(1667181600000000000, None)))..Included(Literal(TimestampNanosecond(1667181719999999999, None))) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        Filter: data.time >= TimestampNanosecond(1667181600000000000, None) AND data.time <= TimestampNanosecond(1667181719999999999, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                          TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
            }

            #[test]
            fn group_by_time_gapfill_default_is_fill_null1() {
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[time], aggr=[[COUNT(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        Filter: data.time <= TimestampNanosecond(1672531200000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                          TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
            }

            #[test]
            fn group_by_time_gapfill_default_is_fill_null2() {
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10s) FILL(null)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[time], aggr=[[COUNT(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        Filter: data.time <= TimestampNanosecond(1672531200000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                          TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
            }

            #[test]
            fn group_by_time_gapfill_default_is_fill_null3() {
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10s) FILL(previous)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[time], aggr=[[LOCF(COUNT(data.f64_field))]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        Filter: data.time <= TimestampNanosecond(1672531200000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                          TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
            }

            #[test]
            fn group_by_time_gapfill_default_is_fill_null4() {
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10s) FILL(0)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[time], aggr=[[COUNT(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        Filter: data.time <= TimestampNanosecond(1672531200000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                          TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
            }

            #[test]
            fn group_by_time_gapfill_default_is_fill_null5() {
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10s) FILL(linear)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[time], aggr=[[INTERPOLATE(COUNT(data.f64_field))]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        Filter: data.time <= TimestampNanosecond(1672531200000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                          TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
            }

            #[test]
            fn group_by_time_gapfill_coalesces_the_fill_value() {
                // Coalesces the fill value, which is a float, to the matching type of a `COUNT` aggregate.
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10s) FILL(3.2)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, coalesce_struct(COUNT(data.f64_field), Int64(3)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[time], aggr=[[COUNT(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        Filter: data.time <= TimestampNanosecond(1672531200000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                          TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
            }

            #[test]
            fn group_by_time_gapfill_aggregates_part_of_binary_expression() {
                // Aggregates as part of a binary expression
                assert_snapshot!(plan("SELECT COUNT(f64_field) + MEAN(f64_field) FROM data GROUP BY TIME(10s) FILL(3.2)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count_mean:Float64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, coalesce_struct(COUNT(data.f64_field), Int64(3)) + coalesce_struct(AVG(data.f64_field), Float64(3.2)) AS count_mean [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count_mean:Float64;N]
                    GapFill: groupBy=[time], aggr=[[COUNT(data.f64_field), AVG(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Included(Literal(TimestampNanosecond(1672531200000000000, None))) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N, AVG(data.f64_field):Float64;N]
                      Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field), AVG(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N, AVG(data.f64_field):Float64;N]
                        Filter: data.time <= TimestampNanosecond(1672531200000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                          TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
            }

            #[test]
            fn with_limit_or_offset() {
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY foo LIMIT 1"), @r###"
                Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                  Projection: iox::measurement, time, foo, count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                    Filter: iox::row <= Int64(1) [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N, iox::row:UInt64;N]
                      WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [foo] ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS iox::row]] [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N, iox::row:UInt64;N]
                        Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                          Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, data.foo AS foo, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                            Aggregate: groupBy=[[data.foo]], aggr=[[COUNT(data.f64_field)]] [foo:Dictionary(Int32, Utf8);N, COUNT(data.f64_field):Int64;N]
                              TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
            }

            #[test]
            fn with_limit_or_offset2() {
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY foo OFFSET 1"), @r###"
                Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                  Projection: iox::measurement, time, foo, count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                    Filter: iox::row > Int64(1) [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N, iox::row:UInt64;N]
                      WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [foo] ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS iox::row]] [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N, iox::row:UInt64;N]
                        Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                          Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, data.foo AS foo, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                            Aggregate: groupBy=[[data.foo]], aggr=[[COUNT(data.f64_field)]] [foo:Dictionary(Int32, Utf8);N, COUNT(data.f64_field):Int64;N]
                              TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
            }

            #[test]
            fn with_limit_or_offset3() {
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY foo LIMIT 2 OFFSET 3"), @r###"
                Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                  Projection: iox::measurement, time, foo, count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                    Filter: iox::row BETWEEN Int64(4) AND Int64(5) [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N, iox::row:UInt64;N]
                      WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [foo] ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS iox::row]] [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N, iox::row:UInt64;N]
                        Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                          Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, data.foo AS foo, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                            Aggregate: groupBy=[[data.foo]], aggr=[[COUNT(data.f64_field)]] [foo:Dictionary(Int32, Utf8);N, COUNT(data.f64_field):Int64;N]
                              TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
            }

            #[test]
            fn with_limit_or_offset_errors() {
                // Fallible

                // returns an error if LIMIT or OFFSET values exceed i64::MAX
                let max = (i64::MAX as u64) + 1;
                assert_snapshot!(plan(format!("SELECT COUNT(f64_field) FROM data GROUP BY foo LIMIT {max}")), @"Error during planning: limit out of range");
                assert_snapshot!(plan(format!("SELECT COUNT(f64_field) FROM data GROUP BY foo OFFSET {max}")), @"Error during planning: offset out of range");
            }

            /// These tests validate the planner returns an error when using features that
            /// are not implemented or supported.
            mod not_implemented {
                use super::*;

                /// Tracked by <https://github.com/influxdata/influxdb_iox/issues/7204>
                #[test]
                fn group_by_time_precision() {
                    assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10u) FILL(none)"), @r###"
                    Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                      Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, coalesce_struct(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                        Aggregate: groupBy=[[date_bin(IntervalMonthDayNano("10000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                          Filter: data.time <= TimestampNanosecond(1672531200000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                            TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                    "###);
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
            assert_snapshot!(plan("SELECT f64_field FROM data"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Float64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT time, f64_field FROM data"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Float64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT time as timestamp, f64_field FROM data"), @r###"
            Sort: timestamp ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), timestamp:Timestamp(Nanosecond, None), f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS timestamp, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), timestamp:Timestamp(Nanosecond, None), f64_field:Float64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT foo, f64_field FROM data"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT foo, f64_field, i64_field FROM data"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N, i64_field:Int64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field, data.i64_field AS i64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N, i64_field:Int64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT /^f/ FROM data"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.f64_field AS f64_field, data.foo AS foo [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT * FROM data"), @r###"
            Sort: time ASC NULLS LAST, bar ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, with space:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.TIME AS TIME, data.bar AS bar, data.bool_field AS bool_field, data.f64_field AS f64_field, data.foo AS foo, data.i64_field AS i64_field, data.mixedCase AS mixedCase, data.str_field AS str_field, data.with space AS with space [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, with space:Float64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT TIME FROM data"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), TIME:Boolean;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.TIME AS TIME [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), TIME:Boolean;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###); // TIME is a field
        }

        /// Arithmetic expressions in the projection list
        #[test]
        fn test_simple_arithmetic_in_projection() {
            assert_snapshot!(plan("SELECT foo, f64_field + f64_field FROM data"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field_f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field + data.f64_field AS f64_field_f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field_f64_field:Float64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT foo, sin(f64_field) FROM data"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, sin:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, sin(data.f64_field) AS sin [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, sin:Float64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT foo, atan2(f64_field, 2) FROM data"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, atan2:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, atan2(data.f64_field, Int64(2)) AS atan2 [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, atan2:Float64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT foo, f64_field + 0.5 FROM data"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field + Float64(0.5) AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
        }

        #[test]
        fn test_select_single_measurement_group_by() {
            // Sort should be cpu, time
            assert_snapshot!(plan("SELECT usage_idle FROM cpu GROUP BY cpu"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.cpu AS cpu, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            // Sort should be cpu, time
            assert_snapshot!(plan("SELECT cpu, usage_idle FROM cpu GROUP BY cpu"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.cpu AS cpu, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            // Sort should be cpu, region, time
            assert_snapshot!(plan("SELECT usage_idle FROM cpu GROUP BY cpu, region"), @r###"
            Sort: cpu ASC NULLS LAST, region ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.cpu AS cpu, cpu.region AS region, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            // Sort should be cpu, region, time
            assert_snapshot!(plan("SELECT usage_idle FROM cpu GROUP BY region, cpu"), @r###"
            Sort: cpu ASC NULLS LAST, region ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.cpu AS cpu, cpu.region AS region, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            // Sort should be cpu, time, region
            assert_snapshot!(plan("SELECT region, usage_idle FROM cpu GROUP BY cpu"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST, region ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.cpu AS cpu, cpu.region AS region, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            // If a tag specified in a GROUP BY does not exist in the measurement, it should be omitted from the sort
            assert_snapshot!(plan("SELECT usage_idle FROM cpu GROUP BY cpu, non_existent"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, non_existent:Null;N, usage_idle:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.cpu AS cpu, NULL AS non_existent, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, non_existent:Null;N, usage_idle:Float64;N]
                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            // If a tag specified in a projection does not exist in the measurement, it should be omitted from the sort
            assert_snapshot!(plan("SELECT usage_idle, cpu, non_existent FROM cpu GROUP BY cpu"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), usage_idle:Float64;N, cpu:Dictionary(Int32, Utf8);N, non_existent:Null;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.usage_idle AS usage_idle, cpu.cpu AS cpu, NULL AS non_existent [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), usage_idle:Float64;N, cpu:Dictionary(Int32, Utf8);N, non_existent:Null;N]
                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            // If a non-existent field is included in the GROUP BY and projection, it should not be duplicated
            assert_snapshot!(plan("SELECT usage_idle, non_existent FROM cpu GROUP BY cpu, non_existent"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N, non_existent:Null;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.cpu AS cpu, cpu.usage_idle AS usage_idle, NULL AS non_existent [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N, non_existent:Null;N]
                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
        }

        #[test]
        fn test_select_multiple_measurements_group_by() {
            // Sort should be iox::measurement, cpu, time
            assert_snapshot!(plan("SELECT usage_idle, bytes_free FROM cpu, disk GROUP BY cpu"), @r###"
            Sort: iox::measurement ASC NULLS LAST, cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
              Union [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, CAST(cpu.cpu AS Utf8) AS cpu, cpu.usage_idle AS usage_idle, CAST(NULL AS Int64) AS bytes_free [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                Projection: Dictionary(Int32, Utf8("disk")) AS iox::measurement, disk.time AS time, CAST(NULL AS Utf8) AS cpu, CAST(NULL AS Float64) AS usage_idle, disk.bytes_free AS bytes_free [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                  TableScan: disk [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
            "###);

            // Sort should be iox::measurement, cpu, device, time
            assert_snapshot!(plan("SELECT usage_idle, bytes_free FROM cpu, disk GROUP BY device, cpu"), @r###"
            Sort: iox::measurement ASC NULLS LAST, cpu ASC NULLS LAST, device ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Utf8;N, device:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
              Union [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Utf8;N, device:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, CAST(cpu.cpu AS Utf8) AS cpu, CAST(NULL AS Utf8) AS device, cpu.usage_idle AS usage_idle, CAST(NULL AS Int64) AS bytes_free [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Utf8;N, device:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                Projection: Dictionary(Int32, Utf8("disk")) AS iox::measurement, disk.time AS time, CAST(NULL AS Utf8) AS cpu, CAST(disk.device AS Utf8) AS device, CAST(NULL AS Float64) AS usage_idle, disk.bytes_free AS bytes_free [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Utf8;N, device:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                  TableScan: disk [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
            "###);

            // Sort should be iox::measurement, cpu, time, device
            assert_snapshot!(plan("SELECT device, usage_idle, bytes_free FROM cpu, disk GROUP BY cpu"), @r###"
            Sort: iox::measurement ASC NULLS LAST, cpu ASC NULLS LAST, time ASC NULLS LAST, device ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Utf8;N, device:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
              Union [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Utf8;N, device:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, CAST(cpu.cpu AS Utf8) AS cpu, CAST(NULL AS Utf8) AS device, cpu.usage_idle AS usage_idle, CAST(NULL AS Int64) AS bytes_free [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Utf8;N, device:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                Projection: Dictionary(Int32, Utf8("disk")) AS iox::measurement, disk.time AS time, CAST(NULL AS Utf8) AS cpu, CAST(disk.device AS Utf8) AS device, CAST(NULL AS Float64) AS usage_idle, disk.bytes_free AS bytes_free [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Utf8;N, device:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                  TableScan: disk [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
            "###);

            // Sort should be iox::measurement, cpu, device, time
            assert_snapshot!(plan("SELECT cpu, usage_idle, bytes_free FROM cpu, disk GROUP BY cpu, device"), @r###"
            Sort: iox::measurement ASC NULLS LAST, cpu ASC NULLS LAST, device ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), device:Utf8;N, cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
              Union [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), device:Utf8;N, cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, CAST(NULL AS Utf8) AS device, CAST(cpu.cpu AS Utf8) AS cpu, cpu.usage_idle AS usage_idle, CAST(NULL AS Int64) AS bytes_free [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), device:Utf8;N, cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                Projection: Dictionary(Int32, Utf8("disk")) AS iox::measurement, disk.time AS time, CAST(disk.device AS Utf8) AS device, CAST(NULL AS Utf8) AS cpu, CAST(NULL AS Float64) AS usage_idle, disk.bytes_free AS bytes_free [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), device:Utf8;N, cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                  TableScan: disk [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
            "###);

            // Sort should be iox::measurement, device, time, cpu
            assert_snapshot!(plan("SELECT cpu, usage_idle, bytes_free FROM cpu, disk GROUP BY device"), @r###"
            Sort: iox::measurement ASC NULLS LAST, device ASC NULLS LAST, time ASC NULLS LAST, cpu ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), device:Utf8;N, cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
              Union [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), device:Utf8;N, cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, CAST(NULL AS Utf8) AS device, CAST(cpu.cpu AS Utf8) AS cpu, cpu.usage_idle AS usage_idle, CAST(NULL AS Int64) AS bytes_free [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), device:Utf8;N, cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                Projection: Dictionary(Int32, Utf8("disk")) AS iox::measurement, disk.time AS time, CAST(disk.device AS Utf8) AS device, CAST(NULL AS Utf8) AS cpu, CAST(NULL AS Float64) AS usage_idle, disk.bytes_free AS bytes_free [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), device:Utf8;N, cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                  TableScan: disk [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
            "###);

            // If a tag specified in a GROUP BY does not exist across all measurements, it should be omitted from the sort
            assert_snapshot!(plan("SELECT cpu, usage_idle, bytes_free FROM cpu, disk GROUP BY device, non_existent"), @r###"
            Sort: iox::measurement ASC NULLS LAST, device ASC NULLS LAST, time ASC NULLS LAST, cpu ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), device:Utf8;N, non_existent:Null;N, cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
              Union [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), device:Utf8;N, non_existent:Null;N, cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, CAST(NULL AS Utf8) AS device, NULL AS non_existent, CAST(cpu.cpu AS Utf8) AS cpu, cpu.usage_idle AS usage_idle, CAST(NULL AS Int64) AS bytes_free [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), device:Utf8;N, non_existent:Null;N, cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                Projection: Dictionary(Int32, Utf8("disk")) AS iox::measurement, disk.time AS time, CAST(disk.device AS Utf8) AS device, NULL AS non_existent, CAST(NULL AS Utf8) AS cpu, CAST(NULL AS Float64) AS usage_idle, disk.bytes_free AS bytes_free [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), device:Utf8;N, non_existent:Null;N, cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N]
                  TableScan: disk [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
            "###);

            // If a tag specified in a projection does not exist across all measurements, it should be omitted from the sort
            assert_snapshot!(plan("SELECT cpu, usage_idle, bytes_free, non_existent FROM cpu, disk GROUP BY device"), @r###"
            Sort: iox::measurement ASC NULLS LAST, device ASC NULLS LAST, time ASC NULLS LAST, cpu ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), device:Utf8;N, cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N, non_existent:Null;N]
              Union [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), device:Utf8;N, cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N, non_existent:Null;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, CAST(NULL AS Utf8) AS device, CAST(cpu.cpu AS Utf8) AS cpu, cpu.usage_idle AS usage_idle, CAST(NULL AS Int64) AS bytes_free, NULL AS non_existent [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), device:Utf8;N, cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N, non_existent:Null;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                Projection: Dictionary(Int32, Utf8("disk")) AS iox::measurement, disk.time AS time, CAST(disk.device AS Utf8) AS device, CAST(NULL AS Utf8) AS cpu, CAST(NULL AS Float64) AS usage_idle, disk.bytes_free AS bytes_free, NULL AS non_existent [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), device:Utf8;N, cpu:Utf8;N, usage_idle:Float64;N, bytes_free:Int64;N, non_existent:Null;N]
                  TableScan: disk [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
            "###);
        }

        #[test]
        fn test_select_group_by_limit_offset() {
            assert_snapshot!(plan("SELECT usage_idle FROM cpu GROUP BY cpu LIMIT 1"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
              Projection: iox::measurement, time, cpu, usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                Filter: iox::row <= Int64(1) [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N, iox::row:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [cpu] ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS iox::row]] [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N, iox::row:UInt64;N]
                    Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                      Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.cpu AS cpu, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                        TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT usage_idle FROM cpu GROUP BY cpu OFFSET 1"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
              Projection: iox::measurement, time, cpu, usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                Filter: iox::row > Int64(1) [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N, iox::row:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [cpu] ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS iox::row]] [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N, iox::row:UInt64;N]
                    Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                      Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.cpu AS cpu, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                        TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT usage_idle FROM cpu GROUP BY cpu LIMIT 1 OFFSET 1"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
              Projection: iox::measurement, time, cpu, usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                Filter: iox::row BETWEEN Int64(2) AND Int64(2) [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N, iox::row:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [cpu] ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS iox::row]] [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N, iox::row:UInt64;N]
                    Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                      Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.cpu AS cpu, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                        TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
        }

        #[test]
        fn test_select_function_tag_column() {
            assert_snapshot!(plan("SELECT last(foo) as foo, first(usage_idle) from cpu group by foo"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Null;N, first:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, TimestampNanosecond(0, None) AS time, NULL AS foo, (selector_first(cpu.usage_idle,cpu.time))[value] AS first [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Null;N, first:Float64;N]
                Aggregate: groupBy=[[]], aggr=[[selector_first(cpu.usage_idle, cpu.time)]] [selector_first(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT count(foo) as foo, first(usage_idle) from cpu group by foo"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Null;N, foo_1:Null;N, first:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, TimestampNanosecond(0, None) AS time, NULL AS foo, (coalesce_struct(selector_first(cpu.usage_idle,cpu.time,NULL), Struct({value:Float64(0),time:TimestampNanosecond(0, None),other_1:NULL})))[other_1] AS foo_1, (selector_first(cpu.usage_idle,cpu.time,NULL))[value] AS first [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Null;N, foo_1:Null;N, first:Float64;N]
                Aggregate: groupBy=[[]], aggr=[[selector_first(cpu.usage_idle, cpu.time, NULL)]] [selector_first(cpu.usage_idle,cpu.time,NULL):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "other_1", data_type: Null, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
        }

        // The following is an outline of additional scenarios to develop
        // as the planner learns more features.
        // This is not an exhaustive list and is expected to grow as the
        // planner feature list expands.

        //
        // Scenarios: field matching rules
        //

        // Correctly matches mixed case
        // assert_snapshot!(plan("SELECT mixedCase FROM data"));
        // assert_snapshot!(plan("SELECT \"mixedCase\" FROM data"));

        // Does not match when case differs
        // assert_snapshot!(plan("SELECT MixedCase FROM data"));

        // Matches those that require quotes
        // assert_snapshot!(plan("SELECT \"with space\" FROM data"));
        // assert_snapshot!(plan("SELECT /(with|f64)/ FROM data"));

        //
        // Scenarios: Measurement doesn't exist
        //
        // assert_snapshot!(plan("SELECT f64_field FROM data_1"));
        // assert_snapshot!(plan("SELECT foo, f64_field FROM data_1"));
        // assert_snapshot!(plan("SELECT /^f/ FROM data_1"));
        // assert_snapshot!(plan("SELECT * FROM data_1"));

        //
        // Scenarios: measurement exists, mixture of fields that do and don't exist
        //
        // assert_snapshot!(plan("SELECT f64_field, missing FROM data"));
        // assert_snapshot!(plan("SELECT foo, missing FROM data"));

        //
        // Scenarios: Mathematical scalar functions in the projection list, including
        // those in arithmetic expressions.
        //
        // assert_snapshot!(plan("SELECT abs(f64_field) FROM data"));
        // assert_snapshot!(plan("SELECT ceil(f64_field) FROM data"));
        // assert_snapshot!(plan("SELECT floor(f64_field) FROM data"));
        // assert_snapshot!(plan("SELECT pow(f64_field, 3) FROM data"));
        // assert_snapshot!(plan("SELECT pow(i64_field, 3) FROM data"));

        //
        // Scenarios: Invalid scalar functions in the projection list
        //

        //
        // Scenarios: WHERE clause with time range, now function and literal values
        // See `getTimeRange`: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L5791
        //

        //
        // Scenarios: WHERE clause with conditional expressions for tag and field
        // references, including
        //
        // * arithmetic expressions,
        // * regular expressions
        //

        //
        // Scenarios: Mathematical expressions in the WHERE clause
        //

        //
        // Scenarios: Unsupported scalar expressions in the WHERE clause
        //

        //
        // Scenarios: GROUP BY tags only
        //

        //
        // Scenarios: LIMIT and OFFSET clauses
        //

        //
        // Scenarios: DISTINCT clause and function
        //

        //
        // Scenarios: Unsupported multiple DISTINCT clauses and function calls
        //

        //
        // Scenarios: Multiple measurements, including
        //
        // * explicitly specified,
        // * regular expression matching
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
            assert_snapshot!(plan("SELECT f64_field + str_field::float FROM data"), @"");
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
