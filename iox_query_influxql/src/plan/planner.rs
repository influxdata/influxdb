mod select;

use crate::plan::error;
use crate::plan::planner::select::{
    check_exprs_satisfy_columns, fields_to_exprs_no_nulls, make_tag_key_column_meta,
    plan_with_sort, ToSortExpr,
};
use crate::plan::planner_rewrite_expression::{rewrite_conditional, rewrite_expr};
use crate::plan::planner_time_range_expression::{
    duration_expr_to_nanoseconds, expr_to_df_interval_dt, time_range_to_df_expr,
};
use crate::plan::rewriter::{
    rewrite_statement, select_statement_info, ProjectionType, SelectStatementInfo,
};
use crate::plan::util::{binary_operator_to_df_operator, rebase_expr, Schemas};
use crate::plan::var_ref::{column_type_to_var_ref_data_type, var_ref_data_type_to_data_type};
use arrow::array::{StringBuilder, StringDictionaryBuilder};
use arrow::datatypes::{DataType, Field as ArrowField, Int32Type, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use chrono_tz::Tz;
use datafusion::catalog::TableReference;
use datafusion::common::tree_node::{TreeNode, TreeNodeRewriter};
use datafusion::common::{DFSchema, DFSchemaRef, Result, ScalarValue, ToDFSchema};
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::expr_rewriter::normalize_col;
use datafusion::logical_expr::logical_plan::builder::project;
use datafusion::logical_expr::logical_plan::Analyze;
use datafusion::logical_expr::utils::{expr_as_column_expr, find_aggregate_exprs};
use datafusion::logical_expr::{
    binary_expr, col, date_bin, expr, expr::WindowFunction, lit, lit_timestamp_nano, now,
    window_function, Aggregate, AggregateFunction, AggregateUDF, Between, BinaryExpr,
    BuiltInWindowFunction, BuiltinScalarFunction, EmptyRelation, Explain, Expr, ExprSchemable,
    Extension, GetIndexedField, LogicalPlan, LogicalPlanBuilder, Operator, PlanType, ScalarUDF,
    TableSource, ToStringifiedPlan, WindowFrame, WindowFrameBound, WindowFrameUnits,
};
use datafusion::prelude::Column;
use datafusion_util::{lit_dict, AsExpr};
use generated_types::influxdata::iox::querier::v1::InfluxQlMetadata;
use influxdb_influxql_parser::common::{LimitClause, OffsetClause};
use influxdb_influxql_parser::explain::{ExplainOption, ExplainStatement};
use influxdb_influxql_parser::expression::walk::walk_expr;
use influxdb_influxql_parser::expression::{
    Binary, Call, ConditionalBinary, ConditionalExpression, ConditionalOperator, VarRef,
    VarRefDataType,
};
use influxdb_influxql_parser::select::{
    FillClause, GroupByClause, SLimitClause, SOffsetClause, TimeZoneClause,
};
use influxdb_influxql_parser::show_field_keys::ShowFieldKeysStatement;
use influxdb_influxql_parser::show_measurements::{
    ShowMeasurementsStatement, WithMeasurementClause,
};
use influxdb_influxql_parser::show_tag_keys::ShowTagKeysStatement;
use influxdb_influxql_parser::show_tag_values::{ShowTagValuesStatement, WithKeyClause};
use influxdb_influxql_parser::simple_from_clause::ShowFromClause;
use influxdb_influxql_parser::{
    common::{MeasurementName, WhereClause},
    expression::Expr as IQLExpr,
    identifier::Identifier,
    literal::Literal,
    select::{Field, FieldList, FromMeasurementClause, MeasurementSelection, SelectStatement},
    statement::Statement,
};
use iox_query::config::{IoxConfigExt, MetadataCutoff};
use iox_query::exec::gapfill::{FillStrategy, GapFill, GapFillParams};
use iox_query::exec::IOxSessionContext;
use iox_query::logical_optimizer::range_predicate::find_time_range;
use itertools::Itertools;
use once_cell::sync::Lazy;
use query_functions::selectors::{
    selector_first, selector_last, selector_max, selector_min, SelectorOutput,
};
use query_functions::{
    clean_non_meta_escapes,
    selectors::{
        struct_selector_first, struct_selector_last, struct_selector_max, struct_selector_min,
    },
};
use schema::{
    InfluxColumnType, InfluxFieldType, Schema, INFLUXQL_MEASUREMENT_COLUMN_NAME,
    INFLUXQL_METADATA_KEY,
};
use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;
use std::iter;
use std::ops::{Bound, ControlFlow, Deref, Range};
use std::str::FromStr;
use std::sync::Arc;

use super::parse_regex;

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

/// State used to inform the planner.
#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
struct Context<'a> {
    /// `true` if this is a subquery `SELECT` statement.
    is_subquery: bool,
    scope: ExprScope,
    tz: Option<Tz>,

    info: SelectStatementInfo,

    // GROUP BY information
    group_by: Option<&'a GroupByClause>,
    fill: Option<FillClause>,
}

impl<'a> Context<'a> {
    fn new(info: SelectStatementInfo) -> Self {
        Self {
            info,
            ..Default::default()
        }
    }

    fn with_scope(&self, scope: ExprScope) -> Self {
        Self { scope, ..*self }
    }

    fn with_timezone(&self, timezone: Option<TimeZoneClause>) -> Self {
        let tz = timezone.as_deref().cloned();
        Self { tz, ..*self }
    }

    fn with_group_by_fill(&self, select: &'a SelectStatement) -> Self {
        Self {
            group_by: select.group_by.as_ref(),
            fill: select.fill,
            ..*self
        }
    }

    fn fill(&self) -> FillClause {
        self.fill.unwrap_or_default()
    }

    fn is_aggregate(&self) -> bool {
        matches!(
            self.info.projection_type,
            ProjectionType::Aggregate | ProjectionType::Selector { .. }
        )
    }

    fn is_raw_distinct(&self) -> bool {
        matches!(self.info.projection_type, ProjectionType::RawDistinct)
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
                self.select_statement_to_plan(&self.rewrite_select_statement(*select)?)
            }
            Statement::ShowDatabases(_) => error::not_implemented("SHOW DATABASES"),
            Statement::ShowMeasurements(show_measurements) => {
                self.show_measurements_to_plan(*show_measurements)
            }
            Statement::ShowRetentionPolicies(_) => {
                error::not_implemented("SHOW RETENTION POLICIES")
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
        let plan =
            self.select_statement_to_plan(&self.rewrite_select_statement(*explain.select)?)?;
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

    fn rewrite_select_statement(&self, select: SelectStatement) -> Result<SelectStatement> {
        rewrite_statement(self.s, &select)
    }

    /// Create a [`LogicalPlan`] from the specified InfluxQL `SELECT` statement.
    fn select_statement_to_plan(&self, select: &SelectStatement) -> Result<LogicalPlan> {
        let mut plans = self.plan_from_tables(&select.from)?;

        let ctx = Context::new(select_statement_info(select)?)
            .with_timezone(select.timezone)
            .with_group_by_fill(select);

        // The `time` column is always present in the result set
        let mut fields = if find_time_column_index(&select.fields).is_none() {
            vec![Field {
                expr: IQLExpr::VarRef(VarRef {
                    name: "time".into(),
                    data_type: Some(VarRefDataType::Timestamp),
                }),
                alias: Some("time".into()),
            }]
        } else {
            vec![]
        };

        // group_by_tag_set   : a list of tag columns specified in the GROUP BY clause
        // projection_tag_set : a list of tag columns specified exclusively in the SELECT projection
        // is_projected       : a list of booleans indicating whether matching elements in the
        //                      group_by_tag_set are also projected in the query
        let (group_by_tag_set, projection_tag_set, is_projected) =
            if let Some(group_by) = &select.group_by {
                let mut tag_columns =
                    find_tag_and_unknown_columns(&select.fields).collect::<HashSet<_>>();

                // Find the list of tag keys specified in the `GROUP BY` clause, and
                // whether any of the tag keys are also projected in the SELECT list.
                let (tag_set, is_projected): (Vec<_>, Vec<_>) = group_by
                    .tags()
                    .map(|t| t.deref().as_str())
                    .map(|s| (s, tag_columns.contains(s)))
                    // We sort the tag set, to ensure correct ordering of the results. The tag columns
                    // referenced in the `tag_set` variable are added to the sort operator in
                    // lexicographically ascending order.
                    .sorted_by(|a, b| a.0.cmp(b.0))
                    .unzip();

                // Tags specified in the `GROUP BY` clause that are not already added to the
                // projection must be projected, so they can be used in the group key.
                //
                // At the end of the loop, the `tag_columns` set will contain the tag columns that
                // exist in the projection and not in the `GROUP BY`.
                fields.extend(
                    tag_set
                        .iter()
                        .filter_map(|col| match tag_columns.remove(*col) {
                            true => None,
                            false => Some(Field {
                                expr: IQLExpr::VarRef(VarRef {
                                    name: (*col).into(),
                                    data_type: Some(VarRefDataType::Tag),
                                }),
                                alias: Some((*col).into()),
                            }),
                        }),
                );

                (
                    tag_set,
                    tag_columns.into_iter().sorted().collect::<Vec<_>>(),
                    is_projected,
                )
            } else {
                let tag_columns = find_tag_and_unknown_columns(&select.fields)
                    .sorted()
                    .collect::<Vec<_>>();
                (vec![], tag_columns, vec![])
            };

        fields.extend(select.fields.iter().cloned());

        // Build the first non-empty plan
        let plan = {
            loop {
                match plans.pop_front() {
                    Some((plan, proj)) => match self.project_select(
                        &ctx,
                        plan,
                        proj,
                        select,
                        &fields,
                        &group_by_tag_set,
                    )? {
                        // Exclude any plans that produce no data, which is
                        // consistent with InfluxQL.
                        LogicalPlan::EmptyRelation(EmptyRelation {
                            produce_one_row: false,
                            ..
                        }) => continue,
                        plan => break plan,
                    },
                    None => return LogicalPlanBuilder::empty(false).build(),
                }
            }
        };

        // UNION the remaining plans
        let plan = plans.into_iter().try_fold(plan, |prev, (next, proj)| {
            let next = self.project_select(&ctx, next, proj, select, &fields, &group_by_tag_set)?;
            if let LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                ..
            }) = next
            {
                // Exclude any plans that produce no data, which is
                // consistent with InfluxQL.
                Ok(prev)
            } else {
                LogicalPlanBuilder::from(prev).union(next)?.build()
            }
        })?;

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

        // The UNION operator indicates the result set produces multiple tables or measurements.
        let is_multiple_measurements = matches!(plan, LogicalPlan::Union(_));

        let plan = plan_with_sort(
            plan,
            vec![select.order_by.to_sort_expr()],
            is_multiple_measurements,
            &group_by_tag_set,
            &projection_tag_set,
        )?;

        let plan = self.limit(
            plan,
            select.offset,
            select.limit,
            vec![select.order_by.to_sort_expr()],
            is_multiple_measurements,
            &group_by_tag_set,
            &projection_tag_set,
        )?;

        let plan = self.slimit(plan, select.series_offset, select.series_limit)?;

        Ok(plan)
    }

    fn project_select(
        &self,
        ctx: &Context<'_>,
        input: LogicalPlan,
        proj: Vec<Expr>,
        select: &SelectStatement,
        fields: &[Field],
        group_by_tag_set: &[&str],
    ) -> Result<LogicalPlan> {
        let schemas = Schemas::new(input.schema())?;

        // To be consistent with InfluxQL, exclude measurements
        // when the projection has no matching fields.
        if !fields.iter().any(|f| {
            // Walk the expression tree of `f`, looking for a
            // reference to at least one column that is a field
            walk_expr(&f.expr, &mut |e| match e {
                IQLExpr::VarRef(VarRef { name, .. }) => {
                    match schemas.iox_schema.field_by_name(name.deref().as_str()) {
                        Some((InfluxColumnType::Field(_), _)) => ControlFlow::Break(()),
                        _ => ControlFlow::Continue(()),
                    }
                }
                _ => ControlFlow::Continue(()),
            })
            .is_break()
        }) {
            return LogicalPlanBuilder::empty(false).build();
        }

        let plan = self.plan_where_clause(ctx, &select.condition, input, &schemas)?;

        // Transform InfluxQL AST field expressions to a list of DataFusion expressions.
        let mut select_exprs = self.field_list_to_exprs(ctx, &plan, fields, &schemas)?;

        if ctx.is_raw_distinct() {
            // This is a special case, where exactly one column can be projected with a `DISTINCT`
            // clause or the `distinct` function.
            //
            // In addition, the time column is projected as the Unix epoch.

            let Some(time_column_index) = find_time_column_index(fields) else {
                return error::internal("unable to find time column")
            };

            // Take ownership of the alias, so we don't reallocate, and temporarily place a literal
            // `NULL` in its place.
            let Expr::Alias(_, alias) = std::mem::replace(&mut select_exprs[time_column_index], lit(ScalarValue::Null)) else {
                return error::internal("time column is not an alias")
            };

            select_exprs[time_column_index] = lit_timestamp_nano(0).alias(alias);

            // Wrap the plan in a `LogicalPlan::Projection` from the select expressions
            let plan = project(plan, proj.into_iter().chain(select_exprs.into_iter()))?;

            return LogicalPlanBuilder::from(plan).distinct()?.build();
        }

        let (plan, select_exprs_post_aggr) =
            self.select_aggregate(ctx, plan, fields, select_exprs, group_by_tag_set, &schemas)?;

        // Wrap the plan in a `LogicalPlan::Projection` from the select expressions
        project(
            plan,
            proj.into_iter().chain(select_exprs_post_aggr.into_iter()),
        )
    }

    fn select_aggregate(
        &self,
        ctx: &Context<'_>,
        input: LogicalPlan,
        fields: &[Field],
        mut select_exprs: Vec<Expr>,
        group_by_tag_set: &[&str],
        schemas: &Schemas,
    ) -> Result<(LogicalPlan, Vec<Expr>)> {
        if !ctx.is_aggregate() {
            return Ok((input, select_exprs));
        }

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
        let aggr_exprs = find_aggregate_exprs(&select_exprs);

        // This block identifies the time column index and updates the time expression
        // based on the semantics of the projection.
        let time_column_index = {
            let Some(time_column_index) = find_time_column_index(fields) else {
                return error::internal("unable to find time column")
            };

            // Take ownership of the alias, so we don't reallocate, and temporarily place a literal
            // `NULL` in its place.
            let Expr::Alias(_, alias) = std::mem::replace(&mut select_exprs[time_column_index], lit(ScalarValue::Null)) else {
                return error::internal("time column is not an alias")
            };

            // Rewrite the `time` column projection based on a series of rules in the following
            // order. If the query:
            //
            // 1. is binning by time, project the column using the `DATE_BIN` function,
            // 2. is a single-selector query, project the `time` field of the selector aggregate,
            // 3. otherwise, project the Unix epoch (0)
            select_exprs[time_column_index] = if let Some(dim) = ctx.group_by.and_then(|gb| gb.time_dimension()) {
                let stride = expr_to_df_interval_dt(&dim.interval)?;
                let offset = if let Some(offset) = &dim.offset {
                    duration_expr_to_nanoseconds(offset)?
                } else {
                    0
                };

                date_bin(
                    stride,
                    "time".as_expr(),
                    lit(ScalarValue::TimestampNanosecond(Some(offset), None)),
                )
            } else if let ProjectionType::Selector { has_fields } =
                ctx.info.projection_type
            {
                if has_fields {
                    return error::not_implemented("projections with a single selector and fields: See https://github.com/influxdata/influxdb_iox/issues/7533");
                }

                let selector = match aggr_exprs.len() {
                    1 => aggr_exprs[0].clone(),
                    len => {
                        // Should have been validated by `select_statement_info`
                        return error::internal(format!("internal: expected 1 selector expression, got {len}"));
                    }
                };

                Expr::GetIndexedField(GetIndexedField {
                    expr: Box::new(selector),
                    key: ScalarValue::Utf8(Some("time".to_owned())),
                })
            } else {
                lit_timestamp_nano(0)
            }
            .alias(alias);

            time_column_index
        };

        let aggr_group_by_exprs = if let Some(group_by) = ctx.group_by {
            let mut group_by_exprs = Vec::new();

            if group_by.time_dimension().is_some() {
                // Include the GROUP BY TIME(..) expression
                group_by_exprs.push(select_exprs[time_column_index].clone());
            }

            // Exclude tags that do not exist in the current table schema.
            group_by_exprs.extend(group_by_tag_set.iter().filter_map(|name| {
                if schemas
                    .iox_schema
                    .field_by_name(name)
                    .map_or(false, |(dt, _)| dt == InfluxColumnType::Tag)
                {
                    Some(name.as_expr())
                } else {
                    None
                }
            }));

            group_by_exprs
        } else {
            vec![]
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
            let args = match select_exprs[time_column_index].clone().unalias() {
                Expr::ScalarFunction {
                    fun: BuiltinScalarFunction::DateBin,
                    args,
                } => args,
                _ => {
                    // The InfluxQL planner adds the `date_bin` function,
                    // so this condition represents an internal failure.
                    return error::internal("expected DATE_BIN function");
                }
            };

            let fill_strategy = match fill_option {
                FillClause::Null | FillClause::Value(_) => FillStrategy::Null,
                FillClause::Previous => FillStrategy::PrevNullAsMissing,
                FillClause::Linear => FillStrategy::LinearInterpolate,
                FillClause::None => unreachable!(),
            };

            build_gap_fill_node(plan, time_column_index, args, fill_strategy)?
        } else {
            plan
        };

        // Combine the aggregate columns and group by expressions, which represents
        // the final projection from the aggregate operator.
        let aggr_projection_exprs = [aggr_group_by_exprs, aggr_exprs].concat();

        // Replace any expressions that are not a column with a column referencing
        // an output column from the aggregate schema.
        let column_exprs_post_aggr = aggr_projection_exprs
            .iter()
            .map(|expr| expr_as_column_expr(expr, &plan))
            .collect::<Result<Vec<Expr>>>()?;

        // Create a literal expression for `value` if the strategy
        // is `FILL(<value>)`
        let fill_if_null = match fill_option {
            FillClause::Value(v) => Some(v),
            _ => None,
        };

        // Rewrite the aggregate columns from the projection, so that the expressions
        // refer to the columns from the aggregate projection
        let select_exprs_post_aggr = select_exprs
            .iter()
            .zip(fields)
            .map(|(expr, f)| {
                // This implements the `FILL(<value>)` strategy, by coalescing any aggregate
                // expressions to `<value>` when they are `NULL`.
                let fill_if_null = if fill_if_null.is_some() && is_aggregate_field(f) {
                    fill_if_null
                } else {
                    None
                };

                rebase_expr(expr, &aggr_projection_exprs, &fill_if_null, &plan)
            })
            .collect::<Result<Vec<Expr>>>()?;

        // Strip the NULL columns, which are tags that do not exist in the aggregate
        // table schema. The NULL columns are projected as scalar values in the final
        // projection.
        let select_exprs_post_aggr_no_nulls = select_exprs_post_aggr
            .iter()
            .filter(|expr| match expr {
                Expr::Alias(expr, _) => !matches!(**expr, Expr::Literal(ScalarValue::Null)),
                _ => true,
            })
            .cloned()
            .collect::<Vec<_>>();

        // Finally, we ensure that the re-written projection can be resolved
        // from the aggregate output columns and that there are no
        // column references that are not aggregates.
        //
        // This will identify issues such as:
        //
        // SELECT COUNT(field), field FROM foo
        //
        // where the field without the aggregate is not valid.
        check_exprs_satisfy_columns(&column_exprs_post_aggr, &select_exprs_post_aggr_no_nulls)?;

        Ok((plan, select_exprs_post_aggr))
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
    /// - `is_multiple_measurements`: `true` if the `input` produces multiple measurements,
    ///   and therefore the limit should be applied per measurement and any additional group tags.
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
        is_multiple_measurements: bool,
        group_by_tag_set: &[&str],
        projection_tag_set: &[&str],
    ) -> Result<LogicalPlan> {
        if offset.is_none() && limit.is_none() {
            return Ok(input);
        }

        if group_by_tag_set.is_empty() && !is_multiple_measurements {
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
            let window_func_exprs = vec![Expr::WindowFunction(WindowFunction {
                fun: window_function::WindowFunction::BuiltInWindowFunction(
                    BuiltInWindowFunction::RowNumber,
                ),
                args: vec![],
                partition_by: iter::once(INFLUXQL_MEASUREMENT_COLUMN_NAME.as_expr())
                    .chain(fields_to_exprs_no_nulls(input.schema(), group_by_tag_set))
                    .collect::<Vec<_>>(),
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
                is_multiple_measurements,
                group_by_tag_set,
                projection_tag_set,
            )
        }
    }

    /// Verifies the `SLIMIT` and `SOFFSET` clauses are `None`; otherwise, return a
    /// `NotImplemented` error.
    ///
    /// ## Why?
    /// * `SLIMIT` and `SOFFSET` don't work as expected per issue [#7571]
    /// * This issue [is noted](https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#the-slimit-clause) in our official documentation
    ///
    /// [#7571]: https://github.com/influxdata/influxdb/issues/7571
    fn slimit(
        &self,
        input: LogicalPlan,
        offset: Option<SOffsetClause>,
        limit: Option<SLimitClause>,
    ) -> Result<LogicalPlan> {
        if offset.is_none() && limit.is_none() {
            return Ok(input);
        }

        error::not_implemented("SLIMIT or SOFFSET")
    }

    /// Map the InfluxQL `SELECT` projection list into a list of DataFusion expressions.
    fn field_list_to_exprs(
        &self,
        ctx: &Context<'_>,
        plan: &LogicalPlan,
        fields: &[Field],
        schemas: &Schemas,
    ) -> Result<Vec<Expr>> {
        fields
            .iter()
            .map(|field| self.field_to_df_expr(ctx, field, plan, schemas))
            .collect()
    }

    /// Map an InfluxQL [`Field`] to a DataFusion [`Expr`].
    ///
    /// A [`Field`] is analogous to a column in a SQL `SELECT` projection.
    fn field_to_df_expr(
        &self,
        ctx: &Context<'_>,
        field: &Field,
        plan: &LogicalPlan,
        schemas: &Schemas,
    ) -> Result<Expr> {
        let expr =
            self.expr_to_df_expr(&ctx.with_scope(ExprScope::Projection), &field.expr, schemas)?;
        let expr = rewrite_field_expr(expr, schemas)?;
        normalize_col(
            if let Some(alias) = &field.alias {
                expr.alias(alias.deref())
            } else {
                expr
            },
            plan,
        )
    }

    /// Map an InfluxQL [`ConditionalExpression`] to a DataFusion [`Expr`].
    fn conditional_to_df_expr(
        &self,
        ctx: &Context<'_>,
        iql: &ConditionalExpression,
        schemas: &Schemas,
    ) -> Result<Expr> {
        match iql {
            ConditionalExpression::Expr(expr) => self.expr_to_df_expr(ctx, expr, schemas),
            ConditionalExpression::Binary(expr) => {
                self.binary_conditional_to_df_expr(ctx, expr, schemas)
            }
            ConditionalExpression::Grouped(e) => self.conditional_to_df_expr(ctx, e, schemas),
        }
    }

    /// Map an InfluxQL binary conditional expression to a DataFusion [`Expr`].
    fn binary_conditional_to_df_expr(
        &self,
        ctx: &Context<'_>,
        expr: &ConditionalBinary,
        schemas: &Schemas,
    ) -> Result<Expr> {
        let ConditionalBinary { lhs, op, rhs } = expr;

        let op = conditional_op_to_operator(*op)?;

        let (lhs_time, rhs_time) = (is_time_field(lhs), is_time_field(rhs));
        let (lhs, rhs) = if matches!(
            op,
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
        )
            // one or the other is true
            && (lhs_time ^ rhs_time)
        {
            if lhs_time {
                (
                    self.conditional_to_df_expr(ctx, lhs, schemas)?,
                    time_range_to_df_expr(find_expr(rhs)?, ctx.tz)?,
                )
            } else {
                (
                    time_range_to_df_expr(find_expr(lhs)?, ctx.tz)?,
                    self.conditional_to_df_expr(ctx, rhs, schemas)?,
                )
            }
        } else {
            (
                self.conditional_to_df_expr(ctx, lhs, schemas)?,
                self.conditional_to_df_expr(ctx, rhs, schemas)?,
            )
        };

        Ok(binary_expr(lhs, op, rhs))
    }

    /// Map an InfluxQL [`IQLExpr`] to a DataFusion [`Expr`].
    fn expr_to_df_expr(&self, ctx: &Context<'_>, iql: &IQLExpr, schemas: &Schemas) -> Result<Expr> {
        let iox_schema = &schemas.iox_schema;
        match iql {
            // rewriter is expected to expand wildcard expressions
            IQLExpr::Wildcard(_) => error::internal("unexpected wildcard in projection"),
            IQLExpr::VarRef(VarRef {
                name,
                data_type: opt_dst_type,
            }) => {
                let name = normalize_identifier(name);
                Ok(match (ctx.scope, name.as_str()) {
                    // Per the Go implementation, the time column is case-insensitive in the
                    // `WHERE` clause and disregards any postfix type cast operator.
                    //
                    // See: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L5751-L5753
                    (ExprScope::Where, name) if name.eq_ignore_ascii_case("time") => {
                        "time".as_expr()
                    }
                    (ExprScope::Projection, "time") => "time".as_expr(),
                    (_, name) => match iox_schema.field_by_name(name) {
                        Some((col_type, _)) => {
                            let column = name.as_expr();
                            match opt_dst_type {
                                Some(dst_type) => {
                                    let src_type = column_type_to_var_ref_data_type(col_type);
                                    if src_type == *dst_type {
                                        column
                                    } else if src_type.is_numeric_type()
                                        && dst_type.is_numeric_type()
                                    {
                                        // InfluxQL only allows casting between numeric types,
                                        // and it is safe to unconditionally unwrap, as the
                                        // `is_numeric_type` call guarantees it can be mapped to
                                        // an Arrow DataType
                                        column.cast_to(
                                            &var_ref_data_type_to_data_type(*dst_type).unwrap(),
                                            &schemas.df_schema,
                                        )?
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
                    Some(v.timestamp()),
                    None,
                ))),
                Literal::Duration(_) => error::not_implemented("duration literal"),
                Literal::Regex(re) => match ctx.scope {
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
            IQLExpr::Call(call) => self.call_to_df_expr(ctx, call, schemas),
            IQLExpr::Binary(expr) => self.arithmetic_expr_to_df_expr(ctx, expr, schemas),
            IQLExpr::Nested(e) => self.expr_to_df_expr(ctx, e, schemas),
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
    fn call_to_df_expr(&self, ctx: &Context<'_>, call: &Call, schemas: &Schemas) -> Result<Expr> {
        if is_scalar_math_function(call.name.as_str()) {
            return self.scalar_math_func_to_df_expr(ctx, call, schemas);
        }

        match ctx.scope {
            ExprScope::Where => {
                if call.name.eq_ignore_ascii_case("now") {
                    error::not_implemented("now")
                } else {
                    let name = &call.name;
                    error::query(format!("invalid function call in condition: {name}"))
                }
            }
            ExprScope::Projection => self.function_to_df_expr(ctx, call, schemas),
        }
    }

    fn function_to_df_expr(
        &self,
        ctx: &Context<'_>,
        call: &Call,
        schemas: &Schemas,
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

        let Call { name, args } = call;

        match name.as_str() {
            // The DISTINCT function is handled as a `ProjectionType::RawDistinct`
            // query, so the planner only needs to project the single column
            // argument.
            "distinct" => self.expr_to_df_expr(ctx, &args[0], schemas),
            "count" => {
                let (expr, distinct) = match &args[0] {
                    IQLExpr::Call(c) if c.name == "distinct" => {
                        (self.expr_to_df_expr(ctx, &c.args[0], schemas)?, true)
                    }
                    expr => (self.expr_to_df_expr(ctx, expr, schemas)?, false),
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
                )))
            }
            "sum" | "stddev" | "mean" | "median" => {
                let expr = self.expr_to_df_expr(ctx, &args[0], schemas)?;
                if let Expr::Literal(ScalarValue::Null) = expr {
                    return Ok(expr);
                }

                check_arg_count(name, args, 1)?;
                Ok(Expr::AggregateFunction(expr::AggregateFunction::new(
                    AggregateFunction::from_str(name)?,
                    vec![expr],
                    false,
                    None,
                )))
            }
            name @ ("first" | "last" | "min" | "max") => {
                let expr = self.expr_to_df_expr(ctx, &args[0], schemas)?;
                if let Expr::Literal(ScalarValue::Null) = expr {
                    return Ok(expr);
                }

                Ok(
                    if let ProjectionType::Selector { .. } = ctx.info.projection_type {
                        // Selector queries use the `struct_selector_<name>`, as they
                        // will project the value and the time fields of the struct
                        Expr::GetIndexedField(GetIndexedField {
                            expr: Box::new(
                                match name {
                                    "first" => struct_selector_first(),
                                    "last" => struct_selector_last(),
                                    "max" => struct_selector_max(),
                                    "min" => struct_selector_min(),
                                    _ => unreachable!(),
                                }
                                .call(vec![expr, "time".as_expr()]),
                            ),
                            key: ScalarValue::Utf8(Some("value".to_owned())),
                        })
                    } else {
                        // All other queries only require the value of the selector
                        let data_type = &expr.get_type(&schemas.df_schema)?;
                        match name {
                            "first" => selector_first(data_type, SelectorOutput::Value),
                            "last" => selector_last(data_type, SelectorOutput::Value),
                            "max" => selector_max(data_type, SelectorOutput::Value),
                            "min" => selector_min(data_type, SelectorOutput::Value),
                            _ => unreachable!(),
                        }
                        .call(vec![expr, "time".as_expr()])
                    },
                )
            }
            _ => error::query(format!("Invalid function '{name}'")),
        }
    }

    /// Map the InfluxQL scalar function call to a DataFusion scalar function expression.
    fn scalar_math_func_to_df_expr(
        &self,
        ctx: &Context<'_>,
        call: &Call,
        schemas: &Schemas,
    ) -> Result<Expr> {
        let args = call
            .args
            .iter()
            .map(|e| self.expr_to_df_expr(ctx, e, schemas))
            .collect::<Result<Vec<Expr>>>()?;

        match BuiltinScalarFunction::from_str(call.name.as_str())? {
            BuiltinScalarFunction::Log => {
                if args.len() != 2 {
                    error::query("invalid number of arguments for log, expected 2, got 1")
                } else {
                    Ok(Expr::ScalarFunction {
                        fun: BuiltinScalarFunction::Log,
                        args: args.into_iter().rev().collect(),
                    })
                }
            }
            fun => Ok(Expr::ScalarFunction { fun, args }),
        }
    }

    /// Map an InfluxQL arithmetic expression to a DataFusion [`Expr`].
    fn arithmetic_expr_to_df_expr(
        &self,
        ctx: &Context<'_>,
        expr: &Binary,
        schemas: &Schemas,
    ) -> Result<Expr> {
        Ok(binary_expr(
            self.expr_to_df_expr(ctx, &expr.lhs, schemas)?,
            binary_operator_to_df_operator(expr.op),
            self.expr_to_df_expr(ctx, &expr.rhs, schemas)?,
        ))
    }

    /// Generate a logical plan that filters the existing plan based on the
    /// optional InfluxQL conditional expression.
    fn plan_where_clause(
        &self,
        ctx: &Context<'_>,
        condition: &Option<WhereClause>,
        plan: LogicalPlan,
        schemas: &Schemas,
    ) -> Result<LogicalPlan> {
        match condition {
            Some(where_clause) => {
                let filter_expr = self.conditional_to_df_expr(
                    &ctx.with_scope(ExprScope::Where),
                    where_clause,
                    schemas,
                )?;
                let filter_expr = rewrite_conditional_expr(filter_expr, schemas)?;
                let plan = LogicalPlanBuilder::from(plan)
                    .filter(filter_expr)?
                    .build()?;
                Ok(plan)
            }
            None => Ok(plan),
        }
    }

    /// Generate a list of logical plans for each of the tables references in the `FROM`
    /// clause.
    fn plan_from_tables(
        &self,
        from: &FromMeasurementClause,
    ) -> Result<VecDeque<(LogicalPlan, Vec<Expr>)>> {
        // A list of scans and their initial projections
        let mut table_projs = VecDeque::new();
        for ms in from.iter() {
            let Some(table_proj) = match ms {
                MeasurementSelection::Name(qn) => match qn.name {
                    MeasurementName::Name(ref ident) => {
                        self.create_table_ref(normalize_identifier(ident))
                    }
                    // rewriter is expected to expand the regular expression
                    MeasurementName::Regex(_) => error::internal(
                        "unexpected regular expression in FROM clause",
                    ),
                },
                MeasurementSelection::Subquery(_) => error::not_implemented(
                    "subquery in FROM clause",
                ),
            }? else { continue };
            table_projs.push_back(table_proj);
        }
        Ok(table_projs)
    }

    /// Create a [LogicalPlan] that refers to the specified `table_name`.
    ///
    /// Normally, this functions will not return a `None`, as tables have been matched]
    /// by the [`rewrite_statement`] function.
    fn create_table_ref(&self, table_name: String) -> Result<Option<(LogicalPlan, Vec<Expr>)>> {
        Ok(if let Ok(source) = self.s.get_table_provider(&table_name) {
            let table_ref = TableReference::bare(table_name.to_string());
            Some((
                LogicalPlanBuilder::scan(table_ref, source, None)?.build()?,
                vec![lit_dict(&table_name).alias(INFLUXQL_MEASUREMENT_COLUMN_NAME)],
            ))
        } else {
            None
        })
    }

    /// Expand tables from `FROM` clause in metadata queries.
    fn expand_tables(&self, from: Option<ShowFromClause>) -> Result<Vec<String>> {
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

    fn show_tag_keys_to_plan(&self, show_tag_keys: ShowTagKeysStatement) -> Result<LogicalPlan> {
        if show_tag_keys.database.is_some() {
            // How do we handle this? Do we need to perform cross-namespace queries here?
            return error::not_implemented("SHOW TAG KEYS ON <database>");
        }
        if show_tag_keys.condition.is_some() {
            return error::not_implemented("SHOW TAG KEYS WHERE <condition>");
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

        let tables = self.expand_tables(show_tag_keys.from)?;

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
        let plan = LogicalPlanBuilder::scan(
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

        let tables = self.expand_tables(show_field_keys.from)?;

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

        let tables = self.expand_tables(show_tag_values.from)?;
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

            let Some((plan, measurement_expr)) = self.create_table_ref(table)? else {continue;};

            let schemas = Schemas::new(plan.schema())?;
            let plan = self.plan_where_clause(
                &Context::default(),
                &show_tag_values.condition,
                plan,
                &schemas,
            )?;
            let plan = add_time_restriction(plan, metadata_cutoff)?;

            for key in keys {
                let idx = plan
                    .schema()
                    .index_of_column_by_name(None, key)?
                    .expect("where is the key?");

                let plan = LogicalPlanBuilder::from(plan.clone())
                    .select([idx])?
                    .distinct()?
                    .sort([Expr::Column(Column::from_name(key)).sort(true, false)])?
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
        if show_measurements.condition.is_some() {
            return error::not_implemented("SHOW MEASUREMENTS WHERE <condition>");
        }

        let tables = match show_measurements.with_measurement {
            Some(
                WithMeasurementClause::Equals(qualified_name)
                | WithMeasurementClause::Regex(qualified_name),
            ) if qualified_name.database.is_some() => {
                return error::not_implemented("database name in from clause");
            }
            Some(
                WithMeasurementClause::Equals(qualified_name)
                | WithMeasurementClause::Regex(qualified_name),
            ) if qualified_name.retention_policy.is_some() => {
                return error::not_implemented("retention policy in from clause");
            }
            Some(WithMeasurementClause::Equals(qualified_name)) => match qualified_name.name {
                MeasurementName::Name(n) => {
                    let names = self.s.table_names();
                    if names.into_iter().any(|table| table == n.as_str()) {
                        vec![n.as_str().to_owned()]
                    } else {
                        vec![]
                    }
                }
                MeasurementName::Regex(_) => {
                    return error::query("expected string but got regex");
                }
            },
            Some(WithMeasurementClause::Regex(qualified_name)) => match &qualified_name.name {
                MeasurementName::Name(_) => {
                    return error::query("expected regex but got string");
                }
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
                    tables
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
                tables
            }
        };

        let name_col = "name";
        let output_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                INFLUXQL_MEASUREMENT_COLUMN_NAME,
                (&InfluxColumnType::Tag).into(),
                false,
            ),
            ArrowField::new(name_col, (&InfluxColumnType::Tag).into(), false),
        ]));

        let mut dummy_measurement_names_builder = StringDictionaryBuilder::<Int32Type>::new();
        let mut name_builder = StringDictionaryBuilder::<Int32Type>::new();
        for table in tables {
            dummy_measurement_names_builder.append_value("measurements");
            name_builder.append_value(table);
        }
        let plan = LogicalPlanBuilder::scan(
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
            show_measurements.offset,
            show_measurements.limit,
            vec![Expr::Column(Column::new_unqualified(name_col)).sort(true, false)],
            true,
            &[],
            &[],
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
/// * `date_bin_index` - The index of the field in the input schema that refers to the `date_bin` expression.
/// * `date_bin_args` - The list of arguments passed to the `date_bin` function, used to configure the gap-fill parameters.
/// * `fill_strategy` - The strategy used to fill gaps in the data.
fn build_gap_fill_node(
    input: LogicalPlan,
    date_bin_index: usize,
    date_bin_args: Vec<Expr>,
    fill_strategy: FillStrategy,
) -> Result<LogicalPlan> {
    // Extract the gap-fill parameters from the arguments to the `DATE_BIN` function.
    // Any unexpected conditions represents an internal error, as the `DATE_BIN` function is
    // added by the planner.
    let (stride, time_range, origin) = if date_bin_args.len() == 3 {
        let time_col = date_bin_args[1].try_into_col().map_err(|_| {
            error::map::internal("DATE_BIN requires a column as the source argument")
        })?;

        // Ensure that a time range was specified and is valid for gap filling
        let time_range = match find_time_range(input.inputs()[0], &time_col)? {
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
        };

        (
            date_bin_args[0].clone(),
            time_range,
            date_bin_args[2].clone(),
        )
    } else {
        // This is an internal error as the date_bin function is added by the planner and should
        // always contain the correct number of arguments.
        return error::internal(format!(
            "DATE_BIN expects 3 arguments, got {}",
            date_bin_args.len()
        ));
    };

    let aggr = Aggregate::try_from_plan(&input)?;
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

    let time_column = col(input.schema().fields()[date_bin_index].qualified_column());

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

/// Find all the columns where the resolved data type
/// is a tag or is [`None`], which is unknown.
fn find_tag_and_unknown_columns(fields: &FieldList) -> impl Iterator<Item = &str> {
    fields.iter().filter_map(|f| match &f.expr {
        IQLExpr::VarRef(VarRef {
            name,
            data_type: Some(VarRefDataType::Tag) | None,
        }) => Some(name.deref().as_str()),
        _ => None,
    })
}

/// Perform a series of passes to rewrite `expr` in compliance with InfluxQL behavior
/// in an effort to ensure the query executes without error.
fn rewrite_conditional_expr(expr: Expr, schemas: &Schemas) -> Result<Expr> {
    let expr = expr.rewrite(&mut FixRegularExpressions { schemas })?;
    rewrite_conditional(expr, schemas)
}

/// Perform a series of passes to rewrite `expr`, used as a column projection,
/// to match the behavior of InfluxQL.
fn rewrite_field_expr(expr: Expr, schemas: &Schemas) -> Result<Expr> {
    rewrite_expr(expr, schemas)
}

/// Rewrite regex conditional expressions to match InfluxQL behaviour.
struct FixRegularExpressions<'a> {
    schemas: &'a Schemas,
}

impl<'a> TreeNodeRewriter for FixRegularExpressions<'a> {
    type N = Expr;

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        match expr {
            // InfluxQL evaluates regular expression conditions to false if the column is numeric
            // or the column doesn't exist.
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: op @ (Operator::RegexMatch | Operator::RegexNotMatch),
                right,
            }) => {
                if let Expr::Column(ref col) = *left {
                    match self.schemas.iox_schema.field_by_name(&col.name) {
                        Some((InfluxColumnType::Tag, _)) => {
                            // Regular expressions expect to be compared with a Utf8
                            let left =
                                Box::new(left.cast_to(&DataType::Utf8, &self.schemas.df_schema)?);
                            Ok(Expr::BinaryExpr(BinaryExpr { left, op, right }))
                        }
                        Some((InfluxColumnType::Field(InfluxFieldType::String), _)) => {
                            Ok(Expr::BinaryExpr(BinaryExpr { left, op, right }))
                        }
                        // Any other column type should evaluate to false
                        _ => Ok(lit(false)),
                    }
                } else {
                    // If this is not a simple column expression, evaluate to false,
                    // to be consistent with InfluxQL.
                    //
                    // References:
                    //
                    // * https://github.com/influxdata/influxdb/blob/9308b6586a44e5999180f64a96cfb91e372f04dd/tsdb/index.go#L2487-L2488
                    // * https://github.com/influxdata/influxdb/blob/9308b6586a44e5999180f64a96cfb91e372f04dd/tsdb/index.go#L2509-L2510
                    //
                    // The query engine does not correctly evaluate tag keys and values, always evaluating to false.
                    //
                    // Reference example:
                    //
                    // * `SELECT f64 FROM m0 WHERE tag0 = '' + tag0`
                    Ok(lit(false))
                }
            }
            _ => Ok(expr),
        }
    }
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

// Normalize an identifier. Identifiers in InfluxQL are case sensitive,
// and therefore not transformed to lower case.
fn normalize_identifier(ident: &Identifier) -> String {
    // Dereference the identifier to return the unquoted value.
    ident.deref().clone()
}

/// Find the index of the time column in the fields list.
///
/// > **Note**
/// >
/// > To match InfluxQL, the `time` column must not exist as part of a
/// > complex expression.
pub(crate) fn find_time_column_index(fields: &[Field]) -> Option<usize> {
    fields
        .iter()
        .find_position(
            |f| matches!(&f.expr, IQLExpr::VarRef(VarRef { name, .. }) if name.deref() == "time"),
        )
        .map(|(i, _)| i)
}

/// Returns `true` if `name` is a mathematical scalar function
/// supported by InfluxQL.
pub(crate) fn is_scalar_math_function(name: &str) -> bool {
    static FUNCTIONS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
        HashSet::from([
            "abs", "sin", "cos", "tan", "asin", "acos", "atan", "atan2", "exp", "log", "ln",
            "log2", "log10", "sqrt", "pow", "floor", "ceil", "round",
        ])
    });

    FUNCTIONS.contains(name)
}

/// Returns `true` if `name` is an aggregate or aggregate function
/// supported by InfluxQL.
fn is_aggregate_function(name: &str) -> bool {
    static FUNCTIONS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
        HashSet::from([
            // Scalar-like functions
            "cumulative_sum",
            "derivative",
            "difference",
            "elapsed",
            "moving_average",
            "non_negative_derivative",
            "non_negative_difference",
            // Selector functions
            "bottom",
            "first",
            "last",
            "max",
            "min",
            "percentile",
            "sample",
            "top",
            // Aggregate functions
            "count",
            "integral",
            "mean",
            "median",
            "mode",
            "spread",
            "stddev",
            "sum",
            // Prediction functions
            "holt_winters",
            "holt_winters_with_fit",
            // Technical analysis functions
            "chande_momentum_oscillator",
            "exponential_moving_average",
            "double_exponential_moving_average",
            "kaufmans_efficiency_ratio",
            "kaufmans_adaptive_moving_average",
            "triple_exponential_moving_average",
            "triple_exponential_derivative",
            "relative_strength_index",
        ])
    });

    FUNCTIONS.contains(name)
}

/// Returns true if the conditional expression is a single node that
/// refers to the `time` column.
///
/// In a conditional expression, this comparison is case-insensitive per the [Go implementation][go]
///
/// [go]: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L5751-L5753
fn is_time_field(cond: &ConditionalExpression) -> bool {
    if let ConditionalExpression::Expr(expr) = cond {
        if let IQLExpr::VarRef(VarRef { ref name, .. }) = **expr {
            name.eq_ignore_ascii_case("time")
        } else {
            false
        }
    } else {
        false
    }
}

fn find_expr(cond: &ConditionalExpression) -> Result<&IQLExpr> {
    cond.expr()
        .ok_or_else(|| error::map::internal("incomplete conditional expression"))
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

/// Add time restriction to logical plan if there isn't any.
///
/// This must used directly on top of a potential filter plan, e.g. the one produced by [`plan_where_clause`](InfluxQLToLogicalPlan::plan_where_clause).
fn add_time_restriction(plan: LogicalPlan, cutoff: MetadataCutoff) -> Result<LogicalPlan> {
    let contains_time = if let LogicalPlan::Filter(filter) = &plan {
        let cols = filter.predicate.to_columns()?;
        cols.into_iter().any(|col| col.name == "time")
    } else {
        false
    };

    if contains_time {
        Ok(plan)
    } else {
        let cutoff_expr = match cutoff {
            MetadataCutoff::Absolute(dt) => lit_timestamp_nano(dt.timestamp_nanos()),
            MetadataCutoff::Relative(delta) => binary_expr(
                now(),
                Operator::Minus,
                lit(ScalarValue::IntervalMonthDayNano(Some(
                    i128::try_from(delta.as_nanos())
                        .map_err(|_| error::map::query("default timespan overflow"))?,
                ))),
            ),
        };
        LogicalPlanBuilder::from(plan)
            .filter(col("time").gt_eq(cutoff_expr))?
            .build()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::plan::test_utils::MockSchemaProvider;
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

    /// Verify the list of unsupported statements.
    ///
    /// It is expected certain statements will be unsupported, indefinitely.
    #[test]
    fn test_unsupported_statements() {
        assert_snapshot!(plan("CREATE DATABASE foo"), @"This feature is not implemented: CREATE DATABASE");
        assert_snapshot!(plan("DELETE FROM foo"), @"This feature is not implemented: DELETE");
        assert_snapshot!(plan("DROP MEASUREMENT foo"), @"This feature is not implemented: DROP MEASUREMENT");
        assert_snapshot!(plan("SHOW DATABASES"), @"This feature is not implemented: SHOW DATABASES");
        assert_snapshot!(plan("SHOW RETENTION POLICIES"), @"This feature is not implemented: SHOW RETENTION POLICIES");
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
        }

        #[test]
        fn test_show_tag_keys() {
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
        fn test_show_tag_values() {
            assert_snapshot!(plan("SHOW TAG VALUES WITH KEY = bar"), @r###"
            Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, Dictionary(Int32, Utf8("bar")) AS key, data.bar AS value [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N]
              Sort: data.bar ASC NULLS LAST [bar:Dictionary(Int32, Utf8);N]
                Distinct: [bar:Dictionary(Int32, Utf8);N]
                  Projection: data.bar [bar:Dictionary(Int32, Utf8);N]
                    Filter: data.time >= now() - IntervalMonthDayNano("86400000000000") [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                      TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SHOW TAG VALUES WITH KEY = bar LIMIT 1 OFFSET 2"), @r###"
            Sort: iox::measurement ASC NULLS LAST, key ASC NULLS LAST, value ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N]
              Projection: iox::measurement, key, value [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N]
                Filter: iox::row BETWEEN Int64(3) AND Int64(3) [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N, iox::row:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [iox::measurement] ORDER BY [key ASC NULLS LAST, value ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS iox::row]] [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N, iox::row:UInt64;N]
                    Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, Dictionary(Int32, Utf8("bar")) AS key, data.bar AS value [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N]
                      Sort: data.bar ASC NULLS LAST [bar:Dictionary(Int32, Utf8);N]
                        Distinct: [bar:Dictionary(Int32, Utf8);N]
                          Projection: data.bar [bar:Dictionary(Int32, Utf8);N]
                            Filter: data.time >= now() - IntervalMonthDayNano("86400000000000") [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                              TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SHOW TAG VALUES WITH KEY = bar WHERE foo = 'some_foo'"), @r###"
            Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, Dictionary(Int32, Utf8("bar")) AS key, data.bar AS value [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N]
              Sort: data.bar ASC NULLS LAST [bar:Dictionary(Int32, Utf8);N]
                Distinct: [bar:Dictionary(Int32, Utf8);N]
                  Projection: data.bar [bar:Dictionary(Int32, Utf8);N]
                    Filter: data.time >= now() - IntervalMonthDayNano("86400000000000") [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                      Filter: data.foo = Utf8("some_foo") [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                        TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SHOW TAG VALUES WITH KEY = bar WHERE time > 1337"), @r###"
            Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, Dictionary(Int32, Utf8("bar")) AS key, data.bar AS value [iox::measurement:Dictionary(Int32, Utf8), key:Dictionary(Int32, Utf8), value:Dictionary(Int32, Utf8);N]
              Sort: data.bar ASC NULLS LAST [bar:Dictionary(Int32, Utf8);N]
                Distinct: [bar:Dictionary(Int32, Utf8);N]
                  Projection: data.bar [bar:Dictionary(Int32, Utf8);N]
                    Filter: data.time > TimestampNanosecond(1337, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                      TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
        }
    }

    /// Tests to validate InfluxQL `SELECT` statements, where the projections do not matter,
    /// such as the WHERE clause.
    mod select {
        use super::*;

        /// Tests for the `DISTINCT` clause and `DISTINCT` function
        #[test]
        fn test_distinct() {
            assert_snapshot!(plan("SELECT DISTINCT usage_idle FROM cpu"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), distinct:Float64;N]
              Distinct: [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), distinct:Float64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, TimestampNanosecond(0, None) AS time, cpu.usage_idle AS distinct [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), distinct:Float64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT DISTINCT(usage_idle) FROM cpu"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), distinct:Float64;N]
              Distinct: [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), distinct:Float64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, TimestampNanosecond(0, None) AS time, cpu.usage_idle AS distinct [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), distinct:Float64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT DISTINCT usage_idle FROM cpu GROUP BY cpu"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, distinct:Float64;N]
              Distinct: [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, distinct:Float64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, TimestampNanosecond(0, None) AS time, cpu.cpu AS cpu, cpu.usage_idle AS distinct [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, distinct:Float64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT COUNT(DISTINCT usage_idle) FROM cpu"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), count:Int64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, TimestampNanosecond(0, None) AS time, COUNT(DISTINCT cpu.usage_idle) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), count:Int64;N]
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
            fn test_selectors() {
                // single-selector query
                assert_snapshot!(plan("SELECT LAST(usage_idle) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, (selector_last(cpu.usage_idle,cpu.time))[time] AS time, (selector_last(cpu.usage_idle,cpu.time))[value] AS last [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N]
                    Aggregate: groupBy=[[]], aggr=[[selector_last(cpu.usage_idle, cpu.time)]] [selector_last(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
                // single-selector, grouping by tags
                assert_snapshot!(plan("SELECT LAST(usage_idle) FROM cpu GROUP BY cpu"), @r###"
                Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, cpu:Dictionary(Int32, Utf8);N, last:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, (selector_last(cpu.usage_idle,cpu.time))[time] AS time, cpu.cpu AS cpu, (selector_last(cpu.usage_idle,cpu.time))[value] AS last [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, cpu:Dictionary(Int32, Utf8);N, last:Float64;N]
                    Aggregate: groupBy=[[cpu.cpu]], aggr=[[selector_last(cpu.usage_idle, cpu.time)]] [cpu:Dictionary(Int32, Utf8);N, selector_last(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

                // aggregate query, as we're grouping by time
                assert_snapshot!(plan("SELECT LAST(usage_idle) FROM cpu GROUP BY TIME(5s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, selector_last_value(cpu.usage_idle,cpu.time) AS last [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, last:Float64;N]
                    GapFill: groupBy=[[time]], aggr=[[selector_last_value(cpu.usage_idle,cpu.time)]], time_column=time, stride=IntervalMonthDayNano("5000000000"), range=Unbounded..Excluded(now()) [time:Timestamp(Nanosecond, None);N, selector_last_value(cpu.usage_idle,cpu.time):Float64;N]
                      Aggregate: groupBy=[[datebin(IntervalMonthDayNano("5000000000"), cpu.time, TimestampNanosecond(0, None)) AS time]], aggr=[[selector_last_value(cpu.usage_idle, cpu.time)]] [time:Timestamp(Nanosecond, None);N, selector_last_value(cpu.usage_idle,cpu.time):Float64;N]
                        TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

                // aggregate query, grouping by time with gap filling
                assert_snapshot!(plan("SELECT FIRST(usage_idle) FROM cpu GROUP BY TIME(5s) FILL(0)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, first:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, time, coalesce(selector_first_value(cpu.usage_idle,cpu.time), Float64(0)) AS first [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, first:Float64;N]
                    GapFill: groupBy=[[time]], aggr=[[selector_first_value(cpu.usage_idle,cpu.time)]], time_column=time, stride=IntervalMonthDayNano("5000000000"), range=Unbounded..Excluded(now()) [time:Timestamp(Nanosecond, None);N, selector_first_value(cpu.usage_idle,cpu.time):Float64;N]
                      Aggregate: groupBy=[[datebin(IntervalMonthDayNano("5000000000"), cpu.time, TimestampNanosecond(0, None)) AS time]], aggr=[[selector_first_value(cpu.usage_idle, cpu.time)]] [time:Timestamp(Nanosecond, None);N, selector_first_value(cpu.usage_idle,cpu.time):Float64;N]
                        TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

                // aggregate query, as we're specifying multiple selectors or aggregates
                assert_snapshot!(plan("SELECT LAST(usage_idle), FIRST(usage_idle) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), last:Float64;N, first:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, TimestampNanosecond(0, None) AS time, selector_last_value(cpu.usage_idle,cpu.time) AS last, selector_first_value(cpu.usage_idle,cpu.time) AS first [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), last:Float64;N, first:Float64;N]
                    Aggregate: groupBy=[[]], aggr=[[selector_last_value(cpu.usage_idle, cpu.time), selector_first_value(cpu.usage_idle, cpu.time)]] [selector_last_value(cpu.usage_idle,cpu.time):Float64;N, selector_first_value(cpu.usage_idle,cpu.time):Float64;N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
                assert_snapshot!(plan("SELECT LAST(usage_idle), COUNT(usage_idle) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), last:Float64;N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, TimestampNanosecond(0, None) AS time, selector_last_value(cpu.usage_idle,cpu.time) AS last, COUNT(cpu.usage_idle) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), last:Float64;N, count:Int64;N]
                    Aggregate: groupBy=[[]], aggr=[[selector_last_value(cpu.usage_idle, cpu.time), COUNT(cpu.usage_idle)]] [selector_last_value(cpu.usage_idle,cpu.time):Float64;N, COUNT(cpu.usage_idle):Int64;N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);

                // not implemented
                // See: https://github.com/influxdata/influxdb_iox/issues/7533
                assert_snapshot!(plan("SELECT LAST(usage_idle), usage_system FROM cpu"), @"This feature is not implemented: projections with a single selector and fields: See https://github.com/influxdata/influxdb_iox/issues/7533");

                // Validate we can call the remaining supported selector functions
                assert_snapshot!(plan("SELECT FIRST(usage_idle) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, first:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, (selector_first(cpu.usage_idle,cpu.time))[time] AS time, (selector_first(cpu.usage_idle,cpu.time))[value] AS first [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, first:Float64;N]
                    Aggregate: groupBy=[[]], aggr=[[selector_first(cpu.usage_idle, cpu.time)]] [selector_first(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
                assert_snapshot!(plan("SELECT MAX(usage_idle) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, max:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, (selector_max(cpu.usage_idle,cpu.time))[time] AS time, (selector_max(cpu.usage_idle,cpu.time))[value] AS max [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, max:Float64;N]
                    Aggregate: groupBy=[[]], aggr=[[selector_max(cpu.usage_idle, cpu.time)]] [selector_max(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
                assert_snapshot!(plan("SELECT MIN(usage_idle) FROM cpu"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, min:Float64;N]
                  Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, (selector_min(cpu.usage_idle,cpu.time))[time] AS time, (selector_min(cpu.usage_idle,cpu.time))[value] AS min [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, min:Float64;N]
                    Aggregate: groupBy=[[]], aggr=[[selector_min(cpu.usage_idle, cpu.time)]] [selector_min(cpu.usage_idle,cpu.time):Struct([Field { name: "value", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]);N]
                      TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                "###);
            }
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
            assert_snapshot!(plan("SELECT host, usage_idle FROM non_existent"), @"EmptyRelation []");
            assert_snapshot!(plan("SELECT host, usage_idle FROM cpu, non_existent"), @r###"
            Sort: time ASC NULLS LAST, host ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.host AS host, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            // multiple of same measurement
            assert_snapshot!(plan("SELECT host, usage_idle FROM cpu, cpu"), @r###"
            Sort: iox::measurement ASC NULLS LAST, time ASC NULLS LAST, host ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
              Union [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.host AS host, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.host AS host, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
        }

        #[test]
        fn test_time_range_in_where() {
            assert_snapshot!(
                plan("SELECT foo, f64_field FROM data where time > now() - 10s"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: data.time > now() - IntervalMonthDayNano("10000000000") [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###
            );
            assert_snapshot!(
                plan("SELECT foo, f64_field FROM data where time > '2004-04-09T02:33:45Z'"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: data.time > TimestampNanosecond(1081478025000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
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
                Filter: now() - IntervalMonthDayNano("10000000000") < data.time [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###
            );

            // Regular expression equality tests

            assert_snapshot!(plan("SELECT foo, f64_field FROM data where foo =~ /f/"), @r###"
            Sort: time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: CAST(data.foo AS Utf8) ~ Utf8("f") [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
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
                Filter: CAST(data.foo AS Utf8) !~ Utf8("f") [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
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
            assert_snapshot!(plan("SELECT not_exists::tag FROM data"), @"EmptyRelation []");
            assert_snapshot!(plan("SELECT not_exists::field FROM data"), @"EmptyRelation []");

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
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), count:Int64;N]
                Aggregate: groupBy=[[]], aggr=[[COUNT(data.f64_field)]] [COUNT(data.f64_field):Int64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY non_existent"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), non_existent:Null;N, count:Int64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, NULL AS non_existent, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), non_existent:Null;N, count:Int64;N]
                Aggregate: groupBy=[[]], aggr=[[COUNT(data.f64_field)]] [COUNT(data.f64_field):Int64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY foo"), @r###"
            Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, data.foo AS foo, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                Aggregate: groupBy=[[data.foo]], aggr=[[COUNT(data.f64_field)]] [foo:Dictionary(Int32, Utf8);N, COUNT(data.f64_field):Int64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

                // The `COUNT(f64_field)` aggregate is only projected ones in the Aggregate and reused in the projection
                assert_snapshot!(plan("SELECT COUNT(f64_field), COUNT(f64_field) + COUNT(f64_field), COUNT(f64_field) * 3 FROM data"), @r###"
            Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), count:Int64;N, count_f64_field_count_f64_field:Int64;N, count_f64_field:Int64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, COUNT(data.f64_field) AS count, COUNT(data.f64_field) + COUNT(data.f64_field) AS count_f64_field_count_f64_field, COUNT(data.f64_field) * Int64(3) AS count_f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), count:Int64;N, count_f64_field_count_f64_field:Int64;N, count_f64_field:Int64;N]
                Aggregate: groupBy=[[]], aggr=[[COUNT(data.f64_field)]] [COUNT(data.f64_field):Int64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

                // non-existent tags are excluded from the Aggregate groupBy and Sort operators
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY foo, non_existent"), @r###"
            Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, non_existent:Null;N, count:Int64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, data.foo AS foo, NULL AS non_existent, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, non_existent:Null;N, count:Int64;N]
                Aggregate: groupBy=[[data.foo]], aggr=[[COUNT(data.f64_field)]] [foo:Dictionary(Int32, Utf8);N, COUNT(data.f64_field):Int64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

                // Aggregate expression is projected once and reused in final projection
                assert_snapshot!(plan("SELECT COUNT(f64_field),  COUNT(f64_field) * 2 FROM data"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), count:Int64;N, count_f64_field:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, COUNT(data.f64_field) AS count, COUNT(data.f64_field) * Int64(2) AS count_f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), count:Int64;N, count_f64_field:Int64;N]
                    Aggregate: groupBy=[[]], aggr=[[COUNT(data.f64_field)]] [COUNT(data.f64_field):Int64;N]
                      TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);

                // Aggregate expression selecting non-existent field
                assert_snapshot!(plan("SELECT MEAN(f64_field) + MEAN(non_existent) FROM data"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), mean_f64_field_mean_non_existent:Null;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, NULL AS mean_f64_field_mean_non_existent [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), mean_f64_field_mean_non_existent:Null;N]
                    EmptyRelation []
                "###);

                // Aggregate expression with GROUP BY and non-existent field
                assert_snapshot!(plan("SELECT MEAN(f64_field) + MEAN(non_existent) FROM data GROUP BY foo"), @r###"
                Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, mean_f64_field_mean_non_existent:Null;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, data.foo AS foo, NULL AS mean_f64_field_mean_non_existent [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, mean_f64_field_mean_non_existent:Null;N]
                    Aggregate: groupBy=[[data.foo]], aggr=[[]] [foo:Dictionary(Int32, Utf8);N]
                      TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);

                // Aggregate expression selecting tag, should treat as non-existent
                assert_snapshot!(plan("SELECT MEAN(f64_field), MEAN(f64_field) + MEAN(non_existent) FROM data"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), mean:Float64;N, mean_f64_field_mean_non_existent:Null;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, AVG(data.f64_field) AS mean, NULL AS mean_f64_field_mean_non_existent [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), mean:Float64;N, mean_f64_field_mean_non_existent:Null;N]
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
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    Aggregate: groupBy=[[datebin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);

                // supports offset parameter
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10s, 5s) FILL(none)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    Aggregate: groupBy=[[datebin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(5000000000, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
            }

            #[test]
            fn group_by_time_gapfill() {
                // No time bounds
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[[time]], aggr=[[COUNT(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Excluded(now()) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[datebin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);

                // No lower time bounds
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data WHERE time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[[time]], aggr=[[COUNT(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Excluded(TimestampNanosecond(1667181720000000000, None)) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[datebin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        Filter: data.time < TimestampNanosecond(1667181720000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                          TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);

                // No upper time bounds
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' GROUP BY TIME(10s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[[time]], aggr=[[COUNT(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Included(TimestampNanosecond(1667181600000000000, None))..Excluded(now()) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[datebin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        Filter: data.time >= TimestampNanosecond(1667181600000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                          TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);

                // Default is FILL(null)
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data WHERE time >= '2022-10-31T02:00:00Z' AND time < '2022-10-31T02:02:00Z' GROUP BY TIME(10s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[[time]], aggr=[[COUNT(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Included(TimestampNanosecond(1667181600000000000, None))..Excluded(TimestampNanosecond(1667181720000000000, None)) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[datebin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        Filter: data.time >= TimestampNanosecond(1667181600000000000, None) AND data.time < TimestampNanosecond(1667181720000000000, None) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                          TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10s)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[[time]], aggr=[[COUNT(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Excluded(now()) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[datebin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10s) FILL(null)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[[time]], aggr=[[COUNT(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Excluded(now()) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[datebin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10s) FILL(previous)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[[time]], aggr=[[LOCF(COUNT(data.f64_field))]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Excluded(now()) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[datebin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10s) FILL(0)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, coalesce(COUNT(data.f64_field), Int64(0)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[[time]], aggr=[[COUNT(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Excluded(now()) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[datebin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10s) FILL(linear)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[[time]], aggr=[[INTERPOLATE(COUNT(data.f64_field))]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Excluded(now()) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[datebin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);

                // Coalesces the fill value, which is a float, to the matching type of a `COUNT` aggregate.
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY TIME(10s) FILL(3.2)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, coalesce(COUNT(data.f64_field), Int64(3)) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                    GapFill: groupBy=[[time]], aggr=[[COUNT(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Excluded(now()) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                      Aggregate: groupBy=[[datebin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
                        TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);

                // Aggregates as part of a binary expression
                assert_snapshot!(plan("SELECT COUNT(f64_field) + MEAN(f64_field) FROM data GROUP BY TIME(10s) FILL(3.2)"), @r###"
                Sort: time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count_f64_field_mean_f64_field:Float64;N]
                  Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, coalesce(COUNT(data.f64_field), Int64(3)) + coalesce(AVG(data.f64_field), Float64(3.2)) AS count_f64_field_mean_f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count_f64_field_mean_f64_field:Float64;N]
                    GapFill: groupBy=[[time]], aggr=[[COUNT(data.f64_field), AVG(data.f64_field)]], time_column=time, stride=IntervalMonthDayNano("10000000000"), range=Unbounded..Excluded(now()) [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N, AVG(data.f64_field):Float64;N]
                      Aggregate: groupBy=[[datebin(IntervalMonthDayNano("10000000000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field), AVG(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N, AVG(data.f64_field):Float64;N]
                        TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
            }

            #[test]
            fn with_limit_or_offset() {
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY foo LIMIT 1"), @r###"
                Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                  Projection: iox::measurement, time, foo, count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                    Filter: iox::row <= Int64(1) [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N, iox::row:UInt64;N]
                      WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [iox::measurement, foo] ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS iox::row]] [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N, iox::row:UInt64;N]
                        Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                          Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, data.foo AS foo, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                            Aggregate: groupBy=[[data.foo]], aggr=[[COUNT(data.f64_field)]] [foo:Dictionary(Int32, Utf8);N, COUNT(data.f64_field):Int64;N]
                              TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY foo OFFSET 1"), @r###"
                Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                  Projection: iox::measurement, time, foo, count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                    Filter: iox::row > Int64(1) [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N, iox::row:UInt64;N]
                      WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [iox::measurement, foo] ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS iox::row]] [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N, iox::row:UInt64;N]
                        Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                          Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, data.foo AS foo, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                            Aggregate: groupBy=[[data.foo]], aggr=[[COUNT(data.f64_field)]] [foo:Dictionary(Int32, Utf8);N, COUNT(data.f64_field):Int64;N]
                              TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);
                assert_snapshot!(plan("SELECT COUNT(f64_field) FROM data GROUP BY foo LIMIT 2 OFFSET 3"), @r###"
                Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                  Projection: iox::measurement, time, foo, count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                    Filter: iox::row BETWEEN Int64(4) AND Int64(5) [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N, iox::row:UInt64;N]
                      WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [iox::measurement, foo] ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS iox::row]] [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N, iox::row:UInt64;N]
                        Sort: foo ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                          Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, TimestampNanosecond(0, None) AS time, data.foo AS foo, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, count:Int64;N]
                            Aggregate: groupBy=[[data.foo]], aggr=[[COUNT(data.f64_field)]] [foo:Dictionary(Int32, Utf8);N, COUNT(data.f64_field):Int64;N]
                              TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                "###);

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
                      Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, time, COUNT(data.f64_field) AS count [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None);N, count:Int64;N]
                        Aggregate: groupBy=[[datebin(IntervalMonthDayNano("10000"), data.time, TimestampNanosecond(0, None)) AS time]], aggr=[[COUNT(data.f64_field)]] [time:Timestamp(Nanosecond, None);N, COUNT(data.f64_field):Int64;N]
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
            Projection: iox::measurement, timestamp, f64_field [iox::measurement:Dictionary(Int32, Utf8), timestamp:Timestamp(Nanosecond, None), f64_field:Float64;N]
              Sort: data.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), timestamp:Timestamp(Nanosecond, None), f64_field:Float64;N, time:Timestamp(Nanosecond, None)]
                Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time AS timestamp, data.f64_field AS f64_field, data.time [iox::measurement:Dictionary(Int32, Utf8), timestamp:Timestamp(Nanosecond, None), f64_field:Float64;N, time:Timestamp(Nanosecond, None)]
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
                  WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [iox::measurement, cpu] ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS iox::row]] [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N, iox::row:UInt64;N]
                    Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                      Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.cpu AS cpu, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                        TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT usage_idle FROM cpu GROUP BY cpu OFFSET 1"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
              Projection: iox::measurement, time, cpu, usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                Filter: iox::row > Int64(1) [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N, iox::row:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [iox::measurement, cpu] ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS iox::row]] [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N, iox::row:UInt64;N]
                    Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                      Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.cpu AS cpu, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                        TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT usage_idle FROM cpu GROUP BY cpu LIMIT 1 OFFSET 1"), @r###"
            Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
              Projection: iox::measurement, time, cpu, usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                Filter: iox::row BETWEEN Int64(2) AND Int64(2) [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N, iox::row:UInt64;N]
                  WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [iox::measurement, cpu] ORDER BY [time ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS iox::row]] [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N, iox::row:UInt64;N]
                    Sort: cpu ASC NULLS LAST, time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                      Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time AS time, cpu.cpu AS cpu, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), cpu:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
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
