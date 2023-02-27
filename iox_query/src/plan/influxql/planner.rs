use crate::plan::influxql::planner_rewrite_expression::{rewrite_conditional, rewrite_expr};
use crate::plan::influxql::planner_time_range_expression::time_range_to_df_expr;
use crate::plan::influxql::rewriter::rewrite_statement;
use crate::plan::influxql::util::{binary_operator_to_df_operator, Schemas};
use crate::plan::influxql::var_ref::{
    column_type_to_var_ref_data_type, var_ref_data_type_to_data_type,
};
use crate::DataFusionError;
use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue, ToDFSchema};
use datafusion::logical_expr::expr_rewriter::{normalize_col, ExprRewritable, ExprRewriter};
use datafusion::logical_expr::logical_plan::builder::project;
use datafusion::logical_expr::logical_plan::Analyze;
use datafusion::logical_expr::{
    binary_expr, lit, BinaryExpr, BuiltinScalarFunction, Explain, Expr, ExprSchemable, LogicalPlan,
    LogicalPlanBuilder, Operator, PlanType, Projection, TableSource, ToStringifiedPlan,
};
use datafusion_util::{lit_dict, AsExpr};
use influxdb_influxql_parser::common::OrderByClause;
use influxdb_influxql_parser::explain::{ExplainOption, ExplainStatement};
use influxdb_influxql_parser::expression::{
    BinaryOperator, ConditionalExpression, ConditionalOperator, VarRefDataType,
};
use influxdb_influxql_parser::select::{SLimitClause, SOffsetClause};
use influxdb_influxql_parser::{
    common::{LimitClause, MeasurementName, OffsetClause, WhereClause},
    expression::Expr as IQLExpr,
    identifier::Identifier,
    literal::Literal,
    select::{Field, FieldList, FromMeasurementClause, MeasurementSelection, SelectStatement},
    statement::Statement,
};
use itertools::Itertools;
use once_cell::sync::Lazy;
use query_functions::clean_non_meta_escapes;
use schema::{InfluxColumnType, InfluxFieldType, Schema};
use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;
use std::iter;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

/// The `SchemaProvider` trait allows the InfluxQL query planner to obtain
/// meta-data about tables referenced in InfluxQL statements.
pub trait SchemaProvider {
    /// Getter for a datasource
    fn get_table_provider(&self, name: &str) -> Result<Arc<dyn TableSource>>;

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
#[derive(Debug, Clone, Copy, PartialEq)]
enum ExprScope {
    /// Signals that expressions should be transformed in the context of
    /// the `WHERE` clause.
    Where,
    /// Signals that expressions should be transformed in the context of
    /// the `SELECT` projection list.
    Projection,
}

#[allow(missing_debug_implementations)]
/// InfluxQL query planner
pub struct InfluxQLToLogicalPlan<'a> {
    s: &'a dyn SchemaProvider,
}

impl<'a> InfluxQLToLogicalPlan<'a> {
    pub fn new(s: &'a dyn SchemaProvider) -> Self {
        Self { s }
    }

    pub fn statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        match statement {
            Statement::CreateDatabase(_) => {
                Err(DataFusionError::NotImplemented("CREATE DATABASE".into()))
            }
            Statement::Delete(_) => Err(DataFusionError::NotImplemented("DELETE".into())),
            Statement::DropMeasurement(_) => {
                Err(DataFusionError::NotImplemented("DROP MEASUREMENT".into()))
            }
            Statement::Explain(explain) => self.explain_statement_to_plan(*explain),
            Statement::Select(select) => {
                self.select_statement_to_plan(&self.rewrite_select_statement(*select)?)
            }
            Statement::ShowDatabases(_) => {
                Err(DataFusionError::NotImplemented("SHOW DATABASES".into()))
            }
            Statement::ShowMeasurements(_) => {
                Err(DataFusionError::NotImplemented("SHOW MEASUREMENTS".into()))
            }
            Statement::ShowRetentionPolicies(_) => Err(DataFusionError::NotImplemented(
                "SHOW RETENTION POLICIES".into(),
            )),
            Statement::ShowTagKeys(_) => {
                Err(DataFusionError::NotImplemented("SHOW TAG KEYS".into()))
            }
            Statement::ShowTagValues(_) => {
                Err(DataFusionError::NotImplemented("SHOW TAG VALUES".into()))
            }
            Statement::ShowFieldKeys(_) => {
                Err(DataFusionError::NotImplemented("SHOW FIELD KEYS".into()))
            }
        }
    }

    fn explain_statement_to_plan(&self, explain: ExplainStatement) -> Result<LogicalPlan> {
        let plan =
            self.select_statement_to_plan(&self.rewrite_select_statement(*explain.select)?)?;
        let plan = Arc::new(plan);
        let schema = LogicalPlan::explain_schema();
        let schema = schema.to_dfschema_ref()?;

        let (analyze, verbose) = match explain.options {
            Some(ExplainOption::AnalyzeVerbose) => (true, true),
            Some(ExplainOption::Analyze) => (true, false),
            Some(ExplainOption::Verbose) => (false, true),
            None => (false, false),
        };

        if analyze {
            Ok(LogicalPlan::Analyze(Analyze {
                verbose,
                input: plan,
                schema,
            }))
        } else {
            let stringified_plans = vec![plan.to_stringified(PlanType::InitialLogicalPlan)];
            Ok(LogicalPlan::Explain(Explain {
                verbose,
                plan,
                stringified_plans,
                schema,
                logical_optimization_succeeded: false,
            }))
        }
    }

    fn rewrite_select_statement(&self, select: SelectStatement) -> Result<SelectStatement> {
        rewrite_statement(self.s, &select)
    }

    /// Create a [`LogicalPlan`] from the specified InfluxQL `SELECT` statement.
    fn select_statement_to_plan(&self, select: &SelectStatement) -> Result<LogicalPlan> {
        let mut plans = self.plan_from_tables(&select.from)?;

        let Some(plan) = plans.pop_front() else { return LogicalPlanBuilder::empty(false).build(); };
        let plan = self.project_select(plan, select)?;

        // If there are multiple measurements, we need to sort by the measurement column
        // NOTE: Ideally DataFusion would maintain the order of the UNION ALL, which would eliminate
        //  the need to sort by measurement.
        //  See: https://github.com/influxdata/influxdb_iox/issues/7062
        let mut series_sort = if !plans.is_empty() {
            vec![Expr::sort("iox::measurement".as_expr(), true, false)]
        } else {
            vec![]
        };

        // UNION the remaining plans
        let plan = plans.into_iter().try_fold(plan, |prev, next| {
            let next = self.project_select(next, select)?;
            LogicalPlanBuilder::from(prev).union(next)?.build()
        })?;

        let plan = if select.group_by.is_none() {
            // Generate the following sort:
            // iox::measurement, time, [projected tags, sorted lexicographically]

            series_sort.push(Expr::sort(
                "time".as_expr(),
                match select.order_by {
                    // Default behaviour is to sort by time in ascending order if there is no ORDER BY
                    None | Some(OrderByClause::Ascending) => true,
                    Some(OrderByClause::Descending) => false,
                },
                false,
            ));

            series_sort.extend(
                select
                    .fields
                    .iter()
                    .filter_map(|f| {
                        if let IQLExpr::VarRef {
                            name,
                            data_type: Some(VarRefDataType::Tag),
                        } = &f.expr
                        {
                            Some(name.deref())
                        } else {
                            None
                        }
                    })
                    // the tags must be sorted lexicographically in ascending order to match
                    // the ordering in InfluxQL
                    .sorted()
                    .map(|n| Expr::sort(n.as_expr(), true, false)),
            );
            LogicalPlanBuilder::from(plan).sort(series_sort)?.build()
        } else {
            Err(DataFusionError::NotImplemented(
                "GROUP BY not supported".into(),
            ))
        }?;

        let plan = self.limit(plan, select.offset, select.limit)?;

        let plan = self.slimit(plan, select.series_offset, select.series_limit)?;

        Ok(plan)
    }

    fn project_select(&self, plan: LogicalPlan, select: &SelectStatement) -> Result<LogicalPlan> {
        let (proj, plan) = match plan {
            LogicalPlan::Projection(Projection { expr, input, .. }) => {
                (expr, input.deref().clone())
            }
            // TODO: Review when we support subqueries, as this shouldn't be the case
            _ => (vec![], plan),
        };

        let schemas = Schemas::new(plan.schema())?;

        let tz = select.timezone.as_deref().cloned();
        let plan = self.plan_where_clause(&select.condition, plan, &schemas, tz)?;

        // Process and validate the field expressions in the SELECT projection list
        let select_exprs = self.field_list_to_exprs(&plan, &select.fields, &schemas)?;

        // Wrap the plan in a `LogicalPlan::Projection` from the select expressions
        project(plan, proj.into_iter().chain(select_exprs.into_iter()))
    }

    /// Optionally wrap the input logical plan in a [`LogicalPlan::Limit`] node using the specified
    /// `offset` and `limit`.
    fn limit(
        &self,
        input: LogicalPlan,
        offset: Option<OffsetClause>,
        limit: Option<LimitClause>,
    ) -> Result<LogicalPlan> {
        if offset.is_none() && limit.is_none() {
            return Ok(input);
        }

        let skip = offset.map_or(0, |v| *v as usize);
        let fetch = limit.map(|v| *v as usize);

        LogicalPlanBuilder::from(input).limit(skip, fetch)?.build()
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

        Err(DataFusionError::NotImplemented("SLIMIT or SOFFSET".into()))
    }

    /// Map the InfluxQL `SELECT` projection list into a list of DataFusion expressions.
    fn field_list_to_exprs(
        &self,
        plan: &LogicalPlan,
        fields: &FieldList,
        schemas: &Schemas,
    ) -> Result<Vec<Expr>> {
        // InfluxQL requires the time column is present in the projection list.
        let extra = if !has_time_column(fields) {
            vec![Field {
                expr: IQLExpr::VarRef {
                    name: "time".into(),
                    data_type: Some(VarRefDataType::Timestamp),
                },
                alias: None,
            }]
        } else {
            vec![]
        };

        extra
            .iter()
            .chain(fields.iter())
            .map(|field| self.field_to_df_expr(field, plan, schemas))
            .collect()
    }

    /// Map an InfluxQL [`Field`] to a DataFusion [`Expr`].
    ///
    /// A [`Field`] is analogous to a column in a SQL `SELECT` projection.
    fn field_to_df_expr(
        &self,
        field: &Field,
        plan: &LogicalPlan,
        schemas: &Schemas,
    ) -> Result<Expr> {
        let expr = self.expr_to_df_expr(ExprScope::Projection, &field.expr, schemas)?;
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
        iql: &ConditionalExpression,
        schemas: &Schemas,
        tz: Option<chrono_tz::Tz>,
    ) -> Result<Expr> {
        match iql {
            ConditionalExpression::Expr(expr) => {
                self.expr_to_df_expr(ExprScope::Where, expr, schemas)
            }
            ConditionalExpression::Binary { lhs, op, rhs } => {
                self.binary_conditional_to_df_expr(lhs, *op, rhs, schemas, tz)
            }
            ConditionalExpression::Grouped(e) => self.conditional_to_df_expr(e, schemas, tz),
        }
    }

    /// Map an InfluxQL binary conditional expression to a DataFusion [`Expr`].
    fn binary_conditional_to_df_expr(
        &self,
        lhs: &ConditionalExpression,
        op: ConditionalOperator,
        rhs: &ConditionalExpression,
        schemas: &Schemas,
        tz: Option<chrono_tz::Tz>,
    ) -> Result<Expr> {
        let op = conditional_op_to_operator(op)?;

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
                    self.conditional_to_df_expr(lhs, schemas, tz)?,
                    time_range_to_df_expr(find_expr(rhs)?, tz)?,
                )
            } else {
                (
                    time_range_to_df_expr(find_expr(lhs)?, tz)?,
                    self.conditional_to_df_expr(rhs, schemas, tz)?,
                )
            }
        } else {
            (
                self.conditional_to_df_expr(lhs, schemas, tz)?,
                self.conditional_to_df_expr(rhs, schemas, tz)?,
            )
        };

        Ok(binary_expr(lhs, op, rhs))
    }

    /// Map an InfluxQL [`IQLExpr`] to a DataFusion [`Expr`].
    fn expr_to_df_expr(&self, scope: ExprScope, iql: &IQLExpr, schemas: &Schemas) -> Result<Expr> {
        let iox_schema = &schemas.iox_schema;
        match iql {
            // rewriter is expected to expand wildcard expressions
            IQLExpr::Wildcard(_) => Err(DataFusionError::Internal(
                "unexpected wildcard in projection".into(),
            )),
            IQLExpr::VarRef {
                name,
                data_type: opt_dst_type,
            } => {
                let name = normalize_identifier(name);
                Ok(
                    // Per the Go implementation, the time column is case-insensitive in the
                    // `WHERE` clause and disregards any postfix type cast operator.
                    //
                    // See: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L5751-L5753
                    if scope == ExprScope::Where && name.eq_ignore_ascii_case("time") {
                        "time".as_expr()
                    } else {
                        match iox_schema.find_index_of(&name) {
                            Some(idx) => {
                                let column = name.as_expr();
                                match opt_dst_type {
                                    Some(dst_type) => {
                                        let (col_type, _) = iox_schema.field(idx);
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
                        }
                    },
                )
            }
            IQLExpr::BindParameter(_) => Err(DataFusionError::NotImplemented("parameter".into())),
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
                Literal::Duration(_) => {
                    Err(DataFusionError::NotImplemented("duration literal".into()))
                }
                Literal::Regex(re) => match scope {
                    // a regular expression in a projection list is unexpected,
                    // as it should have been expanded by the rewriter.
                    ExprScope::Projection => Err(DataFusionError::Internal(
                        "unexpected regular expression found in projection".into(),
                    )),
                    ExprScope::Where => Ok(lit(clean_non_meta_escapes(re.as_str()))),
                },
            },
            IQLExpr::Distinct(_) => Err(DataFusionError::NotImplemented("DISTINCT".into())),
            IQLExpr::Call { name, args } => self.call_to_df_expr(scope, name, args, schemas),
            IQLExpr::Binary { lhs, op, rhs } => {
                self.arithmetic_expr_to_df_expr(scope, lhs, *op, rhs, schemas)
            }
            IQLExpr::Nested(e) => self.expr_to_df_expr(scope, e, schemas),
        }
    }

    /// Map an InfluxQL function call to a DataFusion expression.
    fn call_to_df_expr(
        &self,
        scope: ExprScope,
        name: &str,
        args: &[IQLExpr],
        schemas: &Schemas,
    ) -> Result<Expr> {
        if is_scalar_math_function(name) {
            self.scalar_math_func_to_df_expr(scope, name, args, schemas)
        } else {
            match scope {
                ExprScope::Projection => Err(DataFusionError::NotImplemented(
                    "aggregate and selector functions in projection list".into(),
                )),
                ExprScope::Where => {
                    if name.eq_ignore_ascii_case("now") {
                        Err(DataFusionError::NotImplemented("now".into()))
                    } else {
                        Err(DataFusionError::External(
                            format!("invalid function call in condition: {name}").into(),
                        ))
                    }
                }
            }
        }
    }

    /// Map the InfluxQL scalar function call to a DataFusion scalar function expression.
    fn scalar_math_func_to_df_expr(
        &self,
        scope: ExprScope,
        name: &str,
        args: &[IQLExpr],
        schemas: &Schemas,
    ) -> Result<Expr> {
        let fun = BuiltinScalarFunction::from_str(name)?;
        let args = args
            .iter()
            .map(|e| self.expr_to_df_expr(scope, e, schemas))
            .collect::<Result<Vec<Expr>>>()?;
        Ok(Expr::ScalarFunction { fun, args })
    }

    /// Map an InfluxQL arithmetic expression to a DataFusion [`Expr`].
    fn arithmetic_expr_to_df_expr(
        &self,
        scope: ExprScope,
        lhs: &IQLExpr,
        op: BinaryOperator,
        rhs: &IQLExpr,
        schemas: &Schemas,
    ) -> Result<Expr> {
        Ok(binary_expr(
            self.expr_to_df_expr(scope, lhs, schemas)?,
            binary_operator_to_df_operator(op),
            self.expr_to_df_expr(scope, rhs, schemas)?,
        ))
    }

    /// Generate a logical plan that filters the existing plan based on the
    /// optional InfluxQL conditional expression.
    fn plan_where_clause(
        &self,
        condition: &Option<WhereClause>,
        plan: LogicalPlan,
        schemas: &Schemas,
        tz: Option<chrono_tz::Tz>,
    ) -> Result<LogicalPlan> {
        match condition {
            Some(where_clause) => {
                let filter_expr = self.conditional_to_df_expr(where_clause, schemas, tz)?;
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
    fn plan_from_tables(&self, from: &FromMeasurementClause) -> Result<VecDeque<LogicalPlan>> {
        let mut plans = VecDeque::new();
        for ms in from.iter() {
            let Some(plan) = match ms {
                MeasurementSelection::Name(qn) => match qn.name {
                    MeasurementName::Name(ref ident) => {
                        self.create_table_ref(normalize_identifier(ident))
                    }
                    // rewriter is expected to expand the regular expression
                    MeasurementName::Regex(_) => Err(DataFusionError::Internal(
                        "unexpected regular expression in FROM clause".into(),
                    )),
                },
                MeasurementSelection::Subquery(_) => Err(DataFusionError::NotImplemented(
                    "subquery in FROM clause".into(),
                )),
            }? else { continue };
            plans.push_back(plan);
        }
        Ok(plans)
    }

    /// Create a [LogicalPlan] that refers to the specified `table_name`.
    ///
    /// Normally, this functions will not return a `None`, as tables have been matched]
    /// by the [`rewrite_statement`] function.
    fn create_table_ref(&self, table_name: String) -> Result<Option<LogicalPlan>> {
        Ok(if let Ok(source) = self.s.get_table_provider(&table_name) {
            Some(project(
                LogicalPlanBuilder::scan(&table_name, source, None)?.build()?,
                iter::once(lit_dict(&table_name).alias("iox::measurement")),
            )?)
        } else {
            None
        })
    }
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

impl<'a> ExprRewriter for FixRegularExpressions<'a> {
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
                    if let Some(idx) = self.schemas.iox_schema.find_index_of(&col.name) {
                        let (col_type, _) = self.schemas.iox_schema.field(idx);
                        match col_type {
                            InfluxColumnType::Tag => {
                                // Regular expressions expect to be compared with a Utf8
                                let left = Box::new(
                                    left.cast_to(&DataType::Utf8, &self.schemas.df_schema)?,
                                );
                                Ok(Expr::BinaryExpr(BinaryExpr { left, op, right }))
                            }
                            InfluxColumnType::Field(InfluxFieldType::String) => {
                                Ok(Expr::BinaryExpr(BinaryExpr { left, op, right }))
                            }
                            // Any other column type should evaluate to false
                            _ => Ok(lit(false)),
                        }
                    } else {
                        // If the field does not exist, evaluate to false
                        Ok(lit(false))
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
        ConditionalOperator::In => Err(DataFusionError::Internal(
            "unexpected binary operator: IN".into(),
        )),
    }
}

// Normalize an identifier. Identifiers in InfluxQL are case sensitive,
// and therefore not transformed to lower case.
fn normalize_identifier(ident: &Identifier) -> String {
    // Dereference the identifier to return the unquoted value.
    ident.deref().clone()
}

/// Returns true if the field list contains a `time` column.
///
/// > **Note**
/// >
/// > To match InfluxQL, the `time` column must not exist as part of a
/// > complex expression.
fn has_time_column(fields: &FieldList) -> bool {
    fields
        .iter()
        .any(|f| matches!(&f.expr, IQLExpr::VarRef { name, .. } if name.deref() == "time"))
}

static SCALAR_MATH_FUNCTIONS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    HashSet::from([
        "abs", "sin", "cos", "tan", "asin", "acos", "atan", "atan2", "exp", "log", "ln", "log2",
        "log10", "sqrt", "pow", "floor", "ceil", "round",
    ])
});

/// Returns `true` if `name` is a mathematical scalar function
/// supported by InfluxQL.
fn is_scalar_math_function(name: &str) -> bool {
    SCALAR_MATH_FUNCTIONS.contains(name)
}

/// Returns true if the conditional expression is a single node that
/// refers to the `time` column.
///
/// In a conditional expression, this comparison is case-insensitive per the [Go implementation][go]
///
/// [go]: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L5751-L5753
fn is_time_field(cond: &ConditionalExpression) -> bool {
    if let ConditionalExpression::Expr(expr) = cond {
        if let IQLExpr::VarRef { ref name, .. } = **expr {
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
        .ok_or_else(|| DataFusionError::Internal("incomplete conditional expression".into()))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::exec::Executor;
    use crate::plan::influxql::test_utils;
    use crate::plan::influxql::test_utils::TestDatabaseAdapter;
    use crate::test::{TestChunk, TestDatabase};
    use influxdb_influxql_parser::parse_statements;
    use insta::assert_snapshot;

    fn plan(sql: &str) -> String {
        let mut statements = parse_statements(sql).unwrap();
        // index of columns in the above chunk: [bar, foo, i64_field, i64_field_2, time]
        let executor = Arc::new(Executor::new_testing());
        let test_db = TestDatabase::new(Arc::clone(&executor));
        test_db.add_chunk(
            "my_partition_key",
            Arc::new(
                TestChunk::new("data")
                    .with_quiet()
                    .with_id(0)
                    .with_tag_column("foo")
                    .with_tag_column("bar")
                    .with_f64_field_column("f64_field")
                    .with_f64_field_column("mixedCase")
                    .with_f64_field_column("with space")
                    .with_i64_field_column("i64_field")
                    .with_string_field_column_with_stats("str_field", None, None)
                    .with_bool_field_column("bool_field")
                    // InfluxQL is case sensitive
                    .with_bool_field_column("TIME")
                    .with_time_column()
                    .with_one_row_of_data(),
            ),
        );

        // Table with tags and all field types
        test_db.add_chunk(
            "my_partition_key",
            Arc::new(
                TestChunk::new("all_types")
                    .with_quiet()
                    .with_id(1)
                    .with_tag_column("tag0")
                    .with_tag_column("tag1")
                    .with_f64_field_column("f64_field")
                    .with_i64_field_column("i64_field")
                    .with_string_field_column_with_stats("str_field", None, None)
                    .with_bool_field_column("bool_field")
                    .with_u64_field_column_no_stats("u64_field")
                    .with_time_column()
                    .with_one_row_of_data(),
            ),
        );

        test_utils::database::chunks().iter().for_each(|c| {
            test_db.add_chunk("my_partition_key", Arc::clone(c));
        });

        let sp = TestDatabaseAdapter::new(&test_db);

        let planner = InfluxQLToLogicalPlan::new(&sp);

        match planner.statement_to_plan(statements.pop().unwrap()) {
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
        assert_snapshot!(plan("SHOW MEASUREMENTS"), @"This feature is not implemented: SHOW MEASUREMENTS");
        assert_snapshot!(plan("SHOW RETENTION POLICIES"), @"This feature is not implemented: SHOW RETENTION POLICIES");
        assert_snapshot!(plan("SHOW TAG KEYS"), @"This feature is not implemented: SHOW TAG KEYS");
        assert_snapshot!(plan("SHOW TAG VALUES WITH KEY = bar"), @"This feature is not implemented: SHOW TAG VALUES");
        assert_snapshot!(plan("SHOW FIELD KEYS"), @"This feature is not implemented: SHOW FIELD KEYS");
    }

    /// Tests to validate InfluxQL `SELECT` statements, where the projections do not matter,
    /// such as the WHERE clause.
    mod select {
        use super::*;

        /// Verify the behaviour of the `FROM` clause when selecting from zero to many measurements.
        #[test]
        fn test_from_zero_to_many() {
            assert_snapshot!(plan("SELECT host, cpu, device, usage_idle, bytes_used FROM cpu, disk"), @r###"
            Sort: iox::measurement ASC NULLS LAST, time ASC NULLS LAST, cpu ASC NULLS LAST, device ASC NULLS LAST, host ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, cpu:Utf8;N, device:Utf8;N, usage_idle:Float64;N, bytes_used:Int64;N]
              Union [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, cpu:Utf8;N, device:Utf8;N, usage_idle:Float64;N, bytes_used:Int64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time, cpu.host AS host, CAST(cpu.cpu AS Utf8) AS cpu, CAST(NULL AS Utf8) AS device, cpu.usage_idle AS usage_idle, CAST(NULL AS Int64) AS bytes_used [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, cpu:Utf8;N, device:Utf8;N, usage_idle:Float64;N, bytes_used:Int64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                Projection: Dictionary(Int32, Utf8("disk")) AS iox::measurement, disk.time, disk.host AS host, CAST(NULL AS Utf8) AS cpu, CAST(disk.device AS Utf8) AS device, CAST(NULL AS Float64) AS usage_idle, disk.bytes_used AS bytes_used [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, cpu:Utf8;N, device:Utf8;N, usage_idle:Float64;N, bytes_used:Int64;N]
                  TableScan: disk [bytes_free:Int64;N, bytes_used:Int64;N, device:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
            "###);

            // nonexistent
            assert_snapshot!(plan("SELECT host, usage_idle FROM non_existent"), @"EmptyRelation []");
            assert_snapshot!(plan("SELECT host, usage_idle FROM cpu, non_existent"), @r###"
            Sort: cpu.time ASC NULLS LAST, host ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
              Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time, cpu.host AS host, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);

            // multiple of same measurement
            assert_snapshot!(plan("SELECT host, usage_idle FROM cpu, cpu"), @r###"
            Sort: iox::measurement ASC NULLS LAST, time ASC NULLS LAST, host ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
              Union [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time, cpu.host AS host, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
                Projection: Dictionary(Int32, Utf8("cpu")) AS iox::measurement, cpu.time, cpu.host AS host, cpu.usage_idle AS usage_idle [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), host:Dictionary(Int32, Utf8);N, usage_idle:Float64;N]
                  TableScan: cpu [cpu:Dictionary(Int32, Utf8);N, host:Dictionary(Int32, Utf8);N, region:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), usage_idle:Float64;N, usage_system:Float64;N, usage_user:Float64;N]
            "###);
        }

        #[test]
        fn test_time_range_in_where() {
            assert_snapshot!(
                plan("SELECT foo, f64_field FROM data where time > now() - 10s"), @r###"
            Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: data.time > now() - IntervalMonthDayNano("10000000000") [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###
            );
            assert_snapshot!(
                plan("SELECT foo, f64_field FROM data where time > '2004-04-09T02:33:45Z'"), @r###"
            Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
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
            Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: now() - IntervalMonthDayNano("10000000000") < data.time [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###
            );

            // Regular expression equality tests

            assert_snapshot!(plan("SELECT foo, f64_field FROM data where foo =~ /f/"), @r###"
            Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: CAST(data.foo AS Utf8) ~ Utf8("f") [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

            // regular expression for a numeric field is rewritten to `false`
            assert_snapshot!(plan("SELECT foo, f64_field FROM data where f64_field =~ /f/"), @r###"
            Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: Boolean(false) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

            // regular expression for a non-existent field is rewritten to `false`
            assert_snapshot!(
                plan("SELECT foo, f64_field FROM data where non_existent =~ /f/"), @r###"
            Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: Boolean(false) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###
            );

            // Regular expression inequality tests

            assert_snapshot!(plan("SELECT foo, f64_field FROM data where foo !~ /f/"), @r###"
            Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: CAST(data.foo AS Utf8) !~ Utf8("f") [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

            // regular expression for a numeric field is rewritten to `false`
            assert_snapshot!(plan("SELECT foo, f64_field FROM data where f64_field !~ /f/"), @r###"
            Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: Boolean(false) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

            // regular expression for a non-existent field is rewritten to `false`
            assert_snapshot!(
                plan("SELECT foo, f64_field FROM data where non_existent !~ /f/"), @r###"
            Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Filter: Boolean(false) [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###
            );
        }

        #[test]
        fn test_column_matching_rules() {
            // Cast between numeric types
            assert_snapshot!(plan("SELECT f64_field::integer FROM data"), @r###"
            Sort: data.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Int64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, CAST(data.f64_field AS Int64) AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Int64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT i64_field::float FROM data"), @r###"
            Sort: data.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, CAST(data.i64_field AS Float64) AS i64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Float64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

            // use field selector
            assert_snapshot!(plan("SELECT bool_field::field FROM data"), @r###"
            Sort: data.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Boolean;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.bool_field AS bool_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Boolean;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

            // invalid column reverence
            assert_snapshot!(plan("SELECT not_exists::tag FROM data"), @r###"
            Sort: data.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), not_exists:Null;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, NULL AS not_exists [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), not_exists:Null;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT not_exists::field FROM data"), @r###"
            Sort: data.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), not_exists:Null;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, NULL AS not_exists [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), not_exists:Null;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);

            // Returns NULL for invalid casts
            assert_snapshot!(plan("SELECT f64_field::string FROM data"), @r###"
            Sort: data.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Null;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, NULL AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Null;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT f64_field::boolean FROM data"), @r###"
            Sort: data.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Null;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, NULL AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Null;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT str_field::boolean FROM data"), @r###"
            Sort: data.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, NULL AS str_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
        }

        #[test]
        fn test_explain() {
            assert_snapshot!(plan("EXPLAIN SELECT foo, f64_field FROM data"), @r###"
            Explain [plan_type:Utf8, plan:Utf8]
              Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("EXPLAIN VERBOSE SELECT foo, f64_field FROM data"), @r###"
            Explain [plan_type:Utf8, plan:Utf8]
              Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("EXPLAIN ANALYZE SELECT foo, f64_field FROM data"), @r###"
            Analyze [plan_type:Utf8, plan:Utf8]
              Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("EXPLAIN ANALYZE VERBOSE SELECT foo, f64_field FROM data"), @r###"
            Analyze [plan_type:Utf8, plan:Utf8]
              Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                  TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
        }

        #[test]
        fn test_select_cast_postfix_operator() {
            // Float casting
            assert_snapshot!(plan("SELECT f64_field::float FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, all_types.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Float64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT f64_field::unsigned FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:UInt64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, CAST(all_types.f64_field AS UInt64) AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:UInt64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT f64_field::integer FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Int64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, CAST(all_types.f64_field AS Int64) AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Int64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT f64_field::string FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, NULL AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT f64_field::boolean FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, NULL AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);

            // Integer casting
            assert_snapshot!(plan("SELECT i64_field::float FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, CAST(all_types.i64_field AS Float64) AS i64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Float64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT i64_field::unsigned FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:UInt64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, CAST(all_types.i64_field AS UInt64) AS i64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:UInt64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT i64_field::integer FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Int64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, all_types.i64_field AS i64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Int64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT i64_field::string FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, NULL AS i64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT i64_field::boolean FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, NULL AS i64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), i64_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);

            // Unsigned casting
            assert_snapshot!(plan("SELECT u64_field::float FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, CAST(all_types.u64_field AS Float64) AS u64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:Float64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT u64_field::unsigned FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, all_types.u64_field AS u64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT u64_field::integer FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:Int64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, CAST(all_types.u64_field AS Int64) AS u64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:Int64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT u64_field::string FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, NULL AS u64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT u64_field::boolean FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, NULL AS u64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), u64_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);

            // String casting
            assert_snapshot!(plan("SELECT str_field::float FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, NULL AS str_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT str_field::unsigned FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, NULL AS str_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT str_field::integer FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, NULL AS str_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT str_field::string FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Utf8;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, all_types.str_field AS str_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Utf8;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT str_field::boolean FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, NULL AS str_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), str_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);

            // Boolean casting
            assert_snapshot!(plan("SELECT bool_field::float FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, NULL AS bool_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT bool_field::unsigned FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, NULL AS bool_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT bool_field::integer FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, NULL AS bool_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT bool_field::string FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, NULL AS bool_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
            assert_snapshot!(plan("SELECT bool_field::boolean FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Boolean;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, all_types.bool_field AS bool_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), bool_field:Boolean;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);

            // Validate various projection expressions with casts

            assert_snapshot!(plan("SELECT f64_field::integer + i64_field + u64_field::integer FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field_i64_field_u64_field:Int64;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, CAST(all_types.f64_field AS Int64) + all_types.i64_field + CAST(all_types.u64_field AS Int64) AS f64_field_i64_field_u64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field_i64_field_u64_field:Int64;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);

            assert_snapshot!(plan("SELECT f64_field::integer + i64_field + str_field::integer FROM all_types"), @r###"
            Sort: all_types.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field_i64_field_str_field:Null;N]
              Projection: Dictionary(Int32, Utf8("all_types")) AS iox::measurement, all_types.time, NULL AS f64_field_i64_field_str_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field_i64_field_str_field:Null;N]
                TableScan: all_types [bool_field:Boolean;N, f64_field:Float64;N, i64_field:Int64;N, str_field:Utf8;N, tag0:Dictionary(Int32, Utf8);N, tag1:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None), u64_field:UInt64;N]
            "###);
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
            Sort: data.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Float64;N]
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
            Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, data.f64_field AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT foo, f64_field, i64_field FROM data"), @r###"
            Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N, i64_field:Int64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, data.f64_field AS f64_field, data.i64_field AS i64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N, i64_field:Int64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT /^f/ FROM data"), @r###"
            Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.f64_field AS f64_field, data.foo AS foo [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT * FROM data"), @r###"
            Sort: data.time ASC NULLS LAST, bar ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, with space:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.TIME AS TIME, data.bar AS bar, data.bool_field AS bool_field, data.f64_field AS f64_field, data.foo AS foo, data.i64_field AS i64_field, data.mixedCase AS mixedCase, data.str_field AS str_field, data.with space AS with space [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, with space:Float64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT TIME FROM data"), @r###"
            Sort: data.time ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), TIME:Boolean;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.TIME AS TIME [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), TIME:Boolean;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###); // TIME is a field
        }

        /// Arithmetic expressions in the projection list
        #[test]
        fn test_simple_arithmetic_in_projection() {
            assert_snapshot!(plan("SELECT foo, f64_field + f64_field FROM data"), @r###"
            Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field_f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, data.f64_field + data.f64_field AS f64_field_f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field_f64_field:Float64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT foo, sin(f64_field) FROM data"), @r###"
            Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, sin:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, sin(data.f64_field) AS sin [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, sin:Float64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT foo, atan2(f64_field, 2) FROM data"), @r###"
            Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, atan2:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, atan2(data.f64_field, Int64(2)) AS atan2 [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, atan2:Float64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
            "###);
            assert_snapshot!(plan("SELECT foo, f64_field + 0.5 FROM data"), @r###"
            Sort: data.time ASC NULLS LAST, foo ASC NULLS LAST [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
              Projection: Dictionary(Int32, Utf8("data")) AS iox::measurement, data.time, data.foo AS foo, data.f64_field + Float64(0.5) AS f64_field [iox::measurement:Dictionary(Int32, Utf8), time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, f64_field:Float64;N]
                TableScan: data [TIME:Boolean;N, bar:Dictionary(Int32, Utf8);N, bool_field:Boolean;N, f64_field:Float64;N, foo:Dictionary(Int32, Utf8);N, i64_field:Int64;N, mixedCase:Float64;N, str_field:Utf8;N, time:Timestamp(Nanosecond, None), with space:Float64;N]
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
