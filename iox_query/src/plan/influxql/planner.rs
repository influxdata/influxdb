use crate::plan::influxql::planner_time_range_expression::time_range_to_df_expr;
use crate::plan::influxql::rewriter::rewrite_statement;
use crate::plan::influxql::util::binary_operator_to_df_operator;
use crate::{DataFusionError, IOxSessionContext, QueryNamespace};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::expr_rewriter::normalize_col;
use datafusion::logical_expr::logical_plan::builder::project;
use datafusion::logical_expr::{
    lit, BinaryExpr, BuiltinScalarFunction, Expr, LogicalPlan, LogicalPlanBuilder, Operator,
};
use datafusion::prelude::Column;
use datafusion::sql::TableReference;
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
use once_cell::sync::Lazy;
use std::collections::HashSet;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

/// Informs the planner which rules should be applied when transforming
/// an InfluxQL expression.
///
/// Specifically, the scope of available functions is narrowed to mathematical scalar functions
/// when processing the `WHERE` clause. The `SELECT` projection list is permitted
#[derive(Debug, Clone, Copy)]
enum ExprScope {
    /// Signals that expressions should be transformed in the context of
    /// the `WHERE` clause.
    Where,
    /// Signals that expressions should be transformed in the context of
    /// the `SELECT` projection list.
    Projection,
}

/// InfluxQL query planner
#[allow(unused)]
#[derive(Debug)]
pub struct InfluxQLToLogicalPlan<'a> {
    ctx: &'a IOxSessionContext,
    database: Arc<dyn QueryNamespace>,
}

impl<'a> InfluxQLToLogicalPlan<'a> {
    pub fn new(ctx: &'a IOxSessionContext, database: Arc<dyn QueryNamespace>) -> Self {
        Self { ctx, database }
    }

    pub async fn statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        match statement {
            Statement::CreateDatabase(_) => {
                Err(DataFusionError::NotImplemented("CREATE DATABASE".into()))
            }
            Statement::Delete(_) => Err(DataFusionError::NotImplemented("DELETE".into())),
            Statement::DropMeasurement(_) => {
                Err(DataFusionError::NotImplemented("DROP MEASUREMENT".into()))
            }
            Statement::Explain(_) => Err(DataFusionError::NotImplemented("EXPLAIN".into())),
            Statement::Select(select) => {
                let select = rewrite_statement(self.database.as_meta(), &select)?;
                self.select_statement_to_plan(select).await
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

    /// Create a [`LogicalPlan`] from the specified InfluxQL `SELECT` statement.
    async fn select_statement_to_plan(&self, select: SelectStatement) -> Result<LogicalPlan> {
        // Process FROM clause
        let plans = self.plan_from_tables(select.from).await?;

        // Only support a single measurement to begin with
        let plan = match plans.len() {
            0 => Err(DataFusionError::NotImplemented(
                "unsupported FROM: schema must exist".into(),
            )),
            1 => Ok(plans[0].clone()),
            _ => Err(DataFusionError::NotImplemented(
                "unsupported FROM: must target a single measurement".into(),
            )),
        }?;
        let tz = select.timezone.map(|tz| *tz);
        let plan = self.plan_where_clause(select.condition, plan, tz)?;

        // Process and validate the field expressions in the SELECT projection list
        let select_exprs = self.field_list_to_exprs(&plan, select.fields)?;

        // Wrap the plan in a `LogicalPlan::Projection` from the select expressions
        let plan = project(plan, select_exprs)?;

        let plan = self.limit(plan, select.offset, select.limit)?;

        let plan = self.slimit(plan, select.series_offset, select.series_limit)?;

        Ok(plan)
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
    fn field_list_to_exprs(&self, plan: &LogicalPlan, fields: FieldList) -> Result<Vec<Expr>> {
        // InfluxQL requires the time column is present in the projection list.
        let extra = if !has_time_column(&fields) {
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
            .map(|field| self.field_to_df_expr(field, plan))
            .collect()
    }

    /// Map an InfluxQL [`Field`] to a DataFusion [`Expr`].
    ///
    /// A [`Field`] is analogous to a column in a SQL `SELECT` projection.
    fn field_to_df_expr(&self, field: &Field, plan: &LogicalPlan) -> Result<Expr> {
        let input_schema = plan.schema();
        let expr = self.expr_to_df_expr(ExprScope::Projection, &field.expr, input_schema)?;
        if let Some(alias) = &field.alias {
            let expr = Expr::Alias(Box::new(expr), alias.deref().into());
            normalize_col(expr, plan)
        } else {
            normalize_col(expr, plan)
        }
    }

    /// Map an InfluxQL [`ConditionalExpression`] to a DataFusion [`Expr`].
    fn conditional_to_df_expr(
        &self,
        iql: &ConditionalExpression,
        schema: &DFSchema,
        tz: Option<chrono_tz::Tz>,
    ) -> Result<Expr> {
        match iql {
            ConditionalExpression::Expr(expr) => {
                self.expr_to_df_expr(ExprScope::Where, expr, schema)
            }
            ConditionalExpression::Binary { lhs, op, rhs } => {
                self.binary_conditional_to_df_expr(lhs, *op, rhs, schema, tz)
            }
            ConditionalExpression::Grouped(e) => self.conditional_to_df_expr(e, schema, tz),
        }
    }

    /// Map an InfluxQL binary conditional expression to a DataFusion [`Expr`].
    fn binary_conditional_to_df_expr(
        &self,
        lhs: &ConditionalExpression,
        op: ConditionalOperator,
        rhs: &ConditionalExpression,
        schema: &DFSchema,
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
                    self.conditional_to_df_expr(lhs, schema, tz)?,
                    time_range_to_df_expr(find_expr(rhs)?, tz)?,
                )
            } else {
                (
                    time_range_to_df_expr(find_expr(lhs)?, tz)?,
                    self.conditional_to_df_expr(rhs, schema, tz)?,
                )
            }
        } else {
            (
                self.conditional_to_df_expr(lhs, schema, tz)?,
                self.conditional_to_df_expr(rhs, schema, tz)?,
            )
        };

        Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lhs),
            op,
            Box::new(rhs),
        )))
    }

    /// Map an InfluxQL [`IQLExpr`] to a DataFusion [`Expr`].
    fn expr_to_df_expr(&self, scope: ExprScope, iql: &IQLExpr, schema: &DFSchema) -> Result<Expr> {
        match iql {
            // rewriter is expected to expand wildcard expressions
            IQLExpr::Wildcard(_) => Err(DataFusionError::Internal(
                "unexpected wildcard in projection".into(),
            )),
            IQLExpr::VarRef {
                name,
                data_type: None,
            } => Ok(Expr::Column(Column {
                relation: None,
                name: normalize_identifier(name),
            })),
            IQLExpr::VarRef {
                name,
                data_type: Some(_),
            } => Ok(Expr::Column(Column {
                relation: None,
                name: normalize_identifier(name),
            })),
            IQLExpr::BindParameter(_) => Err(DataFusionError::NotImplemented("parameter".into())),
            IQLExpr::Literal(val) => match val {
                Literal::Integer(v) => Ok(lit(ScalarValue::Int64(Some(*v)))),
                Literal::Unsigned(v) => Ok(lit(ScalarValue::UInt64(Some(*v)))),
                Literal::Float(v) => Ok(lit(*v)),
                Literal::String(v) => Ok(lit(v.clone())),
                Literal::Boolean(v) => Ok(lit(*v)),
                Literal::Timestamp(v) => Ok(lit(ScalarValue::TimestampNanosecond(
                    Some(v.timestamp()),
                    None,
                ))),
                Literal::Duration(_) => {
                    Err(DataFusionError::NotImplemented("duration literal".into()))
                }
                Literal::Regex(_) => match scope {
                    // a regular expression in a projection list is unexpected,
                    // as it should have been expanded by the rewriter.
                    ExprScope::Projection => Err(DataFusionError::Internal(
                        "unexpected regular expression found in projection".into(),
                    )),
                    ExprScope::Where => {
                        Err(DataFusionError::NotImplemented("regular expression".into()))
                    }
                },
            },
            IQLExpr::Distinct(_) => Err(DataFusionError::NotImplemented("DISTINCT".into())),
            IQLExpr::Call { name, args } => self.call_to_df_expr(scope, name, args, schema),
            IQLExpr::Binary { lhs, op, rhs } => {
                self.arithmetic_expr_to_df_expr(scope, lhs, *op, rhs, schema)
            }
            IQLExpr::Nested(e) => self.expr_to_df_expr(scope, e, schema),
        }
    }

    /// Map an InfluxQL function call to a DataFusion expression.
    fn call_to_df_expr(
        &self,
        scope: ExprScope,
        name: &str,
        args: &[IQLExpr],
        schema: &DFSchema,
    ) -> Result<Expr> {
        if is_scalar_math_function(name) {
            self.scalar_math_func_to_df_expr(scope, name, args, schema)
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
                            format!("invalid function call in condition: {}", name).into(),
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
        schema: &DFSchema,
    ) -> Result<Expr> {
        let fun = BuiltinScalarFunction::from_str(name)?;
        let args = args
            .iter()
            .map(|e| self.expr_to_df_expr(scope, e, schema))
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
        schema: &DFSchema,
    ) -> Result<Expr> {
        Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(self.expr_to_df_expr(scope, lhs, schema)?),
            binary_operator_to_df_operator(op),
            Box::new(self.expr_to_df_expr(scope, rhs, schema)?),
        )))
    }

    /// Generate a logical plan that filters the existing plan based on the
    /// optional InfluxQL conditional expression.
    fn plan_where_clause(
        &self,
        condition: Option<WhereClause>,
        plan: LogicalPlan,
        tz: Option<chrono_tz::Tz>,
    ) -> Result<LogicalPlan> {
        match condition {
            Some(where_clause) => {
                let input_schema = plan.schema();
                let filter_expr = self.conditional_to_df_expr(&where_clause, input_schema, tz)?;
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
    async fn plan_from_tables(&self, from: FromMeasurementClause) -> Result<Vec<LogicalPlan>> {
        let mut plans = vec![];
        for ms in from.iter() {
            let plan = match ms {
                MeasurementSelection::Name(qn) => match qn.name {
                    MeasurementName::Name(ref ident) => {
                        self.create_table_ref(normalize_identifier(ident)).await
                    }
                    // rewriter is expected to expand the regular expression
                    MeasurementName::Regex(_) => Err(DataFusionError::Internal(
                        "unexpected regular expression in FROM clause".into(),
                    )),
                },
                MeasurementSelection::Subquery(_) => Err(DataFusionError::NotImplemented(
                    "subquery in FROM clause".into(),
                )),
            }?;
            plans.push(plan);
        }
        Ok(plans)
    }

    /// Create a [LogicalPlan] that refers to the specified `table_name` or
    /// an [LogicalPlan::EmptyRelation] if the table does not exist.
    async fn create_table_ref(&self, table_name: String) -> Result<LogicalPlan> {
        let table_ref: TableReference<'_> = table_name.as_str().into();

        if let Ok(provider) = self.ctx.inner().table_provider(table_ref).await {
            LogicalPlanBuilder::scan(&table_name, provider_as_source(provider), None)?.build()
        } else {
            LogicalPlanBuilder::empty(false).build()
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
/// ⚠️ **NOTE**
/// To match InfluxQL, the `time` column must not exist as part of a
/// complex expression.
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
    use crate::exec::{ExecutionContextProvider, Executor};
    use crate::plan::influxql::test_utils;
    use crate::test::{TestChunk, TestDatabase};
    use influxdb_influxql_parser::parse_statements;
    use insta::assert_snapshot;

    async fn plan(sql: &str) -> String {
        let mut statements = parse_statements(sql).unwrap();
        // index of columns in the above chunk: [bar, foo, i64_field, i64_field_2, time]
        let executor = Arc::new(Executor::new_testing());
        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));
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

        test_utils::database::chunks().iter().for_each(|c| {
            test_db.add_chunk("my_partition_key", Arc::clone(c));
        });

        let ctx = test_db.new_query_context(None);
        let planner = InfluxQLToLogicalPlan::new(&ctx, test_db);

        match planner.statement_to_plan(statements.pop().unwrap()).await {
            Ok(res) => res.display_indent_schema().to_string(),
            Err(err) => err.to_string(),
        }
    }

    /// Verify the list of unsupported statements.
    ///
    /// It is expected certain statements will be unsupported, indefinitely.
    #[tokio::test]
    async fn test_unsupported_statements() {
        assert_snapshot!(plan("CREATE DATABASE foo").await);
        assert_snapshot!(plan("DELETE FROM foo").await);
        assert_snapshot!(plan("DROP MEASUREMENT foo").await);
        assert_snapshot!(plan("EXPLAIN SELECT bar FROM foo").await);
        assert_snapshot!(plan("SHOW DATABASES").await);
        assert_snapshot!(plan("SHOW MEASUREMENTS").await);
        assert_snapshot!(plan("SHOW RETENTION POLICIES").await);
        assert_snapshot!(plan("SHOW TAG KEYS").await);
        assert_snapshot!(plan("SHOW TAG VALUES WITH KEY = bar").await);
        assert_snapshot!(plan("SHOW FIELD KEYS").await);
    }

    /// Tests to validate InfluxQL `SELECT` statements, where the projections do not matter,
    /// such as the WHERE clause.
    mod select {
        use super::*;

        #[tokio::test]
        async fn test_time_range_in_where() {
            assert_snapshot!(
                plan("SELECT foo, f64_field FROM data where time > now() - 10s").await
            );
            assert_snapshot!(
                plan("SELECT foo, f64_field FROM data where time > '2004-04-09T02:33:45Z'").await
            );
            assert_snapshot!(
                plan("SELECT foo, f64_field FROM data where time > '2004-04-09T'").await
            );

            // time on the right-hand side
            assert_snapshot!(
                plan("SELECT foo, f64_field FROM data where  now() - 10s < time").await
            );
        }
    }

    /// Tests to validate InfluxQL `SELECT` statements that project columns without specifying
    /// aggregates or `GROUP BY time()` with gap filling.
    mod select_raw {
        use super::*;

        /// Select data from a single measurement
        #[tokio::test]
        async fn test_single_measurement() {
            assert_snapshot!(plan("SELECT f64_field FROM data").await);
            assert_snapshot!(plan("SELECT time, f64_field FROM data").await);
            assert_snapshot!(plan("SELECT time as timestamp, f64_field FROM data").await);
            assert_snapshot!(plan("SELECT foo, f64_field FROM data").await);
            assert_snapshot!(plan("SELECT foo, f64_field, i64_field FROM data").await);
            assert_snapshot!(plan("SELECT /^f/ FROM data").await);
            assert_snapshot!(plan("SELECT * FROM data").await);
            assert_snapshot!(plan("SELECT TIME FROM data").await); // TIME is a field
        }

        /// Arithmetic expressions in the projection list
        #[tokio::test]
        async fn test_simple_arithmetic_in_projection() {
            assert_snapshot!(plan("SELECT foo, f64_field + f64_field FROM data").await);
            assert_snapshot!(plan("SELECT foo, sin(f64_field) FROM data").await);
            assert_snapshot!(plan("SELECT foo, atan2(f64_field, 2) FROM data").await);
            assert_snapshot!(plan("SELECT foo, f64_field + 0.5 FROM data").await);
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
        #[tokio::test]
        #[ignore]
        async fn test_select_coercion_from_str() {
            assert_snapshot!(plan("SELECT f64_field + str_field::float FROM data").await);
        }

        /// **Issue:**
        /// Fails to cast f64_field column to Int64
        /// **Expected:**
        /// Succeeds and plan projection of f64_field contains cast to Int64
        /// **Actual:**
        /// Succeeds and plan projection of f64_field is Float64
        /// **Data:**
        /// m0,tag0=val00 f64=99.0,i64=100i,str="lo",str_f64="5.5" 1667181600000000000
        #[tokio::test]
        #[ignore]
        async fn test_select_explicit_cast() {
            assert_snapshot!(plan("SELECT f64_field::integer FROM data").await);
        }

        /// **Issue:**
        /// InfluxQL identifiers are case-sensitive and query fails to ignore unknown identifiers
        /// **Expected:**
        /// Succeeds and plans the query, returning null values for unknown columns
        /// **Actual:**
        /// Schema error: No field named 'TIME'. Valid fields are 'data'.'bar', 'data'.'bool_field', 'data'.'f64_field', 'data'.'foo', 'data'.'i64_field', 'data'.'mixedCase', 'data'.'str_field', 'data'.'time', 'data'.'with space'.
        #[tokio::test]
        #[ignore]
        async fn test_select_case_sensitivity() {
            // should return no results
            assert_snapshot!(plan("SELECT TIME, f64_Field FROM data").await);

            // should bind to time and f64_field, and i64_Field should return NULL values
            assert_snapshot!(plan("SELECT time, f64_field, i64_Field FROM data").await);
        }
    }
}
