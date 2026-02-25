//! Logical plan node for the SeriesLimit operation.

use arrow::datatypes::{DataType, Field};
use datafusion::{
    common::{
        DFSchema, DFSchemaRef, ExprSchema, Result, TableReference, internal_err,
        tree_node::{Transformed, TreeNodeContainer, TreeNodeRecursion},
    },
    error::DataFusionError,
    logical_expr::{Expr, ExprSchemable, LogicalPlan, SortExpr, UserDefinedLogicalNodeCore},
    sql::sqlparser::ast::NullTreatment,
};
use std::{collections::BTreeMap, sync::Arc};

/// Expression type that describes a time-series column to which per-series
/// `LIMIT` and `OFFSET` operations should be applied.
///
/// This type represents a single value column in a time series that will have
/// limiting applied independently per series group. It encapsulates not just the
/// expression to evaluate, but also how NULL values should be handled and what
/// default value to use when a row falls outside the limit range.
///
/// # Purpose
///
/// `LimitExpr` is used as part of the logical planning phase for InfluxQL queries
/// that apply LIMIT/OFFSET on a per-series basis. Each `LimitExpr` corresponds to
/// one value column in the SELECT clause that needs series-based limiting.
///
/// # NULL Treatment Modes
///
/// The `null_treatment` field controls how NULL values are counted:
///
/// - **`RespectNulls`**: NULL values count toward the row limit and are included
///   in row numbering. This is the default SQL behavior.
///
/// - **`IgnoreNulls`**: NULL values are skipped and don't count toward the limit.
///   Only non-NULL values contribute to the row count.
///
/// # Default Values
///
/// The `default_value` field specifies what value to output when a row is filtered
/// out due to LIMIT/OFFSET constraints, but the timestamp exists in another series.
/// This enables time-aligned output across multiple series even when some series
/// have fewer points.
///
/// # Examples
///
/// ## Basic Usage
///
/// ```text
/// Query: SELECT temperature FROM weather GROUP BY location LIMIT 3
///
/// LimitExpr {
///     expr: Column("temperature"),
///     null_treatment: RespectNulls,
///     default_value: Literal(NULL),
/// }
/// ```
///
/// ## With Default Values (FILL)
///
/// ```text
/// Query: SELECT FILL(0, temperature) FROM weather GROUP BY location LIMIT 3
///
/// LimitExpr {
///     expr: Column("temperature"),
///     null_treatment: RespectNulls,
///     default_value: Literal(0),
/// }
/// ```
///
/// ## Ignoring NULLs
///
/// ```text
/// Query: SELECT temperature IGNORE NULLS FROM weather GROUP BY location LIMIT 3
///
/// LimitExpr {
///     expr: Column("temperature"),
///     null_treatment: IgnoreNulls,
///     default_value: Literal(NULL),
/// }
/// ```
///
/// # Type Safety
///
/// The `expr` and `default_value` must have the same data type. This is validated
/// by the `get_type()` method and enforced during logical plan construction. Type
/// mismatches result in an error during query planning.
///
/// # Relation to Physical Plan
///
/// During physical planning, each `LimitExpr` is converted to a [`PhysicalLimitExpr`]
/// which performs the actual row numbering and filtering during query execution.
///
/// [`PhysicalLimitExpr`]: crate::exec::series_limit::physical::PhysicalLimitExpr
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct LimitExpr {
    /// The expression for the values to which the limit will be applied.
    /// This must reference exactly one column in the input which will
    /// be replaced with the limited version.
    pub expr: Expr,

    /// How nulls in the series should be treated.
    pub null_treatment: NullTreatment,

    /// The default value that should be output if a point in time is
    /// outside of the limits for (or not present in) this series, but
    /// the time is present in another series.
    pub default_value: Expr,
}

impl std::fmt::Display for LimitExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} (default: {})",
            self.expr, self.null_treatment, self.default_value
        )
    }
}

impl LimitExpr {
    /// Returns the data type of this expression when evaluated against the given schema.
    ///
    /// This method ensures that both the expression and default value have the same type,
    /// returning an error if they differ.
    pub fn get_type(&self, schema: &dyn ExprSchema) -> Result<DataType> {
        let expr_dt = self.expr.get_type(schema)?;
        let default_value_dt = self.default_value.get_type(schema)?;
        if expr_dt != default_value_dt {
            return internal_err!(
                "LimitExpr expr and default_value must have the same type, got expr: {expr_dt:?}, default_value: {default_value_dt:?}"
            );
        }
        Ok(expr_dt)
    }

    /// Returns whether this expression is nullable when evaluated against the given schema.
    ///
    /// The result is nullable if either the expression or the default value is nullable,
    /// since either could contribute to the final result.
    pub fn nullable(&self, input_schema: &dyn ExprSchema) -> Result<bool> {
        let expr_nullable = self.expr.nullable(input_schema)?;
        let default_nullable = self.default_value.nullable(input_schema)?;
        Ok(match self.null_treatment {
            // If ignoring nulls, the expression's nullability does not affect the result
            NullTreatment::IgnoreNulls => default_nullable,
            // If respecting nulls, both expression and default value nullability matter
            NullTreatment::RespectNulls => expr_nullable || default_nullable,
        })
    }

    /// Returns both the data type and nullability of this expression.
    ///
    /// This is a convenience method that combines `get_type` and `nullable`.
    pub fn data_type_and_nullable(&self, schema: &dyn ExprSchema) -> Result<(DataType, bool)> {
        let data_type = self.get_type(schema)?;
        let nullable = self.nullable(schema)?;
        Ok((data_type, nullable))
    }

    /// Returns a field representation of this expression with its name, data type, and nullability.
    ///
    /// The field is derived from the underlying expression but with potentially updated
    /// nullability based on both the expression and default value.
    pub fn to_field(
        &self,
        input_schema: &dyn ExprSchema,
    ) -> Result<(Option<TableReference>, Arc<Field>)> {
        let (qualifier, field) = self.expr.to_field(input_schema)?;

        // Get the data type and nullability, which may differ from the base expression
        let data_type = self.get_type(input_schema)?;
        let nullable = self.nullable(input_schema)?;

        // Create a new field with the potentially updated type and nullability
        let new_field =
            Field::new(field.name(), data_type, nullable).with_metadata(field.metadata().clone());

        Ok((qualifier, Arc::new(new_field)))
    }
}

impl<'a> TreeNodeContainer<'a, Expr> for LimitExpr {
    fn apply_elements<F: FnMut(&'a Expr) -> Result<TreeNodeRecursion>>(
        &'a self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        // Apply to the series expression
        let recursion = f(&self.expr)?;
        if recursion == TreeNodeRecursion::Stop {
            return Ok(TreeNodeRecursion::Stop);
        }

        // Apply to the default value expression
        f(&self.default_value)
    }

    fn map_elements<F: FnMut(Expr) -> Result<Transformed<Expr>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        // Transform the series expression
        let expr_result = f(self.expr)?;
        let mut transformed = expr_result.transformed;
        let mut tnr = expr_result.tnr;
        let expr = expr_result.data;

        // Transform the default value expression (if we should continue)
        let default_value = match tnr {
            TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {
                let default_value_result = f(self.default_value)?;
                transformed |= default_value_result.transformed;
                tnr = default_value_result.tnr;
                default_value_result.data
            }
            TreeNodeRecursion::Stop => self.default_value,
        };

        Ok(Transformed {
            data: Self {
                expr,
                null_treatment: self.null_treatment,
                default_value,
            },
            transformed,
            tnr,
        })
    }
}

/// Logical plan node for per-series LIMIT and OFFSET operations.
///
/// This logical plan node represents the InfluxQL-style series limiting operation,
/// which applies LIMIT and OFFSET constraints independently to each time series
/// rather than globally across all results. This is a key semantic difference
/// from standard SQL LIMIT/OFFSET.
///
/// # Purpose
///
/// `SeriesLimit` is a custom logical plan node used during query planning for
/// InfluxQL queries. It captures the intent to limit rows on a per-series basis
/// before being converted to a physical execution plan ([`SeriesLimitExec`]).
///
/// # InfluxQL vs SQL Semantics
///
/// ## Standard SQL LIMIT
/// ```sql
/// SELECT value FROM measurements LIMIT 10
/// ```
/// Returns 10 rows total across all series.
///
/// ## InfluxQL Series-based LIMIT
/// ```sql
/// SELECT value1 FROM measurements GROUP BY location LIMIT 10
/// ```
/// Returns up to 10 rows **per location** (per series), potentially returning
/// many more than 10 rows total.
///
/// ```sql
/// SELECT value1, value2 FROM measurements GROUP BY location LIMIT 5 OFFSET 2
/// ```
/// Returns up to 5 rows **per location**, skipping the first 2 rows in each
/// series. Where the series do not have values with matching timestamps the
/// default_value is used to fill in the gaps where required.
///
/// # Important Note: SLIMIT vs LIMIT
///
/// **This operation does NOT implement InfluxQL's `SLIMIT` or `SOFFSET` clauses.**
///
/// - `SLIMIT`/`SOFFSET`: Limit the number of *series* loaded from storage
/// - `LIMIT`/`OFFSET` (this operation): Limit the number of *rows per series*
///
/// For example:
/// - `SLIMIT 5` → Load at most 5 different series from storage
/// - `LIMIT 10` → Return at most 10 rows from each series
///
/// The implementation of `SLIMIT`/`SOFFSET` is tracked by
/// [issue 6940](https://github.com/influxdata/influxdb_iox/issues/6940).
///
/// # Query Structure
///
/// A typical InfluxQL query using series limiting looks like:
///
/// ```text
/// SELECT <limit_expr>...
/// FROM <measurement>
/// WHERE <conditions>
/// GROUP BY <series_expr>...
/// ORDER BY time [ASC|DESC]
/// LIMIT <fetch> OFFSET <skip>
/// ```
///
/// This translates to a `SeriesLimit` node with:
/// - `series_expr`: The GROUP BY columns that define series boundaries
/// - `order_expr`: The time column and sort direction (from ORDER BY)
/// - `limit_expr`: The value columns from SELECT clause
/// - `skip`: The OFFSET value (number of rows to skip per series)
/// - `fetch`: The LIMIT value (max rows to return per series)
///
/// # Components
///
/// ## Series Expressions (`series_expr`)
///
/// Define what constitutes a unique time series. Typically these are tag columns.
/// Rows with identical values for all series expressions belong to the same series.
///
/// Example: `GROUP BY location, sensor_id` → `series_expr = [Column("location"), Column("sensor_id")]`
///
/// ## Order Expressions (`order_expr`)
///
/// Defines the sort ordering within each series. This will always include the time
/// column, but may also include additional columns. Each series is independently
/// sorted by this expression before applying LIMIT/OFFSET.
///
/// Example: `ORDER BY time DESC` → `order_expr = [SortExpr { expr: Column("time"), asc: false, ... }]`
///
/// ## Limit Expressions (`limit_expr`)
///
/// The value columns that should be included in the output. Each has associated
/// NULL handling and default value semantics. See [`LimitExpr`] for details.
///
/// ## Skip and Fetch
///
/// - `skip`: Number of rows to skip at the start of each series (OFFSET)
/// - `fetch`: Maximum number of rows to return from each series (LIMIT)
///
/// Both are optional `Expr` types wrapped in `Box` to allow for dynamic values
/// or literals. If `skip` is `None`, no rows are skipped. If `fetch` is `None`,
/// all remaining rows (after skip) are returned.
///
/// # Examples
///
/// ## Example 1: Basic Per-Series LIMIT
///
/// ```text
/// Query: SELECT temperature FROM weather GROUP BY location LIMIT 3
///
/// SeriesLimit {
///     input: <scan weather table>,
///     series_expr: [Column("location")],
///     order_expr: [SortExpr { expr: Column("time"), asc: true, ... }],
///     limit_expr: [LimitExpr {
///         expr: Column("temperature"),
///         null_treatment: RespectNulls,
///         default_value: Literal(NULL),
///     }],
///     skip: None,
///     fetch: Some(Box::new(Literal(3))),
/// }
///
/// Result: Up to 3 temperature readings per location
/// ```
///
/// ## Example 2: LIMIT with OFFSET
///
/// ```text
/// Query: SELECT value FROM sensors GROUP BY sensor_id LIMIT 10 OFFSET 5
///
/// SeriesLimit {
///     input: <scan sensors table>,
///     series_expr: [Column("sensor_id")],
///     order_expr: [SortExpr { expr: Column("time"), asc: true, ... }],
///     limit_expr: [LimitExpr { expr: Column("value"), ... }],
///     skip: Some(Box::new(Literal(5))),
///     fetch: Some(Box::new(Literal(10))),
/// }
///
/// Result: Rows 6-15 from each sensor (skip first 5, take next 10)
/// ```
///
/// ## Example 3: Multiple Series Keys and Value Columns
///
/// ```text
/// Query: SELECT temp, humidity FROM weather
///        GROUP BY location, elevation
///        LIMIT 5
///
/// SeriesLimit {
///     series_expr: [Column("location"), Column("elevation")],
///     limit_expr: [
///         LimitExpr { expr: Column("temp"), ... },
///         LimitExpr { expr: Column("humidity"), ... },
///     ],
///     fetch: Some(Box::new(Literal(5))),
///     ...
/// }
///
/// Result: Up to 5 rows each for temp and humidity per (location, elevation) combination,
/// if temp and humidity have different timestamps this could result it up to 10 output
/// rows per (location, elevation).
/// ```
///
/// # See Also
///
/// - [`LimitExpr`]: The expression type for individual value columns
/// - [`SeriesLimitExec`]: The physical execution plan that implements this operation
///
/// [`SeriesLimitExec`]: crate::exec::series_limit::physical::SeriesLimitExec
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct SeriesLimit {
    /// The input for this operation.
    pub input: Arc<LogicalPlan>,

    /// The expressions that definge which series a particular row is
    /// part of.
    pub series_expr: Vec<Expr>,

    /// The expression that defines the ordering of the rows within a
    /// series.
    pub order_expr: Vec<SortExpr>,

    /// The expressions that define the values of each time series that
    /// needs to be processed. Each expression must reference exactly
    /// one column in the input which will be replaced with the output
    /// with the limited version of that column. No two limit
    /// expressions may reference the same input column.
    pub limit_expr: Vec<LimitExpr>,

    /// The number of rows to skip (OFFSET) in each time series.
    pub skip: Option<Box<Expr>>,

    /// The maximum number of rows (LIMIT) to include in each time
    /// series.
    pub fetch: Option<Box<Expr>>,

    /// The schema of the output of this operation.
    pub schema: DFSchemaRef,
}

impl SeriesLimit {
    pub fn try_new(
        input: Arc<LogicalPlan>,
        series_expr: Vec<Expr>,
        order_expr: Vec<SortExpr>,
        limit_expr: Vec<LimitExpr>,
        skip: Option<Box<Expr>>,
        fetch: Option<Box<Expr>>,
    ) -> Result<Self> {
        // Validate that the expressions are all valid against the input schema
        let input_schema = input.schema();

        let mut limited_fields = BTreeMap::new();
        for le in &limit_expr {
            let cols = le.expr.column_refs();
            if cols.len() != 1 {
                return internal_err!(
                    "LimitExpr expr must reference exactly one column, found {} columns",
                    cols.len()
                );
            }
            let col = cols.into_iter().next().unwrap();
            let idx = input_schema.index_of_column(col)?;
            if limited_fields
                .insert(idx, le.to_field(input_schema)?)
                .is_some()
            {
                return internal_err!("LimitExpr contains duplicate column reference: {}", col);
            }
        }

        // The schema is the same as the input with the limited fields
        // potentially modified.
        let qualified_fields = input_schema
            .iter()
            .enumerate()
            .map(|(idx, (qualifier, field))| {
                if let Some((qualifier, field)) = limited_fields.remove(&idx) {
                    (qualifier, field)
                } else {
                    (qualifier.cloned(), Arc::clone(field))
                }
            })
            .collect::<Vec<_>>();

        let schema = Arc::new(DFSchema::new_with_metadata(
            qualified_fields,
            std::collections::HashMap::new(),
        )?);

        Ok(Self {
            input,
            series_expr,
            order_expr,
            limit_expr,
            skip,
            fetch,
            schema,
        })
    }

    pub fn apply_expressions<F: FnMut(&Expr) -> Result<TreeNodeRecursion>>(
        &self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        if self.series_expr.apply_elements(&mut f)? == TreeNodeRecursion::Stop {
            return Ok(TreeNodeRecursion::Stop);
        }
        if self.order_expr.apply_elements(&mut f)? == TreeNodeRecursion::Stop {
            return Ok(TreeNodeRecursion::Stop);
        }
        if self.limit_expr.apply_elements(&mut f)? == TreeNodeRecursion::Stop {
            return Ok(TreeNodeRecursion::Stop);
        }
        if self.skip.apply_elements(&mut f)? == TreeNodeRecursion::Stop {
            return Ok(TreeNodeRecursion::Stop);
        }
        if self.fetch.apply_elements(&mut f)? == TreeNodeRecursion::Stop {
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

// Manual impl because DFSchemaRef doesn't implement PartialOrd
impl PartialOrd for SeriesLimit {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;

        // Compare inputs
        match self.input.partial_cmp(&other.input) {
            Some(Ordering::Equal) => {}
            other => return other,
        }

        // Compare series expressions
        match self.series_expr.partial_cmp(&other.series_expr) {
            Some(Ordering::Equal) => {}
            other => return other,
        }

        // Compare order expressions
        match self.order_expr.partial_cmp(&other.order_expr) {
            Some(Ordering::Equal) => {}
            other => return other,
        }

        // Compare limit expressions
        match self.limit_expr.partial_cmp(&other.limit_expr) {
            Some(Ordering::Equal) => {}
            other => return other,
        }

        // Compare skip
        match self.skip.partial_cmp(&other.skip) {
            Some(Ordering::Equal) => {}
            other => return other,
        }

        // Compare fetch (skip schema since it doesn't implement PartialOrd)
        self.fetch.partial_cmp(&other.fetch)
    }
}

impl UserDefinedLogicalNodeCore for SeriesLimit {
    fn name(&self) -> &str {
        "SeriesLimit"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = Vec::with_capacity(
            self.series_expr.len() + self.order_expr.len() + 2 * self.limit_expr.len() + 2,
        );

        self.apply_expressions(|expr| {
            exprs.push(expr.clone());
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("cannot error");

        exprs
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let series_expr = self
            .series_expr
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<String>>()
            .join(", ");

        let order_expr = self
            .order_expr
            .iter()
            .map(|se| se.to_string())
            .collect::<Vec<String>>()
            .join(", ");

        let limit_expr = self
            .limit_expr
            .iter()
            .map(|le| le.to_string())
            .collect::<Vec<String>>()
            .join(", ");

        write!(
            f,
            "{}: series=[{}], order=[{}], limit_expr=[{}]",
            self.name(),
            series_expr,
            order_expr,
            limit_expr,
        )?;

        if let Some(skip) = &self.skip {
            write!(f, ", skip={}", skip)?;
        }

        if let Some(fetch) = &self.fetch {
            write!(f, ", fetch={}", fetch)?;
        }

        Ok(())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!("SeriesLimit expects exactly 1 input, got {}", inputs.len());
        }

        let input = Arc::new(inputs.into_iter().next().unwrap());

        fn map_exprs<'a, C: TreeNodeContainer<'a, Expr>>(
            c: C,
            exprs: &mut impl Iterator<Item = Expr>,
        ) -> Result<C> {
            c.map_elements(|old| {
                exprs
                    .next()
                    .map(|new| {
                        let transformed = new == old;
                        Transformed::new(new, transformed, TreeNodeRecursion::Continue)
                    })
                    .ok_or(DataFusionError::Internal(String::from(
                        "not enough input expressions for SeriesLimit",
                    )))
            })
            .map(|t| t.data)
        }

        let Self {
            series_expr,
            order_expr,
            limit_expr,
            skip,
            fetch,
            ..
        } = self;

        let mut exprs = exprs.into_iter();

        let series_expr = map_exprs(series_expr.clone(), &mut exprs)?;
        let order_expr = map_exprs(order_expr.clone(), &mut exprs)?;
        let limit_expr = map_exprs(limit_expr.clone(), &mut exprs)?;
        let skip = map_exprs(skip.clone(), &mut exprs)?;
        let fetch = map_exprs(fetch.clone(), &mut exprs)?;

        if exprs.next().is_some() {
            return internal_err!("too many input expressions for SeriesLimit");
        }

        Self::try_new(input, series_expr, order_expr, limit_expr, skip, fetch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        logical_expr::{col, lit},
        prelude::SessionContext,
        scalar::ScalarValue,
    };
    use insta::assert_snapshot;

    /// Helper function to create a simple test schema
    fn test_schema() -> DFSchemaRef {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = SessionContext::new();
            Arc::clone(
                ctx.sql("SELECT 1 as a, 2 as b, 3 as time")
                    .await
                    .unwrap()
                    .into_optimized_plan()
                    .unwrap()
                    .schema(),
            )
        })
    }

    /// Helper function to create a simple LogicalPlan for testing
    fn test_plan() -> Arc<LogicalPlan> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = SessionContext::new();
            Arc::new(
                ctx.sql("SELECT 1 as a, 2 as b, 3 as time")
                    .await
                    .unwrap()
                    .into_optimized_plan()
                    .unwrap(),
            )
        })
    }

    fn test_plan2() -> Arc<LogicalPlan> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = SessionContext::new();
            Arc::new(
                ctx.sql("SELECT 3 as a, 2 as b, 1 as time")
                    .await
                    .unwrap()
                    .into_optimized_plan()
                    .unwrap(),
            )
        })
    }

    /// Helper function to create a LogicalPlan with more columns for testing
    fn test_plan_multi_column() -> Arc<LogicalPlan> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = SessionContext::new();
            Arc::new(
                ctx.sql("SELECT 1 as a, 2 as b, 3 as c, 4 as time")
                    .await
                    .unwrap()
                    .into_optimized_plan()
                    .unwrap(),
            )
        })
    }

    mod limit_expr_tests {
        use super::*;

        #[test]
        fn test_display() {
            let limit_expr = LimitExpr {
                expr: col("temperature"),
                null_treatment: NullTreatment::RespectNulls,
                default_value: lit(0),
            };

            assert_snapshot!(limit_expr.to_string(), @r#"temperature RESPECT NULLS (default: Int32(0))"#);
        }

        #[test]
        fn test_get_type_matching_types() {
            let schema = test_schema();
            let limit_expr = LimitExpr {
                expr: col("a"),
                null_treatment: NullTreatment::RespectNulls,
                default_value: lit(ScalarValue::Int64(Some(42))),
            };

            assert_eq!(
                limit_expr.get_type(schema.as_ref()).unwrap(),
                DataType::Int64
            );
        }

        #[test]
        fn test_nullable() {
            let schema = test_schema();
            let limit_expr = LimitExpr {
                expr: col("a"),
                null_treatment: NullTreatment::RespectNulls,
                default_value: lit(ScalarValue::Int64(Some(42))),
            };

            assert!(!limit_expr.nullable(schema.as_ref()).unwrap());
        }

        #[test]
        fn test_data_type_and_nullable() {
            let schema = test_schema();
            let limit_expr = LimitExpr {
                expr: col("a"),
                null_treatment: NullTreatment::RespectNulls,
                default_value: lit(ScalarValue::Int64(Some(42))),
            };

            assert_eq!(
                limit_expr.data_type_and_nullable(schema.as_ref()).unwrap(),
                (DataType::Int64, false)
            );
        }

        #[test]
        fn test_to_field() {
            let schema = test_schema();
            let limit_expr = LimitExpr {
                expr: col("a"),
                null_treatment: NullTreatment::RespectNulls,
                default_value: lit(ScalarValue::Int64(Some(42))),
            };

            assert_eq!(
                limit_expr.to_field(schema.as_ref()).unwrap(),
                (None, Arc::new(Field::new("a", DataType::Int64, false)))
            );
        }

        #[test]
        fn test_tree_node_container_apply_elements() {
            let limit_expr = LimitExpr {
                expr: col("a"),
                null_treatment: NullTreatment::RespectNulls,
                default_value: lit(42),
            };

            let mut count = 0;
            let result = limit_expr.apply_elements(|_expr| {
                count += 1;
                Ok(TreeNodeRecursion::Continue)
            });

            assert_eq!(result.unwrap(), TreeNodeRecursion::Continue);
            assert_eq!(count, 2); // Should visit both expr and default_value
        }

        #[test]
        fn test_tree_node_container_apply_elements_stop() {
            let limit_expr = LimitExpr {
                expr: col("a"),
                null_treatment: NullTreatment::RespectNulls,
                default_value: lit(42),
            };

            let mut count = 0;
            let result = limit_expr.apply_elements(|_expr| {
                count += 1;
                Ok(TreeNodeRecursion::Stop)
            });

            assert!(result.is_ok());
            assert_eq!(result.unwrap(), TreeNodeRecursion::Stop);
            assert_eq!(count, 1); // Should stop after first expression
        }

        #[test]
        fn test_tree_node_container_map_elements() {
            let limit_expr = LimitExpr {
                expr: col("a"),
                null_treatment: NullTreatment::RespectNulls,
                default_value: lit(42),
            };

            let result = limit_expr
                .clone()
                .map_elements(|expr| Ok(Transformed::no(expr)));

            assert_eq!(result.unwrap(), Transformed::no(limit_expr));
        }

        #[test]
        fn test_tree_node_container_map_elements_with_transform() {
            let limit_expr = LimitExpr {
                expr: col("a"),
                null_treatment: NullTreatment::RespectNulls,
                default_value: lit(42),
            };

            let result = limit_expr
                .clone()
                .map_elements(|expr| Ok(Transformed::yes(expr)));

            assert_eq!(result.unwrap(), Transformed::yes(limit_expr));
        }
    }

    mod series_limit_tests {
        use super::*;
        use arrow::datatypes::Fields;
        use datafusion::logical_expr::{Extension, SortExpr};

        fn create_test_series_limit() -> SeriesLimit {
            let input = test_plan();
            let series_expr = vec![col("a")];
            let order_expr = vec![SortExpr {
                expr: col("time"),
                asc: true,
                nulls_first: false,
            }];
            let limit_expr = vec![LimitExpr {
                expr: col("b"),
                null_treatment: NullTreatment::RespectNulls,
                default_value: lit(ScalarValue::Int64(Some(0))),
            }];

            SeriesLimit::try_new(input, series_expr, order_expr, limit_expr, None, None).unwrap()
        }

        #[test]
        fn test_try_new() {
            let input = test_plan();
            let series_expr = vec![col("a")];
            let order_expr = vec![SortExpr {
                expr: col("time"),
                asc: true,
                nulls_first: false,
            }];
            let limit_expr = vec![LimitExpr {
                expr: col("b"),
                null_treatment: NullTreatment::RespectNulls,
                default_value: lit(ScalarValue::Int64(Some(0))),
            }];

            let result =
                SeriesLimit::try_new(input, series_expr, order_expr, limit_expr, None, None);
            assert!(result.is_ok());
        }

        #[test]
        fn test_try_new_with_skip_and_fetch() {
            let input = test_plan();
            let series_expr = vec![col("a")];
            let order_expr = vec![SortExpr {
                expr: col("time"),
                asc: true,
                nulls_first: false,
            }];
            let limit_expr = vec![LimitExpr {
                expr: col("b"),
                null_treatment: NullTreatment::RespectNulls,
                default_value: lit(ScalarValue::Int64(Some(0))),
            }];
            let skip = Some(Box::new(lit(10)));
            let fetch = Some(Box::new(lit(100)));

            let result =
                SeriesLimit::try_new(input, series_expr, order_expr, limit_expr, skip, fetch);
            assert!(result.is_ok());

            let series_limit = result.unwrap();
            assert!(series_limit.skip.is_some());
            assert!(series_limit.fetch.is_some());
        }

        #[test]
        fn test_name() {
            let series_limit = create_test_series_limit();
            assert_eq!(series_limit.name(), "SeriesLimit");
        }

        #[test]
        fn test_inputs() {
            let series_limit = create_test_series_limit();
            let inputs = series_limit.inputs();
            assert_eq!(inputs.len(), 1);
        }

        #[test]
        fn test_schema() {
            let series_limit = create_test_series_limit();
            let schema = series_limit.schema();
            assert_eq!(
                schema.fields(),
                &Fields::from(vec![
                    Field::new("a", DataType::Int64, false),
                    Field::new("b", DataType::Int64, false),
                    Field::new("time", DataType::Int64, false),
                ])
            );
        }

        #[test]
        fn test_expressions() {
            let series_limit = create_test_series_limit();
            assert_eq!(
                series_limit.expressions(),
                vec![col("a"), col("time"), col("b"), lit(0_i64)]
            );
        }

        #[test]
        fn test_expressions_with_skip_and_fetch() {
            let input = test_plan();
            let series_expr = vec![col("a")];
            let order_expr = vec![SortExpr {
                expr: col("time"),
                asc: true,
                nulls_first: false,
            }];
            let limit_expr = vec![LimitExpr {
                expr: col("b"),
                null_treatment: NullTreatment::RespectNulls,
                default_value: lit(ScalarValue::Int64(Some(0))),
            }];
            let skip = Some(Box::new(lit(10)));
            let fetch = Some(Box::new(lit(100)));

            let series_limit =
                SeriesLimit::try_new(input, series_expr, order_expr, limit_expr, skip, fetch)
                    .unwrap();

            assert_eq!(
                series_limit.expressions(),
                vec![
                    col("a"),
                    col("time"),
                    col("b"),
                    lit(0_i64),
                    lit(10),
                    lit(100)
                ]
            );
        }

        #[test]
        fn test_fmt_for_explain() {
            let series_limit = create_test_series_limit();
            assert_snapshot!(format!("{}", LogicalPlan::Extension(Extension{node: Arc::new(series_limit)})), @r"
            SeriesLimit: series=[a], order=[time ASC NULLS LAST], limit_expr=[b RESPECT NULLS (default: Int64(0))]
              Projection: Int64(1) AS a, Int64(2) AS b, Int64(3) AS time
                EmptyRelation: rows=1
            ")
        }

        #[test]
        fn test_with_exprs_and_inputs() {
            let series_limit = create_test_series_limit();
            let original_exprs = series_limit.expressions();

            // Create new input
            let new_input = test_plan2();

            let series_limit = series_limit
                .with_exprs_and_inputs(original_exprs, vec![(*new_input).clone()])
                .unwrap();

            assert_snapshot!(format!("{}", LogicalPlan::Extension(Extension{node: Arc::new(series_limit)})), @r"
            SeriesLimit: series=[a], order=[time ASC NULLS LAST], limit_expr=[b RESPECT NULLS (default: Int64(0))]
              Projection: Int64(3) AS a, Int64(2) AS b, Int64(1) AS time
                EmptyRelation: rows=1
            ")
        }

        #[test]
        fn test_with_exprs_and_inputs_wrong_input_count() {
            let series_limit = create_test_series_limit();
            let original_exprs = series_limit.expressions();

            // Try with wrong number of inputs (0 instead of 1)
            let result = series_limit.with_exprs_and_inputs(original_exprs, vec![]);

            assert!(result.is_err());
            let err_str = result.unwrap_err().to_string();
            assert!(err_str.contains("expects exactly 1 input"));
        }

        #[test]
        fn test_partial_ord() {
            let series_limit1 = create_test_series_limit();
            let series_limit2 = create_test_series_limit();

            // Should be equal
            assert_eq!(
                series_limit1.partial_cmp(&series_limit2),
                Some(std::cmp::Ordering::Equal)
            );
        }

        #[test]
        fn test_eq() {
            let series_limit1 = create_test_series_limit();
            let series_limit2 = create_test_series_limit();

            assert_eq!(series_limit1, series_limit2);
        }

        #[test]
        fn test_hash() {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            let series_limit1 = create_test_series_limit();
            let series_limit2 = create_test_series_limit();

            let mut hasher1 = DefaultHasher::new();
            series_limit1.hash(&mut hasher1);
            let hash1 = hasher1.finish();

            let mut hasher2 = DefaultHasher::new();
            series_limit2.hash(&mut hasher2);
            let hash2 = hasher2.finish();

            assert_eq!(hash1, hash2);
        }

        #[test]
        fn test_multiple_order_expressions() {
            // Test with multiple order expressions (e.g., ORDER BY time, b)
            // Schema has: a, b, c, time
            let input = test_plan_multi_column();
            let series_expr = vec![col("a")];
            let order_expr = vec![
                SortExpr {
                    expr: col("time"),
                    asc: true,
                    nulls_first: false,
                },
                SortExpr {
                    expr: col("b"),
                    asc: false,
                    nulls_first: true,
                },
            ];
            let limit_expr = vec![LimitExpr {
                expr: col("c"),
                null_treatment: NullTreatment::RespectNulls,
                default_value: lit(ScalarValue::Int64(Some(0))),
            }];

            let series_limit =
                SeriesLimit::try_new(input, series_expr, order_expr, limit_expr, None, None)
                    .unwrap();

            assert_snapshot!(format!("{}", LogicalPlan::Extension(Extension{node: Arc::new(series_limit)})), @r"
            SeriesLimit: series=[a], order=[time ASC NULLS LAST, b DESC NULLS FIRST], limit_expr=[c RESPECT NULLS (default: Int64(0))]
              Projection: Int64(1) AS a, Int64(2) AS b, Int64(3) AS c, Int64(4) AS time
                EmptyRelation: rows=1
            ")
        }

        #[test]
        fn test_expressions_with_multiple_order() {
            // Test that expressions() includes all order expressions
            let input = test_plan_multi_column();
            let series_expr = vec![col("a")];
            let order_expr = vec![
                SortExpr {
                    expr: col("time"),
                    asc: true,
                    nulls_first: false,
                },
                SortExpr {
                    expr: col("b"),
                    asc: false,
                    nulls_first: true,
                },
            ];
            let limit_expr = vec![LimitExpr {
                expr: col("c"),
                null_treatment: NullTreatment::RespectNulls,
                default_value: lit(ScalarValue::Int64(Some(0))),
            }];

            let series_limit =
                SeriesLimit::try_new(input, series_expr, order_expr, limit_expr, None, None)
                    .unwrap();

            let exprs = series_limit.expressions();
            assert_eq!(
                exprs,
                vec![col("a"), col("time"), col("b"), col("c"), lit(0_i64)]
            );
        }

        #[test]
        fn test_with_exprs_and_inputs_multiple_order() {
            // Test with_exprs_and_inputs preserves multiple order expressions
            let input = test_plan_multi_column();
            let series_expr = vec![col("a")];
            let order_expr = vec![
                SortExpr {
                    expr: col("time"),
                    asc: true,
                    nulls_first: false,
                },
                SortExpr {
                    expr: col("b"),
                    asc: false,
                    nulls_first: true,
                },
            ];
            let limit_expr = vec![LimitExpr {
                expr: col("c"),
                null_treatment: NullTreatment::RespectNulls,
                default_value: lit(ScalarValue::Int64(Some(0))),
            }];

            let series_limit = SeriesLimit::try_new(
                Arc::clone(&input),
                series_expr,
                order_expr,
                limit_expr,
                None,
                None,
            )
            .unwrap();

            let original_exprs = series_limit.expressions();
            let new_input = test_plan_multi_column();

            let series_limit = series_limit
                .with_exprs_and_inputs(original_exprs, vec![(*new_input).clone()])
                .unwrap();

            assert_snapshot!(format!("{}", LogicalPlan::Extension(Extension{node: Arc::new(series_limit)})), @r"
            SeriesLimit: series=[a], order=[time ASC NULLS LAST, b DESC NULLS FIRST], limit_expr=[c RESPECT NULLS (default: Int64(0))]
              Projection: Int64(1) AS a, Int64(2) AS b, Int64(3) AS c, Int64(4) AS time
                EmptyRelation: rows=1
            ")
        }

        #[test]
        fn test_fmt_for_explain_multiple_order() {
            // Test that fmt_for_explain includes all order expressions
            let input = test_plan_multi_column();
            let series_expr = vec![col("a")];
            let order_expr = vec![
                SortExpr {
                    expr: col("time"),
                    asc: true,
                    nulls_first: false,
                },
                SortExpr {
                    expr: col("b"),
                    asc: false,
                    nulls_first: true,
                },
            ];
            let limit_expr = vec![LimitExpr {
                expr: col("c"),
                null_treatment: NullTreatment::RespectNulls,
                default_value: lit(ScalarValue::Int64(Some(0))),
            }];

            let series_limit =
                SeriesLimit::try_new(input, series_expr, order_expr, limit_expr, None, None)
                    .unwrap();

            assert_snapshot!(format!("{}", LogicalPlan::Extension(Extension{node: Arc::new(series_limit)})), @r"
            SeriesLimit: series=[a], order=[time ASC NULLS LAST, b DESC NULLS FIRST], limit_expr=[c RESPECT NULLS (default: Int64(0))]
              Projection: Int64(1) AS a, Int64(2) AS b, Int64(3) AS c, Int64(4) AS time
                EmptyRelation: rows=1
            ")
        }
    }
}
