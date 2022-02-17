//! Interface logic between IOx ['Predicate`] and predicates used by the
//! InfluxDB Storage gRPC API
use crate::{rewrite, BinaryExpr, Predicate};

use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_plan::{
    lit, Column, Expr, ExprRewritable, ExprRewriter, ExprSchema, ExprSchemable, ExprSimplifiable,
    Operator, SimplifyInfo,
};
use datafusion::scalar::ScalarValue;
use datafusion_util::AsExpr;
use schema::Schema;
use std::collections::BTreeSet;
use std::sync::Arc;

/// Any column references to this name are rewritten to be
/// the actual table name by the Influx gRPC planner.
///
/// This is required to support predicates like
/// `_measurement = "foo" OR tag1 = "bar"`
///
/// The plan for each table will have the value of `_measurement`
/// filled in with a literal for the respective name of that field
pub const MEASUREMENT_COLUMN_NAME: &str = "_measurement";

/// Any equality expressions using this column name are removed and replaced
/// with projections on the specified column.
///
/// This is required to support predicates like
/// `_field` = temperature
pub const FIELD_COLUMN_NAME: &str = "_field";

/// Any column references to this name are rewritten to be a disjunctive set of
/// expressions to all field columns for the table schema.
///
/// This is required to support predicates like
/// `_value` = 1.77
///
/// The plan for each table will have expression containing `_value` rewritten
/// into multiple expressions (one for each field column).
pub const VALUE_COLUMN_NAME: &str = "_value";

/// Predicate used by the InfluxDB Storage gRPC API
#[derive(Debug, Clone, Default)]
pub struct InfluxRpcPredicate {
    /// Optional table restriction. If present, restricts the results
    /// to only tables whose names are in `table_names`
    table_names: Option<BTreeSet<String>>,

    /// The inner predicate
    inner: Predicate,
}

impl InfluxRpcPredicate {
    /// Create a new [`InfluxRpcPredicate`]
    pub fn new(table_names: Option<BTreeSet<String>>, predicate: Predicate) -> Self {
        Self {
            table_names,
            inner: predicate,
        }
    }

    /// Create a new [`InfluxRpcPredicate`] with the given table
    pub fn new_table(table: impl Into<String>, predicate: Predicate) -> Self {
        Self::new(Some(std::iter::once(table.into()).collect()), predicate)
    }

    /// Removes the timestamp range from this predicate, if the range
    /// is for the entire min/max valid range.
    ///
    /// This is used in certain cases to retain compatibility with the
    /// existing storage engine
    pub fn clear_timestamp_if_max_range(self) -> Self {
        Self {
            inner: self.inner.clear_timestamp_if_max_range(),
            ..self
        }
    }

    /// Convert to a list of [`Predicate`] to apply to specific tables
    ///
    /// Returns a list of [`Predicate`] and their associated table name
    pub fn table_predicates<D: QueryDatabaseMeta>(
        &self,
        table_info: &D,
    ) -> DataFusionResult<Vec<(String, Predicate)>> {
        let table_names = match &self.table_names {
            Some(table_names) => itertools::Either::Left(table_names.iter().cloned()),
            None => itertools::Either::Right(table_info.table_names().into_iter()),
        };

        table_names
            .map(|table| {
                let schema = table_info.table_schema(&table);
                let predicate = normalize_predicate(&table, schema, &self.inner)?;

                Ok((table, predicate))
            })
            .collect()
    }

    /// Returns the table names this predicate is restricted to if any
    pub fn table_names(&self) -> Option<&BTreeSet<String>> {
        self.table_names.as_ref()
    }

    /// Returns true if ths predicate evaluates to true for all rows
    pub fn is_empty(&self) -> bool {
        self.table_names.is_none() && self.inner.is_empty()
    }
}

/// Information required to normalize predicates
pub trait QueryDatabaseMeta {
    /// Returns a list of table names in this DB
    fn table_names(&self) -> Vec<String>;

    /// Schema for a specific table if the table exists.
    fn table_schema(&self, table_name: &str) -> Option<Arc<Schema>>;
}

/// Predicate that has been "specialized" / normalized for a
/// particular table. Specifically:
///
/// * all references to the [MEASUREMENT_COLUMN_NAME] column in any
/// `Exprs` are rewritten with the actual table name
/// * any expression on the [VALUE_COLUMN_NAME] column is rewritten to be
/// applied across all field columns.
/// * any expression on the [FIELD_COLUMN_NAME] is rewritten to be
/// applied for the particular fields.
///
/// For example if the original predicate was
/// ```text
/// _measurement = "some_table"
/// ```
///
/// When evaluated on table "cpu" then the predicate is rewritten to
/// ```text
/// "cpu" = "some_table"
/// ```
///
/// if the original predicate contained
/// ```text
/// _value > 34.2
/// ```
///
/// When evaluated on table "cpu" then the expression is rewritten as a
/// collection of disjunctive expressions against all field columns
/// ```text
/// ("field1" > 34.2 OR "field2" > 34.2 OR "fieldn" > 34.2)
/// ```
fn normalize_predicate(
    table_name: &str,
    schema: Option<Arc<Schema>>,
    predicate: &Predicate,
) -> DataFusionResult<Predicate> {
    let mut predicate = predicate.clone();
    let mut field_projections = BTreeSet::new();
    let mut field_value_exprs = vec![];

    predicate.exprs = predicate
        .exprs
        .into_iter()
        .map(|e| {
            rewrite_measurement_references(table_name, e)
                // Rewrite any references to `_value = some_value` to literal true values.
                // Keeps track of these expressions, which can then be used to
                // augment field projections with conditions using `CASE` statements.
                .and_then(|e| rewrite_field_value_references(&mut field_value_exprs, e))
                // Rewrite any references to `_field = a_field_name` with a literal true
                // and keep track of referenced field names to add to the field
                // column projection set.
                .and_then(|e| rewrite_field_column_references(&mut field_projections, e))
                // apply IOx specific rewrites (that unlock other simplifications)
                .and_then(rewrite::rewrite)
                // Call the core DataFusion simplification logic
                .and_then(|e| {
                    if let Some(schema) = &schema {
                        let adapter = SimplifyAdapter::new(schema.as_ref());
                        // simplify twice to ensure "full" cleanup
                        e.simplify(&adapter)?.simplify(&adapter)
                    } else {
                        Ok(e)
                    }
                })
                .and_then(rewrite::simplify_predicate)
        })
        .collect::<DataFusionResult<Vec<_>>>()?;
    // Store any field value (`_value`) expressions on the `Predicate`.
    predicate.value_expr = field_value_exprs;

    if !field_projections.is_empty() {
        match &mut predicate.field_columns {
            Some(field_columns) => field_columns.extend(field_projections.into_iter()),
            None => predicate.field_columns = Some(field_projections),
        };
    }
    Ok(predicate)
}

struct SimplifyAdapter<'a> {
    schema: &'a Schema,
    execution_props: ExecutionProps,
}

impl<'a> SimplifyAdapter<'a> {
    fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            execution_props: ExecutionProps::new(),
        }
    }

    // returns the field named 'name', if any
    fn field(&self, name: &str) -> Option<&arrow::datatypes::Field> {
        self.schema
            .find_index_of(name)
            .map(|index| self.schema.field(index).1)
    }
}

impl<'a> SimplifyInfo for SimplifyAdapter<'a> {
    fn is_boolean_type(&self, expr: &Expr) -> DataFusionResult<bool> {
        Ok(expr
            .get_type(self)
            .ok()
            .map(|t| matches!(t, arrow::datatypes::DataType::Boolean))
            .unwrap_or(false))
    }

    fn nullable(&self, expr: &Expr) -> DataFusionResult<bool> {
        Ok(expr.nullable(self).ok().unwrap_or(false))
    }

    fn execution_props(&self) -> &ExecutionProps {
        &self.execution_props
    }
}

impl<'a> ExprSchema for SimplifyAdapter<'a> {
    fn nullable(&self, col: &Column) -> DataFusionResult<bool> {
        assert!(col.relation.is_none());
        //if the field isn't present IOx will treat it as null
        Ok(self
            .field(&col.name)
            .map(|f| f.is_nullable())
            .unwrap_or(true))
    }

    fn data_type(&self, col: &Column) -> DataFusionResult<&arrow::datatypes::DataType> {
        assert!(col.relation.is_none());
        self.field(&col.name)
            .map(|f| f.data_type())
            .ok_or_else(|| DataFusionError::Plan(format!("Unknown field {}", &col.name)))
    }
}

/// Rewrites all references to the [MEASUREMENT_COLUMN_NAME] column
/// with the actual table name
fn rewrite_measurement_references(table_name: &str, expr: Expr) -> DataFusionResult<Expr> {
    let mut rewriter = MeasurementRewriter { table_name };
    expr.rewrite(&mut rewriter)
}

struct MeasurementRewriter<'a> {
    table_name: &'a str,
}

impl ExprRewriter for MeasurementRewriter<'_> {
    fn mutate(&mut self, expr: Expr) -> DataFusionResult<Expr> {
        Ok(match expr {
            // rewrite col("_measurement") --> "table_name"
            Expr::Column(Column { relation, name }) if name == MEASUREMENT_COLUMN_NAME => {
                // should not have a qualified foo._measurement
                // reference
                assert!(relation.is_none());
                lit(self.table_name)
            }
            // no rewrite needed
            _ => expr,
        })
    }
}

/// Rewrites an expression on `_value` as a boolean true literal, pushing any
/// encountered expressions onto `value_exprs` so they can be moved onto column
/// projections.
fn rewrite_field_value_references(
    value_exprs: &mut Vec<BinaryExpr>,
    expr: Expr,
) -> DataFusionResult<Expr> {
    let mut rewriter = FieldValueRewriter { value_exprs };
    expr.rewrite(&mut rewriter)
}

struct FieldValueRewriter<'a> {
    value_exprs: &'a mut Vec<BinaryExpr>,
}

impl<'a> ExprRewriter for FieldValueRewriter<'a> {
    fn mutate(&mut self, expr: Expr) -> DataFusionResult<Expr> {
        Ok(match expr {
            Expr::BinaryExpr {
                ref left,
                op,
                ref right,
            } => {
                if let Expr::Column(inner) = &**left {
                    if inner.name == VALUE_COLUMN_NAME {
                        self.value_exprs.push(BinaryExpr {
                            left: inner.to_owned(),
                            op,
                            right: right.as_expr(),
                        });
                        return Ok(lit(true));
                    }
                }
                expr
            }
            _ => expr,
        })
    }
}

/// Rewrites a predicate on `_field` as a projection on a specific defined by
/// the literal in the expression.
///
/// For example, the expression `_field = "load4"` is removed from the
/// normalised expression, and a column "load4" added to the predicate
/// projection.
fn rewrite_field_column_references(
    field_projections: &'_ mut BTreeSet<String>,
    expr: Expr,
) -> DataFusionResult<Expr> {
    let mut rewriter = FieldColumnRewriter { field_projections };
    expr.rewrite(&mut rewriter)
}

struct FieldColumnRewriter<'a> {
    field_projections: &'a mut BTreeSet<String>,
}

impl<'a> ExprRewriter for FieldColumnRewriter<'a> {
    fn mutate(&mut self, expr: Expr) -> DataFusionResult<Expr> {
        Ok(match expr {
            Expr::BinaryExpr {
                ref left,
                op,
                ref right,
            } => {
                if let Expr::Column(inner) = &**left {
                    if inner.name != FIELD_COLUMN_NAME || op != Operator::Eq {
                        // TODO(edd): add support for !=
                        return Ok(expr);
                    }

                    if let Expr::Literal(ScalarValue::Utf8(Some(name))) = &**right {
                        self.field_projections.insert(name.to_owned());
                        return Ok(lit(true));
                    }
                }
                expr
            }
            _ => expr,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_plan::{binary_expr, col};

    #[test]
    fn test_field_value_rewriter() {
        let mut rewriter = FieldValueRewriter {
            value_exprs: &mut vec![],
        };

        let cases = vec![
            (
                binary_expr(col("f1"), Operator::Eq, lit(1.82)),
                binary_expr(col("f1"), Operator::Eq, lit(1.82)),
                vec![],
            ),
            (col("t2"), col("t2"), vec![]),
            (
                binary_expr(col(VALUE_COLUMN_NAME), Operator::Eq, lit(1.82)),
                // _value = 1.82 -> true
                lit(true),
                vec![BinaryExpr {
                    left: Column {
                        relation: None,
                        name: VALUE_COLUMN_NAME.into(),
                    },
                    op: Operator::Eq,
                    right: lit(1.82),
                }],
            ),
        ];

        for (input, exp, mut value_exprs) in cases {
            let rewritten = input.rewrite(&mut rewriter).unwrap();
            assert_eq!(rewritten, exp);
            assert_eq!(rewriter.value_exprs, &mut value_exprs);
        }

        // Test case with single field.
        let mut rewriter = FieldValueRewriter {
            value_exprs: &mut vec![],
        };

        let input = binary_expr(col(VALUE_COLUMN_NAME), Operator::Gt, lit(1.88));
        let rewritten = input.rewrite(&mut rewriter).unwrap();
        assert_eq!(rewritten, lit(true));
        assert_eq!(
            rewriter.value_exprs,
            &mut vec![BinaryExpr {
                left: Column {
                    relation: None,
                    name: VALUE_COLUMN_NAME.into(),
                },
                op: Operator::Gt,
                right: lit(1.88),
            }]
        );
    }

    #[test]
    fn test_field_column_rewriter() {
        let mut field_columns = BTreeSet::new();
        let mut rewriter = FieldColumnRewriter {
            field_projections: &mut field_columns,
        };

        let cases = vec![
            (
                binary_expr(col("f1"), Operator::Eq, lit(1.82)),
                binary_expr(col("f1"), Operator::Eq, lit(1.82)),
                vec![],
            ),
            (
                // TODO - should be rewritten and project onto all field columns
                binary_expr(col(FIELD_COLUMN_NAME), Operator::NotEq, lit("foo")),
                binary_expr(col(FIELD_COLUMN_NAME), Operator::NotEq, lit("foo")),
                vec![],
            ),
            (
                binary_expr(col(FIELD_COLUMN_NAME), Operator::Eq, lit("f1")),
                lit(true),
                vec!["f1"],
            ),
            (
                binary_expr(
                    binary_expr(col(FIELD_COLUMN_NAME), Operator::Eq, lit("f1")),
                    Operator::Or,
                    binary_expr(col(FIELD_COLUMN_NAME), Operator::Eq, lit("f2")),
                ),
                binary_expr(lit(true), Operator::Or, lit(true)),
                vec!["f1", "f2"],
            ),
        ];

        for (input, exp_expr, field_columns) in cases {
            let rewritten = input.rewrite(&mut rewriter).unwrap();

            assert_eq!(rewritten, exp_expr);
            let mut exp_field_columns = field_columns
                .into_iter()
                .map(String::from)
                .collect::<BTreeSet<String>>();
            assert_eq!(rewriter.field_projections, &mut exp_field_columns);
        }
    }
}
