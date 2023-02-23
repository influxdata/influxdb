use crate::Predicate;

use super::FIELD_COLUMN_NAME;
use arrow::array::{as_boolean_array, as_string_array, ArrayRef, StringArray};
use arrow::compute::kernels;
use arrow::record_batch::RecordBatch;
use datafusion::common::DFSchema;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion};
use datafusion::optimizer::utils::split_conjunction_owned;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_plan::ColumnarValue;
use datafusion::prelude::{lit, Expr};
use schema::Schema;
use std::sync::Arc;

/// Logic for rewriting expressions from influxrpc that reference
/// `_field` to projections (aka column selection)
///
/// Rewrites a predicate on `_field` as a projection against a
/// specific schema to a literal true in the expression and remembers
/// which fields were selected.
///
/// For example, if the query has a predicate with the expression
/// `_field = "load4"`, FieldProjectionRewriter rewrites the predicate
/// by replacing `_field = "load4" to `true`, and adds the column "load4"
/// to the [`Predicate`]'s projection.
///
/// This rewrite also handle more complicated expressions such as
/// the expression `_field = "load4" OR _field = "load5" OR ` is
/// replaced by `true`, and the columns ("load4", "load5") are
/// added to the predicate's projection.
///
/// This rewrite can not handle non-conjuction predicates that refer
/// to both `_field` and some other column, such as `_field = "f1" OR
/// tag1 = "host.example.com"). Such predicates would need to be
/// handled at runtime as they depend on the data in the other
/// columns, which is not available at planning time.
#[derive(Debug)]
pub(crate) struct FieldProjectionRewriter {
    /// single column expressions (only refer to `_field`). If there
    /// are any such expressions, ALL of them must evaluate to true
    /// against a field's name in order to include that field in the
    /// output
    field_predicates: Vec<Expr>,
    /// The input schema (from where we know the field)
    schema: Schema,
}

impl FieldProjectionRewriter {
    /// Create a new [`FieldProjectionRewriter`] targeting the given schema
    pub(crate) fn new(schema: Schema) -> Self {
        Self {
            field_predicates: vec![],
            schema,
        }
    }

    /// Rewrites the predicate. See the description on
    /// [`FieldProjectionRewriter`] for more details.
    pub(crate) fn rewrite_field_exprs(&mut self, expr: Expr) -> DataFusionResult<Expr> {
        // for predicates like `A AND B AND C`
        // rewrite `A`, `B` and `C` separately and put them back together
        let rewritten_expr = split_conjunction_owned(expr)
            .into_iter()
            // apply the rewrite individually
            .map(|expr| self.rewrite_single_conjunct(expr))
            // check for errors
            .collect::<DataFusionResult<Vec<Expr>>>()?
            // put the Exprs back together with AND
            .into_iter()
            .reduce(|acc, expr| acc.and(expr))
            .expect("at least one expr");

        Ok(rewritten_expr)
    }

    // Rewrites a single predicate. Does not handle AND specially
    fn rewrite_single_conjunct(&mut self, expr: Expr) -> DataFusionResult<Expr> {
        let finder = expr.accept(ColumnReferencesFinder::default())?;

        // rewrite any expression that only references _field to `true`
        match (finder.saw_field_reference, finder.saw_non_field_reference) {
            // Only saw _field column references, rewrite
            (true, false) => {
                self.field_predicates.push(expr);
                Ok(lit(true))
            }
            // saw both _field and other column references, can't handle this case yet
            // https://github.com/influxdata/influxdb_iox/issues/5310
            (true, true) => Err(DataFusionError::Plan(format!(
                "Unsupported _field predicate: {expr}"
            ))),
            // Didn't see any references, or only non _field references, nothing to do
            (false, _) => Ok(expr),
        }
    }

    /// Converts all field_predicates we have seen into a field column
    /// restriction on the predicate by evaluating the expressions at plan time
    ///
    /// Uses arrow evaluation kernels to support arbitrary predicates,
    /// including regex etc
    pub(crate) fn add_to_predicate(self, predicate: Predicate) -> DataFusionResult<Predicate> {
        // Common case is that there are no _field predicates, in
        // which case we are done
        if self.field_predicates.is_empty() {
            return Ok(predicate);
        }

        // Form an array of strings from the field *names*:
        //
        // ┌─────────┐
        // │ _field  │
        // │  ----   │
        // │  "f1"   │
        // │  "f2"   │
        // │  "f3"   │
        // └─────────┘
        let field_names: ArrayRef = Arc::new(
            self.schema
                .fields_iter()
                .map(|f| f.name())
                .map(Some)
                .collect::<StringArray>(),
        );

        let batch = RecordBatch::try_from_iter(vec![(FIELD_COLUMN_NAME, Arc::clone(&field_names))])
            .expect("Error creating _field record batch");

        // Ceremony to prepare to evaluate the predicates
        let input_schema = batch.schema();
        let input_df_schema: DFSchema = input_schema.as_ref().clone().try_into().unwrap();
        let props = ExecutionProps::default();
        let exprs = self
            .field_predicates
            .into_iter()
            .map(|expr| create_physical_expr(&expr, &input_df_schema, &input_schema, &props))
            .collect::<DataFusionResult<Vec<_>>>()
            .map_err(|e| DataFusionError::Internal(format!("Unsupported _field predicate: {e}")))?;

        // evaluate into a boolean array where each element is true if
        // the field name evaluated to true for all predicates, and
        // false otherwise
        let matching = exprs
            .into_iter()
            // evaluate each field_predicate against the actual field
            // names. For example, if we have two predicates like
            //
            // _field !~= 'f2'
            // _field != 'f3'
            //
            // We will produce two output arrays:
            // ┌─────────┐  ┌─────────┐
            // │  true   │  │  true   │
            // │  false  │  │  true   │
            // │  true   │  │  false  │
            // └─────────┘  └─────────┘
            .map(|expr| match expr.evaluate(&batch) {
                Ok(ColumnarValue::Array(arr)) => arr,
                Ok(ColumnarValue::Scalar(s)) => {
                    panic!("Unexpected result evaluating {expr:?} against {batch:?}: {s:?}")
                }
                Err(e) => panic!("Unexpected err evaluating {expr:?} against {batch:?}: {e}"),
            })
            // Now combine the arrays using AND to get a single output
            // boolean array. For the example above, we would get
            // ┌─────────┐
            // │  true   │
            // │  false  │
            // │  false  │
            // └─────────┘
            .reduce(|acc, arr| {
                // apply boolean AND
                let bool_array =
                    kernels::boolean::and(as_boolean_array(&acc), as_boolean_array(&arr))
                        .expect("Error computing AND");
                Arc::new(bool_array) as ArrayRef
            })
            .unwrap();

        assert_eq!(matching.len(), field_names.len());

        // now find all field names with a 'true' entry in the
        // corresponding row. From our example above:
        // ┌──────┐
        // │_field│
        // │ ---- ├─────────┐
        // │ "f1" │  true ◀─┼─────f1 matches
        // │ "f2" │  false  │
        // │ "f3" │  false  │
        // └──────┴─────────┘
        let new_fields = as_boolean_array(&matching)
            .iter()
            .zip(as_string_array(&field_names).iter())
            .filter_map(|(matching, field_name)| {
                if matching == Some(true) {
                    // this array was constructed with no nulls, so
                    // the field_name should not be null
                    Some(field_name.unwrap())
                } else {
                    None
                }
            });

        Ok(predicate.with_field_columns(new_fields).unwrap())
    }
}

// Analyzes an expressions column references and finds:
// * Column references to `_field`
// * Column references to other columns
#[derive(Debug, Default)]
struct ColumnReferencesFinder {
    saw_field_reference: bool,
    saw_non_field_reference: bool,
}

impl ExpressionVisitor for ColumnReferencesFinder {
    fn pre_visit(mut self, expr: &Expr) -> DataFusionResult<Recursion<Self>> {
        if let Expr::Column(col) = expr {
            if col.name == FIELD_COLUMN_NAME {
                self.saw_field_reference = true;
            } else {
                self.saw_non_field_reference = true;
            }
        }

        // terminate early if we have already found both
        if self.saw_field_reference && self.saw_non_field_reference {
            Ok(Recursion::Stop(self))
        } else {
            Ok(Recursion::Continue(self))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use datafusion::prelude::{case, col};
    use schema::builder::SchemaBuilder;
    use test_helpers::assert_contains;

    #[test]
    fn test_field_column_rewriter() {
        let schema = make_schema();
        let cases = vec![
            (
                // f1 = 1.82
                col("f1").eq(lit(1.82)),
                col("f1").eq(lit(1.82)),
                None,
            ),
            (
                // _field = "f1"
                field_ref().eq(lit("f1")),
                lit(true),
                Some(vec!["f1"]),
            ),
            (
                // _field = "not_a_field"
                // should not match any rows
                field_ref().eq(lit("not_a_field")),
                lit(true),
                Some(vec![]),
            ),
            (
                // _field != "f1"
                field_ref().not_eq(lit("f1")),
                lit(true),
                Some(vec!["f2", "f3", "f4"]),
            ),
            (
                // reverse operand order
                // f1 = _field
                lit("f1").eq(field_ref()),
                lit(true),
                Some(vec!["f1"]),
            ),
            (
                // reverse operand order
                // f1 != _field
                lit("f1").not_eq(field_ref()),
                lit(true),
                Some(vec!["f2", "f3", "f4"]),
            ),
            (
                // mismatched != and =
                // (_field != f1) AND (_field = f3)
                field_ref().not_eq(lit("f1")).and(field_ref().eq(lit("f3"))),
                lit(true).and(lit(true)),
                Some(vec!["f3"]),
            ),
            (
                // mismatched = and !=
                // (_field = f1) OR (_field != f3)
                field_ref().eq(lit("f1")).or(field_ref().not_eq(lit("f3"))),
                lit(true),
                Some(vec!["f1", "f2", "f4"]),
            ),
            (
                // (_field = f1) OR (_field = f2)
                field_ref().eq(lit("f1")).or(field_ref().eq(lit("f2"))),
                lit(true),
                Some(vec!["f1", "f2"]),
            ),
            (
                // mix of _field and non _field, connected by AND
                // (_field = f2) AND (f2 = 5)
                field_ref().eq(lit("f2")).and(col("f2").eq(lit(5.0))),
                lit(true).and(col("f2").eq(lit(5.0))),
                Some(vec!["f2"]),
            ),
            (
                // mix of multiple _field and non _field, but connected by ANDs
                // (f1 = 5) AND (_field = f1) AND (f2 = 6)
                col("f1")
                    .eq(lit(5.0))
                    .and(field_ref().eq(lit("f1")))
                    .and(col("f2").eq(lit(6.0))),
                col("f1")
                    .eq(lit(5.0))
                    .and(lit(true))
                    .and(col("f2").eq(lit(6.0))),
                Some(vec!["f1"]),
            ),
            (
                // (f1 = 5) AND (((_field = f1) OR (_field = f3)) OR (_field = f2))
                col("f1").eq(lit(5.0)).and(
                    field_ref()
                        .eq(lit("f1"))
                        .or(field_ref().eq(lit("f3")))
                        .or(field_ref().eq(lit("f2"))),
                ),
                col("f1").eq(lit(5.0)).and(lit(true)),
                Some(vec!["f1", "f2", "f3"]),
            ),
            (
                // (_field != f1) AND (_field != f2)
                field_ref()
                    .not_eq(lit("f1"))
                    .and(field_ref().not_eq(lit("f2"))),
                lit(true).and(lit(true)),
                Some(vec!["f3", "f4"]),
            ),
            (
                // (f1 = 5) AND (((_field != f1) AND (_field != f3)) AND (_field != f2))
                col("f1").eq(lit(5.0)).and(
                    field_ref()
                        .not_eq(lit("f1"))
                        .and(field_ref().not_eq(lit("f3")))
                        .and(field_ref().not_eq(lit("f2"))),
                ),
                col("f1")
                    .eq(lit(5.0))
                    .and(lit(true))
                    .and(lit(true))
                    .and(lit(true)),
                Some(vec!["f4"]),
            ),
            (
                // _field IS NOT NULL
                field_ref().is_not_null(),
                lit(true),
                Some(vec!["f1", "f2", "f3", "f4"]),
            ),
            (
                // case _field
                //   WHEN "f1" THEN true
                //   WHEN  "f2"  THEN false
                // END
                case(field_ref())
                    .when(lit("f1"), lit(true))
                    .when(lit("f2"), lit(false))
                    .end()
                    .unwrap(),
                lit(true),
                Some(vec!["f1"]),
            ),
            (
                // _field = f1 AND _measurement = m1
                field_ref()
                    .eq(lit("f1"))
                    .and(col("_measurement").eq(lit("m1"))),
                lit(true).and(col("_measurement").eq(lit("m1"))),
                Some(vec!["f1"]),
            ),
            (
                // (_field =~ 'f1')
                regex_match(field_ref(), "f1"),
                lit(true),
                Some(vec!["f1"]),
            ),
            (
                // (_field =~ 'f1|f2')
                regex_match(field_ref(), "f1|f2"),
                lit(true),
                Some(vec!["f1", "f2"]),
            ),
            (
                // RegexNotMatch
                // (_field !=~ 'f1|f2')
                regex_not_match(field_ref(), "f1|f2"),
                lit(true),
                Some(vec!["f3", "f4"]),
            ),
            (
                // (_field =~ 'f1|f2') AND (_field !~= 'f2') AND (foo = 5.0)
                regex_match(field_ref(), "f1|f2")
                    .and(regex_not_match(field_ref(), "f2"))
                    .and(col("foo").eq(lit(5.0))),
                lit(true).and(lit(true)).and(col("foo").eq(lit(5.0))),
                Some(vec!["f1"]),
            ),
        ];

        for (input, exp_expr, exp_field_columns) in cases {
            println!(
                "Running test\ninput: {input:?}\nexpected_expr: {exp_expr:?}\nexpected_field_columns: {exp_field_columns:?}\n"
            );
            let mut rewriter = FieldProjectionRewriter::new(schema.clone());

            let rewritten = rewriter.rewrite_field_exprs(input).unwrap();
            assert_eq!(rewritten, exp_expr);

            let predicate = rewriter.add_to_predicate(Predicate::new()).unwrap();

            let actual_field_columns = predicate
                .field_columns
                .as_ref()
                .map(|field_columns| field_columns.iter().map(|s| s.as_str()).collect::<Vec<_>>());

            assert_eq!(actual_field_columns, exp_field_columns);
        }
    }

    #[test]
    fn test_field_column_rewriter_unsupported() {
        let schema = make_schema();
        let cases = vec![
            (
                // mix of field and non field, connected by OR
                // f1 = _field OR f1 = 5.0
                lit("f1").eq(field_ref()).or(col("f1").eq(lit(5.0))),
                "Unsupported _field predicate",
            ),
            (
                // more complicated
                // f1 = _field AND (_field = f2 OR f2 = 5.0)
                lit("f1")
                    .eq(field_ref())
                    .and(field_ref().eq(lit("f2")).or(col("f2").eq(lit(5.0)))),
                "Unsupported _field predicate",
            ),
        ];

        for (input, exp_error) in cases {
            println!("Running test\ninput: {input:?}\nexpected_error: {exp_error:?}\n");

            let run_case = || {
                let mut rewriter = FieldProjectionRewriter::new(schema.clone());
                // check for error in rewrite_field_exprs
                rewriter.rewrite_field_exprs(input)?;
                // check for error adding to predicate
                rewriter.add_to_predicate(Predicate::new())
            };

            let err = run_case().expect_err("Expected error rewriting, but was successful");
            assert_contains!(err.to_string(), exp_error);
        }
    }

    /// returns a reference to the special _field column
    fn field_ref() -> Expr {
        col(FIELD_COLUMN_NAME)
    }

    /// Returns a regex_match expression arg ~= pattern
    fn regex_match(arg: Expr, pattern: impl Into<String>) -> Expr {
        query_functions::regex_match_expr(arg, pattern.into())
    }

    /// Returns a regex_match expression arg !~= pattern
    fn regex_not_match(arg: Expr, pattern: impl Into<String>) -> Expr {
        query_functions::regex_not_match_expr(arg, pattern.into())
    }

    fn make_schema() -> Schema {
        SchemaBuilder::new()
            .tag("foo")
            .tag("bar")
            .field("f1", DataType::Float64)
            .unwrap()
            .field("f2", DataType::Float64)
            .unwrap()
            .field("f3", DataType::Float64)
            .unwrap()
            .field("f4", DataType::Float64)
            .unwrap()
            .build()
            .unwrap()
    }
}
