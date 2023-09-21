use datafusion::common::tree_node::{TreeNode, TreeNodeRewriter};
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::{lit, Expr};

use crate::ValueExpr;

/// Rewrites an expression on `_value` as a boolean true literal, pushing any
/// encountered expressions onto `value_exprs` so they can be moved onto column
/// projections.
pub(crate) fn rewrite_field_value_references(
    value_exprs: &mut Vec<ValueExpr>,
    expr: Expr,
) -> DataFusionResult<Expr> {
    let mut rewriter = FieldValueRewriter { value_exprs };
    expr.rewrite(&mut rewriter)
}

struct FieldValueRewriter<'a> {
    value_exprs: &'a mut Vec<ValueExpr>,
}

impl<'a> TreeNodeRewriter for FieldValueRewriter<'a> {
    type N = Expr;

    fn mutate(&mut self, expr: Expr) -> DataFusionResult<Expr> {
        // try and convert Expr into a ValueExpr
        match expr.try_into() {
            // found a value expr. Save and replace with true
            Ok(value_expr) => {
                self.value_exprs.push(value_expr);
                Ok(lit(true))
            }
            // not a ValueExpr, so leave the same
            Err(expr) => Ok(expr),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_predicate::VALUE_COLUMN_NAME;

    use datafusion::prelude::col;

    #[test]
    fn test_field_value_rewriter() {
        let mut rewriter = FieldValueRewriter {
            value_exprs: &mut vec![],
        };

        let cases = vec![
            (col("f1").eq(lit(1.82)), col("f1").eq(lit(1.82)), vec![]),
            (col("t2"), col("t2"), vec![]),
            (
                col(VALUE_COLUMN_NAME).eq(lit(1.82)),
                // _value = 1.82 -> true
                lit(true),
                vec![ValueExpr {
                    expr: col(VALUE_COLUMN_NAME).eq(lit(1.82)),
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

        let input = col(VALUE_COLUMN_NAME).gt(lit(1.88));
        let rewritten = input.clone().rewrite(&mut rewriter).unwrap();
        assert_eq!(rewritten, lit(true));
        assert_eq!(rewriter.value_exprs, &mut vec![ValueExpr { expr: input }]);
    }
}
