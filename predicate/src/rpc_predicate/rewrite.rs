use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    error::Result,
    logical_expr::{binary_expr, expr::Case, BinaryExpr, Cast, Like, Operator},
    prelude::Expr,
};

/// Special purpose `Expr` rewrite rules for IOx
///
/// DataFusion has many `Expr` rewrite / simplification rules that are
/// generally useful.  However, there are several that (currently)
/// only seem to make sense for IOx which are placed here:
///
/// 1. Fold past CASE blocks
///
/// # Fold past CASE blocks / translate to boolean CASE #3585
///
/// Inlines boolean BinaryExprs that have a CASE as an argument such
/// as the following (note the `= 'cpu'`):
///
/// ```sql
/// CASE
///   WHEN tag_col IS NULL THEN ''
///   ELSE tag_col
///   END = 'cpu'
/// ```
///
/// By "inlining" the = 'cpu' to each WHEN into:
///
/// ```sql
/// CASE
///  WHEN tag_col IS NULL THEN '' = 'cpu'
///  ELSE tag_col = 'cpu'
/// END
/// ```
pub fn iox_expr_rewrite(expr: Expr) -> Result<Expr> {
    expr.transform(&iox_expr_rewrite_inner)
}

fn iox_expr_rewrite_inner(expr: Expr) -> Result<Transformed<Expr>> {
    Ok(match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_case(&left) && is_comparison(op) => {
            Transformed::Yes(inline_case(true, *left, *right, op))
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if is_case(&right) && is_comparison(op) =>
        {
            Transformed::Yes(inline_case(false, *left, *right, op))
        }
        expr => Transformed::No(expr),
    })
}

/// Special purpose `Expr` rewrite rules for an Expr that is used as a predicate.
///
/// In general the rewrite rules in Datafusion and IOx attempt to
/// preserve the semantics of an expression, especially with respect to
/// nulls. This means that certain expressions can not be simplified
/// (as they may become null)
///
/// However, for `Expr`s used as filters, only rows for which the
/// `Expr` evaluates to 'true' are returned. Those rows for which the
/// `Expr` evaluates to `false` OR `null` are filtered out.
///
/// This function simplifies `Expr`s that are being used as
/// predicates.
///
/// Currently it is special cases, but it would be great to generalize
/// it and contribute it back to DataFusion
pub fn simplify_predicate(expr: Expr) -> Result<Expr> {
    expr.transform(&simplify_predicate_inner)
}

fn simplify_predicate_inner(expr: Expr) -> Result<Transformed<Expr>> {
    // look for this structure:
    //
    //  NOT(col IS NULL) AND col = 'foo'
    //
    // and replace it with
    //
    // col = 'foo'
    //
    // Proof:
    // Case 1: col is NULL
    //
    // not (NULL IS NULL) AND col = 'foo'
    // not (true) AND NULL = 'foo'
    // NULL
    //
    // Case 2: col is not NULL and not equal to 'foo'
    // not (false) AND false
    // true AND false
    // false
    //
    // Case 3: col is not NULL and equal to 'foo'
    // not (false) AND true
    // true AND true
    // true
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            if let (Some(coll), Some(colr)) = (is_col_not_null(&left), is_col_op_lit(&right)) {
                if colr == coll {
                    return Ok(Transformed::Yes(*right));
                }
            } else if let (Some(coll), Some(colr)) = (is_col_op_lit(&left), is_col_not_null(&right))
            {
                if colr == coll {
                    return Ok(Transformed::Yes(*left));
                }
            };

            Ok(Transformed::No(Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            })))
        }
        expr => Ok(Transformed::No(expr)),
    }
}

/// if we can rewrite this case statement
fn is_case(expr: &Expr) -> bool {
    // don't support the `CASE <expr> WHEN <..> ELSE <..> END` syntax yet
    matches!(expr, Expr::Case(Case { expr: None, .. }))
}

/// Returns true if this binary operator returns a boolean value
fn is_comparison(op: Operator) -> bool {
    // explicitly list all of these operators so when new ones are
    // added to the enum we will have to update this `match`
    match op {
        Operator::BitwiseAnd => false,
        Operator::BitwiseOr => false,
        Operator::BitwiseShiftLeft => false,
        Operator::BitwiseShiftRight => false,
        Operator::BitwiseXor => false,
        Operator::Eq => true,
        Operator::NotEq => true,
        Operator::Lt => true,
        Operator::LtEq => true,
        Operator::Gt => true,
        Operator::GtEq => true,
        Operator::Plus => false,
        Operator::Minus => false,
        Operator::Multiply => false,
        Operator::Divide => false,
        Operator::Modulo => false,
        Operator::And => true,
        Operator::Or => true,
        Operator::IsDistinctFrom => true,
        Operator::IsNotDistinctFrom => true,
        Operator::RegexMatch => true,
        Operator::RegexIMatch => true,
        Operator::RegexNotMatch => true,
        Operator::RegexNotIMatch => true,
        Operator::StringConcat => false,
        // array containment operators
        Operator::ArrowAt => true,
        Operator::AtArrow => true,
    }
}

fn inline_case(case_on_left: bool, left: Expr, right: Expr, op: Operator) -> Expr {
    let (when_then_expr, else_expr, other) = match (case_on_left, left, right) {
        (
            true,
            Expr::Case(Case {
                expr: None,
                when_then_expr,
                else_expr,
            }),
            right,
        ) => (when_then_expr, else_expr, right),
        (
            false,
            left,
            Expr::Case(Case {
                expr: None,
                when_then_expr,
                else_expr,
            }),
        ) => (when_then_expr, else_expr, left),
        _ => unreachable!(),
    };

    let when_then_expr = when_then_expr
        .into_iter()
        .map(|(when, then)| {
            let then = Box::new(if case_on_left {
                binary_expr(*then, op, other.clone())
            } else {
                binary_expr(other.clone(), op, *then)
            });
            (when, then)
        })
        .collect();

    let else_expr = else_expr.map(|else_expr| {
        Box::new(if case_on_left {
            binary_expr(*else_expr, op, other)
        } else {
            binary_expr(other, op, *else_expr)
        })
    });

    Expr::Case(Case {
        expr: None,
        when_then_expr,
        else_expr,
    })
}

/// returns the column name for a column expression
fn is_col(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(c) => Some(c.name.as_str()),
        Expr::Cast(Cast { expr, data_type: _ }) => is_col(expr),
        _ => None,
    }
}

/// returns the column name for an expression like `IS NULL(col)`
fn is_col_null(expr: &Expr) -> Option<&str> {
    if let Expr::IsNull(arg) = &expr {
        is_col(arg)
    } else {
        None
    }
}

/// returns the column name for an expression like `IS NOT NULL(col)` or `NOT(IS NULL(col))`
fn is_col_not_null(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::IsNotNull(arg) => is_col(arg),
        Expr::Not(arg) => is_col_null(arg),
        _ => None,
    }
}

fn is_lit(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(_))
}

/// returns the column name for an expression like `col = <lit>`
fn is_col_op_lit(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op: _, right }) if is_lit(right) => is_col(left),
        Expr::BinaryExpr(BinaryExpr { left, op: _, right }) if is_lit(left) => is_col(right),
        Expr::Like(Like { expr, pattern, .. }) if is_lit(pattern) => is_col(expr),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{Add, Not};

    use super::*;
    use arrow::datatypes::DataType;
    use datafusion::prelude::{case, cast, col, lit, when};

    #[test]
    fn test_fold_case_expr() {
        // no rewrites with base expression form
        let expr = case(col("tag"))
            .when(lit("foo"), lit("case1"))
            .when(lit("bar"), lit("case2"))
            .otherwise(lit("case3"))
            .unwrap()
            .eq(lit("case2"));

        let expected = expr.clone();
        assert_eq!(expected, iox_expr_rewrite(expr).unwrap());
    }

    #[test]
    fn test_fold_case_basic() {
        // CASE WHEN tag IS NULL then '' ELSE tag END = 'bar'
        let expr = make_case(col("tag").is_null(), lit(""), col("tag")).eq(lit("bar"));

        // CASE WHEN tag IS NULL then '' = 'bar' ELSE tag = 'bar' END
        let expected = make_case(
            col("tag").is_null(),
            lit("").eq(lit("bar")),
            col("tag").eq(lit("bar")),
        );

        assert_eq!(expected, iox_expr_rewrite(expr).unwrap());
    }

    #[test]
    fn test_fold_case_basic_reversed() {
        // test with "foo" = CASE...

        //  'bar' = CASE WHEN tag IS NULL then '' ELSE tag END
        let expr = lit("bar").eq(make_case(col("tag").is_null(), lit(""), col("tag")));

        // CASE WHEN tag IS NULL then '' = 'bar' ELSE tag = 'bar' END
        let expected = make_case(
            col("tag").is_null(),
            lit("bar").eq(lit("")),
            lit("bar").eq(col("tag")),
        );

        assert_eq!(expected, iox_expr_rewrite(expr).unwrap());
    }

    #[test]
    fn test_fold_case_both_sides() {
        //  CASE WHEN tag IS NULL then '' ELSE tag END =
        //  CASE WHEN other_tag IS NULL then '' ELSE other_tag END
        let expr = make_case(col("tag").is_null(), lit(""), col("tag")).eq(make_case(
            col("other_tag").is_null(),
            lit(""),
            col("other_tag"),
        ));

        let expected = make_case(
            col("tag").is_null(),
            lit("").eq(make_case(
                col("other_tag").is_null(),
                lit(""),
                col("other_tag"),
            )),
            col("tag").eq(make_case(
                col("other_tag").is_null(),
                lit(""),
                col("other_tag"),
            )),
        );

        assert_eq!(expected, iox_expr_rewrite(expr).unwrap());
    }

    #[test]
    fn test_fold_case_ops() {
        run_case(Operator::BitwiseAnd, false, lit(1), lit(2));
        run_case(Operator::Eq, true, lit("foo"), lit("bar"));
        run_case(Operator::NotEq, true, lit("foo"), lit("bar"));
        run_case(Operator::Lt, true, lit("foo"), lit("bar"));
        run_case(Operator::LtEq, true, lit("foo"), lit("bar"));
        run_case(Operator::Gt, true, lit("foo"), lit("bar"));
        run_case(Operator::GtEq, true, lit("foo"), lit("bar"));
        run_case(Operator::Plus, false, lit(1), lit(2));
        run_case(Operator::Minus, false, lit(1), lit(2));
        run_case(Operator::Multiply, false, lit(1), lit(2));
        run_case(Operator::Divide, false, lit(1), lit(2));
        run_case(Operator::Modulo, false, lit(1), lit(2));
        run_case(Operator::And, true, lit("foo"), lit("bar"));
        run_case(Operator::Or, true, lit("foo"), lit("bar"));
        run_case(Operator::IsDistinctFrom, true, lit("foo"), lit("bar"));
        run_case(Operator::IsNotDistinctFrom, true, lit("foo"), lit("bar"));
        run_case(Operator::RegexMatch, true, lit("foo"), lit("bar"));
        run_case(Operator::RegexIMatch, true, lit("foo"), lit("bar"));
        run_case(Operator::RegexNotMatch, true, lit("foo"), lit("bar"));
        run_case(Operator::RegexNotIMatch, true, lit("foo"), lit("bar"));
    }

    fn run_case(op: Operator, expect_rewrite: bool, lit1: Expr, lit2: Expr) {
        // CASE WHEN tag IS NULL then '' ELSE tag END = 'bar'
        let expr = binary_expr(
            make_case(col("tag").is_null(), lit1.clone(), col("tag")),
            op,
            lit2.clone(),
        );

        // CASE WHEN tag IS NULL then '' = 'bar' ELSE tag = 'bar' END
        let expected = if expect_rewrite {
            make_case(
                col("tag").is_null(),
                binary_expr(lit1, op, lit2.clone()),
                binary_expr(col("tag"), op, lit2),
            )
        } else {
            expr.clone()
        };

        assert_eq!(expected, iox_expr_rewrite(expr).unwrap());
    }

    #[test]
    // test with more than one when expr
    fn test_fold_case_multiple_when_expr() {
        // CASE
        //  WHEN tag IS NULL     THEN 'is null'
        //  WHEN tag IS NOT NULL THEN 'is not null'
        //  ELSE 'WTF?`
        // END = 'is null'
        let expr = when(col("tag").is_null(), lit("is null"))
            .when(col("tag").is_not_null(), lit("is not null"))
            .otherwise(lit("WTF?"))
            .unwrap()
            .eq(lit("is null"));

        // CASE
        //  WHEN tag IS NULL     THEN 'is null' = 'is null'
        //  WHEN tag IS NOT NULL THEN 'is not null' = 'is null'
        //  ELSE 'WTF?' = 'is null'
        // END
        let expected = when(col("tag").is_null(), lit("is null").eq(lit("is null")))
            .when(
                col("tag").is_not_null(),
                lit("is not null").eq(lit("is null")),
            )
            .otherwise(lit("WTF?").eq(lit("is null")))
            .unwrap();

        assert_eq!(expected, iox_expr_rewrite(expr).unwrap());
    }

    #[test]
    // negative  test with a non binary expr
    fn test_fold_case_non_binary() {
        // CASE
        //  WHEN tag IS NULL     THEN 1
        //  ELSE 2
        // END = 1
        let expr = when(col("tag").is_null(), lit(1))
            .otherwise(lit(2))
            .unwrap()
            .add(lit(1));

        let expected = expr.clone();
        assert_eq!(expected, iox_expr_rewrite(expr).unwrap());
    }

    fn make_case(when_expr: Expr, then_expr: Expr, otherwise_expr: Expr) -> Expr {
        when(when_expr, then_expr)
            .otherwise(otherwise_expr)
            .unwrap()
    }

    #[test]
    fn test_simplify_predicate() {
        let expr = col("foo").is_null().not().and(col("foo").eq(lit("bar")));
        let expected = col("foo").eq(lit("bar"));
        assert_eq!(expected, simplify_predicate(expr).unwrap());
    }

    #[test]
    fn test_simplify_predicate_reversed() {
        let expr = col("foo").eq(lit("bar")).and(col("foo").is_null().not());
        let expected = col("foo").eq(lit("bar"));
        assert_eq!(expected, simplify_predicate(expr).unwrap());
    }

    #[test]
    fn test_simplify_predicate_different_col() {
        // only works when col references are the same
        let expr = col("foo").is_null().not().and(col("foo2").eq(lit("bar")));
        let expected = expr.clone();
        assert_eq!(expected, simplify_predicate(expr).unwrap());
    }

    #[test]
    fn test_simplify_predicate_different_col_reversed() {
        // only works when col references are the same
        let expr = col("foo2").eq(lit("bar")).and(col("foo").is_null().not());
        let expected = expr.clone();
        assert_eq!(expected, simplify_predicate(expr).unwrap());
    }

    #[test]
    fn test_simplify_predicate_is_not_null() {
        let expr = col("foo").is_not_null().and(col("foo").eq(lit("bar")));
        let expected = col("foo").eq(lit("bar"));
        assert_eq!(expected, simplify_predicate(expr).unwrap());
    }

    #[test]
    fn test_simplify_predicate_complex() {
        // can't rewrite to some thing else fancy on the right
        let expr = col("foo").is_null().not().and(col("foo").eq(col("foo")));
        let expected = expr.clone();
        assert_eq!(expected, simplify_predicate(expr).unwrap());
    }

    #[test]
    fn test_simplify_predicate_cast_left() {
        let expr = cast(col("foo"), DataType::Utf8)
            .is_null()
            .not()
            .and(col("foo").eq(lit("bar")));
        let expected = col("foo").eq(lit("bar"));
        assert_eq!(expected, simplify_predicate(expr).unwrap());
    }

    #[test]
    fn test_simplify_predicate_cast_right() {
        let expr = col("foo")
            .is_null()
            .not()
            .and(cast(col("foo"), DataType::Utf8).eq(lit("bar")));
        let expected = cast(col("foo"), DataType::Utf8).eq(lit("bar"));
        assert_eq!(expected, simplify_predicate(expr).unwrap());
    }

    #[test]
    fn test_simplify_predicate_cast_both() {
        let expr = cast(col("foo"), DataType::Utf8)
            .is_null()
            .not()
            .and(cast(col("foo"), DataType::Utf8).eq(lit("bar")));
        let expected = cast(col("foo"), DataType::Utf8).eq(lit("bar"));
        assert_eq!(expected, simplify_predicate(expr).unwrap());
    }

    fn like(expr: Expr, pattern: Expr) -> Expr {
        let expr = Box::new(expr);
        let pattern = Box::new(pattern);
        Expr::Like(Like {
            negated: false,
            expr,
            pattern,
            escape_char: None,
            case_insensitive: false,
        })
    }

    #[test]
    fn test_simplify_predicate_like() {
        let expr = col("foo").is_null().not().and(like(col("foo"), lit("bar")));
        let expected = like(col("foo"), lit("bar"));
        assert_eq!(expected, simplify_predicate(expr).unwrap());
    }
}
