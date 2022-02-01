use datafusion::{
    error::Result,
    logical_plan::{binary_expr, Expr, ExprRewriter, Operator},
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

pub fn rewrite(expr: Expr) -> Result<Expr> {
    expr.rewrite(&mut IOxExprRewriter::new())
}

/// see docs on [rewrite]
struct IOxExprRewriter {}

impl IOxExprRewriter {
    fn new() -> Self {
        Self {}
    }
}

/// if we can rewrite this case statement
fn is_case(expr: &Expr) -> bool {
    // don't support the `CASE <expr> WHEN <..> ELSE <..> END` syntax yet
    matches!(expr, Expr::Case { expr: None, .. })
}

/// Returns true if this binary operator returns a boolean value
fn is_comparison(op: Operator) -> bool {
    // explicitly list all of these operators so when new ones are
    // added to the enum we will have to update this `match`
    match op {
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
        Operator::Like => true,
        Operator::NotLike => true,
        Operator::IsDistinctFrom => true,
        Operator::IsNotDistinctFrom => true,
        Operator::RegexMatch => true,
        Operator::RegexIMatch => true,
        Operator::RegexNotMatch => true,
        Operator::RegexNotIMatch => true,
    }
}

impl ExprRewriter for IOxExprRewriter {
    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        match expr {
            Expr::BinaryExpr { left, op, right } if is_case(&left) && is_comparison(op) => {
                Ok(inline_case(true, *left, *right, op))
            }
            Expr::BinaryExpr { left, op, right } if is_case(&right) && is_comparison(op) => {
                Ok(inline_case(false, *left, *right, op))
            }
            expr => Ok(expr),
        }
    }
}

fn inline_case(case_on_left: bool, left: Expr, right: Expr, op: Operator) -> Expr {
    let (when_then_expr, else_expr, other) = match (case_on_left, left, right) {
        (
            true,
            Expr::Case {
                expr: None,
                when_then_expr,
                else_expr,
            },
            right,
        ) => (when_then_expr, else_expr, right),
        (
            false,
            left,
            Expr::Case {
                expr: None,
                when_then_expr,
                else_expr,
            },
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

    Expr::Case {
        expr: None,
        when_then_expr,
        else_expr,
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;

    use super::*;
    use datafusion::logical_plan::{case, col, lit, when};

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
        assert_eq!(expected, rewrite(expr).unwrap());
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

        assert_eq!(expected, rewrite(expr).unwrap());
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

        assert_eq!(expected, rewrite(expr).unwrap());
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

        assert_eq!(expected, rewrite(expr).unwrap());
    }

    #[test]
    fn test_fold_case_ops() {
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
        run_case(Operator::Like, true, lit("foo"), lit("bar"));
        run_case(Operator::NotLike, true, lit("foo"), lit("bar"));
        run_case(Operator::IsDistinctFrom, true, lit("foo"), lit("bar"));
        run_case(Operator::IsNotDistinctFrom, true, lit("foo"), lit("bar"));
        run_case(Operator::RegexMatch, true, lit("foo"), lit("bar"));
        run_case(Operator::RegexIMatch, true, lit("foo"), lit("bar"));
        run_case(Operator::RegexNotMatch, true, lit("foo"), lit("bar"));
        run_case(Operator::RegexNotIMatch, true, lit("foo"), lit("bar"));
    }

    fn run_case(op: Operator, expect_rewrite: bool, lit1: Expr, lit2: Expr) {
        // CASE WHEN tag IS NULL then '' ELSE tag END = 'bar'
        let expr = Expr::BinaryExpr {
            left: Box::new(make_case(col("tag").is_null(), lit1.clone(), col("tag"))),
            op,
            right: Box::new(lit2.clone()),
        };

        // CASE WHEN tag IS NULL then '' = 'bar' ELSE tag = 'bar' END
        let expected = if expect_rewrite {
            make_case(
                col("tag").is_null(),
                Expr::BinaryExpr {
                    left: Box::new(lit1),
                    op,
                    right: Box::new(lit2.clone()),
                },
                Expr::BinaryExpr {
                    left: Box::new(col("tag")),
                    op,
                    right: Box::new(lit2),
                },
            )
        } else {
            expr.clone()
        };

        assert_eq!(expected, rewrite(expr).unwrap());
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

        assert_eq!(expected, rewrite(expr).unwrap());
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
        assert_eq!(expected, rewrite(expr).unwrap());
    }

    fn make_case(when_expr: Expr, then_expr: Expr, otherwise_expr: Expr) -> Expr {
        when(when_expr, then_expr)
            .otherwise(otherwise_expr)
            .unwrap()
    }
}
