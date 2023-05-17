use crate::expression::{Binary, Call, ConditionalBinary, ConditionalExpression, Expr};

/// Expression distinguishes InfluxQL [`ConditionalExpression`] or [`Expr`]
/// nodes when visiting a [`ConditionalExpression`] tree. See [`walk_expression`].
#[derive(Debug)]
pub enum Expression<'a> {
    /// Specifies a conditional expression.
    Conditional(&'a ConditionalExpression),
    /// Specifies an arithmetic expression.
    Arithmetic(&'a Expr),
}

/// ExpressionMut is the same as [`Expression`] with the exception that
/// it provides mutable access to the nodes of the tree.
#[derive(Debug)]
pub enum ExpressionMut<'a> {
    /// Specifies a conditional expression.
    Conditional(&'a mut ConditionalExpression),
    /// Specifies an arithmetic expression.
    Arithmetic(&'a mut Expr),
}

/// Perform a depth-first traversal of an expression tree.
pub fn walk_expression<'a, B>(
    node: &'a ConditionalExpression,
    visit: &mut impl FnMut(Expression<'a>) -> std::ops::ControlFlow<B>,
) -> std::ops::ControlFlow<B> {
    match node {
        ConditionalExpression::Expr(n) => walk_expr(n, &mut |n| visit(Expression::Arithmetic(n)))?,
        ConditionalExpression::Binary(ConditionalBinary { lhs, rhs, .. }) => {
            walk_expression(lhs, visit)?;
            walk_expression(rhs, visit)?;
        }
        ConditionalExpression::Grouped(n) => walk_expression(n, visit)?,
    }

    visit(Expression::Conditional(node))
}

/// Perform a depth-first traversal of a mutable arithmetic or conditional expression tree.
pub fn walk_expression_mut<B>(
    node: &mut ConditionalExpression,
    visit: &mut impl FnMut(ExpressionMut<'_>) -> std::ops::ControlFlow<B>,
) -> std::ops::ControlFlow<B> {
    match node {
        ConditionalExpression::Expr(n) => {
            walk_expr_mut(n, &mut |n| visit(ExpressionMut::Arithmetic(n)))?
        }
        ConditionalExpression::Binary(ConditionalBinary { lhs, rhs, .. }) => {
            walk_expression_mut(lhs, visit)?;
            walk_expression_mut(rhs, visit)?;
        }
        ConditionalExpression::Grouped(n) => walk_expression_mut(n, visit)?,
    }

    visit(ExpressionMut::Conditional(node))
}

/// Perform a depth-first traversal of the arithmetic expression tree.
pub fn walk_expr<'a, B>(
    expr: &'a Expr,
    visit: &mut impl FnMut(&'a Expr) -> std::ops::ControlFlow<B>,
) -> std::ops::ControlFlow<B> {
    match expr {
        Expr::Binary(Binary { lhs, rhs, .. }) => {
            walk_expr(lhs, visit)?;
            walk_expr(rhs, visit)?;
        }
        Expr::Nested(n) => walk_expr(n, visit)?,
        Expr::Call(Call { args, .. }) => {
            args.iter().try_for_each(|n| walk_expr(n, visit))?;
        }
        Expr::VarRef { .. }
        | Expr::BindParameter(_)
        | Expr::Literal(_)
        | Expr::Wildcard(_)
        | Expr::Distinct(_) => {}
    }

    visit(expr)
}

/// Perform a depth-first traversal of a mutable arithmetic expression tree.
pub fn walk_expr_mut<B>(
    expr: &mut Expr,
    visit: &mut impl FnMut(&mut Expr) -> std::ops::ControlFlow<B>,
) -> std::ops::ControlFlow<B> {
    match expr {
        Expr::Binary(Binary { lhs, rhs, .. }) => {
            walk_expr_mut(lhs, visit)?;
            walk_expr_mut(rhs, visit)?;
        }
        Expr::Nested(n) => walk_expr_mut(n, visit)?,
        Expr::Call(Call { args, .. }) => {
            args.iter_mut().try_for_each(|n| walk_expr_mut(n, visit))?;
        }
        Expr::VarRef { .. }
        | Expr::BindParameter(_)
        | Expr::Literal(_)
        | Expr::Wildcard(_)
        | Expr::Distinct(_) => {}
    }

    visit(expr)
}

#[cfg(test)]
mod test {
    use crate::expression::walk::{walk_expr_mut, walk_expression_mut, ExpressionMut};
    use crate::expression::{
        arithmetic_expression, conditional_expression, ConditionalBinary, ConditionalExpression,
        ConditionalOperator, Expr, VarRef,
    };
    use crate::literal::Literal;

    #[test]
    fn test_walk_expression() {
        fn walk_expression(s: &str) -> String {
            let (_, ref expr) = conditional_expression(s).unwrap();
            let mut calls = Vec::new();
            let mut call_no = 0;
            super::walk_expression::<()>(expr, &mut |n| {
                calls.push(format!("{call_no}: {n:?}"));
                call_no += 1;
                std::ops::ControlFlow::Continue(())
            });
            calls.join("\n")
        }

        insta::assert_display_snapshot!(walk_expression("5 + 6 = 2 + 9"));
        insta::assert_display_snapshot!(walk_expression("time > now() + 1h"));
    }

    #[test]
    fn test_walk_expression_mut_modify() {
        let (_, ref mut expr) = conditional_expression("foo + bar + 5 =~ /str/").unwrap();
        walk_expression_mut::<()>(expr, &mut |e| {
            match e {
                ExpressionMut::Arithmetic(n) => match n {
                    Expr::VarRef(VarRef { name, .. }) => *name = format!("c_{name}").into(),
                    Expr::Literal(Literal::Integer(v)) => *v *= 10,
                    Expr::Literal(Literal::Regex(v)) => *v = format!("c_{}", v.0).into(),
                    _ => {}
                },
                ExpressionMut::Conditional(n) => {
                    if let ConditionalExpression::Binary(ConditionalBinary { op, .. }) = n {
                        *op = ConditionalOperator::NotEqRegex
                    }
                }
            }
            std::ops::ControlFlow::Continue(())
        });
        assert_eq!(expr.to_string(), "c_foo + c_bar + 50 !~ /c_str/")
    }

    #[test]
    fn test_walk_expr() {
        fn walk_expr(s: &str) -> String {
            let (_, expr) = arithmetic_expression(s).unwrap();
            let mut calls = Vec::new();
            let mut call_no = 0;
            super::walk_expr::<()>(&expr, &mut |n| {
                calls.push(format!("{call_no}: {n:?}"));
                call_no += 1;
                std::ops::ControlFlow::Continue(())
            });
            calls.join("\n")
        }

        insta::assert_display_snapshot!(walk_expr("5 + 6"));
        insta::assert_display_snapshot!(walk_expr("now() + 1h"));
    }

    #[test]
    fn test_walk_expr_mut() {
        fn walk_expr_mut(s: &str) -> String {
            let (_, mut expr) = arithmetic_expression(s).unwrap();
            let mut calls = Vec::new();
            let mut call_no = 0;
            super::walk_expr_mut::<()>(&mut expr, &mut |n| {
                calls.push(format!("{call_no}: {n:?}"));
                call_no += 1;
                std::ops::ControlFlow::Continue(())
            });
            calls.join("\n")
        }

        insta::assert_display_snapshot!(walk_expr_mut("5 + 6"));
        insta::assert_display_snapshot!(walk_expr_mut("now() + 1h"));
    }

    #[test]
    fn test_walk_expr_mut_modify() {
        let (_, mut expr) = arithmetic_expression("foo + bar + 5").unwrap();
        walk_expr_mut::<()>(&mut expr, &mut |e| {
            match e {
                Expr::VarRef(VarRef { name, .. }) => *name = format!("c_{name}").into(),
                Expr::Literal(Literal::Integer(v)) => *v *= 10,
                _ => {}
            }
            std::ops::ControlFlow::Continue(())
        });
        assert_eq!(expr.to_string(), "c_foo + c_bar + 50")
    }
}
