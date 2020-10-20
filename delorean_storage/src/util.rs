//! This module contains DataFusion utility functions and helpers
use delorean_arrow::datafusion::logical_plan::Expr;

/// Encode the traversal of an expression tree. When passed to
/// `visit_expression`, `ExpressionVisitor::visit` is invoked
/// recursively on all nodes of an expression tree
///
/// TODO contribute this back upstream to datafusion??
pub trait ExpressionVisitor {
    fn visit(&mut self, expr: &Expr);
}

pub fn visit_expression<V: ExpressionVisitor>(expr: &Expr, visitor: &mut V) {
    visitor.visit(expr);

    // recurse
    match expr {
        // expression types without inputs
        Expr::Alias(..)
        | Expr::Column(..)
        | Expr::ScalarVariable(..)
        | Expr::Literal(..)
        | Expr::Wildcard => {
            // No inputs, so no more recursion needed
        }
        Expr::BinaryExpr { left, right, .. } => {
            visit_expression(left, visitor);
            visit_expression(right, visitor);
        }
        Expr::Cast { expr, .. } => visit_expression(expr, visitor),
        Expr::Not(expr) => visit_expression(expr, visitor),
        Expr::IsNull(expr) => visit_expression(expr, visitor),
        Expr::IsNotNull(expr) => visit_expression(expr, visitor),
        Expr::ScalarFunction { args, .. } => {
            for arg in args {
                visit_expression(arg, visitor)
            }
        }
        Expr::ScalarUDF { args, .. } => {
            for arg in args {
                visit_expression(arg, visitor)
            }
        }
        Expr::AggregateFunction { args, .. } => {
            for arg in args {
                visit_expression(arg, visitor)
            }
        }
        Expr::AggregateUDF { args, .. } => {
            for arg in args {
                visit_expression(arg, visitor)
            }
        }
        Expr::Nested(expr) => visit_expression(expr, visitor),
        Expr::Sort { expr, .. } => visit_expression(expr, visitor),
    }
}
