//! This module contains DataFusion utility functions and helpers
use arrow_deps::datafusion::{logical_plan::Expr, logical_plan::Operator};

/// Encode the traversal of an expression tree. When passed to
/// `visit_expression`, `ExpressionVisitor::visit` is invoked
/// recursively on all nodes of an expression tree
///
/// TODO contribute this back upstream to datafusion??
pub trait ExpressionVisitor {
    /// Invoked before children of expr are visisted
    fn pre_visit(&mut self, expr: &Expr);

    /// Invoked after children of expr are visited. Default
    /// implementation does nothing.
    fn post_visit(&mut self, _expr: &Expr) {}
}

pub fn visit_expression<V: ExpressionVisitor>(expr: &Expr, visitor: &mut V) {
    visitor.pre_visit(expr);

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
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
        } => {
            if let Some(expr) = expr.as_ref() {
                visit_expression(expr, visitor);
            }
            when_then_expr.iter().for_each(|(when, then)| {
                visit_expression(when, visitor);
                visit_expression(then, visitor);
            });
            if let Some(else_expr) = else_expr.as_ref() {
                visit_expression(else_expr, visitor);
            }
        }
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
        Expr::Sort { expr, .. } => visit_expression(expr, visitor),
    }

    visitor.post_visit(expr);
}

/// Creates a single expression representing the conjunction (aka
/// AND'ing) together of a set of expressions
#[derive(Debug, Default)]
pub struct AndExprBuilder {
    cur_expr: Option<Expr>,
}

impl AndExprBuilder {
    /// append `new_expr` to the expression chain being built
    pub fn append_opt_ref(self, new_expr: Option<&Expr>) -> Self {
        match new_expr {
            None => self,
            Some(new_expr) => self.append_expr(new_expr.clone()),
        }
    }

    /// append `new_expr` to the expression chain being built
    pub fn append_opt(self, new_expr: Option<Expr>) -> Self {
        match new_expr {
            None => self,
            Some(new_expr) => self.append_expr(new_expr),
        }
    }

    /// Append `new_expr` to the expression chain being built
    pub fn append_expr(self, new_expr: Expr) -> Self {
        let Self { cur_expr } = self;

        let cur_expr = if let Some(cur_expr) = cur_expr {
            Expr::BinaryExpr {
                left: Box::new(cur_expr),
                op: Operator::And,
                right: Box::new(new_expr),
            }
        } else {
            new_expr
        };

        let cur_expr = Some(cur_expr);

        Self { cur_expr }
    }

    /// Creates the new filter expression, consuming Self
    pub fn build(self) -> Option<Expr> {
        self.cur_expr
    }
}
