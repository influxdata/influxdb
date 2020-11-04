//! This module contains DataFusion utility functions and helpers
use delorean_arrow::datafusion::{
    logical_plan::Expr, logical_plan::LogicalPlan, logical_plan::Operator, optimizer::utils::inputs,
};
use std::io::Write;

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

    visitor.post_visit(expr);
}

// Return a string representation of this expression, without children
// (for use in indented pretty printing of the expression)
fn expr_str_without_children(expr: &Expr) -> String {
    match expr {
        Expr::Alias(_, name) => format!("Alias(<expr>, {})", name),
        Expr::Column(name) => format!("Column({})", name),
        Expr::ScalarVariable(names) => format!("ScalarVariable({:?})", names),
        Expr::Literal(value) => format!("Literal({})", value),
        Expr::BinaryExpr { op, .. } => format!("BinaryExpr(lhs=<expr>, op={:?}, rhs=<expr>)", op),
        Expr::Nested(_) => "Nested(<expr>)".to_string(),
        Expr::Not(_) => "Not(<expr>)".to_string(),
        Expr::IsNotNull(_) => "IsNotNull(<expr>)".to_string(),
        Expr::IsNull(_) => "IsNull(<expr>)".to_string(),
        Expr::Cast { data_type, .. } => format!("Cast(<expr>, data_type={:?})", data_type),
        Expr::Sort {
            asc, nulls_first, ..
        } => format!("Sort(<expr>, asc={}, nulls_first={})", asc, nulls_first),
        Expr::ScalarFunction { fun, .. } => format!("ScalarFunction(fun={:?}, args=<exprs>)", fun),
        Expr::ScalarUDF { fun, .. } => format!("ScalarUDF(fun={:?}, args=<exprs>)", fun),
        Expr::AggregateFunction { fun, distinct, .. } => format!(
            "ScalarUDF(fun={:?}, args=<exprs>, distinct={})",
            fun, distinct
        ),
        Expr::AggregateUDF { fun, .. } => format!("AggregateUDF(fun={:?}, args=<exprs>)", fun),
        Expr::Wildcard => "Wildcard".to_string(),
    }
}

/// Creates an indented representation of expr
pub fn dump_expr(expr: &Expr) -> String {
    struct ExprToString {
        indent: usize,
        output: String,
    }

    impl ExpressionVisitor for ExprToString {
        /// Invoked before children of expr are visisted
        fn pre_visit(&mut self, expr: &Expr) {
            for _ in 0..self.indent {
                self.output.push(' ')
            }
            self.output.push_str(&expr_str_without_children(expr));
            self.output.push('\n');
            self.indent += 1;
        }

        /// Invoked after children of expr are visited
        fn post_visit(&mut self, _expr: &Expr) {
            self.indent -= 1;
        }
    }

    let mut visitor = ExprToString {
        indent: 0,
        output: String::new(),
    };
    visit_expression(expr, &mut visitor);
    let ExprToString { indent: _, output } = visitor;
    output
}

/// dumps the plan, and schema information to a string
pub fn dump_plan(p: &LogicalPlan) -> String {
    let mut buf = Vec::new();
    dump_plan_impl("", p, &mut buf);
    String::from_utf8_lossy(&buf).to_string()
}

fn dump_plan_impl(prefix: &str, p: &LogicalPlan, buf: &mut impl Write) {
    writeln!(buf, "output schema: {:?}", p.schema()).unwrap();
    writeln!(buf, "{:?}", p).unwrap();

    let new_prefix = format!("{}    ", prefix);
    for i in inputs(p) {
        writeln!(buf).unwrap();
        dump_plan_impl(&new_prefix, i, buf);
    }
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
