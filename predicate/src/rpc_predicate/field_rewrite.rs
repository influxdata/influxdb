//! Logic for rewriting _field = "val" expressions to projections

use super::FIELD_COLUMN_NAME;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_plan::{
    lit, Expr, ExprRewritable, ExprRewriter, ExprVisitable, ExpressionVisitor, Operator, Recursion,
};
use datafusion::scalar::ScalarValue;
use std::collections::BTreeSet;

/// What fields / columns should appear in the result
#[derive(Debug, PartialEq)]
pub(crate) enum FieldProjection {
    /// No restriction on fields/columns
    None,
    /// Only fields / columns that appear should be produced
    Include(BTreeSet<String>),
    /// Only fields / columns that are *NOT* in this list should appear
    Exclude(BTreeSet<String>),
}

impl FieldProjection {
    // Add `name` as a included field name
    fn add_include(&mut self, name: impl Into<String>) {
        match self {
            Self::None => {
                *self = Self::Include([name.into()].into_iter().collect());
            }
            Self::Include(cols) => {
                cols.insert(name.into());
            }
            Self::Exclude(_) => {
                unreachable!("previously had '!=' and now have '=' not caught")
            }
        }
    }

    // Add `name` as a excluded field name
    fn add_exclude(&mut self, name: impl Into<String>) {
        match self {
            Self::None => {
                *self = Self::Exclude([name.into()].into_iter().collect());
            }
            Self::Include(_) => {
                unreachable!("previously had '=' and now have '!=' not caught")
            }
            Self::Exclude(cols) => {
                cols.insert(name.into());
            }
        }
    }
}

/// Rewrites a predicate on `_field` as a projection on a specific defined by
/// the literal in the expression.
///
/// For example, the expression `_field = "load4"` is removed from the
/// normalised expression, and a column "load4" added to the predicate's
/// projection.
#[derive(Debug)]
pub(crate) struct FieldProjectionRewriter {
    /// Remember what we have seen for expressions across calls to
    /// ensure that multiple exprs don't end up having incompatible
    /// options
    checker: FieldRefChecker,
    projection: FieldProjection,
}

impl FieldProjectionRewriter {
    pub(crate) fn new() -> Self {
        Self {
            checker: FieldRefChecker::new(),
            projection: FieldProjection::None,
        }
    }

    // Rewrites the predicate, looking for _field predicates
    pub(crate) fn rewrite_field_exprs(&mut self, expr: Expr) -> DataFusionResult<Expr> {
        // Check for incompatible structures / patterns
        self.checker = expr.accept(self.checker.clone())?;

        // Actually rewrite the expression
        let expr = expr.rewrite(&mut FieldProjectionRewriterWrapper(self))?;

        // Ensure that there are no remaining _field references
        expr.accept(FieldRefFinder {})?;

        Ok(expr)
    }

    /// Returns any projection found in the predicate
    pub(crate) fn into_projection(self) -> FieldProjection {
        self.projection
    }
}

// newtype so users aren't confused and use `FieldProjectionRewriter` directly
struct FieldProjectionRewriterWrapper<'a>(&'a mut FieldProjectionRewriter);

impl<'a> ExprRewriter for FieldProjectionRewriterWrapper<'a> {
    /// This looks for very specific patterns
    /// (_field = "foo") OR (_field = "bar") OR ...
    /// (_field != "foo") AND (_field != "bar") AND ...
    fn mutate(&mut self, expr: Expr) -> DataFusionResult<Expr> {
        match check_binary(&expr) {
            Some((Operator::Eq, name)) => {
                self.0.projection.add_include(name);
                Ok(lit(true))
            }
            Some((Operator::NotEq, name)) => {
                self.0.projection.add_exclude(name);
                Ok(lit(true))
            }
            _ => Ok(expr),
        }
    }
}

/// Returns Some(op, str) if this expr represents `FILD_NAME_COLUMNN = <string lit>`
fn check_binary(expr: &Expr) -> Option<(Operator, &str)> {
    if let Expr::BinaryExpr { left, op, right } = expr {
        match (as_col_name(left), Some(*op), as_str_lit(right)) {
            // col eq lit
            (Some(FIELD_COLUMN_NAME), Some(Operator::Eq), Some(name))
            | (Some(FIELD_COLUMN_NAME), Some(Operator::NotEq), Some(name)) => Some((*op, name)),
            _ => None,
        }
    } else {
        None
    }
}

/// if `expr` is a column name, returns the name
fn as_col_name(expr: &Expr) -> Option<&str> {
    if let Expr::Column(inner) = expr {
        Some(inner.name.as_ref())
    } else {
        None
    }
}

/// if the expr is a non null literal string, returns the string value
fn as_str_lit(expr: &Expr) -> Option<&str> {
    if let Expr::Literal(ScalarValue::Utf8(Some(name))) = &expr {
        Some(name.as_str())
    } else {
        None
    }
}

/// Searches for `_field` and errors if any are found
///
/// Note that this catches for different error cases than
/// [`FieldRefChecker`]. Specifically, it will error for predicates
/// such as `_field ~ 'foo'`, that have a `_field` reference but were
/// not rewritten or previously idetified as illegal.
struct FieldRefFinder {}

impl ExpressionVisitor for FieldRefFinder {
    fn pre_visit(self, expr: &Expr) -> DataFusionResult<Recursion<Self>> {
        match as_col_name(expr) {
            Some(FIELD_COLUMN_NAME) => Err(DataFusionError::Plan(
                "Unsupported _field predicate, could not rewrite for IOx".into(),
            )),
            _ => Ok(Recursion::Continue(self)),
        }
    }
}

/// What type of structures have been seen in children
#[derive(Debug, Clone, Copy)]
enum FieldRefState {
    /// Have not yet seen a _field predicate
    NoPred,
    /// Seen a _field = &str predicate
    Eq,
    /// Seen a _field = &str predicate connected by ORs
    EqOr,
    /// Seen a _field != &str predicate
    NotEq,
    /// Seen a _field != &str predicate connected by ANDs
    NotEqAnd,
}

/// Tracks the state of an operator stack
/// (foo = bar) AND ((_field = 's') OR (_field != 'r'))
#[derive(Debug, Clone)]
struct OpState {
    /// the operator in the stack
    operator: Operator,

    /// Is this expr a FieldRef?
    is_field_ref: bool,

    /// the state of all children (filled in on return)
    child_states: Vec<FieldRefState>,
}

fn unsupported_op_error() -> DataFusionError {
    DataFusionError::Plan("Unsupported structure with _field reference".to_string())
}

impl OpState {
    /// Validates the state of the operator children Returns the state of this operator
    fn validate(self) -> DataFusionResult<FieldRefState> {
        use FieldRefState::*;

        let Self {
            operator,
            is_field_ref,
            child_states,
        } = self;

        match (operator, is_field_ref) {
            (Operator::Eq, true) => Ok(Eq),
            (Operator::NotEq, true) => Ok(NotEq),
            (Operator::And, _) => {
                child_states
                    .into_iter()
                    .try_fold(NoPred, |state, child_state| match (state, child_state) {
                        // AND didn't connect children of Pred and NoPreds so it is ok
                        (NoPred, other) | (other, NoPred) => Ok(other),
                        (NotEq, NotEq) => Ok(NotEq),
                        (NotEq, NotEqAnd) | (NotEqAnd, NotEq) | (NotEqAnd, NotEqAnd) => {
                            Ok(NotEqAnd)
                        }
                        _ => Err(unsupported_op_error()),
                    })
            }
            (Operator::Or, _) => {
                child_states
                    .into_iter()
                    .try_fold(NoPred, |state, child_state| match (state, child_state) {
                        // OR didn't connect children of Pred and NoPreds so it is ok
                        (NoPred, other) | (other, NoPred) => Ok(other),
                        (Eq, Eq) => Ok(Eq),
                        (Eq, EqOr) | (EqOr, Eq) | (EqOr, EqOr) => Ok(EqOr),
                        _ => Err(unsupported_op_error()),
                    })
            }
            // Any other operator ensure all children are NoPred
            _ => {
                child_states.into_iter().try_for_each(|child_state| {
                    if !matches!(child_state, NoPred) {
                        Err(unsupported_op_error())
                    } else {
                        Ok(())
                    }
                })?;
                Ok(NoPred)
            }
        }
    }
}

/// searches for unsupported patterns of predicates that we can't
/// translate into an include or exclude list
#[derive(Debug, Clone)]
struct FieldRefChecker {
    /// has seen a _field = <lit>
    seen_eq: bool,
    /// has seen a _field != <lit>
    seen_not_eq: bool,
    /// List of operators between the current node and the root of the expression
    /// on `post_visit` the invariant i that the top of the stack
    op_stack: Vec<OpState>,
}

impl FieldRefChecker {
    fn new() -> Self {
        Self {
            seen_eq: false,
            seen_not_eq: false,
            op_stack: vec![],
        }
    }

    /// validates that we have not seen both != and == operators against _field
    fn validate_operators(&self) -> DataFusionResult<()> {
        if self.seen_eq && self.seen_not_eq {
            Err(DataFusionError::Plan(
                "Unsupported _field predicates: both '=' and '!=' present".into(),
            ))
        } else {
            Ok(())
        }
    }

    /// Pops an operator from the op stack after validating that the
    /// stack does not contain an invalid pattern
    fn pop_stack_and_validate(&mut self, op: &Operator) -> DataFusionResult<()> {
        let self_state = self.op_stack.pop().ok_or_else(|| {
            DataFusionError::Plan("Internal error: _field operator stack mismatch".into())
        })?;

        assert!(self_state.operator == *op);

        let field_ref_state = self_state.validate()?;

        // Add our state to our parent, if any. No parent means this
        // is the root expr)
        let parent_state = match self.op_stack.last_mut() {
            Some(parent_state) => parent_state,
            None => return Ok(()),
        };

        // Remeber our state on parent so it can combine on the way up
        parent_state.child_states.push(field_ref_state);
        Ok(())
    }
}

impl ExpressionVisitor for FieldRefChecker {
    /// Invoked before any children of `expr` are visisted.
    fn pre_visit(mut self, expr: &Expr) -> DataFusionResult<Recursion<Self>> {
        // Now check for = and != compatibility
        let is_field_ref = match check_binary(expr) {
            Some((Operator::Eq, _name)) => {
                self.seen_eq = true;
                self.validate_operators()?;
                true
            }
            Some((Operator::NotEq, _name)) => {
                self.seen_not_eq = true;
                self.validate_operators()?;
                true
            }
            _ => false,
        };

        if let Expr::BinaryExpr {
            left: _,
            op,
            right: _,
        } = expr
        {
            self.op_stack.push(OpState {
                operator: *op,
                is_field_ref,
                child_states: vec![],
            })
        }
        Ok(Recursion::Continue(self))
    }

    /// Invoked after all children of `expr` are visited.
    fn post_visit(mut self, expr: &Expr) -> DataFusionResult<Self> {
        if let Expr::BinaryExpr {
            left: _,
            op,
            right: _,
        } = expr
        {
            self.pop_stack_and_validate(op)?;
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_plan::{case, col};
    use test_helpers::assert_contains;

    #[test]
    fn test_field_column_rewriter() {
        let cases = vec![
            (
                // f1 = 1.82
                col("f1").eq(lit(1.82)),
                col("f1").eq(lit(1.82)),
                FieldProjection::None,
            ),
            (
                // _field != "f1"
                field_ref().not_eq(lit("f1")),
                lit(true),
                exclude(vec!["f1"]),
            ),
            (
                // _field = f1
                field_ref().eq(lit("f1")),
                lit(true),
                include(vec!["f1"]),
            ),
            (
                // _field = f1 AND _measurement = m1
                field_ref()
                    .eq(lit("f1"))
                    .and(col("_measurement").eq(lit("m1"))),
                lit(true).and(col("_measurement").eq(lit("m1"))),
                include(vec!["f1"]),
            ),
            (
                // (_field = f1) OR (_field = f2)
                field_ref().eq(lit("f1")).or(field_ref().eq(lit("f2"))),
                lit(true).or(lit(true)),
                include(vec!["f1", "f2"]),
            ),
            (
                // (f1 = 5) AND (((_field = f1) OR (_field = f3)) OR (_field = f2))
                col("f1").eq(lit(5.0)).and(
                    field_ref()
                        .eq(lit("f1"))
                        .or(field_ref().eq(lit("f3")))
                        .or(field_ref().eq(lit("f2"))),
                ),
                col("f1")
                    .eq(lit(5.0))
                    .and(lit(true).or(lit(true)).or(lit(true))),
                include(vec!["f1", "f2", "f3"]),
            ),
            (
                // (_field != f1) AND (_field != f2)
                field_ref()
                    .not_eq(lit("f1"))
                    .and(field_ref().not_eq(lit("f2"))),
                lit(true).and(lit(true)),
                exclude(vec!["f1", "f2"]),
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
                    .and(lit(true).and(lit(true)).and(lit(true))),
                exclude(vec!["f1", "f2", "f3"]),
            ),
        ];

        for (input, exp_expr, exp_projection) in cases {
            println!(
                "Running test\ninput: {:?}\nexpected_expr: {:?}\nexpected_projection: {:?}\n",
                input, exp_expr, exp_projection
            );
            let mut rewriter = FieldProjectionRewriter::new();

            let rewritten = rewriter.rewrite_field_exprs(input).unwrap();
            assert_eq!(rewritten, exp_expr);

            let field_projection = rewriter.into_projection();
            assert_eq!(field_projection, exp_projection);
        }
    }

    #[test]
    fn test_field_column_rewriter_unsupported() {
        let cases = vec![
            (
                // reverse operand order not supported
                // f1 = _field
                lit("f1").eq(field_ref()),
                "Unsupported _field predicate, could not rewrite for IOx",
            ),
            (
                // reverse operand order not supported
                // f1 != _field
                lit("f1").not_eq(field_ref()),
                "Unsupported _field predicate, could not rewrite for IOx",
            ),
            (
                // mismatched != and =
                // (_field != f1) AND (_field = f3)
                field_ref().not_eq(lit("f1")).and(field_ref().eq(lit("f3"))),
                "Error during planning: Unsupported _field predicates: both '=' and '!=' present",
            ),
            (
                // mismatched = and !=
                // (_field = f1) OR (_field != f3)
                field_ref().eq(lit("f1")).or(field_ref().not_eq(lit("f3"))),
                "Unsupported _field predicates: both '=' and '!=' present",
            ),
            (
                // mismatched AND and OR (unsupported)
                // (_field = f1) OR (_field = f3) AND (_field = f2)
                field_ref()
                    .eq(lit("f1"))
                    .or(field_ref().eq(lit("f3")))
                    .and(field_ref().eq(lit("f2"))),
                "Unsupported structure with _field reference",
            ),
            (
                // mismatched AND and OR (unsupported)
                // (_field = f1) AND (_field = f3) OR (_field = f2)
                field_ref()
                    .eq(lit("f1"))
                    .and(field_ref().eq(lit("f3")))
                    .or(field_ref().eq(lit("f2"))),
                "Unsupported structure with _field reference",
            ),
            (
                // mismatched OR
                // (_field != f1) OR (_field != f3)
                field_ref()
                    .not_eq(lit("f1"))
                    .or(field_ref().not_eq(lit("f3"))),
                "Unsupported structure with _field reference",
            ),
            (
                // mismatched AND
                // (_field != f1) AND (_field = f3)
                field_ref().not_eq(lit("f1")).and(field_ref().eq(lit("f3"))),
                "Unsupported _field predicates: both '=' and '!=' present",
            ),
            (
                // bogus expr that we can't rewrite
                // _field IS NOT NULL
                field_ref().is_not_null(),
                "Unsupported _field predicate, could not rewrite for IOx",
            ),
            (
                // bogus expr that has a mix of = and !=
                // case X
                //   WHEN _field = "foo" THEN true
                //   WHEN  _field != "bar" THEN false
                // END
                case(col("x"))
                    .when(field_ref().eq(lit("foo")), lit(true))
                    .when(field_ref().not_eq(lit("bar")), lit(false))
                    .end()
                    .unwrap(),
                "Unsupported _field predicates: both '=' and '!=' present",
            ),
            (
                // bogus expr that has a mix of != and ==
                // case X
                //   WHEN  _field != "bar" THEN false
                //   WHEN _field = "foo" THEN true
                // END
                case(col("x"))
                    .when(field_ref().not_eq(lit("bar")), lit(false))
                    .when(field_ref().eq(lit("foo")), lit(true))
                    .end()
                    .unwrap(),
                "Unsupported _field predicates: both '=' and '!=' present",
            ),
        ];

        for (input, exp_error) in cases {
            println!(
                "Running test\ninput: {:?}\nexpected_error: {:?}\n",
                input, exp_error
            );
            let mut rewriter = FieldProjectionRewriter::new();
            let err = rewriter
                .rewrite_field_exprs(input)
                .expect_err("Expected error rewriting, but was successful");
            assert_contains!(err.to_string(), exp_error);
        }
    }

    /// returls a reference to the speciial _field column
    fn field_ref() -> Expr {
        col(FIELD_COLUMN_NAME)
    }

    /// Create a `FieldProjection::Include`
    fn include(names: impl IntoIterator<Item = &'static str>) -> FieldProjection {
        FieldProjection::Include(names.into_iter().map(|s| s.to_string()).collect())
    }

    /// Create a `FieldProjection::Include`
    fn exclude(names: impl IntoIterator<Item = &'static str>) -> FieldProjection {
        FieldProjection::Exclude(names.into_iter().map(|s| s.to_string()).collect())
    }
}
