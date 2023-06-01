use crate::plan::error;
use datafusion::common;
use datafusion::common::tree_node::{TreeNode, TreeNodeVisitor, VisitRecursion};
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{binary_expr, lit, Expr, Operator};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::optimizer::utils::disjunction;
use datafusion_util::AsExpr;
use std::cmp::Ordering;
use std::collections::Bound;

/// Traverse `expr` and promote time range expressions to the root
/// binary node on the left hand side and combined using the conjunction (`AND`)
/// operator to ensure the time range is applied to the entire predicate.
///
/// Additionally, multiple `time = <timestamp>` expressions are combined
/// using the disjunction (OR) operator, whereas, InfluxQL only supports
/// a single `time = <timestamp>` expression.
///
/// # NOTE
///
/// Combining relational operators like `time > now() - 5s` and equality
/// operators like `time = <timestamp>` with a disjunction (`OR`)
/// will evaluate to `false`, like InfluxQL.
///
/// # Background
///
/// The InfluxQL query engine always promotes the time range expression to filter
/// all results. It is misleading that time ranges are written in the `WHERE` clause,
/// as the `WHERE` predicate is not evaluated in its entirety for each row. Rather,
/// InfluxQL extracts the time range to form a time bound for the entire query and
/// removes any time range expressions from the filter predicate. The time range
/// is determined using the `>` and `≥` operators to form the lower bound and
/// the `<` and `≤` operators to form the upper bound. When multiple instances of
/// the lower or upper bound operators are found, the time bounds will form the
/// intersection. For example
///
/// ```sql
/// WHERE time >= 1000 AND time >= 2000 AND time < 10000 and time < 9000
/// ```
///
/// is equivalent to
///
/// ```sql
/// WHERE time >= 2000 AND time < 9000
/// ```
///
/// Further, InfluxQL only allows a single `time = <value>` binary expression. Multiple
/// occurrences result in an empty result set.
///
/// ## Examples
///
/// Lets illustrate how InfluxQL applies predicates with a typical example, using the
/// `metrics.lp` data in the IOx repository:
///
/// ```sql
/// SELECT cpu, usage_idle FROM cpu
/// WHERE
///   time > '2020-06-11T16:53:30Z' AND time < '2020-06-11T16:55:00Z' AND cpu = 'cpu0'
/// ```
///
/// InfluxQL first filters rows based on the time range:
///
/// ```sql
/// '2020-06-11T16:53:30Z' < time <  '2020-06-11T16:55:00Z'
/// ```
///
/// and then applies the predicate to the individual rows:
///
/// ```sql
/// cpu = 'cpu0'
/// ```
///
/// Producing the following result:
///
/// ```text
/// name: cpu
/// time                 cpu  usage_idle
/// ----                 ---  ----------
/// 2020-06-11T16:53:40Z cpu0 90.29029029029029
/// 2020-06-11T16:53:50Z cpu0 89.8
/// 2020-06-11T16:54:00Z cpu0 90.09009009009009
/// 2020-06-11T16:54:10Z cpu0 88.82235528942115
/// ```
///
/// The following example is a little more complicated, but shows again how InfluxQL
/// separates the time ranges from the predicate:
///
/// ```sql
/// SELECT cpu, usage_idle FROM cpu
/// WHERE
///   time > '2020-06-11T16:53:30Z' AND time < '2020-06-11T16:55:00Z' AND cpu = 'cpu0' OR cpu = 'cpu1'
/// ```
///
/// InfluxQL first filters rows based on the time range:
///
/// ```sql
/// '2020-06-11T16:53:30Z' < time <  '2020-06-11T16:55:00Z'
/// ```
///
/// and then applies the predicate to the individual rows:
///
/// ```sql
/// cpu = 'cpu0' OR cpu = 'cpu1'
/// ```
///
/// This is certainly quite different to SQL, which would evaluate the predicate as:
///
/// ```sql
/// SELECT cpu, usage_idle FROM cpu
/// WHERE
///   (time > '2020-06-11T16:53:30Z' AND time < '2020-06-11T16:55:00Z' AND cpu = 'cpu0') OR cpu = 'cpu1'
/// ```
///
/// ## Time ranges are not normal
///
/// Here we demonstrate how the operators combining time ranges do not matter. Using the
/// original query:
///
/// ```sql
/// SELECT cpu, usage_idle FROM cpu
/// WHERE
///   time > '2020-06-11T16:53:30Z' AND time < '2020-06-11T16:55:00Z' AND cpu = 'cpu0'
/// ```
///
/// we replace all `AND` operators with `OR`:
///
/// ```sql
/// SELECT cpu, usage_idle FROM cpu
/// WHERE
///   time > '2020-06-11T16:53:30Z' OR time < '2020-06-11T16:55:00Z' OR cpu = 'cpu0'
/// ```
///
/// This should return all rows, but yet it returns the same result 🤯:
///
/// ```text
/// name: cpu
/// time                 cpu  usage_idle
/// ----                 ---  ----------
/// 2020-06-11T16:53:40Z cpu0 90.29029029029029
/// 2020-06-11T16:53:50Z cpu0 89.8
/// 2020-06-11T16:54:00Z cpu0 90.09009009009009
/// 2020-06-11T16:54:10Z cpu0 88.82235528942115
/// ```
///
/// It becomes clearer, if we again review at how InfluxQL OG evaluates the `WHERE`
/// predicate, InfluxQL first filters rows based on the time range, which uses the
/// rules previously defined by finding `>` and `≥` to determine the lower bound
/// and `<` and `≤`:
///
/// ```sql
/// '2020-06-11T16:53:30Z' < time <  '2020-06-11T16:55:00Z'
/// ```
///
/// and then applies the predicate to the individual rows:
///
/// ```sql
/// cpu = 'cpu0'
/// ```
///
/// ## How to think of time ranges intuitively
///
/// Imagine a slight variation of InfluxQL has a separate _time bounds clause_.
/// It could have two forms, first as a `BETWEEN`
///
/// ```sql
/// SELECT cpu, usage_idle FROM cpu
/// WITH TIME BETWEEN '2020-06-11T16:53:30Z' AND '2020-06-11T16:55:00Z'
/// WHERE
///   cpu = 'cpu0'
/// ```
///
/// or as an `IN` to select multiple points:
///
/// ```sql
/// SELECT cpu, usage_idle FROM cpu
/// WITH TIME IN ('2004-04-09T12:00:00Z', '2004-04-09T12:00:10Z', ...)
/// WHERE
///   cpu = 'cpu0'
/// ```
pub fn rewrite_time_range_exprs(
    expr: Expr,
    simplifier: &ExprSimplifier<SimplifyContext<'_>>,
) -> common::Result<Expr> {
    let has_time_range = matches!(
        expr.apply(&mut |expr| Ok(match expr {
            expr @ Expr::BinaryExpr(_) if is_time_range(expr) => {
                VisitRecursion::Stop
            }
            _ => VisitRecursion::Continue,
        }))
        .expect("infallible"),
        VisitRecursion::Stop
    );

    // Nothing to do if there are no time range expressions
    if !has_time_range {
        return Ok(expr);
    }

    let mut rw = SeparateTimeRange::new(simplifier);
    expr.visit(&mut rw)?;

    // When `expr` contains both time expressions using relational
    // operators like > or <= and equality, such as
    //
    // WHERE time > now() - 5s OR time = '2004-04-09:12:00:00Z' AND cpu = 'cpu0'
    //
    // the entire expression evaluates to `false` to be consistent with InfluxQL.
    if !rw.time_range.is_unbounded() && !rw.equality.is_empty() {
        return Ok(lit(false));
    }

    let lhs = if let Some(expr) = rw.time_range.as_expr() {
        Some(expr)
    } else if !rw.equality.is_empty() {
        disjunction(rw.equality.iter().map(|t| {
            "time"
                .as_expr()
                .eq(lit(ScalarValue::TimestampNanosecond(Some(*t), None)))
        }))
    } else {
        None
    };

    let rhs = rw
        .stack
        .pop()
        .ok_or_else(|| error::map::internal("expected expression on stack"))?;

    Ok(match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => binary_expr(lhs, Operator::And, rhs),
        (Some(expr), None) | (None, Some(expr)) => expr,
        (None, None) => lit(true),
    })
}

/// Returns `true` if `expr` refers to the `time` column.
fn is_time_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(Column{ name, .. }) if name == "time")
}

/// Returns `true` if `expr` is a time range expression
///
/// Examples include:
///
/// ```text
/// time = '2004-04-09T12:00:00Z'
/// ```
///
/// or
///
/// ```text
/// time > now() - 5m
/// ```
fn is_time_range(expr: &Expr) -> bool {
    use datafusion::logical_expr::BinaryExpr;
    use datafusion::logical_expr::Operator::*;
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Eq | NotEq | Gt | Lt | GtEq | LtEq,
            right,
        }) => is_time_column(left) || is_time_column(right),
        _ => false,
    }
}

/// Represents the lower bound, in nanoseconds, of a [`TimeRange`].
#[derive(Clone, Debug)]
struct LowerBound(Bound<i64>);

impl LowerBound {
    /// Create a new, time bound that is unbounded
    fn unbounded() -> Self {
        Self(Bound::Unbounded)
    }

    /// Create a new, time bound that includes `v`
    fn included(v: i64) -> Self {
        Self(Bound::Included(v))
    }

    /// Create a new, time bound that excludes `v`
    fn excluded(v: i64) -> Self {
        Self(Bound::Excluded(v))
    }

    /// Returns `true` if the receiver is unbounded.
    fn is_unbounded(&self) -> bool {
        matches!(self.0, Bound::Unbounded)
    }

    fn as_expr(&self) -> Option<Expr> {
        match self.0 {
            Bound::Included(ts) => Some(
                "time"
                    .as_expr()
                    .gt_eq(lit(ScalarValue::TimestampNanosecond(Some(ts), None))),
            ),
            Bound::Excluded(ts) => Some(
                "time"
                    .as_expr()
                    .gt(lit(ScalarValue::TimestampNanosecond(Some(ts), None))),
            ),
            Bound::Unbounded => None,
        }
    }
}

impl Default for LowerBound {
    fn default() -> Self {
        Self::unbounded()
    }
}

impl Eq for LowerBound {}

impl PartialEq<Self> for LowerBound {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd<Self> for LowerBound {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LowerBound {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.0, other.0) {
            (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
            (Bound::Unbounded, _) => Ordering::Less,
            (_, Bound::Unbounded) => Ordering::Greater,
            (Bound::Included(a), Bound::Included(b)) | (Bound::Excluded(a), Bound::Excluded(b)) => {
                a.cmp(&b)
            }
            (Bound::Included(a), Bound::Excluded(b)) => match a.cmp(&b) {
                Ordering::Equal => Ordering::Less,
                // We know that if a > b, b + 1 is safe from overflow
                Ordering::Greater if a == b + 1 => Ordering::Equal,
                ordering => ordering,
            },
            (Bound::Excluded(a), Bound::Included(b)) => match a.cmp(&b) {
                Ordering::Equal => Ordering::Greater,
                // We know that if a < b, a + 1 is safe from overflow
                Ordering::Less if a + 1 == b => Ordering::Equal,
                ordering => ordering,
            },
        }
    }
}

/// Represents the upper bound, in nanoseconds, of a [`TimeRange`].
#[derive(Clone, Debug)]
struct UpperBound(Bound<i64>);

impl UpperBound {
    /// Create a new, unbounded upper bound.
    fn unbounded() -> Self {
        Self(Bound::Unbounded)
    }

    /// Create a new, upper bound that includes `v`
    fn included(v: i64) -> Self {
        Self(Bound::Included(v))
    }

    /// Create a new, upper bound that excludes `v`
    fn excluded(v: i64) -> Self {
        Self(Bound::Excluded(v))
    }

    /// Returns `true` if the receiver is unbounded.
    fn is_unbounded(&self) -> bool {
        matches!(self.0, Bound::Unbounded)
    }

    fn as_expr(&self) -> Option<Expr> {
        match self.0 {
            Bound::Included(ts) => Some(
                "time"
                    .as_expr()
                    .lt_eq(lit(ScalarValue::TimestampNanosecond(Some(ts), None))),
            ),
            Bound::Excluded(ts) => Some(
                "time"
                    .as_expr()
                    .lt(lit(ScalarValue::TimestampNanosecond(Some(ts), None))),
            ),
            Bound::Unbounded => None,
        }
    }
}

impl Default for UpperBound {
    fn default() -> Self {
        Self::unbounded()
    }
}

impl Eq for UpperBound {}

impl PartialEq<Self> for UpperBound {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd<Self> for UpperBound {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for UpperBound {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.0, other.0) {
            (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
            (Bound::Unbounded, _) => Ordering::Greater,
            (_, Bound::Unbounded) => Ordering::Less,
            (Bound::Included(a), Bound::Included(b)) | (Bound::Excluded(a), Bound::Excluded(b)) => {
                a.cmp(&b)
            }
            (Bound::Included(a), Bound::Excluded(b)) => match a.cmp(&b) {
                Ordering::Equal => Ordering::Greater,
                // We know that if a < b, b - 1 is safe from underflow
                Ordering::Less if a == b - 1 => Ordering::Equal,
                ordering => ordering,
            },
            (Bound::Excluded(a), Bound::Included(b)) => match a.cmp(&b) {
                Ordering::Equal => Ordering::Less,
                // We know that if a > b, a - 1 is safe from overflow
                Ordering::Greater if a - 1 == b => Ordering::Equal,
                ordering => ordering,
            },
        }
    }
}

/// Represents a time range, with a single lower and upper bound.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct TimeRange {
    lower: LowerBound,
    upper: UpperBound,
}

impl TimeRange {
    /// Returns `true` if the time range is unbounded.
    fn is_unbounded(&self) -> bool {
        self.lower.is_unbounded() && self.upper.is_unbounded()
    }

    /// Returns the time range as a conditional expression.
    fn as_expr(&self) -> Option<Expr> {
        match (self.lower.as_expr(), self.upper.as_expr()) {
            (None, None) => None,
            (Some(e), None) | (None, Some(e)) => Some(e),
            (Some(lower), Some(upper)) => Some(lower.and(upper)),
        }
    }
}

struct SeparateTimeRange<'a> {
    simplifier: &'a ExprSimplifier<SimplifyContext<'a>>,
    stack: Vec<Option<Expr>>,
    time_range: TimeRange,

    /// Equality time range expressions, such as:
    ///
    /// ```sql
    /// time = '2004-04-09T12:00:00Z'
    equality: Vec<i64>,
}

impl<'a> SeparateTimeRange<'a> {
    fn new(simplifier: &'a ExprSimplifier<SimplifyContext<'_>>) -> Self {
        Self {
            simplifier,
            stack: vec![],
            time_range: Default::default(),
            equality: vec![],
        }
    }
}

impl<'a> TreeNodeVisitor for SeparateTimeRange<'a> {
    type N = Expr;

    fn pre_visit(&mut self, node: &Self::N) -> common::Result<VisitRecursion> {
        if matches!(node, Expr::BinaryExpr(_)) {
            Ok(VisitRecursion::Continue)
        } else {
            Ok(VisitRecursion::Skip)
        }
    }

    fn post_visit(&mut self, node: &Self::N) -> common::Result<VisitRecursion> {
        use datafusion::logical_expr::BinaryExpr;
        use datafusion::logical_expr::Operator::*;

        match node {
            // A binary expression where either the left or right
            // expression refers to the "time" column.
            //
            // "time"     OP expression
            // expression OP "time"
            //
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: op @ (Eq | NotEq | Gt | Lt | GtEq | LtEq),
                right,
            }) if is_time_column(left) | is_time_column(right) => {
                if matches!(op, NotEq) {
                    // Stop recursing, as != is an invalid operator for time expressions
                    return Ok(VisitRecursion::Stop);
                }

                self.stack.push(None);

                /// Op is the limited set of operators expected from here on,
                /// to avoid repeated wildcard match arms with unreachable!().
                enum Op {
                    Eq,
                    Gt,
                    GtEq,
                    Lt,
                    LtEq,
                }

                // Map the DataFusion Operator to Op
                let op = match op {
                    Eq => Op::Eq,
                    Gt => Op::Gt,
                    GtEq => Op::GtEq,
                    Lt => Op::Lt,
                    LtEq => Op::LtEq,
                    _ => unreachable!("expected: Eq | Gt | GtEq | Lt | LtEq"),
                };

                let (expr, op) = if is_time_column(left) {
                    (right, op)
                } else {
                    (
                        left,
                        match op {
                            Op::Eq => Op::Eq,
                            // swap the relational operators when the conditional is `expression OP "time"`
                            Op::Gt => Op::Lt,
                            Op::GtEq => Op::LtEq,
                            Op::Lt => Op::Gt,
                            Op::LtEq => Op::GtEq,
                        },
                    )
                };

                // resolve `now()` and reduce binary expressions to a single constant
                let expr = self.simplifier.simplify(*expr.clone())?;

                let ts = match expr {
                    Expr::Literal(ScalarValue::TimestampNanosecond(Some(ts), _)) => ts,
                    expr => {
                        return error::internal(format!(
                            "expected TimestampNanosecond, got: {}",
                            expr
                        ))
                    }
                };

                match op {
                    Op::Eq => {
                        if self.time_range.is_unbounded() {
                            self.equality.push(ts);
                        } else {
                            // Stop recursing, as we have observed incompatible
                            // time conditions using equality and relational operators
                            return Ok(VisitRecursion::Stop);
                        };
                    }
                    Op::Gt | Op::GtEq | Op::Lt | Op::LtEq if !self.equality.is_empty() => {
                        // Stop recursing, as we have observed incompatible
                        // time conditions using equality and relational operators
                        return Ok(VisitRecursion::Stop);
                    }
                    Op::Gt => {
                        let ts = LowerBound::excluded(ts);
                        if ts > self.time_range.lower {
                            self.time_range.lower = ts;
                        }
                    }
                    Op::GtEq => {
                        let ts = LowerBound::included(ts);
                        if ts > self.time_range.lower {
                            self.time_range.lower = ts;
                        }
                    }
                    Op::Lt => {
                        let ts = UpperBound::excluded(ts);
                        if ts < self.time_range.upper {
                            self.time_range.upper = ts;
                        }
                    }
                    Op::LtEq => {
                        let ts = UpperBound::included(ts);
                        if ts < self.time_range.upper {
                            self.time_range.upper = ts;
                        }
                    }
                }

                Ok(VisitRecursion::Continue)
            }

            node @ Expr::BinaryExpr(BinaryExpr {
                op:
                    Eq | NotEq | Gt | GtEq | Lt | LtEq | RegexMatch | RegexNotMatch | RegexIMatch
                    | RegexNotIMatch,
                ..
            }) => {
                self.stack.push(Some(node.clone()));
                Ok(VisitRecursion::Continue)
            }
            Expr::BinaryExpr(BinaryExpr {
                op: op @ (And | Or),
                ..
            }) => {
                let right = self
                    .stack
                    .pop()
                    .ok_or_else(|| error::map::internal("invalid expr stack"))?;
                let left = self
                    .stack
                    .pop()
                    .ok_or_else(|| error::map::internal("invalid expr stack"))?;
                self.stack.push(match (left, right) {
                    (Some(left), Some(right)) => Some(binary_expr(left, *op, right)),
                    (None, Some(node)) | (Some(node), None) => Some(node),
                    (None, None) => None,
                });

                Ok(VisitRecursion::Continue)
            }
            _ => Ok(VisitRecursion::Continue),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::plan::planner::time_range::{LowerBound, UpperBound};

    // #[test]
    // fn test_split_exprs() {
    //     use super::{LowerBound as L, TimeCondition as TC, UpperBound as U};
    //     use datafusion::common::ScalarValue as V;
    //
    //     let props = execution_props();
    //     let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
    //         "time",
    //         (&InfluxColumnType::Timestamp).into(),
    //         false,
    //     )]));
    //     let df_schema = schema.to_dfschema_ref().unwrap();
    //     let simplify_context = SimplifyContext::new(&props).with_schema(Arc::clone(&df_schema));
    //     let simplifier = ExprSimplifier::new(simplify_context);
    //
    //     use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
    //
    //     let split_exprs = |expr| super::split(expr, &simplifier).unwrap();
    //
    //     macro_rules! range {
    //         (lower=$LOWER:literal) => {
    //             TC::Range(TimeRange{lower: L::included($LOWER), upper: U::unbounded()})
    //         };
    //         (lower=$LOWER:literal, upper ex=$UPPER:literal) => {
    //             TC::Range(TimeRange{lower: L::included($LOWER), upper: U::excluded($UPPER)})
    //         };
    //         (lower ex=$LOWER:literal) => {
    //             TC::Range(TimeRange{lower: L::excluded($LOWER), upper: U::unbounded()})
    //         };
    //         (upper=$UPPER:literal) => {
    //             TC::Range(TimeRange{lower: L::unbounded(), upper: U::included($UPPER)})
    //         };
    //         (upper ex=$UPPER:literal) => {
    //             TC::Range(TimeRange{lower: L::unbounded(), upper: U::excluded($UPPER)})
    //         };
    //         (list=$($TS:literal),*) => {
    //             TC::List(vec![$($TS),*])
    //         }
    //     }
    //
    //     let expr = "time"
    //         .as_expr()
    //         .gt_eq(now() - lit(V::new_interval_dt(0, 1000)));
    //     let (cond, tr) = split_exprs(expr);
    //     assert!(cond.is_none());
    //     assert_eq!(tr.unwrap(), range!(lower = 1672531199000000000));
    //
    //     // reduces the lower bound to a single expression
    //     let expr = "time"
    //         .as_expr()
    //         .gt_eq(now() - lit(V::new_interval_dt(0, 1000)))
    //         .and(
    //             "time"
    //                 .as_expr()
    //                 .gt_eq(now() - lit(V::new_interval_dt(0, 500))),
    //         );
    //     let (cond, tr) = split_exprs(expr);
    //     assert!(cond.is_none());
    //     assert_eq!(tr.unwrap(), range!(lower = 1672531199500000000));
    //
    //     let expr = "time"
    //         .as_expr()
    //         .lt_eq(now() - lit(V::new_interval_dt(0, 1000)));
    //     let (cond, tr) = split_exprs(expr);
    //     assert!(cond.is_none());
    //     assert_eq!(tr.unwrap(), range!(upper = 1672531199000000000));
    //
    //     // reduces the upper bound to a single expression
    //     let expr = "time"
    //         .as_expr()
    //         .lt_eq(now() + lit(V::new_interval_dt(0, 1000)))
    //         .and(
    //             "time"
    //                 .as_expr()
    //                 .lt_eq(now() + lit(V::new_interval_dt(0, 500))),
    //         );
    //     let (cond, tr) = split_exprs(expr);
    //     assert!(cond.is_none());
    //     assert_eq!(tr.unwrap(), range!(upper = 1672531200500000000));
    //
    //     let expr = "time"
    //         .as_expr()
    //         .gt_eq(now() - lit(V::new_interval_dt(0, 1000)))
    //         .and("time".as_expr().lt(now()));
    //     let (cond, tr) = split_exprs(expr);
    //     assert!(cond.is_none());
    //     assert_eq!(
    //         tr.unwrap(),
    //         range!(lower=1672531199000000000, upper ex=1672531200000000000)
    //     );
    //
    //     let expr = "time"
    //         .as_expr()
    //         .gt_eq(now() - lit(V::new_interval_dt(0, 1000)))
    //         .and("cpu".as_expr().eq(lit("cpu0")));
    //     let (cond, tr) = split_exprs(expr);
    //     assert_eq!(cond.unwrap().to_string(), r#"cpu = Utf8("cpu0")"#);
    //     assert_eq!(tr.unwrap(), range!(lower = 1672531199000000000));
    //
    //     let expr = "time".as_expr().eq(lit_timestamp_nano(0));
    //     let (cond, tr) = split_exprs(expr);
    //     assert!(cond.is_none());
    //     assert_eq!(tr.unwrap(), range!(list = 0));
    //
    //     let expr = "instance"
    //         .as_expr()
    //         .eq(lit("instance-01"))
    //         .or("instance".as_expr().eq(lit("instance-02")))
    //         .and(
    //             "time"
    //                 .as_expr()
    //                 .gt_eq(now() - lit(V::new_interval_dt(0, 1000))),
    //         );
    //     let (cond, tr) = split_exprs(expr);
    //     assert_eq!(
    //         cond.unwrap().to_string(),
    //         r#"instance = Utf8("instance-01") OR instance = Utf8("instance-02")"#
    //     );
    //     assert_eq!(tr.unwrap(), range!(lower = 1672531199000000000));
    //
    //     let expr = "time"
    //         .as_expr()
    //         .gt_eq(now() - lit(V::new_interval_dt(0, 1000)))
    //         .and("time".as_expr().lt(now()))
    //         .and(
    //             "cpu"
    //                 .as_expr()
    //                 .eq(lit("cpu0"))
    //                 .or("cpu".as_expr().eq(lit("cpu1"))),
    //         );
    //     let (cond, tr) = split_exprs(expr);
    //     assert_eq!(
    //         cond.unwrap().to_string(),
    //         r#"cpu = Utf8("cpu0") OR cpu = Utf8("cpu1")"#
    //     );
    //     assert_eq!(
    //         tr.unwrap(),
    //         range!(lower=1672531199000000000, upper ex=1672531200000000000)
    //     );
    //
    //     // time >= now - 60s AND time < now() OR cpu = 'cpu0' OR cpu = 'cpu1'
    //     //
    //     // Split the time range, despite using the disjunction (OR) operator
    //     let expr = "time"
    //         .as_expr()
    //         .gt_eq(now() - lit(V::new_interval_dt(0, 1000)))
    //         .and("time".as_expr().lt(now()))
    //         .or("cpu"
    //             .as_expr()
    //             .eq(lit("cpu0"))
    //             .or("cpu".as_expr().eq(lit("cpu1"))));
    //     let (cond, tr) = split_exprs(expr);
    //     assert_eq!(
    //         cond.unwrap().to_string(),
    //         r#"cpu = Utf8("cpu0") OR cpu = Utf8("cpu1")"#
    //     );
    //     assert_eq!(
    //         tr.unwrap(),
    //         range!(lower=1672531199000000000, upper ex=1672531200000000000)
    //     );
    //
    //     // time = 0 OR time = 10 AND cpu = 'cpu0'
    //     let expr = "time".as_expr().eq(lit_timestamp_nano(0)).or("time"
    //         .as_expr()
    //         .eq(lit_timestamp_nano(10))
    //         .and("cpu".as_expr().eq(lit("cpu0"))));
    //     let (cond, tr) = split_exprs(expr);
    //     assert_eq!(cond.unwrap().to_string(), r#"cpu = Utf8("cpu0")"#);
    //     assert_eq!(tr.unwrap(), range!(list = 0, 10));
    //
    //     // no time
    //     let expr = "f64".as_expr().gt_eq(lit(19.5_f64)).or(binary_expr(
    //         "f64".as_expr(),
    //         Operator::RegexMatch,
    //         lit("foo"),
    //     ));
    //     let (cond, tr) = split_exprs(expr);
    //     assert_eq!(
    //         cond.unwrap().to_string(),
    //         r#"f64 >= Float64(19.5) OR (f64 ~ Utf8("foo"))"#
    //     );
    //     assert!(tr.is_none());
    //
    //     // fallible
    //
    //     let expr = "time"
    //         .as_expr()
    //         .eq(lit_timestamp_nano(0))
    //         .or("time".as_expr().gt(now()));
    //     let (cond, tr) = split_exprs(expr);
    //     assert_eq!(cond.unwrap().to_string(), r#"Boolean(false)"#);
    //     assert!(tr.is_none());
    // }

    #[test]
    fn test_lower_bound_cmp() {
        let (a, b) = (LowerBound::unbounded(), LowerBound::unbounded());
        assert!(a == b);

        let (a, b) = (LowerBound::included(5), LowerBound::included(5));
        assert!(a == b);

        let (a, b) = (LowerBound::included(5), LowerBound::included(6));
        assert!(a < b);

        // a >= 6 gt a >= 5
        let (a, b) = (LowerBound::included(6), LowerBound::included(5));
        assert!(a > b);

        let (a, b) = (LowerBound::excluded(5), LowerBound::excluded(5));
        assert!(a == b);

        let (a, b) = (LowerBound::excluded(5), LowerBound::excluded(6));
        assert!(a < b);

        let (a, b) = (LowerBound::excluded(6), LowerBound::excluded(5));
        assert!(a > b);

        let (a, b) = (LowerBound::unbounded(), LowerBound::included(5));
        assert!(a < b);

        let (a, b) = (LowerBound::unbounded(), LowerBound::excluded(5));
        assert!(a < b);

        let (a, b) = (LowerBound::included(5), LowerBound::unbounded());
        assert!(a > b);

        let (a, b) = (LowerBound::excluded(5), LowerBound::unbounded());
        assert!(a > b);

        let (a, b) = (LowerBound::included(5), LowerBound::excluded(5));
        assert!(a < b);

        let (a, b) = (LowerBound::included(5), LowerBound::excluded(4));
        assert!(a == b);

        let (a, b) = (LowerBound::included(5), LowerBound::excluded(6));
        assert!(a < b);

        let (a, b) = (LowerBound::included(6), LowerBound::excluded(5));
        assert!(a == b);

        let (a, b) = (LowerBound::excluded(5), LowerBound::included(5));
        assert!(a > b);

        let (a, b) = (LowerBound::excluded(5), LowerBound::included(6));
        assert!(a == b);

        let (a, b) = (LowerBound::excluded(6), LowerBound::included(5));
        assert!(a > b);
    }

    #[test]
    fn test_upper_bound_cmp() {
        let (a, b) = (UpperBound::unbounded(), UpperBound::unbounded());
        assert!(a == b);

        let (a, b) = (UpperBound::included(5), UpperBound::included(5));
        assert!(a == b);

        let (a, b) = (UpperBound::included(5), UpperBound::included(6));
        assert!(a < b);

        let (a, b) = (UpperBound::included(6), UpperBound::included(5));
        assert!(a > b);

        let (a, b) = (UpperBound::excluded(5), UpperBound::excluded(5));
        assert!(a == b);

        let (a, b) = (UpperBound::excluded(5), UpperBound::excluded(6));
        assert!(a < b);

        let (a, b) = (UpperBound::excluded(6), UpperBound::excluded(5));
        assert!(a > b);

        let (a, b) = (UpperBound::unbounded(), UpperBound::included(5));
        assert!(a > b);

        let (a, b) = (UpperBound::unbounded(), UpperBound::excluded(5));
        assert!(a > b);

        let (a, b) = (UpperBound::included(5), UpperBound::unbounded());
        assert!(a < b);

        let (a, b) = (UpperBound::excluded(5), UpperBound::unbounded());
        assert!(a < b);

        let (a, b) = (UpperBound::included(5), UpperBound::excluded(5));
        assert!(a > b);

        let (a, b) = (UpperBound::included(5), UpperBound::excluded(4));
        assert!(a > b);

        let (a, b) = (UpperBound::included(5), UpperBound::excluded(6));
        assert!(a == b);

        let (a, b) = (UpperBound::included(5), UpperBound::excluded(7));
        assert!(a < b);

        let (a, b) = (UpperBound::excluded(5), UpperBound::included(5));
        assert!(a < b);

        let (a, b) = (UpperBound::excluded(5), UpperBound::included(6));
        assert!(a < b);

        let (a, b) = (UpperBound::excluded(5), UpperBound::included(4));
        assert!(a == b);
    }
}
