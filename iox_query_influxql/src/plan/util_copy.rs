// NOTE: This code is copied from DataFusion, as it is not public,
// so all warnings are disabled.
#![allow(warnings)]
#![allow(clippy::all)]
//! A collection of utility functions copied from DataFusion.
//!
//! If these APIs are stabilised and made public, they can be removed from IOx.
//!
//! NOTE
use datafusion::common::tree_node::{TreeNode, VisitRecursion};
use datafusion::common::Result;
use datafusion::logical_expr::expr::{
    AggregateUDF, Alias, InList, InSubquery, Placeholder, ScalarFunction, ScalarUDF,
};
use datafusion::logical_expr::{
    expr::{
        AggregateFunction, Between, BinaryExpr, Case, Cast, Expr, GetIndexedField, GroupingSet,
        Like, Sort, TryCast, WindowFunction,
    },
    utils::expr_as_column_expr,
    LogicalPlan,
};

/// Returns a cloned `Expr`, but any of the `Expr`'s in the tree may be
/// replaced/customized by the replacement function.
///
/// The replacement function is called repeatedly with `Expr`, starting with
/// the argument `expr`, then descending depth-first through its
/// descendants. The function chooses to replace or keep (clone) each `Expr`.
///
/// The function's return type is `Result<Option<Expr>>>`, where:
///
/// * `Ok(Some(replacement_expr))`: A replacement `Expr` is provided; it is
///       swapped in at the particular node in the tree. Any nested `Expr` are
///       not subject to cloning/replacement.
/// * `Ok(None)`: A replacement `Expr` is not provided. The `Expr` is
///       recreated, with all of its nested `Expr`'s subject to
///       cloning/replacement.
/// * `Err(err)`: Any error returned by the function is returned as-is by
///       `clone_with_replacement()`.
///
/// Source: <https://github.com/apache/arrow-datafusion/blob/26e1b20ea/datafusion/sql/src/utils.rs#L153>
pub(super) fn clone_with_replacement<F>(expr: &Expr, replacement_fn: &F) -> Result<Expr>
where
    F: Fn(&Expr) -> Result<Option<Expr>>,
{
    let replacement_opt = replacement_fn(expr)?;

    match replacement_opt {
        // If we were provided a replacement, use the replacement. Do not
        // descend further.
        Some(replacement) => Ok(replacement),
        // No replacement was provided, clone the node and recursively call
        // clone_with_replacement() on any nested expressions.
        None => {
            match expr {
                Expr::AggregateFunction(AggregateFunction {
                    fun,
                    args,
                    distinct,
                    filter,
                    order_by,
                }) => Ok(Expr::AggregateFunction(AggregateFunction::new(
                    fun.clone(),
                    args.iter()
                        .map(|e| clone_with_replacement(e, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                    *distinct,
                    filter.clone(),
                    order_by.clone(),
                ))),
                Expr::WindowFunction(WindowFunction {
                    fun,
                    args,
                    partition_by,
                    order_by,
                    window_frame,
                }) => Ok(Expr::WindowFunction(WindowFunction::new(
                    fun.clone(),
                    args.iter()
                        .map(|e| clone_with_replacement(e, replacement_fn))
                        .collect::<Result<Vec<_>>>()?,
                    partition_by
                        .iter()
                        .map(|e| clone_with_replacement(e, replacement_fn))
                        .collect::<Result<Vec<_>>>()?,
                    order_by
                        .iter()
                        .map(|e| clone_with_replacement(e, replacement_fn))
                        .collect::<Result<Vec<_>>>()?,
                    window_frame.clone(),
                ))),
                Expr::AggregateUDF(AggregateUDF {
                    fun,
                    args,
                    filter,
                    order_by,
                }) => Ok(Expr::AggregateUDF(AggregateUDF {
                    fun: fun.clone(),
                    args: args
                        .iter()
                        .map(|e| clone_with_replacement(e, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                    filter: filter.clone(),
                    order_by: order_by.clone(),
                })),
                Expr::Alias(Alias {
                    expr: nested_expr,
                    name: alias_name,
                }) => Ok(Expr::Alias(Alias {
                    expr: Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                    name: alias_name.clone(),
                })),
                Expr::Between(Between {
                    expr,
                    negated,
                    low,
                    high,
                }) => Ok(Expr::Between(Between::new(
                    Box::new(clone_with_replacement(expr, replacement_fn)?),
                    *negated,
                    Box::new(clone_with_replacement(low, replacement_fn)?),
                    Box::new(clone_with_replacement(high, replacement_fn)?),
                ))),
                Expr::InList(InList {
                    expr: nested_expr,
                    list,
                    negated,
                }) => Ok(Expr::InList(InList {
                    expr: Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                    list: list
                        .iter()
                        .map(|e| clone_with_replacement(e, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                    negated: *negated,
                })),
                Expr::BinaryExpr(BinaryExpr { left, right, op }) => {
                    Ok(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(clone_with_replacement(left, replacement_fn)?),
                        *op,
                        Box::new(clone_with_replacement(right, replacement_fn)?),
                    )))
                }
                Expr::Like(Like {
                    negated,
                    expr,
                    pattern,
                    case_insensitive,
                    escape_char,
                }) => Ok(Expr::Like(Like::new(
                    *negated,
                    Box::new(clone_with_replacement(expr, replacement_fn)?),
                    Box::new(clone_with_replacement(pattern, replacement_fn)?),
                    *escape_char,
                    *case_insensitive,
                ))),
                Expr::SimilarTo(Like {
                    negated,
                    expr,
                    pattern,
                    case_insensitive,
                    escape_char,
                }) => Ok(Expr::SimilarTo(Like::new(
                    *negated,
                    Box::new(clone_with_replacement(expr, replacement_fn)?),
                    Box::new(clone_with_replacement(pattern, replacement_fn)?),
                    *escape_char,
                    *case_insensitive,
                ))),
                Expr::Case(case) => Ok(Expr::Case(Case::new(
                    match &case.expr {
                        Some(case_expr) => {
                            Some(Box::new(clone_with_replacement(case_expr, replacement_fn)?))
                        }
                        None => None,
                    },
                    case.when_then_expr
                        .iter()
                        .map(|(a, b)| {
                            Ok((
                                Box::new(clone_with_replacement(a, replacement_fn)?),
                                Box::new(clone_with_replacement(b, replacement_fn)?),
                            ))
                        })
                        .collect::<Result<Vec<(_, _)>>>()?,
                    match &case.else_expr {
                        Some(else_expr) => {
                            Some(Box::new(clone_with_replacement(else_expr, replacement_fn)?))
                        }
                        None => None,
                    },
                ))),
                Expr::ScalarFunction(ScalarFunction { fun, args }) => {
                    Ok(Expr::ScalarFunction(ScalarFunction {
                        fun: fun.clone(),
                        args: args
                            .iter()
                            .map(|e| clone_with_replacement(e, replacement_fn))
                            .collect::<Result<Vec<Expr>>>()?,
                    }))
                }
                Expr::ScalarUDF(ScalarUDF { fun, args }) => Ok(Expr::ScalarUDF(ScalarUDF {
                    fun: fun.clone(),
                    args: args
                        .iter()
                        .map(|arg| clone_with_replacement(arg, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                })),
                Expr::Negative(nested_expr) => Ok(Expr::Negative(Box::new(
                    clone_with_replacement(nested_expr, replacement_fn)?,
                ))),
                Expr::Not(nested_expr) => Ok(Expr::Not(Box::new(clone_with_replacement(
                    nested_expr,
                    replacement_fn,
                )?))),
                Expr::IsNotNull(nested_expr) => Ok(Expr::IsNotNull(Box::new(
                    clone_with_replacement(nested_expr, replacement_fn)?,
                ))),
                Expr::IsNull(nested_expr) => Ok(Expr::IsNull(Box::new(clone_with_replacement(
                    nested_expr,
                    replacement_fn,
                )?))),
                Expr::IsTrue(nested_expr) => Ok(Expr::IsTrue(Box::new(clone_with_replacement(
                    nested_expr,
                    replacement_fn,
                )?))),
                Expr::IsFalse(nested_expr) => Ok(Expr::IsFalse(Box::new(clone_with_replacement(
                    nested_expr,
                    replacement_fn,
                )?))),
                Expr::IsUnknown(nested_expr) => Ok(Expr::IsUnknown(Box::new(
                    clone_with_replacement(nested_expr, replacement_fn)?,
                ))),
                Expr::IsNotTrue(nested_expr) => Ok(Expr::IsNotTrue(Box::new(
                    clone_with_replacement(nested_expr, replacement_fn)?,
                ))),
                Expr::IsNotFalse(nested_expr) => Ok(Expr::IsNotFalse(Box::new(
                    clone_with_replacement(nested_expr, replacement_fn)?,
                ))),
                Expr::IsNotUnknown(nested_expr) => Ok(Expr::IsNotUnknown(Box::new(
                    clone_with_replacement(nested_expr, replacement_fn)?,
                ))),
                Expr::Cast(Cast { expr, data_type }) => Ok(Expr::Cast(Cast::new(
                    Box::new(clone_with_replacement(expr, replacement_fn)?),
                    data_type.clone(),
                ))),
                Expr::TryCast(TryCast {
                    expr: nested_expr,
                    data_type,
                }) => Ok(Expr::TryCast(TryCast::new(
                    Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                    data_type.clone(),
                ))),
                Expr::Sort(Sort {
                    expr: nested_expr,
                    asc,
                    nulls_first,
                }) => Ok(Expr::Sort(Sort::new(
                    Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                    *asc,
                    *nulls_first,
                ))),
                Expr::Column { .. }
                | Expr::OuterReferenceColumn(_, _)
                | Expr::Literal(_)
                | Expr::ScalarVariable(_, _)
                | Expr::Exists { .. }
                | Expr::ScalarSubquery(_) => Ok(expr.clone()),
                Expr::InSubquery(InSubquery {
                    expr: nested_expr,
                    subquery,
                    negated,
                }) => Ok(Expr::InSubquery(InSubquery {
                    expr: Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                    subquery: subquery.clone(),
                    negated: *negated,
                })),
                Expr::Wildcard => Ok(Expr::Wildcard),
                Expr::QualifiedWildcard { .. } => Ok(expr.clone()),
                Expr::GetIndexedField(GetIndexedField { key, expr }) => {
                    Ok(Expr::GetIndexedField(GetIndexedField::new(
                        Box::new(clone_with_replacement(expr.as_ref(), replacement_fn)?),
                        key.clone(),
                    )))
                }
                Expr::GroupingSet(set) => match set {
                    GroupingSet::Rollup(exprs) => Ok(Expr::GroupingSet(GroupingSet::Rollup(
                        exprs
                            .iter()
                            .map(|e| clone_with_replacement(e, replacement_fn))
                            .collect::<Result<Vec<Expr>>>()?,
                    ))),
                    GroupingSet::Cube(exprs) => Ok(Expr::GroupingSet(GroupingSet::Cube(
                        exprs
                            .iter()
                            .map(|e| clone_with_replacement(e, replacement_fn))
                            .collect::<Result<Vec<Expr>>>()?,
                    ))),
                    GroupingSet::GroupingSets(lists_of_exprs) => {
                        let mut new_lists_of_exprs = vec![];
                        for exprs in lists_of_exprs {
                            new_lists_of_exprs.push(
                                exprs
                                    .iter()
                                    .map(|e| clone_with_replacement(e, replacement_fn))
                                    .collect::<Result<Vec<Expr>>>()?,
                            );
                        }
                        Ok(Expr::GroupingSet(GroupingSet::GroupingSets(
                            new_lists_of_exprs,
                        )))
                    }
                },
                Expr::Placeholder(Placeholder { id, data_type }) => {
                    Ok(Expr::Placeholder(Placeholder {
                        id: id.clone(),
                        data_type: data_type.clone(),
                    }))
                }
            }
        }
    }
}

/// Search the provided `Expr`'s, and all of their nested `Expr`, for any that
/// pass the provided test. The returned `Expr`'s are deduplicated and returned
/// in order of appearance (depth first).
///
/// # NOTE
///
/// Copied from DataFusion
pub(super) fn find_exprs_in_exprs<F>(exprs: &[Expr], test_fn: &F) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    exprs
        .iter()
        .flat_map(|expr| find_exprs_in_expr(expr, test_fn))
        .fold(vec![], |mut acc, expr| {
            if !acc.contains(&expr) {
                acc.push(expr)
            }
            acc
        })
}

/// Search an `Expr`, and all of its nested `Expr`'s, for any that pass the
/// provided test. The returned `Expr`'s are deduplicated and returned in order
/// of appearance (depth first).
///
/// # NOTE
///
/// Copied from DataFusion
fn find_exprs_in_expr<F>(expr: &Expr, test_fn: &F) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    let mut exprs = vec![];
    expr.apply(&mut |expr| {
        if test_fn(expr) {
            if !(exprs.contains(expr)) {
                exprs.push(expr.clone())
            }
            // stop recursing down this expr once we find a match
            return Ok(VisitRecursion::Skip);
        }

        Ok(VisitRecursion::Continue)
    })
    // pre_visit always returns OK, so this will always too
    .expect("no way to return error during recursion");
    exprs
}
