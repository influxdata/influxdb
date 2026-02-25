//! An optimizer rule that transforms a plan
//! to fill gaps in time series data.

pub mod range_predicate;
mod virtual_function;

use crate::exec::gapfill::{FillExpr, FillStrategy, GapFill};
use datafusion::common::{
    DFSchema, ExprSchema, internal_datafusion_err, plan_datafusion_err, plan_err,
};
use datafusion::logical_expr::{ExprSchemable, expr::AggregateFunction};
use datafusion::scalar::ScalarValue;
use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter},
    config::ConfigOptions,
    error::{DataFusionError, Result},
    logical_expr::{
        Aggregate, Extension, LogicalPlan, Projection, ScalarUDF,
        expr::{Alias, ScalarFunction},
        utils::expr_to_columns,
    },
    optimizer::AnalyzerRule,
    prelude::{Column, Expr, col},
};
use hashbrown::{HashMap, hash_map};
use query_functions::gapfill::{GapFillWrapper, INTERPOLATE_UDF_NAME, LOCF_UDF_NAME};
use query_functions::tz::TzUDF;
use std::{
    collections::HashSet,
    ops::{Bound, Range},
    sync::Arc,
};
use virtual_function::{VirtualFunction, VirtualFunctionFinder};

/// This optimizer rule enables gap-filling semantics for SQL queries
/// that contain calls to `DATE_BIN_GAPFILL()` and related functions
/// like `LOCF()`.
///
/// In SQL a typical gap-filling query might look like this:
/// ```sql
/// SELECT
///   location,
///   DATE_BIN_GAPFILL(INTERVAL '1 minute', time, '1970-01-01T00:00:00Z') AS minute,
///   LOCF(AVG(temp))
/// FROM temps
/// WHERE time > NOW() - INTERVAL '6 hours' AND time < NOW()
/// GROUP BY LOCATION, MINUTE
/// ```
///
/// The initial logical plan will look like this:
///
/// ```text
///   Projection: location, date_bin_gapfill(...) as minute, LOCF(AVG(temps.temp))
///     Aggregate: groupBy=[[location, date_bin_gapfill(...)]], aggr=[[AVG(temps.temp)]]
///       ...
/// ```
///
/// This optimizer rule transforms it to this:
///
/// ```text
///   Projection: location, date_bin_gapfill(...) as minute, AVG(temps.temp)
///     GapFill: groupBy=[[location, date_bin_gapfill(...))]], aggr=[[LOCF(AVG(temps.temp))]], start=..., stop=...
///       Aggregate: groupBy=[[location, date_bin(...))]], aggr=[[AVG(temps.temp)]]
///         ...
/// ```
///
/// For `Aggregate` nodes that contain calls to `DATE_BIN_GAPFILL`, this rule will:
/// - Convert `DATE_BIN_GAPFILL()` to `DATE_BIN()`
/// - Create a `GapFill` node that fills in gaps in the query
/// - The range for gap filling is found by analyzing any preceding `Filter` nodes
///
/// If there is a `Projection` above the `GapFill` node that gets created:
/// - Look for calls to gap-filling functions like `LOCF`
/// - Push down these functions into the `GapFill` node, updating the fill strategy for the column.
///
/// Note: both `DATE_BIN_GAPFILL` and `LOCF` are functions that don't have implementations.
/// This rule must rewrite the plan to get rid of them.
#[derive(Debug)]
pub struct HandleGapFill;

impl HandleGapFill {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for HandleGapFill {
    fn default() -> Self {
        Self::new()
    }
}

impl AnalyzerRule for HandleGapFill {
    fn name(&self) -> &str {
        "handle_gap_fill"
    }
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(handle_gap_fill).map(|t| t.data)
    }
}

fn handle_gap_fill(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let res = match plan {
        LogicalPlan::Aggregate(aggr) => {
            handle_aggregate(aggr).map_err(|e| e.context("handle_aggregate"))?
        }
        LogicalPlan::Projection(proj) => {
            handle_projection(proj).map_err(|e| e.context("handle_projection"))?
        }
        _ => Transformed::no(plan),
    };

    if !res.transformed {
        // no transformation was applied,
        // so make sure the plan is not using gap filling
        // functions in an unsupported way.
        check_node(&res.data)?;
    }

    Ok(res)
}

fn handle_aggregate(aggr: Aggregate) -> Result<Transformed<LogicalPlan>> {
    match replace_date_bin_gapfill(aggr).map_err(|e| e.context("replace_date_bin_gapfill"))? {
        // No change: return as-is
        RewriteInfo::Unchanged(aggr) => Ok(Transformed::no(LogicalPlan::Aggregate(aggr))),
        // Changed: new_aggr has DATE_BIN_GAPFILL replaced with DATE_BIN.
        RewriteInfo::Changed {
            new_aggr,
            date_bin_gapfill_index,
            date_bin_gapfill_args,
            date_bin_udf,
        } => {
            let new_aggr_plan = LogicalPlan::Aggregate(new_aggr);
            check_node(&new_aggr_plan).map_err(|e| e.context("check_node"))?;

            let new_gap_fill_plan = build_gapfill_node(
                new_aggr_plan,
                date_bin_gapfill_index,
                date_bin_gapfill_args,
                date_bin_udf,
            )
            .map_err(|e| e.context("build_gapfill_node"))?;
            Ok(Transformed::yes(new_gap_fill_plan))
        }
    }
}

fn build_gapfill_node(
    new_aggr_plan: LogicalPlan,
    date_bin_gapfill_index: usize,
    date_bin_gapfill_args: Vec<Expr>,
    date_bin_udf: Arc<ScalarUDF>,
) -> Result<LogicalPlan> {
    match date_bin_gapfill_args.len() {
        2 | 3 => (),
        nargs @ (0 | 1 | 4..) => {
            return Err(DataFusionError::Plan(format!(
                "DATE_BIN_GAPFILL expects 2 or 3 arguments, got {nargs}",
            )));
        }
    };

    let mut args_iter = date_bin_gapfill_args.into_iter();

    // Ensure that stride argument is a scalar
    let stride = args_iter.next().unwrap();
    validate_scalar_expr("stride argument to DATE_BIN_GAPFILL", &stride)
        .map_err(|e| e.context("validate_scalar_expr"))?;

    fn get_column(expr: Expr) -> Result<Column> {
        match expr {
            Expr::Column(c) => Ok(c),
            Expr::Cast(c) => get_column(*c.expr),
            Expr::ScalarFunction(ScalarFunction { func, args })
                if func.inner().as_any().is::<TzUDF>() =>
            {
                get_column(args[0].clone())
            }
            _ => Err(DataFusionError::Plan(
                "DATE_BIN_GAPFILL requires a column as the source argument".to_string(),
            )),
        }
    }

    // Ensure that the source argument is a column
    let time_col =
        get_column(args_iter.next().unwrap()).map_err(|e| e.context("get time column"))?;

    // Ensure that a time range was specified and is valid for gap filling
    let time_range = range_predicate::find_time_range(new_aggr_plan.inputs()[0], &time_col)
        .map_err(|e| e.context("find time range"))?;
    validate_time_range(&time_range).map_err(|e| e.context("validate time range"))?;

    // Ensure that origin argument is a scalar
    let origin = args_iter.next();
    if let Some(ref origin) = origin {
        validate_scalar_expr("origin argument to DATE_BIN_GAPFILL", origin)
            .map_err(|e| e.context("validate origin"))?;
    }

    // Make sure the time output to the gapfill node matches what the
    // aggregate output was.
    let time_column = col(datafusion::common::Column::from(
        new_aggr_plan
            .schema()
            .qualified_field(date_bin_gapfill_index),
    ));
    let time_column_alias = time_column.name_for_alias()?;

    let time_expr = date_bin_udf
        .call(if let Some(origin) = origin {
            vec![stride, time_column, origin]
        } else {
            vec![stride, time_column]
        })
        .alias(time_column_alias);

    let LogicalPlan::Aggregate(aggr) = &new_aggr_plan else {
        return Err(DataFusionError::Internal(format!(
            "Expected Aggregate plan, got {}",
            new_aggr_plan.display()
        )));
    };

    let mut col_it = aggr.schema.iter();
    let series_expr = (&mut col_it)
        .take(aggr.group_expr.len())
        .enumerate()
        .filter(|(idx, _)| *idx != date_bin_gapfill_index)
        .map(|(_, (qualifier, field))| {
            Expr::Column(datafusion::common::Column::from((
                qualifier,
                field.as_ref(),
            )))
        })
        .collect();

    // this schema is used for the `FillStrategy::Default` checks below. It also represents the
    // schema of the projection of `aggr`, meaning that it shows the columns/fields as they exist
    // after they've been transformed by `aggr` and once they're being input into the next step.
    // Because we are using it to get the data types of the output columns, to then get the default
    // value of those types according to the AggregateFunction below, it all works out.
    let schema = &aggr.schema;

    let fill_expr = col_it
        .map(|(qualifier, field)| {
            Expr::Column(datafusion::common::Column::from((
                qualifier,
                field.as_ref(),
            )))
        })
        // `aggr_expr` and `aggr.aggr_expr` should line up in the sense that `aggr.aggr_expr[n]`
        // represents a transformation that was done to produce `aggr_expr[n]`, so we can zip them
        // together like this to determine the correct fill type for the produced expression
        .zip(aggr.aggr_expr.iter())
        .map(|(col_expr, aggr_expr)| {
            // if aggr_expr is a function, then it may need special handling for what its
            // FillStrategy may be - specifically, it may produce a non-nullable column, so we need
            // to check if that's the case, and if it is, we need to determine the correct default
            // type to fill in with gapfill instead of just placing nulls. We also need to verify
            // that, after it's passed through `Aggregate`, it does produce a column - since
            // `col_expr` should be the 'computed'/'transformed' representation of `aggr_expr`, we
            // `aggr_expr`, we need to make sure that it's a column or else this doesn't really
            // matter to calculate.
            default_return_value_for_aggr_fn(aggr_expr, schema, col_expr.try_as_col()).map(|rt| {
                FillExpr {
                    expr: col_expr,
                    strategy: FillStrategy::Default(rt),
                }
            })
        })
        .collect::<Result<Vec<_>>>()?;

    match (fill_expr.len(), aggr.aggr_expr.len()) {
        (f, e) if f != e => {
            return Err(internal_datafusion_err!(
                "The number of aggregate expressions has gotten lost; expected {e}, found {f}. This is a bug, please report it."
            ));
        }
        _ => (),
    }

    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(
            GapFill::try_new(
                Arc::new(new_aggr_plan),
                series_expr,
                time_expr,
                fill_expr,
                time_range,
            )
            .map_err(|e| e.context("GapFill::try_new"))?,
        ),
    }))
}

fn validate_time_range(range: &Range<Bound<Expr>>) -> Result<()> {
    let Range { start, end } = range;
    let (start, end) = match (start, end) {
        (Bound::Unbounded, Bound::Unbounded) => {
            return Err(DataFusionError::Plan(
                "gap-filling query is missing both upper and lower time bounds".to_string(),
            ));
        }
        (Bound::Unbounded, _) => Err(DataFusionError::Plan(
            "gap-filling query is missing lower time bound".to_string(),
        )),
        (_, Bound::Unbounded) => Err(DataFusionError::Plan(
            "gap-filling query is missing upper time bound".to_string(),
        )),
        (
            Bound::Included(start) | Bound::Excluded(start),
            Bound::Included(end) | Bound::Excluded(end),
        ) => Ok((start, end)),
    }?;
    validate_scalar_expr("lower time bound", start)?;
    validate_scalar_expr("upper time bound", end)
}

fn validate_scalar_expr(what: &str, e: &Expr) -> Result<()> {
    let mut cols = HashSet::new();
    expr_to_columns(e, &mut cols)?;
    if !cols.is_empty() {
        Err(DataFusionError::Plan(format!(
            "{what} for gap fill query must evaluate to a scalar"
        )))
    } else {
        Ok(())
    }
}

enum RewriteInfo {
    // Group expressions were unchanged
    Unchanged(Aggregate),
    // Group expressions were changed
    Changed {
        // Group expressions with DATE_BIN_GAPFILL rewritten to DATE_BIN.
        new_aggr: Aggregate,
        // The index of the group expression that contained the call to DATE_BIN_GAPFILL.
        date_bin_gapfill_index: usize,
        // The arguments to the call to DATE_BIN_GAPFILL.
        date_bin_gapfill_args: Vec<Expr>,
        // The name of the UDF that provides the DATE_BIN like functionality.
        date_bin_udf: Arc<ScalarUDF>,
    },
}

// Iterate over the group expression list.
// If it finds no occurrences of date_bin_gapfill, it will return None.
// If it finds more than one occurrence it will return an error.
// Otherwise it will return a RewriteInfo for the analyzer rule to use.
fn replace_date_bin_gapfill(aggr: Aggregate) -> Result<RewriteInfo> {
    let mut date_bin_gapfill_count = 0;
    let mut dbg_idx = None;
    aggr.group_expr
        .iter()
        .enumerate()
        .try_for_each(|(i, e)| -> Result<()> {
            let mut functions = vec![];
            e.visit(&mut VirtualFunctionFinder::new(&mut functions))?;
            let fn_cnt = functions
                .iter()
                .filter(|vf| matches!(vf, VirtualFunction::GapFill(_)))
                .count();
            date_bin_gapfill_count += fn_cnt;
            if fn_cnt > 0 {
                dbg_idx = Some(i);
            }
            Ok(())
        })?;

    let (date_bin_gapfill_index, date_bin) = match date_bin_gapfill_count {
        0 => return Ok(RewriteInfo::Unchanged(aggr)),
        1 => {
            // Make sure that the call to DATE_BIN_GAPFILL is root expression
            // excluding aliases.
            let dbg_idx = dbg_idx.expect("should be found exactly one call");
            VirtualFunction::maybe_from_expr(unwrap_alias(&aggr.group_expr[dbg_idx]))
                .and_then(|vf| vf.date_bin_udf().cloned())
                .map(|f| (dbg_idx, f))
                .ok_or(plan_datafusion_err!("DATE_BIN_GAPFILL must be a top-level expression in the GROUP BY clause when gap filling. It cannot be part of another expression or cast"))?
        }
        _ => {
            return Err(DataFusionError::Plan(
                "DATE_BIN_GAPFILL specified more than once".to_string(),
            ));
        }
    };

    let date_bin_udf = Arc::clone(&date_bin);
    let mut rewriter = DateBinGapfillRewriter {
        args: None,
        date_bin,
    };
    let new_group_expr = aggr
        .group_expr
        .into_iter()
        .enumerate()
        .map(|(i, e)| {
            if i == date_bin_gapfill_index {
                e.rewrite(&mut rewriter).map(|t| t.data)
            } else {
                Ok(e)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let date_bin_gapfill_args = rewriter.args.expect("should have found args");

    // Create the aggregate node with the same output schema as the original
    // one. This means that there will be an output column called `date_bin_gapfill(...)`
    // even though the actual expression populating that column will be `date_bin(...)`.
    // This seems acceptable since it avoids having to deal with renaming downstream.
    let new_aggr =
        Aggregate::try_new_with_schema(aggr.input, new_group_expr, aggr.aggr_expr, aggr.schema)
            .map_err(|e| e.context("Aggregate::try_new_with_schema"))?;
    Ok(RewriteInfo::Changed {
        new_aggr,
        date_bin_gapfill_index,
        date_bin_gapfill_args,
        date_bin_udf,
    })
}

fn unwrap_alias(mut e: &Expr) -> &Expr {
    loop {
        match e {
            Expr::Alias(Alias { expr, .. }) => e = expr.as_ref(),
            e => break e,
        }
    }
}

struct DateBinGapfillRewriter {
    args: Option<Vec<Expr>>,
    date_bin: Arc<ScalarUDF>,
}

impl TreeNodeRewriter for DateBinGapfillRewriter {
    type Node = Expr;
    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        match &expr {
            Expr::ScalarFunction(fun) if fun.func.inner().as_any().is::<GapFillWrapper>() => {
                Ok(Transformed::new(expr, true, TreeNodeRecursion::Jump))
            }
            _ => Ok(Transformed::no(expr)),
        }
    }

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        // We need to preserve the name of the original expression
        // so that everything stays wired up.
        let orig_name = expr.schema_name().to_string();
        match expr {
            Expr::ScalarFunction(ScalarFunction { func, args })
                if func.inner().as_any().is::<GapFillWrapper>() =>
            {
                self.args = Some(args.clone());
                Ok(Transformed::yes(
                    Expr::ScalarFunction(ScalarFunction {
                        func: Arc::clone(&self.date_bin),
                        args,
                    })
                    .alias(orig_name),
                ))
            }
            _ => Ok(Transformed::no(expr)),
        }
    }
}

fn udf_to_fill_strategy(name: &str) -> Option<FillStrategy> {
    match name {
        LOCF_UDF_NAME => Some(FillStrategy::PrevNullAsMissing),
        INTERPOLATE_UDF_NAME => Some(FillStrategy::LinearInterpolate),
        _ => None,
    }
}

fn handle_projection(mut proj: Projection) -> Result<Transformed<LogicalPlan>> {
    let Some(child_gapfill) = (match proj.input.as_ref() {
        LogicalPlan::Extension(Extension { node }) => node.as_any().downcast_ref::<GapFill>(),
        _ => None,
    }) else {
        // If this is not a projection that is a parent to a GapFill node,
        // then there is nothing to do.
        return Ok(Transformed::no(LogicalPlan::Projection(proj)));
    };

    let mut fill_fn_rewriter = FillFnRewriter {
        aggr_col_fill_map: HashMap::new(),
    };

    let new_proj_exprs = proj
        .expr
        .iter()
        .map(|expr| {
            expr.clone()
                .rewrite(&mut fill_fn_rewriter)
                .map(|t| t.data)
                .map_err(|e| e.context(format!("rewrite: {expr}")))
        })
        .collect::<Result<Vec<Expr>>>()?;
    let FillFnRewriter { aggr_col_fill_map } = fill_fn_rewriter;

    if aggr_col_fill_map.is_empty() {
        return Ok(Transformed::no(LogicalPlan::Projection(proj)));
    }

    // Clone the existing GapFill node, then modify it in place
    // to reflect the new fill strategy.
    let mut new_gapfill = child_gapfill.clone();
    for (e, (fs, udf)) in aggr_col_fill_map {
        if new_gapfill.replace_fill_strategy(&e, fs).is_none() {
            // There was a gap filling function called on a non-aggregate column.
            return Err(DataFusionError::Plan(format!(
                "{udf} must be called on an aggregate column in a gap-filling query",
            )));
        }
    }
    proj.expr = new_proj_exprs;
    proj.input = Arc::new(LogicalPlan::Extension(Extension {
        node: Arc::new(new_gapfill),
    }));
    Ok(Transformed::yes(LogicalPlan::Projection(proj)))
}

/// Implements `TreeNodeRewriter`:
/// - Traverses over the expressions in a projection node
/// - If it finds a function that requires a non-Null FillStrategy (determined by
///   `udf_to_fill_strategy`), it replaces them with `col AS <original name>`
/// - Collects into [`Self::aggr_col_fill_map`] which correlates
///   aggregate columns to their [`FillStrategy`].
struct FillFnRewriter {
    aggr_col_fill_map: HashMap<Expr, (FillStrategy, String)>,
}

impl TreeNodeRewriter for FillFnRewriter {
    type Node = Expr;
    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        match &expr {
            Expr::ScalarFunction(fun) if udf_to_fill_strategy(fun.name()).is_some() => {
                Ok(Transformed::new(expr, true, TreeNodeRecursion::Jump))
            }
            _ => Ok(Transformed::no(expr)),
        }
    }

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        let orig_name = expr.schema_name().to_string();
        match expr {
            Expr::ScalarFunction(mut fun) => {
                let Some(fs) = udf_to_fill_strategy(fun.name()) else {
                    return Ok(Transformed::no(Expr::ScalarFunction(fun)));
                };

                let arg = fun.args.remove(0);
                self.add_fill_strategy(arg.clone(), fs, fun.name().to_string())?;
                Ok(Transformed::yes(arg.alias(orig_name)))
            }
            _ => Ok(Transformed::no(expr)),
        }
    }
}

impl FillFnRewriter {
    fn add_fill_strategy(&mut self, e: Expr, fs: FillStrategy, name: String) -> Result<()> {
        match self.aggr_col_fill_map.entry(e) {
            hash_map::Entry::Occupied(_) => Err(DataFusionError::NotImplemented(
                "multiple fill strategies for the same column".to_string(),
            )),
            hash_map::Entry::Vacant(ve) => {
                ve.insert((fs, name));
                Ok(())
            }
        }
    }
}

fn check_node(node: &LogicalPlan) -> Result<()> {
    node.expressions().iter().try_for_each(|expr| {
        let mut functions = vec![];
        expr.visit(&mut VirtualFunctionFinder::new(&mut functions))?;
        if functions.is_empty() {
            Ok(())
        } else {
            // There should be no virtual functions in this node, base the error message on the first one.
            match &functions[0] {
                VirtualFunction::GapFill(wrapped) => plan_err!(
                    "{}_gapfill may only be used as a GROUP BY expression",
                    wrapped.name()
                ),
                VirtualFunction::Locf => plan_err!("{LOCF_UDF_NAME} may only be used in the SELECT list of a gap-filling query"),
                VirtualFunction::Interpolate=> plan_err!("{INTERPOLATE_UDF_NAME} may only be used in the SELECT list of a gap-filling query"),
            }
        }
    })
}

/// Tries to process `maybe_wrapped_fun` as an [`AggregateFunction`] which may be wrapped by a type
/// which does not change its return type (e.g. [`Alias`]; read comment on `get_aggr_fn` inside for
/// more detail) and get its default return value, assuming that its output column (if `Some(_)` is
/// passed in for `output_column`) or its arguments (if `None` is passed in for `output_column`)
/// exist in `schema`.
///
/// This also only returns `Ok(Some(_))` if the wrapped func produces non-nullable output - because
/// this function, at time of writing, is only used to facilitate gapfilling values for
/// non-nullable columns, we only care about the return types for functions that cannot have
/// nullable outputs
///
/// # Arguments
///
/// * `maybe_wrapped_fun` - An expression that may contain an `AggregateFunction` whose output type
///   will not be transformed by the expressions wrapping it. If this turns out to not contain such a
///   AggregateFunction, this function returns Ok(None)
/// * `schema` - The schema that the output column or agg func argument types are expected to
///   reside in. read comment above for meaning of 'or' here
/// * `output_column` - A [`Column`] representing the output of this AggregateFunction with this
///   schema, if it is already know. `None` can be passed in for this function if unknown; passing in
///   `Some` just unlocks small performance gains.
pub fn default_return_value_for_aggr_fn(
    maybe_wrapped_fun: &Expr,
    schema: &DFSchema,
    output_column: Option<&Column>,
) -> Result<ScalarValue> {
    // we use this function to recurse through the structure of the expr that we're given to try to
    // find an aggregate function nested deep in it which still retains its same return type. E.g.
    // if an aggregate function is nested inside a `between`, it doesn't retain its return type,
    // since the type of the column is the return type of `between`, not of the function. But if
    // it's inside an `Alias`, it does retain its return type, since the output is not transformed
    // at all (just labeled differently).
    fn get_aggr_fn(expr: &Expr) -> Option<&AggregateFunction> {
        match expr {
            Expr::AggregateFunction(fun) => Some(fun),
            Expr::Alias(alias) => get_aggr_fn(&alias.expr),
            _ => None,
        }
    }

    // If there's not an aggregate function in there, we don't care
    let Some(fun) = get_aggr_fn(maybe_wrapped_fun) else {
        return maybe_wrapped_fun.get_type(schema)?.try_into();
    };

    // so if we already know the output column...
    output_column
        .map(|col| {
            // ...just get its type.
            schema
                .field_from_column(col)
                .map(|field| field.data_type().clone())
        })
        .unwrap_or_else(|| {
            // if we don't know the output column, query the aggregate function for its return type
            // with the provided arguments
            let args = fun
                .params
                .args
                .iter()
                .map(|arg| arg.to_field(schema).map(|f| f.1))
                .collect::<Result<Vec<_>, _>>()?;

            fun.func.return_field(&args).map(|f| f.data_type().clone())
        })
        // and then get the default value for that return type.
        .and_then(|return_type| fun.func.default_value(&return_type))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::HandleGapFill;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::ScalarValue;
    use datafusion::config::ConfigOptions;
    use datafusion::error::Result;
    use datafusion::functions_aggregate::expr_fn::{avg, min};
    use datafusion::logical_expr::builder::table_scan_with_filters;
    use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, logical_plan};
    use datafusion::optimizer::{Analyzer, AnalyzerRule};
    use datafusion::prelude::{Expr, case, col, lit};
    use datafusion_util::lit_timestamptz_nano;
    use query_functions::gapfill::{
        DATE_BIN_GAPFILL_UDF_NAME, INTERPOLATE_UDF_NAME, LOCF_UDF_NAME,
    };

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "time2",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("loc", DataType::Utf8, false),
            Field::new("temp", DataType::Float64, false),
        ])
    }

    fn table_scan() -> Result<LogicalPlan> {
        logical_plan::table_scan(Some("temps"), &schema(), None)?.build()
    }

    fn date_bin_gapfill(interval: Expr, time: Expr) -> Result<Expr> {
        date_bin_gapfill_with_origin(interval, time, None)
    }

    fn date_bin_gapfill_with_origin(
        interval: Expr,
        time: Expr,
        origin: Option<Expr>,
    ) -> Result<Expr> {
        let mut args = vec![interval, time];
        if let Some(origin) = origin {
            args.push(origin)
        }

        Ok(query_functions::registry()
            .udf(DATE_BIN_GAPFILL_UDF_NAME)?
            .call(args))
    }

    fn locf(arg: Expr) -> Result<Expr> {
        Ok(query_functions::registry()
            .udf(LOCF_UDF_NAME)?
            .call(vec![arg]))
    }

    fn interpolate(arg: Expr) -> Result<Expr> {
        Ok(query_functions::registry()
            .udf(INTERPOLATE_UDF_NAME)?
            .call(vec![arg]))
    }

    fn observe(_plan: &LogicalPlan, _rule: &dyn AnalyzerRule) {}

    fn analyze(plan: LogicalPlan) -> Result<LogicalPlan> {
        let analyzer = Analyzer::with_rules(vec![Arc::new(HandleGapFill)]);
        analyzer.execute_and_check(plan, &ConfigOptions::new(), observe)
    }

    fn assert_analyzer_err(plan: LogicalPlan, expected: &str) {
        match analyze(plan) {
            Ok(plan) => assert_eq!(format!("{}", plan.display_indent()), "an error"),
            Err(ref e) => {
                let actual = e.to_string();
                if expected.is_empty() || !actual.contains(expected) {
                    assert_eq!(actual, expected)
                }
            }
        }
    }

    fn assert_analyzer_skipped(plan: LogicalPlan) -> Result<()> {
        let new_plan = analyze(plan.clone())?;
        assert_eq!(
            format!("{}", plan.display_indent()),
            format!("{}", new_plan.display_indent())
        );
        Ok(())
    }

    fn format_analyzed_plan(plan: LogicalPlan) -> Result<Vec<String>> {
        let plan = analyze(plan)?.display_indent().to_string();
        Ok(plan.split('\n').map(|s| s.to_string()).collect())
    }

    #[test]
    fn misplaced_dbg_err() -> Result<()> {
        // date_bin_gapfill used in a filter should produce an error
        let scan = table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .filter(
                date_bin_gapfill(lit(ScalarValue::new_interval_dt(0, 600_000)), col("temp"))?
                    .gt(lit(100.0)),
            )?
            .build()?;
        assert_analyzer_err(
            plan,
            "Error during planning: date_bin_gapfill may only be used as a GROUP BY expression",
        );
        Ok(())
    }

    /// calling LOCF in a WHERE predicate is not valid
    #[test]
    fn misplaced_locf_err() -> Result<()> {
        // date_bin_gapfill used in a filter should produce an error
        let scan = table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .filter(locf(col("temp"))?.gt(lit(100.0)))?
            .build()?;
        assert_analyzer_err(
            plan,
            "Error during planning: locf may only be used in the SELECT list of a gap-filling query",
        );
        Ok(())
    }

    /// calling INTERPOLATE in a WHERE predicate is not valid
    #[test]
    fn misplaced_interpolate_err() -> Result<()> {
        // date_bin_gapfill used in a filter should produce an error
        let scan = table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .filter(interpolate(col("temp"))?.gt(lit(100.0)))?
            .build()?;
        assert_analyzer_err(
            plan,
            "Error during planning: interpolate may only be used in the SELECT list of a gap-filling query",
        );
        Ok(())
    }
    /// calling LOCF on the SELECT list but not on an aggregate column is not valid.
    #[test]
    fn misplaced_locf_non_agg_err() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![
                    col("loc"),
                    date_bin_gapfill(lit(ScalarValue::new_interval_dt(0, 60_000)), col("time"))?,
                ],
                vec![avg(col("temp")), min(col("temp"))],
            )?
            .project(vec![
                locf(col("loc"))?,
                col("date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)"),
                locf(col("avg(temps.temp)"))?,
                locf(col("min(temps.temp)"))?,
            ])?
            .build()?;
        assert_analyzer_err(
            plan,
            "locf must be called on an aggregate column in a gap-filling query",
        );
        Ok(())
    }

    #[test]
    fn different_fill_strategies_one_col() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![
                    col("loc"),
                    date_bin_gapfill(lit(ScalarValue::new_interval_dt(0, 60_000)), col("time"))?,
                ],
                vec![avg(col("temp")), min(col("temp"))],
            )?
            .project(vec![
                locf(col("loc"))?,
                col("date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)"),
                locf(col("avg(temps.temp)"))?,
                interpolate(col("avg(temps.temp)"))?,
            ])?
            .build()?;
        assert_analyzer_err(
            plan,
            "This feature is not implemented: multiple fill strategies for the same column",
        );
        Ok(())
    }

    #[test]
    fn nonscalar_origin() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill_with_origin(
                    lit(ScalarValue::new_interval_dt(0, 60_000)),
                    col("time"),
                    Some(col("time2")),
                )?],
                vec![avg(col("temp"))],
            )?
            .build()?;
        assert_analyzer_err(
            plan,
            "Error during planning: origin argument to DATE_BIN_GAPFILL for gap fill query must evaluate to a scalar",
        );
        Ok(())
    }

    #[test]
    fn nonscalar_stride() -> Result<()> {
        let stride = case(col("loc"))
            .when(lit("kitchen"), lit(ScalarValue::new_interval_dt(0, 60_000)))
            .otherwise(lit(ScalarValue::new_interval_dt(0, 30_000)))
            .unwrap();

        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(stride, col("time"))?],
                vec![avg(col("temp"))],
            )?
            .build()?;
        assert_analyzer_err(
            plan,
            "Error during planning: stride argument to DATE_BIN_GAPFILL for gap fill query must evaluate to a scalar",
        );
        Ok(())
    }

    #[test]
    fn time_range_errs() -> Result<()> {
        let cases = vec![
            (
                lit(true),
                "Error during planning: gap-filling query is missing both upper and lower time bounds",
            ),
            (
                col("time").gt_eq(lit_timestamptz_nano(1000)),
                "Error during planning: gap-filling query is missing upper time bound",
            ),
            (
                col("time").lt(lit_timestamptz_nano(2000)),
                "Error during planning: gap-filling query is missing lower time bound",
            ),
            (
                col("time")
                    .gt_eq(col("time2"))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
                "Error during planning: lower time bound for gap fill query must evaluate to a scalar",
            ),
            (
                col("time")
                    .gt_eq(lit_timestamptz_nano(2000))
                    .and(col("time").lt(col("time2"))),
                "Error during planning: upper time bound for gap fill query must evaluate to a scalar",
            ),
        ];
        for c in cases {
            let plan = LogicalPlanBuilder::from(table_scan()?)
                .filter(c.0)?
                .aggregate(
                    vec![date_bin_gapfill(
                        lit(ScalarValue::new_interval_dt(0, 60_000)),
                        col("time"),
                    )?],
                    vec![avg(col("temp"))],
                )?
                .build()?;
            assert_analyzer_err(plan, c.1);
        }
        Ok(())
    }

    #[test]
    fn no_change() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .aggregate(vec![col("loc")], vec![avg(col("temp"))])?
            .build()?;
        assert_analyzer_skipped(plan)?;
        Ok(())
    }

    #[test]
    fn date_bin_gapfill_simple() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(
                    lit(ScalarValue::new_interval_dt(0, 60_000)),
                    col("time"),
                )?],
                vec![avg(col("temp"))],
            )?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_analyzed_plan(plan)?,
            @r#"
        - "GapFill: series=[], time=date_bin(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)) AS date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time), fill=[avg(temps.temp)], range=Included(Literal(TimestampNanosecond(1000, None), None))..Excluded(Literal(TimestampNanosecond(2000, None), None))"
        - "  Aggregate: groupBy=[[date_bin(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)]], aggr=[[avg(temps.temp)]]"
        - "    Filter: temps.time >= TimestampNanosecond(1000, None) AND temps.time < TimestampNanosecond(2000, None)"
        - "      TableScan: temps"
        "#);
        Ok(())
    }

    #[test]
    fn date_bin_gapfill_origin() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill_with_origin(
                    lit(ScalarValue::new_interval_dt(0, 60_000)),
                    col("time"),
                    Some(lit_timestamptz_nano(7)),
                )?],
                vec![avg(col("temp"))],
            )?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_analyzed_plan(plan)?,
            @r#"
        - "GapFill: series=[], time=date_bin(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time,TimestampNanosecond(7, None)), TimestampNanosecond(7, None)) AS date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time,TimestampNanosecond(7, None)), fill=[avg(temps.temp)], range=Included(Literal(TimestampNanosecond(1000, None), None))..Excluded(Literal(TimestampNanosecond(2000, None), None))"
        - "  Aggregate: groupBy=[[date_bin(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), temps.time, TimestampNanosecond(7, None)) AS date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time,TimestampNanosecond(7, None))]], aggr=[[avg(temps.temp)]]"
        - "    Filter: temps.time >= TimestampNanosecond(1000, None) AND temps.time < TimestampNanosecond(2000, None)"
        - "      TableScan: temps"
        "#);
        Ok(())
    }

    #[test]
    fn two_group_exprs() -> Result<()> {
        // grouping by date_bin_gapfill(...), loc
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![
                    date_bin_gapfill(lit(ScalarValue::new_interval_dt(0, 60_000)), col("time"))?,
                    col("loc"),
                ],
                vec![avg(col("temp"))],
            )?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_analyzed_plan(plan)?,
            @r#"
        - "GapFill: series=[temps.loc], time=date_bin(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)) AS date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time), fill=[avg(temps.temp)], range=Included(Literal(TimestampNanosecond(1000, None), None))..Excluded(Literal(TimestampNanosecond(2000, None), None))"
        - "  Aggregate: groupBy=[[date_bin(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time), temps.loc]], aggr=[[avg(temps.temp)]]"
        - "    Filter: temps.time >= TimestampNanosecond(1000, None) AND temps.time < TimestampNanosecond(2000, None)"
        - "      TableScan: temps"
        "#);
        Ok(())
    }

    #[test]
    fn double_date_bin_gapfill() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .aggregate(
                vec![
                    date_bin_gapfill(lit(ScalarValue::new_interval_dt(0, 60_000)), col("time"))?,
                    date_bin_gapfill(lit(ScalarValue::new_interval_dt(0, 30_000)), col("time"))?,
                ],
                vec![avg(col("temp"))],
            )?
            .build()?;
        assert_analyzer_err(
            plan,
            "Error during planning: DATE_BIN_GAPFILL specified more than once",
        );
        Ok(())
    }

    #[test]
    fn with_projection() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(
                    lit(ScalarValue::new_interval_dt(0, 60_000)),
                    col("time"),
                )?],
                vec![avg(col("temp"))],
            )?
            .project(vec![
                col("date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)"),
                col("avg(temps.temp)"),
            ])?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_analyzed_plan(plan)?,
            @r#"
        - "Projection: date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time), avg(temps.temp)"
        - "  GapFill: series=[], time=date_bin(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)) AS date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time), fill=[avg(temps.temp)], range=Included(Literal(TimestampNanosecond(1000, None), None))..Excluded(Literal(TimestampNanosecond(2000, None), None))"
        - "    Aggregate: groupBy=[[date_bin(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)]], aggr=[[avg(temps.temp)]]"
        - "      Filter: temps.time >= TimestampNanosecond(1000, None) AND temps.time < TimestampNanosecond(2000, None)"
        - "        TableScan: temps"
        "#);
        Ok(())
    }

    #[test]
    fn with_locf() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(
                    lit(ScalarValue::new_interval_dt(0, 60_000)),
                    col("time"),
                )?],
                vec![avg(col("temp")), min(col("temp"))],
            )?
            .project(vec![
                col("date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)"),
                locf(col("avg(temps.temp)"))?,
                locf(col("min(temps.temp)"))?,
            ])?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_analyzed_plan(plan)?,
            @r#"
        - "Projection: date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time), avg(temps.temp) AS locf(avg(temps.temp)), min(temps.temp) AS locf(min(temps.temp))"
        - "  GapFill: series=[], time=date_bin(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)) AS date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time), fill=[LOCF(avg(temps.temp)), LOCF(min(temps.temp))], range=Included(Literal(TimestampNanosecond(1000, None), None))..Excluded(Literal(TimestampNanosecond(2000, None), None))"
        - "    Aggregate: groupBy=[[date_bin(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)]], aggr=[[avg(temps.temp), min(temps.temp)]]"
        - "      Filter: temps.time >= TimestampNanosecond(1000, None) AND temps.time < TimestampNanosecond(2000, None)"
        - "        TableScan: temps"
        "#);
        Ok(())
    }

    #[test]
    fn with_locf_aliased() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(
                    lit(ScalarValue::new_interval_dt(0, 60_000)),
                    col("time"),
                )?],
                vec![avg(col("temp")), min(col("temp"))],
            )?
            .project(vec![
                col("date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)"),
                locf(col("min(temps.temp)"))?.alias("locf_min_temp"),
            ])?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_analyzed_plan(plan)?,
            @r#"
        - "Projection: date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time), min(temps.temp) AS locf(min(temps.temp)) AS locf_min_temp"
        - "  GapFill: series=[], time=date_bin(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)) AS date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time), fill=[avg(temps.temp), LOCF(min(temps.temp))], range=Included(Literal(TimestampNanosecond(1000, None), None))..Excluded(Literal(TimestampNanosecond(2000, None), None))"
        - "    Aggregate: groupBy=[[date_bin(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)]], aggr=[[avg(temps.temp), min(temps.temp)]]"
        - "      Filter: temps.time >= TimestampNanosecond(1000, None) AND temps.time < TimestampNanosecond(2000, None)"
        - "        TableScan: temps"
        "#);
        Ok(())
    }

    #[test]
    fn with_interpolate() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(
                    lit(ScalarValue::new_interval_dt(0, 60_000)),
                    col("time"),
                )?],
                vec![avg(col("temp")), min(col("temp"))],
            )?
            .project(vec![
                col("date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)"),
                interpolate(col("avg(temps.temp)"))?,
                interpolate(col("min(temps.temp)"))?,
            ])?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_analyzed_plan(plan)?,
            @r#"
        - "Projection: date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time), avg(temps.temp) AS interpolate(avg(temps.temp)), min(temps.temp) AS interpolate(min(temps.temp))"
        - "  GapFill: series=[], time=date_bin(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)) AS date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time), fill=[INTERPOLATE(avg(temps.temp)), INTERPOLATE(min(temps.temp))], range=Included(Literal(TimestampNanosecond(1000, None), None))..Excluded(Literal(TimestampNanosecond(2000, None), None))"
        - "    Aggregate: groupBy=[[date_bin(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)]], aggr=[[avg(temps.temp), min(temps.temp)]]"
        - "      Filter: temps.time >= TimestampNanosecond(1000, None) AND temps.time < TimestampNanosecond(2000, None)"
        - "        TableScan: temps"
        "#);
        Ok(())
    }

    #[test]
    fn scan_filter_not_part_of_projection() {
        let schema = schema();
        let plan = table_scan_with_filters(
            Some("temps"),
            &schema,
            Some(vec![schema.index_of("time").unwrap()]),
            vec![
                col("temps.time").gt_eq(lit_timestamptz_nano(1000)),
                col("temps.time").lt(lit_timestamptz_nano(2000)),
                col("temps.loc").eq(lit("foo")),
            ],
        )
        .unwrap()
        .aggregate(
            vec![
                date_bin_gapfill(lit(ScalarValue::new_interval_dt(0, 60_000)), col("time"))
                    .unwrap(),
            ],
            std::iter::empty::<Expr>(),
        )
        .unwrap()
        .build()
        .unwrap();

        insta::assert_yaml_snapshot!(
            format_analyzed_plan(plan).unwrap(),
            @r#"
        - "GapFill: series=[], time=date_bin(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)) AS date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time), fill=[], range=Included(Literal(TimestampNanosecond(1000, None), None))..Excluded(Literal(TimestampNanosecond(2000, None), None))"
        - "  Aggregate: groupBy=[[date_bin(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"),temps.time)]], aggr=[[]]"
        - "    TableScan: temps projection=[time], full_filters=[temps.time >= TimestampNanosecond(1000, None), temps.time < TimestampNanosecond(2000, None), temps.loc = Utf8(\"foo\")]"
        "#);
    }
}
