//! An optimizer rule that transforms a plan
//! to fill gaps in time series data.

pub mod range_predicate;

use crate::exec::gapfill::{FillStrategy, GapFill, GapFillParams};
use datafusion::{
    common::tree_node::{RewriteRecursion, TreeNode, TreeNodeRewriter, VisitRecursion},
    error::{DataFusionError, Result},
    logical_expr::{
        expr::{Alias, ScalarFunction, ScalarUDF},
        utils::expr_to_columns,
        Aggregate, BuiltinScalarFunction, Extension, LogicalPlan, Projection,
    },
    optimizer::{optimizer::ApplyOrder, OptimizerConfig, OptimizerRule},
    prelude::{col, Expr},
};
use hashbrown::{hash_map, HashMap};
use query_functions::gapfill::{DATE_BIN_GAPFILL_UDF_NAME, INTERPOLATE_UDF_NAME, LOCF_UDF_NAME};
use std::{
    collections::HashSet,
    ops::{Bound, Range},
    sync::Arc,
};

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

impl OptimizerRule for HandleGapFill {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        handle_gap_fill(plan)
    }

    fn name(&self) -> &str {
        "handle_gap_fill"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}

fn handle_gap_fill(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let res = match plan {
        LogicalPlan::Aggregate(aggr) => handle_aggregate(aggr)?,
        LogicalPlan::Projection(proj) => handle_projection(proj)?,
        _ => None,
    };

    if res.is_none() {
        // no transformation was applied,
        // so make sure the plan is not using gap filling
        // functions in an unsupported way.
        check_node(plan)?;
    }

    Ok(res)
}

fn handle_aggregate(aggr: &Aggregate) -> Result<Option<LogicalPlan>> {
    let Aggregate {
        input,
        group_expr,
        aggr_expr,
        schema,
        ..
    } = aggr;

    // new_group_expr has DATE_BIN_GAPFILL replaced with DATE_BIN.
    let RewriteInfo {
        new_group_expr,
        date_bin_gapfill_index,
        date_bin_gapfill_args,
    } = if let Some(v) = replace_date_bin_gapfill(group_expr)? {
        v
    } else {
        return Ok(None);
    };

    let new_aggr_plan = {
        // Create the aggregate node with the same output schema as the orignal
        // one. This means that there will be an output column called `date_bin_gapfill(...)`
        // even though the actual expression populating that column will be `date_bin(...)`.
        // This seems acceptable since it avoids having to deal with renaming downstream.
        let new_aggr_plan = Aggregate::try_new_with_schema(
            Arc::clone(input),
            new_group_expr,
            aggr_expr.clone(),
            Arc::clone(schema),
        )?;
        let new_aggr_plan = LogicalPlan::Aggregate(new_aggr_plan);
        check_node(&new_aggr_plan)?;
        new_aggr_plan
    };

    let new_gap_fill_plan =
        build_gapfill_node(new_aggr_plan, date_bin_gapfill_index, date_bin_gapfill_args)?;
    Ok(Some(new_gap_fill_plan))
}

fn build_gapfill_node(
    new_aggr_plan: LogicalPlan,
    date_bin_gapfill_index: usize,
    date_bin_gapfill_args: Vec<Expr>,
) -> Result<LogicalPlan> {
    match date_bin_gapfill_args.len() {
        2 | 3 => (),
        nargs => {
            return Err(DataFusionError::Plan(format!(
                "DATE_BIN_GAPFILL expects 2 or 3 arguments, got {nargs}",
            )));
        }
    }

    let mut args_iter = date_bin_gapfill_args.into_iter();

    // Ensure that stride argument is a scalar
    let stride = args_iter.next().unwrap();
    validate_scalar_expr("stride argument to DATE_BIN_GAPFILL", &stride)?;

    // Ensure that the source argument is a column
    let time_col = args_iter.next().unwrap().try_into_col().map_err(|_| {
        DataFusionError::Plan(
            "DATE_BIN_GAPFILL requires a column as the source argument".to_string(),
        )
    })?;

    // Ensure that a time range was specified and is valid for gap filling
    let time_range = range_predicate::find_time_range(new_aggr_plan.inputs()[0], &time_col)?;
    validate_time_range(&time_range)?;

    // Ensure that origin argument is a scalar
    let origin = args_iter.next();
    if let Some(ref origin) = origin {
        validate_scalar_expr("origin argument to DATE_BIN_GAPFILL", origin)?;
    }

    // Make sure the time output to the gapfill node matches what the
    // aggregate output was.
    let time_column =
        col(new_aggr_plan.schema().fields()[date_bin_gapfill_index].qualified_column());

    let LogicalPlan::Aggregate(aggr) = &new_aggr_plan else {
        return Err(DataFusionError::Internal(format!("Expected Aggregate plan, got {}", new_aggr_plan.display())));
    };
    let mut new_group_expr: Vec<_> = aggr
        .schema
        .fields()
        .iter()
        .map(|f| Expr::Column(f.qualified_column()))
        .collect();
    let aggr_expr = new_group_expr.split_off(aggr.group_expr.len());

    let fill_behavior = aggr_expr
        .iter()
        .cloned()
        .map(|e| (e, FillStrategy::Null))
        .collect();

    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(GapFill::try_new(
            Arc::new(new_aggr_plan),
            new_group_expr,
            aggr_expr,
            GapFillParams {
                stride,
                time_column,
                origin,
                time_range,
                fill_strategy: fill_behavior,
            },
        )?),
    }))
}

fn validate_time_range(range: &Range<Bound<Expr>>) -> Result<()> {
    let Range { ref start, ref end } = range;
    let (start, end) = match (start, end) {
        (Bound::Unbounded, Bound::Unbounded) => {
            return Err(DataFusionError::Plan(
                "gap-filling query is missing both upper and lower time bounds".to_string(),
            ))
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

struct RewriteInfo {
    // Group expressions with DATE_BIN_GAPFILL rewritten to DATE_BIN.
    new_group_expr: Vec<Expr>,
    // The index of the group expression that contained the call to DATE_BIN_GAPFILL.
    date_bin_gapfill_index: usize,
    // The arguments to the call to DATE_BIN_GAPFILL.
    date_bin_gapfill_args: Vec<Expr>,
}

// Iterate over the group expression list.
// If it finds no occurrences of date_bin_gapfill, it will return None.
// If it finds more than one occurrence it will return an error.
// Otherwise it will return a RewriteInfo for the optimizer rule to use.
fn replace_date_bin_gapfill(group_expr: &[Expr]) -> Result<Option<RewriteInfo>> {
    let mut date_bin_gapfill_count = 0;
    let mut dbg_idx = None;
    group_expr
        .iter()
        .enumerate()
        .try_for_each(|(i, e)| -> Result<()> {
            let fn_cnt = count_udf(e, DATE_BIN_GAPFILL_UDF_NAME)?;
            date_bin_gapfill_count += fn_cnt;
            if fn_cnt > 0 {
                dbg_idx = Some(i);
            }
            Ok(())
        })?;
    match date_bin_gapfill_count {
        0 => return Ok(None),
        1 => {
            // Make sure that the call to DATE_BIN_GAPFILL is root expression
            // excluding aliases.
            let dbg_idx = dbg_idx.expect("should have found exactly one call");
            if !matches_udf(
                unwrap_alias(&group_expr[dbg_idx]),
                DATE_BIN_GAPFILL_UDF_NAME,
            ) {
                return Err(DataFusionError::Plan(
                    "DATE_BIN_GAPFILL must a top-level expression in the GROUP BY clause when gap filling. It cannot be part of another expression or cast".to_string(),
                ));
            }
        }
        _ => {
            return Err(DataFusionError::Plan(
                "DATE_BIN_GAPFILL specified more than once".to_string(),
            ))
        }
    }

    let date_bin_gapfill_index = dbg_idx.expect("should be found exactly one call");

    let mut rewriter = DateBinGapfillRewriter { args: None };
    let group_expr = group_expr
        .iter()
        .enumerate()
        .map(|(i, e)| {
            if i == date_bin_gapfill_index {
                e.clone().rewrite(&mut rewriter)
            } else {
                Ok(e.clone())
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let date_bin_gapfill_args = rewriter.args.expect("should have found args");

    Ok(Some(RewriteInfo {
        new_group_expr: group_expr,
        date_bin_gapfill_index,
        date_bin_gapfill_args,
    }))
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
}

impl TreeNodeRewriter for DateBinGapfillRewriter {
    type N = Expr;
    fn pre_visit(&mut self, expr: &Expr) -> Result<RewriteRecursion> {
        match expr {
            Expr::ScalarUDF(ScalarUDF { fun, .. }) if fun.name == DATE_BIN_GAPFILL_UDF_NAME => {
                Ok(RewriteRecursion::Mutate)
            }
            _ => Ok(RewriteRecursion::Continue),
        }
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        // We need to preserve the name of the original expression
        // so that everything stays wired up.
        let orig_name = expr.display_name()?;
        match expr {
            Expr::ScalarUDF(ScalarUDF { fun, args }) if fun.name == DATE_BIN_GAPFILL_UDF_NAME => {
                self.args = Some(args.clone());
                Ok(Expr::ScalarFunction(ScalarFunction {
                    fun: BuiltinScalarFunction::DateBin,
                    args,
                })
                .alias(orig_name))
            }
            _ => Ok(expr),
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

fn fill_strategy_to_udf(fs: &FillStrategy) -> Result<&'static str> {
    match fs {
        FillStrategy::PrevNullAsMissing => Ok(LOCF_UDF_NAME),
        FillStrategy::LinearInterpolate => Ok(INTERPOLATE_UDF_NAME),
        _ => Err(DataFusionError::Internal(format!(
            "unknown UDF for fill strategy {fs:?}"
        ))),
    }
}

fn handle_projection(proj: &Projection) -> Result<Option<LogicalPlan>> {
    let Projection {
        input,
        expr: proj_exprs,
        schema: proj_schema,
        ..
    } = proj;
    let Some(child_gapfill) = (match input.as_ref() {
        LogicalPlan::Extension(Extension { node }) => node.as_any().downcast_ref::<GapFill>(),
        _ => None,
    }) else {
        // If this is not a projection that is a parent to a GapFill node,
        // then there is nothing to do.
        return Ok(None)
    };

    let mut fill_fn_rewriter = FillFnRewriter {
        aggr_col_fill_map: HashMap::new(),
    };
    let new_proj_exprs = proj_exprs
        .iter()
        .map(|e| e.clone().rewrite(&mut fill_fn_rewriter))
        .collect::<Result<Vec<Expr>>>()?;

    let FillFnRewriter { aggr_col_fill_map } = fill_fn_rewriter;
    if aggr_col_fill_map.is_empty() {
        return Ok(None);
    }

    // Clone the existing GapFill node, then modify it in place
    // to reflect the new fill strategy.
    let mut new_gapfill = child_gapfill.clone();
    for (e, fs) in aggr_col_fill_map {
        let udf = fill_strategy_to_udf(&fs)?;
        if new_gapfill.replace_fill_strategy(&e, fs).is_none() {
            // There was a gap filling function called on a non-aggregate column.
            return Err(DataFusionError::Plan(format!(
                "{udf} must be called on an aggregate column in a gap-filling query",
            )));
        }
    }

    let new_proj = {
        let mut proj = proj.clone();
        proj.expr = new_proj_exprs;
        proj.input = Arc::new(LogicalPlan::Extension(Extension {
            node: Arc::new(new_gapfill),
        }));
        proj.schema = Arc::clone(proj_schema);
        LogicalPlan::Projection(proj)
    };

    Ok(Some(new_proj))
}

/// Implements `TreeNodeRewriter`:
/// - Traverses over the expressions in a projection node
/// - If it finds `locf(col)` or `interpolate(col)`,
///   it replaces them with `col AS <original name>`
/// - Collects into [`Self::aggr_col_fill_map`] which correlates
///   aggregate columns to their [`FillStrategy`].
struct FillFnRewriter {
    aggr_col_fill_map: HashMap<Expr, FillStrategy>,
}

impl TreeNodeRewriter for FillFnRewriter {
    type N = Expr;
    fn pre_visit(&mut self, expr: &Expr) -> Result<RewriteRecursion> {
        match expr {
            Expr::ScalarUDF(ScalarUDF { fun, .. }) if udf_to_fill_strategy(&fun.name).is_some() => {
                Ok(RewriteRecursion::Mutate)
            }
            _ => Ok(RewriteRecursion::Continue),
        }
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        let orig_name = expr.display_name()?;
        match expr {
            Expr::ScalarUDF(ScalarUDF { ref fun, .. })
                if udf_to_fill_strategy(&fun.name).is_none() =>
            {
                Ok(expr)
            }
            Expr::ScalarUDF(ScalarUDF { fun, mut args }) => {
                let fs = udf_to_fill_strategy(&fun.name).expect("must be a fill fn");
                let arg = args.remove(0);
                self.add_fill_strategy(arg.clone(), fs)?;
                Ok(arg.alias(orig_name))
            }
            _ => Ok(expr),
        }
    }
}

impl FillFnRewriter {
    fn add_fill_strategy(&mut self, e: Expr, fs: FillStrategy) -> Result<()> {
        match self.aggr_col_fill_map.entry(e) {
            hash_map::Entry::Occupied(_) => Err(DataFusionError::NotImplemented(
                "multiple fill strategies for the same column".to_string(),
            )),
            hash_map::Entry::Vacant(ve) => {
                ve.insert(fs);
                Ok(())
            }
        }
    }
}

fn count_udf(e: &Expr, name: &str) -> Result<usize> {
    let mut count = 0;
    e.apply(&mut |expr| {
        if matches_udf(expr, name) {
            count += 1;
        }
        Ok(VisitRecursion::Continue)
    })?;
    Ok(count)
}

fn matches_udf(e: &Expr, name: &str) -> bool {
    matches!(
        e,
        Expr::ScalarUDF(ScalarUDF { fun, .. }) if fun.name == name
    )
}

fn check_node(node: &LogicalPlan) -> Result<()> {
    node.expressions().iter().try_for_each(|expr| {
        let dbg_count = count_udf(expr, DATE_BIN_GAPFILL_UDF_NAME)?;
        if dbg_count > 0 {
            return Err(DataFusionError::Plan(format!(
                "{DATE_BIN_GAPFILL_UDF_NAME} may only be used as a GROUP BY expression"
            )));
        }

        for fn_name in [LOCF_UDF_NAME, INTERPOLATE_UDF_NAME] {
            if count_udf(expr, fn_name)? > 0 {
                return Err(DataFusionError::Plan(format!(
                    "{fn_name} may only be used in the SELECT list of a gap-filling query"
                )));
            }
        }
        Ok(())
    })
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::HandleGapFill;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::error::Result;
    use datafusion::logical_expr::expr::ScalarUDF;
    use datafusion::logical_expr::{logical_plan, LogicalPlan, LogicalPlanBuilder};
    use datafusion::optimizer::optimizer::Optimizer;
    use datafusion::optimizer::OptimizerContext;
    use datafusion::prelude::{avg, case, col, lit, lit_timestamp_nano, min, Expr};
    use datafusion::scalar::ScalarValue;
    use query_functions::gapfill::{
        DATE_BIN_GAPFILL_UDF_NAME, INTERPOLATE_UDF_NAME, LOCF_UDF_NAME,
    };

    fn table_scan() -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
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
        ]);
        logical_plan::table_scan(Some("temps"), &schema, None)?.build()
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
        Ok(Expr::ScalarUDF(ScalarUDF {
            fun: query_functions::registry().udf(DATE_BIN_GAPFILL_UDF_NAME)?,
            args,
        }))
    }

    fn locf(arg: Expr) -> Result<Expr> {
        Ok(Expr::ScalarUDF(ScalarUDF {
            fun: query_functions::registry().udf(LOCF_UDF_NAME)?,
            args: vec![arg],
        }))
    }

    fn interpolate(arg: Expr) -> Result<Expr> {
        Ok(Expr::ScalarUDF(ScalarUDF {
            fun: query_functions::registry().udf(INTERPOLATE_UDF_NAME)?,
            args: vec![arg],
        }))
    }

    fn optimize(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let optimizer = Optimizer::with_rules(vec![Arc::new(HandleGapFill)]);
        optimizer.optimize_recursively(
            optimizer.rules.get(0).unwrap(),
            plan,
            &OptimizerContext::new(),
        )
    }

    fn assert_optimizer_err(plan: &LogicalPlan, expected: &str) {
        match optimize(plan) {
            Ok(plan) => assert_eq!(format!("{}", plan.unwrap().display_indent()), "an error"),
            Err(ref e) => {
                let actual = e.to_string();
                if expected.is_empty() || !actual.contains(expected) {
                    assert_eq!(actual, expected)
                }
            }
        }
    }

    fn assert_optimization_skipped(plan: &LogicalPlan) -> Result<()> {
        let new_plan = optimize(plan)?;
        if new_plan.is_none() {
            return Ok(());
        }
        assert_eq!(
            format!("{}", plan.display_indent()),
            format!("{}", new_plan.unwrap().display_indent())
        );
        Ok(())
    }

    fn format_optimized_plan(plan: &LogicalPlan) -> Result<Vec<String>> {
        let plan = optimize(plan)?
            .expect("plan should have been optimized")
            .display_indent()
            .to_string();
        Ok(plan.split('\n').map(|s| s.to_string()).collect())
    }

    #[test]
    fn misplaced_dbg_err() -> Result<()> {
        // date_bin_gapfill used in a filter should produce an error
        let scan = table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .filter(
                date_bin_gapfill(
                    lit(ScalarValue::IntervalDayTime(Some(600_000))),
                    col("temp"),
                )?
                .gt(lit(100.0)),
            )?
            .build()?;
        assert_optimizer_err(
            &plan,
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
        assert_optimizer_err(
            &plan,
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
        assert_optimizer_err(
            &plan,
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
                    .gt_eq(lit_timestamp_nano(1000))
                    .and(col("time").lt(lit_timestamp_nano(2000))),
            )?
            .aggregate(
                vec![
                    col("loc"),
                    date_bin_gapfill(lit(ScalarValue::IntervalDayTime(Some(60_000))), col("time"))?,
                ],
                vec![avg(col("temp")), min(col("temp"))],
            )?
            .project(vec![
                locf(col("loc"))?,
                col("date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)"),
                locf(col("AVG(temps.temp)"))?,
                locf(col("MIN(temps.temp)"))?,
            ])?
            .build()?;
        assert_optimizer_err(
            &plan,
            "locf must be called on an aggregate column in a gap-filling query",
        );
        Ok(())
    }

    #[test]
    fn different_fill_strategies_one_col() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamp_nano(1000))
                    .and(col("time").lt(lit_timestamp_nano(2000))),
            )?
            .aggregate(
                vec![
                    col("loc"),
                    date_bin_gapfill(lit(ScalarValue::IntervalDayTime(Some(60_000))), col("time"))?,
                ],
                vec![avg(col("temp")), min(col("temp"))],
            )?
            .project(vec![
                locf(col("loc"))?,
                col("date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)"),
                locf(col("AVG(temps.temp)"))?,
                interpolate(col("AVG(temps.temp)"))?,
            ])?
            .build()?;
        assert_optimizer_err(
            &plan,
            "This feature is not implemented: multiple fill strategies for the same column",
        );
        Ok(())
    }

    #[test]
    fn nonscalar_origin() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamp_nano(1000))
                    .and(col("time").lt(lit_timestamp_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill_with_origin(
                    lit(ScalarValue::IntervalDayTime(Some(60_000))),
                    col("time"),
                    Some(col("time2")),
                )?],
                vec![avg(col("temp"))],
            )?
            .build()?;
        assert_optimizer_err(
            &plan,
            "Error during planning: origin argument to DATE_BIN_GAPFILL for gap fill query must evaluate to a scalar",
        );
        Ok(())
    }

    #[test]
    fn nonscalar_stride() -> Result<()> {
        let stride = case(col("loc"))
            .when(
                lit("kitchen"),
                lit(ScalarValue::IntervalDayTime(Some(60_000))),
            )
            .otherwise(lit(ScalarValue::IntervalDayTime(Some(30_000))))
            .unwrap();

        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamp_nano(1000))
                    .and(col("time").lt(lit_timestamp_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(stride, col("time"))?],
                vec![avg(col("temp"))],
            )?
            .build()?;
        assert_optimizer_err(
            &plan,
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
                col("time").gt_eq(lit_timestamp_nano(1000)),
                "Error during planning: gap-filling query is missing upper time bound",
            ),
            (
                col("time").lt(lit_timestamp_nano(2000)),
                "Error during planning: gap-filling query is missing lower time bound",
            ),
            (
                col("time").gt_eq(col("time2")).and(
                    col("time").lt(lit_timestamp_nano(2000))),
                "Error during planning: lower time bound for gap fill query must evaluate to a scalar",
            ),
            (
                col("time").gt_eq(lit_timestamp_nano(2000)).and(
                    col("time").lt(col("time2"))),
                "Error during planning: upper time bound for gap fill query must evaluate to a scalar",
            )
        ];
        for c in cases {
            let plan = LogicalPlanBuilder::from(table_scan()?)
                .filter(c.0)?
                .aggregate(
                    vec![date_bin_gapfill(
                        lit(ScalarValue::IntervalDayTime(Some(60_000))),
                        col("time"),
                    )?],
                    vec![avg(col("temp"))],
                )?
                .build()?;
            assert_optimizer_err(&plan, c.1);
        }
        Ok(())
    }

    #[test]
    fn no_change() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .aggregate(vec![col("loc")], vec![avg(col("temp"))])?
            .build()?;
        assert_optimization_skipped(&plan)?;
        Ok(())
    }

    #[test]
    fn date_bin_gapfill_simple() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamp_nano(1000))
                    .and(col("time").lt(lit_timestamp_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(
                    lit(ScalarValue::IntervalDayTime(Some(60_000))),
                    col("time"),
                )?],
                vec![avg(col("temp"))],
            )?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_optimized_plan(&plan)?,
            @r###"
        ---
        - "GapFill: groupBy=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)], aggr=[[AVG(temps.temp)]], time_column=date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), stride=IntervalDayTime(\"60000\"), range=Included(Literal(TimestampNanosecond(1000, None)))..Excluded(Literal(TimestampNanosecond(2000, None)))"
        - "  Aggregate: groupBy=[[date_bin(IntervalDayTime(\"60000\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)]], aggr=[[AVG(temps.temp)]]"
        - "    Filter: temps.time >= TimestampNanosecond(1000, None) AND temps.time < TimestampNanosecond(2000, None)"
        - "      TableScan: temps"
        "###);
        Ok(())
    }

    #[test]
    fn date_bin_gapfill_origin() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamp_nano(1000))
                    .and(col("time").lt(lit_timestamp_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill_with_origin(
                    lit(ScalarValue::IntervalDayTime(Some(60_000))),
                    col("time"),
                    Some(lit_timestamp_nano(7)),
                )?],
                vec![avg(col("temp"))],
            )?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_optimized_plan(&plan)?,
            @r###"
        ---
        - "GapFill: groupBy=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,TimestampNanosecond(7, None))], aggr=[[AVG(temps.temp)]], time_column=date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,TimestampNanosecond(7, None)), stride=IntervalDayTime(\"60000\"), range=Included(Literal(TimestampNanosecond(1000, None)))..Excluded(Literal(TimestampNanosecond(2000, None)))"
        - "  Aggregate: groupBy=[[date_bin(IntervalDayTime(\"60000\"), temps.time, TimestampNanosecond(7, None)) AS date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,TimestampNanosecond(7, None))]], aggr=[[AVG(temps.temp)]]"
        - "    Filter: temps.time >= TimestampNanosecond(1000, None) AND temps.time < TimestampNanosecond(2000, None)"
        - "      TableScan: temps"
        "###);
        Ok(())
    }
    #[test]
    fn two_group_exprs() -> Result<()> {
        // grouping by date_bin_gapfill(...), loc
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamp_nano(1000))
                    .and(col("time").lt(lit_timestamp_nano(2000))),
            )?
            .aggregate(
                vec![
                    date_bin_gapfill(lit(ScalarValue::IntervalDayTime(Some(60_000))), col("time"))?,
                    col("loc"),
                ],
                vec![avg(col("temp"))],
            )?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_optimized_plan(&plan)?,
            @r###"
        ---
        - "GapFill: groupBy=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), temps.loc], aggr=[[AVG(temps.temp)]], time_column=date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), stride=IntervalDayTime(\"60000\"), range=Included(Literal(TimestampNanosecond(1000, None)))..Excluded(Literal(TimestampNanosecond(2000, None)))"
        - "  Aggregate: groupBy=[[date_bin(IntervalDayTime(\"60000\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), temps.loc]], aggr=[[AVG(temps.temp)]]"
        - "    Filter: temps.time >= TimestampNanosecond(1000, None) AND temps.time < TimestampNanosecond(2000, None)"
        - "      TableScan: temps"
        "###);
        Ok(())
    }

    #[test]
    fn double_date_bin_gapfill() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .aggregate(
                vec![
                    date_bin_gapfill(lit(ScalarValue::IntervalDayTime(Some(60_000))), col("time"))?,
                    date_bin_gapfill(lit(ScalarValue::IntervalDayTime(Some(30_000))), col("time"))?,
                ],
                vec![avg(col("temp"))],
            )?
            .build()?;
        assert_optimizer_err(
            &plan,
            "Error during planning: DATE_BIN_GAPFILL specified more than once",
        );
        Ok(())
    }

    #[test]
    fn with_projection() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamp_nano(1000))
                    .and(col("time").lt(lit_timestamp_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(
                    lit(ScalarValue::IntervalDayTime(Some(60_000))),
                    col("time"),
                )?],
                vec![avg(col("temp"))],
            )?
            .project(vec![
                col("date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)"),
                col("AVG(temps.temp)"),
            ])?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_optimized_plan(&plan)?,
            @r###"
        ---
        - "Projection: date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), AVG(temps.temp)"
        - "  GapFill: groupBy=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)], aggr=[[AVG(temps.temp)]], time_column=date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), stride=IntervalDayTime(\"60000\"), range=Included(Literal(TimestampNanosecond(1000, None)))..Excluded(Literal(TimestampNanosecond(2000, None)))"
        - "    Aggregate: groupBy=[[date_bin(IntervalDayTime(\"60000\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)]], aggr=[[AVG(temps.temp)]]"
        - "      Filter: temps.time >= TimestampNanosecond(1000, None) AND temps.time < TimestampNanosecond(2000, None)"
        - "        TableScan: temps"
        "###);
        Ok(())
    }

    #[test]
    fn with_locf() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamp_nano(1000))
                    .and(col("time").lt(lit_timestamp_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(
                    lit(ScalarValue::IntervalDayTime(Some(60_000))),
                    col("time"),
                )?],
                vec![avg(col("temp")), min(col("temp"))],
            )?
            .project(vec![
                col("date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)"),
                locf(col("AVG(temps.temp)"))?,
                locf(col("MIN(temps.temp)"))?,
            ])?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_optimized_plan(&plan)?,
            @r###"
        ---
        - "Projection: date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), AVG(temps.temp) AS locf(AVG(temps.temp)), MIN(temps.temp) AS locf(MIN(temps.temp))"
        - "  GapFill: groupBy=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)], aggr=[[LOCF(AVG(temps.temp)), LOCF(MIN(temps.temp))]], time_column=date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), stride=IntervalDayTime(\"60000\"), range=Included(Literal(TimestampNanosecond(1000, None)))..Excluded(Literal(TimestampNanosecond(2000, None)))"
        - "    Aggregate: groupBy=[[date_bin(IntervalDayTime(\"60000\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)]], aggr=[[AVG(temps.temp), MIN(temps.temp)]]"
        - "      Filter: temps.time >= TimestampNanosecond(1000, None) AND temps.time < TimestampNanosecond(2000, None)"
        - "        TableScan: temps"
        "###);
        Ok(())
    }

    #[test]
    fn with_locf_aliased() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamp_nano(1000))
                    .and(col("time").lt(lit_timestamp_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(
                    lit(ScalarValue::IntervalDayTime(Some(60_000))),
                    col("time"),
                )?],
                vec![avg(col("temp")), min(col("temp"))],
            )?
            .project(vec![
                col("date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)"),
                locf(col("MIN(temps.temp)"))?.alias("locf_min_temp"),
            ])?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_optimized_plan(&plan)?,
            @r###"
        ---
        - "Projection: date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), MIN(temps.temp) AS locf(MIN(temps.temp)) AS locf_min_temp"
        - "  GapFill: groupBy=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)], aggr=[[AVG(temps.temp), LOCF(MIN(temps.temp))]], time_column=date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), stride=IntervalDayTime(\"60000\"), range=Included(Literal(TimestampNanosecond(1000, None)))..Excluded(Literal(TimestampNanosecond(2000, None)))"
        - "    Aggregate: groupBy=[[date_bin(IntervalDayTime(\"60000\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)]], aggr=[[AVG(temps.temp), MIN(temps.temp)]]"
        - "      Filter: temps.time >= TimestampNanosecond(1000, None) AND temps.time < TimestampNanosecond(2000, None)"
        - "        TableScan: temps"
        "###);
        Ok(())
    }

    #[test]
    fn with_interpolate() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamp_nano(1000))
                    .and(col("time").lt(lit_timestamp_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(
                    lit(ScalarValue::IntervalDayTime(Some(60_000))),
                    col("time"),
                )?],
                vec![avg(col("temp")), min(col("temp"))],
            )?
            .project(vec![
                col("date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)"),
                interpolate(col("AVG(temps.temp)"))?,
                interpolate(col("MIN(temps.temp)"))?,
            ])?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_optimized_plan(&plan)?,
            @r###"
        ---
        - "Projection: date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), AVG(temps.temp) AS interpolate(AVG(temps.temp)), MIN(temps.temp) AS interpolate(MIN(temps.temp))"
        - "  GapFill: groupBy=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)], aggr=[[INTERPOLATE(AVG(temps.temp)), INTERPOLATE(MIN(temps.temp))]], time_column=date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), stride=IntervalDayTime(\"60000\"), range=Included(Literal(TimestampNanosecond(1000, None)))..Excluded(Literal(TimestampNanosecond(2000, None)))"
        - "    Aggregate: groupBy=[[date_bin(IntervalDayTime(\"60000\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)]], aggr=[[AVG(temps.temp), MIN(temps.temp)]]"
        - "      Filter: temps.time >= TimestampNanosecond(1000, None) AND temps.time < TimestampNanosecond(2000, None)"
        - "        TableScan: temps"
        "###);
        Ok(())
    }
}
