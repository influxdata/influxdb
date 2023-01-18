//! An optimizer rule that transforms a plan
//! to fill gaps in time series data.

use crate::exec::gapfill::GapFill;
use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::{
        expr_rewriter::{ExprRewritable, ExprRewriter, RewriteRecursion},
        expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion},
        Aggregate, BuiltinScalarFunction, Extension, LogicalPlan, Sort,
    },
    optimizer::{optimizer::ApplyOrder, OptimizerConfig, OptimizerRule},
    prelude::{col, Expr},
};
use query_functions::gapfill::DATE_BIN_GAPFILL_UDF_NAME;
use std::sync::Arc;

// This optimizer rule enables gap-filling semantics for SQL queries
/// that contain calls to `DATE_BIN_GAPFILL()`.
///
/// In SQL a typical gap-filling query might look like this:
/// ```sql
/// SELECT
///   location,
///   DATE_BIN_GAPFILL(INTERVAL '1 minute', time, '1970-01-01T00:00:00Z') AS minute,
///   AVG(temp)
/// FROM temps
/// WHERE time > NOW() - INTERVAL '6 hours' AND time < NOW()
/// GROUP BY LOCATION, MINUTE
/// ```
/// The aggregation step of the initial logical plan looks like this:
/// ```text
///   Aggregate: groupBy=[[datebingapfill(IntervalDayTime("60000"), temps.time, TimestampNanosecond(0, None)))]], aggr=[[AVG(temps.temp)]]
/// ```
/// However, `DATE_BIN_GAPFILL()` does not have an actual implementation like other functions.
/// Instead, the plan is transformed to this:
/// ```text
/// GapFill: groupBy=[[datebingapfill(IntervalDayTime("60000"), temps.time, TimestampNanosecond(0, None)))]], aggr=[[AVG(temps.temp)]], start=..., stop=...
///   Sort: datebingapfill(IntervalDayTime("60000"), temps.time, TimestampNanosecond(0, None))
///     Aggregate: groupBy=[[datebingapfill(IntervalDayTime("60000"), temps.time, TimestampNanosecond(0, None)))]], aggr=[[AVG(temps.temp)]]
/// ```
/// This optimizer rule makes that transformation.
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
    let (new_group_expr, dbg_idx) = if let Some(v) = replace_date_bin_gapfill(group_expr)? {
        v
    } else {
        return Ok(None);
    };

    let orig_dbg_name = schema.fields()[dbg_idx].name();

    let new_aggr_plan = {
        let new_aggr_plan =
            Aggregate::try_new(Arc::clone(input), new_group_expr, aggr_expr.clone())?;
        let new_aggr_plan = LogicalPlan::Aggregate(new_aggr_plan);
        check_node(&new_aggr_plan)?;
        new_aggr_plan
    };

    let new_sort_plan = {
        let mut sort_exprs: Vec<_> = new_aggr_plan
            .schema()
            .fields()
            .iter()
            .take(group_expr.len())
            .map(|f| col(f.qualified_column()).sort(true, true))
            .collect();
        // ensure that date_bin_gapfill is the last sort expression.
        let last_elm = sort_exprs.len() - 1;
        sort_exprs.swap(dbg_idx, last_elm);

        LogicalPlan::Sort(Sort {
            expr: sort_exprs,
            input: Arc::new(new_aggr_plan),
            fetch: None,
        })
    };

    let new_gap_fill_plan = {
        let mut new_group_expr: Vec<_> = new_sort_plan
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let e = Expr::Column(f.qualified_column());
                if i == dbg_idx {
                    // Make the column name look the same as in the original
                    // Aggregate node.
                    Expr::Alias(Box::new(e), orig_dbg_name.to_string())
                } else {
                    e
                }
            })
            .collect();
        let aggr_expr = new_group_expr.split_off(group_expr.len());
        let time_column = col(new_sort_plan.schema().fields()[dbg_idx].qualified_column());
        LogicalPlan::Extension(Extension {
            node: Arc::new(GapFill::try_new(
                Arc::new(new_sort_plan),
                new_group_expr,
                aggr_expr,
                time_column,
            )?),
        })
    };
    Ok(Some(new_gap_fill_plan))
}

// Iterate over the group expression list.
// If it finds no occurrences of date_bin_gapfill at the top of
// each expression tree, it will return None.
// If it finds such an occurrence, it will return a new expression list
// with the date_bin_gapfill replaced with date_bin, and the index of
// where the replacement occurred.
fn replace_date_bin_gapfill(group_expr: &[Expr]) -> Result<Option<(Vec<Expr>, usize)>> {
    let mut date_bin_gapfill_count = 0;
    group_expr.iter().try_for_each(|e| -> Result<()> {
        let fn_cnt = count_date_bin_gapfill(e)?;
        date_bin_gapfill_count += fn_cnt;
        Ok(())
    })?;
    match date_bin_gapfill_count {
        0 => return Ok(None),
        2.. => {
            return Err(DataFusionError::Plan(
                "DATE_BIN_GAPFILL specified more than once".to_string(),
            ))
        }
        _ => (),
    }

    let group_expr = group_expr.to_owned();
    let mut new_group_expr = Vec::with_capacity(group_expr.len());
    let mut dbg_idx = None;

    group_expr
        .into_iter()
        .enumerate()
        .try_for_each(|(i, e)| -> Result<()> {
            let mut rewriter = DateBinGapfillRewriter { found: false };
            new_group_expr.push(e.rewrite(&mut rewriter)?);
            if rewriter.found {
                dbg_idx = Some(i);
            }
            Ok(())
        })?;
    Ok(Some((
        new_group_expr,
        dbg_idx.expect("should have found a call to DATE_BIN_GAPFILL based on previous check"),
    )))
}

struct DateBinGapfillRewriter {
    found: bool,
}

impl ExprRewriter for DateBinGapfillRewriter {
    fn pre_visit(&mut self, expr: &Expr) -> Result<RewriteRecursion> {
        match expr {
            Expr::ScalarUDF { fun, .. } if fun.name == DATE_BIN_GAPFILL_UDF_NAME => {
                Ok(RewriteRecursion::Mutate)
            }
            _ => Ok(RewriteRecursion::Continue),
        }
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        match expr {
            Expr::ScalarUDF { fun, args } if fun.name == DATE_BIN_GAPFILL_UDF_NAME => {
                self.found = true;
                Ok(Expr::ScalarFunction {
                    fun: BuiltinScalarFunction::DateBin,
                    args,
                })
            }
            _ => Ok(expr),
        }
    }
}

fn count_date_bin_gapfill(e: &Expr) -> Result<usize> {
    struct Finder {
        count: usize,
    }
    impl ExpressionVisitor for Finder {
        fn pre_visit(mut self, expr: &Expr) -> Result<Recursion<Self>> {
            match expr {
                Expr::ScalarUDF { fun, .. } if fun.name == DATE_BIN_GAPFILL_UDF_NAME => {
                    self.count += 1;
                }
                _ => (),
            };
            Ok(Recursion::Continue(self))
        }
    }
    let f = Finder { count: 0 };
    let f = e.accept(f)?;
    Ok(f.count)
}

fn check_node(node: &LogicalPlan) -> Result<()> {
    node.expressions().iter().try_for_each(|expr| {
        let count = count_date_bin_gapfill(expr)?;
        if count > 0 {
            Err(DataFusionError::Plan(format!(
                "{} may only be used as a GROUP BY expression",
                DATE_BIN_GAPFILL_UDF_NAME
            )))
        } else {
            Ok(())
        }
    })
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::HandleGapFill;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::error::Result;
    use datafusion::logical_expr::{logical_plan, LogicalPlan, LogicalPlanBuilder};
    use datafusion::optimizer::optimizer::Optimizer;
    use datafusion::optimizer::OptimizerContext;
    use datafusion::prelude::{avg, col, lit, lit_timestamp_nano, Expr};
    use datafusion::scalar::ScalarValue;
    use query_functions::gapfill::DATE_BIN_GAPFILL_UDF_NAME;

    fn table_scan() -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("loc", DataType::Utf8, false),
            Field::new("temp", DataType::Float64, false),
        ]);
        logical_plan::table_scan(Some("temps"), &schema, None)?.build()
    }

    fn date_bin_gapfill(interval: Expr, time: Expr) -> Result<Expr> {
        Ok(Expr::ScalarUDF {
            fun: query_functions::registry().udf(DATE_BIN_GAPFILL_UDF_NAME)?,
            args: vec![interval, time, lit_timestamp_nano(0)],
        })
    }

    fn optimize(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let optimizer = Optimizer::with_rules(vec![Arc::new(HandleGapFill::default())]);
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

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) -> Result<()> {
        let new_plan = optimize(plan)?;
        let new_lines = new_plan
            .expect("plan should have been optimized")
            .display_indent()
            .to_string();
        let new_lines = new_lines.split('\n');
        let expected_lines = expected.split('\n');
        // compare each line rather than the whole blob, to make it easier
        // to read output when tests fail.
        expected_lines
            .zip(new_lines)
            .for_each(|(expected, actual)| assert_eq!(expected, actual));
        Ok(())
    }

    #[test]
    fn misplaced_fns_err() -> Result<()> {
        // date_bin_gapfill used in a filter should produce an error
        let scan = table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .filter(
                date_bin_gapfill(
                    lit(ScalarValue::IntervalDayTime(Some(60_0000))),
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
            .aggregate(
                vec![date_bin_gapfill(
                    lit(ScalarValue::IntervalDayTime(Some(60_000))),
                    col("time"),
                )?],
                vec![avg(col("temp"))],
            )?
            .build()?;

        let dbg_args = "IntervalDayTime(\"60000\"),temps.time,TimestampNanosecond(0, None)";
        let expected = format!(
            "GapFill: groupBy=[[datebin({dbg_args}) AS date_bin_gapfill({dbg_args})]], aggr=[[AVG(temps.temp)]], time_column=datebin({dbg_args})\
           \n  Sort: datebin({dbg_args}) ASC NULLS FIRST\
           \n    Aggregate: groupBy=[[datebin(IntervalDayTime(\"60000\"), temps.time, TimestampNanosecond(0, None))]], aggr=[[AVG(temps.temp)]]\
           \n      TableScan: temps");
        assert_optimized_plan_eq(&plan, &expected)?;
        Ok(())
    }

    #[test]
    fn reordered_sort_exprs() -> Result<()> {
        // grouping by date_bin_gapfill(...), loc
        // but the sort node should have date_bin_gapfill last.
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .aggregate(
                vec![
                    date_bin_gapfill(lit(ScalarValue::IntervalDayTime(Some(60_000))), col("time"))?,
                    col("loc"),
                ],
                vec![avg(col("temp"))],
            )?
            .build()?;

        let dbg_args = "IntervalDayTime(\"60000\"),temps.time,TimestampNanosecond(0, None)";
        let expected = format!(
            "GapFill: groupBy=[[datebin({dbg_args}) AS date_bin_gapfill({dbg_args}), temps.loc]], aggr=[[AVG(temps.temp)]], time_column=datebin({dbg_args})\
           \n  Sort: temps.loc ASC NULLS FIRST, datebin({dbg_args}) ASC NULLS FIRST\
           \n    Aggregate: groupBy=[[datebin(IntervalDayTime(\"60000\"), temps.time, TimestampNanosecond(0, None)), temps.loc]], aggr=[[AVG(temps.temp)]]\
           \n      TableScan: temps");
        assert_optimized_plan_eq(&plan, &expected)?;
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
        let dbg_args = "IntervalDayTime(\"60000\"),temps.time,TimestampNanosecond(0, None)";
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .aggregate(
                vec![date_bin_gapfill(
                    lit(ScalarValue::IntervalDayTime(Some(60_000))),
                    col("time"),
                )?],
                vec![avg(col("temp"))],
            )?
            .project(vec![
                col(format!("date_bin_gapfill({dbg_args})")),
                col("AVG(temps.temp)"),
            ])?
            .build()?;

        let expected = format!(
            "Projection: date_bin_gapfill({dbg_args}), AVG(temps.temp)\
           \n  GapFill: groupBy=[[datebin({dbg_args}) AS date_bin_gapfill({dbg_args})]], aggr=[[AVG(temps.temp)]], time_column=datebin({dbg_args})\
           \n    Sort: datebin({dbg_args}) ASC NULLS FIRST\
           \n      Aggregate: groupBy=[[datebin(IntervalDayTime(\"60000\"), temps.time, TimestampNanosecond(0, None))]], aggr=[[AVG(temps.temp)]]\
           \n        TableScan: temps");
        assert_optimized_plan_eq(&plan, &expected)?;
        Ok(())
    }
}
