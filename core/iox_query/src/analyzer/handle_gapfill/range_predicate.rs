//! Find the time range from the filters in a logical plan.
use std::{
    ops::{Bound, Range},
    sync::Arc,
};

use arrow::datatypes::DataType;
use datafusion::{
    common::{
        DFSchema, internal_err,
        tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor},
    },
    error::Result,
    logical_expr::{
        Between, BinaryExpr, Cast, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Operator,
        expr::Alias, utils::split_conjunction,
    },
    prelude::{Column, Expr},
};

use super::unwrap_alias;

/// Given a plan and a column, finds the predicates that use that column
/// and return a range with expressions for upper and lower bounds.
pub fn find_time_range(plan: &LogicalPlan, time_col: &Column) -> Result<Range<Bound<Expr>>> {
    let mut v = TimeRangeVisitor {
        col: time_col.clone(),
        range: TimeRange::default(),
    };
    plan.visit(&mut v)?;
    Ok(v.range.0)
}

struct TimeRangeVisitor {
    col: Column,
    range: TimeRange,
}

impl TreeNodeVisitor<'_> for TimeRangeVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, plan: &LogicalPlan) -> Result<TreeNodeRecursion> {
        match plan {
            LogicalPlan::Projection(p) => {
                let idx = p.schema.index_of_column(&self.col)?;
                match unwrap_alias(&p.expr[idx]) {
                    Expr::Column(c) => {
                        self.col = c.clone();
                        Ok(TreeNodeRecursion::Continue)
                    }
                    _ => Ok(TreeNodeRecursion::Stop),
                }
            }
            LogicalPlan::Filter(f) => {
                let range = self.range.clone();
                let range = split_conjunction(&f.predicate)
                    .iter()
                    .try_fold(range, |range, expr| {
                        range.with_expr(f.input.schema().as_ref(), &self.col, expr)
                    })?;
                self.range = range;
                Ok(TreeNodeRecursion::Continue)
            }
            LogicalPlan::TableScan(t) => {
                let range = self.range.clone();

                // filters may use columns that are NOT part of a projection, so we need the underlying schema. Because
                // that's a bit of a mess in DF, we reconstruct the schema using the plan builder.
                let unprojected_scan = LogicalPlanBuilder::scan_with_filters(
                    t.table_name.to_owned(),
                    Arc::clone(&t.source),
                    None,
                    t.filters.clone(),
                )
                .map_err(|e| e.context("reconstruct unprojected schema"))?;
                let unprojected_schema = unprojected_scan.schema();
                let range = t
                    .filters
                    .iter()
                    .flat_map(split_conjunction)
                    .try_fold(range, |range, expr| {
                        range.with_expr(unprojected_schema, &self.col, expr)
                    })?;
                self.range = range;
                Ok(TreeNodeRecursion::Continue)
            }
            LogicalPlan::SubqueryAlias(_) => {
                // The nodes below this one refer to the column with a different table name,
                // just unset the relation so we match on the column name.
                self.col.relation = None;
                Ok(TreeNodeRecursion::Continue)
            }
            // These nodes do not alter their schema, so we can recurse through them
            LogicalPlan::Sort(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Distinct(_) => Ok(TreeNodeRecursion::Continue),
            // At some point we may wish to handle joins here too.
            _ => Ok(TreeNodeRecursion::Stop),
        }
    }
}

/// Encapsulates the upper and lower bounds of a time column
/// in a logical plan.
#[derive(Clone)]
struct TimeRange(pub Range<Bound<Expr>>);

impl Default for TimeRange {
    fn default() -> Self {
        Self(Range {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        })
    }
}

impl TimeRange {
    // If the given expression uses the given column with comparison operators, update
    // this time range to reflect that.
    fn with_expr(self, schema: &DFSchema, time_col: &Column, expr: &Expr) -> Result<Self> {
        Ok(match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if is_time_col(schema, time_col, left)? =>
            {
                let bound_expr = invert_time_col_casts(schema, time_col, left, *right.clone())?;
                match op {
                    Operator::Lt => self.with_upper(Bound::Excluded(bound_expr)),
                    Operator::LtEq => self.with_upper(Bound::Included(bound_expr)),
                    Operator::Gt => self.with_lower(Bound::Excluded(bound_expr)),
                    Operator::GtEq => self.with_lower(Bound::Included(bound_expr)),
                    _ => self,
                }
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if is_time_col(schema, time_col, right)? =>
            {
                let bound_expr = invert_time_col_casts(schema, time_col, right, *left.clone())?;
                match op {
                    Operator::Lt => self.with_lower(Bound::Excluded(bound_expr)),
                    Operator::LtEq => self.with_lower(Bound::Included(bound_expr)),
                    Operator::Gt => self.with_upper(Bound::Excluded(bound_expr)),
                    Operator::GtEq => self.with_upper(Bound::Included(bound_expr)),
                    _ => self,
                }
            }
            // Between bounds are inclusive
            Expr::Between(Between {
                expr,
                negated: false,
                low,
                high,
            }) if is_time_col(schema, time_col, expr)? => {
                let low = invert_time_col_casts(schema, time_col, expr, *low.clone())?;
                let high = invert_time_col_casts(schema, time_col, expr, *high.clone())?;
                self.with_lower(Bound::Included(low))
                    .with_upper(Bound::Included(high))
            }
            _ => self,
        })
    }

    fn with_lower(self, start: Bound<Expr>) -> Self {
        Self(Range {
            start,
            end: self.0.end,
        })
    }

    fn with_upper(self, end: Bound<Expr>) -> Self {
        Self(Range {
            start: self.0.start,
            end,
        })
    }
}

/// inverts casts on a time column expression and applies them to a time range bound expression
///
/// the `time_col_expr` is a reference to the gapfill time column corresponding to the `time_col` argument.
/// the `time_col_expr` can optionally be wrapped by `Cast`s to other timestamp types or `Alias` expressions
///
/// the `bound_expr` is an arbitrary expression that represents an upper or lower bound on the
/// gapfill time range
///
/// the associated `schema` is used to resolve types of the expressions
///
/// if the `time_col_expr` contains any nested `Cast` expressions, the direction of the cast will be
/// "inverted" and applied to the `bound_expr`
///
/// The return expression is a new `bound_expr` with all of the "inverted" casts applied to it.
fn invert_time_col_casts(
    schema: &DFSchema,
    time_col: &Column,
    time_col_expr: &Expr,
    bound_expr: Expr,
) -> Result<Expr> {
    Ok(match time_col_expr {
        // found a column expression - no cast needed so we return the bound expression
        Expr::Column(_) if is_time_col(schema, time_col, time_col_expr)? => bound_expr,
        // ignore aliase and recurse
        Expr::Alias(Alias {
            expr: inner_time_expr,
            ..
        }) => invert_time_col_casts(schema, time_col, inner_time_expr, bound_expr)?,
        // found a timestamp cast - apply the "inverse" cast to the bound_expr and recurse
        Expr::Cast(Cast {
            expr: inner_time_expr,
            data_type: DataType::Timestamp(..),
        }) => {
            let casted_bound_expr = Expr::Cast(Cast {
                expr: Box::new(bound_expr),
                data_type: inner_time_expr.get_type(schema)?,
            });
            invert_time_col_casts(schema, time_col, inner_time_expr, casted_bound_expr)?
        }
        _ => {
            return internal_err!(
                "Expected a time column expression with optional nested timestamp casts or aliases"
            );
        }
    })
}

/// Returns true if the given `expr` is a reference to the `time_col`, optionally wrapped by any
/// number of `Alias` or timestamp `Cast` expressions
fn is_time_col(schema: &DFSchema, time_col: &Column, expr: &Expr) -> Result<bool> {
    match expr {
        Expr::Alias(Alias { expr: e, .. }) => is_time_col(schema, time_col, e),
        Expr::Cast(Cast {
            expr: e,
            data_type: DataType::Timestamp(..),
        }) => is_time_col(schema, time_col, e),
        Expr::Column(col) => Ok(schema.index_of_column(col)? == schema.index_of_column(time_col)?),
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ops::{Bound, Range},
        sync::Arc,
    };

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::{
        error::Result,
        logical_expr::{
            Between, LogicalPlan, LogicalPlanBuilder, cast,
            logical_plan::{self, builder::LogicalTableSource},
        },
        prelude::{Column, Expr, Partitioning, col, lit},
        scalar::ScalarValue,
        sql::TableReference,
    };
    use datafusion_util::lit_timestamptz_nano;

    use super::find_time_range;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("temp", DataType::Float64, false),
        ])
    }

    fn table_scan() -> Result<LogicalPlan> {
        let schema = schema();
        logical_plan::table_scan(Some("t"), &schema, None)?.build()
    }

    fn simple_filter_plan(pred: Expr, inline_filter: bool) -> Result<LogicalPlan> {
        let schema = schema();
        let table_source = Arc::new(LogicalTableSource::new(Arc::new(schema)));
        let name = TableReference::from("t").to_quoted_string();
        if inline_filter {
            LogicalPlanBuilder::scan_with_filters(name, table_source, None, vec![pred])?.build()
        } else {
            LogicalPlanBuilder::scan(name, table_source, None)?
                .filter(pred)?
                .build()
        }
    }

    fn between(expr: Expr, low: Expr, high: Expr) -> Expr {
        Expr::Between(Between {
            expr: Box::new(expr),
            negated: false,
            low: Box::new(low),
            high: Box::new(high),
        })
    }

    fn lit_timestamptz_nano_with_tz(t: i64, tz: impl Into<Arc<str>>) -> Expr {
        lit(ScalarValue::TimestampNanosecond(Some(t), Some(tz.into())))
    }

    fn timestamp_nano_type_with_tz(tz: impl Into<Arc<str>>) -> DataType {
        DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.into()))
    }

    #[test]
    fn test_find_range() -> Result<()> {
        let time_col = Column::from_name("time");
        let time_col_type = schema().field_with_name("time")?.data_type().clone();

        let cases = vec![
            (
                "unbounded",
                lit(true),
                Range {
                    start: Bound::Unbounded,
                    end: Bound::Unbounded,
                },
            ),
            (
                "time_gt_val",
                col("time").gt(lit_timestamptz_nano(1000)),
                Range {
                    start: Bound::Excluded(lit_timestamptz_nano(1000)),
                    end: Bound::Unbounded,
                },
            ),
            (
                "time_gt_eq_val",
                col("time").gt_eq(lit_timestamptz_nano(1000)),
                Range {
                    start: Bound::Included(lit_timestamptz_nano(1000)),
                    end: Bound::Unbounded,
                },
            ),
            (
                "time_lt_val",
                col("time").lt(lit_timestamptz_nano(1000)),
                Range {
                    start: Bound::Unbounded,
                    end: Bound::Excluded(lit_timestamptz_nano(1000)),
                },
            ),
            (
                "time_lt_eq_val",
                col("time").lt_eq(lit_timestamptz_nano(1000)),
                Range {
                    start: Bound::Unbounded,
                    end: Bound::Included(lit_timestamptz_nano(1000)),
                },
            ),
            (
                "val_gt_time",
                lit_timestamptz_nano(1000).gt(col("time")),
                Range {
                    start: Bound::Unbounded,
                    end: Bound::Excluded(lit_timestamptz_nano(1000)),
                },
            ),
            (
                "val_gt_eq_time",
                lit_timestamptz_nano(1000).gt_eq(col("time")),
                Range {
                    start: Bound::Unbounded,
                    end: Bound::Included(lit_timestamptz_nano(1000)),
                },
            ),
            (
                "val_lt_time",
                lit_timestamptz_nano(1000).lt(col("time")),
                Range {
                    start: Bound::Excluded(lit_timestamptz_nano(1000)),
                    end: Bound::Unbounded,
                },
            ),
            (
                "val_lt_eq_time",
                lit_timestamptz_nano(1000).lt_eq(col("time")),
                Range {
                    start: Bound::Included(lit_timestamptz_nano(1000)),
                    end: Bound::Unbounded,
                },
            ),
            (
                "and",
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
                Range {
                    start: Bound::Included(lit_timestamptz_nano(1000)),
                    end: Bound::Excluded(lit_timestamptz_nano(2000)),
                },
            ),
            (
                "between",
                between(
                    col("time"),
                    lit_timestamptz_nano(1000),
                    lit_timestamptz_nano(2000),
                ),
                Range {
                    start: Bound::Included(lit_timestamptz_nano(1000)),
                    end: Bound::Included(lit_timestamptz_nano(2000)),
                },
            ),
            (
                "time_col_alias",
                col("time")
                    .alias("time_alias")
                    .gt_eq(lit_timestamptz_nano(1000)),
                Range {
                    start: Bound::Included(lit_timestamptz_nano(1000)),
                    end: Bound::Unbounded,
                },
            ),
            (
                "time_col_cast",
                cast(col("time"), timestamp_nano_type_with_tz("+01:00"))
                    .gt_eq(lit_timestamptz_nano_with_tz(1000, "+01:00")),
                Range {
                    start: Bound::Included(cast(
                        lit_timestamptz_nano_with_tz(1000, "+01:00"),
                        time_col_type.clone(),
                    )),
                    end: Bound::Unbounded,
                },
            ),
            (
                "time_col_alias_cast",
                cast(col("time"), timestamp_nano_type_with_tz("+01:00"))
                    .alias("time_alias")
                    .gt_eq(lit_timestamptz_nano(1000)),
                Range {
                    start: Bound::Included(cast(
                        lit_timestamptz_nano_with_tz(1000, "+01:00"),
                        time_col_type.clone(),
                    )),
                    end: Bound::Unbounded,
                },
            ),
            (
                "time_col_double_cast",
                cast(
                    cast(col("time"), timestamp_nano_type_with_tz("UTC")),
                    timestamp_nano_type_with_tz("+01:00"),
                )
                .alias("time_alias")
                .gt_eq(lit_timestamptz_nano_with_tz(1000, "+01:00")),
                Range {
                    start: Bound::Included(cast(
                        cast(
                            lit_timestamptz_nano_with_tz(1000, "+01:00"),
                            timestamp_nano_type_with_tz("UTC"),
                        ),
                        time_col_type.clone(),
                    )),
                    end: Bound::Unbounded,
                },
            ),
            (
                "time_col_cast_and",
                cast(col("time"), timestamp_nano_type_with_tz("+01:00"))
                    .gt_eq(lit_timestamptz_nano_with_tz(1000, "+01:00"))
                    .and(col("time").lt(lit_timestamptz_nano_with_tz(2000, "+01:00"))),
                Range {
                    start: Bound::Included(cast(
                        lit_timestamptz_nano_with_tz(1000, "+01:00"),
                        time_col_type.clone(),
                    )),
                    end: Bound::Excluded(lit_timestamptz_nano_with_tz(2000, "+01:00")),
                },
            ),
            (
                "time_col_cast_between",
                between(
                    cast(col("time"), timestamp_nano_type_with_tz("+01:00")),
                    lit_timestamptz_nano_with_tz(1000, "+01:00"),
                    lit_timestamptz_nano_with_tz(2000, "+01:00"),
                ),
                Range {
                    start: Bound::Included(cast(
                        lit_timestamptz_nano_with_tz(1000, "+01:00"),
                        time_col_type.clone(),
                    )),
                    end: Bound::Included(cast(
                        lit_timestamptz_nano_with_tz(2000, "+01:00"),
                        time_col_type.clone(),
                    )),
                },
            ),
        ];
        for (name, pred, expected) in cases {
            for inline_filter in [false, true] {
                let plan = simple_filter_plan(pred.clone(), inline_filter)?;
                let actual = find_time_range(&plan, &time_col)?;
                assert_eq!(
                    expected, actual,
                    "test case `{name}` with inline_filter={inline_filter} failed",
                );
            }
        }
        Ok(())
    }

    #[test]
    fn plan_traversal() -> Result<()> {
        // Show that the time range can be found
        // - through nodes that don't alter their schema
        // - even when predicates are in different filter nodes
        // - through projections that alias columns
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(col("time").gt_eq(lit_timestamptz_nano(1000)))?
            .sort(vec![col("time").sort(true, true)])?
            .limit(0, Some(10))?
            .project(vec![col("time").alias("other_time")])?
            .filter(col("other_time").lt(lit_timestamptz_nano(2000)))?
            .distinct()?
            .repartition(Partitioning::RoundRobinBatch(1))?
            .project(vec![col("other_time").alias("my_time")])?
            .build()?;
        let time_col = Column::from_name("my_time");
        let actual = find_time_range(&plan, &time_col)?;
        let expected = Range {
            start: Bound::Included(lit_timestamptz_nano(1000)),
            end: Bound::Excluded(lit_timestamptz_nano(2000)),
        };
        assert_eq!(expected, actual);
        Ok(())
    }
}
