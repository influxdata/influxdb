//! Find the time range from the filters in a logical plan.
use std::ops::{Bound, Range};

use datafusion::{
    common::{
        tree_node::{TreeNode, TreeNodeVisitor, VisitRecursion},
        DFSchema,
    },
    error::Result,
    logical_expr::{Between, BinaryExpr, LogicalPlan, Operator},
    optimizer::utils::split_conjunction,
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

impl TreeNodeVisitor for TimeRangeVisitor {
    type N = LogicalPlan;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<VisitRecursion> {
        match plan {
            LogicalPlan::Projection(p) => {
                let idx = p.schema.index_of_column(&self.col)?;
                match unwrap_alias(&p.expr[idx]) {
                    Expr::Column(ref c) => {
                        self.col = c.clone();
                        Ok(VisitRecursion::Continue)
                    }
                    _ => Ok(VisitRecursion::Stop),
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
                Ok(VisitRecursion::Continue)
            }
            LogicalPlan::TableScan(t) => {
                let range = self.range.clone();
                let range = t
                    .filters
                    .iter()
                    .flat_map(split_conjunction)
                    .try_fold(range, |range, expr| {
                        range.with_expr(&t.projected_schema, &self.col, expr)
                    })?;
                self.range = range;
                Ok(VisitRecursion::Continue)
            }
            LogicalPlan::SubqueryAlias(_) => {
                // The nodes below this one refer to the column with a different table name,
                // just unset the relation so we match on the column name.
                self.col.relation = None;
                Ok(VisitRecursion::Continue)
            }
            // These nodes do not alter their schema, so we can recurse through them
            LogicalPlan::Sort(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Distinct(_) => Ok(VisitRecursion::Continue),
            // At some point we may wish to handle joins here too.
            _ => Ok(VisitRecursion::Stop),
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
        let is_time_col = |e| -> Result<bool> {
            match Expr::try_into_col(e) {
                Ok(col) => Ok(schema.index_of_column(&col)? == schema.index_of_column(time_col)?),
                Err(_) => Ok(false),
            }
        };

        Ok(match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_time_col(left)? => match op {
                Operator::Lt => self.with_upper(Bound::Excluded(*right.clone())),
                Operator::LtEq => self.with_upper(Bound::Included(*right.clone())),
                Operator::Gt => self.with_lower(Bound::Excluded(*right.clone())),
                Operator::GtEq => self.with_lower(Bound::Included(*right.clone())),
                _ => self,
            },
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_time_col(right)? => match op {
                Operator::Lt => self.with_lower(Bound::Excluded(*left.clone())),
                Operator::LtEq => self.with_lower(Bound::Included(*left.clone())),
                Operator::Gt => self.with_upper(Bound::Excluded(*left.clone())),
                Operator::GtEq => self.with_upper(Bound::Included(*left.clone())),
                _ => self,
            },
            // Between bounds are inclusive
            Expr::Between(Between {
                expr,
                negated: false,
                low,
                high,
            }) if is_time_col(expr)? => self
                .with_lower(Bound::Included(*low.clone()))
                .with_upper(Bound::Included(*high.clone())),
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
            logical_plan::{self, builder::LogicalTableSource},
            Between, LogicalPlan, LogicalPlanBuilder,
        },
        prelude::{col, lit, lit_timestamp_nano, Column, Expr, Partitioning},
        sql::TableReference,
    };

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

    #[test]
    fn test_find_range() -> Result<()> {
        let time_col = Column::from_name("time");

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
                col("time").gt(lit_timestamp_nano(1000)),
                Range {
                    start: Bound::Excluded(lit_timestamp_nano(1000)),
                    end: Bound::Unbounded,
                },
            ),
            (
                "time_gt_eq_val",
                col("time").gt_eq(lit_timestamp_nano(1000)),
                Range {
                    start: Bound::Included(lit_timestamp_nano(1000)),
                    end: Bound::Unbounded,
                },
            ),
            (
                "time_lt_val",
                col("time").lt(lit_timestamp_nano(1000)),
                Range {
                    start: Bound::Unbounded,
                    end: Bound::Excluded(lit_timestamp_nano(1000)),
                },
            ),
            (
                "time_lt_eq_val",
                col("time").lt_eq(lit_timestamp_nano(1000)),
                Range {
                    start: Bound::Unbounded,
                    end: Bound::Included(lit_timestamp_nano(1000)),
                },
            ),
            (
                "val_gt_time",
                lit_timestamp_nano(1000).gt(col("time")),
                Range {
                    start: Bound::Unbounded,
                    end: Bound::Excluded(lit_timestamp_nano(1000)),
                },
            ),
            (
                "val_gt_eq_time",
                lit_timestamp_nano(1000).gt_eq(col("time")),
                Range {
                    start: Bound::Unbounded,
                    end: Bound::Included(lit_timestamp_nano(1000)),
                },
            ),
            (
                "val_lt_time",
                lit_timestamp_nano(1000).lt(col("time")),
                Range {
                    start: Bound::Excluded(lit_timestamp_nano(1000)),
                    end: Bound::Unbounded,
                },
            ),
            (
                "val_lt_eq_time",
                lit_timestamp_nano(1000).lt_eq(col("time")),
                Range {
                    start: Bound::Included(lit_timestamp_nano(1000)),
                    end: Bound::Unbounded,
                },
            ),
            (
                "and",
                col("time")
                    .gt_eq(lit_timestamp_nano(1000))
                    .and(col("time").lt(lit_timestamp_nano(2000))),
                Range {
                    start: Bound::Included(lit_timestamp_nano(1000)),
                    end: Bound::Excluded(lit_timestamp_nano(2000)),
                },
            ),
            (
                "between",
                between(
                    col("time"),
                    lit_timestamp_nano(1000),
                    lit_timestamp_nano(2000),
                ),
                Range {
                    start: Bound::Included(lit_timestamp_nano(1000)),
                    end: Bound::Included(lit_timestamp_nano(2000)),
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
            .filter(col("time").gt_eq(lit_timestamp_nano(1000)))?
            .sort(vec![col("time")])?
            .limit(0, Some(10))?
            .project(vec![col("time").alias("other_time")])?
            .filter(col("other_time").lt(lit_timestamp_nano(2000)))?
            .distinct()?
            .repartition(Partitioning::RoundRobinBatch(1))?
            .project(vec![col("other_time").alias("my_time")])?
            .build()?;
        let time_col = Column::from_name("my_time");
        let actual = find_time_range(&plan, &time_col)?;
        let expected = Range {
            start: Bound::Included(lit_timestamp_nano(1000)),
            end: Bound::Excluded(lit_timestamp_nano(2000)),
        };
        assert_eq!(expected, actual);
        Ok(())
    }
}
