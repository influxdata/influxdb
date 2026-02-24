use std::{borrow::Borrow, fmt::Debug, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::{ColumnStatistics, Statistics, stats::Precision},
    physical_plan::{ExecutionPlan, ExecutionPlanProperties},
};
use itertools::Itertools;

pub(super) fn partition_count(plan: &dyn ExecutionPlan) -> usize {
    plan.output_partitioning().partition_count()
}

pub(super) fn children_partition_count(plan: &dyn ExecutionPlan) -> usize {
    plan.children()
        .iter()
        .map(|child| partition_count(child.as_ref()))
        .sum()
}

pub(super) fn pretty_fmt_fields(schema: &SchemaRef) -> String {
    schema.fields().iter().map(|field| field.name()).join(",")
}

pub(super) fn make_column_statistics_inexact(
    column_statistics: Vec<ColumnStatistics>,
) -> Vec<ColumnStatistics> {
    column_statistics
        .into_iter()
        .map(
            |ColumnStatistics {
                 null_count,
                 min_value,
                 max_value,
                 distinct_count,
                 sum_value,
             }| {
                ColumnStatistics {
                    null_count: make_inexact_or_keep_absent(null_count),
                    min_value: make_inexact_or_keep_absent(min_value),
                    max_value: make_inexact_or_keep_absent(max_value),
                    distinct_count: make_inexact_or_keep_absent(distinct_count),
                    sum_value: make_inexact_or_keep_absent(sum_value),
                }
            },
        )
        .collect_vec()
}

fn make_inexact_or_keep_absent<T: Debug + Clone + Eq + PartialOrd>(
    val: Precision<T>,
) -> Precision<T> {
    match val {
        Precision::Exact(val) => Precision::Inexact(val),
        _ => val,
    }
}

/// Merge a collection of [`Statistics`] into a single stat.
///
/// This takes statistics references, which may or may not be arc'ed.
pub(super) fn merge_stats_collection<T: Borrow<Statistics> + Clone + Into<Arc<Statistics>>>(
    mut stats: impl Iterator<Item = T>,
    target_schema: &SchemaRef,
) -> Statistics {
    let Some(start) = stats.next() else {
        // stats is empty
        return Statistics::new_unknown(target_schema);
    };
    stats.fold(start.borrow().clone(), |a, b| merge_stats(a, b.borrow()))
}

/// Merge together two [`Statistics`].
fn merge_stats(a: Statistics, b: &Statistics) -> Statistics {
    assert_eq!(
        a.column_statistics.len(),
        b.column_statistics.len(),
        "failed to merge statistics, due to different column schema"
    );

    Statistics {
        num_rows: a.num_rows.add(&b.num_rows),
        total_byte_size: a.total_byte_size.add(&b.total_byte_size),
        column_statistics: a
            .column_statistics
            .into_iter()
            .zip(b.column_statistics.iter())
            .map(|(a, b)| merge_col_stats(a, b))
            .collect_vec(),
    }
}

pub(crate) fn merge_col_stats(a: ColumnStatistics, b: &ColumnStatistics) -> ColumnStatistics {
    ColumnStatistics {
        null_count: a.null_count.add(&b.null_count),
        min_value: a.min_value.min(&b.min_value),
        max_value: a.max_value.max(&b.max_value),
        distinct_count: Precision::Absent,
        sum_value: Precision::Absent,
    }
}
