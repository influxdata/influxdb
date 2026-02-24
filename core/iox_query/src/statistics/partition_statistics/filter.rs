use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::{ColumnStatistics, Statistics, stats::Precision},
    physical_expr::{AnalysisContext, ExprBoundaries, analyze, intervals::utils::check_support},
    physical_plan::PhysicalExpr,
};

/// Calculates [`Statistics`] by applying selectivity (either default, or estimated) to input statistics.
///
/// This is a (slightly) modified from a private function in datafusion. Refer to:
/// <https://github.com/apache/datafusion/blob/07a310fea8d287805d1490e9dc3c1b7b1c2775d8/datafusion/physical-plan/src/filter.rs#L175-L216>
pub(super) fn apply_filter(
    input_stats: Statistics,
    input_schema: &SchemaRef,
    predicate: &Arc<dyn PhysicalExpr>,
    default_selectivity: u8,
) -> datafusion::common::Result<Statistics> {
    if !check_support(predicate, input_schema) {
        let selectivity = default_selectivity as f64 / 100.0;
        let mut stats = input_stats.to_inexact();
        stats.num_rows = stats.num_rows.with_estimated_selectivity(selectivity);
        stats.total_byte_size = stats
            .total_byte_size
            .with_estimated_selectivity(selectivity);
        return Ok(stats);
    }

    let num_rows = input_stats.num_rows;
    let total_byte_size = input_stats.total_byte_size;
    let input_analysis_ctx =
        AnalysisContext::try_from_statistics(input_schema, &input_stats.column_statistics)?;

    let analysis_ctx = analyze(predicate, input_analysis_ctx, input_schema)?;

    // Estimate (inexact) selectivity of predicate
    let selectivity = analysis_ctx.selectivity.unwrap_or(1.0);
    let num_rows = num_rows.with_estimated_selectivity(selectivity);
    let total_byte_size = total_byte_size.with_estimated_selectivity(selectivity);

    let column_statistics =
        collect_new_statistics(&input_stats.column_statistics, analysis_ctx.boundaries);
    Ok(Statistics {
        num_rows,
        total_byte_size,
        column_statistics,
    })
}

/// This function ensures that all bounds in the `ExprBoundaries` vector are
/// converted to closed bounds. If a lower/upper bound is initially open, it
/// is adjusted by using the next/previous value for its data type to convert
/// it into a closed bound.
///
/// This is a (slightly) modified from a private function in datafusion. Refer to:
/// <https://github.com/apache/datafusion/blob/07a310fea8d287805d1490e9dc3c1b7b1c2775d8/datafusion/physical-plan/src/filter.rs#L498-L544>
fn collect_new_statistics(
    input_column_stats: &[ColumnStatistics],
    analysis_boundaries: Vec<ExprBoundaries>,
) -> Vec<ColumnStatistics> {
    analysis_boundaries
        .into_iter()
        .enumerate()
        .map(
            |(
                idx,
                ExprBoundaries {
                    interval,
                    distinct_count,
                    ..
                },
            )| {
                let (min_value, max_value) = if let Some(interval) = interval {
                    let (lower, upper) = interval.into_bounds();
                    if lower.eq(&upper) {
                        (Precision::Exact(lower), Precision::Exact(upper))
                    } else {
                        (Precision::Inexact(lower), Precision::Inexact(upper))
                    }
                } else {
                    (Precision::Absent, Precision::Absent)
                };

                ColumnStatistics {
                    null_count: input_column_stats[idx].null_count.to_inexact(),
                    max_value,
                    min_value,
                    distinct_count: distinct_count.to_inexact(),
                    sum_value: Precision::Absent,
                }
            },
        )
        .collect()
}
