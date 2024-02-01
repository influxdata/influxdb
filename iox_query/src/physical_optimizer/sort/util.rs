use std::sync::Arc;

use crate::statistics::{column_statistics_min_max, compute_stats_column_min_max, overlap};
use arrow::compute::{rank, SortOptions};
use datafusion::{error::Result, physical_plan::ExecutionPlan, scalar::ScalarValue};
use observability_deps::tracing::trace;

/// Compute statistics for the given plans on a given column name
/// Return none if the statistics are not available
pub(crate) fn collect_statistics_min_max(
    plans: &[Arc<dyn ExecutionPlan>],
    col_name: &str,
) -> Result<Option<Vec<(ScalarValue, ScalarValue)>>> {
    // temp solution while waiting for DF's statistics to get mature
    // Compute min max stats for all inputs of UnionExec on the sorted column
    // https://github.com/apache/arrow-datafusion/issues/8078
    let col_stats = plans
        .iter()
        .map(|plan| compute_stats_column_min_max(&**plan, col_name))
        .collect::<Result<Vec<_>>>()?;

    // If min and max not available, return none
    let mut value_ranges = Vec::with_capacity(col_stats.len());
    for stats in &col_stats {
        let Some((min, max)) = column_statistics_min_max(stats) else {
            trace!("-------- min_max not available");
            return Ok(None);
        };

        value_ranges.push((min, max));
    }

    // todo: use this when DF satistics is ready
    // // Get statistics for the inputs of UnionExec on the sorted column
    // let Some(value_ranges) = statistics_min_max(plans, col_name)
    // else {
    //     return Ok(None);
    // };

    Ok(Some(value_ranges))
}

/// Plans and their corresponding value ranges
pub(crate) struct PlansValueRanges {
    pub plans: Vec<Arc<dyn ExecutionPlan>>,
    // Min and max values of the plan on a specific column
    pub value_ranges: Vec<(ScalarValue, ScalarValue)>,
}

/// Sort the given plans by value ranges
/// Return none if
///    . the number of plans is not the same as the number of value ranges
///    . the value ranges overlap
pub(crate) fn sort_by_value_ranges(
    plans: Vec<Arc<dyn ExecutionPlan>>,
    value_ranges: Vec<(ScalarValue, ScalarValue)>,
    sort_options: SortOptions,
) -> Result<Option<PlansValueRanges>> {
    if plans.len() != value_ranges.len() {
        trace!(
            plans.len = plans.len(),
            value_ranges.len = value_ranges.len(),
            "--------- number of plans is not the same as the number of value ranges"
        );
        return Ok(None);
    }

    if overlap(&value_ranges)? {
        trace!("--------- value ranges overlap");
        return Ok(None);
    }

    // get the min value of each value range
    let min_iter = value_ranges.iter().map(|(min, _)| min.clone());
    let mins = ScalarValue::iter_to_array(min_iter)?;

    // rank the min values
    let ranks = rank(&*mins, Some(sort_options))?;

    // sort the plans by the ranks of their min values
    let mut plan_rank_zip: Vec<(Arc<dyn ExecutionPlan>, u32)> =
        plans.into_iter().zip(ranks.clone()).collect::<Vec<_>>();
    plan_rank_zip.sort_by(|(_, min1), (_, min2)| min1.cmp(min2));
    let plans = plan_rank_zip
        .into_iter()
        .map(|(plan, _)| plan)
        .collect::<Vec<_>>();

    // Sort the value ranges by the ranks of their min values
    let mut value_range_rank_zip: Vec<((ScalarValue, ScalarValue), u32)> =
        value_ranges.into_iter().zip(ranks).collect::<Vec<_>>();
    value_range_rank_zip.sort_by(|(_, min1), (_, min2)| min1.cmp(min2));
    let value_ranges = value_range_rank_zip
        .into_iter()
        .map(|(value_range, _)| value_range)
        .collect::<Vec<_>>();

    Ok(Some(PlansValueRanges {
        plans,
        value_ranges,
    }))
}
