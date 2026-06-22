use arrow::compute::rank;
use arrow::datatypes::Field;
use datafusion::common::ColumnStatistics;
use datafusion::common::stats::Precision;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::scalar::ScalarValue;

/// `ColumnStatistics` for a column absent from a chunk but present in the
/// outer schema. Min/max use a typed null derived from `field.data_type()`
/// so synthesized bounds match those produced for the same column on
/// chunks that have it. Falls back to untyped `ScalarValue::Null` for
/// Arrow types that `ScalarValue::try_from` does not handle.
///
/// See <https://github.com/influxdata/EAR/issues/6894>.
pub(crate) fn synthesize_absent_column_stats(
    field: &Field,
    null_count: Precision<usize>,
) -> ColumnStatistics {
    let typed_null = ScalarValue::try_from(field.data_type()).unwrap_or(ScalarValue::Null);
    ColumnStatistics {
        null_count,
        max_value: Precision::Exact(typed_null.clone()),
        min_value: Precision::Exact(typed_null),
        distinct_count: Precision::Absent,
        sum_value: Precision::Absent,
    }
}

/// Return min max of a ColumnStatistics with precise values
pub fn column_statistics_min_max(
    column_statistics: ColumnStatistics,
) -> Option<(ScalarValue, ScalarValue)> {
    match (column_statistics.min_value, column_statistics.max_value) {
        (Precision::Exact(min), Precision::Exact(max)) => Some((min, max)),
        // the statistics values are absent or imprecise
        _ => None,
    }
}

/// Return true if at least 2 min_max ranges in the given array overlap
pub fn overlap(value_ranges: &[(ScalarValue, ScalarValue)]) -> Result<bool, DataFusionError> {
    // interleave min and max into one iterator
    let value_ranges_iter = value_ranges.iter().flat_map(|(min, max)| {
        // panics if min > max
        if min > max {
            panic!("min ({min:?}) > max ({max:?})");
        }
        vec![min.clone(), max.clone()]
    });

    let value_ranges = ScalarValue::iter_to_array(value_ranges_iter)?;

    // rank it
    let ranks = rank(&*value_ranges, None)?;

    // check overlap by checking if the max is rank right behind its corresponding min
    //  . non-overlap example: values of min-max pairs [3, 5,   9, 12,   1, 1,   6, 8]
    //     ranks:  [3, 4,   7, 8,  2, 2,  5, 6] : max (even index) = its correspnding min (odd index) for same min max OR min + 1
    //  . overlap example:  [3, 5,   9, 12,   1, 1,   4, 6] : pair [3, 5] interleaves with pair [4, 6]
    //     ranks:  [3, 5,   7, 8,  2, 2,  4, 6]
    for i in (0..ranks.len()).step_by(2) {
        if !((ranks[i] == ranks[i + 1]) || (ranks[i + 1] == ranks[i] + 1)) {
            return Ok(true);
        }
    }

    Ok(false)
}

pub(crate) fn projection_includes_pure_columns(projection: &ProjectionExec) -> bool {
    projection.expr().iter().all(|projection_expr| {
        projection_expr
            .expr
            .as_any()
            .downcast_ref::<Column>()
            .is_some()
    })
}
