//! Build gap-filled output arrays
use arrow::{
    array::{ArrayRef, TimestampNanosecondArray, UInt64Array},
    compute::kernels::take,
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};

use super::{algo::GapFillParams, series::Series};
use datafusion::error::{DataFusionError, Result};
use std::sync::Arc;

/// Build a [RecordBatch] from the given slice of [Series].
///
/// # Arguments
///
/// * `schema` - the output schema
/// * `params` - the stride, first and last timestamps for the series
/// * `input_time_array` - the array of timestamps from the input.
///         The first item in the tuple is its offset in the array
///         of output columns.
/// * `group_arr` - Arrays of group columns from the input.
///         The first item in each tuple is the array's offset in the schema.
/// * `aggr_arr` - Arrays of aggregate columns from the input.
///         The first item in each tuple is the array's offset in the schema.
/// * `series_batch` - The [Series] that will be written to the output batch.
pub(super) fn build_output(
    schema: SchemaRef,
    params: &GapFillParams,
    input_time_array: (usize, &TimestampNanosecondArray),
    group_arr: &[(usize, ArrayRef)],
    aggr_arr: &[(usize, ArrayRef)],
    series_batch: &[Series],
) -> Result<RecordBatch> {
    let mut output_arrays: Vec<(usize, ArrayRef)> =
        Vec::with_capacity(group_arr.len() + aggr_arr.len() + 1); // plus one for time column
    let total_rows = series_batch
        .iter()
        .map(|sm| sm.total_output_row_count(params))
        .sum();

    // build the time column
    let (time_idx, input_time_array) = input_time_array;
    let mut time_vec: Vec<Option<i64>> = Vec::with_capacity(total_rows);
    for series in series_batch {
        series.append_time_values(params, &mut time_vec);
    }
    if time_vec.len() != total_rows {
        return Err(DataFusionError::Internal(format!(
            "gapfill time column has {} rows, expected {}",
            time_vec.len(),
            total_rows
        )));
    }
    output_arrays.push((time_idx, Arc::new(TimestampNanosecondArray::from(time_vec))));

    // build the other group columns
    for (idx, ga) in group_arr.iter() {
        let mut take_vec: Vec<u64> = Vec::with_capacity(total_rows);
        for series in series_batch {
            series.append_group_take(params, &mut take_vec);
        }
        if take_vec.len() != total_rows {
            return Err(DataFusionError::Internal(format!(
                "gapfill group column has {} rows, expected {}",
                take_vec.len(),
                total_rows
            )));
        }
        let take_arr = UInt64Array::from(take_vec);
        output_arrays.push((*idx, take::take(ga, &take_arr, None)?))
    }

    // Build the aggregate columns
    for (idx, aa) in aggr_arr.iter() {
        let mut take_vec: Vec<Option<u64>> = Vec::with_capacity(total_rows);
        for series in series_batch {
            series.append_aggr_take(params, input_time_array, &mut take_vec);
        }
        if take_vec.len() != total_rows {
            return Err(DataFusionError::Internal(format!(
                "gapfill aggregate column has {} rows, expected {}",
                take_vec.len(),
                total_rows
            )));
        }
        let take_arr = UInt64Array::from(take_vec);
        output_arrays.push((*idx, take::take(aa, &take_arr, None)?))
    }

    output_arrays.sort_by(|(a, _), (b, _)| a.cmp(b));
    let output_arrays: Vec<_> = output_arrays.into_iter().map(|(_, arr)| arr).collect();
    RecordBatch::try_new(Arc::clone(&schema), output_arrays).map_err(DataFusionError::ArrowError)
}

#[cfg(test)]
mod tests {
    use std::{ops::Range, sync::Arc};

    use arrow::{
        array::{DictionaryArray, Float64Array, TimestampNanosecondArray},
        datatypes::{DataType, Field, Int32Type, Schema, SchemaRef, TimeUnit},
        error::ArrowError,
        record_batch::RecordBatch,
    };
    use arrow_util::assert_batches_eq;
    use datafusion::error::Result;

    use crate::exec::gapfill::{algo::GapFillParams, series::Series};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(
                "g0",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("t", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("a", DataType::Float64, true),
        ]))
    }

    struct TestBatch {
        g0: Vec<Option<&'static str>>,
        t: Vec<Option<i64>>,
        a: Vec<Option<f64>>,
    }

    impl TryFrom<TestBatch> for RecordBatch {
        type Error = ArrowError;

        fn try_from(value: TestBatch) -> std::result::Result<Self, ArrowError> {
            Self::try_new(
                schema(),
                vec![
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(
                        value.g0.into_iter(),
                    )),
                    Arc::new(TimestampNanosecondArray::from(value.t)),
                    Arc::new(Float64Array::from(value.a)),
                ],
            )
        }
    }

    #[test]
    fn test_build_output() -> Result<()> {
        let in_batch: RecordBatch = TestBatch {
            g0: vec![Some("a"), Some("a")],
            t: vec![Some(1000), Some(1100)],
            a: vec![Some(9.9), Some(18.18)],
        }
        .try_into()?;
        let params = GapFillParams {
            stride: 25,
            first_ts: 975,
            last_ts: 1125,
        };
        let series = {
            let time_column = in_batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Series::new(Range { start: 0, end: 2 }, time_column)
        };
        let out_batch = super::build_output(
            schema(),
            &params,
            (
                1,
                in_batch.columns()[1]
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap(),
            ),
            &[(0, Arc::clone(&in_batch.columns()[0]))],
            &[(2, Arc::clone(&in_batch.columns()[2]))],
            &[series],
        )?;
        let expected = [
            "+----+--------------------------------+-------+",
            "| g0 | t                              | a     |",
            "+----+--------------------------------+-------+",
            "| a  | 1970-01-01T00:00:00.000000975Z |       |",
            "| a  | 1970-01-01T00:00:00.000001Z    | 9.9   |",
            "| a  | 1970-01-01T00:00:00.000001025Z |       |",
            "| a  | 1970-01-01T00:00:00.000001050Z |       |",
            "| a  | 1970-01-01T00:00:00.000001075Z |       |",
            "| a  | 1970-01-01T00:00:00.000001100Z | 18.18 |",
            "| a  | 1970-01-01T00:00:00.000001125Z |       |",
            "+----+--------------------------------+-------+",
        ];
        assert_batches_eq!(expected, &[out_batch]);
        Ok(())
    }
}
