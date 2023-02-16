//! Build gap-filled output arrays
use arrow::{
    array::{ArrayRef, TimestampNanosecondArray, UInt64Array},
    compute::kernels::take,
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};

use super::{
    params::GapFillParams,
    series::{SeriesAppender, SeriesState},
};
use datafusion::error::{DataFusionError, Result};
use std::sync::Arc;

/// Represents the result of creating an output [RecordBatch],
/// including the batch itself and [SeriesState] for resuming
/// the current series in the next batch if needed.
pub(super) struct OutputState {
    pub output_batch: RecordBatch,
    pub new_series_state: SeriesState,
}

/// Build a [RecordBatch] from the given slice of [SeriesAppender]s.
/// It will build entire output columns at the same time, for every
/// input [SeriesAppender], before proceeding to the next output column.
///
/// # Arguments
///
/// * `schema` - the input/output schema
/// * `params` - the stride, first and last timestamps for the series
/// * `input_time_array` - the array of timestamps from the input.
///         The first item in the tuple is its offset in the schema.
/// * `group_arr` - Arrays of group columns from the input.
///         The first item in each tuple is the array's offset in the schema.
/// * `aggr_arr` - Arrays of aggregate columns from the input.
///         The first item in each tuple is the array's offset in the schema.
/// * `series_batch` - The [SeriesAppender]s that will be used to create the output batch.
pub(super) fn build_output(
    input_series_state: SeriesState,
    schema: SchemaRef,
    params: &GapFillParams,
    input_time_array: (usize, &TimestampNanosecondArray),
    group_arr: &[(usize, ArrayRef)],
    aggr_arr: &[(usize, ArrayRef)],
    series_batch: &[SeriesAppender],
) -> Result<OutputState> {
    let mut output_arrays: Vec<(usize, ArrayRef)> =
        Vec::with_capacity(group_arr.len() + aggr_arr.len() + 1); // plus one for time column
    let output_batch_size = input_series_state.remaining_output_batch_size();

    // build the time column
    let (time_idx, input_time_array) = input_time_array;
    let mut time_vec: Vec<Option<i64>> = Vec::with_capacity(output_batch_size);
    let final_series_state = build_vec(
        params,
        series_batch,
        input_series_state.clone(),
        |series_state, series| {
            series.append_time_values(params, input_time_array, series_state, &mut time_vec)
        },
    )?;
    let output_time_len = time_vec.len();
    output_arrays.push((time_idx, Arc::new(TimestampNanosecondArray::from(time_vec))));

    // build the other group columns
    for (idx, ga) in group_arr.iter() {
        let mut take_vec: Vec<u64> = Vec::with_capacity(output_batch_size);
        build_vec(
            params,
            series_batch,
            input_series_state.clone(),
            |series_state, series| {
                series.append_group_take(params, input_time_array, series_state, &mut take_vec)
            },
        )?;
        if take_vec.len() != output_time_len {
            return Err(DataFusionError::Internal(format!(
                "gapfill group column has {} rows, expected {}",
                take_vec.len(),
                output_time_len
            )));
        }
        let take_arr = UInt64Array::from(take_vec);
        output_arrays.push((*idx, take::take(ga, &take_arr, None)?))
    }

    // Build the aggregate columns
    for (idx, aa) in aggr_arr.iter() {
        let mut take_vec: Vec<Option<u64>> = Vec::with_capacity(output_batch_size);
        build_vec(
            params,
            series_batch,
            input_series_state.clone(),
            |series_state, series| {
                series.append_aggr_take(params, input_time_array, series_state, &mut take_vec)
            },
        )?;
        if take_vec.len() != output_time_len {
            return Err(DataFusionError::Internal(format!(
                "gapfill aggr column has {} rows, expected {}",
                take_vec.len(),
                output_time_len
            )));
        }
        let take_arr = UInt64Array::from(take_vec);
        output_arrays.push((*idx, take::take(aa, &take_arr, None)?));
    }

    output_arrays.sort_by(|(a, _), (b, _)| a.cmp(b));
    let output_arrays: Vec<_> = output_arrays.into_iter().map(|(_, arr)| arr).collect();
    Ok(OutputState {
        output_batch: RecordBatch::try_new(Arc::clone(&schema), output_arrays)
            .map_err(DataFusionError::ArrowError)?,
        new_series_state: final_series_state,
    })
}

fn build_vec<F>(
    params: &GapFillParams,
    series_batch: &[SeriesAppender],
    mut series_state: SeriesState,
    mut f: F,
) -> Result<SeriesState>
where
    F: FnMut(SeriesState, &SeriesAppender) -> SeriesState,
{
    let first_series = series_batch.first().ok_or(DataFusionError::Internal(
        "expected at least one item in series batch".to_string(),
    ))?;
    // Process the first series separately as it may just be a part of what
    // did not fit in the previous output batch.
    series_state = f(series_state, first_series);
    for series in series_batch.iter().skip(1) {
        series_state.fresh_series(params);
        series_state = f(series_state, series);
    }
    Ok(series_state)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{DictionaryArray, Float64Array, TimestampNanosecondArray},
        datatypes::{Int32Type, SchemaRef},
        error::ArrowError,
        record_batch::RecordBatch,
    };
    use arrow_util::assert_batches_eq;
    use datafusion::error::Result;
    use schema::{InfluxFieldType, SchemaBuilder};

    use crate::exec::gapfill::{
        builder::OutputState,
        params::GapFillParams,
        series::{SeriesAppender, SeriesState},
    };

    fn schema() -> SchemaRef {
        SchemaBuilder::new()
            .tag("g0")
            .timestamp()
            .influx_field("a", InfluxFieldType::Float)
            .build()
            .unwrap()
            .into()
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
        let series = SeriesAppender::new_with_input_end_offset(2);
        let OutputState {
            output_batch: batch,
            ..
        } = super::build_output(
            SeriesState::new(&params, 10000),
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
            "| g0 | time                           | a     |",
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
        assert_batches_eq!(expected, &[batch]);
        Ok(())
    }
}
