//! Filling gaps with interpolated values.
use std::sync::Arc;

use arrow::{
    array::{as_primitive_array, Array, ArrayRef, PrimitiveArray, TimestampNanosecondArray},
    datatypes::{ArrowPrimitiveType, DataType, Float64Type, Int64Type, UInt64Type},
};

use crate::exec::gapfill::params::GapFillParams;

use datafusion::{
    error::{DataFusionError, Result},
    scalar::ScalarValue,
};

use super::{AggrColState, Cursor, RowStatus, VecBuilder};

/// [Cursor] methods that are related to interpolation.
impl Cursor {
    /// Create an Arrow array with gaps filled in between values
    /// using linear interpolation.
    pub(super) fn build_aggr_fill_interpolate(
        &mut self,
        params: &GapFillParams,
        series_ends: &[usize],
        input_time_array: &TimestampNanosecondArray,
        input_aggr_array: &ArrayRef,
    ) -> Result<ArrayRef> {
        match input_aggr_array.data_type() {
            DataType::Int64 => {
                let input_aggr_array = as_primitive_array::<Int64Type>(input_aggr_array);
                self.build_aggr_fill_interpolate_typed(
                    params,
                    series_ends,
                    input_time_array,
                    input_aggr_array,
                )
            }
            DataType::UInt64 => {
                let input_aggr_array = as_primitive_array::<UInt64Type>(input_aggr_array);
                self.build_aggr_fill_interpolate_typed(
                    params,
                    series_ends,
                    input_time_array,
                    input_aggr_array,
                )
            }
            DataType::Float64 => {
                let input_aggr_array = as_primitive_array::<Float64Type>(input_aggr_array);
                self.build_aggr_fill_interpolate_typed(
                    params,
                    series_ends,
                    input_time_array,
                    input_aggr_array,
                )
            }
            dt => Err(DataFusionError::Execution(format!(
                "unsupported data type {dt} for interpolation gap filling"
            ))),
        }
    }

    /// Create an Arrow array with gaps filled in between values
    /// using linear interpolation.
    ///
    /// This method has a template parameter and so accepts Arrow arrays of either
    /// [Int64Array], [UInt64Array], or [Float64Array].
    ///
    /// [Int64Array]: arrow::array::Int64Array
    /// [UInt64Array]: arrow::array::UInt64Array
    /// [Float64Array]: arrow::array::Float64Array
    pub(super) fn build_aggr_fill_interpolate_typed<T>(
        &mut self,
        params: &GapFillParams,
        series_ends: &[usize],
        input_time_array: &TimestampNanosecondArray,
        input_aggr_array: &PrimitiveArray<T>,
    ) -> Result<ArrayRef>
    where
        T: ArrowPrimitiveType,
        T::Native: LinearInterpolate,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
        Segment<T::Native>: TryFrom<Segment<ScalarValue>, Error = DataFusionError>,
        Segment<ScalarValue>: From<Segment<T::Native>>,
    {
        let segment = self
            .get_aggr_col_state()
            .segment()
            .as_ref()
            .map(|seg| Segment::<T::Native>::try_from(seg.clone()))
            .transpose()?;
        let mut builder = InterpolateBuilder {
            values: Vec::with_capacity(self.remaining_output_batch_size),
            segment,
            input_time_array,
            input_aggr_array,
        };
        self.build_vec(params, input_time_array, series_ends, &mut builder)?;

        let segment: Option<Segment<ScalarValue>> = builder.segment.clone().map(|seg| seg.into());
        self.set_aggr_col_state(AggrColState::LinearInterpolate(segment));
        let array: PrimitiveArray<T> = builder.values.into();
        Ok(Arc::new(array))
    }
}

/// Represents two non-null data values at two points in time, where the
/// gap between them must be fulled. The template parameter `T` stands in for
/// the type of the input aggregate column being filled.
#[derive(Clone, Debug)]
pub struct Segment<T> {
    start_point: (i64, T),
    end_point: (i64, T),
}

/// A macro to go from `Segment<$NATIVE>` into [`Segment<ScalarValue>`].
/// Between output batches data values in segments are stored as [`ScalarValue`]
/// to avoid type parameters in [`Cursor`].
macro_rules! impl_try_from_segment_native {
    ($NATIVE:ident) => {
        impl TryFrom<Segment<ScalarValue>> for Segment<$NATIVE> {
            type Error = DataFusionError;

            fn try_from(segment: Segment<ScalarValue>) -> Result<Self> {
                let Segment {
                    start_point: (start_ts, start_sv),
                    end_point: (end_ts, end_sv),
                } = segment;

                let start_v = $NATIVE::try_from(start_sv)?;
                let end_v = $NATIVE::try_from(end_sv)?;
                Ok(Segment {
                    start_point: (start_ts, start_v),
                    end_point: (end_ts, end_v),
                })
            }
        }
    };
}

impl_try_from_segment_native!(i64);
impl_try_from_segment_native!(u64);
impl_try_from_segment_native!(f64);

/// A macro to go from [`Segment<ScalarValue>`] into `Segment<$NATIVE>`.
/// When producing an output batch, it's easiest to use the native type
/// to represent segments being filled.
macro_rules! impl_from_segment_scalar_value {
    ($NATIVE:ident) => {
        impl From<Segment<$NATIVE>> for Segment<ScalarValue> {
            fn from(segment: Segment<$NATIVE>) -> Self {
                let Segment {
                    start_point: (start_ts, start_native),
                    end_point: (end_ts, end_native),
                } = segment;

                let start_v = ScalarValue::from(start_native);
                let end_v = ScalarValue::from(end_native);
                Segment {
                    start_point: (start_ts, start_v),
                    end_point: (end_ts, end_v),
                }
            }
        }
    };
}

impl_from_segment_scalar_value!(i64);
impl_from_segment_scalar_value!(u64);
impl_from_segment_scalar_value!(f64);

/// Implements [`VecBuilder`] for build aggregate columns whose gaps
/// are being filled using linear interpolation.
pub(super) struct InterpolateBuilder<'a, T: ArrowPrimitiveType> {
    pub values: Vec<Option<T::Native>>,
    pub segment: Option<Segment<T::Native>>,
    pub input_time_array: &'a TimestampNanosecondArray,
    pub input_aggr_array: &'a PrimitiveArray<T>,
}

impl<'a, T> VecBuilder for InterpolateBuilder<'a, T>
where
    T: ArrowPrimitiveType,
    T::Native: LinearInterpolate,
{
    fn push(&mut self, row_status: RowStatus) -> Result<()> {
        match row_status {
            RowStatus::NullTimestamp { offset, .. } => self.copy_point(offset),
            RowStatus::Present {
                ts,
                offset,
                series_end_offset,
            } => {
                if self.input_aggr_array.is_valid(offset) {
                    let end_offset = self.find_end_offset(offset, series_end_offset);
                    // Find the next non-null value in this column for the series.
                    // If there is one, start a new segment at the current value.
                    self.segment = end_offset.map(|end_offset| Segment {
                        start_point: (ts, self.input_aggr_array.value(offset)),
                        end_point: (
                            self.input_time_array.value(end_offset),
                            self.input_aggr_array.value(end_offset),
                        ),
                    });
                    self.copy_point(offset);
                } else {
                    self.values.push(
                        self.segment
                            .as_ref()
                            .map(|seg| T::Native::interpolate(seg, ts)),
                    );
                }
            }
            RowStatus::Missing { ts, .. } => self.values.push(
                self.segment
                    .as_ref()
                    .map(|seg| T::Native::interpolate(seg, ts)),
            ),
        }
        Ok(())
    }

    fn start_new_series(&mut self) -> Result<()> {
        self.segment = None;
        Ok(())
    }
}

impl<T> InterpolateBuilder<'_, T>
where
    T: ArrowPrimitiveType,
{
    /// Copies a point at `offset` into the vector that will be used to build
    /// an Arrow array.
    fn copy_point(&mut self, offset: usize) {
        let v = self
            .input_aggr_array
            .is_valid(offset)
            .then_some(self.input_aggr_array.value(offset));
        self.values.push(v)
    }

    /// Scan forward to find the endpoint for a segment that starts at `start_offset`.
    /// Skip over any null values.
    ///
    /// We are guaranteed to have buffered enough input to find the next non-null point for this series,
    /// if there is one, by the logic in [`BufferedInput`].
    ///
    /// [`BufferedInput`]: super::super::buffered_input::BufferedInput
    fn find_end_offset(&self, start_offset: usize, series_end_offset: usize) -> Option<usize> {
        ((start_offset + 1)..series_end_offset).find(|&i| self.input_aggr_array.is_valid(i))
    }
}

/// A trait for the native numeric types that can be interpolated
/// by IOx.
///
/// All implementations match what the
/// [1.8 Go implementation](<https://github.com/influxdata/influxdb/blob/688e697c51fd5353725da078555adbeff0363d01/query/linear.go>)
/// of InfluxQL does.
pub(super) trait LinearInterpolate
where
    Self: Sized,
{
    /// Given a [`Segment<Self>`] compute the value of the column at timestamp `ts`.
    fn interpolate(segment: &Segment<Self>, ts: i64) -> Self;
}

impl LinearInterpolate for i64 {
    fn interpolate(segment: &Segment<Self>, ts: i64) -> Self {
        let rise = (segment.end_point.1 - segment.start_point.1) as f64;
        let run = (segment.end_point.0 - segment.start_point.0) as f64;
        let m = rise / run;
        let x = (ts - segment.start_point.0) as f64;
        let b: f64 = segment.start_point.1 as f64;
        (m * x + b) as Self
    }
}

impl LinearInterpolate for u64 {
    fn interpolate(segment: &Segment<Self>, ts: i64) -> Self {
        let rise = if segment.end_point.1 >= segment.start_point.1 {
            (segment.end_point.1 - segment.start_point.1) as f64
        } else {
            -(segment.end_point.1.abs_diff(segment.start_point.1) as f64)
        };
        let run = (segment.end_point.0 - segment.start_point.0) as f64;
        let m = rise / run;
        let x = (ts - segment.start_point.0) as f64;
        let b: f64 = segment.start_point.1 as f64;
        (m * x + b) as Self
    }
}

impl LinearInterpolate for f64 {
    fn interpolate(segment: &Segment<Self>, ts: i64) -> Self {
        let rise = segment.end_point.1 - segment.start_point.1;
        let run = (segment.end_point.0 - segment.start_point.0) as Self;
        let m = rise / run;
        let x = (ts - segment.start_point.0) as Self;
        let b = segment.start_point.1;
        m * x + b
    }
}

/// These tests verify that interpolation works as expected for each data type.
/// For comprehensive tests that handle multiple series and input/output
/// batches, see [crate::exec::gapfill::exec_tests].
#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int64Array, TimestampNanosecondArray, UInt64Array};
    use hashbrown::HashMap;

    use crate::exec::gapfill::{
        algo::tests::{array_to_lines, assert_cursor_end_state, new_cursor_with_batch_size},
        params::GapFillParams,
        FillStrategy,
    };

    /// Verify the rounding behavior (really just truncating towards zero) which is
    /// what InfluxQL does. Also verify that we can have a descending slope in the
    /// line that does not overflow a `u64`.
    #[test]
    fn test_interpolate_u64() {
        let input_times = TimestampNanosecondArray::from(vec![
            // 1000
            Some(1100),
            // 1200
            // 1300
            Some(1400),
            Some(1500),
            // 1600
            Some(1700),
            // 1800
            Some(1900),
            // 2000
        ]);
        let input_aggr_array: ArrayRef = Arc::new(UInt64Array::from(vec![
            //             1000
            Some(100), //  1100
            //             1200
            //             1300
            Some(200), //  1400
            None,      //  1500
            //             1600
            Some(1000), // 1700
            //             1800
            Some(0), //    1900
                     //    2000
        ]));
        let series_ends = vec![input_times.len()];

        let idx = 0;
        let params = GapFillParams {
            stride: 100,
            first_ts: Some(1000),
            last_ts: 2000,
            fill_strategy: interpolate_fill_strategy(idx),
        };

        let output_batch_size = 10000;
        let mut cursor = new_cursor_with_batch_size(&params, output_batch_size);

        let time_arr: TimestampNanosecondArray = cursor
            .clone_for_aggr_col(None)
            .unwrap()
            .build_time_vec(&params, &series_ends, &input_times)
            .unwrap()
            .into();
        let arr = cursor
            .build_aggr_fill_interpolate(&params, &series_ends, &input_times, &input_aggr_array)
            .unwrap();
        insta::assert_yaml_snapshot!(array_to_lines(&time_arr, &arr), @r###"
        ---
        - +--------------------------------+------+
        - "| time                           | a0   |"
        - +--------------------------------+------+
        - "| 1970-01-01T00:00:00.000001Z    |      |"
        - "| 1970-01-01T00:00:00.000001100Z | 100  |"
        - "| 1970-01-01T00:00:00.000001200Z | 133  |"
        - "| 1970-01-01T00:00:00.000001300Z | 166  |"
        - "| 1970-01-01T00:00:00.000001400Z | 200  |"
        - "| 1970-01-01T00:00:00.000001500Z | 466  |"
        - "| 1970-01-01T00:00:00.000001600Z | 733  |"
        - "| 1970-01-01T00:00:00.000001700Z | 1000 |"
        - "| 1970-01-01T00:00:00.000001800Z | 500  |"
        - "| 1970-01-01T00:00:00.000001900Z | 0    |"
        - "| 1970-01-01T00:00:00.000002Z    |      |"
        - +--------------------------------+------+
        "###);

        assert_cursor_end_state(&cursor, &input_times, &params);
    }

    #[test]
    fn test_interpolate_i64() {
        let input_times = TimestampNanosecondArray::from(vec![
            // 1000
            Some(1100),
            // 1200
            // 1300
            Some(1400),
            Some(1500),
            // 1600
            Some(1700),
            // 1800
            Some(1900),
            // 2000
        ]);
        let input_aggr_array: ArrayRef = Arc::new(Int64Array::from(vec![
            //             1000
            Some(100), //  1100
            //             1200
            //             1300
            Some(200), //  1400
            None,      //  1500
            //             1600
            Some(1000), // 1700
            //             1800
            Some(0), //    1900
                     //    2000
        ]));
        let series_ends = vec![input_times.len()];

        let idx = 0;
        let params = GapFillParams {
            stride: 100,
            first_ts: Some(1000),
            last_ts: 2000,
            fill_strategy: interpolate_fill_strategy(idx),
        };

        let output_batch_size = 10000;
        let mut cursor = new_cursor_with_batch_size(&params, output_batch_size);

        let time_arr: TimestampNanosecondArray = cursor
            .clone_for_aggr_col(None)
            .unwrap()
            .build_time_vec(&params, &series_ends, &input_times)
            .unwrap()
            .into();
        let arr = cursor
            .build_aggr_fill_interpolate(&params, &series_ends, &input_times, &input_aggr_array)
            .unwrap();
        insta::assert_yaml_snapshot!(array_to_lines(&time_arr, &arr), @r###"
        ---
        - +--------------------------------+------+
        - "| time                           | a0   |"
        - +--------------------------------+------+
        - "| 1970-01-01T00:00:00.000001Z    |      |"
        - "| 1970-01-01T00:00:00.000001100Z | 100  |"
        - "| 1970-01-01T00:00:00.000001200Z | 133  |"
        - "| 1970-01-01T00:00:00.000001300Z | 166  |"
        - "| 1970-01-01T00:00:00.000001400Z | 200  |"
        - "| 1970-01-01T00:00:00.000001500Z | 466  |"
        - "| 1970-01-01T00:00:00.000001600Z | 733  |"
        - "| 1970-01-01T00:00:00.000001700Z | 1000 |"
        - "| 1970-01-01T00:00:00.000001800Z | 500  |"
        - "| 1970-01-01T00:00:00.000001900Z | 0    |"
        - "| 1970-01-01T00:00:00.000002Z    |      |"
        - +--------------------------------+------+
        "###);

        assert_cursor_end_state(&cursor, &input_times, &params);
    }

    #[test]
    fn test_interpolate_f64() {
        let input_times = TimestampNanosecondArray::from(vec![
            // 1000
            Some(1100),
            // 1200
            // 1300
            Some(1400),
            Some(1500),
            // 1600
            Some(1700),
            // 1800
            Some(1900),
            // 2000
        ]);
        let input_aggr_array: ArrayRef = Arc::new(Float64Array::from(vec![
            //               1000
            Some(100.0), //  1100
            //               1200
            //               1300
            Some(400.0), //  1400
            None,        //  1500
            //               1600
            Some(1000.0), // 1700
            //               1800
            Some(0.0), //    1900
                       //    2000
        ]));
        let series_ends = vec![input_times.len()];

        let idx = 0;
        let params = GapFillParams {
            stride: 100,
            first_ts: Some(1000),
            last_ts: 2000,
            fill_strategy: interpolate_fill_strategy(idx),
        };

        let output_batch_size = 10000;
        let mut cursor = new_cursor_with_batch_size(&params, output_batch_size);

        let time_arr: TimestampNanosecondArray = cursor
            .clone_for_aggr_col(None)
            .unwrap()
            .build_time_vec(&params, &series_ends, &input_times)
            .unwrap()
            .into();
        let arr = cursor
            .build_aggr_fill_interpolate(&params, &series_ends, &input_times, &input_aggr_array)
            .unwrap();
        insta::assert_yaml_snapshot!(array_to_lines(&time_arr, &arr), @r###"
        ---
        - +--------------------------------+--------+
        - "| time                           | a0     |"
        - +--------------------------------+--------+
        - "| 1970-01-01T00:00:00.000001Z    |        |"
        - "| 1970-01-01T00:00:00.000001100Z | 100.0  |"
        - "| 1970-01-01T00:00:00.000001200Z | 200.0  |"
        - "| 1970-01-01T00:00:00.000001300Z | 300.0  |"
        - "| 1970-01-01T00:00:00.000001400Z | 400.0  |"
        - "| 1970-01-01T00:00:00.000001500Z | 600.0  |"
        - "| 1970-01-01T00:00:00.000001600Z | 800.0  |"
        - "| 1970-01-01T00:00:00.000001700Z | 1000.0 |"
        - "| 1970-01-01T00:00:00.000001800Z | 500.0  |"
        - "| 1970-01-01T00:00:00.000001900Z | 0.0    |"
        - "| 1970-01-01T00:00:00.000002Z    |        |"
        - +--------------------------------+--------+
        "###);

        assert_cursor_end_state(&cursor, &input_times, &params);
    }

    fn interpolate_fill_strategy(idx: usize) -> HashMap<usize, FillStrategy> {
        std::iter::once((idx, FillStrategy::LinearInterpolate)).collect()
    }
}
