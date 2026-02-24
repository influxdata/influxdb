use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::FieldRef;
use arrow::datatypes::{DataType, Field, IntervalUnit::MonthDayNano, TimeUnit};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::PartitionEvaluator;
use datafusion::logical_expr::function::PartitionEvaluatorArgs;
use datafusion::logical_expr::{
    Signature, TIMEZONE_WILDCARD, TypeSignature, Volatility, WindowUDFImpl,
    function::WindowUDFFieldArgs,
};

use crate::delta_time;
use crate::{NUMERICS, error};

/// Takes an error message string and a list of ScalarValue variables, and constructs a format! call
/// that includes the data types of the variables at the end of the message.
///
/// Example:
/// ```ignore
/// let value = ScalarValue::Int64(Some(42));
/// let msg = format_column_types!("unsupported type", value);
/// assert_eq!(msg, "unsupported type (value=Int64)");
/// ```
macro_rules! format_column_types {
    ($msg:expr, $first:ident $(, $rest:ident)* $(,)?) => {
        format!(
            concat!($msg, " (", stringify!($first), "={}" $(, ", ", stringify!($rest), "={}")*, ")"),
            $first.data_type() $(, $rest.data_type())*
        )
    };
}

const INTEGRAL_WINDOW_NAME: &str = "integral_window";

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct IntegralUDWF {
    signature: Signature,
}

impl IntegralUDWF {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::one_of(
                NUMERICS
                    .iter()
                    .flat_map(|dt| {
                        [
                            TypeSignature::Exact(vec![
                                dt.clone(),                                      // value
                                DataType::Interval(MonthDayNano),                // unit
                                DataType::Timestamp(TimeUnit::Nanosecond, None), // time
                                DataType::Duration(TimeUnit::Nanosecond),        // duration
                                DataType::Duration(TimeUnit::Nanosecond),        // offset
                            ]),
                            TypeSignature::Exact(vec![
                                dt.clone(),                       // value
                                DataType::Interval(MonthDayNano), // unit
                                // time with tz
                                DataType::Timestamp(
                                    TimeUnit::Nanosecond,
                                    Some(TIMEZONE_WILDCARD.into()),
                                ),
                                DataType::Duration(TimeUnit::Nanosecond), // duration
                                DataType::Duration(TimeUnit::Nanosecond), // offset
                            ]),
                        ]
                    })
                    .collect(),
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for IntegralUDWF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        INTEGRAL_WINDOW_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn field(&self, field_args: WindowUDFFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Float64,
            true,
        )))
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs<'_>,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(TrapezoidalRulePartitionEvaluator {}))
    }
}

/// PartitionEvaluator which returns the trapezoidal rule calculation for input values.
#[derive(Debug)]
struct TrapezoidalRulePartitionEvaluator {}

impl PartitionEvaluator for TrapezoidalRulePartitionEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> Result<Arc<dyn Array>> {
        const ZERO_DURATION: ScalarValue = ScalarValue::DurationNanosecond(Some(0));

        assert_eq!(values.len(), 5);

        let array = Arc::clone(&values[0]);
        let times = Arc::clone(&values[2]);

        // The second element of the values array is the second argument to
        // the 'integral' function. This specifies the unit duration for the
        // integral to use.
        //
        // INVARIANT:
        // The planner guarantees that the second argument is always a duration
        // literal.
        // TODO(adam): consider moving the unit calculation out of this function and into the
        // outer aggregate function, to avoid unnecessary calculations
        let unit = ScalarValue::try_from_array(&values[1], 0)?;

        // the fourth and fifth parameters are supplied from any `GROUP BY time(duration, offset)`
        // on the query. If not provided, they are set to 0.
        // INVARIANT:
        // The planner guarantees that the fourth and fifth argument are always a duration
        // literal.
        let duration = ScalarValue::try_from_array(&values[3], 0)?;
        let offset = ScalarValue::try_from_array(&values[4], 0)?;

        // process first value by saving it for later and outputting a 0 to satisfy the
        // DataFusion invariant that RecordBatch sizes should be equal.
        let mut last_idx = 0;
        let mut last_value = ScalarValue::try_from_array(&array, 0)?;
        let mut last_time = ScalarValue::try_from_array(&times, 0)?;
        let mut areas: Vec<ScalarValue> = Vec::with_capacity(array.len());
        areas.push(ScalarValue::Float64(Some(0.0)));

        // determine sort order by comparing two non-equal times
        let ascending = duration == ZERO_DURATION || {
            let mut ascending = true;
            for idx in 1..array.len() {
                let time = ScalarValue::try_from_array(&times, idx)?;
                if time != last_time {
                    ascending = time > last_time;
                    break;
                }
            }
            ascending
        };

        // track the `GROUP BY time` window
        let mut window_start;
        let mut window_end;
        // when duration is zero, there is no `GROUP BY time` clause.
        // set to null and ignore.
        if duration == ZERO_DURATION {
            (window_start, window_end) = (
                ScalarValue::TimestampNanosecond(None, None),
                ScalarValue::TimestampNanosecond(None, None),
            );
        }
        // otherwise calculate the first window bounds
        else {
            let window_bounds =
                calc_window(&ScalarValue::try_from_array(&times, 0)?, &duration, &offset)?;
            if ascending {
                (window_start, window_end) = window_bounds;
            } else {
                (window_end, window_start) = window_bounds;
            }
        }

        // calculate all subsequent values
        for idx in 1..array.len() {
            let value = ScalarValue::try_from_array(&array, idx)?;
            let time = ScalarValue::try_from_array(&times, idx)?;

            // ignore null values and pass through to output
            if value.is_null() {
                areas.push(ScalarValue::Float64(None));
                continue;
            }
            // if timestamp is the same as last value, skip.
            // we emit a null to satisfy the DataFusion RecordBatch size invariant
            if last_time == time {
                last_value = value.clone();
                areas.push(ScalarValue::Float64(None));
                continue;
            }
            // handle window boundary of `group by time`
            if duration != ZERO_DURATION
                && (if ascending {
                    time >= window_end
                } else {
                    time <= window_end
                })
            {
                // if the previous point is not on the window boundary, interpolate the area at the end
                // of the window and add it to the area of the previous non-null point
                if last_time != window_end {
                    let value =
                        calc_linear(&window_end, (&last_value, &last_time), (&value, &time))?;
                    areas[last_idx] = areas[last_idx].add(ScalarValue::Float64(Some(
                        0.5 * sum_values(&value, &last_value)?
                            * delta_time(&window_end, &last_time, &unit)?,
                    )))?;
                    // save end of this window as previous point, so that the first point of the
                    // new window will interpolate from the window boundary
                    last_time = window_end.clone();
                    last_value = value.clone();
                }
                // calculate the next window
                let window_bounds = calc_window(&time, &duration, &offset)?;
                if ascending {
                    (window_start, window_end) = window_bounds;
                } else {
                    (window_end, window_start) = window_bounds;
                };
            }
            //normal operation: calculate area and emit to output
            areas.push(ScalarValue::Float64(Some(
                0.5 * sum_values(&value, &last_value)? * delta_time(&time, &last_time, &unit)?,
            )));
            last_value = value.clone();
            last_time = time.clone();
            last_idx = idx;
        }
        // if the last point is at the start time, we skip it because there is no area.
        if last_time == window_start {
            areas[last_idx] = ScalarValue::Float64(None);
        }
        Ok(Arc::new(ScalarValue::iter_to_array(areas)?))
    }

    fn uses_window_frame(&self) -> bool {
        false
    }

    fn include_rank(&self) -> bool {
        false
    }
}

/// Given an input `time` value and two known points `prev_point` and `next_point` in a time series,
/// computes the value at input `time` using linear interpolation.
fn calc_linear(
    time: &ScalarValue,
    prev_point: (&ScalarValue, &ScalarValue),
    next_point: (&ScalarValue, &ScalarValue),
) -> Result<ScalarValue> {
    let (prev_value, prev_time) = prev_point;
    let (next_value, next_time) = next_point;
    let (window_time, prev_time, next_time) = match (time, prev_time, next_time) {
        (
            ScalarValue::TimestampNanosecond(Some(window_time), _),
            ScalarValue::TimestampNanosecond(Some(prev_time), _),
            ScalarValue::TimestampNanosecond(Some(next_time), _),
        ) => (window_time, prev_time, next_time),
        _ => {
            return error::internal(format_column_types!(
                "unsupported time type for integral",
                time,
                prev_time,
                next_time
            ));
        }
    };
    let prev_value = match prev_value {
        ScalarValue::Float64(Some(val)) => *val,
        ScalarValue::Int64(Some(val)) => *val as f64,
        ScalarValue::UInt64(Some(val)) => *val as f64,
        _ => {
            return error::internal(format_column_types!(
                "integral attempted on unsupported values",
                prev_value
            ));
        }
    };
    let next_value = match next_value {
        ScalarValue::Float64(Some(val)) => *val,
        ScalarValue::Int64(Some(val)) => *val as f64,
        ScalarValue::UInt64(Some(val)) => *val as f64,
        _ => {
            return error::internal(format_column_types!(
                "integral attempted on unsupported values",
                next_value
            ));
        }
    };
    // compute y = mx + b
    let m = (next_value - prev_value) / ((next_time - prev_time) as f64);
    let x = (window_time - prev_time) as f64;
    let b = prev_value;
    let y = m * x + b;
    Ok(ScalarValue::Float64(Some(y)))
}

/// Given a time value, calculates the (min, max) boundary of the
/// `group by time(duration, offset)` interval that contains it.
fn calc_window(
    time: &ScalarValue,
    duration: &ScalarValue,
    offset: &ScalarValue,
) -> Result<(ScalarValue, ScalarValue)> {
    let ScalarValue::TimestampNanosecond(Some(time), tz) = time else {
        return error::internal(format_column_types!(
            "unsupported time type for integral window calculation",
            time
        ));
    };
    let ScalarValue::DurationNanosecond(Some(offset)) = offset else {
        return error::internal(format_column_types!(
            "unsupported offset type for integral window calculation",
            offset
        ));
    };
    let ScalarValue::DurationNanosecond(Some(duration)) = duration else {
        return error::internal(format_column_types!(
            "unsupported duration type for integral window calculation",
            duration
        ));
    };
    // subtract the offset to the time so we calculate the correct base interval
    let time = time - offset;
    // truncate time by duration
    let mod_time = time % duration;
    // Negative modulo rounds up instead of down (for times less than UNIX epoch)
    let mod_time = if mod_time < 0 {
        mod_time + duration
    } else {
        mod_time
    };
    let window_start = time - mod_time + offset;
    let window_end = time + (duration - mod_time) + offset;
    Ok((
        ScalarValue::TimestampNanosecond(Some(window_start), tz.clone()),
        ScalarValue::TimestampNanosecond(Some(window_end), tz.clone()),
    ))
}

/// Computes a sum of two numeric [ScalarValue]s as 64-bit floating point numbers.
fn sum_values(value: &ScalarValue, last_value: &ScalarValue) -> Result<f64> {
    let value = match value {
        ScalarValue::Float64(Some(val)) => *val,
        ScalarValue::Int64(Some(val)) => *val as f64,
        ScalarValue::UInt64(Some(val)) => *val as f64,
        _ => {
            return error::internal(format_column_types!(
                "integral attempted on unsupported values",
                value
            ));
        }
    };
    let last_value = match last_value {
        ScalarValue::Float64(Some(val)) => *val,
        ScalarValue::Int64(Some(val)) => *val as f64,
        ScalarValue::UInt64(Some(val)) => *val as f64,
        _ => {
            return error::internal(format_column_types!(
                "integral attempted on unsupported values",
                last_value
            ));
        }
    };
    Ok(value + last_value)
}

#[cfg(test)]
mod tests {
    use datafusion::common::ScalarValue;

    #[test]
    fn test_format_column_types_macro() {
        // Test with one variable
        let value = ScalarValue::Int64(Some(42));
        let msg1 = format_column_types!("unsupported type", value);
        assert_eq!(msg1, "unsupported type (value=Int64)");

        // Test with two variables
        let prev_value = ScalarValue::Float64(Some(3.5));
        let msg2 = format_column_types!("unsupported types", value, prev_value);
        assert_eq!(msg2, "unsupported types (value=Int64, prev_value=Float64)");

        // Test with trailing comma
        let msg3 = format_column_types!("unsupported types", value, prev_value,);
        assert_eq!(msg3, "unsupported types (value=Int64, prev_value=Float64)");
    }
}
