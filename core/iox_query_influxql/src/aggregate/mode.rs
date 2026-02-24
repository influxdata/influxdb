use arrow::datatypes::{FieldRef, TimeUnit::Nanosecond};
use assert_matches::assert_matches;
use std::any::Any;
use std::sync::Arc;

use crate::error;
use arrow::{
    array::{Array, ArrayRef, cast},
    datatypes::{DataType, Field},
};
use datafusion::{
    common::Result,
    logical_expr::{Accumulator, Signature, Volatility},
    physical_expr::expressions::format_state_name,
    scalar::ScalarValue,
};
use datafusion::{
    common::plan_err,
    logical_expr::{
        AggregateUDFImpl,
        function::{AccumulatorArgs, StateFieldsArgs},
    },
};
use itertools::Itertools;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct ModeUDF {
    signature: Signature,
}

impl ModeUDF {
    pub(super) fn new() -> Self {
        Self {
            // We need to allow `any` for the first param and a Timestamp for the second.
            // Rather than enumerating every `DataType` here (which could change over time),
            // just allow `any` for both params and check in `update_batch`.
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ModeUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "mode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("mode() requires two arguments");
        }
        match arg_types[1] {
            DataType::Timestamp(Nanosecond, _) => {}
            _ => return plan_err!("mode() requires the second argument to be a timestamp"),
        }
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, arg: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        let time_data_type = arg.exprs[1].data_type(arg.schema)?;
        Ok(Box::new(ModeAccumulator::new(
            arg.return_field.data_type().clone(),
            time_data_type,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        let item_list = DataType::List(Arc::new(Field::new(
            "item",
            args.return_field.data_type().clone(),
            true,
        )));
        let timestamp_list = DataType::List(Arc::new(Field::new(
            "item",
            args.input_fields[1].data_type().clone(),
            true,
        )));

        Ok(vec![
            Arc::new(Field::new(
                format_state_name(args.name, "value"),
                item_list,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "timestamp"),
                timestamp_list,
                true,
            )),
        ])
    }
}

#[derive(Debug)]
struct ModeAccumulator {
    data_type: DataType,
    time_data_type: DataType,
    point_values: Vec<ScalarValue>,
    point_times: Vec<ScalarValue>,
}

impl ModeAccumulator {
    fn new(data_type: DataType, time_data_type: DataType) -> Self {
        Self {
            data_type,
            time_data_type,
            point_values: Vec::new(),
            point_times: Vec::new(),
        }
    }

    fn update(&mut self, point_values: ArrayRef, point_times: ArrayRef) -> Result<()> {
        assert_eq!(point_values.data_type(), &self.data_type);
        assert_matches!(point_times.data_type(), &DataType::Timestamp(Nanosecond, _));

        let nulls = point_values.nulls();
        let null_len = nulls.map_or(0, |nb| nb.null_count());

        self.point_values.reserve(point_values.len() - null_len);
        self.point_times.reserve(point_values.len() - null_len);

        for idx in 0..point_values.len() {
            if nulls.is_none_or(|nb| nb.is_valid(idx)) {
                let value = ScalarValue::try_from_array(&point_values, idx)?;
                let time = ScalarValue::try_from_array(&point_times, idx)?;
                self.point_values.push(value);
                self.point_times.push(time);
            }
        }
        Ok(())
    }

    fn mode(&self) -> Result<ScalarValue> {
        // This algorithm is effectively copied from the original in Influxdb 1.x
        // https://github.com/influxdata/influxdb/blob/903b8cd0ea70daf3733a90978c579dc42b6f088c/query/call_iterator.go#L667

        if self.point_values.len() == 1 {
            return Ok(self.point_values[0].clone());
        }

        let point_times: Result<Vec<i64>, _> =
            self.point_times.iter().map(from_timestamp).collect();
        let point_times = point_times?;

        let points = self.point_values.iter().zip(point_times);
        let mut sorted_points = points
            .sorted_by(|a, b| a.partial_cmp(b).expect("unstable sorting encountered"))
            .peekable();

        let mut curr_freq = 0;
        let mut most_freq = 0;
        let mut curr_mode: &ScalarValue;
        let mut most_mode: &ScalarValue;
        let mut curr_time;
        let mut most_time;

        // Presets the values from the first point
        match sorted_points.peek() {
            Some((value, time)) => {
                curr_mode = value;
                most_mode = value;
                curr_time = *time;
                most_time = *time;
            }
            None => return ScalarValue::try_from(&self.data_type), // We have an empty set of points
        }

        for (value, time) in sorted_points {
            if value != curr_mode {
                curr_freq = 1;
                curr_mode = value;
                curr_time = time;
                continue;
            }
            curr_freq += 1;
            if most_freq > curr_freq || (most_freq == curr_freq && curr_time > most_time) {
                continue;
            }
            most_freq = curr_freq;
            most_mode = value;
            most_time = time;
        }

        Ok(most_mode.clone())
    }

    // calculating the size for fixed length values, taking first batch size * number of batches
    // This method is faster than .full_size(), however it is not suitable for variable length values like strings or complex types
    fn fixed_size(&self) -> usize {
        std::mem::size_of_val(self)
            + (self
                .point_values
                .first()
                .map(ScalarValue::size)
                .unwrap_or(0)
                * self.point_values.capacity())
            + (std::mem::size_of::<i64>() * self.point_times.capacity())
    }

    // calculates the size as accurate as possible, call to this method is expensive
    fn full_size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .point_values
                .iter()
                .map(ScalarValue::size)
                .sum::<usize>()
            + (std::mem::size_of::<i64>() * self.point_times.capacity())
    }
}

impl Accumulator for ModeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].len(), values[1].len());

        self.update(Arc::clone(&values[0]), Arc::clone(&values[1]))
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.mode()
    }

    fn size(&self) -> usize {
        match &self.data_type {
            DataType::Boolean | DataType::Null => self.fixed_size(),
            d if d.is_primitive() => self.fixed_size(),
            _ => self.full_size(),
        }
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let v_arr = ScalarValue::new_list_nullable(&self.point_values, &self.data_type);
        let t_arr = ScalarValue::new_list_nullable(&self.point_times, &self.time_data_type);

        Ok(vec![ScalarValue::List(v_arr), ScalarValue::List(t_arr)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        assert_eq!(states.len(), 2);
        assert_eq!(states[0].len(), states[1].len());

        let point_values_array = cast::as_list_array(&states[0]);
        let point_times_array = cast::as_list_array(&states[1]);
        for idx in 0..point_values_array.len() {
            self.update(point_values_array.value(idx), point_times_array.value(idx))?;
        }
        Ok(())
    }
}

/// Take a scalar timestamptz, and return a UTC nanoseconds.
///
/// Nanoseconds provided should always be in UTC, even if TZ differs. The offset
/// information is not retained.
fn from_timestamp(time: &ScalarValue) -> Result<i64> {
    match time {
        ScalarValue::TimestampNanosecond(nano, _tz) => Ok(nano.unwrap_or(0)),
        sv => error::internal(format!(
            "expected TimestampNanosecond for mode timestamp, got {}",
            sv.data_type()
        )),
    }
}
