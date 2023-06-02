//! Internal implementaton of InfluxDB "Selector" Functions
//! Tests are in selector module
//!
//! This module is implemented with macros rather than generic types;
//! I tried valiantly (at least in my mind) to use Generics , but I
//! couldn't get the traits to work out correctly (as Bool, I64/F64
//! and Utf8 arrow types don't share enough in common).

use std::{fmt::Debug, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, TimestampNanosecondArray},
    compute::kernels::aggregate::{max as array_max, min as array_min},
    datatypes::DataType,
};
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    physical_plan::{
        expressions::{MaxAccumulator, MinAccumulator},
        Accumulator,
    },
    scalar::ScalarValue,
};

use super::type_handling::make_struct_scalar;

/// Implements the logic of the specific selector function (this is a
/// cutdown version of the Accumulator DataFusion trait, to allow
/// sharing between implementations)
pub trait Selector: Debug + Send + Sync {
    /// return state in a form that DataFusion can store during execution
    fn datafusion_state(&self) -> DataFusionResult<Vec<ScalarValue>>;

    /// produces the final value of this selector for the specified output type
    fn evaluate(&self) -> DataFusionResult<ScalarValue>;

    /// Update this selector's state based on values in value_arr and time_arr
    fn update_batch(
        &mut self,
        value_arr: &ArrayRef,
        time_arr: &ArrayRef,
        other_arrs: &[ArrayRef],
    ) -> DataFusionResult<()>;

    /// Allocated size required for this selector, in bytes,
    /// including `Self`.  Allocated means that for internal
    /// containers such as `Vec`, the `capacity` should be used not
    /// the `len`
    fn size(&self) -> usize;
}

#[derive(Debug)]
pub struct FirstSelector {
    value: ScalarValue,
    time: Option<i64>,
    other: Box<[ScalarValue]>,
}

impl FirstSelector {
    pub fn new<'a>(
        data_type: &'a DataType,
        other_types: impl IntoIterator<Item = &'a DataType>,
    ) -> DataFusionResult<Self> {
        Ok(Self {
            value: ScalarValue::try_from(data_type)?,
            time: None,
            other: other_types
                .into_iter()
                .map(ScalarValue::try_from)
                .collect::<DataFusionResult<_>>()?,
        })
    }
}

impl Selector for FirstSelector {
    fn datafusion_state(&self) -> DataFusionResult<Vec<ScalarValue>> {
        Ok([
            self.value.clone(),
            ScalarValue::TimestampNanosecond(self.time, None),
        ]
        .into_iter()
        .chain(self.other.iter().cloned())
        .collect())
    }

    fn evaluate(&self) -> DataFusionResult<ScalarValue> {
        Ok(make_struct_scalar(
            &self.value,
            &ScalarValue::TimestampNanosecond(self.time, None),
            self.other.iter(),
        ))
    }

    fn update_batch(
        &mut self,
        value_arr: &ArrayRef,
        time_arr: &ArrayRef,
        other_arrs: &[ArrayRef],
    ) -> DataFusionResult<()> {
        // Only look for times where the array also has a non
        // null value (the time array should have no nulls itself)
        //
        // For example, for the following input, the correct
        // current min time is 200 (not 100)
        //
        // value | time
        // --------------
        // NULL  | 100
        // A     | 200
        // B     | 300
        //
        let time_arr = arrow::compute::nullif(time_arr, &arrow::compute::is_null(&value_arr)?)?;

        let time_arr = time_arr
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            // the input type arguments should be ensured by datafusion
            .expect("Second argument was time");
        let cur_min_time = array_min(time_arr);

        let need_update = match (&self.time, &cur_min_time) {
            (Some(time), Some(cur_min_time)) => cur_min_time < time,
            // No existing minimum, so update needed
            (None, Some(_)) => true,
            // No actual minimum time found, so no update needed
            (_, None) => false,
        };

        if need_update {
            let index = time_arr
                .iter()
                // arrow doesn't tell us what index had the
                // minimum, so need to find it ourselves see also
                // https://github.com/apache/arrow-datafusion/issues/600
                .enumerate()
                .find(|(_, time)| cur_min_time == *time)
                .map(|(idx, _)| idx)
                .unwrap(); // value always exists

            // update all or nothing in case of an error
            let value_new = ScalarValue::try_from_array(&value_arr, index)?;
            let other_new = other_arrs
                .iter()
                .map(|arr| ScalarValue::try_from_array(arr, index))
                .collect::<DataFusionResult<_>>()?;

            self.time = cur_min_time;
            self.value = value_new;
            self.other = other_new;
        }

        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.value) + self.value.size()
    }
}

#[derive(Debug)]
pub struct LastSelector {
    value: ScalarValue,
    time: Option<i64>,
}

impl LastSelector {
    pub fn new(data_type: &DataType) -> DataFusionResult<Self> {
        Ok(Self {
            value: ScalarValue::try_from(data_type)?,
            time: None,
        })
    }
}

impl Selector for LastSelector {
    fn datafusion_state(&self) -> DataFusionResult<Vec<ScalarValue>> {
        Ok(vec![
            self.value.clone(),
            ScalarValue::TimestampNanosecond(self.time, None),
        ])
    }

    fn evaluate(&self) -> DataFusionResult<ScalarValue> {
        Ok(make_struct_scalar(
            &self.value,
            &ScalarValue::TimestampNanosecond(self.time, None),
            [],
        ))
    }

    fn update_batch(
        &mut self,
        value_arr: &ArrayRef,
        time_arr: &ArrayRef,
        other_arrs: &[ArrayRef],
    ) -> DataFusionResult<()> {
        if !other_arrs.is_empty() {
            return Err(DataFusionError::NotImplemented(
                "selector last w/ additional args".to_string(),
            ));
        }

        // Only look for times where the array also has a non
        // null value (the time array should have no nulls itself)
        //
        // For example, for the following input, the correct
        // current max time is 200 (not 300)
        //
        // value | time
        // --------------
        // A     | 100
        // B     | 200
        // NULL  | 300
        //
        let time_arr = arrow::compute::nullif(time_arr, &arrow::compute::is_null(&value_arr)?)?;

        let time_arr = time_arr
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            // the input type arguments should be ensured by datafusion
            .expect("Second argument was time");
        let cur_max_time = array_max(time_arr);

        let need_update = match (&self.time, &cur_max_time) {
            (Some(time), Some(cur_max_time)) => time < cur_max_time,
            // No existing maximum, so update needed
            (None, Some(_)) => true,
            // No actual maximum value found, so no update needed
            (_, None) => false,
        };

        if need_update {
            let index = time_arr
                .iter()
                // arrow doesn't tell us what index had the
                // maximum, so need to find it ourselves
                .enumerate()
                .find(|(_, time)| cur_max_time == *time)
                .map(|(idx, _)| idx)
                .unwrap(); // value always exists

            // update all or nothing in case of an error
            let value_new = ScalarValue::try_from_array(&value_arr, index)?;

            self.time = cur_max_time;
            self.value = value_new;
        }

        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.value) + self.value.size()
    }
}

/// Did we find a new min/max
#[derive(Debug)]
enum ActionNeeded {
    UpdateValueAndTime,
    UpdateTime,
    Nothing,
}

impl ActionNeeded {
    fn update_value(&self) -> bool {
        match self {
            Self::UpdateValueAndTime => true,
            Self::UpdateTime => false,
            Self::Nothing => false,
        }
    }
    fn update_time(&self) -> bool {
        match self {
            Self::UpdateValueAndTime => true,
            Self::UpdateTime => true,
            Self::Nothing => false,
        }
    }
}

#[derive(Debug)]
pub struct MinSelector {
    value: ScalarValue,
    time: Option<i64>,
}

impl MinSelector {
    pub fn new(data_type: &DataType) -> DataFusionResult<Self> {
        Ok(Self {
            value: ScalarValue::try_from(data_type)?,
            time: None,
        })
    }
}

impl Selector for MinSelector {
    fn datafusion_state(&self) -> DataFusionResult<Vec<ScalarValue>> {
        Ok(vec![
            self.value.clone(),
            ScalarValue::TimestampNanosecond(self.time, None),
        ])
    }

    fn evaluate(&self) -> DataFusionResult<ScalarValue> {
        Ok(make_struct_scalar(
            &self.value,
            &ScalarValue::TimestampNanosecond(self.time, None),
            [],
        ))
    }

    fn update_batch(
        &mut self,
        value_arr: &ArrayRef,
        time_arr: &ArrayRef,
        other_arrs: &[ArrayRef],
    ) -> DataFusionResult<()> {
        use ActionNeeded::*;

        if !other_arrs.is_empty() {
            return Err(DataFusionError::NotImplemented(
                "selector min w/ additional args".to_string(),
            ));
        }

        let mut min_accu = MinAccumulator::try_new(value_arr.data_type())?;
        min_accu.update_batch(&[Arc::clone(value_arr)])?;
        let cur_min_value = min_accu.evaluate()?;

        let action_needed = match (self.value.is_null(), cur_min_value.is_null()) {
            (false, false) => {
                if cur_min_value < self.value {
                    // new minimim found
                    UpdateValueAndTime
                } else if cur_min_value == self.value {
                    // same minimum found, time might need update
                    UpdateTime
                } else {
                    Nothing
                }
            }
            // No existing minimum time, so update needed
            (true, false) => UpdateValueAndTime,
            // No actual minimum time  found, so no update needed
            (_, true) => Nothing,
        };

        if action_needed.update_value() {
            self.value = cur_min_value;
            self.time = None; // ignore time associated with old value
        }

        if action_needed.update_time() {
            // only keep values where we've found our current value.
            // Note: We MUST also mask-out NULLs in `value_arr`, otherwise we may easily select that!
            let time_arr = arrow::compute::nullif(
                time_arr,
                &arrow::compute::neq_dyn(&self.value.to_array_of_size(time_arr.len()), &value_arr)?,
            )?;
            let time_arr =
                arrow::compute::nullif(&time_arr, &arrow::compute::is_null(&value_arr)?)?;

            let time_arr = time_arr
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                // the input type arguments should be ensured by datafusion
                .expect("Second argument was time");
            self.time = match (array_min(time_arr), self.time) {
                (Some(x), Some(y)) if x < y => Some(x),
                (Some(_), Some(x)) => Some(x),
                (None, Some(x)) => Some(x),
                (Some(x), None) => Some(x),
                (None, None) => None,
            };
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.value) + self.value.size()
    }
}

#[derive(Debug)]
pub struct MaxSelector {
    value: ScalarValue,
    time: Option<i64>,
}

impl MaxSelector {
    pub fn new(data_type: &DataType) -> DataFusionResult<Self> {
        Ok(Self {
            value: ScalarValue::try_from(data_type)?,
            time: None,
        })
    }
}

impl Selector for MaxSelector {
    fn datafusion_state(&self) -> DataFusionResult<Vec<ScalarValue>> {
        Ok(vec![
            self.value.clone(),
            ScalarValue::TimestampNanosecond(self.time, None),
        ])
    }

    fn evaluate(&self) -> DataFusionResult<ScalarValue> {
        Ok(make_struct_scalar(
            &self.value,
            &ScalarValue::TimestampNanosecond(self.time, None),
            [],
        ))
    }

    fn update_batch(
        &mut self,
        value_arr: &ArrayRef,
        time_arr: &ArrayRef,
        other_arrs: &[ArrayRef],
    ) -> DataFusionResult<()> {
        use ActionNeeded::*;

        if !other_arrs.is_empty() {
            return Err(DataFusionError::NotImplemented(
                "selector max w/ additional args".to_string(),
            ));
        }

        let time_arr = time_arr
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            // the input type arguments should be ensured by datafusion
            .expect("Second argument was time");

        let mut max_accu = MaxAccumulator::try_new(value_arr.data_type())?;
        max_accu.update_batch(&[Arc::clone(value_arr)])?;
        let cur_max_value = max_accu.evaluate()?;

        let action_needed = match (&self.value.is_null(), &cur_max_value.is_null()) {
            (false, false) => {
                if self.value < cur_max_value {
                    // new maximum found
                    UpdateValueAndTime
                } else if cur_max_value == self.value {
                    // same maximum found, time might need update
                    UpdateTime
                } else {
                    Nothing
                }
            }
            // No existing maxmimum value, so update needed
            (true, false) => UpdateValueAndTime,
            // No actual maximum value found, so no update needed
            (_, true) => Nothing,
        };

        if action_needed.update_value() {
            self.value = cur_max_value;
            self.time = None; // ignore time associated with old value
        }

        // Note even though we are computing the MAX value,
        // the timestamp returned is the one with the *lowest*
        // numerical value
        if action_needed.update_time() {
            // only keep values where we've found our current value.
            // Note: We MUST also mask-out NULLs in `value_arr`, otherwise we may easily select that!
            let time_arr = arrow::compute::nullif(
                time_arr,
                &arrow::compute::neq_dyn(&self.value.to_array_of_size(time_arr.len()), &value_arr)?,
            )?;
            let time_arr =
                arrow::compute::nullif(&time_arr, &arrow::compute::is_null(&value_arr)?)?;

            let time_arr = time_arr
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                // the input type arguments should be ensured by datafusion
                .expect("Second argument was time");

            // Note: we still use the MINIMUM timestamp here even though this is the max VALUE aggregator.
            self.time = match (array_min(time_arr), self.time) {
                (Some(x), Some(y)) if x < y => Some(x),
                (Some(_), Some(x)) => Some(x),
                (None, Some(x)) => Some(x),
                (Some(x), None) => Some(x),
                (None, None) => None,
            };
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.value) + self.value.size()
    }
}
