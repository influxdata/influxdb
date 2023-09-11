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

/// How to compare values/time.
#[derive(Debug, Clone, Copy)]
pub enum Comparison {
    Min,
    Max,
}

impl Comparison {
    fn is_update<T>(&self, old: &T, new: &T) -> bool
    where
        T: PartialOrd,
    {
        match self {
            Self::Min => new < old,
            Self::Max => old < new,
        }
    }
}

/// What to compare?
#[derive(Debug, Clone, Copy)]
pub enum Target {
    Time,
    Value,
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

/// Common state implementation for different selectors.
#[derive(Debug)]
pub struct Selector {
    comp: Comparison,
    target: Target,
    value: ScalarValue,
    time: Option<i64>,
    other: Box<[ScalarValue]>,
}

impl Selector {
    pub fn new<'a>(
        comp: Comparison,
        target: Target,
        data_type: &'a DataType,
        other_types: impl IntoIterator<Item = &'a DataType>,
    ) -> DataFusionResult<Self> {
        Ok(Self {
            comp,
            target,
            value: ScalarValue::try_from(data_type)?,
            time: None,
            other: other_types
                .into_iter()
                .map(ScalarValue::try_from)
                .collect::<DataFusionResult<_>>()?,
        })
    }

    fn update_time_based(
        &mut self,
        value_arr: &ArrayRef,
        time_arr: &ArrayRef,
        other_arrs: &[ArrayRef],
    ) -> DataFusionResult<()> {
        let time_arr = arrow::compute::nullif(time_arr, &arrow::compute::is_null(&value_arr)?)?;

        let time_arr = time_arr
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            // the input type arguments should be ensured by datafusion
            .expect("Second argument was time");
        let cur_time = match self.comp {
            Comparison::Min => array_min(time_arr),
            Comparison::Max => array_max(time_arr),
        };

        let need_update = match (&self.time, &cur_time) {
            (Some(time), Some(cur_time)) => self.comp.is_update(time, cur_time),
            // No existing min/max, so update needed
            (None, Some(_)) => true,
            // No actual min/max time found, so no update needed
            (_, None) => false,
        };

        if need_update {
            let index = time_arr
                .iter()
                // arrow doesn't tell us what index had the
                // min/max, so need to find it ourselves
                .enumerate()
                .filter(|(_, time)| cur_time == *time)
                .map(|(idx, _)| idx)
                // break tie: favor first value
                .next()
                .unwrap(); // value always exists

            // update all or nothing in case of an error
            let value_new = ScalarValue::try_from_array(&value_arr, index)?;
            let other_new = other_arrs
                .iter()
                .map(|arr| ScalarValue::try_from_array(arr, index))
                .collect::<DataFusionResult<_>>()?;

            self.time = cur_time;
            self.value = value_new;
            self.other = other_new;
        }

        Ok(())
    }

    fn update_value_based(
        &mut self,
        value_arr: &ArrayRef,
        time_arr: &ArrayRef,
        other_arrs: &[ArrayRef],
    ) -> DataFusionResult<()> {
        use ActionNeeded::*;

        let cur_value = match self.comp {
            Comparison::Min => {
                let mut min_accu = MinAccumulator::try_new(value_arr.data_type())?;
                min_accu.update_batch(&[Arc::clone(value_arr)])?;
                min_accu.evaluate()?
            }
            Comparison::Max => {
                let mut max_accu = MaxAccumulator::try_new(value_arr.data_type())?;
                max_accu.update_batch(&[Arc::clone(value_arr)])?;
                max_accu.evaluate()?
            }
        };

        let action_needed = match (&self.value.is_null(), &cur_value.is_null()) {
            (false, false) => {
                if self.comp.is_update(&self.value, &cur_value) {
                    // new min/max found
                    UpdateValueAndTime
                } else if cur_value == self.value {
                    // same maximum found, time might need update
                    UpdateTime
                } else {
                    Nothing
                }
            }
            // No existing min/max value, so update needed
            (true, false) => UpdateValueAndTime,
            // No actual min/max value found, so no update needed
            (_, true) => Nothing,
        };

        if action_needed.update_value() {
            self.value = cur_value;
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
                &arrow::compute::kernels::cmp::neq(
                    &self.value.to_array_of_size(time_arr.len()),
                    &value_arr,
                )?,
            )?;
            let time_arr =
                arrow::compute::nullif(&time_arr, &arrow::compute::is_null(&value_arr)?)?;

            let time_arr = time_arr
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                // the input type arguments should be ensured by datafusion
                .expect("Second argument was time");

            // Note: we still use the MINIMUM timestamp here even if this is the max VALUE aggregator.
            let found_new_time = match (array_min(time_arr), self.time) {
                (Some(x), Some(y)) => {
                    if x < y {
                        self.time = Some(x);
                        true
                    } else {
                        false
                    }
                }
                (Some(x), None) => {
                    self.time = Some(x);
                    true
                }
                (None, _) => false,
            };

            // update other if required
            if found_new_time && !self.other.is_empty() {
                let index = time_arr
                    .iter()
                    // arrow doesn't tell us what index had the
                    // minimum, so need to find it ourselves
                    .enumerate()
                    .filter(|(_, time)| self.time == *time)
                    .map(|(idx, _)| idx)
                    // break tie: favor first value
                    .next()
                    .unwrap(); // value always exists

                self.other = other_arrs
                    .iter()
                    .map(|arr| ScalarValue::try_from_array(arr, index))
                    .collect::<DataFusionResult<_>>()?;
            }
        }

        Ok(())
    }
}

impl Accumulator for Selector {
    fn state(&self) -> DataFusionResult<Vec<ScalarValue>> {
        Ok([
            self.value.clone(),
            ScalarValue::TimestampNanosecond(self.time, None),
        ]
        .into_iter()
        .chain(self.other.iter().cloned())
        .collect())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        if values.len() < 2 {
            return Err(DataFusionError::Internal(format!(
                "Internal error: Expected at least 2 arguments passed to selector function but got {}",
                values.len()
            )));
        }

        let value_arr = &values[0];
        let time_arr = &values[1];
        let other_arrs = &values[2..];

        match self.target {
            Target::Time => self.update_time_based(value_arr, time_arr, other_arrs)?,
            Target::Value => self.update_value_based(value_arr, time_arr, other_arrs)?,
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DataFusionResult<()> {
        // merge is the same operation as update for these selectors
        self.update_batch(states)
    }

    fn evaluate(&self) -> DataFusionResult<ScalarValue> {
        Ok(make_struct_scalar(
            &self.value,
            &ScalarValue::TimestampNanosecond(self.time, None),
            self.other.iter(),
        ))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.value)
            + self.value.size()
            + self.other.iter().map(|s| s.size()).sum::<usize>()
    }
}
