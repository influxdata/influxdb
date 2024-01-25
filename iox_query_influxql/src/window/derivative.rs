use crate::{error, NUMERICS};
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDFImpl, TIMEZONE_WILDCARD,
};
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct DerivativeUDWF {
    signature: Signature,
}

impl DerivativeUDWF {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::one_of(
                NUMERICS
                    .iter()
                    .flat_map(|dt| {
                        [
                            TypeSignature::Exact(vec![
                                dt.clone(),
                                DataType::Duration(TimeUnit::Nanosecond),
                                DataType::Timestamp(TimeUnit::Nanosecond, None),
                            ]),
                            TypeSignature::Exact(vec![
                                dt.clone(),
                                DataType::Duration(TimeUnit::Nanosecond),
                                DataType::Timestamp(
                                    TimeUnit::Nanosecond,
                                    Some(TIMEZONE_WILDCARD.into()),
                                ),
                            ]),
                        ]
                    })
                    .collect(),
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for DerivativeUDWF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "derivative"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn partition_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(DifferencePartitionEvaluator {}))
    }
}

/// PartitionEvaluator which returns the derivative between input values,
/// in the provided units.
#[derive(Debug)]
struct DifferencePartitionEvaluator {}

impl PartitionEvaluator for DifferencePartitionEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> Result<Arc<dyn Array>> {
        assert_eq!(values.len(), 3);

        let array = Arc::clone(&values[0]);
        let times = Arc::clone(&values[2]);

        // The second element of the values array is the second argument to
        // the 'derivative' function. This specifies the unit duration for the
        // derivation to use.
        //
        // INVARIANT:
        // The planner guarantees that the second argument is always a duration
        // literal.
        let unit = ScalarValue::try_from_array(&values[1], 0)?;

        let mut idx: usize = 0;
        let mut last: ScalarValue = array.data_type().try_into()?;
        let mut last_time: ScalarValue = times.data_type().try_into()?;
        let mut derivative: Vec<ScalarValue> = vec![];

        while idx < array.len() {
            last = ScalarValue::try_from_array(&array, idx)?;
            last_time = ScalarValue::try_from_array(&times, idx)?;
            derivative.push(ScalarValue::Float64(None));
            idx += 1;
            if !last.is_null() {
                break;
            }
        }
        while idx < array.len() {
            let v = ScalarValue::try_from_array(&array, idx)?;
            let t = ScalarValue::try_from_array(&times, idx)?;
            if v.is_null() {
                derivative.push(ScalarValue::Float64(None));
            } else {
                derivative.push(ScalarValue::Float64(Some(
                    delta(&v, &last)? / delta_time(&t, &last_time, &unit)?,
                )));
                last = v.clone();
                last_time = t.clone();
            }
            idx += 1;
        }
        Ok(Arc::new(ScalarValue::iter_to_array(derivative)?))
    }

    fn uses_window_frame(&self) -> bool {
        false
    }

    fn include_rank(&self) -> bool {
        false
    }
}

fn delta(curr: &ScalarValue, prev: &ScalarValue) -> Result<f64> {
    match (curr, prev) {
        (ScalarValue::Float64(Some(curr)), ScalarValue::Float64(Some(prev))) => Ok(*curr - *prev),
        (ScalarValue::Int64(Some(curr)), ScalarValue::Int64(Some(prev))) => {
            Ok(*curr as f64 - *prev as f64)
        }
        (ScalarValue::UInt64(Some(curr)), ScalarValue::UInt64(Some(prev))) => {
            Ok(*curr as f64 - *prev as f64)
        }
        _ => error::internal("derivative attempted on unsupported values"),
    }
}

fn delta_time(curr: &ScalarValue, prev: &ScalarValue, unit: &ScalarValue) -> Result<f64> {
    if let (
        ScalarValue::TimestampNanosecond(Some(curr), _),
        ScalarValue::TimestampNanosecond(Some(prev), _),
        ScalarValue::IntervalMonthDayNano(Some(unit)),
    ) = (curr, prev, unit)
    {
        Ok((*curr as f64 - *prev as f64) / *unit as f64)
    } else {
        error::internal("derivative attempted on unsupported values")
    }
}
