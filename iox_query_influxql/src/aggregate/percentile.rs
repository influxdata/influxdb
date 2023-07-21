use crate::error;
use arrow::array::{as_list_array, Array, ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{downcast_value, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{Accumulator, Signature, TypeSignature, Volatility};
use once_cell::sync::Lazy;
use std::sync::Arc;

/// The name of the percentile aggregate function.
pub(super) const NAME: &str = "percentile";

/// Valid signatures for the percentile aggregate function.
pub(super) static SIGNATURE: Lazy<Signature> = Lazy::new(|| {
    Signature::one_of(
        crate::NUMERICS
            .iter()
            .flat_map(|dt| {
                [
                    TypeSignature::Exact(vec![dt.clone(), DataType::Int64]),
                    TypeSignature::Exact(vec![dt.clone(), DataType::Float64]),
                ]
            })
            .collect(),
        Volatility::Immutable,
    )
});

/// Calculate the return type given the function signature. Percentile
/// always returns the same type as the input column.
pub(super) fn return_type(signature: &[DataType]) -> Result<Arc<DataType>> {
    Ok(Arc::new(signature[0].clone()))
}

/// Create a new accumulator for the data type.
pub(super) fn accumulator(dt: &DataType) -> Result<Box<dyn Accumulator>> {
    Ok(Box::new(PercentileAccumulator::new(dt.clone())))
}

/// Calculate the intermediate merge state for the aggregator.
pub(super) fn state_type(dt: &DataType) -> Result<Arc<Vec<DataType>>> {
    Ok(Arc::new(vec![
        DataType::List(Arc::new(Field::new("item", dt.clone(), true))),
        DataType::Float64,
    ]))
}

#[derive(Debug)]
struct PercentileAccumulator {
    data_type: DataType,
    data: Vec<ScalarValue>,
    percentile: Option<f64>,
}

impl PercentileAccumulator {
    fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            data: vec![],
            percentile: None,
        }
    }

    fn update(&mut self, array: ArrayRef) -> Result<()> {
        let array = Arc::clone(&array);
        assert_eq!(array.data_type(), &self.data_type);

        let nulls = array.nulls();
        let null_len = nulls.map_or(0, |nb| nb.null_count());
        self.data.reserve(array.len() - null_len);
        for idx in 0..array.len() {
            if nulls.map_or(true, |nb| nb.is_valid(idx)) {
                self.data.push(ScalarValue::try_from_array(&array, idx)?)
            }
        }
        Ok(())
    }

    fn set_percentile(&mut self, array: ArrayRef) -> Result<()> {
        if self.percentile.is_none() {
            self.percentile = match array.data_type() {
                DataType::Int64 => Some(downcast_value!(array, Int64Array).value(0) as f64),
                DataType::Float64 => Some(downcast_value!(array, Float64Array).value(0)),
                dt => {
                    return error::internal(format!(
                        "invalid data type ({dt}) for PERCENTILE n argument"
                    ))
                }
            };
        }
        Ok(())
    }
}

impl Accumulator for PercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq!(values.len(), 2);

        self.set_percentile(Arc::clone(&values[1]))?;
        self.update(Arc::clone(&values[0]))
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let idx = self
            .percentile
            .and_then(|n| percentile_idx(self.data.len(), n));
        if idx.is_none() {
            return Ok(ScalarValue::Null);
        }

        let array = ScalarValue::iter_to_array(self.data.clone())?;
        let indices = arrow::compute::sort_to_indices(&array, None, None)?;
        let array_idx = indices.value(idx.unwrap());
        ScalarValue::try_from_array(&array, array_idx as usize)
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Option<f64>>()
            + std::mem::size_of::<DataType>()
            + ScalarValue::size_of_vec(&self.data)
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::new_list(Some(self.data.clone()), self.data_type.clone()),
            ScalarValue::Float64(self.percentile),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        assert_eq!(states.len(), 2);

        self.set_percentile(Arc::clone(&states[1]))?;

        let array = Arc::clone(&states[0]);
        let list_array = as_list_array(&array);
        for idx in 0..list_array.len() {
            self.update(list_array.value(idx))?;
        }
        Ok(())
    }
}

/// Calculate the location in an ordered list of len items where the
/// location of the item at the given percentile would be found.
///
/// This uses the same algorithm as the original influxdb implementation
/// of percentile as can be found in
/// <https://github.com/influxdata/influxdb/blob/75a8bcfae2af7b0043933be9f96b98c0741ceee3/influxql/query/call_iterator.go#L1087>.
fn percentile_idx(len: usize, percentile: f64) -> Option<usize> {
    match TryInto::<usize>::try_into(
        (((len as f64) * percentile / 100.0 + 0.5).floor() as isize) - 1,
    ) {
        Ok(idx) if idx < len => Some(idx),
        _ => None,
    }
}
