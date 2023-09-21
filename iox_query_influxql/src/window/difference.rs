use crate::NUMERICS;
use arrow::array::{Array, ArrayRef};
use arrow::compute::kernels::numeric::sub_wrapping;
use arrow::compute::shift;
use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{PartitionEvaluator, Signature, TypeSignature, Volatility};
use once_cell::sync::Lazy;
use std::sync::Arc;

/// The name of the difference window function.
pub(super) const NAME: &str = "difference";

/// Valid signatures for the difference window function.
pub(super) static SIGNATURE: Lazy<Signature> = Lazy::new(|| {
    Signature::one_of(
        NUMERICS
            .iter()
            .map(|dt| TypeSignature::Exact(vec![dt.clone()]))
            .collect(),
        Volatility::Immutable,
    )
});

/// Calculate the return type given the function signature.
pub(super) fn return_type(sig: &[DataType]) -> Result<Arc<DataType>> {
    Ok(Arc::new(sig[0].clone()))
}

/// Create a new partition_evaluator_factory.
pub(super) fn partition_evaluator_factory() -> Result<Box<dyn PartitionEvaluator>> {
    Ok(Box::new(DifferencePartitionEvaluator {}))
}

/// PartitionEvaluator which returns the difference between input values.
#[derive(Debug)]
struct DifferencePartitionEvaluator {}

impl PartitionEvaluator for DifferencePartitionEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> Result<Arc<dyn Array>> {
        assert_eq!(values.len(), 1);

        let array = Arc::clone(&values[0]);
        if array.null_count() == 0 {
            // If there are no gaps then use arrow kernels.
            Ok(sub_wrapping(&array, &shift(&array, 1)?)?)
        } else {
            let mut idx: usize = 0;
            let mut last: ScalarValue = array.data_type().try_into()?;
            let mut difference: Vec<ScalarValue> = vec![];
            while idx < array.len() {
                last = ScalarValue::try_from_array(&array, idx)?;
                difference.push(array.data_type().try_into()?);
                idx += 1;
                if !last.is_null() {
                    break;
                }
            }
            while idx < array.len() {
                let v = ScalarValue::try_from_array(&array, idx)?;
                if v.is_null() {
                    difference.push(array.data_type().try_into()?);
                } else {
                    difference.push(v.sub(last)?);
                    last = v;
                }
                idx += 1;
            }
            Ok(Arc::new(ScalarValue::iter_to_array(difference)?))
        }
    }

    fn uses_window_frame(&self) -> bool {
        false
    }

    fn include_rank(&self) -> bool {
        false
    }
}
