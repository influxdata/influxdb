use crate::NUMERICS;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{PartitionEvaluator, Signature, TypeSignature, Volatility};
use once_cell::sync::Lazy;
use std::sync::Arc;

/// The name of the cumulative_sum window function.
pub(super) const NAME: &str = "cumumlative_sum";

/// Valid signatures for the cumulative_sum window function.
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
    Ok(Box::new(CumulativeSumPartitionEvaluator {}))
}

/// PartitionEvaluator which returns the cumulative sum of the input.
#[derive(Debug)]
struct CumulativeSumPartitionEvaluator {}

impl PartitionEvaluator for CumulativeSumPartitionEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<Arc<dyn Array>> {
        assert_eq!(values.len(), 1);

        let array = Arc::clone(&values[0]);
        let mut sum = ScalarValue::new_zero(array.data_type())?;
        let mut cumulative: Vec<ScalarValue> = vec![];
        for idx in 0..num_rows {
            let v = ScalarValue::try_from_array(&array, idx)?;
            let res = if v.is_null() {
                v
            } else {
                sum = sum.add(&v)?;
                sum.clone()
            };
            cumulative.push(res);
        }
        Ok(Arc::new(ScalarValue::iter_to_array(cumulative)?))
    }

    fn uses_window_frame(&self) -> bool {
        false
    }

    fn include_rank(&self) -> bool {
        false
    }
}
