use arrow::array::Array;
use arrow::compute::kernels::cmp::lt;
use arrow::compute::nullif;
use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::window_state::WindowAggState;
use datafusion::logical_expr::{PartitionEvaluator, Signature, WindowUDFImpl};
use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

/// Wrap a WindowUDF so that all values are non-negative.

#[derive(Debug)]
pub(super) struct NonNegativeUDWF<U: WindowUDFImpl> {
    name: String,
    inner: U,
}

impl<U: WindowUDFImpl> NonNegativeUDWF<U> {
    pub(super) fn new(name: impl Into<String>, inner: U) -> Self {
        Self {
            name: name.into(),
            inner,
        }
    }
}

impl<U: WindowUDFImpl + 'static> WindowUDFImpl for NonNegativeUDWF<U> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn partition_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(NonNegative {
            partition_evaluator: self.inner.partition_evaluator()?,
        }))
    }
}

/// Wraps an existing [`PartitionEvaluator`] and ensures that all values are
/// non-negative.
#[derive(Debug)]
struct NonNegative {
    partition_evaluator: Box<dyn PartitionEvaluator>,
}
impl PartitionEvaluator for NonNegative {
    fn memoize(&mut self, state: &mut WindowAggState) -> Result<()> {
        self.partition_evaluator.memoize(state)
    }

    fn get_range(&self, idx: usize, n_rows: usize) -> Result<Range<usize>> {
        self.partition_evaluator.get_range(idx, n_rows)
    }

    fn evaluate_all(
        &mut self,
        values: &[Arc<dyn Array>],
        num_rows: usize,
    ) -> Result<Arc<dyn Array>> {
        let array = self.partition_evaluator.evaluate_all(values, num_rows)?;
        let zero = ScalarValue::new_zero(array.data_type())?;
        let predicate = lt(&array, &zero.to_scalar()?)?;
        Ok(nullif(&array, &predicate)?)
    }

    fn evaluate(&mut self, values: &[Arc<dyn Array>], range: &Range<usize>) -> Result<ScalarValue> {
        let value = self.partition_evaluator.evaluate(values, range)?;
        Ok(match value {
            ScalarValue::Float64(Some(v)) if v < 0.0 => ScalarValue::Float64(None),
            ScalarValue::Int64(Some(v)) if v < 0 => ScalarValue::Int64(None),
            v => v,
        })
    }

    fn evaluate_all_with_rank(
        &self,
        num_rows: usize,
        ranks_in_partition: &[Range<usize>],
    ) -> Result<Arc<dyn Array>> {
        let array = self
            .partition_evaluator
            .evaluate_all_with_rank(num_rows, ranks_in_partition)?;

        let zero = ScalarValue::new_zero(array.data_type())?;
        let predicate = lt(&array, &zero.to_scalar()?)?;
        Ok(nullif(&array, &predicate)?)
    }

    fn supports_bounded_execution(&self) -> bool {
        self.partition_evaluator.supports_bounded_execution()
    }

    fn uses_window_frame(&self) -> bool {
        self.partition_evaluator.uses_window_frame()
    }

    fn include_rank(&self) -> bool {
        self.partition_evaluator.include_rank()
    }
}
