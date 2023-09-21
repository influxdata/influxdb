use arrow::array::Array;
use arrow::compute::kernels::cmp::lt;
use arrow::compute::nullif;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::window_state::WindowAggState;
use datafusion::logical_expr::PartitionEvaluator;
use std::ops::Range;
use std::sync::Arc;

/// Wrap a PartitionEvaluator in a non-negative filter.
pub(super) fn wrapper(
    partition_evaluator: Box<dyn PartitionEvaluator>,
) -> Box<dyn PartitionEvaluator> {
    Box::new(NonNegative {
        partition_evaluator,
    })
}

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
        let predicate = lt(&array, &zero.to_scalar())?;
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
        let predicate = lt(&array, &zero.to_scalar())?;
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
