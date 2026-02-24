use crate::NUMERICS;
use arrow::array::{Array, ArrayRef};
use arrow::compute::kernels::numeric::sub_wrapping;
use arrow::compute::shift;
use arrow::datatypes::{Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDFImpl,
};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct DifferenceUDWF {
    signature: Signature,
}

impl DifferenceUDWF {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::one_of(
                NUMERICS
                    .iter()
                    .map(|dt| TypeSignature::Exact(vec![dt.clone()]))
                    .collect(),
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for DifferenceUDWF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "difference"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn field(&self, field_args: WindowUDFFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            field_args.input_fields()[0].data_type().clone(),
            true,
        )))
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs<'_>,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(DifferencePartitionEvaluator {}))
    }
}

/// PartitionEvaluator which returns the difference between input values.
#[derive(Debug)]
struct DifferencePartitionEvaluator {}

impl PartitionEvaluator for DifferencePartitionEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> Result<Arc<dyn Array>> {
        assert_eq!(values.len(), 1);

        window_difference(Arc::clone(&values[0]))
    }

    fn uses_window_frame(&self) -> bool {
        false
    }

    fn include_rank(&self) -> bool {
        false
    }
}

pub(crate) fn window_difference(array: ArrayRef) -> Result<Arc<dyn Array>> {
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
