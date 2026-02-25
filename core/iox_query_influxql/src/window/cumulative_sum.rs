use crate::NUMERICS;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDFImpl,
};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CumulativeSumUDWF {
    signature: Signature,
}

impl CumulativeSumUDWF {
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

impl WindowUDFImpl for CumulativeSumUDWF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "cumumlative_sum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs<'_>,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(CumulativeSumPartitionEvaluator {}))
    }

    fn field(&self, field_args: WindowUDFFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            field_args.input_fields()[0].data_type().clone(),
            true,
        )))
    }
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
