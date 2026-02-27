use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::AggregateUDFImpl;
use datafusion::logical_expr::function::StateFieldsArgs;
use datafusion::{
    logical_expr::{Signature, TypeSignature, Volatility, function::AccumulatorArgs},
    physical_plan::{Accumulator, expressions::format_state_name},
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct SpreadUDF {
    signature: Signature,
}

impl SpreadUDF {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::one_of(
                crate::NUMERICS
                    .iter()
                    .map(|dt| TypeSignature::Exact(vec![dt.clone()]))
                    .collect(),
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for SpreadUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "spread"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Calculate the return type given the function signature. Spread
    /// always returns the same type as the input column.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, arg: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SpreadAccumulator::new(
            arg.return_field.data_type().clone(),
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(args.name, "max"),
                args.return_field.data_type().clone(),
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "min"),
                args.return_field.data_type().clone(),
                true,
            )),
        ])
    }
}

enum Update {
    Min,
    Max,
    Both,
}

#[derive(Debug)]
struct SpreadAccumulator {
    data_type: DataType,
    min: ScalarValue,
    max: ScalarValue,
}

impl SpreadAccumulator {
    fn new(data_type: DataType) -> Result<Self> {
        let min = ScalarValue::try_from(&data_type)?;
        let max = ScalarValue::new_zero(&data_type)?;
        Ok(Self {
            data_type,
            min,
            max,
        })
    }

    fn update(&mut self, array: ArrayRef, update: Update) -> Result<()> {
        let array = Arc::clone(&array);
        assert_eq!(array.data_type(), &self.data_type);
        let nulls = array.nulls();
        for idx in 0..array.len() {
            if nulls.is_none_or(|nb| nb.is_valid(idx)) {
                let v = ScalarValue::try_from_array(&array, idx)?;
                match update {
                    Update::Min => self.maybe_set_min(v),
                    Update::Max => self.maybe_set_max(v),
                    Update::Both => {
                        self.maybe_set_min(v.clone());
                        self.maybe_set_max(v);
                    }
                }
            }
        }

        Ok(())
    }

    fn maybe_set_min(&mut self, v: ScalarValue) {
        if self.min.is_null() || v < self.min {
            self.min = v.clone()
        }
    }

    fn maybe_set_max(&mut self, v: ScalarValue) {
        if v > self.max {
            self.max = v
        }
    }

    fn spread(&self) -> Result<ScalarValue> {
        self.max.sub(&self.min)
    }
}

impl Accumulator for SpreadAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq!(values.len(), 1);

        self.update(Arc::clone(&values[0]), Update::Both)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.spread()
    }

    fn size(&self) -> usize {
        std::mem::size_of::<DataType>() + (2 * std::mem::size_of::<ScalarValue>())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let state = vec![self.min.clone(), self.max.clone()];
        Ok(state)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        assert_eq!(states.len(), 2);

        self.update(Arc::clone(&states[0]), Update::Min)?;
        self.update(Arc::clone(&states[1]), Update::Max)?;

        Ok(())
    }
}
