use crate::{error, NUMERICS};
use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use datafusion::common::{downcast_value, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    Accumulator, AccumulatorFactoryFunction, AggregateUDF, ReturnTypeFunction, Signature,
    StateTypeFunction, TypeSignature, Volatility,
};
use once_cell::sync::Lazy;
use std::sync::Arc;

/// Name of the `MOVING_AVERAGE` user-defined aggregate function.
pub(crate) const MOVING_AVERAGE_NAME: &str = "moving_average";

/// Definition of the `MOVING_AVERAGE` user-defined aggregate function.
pub(crate) static MOVING_AVERAGE: Lazy<Arc<AggregateUDF>> = Lazy::new(|| {
    let return_type: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Float64)));
    let accumulator: AccumulatorFactoryFunction =
        Arc::new(|_| Ok(Box::new(AvgNAccumulator::new(&DataType::Float64))));
    let state_type: StateTypeFunction = Arc::new(|_| Ok(Arc::new(vec![])));
    Arc::new(AggregateUDF::new(
        MOVING_AVERAGE_NAME,
        &Signature::one_of(
            NUMERICS
                .iter()
                .map(|dt| TypeSignature::Exact(vec![dt.clone(), DataType::Int64]))
                .collect(),
            Volatility::Immutable,
        ),
        &return_type,
        &accumulator,
        // State shouldn't be called, so no schema to report
        &state_type,
    ))
});

/// A moving average accumulator that accumulates exactly `N` values
/// before producing a non-null result.
#[derive(Debug)]
struct AvgNAccumulator {
    /// The data type of the values being accumulated in [`Self::all_values`].
    data_type: DataType,
    all_values: Vec<ScalarValue>,
    /// Holds the number of non-null values to be accumulated and represents
    /// the second argument to the UDAF.
    n: usize,
    /// The index into [`Self::all_values`] to store the next non-null value.
    i: usize,
    /// `true` if the last value observed was `NULL`
    last_is_null: bool,
}

impl AvgNAccumulator {
    /// Creates a new `AvgNAccumulator`
    fn new(datatype: &DataType) -> Self {
        Self {
            data_type: datatype.clone(),
            all_values: vec![],
            n: 0,
            i: 0,
            last_is_null: true,
        }
    }
}

impl Accumulator for AvgNAccumulator {
    /// `state` is only called when used as an aggregate function. It can be
    /// can safely left unimplemented, as this accumulator is only used as a window aggregate.
    ///
    /// See: <https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.Accumulator.html#tymethod.state>
    fn state(&self) -> Result<Vec<ScalarValue>> {
        error::internal("unexpected call to AvgNAccumulator::state")
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq!(values.len(), 2, "AVG_N expects two arguments");

        // The second element of the values array is the second argument to the `moving_average`
        // function, which specifies the minimum number of values that must be aggregated.
        //
        // INVARIANT:
        // The planner and rewriter guarantee that the second argument is
        // always a numeric constant.
        //
        // See: FieldChecker::check_moving_average
        let n_values = downcast_value!(&values[1], Int64Array);
        let n = n_values.value(0) as usize;
        // first observation of the second argument, N
        if self.n == 0 {
            assert!(self.all_values.is_empty());
            self.n = n;
            self.all_values = vec![ScalarValue::try_from(&self.data_type)?; n];
        }

        let array = &values[0];
        match array.len() {
            1 => {
                let value = ScalarValue::try_from_array(array, 0)?;
                self.last_is_null = value.is_null();
                if !self.last_is_null {
                    self.all_values[self.i] = value;
                    self.i = (self.i + 1) % self.n;
                }

                Ok(())
            }
            n => error::internal(format!(
                "AvgNAccumulator: unexpected number of rows: got {n}, expected 1"
            )),
        }
    }

    /// `merge_batch` is only called when used as an aggregate function. It can be
    /// can safely left unimplemented, as this accumulator is only used as a window aggregate.
    ///
    /// See: <https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.Accumulator.html#tymethod.state>
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        error::internal("unexpected call to AvgNAccumulator::merge_batch")
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        if self.last_is_null || self.all_values.iter().any(|v| v.is_null()) {
            return ScalarValue::try_from(&self.data_type);
        }

        let sum: ScalarValue = self
            .all_values
            .iter()
            .cloned()
            .reduce(|acc, v| acc.add(v).unwrap())
            // safe to unwrap, as all_values is known to contain only the same primitive values
            .unwrap();

        let n = self.n as f64;
        Ok(match sum {
            ScalarValue::Float64(Some(v)) => ScalarValue::from(v / n),
            ScalarValue::Int64(Some(v)) => ScalarValue::from(v as f64 / n),
            ScalarValue::UInt64(Some(v)) => ScalarValue::from(v as f64 / n),
            _ => return error::internal("unexpected scalar value type"),
        })
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + ScalarValue::size_of_vec(&self.all_values)
            - std::mem::size_of_val(&self.all_values)
            + self.data_type.size()
            - std::mem::size_of_val(&self.data_type)
    }
}
