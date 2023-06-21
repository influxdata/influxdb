use crate::plan::error;
use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use datafusion::common::{downcast_value, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    Accumulator, AccumulatorFunctionImplementation, AggregateUDF, ReturnTypeFunction, Signature,
    StateTypeFunction, TypeSignature, Volatility,
};
use once_cell::sync::Lazy;
use std::sync::Arc;

/// A list of the numeric types supported by InfluxQL that can be be used
/// as input to user-defined aggregate functions.
static NUMERICS: &[DataType] = &[DataType::Int64, DataType::UInt64, DataType::Float64];

/// Name of the `MOVING_AVERAGE` user-defined aggregate function.
pub(crate) const MOVING_AVERAGE_NAME: &str = "moving_average";

/// Definition of the `MOVING_AVERAGE` user-defined aggregate function.
pub(crate) static MOVING_AVERAGE: Lazy<Arc<AggregateUDF>> = Lazy::new(|| {
    let return_type: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Float64)));
    let accumulator: AccumulatorFunctionImplementation =
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
    fn state(&self) -> Result<Vec<ScalarValue>> {
        error::internal("unexpected call to AvgNAccumulator::state")
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq!(values.len(), 2, "AVG_N expects two arguments");

        // The second element of the values array is the second argument to the `AVG_N` function,
        // which specifies the minimum number of values that must be aggregated.
        let n_values = downcast_value!(&values[1], Int64Array);
        let n = n_values.value(0) as usize;
        // first observation of the second argument, N
        if self.n == 0 {
            assert!(self.all_values.is_empty());
            self.n = n;
            self.all_values = vec![ScalarValue::try_from(&self.data_type)?; n];
        } else if self.n != n {
            return Err(DataFusionError::External(
                "AVG_N: N must be constant".into(),
            ));
        }

        let array = &values[0];
        assert!(array.len() < 2, "this accumulator should be used with an");
        for index in 0..array.len() {
            let value = ScalarValue::try_from_array(array, index)?;
            self.last_is_null = value.is_null();
            if !self.last_is_null {
                self.all_values[self.i] = value;
                self.i = (self.i + 1) % self.n;
            }
        }

        Ok(())
    }

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

/// Name of the `DIFFERENCE` user-defined aggregate function.
pub(crate) const DIFFERENCE_NAME: &str = "difference";

/// Definition of the `DIFFERENCE` user-defined aggregate function.
pub(crate) static DIFFERENCE: Lazy<Arc<AggregateUDF>> = Lazy::new(|| {
    let return_type: ReturnTypeFunction = Arc::new(|dt| Ok(Arc::new(dt[0].clone())));
    let accumulator: AccumulatorFunctionImplementation =
        Arc::new(|dt| Ok(Box::new(DifferenceAccumulator::new(dt))));
    let state_type: StateTypeFunction = Arc::new(|_| Ok(Arc::new(vec![])));
    Arc::new(AggregateUDF::new(
        DIFFERENCE_NAME,
        &Signature::one_of(
            NUMERICS
                .iter()
                .map(|dt| TypeSignature::Exact(vec![dt.clone()]))
                .collect(),
            Volatility::Immutable,
        ),
        &return_type,
        &accumulator,
        // State shouldn't be called, so no schema to report
        &state_type,
    ))
});

#[derive(Debug)]
struct DifferenceAccumulator {
    data_type: DataType,
    last: ScalarValue,
    diff: ScalarValue,
}

impl DifferenceAccumulator {
    fn new(data_type: &DataType) -> Self {
        let last: ScalarValue = data_type.try_into().expect("data_type → ScalarValue");
        let diff = last.clone();
        Self {
            data_type: data_type.clone(),
            last,
            diff,
        }
    }
}

impl Accumulator for DifferenceAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        error::internal("unexpected call to DifferenceAccumulator::state")
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        for index in 0..arr.len() {
            let scalar = ScalarValue::try_from_array(arr, index)?;
            if !scalar.is_null() {
                if !self.last.is_null() {
                    self.diff = scalar.sub(self.last.clone())?
                }
                self.last = scalar;
            } else {
                self.diff = ScalarValue::try_from(&self.data_type).unwrap()
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        error::internal("unexpected call to DifferenceAccumulator::merge_batch")
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.diff.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// Name of the `NON_NEGATIVE_DIFFERENCE` user-defined aggregate function.
pub(crate) const NON_NEGATIVE_DIFFERENCE_NAME: &str = "non_negative_difference";

/// Definition of the `NON_NEGATIVE_DIFFERENCE` user-defined aggregate function.
pub(crate) static NON_NEGATIVE_DIFFERENCE: Lazy<Arc<AggregateUDF>> = Lazy::new(|| {
    let return_type: ReturnTypeFunction = Arc::new(|dt| Ok(Arc::new(dt[0].clone())));
    let accumulator: AccumulatorFunctionImplementation =
        Arc::new(|dt| Ok(Box::new(NonNegativeDifferenceAccumulator::new(dt))));
    let state_type: StateTypeFunction = Arc::new(|_| Ok(Arc::new(vec![])));
    Arc::new(AggregateUDF::new(
        NON_NEGATIVE_DIFFERENCE_NAME,
        &Signature::one_of(
            NUMERICS
                .iter()
                .map(|dt| TypeSignature::Exact(vec![dt.clone()]))
                .collect(),
            Volatility::Immutable,
        ),
        &return_type,
        &accumulator,
        // State shouldn't be called, so no schema to report
        &state_type,
    ))
});

#[derive(Debug)]
struct NonNegativeDifferenceAccumulator {
    acc: DifferenceAccumulator,
}

impl NonNegativeDifferenceAccumulator {
    fn new(data_type: &DataType) -> Self {
        let acc = DifferenceAccumulator::new(data_type);
        Self { acc }
    }
}

impl Accumulator for NonNegativeDifferenceAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        error::internal("unexpected call to NonNegativeDifferenceAccumulator::state")
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.acc.update_batch(values)
    }

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        error::internal("unexpected call to NonNegativeDifferenceAccumulator::merge_batch")
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(match self.acc.evaluate()? {
            ScalarValue::Float64(Some(v)) if v < 0.0 => ScalarValue::Float64(None),
            ScalarValue::Int64(Some(v)) if v < 0 => ScalarValue::Int64(None),
            v => v,
        })
    }

    fn size(&self) -> usize {
        self.acc.size()
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_avg_n() {}
}
