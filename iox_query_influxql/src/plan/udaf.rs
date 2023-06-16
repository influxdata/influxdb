use crate::plan::error;
use arrow::array::{Array, ArrayRef, Int64Array, UInt64Array};
use arrow::datatypes::DataType;
use datafusion::common::{downcast_value, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    Accumulator, AccumulatorFunctionImplementation, AggregateUDF, ReturnTypeFunction, Signature,
    StateTypeFunction, TypeSignature, Volatility,
};
use datafusion::physical_expr::expressions::AvgAccumulator;
use once_cell::sync::Lazy;
use std::iter;
use std::sync::Arc;

pub(crate) const AVG_N_NAME: &str = "avg_n";

pub(crate) static AVG_N: Lazy<Arc<AggregateUDF>> = Lazy::new(|| {
    let rt_func: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Float64)));
    let accumulator: AccumulatorFunctionImplementation =
        Arc::new(|_| Ok(Box::new(AvgNAcc::try_new(&DataType::Float64))));
    // State is count, sum, N
    let st_func: StateTypeFunction = Arc::new(move |_| {
        Ok(Arc::new(vec![
            DataType::UInt64,
            DataType::Float64,
            DataType::UInt64,
        ]))
    });

    let udaf = AggregateUDF::new(
        AVG_N_NAME,
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Float64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::UInt64, DataType::Int64]),
            ],
            Volatility::Immutable,
        ),
        &rt_func,
        &accumulator,
        &st_func,
    );

    Arc::new(udaf)
});

#[derive(Debug)]
struct AvgNAcc {
    data_type: DataType,
    n: usize,
    i: usize,
    last_is_null: bool,
    all_values: Vec<ScalarValue>,
}

impl AvgNAcc {
    /// Creates a try_new `AvgNAcc`
    pub fn try_new(datatype: &DataType) -> Self {
        Self {
            data_type: datatype.clone(),
            n: 0,
            i: 0,
            last_is_null: true,
            all_values: vec![],
        }
    }
}

impl Accumulator for AvgNAcc {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let state = ScalarValue::new_list(Some(self.all_values.clone()), self.data_type.clone());
        Ok(vec![state])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq!(values.len(), 2);
        let array = &values[0];

        // the second element of the values array is the second argument to the `AVG_N` function,
        // which specifies the minimum number of values that must be aggregated.
        let n_values = downcast_value!(&values[1], Int64Array);
        let n = n_values.value(0) as usize;
        // first observation of the second argument N
        if self.n == 0 {
            assert!(self.all_values.is_empty());
            self.n = n;
            self.all_values = vec![ScalarValue::try_from(&self.data_type)?; n];
        } else if self.n != n {
            return Err(DataFusionError::External("N is not constant".into()));
        }

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

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        todo!();
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        // if any values are NULL return a null result
        if self.last_is_null || self.all_values.iter().any(|v| v.is_null()) {
            return ScalarValue::try_from(&self.data_type);
        }

        let sum: ScalarValue = self
            .all_values
            .iter()
            .cloned()
            .reduce(|acc, v| acc.add(v).unwrap())
            .unwrap();

        let n = self.n as f64;
        Ok(match sum {
            ScalarValue::Float64(Some(v)) => ScalarValue::from(v / n),
            ScalarValue::Int64(Some(v)) => ScalarValue::from(v as f64 / n),
            ScalarValue::UInt64(Some(v)) => ScalarValue::from(v as f64 / n),
            // TODO(sgc): This should return an error
            _ => ScalarValue::try_from(&self.data_type).unwrap(),
        })
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + ScalarValue::size_of_vec(&self.all_values)
            - std::mem::size_of_val(&self.all_values)
            + self.data_type.size()
            - std::mem::size_of_val(&self.data_type)
    }
}

/// An accumulator to compute the average
#[derive(Debug)]
struct AvgNAccumulator {
    acc: AvgAccumulator,
    n: u64,
}

impl AvgNAccumulator {
    /// Creates a try_new `AvgNAccumulator`
    pub fn try_new(datatype: &DataType, return_data_type: &DataType) -> Self {
        Self {
            acc: AvgAccumulator::try_new(datatype, return_data_type).unwrap(),
            n: 0,
        }
    }
}

impl Accumulator for AvgNAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(self
            .acc
            .state()?
            .into_iter()
            .chain(iter::once(ScalarValue::UInt64(Some(self.n))))
            .collect())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let col_values = &values[0];
        let col_len = col_values.len();
        let col_nulls = col_values.null_count();

        // the second element of the values array is the second argument to the `AVG_N` function,
        // which specifies the minimum number of values that must be aggregated.
        let n_values = downcast_value!(&values[1], Int64Array);
        let n = n_values.value(0);
        if self.n > 0 && self.n != n as u64 {
            Err(DataFusionError::External("N is not constant".into()))
        } else {
            self.n = n as u64;
            self.acc.update_batch(&[Arc::clone(col_values)])
        }
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        self.acc.retract_batch(&[Arc::clone(values)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let n = downcast_value!(states[2], UInt64Array);
        // fetch N from one
        self.n = *n.values().get(0).unwrap_or(&0);
        self.acc.merge_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let state = self.state()?;
        // check it has accumulated at least N values, otherwise return NULL
        match state.get(0) {
            Some(ScalarValue::UInt64(Some(count))) if *count < self.n => {
                Ok(ScalarValue::Float64(None))
            }
            Some(ScalarValue::UInt64(None)) | None => Ok(ScalarValue::Float64(None)),
            Some(_) => self.acc.evaluate(),
        }
    }

    fn size(&self) -> usize {
        self.acc.size()
    }
}

pub(crate) const DIFFERENCE_NAME: &str = "difference";

pub(crate) static DIFFERENCE: Lazy<Arc<AggregateUDF>> = Lazy::new(|| {
    let rt_func: ReturnTypeFunction = Arc::new(move |dt| Ok(Arc::new(dt[0].clone())));
    let accumulator: AccumulatorFunctionImplementation =
        Arc::new(|dt| Ok(Box::new(DifferenceAccumulator::try_new(dt)?)));
    let st_func: StateTypeFunction = Arc::new(move |dt| Ok(Arc::new(vec![dt.clone(), dt.clone()])));

    let udaf = AggregateUDF::new(
        DIFFERENCE_NAME,
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Float64]),
                TypeSignature::Exact(vec![DataType::UInt64]),
            ],
            Volatility::Immutable,
        ),
        &rt_func,
        &accumulator,
        &st_func,
    );

    Arc::new(udaf)
});

#[derive(Debug)]
struct DifferenceAccumulator {
    data_type: DataType,
    last: ScalarValue,
    diff: ScalarValue,
}

impl DifferenceAccumulator {
    fn try_new(data_type: &DataType) -> Result<Self> {
        let last: ScalarValue = data_type.try_into()?;
        let diff = last.clone();
        Ok(Self {
            data_type: data_type.clone(),
            last,
            diff,
        })
    }
}

impl Accumulator for DifferenceAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.last.clone(), self.diff.clone()])
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
        // This API is not called for difference
        error::not_implemented("DifferenceAccumulator::merge_batch")
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.diff.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}
