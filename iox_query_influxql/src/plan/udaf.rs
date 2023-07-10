use crate::plan::error;
use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{downcast_value, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    Accumulator, AccumulatorFactoryFunction, AggregateUDF, ReturnTypeFunction, Signature,
    StateTypeFunction, TypeSignature, Volatility,
};
use once_cell::sync::Lazy;
use std::mem::replace;
use std::sync::Arc;

/// A list of the numeric types supported by InfluxQL that can be be used
/// as input to user-defined aggregate functions.
pub(crate) static NUMERICS: &[DataType] = &[DataType::Int64, DataType::UInt64, DataType::Float64];

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

/// Name of the `DIFFERENCE` user-defined aggregate function.
pub(crate) const DIFFERENCE_NAME: &str = "difference";

/// Definition of the `DIFFERENCE` user-defined aggregate function.
pub(crate) static DIFFERENCE: Lazy<Arc<AggregateUDF>> = Lazy::new(|| {
    let return_type: ReturnTypeFunction = Arc::new(|dt| Ok(Arc::new(dt[0].clone())));
    let accumulator: AccumulatorFactoryFunction =
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
    /// `state` is only called when used as an aggregate function. It can be
    /// can safely left unimplemented, as this accumulator is only used as a window aggregate.
    ///
    /// See: <https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.Accumulator.html#tymethod.state>
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

    /// `merge_batch` is only called when used as an aggregate function. It can be
    /// can safely left unimplemented, as this accumulator is only used as a window aggregate.
    ///
    /// See: <https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.Accumulator.html#tymethod.state>
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
    let accumulator: AccumulatorFactoryFunction = Arc::new(|dt| {
        Ok(Box::new(NonNegative::<_>::new(DifferenceAccumulator::new(
            dt,
        ))))
    });
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

/// NonNegative is a wrapper around an Accumulator that transposes
/// negative value to be NULL.
#[derive(Debug)]
struct NonNegative<T> {
    acc: T,
}

impl<T> NonNegative<T> {
    fn new(acc: T) -> Self {
        Self { acc }
    }
}

impl<T: Accumulator> Accumulator for NonNegative<T> {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        self.acc.state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.acc.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.acc.merge_batch(states)
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

/// Name of the `DERIVATIVE` user-defined aggregate function.
pub(crate) const DERIVATIVE_NAME: &str = "derivative";

pub(crate) fn derivative_udf(unit: i64) -> AggregateUDF {
    let return_type: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Float64)));
    let accumulator: AccumulatorFactoryFunction =
        Arc::new(move |_| Ok(Box::new(DerivativeAccumulator::new(unit))));
    let state_type: StateTypeFunction = Arc::new(|_| Ok(Arc::new(vec![])));
    let sig = Signature::one_of(
        NUMERICS
            .iter()
            .map(|dt| {
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    dt.clone(),
                ])
            })
            .collect(),
        Volatility::Immutable,
    );
    AggregateUDF::new(
        format!("{DERIVATIVE_NAME}(unit: {unit})").as_str(),
        &sig,
        &return_type,
        &accumulator,
        // State shouldn't be called, so no schema to report
        &state_type,
    )
}

/// Name of the `NON_NEGATIVE_DERIVATIVE` user-defined aggregate function.
pub(crate) const NON_NEGATIVE_DERIVATIVE_NAME: &str = "non_negative_derivative";

pub(crate) fn non_negative_derivative_udf(unit: i64) -> AggregateUDF {
    let return_type: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Float64)));
    let accumulator: AccumulatorFactoryFunction = Arc::new(move |_| {
        Ok(Box::new(NonNegative::<_>::new(DerivativeAccumulator::new(
            unit,
        ))))
    });
    let state_type: StateTypeFunction = Arc::new(|_| Ok(Arc::new(vec![])));
    let sig = Signature::one_of(
        NUMERICS
            .iter()
            .map(|dt| {
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    dt.clone(),
                ])
            })
            .collect(),
        Volatility::Immutable,
    );
    AggregateUDF::new(
        format!("{NON_NEGATIVE_DERIVATIVE_NAME}(unit: {unit})").as_str(),
        &sig,
        &return_type,
        &accumulator,
        // State shouldn't be called, so no schema to report
        &state_type,
    )
}

#[derive(Debug)]
struct DerivativeAccumulator {
    unit: i64,
    prev: Option<Point>,
    curr: Option<Point>,
}

impl DerivativeAccumulator {
    fn new(unit: i64) -> Self {
        Self {
            unit,
            prev: None,
            curr: None,
        }
    }
}

impl Accumulator for DerivativeAccumulator {
    /// `state` is only called when used as an aggregate function. It can be
    /// can safely left unimplemented, as this accumulator is only used as a window aggregate.
    ///
    /// See: <https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.Accumulator.html#tymethod.state>
    fn state(&self) -> Result<Vec<ScalarValue>> {
        error::internal("unexpected call to DerivativeAccumulator::state")
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let times = &values[0];
        let arr = &values[1];
        for index in 0..arr.len() {
            let time = match ScalarValue::try_from_array(times, index)? {
                ScalarValue::TimestampNanosecond(Some(ts), _) => ts,
                v => {
                    return Err(DataFusionError::Internal(format!(
                        "invalid time value: {}",
                        v
                    )))
                }
            };
            let curr = Point::new(time, ScalarValue::try_from_array(arr, index)?);
            let prev = replace(&mut self.curr, curr);

            // don't replace the previous value if the current value has the same timestamp.
            if self.prev.is_none()
                || prev
                    .as_ref()
                    .is_some_and(|prev| prev.time > self.prev.as_ref().unwrap().time)
            {
                self.prev = prev
            }
        }
        Ok(())
    }

    /// `merge_batch` is only called when used as an aggregate function. It can be
    /// can safely left unimplemented, as this accumulator is only used as a window aggregate.
    ///
    /// See: <https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.Accumulator.html#tymethod.state>
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        error::internal("unexpected call to DerivativeAccumulator::merge_batch")
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(
            self.curr
                .as_ref()
                .and_then(|c| c.derivative(self.prev.as_ref(), self.unit)),
        ))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

#[derive(Debug)]
struct Point {
    time: i64,
    value: ScalarValue,
}

impl Point {
    fn new(time: i64, value: ScalarValue) -> Option<Self> {
        if value.is_null() {
            None
        } else {
            Some(Self { time, value })
        }
    }

    fn value_as_f64(&self) -> f64 {
        match self.value {
            ScalarValue::Int64(Some(v)) => v as f64,
            ScalarValue::Float64(Some(v)) => v,
            ScalarValue::UInt64(Some(v)) => v as f64,
            _ => panic!("invalid point {:?}", self),
        }
    }

    fn derivative(&self, prev: Option<&Self>, unit: i64) -> Option<f64> {
        prev.and_then(|prev| {
            let diff = self.value_as_f64() - prev.value_as_f64();
            let elapsed = match self.time - prev.time {
                // if the time hasn't changed then it is a NULL.
                0 => return None,
                v => v,
            } as f64;
            let devisor = elapsed / (unit as f64);
            Some(diff / devisor)
        })
    }
}
