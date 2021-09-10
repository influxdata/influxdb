//! Implementaton of InfluxDB "Selector" Functions
//!
//! Selector functions are similar to aggregate functions in that they
//! collapse down an input set of rows into just one.
//!
//! Selector functions are different than aggregate functions because
//! they also return multiple column values rather than a single
//! scalar. Selector functions return the entire row that was
//! "selected" from the timeseries (value and time pair).
//!
//! Note: At the time of writing, DataFusion aggregate functions have
//! no way to handle aggregates that produce multiple columns.
//!
//! This module implements a workaround of "do the aggregation twice
//! with two distinct functions" to get something working. It should
//! should be removed when DataFusion / Arrow has proper support
use std::{fmt::Debug, sync::Arc};

use arrow::{array::ArrayRef, datatypes::DataType};
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    physical_plan::{
        aggregates::{AccumulatorFunctionImplementation, StateTypeFunction},
        functions::{ReturnTypeFunction, Signature},
        udaf::AggregateUDF,
        Accumulator,
    },
    scalar::ScalarValue,
};

// Internal implementations of the selector functions
mod internal;
use internal::{
    BooleanFirstSelector, BooleanLastSelector, BooleanMaxSelector, BooleanMinSelector,
    F64FirstSelector, F64LastSelector, F64MaxSelector, F64MinSelector, I64FirstSelector,
    I64LastSelector, I64MaxSelector, I64MinSelector, Utf8FirstSelector, Utf8LastSelector,
    Utf8MaxSelector, Utf8MinSelector,
};
use internal_types::schema::TIME_DATA_TYPE;

/// Returns a DataFusion user defined aggregate function for computing
/// one field of the first() selector function.
///
/// Note that until <https://issues.apache.org/jira/browse/ARROW-10945>
/// is fixed, selector functions must be computed using two separate
/// function calls, one each for the value and time part
///
/// first(value_column, timestamp_column) -> value and timestamp
///
/// timestamp is the minimum value of the timestamp_column
///
/// value is the value of the value_column at the position of the
/// minimum of the timestamp column. If there are multiple rows with
/// the minimum timestamp value, the value of the value_column is
/// arbitrarily picked
pub fn selector_first(data_type: &DataType, output: SelectorOutput) -> AggregateUDF {
    let name = match output {
        SelectorOutput::Value => "selector_first_value",
        SelectorOutput::Time => "selector_first_time",
    };

    match data_type {
        DataType::Float64 => make_uda::<F64FirstSelector>(name, output),
        DataType::Int64 => make_uda::<I64FirstSelector>(name, output),
        DataType::Utf8 => make_uda::<Utf8FirstSelector>(name, output),
        DataType::Boolean => make_uda::<BooleanFirstSelector>(name, output),
        _ => unimplemented!("first not supported for {:?}", data_type),
    }
}

/// Returns a DataFusion user defined aggregate function for computing
/// one field of the last() selector function.
///
/// Note that until <https://issues.apache.org/jira/browse/ARROW-10945>
/// is fixed, selector functions must be computed using two separate
/// function calls, one each for the value and time part
///
/// selector_last(data_column, timestamp_column) -> value and timestamp
///
/// timestamp is the maximum value of the timestamp_column
///
/// value is the value of the data_column at the position of the
/// maximum of the timestamp column. If there are multiple rows with
/// the maximum timestamp value, the value of the data_column is
/// arbitrarily picked
pub fn selector_last(data_type: &DataType, output: SelectorOutput) -> AggregateUDF {
    let name = match output {
        SelectorOutput::Value => "selector_last_value",
        SelectorOutput::Time => "selector_last_time",
    };

    match data_type {
        DataType::Float64 => make_uda::<F64LastSelector>(name, output),
        DataType::Int64 => make_uda::<I64LastSelector>(name, output),
        DataType::Utf8 => make_uda::<Utf8LastSelector>(name, output),
        DataType::Boolean => make_uda::<BooleanLastSelector>(name, output),
        _ => unimplemented!("last not supported for {:?}", data_type),
    }
}

/// Returns a DataFusion user defined aggregate function for computing
/// one field of the min() selector function.
///
/// Note that until <https://issues.apache.org/jira/browse/ARROW-10945>
/// is fixed, selector functions must be computed using two separate
/// function calls, one each for the value and time part
///
/// selector_min(data_column, timestamp_column) -> value and timestamp
///
/// value is the minimum value of the data_column
///
/// timestamp is the value of the timestamp_column at the position of
/// the minimum value_column. If there are multiple rows with the
/// minimum timestamp value, the value of the data_column with the
/// first (earliest/smallest) timestamp is chosen
pub fn selector_min(data_type: &DataType, output: SelectorOutput) -> AggregateUDF {
    let name = match output {
        SelectorOutput::Value => "selector_min_value",
        SelectorOutput::Time => "selector_min_time",
    };

    match data_type {
        DataType::Float64 => make_uda::<F64MinSelector>(name, output),
        DataType::Int64 => make_uda::<I64MinSelector>(name, output),
        DataType::Utf8 => make_uda::<Utf8MinSelector>(name, output),
        DataType::Boolean => make_uda::<BooleanMinSelector>(name, output),
        _ => unimplemented!("min not supported for {:?}", data_type),
    }
}

/// Returns a DataFusion user defined aggregate function for computing
/// one field of the max() selector function.
///
/// Note that until <https://issues.apache.org/jira/browse/ARROW-10945>
/// is fixed, selector functions must be computed using two separate
/// function calls, one each for the value and time part
///
/// selector_max(data_column, timestamp_column) -> value and timestamp
///
/// value is the maximum value of the data_column
///
/// timestamp is the value of the timestamp_column at the position of
/// the maximum value_column. If there are multiple rows with the
/// maximum timestamp value, the value of the data_column with the
/// first (earliest/smallest) timestamp is chosen
pub fn selector_max(data_type: &DataType, output: SelectorOutput) -> AggregateUDF {
    let name = match output {
        SelectorOutput::Value => "selector_max_value",
        SelectorOutput::Time => "selector_max_time",
    };

    match data_type {
        DataType::Float64 => make_uda::<F64MaxSelector>(name, output),
        DataType::Int64 => make_uda::<I64MaxSelector>(name, output),
        DataType::Utf8 => make_uda::<Utf8MaxSelector>(name, output),
        DataType::Boolean => make_uda::<BooleanMaxSelector>(name, output),
        _ => unimplemented!("max not supported for {:?}", data_type),
    }
}

/// Implements the logic of the specific selector function (this is a
/// cutdown version of the Accumulator DataFusion trait, to allow
/// sharing between implementations)
trait Selector: Debug + Default + Send + Sync {
    /// What type of values does this selector function work with (time is
    /// always I64)
    fn value_data_type() -> DataType;

    /// return state in a form that DataFusion can store during execution
    fn datafusion_state(&self) -> DataFusionResult<Vec<ScalarValue>>;

    /// produces the final value of this selector for the specified output type
    fn evaluate(&self, output: &SelectorOutput) -> DataFusionResult<ScalarValue>;

    /// Update this selector's state based on values in value_arr and time_arr
    fn update_batch(&mut self, value_arr: &ArrayRef, time_arr: &ArrayRef) -> DataFusionResult<()>;
}

// Describes which part of the selector to return: the timestamp or
// the value (when https://issues.apache.org/jira/browse/ARROW-10945
// is fixed, this enum should be removed)
#[derive(Debug, Clone, Copy)]
pub enum SelectorOutput {
    Value,
    Time,
}

impl SelectorOutput {
    /// return the data type produced for this type of output
    fn return_type(&self, input_type: &DataType) -> DataType {
        match self {
            Self::Value => input_type.clone(),
            // timestamps are always the same type
            Self::Time => TIME_DATA_TYPE(),
        }
    }
}

/// Factory function for creating the UDA function for DataFusion
fn make_uda<SELECTOR>(name: &'static str, output: SelectorOutput) -> AggregateUDF
where
    SELECTOR: Selector + 'static,
{
    let value_data_type = SELECTOR::value_data_type();
    let input_signature = Signature::Exact(vec![value_data_type.clone(), TIME_DATA_TYPE()]);

    let state_type = Arc::new(vec![value_data_type.clone(), TIME_DATA_TYPE()]);
    let state_type_factory: StateTypeFunction = Arc::new(move |_| Ok(Arc::clone(&state_type)));

    let factory: AccumulatorFunctionImplementation =
        Arc::new(move || Ok(Box::new(SelectorAccumulator::<SELECTOR>::new(output))));

    let return_type = Arc::new(output.return_type(&value_data_type));
    let return_type_func: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::clone(&return_type)));

    AggregateUDF::new(
        name,
        &input_signature,
        &return_type_func,
        &factory,
        &state_type_factory,
    )
}

/// Structure that implements the Accumulator trait for DataFusion
/// and processes (value, timestamp) pair and computes values
#[derive(Debug)]
struct SelectorAccumulator<SELECTOR>
where
    SELECTOR: Selector,
{
    // The underlying implementation for the selector
    selector: SELECTOR,
    // Determine which value is output
    output: SelectorOutput,
}

impl<SELECTOR> SelectorAccumulator<SELECTOR>
where
    SELECTOR: Selector,
{
    pub fn new(output: SelectorOutput) -> Self {
        Self {
            output,
            selector: SELECTOR::default(),
        }
    }
}

impl<SELECTOR> Accumulator for SelectorAccumulator<SELECTOR>
where
    SELECTOR: Selector + 'static,
{
    // this function serializes our state to a vector of
    // `ScalarValue`s, which DataFusion uses to pass this state
    // between execution stages.
    fn state(&self) -> DataFusionResult<Vec<ScalarValue>> {
        self.selector.datafusion_state()
    }

    fn update(&mut self, _values: &[ScalarValue]) -> DataFusionResult<()> {
        unreachable!("Should only be calling update_batch for performance reasons");
    }

    // this function receives states from other accumulators
    // (Vec<ScalarValue>) and updates the accumulator.
    fn merge(&mut self, _states: &[ScalarValue]) -> DataFusionResult<()> {
        unreachable!("Should only be calling merge_batch for performance reasons");
    }

    // Return the final value of this aggregator.
    fn evaluate(&self) -> DataFusionResult<ScalarValue> {
        self.selector.evaluate(&self.output)
    }

    // This function receives one entry per argument of this
    // accumulator and updates the selector state function appropriately
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        if values.len() != 2 {
            return Err(DataFusionError::Internal(format!(
                "Internal error: Expected 2 arguments passed to selector function but got {}",
                values.len()
            )));
        }

        // invoke the actual worker function.
        self.selector.update_batch(&values[0], &values[1])?;
        Ok(())
    }

    // The input values and accumulator state are the same types for
    // selectors, and thus we can merge intermediate states with the
    // same function as inputs
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DataFusionResult<()> {
        // merge is the same operation as update for these selectors
        self.update_batch(states)
    }
}

#[cfg(test)]
mod test {
    use arrow::{
        array::{BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray},
        datatypes::{Field, Schema, SchemaRef},
        record_batch::RecordBatch,
        util::pretty::pretty_format_batches,
    };
    use datafusion::{datasource::MemTable, logical_plan::Expr, prelude::*};
    use internal_types::schema::TIME_DATA_TIMEZONE;

    use super::*;

    #[tokio::test]
    async fn test_selector_first() {
        let cases = vec![
            (
                selector_first(&DataType::Float64, SelectorOutput::Value),
                selector_first(&DataType::Float64, SelectorOutput::Time),
                "f64_value",
                vec![
                    "+------------------------------------------+-----------------------------------------+",
                    "| selector_first_value(t.f64_value,t.time) | selector_first_time(t.f64_value,t.time) |",
                    "+------------------------------------------+-----------------------------------------+",
                    "| 2                                        | 1970-01-01 00:00:00.000001              |",
                    "+------------------------------------------+-----------------------------------------+",
                    "",
                ],
            ),
            (
                selector_first(&DataType::Int64, SelectorOutput::Value),
                selector_first(&DataType::Int64, SelectorOutput::Time),
                "i64_value",
                vec![
                    "+------------------------------------------+-----------------------------------------+",
                    "| selector_first_value(t.i64_value,t.time) | selector_first_time(t.i64_value,t.time) |",
                    "+------------------------------------------+-----------------------------------------+",
                    "| 20                                       | 1970-01-01 00:00:00.000001              |",
                    "+------------------------------------------+-----------------------------------------+",
                    "",
                ],
            ),
            (
                selector_first(&DataType::Utf8, SelectorOutput::Value),
                selector_first(&DataType::Utf8, SelectorOutput::Time),
                "string_value",
                vec![
                    "+---------------------------------------------+--------------------------------------------+",
                    "| selector_first_value(t.string_value,t.time) | selector_first_time(t.string_value,t.time) |",
                    "+---------------------------------------------+--------------------------------------------+",
                    "| two                                         | 1970-01-01 00:00:00.000001                 |",
                    "+---------------------------------------------+--------------------------------------------+",
                    "",
                ],
            ),
            (
                selector_first(&DataType::Boolean, SelectorOutput::Value),
                selector_first(&DataType::Boolean, SelectorOutput::Time),
                "bool_value",
                vec![
                    "+-------------------------------------------+------------------------------------------+",
                    "| selector_first_value(t.bool_value,t.time) | selector_first_time(t.bool_value,t.time) |",
                    "+-------------------------------------------+------------------------------------------+",
                    "| true                                      | 1970-01-01 00:00:00.000001               |",
                    "+-------------------------------------------+------------------------------------------+",
                    "",
                ],
            )
        ];

        for (val_func, time_func, val_column, expected) in cases.into_iter() {
            let args = vec![col(val_column), col("time")];
            let aggs = vec![val_func.call(args.clone()), time_func.call(args)];
            let actual = run_plan(aggs).await;

            assert_eq!(
                expected, actual,
                "\n\nEXPECTED:\n{:#?}\nACTUAL:\n{:#?}\n",
                expected, actual
            );
        }
    }

    #[tokio::test]
    async fn test_selector_last() {
        let cases = vec![
            (
                selector_last(&DataType::Float64, SelectorOutput::Value),
                selector_last(&DataType::Float64, SelectorOutput::Time),
                "f64_value",
                vec![
                    "+-----------------------------------------+----------------------------------------+",
                    "| selector_last_value(t.f64_value,t.time) | selector_last_time(t.f64_value,t.time) |",
                    "+-----------------------------------------+----------------------------------------+",
                    "| 3                                       | 1970-01-01 00:00:00.000006             |",
                    "+-----------------------------------------+----------------------------------------+",
                    "",
                ],
            ),
            (
                selector_last(&DataType::Int64, SelectorOutput::Value),
                selector_last(&DataType::Int64, SelectorOutput::Time),
                "i64_value",
                vec![
                    "+-----------------------------------------+----------------------------------------+",
                    "| selector_last_value(t.i64_value,t.time) | selector_last_time(t.i64_value,t.time) |",
                    "+-----------------------------------------+----------------------------------------+",
                    "| 30                                      | 1970-01-01 00:00:00.000006             |",
                    "+-----------------------------------------+----------------------------------------+",
                    "",
                ],
            ),
            (
                selector_last(&DataType::Utf8, SelectorOutput::Value),
                selector_last(&DataType::Utf8, SelectorOutput::Time),
                "string_value",
                vec![
                    "+--------------------------------------------+-------------------------------------------+",
                    "| selector_last_value(t.string_value,t.time) | selector_last_time(t.string_value,t.time) |",
                    "+--------------------------------------------+-------------------------------------------+",
                    "| three                                      | 1970-01-01 00:00:00.000006                |",
                    "+--------------------------------------------+-------------------------------------------+",
                    "",
                ],
            ),
            (
                selector_last(&DataType::Boolean, SelectorOutput::Value),
                selector_last(&DataType::Boolean, SelectorOutput::Time),
                "bool_value",
                vec![
                    "+------------------------------------------+-----------------------------------------+",
                    "| selector_last_value(t.bool_value,t.time) | selector_last_time(t.bool_value,t.time) |",
                    "+------------------------------------------+-----------------------------------------+",
                    "| false                                    | 1970-01-01 00:00:00.000006              |",
                    "+------------------------------------------+-----------------------------------------+",
                    "",
                ],
            )
        ];

        for (val_func, time_func, val_column, expected) in cases.into_iter() {
            let args = vec![col(val_column), col("time")];
            let aggs = vec![val_func.call(args.clone()), time_func.call(args)];
            let actual = run_plan(aggs).await;

            assert_eq!(
                expected, actual,
                "\n\nEXPECTED:\n{:#?}\nACTUAL:\n{:#?}\n",
                expected, actual
            );
        }
    }

    #[tokio::test]
    async fn test_selector_min() {
        let cases = vec![
            (
                selector_min(&DataType::Float64, SelectorOutput::Value),
                selector_min(&DataType::Float64, SelectorOutput::Time),
                "f64_value",
                vec![
                    "+----------------------------------------+---------------------------------------+",
                    "| selector_min_value(t.f64_value,t.time) | selector_min_time(t.f64_value,t.time) |",
                    "+----------------------------------------+---------------------------------------+",
                    "| 1                                      | 1970-01-01 00:00:00.000004            |",
                    "+----------------------------------------+---------------------------------------+",
                    "",
                ],
            ),
            (
                selector_min(&DataType::Int64, SelectorOutput::Value),
                selector_min(&DataType::Int64, SelectorOutput::Time),
                "i64_value",
                vec![
                    "+----------------------------------------+---------------------------------------+",
                    "| selector_min_value(t.i64_value,t.time) | selector_min_time(t.i64_value,t.time) |",
                    "+----------------------------------------+---------------------------------------+",
                    "| 10                                     | 1970-01-01 00:00:00.000004            |",
                    "+----------------------------------------+---------------------------------------+",
                    "",
                ],
            ),
            (
                selector_min(&DataType::Utf8, SelectorOutput::Value),
                selector_min(&DataType::Utf8, SelectorOutput::Time),
                "string_value",
                vec![
                    "+-------------------------------------------+------------------------------------------+",
                    "| selector_min_value(t.string_value,t.time) | selector_min_time(t.string_value,t.time) |",
                    "+-------------------------------------------+------------------------------------------+",
                    "| a_one                                     | 1970-01-01 00:00:00.000004               |",
                    "+-------------------------------------------+------------------------------------------+",
                    "",
                ],
            ),
            (
                selector_min(&DataType::Boolean, SelectorOutput::Value),
                selector_min(&DataType::Boolean, SelectorOutput::Time),
                "bool_value",
                vec![
                    "+-----------------------------------------+----------------------------------------+",
                    "| selector_min_value(t.bool_value,t.time) | selector_min_time(t.bool_value,t.time) |",
                    "+-----------------------------------------+----------------------------------------+",
                    "| false                                   | 1970-01-01 00:00:00.000002             |",
                    "+-----------------------------------------+----------------------------------------+",
                    "",
                ],
            )
        ];

        for (val_func, time_func, val_column, expected) in cases.into_iter() {
            let args = vec![col(val_column), col("time")];
            let aggs = vec![val_func.call(args.clone()), time_func.call(args)];
            let actual = run_plan(aggs).await;

            assert_eq!(
                expected, actual,
                "\n\nEXPECTED:\n{:#?}\nACTUAL:\n{:#?}\n",
                expected, actual
            );
        }
    }

    #[tokio::test]
    async fn test_selector_max() {
        let cases = vec![
            (
                selector_max(&DataType::Float64, SelectorOutput::Value),
                selector_max(&DataType::Float64, SelectorOutput::Time),
                "f64_value",
                vec![
                    "+----------------------------------------+---------------------------------------+",
                    "| selector_max_value(t.f64_value,t.time) | selector_max_time(t.f64_value,t.time) |",
                    "+----------------------------------------+---------------------------------------+",
                    "| 5                                      | 1970-01-01 00:00:00.000005            |",
                    "+----------------------------------------+---------------------------------------+",
                    "",
                ],
            ),
            (
                selector_max(&DataType::Int64, SelectorOutput::Value),
                selector_max(&DataType::Int64, SelectorOutput::Time),
                "i64_value",
                vec![
                    "+----------------------------------------+---------------------------------------+",
                    "| selector_max_value(t.i64_value,t.time) | selector_max_time(t.i64_value,t.time) |",
                    "+----------------------------------------+---------------------------------------+",
                    "| 50                                     | 1970-01-01 00:00:00.000005            |",
                    "+----------------------------------------+---------------------------------------+",
                    "",
                ],
            ),
            (
                selector_max(&DataType::Utf8, SelectorOutput::Value),
                selector_max(&DataType::Utf8, SelectorOutput::Time),
                "string_value",
                vec![
                    "+-------------------------------------------+------------------------------------------+",
                    "| selector_max_value(t.string_value,t.time) | selector_max_time(t.string_value,t.time) |",
                    "+-------------------------------------------+------------------------------------------+",
                    "| z_five                                    | 1970-01-01 00:00:00.000005               |",
                    "+-------------------------------------------+------------------------------------------+",
                    "",
                ],
            ),
            (
                selector_max(&DataType::Boolean, SelectorOutput::Value),
                selector_max(&DataType::Boolean, SelectorOutput::Time),
                "bool_value",
                vec![
                    "+-----------------------------------------+----------------------------------------+",
                    "| selector_max_value(t.bool_value,t.time) | selector_max_time(t.bool_value,t.time) |",
                    "+-----------------------------------------+----------------------------------------+",
                    "| true                                    | 1970-01-01 00:00:00.000001             |",
                    "+-----------------------------------------+----------------------------------------+",
                    "",
                ],
            )
        ];

        for (val_func, time_func, val_column, expected) in cases.into_iter() {
            let args = vec![col(val_column), col("time")];
            let aggs = vec![val_func.call(args.clone()), time_func.call(args)];
            let actual = run_plan(aggs).await;

            assert_eq!(
                expected, actual,
                "\n\nEXPECTED:\n{:#?}\nACTUAL:\n{:#?}\n",
                expected, actual
            );
        }
    }

    /// Run a plan against the following input table as "t"
    ///
    /// ```text
    /// +-----------+-----------+--------------+------------+----------------------------+,
    /// | f64_value | i64_value | string_value | bool_value | time                       |,
    /// +-----------+-----------+--------------+------------+----------------------------+,
    /// | 2         | 20        | two          | true       | 1970-01-01 00:00:00.000001 |,
    /// | 4         | 40        | four         | false      | 1970-01-01 00:00:00.000002 |,
    /// |           |           |                           | 1970-01-01 00:00:00.000003 |,
    /// | 1         | 10        | a_one        | true       | 1970-01-01 00:00:00.000004 |,
    /// | 5         | 50        | z_five       | false      | 1970-01-01 00:00:00.000005 |,
    /// | 3         | 30        | three        | false      | 1970-01-01 00:00:00.000006 |,
    /// +-----------+-----------+--------------+------------+----------------------------+,
    /// ```
    async fn run_plan(aggs: Vec<Expr>) -> Vec<String> {
        // define a schema for input
        // (value) and timestamp
        let schema = Arc::new(Schema::new(vec![
            Field::new("f64_value", DataType::Float64, false),
            Field::new("i64_value", DataType::Int64, false),
            Field::new("string_value", DataType::Utf8, false),
            Field::new("bool_value", DataType::Boolean, false),
            Field::new("time", TIME_DATA_TYPE(), true),
        ]));

        // define data in two partitions
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float64Array::from(vec![Some(2.0), Some(4.0), None])),
                Arc::new(Int64Array::from(vec![Some(20), Some(40), None])),
                Arc::new(StringArray::from(vec![Some("two"), Some("four"), None])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
                Arc::new(TimestampNanosecondArray::from_vec(
                    vec![1000, 2000, 3000],
                    TIME_DATA_TIMEZONE(),
                )),
            ],
        )
        .unwrap();

        // No values in this batch
        let batch2 = match RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)),
                Arc::new(Int64Array::from(vec![] as Vec<Option<i64>>)),
                Arc::new(StringArray::from(vec![] as Vec<Option<&str>>)),
                Arc::new(BooleanArray::from(vec![] as Vec<Option<bool>>)),
                Arc::new(TimestampNanosecondArray::from_vec(
                    vec![],
                    TIME_DATA_TIMEZONE(),
                )),
            ],
        ) {
            Ok(a) => a,
            _ => unreachable!(),
        };

        let batch3 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float64Array::from(vec![Some(1.0), Some(5.0), Some(3.0)])),
                Arc::new(Int64Array::from(vec![Some(10), Some(50), Some(30)])),
                Arc::new(StringArray::from(vec![
                    Some("a_one"),
                    Some("z_five"),
                    Some("three"),
                ])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    Some(false),
                ])),
                Arc::new(TimestampNanosecondArray::from_vec(
                    vec![4000, 5000, 6000],
                    TIME_DATA_TIMEZONE(),
                )),
            ],
        )
        .unwrap();

        // Ensure the answer is the same regardless of the order of inputs
        let input = vec![batch1, batch2, batch3];
        let input_string = pretty_format_batches(&input).unwrap();
        let results = run_with_inputs(Arc::clone(&schema), aggs.clone(), input.clone()).await;

        use itertools::Itertools;
        // Get all permutations of the input
        for p in input.iter().permutations(3) {
            let p_batches = p.into_iter().cloned().collect::<Vec<_>>();
            let p_input_string = pretty_format_batches(&p_batches).unwrap();
            let p_results = run_with_inputs(Arc::clone(&schema), aggs.clone(), p_batches).await;
            assert_eq!(
                results, p_results,
                "Mismatch with permutation.\n\
                        Input1 \n\n\
                        {}\n\n\
                        produces output:\n\n\
                        {:#?}\n\n\
                        Input 2\n\n\
                        {}\n\n\
                        produces output:\n\n\
                        {:#?}\n\n",
                input_string, results, p_input_string, p_results
            );
        }

        results
    }

    async fn run_with_inputs(
        schema: SchemaRef,
        aggs: Vec<Expr>,
        inputs: Vec<RecordBatch>,
    ) -> Vec<String> {
        let provider = MemTable::try_new(Arc::clone(&schema), vec![inputs]).unwrap();
        let mut ctx = ExecutionContext::new();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let df = ctx.table("t").unwrap();
        let df = df.aggregate(vec![], aggs).unwrap();

        // execute the query
        let record_batches = df.collect().await.unwrap();

        pretty_format_batches(&record_batches)
            .unwrap()
            .split('\n')
            .map(|s| s.to_owned())
            .collect()
    }
}
