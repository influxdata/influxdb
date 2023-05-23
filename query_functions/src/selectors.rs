//! ## Overview
//!
//! *Selector functions* are special IOx SQL aggregate functions,
//! designed to provide the same semantics as the [selector functions]
//! in [InfluxQL]
//!
//! Selector functions are similar to standard aggregate functions in
//! that they collapse (aggregate) an input set of rows into a single
//! row.
//!
//! Selector functions are different than regular aggregate functions
//! because they rely on and return a `time` in addition to the
//! `value`. Time is implicit in InfluxQL, but not in SQL, so the
//! selector function invocation is slightly different in IOx SQL than
//! in InfluxQL.
//!
//! Each selector function returns a two part value, the `value` of
//! the first argument as well as the value for the corresponding
//! second argument, which must timestamp.
//!
//! ## Example
//!
//! Given the following input:
//!
//! ```text
//! +----------------------+-------------+
//! | time                 | water_level |
//! +----------------------+-------------+
//! | 2019-08-28T07:22:00Z | 9.8         |
//! | 2019-08-28T07:23:00Z | 9.7         |
//! | 2019-08-28T07:24:00Z | 10.00       |
//! | 2019-08-28T07:25:00Z | 9.9         |
//! +----------------------+-------------+
//! ```
//!
//! Using the SQL `min` aggregate function (not a selector) finds the
//! minimum `water_level` value, `9.7` in this case:
//!
//! ```sql
//! select min(water_level) from "h2o_feet";
//!
//! +-------------+
//! | water_level |
//! +-------------+
//! | 9.7         |
//! +-------------+
//! ```
//!
//! There is no easy way in SQL to determine at which value of `time`
//! the minimum value occurred, however `selector_min` returns this as well:
//!
//! ```sql
//! select selector_min(water_level, time) from "h2o_feet";
//!
//! +----------------------------------------------+
//! | selector_min(water_level,time)               |
//! +----------------------------------------------+
//! | {"value": 9.7, "time": 2019-08-28T07:23:00Z} |
//! +----------------------------------------------+
//! ```
//!
//! Note that the output is a `struct` with two fields, `value` and
//! `time`. To access the values, you can use the field reference
//! `['field_name']` syntax (note the use of single quotes `'` around
//! the field names):
//!
//! ```sql
//! select
//!   selector_min(water_level, time)['time'],
//!   selector_min(water_level, time)['value']
//! from "h2o_feet";
//! +----------------------------------------+-----------------------------------------+
//! | selector_first(water_level,time)[time] | selector_first(water_level,time)[value] |
//! +----------------------------------------+-----------------------------------------+
//! | 2019-08-28T07:23:00Z                   | 9.7                                     |
//! +----------------------------------------+-----------------------------------------+
//! ```
//!
//! ## Supported Selectors
//!
//! IOx supports the following selectors:
//!
//! 1. `selector_first`: `time` and `value` of the row with earliest `time` in the group
//! 2. `selector_last`: `time` and `value` of the row with latest `time` in the group
//! 3. `selector_min`: `time` and `value` of the row with smallest `value` in the group
//! 4. `selector_max`: `time` and `value` of the row with largest `value` in the group
//!
//! For `selector_first` / `selector_last`, if there are multiple
//! rows with same minimum / maximum timestamp, the value returned is
//! arbitrary
//!
//! For `selector_min` / `selector_max`, if there are multiple rows
//! with the same minimum / maximum value, the value with the smallest
//! timestamp is chosen.
//!
//! [InfluxQL]: https://docs.influxdata.com/influxdb/v1.8/query_language/
//! [selector functions]: https://docs.influxdata.com/influxdb/v1.8/query_language/functions/#selectors
use std::{fmt::Debug, sync::Arc};

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field, Fields},
};
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{AccumulatorFunctionImplementation, Signature, TypeSignature, Volatility},
    physical_plan::{udaf::AggregateUDF, Accumulator},
    prelude::SessionContext,
    scalar::ScalarValue,
};

/// Internal implementations of the selector functions
mod internal;
use internal::{FirstSelector, LastSelector, MaxSelector, MinSelector};
use schema::TIME_DATA_TYPE;

/// registers selector functions so they can be invoked via SQL
pub fn register_selector_aggregates(ctx: &SessionContext) {
    ctx.register_udaf(selector_first());
    ctx.register_udaf(selector_last());
    ctx.register_udaf(selector_min());
    ctx.register_udaf(selector_max());
}

/// Returns a DataFusion user defined aggregate function for computing
/// the first(value, time) selector function, returning a struct:
///
/// first(value, time) -> struct { value, time }
///
/// ```text
/// {
///   value: value at the row of the minimum of the time column.
///   time: value of the minimum time column
/// }
/// ```
///
/// If there are multiple rows with the minimum timestamp value, the
/// value returned is arbitrary
pub fn selector_first() -> AggregateUDF {
    make_uda("selector_first", FactoryBuilder::new(SelectorType::First))
}

/// Returns a DataFusion user defined aggregate function for computing
/// the last(value, time) selector function, returning a struct:
///
/// last(value, time) -> struct { value, time }
///
/// ```text
/// {
///   value: value at the row of the maximum of the time column.
///   time: value of the maximum time column
/// }
/// ```
///
/// If there are multiple rows with the maximum timestamp value, the
/// value is arbitrary
pub fn selector_last() -> AggregateUDF {
    make_uda("selector_last", FactoryBuilder::new(SelectorType::Last))
}

/// Returns a DataFusion user defined aggregate function for computing
/// the min(value, time) selector function, returning a struct:
///
/// min(value, time) -> struct { value, time }
///
/// ```text
/// {
///   value: value at the row with minimum value
///   time: value of time for row with minimum value
/// }
/// ```
///
/// If there are multiple rows with the same minimum value, the value
/// with the first (earliest/smallest) timestamp is chosen
pub fn selector_min() -> AggregateUDF {
    make_uda("selector_min", FactoryBuilder::new(SelectorType::Min))
}

/// Returns a DataFusion user defined aggregate function for computing
/// the max(value, time) selector function, returning a struct:
///
/// max(value, time) -> struct { value, time }
///
/// ```text
/// {
///   value: value at the row with maximum value
///   time: value of time for row with maximum value
/// }
/// ```
///
/// If there are multiple rows with the same maximum value, the value
/// with the first (earliest/smallest) timestamp is chosen
pub fn selector_max() -> AggregateUDF {
    make_uda("selector_max", FactoryBuilder::new(SelectorType::Max))
}

#[derive(Debug, Clone, Copy)]
enum SelectorType {
    First,
    Last,
    Min,
    Max,
}

/// Builder to create the appropriate typed factory functions for selectors
#[derive(Debug)]
struct FactoryBuilder {
    selector_type: SelectorType,

    /// If the selector output is "time" we can't determine the
    /// accumuator type from the return type, so hold we pass the data type explicitly
    value_type: Option<DataType>,
}

impl FactoryBuilder {
    fn new(selector_type: SelectorType) -> Self {
        Self {
            selector_type,
            value_type: None,
        }
    }

    fn build_state_type_factory(&self) -> StateTypeFactory {
        let value_type = self.value_type.clone();

        Arc::new(move |return_type| {
            let value_type = match &value_type {
                Some(t) => t,
                None => value_data_type_from_return_data_type(return_type),
            };

            let state_types = make_state_datatypes(value_type.clone());
            Ok(Arc::new(state_types))
        })
    }

    /// Returns a function that instantiates the accumulator, consuming self
    fn build_accumulator_factory(self) -> AccumulatorFunctionImplementation {
        let Self {
            selector_type,
            value_type,
        } = self;

        Arc::new(move |return_type| {
            let value_type = match &value_type {
                Some(t) => t,
                None => value_data_type_from_return_data_type(return_type),
            };

            let accumulator: Box<dyn Accumulator> = match selector_type {
                SelectorType::First => {
                    Box::new(SelectorAccumulator::new(FirstSelector::new(value_type)?))
                }
                SelectorType::Last => {
                    Box::new(SelectorAccumulator::new(LastSelector::new(value_type)?))
                }
                SelectorType::Min => {
                    Box::new(SelectorAccumulator::new(MinSelector::new(value_type)?))
                }
                SelectorType::Max => {
                    Box::new(SelectorAccumulator::new(MaxSelector::new(value_type)?))
                }
            };
            Ok(accumulator)
        })
    }
}

/// Implements the logic of the specific selector function (this is a
/// cutdown version of the Accumulator DataFusion trait, to allow
/// sharing between implementations)
trait Selector: Debug + Send + Sync {
    /// return state in a form that DataFusion can store during execution
    fn datafusion_state(&self) -> DataFusionResult<Vec<ScalarValue>>;

    /// produces the final value of this selector for the specified output type
    fn evaluate(&self) -> DataFusionResult<ScalarValue>;

    /// Update this selector's state based on values in value_arr and time_arr
    fn update_batch(&mut self, value_arr: &ArrayRef, time_arr: &ArrayRef) -> DataFusionResult<()>;

    /// Allocated size required for this selector, in bytes,
    /// including `Self`.  Allocated means that for internal
    /// containers such as `Vec`, the `capacity` should be used not
    /// the `len`
    fn size(&self) -> usize;
}

/// Create the struct fields for a selector with DataType `value_type`
fn make_struct_fields(value_type: DataType) -> Fields {
    Fields::from(vec![
        Field::new("value", value_type, true),
        Field::new("time", TIME_DATA_TYPE(), true),
    ])
}

/// Return the value type given the (struct) output data type
fn value_data_type_from_return_data_type(output_type: &DataType) -> &DataType {
    match output_type {
        DataType::Struct(fields) => fields[0].data_type(),
        t => t,
    }
}

type ReturnTypeFunction = Arc<dyn Fn(&[DataType]) -> DataFusionResult<Arc<DataType>> + Send + Sync>;
type StateTypeFactory =
    Arc<dyn Fn(&DataType) -> DataFusionResult<Arc<Vec<DataType>>> + Send + Sync>;

/// Create a User Defined Aggregate Function (UDAF) for datafusion.
fn make_uda(name: &str, factory_builder: FactoryBuilder) -> AggregateUDF {
    // All selectors support the same input types / signatures
    let input_signature = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Float64, TIME_DATA_TYPE()]),
            TypeSignature::Exact(vec![DataType::Int64, TIME_DATA_TYPE()]),
            TypeSignature::Exact(vec![DataType::UInt64, TIME_DATA_TYPE()]),
            TypeSignature::Exact(vec![DataType::Utf8, TIME_DATA_TYPE()]),
            TypeSignature::Exact(vec![DataType::Boolean, TIME_DATA_TYPE()]),
        ],
        Volatility::Stable,
    );

    // return type of the selector is based on the input arguments.
    //
    // The inputs are (value, time) and the output is a struct with a
    // 'value' and 'time' field of the same time.
    let captured_name = name.to_string();
    let return_type_func: ReturnTypeFunction = Arc::new(move |arg_types| {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "{} requires exactly 2 arguments, got {}",
                captured_name,
                arg_types.len()
            )));
        }

        let input_type = &arg_types[0];
        let time_type = &arg_types[1];
        if time_type != &TIME_DATA_TYPE() {
            return Err(DataFusionError::Plan(format!(
                "{captured_name} second argument must be a timestamp, but got {time_type}"
            )));
        }
        let return_type = DataType::Struct(make_struct_fields(input_type.clone()));

        Ok(Arc::new(return_type))
    });

    // state type given the return type
    let state_type_factory = factory_builder.build_state_type_factory();

    AggregateUDF::new(
        name,
        &input_signature,
        &return_type_func,
        &factory_builder.build_accumulator_factory(),
        &state_type_factory,
    )
}

/// Return the state in which the arguments are stored
fn make_state_datatypes(value_type: DataType) -> Vec<DataType> {
    vec![value_type, TIME_DATA_TYPE()]
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
}

impl<SELECTOR> SelectorAccumulator<SELECTOR>
where
    SELECTOR: Selector,
{
    pub fn new(selector: SELECTOR) -> Self {
        Self { selector }
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

    /// Allocated size required for this accumulator, in bytes,
    /// including `Self`.  Allocated means that for internal
    /// containers such as `Vec`, the `capacity` should be used not
    /// the `len`
    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.selector) + self.selector.size()
    }

    // Return the final value of this aggregator.
    fn evaluate(&self) -> DataFusionResult<ScalarValue> {
        self.selector.evaluate()
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
        array::{
            BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
            UInt64Array,
        },
        datatypes::{Field, Schema, SchemaRef},
        record_batch::RecordBatch,
        util::pretty::pretty_format_batches,
    };
    use datafusion::{datasource::MemTable, prelude::*};

    use super::*;
    use utils::run_case;

    mod first {
        use super::*;

        #[tokio::test]
        async fn test_f64() {
            run_case(
                selector_first().call(vec![col("f64_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_first(t.f64_value,t.time)             |",
                    "+------------------------------------------------+",
                    "| {value: 2.0, time: 1970-01-01T00:00:00.000001} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_first().call(vec![col("f64_not_normal_1_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_first(t.f64_not_normal_1_value,t.time) |",
                    "+-------------------------------------------------+",
                    "| {value: NaN, time: 1970-01-01T00:00:00.000001}  |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_first().call(vec![col("f64_not_normal_2_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_first(t.f64_not_normal_2_value,t.time) |",
                    "+-------------------------------------------------+",
                    "| {value: -inf, time: 1970-01-01T00:00:00.000001} |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_first().call(vec![col("f64_not_normal_3_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_first(t.f64_not_normal_3_value,t.time) |",
                    "+-------------------------------------------------+",
                    "| {value: NaN, time: 1970-01-01T00:00:00.000001}  |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_first().call(vec![col("f64_not_normal_4_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_first(t.f64_not_normal_4_value,t.time) |",
                    "+-------------------------------------------------+",
                    "| {value: 1.0, time: 1970-01-01T00:00:00.000001}  |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_first().call(vec![col("f64_not_normal_5_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_first(t.f64_not_normal_5_value,t.time) |",
                    "+-------------------------------------------------+",
                    "| {value: 2.0, time: 1970-01-01T00:00:00.000001}  |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_i64() {
            run_case(
                selector_first().call(vec![col("i64_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_first(t.i64_value,t.time)            |",
                    "+-----------------------------------------------+",
                    "| {value: 20, time: 1970-01-01T00:00:00.000001} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_u64() {
            run_case(
                selector_first().call(vec![col("u64_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_first(t.u64_value,t.time)            |",
                    "+-----------------------------------------------+",
                    "| {value: 20, time: 1970-01-01T00:00:00.000001} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_string() {
            run_case(
                selector_first().call(vec![col("string_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_first(t.string_value,t.time)          |",
                    "+------------------------------------------------+",
                    "| {value: two, time: 1970-01-01T00:00:00.000001} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_bool() {
            run_case(
                selector_first().call(vec![col("bool_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_first(t.bool_value,t.time)             |",
                    "+-------------------------------------------------+",
                    "| {value: true, time: 1970-01-01T00:00:00.000001} |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;
        }
    }

    mod last {
        use super::*;

        #[tokio::test]
        async fn test_f64() {
            run_case(
                selector_last().call(vec![col("f64_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_last(t.f64_value,t.time)              |",
                    "+------------------------------------------------+",
                    "| {value: 3.0, time: 1970-01-01T00:00:00.000006} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_last().call(vec![col("f64_not_normal_1_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_last(t.f64_not_normal_1_value,t.time)  |",
                    "+-------------------------------------------------+",
                    "| {value: -inf, time: 1970-01-01T00:00:00.000006} |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_last().call(vec![col("f64_not_normal_2_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_last(t.f64_not_normal_2_value,t.time) |",
                    "+------------------------------------------------+",
                    "| {value: inf, time: 1970-01-01T00:00:00.000006} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_last().call(vec![col("f64_not_normal_3_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_last(t.f64_not_normal_3_value,t.time) |",
                    "+------------------------------------------------+",
                    "| {value: NaN, time: 1970-01-01T00:00:00.000006} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_last().call(vec![col("f64_not_normal_4_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_last(t.f64_not_normal_4_value,t.time) |",
                    "+------------------------------------------------+",
                    "| {value: 3.0, time: 1970-01-01T00:00:00.000006} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_last().call(vec![col("f64_not_normal_5_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_last(t.f64_not_normal_5_value,t.time) |",
                    "+------------------------------------------------+",
                    "| {value: 4.0, time: 1970-01-01T00:00:00.000006} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_i64() {
            run_case(
                selector_last().call(vec![col("i64_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_last(t.i64_value,t.time)             |",
                    "+-----------------------------------------------+",
                    "| {value: 30, time: 1970-01-01T00:00:00.000006} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_u64() {
            run_case(
                selector_last().call(vec![col("u64_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_last(t.u64_value,t.time)             |",
                    "+-----------------------------------------------+",
                    "| {value: 30, time: 1970-01-01T00:00:00.000006} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_string() {
            run_case(
                selector_last().call(vec![col("string_value"), col("time")]),
                vec![
                    "+--------------------------------------------------+",
                    "| selector_last(t.string_value,t.time)             |",
                    "+--------------------------------------------------+",
                    "| {value: three, time: 1970-01-01T00:00:00.000006} |",
                    "+--------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_bool() {
            run_case(
                selector_last().call(vec![col("bool_value"), col("time")]),
                vec![
                    "+--------------------------------------------------+",
                    "| selector_last(t.bool_value,t.time)               |",
                    "+--------------------------------------------------+",
                    "| {value: false, time: 1970-01-01T00:00:00.000006} |",
                    "+--------------------------------------------------+",
                ],
            )
            .await;
        }
    }

    mod min {
        use super::*;

        #[tokio::test]
        async fn test_f64() {
            run_case(
                selector_min().call(vec![col("f64_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_min(t.f64_value,t.time)               |",
                    "+------------------------------------------------+",
                    "| {value: 1.0, time: 1970-01-01T00:00:00.000004} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_min().call(vec![col("f64_not_normal_1_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_min(t.f64_not_normal_1_value,t.time)   |",
                    "+-------------------------------------------------+",
                    "| {value: -inf, time: 1970-01-01T00:00:00.000003} |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_min().call(vec![col("f64_not_normal_2_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_min(t.f64_not_normal_2_value,t.time)   |",
                    "+-------------------------------------------------+",
                    "| {value: -inf, time: 1970-01-01T00:00:00.000001} |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_min().call(vec![col("f64_not_normal_3_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_min(t.f64_not_normal_3_value,t.time)  |",
                    "+------------------------------------------------+",
                    "| {value: NaN, time: 1970-01-01T00:00:00.000001} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_i64() {
            run_case(
                selector_min().call(vec![col("i64_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_min(t.i64_value,t.time)              |",
                    "+-----------------------------------------------+",
                    "| {value: 10, time: 1970-01-01T00:00:00.000004} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_min().call(vec![col("i64_2_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_min(t.i64_2_value,t.time)            |",
                    "+-----------------------------------------------+",
                    "| {value: 30, time: 1970-01-01T00:00:00.000006} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_u64() {
            run_case(
                selector_min().call(vec![col("u64_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_min(t.u64_value,t.time)              |",
                    "+-----------------------------------------------+",
                    "| {value: 10, time: 1970-01-01T00:00:00.000004} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_string() {
            run_case(
                selector_min().call(vec![col("string_value"), col("time")]),
                vec![
                    "+--------------------------------------------------+",
                    "| selector_min(t.string_value,t.time)              |",
                    "+--------------------------------------------------+",
                    "| {value: a_one, time: 1970-01-01T00:00:00.000004} |",
                    "+--------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_bool() {
            run_case(
                selector_min().call(vec![col("bool_value"), col("time")]),
                vec![
                    "+--------------------------------------------------+",
                    "| selector_min(t.bool_value,t.time)                |",
                    "+--------------------------------------------------+",
                    "| {value: false, time: 1970-01-01T00:00:00.000002} |",
                    "+--------------------------------------------------+",
                ],
            )
            .await;
        }
    }

    mod max {
        use super::*;

        #[tokio::test]
        async fn test_f64() {
            run_case(
                selector_max().call(vec![col("f64_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_max(t.f64_value,t.time)               |",
                    "+------------------------------------------------+",
                    "| {value: 5.0, time: 1970-01-01T00:00:00.000005} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_max().call(vec![col("f64_not_normal_1_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_max(t.f64_not_normal_1_value,t.time)  |",
                    "+------------------------------------------------+",
                    "| {value: NaN, time: 1970-01-01T00:00:00.000001} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_max().call(vec![col("f64_not_normal_2_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_max(t.f64_not_normal_2_value,t.time)  |",
                    "+------------------------------------------------+",
                    "| {value: inf, time: 1970-01-01T00:00:00.000004} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_max().call(vec![col("f64_not_normal_3_value"), col("time")]),
                vec![
                    "+------------------------------------------------+",
                    "| selector_max(t.f64_not_normal_3_value,t.time)  |",
                    "+------------------------------------------------+",
                    "| {value: NaN, time: 1970-01-01T00:00:00.000001} |",
                    "+------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_i64() {
            run_case(
                selector_max().call(vec![col("i64_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_max(t.i64_value,t.time)              |",
                    "+-----------------------------------------------+",
                    "| {value: 50, time: 1970-01-01T00:00:00.000005} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;

            run_case(
                selector_max().call(vec![col("i64_2_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_max(t.i64_2_value,t.time)            |",
                    "+-----------------------------------------------+",
                    "| {value: 50, time: 1970-01-01T00:00:00.000005} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_u64() {
            run_case(
                selector_max().call(vec![col("u64_value"), col("time")]),
                vec![
                    "+-----------------------------------------------+",
                    "| selector_max(t.u64_value,t.time)              |",
                    "+-----------------------------------------------+",
                    "| {value: 50, time: 1970-01-01T00:00:00.000005} |",
                    "+-----------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_string() {
            run_case(
                selector_max().call(vec![col("string_value"), col("time")]),
                vec![
                    "+---------------------------------------------------+",
                    "| selector_max(t.string_value,t.time)               |",
                    "+---------------------------------------------------+",
                    "| {value: z_five, time: 1970-01-01T00:00:00.000005} |",
                    "+---------------------------------------------------+",
                ],
            )
            .await;
        }

        #[tokio::test]
        async fn test_bool() {
            run_case(
                selector_max().call(vec![col("bool_value"), col("time")]),
                vec![
                    "+-------------------------------------------------+",
                    "| selector_max(t.bool_value,t.time)               |",
                    "+-------------------------------------------------+",
                    "| {value: true, time: 1970-01-01T00:00:00.000001} |",
                    "+-------------------------------------------------+",
                ],
            )
            .await;
        }
    }

    mod utils {
        use super::*;

        /// Runs the expr using `run_plan` and compares the result to `expected`
        pub async fn run_case(expr: Expr, expected: Vec<&'static str>) {
            println!("Running case for {expr}");

            let actual = run_plan(vec![expr.clone()]).await;

            assert_eq!(
                expected, actual,
                "\n\nexpr: {expr}\n\nEXPECTED:\n{expected:#?}\nACTUAL:\n{actual:#?}\n"
            );
        }

        /// Run a plan against the following input table as "t"
        ///
        /// ```text
        /// +-----------+-----------+-----------+--------------+------------+----------------------------+,
        /// | f64_value | i64_value | u64_value | string_value | bool_value | time                       |,
        /// +-----------+-----------+--------------+------------+----------------------------+,
        /// | 2         | 20        | 20        | two          | true       | 1970-01-01T00:00:00.000001 |,
        /// | 4         | 40        | 40        | four         | false      | 1970-01-01T00:00:00.000002 |,
        /// |           |           |           |              |            | 1970-01-01T00:00:00.000003 |,
        /// | 1         | 10        | 10        | a_one        | true       | 1970-01-01T00:00:00.000004 |,
        /// | 5         | 50        | 50        | z_five       | false      | 1970-01-01T00:00:00.000005 |,
        /// | 3         | 30        | 30        | three        | false      | 1970-01-01T00:00:00.000006 |,
        /// +-----------+-----------+--------------+------------+----------------------------+,
        /// ```
        async fn run_plan(aggs: Vec<Expr>) -> Vec<String> {
            // define a schema for input
            // (value) and timestamp
            let schema = Arc::new(Schema::new(vec![
                Field::new("f64_value", DataType::Float64, true),
                Field::new("f64_not_normal_1_value", DataType::Float64, true),
                Field::new("f64_not_normal_2_value", DataType::Float64, true),
                Field::new("f64_not_normal_3_value", DataType::Float64, true),
                Field::new("f64_not_normal_4_value", DataType::Float64, true),
                Field::new("f64_not_normal_5_value", DataType::Float64, true),
                Field::new("i64_value", DataType::Int64, true),
                Field::new("i64_2_value", DataType::Int64, true),
                Field::new("u64_value", DataType::UInt64, true),
                Field::new("string_value", DataType::Utf8, true),
                Field::new("bool_value", DataType::Boolean, true),
                Field::new("time", TIME_DATA_TYPE(), true),
            ]));

            // define data in two partitions
            let batch1 = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Float64Array::from(vec![Some(2.0), Some(4.0), None])),
                    Arc::new(Float64Array::from(vec![
                        Some(f64::NAN),
                        Some(f64::INFINITY),
                        Some(f64::NEG_INFINITY),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(f64::NEG_INFINITY),
                        Some(f64::NEG_INFINITY),
                        Some(f64::NEG_INFINITY),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(f64::NAN),
                        Some(f64::NAN),
                        Some(f64::NAN),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(1.0),
                        Some(f64::NEG_INFINITY),
                        Some(f64::NEG_INFINITY),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(2.0),
                        Some(f64::NAN),
                        Some(f64::NAN),
                    ])),
                    Arc::new(Int64Array::from(vec![Some(20), Some(40), None])),
                    Arc::new(Int64Array::from(vec![None, None, None])),
                    Arc::new(UInt64Array::from(vec![Some(20), Some(40), None])),
                    Arc::new(StringArray::from(vec![Some("two"), Some("four"), None])),
                    Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
                    Arc::new(TimestampNanosecondArray::from(vec![1000, 2000, 3000])),
                ],
            )
            .unwrap();

            // No values in this batch
            let batch2 = match RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)),
                    Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)),
                    Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)),
                    Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)),
                    Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)),
                    Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)),
                    Arc::new(Int64Array::from(vec![] as Vec<Option<i64>>)),
                    Arc::new(Int64Array::from(vec![] as Vec<Option<i64>>)),
                    Arc::new(UInt64Array::from(vec![] as Vec<Option<u64>>)),
                    Arc::new(StringArray::from(vec![] as Vec<Option<&str>>)),
                    Arc::new(BooleanArray::from(vec![] as Vec<Option<bool>>)),
                    Arc::new(TimestampNanosecondArray::from(vec![] as Vec<i64>)),
                ],
            ) {
                Ok(a) => a,
                _ => unreachable!(),
            };

            let batch3 = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Float64Array::from(vec![Some(1.0), Some(5.0), Some(3.0)])),
                    Arc::new(Float64Array::from(vec![
                        Some(f64::NAN),
                        Some(f64::INFINITY),
                        Some(f64::NEG_INFINITY),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(f64::INFINITY),
                        Some(f64::INFINITY),
                        Some(f64::INFINITY),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(f64::NAN),
                        Some(f64::NAN),
                        Some(f64::NAN),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(f64::INFINITY),
                        Some(f64::INFINITY),
                        Some(3.0),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(f64::NAN),
                        Some(f64::NAN),
                        Some(4.0),
                    ])),
                    Arc::new(Int64Array::from(vec![Some(10), Some(50), Some(30)])),
                    Arc::new(Int64Array::from(vec![None, Some(50), Some(30)])),
                    Arc::new(UInt64Array::from(vec![Some(10), Some(50), Some(30)])),
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
                    Arc::new(TimestampNanosecondArray::from(vec![4000, 5000, 6000])),
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
                            {input_string}\n\n\
                            produces output:\n\n\
                            {results:#?}\n\n\
                            Input 2\n\n\
                            {p_input_string}\n\n\
                            produces output:\n\n\
                            {p_results:#?}\n\n"
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
            let ctx = SessionContext::new();
            ctx.register_table("t", Arc::new(provider)).unwrap();

            let df = ctx.table("t").await.unwrap();
            let df = df.aggregate(vec![], aggs).unwrap();

            // execute the query
            let record_batches = df.collect().await.unwrap();

            pretty_format_batches(&record_batches)
                .unwrap()
                .to_string()
                .split('\n')
                .map(|s| s.to_owned())
                .collect()
        }
    }
}
