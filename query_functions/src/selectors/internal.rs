//! Internal implementaton of InfluxDB "Selector" Functions
//! Tests are in selector module
//!
//! This module is implemented with macros rather than generic types;
//! I tried valiantly (at least in my mind) to use Generics , but I
//! couldn't get the traits to work out correctly (as Bool, I64/F64
//! and Utf8 arrow types don't share enough in common).

use std::fmt::Debug;

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
        TimestampNanosecondArray, UInt64Array,
    },
    compute::kernels::aggregate::{
        max as array_max, max_boolean as array_max_boolean, max_string as array_max_string,
        min as array_min, min_boolean as array_min_boolean, min_string as array_min_string,
    },
    datatypes::{DataType, Field},
};
use datafusion::{error::Result as DataFusionResult, scalar::ScalarValue};

use observability_deps::tracing::debug;

use super::{Selector, SelectorOutput};

/// Trait for comparing values in arrays with their native
/// representation. This so the same comparison expression can be used
/// in the macro definitions.
///
/// Note the only one that is different String <--> &str
trait LtVal<T> {
    /// return true if v is less than self
    fn lt_val(&self, v: &T) -> bool;
}

impl LtVal<Self> for f64 {
    fn lt_val(&self, v: &Self) -> bool {
        self < v
    }
}

impl LtVal<Self> for i64 {
    fn lt_val(&self, v: &Self) -> bool {
        self < v
    }
}

impl LtVal<Self> for u64 {
    fn lt_val(&self, v: &Self) -> bool {
        self < v
    }
}

impl LtVal<Self> for bool {
    fn lt_val(&self, v: &Self) -> bool {
        self < v
    }
}

impl LtVal<String> for &str {
    fn lt_val(&self, v: &String) -> bool {
        *self < v.as_str()
    }
}

impl LtVal<&str> for String {
    fn lt_val(&self, v: &&str) -> bool {
        self.as_str() < *v
    }
}

/// Trait for comparing converting the result of aggregate kernels to their
/// native representation Note the only one that is different is &str --> String
trait ToState<T> {
    fn to_state(&self) -> T;
}

impl ToState<Self> for f64 {
    fn to_state(&self) -> Self {
        *self
    }
}

impl ToState<Self> for i64 {
    fn to_state(&self) -> Self {
        *self
    }
}

impl ToState<Self> for u64 {
    fn to_state(&self) -> Self {
        *self
    }
}

impl ToState<Self> for bool {
    fn to_state(&self) -> Self {
        *self
    }
}

impl ToState<String> for &str {
    fn to_state(&self) -> String {
        (*self).to_owned()
    }
}

fn make_scalar_struct(data_fields: Vec<ScalarValue>) -> ScalarValue {
    let fields = vec![
        Field::new("value", data_fields[0].get_datatype(), true),
        Field::new("time", data_fields[1].get_datatype(), true),
    ];

    ScalarValue::Struct(Some(data_fields), Box::new(fields))
}

macro_rules! make_first_selector {
    ($STRUCTNAME:ident, $RUSTTYPE:ident, $ARROWTYPE:expr, $ARRTYPE:ident, $MINFUNC:ident, $TO_SCALARVALUE: expr) => {
        #[derive(Debug)]
        pub struct $STRUCTNAME {
            value: Option<$RUSTTYPE>,
            time: Option<i64>,
        }

        impl Default for $STRUCTNAME {
            fn default() -> Self {
                Self {
                    value: None,
                    time: None,
                }
            }
        }

        impl Selector for $STRUCTNAME {
            fn value_data_type() -> DataType {
                $ARROWTYPE
            }

            fn datafusion_state(&self) -> DataFusionResult<Vec<ScalarValue>> {
                Ok(vec![
                    $TO_SCALARVALUE(self.value.clone()),
                    ScalarValue::TimestampNanosecond(self.time, None),
                ])
            }

            fn evaluate(&self, output: &SelectorOutput) -> DataFusionResult<ScalarValue> {
                match output {
                    SelectorOutput::Value => Ok($TO_SCALARVALUE(self.value.clone())),
                    SelectorOutput::Time => Ok(ScalarValue::TimestampNanosecond(self.time, None)),
                    SelectorOutput::Struct => Ok(make_scalar_struct(vec![
                        $TO_SCALARVALUE(self.value.clone()),
                        ScalarValue::TimestampNanosecond(self.time, None),
                    ])),
                }
            }

            fn update_batch(
                &mut self,
                value_arr: &ArrayRef,
                time_arr: &ArrayRef,
            ) -> DataFusionResult<()> {
                let value_arr = value_arr
                    .as_any()
                    .downcast_ref::<$ARRTYPE>()
                    // the input type arguments should be ensured by datafusion
                    .expect("First argument was value");

                let time_arr = time_arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    // the input type arguments should be ensured by datafusion
                    .expect("Second argument was time");

                // Only look for times where the array also has a non
                // null value (the time array should have no nulls itself)
                //
                // For example, for the following input, the correct
                // current min time is 200 (not 100)
                //
                // value | time
                // --------------
                // NULL  | 100
                // A     | 200
                // B     | 300
                //
                // Note this could likely be faster if we used `ArrayData` APIs
                let time_arr: TimestampNanosecondArray = time_arr
                    .iter()
                    .zip(value_arr.iter())
                    .map(|(ts, value)| if value.is_some() { ts } else { None })
                    .collect();

                let cur_min_time = $MINFUNC(&time_arr);

                let need_update = match (&self.time, &cur_min_time) {
                    (Some(time), Some(cur_min_time)) => cur_min_time < time,
                    // No existing minimum, so update needed
                    (None, Some(_)) => true,
                    // No actual minimum time found, so no update needed
                    (_, None) => false,
                };

                if need_update {
                    let index = time_arr
                        .iter()
                        // arrow doesn't tell us what index had the
                        // minimum, so need to find it ourselves see also
                        // https://github.com/apache/arrow-datafusion/issues/600
                        .enumerate()
                        .find(|(_, time)| cur_min_time == *time)
                        .map(|(idx, _)| idx)
                        .unwrap(); // value always exists

                    self.time = cur_min_time;
                    self.value = if value_arr.is_null(index) {
                        None
                    } else {
                        Some(value_arr.value(index).to_owned())
                    };
                }

                Ok(())
            }

            fn size(&self) -> usize {
                // no nested types
                std::mem::size_of_val(self)
            }
        }
    };
}

macro_rules! make_last_selector {
    ($STRUCTNAME:ident, $RUSTTYPE:ident, $ARROWTYPE:expr, $ARRTYPE:ident, $MAXFUNC:ident, $TO_SCALARVALUE: expr) => {
        #[derive(Debug)]
        pub struct $STRUCTNAME {
            value: Option<$RUSTTYPE>,
            time: Option<i64>,
        }

        impl Default for $STRUCTNAME {
            fn default() -> Self {
                Self {
                    value: None,
                    time: None,
                }
            }
        }

        impl Selector for $STRUCTNAME {
            fn value_data_type() -> DataType {
                $ARROWTYPE
            }

            fn datafusion_state(&self) -> DataFusionResult<Vec<ScalarValue>> {
                Ok(vec![
                    $TO_SCALARVALUE(self.value.clone()),
                    ScalarValue::TimestampNanosecond(self.time, None),
                ])
            }

            fn evaluate(&self, output: &SelectorOutput) -> DataFusionResult<ScalarValue> {
                match output {
                    SelectorOutput::Value => Ok($TO_SCALARVALUE(self.value.clone())),
                    SelectorOutput::Time => Ok(ScalarValue::TimestampNanosecond(self.time, None)),
                    SelectorOutput::Struct => Ok(make_scalar_struct(vec![
                        $TO_SCALARVALUE(self.value.clone()),
                        ScalarValue::TimestampNanosecond(self.time, None),
                    ])),
                }
            }

            fn update_batch(
                &mut self,
                value_arr: &ArrayRef,
                time_arr: &ArrayRef,
            ) -> DataFusionResult<()> {
                let value_arr = value_arr
                    .as_any()
                    .downcast_ref::<$ARRTYPE>()
                    // the input type arguments should be ensured by datafusion
                    .expect("First argument was value");

                let time_arr = time_arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    // the input type arguments should be ensured by datafusion
                    .expect("Second argument was time");

                // Only look for times where the array also has a non
                // null value (the time array should have no nulls itself)
                //
                // For example, for the following input, the correct
                // current max time is 200 (not 300)
                //
                // value | time
                // --------------
                // A     | 100
                // B     | 200
                // NULL  | 300
                //
                // Note this could likely be faster if we used `ArrayData` APIs
                let time_arr: TimestampNanosecondArray = time_arr
                    .iter()
                    .zip(value_arr.iter())
                    .map(|(ts, value)| if value.is_some() { ts } else { None })
                    .collect();

                let cur_max_time = $MAXFUNC(&time_arr);

                let need_update = match (&self.time, &cur_max_time) {
                    (Some(time), Some(cur_max_time)) => time < cur_max_time,
                    // No existing maximum, so update needed
                    (None, Some(_)) => true,
                    // No actual maximum value found, so no update needed
                    (_, None) => false,
                };

                if need_update {
                    let index = time_arr
                        .iter()
                        // arrow doesn't tell us what index had the
                        // maximum, so need to find it ourselves
                        .enumerate()
                        .find(|(_, time)| cur_max_time == *time)
                        .map(|(idx, _)| idx)
                        .unwrap(); // value always exists

                    self.time = cur_max_time;
                    self.value = if value_arr.is_null(index) {
                        None
                    } else {
                        Some(value_arr.value(index).to_owned())
                    };
                }

                Ok(())
            }

            fn size(&self) -> usize {
                // no nested types
                std::mem::size_of_val(self)
            }
        }
    };
}

/// Did we find a new min/max
#[derive(Debug)]
enum ActionNeeded {
    UpdateValueAndTime,
    UpdateTime,
    Nothing,
}

impl ActionNeeded {
    fn update_value(&self) -> bool {
        match self {
            Self::UpdateValueAndTime => true,
            Self::UpdateTime => false,
            Self::Nothing => false,
        }
    }
    fn update_time(&self) -> bool {
        match self {
            Self::UpdateValueAndTime => true,
            Self::UpdateTime => true,
            Self::Nothing => false,
        }
    }
}

macro_rules! make_min_selector {
    ($STRUCTNAME:ident, $RUSTTYPE:ident, $ARROWTYPE:expr, $ARRTYPE:ident, $MINFUNC:ident, $TO_SCALARVALUE: expr) => {
        #[derive(Debug)]
        pub struct $STRUCTNAME {
            value: Option<$RUSTTYPE>,
            time: Option<i64>,
        }

        impl Default for $STRUCTNAME {
            fn default() -> Self {
                Self {
                    value: None,
                    time: None,
                }
            }
        }

        impl Selector for $STRUCTNAME {
            fn value_data_type() -> DataType {
                $ARROWTYPE
            }

            fn datafusion_state(&self) -> DataFusionResult<Vec<ScalarValue>> {
                Ok(vec![
                    $TO_SCALARVALUE(self.value.clone()),
                    ScalarValue::TimestampNanosecond(self.time, None),
                ])
            }

            fn evaluate(&self, output: &SelectorOutput) -> DataFusionResult<ScalarValue> {
                match output {
                    SelectorOutput::Value => Ok($TO_SCALARVALUE(self.value.clone())),
                    SelectorOutput::Time => Ok(ScalarValue::TimestampNanosecond(self.time, None)),
                    SelectorOutput::Struct => Ok(make_scalar_struct(vec![
                        $TO_SCALARVALUE(self.value.clone()),
                        ScalarValue::TimestampNanosecond(self.time, None),
                    ])),
                }
            }

            fn update_batch(
                &mut self,
                value_arr: &ArrayRef,
                time_arr: &ArrayRef,
            ) -> DataFusionResult<()> {
                use ActionNeeded::*;
                let value_arr = value_arr
                    .as_any()
                    .downcast_ref::<$ARRTYPE>()
                    // the input type arguments should be ensured by datafusion
                    .expect("First argument was value");

                let time_arr = time_arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    // the input type arguments should be ensured by datafusion
                    .expect("Second argument was time");

                let cur_min_value = $MINFUNC(&value_arr);

                let action_needed = match (&self.value, cur_min_value) {
                    (Some(value), Some(cur_min_value)) => {
                        if cur_min_value.lt_val(value) {
                            // new minimim found
                            UpdateValueAndTime
                        } else if cur_min_value.eq(value) {
                            // same minimum found, time might need update
                            UpdateTime
                        } else {
                            Nothing
                        }
                    }
                    // No existing minimum time, so update needed
                    (None, Some(_)) => UpdateValueAndTime,
                    // No actual minimum time  found, so no update needed
                    (_, None) => Nothing,
                };

                if action_needed.update_value() {
                    self.value = cur_min_value.map(|v| v.to_state());
                    self.time = None; // ignore time associated with old value
                }

                if action_needed.update_time() {
                    // arrow doesn't tell us what index(es) had the
                    // minimum value, so need to find them ourselves
                    // and compute the minimum timestamp found. See
                    // https://github.com/apache/arrow-datafusion/issues/600
                    self.time = value_arr
                        .iter()
                        .enumerate()
                        // stream of Option<i64>
                        .map(|(idx, value)| {
                            // Note: time should never be null but handle it anyways
                            let null_time = time_arr.is_null(idx);
                            if null_time {
                                debug!(idx, "MIN selector saw null time value");
                            }
                            if value == cur_min_value && !null_time {
                                Some(time_arr.value(idx))
                            } else {
                                None
                            }
                        })
                        // include existing time, potentially
                        .chain(std::iter::once(self.time.take()))
                        // clean out any Nones
                        .filter_map(|v| v)
                        .min();
                }
                Ok(())
            }

            fn size(&self) -> usize {
                // no nested types
                std::mem::size_of_val(self)
            }
        }
    };
}

macro_rules! make_max_selector {
    ($STRUCTNAME:ident, $RUSTTYPE:ident, $ARROWTYPE:expr, $ARRTYPE:ident, $MAXFUNC:ident, $TO_SCALARVALUE: expr) => {
        #[derive(Debug)]
        pub struct $STRUCTNAME {
            value: Option<$RUSTTYPE>,
            time: Option<i64>,
        }

        impl Default for $STRUCTNAME {
            fn default() -> Self {
                Self {
                    value: None,
                    time: None,
                }
            }
        }

        impl Selector for $STRUCTNAME {
            fn value_data_type() -> DataType {
                $ARROWTYPE
            }

            fn datafusion_state(&self) -> DataFusionResult<Vec<ScalarValue>> {
                Ok(vec![
                    $TO_SCALARVALUE(self.value.clone()),
                    ScalarValue::TimestampNanosecond(self.time, None),
                ])
            }

            fn evaluate(&self, output: &SelectorOutput) -> DataFusionResult<ScalarValue> {
                match output {
                    SelectorOutput::Value => Ok($TO_SCALARVALUE(self.value.clone())),
                    SelectorOutput::Time => Ok(ScalarValue::TimestampNanosecond(self.time, None)),
                    SelectorOutput::Struct => Ok(make_scalar_struct(vec![
                        $TO_SCALARVALUE(self.value.clone()),
                        ScalarValue::TimestampNanosecond(self.time, None),
                    ])),
                }
            }

            fn update_batch(
                &mut self,
                value_arr: &ArrayRef,
                time_arr: &ArrayRef,
            ) -> DataFusionResult<()> {
                use ActionNeeded::*;
                let value_arr = value_arr
                    .as_any()
                    .downcast_ref::<$ARRTYPE>()
                    // the input type arguments should be ensured by datafusion
                    .expect("First argument was value");

                let time_arr = time_arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    // the input type arguments should be ensured by datafusion
                    .expect("Second argument was time");

                let cur_max_value = $MAXFUNC(&value_arr);

                let action_needed = match (&self.value, &cur_max_value) {
                    (Some(value), Some(cur_max_value)) => {
                        if value.lt_val(cur_max_value) {
                            // new maximum found
                            UpdateValueAndTime
                        } else if cur_max_value.eq(value) {
                            // same maximum found, time might need update
                            UpdateTime
                        } else {
                            Nothing
                        }
                    }
                    // No existing maxmimum value, so update needed
                    (None, Some(_)) => UpdateValueAndTime,
                    // No actual maximum value found, so no update needed
                    (_, None) => Nothing,
                };

                if action_needed.update_value() {
                    self.value = cur_max_value.map(|v| v.to_state());
                    self.time = None; // ignore time associated with old value
                }

                // Note even though we are computing the MAX value,
                // the timestamp returned is the one with the *lowest*
                // numerical value
                if action_needed.update_time() {
                    // arrow doesn't tell us what index(es) had the
                    // minimum value, so need to find them ourselves
                    // and compute the minimum timestamp found. See
                    // https://github.com/apache/arrow-datafusion/issues/600
                    self.time = value_arr
                        .iter()
                        .enumerate()
                        .map(|(idx, value)| {
                            let null_time = time_arr.is_null(idx);
                            if null_time {
                                debug!(idx, "MAX selector saw null time value");
                            }
                            if value == cur_max_value && !null_time {
                                Some(time_arr.value(idx))
                            } else {
                                None
                            }
                        })
                        // include existing time, potentially
                        .chain(std::iter::once(self.time.take()))
                        // clean out any Nones
                        .filter_map(|v| v)
                        .min(); // still use min
                }
                Ok(())
            }

            fn size(&self) -> usize {
                // no nested types
                std::mem::size_of_val(self)
            }
        }
    };
}

// FIRST

make_first_selector!(
    F64FirstSelector,
    f64,
    DataType::Float64,
    Float64Array,
    array_min,
    ScalarValue::Float64
);
make_first_selector!(
    I64FirstSelector,
    i64,
    DataType::Int64,
    Int64Array,
    array_min,
    ScalarValue::Int64
);
make_first_selector!(
    U64FirstSelector,
    u64,
    DataType::UInt64,
    UInt64Array,
    array_min,
    ScalarValue::UInt64
);
make_first_selector!(
    Utf8FirstSelector,
    String,
    DataType::Utf8,
    StringArray,
    array_min,
    ScalarValue::Utf8
);
make_first_selector!(
    BooleanFirstSelector,
    bool,
    DataType::Boolean,
    BooleanArray,
    array_min,
    ScalarValue::Boolean
);

// LAST

make_last_selector!(
    F64LastSelector,
    f64,
    DataType::Float64,
    Float64Array,
    array_max,
    ScalarValue::Float64
);
make_last_selector!(
    I64LastSelector,
    i64,
    DataType::Int64,
    Int64Array,
    array_max,
    ScalarValue::Int64
);
make_last_selector!(
    U64LastSelector,
    u64,
    DataType::UInt64,
    UInt64Array,
    array_max,
    ScalarValue::UInt64
);
make_last_selector!(
    Utf8LastSelector,
    String,
    DataType::Utf8,
    StringArray,
    array_max,
    ScalarValue::Utf8
);
make_last_selector!(
    BooleanLastSelector,
    bool,
    DataType::Boolean,
    BooleanArray,
    array_max,
    ScalarValue::Boolean
);

// MIN

make_min_selector!(
    F64MinSelector,
    f64,
    DataType::Float64,
    Float64Array,
    array_min,
    ScalarValue::Float64
);
make_min_selector!(
    I64MinSelector,
    i64,
    DataType::Int64,
    Int64Array,
    array_min,
    ScalarValue::Int64
);
make_min_selector!(
    U64MinSelector,
    u64,
    DataType::UInt64,
    UInt64Array,
    array_min,
    ScalarValue::UInt64
);
make_min_selector!(
    Utf8MinSelector,
    String,
    DataType::Utf8,
    StringArray,
    array_min_string,
    ScalarValue::Utf8
);
make_min_selector!(
    BooleanMinSelector,
    bool,
    DataType::Boolean,
    BooleanArray,
    array_min_boolean,
    ScalarValue::Boolean
);

// MAX

make_max_selector!(
    F64MaxSelector,
    f64,
    DataType::Float64,
    Float64Array,
    array_max,
    ScalarValue::Float64
);
make_max_selector!(
    I64MaxSelector,
    i64,
    DataType::Int64,
    Int64Array,
    array_max,
    ScalarValue::Int64
);
make_max_selector!(
    U64MaxSelector,
    u64,
    DataType::UInt64,
    UInt64Array,
    array_max,
    ScalarValue::UInt64
);
make_max_selector!(
    Utf8MaxSelector,
    String,
    DataType::Utf8,
    StringArray,
    array_max_string,
    ScalarValue::Utf8
);
make_max_selector!(
    BooleanMaxSelector,
    bool,
    DataType::Boolean,
    BooleanArray,
    array_max_boolean,
    ScalarValue::Boolean
);
