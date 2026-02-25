//! Types to represent values produced by the InfluxQL queries in
//! InfluxDB. These types are used to serialize the results for the
//! v1 API.
use arrow::array::timezone::Tz;
use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray};
use arrow::datatypes::{
    DataType, Float64Type, Int32Type, Int64Type, TimeUnit, TimestampNanosecondType, UInt64Type,
};
use arrow::temporal_conversions::timestamp_ns_to_datetime;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use crate::types::Precision;

/// The InfluxQL type of a value.
#[derive(Debug, PartialEq)]
pub(crate) enum ValueType {
    Boolean,
    Integer,
    Float,
    String,
    Timestamp(Option<Arc<str>>),
    Unsigned,
    Null,
}

/// A velue produced by an InfluxQL query. This is a reference to a
/// single element of a an arrow array.
pub(crate) struct Value {
    arr: ArrayRef,
    row: usize,
}

impl Value {
    /// Create a new value wrapping the element at `row` in the `arr`
    /// array.
    pub(crate) fn new(arr: &ArrayRef, row: usize) -> Self {
        Self {
            arr: ArrayRef::clone(arr),
            row,
        }
    }

    /// Return the InfluxQL type of the value.
    pub(crate) fn value_type(&self) -> ValueType {
        match self.arr.data_type() {
            DataType::Boolean => ValueType::Boolean,
            DataType::Int64 => ValueType::Integer,
            DataType::Float64 | DataType::Float32 | DataType::Float16 => ValueType::Float,
            DataType::Utf8 => ValueType::String,
            DataType::Dictionary(k, v)
                if k.equals_datatype(&DataType::Int32) && v.equals_datatype(&DataType::Utf8) =>
            {
                ValueType::String
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => ValueType::Timestamp(tz.clone()),
            DataType::UInt64 => ValueType::Unsigned,
            DataType::Null => ValueType::Null,
            dt => panic!("Unsupported InfluxQL data type: {dt}"),
        }
    }

    /// Return the value as a boolean, if it is one.
    pub(crate) fn as_boolean_opt(&self) -> Option<bool> {
        if self.arr.is_valid(self.row) {
            self.arr.as_boolean_opt().map(|a| a.value(self.row))
        } else {
            None
        }
    }

    /// Return the value as an integer, if it is one.
    pub(crate) fn as_integer_opt(&self) -> Option<i64> {
        self.as_primitive_opt::<Int64Type>()
    }

    /// Return the value as a float, if it is one.
    pub(crate) fn as_float_opt(&self) -> Option<f64> {
        self.as_primitive_opt::<Float64Type>()
    }

    fn as_primitive_opt<T: ArrowPrimitiveType>(&self) -> Option<T::Native> {
        if self.arr.is_valid(self.row) {
            self.arr.as_primitive_opt::<T>().map(|a| a.value(self.row))
        } else {
            None
        }
    }

    /// Return the value as a string, if it is one.
    pub(crate) fn as_string_opt(&self) -> Option<&str> {
        if self.arr.is_valid(self.row) {
            let (arr, idx) = match self.arr.as_dictionary_opt::<Int32Type>() {
                Some(a) => (a.values(), a.key(self.row)),
                None => (&self.arr, Some(self.row)),
            };
            idx.and_then(|idx| arr.as_string_opt::<i32>().map(|a| a.value(idx)))
        } else {
            None
        }
    }

    /// Return the value as a timestamp, if it is one.
    pub(crate) fn as_timestamp_opt(&self) -> Option<DateTime<Utc>> {
        self.as_primitive_opt::<TimestampNanosecondType>()
            .and_then(timestamp_ns_to_datetime)
            .map(|t| t.and_utc())
    }

    /// Return the value as an unsigned integer, if it is one.
    pub(crate) fn as_unsigned_opt(&self) -> Option<u64> {
        self.as_primitive_opt::<UInt64Type>()
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.value_type() {
            ValueType::Boolean => write!(f, "{:?}", self.as_boolean_opt()),
            ValueType::Integer => write!(f, "{:?}", self.as_integer_opt()),
            ValueType::Float => write!(f, "{:?}", self.as_float_opt()),
            ValueType::String => write!(f, "{:?}", self.as_string_opt()),
            ValueType::Timestamp(_) => write!(f, "{:?}", self.as_timestamp_opt()),
            ValueType::Unsigned => write!(f, "{:?}", self.as_unsigned_opt()),
            ValueType::Null => write!(f, "null"),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.value_type() {
            ValueType::Boolean => self
                .as_boolean_opt()
                .map(|v| write!(f, "{v}"))
                .unwrap_or(Ok(())),
            ValueType::Integer => self
                .as_integer_opt()
                .map(|v| write!(f, "{v}"))
                .unwrap_or(Ok(())),
            ValueType::Float => self
                .as_float_opt()
                .map(|v| write!(f, "{v}"))
                .unwrap_or(Ok(())),
            ValueType::String => self
                .as_string_opt()
                .map(|v| write!(f, "{v}"))
                .unwrap_or(Ok(())),
            ValueType::Timestamp(tz) => {
                let tz = tz
                    .and_then(|tz| Tz::from_str(&tz).ok())
                    .unwrap_or_else(|| Tz::from_str("UTC").unwrap());
                self.as_timestamp_opt()
                    .map(|t| write!(f, "{}", t.with_timezone(&tz).to_rfc3339()))
                    .unwrap_or(Ok(()))
            }
            ValueType::Unsigned => self
                .as_unsigned_opt()
                .map(|v| write!(f, "{v}"))
                .unwrap_or(Ok(())),
            ValueType::Null => Ok(()),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        if self.value_type() != other.value_type() {
            return false;
        }
        match self.value_type() {
            ValueType::Boolean => self.as_boolean_opt() == other.as_boolean_opt(),
            ValueType::Integer => self.as_integer_opt() == other.as_integer_opt(),
            ValueType::Float => self.as_float_opt() == other.as_float_opt(),
            ValueType::String => self.as_string_opt() == other.as_string_opt(),
            ValueType::Timestamp(_) => self.as_timestamp_opt() == other.as_timestamp_opt(),
            ValueType::Unsigned => self.as_unsigned_opt() == other.as_unsigned_opt(),
            ValueType::Null => true,
        }
    }
}

pub(crate) struct ValueSerializer<'a> {
    value: &'a Value,
    epoch: Option<Precision>,
    // Allow infinite values
    allow_inf: bool,
}

impl<'a> ValueSerializer<'a> {
    pub(crate) fn new(value: &'a Value, epoch: Option<Precision>, allow_inf: bool) -> Self {
        Self {
            value,
            epoch,
            allow_inf,
        }
    }
}

impl Serialize for ValueSerializer<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.value.value_type() {
            ValueType::Boolean => {
                if let Some(v) = self.value.as_boolean_opt() {
                    serializer.serialize_bool(v)
                } else {
                    serializer.serialize_none()
                }
            }
            ValueType::Integer => {
                if let Some(v) = self.value.as_integer_opt() {
                    serializer.serialize_i64(v)
                } else {
                    serializer.serialize_none()
                }
            }
            ValueType::Float => {
                if let Some(v) = self.value.as_float_opt() {
                    if v.fract() == 0.0 && (v.abs() < (i64::MAX as f64)) {
                        // Only turn x.0 into x if it is small enough to fit in an i64.
                        // For example, 100.0 becomes 100.
                        // But 1_000_000_000_000_000_000_000.0 still stays as 1_000_000_000_000_000_000_000.0
                        // because it is too large to fit in an i64.
                        serializer.serialize_i64(v as i64)
                    } else if v.is_infinite() && !self.allow_inf {
                        // JSON and /query 1.x doesn't support infinite values
                        //
                        // https://www.rfc-editor.org/rfc/rfc4627#:~:text=Numeric%20values%20that%20cannot%20be%20represented%20as%20sequences%20of%20digits%0A%20%20%20(such%20as%20Infinity%20and%20NaN)%20are%20not%20permitted.
                        if v > 0.0 {
                            Err(serde::ser::Error::custom("json: unsupported value: +Inf"))
                        } else {
                            Err(serde::ser::Error::custom("json: unsupported value: -Inf"))
                        }
                    } else if v.is_nan() {
                        // /query 1.x serilizes NaN as null for json and msgpack
                        serializer.serialize_none()
                    } else {
                        serializer.serialize_f64(v)
                    }
                } else {
                    serializer.serialize_none()
                }
            }
            ValueType::String => {
                if let Some(v) = self.value.as_string_opt() {
                    serializer.serialize_str(v)
                } else {
                    serializer.serialize_none()
                }
            }
            ValueType::Timestamp(tz) => {
                if let Some(v) = self.value.as_timestamp_opt() {
                    match self.epoch {
                        Some(Precision::Nanoseconds) => {
                            chrono::serde::ts_nanoseconds::serialize(&v, serializer)
                        }
                        Some(Precision::Microseconds) => {
                            chrono::serde::ts_microseconds::serialize(&v, serializer)
                        }
                        Some(Precision::Milliseconds) => {
                            chrono::serde::ts_milliseconds::serialize(&v, serializer)
                        }
                        Some(Precision::Seconds) => {
                            chrono::serde::ts_seconds::serialize(&v, serializer)
                        }
                        Some(Precision::Minutes) => serializer.serialize_i64(v.timestamp() / 60),
                        Some(Precision::Hours) => {
                            serializer.serialize_i64(v.timestamp() / (60 * 60))
                        }
                        Some(Precision::Days) => {
                            serializer.serialize_i64(v.timestamp() / (60 * 60 * 24))
                        }
                        Some(Precision::Weeks) => {
                            serializer.serialize_i64(v.timestamp() / (60 * 60 * 24 * 7))
                        }
                        None => match tz.and_then(|tz| Tz::from_str(tz.as_ref()).ok()) {
                            Some(tz) => v
                                .with_timezone(&tz)
                                .to_rfc3339_opts(SecondsFormat::AutoSi, true)
                                .serialize(serializer),
                            None => v
                                .to_rfc3339_opts(SecondsFormat::AutoSi, true)
                                .serialize(serializer),
                        },
                    }
                } else {
                    serializer.serialize_none()
                }
            }
            ValueType::Unsigned => {
                if let Some(v) = self.value.as_unsigned_opt() {
                    serializer.serialize_u64(v)
                } else {
                    serializer.serialize_none()
                }
            }
            ValueType::Null => serializer.serialize_none(),
        }
    }
}
