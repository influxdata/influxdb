//! General-purpose data type and utilities for working with
//! values that can be supplied as an InfluxDB bind parameter.
use std::{
    borrow::Cow,
    collections::{HashMap, hash_map},
    ops::Index,
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, AsArray},
    datatypes::{
        DataType, Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
        UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
    record_batch::RecordBatch,
};
use datafusion::scalar::ScalarValue;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

// remap protobuf types for convenience
mod proto {
    pub(super) use generated_types::influxdata::iox::querier::v1::{
        QueryParam,
        query_param::{NullValue, Value},
    };
}

#[derive(Debug, Error)]
/// Parameter errors
pub enum Error {
    /// Data conversion error
    #[error("{}", msg)]
    Conversion { msg: String },
}

/// A helper macro to construct a `StatementParams` collection.
/// ```
/// use iox_query_params::{params, StatementParams};
/// let my_params: StatementParams = params!(
///     "key1" => true,
///     "key2" => "string value",
///     "key3" => 1234,
///     "key4" => 3.14
/// );
/// assert_eq!(my_params.len(), 4);
/// ```
#[macro_export]
macro_rules! params {
    () => (
        $crate::StatementParams::new()
    );
    ($($key:expr => $val:expr),+ $(,)?) => (
        $crate::StatementParams::from([$((String::from($key), $crate::StatementParam::from($val))),+])
    );
}

/// A collection of statement parameter (name,value) pairs.
///
/// This is a collection wrapper to facilitate data conversions and provide
/// usability improvements for working with collections of parameters.
///
/// [From] and [TryFrom] instances can be used to convert to/from protobuf and JSON
/// protocol formats.
///
/// There is also a [From] instance to convert to
/// [datafusion::common::ParamValues] which makes it possible to pass
/// parameters into a [datafusion::logical_expr::LogicalPlan]
#[repr(transparent)]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatementParams(HashMap<String, StatementParam>);

impl StatementParams {
    /// Creates an empty parameter map.
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// An iterator visiting all parameter names. Currently this will iterate
    /// in an arbitrary order, but specific ordering is subject to change and shouldn't
    /// be relied upon.
    pub fn names(&self) -> Names<'_> {
        Names(self.0.keys())
    }

    /// Creates a consuming iterator visiting all parameter names. Currently this will iterate
    /// in an arbitrary order, but specific ordering is subject to change and shouldn't
    /// be relied upon.
    pub fn into_names(self) -> IntoNames {
        IntoNames(self.0.into_keys())
    }

    /// An iterator visiting all parameter values. Currently this will iterate
    /// in an arbitrary order, but specific ordering is subject to change and shouldn't
    /// be relied upon.
    pub fn values(&self) -> Values<'_> {
        Values(self.0.values())
    }

    /// An iterator visiting all parameter values as mutable references. Currently this will iterate
    /// in an arbitrary order, but specific ordering is subject to change and shouldn't
    /// be relied upon.
    pub fn values_mut(&mut self) -> ValuesMut<'_> {
        ValuesMut(self.0.values_mut())
    }

    /// Creates a consuming iterator visiting all parameter values. Currently this will iterate
    /// in an arbitrary order, but specific ordering is subject to change and shouldn't
    /// be relied upon.
    pub fn into_values(self) -> IntoValues {
        IntoValues(self.0.into_values())
    }

    /// An iterator visiting all name-value pairs. Currently this will iterate
    /// in an arbitrary order, but specific ordering is subject to change and shouldn't
    /// be relied upon.
    pub fn iter(&self) -> Iter<'_> {
        Iter(self.0.iter())
    }

    /// An iterator visiting all name-value pairs with mutable references to the values.
    /// Currently this will iterate in an arbitrary order, but specific ordering is subject
    /// to change and shouldn't be relied upon.
    pub fn iter_mut(&mut self) -> IterMut<'_> {
        IterMut(self.0.iter_mut())
    }

    /// Return the number of parameters in this collection.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true when there are no parameters defined in the collection.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Clears the map, removing all name-value pairs.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Retrieve the value of a parameter with the given name.
    pub fn get(&self, name: &str) -> Option<&StatementParam> {
        self.0.get(name)
    }

    pub fn get_name_value(&self, name: &str) -> Option<(&String, &StatementParam)> {
        self.0.get_key_value(name)
    }

    /// Returnd `true` if the map contains a parameter with the specified name.
    pub fn contains_name(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    /// Retrieves a mutable reference to the value corresponding to the given name.
    pub fn get_mut(&mut self, name: &str) -> Option<&mut StatementParam> {
        self.0.get_mut(name)
    }

    /// Inserts a parameter name-value pair into the map.
    ///
    /// If the map did not have this parameter name present, `None` is returned.
    ///
    /// If the map did have the parameter name present, the value is updated, and the old value is
    /// returned.
    pub fn insert(
        &mut self,
        name: impl Into<String>,
        value: impl Into<StatementParam>,
    ) -> Option<StatementParam> {
        self.0.insert(name.into(), value.into())
    }

    /// Attempts to insert the given name-value pair, using [`StatementParam::try_from`]
    /// on the given value prior to inserting into the map.
    ///
    /// If the [`StatementParam::try_from`] call results in an error, it will be returned as
    /// an `Err` result.
    ///
    /// If the map did not have this parameter name present, the value is inserted
    /// and `Ok(None)` is returned.
    ///
    /// If the map did have the parameter name present, the value is updated, and the old value is
    /// returned as an `Ok` result.
    pub fn try_insert<V>(
        &mut self,
        name: impl Into<String>,
        value: V,
    ) -> Result<Option<StatementParam>, V::Error>
    where
        V: TryInto<StatementParam>,
    {
        Ok(self.0.insert(name.into(), value.try_into()?))
    }

    /// Removes a parameter from the map by name, returning the parameter value if the name was previously in the map.
    pub fn remove(&mut self, name: &str) -> Option<StatementParam> {
        self.0.remove(name)
    }

    /// Removes a parameter from the map by name, returning the name-value pair if the parameter name was previously in the map.
    pub fn remove_entry(&mut self, name: &str) -> Option<(String, StatementParam)> {
        self.0.remove_entry(name)
    }

    /// Convert into a HashMap of (name, value) pairs
    pub fn into_hashmap<V: From<StatementParam>>(self) -> HashMap<String, V> {
        self.0
            .into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect::<HashMap<String, V>>()
    }
}

impl std::fmt::Display for StatementParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Params {{")?;
        for (name, value) in &self.0 {
            write!(f, " {name:?} => {value},")?;
        }
        write!(f, " }}")?;
        Ok(())
    }
}

impl Index<&str> for StatementParams {
    type Output = StatementParam;
    fn index(&self, index: &str) -> &Self::Output {
        self.0.index(index)
    }
}

/// From fixed-size array
impl<const N: usize> From<[(String, StatementParam); N]> for StatementParams {
    fn from(value: [(String, StatementParam); N]) -> Self {
        Self(HashMap::from(value))
    }
}

/// From HashMap
impl From<HashMap<String, StatementParam>> for StatementParams {
    fn from(value: HashMap<String, StatementParam>) -> Self {
        Self(value)
    }
}

/// Convert from an arrow [RecordBatch]
impl TryFrom<RecordBatch> for StatementParams {
    type Error = self::Error;
    fn try_from(batch: RecordBatch) -> Result<Self, Self::Error> {
        let mut params = HashMap::with_capacity(batch.num_columns());
        let schema = batch.schema();
        for field in schema.flattened_fields() {
            params.insert(
                field.name().clone(),
                StatementParam::try_from(Arc::clone(&batch[field.name()]))?,
            );
        }
        Ok(Self(params))
    }
}

/// To HashMap
impl From<StatementParams> for HashMap<String, StatementParam> {
    fn from(value: StatementParams) -> Self {
        value.0
    }
}

/// Converting to [datafusion::common::ParamValues] allows for
/// parameters to be passed to DataFusion
impl From<StatementParams> for datafusion::common::ParamValues {
    fn from(params: StatementParams) -> Self {
        Self::Map(params.into_hashmap())
    }
}

/// Convert from protobuf
impl TryFrom<Vec<proto::QueryParam>> for StatementParams {
    type Error = self::Error;
    fn try_from(proto: Vec<proto::QueryParam>) -> Result<Self, Self::Error> {
        let params = proto
            .into_iter()
            .map(|param| {
                match param.value {
                    Some(value) => Ok((param.name, StatementParam::from(value))),
                    None => Err(Error::Conversion {
                        msg: format!(
                            "Missing value for parameter \"{}\" when decoding query parameters in Flight gRPC ticket.",
                            param.name)
                    })
                }
            }).collect::<Result<HashMap<_, _>, _>>()?;
        Ok(Self(params))
    }
}

/// Convert into protobuf
impl From<StatementParams> for Vec<proto::QueryParam> {
    fn from(params: StatementParams) -> Self {
        params
            .0
            .into_iter()
            .map(|(name, value)| proto::QueryParam {
                name,
                value: Some(value.into()),
            })
            .collect()
    }
}

/// Creates a struct that wraps an existing collection iterator
macro_rules! iterator_wrapper {
    ($($lifetime:lifetime ,)? $iter_type:ident, $item_type:ty, $inner_type:ty  $(,)? ) => {

        #[derive(Debug)]
        #[repr(transparent)]
        pub struct $iter_type $(<$lifetime>)? ($inner_type);
        impl $(<$lifetime>)? Iterator for $iter_type $(<$lifetime>)? {
            type Item = $item_type;

            fn next(&mut self) -> Option<Self::Item> {
              self.0.next()
            }
        }

        impl $(<$lifetime>)? ExactSizeIterator for $iter_type $(<$lifetime>)? {
            fn len(&self) -> usize {
                self.0.len()
            }
        }

        impl $(<$lifetime>)? std::iter::FusedIterator for $iter_type $(<$lifetime>)? { }
    };
}
iterator_wrapper!('a, Names, &'a String, hash_map::Keys<'a, String, StatementParam>);
iterator_wrapper!(IntoNames, String, hash_map::IntoKeys<String, StatementParam>);
iterator_wrapper!('a, Values, &'a StatementParam, hash_map::Values<'a, String, StatementParam>);
iterator_wrapper!('a, ValuesMut, &'a mut StatementParam, hash_map::ValuesMut<'a, String, StatementParam>);
iterator_wrapper!(IntoValues, StatementParam, hash_map::IntoValues<String, StatementParam>);
iterator_wrapper!('a, Iter, (&'a String, &'a StatementParam), hash_map::Iter<'a, String, StatementParam>);
iterator_wrapper!('a, IterMut, (&'a String, &'a mut StatementParam), hash_map::IterMut<'a, String, StatementParam>);
iterator_wrapper!(IntoIter, (String, StatementParam), hash_map::IntoIter<String, StatementParam>);

impl IntoIterator for StatementParams {
    type Item = (String, StatementParam);
    type IntoIter = IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        IntoIter(self.0.into_iter())
    }
}

impl<'a> IntoIterator for &'a StatementParams {
    type Item = (&'a String, &'a StatementParam);
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<'a> IntoIterator for &'a mut StatementParams {
    type Item = (&'a String, &'a mut StatementParam);
    type IntoIter = IterMut<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl FromIterator<(String, StatementParam)> for StatementParams {
    fn from_iter<T: IntoIterator<Item = (String, StatementParam)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// Allows extending the parameters from an iterator
impl Extend<(String, StatementParam)> for StatementParams {
    fn extend<T: IntoIterator<Item = (String, StatementParam)>>(&mut self, iter: T) {
        self.0.extend(iter)
    }
}

/// Enum of possible data types that can be used as parameters in an InfluxDB query.
///
/// # Creating Values
///
/// [From] implementations for many builtin types are provided to make creation of parameter values
/// easier from the influxdb client.
///
/// # Protocol and serialized Formats
///
/// There are [From]/[TryFrom] implementations to convert to/from
/// protobuf and JSON. These are used for deserialization/serialization of
/// protocol messages across gRPC and the legacy REST API
///
/// # Planning/Execution
///
/// There is a [From] implementation to convert to DataFusion [ScalarValue]s. This
/// allows params to be passed into the DataFusion [datafusion::logical_expr::LogicalPlan]
///
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(try_from = "serde_json::Value", into = "serde_json::Value")]
pub enum StatementParam {
    /// a NULL value
    #[default]
    Null,
    /// a boolean value
    Boolean(bool),
    /// an unsigned integer value
    UInt64(u64),
    /// a signed integer value
    Int64(i64),
    /// a floating point value
    Float64(f64),
    /// a UTF-8 string value
    String(String),
}

/// Display as "SQL-like" literals
impl std::fmt::Display for StatementParam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Boolean(b) => write!(f, "{}", b.to_string().to_uppercase()),
            Self::UInt64(u) => write!(f, "{u}"),
            Self::Int64(i) => write!(f, "{i}"),
            Self::Float64(fl) => write!(f, "{fl}"),
            Self::String(s) => write!(f, "'{}'", s.replace('\'', "''")),
        }
    }
}

impl PartialEq<Self> for StatementParam {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Boolean(b1), Self::Boolean(b2)) => b1 == b2,
            (Self::UInt64(u1), Self::UInt64(u2)) => u1 == u2,
            (Self::Int64(i1), Self::Int64(i2)) => i1 == i2,
            (Self::Float64(f1), Self::Float64(f2)) => f1 == f2,
            (Self::String(s1), Self::String(s2)) => s1 == s2,
            // do not use a `_` pattern here because we want the exhaustiveness
            // check to fail if a new param variant is added
            (
                Self::Null
                | Self::Boolean(_)
                | Self::UInt64(_)
                | Self::Int64(_)
                | Self::Float64(_)
                | Self::String(_),
                _,
            ) => false,
        }
    }
}

impl Eq for StatementParam {}

/// Convert into protobuf representation
impl From<StatementParam> for proto::Value {
    fn from(value: StatementParam) -> Self {
        use proto::NullValue;
        match value {
            StatementParam::Null => Self::Null(NullValue::Unspecified.into()),
            StatementParam::Boolean(b) => Self::Boolean(b),
            StatementParam::UInt64(u) => Self::UInt64(u),
            StatementParam::Int64(i) => Self::Int64(i),
            StatementParam::Float64(f) => Self::Float64(f),
            StatementParam::String(s) => Self::String(s),
        }
    }
}

/// Convert into JSON representation
impl From<StatementParam> for serde_json::Value {
    fn from(param: StatementParam) -> Self {
        match param {
            StatementParam::Null => Self::Null,
            StatementParam::Boolean(b) => Self::Bool(b),
            StatementParam::Float64(f) => Self::from(f),
            StatementParam::UInt64(u) => Self::from(u),
            StatementParam::Int64(i) => Self::from(i),
            StatementParam::String(s) => Self::String(s),
        }
    }
}

/// Convert to DataFusion [ScalarValue]. This makes it possible to pass parameters
/// into a datafusion [datafusion::logical_expr::LogicalPlan]
impl From<StatementParam> for ScalarValue {
    fn from(value: StatementParam) -> Self {
        match value {
            StatementParam::Null => Self::Null,
            StatementParam::Boolean(b) => Self::Boolean(Some(b)),
            StatementParam::UInt64(u) => Self::UInt64(Some(u)),
            StatementParam::Int64(i) => Self::Int64(Some(i)),
            StatementParam::Float64(f) => Self::Float64(Some(f)),
            StatementParam::String(s) => Self::Utf8(Some(s)),
        }
    }
}

/// Convert from protobuf representation
impl From<proto::Value> for StatementParam {
    fn from(value: proto::Value) -> Self {
        match value {
            proto::Value::Null(n) => {
                const UNSPECIFIED: i32 = proto::NullValue::Unspecified as i32;
                if n != UNSPECIFIED {
                    warn!(
                        "Malformed Null in protobuf when decoding parameter \
                        value into StatementParam. Expected Null({UNSPECIFIED}) \
                        but found Null({n}). Possibly mismatched protobuf \
                        versions.
                        "
                    );
                }
                Self::Null
            }
            proto::Value::Boolean(b) => Self::Boolean(b),
            proto::Value::Float64(f) => Self::from(f),
            proto::Value::UInt64(u) => Self::from(u),
            proto::Value::Int64(i) => Self::from(i),
            proto::Value::String(s) => Self::String(s),
        }
    }
}

/// Convert from JSON representation
impl TryFrom<serde_json::Value> for StatementParam {
    type Error = self::Error;
    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        use serde_json::Value;
        match value {
            Value::Null => Ok(Self::Null),
            Value::Bool(b) => Ok(Self::Boolean(b)),
            Value::Number(n) => {
                if let Some(u) = n.as_u64() {
                    Ok(Self::UInt64(u))
                } else if let Some(i) = n.as_i64() {
                    Ok(Self::Int64(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(Self::Float64(f))
                } else {
                    // NOTE: without the "arbitrary_precision" feature enabled on serde_json,
                    // deserialization will never encounter this branch
                    Err(Error::Conversion {
                        msg: format!("Could not convert JSON number to i64 or f64: {n}"),
                    })
                }
            }
            Value::String(s) => Ok(Self::String(s)),
            Value::Array(_) => Err(Error::Conversion {
                msg: "JSON arrays are not supported as query parameters. Expected null, boolean, number, or string.".to_string(),
            }),
            Value::Object(_) => Err(Error::Conversion {
                msg: "JSON objects are not supported as query parameters. Expected null, boolean, number, or string".to_string(),
            }),
        }
    }
}

/// Convert from an arrow [ArrayRef] column. Only the first element in the array is used.
/// Fails when array length != 1
impl TryFrom<ArrayRef> for StatementParam {
    type Error = self::Error;
    fn try_from(arr: ArrayRef) -> Result<Self, Self::Error> {
        if arr.len() != 1 {
            return Err(Error::Conversion {
                msg: format!("Expected array to have length 1 but found {}", arr.len()),
            });
        }
        fn unsupported_type(type_name: &'static str) -> Result<StatementParam, Error> {
            Err(Error::Conversion {
                msg: format!(
                    "Arrow type {type_name} is not supported as query parameter. Expected null, boolean, numeric, or UTF-8 types"
                ),
            })
        }
        match arr.data_type() {
            DataType::Null => Ok(Self::Null),
            DataType::Boolean => Ok(Self::from(arr.as_boolean().value(0))),
            DataType::Int8 => Ok(Self::from(arr.as_primitive::<Int8Type>().value(0))),
            DataType::Int16 => Ok(Self::from(arr.as_primitive::<Int16Type>().value(0))),
            DataType::Int32 => Ok(Self::from(arr.as_primitive::<Int32Type>().value(0))),
            DataType::Int64 => Ok(Self::from(arr.as_primitive::<Int64Type>().value(0))),
            DataType::UInt8 => Ok(Self::from(arr.as_primitive::<UInt8Type>().value(0))),
            DataType::UInt16 => Ok(Self::from(arr.as_primitive::<UInt16Type>().value(0))),
            DataType::UInt32 => Ok(Self::from(arr.as_primitive::<UInt32Type>().value(0))),
            DataType::UInt64 => Ok(Self::from(arr.as_primitive::<UInt64Type>().value(0))),
            DataType::Float16 => Ok(Self::from(
                arr.as_primitive::<Float16Type>().value(0).to_f64(),
            )),
            DataType::Float32 => Ok(Self::from(arr.as_primitive::<Float32Type>().value(0))),
            DataType::Float64 => Ok(Self::from(arr.as_primitive::<Float64Type>().value(0))),
            DataType::Utf8 => Ok(Self::from(arr.as_string::<i32>().value(0))),
            DataType::LargeUtf8 => Ok(Self::from(arr.as_string::<i64>().value(0))),
            DataType::Timestamp(_, _) => unsupported_type("Timestamp"),
            DataType::Date32 => unsupported_type("Date32"),
            DataType::Date64 => unsupported_type("Date64"),
            DataType::Time32(_) => unsupported_type("Time32"),
            DataType::Time64(_) => unsupported_type("Time64"),
            DataType::Duration(_) => unsupported_type("Duration"),
            DataType::Interval(_) => unsupported_type("Interval"),
            DataType::Binary => unsupported_type("Binary"),
            DataType::FixedSizeBinary(_) => unsupported_type("FixedSizeBinary"),
            DataType::LargeBinary => unsupported_type("LargeBinary"),
            DataType::List(_) => unsupported_type("List"),
            DataType::FixedSizeList(_, _) => unsupported_type("FixedSizeList"),
            DataType::LargeList(_) => unsupported_type("LargeList"),
            DataType::Struct(_) => unsupported_type("Struct"),
            DataType::Union(_, _) => unsupported_type("Union"),
            DataType::Dictionary(_, _) => unsupported_type("Dictionary"),
            DataType::Decimal32(_, _) => unsupported_type("Decimal32"),
            DataType::Decimal64(_, _) => unsupported_type("Decimal64"),
            DataType::Decimal128(_, _) => unsupported_type("Decimal128"),
            DataType::Decimal256(_, _) => unsupported_type("Decimal256"),
            DataType::Map(_, _) => unsupported_type("Map"),
            DataType::RunEndEncoded(_, _) => unsupported_type("RunEndEncoded"),
            DataType::BinaryView => unsupported_type("BinaryView"),
            DataType::Utf8View => unsupported_type("Utf8View"),
            DataType::ListView(_) => unsupported_type("ListView"),
            DataType::LargeListView(_) => unsupported_type("LargeListView"),
        }
    }
}

/// [`Option`] values are unwrapped and [`None`] values are converted to NULL
impl<T> From<Option<T>> for StatementParam
where
    Self: From<T>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            None => Self::Null,
            Some(value) => value.into(),
        }
    }
}

/// Unit type is converted to NULL
impl From<()> for StatementParam {
    fn from(_value: ()) -> Self {
        Self::Null
    }
}

impl From<bool> for StatementParam {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<u8> for StatementParam {
    fn from(value: u8) -> Self {
        Self::UInt64(value as u64)
    }
}

impl From<u16> for StatementParam {
    fn from(value: u16) -> Self {
        Self::UInt64(value as u64)
    }
}

impl From<u32> for StatementParam {
    fn from(value: u32) -> Self {
        Self::UInt64(value as u64)
    }
}

impl From<u64> for StatementParam {
    fn from(value: u64) -> Self {
        Self::UInt64(value)
    }
}

impl From<usize> for StatementParam {
    fn from(value: usize) -> Self {
        Self::UInt64(value.try_into().unwrap())
    }
}

impl From<i8> for StatementParam {
    fn from(value: i8) -> Self {
        Self::Int64(value as i64)
    }
}

impl From<i16> for StatementParam {
    fn from(value: i16) -> Self {
        Self::Int64(value as i64)
    }
}

impl From<i32> for StatementParam {
    fn from(value: i32) -> Self {
        Self::Int64(value.into())
    }
}

impl From<i64> for StatementParam {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}

impl From<isize> for StatementParam {
    fn from(value: isize) -> Self {
        Self::Int64(value.try_into().unwrap())
    }
}

impl From<f32> for StatementParam {
    fn from(value: f32) -> Self {
        Self::Float64(value.into())
    }
}

impl From<f64> for StatementParam {
    fn from(value: f64) -> Self {
        Self::Float64(value)
    }
}

impl From<&str> for StatementParam {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<String> for StatementParam {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl<'a> From<Cow<'a, str>> for StatementParam {
    fn from(value: Cow<'a, str>) -> Self {
        Self::String(value.into_owned())
    }
}

#[cfg(test)]
#[expect(clippy::approx_constant)] // allow 3.14  >:)
mod tests {
    use assert_matches::assert_matches;
    use serde_json::json;

    use super::*;

    #[test]
    fn params_from_protobuf_value() {
        // empty case
        assert_matches!(StatementParams::try_from(vec![]), Ok(StatementParams(hm)) if hm.is_empty());

        // test happy path with all value types
        let proto: Vec<proto::QueryParam> = [
            ("foo", proto::Value::String("Test String".to_string())),
            ("bar", proto::Value::Float64(3.14)),
            ("baz", proto::Value::UInt64(1234)),
            ("int", proto::Value::Int64(-1234)),
            ("1", proto::Value::Boolean(false)),
            ("2", proto::Value::Null(0)),
        ]
        .map(|(key, value)| proto::QueryParam {
            name: key.to_string(),
            value: Some(value),
        })
        .into();
        let result = StatementParams::try_from(proto);
        let params = result.unwrap();
        assert_eq!(
            params,
            params! {
                "foo" => "Test String",
                "bar" => 3.14_f64,
                "baz" => 1234_u64,
                "int" => -1234_i64,
                "1" => false,
                "2" => StatementParam::Null,
            }
        );
    }

    #[test]
    fn params_from_json_values() {
        use serde_json::Value;
        assert_matches!(
            StatementParam::try_from(Value::from("Test String")),
            Ok(StatementParam::String(s)) if s == "Test String");
        assert_matches!(
            StatementParam::try_from(Value::from(3.14)),
            Ok(StatementParam::Float64(n)) if n == 3.14
        );
        assert_matches!(
            StatementParam::try_from(Value::from(1234)),
            Ok(StatementParam::UInt64(1234))
        );
        assert_matches!(
            StatementParam::try_from(Value::from(-1234)),
            Ok(StatementParam::Int64(-1234))
        );
        assert_matches!(
            StatementParam::try_from(Value::from(false)),
            Ok(StatementParam::Boolean(false))
        );
        assert_matches!(
            StatementParam::try_from(Value::Null),
            Ok(StatementParam::Null)
        );
        // invalid values
        assert_matches!(
            StatementParam::try_from(json!([1, 2, 3])),
            Err(Error::Conversion { .. })
        );
        assert_matches!(
            StatementParam::try_from(json!({ "a": 1, "b": 2, "c": 3})),
            Err(Error::Conversion { .. })
        );
    }

    #[test]
    fn params_from_json_str() {
        let json = r#"
            {
                "foo": "Test String",
                "bar": 3.14,
                "baz": 1234,
                "int": -1234,
                "1": false,
                "2": null
            }
        "#;
        let result = serde_json::from_str::<StatementParams>(json);
        let params = result.unwrap();
        assert_eq!(
            params,
            params! {
                "foo" => "Test String",
                "bar" => 3.14_f64,
                "baz" => 1234_u64,
                "int" => -1234_i64,
                "1" => false,
                "2" => StatementParam::Null,
            }
        );
    }

    #[test]
    fn params_from_json_str_invalid() {
        // invalid top-level values
        assert_matches!(serde_json::from_str::<StatementParams>("null"), Err(_));
        assert_matches!(serde_json::from_str::<StatementParams>("100"), Err(_));
        assert_matches!(serde_json::from_str::<StatementParams>("3.14"), Err(_));
        assert_matches!(serde_json::from_str::<StatementParams>("true"), Err(_));
        assert_matches!(serde_json::from_str::<StatementParams>("[\"foo\"]"), Err(_));

        // nested lists are invalid
        let json = r#"
            {
                "foo": [],
            }
        "#;
        let result = serde_json::from_str::<StatementParams>(json);
        assert_matches!(result, Err(serde_json::Error { .. }));

        // nested objects are invalid
        let json = r#"
            {
                "foo": {},
            }
        "#;
        let result = serde_json::from_str::<StatementParams>(json);
        assert_matches!(result, Err(serde_json::Error { .. }));

        // nested list with contents
        let json = r#"
            {
                "foo bar": [1, 2, "3", "4 5 6", [null], [[]], {}],
                "baz": null
            }
        "#;
        let result = serde_json::from_str::<StatementParams>(json);
        assert_matches!(result, Err(serde_json::Error { .. }));

        // nested object with contents
        let json = r#"
            {
                "fazbar": {
                    "a": 1,
                    "b": 2,
                    "c": null
                },
                "baz": null
            }
        "#;
        let result = serde_json::from_str::<StatementParams>(json);
        assert_matches!(result, Err(serde_json::Error { .. }));
    }

    // tests what happens when integer and float are out of bounds
    //
    // without `arbitrary_precision` flag, `serde_json` will always deserialize numbers to
    // either i64 or f64.
    //
    // one potential edge case to be aware of is what happens when `serde_json::Value`` deserializes
    // an integer number that's out-of-bounds for i64, but in-bounds for f64. In this case
    // it will be interpreted as a float, and rounding errors can be introduced. This case
    // is unlikely to occur as long as clients are properly validating that their integers
    // are within 64-bit bounds, but it's possible that a client serializing a bigdecimal could
    // encounter this case. this has not been testing when `serde_json` has `arbitrary_precision` enabled,
    // so it's possible adding that feature would prevent rounding errors in this case.
    // supporting bigdecimal parameters would also fix this edge case.
    #[test]
    fn params_from_json_str_bignum() {
        let json = format! {" {{ \"abc\" : {}999 }} ", f64::MAX};
        let result = serde_json::from_str::<StatementParams>(&json);
        // NOTE: without the "arbitrary_precision" feature enabled on serde_json, deserialization will never encounter
        // our out-of-bounds guard
        let err = result.unwrap_err();
        assert!(err.to_string().contains("number out of range"));
    }

    #[test]
    fn params_conversions() {
        assert_matches!(StatementParam::from(true), StatementParam::Boolean(true));
        assert_matches!(StatementParam::from(123_u32), StatementParam::UInt64(123));
        assert_matches!(StatementParam::from(-123), StatementParam::Int64(-123));
        assert_matches!(StatementParam::from(1.23), StatementParam::Float64(f) if f == 1.23);
        assert_matches!(StatementParam::from("a string"), StatementParam::String(s) if s == "a string");
        assert_matches!(StatementParam::from("a string".to_owned()), StatementParam::String(s) if s == "a string");
        assert_matches!(StatementParam::from(Cow::from("a string")), StatementParam::String(s) if s == "a string");
        assert_matches!(StatementParam::from(()), StatementParam::Null);
        assert_matches!(
            StatementParam::from(None::<Option<bool>>),
            StatementParam::Null
        );
        assert_matches!(
            StatementParam::from(Some(true)),
            StatementParam::Boolean(true)
        );
        assert_matches!(
            StatementParam::from(Some(123_u32)),
            StatementParam::UInt64(123)
        );
        assert_matches!(
            StatementParam::from(Some(-123)),
            StatementParam::Int64(-123)
        );
        assert_matches!(StatementParam::from(Some(1.23)), StatementParam::Float64(f) if f == 1.23);
        assert_matches!(StatementParam::from(Some("a string")), StatementParam::String(s) if s == "a string");
        assert_matches!(StatementParam::from(Some("a string".to_owned())), StatementParam::String(s) if s == "a string");
        assert_matches!(StatementParam::from(Some(Cow::from("a string"))), StatementParam::String(s) if s == "a string");
        assert_matches!(StatementParam::from(Some(())), StatementParam::Null);
        assert_matches!(
            StatementParam::from(Some(None::<Option<i32>>)),
            StatementParam::Null
        );
        assert_matches!(
            StatementParam::from(Some(Some(true))),
            StatementParam::Boolean(true)
        );
    }

    // test equality comparisons for StatementParams
    #[test]
    fn params_equality() {
        let values = [
            StatementParam::Null,
            StatementParam::from(true),
            StatementParam::from(32_u32),
            StatementParam::from(-23),
            StatementParam::from(32.23),
            StatementParam::from("a string"),
        ];
        for (i, value1) in values.iter().enumerate() {
            for (j, value2) in values.iter().enumerate() {
                if i == j {
                    assert_eq!(value1, value2);
                } else {
                    assert_ne!(value1, value2);
                }
            }
        }
        assert_ne!(StatementParam::from(true), StatementParam::from(false));
        assert_ne!(
            StatementParam::from(1984_u32),
            StatementParam::from(2077_u32)
        );
        assert_ne!(StatementParam::from(-100), StatementParam::from(100));
        assert_ne!(StatementParam::from(-1.23), StatementParam::from(1.23));
        assert_ne!(
            StatementParam::from("string1"),
            StatementParam::from("string2")
        );
    }

    // Test try_insert for StatementParams
    #[test]
    fn params_try_insert() {
        let mut params = StatementParams::new();
        // test From instance
        let value = params.try_insert("param1", "string value").unwrap();
        assert_eq!(value, None);
        // test a TryFrom instance that succeeds
        let value = params.try_insert("param2", json!(true)).unwrap();
        assert_eq!(value, None);
        // test updating an existing param with a TryFrom instance
        let value = params
            .try_insert("param2", json!("string from JSON"))
            .unwrap();
        assert_eq!(value, Some(StatementParam::Boolean(true)));
        // invalid conversion - JSON object cannot be converted to param
        let err = params.try_insert("param3", json!({})).unwrap_err();
        assert_matches!(err, Error::Conversion { .. });
        assert_eq!(params.len(), 2);
        assert_eq!(
            params,
            params!(
                "param1" => "string value",
                "param2" => "string from JSON"
            )
        );
    }
}
