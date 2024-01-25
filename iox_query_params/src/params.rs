//! General-purpose data type and utilities for working with
//! values that can be supplied as an InfluxDB bind parameter.
use std::{borrow::Cow, collections::HashMap};

use datafusion::scalar::ScalarValue;
use observability_deps::tracing::warn;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// remap protobuf types for convenience
mod proto {
    pub(super) use generated_types::influxdata::iox::querier::v1::read_info::{
        query_param::{NullValue, Value},
        QueryParam,
    };
}

#[derive(Debug, Error)]
/// Parameter errors
pub enum Error {
    /// Data conversion error
    #[error("{}", msg)]
    Conversion { msg: String },
}

/// A helper macro to construct a `HashMap` over `(String, StatementParam)` pairs.
#[macro_export]
macro_rules! params {
    () => (
        std::collections::HashMap::new()
    );
    ($($key:expr => $val:expr),+ $(,)?) => (
        std::collections::HashMap::from([$((String::from($key), $crate::StatementParam::from($val))),+])
    );
}

/// A collection of statement parameter (name,value) pairs.
///
/// This is a newtype wrapper to facillitate data conversions.
/// [From] instances can be used to convert to/from protobuf and JSON
/// protocol formats.
///
/// There is also a [From] instance to convert to
/// [datafusion::common::ParamValues] which makes it possible to pass
/// parameters into a [datafusion::logical_expr::LogicalPlan]
#[repr(transparent)]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatementParams(HashMap<String, StatementParam>);

impl StatementParams {
    /// Convert to internal representation.
    pub fn into_inner(&self) -> &HashMap<String, StatementParam> {
        &self.0
    }

    /// Convert into a HashMap of (name, value) pairs
    pub fn into_hashmap<V: From<StatementParam>>(self) -> HashMap<String, V> {
        self.0
            .into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect::<HashMap<String, V>>()
    }

    /// Convert to [datafusion::common::ParamValues] used by [datafusion::logical_expr::LogicalPlan]::with_param_values
    pub fn into_df_param_values(self) -> datafusion::common::ParamValues {
        self.into()
    }
}

/// From HashMap
impl From<HashMap<String, StatementParam>> for StatementParams {
    fn from(value: HashMap<String, StatementParam>) -> Self {
        Self(value)
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

/// Enum of possible data types that can be used as parameters in an InfluxQL query.
///
/// # creating values
///
/// [From] implementations for many builtin types are provided to make creation of parameter values
/// easier from the influxdb client.
///
/// # protocol formats
///
/// There are [From]/[TryFrom] implementations to convert to/from
/// protobuf and JSON. These are used for deserialization/serialization of
/// protocol messages across gRPC and the legacy REST API
///
/// # planning/execution
///
/// There is a [From] implementation to convert to DataFusion [ScalarValue]s. This
/// allows params to be passed into the DataFusion [datafusion::logical_expr::LogicalPlan]
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
            Self::UInt64(u) => write!(f, "{}", u),
            Self::Int64(i) => write!(f, "{}", i),
            Self::Float64(fl) => write!(f, "{}", fl),
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
#[allow(clippy::approx_constant)] // allow 3.14  >:)
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
        let params = result.unwrap().0;
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
        let params = result.unwrap().0;
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
}
