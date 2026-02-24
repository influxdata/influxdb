use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::SendableRecordBatchStream;
use serde::{Deserialize, Serialize};

use iox_query::query_log::PermitAndToken;

/// UNIX epoch precision.
/// Doc: <https://docs.influxdata.com/influxdb/v1/query_language/spec/#duration-units>
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub enum Precision {
    #[serde(rename = "ns")]
    Nanoseconds,
    #[serde(rename = "u", alias = "µ")]
    Microseconds,
    #[serde(rename = "ms")]
    Milliseconds,
    #[serde(rename = "s")]
    Seconds,
    #[serde(rename = "m")]
    Minutes,
    #[serde(rename = "h")]
    Hours,
    #[serde(rename = "d")]
    Days,
    #[serde(rename = "w")]
    Weeks,
}

/// An executing InfluxQL statement that produces results that can be
/// streamed as CSV.
pub(crate) struct Statement {
    pub schema: SchemaRef,
    /// Optional Permit/Token to support commands such as `SHOW DATABASES`,
    /// which do not go through the query planner/executor.
    pub permit_state: Option<PermitAndToken>,
    pub stream: SendableRecordBatchStream,
}

impl Statement {
    pub(crate) fn new(
        schema: SchemaRef,
        permit_state: Option<PermitAndToken>,
        stream: SendableRecordBatchStream,
    ) -> Self {
        Self {
            schema,
            permit_state,
            stream,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::{from_str, to_string};

    #[test]
    fn test_precision_deserialize() {
        // Test standard rename attributes
        assert_eq!(
            from_str::<Precision>(r#""ns""#).unwrap(),
            Precision::Nanoseconds
        );
        assert_eq!(
            from_str::<Precision>(r#""u""#).unwrap(),
            Precision::Microseconds
        );
        assert_eq!(
            from_str::<Precision>(r#""ms""#).unwrap(),
            Precision::Milliseconds
        );
        assert_eq!(from_str::<Precision>(r#""s""#).unwrap(), Precision::Seconds);
        assert_eq!(from_str::<Precision>(r#""m""#).unwrap(), Precision::Minutes);
        assert_eq!(from_str::<Precision>(r#""h""#).unwrap(), Precision::Hours);
        assert_eq!(from_str::<Precision>(r#""d""#).unwrap(), Precision::Days);
        assert_eq!(from_str::<Precision>(r#""w""#).unwrap(), Precision::Weeks);

        // Test the alias for Microseconds
        assert_eq!(
            from_str::<Precision>(r#""µ""#).unwrap(),
            Precision::Microseconds
        );
    }

    #[test]
    fn test_precision_serialize() {
        // Test that each enum variant serializes to the expected string
        assert_eq!(to_string(&Precision::Nanoseconds).unwrap(), r#""ns""#);
        assert_eq!(to_string(&Precision::Microseconds).unwrap(), r#""u""#); // Note: serializes to "u", not "µ"
        assert_eq!(to_string(&Precision::Milliseconds).unwrap(), r#""ms""#);
        assert_eq!(to_string(&Precision::Seconds).unwrap(), r#""s""#);
        assert_eq!(to_string(&Precision::Minutes).unwrap(), r#""m""#);
        assert_eq!(to_string(&Precision::Hours).unwrap(), r#""h""#);
        assert_eq!(to_string(&Precision::Days).unwrap(), r#""d""#);
        assert_eq!(to_string(&Precision::Weeks).unwrap(), r#""w""#);
    }

    #[test]
    fn test_precision_error_cases() {
        // Test invalid inputs
        let invalid_result = from_str::<Precision>(r#""invalid""#);
        assert!(invalid_result.is_err());

        // Test case sensitivity (serde is case-sensitive by default)
        let uppercase_result = from_str::<Precision>(r#""NS""#);
        assert!(uppercase_result.is_err());
    }

    #[test]
    fn test_precision_roundtrip() {
        // Test serialization and deserialization roundtrip for all variants
        let variants = vec![
            Precision::Nanoseconds,
            Precision::Microseconds,
            Precision::Milliseconds,
            Precision::Seconds,
            Precision::Minutes,
            Precision::Hours,
            Precision::Days,
            Precision::Weeks,
        ];

        for variant in variants {
            let serialized = to_string(&variant).unwrap();
            let deserialized = from_str::<Precision>(&serialized).unwrap();
            assert_eq!(variant, deserialized);
        }
    }
}
