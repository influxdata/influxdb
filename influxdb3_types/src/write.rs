use serde::{Deserialize, Serialize};

/// The precision of the timestamp
///
/// Auto is only supported by v3 core and enterprise
/// Lowercase full names and short forms s, ms, us, u, ns, n are supported by cloud v3
/// Short forms s, ms, us, ns are supported by v2
/// Short forms h, m, s, ms, u, n are supported by v1, but h (hour) and m (minute) aren't supported here.
///
/// from_str is used by parse() within clap for the write command and
/// deserialization within request processing
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Precision {
    #[default]
    Auto,
    #[serde(alias = "s")]
    Second,
    #[serde(alias = "ms")]
    Millisecond,
    #[serde(alias = "us", alias = "u")]
    Microsecond,
    #[serde(alias = "ns", alias = "n")]
    Nanosecond,
}

impl From<iox_http::write::Precision> for Precision {
    fn from(legacy: iox_http::write::Precision) -> Self {
        match legacy {
            iox_http::write::Precision::Second => Precision::Second,
            iox_http::write::Precision::Millisecond => Precision::Millisecond,
            iox_http::write::Precision::Microsecond => Precision::Microsecond,
            iox_http::write::Precision::Nanosecond => Precision::Nanosecond,
        }
    }
}

impl std::str::FromStr for Precision {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let p = match s {
            "auto" => Self::Auto,
            "s" | "second" => Self::Second,
            "ms" | "millisecond" => Self::Millisecond,
            "us" | "u" | "microsecond" => Self::Microsecond,
            "ns" | "n" | "nanosecond" => Self::Nanosecond,
            _ => return Err(format!("unrecognized precision unit: {s}")),
        };
        Ok(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_precision_serialization_and_parsing() {
        let deserialization_expectations = [
            ("auto", Precision::Auto),
            ("s", Precision::Second),
            ("second", Precision::Second),
            ("ms", Precision::Millisecond),
            ("millisecond", Precision::Millisecond),
            ("us", Precision::Microsecond),
            ("u", Precision::Microsecond),
            ("microsecond", Precision::Microsecond),
            ("ns", Precision::Nanosecond),
            ("n", Precision::Nanosecond),
            ("nanosecond", Precision::Nanosecond),
        ];

        for (input, expected) in deserialization_expectations {
            let from_str_result = input.parse::<Precision>();
            assert!(
                from_str_result.is_ok(),
                "from_str failed for input: {}",
                input
            );
            let from_str_value = from_str_result.unwrap();
            assert_eq!(
                from_str_value, expected,
                "from_str produced wrong result for input: {}",
                input
            );

            let serde_result = serde_json::from_str::<Precision>(&format!("\"{}\"", input));
            assert!(
                serde_result.is_ok(),
                "serde deserialization failed for input: {}",
                input
            );
            let serde_value = serde_result.unwrap();
            assert_eq!(
                serde_value, expected,
                "serde produced wrong result for input: {}",
                input
            );

            // Verify from_str and deserialization match
            assert_eq!(
                from_str_value, serde_value,
                "from_str and serde mismatch for input: {}",
                input
            );
        }

        let serialization_tests = [
            (Precision::Auto, "auto"),
            (Precision::Second, "second"),
            (Precision::Millisecond, "millisecond"),
            (Precision::Microsecond, "microsecond"),
            (Precision::Nanosecond, "nanosecond"),
        ];

        for (precision, expected_output) in serialization_tests {
            let serialized = serde_json::to_string(&precision).unwrap();
            assert_eq!(
                serialized,
                format!("\"{}\"", expected_output),
                "Serialization failed for {:?}",
                precision
            );

            let deserialized = serde_json::from_str::<Precision>(&serialized).unwrap();
            assert_eq!(
                deserialized, precision,
                "Round-trip failed for {:?}",
                precision
            );
        }

        let invalid_inputs = ["invalid", "sec", "milliseconds", "micro", "nano"];
        for input in invalid_inputs {
            let from_str_result = input.parse::<Precision>();
            let serde_result = serde_json::from_str::<Precision>(&format!("\"{}\"", input));

            assert!(
                from_str_result.is_err(),
                "from_str should reject invalid input: {}",
                input
            );
            assert!(
                serde_result.is_err(),
                "serde should reject invalid input: {}",
                input
            );
        }
    }
}
