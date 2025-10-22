use serde::{Deserialize, Serialize};

/// Nanoseconds has units of nanoseconds (naturally)
pub type Nanoseconds = i64;
/// TimestampNoUnits has unit information carried separately
pub type TimestampNoUnits = i64;

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

impl Precision {
    /// to_nanos returns a timestamp in nanoseconds respecting the precision units.
    ///
    /// If a timestamp isn't provided, the default timestamp is used, and that value is
    /// truncated (rounded down) to the nearest multiple of the precision. The default
    /// timestamp in practice is the ingest timestamp; this value is strictly positive.
    ///
    /// The method properly handles Precision::Auto which is meant to infer the units of
    /// the timestamp, but doesn't apply if the default is being used.
    ///
    /// The returned value has units of nanoseconds in all cases.
    pub fn to_nanos(
        &self,
        timestamp: Option<TimestampNoUnits>,
        default_timestamp: Nanoseconds,
    ) -> Result<Nanoseconds, anyhow::Error> {
        debug_assert!(
            default_timestamp >= 0,
            "in modern era, the default timestamp should be positive"
        );
        match timestamp {
            Some(ts) => {
                let multiplier = self.infer_precision(ts).multiplier();
                ts.checked_mul(multiplier).ok_or_else(|| {
                    anyhow::anyhow!("timestamp, {}, out of range for precision: {:?}", ts, self)
                })
            }
            None => Ok(self.truncate_to_precision(default_timestamp)),
        }
    }

    /// truncate_to_precision rounds the provided nanosecond value towards zero to a multiple
    /// of precision. If it's auto, nanos is returned unchanged.
    fn truncate_to_precision(&self, nanos: i64) -> i64 {
        match self {
            Precision::Auto => nanos,
            precision => {
                let multiplier = precision.multiplier();
                (nanos / multiplier) * multiplier
            }
        }
    }

    /// multiplier returns the value needed to scale a timestamp with precision units to nanoseconds.
    /// The returned value is also suitable to use for truncating a timestamp to a multiple.
    /// Note: in this context, Precision::Auto makes no sense and is a programming error.
    fn multiplier(&self) -> i64 {
        match self {
            Precision::Auto => panic!("no single multiplier for auto; must infer first."),
            Precision::Second => 1_000_000_000,
            Precision::Millisecond => 1_000_000,
            Precision::Microsecond => 1_000,
            Precision::Nanosecond => 1,
        }
    }

    /// infer_precision returns a precision for the given timestamp, inferring if Auto is the initial
    /// precision; this method never returns the Precision::Auto variant.
    fn infer_precision(&self, timestamp: TimestampNoUnits) -> Precision {
        if matches!(self, Precision::Auto) {
            Precision::guess_precision(timestamp)
        } else {
            *self
        }
    }

    /// Guess precision based off of a given timestamp.
    // Note that this will fail in June 2128, but that's not our problem
    fn guess_precision(timestamp: TimestampNoUnits) -> Precision {
        const NANO_SECS_PER_SEC: i64 = 1_000_000_000;
        // Get the absolute value of the timestamp so we can work with negative
        // numbers
        let val = timestamp.abs() / NANO_SECS_PER_SEC;

        if val < 5 {
            // If the time sent to us is in seconds then this will be a number less than
            // 5 so for example if the time in seconds is 1_708_976_567 then it will be
            // 1 (due to integer truncation) and be less than 5
            Precision::Second
        } else if val < 5_000 {
            // If however the value is milliseconds and not seconds than the same number
            // for time but now in milliseconds 1_708_976_567_000 when divided will now
            // be 1708 which is bigger than the previous if statement but less than this
            // one and so we return milliseconds
            Precision::Millisecond
        } else if val < 5_000_000 {
            // If we do the same thing here by going up another order of magnitude then
            // 1_708_976_567_000_000 when divided will be 1708976 which is large enough
            // for this if statement
            Precision::Microsecond
        } else {
            // Anything else we can assume is large enough of a number that it must
            // be nanoseconds
            Precision::Nanosecond
        }
    }
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

    const TEST_TS_SECONDS: i64 = 1_761_048_000;
    const TEST_TS_NANOS: i64 = 1_761_048_000_123_456_789;

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

    #[test]
    fn test_guess_precision() {
        assert_eq!(Precision::guess_precision(0), Precision::Second);
        assert_eq!(Precision::guess_precision(1), Precision::Second);
        assert_eq!(Precision::guess_precision(4_999_999_999), Precision::Second);
        assert_eq!(
            Precision::guess_precision(5_000_000_000),
            Precision::Millisecond
        );
        assert_eq!(
            Precision::guess_precision(TEST_TS_SECONDS),
            Precision::Second
        );
        assert_eq!(
            Precision::guess_precision(TEST_TS_SECONDS * 1_000),
            Precision::Millisecond
        );
        assert_eq!(
            Precision::guess_precision(4_999_999_999_999),
            Precision::Millisecond
        );
        assert_eq!(
            Precision::guess_precision(5_000_000_000_000),
            Precision::Microsecond
        );
        assert_eq!(
            Precision::guess_precision(TEST_TS_SECONDS * 1_000_000),
            Precision::Microsecond
        );
        assert_eq!(
            Precision::guess_precision(4_999_999_999_999_999),
            Precision::Microsecond
        );
        assert_eq!(
            Precision::guess_precision(5_000_000_000_000_000),
            Precision::Nanosecond
        );
        assert_eq!(
            Precision::guess_precision(TEST_TS_SECONDS * 1_000_000_000),
            Precision::Nanosecond
        );

        assert_eq!(Precision::guess_precision(-1), Precision::Second);
        assert_eq!(
            Precision::guess_precision(-4_999_999_999),
            Precision::Second
        );
        assert_eq!(
            Precision::guess_precision(-5_000_000_000),
            Precision::Millisecond
        );
        assert_eq!(
            Precision::guess_precision(-TEST_TS_SECONDS * 1_000),
            Precision::Millisecond
        );
        assert_eq!(
            Precision::guess_precision(-TEST_TS_SECONDS * 1_000_000),
            Precision::Microsecond
        );
        assert_eq!(
            Precision::guess_precision(-TEST_TS_SECONDS * 1_000_000_000),
            Precision::Nanosecond
        );
    }

    #[test]
    fn test_multiplier_all_precisions() {
        assert_eq!(Precision::Second.multiplier(), 1_000_000_000);
        assert_eq!(Precision::Millisecond.multiplier(), 1_000_000);
        assert_eq!(Precision::Microsecond.multiplier(), 1_000);
        assert_eq!(Precision::Nanosecond.multiplier(), 1);
    }

    #[test]
    fn test_truncate_to_precision() {
        assert_eq!(
            Precision::Second.truncate_to_precision(1_500_000_000),
            1_000_000_000
        );
        assert_eq!(
            Precision::Second.truncate_to_precision(-1_500_000_000),
            -1_000_000_000
        );
        assert_eq!(Precision::Second.truncate_to_precision(0), 0);
        assert_eq!(Precision::Second.truncate_to_precision(1), 0);
        assert_eq!(
            Precision::Second.truncate_to_precision(TEST_TS_NANOS),
            TEST_TS_SECONDS * 1_000_000_000
        );

        assert_eq!(
            Precision::Millisecond.truncate_to_precision(1_500_000),
            1_000_000
        );
        assert_eq!(
            Precision::Millisecond.truncate_to_precision(-1_500_000),
            -1_000_000
        );
        assert_eq!(
            Precision::Millisecond.truncate_to_precision(TEST_TS_NANOS / 1_000),
            TEST_TS_SECONDS * 1_000_000
        );

        assert_eq!(Precision::Microsecond.truncate_to_precision(1_500), 1_000);
        assert_eq!(Precision::Microsecond.truncate_to_precision(-1_500), -1_000);
        assert_eq!(
            Precision::Microsecond.truncate_to_precision(TEST_TS_NANOS / 1_000),
            (TEST_TS_NANOS / 1_000_000) * 1_000
        );

        assert_eq!(
            Precision::Nanosecond.truncate_to_precision(TEST_TS_NANOS),
            TEST_TS_NANOS
        );
        assert_eq!(
            Precision::Nanosecond.truncate_to_precision(-TEST_TS_NANOS),
            -TEST_TS_NANOS
        );

        assert_eq!(
            Precision::Auto.truncate_to_precision(TEST_TS_NANOS),
            TEST_TS_NANOS
        );
        assert_eq!(
            Precision::Auto.truncate_to_precision(-TEST_TS_NANOS),
            -TEST_TS_NANOS
        );
    }

    #[test]
    fn test_to_nanos_explicit_with_overflow() {
        let result = Precision::Second.to_nanos(Some(i64::MAX), 0);
        assert!(result.is_err());

        let result = Precision::Millisecond.to_nanos(Some(i64::MIN), 0);
        assert!(result.is_err());

        let near_max = i64::MAX / 1_000_000_000;
        let result = Precision::Second.to_nanos(Some(near_max), 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), near_max * 1_000_000_000);

        let near_min = i64::MIN / 1_000_000_000;
        let result = Precision::Second.to_nanos(Some(near_min), 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), near_min * 1_000_000_000);
    }

    #[test]
    fn test_to_nanos_explicit_all_precisions() {
        let expected_nanos = TEST_TS_SECONDS * 1_000_000_000;

        assert_eq!(
            Precision::Second
                .to_nanos(Some(TEST_TS_SECONDS), 0)
                .unwrap(),
            expected_nanos
        );
        assert_eq!(
            Precision::Auto.to_nanos(Some(TEST_TS_SECONDS), 0).unwrap(),
            expected_nanos
        );
    }

    #[test]
    fn test_to_nanos_default_truncation() {
        assert_eq!(
            Precision::Second.to_nanos(None, TEST_TS_NANOS).unwrap(),
            TEST_TS_SECONDS * 1_000_000_000
        );
        assert_eq!(
            Precision::Millisecond
                .to_nanos(None, TEST_TS_NANOS)
                .unwrap(),
            1_761_048_000_123_000_000
        );
        assert_eq!(
            Precision::Microsecond
                .to_nanos(None, TEST_TS_NANOS)
                .unwrap(),
            1_761_048_000_123_456_000
        );
        assert_eq!(
            Precision::Nanosecond.to_nanos(None, TEST_TS_NANOS).unwrap(),
            TEST_TS_NANOS
        );

        assert_eq!(
            Precision::Auto.to_nanos(None, TEST_TS_NANOS).unwrap(),
            TEST_TS_NANOS
        );

        assert_eq!(Precision::Second.to_nanos(None, 0).unwrap(), 0);
        assert_eq!(Precision::Millisecond.to_nanos(None, 0).unwrap(), 0);
    }
}
