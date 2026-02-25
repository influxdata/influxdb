//! Contains the IOx InfluxQL query planner

use arrow::datatypes::DataType;
use datafusion::{common::Result, scalar::ScalarValue};

use tracing::warn;
// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

mod aggregate;
mod error;
pub mod frontend;
pub mod plan;
pub mod show_databases;
pub mod show_retention_policies;
mod window;

/// A list of the numeric types supported by InfluxQL that can be be used
/// as input to user-defined functions.
static NUMERICS: &[DataType] = &[DataType::Int64, DataType::UInt64, DataType::Float64];

/// Calculate the nanosecond time difference, scaled to the unit.
pub(crate) fn delta_time(
    curr: &ScalarValue,
    prev: &ScalarValue,
    unit: &ScalarValue,
) -> Result<f64> {
    if let (
        ScalarValue::TimestampNanosecond(Some(curr), tz_curr),
        ScalarValue::TimestampNanosecond(Some(prev), tz_prev),
        ScalarValue::IntervalMonthDayNano(Some(unit)),
    ) = (curr, prev, unit)
    {
        if !tz_curr.eq(tz_prev) {
            warn!(
                "timezones do not match for the delta_time comparison, however, the scalar nanoseconds should always be in UTC"
            )
        }
        Ok(((*curr - *prev) as f64) / unit.nanoseconds as f64)
    } else {
        error::internal("time delta attempted on unsupported values")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::IntervalMonthDayNano;
    use assert_matches::assert_matches;

    use super::*;

    const NS_PER_SECOND: i64 = 1000000000;
    const NS_PER_HOUR: i64 = NS_PER_SECOND * 60 * 60;

    struct DeltaTimeCase {
        curr_ns: Option<i64>,
        prev_ns: Option<i64>,
        unit: Option<IntervalMonthDayNano>,
        expected: Result<f64>,
    }

    type TZ = Arc<str>;

    fn run_delta_time_test(delta_test_cases: Vec<DeltaTimeCase>) {
        let timezones: Vec<(Option<TZ>, Option<TZ>)> = vec![
            (None, None),
            // UTCs
            (Some("UTC".into()), None),
            (None, Some("UTC".into())),
            (Some("UTC".into()), Some("UTC".into())),
            // non-UTCs
            (Some("PDT".into()), None),
            (None, Some("PDT".into())),
            (Some("PDT".into()), Some("PDT".into())),
            // diff tz
            (Some("UTC".into()), Some("PDT".into())),
        ];

        for DeltaTimeCase {
            curr_ns,
            prev_ns,
            unit,
            expected,
        } in delta_test_cases
        {
            for (curr_tz, prev_tz) in timezones.clone() {
                let res = delta_time(
                    &ScalarValue::TimestampNanosecond(curr_ns, curr_tz),
                    &ScalarValue::TimestampNanosecond(prev_ns, prev_tz),
                    &ScalarValue::IntervalMonthDayNano(unit),
                );
                match expected {
                    Ok(expected) => {
                        assert_matches!(
                            res,
                            Ok(res) if res == expected,
                            "should have {} difference measured in {:?}, instead found {:?}", expected, unit, res
                        );
                    }
                    Err(_) => {
                        assert_matches!(res, Err(_), "should have error, instead found {:?}", res);
                    }
                }
            }
        }
    }

    #[test]
    fn test_delta_time() {
        let delta_test_cases = vec![
            // with prev as Some(0)
            DeltaTimeCase {
                curr_ns: Some(NS_PER_HOUR), // one hour later
                prev_ns: Some(0),
                unit: Some(IntervalMonthDayNano::new(0, 0, NS_PER_HOUR)), // diff in hours
                expected: Ok(1.0),
            },
            DeltaTimeCase {
                curr_ns: Some(NS_PER_HOUR), // one hour later
                prev_ns: Some(0),
                unit: Some(IntervalMonthDayNano::new(0, 0, NS_PER_SECOND)), // diff in seconds
                expected: Ok(60.0 * 60.0),
            },
            // with prev as > Some(0)
            DeltaTimeCase {
                curr_ns: Some(NS_PER_HOUR + NS_PER_HOUR * 12), // 12 hours later
                prev_ns: Some(NS_PER_HOUR),
                unit: Some(IntervalMonthDayNano::new(0, 0, NS_PER_HOUR)), // diff in hours
                expected: Ok(1.0 * 12.0),
            },
            DeltaTimeCase {
                curr_ns: Some(NS_PER_HOUR + NS_PER_HOUR * 12), // 12 hours later
                prev_ns: Some(NS_PER_HOUR),
                unit: Some(IntervalMonthDayNano::new(0, 0, NS_PER_SECOND)), // diff in seconds
                expected: Ok(60.0 * 60.0 * 12.0),
            },
        ];

        run_delta_time_test(delta_test_cases);
    }

    #[test]
    fn test_delta_time_different_units() {
        let inf = f64::INFINITY;

        let delta_test_cases = vec![
            // nanoseconds
            DeltaTimeCase {
                curr_ns: Some(NS_PER_HOUR + NS_PER_HOUR * 12), // 12 hours later
                prev_ns: Some(NS_PER_HOUR),
                unit: Some(IntervalMonthDayNano::new(0, 0, NS_PER_SECOND * 60 * 60)), // diff in hours
                expected: Ok(12.0),
            },
            // days, incorrect
            DeltaTimeCase {
                curr_ns: Some(NS_PER_HOUR + NS_PER_HOUR * 48), // 48 hours later
                prev_ns: Some(NS_PER_HOUR),
                unit: Some(IntervalMonthDayNano::new(0, 1, 0)), // diff in days
                // the contract of IntervalMonthDayNano assumes that the nanosecond units will always exist
                // TODO: should we be returning infinity in this case? or erroring?
                expected: Ok(inf),
            },
            // days, correct
            DeltaTimeCase {
                curr_ns: Some(NS_PER_HOUR + NS_PER_HOUR * 48), // 48 hours later
                prev_ns: Some(NS_PER_HOUR),
                unit: Some(IntervalMonthDayNano::new(0, 1, NS_PER_HOUR * 24)), // diff in days
                expected: Ok(2.0),
            },
            // month, incorrect
            DeltaTimeCase {
                curr_ns: Some(NS_PER_HOUR + NS_PER_HOUR * 24 * 31), // 1 month later
                prev_ns: Some(NS_PER_HOUR),
                unit: Some(IntervalMonthDayNano::new(1, 0, 0)), // diff in months
                // the contract of IntervalMonthDayNano assumes that the nanosecond units will always exist
                // TODO: should we be returning infinity in this case? or erroring?
                expected: Ok(inf),
            },
            // month, correct
            DeltaTimeCase {
                curr_ns: Some(NS_PER_HOUR + NS_PER_HOUR * 24 * 31), // 1 month later
                prev_ns: Some(NS_PER_HOUR),
                unit: Some(IntervalMonthDayNano::new(1, 0, NS_PER_HOUR * 24 * 31)), //  diff in months
                expected: Ok(1.0),
            },
        ];

        run_delta_time_test(delta_test_cases);
    }

    #[test]
    fn test_delta_time_should_error() {
        let delta_test_cases = vec![
            // curr & prev are None
            DeltaTimeCase {
                curr_ns: None,
                prev_ns: None,
                unit: Some(IntervalMonthDayNano::new(0, 0, NS_PER_HOUR)),
                expected: error::internal("delta attempted on unsupported values"),
            },
            // with prev as None
            DeltaTimeCase {
                curr_ns: Some(NS_PER_HOUR),
                prev_ns: None,
                unit: Some(IntervalMonthDayNano::new(0, 0, NS_PER_HOUR)),
                expected: error::internal("delta attempted on unsupported values"),
            },
            // with curr as None
            DeltaTimeCase {
                curr_ns: None,
                prev_ns: Some(0),
                unit: Some(IntervalMonthDayNano::new(0, 0, NS_PER_HOUR)),
                expected: error::internal("delta attempted on unsupported values"),
            },
            // with unit as None
            DeltaTimeCase {
                curr_ns: Some(NS_PER_HOUR),
                prev_ns: Some(0),
                unit: None,
                expected: error::internal("delta attempted on unsupported values"),
            },
        ];

        run_delta_time_test(delta_test_cases);
    }
}
