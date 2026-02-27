//! Implementation of the date_bin_wallclock UDF.

use arrow::array::TimestampNanosecondBuilder;
use arrow::array::timezone::Tz;
use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};
use arrow::temporal_conversions::timestamp_ns_to_datetime;
use chrono::{MappedLocalTime, Offset, TimeZone};
use datafusion::common::cast::as_timestamp_nanosecond_array;
use datafusion::common::{Result, exec_err, internal_err, not_impl_err};
use datafusion::error::DataFusionError;
use datafusion::functions::datetime::date_bin;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TIMEZONE_WILDCARD,
    TypeSignature, Volatility,
};
use datafusion::scalar::ScalarValue;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

/// The name of the date_bin_gapfill UDF given to DataFusion.
pub(crate) const DATE_BIN_WALLCLOCK_UDF_NAME: &str = "date_bin_wallclock";

pub(crate) static DATE_BIN_WALLCLOCK_UDF: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(DateBinWallclockUDF::default())));

/// Get a reference to the date_bin_wallclock ScalarUDF instance.
pub fn date_bin_wallclock() -> Arc<ScalarUDF> {
    Arc::clone(&*DATE_BIN_WALLCLOCK_UDF)
}

/// Inplementation of the date_bin_wallclock scalar function.
/// This function is similar to date_bin, but the bins are defined
/// using wallclock time in the timezone of the input. The behaviour
/// is based on versions 1.x and 2.x of InfluxDB.
///
/// The function signature is either:
///
/// ```text
/// date_bin_wallclock(interval, timestamp)
/// ```
///
/// or:
///
/// ```text
/// date_bin_wallclock(interval, timestamp, origin)
/// ```
///
/// The `interval` argument is a literal interval defining the size
/// of the bins. Intervals containing months or years are not supported.
///
/// The `timestamp` argument is a timestamp column or literal. The timezone
/// used to determine the wallclock time is taken from the data type of
/// this argument. The result will have the same data type (including timezone).
///
/// The `origin` argument can be used to offset the start of the bins.
/// This must be a wallclock timestamp, and cannot have a timezone. If
/// omitted then the value `1970-01-01T00:00:00` is used.
///
/// For the `UTC` timezone this function should behave identically to
/// `date_bin`, which should be preferred. Also intervals that are short
/// enough that compensating for daylight savings offsets is unnecessary
/// should use `date_bin` instead.
///
/// ## Daylight Saving Time
///
/// Many regions use daylight saving time which introduce discontinuities
/// into the wallclock time. Typically an hour is skipped at the start of
/// daylight savings and repeated at the end.
///
/// If a wallclock time bin starts at a time that does not exist in a
/// region then it is adjusted to the time that is same offset from the
/// start of the day in the region that the wallclock time would have been.
///
/// If a wallclock time represents an ambiguous time in the region then
/// the behaviour depends on the size of the interval. If the interval is
/// larger than the difference between the two possible timestamps then
/// the earlier timestamp is used. Otherwise the timestamp that matches
/// the UTC offset of the input is used.
///
/// N.B. Using an origin and interval such that there are bins within a
/// discontinuity, but do not align with the point that the discontinuity
/// happens can produce unexpected results. For example the result of
/// `SELECT date_bin_wallclock(INTERVAL '1 hour', '2020-10-25T02:29:00+0100'
/// AT TIME ZONE 'Europe/Paris', '1970-01-01T00:30:00') AT TIME ZONE 'UTC';`
/// is `2020-10-24T23:30:00Z`, but the result of `SELECT date_bin_wallclock(INTERVAL
/// '1 hour', '2020-10-25T02:30:00+0100' AT TIME ZONE 'Europe/Paris',
/// '1970-01-01T00:30:00') AT TIME ZONE 'UTC';` is `2020-10-25T01:30:00Z`. I.e.
/// timestamps that are one minute apart are put into bins 2 hours apart, despite
/// expected interval being 1 hour. Care should be taken to avoid such cases.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DateBinWallclockUDF {
    signature: Signature,
    date_bin: Arc<ScalarUDF>,
}

impl DateBinWallclockUDF {
    /// Create a new instance of the date_bin_wallclock UDF, using the
    /// provided date_bin scalar function.
    pub fn new(date_bin: Arc<ScalarUDF>) -> Self {
        let signatures = vec![
            TypeSignature::Exact(vec![
                DataType::Interval(IntervalUnit::MonthDayNano),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ]),
            TypeSignature::Exact(vec![
                DataType::Interval(IntervalUnit::MonthDayNano),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ]),
            TypeSignature::Exact(vec![
                DataType::Interval(IntervalUnit::MonthDayNano),
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from(TIMEZONE_WILDCARD))),
            ]),
            TypeSignature::Exact(vec![
                DataType::Interval(IntervalUnit::MonthDayNano),
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from(TIMEZONE_WILDCARD))),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ]),
        ];
        Self {
            signature: Signature::one_of(signatures, Volatility::Immutable),
            date_bin,
        }
    }
}

impl Default for DateBinWallclockUDF {
    fn default() -> Self {
        Self::new(date_bin())
    }
}

impl ScalarUDFImpl for DateBinWallclockUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        DATE_BIN_WALLCLOCK_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(args[1].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field,
            config_options,
        } = args;

        if args.len() != arg_fields.len() {
            return internal_err!("args and fields do not align");
        }

        let ((interval_arg, internal_field), (source_arg, source_field), origin) =
            match args.as_slice() {
                [interval, source] => ((interval, &arg_fields[0]), (source, &arg_fields[1]), None),
                [interval, source, origin] => (
                    (interval, &arg_fields[0]),
                    (source, &arg_fields[1]),
                    Some((origin, &arg_fields[2])),
                ),
                _ => return exec_err!("DATE_BIN_WALLCLOCK invalid number of arguments"),
            };
        let DataType::Timestamp(_, tz) = source_arg.data_type() else {
            return exec_err!(
                "DATE_BIN_WALLCLOCK expects a timestamp value, not {:?}",
                source_arg.data_type()
            );
        };
        let origin = origin.map(|(origin_arg, origin_field)| match origin_arg {
            ColumnarValue::Scalar(scalar) => {
                match scalar {
                    ScalarValue::TimestampNanosecond(origin, _) => Ok((*origin, origin_field)),
                    v => exec_err!(
                        "DATE_BIN_WALLCLOCK expects origin argument to be a TIMESTAMP with nanosecond precision but got {}",
                        v.data_type()
                    ),
                }
            }
            _ => not_impl_err!("DATE_BIN_WALLCLOCK only supports literal values for the origin argument, not arrays"),
        }).transpose()?;

        if is_utc(&tz) {
            // date_bin_wallclock works exactly the same as date_bin for
            // UTC like times. Pass execution off to date_bin.
            return match origin {
                Some((origin, origin_field)) => {
                    // DATE_BIN_WALLCLOCK requires that the origin parameter has no time zone,
                    // according to the signature this isn't valid for DATE_BIN. The value doesn't
                    // need adjusting but set the timezone to match the times array.
                    self.date_bin.invoke_with_args(ScalarFunctionArgs {
                        args: vec![
                            interval_arg.clone(),
                            source_arg.clone(),
                            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                                origin,
                                tz.clone(),
                            )),
                        ],
                        arg_fields: vec![
                            Arc::clone(internal_field),
                            Arc::clone(source_field),
                            Arc::new(
                                origin_field
                                    .as_ref()
                                    .clone()
                                    .with_data_type(DataType::Timestamp(TimeUnit::Nanosecond, tz)),
                            ),
                        ],
                        number_rows,
                        return_field,
                        config_options,
                    })
                }
                None => self.date_bin.invoke_with_args(ScalarFunctionArgs {
                    args: vec![interval_arg.clone(), source_arg.clone()],
                    arg_fields: vec![Arc::clone(internal_field), Arc::clone(source_field)],
                    number_rows,
                    return_field,
                    config_options,
                }),
            };
        }

        let arrowtz = Tz::from_str(tz.as_ref().unwrap().as_ref())
            .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))?;

        let interval_ns = match &interval_arg {
            ColumnarValue::Scalar(scalar) => {
                let ScalarValue::IntervalMonthDayNano(interval) = scalar else {
                    return exec_err!("DATE_BIN_WALLCLOCK invalid interval type");
                };
                interval.and_then(|interval| {
                    if interval.months == 0 && interval.days == 0 {
                        Some(interval.nanoseconds)
                    } else {
                        None
                    }
                })
            }
            _ => {
                return exec_err!("DATE_BIN_WALLCLOCK requires scalar interval value");
            }
        };

        let (wallclocks, utc_offsets) = match source_arg {
            ColumnarValue::Scalar(scalar) => {
                let ScalarValue::TimestampNanosecond(maybe_utc_ns, _) = scalar else {
                    return exec_err!("DATE_BIN_WALLCLOCK invalid scalar type");
                };
                let maybe_local_ns_and_utc_offset_ns = maybe_utc_ns.map(|utc_ns| {
                    let utc_offset_ns = offset_from_utc_ns(utc_ns, &arrowtz);
                    (utc_ns + utc_offset_ns, utc_offset_ns)
                });
                let (local_ns, utc_offset_ns) = match maybe_local_ns_and_utc_offset_ns {
                    Some((local_ns, utc_offset_ns)) => (
                        ScalarValue::TimestampNanosecond(Some(local_ns), None),
                        utc_offset_ns,
                    ),
                    None => (ScalarValue::TimestampNanosecond(None, None), 0),
                };
                (ColumnarValue::Scalar(local_ns), vec![utc_offset_ns])
            }
            ColumnarValue::Array(array) => {
                let array = as_timestamp_nanosecond_array(&array)?;
                let (mut wallclock, offset) = array
                    .into_iter()
                    .map(|maybe_utc_ns| {
                        maybe_utc_ns.map(|utc_ns| {
                            let utc_offset_ns = offset_from_utc_ns(utc_ns, &arrowtz);
                            (utc_ns + utc_offset_ns, utc_offset_ns)
                        })
                    })
                    .map(|maybe_pair| match maybe_pair {
                        Some((wallclock, utc_offset)) => (Some(wallclock), utc_offset),
                        None => (None, 0),
                    })
                    .unzip::<_, _, TimestampNanosecondBuilder, Vec<_>>();
                (ColumnarValue::Array(Arc::new(wallclock.finish())), offset)
            }
        };
        let wallclocks_field = Arc::new(Field::new("wallclocks", wallclocks.data_type(), false));

        let result = match origin {
            Some((origin, origin_field)) => self.date_bin.invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    interval_arg.clone(),
                    wallclocks,
                    ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(origin, None)),
                ],
                arg_fields: vec![
                    Arc::clone(internal_field),
                    wallclocks_field,
                    Arc::new(
                        origin_field
                            .as_ref()
                            .clone()
                            .with_data_type(DataType::Timestamp(TimeUnit::Nanosecond, None)),
                    ),
                ],
                number_rows,
                return_field,
                config_options,
            })?,
            None => self.date_bin.invoke_with_args(ScalarFunctionArgs {
                args: vec![interval_arg.clone(), wallclocks],
                arg_fields: vec![Arc::clone(internal_field), wallclocks_field],
                number_rows,
                return_field,
                config_options,
            })?,
        };

        let result = match result {
            ColumnarValue::Scalar(scalar) => {
                let ScalarValue::TimestampNanosecond(maybe_local_ns, _) = scalar else {
                    return exec_err!("DATE_BIN_WALLCLOCK invalid return value from DATE_BIN");
                };
                let maybe_utc_ns = maybe_local_ns.map(|local_ns| {
                    let local_offset_ns =
                        offset_from_local_ns(local_ns, &arrowtz, interval_ns, utc_offsets[0]);
                    local_ns + local_offset_ns
                });
                ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(maybe_utc_ns, tz))
            }
            ColumnarValue::Array(array) => {
                let array = as_timestamp_nanosecond_array(&array)?;
                let mut builder =
                    TimestampNanosecondBuilder::with_capacity(array.len()).with_timezone_opt(tz);
                builder.extend(array.into_iter().zip(utc_offsets).map(
                    |(maybe_local_ns, utc_offset)| {
                        maybe_local_ns.map(|local_ns| {
                            let local_offset_ns =
                                offset_from_local_ns(local_ns, &arrowtz, interval_ns, utc_offset);
                            local_ns + local_offset_ns
                        })
                    },
                ));
                ColumnarValue::Array(Arc::new(builder.finish()))
            }
        };
        Ok(result)
    }
}

/// Returns whether a timezone is a known UTC variant.
/// At least enough that date_bin_wallclock would work
/// exactly the same as date_bin would.
fn is_utc(tz: &Option<Arc<str>>) -> bool {
    tz.as_ref()
        .map(|tz| {
            matches!(
                tz.as_ref(),
                "+00"
                    | "+0000"
                    | "+00:00"
                    | "-00"
                    | "-0000"
                    | "-00:00"
                    | "00"
                    | "0000"
                    | "00:00"
                    | "Etc/GMT"
                    | "Etc/GMT0"
                    | "Etc/GMTMinus0"
                    | "Etc/GMTPlus0"
                    | "Etc/Greenwich"
                    | "Etc/UCT"
                    | "Etc/UTC"
                    | "Etc/Universal"
                    | "Etc/Zulu"
                    | "GMT"
                    | "GMT0"
                    | "GMTMinus0"
                    | "GMTPlus0"
                    | "Greenwich"
                    | "UCT"
                    | "UTC"
                    | "Universal"
                    | "Zulu"
            )
        })
        .unwrap_or(true)
}

/// Calculate the offset in nanoseconds that needs to be added to a UTC
/// timestamp in a timezone to get a local time value.
pub fn offset_from_utc_ns(utc_ns: i64, tz: &impl TimeZone) -> i64 {
    timestamp_ns_to_datetime(utc_ns)
        .map(|dt| (tz.offset_from_utc_datetime(&dt).fix().local_minus_utc() as i64) * 1_000_000_000)
        .unwrap_or_default()
}

/// Calculate the offset in nanoseconds that needs to be added to
/// a local timestamp to get a UTC timestamp in a timezone.
///
/// If the time represented by `local_ns` is ambiguous in the timezone
/// `tz` then the offset will be the earliest if the interval is larger
/// than the time between the two potential offsets, otherwise the
/// negated value of `utc_offset_ns` is used. That is expected to be
/// the offset that was calculated using `offset_from_utc_ns` for the
/// original UTC timestamp.
///
/// Note that if `interval_ns` is `None` then it is assumed that the
/// interval has some day or month component and must be longer than the
/// difference between the two potential offsets.
fn offset_from_local_ns(
    local_ns: i64,
    tz: &impl TimeZone,
    interval_ns: Option<i64>,
    utc_offset_ns: i64,
) -> i64 {
    timestamp_ns_to_datetime(local_ns)
        .map(|dt| match tz.offset_from_local_datetime(&dt) {
            MappedLocalTime::Single(offset) => {
                (offset.fix().utc_minus_local() as i64) * 1_000_000_000
            }
            MappedLocalTime::Ambiguous(earliest, latest) => {
                // This occurs when time jumps backwards when moving out
                // of DST. In this case how the adjustment is calculated
                // depends on the interval. If the interval is shorter than
                // the time between the two potential offsets then the
                // originally calculated UTC offset is used, otherwise the
                // earlier offset is chosen.
                let earliest_ns = earliest.fix().utc_minus_local() as i64 * 1_000_000_000;
                let latest_ns = latest.fix().utc_minus_local() as i64 * 1_000_000_000;
                if match interval_ns {
                    Some(interval_ns) => interval_ns <= latest_ns - earliest_ns,
                    None => false,
                } {
                    -utc_offset_ns
                } else {
                    earliest_ns
                }
            }
            MappedLocalTime::None => {
                // This occurs when time jumps forward when moving into
                // DST. The adjustment to use in this case is the one from
                // before the discontinuity. Calculate the adjustment from
                // the local time 24 hours in the past.
                offset_from_local_ns(
                    local_ns - 24 * 60 * 60 * 1_000_000_000,
                    tz,
                    interval_ns,
                    utc_offset_ns,
                )
            }
        })
        .unwrap_or_default()
}

/// Functions for creating expressions that call the `date_bin_wallclock` UDF.
pub mod expr_fn {
    use datafusion::logical_expr::Expr;
    use datafusion::logical_expr::expr::ScalarFunction;

    /// Create a scalar function expression that calls the `date_bin_wallclock` function.
    pub fn date_bin_wallclock(stride: Expr, source: Expr, origin: Expr) -> Expr {
        Expr::ScalarFunction(ScalarFunction {
            func: super::date_bin_wallclock(),
            args: vec![stride, source, origin],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::default_config_options;
    use arrow::{
        array::{IntervalMonthDayNanoArray, TimestampNanosecondArray},
        datatypes::{FieldRef, IntervalMonthDayNano},
    };

    const MAYDAY: i64 = 1619827200000000000; // 2021-05-01T00:00:00Z

    #[test]
    fn scalar_value_passthrough() {
        let udf = DateBinWallclockUDF::default();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(0, 1, 0),
            ))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(MAYDAY + 60 * 60 * 1_000_000_000),
                None,
            )),
        ];
        let arg_fields = arg_to_fields(&args);
        let result = match udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 13,
                return_field: return_field(DataType::Timestamp(TimeUnit::Nanosecond, None)),
                config_options: default_config_options(),
            })
            .unwrap()
        {
            ColumnarValue::Scalar(scalar) => scalar,
            _ => panic!("Expected scalar value"),
        };
        assert_eq!(result, ScalarValue::TimestampNanosecond(Some(MAYDAY), None),);
    }

    #[test]
    fn scalar_value_with_timezone() {
        let udf = DateBinWallclockUDF::default();
        let tz = Arc::from("America/New_York");
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(0, 1, 0),
            ))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(MAYDAY + 60 * 60 * 1_000_000_000),
                Some(Arc::clone(&tz)),
            )),
        ];
        let arg_fields = arg_to_fields(&args);
        let result = match udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 13,
                return_field: return_field(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::clone(&tz)),
                )),
                config_options: default_config_options(),
            })
            .unwrap()
        {
            ColumnarValue::Scalar(scalar) => scalar,
            _ => panic!("Expected scalar value"),
        };
        assert_eq!(
            result,
            ScalarValue::TimestampNanosecond(
                Some(MAYDAY - 20 * 60 * 60 * 1_000_000_000),
                Some(Arc::clone(&tz))
            ),
        );
    }

    #[test]
    fn at_boundary_for_timezone() {
        let udf = DateBinWallclockUDF::default();
        let tz = Arc::from("Europe/Paris");
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(0, 1, 0),
            ))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(MAYDAY - 2 * 60 * 60 * 1_000_000_000),
                Some(Arc::clone(&tz)),
            )),
        ];
        let arg_fields = arg_to_fields(&args);
        let result = match udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 13,
                return_field: return_field(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::clone(&tz)),
                )),
                config_options: default_config_options(),
            })
            .unwrap()
        {
            ColumnarValue::Scalar(scalar) => scalar,
            _ => panic!("Expected scalar value"),
        };
        assert_eq!(
            result,
            ScalarValue::TimestampNanosecond(
                Some(MAYDAY - 2 * 60 * 60 * 1_000_000_000),
                Some(Arc::clone(&tz))
            ),
        );
    }

    #[test]
    fn with_origin() {
        let udf = DateBinWallclockUDF::default();
        let tz = Arc::from("Europe/London");
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(0, 1, 0),
            ))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(MAYDAY),
                Some(Arc::clone(&tz)),
            )),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(MAYDAY - 24 * 60 * 60 * 1_000_000_000),
                None,
            )),
        ];
        let arg_fields = arg_to_fields(&args);
        let result = match udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 13,
                return_field: return_field(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::clone(&tz)),
                )),
                config_options: default_config_options(),
            })
            .unwrap()
        {
            ColumnarValue::Scalar(scalar) => scalar,
            _ => panic!("Expected scalar value"),
        };
        assert_eq!(
            result,
            ScalarValue::TimestampNanosecond(
                Some(MAYDAY - 60 * 60 * 1_000_000_000),
                Some(Arc::clone(&tz))
            ),
        );
    }

    #[test]
    fn with_origin_in_future() {
        let udf = DateBinWallclockUDF::default();
        let tz = Arc::from("Europe/London");
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(0, 1, 0),
            ))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(MAYDAY + 60 * 60 * 1_000_000_000),
                Some(Arc::clone(&tz)),
            )),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(MAYDAY + 24 * 60 * 60 * 1_000_000_000),
                None,
            )),
        ];
        let arg_fields = arg_to_fields(&args);
        let result = match udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 13,
                return_field: return_field(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::clone(&tz)),
                )),
                config_options: default_config_options(),
            })
            .unwrap()
        {
            ColumnarValue::Scalar(scalar) => scalar,
            _ => panic!("Expected scalar value"),
        };
        assert_eq!(
            result,
            ScalarValue::TimestampNanosecond(
                Some(MAYDAY - 60 * 60 * 1_000_000_000),
                Some(Arc::clone(&tz))
            ),
        );
    }

    #[test]
    fn array_value() {
        let udf = DateBinWallclockUDF::default();
        let arr = Arc::new(
            TimestampNanosecondArray::from(vec![
                Some(MAYDAY + 60 * 60 * 1_000_000_000),
                None,
                Some(MAYDAY + 24 * 60 * 60 * 1_000_000_000),
            ])
            .with_timezone("Europe/Paris"),
        );
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(0, 1, 0),
            ))),
            ColumnarValue::Array(arr),
        ];
        let arg_fields = arg_to_fields(&args);
        let result = match udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 3,
                return_field: return_field(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::from("Europe/Paris")),
                )),
                config_options: default_config_options(),
            })
            .unwrap()
        {
            ColumnarValue::Array(arr) => as_timestamp_nanosecond_array(&arr).unwrap().clone(),
            _ => panic!("Expected array value"),
        };
        assert_eq!(
            result,
            TimestampNanosecondArray::from(vec![
                Some(MAYDAY - 2 * 60 * 60 * 1_000_000_000),
                None,
                Some(MAYDAY + 22 * 60 * 60 * 1_000_000_000),
            ])
            .with_timezone("Europe/Paris")
        );
    }

    #[test]
    fn array_interval_error() {
        let udf = DateBinWallclockUDF::default();
        let tz = Arc::from("UTC");
        let arr = Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano {
                months: 0,
                days: 1,
                nanoseconds: 0,
            }),
            None,
            Some(IntervalMonthDayNano {
                months: 0,
                days: 1,
                nanoseconds: 0,
            }),
        ]));
        let args = vec![
            ColumnarValue::Array(arr),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(MAYDAY + 60 * 60 * 1_000_000_000),
                Some(Arc::clone(&tz)),
            )),
        ];
        let arg_fields = arg_to_fields(&args);
        let result = udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 3,
            return_field: return_field(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some(Arc::clone(&tz)),
            )),
            config_options: default_config_options(),
        });
        assert_eq!(
            result.unwrap_err().to_string(),
            "This feature is not implemented: DATE_BIN only supports literal values for the stride argument, not arrays"
        );
    }

    #[test]
    fn array_origin_error() {
        let udf = DateBinWallclockUDF::default();
        let tz = Arc::from("UTC");
        let arr = Arc::new(TimestampNanosecondArray::from(vec![
            Some(MAYDAY + 60 * 60 * 1_000_000_000),
            None,
            Some(MAYDAY + 24 * 60 * 60 * 1_000_000_000),
        ]));
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(0, 1, 0),
            ))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(MAYDAY + 60 * 60 * 1_000_000_000),
                Some(Arc::clone(&tz)),
            )),
            ColumnarValue::Array(arr),
        ];
        let arg_fields = arg_to_fields(&args);
        let result = udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 3,
            return_field: return_field(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some(Arc::clone(&tz)),
            )),
            config_options: default_config_options(),
        });
        assert_eq!(
            result.unwrap_err().to_string(),
            "This feature is not implemented: DATE_BIN_WALLCLOCK only supports literal values for the origin argument, not arrays"
        );
    }

    #[test]
    fn utc_origin_conversion() {
        let udf = DateBinWallclockUDF::new(Arc::new(ScalarUDF::new_from_impl(
            TimeZoneCheckingUDF::default(),
        )));
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(0, 1, 0),
            ))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(MAYDAY + 60 * 60 * 1_000_000_000),
                Some(Arc::from("Etc/Universal")),
            )),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(6 * 60 * 60 * 1_000_000_000),
                None,
            )),
        ];
        let arg_fields = arg_to_fields(&args);
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 13,
            return_field: return_field(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some(Arc::from("Etc/Universal")),
            )),
            config_options: default_config_options(),
        })
        .unwrap();
    }

    /// A UDF that is a stub `date_bin` which checks that the `source`
    /// and `origin` arguments have the same data type, expecially that
    /// they have identical timezones.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct TimeZoneCheckingUDF {
        signature: Signature,
    }

    impl Default for TimeZoneCheckingUDF {
        fn default() -> Self {
            Self {
                signature: Signature::exact(
                    vec![
                        DataType::Interval(IntervalUnit::MonthDayNano),
                        DataType::Timestamp(
                            TimeUnit::Nanosecond,
                            Some(Arc::from(TIMEZONE_WILDCARD)),
                        ),
                        DataType::Timestamp(
                            TimeUnit::Nanosecond,
                            Some(Arc::from(TIMEZONE_WILDCARD)),
                        ),
                    ],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for TimeZoneCheckingUDF {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &'static str {
            "test_date_bin_udf"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
            Ok(arg_types[1].clone())
        }

        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            let [_, source, origin] = args.args.as_slice() else {
                panic!("Expected three arguments");
            };
            assert_eq!(source.data_type(), origin.data_type());
            let DataType::Timestamp(_, tz) = source.data_type() else {
                panic!("Expected timestamp data type");
            };
            assert!(tz.is_some());
            Ok(source.clone())
        }
    }

    fn arg_to_fields(args: &[ColumnarValue]) -> Vec<FieldRef> {
        args.iter()
            .enumerate()
            .map(|(idx, val)| Arc::new(Field::new(format!("arg_{idx}"), val.data_type(), false)))
            .collect()
    }

    fn return_field(dt: DataType) -> FieldRef {
        Arc::new(Field::new("r", dt, false))
    }
}
