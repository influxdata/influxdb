use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use arrow::array::timezone::Tz;
use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};

use datafusion::common::cast::as_timestamp_nanosecond_array;
use datafusion::common::{internal_err, plan_err};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TIMEZONE_WILDCARD, TypeSignature, Volatility,
};
use datafusion::scalar::ScalarValue;

pub(crate) const TZ_UDF_NAME: &str = "tz";

/// A static instance of the `tz` scalar function.
pub static TZ_UDF: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(TzUDF::default())));

/// A scalar UDF that converts a timestamp to a different timezone.
/// This function understands that input timestamps are in UTC despite
/// the lack of time zone information.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct TzUDF {
    signature: Signature,
}

impl TzUDF {
    fn is_valid_timezone(tz: &str) -> Result<()> {
        let iserr = Tz::from_str(tz);
        if iserr.is_err() {
            plan_err!("TZ requires a valid timezone string, got {:?}", tz)
        } else {
            Ok(())
        }
    }
}

impl Default for TzUDF {
    fn default() -> Self {
        let signatures = vec![
            TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Nanosecond, None)]),
            TypeSignature::Exact(vec![DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some(Arc::from(TIMEZONE_WILDCARD)),
            )]),
            TypeSignature::Exact(vec![
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Utf8,
            ]),
            TypeSignature::Exact(vec![
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from(TIMEZONE_WILDCARD))),
                DataType::Utf8,
            ]),
        ];
        Self {
            signature: Signature::one_of(signatures, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TzUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        TZ_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("tz should call return_field_from_args")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        match args.arg_fields {
            [time] => Ok(Arc::new(
                Field::new(
                    self.name(),
                    DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
                    false,
                )
                .with_metadata(time.metadata().clone()),
            )),
            [time, _tz] => {
                let Some(ScalarValue::Utf8(Some(tz))) = &args.scalar_arguments[1] else {
                    return plan_err!(
                        "tz requires its second argument to be a timezone string, got {:?}",
                        &args.scalar_arguments[1]
                    );
                };
                Self::is_valid_timezone(tz)?;

                Ok(Arc::new(
                    Field::new(
                        self.name(),
                        DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from(tz.as_str()))),
                        false,
                    )
                    .with_metadata(time.metadata().clone()),
                ))
            }
            _ => {
                plan_err!(
                    "tz expects 1 or 2 arguments, got {:?}",
                    args.arg_fields.len()
                )
            }
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let new_tz = match args.args.len() {
            1 => "UTC",
            2 => match &args.args[1] {
                ColumnarValue::Scalar(ScalarValue::Utf8(tz)) => match tz {
                    Some(tz) => {
                        Self::is_valid_timezone(tz)?;
                        tz.as_str()
                    }
                    None => "UTC",
                },
                _ => {
                    return plan_err!(
                        "TZ expects the second argument to be a string defining the desired timezone"
                    );
                }
            },
            _ => return plan_err!("TZ expects one or two arguments"),
        };

        match &args.args[0] {
            ColumnarValue::Scalar(scalar) => match scalar {
                ScalarValue::TimestampNanosecond(epoch_nanos, _) => Ok(ColumnarValue::Scalar(
                    ScalarValue::TimestampNanosecond(*epoch_nanos, Some(Arc::from(new_tz))),
                )),
                scalar_value => plan_err!(
                    "TZ expects a nanosecond timestamp as the first parameter, got {:?}",
                    scalar_value.data_type()
                ),
            },
            ColumnarValue::Array(array) => {
                let dt = array.data_type();
                match dt {
                    DataType::Timestamp(_, _) => (), // This is what we want
                    _ => {
                        return plan_err!("TZ expects nanosecond timestamp data, got {:?}", dt);
                    }
                };

                Ok(ColumnarValue::Array(Arc::new(
                    as_timestamp_nanosecond_array(array.as_ref())?
                        .clone()
                        .with_timezone(new_tz),
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::default_config_options;
    use arrow::array::{Int64Array, TimestampNanosecondArray, TimestampNanosecondBuilder};

    const TODAY: i64 = 1728668794000000000;

    #[test]
    fn default_args() {
        let udf = TzUDF::default();
        let args = vec![ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
            Some(TODAY),
            None,
        ))];
        let arg_fields = arg_to_fields(&args);
        let result = match udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 13,
                return_field: return_field(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::from("UTC")),
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
            ScalarValue::TimestampNanosecond(Some(TODAY), Some(Arc::from("UTC"))),
        );
    }

    #[test]
    fn utc() {
        let udf = TzUDF::default();
        let args = vec![ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
            Some(TODAY),
            Some(Arc::from("UTC")),
        ))];
        let arg_fields = arg_to_fields(&args);
        let result = match udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 13,
                return_field: return_field(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::from("UTC")),
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
            ScalarValue::TimestampNanosecond(Some(TODAY), Some(Arc::from("UTC"))),
        );

        let args = vec![
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(TODAY), None)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("UTC".into()))),
        ];
        let arg_fields = arg_to_fields(&args);
        let result = match udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 13,
                return_field: return_field(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::from("UTC")),
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
            ScalarValue::TimestampNanosecond(Some(TODAY), Some(Arc::from("UTC"))),
        );
    }

    #[test]
    fn timezone() {
        let udf = TzUDF::default();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(TODAY), None)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("America/New_York".into()))),
        ];
        let arg_fields = arg_to_fields(&args);
        let result = match udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 13,
                return_field: return_field(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::from("America/New_York")),
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
            ScalarValue::TimestampNanosecond(Some(TODAY), Some(Arc::from("America/New_York"))),
        );

        let args = vec![
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(TODAY),
                Some(Arc::from("America/Denver")),
            )),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("America/New_York".into()))),
        ];
        let arg_fields = arg_to_fields(&args);
        let result = match udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 13,
                return_field: return_field(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::from("America/New_York")),
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
            ScalarValue::TimestampNanosecond(Some(TODAY), Some(Arc::from("America/New_York"))),
        );
    }

    #[test]
    fn offset() {
        let udf = TzUDF::default();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(TODAY), None)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("+05:00".into()))),
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
        assert_eq!(
            result,
            ScalarValue::TimestampNanosecond(Some(TODAY), Some(Arc::from("+05:00"))),
        );

        let args = vec![
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(TODAY),
                Some(Arc::from("America/Denver")),
            )),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("+05:00".into()))),
        ];
        let arg_fields = arg_to_fields(&args);
        let result = match udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 13,
                return_field: return_field(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::from("+05:00")),
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
            ScalarValue::TimestampNanosecond(Some(TODAY), Some(Arc::from("+05:00"))),
        );
    }

    #[test]
    fn arrays() {
        let udf = TzUDF::default();
        let arr = Arc::new(TimestampNanosecondArray::from(vec![
            Some(TODAY + 1_000_000_000),
            Some(TODAY + 2 * 1_000_000_000),
        ]));
        let args = vec![
            ColumnarValue::Array(arr),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Europe/Brussels".to_owned()))),
        ];
        let arg_fields = arg_to_fields(&args);
        let result = match udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 13,
                return_field: return_field(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::from("Europe/Brussels")),
                )),
                config_options: default_config_options(),
            })
            .unwrap()
        {
            ColumnarValue::Array(array) => as_timestamp_nanosecond_array(&array).unwrap().clone(),
            _ => panic!("Expected array"),
        };

        let mut expected =
            TimestampNanosecondBuilder::new().with_timezone_opt(Some("Europe/Brussels"));
        expected.append_option(Some(TODAY + 1_000_000_000));
        expected.append_option(Some(TODAY + 2 * 1_000_000_000));

        assert_eq!(result, expected.finish());
    }

    #[test]
    fn wrong_timezone_error() {
        let udf = TzUDF::default();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(TODAY), None)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("NewYork".into()))),
        ];
        let arg_fields = arg_to_fields(&args);
        let result = udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 13,
            return_field: return_field(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some(Arc::from("NewYork")),
            )),
            config_options: default_config_options(),
        });
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("TZ requires a valid timezone string, got \"NewYork\"")
        );
    }

    #[test]
    fn wrong_type_error() {
        let udf = TzUDF::default();
        let args = vec![ColumnarValue::Scalar(ScalarValue::TimestampSecond(
            Some(100),
            None,
        ))];
        let arg_fields = arg_to_fields(&args);
        let result = udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 13,
            return_field: return_field(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            config_options: default_config_options(),
        });
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(
            "TZ expects a nanosecond timestamp as the first parameter, got Timestamp(Second, None)"
        ));

        let udf = TzUDF::default();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(Some(100), None)),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
        ];
        let arg_fields = arg_to_fields(&args);
        let result = udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 13,
            return_field: return_field(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            config_options: default_config_options(),
        });
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(
            "TZ expects the second argument to be a string defining the desired timezone"
        ));

        let udf = TzUDF::default();
        let arr = Arc::new(Int64Array::from(vec![
            Some(TODAY + 1_000_000_000),
            Some(TODAY + 2 * 1_000_000_000),
        ]));
        let args = vec![ColumnarValue::Array(arr)];
        let arg_fields = arg_to_fields(&args);

        let result = udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 13,
            return_field: return_field(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            config_options: default_config_options(),
        });
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("TZ expects nanosecond timestamp data, got Int64")
        );
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
