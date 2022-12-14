mod internal;

pub use internal::{Duration, Window};
use schema::TIME_DATA_TYPE;

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, TimestampNanosecondArray},
    datatypes::DataType,
};
use datafusion::{
    logical_expr::{ScalarUDF, Volatility},
    physical_plan::ColumnarValue,
    prelude::*,
    scalar::ScalarValue,
};
use once_cell::sync::Lazy;

use crate::group_by::WindowDuration;

// Reuse DataFusion error and Result types for this module
pub use datafusion::error::{DataFusionError, Result as DataFusionResult};

/// The name of the window_bounds UDF given to DataFusion.
pub(crate) const WINDOW_BOUNDS_UDF_NAME: &str = "influx_window_bounds";

/// Implementation of window_bounds
pub(crate) static WINDOW_BOUNDS_UDF: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    Arc::new(create_udf(
        WINDOW_BOUNDS_UDF_NAME,
        // takes 7 arguments (see [`window_bounds_udf`] for details)
        vec![
            TIME_DATA_TYPE(),
            // encoded every
            DataType::Utf8,
            DataType::Int64,
            DataType::Boolean,
            // encoded offset
            DataType::Utf8,
            DataType::Int64,
            DataType::Boolean,
        ],
        Arc::new(TIME_DATA_TYPE()),
        Volatility::Stable,
        Arc::new(window_bounds_udf),
    ))
});

/// Implement the window bounds function as a DataFusion UDF where
/// each of the two `WindowDuration` argument has been encoded as
/// three distinct arguments, for 7 total arguments
///
/// ```text
/// window_bounds(arg, every, offset)
/// ```
///
/// Becomes
///
/// ```text
/// window_bounds_udf(arg, every_type, every.field1, every.field2, offset_type, offset.field1, duration.field2)
/// ```
///
/// For example this would mean that `window_bounds` like this:
///
/// ```text
/// window_bounds(
///   col(time),
///   WindowDuration::Fixed(10),
///   WindowDuration::Variable(11, false)
/// )
/// ```
/// Would be called like:
///
/// ```text
/// window_bounds_udf(
///   col(time),
///   "fixed", 10, NULL,
///   "variable", 11, false
/// )
/// ```
///
/// Note: [`EncodedWindowDuration`] Handles the encoding / decoding of these arguments
fn window_bounds_udf(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    assert_eq!(args.len(), 7);

    // extract the arguments as a Scalar
    macro_rules! extract_scalar {
        ($ARG:expr) => {
            match &args[$ARG] {
                ColumnarValue::Array(_) => {
                    return Err(DataFusionError::Internal(format!(
                        "window_bounds_udf argument {} not a scalar",
                        $ARG
                    )))
                }
                ColumnarValue::Scalar(v) => v.clone(),
            }
        };
    }

    let every: WindowDuration = EncodedWindowDuration {
        ty: extract_scalar!(1),
        field1: extract_scalar!(2),
        field2: extract_scalar!(3),
    }
    .try_into()?;

    let offset: WindowDuration = EncodedWindowDuration {
        ty: extract_scalar!(4),
        field1: extract_scalar!(5),
        field2: extract_scalar!(6),
    }
    .try_into()?;

    let arg = match &args[0] {
        ColumnarValue::Scalar(v) => {
            return Err(DataFusionError::NotImplemented(format!(
                "window_bounds against scalar arguments ({:?}) not yet implemented",
                v
            )))
        }
        ColumnarValue::Array(arr) => arr,
    };

    Ok(ColumnarValue::Array(window_bounds(arg, every, offset)))
}

/// This is the implementation of the `window_bounds` user defined
/// function used in IOx to compute window boundaries when doing
/// grouping by windows.
fn window_bounds(arg: &dyn Array, every: WindowDuration, offset: WindowDuration) -> ArrayRef {
    // `arg` and output are dynamically-typed Arrow arrays, which means that we
    // need to:
    //
    // 1. cast the values to the type we want
    // 2. perform the window_bounds calculation for every element in the
    //     timestamp array
    // 3. construct the resulting array

    let time = arg
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("cast of time failed");

    // Note: the Go code uses the `Stop` field of the `GetEarliestBounds` call as
    // the window boundary https://github.com/influxdata/influxdb/blob/master/storage/reads/array_cursor.gen.go#L546

    // Note window doesn't use the period argument
    let period = internal::Duration::from_nsecs(0);
    let window = internal::Window::new((&every).into(), period, (&offset).into());

    // calculate the output times, one at a time, one element at a time

    let values = time.iter().map(|ts| {
        ts.map(|ts| {
            let bounds = window.get_earliest_bounds(ts);
            bounds.stop
        })
    });

    let array = values.collect::<TimestampNanosecondArray>();
    Arc::new(array) as ArrayRef
}

/// Represents a [`WindowDuration`] encoded as a set of
/// [`ScalarValue`]s that can be passed as arguments to a DataFusion
/// function
pub(crate) struct EncodedWindowDuration {
    /// type: uf8: "fixed" or "variable"
    pub(crate) ty: ScalarValue,

    /// type: i64
    /// if "fixed", holds nanoseconds
    /// if "variable", months
    pub(crate) field1: ScalarValue,

    /// type: bool
    /// if "fixed", null
    /// if "variable", holds `negative`
    pub(crate) field2: ScalarValue,
}

impl From<WindowDuration> for EncodedWindowDuration {
    // Turn a [`WindowDuration`] into a [`EncodedWindowDuration`]
    // suitable for passing as a parameter to a datafusion parameter
    fn from(window_duration: WindowDuration) -> Self {
        match window_duration {
            WindowDuration::Variable { months, negative } => Self {
                ty: ScalarValue::Utf8(Some("variable".into())),
                field1: ScalarValue::Int64(Some(months)),
                field2: ScalarValue::Boolean(Some(negative)),
            },
            WindowDuration::Fixed { nanoseconds } => Self {
                ty: ScalarValue::Utf8(Some("fixed".into())),
                field1: ScalarValue::Int64(Some(nanoseconds)),
                field2: ScalarValue::Boolean(None),
            },
        }
    }
}

impl TryFrom<EncodedWindowDuration> for WindowDuration {
    type Error = DataFusionError;

    // attempt to convert the encoded duration back to a WindowDuration
    fn try_from(value: EncodedWindowDuration) -> Result<Self, Self::Error> {
        let EncodedWindowDuration { ty, field1, field2 } = value;

        match ty {
            ScalarValue::Utf8(Some(val)) if val == "variable" => {
                let months = if let ScalarValue::Int64(Some(v)) = field1 {
                    v
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "Invalid variable WindowDuration encoding. Expected int64 for months \
                             but got '{:?}'",
                        field1
                    )));
                };

                let negative = if let ScalarValue::Boolean(Some(v)) = field2 {
                    v
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "Invalid variable WindowDuration encoding. Expected bool for negative \
                             but got '{:?}'",
                        field2
                    )));
                };

                Ok(Self::Variable { months, negative })
            }
            ScalarValue::Utf8(Some(val)) if val == "fixed" => {
                let nanoseconds = if let ScalarValue::Int64(Some(v)) = field1 {
                    v
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "Invalid fixed WindowDuration encoding. Expected int64 for nanoseconds \
                             but got '{:?}'",
                        field1
                    )));
                };

                if let ScalarValue::Boolean(None) = field2 {
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "Invalid fixed WindowDuration encoding. Expected Null bool in field2 \
                             but got '{:?}'",
                        field2
                    )));
                };

                Ok(Self::Fixed { nanoseconds })
            }
            _ => Err(DataFusionError::Internal(format!(
                "Invalid WindowDuration encoding. Expected string 'variable' or 'fixed' but \
                    got '{:?}'",
                ty
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::TimestampNanosecondArray;

    use super::*;

    #[test]
    fn test_window_bounds() {
        let input: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            Some(100),
            None,
            Some(200),
            Some(300),
            Some(400),
        ]));

        let every = WindowDuration::from_nanoseconds(200);
        let offset = WindowDuration::from_nanoseconds(50);

        let bounds_array = window_bounds(&input, every, offset);

        let expected_array: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            Some(250),
            None,
            Some(250),
            Some(450),
            Some(450),
        ]));

        assert_eq!(
            &expected_array, &bounds_array,
            "Expected:\n{:?}\nActual:\n{:?}",
            expected_array, bounds_array,
        );
    }

    #[test]
    fn test_encoded_duration_roundtrop() {
        /// That `window_duration` survives encoding and decoding
        fn round_trip(window_duration: WindowDuration) {
            let encoded_window_duration: EncodedWindowDuration = window_duration.into();
            let decoded_window_duration: WindowDuration =
                encoded_window_duration.try_into().expect("decode failed");
            assert_eq!(window_duration, decoded_window_duration);
        }

        round_trip(WindowDuration::Fixed { nanoseconds: 42 });
        round_trip(WindowDuration::Variable {
            months: 11,
            negative: true,
        });
        round_trip(WindowDuration::Variable {
            months: 3,
            negative: false,
        });
    }

    #[test]
    #[should_panic(
        expected = "Invalid WindowDuration encoding. Expected string 'variable' or 'fixed' but got \
        'Boolean(true)'"
    )]
    fn test_decoding_error_wrong_type_type() {
        decode(EncodedWindowDuration {
            // incorrect type for ty
            ty: ScalarValue::Boolean(Some(true)),
            ..good_variable_duration()
        })
    }

    #[test]
    #[should_panic(
        expected = "Invalid WindowDuration encoding. Expected string 'variable' or 'fixed' but got \
        'Utf8(\\\"FIXED\\\")"
    )]
    fn test_decoding_error_wrong_type_value() {
        decode(EncodedWindowDuration {
            // incorrect value
            ty: ScalarValue::Utf8(Some("FIXED".to_string())),
            ..good_variable_duration()
        })
    }

    #[test]
    #[should_panic(
        expected = "Invalid variable WindowDuration encoding. Expected int64 for months but got \
        'UInt64(11)'"
    )]
    fn test_decoding_error_wrong_variable_months() {
        let _: WindowDuration = EncodedWindowDuration {
            field1: ScalarValue::UInt64(Some(11)),
            ..good_variable_duration()
        }
        .try_into()
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Invalid variable WindowDuration encoding. Expected bool for negative but got \
        'UInt64(11)'"
    )]
    fn test_decoding_error_wrong_variable_negative() {
        decode(EncodedWindowDuration {
            field2: ScalarValue::UInt64(Some(11)),
            ..good_variable_duration()
        })
    }

    #[test]
    #[should_panic(
        expected = "Invalid fixed WindowDuration encoding. Expected int64 for nanoseconds but got \
        'UInt64(11)'"
    )]
    fn test_decoding_error_wrong_fixed_nanos() {
        decode(EncodedWindowDuration {
            field1: ScalarValue::UInt64(Some(11)),
            ..good_fixed_duration()
        })
    }

    #[test]
    #[should_panic(
        expected = "Invalid fixed WindowDuration encoding. Expected Null bool in field2 but got \
        'Boolean(false)'"
    )]
    fn test_decoding_error_wrong_fixed_field2() {
        decode(EncodedWindowDuration {
            field2: ScalarValue::Boolean(Some(false)),
            ..good_fixed_duration()
        })
    }

    /// decodes the encoded value as a `WindowDuration`, panic'ing on failure
    fn decode(encoded: EncodedWindowDuration) {
        let _: WindowDuration = encoded.try_into().unwrap();
    }

    fn good_variable_duration() -> EncodedWindowDuration {
        EncodedWindowDuration {
            // incorrect type for ty
            ty: ScalarValue::Utf8(Some("variable".to_string())),
            field1: ScalarValue::Int64(Some(11)),
            field2: ScalarValue::Boolean(Some(false)),
        }
    }

    fn good_fixed_duration() -> EncodedWindowDuration {
        EncodedWindowDuration {
            // incorrect type for ty
            ty: ScalarValue::Utf8(Some("fixed".to_string())),
            field1: ScalarValue::Int64(Some(11)),
            field2: ScalarValue::Boolean(None),
        }
    }
}
