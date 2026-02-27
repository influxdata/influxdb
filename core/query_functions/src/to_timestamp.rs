use std::sync::{Arc, LazyLock};

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::{
    common::plan_datafusion_err,
    error::Result,
    logical_expr::{ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility},
    physical_plan::ColumnarValue,
};

/// The name of the function
pub const TO_TIMESTAMP_FUNCTION_NAME: &str = "to_timestamp";

/// Implementation of `to_timestamp` function that
/// overrides the built in version in DataFusion because the semantics changed
/// upstream: <https://github.com/apache/arrow-datafusion/pull/7844>
///
/// See <https://github.com/influxdata/influxdb_iox/issues/9164> for more details
#[derive(Debug, Hash, PartialEq, Eq)]
struct ToTimestampUDF {
    signature: Signature,
    /// Fall back to DataFusion's built in to_timestamp implementation
    fallback: Arc<ScalarUDF>,
}

impl ToTimestampUDF {
    fn new() -> Self {
        Self {
            // This function can take 1 or more arguments. The first can be Int64, Timestamp, or Utf8.
            // The remainder, if they exist, must be Utf8. The documentation for what should be
            // passed in for the remainder arguments can best be found at apache's datafusion
            // documentation for this same function (`to_timestamp`).
            signature: Signature::user_defined(Volatility::Immutable),
            // Fallback to default implementation
            fallback: datafusion::functions::datetime::to_timestamp(),
        }
    }
}

impl ScalarUDFImpl for ToTimestampUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        TO_TIMESTAMP_FUNCTION_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            // You may only have a single argument if you're not parsing utf8
            [DataType::Timestamp(_, _) | DataType::Int64] => Ok(arg_types.to_vec()),
            // But multple arguments are allowed when the first is utf8
            [DataType::Utf8, rest @ ..] if rest.iter().all(|d| *d == DataType::Utf8) => {
                Ok(arg_types.to_vec())
            }
            _ => Err(plan_datafusion_err!(
                "{TO_TIMESTAMP_FUNCTION_NAME} supports either 1 argument (being Int64 or Timestamp), or multiple arguments, all of which being Utf8. Could not coerce the provided {arg_types:?} to these types."
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.as_slice() {
            // call through to arrow cast kernel
            [arg0]
                if matches!(
                    arg0.data_type(),
                    DataType::Int64 | DataType::Timestamp(_, _)
                ) =>
            {
                arg0.cast_to(&DataType::Timestamp(TimeUnit::Nanosecond, None), None)
            }
            _ => self.fallback.invoke_with_args(args),
        }
    }
}

/// Implementation of to_timestamp
pub(crate) static TO_TIMESTAMP_UDF: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(ToTimestampUDF::new())));
