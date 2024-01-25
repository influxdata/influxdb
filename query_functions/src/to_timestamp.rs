//! Implementation of `to_timestamp` function that
//! overrides the built in version in DataFusion because the semantics changed
//! upstream: <https://github.com/apache/arrow-datafusion/pull/7844>
//!
//!
//! See <https://github.com/influxdata/influxdb_iox/issues/9164> for more details
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::TimeUnit;
use datafusion::common::internal_err;
use datafusion::error::Result;
use datafusion::logical_expr::ScalarUDFImpl;
use datafusion::logical_expr::Signature;
use datafusion::physical_expr::datetime_expressions;
use datafusion::physical_expr::expressions::cast_column;
use datafusion::{
    error::DataFusionError,
    logical_expr::{ScalarUDF, Volatility},
    physical_plan::ColumnarValue,
};
use once_cell::sync::Lazy;

/// The name of the function
pub const TO_TIMESTAMP_FUNCTION_NAME: &str = "to_timestamp";

#[derive(Debug)]
struct ToTimestampUDF {
    signature: Signature,
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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return internal_err!("to_timestamp expected 1 argument, got {}", args.len());
        }

        match args[0].data_type() {
            // call through to arrow cast kernel
            DataType::Int64 | DataType::Timestamp(_, _) => cast_column(
                &args[0],
                &DataType::Timestamp(TimeUnit::Nanosecond, None),
                None,
            ),
            DataType::Utf8 => datetime_expressions::to_timestamp_nanos(args),
            dt => internal_err!("to_timestamp does not support argument type '{dt}'"),
        }
    }
}

/// Implementation of to_timestamp
pub(crate) static TO_TIMESTAMP_UDF: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    Arc::new(ScalarUDF::from(ToTimestampUDF {
        signature: Signature::uniform(
            1,
            vec![
                DataType::Int64,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Second, None),
                DataType::Utf8,
            ],
            Volatility::Immutable,
        ),
    }))
});

// https://github.com/apache/arrow-datafusion/pull/7844
