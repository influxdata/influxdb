use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::{ScalarUDF, ScalarUDFImpl, Signature, Volatility},
    physical_plan::ColumnarValue,
};
use once_cell::sync::Lazy;

/// The name of the "sleep" UDF given to DataFusion.
pub const SLEEP_UDF_NAME: &str = "sleep";

#[derive(Debug)]
struct SleepUDF {
    signature: Signature,
}

impl ScalarUDFImpl for SleepUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        SLEEP_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Null)
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        Err(DataFusionError::Internal(
            "sleep function should have been replaced by optimizer pass to avoid thread blocking"
                .to_owned(),
        ))
    }
}

/// Implementation of "sleep"
pub(crate) static SLEEP_UDF: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    Arc::new(ScalarUDF::from(SleepUDF {
        signature: Signature::uniform(
            1,
            vec![
                DataType::Null,
                DataType::Duration(TimeUnit::Second),
                DataType::Duration(TimeUnit::Millisecond),
                DataType::Duration(TimeUnit::Millisecond),
                DataType::Duration(TimeUnit::Microsecond),
                DataType::Duration(TimeUnit::Nanosecond),
                DataType::Float32,
                DataType::Float64,
            ],
            Volatility::Volatile,
        ),
    }))
});

#[cfg(test)]
mod tests {
    use datafusion::{
        common::assert_contains,
        logical_expr::LogicalPlanBuilder,
        physical_plan::common::collect,
        prelude::{lit, SessionContext},
        scalar::ScalarValue,
    };

    use super::*;

    #[tokio::test]
    async fn test() {
        let ctx = SessionContext::new();
        let plan = LogicalPlanBuilder::empty(true)
            .project([SLEEP_UDF.call(vec![lit(ScalarValue::Null)]).alias("sleep")])
            .unwrap()
            .build()
            .unwrap();
        let plan = ctx.state().create_physical_plan(&plan).await.unwrap();
        let err = collect(plan.execute(0, ctx.task_ctx()).unwrap())
            .await
            .unwrap_err();

        assert_contains!(
            err.to_string(),
            "sleep function should have been replaced by optimizer pass"
        );
    }
}
