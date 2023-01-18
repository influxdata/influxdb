use std::sync::Arc;

use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::{
    error::DataFusionError,
    logical_expr::{ScalarFunctionImplementation, ScalarUDF, Volatility},
    prelude::create_udf,
};
use once_cell::sync::Lazy;

/// The name of the date_bin_gapfill UDF given to DataFusion.
pub const DATE_BIN_GAPFILL_UDF_NAME: &str = "date_bin_gapfill";

/// Implementation of date_bin_gapfill.
/// This function takes arguments identical to date_bin() but
/// will fill in gaps with nulls (or the last observed value
/// if used with locf).
/// This function will never have an actual implementation because it
/// is a placeholder for a custom plan node that does gap filling.
pub(crate) static DATE_BIN_GAPFILL: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    Arc::new(create_udf(
        DATE_BIN_GAPFILL_UDF_NAME,
        vec![
            DataType::Interval(IntervalUnit::DayTime),       // stride
            DataType::Timestamp(TimeUnit::Nanosecond, None), // source
            DataType::Timestamp(TimeUnit::Nanosecond, None), // origin
        ],
        Arc::new(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        Volatility::Volatile,
        unimplemented_scalar_impl(DATE_BIN_GAPFILL_UDF_NAME),
    ))
});

fn unimplemented_scalar_impl(name: &'static str) -> ScalarFunctionImplementation {
    Arc::new(move |_| {
        Err(DataFusionError::NotImplemented(format!(
            "{} is not yet implemented",
            name
        )))
    })
}

#[cfg(test)]
mod test {
    use arrow::array::{ArrayRef, TimestampNanosecondArray};
    use arrow::record_batch::RecordBatch;
    use datafusion::error::Result;
    use datafusion::prelude::{col, lit_timestamp_nano, Expr};
    use datafusion::scalar::ScalarValue;
    use datafusion_util::context_with_table;
    use std::sync::Arc;

    fn date_bin_gapfill(stride: Expr, source: Expr, origin: Expr) -> Expr {
        crate::registry()
            .udf(super::DATE_BIN_GAPFILL_UDF_NAME)
            .expect("should be registered")
            .call(vec![stride, source, origin])
    }

    fn lit_interval_milliseconds(v: i64) -> Expr {
        Expr::Literal(ScalarValue::IntervalDayTime(Some(v)))
    }

    #[tokio::test]
    async fn date_bin_gapfill_errs() -> Result<()> {
        let times = Arc::new(TimestampNanosecondArray::from(vec![Some(1000)]));
        let rb = RecordBatch::try_from_iter(vec![("time", times as ArrayRef)])?;
        let ctx = context_with_table(rb);
        let df = ctx.table("t").await?.select(vec![date_bin_gapfill(
            lit_interval_milliseconds(360_000),
            col("time"),
            lit_timestamp_nano(0),
        )])?;
        let res = df.collect().await;
        let expected = "date_bin_gapfill is not yet implemented";
        assert!(res
            .expect_err("should be an error")
            .to_string()
            .contains(expected));
        Ok(())
    }
}
