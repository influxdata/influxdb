//! Scalar functions to support queries that perform
//! gap filling.
//!
//! Gap filling in IOx occurs with queries of the form:
//!
//! ```sql
//! SELECT
//!   location,
//!   DATE_BIN_GAPFILL(INTERVAL '1 minute', time, '1970-01-01T00:00:00Z') AS minute,
//!   LOCF(AVG(temp))
//!   INTERPOLATE(AVG(humidity))
//! FROM temps
//! WHERE time > NOW() - INTERVAL '6 hours' AND time < NOW()
//! GROUP BY LOCATION, MINUTE
//! ```
//!
//! The functions `DATE_BIN_GAPFILL`, `LOCF`, and `INTERPOLATE` are special,
//! in that they don't have normal implementations, but instead
//! are transformed by logical optimizer rule `HandleGapFill` to
//! produce a plan that fills gaps.
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::{
    error::DataFusionError,
    logical_expr::{
        BuiltinScalarFunction, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF,
        Signature, TypeSignature, Volatility,
    },
};
use once_cell::sync::Lazy;
use schema::InfluxFieldType;

/// The name of the date_bin_gapfill UDF given to DataFusion.
pub const DATE_BIN_GAPFILL_UDF_NAME: &str = "date_bin_gapfill";

/// (Non-)Implementation of date_bin_gapfill.
/// This function takes arguments identical to `date_bin()` but
/// works in conjunction with the logical optimizer rule
/// `HandleGapFill` to fill gaps in time series data.
pub(crate) static DATE_BIN_GAPFILL: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    // DATE_BIN_GAPFILL should have the same signature as DATE_BIN,
    // so that just adding _GAPFILL can turn a query into a gap-filling query.
    let mut signatures = BuiltinScalarFunction::DateBin.signature();
    // We don't want this to be optimized away before we can give a helpful error message
    signatures.volatility = Volatility::Volatile;

    let return_type_fn: ReturnTypeFunction =
        Arc::new(|_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Nanosecond, None))));
    Arc::new(ScalarUDF::new(
        DATE_BIN_GAPFILL_UDF_NAME,
        &signatures,
        &return_type_fn,
        &unimplemented_scalar_impl(DATE_BIN_GAPFILL_UDF_NAME),
    ))
});

/// The name of the locf UDF given to DataFusion.
pub const LOCF_UDF_NAME: &str = "locf";

/// (Non-)Implementation of locf.
/// This function takes a single argument of any type and
/// produces a value of the same type. It is
/// used in the context of gap-filling queries to represent
/// "last observation carried forward." It does not have
/// an implementation since it will be consumed by the logical optimizer rule
/// `HandleGapFill`.
pub(crate) static LOCF: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    let return_type_fn: ReturnTypeFunction = Arc::new(|args| Ok(Arc::new(args[0].clone())));
    Arc::new(ScalarUDF::new(
        LOCF_UDF_NAME,
        &Signature::any(1, Volatility::Volatile),
        &return_type_fn,
        &unimplemented_scalar_impl(LOCF_UDF_NAME),
    ))
});

/// The name of the interpolate UDF given to DataFusion.
pub const INTERPOLATE_UDF_NAME: &str = "interpolate";

/// (Non-)Implementation of interpolate.
/// This function takes a single numeric argument and
/// produces a value of the same type. It is
/// used in the context of gap-filling queries to indicate
/// columns that should be inmterpolated. It does not have
/// an implementation since it will be consumed by the logical optimizer rule
/// `HandleGapFill`.
pub(crate) static INTERPOLATE: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    let return_type_fn: ReturnTypeFunction = Arc::new(|args| Ok(Arc::new(args[0].clone())));
    let signatures = vec![
        InfluxFieldType::Float,
        InfluxFieldType::Integer,
        InfluxFieldType::UInteger,
    ]
    .iter()
    .map(|&influx_type| TypeSignature::Exact(vec![influx_type.into()]))
    .collect();
    Arc::new(ScalarUDF::new(
        INTERPOLATE_UDF_NAME,
        &Signature::one_of(signatures, Volatility::Volatile),
        &return_type_fn,
        &unimplemented_scalar_impl(INTERPOLATE_UDF_NAME),
    ))
});

fn unimplemented_scalar_impl(name: &'static str) -> ScalarFunctionImplementation {
    Arc::new(move |_| {
        Err(DataFusionError::NotImplemented(format!(
            "{name} is not yet implemented"
        )))
    })
}

#[cfg(test)]
mod test {
    use arrow::array::{ArrayRef, Float64Array, TimestampNanosecondArray};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::assert_contains;
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
        Expr::Literal(ScalarValue::new_interval_mdn(0, 0, v * 1_000_000))
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
        assert_contains!(res.expect_err("should be an error").to_string(), expected);
        Ok(())
    }

    fn locf(arg: Expr) -> Expr {
        crate::registry()
            .udf(super::LOCF_UDF_NAME)
            .expect("should be registered")
            .call(vec![arg])
    }

    #[tokio::test]
    async fn locf_errs() {
        let arg = Arc::new(Float64Array::from(vec![100.0]));
        let rb = RecordBatch::try_from_iter(vec![("f0", arg as ArrayRef)]).unwrap();
        let ctx = context_with_table(rb);
        let df = ctx
            .table("t")
            .await
            .unwrap()
            .select(vec![locf(col("f0"))])
            .unwrap();
        let res = df.collect().await;
        let expected = "locf is not yet implemented";
        assert_contains!(res.expect_err("should be an error").to_string(), expected);
    }

    fn interpolate(arg: Expr) -> Expr {
        crate::registry()
            .udf(super::INTERPOLATE_UDF_NAME)
            .expect("should be registered")
            .call(vec![arg])
    }

    #[tokio::test]
    async fn interpolate_errs() {
        let arg = Arc::new(Float64Array::from(vec![100.0]));
        let rb = RecordBatch::try_from_iter(vec![("f0", arg as ArrayRef)]).unwrap();
        let ctx = context_with_table(rb);
        let df = ctx
            .table("t")
            .await
            .unwrap()
            .select(vec![interpolate(col("f0"))])
            .unwrap();
        let res = df.collect().await;
        let expected = "interpolate is not yet implemented";
        assert!(res
            .expect_err("should be an error")
            .to_string()
            .contains(expected));
    }
}
