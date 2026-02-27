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
//! The functions `DATE_BIN_GAPFILL`, `DATE_BIN_WALLCLOCK_GAPFILL`,
//! `LOCF`, and `INTERPOLATE` are special, in that they don't have
//! normal implementations, but instead are transformed by logical
//! analyzer rule `HandleGapFill` to produce a plan that fills gaps.
use std::sync::{Arc, LazyLock};

use arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::{
    error::{DataFusionError, Result},
    functions::datetime::date_bin,
    logical_expr::{
        ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
    },
    physical_plan::ColumnarValue,
};
use schema::InfluxFieldType;

use crate::date_bin_wallclock;

/// The name of the date_bin_gapfill UDF given to DataFusion.
pub const DATE_BIN_GAPFILL_UDF_NAME: &str = "date_bin_gapfill";

/// (Non-)Implementation of date_bin_gapfill.
/// This function takes arguments identical to `date_bin()` but
/// works in conjunction with the logical optimizer rule
/// `HandleGapFill` to fill gaps in time series data.
pub(crate) static DATE_BIN_GAPFILL: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(GapFillWrapper::new(date_bin()))));

/// The name of the date_bin_wallclock_gapfill UDF given to DataFusion.
pub const DATE_BIN_WALLCLOCK_GAPFILL_UDF_NAME: &str = "date_bin_wallclock_gapfill";

/// (Non-)Implementation of date_bin_wallclock_gapfill.
/// This function takes arguments identical to `date_bin_wallclock()` but
/// works in conjunction with the logical optimizer rule
/// `HandleGapFill` to fill gaps in time series data.
pub(crate) static DATE_BIN_WALLCLOCK_GAPFILL: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(GapFillWrapper::new(
        date_bin_wallclock::date_bin_wallclock(),
    )))
});

/// Wrapper around date_bin style functions to enable gap filling
/// functionality. Although presented as a scalar function the planner
/// will rewrite queries including this wrapper to use a GapFill node.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct GapFillWrapper {
    udf: Arc<ScalarUDF>,
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for GapFillWrapper {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.udf.inner().return_type(arg_types)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Err(DataFusionError::NotImplemented(format!(
            "{} is not yet implemented",
            self.name
        )))
    }
}

impl GapFillWrapper {
    /// Create a new GapFillUDFWrapper around a date_bin style UDF.
    fn new(udf: Arc<ScalarUDF>) -> Self {
        let name = format!("{}_gapfill", udf.name());
        // The gapfill UDFs have the same type signature as the underlying UDF.
        let Signature {
            type_signature,
            volatility: _,
        } = udf.signature().clone();
        // We don't want this to be optimized away before we can give a helpful error message
        let signature = Signature {
            type_signature,
            volatility: Volatility::Volatile,
        };
        Self {
            udf,
            name,
            signature,
        }
    }

    /// Get the wrapped UDF.
    pub fn inner(&self) -> &Arc<ScalarUDF> {
        &self.udf
    }
}

/// The name of the locf UDF given to DataFusion.
pub const LOCF_UDF_NAME: &str = "locf";

/// The virtual function definition for the `locf` gap-filling
/// function. This function is never actually invoked, but is used to
/// provider parameters for the GapFill node that is added to the plan.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct LocfUDF {
    signature: Signature,
}

impl ScalarUDFImpl for LocfUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        LOCF_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "{LOCF_UDF_NAME} should have at least 1 argument"
            )));
        }
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Err(DataFusionError::NotImplemented(format!(
            "{LOCF_UDF_NAME} is not yet implemented"
        )))
    }
}

/// (Non-)Implementation of locf.
/// This function takes a single argument of any type and
/// produces a value of the same type. It is
/// used in the context of gap-filling queries to represent
/// "last observation carried forward." It does not have
/// an implementation since it will be consumed by the logical optimizer rule
/// `HandleGapFill`.
pub(crate) static LOCF: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(LocfUDF {
        signature: Signature::any(1, Volatility::Volatile),
    }))
});

/// The name of the interpolate UDF given to DataFusion.
pub const INTERPOLATE_UDF_NAME: &str = "interpolate";

/// The virtual function definition for the `interpolate` gap-filling
/// function. This function is never actually invoked, but is used to
/// provider parameters for the GapFill node that is added to the plan.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct InterpolateUDF {
    signature: Signature,
}

impl ScalarUDFImpl for InterpolateUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        INTERPOLATE_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "{INTERPOLATE_UDF_NAME} should have at least 1 argument"
            )));
        }
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Err(DataFusionError::NotImplemented(format!(
            "{INTERPOLATE_UDF_NAME} is not yet implemented"
        )))
    }
}

/// (Non-)Implementation of interpolate.
/// This function takes a single numeric argument and
/// produces a value of the same type. It is
/// used in the context of gap-filling queries to indicate
/// columns that should be inmterpolated. It does not have
/// an implementation since it will be consumed by the logical optimizer rule
/// `HandleGapFill`.
pub(crate) static INTERPOLATE: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    let signatures = [
        InfluxFieldType::Float,
        InfluxFieldType::Integer,
        InfluxFieldType::UInteger,
    ]
    .iter()
    .flat_map(|&influx_type| {
        [
            TypeSignature::Exact(vec![influx_type.into()]),
            TypeSignature::Exact(vec![DataType::Struct(
                vec![
                    Field::new("value", influx_type.into(), true),
                    Field::new(
                        "time",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        true,
                    ),
                ]
                .into(),
            )]),
            TypeSignature::Exact(vec![DataType::Struct(
                vec![
                    Field::new("value", influx_type.into(), true),
                    Field::new(
                        "time",
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                        true,
                    ),
                ]
                .into(),
            )]),
        ]
    })
    .collect();
    Arc::new(ScalarUDF::from(InterpolateUDF {
        signature: Signature::one_of(signatures, Volatility::Volatile),
    }))
});

#[cfg(test)]
mod test {
    use arrow::array::{ArrayRef, Float64Array, TimestampNanosecondArray};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::assert_contains;
    use datafusion::error::Result;
    use datafusion::prelude::{Expr, SessionContext, col};
    use datafusion::scalar::ScalarValue;
    use datafusion_util::lit_timestamptz_nano;
    use schema::TIME_DATA_TIMEZONE;
    use std::sync::Arc;

    fn date_bin_gapfill(stride: Expr, source: Expr, origin: Expr) -> Expr {
        crate::registry()
            .udf(super::DATE_BIN_GAPFILL_UDF_NAME)
            .expect("should be registered")
            .call(vec![stride, source, origin])
    }

    fn lit_interval_milliseconds(v: i64) -> Expr {
        Expr::Literal(ScalarValue::new_interval_mdn(0, 0, v * 1_000_000), None)
    }

    #[tokio::test]
    async fn date_bin_gapfill_errs() -> Result<()> {
        let times = Arc::new(
            TimestampNanosecondArray::from(vec![Some(1000)])
                .with_timezone_opt(TIME_DATA_TIMEZONE()),
        );
        let rb = RecordBatch::try_from_iter(vec![("time", times as ArrayRef)])?;
        let ctx = SessionContext::new();
        ctx.register_batch("t", rb).unwrap();

        let df = ctx.table("t").await?.select(vec![date_bin_gapfill(
            lit_interval_milliseconds(360_000),
            col("time"),
            lit_timestamptz_nano(0),
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
        let ctx = SessionContext::new();
        ctx.register_batch("t", rb).unwrap();
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
        let ctx = SessionContext::new();
        ctx.register_batch("t", rb).unwrap();
        let df = ctx
            .table("t")
            .await
            .unwrap()
            .select(vec![interpolate(col("f0"))])
            .unwrap();
        let res = df.collect().await;
        let expected = "interpolate is not yet implemented";
        assert!(
            res.expect_err("should be an error")
                .to_string()
                .contains(expected)
        );
    }
}
