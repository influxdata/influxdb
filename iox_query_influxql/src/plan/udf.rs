use crate::plan::error;
use crate::plan::util_copy::find_exprs_in_exprs;
use arrow::datatypes::DataType;
use datafusion::logical_expr::{
    Expr, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature, TypeSignature,
    Volatility,
};
use once_cell::sync::Lazy;
use std::sync::Arc;

pub(super) enum WindowFunction {
    MovingAverage,
    Difference,
}

impl WindowFunction {
    /// Try to return the equivalent [`WindowFunction`] for `fun`.
    pub(super) fn try_from_scalar_udf(fun: Arc<ScalarUDF>) -> Option<Self> {
        match fun.name.as_str() {
            MOVING_AVERAGE_UDF_NAME => Some(Self::MovingAverage),
            DIFFERENCE_UDF_NAME => Some(Self::Difference),
            _ => None,
        }
    }
}

/// Find all [`Expr::ScalarUDF`] expressions for one of the supported window UDF functions.
pub(super) fn find_window_udfs(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs(
        exprs,
        &|nested_expr| matches!(nested_expr, Expr::ScalarUDF(s) if WindowFunction::try_from_scalar_udf(s.fun.clone()).is_some()),
    )
}

const MOVING_AVERAGE_UDF_NAME: &str = "moving_average";

/// Create an expression to represent the `MOVING_AVERAGE` function.
pub(crate) fn moving_average(args: Vec<Expr>) -> Expr {
    MOVING_AVERAGE.call(args)
}

/// Definition of the `MOVING_AVERAGE` function.
static MOVING_AVERAGE: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    static RETURN_TYPE: Lazy<Arc<DataType>> = Lazy::new(|| Arc::new(DataType::Float64));

    let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(RETURN_TYPE.clone()));
    Arc::new(ScalarUDF::new(
        MOVING_AVERAGE_UDF_NAME,
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Float64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::UInt64, DataType::Int64]),
            ],
            Volatility::Volatile,
        ),
        &return_type_fn,
        &stand_in_impl(MOVING_AVERAGE_UDF_NAME),
    ))
});

const DIFFERENCE_UDF_NAME: &str = "difference";

/// Create an expression to represent the `DIFFERENCE` function.
pub(crate) fn difference(args: Vec<Expr>) -> Expr {
    DIFFERENCE.call(args)
}

/// Definition of the `DIFFERENCE` function.
static DIFFERENCE: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    let return_type_fn: ReturnTypeFunction = Arc::new(|args| Ok(Arc::new(args[0].clone())));
    Arc::new(ScalarUDF::new(
        DIFFERENCE_UDF_NAME,
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Float64]),
                TypeSignature::Exact(vec![DataType::Int64]),
                TypeSignature::Exact(vec![DataType::UInt64]),
            ],
            Volatility::Volatile,
        ),
        &return_type_fn,
        &stand_in_impl(DIFFERENCE_UDF_NAME),
    ))
});

/// Returns an implementation that always returns an error.
fn stand_in_impl(name: &'static str) -> ScalarFunctionImplementation {
    Arc::new(move |_| error::internal(format!("{name} should not exist in the final logical plan")))
}
