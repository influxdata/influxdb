//! User-defined functions to serve as stand-ins when constructing the
//! projection expressions for the logical plan.
//!
//! As stand-ins, they allow the projection operator to represent the same
//! call information as the InfluxQL AST. These expressions are then
//! rewritten at a later stage of planning, with more context available.

use crate::plan::util::find_exprs_in_exprs;
use crate::{error, NUMERICS};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::logical_expr::{
    Expr, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature, TypeSignature,
    Volatility,
};
use once_cell::sync::Lazy;
use std::sync::Arc;

pub(super) enum WindowFunction {
    MovingAverage,
    Difference,
    NonNegativeDifference,
    Derivative,
    NonNegativeDerivative,
    CumulativeSum,
}

impl WindowFunction {
    /// Try to return the equivalent [`WindowFunction`] for `fun`.
    pub(super) fn try_from_scalar_udf(fun: Arc<ScalarUDF>) -> Option<Self> {
        match fun.name.as_str() {
            MOVING_AVERAGE_UDF_NAME => Some(Self::MovingAverage),
            DIFFERENCE_UDF_NAME => Some(Self::Difference),
            NON_NEGATIVE_DIFFERENCE_UDF_NAME => Some(Self::NonNegativeDifference),
            DERIVATIVE_UDF_NAME => Some(Self::Derivative),
            NON_NEGATIVE_DERIVATIVE_UDF_NAME => Some(Self::NonNegativeDerivative),
            CUMULATIVE_SUM_UDF_NAME => Some(Self::CumulativeSum),
            _ => None,
        }
    }
}

/// Find all [`Expr::ScalarUDF`] expressions that match one of the supported
/// window UDF functions.
pub(super) fn find_window_udfs(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs(
        exprs,
        &|nested_expr| matches!(nested_expr, Expr::ScalarUDF(s) if WindowFunction::try_from_scalar_udf(Arc::clone(&s.fun)).is_some()),
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
            NUMERICS
                .iter()
                .map(|dt| TypeSignature::Exact(vec![dt.clone(), DataType::Int64]))
                .collect(),
            Volatility::Immutable,
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
            NUMERICS
                .iter()
                .map(|dt| TypeSignature::Exact(vec![dt.clone()]))
                .collect(),
            Volatility::Immutable,
        ),
        &return_type_fn,
        &stand_in_impl(DIFFERENCE_UDF_NAME),
    ))
});

const NON_NEGATIVE_DIFFERENCE_UDF_NAME: &str = "non_negative_difference";

/// Create an expression to represent the `NON_NEGATIVE_DIFFERENCE` function.
pub(crate) fn non_negative_difference(args: Vec<Expr>) -> Expr {
    NON_NEGATIVE_DIFFERENCE.call(args)
}

/// Definition of the `NON_NEGATIVE_DIFFERENCE` function.
static NON_NEGATIVE_DIFFERENCE: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    let return_type_fn: ReturnTypeFunction = Arc::new(|args| Ok(Arc::new(args[0].clone())));
    Arc::new(ScalarUDF::new(
        NON_NEGATIVE_DIFFERENCE_UDF_NAME,
        &Signature::one_of(
            NUMERICS
                .iter()
                .map(|dt| TypeSignature::Exact(vec![dt.clone()]))
                .collect(),
            Volatility::Immutable,
        ),
        &return_type_fn,
        &stand_in_impl(NON_NEGATIVE_DIFFERENCE_UDF_NAME),
    ))
});

const DERIVATIVE_UDF_NAME: &str = "derivative";

/// Create an expression to represent the `DERIVATIVE` function.
pub(crate) fn derivative(args: Vec<Expr>) -> Expr {
    DERIVATIVE.call(args)
}

/// Definition of the `DERIVATIVE` function.
static DERIVATIVE: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Float64)));
    Arc::new(ScalarUDF::new(
        DERIVATIVE_UDF_NAME,
        &Signature::one_of(
            NUMERICS
                .iter()
                .flat_map(|dt| {
                    vec![
                        TypeSignature::Exact(vec![dt.clone()]),
                        TypeSignature::Exact(vec![
                            dt.clone(),
                            DataType::Duration(TimeUnit::Nanosecond),
                        ]),
                    ]
                })
                .collect(),
            Volatility::Immutable,
        ),
        &return_type_fn,
        &stand_in_impl(DERIVATIVE_UDF_NAME),
    ))
});

const NON_NEGATIVE_DERIVATIVE_UDF_NAME: &str = "non_negative_derivative";

/// Create an expression to represent the `NON_NEGATIVE_DERIVATIVE` function.
pub(crate) fn non_negative_derivative(args: Vec<Expr>) -> Expr {
    NON_NEGATIVE_DERIVATIVE.call(args)
}

/// Definition of the `NON_NEGATIVE_DERIVATIVE` function.
static NON_NEGATIVE_DERIVATIVE: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    let return_type_fn: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Float64)));
    Arc::new(ScalarUDF::new(
        NON_NEGATIVE_DERIVATIVE_UDF_NAME,
        &Signature::one_of(
            NUMERICS
                .iter()
                .flat_map(|dt| {
                    vec![
                        TypeSignature::Exact(vec![dt.clone()]),
                        TypeSignature::Exact(vec![
                            dt.clone(),
                            DataType::Duration(TimeUnit::Nanosecond),
                        ]),
                    ]
                })
                .collect(),
            Volatility::Immutable,
        ),
        &return_type_fn,
        &stand_in_impl(NON_NEGATIVE_DERIVATIVE_UDF_NAME),
    ))
});

const CUMULATIVE_SUM_UDF_NAME: &str = "cumulative_sum";

/// Create an expression to represent the `CUMULATIVE_SUM` function.
pub(crate) fn cumulative_sum(args: Vec<Expr>) -> Expr {
    CUMULATIVE_SUM.call(args)
}
/// Definition of the `CUMULATIVE_SUM` function.
static CUMULATIVE_SUM: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    let return_type_fn: ReturnTypeFunction = Arc::new(|args| Ok(Arc::new(args[0].clone())));
    Arc::new(ScalarUDF::new(
        CUMULATIVE_SUM_UDF_NAME,
        &Signature::one_of(
            NUMERICS
                .iter()
                .map(|dt| TypeSignature::Exact(vec![dt.clone()]))
                .collect(),
            Volatility::Immutable,
        ),
        &return_type_fn,
        &stand_in_impl(CUMULATIVE_SUM_UDF_NAME),
    ))
});

/// Returns an implementation that always returns an error.
fn stand_in_impl(name: &'static str) -> ScalarFunctionImplementation {
    Arc::new(move |_| error::internal(format!("{name} should not exist in the final logical plan")))
}
