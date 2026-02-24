//! User-defined functions to serve as stand-ins when constructing the
//! projection expressions for the logical plan.
//!
//! As stand-ins, they allow the projection operator to represent the same
//! call information as the InfluxQL AST. These expressions are then
//! rewritten at a later stage of planning, with more context available.

use crate::{NUMERICS, error, plan::util::find_exprs_in_exprs};
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::{
        Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
        expr::ScalarFunction,
    },
    physical_plan::ColumnarValue,
};
use std::{
    any::Any,
    sync::{Arc, LazyLock},
};

pub(super) enum WindowFunction {
    MovingAverage,
    Difference,
    NonNegativeDifference,
    Derivative,
    NonNegativeDerivative,
    CumulativeSum,
    Elapsed,
}

impl WindowFunction {
    /// Try to return the equivalent [`WindowFunction`] for `fun`.
    pub(super) fn try_from_scalar_udf(fun: Arc<ScalarUDF>) -> Option<Self> {
        match fun.name() {
            MOVING_AVERAGE_UDF_NAME => Some(Self::MovingAverage),
            DIFFERENCE_UDF_NAME => Some(Self::Difference),
            NON_NEGATIVE_DIFFERENCE_UDF_NAME => Some(Self::NonNegativeDifference),
            DERIVATIVE_UDF_NAME => Some(Self::Derivative),
            NON_NEGATIVE_DERIVATIVE_UDF_NAME => Some(Self::NonNegativeDerivative),
            CUMULATIVE_SUM_UDF_NAME => Some(Self::CumulativeSum),
            ELAPSED_UDF_NAME => Some(Self::Elapsed),
            _ => None,
        }
    }
}

/// Find all [`ScalarUDF`] expressions that match one of the supported
/// window UDF functions.
pub(super) fn find_window_udfs(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| {
        let Expr::ScalarFunction(fun) = nested_expr else {
            return false;
        };
        WindowFunction::try_from_scalar_udf(Arc::clone(&fun.func)).is_some()
    })
}

const MOVING_AVERAGE_UDF_NAME: &str = "moving_average";

#[derive(Debug, PartialEq, Eq, Hash)]
struct MovingAverageUDF {
    signature: Signature,
}

impl ScalarUDFImpl for MovingAverageUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        MOVING_AVERAGE_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        error::internal(format!(
            "{MOVING_AVERAGE_UDF_NAME} should not exist in the final logical plan"
        ))
    }
}

/// Create an expression to represent the `MOVING_AVERAGE` function.
pub(crate) fn moving_average(args: Vec<Expr>) -> Expr {
    MOVING_AVERAGE.call(args)
}

/// Definition of the `MOVING_AVERAGE` function.
static MOVING_AVERAGE: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(MovingAverageUDF {
        signature: Signature::one_of(
            NUMERICS
                .iter()
                .map(|dt| TypeSignature::Exact(vec![dt.clone(), DataType::Int64]))
                .collect(),
            Volatility::Immutable,
        ),
    }))
});

const DIFFERENCE_UDF_NAME: &str = "difference";

#[derive(Debug, PartialEq, Eq, Hash)]
struct DifferenceUDF {
    signature: Signature,
}

impl ScalarUDFImpl for DifferenceUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        DIFFERENCE_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "{DIFFERENCE_UDF_NAME} expects at least 1 argument"
            )));
        }
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        error::internal(format!(
            "{DIFFERENCE_UDF_NAME} should not exist in the final logical plan"
        ))
    }
}

/// Create an expression to represent the `DIFFERENCE` function.
pub(crate) fn difference(args: Vec<Expr>) -> Expr {
    DIFFERENCE.call(args)
}

/// Definition of the `DIFFERENCE` function.
static DIFFERENCE: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(DifferenceUDF {
        signature: Signature::one_of(
            NUMERICS
                .iter()
                .map(|dt| TypeSignature::Exact(vec![dt.clone()]))
                .collect(),
            Volatility::Immutable,
        ),
    }))
});

const ELAPSED_UDF_NAME: &str = "elapsed";

#[derive(Debug, PartialEq, Eq, Hash)]
struct ElapsedUDF {
    signature: Signature,
}

impl ScalarUDFImpl for ElapsedUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        ELAPSED_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        error::internal(format!(
            "{ELAPSED_UDF_NAME} should not exist in the final logical plan"
        ))
    }
}

/// Create an expression to represent the `ELAPSED` function.
pub(crate) fn elapsed(args: Vec<Expr>) -> Expr {
    ELAPSED.call(args)
}

/// Definition of the `ELAPSED` function.
static ELAPSED: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(ElapsedUDF {
        signature: Signature::any(3, Volatility::Immutable),
    }))
});

const NON_NEGATIVE_DIFFERENCE_UDF_NAME: &str = "non_negative_difference";

#[derive(Debug, PartialEq, Eq, Hash)]
struct NonNegativeDifferenceUDF {
    signature: Signature,
}

impl ScalarUDFImpl for NonNegativeDifferenceUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        NON_NEGATIVE_DIFFERENCE_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "{NON_NEGATIVE_DIFFERENCE_UDF_NAME} expects at least 1 argument"
            )));
        }
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        error::internal(format!(
            "{NON_NEGATIVE_DIFFERENCE_UDF_NAME} should not exist in the final logical plan"
        ))
    }
}

/// Create an expression to represent the `NON_NEGATIVE_DIFFERENCE` function.
pub(crate) fn non_negative_difference(args: Vec<Expr>) -> Expr {
    NON_NEGATIVE_DIFFERENCE.call(args)
}

/// Definition of the `NON_NEGATIVE_DIFFERENCE` function.
static NON_NEGATIVE_DIFFERENCE: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(NonNegativeDifferenceUDF {
        signature: Signature::one_of(
            NUMERICS
                .iter()
                .map(|dt| TypeSignature::Exact(vec![dt.clone()]))
                .collect(),
            Volatility::Immutable,
        ),
    }))
});

pub(crate) const INTEGRAL_UDF_NAME: &str = "integral";

#[derive(Debug, PartialEq, Eq, Hash)]
struct IntegralUDF {
    signature: Signature,
}

impl ScalarUDFImpl for IntegralUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        INTEGRAL_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        error::internal(format!(
            "{INTEGRAL_UDF_NAME} should not exist in the final logical plan"
        ))
    }
}

/// Create an expression to represent the `INTEGRAL` function.
///
/// INTEGRAL is represented as a SUM wrapping an internal integral window function.
pub(crate) fn integral(args: Vec<Expr>) -> Expr {
    INTEGRAL.call(args)
}

pub(crate) fn is_integral_udf(expr: &Expr) -> bool {
    let Expr::ScalarFunction(ScalarFunction { func, .. }) = expr else {
        return false;
    };
    func.name() == INTEGRAL_UDF_NAME
}

pub(crate) fn find_integral_udfs(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &is_integral_udf)
}

/// Definition of the `INTEGRAL` function.
static INTEGRAL: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(IntegralUDF {
        signature: Signature::one_of(
            NUMERICS
                .iter()
                .flat_map(|dt| {
                    vec![
                        TypeSignature::Exact(vec![dt.clone()]),
                        TypeSignature::Exact(vec![
                            dt.clone(),
                            DataType::Duration(TimeUnit::Nanosecond),
                        ]),
                        TypeSignature::Exact(vec![
                            dt.clone(),
                            DataType::Interval(IntervalUnit::MonthDayNano),
                        ]),
                    ]
                })
                .collect(),
            Volatility::Immutable,
        ),
    }))
});

const DERIVATIVE_UDF_NAME: &str = "derivative";

#[derive(Debug, PartialEq, Eq, Hash)]
struct DerivativeUDF {
    signature: Signature,
}

impl ScalarUDFImpl for DerivativeUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        DERIVATIVE_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        error::internal(format!(
            "{DERIVATIVE_UDF_NAME} should not exist in the final logical plan"
        ))
    }
}

/// Create an expression to represent the `DERIVATIVE` function.
pub(crate) fn derivative(args: Vec<Expr>) -> Expr {
    DERIVATIVE.call(args)
}

/// Definition of the `DERIVATIVE` function.
static DERIVATIVE: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(DerivativeUDF {
        signature: Signature::one_of(
            NUMERICS
                .iter()
                .flat_map(|dt| {
                    vec![
                        TypeSignature::Exact(vec![dt.clone()]),
                        TypeSignature::Exact(vec![
                            dt.clone(),
                            DataType::Duration(TimeUnit::Nanosecond),
                        ]),
                        TypeSignature::Exact(vec![
                            dt.clone(),
                            DataType::Interval(IntervalUnit::MonthDayNano),
                        ]),
                    ]
                })
                .collect(),
            Volatility::Immutable,
        ),
    }))
});

const NON_NEGATIVE_DERIVATIVE_UDF_NAME: &str = "non_negative_derivative";

#[derive(Debug, PartialEq, Eq, Hash)]
struct NonNegativeDerivativeUDF {
    signature: Signature,
}

impl ScalarUDFImpl for NonNegativeDerivativeUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        NON_NEGATIVE_DERIVATIVE_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        error::internal(format!(
            "{NON_NEGATIVE_DERIVATIVE_UDF_NAME} should not exist in the final logical plan"
        ))
    }
}
/// Create an expression to represent the `NON_NEGATIVE_DERIVATIVE` function.
pub(crate) fn non_negative_derivative(args: Vec<Expr>) -> Expr {
    NON_NEGATIVE_DERIVATIVE.call(args)
}

/// Definition of the `NON_NEGATIVE_DERIVATIVE` function.
static NON_NEGATIVE_DERIVATIVE: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(NonNegativeDerivativeUDF {
        signature: Signature::one_of(
            NUMERICS
                .iter()
                .flat_map(|dt| {
                    [
                        TypeSignature::Exact(vec![dt.clone()]),
                        TypeSignature::Exact(vec![
                            dt.clone(),
                            DataType::Duration(TimeUnit::Nanosecond),
                        ]),
                        TypeSignature::Exact(vec![
                            dt.clone(),
                            DataType::Interval(IntervalUnit::MonthDayNano),
                        ]),
                    ]
                })
                .collect(),
            Volatility::Immutable,
        ),
    }))
});

const CUMULATIVE_SUM_UDF_NAME: &str = "cumulative_sum";

#[derive(Debug, PartialEq, Eq, Hash)]
struct CumulativeSumUDF {
    signature: Signature,
}

impl ScalarUDFImpl for CumulativeSumUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        CUMULATIVE_SUM_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "{CUMULATIVE_SUM_UDF_NAME} expects at least 1 argument"
            )));
        }
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        error::internal(format!(
            "{CUMULATIVE_SUM_UDF_NAME} should not exist in the final logical plan"
        ))
    }
}

/// Create an expression to represent the `CUMULATIVE_SUM` function.
pub(crate) fn cumulative_sum(args: Vec<Expr>) -> Expr {
    CUMULATIVE_SUM.call(args)
}
/// Definition of the `CUMULATIVE_SUM` function.
static CUMULATIVE_SUM: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(CumulativeSumUDF {
        signature: Signature::one_of(
            NUMERICS
                .iter()
                .map(|dt| TypeSignature::Exact(vec![dt.clone()]))
                .collect(),
            Volatility::Immutable,
        ),
    }))
});
