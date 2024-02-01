//! User-defined functions to serve as stand-ins when constructing the
//! projection expressions for the logical plan.
//!
//! As stand-ins, they allow the projection operator to represent the same
//! call information as the InfluxQL AST. These expressions are then
//! rewritten at a later stage of planning, with more context available.

use crate::plan::util::find_exprs_in_exprs;
use crate::{error, NUMERICS};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::{
        Expr, ScalarFunctionDefinition, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
        Volatility,
    },
    physical_plan::ColumnarValue,
};
use once_cell::sync::Lazy;
use std::{any::Any, sync::Arc};

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
        match fun.name() {
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

/// Find all [`ScalarUDF`] expressions that match one of the supported
/// window UDF functions.
pub(super) fn find_window_udfs(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| {
        let Expr::ScalarFunction(fun) = nested_expr else {
            return false;
        };
        let ScalarFunctionDefinition::UDF(udf) = &fun.func_def else {
            return false;
        };
        WindowFunction::try_from_scalar_udf(Arc::clone(udf)).is_some()
    })
}

const MOVING_AVERAGE_UDF_NAME: &str = "moving_average";

#[derive(Debug)]
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

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
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
static MOVING_AVERAGE: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
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

#[derive(Debug)]
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

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
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
static DIFFERENCE: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
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

const NON_NEGATIVE_DIFFERENCE_UDF_NAME: &str = "non_negative_difference";

#[derive(Debug)]
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

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
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
static NON_NEGATIVE_DIFFERENCE: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
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

const DERIVATIVE_UDF_NAME: &str = "derivative";

#[derive(Debug)]
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

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
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
static DERIVATIVE: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
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
                    ]
                })
                .collect(),
            Volatility::Immutable,
        ),
    }))
});

const NON_NEGATIVE_DERIVATIVE_UDF_NAME: &str = "non_negative_derivative";

#[derive(Debug)]
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

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
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
static NON_NEGATIVE_DERIVATIVE: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    Arc::new(ScalarUDF::from(NonNegativeDerivativeUDF {
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
                    ]
                })
                .collect(),
            Volatility::Immutable,
        ),
    }))
});

const CUMULATIVE_SUM_UDF_NAME: &str = "cumulative_sum";

#[derive(Debug)]
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

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
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
static CUMULATIVE_SUM: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
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
