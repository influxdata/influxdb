//! User defined window functions implementing influxQL features.

use datafusion::logical_expr::{
    PartitionEvaluatorFactory, ReturnTypeFunction, WindowFunction, WindowUDF,
};
use once_cell::sync::Lazy;
use std::sync::Arc;

mod cumulative_sum;
mod derivative;
mod difference;
mod moving_average;
mod non_negative;
mod percent_row_number;

/// Definition of the `CUMULATIVE_SUM` user-defined window function.
pub(crate) static CUMULATIVE_SUM: Lazy<WindowFunction> = Lazy::new(|| {
    let return_type: ReturnTypeFunction = Arc::new(cumulative_sum::return_type);
    let partition_evaluator_factory: PartitionEvaluatorFactory =
        Arc::new(cumulative_sum::partition_evaluator_factory);

    WindowFunction::WindowUDF(Arc::new(WindowUDF::new(
        cumulative_sum::NAME,
        &cumulative_sum::SIGNATURE,
        &return_type,
        &partition_evaluator_factory,
    )))
});

/// Definition of the `DERIVATIVE` user-defined window function.
pub(crate) static DERIVATIVE: Lazy<WindowFunction> = Lazy::new(|| {
    let return_type: ReturnTypeFunction = Arc::new(derivative::return_type);
    let partition_evaluator_factory: PartitionEvaluatorFactory =
        Arc::new(derivative::partition_evaluator_factory);

    WindowFunction::WindowUDF(Arc::new(WindowUDF::new(
        derivative::NAME,
        &derivative::SIGNATURE,
        &return_type,
        &partition_evaluator_factory,
    )))
});

/// Definition of the `DIFFERENCE` user-defined window function.
pub(crate) static DIFFERENCE: Lazy<WindowFunction> = Lazy::new(|| {
    let return_type: ReturnTypeFunction = Arc::new(difference::return_type);
    let partition_evaluator_factory: PartitionEvaluatorFactory =
        Arc::new(difference::partition_evaluator_factory);

    WindowFunction::WindowUDF(Arc::new(WindowUDF::new(
        difference::NAME,
        &difference::SIGNATURE,
        &return_type,
        &partition_evaluator_factory,
    )))
});

/// Definition of the `MOVING_AVERAGE` user-defined window function.
pub(crate) static MOVING_AVERAGE: Lazy<WindowFunction> = Lazy::new(|| {
    let return_type: ReturnTypeFunction = Arc::new(moving_average::return_type);
    let partition_evaluator_factory: PartitionEvaluatorFactory =
        Arc::new(moving_average::partition_evaluator_factory);

    WindowFunction::WindowUDF(Arc::new(WindowUDF::new(
        moving_average::NAME,
        &moving_average::SIGNATURE,
        &return_type,
        &partition_evaluator_factory,
    )))
});

const NON_NEGATIVE_DERIVATIVE_NAME: &str = "non_negative_derivative";

/// Definition of the `NON_NEGATIVE_DERIVATIVE` user-defined window function.
pub(crate) static NON_NEGATIVE_DERIVATIVE: Lazy<WindowFunction> = Lazy::new(|| {
    let return_type: ReturnTypeFunction = Arc::new(derivative::return_type);
    let partition_evaluator_factory: PartitionEvaluatorFactory = Arc::new(|| {
        Ok(non_negative::wrapper(
            derivative::partition_evaluator_factory()?,
        ))
    });

    WindowFunction::WindowUDF(Arc::new(WindowUDF::new(
        NON_NEGATIVE_DERIVATIVE_NAME,
        &derivative::SIGNATURE,
        &return_type,
        &partition_evaluator_factory,
    )))
});

const NON_NEGATIVE_DIFFERENCE_NAME: &str = "non_negative_difference";

/// Definition of the `NON_NEGATIVE_DIFFERENCE` user-defined window function.
pub(crate) static NON_NEGATIVE_DIFFERENCE: Lazy<WindowFunction> = Lazy::new(|| {
    let return_type: ReturnTypeFunction = Arc::new(difference::return_type);
    let partition_evaluator_factory: PartitionEvaluatorFactory = Arc::new(|| {
        Ok(non_negative::wrapper(
            difference::partition_evaluator_factory()?,
        ))
    });

    WindowFunction::WindowUDF(Arc::new(WindowUDF::new(
        NON_NEGATIVE_DIFFERENCE_NAME,
        &difference::SIGNATURE,
        &return_type,
        &partition_evaluator_factory,
    )))
});

/// Definition of the `PERCENT_ROW_NUMBER` user-defined window function.
pub(crate) static PERCENT_ROW_NUMBER: Lazy<WindowFunction> = Lazy::new(|| {
    let return_type: ReturnTypeFunction = Arc::new(percent_row_number::return_type);
    let partition_evaluator_factory: PartitionEvaluatorFactory =
        Arc::new(percent_row_number::partition_evaluator_factory);

    WindowFunction::WindowUDF(Arc::new(WindowUDF::new(
        percent_row_number::NAME,
        &percent_row_number::SIGNATURE,
        &return_type,
        &partition_evaluator_factory,
    )))
});
