//! User defined window functions implementing influxQL features.

use datafusion::logical_expr::{WindowFunctionDefinition, WindowUDF};
use once_cell::sync::Lazy;
use std::sync::Arc;

mod cumulative_sum;
mod derivative;
mod difference;
mod moving_average;
mod non_negative;
mod percent_row_number;

/// Definition of the `CUMULATIVE_SUM` user-defined window function.
pub(crate) static CUMULATIVE_SUM: Lazy<WindowFunctionDefinition> = Lazy::new(|| {
    WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::new_from_impl(
        cumulative_sum::CumulativeSumUDWF::new(),
    )))
});

/// Definition of the `DERIVATIVE` user-defined window function.
pub(crate) static DERIVATIVE: Lazy<WindowFunctionDefinition> = Lazy::new(|| {
    WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::new_from_impl(
        derivative::DerivativeUDWF::new(),
    )))
});

/// Definition of the `DIFFERENCE` user-defined window function.
pub(crate) static DIFFERENCE: Lazy<WindowFunctionDefinition> = Lazy::new(|| {
    WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::new_from_impl(
        difference::DifferenceUDWF::new(),
    )))
});

/// Definition of the `MOVING_AVERAGE` user-defined window function.
pub(crate) static MOVING_AVERAGE: Lazy<WindowFunctionDefinition> = Lazy::new(|| {
    WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::new_from_impl(
        moving_average::MovingAverageUDWF::new(),
    )))
});

/// Definition of the `NON_NEGATIVE_DERIVATIVE` user-defined window function.
pub(crate) static NON_NEGATIVE_DERIVATIVE: Lazy<WindowFunctionDefinition> = Lazy::new(|| {
    WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::new_from_impl(
        non_negative::NonNegativeUDWF::new(
            "non_negative_derivative",
            derivative::DerivativeUDWF::new(),
        ),
    )))
});
/// Definition of the `NON_NEGATIVE_DIFFERENCE` user-defined window function.
pub(crate) static NON_NEGATIVE_DIFFERENCE: Lazy<WindowFunctionDefinition> = Lazy::new(|| {
    WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::new_from_impl(
        non_negative::NonNegativeUDWF::new(
            "non_negative_difference",
            difference::DifferenceUDWF::new(),
        ),
    )))
});

/// Definition of the `PERCENT_ROW_NUMBER` user-defined window function.
pub(crate) static PERCENT_ROW_NUMBER: Lazy<WindowFunctionDefinition> = Lazy::new(|| {
    WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::new_from_impl(
        percent_row_number::PercentRowNumberUDWF::new(),
    )))
});
