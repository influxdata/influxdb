//! User defined window functions implementing influxQL features.

use datafusion::logical_expr::{WindowFunctionDefinition, WindowUDF};
use std::sync::{Arc, LazyLock};

mod cumulative_sum;
mod derivative;
mod difference;
mod elapsed;
mod integral;
mod moving_average;
mod non_negative;
mod percent_row_number;

/// Definition of the `CUMULATIVE_SUM` user-defined window function.
pub(crate) static CUMULATIVE_SUM: LazyLock<WindowFunctionDefinition> = LazyLock::new(|| {
    WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::new_from_impl(
        cumulative_sum::CumulativeSumUDWF::new(),
    )))
});

/// Definition of the `DERIVATIVE` user-defined window function.
pub(crate) static DERIVATIVE: LazyLock<WindowFunctionDefinition> = LazyLock::new(|| {
    WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::new_from_impl(
        derivative::DerivativeUDWF::new(),
    )))
});

/// Definition of the `DIFFERENCE` user-defined window function.
pub(crate) static DIFFERENCE: LazyLock<WindowFunctionDefinition> = LazyLock::new(|| {
    WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::new_from_impl(
        difference::DifferenceUDWF::new(),
    )))
});

/// Definition of the `ELAPSED` user-defined window function.
pub(crate) static ELAPSED: LazyLock<WindowFunctionDefinition> = LazyLock::new(|| {
    WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::new_from_impl(
        elapsed::ElapsedUDWF::new(),
    )))
});

/// Definition of the internal `INTEGRAL_WINDOW` user-defined window function.
pub(crate) static INTEGRAL_WINDOW: LazyLock<WindowFunctionDefinition> = LazyLock::new(|| {
    WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::new_from_impl(
        integral::IntegralUDWF::new(),
    )))
});

/// Definition of the `MOVING_AVERAGE` user-defined window function.
pub(crate) static MOVING_AVERAGE: LazyLock<WindowFunctionDefinition> = LazyLock::new(|| {
    WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::new_from_impl(
        moving_average::MovingAverageUDWF::new(),
    )))
});

/// Definition of the `NON_NEGATIVE_DERIVATIVE` user-defined window function.
pub(crate) static NON_NEGATIVE_DERIVATIVE: LazyLock<WindowFunctionDefinition> =
    LazyLock::new(|| {
        WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::new_from_impl(
            non_negative::NonNegativeUDWF::new(
                "non_negative_derivative",
                derivative::DerivativeUDWF::new(),
            ),
        )))
    });
/// Definition of the `NON_NEGATIVE_DIFFERENCE` user-defined window function.
pub(crate) static NON_NEGATIVE_DIFFERENCE: LazyLock<WindowFunctionDefinition> =
    LazyLock::new(|| {
        WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::new_from_impl(
            non_negative::NonNegativeUDWF::new(
                "non_negative_difference",
                difference::DifferenceUDWF::new(),
            ),
        )))
    });

/// Definition of the `PERCENT_ROW_NUMBER` user-defined window function.
pub(crate) static PERCENT_ROW_NUMBER: LazyLock<WindowFunctionDefinition> = LazyLock::new(|| {
    WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::new_from_impl(
        percent_row_number::PercentRowNumberUDWF::new(),
    )))
});
