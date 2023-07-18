//! User defined window functions implementing influxQL features.

use datafusion::logical_expr::{
    PartitionEvaluatorFactory, ReturnTypeFunction, WindowFunction, WindowUDF,
};
use once_cell::sync::Lazy;
use std::sync::Arc;

mod cumulative_sum;
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
