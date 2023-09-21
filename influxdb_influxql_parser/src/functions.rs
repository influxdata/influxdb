//! # [Functions] supported by InfluxQL
//!
//! [Functions]: https://docs.influxdata.com/influxdb/v1.8/query_language/functions/

use std::collections::HashSet;

use once_cell::sync::Lazy;

/// Returns `true` if `name` is a mathematical scalar function
/// supported by InfluxQL.
pub fn is_scalar_math_function(name: &str) -> bool {
    static FUNCTIONS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
        HashSet::from([
            "abs", "sin", "cos", "tan", "asin", "acos", "atan", "atan2", "exp", "log", "ln",
            "log2", "log10", "sqrt", "pow", "floor", "ceil", "round",
        ])
    });

    FUNCTIONS.contains(name)
}

/// Returns `true` if `name` is an aggregate or aggregate function
/// supported by InfluxQL.
pub fn is_aggregate_function(name: &str) -> bool {
    static FUNCTIONS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
        HashSet::from([
            // Scalar-like functions
            "cumulative_sum",
            "derivative",
            "difference",
            "elapsed",
            "moving_average",
            "non_negative_derivative",
            "non_negative_difference",
            // Selector functions
            "bottom",
            "first",
            "last",
            "max",
            "min",
            "percentile",
            "sample",
            "top",
            // Aggregate functions
            "count",
            "integral",
            "mean",
            "median",
            "mode",
            "spread",
            "stddev",
            "sum",
            // Prediction functions
            "holt_winters",
            "holt_winters_with_fit",
            // Technical analysis functions
            "chande_momentum_oscillator",
            "exponential_moving_average",
            "double_exponential_moving_average",
            "kaufmans_efficiency_ratio",
            "kaufmans_adaptive_moving_average",
            "triple_exponential_moving_average",
            "triple_exponential_derivative",
            "relative_strength_index",
        ])
    });

    FUNCTIONS.contains(name)
}

/// Returns `true` if `name` is `"now"`.
pub fn is_now_function(name: &str) -> bool {
    name == "now"
}
