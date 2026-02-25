//! User defined aggregate functions implementing influxQL features.

use datafusion::logical_expr::AggregateUDF;
use std::sync::{Arc, LazyLock};

mod mode;
mod percentile;
mod spread;

/// Definition of the `PERCENTILE` user-defined aggregate function.
pub(crate) static PERCENTILE: LazyLock<Arc<AggregateUDF>> =
    LazyLock::new(|| Arc::new(AggregateUDF::new_from_impl(percentile::PercentileUDF::new())));

pub(crate) static SPREAD: LazyLock<Arc<AggregateUDF>> =
    LazyLock::new(|| Arc::new(AggregateUDF::new_from_impl(spread::SpreadUDF::new())));

pub(crate) static MODE: LazyLock<Arc<AggregateUDF>> =
    LazyLock::new(|| Arc::new(AggregateUDF::new_from_impl(mode::ModeUDF::new())));
