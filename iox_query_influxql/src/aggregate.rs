//! User defined aggregate functions implementing influxQL features.

use datafusion::logical_expr::{
    AccumulatorFactoryFunction, AggregateUDF, ReturnTypeFunction, StateTypeFunction,
};
use once_cell::sync::Lazy;
use std::sync::Arc;

mod percentile;

/// Definition of the `PERCENTILE` user-defined aggregate function.
pub(crate) static PERCENTILE: Lazy<Arc<AggregateUDF>> = Lazy::new(|| {
    let return_type: ReturnTypeFunction = Arc::new(percentile::return_type);
    let accumulator: AccumulatorFactoryFunction = Arc::new(percentile::accumulator);
    let state_type: StateTypeFunction = Arc::new(percentile::state_type);

    Arc::new(AggregateUDF::new(
        percentile::NAME,
        &percentile::SIGNATURE,
        &return_type,
        &accumulator,
        &state_type,
    ))
});
