use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;

use self::influx_regex_to_datafusion_regex::InfluxRegexToDataFusionRegex;

mod influx_regex_to_datafusion_regex;

/// Register IOx-specific logical [`OptimizerRule`]s with the SessionContext
///
/// [`OptimizerRule`]: datafusion::optimizer::OptimizerRule
pub fn register_iox_logical_optimizers(state: SessionStateBuilder) -> SessionStateBuilder {
    state.with_optimizer_rule(Arc::new(InfluxRegexToDataFusionRegex::new()))
}
