use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;

use self::influx_regex_to_datafusion_regex::InfluxRegexToDataFusionRegex;
use self::regex_to_in_list::RegexToInList;

mod influx_regex_to_datafusion_regex;
mod regex_to_in_list;

/// Register IOx-specific logical [`OptimizerRule`]s with the SessionContext
///
/// [`OptimizerRule`]: datafusion::optimizer::OptimizerRule
pub fn register_iox_logical_optimizers(state: SessionStateBuilder) -> SessionStateBuilder {
    state
        .with_optimizer_rule(Arc::new(InfluxRegexToDataFusionRegex::new()))
        // Must run after `InfluxRegexToDataFusionRegex` so that the `~` / `!~`
        // operators it produces are visible in the same optimizer pass.
        .with_optimizer_rule(Arc::new(RegexToInList::new()))
}
