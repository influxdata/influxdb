use std::sync::Arc;

use datafusion::optimizer::optimizer::Optimizer;

use self::influx_regex_to_datafusion_regex::InfluxRegexToDataFusionRegex;

mod influx_regex_to_datafusion_regex;

/// Create IOx-specific logical [`Optimizer`].
///
/// This is mostly the default optimizer that DataFusion provides but with some additional passes.
pub fn iox_optimizer() -> Optimizer {
    let mut opt = Optimizer::new();
    opt.rules
        .push(Arc::new(InfluxRegexToDataFusionRegex::new()));
    opt
}
