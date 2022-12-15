use std::sync::Arc;

use datafusion::optimizer::{optimizer::Optimizer, OptimizerConfig};

use self::influx_regex_to_datafusion_regex::InfluxRegexToDataFusionRegex;

mod influx_regex_to_datafusion_regex;

/// Create IOx-specific logical [`Optimizer`].
///
/// This is mostly the default optimizer that DataFusion provides but with some additional passes.
pub fn iox_optimizer() -> Optimizer {
    let mut opt = Optimizer::new(&OptimizerConfig::default());
    opt.rules
        .push(Arc::new(InfluxRegexToDataFusionRegex::new()));
    opt
}
