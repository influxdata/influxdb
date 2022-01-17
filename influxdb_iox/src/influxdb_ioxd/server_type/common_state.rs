use std::sync::Arc;

use snafu::{ResultExt, Snafu};
use trace::TraceCollector;

use crate::{
    clap_blocks::run_config::RunConfig, influxdb_ioxd::serving_readiness::ServingReadiness,
};

#[derive(Debug, Snafu)]
pub enum CommonServerStateError {
    #[snafu(display("Cannot create tracing pipeline: {}", source))]
    Tracing { source: trace_exporters::Error },
}

/// Common state used by all server types (e.g. `Database` and `Router`)
#[derive(Debug)]
pub struct CommonServerState {
    run_config: RunConfig,
    serving_readiness: ServingReadiness,
    trace_exporter: Option<Arc<trace_exporters::export::AsyncExporter>>,
}

impl CommonServerState {
    pub fn from_config(run_config: RunConfig) -> Result<Self, CommonServerStateError> {
        let serving_readiness = run_config.initial_serving_state.clone().into();
        let trace_exporter = run_config.tracing_config.build().context(TracingSnafu)?;

        Ok(Self {
            run_config,
            serving_readiness,
            trace_exporter,
        })
    }

    #[cfg(test)]
    pub fn for_testing() -> Self {
        use clap::Parser;

        Self::from_config(
            RunConfig::try_parse_from(&["not_used"]).expect("default parsing should work"),
        )
        .expect("default configs should work")
    }

    pub fn run_config(&self) -> &RunConfig {
        &self.run_config
    }

    pub fn serving_readiness(&self) -> &ServingReadiness {
        &self.serving_readiness
    }

    pub fn trace_exporter(&self) -> Option<Arc<trace_exporters::export::AsyncExporter>> {
        self.trace_exporter.clone()
    }

    pub fn trace_collector(&self) -> Option<Arc<dyn TraceCollector>> {
        self.trace_exporter
            .clone()
            .map(|x| -> Arc<dyn TraceCollector> { x })
    }
}
