use std::{
    borrow::Cow,
    sync::{Arc, Weak},
};

use datafusion::execution::memory_pool::MemoryPool;
use metric::{Attributes, Instrument, MetricKind, Observation, Reporter};

/// Hooks DataFusion [`MemoryPool`] into our [`metric`] crate.
#[derive(Debug, Clone)]
pub struct DataFusionMemoryPoolMetricsBridge {
    pool: Weak<dyn MemoryPool>,
    limit: usize,
}

impl DataFusionMemoryPoolMetricsBridge {
    /// Register new pool.
    pub fn new(pool: &Arc<dyn MemoryPool>, limit: usize) -> Self {
        Self {
            pool: Arc::downgrade(pool),
            limit,
        }
    }
}

impl Instrument for DataFusionMemoryPoolMetricsBridge {
    fn report(&self, reporter: &mut dyn Reporter) {
        reporter.start_metric(
            "datafusion_mem_pool_bytes",
            "Number of bytes within the datafusion memory pool",
            MetricKind::U64Gauge,
        );
        let Some(pool_arc) = self.pool.upgrade() else {
            return;
        };

        reporter.report_observation(
            &Attributes::from([("state", Cow::Borrowed("limit"))]),
            Observation::U64Gauge(self.limit as u64),
        );

        reporter.report_observation(
            &Attributes::from([("state", Cow::Borrowed("reserved"))]),
            Observation::U64Gauge(pool_arc.reserved() as u64),
        );
        reporter.finish_metric();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
