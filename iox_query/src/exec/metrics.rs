use std::{
    borrow::Cow,
    sync::{Arc, Weak},
};

use datafusion::execution::memory_pool::MemoryPool;
use metric::{Attributes, Instrument, MetricKind, Observation, Reporter};
use parking_lot::Mutex;

/// Hooks DataFusion [`MemoryPool`] into our [`metric`] crate.
#[derive(Debug, Clone, Default)]
pub struct DataFusionMemoryPoolMetricsBridge {
    pools: Arc<Mutex<Vec<Pool>>>,
}

impl DataFusionMemoryPoolMetricsBridge {
    /// Register new pool.
    pub fn register_pool(&self, pool: &Arc<dyn MemoryPool>, limit: usize) {
        self.pools.lock().push(Pool {
            pool: Arc::downgrade(pool),
            limit,
        });
    }
}

impl Instrument for DataFusionMemoryPoolMetricsBridge {
    fn report(&self, reporter: &mut dyn Reporter) {
        reporter.start_metric(
            "datafusion_mem_pool_bytes",
            "Number of bytes within the datafusion memory pool",
            MetricKind::U64Gauge,
        );
        let pools = self.pools.lock();
        for (idx, pool) in pools.iter().enumerate() {
            let Some(pool_arc) = pool.pool.upgrade() else {
                continue;
            };

            reporter.report_observation(
                &Attributes::from([
                    ("pool_id", Cow::Owned(idx.to_string())),
                    ("state", Cow::Borrowed("limit")),
                ]),
                Observation::U64Gauge(pool.limit as u64),
            );

            reporter.report_observation(
                &Attributes::from([
                    ("pool_id", Cow::Owned(idx.to_string())),
                    ("state", Cow::Borrowed("reserved")),
                ]),
                Observation::U64Gauge(pool_arc.reserved() as u64),
            );
        }
        reporter.finish_metric();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
struct Pool {
    pool: Weak<dyn MemoryPool>,
    limit: usize,
}
