use std::sync::Arc;

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, UnboundedMemoryPool};
use metric::{Instrument, Observation, RawReporter};

use super::DataFusionMemoryPoolMetricsBridge;

const LIMIT: usize = 1024;

fn pool() -> Arc<dyn MemoryPool> {
    Arc::new(UnboundedMemoryPool::default())
}

#[test]
fn report_skips_dropped_pool() {
    let pool = pool();
    let bridge = DataFusionMemoryPoolMetricsBridge::new(&pool, LIMIT);
    drop(pool);

    let mut reporter = RawReporter::default();
    bridge.report(&mut reporter);

    assert!(reporter.observations().is_empty());
}

#[test]
fn report_live_pool() {
    let pool = pool();
    let bridge = DataFusionMemoryPoolMetricsBridge::new(&pool, LIMIT);
    let mut reservation = MemoryConsumer::new("test").register(&pool);
    reservation.try_grow(100).unwrap();

    let mut reporter = RawReporter::default();
    bridge.report(&mut reporter);

    let metric = reporter
        .metric("datafusion_mem_pool_bytes")
        .expect("metric reported");
    assert_eq!(
        metric.observation(&[("state", "limit")]),
        Some(&Observation::U64Gauge(LIMIT as u64))
    );
    assert_eq!(
        metric.observation(&[("state", "reserved")]),
        Some(&Observation::U64Gauge(100))
    );
}
