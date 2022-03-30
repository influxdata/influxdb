use std::any::Any;

use tikv_jemalloc_ctl::{epoch, stats};

use metric::{Attributes, MetricKind, Observation, Reporter};

/// A `metric::Instrument` that reports jemalloc memory statistics, specifically:
///
/// - a u64 gauge called "jemalloc_memstats_bytes"
#[derive(Debug, Clone)]
pub struct JemallocMetrics {
    active: Attributes,
    alloc: Attributes,
    metadata: Attributes,
    mapped: Attributes,
    resident: Attributes,
    retained: Attributes,
}

impl JemallocMetrics {
    pub fn new() -> Self {
        Self {
            active: Attributes::from(&[("stat", "active")]),
            alloc: Attributes::from(&[("stat", "alloc")]),
            metadata: Attributes::from(&[("stat", "metadata")]),
            mapped: Attributes::from(&[("stat", "mapped")]),
            resident: Attributes::from(&[("stat", "resident")]),
            retained: Attributes::from(&[("stat", "retained")]),
        }
    }
}

impl metric::Instrument for JemallocMetrics {
    fn report(&self, reporter: &mut dyn Reporter) {
        reporter.start_metric(
            "jemalloc_memstats_bytes",
            "jemalloc metrics - http://jemalloc.net/jemalloc.3.html#stats.active",
            MetricKind::U64Gauge,
        );

        epoch::advance().unwrap();

        reporter.report_observation(
            &self.active,
            Observation::U64Gauge(stats::active::read().unwrap() as u64),
        );

        reporter.report_observation(
            &self.alloc,
            Observation::U64Gauge(stats::allocated::read().unwrap() as u64),
        );

        reporter.report_observation(
            &self.metadata,
            Observation::U64Gauge(stats::metadata::read().unwrap() as u64),
        );

        reporter.report_observation(
            &self.mapped,
            Observation::U64Gauge(stats::mapped::read().unwrap() as u64),
        );

        reporter.report_observation(
            &self.resident,
            Observation::U64Gauge(stats::resident::read().unwrap() as u64),
        );

        reporter.report_observation(
            &self.retained,
            Observation::U64Gauge(stats::retained::read().unwrap() as u64),
        );

        reporter.finish_metric();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
