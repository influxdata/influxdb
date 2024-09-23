use std::{sync::Arc, time::Duration};

use observability_deps::tracing::{debug, warn};
use serde::Serialize;

use crate::{
    cpu_mem_sampler::sample_cpu_and_memory, sender::send_telemetry_in_background, stats::stats,
};

/// This store is responsible for holding all the stats which
/// will be sent in the background to the server.
pub struct TelemetryStore {
    inner: parking_lot::Mutex<TelemetryStoreInner>,
}

const ONE_MIN_SECS: u64 = 60;
const ONE_HOUR_SECS: u64 = 60 * 60;

impl TelemetryStore {
    pub async fn new(
        instance_id: Arc<str>,
        os: Arc<str>,
        influx_version: Arc<str>,
        storage_type: Arc<str>,
        cores: usize,
    ) -> Arc<Self> {
        debug!(
            instance_id = ?instance_id,
            os = ?os,
            influx_version = ?influx_version,
            storage_type = ?storage_type,
            cores = ?cores,
            "Initializing telemetry store"
        );
        let inner = TelemetryStoreInner::new(instance_id, os, influx_version, storage_type, cores);
        let store = Arc::new(TelemetryStore {
            inner: parking_lot::Mutex::new(inner),
        });
        sample_cpu_and_memory(store.clone(), Duration::from_secs(ONE_MIN_SECS)).await;
        send_telemetry_in_background(store.clone(), Duration::from_secs(ONE_HOUR_SECS)).await;
        store
    }

    pub fn add_cpu_and_memory(&self, cpu: f32, memory: u64) {
        let mut inner_store = self.inner.lock();
        inner_store
            .add_cpu_and_memory(cpu, memory)
            .unwrap_or_else(|| {
                // num::cast probably has had overflow. Best to
                // reset all metrics to start again
                warn!("cpu/memory could not be added, resetting metrics");
                inner_store.reset_metrics();
            });
    }

    pub fn reset_metrics(&self) {
        let mut inner_store = self.inner.lock();
        inner_store.reset_metrics();
    }

    pub(crate) fn snapshot(&self) -> TelemetryPayload {
        let inner_store = self.inner.lock();
        inner_store.snapshot()
    }
}

struct TelemetryStoreInner {
    instance_id: Arc<str>,
    os: Arc<str>,
    influx_version: Arc<str>,
    storage_type: Arc<str>,
    cores: usize,

    cpu_utilization_percent_min: f64,
    cpu_utilization_percent_max: f64,
    cpu_utilization_percent_avg: f64,

    memory_used_mb_min: u64,
    memory_used_mb_max: u64,
    memory_used_mb_avg: u64,

    num_samples_cpu_mem: u64,
}

impl TelemetryStoreInner {
    pub fn new(
        instance_id: Arc<str>,
        os: Arc<str>,
        influx_version: Arc<str>,
        storage_type: Arc<str>,
        cores: usize,
    ) -> Self {
        TelemetryStoreInner {
            os,
            instance_id,
            influx_version,
            storage_type,
            cores,

            // cpu
            cpu_utilization_percent_min: 0.0,
            cpu_utilization_percent_max: 0.0,
            cpu_utilization_percent_avg: 0.0,

            // mem
            memory_used_mb_min: 0,
            memory_used_mb_max: 0,
            memory_used_mb_avg: 0,

            num_samples_cpu_mem: 0,
        }
    }

    pub(crate) fn snapshot(&self) -> TelemetryPayload {
        TelemetryPayload {
            os: self.os.clone(),
            version: self.influx_version.clone(),
            instance_id: self.instance_id.clone(),
            storage_type: self.storage_type.clone(),
            cores: self.cores,
            product_type: "OSS",

            cpu_utilization_percent_min: self.cpu_utilization_percent_min,
            cpu_utilization_percent_max: self.cpu_utilization_percent_max,
            cpu_utilization_percent_avg: self.cpu_utilization_percent_avg,

            memory_used_mb_min: self.memory_used_mb_min,
            memory_used_mb_max: self.memory_used_mb_max,
            memory_used_mb_avg: self.memory_used_mb_avg,
        }
    }

    fn reset_metrics(&mut self) {
        self.cpu_utilization_percent_min = 0.0;
        self.cpu_utilization_percent_max = 0.0;
        self.cpu_utilization_percent_avg = 0.0;

        self.memory_used_mb_min = 0;
        self.memory_used_mb_max = 0;
        self.memory_used_mb_avg = 0;

        self.num_samples_cpu_mem = 0;
    }

    fn add_cpu_and_memory(&mut self, cpu: f32, memory: u64) -> Option<()> {
        self.add_cpu_utilization(cpu)?;
        self.add_memory(memory)?;
        self.num_samples_cpu_mem += 1;
        Some(())
    }

    fn add_memory(&mut self, value: u64) -> Option<()> {
        // convert to MB
        let mem_used_mb = value / (1024 * 1024);
        let (min, max, avg) = if self.num_samples_cpu_mem == 0 {
            (mem_used_mb, mem_used_mb, mem_used_mb)
        } else {
            stats(
                self.memory_used_mb_min,
                self.memory_used_mb_max,
                self.memory_used_mb_avg,
                self.num_samples_cpu_mem,
                mem_used_mb,
            )?
        };
        self.memory_used_mb_min = min;
        self.memory_used_mb_max = max;
        self.memory_used_mb_avg = avg;
        Some(())
    }

    fn add_cpu_utilization(&mut self, value: f32) -> Option<()> {
        let cpu_used: f64 = value.into();
        let (min, max, avg) = if self.num_samples_cpu_mem == 0 {
            (cpu_used, cpu_used, cpu_used)
        } else {
            stats(
                self.cpu_utilization_percent_min,
                self.cpu_utilization_percent_max,
                self.cpu_utilization_percent_avg,
                self.num_samples_cpu_mem,
                cpu_used,
            )?
        };
        self.cpu_utilization_percent_min = min;
        self.cpu_utilization_percent_max = max;
        self.cpu_utilization_percent_avg = to_2_decimal_places(avg);
        Some(())
    }
}

fn to_2_decimal_places(avg: f64) -> f64 {
    (avg * 100.0).round() / 100.0
}

#[derive(Serialize, Debug)]
pub(crate) struct TelemetryPayload {
    pub os: Arc<str>,
    pub version: Arc<str>,
    pub storage_type: Arc<str>,
    pub instance_id: Arc<str>,
    pub cores: usize,
    pub product_type: &'static str,
    // cpu
    pub cpu_utilization_percent_min: f64,
    pub cpu_utilization_percent_max: f64,
    pub cpu_utilization_percent_avg: f64,
    // mem
    pub memory_used_mb_min: u64,
    pub memory_used_mb_max: u64,
    pub memory_used_mb_avg: u64,
}

#[cfg(test)]
mod tests {
    use observability_deps::tracing::info;

    use crate::store::to_2_decimal_places;

    use super::*;

    #[test_log::test(tokio::test)]
    async fn test_telemetry_handle_creation() {
        // create store
        let store: Arc<TelemetryStore> = TelemetryStore::new(
            Arc::from("some-instance-id"),
            Arc::from("Linux"),
            Arc::from("OSS-v3.0"),
            Arc::from("Memory"),
            10,
        )
        .await;
        // check snapshot
        let snapshot = store.snapshot();
        assert_eq!("some-instance-id", &*snapshot.instance_id);

        // add cpu/mem and snapshot 1
        let mem_used_bytes = 123456789;
        let expected_mem_in_mb = 117;
        store.add_cpu_and_memory(89.0, mem_used_bytes);
        let snapshot = store.snapshot();
        info!(snapshot = ?snapshot, "dummy snapshot 1");
        assert_eq!(89.0, snapshot.cpu_utilization_percent_min);
        assert_eq!(89.0, snapshot.cpu_utilization_percent_max);
        assert_eq!(89.0, snapshot.cpu_utilization_percent_avg);
        assert_eq!(expected_mem_in_mb, snapshot.memory_used_mb_min);
        assert_eq!(expected_mem_in_mb, snapshot.memory_used_mb_max);
        assert_eq!(expected_mem_in_mb, snapshot.memory_used_mb_avg);

        // add cpu/mem snapshot 2
        store.add_cpu_and_memory(100.0, 134567890);
        let snapshot = store.snapshot();
        info!(snapshot = ?snapshot, "dummy snapshot 2");
        assert_eq!(89.0, snapshot.cpu_utilization_percent_min);
        assert_eq!(100.0, snapshot.cpu_utilization_percent_max);
        assert_eq!(94.5, snapshot.cpu_utilization_percent_avg);
        assert_eq!(expected_mem_in_mb, snapshot.memory_used_mb_min);
        assert_eq!(128, snapshot.memory_used_mb_max);
        assert_eq!(122, snapshot.memory_used_mb_avg);

        // reset
        store.reset_metrics();
        // check snapshot 3
        let snapshot = store.snapshot();
        info!(snapshot = ?snapshot, "dummy snapshot 3");
        assert_eq!(0.0, snapshot.cpu_utilization_percent_min);
        assert_eq!(0.0, snapshot.cpu_utilization_percent_max);
        assert_eq!(0.0, snapshot.cpu_utilization_percent_avg);
        assert_eq!(0, snapshot.memory_used_mb_min);
        assert_eq!(0, snapshot.memory_used_mb_max);
        assert_eq!(0, snapshot.memory_used_mb_avg);
    }

    #[test]
    fn test_to_2_decimal_places() {
        let x = 25.486842105263158;
        let rounded = to_2_decimal_places(x);
        assert_eq!(25.49, rounded);
    }
}
