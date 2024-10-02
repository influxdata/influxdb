use std::{sync::Arc, time::Duration};

use num::Float;
use observability_deps::tracing::{debug, warn};

use crate::{
    bucket::EventsBucket,
    metrics::{Cpu, Memory, Queries, Writes},
    sampler::sample_metrics,
    sender::{send_telemetry_in_background, TelemetryPayload},
};

/// This store is responsible for holding all the stats which
/// will be sent in the background to the server.
#[derive(Debug)]
pub struct TelemetryStore {
    inner: parking_lot::Mutex<TelemetryStoreInner>,
}

const SAMPLER_INTERVAL_SECS: u64 = 60;
const MAIN_SENDER_INTERVAL_SECS: u64 = 60 * 60;

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

        if !cfg!(test) {
            sample_metrics(store.clone(), Duration::from_secs(SAMPLER_INTERVAL_SECS)).await;
            send_telemetry_in_background(
                store.clone(),
                Duration::from_secs(MAIN_SENDER_INTERVAL_SECS),
            )
            .await;
        }
        store
    }

    pub fn new_without_background_runners() -> Arc<Self> {
        let instance_id = Arc::from("dummy-instance-id");
        let os = Arc::from("Linux");
        let influx_version = Arc::from("influxdb3-0.1.0");
        let storage_type = Arc::from("Memory");
        let cores = 10;
        let inner = TelemetryStoreInner::new(instance_id, os, influx_version, storage_type, cores);
        Arc::new(TelemetryStore {
            inner: parking_lot::Mutex::new(inner),
        })
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

    pub fn add_write_metrics(&self, num_lines: usize, write_bytes: usize) {
        let mut inner_store = self.inner.lock();
        inner_store
            .per_minute_events_bucket
            .add_write_sample(num_lines, write_bytes);
    }

    pub fn update_num_queries(&self) {
        let mut inner_store = self.inner.lock();
        inner_store.per_minute_events_bucket.update_num_queries();
    }

    pub(crate) fn rollup_events(&self) {
        let mut inner_store = self.inner.lock();
        inner_store.rollup_reads_and_writes();
    }

    pub(crate) fn reset_metrics(&self) {
        let mut inner_store = self.inner.lock();
        inner_store.reset_metrics();
    }

    pub(crate) fn snapshot(&self) -> TelemetryPayload {
        let inner_store = self.inner.lock();
        inner_store.snapshot()
    }
}

#[derive(Debug)]
struct TelemetryStoreInner {
    instance_id: Arc<str>,
    os: Arc<str>,
    influx_version: Arc<str>,
    storage_type: Arc<str>,
    cores: usize,
    cpu: Cpu,
    memory: Memory,
    // Both write/read events are captured in this bucket
    // and then later rolledup / summarized in
    // the fields below at 1 minute intervals
    // This bucket internally holds per minute
    // metrics and aggregates it before the sampler
    // picks up this 1 minute summary and add it to 1 hour
    // summary and then resets it
    per_minute_events_bucket: EventsBucket,
    // write metrics for 1 hr
    writes: Writes,
    // read metrics for 1 hr
    reads: Queries,
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
            cpu: Cpu::default(),
            // mem
            memory: Memory::default(),
            per_minute_events_bucket: EventsBucket::new(),
            // writes
            writes: Writes::default(),
            // reads
            reads: Queries::default(),
        }
    }

    pub(crate) fn snapshot(&self) -> TelemetryPayload {
        debug!(
            min = ?self.writes.size_bytes.min,
            max = ?self.writes.size_bytes.max,
            avg = ?self.writes.size_bytes.avg,
            "Snapshot write size in bytes"
        );
        TelemetryPayload {
            os: self.os.clone(),
            version: self.influx_version.clone(),
            instance_id: self.instance_id.clone(),
            storage_type: self.storage_type.clone(),
            cores: self.cores,
            product_type: "OSS",

            cpu_utilization_percent_min: self.cpu.utilization.min,
            cpu_utilization_percent_max: self.cpu.utilization.max,
            cpu_utilization_percent_avg: to_2_decimal_places(self.cpu.utilization.avg),

            memory_used_mb_min: self.memory.usage.min,
            memory_used_mb_max: self.memory.usage.max,
            memory_used_mb_avg: self.memory.usage.avg,

            write_requests_min: self.writes.num_writes.min,
            write_requests_max: self.writes.num_writes.max,
            write_requests_avg: self.writes.num_writes.avg,

            write_lines_min: self.writes.lines.min,
            write_lines_max: self.writes.lines.max,
            write_lines_avg: self.writes.lines.avg,

            // if writes are very low, less than 1 MB it rounds it to 0
            // TODO: Maybe this should be float to hold less than 1 MB or
            //       leave it as 0 if it does not matter.
            write_mb_min: to_mega_bytes(self.writes.size_bytes.min),
            write_mb_max: to_mega_bytes(self.writes.size_bytes.max),
            write_mb_avg: to_mega_bytes(self.writes.size_bytes.avg),

            query_requests_min: self.reads.num_queries.min,
            query_requests_max: self.reads.num_queries.max,
            query_requests_avg: self.reads.num_queries.avg,
        }
    }

    pub(crate) fn rollup_reads_and_writes(&mut self) {
        debug!(
            events_summary = ?self.per_minute_events_bucket,
            "Rolling up writes/reads"
        );
        self.rollup_writes();
        self.rollup_reads();
        self.per_minute_events_bucket.reset();
    }

    fn rollup_writes(&mut self) {
        let events_summary = &self.per_minute_events_bucket;
        self.writes.add_sample(events_summary);
    }

    fn rollup_reads(&mut self) {
        let events_summary = &self.per_minute_events_bucket;
        self.reads.add_sample(events_summary);
    }

    fn reset_metrics(&mut self) {
        self.cpu.reset();
        self.memory.reset();
        self.writes.reset();
        self.reads.reset();
    }

    fn add_cpu_and_memory(&mut self, cpu: f32, memory: u64) -> Option<()> {
        self.add_cpu_utilization(cpu)?;
        self.add_memory(memory)?;
        Some(())
    }

    fn add_memory(&mut self, value: u64) -> Option<()> {
        // convert to MB
        let mem_used_mb = value / (1024 * 1024);
        self.memory.add_sample(mem_used_mb)
    }

    fn add_cpu_utilization(&mut self, cpu_used: f32) -> Option<()> {
        self.cpu.add_sample(cpu_used)
    }
}

fn to_mega_bytes(size_bytes: u64) -> u64 {
    size_bytes / (1_000 * 1_000)
}

fn to_2_decimal_places<T: Float>(avg: T) -> T {
    round_to_decimal_places(avg, 2)
}

fn round_to_decimal_places<T: Float>(avg: T, num_places: u8) -> T {
    let factor = if num_places == 4 { 10_000.0 } else { 100.0 };
    let factor_as_float = num::cast(factor).unwrap();
    (avg * factor_as_float).round() / factor_as_float
}

#[cfg(test)]
mod tests {
    use observability_deps::tracing::info;

    use crate::store::to_2_decimal_places;

    use super::*;

    #[test_log::test(tokio::test)]
    async fn test_telemetry_store_cpu_mem() {
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

        // add some writes
        store.add_write_metrics(100, 100);
        store.add_write_metrics(120, 120);
        store.add_write_metrics(1, 20);
        store.add_write_metrics(1, 22);

        // and reads
        store.update_num_queries();
        store.update_num_queries();
        store.update_num_queries();

        // now rollup reads/writes
        store.rollup_events();
        let snapshot = store.snapshot();
        info!(
            snapshot = ?snapshot,
            "After rolling up reads/writes"
        );

        assert_eq!(1, snapshot.write_lines_min);
        assert_eq!(120, snapshot.write_lines_max);
        assert_eq!(56, snapshot.write_lines_avg);

        assert_eq!(0, snapshot.write_mb_min);
        assert_eq!(0, snapshot.write_mb_max);
        assert_eq!(0, snapshot.write_mb_avg);

        assert_eq!(3, snapshot.query_requests_min);
        assert_eq!(3, snapshot.query_requests_max);
        assert_eq!(3, snapshot.query_requests_avg);

        // add more writes after rollup
        store.add_write_metrics(100, 101_024_000);
        store.add_write_metrics(120, 107_000);
        store.add_write_metrics(1, 100_000_000);
        store.add_write_metrics(1, 200_000_000);

        // add more reads after rollup
        store.update_num_queries();
        store.update_num_queries();

        store.rollup_events();
        let snapshot = store.snapshot();
        info!(
            snapshot = ?snapshot,
            "After rolling up reads/writes 2nd time"
        );
        assert_eq!(1, snapshot.write_lines_min);
        assert_eq!(120, snapshot.write_lines_max);
        assert_eq!(56, snapshot.write_lines_avg);

        assert_eq!(0, snapshot.write_mb_min);
        assert_eq!(200, snapshot.write_mb_max);
        assert_eq!(50, snapshot.write_mb_avg);

        assert_eq!(2, snapshot.query_requests_min);
        assert_eq!(3, snapshot.query_requests_max);
        assert_eq!(3, snapshot.query_requests_avg);

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

    #[test]
    fn test_to_4_decimal_places() {
        let x = 25.486842105263158;
        let rounded = round_to_decimal_places(x, 4);
        assert_eq!(25.4868, rounded);
    }
}
