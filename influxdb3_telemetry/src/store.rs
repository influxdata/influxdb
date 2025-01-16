use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use num::Float;
use observability_deps::tracing::{debug, warn};

use crate::{
    bucket::EventsBucket,
    metrics::{Cpu, Memory, Queries, Writes},
    sampler::sample_metrics,
    sender::{send_telemetry_in_background, TelemetryPayload},
    ParquetMetrics,
};

/// This store is responsible for holding all the stats which will be sent in the background
/// to the server. There are primarily 4 different types of data held in the store:
///   - static info (like instance ids, OS etc): These are passed in to create telemetry store.
///   - hourly samples (like parquet file metrics): These are sampled at the point of creating
///     payload before sending to the server.
///   - rates (cpu/mem): These are sampled every minute but these are regular time
///     series data. These metrics are backed by [`crate::stats::Stats<T>`] type.
///   - events (reads/writes): These are just raw events and in order to convert it into a
///     time series, it is collected in a bucket first and then sampled at per minute interval.
///     These metrics are usually backed by [`crate::stats::RollingStats<T>`] type.
///     There are couple of metrics like number of writes/reads that is backed by just
///     [`crate::stats::Stats<T>`] type as they are just counters for per minute
#[derive(Debug)]
pub struct TelemetryStore {
    inner: parking_lot::Mutex<TelemetryStoreInner>,
    persisted_files: Option<Arc<dyn ParquetMetrics>>,
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
        persisted_files: Option<Arc<dyn ParquetMetrics>>,
        telemetry_endpoint: String,
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
            persisted_files,
        });

        if !cfg!(test) {
            sample_metrics(store.clone(), Duration::from_secs(SAMPLER_INTERVAL_SECS)).await;
            send_telemetry_in_background(
                telemetry_endpoint,
                store.clone(),
                Duration::from_secs(MAIN_SENDER_INTERVAL_SECS),
            )
            .await;
        }
        store
    }

    pub fn new_without_background_runners(
        persisted_files: Option<Arc<dyn ParquetMetrics>>,
    ) -> Arc<Self> {
        let instance_id = Arc::from("sample-instance-id");
        let os = Arc::from("Linux");
        let influx_version = Arc::from("influxdb3-0.1.0");
        let storage_type = Arc::from("Memory");
        let cores = 10;
        let inner = TelemetryStoreInner::new(instance_id, os, influx_version, storage_type, cores);
        Arc::new(TelemetryStore {
            inner: parking_lot::Mutex::new(inner),
            persisted_files,
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
                inner_store.reset_metrics_1h();
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

    pub(crate) fn rollup_events_1m(&self) {
        let mut inner_store = self.inner.lock();
        inner_store.rollup_reads_and_writes_1m();
    }

    pub(crate) fn reset_metrics_1h(&self) {
        let mut inner_store = self.inner.lock();
        inner_store.reset_metrics_1h();
    }

    pub(crate) fn snapshot(&self) -> TelemetryPayload {
        let inner_store = self.inner.lock();
        let mut payload = inner_store.snapshot();
        if let Some(persisted_files) = &self.persisted_files {
            let (file_count, size_mb, row_count) = persisted_files.get_metrics();
            payload.parquet_file_count = file_count;
            payload.parquet_file_size_mb = size_mb;
            payload.parquet_row_count = row_count;
        }
        payload
    }
}

#[derive(Debug)]
struct TelemetryStoreInner {
    instance_id: Arc<str>,
    os: Arc<str>,
    influx_version: Arc<str>,
    storage_type: Arc<str>,
    start_timer: Instant,
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
            start_timer: Instant::now(),
            cpu: Cpu::default(),
            memory: Memory::default(),
            per_minute_events_bucket: EventsBucket::new(),
            writes: Writes::default(),
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
            product_type: "Core",
            uptime_secs: self.start_timer.elapsed().as_secs(),

            cpu_utilization_percent_min_1m: self.cpu.utilization.min,
            cpu_utilization_percent_max_1m: self.cpu.utilization.max,
            cpu_utilization_percent_avg_1m: to_2_decimal_places(self.cpu.utilization.avg),

            memory_used_mb_min_1m: self.memory.usage.min,
            memory_used_mb_max_1m: self.memory.usage.max,
            memory_used_mb_avg_1m: self.memory.usage.avg,

            write_requests_min_1m: self.writes.num_writes.min,
            write_requests_max_1m: self.writes.num_writes.max,
            write_requests_avg_1m: self.writes.num_writes.avg,

            write_lines_min_1m: self.writes.lines.min,
            write_lines_max_1m: self.writes.lines.max,
            write_lines_avg_1m: self.writes.lines.avg,

            // if writes are very low, less than 1 MB it rounds it to 0
            // TODO: Maybe this should be float to hold less than 1 MB or
            //       leave it as 0 if it does not matter.
            write_mb_min_1m: to_mega_bytes(self.writes.size_bytes.min),
            write_mb_max_1m: to_mega_bytes(self.writes.size_bytes.max),
            write_mb_avg_1m: to_mega_bytes(self.writes.size_bytes.avg),

            query_requests_min_1m: self.reads.num_queries.min,
            query_requests_max_1m: self.reads.num_queries.max,
            query_requests_avg_1m: self.reads.num_queries.avg,

            // hmmm. will be nice to pass persisted file in?
            parquet_file_count: 0,
            parquet_file_size_mb: 0.0,
            parquet_row_count: 0,

            // sums over hour
            write_requests_sum_1h: self.writes.total_num_writes,
            write_lines_sum_1h: self.writes.total_lines,
            write_mb_sum_1h: to_mega_bytes(self.writes.total_size_bytes),
            query_requests_sum_1h: self.reads.total_num_queries,
        }
    }

    pub(crate) fn rollup_reads_and_writes_1m(&mut self) {
        debug!(
            events_summary = ?self.per_minute_events_bucket,
            "Rolling up writes/reads"
        );
        self.rollup_writes_1m();
        self.rollup_reads_1m();
        self.per_minute_events_bucket.reset();
    }

    fn rollup_writes_1m(&mut self) {
        let events_summary = &self.per_minute_events_bucket;
        self.writes.add_sample(events_summary);
    }

    fn rollup_reads_1m(&mut self) {
        let events_summary = &self.per_minute_events_bucket;
        self.reads.add_sample(events_summary);
    }

    fn reset_metrics_1h(&mut self) {
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

    #[derive(Debug)]
    struct SampleParquetMetrics;

    impl ParquetMetrics for SampleParquetMetrics {
        fn get_metrics(&self) -> (u64, f64, u64) {
            (200, 500.25, 100)
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_telemetry_store_cpu_mem() {
        // create store
        let parqet_file_metrics = Arc::new(SampleParquetMetrics);
        let store: Arc<TelemetryStore> = TelemetryStore::new(
            Arc::from("some-instance-id"),
            Arc::from("Linux"),
            Arc::from("Core-v3.0"),
            Arc::from("Memory"),
            10,
            Some(parqet_file_metrics),
            "http://localhost/telemetry".to_string(),
        )
        .await;
        tokio::time::sleep(Duration::from_secs(1)).await;

        // check snapshot
        let snapshot = store.snapshot();
        assert_eq!("some-instance-id", &*snapshot.instance_id);
        assert_eq!(1, snapshot.uptime_secs);

        // add cpu/mem and snapshot 1
        let mem_used_bytes = 123456789;
        let expected_mem_in_mb = 117;
        store.add_cpu_and_memory(89.0, mem_used_bytes);
        let snapshot = store.snapshot();
        info!(snapshot = ?snapshot, "sample snapshot 1");
        assert_eq!(89.0, snapshot.cpu_utilization_percent_min_1m);
        assert_eq!(89.0, snapshot.cpu_utilization_percent_max_1m);
        assert_eq!(89.0, snapshot.cpu_utilization_percent_avg_1m);
        assert_eq!(expected_mem_in_mb, snapshot.memory_used_mb_min_1m);
        assert_eq!(expected_mem_in_mb, snapshot.memory_used_mb_max_1m);
        assert_eq!(expected_mem_in_mb, snapshot.memory_used_mb_avg_1m);

        // add cpu/mem snapshot 2
        store.add_cpu_and_memory(100.0, 134567890);
        let snapshot = store.snapshot();
        info!(snapshot = ?snapshot, "sample snapshot 2");
        assert_eq!(89.0, snapshot.cpu_utilization_percent_min_1m);
        assert_eq!(100.0, snapshot.cpu_utilization_percent_max_1m);
        assert_eq!(94.5, snapshot.cpu_utilization_percent_avg_1m);
        assert_eq!(expected_mem_in_mb, snapshot.memory_used_mb_min_1m);
        assert_eq!(128, snapshot.memory_used_mb_max_1m);
        assert_eq!(122, snapshot.memory_used_mb_avg_1m);
        assert_eq!(200, snapshot.parquet_file_count);
        assert_eq!(500.25, snapshot.parquet_file_size_mb);
        assert_eq!(100, snapshot.parquet_row_count);

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
        store.rollup_events_1m();
        let snapshot = store.snapshot();
        info!(
            snapshot = ?snapshot,
            "After rolling up reads/writes"
        );

        assert_eq!(1, snapshot.write_lines_min_1m);
        assert_eq!(120, snapshot.write_lines_max_1m);
        assert_eq!(56, snapshot.write_lines_avg_1m);
        assert_eq!(222, snapshot.write_lines_sum_1h);

        assert_eq!(0, snapshot.write_mb_min_1m);
        assert_eq!(0, snapshot.write_mb_max_1m);
        assert_eq!(0, snapshot.write_mb_avg_1m);
        assert_eq!(0, snapshot.write_mb_sum_1h);

        assert_eq!(3, snapshot.query_requests_min_1m);
        assert_eq!(3, snapshot.query_requests_max_1m);
        assert_eq!(3, snapshot.query_requests_avg_1m);
        assert_eq!(3, snapshot.query_requests_sum_1h);

        // add more writes after rollup
        store.add_write_metrics(100, 101_024_000);
        store.add_write_metrics(120, 107_000);
        store.add_write_metrics(1, 100_000_000);
        store.add_write_metrics(1, 200_000_000);

        // add more reads after rollup
        store.update_num_queries();
        store.update_num_queries();

        store.rollup_events_1m();
        let snapshot = store.snapshot();
        info!(
            snapshot = ?snapshot,
            "After rolling up reads/writes 2nd time"
        );
        assert_eq!(1, snapshot.write_lines_min_1m);
        assert_eq!(120, snapshot.write_lines_max_1m);
        assert_eq!(56, snapshot.write_lines_avg_1m);
        assert_eq!(444, snapshot.write_lines_sum_1h);

        assert_eq!(0, snapshot.write_mb_min_1m);
        assert_eq!(200, snapshot.write_mb_max_1m);
        assert_eq!(50, snapshot.write_mb_avg_1m);
        assert_eq!(401, snapshot.write_mb_sum_1h);

        assert_eq!(2, snapshot.query_requests_min_1m);
        assert_eq!(3, snapshot.query_requests_max_1m);
        assert_eq!(3, snapshot.query_requests_avg_1m);
        assert_eq!(5, snapshot.query_requests_sum_1h);

        // reset
        store.reset_metrics_1h();
        // check snapshot 3
        let snapshot = store.snapshot();
        info!(snapshot = ?snapshot, "sample snapshot 3");
        assert_eq!(0.0, snapshot.cpu_utilization_percent_min_1m);
        assert_eq!(0.0, snapshot.cpu_utilization_percent_max_1m);
        assert_eq!(0.0, snapshot.cpu_utilization_percent_avg_1m);
        assert_eq!(0, snapshot.memory_used_mb_min_1m);
        assert_eq!(0, snapshot.memory_used_mb_max_1m);
        assert_eq!(0, snapshot.memory_used_mb_avg_1m);

        assert_eq!(0, snapshot.write_lines_min_1m);
        assert_eq!(0, snapshot.write_lines_max_1m);
        assert_eq!(0, snapshot.write_lines_avg_1m);
        assert_eq!(0, snapshot.write_lines_sum_1h);

        assert_eq!(0, snapshot.write_mb_min_1m);
        assert_eq!(0, snapshot.write_mb_max_1m);
        assert_eq!(0, snapshot.write_mb_avg_1m);
        assert_eq!(0, snapshot.write_mb_sum_1h);

        assert_eq!(0, snapshot.query_requests_min_1m);
        assert_eq!(0, snapshot.query_requests_max_1m);
        assert_eq!(0, snapshot.query_requests_avg_1m);
        assert_eq!(0, snapshot.query_requests_sum_1h);
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
