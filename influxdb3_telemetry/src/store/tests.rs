use observability_deps::tracing::info;

use crate::store::to_2_decimal_places;

use super::*;

#[derive(Debug)]
struct SampleMetrics;

impl ParquetMetrics for SampleMetrics {
    fn get_metrics(&self) -> (u64, f64, u64) {
        (200, 500.25, 100)
    }
}

impl ProcessingEngineMetrics for SampleMetrics {
    fn num_triggers(&self) -> (u64, u64, u64, u64) {
        (150, 160, 200, 250)
    }
}

#[test_log::test(tokio::test)]
async fn test_telemetry_store() {
    // create store
    let parqet_file_metrics = Arc::new(SampleMetrics);
    let store: Arc<TelemetryStore> = TelemetryStore::new(CreateTelemetryStoreArgs {
        instance_id: Arc::from("some-instance-id"),
        os: Arc::from("Linux"),
        influx_version: Arc::from("Core-v3.0"),
        storage_type: Arc::from("Memory"),
        cores: 10,
        persisted_files: Some(parqet_file_metrics),
        telemetry_endpoint: "http://localhost/telemetry".to_owned(),
        catalog_uuid: "catalog_but_cluster_uuid".to_owned(),
        serve_invocation_method: ServeInvocationMethod::Tests,
        processing_engine_metrics: Arc::from(SampleMetrics) as Arc<dyn ProcessingEngineMetrics>,
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // check snapshot
    let snapshot = store.snapshot();
    assert_eq!("some-instance-id", &*snapshot.instance_id);
    assert_eq!(1, snapshot.uptime_secs);
    assert_eq!("catalog_but_cluster_uuid", &*snapshot.cluster_uuid);

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
    assert_eq!(150, snapshot.wal_single_triggers_count);
    assert_eq!(160, snapshot.wal_all_triggers_count);
    assert_eq!(200, snapshot.schedule_triggers_count);
    assert_eq!(250, snapshot.request_triggers_count);

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
