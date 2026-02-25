use crate::{ParquetMetrics, ProcessingEngineMetrics};

use super::*;

#[derive(Debug)]
struct MockMetrics;

impl ParquetMetrics for MockMetrics {
    fn get_metrics(&self) -> (u64, f64, u64) {
        (10, 20.0, 30)
    }
}

impl ProcessingEngineMetrics for MockMetrics {
    fn num_triggers(&self) -> (u64, u64, u64, u64) {
        (100, 110, 150, 200)
    }
}

#[test]
fn test_sample_all_metrics() {
    let mut mock_sys_info_provider = MockSystemInfoProvider::new();
    let store = TelemetryStore::new_without_background_runners(
        Some(Arc::from(MockMetrics)),
        Arc::from(MockMetrics) as Arc<dyn ProcessingEngineMetrics>,
    );

    mock_sys_info_provider
        .expect_get_pid()
        .return_const(Ok(Pid::from(5)));
    mock_sys_info_provider
        .expect_refresh_metrics()
        .return_const(());
    mock_sys_info_provider
        .expect_get_process_specific_metrics()
        .return_const(Some((10.0f32, 100u64)));

    let mut sampler = CpuAndMemorySampler::new(mock_sys_info_provider);

    sample_all_metrics(&mut sampler, &store);
}

#[test]
fn test_sample_all_metrics_with_call_failure() {
    let mut mock_sys_info_provider = MockSystemInfoProvider::new();
    let store = TelemetryStore::new_without_background_runners(
        Some(Arc::from(MockMetrics)),
        Arc::from(MockMetrics) as Arc<dyn ProcessingEngineMetrics>,
    );

    mock_sys_info_provider
        .expect_get_pid()
        .return_const(Ok(Pid::from(5)));
    mock_sys_info_provider
        .expect_refresh_metrics()
        .return_const(());
    mock_sys_info_provider
        .expect_get_process_specific_metrics()
        .return_const(None);

    let mut sampler = CpuAndMemorySampler::new(mock_sys_info_provider);

    sample_all_metrics(&mut sampler, &store);
}
