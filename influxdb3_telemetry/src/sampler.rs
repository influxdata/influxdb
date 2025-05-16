use std::{sync::Arc, time::Duration};

#[cfg(test)]
use mockall::{automock, predicate::*};
use observability_deps::tracing::debug;
use sysinfo::{Pid, ProcessRefreshKind, System};

use crate::Result;
use crate::store::TelemetryStore;

#[cfg_attr(test, automock)]
pub(crate) trait SystemInfoProvider: Send + Sync + 'static {
    fn refresh_metrics(&mut self, pid: Pid);

    fn get_pid(&self) -> Result<Pid, &'static str>;

    fn get_process_specific_metrics(&self, pid: Pid) -> Option<(f32, u64)>;
}

struct SystemInfo {
    system: System,
}

impl SystemInfo {
    pub(crate) fn new() -> SystemInfo {
        Self {
            system: System::new(),
        }
    }
}

impl SystemInfoProvider for SystemInfo {
    /// This method picks the memory and cpu usage for this process using the
    /// pid.
    fn refresh_metrics(&mut self, pid: Pid) {
        self.system.refresh_pids_specifics(
            &[pid],
            ProcessRefreshKind::new()
                .with_cpu()
                .with_memory()
                .with_disk_usage(),
        );
    }

    fn get_pid(&self) -> Result<Pid, &'static str> {
        sysinfo::get_current_pid()
    }

    fn get_process_specific_metrics<'a>(&self, pid: Pid) -> Option<(f32, u64)> {
        let process = self.system.process(pid)?;

        let cpu_used = process.cpu_usage();
        let memory_used = process.memory();
        Some((cpu_used, memory_used))
    }
}

struct CpuAndMemorySampler {
    system: Box<dyn SystemInfoProvider>,
}

impl CpuAndMemorySampler {
    pub(crate) fn new(system: impl SystemInfoProvider) -> Self {
        Self {
            system: Box::new(system),
        }
    }

    pub(crate) fn get_cpu_and_mem_used(&mut self) -> Option<(f32, u64)> {
        let pid = self.system.get_pid().ok()?;
        self.system.refresh_metrics(pid);
        let (cpu_used, memory_used) = self.system.get_process_specific_metrics(pid)?;
        debug!(
            cpu_used = ?cpu_used,
            mem_used = ?memory_used,
            "trying to sample data for cpu/memory");

        Some((cpu_used, memory_used))
    }
}

pub(crate) async fn sample_metrics(
    store: Arc<TelemetryStore>,
    duration_secs: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut sampler = CpuAndMemorySampler::new(SystemInfo::new());

        // sample every minute
        let mut interval = tokio::time::interval(duration_secs);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            sample_all_metrics(&mut sampler, &store);
        }
    })
}

fn sample_all_metrics(sampler: &mut CpuAndMemorySampler, store: &Arc<TelemetryStore>) {
    if let Some((cpu_used, memory_used)) = sampler.get_cpu_and_mem_used() {
        store.add_cpu_and_memory(cpu_used, memory_used);
    } else {
        debug!("Cannot get cpu/mem usage stats for this process");
    }
    store.rollup_events_1m();
}

#[cfg(test)]
mod tests {

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
        fn num_triggers(&self) -> u64 {
            100
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
}
