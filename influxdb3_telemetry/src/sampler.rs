use std::{sync::Arc, time::Duration};

use observability_deps::tracing::debug;
use sysinfo::{ProcessRefreshKind, System};

use crate::Result;
use crate::{store::TelemetryStore, TelemetryError};

struct CpuAndMemorySampler {
    system: System,
}

impl CpuAndMemorySampler {
    pub fn new(system: System) -> Self {
        Self { system }
    }

    pub fn get_cpu_and_mem_used(&mut self) -> Result<(f32, u64)> {
        let pid = sysinfo::get_current_pid().map_err(TelemetryError::CannotGetPid)?;
        self.system.refresh_pids_specifics(
            &[pid],
            ProcessRefreshKind::new()
                .with_cpu()
                .with_memory()
                .with_disk_usage(),
        );

        let process = self
            .system
            .process(pid)
            .unwrap_or_else(|| panic!("cannot get process with pid: {}", pid));

        let memory_used = process.memory();
        let cpu_used = process.cpu_usage();

        debug!(
            mem_used = ?memory_used,
            cpu_used = ?cpu_used,
            "trying to sample data for cpu/memory");

        Ok((cpu_used, memory_used))
    }
}

pub(crate) async fn sample_metrics(
    store: Arc<TelemetryStore>,
    duration_secs: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut sampler = CpuAndMemorySampler::new(System::new());

        // sample every minute
        let mut interval = tokio::time::interval(duration_secs);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            if let Ok((cpu_used, memory_used)) = sampler.get_cpu_and_mem_used() {
                store.add_cpu_and_memory(cpu_used, memory_used);
                store.rollup_events();
            }
        }
    })
}
