use std::{sync::atomic::AtomicU64, sync::atomic::Ordering};

// Various statistics for execution
#[derive(Debug, Default)]
pub struct ExecutionCounters {
    pub plans_run: AtomicU64,
}

impl ExecutionCounters {
    pub fn inc_plans_run(&self) {
        self.plans_run.fetch_add(1, Ordering::Relaxed);
    }
}
