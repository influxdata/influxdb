use std::sync::Arc;

use async_trait::async_trait;
use compactor_scheduler::{CompactionJob, Scheduler};

use super::CompactionJobsSource;

#[derive(Debug)]
pub struct ScheduledCompactionJobsSource {
    scheduler: Arc<dyn Scheduler>,
}

impl ScheduledCompactionJobsSource {
    pub fn new(scheduler: Arc<dyn Scheduler>) -> Self {
        Self { scheduler }
    }
}

#[async_trait]
impl CompactionJobsSource for ScheduledCompactionJobsSource {
    async fn fetch(&self) -> Vec<CompactionJob> {
        self.scheduler.get_jobs().await
    }
}

impl std::fmt::Display for ScheduledCompactionJobsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "scheduled_compaction_jobs_source({})", self.scheduler)
    }
}

#[cfg(test)]
mod tests {
    use compactor_scheduler::create_test_scheduler;
    use iox_tests::TestCatalog;
    use iox_time::{MockProvider, Time};

    use super::*;

    #[test]
    fn test_display() {
        let scheduler = create_test_scheduler(
            TestCatalog::new().catalog(),
            Arc::new(MockProvider::new(Time::MIN)),
            None,
        );
        let source = ScheduledCompactionJobsSource { scheduler };

        assert_eq!(
            source.to_string(),
            "scheduled_compaction_jobs_source(local_compaction_scheduler)",
        );
    }
}
