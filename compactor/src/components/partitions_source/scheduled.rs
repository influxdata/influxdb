use std::sync::Arc;

use async_trait::async_trait;
use compactor_scheduler::{CompactionJob, Scheduler};

use super::PartitionsSource;

#[derive(Debug)]
pub struct ScheduledPartitionsSource {
    scheduler: Arc<dyn Scheduler>,
}

impl ScheduledPartitionsSource {
    pub fn new(scheduler: Arc<dyn Scheduler>) -> Self {
        Self { scheduler }
    }
}

#[async_trait]
impl PartitionsSource for ScheduledPartitionsSource {
    async fn fetch(&self) -> Vec<CompactionJob> {
        self.scheduler.get_jobs().await
    }
}

impl std::fmt::Display for ScheduledPartitionsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "scheduled_partitions_source({})", self.scheduler)
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
        let source = ScheduledPartitionsSource { scheduler };

        assert_eq!(
            source.to_string(),
            "scheduled_partitions_source(local_compaction_scheduler)",
        );
    }
}
