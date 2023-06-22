use std::sync::Arc;

use async_trait::async_trait;
use compactor_scheduler::{LocalScheduler, PartitionsSource};
use data_types::PartitionId;

use crate::config::Config;

#[derive(Debug)]
pub struct ScheduledPartitionsSource {
    scheduler: LocalScheduler, // TODO: followon PR will replace with Arc<dyn Scheduler>
}

impl ScheduledPartitionsSource {
    pub fn new(config: &Config) -> Self {
        // TODO: followon PR:
        // * will have the Arc<dyn Scheduler> provided as a component to the compactor
        // * this Scheduler will be created with the below components
        let scheduler = LocalScheduler::new(
            config.partitions_source.clone(),
            config.shard_config.clone(),
            config.backoff_config.clone(),
            Arc::clone(&config.catalog),
            Arc::clone(&config.time_provider),
        );

        Self { scheduler }
    }
}

#[async_trait]
impl PartitionsSource for ScheduledPartitionsSource {
    async fn fetch(&self) -> Vec<PartitionId> {
        self.scheduler.fetch().await
    }
}

impl std::fmt::Display for ScheduledPartitionsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "scheduled_partitions_source({})", self.scheduler)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use backoff::BackoffConfig;
    use compactor_scheduler::PartitionsSourceConfig;
    use iox_tests::TestCatalog;
    use iox_time::{MockProvider, Time};

    use super::*;

    #[test]
    fn test_display() {
        let scheduler = LocalScheduler::new(
            PartitionsSourceConfig::Fixed(HashSet::new()),
            None,
            BackoffConfig::default(),
            TestCatalog::new().catalog(),
            Arc::new(MockProvider::new(Time::MIN)),
        );
        let source = ScheduledPartitionsSource { scheduler };

        assert_eq!(
            source.to_string(),
            "scheduled_partitions_source(local_compaction_scheduler)",
        );
    }
}
