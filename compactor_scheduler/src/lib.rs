//! service for scheduling compactor tasks.

#![deny(
    rustdoc::broken_intra_doc_links,
    rust_2018_idioms,
    missing_debug_implementations,
    unreachable_pub
)]
#![warn(
    missing_docs,
    clippy::todo,
    clippy::dbg_macro,
    clippy::explicit_iter_loop,
    clippy::clone_on_ref_ptr,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    unused_crate_dependencies
)]
#![allow(clippy::missing_docs_in_private_items)]

use std::collections::HashSet;

use backoff::BackoffConfig;
use data_types::PartitionId;
use iox_time::TimeProvider;
// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub(crate) mod commit;
pub(crate) use commit::mock::MockCommit;
pub use commit::{Commit, CommitWrapper, Error as CommitError};

mod error;
pub use error::ErrorKind;

mod local_scheduler;
#[allow(unused_imports)] // for testing
pub(crate) use local_scheduler::partition_done_sink::mock::MockPartitionDoneSink;
pub use local_scheduler::{
    combos::throttle_partition::Error as ThrottleError,
    partitions_source_config::PartitionsSourceConfig, shard_config::ShardConfig,
    LocalSchedulerConfig,
};
pub(crate) use local_scheduler::{
    combos::unique_partitions::Error as UniquePartitionsError,
    id_only_partition_filter::IdOnlyPartitionFilter,
    partition_done_sink::Error as PartitionDoneSinkError, partition_done_sink::PartitionDoneSink,
    LocalScheduler,
};

// partitions_source trait
mod partitions_source;
pub(crate) use partitions_source::*;

// scheduler trait and associated types
mod scheduler;
pub use scheduler::*;

use std::sync::Arc;

use iox_catalog::interface::Catalog;

/// Instantiate a compaction scheduler service
pub fn create_scheduler(
    config: SchedulerConfig,
    catalog: Arc<dyn Catalog>,
    time_provider: Arc<dyn TimeProvider>,
    metrics: Arc<metric::Registry>,
    shadow_mode: bool,
) -> Arc<dyn Scheduler> {
    match config {
        SchedulerConfig::Local(scheduler_config) => {
            let scheduler = LocalScheduler::new(
                scheduler_config,
                BackoffConfig::default(),
                catalog,
                time_provider,
                metrics,
                shadow_mode,
            );
            Arc::new(scheduler)
        }
    }
}

/// Create a new [`Scheduler`] for testing.
///
/// If no mocked_partition_ids, the scheduler will use a [`LocalScheduler`] in default configuration.
/// Whereas if mocked_partition_ids are provided, the scheduler will use a [`LocalScheduler`] with [`PartitionsSourceConfig::Fixed`].
pub fn create_test_scheduler(
    catalog: Arc<dyn Catalog>,
    time_provider: Arc<dyn TimeProvider>,
    mocked_partition_ids: Option<Vec<PartitionId>>,
) -> Arc<dyn Scheduler> {
    let scheduler_config = match mocked_partition_ids {
        None => SchedulerConfig::default(),
        Some(partition_ids) => SchedulerConfig::Local(LocalSchedulerConfig {
            commit_wrapper: None,
            partitions_source_config: PartitionsSourceConfig::Fixed(
                partition_ids.into_iter().collect::<HashSet<PartitionId>>(),
            ),
            shard_config: None,
            ignore_partition_skip_marker: false,
        }),
    };
    create_scheduler(
        scheduler_config,
        catalog,
        time_provider,
        Arc::new(metric::Registry::default()),
        false,
    )
}

#[cfg(test)]
mod tests {
    use iox_tests::TestCatalog;
    use iox_time::{MockProvider, Time};

    use super::*;

    #[test]
    fn test_display_will_not_change_for_external_tests() {
        let scheduler = create_test_scheduler(
            TestCatalog::new().catalog(),
            Arc::new(MockProvider::new(Time::MIN)),
            None,
        );

        assert_eq!(scheduler.to_string(), "local_compaction_scheduler");

        let scheduler = create_test_scheduler(
            TestCatalog::new().catalog(),
            Arc::new(MockProvider::new(Time::MIN)),
            Some(vec![PartitionId::new(0)]),
        );

        assert_eq!(scheduler.to_string(), "local_compaction_scheduler");
    }

    #[tokio::test]
    async fn test_test_scheduler_with_mocked_parition_ids() {
        let partitions = vec![PartitionId::new(0), PartitionId::new(1234242)];

        let scheduler = create_test_scheduler(
            TestCatalog::new().catalog(),
            Arc::new(MockProvider::new(Time::MIN)),
            Some(partitions.clone()),
        );
        let mut result = scheduler
            .get_jobs()
            .await
            .iter()
            .map(|j| j.partition_id)
            .collect::<Vec<PartitionId>>();
        result.sort();

        assert_eq!(result, partitions);
    }
}
