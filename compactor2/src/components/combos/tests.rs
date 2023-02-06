use std::{sync::Arc, time::Duration};

use data_types::{CompactionLevel, PartitionId};
use iox_time::{MockProvider, Time};

use crate::components::{
    combos::{throttle_partition::throttle_partition, unique_partitions::unique_partitions},
    commit::{mock::MockCommit, Commit},
    partition_done_sink::{mock::MockPartitionDoneSink, PartitionDoneSink},
    partitions_source::{mock::MockPartitionsSource, PartitionsSource},
};

#[tokio::test]
async fn test_unique_and_throttle() {
    let inner_source = Arc::new(MockPartitionsSource::new(vec![
        PartitionId::new(1),
        PartitionId::new(2),
        PartitionId::new(3),
    ]));
    let inner_commit = Arc::new(MockCommit::new());
    let inner_sink = Arc::new(MockPartitionDoneSink::new());
    let time_provider = Arc::new(MockProvider::new(Time::MIN));

    let (source, sink) = unique_partitions(Arc::clone(&inner_source), Arc::clone(&inner_sink), 1);
    let (source, commit, sink) = throttle_partition(
        source,
        Arc::clone(&inner_commit),
        sink,
        Arc::clone(&time_provider) as _,
        Duration::from_secs(1),
        1,
    );

    assert_eq!(
        source.fetch().await,
        vec![
            PartitionId::new(1),
            PartitionId::new(2),
            PartitionId::new(3)
        ],
    );

    assert_eq!(source.fetch().await, vec![],);

    commit
        .commit(PartitionId::new(1), &[], &[], &[], CompactionLevel::Initial)
        .await;
    sink.record(PartitionId::new(1), Ok(())).await;
    sink.record(PartitionId::new(2), Ok(())).await;

    inner_source.set(vec![
        PartitionId::new(1),
        PartitionId::new(2),
        PartitionId::new(3),
        PartitionId::new(4),
    ]);

    assert_eq!(
        source.fetch().await,
        vec![PartitionId::new(1), PartitionId::new(4)],
    );

    time_provider.inc(Duration::from_secs(1));

    assert_eq!(source.fetch().await, vec![PartitionId::new(2)],);
}
