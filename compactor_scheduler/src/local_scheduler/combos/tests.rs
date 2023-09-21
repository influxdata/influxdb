use std::{sync::Arc, time::Duration};

use data_types::{CompactionLevel, PartitionId};
use iox_time::{MockProvider, Time};

use crate::{
    Commit, MockCommit, MockPartitionDoneSink, MockPartitionsSource, PartitionDoneSink,
    PartitionsSource,
};

use super::{throttle_partition::throttle_partition, unique_partitions::unique_partitions};

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
        .await
        .expect("commit failed");
    sink.record(PartitionId::new(1), Ok(()))
        .await
        .expect("record failed");
    sink.record(PartitionId::new(2), Ok(()))
        .await
        .expect("record failed");

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
