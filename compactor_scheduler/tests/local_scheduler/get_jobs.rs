use std::sync::Arc;

use assert_matches::assert_matches;
use compactor_scheduler::{create_test_scheduler, CompactionJob};
use data_types::PartitionId;
use iox_tests::TestCatalog;

use super::{super::helpers, TestLocalScheduler};

#[tokio::test]
async fn test_mocked_partition_ids() {
    let catalog = TestCatalog::new();
    let partitions = vec![PartitionId::new(0), PartitionId::new(1234242)];

    let scheduler = create_test_scheduler(
        catalog.catalog(),
        Arc::clone(&catalog.time_provider()),
        Some(partitions.clone()),
    );

    let mut result = scheduler
        .get_jobs()
        .await
        .iter()
        .map(|j| j.partition_id)
        .collect::<Vec<PartitionId>>();
    result.sort();

    assert_eq!(
        result,
        partitions,
        "expected the partitions provided to create_test_scheduler() should be returned on `get_jobs()`, instead got `{:?}`", result
    );
}

#[tokio::test]
async fn test_returns_hot_partition() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;

    let jobs = test_scheduler.scheduler.get_jobs().await;
    test_scheduler.assert_matches_seeded_hot_partition(&jobs);
}

#[tokio::test]
async fn test_will_not_fetch_leased_partitions() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let scheduler = Arc::clone(&test_scheduler.scheduler);

    let jobs = scheduler.get_jobs().await;
    test_scheduler.assert_matches_seeded_hot_partition(&jobs);

    helpers::assert_all_partitions_leased(scheduler).await;
}

#[tokio::test]
async fn test_can_refetch_previously_completed_partitions() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let expected_partition = test_scheduler.get_partition_id();
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let mut jobs = scheduler.get_jobs().await;
    let (existing_1, _) = test_scheduler.get_seeded_files();

    // make commit, so not throttled
    helpers::can_do_upgrade_commit(Arc::clone(&scheduler), jobs[0].clone(), existing_1).await;
    // complete
    helpers::can_do_complete(Arc::clone(&scheduler), jobs[0].clone()).await;

    // TEST: can refetch
    jobs = scheduler.get_jobs().await;
    assert_matches!(
        jobs[..],
        [CompactionJob { partition_id, .. }] if partition_id == expected_partition,
        "expected partition is available after lease is complete, but found {:?}", jobs
    );
}

#[tokio::test]
async fn test_will_throttle_partition_if_no_commits() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let mut jobs = scheduler.get_jobs().await;

    // no commit

    // complete
    helpers::can_do_complete(Arc::clone(&scheduler), jobs[0].clone()).await;

    // TEST: is throttled
    jobs = scheduler.get_jobs().await;
    assert_matches!(
        jobs[..],
        [],
        "expect partition should be throttled, but found {:?}",
        jobs
    );
}
