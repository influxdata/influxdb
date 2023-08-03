use std::sync::Arc;

use assert_matches::assert_matches;
use compactor_scheduler::{CompactionJob, CompactionJobEnd, CompactionJobEndVariant, SkipReason};
use data_types::SkippedCompaction;

use super::{super::helpers, TestLocalScheduler};

#[tokio::test]
async fn test_must_end_job_to_make_partition_reavailable() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let mut jobs = scheduler.get_jobs().await;
    let (existing_1, _) = test_scheduler.get_seeded_files();

    // upgrade commit (to avoid throttling)
    helpers::can_do_upgrade_commit(Arc::clone(&scheduler), jobs[0].clone(), existing_1).await;

    // lease is still in place (even after commits)
    helpers::assert_all_partitions_leased(Arc::clone(&scheduler)).await;

    // mark job as complete
    helpers::can_do_complete(Arc::clone(&scheduler), jobs[0].clone()).await;

    // TEST: partition is available again
    jobs = scheduler.get_jobs().await;
    let expected_partition = test_scheduler.get_partition_id();
    assert_matches!(
        jobs[..],
        [CompactionJob { partition_id, .. }] if partition_id == expected_partition,
        "expect partition is available again, instead found {:?}", jobs
    );
}

#[tokio::test]
async fn test_job_ended_without_commits_should_be_throttled() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let mut jobs = scheduler.get_jobs().await;

    // no commit

    // lease is still in place
    helpers::assert_all_partitions_leased(Arc::clone(&scheduler)).await;

    // mark job as complete
    helpers::can_do_complete(Arc::clone(&scheduler), jobs[0].clone()).await;

    // TEST: partition is throttled
    jobs = scheduler.get_jobs().await;
    assert_matches!(
        jobs[..],
        [],
        "expect partition should be throttled, instead found {:?}",
        jobs
    );
}

#[tokio::test]
async fn test_error_reporting_is_not_sufficient_to_avoid_throttling() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let mut jobs = scheduler.get_jobs().await;

    // no commit
    // only error reporting
    helpers::can_send_error(Arc::clone(&scheduler), jobs[0].clone()).await;

    // lease is still in place
    helpers::assert_all_partitions_leased(Arc::clone(&scheduler)).await;

    // mark job as complete
    helpers::can_do_complete(Arc::clone(&scheduler), jobs[0].clone()).await;

    // TEST: partition is throttled
    jobs = scheduler.get_jobs().await;
    assert_matches!(
        jobs[..],
        [],
        "expect partition should be throttled, instead found {:?}",
        jobs
    );
}

#[tokio::test]
async fn test_after_complete_job_cannot_end_again() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let jobs = scheduler.get_jobs().await;

    // mark job as complete
    helpers::can_do_complete(Arc::clone(&scheduler), jobs[0].clone()).await;

    // TEST: end_job complete => will fail, since job is already complete
    let res = scheduler
        .end_job(CompactionJobEnd {
            job: jobs[0].clone(),
            end_action: CompactionJobEndVariant::Complete,
        })
        .await;
    assert_matches!(
        res,
        Err(err) if err.to_string().contains("Unknown or already done partition:"),
        "should error if attempt complete after complete, instead found {:?}", res
    );

    // TEST: end_job skip request => will fail, since job is already complete
    let res = scheduler
        .end_job(CompactionJobEnd {
            job: jobs[0].clone(),
            end_action: CompactionJobEndVariant::RequestToSkip(SkipReason(
                "skip this partition".to_string(),
            )),
        })
        .await;
    assert_matches!(
        res,
        Err(err) if err.to_string().contains("Unknown or already done partition:"),
        "should error if attempt skip partition after complete, instead found {:?}", res
    );
}

#[tokio::test]
async fn test_after_skip_request_cannot_end_again() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let jobs = scheduler.get_jobs().await;

    // mark partition as skipped (also completes the job)
    helpers::can_do_skip_request(Arc::clone(&scheduler), jobs[0].clone()).await;

    // TEST: end_job complete => will fail, since job is already complete
    let res = scheduler
        .end_job(CompactionJobEnd {
            job: jobs[0].clone(),
            end_action: CompactionJobEndVariant::Complete,
        })
        .await;
    assert_matches!(
        res,
        Err(err) if err.to_string().contains("Unknown or already done partition:"),
        "should error if attempt complete after skip request, instead found {:?}", res
    );

    // TEST: end_job skip request => will fail, since job is already complete
    let res = scheduler
        .end_job(CompactionJobEnd {
            job: jobs[0].clone(),
            end_action: CompactionJobEndVariant::RequestToSkip(SkipReason(
                "skip this partition".to_string(),
            )),
        })
        .await;
    assert_matches!(
        res,
        Err(err) if err.to_string().contains("Unknown or already done partition:"),
        "should error if attempt skip request after skip request, instead found {:?}", res
    );
}

#[tokio::test]
async fn test_what_skip_request_does() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let expected_partition = test_scheduler.get_partition_id();
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let catalog = test_scheduler.catalog.catalog();

    let mut jobs = scheduler.get_jobs().await;
    let (existing_1, _) = test_scheduler.get_seeded_files();

    // upgrade commit (to avoid throttling as a confounding variable)
    helpers::can_do_upgrade_commit(Arc::clone(&scheduler), jobs[0].clone(), existing_1).await;

    helpers::assert_all_partitions_leased(Arc::clone(&scheduler)).await;

    // mark partition as skipped (also completes the job)
    helpers::can_do_skip_request(Arc::clone(&scheduler), jobs[0].clone()).await;

    // skipped partitions are no longer available
    jobs = scheduler.get_jobs().await;
    assert_matches!(
        jobs[..],
        [],
        "expect partition is not returned from get_jobs(), instead found {:?}",
        jobs
    );

    // confirm is marked as skipped in catalog
    let catalog_marked_as_skipped = catalog
        .repositories()
        .await
        .partitions()
        .get_in_skipped_compactions(&[expected_partition])
        .await;
    assert_matches!(
        catalog_marked_as_skipped.as_deref(),
        Ok([SkippedCompaction { partition_id, .. }]) if *partition_id == expected_partition,
        "expect partition should be marked as skipped in catalog, instead found {:?}", catalog_marked_as_skipped
    );
}
