use std::sync::Arc;

use assert_matches::assert_matches;
use compactor_scheduler::{CommitUpdate, CompactionJobStatus, CompactionJobStatusVariant};
use data_types::CompactionLevel;

use super::{super::helpers, TestLocalScheduler};

#[tokio::test]
async fn test_has_two_commit_types() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let jobs = scheduler.get_jobs().await;
    let (existing_1, existing_2) = test_scheduler.get_seeded_files();

    // upgrade commit
    helpers::can_do_upgrade_commit(Arc::clone(&scheduler), jobs[0].clone(), existing_1).await;

    // replacement commit
    helpers::can_do_replacement_commit(
        scheduler,
        jobs[0].clone(),
        vec![existing_2], // deleted parquet_files
        vec![test_scheduler.create_params_for_new_parquet_file().await], // to_create
    )
    .await;
}

#[tokio::test]
async fn test_no_empty_commits_permitted() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let jobs = scheduler.get_jobs().await;

    // empty commit
    let res = scheduler
        .update_job_status(CompactionJobStatus {
            job: jobs[0].clone(),
            status: CompactionJobStatusVariant::Update(CommitUpdate::new(
                test_scheduler.get_partition_id(),
                vec![],
                vec![],
                vec![],
                CompactionLevel::Final, // no args
            )),
        })
        .await;

    assert_matches!(
        res,
        Err(err) if err.to_string().contains("commit must have files to upgrade, and/or a set of files to replace (delete and create)"),
        "should reject empty commits (no args provided), instead got {:?}", res
    );
}

#[tokio::test]
async fn test_incomplete_replacement_commits_should_error() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let jobs = scheduler.get_jobs().await;
    let (existing_1, existing_2) = test_scheduler.get_seeded_files();

    // incomplete replacement commit
    let res = scheduler
        .update_job_status(CompactionJobStatus {
            job: jobs[0].clone(),
            status: CompactionJobStatusVariant::Update(CommitUpdate::new(
                test_scheduler.get_partition_id(),
                vec![existing_1.clone()], // to delete
                vec![],
                vec![], // missing to create
                CompactionLevel::Final,
            )),
        })
        .await;

    assert_matches!(
        res,
        Err(err) if err.to_string().contains("replacement commits must have both files to delete and files to create"),
        "should reject incomplete replacement commit, instead got {:?}", res
    );

    // incomplete replacement commit, + complete upgrade commit
    let res = scheduler
        .update_job_status(CompactionJobStatus {
            job: jobs[0].clone(),
            status: CompactionJobStatusVariant::Update(CommitUpdate::new(
                test_scheduler.get_partition_id(),
                vec![existing_1], // to delete
                vec![existing_2], // to upgrade
                vec![],           // missing to create
                CompactionLevel::Final,
            )),
        })
        .await;

    assert_matches!(
        res,
        Err(err) if err.to_string().contains("replacement commits must have both files to delete and files to create"),
        "should reject incomplete replacement commit (even when an upgrade commit is also provided), but found {:?}", res
    );
}

#[tokio::test]
async fn test_has_error_reporting() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let jobs = scheduler.get_jobs().await;

    helpers::can_send_error(scheduler, jobs[0].clone()).await;
}

#[tokio::test]
async fn test_can_commit_after_error_reporting() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let jobs = scheduler.get_jobs().await;
    let (existing_1, existing_2) = test_scheduler.get_seeded_files();

    // error
    helpers::can_send_error(Arc::clone(&scheduler), jobs[0].clone()).await;

    // TEST: can do commits
    helpers::can_do_upgrade_commit(Arc::clone(&scheduler), jobs[0].clone(), existing_1).await;
    helpers::can_do_replacement_commit(
        scheduler,
        jobs[0].clone(),
        vec![existing_2],                                                // deleted
        vec![test_scheduler.create_params_for_new_parquet_file().await], // to create
    )
    .await;
}

#[tokio::test]
async fn test_can_report_errors_after_commits() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let jobs = scheduler.get_jobs().await;
    let (existing_1, existing_2) = test_scheduler.get_seeded_files();

    // do commits
    helpers::can_do_upgrade_commit(Arc::clone(&scheduler), jobs[0].clone(), existing_1).await;
    helpers::can_do_replacement_commit(
        Arc::clone(&scheduler),
        jobs[0].clone(),
        vec![existing_2],                                                // deleted
        vec![test_scheduler.create_params_for_new_parquet_file().await], // to create
    )
    .await;

    // TEST: can still send error
    helpers::can_send_error(scheduler, jobs[0].clone()).await;
}

#[tokio::test]
async fn test_cannot_commit_after_complete() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let jobs = scheduler.get_jobs().await;
    let (existing_1, _) = test_scheduler.get_seeded_files();

    // mark job as complete
    helpers::can_do_complete(Arc::clone(&scheduler), jobs[0].clone()).await;

    // TEST: cannot do a commit
    let res = scheduler
        .update_job_status(CompactionJobStatus {
            job: jobs[0].clone(),
            status: CompactionJobStatusVariant::Update(CommitUpdate::new(
                test_scheduler.get_partition_id(),
                vec![],
                vec![existing_1],
                vec![],
                CompactionLevel::Final,
            )),
        })
        .await;
    assert_matches!(
        res,
        Err(err) if err.to_string().contains("Unknown or already done partition:"),
        "should reject commit after complete, but found {:?}", res
    );
}

#[tokio::test]
async fn test_can_do_more_error_reporting_after_complete() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let jobs = scheduler.get_jobs().await;

    // mark job as complete
    helpers::can_do_complete(Arc::clone(&scheduler), jobs[0].clone()).await;

    // TEST: can do more error reporting
    helpers::can_send_error(scheduler, jobs[0].clone()).await;
}

#[tokio::test]
async fn test_cannot_commit_after_skipping() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let jobs = scheduler.get_jobs().await;
    let (existing_1, _) = test_scheduler.get_seeded_files();

    // mark partition as skipped (also completes the job)
    helpers::can_do_skip_request(Arc::clone(&scheduler), jobs[0].clone()).await;

    // TEST: cannot do a commit
    let res = scheduler
        .update_job_status(CompactionJobStatus {
            job: jobs[0].clone(),
            status: CompactionJobStatusVariant::Update(CommitUpdate::new(
                test_scheduler.get_partition_id(),
                vec![],
                vec![existing_1],
                vec![],
                CompactionLevel::Final,
            )),
        })
        .await;
    assert_matches!(
        res,
        Err(err) if err.to_string().contains("Unknown or already done partition:"),
        "should reject commit after skipping, but found {:?}", res
    );
}

#[tokio::test]
async fn test_can_do_more_error_reporting_after_skipping() {
    test_helpers::maybe_start_logging();

    let test_scheduler = TestLocalScheduler::builder().await;
    let scheduler = Arc::clone(&test_scheduler.scheduler);
    let jobs = scheduler.get_jobs().await;

    // mark partition as skipped (also completes the job)
    helpers::can_do_skip_request(Arc::clone(&scheduler), jobs[0].clone()).await;

    // TEST: can do more error reporting
    helpers::can_send_error(scheduler, jobs[0].clone()).await;
}
