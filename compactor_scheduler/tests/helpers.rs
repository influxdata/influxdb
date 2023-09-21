//! Helpers for testing.
//!
//! Usable for any Scheduler API (remote or local).

use std::sync::Arc;

use assert_matches::assert_matches;
use compactor_scheduler::{
    CommitUpdate, CompactionJob, CompactionJobEnd, CompactionJobEndVariant, CompactionJobStatus,
    CompactionJobStatusResponse, CompactionJobStatusVariant, ErrorKind, Scheduler, SkipReason,
};
use data_types::{CompactionLevel, ParquetFile, ParquetFileParams};

pub async fn can_do_replacement_commit(
    scheduler: Arc<dyn Scheduler>,
    job: CompactionJob,
    deleted_parquet_files: Vec<ParquetFile>,
    created_parquet_files: Vec<ParquetFileParams>,
) {
    let commit_update = CommitUpdate::new(
        job.partition_id,
        deleted_parquet_files,
        vec![],
        created_parquet_files,
        CompactionLevel::Final,
    );

    let res = scheduler
        .update_job_status(CompactionJobStatus {
            job,
            status: CompactionJobStatusVariant::Update(commit_update),
        })
        .await;

    assert_matches!(
        res,
        Ok(CompactionJobStatusResponse::CreatedParquetFiles(files)) if files.len() == 1,
        "expected replacement commit to succeed with exactly one file to be created, got {:?}", res
    );
}

pub async fn can_do_upgrade_commit(
    scheduler: Arc<dyn Scheduler>,
    job: CompactionJob,
    existing_parquet_file: ParquetFile,
) {
    let commit_update = CommitUpdate::new(
        job.partition_id,
        vec![],
        vec![existing_parquet_file],
        vec![],
        CompactionLevel::Final,
    );

    let res = scheduler
        .update_job_status(CompactionJobStatus {
            job,
            status: CompactionJobStatusVariant::Update(commit_update),
        })
        .await;

    assert_matches!(
        res,
        Ok(CompactionJobStatusResponse::CreatedParquetFiles(files)) if files.is_empty(),
        "expected upgrade commit to succeed with no files to be created, got {:?}", res
    );
}

pub async fn can_send_error(scheduler: Arc<dyn Scheduler>, job: CompactionJob) {
    let res = scheduler
        .update_job_status(CompactionJobStatus {
            job,
            status: CompactionJobStatusVariant::Error(ErrorKind::Unknown(
                "error reported (without partition-skip request)".into(),
            )),
        })
        .await;

    assert_matches!(
        res,
        Ok(CompactionJobStatusResponse::Ack),
        "expected error to be accepted, got {:?}",
        res
    );
}

pub async fn can_do_complete(scheduler: Arc<dyn Scheduler>, job: CompactionJob) {
    let res = scheduler
        .end_job(CompactionJobEnd {
            job,
            end_action: CompactionJobEndVariant::Complete,
        })
        .await;

    assert_matches!(
        res,
        Ok(()),
        "expected job to be marked as complete, got {:?}",
        res
    );
}

pub async fn can_do_skip_request(scheduler: Arc<dyn Scheduler>, job: CompactionJob) {
    let res = scheduler
        .end_job(CompactionJobEnd {
            job,
            end_action: CompactionJobEndVariant::RequestToSkip(SkipReason(
                "error reason given for request the skip".into(),
            )),
        })
        .await;

    assert_matches!(
        res,
        Ok(()),
        "expected job to be marked as skipped, got {:?}",
        res
    );
}

pub async fn assert_all_partitions_leased(scheduler: Arc<dyn Scheduler>) {
    let jobs = scheduler.get_jobs().await;
    assert_matches!(
        jobs[..],
        [],
        "expected no partition found (all partitions leased), but instead got {:?}",
        jobs,
    );
}
