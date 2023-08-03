use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use chrono::Utc;
use compactor_scheduler::CompactionJob;
use data_types::{CompactionLevel, ParquetFile, ParquetFileParams, PartitionId};
use futures::{stream, StreamExt, TryStreamExt};
use iox_query::exec::query_tracing::send_metrics_to_tracing;
use observability_deps::tracing::info;
use parquet_file::ParquetFilePath;
use tokio::sync::watch::Sender;
use trace::span::Span;
use trace::span::SpanRecorder;
use tracker::InstrumentedAsyncSemaphore;

use crate::{
    components::{
        changed_files_filter::SavedParquetFileState,
        scratchpad::Scratchpad,
        timeout::{timeout_with_progress_checking, TimeoutWithProgress},
        Components,
    },
    error::{DynError, ErrorKind, SimpleError},
    file_classification::{FileClassification, FilesForProgress},
    partition_info::PartitionInfo,
    PlanIR, RoundInfo,
};

/// Tries to compact all eligible partitions, up to
/// partition_concurrency at a time.
pub async fn compact(
    trace_collector: Option<Arc<dyn trace::TraceCollector>>,
    partition_concurrency: NonZeroUsize,
    partition_timeout: Duration,
    df_semaphore: Arc<InstrumentedAsyncSemaphore>,
    components: &Arc<Components>,
) {
    components
        .compaction_job_stream
        .stream()
        .map(|job| {
            let components = Arc::clone(components);

            // A root span is created for each compaction job (a.k.a. partition).
            // Later this can be linked to the
            // scheduler's span via something passed through compaction_job_stream.
            let root_span: Option<Span> = trace_collector
                .as_ref()
                .map(|collector| Span::root("compaction", Arc::clone(collector)));
            let span = SpanRecorder::new(root_span);

            compact_partition(
                span,
                job,
                partition_timeout,
                Arc::clone(&df_semaphore),
                components,
            )
        })
        .buffer_unordered(partition_concurrency.get())
        .collect::<()>()
        .await;
}

async fn compact_partition(
    mut span: SpanRecorder,
    job: CompactionJob,
    partition_timeout: Duration,
    df_semaphore: Arc<InstrumentedAsyncSemaphore>,
    components: Arc<Components>,
) {
    let partition_id = job.partition_id;
    info!(partition_id = partition_id.get(), timeout = ?partition_timeout, "compact partition",);
    span.set_metadata("partition_id", partition_id.get().to_string());
    let scratchpad = components.scratchpad_gen.pad();

    let res = timeout_with_progress_checking(partition_timeout, |transmit_progress_signal| {
        let components = Arc::clone(&components);
        let scratchpad = Arc::clone(&scratchpad);
        async {
            try_compact_partition(
                span,
                job.clone(),
                df_semaphore,
                components,
                scratchpad,
                transmit_progress_signal,
            )
            .await // errors detected in the CompactionJob update_job_status(), will be handled in the timeout_with_progress_checking
        }
    })
    .await;

    let res = match res {
        // If `try_compact_partition` timed out and didn't make any progress, something is wrong
        // with this partition and it should get added to the `skipped_compactions` table by
        // sending a timeout error to the `compaction_job_done_sink`.
        TimeoutWithProgress::NoWorkTimeOutError => Err(Box::new(SimpleError::new(
            ErrorKind::Timeout,
            "timeout without making any progress",
        )) as _),
        // If `try_compact_partition` timed out but *did* make some progress, this is fine, don't
        // add it to the `skipped_compactions` table.
        TimeoutWithProgress::SomeWorkTryAgain => Ok(()),
        // If `try_compact_partition` finished before the timeout, return the `Result` that it
        // returned. If an error was returned, there could be something wrong with the partiton;
        // let the `compaction_job_done_sink` decide if the error means the partition should be added
        // to the `skipped_compactions` table or not.
        TimeoutWithProgress::Completed(res) => res,
    };

    // TODO: how handle errors detected in the CompactionJob ending actions?
    let _ = components.compaction_job_done_sink.record(job, res).await;

    scratchpad.clean().await;
    info!(partition_id = partition_id.get(), "compaction job done",);
}

/// Main function to compact files of a single partition.
///
/// Input: any files in the partitions (L0s, L1s, L2s)
/// Output:
/// 1. No overlapped  L0 files
/// 2. Up to N non-overlapped L1 and L2 files,  subject to  the total size of the files.
///
/// N: config max_number_of_files
///
/// Note that since individual files also have a maximum size limit, the
/// actual number of files can be more than  N.  Also since the Parquet format
/// features high and variable compression (page splits, RLE, zstd compression),
/// splits are based on estimated output file sizes which may deviate from actual file sizes
///
/// Algorithms
///
/// GENERAL IDEA OF THE CODE: DIVIDE & CONQUER  (we have not used all of its power yet)
///
/// The files are split into non-time-overlaped branches, each is compacted in parallel.
/// The output of each branch is then combined and re-branch in next round until
/// they should not be compacted based on defined stop conditions.
///
/// Example: Partition has 7 files: f1, f2, f3, f4, f5, f6, f7
///  Input: shown by their time range
///          |--f1--|               |----f3----|  |-f4-||-f5-||-f7-|
///               |------f2----------|                   |--f6--|
///
/// - Round 1: Assuming 7 files are split into 2 branches:
///  . Branch 1: has 3 files: f1, f2, f3
///  . Branch 2: has 4 files: f4, f5, f6, f7
///          |<------- Branch 1 -------------->|  |<-- Branch 2 -->|
///          |--f1--|               |----f3----|  |-f4-||-f5-||-f7-|
///               |------f2----------|                   |--f6--|
///
///    Output: 3 files f8, f9, f10
///          |<------- Branch 1 -------------->|  |<-- Branch 2--->|
///          |---------f8---------------|--f9--|  |-----f10--------|
///
/// - Round 2: 3 files f8, f9, f10 are in one branch and compacted into 2 files
///    Output: two files f11, f12
///          |-------------------f11---------------------|---f12---|
///
/// - Stop condition meets and the final output is f11 & F12
///
/// The high level flow is:
///
///   . Mutiple rounds, each round process mutltiple branches. Each branch includes at most 200 files
///   . Each branch will compact files lowest level (aka start-level) into its next level (aka target-level), either:
///      - Compact many L0s into fewer and larger L0s. Start-level = target-level = 0
///      - Compact many L1s into fewer and larger L1s. Start-level = target-level = 1
///      - Compact (L0s & L1s) to L1s if there are L0s. Start-level = 0, target-level = 1
///      - Compact (L1s & L2s) to L2s if no L0s. Start-level = 1, target-level = 2
///      - Split L0s each of which overlaps with more than 1 L1s into many L0s, each overlaps with at most one L1 files
///      - Split L1s each of which overlaps with more than 1 L2s into many L1s, each overlaps with at most one L2 files
///   . Each branch does find non-overlaps and upgragde files to avoid unecessary recompacting.
///     The actually split files:
///      1. files_to_keep: do not compact these files because they are already higher than target level
///      2. files_to_upgrade: upgrade this initial-level files to target level because they are not overlap with
///          any target-level and initial-level files and large enough (> desired max size)
///      3. files_to_split_or_compact: this is either files to split or files to compact and will be handled accordingly

///
/// Example: 4 files: two L0s, two L1s and one L2
///  Input:
///                                      |-L0.1-||------L0.2-------|
///                  |-----L1.1-----||--L1.2--|
///     |----L2.1-----|
///
///  - Round 1: There are L0s, let compact L0s with L1s. But let split them first:
///    . files_higher_keep: L2.1 (higher leelve than targetlevel) and L1.1 (not overlapped wot any L0s)
///    . files_upgrade: L0.2
///    . files_compact: L0.1, L1.2
///    Output: 4 files
///                                               |------L1.4-------|
///                  |-----L1.1-----||-new L1.3 -|        ^
///     |----L2.1-----|                  ^               |
///                                      |        result of upgrading L0.2
///                            result of compacting L0.1, L1.2
///
///  - Round 2: Compact those 4 files
///    Output: two L2 files
///     |-----------------L2.2---------------------||-----L2.3------|
///
/// Note:
///   . If there are no L0s files in the partition, the first round can just compact L1s and L2s to L2s
///   . Round 2 happens or not depends on the stop condition
async fn try_compact_partition(
    span: SpanRecorder,
    job: CompactionJob,
    df_semaphore: Arc<InstrumentedAsyncSemaphore>,
    components: Arc<Components>,
    scratchpad_ctx: Arc<dyn Scratchpad>,
    transmit_progress_signal: Sender<bool>,
) -> Result<(), DynError> {
    let partition_id = job.partition_id;
    let mut files = components.partition_files_source.fetch(partition_id).await;
    let partition_info = components.partition_info_source.fetch(partition_id).await?;
    let transmit_progress_signal = Arc::new(transmit_progress_signal);

    // loop for each "Round", consider each file in the partition
    // for partitions with a lot of compaction work to do, keeping the work divided into multiple rounds,
    // with mutliple calls to execute_branch is important to frequently clean the scratchpad and prevent
    // high memory use.
    loop {
        let round_span = span.child("round");

        if files.is_empty() {
            // This should be unreachable, but can happen when someone is manually activiting partitions for compaction.
            info!(
                partition_id = partition_info.partition_id.get(),
                "that's odd - no files to compact in partition"
            );
            return Ok(());
        }

        // This is the stop condition which will be different for different version of compaction
        // and describe where the filter is created at version_specific_partition_filters function
        if !components
            .partition_filter
            .apply(&partition_info, &files)
            .await?
        {
            return Ok(());
        }

        let (round_info, branches, files_later) = components
            .round_info_source
            .calculate(
                Arc::<Components>::clone(&components),
                &partition_info,
                files,
            )
            .await?;

        files = files_later;

        info!(
            partition_id = partition_info.partition_id.get(),
            branch_count = branches.len(),
            concurrency_limit = df_semaphore.total_permits(),
            "compacting branches concurrently",
        );

        // concurrently run the branches.
        let branches_output: Vec<Vec<ParquetFile>> = stream::iter(branches.into_iter())
            .map(|branch| {
                let partition_info = Arc::clone(&partition_info);
                let components = Arc::clone(&components);
                let df_semaphore = Arc::clone(&df_semaphore);
                let transmit_progress_signal = Arc::clone(&transmit_progress_signal);
                let scratchpad = Arc::clone(&scratchpad_ctx);
                let job = job.clone();
                let branch_span = round_span.child("branch");

                async move {
                    execute_branch(
                        branch_span,
                        job,
                        branch,
                        df_semaphore,
                        components,
                        scratchpad,
                        partition_info,
                        round_info,
                        transmit_progress_signal,
                    )
                    .await
                }
            })
            .buffer_unordered(df_semaphore.total_permits())
            .try_collect()
            .await?;

        files.extend(branches_output.into_iter().flatten());
    }
}

/// Compact or split given files
#[allow(clippy::too_many_arguments)]
async fn execute_branch(
    span: SpanRecorder,
    job: CompactionJob,
    branch: Vec<ParquetFile>,
    df_semaphore: Arc<InstrumentedAsyncSemaphore>,
    components: Arc<Components>,
    scratchpad_ctx: Arc<dyn Scratchpad>,
    partition_info: Arc<PartitionInfo>,
    round_info: RoundInfo,
    transmit_progress_signal: Arc<Sender<bool>>,
) -> Result<Vec<ParquetFile>, DynError> {
    let files_next: Vec<ParquetFile> = Vec::new();

    // Keep the current state as a check to make sure this is the only compactor modifying this branch's
    // files. Check that the catalog state for the files in this set is the same before committing and, if not,
    // throw away the compaction work we've done.
    let saved_parquet_file_state = SavedParquetFileState::from(&branch);

    // Identify the target level and files that should be
    // compacted together, upgraded, and kept for next round of
    // compaction
    let FileClassification {
        target_level,
        files_to_make_progress_on,
        files_to_keep,
    } = components
        .file_classifier
        .classify(&partition_info, &round_info, branch);

    // Evaluate whether there's work to do or not based on the files classified for
    // making progress on. If there's no work to do, return early.
    //
    // Currently, no work to do mostly means we are unable to compact this partition due to
    // some limitation such as a large file with single timestamp that we cannot split in
    // order to further compact.
    if !components
        .post_classification_partition_filter
        .apply(&partition_info, &files_to_make_progress_on)
        .await?
    {
        return Ok(files_next);
    }

    let FilesForProgress {
        mut upgrade,
        split_or_compact,
    } = files_to_make_progress_on;

    let paths = split_or_compact.file_input_paths();
    let object_store_ids = scratchpad_ctx.uuids(&paths);
    let plans = components.ir_planner.create_plans(
        Arc::clone(&partition_info),
        target_level,
        split_or_compact.clone(),
        object_store_ids,
        paths,
    );

    let mut files_next: Vec<ParquetFile> = Vec::new();

    // The number of plans is often small (1), but can be thousands, especially in vertical splitting
    // scenarios when the partition is highly backlogged.  So we chunk the plans into groups to control
    // memory usage (all files for all plans in a chunk are loaded to the scratchpad at once), and to
    // allow incremental catalog & progress updates.  But the chunk size should still be large enough
    // to facilitate concurrency in plan execution, which can be accomplished with a small multiple on
    // the concurrency limit.
    let mut chunks = plans.into_iter().peekable();
    while chunks.peek().is_some() {
        // 4x run_plans' concurrency limit will allow adequate concurrency.
        let chunk: Vec<PlanIR> = chunks
            .by_ref()
            .take(df_semaphore.total_permits() * 4)
            .collect();

        let files_to_delete = chunk
            .iter()
            .flat_map(|plan| plan.input_parquet_files())
            .collect();

        // Compact & Split
        let created_file_params = run_plans(
            span.child("run_plans"),
            chunk,
            &partition_info,
            &components,
            Arc::clone(&df_semaphore),
            Arc::<dyn Scratchpad>::clone(&scratchpad_ctx),
        )
        .await?;

        // upload files to real object store
        let upload_span = span.child("upload_objects");
        let created_file_params = upload_files_to_object_store(
            created_file_params,
            Arc::<dyn Scratchpad>::clone(&scratchpad_ctx),
        )
        .await;
        drop(upload_span);

        for file_param in &created_file_params {
            info!(
                partition_id = partition_info.partition_id.get(),
                uuid = file_param.object_store_id.to_string(),
                bytes = file_param.file_size_bytes,
                "uploaded file to objectstore",
            );
        }

        let created_file_paths: Vec<ParquetFilePath> = created_file_params
            .iter()
            .map(ParquetFilePath::from)
            .collect();

        // conditionally (if not shaddow mode) remove the newly created files from the scratchpad.
        scratchpad_ctx
            .clean_written_from_scratchpad(&created_file_paths)
            .await;

        // Update the catalog to reflect the newly created files, soft delete the compacted
        // files and update the upgraded files
        let (created_files, upgraded_files) = update_catalog(
            Arc::clone(&components),
            job.clone(),
            &saved_parquet_file_state,
            files_to_delete,
            upgrade,
            created_file_params,
            target_level,
        )
        .await?;

        // we only need to upgrade files on the first iteration, so empty the upgrade list for next loop.
        upgrade = Vec::new();

        // Report to `timeout_with_progress_checking` that some progress has been made; stop
        // if sending this signal fails because something has gone terribly wrong for the other
        // end of the channel to not be listening anymore.
        if let Err(e) = transmit_progress_signal.send(true) {
            return Err(Box::new(e));
        }

        // track this chunk files to return later
        files_next.extend(created_files);
        files_next.extend(upgraded_files);
    }

    files_next.extend(files_to_keep);
    Ok(files_next)
}

/// Compact or split given files
async fn run_plans(
    span: SpanRecorder,
    plans: Vec<PlanIR>,
    partition_info: &Arc<PartitionInfo>,
    components: &Arc<Components>,
    df_semaphore: Arc<InstrumentedAsyncSemaphore>,
    scratchpad_ctx: Arc<dyn Scratchpad>,
) -> Result<Vec<ParquetFileParams>, DynError> {
    let paths: Vec<ParquetFilePath> = plans.iter().flat_map(|plan| plan.input_paths()).collect();

    // stage files.  This could move to execute_plan to reduce peak scratchpad memory use, but that would
    // cost some concurrency in object downloads.
    let download_span = span.child("download_objects");
    let _ = scratchpad_ctx.load_to_scratchpad(&paths).await;
    drop(download_span);

    info!(
        partition_id = partition_info.partition_id.get(),
        plan_count = plans.len(),
        concurrency_limit = df_semaphore.total_permits(),
        "compacting plans concurrently",
    );

    let created_file_params: Vec<Vec<_>> = stream::iter(
        plans
            .into_iter()
            .filter(|plan| !matches!(plan, PlanIR::None { .. })),
    )
    .map(|plan_ir| {
        execute_plan(
            span.child("execute_plan"),
            plan_ir,
            partition_info,
            components,
            Arc::clone(&df_semaphore),
            Arc::<dyn Scratchpad>::clone(&scratchpad_ctx),
        )
    })
    .buffer_unordered(df_semaphore.total_permits())
    .try_collect()
    .await?;

    Ok(created_file_params.into_iter().flatten().collect())
}

async fn execute_plan(
    mut span: SpanRecorder,
    plan_ir: PlanIR,
    partition_info: &Arc<PartitionInfo>,
    components: &Arc<Components>,
    df_semaphore: Arc<InstrumentedAsyncSemaphore>,
    scratchpad_ctx: Arc<dyn Scratchpad>,
) -> Result<Vec<ParquetFileParams>, DynError> {
    span.set_metadata("input_files", plan_ir.input_files().len().to_string());
    span.set_metadata("input_bytes", plan_ir.input_bytes().to_string());
    span.set_metadata("reason", plan_ir.reason());

    let create = {
        // Adjust concurrency based on the column count in the partition.
        let permits = compute_permits(df_semaphore.total_permits(), partition_info.column_count());

        // use the address of the plan as a uniq identifier so logs can be matched despite the concurrency.
        let plan_id = format!("{:p}", &plan_ir);

        info!(
            partition_id = partition_info.partition_id.get(),
            jobs_running = df_semaphore.holders_acquired(),
            jobs_pending = df_semaphore.holders_pending(),
            permits_needed = permits,
            permits_acquired = df_semaphore.permits_acquired(),
            permits_pending = df_semaphore.permits_pending(),
            plan_id,
            "requesting job semaphore",
        );

        // draw semaphore BEFORE creating the DataFusion plan and drop it directly AFTER finishing the
        // DataFusion computation (but BEFORE doing any additional external IO).
        //
        // We guard the DataFusion planning (that doesn't perform any IO) via the semaphore as well in case
        // DataFusion ever starts to pre-allocate buffers during the physical planning. To the best of our
        // knowledge, this is currently (2023-01-25) not the case but if this ever changes, then we are prepared.
        let permit_span = span.child("acquire_permit");
        let permit = df_semaphore
            .acquire_many(permits, None)
            .await
            .expect("semaphore not closed");
        drop(permit_span);

        info!(
            partition_id = partition_info.partition_id.get(),
            column_count = partition_info.column_count(),
            input_files = plan_ir.n_input_files(),
            permits,
            plan_id,
            "job semaphore acquired",
        );

        let df_span = span.child_span("data_fusion");
        let plan = components
            .df_planner
            .plan(&plan_ir, Arc::clone(partition_info))
            .await?;
        let streams = components.df_plan_exec.exec(Arc::<
            dyn datafusion::physical_plan::ExecutionPlan,
        >::clone(&plan));
        let job = components.parquet_files_sink.stream_into_file_sink(
            streams,
            Arc::clone(partition_info),
            plan_ir.target_level(),
            &plan_ir,
        );

        // TODO: react to OOM and try to divide branch
        let res = job.await;

        if let Some(span) = &df_span {
            send_metrics_to_tracing(Utc::now(), span, plan.as_ref(), true);
        };

        drop(permit);
        drop(df_span);

        // inputs can be removed from the scratchpad as soon as we're done with compaction.
        scratchpad_ctx
            .clean_from_scratchpad(&plan_ir.input_paths())
            .await;

        info!(
            partition_id = partition_info.partition_id.get(),
            plan_id, "job semaphore released",
        );

        res?
    };

    span.set_metadata("output_files", create.len().to_string());
    span.set_metadata(
        "output_bytes",
        create
            .iter()
            .map(|f| f.file_size_bytes as usize)
            .sum::<usize>()
            .to_string(),
    );

    Ok(create)
}

async fn upload_files_to_object_store(
    created_file_params: Vec<ParquetFileParams>,
    scratchpad_ctx: Arc<dyn Scratchpad>,
) -> Vec<ParquetFileParams> {
    // Upload files to real object store
    let output_files: Vec<ParquetFilePath> = created_file_params.iter().map(|p| p.into()).collect();
    let output_uuids = scratchpad_ctx.make_public(&output_files).await;

    // Update file params with object_store_id
    created_file_params
        .into_iter()
        .zip(output_uuids)
        .map(|(f, uuid)| ParquetFileParams {
            object_store_id: uuid,
            ..f
        })
        .collect()
}

async fn fetch_and_save_parquet_file_state(
    components: &Components,
    partition_id: PartitionId,
) -> SavedParquetFileState {
    let catalog_files = components.partition_files_source.fetch(partition_id).await;
    SavedParquetFileState::from(&catalog_files)
}

/// Update the catalog to create, soft delete and upgrade corresponding given input
/// to provided target level
/// Return created and upgraded files
async fn update_catalog(
    components: Arc<Components>,
    job: CompactionJob,
    saved_parquet_file_state: &SavedParquetFileState,
    files_to_delete: Vec<ParquetFile>,
    files_to_upgrade: Vec<ParquetFile>,
    file_params_to_create: Vec<ParquetFileParams>,
    target_level: CompactionLevel,
) -> Result<(Vec<ParquetFile>, Vec<ParquetFile>), DynError> {
    let partition_id = job.partition_id;
    let current_parquet_file_state =
        fetch_and_save_parquet_file_state(&components, partition_id).await;

    // Right now this only logs; in the future we might decide not to commit these changes
    let _ignore = components
        .changed_files_filter
        .apply(saved_parquet_file_state, &current_parquet_file_state);

    let created_ids = components
        .commit
        .commit(
            job,
            &files_to_delete,
            &files_to_upgrade,
            &file_params_to_create,
            target_level,
        )
        .await?;

    // Update created ids to their corresponding file params
    let created_file_params = file_params_to_create
        .into_iter()
        .zip(created_ids)
        .map(|(params, id)| ParquetFile::from_params(params, id))
        .collect::<Vec<_>>();

    // Update compaction_level for the files_to_upgrade
    let upgraded_files = files_to_upgrade
        .into_iter()
        .map(|mut f| {
            f.compaction_level = target_level;
            f
        })
        .collect::<Vec<_>>();

    Ok((created_file_params, upgraded_files))
}

// SINGLE_THREADED_COLUMN_COUNT is the number of columns requiring a partition be compacted single threaded.
const SINGLE_THREADED_COLUMN_COUNT: usize = 100;

// Determine how many permits must be acquired from the concurrency limiter semaphore
// based on the column count of this job and the total permits (concurrency).
fn compute_permits(
    total_permits: usize, // total number of permits (max concurrency)
    columns: usize,       // column count for this job
) -> u32 {
    if columns >= SINGLE_THREADED_COLUMN_COUNT {
        // this job requires all permits, forcing it to run by itself.
        return total_permits as u32;
    }

    // compute the share (linearly scaled) of total permits this job requires
    let share = columns as f64 / SINGLE_THREADED_COLUMN_COUNT as f64;

    // Square the share so the required permits is non-linearly scaled.
    // See test cases below for detail, but this makes it extra permissive of low column counts,
    // but still gets to single threaded by SINGLE_THREADED_COLUMN_COUNT.
    let permits = total_permits as f64 * share * share;

    if permits < 1.0 {
        return 1;
    }

    permits as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrency_limits() {
        assert_eq!(compute_permits(100, 1), 1); // 1 column still takes 1 permit
        assert_eq!(compute_permits(100, SINGLE_THREADED_COLUMN_COUNT / 10), 1); // 10% of the max column count takes 1% of total permits
        assert_eq!(
            compute_permits(100, SINGLE_THREADED_COLUMN_COUNT * 2 / 10),
            4
        ); // 20% of the max column count takes 4% of total permits
        assert_eq!(
            compute_permits(100, SINGLE_THREADED_COLUMN_COUNT * 3 / 10),
            9
        ); // 30% of the max column count takes 9% of total permits
        assert_eq!(
            compute_permits(100, SINGLE_THREADED_COLUMN_COUNT * 4 / 10),
            16
        ); // 40% of the max column count takes 16% of total permits
        assert_eq!(
            compute_permits(100, SINGLE_THREADED_COLUMN_COUNT * 5 / 10),
            25
        ); // 50% of the max column count takes 25% of total permits
        assert_eq!(
            compute_permits(100, SINGLE_THREADED_COLUMN_COUNT * 6 / 10),
            36
        ); // 60% of the max column count takes 36% of total permits
        assert_eq!(
            compute_permits(100, SINGLE_THREADED_COLUMN_COUNT * 7 / 10),
            49
        ); // 70% of the max column count takes 49% of total permits
        assert_eq!(
            compute_permits(100, SINGLE_THREADED_COLUMN_COUNT * 8 / 10),
            64
        ); // 80% of the max column count takes 64% of total permits
        assert_eq!(
            compute_permits(100, SINGLE_THREADED_COLUMN_COUNT * 9 / 10),
            81
        ); // 90% of the max column count takes 81% of total permits
        assert_eq!(compute_permits(100, SINGLE_THREADED_COLUMN_COUNT), 100); // 100% of the max column count takes 100% of total permits
        assert_eq!(compute_permits(100, 10000), 100); // huge column count takes exactly all permits (not more than the total)
    }
}
