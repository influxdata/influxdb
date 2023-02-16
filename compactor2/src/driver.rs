use std::{collections::VecDeque, num::NonZeroUsize, sync::Arc, time::Duration};

use data_types::{CompactionLevel, ParquetFile, ParquetFileParams, PartitionId};
use futures::StreamExt;
use observability_deps::tracing::info;
use parquet_file::ParquetFilePath;
use tracker::InstrumentedAsyncSemaphore;

use crate::{
    components::{scratchpad::Scratchpad, Components},
    error::DynError,
    partition_info::PartitionInfo,
};

/// Tries to compact all eligible partitions, up to
/// partition_concurrency at a time.
pub async fn compact(
    partition_concurrency: NonZeroUsize,
    partition_timeout: Duration,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
    components: &Arc<Components>,
) {
    components
        .partition_stream
        .stream()
        .map(|partition_id| {
            let components = Arc::clone(components);

            compact_partition(
                partition_id,
                partition_timeout,
                Arc::clone(&job_semaphore),
                components,
            )
        })
        .buffer_unordered(partition_concurrency.get())
        .collect::<()>()
        .await;
}

async fn compact_partition(
    partition_id: PartitionId,
    partition_timeout: Duration,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
    components: Arc<Components>,
) {
    info!(partition_id = partition_id.get(), "compact partition",);
    let mut scratchpad = components.scratchpad_gen.pad();

    let res = tokio::time::timeout(
        partition_timeout,
        try_compact_partition(
            partition_id,
            job_semaphore,
            Arc::clone(&components),
            scratchpad.as_mut(),
        ),
    )
    .await;
    let res = match res {
        Ok(res) => res,
        Err(e) => Err(Box::new(e) as _),
    };
    components
        .partition_done_sink
        .record(partition_id, res)
        .await;

    scratchpad.clean().await;
    info!(partition_id = partition_id.get(), "compacted partition",);
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
/// -----------------------------------------------------------------------------------------------------
/// VERSION 1 - Naive (implemented here): One round, one branch
///
/// . All L0 and L1 files will be compacted into one or two files if the size > estimated desired max size
/// . We do not generate L2 files in this version.
///
///
/// Example: same Partition has 7 files: f1, f2, f3, f4, f5, f6, f7
///  Input:
///          |--f1--|               |----f3----|  |-f4-||-f5-||-f7-|
///               |------f2----------|                   |--f6--|
/// Output:
///          |---------f8-------------------------------|----f9----|
///
/// -----------------------------------------------------------------------------------------------------
/// VERSION 2 - HotCold (in-progress and will be adding in here under feature flag and new components & filters)
///   . Mutiple rounds, each round has 1 branch
///   . Each branch will compact files lowest level (aka initial level) into its next level (aka target level):
///      - hot:  (L0s & L1s) to L1s   if there are L0s
///      - cold: (L1s & L2s) to L2s   if no L0s
///   . Each branch does find non-overlaps and upgragde files to avoid unecessary recompacting.
///     The actually split files:
///      1. files_higher_level: do not compact these files because they are already higher than target level
///          . Value: nothing for hot and L2s for cold
///      2. files_non_overlap: do not compact these target-level files because they are not overlapped
///          with the initial-level files
///          . Value: non-overlapped L1s for hot and non-overlapped L2s for cold
///          . Definition of overlaps is defined in the split non-overlapped files function
///      3. files_upgrade: upgrade this initial-level files to target level because they are not overlap with
///          any target-level and initial-level files and large enough (> desired max size)
///          . value: non-overlapped L0s for hot and non-overlapped L1s for cold
///      4. files_compact: the rest of the files that must be compacted
///          . Value: (L0s & L1s) for hot and (L1s & L2s) for cold
///
/// Example: 4 files: two L0s, two L1s and one L2
///  Input:
///                                      |-L0.1-||------L0.2-------|
///                  |-----L1.1-----||--L1.2--|
///     |----L2.1-----|
///
///  - Round 1: There are L0s, let compact L0s with L1s. But let split them first:
///    . files_higher_level: L2.1
///    . files_non_overlap: L1.1
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
    partition_id: PartitionId,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
    components: Arc<Components>,
    scratchpad_ctx: &mut dyn Scratchpad,
) -> Result<(), DynError> {
    let mut files = components.partition_files_source.fetch(partition_id).await;
    let partition_info = components.partition_info_source.fetch(partition_id).await?;

    // loop for each "Round", consider each file in the partition
    loop {
        let round_info = components
            .round_info_source
            .calculate(&partition_info, &files)
            .await?;

        files = components.files_filter.apply(files);

        // This is the stop condition which will be different for different version of compaction
        // and describe where the filter is created at version_specific_partition_filters function
        if !components
            .partition_filter
            .apply(&partition_info, &files)
            .await?
        {
            return Ok(());
        }

        let (files_now, files_later) = components.round_split.split(files, round_info.as_ref());

        // Each branch must not overlap with each other
        let mut branches = components
            .divide_initial
            .divide(files_now, round_info.as_ref())
            .into_iter()
            .collect::<VecDeque<_>>();

        let mut files_next = files_later;
        // loop for each "Branch"
        while let Some(branch) = branches.pop_front() {
            let input_paths: Vec<ParquetFilePath> =
                branch.iter().map(ParquetFilePath::from).collect();

            // Identify the target level and files that should be
            // compacted together, upgraded, and kept for next round of
            // compaction
            let file_classification =
                components
                    .file_classifier
                    .classify(&partition_info, &round_info, branch);

            // Cannot run this plan and skip this partition because of over limit of input num_files or size.
            // The partition_resource_limit_filter will throw an error if one of the limits hit and will lead
            // to the partition is added to the `skipped_compactions` catalog table for us to not bother
            // compacting it again.
            // TODO: After https://github.com/influxdata/idpe/issues/17090 is iplemented (aka V3), we will
            //      split files to smaller branches and aslo compact L0s into fewer L0s to deal with all kinds
            //      of conidtions even with limited resource. Then we will remove this resrouce limit check.
            if !components
                .partition_resource_limit_filter
                .apply(&partition_info, &file_classification.files_to_compact)
                .await?
            {
                return Ok(());
            }

            // Compact
            let created_file_params = run_compaction_plan(
                &file_classification.files_to_compact,
                &partition_info,
                &components,
                file_classification.target_level,
                Arc::clone(&job_semaphore),
                scratchpad_ctx,
            )
            .await?;

            // upload files to real object store
            let created_file_params =
                upload_files_to_object_store(created_file_params, scratchpad_ctx).await;

            // clean scratchpad
            scratchpad_ctx.clean_from_scratchpad(&input_paths).await;

            // Update the catalog to reflect the newly created files, soft delete the compacted files and
            // update the upgraded files
            let (created_files, upgraded_files) = update_catalog(
                Arc::clone(&components),
                partition_id,
                file_classification.files_to_compact,
                file_classification.files_to_upgrade,
                created_file_params,
                file_classification.target_level,
            )
            .await;

            // Extend created files, upgraded files and files_to_keep to files_next
            files_next.extend(created_files);
            files_next.extend(upgraded_files);
            files_next.extend(file_classification.files_to_keep);
        }

        files = files_next;
    }
}

/// Compact `files` into a new parquet file of the the given target_level
async fn run_compaction_plan(
    files: &[ParquetFile],
    partition_info: &Arc<PartitionInfo>,
    components: &Arc<Components>,
    target_level: CompactionLevel,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
    scratchpad_ctx: &mut dyn Scratchpad,
) -> Result<Vec<ParquetFileParams>, DynError> {
    if files.is_empty() {
        return Ok(vec![]);
    }

    // stage files
    let input_paths: Vec<ParquetFilePath> = files.iter().map(|f| f.into()).collect();
    let input_uuids_inpad = scratchpad_ctx.load_to_scratchpad(&input_paths).await;
    let branch_inpad: Vec<_> = files
        .iter()
        .zip(input_uuids_inpad)
        .map(|(f, uuid)| ParquetFile {
            object_store_id: uuid,
            ..f.clone()
        })
        .collect();

    let plan_ir =
        components
            .ir_planner
            .plan(branch_inpad, Arc::clone(partition_info), target_level);

    let create = {
        // draw semaphore BEFORE creating the DataFusion plan and drop it directly AFTER finishing the
        // DataFusion computation (but BEFORE doing any additional external IO).
        //
        // We guard the DataFusion planning (that doesn't perform any IO) via the semaphore as well in case
        // DataFusion ever starts to pre-allocate buffers during the physical planning. To the best of our
        // knowledge, this is currently (2023-01-25) not the case but if this ever changes, then we are prepared.
        let permit = job_semaphore
            .acquire(None)
            .await
            .expect("semaphore not closed");
        info!(
            partition_id = partition_info.partition_id.get(),
            "job semaphore acquired",
        );

        let plan = components
            .df_planner
            .plan(&plan_ir, Arc::clone(partition_info))
            .await?;
        let streams = components.df_plan_exec.exec(plan);
        let job = components.parquet_files_sink.stream_into_file_sink(
            streams,
            Arc::clone(partition_info),
            target_level,
            &plan_ir,
        );

        // TODO: react to OOM and try to divide branch
        let res = job.await;

        drop(permit);
        info!(
            partition_id = partition_info.partition_id.get(),
            "job semaphore released",
        );

        res?
    };

    Ok(create)
}

async fn upload_files_to_object_store(
    created_file_params: Vec<ParquetFileParams>,
    scratchpad_ctx: &mut dyn Scratchpad,
) -> Vec<ParquetFileParams> {
    // Ipload files to real object store
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

/// Update the catalog to create, soft delete and upgrade corresponding given input
/// to provided target level
/// Return created and upgraded files
async fn update_catalog(
    components: Arc<Components>,
    partition_id: PartitionId,
    files_to_delete: Vec<ParquetFile>,
    files_to_upgrade: Vec<ParquetFile>,
    file_params_to_create: Vec<ParquetFileParams>,
    target_level: CompactionLevel,
) -> (Vec<ParquetFile>, Vec<ParquetFile>) {
    let created_ids = components
        .commit
        .commit(
            partition_id,
            &files_to_delete,
            &files_to_upgrade,
            &file_params_to_create,
            target_level,
        )
        .await;

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

    (created_file_params, upgraded_files)
}
