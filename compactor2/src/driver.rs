use std::{future::Future, num::NonZeroUsize, sync::Arc, time::Duration};

use data_types::{CompactionLevel, ParquetFile, ParquetFileId, ParquetFileParams, PartitionId};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{stream::FuturesOrdered, StreamExt, TryFutureExt, TryStreamExt};
use iox_time::Time;
use parquet_file::ParquetFilePath;
use tracker::InstrumentedAsyncSemaphore;

use crate::{
    components::{scratchpad::Scratchpad, Components},
    error::DynError,
    partition_info::PartitionInfo,
};

// TODO: modify this comments accordingly as we go
// Currently, we only compact files of level_n with level_n+1 and produce level_n+1 files,
// and with the strictly design that:
//    . Level-0 files can overlap with any files.
//    . Level-N files (N > 0) cannot overlap with any files in the same level.
//    . For Level-0 files, we always pick the smaller `created_at` files to compact (with
//      each other and overlapped L1 files) first.
//    . Level-N+1 files are results of compacting Level-N and/or Level-N+1 files, their `created_at`
//      can be after the `created_at` of other Level-N files but they may include data loaded before
//      the other Level-N files. Hence we should never use `created_at` of Level-N+1 files to order
//      them with Level-N files.
//    . We can only compact different sets of files of the same partition concurrently into the same target_level.
pub async fn compact(
    partition_concurrency: NonZeroUsize,
    partition_timeout: Duration,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
    components: &Arc<Components>,
) {
    let partition_ids = components.partitions_source.fetch().await;

    futures::stream::iter(partition_ids)
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
//
// Example: Partition has 7 files: f1, f2, f3, f4, f5, f6, f7
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

    // fetch partition info only if we need it
    let mut lazy_partition_info = None;

    loop {
        // TODO: I do not think we need this in version 1 becasue we do not generate L2 files
        // So either remove this or move it inside the partition filter for Naive version
        // This filter will stop HotCold version from working correctly
        files = components.files_filter.apply(files);

        // This is the stop condition which will be different for different version of compaction
        // and describe where the filter is created at version_specific_partition_filters function
        if !components
            .partition_filter
            .apply(partition_id, &files)
            .await?
        {
            return Ok(());
        }

        // fetch partition info
        if lazy_partition_info.is_none() {
            lazy_partition_info = Some(fetch_partition_info(partition_id, &components).await?);
        }
        let partition_info = lazy_partition_info.as_ref().expect("just fetched");

        let (files_now, files_later) = components.round_split.split(files);

        // Each branch must not overlap with each other
        let mut branches = components.divide_initial.divide(files_now);

        let mut files_next = files_later;
        while let Some(branch) = branches.pop() {
            let target_level = CompactionLevel::FileNonOverlapped;
            let delete_ids = branch.iter().map(|f| f.id).collect::<Vec<_>>();

            // compute max_l0_created_at
            let max_l0_created_at = branch
                .iter()
                .map(|f| f.max_l0_created_at)
                .max()
                .expect("max_l0_created_at should have value");

            // stage files
            let input_paths: Vec<ParquetFilePath> = branch.iter().map(|f| f.into()).collect();
            let input_uuids_inpad = scratchpad_ctx.load_to_scratchpad(&input_paths).await;
            let branch_inpad: Vec<_> = branch
                .into_iter()
                .zip(input_uuids_inpad)
                .map(|(f, uuid)| ParquetFile {
                    object_store_id: uuid,
                    ..f
                })
                .collect();

            let create = {
                // draw semaphore BEFORE creating the DataFusion plan and drop it directly AFTER finishing the
                // DataFusion computation (but BEFORE doing any additional external IO).
                //
                // We guard the DataFusion planning (that doesn't perform any IO) via the semaphore as well in case
                // DataFusion ever starts to pre-allocate buffers during the physical planning. To the best of our
                // knowledge, this is currently (2023-01-25) not the case but if this ever changes, then we are prepared.
                let _permit = job_semaphore
                    .acquire(None)
                    .await
                    .expect("semaphore not closed");

                // TODO: Need a wraper funtion to:
                //    . split files into L0, L1 and L2
                //    . identify right files for hot/cold compaction
                //    . filter right amount of files
                //    . compact many steps hot/cold (need more thinking)
                let plan = components
                    .df_planner
                    .plan(branch_inpad, Arc::clone(partition_info), target_level)
                    .await?;
                let streams = components.df_plan_exec.exec(plan);
                let job = stream_into_file_sink(
                    streams,
                    Arc::clone(partition_info),
                    target_level,
                    max_l0_created_at.into(),
                    Arc::clone(&components),
                );

                // TODO: react to OOM and try to divide branch
                job.await?
            };

            // The minimum to add in this PR for reviewers to go with the idea
            // TODO: refactor the code above to create these values
            let higher_level_files = vec![];
            let compact_result = CompactResult {
                non_overlapping_files: vec![],
                upgraded_files: vec![],
                created_file_params: create,
                deleted_ids: delete_ids,
            };

            // upload files to real object store
            let create = compact_result.created_file_params;
            let output_files: Vec<ParquetFilePath> = create.iter().map(|p| p.into()).collect();
            let output_uuids = scratchpad_ctx.make_public(&output_files).await;
            let create: Vec<_> = create
                .into_iter()
                .zip(output_uuids)
                .map(|(f, uuid)| ParquetFileParams {
                    object_store_id: uuid,
                    ..f
                })
                .collect();

            // clean scratchpad
            scratchpad_ctx.clean_from_scratchpad(&input_paths).await;

            // Update the catalog to refelct the newly created files, soft delete the compacted files and
            // update the the upgraded files
            let upgraded_ids = compact_result
                .upgraded_files
                .iter()
                .map(|f| f.id)
                .collect::<Vec<_>>();
            let deleted_ids = compact_result.deleted_ids;
            let create_ids = components
                .commit
                .commit(
                    partition_id,
                    &deleted_ids,
                    &upgraded_ids,
                    &create,
                    target_level,
                )
                .await;

            // Extend created files, files_higher_level, overlapped_files and upgraded_files to files_next
            files_next.extend(
                create
                    .into_iter()
                    .zip(create_ids)
                    .map(|(params, id)| ParquetFile::from_params(params, id)),
            );
            files_next.extend(higher_level_files);
            files_next.extend(compact_result.upgraded_files);
            files_next.extend(compact_result.non_overlapping_files);
        }

        files = files_next;
    }
}

/// Struct to store result of compaction results
struct CompactResult {
    /// files of target_level that do not overlap with (target_level - 1)
    non_overlapping_files: Vec<ParquetFile>,
    /// files of (target_level -1) that will be upgraded to target_level
    upgraded_files: Vec<ParquetFile>,
    /// result of compaction of [files_compact - (files_non_overlap + files_upgrade)]
    created_file_params: Vec<ParquetFileParams>,
    /// ids of files in [files_compact - (files_non_overlap + files_upgrade)]
    deleted_ids: Vec<ParquetFileId>,
}

async fn fetch_partition_info(
    partition_id: PartitionId,
    components: &Arc<Components>,
) -> Result<Arc<PartitionInfo>, DynError> {
    // TODO: only read partition, table and its schema info the first time and cache them
    // Get info for the partition
    let partition = components
        .partition_source
        .fetch_by_id(partition_id)
        .await
        .ok_or_else::<DynError, _>(|| String::from("Cannot find partition info").into())?;

    let table = components
        .tables_source
        .fetch(partition.table_id)
        .await
        .ok_or_else::<DynError, _>(|| String::from("Cannot find table").into())?;

    // TODO: after we have catalog funciton to read table schema, we should use it
    // and avoid reading namespace schema
    let namespace = components
        .namespaces_source
        .fetch_by_id(table.namespace_id)
        .await
        .ok_or_else::<DynError, _>(|| String::from("Cannot find namespace").into())?;

    let namespace_schema = components
        .namespaces_source
        .fetch_schema_by_id(table.namespace_id)
        .await
        .ok_or_else::<DynError, _>(|| String::from("Cannot find namespace schema").into())?;

    let table_schema = namespace_schema
        .tables
        .get(&table.name)
        .ok_or_else::<DynError, _>(|| String::from("Cannot find table schema").into())?;

    Ok(Arc::new(PartitionInfo {
        partition_id,
        namespace_id: table.namespace_id,
        namespace_name: namespace.name,
        table: Arc::new(table),
        table_schema: Arc::new(table_schema.clone()),
        sort_key: partition.sort_key(),
        partition_key: partition.partition_key,
    }))
}

fn stream_into_file_sink(
    streams: Vec<SendableRecordBatchStream>,
    partition_info: Arc<PartitionInfo>,
    target_level: CompactionLevel,
    max_l0_created_at: Time,
    components: Arc<Components>,
) -> impl Future<Output = Result<Vec<ParquetFileParams>, DynError>> {
    streams
        .into_iter()
        .map(move |stream| {
            let components = Arc::clone(&components);
            let partition_info = Arc::clone(&partition_info);
            async move {
                components
                    .parquet_file_sink
                    .store(stream, partition_info, target_level, max_l0_created_at)
                    .await
            }
        })
        // NB: FuturesOrdered allows the futures to run in parallel
        .collect::<FuturesOrdered<_>>()
        // Discard the streams that resulted in empty output / no file uploaded
        // to the object store.
        .try_filter_map(|v| futures::future::ready(Ok(v)))
        // Collect all the persisted parquet files together.
        .try_collect::<Vec<_>>()
        .map_err(|e| Box::new(e) as _)
}
