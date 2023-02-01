use std::{future::Future, num::NonZeroUsize, sync::Arc, time::Duration};

use data_types::{CompactionLevel, ParquetFile, ParquetFileParams, PartitionId};
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
        files = components.files_filter.apply(files);

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

        let mut branches = components.divide_initial.divide(files_now);

        let mut files_next = files_later;
        while let Some(branch) = branches.pop() {
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
                let target_level = CompactionLevel::FileNonOverlapped;
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

            // upload files to real object store
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

            let ids = components
                .commit
                .commit(partition_id, &delete_ids, &create)
                .await;

            files_next.extend(
                create
                    .into_iter()
                    .zip(ids)
                    .map(|(params, id)| ParquetFile::from_params(params, id)),
            );
        }

        files = files_next;
    }
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
