use std::{num::NonZeroUsize, sync::Arc};

use data_types::{CompactionLevel, PartitionId};
use futures::{stream::FuturesOrdered, StreamExt, TryStreamExt};

use crate::{components::Components, partition_info::PartitionInfo};

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
pub async fn compact(partition_concurrency: NonZeroUsize, components: &Arc<Components>) {
    let partition_ids = components.partitions_source.fetch().await;

    futures::stream::iter(partition_ids)
        .map(|partition_id| {
            let components = Arc::clone(components);

            compact_partition(partition_id, components)
        })
        .buffer_unordered(partition_concurrency.get())
        .collect::<()>()
        .await;
}

async fn compact_partition(partition_id: PartitionId, components: Arc<Components>) {
    if let Err(e) = try_compact_partition(partition_id, Arc::clone(&components)).await {
        components
            .partition_error_sink
            .record(partition_id, &e.to_string())
            .await;
    }
}

type Error = Box<dyn std::error::Error + Send + Sync>;

async fn try_compact_partition(
    partition_id: PartitionId,
    components: Arc<Components>,
) -> Result<(), Error> {
    let files = components.partition_files_source.fetch(partition_id).await;
    let files = components.files_filter.apply(files);
    let delete_ids = files.iter().map(|f| f.id).collect::<Vec<_>>();

    if !components
        .partition_filter
        .apply(partition_id, &files)
        .await
    {
        return Ok(());
    }

    // TODO: only read partition, table and its schema info the first time and cache them
    // Get info for the partition
    let partition = components
        .partitions_source
        .fetch_by_id(partition_id)
        .await
        .ok_or_else::<Error, _>(|| String::from("Cannot find partition info").into())?;

    let table = components
        .tables_source
        .fetch(files[0].table_id)
        .await
        .ok_or_else::<Error, _>(|| String::from("Cannot find table").into())?;

    // TODO: after we have catalog funciton to read table schema, we should use it
    // and avoid reading namespace schema
    let namespace = components
        .namespaces_source
        .fetch_by_id(table.namespace_id)
        .await
        .ok_or_else::<Error, _>(|| String::from("Cannot find namespace").into())?;

    let namespace_schema = components
        .namespaces_source
        .fetch_schema_by_id(table.namespace_id)
        .await
        .ok_or_else::<Error, _>(|| String::from("Cannot find namespace schema").into())?;

    let table_schema = namespace_schema
        .tables
        .get(&table.name)
        .ok_or_else::<Error, _>(|| String::from("Cannot find table schema").into())?;

    let partition_info = Arc::new(PartitionInfo {
        partition_id,
        namespace_id: table.namespace_id,
        namespace_name: namespace.name,
        table: Arc::new(table),
        table_schema: Arc::new(table_schema.clone()),
        sort_key: partition.sort_key(),
        partition_key: partition.partition_key,
    });

    // TODO: Need a wraper funtion to:
    //    . split files into L0, L1 and L2
    //    . identify right files for hot/cold compaction
    //    . filter right amount of files
    //    . compact many steps hot/cold (need more thinking)
    let target_level = CompactionLevel::FileNonOverlapped;
    let plan = components
        .df_planner
        .plan(files, Arc::clone(&partition_info), target_level)
        .await?;
    let streams = components.df_plan_exec.exec(plan);
    let create = streams
        .into_iter()
        .map(|stream| {
            components
                .parquet_file_sink
                .store(stream, Arc::clone(&partition_info), target_level)
        })
        // NB: FuturesOrdered allows the futures to run in parallel
        .collect::<FuturesOrdered<_>>()
        // Discard the streams that resulted in empty output / no file uploaded
        // to the object store.
        .try_filter_map(|v| futures::future::ready(Ok(v)))
        // Collect all the persisted parquet files together.
        .try_collect::<Vec<_>>()
        .await?;

    components.commit.commit(&delete_ids, &create).await;

    Ok(())
}
