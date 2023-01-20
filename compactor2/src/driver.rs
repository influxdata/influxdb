use std::sync::Arc;

use data_types::CompactionLevel;
use futures::StreamExt;
use observability_deps::tracing::info;

use crate::{
    components::{
        compact::{compact_files::compact_files, partition::PartitionInfo},
        Components,
    },
    config::Config,
};

pub async fn compact(config: &Config, components: &Arc<Components>) {
    let partition_ids = components.partitions_source.fetch().await;

    futures::stream::iter(partition_ids)
        .map(|partition_id| {
            let config = config.clone();
            let components = Arc::clone(components);

            async move {
                let files = components.partition_files_source.fetch(partition_id).await;
                let files = components.files_filter.apply(files);

                if !components.partition_filter.apply(&files) {
                    return;
                }

                // TODO: only read table and namespace info the first time and cache them
                // Get info for the partition
                let table = components.tables_source.fetch(files[0].table_id).await;
                let namespace_schema = components
                    .namespaces_source
                    .fetch(files[0].namespace_id)
                    .await;
                let partition = components.partitions_source.fetch_by_id(partition_id).await;
                let table_schema = namespace_schema
                    .as_ref()
                    .unwrap()
                    .tables
                    .get(&table.as_ref().unwrap().name);

                if table.is_none()
                    || namespace_schema.is_none()
                    || partition.is_none()
                    || table_schema.is_none()
                {
                    // Since we retry reading the catalog and cannot find enough needed info,
                    // this partition won't be able to get compacted
                    components
                        .partition_error_sink
                        .record(
                            partition_id,
                            "Cannot find table or table schema or partition info",
                        )
                        .await;
                    return;
                }

                let partition = partition.unwrap();
                let partition_info = PartitionInfo::new(
                    partition_id,
                    files[0].namespace_id,
                    Arc::new(table.unwrap()),
                    Arc::new(table_schema.unwrap().clone()),
                    partition.sort_key(),
                    partition.partition_key,
                );

                let files = Arc::new(files);

                // TODO: Need a wraper funtion to:
                //    . split files into L0, L1 and L2
                //    . identify right files for hot/cold compaction
                //    . filter right amount of files
                //    . compact many steps hot/cold (need more thinking)
                // This function currently assumes input files are all L0 and L1
                match compact_files(
                    Arc::clone(&files),
                    Arc::new(partition_info),
                    Arc::new(config),
                    CompactionLevel::FileNonOverlapped,
                )
                .await
                {
                    Ok(create) => {
                        let delete_ids = files.iter().map(|f| f.id).collect::<Vec<_>>();
                        components.commit.commit(&delete_ids, &create).await;
                    }
                    // TODO: remove this stop-gap
                    Err(crate::components::compact::compact_files::Error::NotImplemented) => {
                        info!("not implemented");
                    }
                    #[allow(unreachable_patterns)] // TODO: remove this when stop-gap is removed
                    Err(e) => {
                        components
                            .partition_error_sink
                            .record(partition_id, &e.to_string())
                            .await;
                    }
                }
            }
        })
        .buffer_unordered(config.partition_concurrency.get())
        .collect::<()>()
        .await;
}
