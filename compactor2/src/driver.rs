use std::sync::Arc;

use backoff::Backoff;
use data_types::{ParquetFile, PartitionId};
use futures::StreamExt;
use observability_deps::tracing::{error, info};

use crate::{compact::compact_files, components::Components, config::Config};

pub async fn compact(config: &Config, components: &Arc<Components>) {
    let partition_ids = components.partitions_source.fetch().await;

    futures::stream::iter(partition_ids)
        .map(|partition_id| {
            let config = config.clone();
            let components = Arc::clone(components);

            async move {
                let files = get_parquet_files(&config, partition_id).await;

                let files = files
                    .into_iter()
                    .filter(|file| {
                        components
                            .file_filters
                            .iter()
                            .all(|filter| filter.apply(file))
                    })
                    .collect::<Vec<_>>();

                if !components
                    .partition_filters
                    .iter()
                    .all(|filter| filter.apply(&files))
                {
                    return;
                }

                if files.is_empty() {
                    return;
                }

                if let Err(e) = compact_files(&files, &config.catalog).await {
                    error!(
                        %e,
                        partition_id=partition_id.get(),
                        "Error while compacting partition",
                    );
                    // TODO: mark partition as "cannot compact"
                    return;
                }
                info!(
                    input_size = files.iter().map(|f| f.file_size_bytes).sum::<i64>(),
                    input_files = files.len(),
                    partition_id = partition_id.get(),
                    "Compacted partition",
                );
            }
        })
        .buffer_unordered(config.partition_concurrency.get())
        .collect::<()>()
        .await;
}

/// Get parquet files for given partition.
///
/// This method performs retries.
async fn get_parquet_files(config: &Config, partition: PartitionId) -> Vec<ParquetFile> {
    let parquet_files = Backoff::new(&config.backoff_config)
        .retry_all_errors("parquet_files_of_given_partition", || async {
            config
                .catalog
                .repositories()
                .await
                .parquet_files()
                .list_by_partition_not_to_delete(partition)
                .await
        })
        .await
        .expect("retry forever");

    parquet_files
}
