use std::sync::Arc;

use backoff::Backoff;
use data_types::{ParquetFile, PartitionId};
use futures::StreamExt;
use observability_deps::tracing::{error, info};
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};

use crate::{compact::compact_files, config::Config, rules::Rules};

pub async fn compact(config: &Config, rules: &Arc<Rules>) {
    let partition_ids = get_partition_ids(config).await;
    // TODO: implementing ID-based sharding / hash-partitioning so we can run multiple compactors in parallel
    let partition_ids = randomize_partition_order(partition_ids, 1234);

    futures::stream::iter(partition_ids)
        .map(|partition_id| {
            let config = config.clone();
            let rules = Arc::clone(rules);

            async move {
                let files = get_parquet_files(&config, partition_id).await;

                let files = files
                    .into_iter()
                    .filter(|file| rules.file_filters.iter().all(|filter| filter.apply(file)))
                    .collect::<Vec<_>>();

                if !rules
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

/// Get partition IDs from catalog.
///
/// This method performs retries.
///
/// This should only perform basic filtering. It MUST NOT inspect individual parquet files.
async fn get_partition_ids(config: &Config) -> Vec<PartitionId> {
    let time_minutes_ago = config
        .time_provider
        .minutes_ago(config.partition_minute_threshold);
    let partitions = Backoff::new(&config.backoff_config)
        .retry_all_errors("partitions_to_compact", || async {
            config
                .catalog
                .repositories()
                .await
                .partitions()
                .partitions_to_compact(time_minutes_ago.into())
                .await
        })
        .await
        .expect("retry forever");

    partitions
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

fn randomize_partition_order(mut partitions: Vec<PartitionId>, seed: u64) -> Vec<PartitionId> {
    let mut rng = StdRng::seed_from_u64(seed);
    partitions.shuffle(&mut rng);
    partitions
}
