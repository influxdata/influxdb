use std::sync::Arc;

use data_types::{ParquetFile, PartitionId};
use futures::StreamExt;
use iox_catalog::interface::Catalog;
use observability_deps::tracing::{error, info};
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};

use crate::{compact::compact_files, config::Config, rules::Rules};

pub async fn compact(config: &Config, rules: &Arc<Rules>) {
    let partition_ids = get_partition_ids(&config.catalog).await;
    // TODO: implementing ID-based sharding / hash-partitioning so we can run multiple compactors in parallel
    let partition_ids = randomize_partition_order(partition_ids, 1234);

    futures::stream::iter(partition_ids)
        .map(|partition_id| {
            let config = config.clone();
            let rules = Arc::clone(rules);

            async move {
                let files = get_parquet_files(&config.catalog, partition_id).await;

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

/// Get partiion IDs from catalog.
///
/// This method performs retries.
///
/// This should only perform basic filtering. It MUST NOT inspect individual parquet files.
async fn get_partition_ids(_catalog: &Arc<dyn Catalog>) -> Vec<PartitionId> {
    // TODO: get partition IDs from catalog, wrapped by retry
    vec![]
}

/// Get parquet files for given partition.
///
/// This method performs retries.
async fn get_parquet_files(
    _catalog: &Arc<dyn Catalog>,
    _partition: PartitionId,
) -> Vec<ParquetFile> {
    // TODO: get files from from catalog, wrapped by retry
    vec![]
}

fn randomize_partition_order(mut partitions: Vec<PartitionId>, seed: u64) -> Vec<PartitionId> {
    let mut rng = StdRng::seed_from_u64(seed);
    partitions.shuffle(&mut rng);
    partitions
}
