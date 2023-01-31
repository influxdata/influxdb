//! Config-related stuff.
use std::{collections::HashSet, num::NonZeroUsize, sync::Arc, time::Duration};

use backoff::{Backoff, BackoffConfig};
use data_types::{PartitionId, ShardId, ShardIndex};
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use iox_time::TimeProvider;
use parquet_file::storage::ParquetStorage;

/// Config to set up a compactor.
#[derive(Debug, Clone)]
pub struct Config {
    /// Shard Id
    pub shard_id: ShardId,

    /// Metric registry.
    pub metric_registry: Arc<metric::Registry>,

    /// Central catalog.
    pub catalog: Arc<dyn Catalog>,

    /// Store holding the actual parquet files.
    pub parquet_store_real: ParquetStorage,

    /// Store holding temporary files.
    pub parquet_store_scratchpad: ParquetStorage,

    /// Executor.
    pub exec: Arc<Executor>,

    /// Time provider.
    pub time_provider: Arc<dyn TimeProvider>,

    /// Backoff config
    pub backoff_config: BackoffConfig,

    /// Number of partitions that should be compacted in parallel.
    ///
    /// This should usually be larger than the compaction job concurrency since one partition can spawn multiple compaction jobs.
    pub partition_concurrency: NonZeroUsize,

    /// Number of concurrent compaction jobs.
    ///
    /// This should usually be smaller than the partition concurrency since one partition can spawn multiple compaction jobs.
    pub job_concurrency: NonZeroUsize,

    /// Number of jobs PER PARTITION that move files in and out of the scratchpad.
    pub partition_scratchpad_concurrency: NonZeroUsize,

    /// Partitions with recent created files this recent duration are selected for compaction.
    pub partition_threshold: Duration,

    /// Desired max size of compacted parquet files
    /// It is a target desired value than a guarantee
    pub max_desired_file_size_bytes: u64,

    /// Percentage of desired max file size.
    /// If the estimated compacted result is too small, no need to split it.
    /// This percentage is to determine how small it is:
    ///    < percentage_max_file_size * max_desired_file_size_bytes:
    /// This value must be between (0, 100)
    pub percentage_max_file_size: u16,

    /// Split file percentage
    /// If the estimated compacted result is neither too small nor too large, it will be split
    /// into 2 files determined by this percentage.
    ///    . Too small means: < percentage_max_file_size * max_desired_file_size_bytes
    ///    . Too large means: > max_desired_file_size_bytes
    ///    . Any size in the middle will be considered neither too small nor too large
    /// This value must be between (0, 100)
    pub split_percentage: u16,

    /// Maximum duration of the per-partition compaction task.
    pub partition_timeout: Duration,

    /// Filter partitions to the given set of IDs.
    ///
    /// This is mostly useful for debugging.
    pub partition_filter: Option<HashSet<PartitionId>>,

    /// Shadow mode.
    ///
    /// This will NOT write / commit any output to the object store or catalog.
    ///
    /// This is mostly useful for debugging.
    pub shadow_mode: bool,

    /// Ignores "partition marked w/ error and shall be skipped" entries in the catalog.
    ///
    /// This is mostly useful for debugging.
    pub ignore_partition_skip_marker: bool,

    /// Maximum number of input files per partition. If there are more files, we ignore the partition (for now) as a
    /// self-protection mechanism.
    pub max_input_files_per_partition: usize,

    /// Maximum input bytes (in parquet) per partition. If there is more data, we ignore the partition (for now) as a
    /// self-protection mechanism.
    pub max_input_parquet_bytes_per_partition: usize,

    /// Shard config (if sharding should be enabled).
    pub shard_config: Option<ShardConfig>,
}

impl Config {
    /// Fetch shard ID.
    ///
    /// This is likely required to construct a [`Config`] object.
    pub async fn fetch_shard_id(
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
        topic_name: String,
        shard_index: i32,
    ) -> ShardId {
        // Get shardId from topic and shard_index
        // Fetch topic
        let topic = Backoff::new(&backoff_config)
            .retry_all_errors("topic_of_given_name", || async {
                catalog
                    .repositories()
                    .await
                    .topics()
                    .get_by_name(topic_name.as_str())
                    .await
            })
            .await
            .expect("retry forever");

        if topic.is_none() {
            panic!("Topic {} not found", topic_name);
        }
        let topic = topic.unwrap();

        // Fetch shard
        let shard = Backoff::new(&backoff_config)
            .retry_all_errors("sahrd_of_given_index", || async {
                catalog
                    .repositories()
                    .await
                    .shards()
                    .get_by_topic_id_and_shard_index(topic.id, ShardIndex::new(shard_index))
                    .await
            })
            .await
            .expect("retry forever");

        match shard {
            Some(shard) => shard.id,
            None => {
                panic!(
                    "Topic {} and Shard Index {} not found",
                    topic_name, shard_index
                )
            }
        }
    }
}

/// Shard config.
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)]
pub struct ShardConfig {
    /// Number of shards.
    pub n_shards: usize,

    /// Shard ID.
    ///
    /// Starts as 0 and must be smaller than the number of shards.
    pub shard_id: usize,
}
