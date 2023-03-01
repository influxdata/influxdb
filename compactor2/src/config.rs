//! Config-related stuff.
use std::{collections::HashSet, fmt::Display, num::NonZeroUsize, sync::Arc, time::Duration};

use backoff::{Backoff, BackoffConfig};
use data_types::{PartitionId, ShardId, ShardIndex};
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use iox_time::TimeProvider;
use parquet_file::storage::ParquetStorage;

use crate::components::{commit::CommitWrapper, parquet_files_sink::ParquetFilesSink};

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

    /// Source of partitions to consider for comapction.
    pub partitions_source: PartitionsSourceConfig,

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

    /// Maximum input bytes (in parquet) per partition. If there is more data, we ignore the partition (for now) as a
    /// self-protection mechanism.
    pub max_input_parquet_bytes_per_partition: usize,

    /// Shard config (if sharding should be enabled).
    pub shard_config: Option<ShardConfig>,

    /// Minimum number of L1 files to compact to L2
    /// This is to prevent too many small files
    pub min_num_l1_files_to_compact: usize,

    /// Only process all discovered partitions once.
    pub process_once: bool,

    /// Simulate compactor w/o any object store interaction. No parquet
    /// files will be read or written.
    ///
    /// This will still use the catalog
    ///
    /// This is useful for testing.
    pub simulate_without_object_store: bool,

    /// Use the provided [`ParquetFilesSink`] to create parquet files
    /// (used for testing)
    pub parquet_files_sink_override: Option<Arc<dyn ParquetFilesSink>>,

    /// Optionally wrap the `Commit` instance
    ///
    /// This is mostly used for testing
    pub commit_wrapper: Option<Arc<dyn CommitWrapper>>,

    /// Ensure that ALL errors (including object store errors) result in "skipped" partitions.
    ///
    /// This is mostly useful for testing.
    pub all_errors_are_fatal: bool,

    /// Maximum number of columns in the table of a partition that will be considered get comapcted
    /// If there are more columns, the partition will be skipped
    /// This is to prevent too many columns in a table
    pub max_num_columns_per_table: usize,

    /// max number of files per compaction plan
    pub max_num_files_per_plan: usize,
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
            panic!("Topic {topic_name} not found");
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
                panic!("Topic {topic_name} and Shard Index {shard_index} not found")
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

/// Partitions source config.
#[derive(Debug, Clone)]
pub enum PartitionsSourceConfig {
    /// Use the catalog to determine which partitions have recently received writes.
    CatalogRecentWrites,

    /// Use all partitions from the catalog.
    ///
    /// This does NOT consider if/when a partition received any writes.
    CatalogAll,

    /// Use a fixed set of partitions.
    ///
    /// This is mostly useful for debugging.
    Fixed(HashSet<PartitionId>),
}

impl Display for PartitionsSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CatalogRecentWrites => write!(f, "catalog_recent_writes"),
            Self::CatalogAll => write!(f, "catalog_all"),
            Self::Fixed(p_ids) => {
                let mut p_ids = p_ids.iter().copied().collect::<Vec<_>>();
                p_ids.sort();
                write!(f, "fixed({p_ids:?})")
            }
        }
    }
}
