use std::time::Duration;

use clap_blocks::compactor_scheduler::{
    CompactorSchedulerConfig, CompactorSchedulerType, PartitionSourceConfigForLocalScheduler,
    ShardConfigForLocalScheduler,
};
use compactor_scheduler::{
    LocalSchedulerConfig, PartitionsSourceConfig, SchedulerConfig, ShardConfig,
};
use data_types::PartitionId;

fn convert_partitions_source_config(
    config: PartitionSourceConfigForLocalScheduler,
) -> PartitionsSourceConfig {
    let PartitionSourceConfigForLocalScheduler {
        partition_filter,
        process_all_partitions,
        compaction_partition_minute_threshold,
        ignore_partition_skip_marker: _,
    } = config;

    match (partition_filter, process_all_partitions) {
        (None, false) => PartitionsSourceConfig::CatalogRecentWrites {
            threshold: Duration::from_secs(compaction_partition_minute_threshold * 60),
        },
        (None, true) => PartitionsSourceConfig::CatalogAll,
        (Some(ids), false) => {
            PartitionsSourceConfig::Fixed(ids.iter().cloned().map(PartitionId::new).collect())
        }
        (Some(_), true) => panic!(
            "provided partition ID filter and specific 'process all', this does not make sense"
        ),
    }
}

/// Create a new [`ShardConfig`] from a [`ShardConfigForLocalScheduler`].
fn convert_shard_config(config: ShardConfigForLocalScheduler) -> Option<ShardConfig> {
    match (config.shard_count, config.shard_id, config.hostname) {
        // if no shard_count is provided, then we are not sharding
        (None, _, _) => None,
        // always use the shard_id if provided
        (Some(shard_count), Some(shard_id), _) => Some(ShardConfig {
            shard_id,
            n_shards: shard_count,
        }),
        // if no shard_id is provided, then we are sharding by hostname
        (Some(shard_count), None, Some(hostname)) => {
            let parsed_id = hostname
                .chars()
                .skip_while(|ch| !ch.is_ascii_digit())
                .take_while(|ch| ch.is_ascii_digit())
                .fold(None, |acc, ch| {
                    ch.to_digit(10).map(|b| acc.unwrap_or(0) * 10 + b)
                });
            assert!(parsed_id.is_some(), "hostname must end in a shard ID");
            Some(ShardConfig {
                shard_id: parsed_id.unwrap() as usize,
                n_shards: shard_count,
            })
        }
        (Some(_), None, None) => {
            panic!("shard_count must be paired with either shard_id or hostname")
        }
    }
}

pub(crate) fn convert_scheduler_config(config: CompactorSchedulerConfig) -> SchedulerConfig {
    match config.compactor_scheduler_type {
        CompactorSchedulerType::Local => SchedulerConfig::Local(LocalSchedulerConfig {
            commit_wrapper: None,
            partitions_source_config: convert_partitions_source_config(
                config.partition_source_config.clone(),
            ),
            shard_config: convert_shard_config(config.shard_config),
            ignore_partition_skip_marker: config
                .partition_source_config
                .ignore_partition_skip_marker,
        }),
        CompactorSchedulerType::Remote => unimplemented!("Remote scheduler not implemented"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(
        expected = "provided partition ID filter and specific 'process all', this does not make sense"
    )]
    fn process_all_and_partition_filter_incompatible() {
        let config = PartitionSourceConfigForLocalScheduler {
            compaction_partition_minute_threshold: 10,
            partition_filter: Some(vec![1, 7]),
            process_all_partitions: true,
            ignore_partition_skip_marker: false,
        };
        convert_partitions_source_config(config);
    }

    #[test]
    fn fixed_list_of_partitions() {
        let config = PartitionSourceConfigForLocalScheduler {
            compaction_partition_minute_threshold: 10,
            partition_filter: Some(vec![1, 7]),
            process_all_partitions: false,
            ignore_partition_skip_marker: false,
        };
        let partitions_source_config = convert_partitions_source_config(config);

        assert_eq!(
            partitions_source_config,
            PartitionsSourceConfig::Fixed([PartitionId::new(1), PartitionId::new(7)].into())
        );
    }

    #[test]
    fn all_in_the_catalog() {
        let config = PartitionSourceConfigForLocalScheduler {
            compaction_partition_minute_threshold: 10,
            partition_filter: None,
            process_all_partitions: true,
            ignore_partition_skip_marker: false,
        };
        let partitions_source_config = convert_partitions_source_config(config);

        assert_eq!(partitions_source_config, PartitionsSourceConfig::CatalogAll,);
    }

    #[test]
    fn normal_compaction() {
        let config = PartitionSourceConfigForLocalScheduler {
            compaction_partition_minute_threshold: 10,
            partition_filter: None,
            process_all_partitions: false,
            ignore_partition_skip_marker: false,
        };
        let partitions_source_config = convert_partitions_source_config(config);

        assert_eq!(
            partitions_source_config,
            PartitionsSourceConfig::CatalogRecentWrites {
                threshold: Duration::from_secs(600)
            },
        );
    }
}
